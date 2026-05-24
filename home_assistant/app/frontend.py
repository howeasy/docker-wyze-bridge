import contextlib
import json
import os
import re
import socket
import tempfile
import time
from functools import lru_cache, wraps
from pathlib import Path
from urllib.parse import quote_plus, urlsplit

from flask import (
    Flask,
    Response,
    abort,
    make_response,
    redirect,
    render_template,
    request,
    send_from_directory,
)
from werkzeug.exceptions import NotFound

from wyzebridge.build_config import VERSION
from wyze_bridge import WyzeBridge
from wyzebridge import config, web_ui
from wyzebridge.auth import WbAuth
from wyzebridge.go2rtc import send_native_talkback
from wyzebridge.camera_settings import set_camera_stream_mode
from wyzebridge.preview_validation import preview_file_is_image
from wyzebridge.web_ui import url_for

WYZE_DNS_URLS = (
    "https://auth-prod.api.wyze.com",
    "https://api.wyzecam.com/app",
    "https://app-core.cloud.wyze.com/app",
    "https://app.wyzecam.com/app",
    "https://devicemgmt-service.wyze.com",
    "https://webrtc.api.wyze.com",
)
WEBRTC_SIGNAL_API = "https://webrtc.api.wyze.com"
TUTK_HOST_SCAN_PATHS = (
    "/usr/local/lib/libIOTCAPIs_ALL.so",
    "/usr/local/lib/libAVAPIs.so",
)
TUTK_HOST_KEYWORDS = ("iotc", "tutk", "throughtek", "kalay")
HOSTNAME_PATTERN = re.compile(rb"(?<![A-Za-z0-9-])([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+)")


def _truthy_query_value(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _parse_resolv_conf(path: str = "/etc/resolv.conf") -> dict:
    data = {"path": path, "nameservers": [], "search": [], "options": []}
    try:
        with open(path, encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                key, *values = line.split()
                if key == "nameserver" and values:
                    data["nameservers"].append(values[0])
                elif key == "search":
                    data["search"] = values
                elif key == "options":
                    data["options"] = values
    except OSError as ex:
        data["error"] = f"{type(ex).__name__}: {ex}"
    return data


def _decode_route_ipv4(hex_value: str) -> str:
    return socket.inet_ntoa(bytes.fromhex(hex_value)[::-1])


def _parse_default_routes(path: str = "/proc/net/route") -> dict:
    routes = {"path": path, "default": []}
    try:
        with open(path, encoding="utf-8") as handle:
            next(handle, None)
            for raw_line in handle:
                fields = raw_line.split()
                if len(fields) < 4:
                    continue
                iface, destination_hex, gateway_hex, flags_hex = fields[:4]
                if destination_hex != "00000000":
                    continue
                routes["default"].append(
                    {
                        "interface": iface,
                        "gateway": _decode_route_ipv4(gateway_hex),
                        "flags": flags_hex,
                    }
                )
    except OSError as ex:
        routes["error"] = f"{type(ex).__name__}: {ex}"
    return routes


def _detect_outbound_ipv4(target: tuple[str, int] = ("8.8.8.8", 53)) -> dict:
    probe = {"target": f"{target[0]}:{target[1]}", "source_ip": None}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(target)
        probe["source_ip"] = sock.getsockname()[0]
    except OSError as ex:
        probe["error"] = f"{type(ex).__name__}: {ex}"
    finally:
        sock.close()
    return probe


def _host_from_url(value: str) -> str | None:
    host = urlsplit(value).hostname
    return host.lower() if host else None


def _is_plausible_hostname(host: str) -> bool:
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return False
    tld = labels[-1]
    return len(tld) >= 2 and tld.isalpha()


@lru_cache(maxsize=1)
def _tutk_library_hosts() -> tuple[str, ...]:
    hosts: set[str] = set()
    for path in TUTK_HOST_SCAN_PATHS:
        try:
            with open(path, "rb") as handle:
                data = handle.read()
        except OSError:
            continue
        for match in HOSTNAME_PATTERN.finditer(data):
            host = match.group(1).decode("ascii", "ignore").lower().strip(".")
            if _is_plausible_hostname(host) and any(
                keyword in host for keyword in TUTK_HOST_KEYWORDS
            ):
                hosts.add(host)
    return tuple(sorted(hosts))


def _candidate_dns_targets() -> list[str]:
    hosts = {"homeassistant.local"}
    for url in WYZE_DNS_URLS:
        if host := _host_from_url(url):
            hosts.add(host)
    hosts.update(_tutk_library_hosts())
    return sorted(hosts)


def _socket_enum_name(value: int, prefix: str) -> str:
    for name in dir(socket):
        if name.startswith(prefix) and getattr(socket, name, object()) == value:
            return name
    return str(value)


def _resolve_dns_target(host: str, port: int = 443) -> dict:
    result = {"host": host, "port": port, "addresses": []}
    started = time.perf_counter()
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        seen: set[tuple[int, int, int, str | None]] = set()
        for family, socktype, proto, _canonname, sockaddr in infos:
            address = sockaddr[0] if sockaddr else None
            key = (family, socktype, proto, address)
            if key in seen:
                continue
            seen.add(key)
            result["addresses"].append(
                {
                    "family": _socket_enum_name(family, "AF_"),
                    "socktype": _socket_enum_name(socktype, "SOCK_"),
                    "proto": proto,
                    "address": address,
                }
            )
        result["reachable"] = bool(result["addresses"])
    except OSError as ex:
        result["reachable"] = False
        result["error"] = f"{type(ex).__name__}: {ex}"
    result["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 2)
    return result


def network_snapshot() -> dict:
    return {
        "hostname": socket.gethostname(),
        "wb_ip": os.getenv("WB_IP"),
        "outbound_ipv4": _detect_outbound_ipv4(),
        "resolv_conf": _parse_resolv_conf(),
        "routes": _parse_default_routes(),
        "dns": {
            "targets": [_resolve_dns_target(host) for host in _candidate_dns_targets()],
            "tutk_library_hosts": list(_tutk_library_hosts()),
        },
    }


def create_app():
    app_root = Path(__file__).resolve().parent
    app = Flask(
        __name__,
        static_folder=str(app_root / "static"),
        template_folder=str(app_root / "templates"),
    )
    app.jinja_env.globals["url_for"] = url_for
    wb = WyzeBridge()
    talkback_dir = Path(tempfile.gettempdir()) / "wyze-talkback-http"
    talkback_dir.mkdir(parents=True, exist_ok=True)
    try:
        wb.start()
    except RuntimeError as ex:
        print(ex)
        print("Please ensure your host is up to date.")
        exit()
    if _truthy_query_value(os.getenv("NETWORK_TRACE")):
        print(
            f"[NETWORK_TRACE] {json.dumps(network_snapshot(), sort_keys=True)}",
            flush=True,
        )

    def camera_catalog():
        if hasattr(wb, "camera_catalog"):
            return wb.camera_catalog()
        return wb.streams.get_all_cam_info()

    def camera_info(cam_name: str):
        if hasattr(wb, "camera_info"):
            return wb.camera_info(cam_name)
        return wb.streams.get_info(cam_name)

    def _find_lock(slug: str):
        return next((lk for lk in wb.locks.locks.values() if lk.slug == slug.lower()), None)

    def _lock_to_dict(lock) -> dict:
        return {
            "slug": lock.slug,
            "nickname": lock.nickname,
            "mac": lock.mac,
            "model": lock.model,
            "state": lock.lock_state,
            "battery": lock.battery_level,
            "online": bool(lock.props.get("iot-device::iot-state")),
            "firmware": lock.props.get("device-info::firmware-ver"),
            "door_open": (not lock.props.get("lock::door-status")) if lock.has_door_sensor and "lock::door-status" in lock.props else None,
            "last_seen_ts": lock.last_seen_ts,
        }

    def auth_required(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if not wb.api.auth:
                return redirect(url_for("wyze_login"))
            if request.path.startswith("/kvs-config/") and request.remote_addr in {
                "127.0.0.1",
                "::1",
            }:
                return view(*args, **kwargs)
            return web_ui.auth.login_required(view)(*args, **kwargs)

        return wrapped_view

    @app.route("/login", methods=["GET", "POST"])
    def wyze_login():
        if wb.api.auth:
            return redirect(url_for("index"))
        if request.method == "GET":
            return render_template(
                "login.html",
                api=WbAuth.api,
                base_href=url_for("wyze_login").rstrip("/") + "/",
                version=VERSION,
            )

        tokens = request.form.get("tokens")
        refresh = request.form.get("refresh")

        if tokens or refresh:
            wb.api.token_auth(tokens=tokens, refresh=refresh)
            return {"status": "success"}

        credentials = {
            "email": request.form.get("email"),
            "password": request.form.get("password"),
            "key_id": request.form.get("keyId"),
            "api_key": request.form.get("apiKey"),
        }

        if all(credentials.values()):
            wb.api.creds.update(
                email=credentials["email"] or "",
                password=credentials["password"] or "",
                key_id=credentials["key_id"] or "",
                api_key=credentials["api_key"] or "",
            )
            return {"status": "success"}

        return {"status": "missing credentials"}

    @app.route("/")
    @auth_required
    def index():
        if not (columns := request.args.get("columns")):
            columns = request.cookies.get("number_of_columns", "2")

        if not (refresh := request.args.get("refresh")):
            refresh = request.cookies.get("refresh_period", "30")

        number_of_columns = int(columns) if columns.isdigit() else 0
        refresh_period = int(refresh) if refresh.isdigit() else 0
        show_video = bool(request.cookies.get("show_video"))
        autoplay = bool(request.cookies.get("autoplay"))

        if "autoplay" in request.args:
            autoplay = True

        if "video" in request.args:
            show_video = True
        elif "snapshot" in request.args:
            show_video = False

        video_format = request.cookies.get("video", "webrtc")

        if req_video := ({"webrtc", "hls", "kvs"} & set(request.args)):
            video_format = req_video.pop()

        resp = make_response(
            render_template(
                "index.html",
                cam_data=web_ui.all_cams(
                    wb.streams, wb.api.total_cams, cameras=camera_catalog()
                ),
                number_of_columns=number_of_columns,
                refresh_period=refresh_period,
                api=WbAuth.api,
                base_href=url_for("index").rstrip("/") + "/",
                version=VERSION,
                webrtc=bool(config.BRIDGE_IP),
                show_video=show_video,
                video_format=video_format.lower(),
                autoplay=autoplay,
            )
        )
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"

        resp.set_cookie("number_of_columns", str(number_of_columns))
        resp.set_cookie("refresh_period", str(refresh_period))
        resp.set_cookie("show_video", "1" if show_video else "")
        resp.set_cookie("video", video_format)
        fullscreen = "fullscreen" in request.args or bool(
            request.cookies.get("fullscreen")
        )
        resp.set_cookie("fullscreen", "1" if fullscreen else "")
        if order := request.args.get("order"):
            resp.set_cookie("camera_order", quote_plus(order))

        return resp

    @app.route("/health")
    def health():
        """Add-on health check."""
        health_data = wb.health()
        return Response(json.dumps(health_data), mimetype="application/json")

    @app.route("/health/details")
    def health_details():
        details = wb.health_details(request.args.get("stream"))
        if _truthy_query_value(request.args.get("network")):
            details["network"] = network_snapshot()
        return Response(json.dumps(details), mimetype="application/json")

    @app.route("/api/sse_status")
    @auth_required
    def sse_status():
        """Server sent event for camera status."""
        return Response(
            web_ui.sse_generator(wb.streams.get_sse_status),
            mimetype="text/event-stream",
        )

    @app.route("/api/status")
    @auth_required
    def api_status():
        return wb.streams.get_sse_status()

    @app.route("/kvs-config/<string:cam_name>")
    @auth_required
    def kvs_config(cam_name: str):
        if not (cam := wb.streams.get(cam_name)):
            return {"error": f"camera [{cam_name}] not found"}, 404
        config = wb.api.get_kvs_proxy_config(cam_name)
        if not config:
            return {"error": f"KVS config not ready for {cam_name}"}, 503
        return config

    @app.route("/api/locks")
    @auth_required
    def api_all_locks():
        return {
            "total": len(wb.locks.locks),
            "locks": [_lock_to_dict(lock) for lock in wb.locks.locks.values()],
        }

    @app.route("/api/locks/<string:slug>", methods=["GET"])
    @auth_required
    def api_lock_info(slug: str):
        lock = _find_lock(slug)
        if not lock:
            return {"status": "error", "response": f"Lock [{slug}] not found"}, 404
        return _lock_to_dict(lock)

    @app.route("/api/locks/<string:slug>/<string:action>", methods=["POST"])
    @auth_required
    def api_lock_action(slug: str, action: str):
        lock = _find_lock(slug)
        if not lock:
            return {"status": "error", "response": f"Lock [{slug}] not found"}, 404
        if action.lower() not in {"lock", "unlock"}:
            return {"status": "error", "response": "action must be 'lock' or 'unlock'"}, 400
        wb.locks._dispatch_command(lock, action.upper())
        return {"status": "success", "lock": slug, "action": action.lower()}

    @app.route("/api")
    @auth_required
    def api_all_cams():
        return web_ui.all_cams(wb.streams, wb.api.total_cams, cameras=camera_catalog())

    @app.route("/api/<string:cam_name>")
    @auth_required
    def api_cam(cam_name: str):
        if cam := camera_info(cam_name):
            return cam | web_ui.format_stream(cam_name)
        return {"error": f"Could not find camera [{cam_name}]"}

    @app.route("/api/<string:cam_name>/stream-config", methods=["GET", "PUT", "POST"])
    @app.route("/api/<string:cam_name>/stream-mode", methods=["GET", "PUT", "POST"])
    @auth_required
    def api_cam_stream_mode(cam_name: str):
        camera = wb.api.get_camera(cam_name)
        if not camera:
            return {"status": "error", "response": f"Camera [{cam_name}] not found"}, 404

        if request.method == "GET":
            config = wb.camera_stream_config(camera)
            return {"status": "success", "camera": camera.name_uri} | config

        payload = request.get_json(silent=True) or {}
        try:
            if ({"hd_enabled", "sd_enabled", "hd_kbps", "sd_kbps"} & set(payload)):
                config = wb.apply_camera_stream_config(camera, payload)
            elif "mode" in payload:
                mode = str(
                    payload.get("mode") or request.values.get("mode") or request.args.get("mode") or ""
                ).strip().lower()
                saved_mode = set_camera_stream_mode(camera.name_uri, mode)
                config = wb.camera_stream_config(camera)
                config["mode"] = saved_mode
            else:
                config = wb.camera_stream_config(camera)
        except ValueError as ex:
            message = str(ex) or "Invalid stream configuration"
            return {"status": "error", "response": message}, 409 if "not available" in message else 400

        wb.refresh_cams()
        return {"status": "success", "camera": camera.name_uri} | config

    @app.route("/api/<string:cam_name>/talkback", methods=["POST"])
    @auth_required
    def api_cam_talkback(cam_name: str):
        if not (stream := wb.streams.get(cam_name)):
            return {"status": "error", "response": f"Camera [{cam_name}] not found"}, 404

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return {
                "status": "error",
                "response": "Talkback requires a JSON object payload",
            }, 400

        stream_info = stream.get_info()
        if not stream_info.get("talkback_supported"):
            return {
                "status": "error",
                "response": stream_info.get("talkback_reason")
                or "Talkback is not available for this camera",
            }, 409

        alias = stream_info.get("talkback_alias") or stream_info.get("native_alias")
        if not alias:
            return {
                "status": "error",
                "response": "Talkback alias is unavailable for this camera",
            }, 500

        if payload.get("audio_b64") and not payload.get("audio_url"):
            suffix = str(payload.get("file_ext") or payload.get("format") or "wav").strip().lower()
            suffix = "".join(ch for ch in suffix if ch.isalnum()) or "wav"
            token = next(tempfile._get_candidate_names())
            path = talkback_dir / f"{token}.{suffix}"
            try:
                path.write_text(str(payload["audio_b64"]), encoding="ascii")
            except OSError as ex:
                return {
                    "status": "error",
                    "response": f"Unable to stage talkback upload: {ex}",
                }, 500
            payload = dict(payload)
            payload.pop("audio_b64", None)
            payload["audio_url"] = f"http://127.0.0.1:5000/api/talkback-file/{path.name}"

        result = send_native_talkback(payload, alias)
        status_code = 200 if result.get("status") == "success" else 502
        return result, status_code

    @app.route("/api/talkback-file/<string:file_name>")
    def api_talkback_file(file_name: str):
        if request.remote_addr not in {"127.0.0.1", "::1"}:
            abort(404)
        path = talkback_dir / Path(file_name).name
        if not path.is_file():
            abort(404)
        try:
            import base64

            audio_bytes = base64.b64decode(path.read_text(encoding="ascii"), validate=True)
        except (OSError, ValueError):
            abort(404)
        response = make_response(audio_bytes)
        response.headers["Content-Type"] = "audio/wav"
        return response

    @app.route("/api/<cam_name>/<cam_cmd>", methods=["GET", "PUT", "POST"])
    @app.route("/api/<cam_name>/<cam_cmd>/<path:payload>")
    @auth_required
    def api_cam_control(cam_name: str, cam_cmd: str, payload: str | dict = ""):
        """API Endpoint to send tutk commands to the camera."""
        if not payload and (args := request.values.to_dict()):
            args.pop("api", None)
            payload = next(iter(args.values())) if len(args) == 1 else args
        if not payload and request.is_json:
            json = request.get_json()
            if isinstance(json, dict):
                payload = json if len(json) > 1 else list(json.values())[0]
            else:
                payload = json
        elif not payload and request.data:
            payload = request.data.decode()

        return wb.streams.send_cmd(cam_name, cam_cmd.lower(), payload)

    @app.route("/signaling/<string:name>")
    @auth_required
    def webrtc_signaling(name):
        if "kvs" in request.args:
            return wb.api.get_kvs_signal(name)
        return web_ui.get_webrtc_signal(name, WbAuth.api)

    @app.route("/webrtc/<string:name>")
    @auth_required
    def webrtc(name):
        """View WebRTC direct from camera."""
        if (webrtc := wb.api.get_kvs_signal(name)).get("result") == "ok":
            return make_response(render_template("webrtc.html", webrtc=webrtc))
        return webrtc

    @app.route("/snapshot/<string:img_file>")
    @auth_required
    def rtsp_snapshot(img_file: str):
        """Use ffmpeg to take a snapshot from the rtsp stream."""
        if config.SNAPSHOT_TYPE == "api":
            return thumbnail(img_file)
        if wb.streams.get_snapshot(Path(img_file).stem)["ok"]:
            return send_from_directory(config.IMG_PATH, img_file)

        return thumbnail(img_file)

    @app.route("/img/<string:img_file>")
    @auth_required
    def img(img_file: str):
        """
        Serve an existing local image or take a new snapshot from the rtsp stream.

        Use the exp parameter to fetch a new snapshot if the existing one is too old.
        """
        try:
            img_path = config.IMG_PATH + img_file
            if os.path.getsize(img_path) <= 0:
                with contextlib.suppress(OSError):
                    os.remove(img_path)
                raise NotFound
            if not preview_file_is_image(img_path):
                with contextlib.suppress(OSError):
                    os.remove(img_path)
                raise NotFound
            if exp := request.args.get("exp"):
                created_at = os.path.getmtime(img_path)
                if time.time() - created_at > int(exp):
                    raise NotFound
            return send_from_directory(config.IMG_PATH, img_file)
        except (NotFound, FileNotFoundError, ValueError):
            if config.SNAPSHOT_TYPE == "api":
                return thumbnail(img_file)
            return rtsp_snapshot(img_file)

    @app.route("/thumb/<string:img_file>")
    @auth_required
    def thumbnail(img_file: str):
        path = Path(img_file)
        stem = path.stem
        candidates = [stem]
        if stem.endswith("-sub"):
            candidates.append(stem.removesuffix("-sub"))
        for uri in candidates:
            if wb.streams.refresh_preview(uri)["ok"]:
                return send_from_directory(config.IMG_PATH, f"{uri}{path.suffix}")

        return redirect("/static/notavailable.svg", code=307)

    @app.route("/photo/<string:img_file>")
    @auth_required
    def boa_photo(img_file: str):
        """Take a photo on the camera and grab it over the boa http server."""
        uri = Path(img_file).stem
        if not (cam := wb.streams.get(uri)):
            return redirect("/static/notavailable.svg", code=307)
        if photo := web_ui.boa_snapshot(cam):
            return send_from_directory(config.IMG_PATH, f"{uri}_{photo[0]}")
        return redirect(f"/img/{img_file}", code=307)

    @app.route("/restart/<string:restart_cmd>")
    @auth_required
    def restart_bridge(restart_cmd: str):
        """
        Restart parts of the wyze-bridge.

        /restart/cameras:       Restart camera connections.
        /restart/rtsp_server:   Restart rtsp-simple-server.
        /restart/all:           Restart camera connections and rtsp-simple-server.
        """
        if restart_cmd == "cameras":
            wb.streams.stop_all()
            wb.streams.monitor_streams(wb.mtx.health_check)
        elif restart_cmd == "rtsp_server":
            wb.mtx.restart()
        elif restart_cmd == "cam_data":
            wb.refresh_cams()
            restart_cmd = "cameras"
        elif restart_cmd == "all":
            wb.restart(fresh_data=True)
            restart_cmd = "cameras,rtsp_server"
        else:
            return {"result": "error"}
        return {"result": "ok", "restart": restart_cmd.split(",")}

    @app.route("/cams.m3u8")
    @auth_required
    def iptv_playlist():
        """
        Generate an m3u8 playlist with all enabled cameras.
        """
        hostname = request.host.split(":")[0]
        cameras = web_ui.format_streams(wb.streams.get_all_cam_info())
        resp = make_response(
            render_template("m3u8.html", cameras=cameras, hostname=hostname)
        )
        resp.headers.set("content-type", "application/x-mpegURL")
        return resp

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=False, host="0.0.0.0", port=5000)
