import os
from os import makedirs
import signal
import sys
from dataclasses import replace
from threading import Thread

from wyzebridge.build_config import BUILD_STR, VERSION
from wyzebridge.config import (
    BRIDGE_IP,
    HASS_TOKEN,
    IMG_PATH,
    LLHLS,
    ON_DEMAND,
    STREAM_AUTH,
    TOKEN_PATH,
)
from wyzebridge.auth import WbAuth
from wyzebridge.bridge_utils import env_bool, env_cam, is_livestream, migrate_path
from wyzebridge.camera_settings import get_camera_setting, update_camera_settings
from wyzebridge.bridge_diagnostics import collect_bridge_diagnostics
from wyzebridge.hass import setup_hass
from wyzebridge.logging import logger
from wyzebridge.mtx_server import MtxServer
from wyzebridge.stream_manager import StreamManager
from wyzebridge.go2rtc import native_stream_info
from wyzebridge.wyze_api import WyzeApi
from wyzebridge.wyze_stream import WyzeStream, WyzeStreamOptions
from wyzebridge.wyzelocks import WyzeLockManager
from wyzecam.api_models import WyzeAccount, WyzeCamera

setup_hass(HASS_TOKEN)

makedirs(TOKEN_PATH, exist_ok=True)
makedirs(IMG_PATH, exist_ok=True)

if HASS_TOKEN:
    migrate_path("/config/wyze-bridge/", "/config/")


class WyzeBridge(Thread):
    __slots__ = "api", "streams", "mtx", "main_pid", "locks"

    def __init__(self) -> None:
        Thread.__init__(self)
        self.main_pid = os.getpid()  # Store main process PID

        for sig in ["SIGTERM", "SIGINT"]:
            signal.signal(getattr(signal, sig), self.clean_up)

        print(f"\n🚀 DOCKER-WYZE-BRIDGE v{VERSION} {BUILD_STR}\n")
        self.api: WyzeApi = WyzeApi()
        self.streams: StreamManager = StreamManager(self.api)
        self.mtx: MtxServer = MtxServer()
        self.mtx.setup_webrtc(BRIDGE_IP)
        if LLHLS:
            self.mtx.setup_llhls(TOKEN_PATH, bool(HASS_TOKEN))
        self.locks: WyzeLockManager = WyzeLockManager(
            auth_provider=lambda: self.api.auth,
            username_provider=lambda: self.api.creds.email,
        )

    def health(self):
        mtx_alive = self.mtx.sub_process_alive()
        active_streams = len(self.streams.active_streams())
        wyze_authed = (
            self.api.auth is not None and self.api.auth.access_token is not None
        )
        return {
            "mtx_alive": mtx_alive,
            "wyze_authed": wyze_authed,
            "active_streams": active_streams,
        }

    def health_details(self, stream_name: str | None = None):
        stream_info = self.streams.get_info(stream_name) if stream_name else None
        return self.health() | collect_bridge_diagnostics(stream_name, stream_info)

    def run(self, fresh_data: bool = False) -> None:
        self._initialize(fresh_data)

    def _initialize(self, fresh_data: bool = False) -> None:
        self.api.login(fresh_data=fresh_data)
        user = self.api.get_user()
        if user and user.email:
            WbAuth.set_email(email=user.email, force=fresh_data)
        elif self.api.creds.email:
            logger.warning(
                "[AUTH] Wyze user profile unavailable during init; using configured email fallback for local auth"
            )
            WbAuth.set_email(email=self.api.creds.email, force=fresh_data)
        else:
            logger.warning(
                "[AUTH] Wyze user profile unavailable during init and no email fallback is configured"
            )
        self.mtx.setup_auth(WbAuth.api, STREAM_AUTH)
        self.setup_streams(user)
        if self.streams.total < 1 and not self._has_enabled_native_feed():
            return signal.raise_signal(signal.SIGINT)

        if logger.getEffectiveLevel() == 10:  # if we're at debug level
            logger.debug(f"[BRIDGE] MTX config:\n{self.mtx.dump_config()}")

        self.mtx.start()
        if not self.locks.is_alive():
            self.locks.start()
        self.streams.monitor_streams(self.mtx.health_check)

    def restart(self, fresh_data: bool = False) -> None:
        self.mtx.stop()
        self.streams.stop_all()
        self._initialize(fresh_data)

    def refresh_cams(self) -> None:
        self.mtx.stop()
        self.streams.stop_all()
        self.api.get_cameras(fresh_data=True)
        self._initialize(False)

    def setup_streams(self, user: WyzeAccount | None = None):
        """Gather and setup streams for each camera."""
        user = user or self.api.get_user()
        if not user:
            logger.error("[BRIDGE] Unable to load Wyze user profile; skipping stream setup")
            return

        for cam in self.api.filtered_cams():
            logger.info(
                f"[+] Adding {cam.nickname} [{cam.product_model}] at {cam.name_uri}"
            )

            stream_config = self.camera_stream_config(cam)
            feeds = stream_config["feeds"]
            options = WyzeStreamOptions(
                quality=f"hd{feeds['hd']['kbps']}",
                audio=bool(env_cam("enable_audio", cam.name_uri)),
                record=bool(env_cam("record", cam.name_uri)),
                reconnect=(not ON_DEMAND) or is_livestream(cam.name_uri),
            )

            create_main = any(
                feed["enabled"] and feed["path"] == "main" for feed in feeds.values()
            )
            create_sub = bool(feeds["sd"]["enabled"] and feeds["sd"]["path"] == "sub")

            if create_main:
                stream = WyzeStream(user, self.api, cam, options)
                if not cam.is_kvs:
                    stream.rtsp_fw_enabled = self.rtsp_fw_proxy(cam, stream)
                self.mtx.add_path(stream.uri, not options.reconnect, stream.uses_kvs_source)
                self.streams.add(stream)

                if env_cam("record", cam.name_uri):
                    self.mtx.record(stream.uri)

            if create_sub:
                self.add_substream(user, self.api, cam, options)

    def _camera_catalog_entry(self, cam: WyzeCamera) -> dict:
        config = self.camera_stream_config(cam)
        feeds = config["feeds"]
        enabled = any(feed["enabled"] for feed in feeds.values())
        selected_feed = "hd"
        if feeds["hd"]["enabled"] and feeds["hd"]["path"] == "native":
            selected_feed = "hd"
        elif feeds["sd"]["enabled"]:
            selected_feed = "sd"
        native_info = native_stream_info(cam, substream=selected_feed == "sd")
        selected_resolution = feeds[selected_feed].get("resolution")
        connected = bool(
            feeds[selected_feed]["path"] == "native"
            and native_info.get("native_selected")
        )
        status = "connected" if connected else "stopped" if enabled else "disabled"

        return {
            "name_uri": cam.name_uri,
            "camera_uri": cam.name_uri,
            "source": "go2rtc" if feeds[selected_feed]["path"] == "native" else "kvs",
            "status": status,
            "connected": connected,
            "enabled": enabled,
            "motion": False,
            "motion_ts": 0,
            "on_demand": False,
            "audio": bool(env_cam("enable_audio", cam.name_uri)),
            "record": bool(env_cam("record", cam.name_uri)),
            "substream": False,
            "webrtc": False,
            "start_time": 0,
            "req_frame_size": 0,
            "req_bitrate": 0,
            "actual_resolution": selected_resolution,
            "bridge_can_substream": cam.bridge_can_substream,
            "camera_can_substream": cam.can_substream,
            "rtsp_fw_enabled": False,
            "rtsp_url": native_info.get("native_rtsp_url")
            if feeds[selected_feed]["path"] == "native"
            else None,
            "hls_url": None,
            "webrtc_url": None,
            "rtmp_url": None,
            "boa_url": None,
        } | native_info | cam.model_dump(exclude={"p2p_id", "enr", "parent_enr"})

    def camera_info(self, cam_name: str) -> dict:
        if info := self.streams.get_info(cam_name):
            return info
        if cam_name.endswith("-sub"):
            return {}
        if not (cam := self.api.get_camera(cam_name)):
            return {}
        return self._camera_catalog_entry(cam)

    def camera_catalog(self) -> dict:
        catalog = self.streams.get_all_cam_info()
        for cam in self.api.filtered_cams():
            if cam.name_uri in catalog:
                continue
            config = self.camera_stream_config(cam)
            if any(
                feed["enabled"] and feed["path"] in {"main", "native"}
                for feed in config["feeds"].values()
            ):
                catalog[cam.name_uri] = self._camera_catalog_entry(cam)
        return catalog

    def _has_enabled_native_feed(self) -> bool:
        return any(
            feed.get("enabled") and feed.get("path") == "native"
            for cam in self.api.filtered_cams()
            for feed in self.camera_stream_config(cam)["feeds"].values()
        )

    def camera_stream_mode(self, cam: WyzeCamera) -> str:
        return self.camera_stream_config(cam)["mode"]

    def camera_substream_enabled(self, cam: WyzeCamera, stream_mode: str = "") -> bool:
        feed = self.camera_stream_config(cam)["feeds"]["sd"]
        return bool(feed["enabled"] and feed["path"] == "sub")

    def camera_hd_supported(self, cam: WyzeCamera) -> bool:
        if cam.product_model == "HL_BC":
            return False
        return True

    def camera_sd_supported(self, cam: WyzeCamera) -> bool:
        return bool(cam.bridge_can_substream or cam.product_model == "HL_BC")

    def camera_feed_resolution(self, cam: WyzeCamera, feed: str) -> str | None:
        actual = self.camera_actual_resolution(cam, substream=feed == "sd")
        if actual:
            return actual
        if feed == "sd" and self.camera_sd_supported(cam):
            return "640x360"
        if feed == "hd" and self.camera_hd_supported(cam):
            if cam.product_model == "HL_CAM3P":
                return "2560x1440"
            if cam.product_model == "HL_CAM4":
                return "2560x1440" if env_bool("GO2RTC_RTSP_PORT") else "1920x1080"
            if cam.is_2k:
                return "2560x1440"
            return "1920x1080"
        return None

    def camera_actual_resolution(self, cam: WyzeCamera, substream: bool = False) -> str | None:
        info = self.streams.get_info(cam.name_uri + ("-sub" if substream else ""))
        if resolution := info.get("actual_resolution"):
            return str(resolution)
        if not cam.camera_info:
            return None
        parm = cam.camera_info.get("sdParm" if substream else "videoParm") or {}
        width = parm.get("width")
        height = parm.get("height")
        if width and height:
            return f"{width}x{height}"
        return None

    def camera_feed_config(self, cam: WyzeCamera) -> dict:
        hd_supported = self.camera_hd_supported(cam)
        sd_supported = self.camera_sd_supported(cam)
        native_hd = native_stream_info(cam, False)
        native_sd = native_stream_info(cam, True)
        env_uri = cam.name_uri.upper().replace("-", "_")
        hd_env_configured = any(
            os.getenv(key) is not None
            for key in (f"HD_{env_uri}", "HD_ALL", "HD")
        )
        sd_env_configured = any(
            os.getenv(key) is not None
            for key in (f"SD_{env_uri}", "SD_ALL", "SD")
        )
        legacy_mode = str(
            get_camera_setting(cam.name_uri, "stream", "__missing__")
            if get_camera_setting(cam.name_uri, "stream", "__missing__") != "__missing__"
            else env_cam("stream", cam.name_uri)
        ).strip().lower()
        hd_enabled_saved = get_camera_setting(cam.name_uri, "hd", "__missing__")
        sd_enabled_saved = get_camera_setting(cam.name_uri, "sd", "__missing__")
        default_sd_enabled = env_bool(f"SUBSTREAM_{cam.name_uri}") or (
            env_bool("SUBSTREAM") and cam.bridge_can_substream
        )
        hd_enabled = (
            env_cam("hd", cam.name_uri, style="bool")
            if hd_env_configured
            else bool(hd_enabled_saved)
            if hd_enabled_saved != "__missing__"
            else legacy_mode not in {"sub"}
        ) and hd_supported
        sd_enabled = (
            env_cam("sd", cam.name_uri, style="bool")
            if sd_env_configured
            else bool(sd_enabled_saved)
            if sd_enabled_saved != "__missing__"
            else legacy_mode in {"sub", "both"} or (legacy_mode == "" and default_sd_enabled)
        ) and sd_supported
        if not hd_enabled and not sd_enabled:
            if sd_supported:
                sd_enabled = True
            elif hd_supported:
                hd_enabled = True

        sd_path = "main"
        if sd_enabled and sd_supported:
            if env_bool("GO2RTC_RTSP_PORT") and native_sd.get("native_selected"):
                sd_path = "native"
            else:
                sd_path = "sub" if cam.bridge_can_substream else "main"

        hd_path = "main"
        if hd_enabled and hd_supported and env_bool("GO2RTC_RTSP_PORT") and native_hd.get(
            "native_selected"
        ):
            hd_path = "native"

        hd_kbps = int(get_camera_setting(cam.name_uri, "hd_kbps") or env_cam("quality", cam.name_uri, "hd180")[2:] or 180)
        sd_kbps = int(get_camera_setting(cam.name_uri, "sd_kbps") or env_cam("sub_quality", cam.name_uri, "sd30")[2:] or 30)
        mode = "both" if hd_enabled and sd_enabled else "sub" if sd_enabled else "main"
        return {
            "mode": mode,
            "feeds": {
                "hd": {
                    "enabled": hd_enabled,
                    "supported": hd_supported,
                    "kbps": hd_kbps,
                    "resolution": self.camera_feed_resolution(cam, "hd"),
                    "path": hd_path,
                    "reason": "" if hd_supported else "HD stream is not available for this camera",
                },
                "sd": {
                    "enabled": sd_enabled,
                    "supported": sd_supported,
                    "kbps": sd_kbps,
                    "resolution": self.camera_feed_resolution(cam, "sd"),
                    "path": sd_path,
                    "reason": "" if sd_supported else "SD stream is not available for this camera",
                },
            },
        }

    def camera_stream_config(self, cam: WyzeCamera) -> dict:
        return self.camera_feed_config(cam)

    def apply_camera_stream_config(self, cam: WyzeCamera, payload: dict) -> dict:
        config = self.camera_feed_config(cam)
        hd_supported = config["feeds"]["hd"]["supported"]
        sd_supported = config["feeds"]["sd"]["supported"]

        hd_enabled = bool(payload.get("hd_enabled"))
        sd_enabled = bool(payload.get("sd_enabled"))
        if hd_enabled and not hd_supported:
            raise ValueError("HD stream is not available for this camera")
        if sd_enabled and not sd_supported:
            raise ValueError("SD stream is not available for this camera")
        if not hd_enabled and not sd_enabled:
            raise ValueError("At least one feed must stay enabled")

        hd_kbps = int(payload.get("hd_kbps") or config["feeds"]["hd"]["kbps"])
        sd_kbps = int(payload.get("sd_kbps") or config["feeds"]["sd"]["kbps"])
        mode = "both" if hd_enabled and sd_enabled else "sub" if sd_enabled else "main"
        update_camera_settings(
            cam.name_uri,
            {
                "stream": mode,
                "hd": hd_enabled,
                "sd": sd_enabled,
                "hd_kbps": hd_kbps,
                "sd_kbps": sd_kbps,
            },
        )
        return self.camera_feed_config(cam)

    def rtsp_fw_proxy(self, cam: WyzeCamera, stream: WyzeStream) -> bool:
        if rtsp_fw := env_bool("rtsp_fw").lower():
            if rtsp_path := stream.check_rtsp_fw(rtsp_fw == "force"):
                rtsp_uri = f"{cam.name_uri}-fw"
                logger.info(f"[-->] Adding /{rtsp_uri} as a source")
                self.mtx.add_source(rtsp_uri, rtsp_path)
                return True
        return False

    def add_substream(
        self,
        user: WyzeAccount,
        api: WyzeApi,
        cam: WyzeCamera,
        options: WyzeStreamOptions,
    ):
        """Setup and add substream if enabled for camera."""
        if self.camera_substream_enabled(cam):
            quality = f"sd{self.camera_stream_config(cam)['feeds']['sd']['kbps']}"
            record = bool(env_cam("sub_record", cam.name_uri))
            sub_opt = replace(options, substream=True, quality=quality, record=record)
            logger.info(
                f"[++] Adding {cam.name_uri} substream quality: {quality} record: {record}"
            )
            sub = WyzeStream(user, api, cam, sub_opt)
            self.mtx.add_path(sub.uri, not options.reconnect, sub.uses_kvs_source)
            self.streams.add(sub)

    def clean_up(self, *_):
        """Stop all streams and clean up before shutdown."""
        # Only run cleanup in the main process, not in child processes
        if os.getpid() != self.main_pid:
            return
        if self.streams.stop_flag:
            sys.exit(0)
        if self.streams:
            self.streams.stop_all()
        self.mtx.stop()
        if self.locks and self.locks.is_alive():
            self.locks.stop()
        logger.info("👋 goodbye!")
        sys.exit(0)


if __name__ == "__main__":
    wb = WyzeBridge()
    wb.run()
    sys.exit(0)
