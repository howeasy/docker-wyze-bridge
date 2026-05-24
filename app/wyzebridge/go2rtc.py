import base64
import os
import re
import socket
import tempfile
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

import requests

from wyzebridge.config import IMG_PATH, IMG_TYPE
from wyzebridge.logging import logger
from wyzebridge.preview_validation import preview_bytes_are_valid_image

DEFAULT_GO2RTC_API_PORT = 11984
DEFAULT_GO2RTC_RTSP_PORT = 19554
_NATIVE_ALIAS_READY_CACHE_TTL = 10.0
_NATIVE_ALIAS_READY_CACHE: dict[str, tuple[float, bool]] = {}
_VALIDATED_NATIVE_MODELS = {
    "HL_CAM3P": {
        "reason": "HL_CAM3P validated on native go2rtc for the SD feed while the main alias remains unproven on this host",
        "selected": False,
        "sub_selected": True,
    },
    "HL_CAM4": {
        "reason": "HL_CAM4 validated on native go2rtc with higher-resolution main stream",
        "selected": True,
        "sub_selected": True,
    },
    "HL_BC": {
        "reason": "HL_BC stays bridge-first because native go2rtc still validated at 640x360",
        "selected": False,
    },
}


def _go2rtc_api_port() -> int:
    return int(os.getenv("GO2RTC_API_PORT", str(DEFAULT_GO2RTC_API_PORT)))


def _go2rtc_rtsp_port() -> int:
    return int(os.getenv("GO2RTC_RTSP_PORT", str(DEFAULT_GO2RTC_RTSP_PORT)))


def go2rtc_api_base() -> str:
    return f"http://127.0.0.1:{_go2rtc_api_port()}"


def go2rtc_rtsp_base() -> str:
    return f"rtsp://127.0.0.1:{_go2rtc_rtsp_port()}"


def native_alias(name_uri: str, substream: bool = False) -> str:
    if substream or name_uri.endswith("-sub"):
        base_name = name_uri[:-4] if name_uri.endswith("-sub") else name_uri
        return f"{base_name}-sd"
    return name_uri


def native_snapshot_path(cam_name: str) -> Path:
    return Path(f"{IMG_PATH}{cam_name}.{IMG_TYPE}")


def _content_matches_existing(output_path: Path, content: bytes) -> bool:
    try:
        return output_path.exists() and output_path.read_bytes() == content
    except OSError:
        return False


def _validated_native_model(camera) -> dict[str, Any] | None:
    return _VALIDATED_NATIVE_MODELS.get(getattr(camera, "product_model", ""))


def _go2rtc_api_reachable(timeout: float = 0.75) -> bool:
    try:
        response = requests.get(f"{go2rtc_api_base()}/api", timeout=timeout)
        response.raise_for_status()
        return True
    except requests.RequestException:
        return False


def _native_alias_is_ready(alias: str, timeout: float = 2.0) -> bool:
    now = time.monotonic()
    cached = _NATIVE_ALIAS_READY_CACHE.get(alias)
    if cached and now - cached[0] < _NATIVE_ALIAS_READY_CACHE_TTL:
        return cached[1]

    ready = bool(_go2rtc_stream_details(alias, timeout=timeout))
    _NATIVE_ALIAS_READY_CACHE[alias] = (now, ready)
    return ready


def native_stream_info(camera, substream: bool = False) -> dict[str, Any]:
    alias = native_alias(camera.name_uri, substream)
    primary_alias = native_alias(camera.name_uri, False)
    model_support = _validated_native_model(camera)
    api_reachable = _go2rtc_api_reachable()
    supported = bool(model_support and not getattr(camera, "is_gwell", False))
    selected_flag = (
        model_support.get("sub_selected") if substream else model_support.get("selected")
    ) if model_support else False
    # selected is gated only on api_reachable, not alias readiness — alias readiness is a
    # transient startup condition and must not cause native cameras to lose their RTSP URL
    selected = bool(supported and selected_flag and api_reachable)
    # alias_ready is a diagnostic-only field: it tells callers whether the stream is already
    # hot in go2rtc, but a False value does NOT block selection or URL assignment
    alias_ready = bool(selected and _native_alias_is_ready(alias))

    if getattr(camera, "is_gwell", False):
        reason = "GW_* remains blocked until a real Gwell model validates end-to-end"
    elif model_support:
        reason = model_support["reason"]
        if selected_flag and not api_reachable:
            reason = f"{reason}; go2rtc sidecar is not reachable"
    else:
        reason = "bridge remains the default until native go2rtc is validated for this model"

    if substream:
        talkback_supported = False
        talkback_reason = "talkback is only exposed on the primary native alias"
    elif selected:
        talkback_supported = True
        talkback_reason = "API-first talkback is available through the native go2rtc alias"
    elif supported and model_support and selected_flag and not api_reachable:
        talkback_supported = False
        talkback_reason = "talkback requires a reachable go2rtc sidecar"
    elif supported:
        talkback_supported = False
        talkback_reason = "talkback is limited to native-selected cameras in 4.2"
    else:
        talkback_supported = False
        talkback_reason = "talkback is unavailable until native go2rtc is validated for this model"

    return {
        "native_supported": supported,
        "native_selected": selected,
        "native_reason": reason,
        "native_alias": alias,
        "native_rtsp_url": f"{go2rtc_rtsp_base()}/{alias}",
        "native_preload": selected,
        "native_api_reachable": api_reachable,
        "native_alias_ready": alias_ready,
        "snapshot_source": "go2rtc" if selected else "rtsp",
        "talkback_supported": talkback_supported,
        "talkback_reason": talkback_reason,
        "talkback_alias": primary_alias,
        "talkback_source": "go2rtc" if talkback_supported else None,
    }


def _socket_probe(port: int, timeout: float = 0.5) -> dict[str, Any]:
    result = {
        "host": "127.0.0.1",
        "port": port,
        "reachable": False,
    }
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=timeout):
            result["reachable"] = True
    except OSError as ex:
        result["error"] = f"{type(ex).__name__}: {ex}"
    return result


def go2rtc_probe(timeout: float = 1.0, include_streams: bool = False) -> dict[str, Any]:
    result = {
        "api": {
            "listener": go2rtc_api_base(),
            "reachable": False,
        },
        "rtsp": _socket_probe(_go2rtc_rtsp_port()),
    }
    try:
        response = requests.get(f"{go2rtc_api_base()}/api", timeout=timeout)
        result["api"]["status_code"] = response.status_code
        response.raise_for_status()
        result["api"]["reachable"] = True
        if include_streams:
            streams_response = requests.get(
                f"{go2rtc_api_base()}/api/streams", timeout=timeout
            )
            streams_response.raise_for_status()
            data = streams_response.json()
            result["aliases"] = sorted(data.keys()) if isinstance(data, dict) else []
    except requests.RequestException as ex:
        if ex.response is not None:
            result["api"]["status_code"] = ex.response.status_code
        result["api"]["error"] = str(ex)
    return result


def preload_native_stream(alias: str, timeout: float = 2.0) -> dict[str, Any]:
    result = {"alias": alias, "requested": False, "ok": False}
    try:
        response = requests.put(
            f"{go2rtc_api_base()}/api/preload",
            params={"src": alias},
            timeout=timeout,
        )
        result["requested"] = True
        result["status_code"] = response.status_code
        response.raise_for_status()
        result["ok"] = True
    except requests.RequestException as ex:
        if ex.response is not None:
            result["status_code"] = ex.response.status_code
        result["error"] = str(ex)
    return result


def write_native_snapshot(alias: str, cam_name: str, timeout: float = 15.0) -> bool:
    output_path = native_snapshot_path(cam_name)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout
    last_error = "no frame returned"
    while time.monotonic() < deadline:
        try:
            response = requests.get(
                f"{go2rtc_api_base()}/api/frame.jpeg?src={quote(alias, safe='')}",
                timeout=max(0.5, min(5.0, deadline - time.monotonic())),
            )
            if response.status_code == 503:
                last_error = "status=503"
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                continue
            if response.status_code == 404:
                logger.warning(f"❗ [{cam_name}] Native snapshot from {alias} failed: status=404")
                return False
            response.raise_for_status()
            if not response.content:
                last_error = "empty response"
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                continue
            if not preview_bytes_are_valid_image(response.content):
                logger.warning(f"❗ [{cam_name}] Native snapshot from {alias} was not a valid image")
                return False
            if _content_matches_existing(output_path, response.content):
                last_error = "matched existing preview"
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
                continue
            output_path.write_bytes(response.content)
            return output_path.stat().st_size > 0
        except (requests.RequestException, OSError) as ex:
            last_error = f"{type(ex).__name__}: {ex}"
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    logger.warning(f"❗ [{cam_name}] Native snapshot from {alias} failed: {last_error}")
    return False


def _go2rtc_stream_request(
    alias: str, src: str, mode: str, timeout: float = 20.0
) -> dict[str, Any]:
    result = {
        "status": "error",
        "source": "go2rtc",
        "alias": alias,
        "mode": mode,
    }
    try:
        response = requests.post(
            f"{go2rtc_api_base()}/api/streams",
            params={"dst": alias, "src": src},
            timeout=timeout,
        )
        result["status_code"] = response.status_code
        response.raise_for_status()
        result["status"] = "success"
        result["response"] = "ok"
        with suppress(AttributeError, ValueError):
            parsed = response.json()
            if parsed not in (None, ""):
                result["response"] = parsed
        text = getattr(response, "text", "")
        if result["response"] == "ok" and text:
            result["response"] = text.strip() or "ok"
    except requests.RequestException as ex:
        if ex.response is not None:
            result["status_code"] = ex.response.status_code
        body = ""
        with suppress(Exception):
            body = ex.response.text.strip() if ex.response is not None else ""
        result["response"] = body or str(ex)
    return result


def _go2rtc_stream_details(alias: str, timeout: float = 2.0) -> dict[str, Any]:
    try:
        response = requests.get(
            f"{go2rtc_api_base()}/api/streams",
            params={"src": alias, "microphone": "any"},
            timeout=timeout,
        )
        response.raise_for_status()
        data = response.json()
        return data if isinstance(data, dict) else {}
    except (requests.RequestException, ValueError):
        return {}


_MEDIA_CODEC_RE = re.compile(r"^audio,\s+(sendonly|recvonly),\s+([^,]+)$", re.IGNORECASE)


def _ffmpeg_codec_from_go2rtc_media(media: str) -> str | None:
    match = _MEDIA_CODEC_RE.match(str(media).strip())
    if not match or match.group(1).lower() != "sendonly":
        return None

    codec_spec = match.group(2).strip()
    parts = codec_spec.split("/")
    codec_name = parts[0].upper()
    rate = parts[1] if len(parts) > 1 and parts[1].isdigit() else ""

    codec_map = {
        "AAC": "aac",
        "MPEG4-GENERIC": "aac",
        "PCMA": "pcma",
        "PCMU": "pcmu",
        "PCM": "pcm",
        "L16": "pcm",
        "PCML": "pcml",
        "OPUS": "opus",
    }
    ffmpeg_codec = codec_map.get(codec_name)
    if not ffmpeg_codec:
        return None
    return f"{ffmpeg_codec}/{rate}" if rate else ffmpeg_codec


def _talkback_ffmpeg_codec(alias: str, timeout: float = 2.0) -> str | None:
    details = _go2rtc_stream_details(alias, timeout=timeout)

    producers = details.get("producers") if isinstance(details, dict) else None
    if isinstance(producers, list):
        for producer in producers:
            medias = producer.get("medias") if isinstance(producer, dict) else None
            if not isinstance(medias, list):
                continue
            for media in medias:
                if codec := _ffmpeg_codec_from_go2rtc_media(str(media)):
                    return codec

    return None


def _resolve_talkback_ffmpeg_codec(
    alias: str,
    timeout: float = 2.0,
    attempts: int = 3,
    retry_delay: float = 0.35,
) -> str | None:
    for attempt in range(max(attempts, 1)):
        if codec := _talkback_ffmpeg_codec(alias, timeout=timeout):
            return codec
        if attempt == 0:
            preload_native_stream(alias, timeout=timeout)
        if attempt + 1 < max(attempts, 1):
            time.sleep(retry_delay)
    return None


def _talkback_temp_dir() -> Path:
    config_dir = Path("/config")
    if config_dir.is_dir() and os.access(config_dir, os.W_OK):
        return config_dir
    return Path(tempfile.gettempdir())


def _cleanup_stale_talkback_files(max_age_seconds: float = 600.0) -> None:
    cutoff = time.time() - max_age_seconds
    tmp_dir = _talkback_temp_dir()
    for path in tmp_dir.glob("wyze-talkback-*"):
        with suppress(OSError):
            if path.is_file() and path.stat().st_mtime < cutoff:
                path.unlink()


def send_native_talkback(payload: dict[str, Any], alias: str, timeout: float = 20.0) -> dict[str, Any]:
    action = str(payload.get("action") or "").strip().lower()
    text = str(payload.get("text") or payload.get("message") or "").strip()
    audio_b64 = payload.get("audio_b64")
    audio_url = str(payload.get("audio_url") or "").strip()

    if action == "stop":
        return _go2rtc_stream_request(alias, "", mode="stop", timeout=timeout)

    if text and (audio_b64 or audio_url):
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "Provide either text, audio_b64, or audio_url, not multiple talkback sources",
        }

    if audio_b64 and audio_url:
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "Provide either audio_b64 or audio_url, not both",
        }

    if not (text or audio_b64 or audio_url):
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "Talkback payload requires text, audio_b64, or audio_url",
        }

    if text:
        talkback_codec = _resolve_talkback_ffmpeg_codec(alias)
        if not talkback_codec:
            return {
                "status": "error",
                "source": "go2rtc",
                "alias": alias,
                "response": "Unable to determine a compatible go2rtc talkback codec",
            }
        src = f"ffmpeg:tts?{urlencode({'text': text})}#audio={talkback_codec}"
        voice = str(payload.get("voice") or "").strip()
        if voice:
            src = f"ffmpeg:tts?{urlencode({'text': text, 'voice': voice})}#audio={talkback_codec}"
        return _go2rtc_stream_request(alias, src, mode="text", timeout=timeout)

    if audio_url:
        talkback_codec = _resolve_talkback_ffmpeg_codec(alias)
        if not talkback_codec:
            return {
                "status": "error",
                "source": "go2rtc",
                "alias": alias,
                "response": "Unable to determine a compatible go2rtc talkback codec",
            }
        return _go2rtc_stream_request(
            alias,
            f"ffmpeg:{audio_url}#audio={talkback_codec}#input=file",
            mode="url",
            timeout=timeout,
        )

    try:
        audio_bytes = base64.b64decode(str(audio_b64), validate=True)
    except ValueError:
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "audio_b64 must be valid base64",
        }

    if not audio_bytes:
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "audio_b64 decoded to an empty payload",
        }

    talkback_codec = _resolve_talkback_ffmpeg_codec(alias)
    if not talkback_codec:
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": "Unable to determine a compatible go2rtc talkback codec",
        }

    suffix = str(payload.get("file_ext") or payload.get("format") or "wav").strip().lower()
    suffix = "".join(ch for ch in suffix if ch.isalnum()) or "wav"
    try:
        _cleanup_stale_talkback_files()
        with tempfile.NamedTemporaryFile(
            delete=False,
            dir=_talkback_temp_dir(),
            prefix="wyze-talkback-",
            suffix=f".{suffix}",
        ) as handle:
            handle.write(audio_bytes)
        return _go2rtc_stream_request(
            alias,
            f"ffmpeg:{handle.name}#audio={talkback_codec}#input=file",
            mode="file",
            timeout=timeout,
        )
    except OSError as ex:
        return {
            "status": "error",
            "source": "go2rtc",
            "alias": alias,
            "response": f"Unable to stage talkback audio: {ex}",
        }
