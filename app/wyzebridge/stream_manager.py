import contextlib
import json
import os
import time
from subprocess import DEVNULL, PIPE, Popen, TimeoutExpired
from threading import Thread
from typing import Callable, Optional

from wyzebridge.wyze_api import WyzeApi
from wyzebridge.stream import Stream
from wyzebridge.config import IMG_PATH, IMG_TYPE, MOTION, MQTT_DISCOVERY, SNAPSHOT_TYPE
from wyzebridge.ffmpeg import rtsp_snap_cmd, wait_for_purges
from wyzebridge.go2rtc import native_alias, preload_native_stream, write_native_snapshot
from wyzebridge.logging import logger
from wyzebridge.mqtt import bridge_status, cam_control, publish_topic, update_preview
from wyzebridge.mtx_event import RtspEvent
from wyzebridge.preview_validation import preview_file_is_image
from wyzebridge.wyze_events import WyzeEvents
from wyzebridge.bridge_utils_sunset import should_take_snapshot, should_skip_snapshot


def _snapshot_decode_failed(stderr_output: bytes | None) -> bool:
    if not stderr_output:
        return False
    stderr_text = stderr_output.decode("utf-8", errors="ignore").lower()
    return any(
        marker in stderr_text
        for marker in ("error while decoding", "corrupt decoded frame", "bytestream")
    )


def _snapshot_matches_existing(temp_path: str, final_path: str) -> bool:
    if not os.path.exists(final_path):
        return False
    try:
        with open(temp_path, "rb") as temp_file, open(final_path, "rb") as final_file:
            return temp_file.read() == final_file.read()
    except OSError:
        return False


class StreamManager:
    __slots__ = (
        "api",
        "stop_flag",
        "streams",
        "rtsp_snapshots",
        "native_preloads",
        "last_snap",
        "monitor_snapshots_thread",
    )

    def __init__(self, api: WyzeApi):
        self.api: WyzeApi = api
        self.stop_flag: bool = False
        self.streams: dict[str, Stream] = {}
        self.rtsp_snapshots: dict[str, Popen] = {}
        self.native_preloads: set[str] = set()
        self.last_snap: float = 0
        self.monitor_snapshots_thread: Optional[Thread] = None

    @property
    def total(self):
        return len(self.streams)

    @property
    def active(self):
        return len([s for s in self.streams.values() if s.enabled])

    def add(self, stream: Stream) -> str:
        uri = stream.uri
        self.streams[uri] = stream
        return uri

    def get(self, uri: str) -> Optional[Stream]:
        return self.streams.get(uri)

    def get_info(self, uri: str) -> dict:
        return stream.get_info() if (stream := self.get(uri)) else {}

    def get_all_cam_info(self) -> dict:
        return {uri: s.get_info() for uri, s in list(self.streams.items())}

    def stop_all(self) -> None:
        logger.info(f"[STREAM] Stopping {self.total} stream{'s'[: self.total ^ 1]}")
        self.stop_flag = True

        for stream in self.streams.values():
            stream.stop()

        if self.monitor_snapshots_thread is not None:
            logger.info("[STREAM] Stopping monitor_snapshots thread")
            with contextlib.suppress(ValueError, AttributeError, RuntimeError):
                self.monitor_snapshots_thread.join(timeout=5)
            self.monitor_snapshots_thread = None

        wait_for_purges()

    def monitor_streams(self, mtx_health: Callable) -> None:
        self.stop_flag = False

        if MQTT_DISCOVERY:
            self.monitor_snapshots()

        mqtt = cam_control(self.streams, self.send_cmd)
        logger.info(f"🎬 {self.total} stream{'s'[: self.total ^ 1]} enabled")
        event = RtspEvent(self.streams)
        events = WyzeEvents(self.streams) if MOTION and self.streams else None

        while not self.stop_flag:
            event.read(timeout=1)
            self.snap_all(self.active_streams())
            for stream in list(self.streams.values()):
                _ = stream.motion

            if events:
                events.check_motion()

            if int(time.time()) % 15 == 0:
                mtx_health()
                bridge_status(mqtt)

        if mqtt:
            logger.info("[STREAM] Stopping mqtt loop")
            mqtt.loop_stop()
            mqtt = None

        logger.info("[STREAM] Stream monitoring stopped")

    def monitor_snapshots(self) -> None:
        def wrapped():
            logger.info("[STREAM] Starting monitor_snapshots thread")
            try:
                # emit to MQTT the current snapshots on file system
                for cam in self.streams:
                    if not self.stop_flag:
                        update_preview(cam)

                while not self.stop_flag:
                    for cam, ffmpeg in list(self.rtsp_snapshots.items()):
                        if (
                            not self.stop_flag
                            and ffmpeg is not None
                            and (returncode := ffmpeg.returncode) is not None
                        ):
                            if returncode == 0:
                                update_preview(cam)
                            # we have some response, remove from queue
                            self.remove_from_rtsp_snapshots(cam)
                    time.sleep(1)
            except Exception as e:
                logger.error(f"[STREAM] Unexpected error in monitor_snapshots: {e}")

        if self.monitor_snapshots_thread is not None:
            logger.info("[STREAM] Stopping previous monitor_snapshots thread")
            with contextlib.suppress(ValueError, AttributeError, RuntimeError):
                self.monitor_snapshots_thread.join(timeout=5)
            self.monitor_snapshots_thread = None

        self.monitor_snapshots_thread = Thread(target=wrapped, name="monitor_snapshots")
        self.monitor_snapshots_thread.daemon = True  # allow this thread to be abandoned
        self.monitor_snapshots_thread.start()

    def remove_from_rtsp_snapshots(self, cam: str):
        try:
            del self.rtsp_snapshots[cam]
        except KeyError:
            logger.warning(f"[STREAM] {cam} not found in rtsp snapshots.")
        except Exception as ex:
            logger.error(f"[STREAM] [{type(ex).__name__}] removing {cam=} {ex}.")

    def active_streams(self) -> list[str]:
        """
        Health check on all streams and return a list of enabled
        streams that are NOT battery powered.

        Returns:
        - list(str): uri-friendly name of streams that are enabled.
        """
        if self.stop_flag:
            return []
        return [cam for cam, s in list(self.streams.items()) if s.health_check() > 0]

    def enabled_streams(self) -> list[str]:
        if self.stop_flag:
            return []
        return [cam for cam, s in list(self.streams.items()) if getattr(s, "enabled", False)]

    def snap_all(self, cams: Optional[list[str]] = None, force: bool = False):
        """
        Take an rtsp snapshot of the streams in the list.

        Args:
        - cams (list[str], optional): names of the streams to take a snapshot of.
        - force (bool, optional): Ignore interval and force snapshot. Defaults to False.
        """
        if force or should_take_snapshot(SNAPSHOT_TYPE, self.last_snap):
            self.last_snap = time.time()
            snapshot_targets = cams or (
                self.enabled_streams()
                if SNAPSHOT_TYPE == "api"
                else self.active_streams()
            )
            for cam_name in snapshot_targets:
                if should_skip_snapshot(cam_name):
                    continue
                if SNAPSHOT_TYPE == "rtsp":
                    self.stop_subprocess(cam_name)
                    self.rtsp_snap_popen(cam_name, True)
                elif SNAPSHOT_TYPE == "api":
                    self.refresh_preview(cam_name)

    def get_sse_status(self) -> dict:
        return {
            uri: {"status": cam.status(), "motion": cam.motion}
            for uri, cam in list(self.streams.items())
        }

    def send_cmd(
        self, cam_name: str, cmd: str, payload: str | list | dict = ""
    ) -> dict:
        """
        Send a command directly to the camera and wait for a response.

        Parameters:
        - cam_name (str): uri-friendly name of the camera.
        - cmd (str): The camera/tutk command to send.
        - payload (str): value for the tutk command.

        Returns:
        - dictionary: Results that can be converted to JSON.
        """
        resp = {"status": "error", "command": cmd, "payload": payload}

        if cam_name == "all" and cmd == "update_snapshot":
            self.snap_all(force=True)
            return resp | {"status": "success"}

        if not (stream := self.get(cam_name)):
            return resp | {"response": "Camera not found"}

        if cam_resp := stream.send_cmd(cmd, payload):
            status = cam_resp.get("value") if cam_resp.get("status") == "success" else 0

            if isinstance(status, dict):
                status = json.dumps(status)

            if "update_snapshot" in cam_resp:
                demand_opened = not stream.connected
                snap = self.get_snapshot(cam_name)["ok"]
                if demand_opened:
                    stream.stop()

                publish_topic(f"{cam_name}/{cmd}", int(time.time()) if snap else 0)
                return dict(resp, status="success", value=snap, response=snap)

            publish_topic(f"{cam_name}/{cmd}", status)

        return cam_resp if "status" in cam_resp else resp | cam_resp

    def rtsp_snap_popen(self, cam_name: str, interval: bool = False) -> Optional[Popen]:
        if not (stream := self.get(cam_name)):
            return
        stream.start()
        ffmpeg = self.rtsp_snapshots.get(cam_name)
        if not ffmpeg or ffmpeg.poll() is not None:
            # None means inherit from parent process
            ffmpeg = Popen(rtsp_snap_cmd(cam_name, interval), stderr=None)
            self.rtsp_snapshots[cam_name] = ffmpeg
        return ffmpeg

    def get_rtsp_snap(self, cam_name: str) -> bool:
        if not (stream := self.get(cam_name)):
            return False
        stream.start()
        temp_path = f"{IMG_PATH}{cam_name}.{IMG_TYPE}.tmp"
        final_path = f"{IMG_PATH}{cam_name}.{IMG_TYPE}"
        with contextlib.suppress(FileNotFoundError):
            os.remove(temp_path)

        for skip_early_frames, snapshot_timeout in ((True, 15), (False, 30)):
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path)
            ffmpeg = Popen(
                rtsp_snap_cmd(cam_name, skip_early_frames=skip_early_frames)[:-1] + [temp_path],
                stdout=DEVNULL,
                stderr=PIPE,
            )
            timed_out = False
            try:
                _, stderr_output = ffmpeg.communicate(timeout=snapshot_timeout)
                if ffmpeg.returncode == 0 and os.path.getsize(temp_path) > 0:
                    if not preview_file_is_image(temp_path):
                        logger.warning(f"❗ [{cam_name}] Snapshot output was not a valid image; keeping previous preview")
                        return False
                    if _snapshot_decode_failed(stderr_output):
                        logger.warning(f"❗ [{cam_name}] Snapshot decode failed; keeping previous preview")
                        return False
                    if _snapshot_matches_existing(temp_path, final_path):
                        logger.warning(f"❗ [{cam_name}] Snapshot matched existing preview; treating as stale")
                        return False
                    os.replace(temp_path, final_path)
                    return True
            except TimeoutExpired:
                timed_out = True
                suffix = " without frame skip" if not skip_early_frames else ""
                logger.info(f"❗ [{cam_name}] Snapshot timed out{suffix}")
            except Exception as ex:
                logger.error(f"❗ [{cam_name}] [{type(ex).__name__}] {ex}")
            finally:
                if ffmpeg.poll() is None:
                    ffmpeg.kill()
                    ffmpeg.communicate()
                with contextlib.suppress(FileNotFoundError):
                    os.remove(temp_path)
            if not (skip_early_frames and timed_out):
                break
        return False

    def _go2rtc_snapshot(self, cam_name: str, require_selected: bool = False) -> bool:
        if not (stream := self.get(cam_name)):
            if require_selected:
                return False
            aliases = [native_alias(cam_name)]
            alternate_alias = native_alias(cam_name, substream=True)
            if alternate_alias not in aliases:
                aliases.append(alternate_alias)
            return any(write_native_snapshot(alias, cam_name) for alias in aliases)
        info = stream.get_info()
        if require_selected and not info.get("native_selected"):
            return False
        if not info.get("native_api_reachable"):
            return False
        alias = info.get("native_alias")
        if not alias:
            return False
        if alias not in self.native_preloads:
            preload = preload_native_stream(alias)
            if preload.get("ok"):
                self.native_preloads.add(alias)
        return write_native_snapshot(alias, cam_name)

    def get_snapshot(self, cam_name: str) -> dict:
        # All three paths write to IMG_PATH/{cam_name}.{IMG_TYPE} but none of
        # them publish to MQTT — that only happens via monitor_snapshots when
        # ffmpeg from snap_all's queue finishes. For on-demand callers
        # (button press, /snapshot HTTP, motion event), publish here so
        # HA's MQTT-discovery camera entity sees the fresh image.
        if self._go2rtc_snapshot(cam_name, require_selected=True):
            update_preview(cam_name)
            return {"ok": True, "source": "go2rtc"}
        if self._go2rtc_snapshot(cam_name):
            update_preview(cam_name)
            return {"ok": True, "source": "go2rtc"}
        if self.get_rtsp_snap(cam_name):
            update_preview(cam_name)
            return {"ok": True, "source": "rtsp"}
        return {"ok": False, "source": "rtsp"}

    def _restart_stream_for_snapshot(self, cam_name: str) -> bool:
        if not (stream := self.get(cam_name)):
            return False
        info = stream.get_info()
        if alias := info.get("native_alias"):
            self.native_preloads.discard(alias)
        logger.warning(f"♻️ [{cam_name}] Restarting stream after stale or failed snapshot")
        with contextlib.suppress(Exception):
            stream.stop()
        return stream.start()

    def refresh_preview(self, cam_name: str) -> dict:
        snapshot = self.get_snapshot(cam_name)
        if snapshot["ok"]:
            return snapshot
        if self._restart_stream_for_snapshot(cam_name):
            snapshot = self.get_snapshot(cam_name)
            if snapshot["ok"]:
                return snapshot | {"restarted": True}
        return {"ok": self.api.save_thumbnail(cam_name, ""), "source": "api"}

    def stop_subprocess(self, cam: str):
        ffmpeg = self.rtsp_snapshots.get(cam)

        if ffmpeg is not None:
            self.remove_from_rtsp_snapshots(cam)

            if ffmpeg.poll() is None:
                ffmpeg.kill()
                ffmpeg.communicate()
