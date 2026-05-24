"""
Wyze lock manager.

Discovers DX_LB2 (Lock Bolt 2), DX_PVLOC (Palm Lock), and future DX_-family
locks from the Wyze homepage object list. Polls each on an interval, publishes
state and battery to MQTT, emits Home Assistant MQTT auto-discovery so the
locks appear as native HA `lock` entities, and listens on a command topic to
dispatch lock/unlock.

Important: the IoT3 run-action endpoint returns `code='1' msg='SUCCESS'` as
soon as the cloud queues the request. That is NOT confirmation that the
deadbolt physically moved — a jam, dead battery, or out-of-range Wi-Fi can
all leave the device wedged with the cloud none the wiser. The only ground
truth is `lock::lock-status` polled afterward, so this module:
  - publishes `unknown` (HA "jammed" hint) when an action is accepted but
    state doesn't flip within the verification window
  - never advances optimistic state on the MQTT topic
"""

from __future__ import annotations

import contextlib
import json
import time
from threading import Event, Thread
from typing import Any, Callable, Optional

from requests import RequestException

from wyzebridge import iot3_client
from wyzebridge.bridge_utils import clean_cam_name, env_bool
from wyzebridge.build_config import VERSION
from wyzebridge.config import (
    LOCK_OPTIONS,
    LOCK_POLL_INTERVAL,
    LOCK_VERIFY_TIMEOUT,
    LOCKS_ENABLED,
    MQTT_DISCOVERY,
    MQTT_ENABLED,
    MQTT_TOPIC,
)
from wyzebridge.logging import logger
from wyzebridge.mqtt import mqtt_sub_topic, publish_messages, publish_topic
from wyzecam.api import get_homepage_object_list
from wyzecam.api_models import WyzeCredential

# Models we know are IoT3 locks. Detection also matches any `DX_*` prefix so
# new locks Wyze ships in this family work without code changes.
KNOWN_LOCK_MODELS: frozenset[str] = frozenset({"DX_LB2", "DX_PVLOC"})

# Lock Bolt 2 has the full property set; Palm Lock lacks door-status and
# power-source. We always request the full list — missing props just come
# back absent.
LOCK_PROPS: list[str] = [
    "lock::lock-status",
    "lock::door-status",
    "iot-device::iot-state",
    "battery::battery-level",
    "battery::power-source",
    "device-info::firmware-ver",
]

# MQTT payload constants the HA `lock` platform expects.
STATE_LOCKED = "LOCKED"
STATE_UNLOCKED = "UNLOCKED"
STATE_UNKNOWN = "UNKNOWN"
STATE_UNAVAILABLE = "UNAVAILABLE"
CMD_LOCK = "LOCK"
CMD_UNLOCK = "UNLOCK"


def is_lock(product_model: str) -> bool:
    return product_model in KNOWN_LOCK_MODELS or product_model.startswith("DX_")


class WyzeLock:
    """Lightweight record of one lock plus its last-known state."""

    __slots__ = ("mac", "model", "nickname", "slug", "options", "props", "last_seen_ts", "last_action_ts")

    def __init__(self, mac: str, model: str, nickname: str, options: dict[str, Any]) -> None:
        self.mac = mac
        self.model = model
        self.nickname = nickname
        self.slug = clean_cam_name(nickname).lower() or mac.lower()
        self.options = options
        self.props: dict[str, Any] = {}
        self.last_seen_ts: float = 0.0
        self.last_action_ts: float = 0.0

    @property
    def has_door_sensor(self) -> bool:
        return self.model == "DX_LB2"

    @property
    def lock_state(self) -> str:
        if not self.props or not self.props.get("iot-device::iot-state", True):
            return STATE_UNAVAILABLE
        status = self.props.get("lock::lock-status")
        if status is True:
            return STATE_LOCKED
        if status is False:
            return STATE_UNLOCKED
        return STATE_UNKNOWN

    @property
    def battery_level(self) -> Optional[int]:
        val = self.props.get("battery::battery-level")
        return int(val) if isinstance(val, (int, float)) else None


class WyzeLockManager(Thread):
    """Background thread that polls locks and bridges them to MQTT."""

    def __init__(self, auth_provider: Callable[[], Optional[WyzeCredential]], username_provider: Callable[[], str]) -> None:
        super().__init__(daemon=True, name="WyzeLockManager")
        self._auth_provider = auth_provider
        self._username_provider = username_provider
        self._stop = Event()
        self._wake = Event()
        self.locks: dict[str, WyzeLock] = {}
        self._mqtt_client = None

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def kick(self) -> None:
        """Force an immediate poll (used after a command)."""
        self._wake.set()

    def run(self) -> None:
        if not LOCKS_ENABLED:
            logger.info("[LOCKS] disabled (LOCKS_ENABLED not set) — exiting manager")
            return
        if not MQTT_ENABLED:
            logger.warning("[LOCKS] MQTT is not configured; lock state will only appear in logs")

        self._discover_with_retry()
        if not self.locks:
            logger.info("[LOCKS] no locks found on this account; manager idle")
            return

        self._publish_discovery()
        self._subscribe_commands()

        while not self._stop.is_set():
            self._poll_all()
            self._wake.wait(LOCK_POLL_INTERVAL)
            self._wake.clear()

        logger.info("[LOCKS] manager stopped")

    def _discover_with_retry(self, attempts: int = 3) -> None:
        for attempt in range(1, attempts + 1):
            auth = self._auth_provider()
            if not auth:
                logger.warning(f"[LOCKS] discovery attempt {attempt}/{attempts}: no auth yet")
                if self._stop.wait(10):
                    return
                continue
            try:
                self._discover(auth)
                return
            except RequestException as ex:
                logger.error(f"[LOCKS] discovery attempt {attempt}/{attempts} failed: [{type(ex).__name__}] {ex}")
                if self._stop.wait(min(2 ** attempt, 30)):
                    return

    def _discover(self, auth: WyzeCredential) -> None:
        data = get_homepage_object_list(auth)
        candidates = []
        for raw in data.get("device_list", []):
            model = raw.get("product_model") or ""
            mac = raw.get("mac") or ""
            nickname = raw.get("nickname") or mac
            if not is_lock(model):
                continue
            opts = _per_lock_options(nickname, mac)
            if opts.get("disabled"):
                logger.info(f"[LOCKS] skipping {nickname!r} — disabled via LOCK_OPTIONS")
                continue
            candidates.append(WyzeLock(mac=mac, model=model, nickname=nickname, options=opts))

        for lock in candidates:
            self.locks[lock.mac] = lock
            logger.info(f"[LOCKS] discovered {lock.nickname!r} ({lock.model}) → slug={lock.slug!r}")

    def _phone_id(self, auth: WyzeCredential) -> str:
        return auth.phone_id or "wyze-bridge-locks"

    def _poll_all(self) -> None:
        auth = self._auth_provider()
        if not auth:
            return
        phone_id = self._phone_id(auth)
        for lock in self.locks.values():
            self._poll_one(lock, auth.access_token, phone_id)

    def _poll_one(self, lock: WyzeLock, token: str, phone_id: str) -> None:
        try:
            resp = iot3_client.get_property(lock.mac, lock.model, LOCK_PROPS, token, phone_id)
        except RequestException as ex:
            logger.warning(f"[LOCKS] {lock.nickname!r} get-property failed: [{type(ex).__name__}] {ex}")
            return
        if resp.get("code") != "1":
            logger.warning(f"[LOCKS] {lock.nickname!r} get-property non-success: {resp}")
            return
        new_props = (resp.get("data") or {}).get("props") or {}
        if new_props == lock.props:
            return
        old_state = lock.lock_state
        old_battery = lock.battery_level
        lock.props = new_props
        lock.last_seen_ts = time.time()
        new_state = lock.lock_state
        new_battery = lock.battery_level
        if new_state != old_state:
            logger.info(f"[LOCKS] {lock.nickname!r} state: {old_state} → {new_state}")
        self._publish_state(lock)
        if new_battery != old_battery and new_battery is not None:
            logger.debug(f"[LOCKS] {lock.nickname!r} battery: {old_battery}% → {new_battery}%")

    def _publish_state(self, lock: WyzeLock) -> None:
        base = f"locks/{lock.slug}"
        msgs: list[tuple[str, Any, bool]] = [
            (f"{base}/state", lock.lock_state, True),
            (f"{base}/availability", "online" if lock.props.get("iot-device::iot-state") else "offline", True),
        ]
        if (battery := lock.battery_level) is not None:
            msgs.append((f"{base}/battery", str(battery), True))
        if (fw := lock.props.get("device-info::firmware-ver")):
            msgs.append((f"{base}/firmware", str(fw), True))
        if lock.has_door_sensor and (door := lock.props.get("lock::door-status")) is not None:
            msgs.append((f"{base}/door", "CLOSED" if door else "OPEN", True))

        publish_messages([
            {"topic": f"{MQTT_TOPIC}/{topic}", "payload": payload, "qos": 0, "retain": retain}
            for topic, payload, retain in msgs
        ])

    def _subscribe_commands(self) -> None:
        topics = [f"locks/{lock.slug}/set" for lock in self.locks.values()]
        if not topics:
            return
        client = mqtt_sub_topic(topics, self._on_command)
        if client:
            # mqtt_sub_topic stores the callback as user_data but does NOT
            # wire it to on_message — same gotcha cam_control() works around.
            client.on_message = self._on_command
            self._mqtt_client = client
            logger.info(f"[LOCKS] subscribed to {len(topics)} command topic(s)")

    def _on_command(self, client, _userdata, msg) -> None:
        del client
        payload = msg.payload.decode("utf-8", errors="ignore").strip().upper()
        slug = msg.topic.rstrip("/").split("/")[-2] if msg.topic.endswith("/set") else ""
        lock = next((lk for lk in self.locks.values() if lk.slug == slug), None)
        if not lock:
            logger.warning(f"[LOCKS] command for unknown slug={slug!r} topic={msg.topic}")
            return
        if payload not in {CMD_LOCK, CMD_UNLOCK}:
            logger.warning(f"[LOCKS] {lock.nickname!r} ignoring unknown command payload={payload!r}")
            return
        self._dispatch_command(lock, payload)

    def _dispatch_command(self, lock: WyzeLock, payload: str) -> None:
        auth = self._auth_provider()
        if not auth:
            logger.error(f"[LOCKS] {lock.nickname!r} cannot send {payload}: not authenticated")
            return
        action = "lock::lock" if payload == CMD_LOCK else "lock::unlock"
        target_status = payload == CMD_LOCK
        logger.info(f"[LOCKS] {lock.nickname!r} → {action}")
        try:
            resp = iot3_client.run_action(lock.mac, lock.model, action, self._username_provider(), auth.access_token, self._phone_id(auth))
        except RequestException as ex:
            logger.error(f"[LOCKS] {lock.nickname!r} {action} request failed: [{type(ex).__name__}] {ex}")
            return
        if resp.get("code") != "1":
            logger.error(f"[LOCKS] {lock.nickname!r} {action} rejected by cloud: {resp}")
            return
        lock.last_action_ts = time.time()
        # Cloud accepted; verify the deadbolt actually moves.
        Thread(target=self._verify_action, args=(lock, target_status), daemon=True).start()

    def _verify_action(self, lock: WyzeLock, target_status: bool) -> None:
        """Poll until lock-status reflects the command, or report jammed.

        Run in a side thread so the MQTT callback returns immediately. If
        target_status is never observed within LOCK_VERIFY_TIMEOUT seconds,
        publish STATE_UNKNOWN — HA's lock platform renders that as "jammed".
        """
        auth = self._auth_provider()
        if not auth:
            return
        token = auth.access_token
        phone_id = self._phone_id(auth)
        deadline = time.monotonic() + LOCK_VERIFY_TIMEOUT
        attempt = 0
        while time.monotonic() < deadline and not self._stop.is_set():
            attempt += 1
            time.sleep(min(2 + attempt, 5))  # 3s, 4s, 5s, 5s...
            try:
                resp = iot3_client.get_property(lock.mac, lock.model, ["lock::lock-status", "iot-device::iot-state"], token, phone_id)
            except RequestException as ex:
                logger.debug(f"[LOCKS] {lock.nickname!r} verify poll {attempt} failed: [{type(ex).__name__}] {ex}")
                continue
            props = (resp.get("data") or {}).get("props") or {}
            current = props.get("lock::lock-status")
            if current is target_status:
                logger.info(f"[LOCKS] {lock.nickname!r} action verified on attempt {attempt}")
                lock.props.update(props)
                self._publish_state(lock)
                return
        logger.warning(f"[LOCKS] {lock.nickname!r} action NOT verified within {LOCK_VERIFY_TIMEOUT}s — publishing UNKNOWN (possible jam)")
        publish_topic(f"locks/{lock.slug}/state", STATE_UNKNOWN, retain=True)

    def _publish_discovery(self) -> None:
        """Emit Home Assistant MQTT auto-discovery configs for each lock."""
        if not (MQTT_ENABLED and MQTT_DISCOVERY):
            return
        msgs: list[dict[str, Any]] = []
        for lock in self.locks.values():
            base_topic = f"{MQTT_TOPIC}/locks/{lock.slug}"
            device_block = {
                "identifiers": [lock.mac],
                "name": lock.nickname,
                "manufacturer": "Wyze",
                "model": lock.model,
                "via_device": f"docker-wyze-bridge v{VERSION}",
            }
            availability = [{
                "topic": f"{base_topic}/availability",
                "payload_available": "online",
                "payload_not_available": "offline",
            }]
            common = {"device": device_block, "availability": availability}

            msgs.append({
                "topic": f"{MQTT_DISCOVERY}/lock/{lock.mac}/lock/config",
                "payload": json.dumps(common | {
                    "name": None,  # use the device name
                    "unique_id": f"wyze_lock_{lock.mac}",
                    "state_topic": f"{base_topic}/state",
                    "command_topic": f"{base_topic}/set",
                    "payload_lock": CMD_LOCK,
                    "payload_unlock": CMD_UNLOCK,
                    "state_locked": STATE_LOCKED,
                    "state_unlocked": STATE_UNLOCKED,
                    "state_jammed": STATE_UNKNOWN,
                    "optimistic": False,
                }),
                "qos": 0,
                "retain": True,
            })
            msgs.append({
                "topic": f"{MQTT_DISCOVERY}/sensor/{lock.mac}/battery/config",
                "payload": json.dumps(common | {
                    "name": "Battery",
                    "unique_id": f"wyze_lock_{lock.mac}_battery",
                    "state_topic": f"{base_topic}/battery",
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "entity_category": "diagnostic",
                }),
                "qos": 0,
                "retain": True,
            })
            if lock.has_door_sensor:
                msgs.append({
                    "topic": f"{MQTT_DISCOVERY}/binary_sensor/{lock.mac}/door/config",
                    "payload": json.dumps(common | {
                        "name": "Door",
                        "unique_id": f"wyze_lock_{lock.mac}_door",
                        "state_topic": f"{base_topic}/door",
                        "device_class": "door",
                        "payload_on": "OPEN",
                        "payload_off": "CLOSED",
                    }),
                    "qos": 0,
                    "retain": True,
                })

        publish_messages(msgs)
        logger.info(f"[LOCKS] published HA discovery for {len(self.locks)} lock(s)")


def _per_lock_options(nickname: str, mac: str) -> dict[str, Any]:
    """Resolve LOCK_OPTIONS overrides for a single lock.

    LOCK_OPTIONS is JSON: a list of {NICKNAME / MAC / DISABLED} dicts, set by
    config.py either from env or from /data/options.json (HA add-on).
    """
    if not LOCK_OPTIONS:
        return {}
    nickname_upper = nickname.upper()
    mac_upper = mac.upper()
    for entry in LOCK_OPTIONS:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("NICKNAME") or entry.get("name") or "").upper()
        entry_mac = str(entry.get("MAC") or entry.get("mac") or "").upper()
        if (name and name == nickname_upper) or (entry_mac and entry_mac == mac_upper):
            return {
                "disabled": env_bool_truthy(entry.get("DISABLED", entry.get("disabled", False))),
            }
    return {}


def env_bool_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "on"}
    return bool(value)
