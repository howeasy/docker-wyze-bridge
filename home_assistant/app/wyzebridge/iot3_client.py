"""
Wyze IoT3 client.

The new Wyze lock family (DX_LB2 Lock Bolt 2, DX_PVLOC Palm Lock, and likely
future DX_ devices) does not respond to the legacy v2/device/* endpoints or
to the Ford lock service that wyze_sdk knows about. It uses a separate
"IoT3" service.

Protocol reverse-engineered by jfarmer08/wyze-api (Node.js); this is a small
Python port that the lock manager calls. Read-only and write paths confirmed
against a real Palm Lock and Lock Bolt 2.

Endpoints:
    POST https://app.wyzecam.com/app/v4/iot3/get-property
    POST https://app.wyzecam.com/app/v4/iot3/run-action

Signing:
    secret = md5(access_token + "wyze_app_secret_key_132")
    Signature2 = hmac_md5(secret, request_body).hexdigest()
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import time
from typing import Any

from wyzecam.api import get, post  # uses the bridge's SSL_VERIFY-aware wrappers

IOT3_BASE = "https://app.wyzecam.com"
IOT3_APP_INFO = "wyze_android_3.11.0.758"
IOT3_APP_VERSION = "3.11.0.758"
OLIVE_APP_ID = "9319141212m2ik"
OLIVE_SIGNING_SECRET = "wyze_app_secret_key_132"


def _signature(body: str, access_token: str) -> str:
    key = (access_token + OLIVE_SIGNING_SECRET).encode("utf-8")
    secret = hashlib.md5(key).hexdigest()
    return hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.md5).hexdigest()


def _headers(body: str, access_token: str, phone_id: str) -> dict[str, str]:
    return {
        "access_token": access_token,
        "appid": OLIVE_APP_ID,
        "appinfo": IOT3_APP_INFO,
        "appversion": IOT3_APP_VERSION,
        "env": "Prod",
        "phoneid": phone_id,
        "requestid": secrets.token_hex(16),
        "Signature2": _signature(body, access_token),
        "Content-Type": "application/json; charset=utf-8",
    }


def _extract_model(device_mac: str, device_model: str) -> str:
    """Fallback model extraction from MAC when product_model is missing.

    DX_ devices report their model as the MAC prefix (e.g.
    mac=DX_LB2_AABBCCDDEEFF -> model=DX_LB2). We only fall back if the
    caller didn't pass one.
    """
    if device_model:
        return device_model
    parts = device_mac.split("_")
    return "_".join(parts[:2]) if len(parts) >= 3 else device_mac


def _post(url_path: str, payload: dict[str, Any], access_token: str, phone_id: str) -> dict[str, Any]:
    body = json.dumps(payload, separators=(",", ":"))
    resp = post(
        IOT3_BASE + url_path,
        data=body,
        headers=_headers(body, access_token, phone_id),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def get_property(mac: str, model: str, props: list[str], access_token: str, phone_id: str) -> dict[str, Any]:
    """Read one or more properties from an IoT3 device.

    Returns the raw response dict. Properties are at
    `response["data"]["props"]` as a {prop_name: value} map.
    Caller should check `response["code"] == "1"` for success.
    """
    ts = int(time.time() * 1000)
    payload = {
        "nonce": str(ts),
        "payload": {
            "cmd": "get_property",
            "props": props,
            "tid": (ts // 7) % 89000 + 10000,
            "ts": ts,
            "ver": 1,
        },
        "targetInfo": {"id": mac, "model": _extract_model(mac, model)},
    }
    return _post("/app/v4/iot3/get-property", payload, access_token, phone_id)


def run_action(mac: str, model: str, action: str, username: str, access_token: str, phone_id: str) -> dict[str, Any]:
    """Execute an action on an IoT3 device.

    For locks: action is "lock::lock" or "lock::unlock".
    Returns the raw response dict; `code == "1"` means accepted.
    """
    ts = int(time.time() * 1000)
    payload = {
        "nonce": str(ts),
        "payload": {
            "action": action,
            "cmd": "run_action",
            "params": {
                "action_id": (ts // 11) % 90000 + 10000,
                "type": 1,
                "username": username,
            },
            "tid": (ts // 7) % 89000 + 10000,
            "ts": ts,
            "ver": 1,
        },
        "targetInfo": {"id": mac, "model": _extract_model(mac, model)},
    }
    return _post("/app/v4/iot3/run-action", payload, access_token, phone_id)
