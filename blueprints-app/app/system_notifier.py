"""Best-effort System Bridge notifier client for Blueprints-local producers."""

from __future__ import annotations

import logging
import os
import socket
from typing import Any

import httpx

log = logging.getLogger(__name__)

_LEVEL_BY_SEVERITY = {
    "debug": "debug",
    "info": "information",
    "information": "information",
    "warn": "warning",
    "warning": "warning",
    "error": "error",
    "critical": "error",
}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def notifier_primary_enabled() -> bool:
    return _truthy(os.environ.get("SYSTEM_BRIDGE_NOTIFIER_BLUEPRINTS_PRIMARY"))


def _notifier_url() -> str:
    return os.environ.get("SYSTEM_BRIDGE_NOTIFIER_URL", "").strip()


def _notifier_token() -> str:
    return os.environ.get("SYSTEM_BRIDGE_NOTIFIER_TOKEN", "").strip()


async def post_notifier_event(
    *,
    event_type: str,
    title: str,
    message: str,
    severity: str = "info",
    source_component: str = "blueprints-app",
    destinations: list[str] | None = None,
    tags: list[str] | None = None,
    data: dict[str, Any] | None = None,
    importance: str = "neutral",
    dedupe_key: str | None = None,
    recovery: bool = False,
) -> bool:
    """Submit an operator event to system-bridge-notifier.

    Returns ``False`` when notifier config is absent or submission fails. Callers
    can then use their legacy local SSE path as a migration fallback.
    """
    url = _notifier_url()
    token = _notifier_token()
    if not url or not token:
        return False

    level = _LEVEL_BY_SEVERITY.get(str(severity or "info").lower(), "information")
    payload_data = dict(data or {})
    payload_data["event_type"] = event_type
    payload_data.setdefault("importance", importance)

    body = {
        "source_node": socket.gethostname(),
        "source_component": source_component,
        "level": level,
        "title": title,
        "body": message,
        "dedupe_key": dedupe_key or event_type,
        "tags": tags or [],
        "destinations": destinations or ["matrix", "blueprints"],
        "data": payload_data,
        "recovery": bool(recovery),
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=1.5)) as client:
            response = await client.post(
                url,
                json=body,
                headers={"Authorization": f"Bearer {token}"},
            )
            response.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("system notifier post failed for %s: %s", event_type, exc)
        return False
