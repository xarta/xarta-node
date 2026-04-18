"""routes_litellm_hook.py — Blueprints proxy for the xarta LiteLLM model-change hook API.

Routes
------
POST /api/v1/litellm-hook/trigger-test
    Fire a synthetic model.changed event via the xarta hook API trigger endpoint.
    The hook API broadcasts to all registered subscribers; if this node's listener
    service is running and subscribed, it will run sync-now and post Blueprints events.
    Used by the self-diagnostic page to run a full end-to-end event stream test.

GET  /api/v1/litellm-hook/status
    Return hook listener service status and the most recent hook-related Blueprints events.

Configuration
-------------
Set XARTA_HOOK_API_BASES (comma-separated) in the node .env file.
The route returns a 503 with a clear message if the env var is absent.
No default IP addresses are baked into this public file.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .db import get_conn

log = logging.getLogger(__name__)

router = APIRouter(prefix="/litellm-hook", tags=["litellm-hook"])

_CONNECT_TIMEOUT = 10.0
_LISTENER_SERVICE = "xarta-litellm-model-hook.service"
_RUNTIME_DIR = Path("/tmp/xarta-litellm-model-hook")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _hook_bases() -> list[str]:
    """Return the ordered list of hook API base URLs from env.  Never falls back to
    hardcoded addresses — the caller must handle the empty-list case explicitly."""
    raw = (
        os.environ.get("XARTA_HOOK_API_BASES") or os.environ.get("XARTA_HOOK_API_BASE") or ""
    )
    return [b.strip() for b in raw.split(",") if b.strip()]


def _service_status() -> dict[str, Any]:
    """Return basic systemd service status for the hook listener."""
    try:
        result = subprocess.run(
            ["systemctl", "is-active", _LISTENER_SERVICE],
            capture_output=True,
            text=True,
            timeout=5,
        )
        active = result.stdout.strip() == "active"
        return {"active": active, "raw": result.stdout.strip()}
    except Exception as exc:  # noqa: BLE001
        return {"active": False, "raw": "unknown", "error": str(exc)}


def _last_hook_events(limit: int = 5) -> list[dict[str, Any]]:
    """Return recent hook-related Blueprints events from the events table."""
    try:
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT event_id, event_type, severity, title, message, source, created_at, payload_json
                FROM events
                WHERE source = 'litellm-hook-sync'
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "event_id": r["event_id"],
                "event_type": r["event_type"],
                "severity": r["severity"],
                "title": r["title"],
                "message": r["message"],
                "source": r["source"],
                "created_at": float(r["created_at"]),
                "payload": json.loads(r["payload_json"] or "{}"),
            }
            for r in rows
        ]
    except Exception as exc:  # noqa: BLE001
        log.warning("events DB query failed: %s", exc)
        return []


def _last_runtime_json(filename: str) -> dict[str, Any]:
    """Read a runtime JSON file written by the hook listener."""
    path = _RUNTIME_DIR / filename
    try:
        if path.is_file():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.debug("runtime json read failed %s: %s", filename, exc)
    return {}


# ── Routes ────────────────────────────────────────────────────────────────────


@router.post("/trigger-test")
async def trigger_test() -> JSONResponse:
    """Fire a synthetic model.changed event to all hook subscribers.

    Uses POST /hooks/trigger on the first reachable hook API base.
    Subscribers (including this node's listener service if running) receive
    the event and will run sync-now, posting Blueprints SSE events in turn.

    The browser can listen on the 'blueprints:event' DOM CustomEvent for
    confirmation that the full pipeline delivered the event.
    """
    bases = _hook_bases()
    if not bases:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "XARTA_HOOK_API_BASES is not configured in the node .env file.",
            },
        )

    last_error: str | None = None
    for base in bases:
        try:
            async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
                resp = await client.post(f"{base}/hooks/trigger", json={})
                resp.raise_for_status()
                return JSONResponse(
                    content={
                        "ok": True,
                        "hook_base": base,
                        "response": resp.json(),
                        "triggered_at": time.time(),
                    }
                )
        except httpx.HTTPStatusError as exc:
            last_error = f"{base}: HTTP {exc.response.status_code}"
            log.warning("trigger-test HTTP error on %s: %s", base, exc)
        except httpx.RequestError as exc:
            last_error = f"{base}: {exc}"
            log.warning("trigger-test connection error on %s: %s", base, exc)

    return JSONResponse(
        status_code=502,
        content={
            "ok": False,
            "error": f"All hook bases unreachable. Last error: {last_error}",
        },
    )


@router.get("/status")
async def hook_status() -> dict[str, Any]:
    """Return current hook listener service status and recent hook events."""
    service = _service_status()
    recent_events = _last_hook_events(limit=5)
    last_event_data = _last_runtime_json("last-event.json")
    subscription_data = _last_runtime_json("subscription.json")

    last_event_ts: float | None = None
    if recent_events:
        last_event_ts = recent_events[0].get("created_at")

    return {
        "service": {
            "name": _LISTENER_SERVICE,
            "active": service.get("active", False),
            "status": service.get("raw", "unknown"),
        },
        "last_hook_event": last_event_data,
        "subscription": subscription_data.get("response") or subscription_data,
        "recent_blueprints_events": recent_events,
        "last_event_ts": last_event_ts,
    }
