"""routes_litellm_hook.py — Blueprints proxy for the xarta LiteLLM model-change hook API.

Routes
------
POST /api/v1/litellm-hook/sync-now
    Run the LiteLLM reconcile path immediately. This queries the current running-models
    feed, updates the node-local alias config, runs the secondary LiteLLM sync path,
    reloads what is needed, and returns the combined summary.

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
_SYNC_HELPER = Path(
    "/root/xarta-node/.xarta/.claude/skills/litellm-local-hook-sync/scripts/xarta-litellm-hook.sh"
)
_SYNC_TIMEOUT = 300


# ── Helpers ───────────────────────────────────────────────────────────────────


def _hook_bases() -> list[str]:
    """Return the ordered list of hook API base URLs from env.  Never falls back to
    hardcoded addresses — the caller must handle the empty-list case explicitly."""
    raw = (
        os.environ.get("XARTA_HOOK_API_BASES") or os.environ.get("XARTA_HOOK_API_BASE") or ""
    )
    return [b.strip() for b in raw.split(",") if b.strip()]


def _secondary_sync_target_key() -> str:
    """Return the API key name used for the secondary sync surface."""
    raw = (os.environ.get("LITELLM_SYNC_SECONDARY_TARGET_KEY") or "").strip()
    key = raw or "secondary_surface"
    # Keep the response key conservative and JSON-friendly.
    safe = "".join(ch if (ch.isalnum() or ch == "_") else "_" for ch in key)
    return safe.strip("_") or "secondary_surface"


def _secondary_sync_target_label() -> str:
    """Return the operator-facing label for the secondary sync surface."""
    raw = (os.environ.get("LITELLM_SYNC_SECONDARY_TARGET_LABEL") or "").strip()
    return raw or "secondary isolated LiteLLM surface"


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


def _parse_json_tail(text: str) -> dict[str, Any]:
    """Extract the final JSON object from helper stdout if present."""
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    starts = [idx for idx, ch in enumerate(raw) if ch == "{"]
    for idx in starts:
        try:
            return json.loads(raw[idx:])
        except json.JSONDecodeError:
            continue
    return {}


# ── Routes ────────────────────────────────────────────────────────────────────


@router.post("/sync-now")
async def sync_now_route() -> JSONResponse:
    """Force an immediate node-local LiteLLM alias reconcile run.

    This is the same operator action exposed by the AI Providers page button.
    It runs the existing hook-sync helper in apply mode so the latest upstream
    running-models feed is queried and the local LiteLLM config is updated if
    the live model set has changed.
    """
    if not _SYNC_HELPER.is_file():
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "LiteLLM sync helper is not available on this node.",
            },
        )

    bases = _hook_bases()
    if not bases:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "XARTA_HOOK_API_BASES is not configured in the node .env file.",
            },
        )

    try:
        result = subprocess.run(
            ["bash", str(_SYNC_HELPER), "sync-now", "--apply"],
            capture_output=True,
            text=True,
            timeout=_SYNC_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return JSONResponse(
            status_code=504,
            content={
                "ok": False,
                "error": "LiteLLM sync-now timed out before it finished.",
            },
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("sync-now launch failed: %s", exc)
        return JSONResponse(
            status_code=500,
            content={
                "ok": False,
                "error": f"LiteLLM sync-now failed to start: {exc}",
            },
        )

    summary = _parse_json_tail(result.stdout)
    ok = result.returncode == 0 and summary.get("ok", True)
    message = summary.get("message") or (
        "Local LiteLLM aliases reconciled." if ok else "LiteLLM sync-now reported a failure."
    )

    secondary_key = _secondary_sync_target_key()
    secondary_label = _secondary_sync_target_label()

    secondary_target = summary.get(secondary_key)
    if not isinstance(secondary_target, dict):
        secondary_target = summary.get("secondary")

    third_surface_target = summary.get("third_surface")
    if not isinstance(third_surface_target, dict):
        remote_target = summary.get("remote")
        if isinstance(remote_target, dict):
            third_surface_target = remote_target

    if not isinstance(third_surface_target, dict):
        for key, value in summary.items():
            if key in {
                "ok",
                "hook_base",
                "mode_id",
                "timestamp",
                "running_model_count",
                "selected",
                "spec_path",
                "running_models_path",
                "alias_matrix_path",
                "check",
                "applied",
                "reloaded",
                "message",
                "verify",
                "alias_smoke",
                "secondary",
                secondary_key,
                "remote",
                "third_surface",
                "backup_path",
                "reload_stdout",
                "reload_stderr",
                "apply",
                "apply_stdout",
                "apply_stderr",
            }:
                continue
            if isinstance(value, dict) and (
                "ok" in value or "returncode" in value or "summary" in value
            ):
                third_surface_target = value
                break

    related_targets: dict[str, Any] = {
        "local": {
            "ok": bool(summary.get("verify", {}).get("ok") and summary.get("alias_smoke", {}).get("ok")),
            "changed": bool(summary.get("applied") or summary.get("reloaded")),
            "message": summary.get("message") or "",
        },
        secondary_key: secondary_target,
        "secondary": secondary_target,
        "third_surface": third_surface_target,
    }

    payload: dict[str, Any] = {
        "ok": ok,
        "message": message,
        "returncode": result.returncode,
        "summary": summary,
        "sync_targets": related_targets,
        "secondary_target_key": secondary_key,
        "secondary_target_label": secondary_label,
        "hook_bases": bases,
        "triggered_at": time.time(),
    }
    if not ok:
        payload["error"] = summary.get("message") or (result.stderr or result.stdout or "sync-now failed").strip()
        payload["stderr"] = (result.stderr or "").strip()[-4000:]
        payload["stdout"] = (result.stdout or "").strip()[-4000:]

    return JSONResponse(status_code=200 if ok else 500, content=payload)


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
