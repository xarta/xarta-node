"""Node-local browser voice-mode lease endpoints."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .events import AppEvent
from .routes_events import publish_event

router = APIRouter(prefix="/voice-mode", tags=["voice-mode"])

_STATE_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-voice-mode.json")
_state_lock = asyncio.Lock()


class BrowserVoiceState(BaseModel):
    browser_id: str
    browser_label: str | None = None
    stt_enabled: bool = False
    tts_enabled: bool = False


def _clean_browser_id(value: str | None) -> str:
    return str(value or "").strip()[:160]


def _clean_label(value: str | None, fallback: str) -> str:
    label = str(value or "").strip()
    return (label or fallback)[:120]


def _empty_state() -> dict[str, Any]:
    return {
        "active": None,
        "revision": 0.0,
        "updated_at": 0.0,
    }


def _read_state_unlocked() -> dict[str, Any]:
    try:
        raw = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return _empty_state()
    except Exception:
        return _empty_state()
    if not isinstance(raw, dict):
        return _empty_state()
    state = _empty_state()
    state.update(raw)
    if not isinstance(state.get("active"), dict):
        state["active"] = None
    return state


def _write_state_unlocked(state: dict[str, Any]) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_STATE_PATH)


def _public_state(state: dict[str, Any]) -> dict[str, Any]:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    return {
        "ok": True,
        "active": active,
        "revision": float(state.get("revision") or 0),
        "updated_at": float(state.get("updated_at") or 0),
    }


async def _publish_changed(state: dict[str, Any], action: str) -> None:
    public = _public_state(state)
    event = AppEvent.create(
        "voice.mode.changed",
        "Voice Mode Changed",
        "Blueprints voice-mode active browser changed.",
        severity="info",
        source="blueprints-voice-mode",
        payload={
            "action": action,
            "active": public["active"],
            "revision": public["revision"],
            "updated_at": public["updated_at"],
        },
    )
    await publish_event(event)


@router.get("/status")
async def voice_mode_status() -> dict[str, Any]:
    async with _state_lock:
        return _public_state(_read_state_unlocked())


@router.post("/activate")
async def voice_mode_activate(body: BrowserVoiceState):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})
    if not body.stt_enabled and not body.tts_enabled:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": "Enable STT or TTS before activating this browser."},
        )

    now = time.time()
    active = {
        "browser_id": browser_id,
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "stt_enabled": bool(body.stt_enabled),
        "tts_enabled": bool(body.tts_enabled),
        "activated_at": now,
    }
    async with _state_lock:
        state = _read_state_unlocked()
        state["active"] = active
        state["revision"] = now
        state["updated_at"] = now
        _write_state_unlocked(state)
    await _publish_changed(state, "activate")
    return _public_state(state)


@router.post("/deactivate")
async def voice_mode_deactivate(body: BrowserVoiceState):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})

    changed = False
    async with _state_lock:
        state = _read_state_unlocked()
        active = state.get("active") if isinstance(state.get("active"), dict) else None
        if active and active.get("browser_id") == browser_id:
            now = time.time()
            state["active"] = None
            state["revision"] = now
            state["updated_at"] = now
            _write_state_unlocked(state)
            changed = True
    if changed:
        await _publish_changed(state, "deactivate")
    return _public_state(state)
