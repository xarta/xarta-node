"""Server-side alarm clock settings and SSE alarm events."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter
from pydantic import BaseModel, Field

from .db import get_conn, get_setting, set_setting
from .events import AppEvent
from .routes_events import publish_event

log = logging.getLogger(__name__)

router = APIRouter(prefix="/alarms", tags=["alarms"])

_SERVER_SETTINGS_KEY = "alarm.server.settings.v1"
_SCHEDULER_POLL_SECONDS = 2.0
_SLOT_COUNT = 5
_ALARM_LOOP_MAX_SECONDS = 120
_DAYS = set(range(7))


class AlarmSettingsBody(BaseModel):
    settings: dict[str, Any] = Field(default_factory=dict)


class AlarmCommandBody(BaseModel):
    action: str = "dismiss"
    scope: str = "local"
    slot_id: str | None = None
    cycle_id: str | None = None
    snooze_minutes: int | None = Field(default=None, ge=1, le=120)
    settings: dict[str, Any] | None = None
    slot: dict[str, Any] | None = None
    sleep: dict[str, Any] | None = None
    target_browser_id: str | None = None
    target_tab_id: str | None = None
    command_id: str | None = None
    max_age_seconds: int = Field(default=120, ge=5, le=3600)


def _clean_text(value: Any, fallback: str = "", maximum: int = 300) -> str:
    text = str(value or fallback).strip()
    return text[:maximum]


def _clean_bool(value: Any, fallback: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
    return fallback


def _clean_int(value: Any, fallback: int, minimum: int, maximum: int, step: int = 1) -> int:
    try:
        parsed = int(float(value))
    except (TypeError, ValueError):
        parsed = fallback
    parsed = max(minimum, min(maximum, parsed))
    if step > 1:
        parsed = round(parsed / step) * step
    return max(minimum, min(maximum, parsed))


def _clean_float(value: Any, fallback: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = fallback
    return max(minimum, min(maximum, parsed))


def _clean_days(value: Any) -> list[int]:
    if not isinstance(value, list):
        return list(range(7))
    days = []
    for item in value:
        try:
            day = int(item)
        except (TypeError, ValueError):
            continue
        if day in _DAYS and day not in days:
            days.append(day)
    return sorted(days) if days else list(range(7))


def _clean_time(value: Any) -> str:
    text = _clean_text(value, "07:00", 16)
    parts = text.split(":")
    if len(parts) < 2:
        return "07:00"
    hour = _clean_int(parts[0], 7, 0, 23)
    minute = _clean_int(parts[1], 0, 0, 59)
    return f"{hour:02d}:{minute:02d}"


def _clean_asset_path(value: Any) -> str:
    text = _clean_text(value, "", 512).replace("\\", "/").lstrip("/")
    if ".." in text.split("/"):
        return ""
    if text.startswith("fallback-ui/assets/"):
        text = text[len("fallback-ui/assets/") :]
    if text.startswith("assets/"):
        text = text[len("assets/") :]
    return text


def _default_slot(index: int) -> dict[str, Any]:
    return {
        "slot_id": f"server-{index}",
        "enabled": False,
        "time": "07:00",
        "description": f"Server alarm {index}",
        "days": list(range(7)),
        "recurring": True,
        "sound_asset_path": "",
        "fade_seconds": 0,
        "volume": 0.8,
        "loop_seconds": 30,
        "snooze_enabled": True,
        "snooze_minutes": 9,
        "tts_message": "",
        "tts_repeat_seconds": 20,
        "last_fired_cycle": "",
    }


def _clean_slot(value: Any, index: int) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    default = _default_slot(index)
    return {
        "slot_id": f"server-{index}",
        "enabled": _clean_bool(raw.get("enabled"), default["enabled"]),
        "time": _clean_time(raw.get("time", default["time"])),
        "description": _clean_text(raw.get("description"), default["description"], 120),
        "days": _clean_days(raw.get("days")),
        "recurring": _clean_bool(raw.get("recurring"), default["recurring"]),
        "sound_asset_path": _clean_asset_path(raw.get("sound_asset_path")),
        "fade_seconds": _clean_int(raw.get("fade_seconds"), 0, 0, 300, 5),
        "volume": _clean_float(raw.get("volume"), 0.8, 0.0, 1.0),
        "loop_seconds": _clean_int(raw.get("loop_seconds"), 30, 5, _ALARM_LOOP_MAX_SECONDS, 5),
        "snooze_enabled": _clean_bool(raw.get("snooze_enabled"), True),
        "snooze_minutes": _clean_int(raw.get("snooze_minutes"), 9, 1, 60),
        "tts_message": _clean_text(raw.get("tts_message"), "", 500),
        "tts_repeat_seconds": _clean_int(raw.get("tts_repeat_seconds"), 20, 5, 300, 5),
        "last_fired_cycle": _clean_text(raw.get("last_fired_cycle"), "", 80),
    }


def _clean_timezone(value: Any) -> str:
    timezone = _clean_text(value, "UTC", 80) or "UTC"
    try:
        ZoneInfo(timezone)
        return timezone
    except ZoneInfoNotFoundError:
        return "UTC"


def default_server_alarm_settings() -> dict[str, Any]:
    return {
        "schema": "xarta.alarm_clock.server.v1",
        "timezone": "UTC",
        "slots": [_default_slot(i) for i in range(1, _SLOT_COUNT + 1)],
        "updated_at": time.time(),
    }


def clean_server_alarm_settings(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    slots_raw = raw.get("slots") if isinstance(raw.get("slots"), list) else []
    slots = []
    for index in range(1, _SLOT_COUNT + 1):
        slots.append(_clean_slot(slots_raw[index - 1] if index - 1 < len(slots_raw) else {}, index))
    return {
        "schema": "xarta.alarm_clock.server.v1",
        "timezone": _clean_timezone(raw.get("timezone")),
        "slots": slots,
        "updated_at": float(raw.get("updated_at") or time.time()),
    }


def load_server_alarm_settings() -> dict[str, Any]:
    with get_conn() as conn:
        raw = get_setting(conn, _SERVER_SETTINGS_KEY, "")
    if not raw:
        return default_server_alarm_settings()
    try:
        return clean_server_alarm_settings(json.loads(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default_server_alarm_settings()


def save_server_alarm_settings(settings: dict[str, Any]) -> dict[str, Any]:
    clean = clean_server_alarm_settings(settings)
    clean["updated_at"] = time.time()
    with get_conn() as conn:
        set_setting(
            conn,
            _SERVER_SETTINGS_KEY,
            json.dumps(clean, sort_keys=True),
            "Node-local server-side alarm clock settings",
        )
    return clean


def _now_for_settings(settings: dict[str, Any]) -> datetime:
    timezone = _clean_timezone(settings.get("timezone"))
    return datetime.now(ZoneInfo(timezone))


def _cycle_id(slot: dict[str, Any], now: datetime) -> str:
    return f"{slot['slot_id']}:{now.date().isoformat()}T{slot['time']}"


def _slot_due(slot: dict[str, Any], now: datetime) -> str | None:
    if not slot.get("enabled"):
        return None
    if now.strftime("%H:%M") != slot.get("time"):
        return None
    day = (now.weekday() + 1) % 7
    if day not in set(slot.get("days") or list(range(7))):
        return None
    cycle_id = _cycle_id(slot, now)
    if slot.get("last_fired_cycle") == cycle_id:
        return None
    return cycle_id


async def _publish_ring(slot: dict[str, Any], cycle_id: str, settings: dict[str, Any]) -> None:
    event = AppEvent.create(
        "alarm.ring",
        "Alarm",
        slot.get("description") or "Alarm",
        severity="warn",
        source="blueprints-alarms",
        payload={
            "schema": "xarta.alarm.ring.v1",
            "scope": "server",
            "slot": slot,
            "slot_id": slot.get("slot_id"),
            "cycle_id": cycle_id,
            "timezone": settings.get("timezone") or "UTC",
            "created_at": time.time(),
            "max_age_seconds": 600,
        },
        event_id=f"alarm-ring-{cycle_id}",
    )
    await publish_event(event)


async def run_alarm_scheduler() -> None:
    """Poll server alarm settings and publish SSE ring events when due."""
    while True:
        try:
            settings = load_server_alarm_settings()
            now = _now_for_settings(settings)
            dirty = False
            for slot in settings.get("slots", []):
                cycle_id = _slot_due(slot, now)
                if not cycle_id:
                    continue
                slot["last_fired_cycle"] = cycle_id
                if not slot.get("recurring"):
                    slot["enabled"] = False
                dirty = True
                await _publish_ring(dict(slot), cycle_id, settings)
            if dirty:
                save_server_alarm_settings(settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("alarm scheduler tick failed")
        await asyncio.sleep(_SCHEDULER_POLL_SECONDS)


@router.get("/server-settings")
async def get_server_settings() -> dict[str, Any]:
    return {"ok": True, "settings": load_server_alarm_settings()}


@router.put("/server-settings")
async def put_server_settings(body: AlarmSettingsBody) -> dict[str, Any]:
    return {"ok": True, "settings": save_server_alarm_settings(body.settings)}


@router.post("/command")
async def alarm_command(body: AlarmCommandBody) -> dict[str, Any]:
    action = _clean_text(body.action, "dismiss", 80).lower().replace("-", "_").replace(" ", "_")
    if action not in {
        "dismiss",
        "snooze",
        "open_settings",
        "reset_connectivity_dismissal",
        "update_local_settings",
        "update_local_slot",
        "update_sleep",
    }:
        return {"ok": False, "detail": f"Unsupported alarm command: {action}"}
    command_id = _clean_text(body.command_id, "", 120) or uuid.uuid4().hex
    payload: dict[str, Any] = {
        "schema": "xarta.alarm.control.v1",
        "command_id": command_id,
        "action": action,
        "scope": _clean_text(body.scope, "local", 30).lower(),
        "slot_id": _clean_text(body.slot_id, "", 80),
        "cycle_id": _clean_text(body.cycle_id, "", 120),
        "snooze_minutes": body.snooze_minutes,
        "target_browser_id": _clean_text(body.target_browser_id, "", 120),
        "target_tab_id": _clean_text(body.target_tab_id, "", 120),
        "created_at": time.time(),
        "max_age_seconds": int(body.max_age_seconds),
    }
    if body.settings is not None:
        payload["settings"] = body.settings
    if body.slot is not None:
        payload["slot"] = body.slot
    if body.sleep is not None:
        payload["sleep"] = body.sleep
    event = AppEvent.create(
        "alarm.control",
        "Alarm Control",
        f"Alarm command {action}.",
        severity="info",
        source="blueprints-alarms",
        payload=payload,
        event_id=f"alarm-control-{command_id}",
    )
    published = await publish_event(event)
    return {"ok": True, "payload": payload, "event": published.model_dump()}
