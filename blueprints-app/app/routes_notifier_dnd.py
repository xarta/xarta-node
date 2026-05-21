"""Node-local notifier/DND policy settings."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter(prefix="/notifier-dnd", tags=["notifier-dnd"])

Importance = Literal[
    "low_importance",
    "neutral",
    "urgent1",
    "urgent2",
    "danger1",
    "danger2",
]
DndMode = Literal[
    "debug",
    "default",
    "scheduled_dnd_01",
    "scheduled_dnd_02",
    "manual_dnd_1",
    "manual_dnd_2",
]

_CONFIG_PATH = Path("/xarta-node/.lone-wolf/config/system-bridge-notifier-dnd.json")
_LISTENER_TTL_SECONDS = 20.0
_listener_heartbeats: dict[str, dict[str, str | float]] = {}
_speech_claims: dict[str, dict[str, str | float]] = {}


class ScheduleWindow(BaseModel):
    enabled: bool = False
    start: str = Field(default="22:00", pattern=r"^\d{2}:\d{2}$")
    end: str = Field(default="07:00", pattern=r"^\d{2}:\d{2}$")
    mode: Literal["scheduled_dnd_01", "scheduled_dnd_02"] = "scheduled_dnd_01"


class ListenerPolicy(BaseModel):
    phone_wins: bool = True
    desktop_one_per_os_ip: bool = True
    android_listener_future: bool = True
    cloud_tts_fallback_future: bool = True


class DangerPolicy(BaseModel):
    danger2_alarm_planned: bool = True
    alarm_sound_enabled: bool = True
    alarm_sound_path: str | None = Field(default=None, max_length=512)
    danger_alarm_volume: float = Field(default=1.0, ge=0, le=1)


class NotifierDndConfig(BaseModel):
    version: int = 1
    mode: DndMode = "default"
    manual_timeout_minutes: int = Field(default=60, ge=5, le=720)
    manual_until: float | None = None
    minimum_speak_importance: Importance = "neutral"
    quiet_volume: float = Field(default=0.35, ge=0, le=1)
    normal_volume: float = Field(default=0.85, ge=0, le=1)
    debug_volume: float = Field(default=0.60, ge=0, le=1)
    schedules: list[ScheduleWindow] = Field(default_factory=list, max_length=8)
    listener_policy: ListenerPolicy = Field(default_factory=ListenerPolicy)
    danger_policy: DangerPolicy = Field(default_factory=DangerPolicy)
    notes: str = ""
    updated_at: float | None = None
    config_path: str = str(_CONFIG_PATH)


class ListenerHeartbeat(BaseModel):
    listener_id: str = Field(min_length=8, max_length=128)
    kind: Literal["phone", "desktop"] = "desktop"
    os_key: str = Field(default="unknown", max_length=64)


class ListenerHeartbeatResponse(BaseModel):
    listener_id: str
    active_phone_listeners: int
    active_desktop_listeners: int


class SpeechClaimRequest(ListenerHeartbeat):
    event_id: str = Field(default="", max_length=128)


class SpeechClaimResponse(BaseModel):
    allowed: bool
    reason: str = ""
    listener_id: str


def _default_config() -> NotifierDndConfig:
    return NotifierDndConfig(
        schedules=[
            ScheduleWindow(enabled=False, start="22:00", end="00:00", mode="scheduled_dnd_01"),
            ScheduleWindow(enabled=False, start="00:00", end="07:00", mode="scheduled_dnd_02"),
        ],
        notes=(
            "This node-local file is the planning source for notifier speech and DND policy. "
            "Danger2 alarm policy is always armed; automatic playback still needs a dedicated "
            "safety-tested notifier path."
        ),
    )


def _read_config() -> NotifierDndConfig:
    if not _CONFIG_PATH.exists():
        return _default_config()
    try:
        raw = json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))
        data = raw if isinstance(raw, dict) else {}
        data.setdefault("config_path", str(_CONFIG_PATH))
        config = NotifierDndConfig.model_validate(data)
        config.danger_policy.alarm_sound_enabled = True
        return config
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"notifier DND config unreadable: {exc}") from exc


def _write_config(config: NotifierDndConfig) -> NotifierDndConfig:
    config.updated_at = time.time()
    config.config_path = str(_CONFIG_PATH)
    config.danger_policy.alarm_sound_enabled = True
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _CONFIG_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(config.model_dump_json(indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(_CONFIG_PATH)
    try:
        _CONFIG_PATH.chmod(0o600)
    except OSError:
        pass
    return config


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "unknown"


def _prune_runtime_state(now: float | None = None) -> None:
    ts = now or time.time()
    stale_listeners = [
        listener_id
        for listener_id, heartbeat in _listener_heartbeats.items()
        if ts - float(heartbeat.get("updated_at", 0)) > _LISTENER_TTL_SECONDS
    ]
    for listener_id in stale_listeners:
        _listener_heartbeats.pop(listener_id, None)
    stale_claims = [
        claim_key
        for claim_key, claim in _speech_claims.items()
        if ts - float(claim.get("updated_at", 0)) > _LISTENER_TTL_SECONDS
    ]
    for claim_key in stale_claims:
        _speech_claims.pop(claim_key, None)


def _record_heartbeat(body: ListenerHeartbeat, request: Request) -> None:
    _prune_runtime_state()
    _listener_heartbeats[body.listener_id] = {
        "kind": body.kind,
        "os_key": body.os_key,
        "ip": _client_ip(request),
        "updated_at": time.time(),
    }


def _listener_counts() -> tuple[int, int]:
    _prune_runtime_state()
    phones = sum(1 for item in _listener_heartbeats.values() if item.get("kind") == "phone")
    desktops = sum(1 for item in _listener_heartbeats.values() if item.get("kind") == "desktop")
    return phones, desktops


@router.get("/config", response_model=NotifierDndConfig)
async def get_notifier_dnd_config() -> NotifierDndConfig:
    return _read_config()


@router.put("/config", response_model=NotifierDndConfig)
async def put_notifier_dnd_config(body: NotifierDndConfig) -> NotifierDndConfig:
    return _write_config(body)


@router.post("/listeners/heartbeat", response_model=ListenerHeartbeatResponse)
async def heartbeat_notifier_listener(
    body: ListenerHeartbeat,
    request: Request,
) -> ListenerHeartbeatResponse:
    _record_heartbeat(body, request)
    phones, desktops = _listener_counts()
    return ListenerHeartbeatResponse(
        listener_id=body.listener_id,
        active_phone_listeners=phones,
        active_desktop_listeners=desktops,
    )


@router.post("/speech-claim", response_model=SpeechClaimResponse)
async def claim_notifier_speech(
    body: SpeechClaimRequest,
    request: Request,
) -> SpeechClaimResponse:
    config = _read_config()
    _record_heartbeat(body, request)
    phone_wins = config.listener_policy.phone_wins
    desktop_dedupe = config.listener_policy.desktop_one_per_os_ip
    if body.kind != "phone" and phone_wins:
        phone_active = any(
            item.get("kind") == "phone"
            and listener_id != body.listener_id
            for listener_id, item in _listener_heartbeats.items()
        )
        if phone_active:
            return SpeechClaimResponse(
                allowed=False,
                reason="phone_listener_active",
                listener_id=body.listener_id,
            )

    if body.kind == "desktop" and desktop_dedupe:
        event_key = body.event_id or f"no-event:{int(time.time() / _LISTENER_TTL_SECONDS)}"
        claim_key = f"{event_key}:{_client_ip(request)}:{body.os_key}"
        existing = _speech_claims.get(claim_key)
        if existing and existing.get("listener_id") != body.listener_id:
            return SpeechClaimResponse(
                allowed=False,
                reason="desktop_claim_exists",
                listener_id=body.listener_id,
            )
        _speech_claims[claim_key] = {
            "listener_id": body.listener_id,
            "updated_at": time.time(),
        }

    return SpeechClaimResponse(allowed=True, listener_id=body.listener_id)
