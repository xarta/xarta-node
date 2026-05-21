"""Node-local notifier/DND policy settings."""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from .events import AppEvent
from .routes_events import publish_event
from .system_notifier import post_notifier_event

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
_DANGER2_STATE_PATH = Path("/xarta-node/.lone-wolf/state/notifier-danger2-control.json")
_NOTIFIER_STACK_DIR = Path("/xarta-node/.lone-wolf/stacks/system-bridge-notifier")
_LISTENER_TTL_SECONDS = 20.0
_listener_heartbeats: dict[str, dict[str, str | float]] = {}
_speech_claims: dict[str, dict[str, str | float]] = {}


class ScheduleWindow(BaseModel):
    enabled: bool = False
    start: str = Field(default="22:00", pattern=r"^\d{2}:\d{2}$")
    end: str = Field(default="07:00", pattern=r"^\d{2}:\d{2}$")
    mode: Literal["scheduled_dnd_01", "scheduled_dnd_02"] = "scheduled_dnd_01"


class ListenerPolicy(BaseModel):
    # Legacy config key. Enforced true: phones always speak and never suppress desktop/web.
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


class NotifierTestRequest(BaseModel):
    test_id: str = Field(min_length=3, max_length=80)
    confirmed: bool = False


class NotifierTestResponse(BaseModel):
    ok: bool
    status: str
    test_id: str
    event_id: str


class Danger2CancelRequest(BaseModel):
    reason: str = Field(default="operator_cancelled", max_length=160)
    source: str = Field(default="blueprints-browser", max_length=160)


class Danger2State(BaseModel):
    cancel_id: str = ""
    cancelled_at: float = 0.0
    reason: str = ""
    source: str = ""
    updated_at: float = 0.0


class Danger2CancelResponse(Danger2State):
    notification_submitted: bool = False


class NotifierFailureDrillRequest(BaseModel):
    confirmed: bool = False


class NotifierFailureDrillResponse(BaseModel):
    ok: bool
    status: str
    test_id: str
    event_id: str
    attempted_event_id: str
    notifier_submission_failed: bool
    warning_published: bool
    stop_ok: bool
    restart_ok: bool


_NOTIFIER_TESTS: dict[str, dict[str, Any]] = {
    "neutral": {
        "level": "information",
        "importance": "neutral",
        "event_type": "notifier.tests.neutral",
        "title": "Notifier test: neutral",
        "message": "Neutral notifier smoke test.",
        "speech": "Information. Neutral notifier smoke test.",
        "noisy": False,
    },
    "urgent1": {
        "level": "warning",
        "importance": "urgent1",
        "event_type": "notifier.tests.urgent1",
        "title": "Notifier test: urgent1",
        "message": "Urgent one notifier smoke test.",
        "speech": "Warning. Urgent one notifier smoke test.",
        "noisy": True,
    },
    "urgent2": {
        "level": "warning",
        "importance": "urgent2",
        "event_type": "notifier.tests.urgent2",
        "title": "Notifier test: urgent2",
        "message": "Urgent two notifier smoke test.",
        "speech": "Warning. Urgent two notifier smoke test.",
        "noisy": True,
    },
    "danger1": {
        "level": "error",
        "importance": "danger1",
        "event_type": "notifier.tests.danger1",
        "title": "Notifier test: danger1",
        "message": "Danger one notifier smoke test.",
        "speech": "Danger. Danger one notifier smoke test.",
        "noisy": True,
    },
    "danger2_drill": {
        "level": "error",
        "importance": "danger2",
        "event_type": "notifier.tests.danger2_drill",
        "title": "Notifier test: danger2 drill",
        "message": "Danger two drill. This is a notifier-backed alarm test.",
        "speech": "Danger two drill. This is only a notifier-backed alarm test.",
        "noisy": True,
        "requires_confirmation": True,
    },
    "unknown_warning": {
        "level": "warning",
        "importance": None,
        "event_type": "notifier.tests.generic_unknown_warning",
        "title": "Notifier test: unknown warning",
        "message": "Unknown warning notifier smoke test.",
        "speech": "Warning. Unknown warning notifier smoke test.",
        "noisy": True,
    },
    "unknown_error": {
        "level": "error",
        "importance": None,
        "event_type": "notifier.tests.generic_unknown_error",
        "title": "Notifier test: unknown error",
        "message": "Unknown error notifier smoke test.",
        "speech": "Error. Unknown error notifier smoke test.",
        "noisy": True,
    },
    "unknown_information": {
        "level": "information",
        "importance": None,
        "event_type": "notifier.tests.generic_unknown_information",
        "title": "Notifier test: unknown information",
        "message": "Unknown information notifier smoke test.",
        "speech": "Information. Unknown information notifier smoke test.",
        "noisy": False,
    },
    "unknown_debug": {
        "level": "debug",
        "importance": None,
        "event_type": "notifier.tests.generic_unknown_debug",
        "title": "Notifier test: unknown debug",
        "message": "Unknown debug notifier smoke test.",
        "speech": "Debug. Unknown debug notifier smoke test.",
        "noisy": False,
    },
}


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
        config.listener_policy.phone_wins = True
        config.danger_policy.alarm_sound_enabled = True
        return config
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"notifier DND config unreadable: {exc}") from exc


def _write_config(config: NotifierDndConfig) -> NotifierDndConfig:
    config.updated_at = time.time()
    config.config_path = str(_CONFIG_PATH)
    config.listener_policy.phone_wins = True
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


def _read_danger2_state() -> Danger2State:
    if not _DANGER2_STATE_PATH.exists():
        return Danger2State()
    try:
        raw = json.loads(_DANGER2_STATE_PATH.read_text(encoding="utf-8"))
        return Danger2State.model_validate(raw if isinstance(raw, dict) else {})
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"danger2 control state unreadable: {exc}") from exc


def _write_danger2_state(state: Danger2State) -> Danger2State:
    state.updated_at = time.time()
    _DANGER2_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _DANGER2_STATE_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(state.model_dump_json(indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(_DANGER2_STATE_PATH)
    try:
        _DANGER2_STATE_PATH.chmod(0o600)
    except OSError:
        pass
    return state


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


async def _run_notifier_stack_command(*args: str, timeout_seconds: float = 60.0) -> tuple[bool, str]:
    proc = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        *args,
        cwd=str(_NOTIFIER_STACK_DIR),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return False, "docker compose command timed out"
    output = (stdout + stderr).decode("utf-8", "replace").strip()
    return proc.returncode == 0, output[-2000:]


async def _publish_notifier_failure_warning(
    *,
    fallback_event_id: str,
    attempted_event_id: str,
    restart_ok: bool,
) -> None:
    message = (
        "System Bridge notifier failure drill. The notifier submission failed, "
        "and the fallback warning path is working."
    )
    if not restart_ok:
        message += " The notifier stack restart reported a problem."
    event = AppEvent.create(
        event_id=fallback_event_id,
        event_type="system_bridge_notifier.failure.drill",
        title="Notifier failure drill",
        message=message,
        severity="error",
        source="blueprints-notifier-tests",
        payload={
            "event_type": "system_bridge_notifier.failure.drill",
            "blueprints_event_type": "system_bridge_notifier.failure.drill",
            "importance": "danger1",
            "speech": f"Warning. {message}",
            "test_id": "notifier_failure_warning",
            "source_component": "blueprints-notifier-tests",
            "frontend_contract": "notification-tests-modal",
            "test_broadcast_speech": True,
            "notifier_failure_fallback": True,
            "attempted_notifier_event_id": attempted_event_id,
            "restart_ok": restart_ok,
        },
    )
    await publish_event(event)


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
    desktop_dedupe = config.listener_policy.desktop_one_per_os_ip
    if body.kind == "phone":
        return SpeechClaimResponse(allowed=True, listener_id=body.listener_id)

    if body.kind == "desktop" and desktop_dedupe:
        event_key = body.event_id or f"no-event:{int(time.time() / _LISTENER_TTL_SECONDS)}"
        claim_key = f"{event_key}:{_client_ip(request)}"
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


@router.post("/tests", response_model=NotifierTestResponse)
async def submit_notifier_test(body: NotifierTestRequest) -> NotifierTestResponse:
    spec = _NOTIFIER_TESTS.get(body.test_id)
    if not spec:
        raise HTTPException(status_code=400, detail="Unsupported notifier test id")
    if spec.get("requires_confirmation") and not body.confirmed:
        raise HTTPException(status_code=400, detail="This notifier test requires confirmation")

    event_id = f"notifier-test-{body.test_id.replace('_', '-')}-{uuid.uuid4().hex[:12]}"
    event_type = str(spec["event_type"])
    data: dict[str, Any] = {
        "event_type": event_type,
        "blueprints_event_type": event_type,
        "test_id": body.test_id,
        "source_component": "blueprints-notifier-tests",
        "speech": str(spec["speech"]),
        "noisy": bool(spec.get("noisy")),
        "frontend_contract": "notification-tests-modal",
        "test_broadcast_speech": True,
    }
    importance = spec.get("importance")
    if importance is not None:
        data["importance"] = importance
    if body.test_id == "danger2_drill":
        data["danger2_drill"] = True

    submitted = await post_notifier_event(
        event_id=event_id,
        event_type=event_type,
        title=str(spec["title"]),
        message=str(spec["message"]),
        severity=str(spec["level"]),
        source_component="blueprints-notifier-tests",
        destinations=["matrix", "blueprints"],
        tags=["blueprints-notifier-tests", body.test_id],
        data=data,
        importance=importance,
        dedupe_key=event_id,
    )
    if not submitted:
        raise HTTPException(
            status_code=502,
            detail="system-bridge-notifier submission failed or is not configured",
        )
    return NotifierTestResponse(
        ok=True,
        status="submitted_to_notifier",
        test_id=body.test_id,
        event_id=event_id,
    )


@router.post("/tests/notifier-failure-drill", response_model=NotifierFailureDrillResponse)
async def run_notifier_failure_drill(
    body: NotifierFailureDrillRequest,
) -> NotifierFailureDrillResponse:
    if not body.confirmed:
        raise HTTPException(
            status_code=400,
            detail="The notifier failure drill requires confirmation",
        )

    attempted_event_id = f"notifier-failure-drill-attempt-{uuid.uuid4().hex[:12]}"
    fallback_event_id = f"notifier-failure-drill-warning-{uuid.uuid4().hex[:12]}"
    stop_ok = False
    restart_ok = False
    warning_published = False
    notifier_submission_failed = False

    try:
        stop_ok, _stop_output = await _run_notifier_stack_command("stop")
        if not stop_ok:
            raise HTTPException(
                status_code=502,
                detail="failed to stop system-bridge-notifier Dockge stack",
            )

        submitted = await post_notifier_event(
            event_id=attempted_event_id,
            event_type="system_bridge_notifier.failure.drill_attempt",
            title="Notifier failure drill attempt",
            message="This notifier event is expected to fail because the notifier stack is stopped.",
            severity="error",
            source_component="blueprints-notifier-tests",
            destinations=["matrix", "blueprints"],
            tags=["blueprints-notifier-tests", "notifier-failure-drill"],
            data={
                "event_type": "system_bridge_notifier.failure.drill_attempt",
                "blueprints_event_type": "system_bridge_notifier.failure.drill_attempt",
                "importance": "danger1",
                "test_id": "notifier_failure_warning",
                "source_component": "blueprints-notifier-tests",
            },
            importance="danger1",
            dedupe_key=attempted_event_id,
        )
        notifier_submission_failed = not submitted
    finally:
        restart_ok, _restart_output = await _run_notifier_stack_command(
            "up",
            "-d",
            timeout_seconds=90.0,
        )

    if not notifier_submission_failed:
        raise HTTPException(
            status_code=502,
            detail="failure drill did not observe notifier submission failure",
        )

    await _publish_notifier_failure_warning(
        fallback_event_id=fallback_event_id,
        attempted_event_id=attempted_event_id,
        restart_ok=restart_ok,
    )
    warning_published = True

    return NotifierFailureDrillResponse(
        ok=True,
        status="notifier_failure_warning_published",
        test_id="notifier_failure_warning",
        event_id=fallback_event_id,
        attempted_event_id=attempted_event_id,
        notifier_submission_failed=notifier_submission_failed,
        warning_published=warning_published,
        stop_ok=stop_ok,
        restart_ok=restart_ok,
    )


@router.get("/danger2-state", response_model=Danger2State)
async def get_danger2_state() -> Danger2State:
    return _read_danger2_state()


@router.post("/danger2-cancel", response_model=Danger2CancelResponse)
async def cancel_danger2(body: Danger2CancelRequest) -> Danger2CancelResponse:
    state = _write_danger2_state(
        Danger2State(
            cancel_id=f"danger2-cancel-{uuid.uuid4().hex[:12]}",
            cancelled_at=time.time(),
            reason=body.reason or "operator_cancelled",
            source=body.source or "blueprints-browser",
        )
    )
    event_id = f"danger2-cancel-{uuid.uuid4().hex[:12]}"
    submitted = await post_notifier_event(
        event_id=event_id,
        event_type="system_bridge_notifier.danger2.cancelled",
        title="Danger2 alert cancelled",
        message="A Danger2 alert was cancelled by an operator listener.",
        severity="info",
        source_component="blueprints-danger2-control",
        destinations=["matrix", "blueprints"],
        tags=["danger2", "cancel"],
        data={
            "event_type": "system_bridge_notifier.danger2.cancelled",
            "blueprints_event_type": "system_bridge_notifier.danger2.cancelled",
            "importance": "low_importance",
            "cancel_id": state.cancel_id,
            "cancelled_at": state.cancelled_at,
            "reason": state.reason,
            "source_component": "blueprints-danger2-control",
            "suppress_speech": True,
        },
        importance="low_importance",
        dedupe_key=event_id,
    )
    return Danger2CancelResponse(
        **state.model_dump(),
        notification_submitted=submitted,
    )
