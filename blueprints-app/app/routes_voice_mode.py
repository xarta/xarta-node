"""Node-local browser voice-mode activation endpoints."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import websockets
from fastapi import APIRouter, Request, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .db import get_conn, get_setting
from .events import AppEvent
from .routes_events import publish_event
from .routes_matrix_chat import _matrix_chat_stt_relay
from .routes_matrix_chat import _settings as _matrix_chat_settings

router = APIRouter(prefix="/voice-mode", tags=["voice-mode"])

_STATE_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-voice-mode.json")
_WAKE_DEBUG_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-wake-to-talk-debug.json")
_WAKE_DEV_DEBUG_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-wake-dev-debug.json")
_state_lock = asyncio.Lock()
_dependency_health_lock = asyncio.Lock()
_dependency_health_cache: dict[str, Any] = {
    "payload": None,
    "checked_at": 0.0,
    "next_check_seconds": 0.0,
}

_PROBE_TIMEOUT_SECONDS = 2.0
_HEALTHY_CACHE_SECONDS = 30.0
_UNHEALTHY_CACHE_SECONDS = 2.0
_NOISE_STACK_NAMES = {"xarta-voice-agent-integration", "blueprints-dfn-stt-noise"}
_LOCAL_TTS_STACK_NAME = "pockettts-openai"
_PIPECAT_API_BASE = os.getenv("VOICE_MODE_PIPECAT_API_BASE", "").rstrip("/")
_PIPECAT_VERIFY_TLS = str(os.getenv("VOICE_MODE_PIPECAT_VERIFY_TLS", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_AGGREGATION_TIMEOUT_PATH = "/api/service-manager/agent/aggregation-timeout"
_AGGREGATION_TIMEOUT_MIN_MS = 50
_AGGREGATION_TIMEOUT_MAX_MS = 300
_AGGREGATION_TIMEOUT_STEP_MS = 10
_AGGREGATION_TIMEOUT_DEFAULT_MS = 80
_VAD_RESET_TIMEOUT_MIN_MS = 0
_VAD_RESET_TIMEOUT_MAX_MS = 2000
_VAD_RESET_TIMEOUT_STEP_MS = 50
_VAD_RESET_TIMEOUT_DEFAULT_MS = 300
_SILENCE_RESET_TIMEOUT_MIN_MS = 0
_SILENCE_RESET_TIMEOUT_MAX_MS = 3000
_SILENCE_RESET_TIMEOUT_STEP_MS = 300
_SILENCE_RESET_TIMEOUT_DEFAULT_MS = 2100
_WAKE_DEBUG_STREAM_KEEPALIVE_SECONDS = 15.0
_VOICE_DEV_COMMAND_EVENT_TYPE = "voice.mode.dev.command"
_wake_debug_stream_lock = asyncio.Lock()
_wake_debug_subscribers: dict[int, asyncio.Queue[str]] = {}
_wake_debug_stream_sequence = 0
_DEV_COMMAND_MODES = {"manual", "vad", "vad_rearm"}
_DEV_COMMAND_ACTIONS = {
    "enable_test",
    "disable_test",
    "record",
    "stop",
    "clear",
}


class BrowserVoiceState(BaseModel):
    browser_id: str
    browser_label: str | None = None
    stt_enabled: bool = False
    stt_mode: str | None = None
    tts_enabled: bool = False


class VoiceModePolicy(BaseModel):
    tts_companion_model_preference: str | None = None


class WakeSettingsBody(BaseModel):
    wake_to_talk: dict[str, Any] | None = None
    stt: dict[str, Any] | None = None


class AggregationTimeoutBody(BaseModel):
    aggregation_timeout_ms: int = Field(
        default=_AGGREGATION_TIMEOUT_DEFAULT_MS,
        ge=_AGGREGATION_TIMEOUT_MIN_MS,
        le=_AGGREGATION_TIMEOUT_MAX_MS,
    )


class WakeDebugBody(BaseModel):
    browser_id: str
    browser_label: str | None = None
    tab_id: str | None = None
    running: bool = False
    starting: bool = False
    reason: str | None = None
    fsm_state: str | None = None
    session_id: int | None = None
    active_instance_id: str | None = None
    active_send: dict[str, Any] | None = None
    queues: dict[str, Any] | None = None
    transcript: str | None = None
    frozen_send_snapshot: dict[str, Any] | None = None
    command_diagnostics: dict[str, Any] | None = None
    last_action: dict[str, Any] | None = None
    recent_actions: list[dict[str, Any]] | None = None
    recent_stt_events: list[dict[str, Any]] | None = None
    stream_epoch: int | None = None
    audio_frames_sent: int | None = None
    audio_frames_captured: int | None = None
    audio_timing: dict[str, Any] | None = None
    stt_reset_pending_reason: str | None = None
    stt_speech_start_reset_pending: bool = False
    vad_speech_start_reset_armed: bool = False
    audio_delay_frames: int | None = None
    audio_candidate_frames: int | None = None
    stt_delay_frames: int | None = None
    stt_segment_active: bool = False
    stt_segment: dict[str, Any] | None = None
    audio_features: dict[str, Any] | None = None
    vad: dict[str, Any] | None = None
    client_now_ms: float | None = None


class VoiceDevCommandBody(BaseModel):
    mode: str = "manual"
    action: str = "record"
    browser_id: str | None = None
    tab_id: str | None = None
    command_id: str | None = None
    open_modal: bool = False
    target_active_browser: bool = True
    max_age_seconds: int = Field(default=60, ge=5, le=300)


class WakeDevDebugBody(BaseModel):
    browser_id: str
    browser_label: str | None = None
    tab_id: str | None = None
    mode: str | None = None
    source: str | None = None
    status: str | None = None
    transcript: str | None = None
    snapshot: dict[str, Any] | None = None
    client_now_ms: float | None = None


def _clean_issue(value: str) -> str:
    return " ".join(str(value or "").strip().split())[:80]


def _clean_browser_id(value: str | None) -> str:
    return str(value or "").strip()[:160]


def _clean_label(value: str | None, fallback: str) -> str:
    label = str(value or "").strip()
    return (label or fallback)[:120]


def _empty_state() -> dict[str, Any]:
    return {
        "active": None,
        "policy": {
            "tts_companion_model_preference": "codex_spark",
            "wake_to_talk": _default_wake_to_talk_policy(),
            "stt": {
                "speech_aggregation_timeout_ms": _AGGREGATION_TIMEOUT_DEFAULT_MS,
                "vad_reset_timeout_ms": _VAD_RESET_TIMEOUT_DEFAULT_MS,
                "silence_reset_timeout_ms": _SILENCE_RESET_TIMEOUT_DEFAULT_MS,
            },
        },
        "revision": 0.0,
        "updated_at": 0.0,
    }


def _clean_model_preference(value: str | None) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    if raw in {"local", "local_private", "private_local", "no_think", "local_no_think"}:
        return "local_private"
    if raw in {"codex", "codex_spark", "spark", "gpt_5_3_codex_spark"}:
        return "codex_spark"
    return "codex_spark"


def _clean_stt_mode(value: str | None, stt_enabled: bool = False) -> str:
    raw = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    if raw in {"realtime", "real_time", "conversation", "realtime_conversation"}:
        return "realtime_conversation"
    if raw in {"push", "push_to_talk", "ptt", "stt"}:
        return "push_to_talk"
    if raw in {"wake", "wake_to_talk", "wake_word"}:
        return "wake_to_talk"
    if raw in {"", "off", "none", "disabled"}:
        return "push_to_talk" if stt_enabled else ""
    return ""


def _clean_string(value: Any, fallback: str = "", max_length: int = 255) -> str:
    text = " ".join(str(value if value is not None else fallback).strip().split())
    return (text or fallback)[:max_length]


def _clean_dev_command_id(value: str | None = None) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ":", "."})[:100]
    return clean or f"voice-dev-{uuid.uuid4().hex}"


def _clean_dev_command_mode(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _clean_dev_command_action(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _clean_hermes_prefix(value: Any, fallback: str) -> str:
    prefix = _clean_string(value, fallback, 40)
    if not prefix:
        prefix = fallback
    if not prefix.endswith(":"):
        prefix = prefix.rstrip()
    if prefix.endswith(":"):
        prefix = f"{prefix} "
    elif not prefix.endswith(" "):
        prefix = f"{prefix} "
    return prefix[:40]


def _clean_int_step(
    value: Any,
    *,
    fallback: int,
    minimum: int,
    maximum: int,
    step: int,
) -> int:
    try:
        parsed = int(round(float(value)))
    except (TypeError, ValueError):
        parsed = fallback
    clamped = max(minimum, min(maximum, parsed))
    if step > 1:
        clamped = int(round(clamped / step) * step)
        clamped = max(minimum, min(maximum, clamped))
    return clamped


def _wake_aliases(wake_word: str, configured: Any = None) -> list[str]:
    aliases: list[str] = []
    for value in [wake_word, *(configured if isinstance(configured, list) else [])]:
        normalized = " ".join(str(value or "").strip().lower().replace("-", " ").split())
        compact = normalized.replace(" ", "")
        hyphenated = normalized.replace(" ", "-")
        for candidate in (normalized, compact, hyphenated):
            if candidate and candidate not in aliases:
                aliases.append(candidate)
    return aliases[:8]


def _default_wake_instance(
    *,
    label: str,
    matrix_server: str,
    wake_word: str,
    hermes_prefix: str,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "label": label,
        "matrix_server": matrix_server,
        "matrix_room_id": "",
        "wake_word": wake_word,
        "wake_aliases": _wake_aliases(wake_word),
        "hermes_prefix": hermes_prefix,
        "post_wake_pause_ms": 500,
        "initial_silence_cancel_ms": 1000,
        "pause_reset_seconds": 30,
        "auto_execute_silence_ms": 0,
        "commands": {
            "pause": "pause-dictation",
            "resume": "resume-dictation",
            "execute": "execute",
            "cancel": "cancel-dictation",
        },
    }


def _default_wake_to_talk_policy() -> dict[str, Any]:
    return {
        "instances": {
            "local": _default_wake_instance(
                label="local",
                matrix_server="tb1",
                wake_word="Computer",
                hermes_prefix="hermes: ",
            ),
            "vps": _default_wake_instance(
                label="vps",
                matrix_server="vps",
                wake_word="Mini-Me",
                hermes_prefix="hermes-vps: ",
            ),
        }
    }


def _clean_wake_command_map(value: Any) -> dict[str, str]:
    commands = value if isinstance(value, dict) else {}
    defaults = {
        "pause": "pause-dictation",
        "resume": "resume-dictation",
        "execute": "execute",
        "cancel": "cancel-dictation",
    }
    return {
        key: _clean_string(commands.get(key), fallback, 80)
        for key, fallback in defaults.items()
    }


def _clean_wake_instance(instance_id: str, value: Any) -> dict[str, Any]:
    defaults = _default_wake_to_talk_policy()["instances"][instance_id]
    raw = value if isinstance(value, dict) else {}
    matrix_server = _clean_string(raw.get("matrix_server"), defaults["matrix_server"], 16).lower()
    if matrix_server not in {"tb1", "vps"}:
        matrix_server = defaults["matrix_server"]
    wake_word = _clean_string(raw.get("wake_word"), defaults["wake_word"], 80)
    auto_execute_raw = raw.get("auto_execute_silence_ms", defaults["auto_execute_silence_ms"])
    auto_execute = 0 if str(auto_execute_raw).strip() in {"", "0", "false", "off", "disabled"} else _clean_int_step(
        auto_execute_raw,
        fallback=defaults["auto_execute_silence_ms"] or 300,
        minimum=300,
        maximum=3000,
        step=300,
    )
    return {
        # Wake instance activation is controlled by the browser's Wake-to-Talk
        # STT mode plus backend activated-browser state. Keep this field true for
        # compatibility with earlier persisted settings, but do not expose it
        # as a second user-facing enable switch.
        "enabled": True,
        "label": _clean_string(raw.get("label"), defaults["label"], 40),
        "matrix_server": matrix_server,
        "matrix_room_id": _clean_string(raw.get("matrix_room_id"), defaults["matrix_room_id"], 255),
        "wake_word": wake_word,
        "wake_aliases": _wake_aliases(wake_word, raw.get("wake_aliases")),
        "hermes_prefix": _clean_hermes_prefix(raw.get("hermes_prefix"), defaults["hermes_prefix"]),
        "post_wake_pause_ms": _clean_int_step(
            raw.get("post_wake_pause_ms"),
            fallback=defaults["post_wake_pause_ms"],
            minimum=0,
            maximum=2000,
            step=50,
        ),
        "initial_silence_cancel_ms": _clean_int_step(
            raw.get("initial_silence_cancel_ms"),
            fallback=defaults["initial_silence_cancel_ms"],
            minimum=0,
            maximum=2000,
            step=50,
        ),
        "pause_reset_seconds": _clean_int_step(
            raw.get("pause_reset_seconds"),
            fallback=defaults["pause_reset_seconds"],
            minimum=5,
            maximum=120,
            step=5,
        ),
        "auto_execute_silence_ms": auto_execute,
        "commands": _clean_wake_command_map(raw.get("commands")),
    }


def _clean_wake_to_talk_policy(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    instances = raw.get("instances") if isinstance(raw.get("instances"), dict) else {}
    return {
        "instances": {
            "local": _clean_wake_instance("local", instances.get("local")),
            "vps": _clean_wake_instance("vps", instances.get("vps")),
        }
    }


def _clean_stt_policy(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, dict) else {}
    return {
        "speech_aggregation_timeout_ms": _clean_int_step(
            raw.get("speech_aggregation_timeout_ms"),
            fallback=_AGGREGATION_TIMEOUT_DEFAULT_MS,
            minimum=_AGGREGATION_TIMEOUT_MIN_MS,
            maximum=_AGGREGATION_TIMEOUT_MAX_MS,
            step=_AGGREGATION_TIMEOUT_STEP_MS,
        ),
        "vad_reset_timeout_ms": _clean_int_step(
            raw.get("vad_reset_timeout_ms"),
            fallback=_VAD_RESET_TIMEOUT_DEFAULT_MS,
            minimum=_VAD_RESET_TIMEOUT_MIN_MS,
            maximum=_VAD_RESET_TIMEOUT_MAX_MS,
            step=_VAD_RESET_TIMEOUT_STEP_MS,
        ),
        "silence_reset_timeout_ms": _clean_int_step(
            raw.get("silence_reset_timeout_ms"),
            fallback=_SILENCE_RESET_TIMEOUT_DEFAULT_MS,
            minimum=_SILENCE_RESET_TIMEOUT_MIN_MS,
            maximum=_SILENCE_RESET_TIMEOUT_MAX_MS,
            step=_SILENCE_RESET_TIMEOUT_STEP_MS,
        ),
    }


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _url_host(value: str | None) -> str:
    parsed = urlparse(str(value or "").strip())
    return parsed.hostname or ""


def _http_url_from_ws(value: str | None, path: str = "/health") -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
        return raw
    scheme = "https" if parsed.scheme == "wss" else "http"
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{scheme}://{parsed.netloc}{suffix}"


def _url_for_host(host: str, port: int, path: str = "") -> str:
    clean_host = str(host or "").strip()
    if not clean_host:
        return ""
    clean_path = path if path.startswith("/") or not path else f"/{path}"
    return f"http://{clean_host}:{port}{clean_path}"


def _lxc_api_base_for_machine(machine: dict[str, Any]) -> str:
    explicit = os.getenv("VOICE_MODE_LXC_API_BASE", "").strip().rstrip("/")
    if explicit:
        return explicit
    return _url_for_host(str(machine.get("pve_host") or ""), 7871)


def _gpu_monitor_health_url_for_machine(machine: dict[str, Any]) -> str:
    explicit = os.getenv("VOICE_MODE_GPU_MONITOR_HEALTH_URL", "").strip()
    if explicit:
        return explicit
    return _url_for_host(str(machine.get("pve_host") or ""), 7870, "/health")


def _component(
    key: str,
    label: str,
    *,
    configured: bool = True,
    ok: bool = False,
    issue: str = "",
    status: str | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = "ok" if ok else ("unconfigured" if not configured else "error")
    return {
        "key": key,
        "label": label,
        "configured": configured,
        "ok": ok,
        "state": status or state,
        "issue": _clean_issue(issue),
        "detail": detail or {},
    }


async def _probe_http_json(url: str, timeout_seconds: float = _PROBE_TIMEOUT_SECONDS) -> dict[str, Any]:
    if not url:
        return {"ok": False, "status": "unconfigured", "error": "unconfigured"}
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.get(url)
    except httpx.TimeoutException:
        return {"ok": False, "status": "timeout", "error": "timeout"}
    except httpx.RequestError as exc:
        return {"ok": False, "status": "error", "error": str(exc)[:160]}

    body: Any = None
    text = ""
    try:
        body = response.json()
    except ValueError:
        text = (response.text or "").strip()[:240]
    return {
        "ok": response.is_success,
        "status": response.status_code,
        "body": body,
        "detail": text,
    }


def _health_body_issue(probe: dict[str, Any], label: str) -> str:
    body = probe.get("body")
    if not isinstance(body, dict):
        return ""
    status = str(body.get("status") or body.get("health") or "").strip().lower()
    if status and status not in {"ok", "healthy", "ready", "up"}:
        return f"{label} bad health"
    healthy = body.get("healthy")
    if healthy is False:
        return f"{label} bad health"
    return ""


async def _probe_websocket_open(url: str, timeout_seconds: float = _PROBE_TIMEOUT_SECONDS) -> dict[str, Any]:
    if not url:
        return {"ok": False, "status": "unconfigured", "error": "unconfigured"}
    try:
        async with websockets.connect(
            url,
            open_timeout=timeout_seconds,
            close_timeout=0.5,
            max_size=1024 * 1024,
            ping_interval=None,
        ) as ws:
            await ws.send(json.dumps({"type": "config", "sample_rate": 16000}))
            ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout_seconds))
            if ack.get("type") != "config_ack":
                return {"ok": False, "status": "bad_response", "error": "bad config response", "body": ack}
            await ws.send(json.dumps({"type": "ping"}))
            pong = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout_seconds))
            if pong.get("type") != "pong":
                return {"ok": False, "status": "bad_response", "error": "bad ping response", "body": pong}
            with contextlib.suppress(Exception):
                await ws.send(json.dumps({"type": "end"}))
        return {"ok": True, "status": "ready", "body": ack}
    except TimeoutError:
        return {"ok": False, "status": "timeout", "error": "timeout"}
    except Exception as exc:
        return {"ok": False, "status": "error", "error": str(exc)[:160]}


def _machine_for_host(host: str) -> dict[str, Any]:
    if not host:
        return {}
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT config_id, pve_host, pve_name, vmid, vm_type, name, status, ip_address, last_probed
            FROM proxmox_config
            WHERE ip_address = ?
            LIMIT 1
            """,
            (host,),
        ).fetchone()
    return dict(row) if row else {}


async def _pve_lxc_status(machine: dict[str, Any]) -> dict[str, Any]:
    vmid = machine.get("vmid")
    if not vmid:
        return {"ok": False, "issue": "", "detail": {}}
    pve_name = str(machine.get("pve_name") or "PVE host")
    lxc_label = f"lxc{vmid}"
    api_base = _lxc_api_base_for_machine(machine)
    if not api_base:
        return {"ok": False, "issue": "", "detail": {}}

    lxc = await _probe_http_json(f"{api_base}/lxc/{vmid}/status")
    if lxc.get("ok"):
        body = lxc.get("body") if isinstance(lxc.get("body"), dict) else {}
        status = str(body.get("status") or "").strip().lower()
        if status and status != "running":
            if "restart" in status:
                return {"ok": False, "issue": f"{lxc_label} restarting", "detail": {"lxc": lxc}}
            return {"ok": False, "issue": f"{lxc_label} offline", "detail": {"lxc": lxc}}
        return {"ok": True, "issue": "", "detail": {"lxc": lxc}}

    pve_health = await _probe_http_json(f"{api_base}/health")
    if not pve_health.get("ok"):
        gpu_health = await _probe_http_json(_gpu_monitor_health_url_for_machine(machine))
        if not gpu_health.get("ok"):
            return {
                "ok": False,
                "issue": f"{pve_name} offline",
                "detail": {"lxc": lxc, "lxc_api": pve_health, "gpu_monitor": gpu_health},
            }
        return {
            "ok": False,
            "issue": "lxc api offline",
            "detail": {"lxc": lxc, "lxc_api": pve_health, "gpu_monitor": gpu_health},
        }
    return {"ok": False, "issue": f"{lxc_label} status unknown", "detail": {"lxc": lxc, "lxc_api": pve_health}}


async def _active_mode_stack_status(names: set[str], machine: dict[str, Any]) -> dict[str, Any]:
    api_base = _lxc_api_base_for_machine(machine)
    if not api_base:
        return {"ok": False, "issue": "", "detail": {}}
    modes = await _probe_http_json(f"{api_base}/lxc/modes")
    if not modes.get("ok") or not isinstance(modes.get("body"), dict):
        return {"ok": False, "issue": "", "detail": {"modes": modes}}
    body = modes["body"]
    active_mode = body.get("active_mode")
    mode = next((item for item in body.get("modes", []) if item.get("id") == active_mode), {})
    stack_details = mode.get("docker_stack_details") if isinstance(mode, dict) else []
    for stack in stack_details or []:
        name = str(stack.get("name") or "").strip()
        if name in names:
            status = str(stack.get("status") or "").strip().lower()
            if status != "running":
                return {
                    "ok": False,
                    "issue": "noise reduction stack offline",
                    "detail": {"stack": stack, "modes": {"active_mode": active_mode}},
                }
            return {"ok": True, "issue": "", "detail": {"stack": stack, "modes": {"active_mode": active_mode}}}
    return {"ok": False, "issue": "", "detail": {"modes": {"active_mode": active_mode}}}


def _local_dockge_stack_status(stack_name: str) -> dict[str, Any]:
    try:
        from .routes_local_dockge import _inspect_stack

        return _inspect_stack(stack_name)
    except Exception as exc:
        return {"status": "unknown", "health": "unknown", "error": str(exc)[:160]}


async def _tts_component() -> dict[str, Any]:
    settings: dict[str, str] = {}
    missing: list[str] = []
    with get_conn() as conn:
        required = (
            "tts.enabled",
            "tts.local_probe_url",
            "tts.local_speech_url",
            "tts.timeout_ms",
        )
        for key in required:
            value = get_setting(conn, key)
            if value is None or str(value).strip() == "":
                missing.append(key)
            else:
                settings[key] = value

    enabled = _truthy(settings.get("tts.enabled"))
    probe_url = str(settings.get("tts.local_probe_url") or "").strip()
    configured = enabled and not missing and bool(probe_url)
    if not configured:
        return _component(
            "tts",
            "TTS",
            configured=False,
            issue="TTS not configured",
            detail={"missing_settings": missing, "probe_url": probe_url},
        )

    probe = await _probe_http_json(probe_url, timeout_seconds=2.0)
    if probe.get("ok"):
        if issue := _health_body_issue(probe, "TTS stack"):
            return _component("tts", "TTS", issue=issue, detail={"probe_url": probe_url, "probe": probe})
        return _component("tts", "TTS", ok=True, detail={"probe_url": probe_url, "probe": probe})

    host = _url_host(probe_url)
    diagnostic: dict[str, Any] = {"probe_url": probe_url, "probe": probe}
    if host in {"127.0.0.1", "localhost", "::1"}:
        stack = await asyncio.to_thread(_local_dockge_stack_status, _LOCAL_TTS_STACK_NAME)
        diagnostic["stack"] = stack
        status = str(stack.get("status") or "").lower()
        health = str(stack.get("health") or "").lower()
        if status and status != "running":
            return _component("tts", "TTS", issue="TTS stack offline", detail=diagnostic)
        if health and health not in {"healthy", "none"}:
            return _component("tts", "TTS", issue="TTS stack bad health", detail=diagnostic)
    return _component("tts", "TTS", issue="TTS not responding", detail=diagnostic)


async def _stt_component(settings: dict[str, str]) -> dict[str, Any]:
    ws_url = str(settings.get("stt_ws_url") or "").strip()
    if not ws_url:
        return _component("stt", "STT", configured=False, issue="STT not configured")
    health_url = _http_url_from_ws(ws_url, "/health")
    probe = await _probe_http_json(health_url)
    if probe.get("ok"):
        if issue := _health_body_issue(probe, "STT"):
            return _component("stt", "STT", issue=issue, detail={"ws_url": ws_url, "health_url": health_url, "probe": probe})
        return _component("stt", "STT", ok=True, detail={"ws_url": ws_url, "health_url": health_url, "probe": probe})

    machine = _machine_for_host(_url_host(ws_url))
    diagnostic = {"ws_url": ws_url, "health_url": health_url, "probe": probe, "machine": machine}
    parent = await _pve_lxc_status(machine) if machine else {"ok": False, "issue": "", "detail": {}}
    diagnostic.update(parent.get("detail") or {})
    if parent.get("issue"):
        return _component("stt", "STT", issue=parent["issue"], detail=diagnostic)
    return _component("stt", "STT", issue="STT not responding", detail=diagnostic)


async def _noise_component(settings: dict[str, str], *, deep_probe: bool = False) -> dict[str, Any]:
    ws_url = str(settings.get("stt_noise_dfn_ws_url") or "").strip()
    if not ws_url:
        return _component(
            "noise_reduction",
            "Noise reduction",
            configured=False,
            issue="noise reduction not configured",
        )

    machine = _machine_for_host(_url_host(ws_url))
    diagnostic: dict[str, Any] = {
        "ws_url": ws_url,
        "machine": machine,
        "probe": {
            "skipped": not deep_probe,
            "reason": "normal health uses non-invasive LXC/stack status",
        },
    }
    if deep_probe:
        probe = await _probe_websocket_open(ws_url)
        diagnostic["probe"] = probe
        if probe.get("ok"):
            return _component("noise_reduction", "Noise reduction", ok=True, detail=diagnostic)

    parent = await _pve_lxc_status(machine) if machine else {"ok": False, "issue": "", "detail": {}}
    diagnostic.update(parent.get("detail") or {})
    if parent.get("issue"):
        return _component("noise_reduction", "Noise reduction", issue=parent["issue"], detail=diagnostic)
    stack = await _active_mode_stack_status(_NOISE_STACK_NAMES, machine)
    diagnostic.update(stack.get("detail") or {})
    if stack.get("issue"):
        return _component("noise_reduction", "Noise reduction", issue=stack["issue"], detail=diagnostic)
    if stack.get("ok"):
        return _component(
            "noise_reduction",
            "Noise reduction",
            ok=True,
            status="ready",
            detail=diagnostic,
        )
    issue = "noise reduction websocket probe failed" if deep_probe else "noise reduction stack status unknown"
    return _component("noise_reduction", "Noise reduction", issue=issue, detail=diagnostic)


async def _build_dependency_health(*, deep_noise_probe: bool = False) -> dict[str, Any]:
    settings = _matrix_chat_settings("tb1")
    stt, noise, tts = await asyncio.gather(
        _stt_component(settings),
        _noise_component(settings, deep_probe=deep_noise_probe),
        _tts_component(),
    )
    components = {
        "stt": stt,
        "noise_reduction": noise,
        "tts": tts,
    }
    ok = all(component.get("ok") for component in components.values())
    next_check_seconds = _HEALTHY_CACHE_SECONDS if ok else _UNHEALTHY_CACHE_SECONDS
    return {
        "ok": ok,
        "components": components,
        "checked_at": time.time(),
        "next_check_seconds": next_check_seconds,
        "sources": {
            "stt_ws_url": settings.get("stt_ws_url"),
            "noise_ws_url": settings.get("stt_noise_dfn_ws_url"),
        },
        "probe_modes": {
            "noise_reduction": "websocket" if deep_noise_probe else "non_invasive_stack_status",
        },
    }


async def _dependency_health_payload(force: bool = False, *, deep_noise_probe: bool = False) -> dict[str, Any]:
    now = time.time()
    async with _dependency_health_lock:
        cached = _dependency_health_cache.get("payload")
        checked_at = float(_dependency_health_cache.get("checked_at") or 0.0)
        next_check = float(_dependency_health_cache.get("next_check_seconds") or 0.0)
        if not force and not deep_noise_probe and cached and now - checked_at < next_check:
            payload = dict(cached)
            payload["cached"] = True
            payload["cache_age_seconds"] = round(now - checked_at, 3)
            return payload
        payload = await _build_dependency_health(deep_noise_probe=deep_noise_probe)
        if not deep_noise_probe:
            _dependency_health_cache.update({
                "payload": payload,
                "checked_at": payload["checked_at"],
                "next_check_seconds": payload["next_check_seconds"],
            })
        payload = dict(payload)
        payload["cached"] = False
        payload["cache_age_seconds"] = 0
        return payload



def _clean_policy(value: Any) -> dict[str, Any]:
    policy = value if isinstance(value, dict) else {}
    return {
        "tts_companion_model_preference": _clean_model_preference(
            policy.get("tts_companion_model_preference")
        ),
        "wake_to_talk": _clean_wake_to_talk_policy(policy.get("wake_to_talk")),
        "stt": _clean_stt_policy(policy.get("stt")),
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
    state["policy"] = _clean_policy(state.get("policy"))
    return state


def _write_state_unlocked(state: dict[str, Any]) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_STATE_PATH)


def _read_wake_debug_unlocked() -> dict[str, Any]:
    try:
        raw = json.loads(_WAKE_DEBUG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"reports": {}, "updated_at": 0.0}
    except Exception:
        return {"reports": {}, "updated_at": 0.0}
    if not isinstance(raw, dict):
        return {"reports": {}, "updated_at": 0.0}
    reports = raw.get("reports") if isinstance(raw.get("reports"), dict) else {}
    return {
        "reports": reports,
        "updated_at": float(raw.get("updated_at") or 0.0),
    }


def _write_wake_debug_unlocked(debug: dict[str, Any]) -> None:
    _WAKE_DEBUG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _WAKE_DEBUG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(debug, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_WAKE_DEBUG_PATH)


def _read_wake_dev_debug_unlocked() -> dict[str, Any]:
    try:
        raw = json.loads(_WAKE_DEV_DEBUG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"reports": {}, "updated_at": 0.0}
    except Exception:
        return {"reports": {}, "updated_at": 0.0}
    if not isinstance(raw, dict):
        return {"reports": {}, "updated_at": 0.0}
    reports = raw.get("reports") if isinstance(raw.get("reports"), dict) else {}
    return {
        "reports": reports,
        "updated_at": float(raw.get("updated_at") or 0.0),
    }


def _write_wake_dev_debug_unlocked(debug: dict[str, Any]) -> None:
    _WAKE_DEV_DEBUG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _WAKE_DEV_DEBUG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(debug, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_WAKE_DEV_DEBUG_PATH)


def _bounded_json(value: Any, max_chars: int = 20000) -> Any:
    try:
        encoded = json.dumps(value)
    except TypeError:
        return None
    if len(encoded) <= max_chars:
        return value
    return {"truncated": True, "chars": len(encoded)}


def _selected_wake_debug_report(state: dict[str, Any], debug: dict[str, Any]) -> dict[str, Any] | None:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
    selected = reports.get(active_browser_id) if active_browser_id else None
    if not isinstance(selected, dict) and reports and not active_browser_id:
        selected = max(
            (report for report in reports.values() if isinstance(report, dict)),
            key=lambda item: float(item.get("reported_at") or 0.0),
            default=None,
        )
    return selected if isinstance(selected, dict) else None


def _selected_browser_report(state: dict[str, Any], debug: dict[str, Any]) -> dict[str, Any] | None:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
    selected = reports.get(active_browser_id) if active_browser_id else None
    if not isinstance(selected, dict) and reports and not active_browser_id:
        selected = max(
            (report for report in reports.values() if isinstance(report, dict)),
            key=lambda item: float(item.get("reported_at") or 0.0),
            default=None,
        )
    return selected if isinstance(selected, dict) else None


def _public_active(active: dict[str, Any] | None) -> dict[str, Any] | None:
    if not active:
        return None
    public_active = dict(active)
    public_active["stt_mode"] = _clean_stt_mode(
        public_active.get("stt_mode"),
        bool(public_active.get("stt_enabled")),
    )
    public_active["stt_enabled"] = bool(public_active["stt_mode"])
    return public_active


def _mask_wake_debug_for_backend_activation(
    active: dict[str, Any] | None,
    report: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(report, dict):
        return None
    public = dict(report)
    active_browser_id = _clean_browser_id(active.get("browser_id") if isinstance(active, dict) else "")
    report_browser_id = _clean_browser_id(report.get("browser_id"))
    public["authoritative_browser_active"] = bool(active_browser_id and report_browser_id == active_browser_id)
    if public["authoritative_browser_active"]:
        return public
    public["running"] = False
    public["starting"] = False
    public["fsm_state"] = "SELECTED_INACTIVE"
    public["reason"] = "This browser is not activated for Voice Mode."
    public["active_instance_id"] = ""
    return public


def _public_wake_debug(state: dict[str, Any], debug: dict[str, Any]) -> dict[str, Any]:
    selected = _selected_wake_debug_report(state, debug)
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    public_debug = _mask_wake_debug_for_backend_activation(active, selected)
    reported_at = float(public_debug.get("reported_at") or 0.0) if isinstance(public_debug, dict) else 0.0
    reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
    return {
        "ok": True,
        "active": _public_active(active),
        "debug": public_debug,
        "has_debug": isinstance(public_debug, dict),
        "age_seconds": round(max(0.0, time.time() - reported_at), 3) if reported_at else None,
        "reports_count": len(reports),
        "path": str(_WAKE_DEBUG_PATH),
    }


def _public_wake_dev_debug(state: dict[str, Any], debug: dict[str, Any]) -> dict[str, Any]:
    selected = _selected_browser_report(state, debug)
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    report_browser_id = _clean_browser_id(selected.get("browser_id") if isinstance(selected, dict) else "")
    public_debug = dict(selected) if isinstance(selected, dict) else None
    if isinstance(public_debug, dict):
        public_debug["authoritative_browser_active"] = bool(
            active_browser_id and report_browser_id == active_browser_id
        )
    reported_at = float(public_debug.get("reported_at") or 0.0) if isinstance(public_debug, dict) else 0.0
    reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
    return {
        "ok": True,
        "active": _public_active(active),
        "debug": public_debug,
        "has_debug": isinstance(public_debug, dict),
        "age_seconds": round(max(0.0, time.time() - reported_at), 3) if reported_at else None,
        "reports_count": len(reports),
        "path": str(_WAKE_DEV_DEBUG_PATH),
    }


def _wake_debug_sse(payload: dict[str, Any], sequence: int) -> str:
    event = {
        **payload,
        "stream": {
            "sequence": sequence,
            "server_now": time.time(),
            "source": "wake-debug-stream",
        },
    }
    return f"id:{sequence}\nevent:wake-debug\ndata:{json.dumps(event, separators=(',', ':'))}\n\n"


async def _current_public_wake_debug() -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_debug_unlocked()
    return _public_wake_debug(state, debug)


async def _broadcast_wake_debug(payload: dict[str, Any]) -> None:
    global _wake_debug_stream_sequence
    async with _wake_debug_stream_lock:
        _wake_debug_stream_sequence += 1
        frame = _wake_debug_sse(payload, _wake_debug_stream_sequence)
        subscribers = list(_wake_debug_subscribers.values())
    for queue in subscribers:
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(frame)


async def _broadcast_current_wake_debug() -> None:
    await _broadcast_wake_debug(await _current_public_wake_debug())


async def _wake_debug_stream(request: Request):
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=8)
    subscriber_id = id(queue)
    async with _wake_debug_stream_lock:
        _wake_debug_subscribers[subscriber_id] = queue
        sequence = _wake_debug_stream_sequence
    try:
        yield _wake_debug_sse(await _current_public_wake_debug(), sequence)
        while not await request.is_disconnected():
            try:
                frame = await asyncio.wait_for(
                    queue.get(),
                    timeout=_WAKE_DEBUG_STREAM_KEEPALIVE_SECONDS,
                )
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield frame
    finally:
        async with _wake_debug_stream_lock:
            _wake_debug_subscribers.pop(subscriber_id, None)


def _public_state(state: dict[str, Any], debug: dict[str, Any] | None = None) -> dict[str, Any]:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    return {
        "ok": True,
        "active": _public_active(active),
        "policy": _clean_policy(state.get("policy")),
        "revision": float(state.get("revision") or 0),
        "updated_at": float(state.get("updated_at") or 0),
    }


def _activated_browser_from_body(body: BrowserVoiceState, now: float) -> dict[str, Any]:
    stt_mode = _clean_stt_mode(body.stt_mode, body.stt_enabled)
    return {
        "browser_id": _clean_browser_id(body.browser_id),
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "stt_enabled": bool(stt_mode),
        "stt_mode": stt_mode,
        "tts_enabled": bool(body.tts_enabled),
        "activated_at": now,
    }


class _VoiceModeActivationFsm:
    STATE_IDLE = "IDLE"
    STATE_ACTIVATED = "ACTIVATED"
    INPUT_ACTIVATE_REQUEST = "ACTIVATE_REQUEST"
    INPUT_DEACTIVATE_REQUEST = "DEACTIVATE_REQUEST"
    ACTION_ACTIVATE_BROWSER = "ACTIVATE_BROWSER"
    ACTION_DEACTIVATE_IF_OWNER = "DEACTIVATE_IF_OWNER"
    ACTION_IGNORE = "IGNORE"
    TABLE = {
        STATE_IDLE: {
            INPUT_ACTIVATE_REQUEST: (STATE_ACTIVATED, ACTION_ACTIVATE_BROWSER),
            INPUT_DEACTIVATE_REQUEST: (STATE_IDLE, ACTION_IGNORE),
        },
        STATE_ACTIVATED: {
            INPUT_ACTIVATE_REQUEST: (STATE_ACTIVATED, ACTION_ACTIVATE_BROWSER),
            INPUT_DEACTIVATE_REQUEST: (STATE_IDLE, ACTION_DEACTIVATE_IF_OWNER),
        },
    }

    def __init__(self, state: dict[str, Any]):
        self.state = state

    @property
    def name(self) -> str:
        active = self.state.get("active") if isinstance(self.state.get("active"), dict) else None
        return self.STATE_ACTIVATED if active else self.STATE_IDLE

    def dispatch(
        self,
        input_name: str,
        *,
        browser_id: str,
        activated_browser: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        timestamp = float(now if now is not None else time.time())
        input_name = _clean_string(input_name, "", 40).upper()
        browser_id = _clean_browser_id(browser_id)
        before = self.name
        next_state, action = self.TABLE.get(before, {}).get(
            input_name,
            (before, self.ACTION_IGNORE),
        )

        if action == self.ACTION_ACTIVATE_BROWSER:
            if not activated_browser:
                return {"changed": False, "from": before, "to": before, "output": "ignored"}
            self.state["active"] = activated_browser
        elif action == self.ACTION_DEACTIVATE_IF_OWNER:
            active = self.state.get("active") if isinstance(self.state.get("active"), dict) else None
            if not active or _clean_browser_id(active.get("browser_id")) != browser_id:
                return {"changed": False, "from": before, "to": before, "output": "ignored"}
            self.state["active"] = None
        else:
            return {"changed": False, "from": before, "to": before, "output": "ignored"}

        self.state["revision"] = timestamp
        self.state["updated_at"] = timestamp
        return {
            "changed": True,
            "from": before,
            "to": next_state,
            "output": _public_state(self.state),
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
            "policy": public["policy"],
            "revision": public["revision"],
            "updated_at": public["updated_at"],
        },
    )
    await publish_event(event)


@router.get("/status")
async def voice_mode_status() -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        return _public_state(state)


@router.get("/dependency-health")
async def voice_mode_dependency_health(
    force: bool = False,
    deep_noise_probe: bool = False,
) -> dict[str, Any]:
    return await _dependency_health_payload(force=force, deep_noise_probe=deep_noise_probe)


@router.post("/activate")
async def voice_mode_activate(body: BrowserVoiceState):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})
    stt_mode = _clean_stt_mode(body.stt_mode, body.stt_enabled)
    if not stt_mode and not body.tts_enabled:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": "Enable STT or TTS before activating this browser."},
        )

    async with _state_lock:
        state = _read_state_unlocked()
        now = time.time()
        activation = _VoiceModeActivationFsm(state).dispatch(
            _VoiceModeActivationFsm.INPUT_ACTIVATE_REQUEST,
            browser_id=browser_id,
            activated_browser=_activated_browser_from_body(body, now),
            now=now,
        )
        changed = bool(activation.get("changed"))
        if not changed:
            return JSONResponse(status_code=400, content={"ok": False, "detail": "Activation request was rejected."})
        _write_state_unlocked(state)
    await _publish_changed(state, "activate")
    await _broadcast_current_wake_debug()
    return _public_state(state)


@router.post("/deactivate")
async def voice_mode_deactivate(body: BrowserVoiceState):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})

    async with _state_lock:
        state = _read_state_unlocked()
        activation = _VoiceModeActivationFsm(state).dispatch(
            _VoiceModeActivationFsm.INPUT_DEACTIVATE_REQUEST,
            browser_id=browser_id,
            now=time.time(),
        )
        changed = bool(activation.get("changed"))
        if changed:
            _write_state_unlocked(state)
    if changed:
        await _publish_changed(state, "deactivate")
        await _broadcast_current_wake_debug()
    return _public_state(state)


@router.post("/policy")
async def voice_mode_policy(body: VoiceModePolicy):
    async with _state_lock:
        state = _read_state_unlocked()
        policy = _clean_policy(state.get("policy"))
        if body.tts_companion_model_preference is not None:
            policy["tts_companion_model_preference"] = _clean_model_preference(
                body.tts_companion_model_preference
            )
        now = time.time()
        state["policy"] = policy
        state["revision"] = now
        state["updated_at"] = now
        _write_state_unlocked(state)
    await _publish_changed(state, "policy")
    return _public_state(state)


@router.get("/wake-settings")
async def voice_mode_wake_settings() -> dict[str, Any]:
    async with _state_lock:
        policy = _clean_policy(_read_state_unlocked().get("policy"))
    return {
        "ok": True,
        "wake_to_talk": policy["wake_to_talk"],
        "stt": policy["stt"],
    }


@router.get("/wake-debug")
async def voice_mode_wake_debug() -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_debug_unlocked()
    return _public_wake_debug(state, debug)


@router.get("/wake-debug/stream")
async def voice_mode_wake_debug_stream(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _wake_debug_stream(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post("/wake-debug")
async def voice_mode_update_wake_debug(body: WakeDebugBody):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})
    now = time.time()
    report = {
        "browser_id": browser_id,
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "tab_id": _clean_string(body.tab_id, "", 120),
        "running": bool(body.running),
        "starting": bool(body.starting),
        "reason": _clean_string(body.reason, "", 240),
        "fsm_state": _clean_string(body.fsm_state, "", 80),
        "session_id": int(body.session_id or 0),
        "active_instance_id": _clean_string(body.active_instance_id, "", 40),
        "active_send": _bounded_json(body.active_send or {}),
        "queues": _bounded_json(body.queues or {}),
        "transcript": _clean_string(body.transcript, "", 4000),
        "frozen_send_snapshot": _bounded_json(body.frozen_send_snapshot or {}),
        "command_diagnostics": _bounded_json(body.command_diagnostics or {}),
        "last_action": _bounded_json(body.last_action or {}),
        "recent_actions": _bounded_json((body.recent_actions or [])[-40:]),
        "recent_stt_events": _bounded_json((body.recent_stt_events or [])[-80:]),
        "stream_epoch": int(body.stream_epoch or 0),
        "audio_frames_sent": int(body.audio_frames_sent or 0),
        "audio_frames_captured": int(body.audio_frames_captured or 0),
        "audio_timing": _bounded_json(body.audio_timing or {}),
        "stt_reset_pending_reason": _clean_string(body.stt_reset_pending_reason, "", 120),
        "stt_speech_start_reset_pending": bool(body.stt_speech_start_reset_pending),
        "vad_speech_start_reset_armed": bool(body.vad_speech_start_reset_armed),
        "audio_delay_frames": int(body.audio_delay_frames or 0),
        "audio_candidate_frames": int(body.audio_candidate_frames or 0),
        "stt_delay_frames": int(body.stt_delay_frames or 0),
        "stt_segment_active": bool(body.stt_segment_active),
        "stt_segment": _bounded_json(body.stt_segment or {}),
        "audio_features": _bounded_json(body.audio_features or {}),
        "vad": _bounded_json(body.vad or {}),
        "client_now_ms": float(body.client_now_ms or 0),
        "reported_at": now,
    }
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_debug_unlocked()
        reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
        reports[browser_id] = report
        debug = {
            "reports": reports,
            "updated_at": now,
        }
        _write_wake_debug_unlocked(debug)
        public = _public_wake_debug(state, debug)
    await _broadcast_wake_debug(public)
    return public


@router.get("/dev-status")
async def voice_mode_dev_status() -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_dev_debug_unlocked()
    return _public_wake_dev_debug(state, debug)


@router.post("/dev-status")
async def voice_mode_update_dev_status(body: WakeDevDebugBody):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})
    now = time.time()
    report = {
        "browser_id": browser_id,
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "tab_id": _clean_string(body.tab_id, "", 120),
        "mode": _clean_dev_command_mode(body.mode),
        "source": _clean_string(body.source, "", 120),
        "status": _clean_string(body.status, "", 240),
        "transcript": _clean_string(body.transcript, "", 4000),
        "snapshot": _bounded_json(body.snapshot or {}, 50000),
        "client_now_ms": float(body.client_now_ms or 0),
        "reported_at": now,
    }
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_dev_debug_unlocked()
        reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
        reports[browser_id] = report
        debug = {
            "reports": reports,
            "updated_at": now,
        }
        _write_wake_dev_debug_unlocked(debug)
        public = _public_wake_dev_debug(state, debug)
    return public


@router.post("/dev-command")
async def voice_mode_dev_command(body: VoiceDevCommandBody):
    """Publish a browser-directed Wake/VAD dev command over the SSE bus."""
    mode = _clean_dev_command_mode(body.mode)
    action = _clean_dev_command_action(body.action)
    if mode not in _DEV_COMMAND_MODES:
        return JSONResponse(status_code=400, content={"ok": False, "detail": f"Unsupported mode: {mode or 'blank'}"})
    if action not in _DEV_COMMAND_ACTIONS:
        return JSONResponse(status_code=400, content={"ok": False, "detail": f"Unsupported action: {action or 'blank'}"})
    command_id = _clean_dev_command_id(body.command_id)
    explicit_browser_id = _clean_browser_id(body.browser_id)
    explicit_tab_id = _clean_string(body.tab_id, "", 120)
    async with _state_lock:
        state = _read_state_unlocked()
        active = state.get("active") if isinstance(state.get("active"), dict) else None

    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    target_browser_id = explicit_browser_id or (active_browser_id if body.target_active_browser else "")
    if body.target_active_browser and not target_browser_id:
        return JSONResponse(status_code=409, content={"ok": False, "detail": "No active browser is available for Voice Mode"})

    now = time.time()
    payload = {
        "schema": "xarta.voice_mode.dev_command.v1",
        "command_id": command_id,
        "mode": mode,
        "action": action,
        "target_browser_id": target_browser_id,
        "target_tab_id": explicit_tab_id,
        "active_browser_id": active_browser_id,
        "open_modal": bool(body.open_modal),
        "created_at": now,
        "max_age_seconds": int(body.max_age_seconds),
    }
    event = AppEvent.create(
        _VOICE_DEV_COMMAND_EVENT_TYPE,
        "Voice Mode Dev Command",
        f"Voice Mode dev command {mode}:{action}.",
        severity="info",
        source="blueprints-voice-mode",
        payload=payload,
        event_id=f"voice-dev-command-{command_id}",
    )
    published = await publish_event(event)
    return {
        "ok": True,
        "event": published.model_dump(),
        "payload": payload,
    }


@router.post("/wake-settings")
async def voice_mode_update_wake_settings(body: WakeSettingsBody):
    async with _state_lock:
        state = _read_state_unlocked()
        policy = _clean_policy(state.get("policy"))
        if body.wake_to_talk is not None:
            policy["wake_to_talk"] = _clean_wake_to_talk_policy(body.wake_to_talk)
        if body.stt is not None:
            policy["stt"] = _clean_stt_policy(body.stt)
        now = time.time()
        state["policy"] = policy
        state["revision"] = now
        state["updated_at"] = now
        _write_state_unlocked(state)
    await _publish_changed(state, "wake-settings")
    return {
        **_public_state(state),
        "wake_to_talk": policy["wake_to_talk"],
        "stt": policy["stt"],
    }


@router.websocket("/stt/ws")
async def voice_mode_stt_websocket(websocket: WebSocket) -> None:
    await _matrix_chat_stt_relay(
        websocket,
        room_id=None,
        send_matrix_transcript=False,
        return_enhanced_audio=False,
    )


def _aggregation_timeout_url() -> str:
    if not _PIPECAT_API_BASE:
        return ""
    return f"{_PIPECAT_API_BASE}{_AGGREGATION_TIMEOUT_PATH}"


def _aggregation_timeout_payload(ms: int) -> dict[str, Any]:
    clean_ms = _clean_int_step(
        ms,
        fallback=_AGGREGATION_TIMEOUT_DEFAULT_MS,
        minimum=_AGGREGATION_TIMEOUT_MIN_MS,
        maximum=_AGGREGATION_TIMEOUT_MAX_MS,
        step=_AGGREGATION_TIMEOUT_STEP_MS,
    )
    return {
        "aggregation_timeout": clean_ms / 1000.0,
        "aggregation_timeout_ms": clean_ms,
    }


@router.get("/stt/aggregation-timeout")
async def voice_mode_get_aggregation_timeout() -> dict[str, Any]:
    url = _aggregation_timeout_url()
    if not url:
        return {
            "ok": False,
            "supported": False,
            "detail": "VOICE_MODE_PIPECAT_API_BASE is not configured",
            "url": "",
        }
    try:
        async with httpx.AsyncClient(timeout=2.5, verify=_PIPECAT_VERIFY_TLS) as client:
            response = await client.get(url)
        payload = response.json() if response.content else {}
        if not response.is_success:
            return {
                "ok": False,
                "supported": False,
                "detail": f"HTTP {response.status_code}",
                "url": url,
            }
    except Exception as exc:
        return {
            "ok": False,
            "supported": False,
            "detail": str(exc)[:160],
            "url": url,
        }
    ms = payload.get("aggregation_timeout_ms")
    if ms is None and payload.get("aggregation_timeout") is not None:
        ms = round(float(payload.get("aggregation_timeout")) * 1000)
    return {
        "ok": True,
        "supported": True,
        "aggregation_timeout_ms": _clean_int_step(
            ms,
            fallback=_AGGREGATION_TIMEOUT_DEFAULT_MS,
            minimum=_AGGREGATION_TIMEOUT_MIN_MS,
            maximum=_AGGREGATION_TIMEOUT_MAX_MS,
            step=_AGGREGATION_TIMEOUT_STEP_MS,
        ),
        "min_ms": _AGGREGATION_TIMEOUT_MIN_MS,
        "max_ms": _AGGREGATION_TIMEOUT_MAX_MS,
        "step_ms": _AGGREGATION_TIMEOUT_STEP_MS,
        "url": url,
    }


@router.post("/stt/aggregation-timeout")
async def voice_mode_set_aggregation_timeout(body: AggregationTimeoutBody) -> dict[str, Any]:
    payload = _aggregation_timeout_payload(body.aggregation_timeout_ms)
    url = _aggregation_timeout_url()
    if not url:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "supported": False,
                "detail": "VOICE_MODE_PIPECAT_API_BASE is not configured",
                "url": "",
            },
        )
    try:
        async with httpx.AsyncClient(timeout=3.0, verify=_PIPECAT_VERIFY_TLS) as client:
            response = await client.post(url, json={"aggregation_timeout": payload["aggregation_timeout"]})
        response_payload = response.json() if response.content else {}
        if not response.is_success:
            return JSONResponse(
                status_code=502,
                content={"ok": False, "supported": False, "detail": f"HTTP {response.status_code}", "url": url},
            )
    except Exception as exc:
        return JSONResponse(
            status_code=502,
            content={"ok": False, "supported": False, "detail": str(exc)[:160], "url": url},
        )

    async with _state_lock:
        state = _read_state_unlocked()
        policy = _clean_policy(state.get("policy"))
        policy["stt"]["speech_aggregation_timeout_ms"] = payload["aggregation_timeout_ms"]
        now = time.time()
        state["policy"] = policy
        state["revision"] = now
        state["updated_at"] = now
        _write_state_unlocked(state)
    await _publish_changed(state, "aggregation-timeout")
    return {
        "ok": True,
        "supported": True,
        "aggregation_timeout_ms": _clean_int_step(
            response_payload.get("aggregation_timeout_ms", payload["aggregation_timeout_ms"]),
            fallback=payload["aggregation_timeout_ms"],
            minimum=_AGGREGATION_TIMEOUT_MIN_MS,
            maximum=_AGGREGATION_TIMEOUT_MAX_MS,
            step=_AGGREGATION_TIMEOUT_STEP_MS,
        ),
        "min_ms": _AGGREGATION_TIMEOUT_MIN_MS,
        "max_ms": _AGGREGATION_TIMEOUT_MAX_MS,
        "step_ms": _AGGREGATION_TIMEOUT_STEP_MS,
        "url": url,
    }
