"""Node-local Active Browser, voice-mode, and Wake/VAD development endpoints."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import httpx
import websockets
from fastapi import APIRouter, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .db import get_conn, get_setting
from .events import AppEvent
from .routes_events import publish_event
from .routes_matrix_chat import _matrix_chat_stt_relay
from .routes_matrix_chat import _settings as _matrix_chat_settings
from .routes_ui_cache import _read_status as _read_fallback_ui_cache_status

router = APIRouter(prefix="/voice-mode", tags=["voice-mode"])


def _bounded_int_env(name: str, fallback: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(fallback)) or fallback)
    except (TypeError, ValueError):
        value = fallback
    return max(minimum, min(value, maximum))


def _bounded_float_env(name: str, fallback: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(fallback)) or fallback)
    except (TypeError, ValueError):
        value = fallback
    return max(minimum, min(value, maximum))


def _truthy_env(name: str, fallback: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return fallback
    return raw.strip().lower() in {"1", "true", "yes", "on"}


_STATE_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-voice-mode.json")
_WAKE_DEV_DEBUG_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-wake-dev-debug.json")
_state_lock = asyncio.Lock()
_STATE_CACHE: dict[str, Any] | None = None
_WAKE_DEV_DEBUG_CACHE: dict[str, Any] | None = None
_STATE_LAST_PERSISTED_AT = 0.0
_WAKE_DEV_DEBUG_LAST_PERSISTED_AT = 0.0
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
_PRE_ROLL_FRAMES_MIN = 1
_PRE_ROLL_FRAMES_MAX = 4
_PRE_ROLL_FRAMES_STEP = 1
_PRE_ROLL_FRAMES_DEFAULT = 1
_SILENCE_RESET_TIMEOUT_MIN_MS = 0
_SILENCE_RESET_TIMEOUT_MAX_MS = 3000
_SILENCE_RESET_TIMEOUT_STEP_MS = 300
_SILENCE_RESET_TIMEOUT_DEFAULT_MS = 2100
_WORD_DETECTION_PAYLOAD0_TIMEOUT_MIN_MS = 0
_WORD_DETECTION_PAYLOAD0_TIMEOUT_MAX_MS = 3000
_WORD_DETECTION_PAYLOAD0_TIMEOUT_STEP_MS = 300
_WORD_DETECTION_PAYLOAD0_TIMEOUT_DEFAULT_MS = 0
_WORD_DETECTION_CUE_SOUND_MAX_LENGTH = 255
_VOICE_DEV_COMMAND_EVENT_TYPE = "voice.mode.dev.command"
_ACTIVE_BROWSER_COMMAND_EVENT_TYPE = "blueprints.active_browser.command"
_BROWSER_VIEW_MAX_REPORTS = 32
_ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MIN_SECONDS = 1
_ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MAX_SECONDS = 120
_ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_DEFAULT_SECONDS = _bounded_int_env(
    "ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_SECONDS",
    10,
    _ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MIN_SECONDS,
    _ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MAX_SECONDS,
)
_ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS = _bounded_int_env(
    "ACTIVE_BROWSER_CLIENT_MAX_AGE_SECONDS",
    30,
    1,
    3600,
)
_BROWSER_VIEW_TELEMETRY_PERSIST_INTERVAL_SECONDS = _bounded_float_env(
    "VOICE_MODE_BROWSER_VIEW_PERSIST_INTERVAL_SECONDS",
    2.0,
    0.0,
    60.0,
)
_DEV_STATUS_TELEMETRY_PERSIST_INTERVAL_SECONDS = _bounded_float_env(
    "VOICE_MODE_DEV_STATUS_PERSIST_INTERVAL_SECONDS",
    2.0,
    0.0,
    60.0,
)
_VOICE_MODE_HOT_POST_FULL_RESPONSE = _truthy_env("VOICE_MODE_HOT_POST_FULL_RESPONSE")
_ACTIVE_BROWSER_VIEWPORT_THRESHOLDS = {
    # Provisional first-pass thresholds. Keep raw dimensions in reports so these
    # can be tuned against the actual operator monitors and handheld devices.
    "mobile_short_side_max_px": 767,
    "mobile_long_side_max_px": 1180,
    "touch_mobile_short_side_max_px": 900,
    "touch_mobile_long_side_max_px": 1400,
    "desktop_min_landscape_width_px": 900,
    "standard_landscape_min_aspect": 1.45,
    "standard_landscape_max_aspect": 1.85,
    "widescreen_min_aspect": 1.86,
}
_DEV_COMMAND_SURFACES = {"wake_dev", "vad_dev"}
_DEV_COMMAND_MODES = {"manual", "vad", "vad_rearm"}
_DEV_COMMAND_ACTIONS = {
    "enable_test",
    "disable_test",
    "record",
    "stop",
    "clear",
    "enable_vad_record",
    "disable_vad_record",
    "toggle_vad_record",
    "enable_vad_stop",
    "disable_vad_stop",
    "toggle_vad_stop",
    "set_noise_reduction",
    "set_noise_level",
    "set_noise_level_db",
    "set_aggregation_timeout",
    "set_vad_reset_timeout",
    "set_silero_vad",
    "set_vad_detector",
    "set_vad_interrupt_tts",
    "set_vad_interrupt_tts_enabled",
    "set_word_detection_match_interrupt_tts",
    "set_word_detection_match_interrupt_tts_enabled",
    "set_word_detection_prefix_partial_interrupt_tts",
    "set_word_detection_prefix_partial_interrupt_tts_enabled",
    "set_word_detection_prefix_final_interrupt_tts",
    "set_word_detection_prefix_final_interrupt_tts_enabled",
    "set_word_detection_payload0_timeout",
    "set_word_detection_payload0_timeout_ms",
    "set_vad_payload0_timeout",
    "set_vad_payload0_timeout_ms",
    "set_word_detection_match_cue",
    "set_word_detection_match_cue_enabled",
    "set_word_detection_match_cue_sound",
    "set_word_detection_payload0_timeout_cue",
    "set_word_detection_payload0_timeout_cue_enabled",
    "set_word_detection_payload0_timeout_cue_sound",
    "set_word_detection_agent_candidate_cue",
    "set_word_detection_agent_candidate_cue_enabled",
    "set_word_detection_agent_candidate_cue_sound",
    "set_auto_pre_roll",
    "set_always_pre_roll",
    "set_pre_roll_frames",
    "set_num_pre_roll",
    "set_num_pre_roll_frames",
    "set_word_detection_aliases",
    "set_word_detection_words",
    "set_sense_word",
    "set_sense_words",
    "set_wake_to_talk",
    "set_wake_to_talk_enabled",
    "set_stt_mode",
    "set_noise_threshold",
    "set_noise_threshold_db",
    "set_vad_pre_roll",
    "set_vad_pre_roll_db",
    "set_vad_pre_roll_threshold",
}
_ACTIVE_BROWSER_COMMAND_ACTIONS = {
    "hard_refresh",
    "open_chat",
    "open_vad_dev",
    "close_vad_dev",
    "close_modal",
    "open_page",
    "open_modal",
    "open_doc",
    "menu_function",
    "open_synthesis",
    "open_probes",
    "open_settings",
    "selector_action",
    "set_body_shade",
}
_ACTIVE_BROWSER_COMMAND_ALIASES = {
    "refresh": "hard_refresh",
    "reload": "hard_refresh",
    "app_refresh": "hard_refresh",
    "refresh_app": "hard_refresh",
    "chat": "open_chat",
    "vad_dev": "open_vad_dev",
    "close_vad": "close_vad_dev",
    "vad_close": "close_vad_dev",
    "modal_close": "close_modal",
    "page": "open_page",
    "open_tab": "open_page",
    "tab": "open_page",
    "modal": "open_modal",
    "doc": "open_doc",
    "document": "open_doc",
    "fn": "menu_function",
    "function": "menu_function",
    "menu_fn": "menu_function",
    "synthesis": "open_synthesis",
    "probes": "open_probes",
    "settings": "open_settings",
    "selector": "selector_action",
    "body_shade": "set_body_shade",
    "body shade": "set_body_shade",
    "shade": "set_body_shade",
    "shade_up": "set_body_shade",
    "shade up": "set_body_shade",
}
_ACTIVE_BROWSER_EVENT_KIND_ALIASES = {
    "": "click",
    "tap": "click",
    "single": "click",
    "single_click": "click",
    "dblclick": "double_click",
    "double": "double_click",
    "double_tap": "double_click",
    "long": "long_press",
    "hold": "long_press",
    "long_tap": "long_press",
}
_ACTIVE_BROWSER_EVENT_KINDS = {"click", "double_click", "long_press"}
_ACTIVE_BROWSER_BODY_SHADE_STATES = {"up", "down", "toggle"}


class BrowserVoiceState(BaseModel):
    browser_id: str
    browser_label: str | None = None
    tab_id: str | None = None
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


class VoiceDevCommandBody(BaseModel):
    surface: str = "wake_dev"
    mode: str = "manual"
    action: str = "record"
    browser_id: str | None = None
    tab_id: str | None = None
    command_id: str | None = None
    value: Any | None = None
    enabled: bool | None = None
    wake_to_talk_enabled: bool | None = None
    stt_mode: str | None = None
    silero_vad_enabled: bool | None = None
    vad_interrupt_tts_enabled: bool | None = None
    word_detection_match_interrupt_tts_enabled: bool | None = None
    word_detection_prefix_partial_interrupt_tts_enabled: bool | None = None
    word_detection_prefix_final_interrupt_tts_enabled: bool | None = None
    word_detection_payload0_timeout_ms: int | None = None
    vad_payload0_timeout_ms: int | None = None
    word_detection_match_cue_enabled: bool | None = None
    word_detection_match_cue_sound: str | None = None
    word_detection_payload0_timeout_cue_enabled: bool | None = None
    word_detection_payload0_timeout_cue_sound: str | None = None
    word_detection_agent_candidate_cue_enabled: bool | None = None
    word_detection_agent_candidate_cue_sound: str | None = None
    auto_pre_roll_enabled: bool | None = None
    level_db: float | None = None
    noise_level_db: float | None = None
    noise_threshold_db: float | None = None
    threshold_db: float | None = None
    vad_pre_roll_db: float | None = None
    vad_pre_roll_threshold_db: float | None = None
    aggregation_timeout_ms: int | None = None
    speech_aggregation_timeout_ms: int | None = None
    vad_reset_timeout_ms: int | None = None
    reset_timeout_ms: int | None = None
    pre_roll_frames: int | None = None
    num_pre_roll: int | None = None
    num_pre_roll_frames: int | None = None
    always_pre_roll_enabled: bool | None = None
    word_detection_aliases: str | None = None
    sense_words: str | None = None
    open_modal: bool = False
    target_active_browser: bool = True
    max_age_seconds: int = Field(default=60, ge=5, le=300)


class ActiveBrowserCommandBody(BaseModel):
    action: str = "hard_refresh"
    browser_id: str | None = None
    tab_id: str | None = None
    command_id: str | None = None
    group: str | None = None
    menu_group: str | None = None
    page_id: str | None = None
    tab: str | None = None
    menu_id: str | None = None
    menu_item_id: str | None = None
    fn: str | None = None
    modal_id: str | None = None
    doc_id: str | None = None
    path: str | None = None
    doc_path: str | None = None
    highlight_terms: list[str] | None = None
    selector_action: str | None = None
    event_kind: str | None = None
    body_shade: str | None = None
    shade: str | None = None
    instant: bool | None = None
    target_active_browser: bool = True
    max_age_seconds: int = Field(default=60, ge=5, le=300)


class BrowserViewBody(BaseModel):
    browser_id: str
    browser_label: str | None = None
    tab_id: str | None = None
    page: dict[str, Any] | None = None
    modals: list[dict[str, Any]] | None = None
    viewport: dict[str, Any] | None = None
    voice: dict[str, Any] | None = None
    visibility_state: str | None = None
    has_focus: bool = False
    url_path: str | None = None
    url_search: str | None = None
    url_hash: str | None = None
    frontend: dict[str, Any] | None = None
    automation: dict[str, Any] | None = None
    docs: dict[str, Any] | None = None
    body_shade: dict[str, Any] | None = None
    tts: dict[str, Any] | None = None
    client_now_ms: float | None = None


class BrowserClientSelectionBody(BaseModel):
    browser_id: str | None = None
    tab_id: str | None = None
    max_age_seconds: int = Field(
        default=_ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS,
        ge=1,
        le=3600,
    )
    tts_enabled: bool | None = None
    stt_enabled: bool | None = None
    stt_mode: str | None = None


class WakeDevDebugBody(BaseModel):
    browser_id: str
    browser_label: str | None = None
    tab_id: str | None = None
    surface: str | None = None
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
                "pre_roll_frames": _PRE_ROLL_FRAMES_DEFAULT,
                "silero_vad_enabled": False,
                "vad_interrupt_tts_enabled": False,
                "word_detection_match_interrupt_tts_enabled": False,
                "word_detection_prefix_partial_interrupt_tts_enabled": False,
                "word_detection_prefix_final_interrupt_tts_enabled": False,
                "word_detection_payload0_timeout_ms": _WORD_DETECTION_PAYLOAD0_TIMEOUT_DEFAULT_MS,
                "word_detection_match_cue_enabled": False,
                "word_detection_match_cue_sound": "",
                "word_detection_payload0_timeout_cue_enabled": False,
                "word_detection_payload0_timeout_cue_sound": "",
                "word_detection_agent_candidate_cue_enabled": False,
                "word_detection_agent_candidate_cue_sound": "",
                "always_pre_roll_enabled": False,
                "silence_reset_timeout_ms": _SILENCE_RESET_TIMEOUT_DEFAULT_MS,
            },
        },
        "browser_views": {},
        "browser_view_updated_at": 0.0,
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


def _clean_sound_asset_path(value: Any) -> str:
    raw = str(value or "").strip().replace("\\", "/")
    clean = "".join(ch for ch in raw if ch >= " " and ch != "\x7f")
    return clean[:_WORD_DETECTION_CUE_SOUND_MAX_LENGTH]


def _clean_dev_command_id(value: str | None = None) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ":", "."})[:100]
    return clean or f"voice-dev-{uuid.uuid4().hex}"


def _clean_active_browser_command_id(value: str | None = None) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ":", "."})[:100]
    return clean or f"active-browser-{uuid.uuid4().hex}"


def _clean_dev_command_mode(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _clean_dev_command_surface(value: str | None) -> str:
    clean = _clean_dev_command_mode(value)
    return clean or "wake_dev"


def _clean_dev_command_action(value: str | None) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _clean_active_browser_command_action(value: str | None) -> str:
    action = _clean_dev_command_action(value)
    return _ACTIVE_BROWSER_COMMAND_ALIASES.get(action, action)


def _clean_active_browser_event_kind(value: str | None) -> str:
    event_kind = _clean_dev_command_action(value)
    event_kind = _ACTIVE_BROWSER_EVENT_KIND_ALIASES.get(event_kind, event_kind)
    return event_kind if event_kind in _ACTIVE_BROWSER_EVENT_KINDS else "click"


def _clean_active_browser_body_shade(value: str | None) -> str:
    raw = _clean_dev_command_action(value)
    aliases = {
        "": "",
        "raise": "up",
        "raised": "up",
        "open": "up",
        "opened": "up",
        "on": "up",
        "true": "up",
        "1": "up",
        "up": "up",
        "lower": "down",
        "lowered": "down",
        "close": "down",
        "closed": "down",
        "off": "down",
        "false": "down",
        "0": "down",
        "down": "down",
        "toggle": "toggle",
        "flip": "toggle",
    }
    state = aliases.get(raw, raw)
    if not state:
        return ""
    return state if state in _ACTIVE_BROWSER_BODY_SHADE_STATES else "up"


def _clean_active_browser_modal_id(value: str | None) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return clean[:120]


def _clean_active_browser_selector_action(value: str | None) -> str:
    raw = str(value or "").strip().lower().replace("_", "-").replace(" ", "-")
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", ".", ":"})
    return clean[:120]


def _clean_active_browser_group(value: str | None) -> str:
    raw = str(value or "").strip().lower().replace(" ", "-").replace("_", "-")
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-"})
    return clean[:80]


def _clean_active_browser_token(value: Any, *, max_length: int = 160) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return clean[:max_length]


def _clean_active_browser_page_id(value: str | None) -> str:
    return _clean_active_browser_token(value, max_length=160)


def _clean_active_browser_menu_item_id(value: str | None) -> str:
    return _clean_active_browser_token(value, max_length=160)


def _clean_active_browser_fn_key(value: str | None) -> str:
    return _clean_active_browser_token(value, max_length=160)


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
    values: list[Any] = []
    values.extend(str(wake_word or "").split(";"))
    if isinstance(configured, list):
        values.extend(configured)
    for value in values:
        normalized = " ".join(
            str(value or "")
            .strip()
            .lower()
            .replace("-", " ")
            .replace(",", " ")
            .replace(".", " ")
            .split()
        )
        compact = normalized.replace(" ", "")
        hyphenated = normalized.replace(" ", "-")
        for candidate in (normalized, compact, hyphenated):
            if candidate and candidate not in aliases:
                aliases.append(candidate)
    return aliases[:16]


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
        "auto_execute_silence_ms": 0,
        "execute_cancel_ms": 0,
        "partial_settle_ms": 0,
        "commands": {
            "pause": "pause-dictation",
            "execute": "execute",
            "resume": "resume-dictation",
            "cancel": "cancel-dictation",
        },
    }


def _default_wake_to_talk_policy() -> dict[str, Any]:
    return {
        "instances": {
            "local": _default_wake_instance(
                label="hermes-local",
                matrix_server="tb1",
                wake_word="Computer",
                hermes_prefix="hermes: ",
            ),
            "vps": _default_wake_instance(
                label="hermes-VPS",
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
        "execute": "execute",
        "resume": "resume-dictation",
        "cancel": "cancel-dictation",
    }
    return {
        key: _clean_string(commands.get(key), fallback, 80) for key, fallback in defaults.items()
    }


def _clean_wake_step_ms(value: Any, fallback: int = 0) -> int:
    if str(value).strip().lower() in {"", "0", "false", "off", "disabled"}:
        return 0
    return _clean_int_step(
        value,
        fallback=fallback or 300,
        minimum=300,
        maximum=3000,
        step=300,
    )


def _clean_wake_instance(instance_id: str, value: Any) -> dict[str, Any]:
    defaults = _default_wake_to_talk_policy()["instances"][instance_id]
    raw = value if isinstance(value, dict) else {}
    matrix_server = _clean_string(raw.get("matrix_server"), defaults["matrix_server"], 16).lower()
    if matrix_server not in {"tb1", "vps"} or matrix_server != defaults["matrix_server"]:
        matrix_server = defaults["matrix_server"]
    wake_word = _clean_string(raw.get("wake_word"), defaults["wake_word"], 160)
    auto_execute = _clean_wake_step_ms(
        raw.get("auto_execute_silence_ms", defaults["auto_execute_silence_ms"]),
        defaults["auto_execute_silence_ms"],
    )
    execute_cancel = _clean_wake_step_ms(
        raw.get("execute_cancel_ms", defaults["execute_cancel_ms"]),
        defaults["execute_cancel_ms"],
    )
    partial_settle = _clean_wake_step_ms(
        raw.get(
            "partial_settle_ms",
            raw.get("partial_settle_timeout_ms", defaults["partial_settle_ms"]),
        ),
        defaults["partial_settle_ms"],
    )
    return {
        # Wake instance activation is controlled by the browser's Wake-to-Talk
        # STT mode plus backend activated-browser state. Keep this field true for
        # compatibility with earlier persisted settings, but do not expose it
        # as a second user-facing enable switch.
        "enabled": True,
        "label": defaults["label"],
        "matrix_server": matrix_server,
        "matrix_room_id": _clean_string(raw.get("matrix_room_id"), defaults["matrix_room_id"], 255),
        "wake_word": wake_word,
        "wake_aliases": _wake_aliases(wake_word, raw.get("wake_aliases")),
        "hermes_prefix": _clean_hermes_prefix(raw.get("hermes_prefix"), defaults["hermes_prefix"]),
        "auto_execute_silence_ms": auto_execute,
        "execute_cancel_ms": execute_cancel,
        "partial_settle_ms": partial_settle,
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
    prefix_partial_interrupt_tts = _clean_bool(
        raw.get(
            "word_detection_prefix_partial_interrupt_tts_enabled",
            raw.get("match_prefix_partial_interrupt_tts"),
        ),
        fallback=False,
    )
    prefix_final_interrupt_tts = _clean_bool(
        raw.get(
            "word_detection_prefix_final_interrupt_tts_enabled",
            raw.get("match_prefix_final_interrupt_tts"),
        ),
        fallback=False,
    )
    if prefix_partial_interrupt_tts and prefix_final_interrupt_tts:
        prefix_partial_interrupt_tts = False
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
        "pre_roll_frames": _clean_int_step(
            raw.get("pre_roll_frames", raw.get("num_pre_roll_frames", raw.get("num_pre_roll"))),
            fallback=_PRE_ROLL_FRAMES_DEFAULT,
            minimum=_PRE_ROLL_FRAMES_MIN,
            maximum=_PRE_ROLL_FRAMES_MAX,
            step=_PRE_ROLL_FRAMES_STEP,
        ),
        "silero_vad_enabled": _clean_bool(
            raw.get("silero_vad_enabled", raw.get("silero_enabled")),
            fallback=False,
        ),
        "vad_interrupt_tts_enabled": _clean_bool(
            raw.get("vad_interrupt_tts_enabled", raw.get("vad_interrupt_tts")),
            fallback=False,
        ),
        "word_detection_match_interrupt_tts_enabled": _clean_bool(
            raw.get(
                "word_detection_match_interrupt_tts_enabled",
                raw.get("match_interrupt_tts"),
            ),
            fallback=False,
        ),
        "word_detection_prefix_partial_interrupt_tts_enabled": prefix_partial_interrupt_tts,
        "word_detection_prefix_final_interrupt_tts_enabled": prefix_final_interrupt_tts,
        "word_detection_payload0_timeout_ms": _clean_int_step(
            raw.get(
                "word_detection_payload0_timeout_ms",
                raw.get("vad_payload0_timeout_ms", raw.get("payload0_timeout_ms")),
            ),
            fallback=_WORD_DETECTION_PAYLOAD0_TIMEOUT_DEFAULT_MS,
            minimum=_WORD_DETECTION_PAYLOAD0_TIMEOUT_MIN_MS,
            maximum=_WORD_DETECTION_PAYLOAD0_TIMEOUT_MAX_MS,
            step=_WORD_DETECTION_PAYLOAD0_TIMEOUT_STEP_MS,
        ),
        "word_detection_match_cue_enabled": _clean_bool(
            raw.get(
                "word_detection_match_cue_enabled",
                raw.get("word_detection_match_sound_enabled"),
            ),
            fallback=False,
        ),
        "word_detection_match_cue_sound": _clean_sound_asset_path(
            raw.get(
                "word_detection_match_cue_sound",
                raw.get("word_detection_match_sound_path", raw.get("word_detection_match_sound")),
            )
        ),
        "word_detection_payload0_timeout_cue_enabled": _clean_bool(
            raw.get(
                "word_detection_payload0_timeout_cue_enabled",
                raw.get("word_detection_payload0_timeout_sound_enabled"),
            ),
            fallback=False,
        ),
        "word_detection_payload0_timeout_cue_sound": _clean_sound_asset_path(
            raw.get(
                "word_detection_payload0_timeout_cue_sound",
                raw.get(
                    "word_detection_payload0_timeout_sound_path",
                    raw.get("word_detection_payload0_timeout_sound"),
                ),
            )
        ),
        "word_detection_agent_candidate_cue_enabled": _clean_bool(
            raw.get(
                "word_detection_agent_candidate_cue_enabled",
                raw.get("word_detection_agent_candidate_sound_enabled"),
            ),
            fallback=False,
        ),
        "word_detection_agent_candidate_cue_sound": _clean_sound_asset_path(
            raw.get(
                "word_detection_agent_candidate_cue_sound",
                raw.get(
                    "word_detection_agent_candidate_sound_path",
                    raw.get("word_detection_agent_candidate_sound"),
                ),
            )
        ),
        "always_pre_roll_enabled": _clean_bool(
            raw.get("always_pre_roll_enabled", raw.get("always_pre_roll")),
            fallback=False,
        ),
        "silence_reset_timeout_ms": _clean_int_step(
            raw.get("silence_reset_timeout_ms"),
            fallback=_SILENCE_RESET_TIMEOUT_DEFAULT_MS,
            minimum=_SILENCE_RESET_TIMEOUT_MIN_MS,
            maximum=_SILENCE_RESET_TIMEOUT_MAX_MS,
            step=_SILENCE_RESET_TIMEOUT_STEP_MS,
        ),
    }


def _clean_stt_policy_update(current: Any, patch: Any) -> dict[str, Any]:
    """Clean an STT policy patch while preserving unspecified current values."""
    merged = {
        **_clean_stt_policy(current),
        **(patch if isinstance(patch, dict) else {}),
    }
    return _clean_stt_policy(merged)


def _is_default_stt_reset_payload(value: Any, current: Any) -> bool:
    """Detect stale clients submitting a full default STT policy with a wake-only save."""
    if not isinstance(value, dict) or not value:
        return False
    incoming = _clean_stt_policy(value)
    default = _clean_stt_policy({})
    current_clean = _clean_stt_policy(current)
    return incoming == default and current_clean != default


def _clean_bool(value: Any, *, fallback: bool = False) -> bool:
    if value is None:
        return bool(fallback)
    if isinstance(value, bool):
        return value
    return _truthy(value)


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


async def _probe_http_json(
    url: str, timeout_seconds: float = _PROBE_TIMEOUT_SECONDS
) -> dict[str, Any]:
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


async def _probe_websocket_open(
    url: str, timeout_seconds: float = _PROBE_TIMEOUT_SECONDS
) -> dict[str, Any]:
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
                return {
                    "ok": False,
                    "status": "bad_response",
                    "error": "bad config response",
                    "body": ack,
                }
            await ws.send(json.dumps({"type": "ping"}))
            pong = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout_seconds))
            if pong.get("type") != "pong":
                return {
                    "ok": False,
                    "status": "bad_response",
                    "error": "bad ping response",
                    "body": pong,
                }
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
    return {
        "ok": False,
        "issue": f"{lxc_label} status unknown",
        "detail": {"lxc": lxc, "lxc_api": pve_health},
    }


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
            return {
                "ok": True,
                "issue": "",
                "detail": {"stack": stack, "modes": {"active_mode": active_mode}},
            }
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
            return _component(
                "tts", "TTS", issue=issue, detail={"probe_url": probe_url, "probe": probe}
            )
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
            return _component(
                "stt",
                "STT",
                issue=issue,
                detail={"ws_url": ws_url, "health_url": health_url, "probe": probe},
            )
        return _component(
            "stt",
            "STT",
            ok=True,
            detail={"ws_url": ws_url, "health_url": health_url, "probe": probe},
        )

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
        return _component(
            "noise_reduction", "Noise reduction", issue=parent["issue"], detail=diagnostic
        )
    stack = await _active_mode_stack_status(_NOISE_STACK_NAMES, machine)
    diagnostic.update(stack.get("detail") or {})
    if stack.get("issue"):
        return _component(
            "noise_reduction", "Noise reduction", issue=stack["issue"], detail=diagnostic
        )
    if stack.get("ok"):
        return _component(
            "noise_reduction",
            "Noise reduction",
            ok=True,
            status="ready",
            detail=diagnostic,
        )
    issue = (
        "noise reduction websocket probe failed"
        if deep_probe
        else "noise reduction stack status unknown"
    )
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


async def _dependency_health_payload(
    force: bool = False, *, deep_noise_probe: bool = False
) -> dict[str, Any]:
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
            _dependency_health_cache.update(
                {
                    "payload": payload,
                    "checked_at": payload["checked_at"],
                    "next_check_seconds": payload["next_check_seconds"],
                }
            )
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
    global _STATE_CACHE
    if isinstance(_STATE_CACHE, dict):
        return _STATE_CACHE
    try:
        raw = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _STATE_CACHE = _empty_state()
        return _STATE_CACHE
    except Exception:
        _STATE_CACHE = _empty_state()
        return _STATE_CACHE
    if not isinstance(raw, dict):
        _STATE_CACHE = _empty_state()
        return _STATE_CACHE
    state = _empty_state()
    state.update(raw)
    if not isinstance(state.get("active"), dict):
        state["active"] = None
    state["policy"] = _clean_policy(state.get("policy"))
    if not isinstance(state.get("browser_views"), dict):
        state["browser_views"] = {}
    state["browser_view_updated_at"] = float(state.get("browser_view_updated_at") or 0.0)
    _STATE_CACHE = state
    return state


def _write_state_unlocked(state: dict[str, Any]) -> None:
    global _STATE_CACHE, _STATE_LAST_PERSISTED_AT
    _STATE_CACHE = state
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_STATE_PATH)
    _STATE_LAST_PERSISTED_AT = time.monotonic()


def _maybe_write_state_telemetry_unlocked(state: dict[str, Any]) -> bool:
    global _STATE_CACHE
    _STATE_CACHE = state
    interval = _BROWSER_VIEW_TELEMETRY_PERSIST_INTERVAL_SECONDS
    now = time.monotonic()
    if (
        interval <= 0.0
        or not _STATE_LAST_PERSISTED_AT
        or now - _STATE_LAST_PERSISTED_AT >= interval
    ):
        _write_state_unlocked(state)
        return True
    return False


def _read_wake_dev_debug_unlocked() -> dict[str, Any]:
    global _WAKE_DEV_DEBUG_CACHE
    if isinstance(_WAKE_DEV_DEBUG_CACHE, dict):
        return _WAKE_DEV_DEBUG_CACHE
    try:
        raw = json.loads(_WAKE_DEV_DEBUG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _WAKE_DEV_DEBUG_CACHE = {"reports": {}, "updated_at": 0.0}
        return _WAKE_DEV_DEBUG_CACHE
    except Exception:
        _WAKE_DEV_DEBUG_CACHE = {"reports": {}, "updated_at": 0.0}
        return _WAKE_DEV_DEBUG_CACHE
    if not isinstance(raw, dict):
        _WAKE_DEV_DEBUG_CACHE = {"reports": {}, "updated_at": 0.0}
        return _WAKE_DEV_DEBUG_CACHE
    reports = raw.get("reports") if isinstance(raw.get("reports"), dict) else {}
    _WAKE_DEV_DEBUG_CACHE = {
        "reports": reports,
        "updated_at": float(raw.get("updated_at") or 0.0),
    }
    return _WAKE_DEV_DEBUG_CACHE


def _write_wake_dev_debug_unlocked(debug: dict[str, Any]) -> None:
    global _WAKE_DEV_DEBUG_CACHE, _WAKE_DEV_DEBUG_LAST_PERSISTED_AT
    _WAKE_DEV_DEBUG_CACHE = debug
    _WAKE_DEV_DEBUG_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _WAKE_DEV_DEBUG_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(debug, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(_WAKE_DEV_DEBUG_PATH)
    _WAKE_DEV_DEBUG_LAST_PERSISTED_AT = time.monotonic()


def _maybe_write_wake_dev_debug_telemetry_unlocked(debug: dict[str, Any]) -> bool:
    global _WAKE_DEV_DEBUG_CACHE
    _WAKE_DEV_DEBUG_CACHE = debug
    interval = _DEV_STATUS_TELEMETRY_PERSIST_INTERVAL_SECONDS
    now = time.monotonic()
    if (
        interval <= 0.0
        or not _WAKE_DEV_DEBUG_LAST_PERSISTED_AT
        or now - _WAKE_DEV_DEBUG_LAST_PERSISTED_AT >= interval
    ):
        _write_wake_dev_debug_unlocked(debug)
        return True
    return False


def _bounded_json(value: Any, max_chars: int = 20000) -> Any:
    try:
        encoded = json.dumps(value)
    except TypeError:
        return None
    if len(encoded) <= max_chars:
        return value
    return {"truncated": True, "chars": len(encoded)}


def _clean_browser_page_int(value: Any, *, maximum: int) -> int:
    try:
        number = int(float(value or 0))
    except (TypeError, ValueError):
        number = 0
    return max(0, min(number, maximum))


def _clean_viewport_number(value: Any, *, maximum: float = 20000.0, decimals: int = 3) -> float:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0.0
    number = max(0.0, min(number, maximum))
    if decimals <= 0:
        return float(int(round(number)))
    return round(number, decimals)


def _clean_viewport_int(value: Any, *, maximum: int = 20000) -> int:
    return int(round(_clean_viewport_number(value, maximum=float(maximum), decimals=0)))


def _clean_browser_viewport(raw: Any) -> dict[str, Any]:
    viewport = raw if isinstance(raw, dict) else {}
    screen = viewport.get("screen") if isinstance(viewport.get("screen"), dict) else {}
    visual = (
        viewport.get("visualViewport") if isinstance(viewport.get("visualViewport"), dict) else {}
    )
    orientation = (
        viewport.get("orientation") if isinstance(viewport.get("orientation"), dict) else {}
    )
    pointer = viewport.get("pointer") if isinstance(viewport.get("pointer"), dict) else {}

    return {
        "innerWidth": _clean_viewport_int(viewport.get("innerWidth")),
        "innerHeight": _clean_viewport_int(viewport.get("innerHeight")),
        "devicePixelRatio": _clean_viewport_number(viewport.get("devicePixelRatio"), maximum=16.0),
        "screen": {
            "width": _clean_viewport_int(screen.get("width")),
            "height": _clean_viewport_int(screen.get("height")),
            "availWidth": _clean_viewport_int(screen.get("availWidth")),
            "availHeight": _clean_viewport_int(screen.get("availHeight")),
        },
        "orientation": {
            "type": _clean_string(orientation.get("type"), "", 80),
            "angle": _clean_viewport_int(orientation.get("angle"), maximum=360),
        },
        "visualViewport": {
            "width": _clean_viewport_number(visual.get("width")),
            "height": _clean_viewport_number(visual.get("height")),
            "scale": _clean_viewport_number(visual.get("scale"), maximum=16.0),
            "offsetLeft": _clean_viewport_number(visual.get("offsetLeft")),
            "offsetTop": _clean_viewport_number(visual.get("offsetTop")),
            "pageLeft": _clean_viewport_number(visual.get("pageLeft")),
            "pageTop": _clean_viewport_number(visual.get("pageTop")),
        },
        "pointer": {
            "primary": _clean_string(pointer.get("primary"), "", 40),
            "any": _clean_string(pointer.get("any"), "", 40),
            "hover": _clean_string(pointer.get("hover"), "", 40),
            "anyHover": _clean_string(pointer.get("anyHover"), "", 40),
            "coarse": bool(pointer.get("coarse")),
            "fine": bool(pointer.get("fine")),
            "touch": bool(pointer.get("touch")),
            "maxTouchPoints": _clean_viewport_int(pointer.get("maxTouchPoints"), maximum=64),
        },
    }


def _classify_browser_viewport(viewport: dict[str, Any]) -> dict[str, Any]:
    width = int(viewport.get("innerWidth") or 0)
    height = int(viewport.get("innerHeight") or 0)
    screen = viewport.get("screen") if isinstance(viewport.get("screen"), dict) else {}
    pointer = viewport.get("pointer") if isinstance(viewport.get("pointer"), dict) else {}
    if width <= 0 or height <= 0:
        return {
            "primary": "unknown",
            "flags": {
                "mobile_portrait": False,
                "mobile_landscape": False,
                "standard_landscape": False,
                "landscape_1080p_like": False,
                "desktop_portrait": False,
                "widescreen": False,
            },
            "provisional": True,
            "thresholds": dict(_ACTIVE_BROWSER_VIEWPORT_THRESHOLDS),
        }

    short_side = min(width, height)
    long_side = max(width, height)
    portrait = height > width
    landscape = width >= height
    aspect = round(width / height, 4) if height else 0.0
    screen_short = min(int(screen.get("width") or 0), int(screen.get("height") or 0))
    effective_short = (
        min(value for value in [short_side, screen_short] if value > 0)
        if screen_short
        else short_side
    )
    touch_like = bool(
        pointer.get("coarse") or pointer.get("touch") or int(pointer.get("maxTouchPoints") or 0) > 0
    )
    thresholds = _ACTIVE_BROWSER_VIEWPORT_THRESHOLDS
    mobile = bool(
        (
            effective_short <= thresholds["mobile_short_side_max_px"]
            and long_side <= thresholds["mobile_long_side_max_px"]
        )
        or (
            touch_like
            and effective_short <= thresholds["touch_mobile_short_side_max_px"]
            and long_side <= thresholds["touch_mobile_long_side_max_px"]
        )
    )
    standard_landscape = bool(
        not mobile
        and landscape
        and width >= thresholds["desktop_min_landscape_width_px"]
        and thresholds["standard_landscape_min_aspect"]
        <= aspect
        <= thresholds["standard_landscape_max_aspect"]
    )
    widescreen = bool(
        not mobile
        and landscape
        and width >= thresholds["desktop_min_landscape_width_px"]
        and aspect >= thresholds["widescreen_min_aspect"]
    )
    flags = {
        "mobile_portrait": bool(mobile and portrait),
        "mobile_landscape": bool(mobile and landscape),
        "standard_landscape": standard_landscape,
        "landscape_1080p_like": standard_landscape,
        "desktop_portrait": bool(not mobile and portrait),
        "widescreen": widescreen,
    }
    primary = "desktop_landscape"
    for name in [
        "mobile_portrait",
        "mobile_landscape",
        "desktop_portrait",
        "widescreen",
        "landscape_1080p_like",
    ]:
        if flags.get(name):
            primary = name
            break

    return {
        "primary": primary,
        "flags": flags,
        "aspect_ratio": aspect,
        "provisional": True,
        "thresholds": dict(thresholds),
    }


def _clean_browser_voice_state(raw: Any) -> dict[str, Any]:
    voice = raw if isinstance(raw, dict) else {}
    stt_mode = _clean_stt_mode(voice.get("stt_mode"), bool(voice.get("stt_enabled")))
    return {
        "stt_enabled": bool(stt_mode),
        "stt_mode": stt_mode,
        "tts_enabled": bool(voice.get("tts_enabled")),
    }


def _clean_active_browser_order(value: Any) -> int:
    try:
        number = int(float(value or 0))
    except (TypeError, ValueError):
        number = 0
    return max(-10000, min(number, 10000))


def _clean_active_browser_active_on(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    active_on: list[str] = []
    for item in value[:32]:
        clean = _clean_active_browser_page_id(item)
        if clean:
            active_on.append(clean)
    return active_on


def _clean_active_browser_page_capability(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    item_id = _clean_active_browser_page_id(item.get("id"))
    if not item_id:
        return None
    return {
        "id": item_id,
        "label": _clean_string(item.get("label"), "", 120),
        "page_label": _clean_string(item.get("page_label") or item.get("pageLabel"), "", 120),
        "parent": _clean_active_browser_menu_item_id(item.get("parent")),
        "order": _clean_active_browser_order(item.get("order")),
        "target_id": _clean_active_browser_page_id(item.get("target_id")),
        "current": bool(item.get("current")),
        "visible": bool(item.get("visible", True)),
        "blocked": bool(item.get("blocked")),
        "has_panel": bool(item.get("has_panel")),
        "invokable": bool(item.get("invokable")),
    }


def _clean_active_browser_function_capability(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    item_id = _clean_active_browser_menu_item_id(item.get("id"))
    fn_key = _clean_active_browser_fn_key(item.get("fn"))
    if not item_id and not fn_key:
        return None
    return {
        "id": item_id,
        "label": _clean_string(item.get("label"), "", 120),
        "parent": _clean_active_browser_menu_item_id(item.get("parent")),
        "order": _clean_active_browser_order(item.get("order")),
        "fn": fn_key,
        "active_on": _clean_active_browser_active_on(item.get("active_on") or item.get("activeOn")),
        "current_context": bool(item.get("current_context")),
        "visible": bool(item.get("visible", True)),
        "blocked": bool(item.get("blocked")),
        "registered": bool(item.get("registered")),
        "invokable": bool(item.get("invokable")),
    }


def _clean_active_browser_menu_capability(menu: Any) -> dict[str, Any] | None:
    if not isinstance(menu, dict):
        return None
    group = _clean_active_browser_group(menu.get("group"))
    if not group:
        return None
    pages = [
        clean
        for clean in (
            _clean_active_browser_page_capability(item)
            for item in (menu.get("pages") if isinstance(menu.get("pages"), list) else [])
        )
        if clean
    ][:120]
    function_items = [
        clean
        for clean in (
            _clean_active_browser_function_capability(item)
            for item in (
                menu.get("function_items") if isinstance(menu.get("function_items"), list) else []
            )
        )
        if clean
    ][:160]
    current_functions = [
        clean
        for clean in (
            _clean_active_browser_function_capability(item)
            for item in (
                menu.get("current_functions")
                if isinstance(menu.get("current_functions"), list)
                else []
            )
        )
        if clean
    ][:48]
    page_count = (
        _clean_browser_page_int(menu.get("page_count"), maximum=10000)
        if menu.get("page_count") is not None
        else len(pages)
    )
    function_count = (
        _clean_browser_page_int(menu.get("function_count"), maximum=10000)
        if menu.get("function_count") is not None
        else len(function_items)
    )
    return {
        "group": group,
        "active_id": _clean_active_browser_page_id(menu.get("active_id")),
        "layout_item_id": _clean_active_browser_menu_item_id(menu.get("layout_item_id")),
        "pages": pages,
        "function_items": function_items,
        "current_functions": current_functions,
        "page_count": page_count,
        "function_count": function_count,
    }


def _clean_active_browser_selector_capability(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    action = _clean_active_browser_selector_action(item.get("action") or item.get("id"))
    if not action:
        return None
    return {
        "action": action,
        "label": _clean_string(item.get("label"), "", 120),
        "bridge_group": _clean_active_browser_group(
            item.get("bridge_group") or item.get("bridgeGroup")
        ),
    }


def _clean_active_browser_automation_report(raw: Any) -> dict[str, Any]:
    automation = raw if isinstance(raw, dict) else {}
    menus = [
        clean
        for clean in (
            _clean_active_browser_menu_capability(item)
            for item in (
                automation.get("menus") if isinstance(automation.get("menus"), list) else []
            )
        )
        if clean
    ][:8]
    current_menu = _clean_active_browser_menu_capability(automation.get("current_menu"))
    selector_actions = [
        clean
        for clean in (
            _clean_active_browser_selector_capability(item)
            for item in (
                automation.get("selector_actions")
                if isinstance(automation.get("selector_actions"), list)
                else []
            )
        )
        if clean
    ][:80]
    return {
        "current_group": _clean_active_browser_group(automation.get("current_group")),
        "current_page_id": _clean_active_browser_page_id(automation.get("current_page_id")),
        "menus": menus,
        "current_menu": current_menu,
        "selector_actions": selector_actions,
    }


def _clean_browser_view_report(body: BrowserViewBody, now: float) -> dict[str, Any]:
    page = body.page if isinstance(body.page, dict) else {}
    frontend = body.frontend if isinstance(body.frontend, dict) else {}
    tts = _bounded_json(body.tts if isinstance(body.tts, dict) else {}, 8000)
    if not isinstance(tts, dict):
        tts = {}
    body_shade = _bounded_json(body.body_shade if isinstance(body.body_shade, dict) else {}, 1000)
    if not isinstance(body_shade, dict):
        body_shade = {}
    viewport = _clean_browser_viewport(body.viewport)
    viewport_classification = _classify_browser_viewport(viewport)
    voice = _clean_browser_voice_state(body.voice)
    docs = _bounded_json(body.docs if isinstance(body.docs, dict) else {}, 4000)
    if not isinstance(docs, dict):
        docs = {}
    modals: list[dict[str, Any]] = []
    for modal in body.modals or []:
        if not isinstance(modal, dict):
            continue
        modal_id = _clean_string(modal.get("id"), "", 120)
        if not modal_id:
            continue
        modals.append(
            {
                "id": modal_id,
                "label": _clean_string(modal.get("label"), "", 120),
                "open": bool(modal.get("open")),
            }
        )
        if len(modals) >= 24:
            break

    visibility = _clean_string(body.visibility_state, "unknown", 30).lower()
    if visibility not in {"visible", "hidden", "prerender", "unloaded", "unknown"}:
        visibility = "unknown"

    frontend_report = {
        "app": _clean_string(frontend.get("app"), "", 80),
        "asset_version": _clean_string(frontend.get("asset_version"), "", 160),
        "cache_mode": _clean_string(frontend.get("cache_mode"), "", 40),
        "service_worker_cache_version": _clean_string(
            frontend.get("service_worker_cache_version"), "", 120
        ),
        "service_worker_controller": bool(frontend.get("service_worker_controller")),
        "service_worker_state": _clean_string(frontend.get("service_worker_state"), "", 40),
    }

    return {
        "browser_id": _clean_browser_id(body.browser_id),
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "tab_id": _clean_string(body.tab_id, "", 120),
        "page": {
            "group": _clean_string(page.get("group"), "", 80),
            "tab": _clean_string(page.get("tab"), "", 120),
            "loading": bool(page.get("loading")),
            "ready": bool(page.get("ready")),
            "api_in_flight": _clean_browser_page_int(page.get("api_in_flight"), maximum=1000),
            "api_quiet_for_ms": _clean_browser_page_int(
                page.get("api_quiet_for_ms"), maximum=600000
            ),
            "api_sequence": _clean_browser_page_int(page.get("api_sequence"), maximum=1000000000),
        },
        "modals": modals,
        "viewport": viewport,
        "viewport_classification": viewport_classification,
        "viewport_class": viewport_classification["primary"],
        "viewport_flags": viewport_classification["flags"],
        "voice": voice,
        "visibility_state": visibility,
        "has_focus": bool(body.has_focus),
        "url_path": _clean_string(body.url_path, "", 180),
        "url_search": _clean_string(body.url_search, "", 300),
        "url_hash": _clean_string(body.url_hash, "", 180),
        "frontend": frontend_report,
        "automation": _clean_active_browser_automation_report(body.automation),
        "docs": docs,
        "body_shade": {
            "available": bool(body_shade.get("available")),
            "is_up": bool(body_shade.get("is_up")),
            "state": "up" if bool(body_shade.get("is_up")) else "down",
            "active_panel_id": _clean_string(body_shade.get("active_panel_id"), "", 120),
            "handle_present": bool(body_shade.get("handle_present")),
        },
        "tts": tts,
        "client_now_ms": float(body.client_now_ms or 0.0),
        "reported_at": now,
    }


def _browser_view_key(report: dict[str, Any]) -> str:
    browser_id = _clean_browser_id(report.get("browser_id"))
    tab_id = _clean_string(report.get("tab_id"), "", 120)
    return f"{browser_id}::{tab_id}" if tab_id else browser_id


def _fallback_frontend_expectation() -> dict[str, Any]:
    try:
        status = _read_fallback_ui_cache_status().model_dump()
    except Exception:
        status = {}
    return {
        "app": "fallback-ui",
        "asset_version": _clean_string(status.get("asset_version"), "", 180),
        "cache_mode": _clean_string(status.get("current_mode"), "", 40),
        "fallback_root": _clean_string(status.get("fallback_root"), "", 240),
        "state_file": _clean_string(status.get("state_file"), "", 240),
    }


def _annotate_browser_view(report: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(report, dict):
        return None
    public = dict(report)
    frontend = dict(public.get("frontend") if isinstance(public.get("frontend"), dict) else {})
    expected = _fallback_frontend_expectation()
    reported_asset = _clean_string(frontend.get("asset_version"), "", 180)
    expected_asset = _clean_string(expected.get("asset_version"), "", 180)
    public["frontend"] = frontend
    public["frontend_expected"] = expected
    public["frontend_asset_version_match"] = bool(
        reported_asset and expected_asset and reported_asset == expected_asset
    )
    return public


def _browser_report_age_seconds(report: dict[str, Any], now: float | None = None) -> float | None:
    reported_at = float(report.get("reported_at") or 0.0)
    if not reported_at:
        return None
    return round(max(0.0, float(now if now is not None else time.time()) - reported_at), 3)


def _annotate_browser_client(
    report: dict[str, Any],
    active: dict[str, Any] | None,
    *,
    now: float | None = None,
    max_age_seconds: int | None = None,
) -> dict[str, Any] | None:
    public = _annotate_browser_view(report)
    if not public:
        return None
    timestamp = float(now if now is not None else time.time())
    max_age = max(1, int(max_age_seconds or _ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS))
    age = _browser_report_age_seconds(public, timestamp)
    fresh = bool(age is not None and age <= max_age)
    report_browser_id = _clean_browser_id(public.get("browser_id"))
    report_tab_id = _clean_string(public.get("tab_id"), "", 120)
    active_browser_id = _clean_browser_id(
        active.get("browser_id") if isinstance(active, dict) else ""
    )
    active_tab_id = _clean_string(active.get("tab_id") if isinstance(active, dict) else "", "", 120)
    active_browser = bool(active_browser_id and report_browser_id == active_browser_id)
    active_tab = bool(active_browser and (not active_tab_id or active_tab_id == report_tab_id))
    public.update(
        {
            "client_key": _browser_view_key(public),
            "server_now": timestamp,
            "age_seconds": age,
            "fresh": fresh,
            "stale": not fresh,
            "active_browser": active_browser,
            "active_tab": active_tab,
            "lease_status": "active_tab"
            if active_tab
            else ("active_browser" if active_browser else "inactive"),
        }
    )
    return public


def _browser_client_inventory(
    state: dict[str, Any],
    *,
    now: float | None = None,
    max_age_seconds: int | None = None,
) -> list[dict[str, Any]]:
    reports = state.get("browser_views") if isinstance(state.get("browser_views"), dict) else {}
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    timestamp = float(now if now is not None else time.time())
    clients = [
        _annotate_browser_client(report, active, now=timestamp, max_age_seconds=max_age_seconds)
        for report in reports.values()
        if isinstance(report, dict)
    ]
    return sorted(
        (client for client in clients if client),
        key=lambda client: (
            1 if client.get("active_tab") else 0,
            1 if client.get("fresh") else 0,
            1 if client.get("visibility_state") == "visible" else 0,
            1 if client.get("has_focus") else 0,
            float(client.get("reported_at") or 0.0),
        ),
        reverse=True,
    )


def _find_browser_client_report(
    state: dict[str, Any],
    *,
    browser_id: str,
    tab_id: str = "",
    now: float | None = None,
    max_age_seconds: int | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    clean_browser_id = _clean_browser_id(browser_id)
    clean_tab_id = _clean_string(tab_id, "", 120)
    if not clean_browser_id:
        return None, "Missing browser_id"
    clients = [
        client
        for client in _browser_client_inventory(state, now=now, max_age_seconds=max_age_seconds)
        if _clean_browser_id(client.get("browser_id")) == clean_browser_id
        and (not clean_tab_id or _clean_string(client.get("tab_id"), "", 120) == clean_tab_id)
    ]
    if not clients:
        return None, "Browser client was not found"
    selected = clients[0]
    if selected.get("stale"):
        return selected, f"Browser client is stale ({selected.get('age_seconds')}s old)"
    return selected, None


def _public_browser_clients(
    state: dict[str, Any],
    *,
    max_age_seconds: int | None = None,
) -> dict[str, Any]:
    max_age = max(1, int(max_age_seconds or _ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS))
    now = time.time()
    clients = _browser_client_inventory(state, now=now, max_age_seconds=max_age)
    return {
        "ok": True,
        "active": _public_active(
            state.get("active") if isinstance(state.get("active"), dict) else None
        ),
        "clients": clients,
        "count": len(clients),
        "fresh_count": sum(1 for client in clients if client.get("fresh")),
        "stale_count": sum(1 for client in clients if client.get("stale")),
        "max_age_seconds": max_age,
        "server_now": now,
        "frontend_expected": _fallback_frontend_expectation(),
        "viewport_thresholds": dict(_ACTIVE_BROWSER_VIEWPORT_THRESHOLDS),
    }


def _selected_active_browser_view(state: dict[str, Any]) -> dict[str, Any] | None:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    reports = state.get("browser_views") if isinstance(state.get("browser_views"), dict) else {}
    if not reports:
        return None

    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    active_tab_id = _clean_string(active.get("tab_id") if active else "", "", 120)
    candidates = [
        report
        for report in reports.values()
        if isinstance(report, dict)
        and (
            not active_browser_id
            or _clean_browser_id(report.get("browser_id")) == active_browser_id
        )
    ]
    if not candidates:
        return None

    def _score(report: dict[str, Any]) -> tuple[int, int, int, float]:
        report_tab_id = _clean_string(report.get("tab_id"), "", 120)
        return (
            1 if active_tab_id and report_tab_id == active_tab_id else 0,
            1 if report.get("visibility_state") == "visible" else 0,
            1 if report.get("has_focus") else 0,
            float(report.get("reported_at") or 0.0),
        )

    return max(candidates, key=_score)


def _public_active_browser_view(state: dict[str, Any]) -> dict[str, Any]:
    reports = state.get("browser_views") if isinstance(state.get("browser_views"), dict) else {}
    now = time.time()
    recent = sorted(
        (_annotate_browser_view(report) for report in reports.values() if isinstance(report, dict)),
        key=lambda report: float(report.get("reported_at") or 0.0) if report else 0.0,
        reverse=True,
    )
    clients = _browser_client_inventory(
        state,
        now=now,
        max_age_seconds=_ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS,
    )
    return {
        "ok": True,
        "active": _public_active(
            state.get("active") if isinstance(state.get("active"), dict) else None
        ),
        "view": _annotate_browser_view(_selected_active_browser_view(state)),
        "reports": [report for report in recent if report][:10],
        "clients": clients[:10],
        "client_count": len(clients),
        "client_max_age_seconds": _ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS,
        "frontend_expected": _fallback_frontend_expectation(),
        "automation": {
            "default_step_timeout_seconds": _ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_DEFAULT_SECONDS,
            "minimum_step_timeout_seconds": _ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MIN_SECONDS,
            "maximum_step_timeout_seconds": _ACTIVE_BROWSER_AUTOMATION_STEP_TIMEOUT_MAX_SECONDS,
        },
        "browser_view_updated_at": float(state.get("browser_view_updated_at") or 0.0),
    }


def _store_browser_view_report_unlocked(
    state: dict[str, Any], report: dict[str, Any], now: float
) -> bool:
    reports = state.get("browser_views") if isinstance(state.get("browser_views"), dict) else {}
    reports[_browser_view_key(report)] = report
    sorted_items = sorted(
        reports.items(),
        key=lambda item: (
            float(item[1].get("reported_at") or 0.0) if isinstance(item[1], dict) else 0.0
        ),
        reverse=True,
    )
    state["browser_views"] = dict(sorted_items[:_BROWSER_VIEW_MAX_REPORTS])
    state["browser_view_updated_at"] = now

    active = state.get("active") if isinstance(state.get("active"), dict) else None
    if not active:
        return False
    if _clean_browser_id(active.get("browser_id")) != _clean_browser_id(report.get("browser_id")):
        return False

    changed = False
    report_tab_id = _clean_string(report.get("tab_id"), "", 120)
    active_tab_id = _clean_string(active.get("tab_id"), "", 120)
    should_update_tab = bool(
        report_tab_id
        and (
            not active_tab_id
            or active_tab_id == report_tab_id
            or (report.get("visibility_state") == "visible" and bool(report.get("has_focus")))
        )
    )
    if should_update_tab and active_tab_id != report_tab_id:
        active["tab_id"] = report_tab_id
        changed = True
    active["last_view_reported_at"] = now
    active["last_view_page"] = report.get("page")
    active["last_view_modals"] = report.get("modals")
    return changed


def _dev_debug_reports(debug: dict[str, Any]) -> list[dict[str, Any]]:
    reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
    return [report for report in reports.values() if isinstance(report, dict)]


def _latest_dev_debug_report(reports: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    return max(
        reports,
        key=lambda item: float(item.get("reported_at") or 0.0),
        default=None,
    )


def _report_matches_browser(report: dict[str, Any], browser_id: str) -> bool:
    return bool(browser_id) and _clean_browser_id(report.get("browser_id")) == browser_id


def _report_matches_surface(report: dict[str, Any], surface: str) -> bool:
    return bool(surface) and _clean_dev_command_surface(report.get("surface")) == surface


def _report_matches_tab(report: dict[str, Any], tab_id: str) -> bool:
    return bool(tab_id) and _clean_string(report.get("tab_id"), "", 120) == tab_id


def _dev_debug_report_key(report: dict[str, Any]) -> str:
    browser_id = _clean_browser_id(report.get("browser_id"))
    surface = _clean_dev_command_surface(report.get("surface"))
    tab_id = _clean_string(report.get("tab_id"), "", 120)
    parts = [part for part in (browser_id, tab_id, surface) if part]
    return ":".join(parts)


def _selected_browser_report(state: dict[str, Any], debug: dict[str, Any]) -> dict[str, Any] | None:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    active_tab_id = _clean_string(active.get("tab_id") if active else "", "", 120)
    reports = _dev_debug_reports(debug)
    selected = None
    if active_browser_id and active_tab_id:
        selected = _latest_dev_debug_report(
            report
            for report in reports
            if _report_matches_browser(report, active_browser_id)
            and _report_matches_tab(report, active_tab_id)
        )
    if not isinstance(selected, dict) and active_browser_id:
        selected = _latest_dev_debug_report(
            report for report in reports if _report_matches_browser(report, active_browser_id)
        )
    if not isinstance(selected, dict) and reports and not active_browser_id:
        selected = _latest_dev_debug_report(reports)
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


def _select_wake_dev_report(
    state: dict[str, Any],
    debug: dict[str, Any],
    *,
    surface: str = "",
    browser_id: str = "",
) -> dict[str, Any] | None:
    reports = _dev_debug_reports(debug)
    clean_browser_id = _clean_browser_id(browser_id)
    clean_surface = _clean_dev_command_surface(surface) if surface else ""
    if clean_browser_id:
        selected = _latest_dev_debug_report(
            report
            for report in reports
            if _report_matches_browser(report, clean_browser_id)
            and (not clean_surface or _report_matches_surface(report, clean_surface))
        )
        if isinstance(selected, dict):
            return selected
    if clean_surface:
        active = state.get("active") if isinstance(state.get("active"), dict) else None
        active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
        active_tab_id = _clean_string(active.get("tab_id") if active else "", "", 120)
        if active_browser_id and active_tab_id:
            selected = _latest_dev_debug_report(
                report
                for report in reports
                if _report_matches_browser(report, active_browser_id)
                and _report_matches_tab(report, active_tab_id)
                and _report_matches_surface(report, clean_surface)
            )
            if isinstance(selected, dict):
                return selected
        selected = _latest_dev_debug_report(
            report
            for report in reports
            if _report_matches_browser(report, active_browser_id)
            and _report_matches_surface(report, clean_surface)
        )
        if isinstance(selected, dict):
            return selected
        return _latest_dev_debug_report(
            report for report in reports if _report_matches_surface(report, clean_surface)
        )
    return _selected_browser_report(state, debug)


def _public_wake_dev_debug(
    state: dict[str, Any],
    debug: dict[str, Any],
    *,
    surface: str = "",
    browser_id: str = "",
) -> dict[str, Any]:
    selected = _select_wake_dev_report(state, debug, surface=surface, browser_id=browser_id)
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    report_browser_id = _clean_browser_id(
        selected.get("browser_id") if isinstance(selected, dict) else ""
    )
    public_debug = dict(selected) if isinstance(selected, dict) else None
    if isinstance(public_debug, dict):
        public_debug["authoritative_browser_active"] = bool(
            active_browser_id and report_browser_id == active_browser_id
        )
    reported_at = (
        float(public_debug.get("reported_at") or 0.0) if isinstance(public_debug, dict) else 0.0
    )
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


def _public_state(state: dict[str, Any], debug: dict[str, Any] | None = None) -> dict[str, Any]:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    return {
        "ok": True,
        "active": _public_active(active),
        "policy": _clean_policy(state.get("policy")),
        "revision": float(state.get("revision") or 0),
        "updated_at": float(state.get("updated_at") or 0),
    }


def _active_browser_from_body(body: BrowserVoiceState, now: float) -> dict[str, Any]:
    stt_mode = _clean_stt_mode(body.stt_mode, body.stt_enabled)
    return {
        "browser_id": _clean_browser_id(body.browser_id),
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "tab_id": _clean_string(body.tab_id, "", 120),
        "stt_enabled": bool(stt_mode),
        "stt_mode": stt_mode,
        "tts_enabled": bool(body.tts_enabled),
        "activated_at": now,
    }


def _active_browser_from_client_report(
    report: dict[str, Any],
    now: float,
    *,
    body: BrowserClientSelectionBody | None = None,
    current_active: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_same_browser = bool(
        isinstance(current_active, dict)
        and _clean_browser_id(current_active.get("browser_id"))
        == _clean_browser_id(report.get("browser_id"))
    )
    report_voice = report.get("voice") if isinstance(report.get("voice"), dict) else {}
    stt_enabled = (
        bool(body.stt_enabled)
        if body and body.stt_enabled is not None
        else (
            bool(report_voice.get("stt_enabled"))
            if report_voice
            else (bool(current_active.get("stt_enabled")) if current_same_browser else False)
        )
    )
    stt_mode = _clean_stt_mode(
        body.stt_mode
        if body and body.stt_mode is not None
        else (
            report_voice.get("stt_mode")
            if report_voice
            else (current_active.get("stt_mode") if current_same_browser else "")
        ),
        stt_enabled,
    )
    tts_enabled = (
        bool(body.tts_enabled)
        if body and body.tts_enabled is not None
        else (
            bool(report_voice.get("tts_enabled"))
            if report_voice
            else (bool(current_active.get("tts_enabled")) if current_same_browser else False)
        )
    )
    return {
        "browser_id": _clean_browser_id(report.get("browser_id")),
        "browser_label": _clean_label(report.get("browser_label"), "Blueprints browser"),
        "tab_id": _clean_string(report.get("tab_id"), "", 120),
        "stt_enabled": bool(stt_mode),
        "stt_mode": stt_mode,
        "tts_enabled": tts_enabled,
        "activated_at": now,
        "activated_via": "browser-client-api",
    }


class _ActiveBrowserActivationFsm:
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
        active_browser: dict[str, Any] | None = None,
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
            if not active_browser:
                return {"changed": False, "from": before, "to": before, "output": "ignored"}
            self.state["active"] = active_browser
        elif action == self.ACTION_DEACTIVATE_IF_OWNER:
            active = (
                self.state.get("active") if isinstance(self.state.get("active"), dict) else None
            )
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
        "Active Browser Changed",
        "Blueprints Active Browser changed.",
        severity="info",
        source="blueprints-active-browser",
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


@router.get("/active-browser-view")
async def active_browser_view() -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        return _public_active_browser_view(state)


@router.get("/browser-clients")
async def active_browser_clients(
    max_age_seconds: int = _ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS,
) -> dict[str, Any]:
    max_age = max(
        1, min(int(max_age_seconds or _ACTIVE_BROWSER_CLIENT_MAX_AGE_DEFAULT_SECONDS), 3600)
    )
    async with _state_lock:
        state = _read_state_unlocked()
        return _public_browser_clients(state, max_age_seconds=max_age)


@router.post("/browser-view")
async def update_browser_view(body: BrowserViewBody):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})

    async with _state_lock:
        state = _read_state_unlocked()
        now = time.time()
        report = _clean_browser_view_report(body, now)
        active_tab_changed = _store_browser_view_report_unlocked(state, report, now)
        persisted = _maybe_write_state_telemetry_unlocked(state)
        public = _public_active_browser_view(state) if _VOICE_MODE_HOT_POST_FULL_RESPONSE else None

    ack = {
        "ok": True,
        "stored": True,
        "persisted": persisted,
        "updated_at": now,
        "active_tab_changed": active_tab_changed,
    }
    if public:
        return {**public, **ack}
    return ack


@router.post("/browser-clients/activate")
async def active_browser_client_activate(body: BrowserClientSelectionBody):
    browser_id = _clean_browser_id(body.browser_id)
    tab_id = _clean_string(body.tab_id, "", 120)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})

    async with _state_lock:
        state = _read_state_unlocked()
        now = time.time()
        report, rejection = _find_browser_client_report(
            state,
            browser_id=browser_id,
            tab_id=tab_id,
            now=now,
            max_age_seconds=body.max_age_seconds,
        )
        if rejection:
            status_code = 410 if "stale" in rejection.lower() else 404
            return JSONResponse(
                status_code=status_code,
                content={
                    "ok": False,
                    "detail": rejection,
                    "browser_id": browser_id,
                    "tab_id": tab_id,
                    "client": report,
                },
            )
        active = state.get("active") if isinstance(state.get("active"), dict) else None
        activation = _ActiveBrowserActivationFsm(state).dispatch(
            _ActiveBrowserActivationFsm.INPUT_ACTIVATE_REQUEST,
            browser_id=browser_id,
            active_browser=_active_browser_from_client_report(
                report or {}, now, body=body, current_active=active
            ),
            now=now,
        )
        changed = bool(activation.get("changed"))
        if not changed:
            return JSONResponse(
                status_code=400, content={"ok": False, "detail": "Activation request was rejected."}
            )
        _write_state_unlocked(state)
        public_clients = _public_browser_clients(state, max_age_seconds=body.max_age_seconds)

    await _publish_changed(state, "activate-browser-client")
    return public_clients


@router.post("/browser-clients/deactivate")
async def active_browser_client_deactivate(body: BrowserClientSelectionBody | None = None):
    body = body or BrowserClientSelectionBody()
    async with _state_lock:
        state = _read_state_unlocked()
        active = state.get("active") if isinstance(state.get("active"), dict) else None
        browser_id = _clean_browser_id(body.browser_id) or _clean_browser_id(
            active.get("browser_id") if active else ""
        )
        tab_id = _clean_string(body.tab_id, "", 120)
        if not browser_id:
            return JSONResponse(
                status_code=409, content={"ok": False, "detail": "No Active Browser is available"}
            )

        report, rejection = _find_browser_client_report(
            state,
            browser_id=browser_id,
            tab_id=tab_id,
            now=time.time(),
            max_age_seconds=body.max_age_seconds,
        )
        active_matches = bool(active and _clean_browser_id(active.get("browser_id")) == browser_id)
        if rejection and not active_matches:
            status_code = 410 if "stale" in rejection.lower() else 404
            return JSONResponse(
                status_code=status_code,
                content={
                    "ok": False,
                    "detail": rejection,
                    "browser_id": browser_id,
                    "tab_id": tab_id,
                    "client": report,
                },
            )

        activation = _ActiveBrowserActivationFsm(state).dispatch(
            _ActiveBrowserActivationFsm.INPUT_DEACTIVATE_REQUEST,
            browser_id=browser_id,
            now=time.time(),
        )
        changed = bool(activation.get("changed"))
        if changed:
            _write_state_unlocked(state)
        public_clients = _public_browser_clients(state, max_age_seconds=body.max_age_seconds)

    if changed:
        await _publish_changed(state, "deactivate-browser-client")
    return public_clients


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

    async with _state_lock:
        state = _read_state_unlocked()
        now = time.time()
        activation = _ActiveBrowserActivationFsm(state).dispatch(
            _ActiveBrowserActivationFsm.INPUT_ACTIVATE_REQUEST,
            browser_id=browser_id,
            active_browser=_active_browser_from_body(body, now),
            now=now,
        )
        changed = bool(activation.get("changed"))
        if not changed:
            return JSONResponse(
                status_code=400, content={"ok": False, "detail": "Activation request was rejected."}
            )
        _write_state_unlocked(state)
    await _publish_changed(state, "activate")
    return _public_state(state)


@router.post("/deactivate")
async def voice_mode_deactivate(body: BrowserVoiceState):
    browser_id = _clean_browser_id(body.browser_id)
    if not browser_id:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing browser_id"})

    async with _state_lock:
        state = _read_state_unlocked()
        activation = _ActiveBrowserActivationFsm(state).dispatch(
            _ActiveBrowserActivationFsm.INPUT_DEACTIVATE_REQUEST,
            browser_id=browser_id,
            now=time.time(),
        )
        changed = bool(activation.get("changed"))
        if changed:
            _write_state_unlocked(state)
    if changed:
        await _publish_changed(state, "deactivate")
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


@router.get("/dev-status")
async def voice_mode_dev_status(
    surface: str | None = None, browser_id: str | None = None
) -> dict[str, Any]:
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_dev_debug_unlocked()
    return _public_wake_dev_debug(state, debug, surface=surface or "", browser_id=browser_id or "")


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
        "surface": _clean_dev_command_surface(body.surface),
        "mode": _clean_dev_command_mode(body.mode),
        "source": _clean_string(body.source, "", 120),
        "status": _clean_string(body.status, "", 240),
        "transcript": _clean_string(body.transcript, "", 4000),
        "snapshot": _bounded_json(body.snapshot or {}, 120000),
        "client_now_ms": float(body.client_now_ms or 0),
        "reported_at": now,
    }
    async with _state_lock:
        state = _read_state_unlocked()
        debug = _read_wake_dev_debug_unlocked()
        reports = debug.get("reports") if isinstance(debug.get("reports"), dict) else {}
        reports[_dev_debug_report_key(report)] = report
        debug = {
            "reports": reports,
            "updated_at": now,
        }
        persisted = _maybe_write_wake_dev_debug_telemetry_unlocked(debug)
        public = (
            _public_wake_dev_debug(state, debug) if _VOICE_MODE_HOT_POST_FULL_RESPONSE else None
        )
    ack = {
        "ok": True,
        "stored": True,
        "persisted": persisted,
        "updated_at": now,
        "surface": report["surface"],
        "reports_count": len(reports),
    }
    if public:
        return {**public, **ack}
    return ack


@router.post("/dev-command")
async def voice_mode_dev_command(body: VoiceDevCommandBody):
    """Publish a browser-directed Wake/VAD dev command over the SSE bus."""
    surface = _clean_dev_command_surface(body.surface)
    mode = _clean_dev_command_mode(body.mode)
    action = _clean_dev_command_action(body.action)
    if surface not in _DEV_COMMAND_SURFACES:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": f"Unsupported surface: {surface or 'blank'}"},
        )
    if mode not in _DEV_COMMAND_MODES:
        return JSONResponse(
            status_code=400, content={"ok": False, "detail": f"Unsupported mode: {mode or 'blank'}"}
        )
    if action not in _DEV_COMMAND_ACTIONS:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": f"Unsupported action: {action or 'blank'}"},
        )
    command_id = _clean_dev_command_id(body.command_id)
    explicit_browser_id = _clean_browser_id(body.browser_id)
    explicit_tab_id = _clean_string(body.tab_id, "", 120)
    async with _state_lock:
        state = _read_state_unlocked()
        active = state.get("active") if isinstance(state.get("active"), dict) else None

    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    active_tab_id = _clean_string(active.get("tab_id") if active else "", "", 120)
    target_browser_id = explicit_browser_id or (
        active_browser_id if body.target_active_browser else ""
    )
    target_tab_id = explicit_tab_id or (active_tab_id if body.target_active_browser else "")
    if body.target_active_browser and not target_browser_id:
        return JSONResponse(
            status_code=409, content={"ok": False, "detail": "No Active Browser is available"}
        )

    now = time.time()
    payload = {
        "schema": "xarta.voice_mode.dev_command.v1",
        "command_id": command_id,
        "surface": surface,
        "mode": mode,
        "action": action,
        "value": body.value,
        "enabled": body.enabled,
        "wake_to_talk_enabled": body.wake_to_talk_enabled,
        "stt_mode": body.stt_mode,
        "silero_vad_enabled": body.silero_vad_enabled,
        "vad_interrupt_tts_enabled": body.vad_interrupt_tts_enabled,
        "word_detection_match_interrupt_tts_enabled": body.word_detection_match_interrupt_tts_enabled,
        "word_detection_prefix_partial_interrupt_tts_enabled": body.word_detection_prefix_partial_interrupt_tts_enabled,
        "word_detection_prefix_final_interrupt_tts_enabled": body.word_detection_prefix_final_interrupt_tts_enabled,
        "word_detection_payload0_timeout_ms": body.word_detection_payload0_timeout_ms,
        "vad_payload0_timeout_ms": body.vad_payload0_timeout_ms,
        "word_detection_match_cue_enabled": body.word_detection_match_cue_enabled,
        "word_detection_match_cue_sound": body.word_detection_match_cue_sound,
        "word_detection_payload0_timeout_cue_enabled": body.word_detection_payload0_timeout_cue_enabled,
        "word_detection_payload0_timeout_cue_sound": body.word_detection_payload0_timeout_cue_sound,
        "word_detection_agent_candidate_cue_enabled": body.word_detection_agent_candidate_cue_enabled,
        "word_detection_agent_candidate_cue_sound": body.word_detection_agent_candidate_cue_sound,
        "auto_pre_roll_enabled": body.auto_pre_roll_enabled,
        "level_db": body.level_db,
        "noise_level_db": body.noise_level_db,
        "noise_threshold_db": body.noise_threshold_db,
        "threshold_db": body.threshold_db,
        "vad_pre_roll_db": body.vad_pre_roll_db,
        "vad_pre_roll_threshold_db": body.vad_pre_roll_threshold_db,
        "aggregation_timeout_ms": body.aggregation_timeout_ms,
        "speech_aggregation_timeout_ms": body.speech_aggregation_timeout_ms,
        "vad_reset_timeout_ms": body.vad_reset_timeout_ms,
        "reset_timeout_ms": body.reset_timeout_ms,
        "pre_roll_frames": body.pre_roll_frames,
        "num_pre_roll": body.num_pre_roll,
        "num_pre_roll_frames": body.num_pre_roll_frames,
        "always_pre_roll_enabled": body.always_pre_roll_enabled,
        "word_detection_aliases": body.word_detection_aliases,
        "sense_words": body.sense_words,
        "target_browser_id": target_browser_id,
        "target_tab_id": target_tab_id,
        "active_browser_id": active_browser_id,
        "active_tab_id": active_tab_id,
        "open_modal": bool(body.open_modal),
        "created_at": now,
        "max_age_seconds": int(body.max_age_seconds),
    }
    event = AppEvent.create(
        _VOICE_DEV_COMMAND_EVENT_TYPE,
        "Active Browser Dev Command",
        f"Active Browser {surface} dev command {mode}:{action}.",
        severity="info",
        source="blueprints-active-browser",
        payload=payload,
        event_id=f"voice-dev-command-{command_id}",
    )
    published = await publish_event(event)
    return {
        "ok": True,
        "event": published.model_dump(),
        "payload": payload,
    }


@router.post("/active-browser-command")
async def active_browser_command(body: ActiveBrowserCommandBody):
    """Publish a bounded browser automation command to the Active Browser."""
    action = _clean_active_browser_command_action(body.action)
    if action not in _ACTIVE_BROWSER_COMMAND_ACTIONS:
        return JSONResponse(
            status_code=400,
            content={
                "ok": False,
                "detail": f"Unsupported active browser action: {action or 'blank'}",
            },
        )
    command_id = _clean_active_browser_command_id(body.command_id)
    explicit_browser_id = _clean_browser_id(body.browser_id)
    explicit_tab_id = _clean_string(body.tab_id, "", 120)
    async with _state_lock:
        state = _read_state_unlocked()
        active = state.get("active") if isinstance(state.get("active"), dict) else None

    active_browser_id = _clean_browser_id(active.get("browser_id") if active else "")
    active_tab_id = _clean_string(active.get("tab_id") if active else "", "", 120)
    target_browser_id = explicit_browser_id or (
        active_browser_id if body.target_active_browser else ""
    )
    target_tab_id = explicit_tab_id or (active_tab_id if body.target_active_browser else "")
    if body.target_active_browser and not target_browser_id:
        return JSONResponse(
            status_code=409, content={"ok": False, "detail": "No Active Browser is available"}
        )

    now = time.time()
    group = _clean_active_browser_group(body.group or body.menu_group)
    page_id = _clean_active_browser_page_id(body.page_id or body.tab)
    menu_item_id = _clean_active_browser_menu_item_id(body.menu_item_id or body.menu_id)
    fn_key = _clean_active_browser_fn_key(body.fn)
    modal_id = _clean_active_browser_modal_id(body.modal_id)
    selector_action = _clean_active_browser_selector_action(body.selector_action)
    event_kind = _clean_active_browser_event_kind(body.event_kind)
    doc_id = _clean_string(body.doc_id, "", 120)
    doc_path = _clean_string(body.path or body.doc_path, "", 300)
    body_shade = _clean_active_browser_body_shade(body.body_shade or body.shade)
    highlight_terms = [
        term
        for term in (_clean_string(item, "", 80) for item in (body.highlight_terms or []))
        if term
    ][:8]
    payload = {
        "schema": "xarta.active_browser.command.v1",
        "command_id": command_id,
        "action": action,
        "target_browser_id": target_browser_id,
        "target_tab_id": target_tab_id,
        "active_browser_id": active_browser_id,
        "active_tab_id": active_tab_id,
        "created_at": now,
        "max_age_seconds": int(body.max_age_seconds),
    }
    if group:
        payload["group"] = group
    if page_id:
        payload["page_id"] = page_id
    if menu_item_id:
        payload["menu_item_id"] = menu_item_id
    if fn_key:
        payload["fn"] = fn_key
    if modal_id:
        payload["modal_id"] = modal_id
    if doc_id:
        payload["doc_id"] = doc_id
    if doc_path:
        payload["path"] = doc_path
    if highlight_terms:
        payload["highlight_terms"] = highlight_terms
    if selector_action:
        payload["selector_action"] = selector_action
    if body.event_kind is not None:
        payload["event_kind"] = event_kind
    if action == "set_body_shade":
        payload["body_shade"] = body_shade or "up"
    elif body_shade:
        payload["body_shade"] = body_shade
    if body.instant is not None:
        payload["instant"] = bool(body.instant)
    event = AppEvent.create(
        _ACTIVE_BROWSER_COMMAND_EVENT_TYPE,
        "Active Browser Command",
        f"Active Browser command {action}.",
        severity="info",
        source="blueprints-active-browser",
        payload=payload,
        event_id=f"active-browser-command-{command_id}",
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
            if body.wake_to_talk is None or not _is_default_stt_reset_payload(
                body.stt,
                policy.get("stt"),
            ):
                policy["stt"] = _clean_stt_policy_update(policy.get("stt"), body.stt)
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
            response = await client.post(
                url, json={"aggregation_timeout": payload["aggregation_timeout"]}
            )
        response_payload = response.json() if response.content else {}
        if not response.is_success:
            return JSONResponse(
                status_code=502,
                content={
                    "ok": False,
                    "supported": False,
                    "detail": f"HTTP {response.status_code}",
                    "url": url,
                },
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
