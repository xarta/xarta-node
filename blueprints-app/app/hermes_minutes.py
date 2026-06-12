"""Local STT/TTS Minutes helpers.

The local JSONL file is the low-latency repair substrate. Matrix posting is a
durable projection owned by Matrix Chat routes, not by this module.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_MINUTES_CONFIG_FILE = Path("/xarta-node/.lone-wolf/config/hermes-stt/minutes.json")
DEFAULT_MINUTES_INDEX_PATH = Path("/xarta-node/.lone-wolf/state/hermes-stt/minutes/recent.jsonl")
DEFAULT_MINUTES_TTL_SECONDS = 6 * 60 * 60
DEFAULT_RECENT_LIMIT = 8

MINUTES_EVENT_SCHEMA = "xarta.hermes.minutes.event.v1"
MINUTES_SUMMARY_SCHEMA = "xarta.hermes.minutes.summary.v1"
MINUTES_CONFIG_SCHEMA = "xarta.hermes.minutes.config.v1"
BLUEPRINTS_NAV_PROFILE = "hermes-stt-blueprints-nav"

_SPACE_RE = re.compile(r"\s+")
_URL_CREDENTIAL_RE = re.compile(
    r"\b([a-z][a-z0-9+.-]*://)([^/\s:@]{1,160}):([^@\s/]{1,240})@", re.I
)
_TOKENISH_RE = re.compile(
    r"\b(?:api[_-]?key|access[_-]?token|secret|password|passwd|bearer)\s*[:=]\s*([^\s,;]{4,})",
    re.I,
)
_AUTHORISATION_RE = re.compile(r"\bauthori[sz]ation\s+[a-z0-9._-]+(?:\s+[a-z0-9._-]+){0,6}\b", re.I)
_PHONE_RE = re.compile(r"(?<!\w)(?:\+?\d[\d\s().-]{7,}\d)(?!\w)")


def _truthy(value: Any, *, default: bool = False) -> bool:
    text = str(value if value is not None else "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _clip_text(value: Any, limit: int = 600) -> str:
    text = _SPACE_RE.sub(" ", str(value or "").strip())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _clean_key(value: Any, limit: int = 260) -> str:
    return _clip_text(value, limit)


def redact_minutes_text(value: Any, *, limit: int = 1200) -> str:
    text = str(value or "")
    if not text:
        return ""
    text = _URL_CREDENTIAL_RE.sub(r"\1[redacted]@", text)
    text = _TOKENISH_RE.sub(
        lambda match: match.group(0).replace(match.group(1), "[redacted]"), text
    )
    text = _AUTHORISATION_RE.sub("[redacted authorisation]", text)
    text = _PHONE_RE.sub("[redacted phone]", text)
    return _clip_text(text, limit)


def _bounded_json_public(value: Any, limit: int = 2000) -> Any:
    try:
        text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    except (TypeError, ValueError):
        return redact_minutes_text(value, limit=limit)
    if len(text) <= limit:
        try:
            parsed = json.loads(text)
        except ValueError:
            return redact_minutes_text(text, limit=limit)
        return _redact_json_value(parsed, limit=limit)
    return {"truncated_json": redact_minutes_text(text, limit=limit)}


def _redact_json_value(value: Any, *, limit: int = 1200) -> Any:
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, item in value.items():
            key_text = str(key or "")[:120]
            if any(word in key_text.lower() for word in ("token", "secret", "password", "api_key")):
                cleaned[key_text] = "[redacted]"
            else:
                cleaned[key_text] = _redact_json_value(item, limit=limit)
        return cleaned
    if isinstance(value, list):
        return [_redact_json_value(item, limit=limit) for item in value[:40]]
    if isinstance(value, str):
        return redact_minutes_text(value, limit=limit)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return redact_minutes_text(value, limit=limit)


def minutes_config_path(environ: dict[str, str] | None = None) -> Path:
    env = os.environ if environ is None else environ
    raw = str(env.get("HERMES_MINUTES_CONFIG_FILE") or "").strip()
    return Path(raw) if raw else DEFAULT_MINUTES_CONFIG_FILE


def minutes_index_path(environ: dict[str, str] | None = None) -> Path:
    env = os.environ if environ is None else environ
    raw = str(env.get("HERMES_MINUTES_LOCAL_INDEX_PATH") or "").strip()
    if raw:
        return Path(raw)
    config = read_minutes_config(environ)
    return Path(str(config.get("local_index_path") or DEFAULT_MINUTES_INDEX_PATH))


def read_minutes_config(environ: dict[str, str] | None = None) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    path = minutes_config_path(env)
    parsed: dict[str, Any] = {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        parsed = loaded if isinstance(loaded, dict) else {}
    except (OSError, ValueError, TypeError):
        parsed = {}
    return {
        "schema": parsed.get("schema") or MINUTES_CONFIG_SCHEMA,
        "enabled": _truthy(
            env.get("HERMES_MINUTES_ENABLED"), default=_truthy(parsed.get("enabled"), default=True)
        ),
        "local_enabled": _truthy(
            env.get("HERMES_MINUTES_LOCAL_ENABLED"),
            default=_truthy(parsed.get("local_enabled"), default=True),
        ),
        "matrix_post_enabled": _truthy(
            env.get("HERMES_MINUTES_MATRIX_POST_ENABLED"),
            default=_truthy(parsed.get("matrix_post_enabled"), default=bool(parsed.get("room_id"))),
        ),
        "server_id": _clip_text(
            env.get("HERMES_MINUTES_MATRIX_SERVER") or parsed.get("server_id") or "tb1", 40
        ),
        "room_id": _clip_text(
            env.get("HERMES_MINUTES_ROOM_ID") or parsed.get("room_id") or "", 260
        ),
        "room_name": _clip_text(
            env.get("HERMES_MINUTES_ROOM_NAME") or parsed.get("room_name") or "Minutes", 120
        ),
        "require_e2ee": _truthy(
            env.get("HERMES_MINUTES_REQUIRE_E2EE"),
            default=_truthy(parsed.get("require_e2ee"), default=True),
        ),
        "local_index_path": _clip_text(
            env.get("HERMES_MINUTES_LOCAL_INDEX_PATH")
            or parsed.get("local_index_path")
            or str(DEFAULT_MINUTES_INDEX_PATH),
            400,
        ),
        "ttl_seconds": _safe_float(parsed.get("ttl_seconds"), DEFAULT_MINUTES_TTL_SECONDS),
        "max_summary_chars": int(_safe_float(parsed.get("max_summary_chars"), 1800)),
    }


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def append_minutes_event(
    *,
    event_kind: str,
    conversation_key: str,
    payload: dict[str, Any],
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    config = read_minutes_config(environ)
    if not config.get("enabled") or not config.get("local_enabled"):
        return {"ok": True, "skipped": True, "reason": "minutes_local_disabled"}
    path = minutes_index_path(environ)
    clean_kind = _clip_text(event_kind, 80)
    event = {
        "schema": MINUTES_EVENT_SCHEMA,
        "event_kind": clean_kind,
        "conversation_key": _clean_key(conversation_key),
        "created_at_epoch": time.time(),
        "created_at": _utc_now(),
        "payload": _redact_json_value(payload, limit=1800),
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True, sort_keys=True) + "\n")
        try:
            path.chmod(0o600)
        except OSError:
            pass
    except OSError as exc:
        return {"ok": False, "path": str(path), "error": str(exc)[:240]}
    return {
        "ok": True,
        "path": str(path),
        "event_kind": clean_kind,
        "conversation_key": event["conversation_key"],
        "created_at": event["created_at"],
    }


def append_bounded_action_fact(
    *,
    conversation_key: str,
    request_text: str,
    route_profile: str,
    action_record: dict[str, Any],
    context_kind: str,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema": "xarta.hermes.minutes.bounded_action.v1",
        "route_profile": _clip_text(route_profile, 120),
        "context_kind": _clip_text(context_kind, 80),
        "request_text": redact_minutes_text(request_text, limit=600),
        "action": _bounded_json_public(action_record, 5000),
    }
    return append_minutes_event(
        event_kind="bounded_action",
        conversation_key=conversation_key,
        payload=payload,
        environ=environ,
    )


def append_turn_summary(
    *,
    conversation_key: str,
    operator_text: str,
    source_room_id: str = "",
    route: str = "",
    route_status: str = "",
    route_profile: str = "",
    assistant_speech: str = "",
    matrix_detail: str = "",
    tts_event_id: str = "",
    delivery: dict[str, Any] | None = None,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    summary = build_turn_summary(
        conversation_key=conversation_key,
        operator_text=operator_text,
        source_room_id=source_room_id,
        route=route,
        route_status=route_status,
        route_profile=route_profile,
        assistant_speech=assistant_speech,
        matrix_detail=matrix_detail,
        tts_event_id=tts_event_id,
        delivery=delivery or {},
        environ=environ,
    )
    result = append_minutes_event(
        event_kind="turn_summary",
        conversation_key=conversation_key,
        payload=summary,
        environ=environ,
    )
    return {**result, "summary": summary if result.get("ok") else {}}


def build_turn_summary(
    *,
    conversation_key: str,
    operator_text: str,
    source_room_id: str = "",
    route: str = "",
    route_status: str = "",
    route_profile: str = "",
    assistant_speech: str = "",
    matrix_detail: str = "",
    tts_event_id: str = "",
    delivery: dict[str, Any] | None = None,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    config = read_minutes_config(environ)
    limit = int(config.get("max_summary_chars") or 1800)
    clean_operator = redact_minutes_text(operator_text, limit=600)
    clean_speech = redact_minutes_text(assistant_speech, limit=400)
    clean_detail = redact_minutes_text(matrix_detail, limit=min(900, limit))
    delivery_public = _bounded_json_public(delivery or {}, 2200)
    open_question = ""
    if "clarify" in str(route_status):
        open_question = "The system asked the operator to clarify the intended bounded action."
    elif route_status == "command_code_required":
        open_question = "A Command Code challenge is pending for the held request."
    summary = {
        "schema": MINUTES_SUMMARY_SCHEMA,
        "conversation_key": _clean_key(conversation_key),
        "time": _utc_now(),
        "operator_intent_summary": _clip_text(f"Operator said: {clean_operator}", 700),
        "assistant_action_summary": _clip_text(
            f"Route {route or 'unknown'} status {route_status or 'unknown'}"
            + (f"; profile {route_profile}" if route_profile else "")
            + (f"; speech: {clean_speech}" if clean_speech else ""),
            700,
        ),
        "result_summary": _clip_text(
            clean_detail or f"Delivery status: {route_status or route}", limit
        ),
        "open_question": open_question,
        "entities": [],
        "problems": [],
        "followup_affordances": [],
        "source_pointers": {
            "source_room_id": _clip_text(source_room_id, 260),
            "tts_utterance_ids": [tts_event_id] if tts_event_id else [],
        },
        "delivery": delivery_public,
        "confidence": 0.7,
    }
    return summary


def read_recent_minutes(
    *,
    conversation_key: str = "",
    event_kind: str = "",
    limit: int = DEFAULT_RECENT_LIMIT,
    ttl_seconds: float | None = None,
    environ: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    path = minutes_index_path(environ)
    key = _clean_key(conversation_key)
    config = read_minutes_config(environ)
    ttl = (
        ttl_seconds
        if ttl_seconds is not None
        else float(config.get("ttl_seconds") or DEFAULT_MINUTES_TTL_SECONDS)
    )
    now = time.time()
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return []
    entries: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            parsed = json.loads(line)
        except ValueError:
            continue
        if not isinstance(parsed, dict) or parsed.get("schema") != MINUTES_EVENT_SCHEMA:
            continue
        if event_kind and parsed.get("event_kind") != event_kind:
            continue
        if key and _clean_key(parsed.get("conversation_key")) != key:
            continue
        try:
            age = now - float(parsed.get("created_at_epoch") or 0.0)
        except (TypeError, ValueError):
            continue
        if age < 0 or age > ttl:
            continue
        entries.append(parsed)
        if len(entries) >= limit:
            break
    entries.reverse()
    return entries


def recent_blueprints_navigation_context(
    *,
    conversation_key: str = "",
    environ: dict[str, str] | None = None,
    limit: int = DEFAULT_RECENT_LIMIT,
) -> dict[str, Any]:
    if not _clean_key(conversation_key):
        return {}
    events = read_recent_minutes(
        conversation_key=conversation_key,
        event_kind="bounded_action",
        limit=limit,
        environ=environ,
    )
    actions: list[dict[str, Any]] = []
    for event in events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if payload.get("route_profile") != BLUEPRINTS_NAV_PROFILE:
            continue
        action = payload.get("action") if isinstance(payload.get("action"), dict) else {}
        if action:
            actions.append(action)
    if not actions:
        return {}
    latest = actions[-1]
    context_kind = _clip_text(latest.get("context_kind") or latest.get("status") or "", 80)
    unresolved = {}
    last_action = {}
    for action in reversed(actions):
        kind = _clip_text(action.get("context_kind"), 80)
        if not unresolved and kind == "unresolved_navigation":
            unresolved = action
        if not last_action and kind == "last_navigation_action":
            last_action = action
        if unresolved and last_action:
            break
    return {
        "schema": "xarta.wake-stt.blueprints-nav-context.v1",
        "source": "local_minutes",
        "updated_at_epoch": latest.get("updated_at_epoch") or time.time(),
        "updated_at": latest.get("updated_at") or _utc_now(),
        "conversation_key": _clean_key(conversation_key),
        "request_text": _clip_text(latest.get("request_text"), 600),
        "status": _clip_text(latest.get("status"), 80),
        "context_kind": context_kind,
        "decision": latest.get("decision") if isinstance(latest.get("decision"), dict) else {},
        "candidates": latest.get("candidates")
        if isinstance(latest.get("candidates"), list)
        else [],
        "selected_candidate": latest.get("selected_candidate")
        if isinstance(latest.get("selected_candidate"), dict)
        else {},
        "unresolved_navigation": unresolved,
        "last_navigation_action": last_action,
        "recent_actions": actions[-limit:],
    }
