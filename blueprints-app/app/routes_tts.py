"""Blueprints TTS wrapper endpoints with DB-driven policy and in-wrapper fallback audio."""

from __future__ import annotations

import asyncio
import json
import mimetypes
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import config as cfg
from .db import get_conn, get_setting
from .events import AppEvent
from .routes_events import publish_event
from .tts_sanitizer_client import (
    TtsSanitizerUnavailable,
    resolve_tts_sanitizer_url,
    sanitize_tts_text_via_service,
)

router = APIRouter(prefix="/tts", tags=["tts"])
TransformProfile = Literal[
    "default",
    "speech",
    "realtime",
    "real-time",
    "conversation",
    "conversational",
    "live",
    "none",
]
_SANITIZING_TRANSFORM_PROFILES = {
    "default",
    "speech",
    "realtime",
    "real-time",
    "conversation",
    "conversational",
    "live",
}


_REQUIRED_SETTINGS: tuple[str, ...] = (
    "tts.enabled",
    "tts.default_voice",
    "tts.default_message",
    "tts.default_mode",
    "tts.stream_word_threshold",
    "tts.local_probe_url",
    "tts.local_speech_url",
    "tts.timeout_ms",
    "tts.volume",
    "tts.interrupt_default",
    "tts.fallback.enabled",
    "tts.fallback.positive_sound_path",
    "tts.fallback.negative_sound_path",
    "tts.fallback.volume",
)
_OPTIONAL_SETTINGS: tuple[str, ...] = ("tts.sanitizer_url",)


@dataclass
class _ActiveSession:
    session_id: str
    cancelled: bool = False


_active_sessions: dict[str, _ActiveSession] = {}
_active_sessions_lock = asyncio.Lock()


class SpeakRequest(BaseModel):
    text: str | None = None
    voice: str | None = None
    client_id: str | None = None
    interrupt: bool | None = None
    mode: str | None = None
    format: str | None = None
    timeout_ms: int | None = None
    allow_fallback: bool | None = None
    event_kind: str | None = None
    fallback_kind: str | None = None
    sentiment: str | None = None
    sanitize_text: bool | None = None
    sanitise_text: bool | None = None
    tts_sanitize: bool | None = None
    transform_profile: TransformProfile | None = None
    allow_llm_sanitizer: bool | None = None
    allow_llm_santitizer: bool | None = None
    volume_gain: float | None = None


class StopRequest(BaseModel):
    client_id: str | None = None


class SanitizeRequest(BaseModel):
    text: str
    transform_profile: TransformProfile | None = None
    allow_llm_sanitizer: bool | None = None
    allow_llm_santitizer: bool | None = None


class UtteranceTarget(BaseModel):
    kind: str | None = None
    dedupe: str | None = None


class UtteranceRequest(BaseModel):
    utterance_id: str | None = None
    source: str | None = None
    agent_id: str | None = None
    subagent_id: str | None = None
    conversation_id: str | None = None
    text: str
    voice: str | None = None
    mode: str | None = None
    format: str | None = None
    interrupt: bool | None = None
    client_id: str | None = None
    target: UtteranceTarget | None = None
    sanitize_text: bool | None = None
    transform_profile: TransformProfile | None = None
    allow_llm_sanitizer: bool | None = None
    volume: float | None = None
    volume_gain: float | None = None
    timeout_ms: int | None = None
    allow_fallback: bool | None = None
    created_at: float | None = None
    metadata: dict[str, Any] | None = None


def _parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    v = str(value).strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return None


def _parse_int(value: str | None, min_value: int, max_value: int) -> int | None:
    try:
        parsed = int(str(value).strip())
    except Exception:
        return None
    return max(min_value, min(max_value, parsed))


def _parse_float(value: Any, default: float, min_value: float, max_value: float) -> float:
    try:
        parsed = float(str(value).strip())
    except Exception:
        return default
    return max(min_value, min(max_value, parsed))


def _bounded_int(value: int | None, default: int, min_value: int, max_value: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except Exception:
        return default
    return max(min_value, min(max_value, parsed))


def _shared_tts_volume(settings: dict[str, str]) -> float:
    return _parse_float(settings.get("tts.volume"), 0.85, 0.0, 3.0)


def _shared_tts_playback_volume(settings: dict[str, str]) -> float:
    return min(1.0, _shared_tts_volume(settings))


def _shared_tts_volume_gain(settings: dict[str, str]) -> float:
    return max(1.0, _shared_tts_volume(settings))


def _resolve_settings() -> tuple[dict[str, str], list[str]]:
    resolved: dict[str, str] = {}
    missing: list[str] = []
    with get_conn() as conn:
        for key in _REQUIRED_SETTINGS:
            db_val = get_setting(conn, key)
            if db_val is not None and str(db_val).strip() != "":
                resolved[key] = db_val
            else:
                missing.append(key)
        for key in _OPTIONAL_SETTINGS:
            db_val = get_setting(conn, key)
            if db_val is not None and str(db_val).strip() != "":
                resolved[key] = db_val
    return resolved, missing


def _load_recent_utterance_events(limit: int = 20) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT event_id, event_type, severity, title, message, source, created_at, payload_json
            FROM events
            WHERE event_type = 'tts.utterance.requested'
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [
        {
            "event_id": row["event_id"],
            "event_type": row["event_type"],
            "severity": row["severity"],
            "title": row["title"],
            "message": row["message"],
            "source": row["source"],
            "created_at": float(row["created_at"]),
            "payload": json.loads(row["payload_json"] or "{}"),
        }
        for row in rows
    ]


def _resolve_fallback_file(path_value: str) -> Path:
    raw = str(path_value or "").strip()
    if raw.startswith("/"):
        return Path(raw)
    # Database stores asset-relative paths such as sounds/foo.mp3.
    return Path(cfg.REPO_NON_ROOT_PATH) / "gui-fallback" / "assets" / raw


def _resolve_fallback_kind(body: SpeakRequest, default_kind: str = "positive") -> str:
    raw = (body.fallback_kind or body.sentiment or default_kind or "positive").strip().lower()
    if raw in {"negative", "neg", "error", "fail", "failure"}:
        return "negative"
    if raw in {"neutral", "info", "acknowledge", "ack"}:
        return "neutral"
    return "positive"


def _safe_event_id(prefix: str, value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in value.strip())
    safe = safe.strip("-._") or uuid.uuid4().hex
    return f"{prefix}{safe}"[:160]


def _fallback_response(
    *,
    settings: dict[str, str],
    fallback_kind: str,
    voice: str,
    interrupted_previous: bool,
    status_code_on_failure: int,
    detail_on_failure: str,
) -> FileResponse | JSONResponse:
    if fallback_kind == "negative":
        fallback_path = settings.get("tts.fallback.negative_sound_path", "")
    elif fallback_kind == "neutral":
        fallback_path = settings.get("tts.fallback.neutral_sound_path", "")
    else:
        fallback_path = settings.get("tts.fallback.positive_sound_path", "")

    fs_path = _resolve_fallback_file(fallback_path)
    if not fs_path.is_file():
        return JSONResponse(
            status_code=status_code_on_failure,
            content={
                "ok": False,
                "engine": "none",
                "detail": detail_on_failure,
                "fallback_sound_path": fallback_path,
            },
        )

    mime_type, _ = mimetypes.guess_type(str(fs_path))
    headers = {
        "X-Blueprints-TTS-Engine": "sound_fallback",
        "X-Blueprints-TTS-Interrupted-Previous": "true" if interrupted_previous else "false",
        "X-Blueprints-TTS-Voice": voice,
        "X-Blueprints-TTS-Fallback-Path": fallback_path,
    }
    return FileResponse(str(fs_path), media_type=mime_type or "audio/mpeg", headers=headers)


async def _is_local_tts_available(probe_url: str, timeout_ms: int) -> bool:
    try:
        async with httpx.AsyncClient(timeout=timeout_ms / 1000.0) as client:
            resp = await client.get(probe_url)
            return 200 <= resp.status_code < 500
    except Exception:
        return False


def _client_key(req: Request, explicit_client_id: str | None = None) -> str:
    if explicit_client_id and explicit_client_id.strip():
        return explicit_client_id.strip()
    hdr = req.headers.get("x-blueprints-client-id", "").strip()
    if hdr:
        return hdr
    host = req.client.host if req.client else "unknown"
    return f"host:{host}"


def _should_sanitize_text(body: SpeakRequest) -> bool:
    if body.transform_profile == "none":
        return False
    return (
        any(value is True for value in (body.sanitize_text, body.sanitise_text, body.tts_sanitize))
        or body.transform_profile in _SANITIZING_TRANSFORM_PROFILES
    )


def _allow_llm_sanitizer(body: SpeakRequest | SanitizeRequest) -> bool:
    return bool(body.allow_llm_sanitizer or body.allow_llm_santitizer)


async def _start_session(client_key: str, interrupt: bool) -> tuple[_ActiveSession, bool]:
    interrupted_previous = False
    async with _active_sessions_lock:
        prev = _active_sessions.get(client_key)
        if prev and interrupt:
            prev.cancelled = True
            interrupted_previous = True
        new_session = _ActiveSession(session_id=uuid.uuid4().hex)
        _active_sessions[client_key] = new_session
    return new_session, interrupted_previous


async def _clear_session_if_current(client_key: str, session: _ActiveSession) -> None:
    async with _active_sessions_lock:
        current = _active_sessions.get(client_key)
        if current is session:
            _active_sessions.pop(client_key, None)


@router.post("/speak")
async def tts_speak(body: SpeakRequest, request: Request):
    request_started = time.perf_counter()
    sanitizer_ms: float | None = None
    probe_ms: float | None = None
    upstream_headers_ms: float | None = None
    settings, missing = _resolve_settings()
    if missing:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": "TTS wrapper settings are incomplete in the database",
                "missing_settings": missing,
            },
        )

    tts_enabled = _parse_bool(settings.get("tts.enabled"))
    fallback_enabled = _parse_bool(settings.get("tts.fallback.enabled"))
    fallback_allowed = bool(fallback_enabled) and body.allow_fallback is not False
    interrupt_default = _parse_bool(settings.get("tts.interrupt_default"))
    settings_timeout_ms = _parse_int(settings.get("tts.timeout_ms"), 1000, 120000)
    threshold = _parse_int(settings.get("tts.stream_word_threshold"), 1, 2000)

    if None in {tts_enabled, fallback_enabled, interrupt_default, settings_timeout_ms, threshold}:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": "TTS wrapper settings contain invalid values",
            },
        )

    voice = (body.voice or "").strip() or settings.get("tts.default_voice", "")
    req_mode = (body.mode or "").strip().lower() or settings.get(
        "tts.default_mode", ""
    ).strip().lower()
    fmt = (body.format or "wav").strip().lower()
    interrupt = bool(interrupt_default) if body.interrupt is None else bool(body.interrupt)
    timeout_ms = _bounded_int(body.timeout_ms, int(settings_timeout_ms), 1000, 600000)

    raw_text = (body.text or "").strip() or settings.get("tts.default_message", "")
    try:
        sanitizer_started = time.perf_counter()
        sanitized = (
            await sanitize_tts_text_via_service(
                raw_text,
                settings=settings,
                timeout_ms=timeout_ms,
                transform_profile=body.transform_profile or "speech",
                allow_llm_sanitizer=_allow_llm_sanitizer(body),
            )
            if _should_sanitize_text(body)
            else None
        )
        sanitizer_ms = (time.perf_counter() - sanitizer_started) * 1000
    except TtsSanitizerUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "engine": "none",
                "detail": str(exc),
                "sanitizer_url": resolve_tts_sanitizer_url(settings),
            },
        )
    text = sanitized.text if sanitized else raw_text

    if not text or not voice:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": "Missing effective text or voice in DB/call payload"},
        )

    if req_mode not in {"auto", "stream", "batch"}:
        req_mode = "stream"

    if req_mode == "auto":
        words = len([w for w in text.split() if w.strip()])
        effective_mode = "batch" if words <= threshold else "stream"
    else:
        effective_mode = req_mode

    client_key = _client_key(request, body.client_id)
    session, interrupted_previous = await _start_session(client_key, interrupt)
    probe_url = settings.get("tts.local_probe_url", "")
    speech_url = settings.get("tts.local_speech_url", "")

    probe_started = time.perf_counter()
    tts_available = bool(tts_enabled) and await _is_local_tts_available(probe_url, timeout_ms)
    probe_ms = (time.perf_counter() - probe_started) * 1000

    if not tts_available:
        await _clear_session_if_current(client_key, session)
        if not fallback_allowed:
            return JSONResponse(
                status_code=503,
                content={
                    "ok": False,
                    "engine": "none",
                    "interrupted_previous": interrupted_previous,
                    "detail": "Local PocketTTS unavailable and fallback disabled",
                },
            )

        fallback_kind = _resolve_fallback_kind(body, default_kind="positive")
        return _fallback_response(
            settings=settings,
            fallback_kind=fallback_kind,
            voice=voice,
            interrupted_previous=interrupted_previous,
            status_code_on_failure=503,
            detail_on_failure="Fallback sound path from DB does not exist on disk",
        )

    model_name = (settings.get("tts.model") or "pocket-tts").strip() or "pocket-tts"

    volume_gain = (
        body.volume_gain if body.volume_gain is not None else _shared_tts_volume_gain(settings)
    )
    payload = {
        "model": model_name,
        "voice": voice,
        "input": text,
        "response_format": fmt,
        "stream": effective_mode == "stream",
        "sanitize_text": False,
        "transform_profile": "none",
    }
    if volume_gain != 1.0:
        payload["volume_gain"] = volume_gain

    fallback_kind = _resolve_fallback_kind(body, default_kind="positive")
    client = httpx.AsyncClient(timeout=timeout_ms / 1000.0)
    resp = None
    try:
        req = client.build_request("POST", speech_url, json=payload)
        upstream_started = time.perf_counter()
        resp = await client.send(req, stream=True)
        upstream_headers_ms = (time.perf_counter() - upstream_started) * 1000
    except Exception as exc:
        await _clear_session_if_current(client_key, session)
        await client.aclose()
        if fallback_allowed:
            return _fallback_response(
                settings=settings,
                fallback_kind=fallback_kind,
                voice=voice,
                interrupted_previous=interrupted_previous,
                status_code_on_failure=502,
                detail_on_failure="Local PocketTTS request failed and fallback audio is unavailable",
            )
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "engine": "none",
                "interrupted_previous": interrupted_previous,
                "detail": f"Local PocketTTS request failed: {str(exc)[:240] or exc.__class__.__name__}",
            },
        )

    if resp.status_code >= 400:
        body = (await resp.aread() or b"")[:500]
        await resp.aclose()
        await client.aclose()
        await _clear_session_if_current(client_key, session)
        if fallback_allowed:
            return _fallback_response(
                settings=settings,
                fallback_kind=fallback_kind,
                voice=voice,
                interrupted_previous=interrupted_previous,
                status_code_on_failure=502,
                detail_on_failure="PocketTTS upstream rejected TTS request and fallback audio is unavailable",
            )
        return JSONResponse(
            status_code=502,
            content={
                "ok": False,
                "engine": "none",
                "interrupted_previous": interrupted_previous,
                "detail": "Local PocketTTS rejected TTS request",
                "upstream_status": resp.status_code,
                "upstream_body": body.decode("utf-8", "replace"),
            },
        )

    async def stream_audio():
        try:
            async for chunk in resp.aiter_bytes():
                if session.cancelled:
                    break
                if chunk:
                    yield chunk
        finally:
            try:
                await resp.aclose()
            finally:
                await client.aclose()
                await _clear_session_if_current(client_key, session)

    headers = {
        "X-Blueprints-TTS-Engine": "pockettts_stream"
        if effective_mode == "stream"
        else "pockettts_batch",
        "X-Blueprints-TTS-Interrupted-Previous": "true" if interrupted_previous else "false",
        "X-Blueprints-TTS-Voice": voice,
        "X-Blueprints-TTS-Sanitized": "true" if sanitized else "false",
        "X-Blueprints-TTS-Timing-Total-Prestream-Ms": str(
            round((time.perf_counter() - request_started) * 1000)
        ),
        "X-Blueprints-TTS-Timing-Sanitizer-Ms": str(round(sanitizer_ms or 0)),
        "X-Blueprints-TTS-Timing-Probe-Ms": str(round(probe_ms or 0)),
        "X-Blueprints-TTS-Timing-Upstream-Headers-Ms": str(round(upstream_headers_ms or 0)),
    }
    if sanitized:
        headers["X-Blueprints-TTS-Transforms"] = ",".join(sanitized.transforms)
        headers["X-Blueprints-TTS-Transform-Profile"] = body.transform_profile or "speech"
        headers["X-Blueprints-TTS-Allow-LLM-Sanitizer"] = (
            "true" if _allow_llm_sanitizer(body) else "false"
        )
    upstream_ct = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
    if upstream_ct.startswith("audio/"):
        media_type = upstream_ct
    elif fmt == "wav":
        media_type = "audio/wav"
    elif fmt == "mp3":
        media_type = "audio/mpeg"
    else:
        media_type = "application/octet-stream"
    return StreamingResponse(stream_audio(), media_type=media_type, headers=headers)


@router.post("/utterances", status_code=201)
async def tts_create_utterance(body: UtteranceRequest):
    """Publish a browser-directed Hermes speech command over the SSE bus.

    This route does not synthesize audio server-side. It records a short
    control event and active browser listeners call /tts/speak locally, which
    keeps playback near the operator device and preserves browser autoplay
    behavior.
    """
    text = (body.text or "").strip()
    if not text:
        return JSONResponse(
            status_code=400, content={"ok": False, "detail": "Missing utterance text"}
        )

    source = (body.source or "hermes-local").strip() or "hermes-local"
    agent_id = (body.agent_id or "hermes").strip() or "hermes"
    utterance_id = (body.utterance_id or f"utt_{uuid.uuid4().hex}").strip()
    client_id = (body.client_id or f"{source}:{agent_id}").strip()
    target = body.target or UtteranceTarget(
        kind="all_listeners", dedupe="one_webpage_per_client_ip_plus_phone"
    )
    created_at = body.created_at if body.created_at and body.created_at > 0 else time.time()
    settings, missing = _resolve_settings()
    shared_playback_volume = None if missing else _shared_tts_playback_volume(settings)
    shared_volume_gain = None if missing else _shared_tts_volume_gain(settings)

    payload: dict[str, Any] = {
        "utterance_id": utterance_id,
        "source": source,
        "agent_id": agent_id,
        "subagent_id": (body.subagent_id or "").strip(),
        "conversation_id": (body.conversation_id or "").strip(),
        "text": text,
        "voice": (body.voice or "").strip(),
        "mode": (body.mode or "stream").strip().lower() or "stream",
        "format": (body.format or "wav").strip().lower() or "wav",
        "interrupt": bool(body.interrupt) if body.interrupt is not None else False,
        "client_id": client_id,
        "sanitize_text": body.sanitize_text is not False,
        "transform_profile": body.transform_profile or "conversation",
        "allow_llm_sanitizer": bool(body.allow_llm_sanitizer),
        "volume": shared_playback_volume if shared_playback_volume is not None else body.volume,
        "volume_gain": body.volume_gain if body.volume_gain is not None else shared_volume_gain,
        "timeout_ms": body.timeout_ms,
        "allow_fallback": body.allow_fallback,
        "created_at": created_at,
        "metadata": body.metadata or {},
        "target": {
            "kind": target.kind or "all_listeners",
            "dedupe": target.dedupe or "one_webpage_per_client_ip_plus_phone",
        },
    }

    event = AppEvent.create(
        "tts.utterance.requested",
        "Hermes speech",
        "Hermes requested browser speech.",
        severity="info",
        source=source,
        payload=payload,
        event_id=_safe_event_id("tts-utterance-", utterance_id),
    )
    published = await publish_event(event)
    return {"ok": True, "event": published.model_dump(), "payload": payload}


@router.get("/utterances/recent")
async def tts_recent_utterances(
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    return {"ok": True, "events": _load_recent_utterance_events(limit)}


@router.post("/sanitize")
async def tts_sanitize(body: SanitizeRequest):
    settings, _missing = _resolve_settings()
    timeout_ms = _parse_int(settings.get("tts.timeout_ms"), 1000, 120000) or 12000
    try:
        result = await sanitize_tts_text_via_service(
            body.text,
            settings=settings,
            timeout_ms=timeout_ms,
            transform_profile=body.transform_profile or "speech",
            allow_llm_sanitizer=_allow_llm_sanitizer(body),
        )
    except TtsSanitizerUnavailable as exc:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": str(exc),
                "sanitizer_url": resolve_tts_sanitizer_url(settings),
            },
        )
    return {
        "ok": True,
        "text": result.text,
        "transforms": list(result.transforms),
        "profile": body.transform_profile or "speech",
        "allow_llm_sanitizer": _allow_llm_sanitizer(body),
        "sanitizer_url": resolve_tts_sanitizer_url(settings),
    }


@router.post("/stop")
async def tts_stop(body: StopRequest, request: Request):
    client_key = _client_key(request, body.client_id)
    stopped = False
    async with _active_sessions_lock:
        active = _active_sessions.get(client_key)
        if active:
            active.cancelled = True
            _active_sessions.pop(client_key, None)
            stopped = True
    return {"ok": True, "stopped": stopped}


@router.get("/status")
async def tts_status(request: Request):
    settings, missing = _resolve_settings()
    timeout_ms = _parse_int(settings.get("tts.timeout_ms"), 1000, 120000)
    available = False
    sanitizer_available = False
    if timeout_ms is not None and not missing:
        available = await _is_local_tts_available(
            settings.get("tts.local_probe_url", ""), timeout_ms
        )
        try:
            await sanitize_tts_text_via_service(
                "status",
                settings=settings,
                timeout_ms=timeout_ms,
                transform_profile="none",
            )
            sanitizer_available = True
        except TtsSanitizerUnavailable:
            sanitizer_available = False

    client_key = _client_key(request)
    active_session = False
    async with _active_sessions_lock:
        active_session = client_key in _active_sessions

    return {
        "ok": True,
        "config_ready": len(missing) == 0,
        "missing_settings": missing,
        "enabled": _parse_bool(settings.get("tts.enabled")),
        "local_pockettts_available": available,
        "local_sanitizer_available": sanitizer_available,
        "active_session": active_session,
        "active_sessions_total": len(_active_sessions),
        "default_voice": settings.get("tts.default_voice"),
        "default_message": settings.get("tts.default_message"),
        "default_mode": settings.get("tts.default_mode"),
        "tts_volume": settings.get("tts.volume"),
        "tts_playback_volume": _shared_tts_playback_volume(settings) if not missing else None,
        "tts_volume_gain": _shared_tts_volume_gain(settings) if not missing else None,
        "sfx_volume": settings.get("tts.fallback.volume"),
        "probe_url": settings.get("tts.local_probe_url"),
        "speech_url": settings.get("tts.local_speech_url"),
        "sanitizer_url": resolve_tts_sanitizer_url(settings),
    }
