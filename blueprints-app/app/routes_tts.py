"""Blueprints TTS wrapper endpoints with DB-driven policy and in-wrapper fallback audio."""

from __future__ import annotations

import asyncio
import mimetypes
import uuid
from dataclasses import dataclass
from pathlib import Path

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel

from . import config as cfg
from .db import get_conn, get_setting

router = APIRouter(prefix="/tts", tags=["tts"])


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


@dataclass
class _ActiveSession:
    session_id: str
    cancelled: bool = False


_active_sessions: dict[str, _ActiveSession] = {}
_active_sessions_lock = asyncio.Lock()


class SpeakRequest(BaseModel):
    text: str | None = None
    voice: str | None = None
    interrupt: bool | None = None
    mode: str | None = None
    format: str | None = None
    event_kind: str | None = None
    fallback_kind: str | None = None
    sentiment: str | None = None


class StopRequest(BaseModel):
    client_id: str | None = None


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
    return resolved, missing


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
    interrupt_default = _parse_bool(settings.get("tts.interrupt_default"))
    timeout_ms = _parse_int(settings.get("tts.timeout_ms"), 1000, 120000)
    threshold = _parse_int(settings.get("tts.stream_word_threshold"), 1, 2000)

    if None in {tts_enabled, fallback_enabled, interrupt_default, timeout_ms, threshold}:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": "TTS wrapper settings contain invalid values",
            },
        )

    text = (body.text or "").strip() or settings.get("tts.default_message", "")
    voice = (body.voice or "").strip() or settings.get("tts.default_voice", "")
    req_mode = (body.mode or "").strip().lower() or settings.get("tts.default_mode", "").strip().lower()
    fmt = (body.format or "wav").strip().lower()
    interrupt = bool(interrupt_default) if body.interrupt is None else bool(body.interrupt)

    if not text or not voice:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "Missing effective text or voice in DB/call payload"})

    if req_mode not in {"auto", "stream", "batch"}:
        req_mode = "stream"

    if req_mode == "auto":
        words = len([w for w in text.split() if w.strip()])
        effective_mode = "batch" if words <= threshold else "stream"
    else:
        effective_mode = req_mode

    client_key = _client_key(request)
    session, interrupted_previous = await _start_session(client_key, interrupt)

    probe_url = settings.get("tts.local_probe_url", "")
    speech_url = settings.get("tts.local_speech_url", "")

    tts_available = bool(tts_enabled) and await _is_local_tts_available(probe_url, timeout_ms)

    if not tts_available:
        await _clear_session_if_current(client_key, session)
        if not fallback_enabled:
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

    payload = {
        "model": model_name,
        "voice": voice,
        "input": text,
        "response_format": fmt,
        "stream": effective_mode == "stream",
    }

    fallback_kind = _resolve_fallback_kind(body, default_kind="positive")
    client = httpx.AsyncClient(timeout=timeout_ms / 1000.0)
    resp = None
    try:
        req = client.build_request("POST", speech_url, json=payload)
        resp = await client.send(req, stream=True)
    except Exception:
        await _clear_session_if_current(client_key, session)
        await client.aclose()
        if fallback_enabled:
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
                "detail": "Local PocketTTS request failed",
            },
        )

    if resp.status_code >= 400:
        body = (await resp.aread() or b"")[:500]
        await resp.aclose()
        await client.aclose()
        await _clear_session_if_current(client_key, session)
        if fallback_enabled:
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
        "X-Blueprints-TTS-Engine": "pockettts_stream" if effective_mode == "stream" else "pockettts_batch",
        "X-Blueprints-TTS-Interrupted-Previous": "true" if interrupted_previous else "false",
        "X-Blueprints-TTS-Voice": voice,
    }
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
    if timeout_ms is not None and not missing:
        available = await _is_local_tts_available(settings.get("tts.local_probe_url", ""), timeout_ms)

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
        "active_session": active_session,
        "active_sessions_total": len(_active_sessions),
        "default_voice": settings.get("tts.default_voice"),
        "default_message": settings.get("tts.default_message"),
        "default_mode": settings.get("tts.default_mode"),
        "tts_volume": settings.get("tts.volume"),
        "sfx_volume": settings.get("tts.fallback.volume"),
        "probe_url": settings.get("tts.local_probe_url"),
        "speech_url": settings.get("tts.local_speech_url"),
    }
