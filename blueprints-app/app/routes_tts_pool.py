"""routes_tts_pool.py — TTS pool test endpoints.

Provides a backend proxy and helper surface for the Blueprints TTS pool test page.
All connection parameters are DB-driven via the settings table (tts.pool.* keys).
No infra-specific values are hardcoded here.

Endpoints:
    GET  /api/v1/tts-pool/status            — pool health check (stack reachability + config)
    POST /api/v1/tts-pool/synthesize        — proxy POST /v1/audio/speech to active TTS stack
    GET  /api/v1/tts-pool/voice-samples     — list node-local user voice samples
    GET  /api/v1/tts-pool/stack-voices      — list files in the configured stack voices dir
    POST /api/v1/tts-pool/push-voice/{fn}   — push a voice sample to the configured TTS stack
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from .db import get_conn, get_setting
from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args

router = APIRouter(prefix="/tts-pool", tags=["tts-pool"])

# ── Settings keys ─────────────────────────────────────────────────────────────

_POOL_SETTINGS: tuple[str, ...] = (
    "tts.pool.contexts_json",
    "tts.pool.active_context",
    "tts.pool.voice_samples_path",
    "tts.pool.timeout_ms",
)

# Standard OpenAI TTS voice names forwarded as-is to the active TTS stack
_STANDARD_VOICES = ["alloy", "echo", "fable", "onyx", "nova", "shimmer"]

# Audio formats accepted by the OpenAI-compatible /v1/audio/speech endpoint
_SUPPORTED_FORMATS = ["wav", "mp3", "opus", "flac", "aac", "pcm"]

# Valid audio file extensions for voice samples
_AUDIO_EXTS = {".wav", ".mp3"}

# Load infrastructure paths from environment (set in private .env, not public source)
# These must be defined in the deployment environment; there are no public defaults
_LOCAL_LITELLM_SYNC = Path(
    os.environ.get("TTS_POOL_LOCAL_LITELLM_SYNC_PATH") or ""
)
_SECONDARY_LITELLM_SYNC = Path(
    os.environ.get("TTS_POOL_SECONDARY_LITELLM_SYNC_PATH") or ""
)
_REMOTE_LITELLM_SYNC = Path(
    os.environ.get("TTS_POOL_REMOTE_LITELLM_SYNC_PATH") or ""
)
_SYNC_PYTHON = Path(os.environ.get("TTS_POOL_SYNC_PYTHON", "/usr/bin/python3"))
_SYNC_TIMEOUT_SECONDS = int(os.environ.get("TTS_POOL_SYNC_TIMEOUT_SECONDS", "360"))

# ── Request models ────────────────────────────────────────────────────────────


class PoolSynthesizeRequest(BaseModel):
    text: str
    voice: str = "alloy"
    format: str = "wav"


class PoolActivateRequest(BaseModel):
    context_key: str


# ── Settings helpers ──────────────────────────────────────────────────────────


def _resolve_pool_settings() -> tuple[dict[str, str], list[str]]:
    resolved: dict[str, str] = {}
    missing: list[str] = []
    with get_conn() as conn:
        for key in _POOL_SETTINGS:
            val = get_setting(conn, key)
            if key == "tts.pool.timeout_ms":
                if val is not None and str(val).strip():
                    resolved[key] = str(val).strip()
                continue
            if val is not None and str(val).strip():
                resolved[key] = str(val).strip()
            else:
                missing.append(key)
    return resolved, missing


def _parse_contexts_json(raw: str) -> list[dict[str, Any]]:
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    contexts: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        label = str(item.get("label") or key).strip()
        menu_label = str(
            item.get("menu_label") or item.get("short_label") or item.get("nav_label") or ""
        ).strip()
        base_url = str(item.get("base_url") or "").strip().rstrip("/")
        ssh_host = str(item.get("ssh_host") or "").strip()
        voices_path = str(item.get("voices_path") or "").strip()
        provider_alias = str(item.get("provider_alias") or "").strip()
        private_alias = str(item.get("private_alias") or "").strip()
        repo_url = str(item.get("repo_url") or item.get("upstream_url") or "").strip()
        docs_url = str(item.get("docs_url") or "").strip()
        model_url = str(item.get("model_url") or "").strip()
        if not key:
            continue
        contexts.append(
            {
                "key": key,
                "label": label or key,
                "menu_label": menu_label or _derive_context_menu_label(label or key),
                "description": str(item.get("description") or "").strip(),
                "base_url": base_url,
                "ssh_host": ssh_host,
                "voices_path": voices_path,
                "provider_alias": provider_alias,
                "private_alias": private_alias,
                "repo_url": repo_url,
                "docs_url": docs_url,
                "model_url": model_url,
                "supports_streaming": bool(item.get("supports_streaming", False)),
                "supports_voice_cloning": bool(item.get("supports_voice_cloning", False)),
            }
        )
    return contexts


def _resolve_contexts(settings: dict[str, str]) -> list[dict[str, Any]]:
    return _parse_contexts_json(settings.get("tts.pool.contexts_json", ""))


def _resolve_active_context(settings: dict[str, str]) -> tuple[dict[str, Any] | None, list[str], list[dict[str, Any]]]:
    contexts = _resolve_contexts(settings)
    missing: list[str] = []
    if not contexts:
        missing.append("tts.pool.contexts_json")
        return None, missing, contexts

    active_key = str(settings.get("tts.pool.active_context") or "").strip()
    chosen = next((ctx for ctx in contexts if ctx.get("key") == active_key), None)
    if chosen is None:
        chosen = contexts[0]

    if not chosen.get("base_url"):
        missing.append("tts.pool.contexts_json.base_url")
    return chosen, missing, contexts


def _pool_timeout(settings: dict[str, str]) -> float:
    try:
        return max(5.0, int(settings.get("tts.pool.timeout_ms", "30000")) / 1000.0)
    except (ValueError, TypeError):
        return 30.0


def _samples_path(settings: dict[str, str]) -> Path | None:
    raw = settings.get("tts.pool.voice_samples_path", "").strip()
    if not raw:
        return None
    return Path(raw)


def _sanitize_filename(filename: str) -> str | None:
    """Return the basename if safe (no path components, no dots-only names).

    Returns None if the filename is invalid.
    """
    safe = os.path.basename(filename)
    if not safe or safe != filename or ".." in filename:
        return None
    return safe


def _derive_context_menu_label(label: str) -> str:
    collapsed = " ".join(str(label or "").split())
    if not collapsed:
        return ""
    collapsed = collapsed.replace("MOSS-TTS Local ", "MOSS-TTS ")
    collapsed = collapsed.replace(" Local ", " ")
    return collapsed


def _tts_api_base(base_url: str) -> str:
    clean = str(base_url or "").strip().rstrip("/")
    if clean.endswith("/v1"):
        return clean
    return f"{clean}/v1"


def _build_tts_alias_spec(context: dict[str, Any]) -> dict[str, Any]:
    return {
        "tts_openai": {
            "litellm_params": {
                "model": "openai/tts-1",
                "api_base": _tts_api_base(str(context.get("base_url") or "")),
                "api_key": "os.environ/LOCAL_VLLM_KEY",
            }
        }
    }


def _parse_json_tail(text: str) -> dict[str, Any]:
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


def _run_tts_alias_sync(
    script_path: Path,
    *,
    spec_path: Path,
    spec_label: str,
    extra_args: list[str] | None = None,
) -> dict[str, Any]:
    if not _SYNC_PYTHON.is_file():
        return {"ok": False, "error": f"Python runtime not found: {_SYNC_PYTHON}"}
    if not script_path.is_file():
        return {"ok": False, "error": f"Sync helper not found: {script_path}"}

    cmd = [
        str(_SYNC_PYTHON),
        str(script_path),
        "sync-now",
        "--apply",
        "--spec-file",
        str(spec_path),
        "--spec-label",
        spec_label,
    ]
    if extra_args:
        cmd.extend(extra_args)
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_SYNC_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"Timed out after {_SYNC_TIMEOUT_SECONDS}s"}
    except OSError as exc:
        return {"ok": False, "error": str(exc)}

    summary = _parse_json_tail(result.stdout)
    if not summary:
        summary = {
            "ok": result.returncode == 0,
            "stdout": (result.stdout or "").strip()[-4000:],
            "stderr": (result.stderr or "").strip()[-4000:],
        }
    summary.setdefault("ok", result.returncode == 0)
    if result.returncode != 0:
        summary.setdefault("stderr", (result.stderr or "").strip()[-4000:])
        summary.setdefault("stdout", (result.stdout or "").strip()[-4000:])
    return summary


def _promote_tts_aliases(context: dict[str, Any]) -> dict[str, Any]:
    spec = _build_tts_alias_spec(context)
    spec_label = str(context.get("menu_label") or context.get("label") or context.get("key") or "tts-pool")
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(spec, handle)
        spec_path = Path(handle.name)

    try:
        local = _run_tts_alias_sync(
            _LOCAL_LITELLM_SYNC,
            spec_path=spec_path,
            spec_label=spec_label,
            extra_args=["--skip-secondary", "--skip-remote"],
        )
        secondary = _run_tts_alias_sync(
            _SECONDARY_LITELLM_SYNC,
            spec_path=spec_path,
            spec_label=spec_label,
        )
        remote = _run_tts_alias_sync(
            _REMOTE_LITELLM_SYNC,
            spec_path=spec_path,
            spec_label=spec_label,
        )
    finally:
        try:
            spec_path.unlink(missing_ok=True)
        except OSError:
            pass

    return {
        "ok": bool(local.get("ok") and secondary.get("ok") and remote.get("ok")),
        "status": "applied",
        "detail": "PRIMARY-OPENAI-TTS aliases promoted from the selected TTS pool context.",
        "applied_at": int(time.time()),
        "spec": spec,
        "targets": {
            "local": local,
            "secondary": secondary,
            "remote": remote,
        },
    }


# ── Stack SSH helper (sync, called via asyncio.to_thread) ─────────────────────


def _list_stack_voices_sync(ssh_ip: str, voices_path: str) -> list[str]:
    """Return sorted list of filenames in voices_path on the configured SSH host.

    Runs `ls -1` over SSH and returns clean filenames.
    Returns empty list on any SSH/command error.
    """
    try:
        args = make_ssh_args(ssh_ip)
    except (SshTargetNotFound, SshKeyMissing):
        return []

    cmd = ["ssh"] + args + [f"root@{ssh_ip}", f"ls -1 {voices_path} 2>/dev/null"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=12)
        if result.returncode != 0:
            return []
        return [
            line.strip()
            for line in result.stdout.splitlines()
            if line.strip()
        ]
    except (subprocess.TimeoutExpired, OSError):
        return []


def _push_voice_sync(
    ssh_ip: str,
    src_path: str,
    dest_dir: str,
    filename: str,
) -> tuple[bool, str]:
    """SCP a single file to the stack's voices directory.

    Returns (success: bool, message: str).
    """
    try:
        ssh_args = make_ssh_args(ssh_ip)
    except SshTargetNotFound as exc:
        return False, str(exc)
    except SshKeyMissing as exc:
        return False, str(exc)

    # Build SCP args from the SSH args (translate -b → -o BindAddress=)
    scp_opts: list[str] = []
    i = 0
    while i < len(ssh_args):
        arg = ssh_args[i]
        if arg == "-i":
            scp_opts += ["-i", ssh_args[i + 1]]
            i += 2
        elif arg == "-b":
            scp_opts += ["-o", f"BindAddress={ssh_args[i + 1]}"]
            i += 2
        elif arg == "-o":
            scp_opts += ["-o", ssh_args[i + 1]]
            i += 2
        else:
            i += 1

    dest = f"root@{ssh_ip}:{dest_dir}/{filename}"
    cmd = ["scp"] + scp_opts + [src_path, dest]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return True, f"Pushed {filename}"
        detail = (result.stderr or result.stdout or "unknown error").strip()
        return False, f"SCP failed: {detail}"
    except subprocess.TimeoutExpired:
        return False, "SCP timed out"
    except OSError as exc:
        return False, f"SCP error: {exc}"


async def _probe_tts_endpoint(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    try:
        resp = await client.get(url)
    except httpx.TimeoutException:
        return {"ok": False, "status": "timeout", "error": "timeout"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "status": "error", "error": str(exc)[:256]}

    result: dict[str, Any] = {"ok": resp.is_success, "status": resp.status_code}
    body: Any = None
    try:
        body = resp.json()
    except Exception:
        body = None

    if body is not None:
        result["body"] = body
    else:
        text = (resp.text or "").strip()
        if text:
            result["detail"] = text[:512]
    return result


# ── Routes ────────────────────────────────────────────────────────────────────


@router.get("/status")
async def tts_pool_status():
    """Return the pool health status.

    Checks:
    - Whether required settings are configured.
    - Whether the configured TTS stack is reachable and healthy.
    """
    settings, missing = _resolve_pool_settings()
    active_context, context_missing, contexts = _resolve_active_context(settings)
    missing = [item for item in missing if item != "tts.pool.active_context"] + context_missing
    result: dict[str, Any] = {
        "configured": len(missing) == 0,
        "missing_settings": missing,
        "contexts": contexts,
        "active_context": active_context,
        "stack": None,
    }

    base_url = str((active_context or {}).get("base_url") or "").rstrip("/")
    if not base_url:
        result["configured"] = False
        return JSONResponse(status_code=200, content=result)

    timeout = _pool_timeout(settings)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            health, models, voices = await asyncio.gather(
                _probe_tts_endpoint(client, f"{base_url}/health"),
                _probe_tts_endpoint(client, f"{base_url}/v1/models"),
                _probe_tts_endpoint(client, f"{base_url}/v1/voices"),
            )
            stack_data: dict[str, Any] = {
                "reachable": health.get("status") not in {"timeout", "error"},
                "http_status": health.get("status"),
                "body": health.get("body"),
                "error": health.get("error"),
                "endpoints": {
                    "health": health,
                    "models": models,
                    "voices": voices,
                },
            }

            model_body = models.get("body")
            if isinstance(model_body, dict) and isinstance(model_body.get("data"), list):
                stack_data["model_ids"] = [
                    str(row.get("id") or "").strip()
                    for row in model_body["data"]
                    if isinstance(row, dict) and str(row.get("id") or "").strip()
                ]

            voice_body = voices.get("body")
            voice_ids: list[str] = []
            if isinstance(voice_body, dict) and isinstance(voice_body.get("data"), list):
                for row in voice_body["data"]:
                    if isinstance(row, dict):
                        voice_id = str(row.get("id") or row.get("name") or "").strip()
                    else:
                        voice_id = str(row).strip()
                    if voice_id:
                        voice_ids.append(voice_id)
            elif isinstance(voice_body, list):
                for row in voice_body:
                    if isinstance(row, dict):
                        voice_id = str(row.get("id") or row.get("name") or "").strip()
                    else:
                        voice_id = str(row).strip()
                    if voice_id:
                        voice_ids.append(voice_id)
            stack_data["voice_ids"] = voice_ids
            result["stack"] = stack_data
    except httpx.TimeoutException:
        result["stack"] = {"reachable": False, "error": "timeout"}
    except Exception as exc:
        result["stack"] = {"reachable": False, "error": str(exc)[:256]}

    return JSONResponse(status_code=200, content=result)


@router.get("/contexts")
async def tts_pool_contexts():
    settings, _ = _resolve_pool_settings()
    active_context, missing, contexts = _resolve_active_context(settings)
    return JSONResponse(
        content={
            "ok": len(missing) == 0,
            "missing_settings": missing,
            "contexts": contexts,
            "active_context": active_context,
        }
    )


@router.post("/activate")
async def tts_pool_activate(body: PoolActivateRequest):
    context_key = (body.context_key or "").strip()
    if not context_key:
        return JSONResponse(status_code=400, content={"ok": False, "detail": "context_key is required"})

    settings, _ = _resolve_pool_settings()
    _, _, contexts = _resolve_active_context(settings)
    chosen = next((ctx for ctx in contexts if ctx.get("key") == context_key), None)
    if chosen is None:
        return JSONResponse(status_code=404, content={"ok": False, "detail": f"Unknown TTS context: {context_key}"})

    with get_conn() as conn:
        conn.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')",
            ("tts.pool.active_context", context_key),
        )

    promotion = await asyncio.to_thread(_promote_tts_aliases, chosen)
    status_code = 200 if promotion.get("ok") else 500

    return JSONResponse(
        status_code=status_code,
        content={
            "ok": bool(promotion.get("ok")),
            "active_context": chosen,
            "detail": None if promotion.get("ok") else "TTS alias promotion failed on one or more LiteLLM targets.",
            "promotion": promotion,
        }
    )


@router.post("/synthesize")
async def tts_pool_synthesize(body: PoolSynthesizeRequest):
    """Proxy a synthesis request to the active TTS stack.

    Forwards POST /v1/audio/speech and streams the audio response back,
    preserving X-TTFA and X-RTF headers where present.
    """
    settings, missing = _resolve_pool_settings()
    active_context, context_missing, _ = _resolve_active_context(settings)

    unconfigured = missing + context_missing
    if unconfigured:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": "TTS pool active context is not fully configured",
                "missing_settings": unconfigured,
            },
        )

    # Validate format
    fmt = (body.format or "wav").lower()
    if fmt not in _SUPPORTED_FORMATS:
        fmt = "wav"

    base_url = str((active_context or {}).get("base_url") or "").rstrip("/")
    speech_url = f"{base_url}/v1/audio/speech"
    timeout = _pool_timeout(settings)

    payload = {
        "model": "tts-1",
        "input": body.text,
        "voice": body.voice,
        "response_format": fmt,
    }

    # Use httpx with stream=True so headers are available before body is read.
    # We do NOT use a context-manager here so we can return a StreamingResponse
    # that drains the body asynchronously.
    client = httpx.AsyncClient(timeout=timeout)
    try:
        req = client.build_request("POST", speech_url, json=payload)
        resp = await client.send(req, stream=True)
    except httpx.TimeoutException:
        await client.aclose()
        return JSONResponse(status_code=504, content={"ok": False, "detail": "TTS stack timed out"})
    except Exception as exc:
        await client.aclose()
        return JSONResponse(status_code=502, content={"ok": False, "detail": str(exc)[:256]})

    if resp.status_code != 200:
        await resp.aread()
        await resp.aclose()
        await client.aclose()
        return JSONResponse(
            status_code=resp.status_code,
            content={"ok": False, "detail": f"TTS stack returned HTTP {resp.status_code}"},
        )

    # Capture forwarded headers before streaming body
    fwd_headers: dict[str, str] = {}
    ttfa = resp.headers.get("x-ttfa") or resp.headers.get("X-TTFA")
    rtf = resp.headers.get("x-rtf") or resp.headers.get("X-RTF")
    if ttfa:
        fwd_headers["X-TTFA"] = ttfa
    if rtf:
        fwd_headers["X-RTF"] = rtf

    content_type = resp.headers.get("content-type", f"audio/{fmt}")

    async def _stream_body():
        try:
            async for chunk in resp.aiter_bytes(4096):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return StreamingResponse(
        _stream_body(),
        media_type=content_type,
        headers=fwd_headers,
    )


@router.get("/voice-samples")
async def tts_pool_voice_samples():
    """List user voice samples from the node-local voices_user directory.

    Also checks which samples are already present in the configured stack's voices
    directory via SSH, providing the per-sample push-state indicator.
    """
    settings, _ = _resolve_pool_settings()
    active_context, _, _ = _resolve_active_context(settings)
    samples_dir = _samples_path(settings)
    ssh_ip = str((active_context or {}).get("ssh_host") or "")
    voices_path = str((active_context or {}).get("voices_path") or "")

    samples: list[dict[str, Any]] = []
    if samples_dir and samples_dir.is_dir():
        for f in sorted(samples_dir.iterdir()):
            if f.suffix.lower() in _AUDIO_EXTS and f.is_file():
                samples.append(
                    {
                        "filename": f.name,
                        "size_bytes": f.stat().st_size,
                        "format": f.suffix.lower().lstrip("."),
                    }
                )

    # Fetch stack voice list via SSH (best-effort; empty on failure)
    stack_voices: set[str] = set()
    if ssh_ip:
        raw = await asyncio.to_thread(_list_stack_voices_sync, ssh_ip, voices_path)
        stack_voices = set(raw)

    for s in samples:
        s["on_stack"] = s["filename"] in stack_voices

    return JSONResponse(
        content={
            "samples": samples,
            "stack_voices_path": voices_path,
            "stack_ssh_configured": bool(ssh_ip),
        }
    )


@router.get("/stack-voices")
async def tts_pool_stack_voices():
    """List files currently in the configured TTS stack's voices directory."""
    settings, _ = _resolve_pool_settings()
    active_context, _, _ = _resolve_active_context(settings)
    ssh_ip = str((active_context or {}).get("ssh_host") or "")
    voices_path = str((active_context or {}).get("voices_path") or "")

    if not ssh_ip:
        return JSONResponse(
            content={"files": [], "configured": False, "path": voices_path}
        )

    files = await asyncio.to_thread(_list_stack_voices_sync, ssh_ip, voices_path)
    return JSONResponse(content={"files": files, "configured": True, "path": voices_path})


@router.post("/push-voice/{filename}")
async def tts_pool_push_voice(filename: str):
    """Push a voice sample from the node-local voice samples directory to the
    configured TTS stack via SCP. The destination path is set by
    the active TTS pool context in the database.
    """
    settings, missing = _resolve_pool_settings()
    active_context, context_missing, _ = _resolve_active_context(settings)

    missing_required = [k for k in missing if k == "tts.pool.voice_samples_path"] + context_missing
    if missing_required:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "detail": "SSH or voice sample path settings not configured",
                "missing_settings": missing_required,
            },
        )

    safe_name = _sanitize_filename(filename)
    if not safe_name:
        return JSONResponse(
            status_code=400, content={"ok": False, "detail": "Invalid filename"}
        )

    ext = Path(safe_name).suffix.lower()
    if ext not in _AUDIO_EXTS:
        return JSONResponse(
            status_code=400,
            content={"ok": False, "detail": f"Unsupported file extension: {ext!r}"},
        )

    samples_dir = _samples_path(settings)
    if not samples_dir:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "detail": "Voice samples path not configured"},
        )

    src = samples_dir / safe_name
    if not src.is_file():
        return JSONResponse(
            status_code=404,
            content={"ok": False, "detail": f"Voice sample not found: {safe_name}"},
        )

    ssh_ip = str((active_context or {}).get("ssh_host") or "")
    voices_path = str((active_context or {}).get("voices_path") or "")

    ok, msg = await asyncio.to_thread(
        _push_voice_sync,
        ssh_ip,
        str(src),
        voices_path,
        safe_name,
    )

    if ok:
        return JSONResponse(content={"ok": True, "pushed": safe_name, "detail": msg})
    return JSONResponse(status_code=500, content={"ok": False, "detail": msg})
