"""Node-local browser voice-mode lease endpoints."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import websockets
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .db import get_conn, get_setting
from .events import AppEvent
from .routes_events import publish_event
from .routes_matrix_chat import _settings as _matrix_chat_settings

router = APIRouter(prefix="/voice-mode", tags=["voice-mode"])

_STATE_PATH = Path("/xarta-node/.lone-wolf/state/blueprints-voice-mode.json")
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


class BrowserVoiceState(BaseModel):
    browser_id: str
    browser_label: str | None = None
    stt_enabled: bool = False
    stt_mode: str | None = None
    tts_enabled: bool = False


class VoiceModePolicy(BaseModel):
    tts_companion_model_preference: str | None = None


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
            with contextlib.suppress(Exception):
                await ws.send(json.dumps({"type": "ping"}))
        return {"ok": True, "status": "open"}
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


async def _noise_component(settings: dict[str, str]) -> dict[str, Any]:
    ws_url = str(settings.get("stt_noise_dfn_ws_url") or "").strip()
    if not ws_url:
        return _component(
            "noise_reduction",
            "Noise reduction",
            configured=False,
            issue="noise reduction not configured",
        )
    probe = await _probe_websocket_open(ws_url)
    if probe.get("ok"):
        return _component("noise_reduction", "Noise reduction", ok=True, detail={"ws_url": ws_url, "probe": probe})

    machine = _machine_for_host(_url_host(ws_url))
    diagnostic = {"ws_url": ws_url, "probe": probe, "machine": machine}
    parent = await _pve_lxc_status(machine) if machine else {"ok": False, "issue": "", "detail": {}}
    diagnostic.update(parent.get("detail") or {})
    if parent.get("issue"):
        return _component("noise_reduction", "Noise reduction", issue=parent["issue"], detail=diagnostic)
    stack = await _active_mode_stack_status(_NOISE_STACK_NAMES, machine)
    diagnostic.update(stack.get("detail") or {})
    if stack.get("issue"):
        return _component("noise_reduction", "Noise reduction", issue=stack["issue"], detail=diagnostic)
    return _component("noise_reduction", "Noise reduction", issue="noise reduction stack bad health", detail=diagnostic)


async def _build_dependency_health() -> dict[str, Any]:
    settings = _matrix_chat_settings("tb1")
    stt, noise, tts = await asyncio.gather(
        _stt_component(settings),
        _noise_component(settings),
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
    }


async def _dependency_health_payload(force: bool = False) -> dict[str, Any]:
    now = time.time()
    async with _dependency_health_lock:
        cached = _dependency_health_cache.get("payload")
        checked_at = float(_dependency_health_cache.get("checked_at") or 0.0)
        next_check = float(_dependency_health_cache.get("next_check_seconds") or 0.0)
        if not force and cached and now - checked_at < next_check:
            payload = dict(cached)
            payload["cached"] = True
            payload["cache_age_seconds"] = round(now - checked_at, 3)
            return payload
        payload = await _build_dependency_health()
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


def _public_state(state: dict[str, Any]) -> dict[str, Any]:
    active = state.get("active") if isinstance(state.get("active"), dict) else None
    if active:
        active = dict(active)
        active["stt_mode"] = _clean_stt_mode(active.get("stt_mode"), bool(active.get("stt_enabled")))
        active["stt_enabled"] = bool(active["stt_mode"])
    return {
        "ok": True,
        "active": active,
        "policy": _clean_policy(state.get("policy")),
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
            "policy": public["policy"],
            "revision": public["revision"],
            "updated_at": public["updated_at"],
        },
    )
    await publish_event(event)


@router.get("/status")
async def voice_mode_status() -> dict[str, Any]:
    async with _state_lock:
        return _public_state(_read_state_unlocked())


@router.get("/dependency-health")
async def voice_mode_dependency_health(force: bool = False) -> dict[str, Any]:
    return await _dependency_health_payload(force=force)


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

    now = time.time()
    active = {
        "browser_id": browser_id,
        "browser_label": _clean_label(body.browser_label, "Blueprints browser"),
        "stt_enabled": bool(stt_mode),
        "stt_mode": stt_mode,
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
