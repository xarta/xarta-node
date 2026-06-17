"""Coalesced read-only proxy for the local AI GPU monitor."""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/gpu-monitor", tags=["gpu-monitor"])

_DEFAULT_CACHE_TTL_MS = 500
_DEFAULT_TIMEOUT_SECONDS = 2.5

_cache_lock = asyncio.Lock()
_cache_payload: dict[str, Any] | None = None
_cache_fetched_monotonic = 0.0
_cache_fetched_at = ""
_inflight_task: asyncio.Task[dict[str, Any]] | None = None


def _monitor_url() -> str:
    return os.getenv("BLUEPRINTS_GPU_MONITOR_URL", "").strip()


def _cache_ttl_ms() -> int:
    raw = os.getenv("BLUEPRINTS_GPU_MONITOR_CACHE_TTL_MS", "").strip()
    try:
        return max(100, min(5000, int(raw)))
    except (TypeError, ValueError):
        return _DEFAULT_CACHE_TTL_MS


def _timeout_seconds() -> float:
    raw = os.getenv("BLUEPRINTS_GPU_MONITOR_TIMEOUT_SECONDS", "").strip()
    try:
        return max(0.5, min(10.0, float(raw)))
    except (TypeError, ValueError):
        return _DEFAULT_TIMEOUT_SECONDS


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def _fetch_monitor_source() -> dict[str, Any]:
    url = _monitor_url()
    if not url:
        raise RuntimeError("GPU monitor URL is not configured")
    timeout = _timeout_seconds()
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(timeout, connect=min(1.0, timeout)),
    ) as client:
        response = await client.get(url)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("GPU monitor returned a non-object payload")
    if not isinstance(payload.get("gpus"), list):
        raise RuntimeError("GPU monitor payload is missing gpus[]")
    return payload


async def _coalesced_monitor(*, force: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    global _cache_payload, _cache_fetched_at, _cache_fetched_monotonic, _inflight_task

    ttl_seconds = _cache_ttl_ms() / 1000.0
    now = time.monotonic()
    if not force and _cache_payload is not None and now - _cache_fetched_monotonic <= ttl_seconds:
        return _cache_payload, {
            "cached": True,
            "stale": False,
            "fetched_at": _cache_fetched_at,
            "cache_ttl_ms": _cache_ttl_ms(),
        }

    async with _cache_lock:
        now = time.monotonic()
        if (
            not force
            and _cache_payload is not None
            and now - _cache_fetched_monotonic <= ttl_seconds
        ):
            return _cache_payload, {
                "cached": True,
                "stale": False,
                "fetched_at": _cache_fetched_at,
                "cache_ttl_ms": _cache_ttl_ms(),
            }
        if _inflight_task is None or _inflight_task.done():
            _inflight_task = asyncio.create_task(_fetch_monitor_source())
        task = _inflight_task

    try:
        payload = await task
    except Exception:
        async with _cache_lock:
            if _inflight_task is task:
                _inflight_task = None
            stale_payload = _cache_payload
            stale_fetched_at = _cache_fetched_at
        if stale_payload is not None:
            return stale_payload, {
                "cached": True,
                "stale": True,
                "fetched_at": stale_fetched_at,
                "cache_ttl_ms": _cache_ttl_ms(),
            }
        raise

    fetched_at = _utc_now_iso()
    async with _cache_lock:
        _cache_payload = payload
        _cache_fetched_monotonic = time.monotonic()
        _cache_fetched_at = fetched_at
        if _inflight_task is task:
            _inflight_task = None

    return payload, {
        "cached": False,
        "stale": False,
        "fetched_at": fetched_at,
        "cache_ttl_ms": _cache_ttl_ms(),
    }


@router.get("/local-ai")
async def local_ai_gpu_monitor(force: bool = False) -> dict[str, Any]:
    """Return local AI GPU monitor telemetry through one short-lived shared sample."""
    try:
        monitor, meta = await _coalesced_monitor(force=force)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"GPU monitor returned HTTP {exc.response.status_code}",
        ) from exc
    except (httpx.TimeoutException, httpx.RequestError, RuntimeError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return {
        "ok": True,
        "source": "local-ai",
        "monitor": monitor,
        **meta,
    }


def _reset_cache_for_tests() -> None:
    global _cache_payload, _cache_fetched_at, _cache_fetched_monotonic, _inflight_task
    _cache_payload = None
    _cache_fetched_at = ""
    _cache_fetched_monotonic = 0.0
    _inflight_task = None
