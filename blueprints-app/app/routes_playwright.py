"""routes_playwright.py — Node-local Playwright proxy endpoints.

Proxy and status endpoints for the per-node Playwright stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import asyncio
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg
from .db import get_conn, get_setting

router = APIRouter(prefix="/playwright", tags=["playwright"])

_DEFAULT_BASE_URL = "http://localhost:18932"
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 90.0
_AUTOSTART_TIMEOUT = float(os.getenv("BLUEPRINTS_PLAYWRIGHT_AUTOSTART_TIMEOUT_SECONDS", "45"))
_READY_TIMEOUT = float(os.getenv("BLUEPRINTS_PLAYWRIGHT_READY_TIMEOUT_SECONDS", "45"))
_READY_POLL_SECONDS = 0.5
_START_HELPER = Path(
    os.getenv(
        "BLUEPRINTS_PLAYWRIGHT_START_HELPER",
        "/xarta-node/.lone-wolf/stacks/playwright/.claude/skills/"
        "dockge-stack-playwright/scripts/ensure-running.sh",
    )
)
_WATCHDOG_HELPER = Path(
    os.getenv(
        "BLUEPRINTS_PLAYWRIGHT_WATCHDOG_HELPER",
        "/xarta-node/.lone-wolf/stacks/playwright/.claude/skills/"
        "dockge-stack-playwright/scripts/arm-watchdog.sh",
    )
)
_RUNTIME_DIR = Path(
    os.getenv(
        "BLUEPRINTS_PLAYWRIGHT_RUNTIME_DIR",
        "/xarta-node/.lone-wolf/stacks/playwright/.runtime",
    )
)
_LEASE_FILE = _RUNTIME_DIR / "last-used"


def _playwright_autostart_available() -> bool:
    return _START_HELPER.exists() and os.access(_START_HELPER, os.X_OK)


def _playwright_unreachable_payload(url: str, error: str) -> dict[str, Any]:
    autostart_available = _playwright_autostart_available()
    return {
        "present": autostart_available,
        "reachable": False,
        "url": url,
        "autostart_available": autostart_available,
        "lifecycle": "stopped_or_unreachable" if autostart_available else "unavailable",
        "error": error,
    }


class _BrowserBody(BaseModel):
    url: str
    viewport: dict[str, Any] | None = None
    wait_until: str | None = None
    wait_for_selector: str | None = None
    wait_for_timeout_ms: int | None = None
    open_modal_id: str | None = None
    measure_selectors: list[str] | None = None
    full_page: bool | None = None
    ignore_https_errors: bool | None = None
    local_storage: dict[str, str] | None = None
    blueprints_auth: bool = False


def _base_url() -> str:
    with get_conn() as conn:
        return (
            get_setting(conn, "playwright.base_url", _DEFAULT_BASE_URL) or _DEFAULT_BASE_URL
        ).rstrip("/")


async def _playwright_health_body(base_url: str | None = None) -> dict[str, Any] | None:
    url = (base_url or _base_url()).rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
            if r.status_code >= 500:
                return None
            return r.json()
    except Exception:
        return None


async def _playwright_reachable(base_url: str | None = None) -> bool:
    body = await _playwright_health_body(base_url)
    return body is not None and body.get("reachable") is True


def _helper_error(prefix: str, stdout: bytes, stderr: bytes) -> str:
    text = (stderr or stdout).decode("utf-8", "replace").strip()
    if text:
        return f"{prefix}: {text[:500]}"
    return prefix


async def _run_helper(path: Path, timeout: float, *, required: bool) -> bool:
    if not path.exists():
        if required:
            raise HTTPException(503, f"Playwright helper missing: {path}")
        return False
    if not os.access(path, os.X_OK):
        if required:
            raise HTTPException(503, f"Playwright helper is not executable: {path}")
        return False

    proc = await asyncio.create_subprocess_exec(
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError as exc:
        with suppress(ProcessLookupError):
            proc.kill()
        with suppress(Exception):
            await proc.communicate()
        raise HTTPException(503, f"Playwright helper timed out: {path}") from exc

    if proc.returncode != 0:
        if required:
            message = _helper_error("Playwright helper failed", stdout, stderr)
            raise HTTPException(503, message)
        return False
    return True


async def _touch_playwright_lease() -> None:
    def write_lease() -> None:
        _RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        _LEASE_FILE.write_text(f"{int(time.time())}\n", encoding="utf-8")

    with suppress(Exception):
        await asyncio.to_thread(write_lease)
    with suppress(Exception):
        await _run_helper(_WATCHDOG_HELPER, 2.0, required=False)


async def _ensure_playwright_ready() -> str:
    base = _base_url()
    body = await _playwright_health_body(base)
    if body is not None and body.get("reachable") is True:
        await _touch_playwright_lease()
        return base

    started = body is not None
    if body is None:
        await _run_helper(_START_HELPER, _AUTOSTART_TIMEOUT, required=True)
        started = True

    deadline = asyncio.get_running_loop().time() + _READY_TIMEOUT
    while asyncio.get_running_loop().time() < deadline:
        body = await _playwright_health_body(base)
        if body is not None and body.get("reachable") is True:
            await _touch_playwright_lease()
            return base
        if body is None and not started:
            await _run_helper(_START_HELPER, _AUTOSTART_TIMEOUT, required=True)
            started = True
        await asyncio.sleep(_READY_POLL_SECONDS)

    raise HTTPException(503, "Playwright stack not reachable after auto-start")


def _stack_browser_payload(body: _BrowserBody) -> dict[str, Any]:
    payload = body.model_dump(exclude_none=True)
    if body.blueprints_auth:
        if not cfg.API_SECRET:
            raise HTTPException(500, "BLUEPRINTS_API_SECRET is not configured")
        local_storage = dict(payload.get("local_storage") or {})
        local_storage["blueprints_api_secret"] = cfg.API_SECRET
        payload["local_storage"] = local_storage
        payload.pop("blueprints_auth", None)
    return payload


@router.get("/health")
async def playwright_health() -> dict:
    """Check if the Playwright stack is reachable and return its health status."""
    url = _base_url()
    autostart_available = _playwright_autostart_available()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
        if r.status_code < 500:
            try:
                body = r.json()
            except Exception:
                body = {}
            return {"present": True, "autostart_available": autostart_available, "url": url, **body}
        return _playwright_unreachable_payload(url, f"HTTP {r.status_code}")
    except Exception as exc:
        return _playwright_unreachable_payload(url, str(exc))


@router.get("/tools")
async def playwright_tools() -> dict:
    """Return the direct MCP tool list from the local Playwright stack."""
    base = await _ensure_playwright_ready()
    try:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                r = await client.get(f"{base}/tools")
        finally:
            await _touch_playwright_lease()
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        return {
            "ok": True,
            "tool_count": len(data.get("tools", [])),
            "tools": data.get("tools", []),
            "_via": {"endpoint": f"{base}/tools"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Playwright tools fetch failed: {exc}") from exc


@router.post("/probe")
async def playwright_probe(body: _BrowserBody) -> dict:
    """Run a bounded browser navigation probe through the local Playwright stack."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    base = await _ensure_playwright_ready()
    payload = _stack_browser_payload(body)
    try:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
                r = await client.post(f"{base}/probe", json=payload)
        finally:
            await _touch_playwright_lease()
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/probe"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Playwright probe failed: {exc}") from exc


@router.post("/screenshot")
async def playwright_screenshot(body: _BrowserBody) -> dict:
    """Capture a full-page screenshot through the local Playwright stack."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    base = await _ensure_playwright_ready()
    payload = _stack_browser_payload(body)
    try:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
                r = await client.post(f"{base}/screenshot", json=payload)
        finally:
            await _touch_playwright_lease()
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/screenshot"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Playwright screenshot failed: {exc}") from exc
