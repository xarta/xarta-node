"""routes_playwright.py — Node-local Playwright proxy endpoints.

Proxy and status endpoints for the per-node Playwright stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

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


async def _playwright_reachable() -> bool:
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
            if r.status_code >= 500:
                return False
            body = r.json()
            return body.get("reachable") is True
    except Exception:
        return False


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
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
        if r.status_code < 500:
            try:
                body = r.json()
            except Exception:
                body = {}
            return {"present": True, "url": url, **body}
        return {
            "present": True,
            "reachable": False,
            "url": url,
            "error": f"HTTP {r.status_code}",
        }
    except Exception as exc:
        return {"present": False, "reachable": False, "url": url, "error": str(exc)}


@router.get("/tools")
async def playwright_tools() -> dict:
    """Return the direct MCP tool list from the local Playwright stack."""
    if not await _playwright_reachable():
        raise HTTPException(503, "Playwright stack not reachable")

    base = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            r = await client.get(f"{base}/tools")
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

    if not await _playwright_reachable():
        raise HTTPException(503, "Playwright stack not reachable")

    base = _base_url()
    payload = _stack_browser_payload(body)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/probe", json=payload)
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

    if not await _playwright_reachable():
        raise HTTPException(503, "Playwright stack not reachable")

    base = _base_url()
    payload = _stack_browser_payload(body)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/screenshot", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/screenshot"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Playwright screenshot failed: {exc}") from exc
