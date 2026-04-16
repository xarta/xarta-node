"""routes_playwright.py — Node-local Playwright proxy endpoints.

Proxy and status endpoints for the per-node Playwright stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .db import get_conn, get_setting

router = APIRouter(prefix="/playwright", tags=["playwright"])

_DEFAULT_BASE_URL = "http://localhost:18932"
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 90.0


class _UrlBody(BaseModel):
    url: str


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
async def playwright_probe(body: _UrlBody) -> dict:
    """Run a bounded browser navigation probe through the local Playwright stack."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _playwright_reachable():
        raise HTTPException(503, "Playwright stack not reachable")

    base = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/probe", json={"url": body.url})
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
async def playwright_screenshot(body: _UrlBody) -> dict:
    """Capture a full-page screenshot through the local Playwright stack."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _playwright_reachable():
        raise HTTPException(503, "Playwright stack not reachable")

    base = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/screenshot", json={"url": body.url})
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/screenshot"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Playwright screenshot failed: {exc}") from exc
