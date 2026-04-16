"""routes_liteparse.py — Node-local LiteParse proxy endpoints.

Proxy and status endpoints for the per-node LiteParse stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .db import get_conn, get_setting

router = APIRouter(prefix="/liteparse", tags=["liteparse"])

_DEFAULT_BASE_URL = "http://localhost:18444"
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 120.0


class _ParseBody(BaseModel):
    url: str | None = None
    file_path: str | None = None
    output_format: str = "text"
    max_pages: int | None = None  # None → service default (env LITEPARSE_MAX_DOWNLOAD_MB)
    no_ocr: bool = True
    max_chars: int | None = None  # None → service default (env LITEPARSE_MAX_OUTPUT_CHARS)


def _base_url() -> str:
    with get_conn() as conn:
        return (
            get_setting(conn, "liteparse.base_url", _DEFAULT_BASE_URL)
            or _DEFAULT_BASE_URL
        ).rstrip("/")


async def _liteparse_reachable() -> bool:
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
            return r.status_code < 500
    except Exception:
        return False


@router.get("/health")
async def liteparse_health() -> dict:
    """Check if the LiteParse stack is reachable and return its health status."""
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
        if r.status_code < 500:
            try:
                body = r.json()
            except Exception:
                body = {}
            return {"present": True, "reachable": True, "url": url, **body}
        return {
            "present": True,
            "reachable": False,
            "url": url,
            "error": f"HTTP {r.status_code}",
        }
    except Exception as exc:
        return {"present": False, "reachable": False, "url": url, "error": str(exc)}


@router.get("/tools")
async def liteparse_tools() -> dict:
    """Fetch the exposed LiteParse tool list from the local stack."""
    if not await _liteparse_reachable():
        raise HTTPException(503, "LiteParse stack not reachable")

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
        raise HTTPException(502, f"LiteParse tools fetch failed: {exc}") from exc


@router.post("/parse")
async def liteparse_parse(body: _ParseBody) -> dict:
    """Parse a remote PDF/document URL or allowed local file via the LiteParse stack."""
    if bool(body.url) == bool(body.file_path):
        raise HTTPException(400, "Provide exactly one of url or file_path")
    if body.url and not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _liteparse_reachable():
        raise HTTPException(503, "LiteParse stack not reachable")

    base = _base_url()
    payload = body.model_dump(exclude_none=True)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/parse", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/parse"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"LiteParse parse failed: {exc}") from exc
