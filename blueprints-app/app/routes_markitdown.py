"""routes_markitdown.py — Node-local MarkItDown proxy endpoints.

Proxy and status endpoints for the per-node MarkItDown stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from .db import get_conn, get_setting

router = APIRouter(prefix="/markitdown", tags=["markitdown"])

_DEFAULT_BASE_URL = "http://localhost:19000"
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 180.0


class _ConvertBody(BaseModel):
    url: str
    max_chars: int | None = None


def _base_url() -> str:
    with get_conn() as conn:
        return (
            get_setting(conn, "markitdown.base_url", _DEFAULT_BASE_URL)
            or _DEFAULT_BASE_URL
        ).rstrip("/")


async def _markitdown_reachable() -> bool:
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
            return r.status_code < 500
    except Exception:
        return False


@router.get("/health")
async def markitdown_health() -> dict:
    """Check if the MarkItDown stack is reachable and return its health status."""
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
async def markitdown_tools() -> dict:
    """Fetch the MarkItDown tool list from the local stack."""
    if not await _markitdown_reachable():
        raise HTTPException(503, "MarkItDown stack not reachable")

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
        raise HTTPException(502, f"MarkItDown tools fetch failed: {exc}") from exc


@router.post("/convert")
async def markitdown_convert(body: _ConvertBody) -> dict:
    """Convert a remote document or page URL to Markdown via the MarkItDown stack."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _markitdown_reachable():
        raise HTTPException(503, "MarkItDown stack not reachable")

    base = _base_url()
    payload = body.model_dump(exclude_none=True)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/convert", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/convert"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"MarkItDown convert failed: {exc}") from exc


@router.post("/convert-upload")
async def markitdown_convert_upload(
    file: UploadFile = File(...),
    max_chars: int | None = Form(None),
) -> dict:
    """Convert an uploaded local document to Markdown via the MarkItDown stack."""
    if not await _markitdown_reachable():
        raise HTTPException(503, "MarkItDown stack not reachable")

    blob = await file.read()
    if not blob:
        raise HTTPException(400, "Uploaded file is empty")

    base = _base_url()
    form_data: dict[str, str] = {}
    if max_chars is not None:
        form_data["max_chars"] = str(max_chars)

    files = {
        "file": (
            file.filename or "upload.bin",
            blob,
            file.content_type or "application/octet-stream",
        )
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/convert-upload", data=form_data, files=files)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])
        data = r.json()
        data["_via"] = {"endpoint": f"{base}/convert-upload"}
        return data
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"MarkItDown upload convert failed: {exc}") from exc
