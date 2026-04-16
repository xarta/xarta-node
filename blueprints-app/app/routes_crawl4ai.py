"""routes_crawl4ai.py — Node-local Crawl4AI proxy endpoints.

Proxy and status endpoints for the per-node Crawl4AI stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .db import get_conn, get_setting

router = APIRouter(prefix="/crawl4ai", tags=["crawl4ai"])

_DEFAULT_BASE_URL = "http://localhost:11235"
_CONNECT_TIMEOUT  = 5.0
_READ_TIMEOUT     = 90.0   # headless browser crawls can take a while


def _base_url() -> str:
    with get_conn() as conn:
        return (
            get_setting(conn, "crawl4ai.base_url", _DEFAULT_BASE_URL)
            or _DEFAULT_BASE_URL
        ).rstrip("/")


async def _crawl4ai_reachable() -> bool:
    """Quick liveness check against Crawl4AI /health."""
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=_CONNECT_TIMEOUT) as client:
            r = await client.get(f"{url}/health")
            return r.status_code < 500
    except Exception:
        return False


# ── GET /crawl4ai/health ──────────────────────────────────────────────────────

@router.get("/health")
async def crawl4ai_health() -> dict:
    """Check if the Crawl4AI stack is reachable and return its health status."""
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


# ── POST /crawl4ai/crawl ──────────────────────────────────────────────────────

class _CrawlBody(BaseModel):
    url: str


@router.post("/crawl")
async def crawl4ai_crawl(body: _CrawlBody) -> dict:
    """POST a URL crawl request to the local Crawl4AI stack and return markdown.

    Proxies to the Crawl4AI REST /crawl endpoint.  Headless mode is always on.
    The URL is validated to be an http/https address before forwarding.
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    base = _base_url()
    payload = {
        "urls": [body.url],
        "crawler_params": {"headless": True},
    }
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/crawl", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        data = r.json()
        results = data.get("results") or [data]
        result = results[0] if results else {}
        md = result.get("markdown") or ""
        if isinstance(md, dict):
            md = md.get("raw_markdown") or md.get("fit_markdown") or ""

        return {
            "ok": result.get("success", True),
            "url": result.get("url", body.url),
            "markdown": str(md),
            "markdown_len": len(str(md)),
            "_via": {"endpoint": f"{base}/crawl"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Crawl4AI crawl failed: {exc}") from exc
