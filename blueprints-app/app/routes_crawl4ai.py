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

class _UrlBody(BaseModel):
    url: str


# Alias kept for back-compat; crawl endpoint also uses _UrlBody
class _CrawlBody(BaseModel):
    url: str
    accept_cookies: bool = True


def _crawl_payload(url: str, *, accept_cookies: bool = True, screenshot: bool = False, pdf: bool = False) -> dict:
    """Build a /crawl payload with optional consent-handling and output types."""
    crawler_config: dict = {"wait_for_images": True}
    if accept_cookies:
        crawler_config["magic"] = True
        crawler_config["simulate_user"] = True
    if screenshot:
        crawler_config["screenshot"] = True
    if pdf:
        crawler_config["pdf"] = True
    return {
        "urls": [url],
        "browser_config": {"headless": True},
        "crawler_config": crawler_config,
    }


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
    payload = _crawl_payload(body.url, accept_cookies=body.accept_cookies)
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


# ── POST /crawl4ai/screenshot ─────────────────────────────────────────────────

@router.post("/screenshot")
async def crawl4ai_screenshot(body: _UrlBody) -> dict:
    """Capture a full-page screenshot via the local Crawl4AI stack.

    Uses the /crawl endpoint with magic+simulate_user so consent banners are
    dismissed and images are fully loaded before the screenshot is taken.
    Returns the base64-encoded PNG and its decoded byte size.
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    import base64 as _b64

    base = _base_url()
    payload = _crawl_payload(body.url, accept_cookies=True, screenshot=True)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/crawl", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        data = r.json()
        results = data.get("results") or [data]
        result = results[0] if results else {}
        b64 = result.get("screenshot") or ""
        size_bytes = len(_b64.b64decode(b64)) if b64 else 0
        return {
            "ok": bool(b64),
            "url": result.get("url", body.url),
            "screenshot_b64": b64,
            "size_bytes": size_bytes,
            "_via": {"endpoint": f"{base}/crawl"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Crawl4AI screenshot failed: {exc}") from exc


# ── POST /crawl4ai/pdf ────────────────────────────────────────────────────────

@router.post("/pdf")
async def crawl4ai_pdf(body: _UrlBody) -> dict:
    """Generate a PDF of the page via the local Crawl4AI stack.

    Uses the /crawl endpoint with magic+simulate_user so consent banners are
    dismissed and images are fully loaded before the PDF is generated.
    Returns the base64-encoded PDF and its decoded byte size.
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    import base64 as _b64

    base = _base_url()
    payload = _crawl_payload(body.url, accept_cookies=True, pdf=True)
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/crawl", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        data = r.json()
        results = data.get("results") or [data]
        result = results[0] if results else {}
        b64 = result.get("pdf") or ""
        size_bytes = len(_b64.b64decode(b64)) if b64 else 0
        return {
            "ok": bool(b64),
            "url": result.get("url", body.url),
            "pdf_b64": b64,
            "size_bytes": size_bytes,
            "_via": {"endpoint": f"{base}/crawl"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Crawl4AI PDF failed: {exc}") from exc


# ── GET /crawl4ai/mcp-schema ──────────────────────────────────────────────────

@router.get("/mcp-schema")
async def crawl4ai_mcp_schema() -> dict:
    """Fetch Crawl4AI's own MCP tool schema directly from its /mcp/schema endpoint.

    This is distinct from LiteLLM's /mcp/ endpoint — it probes the Crawl4AI
    service's built-in MCP server directly to confirm it is up and list its tools.
    """
    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    base = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            r = await client.get(f"{base}/mcp/schema")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        data = r.json()
        # Schema shape may vary by version; normalise to a list of tool names + descriptions
        tools = []
        raw = data if isinstance(data, list) else data.get("tools", data.get("functions", []))
        for item in raw if isinstance(raw, list) else []:
            if isinstance(item, dict):
                tools.append({
                    "name": item.get("name", ""),
                    "description": item.get("description", ""),
                })
        return {
            "ok": True,
            "tool_count": len(tools),
            "tools": tools,
            "_via": {"endpoint": f"{base}/mcp/schema"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Crawl4AI MCP schema fetch failed: {exc}") from exc
