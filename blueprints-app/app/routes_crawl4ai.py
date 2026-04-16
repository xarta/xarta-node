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
    crawler_config: dict = {"headless": True}
    if body.accept_cookies:
        crawler_config["magic"] = True
        crawler_config["simulate_user"] = True
        crawler_config["js_code"] = (
            "document.querySelectorAll('button,a').forEach(el => {"
            " if (/accept|agree|consent|ok|got it|allow all/i.test(el.textContent)) el.click(); "
            "});"
        )
    payload = {
        "urls": [body.url],
        "browser_config": {"headless": True},
        "crawler_config": crawler_config,
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


# ── POST /crawl4ai/screenshot ─────────────────────────────────────────────────

@router.post("/screenshot")
async def crawl4ai_screenshot(body: _UrlBody) -> dict:
    """Capture a full-page screenshot via the local Crawl4AI stack.

    Returns the base64-encoded PNG and its decoded byte size.
    The URL is validated to be an http/https address before forwarding.
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    base = _base_url()
    payload = {"url": body.url, "screenshot_wait_for": 3, "wait_for_images": True}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/screenshot", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        import base64 as _b64

        data = r.json()
        b64 = data.get("screenshot") or data.get("screenshot_data") or ""
        size_bytes = len(_b64.b64decode(b64)) if b64 else 0
        return {
            "ok": bool(b64),
            "url": body.url,
            "screenshot_b64": b64,
            "size_bytes": size_bytes,
            "_via": {"endpoint": f"{base}/screenshot"},
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Crawl4AI screenshot failed: {exc}") from exc


# ── POST /crawl4ai/pdf ────────────────────────────────────────────────────────

@router.post("/pdf")
async def crawl4ai_pdf(body: _UrlBody) -> dict:
    """Generate a PDF of the page via the local Crawl4AI stack.

    Returns the base64-encoded PDF and its decoded byte size.
    The actual PDF data is NOT forwarded to the browser — only the size is returned
    so the GUI can confirm the operation succeeded without transferring large blobs.
    The URL is validated to be an http/https address before forwarding.
    """
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    if not await _crawl4ai_reachable():
        raise HTTPException(503, "Crawl4AI stack not reachable")

    base = _base_url()
    payload = {"url": body.url}
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            r = await client.post(f"{base}/pdf", json=payload)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        import base64 as _b64

        data = r.json()
        b64 = data.get("pdf") or data.get("pdf_data") or data.get("content") or ""
        size_bytes = len(_b64.b64decode(b64)) if b64 else 0
        return {
            "ok": bool(b64),
            "url": body.url,
            "pdf_b64": b64,
            "size_bytes": size_bytes,
            "_via": {"endpoint": f"{base}/pdf"},
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
