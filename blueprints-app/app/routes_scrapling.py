"""routes_scrapling.py — Node-local Scrapling MCP proxy endpoints.

Proxy and status endpoints for the per-node Scrapling stack.
Config values are read from DB settings (not hardcoded here).
No DB writes; no enqueue_for_all_peers — these are local-node-only operations.
"""

import json as _json

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .db import get_conn, get_setting

router = APIRouter(prefix="/scrapling", tags=["scrapling"])

_DEFAULT_BASE_URL = "http://localhost:18000"
_CONNECT_TIMEOUT = 8.0
_READ_TIMEOUT = 45.0
_MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}


class _ScraplingFetchBody(BaseModel):
    url: str
    tool_name: str = "get"
    extraction_type: str = "markdown"
    main_content_only: bool = True


def _base_url() -> str:
    with get_conn() as conn:
        return (
            get_setting(conn, "scrapling.base_url", _DEFAULT_BASE_URL)
            or _DEFAULT_BASE_URL
        ).rstrip("/")


def _mcp_url() -> str:
    return f"{_base_url()}/mcp"


def _extract_sse_json(raw: str) -> dict:
    for line in raw.splitlines():
        if line.startswith("data:"):
            return _json.loads(line[5:].strip())
    raise ValueError("No data frame in MCP response")


async def _mcp_initialize(client: httpx.AsyncClient) -> tuple[str | None, dict]:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "blueprints-scrapling", "version": "1.0"},
        },
    }
    r = await client.post(_mcp_url(), headers=_MCP_HEADERS, json=payload)
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:500])
    result = _extract_sse_json(r.text).get("result", {})
    session_id = r.headers.get("mcp-session-id") or r.headers.get("Mcp-Session-Id")
    return session_id, result


async def _mcp_list_tools(client: httpx.AsyncClient) -> list[dict]:
    session_id, _ = await _mcp_initialize(client)
    headers = dict(_MCP_HEADERS)
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    r = await client.post(
        _mcp_url(),
        headers=headers,
        json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
    )
    if r.status_code >= 400:
        raise HTTPException(r.status_code, r.text[:500])
    return _extract_sse_json(r.text).get("result", {}).get("tools", [])


@router.get("/health")
async def scrapling_health() -> dict:
    """Check if the Scrapling stack is reachable and return MCP server info."""
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            session_id, init = await _mcp_initialize(client)
        server = init.get("serverInfo", {})
        return {
            "present": True,
            "reachable": True,
            "url": url,
            "mcp_url": _mcp_url(),
            "session_id_present": bool(session_id),
            "server_name": server.get("name", "Scrapling"),
            "version": server.get("version"),
        }
    except Exception as exc:
        return {
            "present": False,
            "reachable": False,
            "url": url,
            "mcp_url": _mcp_url(),
            "error": str(exc),
        }


@router.get("/mcp-tools")
async def scrapling_mcp_tools() -> dict:
    """Return the direct MCP tool list from the local Scrapling stack."""
    url = _base_url()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            tools = await _mcp_list_tools(client)
        return {
            "present": True,
            "reachable": True,
            "url": url,
            "tools": tools,
            "count": len(tools),
        }
    except Exception as exc:
        raise HTTPException(502, f"Scrapling MCP tools fetch failed: {exc}") from exc


@router.post("/fetch")
async def scrapling_fetch(body: _ScraplingFetchBody) -> dict:
    """Run a local Scrapling MCP fetch tool and return extracted markdown/text."""
    if not body.url.startswith(("http://", "https://")):
        raise HTTPException(400, "URL must start with http:// or https://")

    allowed_tools = {"get", "fetch", "stealthy_fetch"}
    if body.tool_name not in allowed_tools:
        raise HTTPException(400, f"tool_name must be one of {sorted(allowed_tools)}")

    args = {
        "url": body.url,
        "extraction_type": body.extraction_type,
        "main_content_only": body.main_content_only,
    }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(_READ_TIMEOUT)) as client:
            session_id, _ = await _mcp_initialize(client)
            headers = dict(_MCP_HEADERS)
            if session_id:
                headers["Mcp-Session-Id"] = session_id
            r = await client.post(
                _mcp_url(),
                headers=headers,
                json={
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": body.tool_name, "arguments": args},
                },
            )
        if r.status_code >= 400:
            raise HTTPException(r.status_code, r.text[:500])

        result = _extract_sse_json(r.text).get("result", {})
        structured = result.get("structuredContent") or {}
        content_items = result.get("content", [])
        text = "\n".join(
            item.get("text", "") for item in content_items if item.get("type") == "text"
        )
        content_list = structured.get("content") or []
        markdown = "\n\n".join(part for part in content_list if isinstance(part, str)).strip()
        if not markdown:
            markdown = text.strip()

        return {
            "ok": not result.get("isError", False),
            "tool_name": body.tool_name,
            "url": structured.get("url") or body.url,
            "status": structured.get("status"),
            "markdown": markdown,
            "markdown_len": len(markdown),
            "raw_text": text,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Scrapling fetch failed: {exc}") from exc
