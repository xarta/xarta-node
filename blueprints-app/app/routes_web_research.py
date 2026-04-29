"""Blueprints proxy for the guarded nullclaw01 public web research adapter."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .db import get_conn
from .tts_sanitizer import prepare_tts_markdown_for_llm, sanitize_tts_text

router = APIRouter(prefix="/web-research", tags=["web-research"])

_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_SPEECH_CACHE_ROOT = _NODE_LOCAL_ROOT / "web-research-speech-cache"
_PRIVACY_MODE_DOC = _NODE_LOCAL_ROOT / "docs" / "null-claw-web-research" / "PRIVACY-MODE.md"
_PRIVACY_MODE_DOC_REL = "docs/null-claw-web-research/PRIVACY-MODE.md"
_DEFAULT_ADAPTER_URL = "http://172.31.250.2:18080"
_DEFAULT_TIMEOUT_SECONDS = 180.0
_WEB_RESEARCH_SPEECH_VERSION = 2
_TASK_TERMINAL_STATES = {"succeeded", "partial", "failed", "timed_out", "canceled", "rejected"}
_DEPTH_POLICIES = {
    "quick": {
        "max_runtime_seconds": 90,
        "max_search_queries": 2,
        "max_search_results": 3,
        "max_fetches": 2,
        "max_source_chars": 6000,
    },
    "standard": {
        "max_runtime_seconds": 120,
        "max_search_queries": 3,
        "max_search_results": 5,
        "max_fetches": 3,
        "max_source_chars": 9000,
    },
    "deep": {
        "max_runtime_seconds": 170,
        "max_search_queries": 5,
        "max_search_results": 5,
        "max_fetches": 4,
        "max_source_chars": 12000,
    },
}
_PRIVATE_TARGET_RE = re.compile(
    r"(?i)\b("
    r"localhost|127\.|0\.0\.0\.0|10\.|172\.(?:1[6-9]|2\d|3[01])\.|192\.168\.|"
    r"169\.254\.|::1|fc[0-9a-f]{2}:|fd[0-9a-f]{2}:|file://|ssh://"
    r")"
)
_SECRETISH_RE = re.compile(
    r"(?i)\b(api[_ -]?key|authorization|bearer|cookie|password|passwd|secret|token)\b"
)
_MARKDOWN_HEADING_RE = re.compile(r"^\s{0,3}(?P<marks>#{1,6})\s+(?P<title>.+?)\s*$")
_EXCLUDED_SPEECH_SECTION_TITLES = {
    "query plan",
    "sources",
    "source",
    "references",
    "reference",
    "citations",
    "citation",
    "warnings",
    "warning",
    "firewall notes",
    "firewall note",
    "boundary notes",
    "boundary note",
    "research boundary notes",
    "research boundary note",
    "diagnostics",
    "diagnostic notes",
}

_WEB_RESEARCH_SPEECH_SYSTEM_PROMPT = """
You write narration scripts for public web research results.

Convert the supplied Markdown synthesis into a plain text narration script for TTS.

Rules:
- Preserve the useful answer, caveats, comparisons, dates, names, and practical takeaways.
- Output plain text only. Do not use Markdown headings, bullets, numbered lists, bold markers, link syntax, tables, or source citations.
- Do not narrate source lists, references, URLs, query plans, diagnostics, adapter notes, firewall notes, or boundary notes.
- Do not say source labels such as S one or bracketed citations.
- Use short paragraph breaks for pacing.
- If a section title helps the listener, write it as a plain sentence with a full stop.
- Rewrite visual fragments into natural spoken prose. Avoid reading raw punctuation or Markdown syntax.
- Do not add commentary about being an AI.
- Output only the narration text.
""".strip()


Depth = Literal["quick", "standard", "deep"]


class WebResearchQueryBody(BaseModel):
    query: str = Field(..., min_length=1, max_length=300)
    depth: Depth = "standard"
    private_mode: bool = False


class WebResearchSpeechBody(BaseModel):
    cache_key: str | None = Field(default=None, max_length=128)
    query: str | None = Field(default=None, max_length=300)
    depth: Depth = "standard"
    markdown: str | None = Field(default=None, max_length=60000)
    display: dict[str, Any] | None = None
    force_refresh: bool = False
    private_mode: bool = False


def _adapter_url() -> str:
    return os.environ.get("NULLCLAW01_RESEARCH_URL", _DEFAULT_ADAPTER_URL).strip().rstrip("/")


def _timeout_seconds() -> float:
    raw = os.environ.get("NULLCLAW01_RESEARCH_TIMEOUT", str(_DEFAULT_TIMEOUT_SECONDS))
    try:
        return max(5.0, min(180.0, float(raw)))
    except ValueError:
        return _DEFAULT_TIMEOUT_SECONDS


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _text(value: Any, limit: int | None = None) -> str:
    clean = " ".join(str(value or "").replace("\r", "\n").split())
    if limit is not None and len(clean) > limit:
        return clean[: max(0, limit - 1)].rstrip() + "..."
    return clean


def _list_text(value: Any, *, limit: int = 12, item_limit: int = 600) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    for item in value:
        clean = _text(item, item_limit)
        if clean:
            out.append(clean)
        if len(out) >= limit:
            break
    return out


def _validate_public_query(query: str) -> None:
    if _PRIVATE_TARGET_RE.search(query):
        raise HTTPException(400, "Query cannot include private, local, or credentialed targets")
    if _SECRETISH_RE.search(query):
        raise HTTPException(400, "Query cannot include credentials or secret-like material")


def _decode_response(response: httpx.Response, operation: str) -> dict[str, Any]:
    if response.status_code >= 400:
        detail = response.text[:600] if response.text else f"HTTP {response.status_code}"
        raise HTTPException(502, f"nullclaw01 research adapter {operation} failed: {detail}")
    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(502, "nullclaw01 research adapter returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(502, "nullclaw01 research adapter returned a non-object response")
    return data


async def _adapter_get(path: str, *, timeout: float = 8.0) -> tuple[bool, dict[str, Any] | None, str | None]:
    base_url = _adapter_url()
    if not base_url:
        return False, None, "NULLCLAW01_RESEARCH_URL is not configured"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            response = await client.get(f"{base_url}{path}")
    except httpx.TimeoutException:
        return False, None, "timeout"
    except httpx.RequestError as exc:
        return False, None, str(exc)
    try:
        data = _decode_response(response, path)
    except HTTPException as exc:
        return False, None, str(exc.detail)
    return True, data, None


def _source_items(raw_sources: Any, raw_citations: Any = None) -> list[dict[str, Any]]:
    if not isinstance(raw_sources, list):
        return []
    claims_by_source: dict[str, list[str]] = {}
    if isinstance(raw_citations, list):
        for citation in raw_citations:
            if not isinstance(citation, dict):
                continue
            source_id = _text(citation.get("source_id"), 80)
            claim = _text(citation.get("claim"), 360)
            if source_id and claim:
                claims_by_source.setdefault(source_id, []).append(claim)
    items: list[dict[str, Any]] = []
    for index, source in enumerate(raw_sources[:5], start=1):
        if not isinstance(source, dict):
            continue
        source_id = _text(source.get("source_id"), 80)
        url = _text(source.get("url"), 2000)
        title = _text(source.get("title"), 240) or url or f"Source {index}"
        snippet = _text(source.get("selection_reason") or source.get("snippet") or source.get("content"), 700)
        claims = claims_by_source.get(source_id) or _list_text(source.get("claims"), limit=6, item_limit=360)
        items.append(
            {
                "label": _text(source.get("citation_label"), 20) or f"[S{index}]",
                "title": title,
                "url": url,
                "domain": _text(source.get("domain"), 180),
                "retrieval_method": _text(source.get("retrieval_method"), 80),
                "source_type": _text(source.get("source_type"), 80),
                "snippet": snippet,
                "claims": claims,
            }
        )
    return items


def _fallback_markdown(query: str, sources: list[dict[str, Any]]) -> str:
    lines = [f"# Research: {query}", ""]
    if not sources:
        lines.append("No sources were returned.")
        return "\n".join(lines)
    lines.append("## Sources")
    for item in sources:
        title = item.get("title") or item.get("url") or "Untitled source"
        snippet = item.get("snippet") or "No snippet returned."
        lines.append(f"- {item['label']} {title}: {snippet}")
    return "\n".join(lines)


def _summary_markdown(data: dict[str, Any], query: str, sources: list[dict[str, Any]]) -> str:
    markdown = str(data.get("markdown") or "").strip()
    result = data.get("result")
    if not markdown and isinstance(result, dict):
        markdown = str(result.get("summary_markdown") or "").strip()
    if not markdown:
        markdown = _fallback_markdown(query, sources)
    if len(markdown) > 24000:
        markdown = markdown[:24000].rsplit("\n", 1)[0].strip() + "\n\n[Research output truncated.]"
    return markdown


def _short_response(markdown: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    text = _speech_safe_text(re.sub(r"(?m)^\s{0,3}#{1,6}\s*", "", markdown))
    text = re.sub(r"\[[Ss]\d+\]", "", text)
    text = _text(text, 420)
    if not text and sources:
        text = f"Found {len(sources)} source{'s' if len(sources) != 1 else ''}."
    return {
        "text": text,
        "tts_ready": bool(text),
        "voice_safe": True,
        "format": "plain_text",
    }


def _speech_safe_text(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\[([^\]\n]{1,180})\]\((?:https?://|mailto:)[^)]+\)", r"\1", text)
    text = re.sub(r"https?://\S+", "source link", text)
    text = re.sub(r"mailto:\S+", "source link", text)
    text = re.sub(r"\[\d+\]", "", text)
    text = re.sub(r"\[[Ss]\d+\]", "", text)
    text = re.sub(r"\s+([.,;!?])", r"\1", text)
    return text


def _strip_think_blocks(text: str) -> str:
    return re.sub(r"\s*<think>.*?</think>\s*", "", str(text or ""), flags=re.DOTALL).strip()


def _clean_section_title(title: str) -> str:
    clean = re.sub(r"[*_`[\]()]+", "", str(title or "")).strip().lower()
    clean = re.sub(r"[^a-z0-9]+", " ", clean)
    return " ".join(clean.split())


def _strip_non_speech_sections(markdown: str) -> str:
    out: list[str] = []
    skip = False
    for raw_line in str(markdown or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        heading = _MARKDOWN_HEADING_RE.match(raw_line)
        if heading:
            title = _clean_section_title(heading.group("title"))
            skip = title in _EXCLUDED_SPEECH_SECTION_TITLES
            if skip:
                continue
        if not skip:
            out.append(raw_line)
    return "\n".join(out).strip()


def _clamp_speech_source_markdown(markdown: str, limit: int = 18000) -> str:
    text = str(markdown or "").strip()
    if len(text) <= limit:
        return text
    clipped = text[:limit].rsplit("\n## ", 1)[0].strip()
    if len(clipped) < limit * 0.5:
        clipped = text[:limit].rsplit("\n", 1)[0].strip()
    return clipped + "\n\n[Research synthesis continues beyond this prompt window.]"


def _clean_web_research_speech_markdown(text: str) -> str:
    cleaned = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"\s+([.,;!?])", r"\1", sanitize_tts_text(cleaned).text)


def _speech_markdown(*, query: str, summary_markdown: str) -> str:
    lines = [f"Web research for: {_speech_safe_text(query)}", ""]
    clean_summary = _speech_safe_text(_strip_non_speech_sections(summary_markdown)).strip()
    if clean_summary:
        lines.append(clean_summary)
    prepared = prepare_tts_markdown_for_llm("\n".join(lines))
    return re.sub(r"\s+([.,;!?])", r"\1", sanitize_tts_text(prepared).text)


async def _complete_web_research_speech_local(messages: list[dict[str, str]]) -> str:
    base_url = (os.environ.get("LITELLM_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("LITELLM_API_KEY") or "").strip()
    model = (
        os.environ.get("WEB_RESEARCH_SPEECH_LLM_MODEL")
        or os.environ.get("DOC_SPEECH_LLM_MODEL")
        or ""
    ).strip()
    if not base_url:
        raise HTTPException(503, "LITELLM_BASE_URL is not configured for web research narration")
    if not api_key:
        raise HTTPException(503, "LITELLM_API_KEY is not configured for web research narration")
    if not model:
        raise HTTPException(503, "WEB_RESEARCH_SPEECH_LLM_MODEL or DOC_SPEECH_LLM_MODEL is not configured")

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": 2600,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    timeout = float(
        os.environ.get(
            "WEB_RESEARCH_SPEECH_LLM_TIMEOUT",
            os.environ.get("DOC_SPEECH_LLM_TIMEOUT", "75"),
        )
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(f"{base_url}/v1/chat/completions", headers=headers, json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "Local LLM web research narration generation timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"Local LLM narration endpoint unavailable: {exc}") from exc
    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"Local LLM web research narration generation failed: {detail}")
    try:
        data = resp.json()
        answer = data["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(502, "Local LLM returned an invalid web research narration response") from exc
    return _strip_think_blocks(str(answer or ""))


async def _generate_web_research_speech_markdown(query: str, summary_markdown: str) -> str:
    speech_source = _clamp_speech_source_markdown(
        prepare_tts_markdown_for_llm(_strip_non_speech_sections(summary_markdown))
    )
    user_prompt = (
        "/no-think\n"
        f"Web research query: {query}\n\n"
        "Rewrite this web research synthesis as a plain text narrated version for TTS playback:\n\n"
        f"{speech_source}"
    )
    answer = await _complete_web_research_speech_local(
        [
            {"role": "system", "content": _WEB_RESEARCH_SPEECH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
    )
    speech = _clean_web_research_speech_markdown(answer)
    if not speech:
        raise HTTPException(502, "Local LLM returned an empty web research narration")
    return speech


def _cache_key(
    *,
    query: str,
    depth: str,
    markdown: str,
) -> str:
    markdown_digest = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
    payload = {
        "query": query.strip(),
        "depth": depth,
        "markdown_digest": markdown_digest,
        "speech_version": _WEB_RESEARCH_SPEECH_VERSION,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:32]


def _cache_path(cache_key: str) -> Path:
    if not re.fullmatch(r"[a-f0-9]{32,64}", cache_key or ""):
        raise HTTPException(400, "Invalid web research speech cache key")
    return _SPEECH_CACHE_ROOT / f"{cache_key}.json"


def _normalize_node_local_ownership(target: Path) -> None:
    try:
        owner = _NODE_LOCAL_ROOT.stat()
    except OSError:
        return
    current = target
    while True:
        try:
            if current.exists():
                os.chown(current, owner.st_uid, owner.st_gid)
        except OSError:
            return
        if current == _NODE_LOCAL_ROOT or current.parent == current:
            break
        current = current.parent


def _read_speech_cache(cache_key: str) -> dict[str, Any] | None:
    path = _cache_path(cache_key)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise HTTPException(500, f"Could not read web research speech cache: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("markdown"), str):
        return None
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    if meta.get("speech_version") != _WEB_RESEARCH_SPEECH_VERSION:
        return None
    return data


def _write_speech_cache(cache_key: str, markdown: str, meta: dict[str, Any]) -> Path:
    path = _cache_path(cache_key)
    payload = {
        "ok": True,
        "cache_key": cache_key,
        "generated_at": _now_iso(),
        "markdown": sanitize_tts_text(markdown).text,
        "meta": {**meta, "speech_version": _WEB_RESEARCH_SPEECH_VERSION},
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        _normalize_node_local_ownership(path.parent)
        _normalize_node_local_ownership(path)
    except OSError as exc:
        raise HTTPException(500, f"Could not write web research speech cache: {exc}") from exc
    return path


def _display_envelope(
    *,
    body: WebResearchQueryBody,
    data: dict[str, Any],
) -> dict[str, Any]:
    sources = _source_items(data.get("sources"), data.get("citations"))
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    warnings = _list_text(data.get("warnings"), limit=12, item_limit=600)
    warnings.extend(_list_text(result.get("limitations"), limit=6, item_limit=600))
    error = data.get("error")
    if isinstance(error, dict) and error.get("message"):
        warnings.append(_text(error.get("message"), 600))
    adapter_notes = []
    raw_notes = data.get("firewall_notes")
    if isinstance(raw_notes, list):
        for note in raw_notes:
            if isinstance(note, dict):
                clean = _text(note.get("message") or note.get("event_type"), 600)
            else:
                clean = _text(note, 600)
            if clean:
                adapter_notes.append(clean)
            if len(adapter_notes) >= 8:
                break
    firewall_notes = adapter_notes or ["Guarded public-web adapter enforced URL and content restrictions."]
    markdown = _summary_markdown(data, body.query, sources)
    audio = _speech_markdown(
        query=body.query,
        summary_markdown=markdown,
    )
    return {
        "summary_markdown": markdown,
        "audio_markdown": audio,
        "short_response": _short_response(markdown, sources),
        "source_items": sources,
        "firewall_notes": firewall_notes,
        "warnings": warnings,
    }


@router.get("/health", response_model=dict)
async def web_research_health() -> dict[str, Any]:
    health_ok, health, health_error = await _adapter_get("/health", timeout=4.0)
    tools_ok, tools, tools_error = await _adapter_get("/tools", timeout=4.0)
    status = "available" if health_ok and tools_ok else "unavailable"
    return {
        "ok": status == "available",
        "status": status,
        "service": "nullclaw01-autonomous-web-research",
        "adapter": {
            "health": health if health_ok else None,
            "tools": {
                "endpoints": tools.get("endpoints", {}) if isinstance(tools, dict) else {},
            },
            "errors": [err for err in (health_error, tools_error) if err],
        },
        "mode": "autonomous",
        "supported_depths": sorted(_DEPTH_POLICIES),
        "maximums": {
            "query_chars": 300,
            "timeout_seconds": _timeout_seconds(),
        },
    }


@router.get("/egress-ip", response_model=dict)
async def web_research_egress_ip() -> dict[str, Any]:
    ok, data, error = await _adapter_get("/egress-ip", timeout=24.0)
    if not ok or not isinstance(data, dict):
        return {
            "ok": False,
            "status": "unavailable",
            "service": "nullclaw01-autonomous-web-research",
            "error": error or "nullclaw01 research adapter did not return egress IP data",
        }
    return {
        "ok": bool(data.get("ok")),
        "status": "available" if data.get("ok") else "unavailable",
        "service": "nullclaw01-autonomous-web-research",
        "ip": _text(data.get("ip"), 80),
        "checked_at": data.get("checked_at"),
        "source": _text(data.get("source"), 120),
        "tool_path": _text(data.get("tool_path"), 220),
        "reverse_dns": _text(data.get("reverse_dns"), 220) if data.get("reverse_dns") else None,
        "server_domain": _text(data.get("server_domain"), 220) if data.get("server_domain") else None,
        "server_dns_lookup": data.get("server_dns_lookup") if isinstance(data.get("server_dns_lookup"), list) else None,
    }


@router.get("/privacy-doc", response_model=dict)
async def web_research_privacy_doc() -> dict[str, Any]:
    try:
        markdown = _PRIVACY_MODE_DOC.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(404, "Web research privacy-mode document is not available") from exc
    with get_conn() as conn:
        row = conn.execute(
            "SELECT doc_id FROM docs WHERE lower(path)=lower(?) LIMIT 1",
            (_PRIVACY_MODE_DOC_REL,),
        ).fetchone()
    return {
        "ok": True,
        "title": "Web Research Privacy Mode",
        "source": _PRIVACY_MODE_DOC_REL,
        "doc_id": row["doc_id"] if row else None,
        "markdown": markdown,
    }


def _task_payload(query: str, depth: str, private_mode: bool = False) -> dict[str, Any]:
    policy = dict(_DEPTH_POLICIES.get(depth, _DEPTH_POLICIES["standard"]))
    policy.update(
        {
            "require_citations": True,
            "require_vlan99_diagnostics": True,
            "allowed_schemes": ["https", "http"],
        }
    )
    return {
        "task_type": "web_research",
        "objective": query,
        "research_profile": "public_web",
        "private_mode": bool(private_mode),
        "inputs": {
            "query": query,
            "urls": [],
            "seed_terms": [],
        },
        "policy": policy,
        "notification": {
            "webhook_url": "http://127.0.0.1:9/blueprints-web-research/noop",
            "required": True,
            "timeout_seconds": 1,
        },
    }


async def _submit_task(base_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(12.0)) as client:
            response = await client.post(f"{base_url}/tasks", json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "nullclaw01 web research task submission timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"nullclaw01 research adapter unavailable: {exc}") from exc
    return _decode_response(response, "task submission")


async def _purge_task(base_url: str, task_id: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(8.0)) as client:
            response = await client.post(f"{base_url}/tasks/{task_id}/purge", json={})
    except httpx.RequestError as exc:
        return {"ok": False, "error": str(exc)}
    try:
        return _decode_response(response, "task purge")
    except HTTPException as exc:
        return {"ok": False, "error": str(exc.detail)}


async def _poll_task(base_url: str, task_id: str) -> dict[str, Any]:
    started = asyncio.get_running_loop().time()
    timeout = _timeout_seconds()
    last: dict[str, Any] | None = None
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        while asyncio.get_running_loop().time() - started < timeout:
            try:
                response = await client.get(f"{base_url}/tasks/{task_id}")
                data = _decode_response(response, "task status")
            except httpx.RequestError as exc:
                raise HTTPException(503, f"nullclaw01 research adapter unavailable while polling: {exc}") from exc
            last = data
            if str(data.get("state") or "") in _TASK_TERMINAL_STATES:
                return data
            await asyncio.sleep(1.2)
    state = str(last.get("state") or "unknown") if last else "unknown"
    raise HTTPException(504, f"nullclaw01 web research did not finish before the Blueprints timeout (state: {state})")


@router.post("/query", response_model=dict)
async def web_research_query(body: WebResearchQueryBody) -> dict[str, Any]:
    query = body.query.strip()
    _validate_public_query(query)

    base_url = _adapter_url()
    if not base_url:
        raise HTTPException(503, "NULLCLAW01_RESEARCH_URL is not configured")
    submitted = await _submit_task(base_url, _task_payload(query, body.depth, body.private_mode))
    task_id = str(submitted.get("id") or "").strip()
    if not task_id:
        raise HTTPException(502, "nullclaw01 research adapter did not return a task id")
    data = await _poll_task(base_url, task_id)
    display = _display_envelope(body=body, data=data)
    cache_key = None if body.private_mode else _cache_key(
        query=query,
        depth=body.depth,
        markdown=display["summary_markdown"],
    )
    purge = await _purge_task(base_url, task_id) if body.private_mode else None
    status = str(data.get("state") or "unknown")
    return {
        "ok": status in {"succeeded", "partial"},
        "query": query,
        "depth": body.depth,
        "private_mode": body.private_mode,
        "status": status,
        "display": display,
        "raw": {
            "adapter": {
                "task_id": task_id,
                "state": status,
                "progress": data.get("progress") if isinstance(data.get("progress"), dict) else {},
                "diagnostics_summary": (
                    data.get("diagnostics", {}).get("summary")
                    if isinstance(data.get("diagnostics"), dict)
                    else {}
                ),
                "source_count": len(display["source_items"]),
                "warning_count": len(display["warnings"]),
                "purged": purge if body.private_mode else None,
            }
        },
        "cache_key": cache_key,
    }


@router.post("/speech", response_model=dict)
async def web_research_speech(body: WebResearchSpeechBody) -> dict[str, Any]:
    cache_key = (body.cache_key or "").strip()
    if cache_key and not body.force_refresh and not body.private_mode:
        cached = _read_speech_cache(cache_key)
        if cached:
            return {
                "ok": True,
                "cache": "hit",
                "cache_key": cache_key,
                "generated_at": cached.get("generated_at"),
                "markdown": cached["markdown"],
            }

    display = body.display if isinstance(body.display, dict) else {}
    markdown = str(body.markdown or display.get("summary_markdown") or "").strip()
    if not markdown:
        if cache_key:
            raise HTTPException(404, "No cached narration found for this web research result")
        raise HTTPException(400, "Provide cache_key or markdown for web research speech")

    query = (body.query or display.get("query") or "web research").strip()
    _validate_public_query(query)
    audio = await _generate_web_research_speech_markdown(query=query, summary_markdown=markdown)
    if body.private_mode:
        return {
            "ok": True,
            "cache": "private",
            "cache_key": None,
            "markdown": audio,
        }
    if not cache_key:
        cache_key = _cache_key(
            query=query,
            depth=body.depth,
            markdown=markdown,
        )
    if body.force_refresh:
        path = _cache_path(cache_key)
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise HTTPException(500, f"Could not invalidate web research speech cache: {exc}") from exc
    _write_speech_cache(
        cache_key,
        audio,
        {
            "query": query,
            "depth": body.depth,
            "source": "speech",
        },
    )
    return {
        "ok": True,
        "cache": "regenerated" if body.force_refresh else "miss",
        "cache_key": cache_key,
        "markdown": audio,
    }
