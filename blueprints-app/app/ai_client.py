"""ai_client.py — Provider-agnostic AI client for Blueprints fleet nodes.

Reads provider config from the local Blueprints DB (ai_providers +
ai_project_assignments tables) and calls LiteLLM-compatible endpoints for
embeddings, reranking, and chat completions.

Usage
-----
    from .ai_client import embed, rerank, complete

    vectors  = await embed("browser-links", ["hello world", "another doc"])
    ranked   = await rerank("browser-links", "my query", ["doc1", "doc2"])
    answer   = await complete("browser-links", [{"role":"user","content":"hi"}])

The active provider for each call is resolved from ai_project_assignments for
the given project_name + role, then fetched from ai_providers. The DB lookup
is non-cached — cheap and avoids stale config between fleet syncs.

Think-tag handling (LLM calls)
-------------------------------
Some LLM backends emit <think>…</think> reasoning blocks before their answer.
Whether the active provider does this depends on the underlying model, not the
generic alias — do not assume PRIMARY-LOCAL always (or never) thinks.

Gate think-related behaviour on the provider record's options JSON:
  {"no_think_supported": true}   → model honours /no-think prefix
  (absence of that key)          → assume raw output; strip/split client-side

Three call-time options:

    complete(..., strip_think=True)    → <think> blocks stripped if present
    complete(..., return_parts=True)   → {"thinking": "…", "answer": "…"}
    complete(..., no_think=True)       → prepends /no-think to user message
                                         (only sent if no_think_supported=true
                                          in the provider options record)
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from .db import get_conn

# ── Internal helpers ──────────────────────────────────────────────────────────


def _strip_think(text: str) -> str:
    """Remove <think>…</think> blocks (and surrounding whitespace)."""
    return re.sub(r"\s*<think>.*?</think>\s*", "", text, flags=re.DOTALL).strip()


def _split_think(text: str) -> dict[str, str]:
    """Return {"thinking": "…", "answer": "…"}. Both empty-string-safe."""
    m = re.search(r"<think>(.*?)</think>\s*", text, flags=re.DOTALL)
    if m:
        thinking = m.group(1).strip()
        answer = text[m.end():].strip()
        return {"thinking": thinking, "answer": answer}
    return {"thinking": "", "answer": text.strip()}


# ── Provider lookup ───────────────────────────────────────────────────────────


def _get_provider(project_name: str, role: str) -> dict[str, Any]:
    """
    Return the highest-priority enabled provider for (project_name, role).
    Raises RuntimeError if none found.
    """
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT p.*
            FROM   ai_providers p
            JOIN   ai_project_assignments a ON a.provider_id = p.provider_id
            WHERE  a.project_name = ?
            AND    a.role         = ?
            AND    a.enabled      = 1
            AND    p.enabled      = 1
            ORDER  BY a.priority DESC
            LIMIT  1
            """,
            (project_name, role),
        ).fetchone()
    if not row:
        raise RuntimeError(
            f"No enabled AI provider for project={project_name!r} role={role!r}. "
            "Seed the ai_providers and ai_project_assignments tables first."
        )
    provider = dict(row)
    # Parse options JSON once here
    raw_opts = provider.get("options") or "{}"
    try:
        provider["_opts"] = json.loads(raw_opts)
    except (json.JSONDecodeError, TypeError):
        provider["_opts"] = {}
    return provider


def _http_client(provider: dict) -> httpx.AsyncClient:
    opts = provider.get("_opts", {})
    verify = opts.get("verify_tls", False)  # default False for fleet internal CA
    timeout = opts.get("timeout", 60)
    return httpx.AsyncClient(
        verify=verify,
        timeout=timeout,
        headers={"Authorization": f"Bearer {provider['api_key']}"},
    )


# ── Public API ────────────────────────────────────────────────────────────────


async def embed(
    project_name: str,
    texts: list[str],
) -> list[list[float]]:
    """
    Embed *texts* using the project's assigned embedding provider.
    Returns a list of float vectors, one per input text.
    """
    provider = _get_provider(project_name, "embedding")
    payload = {"model": provider["model_name"], "input": texts}
    async with _http_client(provider) as client:
        resp = await client.post(
            f"{provider['base_url'].rstrip('/')}/v1/embeddings",
            json=payload,
        )
        resp.raise_for_status()
    data = resp.json()
    return [item["embedding"] for item in sorted(data["data"], key=lambda x: x["index"])]


async def rerank(
    project_name: str,
    query: str,
    documents: list[str],
    top_n: int | None = None,
) -> list[dict[str, Any]]:
    """
    Rerank *documents* against *query* using the project's reranker provider.
    Returns results sorted by relevance_score descending.
    Each item: {"index": int, "relevance_score": float, "document": {"text": str}}.
    """
    provider = _get_provider(project_name, "reranker")
    opts = provider.get("_opts", {})
    rerank_path = opts.get("rerank_endpoint", "/rerank")
    payload: dict[str, Any] = {
        "model": provider["model_name"],
        "query": query,
        "documents": documents,
    }
    if top_n is not None:
        payload["top_n"] = top_n
    async with _http_client(provider) as client:
        resp = await client.post(
            f"{provider['base_url'].rstrip('/')}{rerank_path}",
            json=payload,
        )
        resp.raise_for_status()
    results = resp.json()["results"]
    return sorted(results, key=lambda r: r["relevance_score"], reverse=True)


async def complete(
    project_name: str,
    messages: list[dict[str, str]],
    *,
    max_tokens: int = 1024,
    strip_think: bool = False,
    return_parts: bool = False,
    no_think: bool = False,
) -> str | dict[str, str]:
    """
    Call the LLM provider assigned to *project_name*.

    Parameters
    ----------
    messages      : OpenAI-format message list
    max_tokens    : max output tokens (include headroom for <think> blocks)
    strip_think   : remove <think>…</think> from output
    return_parts  : return {"thinking": "…", "answer": "…"} dict
    no_think      : prepend /no-think to the last user message so the model
                    skips the reasoning step entirely (Qwen3-specific)

    Returns str unless return_parts=True.
    """
    provider = _get_provider(project_name, "llm")
    opts = provider.get("_opts", {})

    msgs = list(messages)
    if no_think and opts.get("no_think_supported"):
        # Prepend /no-think directive to the last user message
        for i in range(len(msgs) - 1, -1, -1):
            if msgs[i].get("role") == "user":
                msgs[i] = {**msgs[i], "content": f"/no-think\n{msgs[i]['content']}"}
                break

    payload = {
        "model": provider["model_name"],
        "messages": msgs,
        "max_tokens": max_tokens,
    }
    async with _http_client(provider) as client:
        resp = await client.post(
            f"{provider['base_url'].rstrip('/')}/v1/chat/completions",
            json=payload,
        )
        resp.raise_for_status()

    raw = resp.json()["choices"][0]["message"]["content"]

    if return_parts:
        return _split_think(raw)
    if strip_think:
        return _strip_think(raw)
    return raw
