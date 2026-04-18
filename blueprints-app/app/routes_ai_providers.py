"""routes_ai_providers.py — CRUD + probe for /api/v1/ai-providers

AI provider records store connection details for LiteLLM-compatible AI
endpoints (embeddings, rerankers, LLMs). Records are fleet-synced so every
node has the same provider catalogue in its local DB.

api_key is stored in the fleet-synced SQLite DB — treat it as
infrastructure-internal. Do not expose the DB file publicly.
"""

import asyncio
import json as _json
import uuid

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from .ai_observability import get_ai_observability_backend
from .db import get_conn, increment_gen
from .models import AiProviderCreate, AiProviderOut, AiProviderUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/ai-providers", tags=["ai-providers"])


def _row_to_out(row) -> AiProviderOut:
    d = dict(row)
    d["enabled"] = bool(d.get("enabled", 1))
    return AiProviderOut(**d)


def _list_provider_dicts() -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ai_providers ORDER BY model_type, name"
        ).fetchall()
    providers: list[dict] = []
    for row in rows:
        item = dict(row)
        item["enabled"] = bool(item.get("enabled", 1))
        providers.append(item)
    return providers


class _AiObservabilityTestBody(BaseModel):
    alias: str


@router.get("", response_model=list[AiProviderOut])
async def list_ai_providers() -> list[AiProviderOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ai_providers ORDER BY model_type, name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.get("/observability", response_model=dict)
async def get_ai_provider_observability() -> dict:
    """Return node-local AI observability data for the active backend, if present."""
    backend = get_ai_observability_backend()
    return await backend.describe(_list_provider_dicts())


@router.post("/observability/test", response_model=dict)
async def test_ai_provider_observability(body: _AiObservabilityTestBody) -> dict:
    alias = (body.alias or "").strip()
    if not alias:
        raise HTTPException(400, "alias is required")
    backend = get_ai_observability_backend()
    result = await backend.test_alias(alias, _list_provider_dicts())
    if result.get("status") == "alias_not_found":
        raise HTTPException(404, result.get("detail") or f"Alias {alias!r} not found")
    return result


@router.post("/observability/vision-test", response_model=dict)
async def test_ai_provider_observability_vision(body: _AiObservabilityTestBody) -> dict:
    alias = (body.alias or "").strip()
    if not alias:
        raise HTTPException(400, "alias is required")
    backend = get_ai_observability_backend()
    result = await backend.test_vision_alias(alias, _list_provider_dicts())
    if result.get("status") == "alias_not_found":
        raise HTTPException(404, result.get("detail") or f"Alias {alias!r} not found")
    return result


@router.post("", response_model=AiProviderOut, status_code=201)
async def create_ai_provider(body: AiProviderCreate) -> AiProviderOut:
    provider_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO ai_providers
                (provider_id, name, base_url, api_key, model_name, model_type,
                 dimensions, enabled, options, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                provider_id,
                body.name,
                body.base_url,
                body.api_key,
                body.model_name,
                body.model_type,
                body.dimensions,
                1 if body.enabled else 0,
                body.options,
                body.notes,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM ai_providers WHERE provider_id=?", (provider_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "ai_providers", provider_id, dict(row), gen)
    return _row_to_out(row)


@router.get("/{provider_id}", response_model=AiProviderOut)
async def get_ai_provider(provider_id: str) -> AiProviderOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM ai_providers WHERE provider_id=?", (provider_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"Provider {provider_id!r} not found")
    return _row_to_out(row)


@router.put("/{provider_id}", response_model=AiProviderOut)
async def update_ai_provider(provider_id: str, body: AiProviderUpdate) -> AiProviderOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT provider_id FROM ai_providers WHERE provider_id=?", (provider_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Provider {provider_id!r} not found")
        enabled_val = None if body.enabled is None else (1 if body.enabled else 0)
        conn.execute(
            """
            UPDATE ai_providers SET
                name       = COALESCE(?, name),
                base_url   = COALESCE(?, base_url),
                api_key    = COALESCE(?, api_key),
                model_name = COALESCE(?, model_name),
                model_type = COALESCE(?, model_type),
                dimensions = COALESCE(?, dimensions),
                enabled    = COALESCE(?, enabled),
                options    = COALESCE(?, options),
                notes      = COALESCE(?, notes),
                updated_at = datetime('now')
            WHERE provider_id = ?
            """,
            (
                body.name, body.base_url, body.api_key, body.model_name,
                body.model_type, body.dimensions, enabled_val,
                body.options, body.notes, provider_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM ai_providers WHERE provider_id=?", (provider_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "ai_providers", provider_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{provider_id}", status_code=204)
async def delete_ai_provider(provider_id: str) -> None:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT provider_id FROM ai_providers WHERE provider_id=?", (provider_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Provider {provider_id!r} not found")
        conn.execute("DELETE FROM ai_providers WHERE provider_id=?", (provider_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "ai_providers", provider_id, {}, gen)


@router.get("/for-project/{project_name}/{role}", response_model=AiProviderOut)
async def get_provider_for_project(project_name: str, role: str) -> AiProviderOut:
    """Return the highest-priority enabled provider assigned to a project+role."""
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
        raise HTTPException(
            404,
            f"No enabled provider found for project={project_name!r} role={role!r}",
        )
    return _row_to_out(row)


# ── Probe ─────────────────────────────────────────────────────────────────────
# POST is used (not GET) to avoid routing conflict with GET /{provider_id}.


@router.post("/probe", response_model=dict)
async def probe_ai_providers(
    inference: bool = Query(
        False,
        description="When True, also send a minimal inference call to each "
        "provider (embedding/reranker/LLM). This triggers GPU.",
    ),
) -> dict:
    """
    Probe all enabled AI providers using their stored connection details.

    Lightweight steps (always, no GPU):
      1. GET {base_url}/health/liveliness — no auth required
      2. GET {base_url}/health/readiness  — no auth required
      3. GET {base_url}/v1/models         — check model_name is listed

    Inference steps (only when ?inference=true, triggers GPU):
      4. embedding providers → POST /v1/embeddings
      5. reranker providers  → POST {rerank_endpoint} (default /rerank)
      6. llm providers       → POST /v1/chat/completions

    Base-URL-level checks (liveliness, readiness, models list) are shared
    across providers that share the same endpoint, so shared endpoints are
    only hit once per check type regardless of how many providers point there.

    As new providers are added to ai_providers the probe automatically covers
    them — no code changes needed.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ai_providers WHERE enabled=1 ORDER BY model_type, name"
        ).fetchall()

    if not rows:
        return {
            "providers": [],
            "inference_tested": inference,
            "all_lightweight_ok": True,
            "summary": "No enabled providers in DB",
        }

    providers: list[dict] = []
    for row in rows:
        p = dict(row)
        try:
            p["_opts"] = _json.loads(p.get("options") or "{}")
        except (ValueError, TypeError):
            p["_opts"] = {}
        providers.append(p)

    # ── Shared per-base_url checks ─────────────────────────────────────────────
    # Deduplicate: multiple providers may share the same LiteLLM endpoint.
    base_url_creds: dict[str, dict] = {}
    for p in providers:
        url = p["base_url"].rstrip("/")
        if url not in base_url_creds:
            base_url_creds[url] = {
                "api_key": p["api_key"],
                "verify": p["_opts"].get("verify_tls", False),
                "timeout": int(p["_opts"].get("timeout", 10)),
            }

    async def _get_check(url: str, headers: dict | None = None,
                         verify: bool = False, timeout: int = 10) -> dict:
        try:
            async with httpx.AsyncClient(verify=verify, timeout=timeout) as client:
                r = await client.get(url, headers=headers or {})
            return {"ok": r.is_success, "status": r.status_code, "error": None}
        except Exception as exc:
            return {"ok": False, "status": None, "error": str(exc)[:200]}

    async def _post_check(url: str, headers: dict | None = None,
                          payload: dict | None = None,
                          verify: bool = False, timeout: int = 30) -> dict:
        try:
            async with httpx.AsyncClient(verify=verify, timeout=timeout) as client:
                r = await client.post(url, headers=headers or {}, json=payload or {})
            return {"ok": r.is_success, "status": r.status_code, "error": None}
        except Exception as exc:
            return {"ok": False, "status": None, "error": str(exc)[:200]}

    async def _probe_base_url(url: str, creds: dict) -> dict:
        verify = creds["verify"]
        to = creds["timeout"]
        auth = {"Authorization": f"Bearer {creds['api_key']}"}

        async def _models_check() -> dict:
            try:
                async with httpx.AsyncClient(verify=verify, timeout=to) as client:
                    r = await client.get(f"{url}/v1/models", headers=auth)
                ids: list[str] = []
                if r.is_success:
                    try:
                        ids = [m["id"] for m in r.json().get("data", [])]
                    except Exception:
                        pass
                return {
                    "ok": r.is_success,
                    "status": r.status_code,
                    "error": None,
                    "model_ids": ids,
                }
            except Exception as exc:
                return {
                    "ok": False, "status": None,
                    "error": str(exc)[:200], "model_ids": [],
                }

        live, ready, models = await asyncio.gather(
            _get_check(f"{url}/health/liveliness", verify=verify, timeout=to),
            _get_check(f"{url}/health/readiness",  verify=verify, timeout=to),
            _models_check(),
        )
        return {
            "liveliness": live,
            "readiness": ready,
            "models_ok": {"ok": models["ok"], "status": models["status"],
                          "error": models.get("error")},
            "model_ids": models["model_ids"],
        }

    # Run all base_url probes concurrently
    base_results: dict[str, dict] = dict(zip(
        base_url_creds.keys(),
        await asyncio.gather(
            *[_probe_base_url(u, c) for u, c in base_url_creds.items()]
        ),
    ))

    # ── Per-provider results ───────────────────────────────────────────────────
    async def _build_provider_result(p: dict) -> dict:
        url = p["base_url"].rstrip("/")
        opts = p["_opts"]
        verify = opts.get("verify_tls", False)
        timeout = int(opts.get("timeout", 30))
        auth = {"Authorization": f"Bearer {p['api_key']}"}

        br = base_results.get(url, {})
        model_in_list = p["model_name"] in br.get("model_ids", [])

        checks: dict = {
            "liveliness": br.get("liveliness"),
            "readiness":  br.get("readiness"),
            "models_ok":  br.get("models_ok"),
            "model_in_list": model_in_list,
        }

        if inference:
            mt = p["model_type"]
            if mt == "embedding":
                checks["inference"] = await _post_check(
                    f"{url}/v1/embeddings", headers=auth,
                    payload={"model": p["model_name"], "input": "connectivity test"},
                    verify=verify, timeout=timeout,
                )
            elif mt == "reranker":
                rerank_path = opts.get("rerank_endpoint", "/rerank")
                checks["inference"] = await _post_check(
                    f"{url}{rerank_path}", headers=auth,
                    payload={
                        "model": p["model_name"],
                        "query": "test",
                        "documents": ["connectivity test"],
                    },
                    verify=verify, timeout=timeout,
                )
            elif mt == "llm":
                checks["inference"] = await _post_check(
                    f"{url}/v1/chat/completions", headers=auth,
                    payload={
                        "model": p["model_name"],
                        "messages": [{"role": "user",
                                      "content": "/no-think Reply with the single word ok"}],
                        "max_tokens": 16,
                    },
                    verify=verify, timeout=timeout,
                )

        lightweight_ok = bool(
            (checks.get("liveliness") or {}).get("ok")
            and (checks.get("readiness") or {}).get("ok")
            and (checks.get("models_ok") or {}).get("ok")
            and model_in_list
        )

        return {
            "provider_id": p["provider_id"],
            "name": p["name"],
            "model_name": p["model_name"],
            "model_type": p["model_type"],
            "base_url": url,
            "checks": checks,
            "lightweight_ok": lightweight_ok,
            "inference_ok": (
                bool((checks.get("inference") or {}).get("ok")) if inference else None
            ),
        }

    # Run inference calls concurrently across providers (fine for small N)
    provider_results = list(
        await asyncio.gather(*[_build_provider_result(p) for p in providers])
    )

    all_lightweight_ok = all(r["lightweight_ok"] for r in provider_results)

    return {
        "providers": provider_results,
        "inference_tested": inference,
        "all_lightweight_ok": all_lightweight_ok,
    }
