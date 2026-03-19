"""routes_ai_providers.py — CRUD for /api/v1/ai-providers

AI provider records store connection details for LiteLLM-compatible AI
endpoints (embeddings, rerankers, LLMs). Records are fleet-synced so every
node has the same provider catalogue in its local DB.

api_key is stored in the fleet-synced SQLite DB — treat it as
infrastructure-internal. Do not expose the DB file publicly.
"""

import uuid

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import AiProviderCreate, AiProviderOut, AiProviderUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/ai-providers", tags=["ai-providers"])


def _row_to_out(row) -> AiProviderOut:
    d = dict(row)
    d["enabled"] = bool(d.get("enabled", 1))
    return AiProviderOut(**d)


@router.get("", response_model=list[AiProviderOut])
async def list_ai_providers() -> list[AiProviderOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ai_providers ORDER BY model_type, name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


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
