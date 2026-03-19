"""routes_ai_project_assignments.py — CRUD for /api/v1/ai-project-assignments

Links AI providers to named projects so application code can ask
"give me the embedding provider for project X" via the convenience endpoint
on routes_ai_providers.  Fleet-synced the same as every other table.
"""

import uuid

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import (
    AiProjectAssignmentCreate,
    AiProjectAssignmentOut,
    AiProjectAssignmentUpdate,
)
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/ai-project-assignments", tags=["ai-project-assignments"])


def _row_to_out(row) -> AiProjectAssignmentOut:
    d = dict(row)
    d["enabled"] = bool(d.get("enabled", 1))
    return AiProjectAssignmentOut(**d)


@router.get("", response_model=list[AiProjectAssignmentOut])
async def list_ai_project_assignments(project: str | None = None) -> list[AiProjectAssignmentOut]:
    with get_conn() as conn:
        if project:
            rows = conn.execute(
                "SELECT * FROM ai_project_assignments WHERE project_name=? ORDER BY role, priority DESC",
                (project,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM ai_project_assignments ORDER BY project_name, role, priority DESC"
            ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=AiProjectAssignmentOut, status_code=201)
async def create_ai_project_assignment(body: AiProjectAssignmentCreate) -> AiProjectAssignmentOut:
    assignment_id = str(uuid.uuid4())
    with get_conn() as conn:
        # Verify referenced provider exists
        provider = conn.execute(
            "SELECT provider_id FROM ai_providers WHERE provider_id=?", (body.provider_id,)
        ).fetchone()
        if not provider:
            raise HTTPException(404, f"Provider {body.provider_id!r} not found")
        conn.execute(
            """
            INSERT INTO ai_project_assignments
                (assignment_id, project_name, provider_id, role, priority, enabled)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                assignment_id,
                body.project_name,
                body.provider_id,
                body.role,
                body.priority,
                1 if body.enabled else 0,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM ai_project_assignments WHERE assignment_id=?", (assignment_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "INSERT", "ai_project_assignments", assignment_id, dict(row), gen
        )
    return _row_to_out(row)


@router.get("/{assignment_id}", response_model=AiProjectAssignmentOut)
async def get_ai_project_assignment(assignment_id: str) -> AiProjectAssignmentOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM ai_project_assignments WHERE assignment_id=?", (assignment_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"Assignment {assignment_id!r} not found")
    return _row_to_out(row)


@router.put("/{assignment_id}", response_model=AiProjectAssignmentOut)
async def update_ai_project_assignment(
    assignment_id: str, body: AiProjectAssignmentUpdate
) -> AiProjectAssignmentOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT assignment_id FROM ai_project_assignments WHERE assignment_id=?",
            (assignment_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Assignment {assignment_id!r} not found")
        if body.provider_id is not None:
            provider = conn.execute(
                "SELECT provider_id FROM ai_providers WHERE provider_id=?", (body.provider_id,)
            ).fetchone()
            if not provider:
                raise HTTPException(404, f"Provider {body.provider_id!r} not found")
        enabled_val = None if body.enabled is None else (1 if body.enabled else 0)
        conn.execute(
            """
            UPDATE ai_project_assignments SET
                project_name = COALESCE(?, project_name),
                provider_id  = COALESCE(?, provider_id),
                role         = COALESCE(?, role),
                priority     = COALESCE(?, priority),
                enabled      = COALESCE(?, enabled),
                updated_at   = datetime('now')
            WHERE assignment_id = ?
            """,
            (
                body.project_name, body.provider_id, body.role,
                body.priority, enabled_val, assignment_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM ai_project_assignments WHERE assignment_id=?", (assignment_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "UPDATE", "ai_project_assignments", assignment_id, dict(row), gen
        )
    return _row_to_out(row)


@router.delete("/{assignment_id}", status_code=204)
async def delete_ai_project_assignment(assignment_id: str) -> None:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT assignment_id FROM ai_project_assignments WHERE assignment_id=?",
            (assignment_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Assignment {assignment_id!r} not found")
        conn.execute(
            "DELETE FROM ai_project_assignments WHERE assignment_id=?", (assignment_id,)
        )
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "ai_project_assignments", assignment_id, {}, gen)
