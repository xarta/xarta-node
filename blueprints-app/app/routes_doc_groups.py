"""routes_doc_groups.py — CRUD for the doc_groups table.

GET    /api/v1/doc-groups              → list all groups ordered by sort_order
POST   /api/v1/doc-groups              → create a group
PUT    /api/v1/doc-groups/{group_id}   → update name / sort_order
DELETE /api/v1/doc-groups/{group_id}   → delete group; moves member docs to Undefined Group
"""

import logging
import uuid

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from .db import get_conn, increment_gen
from .models import DocGroupCreate, DocGroupOut, DocGroupUpdate
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/doc-groups", tags=["doc-groups"])


def _row_to_out(row) -> DocGroupOut:
    return DocGroupOut(
        group_id=row["group_id"],
        name=row["name"],
        sort_order=row["sort_order"] if row["sort_order"] is not None else 0,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[DocGroupOut])
async def list_doc_groups() -> list[DocGroupOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM doc_groups ORDER BY sort_order, name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("", response_model=DocGroupOut, status_code=201)
async def create_doc_group(body: DocGroupCreate) -> DocGroupOut:
    group_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO doc_groups (group_id, name, sort_order) VALUES (?,?,?)",
            (group_id, body.name, body.sort_order),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM doc_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "doc_groups", group_id, dict(row), gen)
    return _row_to_out(row)


# ── Update ────────────────────────────────────────────────────────────────────

@router.put("/{group_id}", response_model=DocGroupOut)
async def update_doc_group(group_id: str, body: DocGroupUpdate) -> DocGroupOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM doc_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "doc group not found")
        conn.execute(
            """UPDATE doc_groups SET
               name       = COALESCE(?, name),
               sort_order = COALESCE(?, sort_order),
               updated_at = datetime('now')
               WHERE group_id = ?""",
            (body.name, body.sort_order, group_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM doc_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "doc_groups", group_id, dict(row), gen)
    return _row_to_out(row)


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{group_id}", status_code=204)
async def delete_doc_group(group_id: str) -> Response:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM doc_groups WHERE group_id=?", (group_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "doc group not found")

        # Move all docs in this group to Undefined Group and enqueue those updates
        affected_docs = conn.execute(
            "SELECT * FROM docs WHERE group_id=?", (group_id,)
        ).fetchall()
        if affected_docs:
            conn.execute(
                "UPDATE docs SET group_id=NULL, updated_at=datetime('now') WHERE group_id=?",
                (group_id,),
            )
            gen = increment_gen(conn, "human")
            for doc_row in affected_docs:
                updated = conn.execute(
                    "SELECT * FROM docs WHERE doc_id=?", (doc_row["doc_id"],)
                ).fetchone()
                enqueue_for_all_peers(conn, "UPDATE", "docs", doc_row["doc_id"], dict(updated), gen)

        # Delete the group
        conn.execute("DELETE FROM doc_groups WHERE group_id=?", (group_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "doc_groups", group_id, {}, gen)

    return Response(status_code=204)
