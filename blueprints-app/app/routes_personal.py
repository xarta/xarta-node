"""Shared Personal Time Activity projection API."""

from __future__ import annotations

import json
import os
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from .db import get_conn, increment_gen
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/personal", tags=["personal"])

DIARY_ROOT = Path(os.environ.get("BLUEPRINTS_DIARY_DIR", "/xarta-node/.lone-wolf/diary"))

PERSONAL_MODES: dict[str, dict[str, Any]] = {
    "today": {
        "label": "Today",
        "filters": {"date": "today", "status_exclude": ["archived"]},
    },
    "work": {
        "label": "Work",
        "filters": {"source_type": "work-management"},
    },
    "personal": {
        "label": "Personal",
        "filters": {"source_type": ["manual", "diary-file", "hermes-minutes", "browser-links"]},
    },
    "blocked": {
        "label": "Blocked",
        "filters": {"status": "blocked"},
    },
    "review": {
        "label": "Review",
        "filters": {"status": ["pending_review", "source_unavailable"]},
    },
    "imports": {
        "label": "Imports",
        "filters": {"source_type": ["interests-ingestion", "git"], "has_import_batch": True},
    },
    "git_activity": {
        "label": "Git Activity",
        "filters": {"source_type": "git"},
    },
}


class PersonalRehydrateRequest(BaseModel):
    event_id: str
    force: bool = False


def _json_value(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    with suppress(json.JSONDecodeError, TypeError):
        parsed = json.loads(value)
        return parsed if parsed is not None else fallback
    return fallback


def _row_to_event(row: Any) -> dict[str, Any]:
    return {
        "event_id": row["event_id"],
        "kind": row["kind"],
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "content_projection": row["content_projection"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
        "local_date": row["local_date"],
        "timezone": row["timezone"],
        "status": row["status"],
        "priority": row["priority"],
        "privacy_level": row["privacy_level"],
        "tags": _json_value(row["tags_json"], []),
        "entities": _json_value(row["entities_json"], []),
        "source": {
            "type": row["source_type"],
            "ref": row["source_ref"],
            "hash": row["source_hash"],
        },
        "related": {
            "work_items": _json_value(row["related_work_items_json"], []),
            "tasks": _json_value(row["related_tasks_json"], []),
            "import_batches": _json_value(row["related_import_batches_json"], []),
        },
        "file_refs": _json_value(row["file_refs_json"], []),
        "db_refs": _json_value(row["db_refs_json"], []),
        "provenance": _json_value(row["provenance_json"], {}),
        "projection_state": row["projection_state"],
        "provenance_state": row["provenance_state"],
        "last_rendered_at": row["last_rendered_at"],
        "projection_expires_at": row["projection_expires_at"],
        "retention_days": row["retention_days"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_import_batch(row: Any) -> dict[str, Any]:
    return {
        "import_batch_id": row["import_batch_id"],
        "source_type": row["source_type"],
        "source_ref": row["source_ref"],
        "title": row["title"],
        "status": row["status"],
        "local_date": row["local_date"],
        "started_at": row["started_at"],
        "completed_at": row["completed_at"],
        "privacy_level": row["privacy_level"],
        "artifact_refs": _json_value(row["artifact_refs_json"], []),
        "blocker_refs": _json_value(row["blocker_refs_json"], []),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_source(row: Any) -> dict[str, Any]:
    return {
        "source_id": row["source_id"],
        "source_type": row["source_type"],
        "label": row["label"],
        "status": row["status"],
        "last_seen_at": row["last_seen_at"],
        "health": _json_value(row["health_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _add_json_array_filter(where: list[str], params: list[Any], column: str, value: str) -> None:
    where.append(f"EXISTS (SELECT 1 FROM json_each({column}) WHERE value = ?)")
    params.append(value)


def _apply_mode(where: list[str], params: list[Any], mode: str | None) -> None:
    if not mode:
        return
    if mode not in PERSONAL_MODES:
        raise HTTPException(400, f"unknown mode: {mode}")
    if mode == "today":
        where.append("local_date = ?")
        params.append(datetime.now().astimezone().strftime("%Y-%m-%d"))
        where.append("status != 'archived'")
    elif mode == "work":
        where.append(
            "(source_type = 'work-management' OR json_array_length(related_work_items_json) > 0)"
        )
    elif mode == "personal":
        where.append("source_type IN ('manual', 'diary-file', 'hermes-minutes', 'browser-links')")
    elif mode == "blocked":
        where.append("status = 'blocked'")
    elif mode == "review":
        where.append(
            "(status IN ('pending_review', 'source_unavailable') OR provenance_state = 'needs_review')"
        )
    elif mode == "imports":
        where.append(
            "(json_array_length(related_import_batches_json) > 0 "
            "OR source_type IN ('interests-ingestion', 'git'))"
        )
    elif mode == "git_activity":
        where.append("source_type = 'git'")


def _pagination(limit: int, offset: int, count: int) -> dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "count": count,
        "has_more": count == limit,
    }


@router.get("/events")
async def list_personal_events(
    date_start: str | None = None,
    date_end: str | None = None,
    source_type: str | None = None,
    status: str | None = None,
    privacy_level: str | None = None,
    tag: str | None = None,
    related_work_item: str | None = None,
    import_batch: str | None = None,
    mode: str | None = None,
    kind: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if date_start:
        where.append("local_date >= ?")
        params.append(date_start)
    if date_end:
        where.append("local_date <= ?")
        params.append(date_end)
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    if status:
        where.append("status = ?")
        params.append(status)
    if privacy_level:
        where.append("privacy_level = ?")
        params.append(privacy_level)
    if kind:
        where.append("kind = ?")
        params.append(kind)
    if tag:
        _add_json_array_filter(where, params, "tags_json", tag)
    if related_work_item:
        _add_json_array_filter(where, params, "related_work_items_json", related_work_item)
    if import_batch:
        _add_json_array_filter(where, params, "related_import_batches_json", import_batch)
    _apply_mode(where, params, mode)

    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT * FROM personal_events
        {clause}
        ORDER BY COALESCE(start_at, created_at) ASC, event_id ASC
        LIMIT ? OFFSET ?
    """
    with get_conn() as conn:
        rows = conn.execute(sql, (*params, limit, offset)).fetchall()
    return {
        "items": [_row_to_event(row) for row in rows],
        "pagination": _pagination(limit, offset, len(rows)),
        "filters": {
            "date_start": date_start,
            "date_end": date_end,
            "source_type": source_type,
            "status": status,
            "privacy_level": privacy_level,
            "tag": tag,
            "related_work_item": related_work_item,
            "import_batch": import_batch,
            "mode": mode,
            "kind": kind,
        },
    }


@router.get("/events/{event_id}")
async def get_personal_event(event_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
    if not row:
        raise HTTPException(404, "event not found")
    return _row_to_event(row)


@router.get("/sources")
async def list_personal_sources() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM personal_sources ORDER BY source_type, label").fetchall()
        event_counts = conn.execute(
            "SELECT source_type, COUNT(*) AS count FROM personal_events GROUP BY source_type"
        ).fetchall()
    return {
        "items": [_row_to_source(row) for row in rows],
        "event_counts": {row["source_type"]: row["count"] for row in event_counts},
    }


@router.get("/modes")
async def list_personal_modes() -> dict[str, Any]:
    return {"items": [{"mode_id": key, **value} for key, value in PERSONAL_MODES.items()]}


@router.get("/import-batches")
async def list_personal_import_batches(
    date_start: str | None = None,
    date_end: str | None = None,
    source_type: str | None = None,
    status: str | None = None,
    privacy_level: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if date_start:
        where.append("local_date >= ?")
        params.append(date_start)
    if date_end:
        where.append("local_date <= ?")
        params.append(date_end)
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    if status:
        where.append("status = ?")
        params.append(status)
    if privacy_level:
        where.append("privacy_level = ?")
        params.append(privacy_level)

    clause = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
        SELECT * FROM personal_import_batches
        {clause}
        ORDER BY COALESCE(started_at, created_at) ASC, import_batch_id ASC
        LIMIT ? OFFSET ?
    """
    with get_conn() as conn:
        rows = conn.execute(sql, (*params, limit, offset)).fetchall()
    return {
        "items": [_row_to_import_batch(row) for row in rows],
        "pagination": _pagination(limit, offset, len(rows)),
        "filters": {
            "date_start": date_start,
            "date_end": date_end,
            "source_type": source_type,
            "status": status,
            "privacy_level": privacy_level,
        },
    }


def _resolve_file_ref(file_ref: str) -> Path:
    raw = Path(str(file_ref or "").strip())
    if not str(raw):
        raise ValueError("empty file ref")
    candidate = raw if raw.is_absolute() else DIARY_ROOT / raw
    root = DIARY_ROOT.resolve()
    resolved = candidate.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("file ref is outside diary root")
    return resolved


def _first_file_ref(row: Any) -> Path:
    refs = _json_value(row["file_refs_json"], [])
    if isinstance(refs, list):
        for ref in refs:
            if isinstance(ref, str):
                return _resolve_file_ref(ref)
            if isinstance(ref, dict) and ref.get("path"):
                return _resolve_file_ref(str(ref["path"]))
    raise ValueError("event has no file ref")


@router.post("/projections/rehydrate")
async def rehydrate_personal_projection(body: PersonalRehydrateRequest) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (body.event_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "event not found")
        if row["projection_state"] == "hot" and row["content_projection"] and not body.force:
            return {
                "ok": True,
                "rehydrated": False,
                "reason": "already_hot",
                "event": _row_to_event(row),
            }
        try:
            source_path = _first_file_ref(row)
            content = source_path.read_text(encoding="utf-8")
        except (OSError, ValueError) as exc:
            provenance = _json_value(row["provenance_json"], {})
            provenance["rehydrate_error"] = str(exc)
            conn.execute(
                """
                UPDATE personal_events
                SET projection_state='needs_rehydrate',
                    provenance_state='missing_source',
                    provenance_json=?,
                    updated_at=datetime('now')
                WHERE event_id=?
                """,
                (json.dumps(provenance, ensure_ascii=True), body.event_id),
            )
            gen = increment_gen(conn, "personal-rehydrate")
            updated = conn.execute(
                "SELECT * FROM personal_events WHERE event_id=?", (body.event_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn, "UPDATE", "personal_events", body.event_id, dict(updated), gen
            )
            return {
                "ok": False,
                "rehydrated": False,
                "reason": "source_unavailable",
                "event": _row_to_event(updated),
            }

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        excerpt = content.strip().replace("\n", " ")[:500]
        conn.execute(
            """
            UPDATE personal_events
            SET content_projection=?,
                body_excerpt=?,
                projection_state='hot',
                provenance_state='rehydrated',
                last_rendered_at=?,
                updated_at=datetime('now')
            WHERE event_id=?
            """,
            (content, excerpt, now, body.event_id),
        )
        gen = increment_gen(conn, "personal-rehydrate")
        updated = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (body.event_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "personal_events", body.event_id, dict(updated), gen)
    return {
        "ok": True,
        "rehydrated": True,
        "reason": "file_ref",
        "event": _row_to_event(updated),
    }
