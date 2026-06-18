"""Shared Personal Time Activity projection API."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import posixpath
import re
import sqlite3
import subprocess
import sys
import uuid
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import hermes_minutes
from .db import get_conn, increment_gen
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/personal", tags=["personal"])

DIARY_ROOT = Path(os.environ.get("BLUEPRINTS_DIARY_DIR", "/xarta-node/.lone-wolf/diary"))
XARTA_AGENT_LIB = Path(os.environ.get("XARTA_AGENT_LIB", "/root/xarta-node/.xarta/.agents/lib"))
LONE_WOLF_ROOT = Path(os.environ.get("BLUEPRINTS_LONE_WOLF_ROOT", "/xarta-node/.lone-wolf"))
INTERESTS_DASHBOARD_REL = Path("docs/interests/HERMES-INTERESTS-INGESTION-DASHBOARD.md")
DAY_SUMMARY_SCHEMA = "xarta.diary.day_summary.v1"
DEFAULT_PERSONAL_GIT_REPOS: tuple[tuple[str, str, str], ...] = (
    ("p300", "/xarta-node", "Public non-root workspace"),
    ("p200", "/root/xarta-node", "Public root workspace"),
    ("p100", "/root/xarta-node/.xarta", "Private inner workspace"),
    ("p400", "/xarta-node/.lone-wolf", "Node-local lone-wolf workspace"),
)

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
        "filters": {
            "source_type": [
                "manual",
                "diary-file",
                "manual-calendar",
                "hermes-minutes",
                "browser-links",
            ]
        },
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


class DiaryEntryCreateRequest(BaseModel):
    body: str
    local_date: str | None = None
    local_time: str | None = None
    timezone: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "diary-page"
    request_id: str | None = None
    run_id: str | None = None
    tags: list[str] = []


class DiarySummaryGenerateRequest(BaseModel):
    local_date: str
    actor: str = "blueprints-ui"
    source_surface: str = "diary-page"
    request_id: str | None = None
    run_id: str | None = None


class DiaryWorkLinkRequest(BaseModel):
    work_item_ref: str
    actor: str = "blueprints-ui"
    source_surface: str = "diary-page"
    request_id: str | None = None
    run_id: str | None = None


class CalendarEventUpsertRequest(BaseModel):
    event_id: str | None = None
    title: str
    body: str | None = None
    local_date: str
    start_time: str | None = None
    end_time: str | None = None
    timezone: str | None = None
    all_day: bool = False
    kind: str = "calendar-event"
    status: str = "open"
    priority: str | None = None
    privacy_level: str = "normal"
    tags: list[str] = []
    related_work_items: list[str] = []
    related_tasks: list[str] = []
    related_import_batches: list[str] = []
    actor: str = "blueprints-ui"
    source_surface: str = "calendar-page"
    request_id: str | None = None
    run_id: str | None = None


class DiaryMinutesProjectRequest(BaseModel):
    local_date: str | None = None
    timezone: str | None = None
    limit: int = 200
    ttl_seconds: float | None = None
    source_owner: str = ""
    actor: str = "blueprints-api"
    source_surface: str = "minutes-projection"
    request_id: str | None = None
    run_id: str | None = None


class DiaryBrowserLinksProjectRequest(BaseModel):
    local_date: str | None = None
    timezone: str | None = None
    limit: int = 1000
    actor: str = "blueprints-api"
    source_surface: str = "browser-links-projection"
    request_id: str | None = None
    run_id: str | None = None


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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _validate_local_date(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return datetime.now().astimezone().strftime("%Y-%m-%d")
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(400, "date must use YYYY-MM-DD") from exc
    return parsed.strftime("%Y-%m-%d")


def _validate_local_time(value: str | None) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    if not re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", text):
        raise HTTPException(400, "time must use HH:MM")
    return text


def _clean_short_text(value: str | None, default: str, *, limit: int = 120) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    return (text or default)[:limit]


def _diary_day_dir(local_date: str) -> Path:
    year, month, day = local_date.split("-")
    return DIARY_ROOT / year / month / day


def _diary_relative_path(path: Path) -> str:
    resolved = Path(path).resolve()
    root = DIARY_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("diary path is outside diary root")
    return resolved.relative_to(root).as_posix()


def _load_xarta_diary_module() -> Any:
    if str(XARTA_AGENT_LIB) not in sys.path:
        sys.path.insert(0, str(XARTA_AGENT_LIB))
    try:
        return importlib.import_module("xarta_diary")
    except ImportError as exc:
        raise HTTPException(503, "diary writer helper is unavailable") from exc


def _read_json_file(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def _read_text_file(path: Path, limit: int = 6000) -> str:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return ""
    return text[:limit]


def _entry_title(body: str, local_time: str | None) -> str:
    first = next((line.strip() for line in body.splitlines() if line.strip()), "")
    return first[:90] if first else f"Personal log {local_time or ''}".strip()


def _body_excerpt(body: str, limit: int = 500) -> str:
    return re.sub(r"\s+", " ", body.strip())[:limit]


def _upsert_personal_source(conn: Any, now: str) -> None:
    conn.execute(
        """
        INSERT INTO personal_sources (
            source_id, source_type, label, status, last_seen_at, health_json, provenance_json,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            status=excluded.status,
            last_seen_at=excluded.last_seen_at,
            health_json=excluded.health_json,
            provenance_json=excluded.provenance_json,
            updated_at=excluded.updated_at
        """,
        (
            "manual-diary",
            "manual",
            "Manual Diary",
            "ok",
            now,
            json.dumps({"write_path": "file-first"}),
            json.dumps({"diary_root": str(DIARY_ROOT)}),
            now,
            now,
        ),
    )


def _upsert_calendar_source(conn: Any, now: str) -> Any:
    conn.execute(
        """
        INSERT INTO personal_sources (
            source_id, source_type, label, status, last_seen_at, health_json, provenance_json,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            status=excluded.status,
            last_seen_at=excluded.last_seen_at,
            health_json=excluded.health_json,
            provenance_json=excluded.provenance_json,
            updated_at=excluded.updated_at
        """,
        (
            "manual-calendar",
            "manual-calendar",
            "Manual Calendar",
            "ok",
            now,
            json.dumps({"write_path": "personal_events"}),
            json.dumps({"events_table": "personal_events"}),
            now,
            now,
        ),
    )
    return conn.execute(
        "SELECT * FROM personal_sources WHERE source_id='manual-calendar'"
    ).fetchone()


def _write_personal_audit(
    conn: Any,
    *,
    audit_id: str,
    actor: str,
    source_surface: str,
    action: str,
    target_ref: str,
    file_ref: str,
    db_ref: str,
    request_id: str,
    run_id: str,
    result: str,
    source_hash: str,
    metadata: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    row = {
        "audit_id": audit_id,
        "actor": actor,
        "source_surface": source_surface,
        "action": action,
        "target_ref": target_ref,
        "file_ref": file_ref,
        "db_ref": db_ref,
        "created_at": created_at,
        "request_id": request_id,
        "run_id": run_id,
        "result": result,
        "source_hash": source_hash,
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
    }
    conn.execute(
        """
        INSERT INTO personal_time_audit (
            audit_id, actor, source_surface, action, target_ref, file_ref, db_ref,
            created_at, request_id, run_id, result, source_hash, metadata_json
        )
        VALUES (
            :audit_id, :actor, :source_surface, :action, :target_ref, :file_ref, :db_ref,
            :created_at, :request_id, :run_id, :result, :source_hash, :metadata_json
        )
        ON CONFLICT(audit_id) DO UPDATE SET
            actor=excluded.actor,
            source_surface=excluded.source_surface,
            action=excluded.action,
            target_ref=excluded.target_ref,
            file_ref=excluded.file_ref,
            db_ref=excluded.db_ref,
            created_at=excluded.created_at,
            request_id=excluded.request_id,
            run_id=excluded.run_id,
            result=excluded.result,
            source_hash=excluded.source_hash,
            metadata_json=excluded.metadata_json
        """,
        row,
    )
    return row


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
        where.append(
            "source_type IN ('manual', 'diary-file', 'manual-calendar', "
            "'hermes-minutes', 'browser-links')"
        )
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


@router.post("/events/{event_id}/work-links")
async def link_personal_event_work_item(
    event_id: str, body: DiaryWorkLinkRequest
) -> dict[str, Any]:
    work_ref = _clean_short_text(body.work_item_ref, "", limit=200)
    if not work_ref:
        raise HTTPException(400, "work item ref is required")
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "diary-page")
    request_id = _clean_short_text(body.request_id, f"work-link-{uuid.uuid4().hex[:12]}", limit=160)
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    audit_id = f"audit-{uuid.uuid4().hex}"
    now = _utc_now_iso()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, "event not found")
        work_items = _json_value(row["related_work_items_json"], [])
        if not isinstance(work_items, list):
            work_items = []
        if work_ref not in work_items:
            work_items.append(work_ref)
        provenance = _json_value(row["provenance_json"], {})
        links = provenance.get("work_link_audit") if isinstance(provenance, dict) else []
        if not isinstance(links, list):
            links = []
        links.append({"work_item_ref": work_ref, "audit_id": audit_id, "created_at": now})
        provenance["work_link_audit"] = links[-12:]
        conn.execute(
            """
            UPDATE personal_events
            SET related_work_items_json=?,
                provenance_json=?,
                updated_at=?
            WHERE event_id=?
            """,
            (
                json.dumps(work_items, ensure_ascii=True),
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                now,
                event_id,
            ),
        )
        updated = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action="link_work_item",
            target_ref=f"personal_events:{event_id}",
            file_ref="",
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=row["source_hash"] or "",
            metadata={"work_item_ref": work_ref},
        )
        gen = increment_gen(conn, "personal-work-link")
        enqueue_for_all_peers(conn, "UPDATE", "personal_events", event_id, dict(updated), gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {"ok": True, "event": _row_to_event(updated), "audit": {"audit_id": audit_id}}


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


def _validate_timezone_name(value: str | None) -> str:
    timezone_name = _clean_short_text(
        value or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
        "Europe/London",
        limit=80,
    )
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise HTTPException(400, "timezone is invalid") from exc
    return timezone_name


def _calendar_event_id(value: str | None, local_date: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", str(value or "").strip()).strip("-")
    if clean:
        return clean[:180]
    return f"calendar-{local_date}-{uuid.uuid4().hex[:12]}"


def _calendar_utc_iso(local_date: str, local_time: str | None, timezone_name: str) -> str:
    time_text = local_time or "00:00"
    local_dt = datetime.strptime(f"{local_date} {time_text}", "%Y-%m-%d %H:%M").replace(
        tzinfo=ZoneInfo(timezone_name)
    )
    return (
        local_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )


def _clean_event_list(values: list[str], *, limit: int = 24) -> list[str]:
    cleaned: list[str] = []
    for value in values[:limit]:
        text = _clean_short_text(value, "", limit=220)
        if text and text not in cleaned:
            cleaned.append(text)
    return cleaned


def _calendar_event_payload(
    body: CalendarEventUpsertRequest,
    *,
    event_id: str | None = None,
) -> dict[str, Any]:
    local_date = _validate_local_date(body.local_date)
    timezone_name = _validate_timezone_name(body.timezone)
    start_time = None if body.all_day else _validate_local_time(body.start_time)
    end_time = None if body.all_day else _validate_local_time(body.end_time)
    start_at = _calendar_utc_iso(local_date, start_time, timezone_name)
    end_at = _calendar_utc_iso(local_date, end_time, timezone_name) if end_time else None
    if end_at and end_at < start_at:
        raise HTTPException(400, "end time must not be before start time")
    title = _clean_short_text(body.title, "", limit=180)
    if not title:
        raise HTTPException(400, "calendar event title is required")
    kind = _clean_short_text(body.kind, "calendar-event", limit=80)
    if kind not in {"calendar-event", "reminder", "todo", "task", "milestone"}:
        raise HTTPException(400, "calendar event kind is invalid")
    status = _clean_short_text(body.status, "open", limit=80)
    privacy_level = _clean_short_text(body.privacy_level, "normal", limit=40)
    tags = _clean_event_list(body.tags, limit=24)
    for required in ("calendar", kind):
        if required not in tags:
            tags.append(required)
    if body.all_day and "all-day" not in tags:
        tags.append("all-day")
    if not body.all_day and "timed" not in tags:
        tags.append("timed")
    content = _body_excerpt(body.body or "", limit=2000)
    provenance = {
        "calendar": {
            "all_day": bool(body.all_day),
            "local_start_time": start_time or "",
            "local_end_time": end_time or "",
            "timezone": timezone_name,
        },
        "actor": _clean_short_text(body.actor, "blueprints-ui"),
        "source_surface": _clean_short_text(body.source_surface, "calendar-page"),
        "request_id": _clean_short_text(
            body.request_id, f"calendar-event-{uuid.uuid4().hex[:12]}", limit=160
        ),
        "run_id": _clean_short_text(
            body.run_id or body.request_id,
            body.request_id or f"calendar-run-{uuid.uuid4().hex[:12]}",
            limit=160,
        ),
    }
    event_payload = {
        "event_id": _calendar_event_id(event_id or body.event_id, local_date),
        "source_type": "manual-calendar",
        "source_ref": "",
        "kind": kind,
        "title": title,
        "body_excerpt": content,
        "content_projection": content,
        "start_at": start_at,
        "end_at": end_at,
        "local_date": local_date,
        "timezone": timezone_name,
        "status": status,
        "priority": _clean_short_text(body.priority, "", limit=40) or None,
        "privacy_level": privacy_level,
        "tags": tags,
        "related_work_items": _clean_event_list(body.related_work_items),
        "related_tasks": _clean_event_list(body.related_tasks),
        "related_import_batches": _clean_event_list(body.related_import_batches),
        "provenance": provenance,
    }
    event_payload["source_ref"] = f"personal_events:{event_payload['event_id']}"
    event_payload["source_hash"] = _hash_json_payload(event_payload)
    return event_payload


def _upsert_calendar_event(
    body: CalendarEventUpsertRequest,
    *,
    event_id: str | None = None,
    action: str,
) -> dict[str, Any]:
    payload = _calendar_event_payload(body, event_id=event_id)
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    actor = payload["provenance"]["actor"]
    source_surface = payload["provenance"]["source_surface"]
    request_id = payload["provenance"]["request_id"]
    run_id = payload["provenance"]["run_id"]
    with get_conn() as conn:
        source_row = _upsert_calendar_source(conn, now)
        previous = conn.execute(
            "SELECT created_at FROM personal_events WHERE event_id=?", (payload["event_id"],)
        ).fetchone()
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        conn.execute(
            """
            INSERT INTO personal_events (
                event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
                content_projection, start_at, end_at, local_date, timezone, status, priority,
                privacy_level, tags_json, related_work_items_json, related_tasks_json,
                related_import_batches_json, file_refs_json, db_refs_json, provenance_json,
                projection_state, provenance_state, last_rendered_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                source_type=excluded.source_type,
                source_ref=excluded.source_ref,
                source_hash=excluded.source_hash,
                kind=excluded.kind,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                content_projection=excluded.content_projection,
                start_at=excluded.start_at,
                end_at=excluded.end_at,
                local_date=excluded.local_date,
                timezone=excluded.timezone,
                status=excluded.status,
                priority=excluded.priority,
                privacy_level=excluded.privacy_level,
                tags_json=excluded.tags_json,
                related_work_items_json=excluded.related_work_items_json,
                related_tasks_json=excluded.related_tasks_json,
                related_import_batches_json=excluded.related_import_batches_json,
                db_refs_json=excluded.db_refs_json,
                provenance_json=excluded.provenance_json,
                projection_state=excluded.projection_state,
                provenance_state=excluded.provenance_state,
                last_rendered_at=excluded.last_rendered_at,
                updated_at=excluded.updated_at
            """,
            (
                payload["event_id"],
                payload["source_type"],
                payload["source_ref"],
                payload["source_hash"],
                payload["kind"],
                payload["title"],
                payload["body_excerpt"],
                payload["content_projection"],
                payload["start_at"],
                payload["end_at"],
                payload["local_date"],
                payload["timezone"],
                payload["status"],
                payload["priority"],
                payload["privacy_level"],
                json.dumps(payload["tags"], ensure_ascii=True),
                json.dumps(payload["related_work_items"], ensure_ascii=True),
                json.dumps(payload["related_tasks"], ensure_ascii=True),
                json.dumps(payload["related_import_batches"], ensure_ascii=True),
                json.dumps([], ensure_ascii=True),
                json.dumps([f"personal_time_audit:{audit_id}"], ensure_ascii=True),
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                "hot",
                "linked",
                now,
                created_at,
                now,
            ),
        )
        event_row = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (payload["event_id"],)
        ).fetchone()
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action=action,
            target_ref=f"personal_events:{payload['event_id']}",
            file_ref="",
            db_ref=f"personal_events:{payload['event_id']}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=payload["source_hash"],
            metadata={
                "local_date": payload["local_date"],
                "kind": payload["kind"],
                "all_day": bool(payload["provenance"]["calendar"]["all_day"]),
            },
        )
        gen = increment_gen(conn, "personal-calendar-event")
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_sources", "manual-calendar", dict(source_row), gen
        )
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_events", payload["event_id"], dict(event_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "event": _row_to_event(event_row),
        "audit": {"audit_id": audit_id, "result": "ok", "action": action},
    }


@router.post("/calendar/events")
async def create_calendar_event(body: CalendarEventUpsertRequest) -> dict[str, Any]:
    return _upsert_calendar_event(body, action="create_calendar_event")


@router.patch("/calendar/events/{event_id}")
async def update_calendar_event(event_id: str, body: CalendarEventUpsertRequest) -> dict[str, Any]:
    clean_event_id = _calendar_event_id(event_id, _validate_local_date(body.local_date))
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT event_id FROM personal_events WHERE event_id=?", (clean_event_id,)
        ).fetchone()
    if not existing:
        raise HTTPException(404, "calendar event not found")
    return _upsert_calendar_event(body, event_id=clean_event_id, action="update_calendar_event")


def _visible_day_events(
    local_date: str,
    *,
    source_filter: str = "all",
    limit: int = 200,
) -> tuple[list[dict[str, Any]], int, dict[str, int]]:
    where = ["local_date = ?", "privacy_level != 'pin'"]
    params: list[Any] = [local_date]
    if source_filter == "manual":
        where.append("source_type IN ('manual', 'diary-file')")
    elif source_filter == "sources":
        where.append("source_type NOT IN ('manual', 'diary-file')")
    elif source_filter == "git":
        where.append("source_type = 'git'")
    elif source_filter == "imports":
        where.append("source_type IN ('interests-ingestion', 'git')")
    elif source_filter == "work":
        where.append(
            "(source_type = 'work-management' OR json_array_length(related_work_items_json) > 0)"
        )
    elif source_filter != "all":
        raise HTTPException(400, f"unknown source filter: {source_filter}")

    clause = f"WHERE {' AND '.join(where)}"
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM personal_events
            {clause}
            ORDER BY COALESCE(start_at, created_at) ASC, event_id ASC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        pin_hidden = conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE local_date=? AND privacy_level='pin'",
            (local_date,),
        ).fetchone()
        source_rows = conn.execute(
            """
            SELECT source_type, COUNT(*) AS count
            FROM personal_events
            WHERE local_date=? AND privacy_level != 'pin'
            GROUP BY source_type
            """,
            (local_date,),
        ).fetchall()
    return (
        [_row_to_event(row) for row in rows],
        int(pin_hidden["count"] if pin_hidden else 0),
        {row["source_type"]: row["count"] for row in source_rows},
    )


def _summary_payload(day_dir: Path, events: list[dict[str, Any]]) -> dict[str, Any]:
    path = day_dir / "day-summary.md"
    exists = path.exists()
    text = _read_text_file(path)
    if exists:
        state = "ready"
    elif events:
        state = "summary_pending"
    else:
        state = "empty"
    return {
        "state": state,
        "exists": exists,
        "path": str(path),
        "file_ref": _diary_relative_path(path) if exists else "",
        "excerpt": re.sub(r"\s+", " ", re.sub(r"(?s)^---.*?---", "", text).strip())[:500]
        if text
        else "",
    }


def _day_file_payload(day_dir: Path) -> dict[str, Any]:
    manifest_path = day_dir / "day-manifest.json"
    ledger_path = day_dir / "source-ledger.json"
    index_path = day_dir / "events-index.md"
    manifest = _read_json_file(manifest_path, {})
    ledger = _read_json_file(ledger_path, {})
    files = manifest.get("files") if isinstance(manifest, dict) else []
    sources = ledger.get("sources") if isinstance(ledger, dict) else []
    return {
        "day_folder": {
            "path": str(day_dir),
            "exists": day_dir.exists(),
        },
        "manifest": {
            "path": str(manifest_path),
            "exists": manifest_path.exists(),
            "file_count": len(files) if isinstance(files, list) else 0,
            "updated_at": manifest.get("updated_at_utc", "") if isinstance(manifest, dict) else "",
            "files": files if isinstance(files, list) else [],
        },
        "source_ledger": {
            "path": str(ledger_path),
            "exists": ledger_path.exists(),
            "source_count": len(sources) if isinstance(sources, list) else 0,
            "updated_at": ledger.get("updated_at_utc", "") if isinstance(ledger, dict) else "",
            "sources": sources[:24] if isinstance(sources, list) else [],
        },
        "events_index": {
            "path": str(index_path),
            "exists": index_path.exists(),
            "excerpt": _read_text_file(index_path, 1200),
        },
    }


def _next_action_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions = []
    for event in events:
        status = event.get("status") or ""
        kind = event.get("kind") or ""
        tags = event.get("tags") if isinstance(event.get("tags"), list) else []
        if status in {"blocked", "pending_review", "open"} and (
            "todo" in tags or "follow-up" in tags or kind in {"todo", "task", "action", "reminder"}
        ):
            actions.append(event)
    return actions[:12]


def _build_diary_day_payload(local_date: str, source_filter: str = "all") -> dict[str, Any]:
    clean_date = _validate_local_date(local_date)
    events, pin_hidden, source_counts = _visible_day_events(clean_date, source_filter=source_filter)
    day_dir = _diary_day_dir(clean_date)
    files = _day_file_payload(day_dir)
    status = "source_unavailable"
    if DIARY_ROOT.exists():
        status = "ready" if events or day_dir.exists() else "empty"
    summary = _summary_payload(day_dir, events)
    return {
        "status": status,
        "generated_at": _utc_now_iso(),
        "local_date": clean_date,
        "timezone": os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
        "source_filter": source_filter,
        "source_counts": source_counts,
        "pin_hidden_count": pin_hidden,
        "summary": summary,
        "events": events,
        "source_moments": events,
        "next_actions": _next_action_events(events),
        "files": files,
        "provenance": {
            "diary_root": str(DIARY_ROOT),
            "day_folder": files["day_folder"],
            "source_ledger": files["source_ledger"],
            "events_endpoint": f"/api/v1/personal/events?date_start={clean_date}&date_end={clean_date}",
            "read_model": "/api/v1/personal/diary-day",
        },
        "filters": {
            "available": ["all", "manual", "sources", "git", "imports", "work"],
            "active": source_filter,
        },
    }


@router.get("/diary-day")
async def get_diary_day(
    date: str | None = None,
    source_filter: str = "all",
) -> dict[str, Any]:
    return _build_diary_day_payload(_validate_local_date(date), source_filter=source_filter)


@router.post("/diary-day/minutes-project")
async def project_diary_day_minutes(body: DiaryMinutesProjectRequest) -> dict[str, Any]:
    return _project_hermes_minutes(body)


@router.post("/diary-day/browser-links-project")
async def project_diary_day_browser_links(body: DiaryBrowserLinksProjectRequest) -> dict[str, Any]:
    return await _project_browser_links(body)


def _update_source_ledger(
    module: Any,
    *,
    day_dir: Path,
    event_id: str,
    audit_id: str,
    file_ref: str,
    source_hash: str,
    actor: str,
    source_surface: str,
    now: str,
) -> None:
    ledger_path = day_dir / "source-ledger.json"
    owner = module.resolve_owner("xarta", "xarta")
    payload = _read_json_file(ledger_path, {})
    if not isinstance(payload, dict):
        payload = {}
    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
    entry = {
        "ledger_entry_id": f"manual-diary:{event_id}",
        "source_type": "manual",
        "source_ref": file_ref,
        "source_hash": source_hash,
        "event_id": event_id,
        "audit_id": audit_id,
        "file_ref": file_ref,
        "db_ref": f"personal_events:{event_id}",
        "audit_ref": f"personal_time_audit:{audit_id}",
        "actor": actor,
        "source_surface": source_surface,
        "created_at_utc": now,
    }
    sources = [
        item
        for item in sources
        if not isinstance(item, dict) or item.get("ledger_entry_id") != entry["ledger_entry_id"]
    ]
    sources.append(entry)
    payload.update(
        {
            "schema": payload.get("schema") or "xarta.diary.source_ledger.v1",
            "updated_at_utc": now,
            "generated_by": "blueprints-personal-api",
            "sources": sources,
        }
    )
    module.atomic_write_json(ledger_path, payload, owner)


STRUCTURED_MARKER_START = "<!-- xarta-diary:structured-files:start -->"
STRUCTURED_MARKER_END = "<!-- xarta-diary:structured-files:end -->"
HERMES_MINUTES_FILE = "hermes-minutes.json"
HERMES_MINUTES_SCHEMA = "xarta.diary.hermes_minutes_projection.v1"
BROWSER_LINKS_FILE = "browser-links-visits.json"
BROWSER_LINKS_SCHEMA = "xarta.diary.browser_links.v1"


def _hash_json_payload(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def _parse_minutes_datetime(value: Any, timezone_name: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(timezone_name))
    return parsed.astimezone(timezone.utc)


def _minutes_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return payload


def _minutes_datetime(event: dict[str, Any], timezone_name: str) -> datetime:
    payload = _minutes_payload(event)
    return _parse_minutes_datetime(payload.get("time") or event.get("created_at"), timezone_name)


def _minutes_local_date(event: dict[str, Any], timezone_name: str) -> str:
    zone = ZoneInfo(timezone_name)
    return _minutes_datetime(event, timezone_name).astimezone(zone).strftime("%Y-%m-%d")


def _compact_text(value: Any, limit: int) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())[:limit]


def _compact_list(value: Any, *, item_limit: int = 12, text_limit: int = 260) -> list[Any]:
    if not isinstance(value, list):
        return []
    compact: list[Any] = []
    for item in value[:item_limit]:
        if isinstance(item, dict):
            compact.append(
                {
                    str(key)[:80]: _compact_text(raw_value, text_limit)
                    if isinstance(raw_value, str)
                    else raw_value
                    for key, raw_value in item.items()
                    if key not in {"body", "raw_body", "transcript", "matrix_detail"}
                }
            )
        else:
            compact.append(_compact_text(item, text_limit))
    return compact


def _minutes_source_pointers(payload: dict[str, Any]) -> dict[str, Any]:
    pointers = (
        payload.get("source_pointers") if isinstance(payload.get("source_pointers"), dict) else {}
    )

    def list_field(key: str) -> list[str]:
        raw = pointers.get(key)
        if not isinstance(raw, list):
            return []
        values = []
        for item in raw[:12]:
            text = _compact_text(item, 260)
            if text and text not in values:
                values.append(text)
        return values

    return {
        "source_room_id": _compact_text(pointers.get("source_room_id"), 260),
        "matrix_event_ids": list_field("matrix_event_ids"),
        "tts_utterance_ids": list_field("tts_utterance_ids"),
        "wake_route_record_ids": list_field("wake_route_record_ids"),
    }


def _minutes_source_owner(payload: dict[str, Any], configured_owner: str) -> str:
    delivery = payload.get("delivery") if isinstance(payload.get("delivery"), dict) else {}
    return _compact_text(configured_owner or delivery.get("server_id") or "tb1", 40)


def _minutes_source_support(
    payload: dict[str, Any],
    pointers: dict[str, Any],
    source_owner: str,
) -> dict[str, str]:
    route = _compact_text(payload.get("route"), 80)
    support: dict[str, str] = {"source_owner": source_owner}
    if pointers.get("matrix_event_ids"):
        support["matrix_source_pointer"] = (
            "supported_by_tb1" if route == "direct_vps" and source_owner == "tb1" else "supported"
        )
    elif pointers.get("source_room_id"):
        support["matrix_source_pointer"] = "needs_design"
    else:
        support["matrix_source_pointer"] = "unavailable"
    support["tts_utterance_pointer"] = (
        "supported" if pointers.get("tts_utterance_ids") else "unavailable"
    )
    support["wake_route_record_pointer"] = (
        "supported" if pointers.get("wake_route_record_ids") else "unavailable"
    )
    return support


def _minutes_title(payload: dict[str, Any]) -> str:
    for key in ("operator_intent_summary", "assistant_action_summary", "result_summary"):
        text = _compact_text(payload.get(key), 90)
        if text:
            return f"Minutes: {text}"[:120]
    return "Hermes Minutes"


def _minutes_excerpt(payload: dict[str, Any]) -> str:
    parts = []
    operator = _compact_text(payload.get("operator_intent_summary"), 180)
    action = _compact_text(payload.get("assistant_action_summary"), 180)
    result = _compact_text(payload.get("result_summary"), 220)
    if operator:
        parts.append(f"Operator: {operator}")
    if action:
        parts.append(f"Action: {action}")
    if result:
        parts.append(f"Result: {result}")
    return " | ".join(parts)[:500] if parts else "Compact Hermes Minutes summary."


def _minutes_projection_entry(
    event: dict[str, Any],
    *,
    local_date: str,
    timezone_name: str,
    source_owner: str,
) -> dict[str, Any]:
    payload = _minutes_payload(event)
    moment = _minutes_datetime(event, timezone_name)
    local_moment = moment.astimezone(ZoneInfo(timezone_name))
    record_hash = _hash_json_payload(
        {
            "schema": event.get("schema"),
            "event_kind": event.get("event_kind"),
            "conversation_key": event.get("conversation_key"),
            "created_at": event.get("created_at"),
            "payload": payload,
        }
    )
    digest = record_hash.removeprefix("sha256:")[:20]
    event_id = f"minutes-{local_date}-{digest}"
    pointers = _minutes_source_pointers(payload)
    owner = _minutes_source_owner(payload, source_owner)
    return {
        "event_id": event_id,
        "minutes_id": event_id,
        "source_record_hash": record_hash,
        "time": moment.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "local_time": local_moment.strftime("%H:%M"),
        "conversation_key": _compact_text(event.get("conversation_key"), 260),
        "route": _compact_text(payload.get("route"), 80),
        "route_status": _compact_text(payload.get("route_status"), 80),
        "route_profile": _compact_text(payload.get("route_profile"), 120),
        "operator_intent_summary": _compact_text(payload.get("operator_intent_summary"), 420),
        "assistant_action_summary": _compact_text(payload.get("assistant_action_summary"), 320),
        "result_summary": _compact_text(payload.get("result_summary"), 360),
        "open_question": _compact_text(payload.get("open_question"), 240),
        "entities": _compact_list(payload.get("entities"), item_limit=12, text_limit=180),
        "problems": _compact_list(payload.get("problems"), item_limit=8, text_limit=220),
        "followup_affordances": _compact_list(
            payload.get("followup_affordances"), item_limit=8, text_limit=220
        ),
        "source_pointers": pointers,
        "source_support": _minutes_source_support(payload, pointers, owner),
        "source_owner": owner,
        "source_detail_policy": _compact_text(payload.get("source_detail_policy"), 260),
        "confidence": payload.get("confidence")
        if isinstance(payload.get("confidence"), (int, float))
        else None,
    }


def _select_minutes_entries(
    *,
    local_date: str,
    timezone_name: str,
    limit: int,
    ttl_seconds: float | None,
    source_owner: str,
) -> tuple[str, Path, str, list[dict[str, Any]]]:
    source_path = hermes_minutes.minutes_index_path()
    if not source_path.exists():
        return "source_unavailable", source_path, "", []
    safe_limit = max(1, min(int(limit or 200), 500))
    ttl = ttl_seconds if ttl_seconds is not None else 36 * 60 * 60
    raw_events = hermes_minutes.read_recent_minutes(
        event_kind="turn_summary",
        limit=safe_limit,
        ttl_seconds=ttl,
    )
    entries = [
        _minutes_projection_entry(
            event,
            local_date=local_date,
            timezone_name=timezone_name,
            source_owner=source_owner,
        )
        for event in raw_events
        if _minutes_local_date(event, timezone_name) == local_date
    ]
    deduped = {entry["source_record_hash"]: entry for entry in entries}
    ordered = sorted(deduped.values(), key=lambda item: (item["time"], item["event_id"]))
    source_hash = ""
    with suppress(OSError):
        source_hash = f"sha256:{hashlib.sha256(source_path.read_bytes()).hexdigest()}"
    return "ok", source_path, source_hash, ordered


def _upsert_index_structured_file(
    module: Any,
    day_dir: Path,
    owner: Any,
    *,
    filename: str = HERMES_MINUTES_FILE,
    label: str = "Hermes Minutes projection",
) -> None:
    index_path = day_dir / "events-index.md"
    if not index_path.exists():
        return
    text = index_path.read_text(encoding="utf-8")
    line = f"- `{filename}` - {label}"
    if line in text:
        return
    if STRUCTURED_MARKER_START in text and STRUCTURED_MARKER_END in text:
        insert_at = text.index(STRUCTURED_MARKER_END)
        before = text[:insert_at]
        after = text[insert_at:]
        if before and not before.endswith("\n"):
            before += "\n"
        updated = before + line + "\n" + after
    else:
        updated = text.rstrip() + "\n\n" + line + "\n"
    module.atomic_write_text(index_path, updated, owner)


def _write_minutes_projection_artifacts(
    module: Any,
    *,
    local_date: str,
    timezone_name: str,
    source_status: str,
    source_path: Path,
    source_index_hash: str,
    entries: list[dict[str, Any]],
    source_owner: str,
    now: str,
) -> dict[str, Any]:
    owner = module.resolve_owner("xarta", "xarta")
    day = module.diary_day(root=DIARY_ROOT, local_date=local_date, timezone_name=timezone_name)
    module.init_day(
        root=DIARY_ROOT, local_date=local_date, timezone_name=timezone_name, owner=owner
    )
    projection_path = day.day_dir / HERMES_MINUTES_FILE
    ledger_refs = [f"hermes-minutes:{entry['event_id']}" for entry in entries]
    payload = module.hermes_minutes_projection_template(
        day, source_owner=source_owner or "tb1", generated_by="blueprints-personal-api"
    )
    payload.update(
        {
            "updated_at_utc": now,
            "status": source_status,
            "source_index": {
                "path": str(source_path),
                "exists": source_path.exists(),
                "content_hash": source_index_hash,
            },
            "entry_count": len(entries),
            "entries": entries,
            "source_ledger_refs": ledger_refs,
            "projected_event_ids": [entry["event_id"] for entry in entries],
        }
    )
    module.atomic_write_json(projection_path, payload, owner)
    _upsert_index_structured_file(module, day.day_dir, owner)
    _update_minutes_source_ledger(
        module,
        day_dir=day.day_dir,
        file_ref=_diary_relative_path(projection_path),
        source_path=source_path,
        source_index_hash=source_index_hash,
        source_status=source_status,
        entries=entries,
        now=now,
    )
    module.write_manifest(day, owner)
    return {
        "day": day,
        "projection_path": projection_path,
        "file_ref": _diary_relative_path(projection_path),
        "source_hash": module.sha256_file(projection_path),
    }


def _update_minutes_source_ledger(
    module: Any,
    *,
    day_dir: Path,
    file_ref: str,
    source_path: Path,
    source_index_hash: str,
    source_status: str,
    entries: list[dict[str, Any]],
    now: str,
) -> None:
    ledger_path = day_dir / "source-ledger.json"
    owner = module.resolve_owner("xarta", "xarta")
    payload = _read_json_file(ledger_path, {})
    if not isinstance(payload, dict):
        payload = {}
    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
    sources = [
        item
        for item in sources
        if not isinstance(item, dict)
        or not str(item.get("ledger_entry_id") or "").startswith("hermes-minutes:")
    ]
    if source_status != "ok":
        sources.append(
            {
                "ledger_entry_id": "hermes-minutes:source-unavailable",
                "source_type": "hermes-minutes",
                "source_ref": str(source_path),
                "source_status": source_status,
                "file_ref": file_ref,
                "created_at_utc": now,
                "provenance_state": "missing_source",
            }
        )
    for entry in entries:
        pointers = (
            entry.get("source_pointers") if isinstance(entry.get("source_pointers"), dict) else {}
        )
        sources.append(
            {
                "ledger_entry_id": f"hermes-minutes:{entry['event_id']}",
                "source_type": "hermes-minutes",
                "source_ref": str(source_path),
                "source_hash": entry["source_record_hash"],
                "source_index_hash": source_index_hash,
                "event_id": entry["event_id"],
                "file_ref": file_ref,
                "db_ref": f"personal_events:{entry['event_id']}",
                "matrix_event_ids": pointers.get("matrix_event_ids") or [],
                "tts_utterance_ids": pointers.get("tts_utterance_ids") or [],
                "wake_route_record_ids": pointers.get("wake_route_record_ids") or [],
                "conversation_key": entry.get("conversation_key") or "",
                "route": entry.get("route") or "",
                "route_profile": entry.get("route_profile") or "",
                "source_owner": entry.get("source_owner") or "",
                "source_support": entry.get("source_support") or {},
                "created_at_utc": now,
                "source_created_at": entry.get("time") or "",
            }
        )
    payload.update(
        {
            "schema": payload.get("schema") or "xarta.diary.source_ledger.v1",
            "updated_at_utc": now,
            "generated_by": "blueprints-personal-api",
            "sources": sources,
        }
    )
    module.atomic_write_json(ledger_path, payload, owner)


def _upsert_hermes_minutes_source(
    conn: Any,
    *,
    source_status: str,
    source_path: Path,
    source_index_hash: str,
    local_date: str,
    entry_count: int,
    now: str,
) -> Any:
    conn.execute(
        """
        INSERT INTO personal_sources (
            source_id, source_type, label, status, last_seen_at, health_json,
            provenance_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            status=excluded.status,
            last_seen_at=excluded.last_seen_at,
            health_json=excluded.health_json,
            provenance_json=excluded.provenance_json,
            updated_at=excluded.updated_at
        """,
        (
            "hermes-minutes",
            "hermes-minutes",
            "Hermes Minutes",
            source_status,
            now,
            json.dumps(
                {
                    "source_index_path": str(source_path),
                    "source_index_hash": source_index_hash,
                    "local_date": local_date,
                    "entry_count": entry_count,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            json.dumps({"reader": "hermes_minutes.read_recent_minutes"}, ensure_ascii=True),
            now,
            now,
        ),
    )
    return conn.execute(
        "SELECT * FROM personal_sources WHERE source_id='hermes-minutes'"
    ).fetchone()


def _upsert_minutes_unavailable_event(
    conn: Any,
    *,
    local_date: str,
    timezone_name: str,
    file_ref: str,
    source_path: Path,
    now: str,
) -> Any:
    event_id = f"minutes-{local_date}-source-unavailable"
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, local_date, timezone, status, privacy_level,
            tags_json, file_refs_json, db_refs_json, provenance_json, projection_state,
            provenance_state, last_rendered_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            source_ref=excluded.source_ref,
            title=excluded.title,
            body_excerpt=excluded.body_excerpt,
            content_projection=excluded.content_projection,
            start_at=excluded.start_at,
            status=excluded.status,
            file_refs_json=excluded.file_refs_json,
            provenance_json=excluded.provenance_json,
            projection_state=excluded.projection_state,
            provenance_state=excluded.provenance_state,
            last_rendered_at=excluded.last_rendered_at,
            updated_at=excluded.updated_at
        """,
        (
            event_id,
            "hermes-minutes",
            str(source_path),
            "",
            "source-status",
            "Hermes Minutes source unavailable",
            "The configured compact Minutes index was unavailable during projection.",
            "",
            now,
            local_date,
            timezone_name,
            "source_unavailable",
            "normal",
            json.dumps(["diary", "hermes-minutes", "source-unavailable"], ensure_ascii=True),
            json.dumps([file_ref], ensure_ascii=True),
            json.dumps([], ensure_ascii=True),
            json.dumps(
                {
                    "source_index_path": str(source_path),
                    "projection_file": file_ref,
                    "source_status": "source_unavailable",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "hot",
            "missing_source",
            now,
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()


def _upsert_minutes_event(
    conn: Any,
    *,
    entry: dict[str, Any],
    local_date: str,
    timezone_name: str,
    file_ref: str,
    now: str,
) -> Any:
    event_id = entry["event_id"]
    provenance = {
        "projection_file": file_ref,
        "source_pointers": entry.get("source_pointers") or {},
        "source_support": entry.get("source_support") or {},
        "source_owner": entry.get("source_owner") or "",
        "conversation_key": entry.get("conversation_key") or "",
        "route": entry.get("route") or "",
        "route_status": entry.get("route_status") or "",
        "route_profile": entry.get("route_profile") or "",
        "source_detail_policy": entry.get("source_detail_policy") or "",
    }
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, local_date, timezone, status, privacy_level,
            tags_json, entities_json, file_refs_json, db_refs_json, provenance_json,
            projection_state, provenance_state, last_rendered_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            source_ref=excluded.source_ref,
            source_hash=excluded.source_hash,
            kind=excluded.kind,
            title=excluded.title,
            body_excerpt=excluded.body_excerpt,
            content_projection=excluded.content_projection,
            start_at=excluded.start_at,
            local_date=excluded.local_date,
            timezone=excluded.timezone,
            status=excluded.status,
            privacy_level=excluded.privacy_level,
            tags_json=excluded.tags_json,
            entities_json=excluded.entities_json,
            file_refs_json=excluded.file_refs_json,
            db_refs_json=excluded.db_refs_json,
            provenance_json=excluded.provenance_json,
            projection_state=excluded.projection_state,
            provenance_state=excluded.provenance_state,
            last_rendered_at=excluded.last_rendered_at,
            updated_at=excluded.updated_at
        """,
        (
            event_id,
            "hermes-minutes",
            entry["source_record_hash"],
            entry["source_record_hash"],
            "hermes-minutes",
            _minutes_title(entry),
            _minutes_excerpt(entry),
            json.dumps(entry, ensure_ascii=True, sort_keys=True),
            entry.get("time") or now,
            local_date,
            timezone_name,
            "open",
            "normal",
            json.dumps(["diary", "hermes-minutes", "minutes-projection"], ensure_ascii=True),
            json.dumps(entry.get("entities") or [], ensure_ascii=True),
            json.dumps([file_ref], ensure_ascii=True),
            json.dumps([], ensure_ascii=True),
            json.dumps(provenance, ensure_ascii=True, sort_keys=True),
            "hot",
            "linked",
            now,
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()


def _project_hermes_minutes(
    body: DiaryMinutesProjectRequest,
) -> dict[str, Any]:
    local_date = _validate_local_date(body.local_date)
    timezone_name = _clean_short_text(
        body.timezone or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
        "Europe/London",
        limit=80,
    )
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise HTTPException(400, "timezone is invalid") from exc
    now = _utc_now_iso()
    source_status, source_path, source_index_hash, entries = _select_minutes_entries(
        local_date=local_date,
        timezone_name=timezone_name,
        limit=body.limit,
        ttl_seconds=body.ttl_seconds,
        source_owner=body.source_owner,
    )
    module = _load_xarta_diary_module()
    artifacts = _write_minutes_projection_artifacts(
        module,
        local_date=local_date,
        timezone_name=timezone_name,
        source_status=source_status,
        source_path=source_path,
        source_index_hash=source_index_hash,
        entries=entries,
        source_owner=body.source_owner,
        now=now,
    )
    file_ref = artifacts["file_ref"]
    audit_id = f"audit-{uuid.uuid4().hex}"
    actor = _clean_short_text(body.actor, "blueprints-api")
    source_surface = _clean_short_text(body.source_surface, "minutes-projection")
    request_id = _clean_short_text(
        body.request_id, f"minutes-project-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    existing_ids = {entry["event_id"] for entry in entries}
    with get_conn() as conn:
        existing_rows = conn.execute(
            """
            SELECT event_id FROM personal_events
            WHERE local_date=? AND source_type='hermes-minutes'
            """,
            (local_date,),
        ).fetchall()
        previously_present = {row["event_id"] for row in existing_rows}
        source_row = _upsert_hermes_minutes_source(
            conn,
            source_status=source_status,
            source_path=source_path,
            source_index_hash=source_index_hash,
            local_date=local_date,
            entry_count=len(entries),
            now=now,
        )
        event_rows = []
        if source_status != "ok":
            event_rows.append(
                _upsert_minutes_unavailable_event(
                    conn,
                    local_date=local_date,
                    timezone_name=timezone_name,
                    file_ref=file_ref,
                    source_path=source_path,
                    now=now,
                )
            )
        else:
            conn.execute(
                """
                DELETE FROM personal_events
                WHERE event_id=? AND source_type='hermes-minutes'
                """,
                (f"minutes-{local_date}-source-unavailable",),
            )
            for entry in entries:
                event_rows.append(
                    _upsert_minutes_event(
                        conn,
                        entry=entry,
                        local_date=local_date,
                        timezone_name=timezone_name,
                        file_ref=file_ref,
                        now=now,
                    )
                )
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action="project_hermes_minutes",
            target_ref=f"diary-day:{local_date}",
            file_ref=file_ref,
            db_ref="personal_sources:hermes-minutes",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result=source_status,
            source_hash=artifacts["source_hash"],
            metadata={
                "local_date": local_date,
                "source_index_path": str(source_path),
                "source_index_hash": source_index_hash,
                "projected_event_count": len(entries),
                "skipped_existing_event_count": len(existing_ids & previously_present),
            },
        )
        gen = increment_gen(conn, "personal-minutes-projection")
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_sources", "hermes-minutes", dict(source_row), gen
        )
        for event_row in event_rows:
            enqueue_for_all_peers(
                conn, "UPDATE", "personal_events", event_row["event_id"], dict(event_row), gen
            )
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "source_available": source_status == "ok",
        "status": source_status,
        "local_date": local_date,
        "timezone": timezone_name,
        "projection": {
            "file_ref": file_ref,
            "source_hash": artifacts["source_hash"],
            "entry_count": len(entries),
            "projected_event_ids": [entry["event_id"] for entry in entries],
            "skipped_existing_event_count": len(existing_ids & previously_present),
        },
        "source": {
            "path": str(source_path),
            "exists": source_path.exists(),
            "content_hash": source_index_hash,
        },
        "audit": {"audit_id": audit_id, "result": source_status},
        "day": _build_diary_day_payload(local_date),
    }


def _browser_date_candidates(local_date: str) -> list[str]:
    base = datetime.strptime(local_date, "%Y-%m-%d")
    return sorted({(base + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in (-1, 0, 1)})


def _parse_browser_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    with suppress(ValueError):
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        with suppress(ValueError):
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
    return None


def _browser_local_datetime(value: Any, timezone_name: str) -> datetime | None:
    parsed = _parse_browser_datetime(value)
    if parsed is None:
        return None
    return parsed.astimezone(ZoneInfo(timezone_name))


def _browser_domain(url: str) -> str:
    with suppress(Exception):
        return urlparse(url).netloc.lower()
    return ""


def _browser_tags(raw: Any) -> list[str]:
    parsed = _json_value(str(raw or "[]"), [])
    if not isinstance(parsed, list):
        return []
    tags = []
    for item in parsed[:24]:
        text = _compact_text(item, 80)
        if text and text not in tags:
            tags.append(text)
    return tags


def _browser_url_hash(value: Any) -> str:
    text = _compact_text(value, 2000)
    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}" if text else ""


def _row_get(row: Any, key: str, default: Any = "") -> Any:
    with suppress(Exception):
        value = row[key]
        return default if value is None else value
    return default


def _select_browser_link_entries(
    conn: Any,
    *,
    local_date: str,
    timezone_name: str,
    limit: int,
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit or 1000), 5000))
    date_candidates = _browser_date_candidates(local_date)
    placeholders = ",".join("?" for _ in date_candidates)
    try:
        bookmark_count = int(conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0])
        visit_count = int(conn.execute("SELECT COUNT(*) FROM visits").fetchone()[0])
        visit_event_count = int(conn.execute("SELECT COUNT(*) FROM visit_events").fetchone()[0])
        event_rows = conn.execute(
            f"""
            SELECT
                e.event_id AS visit_event_id,
                e.normalized_url AS event_normalized_url,
                e.visited_at AS event_visited_at,
                e.dwell_seconds AS event_dwell_seconds,
                v.visit_id,
                v.url,
                v.normalized_url,
                v.domain,
                v.title,
                v.source,
                v.dwell_seconds AS visit_dwell_seconds,
                v.bookmark_id,
                v.visited_at AS visit_last_seen_at,
                v.visit_count
            FROM visit_events e
            LEFT JOIN visits v ON v.normalized_url = e.normalized_url
            WHERE substr(e.visited_at, 1, 10) IN ({placeholders})
            ORDER BY e.visited_at ASC, e.event_id ASC
            LIMIT ?
            """,
            (*date_candidates, safe_limit),
        ).fetchall()
        bookmark_rows = conn.execute(
            f"""
            SELECT bookmark_id, url, normalized_url, title, tags_json, folder, source,
                   archived, created_at, updated_at
            FROM bookmarks
            WHERE substr(created_at, 1, 10) IN ({placeholders})
            ORDER BY created_at ASC, bookmark_id ASC
            LIMIT ?
            """,
            (*date_candidates, safe_limit),
        ).fetchall()
    except sqlite3.Error as exc:
        return {
            "status": "source_unavailable",
            "health": {
                "status": "source_unavailable",
                "sqlite": "error",
                "sqlite_error": _compact_text(exc, 180),
                "bookmark_count": 0,
                "visit_count": 0,
                "visit_event_count": 0,
            },
            "visits": [],
            "bookmarks": [],
            "initiation": {},
            "limited": False,
        }

    visits_by_url: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        local_moment = _browser_local_datetime(_row_get(row, "event_visited_at"), timezone_name)
        if local_moment is None or local_moment.strftime("%Y-%m-%d") != local_date:
            continue
        normalized_url = _compact_text(
            _row_get(row, "normalized_url") or _row_get(row, "event_normalized_url"), 2000
        )
        if not normalized_url:
            continue
        url = _compact_text(_row_get(row, "url") or normalized_url, 2000)
        bucket = visits_by_url.setdefault(
            normalized_url,
            {
                "normalized_url": normalized_url,
                "url": url,
                "url_hash": _browser_url_hash(normalized_url),
                "domain": _compact_text(_row_get(row, "domain") or _browser_domain(url), 180),
                "title": _compact_text(_row_get(row, "title"), 180),
                "source": _compact_text(_row_get(row, "source") or "visit-recorder", 80),
                "visit_ids": [],
                "visit_event_ids": [],
                "bookmark_ids": [],
                "visited_at_values": [],
                "local_times": [],
                "dwell_seconds_total": 0,
                "source_hashes": [],
            },
        )
        visit_id = _compact_text(_row_get(row, "visit_id"), 120)
        if visit_id and visit_id not in bucket["visit_ids"]:
            bucket["visit_ids"].append(visit_id)
        visit_event_id = _compact_text(_row_get(row, "visit_event_id"), 120)
        if visit_event_id and visit_event_id not in bucket["visit_event_ids"]:
            bucket["visit_event_ids"].append(visit_event_id)
        bookmark_id = _compact_text(_row_get(row, "bookmark_id"), 120)
        if bookmark_id and bookmark_id not in bucket["bookmark_ids"]:
            bucket["bookmark_ids"].append(bookmark_id)
        visited_at = local_moment.astimezone(timezone.utc).replace(microsecond=0)
        visited_at_text = visited_at.isoformat().replace("+00:00", "Z")
        bucket["visited_at_values"].append(visited_at_text)
        bucket["local_times"].append(local_moment.strftime("%H:%M"))
        dwell = _row_get(row, "event_dwell_seconds")
        if isinstance(dwell, int):
            bucket["dwell_seconds_total"] += max(0, dwell)
        elif str(dwell or "").isdigit():
            bucket["dwell_seconds_total"] += int(dwell)
        event_hash = _hash_json_payload(
            {
                "visit_event_id": visit_event_id,
                "normalized_url": normalized_url,
                "visited_at": visited_at_text,
                "dwell_seconds": dwell,
            }
        )
        bucket["source_hashes"].append(event_hash)

    visits: list[dict[str, Any]] = []
    for bucket in visits_by_url.values():
        source_record_hash = _hash_json_payload(
            {
                "normalized_url": bucket["normalized_url"],
                "visit_event_ids": bucket["visit_event_ids"],
                "source_hashes": bucket["source_hashes"],
            }
        )
        digest = source_record_hash.removeprefix("sha256:")[:20]
        first_at = min(bucket["visited_at_values"]) if bucket["visited_at_values"] else ""
        last_at = max(bucket["visited_at_values"]) if bucket["visited_at_values"] else ""
        visits.append(
            {
                "visit_projection_id": f"browser-links-visit-{local_date}-{digest}",
                "source_record_hash": source_record_hash,
                "url": bucket["url"],
                "normalized_url": bucket["normalized_url"],
                "url_hash": bucket["url_hash"],
                "domain": bucket["domain"],
                "title": bucket["title"],
                "source": bucket["source"],
                "visit_count": len(bucket["visit_event_ids"]),
                "first_visited_at": first_at,
                "last_visited_at": last_at,
                "first_local_time": min(bucket["local_times"]) if bucket["local_times"] else "",
                "last_local_time": max(bucket["local_times"]) if bucket["local_times"] else "",
                "dwell_seconds_total": bucket["dwell_seconds_total"],
                "visit_ids": bucket["visit_ids"][:24],
                "visit_event_ids": bucket["visit_event_ids"][:80],
                "bookmark_ids": bucket["bookmark_ids"][:12],
                "source_hashes": bucket["source_hashes"][:80],
            }
        )
    visits.sort(key=lambda item: (item["first_visited_at"], item["domain"], item["url_hash"]))

    bookmarks: list[dict[str, Any]] = []
    for row in bookmark_rows:
        local_moment = _browser_local_datetime(_row_get(row, "created_at"), timezone_name)
        if local_moment is None or local_moment.strftime("%Y-%m-%d") != local_date:
            continue
        normalized_url = _compact_text(
            _row_get(row, "normalized_url") or _row_get(row, "url"), 2000
        )
        bookmark_id = _compact_text(_row_get(row, "bookmark_id"), 120)
        tags = _browser_tags(_row_get(row, "tags_json"))
        source_record_hash = _hash_json_payload(
            {
                "bookmark_id": bookmark_id,
                "normalized_url": normalized_url,
                "created_at": _row_get(row, "created_at"),
                "updated_at": _row_get(row, "updated_at"),
                "tags": tags,
                "folder": _row_get(row, "folder"),
                "source": _row_get(row, "source"),
            }
        )
        digest = source_record_hash.removeprefix("sha256:")[:20]
        bookmarks.append(
            {
                "bookmark_projection_id": f"browser-links-bookmark-{local_date}-{digest}",
                "source_record_hash": source_record_hash,
                "bookmark_id": bookmark_id,
                "url": _compact_text(_row_get(row, "url") or normalized_url, 2000),
                "normalized_url": normalized_url,
                "url_hash": _browser_url_hash(normalized_url),
                "domain": _browser_domain(_row_get(row, "url") or normalized_url),
                "title": _compact_text(_row_get(row, "title"), 180),
                "folder": _compact_text(_row_get(row, "folder"), 180),
                "tags": tags,
                "source": _compact_text(_row_get(row, "source") or "manual", 80),
                "archived": bool(_row_get(row, "archived", 0)),
                "created_at": local_moment.astimezone(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
                "local_time": local_moment.strftime("%H:%M"),
                "updated_at": _compact_text(_row_get(row, "updated_at"), 80),
            }
        )

    initiation = _browser_links_initiation_summary(conn, local_date=local_date, now=_utc_now_iso())
    return {
        "status": "ok",
        "health": {
            "status": "ok",
            "sqlite": "ok",
            "bookmark_count": bookmark_count,
            "visit_count": visit_count,
            "visit_event_count": visit_event_count,
        },
        "visits": visits,
        "bookmarks": bookmarks,
        "initiation": initiation,
        "limited": len(event_rows) >= safe_limit or len(bookmark_rows) >= safe_limit,
    }


def _browser_links_initiation_summary(conn: Any, *, local_date: str, now: str) -> dict[str, Any]:
    try:
        bookmark_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM bookmarks WHERE substr(created_at, 1, 10) < ?",
                (local_date,),
            ).fetchone()[0]
        )
        visit_event_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM visit_events WHERE substr(visited_at, 1, 10) < ?",
                (local_date,),
            ).fetchone()[0]
        )
        bookmark_range = conn.execute(
            """
            SELECT MIN(created_at) AS first_at, MAX(created_at) AS last_at
            FROM bookmarks
            WHERE substr(created_at, 1, 10) < ?
            """,
            (local_date,),
        ).fetchone()
        visit_range = conn.execute(
            """
            SELECT MIN(visited_at) AS first_at, MAX(visited_at) AS last_at
            FROM visit_events
            WHERE substr(visited_at, 1, 10) < ?
            """,
            (local_date,),
        ).fetchone()
        domain_rows = conn.execute(
            """
            SELECT COALESCE(NULLIF(v.domain, ''), 'unknown') AS domain, COUNT(*) AS count
            FROM visit_events e
            LEFT JOIN visits v ON v.normalized_url = e.normalized_url
            WHERE substr(e.visited_at, 1, 10) < ?
            GROUP BY domain
            ORDER BY count DESC, domain ASC
            LIMIT 20
            """,
            (local_date,),
        ).fetchall()
        bookmark_samples = conn.execute(
            """
            SELECT bookmark_id, normalized_url, created_at, updated_at
            FROM bookmarks
            WHERE substr(created_at, 1, 10) < ?
            ORDER BY created_at DESC, bookmark_id ASC
            LIMIT 40
            """,
            (local_date,),
        ).fetchall()
    except sqlite3.Error:
        return {}
    samples = []
    for row in bookmark_samples:
        bookmark_id = _compact_text(_row_get(row, "bookmark_id"), 120)
        normalized_url = _compact_text(_row_get(row, "normalized_url"), 2000)
        samples.append(
            {
                "bookmark_id": bookmark_id,
                "url_hash": _browser_url_hash(normalized_url),
                "created_at": _compact_text(_row_get(row, "created_at"), 80),
                "updated_at": _compact_text(_row_get(row, "updated_at"), 80),
                "source_record_hash": _hash_json_payload(
                    {
                        "bookmark_id": bookmark_id,
                        "normalized_url": normalized_url,
                        "created_at": _row_get(row, "created_at"),
                        "updated_at": _row_get(row, "updated_at"),
                    }
                ),
            }
        )
    return {
        "schema": "xarta.diary.browser_links_initiation_summary.v1",
        "generated_at_utc": now,
        "run_date": local_date,
        "source": "browser-links",
        "policy": (
            "Historical Browser Links records stay under _initiation with original source "
            "timestamps; normal diary days only receive records whose source timestamp maps "
            "to that day."
        ),
        "bookmarks_existing_count": bookmark_count,
        "visit_events_existing_count": visit_event_count,
        "bookmark_source_range": {
            "first_at": _compact_text(_row_get(bookmark_range, "first_at"), 80),
            "last_at": _compact_text(_row_get(bookmark_range, "last_at"), 80),
        },
        "visit_source_range": {
            "first_at": _compact_text(_row_get(visit_range, "first_at"), 80),
            "last_at": _compact_text(_row_get(visit_range, "last_at"), 80),
        },
        "top_visit_domains": [
            {"domain": _compact_text(_row_get(row, "domain"), 180), "count": int(row["count"])}
            for row in domain_rows
        ],
        "bookmark_source_samples": samples,
    }


async def _browser_links_search_health(sqlite_health: dict[str, Any]) -> dict[str, Any]:
    health = dict(sqlite_health)
    seekdb_ok = "ok"
    seekdb_err = ""
    seekdb_indexed = 0
    visits_indexed = 0
    try:
        from .seekdb import seekdb_counts_async

        counts = await seekdb_counts_async(timeout=2.0)
        seekdb_indexed = int(counts.get("bookmarks_indexed") or 0)
        visits_indexed = int(counts.get("visits_indexed") or 0)
    except Exception as exc:
        from .seekdb import short_seekdb_error

        seekdb_ok = "error"
        seekdb_err = short_seekdb_error(exc)

    emb_ok = "ok"
    emb_err = ""
    try:
        from .ai_client import embed

        vec = await embed("browser-links", ["health check"])
        if not vec or len(vec[0]) != 2048:
            emb_ok = "error"
            emb_err = "unexpected embedding dimensions"
    except Exception as exc:
        from .seekdb import short_seekdb_error

        emb_ok = "error"
        emb_err = short_seekdb_error(exc)

    bookmark_count = int(health.get("bookmark_count") or 0)
    visit_count = int(health.get("visit_count") or 0)
    health.update(
        {
            "seekdb": seekdb_ok,
            "seekdb_error": seekdb_err,
            "embedding": emb_ok,
            "embedding_error": emb_err,
            "seekdb_indexed": seekdb_indexed,
            "seekdb_stale": max(0, bookmark_count - seekdb_indexed),
            "seekdb_visits_indexed": visits_indexed,
            "seekdb_visits_stale": max(0, visit_count - visits_indexed),
        }
    )
    health["status"] = "ok" if seekdb_ok == "ok" and emb_ok == "ok" else "degraded"
    return health


def _browser_links_summary(
    visits: list[dict[str, Any]], bookmarks: list[dict[str, Any]]
) -> dict[str, Any]:
    domain_counts: dict[str, int] = {}
    for visit in visits:
        domain = _compact_text(visit.get("domain") or "unknown", 180)
        domain_counts[domain] = domain_counts.get(domain, 0) + int(visit.get("visit_count") or 0)
    top_domains = [
        {"domain": domain, "visit_count": count}
        for domain, count in sorted(domain_counts.items(), key=lambda item: (-item[1], item[0]))[
            :12
        ]
    ]
    return {
        "visit_event_count": sum(int(visit.get("visit_count") or 0) for visit in visits),
        "visited_page_count": len(visits),
        "visited_domain_count": len(domain_counts),
        "bookmark_count": len(bookmarks),
        "top_domains": top_domains,
        "first_visit_at": min(
            (visit.get("first_visited_at") or "" for visit in visits), default=""
        ),
        "last_visit_at": max((visit.get("last_visited_at") or "" for visit in visits), default=""),
        "first_bookmark_at": min((item.get("created_at") or "" for item in bookmarks), default=""),
        "last_bookmark_at": max((item.get("created_at") or "" for item in bookmarks), default=""),
    }


def _ensure_owned_dir(path: Path, owner: Any) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with suppress(OSError):
        path.chmod(0o750)
    if getattr(owner, "enabled", False):
        with suppress(OSError):
            os.chown(path, owner.uid, owner.gid)


def _write_browser_links_initiation_files(
    module: Any,
    *,
    local_date: str,
    timezone_name: str,
    initiation: dict[str, Any],
    now: str,
    owner: Any,
) -> dict[str, Any]:
    if not initiation:
        return {}
    initiation_dir = DIARY_ROOT / "_initiation" / local_date / "browser-links"
    _ensure_owned_dir(initiation_dir, owner)
    common = {
        "generated_at_utc": now,
        "run_date": local_date,
        "timezone": timezone_name,
        "source": "browser-links",
        "policy": initiation["policy"],
    }
    visits_path = initiation_dir / "visits-existing-summary.json"
    bookmarks_path = initiation_dir / "bookmarks-existing.json"
    index_path = initiation_dir / "initiation-index.md"
    module.atomic_write_json(
        visits_path,
        {
            **common,
            "schema": "xarta.diary.browser_links_initiation_visits.v1",
            "visit_events_existing_count": initiation["visit_events_existing_count"],
            "visit_source_range": initiation["visit_source_range"],
            "top_visit_domains": initiation["top_visit_domains"],
        },
        owner,
    )
    module.atomic_write_json(
        bookmarks_path,
        {
            **common,
            "schema": "xarta.diary.browser_links_initiation_bookmarks.v1",
            "bookmarks_existing_count": initiation["bookmarks_existing_count"],
            "bookmark_source_range": initiation["bookmark_source_range"],
            "bookmark_source_samples": initiation["bookmark_source_samples"],
        },
        owner,
    )
    index = (
        "---\n"
        "schema: xarta.diary.browser_links_initiation_index.v1\n"
        f"run_date: {local_date}\n"
        f"timezone: {timezone_name}\n"
        "source: browser-links\n"
        "---\n\n"
        "# Browser Links Initiation Backfill\n\n"
        f"{initiation['policy']}\n\n"
        "## Files\n\n"
        "- `visits-existing-summary.json` - compact historical visit counts and source ranges\n"
        "- `bookmarks-existing.json` - compact historical bookmark ids and source hashes\n"
    )
    module.atomic_write_text(index_path, index, owner)
    return {
        "initiation_dir": _diary_relative_path(initiation_dir),
        "files": [
            _diary_relative_path(index_path),
            _diary_relative_path(visits_path),
            _diary_relative_path(bookmarks_path),
        ],
    }


def _update_browser_links_source_ledger(
    module: Any,
    *,
    day_dir: Path,
    file_ref: str,
    source_status: str,
    health: dict[str, Any],
    visits: list[dict[str, Any]],
    bookmarks: list[dict[str, Any]],
    now: str,
) -> list[str]:
    ledger_path = day_dir / "source-ledger.json"
    owner = module.resolve_owner("xarta", "xarta")
    payload = _read_json_file(ledger_path, {})
    if not isinstance(payload, dict):
        payload = {}
    sources = payload.get("sources") if isinstance(payload.get("sources"), list) else []
    sources = [
        item
        for item in sources
        if not isinstance(item, dict)
        or not str(item.get("ledger_entry_id") or "").startswith("browser-links:")
    ]
    ledger_refs: list[str] = []
    if source_status == "source_unavailable":
        entry = {
            "ledger_entry_id": "browser-links:source-unavailable",
            "source_type": "browser-links",
            "source_status": source_status,
            "file_ref": file_ref,
            "created_at_utc": now,
            "health": health,
            "provenance_state": "missing_source",
        }
        sources.append(entry)
        ledger_refs.append(entry["ledger_entry_id"])
    for visit in visits:
        ledger_id = f"browser-links:visit:{visit['visit_projection_id']}"
        sources.append(
            {
                "ledger_entry_id": ledger_id,
                "source_type": "browser-links",
                "source_ref": visit.get("normalized_url") or "",
                "source_hash": visit["source_record_hash"],
                "url_hash": visit.get("url_hash") or "",
                "domain": visit.get("domain") or "",
                "visit_ids": visit.get("visit_ids") or [],
                "visit_event_ids": visit.get("visit_event_ids") or [],
                "bookmark_ids": visit.get("bookmark_ids") or [],
                "file_ref": file_ref,
                "db_ref": f"personal_events:browser-links-{visit['visit_projection_id']}",
                "created_at_utc": now,
                "source_created_at": visit.get("first_visited_at") or "",
            }
        )
        ledger_refs.append(ledger_id)
    for bookmark in bookmarks:
        ledger_id = f"browser-links:bookmark:{bookmark['bookmark_projection_id']}"
        sources.append(
            {
                "ledger_entry_id": ledger_id,
                "source_type": "browser-links",
                "source_ref": bookmark.get("bookmark_id") or "",
                "source_hash": bookmark["source_record_hash"],
                "url_hash": bookmark.get("url_hash") or "",
                "bookmark_id": bookmark.get("bookmark_id") or "",
                "file_ref": file_ref,
                "db_ref": f"personal_events:browser-links-{bookmark['bookmark_projection_id']}",
                "created_at_utc": now,
                "source_created_at": bookmark.get("created_at") or "",
            }
        )
        ledger_refs.append(ledger_id)
    payload.update(
        {
            "schema": payload.get("schema") or "xarta.diary.source_ledger.v1",
            "updated_at_utc": now,
            "generated_by": "blueprints-personal-api",
            "sources": sources,
        }
    )
    module.atomic_write_json(ledger_path, payload, owner)
    return ledger_refs


def _write_browser_links_projection_artifacts(
    module: Any,
    *,
    local_date: str,
    timezone_name: str,
    source_status: str,
    health: dict[str, Any],
    visits: list[dict[str, Any]],
    bookmarks: list[dict[str, Any]],
    initiation: dict[str, Any],
    limited: bool,
    now: str,
) -> dict[str, Any]:
    owner = module.resolve_owner("xarta", "xarta")
    day = module.diary_day(root=DIARY_ROOT, local_date=local_date, timezone_name=timezone_name)
    module.init_day(
        root=DIARY_ROOT, local_date=local_date, timezone_name=timezone_name, owner=owner
    )
    projection_path = day.day_dir / BROWSER_LINKS_FILE
    summary = _browser_links_summary(visits, bookmarks)
    initiation_refs = _write_browser_links_initiation_files(
        module,
        local_date=local_date,
        timezone_name=timezone_name,
        initiation=initiation,
        now=now,
        owner=owner,
    )
    ledger_refs = _update_browser_links_source_ledger(
        module,
        day_dir=day.day_dir,
        file_ref=_diary_relative_path(projection_path),
        source_status=source_status,
        health=health,
        visits=visits,
        bookmarks=bookmarks,
        now=now,
    )
    payload = module.browser_links_template(day, generated_by="blueprints-personal-api")
    payload.update(
        {
            "updated_at_utc": now,
            "status": source_status,
            "source_health": health,
            "limited": limited,
            "summary": summary,
            "visits": visits,
            "bookmarks": bookmarks,
            "initiation_backfill": {
                "policy": initiation.get("policy", ""),
                "run_date": local_date,
                "refs": initiation_refs,
                "bookmarks_existing_count": initiation.get("bookmarks_existing_count", 0),
                "visit_events_existing_count": initiation.get("visit_events_existing_count", 0),
            },
            "source_ledger_refs": ledger_refs,
            "projected_event_ids": [
                event_id
                for event_id in (
                    f"browser-links-{local_date}-visits" if visits else "",
                    f"browser-links-{local_date}-bookmarks" if bookmarks else "",
                    f"browser-links-{local_date}-source-status"
                    if source_status in {"degraded", "source_unavailable"}
                    else "",
                )
                if event_id
            ],
        }
    )
    module.atomic_write_json(projection_path, payload, owner)
    _upsert_index_structured_file(
        module,
        day.day_dir,
        owner,
        filename=BROWSER_LINKS_FILE,
        label="Browser Links projection",
    )
    module.write_manifest(day, owner)
    return {
        "day": day,
        "projection_path": projection_path,
        "file_ref": _diary_relative_path(projection_path),
        "source_hash": module.sha256_file(projection_path),
        "summary": summary,
        "projected_event_ids": payload["projected_event_ids"],
        "initiation_backfill": payload["initiation_backfill"],
    }


def _upsert_browser_links_source(
    conn: Any,
    *,
    source_status: str,
    health: dict[str, Any],
    local_date: str,
    summary: dict[str, Any],
    now: str,
) -> Any:
    conn.execute(
        """
        INSERT INTO personal_sources (
            source_id, source_type, label, status, last_seen_at, health_json,
            provenance_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id) DO UPDATE SET
            status=excluded.status,
            last_seen_at=excluded.last_seen_at,
            health_json=excluded.health_json,
            provenance_json=excluded.provenance_json,
            updated_at=excluded.updated_at
        """,
        (
            "browser-links",
            "browser-links",
            "Browser Links",
            source_status,
            now,
            json.dumps(
                {
                    **health,
                    "local_date": local_date,
                    "day_summary": summary,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            json.dumps(
                {
                    "source_tables": ["bookmarks", "visits", "visit_events"],
                    "projection_file": BROWSER_LINKS_FILE,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_sources WHERE source_id='browser-links'").fetchone()


def _upsert_browser_links_status_event(
    conn: Any,
    *,
    source_status: str,
    local_date: str,
    timezone_name: str,
    file_ref: str,
    health: dict[str, Any],
    now: str,
) -> Any:
    event_id = f"browser-links-{local_date}-source-status"
    title = (
        "Browser Links source unavailable"
        if source_status == "source_unavailable"
        else "Browser Links health degraded"
    )
    status = "source_unavailable" if source_status == "source_unavailable" else "pending_review"
    state = "missing_source" if source_status == "source_unavailable" else "needs_review"
    body = (
        "Browser Links SQLite tables were unavailable during projection."
        if source_status == "source_unavailable"
        else "Browser Links projected SQLite data, but search/index health reported degraded status."
    )
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, local_date, timezone, status, privacy_level,
            tags_json, file_refs_json, db_refs_json, provenance_json, projection_state,
            provenance_state, last_rendered_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            source_ref=excluded.source_ref,
            source_hash=excluded.source_hash,
            title=excluded.title,
            body_excerpt=excluded.body_excerpt,
            content_projection=excluded.content_projection,
            start_at=excluded.start_at,
            status=excluded.status,
            file_refs_json=excluded.file_refs_json,
            provenance_json=excluded.provenance_json,
            projection_state=excluded.projection_state,
            provenance_state=excluded.provenance_state,
            last_rendered_at=excluded.last_rendered_at,
            updated_at=excluded.updated_at
        """,
        (
            event_id,
            "browser-links",
            file_ref,
            _hash_json_payload(health),
            "source-status",
            title,
            body,
            json.dumps({"source_status": source_status, "health": health}, ensure_ascii=True),
            now,
            local_date,
            timezone_name,
            status,
            "normal",
            json.dumps(["diary", "browser-links", "source-status"], ensure_ascii=True),
            json.dumps([file_ref], ensure_ascii=True),
            json.dumps([], ensure_ascii=True),
            json.dumps(
                {
                    "projection_file": file_ref,
                    "source_status": source_status,
                    "health": health,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "hot",
            state,
            now,
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()


def _browser_links_event_projection(
    *,
    file_ref: str,
    summary: dict[str, Any],
    visits: list[dict[str, Any]],
    bookmarks: list[dict[str, Any]],
    kind: str,
) -> dict[str, Any]:
    if kind == "browser-links-visits":
        return {
            "schema": BROWSER_LINKS_SCHEMA,
            "projection_file": file_ref,
            "kind": kind,
            "summary": summary,
            "top_visits": [
                {
                    "visit_projection_id": visit["visit_projection_id"],
                    "url_hash": visit["url_hash"],
                    "domain": visit["domain"],
                    "title": visit["title"],
                    "visit_count": visit["visit_count"],
                    "first_visited_at": visit["first_visited_at"],
                    "last_visited_at": visit["last_visited_at"],
                }
                for visit in sorted(
                    visits, key=lambda item: (-int(item.get("visit_count") or 0), item["domain"])
                )[:12]
            ],
        }
    return {
        "schema": BROWSER_LINKS_SCHEMA,
        "projection_file": file_ref,
        "kind": kind,
        "summary": summary,
        "bookmarks": [
            {
                "bookmark_projection_id": item["bookmark_projection_id"],
                "bookmark_id": item["bookmark_id"],
                "url_hash": item["url_hash"],
                "domain": item["domain"],
                "title": item["title"],
                "created_at": item["created_at"],
            }
            for item in bookmarks[:24]
        ],
    }


def _upsert_browser_links_aggregate_event(
    conn: Any,
    *,
    local_date: str,
    timezone_name: str,
    file_ref: str,
    source_hash: str,
    summary: dict[str, Any],
    visits: list[dict[str, Any]],
    bookmarks: list[dict[str, Any]],
    kind: str,
    now: str,
) -> Any:
    event_id = (
        f"browser-links-{local_date}-{'visits' if kind == 'browser-links-visits' else 'bookmarks'}"
    )
    projection = _browser_links_event_projection(
        file_ref=file_ref, summary=summary, visits=visits, bookmarks=bookmarks, kind=kind
    )
    if kind == "browser-links-visits":
        title = (
            f"Browser activity: {summary['visit_event_count']} visit events "
            f"across {summary['visited_domain_count']} domains"
        )
        top_domains = ", ".join(
            item["domain"] for item in summary.get("top_domains", [])[:4] if item.get("domain")
        )
        body = f"{summary['visited_page_count']} pages visited" + (
            f"; top domains: {top_domains}" if top_domains else ""
        )
        start_at = summary.get("first_visit_at") or now
        end_at = summary.get("last_visit_at") or None
        db_refs = [
            f"visit_events:{event_id}"
            for visit in visits
            for event_id in visit.get("visit_event_ids", [])[:8]
        ][:80]
    else:
        title = f"Browser bookmarks saved: {summary['bookmark_count']}"
        body = f"{summary['bookmark_count']} Browser Links bookmarks saved on {local_date}."
        start_at = summary.get("first_bookmark_at") or now
        end_at = summary.get("last_bookmark_at") or None
        db_refs = [f"bookmarks:{item['bookmark_id']}" for item in bookmarks[:80]]
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, end_at, local_date, timezone, status, privacy_level,
            tags_json, file_refs_json, db_refs_json, provenance_json, projection_state,
            provenance_state, last_rendered_at, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            source_ref=excluded.source_ref,
            source_hash=excluded.source_hash,
            kind=excluded.kind,
            title=excluded.title,
            body_excerpt=excluded.body_excerpt,
            content_projection=excluded.content_projection,
            start_at=excluded.start_at,
            end_at=excluded.end_at,
            local_date=excluded.local_date,
            timezone=excluded.timezone,
            status=excluded.status,
            privacy_level=excluded.privacy_level,
            tags_json=excluded.tags_json,
            file_refs_json=excluded.file_refs_json,
            db_refs_json=excluded.db_refs_json,
            provenance_json=excluded.provenance_json,
            projection_state=excluded.projection_state,
            provenance_state=excluded.provenance_state,
            last_rendered_at=excluded.last_rendered_at,
            updated_at=excluded.updated_at
        """,
        (
            event_id,
            "browser-links",
            file_ref,
            source_hash,
            kind,
            title,
            body[:500],
            json.dumps(projection, ensure_ascii=True, sort_keys=True),
            start_at,
            end_at,
            local_date,
            timezone_name,
            "open",
            "normal",
            json.dumps(["diary", "browser-links", kind], ensure_ascii=True),
            json.dumps([file_ref], ensure_ascii=True),
            json.dumps(db_refs, ensure_ascii=True),
            json.dumps(
                {
                    "projection_file": file_ref,
                    "source_detail_policy": "Use Browser Links source IDs and URL hashes; do not duplicate page bodies.",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "hot",
            "linked",
            now,
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()


async def _project_browser_links(body: DiaryBrowserLinksProjectRequest) -> dict[str, Any]:
    local_date = _validate_local_date(body.local_date)
    timezone_name = _clean_short_text(
        body.timezone or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
        "Europe/London",
        limit=80,
    )
    try:
        ZoneInfo(timezone_name)
    except Exception as exc:
        raise HTTPException(400, "timezone is invalid") from exc
    now = _utc_now_iso()
    with get_conn() as conn:
        selected = _select_browser_link_entries(
            conn,
            local_date=local_date,
            timezone_name=timezone_name,
            limit=body.limit,
        )
    if selected["status"] == "source_unavailable":
        source_status = "source_unavailable"
        health = selected["health"]
    else:
        health = await _browser_links_search_health(selected["health"])
        source_status = "ok" if health.get("status") == "ok" else "degraded"
    module = _load_xarta_diary_module()
    artifacts = _write_browser_links_projection_artifacts(
        module,
        local_date=local_date,
        timezone_name=timezone_name,
        source_status=source_status,
        health=health,
        visits=selected["visits"],
        bookmarks=selected["bookmarks"],
        initiation=selected["initiation"],
        limited=bool(selected.get("limited")),
        now=now,
    )
    file_ref = artifacts["file_ref"]
    audit_id = f"audit-{uuid.uuid4().hex}"
    actor = _clean_short_text(body.actor, "blueprints-api")
    source_surface = _clean_short_text(body.source_surface, "browser-links-projection")
    request_id = _clean_short_text(
        body.request_id, f"browser-links-project-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    projected_event_ids = set(artifacts["projected_event_ids"])
    with get_conn() as conn:
        existing_rows = conn.execute(
            """
            SELECT event_id FROM personal_events
            WHERE local_date=? AND source_type='browser-links'
            """,
            (local_date,),
        ).fetchall()
        previously_present = {row["event_id"] for row in existing_rows}
        source_row = _upsert_browser_links_source(
            conn,
            source_status=source_status,
            health=health,
            local_date=local_date,
            summary=artifacts["summary"],
            now=now,
        )
        event_rows = []
        if source_status in {"degraded", "source_unavailable"}:
            event_rows.append(
                _upsert_browser_links_status_event(
                    conn,
                    source_status=source_status,
                    local_date=local_date,
                    timezone_name=timezone_name,
                    file_ref=file_ref,
                    health=health,
                    now=now,
                )
            )
        else:
            conn.execute(
                """
                DELETE FROM personal_events
                WHERE event_id=? AND source_type='browser-links'
                """,
                (f"browser-links-{local_date}-source-status",),
            )
        if source_status != "source_unavailable":
            if selected["visits"]:
                event_rows.append(
                    _upsert_browser_links_aggregate_event(
                        conn,
                        local_date=local_date,
                        timezone_name=timezone_name,
                        file_ref=file_ref,
                        source_hash=artifacts["source_hash"],
                        summary=artifacts["summary"],
                        visits=selected["visits"],
                        bookmarks=selected["bookmarks"],
                        kind="browser-links-visits",
                        now=now,
                    )
                )
            else:
                conn.execute(
                    "DELETE FROM personal_events WHERE event_id=? AND source_type='browser-links'",
                    (f"browser-links-{local_date}-visits",),
                )
            if selected["bookmarks"]:
                event_rows.append(
                    _upsert_browser_links_aggregate_event(
                        conn,
                        local_date=local_date,
                        timezone_name=timezone_name,
                        file_ref=file_ref,
                        source_hash=artifacts["source_hash"],
                        summary=artifacts["summary"],
                        visits=selected["visits"],
                        bookmarks=selected["bookmarks"],
                        kind="browser-links-bookmarks",
                        now=now,
                    )
                )
            else:
                conn.execute(
                    "DELETE FROM personal_events WHERE event_id=? AND source_type='browser-links'",
                    (f"browser-links-{local_date}-bookmarks",),
                )
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action="project_browser_links",
            target_ref=f"diary-day:{local_date}",
            file_ref=file_ref,
            db_ref="personal_sources:browser-links",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result=source_status,
            source_hash=artifacts["source_hash"],
            metadata={
                "local_date": local_date,
                "visit_event_count": artifacts["summary"]["visit_event_count"],
                "visited_page_count": artifacts["summary"]["visited_page_count"],
                "bookmark_count": artifacts["summary"]["bookmark_count"],
                "skipped_existing_event_count": len(projected_event_ids & previously_present),
                "source_status": source_status,
            },
        )
        gen = increment_gen(conn, "personal-browser-links-projection")
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_sources", "browser-links", dict(source_row), gen
        )
        for event_row in event_rows:
            enqueue_for_all_peers(
                conn, "UPDATE", "personal_events", event_row["event_id"], dict(event_row), gen
            )
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "source_available": source_status != "source_unavailable",
        "status": source_status,
        "local_date": local_date,
        "timezone": timezone_name,
        "projection": {
            "file_ref": file_ref,
            "source_hash": artifacts["source_hash"],
            "visit_event_count": artifacts["summary"]["visit_event_count"],
            "visited_page_count": artifacts["summary"]["visited_page_count"],
            "bookmark_count": artifacts["summary"]["bookmark_count"],
            "projected_event_ids": artifacts["projected_event_ids"],
            "skipped_existing_event_count": len(projected_event_ids & previously_present),
            "initiation_backfill": artifacts["initiation_backfill"],
        },
        "source_health": health,
        "audit": {"audit_id": audit_id, "result": source_status},
        "day": _build_diary_day_payload(local_date),
    }


def _project_personal_log_event(
    *,
    result: dict[str, Any],
    body: str,
    file_ref: str,
    source_hash: str,
    audit_id: str,
    actor: str,
    source_surface: str,
    request_id: str,
    run_id: str,
    now: str,
) -> dict[str, Any]:
    local_date = result["local_date"]
    filename = result["filename"]
    event_id = f"diary-{local_date}-{Path(filename).stem}"
    tags = ["diary", "personal-log", "quick-entry"]
    provenance = {
        "writer": "xarta_diary.create_personal_log",
        "audit_id": audit_id,
        "actor": actor,
        "source_surface": source_surface,
        "request_id": request_id,
        "run_id": run_id,
        "day_path": result.get("day_path", ""),
        "schema": result.get("schema", ""),
    }
    with get_conn() as conn:
        _upsert_personal_source(conn, now)
        conn.execute(
            """
            INSERT INTO personal_events (
                event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
                content_projection, start_at, local_date, timezone, status, privacy_level,
                tags_json, file_refs_json, db_refs_json, provenance_json, projection_state,
                provenance_state, last_rendered_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                source_type=excluded.source_type,
                source_ref=excluded.source_ref,
                source_hash=excluded.source_hash,
                kind=excluded.kind,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                content_projection=excluded.content_projection,
                start_at=excluded.start_at,
                local_date=excluded.local_date,
                timezone=excluded.timezone,
                status=excluded.status,
                privacy_level=excluded.privacy_level,
                tags_json=excluded.tags_json,
                file_refs_json=excluded.file_refs_json,
                db_refs_json=excluded.db_refs_json,
                provenance_json=excluded.provenance_json,
                projection_state=excluded.projection_state,
                provenance_state=excluded.provenance_state,
                last_rendered_at=excluded.last_rendered_at,
                updated_at=excluded.updated_at
            """,
            (
                event_id,
                "manual",
                file_ref,
                source_hash,
                "personal-log",
                _entry_title(body, result.get("local_time")),
                _body_excerpt(body),
                body.strip(),
                now,
                local_date,
                result.get("timezone") or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
                "open",
                "normal",
                json.dumps(tags, ensure_ascii=True),
                json.dumps([file_ref], ensure_ascii=True),
                json.dumps([f"personal_time_audit:{audit_id}"], ensure_ascii=True),
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                "hot",
                "linked",
                now,
                now,
                now,
            ),
        )
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action="create_diary_entry",
            target_ref=f"personal_events:{event_id}",
            file_ref=file_ref,
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={
                "local_date": local_date,
                "kind": "personal-log",
                "body_chars": len(body.strip()),
            },
        )
        gen = increment_gen(conn, "personal-diary-write")
        event_row = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "personal_events", event_id, dict(event_row), gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return _row_to_event(event_row)


@router.post("/diary-day/entries")
async def create_diary_day_entry(body: DiaryEntryCreateRequest) -> dict[str, Any]:
    text = str(body.body or "").strip()
    if not text:
        raise HTTPException(400, "entry body is required")
    if len(text) > 20000:
        raise HTTPException(400, "entry body is too long")
    local_date = _validate_local_date(body.local_date)
    local_time = _validate_local_time(body.local_time)
    module = _load_xarta_diary_module()
    owner = module.resolve_owner("xarta", "xarta")
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "diary-page")
    request_id = _clean_short_text(
        body.request_id, f"diary-entry-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    tags = [str(tag).strip() for tag in body.tags if str(tag).strip()]
    result = module.create_personal_log(
        body=text,
        root=DIARY_ROOT,
        local_date=local_date,
        local_time=local_time,
        timezone_name=body.timezone or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
        author=actor,
        source=source_surface,
        tags=tags,
        owner=owner,
    )
    source_path = Path(result["path"])
    file_ref = _diary_relative_path(source_path)
    source_hash = module.sha256_file(source_path)
    audit_id = f"audit-{uuid.uuid4().hex}"
    now = _utc_now_iso()
    _update_source_ledger(
        module,
        day_dir=Path(result["day_path"]),
        event_id=f"diary-{result['local_date']}-{Path(result['filename']).stem}",
        audit_id=audit_id,
        file_ref=file_ref,
        source_hash=source_hash,
        actor=actor,
        source_surface=source_surface,
        now=now,
    )
    day = module.diary_day(
        root=DIARY_ROOT,
        local_date=result["local_date"],
        timezone_name=result.get("timezone")
        or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
    )
    module.write_manifest(day, owner)
    event = _project_personal_log_event(
        result=result,
        body=text,
        file_ref=file_ref,
        source_hash=source_hash,
        audit_id=audit_id,
        actor=actor,
        source_surface=source_surface,
        request_id=request_id,
        run_id=run_id,
        now=now,
    )
    return {
        "ok": True,
        "write": {
            "file_ref": file_ref,
            "source_hash": source_hash,
            "day_path": result["day_path"],
            "schema": result["schema"],
        },
        "audit": {
            "audit_id": audit_id,
            "actor": actor,
            "source_surface": source_surface,
            "request_id": request_id,
            "result": "ok",
        },
        "event": event,
        "day": _build_diary_day_payload(local_date),
    }


def _summary_markdown(local_date: str, events: list[dict[str, Any]], now: str) -> str:
    lines = [
        "---",
        f"schema: {DAY_SUMMARY_SCHEMA}",
        f"local_date: {local_date}",
        f"generated_at_utc: {now}",
        "generated_by: blueprints-personal-api",
        "privacy: personal",
        "privacy_level: normal",
        "tags:",
        "  - diary",
        "  - day-summary",
        "summary_of:",
    ]
    if events:
        for event in events[:40]:
            lines.append(f"  - {event['event_id']}")
    else:
        lines.append("  - no-visible-events")
    lines.extend(["---", "", f"# Day Summary: {local_date}", ""])
    if not events:
        lines.append("No visible source moments are recorded for this day.")
    else:
        lines.append(f"{len(events)} visible source moment(s) are recorded for this day.")
        lines.append("")
        lines.append("## Source Moments")
        lines.append("")
        for event in events[:20]:
            source = event.get("source", {})
            ref = source.get("ref") or event.get("event_id")
            title = event.get("title") or event.get("kind") or "event"
            lines.append(
                f"- `{event['event_id']}` - {title} ({source.get('type') or 'source'}: `{ref}`)"
            )
    lines.append("")
    return "\n".join(lines)


@router.post("/diary-day/summary")
async def generate_diary_day_summary(body: DiarySummaryGenerateRequest) -> dict[str, Any]:
    local_date = _validate_local_date(body.local_date)
    events, _, _ = _visible_day_events(local_date, source_filter="all")
    module = _load_xarta_diary_module()
    owner = module.resolve_owner("xarta", "xarta")
    day = module.diary_day(
        root=DIARY_ROOT,
        local_date=local_date,
        timezone_name=os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London"),
    )
    module.init_day(
        root=DIARY_ROOT, local_date=local_date, timezone_name=day.timezone_name, owner=owner
    )
    now = _utc_now_iso()
    summary_path = day.day_dir / "day-summary.md"
    content = _summary_markdown(local_date, events, now)
    module.atomic_write_text(summary_path, content, owner)
    module.write_manifest(day, owner)
    file_ref = _diary_relative_path(summary_path)
    source_hash = module.sha256_file(summary_path)
    audit_id = f"audit-{uuid.uuid4().hex}"
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "diary-page")
    request_id = _clean_short_text(
        body.request_id, f"diary-summary-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    with get_conn() as conn:
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action="generate_day_summary",
            target_ref=f"diary-day:{local_date}",
            file_ref=file_ref,
            db_ref="",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={"local_date": local_date, "event_count": len(events)},
        )
        gen = increment_gen(conn, "personal-diary-summary")
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "summary": {
            "file_ref": file_ref,
            "source_hash": source_hash,
            "event_count": len(events),
        },
        "audit": {"audit_id": audit_id, "result": "ok"},
        "day": _build_diary_day_payload(local_date),
    }


def _frontmatter_field(text: str, key: str) -> str:
    if not text.startswith("---"):
        return ""
    end = text.find("\n---", 3)
    if end == -1:
        return ""
    match = re.search(rf"(?m)^{re.escape(key)}:\s*(.+?)\s*$", text[3:end])
    return match.group(1).strip() if match else ""


def _first_markdown_scalar(text: str, label: str) -> str:
    match = re.search(rf"(?m)^-\s+{re.escape(label)}:\s+`?([^`\n]+)`?\s*$", text)
    return match.group(1).strip() if match else ""


def _clean_markdown_cell(value: str) -> str:
    without_link = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value).strip()
    code = re.fullmatch(r"`([^`]*)`", without_link)
    return code.group(1) if code else without_link


def _dashboard_link_path(href: str) -> str:
    clean = href.strip()
    if clean.startswith(("http://", "https://", "/")):
        return clean
    if clean.startswith("docs/"):
        return clean
    return posixpath.normpath((INTERESTS_DASHBOARD_REL.parent / clean).as_posix())


def _markdown_link(value: str) -> dict[str, str] | None:
    match = re.search(r"\[([^\]]+)\]\(([^)]+)\)", value)
    if not match:
        return None
    return {"label": match.group(1).strip(), "path": _dashboard_link_path(match.group(2))}


def _markdown_table(text: str, heading: str, limit: int = 20) -> list[dict[str, str]]:
    pattern = rf"(?ms)^##\s+{re.escape(heading)}\s*\n\n(.+?)(?=\n##\s+|\Z)"
    match = re.search(pattern, text)
    if not match:
        return []
    lines = [line.strip() for line in match.group(1).splitlines() if line.strip().startswith("|")]
    if len(lines) < 2:
        return []
    headers = [part.strip() for part in lines[0].strip("|").split("|")]
    rows: list[dict[str, str]] = []
    for line in lines[2 : 2 + limit]:
        values = [part.strip() for part in line.strip("|").split("|")]
        if len(values) < len(headers):
            values.extend([""] * (len(headers) - len(values)))
        row = {}
        for idx in range(len(headers)):
            header = headers[idx]
            value = values[idx]
            row[header] = _clean_markdown_cell(value)
            link = _markdown_link(value)
            if link:
                row[f"{header}_path"] = link["path"]
        rows.append(row)
    return rows


def _markdown_list_link(text: str, label: str) -> dict[str, str] | None:
    match = re.search(
        rf"(?m)^-\s+{re.escape(label)}:\s+\[([^\]]+)\]\(([^)]+)\)",
        text,
    )
    if not match:
        return None
    return {"label": match.group(1).strip(), "path": _dashboard_link_path(match.group(2))}


def _parse_interests_dashboard() -> dict[str, Any]:
    path = LONE_WOLF_ROOT / INTERESTS_DASHBOARD_REL
    if not path.exists():
        return {
            "status": "source_unavailable",
            "path": str(path),
            "source_digest": "",
            "snapshot_at": "",
            "pending_review": None,
            "actionable_backlog": None,
            "category_summary": [],
            "input_health": [],
            "recent_completed_work": [],
            "source_unavailable": [],
            "blockers": ["interests dashboard source is missing"],
        }

    text = path.read_text(encoding="utf-8")
    digest = "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
    overall = "unknown"
    match = re.search(r"(?m)^Overall:\s+\*\*(.+?)\*\*", text)
    if match:
        overall = match.group(1).strip().lower()
    pending_review = _first_markdown_scalar(text, "Pending review")
    actionable_backlog = _first_markdown_scalar(text, "Actionable backlog")
    completion_blockers = re.search(r"(?ms)^## Completion Blockers\s*\n\n(.+?)(?=\n##\s+|\Z)", text)
    blocker_text = completion_blockers.group(1).strip() if completion_blockers else ""
    blocker_items = (
        [] if "None reported" in blocker_text else [blocker_text] if blocker_text else []
    )
    proof_links = [
        {
            "label": "Hermes Interests Ingestion Dashboard",
            "path": str(INTERESTS_DASHBOARD_REL),
        },
    ]
    for label in ("Completion proof", "Follow-up add-on"):
        link = _markdown_list_link(text, label)
        if link:
            proof_links.append(link)
    return {
        "status": "ok" if overall == "ok" else overall,
        "path": str(path),
        "doc_path": str(INTERESTS_DASHBOARD_REL),
        "snapshot_at": _frontmatter_field(text, "source_snapshot_at")
        or _first_markdown_scalar(text, "Source snapshot"),
        "source_digest": _frontmatter_field(text, "source_digest")
        or _first_markdown_scalar(text, "Source digest")
        or digest,
        "content_digest": digest,
        "pending_review": int(pending_review) if pending_review.isdigit() else pending_review,
        "actionable_backlog": int(actionable_backlog)
        if actionable_backlog.isdigit()
        else actionable_backlog,
        "category_summary": _markdown_table(text, "Category Summary", limit=20),
        "input_health": _markdown_table(text, "Input Health", limit=20),
        "recent_completed_work": _markdown_table(text, "Recent Completed Work", limit=12),
        "source_unavailable": _markdown_table(text, "Source-Unavailable", limit=12),
        "blockers": blocker_items,
        "rerun_status": "idempotent source digest" if "source digest changes" in text else "",
        "proof_links": proof_links,
    }


def _configured_git_repos() -> list[dict[str, str]]:
    configured = os.environ.get("BLUEPRINTS_PERSONAL_GIT_REPOS", "").strip()
    repos: list[dict[str, str]] = []
    if configured:
        for index, item in enumerate(configured.split(",")):
            path = item.strip()
            if not path:
                continue
            repos.append({"repo_id": f"git-{index + 1}", "path": path, "label": Path(path).name})
        return repos
    return [
        {"repo_id": repo_id, "path": path, "label": label}
        for repo_id, path, label in DEFAULT_PERSONAL_GIT_REPOS
    ]


def _git(path: Path, *args: str) -> tuple[bool, str]:
    try:
        result = subprocess.run(
            ["git", "-C", str(path), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=8,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return False, str(exc)
    output = result.stdout.strip() if result.returncode == 0 else result.stderr.strip()
    return result.returncode == 0, output


def _git_repo_status(repo: dict[str, str]) -> dict[str, Any]:
    path = Path(repo["path"])
    base = {
        "repo_id": repo["repo_id"],
        "label": repo["label"],
        "path": str(path),
        "exists": path.exists(),
        "is_repo": False,
        "status": "source_unavailable",
        "branch": "",
        "head": "",
        "head_subject": "",
        "head_author_date": "",
        "dirty_count": 0,
        "untracked_count": 0,
        "daily_commit_count": 0,
        "latest_commits": [],
        "actions": [],
        "error": "",
    }
    if not path.exists():
        base["error"] = "path missing"
        base["actions"] = ["restore or correct watched repo path"]
        return base

    ok, inside = _git(path, "rev-parse", "--is-inside-work-tree")
    if not ok or inside.strip().lower() != "true":
        base["error"] = inside or "not a git repository"
        base["actions"] = ["remove non-repo path from watched repo list"]
        return base

    base["is_repo"] = True
    ok, branch = _git(path, "rev-parse", "--abbrev-ref", "HEAD")
    base["branch"] = branch if ok else ""
    ok, head = _git(path, "rev-parse", "--short=12", "HEAD")
    base["head"] = head if ok else ""
    ok, latest = _git(path, "log", "-1", "--format=%H%x00%h%x00%cI%x00%s")
    if ok and latest:
        parts = latest.split("\x00")
        if len(parts) >= 4:
            base["head_author_date"] = parts[2]
            base["head_subject"] = parts[3]

    ok, porcelain = _git(path, "status", "--porcelain")
    if ok:
        lines = [line for line in porcelain.splitlines() if line.strip()]
        base["dirty_count"] = len(lines)
        base["untracked_count"] = sum(1 for line in lines if line.startswith("??"))
    else:
        base["error"] = porcelain

    today = datetime.now().astimezone().strftime("%Y-%m-%dT00:00:00")
    ok, commits = _git(path, "log", f"--since={today}", "--format=%H%x00%h%x00%cI%x00%s", "-n", "8")
    if ok:
        latest_commits = []
        for line in commits.splitlines():
            parts = line.split("\x00")
            if len(parts) >= 4:
                latest_commits.append(
                    {
                        "repo_id": repo["repo_id"],
                        "repo_label": repo["label"],
                        "sha": parts[0],
                        "short_sha": parts[1],
                        "author_date": parts[2],
                        "subject": parts[3],
                    }
                )
        base["latest_commits"] = latest_commits
        base["daily_commit_count"] = len(latest_commits)

    base["status"] = "ok" if base["dirty_count"] == 0 and not base["error"] else "needs_review"
    if base["dirty_count"]:
        base["actions"].append("review uncommitted changes")
    if base["error"]:
        base["actions"].append("repair git scan")
    return base


def _personal_source_counts() -> dict[str, Any]:
    with get_conn() as conn:
        event_rows = conn.execute(
            "SELECT source_type, COUNT(*) AS count FROM personal_events GROUP BY source_type"
        ).fetchall()
        batch_rows = conn.execute(
            "SELECT source_type, status, COUNT(*) AS count FROM personal_import_batches GROUP BY source_type, status"
        ).fetchall()
    batch_counts: dict[str, dict[str, int]] = {}
    for row in batch_rows:
        batch_counts.setdefault(row["source_type"], {})[row["status"]] = row["count"]
    return {
        "events": {row["source_type"]: row["count"] for row in event_rows},
        "import_batches": batch_counts,
    }


def _git_activity_dashboard(counts: dict[str, Any]) -> dict[str, Any]:
    repos = [_git_repo_status(repo) for repo in _configured_git_repos()]
    latest_commits = sorted(
        [commit for repo in repos for commit in repo["latest_commits"]],
        key=lambda item: item.get("author_date") or "",
        reverse=True,
    )[:12]
    errors = [repo for repo in repos if repo.get("error")]
    actionable = [repo for repo in repos if repo.get("actions")]
    source_ready = all(repo["is_repo"] for repo in repos) if repos else False
    clean = all(repo["dirty_count"] == 0 for repo in repos if repo["is_repo"])
    git_event_count = int(counts["events"].get("git", 0))
    git_batches = counts["import_batches"].get("git", {})
    return {
        "status": "ok" if source_ready and not errors and not actionable else "needs_review",
        "source_ready": source_ready,
        "clean": clean,
        "watched_repos": repos,
        "latest_commits": latest_commits,
        "projection_counts": {
            "events": git_event_count,
            "import_batches": git_batches,
        },
        "import_status": "source_scan_ready" if source_ready else "source_scan_blocked",
        "index_status": "has_projection_rows" if git_event_count else "source_scan_only",
        "daily_summary": {
            "status": "ready" if latest_commits else "no_commits_today",
            "commit_count": len(latest_commits),
            "source": "watched git repositories",
        },
        "actionable_repos": actionable,
        "errors": errors,
    }


@router.get("/imports-dashboard")
async def get_imports_dashboard() -> dict[str, Any]:
    counts = _personal_source_counts()
    interests = _parse_interests_dashboard()
    git_activity = _git_activity_dashboard(counts)
    blockers = []
    if interests.get("blockers"):
        blockers.append({"source": "interests-ingestion", "items": interests["blockers"]})
    if git_activity.get("errors"):
        blockers.append({"source": "git-activity", "items": git_activity["errors"]})

    digest_payload = json.dumps(
        {
            "interests": {
                "source_digest": interests.get("source_digest"),
                "snapshot_at": interests.get("snapshot_at"),
                "pending_review": interests.get("pending_review"),
                "actionable_backlog": interests.get("actionable_backlog"),
            },
            "git": [
                {
                    "repo_id": repo["repo_id"],
                    "head": repo["head"],
                    "dirty_count": repo["dirty_count"],
                    "daily_commit_count": repo["daily_commit_count"],
                    "error": repo["error"],
                }
                for repo in git_activity["watched_repos"]
            ],
            "counts": counts,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    source_digest = "sha256:" + hashlib.sha256(digest_payload.encode("utf-8")).hexdigest()
    status = (
        "ok"
        if interests.get("status") == "ok" and git_activity.get("status") == "ok"
        else "needs_review"
    )
    return {
        "status": status,
        "generated_at": _utc_now_iso(),
        "source_digest": source_digest,
        "source_counts": counts,
        "interests": interests,
        "git_activity": git_activity,
        "recent_work": {
            "interests": interests.get("recent_completed_work", [])[:8],
            "git": git_activity.get("latest_commits", [])[:8],
        },
        "blockers": blockers,
        "proof_links": [
            *interests.get("proof_links", []),
            {
                "label": "Personal Time Activity Step 8 proof",
                "path": "docs/personal/time-activity-goal/PERSONAL-TIME-ACTIVITY-STEP-08-PROOF-2026-06-18.md",
            },
            {
                "label": "Personal projection API v1",
                "path": "docs/personal/time-activity-goal/PERSONAL-TIME-ACTIVITY-PROJECTION-API-V1.md",
            },
        ],
        "refresh": {
            "idempotency": "source_digest changes only when source status changes",
            "timestamp": _utc_now_iso(),
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
