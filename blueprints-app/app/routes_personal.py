"""Shared Personal Time Activity projection API."""

from __future__ import annotations

import hashlib
import json
import os
import posixpath
import re
import subprocess
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
LONE_WOLF_ROOT = Path(os.environ.get("BLUEPRINTS_LONE_WOLF_ROOT", "/xarta-node/.lone-wolf"))
INTERESTS_DASHBOARD_REL = Path("docs/interests/HERMES-INTERESTS-INGESTION-DASHBOARD.md")
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
        row = {headers[idx]: _clean_markdown_cell(values[idx]) for idx in range(len(headers))}
        rows.append(row)
    return rows


def _dashboard_link_path(href: str) -> str:
    clean = href.strip()
    if clean.startswith(("http://", "https://", "/")):
        return clean
    if clean.startswith("docs/"):
        return clean
    return posixpath.normpath((INTERESTS_DASHBOARD_REL.parent / clean).as_posix())


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
