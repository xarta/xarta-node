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

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel
from starlette.responses import FileResponse

from . import hermes_minutes
from .db import get_conn, get_setting, increment_gen, set_setting
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/personal", tags=["personal"])

DIARY_ROOT = Path(os.environ.get("BLUEPRINTS_DIARY_DIR", "/xarta-node/.lone-wolf/diary"))
KANBAN_ROOT = Path(os.environ.get("BLUEPRINTS_KANBAN_DIR", "/xarta-node/.lone-wolf/kanban"))
XARTA_AGENT_LIB = Path(os.environ.get("XARTA_AGENT_LIB", "/root/xarta-node/.xarta/.agents/lib"))
LONE_WOLF_ROOT = Path(os.environ.get("BLUEPRINTS_LONE_WOLF_ROOT", "/xarta-node/.lone-wolf"))
INTERESTS_DASHBOARD_REL = Path("docs/interests/HERMES-INTERESTS-INGESTION-DASHBOARD.md")
INTERESTS_ROOT_REL = Path("interests")
OPENCLAW_BOOKMARK_CANDIDATES_REL = Path(
    "runtime/openclaw-migration/2026-06-12-vm720/derived/bookmark_candidates.jsonl"
)
IMPORT_ARTIFACT_ALLOWED_PREFIXES: tuple[str, ...] = (
    "docs/",
    "interests/",
    "stacks/hermes-local/data/health/",
)
IMPORT_ARTIFACT_TEXT_SUFFIXES: tuple[str, ...] = (
    ".json",
    ".jsonl",
    ".md",
    ".txt",
    ".yaml",
    ".yml",
)
IMPORT_ARTIFACT_PREVIEW_BYTES = 256 * 1024
OPENCLAW_AUDIT_PATTERNS: tuple[tuple[str, str, str], ...] = (
    (
        "ai-developments",
        "AI-ish OpenClaw URL candidates",
        r"\b(ai|llm|large language|language model|openai|anthropic|deepmind|gemini|claude|qwen|llama|mistral|ollama|vllm|litellm|machine learning|neural|transformer|diffusion|embedding|agent|marktechpost|arxiv|model context protocol|mcp)\b",
    ),
    (
        "hardware",
        "electronics / embedded URL candidates",
        r"\b(esp32|arduino|raspberry pi|microcontroller|electronics|pcb|solder|i2c|spi|uart|wifi hotspot|webflasher|embedded)\b",
    ),
    (
        "games",
        "game URL candidates",
        r"\b(wordle|connections|nyt games|steam deck|videogame|boardgame)\b",
    ),
)
OPENCLAW_AI_DEVELOPMENT_DOMAINS: tuple[str, ...] = ("marktechpost.com", "venturebeat.com")
DAY_SUMMARY_SCHEMA = "xarta.diary.day_summary.v1"
KANBAN_ITEM_DETAIL_SCHEMA = "xarta.kanban.item_detail.v1"
KANBAN_DISCUSSION_SCHEMA = "xarta.kanban.discussion.v1"
RICH_DOC_IMAGE_SCHEMA = "xarta.rich_document.image.v1"
RICH_DOC_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
RICH_DOC_IMAGE_MAX_BYTES = 16 * 1024 * 1024
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
    "kanban": {
        "label": "Kanban",
        "filters": {"source_type": "kanban"},
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
KANBAN_DEPTH_LIMIT = 12
KANBAN_SHOW_TEST_ENTRIES_SETTING = "personal.kanban.show_test_entries"
KANBAN_TAG = "kanban"
KANBAN_AGENT_WORKING_OUT_TAG = "agent-working-out"
KANBAN_PROOF_TAG = "proof"


class PersonalRehydrateRequest(BaseModel):
    event_id: str
    force: bool = False


class PersonalProjectionMaintenanceRequest(BaseModel):
    retention_days: int = 60
    limit: int = 250
    dry_run: bool = True
    now: str | None = None


class DiaryEntryCreateRequest(BaseModel):
    body: str
    local_date: str | None = None
    local_time: str | None = None
    end_time: str | None = None
    all_day: bool | None = None
    range_start_date: str | None = None
    range_end_date: str | None = None
    timezone: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "diary-page"
    request_id: str | None = None
    run_id: str | None = None
    tags: list[str] = []


class DiaryEntryUpdateRequest(BaseModel):
    body: str
    local_date: str | None = None
    local_time: str | None = None
    end_time: str | None = None
    all_day: bool | None = None
    range_start_date: str | None = None
    range_end_date: str | None = None
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


class DiaryKanbanLinkRequest(BaseModel):
    kanban_item_ref: str
    actor: str = "blueprints-ui"
    source_surface: str = "diary-page"
    request_id: str | None = None
    run_id: str | None = None


class PersonalEventDeleteRequest(BaseModel):
    actor: str = "blueprints-ui"
    source_surface: str = "personal-page"
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
    related_kanban_items: list[str] = []
    related_tasks: list[str] = []
    related_import_batches: list[str] = []
    actor: str = "blueprints-ui"
    source_surface: str = "calendar-page"
    request_id: str | None = None
    run_id: str | None = None


class PersonalTaskUpsertRequest(BaseModel):
    task_id: str | None = None
    title: str
    body: str | None = None
    mode: str = "personal"
    status: str = "open"
    priority: str | None = None
    due_date: str | None = None
    due_time: str | None = None
    timezone: str | None = None
    privacy_level: str = "normal"
    tags: list[str] = []
    related_kanban_items: list[str] = []
    related_tasks: list[str] = []
    related_import_batches: list[str] = []
    actor: str = "blueprints-ui"
    source_surface: str = "todo-page"
    request_id: str | None = None
    run_id: str | None = None


class PersonalTaskActionRequest(BaseModel):
    actor: str = "blueprints-ui"
    source_surface: str = "todo-page"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemCreateRequest(BaseModel):
    item_id: str | None = None
    parent_item_id: str | None = None
    title: str
    body: str | None = None
    item_type: str = "item"
    state_id: str = "todo"
    priority_id: str = "medium"
    goal_flag: bool = False
    sort_order: int = 0
    tags: list[str] = []
    related_event_ids: list[str] = []
    related_task_ids: list[str] = []
    related_issue_ids: list[str] = []
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemUpdateRequest(BaseModel):
    title: str | None = None
    body: str | None = None
    item_type: str | None = None
    state_id: str | None = None
    priority_id: str | None = None
    goal_flag: bool | None = None
    sort_order: int | None = None
    tags: list[str] | None = None
    related_event_ids: list[str] | None = None
    related_task_ids: list[str] | None = None
    related_issue_ids: list[str] | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemMoveRequest(BaseModel):
    parent_item_id: str | None = None
    state_id: str | None = None
    sort_order: int | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemOrderRequest(BaseModel):
    direction: str
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemActionRequest(BaseModel):
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class RichDocImageAssociateRequest(BaseModel):
    domain: str = "diary"
    markdown: str = ""
    document_type: str = "document"
    document_id: str = ""
    local_date: str = ""
    item_id: str = ""
    discussion_id: str = ""
    actor: str = "blueprints-ui"
    source_surface: str = "rich-document-editor"
    request_id: str | None = None
    run_id: str | None = None


class WorkPreferencesUpdateRequest(BaseModel):
    show_test_entries: bool | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemDetailDocumentUpdateRequest(BaseModel):
    body: str = ""
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkDiscussionCreateRequest(BaseModel):
    discussion_id: str | None = None
    body: str = ""
    author: str | None = None
    status: str = "open"
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkDiscussionUpdateRequest(BaseModel):
    body: str | None = None
    author: str | None = None
    status: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemLinkCreateRequest(BaseModel):
    target_item_id: str
    link_type: str = "related"
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemCommitCreateRequest(BaseModel):
    repo_full_name: str
    sha: str
    short_sha: str | None = None
    html_url: str | None = None
    author_login: str | None = None
    author_name: str | None = None
    committed_at: str | None = None
    message_subject: str | None = None
    message_body: str | None = None
    branch: str | None = None
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class PersonalGraphLinkCreateRequest(BaseModel):
    source_ref: str
    target_ref: str
    link_type: str = "relates_to"
    link_state: str = "declared"
    risk_level: str = "normal"
    title: str | None = None
    metadata: dict[str, Any] = {}
    provenance: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "personal-graph"
    request_id: str | None = None
    run_id: str | None = None


class PersonalGraphSyncRequest(BaseModel):
    actor: str = "blueprints-api"
    source_surface: str = "personal-graph-sync"
    request_id: str | None = None
    run_id: str | None = None


class WorkIssueUpsertRequest(BaseModel):
    item_id: str
    issue_id: str | None = None
    title: str
    body: str | None = None
    status: str = "open"
    priority_id: str = "medium"
    severity_id: str | None = None
    source_ref: str | None = None
    related_task_id: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkTodoUpsertRequest(BaseModel):
    item_id: str
    todo_id: str | None = None
    title: str
    body: str | None = None
    status: str = "open"
    priority_id: str = "medium"
    due_at: str | None = None
    related_task_id: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkBlockerUpsertRequest(BaseModel):
    item_id: str
    blocker_id: str | None = None
    title: str
    body: str | None = None
    status: str = "open"
    blocked_by_ref: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkPromoteRequest(BaseModel):
    source_ref: str
    title: str | None = None
    body: str | None = None
    parent_item_id: str | None = None
    state_id: str = "todo"
    priority_id: str = "medium"
    tags: list[str] = []
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
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


class PersonalSearchSyncRequest(BaseModel):
    force_embeddings: bool = False
    include_embeddings: bool = True
    limit: int = 200


def _json_value(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    with suppress(json.JSONDecodeError, TypeError):
        parsed = json.loads(value)
        return parsed if parsed is not None else fallback
    return fallback


def _work_item_tags(tags: list[str] | None) -> list[str]:
    clean = []
    for tag in _clean_event_list(tags or [], limit=32):
        if tag and tag not in clean:
            clean.append(tag)
    if KANBAN_TAG not in clean:
        clean.append(KANBAN_TAG)
    return clean


def _work_request_is_agent_working_out(meta: dict[str, str], tags: list[str]) -> bool:
    if KANBAN_AGENT_WORKING_OUT_TAG in tags:
        return True
    source_surface = str(meta.get("source_surface") or "").lower()
    actor = str(meta.get("actor") or "").lower()
    request_id = str(meta.get("request_id") or "").lower()
    return (
        source_surface == "kanban-active-browser-proof"
        or actor in {"codex-playwright"}
        or "active-browser-step" in request_id
        or KANBAN_PROOF_TAG in tags
        and source_surface.startswith("kanban-")
    )


def _work_item_tags_for_request(tags: list[str] | None, meta: dict[str, str]) -> list[str]:
    clean = _work_item_tags(tags)
    if _work_request_is_agent_working_out(meta, clean):
        for tag in (KANBAN_AGENT_WORKING_OUT_TAG, KANBAN_PROOF_TAG):
            if tag not in clean:
                clean.append(tag)
    return clean


def _work_row_tags(row: Any) -> list[str]:
    return [str(tag).strip() for tag in _json_value(row["tags_json"], []) if str(tag).strip()]


def _work_row_has_filter_tag(row: Any, tag: str) -> bool:
    clean_tag = str(tag or "").strip().lower()
    return clean_tag in {entry.lower() for entry in _work_row_tags(row)}


def _clean_work_item_type(value: str | None, default: str = "item") -> str:
    item_type = _clean_short_text(value, default, limit=80)
    if item_type.lower() == "todo":
        raise HTTPException(
            400,
            "Kanban ToDo is a todo-lane leaf item or filter tag, not item_type='todo'",
        )
    if item_type.lower() == "work":
        return "item"
    return item_type


def _clean_git_repo_full_name(value: str | None) -> str:
    repo = _clean_short_text(value, "", limit=240)
    if not repo or "/" not in repo or repo.startswith("/") or repo.endswith("/"):
        raise HTTPException(400, "Git commit repo_full_name must be owner/name")
    if any(ch.isspace() for ch in repo):
        raise HTTPException(400, "Git commit repo_full_name must not contain whitespace")
    return repo


def _clean_git_sha(value: str | None) -> str:
    sha = _clean_short_text(value, "", limit=80).lower()
    if not re.fullmatch(r"[0-9a-f]{7,64}", sha):
        raise HTTPException(400, "Git commit sha must be a 7-64 character hex SHA")
    return sha


def _kanban_commit_link_id(item_id: str, repo_full_name: str, sha: str) -> str:
    digest = hashlib.sha256(f"{item_id}\n{repo_full_name}\n{sha}".encode("utf-8")).hexdigest()
    return f"kanban-commit-{digest[:24]}"


def _git_commit_ref(repo_full_name: str, sha: str) -> str:
    return f"git_commit:{repo_full_name}@{sha}"


def _github_commit_url(repo_full_name: str, sha: str) -> str:
    return f"https://github.com/{repo_full_name}/commit/{sha}"


def _row_to_work_commit(row: Any) -> dict[str, Any]:
    repo_full_name = row["repo_full_name"]
    sha = row["sha"]
    return {
        "commit_link_id": row["commit_link_id"],
        "item_id": row["item_id"],
        "repo_full_name": repo_full_name,
        "sha": sha,
        "short_sha": row["short_sha"],
        "html_url": row["html_url"],
        "author_login": row["author_login"],
        "author_name": row["author_name"],
        "committed_at": row["committed_at"],
        "message_subject": row["message_subject"],
        "message_body": row["message_body"],
        "branch": row["branch"],
        "commit_ref": _git_commit_ref(repo_full_name, sha),
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


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
            "kanban_items": _json_value(row["related_kanban_items_json"], []),
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


def _row_to_task(row: Any) -> dict[str, Any]:
    return {
        "task_id": row["task_id"],
        "event_id": row["event_id"],
        "kind": "task",
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "status": row["status"],
        "mode": row["mode"],
        "priority": row["priority"],
        "due_at": row["due_at"],
        "local_date": row["local_date"],
        "timezone": row["timezone"],
        "privacy_level": row["privacy_level"],
        "tags": _json_value(row["tags_json"], []),
        "source": {
            "type": row["source_type"],
            "ref": row["source_ref"],
            "hash": row["source_hash"],
            "authority": "task",
        },
        "related": {
            "kanban_items": _json_value(row["related_kanban_items_json"], []),
            "tasks": _json_value(row["related_tasks_json"], []),
            "import_batches": _json_value(row["related_import_batches_json"], []),
        },
        "file_refs": _json_value(row["file_refs_json"], []),
        "db_refs": _json_value(row["db_refs_json"], []),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "completed_at": row["completed_at"],
        "archived_at": row["archived_at"],
    }


def _row_to_work_state(row: Any) -> dict[str, Any]:
    return {
        "state_id": row["state_id"],
        "label": row["label"],
        "lane_key": row["lane_key"],
        "status_category": row["status_category"],
        "sort_order": row["sort_order"],
        "is_terminal": bool(row["is_terminal"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_priority(row: Any) -> dict[str, Any]:
    return {
        "priority_id": row["priority_id"],
        "label": row["label"],
        "weight": row["weight"],
        "sort_order": row["sort_order"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_item(row: Any) -> dict[str, Any]:
    row_keys = row.keys() if hasattr(row, "keys") else []
    return {
        "item_id": row["item_id"],
        "parent_item_id": row["parent_item_id"],
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "item_type": row["item_type"],
        "state_id": row["state_id"],
        "priority_id": row["priority_id"],
        "depth": row["depth"],
        "sort_order": row["sort_order"],
        "status": row["status"],
        "goal_flag": bool(row["goal_flag"]) if "goal_flag" in row_keys else False,
        "archived_at": row["archived_at"],
        "promoted_from_ref": row["promoted_from_ref"],
        "source": {
            "type": row["source_type"],
            "ref": row["source_ref"],
            "hash": row["source_hash"],
        },
        "tags": _json_value(row["tags_json"], []),
        "related": {
            "events": _json_value(row["related_event_ids_json"], []),
            "tasks": _json_value(row["related_task_ids_json"], []),
            "issues": _json_value(row["related_issue_ids_json"], []),
        },
        "search": {
            "text": row["search_text"],
            "metadata": _json_value(row["search_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
        },
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_issue(row: Any) -> dict[str, Any]:
    provenance = _json_value(row["provenance_json"], {})
    issue_meta = provenance.get("issue") if isinstance(provenance.get("issue"), dict) else {}
    related_tasks = _json_value(row["related_task_ids_json"], [])
    parent_item_id = row["parent_item_id"] or ""
    item_card = _row_to_work_item(row)
    return {
        "issue_id": row["item_id"],
        "item_id": row["item_id"],
        "parent_item_id": parent_item_id,
        "item_type": "issue",
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "status": row["status"],
        "priority_id": row["priority_id"],
        "severity_id": row["priority_id"],
        "source_ref": issue_meta.get("external_source_ref") or row["source_ref"],
        "related_task_id": related_tasks[0] if related_tasks else "",
        "item_card": item_card,
        "search": {
            "text": row["search_text"],
            "metadata": _json_value(row["search_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
        },
        "provenance": provenance,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_todo(row: Any) -> dict[str, Any]:
    provenance = _json_value(row["provenance_json"], {})
    todo_meta = provenance.get("todo") if isinstance(provenance.get("todo"), dict) else {}
    related_tasks = _json_value(row["related_task_ids_json"], [])
    parent_item_id = row["parent_item_id"] or ""
    item_card = _row_to_work_item(row)
    return {
        "todo_id": row["item_id"],
        "item_id": row["item_id"],
        "parent_item_id": parent_item_id,
        "item_type": row["item_type"],
        "todo_view": True,
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "status": row["status"],
        "priority_id": row["priority_id"],
        "due_at": todo_meta.get("due_at") or "",
        "related_task_id": related_tasks[0] if related_tasks else "",
        "item_card": item_card,
        "search": {
            "text": row["search_text"],
            "metadata": _json_value(row["search_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
        },
        "provenance": provenance,
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _kanban_todo_to_task(row: Any) -> dict[str, Any]:
    todo = _row_to_work_todo(row)
    due_at = todo["due_at"] or ""
    local_date = due_at[:10] if len(due_at) >= 10 else ""
    related_task_id = todo["related_task_id"] or ""
    parent_item_id = todo["parent_item_id"] or ""
    return {
        "task_id": todo["todo_id"],
        "event_id": "",
        "kind": "task",
        "title": todo["title"],
        "body_excerpt": todo["body_excerpt"],
        "status": todo["status"],
        "mode": "kanban",
        "priority": todo["priority_id"],
        "due_at": due_at,
        "local_date": local_date,
        "timezone": "",
        "privacy_level": "normal",
        "tags": [KANBAN_TAG, "todo", "task"],
        "source": {
            "type": "kanban-todo",
            "ref": f"kanban_items:{todo['todo_id']}",
            "hash": "",
            "authority": "kanban_todo",
        },
        "related": {
            "kanban_items": [parent_item_id] if parent_item_id else [],
            "tasks": [related_task_id] if related_task_id else [],
            "import_batches": [],
        },
        "file_refs": [],
        "db_refs": [
            f"kanban_items:{todo['todo_id']}",
            *([f"kanban_items:{parent_item_id}"] if parent_item_id else []),
        ],
        "provenance": todo["provenance"],
        "created_at": todo["created_at"],
        "updated_at": todo["updated_at"],
        "completed_at": None,
        "archived_at": todo["updated_at"] if todo["status"] == "archived" else None,
    }


def _row_to_work_blocker(row: Any) -> dict[str, Any]:
    return {
        "blocker_id": row["blocker_id"],
        "item_id": row["item_id"],
        "title": row["title"],
        "body_excerpt": row["body_excerpt"],
        "status": row["status"],
        "blocked_by_ref": row["blocked_by_ref"],
        "search": {
            "text": row["search_text"],
            "metadata": _json_value(row["search_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
        },
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_discussion(row: Any, conn: Any | None = None) -> dict[str, Any]:
    document = _work_discussion_document(conn, row) if conn is not None else None
    payload = {
        "discussion_id": row["discussion_id"],
        "item_id": row["item_id"],
        "author": row["author"],
        "body_excerpt": row["body_excerpt"],
        "body": document["body"] if document else row["body_excerpt"],
        "status": row["status"],
        "search": {
            "text": row["search_text"],
            "metadata": _json_value(row["search_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
        },
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if document:
        payload["document"] = document
        payload["file_refs"] = [document["file_ref"]]
    return payload


def _event_to_task(row: Any) -> dict[str, Any]:
    event = _row_to_event(row)
    tags = event.get("tags") if isinstance(event.get("tags"), list) else []
    related = event.get("related") if isinstance(event.get("related"), dict) else {}
    related_kanban = (
        related.get("kanban_items") if isinstance(related.get("kanban_items"), list) else []
    )
    mode = "kanban" if event["source"]["type"] == "kanban" or related_kanban else "personal"
    if event["status"] == "blocked":
        mode = "blocked"
    elif event["status"] == "pending_review" or "review" in tags:
        mode = "review"
    elif event["status"] in {"done", "completed", "archived"}:
        mode = "done"
    return {
        "task_id": event["event_id"],
        "event_id": event["event_id"],
        "kind": event["kind"],
        "title": event["title"],
        "body_excerpt": event["body_excerpt"],
        "status": event["status"],
        "mode": mode,
        "priority": event["priority"],
        "due_at": event["start_at"],
        "local_date": event["local_date"],
        "timezone": event["timezone"],
        "privacy_level": event["privacy_level"],
        "tags": tags,
        "source": {
            **event["source"],
            "authority": "event",
        },
        "related": related,
        "file_refs": event["file_refs"],
        "db_refs": event["db_refs"],
        "provenance": event["provenance"],
        "created_at": event["created_at"],
        "updated_at": event["updated_at"],
        "completed_at": event["updated_at"] if event["status"] in {"done", "completed"} else None,
        "archived_at": event["updated_at"] if event["status"] == "archived" else None,
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


PERSONAL_PRIVACY_LEVELS = {"normal", "pin", "vault"}


def _clean_privacy_level(value: str | None) -> str:
    privacy_level = _clean_short_text(value, "normal", limit=40)
    if privacy_level not in PERSONAL_PRIVACY_LEVELS:
        raise HTTPException(400, "privacy level is invalid")
    return privacy_level


def _append_personal_privacy_list_filter(
    where: list[str],
    params: list[Any],
    privacy_level: str | None,
    *,
    column: str = "privacy_level",
) -> str | None:
    if not privacy_level:
        where.append(f"{column} != 'pin'")
        return None
    privacy_filter = _clean_privacy_level(privacy_level)
    if privacy_filter == "pin":
        where.append("1 = 0")
        return privacy_filter
    where.append(f"{column} = ?")
    params.append(privacy_filter)
    return privacy_filter


def _diary_day_dir(local_date: str) -> Path:
    year, month, day = local_date.split("-")
    return DIARY_ROOT / year / month / day


def _diary_relative_path(path: Path) -> str:
    resolved = Path(path).resolve()
    root = DIARY_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("diary path is outside diary root")
    return resolved.relative_to(root).as_posix()


def _kanban_relative_path(path: Path) -> str:
    resolved = Path(path).resolve()
    root = KANBAN_ROOT.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("kanban path is outside kanban root")
    return resolved.relative_to(root).as_posix()


def _safe_kanban_slug(value: str | None, default: str = "item") -> str:
    clean = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    clean = re.sub(r"-{2,}", "-", clean)
    return clean or default


def _normalise_markdown_document_body(value: str | None) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n")


def _kanban_markdown_text(schema: str, metadata: dict[str, Any], body: str) -> str:
    frontmatter = {
        "schema": schema,
        **metadata,
    }
    return (
        "---\n"
        f"{json.dumps(frontmatter, ensure_ascii=False, sort_keys=True, indent=2)}\n"
        "---\n\n"
        f"{_normalise_markdown_document_body(body)}"
    )


def _split_kanban_markdown_text(text: str) -> tuple[dict[str, Any], str]:
    if not text.startswith("---\n"):
        return {}, text
    end = text.find("\n---", 4)
    if end < 0:
        return {}, text
    frontmatter_text = text[4:end].strip()
    body = text[end + len("\n---") :]
    if body.startswith("\r\n"):
        body = body[2:]
    elif body.startswith("\n"):
        body = body[1:]
    if body.startswith("\n"):
        body = body[1:]
    metadata = {}
    with suppress(json.JSONDecodeError, TypeError):
        parsed = json.loads(frontmatter_text)
        if isinstance(parsed, dict):
            metadata = parsed
    return metadata, body


def _read_kanban_markdown_document(path: Path) -> tuple[dict[str, Any], str, bool]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}, "", False
    metadata, body = _split_kanban_markdown_text(text)
    return metadata, body, True


def _rich_doc_image_domain_root(domain: str) -> tuple[str, Path]:
    clean = _clean_short_text(domain, "diary", limit=40).lower()
    if clean in {"personal", "diary", "calendar", "todo"}:
        return "diary", DIARY_ROOT
    if clean == "kanban":
        return "kanban", KANBAN_ROOT
    raise HTTPException(400, "rich document image domain is invalid")


def _rich_doc_image_media_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".gif": "image/gif",
        ".jpeg": "image/jpeg",
        ".jpg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(suffix, "application/octet-stream")


def _safe_rich_doc_token(value: str | None, default: str = "document", limit: int = 120) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", str(value or "").strip()).strip("-")
    clean = re.sub(r"-{2,}", "-", clean)
    return (clean or default)[:limit]


def _safe_rich_doc_image_filename(filename: str | None) -> str:
    raw = Path(str(filename or "image.png")).name
    stem = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", Path(raw).stem).strip("-")[:90] or "image"
    suffix = Path(raw).suffix.lower()
    if suffix not in RICH_DOC_IMAGE_SUFFIXES:
        raise HTTPException(400, "uploaded picture type is not supported")
    return f"{stem}{suffix}"


def _unique_rich_doc_path(directory: Path, filename: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    candidate = directory / filename
    counter = 2
    while candidate.exists():
        candidate = directory / f"{stem}-{counter}{suffix}"
        counter += 1
    return candidate


def _rich_doc_image_uri(domain: str, rel_path: str) -> str:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    clean_path = posixpath.normpath(str(rel_path).replace("\\", "/")).lstrip("/")
    return f"blueprints://rich-doc-image/{clean_domain}/{clean_path}"


def _rich_doc_image_url(domain: str, rel_path: str) -> str:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    clean_path = posixpath.normpath(str(rel_path).replace("\\", "/")).lstrip("/")
    return f"/api/v1/personal/rich-doc/images/file/{clean_domain}/{clean_path}"


def _rich_doc_sidecar_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.json")


def _read_rich_doc_image_metadata(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(_rich_doc_sidecar_path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _rich_doc_linked_document(
    *,
    document_type: str,
    document_id: str,
    local_date: str = "",
    item_id: str = "",
    discussion_id: str = "",
) -> dict[str, Any]:
    return {
        "document_type": _safe_rich_doc_token(document_type, "document"),
        "document_id": _safe_rich_doc_token(document_id, "document", limit=180),
        "local_date": local_date,
        "item_id": _safe_rich_doc_token(item_id, "", limit=180) if item_id else "",
        "discussion_id": _safe_rich_doc_token(discussion_id, "", limit=180)
        if discussion_id
        else "",
    }


def _rich_doc_same_link(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return all(
        str(left.get(key) or "") == str(right.get(key) or "")
        for key in ("document_type", "document_id", "local_date", "item_id", "discussion_id")
    )


def _rich_doc_referenced_image_refs(markdown: str) -> list[tuple[str, str]]:
    refs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for domain, rel_path in re.findall(
        r"blueprints://rich-doc-image/([a-zA-Z0-9_-]+)/([^\s)]+)",
        markdown or "",
    ):
        try:
            clean_domain, _root = _rich_doc_image_domain_root(domain)
        except HTTPException:
            continue
        clean_rel = posixpath.normpath(str(rel_path).replace("\\", "/")).lstrip("/")
        if clean_rel.startswith("../") or clean_rel == "..":
            continue
        key = (clean_domain, clean_rel)
        if key not in seen:
            refs.append(key)
            seen.add(key)
    return refs


def _associate_rich_doc_images_for_document(
    *,
    domain: str,
    markdown: str,
    document_type: str,
    document_id: str,
    local_date: str = "",
    item_id: str = "",
    discussion_id: str = "",
    actor: str = "blueprints-ui",
    source_surface: str = "rich-document-editor",
) -> dict[str, Any]:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    doc = _rich_doc_linked_document(
        document_type=document_type,
        document_id=document_id,
        local_date=local_date,
        item_id=item_id,
        discussion_id=discussion_id,
    )
    associated: list[dict[str, Any]] = []
    for uri_domain, rel_path in _rich_doc_referenced_image_refs(markdown):
        if uri_domain != clean_domain:
            continue
        _same_domain, root = _rich_doc_image_domain_root(uri_domain)
        try:
            path = (root / rel_path).resolve()
            path.relative_to(root.resolve())
        except ValueError:
            continue
        if (
            not path.exists()
            or not path.is_file()
            or path.suffix.lower() not in RICH_DOC_IMAGE_SUFFIXES
        ):
            continue
        meta = _read_rich_doc_image_metadata(path)
        links = [link for link in meta.get("linked_documents", []) if isinstance(link, dict)]
        if any(_rich_doc_same_link(link, doc) for link in links):
            associated.append(_rich_doc_image_record(uri_domain, path, meta))
            continue
        now = _utc_now_iso()
        if not meta:
            rel = (
                _kanban_relative_path(path)
                if uri_domain == "kanban"
                else _diary_relative_path(path)
            )
            meta = {
                "schema": RICH_DOC_IMAGE_SCHEMA,
                "image_id": f"image-{uuid.uuid4().hex[:16]}",
                "domain": uri_domain,
                "path": rel,
                "original_filename": path.name,
                "content_type": _rich_doc_image_media_type(path),
                "linked_documents": [],
                "actor": _clean_short_text(actor, "blueprints-ui"),
                "source_surface": _clean_short_text(source_surface, "rich-document-editor"),
                "created_at": now,
            }
            links = []
        links.append(doc)
        meta["linked_documents"] = links
        meta["updated_at"] = now
        _write_rich_doc_image_metadata(path, meta)
        associated.append(_rich_doc_image_record(uri_domain, path, meta))
    return {
        "ok": True,
        "domain": clean_domain,
        "document": doc,
        "images": associated,
        "count": len(associated),
    }


def _rich_doc_image_record(
    domain: str, path: Path, metadata: dict[str, Any] | None = None
) -> dict[str, Any]:
    clean_domain, root = _rich_doc_image_domain_root(domain)
    rel_path = (
        _diary_relative_path(path) if clean_domain == "diary" else _kanban_relative_path(path)
    )
    meta = metadata if metadata is not None else _read_rich_doc_image_metadata(path)
    stat = path.stat()
    image_id = str(
        meta.get("image_id") or hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:24]
    )
    return {
        "image_id": image_id,
        "domain": clean_domain,
        "path": rel_path,
        "filename": path.name,
        "size_bytes": int(stat.st_size),
        "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "content_type": meta.get("content_type") or _rich_doc_image_media_type(path),
        "url": _rich_doc_image_url(clean_domain, rel_path),
        "uri": _rich_doc_image_uri(clean_domain, rel_path),
        "markdown": f"![{Path(path.name).stem}]({_rich_doc_image_uri(clean_domain, rel_path)})",
        "metadata": meta,
        "linked_documents": meta.get("linked_documents", []),
    }


def _rich_doc_image_sidecar_payload(
    *,
    domain: str,
    path: Path,
    original_filename: str,
    content_type: str,
    document_type: str,
    document_id: str,
    local_date: str,
    item_id: str,
    discussion_id: str,
    actor: str,
    source_surface: str,
    now: str,
) -> dict[str, Any]:
    clean_domain = _rich_doc_image_domain_root(domain)[0]
    rel_path = (
        _diary_relative_path(path) if clean_domain == "diary" else _kanban_relative_path(path)
    )
    doc = _rich_doc_linked_document(
        document_type=document_type,
        document_id=document_id,
        local_date=local_date,
        item_id=item_id,
        discussion_id=discussion_id,
    )
    return {
        "schema": RICH_DOC_IMAGE_SCHEMA,
        "image_id": f"image-{uuid.uuid4().hex[:16]}",
        "domain": clean_domain,
        "path": rel_path,
        "original_filename": original_filename,
        "content_type": content_type,
        "linked_documents": [doc],
        "actor": _clean_short_text(actor, "blueprints-ui"),
        "source_surface": _clean_short_text(source_surface, "rich-document-editor"),
        "created_at": now,
        "updated_at": now,
    }


def _write_rich_doc_image_metadata(path: Path, payload: dict[str, Any]) -> None:
    _rich_doc_sidecar_path(path).write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _personal_document_image_dir(
    *,
    document_type: str,
    document_id: str,
    local_date: str,
) -> Path:
    clean_type = _safe_rich_doc_token(document_type, "document")
    clean_id = _safe_rich_doc_token(document_id, "document", limit=180)
    clean_date = (
        _validate_local_date(local_date) if local_date else datetime.now().strftime("%Y-%m-%d")
    )
    with suppress(Exception):
        with get_conn() as conn:
            if clean_type in {"todo", "todo-task", "task"} and clean_id:
                row = conn.execute(
                    "SELECT file_refs_json, local_date FROM personal_time_tasks WHERE task_id=?",
                    (clean_id,),
                ).fetchone()
                refs = _json_value(row["file_refs_json"], []) if row else []
                for ref in refs:
                    candidate = (DIARY_ROOT / str(ref)).resolve()
                    if candidate.suffix.lower() == ".md":
                        candidate.relative_to(DIARY_ROOT.resolve())
                        return candidate.parent / "images"
            if clean_type in {"diary", "diary-entry", "calendar", "calendar-event"} and clean_id:
                row = conn.execute(
                    "SELECT file_refs_json, local_date FROM personal_events WHERE event_id=?",
                    (clean_id,),
                ).fetchone()
                refs = _json_value(row["file_refs_json"], []) if row else []
                for ref in refs:
                    candidate = (DIARY_ROOT / str(ref)).resolve()
                    if candidate.suffix.lower() in {".md", ".markdown", ".txt"}:
                        candidate.relative_to(DIARY_ROOT.resolve())
                        return candidate.parent / "images"
                if row and row["local_date"]:
                    clean_date = _validate_local_date(row["local_date"])
    year, month, day = clean_date.split("-")
    return DIARY_ROOT / "rich-documents" / clean_type / year / month / day / clean_id / "images"


def _kanban_document_image_dir(
    *,
    document_type: str,
    document_id: str,
    item_id: str,
    discussion_id: str,
) -> Path:
    clean_type = _safe_rich_doc_token(document_type, "document")
    clean_doc = _safe_rich_doc_token(document_id or discussion_id or item_id, "document", limit=180)
    return KANBAN_ROOT / "images" / clean_type / clean_doc


def _rich_doc_image_upload_dir(
    *,
    domain: str,
    document_type: str,
    document_id: str,
    local_date: str,
    item_id: str,
    discussion_id: str,
) -> Path:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    if clean_domain == "kanban":
        return _kanban_document_image_dir(
            document_type=document_type,
            document_id=document_id,
            item_id=item_id,
            discussion_id=discussion_id,
        )
    return _personal_document_image_dir(
        document_type=document_type,
        document_id=document_id,
        local_date=local_date,
    )


def _rich_doc_image_matches(
    record: dict[str, Any],
    *,
    document_type: str = "",
    document_id: str = "",
    item_id: str = "",
    discussion_id: str = "",
    q: str = "",
) -> bool:
    query = q.strip().lower()
    if query and query not in record["filename"].lower() and query not in record["path"].lower():
        return False
    doc_type = _safe_rich_doc_token(document_type, "", limit=120) if document_type else ""
    doc_id = _safe_rich_doc_token(document_id, "", limit=180) if document_id else ""
    clean_item = _safe_rich_doc_token(item_id, "", limit=180) if item_id else ""
    clean_discussion = _safe_rich_doc_token(discussion_id, "", limit=180) if discussion_id else ""
    if not any((doc_type, doc_id, clean_item, clean_discussion)):
        return True
    for linked in record.get("linked_documents") or []:
        if not isinstance(linked, dict):
            continue
        if doc_type and linked.get("document_type") != doc_type:
            continue
        if doc_id and linked.get("document_id") != doc_id:
            continue
        if clean_item and linked.get("item_id") != clean_item:
            continue
        if clean_discussion and linked.get("discussion_id") != clean_discussion:
            continue
        return True
    return False


def _iter_rich_doc_image_records(domain: str) -> list[dict[str, Any]]:
    clean_domain, root = _rich_doc_image_domain_root(domain)
    scan_root = root / "images" if clean_domain == "kanban" else root
    if not scan_root.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in scan_root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in RICH_DOC_IMAGE_SUFFIXES:
            continue
        with suppress(Exception):
            records.append(_rich_doc_image_record(clean_domain, path))
    records.sort(key=lambda item: item.get("updated_at") or "", reverse=True)
    return records


@router.get("/rich-doc/images")
async def list_rich_doc_images(
    domain: str = Query("diary"),
    document_type: str = Query(""),
    document_id: str = Query(""),
    item_id: str = Query(""),
    discussion_id: str = Query(""),
    q: str = Query(""),
    limit: int = Query(120, ge=1, le=500),
) -> dict[str, Any]:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    if clean_domain == "kanban":
        records = [
            record
            for record in _iter_rich_doc_image_records(clean_domain)
            if _rich_doc_image_matches(record, q=q)
        ][:limit]
        return {
            "ok": True,
            "domain": clean_domain,
            "scope": "central",
            "images": records,
            "count": len(records),
        }
    records = [
        record
        for record in _iter_rich_doc_image_records(clean_domain)
        if _rich_doc_image_matches(
            record,
            document_type=document_type,
            document_id=document_id,
            item_id=item_id,
            discussion_id=discussion_id,
            q=q,
        )
    ][:limit]
    return {
        "ok": True,
        "domain": clean_domain,
        "images": records,
        "count": len(records),
    }


@router.post("/rich-doc/images/upload")
async def upload_rich_doc_image(
    file: UploadFile = File(...),
    domain: str = Form("diary"),
    document_type: str = Form("document"),
    document_id: str = Form(""),
    local_date: str = Form(""),
    item_id: str = Form(""),
    discussion_id: str = Form(""),
    actor: str = Form("blueprints-ui"),
    source_surface: str = Form("rich-document-editor"),
) -> dict[str, Any]:
    clean_domain, root = _rich_doc_image_domain_root(domain)
    safe_name = _safe_rich_doc_image_filename(file.filename)
    content_type = file.content_type or _rich_doc_image_media_type(Path(safe_name))
    target_dir = _rich_doc_image_upload_dir(
        domain=clean_domain,
        document_type=document_type,
        document_id=document_id,
        local_date=local_date,
        item_id=item_id,
        discussion_id=discussion_id,
    )
    target = _unique_rich_doc_path(target_dir, safe_name)
    resolved_target = target.resolve()
    resolved_target.relative_to(root.resolve())
    data = await file.read(RICH_DOC_IMAGE_MAX_BYTES + 1)
    if len(data) > RICH_DOC_IMAGE_MAX_BYTES:
        raise HTTPException(400, "uploaded picture is too large")
    if not data:
        raise HTTPException(400, "uploaded picture is empty")
    target.write_bytes(data)
    now = _utc_now_iso()
    metadata = _rich_doc_image_sidecar_payload(
        domain=clean_domain,
        path=target,
        original_filename=file.filename or safe_name,
        content_type=content_type,
        document_type=document_type,
        document_id=document_id,
        local_date=local_date,
        item_id=item_id,
        discussion_id=discussion_id,
        actor=actor,
        source_surface=source_surface,
        now=now,
    )
    _write_rich_doc_image_metadata(target, metadata)
    record = _rich_doc_image_record(clean_domain, target, metadata)
    return {"ok": True, "image": record}


@router.post("/rich-doc/images/associate")
async def associate_rich_doc_images(body: RichDocImageAssociateRequest) -> dict[str, Any]:
    return _associate_rich_doc_images_for_document(
        domain=body.domain,
        markdown=body.markdown,
        document_type=body.document_type,
        document_id=body.document_id,
        local_date=body.local_date,
        item_id=body.item_id,
        discussion_id=body.discussion_id,
        actor=body.actor,
        source_surface=body.source_surface,
    )


@router.get("/rich-doc/images/file/{domain}/{rel_path:path}")
async def serve_rich_doc_image(domain: str, rel_path: str) -> FileResponse:
    clean_domain, root = _rich_doc_image_domain_root(domain)
    clean_rel = posixpath.normpath(str(rel_path).replace("\\", "/")).lstrip("/")
    if clean_rel.startswith("../") or clean_rel == "..":
        raise HTTPException(400, "image path is invalid")
    path = (root / clean_rel).resolve()
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise HTTPException(400, "image path is outside document root") from exc
    if (
        not path.exists()
        or not path.is_file()
        or path.suffix.lower() not in RICH_DOC_IMAGE_SUFFIXES
    ):
        raise HTTPException(404, "image not found")
    return FileResponse(str(path), media_type=_rich_doc_image_media_type(path))


@router.get("/rich-doc/documents/{domain}/{document_type}/{document_id}/bundle")
async def get_rich_doc_bundle(
    domain: str,
    document_type: str,
    document_id: str,
) -> dict[str, Any]:
    clean_domain, _root = _rich_doc_image_domain_root(domain)
    clean_type = _safe_rich_doc_token(document_type, "document")
    clean_id = _safe_rich_doc_token(document_id, "document", limit=180)
    document: dict[str, Any] = {
        "domain": clean_domain,
        "document_type": clean_type,
        "document_id": clean_id,
        "body": "",
        "file_ref": {},
        "metadata": {},
    }
    with get_conn() as conn:
        if clean_domain == "kanban" and clean_type in {"body", "item-body", "kanban-item"}:
            row = _work_item_or_404(conn, clean_id)
            document.update(
                {
                    "document_type": "item-body",
                    "body": row["body_excerpt"] or "",
                    "metadata": _json_value(row["provenance_json"], {}),
                    "updated_at": row["updated_at"],
                }
            )
        elif clean_domain == "kanban" and clean_type in {"detail", "item-detail", "kanban-detail"}:
            document.update(_work_item_detail_document(conn, clean_id))
            document["document_type"] = "item-detail"
        elif clean_domain == "kanban" and clean_type in {"discussion", "kanban-discussion"}:
            row = conn.execute(
                "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_id,)
            ).fetchone()
            if not row:
                raise HTTPException(404, "Kanban discussion not found")
            document.update(_work_discussion_document(conn, row))
            document["document_type"] = "discussion"
        elif clean_domain == "kanban" and clean_type in {"issue", "kanban-issue"}:
            row = _work_typed_leaf_item_row(conn, "issue", clean_id)
            if not row:
                raise HTTPException(404, "kanban issue not found")
            document.update(
                {
                    "document_type": "issue",
                    "body": row["body_excerpt"] or "",
                    "item_id": row["parent_item_id"] or "",
                    "metadata": _json_value(row["provenance_json"], {}),
                    "updated_at": row["updated_at"],
                }
            )
        elif clean_domain == "kanban" and clean_type in {"todo", "kanban-todo"}:
            row = _work_typed_leaf_item_row(conn, "todo", clean_id)
            if not row:
                raise HTTPException(404, "kanban todo not found")
            document.update(
                {
                    "document_type": "todo",
                    "body": row["body_excerpt"] or "",
                    "item_id": row["parent_item_id"] or "",
                    "metadata": _json_value(row["provenance_json"], {}),
                    "updated_at": row["updated_at"],
                }
            )
        elif clean_domain == "diary" and clean_type in {"todo", "todo-task", "task"}:
            row = conn.execute(
                "SELECT * FROM personal_time_tasks WHERE task_id=?", (clean_id,)
            ).fetchone()
            if not row:
                raise HTTPException(404, "task not found")
            document.update(
                {
                    "document_type": "todo-task",
                    "body": row["body_excerpt"] or "",
                    "file_refs": _json_value(row["file_refs_json"], []),
                    "metadata": _json_value(row["provenance_json"], {}),
                    "updated_at": row["updated_at"],
                }
            )
        elif clean_domain == "diary":
            row = conn.execute(
                "SELECT * FROM personal_events WHERE event_id=?", (clean_id,)
            ).fetchone()
            if not row:
                raise HTTPException(404, "personal event not found")
            document.update(
                {
                    "document_type": clean_type,
                    "body": row["content_projection"] or row["body_excerpt"] or "",
                    "file_refs": _json_value(row["file_refs_json"], []),
                    "metadata": _json_value(row["provenance_json"], {}),
                    "updated_at": row["updated_at"],
                }
            )
    records = [
        record
        for record in _iter_rich_doc_image_records(clean_domain)
        if _rich_doc_image_matches(
            record,
            document_type=document["document_type"],
            document_id=clean_id,
        )
    ]
    referenced_uris = set(
        re.findall(
            r"blueprints://rich-doc-image/([a-zA-Z0-9_-]+)/([^\s)]+)", document.get("body") or ""
        )
    )
    if referenced_uris:
        seen = {record["uri"] for record in records}
        for uri_domain, uri_path in referenced_uris:
            try:
                record_path = (_rich_doc_image_domain_root(uri_domain)[1] / uri_path).resolve()
                record = _rich_doc_image_record(uri_domain, record_path)
            except Exception:
                continue
            if record["uri"] not in seen:
                records.append(record)
                seen.add(record["uri"])
    return {
        "ok": True,
        "document": document,
        "images": records,
        "count": len(records),
    }


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
    # Compact summaries may still be rendered as markdown, so preserve line breaks.
    text = _normalise_markdown_document_body(body).strip()
    return "\n".join(line.rstrip() for line in text.split("\n"))[:limit]


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


def _upsert_task_source(conn: Any, now: str) -> Any:
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
            "manual-task",
            "manual-task",
            "Manual Tasks",
            "ok",
            now,
            json.dumps({"write_path": "personal_time_tasks"}),
            json.dumps({"tasks_table": "personal_time_tasks", "events_table": "personal_events"}),
            now,
            now,
        ),
    )
    return conn.execute("SELECT * FROM personal_sources WHERE source_id='manual-task'").fetchone()


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
    elif mode == "kanban":
        where.append("(source_type = 'kanban' OR json_array_length(related_kanban_items_json) > 0)")
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


PERSONAL_GRAPH_LINK_TYPES = {
    "relates_to",
    "source_for",
    "summarizes",
    "evidence_for",
    "created_from",
    "promoted_from",
    "blocks",
    "depends_on",
    "documents",
    "implements",
    "same_day_as",
}
PERSONAL_GRAPH_LINK_TYPE_ALIASES = {
    "related": "relates_to",
    "references": "relates_to",
    "duplicate": "relates_to",
    "duplicates": "relates_to",
    "split_from": "created_from",
}
PERSONAL_GRAPH_LINK_STATES = {"declared", "accepted", "needs_review", "inferred", "rejected"}
PERSONAL_GRAPH_RISK_LEVELS = {"normal", "review", "sensitive", "blocked"}


def _clean_graph_ref(value: Any, *, limit: int = 260) -> str:
    return _clean_short_text(str(value or ""), "", limit=limit)


def _clean_graph_link_type(value: Any) -> str:
    link_type = _clean_short_text(str(value or ""), "relates_to", limit=60).replace("-", "_")
    link_type = PERSONAL_GRAPH_LINK_TYPE_ALIASES.get(link_type, link_type)
    if link_type not in PERSONAL_GRAPH_LINK_TYPES:
        raise HTTPException(400, "personal graph link type is invalid")
    return link_type


def _clean_graph_link_state(value: Any, *, default: str = "declared") -> str:
    state = _clean_short_text(str(value or ""), default, limit=40).replace("-", "_")
    if state not in PERSONAL_GRAPH_LINK_STATES:
        raise HTTPException(400, "personal graph link state is invalid")
    return "needs_review" if state == "inferred" else state


def _clean_graph_risk_level(value: Any) -> str:
    risk = _clean_short_text(str(value or ""), "normal", limit=40).replace("-", "_")
    return risk if risk in PERSONAL_GRAPH_RISK_LEVELS else "normal"


def _graph_ref_parts(ref: str) -> tuple[str, str]:
    clean = _clean_graph_ref(ref)
    if ":" not in clean:
        return "", clean
    table, record_id = clean.split(":", 1)
    table = {
        "kanban": "kanban_items",
        "work_item": "kanban_items",
        "task": "personal_time_tasks",
        "import": "personal_import_batches",
        "doc": "docs",
        "file": "files",
        "browser_link": "browser_links",
    }.get(table, table)
    return table, record_id


def _target_ref(default_table: str, value: Any) -> str:
    clean = _clean_graph_ref(value)
    if not clean:
        return ""
    if ":" in clean:
        table, record_id = _graph_ref_parts(clean)
        return f"{table}:{record_id}" if table else record_id
    return f"{default_table}:{clean}" if default_table else clean


def _graph_link_id(source_ref: str, link_type: str, target_ref: str) -> str:
    digest = hashlib.sha256(f"{source_ref}\n{link_type}\n{target_ref}".encode()).hexdigest()
    return f"graph-{digest[:24]}"


def _graph_link_row_payload(
    *,
    source_ref: str,
    target_ref: str,
    link_type: str,
    link_state: str = "accepted",
    risk_level: str = "normal",
    title: str = "",
    metadata: dict[str, Any] | None = None,
    provenance: dict[str, Any] | None = None,
    actor: str = "blueprints-api",
    request_id: str = "",
    now: str,
) -> dict[str, Any]:
    source_table, source_id = _graph_ref_parts(source_ref)
    target_table, target_id = _graph_ref_parts(target_ref)
    clean_type = _clean_graph_link_type(link_type)
    clean_state = _clean_graph_link_state(link_state, default="accepted")
    clean_risk = _clean_graph_risk_level(risk_level)
    return {
        "link_id": _graph_link_id(source_ref, clean_type, target_ref),
        "source_ref": source_ref,
        "source_table": source_table,
        "source_id": source_id,
        "target_ref": target_ref,
        "target_table": target_table,
        "target_id": target_id,
        "link_type": clean_type,
        "link_state": clean_state,
        "risk_level": clean_risk,
        "title": _clean_short_text(title, "", limit=240),
        "metadata_json": json.dumps(metadata or {}, ensure_ascii=True, sort_keys=True),
        "provenance_json": json.dumps(provenance or {}, ensure_ascii=True, sort_keys=True),
        "created_by": _clean_short_text(actor, "blueprints-api", limit=120),
        "request_id": _clean_short_text(request_id, "", limit=160),
        "created_at": now,
        "updated_at": now,
    }


def _row_to_personal_graph_link(row: Any) -> dict[str, Any]:
    return {
        "link_id": row["link_id"],
        "source_ref": row["source_ref"],
        "source": {"table": row["source_table"], "id": row["source_id"]},
        "target_ref": row["target_ref"],
        "target": {"table": row["target_table"], "id": row["target_id"]},
        "link_type": row["link_type"],
        "link_state": row["link_state"],
        "risk_level": row["risk_level"],
        "title": row["title"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_by": row["created_by"],
        "request_id": row["request_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _upsert_graph_link(conn: Any, payload: dict[str, Any]) -> str:
    existing = conn.execute(
        "SELECT * FROM personal_graph_links WHERE link_id=?", (payload["link_id"],)
    ).fetchone()
    if existing:
        semantic_fields = [
            "source_ref",
            "source_table",
            "source_id",
            "target_ref",
            "target_table",
            "target_id",
            "link_type",
            "link_state",
            "risk_level",
            "title",
            "metadata_json",
            "provenance_json",
        ]
        if all(str(existing[key] or "") == str(payload[key] or "") for key in semantic_fields):
            return "unchanged"
        payload = {**payload, "created_at": existing["created_at"]}
        result = "updated"
    else:
        result = "inserted"
    conn.execute(
        """
        INSERT INTO personal_graph_links (
            link_id, source_ref, source_table, source_id, target_ref, target_table,
            target_id, link_type, link_state, risk_level, title, metadata_json,
            provenance_json, created_by, request_id, created_at, updated_at
        )
        VALUES (
            :link_id, :source_ref, :source_table, :source_id, :target_ref,
            :target_table, :target_id, :link_type, :link_state, :risk_level,
            :title, :metadata_json, :provenance_json, :created_by, :request_id,
            :created_at, :updated_at
        )
        ON CONFLICT(link_id) DO UPDATE SET
            source_ref=excluded.source_ref,
            source_table=excluded.source_table,
            source_id=excluded.source_id,
            target_ref=excluded.target_ref,
            target_table=excluded.target_table,
            target_id=excluded.target_id,
            link_type=excluded.link_type,
            link_state=excluded.link_state,
            risk_level=excluded.risk_level,
            title=excluded.title,
            metadata_json=excluded.metadata_json,
            provenance_json=excluded.provenance_json,
            created_by=excluded.created_by,
            request_id=excluded.request_id,
            updated_at=excluded.updated_at
        """,
        payload,
    )
    return result


def _walk_json(value: Any):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key, item
            yield from _walk_json(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_json(item)


def _matrix_pointer_refs(provenance: Any) -> list[str]:
    refs: list[str] = []
    for key, value in _walk_json(provenance):
        if key in {"matrix_event_id", "source_event_id"} and value:
            refs.append(f"matrix_event:{value}")
        elif key == "matrix_event_ids" and isinstance(value, list):
            refs.extend(f"matrix_event:{item}" for item in value if item)
        elif key == "conversation_key" and value:
            refs.append(f"matrix_minutes:{value}")
        elif key == "wake_route_record_ids" and isinstance(value, list):
            refs.extend(f"wake_route:{item}" for item in value if item)
        elif key == "tts_utterance_ids" and isinstance(value, list):
            refs.extend(f"tts_utterance:{item}" for item in value if item)
    deduped: list[str] = []
    for ref in refs:
        clean = _clean_graph_ref(ref)
        if clean and clean not in deduped:
            deduped.append(clean)
    return deduped


def _proof_doc_refs(provenance: Any) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    pending = [provenance]
    while pending:
        value = pending.pop()
        if not isinstance(value, dict):
            if isinstance(value, list):
                pending.extend(value)
            continue
        path = _clean_graph_ref(value.get("path") or value.get("doc_path") or value.get("file_ref"))
        if path:
            label = _clean_short_text(
                value.get("label") or value.get("title") or path, path, limit=240
            )
            refs.append(
                {"ref": f"docs:{path}" if path.endswith(".md") else f"files:{path}", "label": label}
            )
        pending.extend(value.values())
    deduped: dict[str, dict[str, str]] = {}
    for ref in refs:
        deduped.setdefault(ref["ref"], ref)
    return list(deduped.values())


def _add_graph_candidate(
    candidates: dict[str, dict[str, Any]],
    *,
    source_ref: str,
    target_ref: str,
    link_type: str,
    title: str,
    metadata: dict[str, Any],
    provenance: dict[str, Any],
    actor: str,
    request_id: str,
    now: str,
    link_state: str = "accepted",
    risk_level: str = "normal",
) -> None:
    source_ref = _clean_graph_ref(source_ref)
    target_ref = _clean_graph_ref(target_ref)
    if not source_ref or not target_ref or source_ref == target_ref:
        return
    payload = _graph_link_row_payload(
        source_ref=source_ref,
        target_ref=target_ref,
        link_type=link_type,
        link_state=link_state,
        risk_level=risk_level,
        title=title,
        metadata=metadata,
        provenance=provenance,
        actor=actor,
        request_id=request_id,
        now=now,
    )
    candidates[payload["link_id"]] = payload


def _personal_graph_candidates(
    conn: Any,
    *,
    actor: str,
    request_id: str,
    now: str,
) -> dict[str, dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}

    for row in conn.execute("SELECT * FROM personal_events WHERE privacy_level != 'pin'"):
        source_ref = f"personal_events:{row['event_id']}"
        provenance = _json_value(row["provenance_json"], {})
        base = {
            "source_table": "personal_events",
            "source_hash": row["source_hash"] or "",
            "source_type": row["source_type"] or "",
            "source_ref": row["source_ref"] or "",
            "provenance_state": row["provenance_state"] or "",
        }
        for ref in _json_value(row["related_kanban_items_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("kanban_items", str(ref)),
                link_type="relates_to",
                title="Diary event relates to Kanban item",
                metadata={"field": "related_kanban_items_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for ref in _json_value(row["related_tasks_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("personal_time_tasks", ref),
                link_type="relates_to",
                title="Diary event relates to task",
                metadata={"field": "related_tasks_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for ref in _json_value(row["related_import_batches_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("personal_import_batches", ref),
                link_type="created_from",
                title="Diary event came from import batch",
                metadata={"field": "related_import_batches_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for ref in _json_value(row["db_refs_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("", ref),
                link_type="evidence_for",
                title="Diary event keeps database evidence",
                metadata={"field": "db_refs_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for ref in _json_value(row["file_refs_json"], []):
            target = _target_ref("", ref) if ":" in str(ref) else f"files:{ref}"
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=target,
                link_type="source_for",
                title="Diary event keeps file evidence",
                metadata={"field": "file_refs_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        if row["source_type"] == "git" and row["source_ref"]:
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=f"git_commit:{row['source_ref']}",
                link_type="source_for",
                title="Diary event sourced from git commit",
                metadata={"source_type": "git"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        if row["source_type"] == "browser-links":
            browser_ref = row["source_ref"] or row["event_id"]
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=f"browser_links:{browser_ref}",
                link_type="source_for",
                title="Diary event sourced from Browser Links",
                metadata={"source_type": "browser-links"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        if row["local_date"]:
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=f"diary_day:{row['local_date']}",
                link_type="same_day_as",
                title="Diary event belongs to day",
                metadata={"local_date": row["local_date"]},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for target_ref in _matrix_pointer_refs(provenance):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=target_ref,
                link_type="source_for",
                title="Diary event keeps Matrix Minutes pointer",
                metadata={"field": "provenance_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )

    for row in conn.execute("SELECT * FROM personal_time_tasks WHERE privacy_level != 'pin'"):
        source_ref = f"personal_time_tasks:{row['task_id']}"
        base = {"source_table": "personal_time_tasks", "source_hash": row["source_hash"] or ""}
        for ref in _json_value(row["related_kanban_items_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("kanban_items", str(ref)),
                link_type="relates_to",
                title="Task relates to Kanban item",
                metadata={"field": "related_kanban_items_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )

    for row in conn.execute("SELECT * FROM kanban_items WHERE status != 'archived'"):
        source_ref = f"kanban_items:{row['item_id']}"
        base = {"source_table": "kanban_items", "source_hash": row["source_hash"] or ""}
        for ref in _json_value(row["related_event_ids_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("personal_events", ref),
                link_type="evidence_for",
                title="Kanban item links diary evidence",
                metadata={"field": "related_event_ids_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for ref in _json_value(row["related_task_ids_json"], []):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("personal_time_tasks", ref),
                link_type="evidence_for",
                title="Kanban item links task evidence",
                metadata={"field": "related_task_ids_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        if row["promoted_from_ref"]:
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=_target_ref("", row["promoted_from_ref"]),
                link_type="promoted_from",
                title="Kanban item promoted from source record",
                metadata={"field": "promoted_from_ref"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        if row["item_type"] == "issue" and row["parent_item_id"]:
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=f"kanban_items:{row['parent_item_id']}",
                link_type="evidence_for",
                title="Issue item belongs to parent item",
                metadata={"status": row["status"], "item_type": row["item_type"]},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )

    for row in conn.execute("SELECT * FROM kanban_item_links"):
        _add_graph_candidate(
            candidates,
            source_ref=f"kanban_items:{row['source_item_id']}",
            target_ref=f"kanban_items:{row['target_item_id']}",
            link_type=PERSONAL_GRAPH_LINK_TYPE_ALIASES.get(row["link_type"], row["link_type"]),
            title="Kanban item link",
            metadata={"work_item_link_id": row["link_id"], **_json_value(row["metadata_json"], {})},
            provenance={
                "source_table": "kanban_item_links",
                "db_ref": f"kanban_item_links:{row['link_id']}",
            },
            actor=actor,
            request_id=request_id,
            now=now,
        )

    for row in conn.execute("SELECT * FROM kanban_item_commits"):
        _add_graph_candidate(
            candidates,
            source_ref=_git_commit_ref(row["repo_full_name"], row["sha"]),
            target_ref=f"kanban_items:{row['item_id']}",
            link_type="implements",
            title="Git commit implements Kanban item",
            metadata={
                "commit_link_id": row["commit_link_id"],
                "repo_full_name": row["repo_full_name"],
                "sha": row["sha"],
                "short_sha": row["short_sha"],
                "html_url": row["html_url"],
            },
            provenance={
                "source_table": "kanban_item_commits",
                "db_ref": f"kanban_item_commits:{row['commit_link_id']}",
            },
            actor=actor,
            request_id=request_id,
            now=now,
        )

    for row in conn.execute("SELECT * FROM kanban_blockers"):
        if not row["blocked_by_ref"]:
            continue
        _add_graph_candidate(
            candidates,
            source_ref=f"kanban_blockers:{row['blocker_id']}",
            target_ref=_target_ref("", row["blocked_by_ref"]),
            link_type="blocks",
            title="Blocker cites blocking source",
            metadata={"status": row["status"]},
            provenance={
                "source_table": "kanban_blockers",
                "db_ref": f"kanban_blockers:{row['blocker_id']}",
            },
            actor=actor,
            request_id=request_id,
            now=now,
        )

    for row in conn.execute("SELECT * FROM personal_import_batches WHERE privacy_level != 'pin'"):
        source_ref = f"personal_import_batches:{row['import_batch_id']}"
        provenance = _json_value(row["provenance_json"], {})
        base = {
            "source_table": "personal_import_batches",
            "source_type": row["source_type"],
            "source_ref": row["source_ref"] or "",
        }
        for ref in _json_value(row["artifact_refs_json"], []):
            target = _target_ref("", ref) if ":" in str(ref) else f"files:{ref}"
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=target,
                link_type="evidence_for",
                title="Import batch links artifact",
                metadata={"field": "artifact_refs_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )
        for proof in _proof_doc_refs(provenance):
            _add_graph_candidate(
                candidates,
                source_ref=source_ref,
                target_ref=proof["ref"],
                link_type="documents",
                title=proof["label"],
                metadata={"field": "provenance_json"},
                provenance=base,
                actor=actor,
                request_id=request_id,
                now=now,
            )

    return candidates


PERSONAL_SEARCH_PROJECT = "personal-time-activity"
PERSONAL_SEARCH_VECTOR_DIM = 2048


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _search_document_id(record_table: str, record_id: str) -> str:
    return f"{record_table}:{record_id}"


def _search_hash(payload: dict[str, Any]) -> str:
    return _hash_json_payload(payload)


def _search_tags(row: Any) -> list[str]:
    return [str(item) for item in _as_list(_json_value(row["tags_json"], [])) if str(item)]


def _search_related_refs(*values: Any) -> list[str]:
    refs: list[str] = []
    for value in values:
        for item in _as_list(value):
            text = _clean_short_text(str(item), "", limit=240)
            if text and text not in refs:
                refs.append(text)
    return refs


def _event_search_record_type(row: Any) -> str:
    source_type = str(row["source_type"] or "")
    kind = str(row["kind"] or "")
    if source_type == "manual-calendar" or kind.startswith("calendar"):
        return "calendar"
    if source_type == "git" or kind.startswith("git"):
        return "git"
    if source_type in {"manual", "diary-file"} or kind in {"entry", "personal-log"}:
        return "diary"
    if kind == "task":
        return "task_event"
    return "timeline"


def _event_search_mode(row: Any, record_type: str) -> str:
    if record_type == "calendar":
        return "calendar"
    if record_type == "git":
        return "git_activity"
    if record_type == "task_event":
        return "kanban" if _json_value(row["related_kanban_items_json"], []) else "personal"
    if row["status"] == "blocked":
        return "blocked"
    if row["status"] == "pending_review":
        return "review"
    return "personal"


def _event_page_ref(row: Any, record_type: str) -> dict[str, Any]:
    if record_type == "calendar":
        return {"group": "dave", "tab": "calendar", "date": row["local_date"] or ""}
    if record_type == "task_event":
        return {"group": "dave", "tab": "todo", "date": row["local_date"] or ""}
    return {"group": "dave", "tab": "diary", "date": row["local_date"] or ""}


def _search_payload(
    *,
    record_type: str,
    record_table: str,
    record_id: str,
    source_type: str,
    source_ref: str,
    title: str,
    body: str,
    local_date: str | None,
    status: str,
    mode: str,
    privacy_level: str,
    tags: list[str],
    related_refs: list[str],
    page_ref: dict[str, Any],
    source_refs: list[str],
    provenance: dict[str, Any],
    source_hash: str = "",
    updated_at: str | None = None,
) -> dict[str, Any]:
    clean_title = _clean_short_text(title, "", limit=240)
    clean_body = _body_excerpt(body or "", limit=5000)
    search_text = "\n".join(
        part
        for part in [
            clean_title,
            clean_body,
            " ".join(tags),
            source_type,
            source_ref,
            " ".join(related_refs),
        ]
        if part
    )
    document_id = _search_document_id(record_table, record_id)
    hash_input = {
        "record_type": record_type,
        "record_table": record_table,
        "record_id": record_id,
        "source_type": source_type,
        "source_ref": source_ref,
        "title": clean_title,
        "body": clean_body,
        "local_date": local_date,
        "status": status,
        "mode": mode,
        "privacy_level": privacy_level,
        "tags": tags,
        "related_refs": related_refs,
        "page_ref": page_ref,
        "source_refs": source_refs,
    }
    return {
        "document_id": document_id,
        "record_type": record_type,
        "record_table": record_table,
        "record_id": record_id,
        "source_type": source_type,
        "source_ref": source_ref,
        "source_hash": source_hash or _search_hash(hash_input),
        "title": clean_title,
        "body": clean_body,
        "search_text": search_text,
        "local_date": local_date,
        "status": status or "",
        "mode": mode or "",
        "privacy_level": privacy_level or "normal",
        "tags_json": json.dumps(tags, ensure_ascii=True),
        "related_refs_json": json.dumps(related_refs, ensure_ascii=True),
        "page_ref_json": json.dumps(page_ref, ensure_ascii=True, sort_keys=True),
        "source_refs_json": json.dumps(source_refs, ensure_ascii=True),
        "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
        "score_metadata_json": json.dumps(
            {
                "schema": "xarta.personal.search.score_metadata.v1",
                "project": PERSONAL_SEARCH_PROJECT,
                "fts": {"engine": "sqlite-fts5", "rank": "bm25"},
                "embedding": {"dimensions": PERSONAL_SEARCH_VECTOR_DIM},
                "vector": {"engine": "seekdb", "collection": "personal_time_activity_index"},
                "rerank": {"project": PERSONAL_SEARCH_PROJECT},
            },
            ensure_ascii=True,
            sort_keys=True,
        ),
        "vector_index_key": document_id,
        "updated_at": updated_at or _utc_now_iso(),
    }


def _collect_personal_search_documents(conn: Any) -> list[dict[str, Any]]:
    docs: list[dict[str, Any]] = []
    event_rows = conn.execute(
        "SELECT * FROM personal_events WHERE privacy_level != 'pin'"
    ).fetchall()
    for row in event_rows:
        record_type = _event_search_record_type(row)
        tags = _search_tags(row)
        related_refs = _search_related_refs(
            _json_value(row["related_kanban_items_json"], []),
            _json_value(row["related_tasks_json"], []),
            _json_value(row["related_import_batches_json"], []),
            _json_value(row["file_refs_json"], []),
            _json_value(row["db_refs_json"], []),
        )
        docs.append(
            _search_payload(
                record_type=record_type,
                record_table="personal_events",
                record_id=row["event_id"],
                source_type=row["source_type"] or "",
                source_ref=row["source_ref"] or "",
                source_hash=row["source_hash"] or "",
                title=row["title"] or "",
                body=row["content_projection"] or row["body_excerpt"] or "",
                local_date=row["local_date"],
                status=row["status"] or "",
                mode=_event_search_mode(row, record_type),
                privacy_level=row["privacy_level"] or "normal",
                tags=tags,
                related_refs=related_refs,
                page_ref=_event_page_ref(row, record_type),
                source_refs=[f"personal_events:{row['event_id']}", *related_refs],
                provenance=_json_value(row["provenance_json"], {}),
                updated_at=row["updated_at"],
            )
        )

    task_rows = conn.execute(
        "SELECT * FROM personal_time_tasks WHERE privacy_level != 'pin'"
    ).fetchall()
    for row in task_rows:
        tags = _search_tags(row)
        related_refs = _search_related_refs(
            _json_value(row["related_kanban_items_json"], []),
            _json_value(row["related_tasks_json"], []),
            _json_value(row["related_import_batches_json"], []),
            _json_value(row["file_refs_json"], []),
            _json_value(row["db_refs_json"], []),
        )
        docs.append(
            _search_payload(
                record_type="task",
                record_table="personal_time_tasks",
                record_id=row["task_id"],
                source_type=row["source_type"] or "",
                source_ref=row["source_ref"] or "",
                source_hash=row["source_hash"] or "",
                title=row["title"] or "",
                body=row["body_excerpt"] or "",
                local_date=row["local_date"],
                status=row["status"] or "",
                mode=row["mode"] or "personal",
                privacy_level=row["privacy_level"] or "normal",
                tags=tags,
                related_refs=related_refs,
                page_ref={"group": "dave", "tab": "todo", "date": row["local_date"] or ""},
                source_refs=[f"personal_time_tasks:{row['task_id']}", *related_refs],
                provenance=_json_value(row["provenance_json"], {}),
                updated_at=row["updated_at"],
            )
        )

    import_rows = conn.execute(
        "SELECT * FROM personal_import_batches WHERE privacy_level != 'pin'"
    ).fetchall()
    for row in import_rows:
        artifact_refs = _as_list(_json_value(row["artifact_refs_json"], []))
        blocker_refs = _as_list(_json_value(row["blocker_refs_json"], []))
        related_refs = _search_related_refs(artifact_refs, blocker_refs)
        docs.append(
            _search_payload(
                record_type="import",
                record_table="personal_import_batches",
                record_id=row["import_batch_id"],
                source_type=row["source_type"] or "",
                source_ref=row["source_ref"] or "",
                title=row["title"] or "",
                body=" ".join(str(item) for item in [*artifact_refs, *blocker_refs]),
                local_date=row["local_date"],
                status=row["status"] or "",
                mode="imports",
                privacy_level=row["privacy_level"] or "normal",
                tags=["imports", row["source_type"] or ""],
                related_refs=related_refs,
                page_ref={"group": "dave", "tab": "imports", "date": row["local_date"] or ""},
                source_refs=[f"personal_import_batches:{row['import_batch_id']}", *related_refs],
                provenance=_json_value(row["provenance_json"], {}),
                updated_at=row["updated_at"],
            )
        )

    work_specs = [
        ("kanban_items", "item_id", "kanban_item", "manual-kanban"),
        ("kanban_blockers", "blocker_id", "kanban_blocker", "kanban"),
        ("kanban_discussions", "discussion_id", "kanban_discussion", "kanban"),
    ]
    for table, id_col, record_type, default_source in work_specs:
        for row in conn.execute(f"SELECT * FROM {table}").fetchall():
            row_id = row[id_col]
            tags = _json_value(row["tags_json"], []) if "tags_json" in row.keys() else []
            related_refs = []
            if table == "kanban_items":
                related_refs = _search_related_refs(
                    _json_value(row["related_event_ids_json"], []),
                    _json_value(row["related_task_ids_json"], []),
                    _json_value(row["related_issue_ids_json"], []),
                )
                source_type = row["source_type"] or default_source
                source_ref = row["source_ref"] or f"{table}:{row_id}"
                source_hash = row["source_hash"] or ""
                item_id = row["item_id"]
            else:
                related_refs = _search_related_refs(
                    [row["item_id"]] if "item_id" in row.keys() else [],
                    [row["source_ref"]] if "source_ref" in row.keys() and row["source_ref"] else [],
                    [row["related_task_id"]]
                    if "related_task_id" in row.keys() and row["related_task_id"]
                    else [],
                    [row["blocked_by_ref"]]
                    if "blocked_by_ref" in row.keys() and row["blocked_by_ref"]
                    else [],
                )
                source_type = default_source
                source_ref = f"{table}:{row_id}"
                source_hash = _search_hash(dict(row))
                item_id = row["item_id"] if "item_id" in row.keys() else ""
            docs.append(
                _search_payload(
                    record_type=record_type,
                    record_table=table,
                    record_id=row_id,
                    source_type=source_type,
                    source_ref=source_ref,
                    source_hash=source_hash,
                    title=(row["title"] if "title" in row.keys() else row["author"]) or "",
                    body=row["body_excerpt"] or "",
                    local_date=(row["due_at"] or "")[:10]
                    if "due_at" in row.keys() and row["due_at"]
                    else None,
                    status=row["status"] or "",
                    mode="kanban",
                    privacy_level="normal",
                    tags=[str(item) for item in _as_list(tags) if str(item)],
                    related_refs=related_refs,
                    page_ref={"group": "kanban", "tab": "kanban", "item_id": item_id},
                    source_refs=[f"{table}:{row_id}", *related_refs],
                    provenance=_json_value(row["provenance_json"], {}),
                    updated_at=row["updated_at"],
                )
            )
    return docs


def _upsert_personal_search_document(conn: Any, doc: dict[str, Any], now: str) -> str:
    existing = conn.execute(
        "SELECT source_hash, embedding_ref, embedding_model, embedding_updated_at, "
        "vector_index_status, vector_index_updated_at "
        "FROM personal_search_documents WHERE document_id=?",
        (doc["document_id"],),
    ).fetchone()
    source_changed = not existing or existing["source_hash"] != doc["source_hash"]
    embedding_ref = "" if source_changed else existing["embedding_ref"]
    embedding_model = "" if source_changed else existing["embedding_model"]
    embedding_updated_at = None if source_changed else existing["embedding_updated_at"]
    vector_status = "pending" if source_changed else existing["vector_index_status"]
    vector_updated_at = None if source_changed else existing["vector_index_updated_at"]
    created_at = now
    conn.execute(
        """
        INSERT INTO personal_search_documents (
            document_id, record_type, record_table, record_id, source_type, source_ref,
            source_hash, title, body, search_text, local_date, status, mode,
            privacy_level, tags_json, related_refs_json, page_ref_json,
            source_refs_json, provenance_json, score_metadata_json, embedding_ref,
            embedding_model, embedding_updated_at, vector_index_key,
            vector_index_status, vector_index_updated_at, created_at, updated_at
        )
        VALUES (
            :document_id, :record_type, :record_table, :record_id, :source_type,
            :source_ref, :source_hash, :title, :body, :search_text, :local_date,
            :status, :mode, :privacy_level, :tags_json, :related_refs_json,
            :page_ref_json, :source_refs_json, :provenance_json, :score_metadata_json,
            :embedding_ref, :embedding_model, :embedding_updated_at, :vector_index_key,
            :vector_index_status, :vector_index_updated_at, :created_at, :updated_at
        )
        ON CONFLICT(document_id) DO UPDATE SET
            record_type=excluded.record_type,
            record_table=excluded.record_table,
            record_id=excluded.record_id,
            source_type=excluded.source_type,
            source_ref=excluded.source_ref,
            source_hash=excluded.source_hash,
            title=excluded.title,
            body=excluded.body,
            search_text=excluded.search_text,
            local_date=excluded.local_date,
            status=excluded.status,
            mode=excluded.mode,
            privacy_level=excluded.privacy_level,
            tags_json=excluded.tags_json,
            related_refs_json=excluded.related_refs_json,
            page_ref_json=excluded.page_ref_json,
            source_refs_json=excluded.source_refs_json,
            provenance_json=excluded.provenance_json,
            score_metadata_json=excluded.score_metadata_json,
            embedding_ref=excluded.embedding_ref,
            embedding_model=excluded.embedding_model,
            embedding_updated_at=excluded.embedding_updated_at,
            vector_index_key=excluded.vector_index_key,
            vector_index_status=excluded.vector_index_status,
            vector_index_updated_at=excluded.vector_index_updated_at,
            updated_at=excluded.updated_at
        """,
        {
            **doc,
            "embedding_ref": embedding_ref,
            "embedding_model": embedding_model,
            "embedding_updated_at": embedding_updated_at,
            "vector_index_status": vector_status,
            "vector_index_updated_at": vector_updated_at,
            "created_at": created_at,
            "updated_at": doc["updated_at"] or now,
        },
    )
    conn.execute("DELETE FROM personal_search_fts WHERE document_id=?", (doc["document_id"],))
    tags = " ".join(str(item) for item in _as_list(_json_value(doc["tags_json"], [])))
    conn.execute(
        """
        INSERT INTO personal_search_fts (
            document_id, title, body, search_text, tags, source_type, record_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            doc["document_id"],
            doc["title"],
            doc["body"],
            doc["search_text"],
            tags,
            doc["source_type"],
            doc["record_type"],
        ),
    )
    return "updated" if source_changed else "unchanged"


def _sync_personal_search_documents(conn: Any, now: str) -> dict[str, Any]:
    import_status = _sync_personal_import_status_batches(conn, now)
    docs = _collect_personal_search_documents(conn)
    seen = {doc["document_id"] for doc in docs}
    updated = 0
    unchanged = 0
    for doc in docs:
        status = _upsert_personal_search_document(conn, doc, now)
        if status == "updated":
            updated += 1
        else:
            unchanged += 1
    stale_rows = conn.execute("SELECT document_id FROM personal_search_documents").fetchall()
    deleted = 0
    for row in stale_rows:
        if row["document_id"] in seen:
            continue
        conn.execute("DELETE FROM personal_search_fts WHERE document_id=?", (row["document_id"],))
        conn.execute(
            "DELETE FROM personal_search_documents WHERE document_id=?", (row["document_id"],)
        )
        deleted += 1
    return {
        "document_count": len(docs),
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
        "import_status": import_status,
    }


def _sync_personal_import_status_batches(conn: Any, now: str) -> dict[str, Any]:
    counts = _personal_source_counts_from_conn(conn)
    interests = _parse_interests_dashboard()
    git_activity = _git_activity_dashboard(counts)
    today = now[:10]

    interest_artifacts = [
        str(item.get("path") or item.get("label") or "")
        for item in _as_list(interests.get("proof_links"))
    ]
    interest_artifacts = [item for item in interest_artifacts if item]
    if interests.get("doc_path"):
        interest_artifacts.insert(0, str(interests["doc_path"]))
    interest_provenance = {
        "schema": "xarta.personal.import_status.v1",
        "source": "imports-dashboard",
        "source_digest": interests.get("source_digest") or interests.get("content_digest") or "",
        "snapshot_at": interests.get("snapshot_at") or "",
        "pending_review": interests.get("pending_review"),
        "actionable_backlog": interests.get("actionable_backlog"),
    }
    git_repos = _as_list(git_activity.get("watched_repos"))
    git_artifacts = [
        str(repo.get("path") or repo.get("repo_id") or "")
        for repo in git_repos
        if isinstance(repo, dict)
    ]
    git_blockers = [
        json.dumps(item, sort_keys=True, ensure_ascii=True)
        for item in [
            *_as_list(git_activity.get("errors")),
            *_as_list(git_activity.get("actionable_repos")),
        ]
    ]
    git_digest_payload = {
        "repos": [
            {
                "repo_id": repo.get("repo_id"),
                "head": repo.get("head"),
                "dirty_count": repo.get("dirty_count"),
                "untracked_count": repo.get("untracked_count"),
                "error": repo.get("error"),
                "daily_commit_count": repo.get("daily_commit_count"),
            }
            for repo in git_repos
            if isinstance(repo, dict)
        ],
        "latest_commits": [
            {
                "repo_id": commit.get("repo_id"),
                "sha": commit.get("sha"),
                "author_date": commit.get("author_date"),
                "subject": commit.get("subject"),
            }
            for commit in _as_list(git_activity.get("latest_commits"))
            if isinstance(commit, dict)
        ],
    }
    git_snapshot_at = max(
        (
            str(commit.get("author_date") or "")
            for commit in _as_list(git_activity.get("latest_commits"))
            if isinstance(commit, dict) and commit.get("author_date")
        ),
        default="",
    )
    git_provenance = {
        "schema": "xarta.personal.import_status.v1",
        "source": "imports-dashboard",
        "source_digest": _search_hash(git_digest_payload),
        "snapshot_at": git_snapshot_at,
        "watched_repo_count": len(git_repos),
        "daily_commit_count": len(_as_list(git_activity.get("latest_commits"))),
    }
    rows = [
        {
            "import_batch_id": "status-interests-ingestion",
            "source_type": "interests-ingestion",
            "source_ref": str(interests.get("doc_path") or INTERESTS_DASHBOARD_REL),
            "title": "Hermes Interests Ingestion Status",
            "status": str(interests.get("status") or "unknown"),
            "local_date": today,
            "artifact_refs_json": json.dumps(interest_artifacts, ensure_ascii=True),
            "blocker_refs_json": json.dumps(_as_list(interests.get("blockers")), ensure_ascii=True),
            "provenance_json": json.dumps(interest_provenance, sort_keys=True, ensure_ascii=True),
        },
        {
            "import_batch_id": "status-git-activity",
            "source_type": "git",
            "source_ref": "watched-git-repos",
            "title": "Git Activity Import Status",
            "status": str(git_activity.get("status") or "unknown"),
            "local_date": today,
            "artifact_refs_json": json.dumps(git_artifacts, ensure_ascii=True),
            "blocker_refs_json": json.dumps(git_blockers, ensure_ascii=True),
            "provenance_json": json.dumps(git_provenance, sort_keys=True, ensure_ascii=True),
        },
    ]

    inserted = 0
    updated = 0
    unchanged = 0
    content_cols = (
        "source_type",
        "source_ref",
        "title",
        "status",
        "local_date",
        "artifact_refs_json",
        "blocker_refs_json",
        "provenance_json",
    )
    for row in rows:
        existing = conn.execute(
            "SELECT * FROM personal_import_batches WHERE import_batch_id=?",
            (row["import_batch_id"],),
        ).fetchone()
        if existing and all((existing[col] or "") == (row[col] or "") for col in content_cols):
            unchanged += 1
            continue
        if existing:
            conn.execute(
                """
                UPDATE personal_import_batches
                SET source_type=:source_type,
                    source_ref=:source_ref,
                    title=:title,
                    status=:status,
                    local_date=:local_date,
                    completed_at=:completed_at,
                    privacy_level='normal',
                    artifact_refs_json=:artifact_refs_json,
                    blocker_refs_json=:blocker_refs_json,
                    provenance_json=:provenance_json,
                    updated_at=:updated_at
                WHERE import_batch_id=:import_batch_id
                """,
                {
                    **row,
                    "completed_at": now if row["status"] == "ok" else None,
                    "updated_at": now,
                },
            )
            updated += 1
            continue
        conn.execute(
            """
            INSERT INTO personal_import_batches (
                import_batch_id, source_type, source_ref, title, status, local_date,
                started_at, completed_at, privacy_level, artifact_refs_json,
                blocker_refs_json, provenance_json, created_at, updated_at
            )
            VALUES (
                :import_batch_id, :source_type, :source_ref, :title, :status,
                :local_date, :started_at, :completed_at, 'normal',
                :artifact_refs_json, :blocker_refs_json, :provenance_json,
                :created_at, :updated_at
            )
            """,
            {
                **row,
                "started_at": now,
                "completed_at": now if row["status"] == "ok" else None,
                "created_at": now,
                "updated_at": now,
            },
        )
        inserted += 1
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged}


async def _sync_personal_search_vectors(
    *,
    force_embeddings: bool,
    limit: int,
    now: str,
) -> dict[str, Any]:
    safe_limit = max(1, min(int(limit or 200), 1000))
    if force_embeddings:
        where = "search_text != ''"
        params: tuple[Any, ...] = (safe_limit,)
    else:
        where = "search_text != '' AND vector_index_status != 'indexed'"
        params = (safe_limit,)
    with get_conn() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT * FROM personal_search_documents
                WHERE {where}
                ORDER BY updated_at DESC, document_id
                LIMIT ?
                """,
                params,
            ).fetchall()
        ]
    if not rows:
        return {
            "status": "ok",
            "attempted": 0,
            "indexed": 0,
            "error": "",
            "embedding_model": "",
        }

    try:
        from .ai_client import _get_provider, embed
        from .seekdb import short_seekdb_error, upsert_personal_index_async

        provider = _get_provider(PERSONAL_SEARCH_PROJECT, "embedding")
        embedding_model = str(provider.get("model_name") or PERSONAL_SEARCH_PROJECT)
        vectors = await embed(PERSONAL_SEARCH_PROJECT, [row["search_text"] for row in rows])
        if len(vectors) != len(rows) or any(
            len(vector) != PERSONAL_SEARCH_VECTOR_DIM for vector in vectors
        ):
            raise RuntimeError("unexpected embedding dimensions")
        updates = []
        for row, vector in zip(rows, vectors, strict=True):
            await upsert_personal_index_async(row, vector)
            embedding_ref = _search_hash(
                {
                    "document_id": row["document_id"],
                    "source_hash": row["source_hash"],
                    "model": embedding_model,
                    "dimensions": len(vector),
                }
            )
            updates.append((embedding_ref, embedding_model, now, now, now, row["document_id"]))
        with get_conn() as conn:
            conn.executemany(
                """
                UPDATE personal_search_documents
                SET embedding_ref=?,
                    embedding_model=?,
                    embedding_updated_at=?,
                    vector_index_status='indexed',
                    vector_index_updated_at=?,
                    updated_at=?
                WHERE document_id=?
                """,
                updates,
            )
        return {
            "status": "ok",
            "attempted": len(rows),
            "indexed": len(updates),
            "error": "",
            "embedding_model": embedding_model,
        }
    except Exception as exc:
        try:
            err = short_seekdb_error(exc)  # type: ignore[name-defined]
        except Exception:
            err = str(exc).splitlines()[0][:180]
        return {
            "status": "error",
            "attempted": len(rows),
            "indexed": 0,
            "error": err,
            "embedding_model": "",
        }


async def _sync_personal_search_index(
    *,
    force_embeddings: bool = False,
    include_embeddings: bool = True,
    limit: int = 200,
) -> dict[str, Any]:
    now = _utc_now_iso()
    with get_conn() as conn:
        document_summary = _sync_personal_search_documents(conn, now)
    vector_summary = {
        "status": "skipped",
        "attempted": 0,
        "indexed": 0,
        "error": "",
        "embedding_model": "",
    }
    if include_embeddings:
        vector_summary = await _sync_personal_search_vectors(
            force_embeddings=force_embeddings,
            limit=limit,
            now=now,
        )
    return {
        "ok": document_summary["document_count"] >= 0,
        "schema": "xarta.personal.search.sync.v1",
        "generated_at_utc": now,
        "documents": document_summary,
        "vector": vector_summary,
    }


def _fts_query(text: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9_./:-]+", text.lower())[:12]
    quoted = [f'"{token.replace(chr(34), chr(34) + chr(34))}"' for token in tokens if token]
    return " OR ".join(quoted)


def _personal_search_where(
    *,
    date_start: str | None,
    date_end: str | None,
    source_type: str | None,
    status: str | None,
    mode: str | None,
    record_type: str | None,
    tag: str | None,
) -> tuple[list[str], list[Any]]:
    where = ["d.privacy_level != 'pin'"]
    params: list[Any] = []
    if date_start:
        where.append("d.local_date >= ?")
        params.append(date_start)
    if date_end:
        where.append("d.local_date <= ?")
        params.append(date_end)
    if source_type:
        where.append("d.source_type = ?")
        params.append(source_type)
    if status:
        where.append("d.status = ?")
        params.append(status)
    if mode:
        if mode not in {*PERSONAL_MODES.keys(), "calendar"}:
            raise HTTPException(400, f"unknown mode: {mode}")
        where.append("d.mode = ?")
        params.append(mode)
    if record_type:
        where.append("d.record_type = ?")
        params.append(record_type)
    if tag:
        where.append("EXISTS (SELECT 1 FROM json_each(d.tags_json) WHERE value = ?)")
        params.append(tag)
    return where, params


def _personal_exact_candidates(
    conn: Any,
    q: str,
    *,
    where: list[str],
    params: list[Any],
    limit: int,
) -> list[dict[str, Any]]:
    if not q.strip():
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        return [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT d.*, 0 AS exact_rank
                FROM personal_search_documents d
                {clause}
                ORDER BY COALESCE(d.local_date, d.updated_at) DESC, d.updated_at DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        ]
    q_lower = q.lower().strip()
    tokens = [token for token in re.findall(r"[a-z0-9_./:-]+", q_lower) if token][:8]
    match_parts = [
        "lower(d.document_id) = ?",
        "lower(d.record_id) = ?",
        "lower(d.source_ref) = ?",
        "lower(d.title) LIKE ?",
        "lower(d.search_text) LIKE ?",
    ]
    match_params: list[Any] = [q_lower, q_lower, q_lower, f"%{q_lower}%", f"%{q_lower}%"]
    if tokens:
        token_clause = " AND ".join("lower(d.search_text) LIKE ?" for _ in tokens)
        match_parts.append(f"({token_clause})")
        match_params.extend(f"%{token}%" for token in tokens)
    exact_where = [*where, f"({' OR '.join(match_parts)})"]
    exact_params = [*params, *match_params]
    clause = f"WHERE {' AND '.join(exact_where)}"
    rows = conn.execute(
        f"""
        SELECT d.*,
               CASE
                   WHEN lower(d.document_id) = ? OR lower(d.record_id) = ? THEN 0
                   WHEN lower(d.title) LIKE ? THEN 1
                   WHEN lower(d.source_ref) = ? THEN 2
                   ELSE 3
               END AS exact_rank
        FROM personal_search_documents d
        {clause}
        ORDER BY exact_rank ASC, d.updated_at DESC, d.document_id
        LIMIT ?
        """,
        (q_lower, q_lower, f"%{q_lower}%", q_lower, *exact_params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _personal_fts_candidates(
    conn: Any,
    q: str,
    *,
    where: list[str],
    params: list[Any],
    limit: int,
) -> list[dict[str, Any]]:
    match = _fts_query(q)
    if not match:
        return []
    clause = f"AND {' AND '.join(where)}" if where else ""
    try:
        rows = conn.execute(
            f"""
            SELECT d.*, bm25(personal_search_fts) AS bm25_score
            FROM personal_search_fts
            JOIN personal_search_documents d
              ON d.document_id = personal_search_fts.document_id
            WHERE personal_search_fts MATCH ?
            {clause}
            ORDER BY bm25_score ASC, d.updated_at DESC
            LIMIT ?
            """,
            (match, *params, limit),
        ).fetchall()
    except sqlite3.Error:
        return []
    return [dict(row) for row in rows]


async def _personal_vector_candidates(
    q: str, *, limit: int
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not q.strip():
        return [], {"status": "skipped", "error": "", "candidate_count": 0}
    try:
        from .ai_client import embed
        from .seekdb import short_seekdb_error, vector_search_personal_async

        query_embedding = (await embed(PERSONAL_SEARCH_PROJECT, [q]))[0]
        rows = await vector_search_personal_async(query_embedding, limit)
        return rows, {"status": "ok", "error": "", "candidate_count": len(rows)}
    except Exception as exc:
        try:
            err = short_seekdb_error(exc)  # type: ignore[name-defined]
        except Exception:
            err = str(exc).splitlines()[0][:180]
        return [], {"status": "error", "error": err, "candidate_count": 0}


def _row_to_search_result(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "document_id": row["document_id"],
        "record_type": row["record_type"],
        "record_table": row["record_table"],
        "record_id": row["record_id"],
        "title": row["title"],
        "body_excerpt": _body_excerpt(row["body"] or row["search_text"], limit=360),
        "local_date": row["local_date"],
        "status": row["status"],
        "mode": row["mode"],
        "source": {
            "type": row["source_type"],
            "ref": row["source_ref"],
            "hash": row["source_hash"],
        },
        "tags": _json_value(row["tags_json"], []),
        "related_refs": _json_value(row["related_refs_json"], []),
        "page_ref": _json_value(row["page_ref_json"], {}),
        "source_refs": _json_value(row["source_refs_json"], []),
        "provenance": _json_value(row["provenance_json"], {}),
        "search": {
            "text": row["search_text"],
            "score_metadata": _json_value(row["score_metadata_json"], {}),
        },
        "vector": {
            "embedding_ref": row["embedding_ref"],
            "embedding_model": row["embedding_model"],
            "embedding_updated_at": row["embedding_updated_at"],
            "index_key": row["vector_index_key"],
            "index_status": row["vector_index_status"],
            "index_updated_at": row["vector_index_updated_at"],
        },
        "updated_at": row["updated_at"],
    }


def _merge_personal_candidates(
    *,
    exact_rows: list[dict[str, Any]],
    fts_rows: list[dict[str, Any]],
    vector_rows: list[dict[str, Any]],
    document_rows: dict[str, dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    rrf: dict[str, float] = {}
    payload: dict[str, dict[str, Any]] = {}
    k = 60

    def accumulate(row: dict[str, Any], source: str, rank: int, component: dict[str, Any]) -> None:
        doc_id = str(row.get("document_id") or row.get("id") or "")
        if not doc_id:
            return
        source_row = document_rows.get(doc_id) or row
        rrf[doc_id] = rrf.get(doc_id, 0.0) + (1.0 / (k + rank + 1))
        if doc_id not in payload:
            payload[doc_id] = _row_to_search_result(source_row)
            payload[doc_id]["score"] = {
                "rrf_score": 0.0,
                "score_sources": [],
                "components": {},
            }
        score = payload[doc_id]["score"]
        if source not in score["score_sources"]:
            score["score_sources"].append(source)
        score["components"][source] = component

    for rank, row in enumerate(exact_rows):
        accumulate(row, "exact", rank, {"rank": rank + 1, "exact_rank": row.get("exact_rank")})
    for rank, row in enumerate(fts_rows):
        accumulate(row, "fts_bm25", rank, {"rank": rank + 1, "bm25": row.get("bm25_score")})
    for rank, row in enumerate(vector_rows):
        meta = row.get("metadata") or {}
        doc_id = str(meta.get("document_id") or row.get("id") or "")
        if doc_id not in document_rows:
            continue
        accumulate(
            {"document_id": doc_id},
            "vector",
            rank,
            {"rank": rank + 1, "cosine_distance": row.get("distance")},
        )

    for doc_id, result in payload.items():
        result["score"]["rrf_score"] = rrf.get(doc_id, 0.0)
    return sorted(
        payload.values(),
        key=lambda result: (
            -float(result["score"]["rrf_score"]),
            result.get("updated_at") or "",
            result.get("document_id") or "",
        ),
        reverse=False,
    )[:limit]


async def _rerank_personal_results(
    q: str,
    results: list[dict[str, Any]],
    *,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not q.strip() or len(results) < 2:
        for index, result in enumerate(results, start=1):
            result["score"]["reranker_rank"] = index
        return results, {"status": "skipped", "error": "", "candidate_count": len(results)}
    try:
        from .ai_client import rerank
        from .seekdb import short_seekdb_error

        docs = [
            " ".join(
                part
                for part in [
                    result.get("title") or "",
                    result.get("body_excerpt") or "",
                    " ".join(result.get("tags") or []),
                    " ".join(result.get("source_refs") or []),
                ]
                if part
            )
            for result in results
        ]
        ranked = await rerank(PERSONAL_SEARCH_PROJECT, q, docs, top_n=min(limit, len(docs)))
        reranked: list[dict[str, Any]] = []
        by_index = {idx: result for idx, result in enumerate(results)}
        for pos, item in enumerate(ranked, start=1):
            result = by_index.get(int(item.get("index", -1)))
            if not result:
                continue
            result["score"]["reranker_rank"] = pos
            result["score"]["reranker_score"] = item.get("relevance_score")
            reranked.append(result)
        seen = {result["document_id"] for result in reranked}
        for result in results:
            if result["document_id"] not in seen:
                result["score"]["reranker_rank"] = len(reranked) + 1
                reranked.append(result)
        return reranked[:limit], {"status": "ok", "error": "", "candidate_count": len(results)}
    except Exception as exc:
        try:
            err = short_seekdb_error(exc)  # type: ignore[name-defined]
        except Exception:
            err = str(exc).splitlines()[0][:180]
        for index, result in enumerate(results, start=1):
            result["score"]["reranker_rank"] = index
        return results, {"status": "error", "error": err, "candidate_count": len(results)}


@router.post("/search/sync")
async def sync_personal_search(body: PersonalSearchSyncRequest) -> dict[str, Any]:
    return await _sync_personal_search_index(
        force_embeddings=body.force_embeddings,
        include_embeddings=body.include_embeddings,
        limit=body.limit,
    )


@router.get("/search")
async def search_personal_activity(
    q: str | None = "",
    date_start: str | None = None,
    date_end: str | None = None,
    source_type: str | None = None,
    status: str | None = None,
    mode: str | None = None,
    record_type: str | None = None,
    tag: str | None = None,
    include_vector: bool = True,
    rerank_results: bool = True,
    sync: bool = True,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    query = (q or "").strip()
    sync_summary = None
    if sync:
        sync_summary = await _sync_personal_search_index(
            include_embeddings=include_vector,
            limit=max(limit * 4, 40),
        )
    where, params = _personal_search_where(
        date_start=date_start,
        date_end=date_end,
        source_type=source_type,
        status=status,
        mode=mode,
        record_type=record_type,
        tag=tag,
    )
    window = max(limit * 4, 40)
    with get_conn() as conn:
        exact_rows = _personal_exact_candidates(
            conn,
            query,
            where=where,
            params=params,
            limit=window,
        )
        fts_rows = _personal_fts_candidates(
            conn,
            query,
            where=where,
            params=params,
            limit=window,
        )
        filtered_clause = f"WHERE {' AND '.join(where)}" if where else ""
        all_doc_rows = [
            dict(row)
            for row in conn.execute(
                f"SELECT d.* FROM personal_search_documents d {filtered_clause}",
                params,
            ).fetchall()
        ]
    document_rows = {row["document_id"]: row for row in all_doc_rows}
    vector_rows: list[dict[str, Any]] = []
    vector_status = {"status": "skipped", "error": "", "candidate_count": 0}
    if include_vector:
        vector_rows, vector_status = await _personal_vector_candidates(query, limit=window)
    results = _merge_personal_candidates(
        exact_rows=exact_rows,
        fts_rows=fts_rows,
        vector_rows=vector_rows,
        document_rows=document_rows,
        limit=window,
    )
    rerank_status = {"status": "skipped", "error": "", "candidate_count": len(results)}
    if rerank_results:
        results, rerank_status = await _rerank_personal_results(query, results, limit=limit)
    else:
        results = results[:limit]
    return {
        "schema": "xarta.personal.search.results.v1",
        "query": query,
        "count": len(results),
        "results": results,
        "filters": {
            "date_start": date_start,
            "date_end": date_end,
            "source_type": source_type,
            "status": status,
            "mode": mode,
            "record_type": record_type,
            "tag": tag,
        },
        "subsystems": {
            "exact": {"status": "ok", "candidate_count": len(exact_rows)},
            "fts": {"status": "ok", "candidate_count": len(fts_rows), "rank": "bm25"},
            "vector": vector_status,
            "rerank": rerank_status,
            "sync": sync_summary,
        },
    }


@router.post("/graph/sync")
async def sync_personal_graph_links(body: PersonalGraphSyncRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    request_id = _clean_short_text(
        body.request_id,
        "personal-graph-sync",
        limit=160,
    )
    actor = _clean_short_text(body.actor, "blueprints-api", limit=120)
    with get_conn() as conn:
        candidates = _personal_graph_candidates(
            conn,
            actor=actor,
            request_id=request_id,
            now=now,
        )
        counts = {"inserted": 0, "updated": 0, "unchanged": 0}
        changed: list[dict[str, Any]] = []
        for payload in candidates.values():
            result = _upsert_graph_link(conn, payload)
            counts[result] += 1
            if result in {"inserted", "updated"}:
                row = conn.execute(
                    "SELECT * FROM personal_graph_links WHERE link_id=?",
                    (payload["link_id"],),
                ).fetchone()
                if row:
                    changed.append(dict(row))
        if changed:
            gen = increment_gen(conn, "personal-graph-links")
            for row in changed:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "personal_graph_links",
                    row["link_id"],
                    row,
                    gen,
                )
    return {
        "ok": True,
        "schema": "xarta.personal.graph.sync.v1",
        "generated_at_utc": now,
        "candidate_count": len(candidates),
        "links": counts,
    }


@router.get("/graph/links")
async def list_personal_graph_links(
    source_ref: str | None = None,
    target_ref: str | None = None,
    link_type: str | None = None,
    link_state: str | None = None,
    sync: bool = False,
    limit: Annotated[int, Query(ge=1, le=200)] = 80,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    sync_summary = None
    if sync:
        sync_summary = await sync_personal_graph_links(PersonalGraphSyncRequest())
    where: list[str] = []
    params: list[Any] = []
    if source_ref:
        where.append("source_ref = ?")
        params.append(_clean_graph_ref(source_ref))
    if target_ref:
        where.append("target_ref = ?")
        params.append(_clean_graph_ref(target_ref))
    if link_type:
        where.append("link_type = ?")
        params.append(_clean_graph_link_type(link_type))
    if link_state:
        where.append("link_state = ?")
        params.append(_clean_graph_link_state(link_state))
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM personal_graph_links
            {clause}
            ORDER BY updated_at DESC, link_type, link_id
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
    return {
        "ok": True,
        "schema": "xarta.personal.graph.links.v1",
        "count": len(rows),
        "links": [_row_to_personal_graph_link(row) for row in rows],
        "pagination": _pagination(limit, offset, len(rows)),
        "sync": sync_summary,
    }


@router.post("/graph/links")
async def create_personal_graph_link(body: PersonalGraphLinkCreateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    source_ref = _clean_graph_ref(body.source_ref)
    target_ref = _clean_graph_ref(body.target_ref)
    if not source_ref or not target_ref:
        raise HTTPException(400, "source_ref and target_ref are required")
    link_state = _clean_graph_link_state(body.link_state, default="declared")
    request_id = _clean_short_text(
        body.request_id,
        f"personal-graph-link-{uuid.uuid4().hex[:12]}",
        limit=160,
    )
    actor = _clean_short_text(body.actor, "blueprints-ui", limit=120)
    provenance = body.provenance if isinstance(body.provenance, dict) else {}
    provenance = {
        **provenance,
        "declared_by": actor,
        "source_surface": _clean_short_text(body.source_surface, "personal-graph", limit=120),
        "guard": "inferred input is stored as needs_review until explicitly accepted",
    }
    payload = _graph_link_row_payload(
        source_ref=source_ref,
        target_ref=target_ref,
        link_type=body.link_type,
        link_state=link_state,
        risk_level=body.risk_level,
        title=body.title or "",
        metadata=body.metadata if isinstance(body.metadata, dict) else {},
        provenance=provenance,
        actor=actor,
        request_id=request_id,
        now=now,
    )
    with get_conn() as conn:
        result = _upsert_graph_link(conn, payload)
        row = conn.execute(
            "SELECT * FROM personal_graph_links WHERE link_id=?",
            (payload["link_id"],),
        ).fetchone()
        if result in {"inserted", "updated"} and row:
            gen = increment_gen(conn, "personal-graph-link")
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "personal_graph_links",
                row["link_id"],
                dict(row),
                gen,
            )
    return {
        "ok": True,
        "result": result,
        "link": _row_to_personal_graph_link(row),
    }


@router.get("/events")
async def list_personal_events(
    date_start: str | None = None,
    date_end: str | None = None,
    source_type: str | None = None,
    status: str | None = None,
    privacy_level: str | None = None,
    tag: str | None = None,
    related_kanban_item: str | None = None,
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
    if kind:
        where.append("kind = ?")
        params.append(kind)
    if tag:
        _add_json_array_filter(where, params, "tags_json", tag)
    if related_kanban_item:
        _add_json_array_filter(where, params, "related_kanban_items_json", related_kanban_item)
    if import_batch:
        _add_json_array_filter(where, params, "related_import_batches_json", import_batch)
    _apply_mode(where, params, mode)
    _append_personal_privacy_list_filter(where, params, privacy_level)

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
            "related_kanban_item": related_kanban_item,
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


@router.post("/events/{event_id}/kanban-links")
async def link_personal_event_work_item(
    event_id: str, body: DiaryKanbanLinkRequest
) -> dict[str, Any]:
    kanban_ref = _clean_short_text(body.kanban_item_ref, "", limit=200)
    if not kanban_ref:
        raise HTTPException(400, "Kanban item ref is required")
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "diary-page")
    request_id = _clean_short_text(
        body.request_id, f"kanban-link-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    audit_id = f"audit-{uuid.uuid4().hex}"
    now = _utc_now_iso()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, "event not found")
        kanban_items = _json_value(row["related_kanban_items_json"], [])
        if not isinstance(kanban_items, list):
            kanban_items = []
        if kanban_ref not in kanban_items:
            kanban_items.append(kanban_ref)
        provenance = _json_value(row["provenance_json"], {})
        links = provenance.get("kanban_link_audit") if isinstance(provenance, dict) else []
        if not isinstance(links, list):
            links = []
        links.append({"kanban_item_ref": kanban_ref, "audit_id": audit_id, "created_at": now})
        provenance["kanban_link_audit"] = links[-12:]
        conn.execute(
            """
            UPDATE personal_events
            SET related_kanban_items_json=?,
                provenance_json=?,
                updated_at=?
            WHERE event_id=?
            """,
            (
                json.dumps(kanban_items, ensure_ascii=True),
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
            action="link_kanban_item",
            target_ref=f"personal_events:{event_id}",
            file_ref="",
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=row["source_hash"] or "",
            metadata={"kanban_item_ref": kanban_ref},
        )
        gen = increment_gen(conn, "personal-kanban-link")
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
    _append_personal_privacy_list_filter(where, params, privacy_level)

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


@router.get("/tasks")
async def list_personal_tasks(
    date_start: str | None = None,
    date_end: str | None = None,
    status: str | None = None,
    privacy_level: str | None = None,
    tag: str | None = None,
    related_kanban_item: str | None = None,
    import_batch: str | None = None,
    mode: str | None = None,
    source_type: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> dict[str, Any]:
    task_where, task_params = _task_mode_where(mode)
    event_where, event_params = _event_task_where(mode)
    if date_start:
        task_where.append("local_date >= ?")
        task_params.append(date_start)
        event_where.append("local_date >= ?")
        event_params.append(date_start)
    if date_end:
        task_where.append("local_date <= ?")
        task_params.append(date_end)
        event_where.append("local_date <= ?")
        event_params.append(date_end)
    if status:
        task_where.append("status = ?")
        task_params.append(status)
        event_where.append("status = ?")
        event_params.append(status)
    _append_personal_privacy_list_filter(task_where, task_params, privacy_level)
    _append_personal_privacy_list_filter(event_where, event_params, privacy_level)
    if source_type:
        task_where.append("source_type = ?")
        task_params.append(source_type)
        event_where.append("source_type = ?")
        event_params.append(source_type)
    if tag:
        _add_json_array_filter(task_where, task_params, "tags_json", tag)
        _add_json_array_filter(event_where, event_params, "tags_json", tag)
    if related_kanban_item:
        _add_json_array_filter(
            task_where, task_params, "related_kanban_items_json", related_kanban_item
        )
        _add_json_array_filter(
            event_where, event_params, "related_kanban_items_json", related_kanban_item
        )
    if import_batch:
        _add_json_array_filter(task_where, task_params, "related_import_batches_json", import_batch)
        _add_json_array_filter(
            event_where, event_params, "related_import_batches_json", import_batch
        )

    task_clause = f"WHERE {' AND '.join(task_where)}" if task_where else ""
    event_clause = f"WHERE {' AND '.join(event_where)}" if event_where else ""
    fetch_limit = max(limit + offset, 200)
    with get_conn() as conn:
        task_rows = conn.execute(
            f"""
            SELECT * FROM personal_time_tasks
            {task_clause}
            ORDER BY COALESCE(due_at, local_date, created_at) ASC, task_id ASC
            LIMIT ?
            """,
            (*task_params, fetch_limit),
        ).fetchall()
        event_rows = conn.execute(
            f"""
            SELECT * FROM personal_events
            {event_clause}
            ORDER BY COALESCE(start_at, local_date, created_at) ASC, event_id ASC
            LIMIT ?
            """,
            (*event_params, fetch_limit),
        ).fetchall()
        kanban_preferences = _kanban_preferences(conn)
        kanban_todo_rows = _kanban_todo_task_page_rows(
            conn,
            mode=mode,
            date_start=date_start,
            date_end=date_end,
            status=status,
            source_type=source_type,
            related_kanban_item=related_kanban_item,
            fetch_limit=fetch_limit,
        )
        kanban_todo_rows, hidden_kanban_todos = _filter_kanban_todo_test_rows(
            conn,
            kanban_todo_rows,
            bool(kanban_preferences["show_test_entries"]),
        )
        counts = _task_counts(conn, show_test_entries=bool(kanban_preferences["show_test_entries"]))

    items = [_row_to_task(row) for row in task_rows]
    items.extend(_event_to_task(row) for row in event_rows)
    items.extend(_kanban_todo_to_task(row) for row in kanban_todo_rows)
    deduped: dict[str, dict[str, Any]] = {}
    for item in items:
        deduped.setdefault(item["task_id"], item)
    items = sorted(
        deduped.values(),
        key=lambda item: (
            item.get("due_at") or item.get("local_date") or item.get("created_at") or "",
            item.get("task_id") or "",
        ),
    )
    source_counts: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    for item in items:
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        source_type_value = str(source.get("type") or "unknown")
        source_counts[source_type_value] = source_counts.get(source_type_value, 0) + 1
        item_status = str(item.get("status") or "unknown")
        status_counts[item_status] = status_counts.get(item_status, 0) + 1
    page = items[offset : offset + limit]
    return {
        "items": page,
        "pagination": _pagination(limit, offset, len(page)),
        "counts": {
            "modes": counts,
            "sources": source_counts,
            "status": status_counts,
            "total": len(items),
        },
        "kanban_preferences": kanban_preferences,
        "test_entries": {
            "show": bool(kanban_preferences["show_test_entries"]),
            "hidden_kanban_todos": hidden_kanban_todos,
        },
        "filters": {
            "date_start": date_start,
            "date_end": date_end,
            "status": status,
            "privacy_level": privacy_level,
            "tag": tag,
            "related_kanban_item": related_kanban_item,
            "import_batch": import_batch,
            "mode": mode,
            "source_type": source_type,
        },
    }


@router.post("/tasks")
async def create_personal_task(body: PersonalTaskUpsertRequest) -> dict[str, Any]:
    return _upsert_personal_task(body, action="create_task")


def _task_request_from_row(
    row: Any,
    action: PersonalTaskActionRequest,
    *,
    status: str | None = None,
) -> PersonalTaskUpsertRequest:
    provenance = _json_value(row["provenance_json"], {})
    task_meta = provenance.get("task") if isinstance(provenance, dict) else {}
    return PersonalTaskUpsertRequest(
        task_id=row["task_id"],
        title=row["title"],
        body=row["body_excerpt"] or "",
        mode=row["mode"],
        status=status or row["status"],
        priority=row["priority"],
        due_date=row["local_date"] if row["due_at"] else None,
        due_time=task_meta.get("due_time", "") if isinstance(task_meta, dict) else "",
        timezone=row["timezone"],
        privacy_level=row["privacy_level"],
        tags=_json_value(row["tags_json"], []),
        related_kanban_items=_json_value(row["related_kanban_items_json"], []),
        related_tasks=_json_value(row["related_tasks_json"], []),
        related_import_batches=_json_value(row["related_import_batches_json"], []),
        actor=action.actor,
        source_surface=action.source_surface,
        request_id=action.request_id,
        run_id=action.run_id,
    )


@router.patch("/tasks/{task_id}")
async def update_personal_task(task_id: str, body: PersonalTaskUpsertRequest) -> dict[str, Any]:
    clean_task_id = _task_id(task_id, _validate_local_date(body.due_date))
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT task_id FROM personal_time_tasks WHERE task_id=?", (clean_task_id,)
        ).fetchone()
    if not existing:
        raise HTTPException(404, "task not found")
    return _upsert_personal_task(body, task_id=clean_task_id, action="update_task")


@router.post("/tasks/{task_id}/complete")
async def complete_personal_task(task_id: str, body: PersonalTaskActionRequest) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "task not found")
    request = _task_request_from_row(row, body, status="done")
    return _upsert_personal_task(
        request, task_id=task_id, action="complete_task", status_override="done"
    )


@router.post("/tasks/{task_id}/archive")
async def archive_personal_task(task_id: str, body: PersonalTaskActionRequest) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "task not found")
    request = _task_request_from_row(row, body, status="archived")
    return _upsert_personal_task(
        request, task_id=task_id, action="archive_task", status_override="archived"
    )


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


def _task_id(value: str | None, local_date: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", str(value or "").strip()).strip("-")
    if clean:
        return clean[:180]
    return f"task-{local_date}-{uuid.uuid4().hex[:12]}"


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


def _task_status(value: str | None) -> str:
    status = _clean_short_text(value, "open", limit=40)
    if status == "completed":
        return "done"
    if status not in {"open", "blocked", "pending_review", "done", "archived"}:
        raise HTTPException(400, "task status is invalid")
    return status


def _task_mode(value: str | None, related_kanban_items: list[str]) -> str:
    mode = _clean_short_text(value, "personal", limit=40)
    if mode == "today":
        mode = "personal"
    if related_kanban_items and mode == "personal":
        mode = "kanban"
    if mode not in {"personal", "kanban", "review"}:
        raise HTTPException(400, "task mode is invalid")
    return mode


def _task_file_dir(local_date: str) -> Path:
    year, month, day = local_date.split("-")
    return DIARY_ROOT / "tasks" / year / month / day


def _task_file_slug(task_id: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.:-]+", "-", task_id).strip("-")[:180] or "task"


def _write_task_files(payload: dict[str, Any], now: str) -> list[str]:
    task_dir = _task_file_dir(payload["local_date"])
    task_dir.mkdir(parents=True, exist_ok=True)
    slug = _task_file_slug(payload["task_id"])
    json_path = task_dir / f"{slug}.json"
    md_path = task_dir / f"{slug}.md"
    file_payload = {
        "schema": "xarta.todo.task.v1",
        "task_id": payload["task_id"],
        "event_id": payload["event_id"],
        "title": payload["title"],
        "body": payload["body_excerpt"],
        "status": payload["status"],
        "mode": payload["mode"],
        "priority": payload["priority"],
        "due_at": payload["due_at"],
        "local_date": payload["local_date"],
        "timezone": payload["timezone"],
        "privacy_level": payload["privacy_level"],
        "tags": payload["tags"],
        "related_kanban_items": payload["related_kanban_items"],
        "related_tasks": payload["related_tasks"],
        "related_import_batches": payload["related_import_batches"],
        "source_hash": payload["source_hash"],
        "provenance": payload["provenance"],
        "updated_at": now,
    }
    json_path.write_text(
        json.dumps(file_payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(
        "\n".join(
            [
                "---",
                "schema: xarta.todo.task.v1",
                f"task_id: {payload['task_id']}",
                f"event_id: {payload['event_id']}",
                f"status: {payload['status']}",
                f"mode: {payload['mode']}",
                f"local_date: {payload['local_date']}",
                "---",
                "",
                f"# {payload['title']}",
                "",
                payload["body_excerpt"],
                "",
            ]
        ),
        encoding="utf-8",
    )
    return [_diary_relative_path(json_path), _diary_relative_path(md_path)]


def _task_payload(
    body: PersonalTaskUpsertRequest,
    *,
    task_id: str | None = None,
    status_override: str | None = None,
) -> dict[str, Any]:
    local_date = _validate_local_date(body.due_date)
    timezone_name = _validate_timezone_name(body.timezone)
    due_time = _validate_local_time(body.due_time)
    due_at = _calendar_utc_iso(local_date, due_time, timezone_name) if body.due_date else None
    title = _clean_short_text(body.title, "", limit=180)
    if not title:
        raise HTTPException(400, "task title is required")
    related_kanban_items = _clean_event_list(body.related_kanban_items)
    status = _task_status(status_override or body.status)
    mode = _task_mode(body.mode, related_kanban_items)
    tags = _clean_event_list(body.tags)
    mode_tag = KANBAN_TAG if mode == "kanban" else mode
    for required in ("todo", "task", mode_tag):
        if required not in tags:
            tags.append(required)
    if due_at and "due" not in tags:
        tags.append("due")
    if related_kanban_items and KANBAN_TAG not in tags:
        tags.append(KANBAN_TAG)
    clean_task_id = _task_id(task_id or body.task_id, local_date)
    provenance = {
        "task": {
            "mode": mode,
            "due_time": due_time or "",
            "timezone": timezone_name,
        },
        "actor": _clean_short_text(body.actor, "blueprints-ui"),
        "source_surface": _clean_short_text(body.source_surface, "todo-page"),
        "request_id": _clean_short_text(
            body.request_id, f"todo-task-{uuid.uuid4().hex[:12]}", limit=160
        ),
        "run_id": _clean_short_text(
            body.run_id or body.request_id,
            body.request_id or f"todo-run-{uuid.uuid4().hex[:12]}",
            limit=160,
        ),
    }
    payload = {
        "task_id": clean_task_id,
        "event_id": clean_task_id,
        "source_type": "manual-task",
        "source_ref": f"personal_time_tasks:{clean_task_id}",
        "title": title,
        "body_excerpt": _body_excerpt(body.body or "", limit=2000),
        "status": status,
        "mode": mode,
        "priority": _clean_short_text(body.priority, "", limit=40) or None,
        "due_at": due_at,
        "local_date": local_date,
        "timezone": timezone_name,
        "privacy_level": _clean_privacy_level(body.privacy_level),
        "tags": tags,
        "related_kanban_items": related_kanban_items,
        "related_tasks": _clean_event_list(body.related_tasks),
        "related_import_batches": _clean_event_list(body.related_import_batches),
        "provenance": provenance,
    }
    payload["source_hash"] = _hash_json_payload(payload)
    return payload


def _upsert_task_event(
    conn: Any,
    payload: dict[str, Any],
    *,
    file_refs: list[str],
    audit_id: str,
    now: str,
    created_at: str,
) -> Any:
    event_provenance = {
        **payload["provenance"],
        "task": {
            **payload["provenance"]["task"],
            "task_id": payload["task_id"],
            "task_table": "personal_time_tasks",
        },
    }
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, end_at, local_date, timezone, status, priority,
            privacy_level, tags_json, related_kanban_items_json, related_tasks_json,
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
            local_date=excluded.local_date,
            timezone=excluded.timezone,
            status=excluded.status,
            priority=excluded.priority,
            privacy_level=excluded.privacy_level,
            tags_json=excluded.tags_json,
            related_kanban_items_json=excluded.related_kanban_items_json,
            related_tasks_json=excluded.related_tasks_json,
            related_import_batches_json=excluded.related_import_batches_json,
            file_refs_json=excluded.file_refs_json,
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
            "task",
            payload["title"],
            payload["body_excerpt"],
            payload["body_excerpt"],
            payload["due_at"],
            None,
            payload["local_date"],
            payload["timezone"],
            payload["status"],
            payload["priority"],
            payload["privacy_level"],
            json.dumps(payload["tags"], ensure_ascii=True),
            json.dumps(payload["related_kanban_items"], ensure_ascii=True),
            json.dumps([payload["task_id"], *payload["related_tasks"]], ensure_ascii=True),
            json.dumps(payload["related_import_batches"], ensure_ascii=True),
            json.dumps(file_refs, ensure_ascii=True),
            json.dumps(
                [f"personal_time_tasks:{payload['task_id']}", f"personal_time_audit:{audit_id}"],
                ensure_ascii=True,
            ),
            json.dumps(event_provenance, ensure_ascii=True, sort_keys=True),
            "hot",
            "linked",
            now,
            created_at,
            now,
        ),
    )
    return conn.execute(
        "SELECT * FROM personal_events WHERE event_id=?", (payload["event_id"],)
    ).fetchone()


def _upsert_personal_task(
    body: PersonalTaskUpsertRequest,
    *,
    task_id: str | None = None,
    action: str,
    status_override: str | None = None,
) -> dict[str, Any]:
    payload = _task_payload(body, task_id=task_id, status_override=status_override)
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    actor = payload["provenance"]["actor"]
    source_surface = payload["provenance"]["source_surface"]
    request_id = payload["provenance"]["request_id"]
    run_id = payload["provenance"]["run_id"]
    file_refs = _write_task_files(payload, now)
    completed_at = now if payload["status"] == "done" else None
    archived_at = now if payload["status"] == "archived" else None
    with get_conn() as conn:
        source_row = _upsert_task_source(conn, now)
        previous = conn.execute(
            "SELECT created_at, completed_at, archived_at FROM personal_time_tasks WHERE task_id=?",
            (payload["task_id"],),
        ).fetchone()
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        if previous and previous["completed_at"] and payload["status"] == "done":
            completed_at = previous["completed_at"]
        if previous and previous["archived_at"] and payload["status"] == "archived":
            archived_at = previous["archived_at"]
        conn.execute(
            """
            INSERT INTO personal_time_tasks (
                task_id, source_type, source_ref, source_hash, title, body_excerpt,
                status, mode, priority, due_at, local_date, timezone, privacy_level,
                tags_json, related_kanban_items_json, related_tasks_json,
                related_import_batches_json, file_refs_json, db_refs_json, event_id,
                provenance_json, completed_at, archived_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                source_type=excluded.source_type,
                source_ref=excluded.source_ref,
                source_hash=excluded.source_hash,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                status=excluded.status,
                mode=excluded.mode,
                priority=excluded.priority,
                due_at=excluded.due_at,
                local_date=excluded.local_date,
                timezone=excluded.timezone,
                privacy_level=excluded.privacy_level,
                tags_json=excluded.tags_json,
                related_kanban_items_json=excluded.related_kanban_items_json,
                related_tasks_json=excluded.related_tasks_json,
                related_import_batches_json=excluded.related_import_batches_json,
                file_refs_json=excluded.file_refs_json,
                db_refs_json=excluded.db_refs_json,
                event_id=excluded.event_id,
                provenance_json=excluded.provenance_json,
                completed_at=excluded.completed_at,
                archived_at=excluded.archived_at,
                updated_at=excluded.updated_at
            """,
            (
                payload["task_id"],
                payload["source_type"],
                payload["source_ref"],
                payload["source_hash"],
                payload["title"],
                payload["body_excerpt"],
                payload["status"],
                payload["mode"],
                payload["priority"],
                payload["due_at"],
                payload["local_date"],
                payload["timezone"],
                payload["privacy_level"],
                json.dumps(payload["tags"], ensure_ascii=True),
                json.dumps(payload["related_kanban_items"], ensure_ascii=True),
                json.dumps(payload["related_tasks"], ensure_ascii=True),
                json.dumps(payload["related_import_batches"], ensure_ascii=True),
                json.dumps(file_refs, ensure_ascii=True),
                json.dumps(
                    [
                        f"personal_events:{payload['event_id']}",
                        f"personal_time_audit:{audit_id}",
                    ],
                    ensure_ascii=True,
                ),
                payload["event_id"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                completed_at,
                archived_at,
                created_at,
                now,
            ),
        )
        task_row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (payload["task_id"],)
        ).fetchone()
        event_row = _upsert_task_event(
            conn,
            payload,
            file_refs=file_refs,
            audit_id=audit_id,
            now=now,
            created_at=created_at,
        )
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action=action,
            target_ref=f"personal_time_tasks:{payload['task_id']}",
            file_ref=file_refs[0],
            db_ref=f"personal_time_tasks:{payload['task_id']}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=payload["source_hash"],
            metadata={
                "task_id": payload["task_id"],
                "event_id": payload["event_id"],
                "status": payload["status"],
                "mode": payload["mode"],
            },
        )
        gen = increment_gen(conn, "personal-task")
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_sources", "manual-task", dict(source_row), gen
        )
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_time_tasks", payload["task_id"], dict(task_row), gen
        )
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_events", payload["event_id"], dict(event_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "task": _row_to_task(task_row),
        "event": _row_to_event(event_row),
        "audit": {"audit_id": audit_id, "result": "ok", "action": action},
        "write": {"file_refs": file_refs},
    }


def _task_mode_where(mode: str | None) -> tuple[list[str], list[Any]]:
    if not mode:
        return [], []
    if mode not in {"today", "personal", "kanban", "blocked", "review", "done"}:
        raise HTTPException(400, f"unknown task mode: {mode}")
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    if mode == "today":
        return ["local_date = ?", "status IN ('open', 'blocked', 'pending_review')"], [today]
    if mode == "personal":
        return ["mode = 'personal'", "status NOT IN ('done', 'archived')"], []
    if mode == "kanban":
        return ["(mode = 'kanban' OR json_array_length(related_kanban_items_json) > 0)"], []
    if mode == "blocked":
        return ["status = 'blocked'"], []
    if mode == "review":
        return ["(mode = 'review' OR status = 'pending_review')"], []
    return ["status IN ('done', 'archived')"], []


def _event_task_where(mode: str | None) -> tuple[list[str], list[Any]]:
    where = [
        "source_type != 'manual-task'",
        "(kind IN ('todo', 'task', 'action', 'reminder') "
        "OR EXISTS (SELECT 1 FROM json_each(tags_json) WHERE value IN ('todo', 'follow-up')))",
    ]
    params: list[Any] = []
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    if mode == "today":
        where.extend(["local_date = ?", "status IN ('open', 'blocked', 'pending_review')"])
        params.append(today)
    elif mode == "personal":
        where.append(
            "source_type IN ('manual', 'diary-file', 'manual-calendar', 'hermes-minutes', 'browser-links')"
        )
        where.append("json_array_length(related_kanban_items_json) = 0")
        where.append("status NOT IN ('done', 'archived')")
    elif mode == "kanban":
        where.append("(source_type = 'kanban' OR json_array_length(related_kanban_items_json) > 0)")
    elif mode == "blocked":
        where.append("status = 'blocked'")
    elif mode == "review":
        where.append("(status = 'pending_review' OR provenance_state = 'needs_review')")
    elif mode == "done":
        where.append("status IN ('done', 'completed', 'archived')")
    elif mode:
        raise HTTPException(400, f"unknown task mode: {mode}")
    return where, params


def _task_counts(conn: Any, *, show_test_entries: bool = True) -> dict[str, int]:
    counts = {"today": 0, "personal": 0, "kanban": 0, "blocked": 0, "review": 0, "done": 0}
    today = datetime.now().astimezone().strftime("%Y-%m-%d")
    today_row = conn.execute(
        """
        SELECT COUNT(*) AS count FROM personal_time_tasks
        WHERE local_date=? AND status IN ('open', 'blocked', 'pending_review')
          AND privacy_level != 'pin'
        """,
        (today,),
    ).fetchone()
    counts["today"] = int(today_row["count"] if today_row else 0)
    rows = conn.execute(
        """
        SELECT status, mode, COUNT(*) AS count
        FROM personal_time_tasks
        WHERE privacy_level != 'pin'
        GROUP BY status, mode
        """
    ).fetchall()
    for row in rows:
        status = row["status"]
        mode = row["mode"]
        count = int(row["count"])
        if mode in {"personal", "kanban", "review"} and status not in {"done", "archived"}:
            counts[mode] += count
        if status == "blocked":
            counts["blocked"] += count
        if status in {"done", "archived"}:
            counts["done"] += count
    kanban_todo_rows = _kanban_todo_task_page_rows(
        conn,
        mode="kanban",
        date_start=None,
        date_end=None,
        status=None,
        source_type=None,
        related_kanban_item=None,
        fetch_limit=10000,
    )
    kanban_todo_rows, _hidden = _filter_kanban_todo_test_rows(
        conn,
        kanban_todo_rows,
        show_test_entries,
    )
    kanban_todo_counts: dict[str, int] = {}
    for row in kanban_todo_rows:
        status = row["status"]
        kanban_todo_counts[status] = kanban_todo_counts.get(status, 0) + 1
    for status, count in kanban_todo_counts.items():
        if status != "archived":
            counts["kanban"] += count
        if status == "blocked":
            counts["blocked"] += count
        if status in {"done", "archived", "promoted"}:
            counts["done"] += count
    return counts


def _kanban_todo_task_page_rows(
    conn: Any,
    *,
    mode: str | None,
    date_start: str | None,
    date_end: str | None,
    status: str | None,
    source_type: str | None,
    related_kanban_item: str | None,
    fetch_limit: int,
) -> list[Any]:
    work_source_filters = {
        "kanban-todo",
        "kanban_todo",
        "kanban_items",
        "kanban",
    }
    include = mode == "kanban" or bool(related_kanban_item)
    if source_type:
        include = source_type in work_source_filters
    if not include:
        return []
    where: list[str] = ["item_type != 'issue'"]
    params: list[Any] = []
    if status:
        where.append("status = ?")
        params.append(status)
    elif mode == "kanban":
        where.append("status != 'archived'")
    if related_kanban_item:
        where.append("(parent_item_id = ? OR item_id = ?)")
        params.extend([related_kanban_item, related_kanban_item])
    if date_start:
        where.append(
            "COALESCE(substr(json_extract(provenance_json, '$.todo.due_at'), 1, 10), '') >= ?"
        )
        params.append(date_start)
    if date_end:
        where.append(
            "COALESCE(substr(json_extract(provenance_json, '$.todo.due_at'), 1, 10), '') <= ?"
        )
        params.append(date_end)
    clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT * FROM kanban_items
        {clause}
        ORDER BY COALESCE(json_extract(provenance_json, '$.todo.due_at'), updated_at, created_at) ASC, item_id ASC
        LIMIT ?
        """,
        (*params, fetch_limit),
    ).fetchall()
    return [row for row in rows if _work_row_has_filter_tag(row, "todo")]


def _clean_work_id(value: str | None, prefix: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_.:-]+", "-", str(value or "").strip()).strip("-")
    if clean:
        return clean[:180]
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _work_request_meta(body: Any) -> dict[str, str]:
    request_id = _clean_short_text(
        getattr(body, "request_id", None), f"kanban-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(getattr(body, "run_id", None) or request_id, request_id, limit=160)
    return {
        "actor": _clean_short_text(getattr(body, "actor", None), "blueprints-ui"),
        "source_surface": _clean_short_text(getattr(body, "source_surface", None), "kanban-api"),
        "request_id": request_id,
        "run_id": run_id,
    }


def _require_work_state(conn: Any, state_id: str | None) -> Any:
    clean_state = _clean_short_text(state_id, "todo", limit=80)
    row = conn.execute(
        "SELECT * FROM kanban_item_states WHERE state_id=?", (clean_state,)
    ).fetchone()
    if not row:
        raise HTTPException(400, "Kanban item state is invalid")
    return row


def _require_work_priority(conn: Any, priority_id: str | None) -> Any:
    clean_priority = _clean_short_text(priority_id, "medium", limit=80)
    row = conn.execute(
        "SELECT * FROM kanban_item_priorities WHERE priority_id=?", (clean_priority,)
    ).fetchone()
    if not row:
        raise HTTPException(400, "Kanban item priority is invalid")
    return row


def _work_status_for_state(state_row: Any) -> str:
    category = state_row["status_category"]
    if category == "done":
        return "done"
    if category == "blocked":
        return "blocked"
    if category == "active":
        return "active"
    return "open"


def _work_item_or_404(conn: Any, item_id: str) -> Any:
    row = conn.execute("SELECT * FROM kanban_items WHERE item_id=?", (item_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Kanban item not found")
    return row


def _work_root_item(conn: Any, item_id: str) -> Any:
    current = _work_item_or_404(conn, item_id)
    seen: set[str] = set()
    while current["parent_item_id"]:
        if current["item_id"] in seen:
            raise HTTPException(400, "Kanban item parent cycle detected")
        seen.add(current["item_id"])
        current = _work_item_or_404(conn, current["parent_item_id"])
    return current


def _kanban_projects_manifest_path() -> Path:
    return KANBAN_ROOT / "projects.json"


def _read_kanban_projects_manifest() -> dict[str, Any]:
    path = _kanban_projects_manifest_path()
    fallback = {"schema": "xarta.kanban.projects.v1", "projects": {}}
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    if not isinstance(parsed, dict):
        return fallback
    projects = parsed.get("projects")
    if not isinstance(projects, dict):
        parsed["projects"] = {}
    parsed.setdefault("schema", "xarta.kanban.projects.v1")
    return parsed


def _write_kanban_projects_manifest(manifest: dict[str, Any]) -> None:
    KANBAN_ROOT.mkdir(parents=True, exist_ok=True)
    manifest["schema"] = "xarta.kanban.projects.v1"
    manifest.setdefault("projects", {})
    path = _kanban_projects_manifest_path()
    tmp_path = path.with_suffix(".json.tmp")
    tmp_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp_path.replace(path)


def _kanban_project_slug_available(
    slug: str,
    *,
    manifest: dict[str, Any],
    root_item_id: str,
    current_folder: str = "",
) -> bool:
    projects = manifest.get("projects") if isinstance(manifest.get("projects"), dict) else {}
    for other_id, entry in projects.items():
        if other_id == root_item_id:
            continue
        if isinstance(entry, dict) and entry.get("folder") == slug:
            return False
    if slug == current_folder:
        return True
    path = KANBAN_ROOT / slug
    return not path.exists()


def _allocate_kanban_project_slug(
    title: str | None,
    *,
    manifest: dict[str, Any],
    root_item_id: str,
    current_folder: str = "",
) -> str:
    base = _safe_kanban_slug(title or root_item_id, "root-project")
    candidate = base
    suffix = 1
    while not _kanban_project_slug_available(
        candidate,
        manifest=manifest,
        root_item_id=root_item_id,
        current_folder=current_folder,
    ):
        suffix += 1
        candidate = f"{base}-{suffix}"
    return candidate


def _kanban_root_project_slug(conn: Any, root_item: Any, *, ensure_manifest: bool = False) -> str:
    manifest = _read_kanban_projects_manifest()
    projects = manifest.setdefault("projects", {})
    root_id = root_item["item_id"]
    entry = projects.get(root_id) if isinstance(projects.get(root_id), dict) else None
    if entry and entry.get("folder"):
        if ensure_manifest and entry.get("title") != root_item["title"]:
            entry["title"] = root_item["title"]
            entry["updated_at"] = _utc_now_iso()
            _write_kanban_projects_manifest(manifest)
        return str(entry["folder"])
    folder = _allocate_kanban_project_slug(
        root_item["title"], manifest=manifest, root_item_id=root_id
    )
    if ensure_manifest:
        projects[root_id] = {
            "item_id": root_id,
            "title": root_item["title"],
            "folder": folder,
            "pending": None,
            "updated_at": _utc_now_iso(),
        }
        _write_kanban_projects_manifest(manifest)
    return folder


def _sync_kanban_root_project_title(
    conn: Any,
    *,
    root_item_id: str,
    old_title: str,
    new_title: str,
    now: str,
) -> dict[str, Any]:
    manifest = _read_kanban_projects_manifest()
    projects = manifest.setdefault("projects", {})
    entry = projects.get(root_item_id) if isinstance(projects.get(root_item_id), dict) else None
    old_folder = (
        str(entry.get("folder"))
        if entry and entry.get("folder")
        else _safe_kanban_slug(old_title or root_item_id, "root-project")
    )
    new_folder = _allocate_kanban_project_slug(
        new_title,
        manifest=manifest,
        root_item_id=root_item_id,
        current_folder=old_folder,
    )
    if old_folder == new_folder:
        projects[root_item_id] = {
            "item_id": root_item_id,
            "title": new_title,
            "folder": old_folder,
            "pending": None,
            "updated_at": now,
        }
        _write_kanban_projects_manifest(manifest)
        return {"folder": old_folder, "renamed": False}
    projects[root_item_id] = {
        "item_id": root_item_id,
        "title": old_title,
        "folder": old_folder,
        "pending": {
            "action": "rename",
            "from": old_folder,
            "to": new_folder,
            "title": new_title,
            "started_at": now,
        },
        "updated_at": now,
    }
    _write_kanban_projects_manifest(manifest)
    old_path = KANBAN_ROOT / old_folder
    new_path = KANBAN_ROOT / new_folder
    renamed = old_path.exists()
    if old_path.exists():
        if new_path.exists():
            raise HTTPException(409, "kanban project folder target already exists")
        old_path.rename(new_path)
    projects[root_item_id] = {
        "item_id": root_item_id,
        "title": new_title,
        "folder": new_folder,
        "previous_folder": old_folder,
        "pending": None,
        "updated_at": now,
    }
    _write_kanban_projects_manifest(manifest)
    return {"folder": new_folder, "previous_folder": old_folder, "renamed": renamed}


def _kanban_item_dir(conn: Any, item_id: str, *, ensure_project: bool = False) -> Path:
    root = _work_root_item(conn, item_id)
    project_slug = _kanban_root_project_slug(conn, root, ensure_manifest=ensure_project)
    item_slug = _safe_kanban_slug(item_id, "item")
    return KANBAN_ROOT / project_slug / "items" / item_slug


def _kanban_item_detail_path(conn: Any, item_id: str, *, ensure_project: bool = False) -> Path:
    return _kanban_item_dir(conn, item_id, ensure_project=ensure_project) / "detail.md"


def _kanban_discussion_path(
    conn: Any, item_id: str, discussion_id: str, *, ensure_project: bool = False
) -> Path:
    safe_discussion = _safe_kanban_slug(discussion_id, "discussion")
    return (
        _kanban_item_dir(conn, item_id, ensure_project=ensure_project)
        / "discussions"
        / f"{safe_discussion}.md"
    )


def _kanban_document_ref(path: Path) -> dict[str, Any]:
    return {
        "root": "kanban",
        "path": _kanban_relative_path(path),
    }


def _work_item_detail_document(conn: Any, item_id: str) -> dict[str, Any]:
    item = _work_item_or_404(conn, item_id)
    path = _kanban_item_detail_path(conn, item_id)
    metadata, body, exists = _read_kanban_markdown_document(path)
    return {
        "schema": metadata.get("schema") or KANBAN_ITEM_DETAIL_SCHEMA,
        "item_id": item_id,
        "body": body,
        "exists": exists,
        "file_ref": _kanban_document_ref(path),
        "metadata": metadata,
        "updated_at": metadata.get("updated_at") or item["updated_at"],
    }


def _write_work_item_detail_document(
    conn: Any,
    item_id: str,
    body: str,
    *,
    actor: str,
    now: str,
) -> dict[str, Any]:
    item = _work_item_or_404(conn, item_id)
    path = _kanban_item_detail_path(conn, item_id, ensure_project=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_body = _normalise_markdown_document_body(body)
    metadata = {
        "item_id": item_id,
        "root_item_id": _work_root_item(conn, item_id)["item_id"],
        "title": item["title"],
        "actor": actor,
        "updated_at": now,
    }
    path.write_text(
        _kanban_markdown_text(KANBAN_ITEM_DETAIL_SCHEMA, metadata, clean_body),
        encoding="utf-8",
    )
    return _work_item_detail_document(conn, item_id)


def _work_discussion_document(conn: Any, row: Any) -> dict[str, Any]:
    path = _kanban_discussion_path(conn, row["item_id"], row["discussion_id"])
    metadata, body, exists = _read_kanban_markdown_document(path)
    if not exists:
        body = row["body_excerpt"] or ""
    return {
        "schema": metadata.get("schema") or KANBAN_DISCUSSION_SCHEMA,
        "discussion_id": row["discussion_id"],
        "item_id": row["item_id"],
        "body": body,
        "exists": exists,
        "file_ref": _kanban_document_ref(path),
        "metadata": metadata,
        "updated_at": metadata.get("updated_at") or row["updated_at"],
    }


def _write_work_discussion_document(
    conn: Any,
    row: Any,
    body: str,
    *,
    actor: str,
    now: str,
) -> dict[str, Any]:
    path = _kanban_discussion_path(conn, row["item_id"], row["discussion_id"], ensure_project=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_body = _normalise_markdown_document_body(body)
    metadata = {
        "discussion_id": row["discussion_id"],
        "item_id": row["item_id"],
        "author": row["author"],
        "status": row["status"],
        "actor": actor,
        "created_at": row["created_at"],
        "updated_at": now,
    }
    path.write_text(
        _kanban_markdown_text(KANBAN_DISCUSSION_SCHEMA, metadata, clean_body),
        encoding="utf-8",
    )
    return _work_discussion_document(conn, row)


def _work_parent_depth(conn: Any, parent_item_id: str | None, *, moving_item_id: str = "") -> int:
    parent_id = _clean_short_text(parent_item_id, "", limit=180) or None
    if not parent_id:
        return 0
    if moving_item_id and parent_id == moving_item_id:
        raise HTTPException(400, "Kanban item cannot be its own parent")
    immediate_parent = conn.execute(
        "SELECT item_id, parent_item_id, depth FROM kanban_items WHERE item_id=?", (parent_id,)
    ).fetchone()
    if not immediate_parent:
        raise HTTPException(404, "parent Kanban item not found")
    current = parent_id
    seen: set[str] = set()
    while current:
        if current in seen:
            raise HTTPException(400, "Kanban item parent cycle detected")
        seen.add(current)
        row = conn.execute(
            "SELECT item_id, parent_item_id, depth FROM kanban_items WHERE item_id=?", (current,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "parent Kanban item not found")
        if moving_item_id and row["item_id"] == moving_item_id:
            raise HTTPException(400, "Kanban item cannot move under its descendant")
        current = row["parent_item_id"]
    depth = int(immediate_parent["depth"]) + 1
    if depth > KANBAN_DEPTH_LIMIT:
        raise HTTPException(400, "Kanban item depth limit exceeded")
    return depth


def _work_subtree_max_relative_depth(conn: Any, item_id: str) -> int:
    row = conn.execute(
        """
        WITH RECURSIVE descendants(item_id, rel_depth) AS (
            SELECT item_id, 0 FROM kanban_items WHERE item_id=?
            UNION ALL
            SELECT w.item_id, descendants.rel_depth + 1
            FROM kanban_items w
            JOIN descendants ON w.parent_item_id = descendants.item_id
        )
        SELECT COALESCE(MAX(rel_depth), 0) AS max_depth FROM descendants
        """,
        (item_id,),
    ).fetchone()
    return int(row["max_depth"] if row else 0)


def _recompute_work_child_depths(conn: Any, item_id: str, depth: int) -> None:
    rows = conn.execute(
        "SELECT item_id FROM kanban_items WHERE parent_item_id=?", (item_id,)
    ).fetchall()
    for row in rows:
        child_depth = depth + 1
        conn.execute(
            "UPDATE kanban_items SET depth=?, updated_at=? WHERE item_id=?",
            (child_depth, _utc_now_iso(), row["item_id"]),
        )
        _recompute_work_child_depths(conn, row["item_id"], child_depth)


def _work_search_payload(
    *,
    table_name: str,
    row_id: str,
    kind: str,
    title: str,
    body: str,
    tags: list[str] | None = None,
    related_refs: list[str] | None = None,
) -> tuple[str, dict[str, Any], str]:
    tag_values = tags or []
    search_text = "\n".join(part for part in [title, body, " ".join(tag_values)] if part)
    vector_key = f"{table_name}:{row_id}"
    metadata = {
        "schema": "xarta.kanban.search_metadata.v1",
        "table": table_name,
        "row_id": row_id,
        "kind": kind,
        "related_refs": related_refs or [],
        "embedding": {
            "state": "pending",
            "ref": "",
            "model": "",
        },
        "vector": {
            "index": "kanban",
            "key": vector_key,
            "turbo_vec_ready": True,
        },
    }
    return search_text, metadata, vector_key


def _write_work_audit(
    conn: Any,
    *,
    audit_id: str,
    actor: str,
    source_surface: str,
    action: str,
    target_ref: str,
    item_id: str,
    parent_item_id: str,
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
        "item_id": item_id,
        "parent_item_id": parent_item_id,
        "created_at": created_at,
        "request_id": request_id,
        "run_id": run_id,
        "result": result,
        "source_hash": source_hash,
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
    }
    conn.execute(
        """
        INSERT INTO kanban_audit_log (
            audit_id, actor, source_surface, action, target_ref, item_id, parent_item_id,
            created_at, request_id, run_id, result, source_hash, metadata_json
        )
        VALUES (
            :audit_id, :actor, :source_surface, :action, :target_ref, :item_id,
            :parent_item_id, :created_at, :request_id, :run_id, :result,
            :source_hash, :metadata_json
        )
        ON CONFLICT(audit_id) DO UPDATE SET
            actor=excluded.actor,
            source_surface=excluded.source_surface,
            action=excluded.action,
            target_ref=excluded.target_ref,
            item_id=excluded.item_id,
            parent_item_id=excluded.parent_item_id,
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


def _work_scope_item_ids(
    conn: Any,
    item_id: str | None,
    *,
    show_test_entries: bool = True,
) -> list[str]:
    if not item_id:
        rows = conn.execute("SELECT * FROM kanban_items WHERE status != 'archived'").fetchall()
        rows, _hidden = _filter_work_test_rows(rows, show_test_entries)
        return [row["item_id"] for row in rows]
    _work_item_or_404(conn, item_id)
    rows = conn.execute(
        """
        WITH RECURSIVE descendants(item_id) AS (
            SELECT item_id FROM kanban_items WHERE item_id=?
            UNION ALL
            SELECT w.item_id
            FROM kanban_items w
            JOIN descendants ON w.parent_item_id = descendants.item_id
            WHERE w.status != 'archived'
        )
        SELECT item_id FROM descendants
        """,
        (item_id,),
    ).fetchall()
    item_ids = [row["item_id"] for row in rows]
    if show_test_entries or not item_ids:
        return item_ids
    placeholders = ",".join("?" for _ in item_ids)
    scoped_rows = conn.execute(
        f"SELECT * FROM kanban_items WHERE item_id IN ({placeholders})",
        item_ids,
    ).fetchall()
    scoped_rows, _hidden = _filter_work_test_rows(scoped_rows, show_test_entries)
    return [row["item_id"] for row in scoped_rows]


def _work_scope_item_rows(conn: Any, item_id: str, scope: str) -> list[Any]:
    clean_scope = _clean_short_text(scope, "local", limit=40).replace("-", "_")
    if clean_scope in {"local", "self"}:
        return conn.execute(
            "SELECT *, 0 AS rel_depth FROM kanban_items WHERE item_id=?",
            (item_id,),
        ).fetchall()
    if clean_scope not in {"descendant", "descendants", "tree", "subtree"}:
        raise HTTPException(400, "Kanban scope is invalid")
    _work_item_or_404(conn, item_id)
    return conn.execute(
        """
        WITH RECURSIVE scoped(item_id, rel_depth) AS (
            SELECT item_id, 0 FROM kanban_items WHERE item_id=?
            UNION ALL
            SELECT w.item_id, scoped.rel_depth + 1
            FROM kanban_items w
            JOIN scoped ON w.parent_item_id = scoped.item_id
            WHERE w.status != 'archived'
        )
        SELECT w.*, scoped.rel_depth
        FROM scoped
        JOIN kanban_items w ON w.item_id = scoped.item_id
        ORDER BY scoped.rel_depth, COALESCE(w.parent_item_id, ''), w.sort_order, w.updated_at DESC, w.item_id
        """,
        (item_id,),
    ).fetchall()


def _work_scoped_leaf_payload(
    conn: Any,
    *,
    item_id: str,
    kind: str,
    scope: str,
    view: str,
) -> dict[str, Any]:
    root = _work_item_or_404(conn, item_id)
    clean_kind = _clean_short_text(kind, "", limit=40)
    if clean_kind not in {"issues", "todos"}:
        raise HTTPException(400, "Kanban scoped leaf kind is invalid")
    clean_view = _clean_short_text(view, "flat", limit=40).replace("-", "_")
    if clean_view not in {"flat", "grouped", "tree"}:
        raise HTTPException(400, "Kanban scoped leaf view is invalid")
    scope_rows = _work_scope_item_rows(conn, item_id, scope)
    item_ids = [row["item_id"] for row in scope_rows]
    scope_by_item_id = {
        row["item_id"]: {
            "item_id": row["item_id"],
            "parent_item_id": row["parent_item_id"] or "",
            "title": row["title"],
            "depth": int(row["depth"]),
            "depth_offset": int(row["rel_depth"]),
            "relation": "self" if int(row["rel_depth"]) == 0 else "descendant",
        }
        for row in scope_rows
    }
    rows: list[Any] = []
    if item_ids:
        placeholders = ",".join("?" for _ in item_ids)
        if clean_kind == "issues":
            rows = conn.execute(
                f"""
                SELECT * FROM kanban_items
                WHERE item_type='issue' AND parent_item_id IN ({placeholders})
                ORDER BY updated_at DESC, item_id
                """,
                item_ids,
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT w.* FROM kanban_items w
                WHERE w.parent_item_id IN ({placeholders})
                  AND w.state_id='todo'
                  AND w.status != 'archived'
                  AND w.item_type != 'issue'
                  AND NOT EXISTS (
                      SELECT 1 FROM kanban_items child
                      WHERE child.parent_item_id=w.item_id
                        AND child.status != 'archived'
                  )
                ORDER BY COALESCE(json_extract(w.provenance_json, '$.todo.due_at'), w.updated_at), w.item_id
                """,
                item_ids,
            ).fetchall()
    row_mapper = _row_to_work_issue if clean_kind == "issues" else _row_to_work_todo
    records: list[dict[str, Any]] = []
    counts_by_status: dict[str, int] = {}
    counts_by_item: dict[str, int] = {item_ref: 0 for item_ref in item_ids}
    for row in rows:
        record = row_mapper(row)
        scope_info = scope_by_item_id.get(row["parent_item_id"] or "", {})
        record["scope"] = scope_info
        records.append(record)
        counts_by_status[row["status"]] = counts_by_status.get(row["status"], 0) + 1
        parent_item_id = row["parent_item_id"] or ""
        counts_by_item[parent_item_id] = counts_by_item.get(parent_item_id, 0) + 1
    groups: list[dict[str, Any]] = []
    if clean_view in {"grouped", "tree"}:
        records_by_item: dict[str, list[dict[str, Any]]] = {item_ref: [] for item_ref in item_ids}
        for record in records:
            records_by_item.setdefault(record["parent_item_id"], []).append(record)
        for row in scope_rows:
            group = {
                "item": _row_to_work_item(row),
                "scope": scope_by_item_id[row["item_id"]],
                "count": len(records_by_item.get(row["item_id"], [])),
                clean_kind: records_by_item.get(row["item_id"], []),
            }
            if clean_view == "tree":
                group["child_item_ids"] = [
                    child["item_id"]
                    for child in scope_rows
                    if (child["parent_item_id"] or "") == row["item_id"]
                ]
            groups.append(group)
    return {
        "ok": True,
        "item": _row_to_work_item(root),
        "breadcrumbs": _work_breadcrumbs(conn, item_id),
        "kind": clean_kind,
        "scope": "local"
        if len(scope_rows) == 1 and scope_rows[0]["item_id"] == item_id
        else "descendants",
        "view": clean_view,
        "items": records,
        "groups": groups,
        "scope_items": [
            {
                "item": _row_to_work_item(row),
                "scope": scope_by_item_id[row["item_id"]],
            }
            for row in scope_rows
        ],
        "counts": {
            "total": len(records),
            "scope_items": len(scope_rows),
            "descendant_items": sum(1 for row in scope_rows if int(row["rel_depth"]) > 0),
            "by_status": counts_by_status,
            "by_item": counts_by_item,
        },
    }


def _work_breadcrumbs(conn: Any, item_id: str | None) -> list[dict[str, Any]]:
    current = _clean_short_text(item_id, "", limit=180)
    if not current:
        return []
    rows: list[Any] = []
    seen: set[str] = set()
    while current:
        if current in seen:
            raise HTTPException(400, "Kanban item parent cycle detected")
        seen.add(current)
        row = _work_item_or_404(conn, current)
        rows.append(row)
        current = row["parent_item_id"]
        if len(rows) > KANBAN_DEPTH_LIMIT + 2:
            raise HTTPException(400, "Kanban item parent cycle detected")
    return [_row_to_work_item(row) for row in reversed(rows)]


def _work_rollup(
    conn: Any,
    item_id: str | None = None,
    *,
    show_test_entries: bool = True,
) -> dict[str, Any]:
    item_ids = _work_scope_item_ids(conn, item_id, show_test_entries=show_test_entries)
    if not item_ids:
        return {
            "items": {"total": 0, "by_state": {}, "by_status": {}},
            "issues": {"open": 0},
            "todos": {"open": 0},
            "blockers": {"open": 0},
            "depth_limit": KANBAN_DEPTH_LIMIT,
        }
    placeholders = ",".join("?" for _ in item_ids)
    state_rows = conn.execute(
        f"SELECT state_id, COUNT(*) AS count FROM kanban_items WHERE item_id IN ({placeholders}) "
        "GROUP BY state_id",
        item_ids,
    ).fetchall()
    status_rows = conn.execute(
        f"SELECT status, COUNT(*) AS count FROM kanban_items WHERE item_id IN ({placeholders}) "
        "GROUP BY status",
        item_ids,
    ).fetchall()
    descendant_item_ids = [
        scope_item_id for scope_item_id in item_ids if not item_id or scope_item_id != item_id
    ]
    if descendant_item_ids:
        descendant_placeholders = ",".join("?" for _ in descendant_item_ids)
        issue_open = conn.execute(
            f"SELECT COUNT(*) AS count FROM kanban_items WHERE item_id IN ({descendant_placeholders}) "
            "AND item_type='issue' AND status NOT IN ('done', 'closed', 'archived')",
            descendant_item_ids,
        ).fetchone()
        todo_open = conn.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM kanban_items w
            WHERE w.item_id IN ({descendant_placeholders})
              AND w.state_id='todo'
              AND w.status != 'archived'
              AND w.item_type != 'issue'
              AND NOT EXISTS (
                  SELECT 1 FROM kanban_items child
                  WHERE child.parent_item_id=w.item_id
                    AND child.status != 'archived'
              )
            """,
            descendant_item_ids,
        ).fetchone()
    else:
        issue_open = {"count": 0}
        todo_open = {"count": 0}
    blocker_open = conn.execute(
        f"SELECT COUNT(*) AS count FROM kanban_blockers WHERE item_id IN ({placeholders}) "
        "AND status NOT IN ('resolved', 'archived')",
        item_ids,
    ).fetchone()
    return {
        "items": {
            "total": len(item_ids),
            "by_state": {row["state_id"]: row["count"] for row in state_rows},
            "by_status": {row["status"]: row["count"] for row in status_rows},
        },
        "issues": {"open": int(issue_open["count"] if issue_open else 0)},
        "todos": {"open": int(todo_open["count"] if todo_open else 0)},
        "blockers": {"open": int(blocker_open["count"] if blocker_open else 0)},
        "depth_limit": KANBAN_DEPTH_LIMIT,
    }


def _work_priority_sort_map(conn: Any) -> dict[str, dict[str, int]]:
    rows = conn.execute(
        "SELECT priority_id, weight, sort_order FROM kanban_item_priorities"
    ).fetchall()
    return {
        row["priority_id"]: {
            "weight": int(row["weight"] or 0),
            "sort_order": int(row["sort_order"] or 0),
        }
        for row in rows
    }


def _bool_setting_value(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "show"}:
        return True
    if text in {"0", "false", "no", "off", "hide"}:
        return False
    return default


def _kanban_preferences(conn: Any) -> dict[str, Any]:
    return {
        "show_test_entries": _bool_setting_value(
            get_setting(conn, KANBAN_SHOW_TEST_ENTRIES_SETTING),
            default=True,
        )
    }


def _work_item_is_test_entry(row: Any) -> bool:
    tags = {
        str(tag).strip().lower() for tag in _json_value(row["tags_json"], []) if str(tag).strip()
    }
    return KANBAN_AGENT_WORKING_OUT_TAG in tags


def _filter_work_test_rows(rows: list[Any], show_test_entries: bool) -> tuple[list[Any], int]:
    if show_test_entries:
        return list(rows), 0
    visible: list[Any] = []
    hidden = 0
    for row in rows:
        if _work_item_is_test_entry(row):
            hidden += 1
        else:
            visible.append(row)
    return visible, hidden


def _filter_kanban_todo_test_rows(
    conn: Any,
    rows: list[Any],
    show_test_entries: bool,
) -> tuple[list[Any], int]:
    if show_test_entries or not rows:
        return list(rows), 0
    item_ids = sorted({row["item_id"] for row in rows if row["item_id"]})
    if not item_ids:
        return list(rows), 0
    placeholders = ",".join("?" for _ in item_ids)
    item_rows = conn.execute(
        f"SELECT * FROM kanban_items WHERE item_id IN ({placeholders})",
        item_ids,
    ).fetchall()
    hidden_item_ids = {row["item_id"] for row in item_rows if _work_item_is_test_entry(row)}
    visible = [row for row in rows if row["item_id"] not in hidden_item_ids]
    return visible, len(rows) - len(visible)


def _work_lane_parent_key(parent_item_id: str | None) -> str:
    return _clean_short_text(parent_item_id, "", limit=180)


def _work_order_edge_id(
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    before_item_id: str,
    after_item_id: str,
) -> str:
    digest = _hash_json_payload(
        {
            "parent_item_id": _work_lane_parent_key(parent_item_id),
            "state_id": state_id,
            "priority_id": priority_id,
            "before_item_id": before_item_id,
            "after_item_id": after_item_id,
        }
    )[:24]
    return f"kanban-order-{digest}"


def _work_order_group_rows(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
) -> list[Any]:
    parent_key = _work_lane_parent_key(parent_item_id)
    if parent_key:
        return conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND state_id=? AND priority_id=? AND status != 'archived'
            """,
            (parent_key, state_id, priority_id),
        ).fetchall()
    return conn.execute(
        """
        SELECT * FROM kanban_items
        WHERE parent_item_id IS NULL AND state_id=? AND priority_id=? AND status != 'archived'
        """,
        (state_id, priority_id),
    ).fetchall()


def _work_order_edges_for_group(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    item_ids: set[str],
) -> list[Any]:
    if not item_ids:
        return []
    rows = conn.execute(
        """
        SELECT * FROM kanban_item_order_edges
        WHERE parent_item_id=? AND state_id=? AND priority_id=?
        ORDER BY updated_at DESC, edge_id
        """,
        (_work_lane_parent_key(parent_item_id), state_id, priority_id),
    ).fetchall()
    return [
        row
        for row in rows
        if row["before_item_id"] in item_ids
        and row["after_item_id"] in item_ids
        and row["before_item_id"] != row["after_item_id"]
    ]


def _order_work_priority_group(conn: Any, rows: list[Any]) -> list[Any]:
    if len(rows) <= 1:
        return list(rows)
    fallback = sorted(
        rows,
        key=lambda row: (
            int(row["sort_order"] or 0),
            str(row["created_at"] or ""),
            row["item_id"],
        ),
    )
    fallback_index = {row["item_id"]: index for index, row in enumerate(fallback)}
    row_by_id = {row["item_id"]: row for row in fallback}
    item_ids = set(row_by_id)
    first = fallback[0]
    edges = _work_order_edges_for_group(
        conn,
        first["parent_item_id"],
        first["state_id"],
        first["priority_id"],
        item_ids,
    )
    if not edges:
        return fallback
    adjacency: dict[str, set[str]] = {item_id: set() for item_id in item_ids}
    indegree: dict[str, int] = {item_id: 0 for item_id in item_ids}
    seen_pairs: set[tuple[str, str]] = set()
    for edge in edges:
        before = edge["before_item_id"]
        after = edge["after_item_id"]
        pair = (before, after)
        if pair in seen_pairs or after in adjacency[before]:
            continue
        seen_pairs.add(pair)
        adjacency[before].add(after)
        indegree[after] += 1
    ready = sorted(
        [item_id for item_id, value in indegree.items() if value == 0],
        key=lambda item_id: fallback_index[item_id],
    )
    ordered_ids: list[str] = []
    while ready:
        item_id = ready.pop(0)
        ordered_ids.append(item_id)
        for after in sorted(adjacency[item_id], key=lambda value: fallback_index[value]):
            indegree[after] -= 1
            if indegree[after] == 0:
                ready.append(after)
                ready.sort(key=lambda value: fallback_index[value])
    if len(ordered_ids) < len(item_ids):
        ordered_ids.extend(row["item_id"] for row in fallback if row["item_id"] not in ordered_ids)
    return [row_by_id[item_id] for item_id in ordered_ids]


def _sort_kanban_items_for_lane(conn: Any, rows: list[Any]) -> list[Any]:
    if len(rows) <= 1:
        return list(rows)
    priorities = _work_priority_sort_map(conn)
    groups: dict[str, list[Any]] = {}
    for row in rows:
        groups.setdefault(row["priority_id"] or "medium", []).append(row)
    priority_ids = sorted(
        groups,
        key=lambda priority_id: (
            -priorities.get(priority_id, {}).get("weight", 0),
            -priorities.get(priority_id, {}).get("sort_order", 0),
            priority_id,
        ),
    )
    ordered: list[Any] = []
    for priority_id in priority_ids:
        ordered.extend(_order_work_priority_group(conn, groups[priority_id]))
    return ordered


def _work_item_has_order_relation(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    item_id: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM kanban_item_order_edges
        WHERE parent_item_id=? AND state_id=? AND priority_id=?
          AND (before_item_id=? OR after_item_id=?)
        LIMIT 1
        """,
        (_work_lane_parent_key(parent_item_id), state_id, priority_id, item_id, item_id),
    ).fetchone()
    return bool(row)


def _replace_kanban_item_order_edges(
    conn: Any,
    *,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    ordered_ids: list[str],
    now: str,
    meta: dict[str, str],
) -> list[tuple[str, str, dict[str, Any]]]:
    parent_key = _work_lane_parent_key(parent_item_id)
    clean_ids = [item_id for item_id in ordered_ids if item_id]
    wanted_pairs = list(zip(clean_ids, clean_ids[1:]))
    wanted_edge_ids = {
        _work_order_edge_id(parent_key, state_id, priority_id, before, after)
        for before, after in wanted_pairs
    }
    sync_changes: list[tuple[str, str, dict[str, Any]]] = []
    existing = conn.execute(
        """
        SELECT * FROM kanban_item_order_edges
        WHERE parent_item_id=? AND state_id=? AND priority_id=?
        """,
        (parent_key, state_id, priority_id),
    ).fetchall()
    for row in existing:
        if row["edge_id"] in wanted_edge_ids:
            continue
        conn.execute("DELETE FROM kanban_item_order_edges WHERE edge_id=?", (row["edge_id"],))
        sync_changes.append(("DELETE", row["edge_id"], {}))
    for before, after in wanted_pairs:
        edge_id = _work_order_edge_id(parent_key, state_id, priority_id, before, after)
        provenance = {
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
            "before_item_id": before,
            "after_item_id": after,
        }
        payload = {
            "edge_id": edge_id,
            "parent_item_id": parent_key,
            "state_id": state_id,
            "priority_id": priority_id,
            "before_item_id": before,
            "after_item_id": after,
            "provenance": provenance,
        }
        source_hash = _hash_json_payload(payload)
        conn.execute(
            """
            INSERT INTO kanban_item_order_edges (
                edge_id, parent_item_id, state_id, priority_id, before_item_id,
                after_item_id, source_hash, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(edge_id) DO UPDATE SET
                before_item_id=excluded.before_item_id,
                after_item_id=excluded.after_item_id,
                source_hash=excluded.source_hash,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                edge_id,
                parent_key,
                state_id,
                priority_id,
                before,
                after,
                source_hash,
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM kanban_item_order_edges WHERE edge_id=?", (edge_id,)
        ).fetchone()
        sync_changes.append(("UPDATE", edge_id, dict(row)))
    return sync_changes


def _ensure_work_item_lane_order(
    conn: Any,
    item_id: str,
    *,
    prefer_top_if_new: bool,
    now: str,
    meta: dict[str, str],
) -> tuple[list[str], list[tuple[str, str, dict[str, Any]]]]:
    item = _work_item_or_404(conn, item_id)
    rows = _work_order_group_rows(
        conn, item["parent_item_id"], item["state_id"], item["priority_id"]
    )
    ordered_ids = [row["item_id"] for row in _order_work_priority_group(conn, rows)]
    if (
        item_id in ordered_ids
        and prefer_top_if_new
        and not _work_item_has_order_relation(
            conn,
            item["parent_item_id"],
            item["state_id"],
            item["priority_id"],
            item_id,
        )
    ):
        ordered_ids = [item_id, *[value for value in ordered_ids if value != item_id]]
    sync_changes = _replace_kanban_item_order_edges(
        conn,
        parent_item_id=item["parent_item_id"],
        state_id=item["state_id"],
        priority_id=item["priority_id"],
        ordered_ids=ordered_ids,
        now=now,
        meta=meta,
    )
    return ordered_ids, sync_changes


def _work_board_payload(
    conn: Any,
    parent_item_id: str | None = None,
    *,
    show_test_entries: bool | None = None,
) -> dict[str, Any]:
    parent_id = _clean_short_text(parent_item_id, "", limit=180) or None
    parent = _work_item_or_404(conn, parent_id) if parent_id else None
    preferences = _kanban_preferences(conn)
    if show_test_entries is None:
        show_test_entries = bool(preferences["show_test_entries"])
    else:
        preferences["show_test_entries"] = bool(show_test_entries)
    breadcrumbs = _work_breadcrumbs(conn, parent_id)
    remaining_depth = (
        max(0, KANBAN_DEPTH_LIMIT - int(parent["depth"]))
        if parent is not None
        else KANBAN_DEPTH_LIMIT
    )
    states = conn.execute(
        "SELECT * FROM kanban_item_states ORDER BY sort_order, state_id"
    ).fetchall()
    if parent_id:
        rows = conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND status != 'archived'
            ORDER BY state_id, priority_id, sort_order, updated_at DESC, item_id
            """,
            (parent_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id IS NULL AND status != 'archived'
            ORDER BY state_id, priority_id, sort_order, updated_at DESC, item_id
            """
        ).fetchall()
    rows, hidden_test_items = _filter_work_test_rows(rows, bool(show_test_entries))
    raw_items_by_state: dict[str, list[Any]] = {row["state_id"]: [] for row in states}
    for row in rows:
        raw_items_by_state.setdefault(row["state_id"], []).append(row)
    items_by_state = {
        state_id: [_row_to_work_item(row) for row in _sort_kanban_items_for_lane(conn, state_rows)]
        for state_id, state_rows in raw_items_by_state.items()
    }
    return {
        "ok": True,
        "board": {
            "parent": _row_to_work_item(parent) if parent else None,
            "depth_limit": KANBAN_DEPTH_LIMIT,
            "remaining_depth": remaining_depth,
            "breadcrumbs": breadcrumbs,
            "columns": [
                {"state": _row_to_work_state(state), "items": items_by_state[state["state_id"]]}
                for state in states
            ],
            "rollup": _work_rollup(
                conn,
                parent_id,
                show_test_entries=bool(show_test_entries),
            ),
            "preferences": preferences,
            "hidden_test_items": hidden_test_items,
            "test_entries": {
                "show": bool(show_test_entries),
                "hidden": hidden_test_items,
            },
        },
    }


def _work_item_payload(
    conn: Any,
    body: WorkItemCreateRequest,
    *,
    promoted_from_ref: str = "",
) -> dict[str, Any]:
    title = _clean_short_text(body.title, "", limit=180)
    if not title:
        raise HTTPException(400, "Kanban item title is required")
    state = _require_work_state(conn, body.state_id)
    priority = _require_work_priority(conn, body.priority_id)
    parent_id = _clean_short_text(body.parent_item_id, "", limit=180) or None
    depth = _work_parent_depth(conn, parent_id)
    item_id = _clean_work_id(body.item_id, "kanban")
    tags = _work_item_tags_for_request(body.tags, _work_request_meta(body))
    item_type = _clean_work_item_type(body.item_type, "item")
    goal_flag = bool(body.goal_flag)
    body_excerpt = _body_excerpt(body.body or "", limit=4000)
    related_events = _clean_event_list(body.related_event_ids, limit=32)
    related_tasks = _clean_event_list(body.related_task_ids, limit=32)
    related_issues = _clean_event_list(body.related_issue_ids, limit=32)
    related_refs = [
        *[f"personal_events:{event_id}" for event_id in related_events],
        *[f"personal_time_tasks:{task_id}" for task_id in related_tasks],
        *[f"kanban_items:{issue_id}" for issue_id in related_issues],
    ]
    search_text, search_metadata, vector_key = _work_search_payload(
        table_name="kanban_items",
        row_id=item_id,
        kind=item_type,
        title=title,
        body=body_excerpt,
        tags=tags,
        related_refs=related_refs,
    )
    provenance = {
        "kanban": {
            "depth_limit": KANBAN_DEPTH_LIMIT,
            "promoted_from_ref": promoted_from_ref,
        },
        **_work_request_meta(body),
    }
    payload = {
        "item_id": item_id,
        "parent_item_id": parent_id,
        "title": title,
        "body_excerpt": body_excerpt,
        "item_type": item_type,
        "state_id": state["state_id"],
        "priority_id": priority["priority_id"],
        "depth": depth,
        "sort_order": int(body.sort_order),
        "status": _work_status_for_state(state),
        "goal_flag": goal_flag,
        "promoted_from_ref": promoted_from_ref,
        "source_type": "manual-kanban",
        "source_ref": f"kanban_items:{item_id}",
        "tags": tags,
        "related_event_ids": related_events,
        "related_task_ids": related_tasks,
        "related_issue_ids": related_issues,
        "search_text": search_text,
        "search_metadata": search_metadata,
        "vector_index_key": vector_key,
        "provenance": provenance,
    }
    payload["source_hash"] = _hash_json_payload(payload)
    return payload


def _insert_work_item(
    conn: Any,
    payload: dict[str, Any],
    *,
    action: str,
    audit_id: str,
    now: str,
) -> tuple[Any, dict[str, Any]]:
    conn.execute(
        """
        INSERT INTO kanban_items (
            item_id, parent_item_id, title, body_excerpt, item_type, state_id,
            priority_id, depth, sort_order, status, goal_flag, archived_at, promoted_from_ref,
            source_type, source_ref, source_hash, tags_json, related_event_ids_json,
            related_task_ids_json, related_issue_ids_json, search_text,
            search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
            vector_index_key, provenance_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '',
                NULL, ?, ?, ?, ?)
        """,
        (
            payload["item_id"],
            payload["parent_item_id"],
            payload["title"],
            payload["body_excerpt"],
            payload["item_type"],
            payload["state_id"],
            payload["priority_id"],
            payload["depth"],
            payload["sort_order"],
            payload["status"],
            int(payload["goal_flag"]),
            payload["promoted_from_ref"],
            payload["source_type"],
            payload["source_ref"],
            payload["source_hash"],
            json.dumps(payload["tags"], ensure_ascii=True),
            json.dumps(payload["related_event_ids"], ensure_ascii=True),
            json.dumps(payload["related_task_ids"], ensure_ascii=True),
            json.dumps(payload["related_issue_ids"], ensure_ascii=True),
            payload["search_text"],
            json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
            payload["vector_index_key"],
            json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
            now,
            now,
        ),
    )
    item_row = _work_item_or_404(conn, payload["item_id"])
    audit_row = _write_work_audit(
        conn,
        audit_id=audit_id,
        actor=payload["provenance"]["actor"],
        source_surface=payload["provenance"]["source_surface"],
        action=action,
        target_ref=f"kanban_items:{payload['item_id']}",
        item_id=payload["item_id"],
        parent_item_id=payload["parent_item_id"] or "",
        created_at=now,
        request_id=payload["provenance"]["request_id"],
        run_id=payload["provenance"]["run_id"],
        result="ok",
        source_hash=payload["source_hash"],
        metadata={
            "state_id": payload["state_id"],
            "priority_id": payload["priority_id"],
            "depth": payload["depth"],
            "promoted_from_ref": payload["promoted_from_ref"],
        },
    )
    return item_row, audit_row


def _work_state_for_leaf_status(conn: Any, status: str) -> Any:
    clean_status = _clean_work_leaf_status(status)
    if clean_status == "blocked":
        candidates = ["blocked", "doing", "todo"]
    elif clean_status in {"done", "closed", "promoted", "archived"}:
        candidates = ["done", "todo"]
    elif clean_status in {"active", "in_progress"}:
        candidates = ["doing", "todo"]
    elif clean_status in {"pending_review", "source_unavailable"}:
        candidates = ["review", "doing", "todo"]
    else:
        candidates = ["todo", "backlog"]
    for state_id in candidates:
        row = conn.execute(
            "SELECT * FROM kanban_item_states WHERE state_id=?", (state_id,)
        ).fetchone()
        if row:
            return row
    row = conn.execute(
        "SELECT * FROM kanban_item_states ORDER BY sort_order, state_id LIMIT 1"
    ).fetchone()
    if not row:
        raise HTTPException(400, "Kanban item state is invalid")
    return row


def _work_leaf_item_status(status: str, state: Any) -> str:
    clean_status = _clean_work_leaf_status(status)
    if clean_status == "archived":
        return "archived"
    return _work_status_for_state(state)


def _work_row_is_todo_lane_leaf(conn: Any, row: Any) -> bool:
    if row["state_id"] != "todo" or row["status"] == "archived" or row["item_type"] == "issue":
        return False
    child = conn.execute(
        """
        SELECT 1 FROM kanban_items
        WHERE parent_item_id=? AND status != 'archived'
        LIMIT 1
        """,
        (row["item_id"],),
    ).fetchone()
    return child is None


def _work_typed_leaf_item_row(conn: Any, kind: str, leaf_id: str) -> Any | None:
    clean_kind = _clean_short_text(kind, "", limit=40)
    clean_leaf_id = _clean_short_text(leaf_id, "", limit=180)
    if clean_kind not in {"issue", "todo"} or not clean_leaf_id:
        return None
    row = conn.execute("SELECT * FROM kanban_items WHERE item_id=?", (clean_leaf_id,)).fetchone()
    if not row:
        return None
    if row["item_type"] == clean_kind:
        return row
    provenance = _json_value(row["provenance_json"], {})
    kanban_meta = provenance.get("kanban") if isinstance(provenance.get("kanban"), dict) else {}
    if isinstance(provenance.get(clean_kind), dict):
        return row
    if kanban_meta.get("leaf_kind") == clean_kind:
        return row
    if clean_kind == "todo" and (
        _work_row_has_filter_tag(row, "todo") or _work_row_is_todo_lane_leaf(conn, row)
    ):
        return row
    return None


def _work_leaf_tags(parent_row: Any, kind: str, meta: dict[str, str]) -> list[str]:
    parent_tags = {
        str(tag).strip() for tag in _json_value(parent_row["tags_json"], []) if str(tag).strip()
    }
    inherited_tags = (
        [KANBAN_AGENT_WORKING_OUT_TAG] if KANBAN_AGENT_WORKING_OUT_TAG in parent_tags else []
    )
    return _work_item_tags_for_request([kind, *inherited_tags], meta)


def _upsert_typed_work_leaf_item(
    conn: Any,
    *,
    kind: str,
    leaf_id: str,
    parent_row: Any,
    title: str,
    body_excerpt: str,
    leaf_status: str,
    priority_id: str,
    external_source_ref: str = "",
    related_task_id: str = "",
    due_at: str | None = None,
    meta: dict[str, str],
    now: str,
) -> tuple[Any, list[tuple[str, str, dict[str, Any]]]]:
    clean_kind = _clean_short_text(kind, "", limit=40)
    if clean_kind not in {"issue", "todo"}:
        raise HTTPException(400, "typed Kanban leaf kind is invalid")
    clean_leaf_id = _clean_work_id(leaf_id, clean_kind)
    source_ref = f"kanban_items:{clean_leaf_id}"
    parent_id = parent_row["item_id"]
    existing = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id=?", (clean_leaf_id,)
    ).fetchone()
    previous_parent_id = existing["parent_item_id"] if existing else None
    previous_state_id = existing["state_id"] if existing else None
    previous_priority_id = existing["priority_id"] if existing else None
    depth = _work_parent_depth(conn, parent_id, moving_item_id=clean_leaf_id if existing else "")
    state = _work_state_for_leaf_status(conn, leaf_status)
    item_status = _work_leaf_item_status(leaf_status, state)
    archived_at = now if item_status == "archived" else None
    item_type = "issue" if clean_kind == "issue" else "item"
    if (
        existing
        and existing["promoted_from_ref"] == source_ref
        and existing["item_type"] != "issue"
    ):
        item_type = existing["item_type"]
    tags = _work_leaf_tags(parent_row, clean_kind, meta)
    related_tasks = [related_task_id] if related_task_id else []
    related_issues = [clean_leaf_id] if clean_kind == "issue" else []
    related_refs = [
        f"kanban_items:{parent_id}",
        source_ref,
        *([external_source_ref] if external_source_ref else []),
        *([f"personal_time_tasks:{related_task_id}"] if related_task_id else []),
    ]
    search_text, search_metadata, vector_key = _work_search_payload(
        table_name="kanban_items",
        row_id=clean_leaf_id,
        kind=item_type,
        title=title,
        body=body_excerpt,
        tags=tags,
        related_refs=related_refs,
    )
    provenance = _json_value(existing["provenance_json"], {}) if existing else {}
    if not isinstance(provenance, dict):
        provenance = {}
    provenance["kanban"] = {
        **(provenance.get("kanban") if isinstance(provenance.get("kanban"), dict) else {}),
        "typed_leaf_card": clean_kind == "issue",
        "leaf_kind": clean_kind,
        "parent_item_id": parent_id,
    }
    provenance[clean_kind] = {
        **(provenance.get(clean_kind) if isinstance(provenance.get(clean_kind), dict) else {}),
        "item_id": parent_id,
        **(
            {"typed_item_id": clean_leaf_id}
            if clean_kind == "issue"
            else {"kanban_item_id": clean_leaf_id}
        ),
        "external_source_ref": external_source_ref,
        "due_at": due_at or "",
    }
    if clean_kind == "todo" and isinstance(provenance.get("todo"), dict):
        provenance["todo"].pop("typed_item_id", None)
    provenance["last_update"] = meta
    source_hash = _hash_json_payload(
        {
            "item_id": clean_leaf_id,
            "parent_item_id": parent_id,
            "title": title,
            "body": body_excerpt,
            "item_type": item_type,
            "state_id": state["state_id"],
            "priority_id": priority_id,
            "status": item_status,
            "source_ref": source_ref,
            "related_task_id": related_task_id,
            "related_issue_ids": related_issues,
        }
    )
    conn.execute(
        """
        INSERT INTO kanban_items (
            item_id, parent_item_id, title, body_excerpt, item_type, state_id,
            priority_id, depth, sort_order, status, archived_at, promoted_from_ref,
            source_type, source_ref, source_hash, tags_json, related_event_ids_json,
            related_task_ids_json, related_issue_ids_json, search_text,
            search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
            vector_index_key, provenance_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, '', ?, ?, ?, ?, '[]', ?, ?, ?, ?,
                '', '', NULL, ?, ?, ?, ?)
        ON CONFLICT(item_id) DO UPDATE SET
            parent_item_id=excluded.parent_item_id,
            title=excluded.title,
            body_excerpt=excluded.body_excerpt,
            item_type=excluded.item_type,
            state_id=excluded.state_id,
            priority_id=excluded.priority_id,
            depth=excluded.depth,
            status=excluded.status,
            archived_at=excluded.archived_at,
            source_type=excluded.source_type,
            source_ref=excluded.source_ref,
            source_hash=excluded.source_hash,
            tags_json=excluded.tags_json,
            related_event_ids_json=excluded.related_event_ids_json,
            related_task_ids_json=excluded.related_task_ids_json,
            related_issue_ids_json=excluded.related_issue_ids_json,
            search_text=excluded.search_text,
            search_metadata_json=excluded.search_metadata_json,
            vector_index_key=excluded.vector_index_key,
            provenance_json=excluded.provenance_json,
            updated_at=excluded.updated_at
        """,
        (
            clean_leaf_id,
            parent_id,
            title,
            body_excerpt,
            item_type,
            state["state_id"],
            priority_id,
            depth,
            item_status,
            archived_at,
            f"kanban-{clean_kind}",
            source_ref,
            source_hash,
            json.dumps(tags, ensure_ascii=True),
            json.dumps(related_tasks, ensure_ascii=True),
            json.dumps(related_issues, ensure_ascii=True),
            search_text,
            json.dumps(search_metadata, ensure_ascii=True, sort_keys=True),
            vector_key,
            json.dumps(provenance, ensure_ascii=True, sort_keys=True),
            now,
            now,
        ),
    )
    if existing:
        _recompute_work_child_depths(conn, clean_leaf_id, depth)
    item_row = _work_item_or_404(conn, clean_leaf_id)
    _order_ids, order_sync_changes = _ensure_work_item_lane_order(
        conn,
        clean_leaf_id,
        prefer_top_if_new=(
            not existing
            or previous_parent_id != item_row["parent_item_id"]
            or previous_state_id != item_row["state_id"]
            or previous_priority_id != item_row["priority_id"]
        ),
        now=now,
        meta=meta,
    )
    return item_row, order_sync_changes


@router.get("/kanban/config")
async def get_work_config() -> dict[str, Any]:
    with get_conn() as conn:
        states = conn.execute(
            "SELECT * FROM kanban_item_states ORDER BY sort_order, state_id"
        ).fetchall()
        priorities = conn.execute(
            "SELECT * FROM kanban_item_priorities ORDER BY sort_order, priority_id"
        ).fetchall()
        preferences = _kanban_preferences(conn)
    return {
        "ok": True,
        "depth_limit": KANBAN_DEPTH_LIMIT,
        "states": [_row_to_work_state(row) for row in states],
        "priorities": [_row_to_work_priority(row) for row in priorities],
        "preferences": preferences,
    }


@router.get("/kanban/preferences")
async def get_kanban_preferences() -> dict[str, Any]:
    with get_conn() as conn:
        return {"ok": True, "preferences": _kanban_preferences(conn)}


@router.put("/kanban/preferences")
async def update_kanban_preferences(body: WorkPreferencesUpdateRequest) -> dict[str, Any]:
    meta = _work_request_meta(body)
    with get_conn() as conn:
        preferences = _kanban_preferences(conn)
        if body.show_test_entries is not None:
            preferences["show_test_entries"] = bool(body.show_test_entries)
            set_setting(
                conn,
                KANBAN_SHOW_TEST_ENTRIES_SETTING,
                "true" if preferences["show_test_entries"] else "false",
                "Kanban board proof/test entry visibility",
            )
            gen = increment_gen(conn, "kanban-preferences")
            row = conn.execute(
                "SELECT * FROM settings WHERE key=?",
                (KANBAN_SHOW_TEST_ENTRIES_SETTING,),
            ).fetchone()
            if row is not None:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "settings",
                    KANBAN_SHOW_TEST_ENTRIES_SETTING,
                    dict(row),
                    gen,
                )
        audit = {
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
        }
        return {"ok": True, "preferences": preferences, "audit": audit}


@router.get("/kanban/board")
async def get_work_root_board() -> dict[str, Any]:
    with get_conn() as conn:
        return _work_board_payload(conn)


@router.get("/kanban/items/{item_id}/board")
async def get_work_child_board(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_board_payload(conn, item_id)


@router.get("/kanban/items/{item_id}")
async def get_work_item_detail(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        item = _work_item_or_404(conn, item_id)
        children = conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND status != 'archived'
            ORDER BY state_id, sort_order, updated_at DESC, item_id
            """,
            (item_id,),
        ).fetchall()
        issues = conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND item_type='issue' AND status != 'archived'
            ORDER BY updated_at DESC, item_id
            """,
            (item_id,),
        ).fetchall()
        todos = conn.execute(
            """
            SELECT w.* FROM kanban_items w
            WHERE w.parent_item_id=?
              AND w.state_id='todo'
              AND w.status != 'archived'
              AND w.item_type != 'issue'
              AND NOT EXISTS (
                  SELECT 1 FROM kanban_items child
                  WHERE child.parent_item_id=w.item_id
                    AND child.status != 'archived'
              )
            ORDER BY COALESCE(json_extract(w.provenance_json, '$.todo.due_at'), w.updated_at), w.item_id
            """,
            (item_id,),
        ).fetchall()
        blockers = conn.execute(
            "SELECT * FROM kanban_blockers WHERE item_id=? ORDER BY updated_at DESC, blocker_id",
            (item_id,),
        ).fetchall()
        discussions = conn.execute(
            "SELECT * FROM kanban_discussions WHERE item_id=? ORDER BY created_at ASC, discussion_id",
            (item_id,),
        ).fetchall()
        links = conn.execute(
            """
            SELECT * FROM kanban_item_links
            WHERE source_item_id=? OR target_item_id=?
            ORDER BY link_type, updated_at DESC, link_id
            """,
            (item_id, item_id),
        ).fetchall()
        commits = conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=?
            ORDER BY COALESCE(NULLIF(committed_at, ''), updated_at) DESC,
                     repo_full_name, sha
            """,
            (item_id,),
        ).fetchall()
        audit = conn.execute(
            """
            SELECT * FROM kanban_audit_log
            WHERE item_id=?
            ORDER BY created_at DESC
            LIMIT 20
            """,
            (item_id,),
        ).fetchall()
        return {
            "ok": True,
            "item": _row_to_work_item(item),
            "detail_document": _work_item_detail_document(conn, item_id),
            "breadcrumbs": _work_breadcrumbs(conn, item_id),
            "depth_limit": KANBAN_DEPTH_LIMIT,
            "remaining_depth": max(0, KANBAN_DEPTH_LIMIT - int(item["depth"])),
            "children": [_row_to_work_item(row) for row in children],
            "issues": [_row_to_work_issue(row) for row in issues],
            "todos": [_row_to_work_todo(row) for row in todos],
            "blockers": [_row_to_work_blocker(row) for row in blockers],
            "discussions": [_row_to_work_discussion(row, conn) for row in discussions],
            "links": [
                {
                    "link_id": row["link_id"],
                    "source_item_id": row["source_item_id"],
                    "target_item_id": row["target_item_id"],
                    "link_type": row["link_type"],
                    "metadata": _json_value(row["metadata_json"], {}),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
                for row in links
            ],
            "commits": [_row_to_work_commit(row) for row in commits],
            "audit": [
                {
                    "audit_id": row["audit_id"],
                    "action": row["action"],
                    "actor": row["actor"],
                    "source_surface": row["source_surface"],
                    "created_at": row["created_at"],
                    "metadata": _json_value(row["metadata_json"], {}),
                }
                for row in audit
            ],
            "rollup": _work_rollup(conn, item_id),
            "counts": {
                "children": len(children),
                "issues": len(issues),
                "todos": len(todos),
                "blockers": len(blockers),
                "links": len(links),
                "commits": len(commits),
                "audit": len(audit),
                "discussions": len(discussions),
            },
        }


@router.post("/kanban/items")
async def create_work_item(body: WorkItemCreateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    with get_conn() as conn:
        payload = _work_item_payload(conn, body)
        item_row, audit_row = _insert_work_item(
            conn, payload, action="create_work_item", audit_id=audit_id, now=now
        )
        gen = increment_gen(conn, "kanban-item")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_items", payload["item_id"], dict(item_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    image_associations = _associate_rich_doc_images_for_document(
        domain="kanban",
        markdown=payload["body_excerpt"],
        document_type="item-body",
        document_id=payload["item_id"],
        item_id=payload["item_id"],
        actor=body.actor,
        source_surface=body.source_surface,
    )
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "create_work_item", "result": "ok"},
    }


@router.patch("/kanban/items/{item_id}")
async def update_work_item(item_id: str, body: WorkItemUpdateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        state = _require_work_state(conn, body.state_id or existing["state_id"])
        priority = _require_work_priority(conn, body.priority_id or existing["priority_id"])
        tags = (
            _clean_event_list(body.tags, limit=32)
            if body.tags is not None
            else _json_value(existing["tags_json"], [])
        )
        tags = _work_item_tags_for_request(tags, meta)
        related_events = (
            _clean_event_list(body.related_event_ids, limit=32)
            if body.related_event_ids is not None
            else _json_value(existing["related_event_ids_json"], [])
        )
        related_tasks = (
            _clean_event_list(body.related_task_ids, limit=32)
            if body.related_task_ids is not None
            else _json_value(existing["related_task_ids_json"], [])
        )
        related_issues = (
            _clean_event_list(body.related_issue_ids, limit=32)
            if body.related_issue_ids is not None
            else _json_value(existing["related_issue_ids_json"], [])
        )
        title = _clean_short_text(body.title, existing["title"], limit=180)
        if not title:
            raise HTTPException(400, "Kanban item title is required")
        body_excerpt = (
            _body_excerpt(body.body, limit=4000)
            if body.body is not None
            else existing["body_excerpt"]
        )
        item_type = _clean_work_item_type(body.item_type, existing["item_type"])
        existing_goal_flag = (
            bool(existing["goal_flag"]) if "goal_flag" in existing.keys() else False
        )
        goal_flag = existing_goal_flag if body.goal_flag is None else bool(body.goal_flag)
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_items",
            row_id=item_id,
            kind=item_type,
            title=title,
            body=body_excerpt,
            tags=tags,
            related_refs=[
                *[f"personal_events:{event_id}" for event_id in related_events],
                *[f"personal_time_tasks:{task_id}" for task_id in related_tasks],
                *[f"kanban_items:{issue_id}" for issue_id in related_issues],
            ],
        )
        provenance = _json_value(existing["provenance_json"], {})
        provenance["last_update"] = meta
        payload = {
            "item_id": item_id,
            "title": title,
            "body_excerpt": body_excerpt,
            "item_type": item_type,
            "state_id": state["state_id"],
            "priority_id": priority["priority_id"],
            "sort_order": int(
                body.sort_order if body.sort_order is not None else existing["sort_order"]
            ),
            "status": _work_status_for_state(state),
            "goal_flag": goal_flag,
            "tags": tags,
            "related_event_ids": related_events,
            "related_task_ids": related_tasks,
            "related_issue_ids": related_issues,
            "search_text": search_text,
            "search_metadata": search_metadata,
            "vector_index_key": vector_key,
            "provenance": provenance,
        }
        payload["source_hash"] = _hash_json_payload(payload)
        conn.execute(
            """
            UPDATE kanban_items
            SET title=?, body_excerpt=?, item_type=?, state_id=?, priority_id=?,
                sort_order=?, status=?, goal_flag=?, source_hash=?, tags_json=?,
                related_event_ids_json=?, related_task_ids_json=?,
                related_issue_ids_json=?, search_text=?, search_metadata_json=?,
                vector_index_key=?, provenance_json=?, updated_at=?
            WHERE item_id=?
            """,
            (
                payload["title"],
                payload["body_excerpt"],
                payload["item_type"],
                payload["state_id"],
                payload["priority_id"],
                payload["sort_order"],
                payload["status"],
                int(payload["goal_flag"]),
                payload["source_hash"],
                json.dumps(payload["tags"], ensure_ascii=True),
                json.dumps(payload["related_event_ids"], ensure_ascii=True),
                json.dumps(payload["related_task_ids"], ensure_ascii=True),
                json.dumps(payload["related_issue_ids"], ensure_ascii=True),
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                now,
                item_id,
            ),
        )
        item_row = _work_item_or_404(conn, item_id)
        project_document = None
        if not existing["parent_item_id"] and title != existing["title"]:
            project_document = _sync_kanban_root_project_title(
                conn,
                root_item_id=item_id,
                old_title=existing["title"],
                new_title=title,
                now=now,
            )
        order_ids, order_sync_changes = _ensure_work_item_lane_order(
            conn,
            item_id,
            prefer_top_if_new=(
                item_row["state_id"] != existing["state_id"]
                or item_row["priority_id"] != existing["priority_id"]
            ),
            now=now,
            meta=meta,
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="update_work_item",
            target_ref=f"kanban_items:{item_id}",
            item_id=item_id,
            parent_item_id=item_row["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=payload["source_hash"],
            metadata={
                "state_id": item_row["state_id"],
                "priority_id": item_row["priority_id"],
                "goal_flag": bool(item_row["goal_flag"]),
                "kanban_project_document": project_document,
                "lane_order": order_ids,
            },
        )
        gen = increment_gen(conn, "kanban-item")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_items", item_id, dict(item_row), gen)
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    image_associations = (
        _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=payload["body_excerpt"],
            document_type="item-body",
            document_id=item_id,
            item_id=item_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
        )
        if body.body is not None
        else {"ok": True, "domain": "kanban", "document": {}, "images": [], "count": 0}
    )
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "update_work_item", "result": "ok"},
    }


@router.put("/kanban/items/{item_id}/detail")
async def update_work_item_detail_document(
    item_id: str, body: WorkItemDetailDocumentUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_body = _normalise_markdown_document_body(body.body)
    source_hash = _hash_json_payload({"item_id": item_id, "body": clean_body})
    with get_conn() as conn:
        item = _work_item_or_404(conn, item_id)
        document = _write_work_item_detail_document(
            conn,
            item_id,
            clean_body,
            actor=meta["actor"],
            now=now,
        )
        image_associations = _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=clean_body,
            document_type="item-detail",
            document_id=item_id,
            item_id=item_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="update_work_item_detail",
            target_ref=f"kanban_items:{item_id}:detail",
            item_id=item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "file_ref": document["file_ref"],
                "body_bytes": len(clean_body.encode("utf-8")),
                "rich_doc_image_count": len(image_associations.get("images", [])),
            },
        )
        gen = increment_gen(conn, "kanban-item-detail")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item_id": item_id,
        "detail_document": document,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "update_work_item_detail", "result": "ok"},
    }


@router.post("/kanban/items/{item_id}/discussions")
async def create_work_discussion(item_id: str, body: WorkDiscussionCreateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    clean_discussion_id = _clean_work_id(body.discussion_id, "discussion")
    clean_body = _normalise_markdown_document_body(body.body)
    body_excerpt = _body_excerpt(clean_body, limit=4000)
    author = _clean_short_text(body.author or meta["actor"], meta["actor"], limit=120)
    status = _clean_work_leaf_status(body.status, default="open")
    with get_conn() as conn:
        item = _work_item_or_404(conn, clean_item_id)
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_discussions",
            row_id=clean_discussion_id,
            kind="discussion",
            title=author,
            body=body_excerpt,
            related_refs=[f"kanban_items:{clean_item_id}"],
        )
        provenance = {
            "discussion": {"item_id": clean_item_id},
            **meta,
        }
        row = {
            "discussion_id": clean_discussion_id,
            "item_id": clean_item_id,
            "author": author,
            "body_excerpt": body_excerpt,
            "status": status,
            "search_text": search_text,
            "search_metadata_json": json.dumps(search_metadata, ensure_ascii=True, sort_keys=True),
            "embedding_ref": "",
            "embedding_model": "",
            "embedding_updated_at": None,
            "vector_index_key": vector_key,
            "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
            "created_at": now,
            "updated_at": now,
        }
        source_hash = _hash_json_payload({**row, "body": clean_body})
        conn.execute(
            """
            INSERT INTO kanban_discussions (
                discussion_id, item_id, author, body_excerpt, status, search_text,
                search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
                vector_index_key, provenance_json, created_at, updated_at
            )
            VALUES (
                :discussion_id, :item_id, :author, :body_excerpt, :status, :search_text,
                :search_metadata_json, :embedding_ref, :embedding_model, :embedding_updated_at,
                :vector_index_key, :provenance_json, :created_at, :updated_at
            )
            """,
            row,
        )
        discussion_row = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        document = _write_work_discussion_document(
            conn,
            discussion_row,
            clean_body,
            actor=meta["actor"],
            now=now,
        )
        image_associations = _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=clean_body,
            document_type="discussion",
            document_id=clean_discussion_id,
            item_id=clean_item_id,
            discussion_id=clean_discussion_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
        )
        provenance["document"] = {"file_ref": document["file_ref"]}
        conn.execute(
            "UPDATE kanban_discussions SET provenance_json=? WHERE discussion_id=?",
            (json.dumps(provenance, ensure_ascii=True, sort_keys=True), clean_discussion_id),
        )
        discussion_row = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="create_work_discussion",
            target_ref=f"kanban_discussions:{clean_discussion_id}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "status": status,
                "file_ref": document["file_ref"],
                "rich_doc_image_count": len(image_associations.get("images", [])),
            },
        )
        gen = increment_gen(conn, "kanban-discussion")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_discussions", clean_discussion_id, dict(discussion_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        discussion = _row_to_work_discussion(discussion_row, conn)
    return {
        "ok": True,
        "discussion": discussion,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "create_work_discussion", "result": "ok"},
    }


@router.patch("/kanban/discussions/{discussion_id}")
async def update_work_discussion(
    discussion_id: str, body: WorkDiscussionUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_discussion_id = _clean_short_text(discussion_id, "", limit=180)
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, "Kanban discussion not found")
        item = _work_item_or_404(conn, existing["item_id"])
        existing_document = _work_discussion_document(conn, existing)
        clean_body = (
            _normalise_markdown_document_body(body.body)
            if body.body is not None
            else existing_document["body"]
        )
        author = (
            _clean_short_text(body.author, existing["author"], limit=120)
            if body.author is not None
            else existing["author"]
        )
        status = (
            _clean_work_leaf_status(body.status, default=existing["status"])
            if body.status is not None
            else existing["status"]
        )
        body_excerpt = _body_excerpt(clean_body, limit=4000)
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_discussions",
            row_id=clean_discussion_id,
            kind="discussion",
            title=author,
            body=body_excerpt,
            related_refs=[f"kanban_items:{existing['item_id']}"],
        )
        provenance = _json_value(existing["provenance_json"], {})
        provenance["last_update"] = meta
        source_hash = _hash_json_payload(
            {
                "discussion_id": clean_discussion_id,
                "item_id": existing["item_id"],
                "author": author,
                "status": status,
                "body": clean_body,
            }
        )
        conn.execute(
            """
            UPDATE kanban_discussions
            SET author=?, body_excerpt=?, status=?, search_text=?, search_metadata_json=?,
                vector_index_key=?, provenance_json=?, updated_at=?
            WHERE discussion_id=?
            """,
            (
                author,
                body_excerpt,
                status,
                search_text,
                json.dumps(search_metadata, ensure_ascii=True, sort_keys=True),
                vector_key,
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                now,
                clean_discussion_id,
            ),
        )
        discussion_row = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        document = _write_work_discussion_document(
            conn,
            discussion_row,
            clean_body,
            actor=meta["actor"],
            now=now,
        )
        image_associations = _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=clean_body,
            document_type="discussion",
            document_id=clean_discussion_id,
            item_id=existing["item_id"],
            discussion_id=clean_discussion_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
        )
        provenance["document"] = {"file_ref": document["file_ref"]}
        conn.execute(
            "UPDATE kanban_discussions SET provenance_json=? WHERE discussion_id=?",
            (json.dumps(provenance, ensure_ascii=True, sort_keys=True), clean_discussion_id),
        )
        discussion_row = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="update_work_discussion",
            target_ref=f"kanban_discussions:{clean_discussion_id}",
            item_id=existing["item_id"],
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "status": status,
                "file_ref": document["file_ref"],
                "rich_doc_image_count": len(image_associations.get("images", [])),
            },
        )
        gen = increment_gen(conn, "kanban-discussion")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_discussions", clean_discussion_id, dict(discussion_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        discussion = _row_to_work_discussion(discussion_row, conn)
    return {
        "ok": True,
        "discussion": discussion,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "update_work_discussion", "result": "ok"},
    }


@router.delete("/kanban/discussions/{discussion_id}")
async def delete_work_discussion(
    discussion_id: str, body: WorkItemActionRequest | None = None
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body or WorkItemActionRequest())
    clean_discussion_id = _clean_short_text(discussion_id, "", limit=180)
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, "Kanban discussion not found")
        item = _work_item_or_404(conn, existing["item_id"])
        document = _work_discussion_document(conn, existing)
        source_hash = _hash_json_payload({"discussion_id": clean_discussion_id, "deleted": True})
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="delete_work_discussion",
            target_ref=f"kanban_discussions:{clean_discussion_id}",
            item_id=existing["item_id"],
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "file_ref": document["file_ref"],
            },
        )
        conn.execute("DELETE FROM kanban_discussions WHERE discussion_id=?", (clean_discussion_id,))
        with suppress(OSError):
            _kanban_discussion_path(conn, existing["item_id"], clean_discussion_id).unlink()
        gen = increment_gen(conn, "kanban-discussion")
        enqueue_for_all_peers(conn, "DELETE", "kanban_discussions", clean_discussion_id, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        deleted = _row_to_work_discussion(existing, conn)
    return {
        "ok": True,
        "discussion_id": clean_discussion_id,
        "deleted_discussion": deleted,
        "audit": {"audit_id": audit_id, "action": "delete_work_discussion", "result": "ok"},
    }


@router.post("/kanban/items/{item_id}/move")
async def move_work_item(item_id: str, body: WorkItemMoveRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        state = _require_work_state(conn, body.state_id or existing["state_id"])
        new_parent = _clean_short_text(body.parent_item_id, "", limit=180) or None
        new_depth = _work_parent_depth(conn, new_parent, moving_item_id=item_id)
        max_relative = _work_subtree_max_relative_depth(conn, item_id)
        if new_depth + max_relative > KANBAN_DEPTH_LIMIT:
            raise HTTPException(400, "Kanban item depth limit exceeded")
        conn.execute(
            """
            UPDATE kanban_items
            SET parent_item_id=?, state_id=?, status=?, depth=?, sort_order=?, updated_at=?
            WHERE item_id=?
            """,
            (
                new_parent,
                state["state_id"],
                _work_status_for_state(state),
                new_depth,
                int(body.sort_order if body.sort_order is not None else existing["sort_order"]),
                now,
                item_id,
            ),
        )
        _recompute_work_child_depths(conn, item_id, new_depth)
        moved_rows = conn.execute(
            """
            WITH RECURSIVE descendants(item_id) AS (
                SELECT item_id FROM kanban_items WHERE item_id=?
                UNION ALL
                SELECT w.item_id
                FROM kanban_items w
                JOIN descendants ON w.parent_item_id = descendants.item_id
            )
            SELECT w.* FROM kanban_items w JOIN descendants ON descendants.item_id = w.item_id
            """,
            (item_id,),
        ).fetchall()
        item_row = next(row for row in moved_rows if row["item_id"] == item_id)
        order_ids, order_sync_changes = _ensure_work_item_lane_order(
            conn,
            item_id,
            prefer_top_if_new=(
                item_row["parent_item_id"] != existing["parent_item_id"]
                or item_row["state_id"] != existing["state_id"]
                or item_row["priority_id"] != existing["priority_id"]
            ),
            now=now,
            meta=meta,
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="move_work_item",
            target_ref=f"kanban_items:{item_id}",
            item_id=item_id,
            parent_item_id=new_parent or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=item_row["source_hash"],
            metadata={
                "from_parent_item_id": existing["parent_item_id"],
                "to_parent_item_id": new_parent,
                "state_id": state["state_id"],
                "depth": new_depth,
                "lane_order": order_ids,
            },
        )
        gen = increment_gen(conn, "kanban-item")
        for row in moved_rows:
            enqueue_for_all_peers(conn, "UPDATE", "kanban_items", row["item_id"], dict(row), gen)
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    if os.getenv("PYTEST_CURRENT_TEST"):
        browser_refresh = {
            "ok": True,
            "published_count": 0,
            "skipped_count": 0,
            "detail": "skipped during pytest",
        }
    else:
        try:
            from .routes_voice_mode import publish_kanban_external_refresh_commands

            browser_refresh = await publish_kanban_external_refresh_commands(
                item_id=item_id,
                parent_item_id=item_row["parent_item_id"] or "",
                state_id=item_row["state_id"],
                actor=meta["actor"],
                source_surface=meta["source_surface"],
            )
        except Exception as exc:
            browser_refresh = {
                "ok": False,
                "detail": str(exc)[:240],
            }
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "audit": {"audit_id": audit_id, "action": "move_work_item", "result": "ok"},
        "browser_refresh": browser_refresh,
    }


@router.post("/kanban/items/{item_id}/order")
async def order_work_item(item_id: str, body: WorkItemOrderRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    direction = _clean_short_text(body.direction, "", limit=20).lower()
    if direction not in {"up", "down"}:
        raise HTTPException(400, "Kanban item order direction must be up or down")
    with get_conn() as conn:
        item = _work_item_or_404(conn, item_id)
        rows = _work_order_group_rows(
            conn,
            item["parent_item_id"],
            item["state_id"],
            item["priority_id"],
        )
        ordered_rows = _order_work_priority_group(conn, rows)
        ordered_ids = [row["item_id"] for row in ordered_rows]
        if item_id not in ordered_ids:
            raise HTTPException(404, "Kanban item not found in lane order")
        index = ordered_ids.index(item_id)
        target_index = index - 1 if direction == "up" else index + 1
        changed = 0 <= target_index < len(ordered_ids)
        if changed:
            ordered_ids[index], ordered_ids[target_index] = (
                ordered_ids[target_index],
                ordered_ids[index],
            )
        order_sync_changes = _replace_kanban_item_order_edges(
            conn,
            parent_item_id=item["parent_item_id"],
            state_id=item["state_id"],
            priority_id=item["priority_id"],
            ordered_ids=ordered_ids,
            now=now,
            meta=meta,
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="order_work_item",
            target_ref=f"kanban_items:{item_id}",
            item_id=item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=item["source_hash"],
            metadata={
                "direction": direction,
                "changed": changed,
                "state_id": item["state_id"],
                "priority_id": item["priority_id"],
                "lane_order": ordered_ids,
            },
        )
        gen = increment_gen(conn, "kanban-item-order")
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        item_row = _work_item_or_404(conn, item_id)
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "direction": direction,
        "changed": changed,
        "lane_order": ordered_ids,
        "audit": {"audit_id": audit_id, "action": "order_work_item", "result": "ok"},
    }


@router.post("/kanban/items/{item_id}/archive")
async def archive_work_item(item_id: str, body: WorkItemActionRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        conn.execute(
            "UPDATE kanban_items SET status='archived', archived_at=?, updated_at=? WHERE item_id=?",
            (now, now, item_id),
        )
        item_row = _work_item_or_404(conn, item_id)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="archive_work_item",
            target_ref=f"kanban_items:{item_id}",
            item_id=item_id,
            parent_item_id=existing["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=item_row["source_hash"],
            metadata={"archived_at": now},
        )
        gen = increment_gen(conn, "kanban-item")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_items", item_id, dict(item_row), gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "audit": {"audit_id": audit_id, "action": "archive_work_item", "result": "ok"},
    }


@router.get("/kanban/items/{item_id}/rollup")
async def get_work_item_rollup(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        return {"ok": True, "item_id": item_id, "rollup": _work_rollup(conn, item_id)}


@router.get("/kanban/items/{item_id}/issues")
async def list_work_item_issues(
    item_id: str,
    scope: str = "local",
    view: str = "flat",
) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_scoped_leaf_payload(
            conn, item_id=item_id, kind="issues", scope=scope, view=view
        )


@router.get("/kanban/items/{item_id}/todos")
async def list_work_item_todos(
    item_id: str,
    scope: str = "local",
    view: str = "flat",
) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_scoped_leaf_payload(
            conn, item_id=item_id, kind="todos", scope=scope, view=view
        )


@router.get("/kanban/issues/{issue_id}")
async def get_work_issue(issue_id: str) -> dict[str, Any]:
    clean_issue_id = _clean_short_text(issue_id, "", limit=180)
    with get_conn() as conn:
        row = _work_typed_leaf_item_row(conn, "issue", clean_issue_id)
        if not row:
            raise HTTPException(404, "kanban issue not found")
        item = _work_item_or_404(conn, row["parent_item_id"]) if row["parent_item_id"] else None
        return {
            "ok": True,
            "issue": _row_to_work_issue(row),
            "item": _row_to_work_item(item) if item else None,
            "item_card": _row_to_work_item(row),
            "breadcrumbs": _work_breadcrumbs(conn, item["item_id"] if item else row["item_id"]),
            "rich_document": {
                "domain": "kanban",
                "document_type": "issue",
                "document_id": row["item_id"],
                "body": row["body_excerpt"] or "",
                "item_id": row["parent_item_id"] or "",
                "updated_at": row["updated_at"],
            },
        }


@router.get("/kanban/todos/{todo_id}")
async def get_work_todo(todo_id: str) -> dict[str, Any]:
    clean_todo_id = _clean_short_text(todo_id, "", limit=180)
    with get_conn() as conn:
        row = _work_typed_leaf_item_row(conn, "todo", clean_todo_id)
        if not row:
            raise HTTPException(404, "kanban todo not found")
        item = _work_item_or_404(conn, row["parent_item_id"]) if row["parent_item_id"] else None
        return {
            "ok": True,
            "todo": _row_to_work_todo(row),
            "item": _row_to_work_item(item) if item else None,
            "item_card": _row_to_work_item(row),
            "breadcrumbs": _work_breadcrumbs(conn, item["item_id"] if item else row["item_id"]),
            "rich_document": {
                "domain": "kanban",
                "document_type": "todo",
                "document_id": row["item_id"],
                "body": row["body_excerpt"] or "",
                "item_id": row["parent_item_id"] or "",
                "updated_at": row["updated_at"],
            },
        }


def _clean_kanban_link_type(value: str | None) -> str:
    link_type = _clean_short_text(value, "related", limit=60)
    if link_type not in {
        "related",
        "depends_on",
        "blocks",
        "duplicates",
        "references",
        "split_from",
    }:
        raise HTTPException(400, "Kanban item link type is invalid")
    return link_type


@router.post("/kanban/items/{item_id}/links")
async def create_work_item_link(item_id: str, body: WorkItemLinkCreateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    source_item_id = _clean_short_text(item_id, "", limit=180)
    target_item_id = _clean_short_text(body.target_item_id, "", limit=180)
    if not source_item_id or not target_item_id:
        raise HTTPException(400, "source and target Kanban item ids are required")
    if source_item_id == target_item_id:
        raise HTTPException(400, "Kanban item link target must be a different item")
    link_type = _clean_kanban_link_type(body.link_type)
    metadata = body.metadata if isinstance(body.metadata, dict) else {}
    link_id = _clean_work_id(
        metadata.get("link_id") if isinstance(metadata.get("link_id"), str) else None,
        "kanban-link",
    )
    row = {
        "link_id": link_id,
        "source_item_id": source_item_id,
        "target_item_id": target_item_id,
        "link_type": link_type,
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
        "created_at": now,
        "updated_at": now,
    }
    source_hash = _hash_json_payload(row)
    with get_conn() as conn:
        source = _work_item_or_404(conn, source_item_id)
        _work_item_or_404(conn, target_item_id)
        conn.execute(
            """
            INSERT INTO kanban_item_links (
                link_id, source_item_id, target_item_id, link_type, metadata_json,
                created_at, updated_at
            )
            VALUES (
                :link_id, :source_item_id, :target_item_id, :link_type,
                :metadata_json, :created_at, :updated_at
            )
            """,
            row,
        )
        link_row = conn.execute(
            "SELECT * FROM kanban_item_links WHERE link_id=?", (link_id,)
        ).fetchone()
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="create_work_item_link",
            target_ref=f"kanban_item_links:{link_id}",
            item_id=source_item_id,
            parent_item_id=source["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "target_item_id": target_item_id,
                "link_type": link_type,
            },
        )
        gen = increment_gen(conn, "kanban-item-link")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_item_links", link_id, dict(link_row), gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "link": {
            "link_id": link_row["link_id"],
            "source_item_id": link_row["source_item_id"],
            "target_item_id": link_row["target_item_id"],
            "link_type": link_row["link_type"],
            "metadata": _json_value(link_row["metadata_json"], {}),
            "created_at": link_row["created_at"],
            "updated_at": link_row["updated_at"],
        },
        "audit": {"audit_id": audit_id, "action": "create_work_item_link", "result": "ok"},
    }


@router.get("/kanban/items/{item_id}/commits")
async def list_work_item_commits(item_id: str) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    with get_conn() as conn:
        item = _work_item_or_404(conn, clean_item_id)
        rows = conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=?
            ORDER BY COALESCE(NULLIF(committed_at, ''), updated_at) DESC,
                     repo_full_name, sha
            """,
            (clean_item_id,),
        ).fetchall()
        return {
            "ok": True,
            "item": _row_to_work_item(item),
            "count": len(rows),
            "commits": [_row_to_work_commit(row) for row in rows],
        }


@router.post("/kanban/items/{item_id}/commits")
async def record_work_item_commit(
    item_id: str, body: WorkItemCommitCreateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    repo_full_name = _clean_git_repo_full_name(body.repo_full_name)
    sha = _clean_git_sha(body.sha)
    metadata = dict(body.metadata) if isinstance(body.metadata, dict) else {}
    link_id = _kanban_commit_link_id(clean_item_id, repo_full_name, sha)
    with get_conn() as conn:
        item = _work_item_or_404(conn, clean_item_id)
        git_row = None
        with suppress(sqlite3.OperationalError):
            git_row = conn.execute(
                """
                SELECT * FROM personal_git_commits
                WHERE repo_full_name=? AND sha=?
                """,
                (repo_full_name, sha),
            ).fetchone()
        short_sha = _clean_short_text(
            body.short_sha or (git_row["short_sha"] if git_row else "") or sha[:7],
            sha[:7],
            limit=16,
        )
        html_url = _clean_short_text(
            body.html_url or (git_row["html_url"] if git_row else "") or "",
            "",
            limit=500,
        )
        if not html_url:
            html_url = _github_commit_url(repo_full_name, sha)
        row = {
            "commit_link_id": link_id,
            "item_id": clean_item_id,
            "repo_full_name": repo_full_name,
            "sha": sha,
            "short_sha": short_sha,
            "html_url": html_url,
            "author_login": _clean_short_text(
                body.author_login or (git_row["author_login"] if git_row else ""),
                "",
                limit=120,
            ),
            "author_name": _clean_short_text(
                body.author_name or (git_row["author_name"] if git_row else ""),
                "",
                limit=160,
            ),
            "committed_at": _clean_short_text(
                body.committed_at or (git_row["committed_at"] if git_row else ""),
                "",
                limit=80,
            ),
            "message_subject": _clean_short_text(
                body.message_subject or (git_row["message_subject"] if git_row else ""),
                "",
                limit=240,
            ),
            "message_body": _body_excerpt(
                body.message_body
                if body.message_body is not None
                else (git_row["message_body"] if git_row else ""),
                limit=4000,
            ),
            "branch": _clean_short_text(body.branch or "", "", limit=160),
            "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
            "provenance_json": json.dumps(
                {
                    "commit_ref": _git_commit_ref(repo_full_name, sha),
                    "recorded_by": meta["actor"],
                    "source_surface": meta["source_surface"],
                    "request_id": meta["request_id"],
                    "run_id": meta["run_id"],
                    "personal_git_commit_id": git_row["commit_id"] if git_row else "",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "created_at": now,
            "updated_at": now,
        }
        source_hash = _hash_json_payload(row)
        existing = conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=? AND repo_full_name=? AND sha=?
            """,
            (clean_item_id, repo_full_name, sha),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO kanban_item_commits (
                commit_link_id, item_id, repo_full_name, sha, short_sha, html_url,
                author_login, author_name, committed_at, message_subject, message_body,
                branch, metadata_json, provenance_json, created_at, updated_at
            )
            VALUES (
                :commit_link_id, :item_id, :repo_full_name, :sha, :short_sha,
                :html_url, :author_login, :author_name, :committed_at,
                :message_subject, :message_body, :branch, :metadata_json,
                :provenance_json, :created_at, :updated_at
            )
            ON CONFLICT(item_id, repo_full_name, sha) DO UPDATE SET
                short_sha=excluded.short_sha,
                html_url=excluded.html_url,
                author_login=excluded.author_login,
                author_name=excluded.author_name,
                committed_at=excluded.committed_at,
                message_subject=excluded.message_subject,
                message_body=excluded.message_body,
                branch=excluded.branch,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        commit_row = conn.execute(
            "SELECT * FROM kanban_item_commits WHERE commit_link_id=?",
            (existing["commit_link_id"] if existing else link_id,),
        ).fetchone()
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="record_work_commit",
            target_ref=f"kanban_item_commits:{commit_row['commit_link_id']}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "repo_full_name": repo_full_name,
                "sha": sha,
                "commit_ref": _git_commit_ref(repo_full_name, sha),
                "html_url": html_url,
                "upsert": "updated" if existing else "inserted",
            },
        )
        gen = increment_gen(conn, "kanban-item-commit")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_item_commits",
            commit_row["commit_link_id"],
            dict(commit_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "commit": _row_to_work_commit(commit_row),
        "audit": {"audit_id": audit_id, "action": "record_work_commit", "result": "ok"},
    }


def _clean_work_leaf_status(value: str | None, *, default: str = "open") -> str:
    status = _clean_short_text(value, default, limit=40)
    if status not in {
        "open",
        "active",
        "blocked",
        "pending_review",
        "done",
        "closed",
        "archived",
        "promoted",
        "resolved",
    }:
        raise HTTPException(400, "Kanban leaf status is invalid")
    return status


def _upsert_work_issue(
    body: WorkIssueUpsertRequest,
    *,
    issue_id: str | None = None,
    action: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_issue_id = _clean_work_id(issue_id or body.issue_id, "issue")
    with get_conn() as conn:
        parent_item = _work_item_or_404(conn, body.item_id)
        severity_id = body.severity_id or body.priority_id
        priority = _require_work_priority(conn, severity_id)
        title = _clean_short_text(body.title, "", limit=180)
        if not title:
            raise HTTPException(400, "Kanban issue title is required")
        body_excerpt = _body_excerpt(body.body or "", limit=4000)
        issue_status = _clean_work_leaf_status(body.status)
        source_ref = _clean_short_text(body.source_ref, "", limit=220)
        related_task_id = _clean_short_text(body.related_task_id, "", limit=180)
        typed_item_row, order_sync_changes = _upsert_typed_work_leaf_item(
            conn,
            kind="issue",
            leaf_id=clean_issue_id,
            parent_row=parent_item,
            title=title,
            body_excerpt=body_excerpt,
            leaf_status=issue_status,
            priority_id=priority["priority_id"],
            external_source_ref=source_ref,
            related_task_id=related_task_id,
            meta=meta,
            now=now,
        )
        issue_row = _work_item_or_404(conn, clean_issue_id)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action=action,
            target_ref=f"kanban_items:{clean_issue_id}",
            item_id=body.item_id,
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=issue_row["source_hash"],
            metadata={
                "status": issue_row["status"],
                "priority_id": issue_row["priority_id"],
                "severity_id": issue_row["priority_id"],
                "item_card_ref": f"kanban_items:{typed_item_row['item_id']}",
            },
        )
        gen = increment_gen(conn, "kanban-issue")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_items", typed_item_row["item_id"], dict(typed_item_row), gen
        )
        for order_action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(
                conn, order_action, "kanban_item_order_edges", row_id, row_data, gen
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    issue = _row_to_work_issue(issue_row)
    image_associations = _associate_rich_doc_images_for_document(
        domain="kanban",
        markdown=issue["body_excerpt"],
        document_type="issue",
        document_id=issue["issue_id"],
        item_id=issue["item_id"],
        actor=meta["actor"],
        source_surface=meta["source_surface"],
    )
    return {
        "ok": True,
        "issue": issue,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": action, "result": "ok"},
    }


@router.post("/kanban/issues")
async def create_work_issue(body: WorkIssueUpsertRequest) -> dict[str, Any]:
    return _upsert_work_issue(body, action="create_work_issue")


@router.patch("/kanban/issues/{issue_id}")
async def update_work_issue(issue_id: str, body: WorkIssueUpsertRequest) -> dict[str, Any]:
    return _upsert_work_issue(body, issue_id=issue_id, action="update_work_issue")


def _upsert_work_todo(
    body: WorkTodoUpsertRequest,
    *,
    todo_id: str | None = None,
    action: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_todo_id = _clean_work_id(todo_id or body.todo_id, "todo")
    with get_conn() as conn:
        parent_item = _work_item_or_404(conn, body.item_id)
        priority = _require_work_priority(conn, body.priority_id)
        title = _clean_short_text(body.title, "", limit=180)
        if not title:
            raise HTTPException(400, "Kanban todo title is required")
        body_excerpt = _body_excerpt(body.body or "", limit=4000)
        todo_status = _clean_work_leaf_status(body.status)
        due_at = _clean_short_text(body.due_at, "", limit=80) or None
        related_task_id = _clean_short_text(body.related_task_id, "", limit=180)
        typed_item_row, order_sync_changes = _upsert_typed_work_leaf_item(
            conn,
            kind="todo",
            leaf_id=clean_todo_id,
            parent_row=parent_item,
            title=title,
            body_excerpt=body_excerpt,
            leaf_status=todo_status,
            priority_id=priority["priority_id"],
            related_task_id=related_task_id,
            due_at=due_at,
            meta=meta,
            now=now,
        )
        todo_row = _work_item_or_404(conn, clean_todo_id)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action=action,
            target_ref=f"kanban_items:{clean_todo_id}",
            item_id=body.item_id,
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=todo_row["source_hash"],
            metadata={
                "status": todo_row["status"],
                "priority_id": todo_row["priority_id"],
                "item_card_ref": f"kanban_items:{typed_item_row['item_id']}",
            },
        )
        gen = increment_gen(conn, "kanban-todo")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_items", typed_item_row["item_id"], dict(typed_item_row), gen
        )
        for order_action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(
                conn, order_action, "kanban_item_order_edges", row_id, row_data, gen
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    todo = _row_to_work_todo(todo_row)
    image_associations = _associate_rich_doc_images_for_document(
        domain="kanban",
        markdown=todo["body_excerpt"],
        document_type="todo",
        document_id=todo["todo_id"],
        item_id=todo["item_id"],
        actor=meta["actor"],
        source_surface=meta["source_surface"],
    )
    return {
        "ok": True,
        "todo": todo,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": action, "result": "ok"},
    }


@router.post("/kanban/todos")
async def create_work_todo(body: WorkTodoUpsertRequest) -> dict[str, Any]:
    return _upsert_work_todo(body, action="create_work_todo")


@router.patch("/kanban/todos/{todo_id}")
async def update_work_todo(todo_id: str, body: WorkTodoUpsertRequest) -> dict[str, Any]:
    return _upsert_work_todo(body, todo_id=todo_id, action="update_work_todo")


def _upsert_work_blocker(
    body: WorkBlockerUpsertRequest,
    *,
    blocker_id: str | None = None,
    action: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_blocker_id = _clean_work_id(blocker_id or body.blocker_id, "blocker")
    with get_conn() as conn:
        item = _work_item_or_404(conn, body.item_id)
        title = _clean_short_text(body.title, "", limit=180)
        if not title:
            raise HTTPException(400, "Kanban blocker title is required")
        body_excerpt = _body_excerpt(body.body or "", limit=4000)
        blocked_by_ref = _clean_short_text(body.blocked_by_ref, "", limit=220)
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_blockers",
            row_id=clean_blocker_id,
            kind="blocker",
            title=title,
            body=body_excerpt,
            related_refs=[blocked_by_ref] if blocked_by_ref else [],
        )
        provenance = {"blocker": {"item_id": body.item_id}, **meta}
        source_hash = _hash_json_payload(
            {
                "blocker_id": clean_blocker_id,
                "item_id": body.item_id,
                "title": title,
                "body": body_excerpt,
                "status": body.status,
                "blocked_by_ref": blocked_by_ref,
            }
        )
        previous = conn.execute(
            "SELECT created_at FROM kanban_blockers WHERE blocker_id=?", (clean_blocker_id,)
        ).fetchone()
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        conn.execute(
            """
            INSERT INTO kanban_blockers (
                blocker_id, item_id, title, body_excerpt, status, blocked_by_ref,
                search_text, search_metadata_json, embedding_ref, embedding_model,
                embedding_updated_at, vector_index_key, provenance_json, created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, ?)
            ON CONFLICT(blocker_id) DO UPDATE SET
                item_id=excluded.item_id,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                status=excluded.status,
                blocked_by_ref=excluded.blocked_by_ref,
                search_text=excluded.search_text,
                search_metadata_json=excluded.search_metadata_json,
                vector_index_key=excluded.vector_index_key,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                clean_blocker_id,
                body.item_id,
                title,
                body_excerpt,
                _clean_work_leaf_status(body.status),
                blocked_by_ref,
                search_text,
                json.dumps(search_metadata, ensure_ascii=True, sort_keys=True),
                vector_key,
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                created_at,
                now,
            ),
        )
        blocker_row = conn.execute(
            "SELECT * FROM kanban_blockers WHERE blocker_id=?", (clean_blocker_id,)
        ).fetchone()
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action=action,
            target_ref=f"kanban_blockers:{clean_blocker_id}",
            item_id=body.item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "status": blocker_row["status"],
                "blocked_by_ref": blocker_row["blocked_by_ref"],
            },
        )
        gen = increment_gen(conn, "kanban-blocker")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_blockers", clean_blocker_id, dict(blocker_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "blocker": _row_to_work_blocker(blocker_row),
        "audit": {"audit_id": audit_id, "action": action, "result": "ok"},
    }


@router.post("/kanban/blockers")
async def create_work_blocker(body: WorkBlockerUpsertRequest) -> dict[str, Any]:
    return _upsert_work_blocker(body, action="create_work_blocker")


@router.patch("/kanban/blockers/{blocker_id}")
async def update_work_blocker(blocker_id: str, body: WorkBlockerUpsertRequest) -> dict[str, Any]:
    return _upsert_work_blocker(body, blocker_id=blocker_id, action="update_work_blocker")


def _promotion_source_payload(conn: Any, source_ref: str) -> dict[str, Any]:
    if source_ref.startswith("personal_time_tasks:"):
        task_id = source_ref.split(":", 1)[1]
        row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "promotion source task not found")
        return {
            "title": row["title"],
            "body": row["body_excerpt"],
            "related_task_ids": [task_id],
            "tags": [KANBAN_TAG, "task"],
        }
    if source_ref.startswith("kanban_items:"):
        item_id = source_ref.split(":", 1)[1]
        row = conn.execute("SELECT * FROM kanban_items WHERE item_id=?", (item_id,)).fetchone()
        if not row:
            raise HTTPException(404, "promotion source item not found")
        provenance = _json_value(row["provenance_json"], {})
        kanban_meta = provenance.get("kanban") if isinstance(provenance.get("kanban"), dict) else {}
        kind = "issue" if row["item_type"] == "issue" else ""
        if not kind and _work_typed_leaf_item_row(conn, "todo", item_id):
            kind = "todo"
        if not kind:
            kind = _clean_short_text(kanban_meta.get("leaf_kind"), "", limit=40)
        if kind not in {"issue", "todo"}:
            raise HTTPException(400, "promotion source item is not an issue card or ToDo-view item")
        return {
            "title": row["title"],
            "body": row["body_excerpt"],
            "related_task_ids": _json_value(row["related_task_ids_json"], []),
            "related_issue_ids": [item_id] if kind == "issue" else [],
            "tags": [KANBAN_TAG, kind],
            "typed_leaf": {
                "kind": kind,
                "id": item_id,
                "parent_item_id": row["parent_item_id"] or "",
            },
        }
    raise HTTPException(400, "promotion source ref is invalid")


def _promote_existing_typed_leaf_item(
    conn: Any,
    *,
    source_ref: str,
    source: dict[str, Any],
    body: WorkPromoteRequest,
    audit_id: str,
    now: str,
) -> tuple[Any, dict[str, Any], list[tuple[str, str, dict[str, Any]]]] | None:
    typed_leaf = source.get("typed_leaf")
    if not isinstance(typed_leaf, dict):
        return None
    kind = _clean_short_text(typed_leaf.get("kind"), "", limit=40)
    leaf_id = _clean_short_text(typed_leaf.get("id"), "", limit=180)
    if kind not in {"issue", "todo"} or not leaf_id:
        return None
    existing = _work_typed_leaf_item_row(conn, kind, leaf_id)
    if not existing:
        return None
    meta = _work_request_meta(body)
    state = _require_work_state(conn, body.state_id)
    priority = _require_work_priority(conn, body.priority_id)
    new_parent = _clean_short_text(body.parent_item_id, "", limit=180) or None
    new_depth = _work_parent_depth(conn, new_parent, moving_item_id=existing["item_id"])
    max_relative = _work_subtree_max_relative_depth(conn, existing["item_id"])
    if new_depth + max_relative > KANBAN_DEPTH_LIMIT:
        raise HTTPException(400, "Kanban item depth limit exceeded")
    existing_tags = _json_value(existing["tags_json"], [])
    tags = _work_item_tags_for_request(
        _clean_event_list([*existing_tags, *source.get("tags", []), *body.tags], limit=32),
        meta,
    )
    related_tasks = _clean_event_list(source.get("related_task_ids", []), limit=32)
    related_issues = _clean_event_list(source.get("related_issue_ids", []), limit=32)
    title = _clean_short_text(body.title or source.get("title", ""), "", limit=180)
    if not title:
        raise HTTPException(400, "Kanban item title is required")
    body_excerpt = _body_excerpt(
        body.body if body.body is not None else source.get("body", ""),
        limit=4000,
    )
    search_text, search_metadata, vector_key = _work_search_payload(
        table_name="kanban_items",
        row_id=existing["item_id"],
        kind="item",
        title=title,
        body=body_excerpt,
        tags=tags,
        related_refs=[
            *[f"personal_time_tasks:{task_id}" for task_id in related_tasks],
            *[f"kanban_items:{issue_id}" for issue_id in related_issues],
            source_ref,
        ],
    )
    provenance = _json_value(existing["provenance_json"], {})
    if not isinstance(provenance, dict):
        provenance = {}
    provenance["kanban"] = {
        **(provenance.get("kanban") if isinstance(provenance.get("kanban"), dict) else {}),
        "typed_leaf_card": False,
        "converted_from_typed_leaf": True,
        "leaf_kind": kind,
        "promoted_from_ref": source_ref,
    }
    provenance["promotion"] = {
        "source_ref": source_ref,
        "source_kind": kind,
        "converted_item_id": existing["item_id"],
    }
    provenance["last_update"] = meta
    payload = {
        "item_id": existing["item_id"],
        "parent_item_id": new_parent,
        "title": title,
        "body_excerpt": body_excerpt,
        "item_type": "item",
        "state_id": state["state_id"],
        "priority_id": priority["priority_id"],
        "depth": new_depth,
        "sort_order": int(existing["sort_order"]),
        "status": _work_status_for_state(state),
        "promoted_from_ref": source_ref,
        "source_type": "manual-kanban",
        "source_ref": f"kanban_items:{existing['item_id']}",
        "tags": tags,
        "related_event_ids": [],
        "related_task_ids": related_tasks,
        "related_issue_ids": related_issues,
        "search_text": search_text,
        "search_metadata": search_metadata,
        "vector_index_key": vector_key,
        "provenance": provenance,
    }
    payload["source_hash"] = _hash_json_payload(payload)
    conn.execute(
        """
        UPDATE kanban_items
        SET parent_item_id=?, title=?, body_excerpt=?, item_type=?, state_id=?,
            priority_id=?, depth=?, sort_order=?, status=?, archived_at=NULL,
            promoted_from_ref=?, source_type=?, source_ref=?, source_hash=?,
            tags_json=?, related_event_ids_json=?, related_task_ids_json=?,
            related_issue_ids_json=?, search_text=?, search_metadata_json=?,
            vector_index_key=?, provenance_json=?, updated_at=?
        WHERE item_id=?
        """,
        (
            payload["parent_item_id"],
            payload["title"],
            payload["body_excerpt"],
            payload["item_type"],
            payload["state_id"],
            payload["priority_id"],
            payload["depth"],
            payload["sort_order"],
            payload["status"],
            payload["promoted_from_ref"],
            payload["source_type"],
            payload["source_ref"],
            payload["source_hash"],
            json.dumps(payload["tags"], ensure_ascii=True),
            json.dumps(payload["related_event_ids"], ensure_ascii=True),
            json.dumps(payload["related_task_ids"], ensure_ascii=True),
            json.dumps(payload["related_issue_ids"], ensure_ascii=True),
            payload["search_text"],
            json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
            payload["vector_index_key"],
            json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
            now,
            existing["item_id"],
        ),
    )
    _recompute_work_child_depths(conn, existing["item_id"], new_depth)
    item_row = _work_item_or_404(conn, existing["item_id"])
    _order_ids, order_sync_changes = _ensure_work_item_lane_order(
        conn,
        existing["item_id"],
        prefer_top_if_new=(
            item_row["parent_item_id"] != existing["parent_item_id"]
            or item_row["state_id"] != existing["state_id"]
            or item_row["priority_id"] != existing["priority_id"]
        ),
        now=now,
        meta=meta,
    )
    audit_row = _write_work_audit(
        conn,
        audit_id=audit_id,
        actor=meta["actor"],
        source_surface=meta["source_surface"],
        action="promote_work_item",
        target_ref=f"kanban_items:{item_row['item_id']}",
        item_id=item_row["item_id"],
        parent_item_id=item_row["parent_item_id"] or "",
        created_at=now,
        request_id=meta["request_id"],
        run_id=meta["run_id"],
        result="ok",
        source_hash=payload["source_hash"],
        metadata={
            "state_id": item_row["state_id"],
            "priority_id": item_row["priority_id"],
            "depth": item_row["depth"],
            "promoted_from_ref": source_ref,
            "converted_typed_leaf": True,
        },
    )
    return item_row, audit_row, order_sync_changes


@router.post("/kanban/promote")
async def promote_work_item(body: WorkPromoteRequest) -> dict[str, Any]:
    source_ref = _clean_short_text(body.source_ref, "", limit=220)
    if not source_ref:
        raise HTTPException(400, "promotion source ref is required")
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    with get_conn() as conn:
        source = _promotion_source_payload(conn, source_ref)
        converted = _promote_existing_typed_leaf_item(
            conn,
            source_ref=source_ref,
            source=source,
            body=body,
            audit_id=audit_id,
            now=now,
        )
        order_sync_changes: list[tuple[str, str, dict[str, Any]]] = []
        if converted is not None:
            item_row, audit_row, order_sync_changes = converted
            sync_item_id = item_row["item_id"]
        else:
            item_body = WorkItemCreateRequest(
                parent_item_id=body.parent_item_id,
                title=body.title or source.get("title", ""),
                body=body.body if body.body is not None else source.get("body", ""),
                state_id=body.state_id,
                priority_id=body.priority_id,
                tags=_clean_event_list([*source.get("tags", []), *body.tags], limit=32),
                related_task_ids=source.get("related_task_ids", []),
                related_issue_ids=source.get("related_issue_ids", []),
                actor=body.actor,
                source_surface=body.source_surface,
                request_id=body.request_id,
                run_id=body.run_id,
            )
            payload = _work_item_payload(conn, item_body, promoted_from_ref=source_ref)
            item_row, audit_row = _insert_work_item(
                conn, payload, action="promote_work_item", audit_id=audit_id, now=now
            )
            sync_item_id = payload["item_id"]
        gen = increment_gen(conn, "kanban-promote")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_items", sync_item_id, dict(item_row), gen)
        for order_action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(
                conn, order_action, "kanban_item_order_edges", row_id, row_data, gen
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "audit": {"audit_id": audit_id, "action": "promote_work_item", "result": "ok"},
    }


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
    privacy_level = _clean_privacy_level(body.privacy_level)
    tags = _clean_event_list(body.tags, limit=24)
    for required in ("calendar", kind):
        if required not in tags:
            tags.append(required)
    if body.all_day and "all-day" not in tags:
        tags.append("all-day")
    if not body.all_day and "timed" not in tags:
        tags.append("timed")
    # Keep this raw for Markdown preview/edit; body_excerpt is the compact row/search summary.
    content = str(body.body or "").strip()[:20000]
    content_excerpt = _body_excerpt(content, limit=2000)
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
        "body_excerpt": content_excerpt,
        "content_projection": content,
        "start_at": start_at,
        "end_at": end_at,
        "local_date": local_date,
        "timezone": timezone_name,
        "status": status,
        "priority": _clean_short_text(body.priority, "", limit=40) or None,
        "privacy_level": privacy_level,
        "tags": tags,
        "related_kanban_items": _clean_event_list(body.related_kanban_items),
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
                privacy_level, tags_json, related_kanban_items_json, related_tasks_json,
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
                related_kanban_items_json=excluded.related_kanban_items_json,
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
                json.dumps(payload["related_kanban_items"], ensure_ascii=True),
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


def _delete_personal_event_row(
    event_id: str,
    body: PersonalEventDeleteRequest,
    *,
    action: str,
    gen_key: str,
    not_found_message: str,
) -> dict[str, Any]:
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "personal-page")
    request_id = _clean_short_text(body.request_id, f"{action}-{uuid.uuid4().hex[:12]}", limit=160)
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    audit_id = f"audit-{uuid.uuid4().hex}"
    now = _utc_now_iso()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, not_found_message)
        event = _row_to_event(row)
        file_refs = _json_value(row["file_refs_json"], [])
        source_hash = row["source_hash"] or hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action=action,
            target_ref=f"personal_events:{event_id}",
            file_ref=str(file_refs[0]) if file_refs else "",
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={
                "event_id": event_id,
                "local_date": row["local_date"],
                "kind": row["kind"],
                "source_type": row["source_type"],
            },
        )
        conn.execute("DELETE FROM personal_events WHERE event_id=?", (event_id,))
        gen = increment_gen(conn, gen_key)
        enqueue_for_all_peers(conn, "DELETE", "personal_events", event_id, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "event_id": event_id,
        "deleted_event": event,
        "audit": {
            "audit_id": audit_id,
            "actor": actor,
            "source_surface": source_surface,
            "request_id": request_id,
            "result": "ok",
            "action": action,
        },
    }


@router.delete("/calendar/events/{event_id}")
async def delete_calendar_event(event_id: str, body: PersonalEventDeleteRequest) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT source_type FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "calendar event not found")
    if row["source_type"] != "manual-calendar":
        raise HTTPException(400, "only manual calendar events can be deleted here")
    return _delete_personal_event_row(
        event_id,
        body,
        action="delete_calendar_event",
        gen_key="personal-calendar-delete",
        not_found_message="calendar event not found",
    )


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
    elif source_filter == "kanban":
        where.append("(source_type = 'kanban' OR json_array_length(related_kanban_items_json) > 0)")
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
            "available": ["all", "manual", "sources", "git", "imports", "kanban"],
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


def _diary_entry_tags(tags: list[str] | None, *, all_day: bool) -> list[str]:
    event_tags = ["diary", "personal-log", "quick-entry"]
    for tag in tags or []:
        clean = str(tag).strip()
        if clean and clean not in event_tags:
            event_tags.append(clean)
    if all_day:
        if "all-day" not in event_tags:
            event_tags.append("all-day")
        event_tags = [tag for tag in event_tags if tag != "timed"]
    else:
        if "timed" not in event_tags:
            event_tags.append("timed")
        event_tags = [tag for tag in event_tags if tag != "all-day"]
    return event_tags


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
    all_day: bool = True,
    local_time: str | None = None,
    end_time: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    local_date = result["local_date"]
    filename = result["filename"]
    event_id = f"diary-{local_date}-{Path(filename).stem}"
    timezone_name = result.get("timezone") or os.environ.get(
        "XARTA_DIARY_TIMEZONE", "Europe/London"
    )
    event_tags = _diary_entry_tags(tags, all_day=all_day)
    provenance = {
        "writer": "xarta_diary.create_personal_log",
        "calendar": {
            "all_day": bool(all_day),
            "local_start_time": local_time or "",
            "local_end_time": end_time or "",
            "timezone": timezone_name,
        },
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
                _calendar_utc_iso(local_date, local_time, timezone_name),
                local_date,
                timezone_name,
                "open",
                "normal",
                json.dumps(event_tags, ensure_ascii=True),
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
    all_day = bool(body.all_day) if body.all_day is not None else not bool(body.local_time)
    local_time = None if all_day else _validate_local_time(body.local_time)
    end_time = None if all_day else _validate_local_time(body.end_time)
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
        all_day=all_day,
        local_time=local_time,
        end_time=end_time,
        tags=tags,
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


@router.patch("/diary-day/entries/{event_id}")
async def update_diary_day_entry(event_id: str, body: DiaryEntryUpdateRequest) -> dict[str, Any]:
    text = str(body.body or "").strip()
    if not text:
        raise HTTPException(400, "entry body is required")
    if len(text) > 20000:
        raise HTTPException(400, "entry body is too long")
    local_date = _validate_local_date(body.local_date)
    range_end_date = _validate_local_date(body.range_end_date or local_date)
    all_day = bool(body.all_day) if body.all_day is not None else not bool(body.local_time)
    local_time = None if all_day else _validate_local_time(body.local_time)
    end_time = None if all_day else _validate_local_time(body.end_time)
    timezone_name = body.timezone or os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London")
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "diary-page")
    request_id = _clean_short_text(
        body.request_id, f"diary-entry-edit-{uuid.uuid4().hex[:12]}", limit=160
    )
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    event_tags = _diary_entry_tags(
        [str(tag).strip() for tag in body.tags if str(tag).strip()],
        all_day=all_day,
    )
    now = _utc_now_iso()
    source_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, "diary entry not found")
        existing_tags = _json_value(row["tags_json"], [])
        if row["kind"] != "personal-log" and "quick-entry" not in existing_tags:
            raise HTTPException(400, "only diary quick entries can be edited here")
        provenance = _json_value(row["provenance_json"], {})
        provenance["calendar"] = {
            "all_day": bool(all_day),
            "local_start_time": local_time or "",
            "local_end_time": end_time or "",
            "local_end_date": range_end_date,
            "timezone": timezone_name,
        }
        provenance["edited_at"] = now
        provenance["edited_by"] = actor
        provenance["source_surface"] = source_surface
        provenance["request_id"] = request_id
        provenance["run_id"] = run_id
        file_refs = _json_value(row["file_refs_json"], [])
        db_refs = _json_value(row["db_refs_json"], [])
        audit_id = f"audit-{uuid.uuid4().hex}"
        audit_ref = f"personal_time_audit:{audit_id}"
        if audit_ref not in db_refs:
            db_refs.append(audit_ref)
        start_at = _calendar_utc_iso(local_date, local_time, timezone_name)
        end_at = (
            _calendar_utc_iso(range_end_date, end_time, timezone_name)
            if end_time and not all_day
            else None
        )
        conn.execute(
            """
            UPDATE personal_events
            SET source_hash=?,
                title=?,
                body_excerpt=?,
                content_projection=?,
                start_at=?,
                end_at=?,
                local_date=?,
                timezone=?,
                tags_json=?,
                db_refs_json=?,
                provenance_json=?,
                projection_state='hot',
                provenance_state='linked',
                last_rendered_at=?,
                updated_at=?
            WHERE event_id=?
            """,
            (
                source_hash,
                _entry_title(text, local_time),
                _body_excerpt(text),
                text,
                start_at,
                end_at,
                local_date,
                timezone_name,
                json.dumps(event_tags, ensure_ascii=True),
                json.dumps(db_refs, ensure_ascii=True),
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                now,
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
            action="update_diary_entry",
            target_ref=f"personal_events:{event_id}",
            file_ref=str(file_refs[0]) if file_refs else "",
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={
                "local_date": local_date,
                "range_end_date": range_end_date,
                "kind": "personal-log",
                "body_chars": len(text),
            },
        )
        gen = increment_gen(conn, "personal-diary-edit")
        enqueue_for_all_peers(conn, "UPDATE", "personal_events", event_id, dict(updated), gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "audit": {
            "audit_id": audit_id,
            "actor": actor,
            "source_surface": source_surface,
            "request_id": request_id,
            "result": "ok",
        },
        "event": _row_to_event(updated),
        "day": _build_diary_day_payload(local_date),
    }


@router.delete("/diary-day/entries/{event_id}")
async def delete_diary_day_entry(event_id: str, body: PersonalEventDeleteRequest) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT kind, tags_json FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "diary entry not found")
    existing_tags = _json_value(row["tags_json"], [])
    if row["kind"] != "personal-log" and "quick-entry" not in existing_tags:
        raise HTTPException(400, "only diary quick entries can be deleted here")
    result = _delete_personal_event_row(
        event_id,
        body,
        action="delete_diary_entry",
        gen_key="personal-diary-delete",
        not_found_message="diary entry not found",
    )
    local_date = result["deleted_event"].get("local_date") or _validate_local_date(None)
    result["day"] = _build_diary_day_payload(local_date)
    return result


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


def _lone_wolf_rel(path: Any) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    raw = Path(text)
    if raw.is_absolute():
        with suppress(ValueError):
            return raw.resolve().relative_to(LONE_WOLF_ROOT.resolve()).as_posix()
    return text


def _first_trace_candidate(payload: dict[str, Any]) -> dict[str, Any] | None:
    categories = payload.get("categories") if isinstance(payload.get("categories"), dict) else {}
    for category_data in categories.values():
        extracted = category_data.get("extracted") if isinstance(category_data, dict) else []
        for item in _as_list(extracted):
            candidates = item.get("parsed_candidates") if isinstance(item, dict) else []
            for candidate in _as_list(candidates):
                if isinstance(candidate, dict) and candidate.get("game_type"):
                    return candidate
    return None


def _trace_completed_at(payload: dict[str, Any]) -> str:
    categories = payload.get("categories") if isinstance(payload.get("categories"), dict) else {}
    values: list[str] = []
    for category_data in categories.values():
        if not isinstance(category_data, dict):
            continue
        for key in ("results", "importer_state", "worker_state", "wiki_pages"):
            for item in _as_list(category_data.get(key)):
                if isinstance(item, dict):
                    value = str(item.get("completed_at") or item.get("updated_at") or "")
                    if value:
                        values.append(value)
    return max(values) if values else str(payload.get("generated_at") or "")


def _trace_artifacts(trace_path: Path, payload: dict[str, Any]) -> dict[str, list[dict[str, str]]]:
    surfaces = (
        payload.get("operator_surfaces")
        if isinstance(payload.get("operator_surfaces"), dict)
        else {}
    )

    def items(key: str, label: str) -> list[dict[str, str]]:
        paths = [str(path) for path in _as_list(surfaces.get(key)) if str(path or "").strip()]
        return [
            {
                "label": f"{label} {index + 1}" if len(paths) > 1 else label,
                "path": _lone_wolf_rel(path),
            }
            for index, path in enumerate(paths)
        ]

    return {
        "trace": [{"label": "Trace proof", "path": _lone_wolf_rel(trace_path)}],
        "raw": items("raw_records", "Raw record"),
        "results": items("visible_results", "Result"),
        "wiki": items("wiki_pages", "Wiki page"),
    }


def _trace_submission_summary(trace_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    raw_records = _as_list(payload.get("raw_records"))
    dict_raw_records = [item for item in raw_records if isinstance(item, dict)]
    first_raw = next(
        (item for item in dict_raw_records if item.get("event_timestamp")), None
    ) or next(iter(dict_raw_records), {})
    selectors = payload.get("selectors") if isinstance(payload.get("selectors"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    categories = [str(item) for item in _as_list(summary.get("categories")) if str(item)]
    category = categories[0] if categories else trace_path.parents[1].name
    candidate = _first_trace_candidate(payload)
    submitted_at = str(first_raw.get("event_timestamp") or first_raw.get("stored_at") or "")
    title = ""
    detected_as = ""
    outcome = ""
    details: list[str] = []
    urls = [str(url) for url in _as_list(selectors.get("urls")) if str(url)]
    event_ids = [
        str(event_id) for event_id in _as_list(selectors.get("event_ids")) if str(event_id)
    ]

    if candidate:
        game_type = str(candidate.get("game_type") or "").strip().lower()
        detected_as = game_type.title()
        if game_type == "wordle":
            target = str(candidate.get("target_word") or "").strip()
            score = str(candidate.get("score") or "").strip()
            title = f"Wordle screenshot: {target or score or 'detected'}"
            outcome = ", ".join(part for part in [score, target] if part) or "Wordle detected"
            attempts = candidate.get("attempts")
            status = str(candidate.get("status") or "").strip()
            if attempts:
                details.append(f"{attempts} attempts")
            if status:
                details.append(status)
        elif game_type == "connections":
            status = str(candidate.get("status") or "").strip()
            mistakes = candidate.get("mistakes")
            groups = [
                str(group.get("category") or "").strip()
                for group in _as_list(candidate.get("groups"))
                if isinstance(group, dict) and group.get("category")
            ]
            title = "Connections screenshot"
            outcome = f"Connections {status or 'detected'}"
            if mistakes is not None:
                outcome += f", {mistakes} mistakes"
            if groups:
                details.append("; ".join(groups[:4]))
        else:
            title = f"{detected_as} game screenshot"
            outcome = str(candidate.get("status") or "Game detected")
        parser = str(candidate.get("parser") or "").strip()
        if parser:
            details.append(parser)
    else:
        url = (
            urls[0]
            if urls
            else str(first_raw.get("urls", [""])[0] if first_raw.get("urls") else "")
        )
        host = urlparse(url).netloc if url else ""
        labels = (
            first_raw.get("routing", {}).get("labels")
            if isinstance(first_raw.get("routing"), dict)
            else []
        )
        label_text = ", ".join(str(label) for label in _as_list(labels)[:4])
        title = f"{category.title()} URL: {host or 'submitted URL'}"
        detected_as = category
        outcome = "URL captured, bookmark written, wiki updated" if url else "Submission processed"
        if label_text:
            details.append(label_text)
        reason = (
            first_raw.get("routing", {}).get("reason")
            if isinstance(first_raw.get("routing"), dict)
            else ""
        )
        if reason:
            details.append(str(reason))

    return {
        "id": trace_path.stem,
        "title": title or trace_path.stem,
        "kind": "game" if candidate else "url" if urls else "submission",
        "status": "processed" if payload.get("ok") is True else "needs_review",
        "category": category,
        "submitted_at": submitted_at,
        "completed_at": _trace_completed_at(payload),
        "detected_as": detected_as,
        "outcome": outcome,
        "details": details,
        "matrix_event_ids": event_ids,
        "url": urls[0] if urls else "",
        "source_room_id": str(first_raw.get("source_room_id") or ""),
        "trace_path": _lone_wolf_rel(trace_path),
        "artifacts": _trace_artifacts(trace_path, payload),
    }


def _parse_ingestion_trace_submissions(limit: int = 12) -> list[dict[str, Any]]:
    root = LONE_WOLF_ROOT / INTERESTS_ROOT_REL
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return rows
    for path in root.glob("*/results/trace-*.json"):
        payload = _read_json_file(path, {})
        if payload.get("schema") != "xarta.interests.ingestion.traceability.v1":
            continue
        rows.append(_trace_submission_summary(path, payload))
    return sorted(
        rows,
        key=lambda item: (str(item.get("submitted_at") or ""), str(item.get("id") or "")),
        reverse=True,
    )[:limit]


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
    for label in ("Completion proof", "Follow-up add-on", "Traceability proof"):
        link = _markdown_list_link(text, label)
        if link:
            if label == "Traceability proof":
                link["label"] = f"{label}: {link['label']}"
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
        "recent_submissions": _parse_ingestion_trace_submissions(),
        "source_unavailable": _markdown_table(text, "Source-Unavailable", limit=12),
        "blockers": blocker_items,
        "rerun_status": "idempotent source digest" if "source digest changes" in text else "",
        "proof_links": proof_links,
    }


def _resolve_import_artifact_path(raw_path: str) -> tuple[Path, str]:
    text = str(raw_path or "").strip().replace("\\", "/").lstrip("/")
    if not text:
        raise HTTPException(400, "artifact path is required")
    clean = posixpath.normpath(text)
    if clean.startswith("../") or clean == ".." or "/../" in clean:
        raise HTTPException(400, "artifact path escapes the workspace")
    if not any(
        clean == prefix.rstrip("/") or clean.startswith(prefix)
        for prefix in IMPORT_ARTIFACT_ALLOWED_PREFIXES
    ):
        raise HTTPException(403, "artifact path is outside the imports allowlist")
    suffix = Path(clean).suffix.lower()
    if suffix not in IMPORT_ARTIFACT_TEXT_SUFFIXES:
        raise HTTPException(415, "artifact preview is limited to text artifacts")
    root = LONE_WOLF_ROOT.resolve()
    resolved = (root / clean).resolve()
    with suppress(ValueError):
        rel = resolved.relative_to(root).as_posix()
        if rel == clean and resolved.is_file():
            return resolved, rel
    raise HTTPException(404, "artifact not found")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


@router.get("/imports-artifact")
async def get_imports_artifact(
    path: str = Query(..., min_length=1, max_length=500),
) -> dict[str, Any]:
    artifact_path, rel = _resolve_import_artifact_path(path)
    stat = artifact_path.stat()
    too_large = stat.st_size > IMPORT_ARTIFACT_PREVIEW_BYTES
    with artifact_path.open("rb") as handle:
        data = handle.read(IMPORT_ARTIFACT_PREVIEW_BYTES + 1)
    preview = data[:IMPORT_ARTIFACT_PREVIEW_BYTES].decode("utf-8", errors="replace")
    return {
        "ok": True,
        "path": rel,
        "name": artifact_path.name,
        "size_bytes": stat.st_size,
        "sha256": _sha256_file(artifact_path),
        "truncated": too_large,
        "preview": preview,
    }


def _safe_audit_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    parsed = urlparse(text)
    if not parsed.scheme or not parsed.netloc:
        return text.split("?", 1)[0].split("#", 1)[0][:240]
    return parsed._replace(query="", fragment="").geturl()[:240]


def _read_openclaw_bookmark_candidates() -> list[dict[str, Any]]:
    path = LONE_WOLF_ROOT / OPENCLAW_BOOKMARK_CANDIDATES_REL
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            with suppress(json.JSONDecodeError):
                payload = json.loads(text)
                if isinstance(payload, dict):
                    rows.append(payload)
    return rows


def _openclaw_domain_for_url(raw_url: str) -> str:
    host = urlparse(str(raw_url or "")).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    for domain in OPENCLAW_AI_DEVELOPMENT_DOMAINS:
        if host == domain or host.endswith(f".{domain}"):
            return domain
    return ""


def _interest_text_file_paths() -> list[tuple[str, Path]]:
    root = LONE_WOLF_ROOT / INTERESTS_ROOT_REL
    if not root.exists():
        return []
    paths: list[tuple[str, Path]] = []
    for category_root in root.iterdir():
        if not category_root.is_dir():
            continue
        category = category_root.name
        for folder in ("raw", "results", "extracted", "entities", "queries"):
            base = category_root / folder
            if not base.exists():
                continue
            for path in base.rglob("*"):
                if path.is_file() and path.suffix.lower() in IMPORT_ARTIFACT_TEXT_SUFFIXES:
                    paths.append((category, path))
    return paths


def _openclaw_ai_domain_audit(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_domain: dict[str, list[dict[str, Any]]] = {
        domain: [] for domain in OPENCLAW_AI_DEVELOPMENT_DOMAINS
    }
    for row in rows:
        domain = _openclaw_domain_for_url(str(row.get("url") or ""))
        if domain:
            by_domain.setdefault(domain, []).append(row)

    unique_urls: dict[str, dict[str, Any]] = {}
    for domain_rows in by_domain.values():
        for row in domain_rows:
            url = _safe_audit_url(str(row.get("url") or ""))
            if url and url not in unique_urls:
                unique_urls[url] = row

    occurrences: dict[str, dict[str, Any]] = {
        url: {"categories": set(), "paths": []} for url in unique_urls
    }
    needles = {url: (url.lower(), url.lower().rstrip("/")) for url in unique_urls}
    domains = tuple(domain.lower() for domain in OPENCLAW_AI_DEVELOPMENT_DOMAINS)
    for category, path in _interest_text_file_paths():
        try:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
        except OSError:
            continue
        if not any(domain in text for domain in domains):
            continue
        rel = _lone_wolf_rel(path)
        for url, variants in needles.items():
            if any(variant and variant in text for variant in variants):
                occurrences[url]["categories"].add(category)
                if len(occurrences[url]["paths"]) < 5:
                    occurrences[url]["paths"].append(rel)

    audits: list[dict[str, Any]] = []
    for domain, domain_rows in by_domain.items():
        unique_domain_urls = []
        seen: set[str] = set()
        for row in domain_rows:
            url = _safe_audit_url(str(row.get("url") or ""))
            if url and url not in seen:
                seen.add(url)
                unique_domain_urls.append((url, row))
        example_rows = []
        counts = {"in_ai_developments": 0, "in_other_category": 0, "missing_from_interests": 0}
        for url, row in unique_domain_urls:
            cats = sorted(occurrences.get(url, {}).get("categories") or [])
            if "ai-developments" in cats:
                state = "in_ai_developments"
            elif cats:
                state = "in_other_category"
            else:
                state = "missing_from_interests"
            counts[state] += 1
            if len(example_rows) < 10 and state != "in_ai_developments":
                example_rows.append(
                    {
                        "url": url,
                        "timestamp": str(row.get("timestamp") or ""),
                        "state": state,
                        "categories": cats,
                        "paths": occurrences.get(url, {}).get("paths") or [],
                    }
                )
        status = (
            "ok"
            if unique_domain_urls
            and counts["in_other_category"] == 0
            and counts["missing_from_interests"] == 0
            else "needs_review"
            if unique_domain_urls
            else "not_seen"
        )
        note = (
            "all unique OpenClaw URLs for this domain are present in ai-developments"
            if status == "ok"
            else "some OpenClaw URLs for this AI-development domain are missing or outside ai-developments"
            if status == "needs_review"
            else "no OpenClaw bookmark candidates found for this domain"
        )
        audits.append(
            {
                "domain": domain,
                "status": status,
                "candidate_count": len(domain_rows),
                "unique_url_count": len(unique_domain_urls),
                **counts,
                "note": note,
                "examples": example_rows,
            }
        )
    return audits


def _category_summary_by_name(interests: dict[str, Any]) -> dict[str, dict[str, str]]:
    rows = (
        interests.get("category_summary")
        if isinstance(interests.get("category_summary"), list)
        else []
    )
    return {
        str(row.get("Category") or "").strip("`"): row
        for row in rows
        if isinstance(row, dict) and row.get("Category")
    }


def _openclaw_candidate_audit(interests: dict[str, Any]) -> dict[str, Any]:
    source_path = LONE_WOLF_ROOT / OPENCLAW_BOOKMARK_CANDIDATES_REL
    rows = _read_openclaw_bookmark_candidates()
    summary = _category_summary_by_name(interests)
    category_rows: list[dict[str, Any]] = []
    for category, label, pattern in OPENCLAW_AUDIT_PATTERNS:
        regex = re.compile(pattern, re.IGNORECASE)
        hits = [
            row
            for row in rows
            if regex.search(f"{row.get('url') or ''} {row.get('context') or ''}")
        ]
        current = summary.get(category, {})
        examples = [
            {
                "timestamp": str(row.get("timestamp") or ""),
                "url": _safe_audit_url(str(row.get("url") or "")),
            }
            for row in hits[:8]
        ]
        raw_count = int(str(current.get("Raw") or "0").strip("`") or 0)
        results_count = int(str(current.get("Results") or "0").strip("`") or 0)
        status = "ok"
        note = "candidate count is within current raw/result scale"
        if hits and raw_count < max(5, len(hits) // 4):
            status = "needs_review"
            note = (
                "candidate count is much larger than current raw count; import coverage needs audit"
            )
        category_rows.append(
            {
                "category": category,
                "label": label,
                "status": status,
                "candidate_count": len(hits),
                "current_raw": raw_count,
                "current_results": results_count,
                "current_wiki_pages": int(str(current.get("Wiki pages") or "0").strip("`") or 0),
                "note": note,
                "examples": examples,
            }
        )
    domain_audit = _openclaw_ai_domain_audit(rows)
    blockers = [row for row in category_rows if row["status"] != "ok"]
    blockers.extend(row for row in domain_audit if row["status"] == "needs_review")
    return {
        "status": "needs_review" if blockers else "ok",
        "source_path": str(OPENCLAW_BOOKMARK_CANDIDATES_REL),
        "source_exists": source_path.exists(),
        "total_candidates": len(rows),
        "categories": category_rows,
        "ai_development_domains": domain_audit,
        "blockers": blockers,
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


def _personal_source_counts_from_conn(conn: Any) -> dict[str, Any]:
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


def _personal_source_counts() -> dict[str, Any]:
    now = _utc_now_iso()
    with get_conn() as conn:
        _sync_personal_import_status_batches(conn, now)
        return _personal_source_counts_from_conn(conn)


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
    openclaw_coverage = _openclaw_candidate_audit(interests)
    git_activity = _git_activity_dashboard(counts)
    blockers = []
    if interests.get("blockers"):
        blockers.append({"source": "interests-ingestion", "items": interests["blockers"]})
    if openclaw_coverage.get("blockers"):
        blockers.append({"source": "openclaw-carry-over", "items": openclaw_coverage["blockers"]})
    if git_activity.get("errors"):
        blockers.append({"source": "git-activity", "items": git_activity["errors"]})

    digest_payload = json.dumps(
        {
            "interests": {
                "source_digest": interests.get("source_digest"),
                "snapshot_at": interests.get("snapshot_at"),
                "pending_review": interests.get("pending_review"),
                "actionable_backlog": interests.get("actionable_backlog"),
                "recent_submissions": interests.get("recent_submissions", []),
            },
            "openclaw": {
                "status": openclaw_coverage.get("status"),
                "total_candidates": openclaw_coverage.get("total_candidates"),
                "categories": [
                    {
                        "category": row.get("category"),
                        "candidate_count": row.get("candidate_count"),
                        "current_raw": row.get("current_raw"),
                        "current_results": row.get("current_results"),
                        "status": row.get("status"),
                    }
                    for row in openclaw_coverage.get("categories", [])
                ],
                "ai_development_domains": [
                    {
                        "domain": row.get("domain"),
                        "unique_url_count": row.get("unique_url_count"),
                        "in_ai_developments": row.get("in_ai_developments"),
                        "in_other_category": row.get("in_other_category"),
                        "missing_from_interests": row.get("missing_from_interests"),
                        "status": row.get("status"),
                    }
                    for row in openclaw_coverage.get("ai_development_domains", [])
                ],
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
        if interests.get("status") == "ok"
        and openclaw_coverage.get("status") == "ok"
        and git_activity.get("status") == "ok"
        else "needs_review"
    )
    return {
        "status": status,
        "generated_at": _utc_now_iso(),
        "source_digest": source_digest,
        "source_counts": counts,
        "interests": interests,
        "openclaw_coverage": openclaw_coverage,
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


def _parse_utc_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    with suppress(ValueError):
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    with suppress(ValueError):
        parsed = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=timezone.utc)
    return None


def _projection_maintenance_candidates(
    conn: Any,
    *,
    now: datetime,
    retention_days: int,
    limit: int,
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(days=retention_days)
    rows = conn.execute(
        """
        SELECT *
        FROM personal_events
        WHERE projection_state='hot'
          AND COALESCE(content_projection, '') != ''
          AND COALESCE(file_refs_json, '[]') != '[]'
        ORDER BY COALESCE(projection_expires_at, last_rendered_at, updated_at, created_at) ASC,
                 event_id ASC
        LIMIT ?
        """,
        (max(limit * 4, limit),),
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        file_refs = _json_value(row["file_refs_json"], [])
        if not isinstance(file_refs, list) or not file_refs:
            continue
        expires_at = _parse_utc_datetime(row["projection_expires_at"])
        last_rendered_at = _parse_utc_datetime(row["last_rendered_at"])
        updated_at = _parse_utc_datetime(row["updated_at"])
        created_at = _parse_utc_datetime(row["created_at"])
        reason = ""
        stale_at: datetime | None = None
        if expires_at and expires_at <= now:
            reason = "projection_expired"
            stale_at = expires_at
        else:
            rendered_or_updated = last_rendered_at or updated_at or created_at
            if rendered_or_updated and rendered_or_updated <= cutoff:
                reason = "retention_elapsed"
                stale_at = rendered_or_updated
        if not reason:
            continue
        content = row["content_projection"] or ""
        candidates.append(
            {
                "event_id": row["event_id"],
                "source_type": row["source_type"],
                "kind": row["kind"],
                "title": row["title"],
                "local_date": row["local_date"],
                "reason": reason,
                "stale_at": stale_at.isoformat().replace("+00:00", "Z") if stale_at else "",
                "projection_bytes": len(content.encode("utf-8")),
                "file_refs": file_refs,
                "db_refs": _json_value(row["db_refs_json"], []),
                "row": row,
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


@router.post("/projections/maintenance")
async def maintain_personal_projections(
    body: PersonalProjectionMaintenanceRequest,
) -> dict[str, Any]:
    if body.retention_days < 0:
        raise HTTPException(400, "retention_days must be non-negative")
    if body.limit < 1 or body.limit > 1000:
        raise HTTPException(400, "limit must be between 1 and 1000")
    now_dt = _parse_utc_datetime(body.now) if body.now else None
    if body.now and now_dt is None:
        raise HTTPException(400, "now is invalid")
    now_dt = now_dt or datetime.now(timezone.utc).replace(microsecond=0)
    now_text = now_dt.isoformat().replace("+00:00", "Z")
    with get_conn() as conn:
        candidates = _projection_maintenance_candidates(
            conn,
            now=now_dt,
            retention_days=body.retention_days,
            limit=body.limit,
        )
        trimmed_rows = []
        if candidates and not body.dry_run:
            gen = increment_gen(conn, "personal-projection-maintenance")
            for item in candidates:
                row = item["row"]
                provenance = _json_value(row["provenance_json"], {})
                if not isinstance(provenance, dict):
                    provenance = {}
                provenance["hot_cache_maintenance"] = {
                    "schema": "xarta.personal.hot_cache_maintenance.v1",
                    "trimmed_at_utc": now_text,
                    "reason": item["reason"],
                    "retention_days": body.retention_days,
                    "preserved_file_refs": item["file_refs"],
                }
                conn.execute(
                    """
                    UPDATE personal_events
                    SET content_projection='',
                        projection_state='needs_rehydrate',
                        projection_expires_at=NULL,
                        provenance_json=?,
                        updated_at=?
                    WHERE event_id=?
                    """,
                    (
                        json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                        now_text,
                        item["event_id"],
                    ),
                )
                updated = conn.execute(
                    "SELECT * FROM personal_events WHERE event_id=?",
                    (item["event_id"],),
                ).fetchone()
                if updated:
                    trimmed_rows.append(updated)
                    enqueue_for_all_peers(
                        conn,
                        "UPDATE",
                        "personal_events",
                        item["event_id"],
                        dict(updated),
                        gen,
                    )
        public_candidates = [
            {key: value for key, value in item.items() if key != "row"} for item in candidates
        ]
    return {
        "ok": True,
        "schema": "xarta.personal.projection_maintenance.v1",
        "generated_at_utc": now_text,
        "dry_run": body.dry_run,
        "retention_days": body.retention_days,
        "candidate_count": len(public_candidates),
        "trimmed_count": len(trimmed_rows),
        "candidates": public_candidates,
    }


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
