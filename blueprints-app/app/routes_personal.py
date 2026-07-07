"""Shared Personal Time Activity projection API."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import logging
import os
import posixpath
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import yaml
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel
from starlette.responses import FileResponse

from . import config as cfg
from . import hermes_minutes, timing
from .db import get_conn as _sqlite_get_conn
from .db import get_setting, increment_gen, set_setting
from .kanban_datastore import (
    ACTIVE_STORE_POSTGRES,
    CANDIDATE_READ_STORE_POSTGRES,
    CANDIDATE_READ_STORE_SHADOW,
    KANBAN_DATASTORE_TABLES,
    KanbanDatastoreConfigError,
    kanban_datastore_bootstrap_plan,
    kanban_datastore_status,
)
from .kanban_parity import (
    KanbanShadowParityError,
    kanban_postgres_parity_report,
    kanban_shadow_candidate_connection,
    kanban_shadow_parity_report,
)
from .kanban_postgres import KanbanPostgresError, postgres_candidate_connection
from .kanban_store import (
    KanbanItemCycleError,
    KanbanItemNotFound,
    KanbanPriorityRecommendationRead,
    KanbanStore,
)
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/personal", tags=["personal"])
log = logging.getLogger(__name__)


async def _run_personal_sync_work(func: Any, *args: Any, **kwargs: Any) -> Any:
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return func(*args, **kwargs)
    label = getattr(func, "__name__", "personal_sync_work")
    return await timing.to_thread(f"personal.{label}", func, *args, **kwargs)


DIARY_ROOT = Path(os.environ.get("BLUEPRINTS_DIARY_DIR", "/xarta-node/.lone-wolf/diary"))
KANBAN_ROOT = Path(os.environ.get("BLUEPRINTS_KANBAN_DIR", "/xarta-node/.lone-wolf/kanban"))
KANBAN_PRIORITY_SCOPE_ID = "kanban"
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

_WORK_AUTOMATION_PROFILE_CONFIG_CACHE: dict[str, tuple[int, int, dict[str, Any]]] = {}
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
KANBAN_ITEM_REVIEW_SCHEMA = "xarta.kanban.item_review.v1"
KANBAN_REVIEW_DECISION_SCHEMA = "xarta.kanban.review_decision.v1"
KANBAN_REVIEW_FEEDBACK_SCHEMA = "xarta.kanban.operator_feedback.v1"
KANBAN_REVIEW_FEEDBACK_COLLECTION_SCHEMA = "xarta.kanban.operator_feedback.collection.v1"
KANBAN_REVIEW_FEEDBACK_ATTRIBUTION_SCHEMA = "xarta.kanban.operator_feedback.attribution.v1"
KANBAN_REVIEW_OUTPUT_CONTRACT_SCHEMA = "xarta.kanban.review_processor.output_contract.v1"
KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA = "xarta.kanban.review_processor.policy.v1"
KANBAN_REVIEW_METADATA_CONTRACT_SCHEMA = "xarta.kanban.review_processor.metadata_contract.v1"
KANBAN_REVIEW_LEASE_SCHEMA = "xarta.kanban.review_processor.lease.v1"
KANBAN_REVIEW_MARKER_SCHEMA = "xarta.kanban.review_processor.marker.v1"
KANBAN_REVIEW_FAILURE_EVENT_SCHEMA = "xarta.kanban.review_processor.failure_event.v1"
KANBAN_REVIEW_SCHEDULER_SCHEMA = "xarta.kanban.review_processor.scheduler.v1"
KANBAN_REVIEW_RETRY_POLICY_VERSION = "xarta.kanban.review_processor.retry_policy.v1"
KANBAN_REVIEW_RETRY_BACKOFF_SECONDS = (
    5 * 60,
    20 * 60,
    60 * 60,
    4 * 60 * 60,
    12 * 60 * 60,
    24 * 60 * 60,
    2 * 24 * 60 * 60,
    4 * 24 * 60 * 60,
    6 * 24 * 60 * 60,
)
KANBAN_PREPROCESSING_READINESS_CONTRACT_SCHEMA = "xarta.kanban.preprocessing.readiness_contract.v1"
KANBAN_PREPROCESSING_QUEUE_SCHEMA = "xarta.kanban.preprocessing.queue.v1"
KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA = "xarta.kanban.proposal_surfaces.contract.v1"
KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA = "xarta.kanban.automation.idle_worker.v1"
KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA = "xarta.kanban.automation.idle_worker.contract.v1"
KANBAN_AUTOMATION_EXCLUSION_SCHEMA = "xarta.kanban.automation.exclusion.v1"
KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV = "BLUEPRINTS_KANBAN_AUTOMATION_LOCAL_AI_MODEL"
KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE = "required-hermes-kanban-llm"
KANBAN_AUTOMATION_PROFILE_ENGINE = "hermes-profile-openai-compatible-json"
KANBAN_PROFILE_FALLBACK_PROVIDER_DEFAULT = "configured-local-litellm"
KANBAN_PROFILE_FALLBACK_MODEL_DEFAULT = "PRIMARY-LOCAL-PRIVATE-NO-PROTECTION"
HERMES_LOCAL_STACK_ROOT = Path(
    os.environ.get("BLUEPRINTS_HERMES_LOCAL_STACK", "/xarta-node/.lone-wolf/stacks/hermes-local")
)
HERMES_PROFILE_DATA_ROOT = Path(
    os.environ.get(
        "BLUEPRINTS_HERMES_PROFILE_DATA_ROOT",
        str(HERMES_LOCAL_STACK_ROOT / "data/profiles"),
    )
)
KANBAN_PROCESSOR_PROFILE_SPECS: dict[str, dict[str, Any]] = {
    "preprocessing": {
        "profile": "hermes-kanban-preprocessor",
        "api_base_env": "BLUEPRINTS_KANBAN_PREPROCESSOR_API_BASE",
        "api_key_file_env": "BLUEPRINTS_KANBAN_PREPROCESSOR_API_KEY_FILE",
        "api_base": "http://127.0.0.1:8649",
        "api_key_file": str(HERMES_PROFILE_DATA_ROOT / "hermes-kanban-preprocessor/.env"),
        "primary_provider": "openai-codex",
        "primary_model": "gpt-5.5",
        "fallback_provider": KANBAN_PROFILE_FALLBACK_PROVIDER_DEFAULT,
        "fallback_model": KANBAN_PROFILE_FALLBACK_MODEL_DEFAULT,
    },
    "review": {
        "profile": "hermes-kanban-review-processor",
        "api_base_env": "BLUEPRINTS_KANBAN_REVIEW_PROCESSOR_API_BASE",
        "api_key_file_env": "BLUEPRINTS_KANBAN_REVIEW_PROCESSOR_API_KEY_FILE",
        "api_base": "http://127.0.0.1:8650",
        "api_key_file": str(HERMES_PROFILE_DATA_ROOT / "hermes-kanban-review-processor/.env"),
        "primary_provider": "openai-codex",
        "primary_model": "gpt-5.5",
        "fallback_provider": KANBAN_PROFILE_FALLBACK_PROVIDER_DEFAULT,
        "fallback_model": KANBAN_PROFILE_FALLBACK_MODEL_DEFAULT,
    },
}
KANBAN_AUTOMATION_OWNER_NODE_ID_ENV = "BLUEPRINTS_KANBAN_AUTOMATION_OWNER_NODE_ID"
KANBAN_AUTOMATION_PRIMARY_FLAG_ENV = "SYSTEM_BRIDGE_NOTIFIER_BLUEPRINTS_PRIMARY"
KANBAN_AUTOMATION_SINGLETON_OVERRIDE_ENV = "BLUEPRINTS_KANBAN_AUTOMATION_SINGLETON_OVERRIDE"
KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH_ENV = (
    "BLUEPRINTS_KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH"
)
KANBAN_AUTOMATION_SINGLETON_OVERRIDE_DEFAULT_PATH = (
    "/etc/xarta-kanban-automation-singleton-override"
)
KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_PATH = (
    Path(__file__).resolve().parent / "contracts" / "kanban_automation_idle_worker.v1.json"
)
KANBAN_AUTOMATION_DEFAULT_MAX_SCAN_ITEMS = 25
KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP = 100
KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA = (
    "xarta.kanban.processor_marker_blocker.provenance.v1"
)
KANBAN_PREPROCESSING_DECOMPOSITION_MOVE_SCHEMA = "xarta.kanban.preprocessing.decomposition_move.v1"
KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA = (
    "xarta.kanban.preprocessing.decomposition_blocker.provenance.v1"
)
KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA = "xarta.kanban.blocked_leaf_invariant.v1"
KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID = "kanban-5f930fec1321"
KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID = "kanban-203acef17b12"
KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID = "kanban-51aaf9f7eb09"
KANBAN_AGENT_PROPOSAL_WORKSTREAM_ITEM_ID = "kanban-agent-proposal-surfaces-20260627"
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
PERSONAL_EVENT_LOCAL_END_DATE_SQL = (
    "COALESCE(NULLIF(json_extract(provenance_json, '$.calendar.local_end_date'), ''), local_date)"
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
PERSONAL_FILTER_COLORS = {
    "red",
    "yellow",
    "pink",
    "green",
    "purple",
    "orange",
    "blue",
    "brown",
    "white",
    "black",
    "grey",
    "gold",
}

_KANBAN_ACTIVE_POSTGRES_TABLES = set(KANBAN_DATASTORE_TABLES)
_KANBAN_ACTIVE_POSTGRES_TABLE_RE = re.compile(
    r"\b("
    + "|".join(re.escape(table) for table in sorted(_KANBAN_ACTIVE_POSTGRES_TABLES))
    + r")\b",
    flags=re.IGNORECASE,
)
_KANBAN_ACTIVE_POSTGRES_SQLITE_ONLY_RE = re.compile(
    r"\b(sync_queue|sync_meta|nodes|personal_|settings_audit)\b",
    flags=re.IGNORECASE,
)
_KANBAN_ACTIVE_POSTGRES_TRANSACTION_RE = re.compile(
    r"^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b",
    flags=re.IGNORECASE,
)
_KANBAN_ACTIVE_POSTGRES_READ_RE = re.compile(
    r"^\s*(SELECT|WITH|PRAGMA)\b",
    flags=re.IGNORECASE,
)


def _kanban_active_store_is_postgres() -> bool:
    return cfg.KANBAN_DATASTORE_CONFIG.active_store == ACTIVE_STORE_POSTGRES


def _kanban_table_sync_gen(conn: Any, source: str) -> int | None:
    """Return a SQLite sync generation only while Kanban tables are SQLite-backed."""
    if _kanban_active_store_is_postgres():
        return None
    return increment_gen(conn, source)


def _params_contain_value(params: Any, value: str) -> bool:
    if isinstance(params, dict):
        return any(_params_contain_value(item, value) for item in params.values())
    if isinstance(params, (list, tuple, set)):
        return any(_params_contain_value(item, value) for item in params)
    return str(params or "") == value


def _kanban_active_postgres_uses_statement(sql: str, params: Any = None) -> bool:
    statement = str(sql or "")
    if _KANBAN_ACTIVE_POSTGRES_TRANSACTION_RE.match(statement):
        return False
    if _KANBAN_ACTIVE_POSTGRES_SQLITE_ONLY_RE.search(statement):
        return False
    if _KANBAN_ACTIVE_POSTGRES_TABLE_RE.search(statement):
        return True
    if re.search(r"\bsettings\b", statement, flags=re.IGNORECASE):
        return _params_contain_value(params, KANBAN_SHOW_TEST_ENTRIES_SETTING)
    return False


class _KanbanActivePostgresConnection:
    """Route active Kanban table traffic to Postgres only."""

    def __init__(self, sqlite_conn: Any) -> None:
        self._sqlite_conn = sqlite_conn
        self._postgres_conn: Any | None = None
        config = cfg.KANBAN_DATASTORE_CONFIG
        self._current_node_id = config.current_node_id
        self._owner_node_id = config.postgres_owner_node_id
        self._reject_replica_writes = (
            config.active_store == ACTIVE_STORE_POSTGRES
            and bool(config.current_node_id)
            and bool(config.postgres_owner_node_id)
            and config.current_node_id != config.postgres_owner_node_id
            and config.postgres_replica_write_policy == "reject"
        )

    @property
    def row_factory(self) -> Any:
        return self._sqlite_conn.row_factory

    @row_factory.setter
    def row_factory(self, value: Any) -> None:
        self._sqlite_conn.row_factory = value

    def _postgres(self) -> Any:
        if self._postgres_conn is None:
            try:
                self._postgres_conn = postgres_candidate_connection(
                    cfg.KANBAN_DATASTORE_CONFIG.candidate_database_url
                )
                self._postgres_conn.begin()
            except KanbanPostgresError:
                raise
            except Exception as exc:
                raise KanbanPostgresError(
                    f"active Kanban Postgres connection failed: {exc}"
                ) from exc
        return self._postgres_conn

    def execute(self, sql: str, params: Any = None) -> Any:
        if not _kanban_active_postgres_uses_statement(sql, params):
            if params is None:
                return self._sqlite_conn.execute(sql)
            return self._sqlite_conn.execute(sql, params)

        if _KANBAN_ACTIVE_POSTGRES_READ_RE.match(str(sql or "")):
            postgres = self._postgres()
            return postgres.execute(sql, params)
        if self._reject_replica_writes:
            raise HTTPException(
                409,
                (
                    "This node is a Postgres Kanban read replica. "
                    f"Canonical Kanban writes must go through {self._owner_node_id}; "
                    "local replica writes are rejected to avoid multi-writer drift."
                ),
            )

        postgres = self._postgres()
        postgres.execute(sql, params)
        return postgres

    def executemany(self, sql: str, seq_of_params: Any) -> Any:
        params_list = list(seq_of_params)
        if _kanban_active_postgres_uses_statement(sql, params_list):
            if self._reject_replica_writes:
                raise HTTPException(
                    409,
                    (
                        "This node is a Postgres Kanban read replica. "
                        f"Canonical Kanban writes must go through {self._owner_node_id}; "
                        "local replica writes are rejected to avoid multi-writer drift."
                    ),
                )
            return self._postgres().executemany(sql, params_list)
        return self._sqlite_conn.executemany(sql, params_list)

    def commit(self) -> None:
        if self._postgres_conn is not None:
            self._postgres_conn.commit()

    def rollback(self) -> None:
        if self._postgres_conn is not None:
            self._postgres_conn.rollback()

    def close(self) -> None:
        if self._postgres_conn is not None:
            self._postgres_conn.close()
            self._postgres_conn = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sqlite_conn, name)


@contextmanager
def _kanban_postgres_get_conn(*, operation: str) -> Any:
    start_perf_ns = time.perf_counter_ns()
    start_time_ns = time.time_ns()
    conn = None
    ok = True
    error_type = ""
    commit_start_ns = 0
    commit_end_ns = 0
    try:
        conn = postgres_candidate_connection(cfg.KANBAN_DATASTORE_CONFIG.candidate_database_url)
        conn.begin()
        yield conn
        commit_start_ns = time.perf_counter_ns()
        conn.commit()
        commit_end_ns = time.perf_counter_ns()
    except Exception as exc:
        ok = False
        error_type = type(exc).__name__
        if conn is not None:
            with suppress(Exception):
                conn.rollback()
        raise
    finally:
        if conn is not None:
            with suppress(Exception):
                conn.close()
        timing.record_span(
            "kanban_postgres_connection",
            start_perf_ns=start_perf_ns,
            end_perf_ns=time.perf_counter_ns(),
            start_time_ns=start_time_ns,
            end_time_ns=time.time_ns(),
            active_store=ACTIVE_STORE_POSTGRES,
            operation=operation,
            ok=ok,
            error_type=error_type,
            commit_start_perf_ns=commit_start_ns,
            commit_end_perf_ns=commit_end_ns,
            commit_ms=round(max(0, commit_end_ns - commit_start_ns) / 1_000_000, 3)
            if commit_start_ns and commit_end_ns
            else None,
        )


@contextmanager
def _kanban_automation_scan_conn(*, operation: str) -> Any:
    if _kanban_active_store_is_postgres() and getattr(
        cfg.KANBAN_DATASTORE_CONFIG,
        "candidate_database_url",
        "",
    ):
        with _kanban_postgres_get_conn(operation=operation) as conn:
            yield conn
        return
    with get_conn() as conn:
        yield conn


def _kanban_begin_write_transaction(conn: Any) -> None:
    if _kanban_active_store_is_postgres():
        return
    conn.execute("BEGIN IMMEDIATE")


def _is_kanban_active_postgres_connection(conn: Any) -> bool:
    return isinstance(conn, _KanbanActivePostgresConnection)


@contextmanager
def get_conn() -> Any:
    with _sqlite_get_conn() as sqlite_conn:
        if not _kanban_active_store_is_postgres():
            yield sqlite_conn
            return
        conn = _KanbanActivePostgresConnection(sqlite_conn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _kanban_store(conn: Any) -> KanbanStore:
    return KanbanStore(
        conn,
        depth_limit=KANBAN_DEPTH_LIMIT,
        show_test_entries_setting=KANBAN_SHOW_TEST_ENTRIES_SETTING,
        agent_working_out_tag=KANBAN_AGENT_WORKING_OUT_TAG,
        item_detail_document_reader=_work_item_detail_document,
        item_review_document_reader=_work_item_review_document,
        item_detail_document_writer=_write_work_item_detail_document,
        item_review_document_writer=_write_work_item_review_document,
    )


def _kanban_write_store(conn: Any) -> KanbanStore:
    return _kanban_store(conn)


@contextmanager
def _kanban_read_connection(conn: Any) -> Any:
    candidate_conn = None
    if _kanban_active_store_is_postgres():
        if _is_kanban_active_postgres_connection(conn):
            yield conn
            return
        try:
            candidate_conn = postgres_candidate_connection(
                cfg.KANBAN_DATASTORE_CONFIG.candidate_database_url
            )
        except KanbanPostgresError as exc:
            raise HTTPException(503, str(exc)) from exc
        try:
            yield candidate_conn
        finally:
            candidate_conn.close()
        return
    if cfg.KANBAN_DATASTORE_CONFIG.read_store == CANDIDATE_READ_STORE_SHADOW:
        candidate_conn = kanban_shadow_candidate_connection(
            conn,
            support_setting_keys=(KANBAN_SHOW_TEST_ENTRIES_SETTING,),
        )
        try:
            yield candidate_conn
        finally:
            candidate_conn.close()
        return
    if cfg.KANBAN_DATASTORE_CONFIG.read_store == CANDIDATE_READ_STORE_POSTGRES:
        try:
            candidate_conn = postgres_candidate_connection(
                cfg.KANBAN_DATASTORE_CONFIG.candidate_database_url
            )
        except KanbanPostgresError as exc:
            raise HTTPException(503, str(exc)) from exc
        try:
            yield candidate_conn
        finally:
            candidate_conn.close()
        return
    yield conn


@contextmanager
def _kanban_read_store(conn: Any) -> Any:
    with _kanban_read_connection(conn) as read_conn:
        yield _kanban_store(read_conn)


def _raise_kanban_store_error(exc: Exception) -> None:
    if isinstance(exc, KanbanItemNotFound):
        raise HTTPException(404, str(exc)) from exc
    if isinstance(exc, KanbanItemCycleError):
        raise HTTPException(400, str(exc)) from exc
    raise exc


PERSONAL_FILTER_SHAPES = {
    "circle",
    "square",
    "triangle",
    "star",
    "pentagon",
    "rectangle",
    "rhombus",
    "semicircle",
    "crescent",
}
PERSONAL_FILTER_FILLS = {"filled", "outline"}


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
    range_start_date: str | None = None
    range_end_date: str | None = None
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


class PersonalFilterTagUpsertRequest(BaseModel):
    tag_id: str | None = None
    label: str
    color: str = "blue"
    shape: str = "circle"
    fill: str = "outline"
    meta_tag_id: str | None = None
    builtin: bool = False
    actor: str = "blueprints-ui"
    source_surface: str = "personal-filters"
    request_id: str | None = None
    run_id: str | None = None


class PersonalFilterMetaTagUpsertRequest(BaseModel):
    meta_tag_id: str | None = None
    label: str
    color: str = "blue"
    priority: int = 0
    actor: str = "blueprints-ui"
    source_surface: str = "personal-filters"
    request_id: str | None = None
    run_id: str | None = None


class PersonalFilterDeleteRequest(BaseModel):
    force: bool = False
    actor: str = "blueprints-ui"
    source_surface: str = "personal-filters"
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
    automation_excluded: bool = False
    sort_order: int = 0
    tags: list[str] = []
    related_event_ids: list[str] = []
    related_task_ids: list[str] = []
    related_issue_ids: list[str] = []
    blocker_title: str | None = None
    blocker_body: str | None = None
    blocked_by_ref: str | None = None
    automation_source_item_id: str | None = None
    automation_marker_id: str | None = None
    automation_decision_id: str | None = None
    automation_reason: str | None = None
    automation_operation_kind: str | None = None
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
    automation_excluded: bool | None = None
    sort_order: int | None = None
    tags: list[str] | None = None
    related_event_ids: list[str] | None = None
    related_task_ids: list[str] | None = None
    related_issue_ids: list[str] | None = None
    blocker_title: str | None = None
    blocker_body: str | None = None
    blocked_by_ref: str | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkItemMoveRequest(BaseModel):
    parent_item_id: str | None = None
    state_id: str | None = None
    sort_order: int | None = None
    blocker_title: str | None = None
    blocker_body: str | None = None
    blocked_by_ref: str | None = None
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


class WorkPriorityRecommendationInput(BaseModel):
    item_id: str
    title: str | None = None
    summary: str | None = None
    reason: str | None = None
    priority_id: str | None = None
    state_id: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = {}


class WorkPriorityRecommendationsReplaceRequest(BaseModel):
    recommendations: list[WorkPriorityRecommendationInput] = []
    strategy_version: str = "skill-managed-v1"
    generated_at: str | None = None
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


class WorkDatastoreBootstrapRequest(BaseModel):
    apply: bool = False
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


class WorkReviewFeedbackCaptureRequest(BaseModel):
    feedback: str
    session_id: str
    feedback_id: str | None = None
    capture_source: str = "explicit_command"
    source_ref: str | None = None
    related_refs: list[str] = []
    child_item_id: str | None = None
    proof_refs: list[str] = []
    outcome_ref: str | None = None
    outcome_summary: str | None = None
    metadata: dict[str, Any] = {}
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


class WorkReviewDecisionCreateRequest(BaseModel):
    decision_id: str | None = None
    processor_kind: str = "review"
    decision_type: str = "decision"
    title: str | None = None
    summary: str
    rationale: str = ""
    affected_refs: list[str] = []
    confidence: str = ""
    uncertainty: str = ""
    proof_refs: list[str] = []
    commit_link_ids: list[str] = []
    status: str = "recorded"
    provider_mode: str = "local"
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkReviewProcessorLeaseRequest(BaseModel):
    holder_id: str
    item_id: str | None = None
    session_id: str | None = None
    lease_token: str | None = None
    ttl_seconds: int = 1200
    force: bool = False
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkReviewProcessorIdleScanRequest(BaseModel):
    item_id: str | None = None
    max_items: int = 100
    include_empty: bool = False
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkPreprocessingIdleScanRequest(BaseModel):
    item_id: str | None = None
    max_items: int = 100
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkAutomationIdleTickRequest(BaseModel):
    item_id: str | None = None
    max_scan_items: int | None = None
    max_process_items: int | None = None
    holder_id: str | None = None
    lease_ttl_seconds: int | None = None
    marker_timeout_seconds: int | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-automation-status"
    request_id: str | None = None
    run_id: str | None = None


class WorkAutomationFailurePruneRequest(BaseModel):
    item_id: str | None = None
    marker_id: str | None = None
    processor_kind: str | None = None
    error_class: str | None = None
    before_failed_at: str | None = None
    include_active: bool = False
    apply: bool = False
    limit: int = 500
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-automation-status"
    request_id: str | None = None
    run_id: str | None = None


class WorkBlockedLeafInvariantRepairRequest(BaseModel):
    item_id: str | None = None
    include_test_entries: bool = True
    apply: bool = False
    max_items: int = 500
    actor: str = "codex"
    source_surface: str = "kanban-blocked-leaf-repair"
    request_id: str | None = None
    run_id: str | None = None


class WorkReviewProcessorMarkerClaimRequest(BaseModel):
    holder_id: str
    lease_token: str | None = None
    item_id: str | None = None
    timeout_seconds: int = 1200
    eligible_marker_ids: list[str] | None = None
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkReviewProcessorMarkerCompleteRequest(BaseModel):
    holder_id: str
    lease_token: str | None = None
    document_source_hash: str | None = None
    decision_id: str | None = None
    status: str = "processed"
    error: str = ""
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkReviewProcessorTimeoutRequeueRequest(BaseModel):
    item_id: str | None = None
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkAgentHintsUpdateRequest(BaseModel):
    required_skills: list[str] | None = None
    routing_notes: str | None = None
    commit_attribution: dict[str, Any] | None = None
    status: str | None = None
    metadata: dict[str, Any] | None = None
    actor: str = "blueprints-ui"
    source_surface: str = "kanban-agent-context-api"
    request_id: str | None = None
    run_id: str | None = None


class WorkAgentSessionCreateRequest(BaseModel):
    session_id: str | None = None
    agent_id: str
    node_id: str | None = None
    worktree_path: str | None = None
    repo_full_name: str | None = None
    branch: str | None = None
    status: str = "active"
    started_at: str | None = None
    ended_at: str | None = None
    last_seen_at: str | None = None
    request_hash: str | None = None
    source_surface: str = "kanban-agent-session-api"
    summary: str | None = None
    metadata: dict[str, Any] = {}
    actor: str = "blueprints-ui"
    request_id: str | None = None
    run_id: str | None = None


class WorkAgentSessionUpdateRequest(BaseModel):
    agent_id: str | None = None
    node_id: str | None = None
    worktree_path: str | None = None
    repo_full_name: str | None = None
    branch: str | None = None
    status: str | None = None
    started_at: str | None = None
    ended_at: str | None = None
    last_seen_at: str | None = None
    request_hash: str | None = None
    source_surface: str = "kanban-agent-session-api"
    summary: str | None = None
    metadata: dict[str, Any] | None = None
    actor: str = "blueprints-ui"
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


def _work_request_is_kanban_idle_worker(meta: dict[str, str]) -> bool:
    return (
        str(meta.get("source_surface") or "").lower() == "kanban-automation-idle-worker"
        or str(meta.get("actor") or "").lower() == "kanban-idle-worker"
    )


def _work_request_is_automation(meta: dict[str, str]) -> bool:
    actor = str(meta.get("actor") or "").strip().lower()
    source_surface = str(meta.get("source_surface") or "").strip().lower()
    automation_surfaces = {
        "kanban-automation-idle-worker",
        "kanban-review-processor",
        "codex-regression-review",
    }
    return (
        _work_request_is_kanban_idle_worker(meta)
        or source_surface in automation_surfaces
        or source_surface.startswith("kanban-automation-")
        or source_surface.startswith("kanban-review-processor")
        or actor in {"kanban-idle-worker", "kanban-review-processor"}
    )


def _work_request_requires_blocked_leaf_guard(meta: dict[str, str]) -> bool:
    return _work_request_is_automation(meta)


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


def _kanban_review_decision_id() -> str:
    return f"kanban-decision-{uuid.uuid4().hex[:16]}"


def _kanban_agent_hints_id(item_id: str) -> str:
    digest = hashlib.sha256(item_id.encode("utf-8")).hexdigest()
    return f"kanban-agent-hints-{digest[:24]}"


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


def _clean_review_decision_status(value: str | None) -> str:
    status = _clean_short_text(value, "recorded", limit=80).replace("-", "_")
    allowed = {
        "recorded",
        "pending",
        "accepted",
        "declined",
        "superseded",
        "failed",
        "hook_failed",
    }
    return status if status in allowed else "recorded"


def _clean_review_provider_mode(value: str | None) -> str:
    mode = _clean_short_text(value, "local", limit=80).replace("_", "-")
    allowed = {
        "cloud-first",
        "cloud",
        "local-planned",
        "local",
        "manual",
        "required-hermes-kanban-llm",
    }
    return mode if mode in allowed else "local"


def _work_automation_local_ai_model_alias() -> str:
    return _clean_short_text(
        os.environ.get(KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV, ""),
        "",
        limit=220,
    )


def _read_env_file_value(path: Path, name: str) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    prefix = f"{name}="
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or not line.startswith(prefix):
            continue
        return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _work_automation_processor_profile_spec(processor_kind: str) -> dict[str, Any]:
    clean_kind = _clean_short_text(processor_kind or "review", "review", limit=80)
    spec = KANBAN_PROCESSOR_PROFILE_SPECS.get(clean_kind)
    if not spec:
        raise ValueError(f"unsupported Kanban processor profile kind: {clean_kind}")
    return spec


def _work_automation_processor_model_alias(processor_kind: str) -> str:
    route = _work_automation_processor_profile_route(processor_kind)
    return f"{route['profile']}:{route['primary_provider']}/{route['primary_model']}"


def _work_automation_processor_profile_config(
    profile: str,
    *,
    template_path: Path | None = None,
    live_path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    template_path = (
        template_path or HERMES_LOCAL_STACK_ROOT / "config/profiles" / profile / "config.yaml"
    )
    live_path = live_path or HERMES_PROFILE_DATA_ROOT / profile / "config.yaml"
    config_path = live_path if live_path.exists() else template_path
    if not config_path.exists():
        return config_path, {}
    cache_key = str(config_path)
    try:
        stat = config_path.stat()
    except OSError:
        return config_path, {}
    cached = _WORK_AUTOMATION_PROFILE_CONFIG_CACHE.get(cache_key)
    if cached and cached[0] == stat.st_mtime_ns and cached[1] == stat.st_size:
        return config_path, cached[2]
    try:
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        _WORK_AUTOMATION_PROFILE_CONFIG_CACHE.pop(cache_key, None)
        return config_path, {}
    parsed = loaded if isinstance(loaded, dict) else {}
    _WORK_AUTOMATION_PROFILE_CONFIG_CACHE[cache_key] = (
        stat.st_mtime_ns,
        stat.st_size,
        parsed,
    )
    return config_path, parsed


def _work_automation_processor_profile_fallback(
    parsed: dict[str, Any],
    *,
    default_provider: str,
    default_model: str,
) -> tuple[str, str]:
    fallbacks = parsed.get("fallback_providers") if isinstance(parsed, dict) else []
    if isinstance(fallbacks, list):
        for item in fallbacks:
            if not isinstance(item, dict):
                continue
            provider = _clean_short_text(item.get("provider"), "", limit=160)
            model = _clean_short_text(item.get("model"), "", limit=200)
            if provider and model:
                return provider, model
    return default_provider, default_model


def _work_automation_processor_profile_route(processor_kind: str) -> dict[str, Any]:
    spec = _work_automation_processor_profile_spec(processor_kind)
    profile = str(spec["profile"])
    template_config_path = HERMES_LOCAL_STACK_ROOT / "config/profiles" / profile / "config.yaml"
    live_config_path = HERMES_PROFILE_DATA_ROOT / profile / "config.yaml"
    _config_path, parsed_config = _work_automation_processor_profile_config(
        profile,
        template_path=template_config_path,
        live_path=live_config_path,
    )
    fallback_provider, fallback_model = _work_automation_processor_profile_fallback(
        parsed_config,
        default_provider=str(spec["fallback_provider"]),
        default_model=str(spec["fallback_model"]),
    )
    api_base = _clean_short_text(
        os.environ.get(str(spec["api_base_env"]), str(spec["api_base"])),
        str(spec["api_base"]),
        limit=260,
    ).rstrip("/")
    api_key_file = Path(os.environ.get(str(spec["api_key_file_env"]), str(spec["api_key_file"])))
    return {
        "schema": "xarta.kanban.processor_profile.route.v1",
        "processor_kind": processor_kind,
        "provider_mode": KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
        "processor_engine": KANBAN_AUTOMATION_PROFILE_ENGINE,
        "profile": profile,
        "api_base": api_base,
        "api_key_file": str(api_key_file),
        "api_key_present": bool(_read_env_file_value(api_key_file, "API_SERVER_KEY")),
        "primary_provider": str(spec["primary_provider"]),
        "primary_model": str(spec["primary_model"]),
        "fallback_provider": fallback_provider,
        "fallback_model": fallback_model,
        "model_alias": f"{profile}:{spec['primary_provider']}/{spec['primary_model']}",
        "template_config_path": str(template_config_path),
        "live_config_path": str(live_config_path),
    }


def _work_automation_processor_profile_drift(processor_kind: str) -> dict[str, Any]:
    route = _work_automation_processor_profile_route(processor_kind)
    profile = route["profile"]
    template_path = Path(route["template_config_path"])
    live_path = Path(route["live_config_path"])
    config_path = live_path if live_path.exists() else template_path
    problems: list[str] = []
    warnings: list[str] = []
    parsed: dict[str, Any] = {}
    if not config_path.exists():
        problems.append("profile_config_missing")
    else:
        _config_path, parsed = _work_automation_processor_profile_config(
            profile,
            template_path=template_path,
            live_path=live_path,
        )
        if not parsed:
            problems.append("profile_config_unreadable")
    model = parsed.get("model") if isinstance(parsed, dict) else {}
    primary_provider = str(model.get("provider") or "") if isinstance(model, dict) else ""
    primary_model = str(model.get("default") or "") if isinstance(model, dict) else ""
    if primary_provider != route["primary_provider"]:
        problems.append("primary_provider_drift")
    if primary_model != route["primary_model"]:
        problems.append("primary_model_drift")
    fallbacks = parsed.get("fallback_providers") if isinstance(parsed, dict) else []
    fallback_ok = any(
        isinstance(item, dict)
        and item.get("provider") == route["fallback_provider"]
        and item.get("model") == route["fallback_model"]
        for item in (fallbacks if isinstance(fallbacks, list) else [])
    )
    if not fallback_ok:
        warnings.append("fallback_model_drift")
    if not route["api_key_present"]:
        problems.append("api_server_key_missing")
    return {
        "schema": "xarta.kanban.processor_profile.auth_drift.v1",
        "profile": profile,
        "processor_kind": processor_kind,
        "ok": not problems,
        "config_path": str(config_path),
        "live_config_used": config_path == live_path,
        "expected": {
            "primary_provider": route["primary_provider"],
            "primary_model": route["primary_model"],
            "fallback_provider": route["fallback_provider"],
            "fallback_model": route["fallback_model"],
        },
        "observed": {
            "primary_provider": primary_provider,
            "primary_model": primary_model,
            "api_key_present": route["api_key_present"],
        },
        "problems": problems,
        "warnings": warnings,
    }


def _work_review_processing_policy() -> dict[str, Any]:
    processor_routes = {
        kind: _work_automation_processor_profile_route(kind) for kind in ("review", "preprocessing")
    }
    auth_drift = {
        kind: _work_automation_processor_profile_drift(kind) for kind in ("review", "preprocessing")
    }
    policy = {
        "schema": KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA,
        "status": "active",
        "version": "2026-06-29",
        "active_mode": KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
        "applies_to": ["review_processor", "preprocessing"],
        "profile_processing": {
            "state": "active",
            "required": True,
            "automatic_switch": False,
            "routes": processor_routes,
            "auth_drift": auth_drift,
            "failure_policy": "record_retryable_failure_event_and_backoff",
        },
        "local_processing": {
            "state": "fallback-model-only",
            "gate": "hermes_profile_configured_fallback_only",
            "automatic_switch": False,
            "fallback_model": processor_routes["review"]["fallback_model"],
            "fallback_provider": processor_routes["review"]["fallback_provider"],
            "substitute_decisions_allowed": False,
        },
        "provider_choice": {
            "required": True,
            "default_mode": KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
            "allowed_modes": [KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE, "manual"],
            "blocked_until_explicit_api": [],
        },
        "routing_rules": [
            "Review Processor and preprocessing jobs call their dedicated Hermes profile gateway while this policy is active.",
            "Primary model for both profiles is openai-codex/gpt-5.5 through the Codex subscription/auth path.",
            "Configured local fallback is a model fallback only; deterministic substitute decisions are forbidden.",
            "Do not use deterministic substitute processing when a required provider route is unavailable.",
            "If a requested provider/API path is missing, record retryable failure/backoff instead of silently changing provider mode.",
            "Provider mode must be explicit in queue packets and decision records.",
            "Do not silently switch provider modes when profile processing fails.",
        ],
    }
    return policy


def _work_automation_idle_worker_contract() -> dict[str, Any]:
    contract = json.loads(KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_PATH.read_text())
    if contract.get("schema") != KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA:
        raise RuntimeError(
            f"Kanban idle worker contract schema mismatch: {contract.get('schema')!r}"
        )
    return contract


def _work_review_processing_metadata_contract(
    processing_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    processing_policy = processing_policy or _work_review_processing_policy()
    return {
        "schema": KANBAN_REVIEW_METADATA_CONTRACT_SCHEMA,
        "status": "active",
        "version": "2026-06-29",
        "review_document_schema": KANBAN_ITEM_REVIEW_SCHEMA,
        "marker_schema": KANBAN_REVIEW_MARKER_SCHEMA,
        "scheduler_schema": KANBAN_REVIEW_SCHEDULER_SCHEMA,
        "failure_event_schema": KANBAN_REVIEW_FAILURE_EVENT_SCHEMA,
        "retry_policy_version": KANBAN_REVIEW_RETRY_POLICY_VERSION,
        "retry_backoff_seconds": list(KANBAN_REVIEW_RETRY_BACKOFF_SECONDS),
        "provider_mode": {
            "active": processing_policy["active_mode"],
            "profile_processing": processing_policy["profile_processing"],
            "local_processing_gate": processing_policy["local_processing"]["gate"],
            "automatic_switch": processing_policy["profile_processing"]["automatic_switch"],
        },
        "storage": {
            "review_document": "item Review markdown frontmatter",
            "marker_table": "kanban_review_processor_markers",
            "failure_event_table": "kanban_review_processor_failure_events",
            "decision_table": "kanban_review_decisions",
        },
        "required_fields": [
            {
                "field": "body_hash",
                "scope": "review_document.metadata",
                "meaning": "Hash of the normalized Review markdown body.",
                "updates_when": "Review body content changes.",
            },
            {
                "field": "updated_at",
                "scope": "review_document.metadata",
                "alias": "review_updated_at",
                "meaning": "UTC Review content timestamp used by the scanner.",
                "updates_when": "body_hash changes.",
            },
            {
                "field": "operator_feedback.entries",
                "scope": "review_document.metadata",
                "meaning": "Explicit command/discussion feedback captures with feedback id, UTC date, actor, session id, affected item refs, capture source, source ref, structured session/child/proof/outcome attribution, and feedback hash.",
                "updates_when": "A bounded feedback capture appends an operator-feedback section to the Review markdown.",
                "entry_schema": KANBAN_REVIEW_FEEDBACK_SCHEMA,
                "collection_schema": KANBAN_REVIEW_FEEDBACK_COLLECTION_SCHEMA,
                "attribution_schema": KANBAN_REVIEW_FEEDBACK_ATTRIBUTION_SCHEMA,
            },
            {
                "field": "document_source_hash",
                "scope": "kanban_review_processor_markers",
                "meaning": "Stable scanner source hash for the Review document snapshot.",
                "updates_when": "Review body, Review updated_at, schema, item id, or file ref changes.",
            },
            {
                "field": "processed_source_hash",
                "scope": "kanban_review_processor_markers",
                "meaning": "Legacy last processed source hash retained for marker history; duplicate-scan suppression uses last_successful_source_hash and only falls back to this field on processed markers migrated before the success-only field existed.",
                "updates_when": "marker completes with processed success. Retryable failed outcomes clear this field when it matches the failed source so failures cannot masquerade as successful processing.",
            },
            {
                "field": "processed_at",
                "scope": "kanban_review_processor_markers",
                "alias": "last_processed_at",
                "meaning": "UTC time of the last processed success retained for marker history.",
                "updates_when": "marker completes with processed success. Retryable failed outcomes clear this field when the legacy processed source hash matches the failed source.",
            },
            {
                "field": "last_successful_source_hash",
                "scope": "kanban_review_processor_markers",
                "meaning": "Last source hash that completed with processed success.",
                "updates_when": "marker completes with processed status.",
            },
            {
                "field": "next_retry_at",
                "scope": "kanban_review_processor_markers",
                "meaning": "UTC time when a retryable failed marker becomes claimable again.",
                "updates_when": "marker completes with failed status under the retry policy, then clears when claimed, superseded, cancelled, or processed.",
            },
            {
                "field": "retry_attempt_count",
                "scope": "kanban_review_processor_markers",
                "meaning": "Source-specific failed attempt count for the current retry series.",
                "updates_when": "marker completes with failed status for the current document_source_hash; reset when source changes or succeeds.",
            },
            {
                "field": "last_failure_event_id",
                "scope": "kanban_review_processor_markers",
                "meaning": "Pointer to the latest durable failure event for the current retry series.",
                "updates_when": "marker completes with failed status.",
            },
            {
                "field": "last_error_class",
                "scope": "kanban_review_processor_markers",
                "meaning": "Machine-readable class for the latest retryable failure.",
                "updates_when": "marker completes with failed status.",
            },
            {
                "field": "status",
                "scope": "kanban_review_processor_markers",
                "meaning": "Queue state for the Review processing marker.",
                "allowed_values": [
                    "queued",
                    "processing",
                    "processed",
                    "failed",
                    "skipped",
                    "cancelled",
                ],
            },
            {
                "field": "run_id",
                "scope": "marker.provenance",
                "meaning": "Run/request provenance for the scanner, worker claim, timeout, or completion write.",
            },
            {
                "field": "last_error",
                "scope": "kanban_review_processor_markers",
                "meaning": "Machine-readable processing, timeout, supersede, or cancellation reason.",
            },
            {
                "field": "last_outcome_at",
                "scope": "marker.metadata",
                "meaning": "UTC time of the latest terminal completion or timeout requeue outcome.",
            },
            {
                "field": "last_outcome_status",
                "scope": "marker.metadata",
                "meaning": "Latest terminal or timeout outcome such as processed, failed, skipped, cancelled, or timeout_requeued.",
            },
            {
                "field": "processing_expires_at",
                "scope": "kanban_review_processor_markers",
                "meaning": "UTC deadline used to requeue timed-out processing markers.",
            },
            {
                "field": "superseded_at",
                "scope": "kanban_review_processor_markers",
                "meaning": "UTC time a processing marker was superseded by newer Review content.",
            },
            {
                "field": "superseded_by_source_hash",
                "scope": "kanban_review_processor_markers",
                "meaning": "New Review source hash that replaced an in-flight processing attempt.",
            },
        ],
        "transition_rules": [
            "Review saves preserve updated_at and document_source_hash when body_hash is unchanged.",
            "Explicit feedback captures append to the item Review markdown and update review_document.metadata.operator_feedback.",
            "Idle scan queues a marker when document_source_hash differs from processed_source_hash and no current queued/processing marker exists for the same document.",
            "A retryable failed marker records a kanban_review_processor_failure_events row, sets next_retry_at using capped exponential backoff, and does not update processed_source_hash.",
            "A failed marker becomes claimable again after next_retry_at unless the source is superseded, the item is archived/deleted/excluded, or the operator cancels/disables the work.",
            "A processing marker is requeued with last_error=processing_timeout when processing_expires_at is in the past.",
            "A processing marker is requeued with last_error=review_changed_during_processing and superseded fields when Review content changes during processing.",
            "A queued or processing marker is cancelled with last_error=review_document_deleted when Review text is emptied or removed.",
            "A queued or processing marker is cancelled with last_error=item_archived when the source Kanban item is archived.",
        ],
        "cancellation_fields": [
            "status=cancelled",
            "last_error",
            "last_seen_at",
            "processing_started_at",
            "processing_expires_at",
            "metadata.cancelled_previous_status",
        ],
    }


def _work_preprocessing_readiness_contract(
    processing_policy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    processing_policy = processing_policy or _work_review_processing_policy()
    return {
        "schema": KANBAN_PREPROCESSING_READINESS_CONTRACT_SCHEMA,
        "status": "active",
        "version": "2026-06-29",
        "context_packet_schema": "xarta.kanban.context_packet.v1",
        "readiness_marker_schema": "xarta.kanban.context_readiness_marker.v1",
        "readiness_check_schema": "xarta.kanban.context_readiness_check.v1",
        "preprocessing_request_schema": "xarta.kanban.preprocessing_time_request.v1",
        "queue_schema": KANBAN_PREPROCESSING_QUEUE_SCHEMA,
        "marker_storage": "kanban_agent_hints.metadata.context_readiness_marker",
        "provider_mode": {
            "active": processing_policy["active_mode"],
            "profile_processing": processing_policy["profile_processing"],
            "local_processing_gate": processing_policy["local_processing"]["gate"],
            "automatic_switch": processing_policy["profile_processing"]["automatic_switch"],
        },
        "required_fields": [
            {
                "field": "context_hash",
                "scope": "context_readiness_marker",
                "meaning": "Stable hash of the accepted context packet component hashes.",
                "updates_when": "Body, Detail, Review, Discussion, tree, link, blocker, commit, Issue, ToDo, count, or rollup context changes.",
            },
            {
                "field": "component_hashes",
                "scope": "context_readiness_marker",
                "meaning": "Per-component hashes for rich_documents, work_state, and tree_state drift diagnosis.",
            },
            {
                "field": "marked_at",
                "scope": "context_readiness_marker",
                "alias": "last_preprocessed_at",
                "meaning": "UTC time the current packet was accepted by preprocessing.",
            },
            {
                "field": "ready",
                "scope": "context_readiness_check",
                "alias": "readiness_state",
                "meaning": "True only when the current packet matches the stored marker and hard validation checks pass.",
            },
            {
                "field": "reason",
                "scope": "context_readiness_check",
                "meaning": "Machine-readable readiness result such as ready, missing_readiness_marker, readiness_marker_stale, tree_validation_failed, missing_body, or open_blockers.",
            },
            {
                "field": "open_questions",
                "scope": "context_readiness_marker.metadata",
                "meaning": "Questions or preprocessing requests that must be answered before implementation proceeds.",
            },
            {
                "field": "links",
                "scope": "context_packet.work_state",
                "meaning": "Kanban links considered by preprocessing, including link_count and linked item refs.",
            },
            {
                "field": "blockers",
                "scope": "context_packet.work_state",
                "meaning": "Open blockers considered by preprocessing; open blockers fail readiness unless explicitly resolved or handled.",
            },
            {
                "field": "ancestor_context",
                "scope": "preprocessing.hermes_profile_input.evidence",
                "meaning": "Immediate parent and recent ancestor body/detail/review excerpts plus recent decisions supplied to preprocessing, and included in source refs so parent-context changes supersede retry waits.",
            },
            {
                "field": "drift_components",
                "scope": "context_readiness_check",
                "alias": "stale_markers",
                "meaning": "Component names whose current hashes differ from the stored marker.",
            },
            {
                "field": "preprocessing_request",
                "scope": "context_readiness_check",
                "meaning": "Plain-language time request, blocking codes, drift summary, and inspect/mark commands emitted when readiness is not current.",
            },
            {
                "field": "decomposition_items",
                "scope": "preprocessing.hermes_profile_output",
                "meaning": "Concrete child Kanban work items to create or confirm when the current card is not yet an implementation-ready leaf.",
            },
        ],
        "packet_inputs": [
            "workspace_orientation",
            "active_private_skills",
            "helper_commands",
            "body",
            "detail",
            "review",
            "discussions",
            "images",
            "tree_state",
            "validation",
            "open_descendants",
            "links",
            "blockers",
            "commits",
            "issues",
            "todos",
            "counts",
            "rollups",
        ],
        "readiness_states": [
            "ready",
            "missing_readiness_marker",
            "readiness_marker_stale",
            "tree_validation_failed",
            "tree_problem_detected",
            "missing_body",
            "open_blockers",
            "missing_detail",
            "missing_review",
            "missing_discussions",
            "missing_commits",
        ],
        "transition_rules": [
            "Preprocessing creates or replaces context_readiness_marker after the current packet is internally sane.",
            "When preprocessing finds missing implementation work on an otherwise processable card, it creates or confirms child Kanban items instead of only listing recommended next actions.",
            "A not-ready parent can still be a successfully preprocessed topic when the missing work has been decomposed into concrete open children.",
            "Implementation starts only when context readiness check returns ready=true.",
            "A missing or stale marker emits preprocessing_request.request_text; agents must request preprocessing time instead of guessing from title words.",
            "Validation failures, missing item body, and open blockers are hard readiness failures.",
            "Detail, Review, discussions, and commits are warnings by default unless the work item explicitly requires them.",
            "Audit-count changes from recording the marker do not stale the packet; real work-state component changes do.",
        ],
    }


def _work_review_processor_output_contract(
    processing_policy: dict[str, Any] | None = None,
    metadata_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    processing_policy = processing_policy or _work_review_processing_policy()
    metadata_contract = metadata_contract or _work_review_processing_metadata_contract(
        processing_policy
    )
    return {
        "schema": KANBAN_REVIEW_OUTPUT_CONTRACT_SCHEMA,
        "status": "active",
        "version": "2026-06-27",
        "decision_record_schema": KANBAN_REVIEW_DECISION_SCHEMA,
        "processing_policy_schema": processing_policy["schema"],
        "metadata_contract_schema": metadata_contract["schema"],
        "provider_mode": {
            "active": processing_policy["active_mode"],
            "profile_processing": processing_policy["profile_processing"],
            "local_processing_gate": processing_policy["local_processing"]["gate"],
            "automatic_switch": processing_policy["profile_processing"]["automatic_switch"],
        },
        "recording_rules": [
            "Every Review Processor output is recorded as a kanban_review_decisions row.",
            "decision_type identifies the emitted output kind.",
            "Structured fields live under metadata.output_payload with output_schema and output_type.",
            "affected_refs must include every Kanban card, Review document, proposal surface, or commit surface the output changes or evaluates.",
            "Code-producing outputs must reference explicit kanban_item_commits rows before they are accepted.",
            "Pre-commit hook failures are recorded as hook_failed or metadata.hook_status=failed until repaired.",
        ],
        "output_types": [
            {
                "type": "lesson",
                "label": "Lesson",
                "decision_type": "lesson",
                "writes": ["kanban_review_decisions", "kanban_discussions_or_review_doc"],
                "required_payload_fields": [
                    "lesson",
                    "evidence_refs",
                    "scope_refs",
                    "operator_impact",
                ],
            },
            {
                "type": "prompt_change",
                "label": "Prompt Change",
                "decision_type": "prompt_change",
                "writes": ["kanban_review_decisions", "implementation_card"],
                "required_payload_fields": [
                    "target_surface",
                    "current_behavior",
                    "requested_behavior",
                    "validation_refs",
                ],
            },
            {
                "type": "contradiction_check",
                "label": "Contradiction Check",
                "decision_type": "contradiction_check",
                "writes": ["kanban_review_decisions"],
                "required_payload_fields": [
                    "claim_a",
                    "claim_b",
                    "resolution",
                    "source_refs",
                ],
            },
            {
                "type": "follow_up_card",
                "label": "Follow-up Card",
                "decision_type": "follow_up_card",
                "writes": ["kanban_review_decisions", "kanban_items"],
                "required_payload_fields": [
                    "title",
                    "body",
                    "parent_ref",
                    "lane",
                    "priority",
                    "reason",
                ],
            },
        ],
        "minimum_decision_fields": [
            "summary",
            "rationale",
            "affected_refs",
            "confidence",
            "proof_refs",
            "metadata.output_payload",
        ],
    }


def _kanban_item_uri(item_id: str) -> str:
    return f"xarta-kanban:item:{item_id}"


def _work_proposal_surfaces_contract() -> dict[str, Any]:
    return {
        "schema": KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA,
        "status": "active",
        "version": "2026-06-27",
        "surface_root": {
            "item_id": KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID,
            "uri": _kanban_item_uri(KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID),
            "role": "operator-created human-agent sign-off area",
        },
        "workstream": {
            "item_id": KANBAN_AGENT_PROPOSAL_WORKSTREAM_ITEM_ID,
            "uri": _kanban_item_uri(KANBAN_AGENT_PROPOSAL_WORKSTREAM_ITEM_ID),
            "role": "implementation workstream for proposal surface behavior and status UI",
        },
        "inbox": {
            "item_id": KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID,
            "uri": _kanban_item_uri(KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID),
            "role": "agent proposal intake",
            "accepted_entry_types": [
                "proposal",
                "question",
                "approval_request",
                "review_processor_follow_up",
                "operator_follow_up",
            ],
            "required_fields": [
                "entry_type",
                "title",
                "summary",
                "requested_operator_action",
                "source_item_refs",
                "actor",
                "created_at",
                "status",
            ],
            "placement_rules": [
                "Create an INBOX child item when the entry needs its own lifecycle, decision, or follow-up.",
                "Use an INBOX discussion note only for small status notes that do not need independent tracking.",
                "Link each INBOX entry to the implementation card, Review document, decision, or source context that caused it.",
                "Do not treat INBOX as the implementation card; code, docs, proof, and rollout work stay on the smallest relevant workstream leaf.",
                "Operator-feedback-to-Review capture remains explicit command or discussion handling only.",
            ],
        },
        "outbox": {
            "item_id": KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID,
            "uri": _kanban_item_uri(KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID),
            "role": "processed proposal outcome surface",
            "accepted_entry_types": [
                "accepted_plan",
                "declined_proposal",
                "completed_decision",
                "handoff_ready_output",
                "operator_visible_closeout",
            ],
            "required_fields": [
                "entry_type",
                "title",
                "summary",
                "outcome",
                "source_inbox_refs",
                "affected_item_refs",
                "proof_refs",
                "commit_link_ids",
                "actor",
                "created_at",
                "status",
            ],
            "placement_rules": [
                "Create an OUTBOX child item for processed proposal outcomes that should remain visible after the source work is done.",
                "Link OUTBOX outcomes back to source INBOX entries and implementation cards.",
                "Accepted implementation work must still carry explicit commit associations on the implementation card.",
                "Declined proposals must say why they were declined and what evidence or operator instruction drove the decision.",
            ],
        },
        "status_integration": {
            "automation_status_field": "proposal_surfaces",
            "operator_visible": True,
            "expected_surfaces": [
                "automation status modal",
                "desktop portrait bottom-panel tab",
                "ultrawide side-panel tab",
            ],
        },
        "global_rules": [
            "Proposal surfaces are interaction surfaces, not substitutes for implementation workstream cards.",
            "Autonomous decisions must be written in clear natural Kanban language with affected refs and proof refs.",
            "Code-producing outcomes are accepted only after pre-commit issues are fixed and explicit commit links exist.",
            "Use local AI processing while the active processing policy is local.",
        ],
    }


def _review_processor_lease_id(processor_kind: str = "review") -> str:
    kind = _clean_short_text(processor_kind, "review", limit=80).replace("_", "-")
    return f"kanban-review-processor-lease-{kind or 'review'}"


def _parse_utc_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _utc_iso_from_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_review_lease_ttl(value: int | None) -> int:
    try:
        ttl = int(value or 1200)
    except (TypeError, ValueError):
        ttl = 1200
    return max(60, min(ttl, 7200))


def _review_lease_is_active(row: Any | None, now_dt: datetime | None = None) -> bool:
    if row is None or row["status"] != "active":
        return False
    expires_at = _parse_utc_datetime(row["expires_at"])
    if expires_at is None:
        return False
    return expires_at > (now_dt or datetime.now(timezone.utc))


def _review_lease_owner_matches(row: Any, holder_id: str, lease_token: str = "") -> bool:
    if row["holder_id"] != holder_id:
        return False
    return not lease_token or not row["lease_token"] or row["lease_token"] == lease_token


def _row_to_work_review_processor_lease(
    row: Any | None,
    *,
    now_dt: datetime | None = None,
    include_token: bool = False,
) -> dict[str, Any]:
    if row is None:
        return {
            "schema": KANBAN_REVIEW_LEASE_SCHEMA,
            "exists": False,
            "lease_id": _review_processor_lease_id(),
            "processor_kind": "review",
            "holder_id": "",
            "item_id": "",
            "session_id": "",
            "status": "missing",
            "active": False,
            "expired": False,
            "acquired_at": "",
            "heartbeat_at": "",
            "expires_at": "",
            "timeout_seconds": 1200,
            "source_hash": "",
            "metadata": {},
            "provenance": {},
            "created_at": None,
            "updated_at": None,
        }
    now_dt = now_dt or datetime.now(timezone.utc)
    expires_at = _parse_utc_datetime(row["expires_at"])
    active = _review_lease_is_active(row, now_dt)
    payload = {
        "schema": KANBAN_REVIEW_LEASE_SCHEMA,
        "exists": True,
        "lease_id": row["lease_id"],
        "processor_kind": row["processor_kind"],
        "holder_id": row["holder_id"],
        "item_id": row["item_id"],
        "session_id": row["session_id"],
        "status": row["status"],
        "active": active,
        "expired": bool(
            row["status"] == "active" and expires_at is not None and expires_at <= now_dt
        ),
        "acquired_at": row["acquired_at"],
        "heartbeat_at": row["heartbeat_at"],
        "expires_at": row["expires_at"],
        "timeout_seconds": int(row["timeout_seconds"] or 1200),
        "source_hash": row["source_hash"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if include_token:
        payload["lease_token"] = row["lease_token"]
    return payload


def _work_review_processor_lease_row(
    *,
    existing: Any | None,
    holder_id: str,
    item_id: str,
    session_id: str,
    lease_token: str,
    ttl_seconds: int,
    status: str,
    metadata: dict[str, Any],
    provenance: dict[str, Any],
    now: str,
) -> dict[str, Any]:
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    expires_at = _utc_iso_from_datetime(now_dt + timedelta(seconds=ttl_seconds))
    acquired_at = (
        existing["acquired_at"]
        if existing is not None and existing["status"] == "active" and status == "active"
        else now
    )
    row = {
        "lease_id": _review_processor_lease_id(),
        "processor_kind": "review",
        "holder_id": holder_id,
        "lease_token": lease_token,
        "item_id": item_id,
        "session_id": session_id,
        "status": status,
        "acquired_at": acquired_at,
        "heartbeat_at": now,
        "expires_at": expires_at if status == "active" else now,
        "timeout_seconds": ttl_seconds,
        "source_hash": "",
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
        "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
        "created_at": existing["created_at"] if existing is not None else now,
        "updated_at": now,
    }
    row["source_hash"] = _hash_json_payload(
        {
            key: value
            for key, value in row.items()
            if key not in {"source_hash", "created_at", "updated_at"}
        }
    )
    return row


def _write_work_review_processor_lease(
    conn: Any,
    row: dict[str, Any],
) -> Any:
    return _kanban_write_store(conn).upsert_review_processor_lease_row(row)


def _write_work_review_processor_lease_audit(
    conn: Any,
    *,
    action: str,
    result: str,
    meta: dict[str, str],
    now: str,
    item_id: str,
    parent_item_id: str,
    source_hash: str,
    metadata: dict[str, Any],
    lease_row: Any | None = None,
) -> dict[str, Any]:
    audit_id = f"audit-{uuid.uuid4().hex}"
    lease_id = lease_row["lease_id"] if lease_row is not None else _review_processor_lease_id()
    audit_row = _write_work_audit(
        conn,
        audit_id=audit_id,
        actor=meta["actor"],
        source_surface=meta["source_surface"],
        action=action,
        target_ref=f"kanban_review_processor_leases:{lease_id}",
        item_id=item_id,
        parent_item_id=parent_item_id,
        created_at=now,
        request_id=meta["request_id"],
        run_id=meta["run_id"],
        result=result,
        source_hash=source_hash,
        metadata=metadata,
    )
    gen = _kanban_table_sync_gen(conn, "kanban-review-processor-lease")
    if lease_row is not None:
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_review_processor_leases",
            lease_row["lease_id"],
            dict(lease_row),
            gen,
        )
    enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return audit_row


def _review_processor_marker_id(
    item_id: str,
    processor_kind: str = "review",
    document_type: str = "review",
) -> str:
    digest = hashlib.sha256(
        f"{item_id}\n{processor_kind}\n{document_type}".encode("utf-8")
    ).hexdigest()
    return f"kanban-review-marker-{digest[:24]}"


def _clean_review_scan_limit(value: int | None) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError):
        limit = 100
    return max(1, min(limit, 500))


def _clean_review_marker_outcome_status(value: str | None) -> str:
    status = _clean_short_text(value, "processed", limit=80).replace("-", "_")
    allowed = {"processed", "failed", "skipped", "cancelled"}
    return status if status in allowed else "processed"


def _row_value(row: Any | None, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    with suppress(Exception):
        if hasattr(row, "keys") and key not in row.keys():
            return default
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def _work_review_retry_after_seconds(attempt_number: int | str | None) -> int:
    try:
        attempt = int(attempt_number or 1)
    except (TypeError, ValueError):
        attempt = 1
    attempt = max(1, attempt)
    index = min(attempt - 1, len(KANBAN_REVIEW_RETRY_BACKOFF_SECONDS) - 1)
    return int(KANBAN_REVIEW_RETRY_BACKOFF_SECONDS[index])


def _work_review_retry_next_at(now: str, attempt_number: int | str | None) -> str:
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    return _utc_iso_from_datetime(
        now_dt + timedelta(seconds=_work_review_retry_after_seconds(attempt_number))
    )


def _work_review_marker_retry_state(row: Any | None, *, now: str | None = None) -> str:
    status = _clean_short_text(_row_value(row, "status", ""), "", limit=80)
    if status != "failed":
        if status in {"skipped", "cancelled"}:
            return "terminal"
        return status or "unknown"
    next_retry_at = _clean_short_text(_row_value(row, "next_retry_at", ""), "", limit=80)
    if not next_retry_at:
        return "retry_due"
    now_dt = _parse_utc_datetime(now or _utc_now_iso()) or datetime.now(timezone.utc)
    retry_dt = _parse_utc_datetime(next_retry_at)
    if retry_dt is not None and retry_dt > now_dt:
        return "retry_waiting"
    return "retry_due"


def _work_processor_marker_claimable_state_predicate(marker_alias: str = "marker") -> str:
    safe_marker_alias = "".join(
        ch for ch in str(marker_alias or "marker") if ch.isalnum() or ch == "_"
    )
    if not safe_marker_alias:
        safe_marker_alias = "marker"
    return f"""
    (
        {safe_marker_alias}.status='queued'
        OR (
            {safe_marker_alias}.status='failed'
            AND (
                COALESCE({safe_marker_alias}.next_retry_at, '')=''
                OR {safe_marker_alias}.next_retry_at <= ?
            )
        )
    )
    """


def _work_review_failure_event_id(
    marker_id: str,
    source_hash: str,
    attempt_number: int,
    failed_at: str,
) -> str:
    digest = hashlib.sha256(
        f"{marker_id}\n{source_hash}\n{attempt_number}\n{failed_at}".encode("utf-8")
    ).hexdigest()
    return f"kanban-review-failure-{digest[:24]}"


def _work_review_failure_error_class(error: str, metadata: dict[str, Any]) -> str:
    explicit = _clean_short_text(
        str(metadata.get("error_class") or metadata.get("exception_class") or ""),
        "",
        limit=120,
    )
    if explicit:
        return explicit
    message = str(error or "")
    lower = message.lower()
    if "api_server_key" in lower or "auth/config drift" in lower or "profile api" in lower:
        return "hermes_profile_configuration"
    if "llm response missing required field" in lower or "missing required field" in lower:
        return "llm_response_validation"
    if "timeout" in lower:
        return "processing_timeout"
    if "profile" in lower or "llm" in lower:
        return "hermes_profile_processing"
    return "processing_error"


def _review_document_ref(document: dict[str, Any]) -> str:
    file_ref = document.get("file_ref") if isinstance(document, dict) else None
    if isinstance(file_ref, dict):
        root = _clean_short_text(file_ref.get("root"), "kanban", limit=80)
        path = _clean_short_text(file_ref.get("path"), "", limit=400)
        if path:
            return f"{root}:{path}"
    return ""


def _review_document_source(document: dict[str, Any]) -> dict[str, Any]:
    body = document.get("body") if isinstance(document, dict) else ""
    body_text = str(body or "")
    payload = {
        "schema": document.get("schema")
        if isinstance(document, dict)
        else KANBAN_ITEM_REVIEW_SCHEMA,
        "item_id": document.get("item_id") if isinstance(document, dict) else "",
        "updated_at": document.get("updated_at") if isinstance(document, dict) else "",
        "file_ref": document.get("file_ref") if isinstance(document, dict) else {},
        "body": body_text,
    }
    return {
        "document_ref": _review_document_ref(document),
        "document_updated_at": _clean_short_text(payload["updated_at"], "", limit=80),
        "document_source_hash": _hash_json_payload(payload),
        "body_bytes": len(body_text.encode("utf-8")),
        "has_review_text": bool(body_text.strip()),
        "document_exists": bool(document.get("exists")) if isinstance(document, dict) else False,
    }


def _kanban_review_feedback_id() -> str:
    return f"kanban-feedback-{uuid.uuid4().hex[:12]}"


def _clean_review_feedback_capture_source(value: str | None) -> str:
    source = _clean_short_text(value, "explicit_command", limit=80).replace("-", "_")
    allowed = {"explicit_command", "explicit_discussion"}
    if source not in allowed:
        raise HTTPException(
            400,
            "Review feedback capture_source must be explicit_command or explicit_discussion",
        )
    return source


def _review_feedback_quote_block(feedback: str) -> str:
    lines = _normalise_markdown_document_body(feedback).splitlines()
    if not lines:
        return "> "
    return "\n".join(f"> {line}" if line else ">" for line in lines)


def _work_review_feedback_session_snapshot(agent_session: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(agent_session, dict) or not agent_session:
        return {}
    snapshot_keys = (
        "session_id",
        "item_id",
        "agent_id",
        "node_id",
        "worktree_path",
        "repo_full_name",
        "branch",
        "status",
        "started_at",
        "ended_at",
        "last_seen_at",
        "summary",
    )
    snapshot = {key: agent_session.get(key) for key in snapshot_keys}
    metadata = agent_session.get("metadata")
    if isinstance(metadata, dict) and metadata:
        snapshot["metadata"] = metadata
    return {key: value for key, value in snapshot.items() if value not in ("", None, {}, [])}


def _work_review_feedback_attribution(
    *,
    body: WorkReviewFeedbackCaptureRequest,
    session_id: str,
    agent_session: dict[str, Any] | None,
) -> dict[str, Any]:
    child_item_id = _clean_short_text(body.child_item_id or "", "", limit=180)
    proof_refs = _clean_event_list(body.proof_refs or [], limit=24)
    outcome_ref = _clean_short_text(body.outcome_ref, "", limit=300)
    outcome_summary = _body_excerpt(body.outcome_summary or "", limit=1000)
    attribution: dict[str, Any] = {
        "schema": KANBAN_REVIEW_FEEDBACK_ATTRIBUTION_SCHEMA,
        "session_id": session_id,
        "session_ref": f"kanban_agent_sessions:{session_id}",
    }
    session_snapshot = _work_review_feedback_session_snapshot(agent_session)
    if session_snapshot:
        attribution["agent_session"] = session_snapshot
    if child_item_id:
        attribution["child_item_id"] = child_item_id
        attribution["child_ref"] = f"xarta-kanban:item:{child_item_id}"
    if proof_refs:
        attribution["proof_refs"] = proof_refs
    if outcome_ref:
        attribution["outcome_ref"] = outcome_ref
    if outcome_summary:
        attribution["outcome_summary"] = outcome_summary
    return attribution


def _work_review_feedback_entry(
    *,
    item_id: str,
    body: WorkReviewFeedbackCaptureRequest,
    meta: dict[str, str],
    now: str,
    agent_session: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_feedback = _normalise_markdown_document_body(body.feedback).strip()
    if not clean_feedback:
        raise HTTPException(400, "Review feedback text is required")
    session_id = _clean_short_text(body.session_id, "", limit=180)
    if not session_id:
        raise HTTPException(400, "Review feedback session_id is required")
    feedback_id = (
        _clean_work_id(body.feedback_id, "kanban-feedback")
        if body.feedback_id
        else _kanban_review_feedback_id()
    )
    attribution = _work_review_feedback_attribution(
        body=body,
        session_id=session_id,
        agent_session=agent_session,
    )
    extra_refs = [attribution["session_ref"]]
    child_item_id = attribution.get("child_item_id")
    if child_item_id:
        extra_refs.extend([f"kanban_items:{child_item_id}", attribution["child_ref"]])
    extra_refs.extend(attribution.get("proof_refs") or [])
    if attribution.get("outcome_ref"):
        extra_refs.append(attribution["outcome_ref"])
    affected_refs = [
        f"kanban_items:{item_id}",
        f"xarta-kanban:item:{item_id}",
        *_clean_event_list(body.related_refs, limit=62),
        *extra_refs,
    ]
    seen_refs: set[str] = set()
    affected_refs = [
        ref for ref in affected_refs if ref and not (ref in seen_refs or seen_refs.add(ref))
    ]
    feedback_hash = _hash_json_payload(
        {
            "schema": KANBAN_REVIEW_FEEDBACK_SCHEMA,
            "item_id": item_id,
            "feedback": clean_feedback,
            "feedback_id": feedback_id,
            "captured_at": now,
            "attribution": attribution,
        }
    )
    return {
        "schema": KANBAN_REVIEW_FEEDBACK_SCHEMA,
        "feedback_id": feedback_id,
        "affected_item_id": item_id,
        "affected_refs": affected_refs,
        "captured_at": now,
        "actor": meta["actor"],
        "session_id": session_id,
        "capture_source": _clean_review_feedback_capture_source(body.capture_source),
        "source_ref": _clean_short_text(body.source_ref, "", limit=300),
        "source_surface": meta["source_surface"],
        "request_id": meta["request_id"],
        "run_id": meta["run_id"],
        "attribution": attribution,
        "feedback_hash": feedback_hash,
        "feedback_excerpt": _body_excerpt(clean_feedback, limit=500),
        "feedback": clean_feedback,
        "metadata": dict(body.metadata or {}),
    }


def _work_review_feedback_metadata(
    existing_metadata: dict[str, Any],
    entry: dict[str, Any],
) -> dict[str, Any]:
    existing_collection = existing_metadata.get("operator_feedback")
    existing_entries = (
        existing_collection.get("entries") if isinstance(existing_collection, dict) else []
    )
    entries = [
        dict(item)
        for item in existing_entries
        if isinstance(item, dict) and item.get("feedback_id") != entry["feedback_id"]
    ]
    entry_metadata = {key: value for key, value in entry.items() if key != "feedback"}
    entries.append(entry_metadata)
    return {
        "operator_feedback": {
            "schema": KANBAN_REVIEW_FEEDBACK_COLLECTION_SCHEMA,
            "updated_at": entry["captured_at"],
            "count": len(entries),
            "entries": entries,
        }
    }


def _work_review_feedback_markdown(entry: dict[str, Any]) -> str:
    attribution = entry.get("attribution") if isinstance(entry.get("attribution"), dict) else {}
    lines = [
        f"### {entry['captured_at']} - {entry['actor']}",
        "",
        f"- Feedback ID: `{entry['feedback_id']}`",
        f"- Affected item: `xarta-kanban:item:{entry['affected_item_id']}`",
        f"- Session: `{entry['session_id']}`",
        f"- Capture source: `{entry['capture_source']}`",
    ]
    if attribution.get("child_ref"):
        lines.append(f"- Child card: `{attribution['child_ref']}`")
    agent_session = attribution.get("agent_session")
    if isinstance(agent_session, dict) and agent_session.get("item_id"):
        lines.append(f"- Session item: `xarta-kanban:item:{agent_session['item_id']}`")
    if attribution.get("proof_refs"):
        proof_text = ", ".join(f"`{ref}`" for ref in attribution["proof_refs"])
        lines.append(f"- Proof refs: {proof_text}")
    if attribution.get("outcome_ref"):
        lines.append(f"- Outcome ref: `{attribution['outcome_ref']}`")
    if attribution.get("outcome_summary"):
        summary = _clean_short_text(attribution["outcome_summary"], "", limit=500)
        lines.append(f"- Outcome summary: {summary}")
    if entry.get("source_ref"):
        lines.append(f"- Source ref: `{entry['source_ref']}`")
    lines.extend(["", _review_feedback_quote_block(entry["feedback"])])
    return "\n".join(lines).strip()


def _append_work_review_feedback_body(existing_body: str, entry: dict[str, Any]) -> str:
    existing = _normalise_markdown_document_body(existing_body).rstrip()
    section_heading = "## Operator Feedback"
    entry_markdown = _work_review_feedback_markdown(entry)
    if not existing:
        return f"{section_heading}\n\n{entry_markdown}\n"
    if section_heading in existing:
        return f"{existing}\n\n{entry_markdown}\n"
    return f"{existing}\n\n{section_heading}\n\n{entry_markdown}\n"


def _preprocessing_marker_id(item_id: str) -> str:
    return _review_processor_marker_id(
        item_id,
        processor_kind="preprocessing",
        document_type="context_readiness",
    )


def _preprocessing_source_ref(
    *,
    name: str,
    document_id: str,
    document_type: str,
    updated_at: str,
    body: str,
    file_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "document_id": document_id,
        "document_type": document_type,
        "updated_at": _clean_short_text(updated_at, "", limit=80),
        "body_length": len(str(body or "")),
        "image_count": 0,
        "file_ref": dict(file_ref or {}),
    }


def _preprocessing_count(conn: Any, sql: str, args: tuple[Any, ...]) -> int:
    row = conn.execute(sql, args).fetchone()
    return int(row["count"] if row else 0)


def _work_preprocessing_counts_by_item(
    conn: Any,
    item_rows: list[Any],
) -> dict[str, dict[str, int]]:
    item_ids = [str(row["item_id"] or "") for row in item_rows if str(row["item_id"] or "")]
    counts = {
        item_id: {
            "blocker_count": 0,
            "child_count": 0,
            "commit_count": 0,
            "discussion_count": 0,
            "issue_count": 0,
            "link_count": 0,
            "open_descendant_count": 0,
            "open_leaf_descendant_count": 0,
            "todo_count": 0,
        }
        for item_id in item_ids
    }
    for row in item_rows:
        item_id = str(row["item_id"] or "")
        if not item_id:
            continue
        counts[item_id]["issue_count"] = len(_json_value(row["related_issue_ids_json"], []))
        counts[item_id]["todo_count"] = len(_json_value(row["related_task_ids_json"], []))
    if not item_ids:
        return counts

    placeholders = ",".join("?" for _ in item_ids)
    blocker_rows = conn.execute(
        f"""
        SELECT item_id, COUNT(*) AS count FROM kanban_blockers
        WHERE item_id IN ({placeholders})
          AND status NOT IN ('resolved', 'closed', 'done')
          AND COALESCE(json_extract(provenance_json, '$.schema'), '') != ?
        GROUP BY item_id
        """,
        [*item_ids, KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA],
    ).fetchall()
    for row in blocker_rows:
        counts[str(row["item_id"])]["blocker_count"] = int(row["count"] or 0)

    child_rows = conn.execute(
        f"""
        SELECT parent_item_id AS item_id, COUNT(*) AS count
        FROM kanban_items
        WHERE parent_item_id IN ({placeholders})
          AND status!='archived'
        GROUP BY parent_item_id
        """,
        item_ids,
    ).fetchall()
    for row in child_rows:
        counts[str(row["item_id"])]["child_count"] = int(row["count"] or 0)

    commit_rows = conn.execute(
        f"""
        SELECT item_id, COUNT(*) AS count
        FROM kanban_item_commits
        WHERE item_id IN ({placeholders})
        GROUP BY item_id
        """,
        item_ids,
    ).fetchall()
    for row in commit_rows:
        counts[str(row["item_id"])]["commit_count"] = int(row["count"] or 0)

    discussion_rows = conn.execute(
        f"""
        SELECT item_id, COUNT(*) AS count
        FROM kanban_discussions
        WHERE item_id IN ({placeholders})
        GROUP BY item_id
        """,
        item_ids,
    ).fetchall()
    for row in discussion_rows:
        counts[str(row["item_id"])]["discussion_count"] = int(row["count"] or 0)

    link_rows = conn.execute(
        f"""
        SELECT source_item_id, target_item_id
        FROM kanban_item_links
        WHERE source_item_id IN ({placeholders})
           OR target_item_id IN ({placeholders})
        """,
        [*item_ids, *item_ids],
    ).fetchall()
    item_id_set = set(item_ids)
    for row in link_rows:
        linked_ids = {
            str(row["source_item_id"] or ""),
            str(row["target_item_id"] or ""),
        }
        for item_id in linked_ids & item_id_set:
            counts[item_id]["link_count"] += 1
    return counts


def _work_preprocessing_ancestor_rows(
    conn: Any,
    item_row: Any,
    *,
    limit: int = KANBAN_DEPTH_LIMIT + 2,
) -> list[Any]:
    ancestors: list[Any] = []
    seen: set[str] = set()
    parent_item_id = _clean_short_text(item_row["parent_item_id"] or "", "", limit=180)
    while parent_item_id and parent_item_id not in seen and len(ancestors) < limit:
        seen.add(parent_item_id)
        parent_row = conn.execute(
            "SELECT * FROM kanban_items WHERE item_id=?",
            (parent_item_id,),
        ).fetchone()
        if parent_row is None:
            break
        ancestors.append(parent_row)
        parent_item_id = _clean_short_text(
            parent_row["parent_item_id"] or "",
            "",
            limit=180,
        )
    return ancestors


def _work_preprocessing_item_summary(row: Any) -> dict[str, Any]:
    return {
        "item_id": row["item_id"],
        "parent_item_id": row["parent_item_id"] or "",
        "title": row["title"],
        "item_type": row["item_type"],
        "state_id": row["state_id"],
        "status": row["status"],
        "priority_id": row["priority_id"],
        "depth": row["depth"],
        "tags": _json_value(row["tags_json"], []),
        "updated_at": row["updated_at"],
    }


def _work_preprocessing_ancestor_source_refs(
    conn: Any,
    item_row: Any,
    *,
    limit: int = KANBAN_DEPTH_LIMIT + 2,
    document_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    source_refs: list[dict[str, Any]] = []
    for index, ancestor in enumerate(
        _work_preprocessing_ancestor_rows(conn, item_row, limit=limit),
        start=1,
    ):
        prefix = "parent" if index == 1 else f"ancestor_{index}"
        ancestor_id = ancestor["item_id"]
        detail = _work_item_detail_document(conn, ancestor_id, cache=document_cache)
        review = _work_item_review_document(conn, ancestor_id, cache=document_cache)
        source_refs.extend(
            [
                _preprocessing_source_ref(
                    name=f"{prefix}_body",
                    document_id=ancestor_id,
                    document_type="ancestor-item-body",
                    updated_at=ancestor["updated_at"],
                    body=ancestor["body_excerpt"],
                ),
                _preprocessing_source_ref(
                    name=f"{prefix}_detail",
                    document_id=ancestor_id,
                    document_type="ancestor-item-detail",
                    updated_at=detail.get("updated_at") or ancestor["updated_at"],
                    body=detail.get("body") or "",
                    file_ref=detail.get("file_ref") if isinstance(detail, dict) else {},
                ),
                _preprocessing_source_ref(
                    name=f"{prefix}_review",
                    document_id=ancestor_id,
                    document_type="ancestor-item-review",
                    updated_at=review.get("updated_at") or ancestor["updated_at"],
                    body=review.get("body") or "",
                    file_ref=review.get("file_ref") if isinstance(review, dict) else {},
                ),
            ]
        )
    return source_refs


def _work_preprocessing_ancestor_context(
    conn: Any,
    item_row: Any,
    *,
    limit: int = KANBAN_DEPTH_LIMIT + 2,
    document_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ancestors: list[dict[str, Any]] = []
    for index, ancestor in enumerate(
        _work_preprocessing_ancestor_rows(conn, item_row, limit=limit),
        start=1,
    ):
        ancestor_id = ancestor["item_id"]
        detail = _work_item_detail_document(conn, ancestor_id, cache=document_cache)
        review = _work_item_review_document(conn, ancestor_id, cache=document_cache)
        decision_rows = conn.execute(
            """
            SELECT * FROM kanban_review_decisions
            WHERE item_id=?
            ORDER BY updated_at DESC, created_at DESC, decision_id
            LIMIT 5
            """,
            (ancestor_id,),
        ).fetchall()
        ancestors.append(
            {
                "relationship": "parent" if index == 1 else f"ancestor_{index}",
                "canonical_item_ref": f"xarta-kanban:item:{ancestor_id}",
                "item": _work_preprocessing_item_summary(ancestor),
                "documents": {
                    "body_excerpt": _body_excerpt(
                        str(ancestor["body_excerpt"] or ""),
                        limit=5000,
                    ),
                    "detail_excerpt": _body_excerpt(
                        str(detail.get("body") or ""),
                        limit=8000,
                    ),
                    "review_excerpt": _body_excerpt(
                        str(review.get("body") or ""),
                        limit=8000,
                    ),
                },
                "recent_decisions": [
                    {
                        "decision_id": row["decision_id"],
                        "processor_kind": row["processor_kind"],
                        "decision_type": row["decision_type"],
                        "title": row["title"],
                        "summary": row["summary"],
                        "status": row["status"],
                        "provider_mode": row["provider_mode"],
                        "proof_refs": _json_value(row["proof_refs_json"], []),
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                    }
                    for row in decision_rows
                ],
            }
        )
    return {
        "schema": "xarta.kanban.preprocessing.ancestor_context.v1",
        "item_id": item_row["item_id"],
        "ancestor_count": len(ancestors),
        "ancestors": ancestors,
    }


def _work_preprocessing_recent_decisions(
    conn: Any,
    *,
    item_id: str,
    current_source_hash: str,
    limit: int = 8,
) -> list[dict[str, Any]]:
    decision_rows = conn.execute(
        """
        SELECT * FROM kanban_review_decisions
        WHERE item_id=?
        ORDER BY updated_at DESC, created_at DESC, decision_id
        LIMIT ?
        """,
        (item_id, max(limit * 2, limit)),
    ).fetchall()
    decisions: list[dict[str, Any]] = []
    for row in decision_rows:
        metadata = _json_value(row["metadata_json"], {})
        decision_source_hash = str(metadata.get("document_source_hash") or "")
        stale_failed_preprocessing = bool(
            row["processor_kind"] == "preprocessing"
            and row["status"] == "failed"
            and decision_source_hash
            and decision_source_hash != current_source_hash
        )
        if stale_failed_preprocessing:
            continue
        decisions.append(
            {
                "decision_id": row["decision_id"],
                "processor_kind": row["processor_kind"],
                "decision_type": row["decision_type"],
                "title": row["title"],
                "summary": row["summary"],
                "status": row["status"],
                "provider_mode": row["provider_mode"],
                "proof_refs": _json_value(row["proof_refs_json"], []),
                "commit_link_ids": _json_value(row["commit_link_ids_json"], []),
                "created_at": row["created_at"],
            }
        )
        if len(decisions) >= limit:
            break
    return decisions


def _work_preprocessing_source_classification(
    conn: Any,
    item_row: Any,
    *,
    detail: dict[str, Any],
    review: dict[str, Any],
) -> dict[str, Any]:
    discussion_rows = conn.execute(
        """
        SELECT body_excerpt FROM kanban_discussions
        WHERE item_id=? AND status!='archived'
        ORDER BY updated_at DESC, created_at DESC, discussion_id
        LIMIT 6
        """,
        (item_row["item_id"],),
    ).fetchall()
    context_text = "\n".join(
        [
            str(item_row["body_excerpt"] or ""),
            str(detail.get("body") or ""),
            str(review.get("body") or ""),
            *[str(row["body_excerpt"] or "") for row in discussion_rows],
        ]
    ).lower()
    title_text = str(item_row["title"] or "").lower()
    topic_terms = (
        "topic ancestor",
        "holding area",
        "holding-area",
        "category card",
        "umbrella",
        "organize related",
        "collect related",
        "topic/container",
        "broad topic",
    )
    concrete_terms = (
        "proof path",
        "acceptance",
        "implement",
        "fix ",
        "add ",
        "test ",
        "api",
        "ui",
        "commit",
        "deliverable",
        "regression",
        "bug",
    )
    topic_hits = [term for term in topic_terms if term in context_text]
    concrete_hits = [term for term in concrete_terms if term in context_text]
    title_topic_hint = any(term in title_text for term in ("topic", "ancestor", "holding"))
    classification = "concrete_request"
    eligible = True
    reason = "concrete_context"
    if topic_hits and not concrete_hits:
        classification = "topic_container"
        eligible = False
        reason = "topic_context_without_concrete_proof"
    elif topic_hits and concrete_hits:
        classification = "ambiguous_mixed"
        eligible = False
        reason = "mixed_topic_and_concrete_context"
    elif title_topic_hint and not concrete_hits and item_row["state_id"] != "todo":
        classification = "topic_container"
        eligible = False
        reason = "title_topic_hint_with_non_todo_lane_and_no_concrete_context"
    elif item_row["state_id"] == "backlog" and not concrete_hits:
        classification = "topic_container"
        eligible = False
        reason = "backlog_without_concrete_context"
    return {
        "schema": "xarta.kanban.preprocessing.source_classification.v1",
        "classification": classification,
        "eligible": eligible,
        "reason": reason,
        "evidence": {
            "topic_context_terms": topic_hits[:8],
            "concrete_context_terms": concrete_hits[:8],
            "title_topic_hint": title_topic_hint,
            "state_id": item_row["state_id"],
        },
    }


def _work_preprocessing_context_source(
    conn: Any,
    item_row: Any,
    *,
    counts: dict[str, int] | None = None,
    document_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    item_id = item_row["item_id"]
    detail = _work_item_detail_document(conn, item_id, cache=document_cache)
    review = _work_item_review_document(conn, item_id, cache=document_cache)
    hints_row = conn.execute(
        "SELECT * FROM kanban_agent_hints WHERE item_id=?",
        (item_id,),
    ).fetchone()
    hints = _row_to_work_agent_hints(hints_row, item_id)
    marker = hints["metadata"].get("context_readiness_marker")
    if not isinstance(marker, dict):
        marker = {}
    source_refs = [
        _preprocessing_source_ref(
            name="body",
            document_id=item_id,
            document_type="item-body",
            updated_at=item_row["updated_at"],
            body=item_row["body_excerpt"],
        ),
        _preprocessing_source_ref(
            name="detail",
            document_id=item_id,
            document_type="item-detail",
            updated_at=detail.get("updated_at") or item_row["updated_at"],
            body=detail.get("body") or "",
            file_ref=detail.get("file_ref") if isinstance(detail, dict) else {},
        ),
        _preprocessing_source_ref(
            name="review",
            document_id=item_id,
            document_type="item-review",
            updated_at=review.get("updated_at") or item_row["updated_at"],
            body=review.get("body") or "",
            file_ref=review.get("file_ref") if isinstance(review, dict) else {},
        ),
    ]
    source_refs.extend(
        _work_preprocessing_ancestor_source_refs(
            conn,
            item_row,
            document_cache=document_cache,
        )
    )
    source_counts = (
        counts
        if counts is not None
        else {
            "blocker_count": _preprocessing_count(
                conn,
                """
            SELECT COUNT(*) AS count FROM kanban_blockers
            WHERE item_id=?
              AND status NOT IN ('resolved', 'closed', 'done')
              AND COALESCE(json_extract(provenance_json, '$.schema'), '') != ?
            """,
                (item_id, KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA),
            ),
            "child_count": _preprocessing_count(
                conn,
                "SELECT COUNT(*) AS count FROM kanban_items WHERE parent_item_id=? AND status!='archived'",
                (item_id,),
            ),
            "commit_count": _preprocessing_count(
                conn,
                "SELECT COUNT(*) AS count FROM kanban_item_commits WHERE item_id=?",
                (item_id,),
            ),
            "discussion_count": _preprocessing_count(
                conn,
                "SELECT COUNT(*) AS count FROM kanban_discussions WHERE item_id=?",
                (item_id,),
            ),
            "issue_count": len(_json_value(item_row["related_issue_ids_json"], [])),
            "link_count": _preprocessing_count(
                conn,
                """
            SELECT COUNT(*) AS count FROM kanban_item_links
            WHERE source_item_id=? OR target_item_id=?
            """,
                (item_id, item_id),
            ),
            "open_descendant_count": 0,
            "open_leaf_descendant_count": 0,
            "todo_count": len(_json_value(item_row["related_task_ids_json"], [])),
        }
    )
    marker_refs = {
        str(ref.get("name") or ""): ref
        for ref in marker.get("source_refs", [])
        if isinstance(ref, dict) and str(ref.get("name") or "")
    }
    changed_refs = []
    new_refs = []
    missing_refs = []
    for source_ref in source_refs:
        name = source_ref["name"]
        marker_ref = marker_refs.get(name)
        if marker_ref is None:
            new_refs.append(name)
            continue
        if (
            str(marker_ref.get("updated_at") or "") != source_ref["updated_at"]
            or int(marker_ref.get("body_length") or 0) != source_ref["body_length"]
            or int(marker_ref.get("image_count") or 0) != source_ref["image_count"]
        ):
            changed_refs.append(name)
    source_ref_names = {ref["name"] for ref in source_refs}
    missing_refs = [name for name in marker_refs if name not in source_ref_names]
    marker_counts = marker.get("counts") if isinstance(marker.get("counts"), dict) else {}
    count_drift = [
        key for key, value in source_counts.items() if int(marker_counts.get(key) or 0) != value
    ]
    marker_schema = str(marker.get("schema") or "")
    marker_item_id = str(marker.get("item_id") or "")
    if not marker:
        reason = "missing_readiness_marker"
    elif marker_schema != "xarta.kanban.context_readiness_marker.v1":
        reason = "invalid_readiness_marker_schema"
    elif marker_item_id != item_id:
        reason = "readiness_marker_item_mismatch"
    elif changed_refs or new_refs or missing_refs or count_drift:
        reason = "readiness_marker_stale"
    else:
        reason = "ready"
    classification = _work_preprocessing_source_classification(
        conn,
        item_row,
        detail=detail,
        review=review,
    )
    if not classification["eligible"] and reason != "ready":
        reason = f"preprocessing_skipped_{classification['classification']}"
    source_payload = {
        "schema": "xarta.kanban.preprocessing.queue_source.v1",
        "item_id": item_id,
        "item_updated_at": item_row["updated_at"],
        "state_id": item_row["state_id"],
        "status": item_row["status"],
        "source_refs": source_refs,
        "counts": source_counts,
        "marker": {
            "exists": bool(marker),
            "schema": marker_schema,
            "item_id": marker_item_id,
            "marked_at": marker.get("marked_at") or "",
            "context_hash": marker.get("context_hash") or "",
        },
        "drift": {
            "changed_source_refs": changed_refs,
            "new_source_refs": new_refs,
            "missing_source_refs": missing_refs,
            "count_drift": count_drift,
        },
        "classification": classification,
        "reason": reason,
    }
    source_hash = _hash_json_payload(source_payload)
    return {
        **source_payload,
        "document_ref": f"kanban_items:{item_id}:context_readiness",
        "document_updated_at": max(ref["updated_at"] for ref in source_refs),
        "document_source_hash": source_hash,
        "ready": reason == "ready",
        "needs_preprocessing": reason != "ready" and bool(classification.get("eligible", True)),
    }


def _work_preprocessing_marker_row(
    *,
    existing: Any | None,
    item_id: str,
    source: dict[str, Any],
    meta: dict[str, str],
    now: str,
    reason: str,
    scan_metadata: dict[str, Any],
) -> dict[str, Any]:
    provider_mode = _work_review_processing_policy()["active_mode"]
    marker_id = _preprocessing_marker_id(item_id)
    previous_metadata = _json_value(existing["metadata_json"], {}) if existing is not None else {}
    metadata = {
        **previous_metadata,
        **scan_metadata,
        "reason": reason,
        "readiness_reason": source["reason"],
        "previous_status": existing["status"] if existing is not None else "",
        "previous_document_source_hash": (
            existing["document_source_hash"] if existing is not None else ""
        ),
        "source_refs": source["source_refs"],
        "counts": source["counts"],
        "drift": source["drift"],
    }
    requeued_processing_change = bool(existing is not None and existing["status"] == "processing")
    if requeued_processing_change:
        metadata["superseded_processing_attempt"] = True
    last_successful_source_hash = _work_marker_successful_source_hash(existing)
    processed_document_updated_at = ""
    processed_at = ""
    if (
        existing is not None
        and last_successful_source_hash
        and _row_value(existing, "processed_source_hash", "") == last_successful_source_hash
    ):
        processed_document_updated_at = existing["processed_document_updated_at"]
        processed_at = existing["processed_at"]
    row = {
        "marker_id": marker_id,
        "item_id": item_id,
        "processor_kind": "preprocessing",
        "document_type": "context_readiness",
        "document_ref": source["document_ref"],
        "document_updated_at": source["document_updated_at"],
        "document_source_hash": source["document_source_hash"],
        "processed_document_updated_at": processed_document_updated_at,
        "processed_source_hash": last_successful_source_hash,
        "processed_at": processed_at,
        "queued_at": now,
        "last_seen_at": now,
        "processing_started_at": "",
        "processing_expires_at": "",
        "attempt_count": int(existing["attempt_count"] or 0) if existing is not None else 0,
        "last_error": "preprocessing_changed_during_processing"
        if requeued_processing_change
        else "",
        "next_retry_at": "",
        "retry_after_seconds": 0,
        "retry_attempt_count": 0,
        "last_successful_source_hash": last_successful_source_hash,
        "last_failure_event_id": "",
        "last_failure_source_hash": "",
        "last_error_class": "",
        "retry_policy_version": "",
        "superseded_at": (
            now
            if requeued_processing_change
            else (existing["superseded_at"] if existing is not None else "")
        ),
        "superseded_by_source_hash": (
            source["document_source_hash"]
            if requeued_processing_change
            else (existing["superseded_by_source_hash"] if existing is not None else "")
        ),
        "status": "queued",
        "provider_mode": provider_mode,
        "decision_id": existing["decision_id"] if existing is not None else "",
        "source_hash": "",
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
        "provenance_json": json.dumps(
            {
                "schema": KANBAN_REVIEW_MARKER_SCHEMA,
                "recorded_by": meta["actor"],
                "source_surface": meta["source_surface"],
                "request_id": meta["request_id"],
                "run_id": meta["run_id"],
            },
            ensure_ascii=True,
            sort_keys=True,
        ),
        "created_at": existing["created_at"] if existing is not None else now,
        "updated_at": now,
    }
    row["source_hash"] = _hash_json_payload(
        {
            key: value
            for key, value in row.items()
            if key not in {"source_hash", "created_at", "updated_at"}
        }
    )
    return row


def _work_preprocessing_active_rows(conn: Any, scope_ids: list[str]) -> list[Any]:
    args: list[Any] = ["preprocessing"]
    where = (
        "WHERE marker.processor_kind=? AND marker.status='processing' "
        f"AND {_work_preprocessing_candidate_predicate('item')}"
    )
    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        where += f" AND marker.item_id IN ({placeholders})"
        args.extend(scope_ids)
    return conn.execute(
        f"""
        SELECT marker.* FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where}
        ORDER BY marker.processing_started_at ASC, marker.queued_at ASC, marker.marker_id
        """,
        args,
    ).fetchall()


def _work_item_automation_included_predicate(item_alias: str = "item") -> str:
    safe_alias = "".join(ch for ch in str(item_alias or "item") if ch.isalnum() or ch == "_")
    if not safe_alias:
        safe_alias = "item"
    return f"""
    NOT EXISTS (
        WITH RECURSIVE ancestors(item_id, parent_item_id, automation_excluded) AS (
            SELECT {safe_alias}.item_id,
                   {safe_alias}.parent_item_id,
                   COALESCE({safe_alias}.automation_excluded, 0)
            UNION ALL
            SELECT parent.item_id,
                   parent.parent_item_id,
                   COALESCE(parent.automation_excluded, 0)
            FROM kanban_items parent
            JOIN ancestors ON parent.item_id = ancestors.parent_item_id
        )
        SELECT 1 FROM ancestors WHERE COALESCE(automation_excluded, 0) != 0
    )
    """


def _work_preprocessing_candidate_predicate(
    item_alias: str = "item",
    *,
    include_automation: bool = True,
) -> str:
    safe_alias = "".join(ch for ch in str(item_alias or "item") if ch.isalnum() or ch == "_")
    if not safe_alias:
        safe_alias = "item"
    predicates = [
        f"COALESCE({safe_alias}.item_type, '')='item'",
        f"{safe_alias}.status NOT IN ('archived', 'done')",
        f"{safe_alias}.state_id='todo'",
        f"""
        NOT EXISTS (
            SELECT 1 FROM kanban_items child
            WHERE child.parent_item_id={safe_alias}.item_id
              AND child.status != 'archived'
        )
        """,
    ]
    if include_automation:
        predicates.append(_work_item_automation_included_predicate(safe_alias))
    return "(" + " AND ".join(f"({predicate})" for predicate in predicates) + ")"


def _work_processor_marker_claimable_predicate(
    marker_alias: str = "marker",
    item_alias: str = "item",
) -> str:
    safe_marker_alias = "".join(
        ch for ch in str(marker_alias or "marker") if ch.isalnum() or ch == "_"
    )
    if not safe_marker_alias:
        safe_marker_alias = "marker"
    return f"""
    ({_work_item_automation_included_predicate(item_alias)})
    AND (
        {safe_marker_alias}.processor_kind != 'preprocessing'
        OR {_work_preprocessing_candidate_predicate(item_alias, include_automation=False)}
    )
    """


def _work_item_automation_excluded(conn: Any, item_id: str | None) -> bool:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    if not clean_item_id:
        return False
    row = conn.execute(
        """
        WITH RECURSIVE ancestors(item_id, parent_item_id, automation_excluded) AS (
            SELECT item_id, parent_item_id, COALESCE(automation_excluded, 0)
            FROM kanban_items
            WHERE item_id=?
            UNION ALL
            SELECT parent.item_id,
                   parent.parent_item_id,
                   COALESCE(parent.automation_excluded, 0)
            FROM kanban_items parent
            JOIN ancestors ON parent.item_id = ancestors.parent_item_id
        )
        SELECT 1 FROM ancestors WHERE COALESCE(automation_excluded, 0) != 0 LIMIT 1
        """,
        (clean_item_id,),
    ).fetchone()
    return row is not None


def _raise_work_automation_excluded(
    *,
    operation: str,
    item_id: str,
    meta: dict[str, str],
) -> None:
    raise HTTPException(
        409,
        {
            "error": "kanban_automation_excluded_branch",
            "message": "Kanban automation idle worker may not mutate an excluded branch.",
            "operation": operation,
            "item_id": item_id,
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
        },
    )


def _work_marker_successful_source_hash(existing: Any | None) -> str:
    if existing is None:
        return ""
    successful_source_hash = _clean_short_text(
        _row_value(existing, "last_successful_source_hash", ""),
        "",
        limit=120,
    )
    if successful_source_hash:
        return successful_source_hash
    if _row_value(existing, "status", "") == "processed":
        return _clean_short_text(
            _row_value(existing, "processed_source_hash", ""),
            "",
            limit=120,
        )
    return ""


def _work_queued_processor_marker_ids(conn: Any, scope_ids: list[str]) -> list[str]:
    now = _utc_now_iso()
    args: list[Any] = [now]
    where = (
        f"WHERE {_work_processor_marker_claimable_state_predicate()} "
        f"AND {_work_processor_marker_claimable_predicate()}"
    )
    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        where += f" AND marker.item_id IN ({placeholders})"
        args.extend(scope_ids)
    rows = conn.execute(
        f"""
        SELECT marker.marker_id FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where}
        ORDER BY
          CASE marker.processor_kind
            WHEN 'review' THEN 0
            WHEN 'preprocessing' THEN 1
            ELSE 2
          END,
          marker.queued_at ASC,
          marker.document_updated_at ASC,
          marker.marker_id
        """,
        args,
    ).fetchall()
    return [row["marker_id"] for row in rows]


def _row_to_work_review_processor_marker(row: Any) -> dict[str, Any]:
    retry_state = _work_review_marker_retry_state(row)
    retry_active = retry_state in {"retry_waiting", "retry_due"}
    next_retry_at = _row_value(row, "next_retry_at", "") if retry_active else ""
    return {
        "schema": KANBAN_REVIEW_MARKER_SCHEMA,
        "marker_id": row["marker_id"],
        "item_id": row["item_id"],
        "processor_kind": row["processor_kind"],
        "document_type": row["document_type"],
        "document_ref": row["document_ref"],
        "document_updated_at": row["document_updated_at"],
        "document_source_hash": row["document_source_hash"],
        "processed_document_updated_at": row["processed_document_updated_at"],
        "processed_source_hash": row["processed_source_hash"],
        "processed_at": row["processed_at"],
        "queued_at": row["queued_at"],
        "last_seen_at": row["last_seen_at"],
        "processing_started_at": row["processing_started_at"],
        "processing_expires_at": row["processing_expires_at"],
        "attempt_count": int(row["attempt_count"] or 0),
        "last_error": row["last_error"],
        "next_retry_at": next_retry_at,
        "retry_after_seconds": (
            int(_row_value(row, "retry_after_seconds", 0) or 0) if retry_active else 0
        ),
        "retry_attempt_count": int(_row_value(row, "retry_attempt_count", 0) or 0),
        "last_successful_source_hash": _row_value(row, "last_successful_source_hash", ""),
        "last_failure_event_id": _row_value(row, "last_failure_event_id", ""),
        "last_failure_source_hash": _row_value(row, "last_failure_source_hash", ""),
        "last_error_class": _row_value(row, "last_error_class", ""),
        "retry_policy_version": _row_value(row, "retry_policy_version", ""),
        "retry_state": retry_state,
        "retry_waiting": retry_state == "retry_waiting",
        "superseded_at": row["superseded_at"],
        "superseded_by_source_hash": row["superseded_by_source_hash"],
        "status": row["status"],
        "provider_mode": row["provider_mode"],
        "decision_id": row["decision_id"],
        "source_hash": row["source_hash"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _work_review_processor_marker_row(
    *,
    existing: Any | None,
    item_id: str,
    document: dict[str, Any],
    document_source: dict[str, Any],
    meta: dict[str, str],
    now: str,
    reason: str,
    scan_metadata: dict[str, Any],
) -> dict[str, Any]:
    provider_mode = _work_review_processing_policy()["active_mode"]
    marker_id = _review_processor_marker_id(item_id)
    previous_metadata = _json_value(existing["metadata_json"], {}) if existing is not None else {}
    metadata = {
        **previous_metadata,
        **scan_metadata,
        "reason": reason,
        "document_exists": document_source["document_exists"],
        "body_bytes": document_source["body_bytes"],
        "previous_status": existing["status"] if existing is not None else "",
        "previous_document_source_hash": (
            existing["document_source_hash"] if existing is not None else ""
        ),
    }
    requeued_processing_change = bool(existing is not None and existing["status"] == "processing")
    if requeued_processing_change:
        metadata["superseded_processing_attempt"] = True
    last_successful_source_hash = _work_marker_successful_source_hash(existing)
    processed_document_updated_at = ""
    processed_at = ""
    if (
        existing is not None
        and last_successful_source_hash
        and _row_value(existing, "processed_source_hash", "") == last_successful_source_hash
    ):
        processed_document_updated_at = existing["processed_document_updated_at"]
        processed_at = existing["processed_at"]
    provenance = {
        "schema": KANBAN_REVIEW_MARKER_SCHEMA,
        "recorded_by": meta["actor"],
        "source_surface": meta["source_surface"],
        "request_id": meta["request_id"],
        "run_id": meta["run_id"],
    }
    row = {
        "marker_id": marker_id,
        "item_id": item_id,
        "processor_kind": "review",
        "document_type": "review",
        "document_ref": document_source["document_ref"],
        "document_updated_at": document_source["document_updated_at"],
        "document_source_hash": document_source["document_source_hash"],
        "processed_document_updated_at": processed_document_updated_at,
        "processed_source_hash": last_successful_source_hash,
        "processed_at": processed_at,
        "queued_at": now,
        "last_seen_at": now,
        "processing_started_at": "",
        "processing_expires_at": "",
        "attempt_count": int(existing["attempt_count"] or 0) if existing is not None else 0,
        "last_error": "review_changed_during_processing" if requeued_processing_change else "",
        "next_retry_at": "",
        "retry_after_seconds": 0,
        "retry_attempt_count": 0,
        "last_successful_source_hash": last_successful_source_hash,
        "last_failure_event_id": "",
        "last_failure_source_hash": "",
        "last_error_class": "",
        "retry_policy_version": "",
        "superseded_at": (
            now
            if requeued_processing_change
            else (existing["superseded_at"] if existing is not None else "")
        ),
        "superseded_by_source_hash": (
            document_source["document_source_hash"]
            if requeued_processing_change
            else (existing["superseded_by_source_hash"] if existing is not None else "")
        ),
        "status": "queued",
        "provider_mode": provider_mode,
        "decision_id": existing["decision_id"] if existing is not None else "",
        "source_hash": "",
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
        "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
        "created_at": existing["created_at"] if existing is not None else now,
        "updated_at": now,
    }
    row["source_hash"] = _hash_json_payload(
        {
            key: value
            for key, value in row.items()
            if key not in {"source_hash", "created_at", "updated_at"}
        }
    )
    return row


def _write_work_review_processor_marker(conn: Any, row: dict[str, Any]) -> Any:
    return _kanban_write_store(conn).upsert_review_processor_marker_row(row)


def _row_to_work_review_failure_event(row: Any) -> dict[str, Any]:
    return {
        "schema": KANBAN_REVIEW_FAILURE_EVENT_SCHEMA,
        "failure_event_id": row["failure_event_id"],
        "marker_id": row["marker_id"],
        "item_id": row["item_id"],
        "processor_kind": row["processor_kind"],
        "document_type": row["document_type"],
        "source_hash": row["source_hash"],
        "error_class": row["error_class"],
        "error_message": row["error_message"],
        "provider_mode": row["provider_mode"],
        "model_alias": row["model_alias"],
        "attempt_number": int(row["attempt_number"] or 0),
        "failed_at": row["failed_at"],
        "next_retry_at": row["next_retry_at"],
        "retry_after_seconds": int(row["retry_after_seconds"] or 0),
        "retry_policy_version": row["retry_policy_version"],
        "retryable": bool(row["retryable"]),
        "status": row["status"],
        "event_hash": row["event_hash"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _work_review_failure_event_row(
    marker_row: Any,
    *,
    meta: dict[str, str],
    now: str,
    error_class: str,
    error_message: str,
    provider_mode: str,
    model_alias: str,
    attempt_number: int,
    next_retry_at: str,
    retry_after_seconds: int,
    retryable: bool,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    source_hash = _clean_short_text(marker_row["document_source_hash"], "", limit=120)
    failure_event_id = _work_review_failure_event_id(
        marker_row["marker_id"],
        source_hash,
        attempt_number,
        now,
    )
    row = {
        "failure_event_id": failure_event_id,
        "marker_id": marker_row["marker_id"],
        "item_id": marker_row["item_id"],
        "processor_kind": marker_row["processor_kind"],
        "document_type": marker_row["document_type"],
        "source_hash": source_hash,
        "error_class": error_class,
        "error_message": error_message,
        "provider_mode": provider_mode,
        "model_alias": model_alias,
        "attempt_number": attempt_number,
        "failed_at": now,
        "next_retry_at": next_retry_at,
        "retry_after_seconds": retry_after_seconds,
        "retry_policy_version": KANBAN_REVIEW_RETRY_POLICY_VERSION,
        "retryable": 1 if retryable else 0,
        "status": "retry_waiting" if retryable else "terminal",
        "event_hash": "",
        "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
        "provenance_json": json.dumps(
            {
                "schema": KANBAN_REVIEW_FAILURE_EVENT_SCHEMA,
                "recorded_by": meta["actor"],
                "source_surface": meta["source_surface"],
                "request_id": meta["request_id"],
                "run_id": meta["run_id"],
            },
            ensure_ascii=True,
            sort_keys=True,
        ),
        "created_at": now,
        "updated_at": now,
    }
    row["event_hash"] = _hash_json_payload(
        {
            key: value
            for key, value in row.items()
            if key not in {"event_hash", "created_at", "updated_at"}
        }
    )
    return row


def _write_work_review_failure_event(conn: Any, row: dict[str, Any]) -> Any:
    return _kanban_write_store(conn).upsert_review_failure_event_row(row)


def _schedule_work_review_processor_marker_for_document(
    conn: Any,
    *,
    item_id: str,
    document: dict[str, Any],
    meta: dict[str, str],
    now: str,
    reason: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    document_source = _review_document_source(document)
    marker_id = _review_processor_marker_id(item_id)
    existing = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
        (marker_id,),
    ).fetchone()
    if not document_source["has_review_text"]:
        return {
            "action": "skipped_empty",
            "queued": False,
            "marker_row": existing,
            "document_source": document_source,
        }
    if existing is not None:
        same_document = existing["document_source_hash"] == document_source["document_source_hash"]
        already_processed = (
            _work_marker_successful_source_hash(existing) == document_source["document_source_hash"]
        )
        if same_document and existing["status"] in {"queued", "processing"}:
            return {
                "action": "unchanged_pending",
                "queued": False,
                "marker_row": existing,
                "document_source": document_source,
            }
        if same_document and existing["status"] in {"failed", "skipped", "cancelled"}:
            return {
                "action": "unchanged_failed",
                "queued": False,
                "marker_row": existing,
                "document_source": document_source,
            }
        if already_processed and existing["status"] == "processed":
            return {
                "action": "unchanged_current",
                "queued": False,
                "marker_row": existing,
                "document_source": document_source,
            }
    marker_row = _work_review_processor_marker_row(
        existing=existing,
        item_id=item_id,
        document=document,
        document_source=document_source,
        meta=meta,
        now=now,
        reason=reason,
        scan_metadata=metadata,
    )
    saved_row = _write_work_review_processor_marker(conn, marker_row)
    return {
        "action": "queued",
        "queued": True,
        "marker_row": saved_row,
        "document_source": document_source,
    }


def _row_to_work_review_processor_schedule(schedule: dict[str, Any]) -> dict[str, Any]:
    marker_row = schedule.get("marker_row")
    return {
        "schema": KANBAN_REVIEW_SCHEDULER_SCHEMA,
        "action": schedule.get("action") or "",
        "queued": bool(schedule.get("queued")),
        "document_source_hash": (schedule.get("document_source") or {}).get(
            "document_source_hash", ""
        ),
        "marker": (
            _row_to_work_review_processor_marker(marker_row) if marker_row is not None else None
        ),
    }


def _work_review_failure_event_payload(row: Any) -> dict[str, Any]:
    marker_status = _row_value(row, "marker_status", "") or row["status"]
    raw_marker_next_retry_at = _row_value(row, "marker_next_retry_at", "")
    retry_state = _work_review_marker_retry_state(
        {
            "status": marker_status,
            "next_retry_at": raw_marker_next_retry_at,
        }
    )
    marker_next_retry_at = (
        raw_marker_next_retry_at if retry_state in {"retry_waiting", "retry_due"} else ""
    )
    payload = _row_to_work_review_failure_event(row)
    item_id = payload["item_id"]
    payload.update(
        {
            "item_title": _row_value(row, "item_title", ""),
            "item_ref": f"xarta-kanban:item:{item_id}" if item_id else "",
            "marker_status": marker_status,
            "marker_next_retry_at": marker_next_retry_at,
            "raw_marker_next_retry_at": raw_marker_next_retry_at,
            "scheduled_retry_at": payload["next_retry_at"],
            "retry_state": retry_state,
            "retry_waiting": retry_state == "retry_waiting",
            "terminal": retry_state == "terminal",
        }
    )
    return payload


def _work_review_processor_failure_stats(
    conn: Any,
    scope_ids: list[str],
    *,
    limit: int = 10,
    processor_kind: str | None = None,
) -> dict[str, Any]:
    where = "WHERE 1=1"
    args: list[Any] = []
    clean_processor_kind = _clean_short_text(processor_kind, "", limit=80)
    if clean_processor_kind:
        where += " AND event.processor_kind=?"
        args.append(clean_processor_kind)
    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        where += f" AND event.item_id IN ({placeholders})"
        args.extend(scope_ids)
    event_rows = conn.execute(
        f"""
        SELECT event.*,
               item.title AS item_title,
               marker.status AS marker_status,
               marker.next_retry_at AS marker_next_retry_at,
               marker.retry_attempt_count AS marker_retry_attempt_count,
               marker.last_error AS marker_last_error
        FROM kanban_review_processor_failure_events event
        LEFT JOIN kanban_items item ON item.item_id=event.item_id
        LEFT JOIN kanban_review_processor_markers marker ON marker.marker_id=event.marker_id
        {where}
        ORDER BY event.failed_at DESC, event.attempt_number DESC, event.failure_event_id DESC
        LIMIT ?
        """,
        [*args, limit],
    ).fetchall()
    aggregate_rows = conn.execute(
        f"""
        SELECT event.*,
               item.title AS item_title,
               marker.status AS marker_status,
               marker.next_retry_at AS marker_next_retry_at,
               marker.retry_attempt_count AS marker_retry_attempt_count,
               marker.last_error AS marker_last_error
        FROM kanban_review_processor_failure_events event
        LEFT JOIN kanban_items item ON item.item_id=event.item_id
        LEFT JOIN kanban_review_processor_markers marker ON marker.marker_id=event.marker_id
        {where}
        ORDER BY event.failed_at DESC, event.attempt_number DESC, event.failure_event_id DESC
        LIMIT ?
        """,
        [*args, max(100, limit * 20)],
    ).fetchall()
    aggregates: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in aggregate_rows:
        key = (
            row["item_id"],
            row["processor_kind"],
            row["source_hash"],
            row["error_class"],
        )
        payload = _work_review_failure_event_payload(row)
        active_next_retry_at = (
            payload["marker_next_retry_at"]
            if payload["retry_state"] in {"retry_waiting", "retry_due"}
            else ""
        )
        existing = aggregates.get(key)
        failed_at = payload["failed_at"]
        if existing is None:
            existing = {
                "schema": "xarta.kanban.review_processor.failure_aggregate.v1",
                "item_id": payload["item_id"],
                "item_title": payload["item_title"],
                "item_ref": payload["item_ref"],
                "marker_id": payload["marker_id"],
                "processor_kind": payload["processor_kind"],
                "source_hash": payload["source_hash"],
                "error_class": payload["error_class"],
                "attempt_count": 0,
                "event_count": 0,
                "first_failed_at": failed_at,
                "last_failed_at": failed_at,
                "last_error": payload["error_message"],
                "provider_mode": payload["provider_mode"],
                "model_alias": payload["model_alias"],
                "next_retry_at": active_next_retry_at,
                "scheduled_retry_at": payload["next_retry_at"],
                "retry_after_seconds": payload["retry_after_seconds"],
                "retry_policy_version": payload["retry_policy_version"],
                "marker_status": payload["marker_status"],
                "retry_state": payload["retry_state"],
                "retry_waiting": payload["retry_waiting"],
                "terminal": payload["terminal"],
            }
            aggregates[key] = existing
        existing["event_count"] += 1
        existing["attempt_count"] = max(
            int(existing["attempt_count"] or 0),
            int(payload["attempt_number"] or 0),
            int(existing["event_count"] or 0),
        )
        if failed_at < existing["first_failed_at"]:
            existing["first_failed_at"] = failed_at
        if failed_at > existing["last_failed_at"] or (
            failed_at == existing["last_failed_at"]
            and int(payload["attempt_number"] or 0) >= int(existing["attempt_count"] or 0)
        ):
            existing.update(
                {
                    "last_failed_at": failed_at,
                    "last_error": payload["error_message"],
                    "provider_mode": payload["provider_mode"],
                    "model_alias": payload["model_alias"],
                    "next_retry_at": active_next_retry_at,
                    "scheduled_retry_at": payload["next_retry_at"],
                    "retry_after_seconds": payload["retry_after_seconds"],
                    "marker_status": payload["marker_status"],
                    "retry_state": payload["retry_state"],
                    "retry_waiting": payload["retry_waiting"],
                    "terminal": payload["terminal"],
                }
            )
    all_aggregate_payloads = sorted(
        aggregates.values(),
        key=lambda entry: (entry["last_failed_at"], entry["marker_id"]),
        reverse=True,
    )
    aggregate_payloads = all_aggregate_payloads[:limit]
    active_aggregate_payloads = [
        entry
        for entry in all_aggregate_payloads
        if entry.get("retry_state") in {"retry_waiting", "retry_due"}
        or entry.get("marker_status") == "failed"
    ]
    waiting_retry_times = sorted(
        str(entry.get("next_retry_at") or "")
        for entry in all_aggregate_payloads
        if entry.get("retry_waiting") and entry.get("next_retry_at")
    )
    return {
        "schema": KANBAN_REVIEW_FAILURE_EVENT_SCHEMA,
        "processor_kind": clean_processor_kind or "all",
        "event_count": len(aggregate_rows),
        "repeated_failure_count": sum(
            1 for entry in all_aggregate_payloads if int(entry["attempt_count"] or 0) > 1
        ),
        "active_failure_count": len(active_aggregate_payloads),
        "retry_waiting_count": sum(1 for entry in all_aggregate_payloads if entry["retry_waiting"]),
        "retry_due_count": sum(
            1 for entry in all_aggregate_payloads if entry["retry_state"] == "retry_due"
        ),
        "terminal_count": sum(1 for entry in all_aggregate_payloads if entry["terminal"]),
        "last_error": (
            active_aggregate_payloads[0]["last_error"] if active_aggregate_payloads else ""
        ),
        "historical_last_error": aggregate_payloads[0]["last_error"] if aggregate_payloads else "",
        "next_retry_at": waiting_retry_times[0] if waiting_retry_times else "",
        "recent_events": [_work_review_failure_event_payload(row) for row in event_rows],
        "aggregates": aggregate_payloads,
    }


def _work_review_processor_marker_stats(
    conn: Any,
    scope_ids: list[str],
    *,
    limit: int = 10,
    processor_kind: str = "review",
) -> dict[str, Any]:
    clean_processor_kind = _clean_short_text(processor_kind, "review", limit=80)
    where = f"WHERE marker.processor_kind=? AND {_work_item_automation_included_predicate('item')}"
    args: list[Any] = [clean_processor_kind]
    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        where += f" AND marker.item_id IN ({placeholders})"
        args.extend(scope_ids)
    status_rows = conn.execute(
        f"""
        SELECT marker.status, COUNT(*) AS count
        FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where}
        GROUP BY marker.status
        """,
        args,
    ).fetchall()
    recent_rows = conn.execute(
        f"""
        SELECT marker.* FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where}
        ORDER BY marker.updated_at DESC, marker.queued_at DESC, marker.marker_id
        LIMIT ?
        """,
        [*args, limit],
    ).fetchall()
    last_scan_action = (
        "trigger_preprocessing_idle_scan"
        if clean_processor_kind == "preprocessing"
        else "trigger_review_processor_idle_scan"
    )
    last_scan = conn.execute(
        """
        SELECT * FROM kanban_audit_log
        WHERE action=?
        ORDER BY created_at DESC, audit_id
        LIMIT 1
        """,
        (last_scan_action,),
    ).fetchone()
    by_status = {row["status"]: int(row["count"]) for row in status_rows}
    queued_count = int(by_status.get("queued", 0))
    processing_count = int(by_status.get("processing", 0))
    now = _utc_now_iso()
    timeout_row = conn.execute(
        f"""
        SELECT COUNT(*) AS count FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where} AND marker.status='processing'
          AND marker.processing_expires_at != ''
          AND marker.processing_expires_at <= ?
        """,
        [*args, now],
    ).fetchone()
    superseded_row = conn.execute(
        f"""
        SELECT COUNT(*) AS count FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where} AND marker.superseded_at != ''
        """,
        args,
    ).fetchone()
    retry_waiting_row = conn.execute(
        f"""
        SELECT COUNT(*) AS count FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where} AND marker.status='failed'
          AND marker.next_retry_at != ''
          AND marker.next_retry_at > ?
        """,
        [*args, now],
    ).fetchone()
    retry_due_row = conn.execute(
        f"""
        SELECT COUNT(*) AS count FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where} AND marker.status='failed'
          AND (marker.next_retry_at='' OR marker.next_retry_at <= ?)
        """,
        [*args, now],
    ).fetchone()
    next_retry_row = conn.execute(
        f"""
        SELECT MIN(marker.next_retry_at) AS next_retry_at
        FROM kanban_review_processor_markers marker
        JOIN kanban_items item ON item.item_id=marker.item_id
        {where} AND marker.status='failed'
          AND marker.next_retry_at != ''
          AND marker.next_retry_at > ?
        """,
        [*args, now],
    ).fetchone()
    failure_stats = _work_review_processor_failure_stats(
        conn,
        scope_ids,
        limit=limit,
        processor_kind=clean_processor_kind,
    )
    retry_waiting_count = int(retry_waiting_row["count"] if retry_waiting_row else 0)
    retry_due_count = int(retry_due_row["count"] if retry_due_row else 0)
    return {
        "schema": (
            KANBAN_PREPROCESSING_QUEUE_SCHEMA
            if clean_processor_kind == "preprocessing"
            else KANBAN_REVIEW_SCHEDULER_SCHEMA
        ),
        "processor_kind": clean_processor_kind,
        "queue_length": queued_count,
        "active_count": processing_count,
        "claimable_failed_count": retry_due_count,
        "retry_waiting_count": retry_waiting_count,
        "retry_due_count": retry_due_count,
        "pending_count": queued_count + processing_count + retry_due_count,
        "timeout_count": int(timeout_row["count"] if timeout_row else 0),
        "superseded_count": int(superseded_row["count"] if superseded_row else 0),
        "failure_event_count": failure_stats["event_count"],
        "repeated_failure_count": failure_stats["repeated_failure_count"],
        "last_error": failure_stats["last_error"],
        "next_retry_at": (
            next_retry_row["next_retry_at"]
            if next_retry_row and next_retry_row["next_retry_at"]
            else failure_stats["next_retry_at"]
        ),
        "by_status": by_status,
        "last_scan_at": last_scan["created_at"] if last_scan else "",
        "last_scan": {
            "audit_id": last_scan["audit_id"],
            "result": last_scan["result"],
            "metadata": _json_value(last_scan["metadata_json"], {}),
        }
        if last_scan
        else None,
        "recent_markers": [_row_to_work_review_processor_marker(row) for row in recent_rows],
        "failure_events": failure_stats["recent_events"],
        "failure_aggregates": failure_stats["aggregates"],
        "failures": failure_stats,
    }


def _require_work_review_processor_lease_owner(
    conn: Any,
    *,
    holder_id: str,
    lease_token: str,
    now_dt: datetime,
) -> Any:
    lease_row = conn.execute(
        "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
        (_review_processor_lease_id(),),
    ).fetchone()
    if not _review_lease_is_active(lease_row, now_dt):
        raise HTTPException(409, "Review Processor lease is not active")
    if not _review_lease_owner_matches(lease_row, holder_id, lease_token):
        raise HTTPException(409, "Review Processor lease is held by another worker")
    return lease_row


def _work_review_processor_marker_update_row(
    existing: Any,
    *,
    updates: dict[str, Any],
    meta: dict[str, str],
    now: str,
    reason: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    previous_metadata = _json_value(existing["metadata_json"], {})
    merged_metadata = {
        **previous_metadata,
        **metadata,
        "reason": reason,
        "previous_status": existing["status"],
    }
    provenance = {
        "schema": KANBAN_REVIEW_MARKER_SCHEMA,
        "recorded_by": meta["actor"],
        "source_surface": meta["source_surface"],
        "request_id": meta["request_id"],
        "run_id": meta["run_id"],
    }
    row = dict(existing)
    row.setdefault("next_retry_at", "")
    row.setdefault("retry_after_seconds", 0)
    row.setdefault("retry_attempt_count", 0)
    row.setdefault("last_successful_source_hash", "")
    row.setdefault("last_failure_event_id", "")
    row.setdefault("last_failure_source_hash", "")
    row.setdefault("last_error_class", "")
    row.setdefault("retry_policy_version", "")
    row.update(updates)
    row["metadata_json"] = json.dumps(merged_metadata, ensure_ascii=True, sort_keys=True)
    row["provenance_json"] = json.dumps(provenance, ensure_ascii=True, sort_keys=True)
    row["updated_at"] = now
    row["source_hash"] = _hash_json_payload(
        {
            key: value
            for key, value in row.items()
            if key not in {"source_hash", "created_at", "updated_at"}
        }
    )
    return row


def _cancel_work_review_processor_markers(
    conn: Any,
    *,
    item_ids: list[str],
    meta: dict[str, str],
    now: str,
    reason: str,
    metadata: dict[str, Any],
) -> list[Any]:
    clean_item_ids = [_clean_short_text(item_id, "", limit=180) for item_id in item_ids]
    clean_item_ids = [item_id for item_id in clean_item_ids if item_id]
    if not clean_item_ids:
        return []
    placeholders = ",".join("?" for _ in clean_item_ids)
    rows = conn.execute(
        f"""
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id IN ({placeholders})
          AND status IN ('queued', 'processing', 'failed')
        ORDER BY updated_at DESC, marker_id
        """,
        clean_item_ids,
    ).fetchall()
    cancelled_rows = []
    for row in rows:
        updated_row = _work_review_processor_marker_update_row(
            row,
            updates={
                "status": "cancelled",
                "last_seen_at": now,
                "processing_started_at": "",
                "processing_expires_at": "",
                "next_retry_at": "",
                "retry_after_seconds": 0,
                "retry_attempt_count": 0,
                "last_failure_event_id": "",
                "last_failure_source_hash": "",
                "last_error_class": "",
                "retry_policy_version": "",
                "last_error": reason,
            },
            meta=meta,
            now=now,
            reason=reason,
            metadata={
                **metadata,
                "cancelled_previous_status": row["status"],
            },
        )
        cancelled_rows.append(_write_work_review_processor_marker(conn, updated_row))
    return cancelled_rows


def _work_preprocessing_pending_ineligible_reason(conn: Any, marker_row: Any) -> str:
    item = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id=?",
        (marker_row["item_id"],),
    ).fetchone()
    if item is None:
        return "preprocessing_item_missing"
    if _work_item_automation_excluded(conn, marker_row["item_id"]):
        return "automation_excluded"
    if str(item["status"] or "") in {"archived", "done"}:
        return "preprocessing_item_closed"
    if str(item["item_type"] or "") != "item":
        return "preprocessing_non_item_type"
    if str(item["state_id"] or "") != "todo":
        return "preprocessing_not_todo"
    child = conn.execute(
        """
        SELECT 1 FROM kanban_items
        WHERE parent_item_id=? AND status!='archived'
        LIMIT 1
        """,
        (marker_row["item_id"],),
    ).fetchone()
    if child is not None:
        return "preprocessing_not_leaf"
    return ""


def _cancel_invalid_work_preprocessing_markers(
    conn: Any,
    *,
    scope_ids: list[str],
    meta: dict[str, str],
    now: str,
    metadata: dict[str, Any],
) -> tuple[list[Any], list[dict[str, Any]]]:
    args: list[Any] = []
    candidate_predicate = _work_preprocessing_candidate_predicate("item")
    where = f"""
    WHERE marker.processor_kind='preprocessing'
      AND marker.status IN ('queued', 'processing', 'failed')
      AND NOT (item.item_id IS NOT NULL AND {candidate_predicate})
    """
    if scope_ids:
        placeholders = ",".join("?" for _ in scope_ids)
        where += f" AND marker.item_id IN ({placeholders})"
        args.extend(scope_ids)
    rows = conn.execute(
        f"""
        SELECT marker.* FROM kanban_review_processor_markers marker
        LEFT JOIN kanban_items item ON item.item_id=marker.item_id
        {where}
        ORDER BY marker.queued_at ASC, marker.updated_at ASC, marker.marker_id
        """,
        args,
    ).fetchall()
    cancelled_rows = []
    marker_blockers: list[dict[str, Any]] = []
    for row in rows:
        reason = (
            _work_preprocessing_pending_ineligible_reason(
                conn,
                row,
            )
            or "preprocessing_not_candidate"
        )
        updated_row = _work_review_processor_marker_update_row(
            row,
            updates={
                "status": "cancelled",
                "last_seen_at": now,
                "processing_started_at": "",
                "processing_expires_at": "",
                "next_retry_at": "",
                "retry_after_seconds": 0,
                "retry_attempt_count": 0,
                "last_failure_event_id": "",
                "last_failure_source_hash": "",
                "last_error_class": "",
                "retry_policy_version": "",
                "last_error": reason,
                "processed_document_updated_at": row["document_updated_at"],
                "processed_source_hash": row["document_source_hash"],
                "processed_at": now,
            },
            meta=meta,
            now=now,
            reason="preprocessing_not_candidate_cancelled",
            metadata={
                **metadata,
                "contract_schema": KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA,
                "candidate_state_id": "todo",
                "candidate_leaf_required": True,
                "cancelled_previous_status": row["status"],
                "last_outcome_at": now,
                "last_outcome_status": "preprocessing_not_candidate_cancelled",
                "last_error": reason,
            },
        )
        saved_row = _write_work_review_processor_marker(conn, updated_row)
        cancelled_rows.append(saved_row)
        marker_blocker = _upsert_work_processor_marker_blocker(
            conn,
            saved_row,
            meta=meta,
            now=now,
        )
        if marker_blocker is not None:
            marker_blockers.append(marker_blocker)
    return cancelled_rows, marker_blockers


def _row_to_work_review_decision(
    row: Any, commits: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    return {
        "schema": KANBAN_REVIEW_DECISION_SCHEMA,
        "decision_id": row["decision_id"],
        "item_id": row["item_id"],
        "processor_kind": row["processor_kind"],
        "decision_type": row["decision_type"],
        "title": row["title"],
        "summary": row["summary"],
        "rationale": row["rationale"],
        "affected_refs": _json_value(row["affected_refs_json"], []),
        "confidence": row["confidence"],
        "uncertainty": row["uncertainty"],
        "proof_refs": _json_value(row["proof_refs_json"], []),
        "commit_link_ids": _json_value(row["commit_link_ids_json"], []),
        "commit_count": len(commits or []),
        "commits": commits or [],
        "status": row["status"],
        "provider_mode": row["provider_mode"],
        "source_hash": row["source_hash"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _compact_work_review_decision(
    decision: dict[str, Any],
    *,
    include_metadata: bool = False,
) -> dict[str, Any]:
    keys = (
        "schema",
        "decision_id",
        "item_id",
        "processor_kind",
        "decision_type",
        "title",
        "summary",
        "rationale",
        "confidence",
        "uncertainty",
        "commit_link_ids",
        "commit_count",
        "status",
        "provider_mode",
        "source_hash",
        "created_at",
        "updated_at",
    )
    compact = {key: decision.get(key) for key in keys if key in decision}
    if include_metadata:
        compact["affected_refs"] = decision.get("affected_refs") or []
        compact["proof_refs"] = decision.get("proof_refs") or []
        compact["commits"] = decision.get("commits") or []
        compact["metadata"] = decision.get("metadata") or {}
        compact["provenance"] = decision.get("provenance") or {}
    return compact


def _compact_work_output_contract(contract: dict[str, Any]) -> dict[str, Any]:
    output_types = (
        contract.get("output_types") if isinstance(contract.get("output_types"), list) else []
    )
    provider = (
        contract.get("provider_mode") if isinstance(contract.get("provider_mode"), dict) else {}
    )
    return {
        "schema": contract.get("schema") or "",
        "status": contract.get("status") or "",
        "version": contract.get("version") or "",
        "provider_mode": {
            "active": provider.get("active") or "",
            "local_processing_gate": provider.get("local_processing_gate") or "",
            "automatic_switch": provider.get("automatic_switch"),
        },
        "output_types": [
            {
                "type": item.get("type") or "",
                "label": item.get("label") or item.get("type") or "",
                "decision_type": item.get("decision_type") or "",
            }
            for item in output_types
            if isinstance(item, dict)
        ],
    }


def _compact_work_processing_policy(
    policy: dict[str, Any],
    *,
    include_auth_drift: bool = False,
) -> dict[str, Any]:
    profile = (
        policy.get("profile_processing")
        if isinstance(policy.get("profile_processing"), dict)
        else {}
    )
    local = (
        policy.get("local_processing") if isinstance(policy.get("local_processing"), dict) else {}
    )
    compact_profile = {
        "state": profile.get("state") or "",
        "required": bool(profile.get("required")),
        "automatic_switch": bool(profile.get("automatic_switch")),
        "failure_policy": profile.get("failure_policy") or "",
    }
    if include_auth_drift:
        compact_profile["auth_drift"] = profile.get("auth_drift") or {}
    return {
        "schema": policy.get("schema") or "",
        "status": policy.get("status") or "",
        "version": policy.get("version") or "",
        "active_mode": policy.get("active_mode") or "",
        "applies_to": policy.get("applies_to")
        if isinstance(policy.get("applies_to"), list)
        else [],
        "profile_processing": compact_profile,
        "local_processing": {
            "state": local.get("state") or "",
            "gate": local.get("gate") or "",
            "automatic_switch": bool(local.get("automatic_switch")),
            "substitute_decisions_allowed": bool(local.get("substitute_decisions_allowed")),
        },
    }


def _compact_work_contract_header(contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": contract.get("schema") or "",
        "status": contract.get("status") or "",
        "version": contract.get("version") or "",
    }


def _compact_work_proposal_surfaces(contract: dict[str, Any]) -> dict[str, Any]:
    compact = _compact_work_contract_header(contract)
    for key in ("surface_root", "workstream", "inbox", "outbox"):
        value = contract.get(key)
        if isinstance(value, dict):
            compact[key] = {
                "item_id": value.get("item_id") or "",
                "uri": value.get("uri") or "",
                "role": value.get("role") or "",
            }
    return compact


def _compact_work_review_marker(marker: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "schema",
        "marker_id",
        "item_id",
        "processor_kind",
        "document_type",
        "document_ref",
        "document_updated_at",
        "document_source_hash",
        "processed_document_updated_at",
        "processed_source_hash",
        "processed_at",
        "queued_at",
        "last_seen_at",
        "processing_started_at",
        "processing_expires_at",
        "attempt_count",
        "last_error",
        "next_retry_at",
        "retry_after_seconds",
        "retry_attempt_count",
        "last_error_class",
        "retry_state",
        "retry_waiting",
        "superseded_at",
        "superseded_by_source_hash",
        "status",
        "provider_mode",
        "decision_id",
        "source_hash",
        "created_at",
        "updated_at",
    )
    return {key: marker.get(key) for key in keys if key in marker}


def _compact_work_failure_entry(entry: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "schema",
        "failure_event_id",
        "marker_id",
        "item_id",
        "item_title",
        "item_ref",
        "processor_kind",
        "document_type",
        "source_hash",
        "error_class",
        "error_message",
        "provider_mode",
        "model_alias",
        "attempt_number",
        "attempt_count",
        "event_count",
        "first_failed_at",
        "last_failed_at",
        "last_error",
        "failed_at",
        "next_retry_at",
        "scheduled_retry_at",
        "retry_after_seconds",
        "retry_policy_version",
        "retryable",
        "status",
        "marker_status",
        "retry_state",
        "retry_waiting",
        "terminal",
        "created_at",
        "updated_at",
    )
    return {key: entry.get(key) for key in keys if key in entry}


def _compact_work_marker_stats(stats: dict[str, Any]) -> dict[str, Any]:
    compact = dict(stats)
    compact["recent_markers"] = [
        _compact_work_review_marker(marker)
        for marker in stats.get("recent_markers", [])
        if isinstance(marker, dict)
    ]
    compact["failure_events"] = [
        _compact_work_failure_entry(event)
        for event in stats.get("failure_events", [])
        if isinstance(event, dict)
    ]
    compact["failure_aggregates"] = [
        _compact_work_failure_entry(entry)
        for entry in stats.get("failure_aggregates", [])
        if isinstance(entry, dict)
    ]
    return compact


def _compact_work_failure_stats(stats: dict[str, Any]) -> dict[str, Any]:
    compact = dict(stats)
    compact["recent_events"] = [
        _compact_work_failure_entry(event)
        for event in stats.get("recent_events", [])
        if isinstance(event, dict)
    ]
    compact["aggregates"] = [
        _compact_work_failure_entry(entry)
        for entry in stats.get("aggregates", [])
        if isinstance(entry, dict)
    ]
    return compact


def _work_decision_commit_rows(conn: Any, commit_link_ids: list[str]) -> list[Any]:
    clean_ids = _clean_event_list(commit_link_ids, limit=64)
    if not clean_ids:
        return []
    placeholders = ",".join("?" for _ in clean_ids)
    rows = conn.execute(
        f"SELECT * FROM kanban_item_commits WHERE commit_link_id IN ({placeholders})",
        clean_ids,
    ).fetchall()
    by_id = {row["commit_link_id"]: row for row in rows}
    return [by_id[commit_id] for commit_id in clean_ids if commit_id in by_id]


def _work_decision_commit_health(rows: list[Any], conn: Any) -> dict[str, Any]:
    missing: list[str] = []
    linked_decisions = 0
    hook_failures = 0
    for row in rows:
        commit_ids = _clean_event_list(_json_value(row["commit_link_ids_json"], []), limit=64)
        if commit_ids:
            linked_decisions += 1
            found = {
                commit["commit_link_id"] for commit in _work_decision_commit_rows(conn, commit_ids)
            }
            missing.extend(commit_id for commit_id in commit_ids if commit_id not in found)
        metadata = _json_value(row["metadata_json"], {})
        if row["status"] == "hook_failed" or str(metadata.get("hook_status") or "") == "failed":
            hook_failures += 1
    return {
        "decision_count": len(rows),
        "decisions_with_commits": linked_decisions,
        "missing_commit_link_count": len(missing),
        "missing_commit_link_ids": missing[:20],
        "hook_failure_count": hook_failures,
        "ok": not missing and hook_failures == 0,
    }


def _row_to_work_agent_hints(row: Any | None, item_id: str) -> dict[str, Any]:
    if row is None:
        return {
            "schema": "xarta.kanban.agent_hints.v1",
            "exists": False,
            "hint_id": _kanban_agent_hints_id(item_id),
            "item_id": item_id,
            "required_skills": [],
            "routing_notes": "",
            "commit_attribution": {},
            "visibility": "agent",
            "status": "missing",
            "metadata": {},
            "provenance": {},
            "created_at": None,
            "updated_at": None,
        }
    return {
        "schema": "xarta.kanban.agent_hints.v1",
        "exists": True,
        "hint_id": row["hint_id"],
        "item_id": row["item_id"],
        "required_skills": _json_value(row["required_skills_json"], []),
        "routing_notes": row["routing_notes"],
        "commit_attribution": _json_value(row["commit_attribution_json"], {}),
        "visibility": row["visibility"],
        "status": row["status"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_work_agent_session(row: Any) -> dict[str, Any]:
    return {
        "schema": "xarta.kanban.agent_session.v1",
        "session_id": row["session_id"],
        "item_id": row["item_id"],
        "agent_id": row["agent_id"],
        "node_id": row["node_id"],
        "worktree_path": row["worktree_path"],
        "repo_full_name": row["repo_full_name"],
        "branch": row["branch"],
        "status": row["status"],
        "started_at": row["started_at"],
        "ended_at": row["ended_at"],
        "last_seen_at": row["last_seen_at"],
        "request_hash": row["request_hash"],
        "source_surface": row["source_surface"],
        "summary": row["summary"],
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
        "automation_excluded": (
            bool(row["automation_excluded"]) if "automation_excluded" in row_keys else False
        ),
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


def _work_priority_recommendation_id(scope_id: str, rank: int) -> str:
    digest = hashlib.sha256(f"{scope_id}\n{rank}".encode("utf-8")).hexdigest()
    return f"kanban-priority-{digest[:24]}"


def _row_to_work_priority_recommendation(
    read: KanbanPriorityRecommendationRead,
) -> dict[str, Any]:
    row = read.recommendation
    item = read.item
    title = item["title"] if item is not None else row["title"]
    state_id = item["state_id"] if item is not None else row["state_id"]
    priority_id = item["priority_id"] if item is not None else row["priority_id"]
    breadcrumbs = [_row_to_work_item(item_row) for item_row in read.breadcrumbs]
    return {
        "recommendation_id": row["recommendation_id"],
        "scope_id": row["scope_id"],
        "rank": int(row["rank"] or 0),
        "item_id": row["item_id"],
        "canonical_code": f"xarta-kanban:item:{row['item_id']}",
        "title": title,
        "saved_title": row["title"],
        "summary": row["summary"],
        "reason": row["reason"],
        "priority_id": priority_id,
        "saved_priority_id": row["priority_id"],
        "state_id": state_id,
        "saved_state_id": row["state_id"],
        "score": float(row["score"] or 0),
        "strategy_version": row["strategy_version"],
        "source_surface": row["source_surface"],
        "metadata": _json_value(row["metadata_json"], {}),
        "provenance": _json_value(row["provenance_json"], {}),
        "generated_at": row["generated_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "item_missing": item is None,
        "breadcrumbs": breadcrumbs,
        "path": " / ".join(item_row["title"] or item_row["item_id"] for item_row in breadcrumbs),
        "href": f"blueprints://kanban/items/{row['item_id']}",
    }


def _work_priority_recommendations_payload(
    conn: Any,
    *,
    scope_id: str = KANBAN_PRIORITY_SCOPE_ID,
    limit: int = 10,
) -> dict[str, Any]:
    clean_scope_id = _clean_short_text(scope_id, KANBAN_PRIORITY_SCOPE_ID, limit=120)
    clean_limit = max(1, min(int(limit or 10), 50))
    with _kanban_read_store(conn) as store:
        reads = store.priority_recommendations(
            scope_id=clean_scope_id,
            limit=clean_limit,
        )
    recommendations = [_row_to_work_priority_recommendation(read) for read in reads]
    source = "managed" if recommendations else "empty"
    return {
        "ok": True,
        "schema": "xarta.kanban.priority_recommendations.v1",
        "source": source,
        "empty_reason": ""
        if recommendations
        else "No saved Kanban priority list has been recorded yet.",
        "scope_id": clean_scope_id,
        "limit": clean_limit,
        "count": len(recommendations),
        "strategy_version": recommendations[0]["strategy_version"] if recommendations else "",
        "generated_at": recommendations[0]["generated_at"] if recommendations else "",
        "recommendations": recommendations,
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


def _normalise_filter_id(value: Any, default: str = "") -> str:
    text = re.sub(r"&", " and ", str(value or "").strip().lower())
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text or default


def _filter_title(value: str) -> str:
    return " ".join(part[:1].upper() + part[1:] for part in re.split(r"[-_\s]+", value) if part)


def _clean_filter_color(value: str | None, default: str = "blue") -> str:
    clean = _clean_short_text(value, default, limit=32).lower()
    return clean if clean in PERSONAL_FILTER_COLORS else default


def _clean_filter_shape(value: str | None, default: str = "circle") -> str:
    clean = _clean_short_text(value, default, limit=32).lower()
    return clean if clean in PERSONAL_FILTER_SHAPES else default


def _clean_filter_fill(value: str | None, default: str = "outline") -> str:
    clean = _clean_short_text(value, default, limit=32).lower()
    return clean if clean in PERSONAL_FILTER_FILLS else default


def _clean_filter_priority(value: Any) -> int:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        numeric = 0
    return max(0, min(999, numeric))


def _kanban_item_id_from_share_ref(value: Any) -> str:
    clean = _clean_short_text(str(value or ""), "", limit=260)
    if not clean:
        return ""
    if clean.startswith("xarta-kanban:"):
        parts = clean.split(":", 2)
        if len(parts) == 3 and parts[1] in {"item", "issue", "todo"}:
            return _clean_short_text(parts[2], "", limit=180)
    if clean.startswith("kanban_items:"):
        return _clean_short_text(clean.split(":", 1)[1], "", limit=180)
    return ""


def _normalize_kanban_graph_ref(value: Any) -> str:
    item_id = _kanban_item_id_from_share_ref(value)
    return f"kanban_items:{item_id}" if item_id else ""


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


def _kanban_document_body_hash(item_id: str, body: str) -> str:
    return _hash_json_payload({"item_id": item_id, "body": _normalise_markdown_document_body(body)})


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
        elif clean_domain == "kanban" and clean_type in {"review", "item-review", "kanban-review"}:
            document.update(_work_item_review_document(conn, clean_id))
            document["document_type"] = "item-review"
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
    kanban_ref = _normalize_kanban_graph_ref(value)
    if kanban_ref:
        return kanban_ref
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
    sync: bool = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> dict[str, Any]:
    query = (q or "").strip()
    sync_summary = None
    if sync:
        sync_summary = await _sync_personal_search_index(
            include_embeddings=False,
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
    return await _run_personal_sync_work(
        _list_personal_events_sync,
        date_start,
        date_end,
        source_type,
        status,
        privacy_level,
        tag,
        related_kanban_item,
        import_batch,
        mode,
        kind,
        limit,
        offset,
    )


def _list_personal_events_sync(
    date_start: str | None,
    date_end: str | None,
    source_type: str | None,
    status: str | None,
    privacy_level: str | None,
    tag: str | None,
    related_kanban_item: str | None,
    import_batch: str | None,
    mode: str | None,
    kind: str | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if date_start and date_end:
        where.append(f"local_date <= ? AND {PERSONAL_EVENT_LOCAL_END_DATE_SQL} >= ?")
        params.extend([date_end, date_start])
    elif date_start:
        where.append(f"{PERSONAL_EVENT_LOCAL_END_DATE_SQL} >= ?")
        params.append(date_start)
    elif date_end:
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
    return await _run_personal_sync_work(
        _list_personal_tasks_sync,
        date_start,
        date_end,
        status,
        privacy_level,
        tag,
        related_kanban_item,
        import_batch,
        mode,
        source_type,
        limit,
        offset,
    )


def _list_personal_tasks_sync(
    date_start: str | None,
    date_end: str | None,
    status: str | None,
    privacy_level: str | None,
    tag: str | None,
    related_kanban_item: str | None,
    import_batch: str | None,
    mode: str | None,
    source_type: str | None,
    limit: int,
    offset: int,
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

    fetch_limit = max(limit + offset, 200)
    with get_conn() as conn:
        kanban_preferences = _kanban_preferences(conn)
        show_test_entries = bool(kanban_preferences["show_test_entries"])
        hidden_personal_tasks = 0
        hidden_personal_events = 0
        if not show_test_entries:
            hidden_personal_tasks = _count_agent_working_out_rows(
                conn,
                "personal_time_tasks",
                task_where,
                task_params,
            )
            hidden_personal_events = _count_agent_working_out_rows(
                conn,
                "personal_events",
                event_where,
                event_params,
                exclude_task_projection=True,
            )
            task_where.append(_agent_working_out_hidden_sql("tags_json"))
            task_params.append(KANBAN_AGENT_WORKING_OUT_TAG)
            event_where.append(_agent_working_out_hidden_sql("tags_json"))
            event_params.append(KANBAN_AGENT_WORKING_OUT_TAG)
        task_clause = f"WHERE {' AND '.join(task_where)}" if task_where else ""
        event_clause = f"WHERE {' AND '.join(event_where)}" if event_where else ""
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
            show_test_entries,
        )
        counts = _task_counts(conn, show_test_entries=show_test_entries)

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
            "show": show_test_entries,
            "hidden_kanban_todos": hidden_kanban_todos,
            "hidden_personal_tasks": hidden_personal_tasks,
            "hidden_personal_events": hidden_personal_events,
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


def _time_tags(tags: list[str], *, all_day: bool, timed: bool) -> list[str]:
    cleaned = [tag for tag in _clean_event_list(tags) if tag not in {"all-day", "timed"}]
    if all_day:
        cleaned.append("all-day")
    elif timed:
        cleaned.append("timed")
    return cleaned


def _operator_event_text_parts(text: str, fallback_title: str = "") -> tuple[str, str]:
    lines = str(text or "").strip().splitlines()
    title_index = next((idx for idx, line in enumerate(lines) if line.strip()), -1)
    if title_index < 0:
        return _clean_short_text(fallback_title, "Untitled", limit=180), ""
    title = _clean_short_text(lines[title_index], fallback_title or "Untitled", limit=180)
    body = "\n".join(lines[title_index + 1 :]).strip()
    return title, body


def _manual_task_id_from_event_row(row: Any) -> str:
    source_ref = str(row["source_ref"] or "")
    if source_ref.startswith("personal_time_tasks:"):
        return source_ref.split(":", 1)[1]
    provenance = _json_value(row["provenance_json"], {})
    task_meta = provenance.get("task") if isinstance(provenance, dict) else {}
    task_id = str(task_meta.get("task_id") or "").strip() if isinstance(task_meta, dict) else ""
    return task_id or str(row["event_id"] or "")


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
    tag_filter = ""
    tag_params: tuple[str, ...] = ()
    if not show_test_entries:
        tag_filter = f" AND {_agent_working_out_hidden_sql('tags_json')}"
        tag_params = (KANBAN_AGENT_WORKING_OUT_TAG,)
    today_row = conn.execute(
        f"""
        SELECT COUNT(*) AS count FROM personal_time_tasks
        WHERE local_date=? AND status IN ('open', 'blocked', 'pending_review')
          AND privacy_level != 'pin'
          {tag_filter}
        """,
        (today, *tag_params),
    ).fetchone()
    counts["today"] = int(today_row["count"] if today_row else 0)
    rows = conn.execute(
        f"""
        SELECT status, mode, COUNT(*) AS count
        FROM personal_time_tasks
        WHERE privacy_level != 'pin'
        {tag_filter}
        GROUP BY status, mode
        """,
        tag_params,
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


def _clean_work_agent_skills(values: list[str] | None) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        skill = _clean_short_text(value, "", limit=220)
        if not skill or skill in seen:
            continue
        cleaned.append(skill)
        seen.add(skill)
    return cleaned


def _clean_work_agent_hint_status(value: str | None, *, default: str = "active") -> str:
    status = _clean_short_text(value, default, limit=40)
    if status not in {"active", "archived"}:
        raise HTTPException(400, "Kanban agent hints status is invalid")
    return status


def _clean_work_agent_session_status(value: str | None, *, default: str = "active") -> str:
    status = _clean_short_text(value, default, limit=40)
    if status not in {"active", "paused", "done", "abandoned"}:
        raise HTTPException(400, "Kanban agent session status is invalid")
    return status


def _clean_work_agent_session_id(value: str | None) -> str:
    return _clean_work_id(value, "kanban-agent-session")


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


def _kanban_item_review_path(conn: Any, item_id: str, *, ensure_project: bool = False) -> Path:
    return _kanban_item_dir(conn, item_id, ensure_project=ensure_project) / "review.md"


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


def _work_item_detail_document(
    conn: Any,
    item_id: str,
    *,
    cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cache_key = ("detail", item_id)
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    item = _work_item_or_404(conn, item_id)
    path = _kanban_item_detail_path(conn, item_id)
    metadata, body, exists = _read_kanban_markdown_document(path)
    document = {
        "schema": metadata.get("schema") or KANBAN_ITEM_DETAIL_SCHEMA,
        "item_id": item_id,
        "body": body,
        "exists": exists,
        "file_ref": _kanban_document_ref(path),
        "metadata": metadata,
        "updated_at": metadata.get("updated_at") or item["updated_at"],
    }
    if cache is not None:
        cache[cache_key] = document
    return document


def _work_item_review_document(
    conn: Any,
    item_id: str,
    *,
    cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    cache_key = ("review", item_id)
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    item = _work_item_or_404(conn, item_id)
    path = _kanban_item_review_path(conn, item_id)
    metadata, body, exists = _read_kanban_markdown_document(path)
    document = {
        "schema": metadata.get("schema") or KANBAN_ITEM_REVIEW_SCHEMA,
        "item_id": item_id,
        "body": body,
        "exists": exists,
        "file_ref": _kanban_document_ref(path),
        "metadata": metadata,
        "updated_at": metadata.get("updated_at") or item["updated_at"],
    }
    if cache is not None:
        cache[cache_key] = document
    return document


def _write_work_item_markdown_document(
    conn: Any,
    item_id: str,
    body: str,
    *,
    document_kind: str,
    actor: str,
    now: str,
    metadata_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = _work_item_or_404(conn, item_id)
    if document_kind == "review":
        path = _kanban_item_review_path(conn, item_id, ensure_project=True)
        schema = KANBAN_ITEM_REVIEW_SCHEMA
    else:
        path = _kanban_item_detail_path(conn, item_id, ensure_project=True)
        schema = KANBAN_ITEM_DETAIL_SCHEMA
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_body = _normalise_markdown_document_body(body)
    existing_metadata: dict[str, Any] = {}
    existing_body = ""
    existing_exists = False
    if document_kind == "review":
        existing_metadata, existing_body, existing_exists = _read_kanban_markdown_document(path)
    body_hash = _kanban_document_body_hash(item_id, clean_body)
    existing_body_hash = (
        _clean_short_text(existing_metadata.get("body_hash"), "", limit=120)
        if existing_exists
        else ""
    )
    existing_body_clean = _normalise_markdown_document_body(existing_body)
    if existing_exists and not existing_body_hash:
        existing_body_hash = _kanban_document_body_hash(item_id, existing_body_clean)
    body_changed = not existing_exists or existing_body_hash != body_hash
    updated_at = (
        now
        if document_kind != "review" or body_changed
        else _clean_short_text(existing_metadata.get("updated_at"), "", limit=80) or now
    )
    metadata_actor = (
        actor
        if document_kind != "review" or body_changed
        else _clean_short_text(existing_metadata.get("actor"), actor, limit=120)
    )
    protected_metadata_keys = {
        "schema",
        "item_id",
        "root_item_id",
        "title",
        "actor",
        "updated_at",
        "body_hash",
    }
    preserved_metadata = (
        {
            key: value
            for key, value in existing_metadata.items()
            if key not in protected_metadata_keys
        }
        if document_kind == "review" and isinstance(existing_metadata, dict)
        else {}
    )
    extra_metadata = {
        key: value
        for key, value in dict(metadata_extra or {}).items()
        if key not in protected_metadata_keys
    }
    metadata = {
        "item_id": item_id,
        "root_item_id": _work_root_item(conn, item_id)["item_id"],
        "title": item["title"],
        "actor": metadata_actor,
        "updated_at": updated_at,
    }
    if document_kind == "review":
        metadata.update(preserved_metadata)
        metadata.update(extra_metadata)
        metadata["body_hash"] = body_hash
    path.write_text(
        _kanban_markdown_text(schema, metadata, clean_body),
        encoding="utf-8",
    )
    if document_kind == "review":
        return _work_item_review_document(conn, item_id)
    return _work_item_detail_document(conn, item_id)


def _write_work_item_detail_document(
    conn: Any,
    item_id: str,
    body: str,
    *,
    actor: str,
    now: str,
) -> dict[str, Any]:
    return _write_work_item_markdown_document(
        conn,
        item_id,
        body,
        document_kind="detail",
        actor=actor,
        now=now,
    )


def _write_work_item_review_document(
    conn: Any,
    item_id: str,
    body: str,
    *,
    actor: str,
    now: str,
    metadata_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _write_work_item_markdown_document(
        conn,
        item_id,
        body,
        document_kind="review",
        actor=actor,
        now=now,
        metadata_extra=metadata_extra,
    )


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


def _recompute_work_child_depths(
    conn: Any,
    item_id: str,
    depth: int,
    *,
    now: str | None = None,
) -> None:
    _kanban_write_store(conn).recompute_child_depths(
        item_id,
        depth,
        now=now or _utc_now_iso(),
    )


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
    if _work_request_is_automation(
        {"actor": actor, "source_surface": source_surface, "run_id": run_id}
    ) and (not str(request_id or "").strip() or not str(run_id or "").strip()):
        raise RuntimeError("Kanban automation audit rows require request_id and run_id")
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


def _work_open_blocker_rows(conn: Any, item_id: str) -> list[Any]:
    return conn.execute(
        """
        SELECT * FROM kanban_blockers
        WHERE item_id=?
          AND status NOT IN ('resolved', 'closed', 'done')
        ORDER BY updated_at DESC, blocker_id
        """,
        (item_id,),
    ).fetchall()


def _work_open_blocker_count(conn: Any, item_id: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_blockers
        WHERE item_id=?
          AND status NOT IN ('resolved', 'closed', 'done')
        """,
        (item_id,),
    ).fetchone()
    return int(row["count"] if row else 0)


def _work_item_has_non_archived_children(conn: Any, item_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM kanban_items
        WHERE parent_item_id=? AND status != 'archived'
        LIMIT 1
        """,
        (item_id,),
    ).fetchone()
    return row is not None


def _work_lane_order_snapshot(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
) -> list[str]:
    return _kanban_write_store(conn).lane_order_snapshot(parent_item_id, state_id, priority_id)


def _work_blocked_reason_from_text(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for marker in (
        "Blocked reason/question:",
        "Blocked reason:",
        "Blocked question:",
        "State: Blocked",
    ):
        if marker in text:
            return _body_excerpt(text.split(marker, 1)[1].strip(), limit=1600)
    return ""


def _work_blocker_payload_from_request(
    body: Any,
    *,
    item_title: str,
    item_body: str,
    default_title: str = "",
) -> dict[str, str] | None:
    explicit_title = _clean_short_text(getattr(body, "blocker_title", None), "", limit=180)
    explicit_body = _body_excerpt(getattr(body, "blocker_body", None) or "", limit=4000)
    blocked_by_ref = _normalize_kanban_graph_ref(getattr(body, "blocked_by_ref", None))
    if not blocked_by_ref:
        blocked_by_ref = _clean_short_text(getattr(body, "blocked_by_ref", None), "", limit=220)
    inferred_body = _work_blocked_reason_from_text(item_body)
    if not explicit_title and not explicit_body and not inferred_body:
        return None
    title = explicit_title or default_title or f"Blocked: {item_title}"
    body_text = explicit_body or inferred_body or _body_excerpt(item_body, limit=4000)
    return {
        "title": title,
        "body": body_text,
        "blocked_by_ref": blocked_by_ref,
    }


def _work_blocked_leaf_guard_error(
    *,
    item_id: str,
    operation: str,
    actor: str,
    source_surface: str,
) -> HTTPException:
    return HTTPException(
        409,
        {
            "error": "kanban_automation_blocked_leaf_requires_blocker",
            "message": (
                "Automation may not leave a leaf card in the blocked lane without "
                "an open, API-visible Blocker row."
            ),
            "item_id": item_id,
            "operation": operation,
            "actor": actor,
            "source_surface": source_surface,
        },
    )


def _upsert_work_blocker_locked(
    conn: Any,
    *,
    item: Any,
    blocker_id: str | None,
    title: str,
    body: str,
    status: str,
    blocked_by_ref: str,
    meta: dict[str, str],
    now: str,
    action: str,
    provenance_extra: dict[str, Any] | None = None,
    audit_metadata: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    store = _kanban_write_store(conn)
    clean_blocker_id = _clean_work_id(blocker_id, "blocker")
    clean_title = _clean_short_text(title, "", limit=180)
    if not clean_title:
        raise HTTPException(400, "Kanban blocker title is required")
    body_excerpt = _body_excerpt(body or "", limit=4000)
    blocker_status = _clean_work_leaf_status(status)
    clean_blocked_by_ref = _normalize_kanban_graph_ref(blocked_by_ref) or _clean_short_text(
        blocked_by_ref,
        "",
        limit=220,
    )
    search_text, search_metadata, vector_key = _work_search_payload(
        table_name="kanban_blockers",
        row_id=clean_blocker_id,
        kind="blocker",
        title=clean_title,
        body=body_excerpt,
        related_refs=[clean_blocked_by_ref] if clean_blocked_by_ref else [],
    )
    provenance = {
        "blocker": {"item_id": item["item_id"]},
        **meta,
    }
    if provenance_extra:
        provenance.update(provenance_extra)
    source_hash = _hash_json_payload(
        {
            "blocker_id": clean_blocker_id,
            "item_id": item["item_id"],
            "title": clean_title,
            "body": body_excerpt,
            "status": blocker_status,
            "blocked_by_ref": clean_blocked_by_ref,
            "provenance_extra": provenance_extra or {},
        }
    )
    previous = store.blocker_row(clean_blocker_id)
    created_at = previous["created_at"] if previous and previous["created_at"] else now
    blocker_row = store.upsert_blocker_row(
        {
            "blocker_id": clean_blocker_id,
            "item_id": item["item_id"],
            "title": clean_title,
            "body_excerpt": body_excerpt,
            "status": blocker_status,
            "blocked_by_ref": clean_blocked_by_ref,
            "search_text": search_text,
            "search_metadata": search_metadata,
            "vector_index_key": vector_key,
            "provenance": provenance,
            "created_at": created_at,
            "updated_at": now,
        }
    )
    metadata = {
        "status": blocker_row["status"],
        "blocked_by_ref": blocker_row["blocked_by_ref"],
    }
    if audit_metadata:
        metadata.update(audit_metadata)
    audit_row = _write_work_audit(
        conn,
        audit_id=f"audit-{uuid.uuid4().hex}",
        actor=meta["actor"],
        source_surface=meta["source_surface"],
        action=action,
        target_ref=f"kanban_blockers:{clean_blocker_id}",
        item_id=item["item_id"],
        parent_item_id=item["parent_item_id"] or "",
        created_at=now,
        request_id=meta["request_id"],
        run_id=meta["run_id"],
        result="ok",
        source_hash=source_hash,
        metadata=metadata,
    )
    return blocker_row, audit_row


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


def _work_completion_actor_is_restricted(meta: dict[str, str]) -> bool:
    actor = str(meta.get("actor") or "").strip().lower()
    source_surface = str(meta.get("source_surface") or "").strip().lower()
    run_id = str(meta.get("run_id") or "").strip().lower()
    if actor in {"blueprints-ui", "operator", "user", "dave"}:
        return False
    agent_tokens = ("agent", "codex", "claude", "copilot", "roo", "hermes")
    automation_tokens = (
        "automation",
        "processor",
        "preprocess",
        "xarta-kanban-work",
        "blueprints-work-management",
        "skill",
    )
    return (
        any(token in actor for token in agent_tokens)
        or any(token in source_surface for token in (*agent_tokens, *automation_tokens))
        or any(token in run_id for token in agent_tokens)
    )


def _work_state_is_terminal(state: Any) -> bool:
    try:
        return bool(state["is_terminal"])
    except (KeyError, TypeError):
        return False


def _work_completion_blocker_rows(conn: Any, item_id: str) -> dict[str, Any]:
    scope_ids = _work_scope_item_ids(conn, item_id)
    if not scope_ids:
        return {"scope_ids": [], "blockers": []}
    placeholders = ",".join("?" for _ in scope_ids)
    descendant_ids = [scope_id for scope_id in scope_ids if scope_id != item_id]
    blockers: list[dict[str, Any]] = []
    if descendant_ids:
        descendant_placeholders = ",".join("?" for _ in descendant_ids)
        open_descendant_rows = conn.execute(
            f"""
            SELECT item_id, title, state_id, status, parent_item_id, depth
            FROM kanban_items
            WHERE item_id IN ({descendant_placeholders})
              AND status != 'archived'
              AND status != 'done'
            ORDER BY depth ASC, updated_at DESC, item_id
            """,
            descendant_ids,
        ).fetchall()
        if open_descendant_rows:
            blockers.append(
                {
                    "code": "open_descendants",
                    "count": len(open_descendant_rows),
                    "items": [
                        {
                            "item_id": row["item_id"],
                            "title": row["title"],
                            "state_id": row["state_id"],
                            "status": row["status"],
                            "parent_item_id": row["parent_item_id"] or "",
                            "depth": int(row["depth"] or 0),
                        }
                        for row in open_descendant_rows[:12]
                    ],
                }
            )
    blocker_rows = conn.execute(
        f"""
        SELECT blocker_id, item_id, title, status, blocked_by_ref
        FROM kanban_blockers
        WHERE item_id IN ({placeholders})
          AND status NOT IN ('resolved', 'closed', 'done')
        ORDER BY updated_at DESC, blocker_id
        """,
        scope_ids,
    ).fetchall()
    if blocker_rows:
        blockers.append(
            {
                "code": "open_blockers",
                "count": len(blocker_rows),
                "items": [
                    {
                        "blocker_id": row["blocker_id"],
                        "item_id": row["item_id"],
                        "title": row["title"],
                        "status": row["status"],
                        "blocked_by_ref": row["blocked_by_ref"] or "",
                    }
                    for row in blocker_rows[:12]
                ],
            }
        )
    marker_rows = conn.execute(
        f"""
        SELECT marker_id, item_id, processor_kind, document_type, status,
               queued_at, processing_expires_at, attempt_count, last_error
        FROM kanban_review_processor_markers
        WHERE item_id IN ({placeholders})
          AND status IN ('queued', 'processing', 'failed')
        ORDER BY queued_at ASC, marker_id
        """,
        scope_ids,
    ).fetchall()
    if marker_rows:
        blockers.append(
            {
                "code": "pending_processor_markers",
                "count": len(marker_rows),
                "items": [
                    {
                        "marker_id": row["marker_id"],
                        "item_id": row["item_id"],
                        "processor_kind": row["processor_kind"],
                        "document_type": row["document_type"],
                        "status": row["status"],
                        "queued_at": row["queued_at"] or "",
                        "processing_expires_at": row["processing_expires_at"] or "",
                        "attempt_count": int(row["attempt_count"] or 0),
                        "last_error": row["last_error"] or "",
                    }
                    for row in marker_rows[:12]
                ],
            }
        )
    return {"scope_ids": scope_ids, "blockers": blockers}


def _work_processor_marker_blocker_id(marker_id: str) -> str:
    digest = hashlib.sha256(str(marker_id or "").encode("utf-8")).hexdigest()[:24]
    return f"kanban-blocker-processor-{digest}"


def _work_processor_marker_blocker_title(marker_row: Any) -> str:
    processor_kind = _clean_short_text(marker_row["processor_kind"], "processor", limit=80)
    document_type = _clean_short_text(marker_row["document_type"], "document", limit=80)
    last_error = _clean_short_text(marker_row["last_error"], "", limit=240)
    if last_error == (
        f"{KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV} is required for local AI Kanban automation"
    ):
        return "Use private local AI endpoint for Kanban preprocessing"
    return f"{processor_kind.title()} {document_type.replace('_', ' ')} marker failed"


def _work_processor_marker_blocker_body(marker_row: Any) -> str:
    marker_id = marker_row["marker_id"]
    processor_kind = marker_row["processor_kind"] or "processor"
    document_type = marker_row["document_type"] or "document"
    marker_status = marker_row["status"] or "failed"
    last_error = marker_row["last_error"] or ""
    local_model_alias = _work_automation_local_ai_model_alias()
    local_endpoint_detail = (
        f"`{local_model_alias}`"
        if local_model_alias
        else f"the configured `{KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV}` value"
    )
    lines = [
        f"Processor marker `{marker_id}` is `{marker_status}` for `{processor_kind}` / `{document_type}`.",
        "",
        "This marker blocks agent completion until it is reprocessed, superseded, cancelled, or resolved.",
        "",
        f"Required local AI endpoint: {local_endpoint_detail}.",
    ]
    if last_error:
        lines.extend(["", f"Last error: {last_error}"])
    if marker_row["decision_id"]:
        lines.extend(["", f"Decision ref: `kanban_review_decisions:{marker_row['decision_id']}`"])
    lines.extend(["", f"Marker ref: `kanban_review_processor_markers:{marker_id}`"])
    return "\n".join(lines)


def _upsert_work_processor_marker_blocker(
    conn: Any,
    marker_row: Any,
    *,
    meta: dict[str, str],
    now: str,
) -> dict[str, Any] | None:
    store = _kanban_write_store(conn)
    marker_status = marker_row["status"] or ""
    open_status = marker_status in {"queued", "processing", "failed"}
    blocker_id = _work_processor_marker_blocker_id(marker_row["marker_id"])
    previous = store.blocker_row(blocker_id)
    if not open_status and previous is None:
        return None
    item = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id=?",
        (marker_row["item_id"],),
    ).fetchone()
    if item is None:
        return None
    blocker_status = "open" if open_status else "resolved"
    title = (
        _work_processor_marker_blocker_title(marker_row)
        if open_status
        else f"Resolved {marker_row['processor_kind']} marker blocker"
    )
    body_excerpt = (
        _work_processor_marker_blocker_body(marker_row)
        if open_status
        else (
            f"Processor marker `{marker_row['marker_id']}` reached `{marker_status}` "
            "and no longer blocks agent completion."
        )
    )
    blocked_by_ref = f"kanban_review_processor_markers:{marker_row['marker_id']}"
    search_text, search_metadata, vector_key = _work_search_payload(
        table_name="kanban_blockers",
        row_id=blocker_id,
        kind="blocker",
        title=title,
        body=body_excerpt,
        related_refs=[blocked_by_ref],
    )
    provenance = {
        "schema": KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA,
        "marker_id": marker_row["marker_id"],
        "processor_kind": marker_row["processor_kind"],
        "document_type": marker_row["document_type"],
        "marker_status": marker_status,
        "materialized_by": meta["actor"],
        "source_surface": meta["source_surface"],
        "request_id": meta["request_id"],
        "run_id": meta["run_id"],
    }
    created_at = previous["created_at"] if previous and previous["created_at"] else now
    source_hash = _hash_json_payload(
        {
            "blocker_id": blocker_id,
            "item_id": marker_row["item_id"],
            "title": title,
            "body": body_excerpt,
            "status": blocker_status,
            "blocked_by_ref": blocked_by_ref,
            "marker_status": marker_status,
            "last_error": marker_row["last_error"] or "",
            "decision_id": marker_row["decision_id"] or "",
        }
    )
    blocker_row = store.upsert_blocker_row(
        {
            "blocker_id": blocker_id,
            "item_id": marker_row["item_id"],
            "title": title,
            "body_excerpt": _body_excerpt(body_excerpt, limit=4000),
            "status": blocker_status,
            "blocked_by_ref": blocked_by_ref,
            "search_text": search_text,
            "search_metadata": search_metadata,
            "vector_index_key": vector_key,
            "provenance": provenance,
            "created_at": created_at,
            "updated_at": now,
        }
    )
    audit_row = _write_work_audit(
        conn,
        audit_id=f"audit-{uuid.uuid4().hex}",
        actor=meta["actor"],
        source_surface=meta["source_surface"],
        action="materialize_processor_marker_blocker"
        if open_status
        else "resolve_processor_marker_blocker",
        target_ref=f"kanban_blockers:{blocker_id}",
        item_id=marker_row["item_id"],
        parent_item_id=item["parent_item_id"] or "",
        created_at=now,
        request_id=meta["request_id"],
        run_id=meta["run_id"],
        result=blocker_status,
        source_hash=source_hash,
        metadata={
            "marker_id": marker_row["marker_id"],
            "marker_status": marker_status,
            "processor_kind": marker_row["processor_kind"],
            "blocked_by_ref": blocked_by_ref,
        },
    )
    return {"blocker_row": blocker_row, "audit_row": audit_row}


def _resolve_stale_work_processor_marker_blockers(
    conn: Any,
    *,
    item_id: str,
    source: dict[str, Any],
    meta: dict[str, str],
    now: str,
) -> list[dict[str, Any]]:
    store = _kanban_write_store(conn)
    rows = conn.execute(
        """
        SELECT * FROM kanban_blockers
        WHERE item_id=?
          AND status NOT IN ('resolved', 'closed', 'done')
          AND COALESCE(json_extract(provenance_json, '$.schema'), '') = ?
        ORDER BY updated_at ASC, blocker_id
        """,
        (item_id, KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA),
    ).fetchall()
    resolved: list[dict[str, Any]] = []
    current_source_hash = str(source.get("document_source_hash") or "")
    for blocker in rows:
        provenance = _json_value(blocker["provenance_json"], {})
        marker_id = _clean_short_text(
            provenance.get("marker_id")
            or str(blocker["blocked_by_ref"] or "").removeprefix(
                "kanban_review_processor_markers:"
            ),
            "",
            limit=180,
        )
        if not marker_id:
            continue
        marker_row = conn.execute(
            "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
            (marker_id,),
        ).fetchone()
        if marker_row is None:
            stale_reason = "missing_marker"
        elif marker_row["status"] in {"processed", "skipped", "cancelled"}:
            stale_reason = f"marker_{marker_row['status']}"
        elif (
            marker_row["processor_kind"] == "preprocessing"
            and marker_row["status"] == "failed"
            and marker_row["document_source_hash"] != current_source_hash
        ):
            stale_reason = "failed_marker_source_changed"
        else:
            continue
        title = "Resolved stale processor marker blocker"
        body_excerpt = (
            f"Processor marker `{marker_id}` is stale ({stale_reason}) for the current "
            "preprocessing source and no longer blocks agent completion."
        )
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_blockers",
            row_id=blocker["blocker_id"],
            kind="blocker",
            title=title,
            body=body_excerpt,
            related_refs=[f"kanban_review_processor_markers:{marker_id}"],
        )
        updated_provenance = {
            **provenance,
            "resolved_by": meta["actor"],
            "resolved_source_surface": meta["source_surface"],
            "resolved_request_id": meta["request_id"],
            "resolved_run_id": meta["run_id"],
            "stale_reason": stale_reason,
            "current_document_source_hash": current_source_hash,
        }
        blocker_row = store.update_blocker_row(
            blocker["blocker_id"],
            {
                "title": title,
                "body_excerpt": _body_excerpt(body_excerpt, limit=4000),
                "status": "resolved",
                "search_text": search_text,
                "search_metadata": search_metadata,
                "vector_index_key": vector_key,
                "provenance": updated_provenance,
                "updated_at": now,
            },
        )
        source_hash = _hash_json_payload(
            {
                "blocker_id": blocker["blocker_id"],
                "item_id": item_id,
                "marker_id": marker_id,
                "status": "resolved",
                "stale_reason": stale_reason,
                "current_document_source_hash": current_source_hash,
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=f"audit-{uuid.uuid4().hex}",
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="resolve_stale_processor_marker_blocker",
            target_ref=f"kanban_blockers:{blocker['blocker_id']}",
            item_id=item_id,
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="resolved",
            source_hash=source_hash,
            metadata={
                "marker_id": marker_id,
                "stale_reason": stale_reason,
                "current_document_source_hash": current_source_hash,
            },
        )
        resolved.append({"blocker_row": blocker_row, "audit_row": audit_row})
    return resolved


def _work_preprocessing_blocker_requests_parent_context(blocker_row: Any) -> bool:
    provenance = _json_value(blocker_row["provenance_json"], {})
    text = " ".join(
        [
            str(blocker_row["title"] or ""),
            str(blocker_row["body_excerpt"] or ""),
            str(provenance.get("reason") or ""),
        ]
    ).lower()
    if "missing_parent_content" in text:
        return True
    return "parent" in text and any(
        term in text for term in ("content", "context", "issue", "item")
    )


def _resolve_satisfied_work_preprocessing_parent_context_blockers(
    conn: Any,
    *,
    item_row: Any,
    meta: dict[str, str],
    now: str,
    document_cache: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    store = _kanban_write_store(conn)
    item_id = item_row["item_id"]
    rows = conn.execute(
        """
        SELECT * FROM kanban_blockers
        WHERE item_id=?
          AND status NOT IN ('resolved', 'closed', 'done')
          AND COALESCE(json_extract(provenance_json, '$.schema'), '') = ?
        ORDER BY updated_at ASC, blocker_id
        """,
        (item_id, KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA),
    ).fetchall()
    rows = [row for row in rows if _work_preprocessing_blocker_requests_parent_context(row)]
    if not rows:
        return []
    ancestor_context = _work_preprocessing_ancestor_context(
        conn,
        item_row,
        document_cache=document_cache,
    )
    ancestors = ancestor_context.get("ancestors") if isinstance(ancestor_context, dict) else []
    if not isinstance(ancestors, list) or not ancestors:
        return []
    resolved: list[dict[str, Any]] = []
    ancestor_ids = [
        str((ancestor.get("item") or {}).get("item_id") or "")
        for ancestor in ancestors
        if isinstance(ancestor, dict)
    ]
    ancestor_ids = [ancestor_id for ancestor_id in ancestor_ids if ancestor_id]
    for blocker in rows:
        provenance = _json_value(blocker["provenance_json"], {})
        title = "Resolved parent context preprocessing blocker"
        body_excerpt = (
            "Preprocessing now supplies parent and ancestor body/detail/review "
            "excerpts plus recent decisions in ancestor_context, so this "
            "automation-generated parent-context blocker no longer applies."
        )
        related_refs = [f"kanban_items:{ancestor_id}" for ancestor_id in ancestor_ids[:5]]
        search_text, search_metadata, vector_key = _work_search_payload(
            table_name="kanban_blockers",
            row_id=blocker["blocker_id"],
            kind="blocker",
            title=title,
            body=body_excerpt,
            related_refs=related_refs,
        )
        updated_provenance = {
            **provenance,
            "resolved_by": meta["actor"],
            "resolved_source_surface": meta["source_surface"],
            "resolved_request_id": meta["request_id"],
            "resolved_run_id": meta["run_id"],
            "resolved_reason": "ancestor_context_available",
            "ancestor_item_ids": ancestor_ids[:5],
        }
        blocker_row = store.update_blocker_row(
            blocker["blocker_id"],
            {
                "title": title,
                "body_excerpt": _body_excerpt(body_excerpt, limit=4000),
                "status": "resolved",
                "search_text": search_text,
                "search_metadata": search_metadata,
                "vector_index_key": vector_key,
                "provenance": updated_provenance,
                "updated_at": now,
            },
        )
        source_hash = _hash_json_payload(
            {
                "blocker_id": blocker["blocker_id"],
                "item_id": item_id,
                "status": "resolved",
                "resolved_reason": "ancestor_context_available",
                "ancestor_item_ids": ancestor_ids[:5],
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=f"audit-{uuid.uuid4().hex}",
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="resolve_satisfied_preprocessing_parent_context_blocker",
            target_ref=f"kanban_blockers:{blocker['blocker_id']}",
            item_id=item_id,
            parent_item_id=item_row["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="resolved",
            source_hash=source_hash,
            metadata={
                "resolved_reason": "ancestor_context_available",
                "ancestor_item_ids": ancestor_ids[:5],
            },
        )
        resolved.append({"blocker_row": blocker_row, "audit_row": audit_row})
    return resolved


def _assert_agent_completion_not_blocked(
    conn: Any,
    *,
    item_id: str,
    existing: Any,
    target_state: Any,
    meta: dict[str, str],
) -> None:
    if not _work_state_is_terminal(target_state):
        return
    if existing["state_id"] == target_state["state_id"]:
        return
    if not _work_completion_actor_is_restricted(meta):
        return
    completion = _work_completion_blocker_rows(conn, item_id)
    blockers = completion["blockers"]
    if not blockers:
        return
    raise HTTPException(
        409,
        {
            "error": "kanban_agent_completion_blocked",
            "message": (
                "Agent or automation completion is blocked while outstanding "
                "Kanban work or unprocessed review/preprocessing evidence remains."
            ),
            "item_id": item_id,
            "target_state_id": target_state["state_id"],
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
            "blockers": blockers,
        },
    )


def _work_item_is_leaf(conn: Any, item_id: str) -> bool:
    row = conn.execute(
        """
        SELECT COUNT(*) AS count
        FROM kanban_items
        WHERE parent_item_id=? AND status!='archived'
        """,
        (item_id,),
    ).fetchone()
    return int(row["count"] if row else 0) == 0


def _work_item_has_active_agent_session(conn: Any, item_id: str) -> bool:
    row = conn.execute(
        """
        SELECT session_id
        FROM kanban_agent_sessions
        WHERE item_id=? AND status='active'
        ORDER BY updated_at DESC, started_at DESC, session_id
        LIMIT 1
        """,
        (item_id,),
    ).fetchone()
    return row is not None


def _assert_agent_leaf_doing_has_active_session(
    conn: Any,
    *,
    item_id: str,
    existing: Any,
    target_state: Any,
    meta: dict[str, str],
) -> None:
    if target_state["state_id"] != "doing":
        return
    if existing["state_id"] == target_state["state_id"]:
        return
    if not _work_completion_actor_is_restricted(meta):
        return
    if not _work_item_is_leaf(conn, item_id):
        return
    if _work_item_has_active_agent_session(conn, item_id):
        return
    raise HTTPException(
        409,
        {
            "error": "kanban_agent_leaf_doing_without_active_session",
            "message": (
                "Agent or automation lane movement is blocked because leaf cards "
                "may be in Doing only while an active agent session exists for "
                "that leaf. Use To Do for inactive unfinished leaves or Blocked "
                "for blocked leaves."
            ),
            "item_id": item_id,
            "target_state_id": target_state["state_id"],
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
        },
    )


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


def _work_leaf_metrics(rows: list[Any]) -> dict[str, Any]:
    by_state: dict[str, int] = {}
    by_status: dict[str, int] = {}
    active = 0
    active_doing = 0
    blocked = 0
    done = 0
    for row in rows:
        state_id = str(row["state_id"] or "")
        status = str(row["status"] or "")
        by_state[state_id] = by_state.get(state_id, 0) + 1
        by_status[status] = by_status.get(status, 0) + 1
        if state_id in {"backlog", "todo", "doing"}:
            active += 1
        if state_id == "doing":
            active_doing += 1
        if state_id == "blocked" or status == "blocked":
            blocked += 1
        if state_id == "done" or status in {"done", "closed", "promoted"}:
            done += 1
    return {
        "total": len(rows),
        "active": active,
        "active_doing": active_doing,
        "blocked": blocked,
        "done": done,
        "by_state": by_state,
        "by_status": by_status,
    }


def _work_rollup_leaf_metrics(
    conn: Any,
    descendant_item_ids: list[str],
    *,
    issue_cards: bool,
) -> dict[str, Any]:
    if not descendant_item_ids:
        return _work_leaf_metrics([])
    descendant_placeholders = ",".join("?" for _ in descendant_item_ids)
    issue_predicate = "w.item_type='issue'" if issue_cards else "w.item_type!='issue'"
    rows = conn.execute(
        f"""
        SELECT w.*
        FROM kanban_items w
        WHERE w.item_id IN ({descendant_placeholders})
          AND w.status != 'archived'
          AND {issue_predicate}
          AND NOT EXISTS (
              SELECT 1 FROM kanban_items child
              WHERE child.parent_item_id=w.item_id
                AND child.status != 'archived'
          )
        """,
        descendant_item_ids,
    ).fetchall()
    return _work_leaf_metrics(rows)


def _work_rollup(
    conn: Any,
    item_id: str | None = None,
    *,
    show_test_entries: bool = True,
) -> dict[str, Any]:
    item_ids = _work_scope_item_ids(conn, item_id, show_test_entries=show_test_entries)
    if not item_ids:
        return {
            "items": {
                "total": 0,
                "by_state": {},
                "by_status": {},
                "leaf_metrics": _work_leaf_metrics([]),
            },
            "issues": {
                "open": 0,
                "leaf_metrics": _work_leaf_metrics([]),
            },
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
    item_leaf_metrics = _work_rollup_leaf_metrics(
        conn,
        descendant_item_ids,
        issue_cards=False,
    )
    issue_leaf_metrics = _work_rollup_leaf_metrics(
        conn,
        descendant_item_ids,
        issue_cards=True,
    )
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
            "leaf_metrics": item_leaf_metrics,
        },
        "issues": {
            "open": int(issue_open["count"] if issue_open else 0),
            "leaf_metrics": issue_leaf_metrics,
        },
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


def _agent_working_out_hidden_sql(tags_column: str) -> str:
    return f"NOT EXISTS (SELECT 1 FROM json_each({tags_column}) WHERE lower(value) = ?)"


def _agent_working_out_only_sql(tags_column: str) -> str:
    return f"EXISTS (SELECT 1 FROM json_each({tags_column}) WHERE lower(value) = ?)"


def _count_agent_working_out_rows(
    conn: Any,
    table_name: str,
    where: list[str],
    params: list[Any],
    *,
    exclude_task_projection: bool = False,
) -> int:
    if table_name not in {"personal_time_tasks", "personal_events"}:
        return 0
    count_where = [*where, _agent_working_out_only_sql("tags_json")]
    count_params = [*params, KANBAN_AGENT_WORKING_OUT_TAG]
    if exclude_task_projection:
        count_where.append("source_type != 'manual-task'")
    clause = f"WHERE {' AND '.join(count_where)}" if count_where else ""
    row = conn.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} {clause}",
        count_params,
    ).fetchone()
    return int(row["count"] if row else 0)


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
    return _kanban_write_store(conn).order_group_rows(parent_item_id, state_id, priority_id)


def _work_order_edges_for_group(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    item_ids: set[str],
) -> list[Any]:
    return _kanban_write_store(conn)._order_edges_for_group(
        parent_item_id,
        state_id,
        priority_id,
        item_ids,
    )


def _order_work_priority_group(conn: Any, rows: list[Any]) -> list[Any]:
    return _kanban_write_store(conn).order_priority_group(rows)


def _sort_kanban_items_for_lane(conn: Any, rows: list[Any]) -> list[Any]:
    return _kanban_write_store(conn).sort_items_for_lane(rows)


def _work_item_has_order_relation(
    conn: Any,
    parent_item_id: str | None,
    state_id: str,
    priority_id: str,
    item_id: str,
) -> bool:
    return _kanban_write_store(conn).item_has_order_relation(
        parent_item_id,
        state_id,
        priority_id,
        item_id,
    )


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
    return _kanban_write_store(conn).replace_item_order_edges(
        parent_item_id=parent_item_id,
        state_id=state_id,
        priority_id=priority_id,
        ordered_ids=ordered_ids,
        now=now,
        meta=meta,
    )


def _ensure_work_item_lane_order(
    conn: Any,
    item_id: str,
    *,
    prefer_top_if_new: bool,
    now: str,
    meta: dict[str, str],
) -> tuple[list[str], list[tuple[str, str, dict[str, Any]]]]:
    return _kanban_write_store(conn).ensure_item_lane_order(
        item_id,
        prefer_top_if_new=prefer_top_if_new,
        now=now,
        meta=meta,
    )


def _work_board_payload(
    conn: Any,
    parent_item_id: str | None = None,
    *,
    show_test_entries: bool | None = None,
) -> dict[str, Any]:
    try:
        with _kanban_read_store(conn) as store:
            read = store.board(
                parent_item_id,
                show_test_entries=show_test_entries,
            )
    except (KanbanItemNotFound, KanbanItemCycleError) as exc:
        _raise_kanban_store_error(exc)
    return {
        "ok": True,
        "board": {
            "parent": _row_to_work_item(read.parent) if read.parent else None,
            "depth_limit": KANBAN_DEPTH_LIMIT,
            "remaining_depth": read.remaining_depth,
            "breadcrumbs": [_row_to_work_item(row) for row in read.breadcrumbs],
            "columns": [
                {
                    "state": _row_to_work_state(state),
                    "items": [
                        _row_to_work_item(row) for row in read.items_by_state[state["state_id"]]
                    ],
                }
                for state in read.states
            ],
            "rollup": read.rollup,
            "preferences": read.preferences,
            "hidden_test_items": read.hidden_test_items,
            "test_entries": {
                "show": read.show_test_entries,
                "hidden": read.hidden_test_items,
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
    automation_excluded = bool(body.automation_excluded)
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
    automation_metadata = {
        "source_item_id": _clean_short_text(
            getattr(body, "automation_source_item_id", None),
            "",
            limit=180,
        ),
        "marker_id": _clean_short_text(
            getattr(body, "automation_marker_id", None),
            "",
            limit=180,
        ),
        "decision_id": _clean_short_text(
            getattr(body, "automation_decision_id", None),
            "",
            limit=180,
        ),
        "reason": _clean_short_text(
            getattr(body, "automation_reason", None),
            "",
            limit=500,
        ),
        "operation_kind": _clean_short_text(
            getattr(body, "automation_operation_kind", None),
            "",
            limit=120,
        ),
    }
    automation_metadata = {key: value for key, value in automation_metadata.items() if value}
    if automation_metadata:
        provenance["automation"] = automation_metadata
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
        "automation_excluded": automation_excluded,
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
    item_row = _kanban_write_store(conn).insert_item_row(payload, now=now)
    automation_metadata = (
        payload["provenance"].get("automation")
        if isinstance(payload.get("provenance"), dict)
        and isinstance(payload["provenance"].get("automation"), dict)
        else {}
    )
    lane_order = _work_lane_order_snapshot(
        conn,
        item_row["parent_item_id"],
        item_row["state_id"],
        item_row["priority_id"],
    )
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
            "goal_flag": payload["goal_flag"],
            "automation_excluded": payload["automation_excluded"],
            "promoted_from_ref": payload["promoted_from_ref"],
            "previous": None,
            "new": {
                "parent_item_id": item_row["parent_item_id"] or "",
                "depth": int(item_row["depth"] or 0),
                "state_id": item_row["state_id"],
                "sort_order": int(item_row["sort_order"] or 0),
                "lane_order": lane_order,
            },
            "source_item_id": automation_metadata.get("source_item_id", ""),
            "marker_id": automation_metadata.get("marker_id", ""),
            "decision_id": automation_metadata.get("decision_id", ""),
            "reason": automation_metadata.get("reason", ""),
            "operation_kind": automation_metadata.get("operation_kind", action),
            "rollback": {
                "schema": "xarta.kanban.audit.rollback_recipe.v1",
                "operation": "archive_created_item",
                "item_id": payload["item_id"],
                "endpoint": f"/api/v1/personal/kanban/items/{payload['item_id']}/archive",
                "method": "POST",
                "preconditions": [
                    "item still represents the same created card",
                    "operator confirms archiving this created card is the intended rollback",
                ],
            },
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
        _recompute_work_child_depths(conn, clean_leaf_id, depth, now=now)
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


@router.get("/kanban/datastore/status")
async def get_work_kanban_datastore_status() -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_kanban_datastore_status_sync)


def _get_work_kanban_datastore_status_sync() -> dict[str, Any]:
    return kanban_datastore_status(cfg.KANBAN_DATASTORE_CONFIG)


@router.post("/kanban/datastore/bootstrap")
async def bootstrap_work_kanban_datastore(
    body: WorkDatastoreBootstrapRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_bootstrap_work_kanban_datastore_sync, body)


def _bootstrap_work_kanban_datastore_sync(
    body: WorkDatastoreBootstrapRequest,
) -> dict[str, Any]:
    try:
        with get_conn() as conn:
            plan = kanban_datastore_bootstrap_plan(
                conn,
                cfg.KANBAN_DATASTORE_CONFIG,
                apply=body.apply,
                support_setting_keys=(KANBAN_SHOW_TEST_ENTRIES_SETTING,),
            )
    except KanbanDatastoreConfigError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {
        **plan,
        "audit": {
            "actor": body.actor,
            "source_surface": body.source_surface,
            "request_id": body.request_id or "",
            "run_id": body.run_id or "",
        },
    }


@router.get("/kanban/datastore/parity")
async def get_work_kanban_datastore_parity(
    sample_limit: Annotated[int, Query(ge=1, le=20)] = 5,
    include_backups: bool = True,
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _get_work_kanban_datastore_parity_sync,
        sample_limit,
        include_backups,
    )


def _get_work_kanban_datastore_parity_sync(
    sample_limit: int,
    include_backups: bool,
) -> dict[str, Any]:
    with get_conn() as conn:
        if (
            cfg.KANBAN_DATASTORE_CONFIG.active_store == ACTIVE_STORE_POSTGRES
            or cfg.KANBAN_DATASTORE_CONFIG.read_store == CANDIDATE_READ_STORE_POSTGRES
        ):
            try:
                return kanban_postgres_parity_report(
                    conn,
                    depth_limit=KANBAN_DEPTH_LIMIT,
                    show_test_entries_setting=KANBAN_SHOW_TEST_ENTRIES_SETTING,
                    agent_working_out_tag=KANBAN_AGENT_WORKING_OUT_TAG,
                    kanban_root=Path(cfg.KANBAN_DIR),
                    backup_dir=Path(cfg.KANBAN_BACKUP_DIR),
                    datastore_config=cfg.KANBAN_DATASTORE_CONFIG,
                    sample_limit=sample_limit,
                    include_backups=include_backups,
                )
            except (KanbanPostgresError, KanbanShadowParityError) as exc:
                raise HTTPException(503, str(exc)) from exc
        return kanban_shadow_parity_report(
            conn,
            depth_limit=KANBAN_DEPTH_LIMIT,
            show_test_entries_setting=KANBAN_SHOW_TEST_ENTRIES_SETTING,
            agent_working_out_tag=KANBAN_AGENT_WORKING_OUT_TAG,
            kanban_root=Path(cfg.KANBAN_DIR),
            backup_dir=Path(cfg.KANBAN_BACKUP_DIR),
            candidate_backend=cfg.KANBAN_DATASTORE_CONFIG.candidate_backend,
            sample_limit=sample_limit,
            include_backups=include_backups,
        )


@router.get("/kanban/config")
async def get_work_config() -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_config_sync)


def _get_work_config_sync() -> dict[str, Any]:
    with get_conn() as conn:
        with _kanban_read_store(conn) as store:
            read = store.config()
    return {
        "ok": True,
        "depth_limit": read.depth_limit,
        "states": [_row_to_work_state(row) for row in read.states],
        "priorities": [_row_to_work_priority(row) for row in read.priorities],
        "preferences": read.preferences,
    }


@router.get("/kanban/preferences")
async def get_kanban_preferences() -> dict[str, Any]:
    return await _run_personal_sync_work(_get_kanban_preferences_sync)


def _get_kanban_preferences_sync() -> dict[str, Any]:
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
async def get_work_root_board(
    show_test_entries: Annotated[bool | None, Query()] = None,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_root_board_sync, show_test_entries)


def _get_work_root_board_sync(show_test_entries: bool | None = None) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_board_payload(conn, show_test_entries=show_test_entries)


@router.get("/kanban/items/{item_id}/board")
async def get_work_child_board(
    item_id: str,
    show_test_entries: Annotated[bool | None, Query()] = None,
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _get_work_child_board_sync,
        item_id,
        show_test_entries,
    )


def _get_work_child_board_sync(
    item_id: str,
    show_test_entries: bool | None = None,
) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_board_payload(conn, item_id, show_test_entries=show_test_entries)


@router.get("/kanban/items/{item_id}")
async def get_work_item_detail(item_id: str) -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_item_detail_sync, item_id)


def _get_work_item_detail_sync(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        try:
            with _kanban_read_store(conn) as store:
                read = store.item_detail(item_id)
                detail_document = store.item_detail_document(item_id)
                review_document = store.item_review_document(item_id)
                discussion_payloads = [
                    _row_to_work_discussion(row, store.conn) for row in read.discussions
                ]
        except (KanbanItemNotFound, KanbanItemCycleError) as exc:
            _raise_kanban_store_error(exc)
        return {
            "ok": True,
            "item": _row_to_work_item(read.item),
            "detail_document": detail_document,
            "review_document": review_document,
            "breadcrumbs": [_row_to_work_item(row) for row in read.breadcrumbs],
            "depth_limit": read.depth_limit,
            "remaining_depth": read.remaining_depth,
            "children": [_row_to_work_item(row) for row in read.children],
            "issues": [_row_to_work_issue(row) for row in read.issues],
            "todos": [_row_to_work_todo(row) for row in read.todos],
            "blockers": [_row_to_work_blocker(row) for row in read.blockers],
            "discussions": discussion_payloads,
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
                for row in read.links
            ],
            "commits": [_row_to_work_commit(row) for row in read.commits],
            "audit": [
                {
                    "audit_id": row["audit_id"],
                    "action": row["action"],
                    "actor": row["actor"],
                    "source_surface": row["source_surface"],
                    "created_at": row["created_at"],
                    "metadata": _json_value(row["metadata_json"], {}),
                }
                for row in read.audit
            ],
            "rollup": read.rollup,
            "counts": {
                "children": len(read.children),
                "issues": len(read.issues),
                "todos": len(read.todos),
                "blockers": len(read.blockers),
                "links": len(read.links),
                "commits": len(read.commits),
                "audit": len(read.audit),
                "discussions": len(read.discussions),
                "review": 1 if (review_document.get("body") or "").strip() else 0,
            },
        }


@router.get("/kanban/priorities")
async def get_work_priorities(
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_priorities_sync, limit)


def _get_work_priorities_sync(limit: int = 10) -> dict[str, Any]:
    with get_conn() as conn:
        return _work_priority_recommendations_payload(conn, limit=limit)


@router.put("/kanban/priorities")
async def replace_work_priorities(
    body: WorkPriorityRecommendationsReplaceRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    meta = _work_request_meta(body)
    if len(body.recommendations) > 10:
        raise HTTPException(400, "Kanban priority recommendations are limited to top 10")
    strategy_version = _clean_short_text(body.strategy_version, "skill-managed-v1", limit=120)
    generated_at = _clean_short_text(body.generated_at, now, limit=80)
    audit_id = f"audit-{uuid.uuid4().hex}"
    scope_id = KANBAN_PRIORITY_SCOPE_ID
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        seen_items: set[str] = set()
        existing_rows = store.priority_recommendation_rows(scope_id=scope_id)
        wanted_ids: set[str] = set()
        upserted_rows: list[Any] = []
        for index, entry in enumerate(body.recommendations, start=1):
            target_item_id = _clean_short_text(entry.item_id, "", limit=180)
            if not target_item_id:
                raise HTTPException(400, "Kanban priority recommendation item_id is required")
            if target_item_id in seen_items:
                raise HTTPException(
                    400, "Kanban priority recommendations cannot repeat the same item"
                )
            seen_items.add(target_item_id)
            target = _work_item_or_404(conn, target_item_id)
            priority_id = (
                _require_work_priority(conn, entry.priority_id)["priority_id"]
                if entry.priority_id
                else target["priority_id"]
            )
            state_id = (
                _require_work_state(conn, entry.state_id)["state_id"]
                if entry.state_id
                else target["state_id"]
            )
            recommendation_id = _work_priority_recommendation_id(scope_id, index)
            title = _clean_short_text(entry.title, target["title"], limit=180)
            summary = _clean_short_text(entry.summary, target["body_excerpt"] or "", limit=800)
            reason = _clean_short_text(entry.reason, "", limit=1200)
            metadata = entry.metadata if isinstance(entry.metadata, dict) else {}
            provenance = {
                "schema": "xarta.kanban.priority_recommendation.provenance.v1",
                "actor": meta["actor"],
                "source_surface": meta["source_surface"],
                "request_id": meta["request_id"],
                "run_id": meta["run_id"],
                "scope_id": scope_id,
                "item_id": target_item_id,
                "rank": index,
                "strategy_version": strategy_version,
            }
            source_hash = _hash_json_payload(
                {
                    "scope_id": scope_id,
                    "rank": index,
                    "item_id": target_item_id,
                    "title": title,
                    "summary": summary,
                    "reason": reason,
                    "priority_id": priority_id,
                    "state_id": state_id,
                    "score": entry.score,
                    "strategy_version": strategy_version,
                    "metadata": metadata,
                }
            )
            row = store.upsert_priority_recommendation(
                {
                    "recommendation_id": recommendation_id,
                    "scope_id": scope_id,
                    "rank": index,
                    "item_id": target_item_id,
                    "title": title,
                    "summary": summary,
                    "reason": reason,
                    "priority_id": priority_id,
                    "state_id": state_id,
                    "score": entry.score,
                    "strategy_version": strategy_version,
                    "source_surface": meta["source_surface"],
                    "source_hash": source_hash,
                    "metadata": metadata,
                    "provenance": provenance,
                    "generated_at": generated_at,
                },
                now=now,
            )
            wanted_ids.add(recommendation_id)
            upserted_rows.append(row)

        deleted_rows = [row for row in existing_rows if row["recommendation_id"] not in wanted_ids]
        for row in deleted_rows:
            store.delete_priority_recommendation(row["recommendation_id"])

        audit_metadata = {
            "schema": "xarta.kanban.priority_recommendations.replace.v1",
            "scope_id": scope_id,
            "recommendation_ids": [row["recommendation_id"] for row in upserted_rows],
            "deleted_recommendation_ids": [row["recommendation_id"] for row in deleted_rows],
            "strategy_version": strategy_version,
            "generated_at": generated_at,
            "count": len(upserted_rows),
        }
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="replace_priority_recommendations",
            target_ref=f"kanban_priorities:{scope_id}",
            item_id="",
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=_hash_json_payload(audit_metadata),
            metadata=audit_metadata,
        )
        gen = _kanban_table_sync_gen(conn, "kanban-priority-recommendations")
        for row in upserted_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_priority_recommendations",
                row["recommendation_id"],
                dict(row),
                gen,
            )
        for row in deleted_rows:
            enqueue_for_all_peers(
                conn,
                "DELETE",
                "kanban_priority_recommendations",
                row["recommendation_id"],
                {},
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        payload = _work_priority_recommendations_payload(conn, scope_id=scope_id, limit=10)
        payload["audit"] = {
            "audit_id": audit_id,
            "actor": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
        }
        return payload


@router.post("/kanban/items")
async def create_work_item(body: WorkItemCreateRequest) -> dict[str, Any]:
    return await _run_personal_sync_work(_create_work_item_sync, body)


def _create_work_item_sync(body: WorkItemCreateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        payload = _work_item_payload(conn, body)
        if _work_request_is_kanban_idle_worker(meta):
            automation_meta = (
                payload["provenance"].get("automation")
                if isinstance(payload.get("provenance"), dict)
                and isinstance(payload["provenance"].get("automation"), dict)
                else {}
            )
            guarded_item_ids = [
                payload["parent_item_id"],
                automation_meta.get("source_item_id"),
            ]
            if any(
                _work_item_automation_excluded(conn, guarded_id) for guarded_id in guarded_item_ids
            ):
                _raise_work_automation_excluded(
                    operation="create_work_item",
                    item_id=payload["parent_item_id"] or payload["item_id"],
                    meta=meta,
                )
        blocker_payload = None
        if payload["state_id"] == "blocked" and _work_request_requires_blocked_leaf_guard(meta):
            blocker_payload = _work_blocker_payload_from_request(
                body,
                item_title=payload["title"],
                item_body=payload["body_excerpt"],
                default_title=f"Blocked: {payload['title']}",
            )
            if blocker_payload is None:
                raise _work_blocked_leaf_guard_error(
                    item_id=payload["item_id"],
                    operation="create_work_item",
                    actor=meta["actor"],
                    source_surface=meta["source_surface"],
                )
        item_row, audit_row = _insert_work_item(
            conn, payload, action="create_work_item", audit_id=audit_id, now=now
        )
        if item_row["parent_item_id"] != payload["parent_item_id"]:
            raise RuntimeError(
                "Kanban create invariant failed: "
                f"{payload['item_id']} parent={item_row['parent_item_id']!r} "
                f"expected={payload['parent_item_id']!r}"
            )
        if int(item_row["depth"] or 0) != int(payload["depth"] or 0):
            raise RuntimeError(
                "Kanban create invariant failed: "
                f"{payload['item_id']} depth={item_row['depth']!r} "
                f"expected={payload['depth']!r}"
            )
        blocker_row = None
        blocker_audit_row = None
        if blocker_payload is not None:
            blocker_id = f"blocker-{payload['item_id']}-preprocessing"
            blocker_row, blocker_audit_row = _upsert_work_blocker_locked(
                conn,
                item=item_row,
                blocker_id=blocker_id,
                title=blocker_payload["title"],
                body=blocker_payload["body"],
                status="open",
                blocked_by_ref=blocker_payload["blocked_by_ref"]
                or f"kanban_items:{payload['item_id']}",
                meta=meta,
                now=now,
                action="create_work_item_blocker",
                provenance_extra={
                    "schema": KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA,
                    "source_item_id": payload["provenance"]
                    .get("automation", {})
                    .get("source_item_id", ""),
                    "marker_id": payload["provenance"]
                    .get("automation", {})
                    .get(
                        "marker_id",
                        "",
                    ),
                    "reason": payload["provenance"]
                    .get("automation", {})
                    .get(
                        "reason",
                        "",
                    ),
                },
                audit_metadata={
                    "schema": KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA,
                    "created_with_item": True,
                    "item_state_id": item_row["state_id"],
                    "rollback": {
                        "schema": "xarta.kanban.audit.rollback_recipe.v1",
                        "operation": "resolve_created_blocker",
                        "blocker_id": blocker_id,
                        "preconditions": [
                            "blocked leaf invariant has an alternate open blocker or the item has moved out of blocked",
                        ],
                    },
                },
            )
        gen = _kanban_table_sync_gen(conn, "kanban-item")
        enqueue_for_all_peers(
            conn, "UPDATE", "kanban_items", payload["item_id"], dict(item_row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        if blocker_row is not None and blocker_audit_row is not None:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                blocker_row["blocker_id"],
                dict(blocker_row),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                blocker_audit_row["audit_id"],
                blocker_audit_row,
                gen,
            )
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
        "created_blocker": _row_to_work_blocker(blocker_row) if blocker_row is not None else None,
        "audit": {"audit_id": audit_id, "action": "create_work_item", "result": "ok"},
    }


@router.patch("/kanban/items/{item_id}")
async def update_work_item(item_id: str, body: WorkItemUpdateRequest) -> dict[str, Any]:
    return await _run_personal_sync_work(_update_work_item_sync, item_id, body)


def _update_work_item_sync(item_id: str, body: WorkItemUpdateRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        state = _require_work_state(conn, body.state_id or existing["state_id"])
        _assert_agent_completion_not_blocked(
            conn,
            item_id=item_id,
            existing=existing,
            target_state=state,
            meta=meta,
        )
        _assert_agent_leaf_doing_has_active_session(
            conn,
            item_id=item_id,
            existing=existing,
            target_state=state,
            meta=meta,
        )
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
        existing_automation_excluded = (
            bool(existing["automation_excluded"])
            if "automation_excluded" in existing.keys()
            else False
        )
        automation_excluded = (
            existing_automation_excluded
            if body.automation_excluded is None
            else bool(body.automation_excluded)
        )
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
            "automation_excluded": automation_excluded,
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
        blocker_payload = None
        if (
            payload["state_id"] == "blocked"
            and _work_request_requires_blocked_leaf_guard(meta)
            and not _work_item_has_non_archived_children(conn, item_id)
            and _work_open_blocker_count(conn, item_id) <= 0
        ):
            blocker_payload = _work_blocker_payload_from_request(
                body,
                item_title=payload["title"],
                item_body=payload["body_excerpt"],
                default_title=f"Blocked: {payload['title']}",
            )
            if blocker_payload is None:
                raise _work_blocked_leaf_guard_error(
                    item_id=item_id,
                    operation="update_work_item",
                    actor=meta["actor"],
                    source_surface=meta["source_surface"],
                )
        item_row = _kanban_write_store(conn).update_item_row(item_id, payload, now=now)
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
        blocker_row = None
        blocker_audit_row = None
        if blocker_payload is not None:
            blocker_id = f"blocker-{item_id}-blocked-leaf-guard"
            blocker_row, blocker_audit_row = _upsert_work_blocker_locked(
                conn,
                item=item_row,
                blocker_id=blocker_id,
                title=blocker_payload["title"],
                body=blocker_payload["body"],
                status="open",
                blocked_by_ref=blocker_payload["blocked_by_ref"] or f"kanban_items:{item_id}",
                meta=meta,
                now=now,
                action="create_blocked_leaf_guard_blocker",
                provenance_extra={
                    "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                    "created_by_guard": "update_work_item",
                },
                audit_metadata={
                    "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                    "created_by_guard": "update_work_item",
                    "item_state_id": item_row["state_id"],
                },
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
                "automation_excluded": bool(item_row["automation_excluded"])
                if "automation_excluded" in item_row.keys()
                else False,
                "kanban_project_document": project_document,
                "lane_order": order_ids,
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_items", item_id, dict(item_row), gen)
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        if blocker_row is not None and blocker_audit_row is not None:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                blocker_row["blocker_id"],
                dict(blocker_row),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                blocker_audit_row["audit_id"],
                blocker_audit_row,
                gen,
            )
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
        "created_blocker": _row_to_work_blocker(blocker_row) if blocker_row is not None else None,
        "audit": {"audit_id": audit_id, "action": "update_work_item", "result": "ok"},
    }


@router.put("/kanban/items/{item_id}/detail")
async def update_work_item_detail_document(
    item_id: str, body: WorkItemDetailDocumentUpdateRequest
) -> dict[str, Any]:
    return await _run_personal_sync_work(_update_work_item_detail_document_sync, item_id, body)


def _update_work_item_detail_document_sync(
    item_id: str, body: WorkItemDetailDocumentUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_body = _normalise_markdown_document_body(body.body)
    source_hash = _hash_json_payload({"item_id": item_id, "body": clean_body})
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, item_id)
        document = store.write_item_detail_document(
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
        gen = _kanban_table_sync_gen(conn, "kanban-item-detail")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item_id": item_id,
        "detail_document": document,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "update_work_item_detail", "result": "ok"},
    }


@router.put("/kanban/items/{item_id}/review")
async def update_work_item_review_document(
    item_id: str, body: WorkItemDetailDocumentUpdateRequest
) -> dict[str, Any]:
    return await _run_personal_sync_work(_update_work_item_review_document_sync, item_id, body)


def _update_work_item_review_document_sync(
    item_id: str, body: WorkItemDetailDocumentUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_body = _normalise_markdown_document_body(body.body)
    source_hash = _hash_json_payload({"item_id": item_id, "body": clean_body})
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, item_id)
        document = store.write_item_review_document(
            item_id,
            clean_body,
            actor=meta["actor"],
            now=now,
        )
        image_associations = _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=clean_body,
            document_type="item-review",
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
            action="update_work_item_review",
            target_ref=f"kanban_items:{item_id}:review",
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
                "body_hash": document["metadata"].get("body_hash") or "",
                "document_updated_at": document["updated_at"],
                "rich_doc_image_count": len(image_associations.get("images", [])),
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item-review")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item_id": item_id,
        "review_document": document,
        "image_associations": image_associations,
        "audit": {"audit_id": audit_id, "action": "update_work_item_review", "result": "ok"},
    }


@router.post("/kanban/items/{item_id}/review/feedback")
async def append_work_item_review_feedback(
    item_id: str, body: WorkReviewFeedbackCaptureRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    clean_session_id = _clean_short_text(body.session_id, "", limit=180)
    if not clean_session_id:
        raise HTTPException(400, "Review feedback session_id is required")
    clean_child_item_id = _clean_short_text(body.child_item_id or "", "", limit=180)
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, clean_item_id)
        session_row = conn.execute(
            "SELECT * FROM kanban_agent_sessions WHERE session_id=?",
            (clean_session_id,),
        ).fetchone()
        if session_row is None:
            raise HTTPException(404, "Review feedback agent session not found")
        if clean_child_item_id:
            _work_item_or_404(conn, clean_child_item_id)
        entry = _work_review_feedback_entry(
            item_id=clean_item_id,
            body=body,
            meta=meta,
            now=now,
            agent_session=_row_to_work_agent_session(session_row),
        )
        existing_document = store.item_review_document(clean_item_id)
        clean_body = _append_work_review_feedback_body(existing_document["body"], entry)
        feedback_metadata = _work_review_feedback_metadata(
            existing_document.get("metadata") or {},
            entry,
        )
        document = store.write_item_review_document(
            clean_item_id,
            clean_body,
            actor=meta["actor"],
            now=now,
            metadata_extra=feedback_metadata,
        )
        review_marker_schedule = _schedule_work_review_processor_marker_for_document(
            conn,
            item_id=clean_item_id,
            document=document,
            meta=meta,
            now=now,
            reason="operator_feedback_captured",
            metadata={
                "trigger": "operator_feedback_capture",
                "feedback_id": entry["feedback_id"],
                "feedback_hash": entry["feedback_hash"],
                "capture_source": entry["capture_source"],
                "session_id": entry["session_id"],
            },
        )
        image_associations = _associate_rich_doc_images_for_document(
            domain="kanban",
            markdown=clean_body,
            document_type="item-review",
            document_id=clean_item_id,
            item_id=clean_item_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
        )
        source_hash = _hash_json_payload(
            {
                "item_id": clean_item_id,
                "feedback_entry": {key: value for key, value in entry.items() if key != "feedback"},
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="append_work_item_review_feedback",
            target_ref=f"kanban_items:{clean_item_id}:review:operator_feedback:{entry['feedback_id']}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "schema": KANBAN_REVIEW_FEEDBACK_SCHEMA,
                "feedback_id": entry["feedback_id"],
                "feedback_hash": entry["feedback_hash"],
                "capture_source": entry["capture_source"],
                "session_id": entry["session_id"],
                "affected_item_id": clean_item_id,
                "file_ref": document["file_ref"],
                "body_hash": document["metadata"].get("body_hash") or "",
                "document_updated_at": document["updated_at"],
                "rich_doc_image_count": len(image_associations.get("images", [])),
                "review_processor_marker_action": review_marker_schedule["action"],
                "review_processor_marker_queued": bool(review_marker_schedule["queued"]),
                "review_processor_marker_id": (
                    review_marker_schedule["marker_row"]["marker_id"]
                    if review_marker_schedule.get("marker_row") is not None
                    else ""
                ),
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item-review-feedback")
        if review_marker_schedule.get("queued") and review_marker_schedule.get("marker_row"):
            marker_row = review_marker_schedule["marker_row"]
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "item_id": clean_item_id,
        "feedback_entry": entry,
        "review_document": document,
        "review_processor": _row_to_work_review_processor_schedule(review_marker_schedule),
        "image_associations": image_associations,
        "audit": {
            "audit_id": audit_id,
            "action": "append_work_item_review_feedback",
            "result": "ok",
        },
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
        store = _kanban_write_store(conn)
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
        hash_row = {
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
        source_hash = _hash_json_payload({**hash_row, "body": clean_body})
        discussion_row = store.create_discussion_row(
            {
                "discussion_id": clean_discussion_id,
                "item_id": clean_item_id,
                "author": author,
                "body_excerpt": body_excerpt,
                "status": status,
                "search_text": search_text,
                "search_metadata": search_metadata,
                "vector_index_key": vector_key,
                "provenance": provenance,
                "created_at": now,
                "updated_at": now,
            }
        )
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
        discussion_row = store.update_discussion_provenance(
            clean_discussion_id,
            provenance=provenance,
        )
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
        gen = _kanban_table_sync_gen(conn, "kanban-discussion")
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
        store = _kanban_write_store(conn)
        existing = store.discussion_row(clean_discussion_id)
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
        discussion_row = store.update_discussion_row(
            clean_discussion_id,
            {
                "author": author,
                "body_excerpt": body_excerpt,
                "status": status,
                "search_text": search_text,
                "search_metadata": search_metadata,
                "vector_index_key": vector_key,
                "provenance": provenance,
            },
            now=now,
        )
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
        discussion_row = store.update_discussion_provenance(
            clean_discussion_id,
            provenance=provenance,
        )
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
        gen = _kanban_table_sync_gen(conn, "kanban-discussion")
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
        store = _kanban_write_store(conn)
        existing = store.discussion_row(clean_discussion_id)
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
        store.delete_discussion_row(clean_discussion_id)
        with suppress(OSError):
            _kanban_discussion_path(conn, existing["item_id"], clean_discussion_id).unlink()
        gen = _kanban_table_sync_gen(conn, "kanban-discussion")
        enqueue_for_all_peers(conn, "DELETE", "kanban_discussions", clean_discussion_id, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        deleted = _row_to_work_discussion(existing, conn)
    return {
        "ok": True,
        "discussion_id": clean_discussion_id,
        "deleted_discussion": deleted,
        "audit": {"audit_id": audit_id, "action": "delete_work_discussion", "result": "ok"},
    }


def _work_subtree_contains(conn: Any, *, ancestor_item_id: str, item_id: str) -> bool:
    if ancestor_item_id == item_id:
        return True
    row = conn.execute(
        """
        WITH RECURSIVE descendants(item_id) AS (
            SELECT item_id FROM kanban_items WHERE item_id=?
            UNION ALL
            SELECT w.item_id
            FROM kanban_items w
            JOIN descendants ON w.parent_item_id = descendants.item_id
            WHERE w.status != 'archived'
        )
        SELECT 1 FROM descendants WHERE item_id=? LIMIT 1
        """,
        (ancestor_item_id, item_id),
    ).fetchone()
    return row is not None


def _work_preprocessing_scoped_decomposition_reparent(
    *,
    item_id: str,
    target_parent_item_id: str,
    source_item_id: str,
    marker_id: str,
    actor: str,
    request_id: str,
    run_id: str,
    reason: str,
    operation_kind: str,
    decision_id: str = "",
    confirmed_pass_item_ids: list[str] | None = None,
) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    clean_target_parent = _clean_short_text(target_parent_item_id, "", limit=180)
    clean_source_item_id = _clean_short_text(source_item_id, "", limit=180)
    clean_marker_id = _clean_short_text(marker_id, "", limit=180)
    clean_request_id = _clean_short_text(request_id, "", limit=180)
    clean_run_id = _clean_short_text(run_id, "", limit=180)
    clean_reason = _clean_short_text(reason, "", limit=800)
    clean_operation_kind = _clean_short_text(operation_kind, "", limit=120)
    if not all(
        [
            clean_item_id,
            clean_target_parent,
            clean_source_item_id,
            clean_marker_id,
            clean_request_id,
            clean_run_id,
            clean_reason,
            clean_operation_kind,
        ]
    ):
        raise HTTPException(
            400,
            {
                "error": "kanban_preprocessing_scoped_move_missing_context",
                "message": (
                    "Scoped preprocessing decomposition moves require item, target parent, "
                    "source item, marker, request/run ids, reason, and operation kind."
                ),
            },
        )
    if clean_item_id == clean_source_item_id:
        raise HTTPException(
            403,
            {
                "error": "kanban_preprocessing_cannot_move_source_item",
                "item_id": clean_item_id,
                "source_item_id": clean_source_item_id,
            },
        )
    meta = {
        "actor": _clean_short_text(actor, "kanban-idle-worker", limit=160),
        "source_surface": "kanban-automation-idle-worker",
        "request_id": clean_request_id,
        "run_id": clean_run_id,
    }
    now = _utc_now_iso()
    with get_conn() as conn:
        _kanban_begin_write_transaction(conn)
        source_item = _work_item_or_404(conn, clean_source_item_id)
        moving_item = _work_item_or_404(conn, clean_item_id)
        _work_item_or_404(conn, clean_target_parent)
        for guarded_item_id in (clean_source_item_id, clean_item_id, clean_target_parent):
            if _work_item_automation_excluded(conn, guarded_item_id):
                _raise_work_automation_excluded(
                    operation="preprocessing_decomposition_reparent_item",
                    item_id=guarded_item_id,
                    meta=meta,
                )
        classification = _work_preprocessing_source_classification(
            conn,
            source_item,
            detail=_work_item_detail_document(conn, clean_source_item_id),
            review=_work_item_review_document(conn, clean_source_item_id),
        )
        if classification["classification"] != "concrete_request":
            raise HTTPException(
                403,
                {
                    "error": "kanban_preprocessing_source_not_concrete_request",
                    "source_item_id": clean_source_item_id,
                    "classification": classification,
                },
            )
        confirmed = set(confirmed_pass_item_ids or [])
        moving_in_scope = (
            _work_subtree_contains(
                conn,
                ancestor_item_id=clean_source_item_id,
                item_id=clean_item_id,
            )
            or clean_item_id in confirmed
        )
        target_in_scope = _work_subtree_contains(
            conn,
            ancestor_item_id=clean_source_item_id,
            item_id=clean_target_parent,
        )
        if not moving_in_scope or not target_in_scope:
            raise HTTPException(
                403,
                {
                    "error": "kanban_preprocessing_scoped_move_outside_source_subtree",
                    "item_id": clean_item_id,
                    "target_parent_item_id": clean_target_parent,
                    "source_item_id": clean_source_item_id,
                    "moving_in_scope": moving_in_scope,
                    "target_in_scope": target_in_scope,
                },
            )
        new_depth = _work_parent_depth(conn, clean_target_parent, moving_item_id=clean_item_id)
        max_relative = _work_subtree_max_relative_depth(conn, clean_item_id)
        if new_depth + max_relative > KANBAN_DEPTH_LIMIT:
            raise HTTPException(400, "Kanban item depth limit exceeded")
        previous_lane_order = _work_lane_order_snapshot(
            conn,
            moving_item["parent_item_id"],
            moving_item["state_id"],
            moving_item["priority_id"],
        )
        store = _kanban_write_store(conn)
        store.update_item_parent_depth(
            clean_item_id,
            parent_item_id=clean_target_parent,
            depth=new_depth,
            now=now,
        )
        _recompute_work_child_depths(conn, clean_item_id, new_depth, now=now)
        moved_rows = store.subtree_rows(clean_item_id)
        item_row = next(row for row in moved_rows if row["item_id"] == clean_item_id)
        order_ids, order_sync_changes = _ensure_work_item_lane_order(
            conn,
            clean_item_id,
            prefer_top_if_new=True,
            now=now,
            meta=meta,
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=f"audit-{uuid.uuid4().hex}",
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="preprocessing_decomposition_reparent_item",
            target_ref=f"kanban_items:{clean_item_id}",
            item_id=clean_item_id,
            parent_item_id=clean_target_parent,
            created_at=now,
            request_id=clean_request_id,
            run_id=clean_run_id,
            result="ok",
            source_hash=item_row["source_hash"],
            metadata={
                "schema": KANBAN_PREPROCESSING_DECOMPOSITION_MOVE_SCHEMA,
                "source_item_id": clean_source_item_id,
                "marker_id": clean_marker_id,
                "decision_id": _clean_short_text(decision_id, "", limit=180),
                "reason": clean_reason,
                "operation_kind": clean_operation_kind,
                "classification": classification,
                "previous": {
                    "parent_item_id": moving_item["parent_item_id"] or "",
                    "depth": int(moving_item["depth"] or 0),
                    "state_id": moving_item["state_id"],
                    "sort_order": int(moving_item["sort_order"] or 0),
                    "lane_order": previous_lane_order,
                },
                "new": {
                    "parent_item_id": item_row["parent_item_id"] or "",
                    "depth": int(item_row["depth"] or 0),
                    "state_id": item_row["state_id"],
                    "sort_order": int(item_row["sort_order"] or 0),
                    "lane_order": order_ids,
                },
                "rollback": {
                    "schema": "xarta.kanban.audit.rollback_recipe.v1",
                    "operation": "move_item",
                    "item_id": clean_item_id,
                    "parent_item_id": moving_item["parent_item_id"] or "",
                    "state_id": moving_item["state_id"],
                    "sort_order": int(moving_item["sort_order"] or 0),
                    "endpoint": f"/api/v1/personal/kanban/items/{clean_item_id}/move",
                    "method": "POST",
                    "preconditions": [
                        "item still represents the same moved card",
                        "operator confirms this scoped decomposition move should be rolled back",
                    ],
                },
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-preprocessing-decomposition-move")
        for row in moved_rows:
            enqueue_for_all_peers(conn, "UPDATE", "kanban_items", row["item_id"], dict(row), gen)
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_audit_log",
            audit_row["audit_id"],
            audit_row,
            gen,
        )
    return {
        "ok": True,
        "schema": KANBAN_PREPROCESSING_DECOMPOSITION_MOVE_SCHEMA,
        "item": _row_to_work_item(item_row),
        "source_item_id": clean_source_item_id,
        "target_parent_item_id": clean_target_parent,
        "audit": {
            "audit_id": audit_row["audit_id"],
            "action": "preprocessing_decomposition_reparent_item",
            "result": "ok",
        },
    }


@router.post("/kanban/items/{item_id}/move")
async def move_work_item(item_id: str, body: WorkItemMoveRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        state = _require_work_state(conn, body.state_id or existing["state_id"])
        _assert_agent_completion_not_blocked(
            conn,
            item_id=item_id,
            existing=existing,
            target_state=state,
            meta=meta,
        )
        _assert_agent_leaf_doing_has_active_session(
            conn,
            item_id=item_id,
            existing=existing,
            target_state=state,
            meta=meta,
        )
        raw_fields_set = getattr(body, "model_fields_set", None)
        if raw_fields_set is None:
            raw_fields_set = getattr(body, "__fields_set__", set())
        fields_set = set(raw_fields_set)
        if "parent_item_id" in fields_set:
            new_parent = _clean_short_text(body.parent_item_id, "", limit=180) or None
        else:
            new_parent = existing["parent_item_id"]
        parent_changed = (new_parent or "") != (existing["parent_item_id"] or "")
        if _work_request_is_kanban_idle_worker(meta):
            guarded_item_ids = [item_id]
            if new_parent:
                guarded_item_ids.append(new_parent)
            if any(
                _work_item_automation_excluded(conn, guarded_id) for guarded_id in guarded_item_ids
            ):
                _raise_work_automation_excluded(
                    operation="move_work_item",
                    item_id=item_id,
                    meta=meta,
                )
        if parent_changed and _work_request_is_kanban_idle_worker(meta):
            raise HTTPException(
                403,
                {
                    "error": "kanban_idle_worker_parent_change_forbidden",
                    "detail": (
                        "Kanban automation idle worker may change item state/lane, "
                        "but must not reparent existing cards."
                    ),
                    "item_id": item_id,
                    "from_parent_item_id": existing["parent_item_id"] or "",
                    "to_parent_item_id": new_parent or "",
                    "source_surface": meta["source_surface"],
                    "actor": meta["actor"],
                },
            )
        new_depth = _work_parent_depth(conn, new_parent, moving_item_id=item_id)
        max_relative = _work_subtree_max_relative_depth(conn, item_id)
        if new_depth + max_relative > KANBAN_DEPTH_LIMIT:
            raise HTTPException(400, "Kanban item depth limit exceeded")
        blocker_payload = None
        if (
            state["state_id"] == "blocked"
            and _work_request_requires_blocked_leaf_guard(meta)
            and not _work_item_has_non_archived_children(conn, item_id)
            and _work_open_blocker_count(conn, item_id) <= 0
        ):
            blocker_payload = _work_blocker_payload_from_request(
                body,
                item_title=existing["title"],
                item_body=existing["body_excerpt"],
                default_title=f"Blocked: {existing['title']}",
            )
            if blocker_payload is None:
                raise _work_blocked_leaf_guard_error(
                    item_id=item_id,
                    operation="move_work_item",
                    actor=meta["actor"],
                    source_surface=meta["source_surface"],
                )
        previous_lane_order = _work_lane_order_snapshot(
            conn,
            existing["parent_item_id"],
            existing["state_id"],
            existing["priority_id"],
        )
        _kanban_write_store(conn).move_item_row(
            item_id,
            parent_item_id=new_parent,
            state_id=state["state_id"],
            status=_work_status_for_state(state),
            depth=new_depth,
            sort_order=int(
                body.sort_order if body.sort_order is not None else existing["sort_order"]
            ),
            now=now,
        )
        _recompute_work_child_depths(conn, item_id, new_depth, now=now)
        moved_rows = _kanban_write_store(conn).subtree_rows(item_id)
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
        blocker_row = None
        blocker_audit_row = None
        if blocker_payload is not None:
            blocker_id = f"blocker-{item_id}-blocked-leaf-guard"
            blocker_row, blocker_audit_row = _upsert_work_blocker_locked(
                conn,
                item=item_row,
                blocker_id=blocker_id,
                title=blocker_payload["title"],
                body=blocker_payload["body"],
                status="open",
                blocked_by_ref=blocker_payload["blocked_by_ref"] or f"kanban_items:{item_id}",
                meta=meta,
                now=now,
                action="create_blocked_leaf_guard_blocker",
                provenance_extra={
                    "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                    "created_by_guard": "move_work_item",
                },
                audit_metadata={
                    "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                    "created_by_guard": "move_work_item",
                    "item_state_id": item_row["state_id"],
                },
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
                "previous": {
                    "parent_item_id": existing["parent_item_id"] or "",
                    "depth": int(existing["depth"] or 0),
                    "state_id": existing["state_id"],
                    "sort_order": int(existing["sort_order"] or 0),
                    "lane_order": previous_lane_order,
                },
                "new": {
                    "parent_item_id": item_row["parent_item_id"] or "",
                    "depth": int(item_row["depth"] or 0),
                    "state_id": item_row["state_id"],
                    "sort_order": int(item_row["sort_order"] or 0),
                    "lane_order": order_ids,
                },
                "rollback": {
                    "schema": "xarta.kanban.audit.rollback_recipe.v1",
                    "operation": "move_item",
                    "item_id": item_id,
                    "parent_item_id": existing["parent_item_id"] or "",
                    "state_id": existing["state_id"],
                    "sort_order": int(existing["sort_order"] or 0),
                    "endpoint": f"/api/v1/personal/kanban/items/{item_id}/move",
                    "method": "POST",
                    "preconditions": [
                        "item still represents the same card",
                        "operator confirms this move should be rolled back",
                    ],
                },
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item")
        for row in moved_rows:
            enqueue_for_all_peers(conn, "UPDATE", "kanban_items", row["item_id"], dict(row), gen)
        for action, row_id, row_data in order_sync_changes:
            enqueue_for_all_peers(conn, action, "kanban_item_order_edges", row_id, row_data, gen)
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        if blocker_row is not None and blocker_audit_row is not None:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                blocker_row["blocker_id"],
                dict(blocker_row),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                blocker_audit_row["audit_id"],
                blocker_audit_row,
                gen,
            )
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
        "created_blocker": _row_to_work_blocker(blocker_row) if blocker_row is not None else None,
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
        gen = _kanban_table_sync_gen(conn, "kanban-item-order")
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
    return await _run_personal_sync_work(_archive_work_item_sync, item_id, body)


def _archive_work_item_sync(item_id: str, body: WorkItemActionRequest) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    with get_conn() as conn:
        existing = _work_item_or_404(conn, item_id)
        item_row = _kanban_write_store(conn).archive_item_row(item_id, archived_at=now)
        cancelled_marker_rows = _cancel_work_review_processor_markers(
            conn,
            item_ids=[item_id],
            meta=meta,
            now=now,
            reason="item_archived",
            metadata={
                "archived_at": now,
                "archived_item_id": item_id,
            },
        )
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
            metadata={
                "archived_at": now,
                "cancelled_review_marker_count": len(cancelled_marker_rows),
                "cancelled_review_marker_ids": [row["marker_id"] for row in cancelled_marker_rows],
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_items", item_id, dict(item_row), gen)
        for marker_row in cancelled_marker_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        cancelled_markers = [
            _row_to_work_review_processor_marker(row) for row in cancelled_marker_rows
        ]
    return {
        "ok": True,
        "item": _row_to_work_item(item_row),
        "cancelled_review_markers": cancelled_markers,
        "audit": {"audit_id": audit_id, "action": "archive_work_item", "result": "ok"},
    }


@router.get("/kanban/items/{item_id}/rollup")
async def get_work_item_rollup(item_id: str) -> dict[str, Any]:
    return await _run_personal_sync_work(_get_work_item_rollup_sync, item_id)


def _get_work_item_rollup_sync(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        try:
            with _kanban_read_store(conn) as store:
                return {"ok": True, "item_id": item_id, "rollup": store.rollup(item_id)}
        except (KanbanItemNotFound, KanbanItemCycleError) as exc:
            _raise_kanban_store_error(exc)


@router.get("/kanban/rollups")
async def get_work_item_rollups(
    item_id: Annotated[list[str] | None, Query()] = None,
    show_test_entries: Annotated[bool | None, Query()] = None,
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _get_work_item_rollups_sync,
        item_id or [],
        show_test_entries,
    )


def _clean_work_rollup_item_ids(item_ids: list[str]) -> list[str]:
    clean_ids: list[str] = []
    seen: set[str] = set()
    for raw_item_id in item_ids:
        for part in str(raw_item_id or "").split(","):
            clean_item_id = _clean_short_text(part, "", limit=180)
            if not clean_item_id or clean_item_id in seen:
                continue
            seen.add(clean_item_id)
            clean_ids.append(clean_item_id)
    if len(clean_ids) > 200:
        raise HTTPException(400, "Kanban rollup batch is limited to 200 item_id values")
    return clean_ids


def _get_work_item_rollups_sync(
    item_ids: list[str],
    show_test_entries: bool | None = None,
) -> dict[str, Any]:
    clean_ids = _clean_work_rollup_item_ids(item_ids)
    effective_show_test_entries = True if show_test_entries is None else bool(show_test_entries)
    with get_conn() as conn:
        try:
            with _kanban_read_store(conn) as store:
                with timing.span(
                    "kanban.rollups.batch",
                    item_count=len(clean_ids),
                    show_test_entries=effective_show_test_entries,
                ):
                    rollups = store.rollups(
                        clean_ids,
                        show_test_entries=effective_show_test_entries,
                    )
        except (KanbanItemNotFound, KanbanItemCycleError) as exc:
            _raise_kanban_store_error(exc)
    return {"ok": True, "count": len(rollups), "rollups": rollups}


def _work_audit_rollback_recipe(row: Any) -> dict[str, Any]:
    metadata = _json_value(row["metadata_json"], {})
    rollback = metadata.get("rollback") if isinstance(metadata, dict) else None
    if isinstance(rollback, dict) and rollback.get("operation"):
        return rollback
    action = row["action"]
    if action == "move_work_item":
        return {
            "schema": "xarta.kanban.audit.rollback_recipe.v1",
            "operation": "move_item",
            "item_id": row["item_id"],
            "parent_item_id": metadata.get("from_parent_item_id") or "",
            "state_id": (metadata.get("previous") or {}).get("state_id")
            if isinstance(metadata.get("previous"), dict)
            else "",
            "sort_order": (metadata.get("previous") or {}).get("sort_order")
            if isinstance(metadata.get("previous"), dict)
            else None,
            "endpoint": f"/api/v1/personal/kanban/items/{row['item_id']}/move",
            "method": "POST",
        }
    if action in {"create_work_item", "create_work_issue", "create_work_todo"}:
        return {
            "schema": "xarta.kanban.audit.rollback_recipe.v1",
            "operation": "archive_created_item",
            "item_id": row["item_id"],
            "endpoint": f"/api/v1/personal/kanban/items/{row['item_id']}/archive",
            "method": "POST",
        }
    return {}


def _row_to_work_audit_export(row: Any) -> dict[str, Any]:
    metadata = _json_value(row["metadata_json"], {})
    return {
        "audit_id": row["audit_id"],
        "actor": row["actor"],
        "source_surface": row["source_surface"],
        "action": row["action"],
        "target_ref": row["target_ref"],
        "item_id": row["item_id"],
        "parent_item_id": row["parent_item_id"],
        "created_at": row["created_at"],
        "request_id": row["request_id"],
        "run_id": row["run_id"],
        "result": row["result"],
        "source_hash": row["source_hash"],
        "metadata": metadata,
        "rollback": _work_audit_rollback_recipe(row),
    }


@router.get("/kanban/audit")
async def list_work_audit_log(
    item_id: str = "",
    run_id: str = "",
    action: str = "",
    target_ref: str = "",
    actor: str = "",
    source_surface: str = "",
    since: str = "",
    until: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    clean_limit = max(1, min(int(limit or 500), 5000))
    clauses: list[str] = []
    args: list[Any] = []
    filters = {
        "item_id": _clean_short_text(item_id, "", limit=180),
        "run_id": _clean_short_text(run_id, "", limit=180),
        "action": _clean_short_text(action, "", limit=180),
        "target_ref": _clean_short_text(target_ref, "", limit=240),
        "actor": _clean_short_text(actor, "", limit=180),
        "source_surface": _clean_short_text(source_surface, "", limit=180),
        "since": _clean_short_text(since, "", limit=80),
        "until": _clean_short_text(until, "", limit=80),
    }
    for key in ("item_id", "run_id", "action", "target_ref", "actor", "source_surface"):
        if filters[key]:
            clauses.append(f"{key}=?")
            args.append(filters[key])
    if filters["since"]:
        clauses.append("created_at>=?")
        args.append(filters["since"])
    if filters["until"]:
        clauses.append("created_at<=?")
        args.append(filters["until"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM kanban_audit_log
            {where}
            ORDER BY created_at DESC, audit_id DESC
            LIMIT ?
            """,
            (*args, clean_limit),
        ).fetchall()
    return {
        "ok": True,
        "schema": "xarta.kanban.audit_export.v1",
        "filters": filters,
        "count": len(rows),
        "audit": [_row_to_work_audit_export(row) for row in rows],
    }


def _work_blocked_leaf_repair_blocker_id(item_id: str) -> str:
    digest = hashlib.sha256(f"blocked-leaf-invariant\n{item_id}".encode("utf-8")).hexdigest()
    return f"blocker-blocked-leaf-{digest[:24]}"


def _work_blocked_leaf_invariant_rows(
    conn: Any,
    *,
    item_id: str = "",
    include_test_entries: bool = True,
    limit: int = 500,
) -> list[Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    args: list[Any] = []
    scope_join = ""
    scope_where = ""
    if clean_item_id:
        _work_item_or_404(conn, clean_item_id)
        scope_join = """
        WITH RECURSIVE scope(item_id) AS (
            SELECT item_id FROM kanban_items WHERE item_id=?
            UNION ALL
            SELECT child.item_id
            FROM kanban_items child
            JOIN scope ON child.parent_item_id = scope.item_id
            WHERE child.status != 'archived'
        )
        """
        scope_where = "AND item.item_id IN (SELECT item_id FROM scope)"
        args.append(clean_item_id)
    rows = conn.execute(
        f"""
        {scope_join}
        SELECT item.* FROM kanban_items item
        WHERE item.status != 'archived'
          AND item.state_id='blocked'
          {scope_where}
          AND NOT EXISTS (
              SELECT 1 FROM kanban_items child
              WHERE child.parent_item_id=item.item_id
                AND child.status != 'archived'
          )
          AND NOT EXISTS (
              SELECT 1 FROM kanban_blockers blocker
              WHERE blocker.item_id=item.item_id
                AND blocker.status NOT IN ('resolved', 'closed', 'done')
          )
        ORDER BY item.updated_at ASC, item.item_id
        LIMIT ?
        """,
        (*args, max(1, min(int(limit or 500), 5000))),
    ).fetchall()
    if include_test_entries:
        return rows
    visible, _hidden = _filter_work_test_rows(rows, False)
    return visible


def _work_blocked_leaf_repair_plan(conn: Any, row: Any) -> dict[str, Any]:
    body = str(row["body_excerpt"] or "")
    provenance = _json_value(row["provenance_json"], {})
    recent_audit = conn.execute(
        """
        SELECT * FROM kanban_audit_log
        WHERE item_id=? OR target_ref=?
        ORDER BY created_at DESC, audit_id DESC
        LIMIT 12
        """,
        (row["item_id"], f"kanban_items:{row['item_id']}"),
    ).fetchall()
    audit_surfaces = [audit["source_surface"] for audit in recent_audit]
    preprocessing_evidence = (
        body.startswith("Preprocessing child of ")
        or provenance.get("source_surface") == "kanban-automation-idle-worker"
        or "kanban-automation-idle-worker" in audit_surfaces
    )
    blocked_reason = _work_blocked_reason_from_text(body)
    lower_body = body.lower()
    dependency_evidence = any(
        term in lower_body
        for term in ("blocked", "operator decision", "operator input", "dependency", "waiting")
    )
    if preprocessing_evidence and (blocked_reason or dependency_evidence):
        classification = "preprocessing_blocked_child"
        action = "create_blocker"
        reason = blocked_reason or _body_excerpt(body, limit=1200)
        title = f"Preprocessing blocker/question: {row['title']}"
    elif not dependency_evidence:
        classification = "not_actually_blocked"
        action = "move_to_todo"
        reason = "Blocked lane has no blocker evidence in body/provenance/audit."
        title = ""
    else:
        classification = "ambiguous_blocked_leaf"
        action = "create_blocker"
        reason = blocked_reason or _body_excerpt(body, limit=1200)
        title = f"Blocked leaf needs visible blocker: {row['title']}"
    return {
        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
        "item": _row_to_work_item(row),
        "classification": classification,
        "action": action,
        "reason": reason,
        "blocker": {
            "blocker_id": _work_blocked_leaf_repair_blocker_id(row["item_id"]),
            "title": title,
            "body": reason,
            "blocked_by_ref": f"kanban_items:{row['item_id']}",
        }
        if action == "create_blocker"
        else None,
        "move": {
            "state_id": "todo",
            "reason": reason,
        }
        if action == "move_to_todo"
        else None,
        "evidence": {
            "preprocessing_evidence": preprocessing_evidence,
            "blocked_reason": blocked_reason,
            "dependency_evidence": dependency_evidence,
            "recent_audit": [_row_to_work_audit_export(audit) for audit in recent_audit],
        },
    }


@router.get("/kanban/automation/blocked-leaf-invariant/audit")
async def audit_work_blocked_leaf_invariant(
    item_id: str = "",
    include_test_entries: bool = True,
    limit: int = 500,
) -> dict[str, Any]:
    with get_conn() as conn:
        rows = _work_blocked_leaf_invariant_rows(
            conn,
            item_id=item_id,
            include_test_entries=include_test_entries,
            limit=limit,
        )
        findings = [_work_blocked_leaf_repair_plan(conn, row) for row in rows]
    return {
        "ok": True,
        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
        "item_id": _clean_short_text(item_id, "", limit=180),
        "include_test_entries": include_test_entries,
        "count": len(findings),
        "findings": findings,
    }


@router.post("/kanban/automation/blocked-leaf-invariant/repair")
async def repair_work_blocked_leaf_invariant(
    body: WorkBlockedLeafInvariantRepairRequest,
) -> dict[str, Any]:
    meta = _work_request_meta(body)
    now = _utc_now_iso()
    with get_conn() as conn:
        rows = _work_blocked_leaf_invariant_rows(
            conn,
            item_id=_clean_short_text(body.item_id, "", limit=180),
            include_test_entries=bool(body.include_test_entries),
            limit=body.max_items,
        )
        plans = [_work_blocked_leaf_repair_plan(conn, row) for row in rows]
        if not body.apply:
            return {
                "ok": True,
                "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                "applied": False,
                "count": len(plans),
                "plans": plans,
            }
        _kanban_begin_write_transaction(conn)
        repaired: list[dict[str, Any]] = []
        changed_blockers: list[tuple[Any, dict[str, Any]]] = []
        changed_items: list[tuple[Any, dict[str, Any]]] = []
        for plan in plans:
            item_id = plan["item"]["item_id"]
            item = _work_item_or_404(conn, item_id)
            if (
                item["state_id"] != "blocked"
                or _work_item_has_non_archived_children(conn, item_id)
                or _work_open_blocker_count(conn, item_id) > 0
            ):
                repaired.append({**plan, "applied": False, "skip_reason": "already_current"})
                continue
            if plan["action"] == "create_blocker":
                blocker_plan = plan["blocker"] or {}
                blocker_row, blocker_audit = _upsert_work_blocker_locked(
                    conn,
                    item=item,
                    blocker_id=blocker_plan.get("blocker_id"),
                    title=blocker_plan.get("title") or f"Blocked: {item['title']}",
                    body="\n\n".join(
                        part
                        for part in [
                            blocker_plan.get("body") or plan.get("reason") or "",
                            (
                                "Repair note: created because this leaf was in `blocked` "
                                "with zero open API-visible Blocker rows."
                            ),
                        ]
                        if part
                    ),
                    status="open",
                    blocked_by_ref=blocker_plan.get("blocked_by_ref") or f"kanban_items:{item_id}",
                    meta=meta,
                    now=now,
                    action="repair_blocked_leaf_missing_blocker",
                    provenance_extra={
                        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                        "repair_source_surface": meta["source_surface"],
                        "classification": plan["classification"],
                    },
                    audit_metadata={
                        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                        "classification": plan["classification"],
                        "source_evidence": plan["evidence"],
                        "before": {
                            "state_id": item["state_id"],
                            "open_blocker_count": 0,
                            "parent_item_id": item["parent_item_id"] or "",
                        },
                        "after": {
                            "state_id": item["state_id"],
                            "open_blocker_count": 1,
                            "blocker_id": blocker_plan.get("blocker_id"),
                        },
                        "rollback": {
                            "schema": "xarta.kanban.audit.rollback_recipe.v1",
                            "operation": "resolve_created_blocker",
                            "blocker_id": blocker_plan.get("blocker_id"),
                            "preconditions": [
                                "item is no longer blocked or has another open blocker",
                            ],
                        },
                    },
                )
                changed_blockers.append((blocker_row, blocker_audit))
                repaired.append({**plan, "applied": True, "blocker_id": blocker_row["blocker_id"]})
            elif plan["action"] == "move_to_todo":
                previous_lane_order = _work_lane_order_snapshot(
                    conn,
                    item["parent_item_id"],
                    item["state_id"],
                    item["priority_id"],
                )
                target_state = _require_work_state(conn, "todo")
                item_row = _kanban_write_store(conn).update_item_state(
                    item_id,
                    state_id="todo",
                    status=_work_status_for_state(target_state),
                    now=now,
                )
                order_ids, order_sync_changes = _ensure_work_item_lane_order(
                    conn,
                    item_id,
                    prefer_top_if_new=True,
                    now=now,
                    meta=meta,
                )
                audit_row = _write_work_audit(
                    conn,
                    audit_id=f"audit-{uuid.uuid4().hex}",
                    actor=meta["actor"],
                    source_surface=meta["source_surface"],
                    action="repair_blocked_leaf_move_to_todo",
                    target_ref=f"kanban_items:{item_id}",
                    item_id=item_id,
                    parent_item_id=item_row["parent_item_id"] or "",
                    created_at=now,
                    request_id=meta["request_id"],
                    run_id=meta["run_id"],
                    result="ok",
                    source_hash=item_row["source_hash"],
                    metadata={
                        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
                        "classification": plan["classification"],
                        "source_evidence": plan["evidence"],
                        "before": {
                            "state_id": "blocked",
                            "open_blocker_count": 0,
                            "lane_order": previous_lane_order,
                        },
                        "after": {
                            "state_id": "todo",
                            "lane_order": order_ids,
                        },
                        "rollback": {
                            "schema": "xarta.kanban.audit.rollback_recipe.v1",
                            "operation": "move_item",
                            "item_id": item_id,
                            "state_id": "blocked",
                            "endpoint": f"/api/v1/personal/kanban/items/{item_id}/move",
                            "method": "POST",
                        },
                    },
                )
                changed_items.append((item_row, audit_row))
                for order_action, row_id, row_data in order_sync_changes:
                    changed_items.append(
                        (
                            {"_order_action": order_action, "row_id": row_id, "row_data": row_data},
                            {},
                        )
                    )
                repaired.append({**plan, "applied": True, "state_id": "todo"})
        gen = _kanban_table_sync_gen(conn, "kanban-blocked-leaf-repair")
        for blocker_row, blocker_audit in changed_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                blocker_row["blocker_id"],
                dict(blocker_row),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                blocker_audit["audit_id"],
                blocker_audit,
                gen,
            )
        for item_row, item_audit in changed_items:
            if isinstance(item_row, dict) and "_order_action" in item_row:
                enqueue_for_all_peers(
                    conn,
                    item_row["_order_action"],
                    "kanban_item_order_edges",
                    item_row["row_id"],
                    item_row["row_data"],
                    gen,
                )
                continue
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_items",
                item_row["item_id"],
                dict(item_row),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                item_audit["audit_id"],
                item_audit,
                gen,
            )
        after_rows = _work_blocked_leaf_invariant_rows(
            conn,
            item_id=_clean_short_text(body.item_id, "", limit=180),
            include_test_entries=bool(body.include_test_entries),
            limit=body.max_items,
        )
    return {
        "ok": True,
        "schema": KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA,
        "applied": True,
        "before_count": len(plans),
        "after_count": len(after_rows),
        "repaired_count": len([entry for entry in repaired if entry.get("applied")]),
        "repairs": repaired,
    }


@router.get("/kanban/items/{item_id}/issues")
async def list_work_item_issues(
    item_id: str,
    scope: str = "local",
    view: str = "flat",
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _list_work_item_issues_sync,
        item_id,
        scope,
        view,
    )


def _list_work_item_issues_sync(
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
    return await _run_personal_sync_work(
        _list_work_item_todos_sync,
        item_id,
        scope,
        view,
    )


def _list_work_item_todos_sync(
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
    return await _run_personal_sync_work(_get_work_issue_sync, issue_id)


def _get_work_issue_sync(issue_id: str) -> dict[str, Any]:
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
    return await _run_personal_sync_work(_get_work_todo_sync, todo_id)


def _get_work_todo_sync(todo_id: str) -> dict[str, Any]:
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
    target_item_id = _kanban_item_id_from_share_ref(body.target_item_id) or _clean_short_text(
        body.target_item_id, "", limit=180
    )
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
        store = _kanban_write_store(conn)
        source = _work_item_or_404(conn, source_item_id)
        _work_item_or_404(conn, target_item_id)
        link_row = store.create_item_link_row(
            {
                "link_id": link_id,
                "source_item_id": source_item_id,
                "target_item_id": target_item_id,
                "link_type": link_type,
                "metadata": metadata,
                "created_at": now,
                "updated_at": now,
            }
        )
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
        gen = _kanban_table_sync_gen(conn, "kanban-item-link")
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
    return await _run_personal_sync_work(_list_work_item_commits_sync, item_id)


def _list_work_item_commits_sync(item_id: str) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    with get_conn() as conn:
        try:
            with _kanban_read_store(conn) as store:
                read = store.item_detail(clean_item_id)
        except (KanbanItemNotFound, KanbanItemCycleError) as exc:
            _raise_kanban_store_error(exc)
        return {
            "ok": True,
            "item": _row_to_work_item(read.item),
            "count": len(read.commits),
            "commits": [_row_to_work_commit(row) for row in read.commits],
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
        store = _kanban_write_store(conn)
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
        provenance = {
            "commit_ref": _git_commit_ref(repo_full_name, sha),
            "recorded_by": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
            "run_id": meta["run_id"],
            "personal_git_commit_id": git_row["commit_id"] if git_row else "",
        }
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
            "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
            "created_at": now,
            "updated_at": now,
        }
        source_hash = _hash_json_payload(row)
        commit_row, updated_existing = store.upsert_item_commit_row(
            {
                "commit_link_id": link_id,
                "item_id": clean_item_id,
                "repo_full_name": repo_full_name,
                "sha": sha,
                "short_sha": short_sha,
                "html_url": html_url,
                "author_login": row["author_login"],
                "author_name": row["author_name"],
                "committed_at": row["committed_at"],
                "message_subject": row["message_subject"],
                "message_body": row["message_body"],
                "branch": row["branch"],
                "metadata": metadata,
                "provenance": provenance,
                "created_at": now,
                "updated_at": now,
            }
        )
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
                "upsert": "updated" if updated_existing else "inserted",
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-item-commit")
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


@router.get("/kanban/items/{item_id}/decisions")
async def list_work_item_review_decisions(
    item_id: str,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_list_work_item_review_decisions_sync, item_id, limit)


def _list_work_item_review_decisions_sync(
    item_id: str,
    limit: int = 50,
) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    with get_conn() as conn:
        with _kanban_read_connection(conn) as read_conn:
            item = _work_item_or_404(read_conn, clean_item_id)
            rows = read_conn.execute(
                """
                SELECT * FROM kanban_review_decisions
                WHERE item_id=?
                ORDER BY updated_at DESC, created_at DESC, decision_id
                LIMIT ?
                """,
                (clean_item_id, limit),
            ).fetchall()
            decisions = []
            for row in rows:
                commit_ids = _json_value(row["commit_link_ids_json"], [])
                commits = [
                    _row_to_work_commit(commit)
                    for commit in _work_decision_commit_rows(read_conn, commit_ids)
                ]
                decisions.append(_row_to_work_review_decision(row, commits))
            commit_link_health = _work_decision_commit_health(rows, read_conn)
        return {
            "ok": True,
            "item": _row_to_work_item(item),
            "count": len(decisions),
            "decisions": decisions,
            "commit_link_health": commit_link_health,
        }


@router.post("/kanban/items/{item_id}/decisions")
async def record_work_item_review_decision(
    item_id: str, body: WorkReviewDecisionCreateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    decision_id = (
        _clean_work_id(body.decision_id, "kanban-decision")
        if body.decision_id
        else _kanban_review_decision_id()
    )
    summary = _body_excerpt(body.summary, limit=8000)
    if not summary.strip():
        raise HTTPException(400, "Review Processor decision summary is required")
    affected_refs = _clean_event_list(body.affected_refs, limit=64)
    canonical_item_ref = f"kanban_items:{clean_item_id}"
    if canonical_item_ref not in affected_refs:
        affected_refs.insert(0, canonical_item_ref)
    proof_refs = _clean_event_list(body.proof_refs, limit=64)
    commit_link_ids = _clean_event_list(body.commit_link_ids, limit=64)
    status = _clean_review_decision_status(body.status)
    provider_mode = _clean_review_provider_mode(body.provider_mode)
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, clean_item_id)
        commit_rows = _work_decision_commit_rows(conn, commit_link_ids)
        found_commit_ids = {row["commit_link_id"] for row in commit_rows}
        missing_commit_ids = [
            commit_id for commit_id in commit_link_ids if commit_id not in found_commit_ids
        ]
        if missing_commit_ids:
            raise HTTPException(
                400,
                f"Decision references unknown Kanban commit links: {', '.join(missing_commit_ids[:5])}",
            )
        provenance = {
            "schema": KANBAN_REVIEW_DECISION_SCHEMA,
            "recorded_by": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
            "run_id": meta["run_id"],
        }
        row = {
            "decision_id": decision_id,
            "item_id": clean_item_id,
            "processor_kind": _clean_short_text(body.processor_kind, "review", limit=80),
            "decision_type": _clean_short_text(body.decision_type, "decision", limit=80),
            "title": _clean_short_text(body.title or "", "", limit=220),
            "summary": summary,
            "rationale": _body_excerpt(body.rationale, limit=12000),
            "affected_refs_json": json.dumps(affected_refs, ensure_ascii=True),
            "confidence": _clean_short_text(body.confidence, "", limit=80),
            "uncertainty": _body_excerpt(body.uncertainty, limit=4000),
            "proof_refs_json": json.dumps(proof_refs, ensure_ascii=True),
            "commit_link_ids_json": json.dumps(commit_link_ids, ensure_ascii=True),
            "status": status,
            "provider_mode": provider_mode,
            "source_hash": "",
            "metadata_json": json.dumps(
                dict(body.metadata or {}), ensure_ascii=True, sort_keys=True
            ),
            "provenance_json": json.dumps(provenance, ensure_ascii=True, sort_keys=True),
            "created_at": now,
            "updated_at": now,
        }
        row["source_hash"] = _hash_json_payload(
            {
                key: value
                for key, value in row.items()
                if key not in {"source_hash", "created_at", "updated_at"}
            }
        )
        existing = store.review_decision_row(decision_id)
        if existing:
            row["created_at"] = existing["created_at"]
        decision_row = store.upsert_review_decision_row(row)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="record_review_processor_decision",
            target_ref=f"kanban_review_decisions:{decision_id}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=row["source_hash"],
            metadata={
                "decision_id": decision_id,
                "status": status,
                "provider_mode": provider_mode,
                "commit_link_ids": commit_link_ids,
                "affected_refs": affected_refs,
                "upsert": "updated" if existing else "inserted",
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-review-decision")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_review_decisions",
            decision_id,
            dict(decision_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        decision = _row_to_work_review_decision(
            decision_row,
            [_row_to_work_commit(commit) for commit in commit_rows],
        )
    return {
        "ok": True,
        "decision": decision,
        "audit": {
            "audit_id": audit_id,
            "action": "record_review_processor_decision",
            "result": "ok",
        },
    }


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}


@dataclass(frozen=True)
class _EnvIntRangeConfig:
    env_name: str
    raw_value: str | None
    default: int
    minimum: int
    maximum: int
    effective: int
    source: str
    state: str
    error: str = ""

    @property
    def valid(self) -> bool:
        return self.state in {"default", "valid"}

    @property
    def clamped(self) -> bool:
        return self.state == "clamped"

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": "xarta.blueprints.env_range_config.v1",
            "env_name": self.env_name,
            "raw_value": self.raw_value,
            "default": self.default,
            "min": self.minimum,
            "max": self.maximum,
            "effective": self.effective,
            "source": self.source,
            "state": self.state,
            "valid": self.valid,
            "clamped": self.clamped,
            "error": self.error,
        }


def _env_int_range_config(
    name: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> _EnvIntRangeConfig:
    raw = os.environ.get(name)
    if raw is None:
        return _EnvIntRangeConfig(
            env_name=name,
            raw_value=None,
            default=default,
            minimum=minimum,
            maximum=maximum,
            effective=default,
            source="default",
            state="default",
        )
    clean_raw = raw.strip()
    try:
        parsed = int(clean_raw)
    except (TypeError, ValueError):
        return _EnvIntRangeConfig(
            env_name=name,
            raw_value=raw,
            default=default,
            minimum=minimum,
            maximum=maximum,
            effective=default,
            source="env",
            state="error",
            error="not_an_integer",
        )
    if parsed < minimum:
        return _EnvIntRangeConfig(
            env_name=name,
            raw_value=raw,
            default=default,
            minimum=minimum,
            maximum=maximum,
            effective=minimum,
            source="env",
            state="clamped",
            error="below_min",
        )
    if parsed > maximum:
        return _EnvIntRangeConfig(
            env_name=name,
            raw_value=raw,
            default=default,
            minimum=minimum,
            maximum=maximum,
            effective=maximum,
            source="env",
            state="clamped",
            error="above_max",
        )
    return _EnvIntRangeConfig(
        env_name=name,
        raw_value=raw,
        default=default,
        minimum=minimum,
        maximum=maximum,
        effective=parsed,
        source="env",
        state="valid",
    )


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _work_automation_current_node_id() -> str:
    return _clean_short_text(
        getattr(cfg, "NODE_ID", "") or os.environ.get("BLUEPRINTS_NODE_ID", ""),
        "",
        limit=120,
    )


def _work_automation_owner_node_state(current_node_id: str) -> tuple[str, dict[str, Any]]:
    explicit_owner = _clean_short_text(
        os.environ.get(KANBAN_AUTOMATION_OWNER_NODE_ID_ENV, ""),
        "",
        limit=120,
    )
    primary_flag_active = _env_bool(KANBAN_AUTOMATION_PRIMARY_FLAG_ENV, False)
    if explicit_owner:
        owner_node_id = explicit_owner
        source = "owner_node_env"
    elif primary_flag_active and current_node_id:
        owner_node_id = current_node_id
        source = "primary_flag_current_node"
    else:
        owner_node_id = ""
        source = "unset"
    return _clean_short_text(
        owner_node_id,
        "",
        limit=120,
    ), {
        "source": source,
        "primary_flag_env": KANBAN_AUTOMATION_PRIMARY_FLAG_ENV,
        "primary_flag_active": primary_flag_active,
    }


def _work_automation_singleton_override_path() -> str:
    return _clean_short_text(
        os.environ.get(
            KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH_ENV,
            KANBAN_AUTOMATION_SINGLETON_OVERRIDE_DEFAULT_PATH,
        ),
        KANBAN_AUTOMATION_SINGLETON_OVERRIDE_DEFAULT_PATH,
        limit=4096,
    )


def _work_automation_singleton_override_state() -> dict[str, Any]:
    override_path = _work_automation_singleton_override_path()
    path_exists = False
    if override_path:
        with suppress(OSError):
            path_exists = Path(override_path).exists()
    env_override = _env_bool(KANBAN_AUTOMATION_SINGLETON_OVERRIDE_ENV, False)
    return {
        "active": bool(env_override or path_exists),
        "env": {
            "name": KANBAN_AUTOMATION_SINGLETON_OVERRIDE_ENV,
            "active": bool(env_override),
        },
        "file": {
            "env": KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH_ENV,
            "path": override_path,
            "exists": bool(path_exists),
        },
    }


def _work_automation_idle_worker_config() -> dict[str, Any]:
    local_model_alias = _work_automation_local_ai_model_alias()
    enabled = _env_bool("BLUEPRINTS_KANBAN_AUTOMATION_IDLE_WORKER", True)
    current_node_id = _work_automation_current_node_id()
    owner_node_id, owner_node_state = _work_automation_owner_node_state(current_node_id)
    override_state = _work_automation_singleton_override_state()
    owner_match = bool(current_node_id and owner_node_id and current_node_id == owner_node_id)
    runs_on_this_node = bool(owner_match or override_state["active"])
    max_scan_items_config = _env_int_range_config(
        "BLUEPRINTS_KANBAN_AUTOMATION_MAX_SCAN_ITEMS",
        KANBAN_AUTOMATION_DEFAULT_MAX_SCAN_ITEMS,
        minimum=1,
        maximum=KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP,
    )
    return {
        "schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
        "enabled": enabled,
        "effective_enabled": bool(enabled and runs_on_this_node),
        "current_node_id": current_node_id,
        "owner_node_id": owner_node_id,
        "owner_node_env": KANBAN_AUTOMATION_OWNER_NODE_ID_ENV,
        "owner_node_source": owner_node_state["source"],
        "primary_flag_env": owner_node_state["primary_flag_env"],
        "primary_flag_active": owner_node_state["primary_flag_active"],
        "singleton_required": True,
        "singleton_owner_match": owner_match,
        "singleton_override": override_state,
        "runs_on_this_node": runs_on_this_node,
        "skip_reason": "" if runs_on_this_node else "idle_worker_not_owner_node",
        "provider_mode": KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
        "processor_profiles": {
            kind: _work_automation_processor_profile_route(kind)
            for kind in ("review", "preprocessing")
        },
        "local_ai_model_alias": local_model_alias,
        "local_ai_max_tokens": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_LOCAL_AI_MAX_TOKENS",
            1800,
            minimum=512,
            maximum=8192,
        ),
        "root_item_id": _clean_short_text(
            os.environ.get("BLUEPRINTS_KANBAN_AUTOMATION_ROOT_ITEM_ID", ""),
            "",
            limit=180,
        ),
        "initial_delay_seconds": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_INITIAL_DELAY_SECONDS",
            15,
            minimum=0,
            maximum=3600,
        ),
        "interval_seconds": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_IDLE_INTERVAL_SECONDS",
            60,
            minimum=5,
            maximum=86400,
        ),
        "max_scan_items": max_scan_items_config.effective,
        "range_config": {
            "max_scan_items": max_scan_items_config.as_dict(),
        },
        "max_process_items": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_MAX_PROCESS_ITEMS",
            5,
            minimum=1,
            maximum=50,
        ),
        "lease_ttl_seconds": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_LEASE_TTL_SECONDS",
            900,
            minimum=60,
            maximum=7200,
        ),
        "marker_timeout_seconds": _env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_MARKER_TIMEOUT_SECONDS",
            900,
            minimum=60,
            maximum=7200,
        ),
        "holder_id": _clean_short_text(
            os.environ.get("BLUEPRINTS_KANBAN_AUTOMATION_HOLDER_ID", "kanban-idle-worker"),
            "kanban-idle-worker",
            limit=160,
        ),
    }


def _local_ai_json_object(content: str) -> dict[str, Any]:
    text = str(content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("local AI response did not contain a JSON object")
    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise ValueError(f"local AI response JSON was invalid: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("local AI response JSON root was not an object")
    return payload


def _sha256_text(content: str) -> str:
    return "sha256:" + hashlib.sha256(str(content or "").encode("utf-8")).hexdigest()


async def _work_automation_local_ai_json_completion(
    *,
    messages: list[dict[str, str]],
    run_id: str,
    processor_kind: str = "review",
) -> dict[str, Any]:
    return await _work_automation_processor_profile_json_completion(
        messages=messages,
        run_id=run_id,
        processor_kind=processor_kind,
    )


def _work_automation_profile_completion_sync(
    *,
    route: dict[str, Any],
    messages: list[dict[str, str]],
    max_tokens: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    api_key = _read_env_file_value(Path(route["api_key_file"]), "API_SERVER_KEY")
    if not api_key:
        raise ValueError(
            f"{route['profile']} API_SERVER_KEY is required in {route['api_key_file']}"
        )
    request_body = json.dumps(
        {
            "model": route["profile"],
            "messages": messages,
            "temperature": 0,
            "max_tokens": max_tokens,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{route['api_base']}/v1/chat/completions",
        data=request_body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(
            f"{route['profile']} profile API returned HTTP {exc.code}: {_body_excerpt(body, limit=800)}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"{route['profile']} profile API unavailable: {exc}") from exc
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{route['profile']} profile API returned invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{route['profile']} profile API response root was not an object")
    return parsed


async def _work_automation_processor_profile_json_completion(
    *,
    messages: list[dict[str, str]],
    run_id: str,
    processor_kind: str,
) -> dict[str, Any]:
    config = _work_automation_idle_worker_config()
    route = _work_automation_processor_profile_route(processor_kind)
    drift = _work_automation_processor_profile_drift(processor_kind)
    if not drift["ok"]:
        raise ValueError(
            f"{route['profile']} auth/config drift blocks processor route: "
            + ",".join(drift["problems"])
        )
    response = await asyncio.to_thread(
        _work_automation_profile_completion_sync,
        route=route,
        messages=messages,
        max_tokens=config["local_ai_max_tokens"],
        timeout_seconds=_env_int(
            "BLUEPRINTS_KANBAN_AUTOMATION_PROFILE_TIMEOUT_SECONDS",
            1800,
            minimum=30,
            maximum=3600,
        ),
    )
    choices = response.get("choices") if isinstance(response, dict) else None
    if not isinstance(choices, list) or not choices:
        raise ValueError(f"{route['profile']} profile response did not include choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else ""
    parsed = _local_ai_json_object(str(content or ""))
    prompt_content = messages[0].get("content", "") if messages else ""
    return {
        "provider_mode": KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
        "processor_engine": KANBAN_AUTOMATION_PROFILE_ENGINE,
        "profile": route["profile"],
        "primary_provider": route["primary_provider"],
        "primary_model": route["primary_model"],
        "fallback_provider": route["fallback_provider"],
        "fallback_model": route["fallback_model"],
        "model_alias": route["model_alias"],
        "run_id": run_id,
        "api_base": route["api_base"],
        "response_model": _clean_short_text(str(response.get("model") or ""), "", limit=180),
        "prompt_sha256": _sha256_text(prompt_content),
        "content_excerpt": _body_excerpt(str(content or ""), limit=4000),
        "payload": parsed,
    }


def _work_automation_processor_ai_defaults(
    ai: dict[str, Any],
    processor_kind: str,
) -> dict[str, Any]:
    route = _work_automation_processor_profile_route(processor_kind)
    merged = {
        "provider_mode": route["provider_mode"],
        "processor_engine": route["processor_engine"],
        "profile": route["profile"],
        "primary_provider": route["primary_provider"],
        "primary_model": route["primary_model"],
        "fallback_provider": route["fallback_provider"],
        "fallback_model": route["fallback_model"],
        "model_alias": route["model_alias"],
        "api_base": route["api_base"],
        "response_model": "",
        "prompt_sha256": "",
        "content_excerpt": "",
        "payload": {},
    }
    merged.update(ai if isinstance(ai, dict) else {})
    return merged


def _local_ai_required_text(
    payload: dict[str, Any],
    key: str,
    *,
    limit: int = 6000,
) -> str:
    value = _body_excerpt(str(payload.get(key) or "").strip(), limit=limit)
    if not value:
        raise ValueError(f"local AI response missing required field: {key}")
    return value


def _local_ai_optional_text(
    payload: dict[str, Any],
    key: str,
    default: str = "",
    *,
    limit: int = 6000,
) -> str:
    return _body_excerpt(str(payload.get(key) or default).strip(), limit=limit)


def _local_ai_ref_list(payload: dict[str, Any], key: str) -> list[str]:
    values = payload.get(key)
    if not isinstance(values, list):
        return []
    refs = []
    for value in values:
        ref = _clean_short_text(str(value or ""), "", limit=260)
        if ref:
            refs.append(ref)
    return refs[:40]


KANBAN_PROMPT_ROOT = (
    Path(
        os.environ.get(
            "BLUEPRINTS_HERMES_LOCAL_STACK", "/xarta-node/.lone-wolf/stacks/hermes-local"
        )
    )
    / "config/prompts"
)
REVIEW_PROCESSOR_SYSTEM_PROMPT = (
    "You are the Blueprints Kanban Review Processor running on the local "
    "private no-think/no-protection/no-orientation endpoint. Return only one "
    "strict JSON object. Do not use markdown. Process the Review text as "
    "future guidance and acceptance-check data. If required infrastructure "
    "or provider wiring is missing, record that as a blocker/question rather "
    "than substituting a different workflow. Do not claim implementation is "
    "done unless proof in the context actually supports it."
)
PREPROCESSING_SYSTEM_PROMPT = (
    "You are the Blueprints Kanban preprocessing processor running on the "
    "local private no-think/no-protection/no-orientation endpoint. Return "
    "only one strict JSON object. Do not use markdown. Decide whether the "
    "current card context is ready for an agent to start implementation. "
    "The queue_source reason may be missing_readiness_marker or "
    "readiness_marker_stale; treat that as why this pass is running, not "
    "as an automatic failure. "
    "If the card is an actionable leaf whose next step is an audit, file "
    "inspection, command run, or implementation task, mark it ready even "
    "when that work has not been completed yet. "
    "When a required route, API, provider, proof path, or operator decision "
    "is missing, set ready=false and return decomposition_items for the "
    "smallest child work items that should be created, using blocked items "
    "for true operator questions or external blockers. Do not invent "
    "deterministic substitute work."
)
REVIEW_PROCESSOR_SYSTEM_PROMPT_PATH = Path(
    os.environ.get(
        "BLUEPRINTS_KANBAN_REVIEW_PROCESSOR_PROMPT",
        str(KANBAN_PROMPT_ROOT / "kanban-review-processor-system.md"),
    )
)
PREPROCESSING_SYSTEM_PROMPT_PATH = Path(
    os.environ.get(
        "BLUEPRINTS_KANBAN_PREPROCESSING_PROMPT",
        str(KANBAN_PROMPT_ROOT / "kanban-preprocessing-system.md"),
    )
)


def _load_work_prompt(path: Path, fallback: str) -> str:
    try:
        prompt = path.read_text(encoding="utf-8").strip()
    except OSError:
        return fallback
    return prompt or fallback


def _work_review_processor_local_ai_messages(
    *,
    item: Any,
    review_document: dict[str, Any],
    marker: dict[str, Any],
) -> list[dict[str, str]]:
    review_text = str(review_document.get("body") or "")
    context = {
        "schema": "xarta.kanban.review_processor.hermes_profile_input.v1",
        "item": _row_to_work_item(item),
        "canonical_item_ref": f"xarta-kanban:item:{item['item_id']}",
        "marker": {
            "marker_id": marker.get("marker_id") or "",
            "processor_kind": marker.get("processor_kind") or "review",
            "document_ref": marker.get("document_ref") or "",
            "document_source_hash": marker.get("document_source_hash") or "",
            "reason": (marker.get("metadata") or {}).get("reason")
            if isinstance(marker.get("metadata"), dict)
            else "",
        },
        "review_document": {
            "document_ref": _review_document_ref(review_document),
            "updated_at": review_document.get("updated_at") or "",
            "body_bytes": len(review_text.encode("utf-8")),
            "body_excerpt": _body_excerpt(review_text, limit=16000),
            "metadata": review_document.get("metadata") or {},
        },
        "processing_policy": _work_review_processing_policy(),
        "required_output": {
            "title": "short human title",
            "summary": "natural language processing result",
            "rationale": "why this decision follows from the Review data",
            "decision_type": "short snake_case type",
            "confidence": "low|medium|high",
            "uncertainty": "remaining uncertainty or empty string",
            "status": "recorded|pending|accepted|declined|failed",
            "affected_refs": ["kanban_items:<id>", "xarta-kanban:item:<id>"],
            "proof_refs": ["source/proof refs"],
        },
    }
    system = _load_work_prompt(REVIEW_PROCESSOR_SYSTEM_PROMPT_PATH, REVIEW_PROCESSOR_SYSTEM_PROMPT)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(context, ensure_ascii=True, sort_keys=True)},
    ]


def _work_review_idle_marker_context_sync(item_id: str) -> tuple[Any, dict[str, Any]]:
    with get_conn() as conn:
        item = _work_item_or_404(conn, item_id)
        review_document = _work_item_review_document(conn, item_id)
    return item, review_document


async def _process_work_review_idle_marker(
    marker: dict[str, Any],
    *,
    holder_id: str,
    lease_token: str,
    run_id: str,
) -> dict[str, Any]:
    item_id = marker["item_id"]
    item, review_document = await _run_personal_sync_work(
        _work_review_idle_marker_context_sync,
        item_id,
    )
    ai = await _work_automation_local_ai_json_completion(
        messages=_work_review_processor_local_ai_messages(
            item=item,
            review_document=review_document,
            marker=marker,
        ),
        run_id=run_id,
        processor_kind="review",
    )
    ai = _work_automation_processor_ai_defaults(ai, "review")
    payload = ai["payload"]
    title = _local_ai_required_text(payload, "title", limit=220)
    summary = _local_ai_required_text(payload, "summary")
    rationale = _local_ai_required_text(payload, "rationale")
    decision_type = _clean_short_text(
        str(payload.get("decision_type") or "review_document_processed"),
        "review_document_processed",
        limit=120,
    )
    affected_refs = _local_ai_ref_list(payload, "affected_refs") or [
        f"kanban_items:{item_id}",
        f"xarta-kanban:item:{item_id}",
    ]
    if f"kanban_review_processor_markers:{marker['marker_id']}" not in affected_refs:
        affected_refs.append(f"kanban_review_processor_markers:{marker['marker_id']}")
    proof_refs = _local_ai_ref_list(payload, "proof_refs") or [
        f"kanban_review_processor_markers:{marker['marker_id']}",
        marker.get("document_ref") or f"kanban_items:{item_id}:review",
    ]
    uncertainty = _local_ai_optional_text(payload, "uncertainty", limit=3000)
    confidence = _clean_short_text(str(payload.get("confidence") or "medium"), "medium", limit=40)
    status = _clean_review_decision_status(str(payload.get("status") or "recorded"))
    decision_id = _clean_work_id(
        f"kanban-decision-hermes-review-{item_id}-{marker['document_source_hash'][-12:]}",
        "kanban-decision",
    )
    decision = await record_work_item_review_decision(
        item_id,
        WorkReviewDecisionCreateRequest(
            decision_id=decision_id,
            processor_kind="review",
            decision_type=decision_type,
            title=title,
            summary=summary,
            rationale=rationale,
            affected_refs=affected_refs,
            confidence=confidence,
            uncertainty=uncertainty,
            proof_refs=proof_refs,
            status=status,
            provider_mode=KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
            metadata={
                "schema": "xarta.kanban.review_processor.hermes_profile_decision.v1",
                "worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
                "processor_engine": ai["processor_engine"],
                "provider_policy": _work_review_processing_policy()["active_mode"],
                "provider_mode": ai["provider_mode"],
                "profile": ai["profile"],
                "primary_provider": ai["primary_provider"],
                "primary_model": ai["primary_model"],
                "fallback_provider": ai["fallback_provider"],
                "fallback_model": ai["fallback_model"],
                "model_alias": ai["model_alias"],
                "response_model": ai["response_model"],
                "api_base": ai["api_base"],
                "prompt_sha256": ai["prompt_sha256"],
                "marker_id": marker["marker_id"],
                "document_source_hash": marker["document_source_hash"],
                "llm_payload": payload,
                "llm_content_excerpt": ai["content_excerpt"],
            },
            actor=holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-decision-{marker['marker_id']}",
            run_id=run_id,
        ),
    )
    complete = await complete_work_review_processor_marker(
        marker["marker_id"],
        WorkReviewProcessorMarkerCompleteRequest(
            holder_id=holder_id,
            lease_token=lease_token,
            document_source_hash=marker["document_source_hash"],
            decision_id=decision["decision"]["decision_id"],
            status="processed",
            actor=holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-complete-{marker['marker_id']}",
            run_id=run_id,
            metadata={
                "schema": "xarta.kanban.review_processor.hermes_profile_completion.v1",
                "decision_id": decision["decision"]["decision_id"],
                "processor_engine": ai["processor_engine"],
                "provider_mode": ai["provider_mode"],
                "profile": ai["profile"],
                "primary_provider": ai["primary_provider"],
                "primary_model": ai["primary_model"],
                "model_alias": ai["model_alias"],
                "prompt_sha256": ai["prompt_sha256"],
            },
        ),
    )
    return {
        "ok": True,
        "processor_kind": "review",
        "item_id": item_id,
        "marker_id": marker["marker_id"],
        "decision_id": decision["decision"]["decision_id"],
        "completed": complete.get("completed", False),
        "status": complete.get("marker", {}).get("status", ""),
        "provider_mode": ai["provider_mode"],
        "profile": ai["profile"],
        "primary_provider": ai["primary_provider"],
        "primary_model": ai["primary_model"],
        "model_alias": ai["model_alias"],
    }


def _work_preprocessing_local_ai_messages(
    *,
    item: Any,
    source: dict[str, Any],
    detail_document: dict[str, Any],
    review_document: dict[str, Any],
    discussions: list[dict[str, Any]],
    recent_commits: list[dict[str, Any]],
    recent_decisions: list[dict[str, Any]],
    ancestor_context: dict[str, Any],
    marker: dict[str, Any],
) -> list[dict[str, str]]:
    context = {
        "schema": "xarta.kanban.preprocessing.hermes_profile_input.v1",
        "item": _row_to_work_item(item),
        "canonical_item_ref": f"xarta-kanban:item:{item['item_id']}",
        "marker": {
            "marker_id": marker.get("marker_id") or "",
            "processor_kind": marker.get("processor_kind") or "preprocessing",
            "document_source_hash": marker.get("document_source_hash") or "",
        },
        "queue_source": source,
        "documents": {
            "body_excerpt": _body_excerpt(str(item["body_excerpt"] or ""), limit=6000),
            "detail_excerpt": _body_excerpt(str(detail_document.get("body") or ""), limit=12000),
            "review_excerpt": _body_excerpt(str(review_document.get("body") or ""), limit=12000),
            "recent_discussions": [
                {
                    "discussion_id": discussion.get("discussion_id") or "",
                    "created_at": discussion.get("created_at") or "",
                    "updated_at": discussion.get("updated_at") or "",
                    "author": discussion.get("author") or "",
                    "body_excerpt": _body_excerpt(
                        str((discussion.get("document") or {}).get("body") or ""),
                        limit=3000,
                    ),
                }
                for discussion in discussions[:6]
            ],
        },
        "evidence": {
            "recent_commits": recent_commits[:8],
            "recent_decisions": recent_decisions[:8],
            "ancestor_context": ancestor_context,
        },
        "hard_rules": [
            "If required provider/API wiring is missing, ask or block; do not substitute a fallback.",
            "If the item lacks enough information to implement safely, ready must be false and decomposition_items must contain the concrete child work items needed to make progress.",
            "Preprocessing prepares the Kanban tree for agents; do not only list work in prose when child cards should be created.",
            "When missing context is really an operator question or external blocker, create a blocked decomposition item with the exact question/blocker at the smallest useful scope.",
            "Use ancestor_context before reporting missing_parent_content or creating a child that only retrieves parent content; ancestor_context includes the rootward chain up to the Kanban depth limit, so block for parent content only when the needed parent or ancestor is absent or the supplied excerpts are insufficient.",
            "Failed preprocessing decisions from older source hashes are durable history, not current blockers.",
            "If there are open blockers, ready must be false.",
            "A missing or stale context readiness marker is the scheduling reason for this preprocessing pass, not a blocker by itself.",
            "When the current documents, discussions, commits, status proof, and decisions are sufficient, set ready=true so the worker can refresh the readiness marker.",
            "If a concrete leaf card is actionable by inspecting files, running commands, or doing the named investigation, set ready=true; do not mark it not ready merely because the investigation has not been performed yet.",
            "Previous failed preprocessing decisions are historical evidence; do not repeat them when newer timestamped discussions, commits, or status proof resolve the cited blocker.",
            "Background idle-worker audit/status evidence counts as autonomous-run proof when it is newer than a manual-trigger-only blocker decision.",
            "Readiness is not completion.",
            "Leaves should be Doing only while actively operated on; otherwise use To Do or Blocked.",
            "When discussions, decisions, or commits conflict, prefer the newest timestamped Kanban evidence.",
        ],
        "required_output": {
            "ready": True,
            "title": "short human title",
            "summary": "what preprocessing concluded",
            "rationale": "why the item is or is not ready",
            "confidence": "low|medium|high",
            "uncertainty": "remaining uncertainty",
            "blocking_codes": ["missing_cloud_api"],
            "recommended_next_actions": ["ask operator a concrete question"],
            "decomposition_items": [
                {
                    "title": "short child card title",
                    "body": "card body with proof path and why this child exists",
                    "state_id": "todo|blocked",
                    "priority_id": "critical|high|medium|low",
                    "tags": ["kanban", "optional-routing-tag"],
                    "proof_path": "how this child can be proved complete",
                    "blocked_reason": "only when state_id is blocked",
                }
            ],
            "affected_refs": ["kanban_items:<id>", "xarta-kanban:item:<id>"],
            "proof_refs": ["source/proof refs"],
        },
    }
    system = _load_work_prompt(PREPROCESSING_SYSTEM_PROMPT_PATH, PREPROCESSING_SYSTEM_PROMPT)
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(context, ensure_ascii=True, sort_keys=True)},
    ]


def _work_slug_fragment(value: str, *, limit: int = 48) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return slug[:limit].strip("-") or "work"


def _work_preprocessing_title_key(title: str) -> str:
    return re.sub(r"\s+", " ", str(title or "").strip()).casefold()


def _work_preprocessing_child_id(parent_item_id: str, title: str, *, attempt: int = 0) -> str:
    digest_source = (
        f"{parent_item_id}|{title}|{attempt}" if attempt else f"{parent_item_id}|{title}"
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:12]
    base = f"{parent_item_id}-{_work_slug_fragment(title, limit=64)}"
    max_base_length = 180 - len(digest) - 1
    base = base[:max_base_length].rstrip("-") or "kanban-preprocess"
    return _clean_work_id(f"{base}-{digest}", "kanban-preprocess")


def _work_preprocessing_unique_child_id(conn: Any, parent_item_id: str, title: str) -> str:
    title_key = _work_preprocessing_title_key(title)
    for attempt in range(50):
        item_id = _work_preprocessing_child_id(parent_item_id, title, attempt=attempt)
        existing = conn.execute(
            "SELECT item_id, parent_item_id, title, status FROM kanban_items WHERE item_id=?",
            (item_id,),
        ).fetchone()
        if existing is None:
            return item_id
        if (
            existing["parent_item_id"] == parent_item_id
            and existing["status"] != "archived"
            and _work_preprocessing_title_key(existing["title"]) == title_key
        ):
            return item_id
    raise ValueError("could not generate a collision-free preprocessing child item_id")


def _work_preprocessing_assert_child_placement(
    item: dict[str, Any],
    *,
    parent_item_id: str,
    expected_depth: int,
) -> None:
    if item.get("parent_item_id") != parent_item_id:
        raise RuntimeError(
            "preprocessing child placement invariant failed: "
            f"{item.get('item_id', '')} parent={item.get('parent_item_id')!r} "
            f"expected={parent_item_id!r}"
        )
    if int(item.get("depth") or 0) != expected_depth:
        raise RuntimeError(
            "preprocessing child placement invariant failed: "
            f"{item.get('item_id', '')} depth={item.get('depth')!r} "
            f"expected={expected_depth}"
        )


def _clean_work_preprocessing_state(value: Any) -> str:
    state = _clean_short_text(str(value or ""), "", limit=40).lower()
    if state in {"blocked", "blocker"}:
        return "blocked"
    if state in {"backlog"}:
        return "backlog"
    return "todo"


def _clean_work_preprocessing_priority(value: Any, default: str) -> str:
    priority = _clean_short_text(str(value or ""), "", limit=40).lower()
    if priority in {"critical", "high", "medium", "low"}:
        return priority
    return default if default in {"critical", "high", "medium", "low"} else "medium"


def _work_preprocessing_normalise_decomposition_items(
    payload: dict[str, Any],
    *,
    parent_item: Any,
) -> list[dict[str, Any]]:
    parent_item_id = parent_item["item_id"]
    parent_priority = str(parent_item["priority_id"] or "medium")
    parent_tags = _json_value(parent_item["tags_json"], ["kanban"])
    if not isinstance(parent_tags, list):
        parent_tags = ["kanban"]
    raw_items_value = payload.get("decomposition_items")
    if raw_items_value is None:
        raw_items = []
    elif isinstance(raw_items_value, list):
        raw_items = raw_items_value
    else:
        raise ValueError("local AI response field decomposition_items must be a list")
    normalised: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for index, raw_item in enumerate(raw_items[:24]):
        if isinstance(raw_item, str):
            raise ValueError(
                f"local AI decomposition_items[{index}] must be an object with a title"
            )
        elif isinstance(raw_item, dict):
            candidate = raw_item
        else:
            raise ValueError(f"local AI decomposition_items[{index}] must be an object")
        title = _clean_short_text(
            candidate.get("title") or "",
            "",
            limit=180,
        )
        if not title:
            raise ValueError(f"local AI decomposition_items[{index}] missing required field: title")
        title_key = _work_preprocessing_title_key(title)
        if title_key in seen_titles:
            raise ValueError(
                f"local AI decomposition_items[{index}] duplicates another child title"
            )
        seen_titles.add(title_key)
        state_id = _clean_work_preprocessing_state(candidate.get("state_id"))
        blocked_reason = _body_excerpt(str(candidate.get("blocked_reason") or ""), limit=800)
        if blocked_reason:
            state_id = "blocked"
        priority_id = _clean_work_preprocessing_priority(
            candidate.get("priority_id") or candidate.get("priority"),
            parent_priority,
        )
        candidate_tags = candidate.get("tags")
        if not isinstance(candidate_tags, list):
            candidate_tags = []
        tags = []
        for tag in [*parent_tags, *candidate_tags, "preprocessing"]:
            clean_tag = _clean_short_text(str(tag or ""), "", limit=80)
            if clean_tag and clean_tag not in tags:
                tags.append(clean_tag)
        if "kanban" not in tags:
            tags.insert(0, "kanban")
        body_parts = [
            f"Preprocessing child of xarta-kanban:item:{parent_item_id}.",
        ]
        description = _body_excerpt(
            str(
                candidate.get("body")
                or candidate.get("description")
                or candidate.get("rationale")
                or candidate.get("summary")
                or ""
            ),
            limit=1600,
        )
        if description:
            body_parts.append(description)
        proof_path = _body_excerpt(str(candidate.get("proof_path") or ""), limit=800)
        if proof_path:
            body_parts.append(f"Proof path: {proof_path}")
        if blocked_reason:
            body_parts.append(f"Blocked reason/question: {blocked_reason}")
        normalised.append(
            {
                "title": title,
                "body": "\n\n".join(body_parts),
                "state_id": state_id,
                "priority_id": priority_id,
                "tags": tags,
                "proof_path": proof_path,
                "blocked_reason": blocked_reason,
            }
        )
    return normalised


def _work_preprocessing_parent_automation_excluded_sync(parent_item_id: str) -> bool:
    with get_conn() as conn:
        return _work_item_automation_excluded(conn, parent_item_id)


def _work_preprocessing_child_candidate_sync(
    parent_item_id: str,
    expected_child_depth: int,
    candidate: dict[str, Any],
) -> dict[str, Any]:
    with get_conn() as conn:
        sibling_rows = conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=?
              AND status!='archived'
            ORDER BY created_at ASC, item_id
            """,
            (parent_item_id,),
        ).fetchall()
        sibling_by_title = {
            _work_preprocessing_title_key(row["title"]): row for row in sibling_rows
        }
        existing_row = sibling_by_title.get(_work_preprocessing_title_key(candidate["title"]))
        if existing_row is not None:
            existing_item = _row_to_work_item(existing_row)
            _work_preprocessing_assert_child_placement(
                existing_item,
                parent_item_id=parent_item_id,
                expected_depth=expected_child_depth,
            )
            return {"existing_item": existing_item}
        item_id = _work_preprocessing_unique_child_id(
            conn,
            parent_item_id,
            candidate["title"],
        )
        return {"item_id": item_id}


async def _work_preprocessing_create_decomposition_children(
    *,
    parent_item: Any,
    payload: dict[str, Any],
    holder_id: str,
    run_id: str,
    marker_id: str,
) -> dict[str, Any]:
    parent_item_id = parent_item["item_id"]
    expected_child_depth = int(parent_item["depth"] or 0) + 1
    if await _run_personal_sync_work(
        _work_preprocessing_parent_automation_excluded_sync,
        parent_item_id,
    ):
        return {
            "schema": "xarta.kanban.preprocessing.decomposition_result.v1",
            "created_count": 0,
            "existing_count": 0,
            "total_count": 0,
            "created_items": [],
            "existing_items": [],
            "items": [],
            "skipped_reason": "automation_excluded",
        }
    candidates = _work_preprocessing_normalise_decomposition_items(
        payload,
        parent_item=parent_item,
    )
    created: list[dict[str, Any]] = []
    existing: list[dict[str, Any]] = []
    for candidate in candidates:
        candidate_result = await _run_personal_sync_work(
            _work_preprocessing_child_candidate_sync,
            parent_item_id,
            expected_child_depth,
            candidate,
        )
        existing_item = candidate_result.get("existing_item")
        if existing_item is not None:
            existing.append(existing_item)
            continue
        item_id = str(candidate_result["item_id"])
        result = await create_work_item(
            WorkItemCreateRequest(
                item_id=item_id,
                parent_item_id=parent_item_id,
                title=candidate["title"],
                body=candidate["body"],
                item_type="item",
                state_id=candidate["state_id"],
                priority_id=candidate["priority_id"],
                tags=candidate["tags"],
                blocker_title=(
                    f"Preprocessing blocker/question: {candidate['title']}"
                    if candidate["state_id"] == "blocked"
                    else None
                ),
                blocker_body=candidate["blocked_reason"] or None,
                blocked_by_ref=f"kanban_review_processor_markers:{marker_id}",
                automation_source_item_id=parent_item_id,
                automation_marker_id=marker_id,
                automation_reason=(
                    candidate["blocked_reason"] or "preprocessing_decomposition_child"
                ),
                automation_operation_kind="preprocessing_create_child",
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-child-{marker_id}-{item_id}",
                run_id=run_id,
            )
        )
        child_item = result["item"]
        _work_preprocessing_assert_child_placement(
            child_item,
            parent_item_id=parent_item_id,
            expected_depth=expected_child_depth,
        )
        created.append(child_item)
    all_items = [*existing, *created]
    if all_items:
        lines = [
            "Preprocessing decomposition created or confirmed child work items:",
            "",
            *[
                f"- xarta-kanban:item:{item['item_id']} - {item['title']} ({item['state_id']})"
                for item in all_items
            ],
        ]
        await create_work_discussion(
            parent_item_id,
            WorkDiscussionCreateRequest(
                body="\n".join(lines),
                author=holder_id,
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-decomposition-note-{marker_id}",
                run_id=run_id,
            ),
        )
    return {
        "schema": "xarta.kanban.preprocessing.decomposition_result.v1",
        "created_count": len(created),
        "existing_count": len(existing),
        "total_count": len(all_items),
        "created_items": created,
        "existing_items": existing,
        "items": all_items,
    }


def _preprocessing_readiness_marker(
    *,
    item_id: str,
    source: dict[str, Any],
    marker: dict[str, Any],
    actor: str,
    now: str,
    ai_payload: dict[str, Any],
    processor_route: dict[str, Any] | None = None,
    outcome: str = "ready",
    decomposition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    next_actions = ai_payload.get("recommended_next_actions")
    if not isinstance(next_actions, list):
        next_actions = []
    decomposition = decomposition if isinstance(decomposition, dict) else {}
    route = (
        processor_route
        if isinstance(processor_route, dict)
        else _work_automation_processor_profile_route("preprocessing")
    )
    return {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "context_packet_schema": source["schema"],
        "item_id": item_id,
        "canonical_code": f"xarta-kanban:item:{item_id}",
        "marked_at": now,
        "marked_by": actor,
        "context_hash": source["document_source_hash"],
        "component_hashes": {
            "preprocessing_queue_source": source["document_source_hash"],
        },
        "counts": source["counts"],
        "source_refs": source["source_refs"],
        "packet_summary": _body_excerpt(
            str(ai_payload.get("summary") or "Local AI preprocessing accepted current context."),
            limit=1200,
        ),
        "recommended_next_actions": [
            _body_excerpt(str(action or ""), limit=300) for action in next_actions[:12]
        ],
        "processor_marker_id": marker["marker_id"],
        "processor_profile": route.get("profile", ""),
        "processor_provider_mode": route.get(
            "provider_mode", KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
        ),
        "processor_model_alias": route.get("model_alias", ""),
        "processor_primary_provider": route.get("primary_provider", ""),
        "processor_primary_model": route.get("primary_model", ""),
        "preprocessing_outcome": outcome,
        "decomposition_item_ids": [
            str(item.get("item_id") or "")
            for item in decomposition.get("items", [])
            if isinstance(item, dict) and str(item.get("item_id") or "")
        ],
        "implementation_scope": "child_leaves"
        if decomposition.get("total_count")
        else "current_item",
    }


def _work_preprocessing_marker_ineligible_reason_sync(marker: dict[str, Any]) -> str:
    with get_conn() as conn:
        return _work_preprocessing_pending_ineligible_reason(conn, marker)


def _work_preprocessing_idle_marker_context_sync(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        item = _work_item_or_404(conn, item_id)
        source = _work_preprocessing_context_source(conn, item)
        detail_document = _work_item_detail_document(conn, item_id)
        review_document = _work_item_review_document(conn, item_id)
        discussion_rows = conn.execute(
            """
            SELECT * FROM kanban_discussions
            WHERE item_id=? AND status!='archived'
            ORDER BY updated_at DESC, created_at DESC, discussion_id
            LIMIT 6
            """,
            (item_id,),
        ).fetchall()
        discussions = [_row_to_work_discussion(row, conn) for row in discussion_rows]
        commit_rows = conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=?
            ORDER BY COALESCE(NULLIF(committed_at, ''), updated_at) DESC,
                     updated_at DESC, commit_link_id
            LIMIT 8
            """,
            (item_id,),
        ).fetchall()
        recent_commits = [
            {
                "commit_link_id": row["commit_link_id"],
                "repo_full_name": row["repo_full_name"],
                "sha": row["sha"],
                "short_sha": row["short_sha"],
                "message_subject": row["message_subject"],
                "committed_at": row["committed_at"],
                "metadata": _json_value(row["metadata_json"], {}),
            }
            for row in commit_rows
        ]
        recent_decisions = _work_preprocessing_recent_decisions(
            conn,
            item_id=item_id,
            current_source_hash=source["document_source_hash"],
            limit=8,
        )
        hints_row = conn.execute(
            "SELECT * FROM kanban_agent_hints WHERE item_id=?",
            (item_id,),
        ).fetchone()
        hints = _row_to_work_agent_hints(hints_row, item_id)
        ancestor_context = _work_preprocessing_ancestor_context(conn, item)
    return {
        "item": item,
        "source": source,
        "detail_document": detail_document,
        "review_document": review_document,
        "discussions": discussions,
        "recent_commits": recent_commits,
        "recent_decisions": recent_decisions,
        "hints": hints,
        "ancestor_context": ancestor_context,
    }


def _work_preprocessing_updated_context_sync(item_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        updated_item = _work_item_or_404(conn, item_id)
        updated_source = _work_preprocessing_context_source(conn, updated_item)
        hints_row = conn.execute(
            "SELECT * FROM kanban_agent_hints WHERE item_id=?",
            (item_id,),
        ).fetchone()
        updated_hints = _row_to_work_agent_hints(hints_row, item_id)
    return {
        "updated_source": updated_source,
        "updated_hints": updated_hints,
    }


async def _process_work_preprocessing_idle_marker(
    marker: dict[str, Any],
    *,
    holder_id: str,
    lease_token: str,
    run_id: str,
) -> dict[str, Any]:
    item_id = marker["item_id"]
    now = _utc_now_iso()
    ineligible_reason = await _run_personal_sync_work(
        _work_preprocessing_marker_ineligible_reason_sync,
        marker,
    )
    automation_excluded = ineligible_reason == "automation_excluded"
    if automation_excluded:
        complete = await complete_work_review_processor_marker(
            marker["marker_id"],
            WorkReviewProcessorMarkerCompleteRequest(
                holder_id=holder_id,
                lease_token=lease_token,
                document_source_hash=marker["document_source_hash"],
                status="cancelled",
                error="automation_excluded",
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-excluded-{marker['marker_id']}",
                run_id=run_id,
                metadata={
                    "schema": "xarta.kanban.preprocessing.hermes_profile_completion.v1",
                    "reason": "automation_excluded",
                },
            ),
        )
        return {
            "ok": False,
            "processor_kind": "preprocessing",
            "item_id": item_id,
            "marker_id": marker["marker_id"],
            "reason": "automation_excluded",
            "status": complete.get("marker", {}).get("status", "cancelled"),
        }
    if ineligible_reason:
        complete = await complete_work_review_processor_marker(
            marker["marker_id"],
            WorkReviewProcessorMarkerCompleteRequest(
                holder_id=holder_id,
                lease_token=lease_token,
                document_source_hash=marker["document_source_hash"],
                status="cancelled",
                error=ineligible_reason,
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-not-candidate-{marker['marker_id']}",
                run_id=run_id,
                metadata={
                    "schema": "xarta.kanban.preprocessing.hermes_profile_completion.v1",
                    "contract_schema": KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA,
                    "reason": "preprocessing_not_candidate_cancelled",
                    "last_error": ineligible_reason,
                    "candidate_state_id": "todo",
                    "candidate_leaf_required": True,
                },
            ),
        )
        return {
            "ok": False,
            "processor_kind": "preprocessing",
            "item_id": item_id,
            "marker_id": marker["marker_id"],
            "reason": ineligible_reason,
            "status": complete.get("marker", {}).get("status", "cancelled"),
        }
    context = await _run_personal_sync_work(_work_preprocessing_idle_marker_context_sync, item_id)
    item = context["item"]
    source = context["source"]
    detail_document = context["detail_document"]
    review_document = context["review_document"]
    discussions = context["discussions"]
    recent_commits = context["recent_commits"]
    recent_decisions = context["recent_decisions"]
    hints = context["hints"]
    ancestor_context = context["ancestor_context"]
    ai = await _work_automation_local_ai_json_completion(
        messages=_work_preprocessing_local_ai_messages(
            item=item,
            source=source,
            detail_document=detail_document,
            review_document=review_document,
            discussions=discussions,
            recent_commits=recent_commits,
            recent_decisions=recent_decisions,
            ancestor_context=ancestor_context,
            marker=marker,
        ),
        run_id=run_id,
        processor_kind="preprocessing",
    )
    ai = _work_automation_processor_ai_defaults(ai, "preprocessing")
    payload = ai["payload"]
    ready = bool(payload.get("ready"))
    title = _local_ai_optional_text(payload, "title", limit=220) or _clean_short_text(
        str(item["title"] or ""),
        "Preprocessing result",
        limit=220,
    )
    summary = _local_ai_required_text(payload, "summary")
    rationale = _local_ai_required_text(payload, "rationale")
    confidence = _clean_short_text(str(payload.get("confidence") or "medium"), "medium", limit=40)
    uncertainty = _local_ai_optional_text(payload, "uncertainty", limit=3000)
    blocking_codes = payload.get("blocking_codes")
    if not isinstance(blocking_codes, list):
        blocking_codes = []
    blocking_codes = [
        _clean_short_text(str(code or ""), "", limit=120)
        for code in blocking_codes[:20]
        if str(code or "").strip()
    ]
    blocker_count = int(source.get("counts", {}).get("blocker_count") or 0)
    body_ref = next(
        (ref for ref in source.get("source_refs", []) if ref.get("name") == "body"),
        {},
    )
    body_length = int(body_ref.get("body_length") or 0)
    hard_blocking_codes = []
    if blocker_count > 0:
        hard_blocking_codes.append("open_blockers")
    if body_length <= 0:
        hard_blocking_codes.append("missing_body")

    decomposition_result: dict[str, Any] = {
        "schema": "xarta.kanban.preprocessing.decomposition_result.v1",
        "created_count": 0,
        "existing_count": 0,
        "total_count": 0,
        "created_items": [],
        "existing_items": [],
        "items": [],
    }
    readiness_outcome = "ready"
    readiness_normalization: dict[str, Any] = {}
    if not hard_blocking_codes and not ready:
        decomposition_result = await _work_preprocessing_create_decomposition_children(
            parent_item=item,
            payload=payload,
            holder_id=holder_id,
            run_id=run_id,
            marker_id=marker["marker_id"],
        )
        if int(decomposition_result.get("total_count") or 0) <= 0:
            if blocking_codes:
                blocking_codes.append("missing_decomposition_items")
                hard_blocking_codes.append("llm_reported_not_ready")
            else:
                ready = True
                readiness_outcome = "ready_leaf_no_decomposition"
                readiness_normalization = {
                    "schema": "xarta.kanban.preprocessing.readiness_normalization.v1",
                    "reason": "model_reported_not_ready_without_blocker_or_decomposition",
                    "model_ready": False,
                    "model_decomposition_count": 0,
                    "model_blocking_code_count": 0,
                }

    if not ready and int(decomposition_result.get("total_count") or 0) <= 0:
        if "missing_decomposition_items" not in blocking_codes:
            blocking_codes.append("missing_decomposition_items")
        hard_blocking_codes.append("llm_reported_not_ready")
    if hard_blocking_codes:
        all_blocking_codes = list(dict.fromkeys([*blocking_codes, *hard_blocking_codes]))
        decision_id = _clean_work_id(
            f"kanban-decision-hermes-preprocess-blocked-{item_id}-{source['document_source_hash'][-12:]}",
            "kanban-decision",
        )
        decision = await record_work_item_review_decision(
            item_id,
            WorkReviewDecisionCreateRequest(
                decision_id=decision_id,
                processor_kind="preprocessing",
                decision_type="preprocessing_blocker_or_question",
                title=title,
                summary=summary,
                rationale=rationale,
                affected_refs=_local_ai_ref_list(payload, "affected_refs")
                or [
                    f"kanban_items:{item_id}",
                    f"xarta-kanban:item:{item_id}",
                    f"kanban_review_processor_markers:{marker['marker_id']}",
                ],
                confidence=confidence,
                uncertainty=uncertainty,
                proof_refs=_local_ai_ref_list(payload, "proof_refs")
                or [
                    f"kanban_review_processor_markers:{marker['marker_id']}",
                    f"kanban_items:{item_id}:context_readiness",
                ],
                status="failed",
                provider_mode=KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
                metadata={
                    "schema": "xarta.kanban.preprocessing.hermes_profile_blocked_decision.v1",
                    "worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
                    "processor_engine": ai["processor_engine"],
                    "provider_policy": _work_review_processing_policy()["active_mode"],
                    "provider_mode": ai["provider_mode"],
                    "profile": ai["profile"],
                    "primary_provider": ai["primary_provider"],
                    "primary_model": ai["primary_model"],
                    "fallback_provider": ai["fallback_provider"],
                    "fallback_model": ai["fallback_model"],
                    "model_alias": ai["model_alias"],
                    "response_model": ai["response_model"],
                    "api_base": ai["api_base"],
                    "prompt_sha256": ai["prompt_sha256"],
                    "marker_id": marker["marker_id"],
                    "document_source_hash": source["document_source_hash"],
                    "blocking_codes": all_blocking_codes,
                    "decomposition": decomposition_result,
                    "source": source,
                    "llm_payload": payload,
                    "llm_content_excerpt": ai["content_excerpt"],
                },
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-blocked-decision-{marker['marker_id']}",
                run_id=run_id,
            ),
        )
        complete = await complete_work_review_processor_marker(
            marker["marker_id"],
            WorkReviewProcessorMarkerCompleteRequest(
                holder_id=holder_id,
                lease_token=lease_token,
                document_source_hash=marker["document_source_hash"],
                status="failed",
                decision_id=decision["decision"]["decision_id"],
                error=";".join(all_blocking_codes),
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-failed-{marker['marker_id']}",
                run_id=run_id,
                metadata={
                    "schema": "xarta.kanban.preprocessing.hermes_profile_completion.v1",
                    "processor_engine": ai["processor_engine"],
                    "decision_id": decision["decision"]["decision_id"],
                    "provider_mode": ai["provider_mode"],
                    "profile": ai["profile"],
                    "primary_provider": ai["primary_provider"],
                    "primary_model": ai["primary_model"],
                    "model_alias": ai["model_alias"],
                    "prompt_sha256": ai["prompt_sha256"],
                    "blocking_codes": all_blocking_codes,
                },
            ),
        )
        return {
            "ok": False,
            "processor_kind": "preprocessing",
            "item_id": item_id,
            "marker_id": marker["marker_id"],
            "decision_id": decision["decision"]["decision_id"],
            "reason": ";".join(all_blocking_codes),
            "status": complete.get("marker", {}).get("status", "failed"),
            "provider_mode": ai["provider_mode"],
            "profile": ai["profile"],
            "primary_provider": ai["primary_provider"],
            "primary_model": ai["primary_model"],
            "model_alias": ai["model_alias"],
        }

    if int(decomposition_result.get("total_count") or 0) > 0:
        parent_lane_update: dict[str, Any] | None = None
        if str(item["state_id"] or "") == "todo":
            parent_lane_update = await move_work_item(
                item_id,
                WorkItemMoveRequest(
                    state_id="doing",
                    actor=holder_id,
                    source_surface="kanban-automation-idle-worker",
                    request_id=f"{run_id}-preprocess-parent-doing-{marker['marker_id']}",
                    run_id=run_id,
                ),
            )
        updated_context = await _run_personal_sync_work(
            _work_preprocessing_updated_context_sync,
            item_id,
        )
        updated_source = updated_context["updated_source"]
        updated_hints = updated_context["updated_hints"]
        readiness_marker = _preprocessing_readiness_marker(
            item_id=item_id,
            source=updated_source,
            marker=marker,
            actor=holder_id,
            now=now,
            ai_payload=payload,
            processor_route=ai,
            outcome="decomposed",
            decomposition=decomposition_result,
        )
        metadata = dict(updated_hints.get("metadata") or {})
        metadata["context_readiness_marker"] = readiness_marker
        await update_work_item_agent_hints(
            item_id,
            WorkAgentHintsUpdateRequest(
                metadata=metadata,
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-decomposition-readiness-{marker['marker_id']}",
                run_id=run_id,
            ),
        )
        child_refs = [
            f"kanban_items:{child['item_id']}"
            for child in decomposition_result.get("items", [])
            if isinstance(child, dict) and child.get("item_id")
        ]
        decision_id = _clean_work_id(
            f"kanban-decision-idle-preprocess-decomposed-{item_id}-{updated_source['document_source_hash'][-12:]}",
            "kanban-decision",
        )
        decision = await record_work_item_review_decision(
            item_id,
            WorkReviewDecisionCreateRequest(
                decision_id=decision_id,
                processor_kind="preprocessing",
                decision_type="preprocessing_decomposition",
                title=title,
                summary=summary,
                rationale=rationale,
                affected_refs=[
                    f"kanban_items:{item_id}",
                    f"xarta-kanban:item:{item_id}",
                    f"kanban_agent_hints:{item_id}",
                    f"kanban_review_processor_markers:{marker['marker_id']}",
                    *child_refs,
                ],
                confidence=confidence,
                uncertainty=uncertainty,
                proof_refs=_local_ai_ref_list(payload, "proof_refs")
                or [
                    f"kanban_review_processor_markers:{marker['marker_id']}",
                    f"kanban_agent_hints:{item_id}",
                    *child_refs,
                ],
                status="accepted",
                provider_mode=KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
                metadata={
                    "schema": "xarta.kanban.preprocessing.decomposition_decision.v1",
                    "worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
                    "processor_engine": ai["processor_engine"],
                    "provider_policy": _work_review_processing_policy()["active_mode"],
                    "provider_mode": ai["provider_mode"],
                    "profile": ai["profile"],
                    "primary_provider": ai["primary_provider"],
                    "primary_model": ai["primary_model"],
                    "fallback_provider": ai["fallback_provider"],
                    "fallback_model": ai["fallback_model"],
                    "model_alias": ai["model_alias"],
                    "response_model": ai["response_model"],
                    "api_base": ai["api_base"],
                    "prompt_sha256": ai["prompt_sha256"],
                    "marker_id": marker["marker_id"],
                    "document_source_hash": updated_source["document_source_hash"],
                    "readiness_marker": readiness_marker,
                    "decomposition": decomposition_result,
                    "parent_lane_update": {
                        "from_state_id": item["state_id"],
                        "to_state_id": "doing",
                        "reason": "preprocessing_created_child_leaves",
                    }
                    if parent_lane_update
                    else None,
                    "source_before_decomposition": source,
                    "source_after_decomposition": updated_source,
                    "llm_payload": payload,
                    "llm_content_excerpt": ai["content_excerpt"],
                },
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-decomposition-decision-{marker['marker_id']}",
                run_id=run_id,
            ),
        )
        complete = await complete_work_review_processor_marker(
            marker["marker_id"],
            WorkReviewProcessorMarkerCompleteRequest(
                holder_id=holder_id,
                lease_token=lease_token,
                document_source_hash=marker["document_source_hash"],
                decision_id=decision["decision"]["decision_id"],
                status="processed",
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-preprocess-decomposed-complete-{marker['marker_id']}",
                run_id=run_id,
                metadata={
                    "schema": "xarta.kanban.preprocessing.hermes_profile_completion.v1",
                    "decision_id": decision["decision"]["decision_id"],
                    "processor_engine": ai["processor_engine"],
                    "provider_mode": ai["provider_mode"],
                    "profile": ai["profile"],
                    "primary_provider": ai["primary_provider"],
                    "primary_model": ai["primary_model"],
                    "model_alias": ai["model_alias"],
                    "prompt_sha256": ai["prompt_sha256"],
                    "readiness_marker_context_hash": readiness_marker["context_hash"],
                    "decomposition_total_count": decomposition_result["total_count"],
                    "decomposition_created_count": decomposition_result["created_count"],
                    "decomposition_existing_count": decomposition_result["existing_count"],
                },
            ),
        )
        return {
            "ok": True,
            "processor_kind": "preprocessing",
            "item_id": item_id,
            "marker_id": marker["marker_id"],
            "decision_id": decision["decision"]["decision_id"],
            "completed": complete.get("completed", False),
            "status": complete.get("marker", {}).get("status", ""),
            "provider_mode": ai["provider_mode"],
            "profile": ai["profile"],
            "primary_provider": ai["primary_provider"],
            "primary_model": ai["primary_model"],
            "model_alias": ai["model_alias"],
            "decomposition": {
                "total_count": decomposition_result["total_count"],
                "created_count": decomposition_result["created_count"],
                "existing_count": decomposition_result["existing_count"],
                "item_ids": [
                    child["item_id"]
                    for child in decomposition_result.get("items", [])
                    if isinstance(child, dict) and child.get("item_id")
                ],
            },
        }

    readiness_marker = _preprocessing_readiness_marker(
        item_id=item_id,
        source=source,
        marker=marker,
        actor=holder_id,
        now=now,
        ai_payload=payload,
        processor_route=ai,
        outcome=readiness_outcome,
    )
    metadata = dict(hints.get("metadata") or {})
    metadata["context_readiness_marker"] = readiness_marker
    await update_work_item_agent_hints(
        item_id,
        WorkAgentHintsUpdateRequest(
            metadata=metadata,
            actor=holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-readiness-{marker['marker_id']}",
            run_id=run_id,
        ),
    )
    decision_id = _clean_work_id(
        f"kanban-decision-hermes-preprocess-{item_id}-{source['document_source_hash'][-12:]}",
        "kanban-decision",
    )
    decision = await record_work_item_review_decision(
        item_id,
        WorkReviewDecisionCreateRequest(
            decision_id=decision_id,
            processor_kind="preprocessing",
            decision_type="context_readiness_marked",
            title=title,
            summary=summary,
            rationale=rationale,
            affected_refs=[
                f"kanban_items:{item_id}",
                f"xarta-kanban:item:{item_id}",
                f"kanban_agent_hints:{item_id}",
                f"kanban_review_processor_markers:{marker['marker_id']}",
            ],
            confidence=confidence,
            uncertainty=uncertainty,
            proof_refs=_local_ai_ref_list(payload, "proof_refs")
            or [
                f"kanban_review_processor_markers:{marker['marker_id']}",
                f"kanban_agent_hints:{item_id}",
            ],
            status="accepted",
            provider_mode=KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE,
            metadata={
                "schema": "xarta.kanban.preprocessing.hermes_profile_decision.v1",
                "worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
                "processor_engine": ai["processor_engine"],
                "provider_policy": _work_review_processing_policy()["active_mode"],
                "provider_mode": ai["provider_mode"],
                "profile": ai["profile"],
                "primary_provider": ai["primary_provider"],
                "primary_model": ai["primary_model"],
                "fallback_provider": ai["fallback_provider"],
                "fallback_model": ai["fallback_model"],
                "model_alias": ai["model_alias"],
                "response_model": ai["response_model"],
                "api_base": ai["api_base"],
                "prompt_sha256": ai["prompt_sha256"],
                "marker_id": marker["marker_id"],
                "document_source_hash": source["document_source_hash"],
                "readiness_marker": readiness_marker,
                "readiness_normalization": readiness_normalization or None,
                "llm_payload": payload,
                "llm_content_excerpt": ai["content_excerpt"],
            },
            actor=holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-preprocess-decision-{marker['marker_id']}",
            run_id=run_id,
        ),
    )
    complete = await complete_work_review_processor_marker(
        marker["marker_id"],
        WorkReviewProcessorMarkerCompleteRequest(
            holder_id=holder_id,
            lease_token=lease_token,
            document_source_hash=marker["document_source_hash"],
            decision_id=decision["decision"]["decision_id"],
            status="processed",
            actor=holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-preprocess-complete-{marker['marker_id']}",
            run_id=run_id,
            metadata={
                "schema": "xarta.kanban.preprocessing.hermes_profile_completion.v1",
                "decision_id": decision["decision"]["decision_id"],
                "processor_engine": ai["processor_engine"],
                "provider_mode": ai["provider_mode"],
                "profile": ai["profile"],
                "primary_provider": ai["primary_provider"],
                "primary_model": ai["primary_model"],
                "model_alias": ai["model_alias"],
                "prompt_sha256": ai["prompt_sha256"],
                "readiness_marker_context_hash": readiness_marker["context_hash"],
                "readiness_outcome": readiness_outcome,
                "readiness_normalization": readiness_normalization or None,
            },
        ),
    )
    return {
        "ok": True,
        "processor_kind": "preprocessing",
        "item_id": item_id,
        "marker_id": marker["marker_id"],
        "decision_id": decision["decision"]["decision_id"],
        "completed": complete.get("completed", False),
        "status": complete.get("marker", {}).get("status", ""),
        "provider_mode": ai["provider_mode"],
        "profile": ai["profile"],
        "primary_provider": ai["primary_provider"],
        "primary_model": ai["primary_model"],
        "model_alias": ai["model_alias"],
    }


async def _process_work_automation_claimed_marker(
    marker: dict[str, Any],
    *,
    holder_id: str,
    lease_token: str,
    run_id: str,
) -> dict[str, Any]:
    processor_kind = marker.get("processor_kind") or "review"
    try:
        if processor_kind == "preprocessing":
            return await _process_work_preprocessing_idle_marker(
                marker,
                holder_id=holder_id,
                lease_token=lease_token,
                run_id=run_id,
            )
        if processor_kind == "review":
            return await _process_work_review_idle_marker(
                marker,
                holder_id=holder_id,
                lease_token=lease_token,
                run_id=run_id,
            )
        complete = await complete_work_review_processor_marker(
            marker["marker_id"],
            WorkReviewProcessorMarkerCompleteRequest(
                holder_id=holder_id,
                lease_token=lease_token,
                document_source_hash=marker.get("document_source_hash") or "",
                status="skipped",
                error=f"unsupported_processor_kind:{processor_kind}",
                actor=holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-unsupported-{marker['marker_id']}",
                run_id=run_id,
            ),
        )
        return {
            "ok": False,
            "processor_kind": processor_kind,
            "item_id": marker.get("item_id") or "",
            "marker_id": marker["marker_id"],
            "reason": "unsupported_processor_kind",
            "status": complete.get("marker", {}).get("status", "skipped"),
        }
    except Exception as exc:
        failed_complete: dict[str, Any] | None = None
        error_class = _work_review_failure_error_class(str(exc), {})
        route = _work_automation_processor_profile_route(
            processor_kind if processor_kind in KANBAN_PROCESSOR_PROFILE_SPECS else "review"
        )
        with suppress(Exception):
            failed_complete = await complete_work_review_processor_marker(
                marker["marker_id"],
                WorkReviewProcessorMarkerCompleteRequest(
                    holder_id=holder_id,
                    lease_token=lease_token,
                    document_source_hash=marker.get("document_source_hash") or "",
                    status="failed",
                    error=_body_excerpt(str(exc), limit=1000),
                    actor=holder_id,
                    source_surface="kanban-automation-idle-worker",
                    request_id=f"{run_id}-failed-{marker['marker_id']}",
                    run_id=run_id,
                    metadata={
                        "schema": "xarta.kanban.automation.idle_worker.failure.v1",
                        "processor_kind": processor_kind,
                        "provider_mode": route["provider_mode"],
                        "processor_engine": route["processor_engine"],
                        "profile": route["profile"],
                        "primary_provider": route["primary_provider"],
                        "primary_model": route["primary_model"],
                        "model_alias": route["model_alias"],
                        "error_class": error_class,
                    },
                ),
            )
        return {
            "ok": False,
            "processor_kind": processor_kind,
            "item_id": marker.get("item_id") or "",
            "marker_id": marker["marker_id"],
            "reason": "hermes_profile_processing_failed",
            "error": _body_excerpt(str(exc), limit=1000),
            "status": (failed_complete or {}).get("marker", {}).get("status", "failed"),
            "provider_mode": route["provider_mode"],
            "profile": route["profile"],
            "primary_provider": route["primary_provider"],
            "primary_model": route["primary_model"],
            "model_alias": route["model_alias"],
        }


def _work_queued_processor_marker_ids_for_item_sync(clean_item_id: str) -> list[str]:
    with get_conn() as conn:
        scope_ids = _work_scope_item_ids(conn, clean_item_id) if clean_item_id else []
        return _work_queued_processor_marker_ids(conn, scope_ids)


async def run_work_kanban_automation_idle_tick(
    *,
    item_id: str | None = None,
    max_scan_items: int | None = None,
    max_process_items: int | None = None,
    holder_id: str | None = None,
    lease_ttl_seconds: int | None = None,
    marker_timeout_seconds: int | None = None,
) -> dict[str, Any]:
    config = _work_automation_idle_worker_config()
    if not config["enabled"]:
        return {
            "ok": True,
            "schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
            "enabled": False,
            "effective_enabled": False,
            "runs_on_this_node": bool(config.get("runs_on_this_node")),
            "current_node_id": config.get("current_node_id", ""),
            "owner_node_id": config.get("owner_node_id", ""),
            "singleton_override": config.get("singleton_override", {}),
            "reason": "idle_worker_disabled_by_configuration",
            "lease_acquired": False,
            "processed_count": 0,
            "processed_markers": [],
            "claim_results": [],
        }
    if not config["runs_on_this_node"]:
        return {
            "ok": True,
            "schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
            "enabled": True,
            "effective_enabled": False,
            "runs_on_this_node": False,
            "current_node_id": config["current_node_id"],
            "owner_node_id": config["owner_node_id"],
            "singleton_override": config["singleton_override"],
            "reason": "idle_worker_not_owner_node",
            "lease_acquired": False,
            "processed_count": 0,
            "processed_markers": [],
            "claim_results": [],
            "eligible_marker_count": 0,
            "eligible_marker_ids": [],
        }
    clean_item_id = _clean_short_text(item_id or config["root_item_id"], "", limit=180)
    scan_limit = _clean_review_scan_limit(max_scan_items or config["max_scan_items"])
    process_limit = max(1, min(int(max_process_items or config["max_process_items"]), 50))
    clean_holder_id = _clean_short_text(
        holder_id or config["holder_id"],
        config["holder_id"],
        limit=160,
    )
    ttl_seconds = _clean_review_lease_ttl(lease_ttl_seconds or config["lease_ttl_seconds"])
    timeout_seconds = _clean_review_lease_ttl(
        marker_timeout_seconds or config["marker_timeout_seconds"]
    )
    run_id = f"kanban-idle-worker-{uuid.uuid4().hex[:12]}"

    timeout_requeue = await requeue_timed_out_work_review_processor_markers(
        WorkReviewProcessorTimeoutRequeueRequest(
            item_id=clean_item_id or None,
            actor=clean_holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-requeue-timeouts",
            run_id=run_id,
            metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
        )
    )
    review_scan = await trigger_work_review_processor_idle_scan(
        WorkReviewProcessorIdleScanRequest(
            item_id=clean_item_id or None,
            max_items=scan_limit,
            include_empty=False,
            actor=clean_holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-review-scan",
            run_id=run_id,
            metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
        )
    )
    preprocessing_scan = await trigger_work_preprocessing_idle_scan(
        WorkPreprocessingIdleScanRequest(
            item_id=clean_item_id or None,
            max_items=scan_limit,
            actor=clean_holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-preprocessing-scan",
            run_id=run_id,
            metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
        )
    )
    eligible_marker_ids = await _run_personal_sync_work(
        _work_queued_processor_marker_ids_for_item_sync,
        clean_item_id,
    )
    lease = await acquire_work_review_processor_lease(
        WorkReviewProcessorLeaseRequest(
            holder_id=clean_holder_id,
            item_id=clean_item_id or None,
            ttl_seconds=ttl_seconds,
            actor=clean_holder_id,
            source_surface="kanban-automation-idle-worker",
            request_id=f"{run_id}-lease",
            run_id=run_id,
            metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
        )
    )
    processed_markers: list[dict[str, Any]] = []
    claim_results: list[dict[str, Any]] = []
    release: dict[str, Any] | None = None
    if not lease.get("acquired"):
        return {
            "ok": True,
            "schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
            "run_id": run_id,
            "item_id": clean_item_id,
            "enabled": True,
            "effective_enabled": True,
            "runs_on_this_node": True,
            "current_node_id": config["current_node_id"],
            "owner_node_id": config["owner_node_id"],
            "singleton_override": config["singleton_override"],
            "reason": lease.get("reason") or "lease_not_acquired",
            "lease_acquired": False,
            "timeout_requeue": timeout_requeue,
            "review_scan": review_scan,
            "preprocessing_scan": preprocessing_scan,
            "eligible_marker_count": len(eligible_marker_ids),
            "eligible_marker_ids": eligible_marker_ids,
            "processed_count": 0,
            "processed_markers": [],
            "claim_results": [],
        }
    lease_token = lease["lease"].get("lease_token") or ""
    try:
        for index in range(process_limit):
            await heartbeat_work_review_processor_lease(
                WorkReviewProcessorLeaseRequest(
                    holder_id=clean_holder_id,
                    item_id=clean_item_id or None,
                    lease_token=lease_token,
                    ttl_seconds=ttl_seconds,
                    actor=clean_holder_id,
                    source_surface="kanban-automation-idle-worker",
                    request_id=f"{run_id}-heartbeat-{index}",
                    run_id=run_id,
                )
            )
            try:
                claimed = await claim_next_work_review_processor_marker(
                    WorkReviewProcessorMarkerClaimRequest(
                        holder_id=clean_holder_id,
                        lease_token=lease_token,
                        item_id=clean_item_id or None,
                        timeout_seconds=timeout_seconds,
                        eligible_marker_ids=eligible_marker_ids,
                        actor=clean_holder_id,
                        source_surface="kanban-automation-idle-worker",
                        request_id=f"{run_id}-claim-{index}",
                        run_id=run_id,
                        metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
                    )
                )
            except HTTPException as exc:
                detail = str(exc.detail or "")
                if exc.status_code == 409 and "lease" in detail.lower():
                    claim_results.append(
                        {
                            "claimed": False,
                            "reason": "lease_not_active_during_claim",
                            "detail": detail,
                            "marker_id": "",
                            "processor_kind": "",
                        }
                    )
                    break
                raise
            claim_results.append(
                {
                    "claimed": bool(claimed.get("claimed")),
                    "reason": claimed.get("reason") or "",
                    "marker_id": (claimed.get("marker") or {}).get("marker_id", ""),
                    "processor_kind": (claimed.get("marker") or {}).get("processor_kind", ""),
                }
            )
            if not claimed.get("claimed"):
                break
            processed_markers.append(
                await _process_work_automation_claimed_marker(
                    claimed["marker"],
                    holder_id=clean_holder_id,
                    lease_token=lease_token,
                    run_id=run_id,
                )
            )
    finally:
        release = await release_work_review_processor_lease(
            WorkReviewProcessorLeaseRequest(
                holder_id=clean_holder_id,
                item_id=clean_item_id or None,
                lease_token=lease_token,
                actor=clean_holder_id,
                source_surface="kanban-automation-idle-worker",
                request_id=f"{run_id}-release",
                run_id=run_id,
                metadata={"worker_schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
            )
        )
    return {
        "ok": True,
        "schema": KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA,
        "run_id": run_id,
        "item_id": clean_item_id,
        "enabled": True,
        "effective_enabled": True,
        "runs_on_this_node": True,
        "current_node_id": config["current_node_id"],
        "owner_node_id": config["owner_node_id"],
        "singleton_override": config["singleton_override"],
        "lease_acquired": True,
        "timeout_requeue": timeout_requeue,
        "review_scan": review_scan,
        "preprocessing_scan": preprocessing_scan,
        "eligible_marker_count": len(eligible_marker_ids),
        "eligible_marker_ids": eligible_marker_ids,
        "processed_count": len(processed_markers),
        "processed_markers": processed_markers,
        "claim_results": claim_results,
        "release": release,
    }


async def run_work_kanban_automation_idle_loop() -> None:
    config = _work_automation_idle_worker_config()
    if not config["enabled"]:
        log.info("Kanban automation idle worker disabled by configuration")
        return
    if not config["runs_on_this_node"]:
        log.info(
            "Kanban automation idle worker not started on %s; owner node is %s",
            config["current_node_id"] or "unknown-node",
            config["owner_node_id"] or "unknown-owner",
        )
        return
    initial_delay = int(config["initial_delay_seconds"] or 0)
    if initial_delay > 0:
        await asyncio.sleep(initial_delay)
    while True:
        try:
            with timing.span("background_task_cycle", task="kanban_automation_idle_tick"):
                result = await run_work_kanban_automation_idle_tick()
            if int(result.get("processed_count") or 0) > 0:
                log.info(
                    "Kanban automation idle worker processed %s marker(s)",
                    result["processed_count"],
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Kanban automation idle worker tick failed")
        await asyncio.sleep(int(config["interval_seconds"] or 60))


@router.post("/kanban/automation/idle-worker/tick")
async def trigger_work_automation_idle_worker_tick(
    body: WorkAutomationIdleTickRequest,
) -> dict[str, Any]:
    meta = _work_request_meta(body)
    return await run_work_kanban_automation_idle_tick(
        item_id=body.item_id,
        max_scan_items=body.max_scan_items,
        max_process_items=body.max_process_items,
        holder_id=body.holder_id or meta["actor"],
        lease_ttl_seconds=body.lease_ttl_seconds,
        marker_timeout_seconds=body.marker_timeout_seconds,
    )


@router.post("/kanban/automation/review-processor/idle-scan")
async def trigger_work_review_processor_idle_scan(
    body: WorkReviewProcessorIdleScanRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_trigger_work_review_processor_idle_scan_sync, body)


def _trigger_work_review_processor_idle_scan_sync(
    body: WorkReviewProcessorIdleScanRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    scan_limit = _clean_review_scan_limit(body.max_items)
    include_empty = bool(body.include_empty)
    with _kanban_automation_scan_conn(operation="review_processor_idle_scan") as conn:
        root_item = _work_item_or_404(conn, clean_item_id) if clean_item_id else None
        scope_ids = _work_scope_item_ids(conn, clean_item_id) if clean_item_id else []
        args: list[Any] = []
        where = (
            "WHERE item.status != 'archived' "
            f"AND {_work_item_automation_included_predicate('item')}"
        )
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where += f" AND item.item_id IN ({placeholders})"
            args.extend(scope_ids)
        item_rows = conn.execute(
            f"""
            SELECT item.* FROM kanban_items item
            {where}
            ORDER BY item.updated_at DESC, item.item_id
            LIMIT ?
            """,
            [*args, scan_limit],
        ).fetchall()
        scan_entries: list[dict[str, Any]] = []
        skipped_empty_entries: list[dict[str, Any]] = []
        skipped_empty = 0
        for item_row in item_rows:
            document = _work_item_review_document(conn, item_row["item_id"])
            document_source = _review_document_source(document)
            if not document_source["has_review_text"] and not include_empty:
                skipped_empty += 1
                skipped_empty_entries.append(
                    {
                        "item": item_row,
                        "document": document,
                        "document_source": document_source,
                    }
                )
                continue
            scan_entries.append(
                {
                    "item": item_row,
                    "document": document,
                    "document_source": document_source,
                }
            )

        _kanban_begin_write_transaction(conn)
        queued_rows: list[Any] = []
        cancelled_rows: list[Any] = []
        unchanged_current = 0
        unchanged_pending = 0
        unchanged_failed = 0
        for entry in skipped_empty_entries:
            item_id = entry["item"]["item_id"]
            document_source = entry["document_source"]
            marker_id = _review_processor_marker_id(item_id)
            existing = conn.execute(
                "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
                (marker_id,),
            ).fetchone()
            if existing is None or existing["status"] not in {"queued", "processing"}:
                continue
            updated_row = _work_review_processor_marker_update_row(
                existing,
                updates={
                    "status": "cancelled",
                    "document_ref": document_source["document_ref"],
                    "document_updated_at": document_source["document_updated_at"],
                    "document_source_hash": document_source["document_source_hash"],
                    "last_seen_at": now,
                    "processing_started_at": "",
                    "processing_expires_at": "",
                    "last_error": "review_document_deleted",
                },
                meta=meta,
                now=now,
                reason="review_document_deleted",
                metadata={
                    **dict(body.metadata or {}),
                    "document_exists": document_source["document_exists"],
                    "body_bytes": document_source["body_bytes"],
                    "cancelled_previous_status": existing["status"],
                },
            )
            cancelled_rows.append(_write_work_review_processor_marker(conn, updated_row))
        for entry in scan_entries:
            item_id = entry["item"]["item_id"]
            document_source = entry["document_source"]
            marker_id = _review_processor_marker_id(item_id)
            existing = conn.execute(
                "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
                (marker_id,),
            ).fetchone()
            if existing is not None:
                same_document = (
                    existing["document_source_hash"] == document_source["document_source_hash"]
                )
                already_processed = (
                    _work_marker_successful_source_hash(existing)
                    == document_source["document_source_hash"]
                )
                if same_document and existing["status"] in {"queued", "processing"}:
                    unchanged_pending += 1
                    continue
                if same_document and existing["status"] in {"failed", "skipped", "cancelled"}:
                    unchanged_failed += 1
                    continue
                if already_processed and existing["status"] == "processed":
                    unchanged_current += 1
                    continue
            reason = "new_review_document" if existing is None else "review_document_changed"
            marker_row = _work_review_processor_marker_row(
                existing=existing,
                item_id=item_id,
                document=entry["document"],
                document_source=document_source,
                meta=meta,
                now=now,
                reason=reason,
                scan_metadata=dict(body.metadata or {}),
            )
            queued_rows.append(_write_work_review_processor_marker(conn, marker_row))

        source_hash = _hash_json_payload(
            {
                "action": "trigger_review_processor_idle_scan",
                "item_id": clean_item_id,
                "scan_limit": scan_limit,
                "scanned_count": len(item_rows),
                "eligible_review_count": len(scan_entries),
                "queued_count": len(queued_rows),
                "skipped_empty_count": skipped_empty,
                "cancelled_deleted_count": len(cancelled_rows),
                "unchanged_current_count": unchanged_current,
                "unchanged_pending_count": unchanged_pending,
                "unchanged_failed_count": unchanged_failed,
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="trigger_review_processor_idle_scan",
            target_ref="kanban_review_processor_markers:idle-scan",
            item_id=clean_item_id,
            parent_item_id=(root_item["parent_item_id"] or "") if root_item else "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "schema": KANBAN_REVIEW_SCHEDULER_SCHEMA,
                "scan_limit": scan_limit,
                "scanned_count": len(item_rows),
                "eligible_review_count": len(scan_entries),
                "queued_count": len(queued_rows),
                "skipped_empty_count": skipped_empty,
                "cancelled_deleted_count": len(cancelled_rows),
                "unchanged_current_count": unchanged_current,
                "unchanged_pending_count": unchanged_pending,
                "unchanged_failed_count": unchanged_failed,
                "provider_mode": _work_review_processing_policy()["active_mode"],
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-review-idle-scan")
        for marker_row in cancelled_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        for marker_row in queued_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        marker_stats = _work_review_processor_marker_stats(conn, scope_ids)
        queued_markers = [
            _row_to_work_review_processor_marker(marker_row) for marker_row in queued_rows
        ]
        cancelled_markers = [
            _row_to_work_review_processor_marker(marker_row) for marker_row in cancelled_rows
        ]
    return {
        "ok": True,
        "schema": KANBAN_REVIEW_SCHEDULER_SCHEMA,
        "scanned_count": len(item_rows),
        "eligible_review_count": len(scan_entries),
        "queued_count": len(queued_markers),
        "skipped_empty_count": skipped_empty,
        "cancelled_deleted_count": len(cancelled_markers),
        "unchanged_current_count": unchanged_current,
        "unchanged_pending_count": unchanged_pending,
        "unchanged_failed_count": unchanged_failed,
        "queued_markers": queued_markers,
        "cancelled_markers": cancelled_markers,
        "scheduler": marker_stats,
        "audit": {
            "audit_id": audit_id,
            "action": "trigger_review_processor_idle_scan",
            "result": "ok",
        },
    }


@router.post("/kanban/automation/preprocessing/idle-scan")
async def trigger_work_preprocessing_idle_scan(
    body: WorkPreprocessingIdleScanRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_trigger_work_preprocessing_idle_scan_sync, body)


def _trigger_work_preprocessing_idle_scan_sync(
    body: WorkPreprocessingIdleScanRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    scan_limit = _clean_review_scan_limit(body.max_items)
    with _kanban_automation_scan_conn(operation="preprocessing_idle_scan") as conn:
        root_item = _work_item_or_404(conn, clean_item_id) if clean_item_id else None
        scope_ids = _work_scope_item_ids(conn, clean_item_id) if clean_item_id else []
        _kanban_begin_write_transaction(conn)
        (
            cancelled_invalid_rows,
            cancelled_invalid_marker_blockers,
        ) = _cancel_invalid_work_preprocessing_markers(
            conn,
            scope_ids=scope_ids,
            meta=meta,
            now=now,
            metadata=dict(body.metadata or {}),
        )
        active_rows = _work_preprocessing_active_rows(conn, scope_ids)
        if active_rows:
            source_hash = _hash_json_payload(
                {
                    "action": "trigger_preprocessing_idle_scan",
                    "item_id": clean_item_id,
                    "reason": "active_preprocessing",
                    "active_count": len(active_rows),
                    "active_item_ids": [row["item_id"] for row in active_rows],
                    "cancelled_invalid_count": len(cancelled_invalid_rows),
                }
            )
            audit_row = _write_work_audit(
                conn,
                audit_id=audit_id,
                actor=meta["actor"],
                source_surface=meta["source_surface"],
                action="trigger_preprocessing_idle_scan",
                target_ref="kanban_review_processor_markers:preprocessing-idle-scan",
                item_id=clean_item_id,
                parent_item_id=(root_item["parent_item_id"] or "") if root_item else "",
                created_at=now,
                request_id=meta["request_id"],
                run_id=meta["run_id"],
                result="noop",
                source_hash=source_hash,
                metadata={
                    "schema": KANBAN_PREPROCESSING_QUEUE_SCHEMA,
                    "reason": "active_preprocessing",
                    "active_count": len(active_rows),
                    "active_item_ids": [row["item_id"] for row in active_rows],
                    "cancelled_invalid_count": len(cancelled_invalid_rows),
                    "provider_mode": _work_review_processing_policy()["active_mode"],
                },
            )
            gen = _kanban_table_sync_gen(conn, "kanban-preprocessing-idle-scan")
            for marker_row in cancelled_invalid_rows:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_review_processor_markers",
                    marker_row["marker_id"],
                    dict(marker_row),
                    gen,
                )
            for marker_blocker in cancelled_invalid_marker_blockers:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_blockers",
                    marker_blocker["blocker_row"]["blocker_id"],
                    dict(marker_blocker["blocker_row"]),
                    gen,
                )
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_audit_log",
                    marker_blocker["audit_row"]["audit_id"],
                    marker_blocker["audit_row"],
                    gen,
                )
            enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
            cancelled_invalid_markers = [
                _row_to_work_review_processor_marker(row) for row in cancelled_invalid_rows
            ]
            return {
                "ok": True,
                "schema": KANBAN_PREPROCESSING_QUEUE_SCHEMA,
                "idle": False,
                "reason": "active_preprocessing",
                "blocked_by_active_count": len(active_rows),
                "active_markers": [
                    _row_to_work_review_processor_marker(row) for row in active_rows
                ],
                "scanned_count": 0,
                "eligible_preprocessing_count": 0,
                "queued_count": 0,
                "cancelled_current_count": 0,
                "cancelled_invalid_count": len(cancelled_invalid_markers),
                "unchanged_current_count": 0,
                "unchanged_pending_count": 0,
                "unchanged_failed_count": 0,
                "queued_markers": [],
                "cancelled_markers": [],
                "cancelled_invalid_markers": cancelled_invalid_markers,
                "scheduler": _work_review_processor_marker_stats(
                    conn,
                    scope_ids,
                    processor_kind="preprocessing",
                ),
                "audit": {
                    "audit_id": audit_id,
                    "action": "trigger_preprocessing_idle_scan",
                    "result": "noop",
                },
            }

        args: list[Any] = []
        where = f"""
        WHERE {_work_preprocessing_candidate_predicate("item")}
        """
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where += f" AND item.item_id IN ({placeholders})"
            args.extend(scope_ids)
        with timing.span(
            "kanban.preprocessing.idle_scan.phase",
            phase="candidate_detection",
            active_store=cfg.KANBAN_DATASTORE_CONFIG.active_store,
            scan_limit=scan_limit,
            scoped=bool(scope_ids),
        ):
            item_rows = conn.execute(
                f"""
                SELECT item.* FROM kanban_items item
                {where}
                ORDER BY
                  CASE item.priority_id
                    WHEN 'critical' THEN 0
                    WHEN 'high' THEN 1
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 3
                    ELSE 4
                  END,
                  item.updated_at ASC,
                  item.item_id
                LIMIT ?
                """,
                [*args, scan_limit],
            ).fetchall()
        document_cache: dict[tuple[str, str], dict[str, Any]] = {}
        satisfied_parent_context_blockers: list[dict[str, Any]] = []
        with timing.span(
            "kanban.preprocessing.idle_scan.phase",
            phase="parent_context_blockers",
            active_store=cfg.KANBAN_DATASTORE_CONFIG.active_store,
            item_count=len(item_rows),
        ):
            for item_row in item_rows:
                satisfied_parent_context_blockers.extend(
                    _resolve_satisfied_work_preprocessing_parent_context_blockers(
                        conn,
                        item_row=item_row,
                        meta=meta,
                        now=now,
                        document_cache=document_cache,
                    )
                )
        counts_by_item = _work_preprocessing_counts_by_item(conn, item_rows)
        with timing.span(
            "kanban.preprocessing.idle_scan.phase",
            phase="source_build",
            active_store=cfg.KANBAN_DATASTORE_CONFIG.active_store,
            item_count=len(item_rows),
        ):
            scan_entries = [
                {
                    "item": item_row,
                    "source": _work_preprocessing_context_source(
                        conn,
                        item_row,
                        counts=counts_by_item.get(str(item_row["item_id"] or "")),
                        document_cache=document_cache,
                    ),
                }
                for item_row in item_rows
            ]

        queued_rows: list[Any] = []
        cancelled_rows: list[Any] = []
        cancelled_marker_blockers: list[dict[str, Any]] = []
        stale_marker_blockers: list[dict[str, Any]] = []
        unchanged_current = 0
        unchanged_pending = 0
        unchanged_failed = 0
        current_ready = 0
        eligible_preprocessing = 0
        marker_ids = [_preprocessing_marker_id(entry["item"]["item_id"]) for entry in scan_entries]
        existing_markers_by_id: dict[str, Any] = {}
        if marker_ids:
            placeholders = ",".join("?" for _ in marker_ids)
            marker_rows = conn.execute(
                f"""
                SELECT * FROM kanban_review_processor_markers
                WHERE marker_id IN ({placeholders})
                """,
                marker_ids,
            ).fetchall()
            existing_markers_by_id = {row["marker_id"]: row for row in marker_rows}
        for entry in scan_entries:
            item_id = entry["item"]["item_id"]
            source = entry["source"]
            stale_marker_blockers.extend(
                _resolve_stale_work_processor_marker_blockers(
                    conn,
                    item_id=item_id,
                    source=source,
                    meta=meta,
                    now=now,
                )
            )
            marker_id = _preprocessing_marker_id(item_id)
            existing = existing_markers_by_id.get(marker_id)
            same_source = bool(
                existing is not None
                and existing["document_source_hash"] == source["document_source_hash"]
            )
            already_processed = bool(
                existing is not None
                and _work_marker_successful_source_hash(existing) == source["document_source_hash"]
            )
            if source["ready"] or not source.get("needs_preprocessing", True):
                current_ready += 1
                if existing is not None and existing["status"] in {
                    "queued",
                    "processing",
                    "failed",
                }:
                    cancelled_row = _work_review_processor_marker_update_row(
                        existing,
                        updates={
                            "status": "cancelled",
                            "document_ref": source["document_ref"],
                            "document_updated_at": source["document_updated_at"],
                            "document_source_hash": source["document_source_hash"],
                            "last_seen_at": now,
                            "processing_started_at": "",
                            "processing_expires_at": "",
                            "last_error": "preprocessing_current",
                        },
                        meta=meta,
                        now=now,
                        reason="preprocessing_current",
                        metadata={
                            **dict(body.metadata or {}),
                            "readiness_reason": source["reason"],
                            "cancelled_previous_status": existing["status"],
                        },
                    )
                    saved_cancelled_row = _write_work_review_processor_marker(conn, cancelled_row)
                    cancelled_rows.append(saved_cancelled_row)
                    marker_blocker = _upsert_work_processor_marker_blocker(
                        conn,
                        saved_cancelled_row,
                        meta=meta,
                        now=now,
                    )
                    if marker_blocker is not None:
                        cancelled_marker_blockers.append(marker_blocker)
                elif already_processed and existing["status"] == "processed":
                    unchanged_current += 1
                continue

            eligible_preprocessing += 1
            if existing is not None:
                if same_source and existing["status"] in {"queued", "processing"}:
                    unchanged_pending += 1
                    continue
                if same_source and existing["status"] in {"failed", "skipped", "cancelled"}:
                    unchanged_failed += 1
                    continue
                if already_processed and existing["status"] == "processed":
                    unchanged_current += 1
                    continue
            reason = source["reason"] if existing is None else "preprocessing_context_changed"
            marker_row = _work_preprocessing_marker_row(
                existing=existing,
                item_id=item_id,
                source=source,
                meta=meta,
                now=now,
                reason=reason,
                scan_metadata=dict(body.metadata or {}),
            )
            queued_rows.append(_write_work_review_processor_marker(conn, marker_row))

        source_hash = _hash_json_payload(
            {
                "action": "trigger_preprocessing_idle_scan",
                "item_id": clean_item_id,
                "scan_limit": scan_limit,
                "scanned_count": len(item_rows),
                "eligible_preprocessing_count": eligible_preprocessing,
                "queued_count": len(queued_rows),
                "cancelled_current_count": len(cancelled_rows),
                "cancelled_invalid_count": len(cancelled_invalid_rows),
                "stale_marker_blocker_resolved_count": len(stale_marker_blockers),
                "satisfied_parent_context_blocker_resolved_count": len(
                    satisfied_parent_context_blockers
                ),
                "current_ready_count": current_ready,
                "unchanged_current_count": unchanged_current,
                "unchanged_pending_count": unchanged_pending,
                "unchanged_failed_count": unchanged_failed,
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="trigger_preprocessing_idle_scan",
            target_ref="kanban_review_processor_markers:preprocessing-idle-scan",
            item_id=clean_item_id,
            parent_item_id=(root_item["parent_item_id"] or "") if root_item else "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "schema": KANBAN_PREPROCESSING_QUEUE_SCHEMA,
                "scan_limit": scan_limit,
                "scanned_count": len(item_rows),
                "eligible_preprocessing_count": eligible_preprocessing,
                "queued_count": len(queued_rows),
                "cancelled_current_count": len(cancelled_rows),
                "cancelled_invalid_count": len(cancelled_invalid_rows),
                "stale_marker_blocker_resolved_count": len(stale_marker_blockers),
                "satisfied_parent_context_blocker_resolved_count": len(
                    satisfied_parent_context_blockers
                ),
                "current_ready_count": current_ready,
                "unchanged_current_count": unchanged_current,
                "unchanged_pending_count": unchanged_pending,
                "unchanged_failed_count": unchanged_failed,
                "provider_mode": _work_review_processing_policy()["active_mode"],
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-preprocessing-idle-scan")
        for marker_row in cancelled_invalid_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        for marker_row in cancelled_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        for marker_blocker in cancelled_marker_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        for marker_blocker in cancelled_invalid_marker_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        for marker_blocker in stale_marker_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        for marker_blocker in satisfied_parent_context_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        for marker_row in queued_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                marker_row["marker_id"],
                dict(marker_row),
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        queued_markers = [
            _row_to_work_review_processor_marker(marker_row) for marker_row in queued_rows
        ]
        cancelled_markers = [
            _row_to_work_review_processor_marker(marker_row) for marker_row in cancelled_rows
        ]
        cancelled_invalid_markers = [
            _row_to_work_review_processor_marker(marker_row)
            for marker_row in cancelled_invalid_rows
        ]
        scheduler = _work_review_processor_marker_stats(
            conn,
            scope_ids,
            processor_kind="preprocessing",
        )
    return {
        "ok": True,
        "schema": KANBAN_PREPROCESSING_QUEUE_SCHEMA,
        "idle": True,
        "reason": "idle",
        "blocked_by_active_count": 0,
        "scanned_count": len(item_rows),
        "eligible_preprocessing_count": eligible_preprocessing,
        "queued_count": len(queued_markers),
        "cancelled_current_count": len(cancelled_markers),
        "cancelled_invalid_count": len(cancelled_invalid_markers),
        "stale_marker_blocker_resolved_count": len(stale_marker_blockers),
        "satisfied_parent_context_blocker_resolved_count": len(satisfied_parent_context_blockers),
        "current_ready_count": current_ready,
        "unchanged_current_count": unchanged_current,
        "unchanged_pending_count": unchanged_pending,
        "unchanged_failed_count": unchanged_failed,
        "queued_markers": queued_markers,
        "cancelled_markers": cancelled_markers,
        "cancelled_invalid_markers": cancelled_invalid_markers,
        "scheduler": scheduler,
        "audit": {
            "audit_id": audit_id,
            "action": "trigger_preprocessing_idle_scan",
            "result": "ok",
        },
    }


@router.post("/kanban/automation/review-processor/markers/claim-next")
async def claim_next_work_review_processor_marker(
    body: WorkReviewProcessorMarkerClaimRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_claim_next_work_review_processor_marker_sync, body)


def _claim_next_work_review_processor_marker_sync(
    body: WorkReviewProcessorMarkerClaimRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    meta = _work_request_meta(body)
    holder_id = _clean_short_text(body.holder_id, "", limit=160)
    if not holder_id:
        raise HTTPException(400, "Review Processor marker holder_id is required")
    lease_token = _clean_short_text(body.lease_token or "", "", limit=220)
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    clean_eligible_marker_ids: list[str] | None = None
    if body.eligible_marker_ids is not None:
        clean_eligible_marker_ids = [
            _clean_short_text(marker_id, "", limit=180)
            for marker_id in body.eligible_marker_ids[:500]
        ]
        clean_eligible_marker_ids = [
            marker_id for marker_id in clean_eligible_marker_ids if marker_id
        ]
    ttl_seconds = _clean_review_lease_ttl(body.timeout_seconds)
    processing_expires_at = _utc_iso_from_datetime(now_dt + timedelta(seconds=ttl_seconds))
    audit_id = f"audit-{uuid.uuid4().hex}"
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        lease_row = _require_work_review_processor_lease_owner(
            conn,
            holder_id=holder_id,
            lease_token=lease_token,
            now_dt=now_dt,
        )
        scope_ids = _work_scope_item_ids(conn, clean_item_id) if clean_item_id else []
        args: list[Any] = [now]
        where = (
            f"WHERE {_work_processor_marker_claimable_state_predicate()} "
            f"AND {_work_processor_marker_claimable_predicate()}"
        )
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where += f" AND marker.item_id IN ({placeholders})"
            args.extend(scope_ids)
        if clean_eligible_marker_ids is not None:
            if clean_eligible_marker_ids:
                placeholders = ",".join("?" for _ in clean_eligible_marker_ids)
                where += f" AND marker.marker_id IN ({placeholders})"
                args.extend(clean_eligible_marker_ids)
            else:
                where += " AND FALSE"
        marker_row = conn.execute(
            f"""
            SELECT marker.* FROM kanban_review_processor_markers marker
            JOIN kanban_items item ON item.item_id=marker.item_id
            {where}
            ORDER BY
              CASE marker.processor_kind
                WHEN 'review' THEN 0
                WHEN 'preprocessing' THEN 1
                ELSE 2
              END,
              marker.queued_at ASC,
              marker.document_updated_at ASC,
              marker.marker_id
            LIMIT 1
            """,
            args,
        ).fetchone()
        if marker_row is None:
            source_hash = _hash_json_payload(
                {
                    "action": "claim_review_processor_marker",
                    "holder_id": holder_id,
                    "item_id": clean_item_id,
                    "reason": "no_queued_marker",
                    "eligible_marker_count": len(clean_eligible_marker_ids)
                    if clean_eligible_marker_ids is not None
                    else None,
                }
            )
            audit_row = _write_work_audit(
                conn,
                audit_id=audit_id,
                actor=meta["actor"],
                source_surface=meta["source_surface"],
                action="claim_review_processor_marker",
                target_ref="kanban_review_processor_markers:claim-next",
                item_id=clean_item_id,
                parent_item_id="",
                created_at=now,
                request_id=meta["request_id"],
                run_id=meta["run_id"],
                result="noop",
                source_hash=source_hash,
                metadata={
                    "reason": "no_queued_marker",
                    "holder_id": holder_id,
                    "eligible_marker_count": len(clean_eligible_marker_ids)
                    if clean_eligible_marker_ids is not None
                    else None,
                },
            )
            gen = _kanban_table_sync_gen(conn, "kanban-review-marker-claim")
            enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
            return {
                "ok": True,
                "claimed": False,
                "reason": "no_queued_marker",
                "scheduler": _work_review_processor_marker_stats(conn, scope_ids),
                "audit": {
                    "audit_id": audit_id,
                    "action": "claim_review_processor_marker",
                    "result": "noop",
                },
            }
        updated_row = _work_review_processor_marker_update_row(
            marker_row,
            updates={
                "status": "processing",
                "processing_started_at": now,
                "processing_expires_at": processing_expires_at,
                "attempt_count": int(marker_row["attempt_count"] or 0) + 1,
                "last_error": "",
                "next_retry_at": "",
                "retry_after_seconds": 0,
                "last_seen_at": now,
            },
            meta=meta,
            now=now,
            reason="claimed_for_processing",
            metadata={
                **dict(body.metadata or {}),
                "holder_id": holder_id,
                "lease_id": lease_row["lease_id"],
                "timeout_seconds": ttl_seconds,
                "eligible_marker_count": len(clean_eligible_marker_ids)
                if clean_eligible_marker_ids is not None
                else None,
            },
        )
        saved_row = _write_work_review_processor_marker(conn, updated_row)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="claim_review_processor_marker",
            target_ref=f"kanban_review_processor_markers:{saved_row['marker_id']}",
            item_id=saved_row["item_id"],
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=saved_row["source_hash"],
            metadata={
                "holder_id": holder_id,
                "lease_id": lease_row["lease_id"],
                "timeout_seconds": ttl_seconds,
                "attempt_count": int(saved_row["attempt_count"] or 0),
                "eligible_marker_count": len(clean_eligible_marker_ids)
                if clean_eligible_marker_ids is not None
                else None,
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-review-marker-claim")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_review_processor_markers",
            saved_row["marker_id"],
            dict(saved_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        marker = _row_to_work_review_processor_marker(saved_row)
        return {
            "ok": True,
            "claimed": True,
            "marker": marker,
            "scheduler": _work_review_processor_marker_stats(conn, scope_ids),
            "audit": {
                "audit_id": audit_id,
                "action": "claim_review_processor_marker",
                "result": "ok",
            },
        }


@router.post("/kanban/automation/review-processor/markers/{marker_id}/complete")
async def complete_work_review_processor_marker(
    marker_id: str,
    body: WorkReviewProcessorMarkerCompleteRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _complete_work_review_processor_marker_sync, marker_id, body
    )


def _complete_work_review_processor_marker_sync(
    marker_id: str,
    body: WorkReviewProcessorMarkerCompleteRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    meta = _work_request_meta(body)
    holder_id = _clean_short_text(body.holder_id, "", limit=160)
    if not holder_id:
        raise HTTPException(400, "Review Processor marker holder_id is required")
    lease_token = _clean_short_text(body.lease_token or "", "", limit=220)
    clean_marker_id = _clean_short_text(marker_id, "", limit=180)
    expected_source_hash = _clean_short_text(body.document_source_hash or "", "", limit=120)
    outcome_status = _clean_review_marker_outcome_status(body.status)
    audit_id = f"audit-{uuid.uuid4().hex}"
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        lease_row = _require_work_review_processor_lease_owner(
            conn,
            holder_id=holder_id,
            lease_token=lease_token,
            now_dt=now_dt,
        )
        marker_row = conn.execute(
            "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
            (clean_marker_id,),
        ).fetchone()
        if marker_row is None:
            raise HTTPException(404, "Review Processor marker not found")
        if marker_row["status"] != "processing":
            return {
                "ok": True,
                "completed": False,
                "reason": "marker_not_processing",
                "marker": _row_to_work_review_processor_marker(marker_row),
            }
        if outcome_status != "cancelled" and _work_item_automation_excluded(
            conn, marker_row["item_id"]
        ):
            decision_id = _clean_short_text(
                body.decision_id or marker_row["decision_id"], "", limit=180
            )
            saved_row = _write_work_review_processor_marker(
                conn,
                _work_review_processor_marker_update_row(
                    marker_row,
                    updates={
                        "status": "cancelled",
                        "processing_expires_at": "",
                        "last_seen_at": now,
                        "decision_id": decision_id,
                        "last_error": "automation_excluded",
                        "processed_document_updated_at": marker_row["document_updated_at"],
                        "processed_source_hash": marker_row["document_source_hash"],
                        "processed_at": now,
                    },
                    meta=meta,
                    now=now,
                    reason="automation_excluded_cancelled",
                    metadata={
                        **dict(body.metadata or {}),
                        "holder_id": holder_id,
                        "lease_id": lease_row["lease_id"],
                        "decision_id": decision_id,
                        "outcome_status": "cancelled",
                        "last_outcome_at": now,
                        "last_outcome_status": "automation_excluded_cancelled",
                        "last_error": "automation_excluded",
                    },
                ),
            )
            marker_blocker = _upsert_work_processor_marker_blocker(
                conn,
                saved_row,
                meta=meta,
                now=now,
            )
            audit_row = _write_work_audit(
                conn,
                audit_id=audit_id,
                actor=meta["actor"],
                source_surface=meta["source_surface"],
                action="complete_review_processor_marker",
                target_ref=f"kanban_review_processor_markers:{saved_row['marker_id']}",
                item_id=saved_row["item_id"],
                parent_item_id="",
                created_at=now,
                request_id=meta["request_id"],
                run_id=meta["run_id"],
                result="automation_excluded_cancelled",
                source_hash=saved_row["source_hash"],
                metadata={
                    "holder_id": holder_id,
                    "lease_id": lease_row["lease_id"],
                    "decision_id": decision_id,
                    "outcome_status": "cancelled",
                    "reason": "automation_excluded",
                },
            )
            gen = _kanban_table_sync_gen(conn, "kanban-review-marker-complete")
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                saved_row["marker_id"],
                dict(saved_row),
                gen,
            )
            if marker_blocker is not None:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_blockers",
                    marker_blocker["blocker_row"]["blocker_id"],
                    dict(marker_blocker["blocker_row"]),
                    gen,
                )
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_audit_log",
                    marker_blocker["audit_row"]["audit_id"],
                    marker_blocker["audit_row"],
                    gen,
                )
            enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
            return {
                "ok": True,
                "completed": False,
                "reason": "automation_excluded",
                "marker": _row_to_work_review_processor_marker(saved_row),
                "audit": {
                    "audit_id": audit_id,
                    "action": "complete_review_processor_marker",
                    "result": "automation_excluded_cancelled",
                },
            }
        if expected_source_hash and expected_source_hash != marker_row["document_source_hash"]:
            saved_row = _write_work_review_processor_marker(
                conn,
                _work_review_processor_marker_update_row(
                    marker_row,
                    updates={
                        "status": "queued",
                        "queued_at": now,
                        "last_seen_at": now,
                        "processing_started_at": "",
                        "processing_expires_at": "",
                        "next_retry_at": "",
                        "retry_after_seconds": 0,
                        "retry_attempt_count": 0,
                        "last_failure_event_id": "",
                        "last_failure_source_hash": "",
                        "last_error_class": "",
                        "retry_policy_version": "",
                        "last_error": "superseded_source_hash",
                        "superseded_at": now,
                        "superseded_by_source_hash": marker_row["document_source_hash"],
                    },
                    meta=meta,
                    now=now,
                    reason="superseded_source_hash_requeued",
                    metadata={
                        **dict(body.metadata or {}),
                        "holder_id": holder_id,
                        "lease_id": lease_row["lease_id"],
                        "expected_document_source_hash": expected_source_hash,
                        "current_document_source_hash": marker_row["document_source_hash"],
                        "last_outcome_at": now,
                        "last_outcome_status": "superseded_source_hash_requeued",
                    },
                ),
            )
            marker_blocker = _upsert_work_processor_marker_blocker(
                conn,
                saved_row,
                meta=meta,
                now=now,
            )
            audit_row = _write_work_audit(
                conn,
                audit_id=audit_id,
                actor=meta["actor"],
                source_surface=meta["source_surface"],
                action="complete_review_processor_marker",
                target_ref=f"kanban_review_processor_markers:{saved_row['marker_id']}",
                item_id=saved_row["item_id"],
                parent_item_id="",
                created_at=now,
                request_id=meta["request_id"],
                run_id=meta["run_id"],
                result="superseded_source_hash",
                source_hash=saved_row["source_hash"],
                metadata={
                    "holder_id": holder_id,
                    "lease_id": lease_row["lease_id"],
                    "expected_document_source_hash": expected_source_hash,
                    "current_document_source_hash": marker_row["document_source_hash"],
                    "requeued": True,
                },
            )
            gen = _kanban_table_sync_gen(conn, "kanban-review-marker-complete-superseded")
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                saved_row["marker_id"],
                dict(saved_row),
                gen,
            )
            if marker_blocker is not None:
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_blockers",
                    marker_blocker["blocker_row"]["blocker_id"],
                    dict(marker_blocker["blocker_row"]),
                    gen,
                )
                enqueue_for_all_peers(
                    conn,
                    "UPDATE",
                    "kanban_audit_log",
                    marker_blocker["audit_row"]["audit_id"],
                    marker_blocker["audit_row"],
                    gen,
                )
            enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
            return {
                "ok": True,
                "completed": False,
                "reason": "superseded_source_hash",
                "marker": _row_to_work_review_processor_marker(saved_row),
                "audit": {
                    "audit_id": audit_id,
                    "action": "complete_review_processor_marker",
                    "result": "superseded_source_hash",
                },
            }
        decision_id = _clean_short_text(
            body.decision_id or marker_row["decision_id"], "", limit=180
        )
        last_error = _body_excerpt(body.error, limit=4000)
        body_metadata = dict(body.metadata or {})
        failure_event_row: Any | None = None
        failure_event_payload: dict[str, Any] | None = None
        updates: dict[str, Any] = {
            "status": outcome_status,
            "processing_started_at": "",
            "processing_expires_at": "",
            "last_seen_at": now,
            "decision_id": decision_id,
            "last_error": last_error if outcome_status != "processed" else "",
        }
        completion_metadata = {
            **body_metadata,
            "holder_id": holder_id,
            "lease_id": lease_row["lease_id"],
            "decision_id": decision_id,
            "outcome_status": outcome_status,
            "last_outcome_at": now,
            "last_outcome_status": outcome_status,
        }
        if outcome_status == "failed":
            previous_failure_count = conn.execute(
                """
                SELECT COUNT(*) AS count
                FROM kanban_review_processor_failure_events
                WHERE marker_id=? AND source_hash=?
                """,
                (marker_row["marker_id"], marker_row["document_source_hash"]),
            ).fetchone()
            failure_attempt = (
                int(previous_failure_count["count"] if previous_failure_count else 0) + 1
            )
            retry_after_seconds = _work_review_retry_after_seconds(failure_attempt)
            next_retry_at = _work_review_retry_next_at(now, failure_attempt)
            marker_processor_kind = str(marker_row["processor_kind"] or "review")
            route = _work_automation_processor_profile_route(
                marker_processor_kind
                if marker_processor_kind in KANBAN_PROCESSOR_PROFILE_SPECS
                else "review"
            )
            provider_mode = _clean_short_text(
                str(
                    body_metadata.get("provider_mode")
                    or marker_row["provider_mode"]
                    or route["provider_mode"]
                ),
                route["provider_mode"],
                limit=80,
            )
            model_alias = _clean_short_text(
                str(body_metadata.get("model_alias") or route["model_alias"] or ""),
                "",
                limit=180,
            )
            error_class = _work_review_failure_error_class(last_error, body_metadata)
            failure_event_row = _work_review_failure_event_row(
                marker_row,
                meta=meta,
                now=now,
                error_class=error_class,
                error_message=last_error,
                provider_mode=provider_mode,
                model_alias=model_alias,
                attempt_number=failure_attempt,
                next_retry_at=next_retry_at,
                retry_after_seconds=retry_after_seconds,
                retryable=True,
                metadata={
                    **body_metadata,
                    "holder_id": holder_id,
                    "lease_id": lease_row["lease_id"],
                    "decision_id": decision_id,
                    "marker_attempt_count": int(marker_row["attempt_count"] or 0),
                    "retryable": True,
                },
            )
            updates.update(
                {
                    "next_retry_at": next_retry_at,
                    "retry_after_seconds": retry_after_seconds,
                    "retry_attempt_count": failure_attempt,
                    "last_failure_event_id": failure_event_row["failure_event_id"],
                    "last_failure_source_hash": marker_row["document_source_hash"],
                    "last_error_class": error_class,
                    "retry_policy_version": KANBAN_REVIEW_RETRY_POLICY_VERSION,
                }
            )
            if marker_row["processed_source_hash"] == marker_row["document_source_hash"]:
                updates.update(
                    {
                        "processed_document_updated_at": "",
                        "processed_source_hash": "",
                        "processed_at": "",
                    }
                )
            completion_metadata.update(
                {
                    "retryable": True,
                    "failure_event_id": failure_event_row["failure_event_id"],
                    "failure_attempt": failure_attempt,
                    "retry_after_seconds": retry_after_seconds,
                    "next_retry_at": next_retry_at,
                    "retry_policy_version": KANBAN_REVIEW_RETRY_POLICY_VERSION,
                    "last_error": last_error,
                    "last_error_class": error_class,
                }
            )
        else:
            updates.update(
                {
                    "processed_document_updated_at": marker_row["document_updated_at"],
                    "processed_source_hash": marker_row["document_source_hash"],
                    "processed_at": now,
                    "next_retry_at": "",
                    "retry_after_seconds": 0,
                    "retry_attempt_count": 0,
                    "last_failure_event_id": "",
                    "last_failure_source_hash": "",
                    "last_error_class": "",
                    "retry_policy_version": "",
                }
            )
            completion_metadata["last_processed_at"] = now
            if outcome_status == "processed":
                updates["last_successful_source_hash"] = marker_row["document_source_hash"]
        saved_row = _write_work_review_processor_marker(
            conn,
            _work_review_processor_marker_update_row(
                marker_row,
                updates=updates,
                meta=meta,
                now=now,
                reason=f"marker_{outcome_status}",
                metadata=completion_metadata,
            ),
        )
        if failure_event_row is not None:
            failure_event_row = _write_work_review_failure_event(conn, failure_event_row)
            failure_event_payload = _row_to_work_review_failure_event(failure_event_row)
        marker_blocker = _upsert_work_processor_marker_blocker(
            conn,
            saved_row,
            meta=meta,
            now=now,
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="complete_review_processor_marker",
            target_ref=f"kanban_review_processor_markers:{saved_row['marker_id']}",
            item_id=saved_row["item_id"],
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result=outcome_status,
            source_hash=saved_row["source_hash"],
            metadata={
                "holder_id": holder_id,
                "lease_id": lease_row["lease_id"],
                "decision_id": decision_id,
                "outcome_status": outcome_status,
                "failure_event_id": failure_event_row["failure_event_id"]
                if failure_event_row is not None
                else "",
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-review-marker-complete")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_review_processor_markers",
            saved_row["marker_id"],
            dict(saved_row),
            gen,
        )
        if failure_event_row is not None:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_failure_events",
                failure_event_row["failure_event_id"],
                dict(failure_event_row),
                gen,
            )
        if marker_blocker is not None:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        marker = _row_to_work_review_processor_marker(saved_row)
        result = {
            "ok": True,
            "completed": True,
            "marker": marker,
            "audit": {
                "audit_id": audit_id,
                "action": "complete_review_processor_marker",
                "result": outcome_status,
            },
        }
        if marker_blocker is not None:
            result["processor_blocker"] = _row_to_work_blocker(marker_blocker["blocker_row"])
        if failure_event_payload is not None:
            result["failure_event"] = failure_event_payload
        return result


@router.post("/kanban/automation/review-processor/requeue-timeouts")
async def requeue_timed_out_work_review_processor_markers(
    body: WorkReviewProcessorTimeoutRequeueRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(
        _requeue_timed_out_work_review_processor_markers_sync, body
    )


def _requeue_timed_out_work_review_processor_markers_sync(
    body: WorkReviewProcessorTimeoutRequeueRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    audit_id = f"audit-{uuid.uuid4().hex}"
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        scope_ids = _work_scope_item_ids(conn, clean_item_id) if clean_item_id else []
        args: list[Any] = []
        now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
        included_predicate = _work_item_automation_included_predicate("item")
        where = f"""
        WHERE marker.status='processing'
          AND marker.processing_expires_at != ''
          AND {included_predicate}
        """
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where += f" AND marker.item_id IN ({placeholders})"
            args.extend(scope_ids)
        candidate_rows = conn.execute(
            f"""
            SELECT marker.* FROM kanban_review_processor_markers marker
            JOIN kanban_items item ON item.item_id=marker.item_id
            {where}
            ORDER BY marker.processing_expires_at ASC, marker.marker_id
            """,
            args,
        ).fetchall()
        excluded_args = list(args)
        excluded_where = f"""
        WHERE marker.status='processing'
          AND marker.processing_expires_at != ''
          AND NOT ({included_predicate})
        """
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            excluded_where += f" AND marker.item_id IN ({placeholders})"
        excluded_candidate_rows = conn.execute(
            f"""
            SELECT marker.* FROM kanban_review_processor_markers marker
            JOIN kanban_items item ON item.item_id=marker.item_id
            {excluded_where}
            ORDER BY marker.processing_expires_at ASC, marker.marker_id
            """,
            excluded_args,
        ).fetchall()
        rows = [
            row
            for row in candidate_rows
            if (expires_at := _parse_utc_datetime(row["processing_expires_at"])) is not None
            and expires_at <= now_dt
        ]
        rows.sort(
            key=lambda row: (
                _parse_utc_datetime(row["processing_expires_at"]) or now_dt,
                row["marker_id"],
            )
        )
        requeued_rows = []
        cancelled_excluded_rows = []
        cancelled_marker_blockers: list[dict[str, Any]] = []
        excluded_rows = [
            row
            for row in excluded_candidate_rows
            if (expires_at := _parse_utc_datetime(row["processing_expires_at"])) is not None
            and expires_at <= now_dt
        ]
        excluded_rows.sort(
            key=lambda row: (
                _parse_utc_datetime(row["processing_expires_at"]) or now_dt,
                row["marker_id"],
            )
        )
        for row in excluded_rows:
            updated_row = _work_review_processor_marker_update_row(
                row,
                updates={
                    "status": "cancelled",
                    "last_seen_at": now,
                    "processing_started_at": "",
                    "processing_expires_at": "",
                    "last_error": "automation_excluded",
                    "processed_document_updated_at": row["document_updated_at"],
                    "processed_source_hash": row["document_source_hash"],
                    "processed_at": now,
                },
                meta=meta,
                now=now,
                reason="automation_excluded_timeout_cancelled",
                metadata={
                    **dict(body.metadata or {}),
                    "last_outcome_at": now,
                    "last_outcome_status": "automation_excluded_timeout_cancelled",
                    "last_error": "automation_excluded",
                },
            )
            saved_cancelled_row = _write_work_review_processor_marker(conn, updated_row)
            cancelled_excluded_rows.append(saved_cancelled_row)
            marker_blocker = _upsert_work_processor_marker_blocker(
                conn,
                saved_cancelled_row,
                meta=meta,
                now=now,
            )
            if marker_blocker is not None:
                cancelled_marker_blockers.append(marker_blocker)
        for row in rows:
            updated_row = _work_review_processor_marker_update_row(
                row,
                updates={
                    "status": "queued",
                    "queued_at": now,
                    "last_seen_at": now,
                    "processing_started_at": "",
                    "processing_expires_at": "",
                    "last_error": "processing_timeout",
                },
                meta=meta,
                now=now,
                reason="processing_timeout_requeued",
                metadata={
                    **dict(body.metadata or {}),
                    "last_outcome_at": now,
                    "last_outcome_status": "timeout_requeued",
                    "last_error": "processing_timeout",
                },
            )
            requeued_rows.append(_write_work_review_processor_marker(conn, updated_row))
        source_hash = _hash_json_payload(
            {
                "action": "requeue_review_processor_timeouts",
                "item_id": clean_item_id,
                "requeued_count": len(requeued_rows),
                "cancelled_excluded_count": len(cancelled_excluded_rows),
                "marker_ids": [row["marker_id"] for row in requeued_rows],
                "cancelled_excluded_marker_ids": [
                    row["marker_id"] for row in cancelled_excluded_rows
                ],
            }
        )
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="requeue_review_processor_timeouts",
            target_ref="kanban_review_processor_markers:timeouts",
            item_id=clean_item_id,
            parent_item_id="",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "schema": KANBAN_REVIEW_SCHEDULER_SCHEMA,
                "requeued_count": len(requeued_rows),
                "cancelled_excluded_count": len(cancelled_excluded_rows),
                "marker_ids": [row["marker_id"] for row in requeued_rows],
                "cancelled_excluded_marker_ids": [
                    row["marker_id"] for row in cancelled_excluded_rows
                ],
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-review-marker-timeout")
        for row in cancelled_excluded_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                row["marker_id"],
                dict(row),
                gen,
            )
        for row in requeued_rows:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_review_processor_markers",
                row["marker_id"],
                dict(row),
                gen,
            )
        for marker_blocker in cancelled_marker_blockers:
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_blockers",
                marker_blocker["blocker_row"]["blocker_id"],
                dict(marker_blocker["blocker_row"]),
                gen,
            )
            enqueue_for_all_peers(
                conn,
                "UPDATE",
                "kanban_audit_log",
                marker_blocker["audit_row"]["audit_id"],
                marker_blocker["audit_row"],
                gen,
            )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
        markers = [_row_to_work_review_processor_marker(row) for row in requeued_rows]
        return {
            "ok": True,
            "requeued_count": len(markers),
            "requeued_markers": markers,
            "cancelled_excluded_count": len(cancelled_excluded_rows),
            "cancelled_excluded_markers": [
                _row_to_work_review_processor_marker(row) for row in cancelled_excluded_rows
            ],
            "scheduler": _work_review_processor_marker_stats(conn, scope_ids),
            "audit": {
                "audit_id": audit_id,
                "action": "requeue_review_processor_timeouts",
                "result": "ok",
            },
        }


@router.get("/kanban/automation/review-processor/output-contract")
async def get_work_review_processor_output_contract() -> dict[str, Any]:
    return {
        "ok": True,
        "contract": _work_review_processor_output_contract(),
    }


@router.get("/kanban/automation/review-processor/processing-policy")
async def get_work_review_processor_processing_policy() -> dict[str, Any]:
    return {
        "ok": True,
        "policy": _work_review_processing_policy(),
    }


@router.get("/kanban/automation/idle-worker/contract")
async def get_work_automation_idle_worker_contract() -> dict[str, Any]:
    return {
        "ok": True,
        "contract": _work_automation_idle_worker_contract(),
    }


@router.get("/kanban/automation/review-processor/metadata-contract")
async def get_work_review_processor_metadata_contract() -> dict[str, Any]:
    return {
        "ok": True,
        "contract": _work_review_processing_metadata_contract(),
    }


@router.get("/kanban/automation/preprocessing/readiness-contract")
async def get_work_preprocessing_readiness_contract() -> dict[str, Any]:
    return {
        "ok": True,
        "contract": _work_preprocessing_readiness_contract(),
    }


@router.get("/kanban/automation/proposal-surfaces/contract")
async def get_work_proposal_surfaces_contract() -> dict[str, Any]:
    return {
        "ok": True,
        "contract": _work_proposal_surfaces_contract(),
    }


@router.get("/kanban/automation/review-processor/lease")
async def get_work_review_processor_lease() -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (_review_processor_lease_id(),),
        ).fetchone()
        return {
            "ok": True,
            "lease": _row_to_work_review_processor_lease(row),
        }


@router.post("/kanban/automation/review-processor/lease/acquire")
async def acquire_work_review_processor_lease(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_acquire_work_review_processor_lease_sync, body)


def _acquire_work_review_processor_lease_sync(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    meta = _work_request_meta(body)
    holder_id = _clean_short_text(body.holder_id, "", limit=160)
    if not holder_id:
        raise HTTPException(400, "Review Processor lease holder_id is required")
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    clean_session_id = _clean_work_agent_session_id(body.session_id) if body.session_id else ""
    ttl_seconds = _clean_review_lease_ttl(body.ttl_seconds)
    requested_token = _clean_short_text(body.lease_token or "", "", limit=220)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        item = _work_item_or_404(conn, clean_item_id) if clean_item_id else None
        existing = conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (_review_processor_lease_id(),),
        ).fetchone()
        existing_active = _review_lease_is_active(existing, now_dt)
        existing_owner = bool(
            existing is not None
            and existing_active
            and _review_lease_owner_matches(existing, holder_id, requested_token)
        )
        if existing_active and not existing_owner and not body.force:
            source_hash = _hash_json_payload(
                {
                    "action": "acquire_review_processor_lease",
                    "holder_id": holder_id,
                    "item_id": clean_item_id,
                    "blocked_by": existing["holder_id"],
                    "lease_id": existing["lease_id"],
                    "reason": "active_lease",
                    "request_id": meta["request_id"],
                }
            )
            audit_row = _write_work_review_processor_lease_audit(
                conn,
                action="acquire_review_processor_lease",
                result="blocked",
                meta=meta,
                now=now,
                item_id=clean_item_id,
                parent_item_id=(item["parent_item_id"] or "") if item else "",
                source_hash=source_hash,
                metadata={
                    "holder_id": holder_id,
                    "blocked_by": existing["holder_id"],
                    "reason": "active_lease",
                },
            )
            return {
                "ok": True,
                "acquired": False,
                "reason": "active_lease",
                "lease": _row_to_work_review_processor_lease(existing, now_dt=now_dt),
                "audit": {
                    "audit_id": audit_row["audit_id"],
                    "action": "acquire_review_processor_lease",
                    "result": "blocked",
                },
            }
        lease_token = requested_token
        if not lease_token:
            lease_token = (
                existing["lease_token"]
                if existing_owner and existing is not None and existing["lease_token"]
                else f"lease-{uuid.uuid4().hex}"
            )
        lease_item_id = clean_item_id or (
            existing["item_id"] if existing_owner and existing is not None else ""
        )
        lease_session_id = clean_session_id or (
            existing["session_id"] if existing_owner and existing is not None else ""
        )
        provenance = {
            "schema": KANBAN_REVIEW_LEASE_SCHEMA,
            "recorded_by": meta["actor"],
            "source_surface": meta["source_surface"],
            "request_id": meta["request_id"],
            "run_id": meta["run_id"],
        }
        lease_metadata = dict(body.metadata) if isinstance(body.metadata, dict) else {}
        if body.force:
            lease_metadata["force_acquire"] = True
            if existing is not None:
                lease_metadata["previous_holder_id"] = existing["holder_id"]
        row = _work_review_processor_lease_row(
            existing=existing,
            holder_id=holder_id,
            item_id=lease_item_id,
            session_id=lease_session_id,
            lease_token=lease_token,
            ttl_seconds=ttl_seconds,
            status="active",
            metadata=lease_metadata,
            provenance=provenance,
            now=now,
        )
        lease_row = _write_work_review_processor_lease(conn, row)
        audit_row = _write_work_review_processor_lease_audit(
            conn,
            action="acquire_review_processor_lease",
            result="ok",
            meta=meta,
            now=now,
            item_id=lease_item_id,
            parent_item_id=(item["parent_item_id"] or "") if item else "",
            source_hash=lease_row["source_hash"],
            metadata={
                "holder_id": holder_id,
                "item_id": lease_item_id,
                "session_id": lease_session_id,
                "ttl_seconds": ttl_seconds,
                "force": bool(body.force),
                "upsert": "updated" if existing is not None else "inserted",
            },
            lease_row=lease_row,
        )
        return {
            "ok": True,
            "acquired": True,
            "reason": "force_acquired"
            if body.force
            else ("refreshed" if existing_owner else "acquired"),
            "lease": _row_to_work_review_processor_lease(
                lease_row,
                now_dt=now_dt,
                include_token=True,
            ),
            "audit": {
                "audit_id": audit_row["audit_id"],
                "action": "acquire_review_processor_lease",
                "result": "ok",
            },
        }


@router.post("/kanban/automation/review-processor/lease/heartbeat")
async def heartbeat_work_review_processor_lease(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_heartbeat_work_review_processor_lease_sync, body)


def _heartbeat_work_review_processor_lease_sync(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    meta = _work_request_meta(body)
    holder_id = _clean_short_text(body.holder_id, "", limit=160)
    if not holder_id:
        raise HTTPException(400, "Review Processor lease holder_id is required")
    lease_token = _clean_short_text(body.lease_token or "", "", limit=220)
    ttl_seconds = _clean_review_lease_ttl(body.ttl_seconds)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (_review_processor_lease_id(),),
        ).fetchone()
        if existing is None:
            return {
                "ok": True,
                "heartbeated": False,
                "reason": "missing_lease",
                "lease": _row_to_work_review_processor_lease(None, now_dt=now_dt),
            }
        if not _review_lease_is_active(existing, now_dt):
            return {
                "ok": True,
                "heartbeated": False,
                "reason": "inactive_or_expired_lease",
                "lease": _row_to_work_review_processor_lease(existing, now_dt=now_dt),
            }
        if not _review_lease_owner_matches(existing, holder_id, lease_token):
            return {
                "ok": True,
                "heartbeated": False,
                "reason": "not_lease_owner",
                "lease": _row_to_work_review_processor_lease(existing, now_dt=now_dt),
            }
        metadata = _json_value(existing["metadata_json"], {})
        metadata.update(dict(body.metadata) if isinstance(body.metadata, dict) else {})
        row = _work_review_processor_lease_row(
            existing=existing,
            holder_id=existing["holder_id"],
            item_id=existing["item_id"],
            session_id=existing["session_id"],
            lease_token=existing["lease_token"],
            ttl_seconds=ttl_seconds,
            status="active",
            metadata=metadata,
            provenance=_json_value(existing["provenance_json"], {}),
            now=now,
        )
        lease_row = _write_work_review_processor_lease(conn, row)
        audit_row = _write_work_review_processor_lease_audit(
            conn,
            action="heartbeat_review_processor_lease",
            result="ok",
            meta=meta,
            now=now,
            item_id=lease_row["item_id"],
            parent_item_id="",
            source_hash=lease_row["source_hash"],
            metadata={
                "holder_id": holder_id,
                "item_id": lease_row["item_id"],
                "session_id": lease_row["session_id"],
                "ttl_seconds": ttl_seconds,
            },
            lease_row=lease_row,
        )
        return {
            "ok": True,
            "heartbeated": True,
            "reason": "heartbeat_recorded",
            "lease": _row_to_work_review_processor_lease(lease_row, now_dt=now_dt),
            "audit": {
                "audit_id": audit_row["audit_id"],
                "action": "heartbeat_review_processor_lease",
                "result": "ok",
            },
        }


@router.post("/kanban/automation/review-processor/lease/release")
async def release_work_review_processor_lease(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    return await _run_personal_sync_work(_release_work_review_processor_lease_sync, body)


def _release_work_review_processor_lease_sync(
    body: WorkReviewProcessorLeaseRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    now_dt = _parse_utc_datetime(now) or datetime.now(timezone.utc)
    meta = _work_request_meta(body)
    holder_id = _clean_short_text(body.holder_id, "", limit=160)
    if not holder_id:
        raise HTTPException(400, "Review Processor lease holder_id is required")
    lease_token = _clean_short_text(body.lease_token or "", "", limit=220)
    with get_conn() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (_review_processor_lease_id(),),
        ).fetchone()
        if existing is None:
            return {
                "ok": True,
                "released": False,
                "reason": "missing_lease",
                "lease": _row_to_work_review_processor_lease(None, now_dt=now_dt),
            }
        if existing["status"] != "active":
            return {
                "ok": True,
                "released": False,
                "reason": "no_active_lease",
                "lease": _row_to_work_review_processor_lease(existing, now_dt=now_dt),
            }
        if not _review_lease_owner_matches(existing, holder_id, lease_token):
            return {
                "ok": True,
                "released": False,
                "reason": "not_lease_owner",
                "lease": _row_to_work_review_processor_lease(existing, now_dt=now_dt),
            }
        metadata = _json_value(existing["metadata_json"], {})
        metadata.update(dict(body.metadata) if isinstance(body.metadata, dict) else {})
        row = _work_review_processor_lease_row(
            existing=existing,
            holder_id=existing["holder_id"],
            item_id=existing["item_id"],
            session_id=existing["session_id"],
            lease_token=existing["lease_token"],
            ttl_seconds=int(existing["timeout_seconds"] or 1200),
            status="released",
            metadata=metadata,
            provenance=_json_value(existing["provenance_json"], {}),
            now=now,
        )
        lease_row = _write_work_review_processor_lease(conn, row)
        audit_row = _write_work_review_processor_lease_audit(
            conn,
            action="release_review_processor_lease",
            result="ok",
            meta=meta,
            now=now,
            item_id=lease_row["item_id"],
            parent_item_id="",
            source_hash=lease_row["source_hash"],
            metadata={
                "holder_id": holder_id,
                "item_id": lease_row["item_id"],
                "session_id": lease_row["session_id"],
            },
            lease_row=lease_row,
        )
        return {
            "ok": True,
            "released": True,
            "reason": "released",
            "lease": _row_to_work_review_processor_lease(lease_row, now_dt=now_dt),
            "audit": {
                "audit_id": audit_row["audit_id"],
                "action": "release_review_processor_lease",
                "result": "ok",
            },
        }


@router.get("/kanban/automation/status")
async def get_work_automation_status(
    item_id: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    include_contracts: Annotated[bool, Query()] = True,
    include_auth_drift: Annotated[bool, Query()] = False,
    include_decision_metadata: Annotated[bool, Query()] = False,
    metrics: Annotated[bool, Query()] = False,
) -> dict[str, Any]:
    started = time.monotonic()
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    thread_started = 0.0
    thread_finished = 0.0

    def run_status_sync() -> dict[str, Any]:
        nonlocal thread_started, thread_finished
        thread_started = time.monotonic()
        try:
            return _get_work_automation_status_sync(
                clean_item_id,
                limit,
                include_contracts=include_contracts,
                include_auth_drift=include_auth_drift,
                include_decision_metadata=include_decision_metadata,
            )
        finally:
            thread_finished = time.monotonic()

    payload = await _run_personal_sync_work(run_status_sync)
    finished = time.monotonic()
    if metrics:
        sync_seconds = max(0.0, thread_finished - thread_started) if thread_started else 0.0
        payload["server_metrics"] = {
            "schema": "xarta.kanban.automation_status.metrics.v1",
            "total_seconds": round(finished - started, 6),
            "thread_queue_wait_seconds": (
                round(max(0.0, thread_started - started), 6) if thread_started else 0.0
            ),
            "status_thread_seconds": round(sync_seconds, 6),
            "response_assembly_wait_seconds": (
                round(max(0.0, finished - thread_finished), 6) if thread_finished else 0.0
            ),
            "limit": limit,
            "item_id_present": bool(clean_item_id),
            "include_contracts": bool(include_contracts),
            "include_auth_drift": bool(include_auth_drift),
            "include_decision_metadata": bool(include_decision_metadata),
        }
    return payload


def _get_work_automation_status_sync(
    clean_item_id: str,
    limit: int,
    *,
    include_contracts: bool = False,
    include_auth_drift: bool = False,
    include_decision_metadata: bool = False,
) -> dict[str, Any]:
    generated_at = _utc_now_iso()
    now_dt = _parse_utc_datetime(generated_at) or datetime.now(timezone.utc)
    raw_processing_policy = _work_review_processing_policy()
    processing_policy = (
        raw_processing_policy
        if include_contracts
        else _compact_work_processing_policy(
            raw_processing_policy,
            include_auth_drift=include_auth_drift,
        )
    )
    idle_worker_config = _work_automation_idle_worker_config()
    with _kanban_automation_scan_conn(operation="automation_status") as conn:
        scope_ids: list[str] = []
        item_payload: dict[str, Any] | None = None
        if clean_item_id:
            item_payload = _row_to_work_item(_work_item_or_404(conn, clean_item_id))
            scope_ids = _work_scope_item_ids(conn, clean_item_id)
        where = ""
        args: list[Any] = []
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where = f"WHERE item_id IN ({placeholders})"
            args.extend(scope_ids)
        rows = conn.execute(
            f"""
            SELECT * FROM kanban_review_decisions
            {where}
            ORDER BY updated_at DESC, created_at DESC, decision_id
            LIMIT ?
            """,
            [*args, limit],
        ).fetchall()
        recent_decisions = []
        for row in rows:
            commit_ids = _json_value(row["commit_link_ids_json"], [])
            commit_rows = _work_decision_commit_rows(conn, commit_ids)
            recent_decisions.append(
                _compact_work_review_decision(
                    _row_to_work_review_decision(
                        row,
                        [_row_to_work_commit(commit) for commit in commit_rows],
                    ),
                    include_metadata=include_decision_metadata,
                )
            )
        status_rows = conn.execute(
            f"SELECT status, COUNT(*) AS count FROM kanban_review_decisions {where} GROUP BY status",
            args,
        ).fetchall()
        provider_rows = conn.execute(
            f"SELECT provider_mode, COUNT(*) AS count FROM kanban_review_decisions {where} GROUP BY provider_mode",
            args,
        ).fetchall()
        active_session = conn.execute(
            """
            SELECT * FROM kanban_agent_sessions
            WHERE status='active'
            ORDER BY updated_at DESC, started_at DESC, session_id
            LIMIT 1
            """
        ).fetchone()
        if scope_ids:
            scoped_active_session = conn.execute(
                f"""
                SELECT * FROM kanban_agent_sessions
                WHERE status='active' AND item_id IN ({",".join("?" for _ in scope_ids)})
                ORDER BY updated_at DESC, started_at DESC, session_id
                LIMIT 1
                """,
                scope_ids,
            ).fetchone()
            active_session = scoped_active_session or active_session
        lease_row = conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (_review_processor_lease_id(),),
        ).fetchone()
        lease = _row_to_work_review_processor_lease(lease_row, now_dt=now_dt)
        lease_active = bool(lease["active"])
        marker_stats = _compact_work_marker_stats(
            _work_review_processor_marker_stats(conn, scope_ids, limit=limit)
        )
        preprocessing_marker_stats = _compact_work_marker_stats(
            _work_review_processor_marker_stats(
                conn,
                scope_ids,
                limit=limit,
                processor_kind="preprocessing",
            )
        )
        failure_stats = _compact_work_failure_stats(
            _work_review_processor_failure_stats(conn, scope_ids, limit=limit)
        )
        exclusion_where = (
            "WHERE item.status != 'archived' AND COALESCE(item.automation_excluded, 0) != 0"
        )
        exclusion_args: list[Any] = []
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            exclusion_where += f" AND item.item_id IN ({placeholders})"
            exclusion_args.extend(scope_ids)
        exclusion_count_row = conn.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM kanban_items item
            {exclusion_where}
            """,
            exclusion_args,
        ).fetchone()
        exclusion_rows = conn.execute(
            f"""
            SELECT item.* FROM kanban_items item
            {exclusion_where}
            ORDER BY item.updated_at DESC, item.item_id
            LIMIT ?
            """,
            [*exclusion_args, limit],
        ).fetchall()
        preprocessing_active_row = conn.execute(
            f"""
            SELECT marker.* FROM kanban_review_processor_markers marker
            JOIN kanban_items item ON item.item_id=marker.item_id
            WHERE marker.processor_kind='preprocessing'
              AND marker.status='processing'
              AND {_work_item_automation_included_predicate("item")}
              {"AND marker.item_id IN (" + ",".join("?" for _ in scope_ids) + ")" if scope_ids else ""}
            ORDER BY marker.processing_started_at ASC, marker.queued_at ASC, marker.marker_id
            LIMIT 1
            """,
            scope_ids if scope_ids else [],
        ).fetchone()
        last_completed = conn.execute(
            f"""
            SELECT * FROM kanban_review_decisions
            {where}
            ORDER BY updated_at DESC, created_at DESC, decision_id
            LIMIT 1
            """,
            args,
        ).fetchone()
        all_decision_rows = conn.execute(
            f"""
            SELECT decision_id, status, commit_link_ids_json, metadata_json
            FROM kanban_review_decisions
            {where}
            """,
            args,
        ).fetchall()
        metadata_contract = _work_review_processing_metadata_contract(raw_processing_policy)
        output_contract = _work_review_processor_output_contract(
            raw_processing_policy,
            metadata_contract,
        )
        preprocessing_contract = _work_preprocessing_readiness_contract(raw_processing_policy)
        proposal_surfaces = _work_proposal_surfaces_contract()
        idle_worker_contract = _work_automation_idle_worker_contract()
        return {
            "ok": True,
            "schema": "xarta.kanban.automation_status.v1",
            "item": item_payload,
            "generated_at": generated_at,
            "output_contract": (
                output_contract
                if include_contracts
                else _compact_work_output_contract(output_contract)
            ),
            "metadata_contract": (
                metadata_contract
                if include_contracts
                else _compact_work_contract_header(metadata_contract)
            ),
            "preprocessing_contract": (
                preprocessing_contract
                if include_contracts
                else _compact_work_contract_header(preprocessing_contract)
            ),
            "processing_policy": processing_policy,
            "proposal_surfaces": (
                proposal_surfaces
                if include_contracts
                else _compact_work_proposal_surfaces(proposal_surfaces)
            ),
            "idle_worker_contract": (
                idle_worker_contract
                if include_contracts
                else _compact_work_contract_header(idle_worker_contract)
            ),
            "idle_worker": idle_worker_config,
            "automation_exclusions": {
                "schema": KANBAN_AUTOMATION_EXCLUSION_SCHEMA,
                "count": int(exclusion_count_row["count"] if exclusion_count_row else 0),
                "recent_items": [_row_to_work_item(row) for row in exclusion_rows],
            },
            "failures": failure_stats,
            "provider_mode": {
                "active": processing_policy["active_mode"],
                "planned": processing_policy["profile_processing"]["state"],
                "profile_processing": processing_policy["profile_processing"],
                "auth_drift": (
                    processing_policy["profile_processing"].get("auth_drift", {})
                    if include_auth_drift or include_contracts
                    else {}
                ),
                "local_processing_gate": processing_policy["local_processing"]["gate"],
                "automatic_switch": processing_policy["profile_processing"]["automatic_switch"],
                "by_mode": {row["provider_mode"]: int(row["count"]) for row in provider_rows},
            },
            "review_processor": {
                "status": "lease-active" if lease_active else "decision-ledger-ready",
                "queue_length": marker_stats["queue_length"],
                "active_item_id": (
                    lease["item_id"]
                    if lease_active
                    else (active_session["item_id"] if active_session else "")
                ),
                "lease_owner": (
                    lease["holder_id"]
                    if lease_active
                    else (active_session["agent_id"] if active_session else "")
                ),
                "lease_updated_at": (
                    lease["heartbeat_at"] or lease["updated_at"]
                    if lease_active
                    else (active_session["updated_at"] if active_session else "")
                ),
                "lease_expires_at": lease["expires_at"] if lease_active else "",
                "timeout_seconds": int(lease["timeout_seconds"] or 20 * 60),
                "lease": lease,
                "last_completed_item_id": last_completed["item_id"] if last_completed else "",
                "last_completed_at": last_completed["updated_at"] if last_completed else "",
                "pending_decision_count": sum(
                    int(row["count"]) for row in status_rows if row["status"] == "pending"
                ),
                "scheduler": marker_stats,
                "review_markers": marker_stats["recent_markers"],
                "failure_events": marker_stats["failure_events"],
                "failure_aggregates": marker_stats["failure_aggregates"],
                "repeated_failure_count": marker_stats["repeated_failure_count"],
                "retry_waiting_count": marker_stats["retry_waiting_count"],
                "retry_due_count": marker_stats["retry_due_count"],
                "last_error": marker_stats["last_error"],
                "next_retry_at": marker_stats["next_retry_at"],
            },
            "preprocessing": {
                "status": (
                    "processing-active"
                    if preprocessing_active_row
                    else (
                        "queued"
                        if preprocessing_marker_stats["queue_length"] > 0
                        else "readiness-contract-ready"
                    )
                ),
                "queue_length": preprocessing_marker_stats["queue_length"],
                "active_item_id": (
                    preprocessing_active_row["item_id"] if preprocessing_active_row else ""
                ),
                "last_completed_item_id": "",
                "readiness_contract": (
                    preprocessing_contract
                    if include_contracts
                    else _compact_work_contract_header(preprocessing_contract)
                ),
                "scheduler": preprocessing_marker_stats,
                "markers": preprocessing_marker_stats["recent_markers"],
                "failure_events": preprocessing_marker_stats["failure_events"],
                "failure_aggregates": preprocessing_marker_stats["failure_aggregates"],
                "repeated_failure_count": preprocessing_marker_stats["repeated_failure_count"],
                "retry_waiting_count": preprocessing_marker_stats["retry_waiting_count"],
                "retry_due_count": preprocessing_marker_stats["retry_due_count"],
                "last_error": preprocessing_marker_stats["last_error"],
                "next_retry_at": preprocessing_marker_stats["next_retry_at"],
            },
            "decisions": {
                "count": len(all_decision_rows),
                "by_status": {row["status"]: int(row["count"]) for row in status_rows},
                "recent": recent_decisions,
            },
            "commit_link_health": _work_decision_commit_health(all_decision_rows, conn),
        }


@router.post("/kanban/automation/failures/prune")
async def prune_work_automation_failure_events(
    body: WorkAutomationFailurePruneRequest,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(body.item_id, "", limit=180)
    clean_marker_id = _clean_short_text(body.marker_id, "", limit=180)
    clean_processor_kind = _clean_short_text(body.processor_kind or "", "", limit=80)
    clean_error_class = _clean_short_text(body.error_class or "", "", limit=120)
    before_failed_at = _clean_short_text(body.before_failed_at or "", "", limit=80)
    try:
        limit = max(1, min(int(body.limit or 500), 5000))
    except (TypeError, ValueError):
        limit = 500
    with get_conn() as conn:
        scope_ids: list[str] = []
        item_row = None
        if clean_item_id:
            item_row = _work_item_or_404(conn, clean_item_id)
            scope_ids = _work_scope_item_ids(conn, clean_item_id)
        where = "WHERE 1=1"
        args: list[Any] = []
        if scope_ids:
            placeholders = ",".join("?" for _ in scope_ids)
            where += f" AND event.item_id IN ({placeholders})"
            args.extend(scope_ids)
        if clean_marker_id:
            where += " AND event.marker_id=?"
            args.append(clean_marker_id)
        if clean_processor_kind:
            where += " AND event.processor_kind=?"
            args.append(clean_processor_kind)
        if clean_error_class:
            where += " AND event.error_class=?"
            args.append(clean_error_class)
        if before_failed_at:
            where += " AND event.failed_at < ?"
            args.append(before_failed_at)
        rows = conn.execute(
            f"""
            SELECT event.*,
                   item.title AS item_title,
                   marker.status AS marker_status,
                   marker.next_retry_at AS marker_next_retry_at,
                   marker.retry_attempt_count AS marker_retry_attempt_count,
                   marker.last_error AS marker_last_error
            FROM kanban_review_processor_failure_events event
            LEFT JOIN kanban_items item ON item.item_id=event.item_id
            LEFT JOIN kanban_review_processor_markers marker ON marker.marker_id=event.marker_id
            {where}
            ORDER BY event.failed_at DESC, event.attempt_number DESC, event.failure_event_id DESC
            LIMIT ?
            """,
            [*args, limit],
        ).fetchall()
        candidates: list[Any] = []
        skipped_active: list[str] = []
        for row in rows:
            marker_status = _row_value(row, "marker_status", "")
            marker_retry_state = _work_review_marker_retry_state(
                {
                    "status": marker_status,
                    "next_retry_at": _row_value(row, "marker_next_retry_at", ""),
                }
            )
            active_retry = marker_status == "failed" and marker_retry_state in {
                "retry_waiting",
                "retry_due",
            }
            if active_retry and not body.include_active:
                skipped_active.append(row["failure_event_id"])
                continue
            candidates.append(row)
        event_payloads = [_work_review_failure_event_payload(row) for row in candidates[:50]]
        pruned_ids = [row["failure_event_id"] for row in candidates]
        source_hash = _hash_json_payload(
            {
                "item_id": clean_item_id,
                "marker_id": clean_marker_id,
                "processor_kind": clean_processor_kind,
                "error_class": clean_error_class,
                "before_failed_at": before_failed_at,
                "include_active": bool(body.include_active),
                "limit": limit,
                "matched_ids": pruned_ids,
            }
        )
        audit_metadata = {
            "schema": "xarta.kanban.automation.failure_prune.v1",
            "apply": bool(body.apply),
            "item_id": clean_item_id,
            "marker_id": clean_marker_id,
            "processor_kind": clean_processor_kind,
            "error_class": clean_error_class,
            "before_failed_at": before_failed_at,
            "include_active": bool(body.include_active),
            "limit": limit,
            "matched_count": len(candidates),
            "skipped_active_count": len(skipped_active),
            "pruned_event_ids": pruned_ids[:100],
            "skipped_active_event_ids": skipped_active[:100],
        }
        deleted_count = 0
        if body.apply and candidates:
            store = _kanban_write_store(conn)
            gen = _kanban_table_sync_gen(conn, "kanban-review-failure-prune")
            for row in candidates:
                store.delete_review_failure_event_row(row["failure_event_id"])
                enqueue_for_all_peers(
                    conn,
                    "DELETE",
                    "kanban_review_processor_failure_events",
                    row["failure_event_id"],
                    {},
                    gen,
                )
                deleted_count += 1
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="prune_automation_failure_events",
            target_ref="kanban_review_processor_failure_events",
            item_id=clean_item_id,
            parent_item_id=(item_row["parent_item_id"] or "") if item_row is not None else "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={**audit_metadata, "deleted_count": deleted_count},
        )
        gen = _kanban_table_sync_gen(conn, "kanban-audit")
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "schema": "xarta.kanban.automation.failure_prune.v1",
        "apply": bool(body.apply),
        "matched_count": len(candidates),
        "deleted_count": deleted_count,
        "skipped_active_count": len(skipped_active),
        "pruned_event_ids": pruned_ids,
        "skipped_active_event_ids": skipped_active,
        "events": event_payloads,
        "filters": {
            "item_id": clean_item_id,
            "marker_id": clean_marker_id,
            "processor_kind": clean_processor_kind,
            "error_class": clean_error_class,
            "before_failed_at": before_failed_at,
            "include_active": bool(body.include_active),
            "limit": limit,
        },
        "audit": {
            "audit_id": audit_id,
            "action": "prune_automation_failure_events",
            "result": "ok",
        },
    }


@router.get("/kanban/items/{item_id}/agent-hints")
async def get_work_item_agent_hints(item_id: str) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    with get_conn() as conn:
        item = _work_item_or_404(conn, clean_item_id)
        row = conn.execute(
            "SELECT * FROM kanban_agent_hints WHERE item_id=?",
            (clean_item_id,),
        ).fetchone()
        return {
            "ok": True,
            "item": _row_to_work_item(item),
            "agent_hints": _row_to_work_agent_hints(row, clean_item_id),
        }


@router.put("/kanban/items/{item_id}/agent-hints")
async def update_work_item_agent_hints(
    item_id: str, body: WorkAgentHintsUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    hint_id = _kanban_agent_hints_id(clean_item_id)
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, clean_item_id)
        existing = store.agent_hints_row_for_item(clean_item_id)
        required_skills = (
            _clean_work_agent_skills(body.required_skills)
            if body.required_skills is not None
            else (_json_value(existing["required_skills_json"], []) if existing is not None else [])
        )
        routing_notes = (
            _body_excerpt(body.routing_notes, limit=6000)
            if body.routing_notes is not None
            else (existing["routing_notes"] if existing is not None else "")
        )
        commit_attribution = (
            dict(body.commit_attribution)
            if isinstance(body.commit_attribution, dict)
            else (
                _json_value(existing["commit_attribution_json"], {}) if existing is not None else {}
            )
        )
        metadata = (
            dict(body.metadata)
            if isinstance(body.metadata, dict)
            else (_json_value(existing["metadata_json"], {}) if existing is not None else {})
        )
        status = _clean_work_agent_hint_status(
            body.status,
            default=existing["status"] if existing is not None else "active",
        )
        row = {
            "hint_id": existing["hint_id"] if existing is not None else hint_id,
            "item_id": clean_item_id,
            "required_skills_json": json.dumps(required_skills, ensure_ascii=True),
            "routing_notes": routing_notes,
            "commit_attribution_json": json.dumps(
                commit_attribution,
                ensure_ascii=True,
                sort_keys=True,
            ),
            "visibility": "agent",
            "status": status,
            "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
            "provenance_json": json.dumps(
                {
                    "schema": "xarta.kanban.agent_hints.provenance.v1",
                    "visibility": "agent",
                    "recorded_by": meta["actor"],
                    "source_surface": meta["source_surface"],
                    "request_id": meta["request_id"],
                    "run_id": meta["run_id"],
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "created_at": existing["created_at"] if existing is not None else now,
            "updated_at": now,
        }
        source_hash = _hash_json_payload(row)
        hint_row = store.upsert_agent_hints_row(row)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="update_work_agent_hints",
            target_ref=f"kanban_agent_hints:{hint_row['hint_id']}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "required_skill_count": len(required_skills),
                "has_routing_notes": bool(routing_notes),
                "has_commit_attribution": bool(commit_attribution),
                "upsert": "updated" if existing is not None else "inserted",
                "visibility": "agent",
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-agent-hints")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_agent_hints",
            hint_row["hint_id"],
            dict(hint_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "agent_hints": _row_to_work_agent_hints(hint_row, clean_item_id),
        "audit": {"audit_id": audit_id, "action": "update_work_agent_hints", "result": "ok"},
    }


@router.get("/kanban/items/{item_id}/agent-sessions")
async def list_work_item_agent_sessions(item_id: str, limit: int = 12) -> dict[str, Any]:
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    clean_limit = max(1, min(int(limit or 12), 50))
    with get_conn() as conn:
        with _kanban_read_connection(conn) as read_conn:
            item = _work_item_or_404(read_conn, clean_item_id)
            rows = read_conn.execute(
                """
                SELECT * FROM kanban_agent_sessions
                WHERE item_id=?
                ORDER BY COALESCE(NULLIF(last_seen_at, ''), updated_at) DESC,
                         session_id
                LIMIT ?
                """,
                (clean_item_id, clean_limit),
            ).fetchall()
        return {
            "ok": True,
            "item": _row_to_work_item(item),
            "count": len(rows),
            "agent_sessions": [_row_to_work_agent_session(row) for row in rows],
        }


@router.post("/kanban/items/{item_id}/agent-sessions")
async def create_work_item_agent_session(
    item_id: str, body: WorkAgentSessionCreateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_item_id = _clean_short_text(item_id, "", limit=180)
    session_id = _clean_work_agent_session_id(body.session_id)
    started_at = _clean_short_text(body.started_at or now, now, limit=80)
    last_seen_at = _clean_short_text(body.last_seen_at or started_at, started_at, limit=80)
    status = _clean_work_agent_session_status(body.status)
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, clean_item_id)
        row = {
            "session_id": session_id,
            "item_id": clean_item_id,
            "agent_id": _clean_short_text(body.agent_id, "", limit=160),
            "node_id": _clean_short_text(body.node_id or "", "", limit=120),
            "worktree_path": _clean_short_text(body.worktree_path or "", "", limit=500),
            "repo_full_name": _clean_short_text(body.repo_full_name or "", "", limit=240),
            "branch": _clean_short_text(body.branch or "", "", limit=160),
            "status": status,
            "started_at": started_at,
            "ended_at": _clean_short_text(body.ended_at or "", "", limit=80),
            "last_seen_at": last_seen_at,
            "request_hash": _clean_short_text(body.request_hash or "", "", limit=160),
            "source_surface": meta["source_surface"],
            "summary": _body_excerpt(body.summary or "", limit=2000),
            "metadata_json": json.dumps(
                dict(body.metadata) if isinstance(body.metadata, dict) else {},
                ensure_ascii=True,
                sort_keys=True,
            ),
            "provenance_json": json.dumps(
                {
                    "schema": "xarta.kanban.agent_session.provenance.v1",
                    "recorded_by": meta["actor"],
                    "source_surface": meta["source_surface"],
                    "request_id": meta["request_id"],
                    "run_id": meta["run_id"],
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "created_at": now,
            "updated_at": now,
        }
        if not row["agent_id"]:
            raise HTTPException(400, "Kanban agent session agent_id is required")
        source_hash = _hash_json_payload(row)
        existing = store.agent_session_row(session_id)
        if existing is not None and existing["item_id"] != clean_item_id:
            raise HTTPException(409, "Kanban agent session belongs to another item")
        if existing is not None:
            row["created_at"] = existing["created_at"]
        session_row = store.upsert_agent_session_row(row)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="record_work_agent_session",
            target_ref=f"kanban_agent_sessions:{session_id}",
            item_id=clean_item_id,
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "agent_id": row["agent_id"],
                "node_id": row["node_id"],
                "repo_full_name": row["repo_full_name"],
                "branch": row["branch"],
                "status": row["status"],
                "upsert": "updated" if existing is not None else "inserted",
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-agent-session")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_agent_sessions",
            session_id,
            dict(session_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "agent_session": _row_to_work_agent_session(session_row),
        "audit": {"audit_id": audit_id, "action": "record_work_agent_session", "result": "ok"},
    }


@router.patch("/kanban/agent-sessions/{session_id}")
async def update_work_agent_session(
    session_id: str, body: WorkAgentSessionUpdateRequest
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    meta = _work_request_meta(body)
    clean_session_id = _clean_work_agent_session_id(session_id)
    with get_conn() as conn:
        store = _kanban_write_store(conn)
        existing = store.agent_session_row(clean_session_id)
        if existing is None:
            raise HTTPException(404, "Kanban agent session not found")
        item = _work_item_or_404(conn, existing["item_id"])
        status = _clean_work_agent_session_status(
            body.status,
            default=existing["status"],
        )
        metadata = (
            dict(body.metadata)
            if isinstance(body.metadata, dict)
            else _json_value(existing["metadata_json"], {})
        )
        row = {
            "session_id": clean_session_id,
            "agent_id": (
                _clean_short_text(body.agent_id, "", limit=160)
                if body.agent_id is not None
                else existing["agent_id"]
            ),
            "node_id": (
                _clean_short_text(body.node_id, "", limit=120)
                if body.node_id is not None
                else existing["node_id"]
            ),
            "worktree_path": (
                _clean_short_text(body.worktree_path, "", limit=500)
                if body.worktree_path is not None
                else existing["worktree_path"]
            ),
            "repo_full_name": (
                _clean_short_text(body.repo_full_name, "", limit=240)
                if body.repo_full_name is not None
                else existing["repo_full_name"]
            ),
            "branch": (
                _clean_short_text(body.branch, "", limit=160)
                if body.branch is not None
                else existing["branch"]
            ),
            "status": status,
            "started_at": (
                _clean_short_text(body.started_at, "", limit=80)
                if body.started_at is not None
                else existing["started_at"]
            ),
            "ended_at": (
                _clean_short_text(body.ended_at, "", limit=80)
                if body.ended_at is not None
                else (
                    now if status == "done" and not existing["ended_at"] else existing["ended_at"]
                )
            ),
            "last_seen_at": (
                _clean_short_text(body.last_seen_at, "", limit=80)
                if body.last_seen_at is not None
                else now
            ),
            "request_hash": (
                _clean_short_text(body.request_hash, "", limit=160)
                if body.request_hash is not None
                else existing["request_hash"]
            ),
            "source_surface": meta["source_surface"],
            "summary": (
                _body_excerpt(body.summary, limit=2000)
                if body.summary is not None
                else existing["summary"]
            ),
            "metadata_json": json.dumps(metadata, ensure_ascii=True, sort_keys=True),
            "provenance_json": json.dumps(
                {
                    "schema": "xarta.kanban.agent_session.provenance.v1",
                    "recorded_by": meta["actor"],
                    "source_surface": meta["source_surface"],
                    "request_id": meta["request_id"],
                    "run_id": meta["run_id"],
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "updated_at": now,
        }
        if not row["agent_id"]:
            raise HTTPException(400, "Kanban agent session agent_id is required")
        source_hash = _hash_json_payload(row)
        session_row = store.update_agent_session_row(row)
        audit_row = _write_work_audit(
            conn,
            audit_id=audit_id,
            actor=meta["actor"],
            source_surface=meta["source_surface"],
            action="update_work_agent_session",
            target_ref=f"kanban_agent_sessions:{clean_session_id}",
            item_id=existing["item_id"],
            parent_item_id=item["parent_item_id"] or "",
            created_at=now,
            request_id=meta["request_id"],
            run_id=meta["run_id"],
            result="ok",
            source_hash=source_hash,
            metadata={
                "agent_id": row["agent_id"],
                "node_id": row["node_id"],
                "repo_full_name": row["repo_full_name"],
                "branch": row["branch"],
                "status": row["status"],
            },
        )
        gen = _kanban_table_sync_gen(conn, "kanban-agent-session")
        enqueue_for_all_peers(
            conn,
            "UPDATE",
            "kanban_agent_sessions",
            clean_session_id,
            dict(session_row),
            gen,
        )
        enqueue_for_all_peers(conn, "UPDATE", "kanban_audit_log", audit_id, audit_row, gen)
    return {
        "ok": True,
        "agent_session": _row_to_work_agent_session(session_row),
        "audit": {"audit_id": audit_id, "action": "update_work_agent_session", "result": "ok"},
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
        gen = _kanban_table_sync_gen(conn, "kanban-issue")
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
        gen = _kanban_table_sync_gen(conn, "kanban-todo")
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
        store = _kanban_write_store(conn)
        item = _work_item_or_404(conn, body.item_id)
        requested_blocker_id = _clean_short_text(blocker_id or body.blocker_id or "", "", limit=180)
        title = _clean_short_text(body.title, "", limit=180)
        if not title:
            raise HTTPException(400, "Kanban blocker title is required")
        body_excerpt = _body_excerpt(body.body or "", limit=4000)
        blocked_by_ref = _normalize_kanban_graph_ref(body.blocked_by_ref) or _clean_short_text(
            body.blocked_by_ref, "", limit=220
        )
        blocker_status = _clean_work_leaf_status(body.status)
        previous = store.blocker_row(requested_blocker_id) if blocker_id else None
        if previous is not None:
            clean_blocker_id = requested_blocker_id
        else:
            previous = store.blocker_row(clean_blocker_id)
        if (
            previous is not None
            and _work_request_requires_blocked_leaf_guard(meta)
            and blocker_status in {"resolved", "closed", "done"}
            and str(previous["status"] or "") not in {"resolved", "closed", "done"}
            and item["state_id"] == "blocked"
            and not _work_item_has_non_archived_children(conn, body.item_id)
            and _work_open_blocker_count(conn, body.item_id) <= 1
        ):
            raise HTTPException(
                409,
                {
                    "error": "kanban_automation_cannot_resolve_last_blocker_on_blocked_leaf",
                    "message": (
                        "Automation may not resolve the last open blocker while "
                        "leaving a leaf card in the blocked lane."
                    ),
                    "item_id": body.item_id,
                    "blocker_id": clean_blocker_id,
                    "actor": meta["actor"],
                    "source_surface": meta["source_surface"],
                },
            )
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
                "status": blocker_status,
                "blocked_by_ref": blocked_by_ref,
            }
        )
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        blocker_row = store.upsert_blocker_row(
            {
                "blocker_id": clean_blocker_id,
                "item_id": body.item_id,
                "title": title,
                "body_excerpt": body_excerpt,
                "status": blocker_status,
                "blocked_by_ref": blocked_by_ref,
                "search_text": search_text,
                "search_metadata": search_metadata,
                "vector_index_key": vector_key,
                "provenance": provenance,
                "created_at": created_at,
                "updated_at": now,
            }
        )
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
        gen = _kanban_table_sync_gen(conn, "kanban-blocker")
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
    _recompute_work_child_depths(conn, existing["item_id"], new_depth, now=now)
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
        gen = _kanban_table_sync_gen(conn, "kanban-promote")
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


def _row_to_filter_meta_tag(row: Any) -> dict[str, Any]:
    return {
        "meta_tag_id": row["meta_tag_id"],
        "id": row["meta_tag_id"],
        "label": row["label"],
        "color": row["color"],
        "priority": int(row["priority"] or 0),
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _row_to_filter_tag(
    row: Any, usage_count: int = 0, *, source: str = "registry"
) -> dict[str, Any]:
    return {
        "tag_id": row["tag_id"],
        "id": row["tag_id"],
        "label": row["label"],
        "color": row["color"],
        "shape": row["shape"],
        "fill": row["fill"],
        "meta_tag_id": row["meta_tag_id"] or "",
        "group": row["meta_tag_id"] or "",
        "builtin": bool(row["builtin"]),
        "custom": not bool(row["builtin"]),
        "source": source,
        "usage_count": usage_count,
        "provenance": _json_value(row["provenance_json"], {}),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _tag_values_from_row(row: Any) -> list[str]:
    return [
        _normalise_filter_id(tag)
        for tag in _json_value(row["tags_json"], [])
        if _normalise_filter_id(tag)
    ]


def _personal_filter_usage_counts(conn: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table_name in ("personal_events", "personal_time_tasks", "kanban_items"):
        try:
            rows = conn.execute(f"SELECT tags_json FROM {table_name}").fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            for tag_id in set(_tag_values_from_row(row)):
                counts[tag_id] = counts.get(tag_id, 0) + 1
    return counts


def _personal_filter_registry_payload(conn: Any) -> dict[str, Any]:
    usage_counts = _personal_filter_usage_counts(conn)
    meta_rows = conn.execute(
        "SELECT * FROM personal_filter_meta_tags ORDER BY priority DESC, label, meta_tag_id"
    ).fetchall()
    tag_rows = conn.execute("SELECT * FROM personal_filter_tags ORDER BY label, tag_id").fetchall()
    now = _utc_now_iso()
    meta_tag_ids = {row["meta_tag_id"] for row in meta_rows}
    orphan_meta_tag_ids = sorted(
        {
            row["meta_tag_id"]
            for row in tag_rows
            if row["meta_tag_id"] and row["meta_tag_id"] not in meta_tag_ids
        }
    )
    tags = {
        row["tag_id"]: _row_to_filter_tag(
            row,
            usage_counts.get(row["tag_id"], 0),
            source="registry",
        )
        for row in tag_rows
    }
    discovered_tag_ids: list[str] = []
    for tag_id, count in usage_counts.items():
        if tag_id in tags:
            continue
        discovered_tag_ids.append(tag_id)
        tags[tag_id] = {
            "tag_id": tag_id,
            "id": tag_id,
            "label": _filter_title(tag_id) or tag_id,
            "color": "blue",
            "shape": "circle",
            "fill": "outline",
            "meta_tag_id": "",
            "group": "",
            "builtin": False,
            "custom": False,
            "source": "discovered",
            "usage_count": count,
            "provenance": {
                "schema": "xarta.personal.filter_tag.discovered.v1",
                "note": "Tag discovered from durable record tags_json; save it to persist presentation settings.",
            },
            "created_at": "",
            "updated_at": now,
        }
    meta_tags = [_row_to_filter_meta_tag(row) for row in meta_rows]
    for meta_tag_id in orphan_meta_tag_ids:
        meta_tags.append(
            {
                "meta_tag_id": meta_tag_id,
                "id": meta_tag_id,
                "label": _filter_title(meta_tag_id) or meta_tag_id,
                "color": "blue",
                "priority": 0,
                "source": "orphaned-assignment",
                "custom": False,
                "provenance": {
                    "schema": "xarta.personal.filter_meta_tag.orphaned_assignment.v1",
                    "note": (
                        "Meta tag discovered from filter tag assignments; save it to persist "
                        "presentation settings."
                    ),
                },
                "created_at": "",
                "updated_at": now,
            }
        )
    return {
        "ok": True,
        "schema": "xarta.personal.filters.v1",
        "meta_tags": meta_tags,
        "tags": sorted(
            tags.values(), key=lambda item: (str(item["label"]).lower(), item["tag_id"])
        ),
        "usage_counts": usage_counts,
        "integrity": {
            "discovered_tag_ids": sorted(discovered_tag_ids),
            "orphan_meta_tag_ids": orphan_meta_tag_ids,
        },
    }


def _personal_filter_body_meta(body: Any, default_request_id: str) -> dict[str, str]:
    request_id = _clean_short_text(body.request_id, default_request_id, limit=160)
    return {
        "actor": _clean_short_text(body.actor, "blueprints-ui", limit=80),
        "source_surface": _clean_short_text(body.source_surface, "personal-filters", limit=120),
        "request_id": request_id,
        "run_id": _clean_short_text(body.run_id or body.request_id, request_id, limit=160),
    }


@router.get("/filters")
async def list_personal_filters() -> dict[str, Any]:
    with get_conn() as conn:
        return _personal_filter_registry_payload(conn)


@router.post("/filters/meta-tags")
async def upsert_personal_filter_meta_tag(
    body: PersonalFilterMetaTagUpsertRequest,
) -> dict[str, Any]:
    label = _clean_short_text(body.label, "", limit=80)
    if not label:
        raise HTTPException(400, "meta tag label is required")
    meta_tag_id = _normalise_filter_id(body.meta_tag_id or label)
    if not meta_tag_id:
        raise HTTPException(400, "meta tag id is required")
    now = _utc_now_iso()
    request_meta = _personal_filter_body_meta(
        body, f"personal-filter-meta-tag-{uuid.uuid4().hex[:12]}"
    )
    provenance = {
        "schema": "xarta.personal.filter_meta_tag.v1",
        **request_meta,
    }
    with get_conn() as conn:
        previous = conn.execute(
            "SELECT created_at FROM personal_filter_meta_tags WHERE meta_tag_id=?",
            (meta_tag_id,),
        ).fetchone()
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        conn.execute(
            """
            INSERT INTO personal_filter_meta_tags (
                meta_tag_id, label, color, priority, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(meta_tag_id) DO UPDATE SET
                label=excluded.label,
                color=excluded.color,
                priority=excluded.priority,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                meta_tag_id,
                label,
                _clean_filter_color(body.color),
                _clean_filter_priority(body.priority),
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                created_at,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM personal_filter_meta_tags WHERE meta_tag_id=?",
            (meta_tag_id,),
        ).fetchone()
        audit_id = f"audit-{uuid.uuid4().hex}"
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=request_meta["actor"],
            source_surface=request_meta["source_surface"],
            action="upsert_personal_filter_meta_tag",
            target_ref=f"personal_filter_meta_tags:{meta_tag_id}",
            file_ref="",
            db_ref=f"personal_filter_meta_tags:{meta_tag_id}",
            created_at=now,
            request_id=request_meta["request_id"],
            run_id=request_meta["run_id"],
            result="ok",
            source_hash=_hash_json_payload(dict(row)),
            metadata={"meta_tag_id": meta_tag_id},
        )
        gen = increment_gen(conn, "personal-filter-meta-tag")
        enqueue_for_all_peers(
            conn, "UPDATE", "personal_filter_meta_tags", meta_tag_id, dict(row), gen
        )
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {"ok": True, "meta_tag": _row_to_filter_meta_tag(row), "audit": audit_row}


@router.patch("/filters/meta-tags/{meta_tag_id}")
async def update_personal_filter_meta_tag(
    meta_tag_id: str,
    body: PersonalFilterMetaTagUpsertRequest,
) -> dict[str, Any]:
    return await upsert_personal_filter_meta_tag(
        PersonalFilterMetaTagUpsertRequest(
            meta_tag_id=meta_tag_id,
            label=body.label,
            color=body.color,
            priority=body.priority,
            actor=body.actor,
            source_surface=body.source_surface,
            request_id=body.request_id,
            run_id=body.run_id,
        )
    )


@router.delete("/filters/meta-tags/{meta_tag_id}")
async def delete_personal_filter_meta_tag(
    meta_tag_id: str,
    body: PersonalFilterDeleteRequest,
) -> dict[str, Any]:
    clean = _normalise_filter_id(meta_tag_id)
    if not clean:
        raise HTTPException(400, "meta tag id is required")
    now = _utc_now_iso()
    request_meta = _personal_filter_body_meta(
        body, f"personal-filter-meta-delete-{uuid.uuid4().hex[:12]}"
    )
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM personal_filter_meta_tags WHERE meta_tag_id=?",
            (clean,),
        ).fetchone()
        if not row:
            raise HTTPException(404, "meta tag not found")
        assigned = conn.execute(
            "SELECT * FROM personal_filter_tags WHERE meta_tag_id=?",
            (clean,),
        ).fetchall()
        if assigned and not body.force:
            raise HTTPException(400, "meta tag is assigned to filter tags")
        if assigned:
            for tag_row in assigned:
                conn.execute(
                    "UPDATE personal_filter_tags SET meta_tag_id='', updated_at=? WHERE tag_id=?",
                    (now, tag_row["tag_id"]),
                )
        conn.execute("DELETE FROM personal_filter_meta_tags WHERE meta_tag_id=?", (clean,))
        audit_id = f"audit-{uuid.uuid4().hex}"
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=request_meta["actor"],
            source_surface=request_meta["source_surface"],
            action="delete_personal_filter_meta_tag",
            target_ref=f"personal_filter_meta_tags:{clean}",
            file_ref="",
            db_ref=f"personal_filter_meta_tags:{clean}",
            created_at=now,
            request_id=request_meta["request_id"],
            run_id=request_meta["run_id"],
            result="ok",
            source_hash=_hash_json_payload(dict(row)),
            metadata={"meta_tag_id": clean, "cleared_filter_tag_count": len(assigned)},
        )
        gen = increment_gen(conn, "personal-filter-meta-tag-delete")
        for tag_row in assigned:
            updated = conn.execute(
                "SELECT * FROM personal_filter_tags WHERE tag_id=?",
                (tag_row["tag_id"],),
            ).fetchone()
            enqueue_for_all_peers(
                conn, "UPDATE", "personal_filter_tags", tag_row["tag_id"], dict(updated), gen
            )
        enqueue_for_all_peers(conn, "DELETE", "personal_filter_meta_tags", clean, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {"ok": True, "meta_tag_id": clean, "audit": audit_row}


@router.post("/filters/tags")
async def upsert_personal_filter_tag(body: PersonalFilterTagUpsertRequest) -> dict[str, Any]:
    label = _clean_short_text(body.label, "", limit=80)
    if not label:
        raise HTTPException(400, "filter tag label is required")
    tag_id = _normalise_filter_id(body.tag_id or label)
    if not tag_id:
        raise HTTPException(400, "filter tag id is required")
    meta_tag_id = _normalise_filter_id(body.meta_tag_id or "")
    now = _utc_now_iso()
    request_meta = _personal_filter_body_meta(body, f"personal-filter-tag-{uuid.uuid4().hex[:12]}")
    provenance = {
        "schema": "xarta.personal.filter_tag.v1",
        **request_meta,
    }
    with get_conn() as conn:
        if meta_tag_id:
            exists = conn.execute(
                "SELECT meta_tag_id FROM personal_filter_meta_tags WHERE meta_tag_id=?",
                (meta_tag_id,),
            ).fetchone()
            if not exists:
                raise HTTPException(400, "assigned meta tag does not exist")
        previous = conn.execute(
            "SELECT created_at FROM personal_filter_tags WHERE tag_id=?",
            (tag_id,),
        ).fetchone()
        created_at = previous["created_at"] if previous and previous["created_at"] else now
        conn.execute(
            """
            INSERT INTO personal_filter_tags (
                tag_id, label, color, shape, fill, meta_tag_id, builtin,
                provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tag_id) DO UPDATE SET
                label=excluded.label,
                color=excluded.color,
                shape=excluded.shape,
                fill=excluded.fill,
                meta_tag_id=excluded.meta_tag_id,
                builtin=excluded.builtin,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                tag_id,
                label,
                _clean_filter_color(body.color),
                _clean_filter_shape(body.shape),
                _clean_filter_fill(body.fill),
                meta_tag_id,
                1 if body.builtin else 0,
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                created_at,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM personal_filter_tags WHERE tag_id=?", (tag_id,)
        ).fetchone()
        usage_count = _personal_filter_usage_counts(conn).get(tag_id, 0)
        audit_id = f"audit-{uuid.uuid4().hex}"
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=request_meta["actor"],
            source_surface=request_meta["source_surface"],
            action="upsert_personal_filter_tag",
            target_ref=f"personal_filter_tags:{tag_id}",
            file_ref="",
            db_ref=f"personal_filter_tags:{tag_id}",
            created_at=now,
            request_id=request_meta["request_id"],
            run_id=request_meta["run_id"],
            result="ok",
            source_hash=_hash_json_payload(dict(row)),
            metadata={"tag_id": tag_id, "meta_tag_id": meta_tag_id},
        )
        gen = increment_gen(conn, "personal-filter-tag")
        enqueue_for_all_peers(conn, "UPDATE", "personal_filter_tags", tag_id, dict(row), gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {"ok": True, "tag": _row_to_filter_tag(row, usage_count), "audit": audit_row}


@router.patch("/filters/tags/{tag_id}")
async def update_personal_filter_tag(
    tag_id: str,
    body: PersonalFilterTagUpsertRequest,
) -> dict[str, Any]:
    return await upsert_personal_filter_tag(
        PersonalFilterTagUpsertRequest(
            tag_id=tag_id,
            label=body.label,
            color=body.color,
            shape=body.shape,
            fill=body.fill,
            meta_tag_id=body.meta_tag_id,
            builtin=body.builtin,
            actor=body.actor,
            source_surface=body.source_surface,
            request_id=body.request_id,
            run_id=body.run_id,
        )
    )


@router.delete("/filters/tags/{tag_id}")
async def delete_personal_filter_tag(
    tag_id: str,
    body: PersonalFilterDeleteRequest,
) -> dict[str, Any]:
    clean = _normalise_filter_id(tag_id)
    if not clean:
        raise HTTPException(400, "filter tag id is required")
    now = _utc_now_iso()
    request_meta = _personal_filter_body_meta(
        body, f"personal-filter-tag-delete-{uuid.uuid4().hex[:12]}"
    )
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_filter_tags WHERE tag_id=?", (clean,)).fetchone()
        if not row:
            raise HTTPException(404, "filter tag not found")
        usage_count = _personal_filter_usage_counts(conn).get(clean, 0)
        if usage_count and not body.force:
            raise HTTPException(400, "filter tag is assigned to records")
        conn.execute("DELETE FROM personal_filter_tags WHERE tag_id=?", (clean,))
        audit_id = f"audit-{uuid.uuid4().hex}"
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=request_meta["actor"],
            source_surface=request_meta["source_surface"],
            action="delete_personal_filter_tag",
            target_ref=f"personal_filter_tags:{clean}",
            file_ref="",
            db_ref=f"personal_filter_tags:{clean}",
            created_at=now,
            request_id=request_meta["request_id"],
            run_id=request_meta["run_id"],
            result="ok",
            source_hash=_hash_json_payload(dict(row)),
            metadata={"tag_id": clean, "usage_count": usage_count},
        )
        gen = increment_gen(conn, "personal-filter-tag-delete")
        enqueue_for_all_peers(conn, "DELETE", "personal_filter_tags", clean, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {"ok": True, "tag_id": clean, "audit": audit_row}


def _calendar_event_payload(
    body: CalendarEventUpsertRequest,
    *,
    event_id: str | None = None,
) -> dict[str, Any]:
    raw_local_date = _validate_local_date(body.local_date)
    range_start_date = _validate_local_date(body.range_start_date or raw_local_date)
    range_end_date = _validate_local_date(body.range_end_date or range_start_date)
    if range_end_date < range_start_date:
        range_start_date, range_end_date = range_end_date, range_start_date
    local_date = range_start_date
    timezone_name = _validate_timezone_name(body.timezone)
    start_time = None if body.all_day else _validate_local_time(body.start_time)
    end_time = None if body.all_day else _validate_local_time(body.end_time)
    start_at = _calendar_utc_iso(local_date, start_time, timezone_name)
    end_at = _calendar_utc_iso(range_end_date, end_time, timezone_name) if end_time else None
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
            "local_end_date": range_end_date,
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
                "range_end_date": payload["provenance"]["calendar"].get("local_end_date"),
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
            "SELECT * FROM personal_events WHERE event_id=?", (clean_event_id,)
        ).fetchone()
    if not existing:
        raise HTTPException(404, "calendar event not found")
    if existing["source_type"] == "manual-task":
        result = _operator_update_manual_task_event(
            _manual_task_id_from_event_row(existing),
            existing,
            title=_clean_short_text(body.title, existing["title"] or "Untitled", limit=180),
            body_text=str(body.body or "").strip(),
            local_date=_validate_local_date(body.local_date),
            local_time=None if body.all_day else _validate_local_time(body.start_time),
            all_day=bool(body.all_day),
            timezone_name=_validate_timezone_name(body.timezone),
            tags=_time_tags(body.tags, all_day=bool(body.all_day), timed=not bool(body.all_day)),
            actor=_clean_short_text(body.actor, "blueprints-ui"),
            source_surface=_clean_short_text(body.source_surface, "calendar-page"),
            request_id=_clean_short_text(
                body.request_id, f"calendar-task-edit-{uuid.uuid4().hex[:12]}", limit=160
            ),
            run_id=_clean_short_text(
                body.run_id or body.request_id,
                body.request_id or f"calendar-task-run-{uuid.uuid4().hex[:12]}",
                limit=160,
            ),
            action="update_calendar_task_event",
        )
        if result:
            return result
    if existing["source_type"] != "manual-calendar":
        raw_local_date = _validate_local_date(body.local_date)
        range_start_date = _validate_local_date(body.range_start_date or raw_local_date)
        range_end_date = _validate_local_date(body.range_end_date or range_start_date)
        if range_end_date < range_start_date:
            range_start_date, range_end_date = range_end_date, range_start_date
        local_date = range_start_date
        all_day = bool(body.all_day)
        start_time = None if all_day else _validate_local_time(body.start_time)
        end_time = None if all_day else _validate_local_time(body.end_time)
        timezone_name = _validate_timezone_name(body.timezone)
        actor = _clean_short_text(body.actor, "blueprints-ui")
        source_surface = _clean_short_text(body.source_surface, "calendar-page")
        request_id = _clean_short_text(
            body.request_id, f"calendar-event-edit-{uuid.uuid4().hex[:12]}", limit=160
        )
        run_id = _clean_short_text(
            body.run_id or body.request_id,
            body.request_id or f"calendar-run-{uuid.uuid4().hex[:12]}",
            limit=160,
        )
        result = _operator_update_personal_event_row(
            clean_event_id,
            title=_clean_short_text(body.title, existing["title"] or "Untitled", limit=180),
            body_text=str(body.body or "").strip(),
            content_text=str(body.body or "").strip(),
            local_date=local_date,
            range_end_date=range_end_date,
            local_time=start_time,
            end_time=end_time,
            all_day=all_day,
            timezone_name=timezone_name,
            tags=_time_tags(body.tags, all_day=all_day, timed=not all_day),
            actor=actor,
            source_surface=source_surface,
            request_id=request_id,
            run_id=run_id,
            action="update_calendar_event",
            not_found_message="calendar event not found",
        )
        return result
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


def _operator_update_manual_task_event(
    task_id: str,
    event_row: Any,
    *,
    title: str,
    body_text: str,
    local_date: str,
    local_time: str | None,
    all_day: bool,
    timezone_name: str,
    tags: list[str],
    actor: str,
    source_surface: str,
    request_id: str,
    run_id: str,
    action: str,
) -> dict[str, Any] | None:
    with get_conn() as conn:
        task_row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
    if not task_row:
        return None
    request = PersonalTaskUpsertRequest(
        task_id=task_id,
        title=title,
        body=body_text,
        mode=task_row["mode"],
        status=task_row["status"],
        priority=task_row["priority"],
        due_date=local_date,
        due_time=None if all_day else local_time,
        timezone=timezone_name,
        privacy_level=task_row["privacy_level"],
        tags=tags,
        related_kanban_items=_json_value(task_row["related_kanban_items_json"], []),
        related_tasks=_json_value(task_row["related_tasks_json"], []),
        related_import_batches=_json_value(task_row["related_import_batches_json"], []),
        actor=actor,
        source_surface=source_surface,
        request_id=request_id,
        run_id=run_id,
    )
    result = _upsert_personal_task(request, task_id=task_id, action=action)
    result["day"] = _build_diary_day_payload(
        result["event"].get("local_date") or event_row["local_date"] or local_date
    )
    return result


def _operator_update_personal_event_row(
    event_id: str,
    *,
    title: str,
    body_text: str,
    content_text: str,
    local_date: str,
    range_end_date: str,
    local_time: str | None,
    end_time: str | None,
    all_day: bool,
    timezone_name: str,
    tags: list[str],
    actor: str,
    source_surface: str,
    request_id: str,
    run_id: str,
    action: str,
    not_found_message: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    audit_id = f"audit-{uuid.uuid4().hex}"
    start_at = _calendar_utc_iso(local_date, local_time, timezone_name)
    end_at = (
        _calendar_utc_iso(range_end_date, end_time, timezone_name)
        if end_time and not all_day
        else None
    )
    source_hash = _hash_json_payload(
        {
            "event_id": event_id,
            "title": title,
            "content": content_text,
            "local_date": local_date,
            "range_end_date": range_end_date,
            "local_time": local_time,
            "end_time": end_time,
            "tags": tags,
        }
    )
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, not_found_message)
        db_refs = _json_value(row["db_refs_json"], [])
        audit_ref = f"personal_time_audit:{audit_id}"
        if audit_ref not in db_refs:
            db_refs.append(audit_ref)
        provenance = _json_value(row["provenance_json"], {})
        if not isinstance(provenance, dict):
            provenance = {}
        provenance["calendar"] = {
            "all_day": bool(all_day),
            "local_start_time": local_time or "",
            "local_end_time": end_time or "",
            "local_end_date": range_end_date,
            "timezone": timezone_name,
        }
        provenance["operator_edit"] = {
            "edited_at": now,
            "edited_by": actor,
            "source_surface": source_surface,
            "request_id": request_id,
            "run_id": run_id,
            "preserved_source_type": row["source_type"],
        }
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
                title,
                _body_excerpt(body_text or content_text, limit=2000),
                content_text,
                start_at,
                end_at,
                local_date,
                timezone_name,
                json.dumps(tags, ensure_ascii=True),
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
            action=action,
            target_ref=f"personal_events:{event_id}",
            file_ref="",
            db_ref=f"personal_events:{event_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={
                "event_id": event_id,
                "local_date": local_date,
                "kind": row["kind"],
                "source_type": row["source_type"],
                "operator_override": True,
            },
        )
        gen = increment_gen(conn, "personal-event-operator-edit")
        enqueue_for_all_peers(conn, "UPDATE", "personal_events", event_id, dict(updated), gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "event": _row_to_event(updated),
        "audit": {
            "audit_id": audit_id,
            "actor": actor,
            "source_surface": source_surface,
            "request_id": request_id,
            "result": "ok",
            "action": action,
        },
    }


def _operator_delete_manual_task_event(
    task_id: str,
    body: PersonalEventDeleteRequest,
    *,
    action: str,
    not_found_message: str,
) -> dict[str, Any] | None:
    actor = _clean_short_text(body.actor, "blueprints-ui")
    source_surface = _clean_short_text(body.source_surface, "personal-page")
    request_id = _clean_short_text(body.request_id, f"{action}-{uuid.uuid4().hex[:12]}", limit=160)
    run_id = _clean_short_text(body.run_id, request_id, limit=160)
    audit_id = f"audit-{uuid.uuid4().hex}"
    now = _utc_now_iso()
    with get_conn() as conn:
        task_row = conn.execute(
            "SELECT * FROM personal_time_tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if not task_row:
            return None
        event_id = task_row["event_id"] or task_id
        event_row = conn.execute(
            "SELECT * FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()
        if not event_row:
            raise HTTPException(404, not_found_message)
        source_hash = task_row["source_hash"] or hashlib.sha256(task_id.encode("utf-8")).hexdigest()
        audit_row = _write_personal_audit(
            conn,
            audit_id=audit_id,
            actor=actor,
            source_surface=source_surface,
            action=action,
            target_ref=f"personal_time_tasks:{task_id}",
            file_ref="",
            db_ref=f"personal_time_tasks:{task_id}",
            created_at=now,
            request_id=request_id,
            run_id=run_id,
            result="ok",
            source_hash=source_hash,
            metadata={
                "task_id": task_id,
                "event_id": event_id,
                "source_type": task_row["source_type"],
                "operator_override": True,
            },
        )
        conn.execute("DELETE FROM personal_time_tasks WHERE task_id=?", (task_id,))
        conn.execute("DELETE FROM personal_events WHERE event_id=?", (event_id,))
        gen = increment_gen(conn, "personal-task-operator-delete")
        enqueue_for_all_peers(conn, "DELETE", "personal_time_tasks", task_id, {}, gen)
        enqueue_for_all_peers(conn, "DELETE", "personal_events", event_id, {}, gen)
        enqueue_for_all_peers(conn, "UPDATE", "personal_time_audit", audit_id, audit_row, gen)
    return {
        "ok": True,
        "event_id": event_id,
        "deleted_event": _row_to_event(event_row),
        "deleted_task": _row_to_task(task_row),
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
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
    if not row:
        raise HTTPException(404, "calendar event not found")
    if row["source_type"] == "manual-task":
        result = _operator_delete_manual_task_event(
            _manual_task_id_from_event_row(row),
            body,
            action="delete_calendar_event",
            not_found_message="calendar event not found",
        )
        if result:
            return result
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
    day_overlap = f"local_date <= ? AND {PERSONAL_EVENT_LOCAL_END_DATE_SQL} >= ?"
    where = [day_overlap, "privacy_level != 'pin'"]
    params: list[Any] = [local_date, local_date]
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
            f"""
            SELECT COUNT(*) AS count
            FROM personal_events
            WHERE {day_overlap} AND privacy_level='pin'
            """,
            (local_date, local_date),
        ).fetchone()
        source_rows = conn.execute(
            f"""
            SELECT source_type, COUNT(*) AS count
            FROM personal_events
            WHERE {day_overlap} AND privacy_level != 'pin'
            GROUP BY source_type
            """,
            (local_date, local_date),
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


def _is_automation_proof_entry(
    *,
    body: str = "",
    actor: str = "",
    source_surface: str = "",
    request_id: str = "",
    run_id: str = "",
    tags: list[str] | None = None,
    existing_kind: str = "",
) -> bool:
    clean_tags = [str(tag).strip().lower() for tag in tags or [] if str(tag).strip()]
    if existing_kind == "automation-proof" or "automation-proof" in clean_tags:
        return True
    context = " ".join(
        [
            body,
            actor,
            source_surface,
            request_id,
            run_id,
            " ".join(clean_tags),
        ]
    ).lower()
    proofish = "proof" in context or "playwright" in context
    automationish = any(
        marker in context
        for marker in (
            "codex",
            "playwright",
            "automation",
            "active browser",
            "live proof",
        )
    )
    return proofish and automationish


def _diary_entry_tags(
    tags: list[str] | None,
    *,
    all_day: bool,
    body: str = "",
    actor: str = "",
    source_surface: str = "",
    request_id: str = "",
    run_id: str = "",
    existing_kind: str = "",
) -> list[str]:
    automation_proof = _is_automation_proof_entry(
        body=body,
        actor=actor,
        source_surface=source_surface,
        request_id=request_id,
        run_id=run_id,
        tags=tags,
        existing_kind=existing_kind,
    )
    event_tags = ["diary", "quick-entry"]
    if automation_proof:
        event_tags.append("automation-proof")
    else:
        event_tags.insert(1, "personal-log")
    for tag in tags or []:
        clean = str(tag).strip()
        if automation_proof and clean.lower() == "personal-log":
            continue
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
    range_end_date: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    local_date = result["local_date"]
    range_end_date = range_end_date or local_date
    filename = result["filename"]
    event_id = f"diary-{local_date}-{Path(filename).stem}"
    timezone_name = result.get("timezone") or os.environ.get(
        "XARTA_DIARY_TIMEZONE", "Europe/London"
    )
    event_tags = _diary_entry_tags(
        tags,
        all_day=all_day,
        body=body,
        actor=actor,
        source_surface=source_surface,
        request_id=request_id,
        run_id=run_id,
    )
    event_kind = "automation-proof" if "automation-proof" in event_tags else "personal-log"
    provenance = {
        "writer": "xarta_diary.create_personal_log",
        "calendar": {
            "all_day": bool(all_day),
            "local_start_time": local_time or "",
            "local_end_time": end_time or "",
            "local_end_date": range_end_date,
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
    start_at = _calendar_utc_iso(local_date, local_time, timezone_name)
    end_at = (
        _calendar_utc_iso(range_end_date, end_time, timezone_name)
        if end_time and not all_day
        else None
    )
    with get_conn() as conn:
        _upsert_personal_source(conn, now)
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
                event_kind,
                _entry_title(body, result.get("local_time")),
                _body_excerpt(body),
                body.strip(),
                start_at,
                end_at,
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
                "range_end_date": range_end_date,
                "kind": event_kind,
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
    raw_local_date = _validate_local_date(body.local_date)
    range_start_date = _validate_local_date(body.range_start_date or raw_local_date)
    range_end_date = _validate_local_date(body.range_end_date or range_start_date)
    if range_end_date < range_start_date:
        range_start_date, range_end_date = range_end_date, range_start_date
    local_date = range_start_date
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
        range_end_date=range_end_date,
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
    now = _utc_now_iso()
    source_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
    if not row:
        raise HTTPException(404, "diary entry not found")
    existing_tags = _json_value(row["tags_json"], [])
    if row["source_type"] == "manual-task":
        title, task_body = _operator_event_text_parts(text, row["title"])
        result = _operator_update_manual_task_event(
            _manual_task_id_from_event_row(row),
            row,
            title=title,
            body_text=task_body,
            local_date=local_date,
            local_time=local_time,
            all_day=all_day,
            timezone_name=timezone_name,
            tags=_time_tags(body.tags, all_day=all_day, timed=bool(local_time)),
            actor=actor,
            source_surface=source_surface,
            request_id=request_id,
            run_id=run_id,
            action="update_diary_task_event",
        )
        if result:
            return result
    if row["kind"] != "personal-log" and "quick-entry" not in existing_tags:
        title, source_body = _operator_event_text_parts(text, row["title"])
        result = _operator_update_personal_event_row(
            event_id,
            title=title,
            body_text=source_body,
            content_text=text,
            local_date=local_date,
            range_end_date=range_end_date,
            local_time=local_time,
            end_time=end_time,
            all_day=all_day,
            timezone_name=timezone_name,
            tags=_time_tags(body.tags, all_day=all_day, timed=bool(local_time)),
            actor=actor,
            source_surface=source_surface,
            request_id=request_id,
            run_id=run_id,
            action="update_diary_source_event",
            not_found_message="diary entry not found",
        )
        result["day"] = _build_diary_day_payload(local_date)
        return result
    event_tags = _diary_entry_tags(
        [str(tag).strip() for tag in body.tags if str(tag).strip()],
        all_day=all_day,
        body=text,
        actor=actor,
        source_surface=source_surface,
        request_id=request_id,
        run_id=run_id,
        existing_kind=row["kind"],
    )
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
        if not row:
            raise HTTPException(404, "diary entry not found")
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
                "kind": row["kind"],
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
        row = conn.execute("SELECT * FROM personal_events WHERE event_id=?", (event_id,)).fetchone()
    if not row:
        raise HTTPException(404, "diary entry not found")
    if row["source_type"] == "manual-task":
        result = _operator_delete_manual_task_event(
            _manual_task_id_from_event_row(row),
            body,
            action="delete_diary_entry",
            not_found_message="diary entry not found",
        )
        if result:
            local_date = result["deleted_event"].get("local_date") or _validate_local_date(None)
            result["day"] = _build_diary_day_payload(local_date)
            return result
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
