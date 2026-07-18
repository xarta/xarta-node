"""
routes_sync.py — endpoints that implement the Layer 1 + Layer 2 sync protocol.

POST /api/v1/sync/actions             — receive a batch of CRUD actions from a peer
                                       (commit guard: rejects DB writes from older commits)
POST /api/v1/sync/restore             — receive a full DB backup zip (Layer 1) + SHA-256
GET  /api/v1/sync/export              — serve the current DB backup zip for a peer to pull
GET  /api/v1/sync/status              — current sync state summary
POST /api/v1/sync/git-pull            — trigger a git pull on this node + queue for peers
GET  /api/v1/sync/table-hash/{table}  — return row-count + SHA-256 digest of table content
GET  /api/v1/sync/parity/{table}      — compare local table hash against all peers
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import sqlite3
import ssl
import subprocess as _sp
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from . import config as cfg
from . import timing
from .auth import compute_token
from .db import get_conn, get_gen, get_meta, get_read_conn, increment_gen
from .events import bus as events_bus
from .kanban_automation_scheduler import (
    PROVIDER_ID as KANBAN_PROVIDER_ID,
)
from .kanban_automation_scheduler import (
    pause_kanban_claims_for_restart,
    resume_kanban_claims_after_restart_abort,
)
from .models import GitPullRequest, SyncActionsPayload, SyncStatus
from .personal_search_scheduler import (
    PROVIDER_ID as PERSONAL_SEARCH_PROVIDER_ID,
)
from .personal_search_scheduler import (
    pause_personal_search_claims_for_restart,
    resume_personal_search_claims_after_restart_abort,
)
from .routes_scheduler_coordination import scheduler_local_get_json
from .sync.queue import (
    enqueue,
    enqueue_for_all_peers,
    get_sent_queue_retention_summary,
    kanban_table_fleet_sync_disabled,
    prune_sent_actions,
)
from .sync.restore import apply_restore, make_full_backup
from .sync.sqlite_maintenance import sqlite_file_stats

log = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["sync"])

_FORCE_RESTORE_HEADER = "x-blueprints-force-restore"
_RESTORE_OP_HEADER = "x-blueprints-restore-op"
_SAFE_BACKUP_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{6}-blueprints\.db\.tar\.gz$")
_receive_actions_apply_lock: asyncio.Lock | None = None
_SYNC_STATUS_CACHE_TTL_SECONDS = 0.1
_sync_status_cache_lock = asyncio.Lock()
_sync_status_cache_payload: SyncStatus | None = None
_sync_status_cache_expires_monotonic = 0.0
_sync_status_inflight_task: asyncio.Task[SyncStatus] | None = None
_SYNC_STATUS_SQLITE_BUSY_TIMEOUT_MS = 100

# Tables that actions are permitted to touch (safeguard against bad payloads)
# NOTE: "nodes" is intentionally excluded — the nodes table is local-only,
# populated from .nodes.json on each node. Incoming sync entries for nodes
# are silently dropped to prevent stale peer data overwriting the JSON-derived
# addresses and config.
_ALLOWED_TABLES = {
    "services",
    "machines",
    "pfsense_dns",
    "proxmox_config",
    "proxmox_nets",
    "vlans",
    "dockge_stacks",
    "dockge_stack_services",
    "caddy_configs",
    "settings",
    "pve_hosts",
    "arp_manual",
    "ssh_targets",
    "manual_links",
    "manual_link_categories",
    "manual_link_category_items",
    "docs",
    "doc_groups",
    "doc_images",
    "ai_providers",
    "ai_project_assignments",
    "bookmarks",
    "visits",
    "bookmark_deletions",
    "visit_events",
    "nav_items",
    "form_controls",
    "embed_menu_items",
    "table_layout_catalog",
    "table_layouts",
    "pockettts_tags",
    "pockettts_voices",
    "pockettts_voice_meta",
    "pockettts_voice_tags",
    "pockettts_tag_order",
    "disks_filesystem_favorites",
    "personal_events",
    "personal_time_tasks",
    "personal_sources",
    "personal_filter_tags",
    "personal_filter_meta_tags",
    "personal_import_batches",
    "personal_git_repositories",
    "personal_git_commits",
    "personal_git_features",
    "personal_git_kanban_arcs",
    "personal_git_daily_summaries",
    "personal_git_import_runs",
    "personal_time_audit",
    "personal_graph_links",
    "kanban_item_states",
    "kanban_item_priorities",
    "kanban_items",
    "kanban_item_order_edges",
    "kanban_priority_recommendations",
    "kanban_item_links",
    "kanban_item_commits",
    "kanban_review_decisions",
    "kanban_review_processor_leases",
    "kanban_review_processor_markers",
    "kanban_review_processor_failure_events",
    "kanban_agent_hints",
    "kanban_agent_sessions",
    "kanban_blockers",
    "kanban_discussions",
    "kanban_audit_log",
}

# Action types that trigger local execution rather than a DB write
_SYSTEM_ACTION_TYPES = {"sync_git_outer", "sync_git_inner", "sync_git_non_root"}
_GIT_PULL_SCOPE_ORDER = ("outer", "non_root", "inner")
_GIT_PULL_LOCK = asyncio.Lock()
_RESTART_PENDING = False
_ACTIVE_GIT_PULL_OPERATION_IDS: set[str] = set()
_GIT_PULL_OPERATION_ID = re.compile(r"^git-pull-[0-9a-f]{32}$")
_GIT_PULL_RECEIPT_LIMIT = 128
_GIT_PULL_RESULT_MAX_BYTES = 32_768
_PROCESS_STARTED_AT = datetime.now(timezone.utc).isoformat()
_BLUEPRINTS_SCHEDULER_PROVIDER_IDS = {
    PERSONAL_SEARCH_PROVIDER_ID,
    KANBAN_PROVIDER_ID,
}
_SCHEDULER_RESTART_MAX_SCHEDULES = 32
_SCHEDULER_SCHEDULE_ID = re.compile(r"^xarta-schedule-[0-9a-f]{24}$")
_SCHEDULER_RESTART_SNAPSHOT_SCHEMA = "xarta.scheduler.restart_active_work_snapshot.v2"
_POSTGRES_SNAPSHOT_ID = re.compile(r"^[0-9]+:[0-9]+:(?:[0-9]+(?:,[0-9]+)*)?$")


class GitPullOperationConflict(RuntimeError):
    pass


class SchedulerRestartRefused(RuntimeError):
    def __init__(self, code: str, detail: dict[str, Any]):
        super().__init__(code)
        self.code = code
        self.detail = detail


# ── Git pull helpers ──────────────────────────────────────────────────────────


def _repo_pull_targets() -> dict[str, tuple[str, bool]]:
    return {
        "outer": (cfg.REPO_OUTER_PATH, True),
        "inner": (cfg.REPO_INNER_PATH, True),
        "non_root": (cfg.REPO_NON_ROOT_PATH, False),
    }


def _git_head_sync(repo_path: str, label: str) -> str | None:
    if not repo_path or not os.path.isdir(os.path.join(repo_path, ".git")):
        return None
    try:
        return _sp.check_output(
            ["git", "-C", repo_path, "rev-parse", "HEAD"],
            stderr=_sp.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        log.warning("git head [%s] failed during runtime snapshot", label)
        return None


def _capture_runtime_repo_heads() -> dict[str, str]:
    heads: dict[str, str] = {}
    for label, (repo_path, restart_service) in _repo_pull_targets().items():
        if not restart_service:
            continue
        head = _git_head_sync(repo_path, label)
        if head:
            heads[label] = head
    return heads


_RUNNING_RUNTIME_REPO_HEADS = _capture_runtime_repo_heads()


def _ordered_git_scopes(scopes) -> list[str]:
    wanted = {scope for scope in scopes if scope in _GIT_PULL_SCOPE_ORDER}
    return [scope for scope in _GIT_PULL_SCOPE_ORDER if scope in wanted]


def _git_pull_request_json(scopes: list[str], expected_head: str) -> str:
    return json.dumps(
        {"expected_head": expected_head, "local_only": True, "scopes": scopes},
        sort_keys=True,
        separators=(",", ":"),
    )


def _git_pull_operation_create_sync(
    operation_id: str, scopes: list[str], expected_head: str
) -> dict[str, Any]:
    request_json = _git_pull_request_json(scopes, expected_head)
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM sync_git_pull_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        if existing is not None:
            if existing["request_json"] != request_json:
                raise GitPullOperationConflict("operation_id was reused with a different request")
            return {**_git_pull_operation_public(existing), "idempotent_replay": True}
        conn.execute(
            """INSERT INTO sync_git_pull_operations(
                   operation_id,request_json,status,result_json,error_code
               ) VALUES(?,?,?,'{}','')""",
            (operation_id, request_json, "queued"),
        )
        conn.execute(
            """DELETE FROM sync_git_pull_operations
               WHERE status IN ('completed','blocked','failed')
                 AND operation_id NOT IN (
                   SELECT operation_id FROM sync_git_pull_operations
                   ORDER BY updated_at DESC,operation_id DESC LIMIT ?
               )""",
            (_GIT_PULL_RECEIPT_LIMIT,),
        )
        row = conn.execute(
            "SELECT * FROM sync_git_pull_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        assert row is not None
        return {**_git_pull_operation_public(row), "idempotent_replay": False}


def _git_pull_operation_update_sync(
    operation_id: str,
    status: str,
    result: dict[str, Any],
    error_code: str = "",
) -> dict[str, Any]:
    result_json = json.dumps(result, sort_keys=True, separators=(",", ":"))
    if len(result_json.encode("utf-8")) > _GIT_PULL_RESULT_MAX_BYTES:
        raise ValueError("git-pull operation result exceeded its bound")
    with get_conn() as conn:
        cursor = conn.execute(
            """UPDATE sync_git_pull_operations
               SET status=?,result_json=?,error_code=?,
                   updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
               WHERE operation_id=?""",
            (status, result_json, error_code, operation_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(operation_id)
        row = conn.execute(
            "SELECT * FROM sync_git_pull_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        assert row is not None
        return _git_pull_operation_public(row)


def _git_pull_operation_get_sync(operation_id: str) -> dict[str, Any] | None:
    with get_read_conn(
        busy_timeout_ms=100,
        operation="sync_git_pull_operation_get",
    ) as conn:
        row = conn.execute(
            "SELECT * FROM sync_git_pull_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        return _git_pull_operation_public(row) if row is not None else None


def _restart_operation_create_sync(operation_id: str) -> dict[str, Any]:
    request_json = json.dumps(
        {"operation": "blueprints_restart", "node_id": cfg.NODE_ID},
        sort_keys=True,
        separators=(",", ":"),
    )
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO sync_git_pull_operations(
                   operation_id,request_json,status,result_json,error_code
               ) VALUES(?,?,?,'{}','')""",
            (operation_id, request_json, "queued"),
        )
        conn.execute(
            """DELETE FROM sync_git_pull_operations
               WHERE status IN ('completed','blocked','failed')
                 AND operation_id NOT IN (
                   SELECT operation_id FROM sync_git_pull_operations
                   ORDER BY updated_at DESC,operation_id DESC LIMIT ?
               )""",
            (_GIT_PULL_RECEIPT_LIMIT,),
        )
        row = conn.execute(
            "SELECT * FROM sync_git_pull_operations WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        assert row is not None
        return _git_pull_operation_public(row)


def _restart_requested_operations_sync() -> list[dict[str, Any]]:
    with get_read_conn(
        busy_timeout_ms=100,
        operation="sync_restart_requested_operations",
    ) as conn:
        rows = conn.execute(
            """SELECT * FROM sync_git_pull_operations
               WHERE status IN ('restart_requested','post_restart_proof_failed')
               ORDER BY updated_at DESC,operation_id DESC LIMIT 16"""
        ).fetchall()
        return [_git_pull_operation_public(row) for row in rows]


def _git_pull_operation_public(row) -> dict[str, Any]:
    try:
        request = json.loads(row["request_json"])
        result = json.loads(row["result_json"])
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("git-pull operation receipt is malformed") from exc
    if not isinstance(request, dict) or not isinstance(result, dict):
        raise RuntimeError("git-pull operation receipt is malformed")
    return {
        "schema": "xarta.blueprints.git_pull_operation.v1",
        "operation_id": row["operation_id"],
        "request": request,
        "status": row["status"],
        "result": result,
        "error_code": row["error_code"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "terminal": row["status"] in {"completed", "blocked", "failed"},
    }


async def _update_git_pull_operation(
    operation_id: str | None,
    status: str,
    result: dict[str, Any],
    error_code: str = "",
) -> dict[str, Any] | None:
    if operation_id is None:
        return None
    return await timing.to_thread(
        "sync.git_pull_operation_update",
        _git_pull_operation_update_sync,
        operation_id,
        status,
        result,
        error_code,
    )


def _raw_client_is_loopback(request: Request) -> bool:
    host = request.client.host if request.client is not None else ""
    return host in {"127.0.0.1", "::1"}


def _scope_for_system_action(action_type: str) -> str | None:
    prefix = "sync_git_"
    if action_type.startswith(prefix):
        return action_type[len(prefix) :]
    return None


async def _git_head(repo_path: str, label: str) -> str | None:
    proc = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "rev-parse",
        "HEAD",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        log.warning("git head [%s] failed: %s", label, stderr.decode().strip())
        return None
    return stdout.decode().strip()


async def _git_pull(repo_path: str, label: str) -> bool:
    """Run git pull for one repo and return whether HEAD changed."""
    if not repo_path:
        raise RuntimeError(f"git_pull_repo_missing:{label}")
    before = await _git_head(repo_path, label)
    proc = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "pull",
        "--ff-only",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode == 0:
        result = stdout.decode().strip()
        after = await _git_head(repo_path, label)
        changed = before is not None and after is not None and before != after
        if before is None or after is None:
            changed = "Already up to date." not in result
        log.info("git pull [%s]: %s (%s)", label, result, "changed" if changed else "unchanged")
        return changed
    log.warning("git pull [%s] failed: %s", label, stderr.decode().strip())
    raise RuntimeError(f"git_pull_failed:{label}")


async def _git_pull_exact(repo_path: str, label: str, expected_head: str) -> bool:
    """Fetch and fast-forward to one reviewed commit, never an unreviewed newer tip."""
    if not repo_path:
        raise RuntimeError(f"git_pull_repo_missing:{label}")
    before = await _git_head(repo_path, label)
    fetch = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "fetch",
        "--no-tags",
        "origin",
        "main",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _fetch_stdout, fetch_stderr = await fetch.communicate()
    if fetch.returncode != 0:
        log.warning("git fetch exact [%s] failed: %s", label, fetch_stderr.decode().strip())
        raise RuntimeError(f"git_fetch_expected_failed:{label}")
    contains = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "merge-base",
        "--is-ancestor",
        expected_head,
        "origin/main",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _contains_stdout, contains_stderr = await contains.communicate()
    if contains.returncode != 0:
        log.warning(
            "expected head [%s] is not on origin/main: %s",
            label,
            contains_stderr.decode().strip(),
        )
        raise RuntimeError(f"expected_head_not_on_origin_main:{label}")
    merge = await asyncio.create_subprocess_exec(
        "git",
        "-C",
        repo_path,
        "merge",
        "--ff-only",
        expected_head,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    merge_stdout, merge_stderr = await merge.communicate()
    if merge.returncode != 0:
        log.warning("git merge exact [%s] failed: %s", label, merge_stderr.decode().strip())
        raise RuntimeError(f"git_merge_expected_failed:{label}")
    after = await _git_head(repo_path, label)
    if after != expected_head:
        raise RuntimeError(f"expected_head_mismatch:{label}")
    changed = before != after
    log.info(
        "git exact [%s]: %s (%s)",
        label,
        merge_stdout.decode().strip(),
        "changed" if changed else "unchanged",
    )
    return changed


async def _runtime_repo_is_stale(repo_path: str, label: str) -> bool:
    """Return True when the checked-out runtime repo is newer than this process."""
    if not repo_path:
        return False
    head = await _git_head(repo_path, label)
    running = _RUNNING_RUNTIME_REPO_HEADS.get(label)
    if not head or not running:
        return False
    stale = head != running
    if stale:
        log.info(
            "runtime repo [%s] is newer than process: running=%s disk=%s",
            label,
            running[:12],
            head[:12],
        )
    return stale


def _strict_nonnegative_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise SchedulerRestartRefused(
            "scheduler_status_malformed",
            {"field": field, "reason": "expected_nonnegative_integer"},
        )
    return value


def _strict_scheduler_restart_snapshot(
    payload: dict[str, Any], *, queued_provider_ids: set[str] | None = None
) -> dict[str, Any]:
    if payload.get("schema") != _SCHEDULER_RESTART_SNAPSHOT_SCHEMA:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "schema", "reason": "unsupported"}
        )
    captured_at = payload.get("captured_at")
    snapshot_id = payload.get("snapshot_id")
    generation = _strict_nonnegative_int(payload.get("generation"), "generation")
    if generation < 1:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "generation", "reason": "expected_positive"}
        )
    if not isinstance(captured_at, str) or not captured_at:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "captured_at", "reason": "expected_string"}
        )
    if not isinstance(snapshot_id, str) or _POSTGRES_SNAPSHOT_ID.fullmatch(snapshot_id) is None:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "snapshot_id", "reason": "invalid"}
        )
    health = payload.get("health")
    if not isinstance(health, dict):
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "health", "reason": "expected_object"}
        )
    required_true = ("ok", "coordinator_available", "executor_available")
    for field in required_true:
        if health.get(field) is not True:
            raise SchedulerRestartRefused(
                "scheduler_unhealthy",
                {"field": f"health.{field}", "value": health.get(field)},
            )
    if health.get("database") != "available":
        raise SchedulerRestartRefused(
            "scheduler_unhealthy",
            {"field": "health.database", "value": health.get("database")},
        )
    schedule_count = _strict_nonnegative_int(payload.get("schedule_count"), "schedule_count")
    if schedule_count > _SCHEDULER_RESTART_MAX_SCHEDULES:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed",
            {"field": "schedule_count", "reason": "exceeds_bound"},
        )
    schedules = payload.get("schedules")
    if not isinstance(schedules, list) or len(schedules) != schedule_count:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed",
            {
                "expected_schedule_count": schedule_count,
                "actual_schedule_count": len(schedules) if isinstance(schedules, list) else None,
            },
        )
    active_work: list[dict[str, Any]] = []
    seen: set[str] = set()
    for schedule in schedules:
        if not isinstance(schedule, dict):
            raise SchedulerRestartRefused(
                "scheduler_snapshot_malformed", {"reason": "schedule_not_object"}
            )
        schedule_id = schedule.get("schedule_id")
        execution_mode = schedule.get("execution_mode")
        provider_id = schedule.get("provider_id")
        if (
            not isinstance(schedule_id, str)
            or _SCHEDULER_SCHEDULE_ID.fullmatch(schedule_id) is None
            or schedule_id in seen
            or not isinstance(provider_id, str)
            or execution_mode not in {"local", "provider"}
            or (execution_mode == "provider" and not provider_id)
            or (execution_mode == "local" and provider_id)
        ):
            raise SchedulerRestartRefused(
                "scheduler_snapshot_malformed",
                {"reason": "schedule_identity_invalid", "schedule_id": schedule_id},
            )
        seen.add(schedule_id)
        active_work.append(
            {
                "schedule_id": schedule_id,
                "execution_mode": execution_mode,
                "provider_id": provider_id,
                "queued_runs": _strict_nonnegative_int(
                    schedule.get("queued_runs"), "schedule.queued_runs"
                ),
                "running_runs": _strict_nonnegative_int(
                    schedule.get("running_runs"), "schedule.running_runs"
                ),
                "stale_running_runs": _strict_nonnegative_int(
                    schedule.get("stale_running_runs"), "schedule.stale_running_runs"
                ),
                "owner_mismatch_runs": _strict_nonnegative_int(
                    schedule.get("owner_mismatch_runs"), "schedule.owner_mismatch_runs"
                ),
            }
        )
    global_work = payload.get("global_work")
    if not isinstance(global_work, dict):
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed", {"field": "global_work", "reason": "expected_object"}
        )
    global_counts = {
        field: _strict_nonnegative_int(global_work.get(field), f"global_work.{field}")
        for field in ("queued_runs", "running_runs", "stale_running_runs")
    }
    for field in global_counts:
        if sum(row[field] for row in active_work) != global_counts[field]:
            raise SchedulerRestartRefused(
                "scheduler_snapshot_malformed", {"field": field, "reason": "sum_mismatch"}
            )
    actual_blueprints = {
        schedule["provider_id"]
        for schedule in active_work
        if schedule["provider_id"] in _BLUEPRINTS_SCHEDULER_PROVIDER_IDS
    }
    if actual_blueprints != _BLUEPRINTS_SCHEDULER_PROVIDER_IDS:
        raise SchedulerRestartRefused(
            "scheduler_snapshot_malformed",
            {
                "reason": "blueprints_provider_inventory_incomplete",
                "expected_provider_ids": sorted(_BLUEPRINTS_SCHEDULER_PROVIDER_IDS),
                "actual_provider_ids": sorted(actual_blueprints),
            },
        )
    if global_counts["stale_running_runs"]:
        raise SchedulerRestartRefused("scheduler_stale_work", global_counts)
    ownership_mismatches = [row for row in active_work if row["owner_mismatch_runs"]]
    if ownership_mismatches:
        raise SchedulerRestartRefused(
            "scheduler_active_owner_mismatch", {"active_work": ownership_mismatches}
        )
    queued_provider_ids = (
        set(_BLUEPRINTS_SCHEDULER_PROVIDER_IDS)
        if queued_provider_ids is None
        else queued_provider_ids
    )
    unsafe_blueprints = [
        row
        for row in active_work
        if row["provider_id"] in _BLUEPRINTS_SCHEDULER_PROVIDER_IDS
        and (
            row["running_runs"]
            or (row["provider_id"] in queued_provider_ids and row["queued_runs"])
        )
    ]
    if unsafe_blueprints:
        raise SchedulerRestartRefused(
            "blueprints_scheduler_not_quiescent", {"active_work": unsafe_blueprints}
        )
    return {
        "schema": _SCHEDULER_RESTART_SNAPSHOT_SCHEMA,
        "captured_at": captured_at,
        "snapshot_id": snapshot_id,
        "generation": generation,
        "schedule_count": schedule_count,
        "global_work": global_counts,
        "health": {
            "ok": True,
            "database": "available",
            "coordinator_available": True,
            "executor_available": True,
            "worker_stale_seconds": _strict_nonnegative_int(
                health.get("worker_stale_seconds"), "health.worker_stale_seconds"
            ),
        },
        "active_work": active_work,
        "non_blueprints_active_work": [
            row
            for row in active_work
            if row["provider_id"] not in _BLUEPRINTS_SCHEDULER_PROVIDER_IDS
            and (row["queued_runs"] or row["running_runs"])
        ],
    }


async def _scheduler_quiescence_snapshot(
    providers: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    queued_provider_ids = None
    if providers is not None:
        queued_provider_ids = {
            str(provider["provider_id"])
            for provider in providers
            if provider.get("provider_effective_enabled") is True
        }
    try:
        return _strict_scheduler_restart_snapshot(
            await scheduler_local_get_json("/restart-snapshot"),
            queued_provider_ids=queued_provider_ids,
        )
    except SchedulerRestartRefused:
        raise
    except Exception as exc:
        raise SchedulerRestartRefused(
            "scheduler_status_unavailable",
            {"error_class": type(exc).__name__},
        ) from exc


async def _resume_blueprints_provider_claims() -> None:
    results = await asyncio.gather(
        resume_personal_search_claims_after_restart_abort(),
        resume_kanban_claims_after_restart_abort(),
        return_exceptions=True,
    )
    for result in results:
        if isinstance(result, BaseException):
            log.exception("failed to resume a Blueprints scheduler claim gate", exc_info=result)


async def _pause_blueprints_provider_claims() -> list[dict[str, Any]]:
    results = await asyncio.gather(
        pause_personal_search_claims_for_restart(),
        pause_kanban_claims_for_restart(),
        return_exceptions=True,
    )
    if any(isinstance(result, BaseException) for result in results):
        await _resume_blueprints_provider_claims()
        raise SchedulerRestartRefused(
            "provider_claim_gate_failed",
            {
                "errors": [
                    type(result).__name__ for result in results if isinstance(result, BaseException)
                ]
            },
        )
    providers = [result for result in results if isinstance(result, dict)]
    expected_provider_ids = {PERSONAL_SEARCH_PROVIDER_ID, KANBAN_PROVIDER_ID}
    actual_provider_ids = {
        provider.get("provider_id")
        for provider in providers
        if isinstance(provider.get("provider_id"), str)
    }
    malformed_provider = any(
        provider.get("claim_loop_paused") is not True
        or not isinstance(provider.get("provider_effective_enabled"), bool)
        or not isinstance(provider.get("active_run_ids"), list)
        for provider in providers
    )
    if actual_provider_ids != expected_provider_ids or malformed_provider:
        await _resume_blueprints_provider_claims()
        raise SchedulerRestartRefused(
            "provider_claim_gate_malformed",
            {
                "expected_provider_ids": sorted(expected_provider_ids),
                "actual_provider_ids": sorted(actual_provider_ids),
                "result_count": len(providers),
            },
        )
    active = {
        str(provider.get("provider_id") or "unknown"): provider.get("active_run_ids")
        for provider in providers
        if provider.get("active_run_ids")
    }
    legacy_enabled = any(
        provider.get("legacy_loop_effective_enabled") is True for provider in providers
    )
    if active or legacy_enabled:
        await _resume_blueprints_provider_claims()
        raise SchedulerRestartRefused(
            "blueprints_provider_not_quiescent",
            {"active_run_ids": active, "legacy_loop_effective_enabled": legacy_enabled},
        )
    return providers


async def _pause_and_snapshot_scheduler() -> dict[str, Any]:
    providers = await _pause_blueprints_provider_claims()
    try:
        scheduler = await _scheduler_quiescence_snapshot(providers)
    except Exception:
        await _resume_blueprints_provider_claims()
        raise
    return {"providers": providers, "scheduler": scheduler}


async def _runtime_heads() -> dict[str, str]:
    heads: dict[str, str] = {}
    for label, (repo_path, restart_service) in _repo_pull_targets().items():
        if not restart_service:
            continue
        head = await _git_head(repo_path, label)
        if not isinstance(head, str) or re.fullmatch(r"[0-9a-f]{40}", head) is None:
            raise RuntimeError(f"git_head_unavailable:{label}")
        heads[label] = head
    return heads


async def _git_pull_scopes_and_maybe_restart(
    scopes,
    *,
    source: str = "",
    operation_id: str | None = None,
    expected_head: str = "",
) -> dict[str, Any] | None:
    """Run a coalesced git-pull batch and restart once if runtime code changed."""
    global _RESTART_PENDING
    ordered_scopes = _ordered_git_scopes(scopes)
    if not ordered_scopes:
        return None
    source_label = f" {source}" if source else ""
    targets = _repo_pull_targets()
    restart_needed = False
    claims_paused = False
    guard_before: dict[str, Any] = {}
    running_heads_before: dict[str, str] = {}
    operation_evidence: dict[str, Any] = {}
    restart_receipt_id = operation_id
    async with _GIT_PULL_LOCK:
        if _RESTART_PENDING:
            log.info("git pull batch%s skipped: service restart already pending", source_label)
            await _update_git_pull_operation(
                operation_id,
                "blocked",
                {"reason": "restart_already_pending"},
                "restart_already_pending",
            )
            return {"ok": False, "code": "restart_already_pending"}
        try:
            if operation_id is not None:
                running_heads_before = dict(_RUNNING_RUNTIME_REPO_HEADS)
                guard_before = await _pause_and_snapshot_scheduler()
                claims_paused = True
                operation_evidence = {
                    "guard_before_pull": guard_before,
                    "initiating_node_id": cfg.NODE_ID,
                    "initiating_pid": os.getpid(),
                    "process_started_at": _PROCESS_STARTED_AT,
                    "running_heads_before": running_heads_before,
                }
                await _update_git_pull_operation(
                    operation_id,
                    "pulling",
                    operation_evidence,
                )
            log.info("git pull batch%s: scopes=%s", source_label, ",".join(ordered_scopes))
            for scope in ordered_scopes:
                repo_path, restart_service = targets[scope]
                changed = (
                    await _git_pull_exact(repo_path, scope, expected_head)
                    if operation_id is not None
                    else await _git_pull(repo_path, scope)
                )
                runtime_stale = restart_service and await _runtime_repo_is_stale(repo_path, scope)
                restart_needed = restart_needed or (restart_service and (changed or runtime_stale))
            disk_heads_after = await _runtime_heads() if operation_id is not None else {}
            if operation_id is not None and disk_heads_after.get("outer") != expected_head:
                raise RuntimeError("expected_head_mismatch:outer")
            if restart_needed:
                if not cfg.SERVICE_RESTART_CMD:
                    raise RuntimeError("service_restart_command_unset")
                if restart_receipt_id is None:
                    restart_receipt_id = f"git-pull-{uuid.uuid4().hex}"
                    await timing.to_thread(
                        "sync.restart_operation_create",
                        _restart_operation_create_sync,
                        restart_receipt_id,
                    )
                _RESTART_PENDING = True
                restart_heads = (
                    disk_heads_after if operation_id is not None else await _runtime_heads()
                )
                restarted = await _restart_service(
                    operation_id=restart_receipt_id,
                    expected_runtime_heads=restart_heads,
                    guard_before=guard_before,
                    claims_already_paused=claims_paused,
                )
                if not restarted:
                    raise RuntimeError("service_restart_command_failed")
                return {
                    "ok": True,
                    "status": "restart_requested",
                    "expected_runtime_heads": restart_heads,
                }
            result = {
                "restart_required": False,
                "expected_head": expected_head,
                "runtime_heads": disk_heads_after,
                "guard_before_pull": guard_before,
            }
            await _update_git_pull_operation(operation_id, "completed", result)
            if claims_paused:
                await _resume_blueprints_provider_claims()
                claims_paused = False
            log.info(
                "git pull batch%s completed with no runtime repo changes; restart skipped",
                source_label,
            )
            return {"ok": True, "status": "completed", **result}
        except SchedulerRestartRefused as exc:
            _RESTART_PENDING = False
            if claims_paused:
                await _resume_blueprints_provider_claims()
            await _update_git_pull_operation(
                restart_receipt_id,
                "blocked",
                {**operation_evidence, "reason": exc.code, "detail": exc.detail},
                exc.code,
            )
            log.warning("git pull batch%s restart refused: %s", source_label, exc.code)
            return {"ok": False, "code": exc.code, "detail": exc.detail}
        except Exception as exc:
            _RESTART_PENDING = False
            if claims_paused:
                await _resume_blueprints_provider_claims()
            code = str(exc).split(":", 1)[0] if str(exc) else type(exc).__name__
            await _update_git_pull_operation(
                restart_receipt_id,
                "failed",
                {
                    **operation_evidence,
                    "reason": code,
                    "error_class": type(exc).__name__,
                },
                code,
            )
            log.exception("git pull batch%s failed", source_label)
            return {"ok": False, "code": code}


def _restart_command_parts() -> list[str]:
    parts = shlex.split(cfg.SERVICE_RESTART_CMD)
    if (
        len(parts) >= 3
        and parts[0] == "systemctl"
        and parts[1] == "restart"
        and "--no-block" not in parts
    ):
        unit = f"blueprints-app-self-restart-{os.getpid()}-{int(time.time() * 1000)}"
        return [
            "systemd-run",
            "--unit",
            unit,
            "--collect",
            "/bin/systemctl",
            "restart",
            *parts[2:],
        ]
    return parts


async def _restart_service(
    *,
    operation_id: str | None = None,
    expected_runtime_heads: dict[str, str] | None = None,
    guard_before: dict[str, Any] | None = None,
    claims_already_paused: bool = False,
) -> bool:
    """Run SERVICE_RESTART_CMD, logging the outcome.

    Sleep briefly first so the HTTP response that triggered this call
    has time to be flushed through Caddy before the process is killed.
    """
    subscriber_count = events_bus.subscriber_count
    if subscriber_count:
        log.info("closing %d SSE subscriber(s) before restart", subscriber_count)
        await events_bus.close_all()
    await asyncio.sleep(1)
    providers = (
        list((guard_before or {}).get("providers") or [])
        if claims_already_paused
        else await _pause_blueprints_provider_claims()
    )
    dispatched_successfully = False
    try:
        scheduler_immediately_before = await _scheduler_quiescence_snapshot(providers)
        parts = _restart_command_parts()
        if not parts:
            return False
        await _update_git_pull_operation(
            operation_id,
            "restart_requested",
            {
                "expected_runtime_heads": expected_runtime_heads or {},
                "guard_before_pull": guard_before or {},
                "guard_immediately_before_restart": {
                    "providers": providers,
                    "scheduler": scheduler_immediately_before,
                },
                "initiating_node_id": cfg.NODE_ID,
                "initiating_pid": os.getpid(),
                "process_started_at": _PROCESS_STARTED_AT,
            },
        )
        log.info("service restart requested: %s", " ".join(parts))
        proc = await asyncio.create_subprocess_exec(
            *parts,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode == 0:
            dispatched_successfully = True
            log.info("service restart: %s", stdout.decode().strip() or "ok")
            return True
        log.warning("service restart failed: %s", stderr.decode().strip())
        return False
    finally:
        if not dispatched_successfully:
            await _resume_blueprints_provider_claims()


# ── Internal helper: apply one sync action ────────────────────────────────────


def _parse_sync_updated_at(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        pass

    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _should_skip_stale_kanban_item_upsert(conn, action) -> bool:
    if action.table_name != "kanban_items" or action.action_type not in ("INSERT", "UPDATE"):
        return False
    data = action.row_data or {}
    incoming_updated_at = _parse_sync_updated_at(data.get("updated_at"))
    if incoming_updated_at is None:
        return False

    item_id = data.get("item_id") or action.row_id
    if not item_id:
        return False

    row = conn.execute(
        "SELECT updated_at FROM kanban_items WHERE item_id=?",
        (item_id,),
    ).fetchone()
    if row is None:
        return False

    local_updated_at = _parse_sync_updated_at(row["updated_at"])
    if local_updated_at is None:
        return False

    if incoming_updated_at < local_updated_at:
        log.warning(
            "stale kanban sync: skipping %s for %s (incoming updated_at=%r < local updated_at=%r)",
            action.action_type,
            item_id,
            data.get("updated_at"),
            row["updated_at"],
        )
        return True
    return False


def _sync_normalise_filter_id(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _sync_tags_json_contains(tags_json: object, tag_id: str) -> bool:
    clean = _sync_normalise_filter_id(tag_id)
    if not clean:
        return False
    try:
        tags = json.loads(str(tags_json or "[]"))
    except (TypeError, ValueError):
        return False
    if not isinstance(tags, list):
        return False
    return any(_sync_normalise_filter_id(tag) == clean for tag in tags)


def _personal_filter_tag_assignment_count(conn, tag_id: str) -> int:
    count = 0
    for table_name in ("personal_events", "personal_time_tasks", "kanban_items"):
        try:
            rows = conn.execute(f"SELECT tags_json FROM {table_name}").fetchall()
        except sqlite3.OperationalError:
            continue
        for row in rows:
            if _sync_tags_json_contains(row["tags_json"], tag_id):
                count += 1
    return count


def _personal_filter_meta_tag_assignment_count(conn, meta_tag_id: str) -> int:
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS count FROM personal_filter_tags WHERE meta_tag_id=?",
            (_sync_normalise_filter_id(meta_tag_id),),
        ).fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row["count"] if row else 0)


def _should_skip_personal_filter_delete(conn, action) -> bool:
    if action.action_type != "DELETE":
        return False
    if action.table_name == "personal_filter_tags":
        usage_count = _personal_filter_tag_assignment_count(conn, action.row_id)
        if usage_count:
            log.warning(
                "skipping incoming delete for assigned personal filter tag %s (usage_count=%d)",
                action.row_id,
                usage_count,
            )
            return True
    if action.table_name == "personal_filter_meta_tags":
        assignment_count = _personal_filter_meta_tag_assignment_count(conn, action.row_id)
        if assignment_count:
            log.warning(
                "skipping incoming delete for assigned personal filter meta tag %s "
                "(filter_tag_count=%d)",
                action.row_id,
                assignment_count,
            )
            return True
    return False


def _full_restore_allowed(*, force_restore: bool, integrity_ok: bool) -> bool:
    """Full DB replacement is recovery-only unless explicitly forced."""
    return bool(force_restore or not integrity_ok)


def _apply_action(conn, action) -> None:
    """
    Replay a single peer action against the local DB.
    Does NOT enqueue back to peers (sync writes don't re-propagate).
    """
    table = action.table_name
    if table not in _ALLOWED_TABLES:
        log.warning("ignoring sync action for unknown table '%s'", table)
        return

    if action.action_type == "DELETE":
        if _should_skip_personal_filter_delete(conn, action):
            return
        pk_col = _pk_for_table(table)
        conn.execute(f"DELETE FROM {table} WHERE {pk_col}=?", (action.row_id,))

    elif action.action_type in ("INSERT", "UPDATE"):
        if not action.row_data:
            log.warning("sync action %s/%s has no row_data — skipping", table, action.row_id)
            return
        data = action.row_data
        cols = list(data.keys())
        placeholders = ", ".join("?" * len(cols))
        update_clause = ", ".join(f"{c}=excluded.{c}" for c in cols if c != _pk_for_table(table))
        values = [data[c] for c in cols]
        conn.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT({_pk_for_table(table)}) DO UPDATE SET {update_clause}",
            values,
        )
    else:
        log.warning("unknown action_type '%s' — skipping", action.action_type)


def _pk_for_table(table: str) -> str:
    pk_map = {
        "services": "service_id",
        "machines": "machine_id",
        "nodes": "node_id",
        "pfsense_dns": "dns_entry_id",
        "proxmox_config": "config_id",
        "proxmox_nets": "net_id",
        "vlans": "vlan_id",
        "dockge_stacks": "stack_id",
        "dockge_stack_services": "service_id",
        "caddy_configs": "caddy_id",
        "settings": "key",
        "pve_hosts": "pve_id",
        "arp_manual": "entry_id",
        "ssh_targets": "ip_address",
        "manual_links": "link_id",
        "manual_link_categories": "category_id",
        "manual_link_category_items": "mapping_id",
        "docs": "doc_id",
        "doc_groups": "group_id",
        "doc_images": "image_id",
        "ai_providers": "provider_id",
        "ai_project_assignments": "assignment_id",
        "bookmarks": "bookmark_id",
        "visits": "visit_id",
        "bookmark_deletions": "bookmark_id",
        "visit_events": "event_id",
        "nav_items": "item_id",
        "form_controls": "control_id",
        "embed_menu_items": "item_id",
        "table_layout_catalog": "table_code",
        "table_layouts": "layout_key",
        "pockettts_tags": "tag_id",
        "pockettts_voices": "voice_id",
        "pockettts_voice_meta": "voice_id",
        "pockettts_voice_tags": "assignment_id",
        "pockettts_tag_order": "order_id",
        "disks_filesystem_favorites": "favorite_id",
        "personal_events": "event_id",
        "personal_time_tasks": "task_id",
        "personal_sources": "source_id",
        "personal_filter_tags": "tag_id",
        "personal_filter_meta_tags": "meta_tag_id",
        "personal_import_batches": "import_batch_id",
        "personal_git_repositories": "repo_full_name",
        "personal_git_commits": "commit_id",
        "personal_git_features": "feature_id",
        "personal_git_kanban_arcs": "arc_id",
        "personal_git_daily_summaries": "summary_id",
        "personal_git_import_runs": "run_id",
        "personal_time_audit": "audit_id",
        "personal_graph_links": "link_id",
        "kanban_item_states": "state_id",
        "kanban_item_priorities": "priority_id",
        "kanban_items": "item_id",
        "kanban_item_order_edges": "edge_id",
        "kanban_priority_recommendations": "recommendation_id",
        "kanban_item_links": "link_id",
        "kanban_item_commits": "commit_link_id",
        "kanban_review_decisions": "decision_id",
        "kanban_review_processor_leases": "lease_id",
        "kanban_review_processor_markers": "marker_id",
        "kanban_review_processor_failure_events": "failure_event_id",
        "kanban_agent_hints": "hint_id",
        "kanban_agent_sessions": "session_id",
        "kanban_blockers": "blocker_id",
        "kanban_discussions": "discussion_id",
        "kanban_audit_log": "audit_id",
    }
    return pk_map.get(table, "id")


# ── Forwarding helpers (Phase 2 — GUID-deduplicated relay) ───────────────────


def _can_reach_directly(source: dict, target: dict) -> bool:
    """Return True if source node can reach target via any direct network path.

    Two criteria (either is sufficient):
    1. LAN: both nodes have a primary_ip → they share the on-premises network.
    2. Tailnet: both nodes carry the same non-empty tailnet string.
    """
    if source.get("primary_ip") and target.get("primary_ip"):
        return True
    if source.get("tailnet") and target.get("tailnet") and source["tailnet"] == target["tailnet"]:
        return True
    return False


def _forward_actions(
    conn: sqlite3.Connection,
    source_node_id: str,
    actions: list,
) -> None:
    """Re-enqueue newly applied actions for peers the originator cannot reach.

    Only fires when there is at least one peer that:
      • the source node CANNOT reach directly, AND
      • this node CAN reach directly.

    Each forwarded copy carries the original GUID so the receiving node's
    dedup check prevents double-application regardless of how many relay
    nodes attempt the forward.  System actions are never forwarded.
    """
    if not actions:
        return
    source_node = cfg.NODE_MAP.get(source_node_id)
    if source_node is None:
        return  # Unknown originator — no forwarding

    relay_peers = [
        n
        for n in cfg.PEER_NODES
        if not _can_reach_directly(source_node, n) and _can_reach_directly(cfg.SELF_NODE, n)
    ]
    if not relay_peers:
        return

    for peer in relay_peers:
        for action in actions:
            try:
                enqueue(
                    conn,
                    peer["node_id"],
                    action.action_type,
                    action.table_name,
                    action.row_id,
                    action.row_data,
                    action.gen,
                    guid=action.guid,
                )
            except Exception:
                log.exception(
                    "failed to forward action %s/%s to relay peer %s",
                    action.table_name,
                    action.row_id,
                    peer["node_id"],
                )
    log.debug(
        "forwarded %d action(s) from %s to relay peer(s) %s",
        len(actions),
        source_node_id,
        [p["node_id"] for p in relay_peers],
    )


def _get_receive_actions_apply_lock() -> asyncio.Lock:
    global _receive_actions_apply_lock
    if _receive_actions_apply_lock is None:
        _receive_actions_apply_lock = asyncio.Lock()
    return _receive_actions_apply_lock


def _copy_sync_status(status: SyncStatus) -> SyncStatus:
    return status.model_copy(deep=True)


def _invalidate_sync_status_cache() -> None:
    global _sync_status_cache_payload, _sync_status_cache_expires_monotonic
    _sync_status_cache_payload = None
    _sync_status_cache_expires_monotonic = 0.0


def _sync_status_sync() -> SyncStatus:
    """Build sync status through one bounded read-only SQLite connection."""
    with get_read_conn(
        busy_timeout_ms=_SYNC_STATUS_SQLITE_BUSY_TIMEOUT_MS,
        operation="sync_status",
    ) as conn:
        with timing.span("sync.status.read_meta"):
            gen = get_gen(conn)
            integrity_ok = get_meta(conn, "integrity_ok") == "true"
            last_write_at = get_meta(conn, "last_write_at")
            last_write_by = get_meta(conn, "last_write_by")
            peer_rows = conn.execute(
                "SELECT node_id FROM nodes WHERE node_id != ?", (cfg.NODE_ID,)
            ).fetchall()
            peer_ids = [r["node_id"] for r in peer_rows]

        with timing.span("sync.status.queue_depths", peer_count=len(peer_ids)):
            queue_depths = {peer_id: 0 for peer_id in peer_ids}
            if peer_ids:
                placeholders = ", ".join("?" for _ in peer_ids)
                rows = conn.execute(
                    f"""
                    SELECT target_node_id, COUNT(*) AS pending_count
                    FROM sync_queue
                    WHERE sent=0 AND target_node_id IN ({placeholders})
                    GROUP BY target_node_id
                    """,
                    peer_ids,
                ).fetchall()
                for row in rows:
                    queue_depths[str(row["target_node_id"])] = int(row["pending_count"] or 0)

    return SyncStatus(
        node_id=cfg.NODE_ID,
        node_name=cfg.NODE_NAME,
        gen=gen,
        integrity_ok=integrity_ok,
        last_write_at=last_write_at,
        last_write_by=last_write_by,
        queue_depths=queue_depths,
        peer_count=len(peer_ids),
    )


async def _sync_status_coalesced() -> SyncStatus:
    """Coalesce bursty GUI status reads while bounding freshness to 100ms."""
    global _sync_status_cache_payload, _sync_status_cache_expires_monotonic
    global _sync_status_inflight_task

    now = time.monotonic()
    if _sync_status_cache_payload is not None and now < _sync_status_cache_expires_monotonic:
        with timing.span(
            "sync.status.cache",
            result="hit",
            ttl_ms=round(_SYNC_STATUS_CACHE_TTL_SECONDS * 1000),
        ):
            return _copy_sync_status(_sync_status_cache_payload)

    async with _sync_status_cache_lock:
        now = time.monotonic()
        if _sync_status_cache_payload is not None and now < _sync_status_cache_expires_monotonic:
            with timing.span(
                "sync.status.cache",
                result="hit_after_lock",
                ttl_ms=round(_SYNC_STATUS_CACHE_TTL_SECONDS * 1000),
            ):
                return _copy_sync_status(_sync_status_cache_payload)
        if _sync_status_inflight_task is None or _sync_status_inflight_task.done():
            _sync_status_inflight_task = asyncio.create_task(
                timing.to_thread("sync.status", _sync_status_sync)
            )
            cache_result = "miss"
        else:
            cache_result = "coalesced_inflight"
        task = _sync_status_inflight_task

    with timing.span(
        "sync.status.cache",
        result=cache_result,
        ttl_ms=round(_SYNC_STATUS_CACHE_TTL_SECONDS * 1000),
    ):
        status = await task

    async with _sync_status_cache_lock:
        if _sync_status_inflight_task is task:
            _sync_status_inflight_task = None
        _sync_status_cache_payload = _copy_sync_status(status)
        _sync_status_cache_expires_monotonic = time.monotonic() + _SYNC_STATUS_CACHE_TTL_SECONDS

    return _copy_sync_status(status)


def _receive_db_actions_sync(payload: SyncActionsPayload, db_actions: list) -> int:
    with get_conn() as conn:
        # Check own integrity — if not OK, refuse to accept (corrupt state)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        if not integrity_ok:
            raise HTTPException(
                503,
                "node integrity check failed — not accepting sync actions; "
                "request a full restore instead",
            )

        newly_applied: list = []
        failures: list[str] = []
        for action in db_actions:
            # ── GUID dedup: drop actions already processed on this node ───────
            # Empty GUID = legacy peer (pre-Phase-2); skip dedup, apply normally.
            if action.guid:
                try:
                    conn.execute(
                        "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                        (action.guid, int(time.time())),
                    )
                except sqlite3.IntegrityError:
                    log.debug(
                        "GUID %s already seen — skipping duplicate from %s",
                        action.guid[:8],
                        payload.source_node_id,
                    )
                    continue

            try:
                if kanban_table_fleet_sync_disabled(action.table_name):
                    log.debug(
                        "skipping incoming Kanban SQLite mirror sync action for %s/%s "
                        "while active_store=postgres",
                        action.table_name,
                        action.row_id,
                    )
                    continue
                if _should_skip_stale_kanban_item_upsert(conn, action):
                    continue
                _apply_action(conn, action)
                # Write source as "sync" so the gen counter tracks sync writes separately
                _ = increment_gen(conn, "sync")
                newly_applied.append(action)
            except Exception:
                log.exception(
                    "failed to apply sync action %s/%s from %s — skipping",
                    action.table_name,
                    action.row_id,
                    payload.source_node_id,
                )
                failures.append(f"{action.table_name}/{action.row_id}")

        if failures:
            log.warning(
                "skipped %d action(s) from %s due to errors: %s",
                len(failures),
                payload.source_node_id,
                ", ".join(failures),
            )

        # Update last_seen for the source node
        conn.execute(
            "UPDATE nodes SET last_seen=datetime('now') WHERE node_id=?",
            (payload.source_node_id,),
        )

        # Smart forwarding — relay newly applied actions to any peers the
        # originator cannot reach but we can (no-op for current 6-node fleet).
        _forward_actions(conn, payload.source_node_id, newly_applied)

    return len(newly_applied)


# ── Routes ───────────────────────────────────────────────────────────────────


@router.post("/actions", status_code=204)
async def receive_actions(payload: SyncActionsPayload) -> Response:
    """
    Receive a batch of CRUD actions from a peer node and apply them locally.

    Commit guard: if the source node's commit timestamp is older than ours,
    DB-write actions are rejected (409) to prevent stale data overwriting
    newer-schema rows. System actions (git-pull) are always accepted so the
    stale node can be told to pull newer code.
    """
    if not payload.actions:
        return Response(status_code=204)

    # Separate system actions (fire-and-forget) from DB-write actions
    system_actions = [a for a in payload.actions if a.action_type in _SYSTEM_ACTION_TYPES]
    db_actions = [a for a in payload.actions if a.action_type not in _SYSTEM_ACTION_TYPES]

    # Schedule system actions as one ordered batch — always accepted regardless
    # of commit age so a newer node can tell an older one to git-pull.
    system_scopes = _ordered_git_scopes(
        _scope_for_system_action(action.action_type) for action in system_actions
    )
    if system_scopes:
        asyncio.create_task(
            _git_pull_scopes_and_maybe_restart(
                system_scopes,
                source=f"from {payload.source_node_id}",
            )
        )
        log.info(
            "scheduled git pull scopes %s from %s", ",".join(system_scopes), payload.source_node_id
        )

    if not db_actions:
        return Response(status_code=204)

    # ── Commit guard: reject DB writes from older-commit peers ────────────
    if cfg.COMMIT_TS and payload.source_commit_ts:
        if payload.source_commit_ts < cfg.COMMIT_TS:
            log.warning(
                "commit guard: rejecting %d DB actions from %s (source_ts=%d < local_ts=%d)",
                len(db_actions),
                payload.source_node_id,
                payload.source_commit_ts,
                cfg.COMMIT_TS,
            )
            raise HTTPException(
                409,
                f"commit guard: source commit ({payload.source_commit_ts}) "
                f"is older than local ({cfg.COMMIT_TS}) — "
                "pull newer code before syncing data",
            )

    async with _get_receive_actions_apply_lock():
        applied_count = await asyncio.to_thread(_receive_db_actions_sync, payload, db_actions)

    if applied_count:
        _invalidate_sync_status_cache()

    log.info(
        "applied %d sync actions from %s",
        applied_count,
        payload.source_node_id,
    )
    return Response(status_code=204)


def _enqueue_git_pull_scopes_sync(scopes: list[str]) -> None:
    with get_conn() as conn:
        gen = get_gen(conn)
        for scope in scopes:
            action_type = f"sync_git_{scope}"
            enqueue_for_all_peers(conn, action_type, "_system", scope, None, gen)


async def _reconcile_git_pull_operation(
    receipt: dict[str, Any], *, require_new_process_proof: bool = False
) -> dict[str, Any]:
    if receipt.get("status") not in {"restart_requested", "post_restart_proof_failed"}:
        return receipt
    result = receipt.get("result") if isinstance(receipt.get("result"), dict) else {}
    expected = result.get("expected_runtime_heads")
    if not isinstance(expected, dict) or not expected:
        if require_new_process_proof:
            raise SchedulerRestartRefused(
                "restart_receipt_malformed", {"field": "expected_runtime_heads"}
            )
        return receipt
    if result.get("initiating_node_id") != cfg.NODE_ID:
        if require_new_process_proof:
            raise SchedulerRestartRefused(
                "restart_receipt_wrong_node",
                {
                    "expected_node_id": cfg.NODE_ID,
                    "initiating_node_id": result.get("initiating_node_id"),
                },
            )
        return receipt
    process_started = _parse_sync_updated_at(_PROCESS_STARTED_AT)
    initiating_process_started = _parse_sync_updated_at(result.get("process_started_at"))
    if (
        process_started is None
        or initiating_process_started is None
        or process_started <= initiating_process_started
    ):
        if require_new_process_proof:
            raise SchedulerRestartRefused(
                "restart_process_not_advanced",
                {
                    "current_process_started_at": _PROCESS_STARTED_AT,
                    "initiating_process_started_at": result.get("process_started_at"),
                },
            )
        return receipt
    if expected != _RUNNING_RUNTIME_REPO_HEADS:
        if require_new_process_proof:
            raise SchedulerRestartRefused(
                "restart_runtime_heads_mismatch",
                {
                    "expected_runtime_heads": expected,
                    "running_runtime_heads": dict(_RUNNING_RUNTIME_REPO_HEADS),
                },
            )
        return receipt
    # The pre-restart receipt already proved that every locally effective
    # provider had no queued work and that no Blueprints provider was running.
    # A new scheduled occurrence may be queued while the app is down; it must
    # not deadlock startup before the new provider loops can consume it.
    scheduler_immediately_after = await _scheduler_quiescence_snapshot([])
    completed = {
        **result,
        "completed_node_id": cfg.NODE_ID,
        "completed_process_started_at": _PROCESS_STARTED_AT,
        "running_runtime_heads": dict(_RUNNING_RUNTIME_REPO_HEADS),
        "guard_immediately_after_restart": {
            "scheduler": scheduler_immediately_after,
        },
        "restart_observed": True,
    }
    updated = await _update_git_pull_operation(str(receipt["operation_id"]), "completed", completed)
    assert updated is not None
    return updated


async def reconcile_pending_restart_operations() -> list[dict[str, Any]]:
    """Capture immediate after-restart scheduler proof before providers start."""
    receipts = await timing.to_thread(
        "sync.restart_requested_operations",
        _restart_requested_operations_sync,
    )
    reconciled: list[dict[str, Any]] = []
    for receipt in receipts:
        try:
            reconciled.append(
                await _reconcile_git_pull_operation(receipt, require_new_process_proof=True)
            )
        except Exception as exc:
            result = receipt.get("result") if isinstance(receipt.get("result"), dict) else {}
            proof_code = (
                exc.code
                if isinstance(exc, SchedulerRestartRefused)
                else "scheduler_status_unavailable"
            )
            permanent_receipt_failure = proof_code in {
                "restart_receipt_malformed",
                "restart_receipt_wrong_node",
                "restart_process_not_advanced",
                "restart_runtime_heads_mismatch",
            }
            failure = {
                **result,
                "completed_node_id": cfg.NODE_ID,
                "completed_process_started_at": _PROCESS_STARTED_AT,
                "restart_observed": True,
                "post_restart_proof_error": {
                    "code": proof_code,
                    "error_class": type(exc).__name__,
                },
            }
            updated = await _update_git_pull_operation(
                str(receipt["operation_id"]),
                "failed" if permanent_receipt_failure else "post_restart_proof_failed",
                failure,
                proof_code if permanent_receipt_failure else "post_restart_scheduler_proof_failed",
            )
            if updated is not None:
                reconciled.append(updated)
            log.exception(
                "post-restart scheduler proof failed for operation %s",
                receipt["operation_id"],
            )
    failures = [item for item in reconciled if item.get("status") != "completed"]
    if failures:
        raise RuntimeError(
            "post_restart_scheduler_proof_failed:"
            + ",".join(str(item.get("operation_id")) for item in failures)
        )
    return reconciled


async def _run_tracked_git_pull_operation(
    operation_id: str,
    scopes: list[str],
    expected_head: str,
) -> None:
    try:
        await _git_pull_scopes_and_maybe_restart(
            scopes,
            source=f"local operation {operation_id}",
            operation_id=operation_id,
            expected_head=expected_head,
        )
    finally:
        _ACTIVE_GIT_PULL_OPERATION_IDS.discard(operation_id)


async def _run_guarded_restart(
    *, operation_id: str | None = None, expected_runtime_heads: dict[str, str] | None = None
) -> None:
    global _RESTART_PENDING
    try:
        async with _GIT_PULL_LOCK:
            if _RESTART_PENDING:
                await _update_git_pull_operation(
                    operation_id,
                    "blocked",
                    {"reason": "restart_already_pending"},
                    "restart_already_pending",
                )
                log.warning("service restart refused: restart_already_pending")
                return
            _RESTART_PENDING = True
            restarted = await _restart_service(
                operation_id=operation_id,
                expected_runtime_heads=expected_runtime_heads,
            )
            if not restarted:
                _RESTART_PENDING = False
                await _update_git_pull_operation(
                    operation_id,
                    "failed",
                    {"reason": "restart_dispatch_failed"},
                    "restart_dispatch_failed",
                )
    except SchedulerRestartRefused as exc:
        _RESTART_PENDING = False
        await _update_git_pull_operation(
            operation_id,
            "blocked",
            {"reason": exc.code, "detail": exc.detail},
            exc.code,
        )
        log.warning("service restart refused: %s detail=%s", exc.code, exc.detail)
    except Exception as exc:
        _RESTART_PENDING = False
        await _update_git_pull_operation(
            operation_id,
            "failed",
            {"reason": "restart_exception", "error_class": type(exc).__name__},
            "restart_exception",
        )
        log.exception("service restart failed before command completion")


@router.post("/git-pull")
async def trigger_git_pull(payload: GitPullRequest, request: Request) -> Response:
    """
    Trigger a git pull on this node and enqueue sync_git actions for all peers.
    scope: "outer" | "inner" | "both" | "non_root" | "all"
    Any peer that receives the action will pull its own local repo.
    """
    if payload.scope not in ("outer", "inner", "both", "non_root", "all"):
        raise HTTPException(400, "scope must be 'outer', 'inner', 'both', 'non_root', or 'all'")

    if payload.scope == "both":
        scopes = ["outer", "inner"]
    elif payload.scope == "all":
        scopes = ["outer", "non_root", "inner"]
    else:
        scopes = [payload.scope]

    if payload.local_only:
        if not _raw_client_is_loopback(request):
            raise HTTPException(403, "local_only git pull requires a raw loopback connection")
        if payload.scope != "outer":
            raise HTTPException(400, "local_only git pull currently supports scope='outer' only")
        if payload.operation_id is None or payload.expected_head is None:
            raise HTTPException(
                400,
                "local_only git pull requires operation_id and expected_head",
            )
        try:
            receipt = await timing.to_thread(
                "sync.git_pull_operation_create",
                _git_pull_operation_create_sync,
                payload.operation_id,
                scopes,
                payload.expected_head,
            )
        except GitPullOperationConflict as exc:
            raise HTTPException(409, str(exc)) from None
        resumable = receipt.get("status") in {"queued", "pulling"}
        if resumable and payload.operation_id not in _ACTIVE_GIT_PULL_OPERATION_IDS:
            _ACTIVE_GIT_PULL_OPERATION_IDS.add(payload.operation_id)
            asyncio.create_task(
                _run_tracked_git_pull_operation(
                    payload.operation_id,
                    scopes,
                    payload.expected_head,
                )
            )
        return JSONResponse(status_code=202, content=receipt)

    if payload.operation_id is not None or payload.expected_head is not None:
        raise HTTPException(400, "operation_id and expected_head require local_only=true")

    await timing.to_thread("sync.git_pull_broadcast_enqueue", _enqueue_git_pull_scopes_sync, scopes)

    _invalidate_sync_status_cache()
    asyncio.create_task(_git_pull_scopes_and_maybe_restart(scopes, source="local trigger"))
    log.info("triggered git pull scopes %s locally + queued for all peers", ",".join(scopes))

    return Response(status_code=204)


@router.get("/git-pull/{operation_id}")
async def git_pull_operation(operation_id: str) -> dict[str, Any]:
    if _GIT_PULL_OPERATION_ID.fullmatch(operation_id) is None:
        raise HTTPException(404, "git-pull operation not found")
    receipt = await timing.to_thread(
        "sync.git_pull_operation_get",
        _git_pull_operation_get_sync,
        operation_id,
    )
    if receipt is None:
        raise HTTPException(404, "git-pull operation not found")
    return await _reconcile_git_pull_operation(receipt)


@router.get("/git-pull-capabilities")
async def git_pull_capabilities() -> dict[str, Any]:
    """Advertise the fail-closed node-local deployment protocol without mutation."""
    return {
        "schema": "xarta.blueprints.git_pull_capabilities.v1",
        "node_id": cfg.NODE_ID,
        "local_only": True,
        "broadcast": False,
        "operation_receipt_schema": "xarta.blueprints.git_pull_operation.v1",
        "supported_scopes": ["outer"],
        "restart_guard": "atomic-provider-scoped-snapshot-v5",
        "exact_expected_head": True,
    }


@router.post("/restart", status_code=204)
async def trigger_restart() -> Response:
    """Restart the blueprints-app service on this node (via SERVICE_RESTART_CMD)."""
    if not cfg.SERVICE_RESTART_CMD:
        raise HTTPException(503, "SERVICE_RESTART_CMD not configured on this node")
    operation_id = f"git-pull-{uuid.uuid4().hex}"
    await timing.to_thread(
        "sync.restart_operation_create",
        _restart_operation_create_sync,
        operation_id,
    )
    asyncio.create_task(
        _run_guarded_restart(
            operation_id=operation_id,
            expected_runtime_heads=dict(_RUNNING_RUNTIME_REPO_HEADS),
        )
    )
    log.info("service restart triggered via API operation_id=%s", operation_id)
    return Response(status_code=204)


@router.post("/retouch/{table_name}")
async def retouch_table(table_name: str):
    """
    Re-enqueue all current rows of a table for sync to all peers.

    Safe to call at any time — the receive side uses INSERT ... ON CONFLICT DO UPDATE
    so rows are upserted, never duplicated. Useful for recovering from a commit-guard
    purge or after a new node joins the fleet.
    """
    if table_name not in _ALLOWED_TABLES:
        raise HTTPException(400, f"Table '{table_name}' is not in the syncable table list")
    pk_col = _pk_for_table(table_name)
    with get_conn() as conn:
        rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        if not rows:
            return {"requeued": 0, "table": table_name}
        gen = increment_gen(conn, "human")
        for row in rows:
            row_dict = dict(row)
            row_id = str(row_dict[pk_col])
            enqueue_for_all_peers(conn, "UPDATE", table_name, row_id, row_dict, gen)
    _invalidate_sync_status_cache()
    log.info("retouch: re-queued %d rows from %s", len(rows), table_name)
    return {"requeued": len(rows), "table": table_name}


def _table_content_hash(table_name: str) -> dict:
    """Return row_count + SHA-256 digest of sorted serialised row data for a table."""
    if table_name not in _ALLOWED_TABLES:
        raise HTTPException(400, f"Table '{table_name}' is not in the syncable table list")
    pk_col = _pk_for_table(table_name)
    with get_conn() as conn:
        rows = conn.execute(f"SELECT * FROM {table_name} ORDER BY {pk_col}").fetchall()
    row_count = len(rows)
    h = hashlib.sha256()
    for row in rows:
        h.update(json.dumps(dict(row), sort_keys=True, default=str).encode())
    return {"table": table_name, "row_count": row_count, "checksum": h.hexdigest()}


@router.get("/table-hash/{table_name}")
async def table_hash(table_name: str) -> dict:
    """
    Return the row count and SHA-256 digest of all row data in a syncable table.

    Used by the Retouch All parity check to determine whether a table on this
    node matches the same table on peer nodes.  Safe, read-only, no side effects.
    """
    return _table_content_hash(table_name)


@router.get("/parity/{table_name}")
async def table_parity(table_name: str) -> dict:
    """
    Compare this node's table content against all peer nodes.

    Returns:
      - local: {row_count, checksum}
      - peers: [{node_id, row_count, checksum, match, error}]
      - needs_retouch: True if any peer hash differs or returned an error
    """
    local = _table_content_hash(table_name)

    peer_results = []
    needs_retouch = False

    async def _fetch_peer_hash(node_id: str, urls: list[str], client: httpx.AsyncClient) -> dict:
        for url in urls:
            try:
                r = await client.get(
                    f"{url}/api/v1/sync/table-hash/{table_name}",
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)}
                    if cfg.SYNC_SECRET
                    else {},
                )
                if r.status_code == 200:
                    data = r.json()
                    match = data.get("checksum") == local["checksum"]
                    return {
                        "node_id": node_id,
                        "row_count": data.get("row_count"),
                        "checksum": data.get("checksum"),
                        "match": match,
                        "error": None,
                    }
                return {
                    "node_id": node_id,
                    "row_count": None,
                    "checksum": None,
                    "match": False,
                    "error": f"HTTP {r.status_code}",
                }
            except httpx.ConnectError:
                continue
            except httpx.TimeoutException:
                return {
                    "node_id": node_id,
                    "row_count": None,
                    "checksum": None,
                    "match": False,
                    "error": "timeout",
                }
            except Exception as e:
                return {
                    "node_id": node_id,
                    "row_count": None,
                    "checksum": None,
                    "match": False,
                    "error": str(e),
                }
        return {
            "node_id": node_id,
            "row_count": None,
            "checksum": None,
            "match": False,
            "error": "all addresses unreachable",
        }

    if cfg._peer_nodes:
        async with _probe_client(timeout=10.0) as client:
            tasks = [
                _fetch_peer_hash(n["node_id"], cfg.PEER_SYNC_URLS.get(n["node_id"], []), client)
                for n in cfg._peer_nodes
            ]
            peer_results = list(await asyncio.gather(*tasks))
        needs_retouch = any(not r["match"] for r in peer_results)

    return {
        "table": table_name,
        "local": {"row_count": local["row_count"], "checksum": local["checksum"]},
        "peers": peer_results,
        "needs_retouch": needs_retouch,
    }


@router.post("/restore", status_code=204)
async def receive_restore(request: Request) -> Response:
    """
    Layer 1 restore endpoint.
    Accepts a multipart or raw body with:
      - the zipped DB backup (bytes)
      - SHA-256 hex in X-Blueprints-Checksum header

    Verifies the checksum then atomically replaces the local DB only for
    degraded-node recovery or explicit force restore.
    """
    sha256_hex = request.headers.get("x-blueprints-checksum", "")
    if not sha256_hex:
        raise HTTPException(400, "missing X-Blueprints-Checksum header")

    force_restore = request.headers.get(_FORCE_RESTORE_HEADER, "").lower() == "true"
    restore_op = request.headers.get(_RESTORE_OP_HEADER, "")

    zip_bytes = await request.body()
    if not zip_bytes:
        raise HTTPException(400, "empty restore payload")

    with get_conn() as conn:
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        my_gen = get_gen(conn)

    if not _full_restore_allowed(force_restore=force_restore, integrity_ok=integrity_ok):
        sender_gen = request.headers.get("x-blueprints-gen", "")
        log.warning(
            "receive_restore: rejecting unforced full DB restore on healthy node "
            "(sender_gen=%s, my_gen=%d)",
            sender_gen or "missing",
            my_gen,
        )
        raise HTTPException(
            409,
            "full DB restore is recovery-only on healthy nodes; use explicit force restore "
            "for operator-authorized fleet replacement",
        )

    ok = await apply_restore(zip_bytes, sha256_hex)
    if not ok:
        raise HTTPException(
            422, "restore failed — checksum mismatch, integrity failure, or corrupt payload"
        )

    if force_restore:
        log.warning(
            "authoritative full DB restore applied (%d bytes)%s",
            len(zip_bytes),
            f" op={restore_op}" if restore_op else "",
        )
    else:
        log.info("full DB restore applied (%d bytes)", len(zip_bytes))
    _invalidate_sync_status_cache()
    return Response(status_code=204)


@router.get("/export")
async def export_backup() -> Response:
    """
    Serve the current DB as a full backup zip so a peer can pull it during
    boot-up catch-up or after a crash.  The SHA-256 checksum is returned in
    the X-Blueprints-Checksum response header, matching the restore endpoint.
    """
    with get_conn() as conn:
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        current_gen = get_gen(conn)
    if not integrity_ok:
        raise HTTPException(
            503,
            "node integrity check failed — cannot export a backup; "
            "this node needs to restore from a healthy peer first",
        )
    try:
        zip_bytes, sha256_hex = make_full_backup()
    except FileNotFoundError:
        raise HTTPException(503, "DB not found — node not fully initialised yet")
    except Exception:
        log.exception("export_backup: failed to create backup zip")
        raise HTTPException(500, "failed to create backup")

    log.info("exporting full backup (%d bytes, gen=%d) to peer", len(zip_bytes), current_gen)
    return Response(
        content=zip_bytes,
        media_type="application/octet-stream",
        headers={
            "X-Blueprints-Checksum": sha256_hex,
            "X-Blueprints-Gen": str(current_gen),
        },
    )


@router.get("/tables")
async def list_sync_tables() -> dict:
    """Return the sorted list of tables that participate in fleet sync."""
    return {"tables": sorted(_ALLOWED_TABLES)}


def _runtime_readiness_result(
    *,
    node_id: str,
    health: dict | None,
    tables: list[str] | None,
    min_commit_ts: int | None,
    required_tables: list[str],
    error: str | None = None,
) -> dict:
    commit_ts = int((health or {}).get("commit_ts") or 0)
    table_set = set(tables or [])
    missing_tables = sorted(t for t in required_tables if t not in table_set)
    commit_ready = min_commit_ts is None or commit_ts >= min_commit_ts
    ready = error is None and commit_ready and not missing_tables
    return {
        "node_id": node_id,
        "ready": ready,
        "commit": (health or {}).get("commit"),
        "commit_ts": commit_ts or None,
        "min_commit_ts": min_commit_ts,
        "commit_ready": commit_ready,
        "missing_tables": missing_tables,
        "error": error,
    }


@router.get("/runtime-readiness")
async def runtime_readiness(
    min_commit_ts: int | None = None,
    required_table: list[str] | None = Query(default=None),
) -> dict:
    """Check whether local and peer runtimes can accept a synced write set.

    This is stricter than "git pull succeeded": it proves the running
    FastAPI process is new enough and exposes every required sync table over
    the same mTLS transport used by the queue drain.
    """
    required_tables = sorted(set(required_table or []))
    local = _runtime_readiness_result(
        node_id=cfg.NODE_ID,
        health={
            "commit": cfg.COMMIT_HASH,
            "commit_ts": cfg.COMMIT_TS,
        },
        tables=sorted(_ALLOWED_TABLES),
        min_commit_ts=min_commit_ts,
        required_tables=required_tables,
    )

    async def _fetch_peer_runtime(node_id: str, urls: list[str], client: httpx.AsyncClient) -> dict:
        last_error = "no sync addresses configured"
        for url in urls:
            try:
                health_resp = await client.get(f"{url}/health")
                if not health_resp.is_success:
                    last_error = f"health HTTP {health_resp.status_code}"
                    continue
                tables_resp = await client.get(
                    f"{url}/api/v1/sync/tables",
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)}
                    if cfg.SYNC_SECRET
                    else {},
                )
                if not tables_resp.is_success:
                    last_error = f"tables HTTP {tables_resp.status_code}"
                    continue
                return _runtime_readiness_result(
                    node_id=node_id,
                    health=health_resp.json(),
                    tables=(tables_resp.json() or {}).get("tables") or [],
                    min_commit_ts=min_commit_ts,
                    required_tables=required_tables,
                )
            except httpx.ConnectError as exc:
                last_error = f"connect error: {exc}"
            except httpx.TimeoutException:
                last_error = "timeout"
            except Exception as exc:
                last_error = str(exc)
        return _runtime_readiness_result(
            node_id=node_id,
            health=None,
            tables=None,
            min_commit_ts=min_commit_ts,
            required_tables=required_tables,
            error=last_error,
        )

    peers = []
    if cfg._peer_nodes:
        async with _probe_client(timeout=10.0) as client:
            tasks = [
                _fetch_peer_runtime(
                    n["node_id"],
                    cfg.PEER_SYNC_URLS.get(n["node_id"], []),
                    client,
                )
                for n in cfg._peer_nodes
            ]
            peers = list(await asyncio.gather(*tasks))

    all_nodes = [local, *peers]
    return {
        "all_ready": all(node["ready"] for node in all_nodes),
        "min_commit_ts": min_commit_ts,
        "required_tables": required_tables,
        "local": local,
        "peers": peers,
    }


@router.get("/status", response_model=SyncStatus)
async def sync_status() -> SyncStatus:
    """Return current sync state: gen, integrity, queue depths per peer."""
    with timing.span("handler", route="sync_status"):
        try:
            return await _sync_status_coalesced()
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "database is locked" in message or "database is busy" in message:
                raise HTTPException(status_code=503, detail="database_locked") from exc
            raise


def _validated_backup_confirmation(filename: str) -> dict:
    if not filename:
        raise HTTPException(
            409,
            "backup_filename is required before applying sent sync_queue pruning",
        )
    if not _SAFE_BACKUP_NAME.match(filename):
        raise HTTPException(400, "Invalid backup filename.")
    if not cfg.BACKUP_DIR:
        raise HTTPException(503, "BLUEPRINTS_BACKUP_DIR is not configured on this node.")
    path = os.path.join(cfg.BACKUP_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(409, f"Confirmed backup was not found: {filename}")
    return {
        "filename": filename,
        "path": path,
        "size_bytes": os.path.getsize(path),
    }


@router.get("/queue-maintenance/status")
async def sync_queue_maintenance_status(
    older_than_hours: int = Query(
        default=24,
        ge=0,
        description="Sent rows older than this are eligible; 0 selects all sent rows.",
    ),
    limit: int = Query(default=0, ge=0, description="Maximum rows to select; 0 means no limit."),
) -> dict:
    """Preview sent sync_queue retention without mutating queue rows."""
    try:
        retention = get_sent_queue_retention_summary(
            older_than_hours=older_than_hours,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {
        "schema": "xarta.sync_queue.maintenance_status.v1",
        "retention": retention,
        "database": sqlite_file_stats(cfg.DB_PATH),
        "backup_required_for_apply": True,
    }


@router.post("/queue-maintenance/prune-sent")
async def prune_sent_sync_queue(
    apply: bool = Query(default=False, description="When false, only preview the delete set."),
    older_than_hours: int = Query(
        default=24,
        ge=0,
        description="Sent rows older than this are eligible; 0 selects all sent rows.",
    ),
    limit: int = Query(default=0, ge=0, description="Maximum rows to select; 0 means no limit."),
    backup_filename: str = Query(
        default="",
        description="Existing /api/v1/backup filename required when apply=true.",
    ),
    require_backup: bool = Query(
        default=True,
        description="Keep true for operator use; tests may disable this for isolated DBs.",
    ),
) -> dict:
    """
    Prune only sent sync_queue rows.

    Unsent rows are never eligible, so incremental drain reliability is
    preserved. Live file shrink still requires a separate SQLite compaction
    step after pruning.
    """
    confirmed_backup = None
    if apply and require_backup:
        confirmed_backup = _validated_backup_confirmation(backup_filename)
    try:
        result = prune_sent_actions(
            older_than_hours=older_than_hours,
            limit=limit,
            apply=apply,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if apply:
        _invalidate_sync_status_cache()
        log.info(
            "sync_queue sent-row prune applied: deleted=%d older_than_hours=%d limit=%d backup=%s",
            result["deleted_rows"],
            older_than_hours,
            limit,
            backup_filename or "",
        )
    return {
        "schema": "xarta.sync_queue.prune_sent_response.v1",
        "backup": confirmed_backup,
        "database": sqlite_file_stats(cfg.DB_PATH),
        **result,
    }


# ── Diagnostic probe endpoints ────────────────────────────────────────────────


def _probe_client(timeout: float) -> httpx.AsyncClient:
    """Return an httpx.AsyncClient configured identically to the sync drain.

    When SYNC_TLS_CA/CERT/KEY are all set the client uses mTLS:
    - server cert verified against the fleet CA
    - this node's client cert+key are presented
    Falls back to plain HTTP if any TLS var is absent.
    """
    if cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cfg.SYNC_TLS_CA)
        ctx.load_cert_chain(cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY)
        return httpx.AsyncClient(timeout=timeout, verify=ctx)
    return httpx.AsyncClient(timeout=timeout)


@router.get("/mtls-probe")
async def mtls_probe() -> dict:
    """
    Probe each fleet peer via mTLS — the same transport the sync drain uses.

    Probes every configured sync address per peer (primary LAN + tailnet
    fallback where applicable), mirroring the multi-address failover order used
    by drain.py.  Each address is probed independently so the GUI can show
    which path is healthy.

    Per-address status values:
      ok:         TLS handshake succeeded, /health returned 2xx
      tls_error:  SSL handshake failed (cert/CA mismatch or missing client cert)
      refused:    TCP connection refused (port closed or firewall drop)
      timeout:    connection or read timed out
      http_error: TLS OK but peer returned non-2xx
      error:      other unexpected failure
    A spurious "ok" is not possible — the remote must accept the client cert
    AND return a 2xx response.
    """
    tls_configured = bool(cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY)
    results = []

    async def _probe_address(node_id: str, url: str, client: httpx.AsyncClient) -> dict:
        health_url = f"{url}/health"
        try:
            r = await client.get(health_url)
            return {
                "node_id": node_id,
                "address": health_url,
                "status": "ok" if r.is_success else "http_error",
                "http_status": r.status_code,
                "error": None,
            }
        except httpx.ConnectError as e:
            err = str(e)
            status = (
                "tls_error"
                if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                else "refused"
            )
            return {
                "node_id": node_id,
                "address": health_url,
                "status": status,
                "http_status": None,
                "error": err,
            }
        except httpx.TimeoutException:
            return {
                "node_id": node_id,
                "address": health_url,
                "status": "timeout",
                "http_status": None,
                "error": "connection timed out",
            }
        except Exception as e:
            return {
                "node_id": node_id,
                "address": health_url,
                "status": "error",
                "http_status": None,
                "error": str(e),
            }

    async with _probe_client(timeout=8.0) as client:
        for n in cfg._peer_nodes:
            node_id = n["node_id"]
            peer_urls = cfg.PEER_SYNC_URLS.get(node_id, [])
            if not peer_urls:
                # Peer has no configured sync addresses (misconfigured node)
                results.append(
                    {
                        "node_id": node_id,
                        "address": None,
                        "status": "error",
                        "http_status": None,
                        "error": "no sync addresses configured",
                    }
                )
                continue
            for url in peer_urls:
                result = await _probe_address(node_id, url, client)
                results.append(result)

    return {"tls_configured": tls_configured, "peers": results}


@router.get("/ssh-probe")
async def ssh_probe() -> dict:
    """
    Probe each fleet peer via SSH using the xarta-node fleet key.

    Runs: ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new
              -o ConnectTimeout=5 -i <key> root@<ip> echo ok

    BatchMode=yes: no password prompts — immediate failure on auth rejection.
    Distinguishes:
      ok:                echo ok received — SSH auth and connectivity confirmed
      auth_failed:       Permission denied / Authentication failed
      host_key_changed:  Remote host identification changed (MITM risk or rebuild)
      refused:           Connection refused
      no_route:          No route to host / Network unreachable
      timeout:           Connection timed out
      no_key:            XARTA_NODE_SSH_KEY unset or file not found
      error:             Other SSH failure
    A spurious "ok" is not possible — remote must respond with "ok" to stdout.
    """
    ssh_key = os.environ.get("XARTA_NODE_SSH_KEY", "")
    if not ssh_key or not os.path.isfile(ssh_key):
        return {
            "ssh_key_present": False,
            "ssh_key_path": ssh_key or None,
            "peers": [],
            "error": "XARTA_NODE_SSH_KEY not set or key file not found",
        }

    async def _probe_peer(n: dict) -> dict:
        ip = n["primary_ip"]
        node_id = n["node_id"]
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh",
                "-n",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-o",
                "ConnectTimeout=5",
                "-i",
                ssh_key,
                f"root@{ip}",
                "echo ok",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=12.0)
            rc = proc.returncode
            out = stdout.decode().strip()
            err = stderr.decode().strip()
            if rc == 0 and out == "ok":
                return {"node_id": node_id, "ip": ip, "status": "ok", "error": None}
            if "REMOTE HOST IDENTIFICATION HAS CHANGED" in err:
                return {"node_id": node_id, "ip": ip, "status": "host_key_changed", "error": err}
            if "Permission denied" in err or "Authentication failed" in err:
                return {"node_id": node_id, "ip": ip, "status": "auth_failed", "error": err}
            if "Connection refused" in err:
                return {"node_id": node_id, "ip": ip, "status": "refused", "error": err}
            if "No route to host" in err or "Network unreachable" in err:
                return {"node_id": node_id, "ip": ip, "status": "no_route", "error": err}
            if "Connection timed out" in err or "Operation timed out" in err:
                return {"node_id": node_id, "ip": ip, "status": "timeout", "error": err}
            return {
                "node_id": node_id,
                "ip": ip,
                "status": "error",
                "error": err or f"exit {rc}: {out}",
            }
        except asyncio.TimeoutError:
            return {
                "node_id": node_id,
                "ip": ip,
                "status": "timeout",
                "error": "SSH timed out (12s)",
            }
        except Exception as e:
            return {"node_id": node_id, "ip": ip, "status": "error", "error": str(e)}

    results = await asyncio.gather(*[_probe_peer(n) for n in cfg._peer_nodes])
    return {
        "ssh_key_present": True,
        "ssh_key_path": ssh_key,
        "peers": list(results),
    }


# Port used as the synthetic "dead" first address in the failover probe.
# Must never be in use on any fleet node.  Port 19999 is in the ephemeral
# range boundary and is not assigned to any standard service.
_FAILOVER_DEAD_PORT = 19999


@router.get("/failover-probe")
async def failover_probe() -> dict:
    """
    Validate the Phase 1 multi-address failover logic without a real remote node.

    For each fleet peer, constructs a synthetic 2-URL list:
      1. Dead URL:  <scheme>://<peer_ip>:<_FAILOVER_DEAD_PORT>  — same IP as
                    the real peer, but a port that is never listening anywhere.
                    TCP connect is refused instantly (ECONNREFUSED) — no timeout
                    wait.  This simulates the drain hitting an unreachable primary
                    address (primary LAN down, or future VPS with no primary_ip).
      2. Real URL:  the peer's actual configured sync URL (from PEER_SYNC_URLS).

    The probe attempts the dead URL, expects ConnectError, then falls through
    to the real URL — exactly as drain.py does.

    A peer PASSES when:
      - dead_status == "refused" (ConnectError on port 19999, as expected)
      - real_status == "ok"       (peer responded normally on real URL)

    This validates the full failover code path (config.py → drain URL list →
    httpx ConnectError catch → next URL) against every live peer, on every
    node that runs the diagnostic.

    Phase 2 hook: when GUID deduplication is implemented, a companion
    /failover-guid-probe endpoint can test that the same GUID arriving twice
    is silently dropped on the second application.
    """
    import time

    async def _probe_one(n: dict, client: httpx.AsyncClient) -> dict:
        node_id = n["node_id"]
        scheme = n.get("sync_scheme", "http")
        peer_ip = n.get("primary_ip") or "127.0.0.1"

        dead_url = f"{scheme}://{peer_ip}:{_FAILOVER_DEAD_PORT}"
        real_urls = cfg.PEER_SYNC_URLS.get(node_id, [])
        real_url = real_urls[0] if real_urls else None

        # ── Step 1: attempt the dead URL ─────────────────────────────────
        t0 = time.monotonic()
        dead_status = "unknown"
        dead_error = None
        try:
            await client.get(f"{dead_url}/health")
            dead_status = "open"  # unexpected — port 19999 should be closed
        except httpx.ConnectError as e:
            err = str(e)
            dead_status = (
                "tls_error"
                if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                else "refused"
            )
            dead_error = err
        except httpx.TimeoutException:
            dead_status = "timeout"
            dead_error = "connection timed out"
        except Exception as e:
            dead_status = "error"
            dead_error = str(e)
        dead_ms = round((time.monotonic() - t0) * 1000)

        # ── Step 2: attempt the real URL ─────────────────────────────────
        real_status = "no_url"
        real_http_status = None
        real_error = None
        real_ms = 0
        if real_url:
            t1 = time.monotonic()
            try:
                r = await client.get(f"{real_url}/health")
                real_status = "ok" if r.is_success else "http_error"
                real_http_status = r.status_code
            except httpx.ConnectError as e:
                err = str(e)
                real_status = (
                    "tls_error"
                    if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                    else "refused"
                )
                real_error = err
            except httpx.TimeoutException:
                real_status = "timeout"
                real_error = "connection timed out"
            except Exception as e:
                real_status = "error"
                real_error = str(e)
            real_ms = round((time.monotonic() - t1) * 1000)

        failover_ok = (dead_status in ("refused", "timeout")) and real_status == "ok"

        return {
            "node_id": node_id,
            "dead_url": f"{dead_url}/health",
            "dead_status": dead_status,
            "dead_ms": dead_ms,
            "dead_error": dead_error,
            "real_url": f"{real_url}/health" if real_url else None,
            "real_status": real_status,
            "real_http_status": real_http_status,
            "real_ms": real_ms,
            "real_error": real_error,
            "failover_ok": failover_ok,
        }

    results = []
    async with _probe_client(timeout=8.0) as client:
        for n in cfg._peer_nodes:
            results.append(await _probe_one(n, client))

    all_passed = all(r["failover_ok"] for r in results) if results else False
    return {
        "method": f"synthetic dead-port (port {_FAILOVER_DEAD_PORT}) + real configured URL",
        "dead_port": _FAILOVER_DEAD_PORT,
        "all_passed": all_passed,
        "peers": results,
    }


@router.get("/guid-probe")
async def guid_probe() -> dict:
    """
    Validate Phase 2 GUID deduplication and smart forwarding logic.

    Three sub-tests — all non-destructive and self-cleaning:

    1. GUID dedup (DB layer)
       Inserts a synthetic UUID4 into sync_seen_guids (first insert → accepted),
       then tries to insert the same GUID again (second insert → IntegrityError,
       i.e. correctly deduplicated).  The test GUID is deleted at the end.

    2. Fleet forwarding topology
       Runs _can_reach_directly() for every known peer and returns whether each
       is reachable by this node.  In the current 6-node on-prem fleet every
       peer shares the LAN so all are directly reachable and no forwarding is
       ever needed (zero-overhead path).

    3. Mock remote node relay
       Simulates a phantom source node that has no primary_ip (like a future
       remote VPS) but shares this node's tailnet.  Reports which peers would
       be identified as relay targets — i.e. peers reachable by this node but
       NOT by the phantom source.
    """
    import sqlite3 as _sqlite3

    # ── Test 1: GUID dedup ────────────────────────────────────────────────────
    test_guid = f"__probe__{uuid.uuid4().hex}"
    first_insert = "error"
    second_insert = "error"
    cleanup = "ok"
    dedup_ok = False

    try:
        with get_conn() as conn:
            # First insert — should succeed
            try:
                conn.execute(
                    "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                    (test_guid, int(time.time())),
                )
                first_insert = "accepted"
            except Exception as e:
                first_insert = f"error: {e}"

            # Second insert — should raise IntegrityError (PRIMARY KEY conflict)
            try:
                conn.execute(
                    "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                    (test_guid, int(time.time())),
                )
                second_insert = "unexpected_accepted"  # bug — should have been rejected
            except _sqlite3.IntegrityError:
                second_insert = "deduplicated"
            except Exception as e:
                second_insert = f"error: {e}"

            # Cleanup — remove the probe GUID
            try:
                conn.execute("DELETE FROM sync_seen_guids WHERE guid=?", (test_guid,))
            except Exception as e:
                cleanup = f"failed: {e}"

        dedup_ok = (
            first_insert == "accepted" and second_insert == "deduplicated" and cleanup == "ok"
        )

    except Exception as e:
        first_insert = f"db_error: {e}"

    # ── Test 2: Fleet forwarding topology ─────────────────────────────────────
    topology = []
    for peer in cfg.PEER_NODES:
        topology.append(
            {
                "peer_node_id": peer["node_id"],
                "peer_has_primary_ip": bool(peer.get("primary_ip")),
                "peer_tailnet": peer.get("tailnet", ""),
                "self_can_reach": _can_reach_directly(cfg.SELF_NODE, peer),
            }
        )

    # ── Test 3: Mock remote node relay ────────────────────────────────────────
    # Simulate a source with no primary_ip (VPS) on the same tailnet as self.
    self_tailnet = cfg.SELF_NODE.get("tailnet", "")
    mock_source = {
        "node_id": "__mock_vps__",
        "primary_ip": "",
        "tailnet": self_tailnet,
    }
    relay_peers = [
        p["node_id"]
        for p in cfg.PEER_NODES
        if not _can_reach_directly(mock_source, p) and _can_reach_directly(cfg.SELF_NODE, p)
    ]
    # In the current 6-node on-prem fleet (all have primary_ip, all same-tailnet covered),
    # expected relay count depends on peer tailnet membership:
    # peers with primary_ip CAN be reached by the mock source only via primary_ip path.
    # Since mock_source has no primary_ip → source can't reach LAN peers unless they share tailnet.
    # Peers sharing self_tailnet: mock_source CAN reach them via tailnet (same tailnet).
    # Peers NOT on self_tailnet: only reachable via relay.
    peers_not_on_self_tailnet = [
        p["node_id"] for p in cfg.PEER_NODES if p.get("tailnet", "") != self_tailnet
    ]
    expected_relay_ids = set(peers_not_on_self_tailnet)
    relay_ok = set(relay_peers) == expected_relay_ids

    mock_relay = {
        "mock_source_has_primary_ip": False,
        "mock_source_tailnet": self_tailnet or "(none)",
        "relay_peers": relay_peers,
        "expected_relay_peers": sorted(expected_relay_ids),
        "relay_ok": relay_ok,
    }

    all_passed = dedup_ok and relay_ok
    return {
        "all_passed": all_passed,
        "dedup": {
            "ok": dedup_ok,
            "first_insert": first_insert,
            "second_insert": second_insert,
            "cleanup": cleanup,
        },
        "topology": topology,
        "mock_relay": mock_relay,
    }


@router.post("/roundtrip-test")
async def sync_roundtrip_test() -> dict:
    """
    End-to-end data propagation test.

    Writes a temporary canary row to the settings table, enqueues it for peers
    via the normal drain path, then polls the first available peer's API to
    confirm the row arrived.  Cleans up (deletes the canary) regardless of
    outcome.

    Returns:
      status:         ok | timeout | auth_failed | no_peers | no_secret | error
      elapsed_ms:     time from write to confirmed propagation (or timeout)
      propagated_to:  node_id of the peer that received the canary
      error:          human-readable failure reason, or null on success

    Timeout: 25s — covers more than one drain cycle (drain sleeps 1-20s randomly).
    """
    if not cfg._peer_nodes:
        return {
            "status": "no_peers",
            "elapsed_ms": 0,
            "propagated_to": None,
            "error": "no active peer nodes configured",
        }
    if not cfg.SYNC_SECRET:
        return {
            "status": "no_secret",
            "elapsed_ms": 0,
            "propagated_to": None,
            "error": "BLUEPRINTS_SYNC_SECRET not configured — cannot authenticate peer read",
        }

    canary_key = f"_bp_diag_canary_{uuid.uuid4().hex[:16]}"
    peer = cfg._peer_nodes[0]
    peer_base = (
        f"{peer.get('sync_scheme', 'http')}://{peer['primary_ip']}:{peer.get('sync_port', 8080)}"
    )
    start_ts = time.monotonic()
    propagated = False
    early_result: dict | None = None

    try:
        # Write canary locally and queue for peers via normal sync path
        canary_row = {
            "key": canary_key,
            "value": "diagnostic-probe",
            "description": "Temporary sync round-trip test — will auto-delete",
        }
        with get_conn() as conn:
            gen = increment_gen(conn, "human")
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)",
                (canary_key, canary_row["value"], canary_row["description"]),
            )
            enqueue_for_all_peers(conn, "INSERT", "settings", canary_key, canary_row, gen)
        _invalidate_sync_status_cache()
        log.info("roundtrip-test: wrote canary '%s', polling %s", canary_key, peer["node_id"])

        # Poll peer for canary; 12 x 2s = 24s max
        async with _probe_client(timeout=8.0) as client:
            for _ in range(12):
                await asyncio.sleep(2)
                try:
                    token = compute_token(cfg.SYNC_SECRET)
                    r = await client.get(
                        f"{peer_base}/api/v1/settings/{canary_key}",
                        headers={"X-API-Token": token},
                    )
                    if r.status_code == 200:
                        propagated = True
                        break
                    if r.status_code == 401:
                        elapsed = round((time.monotonic() - start_ts) * 1000)
                        # Disambiguate wrong-secret vs clock-skew.
                        # TOTP window = ±1 × 5s = 15s tolerance.
                        # /health is auth-exempt — fetch it to get the peer's
                        # Date response header and compare against local time.
                        totp_note = ""
                        try:
                            health_r = await client.get(f"{peer_base}/health")
                            peer_date = health_r.headers.get("date", "")
                            if peer_date:
                                peer_ts = parsedate_to_datetime(peer_date).timestamp()
                                skew_s = abs(peer_ts - time.time())
                                if skew_s > 15:
                                    totp_note = (
                                        f" — clock skew {skew_s:.0f}s detected "
                                        f"(TOTP window is \u00b115s; sync NTP/chrony on peer)"
                                    )
                                else:
                                    totp_note = (
                                        f" — clock skew only {skew_s:.1f}s "
                                        f"(within TOTP tolerance; likely wrong BLUEPRINTS_SYNC_SECRET)"
                                    )
                        except Exception:
                            pass  # if health fetch fails, omit the clock note
                        early_result = {
                            "status": "auth_failed",
                            "elapsed_ms": elapsed,
                            "propagated_to": None,
                            "error": (
                                f"peer {peer['node_id']} rejected token (HTTP 401){totp_note}"
                            ),
                        }
                        break
                except Exception as poll_err:
                    log.debug("roundtrip-test: poll error: %s", poll_err)
    finally:
        # Always clean up — delete canary locally and queue delete for peers
        try:
            with get_conn() as conn:
                conn.execute("DELETE FROM settings WHERE key=?", (canary_key,))
                gen = increment_gen(conn, "human")
                enqueue_for_all_peers(conn, "DELETE", "settings", canary_key, None, gen)
            _invalidate_sync_status_cache()
            log.info("roundtrip-test: canary '%s' deleted", canary_key)
        except Exception as cleanup_err:
            log.warning("roundtrip-test: cleanup failed: %s", cleanup_err)

    elapsed = round((time.monotonic() - start_ts) * 1000)
    if early_result:
        return early_result
    if propagated:
        log.info("roundtrip-test: propagated to %s in %dms", peer["node_id"], elapsed)
        return {
            "status": "ok",
            "elapsed_ms": elapsed,
            "propagated_to": peer["node_id"],
            "error": None,
        }
    log.warning(
        "roundtrip-test: timeout after %dms — canary not found on %s", elapsed, peer["node_id"]
    )
    return {
        "status": "timeout",
        "elapsed_ms": elapsed,
        "propagated_to": None,
        "error": f"canary not found on {peer['node_id']} within 25s — drain may be stalled or queue depth too high",
    }
