"""Read-only Kanban shadow datastore parity checks."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import tarfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .kanban_datastore import (
    ACTIVE_STORE_POSTGRES,
    CANDIDATE_READ_STORE_POSTGRES,
    KANBAN_DATASTORE_TABLES,
    KanbanDatastoreConfig,
)
from .kanban_postgres import postgres_candidate_connection
from .kanban_store import (
    KanbanItemCycleError,
    KanbanItemNotFound,
    KanbanStore,
)

KANBAN_SHADOW_PARITY_SCHEMA = "xarta.kanban.datastore.shadow_parity.v1"
KANBAN_SHADOW_PARITY_COMPARISON_SCHEMA = "xarta.kanban.datastore.shadow_parity.comparison.v1"
KANBAN_SHADOW_BACKEND = "sqlite-shadow"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SUPPORT_TABLES = ("settings",)
_KANBAN_FILE_SKIP_ROOTS = {"backups", ".stfolder", ".stversions"}


class KanbanShadowParityError(RuntimeError):
    """Raised when the shadow parity runner cannot build a safe report."""


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise KanbanShadowParityError(f"unsafe SQLite identifier: {value!r}")
    return f'"{value}"'


def _sha256_json(payload: Any) -> str:
    data = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return value


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    if not rows:
        raise KanbanShadowParityError(f"Kanban parity table missing: {table}")
    return [str(row["name"]) for row in rows]


def _table_pk(conn: sqlite3.Connection, table: str) -> str:
    rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    for row in rows:
        if int(row["pk"] or 0):
            return str(row["name"])
    raise KanbanShadowParityError(f"Kanban parity table has no primary key: {table}")


def _collect_table_data(conn: sqlite3.Connection) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for table in KANBAN_DATASTORE_TABLES:
        quoted_table = _quote_identifier(table)
        columns = _table_columns(conn, table)
        pk = _table_pk(conn, table)
        rows = conn.execute(
            f"SELECT * FROM {quoted_table} ORDER BY {_quote_identifier(pk)}"
        ).fetchall()
        tables[table] = {
            "columns": columns,
            "primary_key": pk,
            "rows": [{column: row[column] for column in columns} for row in rows],
        }
    return {
        "schema": "xarta.kanban.datastore.table_data.v1",
        "tables": tables,
        "excluded_tables": ["sync_queue"],
        "sync_queue_included": False,
    }


def _table_counts(table_data: dict[str, Any]) -> dict[str, int]:
    return {
        table: len(payload.get("rows") or [])
        for table, payload in (table_data.get("tables") or {}).items()
    }


def _table_hashes(table_data: dict[str, Any]) -> dict[str, str]:
    tables = table_data.get("tables") or {}
    return {table: _sha256_json(tables.get(table, {})) for table in KANBAN_DATASTORE_TABLES}


def _rows_by_primary_key(
    conn: sqlite3.Connection, table: str
) -> tuple[str, list[str], dict[str, dict[str, Any]]]:
    columns = _table_columns(conn, table)
    pk = _table_pk(conn, table)
    quoted_table = _quote_identifier(table)
    rows = {
        str(row[pk]): {column: row[column] for column in columns}
        for row in conn.execute(f"SELECT * FROM {quoted_table}").fetchall()
    }
    return pk, columns, rows


def _row_hash(row: dict[str, Any], columns: list[str]) -> str:
    return _sha256_json({column: row.get(column) for column in columns})


def _diff_table_data(conn: sqlite3.Connection, table_data: dict[str, Any]) -> dict[str, Any]:
    table_reports: dict[str, Any] = {}
    totals = {
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "deleted": 0,
        "conflicts": 0,
    }
    for table in KANBAN_DATASTORE_TABLES:
        pk, db_columns, current_rows = _rows_by_primary_key(conn, table)
        payload = table_data["tables"][table]
        backup_columns = [column for column in payload.get("columns", []) if column in db_columns]
        backup_rows = {
            str(row.get(payload.get("primary_key") or pk)): row for row in payload.get("rows") or []
        }
        inserted_ids = sorted(set(backup_rows) - set(current_rows))
        deleted_ids = sorted(set(current_rows) - set(backup_rows))
        shared_ids = sorted(set(current_rows) & set(backup_rows))
        updated_ids = [
            row_id
            for row_id in shared_ids
            if _row_hash(current_rows[row_id], backup_columns)
            != _row_hash(backup_rows[row_id], backup_columns)
        ]
        unchanged_ids = [row_id for row_id in shared_ids if row_id not in set(updated_ids)]
        conflicts = len(updated_ids) + len(deleted_ids)
        table_reports[table] = {
            "primary_key": payload.get("primary_key") or pk,
            "inserted": len(inserted_ids),
            "updated": len(updated_ids),
            "unchanged": len(unchanged_ids),
            "deleted": len(deleted_ids),
            "conflicts": conflicts,
            "sample_inserted_ids": inserted_ids[:10],
            "sample_updated_ids": updated_ids[:10],
            "sample_deleted_ids": deleted_ids[:10],
        }
        totals["inserted"] += len(inserted_ids)
        totals["updated"] += len(updated_ids)
        totals["unchanged"] += len(unchanged_ids)
        totals["deleted"] += len(deleted_ids)
        totals["conflicts"] += conflicts

    return {
        "schema": "xarta.kanban.datastore.shadow_import_preview.v1",
        "table_count": len(KANBAN_DATASTORE_TABLES),
        "tables": table_reports,
        "totals": totals,
        "idempotent": not (totals["inserted"] or totals["updated"] or totals["deleted"]),
        "sync_queue_included": False,
        "sync_queue_rows_created": False,
        "excluded_tables": ["sync_queue"],
    }


def _schema_rows(live_conn: sqlite3.Connection) -> list[sqlite3.Row]:
    shadow_tables = (*KANBAN_DATASTORE_TABLES, *_SUPPORT_TABLES)
    placeholders = ",".join("?" for _ in shadow_tables)
    return live_conn.execute(
        f"""
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND (
            (type='table' AND name IN ({placeholders}))
            OR (type='index' AND tbl_name IN ({placeholders}))
          )
        ORDER BY
          CASE type WHEN 'table' THEN 0 ELSE 1 END,
          name
        """,
        (*shadow_tables, *shadow_tables),
    ).fetchall()


def _create_shadow_schema(live_conn: sqlite3.Connection) -> sqlite3.Connection:
    candidate = sqlite3.connect(":memory:")
    candidate.row_factory = sqlite3.Row
    for row in _schema_rows(live_conn):
        candidate.execute(str(row["sql"]))
    return candidate


def _insert_table_data(candidate: sqlite3.Connection, table_data: dict[str, Any]) -> int:
    inserted = 0
    for table, payload in (table_data.get("tables") or {}).items():
        columns = list(payload.get("columns") or [])
        if not columns:
            continue
        quoted_table = _quote_identifier(table)
        quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
        placeholders = ", ".join("?" for _ in columns)
        candidate.executemany(
            f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})",
            ([row.get(column) for column in columns] for row in payload.get("rows") or []),
        )
        inserted += len(payload.get("rows") or [])
    return inserted


def _copy_support_settings(
    live_conn: sqlite3.Connection,
    candidate: sqlite3.Connection,
    *,
    setting_keys: tuple[str, ...],
) -> int:
    if not setting_keys:
        return 0
    columns = _table_columns(live_conn, "settings")
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    copied = 0
    for key in setting_keys:
        row = live_conn.execute("SELECT * FROM settings WHERE key=?", (key,)).fetchone()
        if row is None:
            continue
        candidate.execute(
            f"INSERT INTO settings ({quoted_columns}) VALUES ({placeholders})",
            [row[column] for column in columns],
        )
        copied += 1
    return copied


def kanban_shadow_candidate_connection(
    live_conn: sqlite3.Connection,
    *,
    support_setting_keys: tuple[str, ...] = (),
) -> sqlite3.Connection:
    """Return an in-memory candidate loaded from live Kanban tables.

    The caller owns the returned connection and must close it. This is for
    proof-read mode only; it does not make the candidate a persistent datastore.
    """

    table_data = _collect_table_data(live_conn)
    candidate = _create_shadow_schema(live_conn)
    try:
        _insert_table_data(candidate, table_data)
        _copy_support_settings(live_conn, candidate, setting_keys=support_setting_keys)
    except Exception:
        candidate.close()
        raise
    return candidate


def _row_subset(row: Any, fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: row[field] for field in fields if field in row.keys()}


def _item_summary(row: Any | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "item_id": row["item_id"],
        "parent_item_id": row["parent_item_id"] or "",
        "title": row["title"],
        "item_type": row["item_type"],
        "state_id": row["state_id"],
        "priority_id": row["priority_id"],
        "depth": int(row["depth"] or 0),
        "sort_order": int(row["sort_order"] or 0),
        "status": row["status"],
        "goal_flag": bool(row["goal_flag"]),
        "automation_excluded": bool(row["automation_excluded"]),
        "tags": _json_value(row["tags_json"], []),
        "source_ref": row["source_ref"],
        "source_hash": row["source_hash"],
        "updated_at": row["updated_at"],
    }


def _state_snapshot(row: Any) -> dict[str, Any]:
    return {
        "state_id": row["state_id"],
        "label": row["label"],
        "lane_key": row["lane_key"],
        "status_category": row["status_category"],
        "sort_order": int(row["sort_order"] or 0),
        "is_terminal": bool(row["is_terminal"]),
    }


def _priority_snapshot(row: Any) -> dict[str, Any]:
    return {
        "priority_id": row["priority_id"],
        "label": row["label"],
        "weight": int(row["weight"] or 0),
        "sort_order": int(row["sort_order"] or 0),
    }


def _store(conn: sqlite3.Connection, config: dict[str, Any]) -> KanbanStore:
    return KanbanStore(
        conn,
        depth_limit=int(config["depth_limit"]),
        show_test_entries_setting=str(config["show_test_entries_setting"]),
        agent_working_out_tag=str(config["agent_working_out_tag"]),
    )


def _config_snapshot(conn: sqlite3.Connection, config: dict[str, Any]) -> dict[str, Any]:
    read = _store(conn, config).config()
    return {
        "schema": "xarta.kanban.datastore.shadow_config_snapshot.v1",
        "states": [_state_snapshot(row) for row in read.states],
        "priorities": [_priority_snapshot(row) for row in read.priorities],
        "preferences": read.preferences,
        "depth_limit": read.depth_limit,
    }


def _board_snapshot(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    parent_item_id: str | None = None,
) -> dict[str, Any]:
    try:
        read = _store(conn, config).board(parent_item_id)
    except (KanbanItemNotFound, KanbanItemCycleError) as exc:
        return {"error": str(exc), "parent_item_id": parent_item_id or ""}
    return {
        "schema": "xarta.kanban.datastore.shadow_board_snapshot.v1",
        "parent": _item_summary(read.parent),
        "breadcrumbs": [_item_summary(row) for row in read.breadcrumbs],
        "remaining_depth": read.remaining_depth,
        "preferences": read.preferences,
        "hidden_test_items": read.hidden_test_items,
        "test_entries": {
            "show": read.show_test_entries,
            "hidden": read.hidden_test_items,
        },
        "columns": [
            {
                "state": _state_snapshot(state),
                "item_ids": [row["item_id"] for row in read.items_by_state[state["state_id"]]],
                "items": [_item_summary(row) for row in read.items_by_state[state["state_id"]]],
            }
            for state in read.states
        ],
        "rollup": read.rollup,
    }


def _item_detail_snapshot(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    item_id: str,
) -> dict[str, Any]:
    try:
        read = _store(conn, config).item_detail(item_id)
    except (KanbanItemNotFound, KanbanItemCycleError) as exc:
        return {"error": str(exc), "item_id": item_id}
    return {
        "schema": "xarta.kanban.datastore.shadow_item_detail_snapshot.v1",
        "item": _item_summary(read.item),
        "breadcrumbs": [_item_summary(row) for row in read.breadcrumbs],
        "children": [_item_summary(row) for row in read.children],
        "issues": [_item_summary(row) for row in read.issues],
        "todos": [_item_summary(row) for row in read.todos],
        "blockers": [
            _row_subset(row, ("blocker_id", "item_id", "title", "status", "updated_at"))
            for row in read.blockers
        ],
        "discussions": [
            _row_subset(
                row,
                ("discussion_id", "item_id", "author", "body_excerpt", "status", "updated_at"),
            )
            for row in read.discussions
        ],
        "links": [
            _row_subset(
                row,
                ("link_id", "source_item_id", "target_item_id", "link_type", "updated_at"),
            )
            for row in read.links
        ],
        "commits": [
            _row_subset(
                row,
                (
                    "commit_link_id",
                    "item_id",
                    "repo_full_name",
                    "sha",
                    "branch",
                    "updated_at",
                ),
            )
            for row in read.commits
        ],
        "audit": [
            _row_subset(row, ("audit_id", "item_id", "action", "target_ref", "created_at"))
            for row in read.audit
        ],
        "rollup": read.rollup,
        "counts": {
            "children": len(read.children),
            "issues": len(read.issues),
            "todos": len(read.todos),
            "blockers": len(read.blockers),
            "discussions": len(read.discussions),
            "links": len(read.links),
            "commits": len(read.commits),
            "audit": len(read.audit),
        },
        "remaining_depth": read.remaining_depth,
        "depth_limit": read.depth_limit,
    }


def _priority_snapshot_payload(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    limit: int,
) -> dict[str, Any]:
    reads = _store(conn, config).priority_recommendations(scope_id="kanban", limit=limit)
    recommendations: list[dict[str, Any]] = []
    for read in reads:
        row = read.recommendation
        recommendations.append(
            {
                "recommendation_id": row["recommendation_id"],
                "scope_id": row["scope_id"],
                "rank": int(row["rank"] or 0),
                "item_id": row["item_id"],
                "title": row["title"],
                "summary": row["summary"],
                "reason": row["reason"],
                "priority_id": row["priority_id"],
                "state_id": row["state_id"],
                "score": float(row["score"] or 0),
                "strategy_version": row["strategy_version"],
                "source_surface": row["source_surface"],
                "metadata": _json_value(row["metadata_json"], {}),
                "generated_at": row["generated_at"],
                "updated_at": row["updated_at"],
                "item": _item_summary(read.item),
                "breadcrumb_ids": [breadcrumb["item_id"] for breadcrumb in read.breadcrumbs],
            }
        )
    return {
        "schema": "xarta.kanban.datastore.shadow_priorities_snapshot.v1",
        "scope_id": "kanban",
        "limit": limit,
        "count": len(recommendations),
        "recommendations": recommendations,
    }


def _table_api_snapshot(
    conn: sqlite3.Connection,
    table: str,
    fields: tuple[str, ...],
    *,
    limit: int,
) -> dict[str, Any]:
    pk = _table_pk(conn, table)
    quoted_table = _quote_identifier(table)
    count = conn.execute(f"SELECT COUNT(*) AS count FROM {quoted_table}").fetchone()["count"]
    rows = conn.execute(
        f"SELECT * FROM {quoted_table} ORDER BY {_quote_identifier(pk)} LIMIT ?",
        (limit,),
    ).fetchall()
    return {
        "schema": "xarta.kanban.datastore.shadow_table_api_snapshot.v1",
        "table": table,
        "primary_key": pk,
        "count": int(count or 0),
        "sample_ids": [row[pk] for row in rows],
        "rows": [_row_subset(row, fields) for row in rows],
    }


def _sample_item_ids(conn: sqlite3.Connection, limit: int) -> list[str]:
    candidates: list[str] = []
    queries = [
        """
        SELECT parent_item_id AS item_id
        FROM kanban_items
        WHERE parent_item_id IS NOT NULL AND status != 'archived'
        GROUP BY parent_item_id
        ORDER BY COUNT(*) DESC, parent_item_id
        LIMIT 3
        """,
        """
        SELECT item_id
        FROM kanban_discussions
        GROUP BY item_id
        ORDER BY COUNT(*) DESC, item_id
        LIMIT 3
        """,
        """
        SELECT item_id
        FROM kanban_priority_recommendations
        ORDER BY rank, item_id
        LIMIT 3
        """,
        """
        SELECT item_id
        FROM kanban_review_processor_markers
        ORDER BY updated_at DESC, item_id
        LIMIT 3
        """,
        """
        SELECT item_id
        FROM kanban_agent_sessions
        ORDER BY updated_at DESC, item_id
        LIMIT 3
        """,
        """
        SELECT item_id
        FROM kanban_items
        WHERE status != 'archived'
        ORDER BY updated_at DESC, item_id
        LIMIT 5
        """,
    ]
    for query in queries:
        for row in conn.execute(query).fetchall():
            item_id = str(row["item_id"] or "")
            if item_id and item_id not in candidates:
                candidates.append(item_id)
            if len(candidates) >= limit:
                return candidates
    return candidates


def _child_board_parent_id(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT parent_item_id AS item_id
        FROM kanban_items
        WHERE parent_item_id IS NOT NULL AND status != 'archived'
        GROUP BY parent_item_id
        ORDER BY COUNT(*) DESC, parent_item_id
        LIMIT 1
        """
    ).fetchone()
    return str(row["item_id"] or "") if row else ""


def _comparison(name: str, live_payload: Any, candidate_payload: Any) -> dict[str, Any]:
    live_hash = _sha256_json(live_payload)
    candidate_hash = _sha256_json(candidate_payload)
    return {
        "schema": KANBAN_SHADOW_PARITY_COMPARISON_SCHEMA,
        "name": name,
        "ok": live_hash == candidate_hash,
        "live_sha256": live_hash,
        "candidate_sha256": candidate_hash,
    }


def _kanban_file_count(root: Path) -> int:
    if not root.exists():
        return 0
    count = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] in _KANBAN_FILE_SKIP_ROOTS:
            continue
        count += 1
    return count


def _backup_manifest_summaries(backup_dir: Path, *, limit: int) -> dict[str, Any]:
    warnings: list[str] = []
    paths = (
        sorted(backup_dir.glob("*-kanban-*.tar.gz"), reverse=True) if backup_dir.exists() else []
    )
    entries: list[dict[str, Any]] = []
    for path in paths[:limit]:
        try:
            with tarfile.open(path, "r:gz") as tar:
                manifest_file = tar.extractfile("manifest.json")
                if manifest_file is None:
                    raise KanbanShadowParityError("manifest.json is not readable")
                manifest = json.loads(manifest_file.read().decode("utf-8"))
        except Exception as exc:
            warnings.append(f"{path.name}: {exc}")
            continue
        entries.append(
            {
                "filename": path.name,
                "backup_id": str(manifest.get("backup_id") or ""),
                "kind": str(manifest.get("kind") or ""),
                "purpose": str(manifest.get("purpose") or ""),
                "db_gen": manifest.get("db_gen"),
                "table_count": len(manifest.get("table_counts") or {}),
                "table_counts": manifest.get("table_counts") or {},
                "file_count": manifest.get("file_count"),
                "sync_queue_included": bool(manifest.get("sync_queue_included")),
                "table_data_sha256_present": bool(manifest.get("table_data_sha256")),
                "file_hashes_present": bool(manifest.get("file_hashes")),
            }
        )
    return {
        "schema": "xarta.kanban.datastore.shadow_backup_snapshot.v1",
        "backup_dir": str(backup_dir),
        "count": len(paths),
        "sample_limit": limit,
        "backups": entries,
        "warnings": warnings,
    }


def _snapshot_payloads(
    conn: sqlite3.Connection,
    config: dict[str, Any],
    *,
    sample_item_ids: list[str],
    child_parent_id: str,
    sample_limit: int,
    kanban_root: Path,
    backup_dir: Path,
    include_backups: bool,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {
        "config": _config_snapshot(conn, config),
        "root_board": _board_snapshot(conn, config),
        "priorities": _priority_snapshot_payload(conn, config, limit=50),
        "review_decisions": _table_api_snapshot(
            conn,
            "kanban_review_decisions",
            (
                "decision_id",
                "item_id",
                "processor_kind",
                "decision_type",
                "title",
                "status",
                "provider_mode",
                "updated_at",
            ),
            limit=sample_limit,
        ),
        "automation_markers": _table_api_snapshot(
            conn,
            "kanban_review_processor_markers",
            (
                "marker_id",
                "item_id",
                "processor_kind",
                "document_type",
                "status",
                "provider_mode",
                "decision_id",
                "updated_at",
            ),
            limit=sample_limit,
        ),
        "automation_failure_events": _table_api_snapshot(
            conn,
            "kanban_review_processor_failure_events",
            (
                "failure_event_id",
                "marker_id",
                "item_id",
                "processor_kind",
                "status",
                "error_class",
                "provider_mode",
                "updated_at",
            ),
            limit=sample_limit,
        ),
        "agent_sessions": _table_api_snapshot(
            conn,
            "kanban_agent_sessions",
            (
                "session_id",
                "item_id",
                "agent_id",
                "node_id",
                "worktree_path",
                "repo_full_name",
                "branch",
                "status",
                "summary",
                "updated_at",
            ),
            limit=sample_limit,
        ),
        "file_backed_docs": {
            "schema": "xarta.kanban.datastore.shadow_file_snapshot.v1",
            "kanban_root": str(kanban_root),
            "file_count": _kanban_file_count(kanban_root),
            "candidate_independent": True,
        },
    }
    if child_parent_id:
        payloads["child_board"] = _board_snapshot(conn, config, parent_item_id=child_parent_id)
    for item_id in sample_item_ids:
        payloads[f"item_detail:{item_id}"] = _item_detail_snapshot(conn, config, item_id)
    if include_backups:
        payloads["backup_packages"] = _backup_manifest_summaries(
            backup_dir,
            limit=max(1, min(sample_limit, 10)),
        )
    return payloads


def kanban_shadow_parity_report(
    live_conn: sqlite3.Connection,
    *,
    depth_limit: int,
    show_test_entries_setting: str,
    agent_working_out_tag: str,
    kanban_root: Path,
    backup_dir: Path,
    candidate_backend: str,
    sample_limit: int = 5,
    include_backups: bool = True,
    candidate_mutator: Callable[[sqlite3.Connection], None] | None = None,
) -> dict[str, Any]:
    """Build an in-memory candidate store and compare read-shaped Kanban payloads."""

    clean_sample_limit = max(1, min(int(sample_limit or 5), 20))
    store_config = {
        "depth_limit": depth_limit,
        "show_test_entries_setting": show_test_entries_setting,
        "agent_working_out_tag": agent_working_out_tag,
    }
    sync_before = live_conn.execute("SELECT COUNT(*) AS count FROM sync_queue").fetchone()["count"]
    table_data = _collect_table_data(live_conn)
    live_table_hashes = _table_hashes(table_data)
    live_table_counts = _table_counts(table_data)
    candidate = _create_shadow_schema(live_conn)
    try:
        preload_preview = _diff_table_data(candidate, table_data)
        loaded_rows = _insert_table_data(candidate, table_data)
        support_settings_copied = _copy_support_settings(
            live_conn,
            candidate,
            setting_keys=(show_test_entries_setting,),
        )
        if candidate_mutator is not None:
            candidate_mutator(candidate)
        postload_preview = _diff_table_data(candidate, table_data)
        candidate_table_data = _collect_table_data(candidate)
        candidate_table_hashes = _table_hashes(candidate_table_data)
        candidate_table_counts = _table_counts(candidate_table_data)
        sample_item_ids = _sample_item_ids(live_conn, clean_sample_limit)
        child_parent_id = _child_board_parent_id(live_conn)
        live_payloads = _snapshot_payloads(
            live_conn,
            store_config,
            sample_item_ids=sample_item_ids,
            child_parent_id=child_parent_id,
            sample_limit=clean_sample_limit,
            kanban_root=kanban_root,
            backup_dir=backup_dir,
            include_backups=include_backups,
        )
        candidate_payloads = _snapshot_payloads(
            candidate,
            store_config,
            sample_item_ids=sample_item_ids,
            child_parent_id=child_parent_id,
            sample_limit=clean_sample_limit,
            kanban_root=kanban_root,
            backup_dir=backup_dir,
            include_backups=include_backups,
        )
    finally:
        candidate.close()

    sync_after = live_conn.execute("SELECT COUNT(*) AS count FROM sync_queue").fetchone()["count"]
    table_hash_mismatches = [
        table
        for table in KANBAN_DATASTORE_TABLES
        if live_table_hashes.get(table) != candidate_table_hashes.get(table)
    ]
    comparisons = [
        _comparison(name, live_payloads[name], candidate_payloads.get(name))
        for name in sorted(live_payloads)
    ]
    failed_comparisons = [comparison["name"] for comparison in comparisons if not comparison["ok"]]
    coverage = {
        "schema": "xarta.kanban.datastore.shadow_parity.coverage.v1",
        "api_shapes": sorted(live_payloads),
        "sampled_item_detail_count": len(
            [name for name in live_payloads if name.startswith("item_detail:")]
        ),
        "backup_package_count": int(live_payloads.get("backup_packages", {}).get("count") or 0),
        "kanban_file_count": int(live_payloads.get("file_backed_docs", {}).get("file_count") or 0),
        "automation_marker_count": int(
            live_payloads.get("automation_markers", {}).get("count") or 0
        ),
        "automation_failure_event_count": int(
            live_payloads.get("automation_failure_events", {}).get("count") or 0
        ),
        "agent_session_count": int(live_payloads.get("agent_sessions", {}).get("count") or 0),
        "review_decision_count": int(live_payloads.get("review_decisions", {}).get("count") or 0),
    }
    migration_ok = (
        preload_preview["totals"]["updated"] == 0
        and preload_preview["totals"]["deleted"] == 0
        and preload_preview["totals"]["conflicts"] == 0
        and postload_preview["idempotent"]
        and not table_hash_mismatches
    )
    safety = {
        "destructive": False,
        "candidate_storage": "memory",
        "live_reads_changed": False,
        "live_writes_changed": False,
        "sqlite_rows_retained": True,
        "sync_queue_rows_created": False,
        "sync_queue_count_before": int(sync_before or 0),
        "sync_queue_count_after": int(sync_after or 0),
        "sync_queue_count_changed": int(sync_before or 0) != int(sync_after or 0),
    }
    ok = migration_ok and not failed_comparisons and not safety["sync_queue_count_changed"]
    return {
        "ok": ok,
        "schema": KANBAN_SHADOW_PARITY_SCHEMA,
        "candidate": {
            "backend": candidate_backend,
            "shadow_backend": KANBAN_SHADOW_BACKEND,
            "storage": "memory",
            "live_reads_enabled": False,
            "live_writes_enabled": False,
        },
        "tables": {
            "included": list(KANBAN_DATASTORE_TABLES),
            "support_tables": list(_SUPPORT_TABLES),
            "excluded": ["sync_queue"],
            "live_counts": live_table_counts,
            "candidate_counts": candidate_table_counts,
            "hash_mismatches": table_hash_mismatches,
        },
        "migration": {
            "schema": "xarta.kanban.datastore.shadow_migration_preview.v1",
            "preload_preview": preload_preview,
            "postload_preview": postload_preview,
            "loaded_rows": loaded_rows,
            "support_settings_copied": support_settings_copied,
            "idempotent_after_load": postload_preview["idempotent"],
            "conflicts_after_load": postload_preview["totals"]["conflicts"],
        },
        "samples": {
            "sample_limit": clean_sample_limit,
            "item_ids": sample_item_ids,
            "child_board_parent_id": child_parent_id,
        },
        "coverage": coverage,
        "api_comparisons": comparisons,
        "failed_comparisons": failed_comparisons,
        "safety": safety,
    }


def kanban_postgres_parity_report(
    live_conn: sqlite3.Connection,
    *,
    depth_limit: int,
    show_test_entries_setting: str,
    agent_working_out_tag: str,
    kanban_root: Path,
    backup_dir: Path,
    datastore_config: KanbanDatastoreConfig,
    sample_limit: int = 5,
    include_backups: bool = True,
) -> dict[str, Any]:
    """Compare live SQLite Kanban payloads with the persistent Postgres candidate."""

    if not datastore_config.candidate_database_url_configured:
        raise KanbanShadowParityError("Postgres candidate database URL is not configured")

    clean_sample_limit = max(1, min(int(sample_limit or 5), 20))
    store_config = {
        "depth_limit": depth_limit,
        "show_test_entries_setting": show_test_entries_setting,
        "agent_working_out_tag": agent_working_out_tag,
    }
    sync_before = live_conn.execute("SELECT COUNT(*) AS count FROM sync_queue").fetchone()["count"]
    live_table_data = _collect_table_data(live_conn)
    live_table_hashes = _table_hashes(live_table_data)
    live_table_counts = _table_counts(live_table_data)
    sample_item_ids = _sample_item_ids(live_conn, clean_sample_limit)
    child_parent_id = _child_board_parent_id(live_conn)
    live_payloads = _snapshot_payloads(
        live_conn,
        store_config,
        sample_item_ids=sample_item_ids,
        child_parent_id=child_parent_id,
        sample_limit=clean_sample_limit,
        kanban_root=kanban_root,
        backup_dir=backup_dir,
        include_backups=include_backups,
    )

    candidate = postgres_candidate_connection(datastore_config.candidate_database_url)
    try:
        candidate_table_data = _collect_table_data(candidate)  # type: ignore[arg-type]
        candidate_table_hashes = _table_hashes(candidate_table_data)
        candidate_table_counts = _table_counts(candidate_table_data)
        candidate_payloads = _snapshot_payloads(
            candidate,  # type: ignore[arg-type]
            store_config,
            sample_item_ids=sample_item_ids,
            child_parent_id=child_parent_id,
            sample_limit=clean_sample_limit,
            kanban_root=kanban_root,
            backup_dir=backup_dir,
            include_backups=include_backups,
        )
    finally:
        candidate.close()

    sync_after = live_conn.execute("SELECT COUNT(*) AS count FROM sync_queue").fetchone()["count"]
    table_hash_mismatches = [
        table
        for table in KANBAN_DATASTORE_TABLES
        if live_table_hashes.get(table) != candidate_table_hashes.get(table)
    ]
    comparisons = [
        _comparison(name, live_payloads[name], candidate_payloads.get(name))
        for name in sorted(live_payloads)
    ]
    failed_comparisons = [comparison["name"] for comparison in comparisons if not comparison["ok"]]
    coverage = {
        "schema": "xarta.kanban.datastore.postgres_parity.coverage.v1",
        "api_shapes": sorted(live_payloads),
        "sampled_item_detail_count": len(
            [name for name in live_payloads if name.startswith("item_detail:")]
        ),
        "backup_package_count": int(live_payloads.get("backup_packages", {}).get("count") or 0),
        "kanban_file_count": int(live_payloads.get("file_backed_docs", {}).get("file_count") or 0),
        "automation_marker_count": int(
            live_payloads.get("automation_markers", {}).get("count") or 0
        ),
        "automation_failure_event_count": int(
            live_payloads.get("automation_failure_events", {}).get("count") or 0
        ),
        "agent_session_count": int(live_payloads.get("agent_sessions", {}).get("count") or 0),
        "review_decision_count": int(live_payloads.get("review_decisions", {}).get("count") or 0),
    }
    live_reads_enabled = (
        datastore_config.active_store == ACTIVE_STORE_POSTGRES
        or datastore_config.read_store == CANDIDATE_READ_STORE_POSTGRES
    )
    live_writes_enabled = datastore_config.active_store == ACTIVE_STORE_POSTGRES
    safety = {
        "destructive": False,
        "candidate_storage": "postgres",
        "live_reads_enabled": live_reads_enabled,
        "live_writes_enabled": live_writes_enabled,
        "sqlite_rows_retained": True,
        "sync_queue_rows_created": False,
        "sync_queue_count_before": int(sync_before or 0),
        "sync_queue_count_after": int(sync_after or 0),
        "sync_queue_count_changed": int(sync_before or 0) != int(sync_after or 0),
    }
    ok = (
        not table_hash_mismatches
        and not failed_comparisons
        and not safety["sync_queue_count_changed"]
    )
    return {
        "ok": ok,
        "schema": "xarta.kanban.datastore.postgres_parity.v1",
        "candidate": {
            "backend": datastore_config.candidate_backend,
            "storage": "postgres",
            "live_reads_enabled": live_reads_enabled,
            "live_writes_enabled": live_writes_enabled,
            "database_url_configured": datastore_config.candidate_database_url_configured,
        },
        "tables": {
            "included": list(KANBAN_DATASTORE_TABLES),
            "support_tables": list(_SUPPORT_TABLES),
            "excluded": ["sync_queue"],
            "live_counts": live_table_counts,
            "candidate_counts": candidate_table_counts,
            "hash_mismatches": table_hash_mismatches,
        },
        "migration": {
            "schema": "xarta.kanban.datastore.postgres_migration_observation.v1",
            "idempotent_after_load": not table_hash_mismatches,
            "conflicts_after_load": len(table_hash_mismatches),
        },
        "samples": {
            "sample_limit": clean_sample_limit,
            "item_ids": sample_item_ids,
            "child_board_parent_id": child_parent_id,
        },
        "coverage": coverage,
        "api_comparisons": comparisons,
        "failed_comparisons": failed_comparisons,
        "safety": safety,
    }
