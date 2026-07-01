"""Disabled Kanban datastore configuration and bootstrap planning.

This module is intentionally conservative: live Kanban reads and writes remain
on the current Blueprints SQLite tables until later cutover cards prove export,
parity, rollback, and helper/browser behavior.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

KANBAN_DATASTORE_CONFIG_SCHEMA = "xarta.kanban.datastore.config.v1"
KANBAN_DATASTORE_STATUS_SCHEMA = "xarta.kanban.datastore.status.v1"
KANBAN_DATASTORE_BOOTSTRAP_SCHEMA = "xarta.kanban.datastore.bootstrap.v1"

KANBAN_DATASTORE_MODE_ENV = "BLUEPRINTS_KANBAN_DATASTORE_MODE"
KANBAN_CANDIDATE_BACKEND_ENV = "BLUEPRINTS_KANBAN_CANDIDATE_STORE_BACKEND"
KANBAN_CANDIDATE_DATABASE_URL_ENV = "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL"

ACTIVE_STORE_SQLITE = "sqlite"
SUPPORTED_ACTIVE_STORES = {ACTIVE_STORE_SQLITE}
SUPPORTED_CANDIDATE_BACKENDS = {"postgres"}

KANBAN_DATASTORE_TABLES: tuple[str, ...] = (
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
)


class KanbanDatastoreConfigError(RuntimeError):
    """Raised when Kanban datastore configuration is invalid or unsafe."""


@dataclass(frozen=True)
class KanbanDatastoreConfig:
    active_store: str
    candidate_backend: str
    candidate_database_url_configured: bool


def _clean_env_value(value: str | None, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def load_kanban_datastore_config(
    env: Mapping[str, str] | None = None,
) -> KanbanDatastoreConfig:
    """Load and validate datastore config without selecting a live candidate store."""

    source = env or os.environ
    active_store = _clean_env_value(source.get(KANBAN_DATASTORE_MODE_ENV), ACTIVE_STORE_SQLITE)
    if active_store not in SUPPORTED_ACTIVE_STORES:
        supported = ", ".join(sorted(SUPPORTED_ACTIVE_STORES))
        raise KanbanDatastoreConfigError(
            f"{KANBAN_DATASTORE_MODE_ENV}={active_store!r} is not enabled. "
            f"Supported live Kanban datastore modes in this slice: {supported}."
        )

    candidate_backend = _clean_env_value(
        source.get(KANBAN_CANDIDATE_BACKEND_ENV),
        "postgres",
    )
    if candidate_backend not in SUPPORTED_CANDIDATE_BACKENDS:
        supported = ", ".join(sorted(SUPPORTED_CANDIDATE_BACKENDS))
        raise KanbanDatastoreConfigError(
            f"{KANBAN_CANDIDATE_BACKEND_ENV}={candidate_backend!r} is invalid. "
            f"Supported candidate Kanban datastore backends: {supported}."
        )

    return KanbanDatastoreConfig(
        active_store=active_store,
        candidate_backend=candidate_backend,
        candidate_database_url_configured=bool(
            str(source.get(KANBAN_CANDIDATE_DATABASE_URL_ENV, "")).strip()
        ),
    )


def kanban_datastore_status(config: KanbanDatastoreConfig) -> dict[str, Any]:
    """Return operator/agent-visible status without exposing connection secrets."""

    return {
        "ok": True,
        "schema": KANBAN_DATASTORE_STATUS_SCHEMA,
        "config_schema": KANBAN_DATASTORE_CONFIG_SCHEMA,
        "active_store": config.active_store,
        "reads": {
            "store": ACTIVE_STORE_SQLITE,
            "candidate_enabled": False,
        },
        "writes": {
            "store": ACTIVE_STORE_SQLITE,
            "candidate_enabled": False,
            "audit_sync_semantics": "existing SQLite audit and sync_queue writes remain authoritative",
        },
        "candidate": {
            "backend": config.candidate_backend,
            "database_url_configured": config.candidate_database_url_configured,
            "database_url_env": KANBAN_CANDIDATE_DATABASE_URL_ENV,
            "backend_env": KANBAN_CANDIDATE_BACKEND_ENV,
            "bootstrap_dry_run_supported": True,
            "bootstrap_apply_supported": False,
        },
        "safety": {
            "destructive": False,
            "sqlite_rows_retained": True,
            "sqlite_reads_retained": True,
            "sqlite_writes_retained": True,
            "cutover_requires_separate_backup_parity_and_rollback_proof": True,
        },
        "tables": list(KANBAN_DATASTORE_TABLES),
    }


def _row_get(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        return getattr(row, key)


def _postgres_bootstrap_sql(sql: str) -> str:
    """Translate current SQLite DDL into a conservative Postgres dry-run statement."""

    statement = sql.strip().rstrip(";")
    statement = re.sub(
        r"DEFAULT\s+\(datetime\('now'\)\)",
        "DEFAULT (CURRENT_TIMESTAMP::text)",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(r"\bAUTOINCREMENT\b", "", statement, flags=re.IGNORECASE)
    return f"{statement};"


def _kanban_schema_rows(conn: Any) -> list[Any]:
    return conn.execute(
        """
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('table', 'index')
          AND (name LIKE 'kanban_%' OR tbl_name LIKE 'kanban_%')
        ORDER BY
          CASE type WHEN 'table' THEN 0 ELSE 1 END,
          name
        """
    ).fetchall()


def kanban_datastore_bootstrap_plan(
    conn: Any,
    config: KanbanDatastoreConfig,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    """Build a candidate-store schema plan without mutating live or candidate data."""

    if apply:
        raise KanbanDatastoreConfigError(
            "Kanban candidate datastore bootstrap apply is disabled until the cutover "
            "cards prove backup, parity, rollback, API, helper, and browser behavior."
        )

    schema_rows = _kanban_schema_rows(conn)
    tables = sorted(
        str(_row_get(row, "name")) for row in schema_rows if str(_row_get(row, "type")) == "table"
    )
    missing_tables = [table for table in KANBAN_DATASTORE_TABLES if table not in tables]
    statements = [
        {
            "type": str(_row_get(row, "type")),
            "name": str(_row_get(row, "name")),
            "table": str(_row_get(row, "tbl_name")),
            "sql": _postgres_bootstrap_sql(str(_row_get(row, "sql"))),
        }
        for row in schema_rows
    ]
    warnings: list[str] = []
    if missing_tables:
        warnings.append(
            "current SQLite schema is missing expected Kanban tables: " + ", ".join(missing_tables)
        )
    if not config.candidate_database_url_configured:
        warnings.append(
            f"{KANBAN_CANDIDATE_DATABASE_URL_ENV} is not configured; returning dry-run SQL only."
        )

    return {
        "ok": True,
        "schema": KANBAN_DATASTORE_BOOTSTRAP_SCHEMA,
        "applied": False,
        "dry_run": True,
        "active_store": config.active_store,
        "candidate_backend": config.candidate_backend,
        "candidate_database_url_configured": config.candidate_database_url_configured,
        "apply_supported": False,
        "tables": tables,
        "expected_tables": list(KANBAN_DATASTORE_TABLES),
        "missing_tables": missing_tables,
        "statement_count": len(statements),
        "statements": statements,
        "warnings": warnings,
        "safety": {
            "destructive": False,
            "live_reads_changed": False,
            "live_writes_changed": False,
            "sqlite_rows_retained": True,
            "sync_queue_rows_created": False,
        },
    }
