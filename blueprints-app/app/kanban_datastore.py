"""Kanban datastore configuration, status, and guarded candidate bootstrap.

Kanban can run in the original SQLite mode or in active Postgres mode. SQLite
rows are still retained as a rollback/archive mirror when Postgres is active,
but those mirror rows are not a fleet row-sync source.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

KANBAN_DATASTORE_CONFIG_SCHEMA = "xarta.kanban.datastore.config.v1"
KANBAN_DATASTORE_STATUS_SCHEMA = "xarta.kanban.datastore.status.v1"
KANBAN_DATASTORE_BOOTSTRAP_SCHEMA = "xarta.kanban.datastore.bootstrap.v1"
KANBAN_POSTGRES_DISTRIBUTION_SCHEMA = "xarta.kanban.postgres_distribution.v1"

KANBAN_DATASTORE_MODE_ENV = "BLUEPRINTS_KANBAN_DATASTORE_MODE"
KANBAN_READ_STORE_ENV = "BLUEPRINTS_KANBAN_READ_STORE"
KANBAN_CANDIDATE_BACKEND_ENV = "BLUEPRINTS_KANBAN_CANDIDATE_STORE_BACKEND"
KANBAN_CANDIDATE_DATABASE_URL_ENV = "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL"
KANBAN_POSTGRES_OWNER_NODE_ID_ENV = "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID"
KANBAN_NODE_ID_ENV = "BLUEPRINTS_NODE_ID"

ACTIVE_STORE_SQLITE = "sqlite"
ACTIVE_STORE_POSTGRES = "postgres"
CANDIDATE_READ_STORE_SHADOW = "candidate-shadow"
CANDIDATE_READ_STORE_POSTGRES = "candidate-postgres"
SUPPORTED_ACTIVE_STORES = {ACTIVE_STORE_SQLITE, ACTIVE_STORE_POSTGRES}
SUPPORTED_READ_STORES = {
    ACTIVE_STORE_SQLITE,
    ACTIVE_STORE_POSTGRES,
    CANDIDATE_READ_STORE_SHADOW,
    CANDIDATE_READ_STORE_POSTGRES,
}
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
    read_store: str
    candidate_backend: str
    candidate_database_url_configured: bool
    candidate_database_url: str = ""
    current_node_id: str = ""
    postgres_owner_node_id: str = ""


def _clean_env_value(value: str | None, default: str) -> str:
    return str(value if value is not None else default).strip().lower()


def _clean_node_id(value: str | None, default: str = "") -> str:
    return str(value if value is not None else default).strip()


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

    default_read_store = (
        ACTIVE_STORE_POSTGRES if active_store == ACTIVE_STORE_POSTGRES else ACTIVE_STORE_SQLITE
    )
    read_store = _clean_env_value(source.get(KANBAN_READ_STORE_ENV), default_read_store)
    if read_store not in SUPPORTED_READ_STORES:
        supported = ", ".join(sorted(SUPPORTED_READ_STORES))
        raise KanbanDatastoreConfigError(
            f"{KANBAN_READ_STORE_ENV}={read_store!r} is invalid. "
            f"Supported Kanban read stores in this slice: {supported}."
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

    candidate_database_url = str(source.get(KANBAN_CANDIDATE_DATABASE_URL_ENV, "")).strip()
    if active_store == ACTIVE_STORE_POSTGRES and not candidate_database_url:
        raise KanbanDatastoreConfigError(
            f"{KANBAN_CANDIDATE_DATABASE_URL_ENV} is required when "
            f"{KANBAN_DATASTORE_MODE_ENV}=postgres."
        )
    current_node_id = _clean_node_id(
        source.get(KANBAN_NODE_ID_ENV) or os.environ.get(KANBAN_NODE_ID_ENV, "")
    )
    explicit_postgres_owner_node_id = _clean_node_id(
        source.get(KANBAN_POSTGRES_OWNER_NODE_ID_ENV)
        or os.environ.get(KANBAN_POSTGRES_OWNER_NODE_ID_ENV, ""),
    )
    postgres_owner_node_id = explicit_postgres_owner_node_id
    if active_store == ACTIVE_STORE_POSTGRES and not postgres_owner_node_id:
        postgres_owner_node_id = current_node_id

    return KanbanDatastoreConfig(
        active_store=active_store,
        read_store=read_store,
        candidate_backend=candidate_backend,
        candidate_database_url_configured=bool(candidate_database_url),
        candidate_database_url=candidate_database_url,
        current_node_id=current_node_id,
        postgres_owner_node_id=postgres_owner_node_id,
    )


def _postgres_distribution_role(config: KanbanDatastoreConfig) -> str:
    is_owner = (
        bool(config.current_node_id)
        and bool(config.postgres_owner_node_id)
        and config.current_node_id == config.postgres_owner_node_id
    )
    if config.active_store == ACTIVE_STORE_POSTGRES:
        return "postgres-owner" if is_owner else "postgres-non-owner-warning"
    if is_owner:
        return "owner-sqlite-rollback-or-precutover"
    return "sqlite-peer"


def _kanban_postgres_distribution_status(config: KanbanDatastoreConfig) -> dict[str, Any]:
    active_postgres = config.active_store == ACTIVE_STORE_POSTGRES
    is_owner = (
        bool(config.current_node_id)
        and bool(config.postgres_owner_node_id)
        and config.current_node_id == config.postgres_owner_node_id
    )
    owner_writes = active_postgres and is_owner

    return {
        "schema": KANBAN_POSTGRES_DISTRIBUTION_SCHEMA,
        "current_node_id": config.current_node_id,
        "owner_node_id": config.postgres_owner_node_id,
        "owner_node_env": KANBAN_POSTGRES_OWNER_NODE_ID_ENV,
        "current_node_env": KANBAN_NODE_ID_ENV,
        "this_node_role": _postgres_distribution_role(config),
        "authority": {
            "canonical_owner_node_id": config.postgres_owner_node_id,
            "this_node_is_owner": is_owner,
            "reads_authoritative_postgres": active_postgres,
            "writes_authoritative_postgres": owner_writes,
            "write_authority": (
                "owner-local-postgres"
                if owner_writes
                else "sqlite-mode-or-non-owner-no-postgres-writes"
            ),
            "multi_writer_supported": False,
        },
        "service": {
            "stack_name": "blueprints-kanban-postgres",
            "stack_path": "/xarta-node/.lone-wolf/stacks/blueprints-kanban-postgres",
            "network_exposure": "loopback-only",
            "default_port": 15433,
            "credential_boundary": (
                "Postgres password and DATABASE_URL stay in node-local ignored config; "
                "status payloads never expose connection secrets."
            ),
        },
        "fleet": {
            "expected_owner_active_store": ACTIVE_STORE_POSTGRES,
            "expected_peer_active_store": ACTIVE_STORE_SQLITE,
            "kanban_sqlite_row_sync": (
                "disabled-for-kanban-tables-while-owner-postgres-active"
                if active_postgres
                else "normal-sqlite-sync-queue-while-sqlite-active"
            ),
            "peer_postgres_required_now": False,
            "code_distribution": "git/fleet-pull",
            "document_distribution": "xarta-kanban Syncthing folder for file-backed docs/images",
            "data_distribution": (
                "owner-local Postgres is canonical; use Kanban backup export/import "
                "for explicit peer restore/distribution until replication is separately "
                "implemented and proven."
            ),
        },
        "backup_restore": {
            "kanban_backup_api": "/api/v1/personal/kanban/backups",
            "full_blueprints_backup_api": "/api/v1/backup",
            "restore_requires_backup_before_import": True,
            "restore_files_supported": True,
            "sqlite_mirror_recovery": (
                "Use full Blueprints backup/restore or explicit Kanban import; do not "
                "drain Kanban SQLite mirror rows through fleet sync while Postgres is active."
            ),
        },
        "offline_and_conflicts": {
            "owner_offline": (
                "Peers keep their local SQLite snapshots but must not become autonomous "
                "Postgres writers without an operator-selected owner/failover action."
            ),
            "peer_offline": "No Kanban Postgres replication backlog is expected on peers yet.",
            "conflict_strategy": (
                "Single canonical writer. Multi-writer conflict resolution is not "
                "implemented; use backup/parity/rollback proof before imports or failover."
            ),
        },
        "rollback": {
            "sqlite_archive_mirror_retained": active_postgres,
            "read_rollback_env": KANBAN_READ_STORE_ENV,
            "read_rollback_value": ACTIVE_STORE_SQLITE,
            "destructive_delete_allowed": False,
        },
        "operator_safety": {
            "sqlite_rows_retained": True,
            "old_sqlite_rows_deletion_allowed": False,
            "requires_green_checks": [
                "backup",
                "restore",
                "parity",
                "rollback",
                "fleet_distribution",
                "operator_safety",
            ],
        },
        "proof_commands": [
            "curl -fsS http://127.0.0.1:8080/api/v1/personal/kanban/datastore/status | jq .",
            "curl -fsS http://127.0.0.1:8080/api/v1/personal/kanban/datastore/parity | jq .",
            "curl -fsS http://127.0.0.1:8080/api/v1/sync/status | jq .",
            (
                "docker exec blueprints-kanban-postgres psql -U blueprints_kanban "
                "-d blueprints_kanban -c 'select count(*) from kanban_items;'"
            ),
        ],
    }


def kanban_datastore_status(config: KanbanDatastoreConfig) -> dict[str, Any]:
    """Return operator/agent-visible status without exposing connection secrets."""

    active_postgres = config.active_store == ACTIVE_STORE_POSTGRES
    if active_postgres:
        candidate_mode = "active-postgres"
    elif config.read_store == CANDIDATE_READ_STORE_SHADOW:
        candidate_mode = "sqlite-shadow"
    elif config.read_store in {CANDIDATE_READ_STORE_POSTGRES, ACTIVE_STORE_POSTGRES}:
        candidate_mode = "postgres"
    else:
        candidate_mode = "disabled"

    return {
        "ok": True,
        "schema": KANBAN_DATASTORE_STATUS_SCHEMA,
        "config_schema": KANBAN_DATASTORE_CONFIG_SCHEMA,
        "active_store": config.active_store,
        "reads": {
            "store": ACTIVE_STORE_POSTGRES if active_postgres else config.read_store,
            "candidate_enabled": active_postgres or config.read_store != ACTIVE_STORE_SQLITE,
            "candidate_mode": candidate_mode,
            "read_store_env": KANBAN_READ_STORE_ENV,
        },
        "writes": {
            "store": ACTIVE_STORE_POSTGRES if active_postgres else ACTIVE_STORE_SQLITE,
            "candidate_enabled": active_postgres,
            "audit_sync_semantics": (
                "Postgres is authoritative for Kanban table writes; SQLite receives a "
                "rollback/archive mirror, but Kanban SQLite mirror rows are not enqueued "
                "for fleet sync"
                if active_postgres
                else "existing SQLite audit and sync_queue writes remain authoritative"
            ),
        },
        "candidate": {
            "backend": config.candidate_backend,
            "database_url_configured": config.candidate_database_url_configured,
            "database_url_env": KANBAN_CANDIDATE_DATABASE_URL_ENV,
            "backend_env": KANBAN_CANDIDATE_BACKEND_ENV,
            "bootstrap_dry_run_supported": True,
            "bootstrap_apply_supported": config.candidate_database_url_configured,
            "read_shadow_supported": True,
            "read_shadow_persistent": False,
            "read_postgres_supported": True,
            "read_postgres_persistent": True,
        },
        "safety": {
            "destructive": False,
            "sqlite_rows_retained": True,
            "sqlite_reads_retained": (
                not active_postgres and config.read_store == ACTIVE_STORE_SQLITE
            ),
            "sqlite_writes_retained": not active_postgres,
            "sqlite_archive_mirror_retained": active_postgres,
            "cutover_requires_separate_backup_parity_and_rollback_proof": not active_postgres,
        },
        "distribution": _kanban_postgres_distribution_status(config),
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
        r"^CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)",
        "CREATE TABLE IF NOT EXISTS ",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(
        r"^CREATE\s+UNIQUE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(
        r"^CREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS)",
        "CREATE INDEX IF NOT EXISTS ",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(
        r"DEFAULT\s+\(datetime\('now'\)\)",
        "DEFAULT (CURRENT_TIMESTAMP::text)",
        statement,
        flags=re.IGNORECASE,
    )
    statement = re.sub(r"\bAUTOINCREMENT\b", "", statement, flags=re.IGNORECASE)
    return f"{statement};"


def _kanban_schema_rows(conn: Any, *, support_tables: tuple[str, ...] = ()) -> list[Any]:
    support_filter = ""
    params: tuple[str, ...] = ()
    if support_tables:
        placeholders = ",".join("?" for _ in support_tables)
        support_filter = f" OR name IN ({placeholders}) OR tbl_name IN ({placeholders})"
        params = (*support_tables, *support_tables)
    return conn.execute(
        f"""
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('table', 'index')
          AND (name LIKE 'kanban_%' OR tbl_name LIKE 'kanban_%'{support_filter})
        ORDER BY
          CASE type WHEN 'table' THEN 0 ELSE 1 END,
          name
        """,
        params,
    ).fetchall()


def kanban_datastore_bootstrap_plan(
    conn: Any,
    config: KanbanDatastoreConfig,
    *,
    apply: bool = False,
    support_setting_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build or apply a guarded Postgres candidate-store schema/data bootstrap."""

    support_tables = ("settings",) if support_setting_keys else ()
    schema_rows = _kanban_schema_rows(conn, support_tables=support_tables)
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
    apply_supported = config.candidate_database_url_configured

    applied_result: dict[str, Any] | None = None
    if apply:
        if not apply_supported:
            raise KanbanDatastoreConfigError(
                f"{KANBAN_CANDIDATE_DATABASE_URL_ENV} is not configured; cannot apply "
                "Kanban Postgres candidate bootstrap."
            )
        try:
            from .kanban_postgres import bootstrap_postgres_candidate

            applied_result = bootstrap_postgres_candidate(
                conn,
                database_url=config.candidate_database_url,
                statements=statements,
                support_setting_keys=support_setting_keys,
            )
        except Exception as exc:
            raise KanbanDatastoreConfigError(
                f"Kanban Postgres candidate bootstrap failed: {exc}"
            ) from exc

    return {
        "ok": True,
        "schema": KANBAN_DATASTORE_BOOTSTRAP_SCHEMA,
        "applied": applied_result is not None,
        "dry_run": applied_result is None,
        "active_store": config.active_store,
        "candidate_backend": config.candidate_backend,
        "candidate_database_url_configured": config.candidate_database_url_configured,
        "apply_supported": apply_supported,
        "tables": tables,
        "expected_tables": list(KANBAN_DATASTORE_TABLES),
        "support_tables": list(support_tables),
        "missing_tables": missing_tables,
        "statement_count": len(statements),
        "statements": statements,
        "warnings": warnings,
        "result": applied_result or {},
        "safety": {
            "destructive": False,
            "live_reads_changed": False,
            "live_writes_changed": False,
            "sqlite_rows_retained": True,
            "candidate_data_replaced": applied_result is not None,
            "sync_queue_rows_created": False,
        },
    }
