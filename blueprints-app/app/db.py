"""
db.py — SQLite initialisation, schema, integrity check, and connection helper.

WAL mode + synchronous=NORMAL gives crash-safe atomic commits with good
read concurrency. The generation counter in sync_meta provides a total
ordering across all committed writes — critical for the sync engine.
"""

import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

from . import config as cfg

log = logging.getLogger(__name__)

# ── Schema DDL ────────────────────────────────────────────────────────────────
_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS machines (
    machine_id        TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    type              TEXT NOT NULL,
    parent_machine_id TEXT,
    ip_addresses      TEXT,
    description       TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS services (
    service_id        TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    description       TEXT,
    host_machine      TEXT,
    vm_or_lxc         TEXT,
    ports             TEXT,
    caddy_routes      TEXT,
    dns_info          TEXT,
    credential_hints  TEXT,
    dependencies      TEXT,
    project_status    TEXT DEFAULT 'deployed',
    tags              TEXT,
    links             TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    host_machine TEXT,
    tailnet      TEXT,
    addresses    TEXT,
    ui_url       TEXT,
    last_seen    TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sync_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_queue (
    queue_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    target_node_id TEXT    NOT NULL,
    action_type    TEXT    NOT NULL,
    table_name     TEXT    NOT NULL,
    row_id         TEXT    NOT NULL,
    row_data       TEXT,
    gen            INTEGER NOT NULL,
    created_at     TEXT    DEFAULT (datetime('now')),
    sent           INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_target
    ON sync_queue(target_node_id, sent, queue_id);

CREATE TABLE IF NOT EXISTS pfsense_dns (
    dns_entry_id  TEXT PRIMARY KEY,
    ip_address    TEXT NOT NULL,
    fqdn          TEXT NOT NULL,
    record_type   TEXT,
    source        TEXT,
    mac_address   TEXT,
    active        INTEGER DEFAULT 1,
    last_seen     TEXT DEFAULT (datetime('now')),
    last_probed   TEXT DEFAULT (datetime('now')),
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pfsense_dns_ip
    ON pfsense_dns(ip_address);
CREATE INDEX IF NOT EXISTS idx_pfsense_dns_fqdn
    ON pfsense_dns(fqdn);
"""

_SEED_SQL = """
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gen',             '0');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('integrity_ok',   'true');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_at',  datetime('now'));
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_by',  'system');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gui_version',    'initial');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_node', '');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_at',   '');
"""


# ── Public API ────────────────────────────────────────────────────────────────

def _run_migrations(conn: sqlite3.Connection) -> None:
    """Idempotent ALTER TABLE migrations for columns added after initial deploy."""
    migrations = [
        ("nodes",    "ui_url",                "TEXT"),
        # ── Phase-1 schema evolution (2026-03-11) ─────────────────────────
        # services: structured hosting, classification, health, flexibility
        ("services", "host_machine_id",       "TEXT"),
        ("services", "service_kind",          "TEXT DEFAULT 'app'"),
        ("services", "exposure_level",        "TEXT DEFAULT 'internal'"),
        ("services", "health_path",           "TEXT"),
        ("services", "health_expected_status", "INTEGER DEFAULT 200"),
        ("services", "runtime_notes_json",    "TEXT"),
        # machines: richer type taxonomy, platform, status, extensibility
        ("machines", "machine_kind",          "TEXT"),
        ("machines", "platform",              "TEXT"),
        ("machines", "status",                "TEXT DEFAULT 'active'"),
        ("machines", "labels",                "TEXT"),
        ("machines", "properties_json",       "TEXT"),
        # nodes: canonical machine mapping
        ("nodes",    "machine_id",            "TEXT"),
    ]
    existing_cols: dict[str, set[str]] = {}
    for table, column, col_type in migrations:
        if table not in existing_cols:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing_cols[table] = {r[1] for r in rows}
        if column not in existing_cols[table]:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            log.info("migration: added column %s.%s", table, column)


def init_db() -> None:
    """Create schema, run migrations, and seed sync_meta on first use."""
    os.makedirs(cfg.DB_DIR, exist_ok=True)
    with sqlite3.connect(cfg.DB_PATH) as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_SEED_SQL)
        _run_migrations(conn)
    log.info("database initialised at %s", cfg.DB_PATH)


def check_integrity() -> bool:
    """
    Run PRAGMA integrity_check against the main DB.
    Persists the result into sync_meta['integrity_ok'].
    Returns True if the DB is healthy.
    """
    try:
        with sqlite3.connect(cfg.DB_PATH) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        ok = bool(row and row[0] == "ok")
    except Exception:
        log.exception("integrity_check failed with exception")
        ok = False

    flag = "true" if ok else "false"
    with get_conn() as conn:
        conn.execute(
            "UPDATE sync_meta SET value=? WHERE key='integrity_ok'", (flag,)
        )
    if not ok:
        log.error("DB integrity check FAILED — node will NOT sync out to peers")
    return ok


@contextmanager
def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    Yield a WAL-mode SQLite connection with row_factory set.
    Commits on clean exit; rolls back on exception.
    """
    conn = sqlite3.connect(cfg.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def increment_gen(conn: sqlite3.Connection, source: str = "human") -> int:
    """
    Atomically increment the generation counter within an open transaction.
    Call this inside any write that should be replicated to peers.
    Returns the new gen value.
    """
    conn.execute(
        "UPDATE sync_meta "
        "SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key = 'gen'"
    )
    conn.execute(
        "UPDATE sync_meta SET value=datetime('now') WHERE key='last_write_at'"
    )
    conn.execute(
        "UPDATE sync_meta SET value=? WHERE key='last_write_by'", (source,)
    )
    row = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'"
    ).fetchone()
    return int(row[0]) if row else 0


def get_gen(conn: sqlite3.Connection) -> int:
    """Return current generation counter (read-only)."""
    row = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'"
    ).fetchone()
    return int(row[0]) if row else 0


def get_meta(conn: sqlite3.Connection, key: str) -> str:
    """Return a sync_meta value by key, or empty string if missing."""
    row = conn.execute(
        "SELECT value FROM sync_meta WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else ""
