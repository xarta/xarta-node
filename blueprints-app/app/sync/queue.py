"""
sync/queue.py — persistent FIFO action queue backed by SQLite.

One queue row per (target_node_id, action). Queues survive crashes because
they live in the same WAL-mode DB as the main data. The drain loop marks
rows as sent=1 on success. Sent rows may be pruned by explicit maintenance
after backup/proof; unsent rows are never pruned by retention helpers.

Queue overflow is handled by throttled row-action drain. Full-DB restore is a
separate recovery or explicit force-restore path, not a queue relief mechanism.
"""

import json
import logging
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

from .. import config as cfg
from ..db import get_conn
from ..kanban_datastore import ACTIVE_STORE_POSTGRES, KANBAN_DATASTORE_TABLES

log = logging.getLogger(__name__)

_SYSTEM_ACTION_TYPES = ("sync_git_outer", "sync_git_non_root", "sync_git_inner")
_KANBAN_FLEET_SYNC_TABLES = frozenset(KANBAN_DATASTORE_TABLES)
_MARK_SENT_BUSY_TIMEOUT_MS = 50


def _utc_sqlite_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _sqlite_database_locked(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


@contextmanager
def _mark_sent_nowait_conn() -> Any:
    conn = sqlite3.connect(
        cfg.DB_PATH,
        timeout=_MARK_SENT_BUSY_TIMEOUT_MS / 1000,
        check_same_thread=False,
    )
    try:
        conn.execute(f"PRAGMA busy_timeout={_MARK_SENT_BUSY_TIMEOUT_MS}")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def kanban_table_fleet_sync_disabled(table_name: str) -> bool:
    """Return True when active Postgres makes Kanban SQLite row sync obsolete."""
    config = getattr(cfg, "KANBAN_DATASTORE_CONFIG", None)
    return (
        str(table_name or "") in _KANBAN_FLEET_SYNC_TABLES
        and getattr(config, "active_store", None) == ACTIVE_STORE_POSTGRES
    )


def _sync_queue_columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(sync_queue)").fetchall()}


def _sent_time_expr(conn: sqlite3.Connection) -> str:
    columns = _sync_queue_columns(conn)
    if "sent_at" in columns:
        return "COALESCE(NULLIF(sent_at, ''), created_at)"
    return "created_at"


def _queue_table_stats(conn: sqlite3.Connection) -> dict[str, Any]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN sent=0 THEN 1 ELSE 0 END), 0) AS unsent,
            COALESCE(SUM(CASE WHEN sent=1 THEN 1 ELSE 0 END), 0) AS sent,
            MIN(created_at) AS oldest_created_at,
            MAX(created_at) AS newest_created_at
        FROM sync_queue
        """
    ).fetchone()
    per_target_rows = conn.execute(
        """
        SELECT target_node_id, sent, COUNT(*) AS row_count
        FROM sync_queue
        GROUP BY target_node_id, sent
        ORDER BY target_node_id, sent
        """
    ).fetchall()
    return {
        "total": int(row["total"] or 0),
        "unsent": int(row["unsent"] or 0),
        "sent": int(row["sent"] or 0),
        "oldest_created_at": row["oldest_created_at"] or "",
        "newest_created_at": row["newest_created_at"] or "",
        "per_target": [
            {
                "target_node_id": target["target_node_id"],
                "sent": int(target["sent"] or 0),
                "row_count": int(target["row_count"] or 0),
            }
            for target in per_target_rows
        ],
    }


def _eligible_sent_where(
    conn: sqlite3.Connection,
    *,
    older_than_hours: int,
) -> tuple[str, list[Any], str | None]:
    if older_than_hours < 0:
        raise ValueError("older_than_hours must be >= 0")
    where = ["sent=1"]
    params: list[Any] = []
    cutoff_at: str | None = None
    if older_than_hours > 0:
        cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=older_than_hours)
        cutoff_at = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        where.append(f"datetime({_sent_time_expr(conn)}) <= datetime(?)")
        params.append(cutoff_at)
    return " AND ".join(where), params, cutoff_at


def _sent_queue_retention_summary_for_conn(
    conn: sqlite3.Connection,
    *,
    older_than_hours: int,
    limit: int,
) -> dict[str, Any]:
    where_sql, params, cutoff_at = _eligible_sent_where(
        conn,
        older_than_hours=older_than_hours,
    )
    eligible_row = conn.execute(
        f"SELECT COUNT(*) FROM sync_queue WHERE {where_sql}",
        params,
    ).fetchone()
    eligible_count = int(eligible_row[0] if eligible_row else 0)
    stats = _queue_table_stats(conn)
    has_sent_at = "sent_at" in _sync_queue_columns(conn)
    selected_count = min(eligible_count, limit) if limit else eligible_count
    return {
        "schema": "xarta.sync_queue.retention_status.v1",
        "older_than_hours": older_than_hours,
        "cutoff_at": cutoff_at or "",
        "limit": limit,
        "has_sent_at": has_sent_at,
        "eligible_sent_rows": eligible_count,
        "selected_sent_rows": selected_count,
        "would_delete": selected_count,
        "queue": stats,
    }


def get_sent_queue_retention_summary(
    *,
    older_than_hours: int = 24,
    limit: int = 0,
) -> dict[str, Any]:
    """Return queue-retention status without deleting rows."""
    if limit < 0:
        raise ValueError("limit must be >= 0")
    with get_conn() as conn:
        return _sent_queue_retention_summary_for_conn(
            conn,
            older_than_hours=older_than_hours,
            limit=limit,
        )


def prune_sent_actions(
    *,
    older_than_hours: int = 24,
    limit: int = 0,
    apply: bool = False,
) -> dict[str, Any]:
    """Delete only sent sync_queue rows that match the retention window."""
    if limit < 0:
        raise ValueError("limit must be >= 0")
    with get_conn() as conn:
        before = _sent_queue_retention_summary_for_conn(
            conn,
            older_than_hours=older_than_hours,
            limit=limit,
        )
        deleted = 0
        if apply and before["selected_sent_rows"]:
            where_sql, params, _cutoff_at = _eligible_sent_where(
                conn,
                older_than_hours=older_than_hours,
            )
            if limit:
                cur = conn.execute(
                    f"""
                    DELETE FROM sync_queue
                    WHERE queue_id IN (
                        SELECT queue_id
                        FROM sync_queue
                        WHERE {where_sql}
                        ORDER BY queue_id
                        LIMIT ?
                    )
                    """,
                    [*params, limit],
                )
            else:
                cur = conn.execute(
                    f"DELETE FROM sync_queue WHERE {where_sql}",
                    params,
                )
            deleted = int(cur.rowcount or 0)
        after = _sent_queue_retention_summary_for_conn(
            conn,
            older_than_hours=older_than_hours,
            limit=limit,
        )
    return {
        "schema": "xarta.sync_queue.prune_sent.v1",
        "apply": apply,
        "deleted_rows": deleted,
        "before": before,
        "after": after,
    }


# ── Enqueue ───────────────────────────────────────────────────────────────────


def enqueue(
    conn: sqlite3.Connection,
    target_node_id: str,
    action_type: str,
    table_name: str,
    row_id: str,
    row_data: dict[str, Any] | None,
    gen: int,
    guid: str | None = None,
) -> None:
    """
    Append one action to a peer's queue within an open transaction.
    Must be called from inside a get_conn() context after a write.
    """
    if kanban_table_fleet_sync_disabled(table_name):
        log.debug(
            "skipping Kanban SQLite mirror sync enqueue for %s/%s while active_store=%s",
            table_name,
            row_id,
            ACTIVE_STORE_POSTGRES,
        )
        return
    if guid is None:
        guid = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO sync_queue
            (target_node_id, action_type, table_name, row_id, row_data, gen, guid)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target_node_id,
            action_type,
            table_name,
            row_id,
            json.dumps(row_data) if row_data is not None else None,
            gen,
            guid,
        ),
    )


def enqueue_for_all_peers(
    conn: sqlite3.Connection,
    action_type: str,
    table_name: str,
    row_id: str,
    row_data: dict[str, Any] | None,
    gen: int | None,
    exclude_node_id: str | None = None,
    guid: str | None = None,
) -> None:
    """
    Enqueue a write action for every registered peer node.

    A single GUID is generated (or accepted from the caller) and shared
    across all per-peer queue entries — this lets receiving nodes deduplicate
    forwarded copies via sync_seen_guids.

    exclude_node_id: optionally skip one node when a caller has a separate,
    explicit plan for seeding or recovering that node.
    """
    if kanban_table_fleet_sync_disabled(table_name):
        log.debug(
            "skipping Kanban SQLite mirror sync fanout for %s/%s while active_store=%s",
            table_name,
            row_id,
            ACTIVE_STORE_POSTGRES,
        )
        return
    if gen is None:
        raise ValueError("sync generation is required for SQLite sync fanout")
    shared_guid = guid if guid is not None else uuid.uuid4().hex
    try:
        peer_rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_id != ?", (cfg.NODE_ID,)
        ).fetchall()
    except Exception:
        log.exception("could not fetch peer nodes for enqueue")
        return

    for row in peer_rows:
        peer_id = row["node_id"]
        if peer_id == exclude_node_id:
            continue
        try:
            enqueue(conn, peer_id, action_type, table_name, row_id, row_data, gen, guid=shared_guid)
        except Exception:
            log.exception("failed to enqueue action for peer %s", peer_id)


# ── Query ─────────────────────────────────────────────────────────────────────


def get_pending_actions(target_node_id: str, limit: int = 50) -> list[dict]:
    """Return pending actions for a peer.

    Git-pull system actions get priority over regular DB writes and are
    returned alone. A stale peer should pull newer code before it receives
    any more data rows from that newer runtime.
    """
    with get_conn() as conn:
        system_rows = conn.execute(
            """
            SELECT queue_id, action_type, table_name, row_id, row_data, gen, guid
            FROM   sync_queue
            WHERE  target_node_id=? AND sent=0
              AND  action_type IN (?, ?, ?)
            ORDER  BY queue_id ASC
            LIMIT  ?
            """,
            (target_node_id, *_SYSTEM_ACTION_TYPES, limit),
        ).fetchall()
        if system_rows:
            return [dict(r) for r in system_rows]

        rows = conn.execute(
            """
            SELECT queue_id, action_type, table_name, row_id, row_data, gen, guid
            FROM   sync_queue
            WHERE  target_node_id=? AND sent=0
            ORDER  BY queue_id ASC
            LIMIT  ?
            """,
            (target_node_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def get_queue_depth(target_node_id: str) -> int:
    """Return count of unsent actions for a given peer."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM sync_queue WHERE target_node_id=? AND sent=0",
            (target_node_id,),
        ).fetchone()
    return int(row[0]) if row else 0


def get_queue_depths(peer_ids: list[str]) -> dict[str, int]:
    """Return {node_id: pending_count} for a list of peer IDs."""
    return {pid: get_queue_depth(pid) for pid in peer_ids}


def _mark_sent_with_conn(conn: sqlite3.Connection, queue_ids: list[int]) -> None:
    if not queue_ids:
        return
    placeholders = ", ".join("?" * len(queue_ids))
    try:
        conn.execute(
            f"UPDATE sync_queue SET sent=1, sent_at=? WHERE queue_id IN ({placeholders})",
            [_utc_sqlite_now(), *queue_ids],
        )
    except sqlite3.OperationalError as exc:
        if "sent_at" not in str(exc):
            raise
        conn.execute(
            f"UPDATE sync_queue SET sent=1 WHERE queue_id IN ({placeholders})",
            queue_ids,
        )


def mark_sent(queue_ids: list[int]) -> None:
    """Mark a batch of queue_ids as sent=1."""
    with get_conn() as conn:
        _mark_sent_with_conn(conn, queue_ids)


def try_mark_sent(queue_ids: list[int]) -> bool:
    """Best-effort sent marker for background drain; returns False if SQLite is busy."""
    if not queue_ids:
        return True
    try:
        with _mark_sent_nowait_conn() as conn:
            _mark_sent_with_conn(conn, queue_ids)
    except sqlite3.OperationalError as exc:
        if not _sqlite_database_locked(exc):
            raise
        log.warning(
            "sync_queue mark-sent deferred: sqlite writer busy for %d row(s); will retry",
            len(queue_ids),
        )
        return False
    return True


def get_peers_with_pending() -> list[str]:
    """Return distinct node IDs that have at least one unsent action."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT DISTINCT target_node_id FROM sync_queue WHERE sent=0"
        ).fetchall()
    return [r["target_node_id"] for r in rows]


def purge_unsent_db_actions(target_node_id: str) -> int:
    """Mark all unsent DB-write actions for a peer as sent (purge).

    System actions (sync_git_*) are preserved — they must always be delivered
    so a newer node can tell a stale peer to git-pull.

    Returns the number of rows purged.
    """
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE sync_queue SET sent=1 "
            "WHERE target_node_id=? AND sent=0 "
            "AND action_type NOT IN (?, ?, ?)",
            (target_node_id, *_SYSTEM_ACTION_TYPES),
        )
    return cur.rowcount


def get_peer_url(node_id: str) -> str | None:
    """Look up the first address URL for a registered peer node."""
    with get_conn() as conn:
        row = conn.execute("SELECT addresses FROM nodes WHERE node_id=?", (node_id,)).fetchone()
    if not row or not row["addresses"]:
        return None
    try:
        addrs = json.loads(row["addresses"])
        return addrs[0].rstrip("/") if addrs else None
    except (json.JSONDecodeError, TypeError, IndexError):
        return None


def get_peer_urls(node_id: str) -> list[str]:
    """Return the ordered sync URL list for a peer from config.

    Uses PEER_SYNC_URLS (built at startup from .nodes.json) which applies the
    same-tailnet filter: primary LAN (primary_ip) first, tailnet fallback only
    when both this node and the peer share the same tailnet string.

    Returns an empty list if the node_id is not a configured sync peer (e.g.
    it was removed from .nodes.json without an app restart).
    """
    return list(cfg.PEER_SYNC_URLS.get(node_id, []))
