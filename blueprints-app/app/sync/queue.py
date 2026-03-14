"""
sync/queue.py — persistent FIFO action queue backed by SQLite.

One queue row per (target_node_id, action). Queues survive crashes because
they live in the same WAL-mode DB as the main data. The drain loop marks
rows as sent=1 on success; rows are never deleted (provides an audit trail
and allows re-inspection; the index keeps queries fast).

Queue overflow: if pending depth for a peer reaches SYNC_QUEUE_MAX_DEPTH,
the drain loop is responsible for switching to a full-DB backup send (handled
in drain.py). The queue functions here are pure enqueue/query helpers.
"""

import json
import logging
import sqlite3
from typing import Any

from .. import config as cfg
from ..db import get_conn

log = logging.getLogger(__name__)


# ── Enqueue ───────────────────────────────────────────────────────────────────

def enqueue(
    conn: sqlite3.Connection,
    target_node_id: str,
    action_type: str,
    table_name: str,
    row_id: str,
    row_data: dict[str, Any] | None,
    gen: int,
) -> None:
    """
    Append one action to a peer's queue within an open transaction.
    Must be called from inside a get_conn() context after a write.
    """
    conn.execute(
        """
        INSERT INTO sync_queue
            (target_node_id, action_type, table_name, row_id, row_data, gen)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            target_node_id,
            action_type,
            table_name,
            row_id,
            json.dumps(row_data) if row_data is not None else None,
            gen,
        ),
    )


def enqueue_for_all_peers(
    conn: sqlite3.Connection,
    action_type: str,
    table_name: str,
    row_id: str,
    row_data: dict[str, Any] | None,
    gen: int,
    exclude_node_id: str | None = None,
) -> None:
    """
    Enqueue a write action for every registered peer node.

    exclude_node_id: optionally skip one node (e.g. when registering that node
    itself — the new node will receive a full backup instead of incremental
    actions).
    """
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
            enqueue(conn, peer_id, action_type, table_name, row_id, row_data, gen)
        except Exception:
            log.exception("failed to enqueue action for peer %s", peer_id)


# ── Query ─────────────────────────────────────────────────────────────────────

def get_pending_actions(target_node_id: str, limit: int = 50) -> list[dict]:
    """Return the oldest unsent actions for a peer, in FIFO order."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT queue_id, action_type, table_name, row_id, row_data, gen
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


def mark_sent(queue_ids: list[int]) -> None:
    """Mark a batch of queue_ids as sent=1."""
    if not queue_ids:
        return
    placeholders = ", ".join("?" * len(queue_ids))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE sync_queue SET sent=1 WHERE queue_id IN ({placeholders})",
            queue_ids,
        )


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
            "AND action_type NOT IN ('sync_git_outer', 'sync_git_inner')",
            (target_node_id,),
        )
    return cur.rowcount


def get_peer_url(node_id: str) -> str | None:
    """Look up the first address URL for a registered peer node."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT addresses FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
    if not row or not row["addresses"]:
        return None
    try:
        addrs = json.loads(row["addresses"])
        return addrs[0].rstrip("/") if addrs else None
    except (json.JSONDecodeError, TypeError, IndexError):
        return None
