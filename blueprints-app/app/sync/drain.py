"""
sync/drain.py — async queue-drain background task.

Wakes every 1–20 seconds (random jitter per the spec), checks for peers with
pending actions, and POSTs them in batches to each peer's /api/v1/sync/actions
endpoint.

If a peer's queue depth is at or above SYNC_QUEUE_MAX_DEPTH, a full DB backup
is sent instead (via the Layer 1 restore endpoint) and the queue is cleared.

The drain task is started once at application startup via start_drain_loop().
"""

import asyncio
import json
import logging
import random

import httpx

from .. import config as cfg
from ..auth import compute_token
from ..db import get_conn, get_meta
from ..sync.queue import (
    get_peers_with_pending,
    get_pending_actions,
    get_queue_depth,
    get_peer_url,
    mark_sent,
    purge_unsent_db_actions,
)
from ..sync.restore import make_full_backup

log = logging.getLogger(__name__)

_drain_task: asyncio.Task | None = None


# ── mTLS client factory ──────────────────────────────────────────────────────

def _make_sync_client(timeout: float) -> httpx.AsyncClient:
    """Return an httpx.AsyncClient configured for sync drain requests.

    When SYNC_TLS_CA/CERT/KEY are all set in config, the client uses mTLS:
      - server cert is verified against the fleet CA (not the system bundle)
      - this node's cert+key are presented as the client certificate

    When any of the three are unset/empty, falls back to plain HTTP (no TLS).
    """
    if cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY:
        return httpx.AsyncClient(
            timeout=timeout,
            verify=cfg.SYNC_TLS_CA,
            cert=(cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY),
        )
    return httpx.AsyncClient(timeout=timeout)


async def start_drain_loop() -> None:
    """Start the background queue-drain task (idempotent)."""
    global _drain_task
    if _drain_task is None or _drain_task.done():
        _drain_task = asyncio.create_task(_drain_loop())
        log.info("queue drain loop started")


# ── Internal ──────────────────────────────────────────────────────────────────

async def _drain_loop() -> None:
    """Main drain loop — runs indefinitely."""
    while True:
        try:
            await _drain_all_peers()
        except Exception:
            log.exception("unhandled error in drain loop — continuing")
        delay = random.randint(cfg.SYNC_DRAIN_INTERVAL_MIN, cfg.SYNC_DRAIN_INTERVAL_MAX)
        await asyncio.sleep(delay)


async def _drain_all_peers() -> None:
    """Find all peers with pending actions and drain each one."""
    # Guard: don't send anything if our own DB is degraded — we could
    # propagate corrupt state to healthy peers.
    with get_conn() as conn:
        if get_meta(conn, "integrity_ok") != "true":
            log.warning("drain suspended: integrity_ok=false — waiting for recovery")
            return

    pending_peers = get_peers_with_pending()
    if not pending_peers:
        return

    for node_id in pending_peers:
        peer_url = get_peer_url(node_id)
        if not peer_url:
            log.debug("no URL for peer %s — skipping drain", node_id)
            continue
        try:
            await _drain_peer(node_id, peer_url)
        except Exception:
            log.exception("drain failed for peer %s (%s)", node_id, peer_url)


async def _drain_peer(node_id: str, peer_url: str) -> None:
    """
    Drain the action queue for one peer.

    If the depth is at/above SYNC_QUEUE_MAX_DEPTH, send a full backup instead
    and mark all pending items as sent (they will be covered by the backup).
    """
    depth = get_queue_depth(node_id)
    log.debug("draining peer %s: %d pending actions", node_id, depth)

    if depth >= cfg.SYNC_QUEUE_MAX_DEPTH:
        log.warning(
            "queue overflow for peer %s (depth=%d) — sending full backup",
            node_id,
            depth,
        )
        await _send_full_backup(node_id, peer_url)
        return

    actions = get_pending_actions(node_id, limit=cfg.SYNC_BATCH_SIZE)
    if not actions:
        return

    payload = {
        "source_node_id": cfg.NODE_ID,
        "source_commit_ts": cfg.COMMIT_TS,
        "actions": [
            {
                "action_type": a["action_type"],
                "table_name":  a["table_name"],
                "row_id":      a["row_id"],
                "row_data":    json.loads(a["row_data"]) if a["row_data"] else None,
                "gen":         a["gen"],
                "source_node_id": cfg.NODE_ID,
            }
            for a in actions
        ],
    }

    try:
        async with _make_sync_client(15.0) as client:
            resp = await client.post(
                f"{peer_url}/api/v1/sync/actions",
                json=payload,
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
        if resp.status_code == 204:
            queue_ids = [a["queue_id"] for a in actions]
            mark_sent(queue_ids)
            log.debug(
                "drained %d actions to peer %s", len(actions), node_id
            )
        elif resp.status_code == 409:
            # Commit guard: peer is on a newer commit and refused our data.
            # Purge all unsent DB-write actions for this peer (they carry
            # stale-schema data). System actions are preserved.
            purged = purge_unsent_db_actions(node_id)
            log.warning(
                "commit guard: peer %s rejected actions (409) — "
                "purged %d outgoing DB actions. Local code is behind.",
                node_id,
                purged,
            )
        else:
            log.warning(
                "peer %s rejected actions: HTTP %d — will retry",
                node_id,
                resp.status_code,
            )
    except httpx.ConnectError:
        log.debug("peer %s unreachable — will retry later", node_id)
    except Exception:
        log.exception("error draining to peer %s", node_id)


async def _send_full_backup(node_id: str, peer_url: str) -> None:
    """Send a full DB backup zip to a peer's Layer 1 restore endpoint."""
    try:
        zip_bytes, sha256_hex = make_full_backup()
    except Exception:
        log.exception("failed to create full backup for peer %s", node_id)
        return

    try:
        async with _make_sync_client(60.0) as client:
            _restore_headers = {
                "content-type": "application/octet-stream",
                "x-blueprints-checksum": sha256_hex,
            }
            if cfg.SYNC_SECRET:
                _restore_headers["x-api-token"] = compute_token(cfg.SYNC_SECRET)
            resp = await client.post(
                f"{peer_url}/api/v1/sync/restore",
                content=zip_bytes,
                headers=_restore_headers,
            )
        if resp.status_code == 204:
            log.info("full backup sent to peer %s", node_id)
            # Mark all pending actions as sent — the backup supersedes them
            from ..db import get_conn
            with get_conn() as conn:
                conn.execute(
                    "UPDATE sync_queue SET sent=1 WHERE target_node_id=? AND sent=0",
                    (node_id,),
                )
        else:
            log.warning(
                "peer %s rejected full backup: HTTP %d",
                node_id,
                resp.status_code,
            )
    except httpx.ConnectError:
        log.debug("peer %s unreachable for full backup — will retry", node_id)
    except Exception:
        log.exception("error sending full backup to peer %s", node_id)
