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
import ssl

import httpx

from .. import config as cfg
from ..auth import compute_token
from ..db import get_conn, get_meta
from ..sync.queue import (
    get_peers_with_pending,
    get_pending_actions,
    get_queue_depth,
    get_peer_urls,
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

    Note: ssl.SSLContext is built manually rather than passing string paths to
    httpx.  In httpx ≥0.28 passing verify=<str> triggers an early return that
    silently drops the cert= parameter, meaning no client cert is presented and
    Caddy rejects the connection with TLSV13_ALERT_CERTIFICATE_REQUIRED.
    """
    if cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cfg.SYNC_TLS_CA)
        ctx.load_cert_chain(cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY)
        return httpx.AsyncClient(timeout=timeout, verify=ctx)
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
        peer_urls = get_peer_urls(node_id)
        if not peer_urls:
            log.debug("no URLs configured for peer %s — skipping drain", node_id)
            continue
        try:
            await _drain_peer(node_id, peer_urls)
        except Exception:
            log.exception("drain failed for peer %s", node_id)


async def _drain_peer(node_id: str, peer_urls: list[str]) -> None:
    """
    Drain the action queue for one peer.

    Tries each URL in peer_urls in order (primary LAN first, tailnet
    fallback second).  Stops at the first successful connection.  If all
    addresses fail the peer is left queued and retried on the next drain cycle.

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
        await _send_full_backup(node_id, peer_urls)
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

    async with _make_sync_client(15.0) as client:
        for url in peer_urls:
            try:
                resp = await client.post(
                    f"{url}/api/v1/sync/actions",
                    json=payload,
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
                )
                if resp.status_code == 204:
                    queue_ids = [a["queue_id"] for a in actions]
                    mark_sent(queue_ids)
                    log.debug(
                        "drained %d actions to peer %s via %s",
                        len(actions), node_id, url,
                    )
                    return
                elif resp.status_code == 409:
                    # Commit guard: peer is on a newer commit and refused our
                    # data. Purge stale outgoing DB-write actions; system
                    # actions are preserved.  No point trying other addresses
                    # — the issue is a code mismatch, not connectivity.
                    purged = purge_unsent_db_actions(node_id)
                    log.warning(
                        "commit guard: peer %s rejected actions (409) — "
                        "purged %d outgoing DB actions. Local code is behind.",
                        node_id,
                        purged,
                    )
                    return
                else:
                    log.warning(
                        "peer %s rejected actions via %s: HTTP %d — trying next address",
                        node_id, url, resp.status_code,
                    )
            except httpx.ConnectError:
                log.debug("peer %s unreachable at %s — trying next address", node_id, url)
            except Exception:
                log.exception("error draining to peer %s at %s", node_id, url)
        log.debug("peer %s unreachable on all addresses — will retry next cycle", node_id)


async def _send_full_backup(node_id: str, peer_urls: list[str]) -> None:
    """Send a full DB backup zip to a peer's Layer 1 restore endpoint.

    Tries each URL in peer_urls in order, stopping at the first successful
    delivery.  If all addresses fail the peer remains queued for retry.
    """
    try:
        zip_bytes, sha256_hex = make_full_backup()
    except Exception:
        log.exception("failed to create full backup for peer %s", node_id)
        return

    _restore_headers = {
        "content-type": "application/octet-stream",
        "x-blueprints-checksum": sha256_hex,
    }
    if cfg.SYNC_SECRET:
        _restore_headers["x-api-token"] = compute_token(cfg.SYNC_SECRET)

    async with _make_sync_client(60.0) as client:
        for url in peer_urls:
            try:
                resp = await client.post(
                    f"{url}/api/v1/sync/restore",
                    content=zip_bytes,
                    headers=_restore_headers,
                )
                if resp.status_code == 204:
                    log.info("full backup sent to peer %s via %s", node_id, url)
                    # Mark all pending actions as sent — the backup supersedes them
                    from ..db import get_conn
                    with get_conn() as conn:
                        conn.execute(
                            "UPDATE sync_queue SET sent=1 WHERE target_node_id=? AND sent=0",
                            (node_id,),
                        )
                    return
                else:
                    log.warning(
                        "peer %s rejected full backup via %s: HTTP %d — trying next address",
                        node_id, url, resp.status_code,
                    )
            except httpx.ConnectError:
                log.debug("peer %s unreachable at %s for full backup — trying next address", node_id, url)
            except Exception:
                log.exception("error sending full backup to peer %s at %s", node_id, url)
        log.debug("peer %s unreachable on all addresses for full backup — will retry", node_id)
