"""
sync/drain.py — async queue-drain background task.

Wakes every 1–20 seconds (random jitter per the spec), checks for peers with
pending actions, and POSTs them in batches to each peer's /api/v1/sync/actions
endpoint.

Full-DB restore is deliberately not a queue pressure relief path: normal
catch-up must flow through row actions so per-row conflict guards still apply.

The drain task is started once at application startup via start_drain_loop().
"""

import asyncio
import json
import logging
import random
import sqlite3
import ssl
import time
from contextlib import contextmanager

import httpx

from .. import config as cfg
from .. import timing
from ..auth import compute_token
from ..db import get_conn, get_meta
from ..sync.queue import (
    get_peer_urls,
    get_peers_with_pending,
    get_pending_actions,
    get_queue_depth,
    try_mark_sent,
)

log = logging.getLogger(__name__)

_drain_task: asyncio.Task | None = None
_last_guid_cleanup: float = 0.0
_DRAIN_SQLITE_BUSY_TIMEOUT_MS = 50


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


async def stop_drain_loop() -> None:
    """Cancel the background queue-drain task and wait for it to exit."""
    global _drain_task
    if _drain_task is None:
        return
    task = _drain_task
    _drain_task = None
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        log.info("queue drain loop stopped")


# ── Internal ──────────────────────────────────────────────────────────────────


def _sqlite_database_locked(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return "database is locked" in message or "database is busy" in message


@contextmanager
def _drain_housekeeping_conn_nowait() -> object:
    conn = sqlite3.connect(
        cfg.DB_PATH,
        timeout=_DRAIN_SQLITE_BUSY_TIMEOUT_MS / 1000,
        check_same_thread=False,
    )
    try:
        conn.execute(f"PRAGMA busy_timeout={_DRAIN_SQLITE_BUSY_TIMEOUT_MS}")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _maybe_cleanup_guids() -> None:
    """Delete sync_seen_guids entries older than 3 days.

    Called once per drain cycle but rate-limited to at most once per hour.
    """
    global _last_guid_cleanup
    now = time.time()
    if now - _last_guid_cleanup < 3600:
        return
    cutoff = int(now) - 3 * 86400
    try:
        with _drain_housekeeping_conn_nowait() as conn:
            result = conn.execute("DELETE FROM sync_seen_guids WHERE received_at < ?", (cutoff,))
            n = result.rowcount
    except sqlite3.OperationalError as exc:
        if not _sqlite_database_locked(exc):
            raise
        log.warning("guid cleanup deferred: sqlite writer busy; will retry")
        return
    _last_guid_cleanup = now
    if n:
        log.info("guid cleanup: removed %d expired seen-GUID entries", n)


def _discard_self_target_actions() -> None:
    """Mark impossible self-target queue rows as sent.

    Normal enqueue paths exclude cfg.NODE_ID. If a standalone script runs
    without node identity and creates rows addressed to this node, those rows
    can never drain because self is not a configured peer.
    """
    try:
        with _drain_housekeeping_conn_nowait() as conn:
            result = conn.execute(
                "UPDATE sync_queue SET sent=1 WHERE target_node_id=? AND sent=0",
                (cfg.NODE_ID,),
            )
            discarded = result.rowcount
    except sqlite3.OperationalError as exc:
        if not _sqlite_database_locked(exc):
            raise
        log.warning("self-target sync_queue cleanup deferred: sqlite writer busy; will retry")
        return
    if discarded:
        log.warning("discarded %d self-targeted sync_queue action(s)", discarded)


def _drain_integrity_ok_sync() -> bool:
    with get_conn() as conn:
        return get_meta(conn, "integrity_ok") == "true"


def _drain_peer_payload_sync(node_id: str) -> tuple[int, list[int], dict[str, object] | None]:
    depth = get_queue_depth(node_id)
    actions = get_pending_actions(node_id, limit=cfg.SYNC_BATCH_SIZE)
    if not actions:
        return depth, [], None
    queue_ids = [a["queue_id"] for a in actions]
    payload = {
        "source_node_id": cfg.NODE_ID,
        "source_commit_ts": cfg.COMMIT_TS,
        "actions": [
            {
                "action_type": a["action_type"],
                "table_name": a["table_name"],
                "row_id": a["row_id"],
                "row_data": json.loads(a["row_data"]) if a["row_data"] else None,
                "gen": a["gen"],
                "source_node_id": cfg.NODE_ID,
                "guid": a.get("guid", ""),
            }
            for a in actions
        ],
    }
    return depth, queue_ids, payload


async def _drain_loop() -> None:
    """Main drain loop — runs indefinitely."""
    while True:
        try:
            await _drain_all_peers()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("unhandled error in drain loop — continuing")
        delay = random.randint(cfg.SYNC_DRAIN_INTERVAL_MIN, cfg.SYNC_DRAIN_INTERVAL_MAX)
        await asyncio.sleep(delay)


async def _drain_all_peers() -> None:
    """Find all peers with pending actions and drain each one."""
    with timing.span("background_task_cycle", task="sync_drain_all_peers"):
        # Guard: don't send anything if our own DB is degraded — we could
        # propagate corrupt state to healthy peers.
        if not await timing.to_thread("sync.drain_integrity_ok", _drain_integrity_ok_sync):
            log.warning("drain suspended: integrity_ok=false — waiting for recovery")
            return

        await timing.to_thread("sync.cleanup_guids", _maybe_cleanup_guids)
        await timing.to_thread("sync.discard_self_target_actions", _discard_self_target_actions)
        pending_peers = await timing.to_thread(
            "sync.get_peers_with_pending", get_peers_with_pending
        )
        if not pending_peers:
            return

        for node_id in pending_peers:
            peer_urls = await timing.to_thread("sync.get_peer_urls", get_peer_urls, node_id)
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

    If the depth is at/above SYNC_QUEUE_MAX_DEPTH, log the condition but keep
    sending row actions. A generation counter is not a whole-database freshness
    proof, so queue overflow must never trigger a DB replacement.
    """
    depth, queue_ids, payload = await timing.to_thread(
        "sync.drain_peer_payload",
        _drain_peer_payload_sync,
        node_id,
    )
    log.debug("draining peer %s: %d pending actions", node_id, depth)

    if depth >= cfg.SYNC_QUEUE_MAX_DEPTH:
        log.warning(
            "queue overflow for peer %s (depth=%d >= %d) — continuing batched row drain; "
            "full DB restore is recovery-only",
            node_id,
            depth,
            cfg.SYNC_QUEUE_MAX_DEPTH,
        )

    if not payload:
        return
    action_count = len(payload["actions"]) if isinstance(payload.get("actions"), list) else 0

    async with _make_sync_client(15.0) as client:
        for url in peer_urls:
            try:
                resp = await client.post(
                    f"{url}/api/v1/sync/actions",
                    json=payload,
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)}
                    if cfg.SYNC_SECRET
                    else {},
                )
                if resp.status_code == 204:
                    marked_sent = await timing.to_thread(
                        "sync.try_mark_sent", try_mark_sent, queue_ids
                    )
                    if not marked_sent:
                        log.debug(
                            "sent %d actions to peer %s via %s; mark-sent deferred until SQLite is free",
                            action_count,
                            node_id,
                            url,
                        )
                        return
                    log.debug(
                        "drained %d actions to peer %s via %s",
                        action_count,
                        node_id,
                        url,
                    )
                    return
                elif resp.status_code == 409:
                    # Commit guard: peer is on a newer commit and refused our
                    # data. Keep the rows queued so they can drain after this
                    # node pulls/restarts onto the same runtime as the peer.
                    # No point trying other addresses — the issue is a code
                    # mismatch, not connectivity.
                    log.warning(
                        "commit guard: peer %s rejected actions (409) — "
                        "leaving %d outgoing action(s) queued. Local code is behind.",
                        node_id,
                        action_count,
                    )
                    return
                else:
                    log.warning(
                        "peer %s rejected actions via %s: HTTP %d — trying next address",
                        node_id,
                        url,
                        resp.status_code,
                    )
            except httpx.ConnectError:
                log.debug("peer %s unreachable at %s — trying next address", node_id, url)
            except Exception:
                log.exception("error draining to peer %s at %s", node_id, url)
        log.debug("peer %s unreachable on all addresses — will retry next cycle", node_id)
