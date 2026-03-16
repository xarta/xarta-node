"""
routes_sync.py — endpoints that implement the Layer 1 + Layer 2 sync protocol.

POST /api/v1/sync/actions   — receive a batch of CRUD actions from a peer
                             (commit guard: rejects DB writes from older commits)
POST /api/v1/sync/restore   — receive a full DB backup zip (Layer 1) + SHA-256
GET  /api/v1/sync/export    — serve the current DB backup zip for a peer to pull
GET  /api/v1/sync/status    — current sync state summary
POST /api/v1/sync/git-pull  — trigger a git pull on this node + queue for peers
"""

import asyncio
import json
import logging
import os

from fastapi import APIRouter, HTTPException, Request, Response

from . import config as cfg
from .db import get_conn, get_gen, get_meta, increment_gen
from .models import GitPullRequest, SyncActionsPayload, SyncStatus
from .sync.queue import enqueue_for_all_peers, get_queue_depths
from .sync.restore import apply_restore, make_full_backup

log = logging.getLogger(__name__)

router = APIRouter(prefix="/sync", tags=["sync"])

# Tables that actions are permitted to touch (safeguard against bad payloads)
# NOTE: "nodes" is intentionally excluded — the nodes table is local-only,
# populated from .nodes.json on each node. Incoming sync entries for nodes
# are silently dropped to prevent stale peer data overwriting the JSON-derived
# addresses and config.
_ALLOWED_TABLES = {
    "services", "machines",
    "pfsense_dns",
    "proxmox_config", "proxmox_nets", "vlans", "dockge_stacks", "dockge_stack_services", "caddy_configs",
    "settings", "pve_hosts",
    "arp_manual",
    "ssh_targets",
    "manual_links",
}

# Action types that trigger local execution rather than a DB write
_SYSTEM_ACTION_TYPES = {"sync_git_outer", "sync_git_inner"}


# ── Git pull helpers ──────────────────────────────────────────────────────────

async def _git_pull_and_restart(repo_path: str, label: str) -> None:
    """Run git pull on a repo, then restart the service."""
    if not repo_path or not os.path.isdir(os.path.join(repo_path, ".git")):
        log.info("git pull [%s] skipped: no repo at %r", label, repo_path)
        return
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", repo_path, "pull", "--ff-only",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode == 0:
        result = stdout.decode().strip()
        log.info("git pull [%s]: %s", label, result)
        if cfg.SERVICE_RESTART_CMD:
            await _restart_service()
    else:
        log.warning("git pull [%s] failed: %s", label, stderr.decode().strip())


async def _restart_service() -> None:
    """Run SERVICE_RESTART_CMD, logging the outcome."""
    parts = cfg.SERVICE_RESTART_CMD.split()
    proc = await asyncio.create_subprocess_exec(
        *parts,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode == 0:
        log.info("service restart: %s", stdout.decode().strip() or "ok")
    else:
        log.warning("service restart failed: %s", stderr.decode().strip())


# ── Internal helper: apply one sync action ────────────────────────────────────

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
        "services":       "service_id",
        "machines":       "machine_id",
        "nodes":          "node_id",
        "pfsense_dns":    "dns_entry_id",
        "proxmox_config": "config_id",
        "proxmox_nets":   "net_id",
        "vlans":          "vlan_id",
        "dockge_stacks":         "stack_id",
        "dockge_stack_services":  "service_id",
        "caddy_configs":          "caddy_id",
        "settings":       "key",
        "pve_hosts":      "pve_id",
        "arp_manual":     "entry_id",
        "ssh_targets":    "ip_address",
        "manual_links":   "link_id",
    }
    return pk_map.get(table, "id")


# ── Routes ────────────────────────────────────────────────────────────────────

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

    # Schedule system actions immediately — always accepted regardless of
    # commit age so a newer node can tell an older one to git-pull.
    for action in system_actions:
        if action.action_type == "sync_git_outer":
            asyncio.create_task(_git_pull_and_restart(cfg.REPO_OUTER_PATH, "outer"))
            log.info("scheduled git pull [outer] from %s", payload.source_node_id)
        elif action.action_type == "sync_git_inner":
            asyncio.create_task(_git_pull_and_restart(cfg.REPO_INNER_PATH, "inner"))
            log.info("scheduled git pull [inner] from %s", payload.source_node_id)

    if not db_actions:
        return Response(status_code=204)

    # ── Commit guard: reject DB writes from older-commit peers ────────────
    if cfg.COMMIT_TS and payload.source_commit_ts:
        if payload.source_commit_ts < cfg.COMMIT_TS:
            log.warning(
                "commit guard: rejecting %d DB actions from %s "
                "(source_ts=%d < local_ts=%d)",
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

    with get_conn() as conn:
        # Check own integrity — if not OK, refuse to accept (corrupt state)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        if not integrity_ok:
            raise HTTPException(
                503,
                "node integrity check failed — not accepting sync actions; "
                "request a full restore instead",
            )

        failures = []
        for action in db_actions:
            try:
                _apply_action(conn, action)
                # Write source as "sync" so the gen counter tracks sync writes separately
                _ = increment_gen(conn, "sync")
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
                len(failures), payload.source_node_id, ", ".join(failures),
            )

        # Update last_seen for the source node
        conn.execute(
            "UPDATE nodes SET last_seen=datetime('now') WHERE node_id=?",
            (payload.source_node_id,),
        )

    log.info(
        "applied %d sync actions from %s",
        len(db_actions),
        payload.source_node_id,
    )
    return Response(status_code=204)


@router.post("/git-pull", status_code=204)
async def trigger_git_pull(payload: GitPullRequest) -> Response:
    """
    Trigger a git pull on this node and enqueue sync_git actions for all peers.
    scope: "outer" | "inner" | "both"
    Any peer that receives the action will pull its own local repo.
    """
    if payload.scope not in ("outer", "inner", "both"):
        raise HTTPException(400, "scope must be 'outer', 'inner', or 'both'")

    scopes = ["outer", "inner"] if payload.scope == "both" else [payload.scope]

    with get_conn() as conn:
        gen = get_gen(conn)
        for scope in scopes:
            action_type = f"sync_git_{scope}"
            enqueue_for_all_peers(conn, action_type, "_system", scope, None, gen)

    for scope in scopes:
        repo = cfg.REPO_OUTER_PATH if scope == "outer" else cfg.REPO_INNER_PATH
        asyncio.create_task(_git_pull_and_restart(repo, scope))
        log.info("triggered git pull [%s] locally + queued for all peers", scope)

    return Response(status_code=204)


@router.post("/restart", status_code=204)
async def trigger_restart() -> Response:
    """Restart the blueprints-app service on this node (via SERVICE_RESTART_CMD)."""
    if not cfg.SERVICE_RESTART_CMD:
        raise HTTPException(503, "SERVICE_RESTART_CMD not configured on this node")
    asyncio.create_task(_restart_service())
    log.info("service restart triggered via API")
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
    log.info("retouch: re-queued %d rows from %s", len(rows), table_name)
    return {"requeued": len(rows), "table": table_name}


@router.post("/restore", status_code=204)
async def receive_restore(request: Request) -> Response:
    """
    Layer 1 restore endpoint.
    Accepts a multipart or raw body with:
      - the zipped DB backup (bytes)
      - SHA-256 hex in X-Blueprints-Checksum header

    Verifies the checksum then atomically replaces the local DB.
    """
    sha256_hex = request.headers.get("x-blueprints-checksum", "")
    if not sha256_hex:
        raise HTTPException(400, "missing X-Blueprints-Checksum header")

    zip_bytes = await request.body()
    if not zip_bytes:
        raise HTTPException(400, "empty restore payload")

    # Generation guard — reject stale backups from nodes with lower gen.
    # A healthy node with gen=N should never be overwritten by a backup with
    # gen<=N. This prevents a fresh empty node from wiping an established one.
    # Nodes that are degraded (integrity_ok=false) always accept any backup.
    sender_gen_str = request.headers.get("x-blueprints-gen", "")
    if sender_gen_str:
        try:
            sender_gen = int(sender_gen_str)
            with get_conn() as conn:
                my_gen = get_gen(conn)
                integrity_ok = get_meta(conn, "integrity_ok") == "true"
            if integrity_ok and sender_gen <= my_gen:
                log.info(
                    "receive_restore: rejecting stale backup "
                    "(sender gen=%d <= my gen=%d)",
                    sender_gen, my_gen,
                )
                raise HTTPException(
                    409,
                    f"stale backup rejected: sender gen={sender_gen} <= my gen={my_gen}",
                )
        except HTTPException:
            raise
        except (ValueError, TypeError):
            pass  # unparseable gen header — allow (backwards compat)

    ok = await apply_restore(zip_bytes, sha256_hex)
    if not ok:
        raise HTTPException(422, "restore failed — checksum mismatch or corrupt zip")

    log.info("full DB restore applied (%d bytes)", len(zip_bytes))
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


@router.get("/status", response_model=SyncStatus)
async def sync_status() -> SyncStatus:
    """Return a sync-state summary for the dashboard and monitoring."""
    from .sync.queue import get_queue_depths

    with get_conn() as conn:
        gen = get_gen(conn)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        last_write_at = get_meta(conn, "last_write_at") or ""
        last_write_by = get_meta(conn, "last_write_by") or ""
        peer_rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_id != ?", (cfg.NODE_ID,)
        ).fetchall()

    peer_ids = [r["node_id"] for r in peer_rows]
    return SyncStatus(
        node_id=cfg.NODE_ID,
        node_name=cfg.NODE_NAME,
        gen=gen,
        integrity_ok=integrity_ok,
        last_write_at=last_write_at,
        last_write_by=last_write_by,
        queue_depths=get_queue_depths(peer_ids),
        peer_count=len(peer_ids),
    )


@router.get("/status", response_model=SyncStatus)
async def sync_status() -> SyncStatus:
    """Return current sync state: gen, integrity, queue depths per peer."""
    with get_conn() as conn:
        gen = get_gen(conn)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
        last_write_at = get_meta(conn, "last_write_at")
        last_write_by = get_meta(conn, "last_write_by")
        peer_rows = conn.execute(
            "SELECT node_id FROM nodes WHERE node_id != ?", (cfg.NODE_ID,)
        ).fetchall()
        peer_ids = [r["node_id"] for r in peer_rows]

    queue_depths = get_queue_depths(peer_ids)

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
