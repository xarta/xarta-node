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
import sqlite3
import ssl
import time
import uuid
from email.utils import parsedate_to_datetime

import httpx
from fastapi import APIRouter, HTTPException, Request, Response

from . import config as cfg
from .auth import compute_token
from .db import get_conn, get_gen, get_meta, increment_gen
from .models import GitPullRequest, SyncActionsPayload, SyncStatus
from .sync.queue import enqueue, enqueue_for_all_peers, get_queue_depths
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
    "docs", "doc_groups",
    "doc_images",
    "ai_providers",
    "ai_project_assignments",
    "bookmarks",
    "visits",
    "bookmark_deletions",
    "visit_events",
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
    """Run SERVICE_RESTART_CMD, logging the outcome.

    Sleep briefly first so the HTTP response that triggered this call
    has time to be flushed through Caddy before the process is killed.
    """
    await asyncio.sleep(1)
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
        "docs":                    "doc_id",
        "doc_groups":              "group_id",
        "doc_images":              "image_id",
        "ai_providers":            "provider_id",
        "ai_project_assignments":  "assignment_id",
        "bookmarks":               "bookmark_id",
        "visits":                  "visit_id",
        "bookmark_deletions":      "bookmark_id",
        "visit_events":            "event_id",
    }
    return pk_map.get(table, "id")


# ── Forwarding helpers (Phase 2 — GUID-deduplicated relay) ───────────────────

def _can_reach_directly(source: dict, target: dict) -> bool:
    """Return True if source node can reach target via any direct network path.

    Two criteria (either is sufficient):
    1. LAN: both nodes have a primary_ip → they share the on-premises network.
    2. Tailnet: both nodes carry the same non-empty tailnet string.
    """
    if source.get("primary_ip") and target.get("primary_ip"):
        return True
    if (
        source.get("tailnet")
        and target.get("tailnet")
        and source["tailnet"] == target["tailnet"]
    ):
        return True
    return False


def _forward_actions(
    conn: sqlite3.Connection,
    source_node_id: str,
    actions: list,
) -> None:
    """Re-enqueue newly applied actions for peers the originator cannot reach.

    Only fires when there is at least one peer that:
      • the source node CANNOT reach directly, AND
      • this node CAN reach directly.

    Each forwarded copy carries the original GUID so the receiving node's
    dedup check prevents double-application regardless of how many relay
    nodes attempt the forward.  System actions are never forwarded.
    """
    if not actions:
        return
    source_node = cfg.NODE_MAP.get(source_node_id)
    if source_node is None:
        return  # Unknown originator — no forwarding

    relay_peers = [
        n for n in cfg.PEER_NODES
        if not _can_reach_directly(source_node, n)
        and _can_reach_directly(cfg.SELF_NODE, n)
    ]
    if not relay_peers:
        return

    for peer in relay_peers:
        for action in actions:
            try:
                enqueue(
                    conn, peer["node_id"],
                    action.action_type, action.table_name,
                    action.row_id, action.row_data,
                    action.gen, guid=action.guid,
                )
            except Exception:
                log.exception(
                    "failed to forward action %s/%s to relay peer %s",
                    action.table_name, action.row_id, peer["node_id"],
                )
    log.debug(
        "forwarded %d action(s) from %s to relay peer(s) %s",
        len(actions), source_node_id, [p["node_id"] for p in relay_peers],
    )


# ── Routes ───────────────────────────────────────────────────────────────────

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

        newly_applied: list = []
        failures: list[str] = []
        for action in db_actions:
            # ── GUID dedup: drop actions already processed on this node ───────
            # Empty GUID = legacy peer (pre-Phase-2); skip dedup, apply normally.
            if action.guid:
                try:
                    conn.execute(
                        "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                        (action.guid, int(time.time())),
                    )
                except sqlite3.IntegrityError:
                    log.debug(
                        "GUID %s already seen — skipping duplicate from %s",
                        action.guid[:8], payload.source_node_id,
                    )
                    continue

            try:
                _apply_action(conn, action)
                # Write source as "sync" so the gen counter tracks sync writes separately
                _ = increment_gen(conn, "sync")
                newly_applied.append(action)
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

        # Smart forwarding — relay newly applied actions to any peers the
        # originator cannot reach but we can (no-op for current 6-node fleet).
        _forward_actions(conn, payload.source_node_id, newly_applied)

    log.info(
        "applied %d sync actions from %s",
        len(newly_applied),
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


# ── Diagnostic probe endpoints ────────────────────────────────────────────────

def _probe_client(timeout: float) -> httpx.AsyncClient:
    """Return an httpx.AsyncClient configured identically to the sync drain.

    When SYNC_TLS_CA/CERT/KEY are all set the client uses mTLS:
    - server cert verified against the fleet CA
    - this node's client cert+key are presented
    Falls back to plain HTTP if any TLS var is absent.
    """
    if cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.load_verify_locations(cfg.SYNC_TLS_CA)
        ctx.load_cert_chain(cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY)
        return httpx.AsyncClient(timeout=timeout, verify=ctx)
    return httpx.AsyncClient(timeout=timeout)


@router.get("/mtls-probe")
async def mtls_probe() -> dict:
    """
    Probe each fleet peer via mTLS — the same transport the sync drain uses.

    Probes every configured sync address per peer (primary LAN + tailnet
    fallback where applicable), mirroring the multi-address failover order used
    by drain.py.  Each address is probed independently so the GUI can show
    which path is healthy.

    Per-address status values:
      ok:         TLS handshake succeeded, /health returned 2xx
      tls_error:  SSL handshake failed (cert/CA mismatch or missing client cert)
      refused:    TCP connection refused (port closed or firewall drop)
      timeout:    connection or read timed out
      http_error: TLS OK but peer returned non-2xx
      error:      other unexpected failure
    A spurious "ok" is not possible — the remote must accept the client cert
    AND return a 2xx response.
    """
    tls_configured = bool(cfg.SYNC_TLS_CA and cfg.SYNC_TLS_CERT and cfg.SYNC_TLS_KEY)
    results = []

    async def _probe_address(node_id: str, url: str, client: httpx.AsyncClient) -> dict:
        health_url = f"{url}/health"
        try:
            r = await client.get(health_url)
            return {
                "node_id": node_id,
                "address": health_url,
                "status": "ok" if r.is_success else "http_error",
                "http_status": r.status_code,
                "error": None,
            }
        except httpx.ConnectError as e:
            err = str(e)
            status = (
                "tls_error"
                if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                else "refused"
            )
            return {"node_id": node_id, "address": health_url, "status": status, "http_status": None, "error": err}
        except httpx.TimeoutException:
            return {"node_id": node_id, "address": health_url, "status": "timeout", "http_status": None, "error": "connection timed out"}
        except Exception as e:
            return {"node_id": node_id, "address": health_url, "status": "error", "http_status": None, "error": str(e)}

    async with _probe_client(timeout=8.0) as client:
        for n in cfg._peer_nodes:
            node_id = n["node_id"]
            peer_urls = cfg.PEER_SYNC_URLS.get(node_id, [])
            if not peer_urls:
                # Peer has no configured sync addresses (misconfigured node)
                results.append({
                    "node_id": node_id,
                    "address": None,
                    "status": "error",
                    "http_status": None,
                    "error": "no sync addresses configured",
                })
                continue
            for url in peer_urls:
                result = await _probe_address(node_id, url, client)
                results.append(result)

    return {"tls_configured": tls_configured, "peers": results}


@router.get("/ssh-probe")
async def ssh_probe() -> dict:
    """
    Probe each fleet peer via SSH using the xarta-node fleet key.

    Runs: ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new
              -o ConnectTimeout=5 -i <key> root@<ip> echo ok

    BatchMode=yes: no password prompts — immediate failure on auth rejection.
    Distinguishes:
      ok:                echo ok received — SSH auth and connectivity confirmed
      auth_failed:       Permission denied / Authentication failed
      host_key_changed:  Remote host identification changed (MITM risk or rebuild)
      refused:           Connection refused
      no_route:          No route to host / Network unreachable
      timeout:           Connection timed out
      no_key:            XARTA_NODE_SSH_KEY unset or file not found
      error:             Other SSH failure
    A spurious "ok" is not possible — remote must respond with "ok" to stdout.
    """
    ssh_key = os.environ.get("XARTA_NODE_SSH_KEY", "")
    if not ssh_key or not os.path.isfile(ssh_key):
        return {
            "ssh_key_present": False,
            "ssh_key_path": ssh_key or None,
            "peers": [],
            "error": "XARTA_NODE_SSH_KEY not set or key file not found",
        }

    async def _probe_peer(n: dict) -> dict:
        ip = n["primary_ip"]
        node_id = n["node_id"]
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh", "-n",
                "-o", "BatchMode=yes",
                "-o", "StrictHostKeyChecking=accept-new",
                "-o", "ConnectTimeout=5",
                "-i", ssh_key,
                f"root@{ip}",
                "echo ok",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=12.0)
            rc = proc.returncode
            out = stdout.decode().strip()
            err = stderr.decode().strip()
            if rc == 0 and out == "ok":
                return {"node_id": node_id, "ip": ip, "status": "ok", "error": None}
            if "REMOTE HOST IDENTIFICATION HAS CHANGED" in err:
                return {"node_id": node_id, "ip": ip, "status": "host_key_changed", "error": err}
            if "Permission denied" in err or "Authentication failed" in err:
                return {"node_id": node_id, "ip": ip, "status": "auth_failed", "error": err}
            if "Connection refused" in err:
                return {"node_id": node_id, "ip": ip, "status": "refused", "error": err}
            if "No route to host" in err or "Network unreachable" in err:
                return {"node_id": node_id, "ip": ip, "status": "no_route", "error": err}
            if "Connection timed out" in err or "Operation timed out" in err:
                return {"node_id": node_id, "ip": ip, "status": "timeout", "error": err}
            return {"node_id": node_id, "ip": ip, "status": "error", "error": err or f"exit {rc}: {out}"}
        except asyncio.TimeoutError:
            return {"node_id": node_id, "ip": ip, "status": "timeout", "error": "SSH timed out (12s)"}
        except Exception as e:
            return {"node_id": node_id, "ip": ip, "status": "error", "error": str(e)}

    results = await asyncio.gather(*[_probe_peer(n) for n in cfg._peer_nodes])
    return {
        "ssh_key_present": True,
        "ssh_key_path": ssh_key,
        "peers": list(results),
    }


# Port used as the synthetic "dead" first address in the failover probe.
# Must never be in use on any fleet node.  Port 19999 is in the ephemeral
# range boundary and is not assigned to any standard service.
_FAILOVER_DEAD_PORT = 19999


@router.get("/failover-probe")
async def failover_probe() -> dict:
    """
    Validate the Phase 1 multi-address failover logic without a real remote node.

    For each fleet peer, constructs a synthetic 2-URL list:
      1. Dead URL:  <scheme>://<peer_ip>:<_FAILOVER_DEAD_PORT>  — same IP as
                    the real peer, but a port that is never listening anywhere.
                    TCP connect is refused instantly (ECONNREFUSED) — no timeout
                    wait.  This simulates the drain hitting an unreachable primary
                    address (primary LAN down, or future VPS with no primary_ip).
      2. Real URL:  the peer's actual configured sync URL (from PEER_SYNC_URLS).

    The probe attempts the dead URL, expects ConnectError, then falls through
    to the real URL — exactly as drain.py does.

    A peer PASSES when:
      - dead_status == "refused" (ConnectError on port 19999, as expected)
      - real_status == "ok"       (peer responded normally on real URL)

    This validates the full failover code path (config.py → drain URL list →
    httpx ConnectError catch → next URL) against every live peer, on every
    node that runs the diagnostic.

    Phase 2 hook: when GUID deduplication is implemented, a companion
    /failover-guid-probe endpoint can test that the same GUID arriving twice
    is silently dropped on the second application.
    """
    import time

    async def _probe_one(n: dict, client: httpx.AsyncClient) -> dict:
        node_id = n["node_id"]
        scheme  = n.get("sync_scheme", "http")
        peer_ip = n.get("primary_ip") or "127.0.0.1"

        dead_url = f"{scheme}://{peer_ip}:{_FAILOVER_DEAD_PORT}"
        real_urls = cfg.PEER_SYNC_URLS.get(node_id, [])
        real_url  = real_urls[0] if real_urls else None

        # ── Step 1: attempt the dead URL ─────────────────────────────────
        t0 = time.monotonic()
        dead_status = "unknown"
        dead_error  = None
        try:
            await client.get(f"{dead_url}/health")
            dead_status = "open"   # unexpected — port 19999 should be closed
        except httpx.ConnectError as e:
            err = str(e)
            dead_status = (
                "tls_error"
                if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                else "refused"
            )
            dead_error = err
        except httpx.TimeoutException:
            dead_status = "timeout"
            dead_error  = "connection timed out"
        except Exception as e:
            dead_status = "error"
            dead_error  = str(e)
        dead_ms = round((time.monotonic() - t0) * 1000)

        # ── Step 2: attempt the real URL ─────────────────────────────────
        real_status      = "no_url"
        real_http_status = None
        real_error       = None
        real_ms          = 0
        if real_url:
            t1 = time.monotonic()
            try:
                r = await client.get(f"{real_url}/health")
                real_status      = "ok" if r.is_success else "http_error"
                real_http_status = r.status_code
            except httpx.ConnectError as e:
                err = str(e)
                real_status = (
                    "tls_error"
                    if ("[SSL:" in err or "CERTIFICATE" in err or "handshake" in err.lower())
                    else "refused"
                )
                real_error = err
            except httpx.TimeoutException:
                real_status = "timeout"
                real_error  = "connection timed out"
            except Exception as e:
                real_status = "error"
                real_error  = str(e)
            real_ms = round((time.monotonic() - t1) * 1000)

        failover_ok = (dead_status in ("refused", "timeout")) and real_status == "ok"

        return {
            "node_id":         node_id,
            "dead_url":        f"{dead_url}/health",
            "dead_status":     dead_status,
            "dead_ms":         dead_ms,
            "dead_error":      dead_error,
            "real_url":        f"{real_url}/health" if real_url else None,
            "real_status":     real_status,
            "real_http_status": real_http_status,
            "real_ms":         real_ms,
            "real_error":      real_error,
            "failover_ok":     failover_ok,
        }

    results = []
    async with _probe_client(timeout=8.0) as client:
        for n in cfg._peer_nodes:
            results.append(await _probe_one(n, client))

    all_passed = all(r["failover_ok"] for r in results) if results else False
    return {
        "method":     f"synthetic dead-port (port {_FAILOVER_DEAD_PORT}) + real configured URL",
        "dead_port":  _FAILOVER_DEAD_PORT,
        "all_passed": all_passed,
        "peers":      results,
    }


@router.get("/guid-probe")
async def guid_probe() -> dict:
    """
    Validate Phase 2 GUID deduplication and smart forwarding logic.

    Three sub-tests — all non-destructive and self-cleaning:

    1. GUID dedup (DB layer)
       Inserts a synthetic UUID4 into sync_seen_guids (first insert → accepted),
       then tries to insert the same GUID again (second insert → IntegrityError,
       i.e. correctly deduplicated).  The test GUID is deleted at the end.

    2. Fleet forwarding topology
       Runs _can_reach_directly() for every known peer and returns whether each
       is reachable by this node.  In the current 6-node on-prem fleet every
       peer shares the LAN so all are directly reachable and no forwarding is
       ever needed (zero-overhead path).

    3. Mock remote node relay
       Simulates a phantom source node that has no primary_ip (like a future
       remote VPS) but shares this node's tailnet.  Reports which peers would
       be identified as relay targets — i.e. peers reachable by this node but
       NOT by the phantom source.
    """
    import sqlite3 as _sqlite3

    # ── Test 1: GUID dedup ────────────────────────────────────────────────────
    test_guid = f"__probe__{uuid.uuid4().hex}"
    first_insert   = "error"
    second_insert  = "error"
    cleanup        = "ok"
    dedup_ok       = False

    try:
        with get_conn() as conn:
            # First insert — should succeed
            try:
                conn.execute(
                    "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                    (test_guid, int(time.time())),
                )
                first_insert = "accepted"
            except Exception as e:
                first_insert = f"error: {e}"

            # Second insert — should raise IntegrityError (PRIMARY KEY conflict)
            try:
                conn.execute(
                    "INSERT INTO sync_seen_guids (guid, received_at) VALUES (?, ?)",
                    (test_guid, int(time.time())),
                )
                second_insert = "unexpected_accepted"  # bug — should have been rejected
            except _sqlite3.IntegrityError:
                second_insert = "deduplicated"
            except Exception as e:
                second_insert = f"error: {e}"

            # Cleanup — remove the probe GUID
            try:
                conn.execute(
                    "DELETE FROM sync_seen_guids WHERE guid=?", (test_guid,)
                )
            except Exception as e:
                cleanup = f"failed: {e}"

        dedup_ok = (first_insert == "accepted" and second_insert == "deduplicated" and cleanup == "ok")

    except Exception as e:
        first_insert = f"db_error: {e}"

    # ── Test 2: Fleet forwarding topology ─────────────────────────────────────
    topology = []
    for peer in cfg.PEER_NODES:
        topology.append({
            "peer_node_id":       peer["node_id"],
            "peer_has_primary_ip": bool(peer.get("primary_ip")),
            "peer_tailnet":        peer.get("tailnet", ""),
            "self_can_reach":     _can_reach_directly(cfg.SELF_NODE, peer),
        })

    # ── Test 3: Mock remote node relay ────────────────────────────────────────
    # Simulate a source with no primary_ip (VPS) on the same tailnet as self.
    self_tailnet = cfg.SELF_NODE.get("tailnet", "")
    mock_source = {
        "node_id":    "__mock_vps__",
        "primary_ip": "",
        "tailnet":    self_tailnet,
    }
    relay_peers = [
        p["node_id"] for p in cfg.PEER_NODES
        if not _can_reach_directly(mock_source, p)
        and _can_reach_directly(cfg.SELF_NODE, p)
    ]
    # In the current 6-node on-prem fleet (all have primary_ip, all same-tailnet covered),
    # expected relay count depends on peer tailnet membership:
    # peers with primary_ip CAN be reached by the mock source only via primary_ip path.
    # Since mock_source has no primary_ip → source can't reach LAN peers unless they share tailnet.
    # Peers sharing self_tailnet: mock_source CAN reach them via tailnet (same tailnet).
    # Peers NOT on self_tailnet: only reachable via relay.
    peers_not_on_self_tailnet = [
        p["node_id"] for p in cfg.PEER_NODES
        if p.get("tailnet", "") != self_tailnet
    ]
    expected_relay_ids = set(peers_not_on_self_tailnet)
    relay_ok = set(relay_peers) == expected_relay_ids

    mock_relay = {
        "mock_source_has_primary_ip": False,
        "mock_source_tailnet":        self_tailnet or "(none)",
        "relay_peers":                relay_peers,
        "expected_relay_peers":       sorted(expected_relay_ids),
        "relay_ok":                   relay_ok,
    }

    all_passed = dedup_ok and relay_ok
    return {
        "all_passed": all_passed,
        "dedup": {
            "ok":           dedup_ok,
            "first_insert": first_insert,
            "second_insert": second_insert,
            "cleanup":      cleanup,
        },
        "topology": topology,
        "mock_relay": mock_relay,
    }


@router.post("/roundtrip-test")
async def sync_roundtrip_test() -> dict:
    """
    End-to-end data propagation test.

    Writes a temporary canary row to the settings table, enqueues it for peers
    via the normal drain path, then polls the first available peer's API to
    confirm the row arrived.  Cleans up (deletes the canary) regardless of
    outcome.

    Returns:
      status:         ok | timeout | auth_failed | no_peers | no_secret | error
      elapsed_ms:     time from write to confirmed propagation (or timeout)
      propagated_to:  node_id of the peer that received the canary
      error:          human-readable failure reason, or null on success

    Timeout: 25s — covers more than one drain cycle (drain sleeps 1-20s randomly).
    """
    if not cfg._peer_nodes:
        return {"status": "no_peers", "elapsed_ms": 0, "propagated_to": None,
                "error": "no active peer nodes configured"}
    if not cfg.SYNC_SECRET:
        return {"status": "no_secret", "elapsed_ms": 0, "propagated_to": None,
                "error": "BLUEPRINTS_SYNC_SECRET not configured — cannot authenticate peer read"}

    canary_key = f"_bp_diag_canary_{uuid.uuid4().hex[:16]}"
    peer = cfg._peer_nodes[0]
    peer_base = (
        f"{peer.get('sync_scheme', 'http')}://{peer['primary_ip']}"
        f":{peer.get('sync_port', 8080)}"
    )
    start_ts = time.monotonic()
    propagated = False
    early_result: dict | None = None

    try:
        # Write canary locally and queue for peers via normal sync path
        canary_row = {
            "key": canary_key,
            "value": "diagnostic-probe",
            "description": "Temporary sync round-trip test — will auto-delete",
        }
        with get_conn() as conn:
            gen = increment_gen(conn, "human")
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value, description) VALUES (?, ?, ?)",
                (canary_key, canary_row["value"], canary_row["description"]),
            )
            enqueue_for_all_peers(conn, "INSERT", "settings", canary_key, canary_row, gen)
        log.info("roundtrip-test: wrote canary '%s', polling %s", canary_key, peer["node_id"])

        # Poll peer for canary; 12 x 2s = 24s max
        async with _probe_client(timeout=8.0) as client:
            for _ in range(12):
                await asyncio.sleep(2)
                try:
                    token = compute_token(cfg.SYNC_SECRET)
                    r = await client.get(
                        f"{peer_base}/api/v1/settings/{canary_key}",
                        headers={"X-API-Token": token},
                    )
                    if r.status_code == 200:
                        propagated = True
                        break
                    if r.status_code == 401:
                        elapsed = round((time.monotonic() - start_ts) * 1000)
                        # Disambiguate wrong-secret vs clock-skew.
                        # TOTP window = ±1 × 5s = 15s tolerance.
                        # /health is auth-exempt — fetch it to get the peer's
                        # Date response header and compare against local time.
                        totp_note = ""
                        try:
                            health_r = await client.get(f"{peer_base}/health")
                            peer_date = health_r.headers.get("date", "")
                            if peer_date:
                                peer_ts = parsedate_to_datetime(peer_date).timestamp()
                                skew_s = abs(peer_ts - time.time())
                                if skew_s > 15:
                                    totp_note = (
                                        f" — clock skew {skew_s:.0f}s detected "
                                        f"(TOTP window is \u00b115s; sync NTP/chrony on peer)"
                                    )
                                else:
                                    totp_note = (
                                        f" — clock skew only {skew_s:.1f}s "
                                        f"(within TOTP tolerance; likely wrong BLUEPRINTS_SYNC_SECRET)"
                                    )
                        except Exception:
                            pass  # if health fetch fails, omit the clock note
                        early_result = {
                            "status": "auth_failed",
                            "elapsed_ms": elapsed,
                            "propagated_to": None,
                            "error": (
                                f"peer {peer['node_id']} rejected token (HTTP 401)"
                                f"{totp_note}"
                            ),
                        }
                        break
                except Exception as poll_err:
                    log.debug("roundtrip-test: poll error: %s", poll_err)
    finally:
        # Always clean up — delete canary locally and queue delete for peers
        try:
            with get_conn() as conn:
                conn.execute("DELETE FROM settings WHERE key=?", (canary_key,))
                gen = increment_gen(conn, "human")
                enqueue_for_all_peers(conn, "DELETE", "settings", canary_key, None, gen)
            log.info("roundtrip-test: canary '%s' deleted", canary_key)
        except Exception as cleanup_err:
            log.warning("roundtrip-test: cleanup failed: %s", cleanup_err)

    elapsed = round((time.monotonic() - start_ts) * 1000)
    if early_result:
        return early_result
    if propagated:
        log.info("roundtrip-test: propagated to %s in %dms", peer["node_id"], elapsed)
        return {
            "status": "ok",
            "elapsed_ms": elapsed,
            "propagated_to": peer["node_id"],
            "error": None,
        }
    log.warning("roundtrip-test: timeout after %dms — canary not found on %s", elapsed, peer["node_id"])
    return {
        "status": "timeout",
        "elapsed_ms": elapsed,
        "propagated_to": None,
        "error": f"canary not found on {peer['node_id']} within 25s — drain may be stalled or queue depth too high",
    }
