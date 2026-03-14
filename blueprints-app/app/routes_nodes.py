"""routes_nodes.py — node registration and self-identity endpoints.

/api/v1/nodes/self — returns this node's identity (from env vars, not DB)
/api/v1/nodes      — list peer nodes registered in the DB
POST /api/v1/nodes — register a peer node (operator-triggered); on first
                     registration this node immediately sends the new peer a
                     full DB backup, acting as temporary primary for the
                     onboarding.  Nodes never self-register — the operator
                     always introduces nodes to each other.
"""

import asyncio
import json
import logging
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, get_gen, increment_gen
from .models import NodeCreate, NodeOut

log = logging.getLogger(__name__)

router = APIRouter(prefix="/nodes", tags=["nodes"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> NodeOut:
    addrs = row["addresses"]
    keys = row.keys()
    addr_list: list[str] = json.loads(addrs) if addrs else []

    # A node is a fleet peer if it is self, or if any of its addresses appears
    # in the configured PEER_URLS (these are the nodes this instance syncs to).
    _peer_set = {u.rstrip('/') for u in cfg.PEER_URLS}
    fleet_peer: bool = (
        row["node_id"] == cfg.NODE_ID
        or any(a.rstrip('/') in _peer_set for a in addr_list)
    )

    return NodeOut(
        node_id=row["node_id"],
        display_name=row["display_name"],
        host_machine=row["host_machine"],
        tailnet=row["tailnet"],
        addresses=addr_list or None,
        ui_url=row["ui_url"] if "ui_url" in keys else None,
        machine_id=row["machine_id"] if "machine_id" in keys else None,
        last_seen=row["last_seen"],
        created_at=row["created_at"],
        fleet_peer=fleet_peer,
        pending_count=row["pending_count"] if "pending_count" in keys else 0,
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/self", response_model=NodeOut)
async def get_self() -> NodeOut:
    """Return this node's identity as derived from env vars."""
    return NodeOut(
        node_id=cfg.NODE_ID,
        display_name=cfg.NODE_NAME,
        host_machine=cfg.HOST_MACHINE,
        tailnet=None,
        addresses=cfg.PEER_URLS,  # own addresses not known without .env; use peers as context
        last_seen=None,
        created_at="",
    )


@router.get("", response_model=list[NodeOut])
async def list_nodes() -> list[NodeOut]:
    """List all peer nodes registered in the local DB."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT n.*,
                   (SELECT COUNT(*) FROM sync_queue
                    WHERE target_node_id = n.node_id AND sent = 0) AS pending_count
            FROM nodes n ORDER BY n.display_name
            """
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.delete("/{node_id}", status_code=204)
async def delete_node(node_id: str) -> Response:
    """Remove a node record from the local DB (does not purge the sync queue)."""
    with get_conn() as conn:
        deleted = conn.execute(
            "DELETE FROM nodes WHERE node_id=?", (node_id,)
        ).rowcount
    if not deleted:
        raise HTTPException(404, f"node '{node_id}' not found")
    log.info("deleted node %s from local DB", node_id)
    return Response(status_code=204)


@router.delete("/{node_id}/sync-queue", status_code=204)
async def purge_node_sync_queue(node_id: str) -> Response:
    """Purge all unsent sync queue entries targeting a specific node."""
    with get_conn() as conn:
        n = conn.execute(
            "DELETE FROM sync_queue WHERE target_node_id=? AND sent=0", (node_id,)
        ).rowcount
    log.info("purged %d unsent sync queue entries for node %s", n, node_id)
    return Response(status_code=204)


@router.post("/{node_id}/git-pull", status_code=204)
async def proxy_node_git_pull(node_id: str) -> Response:
    """Proxy a git-pull (scope=outer) request to the named peer node."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT addresses FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
    if not row or not row["addresses"]:
        raise HTTPException(404, f"node '{node_id}' not found or has no addresses")
    addrs: list[str] = json.loads(row["addresses"])
    if not addrs:
        raise HTTPException(422, f"node '{node_id}' has no addresses configured")
    target = addrs[0].rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{target}/api/v1/sync/git-pull",
                json={"scope": "outer"},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code not in (200, 204):
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    log.info("proxied git-pull to %s (%s)", node_id, target)
    return Response(status_code=204)


@router.post("", response_model=NodeOut, status_code=201)
async def register_node(body: NodeCreate) -> NodeOut:
    """
    Register a peer node.

    Stores the node in the local DB and enqueues the registration to all
    existing peers (Phase 2: also triggers full-DB backup to the new node).
    """
    if body.node_id == cfg.NODE_ID:
        raise HTTPException(400, "cannot register self as a peer")

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT node_id FROM nodes WHERE node_id=?", (body.node_id,)
        ).fetchone()

        if existing:
            # Update addresses + last_seen on re-registration
            conn.execute(
                "UPDATE nodes SET display_name=?, host_machine=?, tailnet=?, "
                "addresses=?, ui_url=?, machine_id=?, last_seen=datetime('now') WHERE node_id=?",
                (
                    body.display_name,
                    body.host_machine,
                    body.tailnet,
                    json.dumps(body.addresses) if body.addresses else None,
                    body.ui_url,
                    body.machine_id,
                    body.node_id,
                ),
            )
            log.info("updated peer node %s", body.node_id)
        else:
            gen = increment_gen(conn, "human")
            conn.execute(
                """
                INSERT INTO nodes
                    (node_id, display_name, host_machine, tailnet, addresses, ui_url, machine_id, last_seen)
                VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    body.node_id,
                    body.display_name,
                    body.host_machine,
                    body.tailnet,
                    json.dumps(body.addresses) if body.addresses else None,
                    body.ui_url,
                    body.machine_id,
                ),
            )
            log.info(
                "registered new peer node %s — queuing for existing peers",
                body.node_id,
            )
            # Enqueue the new node registration to all OTHER existing peers
            from .sync.queue import enqueue_for_all_peers
            row = conn.execute(
                "SELECT * FROM nodes WHERE node_id=?", (body.node_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn, "INSERT", "nodes", body.node_id, dict(row), gen,
                exclude_node_id=body.node_id,
            )

            # Send a full DB backup to the new peer if we know their address.
            # A brand-new peer has no data yet — the incremental queue alone
            # won't help them catch up, so we push the whole DB immediately.
            if body.addresses:
                asyncio.create_task(
                    _send_initial_backup(body.node_id, body.addresses[0])
                )
                log.info(
                    "scheduled initial full backup for new peer %s at %s",
                    body.node_id, body.addresses[0],
                )

        row = conn.execute(
            "SELECT * FROM nodes WHERE node_id=?", (body.node_id,)
        ).fetchone()
    return _row_to_out(row)


async def _send_initial_backup(node_id: str, peer_url: str) -> None:
    """
    Send a full DB backup zip to a newly registered peer's restore endpoint.
    Called as a background task immediately after a new node is registered.
    Waits 2 s so the peer has time to finish processing the registration
    response before we start sending.
    """
    await asyncio.sleep(2)
    from .sync.restore import make_full_backup
    from .db import get_conn, get_gen

    try:
        zip_bytes, sha256_hex = make_full_backup()
    except Exception:
        log.exception("initial backup: failed to create backup for new peer %s", node_id)
        return

    # Include our current gen so the receiver can apply the generation guard.
    with get_conn() as conn:
        current_gen = get_gen(conn)

    target = peer_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(
                f"{target}/api/v1/sync/restore",
                content=zip_bytes,
                headers={
                    "content-type": "application/octet-stream",
                    "x-blueprints-checksum": sha256_hex,
                    "x-blueprints-gen": str(current_gen),
                },
            )
        if resp.status_code == 204:
            log.info(
                "initial backup sent to new peer %s — %d bytes",
                node_id, len(zip_bytes),
            )
        else:
            log.warning(
                "initial backup: peer %s rejected restore: HTTP %d",
                node_id, resp.status_code,
            )
    except httpx.ConnectError:
        log.warning("initial backup: peer %s unreachable — they will sync via queue later", node_id)
    except Exception:
        log.exception("initial backup: unexpected error sending to peer %s", node_id)
