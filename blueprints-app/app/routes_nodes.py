"""routes_nodes.py — node management endpoints.

/api/v1/nodes/self    — returns this node's identity (from config, not DB)
/api/v1/nodes         — list nodes registered in the DB
POST /api/v1/nodes/refresh — re-read .nodes.json and upsert DB (Refresh button)
DELETE /api/v1/nodes/{id}  — mark node inactive in .nodes.json and update DB
POST /api/v1/nodes/{id}/pct — start or stop the LXC for a fleet node via pct on its PVE host
GET  /api/v1/nodes/{id}/pct-status — return current pct status from proxmox_config DB
"""

import asyncio
import json
import logging
import os
import subprocess
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import Response

from . import config as cfg
from .auth import compute_token
from .db import get_conn
from .models import NodeOut

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
        display_order=row["display_order"] if "display_order" in keys else 0,
        host_machine=row["host_machine"],
        tailnet=row["tailnet"],
        primary_hostname=row["primary_hostname"] if "primary_hostname" in keys else None,
        tailnet_hostname=row["tailnet_hostname"] if "tailnet_hostname" in keys else None,
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
    """Return this node's identity as derived from .nodes.json."""
    return NodeOut(
        node_id=cfg.NODE_ID,
        display_name=cfg.NODE_NAME,
        host_machine=cfg.HOST_MACHINE,
        tailnet=None,
        addresses=[cfg.SELF_ADDRESS],
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
            FROM nodes n ORDER BY n.display_order, n.display_name
            """
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("/refresh", status_code=200)
async def refresh_nodes() -> dict:
    """
    Re-read .nodes.json and upsert all active nodes into the local DB.
    Called by the Refresh button in the Nodes UI tab.
    """
    _upsert_nodes_from_config()
    log.info("nodes refreshed from .nodes.json via API")
    return {"status": "ok", "active_nodes": len([n for n in cfg.NODES_DATA if n.get("active", False)])}


def _upsert_nodes_from_config() -> int:
    """Upsert all active nodes from cfg.NODES_DATA into the local DB. Returns count."""
    count = 0
    with get_conn() as conn:
        for node in cfg.NODES_DATA:
            if not node.get("active", False):
                continue
            nid     = node["node_id"]
            name    = node["display_name"]
            order   = node.get("display_order", 0)
            host    = node["host_machine"]
            tailnet = node.get("tailnet", "")
            pip     = node["primary_ip"]
            ph      = node["primary_hostname"]
            tip     = node["tailnet_ip"]
            th      = node.get("tailnet_hostname", "")
            port    = node["sync_port"]

            addresses = json.dumps([
                f"http://{pip}:{port}",
                f"http://{tip}:{port}",
            ])
            ui_url = f"https://{ph}"

            existing = conn.execute(
                "SELECT node_id FROM nodes WHERE node_id=?", (nid,)
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE nodes SET display_name=?, display_order=?, host_machine=?, tailnet=?, "
                    "primary_hostname=?, tailnet_hostname=?, "
                    "addresses=?, ui_url=?, last_seen=datetime('now') WHERE node_id=?",
                    (name, order, host, tailnet, ph, th, addresses, ui_url, nid),
                )
            else:
                conn.execute(
                    "INSERT INTO nodes (node_id, display_name, display_order, host_machine, tailnet, "
                    "primary_hostname, tailnet_hostname, addresses, ui_url, last_seen) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                    (nid, name, order, host, tailnet, ph, th, addresses, ui_url),
                )
            count += 1
    return count


@router.delete("/{node_id}", status_code=204)
async def delete_node(node_id: str) -> Response:
    """
    Mark a node inactive in .nodes.json (via bp-nodes-delete.sh) and remove
    its DB record from this node. Does not propagate via sync queue — nodes
    table is local-only, sourced from .nodes.json.
    """
    if node_id == cfg.NODE_ID:
        raise HTTPException(400, "cannot delete self")

    # Run bp-nodes-delete.sh to mark inactive in JSON and reload
    script = os.path.join(cfg.REPO_OUTER_PATH, "bp-nodes-delete.sh")
    if os.path.isfile(script):
        result = subprocess.run(
            ["bash", script, node_id],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            log.error("bp-nodes-delete.sh failed: %s", result.stderr)
            raise HTTPException(500, f"failed to update .nodes.json: {result.stderr.strip()}")
        log.info("bp-nodes-delete.sh marked %s inactive in .nodes.json", node_id)
    else:
        log.warning(
            "bp-nodes-delete.sh not found at %s — deleting from DB only", script
        )

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
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code not in (200, 204):
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    log.info("proxied git-pull to %s (%s)", node_id, target)
    return Response(status_code=204)


@router.post("/{node_id}/restart", status_code=204)
async def proxy_node_restart(node_id: str) -> Response:
    """Proxy a service restart request to the named peer node."""
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
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{target}/api/v1/sync/restart",
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code not in (200, 204):
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    log.info("proxied restart to %s (%s)", node_id, target)
    return Response(status_code=204)


class PctAction(BaseModel):
    action: str  # "start" | "stop"


@router.get("/{node_id}/pct-status", status_code=200)
async def get_node_pct_status(node_id: str) -> dict:
    """Return the live pct status of the LXC for this node via SSH to its PVE host."""
    with get_conn() as conn:
        node_row = conn.execute(
            "SELECT host_machine FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
    if not node_row:
        raise HTTPException(404, f"node '{node_id}' not found")

    with get_conn() as conn:
        pve_row = conn.execute(
            "SELECT pve_host, vmid FROM proxmox_config WHERE name=? AND vm_type='lxc' LIMIT 1",
            (node_row["host_machine"],),
        ).fetchone()

    if not pve_row:
        return {"node_id": node_id, "status": "unknown", "vmid": None, "pve_host": None}

    pve_host = pve_row["pve_host"]
    vmid     = pve_row["vmid"]

    from .ssh import make_ssh_args, SshTargetNotFound, SshKeyMissing, resolve_env_key

    try:
        ssh_args = make_ssh_args(pve_host, connect_timeout=6)
    except SshTargetNotFound:
        try:
            key_path = resolve_env_key("PROXMOX_SSH_KEY")
        except SshKeyMissing as exc:
            return {"node_id": node_id, "status": "unknown", "vmid": vmid, "pve_host": pve_host, "error": str(exc)}
        ssh_args = ["-i", key_path, "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", "-o", "ConnectTimeout=6"]
    except SshKeyMissing as exc:
        return {"node_id": node_id, "status": "unknown", "vmid": vmid, "pve_host": pve_host, "error": str(exc)}

    cmd = ["ssh"] + ssh_args + [f"root@{pve_host}", f"pct status {vmid}"]
    try:
        result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=8)
    except subprocess.TimeoutExpired:
        return {"node_id": node_id, "status": "unknown", "vmid": vmid, "pve_host": pve_host, "error": "SSH timed out"}

    output = result.stdout.strip().lower()
    if "running" in output:
        status = "running"
    elif "stopped" in output:
        status = "stopped"
    else:
        status = "unknown"

    return {"node_id": node_id, "status": status, "vmid": vmid, "pve_host": pve_host}


@router.post("/{node_id}/pct", status_code=200)
async def node_pct_action(node_id: str, body: PctAction) -> dict:
    """Start or stop the LXC for the named node via pct on its PVE host."""
    action = body.action.strip().lower()
    if action not in ("start", "stop"):
        raise HTTPException(400, f"invalid action '{action}'; must be 'start' or 'stop'")

    with get_conn() as conn:
        node_row = conn.execute(
            "SELECT host_machine FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
    if not node_row:
        raise HTTPException(404, f"node '{node_id}' not found")

    host_machine = node_row["host_machine"]

    with get_conn() as conn:
        pve_row = conn.execute(
            "SELECT pve_host, vmid FROM proxmox_config WHERE name=? AND vm_type='lxc' LIMIT 1",
            (host_machine,),
        ).fetchone()
    if not pve_row:
        raise HTTPException(
            404,
            f"no proxmox_config entry for '{host_machine}' — run a Proxmox probe first",
        )

    pve_host = pve_row["pve_host"]
    vmid     = pve_row["vmid"]

    from .ssh import make_ssh_args, SshTargetNotFound, SshKeyMissing, resolve_env_key

    try:
        ssh_args = make_ssh_args(pve_host, connect_timeout=10)
    except SshTargetNotFound:
        # Fallback: use PROXMOX_SSH_KEY directly (no source-IP binding)
        try:
            key_path = resolve_env_key("PROXMOX_SSH_KEY")
        except SshKeyMissing as exc:
            raise HTTPException(503, f"SSH key not available: {exc}") from exc
        ssh_args = [
            "-i", key_path,
            "-o", "StrictHostKeyChecking=no",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
        ]
    except SshKeyMissing as exc:
        raise HTTPException(503, f"SSH key not available: {exc}") from exc

    cmd = ["ssh"] + ssh_args + [f"root@{pve_host}", f"pct {action} {vmid}"]
    try:
        result = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "SSH command timed out") from exc

    if result.returncode != 0:
        raise HTTPException(
            500,
            f"pct {action} {vmid} on {pve_host} failed: {result.stderr.strip() or '(no output)'}",
        )

    log.info("pct %s %s on %s (node %s) succeeded", action, vmid, pve_host, node_id)
    return {
        "status":   "ok",
        "action":   action,
        "vmid":     vmid,
        "pve_host": pve_host,
        "output":   result.stdout.strip(),
    }


@router.post("", status_code=405)
async def register_node_rejected() -> dict:
    """
    Nodes are now managed via .nodes.json (single source of truth).

    To add or remove a node: edit .nodes.json, then distribute it with
    bp-nodes-push.sh and press Refresh in Settings > Nodes (or restart
    the app).  Programmatic registration via this endpoint is no longer
    supported.
    """
    raise HTTPException(
        405,
        detail=(
            "Node registration via the API is no longer supported. "
            "Edit .nodes.json and distribute via bp-nodes-push.sh."
        ),
    )

