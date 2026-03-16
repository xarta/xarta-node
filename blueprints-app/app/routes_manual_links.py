"""routes_manual_links.py — CRUD for /api/v1/manual-links

Manually curated links — VLAN and tailnet addresses for important services
that don't change often. Used as a fallback quick-reference rendered view
in the Blueprints GUI.

Each entry syncs automatically to all fleet peers via the standard gen-based
sync protocol.
"""

import uuid

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import ManualLinkCreate, ManualLinkOut, ManualLinkUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/manual-links", tags=["manual-links"])


def _row_to_out(row) -> ManualLinkOut:
    return ManualLinkOut(**dict(row))


@router.get("", response_model=list[ManualLinkOut])
async def list_manual_links() -> list[ManualLinkOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM manual_links ORDER BY sort_order, group_name, label"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=ManualLinkOut, status_code=201)
async def create_manual_link(body: ManualLinkCreate) -> ManualLinkOut:
    link_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO manual_links (
                link_id, vlan_ip, vlan_uri, tailnet_ip, tailnet_uri,
                label, icon, group_name, parent_id, sort_order,
                pve_host, is_internet, vm_id, vm_name, lxc_id, lxc_name, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                link_id,
                body.vlan_ip, body.vlan_uri, body.tailnet_ip, body.tailnet_uri,
                body.label, body.icon, body.group_name, body.parent_id,
                body.sort_order if body.sort_order is not None else 0,
                body.pve_host, body.is_internet if body.is_internet is not None else 0,
                body.vm_id, body.vm_name, body.lxc_id, body.lxc_name, body.notes,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "manual_links", link_id, dict(row), gen)
    return _row_to_out(row)


@router.get("/{link_id}", response_model=ManualLinkOut)
async def get_manual_link(link_id: str) -> ManualLinkOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"Link {link_id!r} not found")
    return _row_to_out(row)


@router.put("/{link_id}", response_model=ManualLinkOut)
async def update_manual_link(link_id: str, body: ManualLinkUpdate) -> ManualLinkOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT link_id FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Link {link_id!r} not found")
        conn.execute(
            """
            UPDATE manual_links SET
                vlan_ip      = COALESCE(?, vlan_ip),
                vlan_uri     = COALESCE(?, vlan_uri),
                tailnet_ip   = COALESCE(?, tailnet_ip),
                tailnet_uri  = COALESCE(?, tailnet_uri),
                label        = COALESCE(?, label),
                icon         = COALESCE(?, icon),
                group_name   = COALESCE(?, group_name),
                parent_id    = COALESCE(?, parent_id),
                sort_order   = COALESCE(?, sort_order),
                pve_host     = COALESCE(?, pve_host),
                is_internet  = COALESCE(?, is_internet),
                vm_id        = COALESCE(?, vm_id),
                vm_name      = COALESCE(?, vm_name),
                lxc_id       = COALESCE(?, lxc_id),
                lxc_name     = COALESCE(?, lxc_name),
                notes        = COALESCE(?, notes),
                updated_at   = datetime('now')
            WHERE link_id = ?
            """,
            (
                body.vlan_ip, body.vlan_uri, body.tailnet_ip, body.tailnet_uri,
                body.label, body.icon, body.group_name, body.parent_id,
                body.sort_order, body.pve_host, body.is_internet,
                body.vm_id, body.vm_name, body.lxc_id, body.lxc_name,
                body.notes, link_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_links", link_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{link_id}", status_code=204)
async def delete_manual_link(link_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM manual_links WHERE link_id=?", (link_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Link {link_id!r} not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "manual_links", link_id, {}, gen)
