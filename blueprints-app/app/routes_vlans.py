"""routes_vlans.py — CRUD for /api/v1/vlans

Each row represents a known VLAN with its inferred or manually confirmed CIDR.
Auto-populated by the proxmox-config probe whenever a net interface exposes a
vlan_tag + ip_address pair.  CIDRs can be corrected via PUT.
"""

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import VlanOut, VlanUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/vlans", tags=["vlans"])


@router.get("", response_model=list[VlanOut])
async def list_vlans() -> list[VlanOut]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM vlans ORDER BY vlan_id").fetchall()
    return [VlanOut(**dict(r)) for r in rows]


@router.put("/{vlan_id}", response_model=VlanOut)
async def upsert_vlan(vlan_id: int, body: VlanUpdate) -> VlanOut:
    """Upsert a VLAN row.  On update, cidr_inferred is set to 0 (manually confirmed)."""
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT vlan_id FROM vlans WHERE vlan_id=?", (vlan_id,)
        ).fetchone()
        gen = increment_gen(conn, "human")
        if existing:
            conn.execute(
                """
                UPDATE vlans SET
                    cidr        = COALESCE(?, cidr),
                    cidr_inferred = 0,
                    description = COALESCE(?, description),
                    updated_at  = datetime('now')
                WHERE vlan_id=?
                """,
                (body.cidr, body.description, vlan_id),
            )
            op = "UPDATE"
        else:
            conn.execute(
                """
                INSERT INTO vlans (vlan_id, cidr, cidr_inferred, description)
                VALUES (?, ?, 0, ?)
                """,
                (vlan_id, body.cidr, body.description),
            )
            op = "INSERT"
        row = conn.execute("SELECT * FROM vlans WHERE vlan_id=?", (vlan_id,)).fetchone()
        enqueue_for_all_peers(conn, op, "vlans", str(vlan_id), dict(row), gen)
    return VlanOut(**dict(row))


@router.delete("/{vlan_id}", status_code=204)
async def delete_vlan(vlan_id: int) -> None:
    with get_conn() as conn:
        result = conn.execute("DELETE FROM vlans WHERE vlan_id=?", (vlan_id,))
        if result.rowcount == 0:
            raise HTTPException(404, f"VLAN {vlan_id} not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "vlans", str(vlan_id), {}, gen)
