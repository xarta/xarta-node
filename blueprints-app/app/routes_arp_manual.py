"""routes_arp_manual.py — CRUD for /api/v1/arp-manual

Manual IP ↔ MAC entries — human-supplied records for machines that can't be
found by automated discovery (CARP backups, offline hosts, odd appliances, etc.)

Each entry syncs automatically to all fleet peers via the standard gen-based
sync protocol.
"""

import uuid

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import ArpManualCreate, ArpManualOut, ArpManualUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/arp-manual", tags=["arp-manual"])


def _row_to_out(row) -> ArpManualOut:
    return ArpManualOut(**dict(row))


@router.get("", response_model=list[ArpManualOut])
async def list_arp_manual() -> list[ArpManualOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM arp_manual ORDER BY ip_address"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=ArpManualOut, status_code=201)
async def create_arp_manual(body: ArpManualCreate) -> ArpManualOut:
    entry_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO arp_manual (entry_id, ip_address, mac_address, notes)
            VALUES (?, ?, ?, ?)
            """,
            (entry_id, body.ip_address, body.mac_address, body.notes),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM arp_manual WHERE entry_id=?", (entry_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "arp_manual", entry_id, dict(row), gen)
    return _row_to_out(row)


@router.get("/{entry_id}", response_model=ArpManualOut)
async def get_arp_manual(entry_id: str) -> ArpManualOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM arp_manual WHERE entry_id=?", (entry_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"Entry {entry_id!r} not found")
    return _row_to_out(row)


@router.put("/{entry_id}", response_model=ArpManualOut)
async def update_arp_manual(entry_id: str, body: ArpManualUpdate) -> ArpManualOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT entry_id FROM arp_manual WHERE entry_id=?", (entry_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Entry {entry_id!r} not found")
        conn.execute(
            """
            UPDATE arp_manual SET
                ip_address  = COALESCE(?, ip_address),
                mac_address = COALESCE(?, mac_address),
                notes       = COALESCE(?, notes),
                updated_at  = datetime('now')
            WHERE entry_id = ?
            """,
            (body.ip_address, body.mac_address, body.notes, entry_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM arp_manual WHERE entry_id=?", (entry_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "arp_manual", entry_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{entry_id}", status_code=204)
async def delete_arp_manual(entry_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM arp_manual WHERE entry_id=?", (entry_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"Entry {entry_id!r} not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "arp_manual", entry_id, {}, gen)
