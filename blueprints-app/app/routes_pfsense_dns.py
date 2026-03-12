"""routes_pfsense_dns.py — CRUD for /api/v1/pfsense-dns"""

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import PfSenseDnsCreate, PfSenseDnsOut, PfSenseDnsUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/pfsense-dns", tags=["pfsense-dns"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> PfSenseDnsOut:
    return PfSenseDnsOut(
        dns_entry_id=row["dns_entry_id"],
        ip_address=row["ip_address"],
        fqdn=row["fqdn"],
        record_type=row["record_type"],
        source=row["source"],
        mac_address=row["mac_address"],
        active=row["active"],
        last_seen=row["last_seen"],
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[PfSenseDnsOut])
async def list_pfsense_dns() -> list[PfSenseDnsOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pfsense_dns ORDER BY ip_address, fqdn"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=PfSenseDnsOut, status_code=201)
async def create_pfsense_dns(body: PfSenseDnsCreate) -> PfSenseDnsOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
            (body.dns_entry_id,),
        ).fetchone():
            raise HTTPException(409, f"dns_entry_id '{body.dns_entry_id}' already exists")

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO pfsense_dns
                (dns_entry_id, ip_address, fqdn, record_type, source,
                 mac_address, active, last_seen, last_probed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                body.dns_entry_id,
                body.ip_address,
                body.fqdn,
                body.record_type,
                body.source,
                body.mac_address,
                body.active,
                body.last_seen,
                body.last_probed,
            ),
        )
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (body.dns_entry_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "INSERT", "pfsense_dns", body.dns_entry_id, dict(row), gen
        )
    return _row_to_out(row)


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_pfsense_dns(entries: list[PfSenseDnsCreate]) -> dict:
    """Upsert many DNS entries at once — used by the discovery script."""
    created = 0
    updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "pfsense-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
                (body.dns_entry_id,),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE pfsense_dns
                    SET ip_address=?, fqdn=?, record_type=?, source=?,
                        mac_address=?, active=?, last_seen=?, last_probed=?,
                        updated_at=datetime('now')
                    WHERE dns_entry_id=?
                    """,
                    (
                        body.ip_address, body.fqdn, body.record_type, body.source,
                        body.mac_address, body.active, body.last_seen, body.last_probed,
                        body.dns_entry_id,
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO pfsense_dns
                        (dns_entry_id, ip_address, fqdn, record_type, source,
                         mac_address, active, last_seen, last_probed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body.dns_entry_id, body.ip_address, body.fqdn,
                        body.record_type, body.source, body.mac_address,
                        body.active, body.last_seen, body.last_probed,
                    ),
                )
                created += 1

            row = conn.execute(
                "SELECT * FROM pfsense_dns WHERE dns_entry_id=?",
                (body.dns_entry_id,),
            ).fetchone()
            action = "UPDATE" if existing else "INSERT"
            enqueue_for_all_peers(
                conn, action, "pfsense_dns", body.dns_entry_id, dict(row), gen
            )

    return {"created": created, "updated": updated, "total": created + updated}


@router.get("/{dns_entry_id}", response_model=PfSenseDnsOut)
async def get_pfsense_dns(dns_entry_id: str) -> PfSenseDnsOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")
    return _row_to_out(row)


@router.put("/{dns_entry_id}", response_model=PfSenseDnsOut)
async def update_pfsense_dns(dns_entry_id: str, body: PfSenseDnsUpdate) -> PfSenseDnsOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")

        update_data = body.model_dump(exclude_none=True)
        if not update_data:
            return _row_to_out(existing)

        set_parts = []
        values = []
        for field, val in update_data.items():
            set_parts.append(f"{field}=?")
            values.append(val)
        set_parts.append("updated_at=datetime('now')")
        values.append(dns_entry_id)

        gen = increment_gen(conn, "human")
        conn.execute(
            f"UPDATE pfsense_dns SET {', '.join(set_parts)} WHERE dns_entry_id=?",
            values,
        )
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "UPDATE", "pfsense_dns", dns_entry_id, dict(row), gen
        )
    return _row_to_out(row)


@router.delete("/{dns_entry_id}", status_code=204)
async def delete_pfsense_dns(dns_entry_id: str) -> None:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
            (dns_entry_id,),
        ).fetchone():
            raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")

        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,))
        enqueue_for_all_peers(
            conn, "DELETE", "pfsense_dns", dns_entry_id, None, gen
        )
