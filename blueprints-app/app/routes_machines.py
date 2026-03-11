"""routes_machines.py — CRUD for /api/v1/machines"""

import json

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import MachineCreate, MachineOut, MachineUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/machines", tags=["machines"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _loads(val: str | None) -> list | None:
    if not val:
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def _row_to_out(row) -> MachineOut:
    keys = row.keys()
    return MachineOut(
        machine_id=row["machine_id"],
        name=row["name"],
        type=row["type"],
        parent_machine_id=row["parent_machine_id"],
        ip_addresses=_loads(row["ip_addresses"]),
        description=row["description"],
        machine_kind=row["machine_kind"] if "machine_kind" in keys else None,
        platform=row["platform"] if "platform" in keys else None,
        status=row["status"] if "status" in keys else None,
        labels=_loads(row["labels"]) if "labels" in keys else None,
        properties_json=_loads(row["properties_json"]) if "properties_json" in keys else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[MachineOut])
async def list_machines() -> list[MachineOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM machines ORDER BY name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=MachineOut, status_code=201)
async def create_machine(body: MachineCreate) -> MachineOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT machine_id FROM machines WHERE machine_id=?",
            (body.machine_id,),
        ).fetchone():
            raise HTTPException(409, f"machine_id '{body.machine_id}' already exists")

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO machines
                (machine_id, name, type, parent_machine_id, ip_addresses, description,
                 machine_kind, platform, status, labels, properties_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                body.machine_id,
                body.name,
                body.type,
                body.parent_machine_id,
                json.dumps(body.ip_addresses) if body.ip_addresses else None,
                body.description,
                body.machine_kind,
                body.platform,
                body.status,
                json.dumps(body.labels) if body.labels else None,
                json.dumps(body.properties_json) if body.properties_json else None,
            ),
        )
        row = conn.execute(
            "SELECT * FROM machines WHERE machine_id=?", (body.machine_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "INSERT", "machines", body.machine_id, dict(row), gen
        )
    return _row_to_out(row)


@router.get("/{machine_id}", response_model=MachineOut)
async def get_machine(machine_id: str) -> MachineOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM machines WHERE machine_id=?", (machine_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"machine '{machine_id}' not found")
    return _row_to_out(row)


@router.put("/{machine_id}", response_model=MachineOut)
async def update_machine(machine_id: str, body: MachineUpdate) -> MachineOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM machines WHERE machine_id=?", (machine_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"machine '{machine_id}' not found")

        update_data = body.model_dump(exclude_none=True)
        if not update_data:
            return _row_to_out(existing)

        set_parts = []
        values = []
        json_fields = {"ip_addresses", "labels", "properties_json"}
        for field, val in update_data.items():
            set_parts.append(f"{field}=?")
            values.append(json.dumps(val) if field in json_fields else val)
        set_parts.append("updated_at=datetime('now')")
        values.append(machine_id)

        gen = increment_gen(conn, "human")
        conn.execute(
            f"UPDATE machines SET {', '.join(set_parts)} WHERE machine_id=?",
            values,
        )
        row = conn.execute(
            "SELECT * FROM machines WHERE machine_id=?", (machine_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "UPDATE", "machines", machine_id, dict(row), gen
        )
    return _row_to_out(row)


@router.delete("/{machine_id}", status_code=204)
async def delete_machine(machine_id: str) -> None:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT machine_id FROM machines WHERE machine_id=?", (machine_id,)
        ).fetchone():
            raise HTTPException(404, f"machine '{machine_id}' not found")

        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM machines WHERE machine_id=?", (machine_id,))
        enqueue_for_all_peers(
            conn, "DELETE", "machines", machine_id, None, gen
        )
