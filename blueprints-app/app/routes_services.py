"""routes_services.py — CRUD for /api/v1/services"""

import json

from fastapi import APIRouter, HTTPException

from . import config as cfg
from .db import get_conn, increment_gen
from .models import ServiceCreate, ServiceOut, ServiceUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/services", tags=["services"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _loads(val: str | None) -> list | None:
    """Deserialise a JSON text column to a Python list, or None."""
    if not val:
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return None


def _dumps(val) -> str | None:
    """Serialise a list/dict to JSON text for storage, or None."""
    return json.dumps(val) if val is not None else None


def _row_to_out(row) -> ServiceOut:
    keys = row.keys()
    return ServiceOut(
        service_id=row["service_id"],
        name=row["name"],
        description=row["description"],
        host_machine=row["host_machine"],
        vm_or_lxc=row["vm_or_lxc"],
        ports=_loads(row["ports"]),
        caddy_routes=_loads(row["caddy_routes"]),
        dns_info=row["dns_info"],
        credential_hints=row["credential_hints"],
        dependencies=_loads(row["dependencies"]),
        project_status=row["project_status"] or "deployed",
        tags=_loads(row["tags"]),
        links=_loads(row["links"]),
        host_machine_id=row["host_machine_id"] if "host_machine_id" in keys else None,
        service_kind=row["service_kind"] if "service_kind" in keys else None,
        exposure_level=row["exposure_level"] if "exposure_level" in keys else None,
        health_path=row["health_path"] if "health_path" in keys else None,
        health_expected_status=row["health_expected_status"] if "health_expected_status" in keys else None,
        runtime_notes_json=_loads(row["runtime_notes_json"]) if "runtime_notes_json" in keys else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ServiceOut])
async def list_services() -> list[ServiceOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM services ORDER BY name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=ServiceOut, status_code=201)
async def create_service(body: ServiceCreate) -> ServiceOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT service_id FROM services WHERE service_id=?",
            (body.service_id,),
        ).fetchone()
        if existing:
            raise HTTPException(409, f"service_id '{body.service_id}' already exists")

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO services
                (service_id, name, description, host_machine, vm_or_lxc,
                 ports, caddy_routes, dns_info, credential_hints, dependencies,
                 project_status, tags, links,
                 host_machine_id, service_kind, exposure_level,
                 health_path, health_expected_status, runtime_notes_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                body.service_id,
                body.name,
                body.description,
                body.host_machine,
                body.vm_or_lxc,
                _dumps(body.ports),
                _dumps(body.caddy_routes),
                body.dns_info,
                body.credential_hints,
                _dumps(body.dependencies),
                body.project_status,
                _dumps(body.tags),
                _dumps(body.links),
                body.host_machine_id,
                body.service_kind,
                body.exposure_level,
                body.health_path,
                body.health_expected_status,
                _dumps(body.runtime_notes_json),
            ),
        )
        row = conn.execute(
            "SELECT * FROM services WHERE service_id=?", (body.service_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "INSERT", "services", body.service_id, dict(row), gen
        )
    return _row_to_out(row)


@router.get("/{service_id}", response_model=ServiceOut)
async def get_service(service_id: str) -> ServiceOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM services WHERE service_id=?", (service_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"service '{service_id}' not found")
    return _row_to_out(row)


@router.put("/{service_id}", response_model=ServiceOut)
async def update_service(service_id: str, body: ServiceUpdate) -> ServiceOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM services WHERE service_id=?", (service_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"service '{service_id}' not found")

        update_data = body.model_dump(exclude_none=True)
        if not update_data:
            return _row_to_out(existing)

        # Build dynamic SET clause
        json_fields = {"ports", "caddy_routes", "dependencies", "tags", "links", "runtime_notes_json"}
        set_parts = []
        values = []
        for field, val in update_data.items():
            set_parts.append(f"{field}=?")
            values.append(_dumps(val) if field in json_fields else val)
        set_parts.append("updated_at=datetime('now')")
        values.append(service_id)

        gen = increment_gen(conn, "human")
        conn.execute(
            f"UPDATE services SET {', '.join(set_parts)} WHERE service_id=?",
            values,
        )
        row = conn.execute(
            "SELECT * FROM services WHERE service_id=?", (service_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "UPDATE", "services", service_id, dict(row), gen
        )
    return _row_to_out(row)


@router.delete("/{service_id}", status_code=204)
async def delete_service(service_id: str) -> None:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT service_id FROM services WHERE service_id=?", (service_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"service '{service_id}' not found")

        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM services WHERE service_id=?", (service_id,))
        enqueue_for_all_peers(
            conn, "DELETE", "services", service_id, None, gen
        )
