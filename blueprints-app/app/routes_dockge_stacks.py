"""routes_dockge_stacks.py — CRUD + probe for /api/v1/dockge-stacks"""

import asyncio
import json
import os

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import (
    DockgeStackCreate, DockgeStackOut, DockgeStackUpdate,
    DockgeStackServiceCreate, DockgeStackServiceOut, DockgeStackServiceUpdate,
)
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/dockge-stacks", tags=["dockge-stacks"])

_PROBE_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "..",
    ".claude", "skills", "blueprints-dockge-stacks", "scripts",
    "bp-dockge-stacks-probe.sh",
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> DockgeStackOut:
    return DockgeStackOut(
        stack_id=row["stack_id"],
        pve_host=row["pve_host"],
        source_vmid=row["source_vmid"],
        source_lxc_name=row["source_lxc_name"],
        stack_name=row["stack_name"],
        status=row["status"],
        compose_content=row["compose_content"],
        services_json=row["services_json"],
        ports_json=row["ports_json"],
        volumes_json=row["volumes_json"],
        env_file_exists=row["env_file_exists"] or 0,
        stacks_dir=row["stacks_dir"],
        vm_type=row["vm_type"],
        ip_address=row["ip_address"],
        parent_context=row["parent_context"],
        parent_stack_name=row["parent_stack_name"],
        obsolete=row["obsolete"] or 0,
        notes=row["notes"],
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _svc_row_to_out(row) -> DockgeStackServiceOut:
    return DockgeStackServiceOut(
        service_id=row["service_id"],
        stack_id=row["stack_id"],
        service_name=row["service_name"],
        image=row["image"],
        ports_json=row["ports_json"],
        volumes_json=row["volumes_json"],
        container_state=row["container_state"],
        container_id=row["container_id"],
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _upsert_stack(conn, body: DockgeStackCreate, gen: int) -> dict:
    """Insert or update a single dockge_stack row; return dict with row and was_existing."""
    existing = conn.execute(
        "SELECT stack_id FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE dockge_stacks SET
                pve_host=?, source_vmid=?, source_lxc_name=?, stack_name=?,
                status=?, compose_content=?, services_json=?, ports_json=?,
                volumes_json=?, env_file_exists=?, stacks_dir=?,
                vm_type=?, ip_address=?, parent_context=?, parent_stack_name=?,
                last_probed=?, updated_at=datetime('now')
            WHERE stack_id=?
            """,
            (body.pve_host, body.source_vmid, body.source_lxc_name,
             body.stack_name, body.status, body.compose_content,
             body.services_json, body.ports_json, body.volumes_json,
             body.env_file_exists, body.stacks_dir,
             body.vm_type, body.ip_address, body.parent_context,
             body.parent_stack_name, body.last_probed, body.stack_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO dockge_stacks
                (stack_id, pve_host, source_vmid, source_lxc_name, stack_name,
                 status, compose_content, services_json, ports_json, volumes_json,
                 env_file_exists, stacks_dir, vm_type, ip_address,
                 parent_context, parent_stack_name, obsolete, notes, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (body.stack_id, body.pve_host, body.source_vmid, body.source_lxc_name,
             body.stack_name, body.status, body.compose_content, body.services_json,
             body.ports_json, body.volumes_json, body.env_file_exists,
             body.stacks_dir, body.vm_type, body.ip_address,
             body.parent_context, body.parent_stack_name,
             body.obsolete, body.notes, body.last_probed),
        )
    row = conn.execute("SELECT * FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)).fetchone()
    enqueue_for_all_peers(
        conn, "UPDATE" if existing else "INSERT",
        "dockge_stacks", body.stack_id, dict(row), gen,
    )
    return {"row": row, "existing": bool(existing)}


def _upsert_service(conn, svc: DockgeStackServiceCreate, gen: int, existing: bool) -> None:
    """Insert or update a single dockge_stack_services row."""
    if existing:
        conn.execute(
            """
            UPDATE dockge_stack_services SET
                stack_id=?, service_name=?, image=?, ports_json=?,
                volumes_json=?, container_state=?, container_id=?,
                last_probed=?, updated_at=datetime('now')
            WHERE service_id=?
            """,
            (svc.stack_id, svc.service_name, svc.image, svc.ports_json,
             svc.volumes_json, svc.container_state, svc.container_id,
             svc.last_probed, svc.service_id),
        )
    else:
        conn.execute(
            """
            INSERT INTO dockge_stack_services
                (service_id, stack_id, service_name, image, ports_json,
                 volumes_json, container_state, container_id, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (svc.service_id, svc.stack_id, svc.service_name, svc.image,
             svc.ports_json, svc.volumes_json, svc.container_state,
             svc.container_id, svc.last_probed),
        )
    row = conn.execute(
        "SELECT * FROM dockge_stack_services WHERE service_id=?", (svc.service_id,)
    ).fetchone()
    enqueue_for_all_peers(
        conn, "UPDATE" if existing else "INSERT",
        "dockge_stack_services", svc.service_id, dict(row), gen,
    )


# ── Stack Routes ──────────────────────────────────────────────────────────────

@router.get("", response_model=list[DockgeStackOut])
async def list_dockge_stacks() -> list[DockgeStackOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM dockge_stacks ORDER BY pve_host, source_vmid, stack_name"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=DockgeStackOut, status_code=201)
async def create_dockge_stack(body: DockgeStackCreate) -> DockgeStackOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT stack_id FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)
        ).fetchone():
            raise HTTPException(409, f"stack_id '{body.stack_id}' already exists")
        gen = increment_gen(conn, "human")
        result = _upsert_stack(conn, body, gen)
    return _row_to_out(result["row"])


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_dockge_stacks(entries: list[DockgeStackCreate]) -> dict:
    """Upsert many Dockge stack records — used by external callers."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "dockge-probe")
        for body in entries:
            result = _upsert_stack(conn, body, gen)
            if result["existing"]:
                updated += 1
            else:
                created += 1
    return {"created": created, "updated": updated, "total": created + updated}


# ── Service Routes ────────────────────────────────────────────────────────────

@router.get("/services", response_model=list[DockgeStackServiceOut])
async def list_dockge_stack_services(stack_id: str | None = None) -> list[DockgeStackServiceOut]:
    with get_conn() as conn:
        if stack_id:
            rows = conn.execute(
                "SELECT * FROM dockge_stack_services WHERE stack_id=? ORDER BY service_name",
                (stack_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM dockge_stack_services ORDER BY stack_id, service_name"
            ).fetchall()
    return [_svc_row_to_out(r) for r in rows]


@router.post("/services/bulk", response_model=dict, status_code=200)
async def bulk_upsert_dockge_stack_services(entries: list[DockgeStackServiceCreate]) -> dict:
    """Upsert many service rows — used by the probe script."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "dockge-probe")
        for svc in entries:
            exists = bool(conn.execute(
                "SELECT service_id FROM dockge_stack_services WHERE service_id=?",
                (svc.service_id,),
            ).fetchone())
            _upsert_service(conn, svc, gen, exists)
            if exists:
                updated += 1
            else:
                created += 1
    return {"created": created, "updated": updated, "total": created + updated}


@router.delete("/services/{service_id}", status_code=204)
async def delete_dockge_stack_service(service_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM dockge_stack_services WHERE service_id=?", (service_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"service_id '{service_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "dockge_stack_services", service_id, {}, gen)


# ── Probe ─────────────────────────────────────────────────────────────────────

@router.get("/probe/status", response_model=dict)
async def probe_status() -> dict:
    """Return whether SSH keys for Dockge probing are configured."""
    from .ssh import probe_status_for_host_type
    return probe_status_for_host_type("pve")


@router.post("/probe", response_model=dict)
async def probe_dockge_stacks() -> dict:
    """
    Run bp-dockge-stacks-probe.sh.
    Parses ##ENTRIES## (stacks), ##SERVICES## (per-container rows), ##STATS##.
    """
    script = os.path.realpath(_PROBE_SCRIPT)
    if not os.path.isfile(script):
        raise HTTPException(500, f"Probe script not found: {script}")

    from .ssh import resolve_env_key, SshKeyMissing

    env = {**os.environ}
    for key_var in ("PROXMOX_SSH_KEY", "VM_SSH_KEY", "LXC_SSH_KEY",
                    "CITADEL_SSH_KEY", "XARTA_NODE_SSH_KEY"):
        try:
            env[key_var] = resolve_env_key(key_var)
        except SshKeyMissing:
            pass

    try:
        proc = await asyncio.create_subprocess_exec(
            "bash", script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=300)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Probe script timed out after 300s")

    if proc.returncode != 0:
        err = stderr.decode(errors="replace").strip()
        raise HTTPException(502, f"Probe script failed (exit {proc.returncode}): {err[:400]}")

    text = stdout.decode(errors="replace")
    entries_raw: list = []
    services_raw: list = []
    stats_raw: dict = {}
    for line in text.splitlines():
        if line.startswith("##ENTRIES##"):
            try:
                entries_raw = json.loads(line[len("##ENTRIES##"):].strip())
            except json.JSONDecodeError:
                pass
        elif line.startswith("##SERVICES##"):
            try:
                services_raw = json.loads(line[len("##SERVICES##"):].strip())
            except json.JSONDecodeError:
                pass
        elif line.startswith("##STATS##"):
            try:
                stats_raw = json.loads(line[len("##STATS##"):].strip())
            except json.JSONDecodeError:
                pass

    if not entries_raw:
        raise HTTPException(502, "Probe script produced no entries")

    stacks_created = stacks_updated = 0
    svcs_created = svcs_updated = 0

    with get_conn() as conn:
        gen = increment_gen(conn, "dockge-probe")

        for entry in entries_raw:
            sid = entry.get("stack_id")
            if not sid:
                continue
            body = DockgeStackCreate(
                stack_id=sid,
                pve_host=entry.get("pve_host", ""),
                source_vmid=int(entry.get("source_vmid", 0)),
                source_lxc_name=entry.get("source_lxc_name"),
                stack_name=entry.get("stack_name", ""),
                status=entry.get("status"),
                compose_content=entry.get("compose_content"),
                services_json=entry.get("services_json"),
                ports_json=entry.get("ports_json"),
                volumes_json=entry.get("volumes_json"),
                env_file_exists=entry.get("env_file_exists", 0),
                stacks_dir=entry.get("stacks_dir"),
                vm_type=entry.get("vm_type"),
                ip_address=entry.get("ip_address"),
                parent_context=entry.get("parent_context"),
                parent_stack_name=entry.get("parent_stack_name"),
                last_probed=entry.get("last_probed"),
            )
            result = _upsert_stack(conn, body, gen)
            if result["existing"]:
                stacks_updated += 1
            else:
                stacks_created += 1

        for svc_entry in services_raw:
            svc_id = svc_entry.get("service_id")
            if not svc_id:
                continue
            exists = bool(conn.execute(
                "SELECT service_id FROM dockge_stack_services WHERE service_id=?",
                (svc_id,),
            ).fetchone())
            svc = DockgeStackServiceCreate(
                service_id=svc_id,
                stack_id=svc_entry.get("stack_id", ""),
                service_name=svc_entry.get("service_name", ""),
                image=svc_entry.get("image"),
                ports_json=svc_entry.get("ports_json"),
                volumes_json=svc_entry.get("volumes_json"),
                container_state=svc_entry.get("container_state"),
                container_id=svc_entry.get("container_id"),
                last_probed=svc_entry.get("last_probed"),
            )
            _upsert_service(conn, svc, gen, exists)
            if exists:
                svcs_updated += 1
            else:
                svcs_created += 1

    return {
        "stacks_created": stacks_created,
        "stacks_updated": stacks_updated,
        "stacks_total": stacks_created + stacks_updated,
        "services_created": svcs_created,
        "services_updated": svcs_updated,
        "services_total": svcs_created + svcs_updated,
        "dockge_instances_probed": stats_raw.get("dockge_instances_probed", 0),
        "machines_probed": stats_raw.get("machines_probed", 0),
    }


# ── Single-record CRUD ────────────────────────────────────────────────────────

@router.get("/{stack_id}", response_model=DockgeStackOut)
async def get_dockge_stack(stack_id: str) -> DockgeStackOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM dockge_stacks WHERE stack_id=?", (stack_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"stack_id '{stack_id}' not found")
    return _row_to_out(row)


@router.put("/{stack_id}", response_model=DockgeStackOut)
async def update_dockge_stack(stack_id: str, body: DockgeStackUpdate) -> DockgeStackOut:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [stack_id]
    with get_conn() as conn:
        result = conn.execute(
            f"UPDATE dockge_stacks SET {set_clause}, updated_at=datetime('now') WHERE stack_id=?",
            values,
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"stack_id '{stack_id}' not found")
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM dockge_stacks WHERE stack_id=?", (stack_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "dockge_stacks", stack_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{stack_id}", status_code=204)
async def delete_dockge_stack(stack_id: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM dockge_stack_services WHERE stack_id=?", (stack_id,))
        result = conn.execute(
            "DELETE FROM dockge_stacks WHERE stack_id=?", (stack_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"stack_id '{stack_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "dockge_stacks", stack_id, {}, gen)
