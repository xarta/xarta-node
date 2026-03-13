"""routes_dockge_stacks.py — CRUD + probe for /api/v1/dockge-stacks"""

import asyncio
import json
import os

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import DockgeStackCreate, DockgeStackOut, DockgeStackUpdate
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
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

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
        conn.execute(
            """
            INSERT INTO dockge_stacks
                (stack_id, pve_host, source_vmid, source_lxc_name, stack_name,
                 status, compose_content, services_json, ports_json, volumes_json,
                 env_file_exists, stacks_dir, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (body.stack_id, body.pve_host, body.source_vmid, body.source_lxc_name,
             body.stack_name, body.status, body.compose_content, body.services_json,
             body.ports_json, body.volumes_json, body.env_file_exists,
             body.stacks_dir, body.last_probed),
        )
        row = conn.execute(
            "SELECT * FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "dockge_stacks", body.stack_id, dict(row), gen)
    return _row_to_out(row)


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_dockge_stacks(entries: list[DockgeStackCreate]) -> dict:
    """Upsert many Dockge stack records — used by the probe script."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "dockge-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT stack_id FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE dockge_stacks SET
                        pve_host=?, source_vmid=?, source_lxc_name=?, stack_name=?,
                        status=?, compose_content=?, services_json=?, ports_json=?,
                        volumes_json=?, env_file_exists=?, stacks_dir=?, last_probed=?,
                        updated_at=datetime('now')
                    WHERE stack_id=?
                    """,
                    (body.pve_host, body.source_vmid, body.source_lxc_name,
                     body.stack_name, body.status, body.compose_content,
                     body.services_json, body.ports_json, body.volumes_json,
                     body.env_file_exists, body.stacks_dir, body.last_probed,
                     body.stack_id),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO dockge_stacks
                        (stack_id, pve_host, source_vmid, source_lxc_name, stack_name,
                         status, compose_content, services_json, ports_json, volumes_json,
                         env_file_exists, stacks_dir, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (body.stack_id, body.pve_host, body.source_vmid,
                     body.source_lxc_name, body.stack_name, body.status,
                     body.compose_content, body.services_json, body.ports_json,
                     body.volumes_json, body.env_file_exists, body.stacks_dir,
                     body.last_probed),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM dockge_stacks WHERE stack_id=?", (body.stack_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "dockge_stacks", body.stack_id, dict(row), gen,
            )
    return {"created": created, "updated": updated, "total": created + updated}


# ── Probe ─────────────────────────────────────────────────────────────────────

@router.get("/probe/status", response_model=dict)
async def probe_status() -> dict:
    """Return whether the Proxmox key is configured (needed for pct exec into Dockge LXCs)."""
    from .ssh import probe_status_for_host_type
    return probe_status_for_host_type("pve")


@router.post("/probe", response_model=dict)
async def probe_dockge_stacks() -> dict:
    """
    Run bp-dockge-stacks-probe.sh, parse ##ENTRIES## from stdout,
    upsert all records in-process.
    """
    script = os.path.realpath(_PROBE_SCRIPT)
    if not os.path.isfile(script):
        raise HTTPException(500, f"Probe script not found: {script}")

    from .ssh import probe_status_for_host_type, resolve_env_key, SshKeyMissing
    status = probe_status_for_host_type("pve")
    if not status["configured"]:
        raise HTTPException(503, status["reason"])
    try:
        key_path = resolve_env_key("PROXMOX_SSH_KEY")
    except SshKeyMissing as exc:
        raise HTTPException(503, str(exc))

    env = {**os.environ, "PROXMOX_SSH_KEY": key_path}

    try:
        proc = await asyncio.create_subprocess_exec(
            "bash", script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    except asyncio.TimeoutError:
        raise HTTPException(504, "Probe script timed out after 120s")

    if proc.returncode not in (0,):
        err = stderr.decode(errors="replace").strip()
        raise HTTPException(502, f"Probe script failed (exit {proc.returncode}): {err[:400]}")

    text = stdout.decode(errors="replace")
    entries_raw: list = []
    stats_raw: dict = {}
    for line in text.splitlines():
        if line.startswith("##ENTRIES##"):
            try:
                entries_raw = json.loads(line[len("##ENTRIES##"):].strip())
            except json.JSONDecodeError:
                pass
        elif line.startswith("##STATS##"):
            try:
                stats_raw = json.loads(line[len("##STATS##"):].strip())
            except json.JSONDecodeError:
                pass

    if not entries_raw:
        raise HTTPException(502, "Probe script produced no entries")

    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "dockge-probe")
        for entry in entries_raw:
            sid = entry.get("stack_id")
            if not sid:
                continue
            existing = conn.execute(
                "SELECT stack_id FROM dockge_stacks WHERE stack_id=?", (sid,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE dockge_stacks SET
                        pve_host=?, source_vmid=?, source_lxc_name=?, stack_name=?,
                        status=?, compose_content=?, services_json=?, ports_json=?,
                        volumes_json=?, env_file_exists=?, stacks_dir=?, last_probed=?,
                        updated_at=datetime('now')
                    WHERE stack_id=?
                    """,
                    (entry.get("pve_host"), entry.get("source_vmid"),
                     entry.get("source_lxc_name"), entry.get("stack_name"),
                     entry.get("status"), entry.get("compose_content"),
                     entry.get("services_json"), entry.get("ports_json"),
                     entry.get("volumes_json"), entry.get("env_file_exists", 0),
                     entry.get("stacks_dir"), entry.get("last_probed"), sid),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO dockge_stacks
                        (stack_id, pve_host, source_vmid, source_lxc_name, stack_name,
                         status, compose_content, services_json, ports_json, volumes_json,
                         env_file_exists, stacks_dir, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (sid, entry.get("pve_host"), entry.get("source_vmid"),
                     entry.get("source_lxc_name"), entry.get("stack_name"),
                     entry.get("status"), entry.get("compose_content"),
                     entry.get("services_json"), entry.get("ports_json"),
                     entry.get("volumes_json"), entry.get("env_file_exists", 0),
                     entry.get("stacks_dir"), entry.get("last_probed")),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM dockge_stacks WHERE stack_id=?", (sid,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "dockge_stacks", sid, dict(row), gen,
            )

    return {
        "created": created,
        "updated": updated,
        "total": created + updated,
        "dockge_instances_probed": stats_raw.get("dockge_instances_probed", 0),
        "stacks_found": stats_raw.get("stacks_found", 0),
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
        result = conn.execute(
            "DELETE FROM dockge_stacks WHERE stack_id=?", (stack_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"stack_id '{stack_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "dockge_stacks", stack_id, {}, gen)
