"""routes_caddy_configs.py — CRUD + probe for /api/v1/caddy-configs"""

import asyncio
import json
import os

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import CaddyConfigCreate, CaddyConfigOut, CaddyConfigUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/caddy-configs", tags=["caddy-configs"])

_PROBE_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "..",
    ".claude", "skills", "blueprints-caddy-configs", "scripts",
    "bp-caddy-configs-probe.sh",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> CaddyConfigOut:
    return CaddyConfigOut(
        caddy_id=row["caddy_id"],
        pve_host=row["pve_host"],
        source_vmid=row["source_vmid"],
        source_lxc_name=row["source_lxc_name"],
        caddyfile_path=row["caddyfile_path"],
        caddyfile_content=row["caddyfile_content"],
        domains_json=row["domains_json"],
        upstreams_json=row["upstreams_json"],
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[CaddyConfigOut])
async def list_caddy_configs() -> list[CaddyConfigOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM caddy_configs ORDER BY pve_host, source_vmid"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=CaddyConfigOut, status_code=201)
async def create_caddy_config(body: CaddyConfigCreate) -> CaddyConfigOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT caddy_id FROM caddy_configs WHERE caddy_id=?", (body.caddy_id,)
        ).fetchone():
            raise HTTPException(409, f"caddy_id '{body.caddy_id}' already exists")
        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO caddy_configs
                (caddy_id, pve_host, source_vmid, source_lxc_name,
                 caddyfile_path, caddyfile_content, domains_json,
                 upstreams_json, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (body.caddy_id, body.pve_host, body.source_vmid,
             body.source_lxc_name, body.caddyfile_path, body.caddyfile_content,
             body.domains_json, body.upstreams_json, body.last_probed),
        )
        row = conn.execute(
            "SELECT * FROM caddy_configs WHERE caddy_id=?", (body.caddy_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "caddy_configs", body.caddy_id, dict(row), gen)
    return _row_to_out(row)


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_caddy_configs(entries: list[CaddyConfigCreate]) -> dict:
    """Upsert many Caddy config records — used by the probe script."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "caddy-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT caddy_id FROM caddy_configs WHERE caddy_id=?", (body.caddy_id,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE caddy_configs SET
                        pve_host=?, source_vmid=?, source_lxc_name=?,
                        caddyfile_path=?, caddyfile_content=?, domains_json=?,
                        upstreams_json=?, last_probed=?, updated_at=datetime('now')
                    WHERE caddy_id=?
                    """,
                    (body.pve_host, body.source_vmid, body.source_lxc_name,
                     body.caddyfile_path, body.caddyfile_content, body.domains_json,
                     body.upstreams_json, body.last_probed, body.caddy_id),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO caddy_configs
                        (caddy_id, pve_host, source_vmid, source_lxc_name,
                         caddyfile_path, caddyfile_content, domains_json,
                         upstreams_json, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (body.caddy_id, body.pve_host, body.source_vmid,
                     body.source_lxc_name, body.caddyfile_path,
                     body.caddyfile_content, body.domains_json,
                     body.upstreams_json, body.last_probed),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM caddy_configs WHERE caddy_id=?", (body.caddy_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "caddy_configs", body.caddy_id, dict(row), gen,
            )
    return {"created": created, "updated": updated, "total": created + updated}


# ── Probe ─────────────────────────────────────────────────────────────────────

@router.get("/probe/status", response_model=dict)
async def probe_status() -> dict:
    """Return whether the Proxmox probe key is configured (used for pct exec to reach LXCs)."""
    from .ssh import probe_status_for_host_type
    return probe_status_for_host_type("pve")


@router.post("/probe", response_model=dict)
async def probe_caddy_configs() -> dict:
    """
    Run bp-caddy-configs-probe.sh, parse ##ENTRIES## from stdout,
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
        gen = increment_gen(conn, "caddy-probe")
        for entry in entries_raw:
            cid = entry.get("caddy_id")
            if not cid:
                continue
            existing = conn.execute(
                "SELECT caddy_id FROM caddy_configs WHERE caddy_id=?", (cid,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE caddy_configs SET
                        pve_host=?, source_vmid=?, source_lxc_name=?,
                        caddyfile_path=?, caddyfile_content=?, domains_json=?,
                        upstreams_json=?, last_probed=?, updated_at=datetime('now')
                    WHERE caddy_id=?
                    """,
                    (entry.get("pve_host"), entry.get("source_vmid"),
                     entry.get("source_lxc_name"), entry.get("caddyfile_path"),
                     entry.get("caddyfile_content"), entry.get("domains_json"),
                     entry.get("upstreams_json"), entry.get("last_probed"), cid),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO caddy_configs
                        (caddy_id, pve_host, source_vmid, source_lxc_name,
                         caddyfile_path, caddyfile_content, domains_json,
                         upstreams_json, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (cid, entry.get("pve_host"), entry.get("source_vmid"),
                     entry.get("source_lxc_name"), entry.get("caddyfile_path"),
                     entry.get("caddyfile_content"), entry.get("domains_json"),
                     entry.get("upstreams_json"), entry.get("last_probed")),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM caddy_configs WHERE caddy_id=?", (cid,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "caddy_configs", cid, dict(row), gen,
            )

    return {
        "created": created,
        "updated": updated,
        "total": created + updated,
        "pve_hosts_probed": stats_raw.get("pve_hosts_probed", 0),
        "lxcs_with_caddy": stats_raw.get("lxcs_with_caddy", 0),
    }


# ── Single-record CRUD ────────────────────────────────────────────────────────

@router.get("/{caddy_id}", response_model=CaddyConfigOut)
async def get_caddy_config(caddy_id: str) -> CaddyConfigOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM caddy_configs WHERE caddy_id=?", (caddy_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"caddy_id '{caddy_id}' not found")
    return _row_to_out(row)


@router.put("/{caddy_id}", response_model=CaddyConfigOut)
async def update_caddy_config(caddy_id: str, body: CaddyConfigUpdate) -> CaddyConfigOut:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [caddy_id]
    with get_conn() as conn:
        result = conn.execute(
            f"UPDATE caddy_configs SET {set_clause}, updated_at=datetime('now') WHERE caddy_id=?",
            values,
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"caddy_id '{caddy_id}' not found")
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM caddy_configs WHERE caddy_id=?", (caddy_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "caddy_configs", caddy_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{caddy_id}", status_code=204)
async def delete_caddy_config(caddy_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM caddy_configs WHERE caddy_id=?", (caddy_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"caddy_id '{caddy_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "caddy_configs", caddy_id, {}, gen)
