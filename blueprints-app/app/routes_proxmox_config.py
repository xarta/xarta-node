"""routes_proxmox_config.py — CRUD + probe for /api/v1/proxmox-config"""

import asyncio
import json
import os
import subprocess
import time as _time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import ProxmoxConfigCreate, ProxmoxConfigOut, ProxmoxConfigUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/proxmox-config", tags=["proxmox-config"])

_PROBE_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "..",
    ".claude", "skills", "blueprints-proxmox-discovery", "scripts",
    "bp-proxmox-config-probe.sh",
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _int(val) -> int | None:
    """Coerce a DB value (int, numeric string, or empty string) to int or None."""
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _row_to_out(row) -> ProxmoxConfigOut:
    return ProxmoxConfigOut(
        config_id=row["config_id"],
        pve_host=row["pve_host"],
        pve_name=row["pve_name"],
        vmid=_int(row["vmid"]),
        vm_type=row["vm_type"],
        name=row["name"],
        status=row["status"],
        cores=_int(row["cores"]),
        memory_mb=_int(row["memory_mb"]),
        rootfs=row["rootfs"],
        ip_config=row["ip_config"],
        ip_address=row["ip_address"],
        gateway=row["gateway"],
        mac_address=row["mac_address"],
        vlan_tag=_int(row["vlan_tag"]),
        tags=row["tags"],
        mountpoints_json=row["mountpoints_json"],
        raw_conf=row["raw_conf"],
        vlans_json=row["vlans_json"],
        has_docker=_int(row["has_docker"]),
        dockge_stacks_dir=row["dockge_stacks_dir"],
        has_portainer=_int(row["has_portainer"]),
        portainer_method=row["portainer_method"],
        has_caddy=_int(row["has_caddy"]),
        caddy_conf_path=row["caddy_conf_path"],
        last_probed=row["last_probed"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ProxmoxConfigOut])
async def list_proxmox_config() -> list[ProxmoxConfigOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM proxmox_config ORDER BY pve_host, vmid"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=ProxmoxConfigOut, status_code=201)
async def create_proxmox_config(body: ProxmoxConfigCreate) -> ProxmoxConfigOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT config_id FROM proxmox_config WHERE config_id=?",
            (body.config_id,),
        ).fetchone():
            raise HTTPException(409, f"config_id '{body.config_id}' already exists")
        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO proxmox_config
                (config_id, pve_host, pve_name, vmid, vm_type, name, status,
                 cores, memory_mb, rootfs, ip_config, ip_address, gateway,
                 mac_address, vlan_tag, tags, mountpoints_json, raw_conf,
                 vlans_json, has_docker, dockge_stacks_dir, has_portainer,
                 portainer_method, has_caddy, caddy_conf_path, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (body.config_id, body.pve_host, body.pve_name, body.vmid, body.vm_type,
             body.name, body.status, body.cores, body.memory_mb, body.rootfs,
             body.ip_config, body.ip_address, body.gateway, body.mac_address,
             body.vlan_tag, body.tags, body.mountpoints_json, body.raw_conf,
             body.vlans_json, body.has_docker, body.dockge_stacks_dir,
             body.has_portainer, body.portainer_method, body.has_caddy,
             body.caddy_conf_path, body.last_probed),
        )
        row = conn.execute(
            "SELECT * FROM proxmox_config WHERE config_id=?", (body.config_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "proxmox_config", body.config_id, dict(row), gen)
    return _row_to_out(row)


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_proxmox_config(entries: list[ProxmoxConfigCreate]) -> dict:
    """Upsert many proxmox config records — used by the probe script."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "proxmox-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT config_id FROM proxmox_config WHERE config_id=?",
                (body.config_id,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE proxmox_config SET
                        pve_host=?, pve_name=?, vmid=?, vm_type=?, name=?, status=?,
                        cores=?, memory_mb=?, rootfs=?, ip_config=?, ip_address=?,
                        gateway=?, mac_address=?, vlan_tag=?, tags=?,
                        mountpoints_json=?, raw_conf=?,
                        vlans_json=?, has_docker=?, dockge_stacks_dir=?,
                        has_portainer=?, portainer_method=?,
                        has_caddy=?, caddy_conf_path=?,
                        last_probed=?, updated_at=datetime('now')
                    WHERE config_id=?
                    """,
                    (body.pve_host, body.pve_name, body.vmid, body.vm_type,
                     body.name, body.status, body.cores, body.memory_mb,
                     body.rootfs, body.ip_config, body.ip_address, body.gateway,
                     body.mac_address, body.vlan_tag, body.tags,
                     body.mountpoints_json, body.raw_conf,
                     body.vlans_json, body.has_docker, body.dockge_stacks_dir,
                     body.has_portainer, body.portainer_method,
                     body.has_caddy, body.caddy_conf_path,
                     body.last_probed, body.config_id),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO proxmox_config
                        (config_id, pve_host, pve_name, vmid, vm_type, name, status,
                         cores, memory_mb, rootfs, ip_config, ip_address, gateway,
                         mac_address, vlan_tag, tags, mountpoints_json, raw_conf,
                         vlans_json, has_docker, dockge_stacks_dir, has_portainer,
                         portainer_method, has_caddy, caddy_conf_path, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (body.config_id, body.pve_host, body.pve_name, body.vmid,
                     body.vm_type, body.name, body.status, body.cores,
                     body.memory_mb, body.rootfs, body.ip_config, body.ip_address,
                     body.gateway, body.mac_address, body.vlan_tag, body.tags,
                     body.mountpoints_json, body.raw_conf,
                     body.vlans_json, body.has_docker, body.dockge_stacks_dir,
                     body.has_portainer, body.portainer_method,
                     body.has_caddy, body.caddy_conf_path, body.last_probed),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM proxmox_config WHERE config_id=?", (body.config_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "proxmox_config", body.config_id, dict(row), gen,
            )
    return {"created": created, "updated": updated, "total": created + updated}


# ── Probe ─────────────────────────────────────────────────────────────────────

@router.get("/probe/status", response_model=dict)
async def probe_status() -> dict:
    """Return whether the Proxmox probe key is configured on this node."""
    key_path = os.environ.get("PROXMOX_SSH_KEY", "")
    key_present = bool(key_path) and os.path.isfile(key_path)
    if not key_present:
        reason = (
            "PROXMOX_SSH_KEY not set" if not key_path
            else f"key file not found: {key_path}"
        )
        return {"configured": False, "ssh_key_present": False, "reason": reason}
    return {"configured": True, "ssh_key_present": True, "reason": ""}


@router.post("/probe", response_model=dict)
async def probe_proxmox_config() -> dict:
    """
    Run bp-proxmox-config-probe.sh, parse ##ENTRIES## and ##NETS## from stdout,
    upsert all records in-process (no re-entrant HTTP call).
    """
    script = os.path.realpath(_PROBE_SCRIPT)
    if not os.path.isfile(script):
        raise HTTPException(500, f"Probe script not found: {script}")

    key_path = os.environ.get("PROXMOX_SSH_KEY", "")
    if not key_path or not os.path.isfile(key_path):
        raise HTTPException(503, "PROXMOX_SSH_KEY not configured or key file missing")

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
    nets_raw: list = []
    stats_raw: dict = {}
    for line in text.splitlines():
        if line.startswith("##ENTRIES##"):
            try:
                entries_raw = json.loads(line[len("##ENTRIES##"):].strip())
            except json.JSONDecodeError:
                pass
        elif line.startswith("##NETS##"):
            try:
                nets_raw = json.loads(line[len("##NETS##"):].strip())
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
        gen = increment_gen(conn, "proxmox-probe")
        for entry in entries_raw:
            cid = entry.get("config_id")
            if not cid:
                continue
            existing = conn.execute(
                "SELECT config_id FROM proxmox_config WHERE config_id=?", (cid,)
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE proxmox_config SET
                        pve_host=?, pve_name=?, vmid=?, vm_type=?, name=?, status=?,
                        cores=?, memory_mb=?, rootfs=?, ip_config=?, ip_address=?,
                        gateway=?, mac_address=?, vlan_tag=?, tags=?,
                        mountpoints_json=?, raw_conf=?,
                        vlans_json=?, has_docker=?, dockge_stacks_dir=?,
                        has_portainer=?, portainer_method=?,
                        has_caddy=?, caddy_conf_path=?,
                        last_probed=?, updated_at=datetime('now')
                    WHERE config_id=?
                    """,
                    (entry.get("pve_host"), entry.get("pve_name"), entry.get("vmid"),
                     entry.get("vm_type"), entry.get("name"), entry.get("status"),
                     entry.get("cores"), entry.get("memory_mb"), entry.get("rootfs"),
                     entry.get("ip_config"), entry.get("ip_address"), entry.get("gateway"),
                     entry.get("mac_address"), entry.get("vlan_tag"), entry.get("tags"),
                     entry.get("mountpoints_json"), entry.get("raw_conf"),
                     entry.get("vlans_json"), entry.get("has_docker"),
                     entry.get("dockge_stacks_dir"), entry.get("has_portainer"),
                     entry.get("portainer_method"), entry.get("has_caddy"),
                     entry.get("caddy_conf_path"), entry.get("last_probed"), cid),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO proxmox_config
                        (config_id, pve_host, pve_name, vmid, vm_type, name, status,
                         cores, memory_mb, rootfs, ip_config, ip_address, gateway,
                         mac_address, vlan_tag, tags, mountpoints_json, raw_conf,
                         vlans_json, has_docker, dockge_stacks_dir, has_portainer,
                         portainer_method, has_caddy, caddy_conf_path, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (cid, entry.get("pve_host"), entry.get("pve_name"), entry.get("vmid"),
                     entry.get("vm_type"), entry.get("name"), entry.get("status"),
                     entry.get("cores"), entry.get("memory_mb"), entry.get("rootfs"),
                     entry.get("ip_config"), entry.get("ip_address"), entry.get("gateway"),
                     entry.get("mac_address"), entry.get("vlan_tag"), entry.get("tags"),
                     entry.get("mountpoints_json"), entry.get("raw_conf"),
                     entry.get("vlans_json"), entry.get("has_docker"),
                     entry.get("dockge_stacks_dir"), entry.get("has_portainer"),
                     entry.get("portainer_method"), entry.get("has_caddy"),
                     entry.get("caddy_conf_path"), entry.get("last_probed")),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM proxmox_config WHERE config_id=?", (cid,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "proxmox_config", cid, dict(row), gen,
            )

        # ── Upsert proxmox_nets ────────────────────────────────────────────
        nets_created = nets_updated = 0
        for net in nets_raw:
            nid = net.get("net_id")
            if not nid:
                continue
            net_existing = conn.execute(
                "SELECT net_id FROM proxmox_nets WHERE net_id=?", (nid,)
            ).fetchone()
            if net_existing:
                conn.execute(
                    """
                    UPDATE proxmox_nets SET
                        config_id=?, pve_host=?, vmid=?, net_key=?,
                        mac_address=?, ip_address=?, ip_cidr=?, gateway=?,
                        vlan_tag=?, bridge=?, model=?, raw_str=?,
                        ip_source=coalesce(
                            CASE WHEN ip_source='pfsense' THEN 'pfsense' END,
                            ?
                        ),
                        updated_at=datetime('now')
                    WHERE net_id=?
                    """,
                    (net.get("config_id"), net.get("pve_host"), net.get("vmid"),
                     net.get("net_key"), net.get("mac_address"), net.get("ip_address"),
                     net.get("ip_cidr"), net.get("gateway"), net.get("vlan_tag"),
                     net.get("bridge"), net.get("model"), net.get("raw_str"),
                     net.get("ip_source", "conf"), nid),
                )
                nets_updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO proxmox_nets
                        (net_id, config_id, pve_host, vmid, net_key,
                         mac_address, ip_address, ip_cidr, gateway,
                         vlan_tag, bridge, model, raw_str, ip_source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (nid, net.get("config_id"), net.get("pve_host"), net.get("vmid"),
                     net.get("net_key"), net.get("mac_address"), net.get("ip_address"),
                     net.get("ip_cidr"), net.get("gateway"), net.get("vlan_tag"),
                     net.get("bridge"), net.get("model"), net.get("raw_str"),
                     net.get("ip_source", "conf")),
                )
                nets_created += 1
            net_row = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (nid,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if net_existing else "INSERT",
                "proxmox_nets", nid, dict(net_row), gen,
            )

    return {
        "created": created,
        "updated": updated,
        "total": created + updated,
        "nets_created": nets_created,
        "nets_updated": nets_updated,
        "pve_hosts_probed": stats_raw.get("pve_hosts_probed", 0),
        "conf_files_read": stats_raw.get("conf_files_read", 0),
    }


# ── Single-record CRUD ────────────────────────────────────────────────────────

@router.get("/{config_id}", response_model=ProxmoxConfigOut)
async def get_proxmox_config(config_id: str) -> ProxmoxConfigOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM proxmox_config WHERE config_id=?", (config_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"config_id '{config_id}' not found")
    return _row_to_out(row)


@router.put("/{config_id}", response_model=ProxmoxConfigOut)
async def update_proxmox_config(config_id: str, body: ProxmoxConfigUpdate) -> ProxmoxConfigOut:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [config_id]
    with get_conn() as conn:
        result = conn.execute(
            f"UPDATE proxmox_config SET {set_clause}, updated_at=datetime('now') WHERE config_id=?",
            values,
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"config_id '{config_id}' not found")
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM proxmox_config WHERE config_id=?", (config_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "proxmox_config", config_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{config_id}", status_code=204)
async def delete_proxmox_config(config_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM proxmox_config WHERE config_id=?", (config_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"config_id '{config_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "proxmox_config", config_id, {}, gen)
