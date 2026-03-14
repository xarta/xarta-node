"""routes_ssh_targets.py — deterministic SSH key lookup table.

Each IP address in the fleet maps to exactly one SSH key (via env var) and one
source IP (for same-VLAN binding). All probe scripts look up this table instead
of guessing or hard-coding key selection logic.

Rules (from ssh-access skill):
  - Fleet LXCs (names in FLEET_LXC_NAMES env var)         → XARTA_NODE_SSH_KEY
  - Citadel    (CITADEL_VMID + CITADEL_PVE_HOST env)       → CITADEL_SSH_KEY
  - All other LXCs                                         → LXC_SSH_KEY
  - QEMU VMs in PROXMOX_NESTED_VM_NAMES env                → PROXMOX_SSH_KEY
  - All other QEMU VMs (excl. citadel, pbs*, pfsense*)     → VM_SSH_KEY
  - PVE hosts (pve_hosts table)                            → PROXMOX_SSH_KEY
  - pfSense (PFSENSE_SSH_TARGET / PFSENSE_CLOUSEAU_SSH_TARGET env) → PFSENSE_SSH_KEY
  - pbs* / rusty-backups → SKIP (no direct SSH)
"""

import os
import re

from fastapi import APIRouter, HTTPException

from . import config as cfg
from .db import get_conn, increment_gen
from .models import SshTargetCreate, SshTargetOut, SshTargetUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/ssh-targets", tags=["ssh-targets"])

# ── Helpers (env-driven, no secrets in source) ──────────────────────────────

def _fleet_lxc_names() -> set[str]:
    """Names of xarta-node fleet LXCs — derived from .nodes.json active nodes."""
    return {n.lower() for n in cfg.FLEET_LXC_NAMES}


def _nested_proxmox_names() -> set[str]:
    """QEMU VMs that are actually PVE hosts — from PROXMOX_NESTED_VM_NAMES env var."""
    raw = os.environ.get("PROXMOX_NESTED_VM_NAMES", "")
    return {n.strip().lower() for n in raw.split(",") if n.strip()}


def _vlan_order_case() -> str:
    """SQL CASE fragment for VLAN-preference ordering — derived from VLAN_SOURCE_MAP.
    Falls back to ordering by ip_address when the map is not configured."""
    vsmap = _vlan_source_map()
    if not vsmap:
        return "ip_address"
    parts = [f"WHEN ip_address LIKE '{p}.%' THEN {i}" for i, p in enumerate(vsmap)]
    return "CASE " + " ".join(parts) + f" ELSE {len(vsmap)} END"


# Names/patterns that have no direct SSH access — skip entirely
_SKIP_RE = re.compile(
    r'^(pbs\d*|pbs-\d*|rusty-backups|pfsense.*|pfsense-.*)$',
    re.IGNORECASE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _vlan_source_map() -> dict[str, str]:
    """Parse VLAN_SOURCE_MAP env var → {prefix: source_ip}."""
    raw = os.environ.get("VLAN_SOURCE_MAP", "")
    result: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            prefix, src = entry.split(":", 1)
            result[prefix.strip()] = src.strip()
    return result


def _source_ip(target_ip: str) -> str | None:
    """Return the local source interface IP on the same /24 as target_ip."""
    prefix = ".".join(target_ip.split(".")[:3])
    return _vlan_source_map().get(prefix)


def _classify(name: str, vm_type: str, vmid, pve_host: str) -> str | None:
    """
    Determine key_env_var for a proxmox_config entry.
    Returns env var name string, or None to skip (no SSH access).
    """
    nm = (name or "").lower().strip()

    # Skip — no direct SSH to these
    if _SKIP_RE.match(nm):
        return None

    # QEMU VMs that are actually nested PVE hosts
    if nm in _nested_proxmox_names():
        return "PROXMOX_SSH_KEY"

    # Citadel (env-driven)
    cid_vmid = os.environ.get("CITADEL_VMID", "")
    cid_pve  = os.environ.get("CITADEL_PVE_HOST", "")
    if cid_vmid and cid_pve:
        try:
            if int(vmid) == int(cid_vmid) and pve_host == cid_pve:
                return "CITADEL_SSH_KEY"
        except (ValueError, TypeError):
            pass

    if vm_type == "lxc":
        if nm in _fleet_lxc_names():
            return "XARTA_NODE_SSH_KEY"
        return "LXC_SSH_KEY"

    if vm_type == "qemu":
        return "VM_SSH_KEY"

    return None


def _host_type(name: str, vm_type: str, vmid, pve_host: str, key_env: str) -> str:
    nm = (name or "").lower().strip()
    if key_env == "CITADEL_SSH_KEY":
        return "citadel"
    if nm in _fleet_lxc_names():
        return "lxc-fleet"
    if nm in _nested_proxmox_names():
        return "pve"
    if key_env == "PROXMOX_SSH_KEY":
        return "pve"
    if key_env == "PFSENSE_SSH_KEY":
        return "pfsense"
    if vm_type == "lxc":
        return "lxc"
    if vm_type == "qemu":
        return "qemu"
    return vm_type or "unknown"


def _row_to_out(row) -> SshTargetOut:
    return SshTargetOut(
        ip_address=row["ip_address"],
        key_env_var=row["key_env_var"],
        source_ip=row["source_ip"],
        host_name=row["host_name"],
        host_type=row["host_type"],
        notes=row["notes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── CRUD ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[SshTargetOut])
async def list_ssh_targets() -> list[SshTargetOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM ssh_targets ORDER BY host_type, ip_address"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.get("/{ip_address:path}", response_model=SshTargetOut)
async def get_ssh_target(ip_address: str) -> SshTargetOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM ssh_targets WHERE ip_address=?", (ip_address,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"No ssh_target for IP '{ip_address}'")
    return _row_to_out(row)


@router.post("", response_model=SshTargetOut, status_code=201)
async def create_ssh_target(body: SshTargetCreate) -> SshTargetOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT ip_address FROM ssh_targets WHERE ip_address=?", (body.ip_address,)
        ).fetchone():
            raise HTTPException(409, f"ssh_target for '{body.ip_address}' already exists")
        gen = increment_gen(conn, "human")
        conn.execute(
            """INSERT INTO ssh_targets
               (ip_address, key_env_var, source_ip, host_name, host_type, notes)
               VALUES (?,?,?,?,?,?)""",
            (body.ip_address, body.key_env_var, body.source_ip,
             body.host_name, body.host_type, body.notes),
        )
        row = conn.execute(
            "SELECT * FROM ssh_targets WHERE ip_address=?", (body.ip_address,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "ssh_targets", body.ip_address, dict(row), gen)
    return _row_to_out(row)


@router.put("/{ip_address:path}", response_model=SshTargetOut)
async def update_ssh_target(ip_address: str, body: SshTargetUpdate) -> SshTargetOut:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [ip_address]
    with get_conn() as conn:
        result = conn.execute(
            f"UPDATE ssh_targets SET {set_clause}, updated_at=datetime('now') WHERE ip_address=?",
            values,
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"No ssh_target for IP '{ip_address}'")
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM ssh_targets WHERE ip_address=?", (ip_address,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "ssh_targets", ip_address, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{ip_address:path}", status_code=204)
async def delete_ssh_target(ip_address: str) -> None:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT ip_address FROM ssh_targets WHERE ip_address=?", (ip_address,)
        ).fetchone():
            raise HTTPException(404, f"No ssh_target for IP '{ip_address}'")
        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM ssh_targets WHERE ip_address=?", (ip_address,))
        enqueue_for_all_peers(conn, "DELETE", "ssh_targets", ip_address, {}, gen)


# ── Rebuild ───────────────────────────────────────────────────────────────────

@router.post("/rebuild", response_model=dict)
async def rebuild_ssh_targets() -> dict:
    """
    Clear and repopulate ssh_targets from:
      1. proxmox_config — all running entries with known IPs
      2. pve_hosts table
      3. pfSense env vars (PFSENSE_SSH_TARGET, PFSENSE_CLOUSEAU_SSH_TARGET)

    Key selection is deterministic — no guessing.
    Run this after any proxmox config probe that may have added new hosts.
    """
    vsmap = _vlan_source_map()

    def _src(ip: str) -> str | None:
        prefix = ".".join(ip.split(".")[:3])
        return vsmap.get(prefix)

    added = 0
    skipped = 0
    rows_to_write: list[dict] = []

    with get_conn() as conn:
        # ── 1. proxmox_config entries ──────────────────────────────────────
        configs = conn.execute(
            f"""
            SELECT c.name, c.vmid, c.vm_type, c.pve_host,
                   COALESCE(NULLIF(c.ip_address,''), n.ip_address) AS ip
            FROM proxmox_config c
            LEFT JOIN (
                SELECT config_id, ip_address
                FROM (
                    SELECT config_id, ip_address,
                           ROW_NUMBER() OVER (
                               PARTITION BY config_id
                               ORDER BY {_vlan_order_case()}, ip_address
                           ) AS rn
                    FROM proxmox_nets
                    WHERE ip_address IS NOT NULL AND ip_address != ''
                )
                WHERE rn = 1
            ) n ON n.config_id = c.config_id
            WHERE c.status = 'running'
              AND COALESCE(NULLIF(c.ip_address,''), n.ip_address) IS NOT NULL
            """
        ).fetchall()

        for row in configs:
            ip       = row["ip"]
            name     = row["name"] or ""
            vm_type  = row["vm_type"] or ""
            vmid     = row["vmid"]
            pve_host = row["pve_host"] or ""

            key_env = _classify(name, vm_type, vmid, pve_host)
            if key_env is None:
                skipped += 1
                continue

            rows_to_write.append({
                "ip_address":  ip,
                "key_env_var": key_env,
                "source_ip":   _src(ip),
                "host_name":   name,
                "host_type":   _host_type(name, vm_type, vmid, pve_host, key_env),
                "notes":       f"vmid={vmid} pve={pve_host} type={vm_type}",
            })

        # ── 2. PVE hosts ──────────────────────────────────────────────────
        pve_rows = conn.execute(
            "SELECT ip_address, hostname, pve_name FROM pve_hosts WHERE ip_address IS NOT NULL"
        ).fetchall()
        for prow in pve_rows:
            ip = prow["ip_address"]
            label = prow["pve_name"] or prow["hostname"] or ip
            rows_to_write.append({
                "ip_address":  ip,
                "key_env_var": "PROXMOX_SSH_KEY",
                "source_ip":   _src(ip),
                "host_name":   label,
                "host_type":   "pve",
                "notes":       "pve_hosts table",
            })

        # ── 3. pfSense from env vars ──────────────────────────────────────
        for env_var in ("PFSENSE_SSH_TARGET", "PFSENSE_CLOUSEAU_SSH_TARGET"):
            val = os.environ.get(env_var, "").strip()
            if not val:
                continue
            # Format: user@ip or ip
            ip = val.split("@")[-1].strip()
            if ip:
                rows_to_write.append({
                    "ip_address":  ip,
                    "key_env_var": "PFSENSE_SSH_KEY",
                    "source_ip":   _src(ip),
                    "host_name":   f"pfsense ({env_var})",
                    "host_type":   "pfsense",
                    "notes":       f"from env {env_var}",
                })

        # ── Write: clear + insert all ──────────────────────────────────────
        gen = increment_gen(conn, "ssh-targets-rebuild")
        conn.execute("DELETE FROM ssh_targets")

        seen: set[str] = set()
        for r in rows_to_write:
            ip = r["ip_address"]
            if ip in seen:
                continue
            seen.add(ip)
            conn.execute(
                """INSERT OR REPLACE INTO ssh_targets
                   (ip_address, key_env_var, source_ip, host_name, host_type, notes,
                    updated_at)
                   VALUES (?,?,?,?,?,?,datetime('now'))""",
                (ip, r["key_env_var"], r["source_ip"],
                 r["host_name"], r["host_type"], r["notes"]),
            )
            row = conn.execute(
                "SELECT * FROM ssh_targets WHERE ip_address=?", (ip,)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "ssh_targets", ip, dict(row), gen)
            added += 1

    return {
        "added":   added,
        "skipped": skipped,
        "message": f"Rebuilt ssh_targets: {added} entries added, {skipped} skipped (no SSH access)",
    }
