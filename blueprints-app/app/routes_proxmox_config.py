"""routes_proxmox_config.py — CRUD + probe for /api/v1/proxmox-config"""

import asyncio
import ipaddress
import json
import os

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import ProxmoxConfigCreate, ProxmoxConfigOut, ProxmoxConfigUpdate
from .routes_proxmox_nets import fill_vlan_tags_from_cidrs
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
        dockge_json=row["dockge_json"],
        portainer_json=row["portainer_json"],
        caddy_json=row["caddy_json"],
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
                 portainer_method, has_caddy, caddy_conf_path,
                 dockge_json, portainer_json, caddy_json, last_probed)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (body.config_id, body.pve_host, body.pve_name, body.vmid, body.vm_type,
             body.name, body.status, body.cores, body.memory_mb, body.rootfs,
             body.ip_config, body.ip_address, body.gateway, body.mac_address,
             body.vlan_tag, body.tags, body.mountpoints_json, body.raw_conf,
             body.vlans_json, body.has_docker, body.dockge_stacks_dir,
             body.has_portainer, body.portainer_method, body.has_caddy,
             body.caddy_conf_path, body.dockge_json, body.portainer_json,
             body.caddy_json, body.last_probed),
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
                        dockge_json=?, portainer_json=?, caddy_json=?,
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
                     body.dockge_json, body.portainer_json, body.caddy_json,
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
                         portainer_method, has_caddy, caddy_conf_path,
                         dockge_json, portainer_json, caddy_json, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (body.config_id, body.pve_host, body.pve_name, body.vmid,
                     body.vm_type, body.name, body.status, body.cores,
                     body.memory_mb, body.rootfs, body.ip_config, body.ip_address,
                     body.gateway, body.mac_address, body.vlan_tag, body.tags,
                     body.mountpoints_json, body.raw_conf,
                     body.vlans_json, body.has_docker, body.dockge_stacks_dir,
                     body.has_portainer, body.portainer_method,
                     body.has_caddy, body.caddy_conf_path,
                     body.dockge_json, body.portainer_json, body.caddy_json,
                     body.last_probed),
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
    from .ssh import probe_status_for_host_type
    return probe_status_for_host_type("pve")


@router.post("/probe", response_model=dict)
async def probe_proxmox_config() -> dict:
    """
    Run bp-proxmox-config-probe.sh, parse ##ENTRIES## and ##NETS## from stdout,
    upsert all records in-process (no re-entrant HTTP call).
    """
    script = os.path.realpath(_PROBE_SCRIPT)
    if not os.path.isfile(script):
        raise HTTPException(500, f"Probe script not found: {script}")

    from .ssh import SshKeyMissing, probe_status_for_host_type, resolve_env_key
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
            # ── Clean up stale entries with a renamed config_id ──────────────
            # E.g. old probes stored pve_ip+"_"+vmid as config_id, but the
            # current format uses pve_name+"_"+vmid. Delete any stale row that
            # shares the same (pve_host, vmid, vm_type) but a different
            # config_id, plus its orphaned net rows.
            stale_rows = conn.execute(
                "SELECT config_id FROM proxmox_config"
                " WHERE pve_host=? AND vmid=? AND vm_type=? AND config_id!=?",
                (entry.get("pve_host"), entry.get("vmid"), entry.get("vm_type"), cid),
            ).fetchall()
            for (stale_cid,) in stale_rows:
                stale_nets = conn.execute(
                    "SELECT net_id FROM proxmox_nets WHERE config_id=?", (stale_cid,)
                ).fetchall()
                for (net_id,) in stale_nets:
                    conn.execute("DELETE FROM proxmox_nets WHERE net_id=?", (net_id,))
                    enqueue_for_all_peers(conn, "DELETE", "proxmox_nets", net_id, {}, gen)
                conn.execute("DELETE FROM proxmox_config WHERE config_id=?", (stale_cid,))
                enqueue_for_all_peers(conn, "DELETE", "proxmox_config", stale_cid, {}, gen)
            # ─────────────────────────────────────────────────────────────────
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
                        dockge_json=?, portainer_json=?, caddy_json=?,
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
                     entry.get("caddy_conf_path"),
                     entry.get("dockge_json"), entry.get("portainer_json"),
                     entry.get("caddy_json"), entry.get("last_probed"), cid),
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
                         portainer_method, has_caddy, caddy_conf_path,
                         dockge_json, portainer_json, caddy_json, last_probed)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
                     entry.get("caddy_conf_path"),
                     entry.get("dockge_json"), entry.get("portainer_json"),
                     entry.get("caddy_json"), entry.get("last_probed")),
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
                        mac_address=?,
                        ip_address = CASE WHEN ip_source != 'conf' THEN ip_address ELSE ? END,
                        ip_cidr    = CASE WHEN ip_source != 'conf' THEN ip_cidr    ELSE ? END,
                        gateway    = CASE WHEN ip_source != 'conf' THEN gateway    ELSE ? END,
                        vlan_tag=?, bridge=?, model=?, raw_str=?,
                        ip_source  = CASE WHEN ip_source != 'conf' THEN ip_source  ELSE ? END,
                        updated_at = datetime('now')
                    WHERE net_id=?
                    """,
                    (net.get("config_id"), net.get("pve_host"), net.get("vmid"),
                     net.get("net_key"), net.get("mac_address"),
                     net.get("ip_address"), net.get("ip_cidr"), net.get("gateway"),
                     net.get("vlan_tag"), net.get("bridge"), net.get("model"), net.get("raw_str"),
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

        # ── Auto-populate vlans from inferred CIDRs ──────────────────────────
        # For each unique vlan_tag+IP pair, infer /24 CIDR and INSERT OR IGNORE
        # so manually confirmed CIDRs are never overwritten.
        seen_vlans: set[int] = set()
        for net in nets_raw:
            vt = net.get("vlan_tag")
            ip = net.get("ip_address")
            if vt is None or not ip:
                continue
            if vt in seen_vlans:
                continue
            seen_vlans.add(vt)
            try:
                inferred = str(ipaddress.ip_network(f"{ip}/24", strict=False))
            except ValueError:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO vlans (vlan_id, cidr, cidr_inferred) VALUES (?,?,1)",
                (vt, inferred),
            )
            conn.execute(
                "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=? AND (cidr IS NULL OR cidr='')",
                (inferred, vt),
            )
            vlan_row = conn.execute(
                "SELECT * FROM vlans WHERE vlan_id=?", (vt,)
            ).fetchone()
            if vlan_row:
                enqueue_for_all_peers(conn, "UPDATE", "vlans", vt, dict(vlan_row), gen)

        # Infer vlan_tag for proxmox_nets rows with IP but no vlan_tag (e.g. untagged bridge)
        fill_vlan_tags_from_cidrs(conn, gen)

    return {
        "created": created,
        "updated": updated,
        "total": created + updated,
        "nets_created": nets_created,
        "nets_updated": nets_updated,
        "pve_hosts_probed": stats_raw.get("pve_hosts_probed", 0),
        "conf_files_read": stats_raw.get("conf_files_read", 0),
    }


# ── Probe VM services via SSH ─────────────────────────────────────────────────

_PROBE_SERVICES_CMD = r"""
has_docker=0; docker_usable=0
dockge_json='[]'; portainer_json='[]'; caddy_json='[]'

# append a JSON object to a compact JSON array (no jq needed)
json_append() {
  local arr="$1" obj="$2"
  [ "$arr" = "[]" ] && echo "[${obj}]" && return
  echo "${arr%]},${obj}]"
}

# ── Docker ────────────────────────────────────────────────────────────────────
if [ -S /var/run/docker.sock ]; then
  has_docker=1; docker info >/dev/null 2>&1 && docker_usable=1
elif command -v docker >/dev/null 2>&1; then
  has_docker=1; docker info >/dev/null 2>&1 && docker_usable=1
fi

# ── Container-based discovery ─────────────────────────────────────────────────
if [ "$docker_usable" = "1" ]; then
  containers=$(docker ps --no-trunc --format '{{.ID}}|{{.Names}}|{{.Image}}' 2>/dev/null || true)
  while IFS='|' read -r cid cname cimage; do
    [ -z "$cid" ] && continue

    # Dockge: find stacks host path from /opt/stacks mount, fallback /app/data
    if echo "$cimage" | grep -qiE 'louislam/dockge'; then
      stacks=$(docker inspect "$cid" --format \
        '{{range .Mounts}}{{if eq .Destination "/opt/stacks"}}{{.Source}}{{end}}{{end}}' \
        2>/dev/null | tr -d '[:space:]')
      [ -z "$stacks" ] && stacks=$(docker inspect "$cid" --format \
        '{{range .Mounts}}{{if eq .Destination "/app/data"}}{{.Source}}{{end}}{{end}}' \
        2>/dev/null | tr -d '[:space:]')
      if [ -n "$stacks" ]; then
        dockge_json=$(json_append "$dockge_json" \
          "{\"container\":\"${cname}\",\"stacks_dir\":\"${stacks}\"}")
      fi
    fi

    # Portainer: find data dir from /data mount
    if echo "$cimage" | grep -qiE 'portainer/portainer|portainer-ce|portainer-ee'; then
      data_dir=$(docker inspect "$cid" --format \
        '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' \
        2>/dev/null | tr -d '[:space:]')
      portainer_json=$(json_append "$portainer_json" \
        "{\"container\":\"${cname}\",\"data_dir\":\"${data_dir:-unknown}\",\"method\":\"docker\"}")
    fi

    # Caddy: detect container by image, find Caddyfile in mounts
    if echo "$cimage" | grep -qiE '(^|/)caddy:|lucaslorentz/caddy-docker-proxy'; then
      caddyfile=$(docker inspect "$cid" --format \
        '{{range .Mounts}}{{.Source}} {{end}}' 2>/dev/null | tr ' ' '\n' | \
        xargs -I{} find {} -maxdepth 2 -name "Caddyfile" 2>/dev/null | head -1)
      caddy_json=$(json_append "$caddy_json" \
        "{\"container\":\"${cname}\",\"method\":\"docker\",\"caddyfile\":\"${caddyfile:-}\"}")
    fi
  done <<< "$containers"
fi

# ── Host-level fallbacks ──────────────────────────────────────────────────────
if [ "$portainer_json" = "[]" ]; then
  systemctl is-active --quiet portainer 2>/dev/null \
    && portainer_json='[{"method":"service"}]'
fi

if [ "$caddy_json" = "[]" ]; then
  caddy_running=0
  command -v caddy  >/dev/null 2>&1 && caddy_running=1
  systemctl is-active --quiet caddy  2>/dev/null && caddy_running=1
  systemctl is-active --quiet caddy2 2>/dev/null && caddy_running=1
  if [ "$caddy_running" = "1" ]; then
    caddyfile=$(find /etc/caddy /root /opt /home -name "Caddyfile" -maxdepth 5 2>/dev/null | head -1)
    caddy_json="[{\"method\":\"host\",\"caddyfile\":\"${caddyfile:-}\"}]"
  fi
fi

printf 'PROBE_RESULT:{"has_docker":%s,"dockge_json":%s,"portainer_json":%s,"caddy_json":%s}\n' \
  "$has_docker" "$dockge_json" "$portainer_json" "$caddy_json"
"""


# Infrastructure details (VLAN source IPs, citadel identity) are kept in .env —
# nothing site-specific belongs in this public file.

def _load_vlan_source_map() -> dict[str, str]:
    """Parse VLAN_SOURCE_MAP env var: 'prefix:src_ip,prefix:src_ip,...'"""
    raw = os.environ.get("VLAN_SOURCE_MAP", "")
    result: dict[str, str] = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            prefix, src = entry.split(":", 1)
            result[prefix.strip()] = src.strip()
    return result


def _pick_source_ip(target_ip: str) -> str | None:
    """Return the local source interface IP on the same /24 as target_ip, or None."""
    prefix = ".".join(target_ip.split(".")[:3])
    return _load_vlan_source_map().get(prefix)


def _is_citadel(vmid, pve_host: str) -> bool:
    """True if vmid+pve_host matches the citadel identity defined in .env."""
    cid_vmid = os.environ.get("CITADEL_VMID", "")
    cid_pve  = os.environ.get("CITADEL_PVE_HOST", "")
    if not cid_vmid or not cid_pve:
        return False
    try:
        return int(vmid) == int(cid_vmid) and pve_host == cid_pve
    except (ValueError, TypeError):
        return False


@router.post("/probe-services", response_model=dict)
async def probe_vm_services() -> dict:
    """
    SSH (as root) into all running proxmox_config entries that have a known IP
    and detect docker, portainer, dockge, caddy.

    Key selection is fully deterministic: looks up each target IP in the
    ssh_targets table. If an IP is not in the table, it is skipped with a
    clear message. Run POST /api/v1/ssh-targets/rebuild first to populate
    the table from proxmox_config + pve_hosts + pfSense env vars.
    """
    # Load all ssh_targets into memory once for fast lookup
    with get_conn() as conn:
        _ssh_rows = conn.execute(
            "SELECT ip_address, key_env_var, source_ip FROM ssh_targets"
        ).fetchall()
    ssh_lookup: dict[str, dict] = {
        r["ip_address"]: {"key_env_var": r["key_env_var"], "source_ip": r["source_ip"]}
        for r in _ssh_rows
    }
    if not ssh_lookup:
        raise HTTPException(
            503,
            "ssh_targets table is empty — run POST /api/v1/ssh-targets/rebuild first"
        )

    with get_conn() as conn:
        targets = conn.execute(
            """
            SELECT c.config_id, c.name, c.vmid, c.pve_host,
                   COALESCE(NULLIF(c.ip_address,''), n.ip_address) AS ip
            FROM proxmox_config c
            LEFT JOIN (
                -- Prefer IPs that have a source_ip in ssh_targets (VLAN-matched),
                -- then fall back to any available IP.
                SELECT config_id, ip_address
                FROM (
                    SELECT pn.config_id, pn.ip_address,
                           ROW_NUMBER() OVER (
                               PARTITION BY pn.config_id
                               ORDER BY
                                   CASE WHEN st.source_ip IS NOT NULL THEN 0 ELSE 1 END,
                                   pn.ip_address
                           ) AS rn
                    FROM proxmox_nets pn
                    LEFT JOIN ssh_targets st ON st.ip_address = pn.ip_address
                    WHERE pn.ip_address IS NOT NULL AND pn.ip_address != ''
                )
                WHERE rn = 1
            ) n ON n.config_id = c.config_id
            WHERE c.status = 'running'
              AND COALESCE(NULLIF(c.ip_address,''), n.ip_address) IS NOT NULL
            """
        ).fetchall()

    if not targets:
        return {"checked": 0, "updated": 0, "skipped": 0,
                "message": "No running VMs with known IPs found"}

    sem = asyncio.Semaphore(10)
    results: list[dict] = []

    async def _probe(config_id: str, name: str, ip: str, vmid, pve_host: str) -> None:
        target = ssh_lookup.get(ip)
        if not target:
            results.append({"config_id": config_id, "name": name, "ip": ip,
                             "ok": False,
                             "error": f"no ssh_targets entry for {ip} — run /api/v1/ssh-targets/rebuild"})
            return
        key_env  = target["key_env_var"]
        key_path = os.environ.get(key_env, "")
        if not key_path or not os.path.isfile(key_path):
            results.append({"config_id": config_id, "name": name, "ip": ip,
                             "ok": False,
                             "error": f"key file missing for {key_env}"})
            return
        src = target["source_ip"]
        ssh_cmd = ["ssh", "-i", key_path,
                   "-o", "StrictHostKeyChecking=no",
                   "-o", "ConnectTimeout=8",
                   "-o", "BatchMode=yes",
                   "-o", "LogLevel=ERROR"]
        if src:
            ssh_cmd += ["-b", src]
        ssh_cmd += [f"root@{ip}", _PROBE_SERVICES_CMD.strip()]

        async with sem:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *ssh_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
            except Exception as exc:
                results.append({"config_id": config_id, "name": name, "ip": ip,
                                 "ok": False, "error": str(exc)})
                return

            out = stdout.decode(errors="replace")
            line = next(
                (probe_line for probe_line in out.splitlines() if probe_line.startswith("PROBE_RESULT:")),
                None,
            )
            if not line:
                results.append({"config_id": config_id, "name": name, "ip": ip,
                                 "ok": False, "error": "no probe result"})
                return

            try:
                payload = json.loads(line[len("PROBE_RESULT:"):])
            except Exception:
                results.append({"config_id": config_id, "name": name, "ip": ip,
                                 "ok": False, "error": "malformed probe result"})
                return
            results.append({
                "config_id":      config_id,
                "name":           name,
                "ip":             ip,
                "ok":             True,
                "has_docker":     1 if payload.get("has_docker") == 1 else 0,
                "dockge_json":    json.dumps(payload.get("dockge_json", [])),
                "portainer_json": json.dumps(payload.get("portainer_json", [])),
                "caddy_json":     json.dumps(payload.get("caddy_json", [])),
            })

    await asyncio.gather(*[
        _probe(t["config_id"], t["name"] or t["config_id"], t["ip"],
               t["vmid"], t["pve_host"])
        for t in targets
    ])

    updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "service-probe")
        for r in results:
            if not r["ok"]:
                continue
            conn.execute(
                """UPDATE proxmox_config SET
                       has_docker=?, dockge_json=?, portainer_json=?, caddy_json=?,
                       last_probed=datetime('now'), updated_at=datetime('now')
                   WHERE config_id=?""",
                (r["has_docker"], r["dockge_json"], r["portainer_json"],
                 r["caddy_json"], r["config_id"]),
            )
            row = conn.execute(
                "SELECT * FROM proxmox_config WHERE config_id=?", (r["config_id"],)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "proxmox_config",
                                  r["config_id"], dict(row), gen)
            updated += 1

    skipped = sum(1 for r in results if not r["ok"])
    checked = len(results)
    msg = (f"Checked {checked} VM{'s' if checked != 1 else ''} — "
           f"{updated} updated, {skipped} unreachable/failed")
    return {
        "checked": checked,
        "updated": updated,
        "skipped": skipped,
        "message": msg,
        "details": [
            {"name": r["name"], "ip": r["ip"],
             "status": "ok" if r["ok"] else r.get("error", "?"),
             "detected": (
                 (["docker"] if r.get("has_docker") else [])
                 + (["portainer"] if r.get("portainer_json", "[]") not in ("[]", None, "") else [])
                 + (["dockge"]    if r.get("dockge_json",    "[]") not in ("[]", None, "") else [])
                 + (["caddy"]     if r.get("caddy_json",     "[]") not in ("[]", None, "") else [])
             )}
            for r in results
        ],
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
