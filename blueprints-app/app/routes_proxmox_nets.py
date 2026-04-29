"""routes_proxmox_nets.py — CRUD + pfSense enrichment for /api/v1/proxmox-nets

One row per network interface per VM/LXC.  Populated by the proxmox-config
probe script alongside the parent proxmox_config rows.

The enrich-from-pfsense endpoint does a MAC-address JOIN against pfsense_dns
to fill in ip_address for interfaces where the conf file had no static IP
(common for DHCP-assigned containers and for QEMU VMs that use cloud-init).
"""

import ipaddress
import os

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import ProxmoxNetCreate, ProxmoxNetOut, ProxmoxNetUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/proxmox-nets", tags=["proxmox-nets"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _int(val) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _row_to_out(row) -> ProxmoxNetOut:
    return ProxmoxNetOut(
        net_id=row["net_id"],
        config_id=row["config_id"],
        pve_host=row["pve_host"],
        vmid=_int(row["vmid"]),
        net_key=row["net_key"],
        mac_address=row["mac_address"],
        ip_address=row["ip_address"],
        ip_cidr=row["ip_cidr"],
        gateway=row["gateway"],
        vlan_tag=_int(row["vlan_tag"]),
        bridge=row["bridge"],
        model=row["model"],
        raw_str=row["raw_str"],
        ip_source=row["ip_source"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Shared helper ────────────────────────────────────────────────────────────

def fill_vlan_tags_from_cidrs(conn, gen: int) -> int:
    """
    For every proxmox_nets row that has an IP but no vlan_tag, look up the
    matching VLAN by checking the IP against all known CIDRs in the vlans table.
    Updates vlan_tag in-place and enqueues changes to peers.
    Returns the number of rows updated.
    """
    vlan_cidrs = [
        (r["vlan_id"], ipaddress.ip_network(r["cidr"], strict=False))
        for r in conn.execute(
            "SELECT vlan_id, cidr FROM vlans WHERE cidr IS NOT NULL AND cidr != ''"
        ).fetchall()
    ]
    if not vlan_cidrs:
        return 0

    rows = conn.execute(
        """
        SELECT net_id, ip_address FROM proxmox_nets
        WHERE (vlan_tag IS NULL OR vlan_tag = 0)
          AND ip_address IS NOT NULL AND ip_address != '' AND ip_address != 'dhcp'
        """
    ).fetchall()

    updated = 0
    for row in rows:
        try:
            ip_obj = ipaddress.ip_address(row["ip_address"])
        except ValueError:
            continue
        for vlan_id, network in vlan_cidrs:
            if ip_obj in network:
                conn.execute(
                    "UPDATE proxmox_nets SET vlan_tag=?, updated_at=datetime('now') WHERE net_id=?",
                    (vlan_id, row["net_id"]),
                )
                updated_row = conn.execute(
                    "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
                ).fetchone()
                enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated_row), gen)
                updated += 1
                break
    return updated


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[ProxmoxNetOut])
async def list_proxmox_nets(config_id: str | None = None) -> list[ProxmoxNetOut]:
    """Return all network interfaces, optionally filtered by config_id."""
    with get_conn() as conn:
        if config_id:
            rows = conn.execute(
                "SELECT * FROM proxmox_nets WHERE config_id=? ORDER BY config_id, net_key",
                (config_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM proxmox_nets ORDER BY config_id, net_key"
            ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_proxmox_nets(entries: list[ProxmoxNetCreate]) -> dict:
    """Upsert many proxmox_nets rows — called by the probe endpoint."""
    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "proxmox-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT net_id FROM proxmox_nets WHERE net_id=?", (body.net_id,)
            ).fetchone()
            if existing:
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
                    (body.config_id, body.pve_host, body.vmid, body.net_key,
                     body.mac_address, body.ip_address, body.ip_cidr, body.gateway,
                     body.vlan_tag, body.bridge, body.model, body.raw_str,
                     body.ip_source or "conf",
                     body.net_id),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO proxmox_nets
                        (net_id, config_id, pve_host, vmid, net_key,
                         mac_address, ip_address, ip_cidr, gateway,
                         vlan_tag, bridge, model, raw_str, ip_source)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (body.net_id, body.config_id, body.pve_host, body.vmid,
                     body.net_key, body.mac_address, body.ip_address, body.ip_cidr,
                     body.gateway, body.vlan_tag, body.bridge, body.model,
                     body.raw_str, body.ip_source or "conf"),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (body.net_id,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "proxmox_nets", body.net_id, dict(row), gen,
            )
    return {"created": created, "updated": updated, "total": created + updated}


@router.post("/enrich-from-pfsense", response_model=dict)
async def enrich_nets_from_pfsense() -> dict:
    """
    For each proxmox_nets row that has a mac_address but no ip_address,
    look up ip_address in pfsense_dns by MAC and fill it in (ip_source='pfsense').

    Assumes 1:1 MAC → IPv4 (as is typical in a flat managed network).
    Returns counts of rows enriched.
    """
    enriched = 0
    with get_conn() as conn:
        # Find rows missing an IP but having a MAC
        missing = conn.execute(
            """
            SELECT n.net_id, n.mac_address
            FROM proxmox_nets n
            WHERE n.mac_address IS NOT NULL
              AND (n.ip_address IS NULL OR n.ip_address = '')
            """
        ).fetchall()

        if not missing:
            return {"enriched": 0, "checked": 0}

        gen = increment_gen(conn, "pfsense-enrich")
        checked = len(missing)

        for row in missing:
            net_id = row["net_id"]
            mac    = (row["mac_address"] or "").upper()
            if not mac:
                continue

            # pfsense_dns.mac_address is stored upper-case with colons
            dns_row = conn.execute(
                """
                SELECT ip_address FROM pfsense_dns
                WHERE upper(mac_address) = ?
                  AND ip_address IS NOT NULL AND ip_address != ''
                ORDER BY last_seen DESC
                LIMIT 1
                """,
                (mac,),
            ).fetchone()

            if not dns_row:
                continue

            ip = dns_row["ip_address"]
            conn.execute(
                """
                UPDATE proxmox_nets
                SET ip_address=?, ip_source='pfsense', updated_at=datetime('now')
                WHERE net_id=?
                """,
                (ip, net_id),
            )
            updated_row = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (net_id,)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", net_id, dict(updated_row), gen)
            enriched += 1

        # Infer vlan_tag from known CIDR map for any rows still missing it
        fill_vlan_tags_from_cidrs(conn, gen)

    return {"enriched": enriched, "checked": checked}


@router.post("/enrich-from-pfsense-arp", response_model=dict)
async def enrich_nets_from_pfsense_arp() -> dict:
    """
    SSH to pfSense, read the full ARP table (arp -an), and match MAC addresses
    against proxmox_nets rows that are still missing an IP address.

    This catches hosts on VLANs that neither this node nor the pve hosts have
    L3 interfaces on (e.g. management/isolated VLANs).  Also seeds the vlans
    table: when a match is found, the IP+vlan_tag of the proxmox_nets row is
    used to infer the CIDR.

    Uses PFSENSE_SSH_TARGET (user@host) and PFSENSE_SSH_KEY from the environment.
    """
    import asyncio
    import ipaddress
    import re

    from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args
    ssh_target = os.environ.get("PFSENSE_SSH_TARGET", "")
    if not ssh_target:
        raise HTTPException(503, "PFSENSE_SSH_TARGET is not set in .env")

    # Parse user@host
    if "@" in ssh_target:
        ssh_user, ssh_host = ssh_target.split("@", 1)
    else:
        ssh_user, ssh_host = "root", ssh_target

    try:
        _pfsense_ssh_args = make_ssh_args(ssh_host, connect_timeout=10)
    except (SshTargetNotFound, SshKeyMissing) as exc:
        raise HTTPException(503, str(exc))

    # ── Fetch full ARP table from pfSense ─────────────────────────────────────
    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh", *_pfsense_ssh_args,
            f"{ssh_user}@{ssh_host}",
            "arp -an",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
    except Exception as exc:
        raise HTTPException(502, f"SSH to pfSense failed: {exc}")

    # BSD arp -an format: ? (ip) at mac on iface [flags]
    pfsense_mac_to_ip: dict[str, str] = {}
    for line in stdout.decode(errors="replace").splitlines():
        m = re.search(r"\((\d+\.\d+\.\d+\.\d+)\)\s+at\s+([0-9a-f:]{17})", line, re.I)
        if m:
            pfsense_mac_to_ip[m.group(2).upper()] = m.group(1)

    if not pfsense_mac_to_ip:
        return {"enriched": 0, "checked": 0, "message": "pfSense ARP table empty or SSH failed"}

    # ── Match against missing proxmox_nets rows ────────────────────────────────
    with get_conn() as conn:
        missing = conn.execute(
            """
            SELECT net_id, vlan_tag, mac_address
            FROM proxmox_nets
            WHERE mac_address IS NOT NULL
              AND (ip_address IS NULL OR ip_address = '')
            """
        ).fetchall()
        if not missing:
            return {"enriched": 0, "checked": 0, "message": "No proxmox_nets rows with missing IPs"}

        gen      = increment_gen(conn, "pfsense-arp")
        enriched = 0

        for row in missing:
            mac = (row["mac_address"] or "").upper()
            ip  = pfsense_mac_to_ip.get(mac)
            if not ip:
                continue

            vt     = row["vlan_tag"]
            prefix = 24
            if vt is not None:
                # Infer CIDR from the found IP and seed vlans table
                try:
                    net    = ipaddress.ip_network(f"{ip}/24", strict=False)
                    cidr   = str(net)
                    prefix = net.prefixlen
                    existing_vlan = conn.execute(
                        "SELECT cidr FROM vlans WHERE vlan_id=?", (vt,)
                    ).fetchone()
                    if not existing_vlan:
                        conn.execute(
                            "INSERT OR IGNORE INTO vlans (vlan_id, cidr, cidr_inferred) VALUES (?,?,1)",
                            (vt, cidr),
                        )
                    elif not existing_vlan["cidr"]:
                        conn.execute(
                            "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=?",
                            (cidr, vt),
                        )
                    vlan_row = conn.execute(
                        "SELECT * FROM vlans WHERE vlan_id=?", (vt,)
                    ).fetchone()
                    if vlan_row:
                        enqueue_for_all_peers(conn, "UPDATE", "vlans", vt, dict(vlan_row), gen)
                except ValueError:
                    pass

            ip_cidr = f"{ip}/{prefix}"
            conn.execute(
                """
                UPDATE proxmox_nets SET
                    ip_address=?, ip_cidr=?, ip_source='pfsense-arp',
                    updated_at=datetime('now')
                WHERE net_id=?
                """,
                (ip, ip_cidr, row["net_id"]),
            )
            updated_row = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated_row), gen)
            enriched += 1

        # Infer vlan_tag from known CIDR map for any rows still missing it
        fill_vlan_tags_from_cidrs(conn, gen)

    return {
        "enriched": enriched,
        "checked":  len(missing),
        "arp_entries": len(pfsense_mac_to_ip),
        "message":  f"Read {len(pfsense_mac_to_ip)} ARP entries from pfSense, enriched {enriched} row(s)",
    }


@router.post("/find-ips-via-pve", response_model=dict)
async def find_ips_via_pve() -> dict:
    """
    SSH to each pve_host, discover VLAN CIDRs from its network interfaces,
    seed the vlans table with any newly found CIDRs, then arping-sweep for
    each proxmox_nets row that is still missing an IP.

    Installs iputils-arping on the pve host if not already present.
    All pve hosts are processed concurrently.
    """
    import asyncio
    import ipaddress
    import json as _json

    from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args

    with get_conn() as conn:
        pve_hosts = [
            dict(r) for r in conn.execute(
                "SELECT pve_id, ip_address FROM pve_hosts WHERE ssh_reachable=1"
            ).fetchall()
        ]
        missing_rows = conn.execute(
            """
            SELECT net_id, pve_host, vlan_tag, mac_address
            FROM proxmox_nets
            WHERE (ip_address IS NULL OR ip_address = '')
              AND mac_address IS NOT NULL
            """
        ).fetchall()
        known_cidrs = {
            row["vlan_id"]: row["cidr"]
            for row in conn.execute(
                "SELECT vlan_id, cidr FROM vlans WHERE cidr IS NOT NULL AND cidr != ''"
            ).fetchall()
        }

    if not pve_hosts:
        return {"found": 0, "message": "No ssh_reachable pve hosts"}
    if not missing_rows:
        return {"found": 0, "message": "No proxmox_nets rows with missing IPs"}

    # Group missing rows by pve_host
    by_host: dict[str, list] = {}
    for row in missing_rows:
        by_host.setdefault(row["pve_host"], []).append(dict(row))

    # Remote script: discovers CIDRs via ip-addr, arping-sweeps for target MACs
    REMOTE_SCRIPT = r"""
import subprocess, json, sys, ipaddress, re, shutil

payload   = json.loads(sys.argv[1])
targets   = payload["targets"]   # [{net_id, vlan_tag, mac_address}, ...]
known     = {int(k): v for k, v in payload["known_cidrs"].items()}

# ── Ensure arping available ───────────────────────────────────────────────
if not shutil.which("arping"):
    subprocess.run(["apt-get", "install", "-y", "iputils-arping"],
                   capture_output=True)

# ── Discover VLAN CIDRs from host interfaces ─────────────────────────────
raw = subprocess.check_output(["ip", "-o", "-4", "addr", "show"],
                               stderr=subprocess.DEVNULL).decode()
vlan_to_cidr: dict[int, str] = dict(known)
for line in raw.splitlines():
    # e.g. "5: vmbr0.43    inet 10.0.43.1/24 ..."
    m = re.search(r'\S+\.(\d+)\s+inet\s+(\S+)', line)
    if m:
        vid  = int(m.group(1))
        cidr = str(ipaddress.ip_network(m.group(2), strict=False))
        if vid not in vlan_to_cidr:
            vlan_to_cidr[vid] = cidr

# ── arping each IP in each needed CIDR looking for target MACs ───────────
mac_to_ip: dict[str, str] = {}
need_vlans = {t["vlan_tag"] for t in targets if t["vlan_tag"] is not None}
for vt in need_vlans:
    cidr = vlan_to_cidr.get(vt)
    if not cidr:
        continue
    net = ipaddress.ip_network(cidr, strict=False)
    for host_ip in net.hosts():
        res = subprocess.run(
            ["arping", "-c", "1", "-w", "0.3", str(host_ip)],
            capture_output=True, text=True
        )
        m = re.search(r'\[([0-9A-Fa-f:]{17})\]', res.stdout)
        if m:
            mac_to_ip[m.group(1).upper()] = str(host_ip)

result = {
    "mac_to_ip":    mac_to_ip,
    "vlan_to_cidr": {str(k): v for k, v in vlan_to_cidr.items()},
}
print("##ARPRESULT##" + json.dumps(result))
"""

    async def probe_host(host_ip: str, rows: list) -> dict:
        payload = _json.dumps({
            "targets": rows,
            "known_cidrs": {str(k): v for k, v in known_cidrs.items()},
        })
        # Escape single-quotes in payload for shell
        payload_esc = payload.replace("'", "'\"'\"'")
        cmd = (
            f"python3 -c '{REMOTE_SCRIPT}' '{payload_esc}'"
        )
        try:
            _pve_ssh_args = make_ssh_args(host_ip, connect_timeout=10)
        except (SshTargetNotFound, SshKeyMissing):
            return {}
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh", *_pve_ssh_args,
                f"root@{host_ip}",
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=300)
            text = stdout.decode(errors="replace")
            for line in text.splitlines():
                if line.startswith("##ARPRESULT##"):
                    return _json.loads(line[len("##ARPRESULT##"):])
        except Exception:
            pass
        return {}

    # Run all pve hosts concurrently
    host_ips  = [h["ip_address"] for h in pve_hosts if h["ip_address"] in by_host]
    coros     = [probe_host(ip, by_host[ip]) for ip in host_ips]
    responses = await asyncio.gather(*coros)
    host_results = dict(zip(host_ips, responses))

    # Merge all vlan_to_cidr discoveries and mac_to_ip results
    found       = 0
    vlans_added : set[int] = set()
    vlans_hit   : set[int] = set()

    with get_conn() as conn:
        gen = increment_gen(conn, "pve-arp-scan")

        for host_ip, res in host_results.items():
            new_cidrs  = res.get("vlan_to_cidr", {})
            mac_to_ip  = res.get("mac_to_ip", {})

            # Seed any newly discovered VLAN CIDRs
            for vid_str, cidr in new_cidrs.items():
                try:
                    vid = int(vid_str)
                    ipaddress.ip_network(cidr, strict=False)  # validate
                except ValueError:
                    continue
                existing = conn.execute(
                    "SELECT cidr FROM vlans WHERE vlan_id=?", (vid,)
                ).fetchone()
                conn.execute(
                    "INSERT OR IGNORE INTO vlans (vlan_id, cidr, cidr_inferred) VALUES (?,?,1)",
                    (vid, cidr),
                )
                conn.execute(
                    "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=? AND (cidr IS NULL OR cidr='')",
                    (cidr, vid),
                )
                vlan_row = conn.execute(
                    "SELECT * FROM vlans WHERE vlan_id=?", (vid,)
                ).fetchone()
                if vlan_row:
                    enqueue_for_all_peers(conn, "UPDATE", "vlans", vid, dict(vlan_row), gen)
                if not existing:
                    vlans_added.add(vid)

            # Match MACs and update proxmox_nets
            for row in by_host.get(host_ip, []):
                mac = (row["mac_address"] or "").upper()
                ip  = mac_to_ip.get(mac)
                if not ip:
                    continue
                vt     = row["vlan_tag"]
                prefix = 24
                all_cidrs = {**known_cidrs, **{int(k): v for k, v in new_cidrs.items()}}
                if vt is not None and vt in all_cidrs:
                    try:
                        prefix = ipaddress.ip_network(all_cidrs[vt], strict=False).prefixlen
                    except ValueError:
                        pass
                ip_cidr = f"{ip}/{prefix}"
                conn.execute(
                    """
                    UPDATE proxmox_nets SET
                        ip_address=?, ip_cidr=?, ip_source='pve-arp',
                        updated_at=datetime('now')
                    WHERE net_id=?
                    """,
                    (ip, ip_cidr, row["net_id"]),
                )
                updated = conn.execute(
                    "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
                ).fetchone()
                enqueue_for_all_peers(
                    conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated), gen
                )
                found += 1
                if vt is not None:
                    vlans_hit.add(vt)

    return {
        "found":       found,
        "vlans_added": sorted(vlans_added),
        "vlans_hit":   sorted(vlans_hit),
        "message":     f"Scanned {len(host_ips)} PVE host(s), matched {found} IP(s), added {len(vlans_added)} new VLAN CIDR(s)",
    }


@router.post("/find-ips-by-arp", response_model=dict)
async def find_ips_by_arp() -> dict:
    """
    For every IP in every known VLAN CIDR, send a live ARP request via
    'arping -c 1 -w 1 <ip>' (Layer 2 probe — not a cache read).  Collect
    the MAC from each reply and match against proxmox_nets rows missing an IP.

    arping (iputils-arping) is auto-installed if not present.
    All probes run concurrently.
    """
    import asyncio
    import ipaddress
    import re
    import shutil
    import subprocess

    # ── Ensure arping is present ──────────────────────────────────────────────
    if not shutil.which("arping"):
        subprocess.run(
            ["apt-get", "install", "-y", "iputils-arping"],
            capture_output=True,
        )
    if not shutil.which("arping"):
        return {"scanned": 0, "found": 0, "message": "arping not available and could not be installed"}

    # ── Load missing rows and known VLAN CIDRs from DB ───────────────────────
    with get_conn() as conn:
        missing = conn.execute(
            """
            SELECT net_id, vlan_tag, mac_address
            FROM proxmox_nets
            WHERE (ip_address IS NULL OR ip_address = '')
              AND mac_address IS NOT NULL
            """
        ).fetchall()
        if not missing:
            return {"scanned": 0, "found": 0, "message": "No proxmox_nets rows have missing IPs"}

        vlan_cidrs: dict[int, str] = {
            row["vlan_id"]: row["cidr"]
            for row in conn.execute(
                "SELECT vlan_id, cidr FROM vlans WHERE cidr IS NOT NULL AND cidr != ''"
            ).fetchall()
        }

    if not vlan_cidrs:
        return {"scanned": 0, "found": 0, "message": "No VLANs with known CIDRs configured"}

    # Build index of MAC → missing row for fast lookup
    mac_to_row: dict[str, dict] = {
        (row["mac_address"] or "").upper(): row for row in missing
    }

    # ── arping every IP in every VLAN CIDR concurrently ──────────────────────
    async def arping_ip(ip: str) -> tuple[str, str | None]:
        """Returns (ip, mac_upper) or (ip, None) if no reply."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "arping", "-c", "1", "-w", "0.3", ip,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=1)
            text = stdout.decode(errors="replace")
            # arping output: "Unicast reply from 10.0.0.1 [AA:BB:CC:DD:EE:FF]  0.787ms"
            m = re.search(r"\[([0-9A-Fa-f:]{17})\]", text)
            if m:
                return ip, m.group(1).upper()
        except Exception:
            pass
        return ip, None

    all_ips: list[str] = []
    for cidr in vlan_cidrs.values():
        try:
            net = ipaddress.ip_network(cidr, strict=False)
            all_ips.extend(str(h) for h in net.hosts())
        except ValueError:
            pass

    results = await asyncio.gather(*[arping_ip(ip) for ip in all_ips])

    # ── Match replies against missing rows and update DB ─────────────────────
    found = 0
    vlans_hit: set[int] = set()

    with get_conn() as conn:
        gen = increment_gen(conn, "arp-scan")
        for ip, mac in results:
            if mac is None:
                continue
            row = mac_to_row.get(mac)
            if row is None:
                continue
            # Derive prefix from VLAN CIDR
            vt = row["vlan_tag"]
            prefix = 24
            if vt is not None and vt in vlan_cidrs:
                try:
                    prefix = ipaddress.ip_network(vlan_cidrs[vt], strict=False).prefixlen
                except ValueError:
                    pass
            ip_cidr = f"{ip}/{prefix}"
            conn.execute(
                """
                UPDATE proxmox_nets SET
                    ip_address=?, ip_cidr=?, ip_source='arp-scan',
                    updated_at=datetime('now')
                WHERE net_id=?
                """,
                (ip, ip_cidr, row["net_id"]),
            )
            updated = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated), gen)
            found += 1
            if vt is not None:
                vlans_hit.add(vt)

    return {
        "scanned": len(all_ips),
        "found": found,
        "vlans_hit": sorted(vlans_hit),
        "message": f"arping'd {len(all_ips)} IPs across {len(vlan_cidrs)} VLANs, matched {found} MAC(s)",
    }


@router.post("/find-ips-via-qemu-agent", response_model=dict)
async def find_ips_via_qemu_agent() -> dict:
    """
    For each running VM with a missing IP, SSH to its PVE host and call
    'qm agent <vmid> network-get-interfaces' to retrieve IPs directly from
    the QEMU guest agent.  Matches results by MAC address.
    """
    import asyncio
    import json as _json

    from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args

    with get_conn() as conn:
        missing = conn.execute(
            """
            SELECT pn.net_id, pn.pve_host, pn.vmid, pn.net_key, pn.mac_address, pn.vlan_tag
            FROM proxmox_nets pn
            JOIN proxmox_config pc ON pc.config_id = pn.config_id
            WHERE (pn.ip_address IS NULL OR pn.ip_address = '')
              AND pn.mac_address IS NOT NULL
              AND pc.status = 'running'
            """
        ).fetchall()

    if not missing:
        return {"found": 0, "checked": 0, "message": "No running VMs with missing IPs"}

    # Group by pve_host → vmid → [rows]
    by_host: dict[str, dict[int, list]] = {}
    for row in missing:
        by_host.setdefault(row["pve_host"], {}).setdefault(row["vmid"], []).append(dict(row))

    async def query_agent(pve_ip: str, vmid: int) -> dict:
        """Returns {mac_upper: ip} from the QEMU guest agent."""
        try:
            _pve_ssh_args = make_ssh_args(pve_ip, connect_timeout=10)
        except (SshTargetNotFound, SshKeyMissing):
            return {}
        try:
            proc = await asyncio.create_subprocess_exec(
                "ssh", *_pve_ssh_args,
                f"root@{pve_ip}",
                f"qm agent {vmid} network-get-interfaces 2>/dev/null",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
            data = _json.loads(stdout.decode(errors="replace"))
            ifaces = data.get("result", data) if isinstance(data, dict) else data
            mac_to_ip: dict[str, str] = {}
            for iface in ifaces:
                mac = (iface.get("hardware-address") or "").upper()
                if not mac or mac == "00:00:00:00:00:00":
                    continue
                for addr in iface.get("ip-addresses", []):
                    if addr.get("ip-address-type") == "ipv4":
                        ip = addr.get("ip-address", "")
                        if ip and not ip.startswith("127.") and not ip.startswith("169.254."):
                            mac_to_ip[mac] = ip
                            break
            return mac_to_ip
        except Exception:
            return {}

    tasks = []
    task_keys = []
    for pve_ip, vmid_map in by_host.items():
        for vmid in vmid_map:
            tasks.append(query_agent(pve_ip, vmid))
            task_keys.append((pve_ip, vmid))

    results = await asyncio.gather(*tasks)

    found = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "qemu-agent")
        for (pve_ip, vmid), mac_to_ip in zip(task_keys, results):
            for row in by_host[pve_ip][vmid]:
                mac = (row["mac_address"] or "").upper()
                ip  = mac_to_ip.get(mac)
                if not ip:
                    continue
                conn.execute(
                    """
                    UPDATE proxmox_nets SET
                        ip_address=?, ip_source='qemu-agent',
                        updated_at=datetime('now')
                    WHERE net_id=?
                    """,
                    (ip, row["net_id"]),
                )
                updated = conn.execute(
                    "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
                ).fetchone()
                enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated), gen)
                found += 1
        fill_vlan_tags_from_cidrs(conn, gen)

    return {
        "found":   found,
        "checked": len(missing),
        "message": f"Queried {len(tasks)} VM(s) via QEMU guest agent, found {found} IP(s)",
    }


@router.post("/find-ips-via-pfsense-sweep", response_model=dict)
async def find_ips_via_pfsense_sweep() -> dict:
    """
    SSH to pfSense (which has a leg on every VLAN) and:
    1. Discover all VLAN interface CIDRs from ifconfig
    2. Ping-sweep each needed subnet (background pings, 2s wait) to populate ARP cache
    3. Read arp -a to collect MAC→IP mappings
    4. Match against proxmox_nets rows missing IPs and update DB

    Uses PHP (available on pfSense/FreeBSD) sent via base64 to avoid quoting issues.
    """
    import asyncio
    import base64
    import ipaddress
    import json as _json
    import re

    from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args
    ssh_target = os.environ.get("PFSENSE_SSH_TARGET", "")
    if not ssh_target:
        raise HTTPException(503, "PFSENSE_SSH_TARGET is not set in .env")

    if "@" in ssh_target:
        ssh_user, ssh_host = ssh_target.split("@", 1)
    else:
        ssh_user, ssh_host = "root", ssh_target

    try:
        _pfsense_ssh_args = make_ssh_args(ssh_host, connect_timeout=10)
    except (SshTargetNotFound, SshKeyMissing) as exc:
        raise HTTPException(503, str(exc))

    with get_conn() as conn:
        missing = conn.execute(
            """
            SELECT net_id, vlan_tag, mac_address
            FROM proxmox_nets
            WHERE (ip_address IS NULL OR ip_address = '')
              AND mac_address IS NOT NULL
            """
        ).fetchall()
        known_cidrs: dict[int, str] = {
            row["vlan_id"]: row["cidr"]
            for row in conn.execute(
                "SELECT vlan_id, cidr FROM vlans WHERE cidr IS NOT NULL AND cidr != ''"
            ).fetchall()
        }

    if not missing:
        return {"found": 0, "message": "No proxmox_nets rows with missing IPs"}

    need_vlans = sorted({row["vlan_tag"] for row in missing if row["vlan_tag"] is not None})
    vlans_json = _json.dumps(need_vlans)
    cidrs_json = _json.dumps({str(k): v for k, v in known_cidrs.items()})

    # PHP script — data embedded to avoid shell quoting issues
    # Sent via: echo <base64> | b64decode -r | php
    PHP_TEMPLATE = r"""<?php
$need_vlans  = json_decode(VLANS_JSON, true);
$known_cidrs = json_decode(CIDRS_JSON, true);

// Discover VLAN CIDRs from ifconfig
$ifc = []; exec('ifconfig', $ifc);
$vlan_to_cidr = $known_cidrs;
$current = null;
foreach ($ifc as $line) {
    if (preg_match('/^(\S+):/', $line, $m)) { $current = $m[1]; continue; }
    if ($current && preg_match('/inet (\d+\.\d+\.\d+\.\d+)\s+netmask\s+(0x[0-9a-f]+)/i', $line, $m)) {
        $mask_long = hexdec($m[2]);
        $prefix = substr_count(sprintf('%032b', $mask_long), '1');
        $net = long2ip(ip2long($m[1]) & $mask_long);
        $cidr = "$net/$prefix";
        if (preg_match('/[._](\d+)$/', $current, $vm)) {
            $vid = (int)$vm[1];
            if (!isset($vlan_to_cidr[$vid])) $vlan_to_cidr[$vid] = $cidr;
        }
    }
}

// Ping-sweep needed VLANs in background to populate ARP cache
foreach ($need_vlans as $vt) {
    $cidr = $vlan_to_cidr[$vt] ?? null;
    if (!$cidr) continue;
    list($net_addr, $pfx) = explode('/', $cidr);
    $total = min(1 << (32 - (int)$pfx), 254);
    $base = ip2long($net_addr);
    for ($i = 1; $i <= $total; $i++) {
        exec('ping -c 1 -t 1 ' . escapeshellarg(long2ip($base + $i)) . ' > /dev/null 2>&1 &');
    }
}
sleep(2);

// Read ARP cache
$arp = []; exec('arp -a', $arp);
echo "##SWEEPRESULT##" . json_encode([
    'vlan_to_cidr' => $vlan_to_cidr,
    'arp_output'   => implode("\n", $arp),
]) . "\n";
"""

    php_script = PHP_TEMPLATE \
        .replace("VLANS_JSON", "'" + vlans_json + "'") \
        .replace("CIDRS_JSON", "'" + cidrs_json + "'")

    encoded = base64.b64encode(php_script.encode()).decode()

    try:
        proc = await asyncio.create_subprocess_exec(
            "ssh", *_pfsense_ssh_args,
            f"{ssh_user}@{ssh_host}",
            f"echo {encoded} | b64decode -r | php",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=300)
    except Exception as exc:
        raise HTTPException(502, f"SSH to pfSense failed: {exc}")

    res: dict = {}
    for line in stdout.decode(errors="replace").splitlines():
        if line.startswith("##SWEEPRESULT##"):
            res = _json.loads(line[len("##SWEEPRESULT##"):])
            break

    if not res:
        return {"found": 0, "message": "pfSense PHP script returned no output"}

    arp_mac_to_ip: dict[str, str] = {}
    for line in res.get("arp_output", "").splitlines():
        m = re.search(r"\((\d+\.\d+\.\d+\.\d+)\)\s+at\s+([0-9a-f:]{17})", line, re.I)
        if m:
            arp_mac_to_ip[m.group(2).upper()] = m.group(1)

    new_vlan_cidrs: dict[int, str] = {int(k): v for k, v in res.get("vlan_to_cidr", {}).items()}
    found = 0
    vlans_hit: set[int] = set()

    with get_conn() as conn:
        gen = increment_gen(conn, "pfsense-sweep")
        for vid, cidr in new_vlan_cidrs.items():
            conn.execute(
                "INSERT OR IGNORE INTO vlans (vlan_id, cidr, cidr_inferred) VALUES (?,?,1)", (vid, cidr)
            )
            conn.execute(
                "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=? AND (cidr IS NULL OR cidr='')",
                (cidr, vid),
            )
            vlan_row = conn.execute("SELECT * FROM vlans WHERE vlan_id=?", (vid,)).fetchone()
            if vlan_row:
                enqueue_for_all_peers(conn, "UPDATE", "vlans", vid, dict(vlan_row), gen)

        all_cidrs = {**known_cidrs, **new_vlan_cidrs}
        for row in missing:
            mac = (row["mac_address"] or "").upper()
            ip  = arp_mac_to_ip.get(mac)
            if not ip:
                continue
            vt     = row["vlan_tag"]
            prefix = 24
            if vt is not None and vt in all_cidrs:
                try:
                    prefix = ipaddress.ip_network(all_cidrs[vt], strict=False).prefixlen
                except ValueError:
                    pass
            conn.execute(
                """
                UPDATE proxmox_nets SET
                    ip_address=?, ip_cidr=?, ip_source='pfsense-sweep',
                    updated_at=datetime('now')
                WHERE net_id=?
                """,
                (ip, f"{ip}/{prefix}", row["net_id"]),
            )
            updated = conn.execute(
                "SELECT * FROM proxmox_nets WHERE net_id=?", (row["net_id"],)
            ).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", row["net_id"], dict(updated), gen)
            found += 1
            if vt is not None:
                vlans_hit.add(vt)

        fill_vlan_tags_from_cidrs(conn, gen)

    return {
        "found":       found,
        "arp_entries": len(arp_mac_to_ip),
        "vlans_hit":   sorted(vlans_hit),
        "message":     f"pfSense sweep: {len(arp_mac_to_ip)} ARP entries, matched {found} MAC(s) across {len(need_vlans)} VLAN(s)",
    }


@router.put("/{net_id}", response_model=ProxmoxNetOut)
async def update_proxmox_net(net_id: str, body: ProxmoxNetUpdate) -> ProxmoxNetOut:
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [net_id]
    with get_conn() as conn:
        result = conn.execute(
            f"UPDATE proxmox_nets SET {set_clause}, updated_at=datetime('now') WHERE net_id=?",
            values,
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"net_id '{net_id}' not found")
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM proxmox_nets WHERE net_id=?", (net_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "proxmox_nets", net_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{net_id}", status_code=204)
async def delete_proxmox_net(net_id: str) -> None:
    with get_conn() as conn:
        result = conn.execute(
            "DELETE FROM proxmox_nets WHERE net_id=?", (net_id,)
        )
        if result.rowcount == 0:
            raise HTTPException(404, f"net_id '{net_id}' not found")
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "proxmox_nets", net_id, {}, gen)
