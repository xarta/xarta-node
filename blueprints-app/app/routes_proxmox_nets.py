"""routes_proxmox_nets.py — CRUD + pfSense enrichment for /api/v1/proxmox-nets

One row per network interface per VM/LXC.  Populated by the proxmox-config
probe script alongside the parent proxmox_config rows.

The enrich-from-pfsense endpoint does a MAC-address JOIN against pfsense_dns
to fill in ip_address for interfaces where the conf file had no static IP
(common for DHCP-assigned containers and for QEMU VMs that use cloud-init).
"""

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

    return {"enriched": enriched, "checked": checked}


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
