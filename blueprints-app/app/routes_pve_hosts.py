"""routes_pve_hosts.py — CRUD + CIDR-scan Proxmox discovery for /api/v1/pve-hosts

Probe chain starting-point:
  1. User saves mgmt_cidr in settings (e.g. "10.0.0.0/24")
  2. POST /api/v1/pve-hosts/scan  — scans every IP:8006, looks for Proxmox login
     page markers, upserts discovered hosts into pve_hosts table
  3. Downstream probes (proxmox-config, dockge-stacks, caddy-configs) read their
     target IPs from GET /api/v1/pve-hosts — no hardcoded addresses anywhere
"""

import asyncio
import ipaddress
import logging
import os
import re
import socket
import struct
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, HTTPException

from .db import get_conn, get_setting, increment_gen
from .models import PveHostCreate, PveHostOut, PveHostUpdate
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/pve-hosts", tags=["pve-hosts"])

_SCAN_CONCURRENCY = 40   # max simultaneous HTTPS connections during a CIDR scan
_SCAN_TIMEOUT     = 2.5  # seconds — short timeout to keep scans snappy

# Proxmox web UI signatures — any of these in the response body = hit
_PVE_MARKERS = ("PVELoginForm", "proxmoxlib.js", "Proxmox Virtual Environment")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> PveHostOut:
    return PveHostOut(**dict(row))


def _pfsense_ip() -> str | None:
    """Extract the pfSense IP from PFSENSE_SSH_TARGET (e.g. 'admin@10.0.0.1').
    Returns None when the env var is absent or malformed — callers must handle None."""
    target = os.environ.get("PFSENSE_SSH_TARGET", "").strip()
    if not target:
        return None
    ip = target.split("@")[-1].strip()
    return ip if ip else None


async def _resolve_tailnet_ip(pve_name: str, tailnet_domain: str, timeout: float = 2.0) -> str | None:
    """
    Try to resolve `<pve_name>.<tailnet_domain>` via Tailscale's internal DNS
    server at 100.100.100.100 using a raw UDP query.  This is more reliable than
    the system getaddrinfo because it bypasses stub-resolver configuration and
    directly queries the tailnet nameserver.

    Returns the first A-record IP on success, or None if the host is not in the
    tailnet (NXDOMAIN / SERVFAIL) or the DNS server is unreachable.
    """
    if not pve_name or not tailnet_domain:
        return None
    fqdn = f"{pve_name}.{tailnet_domain}"
    loop = asyncio.get_running_loop()
    try:
        ip = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: _udp_dns_query(fqdn, "100.100.100.100")),
            timeout=timeout,
        )
        if ip:
            log.debug("tailnet resolve via 100.100.100.100: %s → %s", fqdn, ip)
        else:
            log.debug("tailnet resolve via 100.100.100.100: %s — not found (NXDOMAIN/SERVFAIL)", fqdn)
        return ip
    except Exception as exc:
        log.debug("tailnet resolve via 100.100.100.100: %s failed — %s", fqdn, exc)
    return None


def _udp_dns_query(fqdn: str, nameserver: str, port: int = 53, timeout: float = 2.0) -> str | None:
    """
    Send a raw UDP DNS A-record query to `nameserver:port`.
    Returns the first A-record IP string, or None (NXDOMAIN, SERVFAIL, timeout).
    Pure stdlib — no extra dependencies.
    """
    # Build query: header + question
    tid = 0xBB01
    header = struct.pack(">HHHHHH", tid, 0x0100, 1, 0, 0, 0)
    question = b""
    for label in fqdn.rstrip(".").split("."):
        enc = label.encode("ascii")
        question += bytes([len(enc)]) + enc
    question += b"\x00\x00\x01\x00\x01"  # null label + QTYPE=A + QCLASS=IN

    pkt = header + question

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.settimeout(timeout)
        s.sendto(pkt, (nameserver, port))
        resp = s.recv(512)

    if len(resp) < 12:
        return None

    resp_tid, flags, qdcount, ancount = struct.unpack(">HHHH", resp[:8])
    if resp_tid != tid:
        return None
    rcode = flags & 0x000F
    if rcode != 0 or ancount == 0:  # SERVFAIL, NXDOMAIN, or no answers
        return None

    # Skip past the header (12 bytes) and question section
    pos = 12
    for _ in range(qdcount):
        while pos < len(resp):
            n = resp[pos]
            if n == 0:
                pos += 1
                break
            if n & 0xC0 == 0xC0:  # compression pointer
                pos += 2
                break
            pos += n + 1
        pos += 4  # QTYPE + QCLASS

    # Parse answer records, return first A record
    for _ in range(ancount):
        # Skip name
        while pos < len(resp):
            n = resp[pos]
            if n == 0:
                pos += 1
                break
            if n & 0xC0 == 0xC0:
                pos += 2
                break
            pos += n + 1
        if pos + 10 > len(resp):
            break
        rtype, rclass, _ttl, rdlen = struct.unpack(">HHIH", resp[pos:pos + 10])
        pos += 10
        if rtype == 1 and rclass == 1 and rdlen == 4:  # A record, IN class
            return ".".join(str(b) for b in resp[pos:pos + 4])
        pos += rdlen

    return None


async def _check_reachable_tcp(ip: str, port: int, timeout: float = 3.0) -> bool:
    """Best-effort TCP connect to ip:port. Returns True on success."""
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port), timeout=timeout
        )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except Exception:
        return False


async def _reverse_dns(ip: str, dns_server: str, timeout: float = 2.0) -> str | None:
    """
    Non-blocking reverse DNS lookup via `dig -x <ip> @<dns_server> +short`.
    Returns the hostname (trailing dot stripped) or None on any failure.
    Best-effort only — never raises.
    """
    try:
        proc = await asyncio.create_subprocess_exec(
            "dig", "-x", ip, f"@{dns_server}", "+short", "+time=2", "+tries=1",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            return None
        name = stdout.decode().strip()
        # dig returns empty or lines starting with ";" on failure
        if name and not name.startswith(";"):
            return name.split()[0].rstrip(".")
    except Exception:
        pass
    return None


async def _check_proxmox(
    ip: str, port: int, sem: asyncio.Semaphore, timestamp: str,
    dns_server: str | None = None,
    tailnet_domain: str | None = None,
) -> dict | None:
    """
    Try HTTPS on ip:port.  Return a candidate dict when the response looks
    like a Proxmox web UI, otherwise return None.
    Optionally does a best-effort reverse-DNS lookup via dns_server (pfSense).
    """
    url = f"https://{ip}:{port}/"
    async with sem:
        try:
            async with httpx.AsyncClient(
                verify=False, timeout=_SCAN_TIMEOUT,
                follow_redirects=True,
            ) as client:
                r = await client.get(url)
            if not any(marker in r.text for marker in _PVE_MARKERS):
                return None
            # Best-effort version extraction
            version = None
            m = re.search(r'pve(?:version)?\s*[=:"\s]+([0-9]+\.[0-9][^\s"<]*)', r.text, re.I)
            if not m:
                m = re.search(r'(\d+\.\d+-\d+)', r.text)
            if m:
                version = m.group(1)
            # Best-effort reverse DNS (only if pfSense IP is known)
            hostname = None
            pve_name = None
            if dns_server:
                hostname = await _reverse_dns(ip, dns_server)
                if hostname:
                    # Use the first label (e.g. "pve1" from "pve1.infra.example.com")
                    pve_name = hostname.split(".")[0]
            # Best-effort tailnet IP resolution (only if pve_name + tailnet_domain known)
            tailnet_ip = None
            if pve_name and tailnet_domain:
                tailnet_ip = await _resolve_tailnet_ip(pve_name, tailnet_domain)
            return {
                "pve_id":        ip,
                "ip_address":    ip,
                "hostname":      hostname,
                "pve_name":      pve_name,
                "version":       version,
                "port":          port,
                "ssh_reachable": 0,
                "tailnet_ip":    tailnet_ip,
                "last_scanned":  timestamp,
            }
        except Exception:
            return None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[PveHostOut])
async def list_pve_hosts() -> list[PveHostOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pve_hosts ORDER BY ip_address"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=PveHostOut, status_code=201)
async def create_pve_host(body: PveHostCreate) -> PveHostOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT pve_id FROM pve_hosts WHERE pve_id=?", (body.pve_id,)
        ).fetchone():
            raise HTTPException(409, f"pve_id '{body.pve_id}' already exists")
        gen = increment_gen(conn, "pve-hosts-create")
        conn.execute(
            """INSERT INTO pve_hosts
               (pve_id, ip_address, hostname, pve_name, version,
                port, ssh_reachable, tailnet_ip, last_scanned)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (body.pve_id, body.ip_address, body.hostname, body.pve_name,
             body.version, body.port, body.ssh_reachable, body.tailnet_ip, body.last_scanned),
        )
        row = conn.execute(
            "SELECT * FROM pve_hosts WHERE pve_id=?", (body.pve_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "pve_hosts", body.pve_id, dict(row), gen)
    return _row_to_out(row)


@router.get("/scan/status", response_model=dict)
async def scan_status() -> dict:
    """Report whether the CIDR scan is ready to run."""
    with get_conn() as conn:
        cidr = get_setting(conn, "mgmt_cidr")
    if not cidr:
        return {"ready": False, "reason": "mgmt_cidr not set — enter it in Settings"}
    try:
        net = ipaddress.ip_network(cidr, strict=False)
        host_count = net.num_addresses - 2
        return {
            "ready": True, "cidr": cidr,
            "host_count": max(host_count, 0), "reason": "",
        }
    except ValueError as e:
        return {"ready": False, "reason": f"invalid CIDR: {e}"}


@router.post("/scan", response_model=dict)
async def scan_for_proxmox() -> dict:
    """
    Read mgmt_cidr from settings, scan every IP:8006 concurrently, and upsert
    any Proxmox instances found into pve_hosts.

    Uses the Proxmox web UI fingerprint (no credentials needed).
    A /24 typically completes in under 10 seconds.
    """
    with get_conn() as conn:
        cidr = get_setting(conn, "mgmt_cidr")
    if not cidr:
        raise HTTPException(
            400, "mgmt_cidr not set — save it in Settings first"
        )
    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError as e:
        raise HTTPException(400, f"Invalid CIDR '{cidr}': {e}")

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    sem       = asyncio.Semaphore(_SCAN_CONCURRENCY)
    port      = 8006

    log.info("pve-hosts scan: checking %s (%d hosts) on port %d",
             cidr, sum(1 for _ in network.hosts()), port)

    dns_server    = _pfsense_ip()   # None when PFSENSE_SSH_TARGET not set — that's fine
    # Get tailnet domain from .nodes.json via config (e.g. "yourtailnet.ts.net")
    try:
        from . import config as cfg
        tailnet_domain = cfg._self_node.get("tailnet") or None
    except Exception:
        tailnet_domain = None
    if dns_server:
        log.info("pve-hosts scan: reverse-DNS via pfSense at %s", dns_server)
    else:
        log.info("pve-hosts scan: PFSENSE_SSH_TARGET not set, skipping reverse DNS")
    if tailnet_domain:
        log.info("pve-hosts scan: will attempt tailnet resolution via %s", tailnet_domain)

    tasks   = [_check_proxmox(str(ip), port, sem, timestamp, dns_server, tailnet_domain) for ip in network.hosts()]
    results = await asyncio.gather(*tasks)
    found   = [r for r in results if r is not None]

    log.info("pve-hosts scan: found %d candidate(s)", len(found))

    created = updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "pve-hosts-scan")
        for candidate in found:
            cid = candidate["pve_id"]
            existing = conn.execute(
                "SELECT pve_id FROM pve_hosts WHERE pve_id=?", (cid,)
            ).fetchone()
            if existing:
                # Non-destructive update: only fill in blanks or update scanned fields.
                # Never clear an existing tailnet_ip or pve_name that was manually set.
                conn.execute(
                    """UPDATE pve_hosts
                       SET version=?, last_scanned=?, updated_at=datetime('now'),
                           hostname=COALESCE(hostname, ?),
                           pve_name=COALESCE(pve_name, ?),
                           tailnet_ip=COALESCE(tailnet_ip, ?)
                       WHERE pve_id=?""",
                    (candidate["version"], timestamp,
                     candidate["hostname"], candidate["pve_name"],
                     candidate["tailnet_ip"], cid),
                )
                updated += 1
            else:
                conn.execute(
                    """INSERT INTO pve_hosts
                       (pve_id, ip_address, hostname, pve_name, version,
                        port, ssh_reachable, tailnet_ip, last_scanned)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (cid, candidate["ip_address"], candidate["hostname"],
                     candidate["pve_name"], candidate["version"], candidate["port"],
                     candidate["ssh_reachable"], candidate["tailnet_ip"],
                     candidate["last_scanned"]),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM pve_hosts WHERE pve_id=?", (cid,)
            ).fetchone()
            enqueue_for_all_peers(
                conn,
                "UPDATE" if existing else "INSERT",
                "pve_hosts", cid, dict(row), gen,
            )

    tailnet_resolved = sum(1 for c in found if c.get("tailnet_ip"))
    if tailnet_domain and not tailnet_resolved and found:
        log.info(
            "pve-hosts scan: tailnet resolution via %s failed for all %d host(s) "
            "— this node may not have Tailscale MagicDNS access. "
            "Use Edit to set tailnet IPs manually.",
            tailnet_domain, len(found),
        )

    return {
        "ips_checked": sum(1 for _ in network.hosts()),
        "found":   len(found),
        "created": created,
        "updated": updated,
        "cidr":    cidr,
        "tailnet_resolved": tailnet_resolved,
    }


@router.get("/{pve_id}", response_model=PveHostOut)
async def get_pve_host(pve_id: str) -> PveHostOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pve_hosts WHERE pve_id=?", (pve_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"pve_id '{pve_id}' not found")
    return _row_to_out(row)


@router.put("/{pve_id}", response_model=PveHostOut)
async def update_pve_host(pve_id: str, body: PveHostUpdate) -> PveHostOut:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT pve_id FROM pve_hosts WHERE pve_id=?", (pve_id,)
        ).fetchone():
            raise HTTPException(404, f"pve_id '{pve_id}' not found")
        gen    = increment_gen(conn, "pve-hosts-update")
        fields = {k: v for k, v in body.model_dump().items() if v is not None}
        if fields:
            set_clause = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE pve_hosts SET {set_clause}, updated_at=datetime('now')"
                f" WHERE pve_id=?",
                (*fields.values(), pve_id),
            )
        row = conn.execute(
            "SELECT * FROM pve_hosts WHERE pve_id=?", (pve_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "pve_hosts", pve_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{pve_id}", status_code=204)
async def delete_pve_host(pve_id: str) -> None:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT pve_id FROM pve_hosts WHERE pve_id=?", (pve_id,)
        ).fetchone():
            raise HTTPException(404, f"pve_id '{pve_id}' not found")
        gen = increment_gen(conn, "pve-hosts-delete")
        conn.execute("DELETE FROM pve_hosts WHERE pve_id=?", (pve_id,))
        enqueue_for_all_peers(conn, "DELETE", "pve_hosts", pve_id, None, gen)


@router.get("/{pve_id}/reachable", response_model=dict)
async def check_pve_host_reachable(pve_id: str) -> dict:
    """
    TCP-connect probe to the management IP (port 8006) and tailnet IP (port 8006)
    of a known PVE host.  Used by the GUI diagnostic to assess host reachability
    from the backend LXC — which has direct VLAN access the browser lacks.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT ip_address, tailnet_ip, pve_name, port FROM pve_hosts WHERE pve_id=?",
            (pve_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"pve_id '{pve_id}' not found")

    port = row["port"] or 8006
    mgmt_ip    = row["ip_address"]
    tailnet_ip = row["tailnet_ip"]
    pve_name   = row["pve_name"] or pve_id

    mgmt_ok    = await _check_reachable_tcp(mgmt_ip, port) if mgmt_ip else None
    tailnet_ok = await _check_reachable_tcp(tailnet_ip, port) if tailnet_ip else None

    return {
        "pve_id":         pve_id,
        "pve_name":       pve_name,
        "mgmt_ip":        mgmt_ip,
        "mgmt_reachable": mgmt_ok,
        "tailnet_ip":     tailnet_ip,
        "tailnet_reachable": tailnet_ok,
    }
