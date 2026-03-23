"""routes_pfsense_dns.py — CRUD for /api/v1/pfsense-dns"""

import asyncio
import json
import os
import re
import subprocess
import time as _time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import PfSenseDnsCreate, PfSenseDnsOut, PfSenseDnsUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/pfsense-dns", tags=["pfsense-dns"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_out(row) -> PfSenseDnsOut:
    return PfSenseDnsOut(
        dns_entry_id=row["dns_entry_id"],
        ip_address=row["ip_address"],
        fqdn=row["fqdn"],
        record_type=row["record_type"],
        source=row["source"],
        mac_address=row["mac_address"],
        active=row["active"],
        last_seen=row["last_seen"],
        last_probed=row["last_probed"],
        ping_ms=row["ping_ms"],
        last_ping_check=row["last_ping_check"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[PfSenseDnsOut])
async def list_pfsense_dns() -> list[PfSenseDnsOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pfsense_dns ORDER BY ip_address, fqdn"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("", response_model=PfSenseDnsOut, status_code=201)
async def create_pfsense_dns(body: PfSenseDnsCreate) -> PfSenseDnsOut:
    with get_conn() as conn:
        if conn.execute(
            "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
            (body.dns_entry_id,),
        ).fetchone():
            raise HTTPException(409, f"dns_entry_id '{body.dns_entry_id}' already exists")

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO pfsense_dns
                (dns_entry_id, ip_address, fqdn, record_type, source,
                 mac_address, active, last_seen, last_probed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                body.dns_entry_id,
                body.ip_address,
                body.fqdn,
                body.record_type,
                body.source,
                body.mac_address,
                body.active,
                body.last_seen,
                body.last_probed,
            ),
        )
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (body.dns_entry_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "INSERT", "pfsense_dns", body.dns_entry_id, dict(row), gen
        )
    return _row_to_out(row)


@router.post("/bulk", response_model=dict, status_code=200)
async def bulk_upsert_pfsense_dns(entries: list[PfSenseDnsCreate]) -> dict:
    """Upsert many DNS entries at once — used by the discovery script."""
    created = 0
    updated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "pfsense-probe")
        for body in entries:
            existing = conn.execute(
                "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
                (body.dns_entry_id,),
            ).fetchone()

            if existing:
                conn.execute(
                    """
                    UPDATE pfsense_dns
                    SET ip_address=?, fqdn=?, record_type=?, source=?,
                        mac_address=?, active=?, last_seen=?, last_probed=?,
                        updated_at=datetime('now')
                    WHERE dns_entry_id=?
                    """,
                    (
                        body.ip_address, body.fqdn, body.record_type, body.source,
                        body.mac_address, body.active, body.last_seen, body.last_probed,
                        body.dns_entry_id,
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO pfsense_dns
                        (dns_entry_id, ip_address, fqdn, record_type, source,
                         mac_address, active, last_seen, last_probed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        body.dns_entry_id, body.ip_address, body.fqdn,
                        body.record_type, body.source, body.mac_address,
                        body.active, body.last_seen, body.last_probed,
                    ),
                )
                created += 1

            row = conn.execute(
                "SELECT * FROM pfsense_dns WHERE dns_entry_id=?",
                (body.dns_entry_id,),
            ).fetchone()
            action = "UPDATE" if existing else "INSERT"
            enqueue_for_all_peers(
                conn, action, "pfsense_dns", body.dns_entry_id, dict(row), gen
            )

    return {"created": created, "updated": updated, "total": created + updated}


# ── Probe helpers ─────────────────────────────────────────────────────────────

_PROBE_SCRIPT = "/root/xarta-node/.claude/skills/blueprints-pfsense/scripts/bp-pfsense-dns-probe.sh"


def _probe_config() -> dict:
    """Return probe configuration status — used by both /probe and /probe/status."""
    from .ssh import probe_status_for_host_type
    ssh_target = os.environ.get("PFSENSE_SSH_TARGET", "").strip()
    if not ssh_target:
        return {
            "configured":      False,
            "ssh_target_set":  False,
            "ssh_key_present": False,
            "reason": "PFSENSE_SSH_TARGET is not set in .env",
        }
    status = probe_status_for_host_type("pfsense")
    return {
        "configured":      status["configured"],
        "ssh_target_set":  True,
        "ssh_key_present": status["ssh_key_present"],
        "reason":          status["reason"],
    }


@router.get("/probe/status", response_model=dict)
async def probe_status() -> dict:
    """Report whether this node is configured to run the pfSense probe."""
    return _probe_config()


@router.post("/probe", response_model=dict)
async def probe_pfsense_dns() -> dict:
    """Run the pfSense DNS probe script and upsert results into the DB."""
    cfg = _probe_config()
    if not cfg["configured"]:
        raise HTTPException(503, cfg["reason"])

    if not os.path.exists(_PROBE_SCRIPT):
        raise HTTPException(503, f"Probe script not found at {_PROBE_SCRIPT}")

    ssh_target = os.environ.get("PFSENSE_SSH_TARGET", "").strip()

    try:
        proc = subprocess.run(
            ["bash", _PROBE_SCRIPT, ssh_target],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Probe timed out after 120 seconds")

    if proc.returncode != 0:
        detail = (proc.stderr.strip() or proc.stdout.strip() or
                  f"Script exited with code {proc.returncode}")
        raise HTTPException(502, f"Probe failed: {detail}")

    # Parse ##ENTRIES## and ##STATS## lines from script stdout
    entries_raw = None
    stats_raw   = {}
    for line in proc.stdout.splitlines():
        if line.startswith("##ENTRIES## "):
            entries_raw = json.loads(line[len("##ENTRIES## "):])
        elif line.startswith("##STATS##"):
            stats_raw = json.loads(line.split(None, 1)[1])

    if entries_raw is None:
        raise HTTPException(502, f"Probe produced no entries. Output: {proc.stdout[-500:]}")

    # ── In-process bulk upsert (avoids re-entrant HTTP call to self) ──────────
    created = 0
    updated = 0
    deactivated = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "pfsense-probe")

        # ── Staleness: mark entries absent from this probe as inactive ─────────
        # Guard: only run if probe returned data (empty list would wipe everything)
        incoming_ids = {e["dns_entry_id"] for e in entries_raw}
        if incoming_ids:
            currently_active = {
                r["dns_entry_id"]
                for r in conn.execute(
                    "SELECT dns_entry_id FROM pfsense_dns WHERE active=1"
                ).fetchall()
            }
            stale_ids = currently_active - incoming_ids
            for stale_id in stale_ids:
                conn.execute(
                    "UPDATE pfsense_dns SET active=0, updated_at=datetime('now') WHERE dns_entry_id=?",
                    (stale_id,),
                )
                stale_row = conn.execute(
                    "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (stale_id,)
                ).fetchone()
                enqueue_for_all_peers(
                    conn, "UPDATE", "pfsense_dns", stale_id, dict(stale_row), gen
                )
                deactivated += 1

        for entry in entries_raw:
            existing = conn.execute(
                "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
                (entry["dns_entry_id"],),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE pfsense_dns
                    SET ip_address=?, fqdn=?, record_type=?, source=?,
                        mac_address=?, active=?, last_seen=?, last_probed=?,
                        updated_at=datetime('now')
                    WHERE dns_entry_id=?
                    """,
                    (
                        entry.get("ip_address"), entry.get("fqdn"),
                        entry.get("record_type"), entry.get("source"),
                        entry.get("mac_address"), entry.get("active"),
                        entry.get("last_seen"), entry.get("last_probed"),
                        entry["dns_entry_id"],
                    ),
                )
                updated += 1
            else:
                conn.execute(
                    """
                    INSERT INTO pfsense_dns
                        (dns_entry_id, ip_address, fqdn, record_type, source,
                         mac_address, active, last_seen, last_probed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entry["dns_entry_id"], entry.get("ip_address"),
                        entry.get("fqdn"), entry.get("record_type"),
                        entry.get("source"), entry.get("mac_address"),
                        entry.get("active"), entry.get("last_seen"),
                        entry.get("last_probed"),
                    ),
                )
                created += 1
            row = conn.execute(
                "SELECT * FROM pfsense_dns WHERE dns_entry_id=?",
                (entry["dns_entry_id"],),
            ).fetchone()
            action = "UPDATE" if existing else "INSERT"
            enqueue_for_all_peers(
                conn, action, "pfsense_dns", entry["dns_entry_id"], dict(row), gen
            )

    return {
        "created":             created,
        "updated":             updated,
        "deactivated":         deactivated,
        "total":               created + updated,
        "mac_addresses_found": stats_raw.get("mac_addresses_found", 0),
        "mac_enriched":        stats_raw.get("mac_enriched", 0),
    }


@router.post("/ping-sweep", response_model=dict)
async def ping_sweep() -> dict:
    """Ping all IPs in pfsense_dns in parallel (no SSH needed), update ping_ms/mac_address/last_ping_check."""

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    # ── Collect distinct IPs from A/AAAA records ──────────────────────────────
    with get_conn() as conn:
        ip_rows = conn.execute(
            "SELECT DISTINCT ip_address FROM pfsense_dns WHERE record_type IN ('A','AAAA')"
        ).fetchall()
    ips: list[str] = [r["ip_address"] for r in ip_rows]

    # ── Ping all IPs in parallel ───────────────────────────────────────────────
    semaphore = asyncio.Semaphore(60)  # max concurrent pings

    async def ping_one(ip: str) -> tuple[str, float | None]:
        async with semaphore:
            t0 = _time.monotonic()
            try:
                proc = await asyncio.create_subprocess_exec(
                    "ping", "-c", "1", "-W", "1", ip,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=1.5)
                if proc.returncode == 0:
                    text = stdout.decode()
                    # Parse RTT from "time=1.23 ms" or "time<1 ms"
                    m = re.search(r"time[=<](\d+(?:\.\d+)?)\s*ms", text)
                    rtt = float(m.group(1)) if m else round((_time.monotonic() - t0) * 1000, 1)
                    return ip, rtt
            except (asyncio.TimeoutError, Exception):
                pass
            return ip, None

    results: list[tuple[str, float | None]] = await asyncio.gather(
        *[ping_one(ip) for ip in ips]
    )
    ping_map = {ip: ms for ip, ms in results}

    # ── Read local ARP cache from /proc/net/arp ───────────────────────────────
    # Format: IP  HW-type  Flags  HW-address  Mask  Device
    mac_map: dict[str, str] = {}
    try:
        with open("/proc/net/arp") as f:
            for line in f.readlines()[1:]:
                parts = line.split()
                if len(parts) >= 4 and parts[3] not in ("00:00:00:00:00:00", "HW"):
                    mac_map[parts[0]] = parts[3].lower()
    except OSError:
        pass

    # ── Update DB in-process ───────────────────────────────────────────────────
    reached    = sum(1 for ms in ping_map.values() if ms is not None)
    macs_found = 0
    with get_conn() as conn:
        gen = increment_gen(conn, "ping-sweep")
        for ip, ms in ping_map.items():
            mac = mac_map.get(ip)
            if mac:
                macs_found += 1
            # Update MAC only if we actually discovered one (don't wipe existing MACs)
            conn.execute(
                """
                UPDATE pfsense_dns
                SET ping_ms          = ?,
                    last_ping_check  = ?,
                    mac_address      = COALESCE(?, mac_address),
                    updated_at       = datetime('now')
                WHERE ip_address = ?
                """,
                (ms, timestamp, mac, ip),
            )
        # Enqueue sync updates for all touched rows
        placeholders = ",".join("?" * len(ips))
        rows = conn.execute(
            f"SELECT * FROM pfsense_dns WHERE ip_address IN ({placeholders})", ips
        ).fetchall()
        for row in rows:
            enqueue_for_all_peers(
                conn, "UPDATE", "pfsense_dns", row["dns_entry_id"], dict(row), gen
            )

    return {
        "ips_checked":  len(ips),
        "reached":      reached,
        "unreachable":  len(ips) - reached,
        "macs_found":   macs_found,
        "timestamp":    timestamp,
    }


@router.get("/{dns_entry_id}", response_model=PfSenseDnsOut)
async def get_pfsense_dns(dns_entry_id: str) -> PfSenseDnsOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")
    return _row_to_out(row)


@router.put("/{dns_entry_id}", response_model=PfSenseDnsOut)
async def update_pfsense_dns(dns_entry_id: str, body: PfSenseDnsUpdate) -> PfSenseDnsOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")

        update_data = body.model_dump(exclude_none=True)
        if not update_data:
            return _row_to_out(existing)

        set_parts = []
        values = []
        for field, val in update_data.items():
            set_parts.append(f"{field}=?")
            values.append(val)
        set_parts.append("updated_at=datetime('now')")
        values.append(dns_entry_id)

        gen = increment_gen(conn, "human")
        conn.execute(
            f"UPDATE pfsense_dns SET {', '.join(set_parts)} WHERE dns_entry_id=?",
            values,
        )
        row = conn.execute(
            "SELECT * FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,)
        ).fetchone()
        enqueue_for_all_peers(
            conn, "UPDATE", "pfsense_dns", dns_entry_id, dict(row), gen
        )
    return _row_to_out(row)


@router.delete("/{dns_entry_id}", status_code=204)
async def delete_pfsense_dns(dns_entry_id: str) -> None:
    with get_conn() as conn:
        if not conn.execute(
            "SELECT dns_entry_id FROM pfsense_dns WHERE dns_entry_id=?",
            (dns_entry_id,),
        ).fetchone():
            raise HTTPException(404, f"dns entry '{dns_entry_id}' not found")

        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM pfsense_dns WHERE dns_entry_id=?", (dns_entry_id,))
        enqueue_for_all_peers(
            conn, "DELETE", "pfsense_dns", dns_entry_id, None, gen
        )
