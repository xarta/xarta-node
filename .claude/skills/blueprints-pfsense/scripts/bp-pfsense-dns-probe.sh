#!/usr/bin/env bash
# bp-pfsense-dns-probe.sh — Harvest DNS resolver entries (+ ARP MACs) from
# pfSense and bulk-upsert them into the Blueprints pfsense_dns table.
#
# Usage:  bash bp-pfsense-dns-probe.sh [<user>@<pfsense-ip>] [api-base]
# Arguments are optional if PFSENSE_SSH_TARGET is set in the environment.
#
# Examples:
#   bash bp-pfsense-dns-probe.sh admin@10.0.0.1
#   bash bp-pfsense-dns-probe.sh admin@10.0.0.1 http://localhost:8080
#   PFSENSE_SSH_TARGET=admin@10.0.0.1 bash bp-pfsense-dns-probe.sh
#
# Safety: READ-ONLY on pfSense — only uses 'cat' and 'arp' to fetch data.
#         Does NOT modify any pfSense files or restart any services.
#
# Exit codes:
#   0 — success
#   1 — no SSH target configured
#   2 — SSH connectivity failed
#   3 — probe/parse/API error

set -euo pipefail

# ── Config ───────────────────────────────────────────────────────────────────
SSH_TARGET="${1:-${PFSENSE_SSH_TARGET:-}}"
API_BASE="${2:-${PFSENSE_API_BASE:-http://localhost:8080}}"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

if [[ -z "$SSH_TARGET" ]]; then
  echo "ERROR: No SSH target provided." >&2
  echo "       Pass as argument or set PFSENSE_SSH_TARGET in the environment / .env." >&2
  exit 1
fi

# Resolve SSH identity key. Preference order:
#   1. PFSENSE_SSH_KEY env var (explicit path)
#   2. SSH_KEY_NAME env var (key filename in ~/.ssh/, set in .env)
#   3. SSH default key lookup
SSH_KEY_FILE=""
if [[ -n "${PFSENSE_SSH_KEY:-}" ]]; then
  SSH_KEY_FILE="${PFSENSE_SSH_KEY}"
elif [[ -n "${SSH_KEY_NAME:-}" ]]; then
  SSH_KEY_FILE="/root/.ssh/${SSH_KEY_NAME}"
fi

# Build base SSH options array
SSH_OPTS=(-o ConnectTimeout=8 -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
[[ -n "$SSH_KEY_FILE" ]] && SSH_OPTS+=(-i "$SSH_KEY_FILE")

# ── VLAN source binding from ssh_targets ─────────────────────────────────────
_ssh_host_part="${SSH_TARGET##*@}"
_db="${BLUEPRINTS_DB:-/opt/blueprints/data/db/blueprints.db}"
if [[ -f "$_db" ]]; then
    _src=$(sqlite3 "$_db" "SELECT COALESCE(source_ip,'') FROM ssh_targets WHERE ip_address='${_ssh_host_part}' LIMIT 1;" 2>/dev/null) || true
    [[ -n "$_src" ]] && SSH_OPTS+=(-b "$_src")
fi
unset _ssh_host_part _db _src

echo "=== pfSense DNS probe ==="
echo "Target:    ${SSH_TARGET}"
echo "API:       ${API_BASE}"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# ── Step 1: Test SSH connectivity ────────────────────────────────────────────
echo "[1/5] Testing SSH connectivity..."
if ! ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" "uname -s" &>/dev/null; then
  echo "ERROR: Cannot connect to pfSense at ${SSH_TARGET}" >&2
  echo "       Check that the host is reachable and SSH key auth is configured." >&2
  exit 2
fi
echo "  OK — connected"

# ── Step 2: Fetch DNS config files ───────────────────────────────────────────
echo ""
echo "[2/5] Fetching DNS config files from pfSense..."

HOST_ENTRIES=$(ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" \
  "cat /var/unbound/host_entries.conf" || true)

DHCP_ENTRIES=$(ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" \
  "cat /var/unbound/dhcpleases_entries.conf" || true)

echo "  host_entries.conf:       $(echo "${HOST_ENTRIES}" | grep -c . || echo 0) lines"
echo "  dhcpleases_entries.conf: $(echo "${DHCP_ENTRIES}" | grep -c . || echo 0) lines"

# ── Step 3: Fetch ARP table for MAC addresses ────────────────────────────────
echo ""
echo "[3/5] Fetching ARP table for MAC address lookup..."

# FreeBSD arp -a -n format: "? (192.168.1.10) at 00:50:56:aa:bb:cc on em0 [ethernet]"
# Note: need -a -n separately; tcsh on pfSense doesn't accept combined -an
ARP_DATA=$(ssh "${SSH_OPTS[@]}" "${SSH_TARGET}" \
  "arp -a -n" || true)

ARP_LINES=$(echo "${ARP_DATA}" | grep -c "at [0-9a-f]" || echo 0)
echo "  ARP entries with MACs: ${ARP_LINES}"

# ── Step 4: Parse, classify, enrich with MACs, then bulk-upsert ──────────────
echo ""
echo "[4/5] Parsing entries and upserting to API..."

export HOST_ENTRIES DHCP_ENTRIES ARP_DATA API_BASE TIMESTAMP

python3 << 'PYEOF'
import json, sys, os, re
from datetime import datetime, timezone

timestamp = os.environ.get("TIMESTAMP", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
host_data = os.environ.get("HOST_ENTRIES", "")
dhcp_data = os.environ.get("DHCP_ENTRIES", "")
arp_data  = os.environ.get("ARP_DATA",  "")

# ── Build MAC lookup from ARP table ──────────────────────────────────────────
# FreeBSD: "? (192.168.1.10) at 00:50:56:aa:bb:cc on em0 [ethernet]"
mac_by_ip = {}
for line in arp_data.splitlines():
    m = re.match(r'\?\s+\(([^)]+)\)\s+at\s+([0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5})', line)
    if m:
        mac_by_ip[m.group(1)] = m.group(2).lower()

print(f"  MAC addresses from ARP table: {len(mac_by_ip)}")

# ── Parse host_entries.conf ───────────────────────────────────────────────────
entries = {}   # dns_entry_id -> entry dict
ptrs    = {}   # ip -> set of fqdns that have PTR records
a_recs  = []   # (fqdn, ip, record_type) — classify after seeing all PTRs

for line in host_data.splitlines():
    line = line.strip()
    if not line or line.startswith('#'):
        continue

    # PTR: local-data-ptr: "192.168.1.10 myhost.example.com"
    m = re.match(r'^local-data-ptr:\s*"([^\s]+)\s+([^"]+)"', line)
    if m:
        ip, fqdn = m.group(1), m.group(2).rstrip('.')
        ptrs.setdefault(ip, set()).add(fqdn)
        eid = f"dns-ptr-{fqdn}"
        entries[eid] = {
            "dns_entry_id": eid,
            "ip_address":   ip,
            "fqdn":         fqdn,
            "record_type":  "PTR",
            "source":       "host_override",
            "mac_address":  mac_by_ip.get(ip),
            "active":       True,
            "last_seen":    timestamp,
            "last_probed":  timestamp,
        }
        continue

    # A/AAAA: local-data: "myhost.example.com A 192.168.1.10"
    m = re.match(r'^local-data:\s*"([^\s]+)\s+(A|AAAA)\s+([^"]+)"', line)
    if m:
        a_recs.append((m.group(1).rstrip('.'), m.group(2), m.group(3)))

# Classify A/AAAA: host_override (has matching PTR) vs host_override_alias
for fqdn, rtype, ip in a_recs:
    source = "host_override" if fqdn in ptrs.get(ip, set()) else "host_override_alias"
    eid = f"dns-{rtype.lower()}-{fqdn}"
    entries[eid] = {
        "dns_entry_id": eid,
        "ip_address":   ip,
        "fqdn":         fqdn,
        "record_type":  rtype,
        "source":       source,
        "mac_address":  mac_by_ip.get(ip),
        "active":       True,
        "last_seen":    timestamp,
        "last_probed":  timestamp,
    }

# ── Parse dhcpleases_entries.conf ─────────────────────────────────────────────
for line in dhcp_data.splitlines():
    line = line.strip()
    if not line or line.startswith('#'):
        continue

    m = re.match(r'^local-data-ptr:\s*"([^\s]+)\s+([^"]+)"', line)
    if m:
        ip, fqdn = m.group(1), m.group(2).rstrip('.')
        eid = f"dns-ptr-{fqdn}"
        entries[eid] = {
            "dns_entry_id": eid, "ip_address": ip, "fqdn": fqdn,
            "record_type": "PTR", "source": "dhcp_lease",
            "mac_address": mac_by_ip.get(ip),
            "active": True, "last_seen": timestamp, "last_probed": timestamp,
        }
        continue

    m = re.match(r'^local-data:\s*"([^\s]+)\s+(A|AAAA)\s+([^"]+)"', line)
    if m:
        fqdn, rtype, ip = m.group(1).rstrip('.'), m.group(2), m.group(3)
        eid = f"dns-{rtype.lower()}-{fqdn}"
        entries[eid] = {
            "dns_entry_id": eid, "ip_address": ip, "fqdn": fqdn,
            "record_type": rtype, "source": "dhcp_lease",
            "mac_address": mac_by_ip.get(ip),
            "active": True, "last_seen": timestamp, "last_probed": timestamp,
        }

# ── Stats ─────────────────────────────────────────────────────────────────────
by_source    = {}
by_type      = {}
macs_enriched = 0
for e in entries.values():
    by_source[e["source"]] = by_source.get(e["source"], 0) + 1
    by_type[e["record_type"]] = by_type.get(e["record_type"], 0) + 1
    if e.get("mac_address"):
        macs_enriched += 1

print(f"  Total entries: {len(entries)}")
print(f"  By source:     {dict(by_source)}")
print(f"  By type:       {dict(by_type)}")
print(f"  MAC-enriched:  {macs_enriched}")

# ── Output machine-readable lines (parsed by the /probe API endpoint) ─────────
# The API handles the actual DB upsert in-process (avoids re-entrant HTTP call).
stats = {
    "mac_addresses_found": len(mac_by_ip),
    "mac_enriched":        macs_enriched,
    "total_parsed":        len(entries),
}
print(f"##ENTRIES## {json.dumps(list(entries.values()))}")
print(f"##STATS##   {json.dumps(stats)}")
PYEOF

echo ""
echo "[5/5] Done."
