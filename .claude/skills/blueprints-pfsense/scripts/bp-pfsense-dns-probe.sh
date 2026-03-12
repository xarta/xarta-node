#!/usr/bin/env bash
# bp-pfsense-dns-probe.sh — Harvest DNS resolver entries from pfSense and
# bulk-upsert them into the Blueprints pfsense_dns table.
#
# Usage:  bash bp-pfsense-dns-probe.sh <user>@<pfsense-ip> [api-base]
# Example: bash bp-pfsense-dns-probe.sh admin@10.0.0.1
#          bash bp-pfsense-dns-probe.sh admin@10.0.0.1 http://localhost:8080
#
# Safety: READ-ONLY on pfSense — only uses 'cat' to fetch config files.
#         Does NOT modify any pfSense files or restart any services.

set -euo pipefail

SSH_TARGET="${1:?Usage: $0 <user>@<pfsense-ip> [api-base]}"
API_BASE="${2:-http://localhost:8080}"
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

echo "=== pfSense DNS probe ==="
echo "Target:    ${SSH_TARGET}"
echo "API:       ${API_BASE}"
echo "Timestamp: ${TIMESTAMP}"
echo ""

# ── Step 1: Fetch config files from pfSense ──────────────────────────────
echo "[1/4] Fetching DNS config files from pfSense..."

HOST_ENTRIES=$(ssh -o ConnectTimeout=10 "${SSH_TARGET}" \
  "cat /var/unbound/host_entries.conf 2>/dev/null" || true)

DHCP_ENTRIES=$(ssh -o ConnectTimeout=10 "${SSH_TARGET}" \
  "cat /var/unbound/dhcpleases_entries.conf 2>/dev/null" || true)

echo "  host_entries.conf:      $(echo "${HOST_ENTRIES}" | wc -l | tr -d ' ') lines"
echo "  dhcpleases_entries.conf: $(echo "${DHCP_ENTRIES}" | wc -l | tr -d ' ') lines"

# ── Step 2: Parse and classify using Python ──────────────────────────────
echo ""
echo "[2/4] Parsing and classifying DNS entries..."

python3 << 'PYEOF'
import json, sys, os, re
from datetime import datetime, timezone
from urllib.request import Request, urlopen

api_base = os.environ.get("API_BASE", "http://localhost:8080")
timestamp = os.environ.get("TIMESTAMP", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"))
host_data = os.environ.get("HOST_ENTRIES", "")
dhcp_data = os.environ.get("DHCP_ENTRIES", "")

entries = {}  # keyed by dns_entry_id

# ── Parse host_entries.conf ──
# Collect PTR records first to classify host_override vs alias
ptrs = {}  # ip -> set of fqdns that have PTR records
a_records = []  # (fqdn, ip, record_type)

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
            "ip_address": ip,
            "fqdn": fqdn,
            "record_type": "PTR",
            "source": "host_override",
            "active": True,
            "last_seen": timestamp,
            "last_probed": timestamp,
        }
        continue

    # A/AAAA: local-data: "myhost.example.com A 192.168.1.10"
    m = re.match(r'^local-data:\s*"([^\s]+)\s+(A|AAAA)\s+([^"]+)"', line)
    if m:
        fqdn, rtype, ip = m.group(1).rstrip('.'), m.group(2), m.group(3)
        a_records.append((fqdn, ip, rtype))
        continue

# Classify A/AAAA records
for fqdn, ip, rtype in a_records:
    # Check if this fqdn has a matching PTR for the same IP
    ptr_fqdns = ptrs.get(ip, set())
    if fqdn in ptr_fqdns:
        source = "host_override"
    else:
        source = "host_override_alias"

    type_prefix = rtype.lower()
    eid = f"dns-{type_prefix}-{fqdn}"
    entries[eid] = {
        "dns_entry_id": eid,
        "ip_address": ip,
        "fqdn": fqdn,
        "record_type": rtype,
        "source": source,
        "active": True,
        "last_seen": timestamp,
        "last_probed": timestamp,
    }

# ── Parse dhcpleases_entries.conf ──
for line in dhcp_data.splitlines():
    line = line.strip()
    if not line or line.startswith('#'):
        continue

    m = re.match(r'^local-data-ptr:\s*"([^\s]+)\s+([^"]+)"', line)
    if m:
        ip, fqdn = m.group(1), m.group(2).rstrip('.')
        eid = f"dns-ptr-{fqdn}"
        entries[eid] = {
            "dns_entry_id": eid,
            "ip_address": ip,
            "fqdn": fqdn,
            "record_type": "PTR",
            "source": "dhcp_lease",
            "active": True,
            "last_seen": timestamp,
            "last_probed": timestamp,
        }
        continue

    m = re.match(r'^local-data:\s*"([^\s]+)\s+(A|AAAA)\s+([^"]+)"', line)
    if m:
        fqdn, rtype, ip = m.group(1).rstrip('.'), m.group(2), m.group(3)
        type_prefix = rtype.lower()
        eid = f"dns-{type_prefix}-{fqdn}"
        entries[eid] = {
            "dns_entry_id": eid,
            "ip_address": ip,
            "fqdn": fqdn,
            "record_type": rtype,
            "source": "dhcp_lease",
            "active": True,
            "last_seen": timestamp,
            "last_probed": timestamp,
        }
        continue

# ── Stats ──
by_source = {}
by_type = {}
for e in entries.values():
    by_source[e["source"]] = by_source.get(e["source"], 0) + 1
    by_type[e["record_type"]] = by_type.get(e["record_type"], 0) + 1

print(f"  Total entries: {len(entries)}")
print(f"  By source:     {dict(by_source)}")
print(f"  By type:       {dict(by_type)}")

# ── Step 3: Bulk upsert ──
print("")
print("[3/4] Bulk upserting to API...")

payload = json.dumps({"entries": list(entries.values())}).encode()
req = Request(
    f"{api_base}/api/v1/pfsense-dns/bulk",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urlopen(req) as resp:
        result = json.loads(resp.read())
        print(f"  Created: {result.get('created', 0)}")
        print(f"  Updated: {result.get('updated', 0)}")
        print(f"  Total:   {result.get('total', 0)}")
except Exception as e:
    print(f"  ERROR: {e}", file=sys.stderr)
    sys.exit(1)

print("")
print("[4/4] Done.")
print(f"  Unique IPs:    {len(set(e['ip_address'] for e in entries.values()))}")
print(f"  Unique FQDNs:  {len(set(e['fqdn'] for e in entries.values()))}")
PYEOF

echo ""
echo "=== Probe complete ==="
