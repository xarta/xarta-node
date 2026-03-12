#!/usr/bin/env bash
# bp-pfsense-ping-sweep.sh
# Trigger a parallel ping sweep of all IPs in the pfsense_dns table.
# No SSH key required — runs entirely on the local Blueprints node.
#
# Usage:
#   ./bp-pfsense-ping-sweep.sh [--host http://localhost:8080]
#
# Exit codes:
#   0  sweep completed successfully
#   1  API unreachable or returned an error

set -euo pipefail

API_BASE="${1:-http://localhost:8080}"
ENDPOINT="${API_BASE}/api/v1/pfsense-dns/ping-sweep"

echo "➜ Triggering ping sweep via ${ENDPOINT} …"
echo "  (pings all A/AAAA IPs in parallel from this node — no SSH needed)"
echo ""

RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "${ENDPOINT}" \
  -H "Content-Type: application/json" 2>&1)

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [[ "$HTTP_CODE" != "200" ]]; then
    echo "✗ Sweep failed (HTTP ${HTTP_CODE}):"
    echo "  ${BODY}" | head -c 500
    exit 1
fi

# Parse fields with python3 (available in the venv environment)
if command -v python3 &>/dev/null; then
    python3 - <<EOF
import json, sys
d = json.loads('''${BODY}''')
print(f"✓ Ping sweep complete")
print(f"  IPs checked  : {d.get('ips_checked', 'n/a')}")
print(f"  Reached      : {d.get('reached', 'n/a')}")
print(f"  Unreachable  : {d.get('unreachable', 'n/a')}")
print(f"  MACs found   : {d.get('macs_found', 'n/a')}")
print(f"  Timestamp    : {d.get('timestamp', 'n/a')}")
EOF
else
    echo "✓ Sweep complete. Raw response:"
    echo "${BODY}"
fi
