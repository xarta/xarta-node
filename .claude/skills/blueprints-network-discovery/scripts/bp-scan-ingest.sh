#!/usr/bin/env bash
# bp-scan-ingest.sh — Parse scan results JSON and propose Blueprints API calls.
#
# Usage:
#   bash bp-scan-ingest.sh <scan-results.json> [--api-base <url>] [--dry-run]
#
# This script reads a JSON scan results file (output of bp-scan.sh) and:
#   1. Checks each discovered host/port against existing DB records.
#   2. Outputs proposed curl commands for creating/updating entries.
#   3. In --dry-run mode (default), only prints proposals — no API calls made.
#
# Requires: jq, curl, python3

set -euo pipefail

SCAN_FILE="${1:?Usage: bp-scan-ingest.sh <scan-results.json> [--api-base <url>] [--dry-run]}"
shift

API_BASE="${BLUEPRINTS_SELF_ADDRESS:-http://localhost:8080}"
DRY_RUN="true"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --api-base) API_BASE="$2"; shift 2 ;;
        --dry-run)  DRY_RUN="true"; shift ;;
        --execute)  DRY_RUN="false"; shift ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

if [[ ! -f "$SCAN_FILE" ]]; then
    echo "ERROR: File not found: $SCAN_FILE" >&2
    exit 1
fi

command -v jq >/dev/null 2>&1 || { echo "ERROR: jq not installed" >&2; exit 1; }

echo "=== Blueprints Scan Ingest ===" >&2
echo "Source: $SCAN_FILE" >&2
echo "API:    $API_BASE" >&2
echo "Mode:   $([ "$DRY_RUN" = "true" ] && echo "DRY RUN (proposals only)" || echo "EXECUTE")" >&2
echo "" >&2

# Fetch existing services for dedup
EXISTING_SERVICES=$(curl -sf "${API_BASE}/api/v1/services" 2>/dev/null || echo "[]")
EXISTING_MACHINES=$(curl -sf "${API_BASE}/api/v1/machines" 2>/dev/null || echo "[]")

HOST_COUNT=$(jq length "$SCAN_FILE")
echo "Discovered hosts: $HOST_COUNT" >&2
echo "" >&2

# Process each discovered host
python3 - "$SCAN_FILE" "$DRY_RUN" "$API_BASE" <<'PYEOF'
import json, sys, re

scan_file = sys.argv[1]
dry_run = sys.argv[2] == "true"
api_base = sys.argv[3]

with open(scan_file) as f:
    hosts = json.load(f)

proposals = []

for host in hosts:
    ip = host["ip"]
    hostname = host.get("hostname", "")
    ports = host.get("ports", [])

    # Generate a machine_id from hostname or IP
    machine_id = hostname.lower().replace(".", "-") if hostname else f"host-{ip.replace('.', '-')}"

    # Propose machine entry
    machine_proposal = {
        "action": "CREATE_MACHINE",
        "data": {
            "machine_id": machine_id,
            "name": hostname or ip,
            "type": "unknown",
            "ip_addresses": [ip],
            "description": f"Discovered via network scan at {host.get('scan_time', 'unknown')}",
        },
        "curl": (
            f'curl -X POST {api_base}/api/v1/machines '
            f'-H "Content-Type: application/json" '
            f"-d '{json.dumps(machine_proposal_data)}'"
        ) if False else "",  # placeholder
    }

    # Build the curl for machine
    mdata = machine_proposal["data"]
    machine_proposal["curl"] = (
        f'curl -X POST {api_base}/api/v1/machines '
        f'-H "Content-Type: application/json" '
        f"-d '{json.dumps(mdata)}'"
    )
    proposals.append(machine_proposal)

    # Propose service entries for interesting ports
    for p in ports:
        port_num = p["port"]
        service_name = p.get("service", "unknown")
        banner = p.get("banner", "")

        # Generate service_id
        svc_id = f"{machine_id}-{service_name}-{port_num}"

        # Infer service_kind
        service_kind = "app"
        if service_name in ("http", "https", "http-proxy"):
            service_kind = "web"
        elif service_name in ("ssh", "telnet"):
            service_kind = "infra-ui"
        elif service_name in ("mysql", "postgresql", "redis", "mongodb"):
            service_kind = "db"
        elif service_name in ("dns", "domain"):
            service_kind = "dns"

        svc_data = {
            "service_id": svc_id,
            "name": f"{banner or service_name} ({hostname or ip}:{port_num})",
            "description": f"Discovered: {service_name} on {ip}:{port_num}. {banner}".strip(),
            "host_machine_id": machine_id,
            "ports": [str(port_num)],
            "service_kind": service_kind,
            "exposure_level": "internal",
            "tags": ["discovered", "unverified"],
            "project_status": "discovered",
        }

        proposals.append({
            "action": "CREATE_SERVICE",
            "data": svc_data,
            "curl": (
                f'curl -X POST {api_base}/api/v1/services '
                f'-H "Content-Type: application/json" '
                f"-d '{json.dumps(svc_data)}'"
            ),
        })

# Output proposals
print(json.dumps(proposals, indent=2))

summary_machines = sum(1 for p in proposals if p["action"] == "CREATE_MACHINE")
summary_services = sum(1 for p in proposals if p["action"] == "CREATE_SERVICE")
print(f"\n# Proposals: {summary_machines} machines, {summary_services} services", file=sys.stderr)
if dry_run:
    print("# DRY RUN — no API calls made. Review above and re-run with --execute to apply.", file=sys.stderr)
PYEOF
