#!/usr/bin/env bash
# bp-scan.sh — Run an nmap scan and output structured JSON.
#
# Usage:
#   bash bp-scan.sh <target> [--ports <port-spec>] [--output <file>] [--max-rate <rate>]
#
# Examples:
#   bash bp-scan.sh 10.0.50.0/24
#   bash bp-scan.sh 10.0.50.1 --ports 1-65535 --output results.json
#   bash bp-scan.sh 100.64.0.0/10 --ports 80,443,8080 --max-rate 500
#
# Requires: nmap, jq
# Output: JSON array to stdout (and optionally to --output file)

set -euo pipefail

TARGET="${1:?Usage: bp-scan.sh <target> [--ports <port-spec>] [--output <file>] [--max-rate <rate>]}"
shift

PORTS="22,80,443,8006,8080,8443,9090,3000,5000"
OUTPUT_FILE=""
MAX_RATE="1000"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ports)   PORTS="$2";       shift 2 ;;
        --output)  OUTPUT_FILE="$2"; shift 2 ;;
        --max-rate) MAX_RATE="$2";   shift 2 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

command -v nmap >/dev/null 2>&1 || { echo "ERROR: nmap not installed. Run: apt install -y nmap" >&2; exit 1; }
command -v jq   >/dev/null 2>&1 || { echo "ERROR: jq not installed. Run: apt install -y jq" >&2; exit 1; }

TMPXML=$(mktemp /tmp/bp-scan-XXXXXX.xml)
trap 'rm -f "$TMPXML"' EXIT

echo "Scanning $TARGET ports=$PORTS max-rate=$MAX_RATE ..." >&2

nmap -sV -T4 --max-rate "$MAX_RATE" -p "$PORTS" -oX "$TMPXML" "$TARGET" >/dev/null 2>&1

# Determine scanner node id from env
SCANNER_NODE="${BLUEPRINTS_NODE_ID:-$(hostname)}"
SCAN_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Parse nmap XML → JSON using Python (more reliable than xsltproc)
RESULT=$(python3 - "$TMPXML" "$SCANNER_NODE" "$SCAN_TIME" <<'PYEOF'
import sys, xml.etree.ElementTree as ET, json

tree = ET.parse(sys.argv[1])
scanner_node = sys.argv[2]
scan_time = sys.argv[3]
root = tree.getroot()
results = []

for host in root.findall("host"):
    status = host.find("status")
    if status is not None and status.get("state") != "up":
        continue

    addr_el = host.find("address[@addrtype='ipv4']")
    ip = addr_el.get("addr") if addr_el is not None else "unknown"

    hostname = ""
    hn_el = host.find("hostnames/hostname")
    if hn_el is not None:
        hostname = hn_el.get("name", "")

    os_guess = ""
    os_el = host.find("os/osmatch")
    if os_el is not None:
        os_guess = os_el.get("name", "")

    ports = []
    for port in host.findall("ports/port"):
        state_el = port.find("state")
        service_el = port.find("service")
        ports.append({
            "port": int(port.get("portid", 0)),
            "proto": port.get("protocol", "tcp"),
            "state": state_el.get("state", "unknown") if state_el is not None else "unknown",
            "service": service_el.get("name", "") if service_el is not None else "",
            "banner": service_el.get("product", "") if service_el is not None else "",
        })

    # Only include hosts with at least one open port
    open_ports = [p for p in ports if p["state"] == "open"]
    if open_ports:
        results.append({
            "ip": ip,
            "hostname": hostname,
            "ports": open_ports,
            "os_guess": os_guess,
            "scan_time": scan_time,
            "scanner_node": scanner_node,
        })

print(json.dumps(results, indent=2))
PYEOF
)

echo "$RESULT"

if [[ -n "$OUTPUT_FILE" ]]; then
    echo "$RESULT" > "$OUTPUT_FILE"
    echo "Results written to $OUTPUT_FILE" >&2
fi

echo "Scan complete: $(echo "$RESULT" | jq length) hosts with open ports found." >&2
