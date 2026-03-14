#!/usr/bin/env bash

# bp-nodes-hosts.sh
# Generates /etc/hosts entries for all active fleet nodes from .nodes.json.
#
# For each active node, two entries are written:
#   <primary_ip>   <primary_hostname>
#   <tailnet_ip>   <tailnet_hostname>
#
# Idempotent — safe to re-run. The managed block is bounded by sentinel
# comments and replaced on each run.
#
# This replaces setup-hosts.sh reading from fleet-hosts.conf.
# setup-hosts.sh still works for hosts-format entries if needed.
#
# Usage:
#   bash bp-nodes-hosts.sh [/path/to/.nodes.json]
#
# Exit codes:
#   0 = success
#   1 = failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
HOSTS_FILE="/etc/hosts"
SENTINEL_START="# --- BEGIN blueprints-fleet-hosts (managed by bp-nodes-hosts.sh) ---"
SENTINEL_END="# --- END blueprints-fleet-hosts ---"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== bp-nodes-hosts.sh ==="

# ── Determine JSON path ───────────────────────────────────────────────────────
if [[ $# -ge 1 ]]; then
    NODES_JSON="$1"
elif [[ -f "$ENV_FILE" ]]; then
    NODES_JSON="$(grep -E '^NODES_JSON_PATH=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '"' | tr -d "'" || true)"
    : "${NODES_JSON:=$SCRIPT_DIR/.nodes.json}"
else
    NODES_JSON="$SCRIPT_DIR/.nodes.json"
fi

echo "JSON: $NODES_JSON"
echo ""

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} This script must be run as root." >&2
    exit 1
fi

if [[ ! -f "$NODES_JSON" ]]; then
    echo -e "${RED}Error:${NC} $NODES_JSON not found." >&2
    exit 1
fi

# ── Build entries from JSON ───────────────────────────────────────────────────
mapfile -t ENTRIES < <(python3 - "$NODES_JSON" <<'PYEOF'
import json, sys

data = json.load(open(sys.argv[1]))
for n in data.get("nodes", []):
    if not n.get("active", True):
        continue
    primary_ip = n.get("primary_ip", "").strip()
    primary_hostname = n.get("primary_hostname", "").strip()
    tailnet_ip = n.get("tailnet_ip", "").strip()
    tailnet_hostname = n.get("tailnet_hostname", "").strip()
    if primary_ip and primary_hostname:
        print(f"{primary_ip}\t{primary_hostname}")
    if tailnet_ip and tailnet_hostname:
        print(f"{tailnet_ip}\t{tailnet_hostname}")
PYEOF
)

if [[ ${#ENTRIES[@]} -eq 0 ]]; then
    echo -e "${YELLOW}Warning:${NC} No active node entries found — nothing to write."
    exit 0
fi

echo "Entries from .nodes.json:"
for entry in "${ENTRIES[@]}"; do
    echo "    $entry"
done
echo ""

# ── Remove existing managed block (idempotent) ────────────────────────────────
# Also remove legacy block written by setup-hosts.sh (different sentinel marker)
for REMOVE_SENTINEL in \
    "# --- BEGIN blueprints-fleet-hosts (managed by bp-nodes-hosts.sh) ---" \
    "# --- BEGIN blueprints-fleet-hosts (managed by setup-hosts.sh) ---"; do
    END_SENTINEL="${REMOVE_SENTINEL/BEGIN/END}"
    if grep -qF "$REMOVE_SENTINEL" "$HOSTS_FILE" 2>/dev/null; then
        echo "Removing previous managed block from $HOSTS_FILE..."
        sed -i "/$( echo "$REMOVE_SENTINEL" | sed 's/[\/&]/\\&/g' )/,/$( echo "$END_SENTINEL" | sed 's/[\/&]/\\&/g' )/d" "$HOSTS_FILE"
    fi
done

# ── Append new managed block ──────────────────────────────────────────────────
echo "Writing managed block to $HOSTS_FILE..."
{
    echo ""
    echo "$SENTINEL_START"
    for entry in "${ENTRIES[@]}"; do
        echo "$entry"
    done
    echo "$SENTINEL_END"
} >> "$HOSTS_FILE"

echo -e "${GREEN}Done.${NC}"
echo ""

# ── Verify ────────────────────────────────────────────────────────────────────
echo "Verifying resolution:"
ALL_OK=true
for entry in "${ENTRIES[@]}"; do
    expected_ip=$(echo "$entry" | awk '{print $1}')
    hostname=$(echo "$entry" | awk '{print $2}')
    resolved=$(getent hosts "$hostname" 2>/dev/null | awk '{print $1}' || true)
    if [[ "$resolved" == "$expected_ip" ]]; then
        echo -e "    ${GREEN}OK${NC}: $hostname -> $resolved"
    else
        echo -e "    ${YELLOW}WARN${NC}: $hostname resolved to '${resolved:-<nothing>}' (expected $expected_ip)"
        ALL_OK=false
    fi
done

echo ""
if $ALL_OK; then
    echo -e "${GREEN}All hostnames resolve correctly.${NC}"
else
    echo -e "${YELLOW}Some entries did not resolve as expected — check /etc/hosts manually.${NC}"
fi
