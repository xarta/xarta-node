#!/bin/bash

# setup-hosts.sh
# Installs fleet hostnames into /etc/hosts from .xarta/fleet-hosts.conf.
#
# NOTE: On nodes that have .nodes.json, prefer bp-nodes-hosts.sh instead.
#   bp-nodes-hosts.sh reads from .nodes.json (the single source of truth)
#   and writes primary + tailnet entries for every active node automatically.
#   This script remains available for nodes without .nodes.json, or to manage
#   hosts-format custom entries that are not in .nodes.json.
#
# Reads:
#   .xarta/fleet-hosts.conf  — IP/hostname pairs; belongs in the PRIVATE repo.
#                              Never commit this file to the public repo.
#                              Format: standard hosts-file lines (IP hostname).
#                              Blank lines and comments (#) are ignored.
#
# Idempotent — safe to re-run. The block written to /etc/hosts is bounded by
# sentinel comments so it is cleanly replaced on each run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONF_FILE="$SCRIPT_DIR/.xarta/fleet-hosts.conf"
HOSTS_FILE="/etc/hosts"
SENTINEL_START="# --- BEGIN blueprints-fleet-hosts (managed by setup-hosts.sh) ---"
SENTINEL_END="# --- END blueprints-fleet-hosts ---"

# ── Colours ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=== setup-hosts.sh ==="
echo ""

# ── Preflight ─────────────────────────────────────────────────────────────────
if [[ ! -f "$CONF_FILE" ]]; then
    echo -e "${RED}Error:${NC} $CONF_FILE not found." >&2
    echo "  This file lives in the private repo (.xarta/) — ensure it has been cloned." >&2
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} This script must be run as root." >&2
    exit 1
fi

# ── Read conf (strip blank lines and comments) ────────────────────────────────
mapfile -t ENTRIES < <(grep -v '^\s*#' "$CONF_FILE" | grep -v '^\s*$')

if [[ ${#ENTRIES[@]} -eq 0 ]]; then
    echo -e "${YELLOW}Warning:${NC} fleet-hosts.conf has no entries — nothing to write."
    exit 0
fi

echo "Entries from fleet-hosts.conf:"
for entry in "${ENTRIES[@]}"; do
    echo "    $entry"
done
echo ""

# ── Remove existing managed block (idempotent) ────────────────────────────────
if grep -qF "$SENTINEL_START" "$HOSTS_FILE" 2>/dev/null; then
    echo "Removing previous managed block from $HOSTS_FILE..."
    # Delete from sentinel start to sentinel end (inclusive)
    sed -i "/$( echo "$SENTINEL_START" | sed 's/[\/&]/\\&/g' )/,/$( echo "$SENTINEL_END" | sed 's/[\/&]/\\&/g' )/d" "$HOSTS_FILE"
fi

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
