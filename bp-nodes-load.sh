#!/usr/bin/env bash

# bp-nodes-load.sh
# Reads .nodes.json and synchronises the nodes DB table from it.
#
# For each active node in the JSON:
#   - Upserts display_name, host_machine, tailnet, addresses (both primary and
#     tailnet sync addresses), and ui_url (https://{primary_hostname}).
#   - Sets last_seen to now.
#
# Nodes in the DB whose node_id is NOT present in the JSON are left unchanged
# (they may have been added by peer bootstrap). Nodes in the JSON with
# active=false are not upserted but are also not removed.
#
# Called by:
#   - Manually by the operator
#   - The Refresh button API endpoint (POST /api/v1/nodes/refresh)
#   - ExecStartPre= in the systemd service (future: Phase 2)
#
# Usage:
#   bash bp-nodes-load.sh [/path/to/.nodes.json]
#
# Exit codes:
#   0 = success
#   1 = failure (validation error, DB unreachable, etc.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

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

# ── Determine DB path ─────────────────────────────────────────────────────────
DB_PATH=""
if [[ -f "$ENV_FILE" ]]; then
    DB_DIR="$(grep -E '^BLUEPRINTS_DB_DIR=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^BLUEPRINTS_DB_DIR=//' | tr -d '"' | tr -d "'" || true)"
    if [[ -n "$DB_DIR" ]]; then
        DB_PATH="$DB_DIR/blueprints.db"
    fi
fi
: "${DB_PATH:=/opt/blueprints/data/db/blueprints.db}"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== bp-nodes-load.sh ==="
echo "JSON: $NODES_JSON"
echo "DB:   $DB_PATH"
echo ""

# ── 1. Validate JSON ──────────────────────────────────────────────────────────
echo "--- Validating JSON ---"
if ! bash "$SCRIPT_DIR/bp-nodes-validate.sh" "$NODES_JSON"; then
    echo -e "${RED}Aborting: JSON validation failed.${NC}" >&2
    exit 1
fi
echo ""

# ── 2. Check DB is accessible ────────────────────────────────────────────────
if [[ ! -f "$DB_PATH" ]]; then
    echo -e "${YELLOW}Warning:${NC} DB not found at $DB_PATH — skipping DB sync."
    echo "  (This is normal if the app has not been started yet.)"
    echo -e "${GREEN}Done (no DB to update).${NC}"
    exit 0
fi

# ── 3. Upsert active nodes into DB ───────────────────────────────────────────
echo "--- Syncing nodes to DB ---"
python3 - "$NODES_JSON" "$DB_PATH" <<'PYEOF'
import json
import sqlite3
import sys

nodes_path = sys.argv[1]
db_path    = sys.argv[2]

with open(nodes_path) as f:
    data = json.load(f)

nodes = data.get("nodes", [])
upserted = 0
skipped  = 0

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

for node in nodes:
    if not node.get("active", False):
        skipped += 1
        continue

    nid      = node["node_id"]
    name     = node["display_name"]
    host     = node["host_machine"]
    tailnet  = node.get("tailnet", "")
    pip      = node["primary_ip"]
    ph       = node["primary_hostname"]
    tip      = node["tailnet_ip"]
    port     = node["sync_port"]

    # addresses: both primary (VLAN) and tailnet sync addresses
    addresses = json.dumps([
        f"http://{pip}:{port}",
        f"http://{tip}:{port}",
    ])
    ui_url = f"https://{ph}"

    existing = conn.execute(
        "SELECT node_id FROM nodes WHERE node_id=?", (nid,)
    ).fetchone()

    if existing:
        conn.execute(
            "UPDATE nodes SET display_name=?, host_machine=?, tailnet=?, "
            "addresses=?, ui_url=?, last_seen=datetime('now') WHERE node_id=?",
            (name, host, tailnet, addresses, ui_url, nid),
        )
        print(f"  [UPDATED] {nid}  ({ui_url})")
    else:
        conn.execute(
            "INSERT INTO nodes (node_id, display_name, host_machine, tailnet, "
            "addresses, ui_url, last_seen) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
            (nid, name, host, tailnet, addresses, ui_url),
        )
        print(f"  [INSERTED] {nid}  ({ui_url})")

    upserted += 1

conn.commit()
conn.close()

print(f"\nProcessed: {upserted} active, {skipped} inactive (skipped).")
PYEOF

echo ""
echo -e "${GREEN}Done.${NC}"
