#!/usr/bin/env bash

# bp-nodes-delete.sh
# Marks a node as inactive in .nodes.json (sets active=false) and reloads
# the nodes DB table via bp-nodes-load.sh.
#
# This is the correct way to remove a node from fleet syncing.  It does NOT
# delete the record from the JSON file — the node entry is preserved for
# history and can be re-activated by setting active=true.
#
# Called by:
#   - DELETE /api/v1/nodes/{id} API endpoint (routes_nodes.py)
#   - Manually by the operator: bash bp-nodes-delete.sh <node_id>
#
# Usage:
#   bash bp-nodes-delete.sh <node_id> [/path/to/.nodes.json]
#
# Exit codes:
#   0 = success
#   1 = failure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== bp-nodes-delete.sh ==="

# ── Args ──────────────────────────────────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    echo -e "${RED}Usage:${NC} bp-nodes-delete.sh <node_id> [/path/to/.nodes.json]" >&2
    exit 1
fi

NODE_ID="$1"

# ── Determine JSON path ───────────────────────────────────────────────────────
if [[ $# -ge 2 ]]; then
    NODES_JSON="$2"
elif [[ -f "$ENV_FILE" ]]; then
    NODES_JSON="$(grep -E '^NODES_JSON_PATH=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '"' | tr -d "'" || true)"
    : "${NODES_JSON:=$SCRIPT_DIR/.nodes.json}"
else
    NODES_JSON="$SCRIPT_DIR/.nodes.json"
fi

echo "JSON: $NODES_JSON"
echo "Node: $NODE_ID"
echo ""

# ── Self-check: refuse to deactivate self ────────────────────────────────────
SELF_ID=""
if [[ -f "$ENV_FILE" ]]; then
    SELF_ID="$(grep -E '^BLUEPRINTS_NODE_ID=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^BLUEPRINTS_NODE_ID=//' | tr -d '"' | tr -d "'" || true)"
fi

if [[ -n "$SELF_ID" && "$NODE_ID" == "$SELF_ID" ]]; then
    echo -e "${RED}ERROR:${NC} Cannot deactivate this node's own entry ($NODE_ID)." >&2
    exit 1
fi

# ── Sanity: file exists ───────────────────────────────────────────────────────
if [[ ! -f "$NODES_JSON" ]]; then
    echo -e "${RED}ERROR:${NC} Not found: $NODES_JSON" >&2
    exit 1
fi

# ── Perform the update with Python ───────────────────────────────────────────
python3 - "$NODES_JSON" "$NODE_ID" <<'PYEOF'
import json, sys

path = sys.argv[1]
target_id = sys.argv[2]

with open(path) as f:
    data = json.load(f)

found = False
for node in data.get("nodes", []):
    if node.get("node_id") == target_id:
        if not node.get("active", True):
            print(f"  Node '{target_id}' is already inactive — no change needed.")
        else:
            node["active"] = False
            print(f"  Set active=false for '{target_id}'.")
        found = True
        break

if not found:
    print(f"ERROR: node_id '{target_id}' not found in {path}", file=sys.stderr)
    sys.exit(1)

with open(path, "w") as f:
    json.dump(data, f, indent=2)
    f.write("\n")

print("  Wrote updated JSON.")
PYEOF

echo ""

# ── Reload the DB via bp-nodes-load.sh ───────────────────────────────────────
LOAD_SH="$SCRIPT_DIR/bp-nodes-load.sh"
if [[ -x "$LOAD_SH" ]]; then
    echo "Reloading nodes DB..."
    bash "$LOAD_SH" "$NODES_JSON"
else
    echo -e "${YELLOW}WARN:${NC} $LOAD_SH not found or not executable — skipping DB reload."
    echo "  Run 'bash bp-nodes-load.sh' manually to sync the DB."
fi

echo ""
echo -e "${GREEN}Done.${NC} Node '$NODE_ID' marked inactive."
echo "  To distribute this change: bash bp-nodes-push.sh"
