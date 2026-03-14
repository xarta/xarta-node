#!/usr/bin/env bash

# bp-nodes-validate.sh
# Validates .nodes.json structure and consistency.
#
# Checks:
#   - Valid JSON syntax
#   - Top-level has "nodes" array (non-empty)
#   - BLUEPRINTS_NODE_ID from .env matches exactly one entry
#   - Every node has required fields: node_id, display_name, host_machine, active
#   - No duplicate node_id values
#   - Every active node has: primary_ip, primary_hostname, tailnet, tailnet_ip,
#     tailnet_hostname, sync_port
#   - sync_port is a valid integer for every active node
#   - No duplicate primary_ip values across active nodes
#   - No duplicate tailnet_ip values across active nodes
#
# Usage:
#   bash bp-nodes-validate.sh [/path/to/.nodes.json]
#
# Exit codes:
#   0 = valid
#   1 = invalid (human-readable errors printed to stderr)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Determine JSON path ───────────────────────────────────────────────────────
if [[ $# -ge 1 ]]; then
    NODES_JSON="$1"
elif [[ -f "$ENV_FILE" ]]; then
    # Read NODES_JSON_PATH from .env if present (grep exits 1 if absent — that's fine)
    NODES_JSON="$(grep -E '^NODES_JSON_PATH=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '"' | tr -d "'" || true)"
    : "${NODES_JSON:=$SCRIPT_DIR/.nodes.json}"
else
    NODES_JSON="$SCRIPT_DIR/.nodes.json"
fi

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ERRORS=0

fail() {
    echo -e "${RED}ERROR:${NC} $1" >&2
    ERRORS=$(( ERRORS + 1 ))
}

warn() {
    echo -e "${YELLOW}WARN:${NC} $1"
}

echo "=== bp-nodes-validate.sh ==="
echo "JSON: $NODES_JSON"
echo ""

# ── 1. File exists ────────────────────────────────────────────────────────────
if [[ ! -f "$NODES_JSON" ]]; then
    fail "File not found: $NODES_JSON"
    exit 1
fi

# ── 2. Valid JSON syntax ──────────────────────────────────────────────────────
if ! python3 -c "import json,sys; json.load(open(sys.argv[1]))" "$NODES_JSON" 2>/dev/null; then
    fail "Not valid JSON: $NODES_JSON"
    exit 1
fi
echo "  [OK] Valid JSON syntax"

# Use python3 for all subsequent checks (jq not guaranteed on all nodes)
python3 - "$NODES_JSON" "$ENV_FILE" <<'PYEOF'
import json
import os
import sys

nodes_path = sys.argv[1]
env_path   = sys.argv[2] if len(sys.argv) > 2 else ""

with open(nodes_path) as f:
    data = json.load(f)

errors = []
warnings = []

def fail(msg):
    errors.append(msg)

def warn(msg):
    warnings.append(msg)

# ── 3. Top-level "nodes" array ────────────────────────────────────────────────
if "nodes" not in data:
    fail("Top-level 'nodes' key missing")
    for e in errors:
        print(f"\033[0;31mERROR:\033[0m {e}", file=sys.stderr)
    sys.exit(1)

nodes = data["nodes"]
if not isinstance(nodes, list):
    fail("'nodes' must be an array")
    for e in errors:
        print(f"\033[0;31mERROR:\033[0m {e}", file=sys.stderr)
    sys.exit(1)

if len(nodes) == 0:
    fail("'nodes' array is empty")

print("  [OK] 'nodes' array present")

# ── 4. Required fields on every node ─────────────────────────────────────────
REQUIRED_ALL    = {"node_id", "display_name", "host_machine", "active"}
REQUIRED_ACTIVE = {"primary_ip", "primary_hostname", "tailnet",
                   "tailnet_ip", "tailnet_hostname", "sync_port"}

seen_ids        = {}
seen_primary_ip = {}
seen_tailnet_ip = {}

for idx, node in enumerate(nodes):
    nid = node.get("node_id", f"<node[{idx}]>")

    # Required fields for all nodes
    for field in REQUIRED_ALL:
        if field not in node:
            fail(f"Node '{nid}': missing required field '{field}'")

    # Active-only required fields
    if node.get("active", False):
        for field in REQUIRED_ACTIVE:
            if field not in node:
                fail(f"Active node '{nid}': missing required field '{field}'")

        # sync_port must be integer
        sp = node.get("sync_port")
        if sp is not None and not isinstance(sp, int):
            fail(f"Active node '{nid}': sync_port must be an integer, got {sp!r}")

        # Duplicate IPs
        pip = node.get("primary_ip")
        if pip:
            if pip in seen_primary_ip:
                fail(f"Duplicate primary_ip '{pip}' on nodes "
                     f"'{seen_primary_ip[pip]}' and '{nid}'")
            else:
                seen_primary_ip[pip] = nid

        tip = node.get("tailnet_ip")
        if tip:
            if tip in seen_tailnet_ip:
                fail(f"Duplicate tailnet_ip '{tip}' on nodes "
                     f"'{seen_tailnet_ip[tip]}' and '{nid}'")
            else:
                seen_tailnet_ip[tip] = nid

    # Duplicate node_id
    if nid in seen_ids:
        fail(f"Duplicate node_id '{nid}' at index {idx} and {seen_ids[nid]}")
    else:
        seen_ids[nid] = idx

if not errors:
    print("  [OK] All required fields present, no duplicate IDs or IPs")

# ── 5. BLUEPRINTS_NODE_ID must match exactly one entry ────────────────────────
node_id_from_env = None
if os.path.isfile(env_path):
    with open(env_path) as ef:
        for line in ef:
            line = line.strip()
            if line.startswith("BLUEPRINTS_NODE_ID="):
                node_id_from_env = line.split("=", 1)[1].strip().strip('"').strip("'")
                break

if node_id_from_env is None:
    warn("BLUEPRINTS_NODE_ID not found in .env — skipping self-node check")
else:
    matches = [n for n in nodes if n.get("node_id") == node_id_from_env]
    if len(matches) == 0:
        fail(f"BLUEPRINTS_NODE_ID='{node_id_from_env}' not found in nodes array")
    elif len(matches) > 1:
        fail(f"BLUEPRINTS_NODE_ID='{node_id_from_env}' matches {len(matches)} entries")
    else:
        print(f"  [OK] Self-node '{node_id_from_env}' found in nodes array")

# ── Output ────────────────────────────────────────────────────────────────────
for w in warnings:
    print(f"\033[1;33mWARN:\033[0m {w}")

for e in errors:
    print(f"\033[0;31mERROR:\033[0m {e}", file=sys.stderr)

if errors:
    sys.exit(1)
PYEOF

EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
    echo -e "${GREEN}Validation passed.${NC}"
else
    echo -e "${RED}Validation FAILED — see errors above.${NC}" >&2
    exit 1
fi
