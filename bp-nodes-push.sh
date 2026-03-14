#!/usr/bin/env bash

# bp-nodes-push.sh
# Distributes .nodes.json to all active peer nodes and triggers a DB reload
# on each via the /api/v1/nodes/refresh endpoint.
#
# Only pushes to nodes that are NOT this node (skips self).
# Only pushes to nodes with active=true in the JSON.
#
# Usage:
#   bash bp-nodes-push.sh [/path/to/.nodes.json]
#
# Environment:
#   XARTA_NODE_SSH_KEY  — override SSH key (default: /root/.ssh/id_ed25519_xarta_node)
#
# Exit codes:
#   0 = all pushes succeeded
#   1 = one or more pushes failed (continues, reports at end)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
SSH_KEY="${XARTA_NODE_SSH_KEY:-/root/.ssh/id_ed25519_xarta_node}"
SSH_OPTS=(-n -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes)

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== bp-nodes-push.sh ==="

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

# ── Validate first ────────────────────────────────────────────────────────────
VALIDATE_SH="$SCRIPT_DIR/bp-nodes-validate.sh"
if [[ -x "$VALIDATE_SH" ]]; then
    echo "--- Validating ---"
    bash "$VALIDATE_SH" "$NODES_JSON" || exit 1
    echo ""
fi

# ── Resolve self identity ─────────────────────────────────────────────────────
SELF_ID=""
if [[ -f "$ENV_FILE" ]]; then
    SELF_ID="$(grep -E '^BLUEPRINTS_NODE_ID=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^BLUEPRINTS_NODE_ID=//' | tr -d '"' | tr -d "'" || true)"
fi

LOCAL_IPS=" $(hostname -I) "

# ── Extract active peer targets from JSON ─────────────────────────────────────
# Outputs: "<primary_ip> <node_id> <sync_port>" per active peer
PEER_LIST="$(python3 - "$NODES_JSON" "$SELF_ID" <<'PYEOF'
import json, sys

path = sys.argv[1]
self_id = sys.argv[2] if len(sys.argv) > 2 else ""

data = json.load(open(path))
for n in data.get("nodes", []):
    if not n.get("active", True):
        continue
    if n.get("node_id") == self_id:
        continue
    print(n["primary_ip"], n["node_id"], n.get("sync_port", 8080))
PYEOF
)"

if [[ -z "$PEER_LIST" ]]; then
    echo -e "${YELLOW}No active peer nodes found to push to.${NC}"
    exit 0
fi

FAILS=0

while IFS=' ' read -r ip node_id sync_port; do
    if [[ "$LOCAL_IPS" == *" $ip "* ]]; then
        echo "--- (self: $ip $node_id — skipping) ---"
        continue
    fi

    echo "--- $node_id ($ip) ---"

    # 1. SCP the JSON to the remote node
    REMOTE_PATH="$(ssh "${SSH_OPTS[@]}" "root@$ip" \
        "grep -E '^NODES_JSON_PATH=' /root/xarta-node/.env 2>/dev/null \
         | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '\"' | tr -d \"'\" \
         || echo /root/xarta-node/.nodes.json" 2>/dev/null \
        || echo "/root/xarta-node/.nodes.json")"
    REMOTE_PATH="${REMOTE_PATH:-/root/xarta-node/.nodes.json}"

    echo "  → scp to root@$ip:$REMOTE_PATH"
    if scp -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
           -o BatchMode=yes -q "$NODES_JSON" "root@$ip:$REMOTE_PATH"; then
        echo "    [OK] file copied"
    else
        echo -e "    ${RED}[FAIL]${NC} scp failed for $node_id ($ip)"
        FAILS=$(( FAILS + 1 ))
        continue
    fi

    # 2. Trigger /nodes/refresh on the remote node
    echo "  → POST http://$ip:$sync_port/api/v1/nodes/refresh"
    RESPONSE="$(ssh "${SSH_OPTS[@]}" "root@$ip" \
        "curl -s -X POST http://localhost:$sync_port/api/v1/nodes/refresh" 2>/dev/null || echo "curl_failed")"

    if echo "$RESPONSE" | grep -q '"status".*"ok"'; then
        echo "    [OK] nodes refreshed on $node_id"
    else
        echo -e "    ${YELLOW}[WARN]${NC} refresh response: $RESPONSE"
    fi

done <<< "$PEER_LIST"

echo ""
if [[ $FAILS -eq 0 ]]; then
    echo -e "${GREEN}Done.${NC} All active peers updated."
else
    echo -e "${RED}Done with $FAILS failure(s).${NC} Check output above."
    exit 1
fi
