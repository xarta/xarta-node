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
#   BLUEPRINTS_DB_DIR  — directory containing blueprints.db (used for ssh_targets lookup)
#   SSH_KEY_NAME       — fallback key name in /root/.ssh/ (used only when IP not in ssh_targets)
#
# Exit codes:
#   0 = all pushes succeeded
#   1 = one or more pushes failed (continues, reports at end)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ── SSH key resolution ───────────────────────────────────────────────────────
# Primary: query ssh_targets table for each target IP.
# Fallback: SSH_KEY_NAME from .env (used only when IP absent or DB unavailable).
_DB_DIR="$(grep -E '^BLUEPRINTS_DB_DIR=' "$ENV_FILE" 2>/dev/null \
    | head -1 | sed 's/^BLUEPRINTS_DB_DIR=//' | tr -d '"' | tr -d "'" || true)"
DB="${_DB_DIR:-/opt/blueprints/data/db}/blueprints.db"

resolve_key() {
    local ip="$1"
    local key_env key_path
    if [[ -f "$DB" ]]; then
        key_env="$(sqlite3 "$DB" \
            "SELECT key_env_var FROM ssh_targets WHERE ip_address='$ip' LIMIT 1;" \
            2>/dev/null || true)"
        if [[ -n "$key_env" ]]; then
            key_path="$(grep -E "^${key_env}=" "$ENV_FILE" 2>/dev/null \
                | head -1 | sed "s/^${key_env}=//" | tr -d '"' | tr -d "'" || true)"
            if [[ -n "$key_path" && -f "$key_path" ]]; then
                echo "$key_path"
                return
            fi
        fi
    fi
    # Fallback: SSH_KEY_NAME from .env
    local key_name
    key_name="$(grep -E '^SSH_KEY_NAME=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^SSH_KEY_NAME=//' | tr -d '"' | tr -d "'" || true)"
    [[ -n "$key_name" ]] && echo "/root/.ssh/$key_name" || echo ""
}

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

    # Resolve SSH key for this specific IP
    SSH_KEY="$(resolve_key "$ip")"
    if [[ -z "$SSH_KEY" ]]; then
        echo -e "  ${RED}SKIP:${NC} no SSH key resolved for $node_id ($ip)"
        FAILS=$(( FAILS + 1 ))
        continue
    fi
    SSH_OPTS=(-n -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes)

    # 1. SCP the JSON to the remote node
    # Try to read the remote node's NODES_JSON_PATH from its .env; fall back
    # to using the same path as the local JSON file.
    REMOTE_PATH="$(ssh "${SSH_OPTS[@]}" "root@$ip" \
        "grep -E '^NODES_JSON_PATH=' /root/xarta-node/.env 2>/dev/null \
         | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '\"' | tr -d \"'\"" 2>/dev/null \
        || true)"
    REMOTE_PATH="${REMOTE_PATH:-$NODES_JSON}"

    echo "  → scp to root@$ip:$REMOTE_PATH"
    if scp -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
           -o BatchMode=yes -q "$NODES_JSON" "root@$ip:$REMOTE_PATH"; then
        echo "    [OK] file copied"
    else
        echo -e "    ${RED}[FAIL]${NC} scp failed for $node_id ($ip)"
        FAILS=$(( FAILS + 1 ))
        continue
    fi

    # 2. Trigger /nodes/refresh on the remote node via loopback (always plain
    #    HTTP on 8080 — loopback bypasses auth and is unaffected by sync_scheme).
    echo "  → POST http://$ip:8080/api/v1/nodes/refresh"
    RESPONSE="$(ssh "${SSH_OPTS[@]}" "root@$ip" \
        "curl -s -X POST http://localhost:8080/api/v1/nodes/refresh" 2>/dev/null || echo "curl_failed")"

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
