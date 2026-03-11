#!/usr/bin/env bash
# bp-scan-lock.sh — Manage the fleet-wide single-scanner lock.
#
# Usage:
#   bash bp-scan-lock.sh acquire      # Claim the lock for this node
#   bash bp-scan-lock.sh release      # Release the lock
#   bash bp-scan-lock.sh status       # Show current lock state
#
# The lock uses sync_meta keys via the Blueprints REST API.
# Lock TTL defaults to 15 minutes (configurable via SCAN_LOCK_TTL_MINUTES).

set -euo pipefail

ACTION="${1:?Usage: bp-scan-lock.sh acquire|release|status}"
API_BASE="${BLUEPRINTS_SELF_ADDRESS:-http://localhost:8080}"
NODE_ID="${BLUEPRINTS_NODE_ID:-$(hostname)}"
TTL_MINUTES="${SCAN_LOCK_TTL_MINUTES:-15}"

_get_meta() {
    local key="$1"
    curl -sf "${API_BASE}/api/v1/sync/status" 2>/dev/null | jq -r ".${key} // empty" 2>/dev/null || echo ""
}

_now_utc() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

_expiry_utc() {
    date -u -d "+${TTL_MINUTES} minutes" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
        || date -u -v+"${TTL_MINUTES}"M +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
        || python3 -c "
from datetime import datetime, timedelta, timezone
print((datetime.now(timezone.utc) + timedelta(minutes=${TTL_MINUTES})).strftime('%Y-%m-%dT%H:%M:%SZ'))
"
}

_is_expired() {
    local expires="$1"
    if [[ -z "$expires" ]]; then
        echo "yes"
        return
    fi
    python3 -c "
from datetime import datetime, timezone
exp = datetime.fromisoformat('${expires}'.replace('Z','+00:00'))
now = datetime.now(timezone.utc)
print('yes' if now >= exp else 'no')
"
}

# Read current lock state via a lightweight Python script that queries the DB directly
_read_lock() {
    python3 - <<'PYEOF'
import sqlite3, json, os
db_path = os.environ.get("BLUEPRINTS_DB_DIR", "/opt/blueprints/data/db") + "/blueprints.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
result = {}
for key in ("scanner_lock_holder", "scanner_lock_acquired", "scanner_lock_expires"):
    row = conn.execute("SELECT value FROM sync_meta WHERE key=?", (key,)).fetchone()
    result[key] = row[0] if row else ""
conn.close()
print(json.dumps(result))
PYEOF
}

# Write lock state via the DB + increment gen so it syncs to peers
_write_lock() {
    local holder="$1" acquired="$2" expires="$3"
    python3 - "$holder" "$acquired" "$expires" <<'PYEOF'
import sqlite3, os, sys
db_path = os.environ.get("BLUEPRINTS_DB_DIR", "/opt/blueprints/data/db") + "/blueprints.db"
holder, acquired, expires = sys.argv[1], sys.argv[2], sys.argv[3]
conn = sqlite3.connect(db_path)
for key, val in [("scanner_lock_holder", holder), ("scanner_lock_acquired", acquired), ("scanner_lock_expires", expires)]:
    conn.execute("INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)", (key, val))
# Bump gen so the lock state propagates to peers
conn.execute("UPDATE sync_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'gen'")
conn.commit()
conn.close()
PYEOF
}

case "$ACTION" in
    acquire)
        LOCK_JSON=$(_read_lock)
        HOLDER=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_holder // empty')
        EXPIRES=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_expires // empty')

        if [[ -n "$HOLDER" && "$HOLDER" != "$NODE_ID" ]]; then
            EXPIRED=$(_is_expired "$EXPIRES")
            if [[ "$EXPIRED" != "yes" ]]; then
                echo "LOCKED by $HOLDER until $EXPIRES — cannot acquire." >&2
                exit 1
            fi
            echo "Previous lock by $HOLDER expired at $EXPIRES — claiming." >&2
        fi

        NOW=$(_now_utc)
        EXP=$(_expiry_utc)
        _write_lock "$NODE_ID" "$NOW" "$EXP"
        echo "Scanner lock acquired by $NODE_ID until $EXP"
        ;;

    release)
        LOCK_JSON=$(_read_lock)
        HOLDER=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_holder // empty')

        if [[ "$HOLDER" != "$NODE_ID" && -n "$HOLDER" ]]; then
            echo "WARNING: Lock held by $HOLDER, not $NODE_ID. Releasing anyway." >&2
        fi

        _write_lock "" "" ""
        echo "Scanner lock released."
        ;;

    status)
        LOCK_JSON=$(_read_lock)
        HOLDER=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_holder // empty')
        ACQUIRED=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_acquired // empty')
        EXPIRES=$(echo "$LOCK_JSON" | jq -r '.scanner_lock_expires // empty')

        if [[ -z "$HOLDER" ]]; then
            echo "Scanner lock: FREE (no active scanner)"
        else
            EXPIRED=$(_is_expired "$EXPIRES")
            if [[ "$EXPIRED" == "yes" ]]; then
                echo "Scanner lock: EXPIRED (was held by $HOLDER, expired at $EXPIRES)"
            else
                echo "Scanner lock: HELD by $HOLDER since $ACQUIRED until $EXPIRES"
            fi
        fi
        ;;

    *)
        echo "Usage: bp-scan-lock.sh acquire|release|status" >&2
        exit 1
        ;;
esac
