#!/usr/bin/env bash
# Validate Syncthing assets owner-guard behavior end to end.
#
# What this script verifies:
# 1) A root-owned test file under assets/icons is auto-corrected to xarta:xarta.
# 2) Syncthing reports healthy xarta-icons status with no pending work/errors.
# 3) All configured peers report 100% completion for xarta-icons with file present.
# 4) Deleting the test file re-syncs cleanly to all peers (still 100% completion).
#
# Safe behavior:
# - Uses a unique temporary test file name.
# - Cleans up test file automatically via trap.
# - Never touches existing asset files.

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
ENV_FILE="/root/xarta-node/.env"
ICONS_DIR="/xarta-node/gui-fallback/assets/icons"
OWNER_GUARD_CRON_FILE="/etc/cron.d/syncthing-assets-owner"
OWNER_GUARD_SCRIPT="/root/xarta-node/blueprints-app/scripts/syncthing-assets-fix-owner.sh"
OWNER_TIMEOUT_SECONDS="${OWNER_TIMEOUT_SECONDS:-95}"
SCAN_SETTLE_SECONDS="${SCAN_SETTLE_SECONDS:-3}"

fail() {
    echo "[$SCRIPT_NAME] ERROR: $*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "required command missing: $1"
}

json_field() {
    local json_file="$1"
    local field="$2"
    python3 - "$json_file" "$field" <<'PY'
import json
import sys
path, field = sys.argv[1], sys.argv[2]
with open(path, 'r', encoding='utf-8') as f:
    data = json.load(f)
val = data.get(field)
if val is None:
    print("")
else:
    print(val)
PY
}

fetch_json() {
    local url="$1"
    local output_file="$2"
    curl -sf -H "X-API-Key: $SYNCTHING_API_KEY" "$url" > "$output_file"
}

scan_icons() {
    curl -sf -X POST -H "X-API-Key: $SYNCTHING_API_KEY" \
        "http://127.0.0.1:8384/rest/db/scan?folder=xarta-icons" >/dev/null
    sleep "$SCAN_SETTLE_SECONDS"
}

print_completions_and_assert_100() {
    local devices_json="$1"
    local failed=0

    while IFS=$'\t' read -r name completion; do
        [[ -n "$name" ]] || continue
        echo "  - $name completion=$completion"
        # Completion is numeric; require exact 100 to keep this strict.
        [[ "$completion" == "100" ]] || failed=1
    done < <(python3 - "$devices_json" "$SYNCTHING_API_KEY" <<'PY'
import json
import sys
import urllib.request

devices_path = sys.argv[1]
api_key = sys.argv[2]

with open(devices_path, 'r', encoding='utf-8') as f:
    devices = json.load(f)

for d in devices:
    name = d.get('name', '?')
    dev_id = d.get('deviceID', '')
    url = f'http://127.0.0.1:8384/rest/db/completion?folder=xarta-icons&device={dev_id}'
    req = urllib.request.Request(url, headers={'X-API-Key': api_key})
    data = json.load(urllib.request.urlopen(req))
    completion = data.get('completion', -1)
    print(f'{name}\t{completion}')
PY
)

    [[ "$failed" -eq 0 ]] || fail "one or more peers are below 100% completion"
}

if [[ $EUID -ne 0 ]]; then
    fail "must be run as root"
fi

require_cmd curl
require_cmd python3
require_cmd stat
require_cmd chown

[[ -f "$ENV_FILE" ]] || fail "missing $ENV_FILE"
# shellcheck disable=SC1090
source "$ENV_FILE"

[[ -n "${SYNCTHING_API_KEY:-}" ]] || fail "SYNCTHING_API_KEY is empty in $ENV_FILE"
[[ -d "$ICONS_DIR" ]] || fail "missing icons dir: $ICONS_DIR"
[[ -f "$OWNER_GUARD_SCRIPT" ]] || fail "missing owner-guard script: $OWNER_GUARD_SCRIPT"
[[ -f "$OWNER_GUARD_CRON_FILE" ]] || fail "missing owner-guard cron file: $OWNER_GUARD_CRON_FILE"

grep -q "syncthing-assets-fix-owner" "$OWNER_GUARD_CRON_FILE" \
    || fail "owner-guard cron marker missing in $OWNER_GUARD_CRON_FILE"

TEST_FILE="$ICONS_DIR/owner-guard-sync-test-$(date +%Y%m%d-%H%M%S).txt"
STATUS_JSON="$(mktemp /tmp/syncthing-icons-status-XXXXXX.json)"
DEVICES_JSON="$(mktemp /tmp/syncthing-devices-XXXXXX.json)"

cleanup() {
    rm -f "$TEST_FILE" "$STATUS_JSON" "$DEVICES_JSON"
    if [[ -n "${SYNCTHING_API_KEY:-}" ]]; then
        curl -sf -X POST -H "X-API-Key: $SYNCTHING_API_KEY" \
            "http://127.0.0.1:8384/rest/db/scan?folder=xarta-icons" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

echo "== Step 1: create root-owned test file =="
printf 'owner-guard validation %s\n' "$(date -Is)" > "$TEST_FILE"
chown root:root "$TEST_FILE"
chmod 0644 "$TEST_FILE"
stat -c '  created: %U:%G %a %n' "$TEST_FILE"

echo "== Step 2: wait for owner-guard auto-correction (timeout ${OWNER_TIMEOUT_SECONDS}s) =="
corrected=0
for i in $(seq 1 "$OWNER_TIMEOUT_SECONDS"); do
    owner="$(stat -c '%U:%G' "$TEST_FILE")"
    if [[ "$owner" == "xarta:xarta" ]]; then
        corrected=1
        echo "  corrected after ${i}s"
        break
    fi
    sleep 1
done
[[ "$corrected" -eq 1 ]] || fail "owner-guard did not correct file within timeout"
stat -c '  current: %U:%G %a %n' "$TEST_FILE"

echo "== Step 3: verify add-sync health and peer completion =="
scan_icons
fetch_json "http://127.0.0.1:8384/rest/db/status?folder=xarta-icons" "$STATUS_JSON"
state="$(json_field "$STATUS_JSON" state)"
errors="$(json_field "$STATUS_JSON" errors)"
pull_errors="$(json_field "$STATUS_JSON" pullErrors)"
need_files="$(json_field "$STATUS_JSON" needFiles)"
local_files="$(json_field "$STATUS_JSON" localFiles)"
global_files="$(json_field "$STATUS_JSON" globalFiles)"

echo "  status: state=$state errors=$errors pullErrors=$pull_errors needFiles=$need_files localFiles=$local_files globalFiles=$global_files"
[[ "$errors" == "0" ]] || fail "status errors is not zero"
[[ "$pull_errors" == "0" ]] || fail "status pullErrors is not zero"
[[ "$need_files" == "0" ]] || fail "status needFiles is not zero"

fetch_json "http://127.0.0.1:8384/rest/config/devices" "$DEVICES_JSON"
print_completions_and_assert_100 "$DEVICES_JSON"

echo "== Step 4: delete test file and verify cleanup sync =="
rm -f "$TEST_FILE"
scan_icons
fetch_json "http://127.0.0.1:8384/rest/db/status?folder=xarta-icons" "$STATUS_JSON"
state="$(json_field "$STATUS_JSON" state)"
errors="$(json_field "$STATUS_JSON" errors)"
pull_errors="$(json_field "$STATUS_JSON" pullErrors)"
need_files="$(json_field "$STATUS_JSON" needFiles)"
local_files="$(json_field "$STATUS_JSON" localFiles)"
global_files="$(json_field "$STATUS_JSON" globalFiles)"

echo "  status: state=$state errors=$errors pullErrors=$pull_errors needFiles=$need_files localFiles=$local_files globalFiles=$global_files"
[[ "$errors" == "0" ]] || fail "post-delete errors is not zero"
[[ "$pull_errors" == "0" ]] || fail "post-delete pullErrors is not zero"
[[ "$need_files" == "0" ]] || fail "post-delete needFiles is not zero"

print_completions_and_assert_100 "$DEVICES_JSON"

echo "[$SCRIPT_NAME] PASS: owner-guard and sync add/delete validation completed"
