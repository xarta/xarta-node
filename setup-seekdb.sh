#!/usr/bin/env bash
# setup-seekdb.sh — install and verify SeekDB server mode via package manager + systemd.
# Idempotent and safe to re-run.
# This script uses the package-manager/systemd path only.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
VENV_DIR="/opt/blueprints/venv"
SEEKDB_CONFIG_FILE="/etc/seekdb/seekdb.cnf"
SEEKDB_DATA_ROOT="/var/lib/oceanbase"
OCEANBASE_KEY_URL="http://mirrors.oceanbase.com/oceanbase/oceanbase_deb.pub"
OCEANBASE_KEYRING="/usr/share/keyrings/oceanbase-archive-keyring.gpg"
OCEANBASE_LIST_FILE="/etc/apt/sources.list.d/oceanbase.list"
MIN_DATA_FREE_GB=5
RECOMMENDED_DATA_FREE_GB=15
ALLOW_LOW_DISK="${SEEKDB_ALLOW_LOW_DISK:-0}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env not found at $ENV_FILE"
    exit 1
fi

# shellcheck source=/dev/null
source "$ENV_FILE"

for req in SEEKDB_HOST SEEKDB_PORT SEEKDB_DB SEEKDB_USER SEEKDB_PASSWORD; do
    if [[ ! -v $req ]]; then
        echo "ERROR: $req is not set in .env"
        exit 1
    fi
done

if [[ -z "$SEEKDB_HOST" || -z "$SEEKDB_PORT" || -z "$SEEKDB_DB" || -z "$SEEKDB_USER" ]]; then
    echo "ERROR: SEEKDB_HOST/SEEKDB_PORT/SEEKDB_DB/SEEKDB_USER must be non-empty in .env"
    exit 1
fi

if ! [[ "$SEEKDB_PORT" =~ ^[0-9]+$ ]]; then
    echo "ERROR: SEEKDB_PORT must be numeric in .env"
    exit 1
fi

if [[ "$SEEKDB_HOST" != "127.0.0.1" && "$SEEKDB_HOST" != "localhost" ]]; then
    echo "ERROR: this workspace only supports local SeekDB service binding; set SEEKDB_HOST=127.0.0.1"
    exit 1
fi

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: setup-seekdb.sh must be run as root"
    exit 1
fi

if [[ ! -x "$VENV_DIR/bin/pip" ]]; then
    echo "ERROR: Python venv not found at $VENV_DIR"
    echo "Run setup-blueprints.sh first."
    exit 1
fi

upsert_cfg_key() {
    local key="$1"
    local value="$2"

    mkdir -p "$(dirname "$SEEKDB_CONFIG_FILE")"
    touch "$SEEKDB_CONFIG_FILE"

    if grep -q "^${key}=" "$SEEKDB_CONFIG_FILE"; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$SEEKDB_CONFIG_FILE"
    else
        printf '%s=%s\n' "$key" "$value" >> "$SEEKDB_CONFIG_FILE"
    fi
}

oceanbase_repo_line() {
    local distro codename arch
    # Prefer /etc/os-release so we do not depend on lsb-release before apt is usable.
    # shellcheck source=/dev/null
    source /etc/os-release
    distro="${ID}"
    codename="${VERSION_CODENAME}"
    arch="$(dpkg --print-architecture)"
    printf 'deb [signed-by=%s] http://mirrors.aliyun.com/oceanbase/community/stable/%s/%s/%s/ ./\n' \
        "$OCEANBASE_KEYRING" "$distro" "$codename" "$arch"
}

ensure_oceanbase_repo() {
    if ! command -v curl >/dev/null 2>&1 || ! command -v gpg >/dev/null 2>&1; then
        echo "    skip: curl/gpg not yet available to refresh OceanBase repo key"
        return 0
    fi

    install -d -m 0755 /usr/share/keyrings
    install -d -m 0755 /etc/apt/sources.list.d
    curl -fsSL "$OCEANBASE_KEY_URL" | gpg --dearmor > "$OCEANBASE_KEYRING"
    chmod 0644 "$OCEANBASE_KEYRING"
    oceanbase_repo_line > "$OCEANBASE_LIST_FILE"
}

echo "=== SeekDB setup (package-manager/systemd) ==="

if [[ -f "$OCEANBASE_LIST_FILE" ]] || dpkg -s seekdb >/dev/null 2>&1; then
    echo "--- refreshing OceanBase apt repo signing config ..."
    ensure_oceanbase_repo
fi

echo "--- ensuring prerequisites (curl, jq, mysql client, gpg, ca-certificates) ..."
apt-get update -qq
apt-get install -y --no-install-recommends curl jq default-mysql-client gpg ca-certificates >/dev/null

echo "--- upgrading pyseekdb in $VENV_DIR ..."
"$VENV_DIR/bin/pip" install --upgrade pyseekdb >/dev/null

echo "--- checking data-disk headroom against official docs ..."
mkdir -p "$SEEKDB_DATA_ROOT"
avail_kb=$(df -Pk "$SEEKDB_DATA_ROOT" | awk 'NR==2 {print $4}')
avail_gb=$(( avail_kb / 1024 / 1024 ))
if (( avail_gb < MIN_DATA_FREE_GB )); then
    if [[ "$ALLOW_LOW_DISK" == "1" ]]; then
        echo "WARNING: available disk under $SEEKDB_DATA_ROOT is ${avail_gb}G, below the ${MIN_DATA_FREE_GB}G personal-use minimum guidance"
        echo "         proceeding because SEEKDB_ALLOW_LOW_DISK=1 was set explicitly"
    else
        echo "ERROR: SeekDB personal-use disk planning guidance suggests at least ${MIN_DATA_FREE_GB}G available; found ${avail_gb}G under $SEEKDB_DATA_ROOT"
        echo "Resize or mount a larger data disk before installing SeekDB server mode."
        echo "If you intentionally want to proceed anyway on this node, rerun with SEEKDB_ALLOW_LOW_DISK=1."
        exit 1
    fi
fi

if (( avail_gb < RECOMMENDED_DATA_FREE_GB )); then
    echo "WARNING: SeekDB systemd docs attach a >${RECOMMENDED_DATA_FREE_GB}G recommendation to the default data path under $SEEKDB_DATA_ROOT; found ${avail_gb}G"
    echo "         For personal use this is not treated as a hard block, but headroom may become tight as data/log files grow."
fi

echo "--- ensuring seekdb package is installed ..."
if ! dpkg -s seekdb >/dev/null 2>&1; then
    echo "    seekdb not found — refreshing OceanBase apt repo and installing ..."
    ensure_oceanbase_repo
    apt-get update -qq
    apt-get install -y seekdb
fi
echo "    seekdb package present: $(dpkg -s seekdb | awk '/^Version:/{print $2}')"

echo "--- aligning $SEEKDB_CONFIG_FILE with local policy ..."
upsert_cfg_key "port" "$SEEKDB_PORT"
upsert_cfg_key "data-dir" "$SEEKDB_DATA_ROOT/store"
upsert_cfg_key "redo-dir" "$SEEKDB_DATA_ROOT/store/redo"

echo "--- enabling + restarting seekdb ..."
systemctl enable seekdb >/dev/null 2>&1 || true
systemctl restart seekdb
sleep 3

if ! systemctl is-active --quiet seekdb; then
    echo "ERROR: seekdb service failed to start"
    systemctl status seekdb --no-pager || true
    exit 1
fi

echo "--- ensuring database exists ..."
mysql_args=( -h127.0.0.1 -uroot -P"$SEEKDB_PORT" -A )
if [[ -n "$SEEKDB_PASSWORD" ]]; then
    mysql_args+=( "-p$SEEKDB_PASSWORD" )
fi
mysql "${mysql_args[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$SEEKDB_DB\`;"

echo "--- verifying pyseekdb client connectivity ..."
"$VENV_DIR/bin/python" - <<PY
import pyseekdb

client = pyseekdb.Client(
    host="$SEEKDB_HOST",
    port=int("$SEEKDB_PORT"),
    database="$SEEKDB_DB",
    user="$SEEKDB_USER",
    password="$SEEKDB_PASSWORD",
)
collections = client.list_collections()
print("pyseekdb:", getattr(pyseekdb, "__version__", "unknown"))
print("collections:", len(collections))
PY

echo "--- seekdb server reachable on $SEEKDB_HOST:$SEEKDB_PORT"
echo "    config file: $SEEKDB_CONFIG_FILE"
echo "    data root  : $SEEKDB_DATA_ROOT"

echo "--- checking blueprints-app /api/v1/bookmarks/health ..."
BP_PORT="${BLUEPRINTS_PORT:-8080}"
if systemctl is-active --quiet blueprints-app 2>/dev/null; then
    BM_HEALTH="$(curl -sf "http://127.0.0.1:${BP_PORT}/api/v1/bookmarks/health" 2>/dev/null || true)"
    if [[ -z "$BM_HEALTH" ]]; then
        echo "    WARNING: blueprints-app is running but /api/v1/bookmarks/health returned nothing"
    else
        echo "$BM_HEALTH" | python3 -m json.tool 2>/dev/null || echo "$BM_HEALTH"
        # Fail loudly if seekdb or embedding is broken
        if echo "$BM_HEALTH" | python3 -c "
import sys, json
d = json.load(sys.stdin)
bad = [k for k in ('sqlite','seekdb','embedding') if d.get(k) != 'ok']
if bad:
    print('FAIL: ' + ', '.join(bad) + ' not ok', file=sys.stderr)
    sys.exit(1)
" 2>&1; then
            echo "    sqlite, seekdb, embedding all ok"
        else
            echo "    WARNING: one or more subsystems not ok (see above)"
        fi
    fi
else
    echo "    blueprints-app not running — skipping endpoint check"
fi

echo "=== setup-seekdb complete ==="
