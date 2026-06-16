#!/usr/bin/env bash
# Recover the disposable local SeekDB index store used by Browser Links.
#
# SQLite is the canonical store for bookmarks and visits. SeekDB is a per-node
# search index, so a failed/corrupt SeekDB data root can be archived and rebuilt
# without losing canonical application data.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
if [[ -n "${XARTA_REPO_ROOT:-}" ]]; then
    REPO_ROOT="$XARTA_REPO_ROOT"
elif [[ -f "$DEFAULT_REPO_ROOT/.env" ]]; then
    REPO_ROOT="$DEFAULT_REPO_ROOT"
else
    REPO_ROOT="/root/xarta-node"
fi
ENV_FILE="$REPO_ROOT/.env"
VENV_DIR="/opt/blueprints/venv"
SEEKDB_CONFIG_FILE="/etc/seekdb/seekdb.cnf"
SEEKDB_DATA_ROOT="/var/lib/oceanbase"
ARCHIVE_ROOT="/var/lib/oceanbase-rebuilds"
AUTO_COOLDOWN_SECONDS="${XARTA_SEEKDB_RECOVER_COOLDOWN_SECONDS:-21600}"

MODE="check"
FROM_SYSTEMD=0
FORCE=0
TRIGGER_REINDEX=0
RESTART_BLUEPRINTS=1

usage() {
    cat <<'USAGE'
Usage:
  seekdb-index-recover.sh --check
  seekdb-index-recover.sh --recover [--force] [--from-systemd] [--trigger-reindex] [--no-blueprints-restart]

Archives and rebuilds /var/lib/oceanbase only. This is safe for Browser Links
because SQLite is canonical and SeekDB is a rebuildable per-node index.
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)
            MODE="check"
            ;;
        --recover)
            MODE="recover"
            ;;
        --force)
            FORCE=1
            ;;
        --from-systemd)
            FROM_SYSTEMD=1
            ;;
        --trigger-reindex)
            TRIGGER_REINDEX=1
            ;;
        --no-blueprints-restart)
            RESTART_BLUEPRINTS=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: must run as root" >&2
    exit 1
fi

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

SEEKDB_HOST="${SEEKDB_HOST:-127.0.0.1}"
SEEKDB_PORT="${SEEKDB_PORT:-2881}"
SEEKDB_DB="${SEEKDB_DB:-blueprints}"
SEEKDB_USER="${SEEKDB_USER:-root}"
SEEKDB_PASSWORD="${SEEKDB_PASSWORD:-}"
BP_PORT="${BLUEPRINTS_PORT:-8080}"

if [[ "$SEEKDB_HOST" != "127.0.0.1" && "$SEEKDB_HOST" != "localhost" ]]; then
    echo "ERROR: refusing to recover non-local SeekDB host: $SEEKDB_HOST" >&2
    exit 1
fi

if ! [[ "$SEEKDB_PORT" =~ ^[0-9]+$ ]]; then
    echo "ERROR: SEEKDB_PORT must be numeric; got '$SEEKDB_PORT'" >&2
    exit 1
fi

service_active() {
    systemctl is-active --quiet seekdb
}

port_listening() {
    ss -ltn "( sport = :$SEEKDB_PORT )" 2>/dev/null | grep -q ":$SEEKDB_PORT"
}

print_status() {
    local active failed port_state
    active="$(systemctl is-active seekdb 2>/dev/null || true)"
    failed="$(systemctl is-failed seekdb 2>/dev/null || true)"
    if port_listening; then
        port_state="listening"
    else
        port_state="closed"
    fi
    echo "seekdb_active=$active seekdb_failed=$failed port_$SEEKDB_PORT=$port_state data_root=$SEEKDB_DATA_ROOT"
}

if [[ "$MODE" == "check" ]]; then
    print_status
    if service_active && port_listening; then
        exit 0
    fi
    exit 1
fi

if [[ "$FORCE" != "1" ]] && service_active && port_listening; then
    echo "seekdb is already active and port $SEEKDB_PORT is listening; no recovery needed"
    exit 0
fi

mkdir -p "$ARCHIVE_ROOT"
marker="$ARCHIVE_ROOT/.last-auto-recovery"

if [[ "$FROM_SYSTEMD" == "1" && -f "$marker" ]]; then
    now="$(date -u +%s)"
    last="$(cat "$marker" 2>/dev/null || echo 0)"
    if [[ "$last" =~ ^[0-9]+$ ]] && (( now - last < AUTO_COOLDOWN_SECONDS )); then
        echo "automatic recovery cooldown active; last=$last now=$now cooldown=$AUTO_COOLDOWN_SECONDS"
        exit 0
    fi
fi

if [[ "$FROM_SYSTEMD" == "1" ]]; then
    date -u +%s > "$marker"
fi

echo "--- stopping seekdb before archive ..."
systemctl stop seekdb >/dev/null 2>&1 || true

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
archive_path="$ARCHIVE_ROOT/oceanbase-$stamp"
if [[ -e "$SEEKDB_DATA_ROOT" ]]; then
    echo "--- archiving $SEEKDB_DATA_ROOT to $archive_path ..."
    mv "$SEEKDB_DATA_ROOT" "$archive_path"
else
    echo "--- no existing $SEEKDB_DATA_ROOT found; creating a clean root ..."
fi
install -d -m 0755 -o root -g root "$SEEKDB_DATA_ROOT"

echo "--- resetting failed state and starting seekdb ..."
systemctl reset-failed seekdb >/dev/null 2>&1 || true
systemctl start seekdb

deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
    if service_active && port_listening; then
        break
    fi
    sleep 2
done

if ! service_active || ! port_listening; then
    echo "ERROR: seekdb did not become healthy after rebuild" >&2
    systemctl status seekdb --no-pager || true
    exit 1
fi

mysql_args=( -h127.0.0.1 -uroot -P"$SEEKDB_PORT" -A )
if [[ -n "$SEEKDB_PASSWORD" ]]; then
    mysql_args+=( "-p$SEEKDB_PASSWORD" )
fi

if command -v mysql >/dev/null 2>&1; then
    echo "--- ensuring SeekDB database exists ..."
    mysql "${mysql_args[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$SEEKDB_DB\`;"
else
    echo "WARNING: mysql client missing; skipping database creation check"
fi

if [[ -x "$VENV_DIR/bin/python" ]]; then
    echo "--- verifying pyseekdb connectivity ..."
    "$VENV_DIR/bin/python" - <<PY
import pyseekdb

client = pyseekdb.Client(
    host="$SEEKDB_HOST",
    port=int("$SEEKDB_PORT"),
    database="$SEEKDB_DB",
    user="$SEEKDB_USER",
    password="$SEEKDB_PASSWORD",
)
print("collections:", len(client.list_collections()))
PY
else
    echo "WARNING: missing $VENV_DIR/bin/python; skipping pyseekdb verification"
fi

if [[ "$RESTART_BLUEPRINTS" == "1" ]] && systemctl list-unit-files blueprints-app.service >/dev/null 2>&1; then
    if systemctl is-active --quiet blueprints-app; then
        echo "--- restarting blueprints-app to clear cached SeekDB clients ..."
        systemctl restart blueprints-app
    fi
fi

if [[ "$TRIGGER_REINDEX" == "1" ]]; then
    echo "--- triggering bookmark reindex ..."
    curl -fsS -X POST "http://127.0.0.1:${BP_PORT}/api/v1/bookmarks/reindex" || true
    echo
fi

echo "--- recovery complete ---"
print_status
echo "archive=${archive_path}"
