#!/usr/bin/env bash
set -euo pipefail

PUBLIC_REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SETUP_CADDY="${SETUP_CADDY:-$PUBLIC_REPO_ROOT/setup-caddy.sh}"
ACTIVE_CADDY="${XARTA_ACTIVE_CADDYFILE:-/xarta-node/.lone-wolf/Caddyfile}"
ENV_FILE="${XARTA_PUBLIC_ENV_FILE:-$PUBLIC_REPO_ROOT/.env}"

if [[ ! -f "$SETUP_CADDY" ]]; then
    echo "[caddy-route-parity] BLOCKED: setup-caddy.sh not found: $SETUP_CADDY" >&2
    exit 2
fi

if [[ ! -f "$ACTIVE_CADDY" ]]; then
    echo "[caddy-route-parity] WARN: active Caddyfile not found, skipping active-route parity: $ACTIVE_CADDY" >&2
    exit 0
fi

fail=0

env_value() {
    local key="$1"
    [[ -f "$ENV_FILE" ]] || return 0
    awk -F= -v key="$key" '
        $0 !~ /^[[:space:]]*#/ && $1 == key {
            value = substr($0, index($0, "=") + 1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
            gsub(/^["'\'']|["'\'']$/, "", value)
            print value
            exit
        }
    ' "$ENV_FILE"
}

url_host() {
    local value="$1"
    value="${value#*://}"
    value="${value%%/*}"
    value="${value%%:*}"
    printf '%s' "$value"
}

derived_child_hostname() {
    local prefix="$1"
    local ui_host
    ui_host="$(url_host "$(env_value BLUEPRINTS_UI_URL)")"
    [[ -n "$ui_host" ]] || return 0
    if [[ "$ui_host" == *.* ]]; then
        printf '%s.%s' "$prefix" "${ui_host#*.}"
    else
        printf '%s.%s' "$prefix" "$ui_host"
    fi
}

require_setup_token_for_active_route() {
    local setup_token="$1"
    local host="$2"
    local description="$3"

    [[ -n "$host" ]] || return 0

    if grep -qF "$host" "$ACTIVE_CADDY" && ! grep -qF "$setup_token" "$SETUP_CADDY"; then
        echo "[caddy-route-parity] BLOCKED: active Caddyfile contains the $description route from private env, but setup-caddy.sh lacks $setup_token." >&2
        fail=1
    fi
}

require_active_route_for_setup_token() {
    local setup_token="$1"
    local host="$2"
    local description="$3"

    [[ -n "$host" ]] || return 0

    if grep -qF "$setup_token" "$SETUP_CADDY" && ! grep -qF "$host" "$ACTIVE_CADDY"; then
        echo "[caddy-route-parity] BLOCKED: setup-caddy.sh contains $description ($setup_token), but active Caddyfile lacks the matching private-env route." >&2
        fail=1
    fi
}

check_route() {
    local setup_token="$1"
    local host="$2"
    local description="$3"

    require_setup_token_for_active_route "$setup_token" "$host" "$description"
    require_active_route_for_setup_token "$setup_token" "$host" "$description"
}

ntfy_host="$(env_value NTFY_HOSTNAME)"
if [[ -z "$ntfy_host" ]]; then
    ntfy_host="$(derived_child_hostname notify)"
fi

vikunja_host="$(env_value VIKUNJA_HOSTNAME)"
if [[ -z "$vikunja_host" ]]; then
    vikunja_host="$(derived_child_hostname projects)"
fi

check_route "MATRIX_SYNAPSE_HOSTNAME" "$(env_value MATRIX_SYNAPSE_HOSTNAME)" "local Matrix"
check_route "MATRIX_SHARED_HOSTNAME" "$(env_value MATRIX_SHARED_HOSTNAME)" "shared Matrix"
check_route "NTFY_HOSTNAME" "$ntfy_host" "ntfy UnifiedPush"
check_route "VIKUNJA_HOSTNAME" "$vikunja_host" "Vikunja"
check_route "HERMES_LOCAL_DASHBOARD_HOSTNAME" "$(env_value HERMES_LOCAL_DASHBOARD_HOSTNAME)" "Hermes Local dashboard"
check_route "HERMES_VPS_DASHBOARD_HOSTNAME" "$(env_value HERMES_VPS_DASHBOARD_HOSTNAME)" "Hermes VPS dashboard"

if [[ "$fail" -ne 0 ]]; then
    echo "[caddy-route-parity] Fix setup-caddy.sh and regenerate or update the active Caddyfile before committing." >&2
    exit 1
fi

echo "[caddy-route-parity] OK"
