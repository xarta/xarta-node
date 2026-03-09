#!/bin/bash

# auto-update.sh
# Pulls the latest changes from configured git repositories.
# Run directly for testing; the setup script deploys a baked-in copy
# to /usr/local/bin/ for use by cron.
#
# Each pull has a hard timeout so failures never block the LXC's
# primary Tailscale/gateway responsibilities.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    # shellcheck source=.env
    source "$SCRIPT_DIR/.env"
else
    echo "Error: .env not found at $SCRIPT_DIR/.env" >&2
    exit 1
fi

GIT_TIMEOUT="${GIT_TIMEOUT:-5}"
LOG_FILE="${AUTO_UPDATE_LOG}"

log() {
    echo "$(date): $1" | tee -a "$LOG_FILE"
}

pull_repo() {
    local name="$1"
    local path="$2"

    [ -z "$path" ] && return

    if [ ! -d "$path/.git" ]; then
        log "[$name] Not a git repo at $path — skipping."
        return
    fi

    local output exit_code
    output=$(timeout "$GIT_TIMEOUT" git -C "$path" pull --ff-only 2>&1)
    exit_code=$?

    if [ "$exit_code" -eq 124 ]; then
        log "[$name] Timed out after ${GIT_TIMEOUT}s — continuing."
    elif [ "$exit_code" -ne 0 ]; then
        log "[$name] Pull failed (exit $exit_code): $output — continuing."
    else
        log "[$name] $output"
    fi
}

log "=== Auto-update started ==="
pull_repo "outer-repo" "$REPO_OUTER_PATH"
pull_repo "inner-repo" "$REPO_INNER_PATH"
log "=== Auto-update complete ==="
