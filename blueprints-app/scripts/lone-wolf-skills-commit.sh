#!/usr/bin/env bash
# Debounced, role-gated publication of the shared skills payload.
#
# Syncthing changes can arrive without an application save hook, so this writer
# fingerprints the bounded skills tree. A changed fingerprint resets the quiet
# window; an unchanged dirty tree is published after five minutes.

set -euo pipefail

NODE_LOCAL_PARENT="/xarta-node"
LONE_WOLF="${NODE_LOCAL_PARENT}/.lone-wolf"
SKILLS_ROOT="${LONE_WOLF}/skills"
STATE_DIR="/var/lib/xarta/lone-wolf-skills-commit"
PENDING_FILE="${STATE_DIR}/pending"
FINGERPRINT_FILE="${STATE_DIR}/fingerprint"
OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-docs-fix-owner.sh"
PUBLISH_HELPER="/root/xarta-node/.xarta/.agents/bin/xarta-lone-wolf-publish"
GIT_OWNER_HELPER="/root/xarta-node/.xarta/.agents/bin/xarta-lone-wolf-git-owner"
DELAY=300

# The cron installation is role-gated too. Fail closed if this writer is copied
# or invoked on a node that is not the skills backup authority.
# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true
[[ "${THIS_NODE_SKILLS_BACKUP:-false}" == "true" ]] || exit 0

if [[ -f "$OWNER_FIX_SCRIPT" ]]; then
    bash "$OWNER_FIX_SCRIPT"
fi

[[ -x "$PUBLISH_HELPER" && -x "$GIT_OWNER_HELPER" ]] || {
    echo "ERROR: lone-wolf publication boundary is unavailable" >&2
    exit 1
}

status="$("$GIT_OWNER_HELPER" -- status --porcelain=v1 --untracked-files=all -- skills/)"
if [[ -z "$status" ]]; then
    rm -f -- "$PENDING_FILE" "$FINGERPRINT_FILE"
    rmdir -- "$STATE_DIR" 2>/dev/null || true
    exit 0
fi

mkdir -p "$STATE_DIR"
chmod 700 "$STATE_DIR"

tree_fingerprint="$(
    {
        printf '%s\0' "$status"
        if [[ -d "$SKILLS_ROOT" ]]; then
            while IFS= read -r -d '' path; do
                relative="${path#"$SKILLS_ROOT"/}"
                printf '%s\0' "$relative"
                sha256sum -- "$path"
            done < <(find "$SKILLS_ROOT" -xdev -type f -print0 | sort -z)
        fi
    } | sha256sum | cut -d' ' -f1
)"

previous_fingerprint="$(cat "$FINGERPRINT_FILE" 2>/dev/null || true)"
if [[ "$tree_fingerprint" != "$previous_fingerprint" ]]; then
    printf '%s\n' "$tree_fingerprint" > "$FINGERPRINT_FILE"
    touch "$PENDING_FILE"
    exit 0
fi

[[ -f "$PENDING_FILE" ]] || {
    touch "$PENDING_FILE"
    exit 0
}

now="$(date +%s)"
mtime="$(stat -c %Y "$PENDING_FILE")"
age=$(( now - mtime ))
[[ "$age" -ge "$DELAY" ]] || exit 0

timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
"$PUBLISH_HELPER" \
    --node "$(hostname -s)" \
    publish \
    --message "auto: shared skills backup $timestamp" \
    --path skills
rm -f -- "$PENDING_FILE" "$FINGERPRINT_FILE"
rmdir -- "$STATE_DIR" 2>/dev/null || true
