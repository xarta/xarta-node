#!/usr/bin/env bash
# lone-wolf-docs-commit.sh
# Debounced git commit + push of docs/ in the lone-wolf repo.
#
# Run every minute via cron (installed by setup-lone-wolf.sh on backup node only).
# On each doc content save, routes_docs.py touches SENTINEL.
# This script commits only after SENTINEL has been quiet for DELAY seconds —
# any save within that window resets the countdown by re-touching SENTINEL.
#
# Cron entry (managed by setup-lone-wolf.sh):
#   * * * * * root bash /root/xarta-node/blueprints-app/scripts/lone-wolf-docs-commit.sh

set -euo pipefail

NODE_LOCAL_PARENT="/xarta-node"
LONE_WOLF="${NODE_LOCAL_PARENT}/.lone-wolf"
SENTINEL="${LONE_WOLF}/.docs-pending-commit"
OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-docs-fix-owner.sh"
PUBLISH_HELPER="/root/xarta-node/.xarta/.agents/bin/xarta-lone-wolf-publish"
DELAY=300  # seconds — 5 minutes

# The cron installation is role-gated too, but the writer itself fails closed if
# it is copied or invoked on a non-backup node.
# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true
[[ "${THIS_NODE_DOCS_BACKUP:-false}" == "true" ]] || exit 0

[[ -f "$SENTINEL" ]] || exit 0

if [[ -f "$OWNER_FIX_SCRIPT" ]]; then
    bash "$OWNER_FIX_SCRIPT"
fi

now=$(date +%s)
mtime=$(stat -c %Y "$SENTINEL" 2>/dev/null || echo "$now")
age=$(( now - mtime ))

if [[ $age -lt $DELAY ]]; then
    exit 0  # Still within debounce window — wait
fi

# Debounce window elapsed — publish only docs/ through the ownership boundary.
[[ -x "$PUBLISH_HELPER" ]] || {
    echo "ERROR: lone-wolf publication helper is unavailable" >&2
    exit 1
}

if [[ -z "$(runuser -u xarta -- git -C "$LONE_WOLF" status --porcelain --untracked-files=all -- docs/)" ]]; then
    # Nothing actually changed in docs/ (sentinel may be stale)
    rm -f "$SENTINEL"
    exit 0
fi

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"$PUBLISH_HELPER" \
    --node "$(hostname -s)" \
    publish \
    --message "auto: docs backup $TIMESTAMP" \
    --path docs
rm -f "$SENTINEL"
