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

SENTINEL="/xarta-node/.lone-wolf/.docs-pending-commit"
LONE_WOLF="/xarta-node/.lone-wolf"
DELAY=300  # seconds — 5 minutes

[[ -f "$SENTINEL" ]] || exit 0

now=$(date +%s)
mtime=$(stat -c %Y "$SENTINEL" 2>/dev/null || echo "$now")
age=$(( now - mtime ))

if [[ $age -lt $DELAY ]]; then
    exit 0  # Still within debounce window — wait
fi

# Debounce window elapsed — commit anything changed under docs/
cd "$LONE_WOLF"

if git diff --quiet docs/ && git diff --cached --quiet docs/; then
    # Nothing actually changed in docs/ (sentinel may be stale)
    rm -f "$SENTINEL"
    exit 0
fi

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
git add docs/
git commit -m "auto: docs backup $TIMESTAMP"
git push && rm -f "$SENTINEL"
