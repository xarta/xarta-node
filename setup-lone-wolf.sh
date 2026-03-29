#!/usr/bin/env bash
# setup-lone-wolf.sh — run after .env is loaded on any fleet node
# Manages .lone-wolf/.gitignore docs entry and cron backup.
# DOCS_ROOT is set in .env; no symlink needed (Option B).

set -euo pipefail

NODE_LOCAL_PARENT="/xarta-node"
LONE_WOLF="${NODE_LOCAL_PARENT}/.lone-wolf"
GITIGNORE="$LONE_WOLF/.gitignore"
STALE_SYMLINK="/root/xarta-node/.xarta/docs"

# Load .env
# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true

DOCS_BACKUP="${THIS_NODE_DOCS_BACKUP:-false}"

echo "=== setup-lone-wolf.sh ==="

# --- Remove stale symlink (Option B cleanup) ---
if [[ -L "$STALE_SYMLINK" ]]; then
    rm -f "$STALE_SYMLINK"
    echo "  symlink: removed stale $STALE_SYMLINK (Option B: DOCS_ROOT used instead)"
elif [[ -e "$STALE_SYMLINK" ]]; then
    echo "  WARNING: $STALE_SYMLINK exists but is not a symlink — leaving untouched, review manually"
else
    echo "  symlink: not present — OK"
fi

# --- .gitignore ---
COMMIT_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-docs-commit.sh"
CRON_MARKER="lone-wolf-docs-commit"
CRON_LINE="* * * * * root bash $COMMIT_SCRIPT"

if [[ "$DOCS_BACKUP" == "true" ]]; then
    # Backup node: docs/ must NOT be gitignored
    if grep -qx 'docs' "$GITIGNORE" 2>/dev/null; then
        sed -i '/^docs$/d' "$GITIGNORE"
        git -C "$LONE_WOLF" add .gitignore
        git -C "$LONE_WOLF" commit -m "Unignore docs — this is the designated backup node" || true
        echo "  gitignore: removed 'docs' entry (backup node)"
    else
        echo "  gitignore: 'docs' not present — OK (backup node)"
    fi
    # Install cron entry if not already present
    if ! grep -q "$CRON_MARKER" /etc/cron.d/lone-wolf-docs 2>/dev/null; then
        echo "# $CRON_MARKER" > /etc/cron.d/lone-wolf-docs
        echo "$CRON_LINE" >> /etc/cron.d/lone-wolf-docs
        chmod 644 /etc/cron.d/lone-wolf-docs
        echo "  cron: installed lone-wolf-docs-commit (backup node)"
    else
        echo "  cron: already installed — OK (backup node)"
    fi
else
    # Non-backup node: docs must be gitignored
    if ! grep -qx 'docs' "$GITIGNORE" 2>/dev/null; then
        echo 'docs' >> "$GITIGNORE"
        git -C "$LONE_WOLF" add .gitignore
        git -C "$LONE_WOLF" commit -m "Gitignore docs — distributed via Syncthing, not git-tracked here" || true
        echo "  gitignore: added 'docs' entry (non-backup node)"
    else
        echo "  gitignore: 'docs' already present — OK (non-backup node)"
    fi
    # Remove cron entry if present (non-backup node must not commit docs)
    if [[ -f /etc/cron.d/lone-wolf-docs ]]; then
        rm -f /etc/cron.d/lone-wolf-docs
        echo "  cron: removed lone-wolf-docs-commit (non-backup node)"
    else
        echo "  cron: not installed — OK (non-backup node)"
    fi
fi

echo "Done."
