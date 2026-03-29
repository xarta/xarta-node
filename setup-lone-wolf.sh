#!/usr/bin/env bash
# setup-lone-wolf.sh — run after .env is loaded on any fleet node
# Manages .lone-wolf/.gitignore docs entry and the docs symlink
# based on THIS_NODE_DOCS_BACKUP from .env

set -euo pipefail

LONE_WOLF="/xarta-node/.lone-wolf"
GITIGNORE="$LONE_WOLF/.gitignore"
SYMLINK="/root/xarta-node/.xarta/docs"
LONE_WOLF_DOCS="$LONE_WOLF/docs"

# Load .env
# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true

DOCS_BACKUP="${THIS_NODE_DOCS_BACKUP:-false}"

echo "=== setup-lone-wolf.sh ==="

# --- Symlink ---
if [[ -L "$SYMLINK" ]]; then
    echo "  symlink: already exists → $(readlink "$SYMLINK")"
elif [[ -e "$SYMLINK" ]]; then
    echo "  ERROR: $SYMLINK exists but is not a symlink — manual intervention required"
    exit 1
else
    ln -s "$LONE_WOLF_DOCS" "$SYMLINK"
    echo "  symlink: created $SYMLINK → $LONE_WOLF_DOCS"
fi

# --- .gitignore ---
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
fi

echo "Done."
