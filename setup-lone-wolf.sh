#!/usr/bin/env bash
# setup-lone-wolf.sh — run after .env is loaded on any fleet node
# Manages .lone-wolf/.gitignore docs/syncthing entries and cron backup.
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
OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-docs-fix-owner.sh"
STACK_RUNTIME_OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-stack-runtime-fix-owner.sh"
CRON_MARKER="lone-wolf-docs-commit"
CRON_LINE="* * * * * root bash $COMMIT_SCRIPT"
OWNER_CRON_FILE="/etc/cron.d/lone-wolf-docs-owner"
OWNER_CRON_MARKER="lone-wolf-docs-fix-owner"
OWNER_CRON_LINE="* * * * * root bash $OWNER_FIX_SCRIPT"
STACK_RUNTIME_OWNER_CRON_FILE="/etc/cron.d/lone-wolf-stack-runtime-owner"
STACK_RUNTIME_OWNER_CRON_MARKER="lone-wolf-stack-runtime-fix-owner"
STACK_RUNTIME_OWNER_CRON_LINE="* * * * * root bash $STACK_RUNTIME_OWNER_FIX_SCRIPT"

commit_gitignore_change() {
    local message="$1"
    git -C "$LONE_WOLF" add .gitignore
    git -C "$LONE_WOLF" commit --only .gitignore -m "$message" || true
}

ensure_gitignore_line() {
    local line="$1"
    local message="$2"
    local label="$3"

    if ! grep -qxF "$line" "$GITIGNORE" 2>/dev/null; then
        printf '%s\n' "$line" >> "$GITIGNORE"
        commit_gitignore_change "$message"
        echo "  gitignore: added '$line' entry ($label)"
    else
        echo "  gitignore: '$line' already present — OK ($label)"
    fi
}

remove_gitignore_line() {
    local line="$1"
    local message="$2"
    local label="$3"

    if grep -qxF "$line" "$GITIGNORE" 2>/dev/null; then
        local tmp
        tmp="$(mktemp)"
        grep -vxF "$line" "$GITIGNORE" > "$tmp" || true
        mv "$tmp" "$GITIGNORE"
        commit_gitignore_change "$message"
        echo "  gitignore: removed '$line' entry ($label)"
    else
        echo "  gitignore: '$line' not present — OK ($label)"
    fi
}

remove_legacy_docs_issue_exceptions() {
    remove_gitignore_line 'docs/*' "Drop legacy partial docs ignore rule" "legacy docs cleanup"
    remove_gitignore_line '!docs/issues/' "Drop legacy docs/issues exception" "legacy docs cleanup"
    remove_gitignore_line '!docs/issues/**' "Drop legacy docs/issues exception" "legacy docs cleanup"
}

if ! grep -q "$OWNER_CRON_MARKER" "$OWNER_CRON_FILE" 2>/dev/null; then
    echo "# $OWNER_CRON_MARKER" > "$OWNER_CRON_FILE"
    echo "$OWNER_CRON_LINE" >> "$OWNER_CRON_FILE"
    chmod 644 "$OWNER_CRON_FILE"
    echo "  docs-owner-cron: installed — ownership normalization runs every minute"
else
    echo "  docs-owner-cron: already installed — OK"
fi

if ! grep -q "$STACK_RUNTIME_OWNER_CRON_MARKER" "$STACK_RUNTIME_OWNER_CRON_FILE" 2>/dev/null; then
    echo "# $STACK_RUNTIME_OWNER_CRON_MARKER" > "$STACK_RUNTIME_OWNER_CRON_FILE"
    echo "$STACK_RUNTIME_OWNER_CRON_LINE" >> "$STACK_RUNTIME_OWNER_CRON_FILE"
    chmod 644 "$STACK_RUNTIME_OWNER_CRON_FILE"
    echo "  stack-runtime-owner-cron: installed — runtime ownership guard runs every minute"
else
    echo "  stack-runtime-owner-cron: already installed — OK"
fi

if [[ "$DOCS_BACKUP" == "true" ]]; then
    # Backup node: docs must be tracked and syncthing is intentionally selectable.
    remove_gitignore_line 'docs' "Unignore docs — this is the designated backup node" "backup node"
    remove_gitignore_line 'syncthing/' "Unignore syncthing — this is the designated backup node" "backup node"
    remove_legacy_docs_issue_exceptions

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
    # Non-backup node: shared docs and syncthing payloads must not be committed here.
    ensure_gitignore_line 'docs' "Gitignore docs — distributed via Syncthing, not git-tracked here" "non-backup node"
    ensure_gitignore_line 'syncthing/' "Gitignore syncthing — distributed payloads are not git-tracked here" "non-backup node"
    remove_legacy_docs_issue_exceptions

    # Remove cron entry if present (non-backup node must not commit docs)
    if [[ -f /etc/cron.d/lone-wolf-docs ]]; then
        rm -f /etc/cron.d/lone-wolf-docs
        echo "  cron: removed lone-wolf-docs-commit (non-backup node)"
    else
        echo "  cron: not installed — OK (non-backup node)"
    fi
fi

echo "Done."
