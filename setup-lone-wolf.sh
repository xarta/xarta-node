#!/usr/bin/env bash
# setup-lone-wolf.sh — run after .env is loaded on any fleet node
# Manages .lone-wolf/.gitignore runtime/docs/syncthing/skills entries and
# independently role-gated docs and skills backup crons.
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
SKILLS_BACKUP="${THIS_NODE_SKILLS_BACKUP:-false}"

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
SKILLS_COMMIT_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-skills-commit.sh"
OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-docs-fix-owner.sh"
STACK_RUNTIME_OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/lone-wolf-stack-runtime-fix-owner.sh"
CRON_MARKER="lone-wolf-docs-commit"
CRON_LINE="* * * * * root bash $COMMIT_SCRIPT"
SKILLS_CRON_FILE="/etc/cron.d/lone-wolf-skills"
SKILLS_CRON_MARKER="lone-wolf-skills-commit"
SKILLS_CRON_LINE="* * * * * root bash $SKILLS_COMMIT_SCRIPT"
OWNER_CRON_FILE="/etc/cron.d/lone-wolf-docs-owner"
OWNER_CRON_MARKER="lone-wolf-docs-fix-owner"
OWNER_CRON_LINE="* * * * * root bash $OWNER_FIX_SCRIPT"
STACK_RUNTIME_OWNER_CRON_FILE="/etc/cron.d/lone-wolf-stack-runtime-owner"
STACK_RUNTIME_OWNER_CRON_MARKER="lone-wolf-stack-runtime-fix-owner"
STACK_RUNTIME_OWNER_CRON_LINE="* * * * * root bash $STACK_RUNTIME_OWNER_FIX_SCRIPT --check"
PUBLISH_HELPER="/root/xarta-node/.xarta/.agents/bin/xarta-lone-wolf-publish"
GIT_OWNER_HELPER="/root/xarta-node/.xarta/.agents/bin/xarta-lone-wolf-git-owner"

commit_gitignore_change() {
    local message="$1"
    if [[ "$DOCS_BACKUP" == "true" || "$SKILLS_BACKUP" == "true" ]]; then
        "$PUBLISH_HELPER" publish --message "$message" --path .gitignore
        return
    fi

    mapfile -t staged_paths < <("$GIT_OWNER_HELPER" -- diff --cached --name-only)
    for staged_path in "${staged_paths[@]}"; do
        if [[ "$staged_path" != ".gitignore" ]]; then
            echo "ERROR: staged p400 changes already exist outside .gitignore; refusing mixed commit" >&2
            return 1
        fi
    done
    "$GIT_OWNER_HELPER" -- config user.name xarta-node
    "$GIT_OWNER_HELPER" -- config user.email xarta-node@localhost
    "$GIT_OWNER_HELPER" -- add -- .gitignore
    "$GIT_OWNER_HELPER" -- commit -m "$message"
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
        chown --reference="$GITIGNORE" "$tmp"
        chmod --reference="$GITIGNORE" "$tmp"
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

ensure_runtime_gitignore_rules() {
    local rules=(
        '__pycache__/'
        '*.pyc'
        '.pytest_cache/'
        '.mypy_cache/'
        '.ruff_cache/'
        '.coverage'
        'htmlcov/'
    )
    local missing=()
    local line

    for line in "${rules[@]}"; do
        if ! grep -qxF "$line" "$GITIGNORE" 2>/dev/null; then
            missing+=("$line")
        fi
    done

    if (( ${#missing[@]} > 0 )); then
        {
            printf '\n# Generated Python test, type-check, lint, and coverage state\n'
            printf '%s\n' "${missing[@]}"
        } >> "$GITIGNORE"
        commit_gitignore_change "Ignore generated Python runtime and test state"
        echo "  gitignore: added ${#missing[@]} generated-runtime rule(s)"
    else
        echo "  gitignore: generated Python runtime rules already present — OK"
    fi
}

ensure_runtime_gitignore_rules

if ! grep -q "$OWNER_CRON_MARKER" "$OWNER_CRON_FILE" 2>/dev/null; then
    echo "# $OWNER_CRON_MARKER" > "$OWNER_CRON_FILE"
    echo "$OWNER_CRON_LINE" >> "$OWNER_CRON_FILE"
    chmod 644 "$OWNER_CRON_FILE"
    echo "  docs-owner-cron: installed — ownership normalization runs every minute"
else
    echo "  docs-owner-cron: already installed — OK"
fi

if [[ "$(cat "$STACK_RUNTIME_OWNER_CRON_FILE" 2>/dev/null || true)" != "# $STACK_RUNTIME_OWNER_CRON_MARKER
$STACK_RUNTIME_OWNER_CRON_LINE" ]]; then
    printf '# %s\n%s\n' "$STACK_RUNTIME_OWNER_CRON_MARKER" "$STACK_RUNTIME_OWNER_CRON_LINE" > "$STACK_RUNTIME_OWNER_CRON_FILE"
    chmod 644 "$STACK_RUNTIME_OWNER_CRON_FILE"
    echo "  stack-runtime-owner-cron: installed — bounded read-only sentinel check runs every minute"
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

if [[ "$SKILLS_BACKUP" == "true" ]]; then
    # Skills backup/publication authority is independent from docs authority.
    remove_gitignore_line 'skills' "Unignore skills — this is the designated skills backup node" "skills backup node"

    if [[ "$(cat "$SKILLS_CRON_FILE" 2>/dev/null || true)" != "# $SKILLS_CRON_MARKER
$SKILLS_CRON_LINE" ]]; then
        printf '# %s\n%s\n' "$SKILLS_CRON_MARKER" "$SKILLS_CRON_LINE" > "$SKILLS_CRON_FILE"
        chmod 644 "$SKILLS_CRON_FILE"
        echo "  skills-cron: installed lone-wolf-skills-commit (skills backup node)"
    else
        echo "  skills-cron: already installed — OK (skills backup node)"
    fi
else
    # Non-backup nodes may receive skills through Syncthing but cannot publish
    # that shared payload from their node-local lone-wolf repository.
    ensure_gitignore_line 'skills' "Gitignore skills — distributed via Syncthing, not git-tracked here" "non-skills-backup node"

    if [[ -f "$SKILLS_CRON_FILE" ]]; then
        rm -f "$SKILLS_CRON_FILE"
        echo "  skills-cron: removed lone-wolf-skills-commit (non-skills-backup node)"
    else
        echo "  skills-cron: not installed — OK (non-skills-backup node)"
    fi
fi

if [[ "$DOCS_BACKUP" != "true" && "$SKILLS_BACKUP" != "true" ]] &&
   { ! "$GIT_OWNER_HELPER" -- diff --quiet -- .gitignore ||
     ! "$GIT_OWNER_HELPER" -- diff --cached --quiet -- .gitignore; }; then
    commit_gitignore_change "Complete pending lone-wolf Git ignore policy"
fi

echo "Done."
