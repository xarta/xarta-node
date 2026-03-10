#!/bin/bash

# setup-ssh-and-git.sh
# Configures SSH and git for automated pulls on this host.
# Called from setup-lxc-failover.sh — can also be run standalone.
# All site-specific values are read from .env.
# Idempotent.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -f "$SCRIPT_DIR/.env" ]; then
    source "$SCRIPT_DIR/.env"
else
    echo "Error: .env not found at $SCRIPT_DIR/.env" >&2
    exit 1
fi

# Validate required vars
for var in SSH_KEY_NAME GIT_USER_NAME GIT_USER_EMAIL REPO_OUTER_PATH; do
    if [ -z "${!var}" ]; then
        echo "Error: Required variable $var is not set in .env" >&2
        exit 1
    fi
done

SSH_DIR="/root/.ssh"
SSH_KEY="$SSH_DIR/$SSH_KEY_NAME"
SSH_CONFIG="$SSH_DIR/config"
KNOWN_HOSTS="$SSH_DIR/known_hosts"

# SSH key must already exist (deploy it to this host first using ssh-install.sh)
if [ ! -f "$SSH_KEY" ]; then
    echo "Error: SSH key not found at $SSH_KEY"
    echo "Deploy the keypair to this host first, then re-run."
    exit 1
fi

mkdir -p "$SSH_DIR"
chmod 700 "$SSH_DIR"

# SSH config entry for github.com (idempotent — keyed on key filename)
if ! grep -q "$SSH_KEY_NAME" "$SSH_CONFIG" 2>/dev/null; then
    cat >> "$SSH_CONFIG" <<SSHCONF

Host github.com
    HostName github.com
    User git
    IdentityFile $SSH_KEY
    IdentitiesOnly yes
SSHCONF
    chmod 600 "$SSH_CONFIG"
    echo "SSH config updated."
else
    echo "SSH config already set — skipped."
fi

# GitHub host key in known_hosts (idempotent)
if ! grep -q "github.com" "$KNOWN_HOSTS" 2>/dev/null; then
    ssh-keyscan -t ed25519 github.com >> "$KNOWN_HOSTS" 2>/dev/null
    echo "GitHub host key added to known_hosts."
else
    echo "GitHub host key already present — skipped."
fi

# Git global identity (idempotent)
if [ -z "$(git config --global user.name 2>/dev/null)" ]; then
    git config --global user.name "$GIT_USER_NAME"
    echo "Git user.name set."
fi
if [ -z "$(git config --global user.email 2>/dev/null)" ]; then
    git config --global user.email "$GIT_USER_EMAIL"
    echo "Git user.email set."
fi

# Switch repo remotes from HTTPS to SSH (idempotent)
for repo_path in "$REPO_OUTER_PATH" "$REPO_INNER_PATH"; do
    [ -z "$repo_path" ] && continue
    [ -d "$repo_path/.git" ] || continue
    current_url=$(git -C "$repo_path" remote get-url origin 2>/dev/null || true)
    if [[ "$current_url" == https://github.com/* ]]; then
        ssh_url="git@github.com:${current_url#https://github.com/}"
        git -C "$repo_path" remote set-url origin "$ssh_url"
        echo "Switched $repo_path remote to SSH."
    else
        echo "$repo_path remote already SSH — skipped."
    fi
done

echo "SSH and git setup complete."
