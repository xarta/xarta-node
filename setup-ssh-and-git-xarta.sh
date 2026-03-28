#!/usr/bin/env bash
# setup-ssh-and-git-xarta.sh — configure SSH and git for the transitional xarta user.
#
# What this script does (idempotent):
#   1. Mirrors root's inbound authorized_keys onto the xarta user.
#   2. Ensures known_hosts exists for the xarta user.
#   3. Applies the same git identity used by the current root workflow.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

XARTA_USER="${XARTA_USER:-xarta}"
XARTA_HOME="${XARTA_HOME:-/home/$XARTA_USER}"
ROOT_SSH_DIR="/root/.ssh"
XARTA_SSH_DIR="$XARTA_HOME/.ssh"
GIT_USER_NAME="${GIT_USER_NAME:-$(git config --global user.name 2>/dev/null || true)}"
GIT_USER_EMAIL="${GIT_USER_EMAIL:-$(git config --global user.email 2>/dev/null || true)}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

if ! id "$XARTA_USER" >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} user $XARTA_USER does not exist. Run setup-user-xarta.sh first." >&2
    exit 1
fi

echo "=== xarta SSH and git setup ==="
echo ""

install -d -m 700 -o "$XARTA_USER" -g "$XARTA_USER" "$XARTA_SSH_DIR"

if [[ -f "$ROOT_SSH_DIR/authorized_keys" ]]; then
    install -m 600 -o "$XARTA_USER" -g "$XARTA_USER" "$ROOT_SSH_DIR/authorized_keys" "$XARTA_SSH_DIR/authorized_keys"
    echo -e "${GREEN}copied${NC}: authorized_keys from root"
else
    echo -e "${YELLOW}warning${NC}: root authorized_keys not found"
fi

if [[ -f "$ROOT_SSH_DIR/known_hosts" ]]; then
    install -m 644 -o "$XARTA_USER" -g "$XARTA_USER" "$ROOT_SSH_DIR/known_hosts" "$XARTA_SSH_DIR/known_hosts"
else
    touch "$XARTA_SSH_DIR/known_hosts"
    chown "$XARTA_USER:$XARTA_USER" "$XARTA_SSH_DIR/known_hosts"
    chmod 644 "$XARTA_SSH_DIR/known_hosts"
fi

if ! grep -q "github.com" "$XARTA_SSH_DIR/known_hosts" 2>/dev/null; then
    ssh-keyscan -t ed25519 github.com >> "$XARTA_SSH_DIR/known_hosts" 2>/dev/null || true
    chown "$XARTA_USER:$XARTA_USER" "$XARTA_SSH_DIR/known_hosts"
fi

if [[ -n "$GIT_USER_NAME" ]]; then
    runuser -u "$XARTA_USER" -- git config --global user.name "$GIT_USER_NAME"
fi
if [[ -n "$GIT_USER_EMAIL" ]]; then
    runuser -u "$XARTA_USER" -- git config --global user.email "$GIT_USER_EMAIL"
fi
echo -e "${GREEN}configured${NC}: git identity for $XARTA_USER"

echo ""
echo -e "${GREEN}Done.${NC}"