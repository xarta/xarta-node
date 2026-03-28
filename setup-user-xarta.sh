#!/usr/bin/env bash
# setup-user-xarta.sh — create or update the transitional xarta user.
#
# What this script does (idempotent):
#   1. Ensures sudo is installed.
#   2. Creates the xarta user with a home directory and bash shell if absent.
#   3. Ensures membership in sudo and docker.
#   4. Installs an explicit sudoers entry for the user.
#   5. Sets the password from XARTA_PASSWORD when provided by the operator.
#   6. Optionally runs setup-ssh-and-git-xarta.sh so the account is usable.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

XARTA_USER="${XARTA_USER:-xarta}"
XARTA_HOME="${XARTA_HOME:-/home/$XARTA_USER}"
XARTA_SHELL="${XARTA_SHELL:-/bin/bash}"
XARTA_PASSWORD="${XARTA_PASSWORD:-}"
XARTA_RUN_SSH_GIT_SETUP="${XARTA_RUN_SSH_GIT_SETUP:-true}"
SUDOERS_FILE="/etc/sudoers.d/90-${XARTA_USER}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

echo "=== xarta user setup ==="
echo ""

if ! command -v sudo >/dev/null 2>&1; then
    echo "Installing sudo..."
    apt-get update
    apt-get install -y sudo
fi

groupadd -f docker

if id "$XARTA_USER" >/dev/null 2>&1; then
    echo -e "${CYAN}exists${NC}: user $XARTA_USER"
    usermod -s "$XARTA_SHELL" "$XARTA_USER"
    usermod -aG sudo,docker "$XARTA_USER"
else
    useradd --create-home --home-dir "$XARTA_HOME" --shell "$XARTA_SHELL" --groups sudo,docker "$XARTA_USER"
    echo -e "${GREEN}created${NC}: user $XARTA_USER"
fi

install -d -m 755 -o "$XARTA_USER" -g "$XARTA_USER" "$XARTA_HOME"

cat > "$SUDOERS_FILE" <<EOF
$XARTA_USER ALL=(ALL:ALL) ALL
EOF
chmod 440 "$SUDOERS_FILE"
visudo -cf "$SUDOERS_FILE" >/dev/null
echo -e "${GREEN}configured${NC}: sudoers entry $SUDOERS_FILE"

if [[ -n "$XARTA_PASSWORD" ]]; then
    printf '%s:%s\n' "$XARTA_USER" "$XARTA_PASSWORD" | chpasswd
    echo -e "${GREEN}set${NC}: password for $XARTA_USER from environment"
else
    echo -e "${YELLOW}warning${NC}: XARTA_PASSWORD not set; password unchanged"
fi

if [[ "$XARTA_RUN_SSH_GIT_SETUP" == "true" && -f "$SCRIPT_DIR/setup-ssh-and-git-xarta.sh" ]]; then
    echo ""
    echo "Running setup-ssh-and-git-xarta.sh ..."
    bash "$SCRIPT_DIR/setup-ssh-and-git-xarta.sh"
fi

echo ""
echo -e "${GREEN}Done.${NC}"
echo "User: $XARTA_USER"
echo "Home: $XARTA_HOME"