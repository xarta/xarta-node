#!/usr/bin/env bash
# setup-xfce-xrdp.sh — install a lightweight desktop session for the xarta user via XRDP.
#
# What this script does (idempotent):
#   1. Installs XFCE, xrdp, and xorgxrdp.
#   2. Writes ~/.xsession for the xarta user to start XFCE.
#   3. Enables and starts xrdp.
#   4. Optionally marks XRDP as enabled in .env so setup-firewall.sh can open 3389.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

XARTA_USER="${XARTA_USER:-xarta}"
XARTA_HOME="${XARTA_HOME:-/home/$XARTA_USER}"
XARTA_ENABLE_XRDP="${XARTA_ENABLE_XRDP:-true}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

env_set() {
    local key="$1" value="$2"
    [[ -f "$ENV_FILE" ]] || return 0
    if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    else
        printf '\n%s=%s\n' "$key" "$value" >> "$ENV_FILE"
    fi
}

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

if ! id "$XARTA_USER" >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} user $XARTA_USER does not exist. Run setup-user-xarta.sh first." >&2
    exit 1
fi

echo "=== XFCE + XRDP setup ==="
echo ""

apt-get update
apt-get install -y xfce4 xfce4-goodies xrdp xorgxrdp dbus-x11

printf 'startxfce4\n' > "$XARTA_HOME/.xsession"
chown "$XARTA_USER:$XARTA_USER" "$XARTA_HOME/.xsession"
chmod 644 "$XARTA_HOME/.xsession"

if getent group ssl-cert >/dev/null 2>&1; then
    usermod -aG ssl-cert xrdp || true
fi

systemctl enable --now xrdp
systemctl is-active --quiet xrdp

if [[ "$XARTA_ENABLE_XRDP" == "true" ]]; then
    env_set XARTA_ENABLE_XRDP true
    echo -e "${GREEN}set${NC}: XARTA_ENABLE_XRDP=true in .env"
else
    echo -e "${CYAN}left unchanged${NC}: XARTA_ENABLE_XRDP"
fi

echo ""
echo -e "${GREEN}Done.${NC}"
echo -e "${YELLOW}Next step:${NC} re-run setup-firewall.sh if you want TCP 3389 opened."