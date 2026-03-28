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
POLKIT_RULE_FILE="/etc/polkit-1/rules.d/49-${XARTA_USER}-colord.rules"
AUTOSTART_DIR="$XARTA_HOME/.config/autostart"

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
apt-get install -y xfce4 xfce4-goodies xrdp xorgxrdp dbus-x11 autocutsel xclip xsel

printf 'startxfce4\n' > "$XARTA_HOME/.xsession"
chown "$XARTA_USER:$XARTA_USER" "$XARTA_HOME/.xsession"
chmod 644 "$XARTA_HOME/.xsession"

install -d -m 755 "$AUTOSTART_DIR"
cat > "$AUTOSTART_DIR/autocutsel-clipboard.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Version=1.0
Name=Autocutsel Clipboard
Exec=autocutsel -fork
OnlyShowIn=XFCE;
X-GNOME-Autostart-enabled=true
EOF
cat > "$AUTOSTART_DIR/autocutsel-primary.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Version=1.0
Name=Autocutsel Primary
Exec=autocutsel -selection PRIMARY -fork
OnlyShowIn=XFCE;
X-GNOME-Autostart-enabled=true
EOF
chown -R "$XARTA_USER:$XARTA_USER" "$XARTA_HOME/.config"

# In this LXC setup, xrdp's Xorg backend works reliably on IPv4 localhost,
# while xrdp-sesman needs to accept the xrdp control connection on IPv4.
# Keep the Xorg session target on 127.0.0.1 and let sesman listen broadly;
# the host firewall does not expose TCP 3350 externally.
if [[ -f /etc/xrdp/xrdp.ini ]]; then
    sed -i 's/^ip=::1$/ip=127.0.0.1/' /etc/xrdp/xrdp.ini
fi

if [[ -f /etc/xrdp/sesman.ini ]]; then
    sed -i 's/^ListenAddress=127\.0\.0\.1$/ListenAddress=0.0.0.0/' /etc/xrdp/sesman.ini
    sed -i 's/^ListenAddress=::1$/ListenAddress=0.0.0.0/' /etc/xrdp/sesman.ini
fi

install -d -m 755 /etc/polkit-1/rules.d
cat > "$POLKIT_RULE_FILE" <<EOF
polkit.addRule(function(action, subject) {
    if (subject.user === "$XARTA_USER" &&
        action.id.indexOf("org.freedesktop.color-manager.") === 0) {
        return polkit.Result.YES;
    }
});
EOF
chmod 644 "$POLKIT_RULE_FILE"

if getent group ssl-cert >/dev/null 2>&1; then
    usermod -aG ssl-cert xrdp || true
fi

systemctl enable --now xrdp-sesman xrdp
systemctl restart xrdp-sesman xrdp
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