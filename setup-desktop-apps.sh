#!/usr/bin/env bash
# setup-desktop-apps.sh — install desktop user applications for XRDP-enabled nodes.
#
# What this script does (idempotent):
#   1. Installs Microsoft signing key into /etc/apt/keyrings.
#   2. Configures Microsoft Edge and VS Code apt repositories.
#   3. Installs microsoft-edge-stable and code.
#
# Notes:
#   - Intended for Debian 12 xarta-node LXCs with XFCE/XRDP already installed.
#   - Safe to re-run. Existing repo files and packages are updated in place.

set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export APT_LISTCHANGES_FRONTEND=none

KEYRING_DIR="/etc/apt/keyrings"
MICROSOFT_KEYRING="$KEYRING_DIR/packages.microsoft.gpg"
EDGE_LIST="/etc/apt/sources.list.d/microsoft-edge.list"
CODE_LIST="/etc/apt/sources.list.d/vscode.list"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

echo "=== Desktop applications setup ==="
echo ""

apt-get update
apt-get install -y ca-certificates curl gpg

install -d -m 755 "$KEYRING_DIR"
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | \
    gpg --dearmor -o "$MICROSOFT_KEYRING.tmp"
install -m 644 "$MICROSOFT_KEYRING.tmp" "$MICROSOFT_KEYRING"
rm -f "$MICROSOFT_KEYRING.tmp"
echo -e "${GREEN}configured${NC}: Microsoft apt keyring"

# Remove older Microsoft-managed repo definitions so apt does not reject
# the same URI with conflicting Signed-By values.
rm -f \
    /etc/apt/sources.list.d/vscode.sources \
    /etc/apt/sources.list.d/archive_uri-https_packages_microsoft_com_repos_code-*.list \
    /etc/apt/sources.list.d/archive_uri-https_packages_microsoft_com_repos_edge-*.list

cat > "$EDGE_LIST" <<EOF
deb [arch=amd64 signed-by=$MICROSOFT_KEYRING] https://packages.microsoft.com/repos/edge stable main
EOF
echo -e "${CYAN}set${NC}: $EDGE_LIST"

cat > "$CODE_LIST" <<EOF
deb [arch=amd64 signed-by=$MICROSOFT_KEYRING] https://packages.microsoft.com/repos/code stable main
EOF
echo -e "${CYAN}set${NC}: $CODE_LIST"

apt-get update
apt-get install -y microsoft-edge-stable code

dpkg --configure -a >/dev/null 2>&1 || true

echo ""
echo -e "${GREEN}Done.${NC}"
echo "Installed packages:"
dpkg-query -W -f='  ${Package} ${Version}\n' microsoft-edge-stable code