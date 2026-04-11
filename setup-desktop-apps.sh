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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

KEYRING_DIR="/etc/apt/keyrings"
MICROSOFT_KEYRING="$KEYRING_DIR/packages.microsoft.gpg"
EDGE_LIST="/etc/apt/sources.list.d/microsoft-edge.list"
CODE_LIST="/etc/apt/sources.list.d/vscode.list"
XARTA_USER="${XARTA_USER:-xarta}"
XARTA_HOME="${XARTA_HOME:-/home/$XARTA_USER}"
CERT_CA="${CERT_CA:-}"
CERT_CA_INTERMEDIATE="${CERT_CA_INTERMEDIATE:-}"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

import_ca_into_nss() {
    local ca_cert="$1"
    local user_name="$2"
    local user_home="$3"
    local nss_dir="$user_home/.pki/nssdb"
    local user_cert_dir="$user_home/.local/share/xarta-certs"
    local user_ca_cert="$user_cert_dir/$(basename "$ca_cert")"
    local nickname

    [[ -n "$ca_cert" && -f "$ca_cert" ]] || return 0
    id "$user_name" >/dev/null 2>&1 || return 0

    nickname="$(basename "${ca_cert%.crt}")"

    install -d -m 700 -o "$user_name" -g "$user_name" "$user_home/.pki"
    install -d -m 700 -o "$user_name" -g "$user_name" "$nss_dir"
    install -d -m 700 -o "$user_name" -g "$user_name" "$user_cert_dir"
    install -m 644 -o "$user_name" -g "$user_name" "$ca_cert" "$user_ca_cert"

    if ! runuser -u "$user_name" -- certutil -L -d "sql:$nss_dir" >/dev/null 2>&1; then
        runuser -u "$user_name" -- certutil -N -d "sql:$nss_dir" --empty-password
    fi

    runuser -u "$user_name" -- certutil -D -d "sql:$nss_dir" -n "$nickname" >/dev/null 2>&1 || true
    runuser -u "$user_name" -- certutil -A -d "sql:$nss_dir" -n "$nickname" -t "C,," -i "$user_ca_cert"
    echo -e "${CYAN}set${NC}: imported $(basename "$ca_cert") into $user_name NSS trust"
}

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

echo "=== Desktop applications setup ==="
echo ""

apt-get update
apt-get install -y ca-certificates curl gpg libnss3-tools

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

import_ca_into_nss "$CERT_CA" "$XARTA_USER" "$XARTA_HOME"
import_ca_into_nss "$CERT_CA_INTERMEDIATE" "$XARTA_USER" "$XARTA_HOME"

echo ""
echo -e "${GREEN}Done.${NC}"
echo "Installed packages:"
dpkg-query -W -f='  ${Package} ${Version}\n' microsoft-edge-stable code