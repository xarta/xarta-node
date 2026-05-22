#!/usr/bin/env bash
# setup-vps-ops-tools.sh - install everyday operator tools on Debian VPS hosts.
#
# This is deliberately smaller than a full xarta-node bootstrap. It is safe for
# external VPS hosts that run Dockge/Traefik/Headscale support services but do
# not run the Blueprints LXC failover stack.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [[ ${EUID} -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

WITH_DOCKER=false
for arg in "$@"; do
    case "$arg" in
        --with-docker)
            WITH_DOCKER=true
            ;;
        --help|-h)
            cat <<'EOF'
Usage: setup-vps-ops-tools.sh [--with-docker]

Installs common Debian VPS operations tools:
  ca-certificates curl dnsutils git iproute2 iputils-ping jq less lsof
  netcat-openbsd pv python3 python3-pip python3-venv ripgrep rsync tcpdump
  unzip zip

Optional:
  --with-docker   install Docker Engine using setup-docker.sh when present
EOF
            exit 0
            ;;
        *)
            echo -e "${RED}Error:${NC} unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

echo "=== VPS ops tools setup ==="

packages=(
    ca-certificates
    curl
    dnsutils
    git
    iproute2
    iputils-ping
    jq
    less
    lsof
    netcat-openbsd
    pv
    python3
    python3-pip
    python3-venv
    ripgrep
    rsync
    tcpdump
    unzip
    zip
)

missing=()
for package in "${packages[@]}"; do
    if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed'; then
        missing+=("$package")
    fi
done

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "installing: ${missing[*]}"
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
else
    echo -e "${GREEN}already installed:${NC} package set is present"
fi

if [[ "$WITH_DOCKER" == "true" ]]; then
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [[ -x "$script_dir/setup-docker.sh" ]]; then
        "$script_dir/setup-docker.sh"
    elif command -v docker >/dev/null 2>&1; then
        echo -e "${YELLOW}warning:${NC} setup-docker.sh unavailable, but docker already exists: $(docker --version)"
    else
        echo -e "${RED}Error:${NC} --with-docker requested but setup-docker.sh is unavailable" >&2
        exit 1
    fi
fi

for cmd in curl git jq pv python3 rg rsync tcpdump; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        echo -e "${RED}Error:${NC} missing command after install: $cmd" >&2
        exit 1
    fi
done

echo -e "${GREEN}Done.${NC}"
echo "rsync: $(rsync --version | head -n 1)"
echo "pv: $(pv --version | head -n 1)"
echo "rg: $(rg --version | head -n 1)"
echo "jq: $(jq --version)"
