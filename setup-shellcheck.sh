#!/usr/bin/env bash
# setup-shellcheck.sh - install shellcheck on Debian-based xarta-node hosts.

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if [[ ${EUID} -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

if command -v shellcheck >/dev/null 2>&1; then
    echo -e "${GREEN}already installed:${NC} $(shellcheck --version | head -n 1)"
    exit 0
fi

echo "=== shellcheck setup ==="
apt-get update
apt-get install -y shellcheck

if ! command -v shellcheck >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} shellcheck installation failed" >&2
    exit 1
fi

echo -e "${GREEN}Done.${NC} $(shellcheck --version | head -n 1)"
