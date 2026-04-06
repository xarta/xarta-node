#!/usr/bin/env bash
# setup-github-cli.sh — install GitHub CLI (gh) on Debian-based xarta-node hosts.
#
# What this script does (idempotent):
#   1. Installs prerequisites.
#   2. Attempts apt install from distro repositories.
#   3. Falls back to GitHub CLI official apt repository if needed.
#   4. Verifies gh is installed.
#
# This script does NOT authenticate gh. Run:
#   gh auth login
# after install.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

if [[ ${EUID} -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

if command -v gh >/dev/null 2>&1; then
    echo -e "${GREEN}already installed:${NC} $(gh --version | head -n 1)"
    exit 0
fi

echo "=== GitHub CLI setup ==="

apt-get update
apt-get install -y ca-certificates curl gnupg

# First try distro package (fast path on many systems).
if apt-get install -y gh; then
    echo -e "${GREEN}installed:${NC} gh from distro repository"
else
    echo -e "${YELLOW}warning:${NC} distro package not available, falling back to official GitHub CLI apt repo"

    install -d -m 0755 /etc/apt/keyrings
    if [[ ! -f /etc/apt/keyrings/githubcli-archive-keyring.gpg ]]; then
        curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
            | dd of=/etc/apt/keyrings/githubcli-archive-keyring.gpg
        chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg
    fi

    if [[ ! -f /etc/apt/sources.list.d/github-cli.list ]]; then
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
            > /etc/apt/sources.list.d/github-cli.list
    fi

    apt-get update
    apt-get install -y gh
fi

if ! command -v gh >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} gh installation failed" >&2
    exit 1
fi

echo -e "${GREEN}Done.${NC} $(gh --version | head -n 1)"
echo "Next step: run 'gh auth login' as the operator account you will use for issue operations."
