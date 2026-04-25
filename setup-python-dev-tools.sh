#!/usr/bin/env bash
# setup-python-dev-tools.sh — install uv and ruff on xarta-node LXCs.
#
# What this script does (idempotent):
#   1. Installs PyYAML for system Python validation scripts.
#   2. Installs uv  — fast Python package manager / project tool runner.
#   3. Installs ruff — Python linter and formatter.
#
# Both are installed to /usr/local/bin so they are available system-wide
# (root and all users). Safe to re-run — already-installed tools are skipped.
#
# Notes:
#   - Intended for Debian 12 xarta-node LXCs.
#   - Requires curl and python3-pip (installed as part of setup-blueprints.sh).
#   - python3-yaml is installed through apt so system Python can import yaml.
#   - uv is installed via the official astral.sh binary installer.
#   - ruff is installed via pip3 --break-system-packages (Debian 12 externally
#     managed environment requires this flag for system-wide pip installs).

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

echo "=== Python dev tools setup ==="
echo ""

# ── PyYAML for system Python ─────────────────────────────────────────────────

if python3 -c "import yaml" >/dev/null 2>&1; then
    echo -e "${GREEN}already installed:${NC} python3-yaml"
else
    echo "--- installing python3-yaml..."
    apt-get update
    apt-get install -y --no-install-recommends python3-yaml
    if ! python3 -c "import yaml" >/dev/null 2>&1; then
        echo -e "${RED}Error:${NC} python3-yaml installation failed" >&2
        exit 1
    fi
    echo -e "${GREEN}installed:${NC} python3-yaml"
fi

# ── uv ────────────────────────────────────────────────────────────────────────

if command -v uv >/dev/null 2>&1; then
    echo -e "${GREEN}already installed:${NC} $(uv --version)"
else
    echo "--- installing uv..."
    if ! command -v curl >/dev/null 2>&1; then
        apt-get update
        apt-get install -y --no-install-recommends curl
    fi
    curl -LsSf https://astral.sh/uv/install.sh \
        | UV_INSTALL_DIR=/usr/local/bin INSTALLER_NO_MODIFY_PATH=1 sh
    if ! command -v uv >/dev/null 2>&1; then
        echo -e "${RED}Error:${NC} uv installation failed" >&2
        exit 1
    fi
    echo -e "${GREEN}installed:${NC} $(uv --version)"
fi

# ── ruff ──────────────────────────────────────────────────────────────────────

if command -v ruff >/dev/null 2>&1; then
    echo -e "${GREEN}already installed:${NC} $(ruff --version)"
else
    echo "--- installing ruff..."
    if ! command -v pip3 >/dev/null 2>&1; then
        apt-get update
        apt-get install -y --no-install-recommends python3-pip
    fi
    pip3 install --break-system-packages --quiet ruff
    if ! command -v ruff >/dev/null 2>&1; then
        echo -e "${RED}Error:${NC} ruff installation failed" >&2
        exit 1
    fi
    echo -e "${GREEN}installed:${NC} $(ruff --version)"
fi

echo ""
echo -e "${GREEN}Done.${NC}"
