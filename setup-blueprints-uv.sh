#!/usr/bin/env bash
# setup-blueprints-uv.sh — Alternative setup using uv instead of pip.
# 
# This is an alternative implementation to evaluate uv for dependency management.
# It is fully backward-compatible with the existing pip-based setup.
#
# Key differences from original:
#   - Uses `uv sync` instead of `pip install -r requirements.txt`
#   - Creates .venv in project directory (uv convention) instead of /opt/blueprints/venv
#   - Still requires uv to be installed (via setup-python-dev-tools.sh)
#   - Lock file (uv.lock) is committed to ensure reproducible installs
#
# Usage:
#   bash setup-blueprints-uv.sh
#
# Safety note: This is a test script. If you have the Proxmox backup, you can
# safely restore if something goes wrong.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
APP_DIR="$SCRIPT_DIR/blueprints-app"
# uv will create .venv in project root by default
VENV_DIR="$SCRIPT_DIR/.venv"
DATA_DIR="/opt/blueprints/data"
OPT_DIR="/opt/blueprints"

# ── Preflight ─────────────────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: setup.sh must be run as root (needs systemctl)"
    exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERROR: .env not found at $ENV_FILE"
    echo "Copy .env.example to .env and fill in this node's identity first."
    exit 1
fi

if [[ ! -d "$APP_DIR/app" ]]; then
    echo "ERROR: blueprints-app/app/ not found at $APP_DIR"
    echo "Make sure xarta-node is fully cloned and up to date (git pull)."
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: uv not found. Install with: bash setup-python-dev-tools.sh"
    exit 1
fi

if [[ ! -f "$SCRIPT_DIR/pyproject.toml" ]]; then
    echo "ERROR: pyproject.toml not found at $SCRIPT_DIR/pyproject.toml"
    echo "Run migration setup first."
    exit 1
fi

if [[ ! -f "$SCRIPT_DIR/uv.lock" ]]; then
    echo "ERROR: uv.lock not found at $SCRIPT_DIR/uv.lock"
    echo "Run 'uv lock' first."
    exit 1
fi

echo "=== xarta-node setup (uv-based) ==="
echo "Node dir : $SCRIPT_DIR"
echo "App dir  : $APP_DIR"
echo "Venv     : $VENV_DIR"
echo "Data     : $DATA_DIR"
echo ""

# Source .env to pick up BLUEPRINTS_GUI_DIR (needed for the embed symlink below)
# shellcheck source=.env
source "$ENV_FILE"
BLUEPRINTS_GUI_DIR="${BLUEPRINTS_GUI_DIR:-/data/gui}"
BLUEPRINTS_FALLBACK_GUI_DIR="${BLUEPRINTS_FALLBACK_GUI_DIR:-/xarta-node/gui-fallback}"
BLUEPRINTS_SHARED_DB_DIR="${BLUEPRINTS_SHARED_DB_DIR:-/xarta-node/gui-db}"
BLUEPRINTS_EMBED_DIR="${BLUEPRINTS_EMBED_DIR:-/xarta-node/gui-embed}"
BLUEPRINTS_ASSETS_DIR="${BLUEPRINTS_ASSETS_DIR:-/xarta-node/gui-fallback/assets}"

chown_like() {
    local ref_path="$1"
    local target_path="$2"
    local owner

    owner="$(stat -c '%u:%g' "$ref_path")"
    if [[ -L "$target_path" ]]; then
        chown -h "$owner" "$target_path"
    else
        chown "$owner" "$target_path"
    fi
}

echo "GUI dir  : $BLUEPRINTS_GUI_DIR"
echo "Fallback : $BLUEPRINTS_FALLBACK_GUI_DIR"
echo "Shared DB: $BLUEPRINTS_SHARED_DB_DIR"
echo "Embed dir: $BLUEPRINTS_EMBED_DIR"
echo "Assets   : $BLUEPRINTS_ASSETS_DIR"
echo ""

if [[ ! -d "$BLUEPRINTS_FALLBACK_GUI_DIR" ]]; then
    echo "ERROR: fallback GUI directory not found at $BLUEPRINTS_FALLBACK_GUI_DIR"
    exit 1
fi

if [[ ! -d "$BLUEPRINTS_SHARED_DB_DIR" ]]; then
    echo "ERROR: shared GUI DB directory not found at $BLUEPRINTS_SHARED_DB_DIR"
    exit 1
fi

if [[ ! -d "$BLUEPRINTS_EMBED_DIR" ]]; then
    echo "ERROR: embed directory not found at $BLUEPRINTS_EMBED_DIR"
    exit 1
fi

if [[ ! -d "$BLUEPRINTS_ASSETS_DIR" ]]; then
    echo "ERROR: assets directory not found at $BLUEPRINTS_ASSETS_DIR"
    exit 1
fi

# ── 1. Data directories ───────────────────────────────────────────────────────
echo "--- creating data directories..."
mkdir -p "$DATA_DIR/db"
echo "    ok"

# ── 1b. Link gui-embed/ into the GUI directory ───────────────────────────────
echo "--- linking gui-embed into GUI directory..."
mkdir -p "$BLUEPRINTS_GUI_DIR"
chown_like "$(dirname "$BLUEPRINTS_GUI_DIR")" "$BLUEPRINTS_GUI_DIR"
rm -rf "$BLUEPRINTS_GUI_DIR/embed"
ln -s "$BLUEPRINTS_EMBED_DIR" "$BLUEPRINTS_GUI_DIR/embed"
chown_like "$BLUEPRINTS_GUI_DIR" "$BLUEPRINTS_GUI_DIR/embed"
echo "    ok: $BLUEPRINTS_GUI_DIR/embed -> $BLUEPRINTS_EMBED_DIR"

# ── 1bb. Link shared gui-db/ into the GUI directory ──────────────────────────
echo "--- linking gui-db into GUI directory..."
rm -rf "$BLUEPRINTS_GUI_DIR/db"
ln -s "$BLUEPRINTS_SHARED_DB_DIR" "$BLUEPRINTS_GUI_DIR/db"
chown_like "$BLUEPRINTS_GUI_DIR" "$BLUEPRINTS_GUI_DIR/db"
echo "    ok: $BLUEPRINTS_GUI_DIR/db -> $BLUEPRINTS_SHARED_DB_DIR"

# ── 1c. Link gui-embed/ into the gui-fallback directory ─────────────────────
echo "--- linking gui-embed into gui-fallback directory..."
rm -rf "$BLUEPRINTS_FALLBACK_GUI_DIR/embed"
ln -s "$BLUEPRINTS_EMBED_DIR" "$BLUEPRINTS_FALLBACK_GUI_DIR/embed"
chown_like "$BLUEPRINTS_FALLBACK_GUI_DIR" "$BLUEPRINTS_FALLBACK_GUI_DIR/embed"
echo "    ok: $BLUEPRINTS_FALLBACK_GUI_DIR/embed -> $BLUEPRINTS_EMBED_DIR"

# Also expose shared db pages under /fallback-ui/db.
echo "--- linking gui-db into gui-fallback directory..."
rm -rf "$BLUEPRINTS_FALLBACK_GUI_DIR/db"
ln -s "$BLUEPRINTS_SHARED_DB_DIR" "$BLUEPRINTS_FALLBACK_GUI_DIR/db"
chown_like "$BLUEPRINTS_FALLBACK_GUI_DIR" "$BLUEPRINTS_FALLBACK_GUI_DIR/db"
echo "    ok: $BLUEPRINTS_FALLBACK_GUI_DIR/db -> $BLUEPRINTS_SHARED_DB_DIR"

# Also expose shared assets under /fallback-ui/assets.
echo "--- linking assets into gui-fallback directory..."
_ASSETS_LINK="$BLUEPRINTS_FALLBACK_GUI_DIR/assets"
if [[ "$BLUEPRINTS_ASSETS_DIR" == "$_ASSETS_LINK" ]]; then
    echo "    ok: $BLUEPRINTS_ASSETS_DIR is already at canonical location"
else
    rm -rf "$_ASSETS_LINK"
    ln -s "$BLUEPRINTS_ASSETS_DIR" "$_ASSETS_LINK"
    chown_like "$BLUEPRINTS_FALLBACK_GUI_DIR" "$_ASSETS_LINK"
    echo "    ok: $_ASSETS_LINK -> $BLUEPRINTS_ASSETS_DIR"
fi

# ── 2. Ensure Python 3.11 venv support is available ───────────────────────
echo "--- checking python3.11-venv..."
if ! dpkg -l python3.11-venv >/dev/null 2>&1; then
    echo "    installing python3.11-venv..."
    apt-get update 2>/dev/null || true
    apt-get install -y --no-install-recommends python3.11-venv python3-pip
fi
echo "    ok"

# ── 2b. Ensure network + DB tools are available ───────────────────────────
echo "--- checking network/db tools (sqlite3, arping, net-tools, p7zip-full, ripgrep)..."
TOOLS_NEEDED=()
dpkg -l sqlite3          >/dev/null 2>&1 || TOOLS_NEEDED+=(sqlite3)
dpkg -l iputils-arping   >/dev/null 2>&1 || TOOLS_NEEDED+=(iputils-arping)
dpkg -l net-tools        >/dev/null 2>&1 || TOOLS_NEEDED+=(net-tools)
dpkg -l p7zip-full       >/dev/null 2>&1 || TOOLS_NEEDED+=(p7zip-full)
dpkg -l ripgrep          >/dev/null 2>&1 || TOOLS_NEEDED+=(ripgrep)
if [[ ${#TOOLS_NEEDED[@]} -gt 0 ]]; then
    echo "    installing: ${TOOLS_NEEDED[*]}"
    apt-get install -y --no-install-recommends "${TOOLS_NEEDED[@]}"
fi
echo "    ok"

# ── 2c. Ensure Python dev tools are available ─────────────────────────────
echo "--- checking Python dev tools (uv, ruff)..."
if [[ -x "$SCRIPT_DIR/setup-python-dev-tools.sh" ]]; then
    "$SCRIPT_DIR/setup-python-dev-tools.sh"
else
    echo "    warning: setup-python-dev-tools.sh not found or not executable; skipping uv/ruff install"
fi
echo "    ok"

# ── 3. Use uv to create venv and install dependencies ──────────────────────
# uv will:
#  1. Create .venv in the project directory (if not already present)
#  2. Resolve dependencies from pyproject.toml
#  3. Use uv.lock for exact versions (must be committed)
#  4. Install all packages

echo "--- installing dependencies with uv..."
cd "$SCRIPT_DIR"
uv sync --frozen
cd - >/dev/null
echo "    ok: dependencies installed from uv.lock"

# ── 3b. Symlink venv to /opt/blueprints/venv for backward compatibility ────
#   (Optional: If you want to keep service configs pointing to /opt/blueprints/venv)
# echo "--- symlinking venv for backward compatibility..."
# mkdir -p "$OPT_DIR"
# rm -f /opt/blueprints/venv
# ln -s "$VENV_DIR" /opt/blueprints/venv
# echo "    ok: /opt/blueprints/venv -> $VENV_DIR"

# ── 4. Install systemd service ────────────────────────────────────────────────
echo "--- installing systemd service..."
# Substitute the actual paths into the template
# Note: Update ExecStart path to use project .venv instead of /opt/blueprints/venv
VENV_UVICORN="$VENV_DIR/bin/uvicorn"
sed -e "s|/root/xarta-node/blueprints-app|$APP_DIR|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
    -e "s|/opt/blueprints/venv/bin/uvicorn|$VENV_UVICORN|g" \
    "$APP_DIR/blueprints-app.service.template" \
    > /etc/systemd/system/blueprints-app.service
systemctl daemon-reload
echo "    ok"

# ── 5. Enable + start ─────────────────────────────────────────────────────────
echo "--- enabling + starting blueprints-app..."
systemctl enable blueprints-app
systemctl restart blueprints-app
sleep 3

# ── 6. Health check ───────────────────────────────────────────────────────────
echo ""
echo "--- health check..."
if curl -sf http://127.0.0.1:8080/health | python3 -m json.tool; then
    echo ""
    echo "=== setup complete (uv-based) ==="
    echo ""
    echo "Dependencies installed from uv.lock (reproducible across fleet)"
    echo "Venv location: $VENV_DIR"
    echo ""
    echo "Next steps:"
    echo "  1. Introduce this node to an existing peer (if new node):"
    echo "     curl -X POST http://<peer-ip>:8080/api/v1/nodes \\"
    echo "          -H 'Content-Type: application/json' \\"
    echo "          -d '{\"node_id\":\"<this-node-id>\", \"display_name\":\"<name>\", \"addresses\":[\"http://<this-ts-ip>:8080\"]}'"
    echo "  2. This node will boot-catchup from the peer and sync the full DB automatically."
else
    echo ""
    echo "WARNING: health check failed. Check: journalctl -u blueprints-app -n 50"
    exit 1
fi
