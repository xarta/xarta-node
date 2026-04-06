#!/usr/bin/env bash
# setup-blueprints.sh — self-contained node bootstrap for xarta-node.
#
# Run this ONCE on a new node after cloning the repos:
#
#   git clone git@github.com:xarta/xarta-node.git
#   cd xarta-node
#   # Clone the private inner repo into .xarta/
#   git clone ... etc ...
#   cp .env.example .env         # then edit .env with this node's identity
#   bash setup-blueprints.sh
#
# Safe to re-run (idempotent).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
APP_DIR="$SCRIPT_DIR/blueprints-app"
VENV_DIR="/opt/blueprints/venv"
DATA_DIR="/opt/blueprints/data"
OPT_DIR="/opt/blueprints"

# ── Preflight ─────────────────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    echo "ERROR: setup.sh must be run as root (needs /opt/ write + systemctl)"
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

echo "=== xarta-node setup ==="
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
# DB lives in /opt/blueprints/data/db (not in git — persisted separately).
# GUI assets live in .xarta/gui/ (private inner repo) — no directory needed here.
echo "--- creating data directories..."
mkdir -p "$DATA_DIR/db"
echo "    ok"

# ── 1b. Link gui-embed/ into the GUI directory ───────────────────────────────
# gui-embed/ lives in the public outer repo. The app serves it at /ui/embed/
# via a symlink so there is only one copy of the source files.
echo "--- linking gui-embed into GUI directory..."
mkdir -p "$BLUEPRINTS_GUI_DIR"
chown_like "$(dirname "$BLUEPRINTS_GUI_DIR")" "$BLUEPRINTS_GUI_DIR"
# Remove any existing embed/ dir or stale symlink before (re-)creating it
rm -rf "$BLUEPRINTS_GUI_DIR/embed"
ln -s "$BLUEPRINTS_EMBED_DIR" "$BLUEPRINTS_GUI_DIR/embed"
chown_like "$BLUEPRINTS_GUI_DIR" "$BLUEPRINTS_GUI_DIR/embed"
echo "    ok: $BLUEPRINTS_GUI_DIR/embed -> $BLUEPRINTS_EMBED_DIR"

# ── 1bb. Link shared gui-db/ into the GUI directory ──────────────────────────
# gui-db/ lives in the public outer repo and is shared by /ui and /fallback-ui.
echo "--- linking gui-db into GUI directory..."
rm -rf "$BLUEPRINTS_GUI_DIR/db"
ln -s "$BLUEPRINTS_SHARED_DB_DIR" "$BLUEPRINTS_GUI_DIR/db"
chown_like "$BLUEPRINTS_GUI_DIR" "$BLUEPRINTS_GUI_DIR/db"
echo "    ok: $BLUEPRINTS_GUI_DIR/db -> $BLUEPRINTS_SHARED_DB_DIR"

# ── 1c. Link gui-embed/ into the gui-fallback directory ─────────────────────
# gui-fallback/ is the frozen public copy of the UI, served by Caddy at
# /fallback-ui.  It needs the same embed component symlink.
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
    # Assets dir is already the canonical subdirectory — no symlink needed.
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

# ── 3. Create venv ────────────────────────────────────────────────────────────
# Remove broken venv (created before python3-venv package was available)
if [[ -d "$VENV_DIR" && ! -f "$VENV_DIR/bin/pip" ]]; then
    echo "--- removing broken venv..."
    rm -rf "$VENV_DIR"
fi

if [[ ! -d "$VENV_DIR" ]]; then
    echo "--- creating venv at $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
fi

echo "--- installing Python requirements..."
"$VENV_DIR/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"
echo "    ok: $("$VENV_DIR/bin/pip" show fastapi uvicorn httpx 2>/dev/null \
    | grep -E '^Name:|^Version:' | paste - - | awk '{print $2"="$4}' | tr '\n' ' ')"

# ── 4. Install systemd service ────────────────────────────────────────────────
echo "--- installing systemd service..."
# Substitute the actual paths into the template so it works regardless of
# where xarta-node was cloned (not everyone clones to /root/xarta-node).
sed -e "s|/root/xarta-node/blueprints-app|$APP_DIR|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
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
    echo "=== setup complete ==="
    echo ""
    echo "Next steps:"
    echo "  1. Introduce this node to an existing peer:"
    echo "     curl -X POST http://<peer-ip>:8080/api/v1/nodes \\"
    echo "          -H 'Content-Type: application/json' \\"
    echo "          -d '{\"node_id\":\"<this-node-id>\", \"display_name\":\"<name>\", \"addresses\":[\"http://<this-ts-ip>:8080\"]}'"
    echo "  2. This node will boot-catchup from the peer and sync the full DB automatically."
else
    echo ""
    echo "WARNING: health check failed. Check: journalctl -u blueprints-app -n 50"
    exit 1
fi
