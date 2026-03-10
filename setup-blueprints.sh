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

# ── 1. Data directories ───────────────────────────────────────────────────────
# DB lives in /opt/blueprints/data/db (not in git — persisted separately).
# GUI assets live in .xarta/gui/ (private inner repo) — no directory needed here.
echo "--- creating data directories..."
mkdir -p "$DATA_DIR/db"
echo "    ok"

# ── 2. Install env file ───────────────────────────────────────────────────────
echo "--- installing .env to $OPT_DIR/.env..."
cp "$ENV_FILE" "$OPT_DIR/.env"
echo "    ok"

# ── 3. Ensure Python 3.11 venv support is available ──────────────────────────
echo "--- checking python3.11-venv..."
if ! dpkg -l python3.11-venv >/dev/null 2>&1; then
    echo "    installing python3.11-venv..."
    apt-get install -y --no-install-recommends python3.11-venv python3-pip
fi
echo "    ok"

# ── 4. Create venv ────────────────────────────────────────────────────────────
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

# ── 5. Install systemd service ────────────────────────────────────────────────
echo "--- installing systemd service..."
# Substitute the actual clone path into the template so it works regardless of
# where xarta-node was cloned (not everyone clones to /root/xarta-node).
sed "s|/root/xarta-node/blueprints-app|$APP_DIR|g" \
    "$APP_DIR/blueprints-app.service.template" \
    > /etc/systemd/system/blueprints-app.service
systemctl daemon-reload
echo "    ok"

# ── 6. Enable + start ─────────────────────────────────────────────────────────
echo "--- enabling + starting blueprints-app..."
systemctl enable blueprints-app
systemctl restart blueprints-app
sleep 3

# ── 7. Health check ───────────────────────────────────────────────────────────
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
