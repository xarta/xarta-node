#!/usr/bin/env bash
# setup-syncthing.sh — install and configure Syncthing for xarta-node fleet
#
# What this script does (idempotent):
#   1. Installs Syncthing from the official apt repository + python3-bcrypt.
#   2. Ensures managed share paths exist with Syncthing .stfolder markers:
#      gui-fallback/assets/icons/, sounds/, fonts/, .lone-wolf/docs/,
#      and .lone-wolf/syncthing/tts/voices/.
#   3. Generates a stable SYNCTHING_API_KEY in .env (if not already set).
#   4. Temporarily starts syncthing@xarta.service so Syncthing generates its
#      device certificate, then reads the device ID from the local REST API.
#   5. Generates config.xml from .nodes.json + .env: GUI credentials, peer
#      device IDs (via primary_ip), shared folder paths, discovery disabled.
#   6. Writes SYNCTHING_DEVICE_ID to .env.
#   7. Restarts syncthing@xarta.service with the new config.
#
# Two-pass rollout for the fleet:
#   Pass 1 (first run on a node):
#     → Script prints this node's Syncthing device ID.
#     → Operator adds it to .nodes.json (syncthing_device_id for this node_id).
#     → Run: bash bp-nodes-push.sh
#     → Re-run: bash setup-syncthing.sh  (pass 2)
#   Pass 2 (after all nodes have device IDs in .nodes.json):
#     → Full peer mesh is configured. Peers with empty device IDs are skipped.
#
# After this script: re-run setup-caddy.sh to expose the Syncthing GUI at
#   https://$SYNCTHING_HOSTNAME (block is generated when SYNCTHING_HOSTNAME
#   is set in .env).
#
# Required .env vars before running:
#   BLUEPRINTS_NODE_ID      — e.g. my-node-1
#   REPO_OUTER_PATH         — e.g. /root/xarta-node
#   SYNCTHING_HOSTNAME      — e.g. sync.<your-domain>
#   SYNCTHING_GUI_USER      — e.g. admin (defaults to admin if not set)
#   SYNCTHING_GUI_PASSWORD  — set a strong password before running
#   CERT_FILE, CERT_KEY     — fleet TLS cert paths (used by setup-caddy.sh)
#
# Safe to re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
NODES_JSON="$SCRIPT_DIR/.nodes.json"
SYNCTHING_HOME="/home/xarta/.local/state/syncthing"
ASSETS_OWNER_FIX_SCRIPT="/root/xarta-node/blueprints-app/scripts/syncthing-assets-fix-owner.sh"
ASSETS_OWNER_CRON_FILE="/etc/cron.d/syncthing-assets-owner"
ASSETS_OWNER_CRON_MARKER="syncthing-assets-fix-owner"
ASSETS_OWNER_CRON_LINE="* * * * * root bash $ASSETS_OWNER_FIX_SCRIPT"

# ── Colours ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# ── Preflight ─────────────────────────────────────────────────────────────────
if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}Error:${NC} .env not found at $ENV_FILE" >&2
    exit 1
fi

if [[ ! -f "$NODES_JSON" ]]; then
    echo -e "${RED}Error:${NC} .nodes.json not found — run bp-nodes-push.sh first." >&2
    exit 1
fi

source "$ENV_FILE"

NODE_ID="${BLUEPRINTS_NODE_ID:?BLUEPRINTS_NODE_ID not set in .env}"
REPO_OUTER_PATH="${REPO_OUTER_PATH:-$SCRIPT_DIR}"
BLUEPRINTS_FALLBACK_GUI_DIR="${BLUEPRINTS_FALLBACK_GUI_DIR:-/xarta-node/gui-fallback}"
BLUEPRINTS_ASSETS_DIR="${BLUEPRINTS_ASSETS_DIR:-/xarta-node/gui-fallback/assets}"
BLUEPRINTS_DOCS_DIR="${BLUEPRINTS_DOCS_DIR:-/xarta-node/.lone-wolf/docs}"
BLUEPRINTS_TTS_VOICES_DIR="${BLUEPRINTS_TTS_VOICES_DIR:-/xarta-node/.lone-wolf/syncthing/tts/voices}"
SYNCTHING_HOSTNAME="${SYNCTHING_HOSTNAME:?SYNCTHING_HOSTNAME not set — add to .env (e.g. sync.<your-domain>)}"
SYNCTHING_GUI_USER="${SYNCTHING_GUI_USER:-admin}"
SYNCTHING_GUI_PASSWORD="${SYNCTHING_GUI_PASSWORD:?SYNCTHING_GUI_PASSWORD not set — add a strong password to .env before running}"
# Optional JSON array of non-fleet Syncthing devices to keep in config.
# Example:
#   SYNCTHING_EXTRA_DEVICES_JSON='[{"device_id":"AAAA...","name":"my-windows","addresses":["<windows-ip>","dynamic"]}]'
# Address values may be:
#   - "dynamic"
#   - raw IP/host (auto-converted to tcp://<value>:22000)
#   - full Syncthing address (e.g. tcp://<windows-ip>:22000)
SYNCTHING_EXTRA_DEVICES_JSON="${SYNCTHING_EXTRA_DEVICES_JSON:-[]}"
# Optional JSON object mapping folder IDs to extra external device IDs that
# should be included in the folder's device membership on rerun.
# Example:
#   SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON='{"xarta-node-docs":["AAAA...","BBBB..."]}'
SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON="${SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON:-{}}"

# ── Helpers ───────────────────────────────────────────────────────────────────
env_set() {
    local key="$1" value="$2"
    if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        echo -e "    ${CYAN}updated${NC}: ${key}"
    else
        echo "" >> "$ENV_FILE"
        echo "${key}=${value}" >> "$ENV_FILE"
        echo -e "    ${CYAN}added${NC}:   ${key}"
    fi
}

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

echo "=== Syncthing setup ==="
echo "Node         : $NODE_ID"
echo "Config dir   : $SYNCTHING_HOME"
echo "GUI hostname : $SYNCTHING_HOSTNAME"
echo "Repo path    : $REPO_OUTER_PATH"
echo "Assets path  : $BLUEPRINTS_ASSETS_DIR"
echo "Docs path    : $BLUEPRINTS_DOCS_DIR"
echo "TTS voices   : $BLUEPRINTS_TTS_VOICES_DIR"
echo ""

# ── Step 1 — Install Syncthing from official apt repository ──────────────────
echo "Step 1: Installing Syncthing..."
if command -v syncthing >/dev/null 2>&1; then
    echo -e "    already installed: $(syncthing --version 2>/dev/null | head -1)"
else
    echo "    Adding official Syncthing apt repository..."
    apt-get install -y --no-install-recommends curl gpg >/dev/null 2>&1
    curl -1sLf 'https://syncthing.net/release-key.gpg' \
        | gpg --dearmor --yes -o /usr/share/keyrings/syncthing-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/syncthing-archive-keyring.gpg] \
https://apt.syncthing.net/ syncthing stable" \
        > /etc/apt/sources.list.d/syncthing.list
    apt-get update -qq
    apt-get install -y syncthing
    echo -e "    ${GREEN}installed:${NC} $(syncthing --version 2>/dev/null | head -1)"
fi

echo "    Checking python3-bcrypt..."
dpkg -l python3-bcrypt >/dev/null 2>&1 || apt-get install -y python3-bcrypt
echo -e "    ${GREEN}ok${NC}"
echo ""

# ── Step 1b — Ensure Syncthing state directory exists under xarta ─────────────
echo "Step 1b: Ensuring Syncthing state directory..."
mkdir -p "$SYNCTHING_HOME"
chown -R xarta:xarta "/home/xarta/.local" 2>/dev/null || true
echo -e "    ${GREEN}ok${NC}: $SYNCTHING_HOME (owned by xarta)"
echo ""

# ── Step 2 — Managed share directories with Syncthing .stfolder markers ──────
echo "Step 2: Ensuring managed shared directories exist..."
ICONS_DIR="$BLUEPRINTS_ASSETS_DIR/icons"
SOUNDS_DIR="$BLUEPRINTS_ASSETS_DIR/sounds"
FONTS_DIR="$BLUEPRINTS_ASSETS_DIR/fonts"
DOCS_DIR="$BLUEPRINTS_DOCS_DIR"
TTS_VOICES_DIR="$BLUEPRINTS_TTS_VOICES_DIR"
mkdir -p "$BLUEPRINTS_ASSETS_DIR" "$ICONS_DIR" "$SOUNDS_DIR" "$FONTS_DIR" "$DOCS_DIR" "$TTS_VOICES_DIR"
chown_like "$(dirname "$BLUEPRINTS_ASSETS_DIR")" "$BLUEPRINTS_ASSETS_DIR"
chown_like "$BLUEPRINTS_ASSETS_DIR" "$ICONS_DIR"
chown_like "$BLUEPRINTS_ASSETS_DIR" "$SOUNDS_DIR"
chown_like "$BLUEPRINTS_ASSETS_DIR" "$FONTS_DIR"
chown_like "$(dirname "$DOCS_DIR")" "$DOCS_DIR"
chown_like "$(dirname "$TTS_VOICES_DIR")" "$TTS_VOICES_DIR"
# .stfolder is Syncthing's required presence marker. Without it Syncthing will
# refuse to sync the folder (treats a missing marker as an accidental deletion).
touch "$ICONS_DIR/.stfolder"
touch "$SOUNDS_DIR/.stfolder"
touch "$FONTS_DIR/.stfolder"
touch "$DOCS_DIR/.stfolder"
touch "$TTS_VOICES_DIR/.stfolder"
chown_like "$ICONS_DIR" "$ICONS_DIR/.stfolder"
chown_like "$SOUNDS_DIR" "$SOUNDS_DIR/.stfolder"
chown_like "$FONTS_DIR" "$FONTS_DIR/.stfolder"
chown_like "$DOCS_DIR" "$DOCS_DIR/.stfolder"
chown_like "$TTS_VOICES_DIR" "$TTS_VOICES_DIR/.stfolder"
echo -e "    ${GREEN}ok${NC}: $ICONS_DIR ($(find "$ICONS_DIR" -not -name '.stfolder' | wc -l) files)"
echo -e "    ${GREEN}ok${NC}: $SOUNDS_DIR ($(find "$SOUNDS_DIR" -not -name '.stfolder' | wc -l) files)"
echo -e "    ${GREEN}ok${NC}: $FONTS_DIR ($(find "$FONTS_DIR" -not -name '.stfolder' | wc -l) files)"
echo -e "    ${GREEN}ok${NC}: $DOCS_DIR ($(find "$DOCS_DIR" -not -name '.stfolder' | wc -l) files)"
echo -e "    ${GREEN}ok${NC}: $TTS_VOICES_DIR ($(find "$TTS_VOICES_DIR" -not -name '.stfolder' | wc -l) files)"
chown -R xarta:xarta "$BLUEPRINTS_ASSETS_DIR"
echo -e "    ${CYAN}ownership${NC}: xarta:xarta → $BLUEPRINTS_ASSETS_DIR"
chown -R xarta:xarta "$DOCS_DIR"
echo -e "    ${CYAN}ownership${NC}: xarta:xarta → $DOCS_DIR"
chown -R xarta:xarta "$TTS_VOICES_DIR"
echo -e "    ${CYAN}ownership${NC}: xarta:xarta → $TTS_VOICES_DIR"

if [[ -f "$ASSETS_OWNER_FIX_SCRIPT" ]]; then
    chmod 755 "$ASSETS_OWNER_FIX_SCRIPT"
    if ! grep -q "$ASSETS_OWNER_CRON_MARKER" "$ASSETS_OWNER_CRON_FILE" 2>/dev/null; then
        echo "# $ASSETS_OWNER_CRON_MARKER" > "$ASSETS_OWNER_CRON_FILE"
        echo "$ASSETS_OWNER_CRON_LINE" >> "$ASSETS_OWNER_CRON_FILE"
        chmod 644 "$ASSETS_OWNER_CRON_FILE"
        echo -e "    ${CYAN}owner-guard${NC}: installed $ASSETS_OWNER_CRON_FILE (runs every minute)"
    else
        echo -e "    ${GREEN}owner-guard${NC}: already installed"
    fi
else
    echo -e "    ${YELLOW}owner-guard skipped${NC}: missing $ASSETS_OWNER_FIX_SCRIPT"
fi
echo ""

# ── Step 3 — Stable API key (generated once, persisted in .env) ──────────────
echo "Step 3: Checking Syncthing API key..."
if [[ -z "${SYNCTHING_API_KEY:-}" ]]; then
    SYNCTHING_API_KEY=$(python3 -c "import secrets; print(secrets.token_hex(20))")
    env_set "SYNCTHING_API_KEY" "$SYNCTHING_API_KEY"
    echo -e "    ${CYAN}generated${NC} new API key — written to .env"
else
    echo "    API key present in .env — keeping existing"
fi
echo ""

# ── Step 3b — Migrate certs from old home if present (preserves device ID) ───────
OLD_SYNCTHING_HOMES=(
    "/root/.local/state/syncthing"
    "/var/lib/syncthing/.local/state/syncthing"
)
echo "Step 3b: Checking for Syncthing certificates to migrate..."
migrated=0
for OLD_HOME in "${OLD_SYNCTHING_HOMES[@]}"; do
    if [[ -f "$OLD_HOME/cert.pem" ]] && [[ ! -f "$SYNCTHING_HOME/cert.pem" ]]; then
        echo "    Found certs in '$OLD_HOME' — migrating to preserve device ID..."
        mkdir -p "$SYNCTHING_HOME"
        cp "$OLD_HOME/cert.pem" "$SYNCTHING_HOME/cert.pem"
        cp "$OLD_HOME/key.pem"  "$SYNCTHING_HOME/key.pem"
        chown -R xarta:xarta "/home/xarta/.local"
        echo -e "    ${GREEN}migrated${NC}: cert.pem + key.pem → $SYNCTHING_HOME"
        migrated=1
        break
    fi
done
if [[ $migrated -eq 0 ]]; then
    if [[ -f "$SYNCTHING_HOME/cert.pem" ]]; then
        echo "    Certs already present in $SYNCTHING_HOME — no migration needed"
    else
        echo "    No existing certs found — Syncthing will generate a new device identity"
        mkdir -p "$SYNCTHING_HOME"
        chown -R xarta:xarta "/home/xarta/.local"
    fi
fi
echo ""

# ── Step 4 — Start service briefly to generate cert and obtain device ID ──────
echo "Step 4: Starting syncthing@xarta.service to generate device certificate..."
# Stop old service first so it frees port 8384 (new service will start on the
# default port 8384 when the port is available.
if systemctl is-active --quiet syncthing@root.service 2>/dev/null; then
    echo "    Stopping syncthing@root.service (frees port 8384)..."
    systemctl stop syncthing@root.service
fi
systemctl enable syncthing@xarta.service >/dev/null 2>&1
systemctl start syncthing@xarta.service

# Syncthing generates config.xml and its cert/key on first startup.
echo "    Waiting for config.xml (up to 30s)..."
for i in $(seq 1 30); do
    [[ -f "$SYNCTHING_HOME/config.xml" ]] && break
    sleep 1
done

if [[ ! -f "$SYNCTHING_HOME/config.xml" ]]; then
    echo -e "${RED}Error:${NC} Syncthing did not create config.xml within 30s." >&2
    echo "  Running as user 'xarta', home dir: /home/xarta" >&2
    echo "  Expected config: $SYNCTHING_HOME/config.xml" >&2
    echo "  Logs: journalctl -u syncthing@xarta -n 50" >&2
    exit 1
fi
echo -e "    ${GREEN}found${NC}: $SYNCTHING_HOME/config.xml"

# Reuse whatever GUI address Syncthing is currently configured for. This avoids
# deleting config.xml on reruns (which would drop unmanaged shared folders).
GUI_LISTEN_ADDR=$(python3 -c "
import xml.etree.ElementTree as ET
root = ET.parse('$SYNCTHING_HOME/config.xml').getroot()
print(root.findtext('./gui/address') or '127.0.0.1:8384')
" 2>/dev/null)

if [[ "$GUI_LISTEN_ADDR" != 127.0.0.1:* && "$GUI_LISTEN_ADDR" != localhost:* ]]; then
    GUI_LISTEN_ADDR="127.0.0.1:8384"
fi
GUI_API_URL="http://$GUI_LISTEN_ADDR"
echo "    Using API endpoint: $GUI_API_URL"

# Read the auto-generated API key from the fresh config to authenticate the API.
INITIAL_API_KEY=$(python3 -c "
import xml.etree.ElementTree as ET
root = ET.parse('$SYNCTHING_HOME/config.xml').getroot()
print(root.findtext('./gui/apikey') or '')
" 2>/dev/null)

# Poll until the REST API responds (up to 30s).
echo "    Waiting for local REST API on $GUI_API_URL (up to 30s)..."
API_UP=0
for i in $(seq 1 30); do
    if curl -sf -H "X-API-Key: $INITIAL_API_KEY" \
            "$GUI_API_URL/rest/system/ping" >/dev/null 2>&1; then
        API_UP=1
        break
    fi
    sleep 1
done

if [[ $API_UP -eq 0 ]]; then
    echo -e "${RED}Error:${NC} Syncthing API did not respond within 30s." >&2
    echo "  Logs: journalctl -u syncthing@xarta -n 50" >&2
    systemctl stop syncthing@xarta.service || true
    exit 1
fi

# Read this node's device ID from the running instance.
OWN_DEVICE_ID=$(curl -sf \
    -H "X-API-Key: $INITIAL_API_KEY" \
    "$GUI_API_URL/rest/system/status" \
    | python3 -c "import json,sys; print(json.load(sys.stdin)['myID'])")

echo -e "    ${GREEN}Device ID:${NC} $OWN_DEVICE_ID"
systemctl stop syncthing@xarta.service
echo ""

# ── Step 5 — Write device ID to .env ─────────────────────────────────────────
echo "Step 5: Writing device ID to .env..."
env_set "SYNCTHING_DEVICE_ID" "$OWN_DEVICE_ID"
source "$ENV_FILE"
echo ""

# ── Step 6 — Generate config.xml from .nodes.json ────────────────────────────
echo "Step 6: Generating config.xml from .nodes.json + .env..."

# Bcrypt the GUI password at runtime. The plain password stays in .env;
# only the hash is written to config.xml — Syncthing expects bcrypt cost 10.
GUI_PASS_HASH=$(SYNCTHING_GUI_PASSWORD="$SYNCTHING_GUI_PASSWORD" python3 -c "
import bcrypt, os
pw = os.environ['SYNCTHING_GUI_PASSWORD'].encode()
print(bcrypt.hashpw(pw, bcrypt.gensalt(10)).decode())
")

# Write the Python patcher to a tempfile so args are passed cleanly.
TMPPY=$(mktemp /tmp/syncthing-patch-XXXXXXXX.py)
trap "rm -f '$TMPPY'" EXIT

cat > "$TMPPY" << 'PYEOF'
"""Patch Syncthing config.xml with fleet settings from .nodes.json and .env.

Args (positional):
    config_path     — path to config.xml
    nodes_json      — path to .nodes.json
    node_id         — e.g. my-node-1
    own_device_id   — Syncthing device ID for this node
    gui_user        — Syncthing GUI username
    gui_pass_hash   — bcrypt hash of the GUI password
    api_key         — Syncthing REST API key
    assets_dir      — shared assets root (e.g. /root/xarta-node/gui-fallback/assets)
    docs_dir        — shared docs root (e.g. /xarta-node/.lone-wolf/docs)
    tts_voices_dir  — shared TTS voices root (e.g. /xarta-node/.lone-wolf/syncthing/tts/voices)
    extra_devices_json — JSON array of external/non-fleet Syncthing devices
    extra_folder_device_ids_json — JSON object: folder_id -> [device_id,...]
"""
import sys
import json
import xml.etree.ElementTree as ET

if len(sys.argv) != 13:
    print("Usage: syncthing-patch.py <config> <nodes_json> <node_id> "
          "<own_device_id> <gui_user> <gui_pass_hash> <api_key> <assets_dir> <docs_dir> <tts_voices_dir> <extra_devices_json> <extra_folder_device_ids_json>",
          file=sys.stderr)
    sys.exit(1)

(config_path, nodes_json_path, node_id, own_device_id,
 gui_user, gui_pass_hash, api_key, assets_dir, docs_dir, tts_voices_dir,
 extra_devices_json, extra_folder_device_ids_json) = sys.argv[1:]

with open(nodes_json_path) as nf:
    nodes = json.load(nf)['nodes']

own_node = next((n for n in nodes if n['node_id'] == node_id), None)
if own_node is None:
    print(f"ERROR: node_id '{node_id}' not found in .nodes.json", file=sys.stderr)
    sys.exit(1)

peers = [n for n in nodes if n['node_id'] != node_id]

try:
    extra_devices = json.loads(extra_devices_json) if extra_devices_json.strip() else []
except json.JSONDecodeError as exc:
    print(f"ERROR: SYNCTHING_EXTRA_DEVICES_JSON is invalid JSON: {exc}", file=sys.stderr)
    sys.exit(1)

if not isinstance(extra_devices, list):
    print("ERROR: SYNCTHING_EXTRA_DEVICES_JSON must be a JSON array", file=sys.stderr)
    sys.exit(1)

try:
    extra_folder_device_ids = (
        json.loads(extra_folder_device_ids_json)
        if extra_folder_device_ids_json.strip() else {}
    )
except json.JSONDecodeError as exc:
    print(
        f"ERROR: SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON is invalid JSON: {exc}",
        file=sys.stderr,
    )
    sys.exit(1)

if not isinstance(extra_folder_device_ids, dict):
    print(
        "ERROR: SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON must be a JSON object",
        file=sys.stderr,
    )
    sys.exit(1)

tree = ET.parse(config_path)
root = tree.getroot()


def sub_text(parent, tag, text):
    """Set text of an existing child element or create the element if absent."""
    el = parent.find(tag)
    if el is None:
        el = ET.SubElement(parent, tag)
    el.text = text
    return el


# ── GUI ───────────────────────────────────────────────────────────────────────
gui = root.find('gui')
if gui is None:
    gui = ET.SubElement(root, 'gui')
gui.set('enabled', 'true')
gui.set('tls', 'false')         # Caddy provides TLS; keep Syncthing GUI plain HTTP
sub_text(gui, 'address', '127.0.0.1:8384')
sub_text(gui, 'user', gui_user)
sub_text(gui, 'password', gui_pass_hash)
sub_text(gui, 'apikey', api_key)
sub_text(gui, 'theme', 'default')
# Caddy sends Host: localhost (header_up Host localhost) so Syncthing's
# built-in host check passes without the insecureSkipHostcheck override.
sub_text(gui, 'insecureSkipHostcheck', 'false')

# ── Options ───────────────────────────────────────────────────────────────────
opts = root.find('options')
if opts is None:
    opts = ET.SubElement(root, 'options')
sub_text(opts, 'listenAddress', 'tcp://0.0.0.0:22000')
sub_text(opts, 'localAnnounceEnabled', 'false')   # fleet uses static peer IPs
sub_text(opts, 'localAnnouncePort', '21027')
sub_text(opts, 'globalAnnounceEnabled', 'false')  # no external discovery
sub_text(opts, 'relaysEnabled', 'false')           # direct fleet-only sync
sub_text(opts, 'natEnabled', 'false')
sub_text(opts, 'startBrowser', 'false')
sub_text(opts, 'autoUpgradeIntervalH', '0')        # versions managed by apt
sub_text(opts, 'urAccepted', '-1')                 # opt out of telemetry

# ── Devices ───────────────────────────────────────────────────────────────────
# Remove all existing device entries and rebuild cleanly from .nodes.json.
for dev in list(root.findall('device')):
    root.remove(dev)

# Own device (address stays 'dynamic' — syncthing resolves from its listen port).
own_dev = ET.SubElement(root, 'device')
own_dev.set('id', own_device_id)
own_dev.set('name', node_id)
own_dev.set('compression', 'metadata')
own_dev.set('introduceClients', 'false')
own_dev.set('skipIntroductionRemovals', 'false')
own_dev.set('introducedBy', '')
ET.SubElement(own_dev, 'address').text = 'dynamic'

# Peer devices — only those with populated syncthing_device_id.
# On pass 1 (device IDs not yet known) these are all skipped; that is expected.
peer_device_ids = []
skipped_peers = []
device_ids_seen = {own_device_id}
for peer in peers:
    dev_id = peer.get('syncthing_device_id', '').strip()
    if not dev_id:
        skipped_peers.append(peer['node_id'])
        continue
    peer_dev = ET.SubElement(root, 'device')
    peer_dev.set('id', dev_id)
    peer_dev.set('name', peer['node_id'])
    peer_dev.set('compression', 'metadata')
    peer_dev.set('introduceClients', 'false')
    peer_dev.set('skipIntroductionRemovals', 'false')
    peer_dev.set('introducedBy', '')
    ip = peer.get('primary_ip', '')
    ET.SubElement(peer_dev, 'address').text = (
        f'tcp://{ip}:22000' if ip else 'dynamic'
    )
    peer_device_ids.append(dev_id)
    device_ids_seen.add(dev_id)


def normalize_addresses(raw):
    """Convert address value(s) into Syncthing address entries."""
    if raw is None:
        return ['dynamic']

    if isinstance(raw, str):
        candidates = [raw]
    elif isinstance(raw, list):
        candidates = raw
    else:
        return ['dynamic']

    out = []
    for val in candidates:
        if not isinstance(val, str):
            continue
        s = val.strip()
        if not s:
            continue
        if s == 'dynamic':
            out.append('dynamic')
        elif '://' in s:
            out.append(s)
        else:
            out.append(f'tcp://{s}:22000')

    if not out:
        return ['dynamic']
    return out


extra_added = []
extra_skipped = []
for item in extra_devices:
    if not isinstance(item, dict):
        extra_skipped.append('<non-object-entry>')
        continue

    dev_id = str(
        item.get('device_id') or item.get('deviceID') or item.get('id') or ''
    ).strip()
    if not dev_id:
        extra_skipped.append('<missing-device-id>')
        continue

    if dev_id in device_ids_seen:
        extra_skipped.append(dev_id)
        continue

    name = str(item.get('name') or f'external-{dev_id[:7]}').strip()
    if not name:
        name = f'external-{dev_id[:7]}'

    dev = ET.SubElement(root, 'device')
    dev.set('id', dev_id)
    dev.set('name', name)
    dev.set('compression', 'metadata')
    dev.set('introduceClients', 'false')
    dev.set('skipIntroductionRemovals', 'false')
    dev.set('introducedBy', '')

    for addr in normalize_addresses(item.get('addresses', item.get('address'))):
        ET.SubElement(dev, 'address').text = addr

    extra_added.append(dev_id)
    device_ids_seen.add(dev_id)

# ── Shared Folders ────────────────────────────────────────────────────────────
# Remove all known managed folders plus the Syncthing default folder (which
# points to a path that may not exist on this node) and rebuild from scratch.
for fid in ('xarta-icons', 'xarta-sounds', 'xarta-fonts', 'xarta-node-docs', 'xarta-tts-voices', 'default'):
    for f in list(root.findall(f'folder[@id="{fid}"]')):
        root.remove(f)

all_device_ids = [own_device_id] + peer_device_ids


folder_extra_applied = {}
folder_extra_skipped = []


def folder_extra_ids(folder_id):
    """Return valid extra device IDs for a managed folder."""
    raw = extra_folder_device_ids.get(folder_id, [])
    if isinstance(raw, str):
        candidates = [raw]
    elif isinstance(raw, list):
        candidates = raw
    else:
        candidates = []

    out = []
    for val in candidates:
        sid = str(val).strip()
        if not sid:
            continue
        if sid not in device_ids_seen:
            folder_extra_skipped.append(f"{folder_id}:{sid}")
            continue
        if sid not in out:
            out.append(sid)

    if out:
        folder_extra_applied[folder_id] = out
    return out


def add_folder(fid, label, path, extra_ids=None):
    if extra_ids is None:
        extra_ids = []

    folder_device_ids = []
    for did in all_device_ids + list(extra_ids):
        if did not in folder_device_ids:
            folder_device_ids.append(did)

    f = ET.SubElement(root, 'folder')
    f.set('id', fid)
    f.set('label', label)
    f.set('path', path)
    f.set('type', 'sendreceive')
    f.set('rescanIntervalS', '3600')
    f.set('fsWatcherEnabled', 'true')
    f.set('fsWatcherDelayS', '10')
    f.set('autoNormalize', 'true')
    f.set('paused', 'false')
    for did in folder_device_ids:
        d = ET.SubElement(f, 'device')
        d.set('id', did)
        d.set('introducedBy', '')
    ET.SubElement(f, 'maxConflicts').text = '10'
    ET.SubElement(f, 'markerName').text = '.stfolder'


add_folder('xarta-icons', 'Assets - Icons',
           assets_dir + '/icons',
           folder_extra_ids('xarta-icons'))
add_folder('xarta-sounds', 'Assets - Sounds',
           assets_dir + '/sounds',
           folder_extra_ids('xarta-sounds'))
add_folder('xarta-fonts', 'Assets - Fonts',
           assets_dir + '/fonts',
           folder_extra_ids('xarta-fonts'))
add_folder('xarta-node-docs', 'xarta-node-docs',
           docs_dir,
           folder_extra_ids('xarta-node-docs'))
add_folder('xarta-tts-voices', 'tts-voices',
           tts_voices_dir,
           folder_extra_ids('xarta-tts-voices'))

# ── Write ─────────────────────────────────────────────────────────────────────
ET.indent(tree, space='    ')
with open(config_path, 'wb') as fh:
    tree.write(fh, xml_declaration=True, encoding='utf-8')

print(f"    Written: {config_path}")
print(f"    Own device:       {own_device_id}")
print(f"    Peers configured: {len(peer_device_ids)} of {len(peers)}")
if skipped_peers:
    print(f"    Skipped (no device ID yet): {', '.join(skipped_peers)}")
print(f"    External devices added: {len(extra_added)}")
if extra_added:
    print(f"    External IDs added: {', '.join(extra_added)}")
if extra_skipped:
    print(f"    External entries skipped: {', '.join(extra_skipped)}")
if folder_extra_applied:
    for fid, ids in folder_extra_applied.items():
        print(f"    Folder extra members [{fid}]: {', '.join(ids)}")
if folder_extra_skipped:
    print(f"    Folder extra entries skipped: {', '.join(folder_extra_skipped)}")
PYEOF

python3 "$TMPPY" \
    "$SYNCTHING_HOME/config.xml" \
    "$NODES_JSON" \
    "$NODE_ID" \
    "$OWN_DEVICE_ID" \
    "$SYNCTHING_GUI_USER" \
    "$GUI_PASS_HASH" \
    "$SYNCTHING_API_KEY" \
    "$BLUEPRINTS_ASSETS_DIR" \
    "$BLUEPRINTS_DOCS_DIR" \
    "$BLUEPRINTS_TTS_VOICES_DIR" \
    "$SYNCTHING_EXTRA_DEVICES_JSON" \
    "$SYNCTHING_EXTRA_FOLDER_DEVICE_IDS_JSON"
echo ""

# ── Step 7 — Enable and restart Syncthing ────────────────────────────────────
echo "Step 7: Enabling and restarting syncthing@xarta.service..."
if systemctl is-enabled --quiet syncthing@root.service 2>/dev/null; then
    if systemctl is-active --quiet syncthing@root.service 2>/dev/null; then
        echo "    Stopping old syncthing@root.service..."
        systemctl stop syncthing@root.service || true
    fi
    systemctl disable syncthing@root.service 2>/dev/null || true
    echo -e "    ${CYAN}disabled${NC}: syncthing@root.service"
fi
if systemctl is-enabled --quiet syncthing@syncthing.service 2>/dev/null; then
    if systemctl is-active --quiet syncthing@syncthing.service 2>/dev/null; then
        echo "    Stopping legacy syncthing@syncthing.service..."
        systemctl stop syncthing@syncthing.service || true
    fi
    systemctl disable syncthing@syncthing.service 2>/dev/null || true
    echo -e "    ${CYAN}disabled${NC}: syncthing@syncthing.service"
fi
systemctl enable syncthing@xarta.service >/dev/null 2>&1
systemctl restart syncthing@xarta.service
sleep 2

if systemctl is-active --quiet syncthing@xarta.service; then
    echo -e "    ${GREEN}running${NC}: syncthing@xarta.service"
else
    echo -e "${RED}Error:${NC} syncthing@xarta.service failed to start." >&2
    echo "  Logs: journalctl -u syncthing@xarta -n 50" >&2
    exit 1
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
PEER_STATUS=$(python3 -c "
import json
nodes = json.load(open('$NODES_JSON'))['nodes']
peers = [n for n in nodes if n.get('node_id') != '$NODE_ID']
done = sum(1 for p in peers if p.get('syncthing_device_id', '').strip())
print(f'{done}/{len(peers)}')
")

echo "=== Syncthing setup complete ==="
echo ""
echo "  Node         : $NODE_ID"
echo "  Device ID    : $OWN_DEVICE_ID"
echo "  Peers        : $PEER_STATUS device IDs configured in .nodes.json"
echo "  Service      : syncthing@xarta.service ($(systemctl is-active syncthing@xarta.service))"
echo "  Config       : $SYNCTHING_HOME/config.xml"
echo ""
echo "Next steps:"
echo ""
echo "  1. Add this device ID to .nodes.json under node '$NODE_ID':"
echo "       \"syncthing_device_id\": \"$OWN_DEVICE_ID\""
echo ""
echo "  2. Distribute updated .nodes.json to all fleet nodes:"
echo "       bash bp-nodes-push.sh"
echo ""
echo "  3. Re-run this script (pass 2) to inject configured peer device IDs."
echo ""
echo "  4. Add pfSense DNS record: $SYNCTHING_HOSTNAME → <this node's primary_ip>"
echo ""
echo "  5. Re-run setup-caddy.sh to expose the Syncthing GUI at:"
echo "       https://$SYNCTHING_HOSTNAME"
echo ""
echo "  6. Repeat on each remaining fleet node."
echo ""
