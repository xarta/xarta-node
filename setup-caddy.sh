#!/bin/bash

# setup-caddy.sh
# Installs Caddy and configures it as a reverse proxy for blueprints-app.
#
# What this script does (idempotent):
#   1. Runs setup-certificates.sh to ensure TLS certs exist.
#   2. Installs Caddy from the official apt repo (if not present).
#   3. Creates a systemd drop-in so Caddy runs as root (required to read
#      certs stored in the private inner repo under /root/).
#   4. Writes a Caddyfile to $REPO_CADDY_PATH/Caddyfile.
#   5. Symlinks /etc/caddy/Caddyfile → $REPO_CADDY_PATH/Caddyfile.
#   6. Validates the Caddyfile, then enables and (re)starts Caddy.
#   7. Runs a basic health check via HTTPS.
#
# After a successful run:
#   - blueprints-app continues to run on localhost:8080 (unchanged)
#   - Caddy listens on :443 (HTTPS) and :80 (redirects to HTTPS)
#   - Update BLUEPRINTS_UI_URL in .env from http://...:8080 to https://...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Colours ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

# ── Load .env ──────────────────────────────────────────────────────────────────
if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env not found at $ENV_FILE" >&2
    exit 1
fi
source "$ENV_FILE"

# ── Helpers ────────────────────────────────────────────────────────────────────
env_set() {
    local key="$1" value="$2"
    if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
        sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
        echo -e "    ${CYAN}updated${NC}: ${key}=${value}"
    else
        echo "" >> "$ENV_FILE"
        echo "${key}=${value}" >> "$ENV_FILE"
        echo -e "    ${CYAN}added${NC}:   ${key}=${value}"
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

read_fallback_cache_mode() {
    local state_file="$1"

    if [[ ! -f "$state_file" ]]; then
        echo "production"
        return 0
    fi

    python3 - "$state_file" <<'PYEOF'
import json
import sys

path = sys.argv[1]
try:
    with open(path) as f:
        data = json.load(f)
except Exception:
    print("production")
    raise SystemExit(0)

mode = str(data.get("desired_mode") or data.get("current_mode") or "production").strip().lower()
print(mode if mode in {"production", "development"} else "production")
PYEOF
}

compute_fallback_asset_version() {
    local mode="$1"
    local repo_path="${REPO_NON_ROOT_PATH:-/xarta-node}"
    local fallback_root="$2"

    if [[ "$mode" == "development" ]]; then
        date -u +dev-%Y%m%d%H%M%S
        return 0
    fi

    if [[ -n "$repo_path" && -d "$repo_path/.git" ]]; then
        local head ts dirty
        if head="$(git -C "$repo_path" rev-parse --short HEAD 2>/dev/null)" \
            && ts="$(git -C "$repo_path" log -1 --format=%ct 2>/dev/null)"; then
            dirty="$(git -C "$repo_path" status --porcelain 2>/dev/null || true)"
            if [[ -n "$dirty" ]]; then
                printf 'prod-%s-%s-dirty\n' "$head" "$ts"
            else
                printf 'prod-%s-%s\n' "$head" "$ts"
            fi
            return 0
        fi
    fi

    python3 - "$fallback_root" <<'PYEOF'
from pathlib import Path
import sys

root = Path(sys.argv[1])
latest = 0
if root.exists():
    try:
        latest = int(root.stat().st_mtime)
    except OSError:
        latest = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            latest = max(latest, int(path.stat().st_mtime))
        except OSError:
            continue
print(f"prod-mtime-{latest}")
PYEOF
}

write_fallback_cache_state() {
    local state_file="$1"
    local desired_mode="$2"
    local current_mode="$3"
    local asset_version="$4"
    local fallback_root="$5"

    mkdir -p "$(dirname "$state_file")"

    python3 - "$state_file" "$desired_mode" "$current_mode" "$asset_version" "$fallback_root" <<'PYEOF'
import json
import sys
from datetime import datetime, timezone

path, desired_mode, current_mode, asset_version, fallback_root = sys.argv[1:6]
data = {}
try:
    with open(path) as f:
        data = json.load(f)
except Exception:
    data = {}

data.update({
    "desired_mode": desired_mode,
    "current_mode": current_mode,
    "asset_version": asset_version,
    "fallback_root": fallback_root,
    "last_applied_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "last_apply_ok": True,
})

with open(path, "w") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PYEOF
}

# Extract hostname (no scheme, no port) from a URL.
url_host() { echo "$1" | sed 's|^https\?://||' | sed 's|:.*||' | sed 's|/.*||'; }

derive_nodes_json_host() {
    local field_name="$1"

    if [[ -z "${NODES_JSON_PATH:-}" || -z "${BLUEPRINTS_NODE_ID:-}" || ! -f "$NODES_JSON_PATH" ]]; then
        return 0
    fi

    python3 - "$NODES_JSON_PATH" "$BLUEPRINTS_NODE_ID" "$field_name" <<'PYEOF'
import json
import sys

nodes_json_path, node_id, field_name = sys.argv[1:4]
with open(nodes_json_path) as f:
    nodes = json.load(f).get("nodes", [])

node = next((n for n in nodes if n.get("node_id") == node_id), None)
if not node:
    sys.exit(0)

value = (node.get(field_name) or "").strip()
if value:
    print(value)
PYEOF
}

echo "=== Caddy setup ==="
echo ""

# ── Step 1 — Certificates ───────────────────────────────────────────────────
echo "Step 1: Checking certificates..."
bash "$SCRIPT_DIR/setup-certificates.sh"
# Re-source .env in case setup-certificates.sh wrote new values.
source "$ENV_FILE"

if [[ -z "${CERT_FILE:-}" || -z "${CERT_KEY:-}" ]]; then
    echo -e "${RED}Error:${NC} CERT_FILE / CERT_KEY not set after setup-certificates.sh." >&2
    exit 1
fi
echo ""

# ── Step 2 — Install Caddy ───────────────────────────────────────────────────
echo "Step 2: Installing Caddy..."
if command -v caddy >/dev/null 2>&1; then
    echo -e "    Caddy already installed: $(caddy version 2>/dev/null | head -1)"
else
    echo "    Adding Caddy apt repository..."
    apt-get install -y debian-keyring debian-archive-keyring apt-transport-https curl 2>/dev/null | tail -1
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' \
        | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' \
        | tee /etc/apt/sources.list.d/caddy-stable.list > /dev/null
    apt-get update -qq
    apt-get install -y caddy
    echo -e "    ${GREEN}Caddy installed:${NC} $(caddy version 2>/dev/null | head -1)"
fi
echo ""

# ── Step 3 — Systemd drop-in (run Caddy as root) ─────────────────────────────
# The cert key is in /root/ — the default 'caddy' user cannot read it.
echo "Step 3: Configuring Caddy systemd service..."
DROPIN_DIR="/etc/systemd/system/caddy.service.d"
DROPIN_FILE="$DROPIN_DIR/run-as-root.conf"
mkdir -p "$DROPIN_DIR"
if [[ ! -f "$DROPIN_FILE" ]]; then
    cat > "$DROPIN_FILE" <<'EOF'
[Service]
User=root
Group=root
EOF
    echo "    Created: $DROPIN_FILE"
    systemctl daemon-reload
else
    echo "    Already present: $DROPIN_FILE"
fi
echo ""

# ── Step 4 — Write Caddyfile ─────────────────────────────────────────────────
echo "Step 4: Writing Caddyfile..."

if [[ -z "${REPO_CADDY_PATH:-}" ]]; then
    echo -e "${RED}Error:${NC} REPO_CADDY_PATH is not set in .env." >&2
    exit 1
fi

CADDYFILE="$REPO_CADDY_PATH/Caddyfile"
UI_HOST=$(url_host "${BLUEPRINTS_UI_URL:-localhost}")
if [[ -z "$UI_HOST" || "$UI_HOST" == "localhost" ]]; then
    DERIVED_PRIMARY_HOST="$(derive_nodes_json_host primary_hostname)"
    if [[ -n "$DERIVED_PRIMARY_HOST" ]]; then
        UI_HOST="$DERIVED_PRIMARY_HOST"
        echo "    Derived primary UI host from .nodes.json: $UI_HOST"
    fi
fi
REFERENCE_UI_ROOT="${REPO_INNER_PATH:-$SCRIPT_DIR/.xarta}/gui-reference"
BLUEPRINTS_FALLBACK_GUI_DIR="${BLUEPRINTS_FALLBACK_GUI_DIR:-${REPO_OUTER_PATH:-$SCRIPT_DIR}/gui-fallback}"
BLUEPRINTS_DB_DIR="${BLUEPRINTS_DB_DIR:-/opt/blueprints/data/db}"
FALLBACK_CACHE_STATE_FILE="${FALLBACK_CACHE_STATE_FILE:-${BLUEPRINTS_DB_DIR}/fallback-ui-cache-state.json}"
FALLBACK_CACHE_MODE="$(read_fallback_cache_mode "$FALLBACK_CACHE_STATE_FILE")"
FALLBACK_ASSET_VERSION="$(compute_fallback_asset_version "$FALLBACK_CACHE_MODE" "$BLUEPRINTS_FALLBACK_GUI_DIR")"
CODE_SERVER_HOSTNAME="${CODE_SERVER_HOSTNAME:-code.${UI_HOST}}"
CODE_SERVER_PORT="${CODE_SERVER_PORT:-8082}"

if [[ "$FALLBACK_CACHE_MODE" == "development" ]]; then
    FALLBACK_ASSET_CACHE_HEADERS=$(cat <<'EOF'
        header @fallback_assets Cache-Control "no-cache, no-store, must-revalidate"
        header @fallback_assets Pragma "no-cache"
        header @fallback_assets Expires "0"
EOF
)
else
    FALLBACK_ASSET_CACHE_HEADERS=$(cat <<'EOF'
        header @fallback_assets Cache-Control "public, max-age=0, must-revalidate"
EOF
)
fi

echo "    Fallback UI cache mode: $FALLBACK_CACHE_MODE"
echo "    Fallback UI asset version: $FALLBACK_ASSET_VERSION"

# Build the full comma-separated hostname list for the Caddy site blocks.
# Always includes the primary UI host; appends CADDY_EXTRA_NAMES if set.
HTTPS_NAMES="https://${UI_HOST}"
HTTP_NAMES="http://${UI_HOST}"
if [[ -n "${CADDY_EXTRA_NAMES:-}" ]]; then
    IFS=',' read -ra EXTRA <<< "$CADDY_EXTRA_NAMES"
    for name in "${EXTRA[@]}"; do
        name="${name// /}"  # trim whitespace
        [[ -z "$name" ]] && continue
        [[ "$name" == "$UI_HOST" ]] && continue
        HTTPS_NAMES+=", https://${name}"
        HTTP_NAMES+=", http://${name}"
    done
fi

# If mTLS is configured, add a servers block to the global options that
# disables strict SNI-Host enforcement for :8443. Caddy auto-enables this when
# client_auth is present, but fleet peers connect by IP (no SNI in ClientHello)
# so host-matching must be relaxed. mTLS CA verification enforces identity.
MTLS_SERVERS_BLOCK=""
if [[ -n "${SYNC_TLS_CA:-}" && -n "${SYNC_TLS_CERT:-}" && -n "${SYNC_TLS_KEY:-}" ]]; then
    MTLS_SERVERS_BLOCK="
    # Suppress strict SNI-Host enforcement for the mTLS sync port.
    # Peers connect by IP address (no SNI), so SNI-Host matching would
    # reject every request. mTLS CA verification enforces identity instead.
    servers :8443 {
        strict_sni_host insecure_off
    }"
fi

cat > "$CADDYFILE" <<CADDY
# Caddyfile — blueprints-app reverse proxy
# Generated by setup-caddy.sh — re-run to regenerate with updated .env values.
#
# Cert:  ${CERT_FILE}
# Key:   ${CERT_KEY}
# CA:    ${CERT_CA:-"(none)"}
# Upstream: localhost:8080 (blueprints-app uvicorn)

{
    # Disable Caddy's built-in ACME / Let's Encrypt — we supply our own certs.
    auto_https off
    # Disable the admin API endpoint (not needed on this node).
    admin off${MTLS_SERVERS_BLOCK}
}

# HTTPS — proxy everything to blueprints-app.
${HTTPS_NAMES} {
    tls ${CERT_FILE} ${CERT_KEY}

    # Redirect bare root to the GUI.
    redir / /ui/ permanent

    # Fallback UI — frozen public copy of the GUI, served directly by Caddy.
    # Intentionally separate from /ui (which is served by uvicorn).
    # This copy is not updated when the private GUI is overhauled.
    redir /fallback-ui /fallback-ui/ permanent
    handle_path /fallback-ui/* {
        root * ${BLUEPRINTS_FALLBACK_GUI_DIR}
        vars bp_asset_version "${FALLBACK_ASSET_VERSION}"
        vars bp_cache_mode "${FALLBACK_CACHE_MODE}"

        # HTML is the routing / asset-manifest template layer, so keep it non-cacheable.
        @fallback_html {
            path / *.html
        }

        # Static assets use a Caddy-injected asset token in HTML.
        # Production revalidates on reuse so manual ?v= bumps are unnecessary.
        # Development disables browser storage entirely for live device testing.
        @fallback_assets {
            not path / *.html
        }

        header @fallback_html Cache-Control "no-cache, no-store, must-revalidate"
        header @fallback_html Pragma "no-cache"
        header @fallback_html Expires "0"

${FALLBACK_ASSET_CACHE_HEADERS}

        templates

        file_server
    }

    # Reference UI — private web-design pattern library and live demos.
    # Served directly by Caddy from gui-reference/ in the private repo.
    redir /reference-ui /reference-ui/ permanent
    handle_path /reference-ui/* {
        root * ${REFERENCE_UI_ROOT}
        file_server
    }

    # PocketTTS local test UI/API on a dedicated path.
    # handle_path strips /tts/pockettts before proxying to the stack.
    redir /tts/pockettts /tts/pockettts/ permanent
    handle_path /tts/pockettts* {
        reverse_proxy localhost:18884
    }

    # Reverse proxy all traffic to the local uvicorn process.
    # Includes /health, /api/v1/*, and /ui/* (GUI + embed component).
    reverse_proxy localhost:8080
}

# HTTP — redirect all requests to HTTPS.
${HTTP_NAMES} {
    redir https://{host}{uri} permanent
}
CADDY

chown_like "$REPO_CADDY_PATH" "$CADDYFILE"

# ── code-server block — appended when CODE_SERVER is set ─────────────────────
# code-server itself binds to loopback only. Caddy terminates TLS and keeps the
# browser IDE private to local, RFC1918, and tailnet source addresses.
if [[ -n "${CODE_SERVER:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_CODE_SERVER

# code-server — browser IDE for this node.
# Backend binds to loopback only; Caddy exposes it on the private HTTPS entrypoint.
https://${CODE_SERVER_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy localhost:${CODE_SERVER_PORT} {
            transport http {
                read_timeout 3600s
                write_timeout 3600s
            }
        }
    }

    respond 403
}

http://${CODE_SERVER_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_CODE_SERVER
    chown_like "$REPO_CADDY_PATH" "$CADDYFILE"
    echo "    Appended code-server block (https://${CODE_SERVER_HOSTNAME} → localhost:${CODE_SERVER_PORT})"
else
    echo "    Skipped code-server block (CODE_SERVER not set in .env)"
fi

# ── mTLS sync block (:8443) — appended when cert env vars are all set ────────
if [[ -n "${SYNC_TLS_CA:-}" && -n "${SYNC_TLS_CERT:-}" && -n "${SYNC_TLS_KEY:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_MTLS

# mTLS sync transport — accepts inbound sync connections from fleet peers.
# Requires a valid client certificate signed by the fleet CA.
# All verified traffic is proxied through to uvicorn on localhost:8080.
#
# Uses "https://:8443" (HTTPS wildcard on port 8443) so that:
#   - The TLS connection policy has NO SNI filter (catch-all). This is
#     essential because clients connecting by IP don't include SNI in
#     the TLS ClientHello, so named-host policies would never match.
#   - client_auth (require_and_verify) applies to ALL connections on :8443.
#   - HTTP routing has no host matcher either, so all Host headers are served.
https://:8443 {
    tls ${SYNC_TLS_CERT} ${SYNC_TLS_KEY} {
        client_auth {
            mode require_and_verify
            trust_pool file {
                pem_file ${SYNC_TLS_CA}
            }
        }
    }
    reverse_proxy localhost:8080
}
CADDY_MTLS
    chown_like "$REPO_CADDY_PATH" "$CADDYFILE"
    echo "    Appended mTLS :8443 sync block (https://:8443 — SNI-free catch-all)"
else
    echo "    Skipped mTLS :8443 block (SYNC_TLS_CA/CERT/KEY not set — plain HTTP only)"
fi

# ── Syncthing GUI block — appended when SYNCTHING_HOSTNAME is set ─────────────
# The Syncthing GUI binds to loopback:8384 only. This block exposes it over
# HTTPS via the node's syncthing hostname (from SYNCTHING_HOSTNAME in .env).
# header_up sets Host: localhost so Syncthing's built-in host check passes.
# Requires pfSense DNS record: SYNCTHING_HOSTNAME → this node's primary_ip.
if [[ -n "${SYNCTHING_HOSTNAME:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_SYNCTHING

# Syncthing GUI — reverse proxy to the local Syncthing web interface.
# GUI binds to loopback:8384 only; exposed over HTTPS at ${SYNCTHING_HOSTNAME}.
https://${SYNCTHING_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}
    reverse_proxy localhost:8384 {
        header_up Host localhost
    }
}

http://${SYNCTHING_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_SYNCTHING
    chown_like "$REPO_CADDY_PATH" "$CADDYFILE"
    echo "    Appended Syncthing GUI block (https://${SYNCTHING_HOSTNAME})"
else
    echo "    Skipped Syncthing GUI block (SYNCTHING_HOSTNAME not set in .env)"
fi

echo "    Written: $CADDYFILE"
echo ""

# ── Step 5 — Symlink /etc/caddy/Caddyfile → inner repo ──────────────────────
echo "Step 5: Symlinking /etc/caddy/Caddyfile..."
mkdir -p /etc/caddy
if [[ -L /etc/caddy/Caddyfile ]]; then
    CURRENT_TARGET=$(readlink /etc/caddy/Caddyfile)
    if [[ "$CURRENT_TARGET" == "$CADDYFILE" ]]; then
        echo "    Already symlinked: /etc/caddy/Caddyfile → $CADDYFILE"
    else
        ln -sf "$CADDYFILE" /etc/caddy/Caddyfile
        echo "    Updated symlink: /etc/caddy/Caddyfile → $CADDYFILE (was → $CURRENT_TARGET)"
    fi
elif [[ -f /etc/caddy/Caddyfile ]]; then
    echo "    Backing up existing /etc/caddy/Caddyfile → /etc/caddy/Caddyfile.bak"
    mv /etc/caddy/Caddyfile /etc/caddy/Caddyfile.bak
    ln -s "$CADDYFILE" /etc/caddy/Caddyfile
    echo "    Symlinked: /etc/caddy/Caddyfile → $CADDYFILE"
else
    ln -s "$CADDYFILE" /etc/caddy/Caddyfile
    echo "    Symlinked: /etc/caddy/Caddyfile → $CADDYFILE"
fi
echo ""

# ── Step 6 — Validate and (re)start ─────────────────────────────────────────
echo "Step 6: Validating Caddyfile..."
if caddy validate --config "$CADDYFILE" 2>&1 | sed 's/^/    /'; then
    echo -e "    ${GREEN}Valid.${NC}"
else
    echo -e "${RED}Caddyfile validation failed — not starting.${NC}" >&2
    exit 1
fi
echo ""

echo "Step 7: Enabling and starting Caddy..."
systemctl enable caddy
# Note: `caddy reload` uses the admin API (port 2019) which we disable in the
# Caddyfile. Always use systemctl restart instead.
systemctl restart caddy
echo -e "    ${GREEN}Caddy (re)started.${NC}"
echo ""

# ── Step 7 — Health check ────────────────────────────────────────────────────
echo "Step 8: Health check..."
sleep 2
HEALTH_URL="https://${UI_HOST}/health"
HTTP_STATUS=$(curl -sk -o /dev/null -w "%{http_code}" --max-time 5 \
    --cacert "${CERT_CA:-$CERT_FILE}" "$HEALTH_URL" 2>/dev/null || echo "000")
if [[ "$HTTP_STATUS" == "200" ]]; then
    echo -e "    ${GREEN}OK${NC}: $HEALTH_URL → HTTP $HTTP_STATUS"
else
    echo -e "    ${YELLOW}Warning:${NC} $HEALTH_URL → HTTP $HTTP_STATUS"
    echo "    Caddy may still be starting. Check: systemctl status caddy"
fi
echo ""

write_fallback_cache_state \
    "$FALLBACK_CACHE_STATE_FILE" \
    "$FALLBACK_CACHE_MODE" \
    "$FALLBACK_CACHE_MODE" \
    "$FALLBACK_ASSET_VERSION" \
    "$BLUEPRINTS_FALLBACK_GUI_DIR"

# ── Step 8 — Remind about BLUEPRINTS_UI_URL ──────────────────────────────────
CURRENT_UI_URL="${BLUEPRINTS_UI_URL:-}"
HTTPS_UI_URL="https://${UI_HOST}"
if [[ "$CURRENT_UI_URL" != "$HTTPS_UI_URL" ]]; then
    echo -e "${YELLOW}Reminder:${NC} BLUEPRINTS_UI_URL in .env is still:"
    echo "    $CURRENT_UI_URL"
    echo "  Update it to: $HTTPS_UI_URL"
    echo "  Then re-run setup-blueprints.sh to apply the change, and run:"
    echo "    bash .xarta/sync-env-from-xarta-node.sh"
    echo ""
fi

echo -e "${GREEN}Done.${NC}"
