#!/bin/bash

# setup-caddy.sh
# Installs Caddy and configures it as a reverse proxy for blueprints-app.
#
# What this script does (idempotent):
#   1. Runs setup-certificates.sh to ensure TLS certs exist.
#   2. Installs Caddy from the official apt repo (if not present).
#   3. Creates a systemd drop-in so Caddy runs as root (required to read
#      certs stored in the private inner repo under /root/).
#   4. Renders a complete candidate and validates its exact bytes.
#   5. Atomically installs it with the existing destination metadata.
#   6. Preserves the service symlink, then enables and (re)starts Caddy.
#   7. Runs a basic health check via HTTPS.
#
# After a successful run:
#   - blueprints-app continues to run on localhost:8080 (unchanged)
#   - Caddy listens on :443 (HTTPS) and :80 (redirects to HTTPS)
#   - Update BLUEPRINTS_UI_URL in .env from http://...:8080 to https://...

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
RENDER_ONLY=false
ENV_EXPLICIT=false
EXISTING_ONLY=false
OUTPUT_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --render-only) RENDER_ONLY=true; shift ;;
        --existing-only) EXISTING_ONLY=true; shift ;;
        --env-file|--output)
            [[ $# -ge 2 && -n "$2" ]] || { echo "Missing argument for $1" >&2; exit 2; }
            case "$1" in --env-file) ENV_FILE="$2"; ENV_EXPLICIT=true ;; --output) OUTPUT_FILE="$2" ;; esac
            shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done
if $RENDER_ONLY; then
    $ENV_EXPLICIT && [[ "$ENV_FILE" = /* && "$OUTPUT_FILE" = /* ]] || { echo "Render requires absolute --env-file and --output paths" >&2; exit 2; }
elif [[ -n "$OUTPUT_FILE" ]]; then
    echo "--output requires --render-only" >&2; exit 2
fi

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
            dirty="$(GIT_OPTIONAL_LOCKS=0 git -C "$repo_path" status --porcelain 2>/dev/null || true)"
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

# Keep deployment-specific explanations in private configuration. Every supplied
# line must remain a comment, so notes cannot introduce operative Caddy directives.
operator_comments() {
    local section="$1" fallback="$2" indent="$3"
    python3 - "$section" "$fallback" "$indent" "${CADDY_OPERATOR_NOTES_FILE:-}" <<'PYCOMMENTS'
import json
import pathlib
import sys

section, fallback, indent, notes_file = sys.argv[1:]
notes = json.loads(pathlib.Path(notes_file).read_text()) if notes_file else {}
if not isinstance(notes, dict) or set(notes) - {"mcp", "model_hook_http"}:
    raise SystemExit("Invalid Caddy operator-notes sections")
for value in notes.values():
    if not isinstance(value, str) or any(line.strip() and not line.lstrip().startswith("#") for line in value.splitlines()):
        raise SystemExit("Caddy operator notes must contain comments only")
for line in notes.get(section, fallback).splitlines():
    print(indent + line.lstrip() if line.strip() else "")
PYCOMMENTS
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

render_config() {
    CADDYFILE="$1"
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
SPLASH_RENDERER_UPSTREAM="${SPLASH_RENDERER_UPSTREAM:-}"
MODEL_HOOK_UPSTREAM="${MODEL_HOOK_UPSTREAM:-}"
MODEL_HOOK_HTTP_HOST="${MODEL_HOOK_HTTP_HOST:-}"

MAIN_SITE_EXTRA_HANDLES=""
if [[ -n "$SPLASH_RENDERER_UPSTREAM" ]]; then
    MAIN_SITE_EXTRA_HANDLES="${MAIN_SITE_EXTRA_HANDLES}
    # Splash renderer - node-local Pretext-powered ASCII splash pages.
    # handle_path strips /splash-renderer before proxying to the stack.
    redir /splash-renderer /splash-renderer/ permanent
    handle_path /splash-renderer* {
        reverse_proxy ${SPLASH_RENDERER_UPSTREAM}
    }
"
fi
if [[ -n "$MODEL_HOOK_UPSTREAM" ]]; then
    MAIN_SITE_EXTRA_HANDLES="${MAIN_SITE_EXTRA_HANDLES}
    # LiteLLM model-hook listener for the node-local sync skill.
    # Exposed on the existing node HTTP entrypoint so the hook host does not
    # need direct access to the raw listener port.
    handle /xarta/health* {
        reverse_proxy ${MODEL_HOOK_UPSTREAM}
    }
    handle /xarta/webhook* {
        reverse_proxy ${MODEL_HOOK_UPSTREAM}
    }
"
fi
CADDY_MCP_ENABLED="${CADDY_MCP_ENABLED:-false}"
case "$CADDY_MCP_ENABLED" in true|false) ;; *) echo "CADDY_MCP_ENABLED must be true or false" >&2; return 1 ;; esac
if [[ "$CADDY_MCP_ENABLED" == true ]]; then
    # Restrict interpolation to a literal path prefix and one upstream address.
    [[ "${CADDY_MCP_PATH_PREFIX:-}" =~ ^/[a-zA-Z0-9_/-]+/$ && "$CADDY_MCP_PATH_PREFIX" != *'//'* ]] || { echo "Invalid MCP path prefix" >&2; return 1; }
    [[ "${CADDY_MCP_UPSTREAM:-}" =~ ^[a-zA-Z0-9.-]+:[0-9]+$ ]] || { echo "Invalid MCP upstream" >&2; return 1; }
    MCP_COMMENTS="$(operator_comments mcp "# MCP service at ${CADDY_MCP_UPSTREAM}, bearer-token-gated at the HTTPS edge.
# The backend relies on Caddy for authentication; keep backend access restricted.
# A host-networked local MCP service and its local API/search calls should use loopback.
# Uses handle (NOT handle_path) so the ${CADDY_MCP_PATH_PREFIX} prefix is passed through
# unstripped to the MCP server's fixed streamable-HTTP path.
# MCP_BEARER_TOKEN is supplied by the caddy.service environment drop-in.
# It remains a runtime placeholder; never embed its value in this file.
# Missing, wrong or empty credentials receive 401, including an empty service token." "    ")"
    MAIN_SITE_EXTRA_HANDLES="${MAIN_SITE_EXTRA_HANDLES}
${MCP_COMMENTS}
    handle ${CADDY_MCP_PATH_PREFIX}* {
        @mcp_authenticated {
            header Authorization \"Bearer {env.MCP_BEARER_TOKEN}\"
            expression \`{env.MCP_BEARER_TOKEN} != \"\"\`
        }
        handle @mcp_authenticated {
            reverse_proxy ${CADDY_MCP_UPSTREAM}
        }
        handle {
            respond 401
        }
    }
"
fi

if [[ "${THIS_NODE_DOCS_BACKUP:-false}" == "true" ]]; then
    MAIN_SITE_EXTRA_HANDLES="${MAIN_SITE_EXTRA_HANDLES}
    # Exact, read-only Sherlock security-report page for trusted Xarta networks.
    # This avoids sending the tray applet through the authenticated code-server UI.
    @sherlock_security_report_internal {
        path /sherlock/security-report
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }
    handle @sherlock_security_report_internal {
        rewrite * /.lone-wolf/docs/sherlock/SHERLOCK-SECURITY-REPORT.txt
        root * /xarta-node
        header Content-Type \"text/plain; charset=utf-8\"
        header Cache-Control \"no-cache, no-store, must-revalidate\"
        file_server
    }
    handle /sherlock/security-report {
        respond 403
    }
"
fi

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

    # SearXNG local stack UI/API on a dedicated path.
    # handle_path strips /searxng before proxying to the stack.
    redir /searxng /searxng/ permanent
    handle_path /searxng* {
        reverse_proxy localhost:18888
    }
${MAIN_SITE_EXTRA_HANDLES}

    # Reverse proxy all traffic to the local uvicorn process.
    # Includes /health, /api/v1/*, and /ui/* (GUI + embed component).
    reverse_proxy localhost:8080
}

# HTTP — redirect all requests to HTTPS.
${HTTP_NAMES} {
    redir https://{host}{uri} permanent
}
CADDY


if [[ -n "$MODEL_HOOK_UPSTREAM" && -n "$MODEL_HOOK_HTTP_HOST" ]]; then
    MODEL_HOOK_HTTP_COMMENTS="$(operator_comments model_hook_http "# HTTP callback route for the model-hook listener.
# Some upstream hook callers can reach the node by direct IP even when DNS is unavailable.
#
# PLAIN-HTTP ALLOWLIST — deny everything except these two callback handlers:
#   /xarta/health*  -> ${MODEL_HOOK_UPSTREAM}  (model-hook health check)
#   /xarta/webhook* -> ${MODEL_HOOK_UPSTREAM}  (model-hook callback)
#
# Everything else on this block is 403. The full Blueprints API, including
# token-exempt document search, remains on the hostname-based HTTPS entrypoint
# (https://${UI_HOST}); clients must trust its CA, for example via NODE_EXTRA_CA_CERTS.
# Any enabled MCP route is bearer-token-gated and lives only on the HTTPS entrypoint." "")"
    cat >> "$CADDYFILE" <<CADDY_MODEL_HOOK_HTTP

${MODEL_HOOK_HTTP_COMMENTS}
http://${MODEL_HOOK_HTTP_HOST} {
    handle /xarta/health* {
        reverse_proxy ${MODEL_HOOK_UPSTREAM}
    }
    handle /xarta/webhook* {
        reverse_proxy ${MODEL_HOOK_UPSTREAM}
    }
    respond 403
}
CADDY_MODEL_HOOK_HTTP
    echo "    Appended model-hook HTTP callback block (http://${MODEL_HOOK_HTTP_HOST} -> ${MODEL_HOOK_UPSTREAM})"
fi

if [[ -n "${MATRIX_SYNAPSE_HOSTNAME:-}" && -n "${MATRIX_SYNAPSE_UPSTREAM:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_MATRIX

# Matrix Synapse client API for phone/agent messaging.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${MATRIX_SYNAPSE_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_client_well_known {
        path /.well-known/matrix/client
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_federation {
        path /_matrix/federation/* /_matrix/key/*
    }

    handle @matrix_client_well_known {
        header Access-Control-Allow-Origin "*"
        respond \`{"m.homeserver":{"base_url":"https://${MATRIX_SYNAPSE_HOSTNAME}"}}\` 200
    }

    handle @matrix_federation {
        respond 404
    }

    handle @xarta_internal {
        reverse_proxy ${MATRIX_SYNAPSE_UPSTREAM}
    }

    respond 403
}

http://${MATRIX_SYNAPSE_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_MATRIX
    if [[ -n "${MATRIX_SYNAPSE_ALIASES:-}" ]]; then
        IFS=',' read -ra MATRIX_SYNAPSE_ALIAS_LIST <<< "$MATRIX_SYNAPSE_ALIASES"
        for alias in "${MATRIX_SYNAPSE_ALIAS_LIST[@]}"; do
            alias="${alias// /}"
            [[ -z "$alias" || "$alias" == "$MATRIX_SYNAPSE_HOSTNAME" ]] && continue
            cat >> "$CADDYFILE" <<CADDY_MATRIX_ALIAS

# Matrix Synapse client API alias for private WebPKI/tailnet clients.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${alias} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_client_well_known {
        path /.well-known/matrix/client
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_federation {
        path /_matrix/federation/* /_matrix/key/*
    }

    handle @matrix_client_well_known {
        header Access-Control-Allow-Origin "*"
        respond \`{"m.homeserver":{"base_url":"https://${alias}"}}\` 200
    }

    handle @matrix_federation {
        respond 404
    }

    handle @xarta_internal {
        reverse_proxy ${MATRIX_SYNAPSE_UPSTREAM}
    }

    respond 403
}

http://${alias} {
    redir https://{host}{uri} permanent
}
CADDY_MATRIX_ALIAS
            echo "    Appended Matrix Synapse alias block (https://${alias} -> ${MATRIX_SYNAPSE_UPSTREAM})"
        done
    fi
    echo "    Appended Matrix Synapse block (https://${MATRIX_SYNAPSE_HOSTNAME} -> ${MATRIX_SYNAPSE_UPSTREAM})"
fi

if [[ -z "${NTFY_HOSTNAME:-}" ]]; then
    if [[ "$UI_HOST" == *.* ]]; then
        NTFY_HOSTNAME="notify.${UI_HOST#*.}"
    else
        NTFY_HOSTNAME="notify.${UI_HOST}"
    fi
fi
NTFY_UPSTREAM="${NTFY_UPSTREAM:-localhost:18889}"
if [[ -n "${NTFY_HOSTNAME:-}" && -n "${NTFY_UPSTREAM:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_NTFY

# ntfy UnifiedPush gateway for private Matrix/mobile notifications and future local pub/sub.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${NTFY_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy ${NTFY_UPSTREAM}
    }

    respond 403
}

http://${NTFY_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_NTFY
    if [[ -n "${NTFY_ALIASES:-}" ]]; then
        IFS=',' read -ra NTFY_ALIAS_LIST <<< "$NTFY_ALIASES"
        for alias in "${NTFY_ALIAS_LIST[@]}"; do
            alias="${alias// /}"
            [[ -z "$alias" || "$alias" == "$NTFY_HOSTNAME" ]] && continue
            cat >> "$CADDYFILE" <<CADDY_NTFY_ALIAS

# ntfy UnifiedPush gateway alias for private WebPKI/tailnet clients.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${alias} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy ${NTFY_UPSTREAM}
    }

    respond 403
}

http://${alias} {
    redir https://{host}{uri} permanent
}
CADDY_NTFY_ALIAS
            echo "    Appended ntfy UnifiedPush alias block (https://${alias} -> ${NTFY_UPSTREAM})"
        done
    fi
    echo "    Appended ntfy UnifiedPush block (https://${NTFY_HOSTNAME} -> ${NTFY_UPSTREAM})"
fi

# ── Vikunja work-memory block ────────────────────────────────────────────────
# Backend binds to loopback only. Caddy exposes it on a private HTTPS hostname
# for local/RFC1918/tailnet clients.
if [[ -z "${VIKUNJA_HOSTNAME:-}" ]]; then
    if [[ "$UI_HOST" == *.* ]]; then
        VIKUNJA_HOSTNAME="projects.${UI_HOST#*.}"
    else
        VIKUNJA_HOSTNAME="projects.${UI_HOST}"
    fi
fi
VIKUNJA_PORT="${VIKUNJA_PORT:-3456}"
cat >> "$CADDYFILE" <<CADDY_VIKUNJA

# Vikunja - private work-memory UI.
# Backend binds to loopback only; Caddy exposes it on the private HTTPS entrypoint.
https://${VIKUNJA_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy localhost:${VIKUNJA_PORT} {
            transport http {
                read_timeout 600s
                write_timeout 600s
            }
        }
    }

    respond 403
}

http://${VIKUNJA_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_VIKUNJA
echo "    Appended Vikunja block (https://${VIKUNJA_HOSTNAME} → localhost:${VIKUNJA_PORT})"

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
    echo "    Appended code-server block (https://${CODE_SERVER_HOSTNAME} → localhost:${CODE_SERVER_PORT})"
else
    echo "    Skipped code-server block (CODE_SERVER not set in .env)"
fi

if [[ -n "${PAPERCLIP_HOSTNAME:-}" && -n "${PAPERCLIP_UPSTREAM:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_PAPERCLIP

# Paperclip — private agent-company orchestration UI.
# Backend binds to loopback only; Caddy exposes it on the private HTTPS entrypoint.
https://${PAPERCLIP_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy ${PAPERCLIP_UPSTREAM} {
            transport http {
                read_timeout 3600s
                write_timeout 3600s
            }
        }
    }

    respond 403
}

http://${PAPERCLIP_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_PAPERCLIP
    echo "    Appended Paperclip block (https://${PAPERCLIP_HOSTNAME} -> ${PAPERCLIP_UPSTREAM})"
fi

# ── Hermes Local dashboard block ────────────────────────────────────────────
# The dashboard process binds to loopback only. Caddy exposes it on a private
# infra hostname for local/RFC1918/tailnet clients and preserves Hermes'
# localhost host-header expectation.
HERMES_LOCAL_DASHBOARD_EXPOSE="${HERMES_LOCAL_DASHBOARD_EXPOSE:-0}"
if [[ "$HERMES_LOCAL_DASHBOARD_EXPOSE" == "1" || "$HERMES_LOCAL_DASHBOARD_EXPOSE" == "true" ]]; then
    if [[ -z "${HERMES_LOCAL_DASHBOARD_HOSTNAME:-}" || -z "${HERMES_LOCAL_DASHBOARD_UPSTREAM:-}" ]]; then
        echo "    Skipped Hermes Local dashboard block (hostname/upstream not set)"
    else
    cat >> "$CADDYFILE" <<CADDY_HERMES_LOCAL

# Hermes Local dashboard — privileged local agent UI.
# Backend binds to loopback only; Caddy exposes it on the private HTTPS entrypoint.
https://${HERMES_LOCAL_DASHBOARD_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        forward_auth 127.0.0.1:8080 {
            uri /api/v1/dashboard-auth/hermes-local/validate
            header_up X-Forwarded-Host {host}
            header_up X-Forwarded-Uri {uri}
            header_up X-Forwarded-Proto {scheme}
            header_up X-Forwarded-Method {method}
        }

        reverse_proxy ${HERMES_LOCAL_DASHBOARD_UPSTREAM} {
            header_up Host localhost
            transport http {
                read_timeout 3600s
                write_timeout 3600s
            }
        }
    }

    respond 403
}

http://${HERMES_LOCAL_DASHBOARD_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_HERMES_LOCAL
    if [[ -n "${HERMES_LOCAL_DASHBOARD_ALIASES:-}" ]]; then
        IFS=',' read -ra HERMES_ALIASES <<< "$HERMES_LOCAL_DASHBOARD_ALIASES"
        for alias in "${HERMES_ALIASES[@]}"; do
            alias="${alias// /}"
            [[ -z "$alias" || "$alias" == "$HERMES_LOCAL_DASHBOARD_HOSTNAME" ]] && continue
            cat >> "$CADDYFILE" <<CADDY_HERMES_ALIAS

# Hermes Local dashboard alias — redirect to preferred per-node hostname.
https://${alias} {
    tls ${CERT_FILE} ${CERT_KEY}
    redir https://${HERMES_LOCAL_DASHBOARD_HOSTNAME}{uri} permanent
}

http://${alias} {
    redir https://${HERMES_LOCAL_DASHBOARD_HOSTNAME}{uri} permanent
}
CADDY_HERMES_ALIAS
            echo "    Appended Hermes Local dashboard alias (https://${alias} -> https://${HERMES_LOCAL_DASHBOARD_HOSTNAME})"
        done
    fi
    echo "    Appended Hermes Local dashboard block (https://${HERMES_LOCAL_DASHBOARD_HOSTNAME} -> ${HERMES_LOCAL_DASHBOARD_UPSTREAM})"
    fi
else
    echo "    Skipped Hermes Local dashboard block (HERMES_LOCAL_DASHBOARD_EXPOSE not enabled)"
fi

# ── Hermes Local SMS webhook bridge ─────────────────────────────────────────
# Public HTTPS terminates at the VPS Traefik edge. The private upstream leg from
# the VPS bridge reaches this HTTP-only Caddy host block and is constrained to
# the expected Twilio route before proxying to Hermes' loopback SMS listener.
HERMES_LOCAL_SMS_WEBHOOK_EXPOSE="${HERMES_LOCAL_SMS_WEBHOOK_EXPOSE:-0}"
if [[ "$HERMES_LOCAL_SMS_WEBHOOK_EXPOSE" == "1" || "$HERMES_LOCAL_SMS_WEBHOOK_EXPOSE" == "true" ]]; then
    if [[ -z "${HERMES_LOCAL_SMS_WEBHOOK_HOSTNAME:-}" \
       || -z "${HERMES_LOCAL_SMS_WEBHOOK_UPSTREAM:-}" \
       || -z "${HERMES_LOCAL_SMS_WEBHOOK_PUBLIC_PATH:-}" \
       || -z "${HERMES_LOCAL_SMS_WEBHOOK_BACKEND_PATH:-}" \
       || -z "${HERMES_LOCAL_SMS_WEBHOOK_ALLOWED_REMOTE_IPS:-}" ]]; then
        echo "    Skipped Hermes Local SMS webhook block (required private env not set)"
    else
    HERMES_LOCAL_SMS_WEBHOOK_MAX_BODY="${HERMES_LOCAL_SMS_WEBHOOK_MAX_BODY:-64KB}"
    cat >> "$CADDYFILE" <<CADDY_HERMES_LOCAL_SMS

# Hermes Local SMS webhook — private upstream leg from the VPS public edge.
# Public URL: https://${HERMES_LOCAL_SMS_WEBHOOK_HOSTNAME}${HERMES_LOCAL_SMS_WEBHOOK_PUBLIC_PATH}
# Hermes backend path remains ${HERMES_LOCAL_SMS_WEBHOOK_BACKEND_PATH}.
http://${HERMES_LOCAL_SMS_WEBHOOK_HOSTNAME} {
    @hermes_sms_webhook {
        remote_ip ${HERMES_LOCAL_SMS_WEBHOOK_ALLOWED_REMOTE_IPS}
        method POST
        path ${HERMES_LOCAL_SMS_WEBHOOK_PUBLIC_PATH}
    }

    handle @hermes_sms_webhook {
        request_body {
            max_size ${HERMES_LOCAL_SMS_WEBHOOK_MAX_BODY}
        }
        rewrite * ${HERMES_LOCAL_SMS_WEBHOOK_BACKEND_PATH}
        reverse_proxy ${HERMES_LOCAL_SMS_WEBHOOK_UPSTREAM} {
            header_up Host localhost
            transport http {
                read_timeout 30s
                write_timeout 30s
            }
        }
    }

    respond 404
}
CADDY_HERMES_LOCAL_SMS
    echo "    Appended Hermes Local SMS webhook block (http://${HERMES_LOCAL_SMS_WEBHOOK_HOSTNAME}${HERMES_LOCAL_SMS_WEBHOOK_PUBLIC_PATH} -> ${HERMES_LOCAL_SMS_WEBHOOK_UPSTREAM}${HERMES_LOCAL_SMS_WEBHOOK_BACKEND_PATH})"
    fi
else
    echo "    Skipped Hermes Local SMS webhook block (HERMES_LOCAL_SMS_WEBHOOK_EXPOSE not enabled)"
fi

# ── Hermes VPS dashboard block ──────────────────────────────────────────────
# The VPS dashboard is reached over a private bridge/proxy path. Caddy remains
# the browser-facing boundary and validates a separate Blueprints dashboard
# session before proxying any Hermes HTML.
HERMES_VPS_DASHBOARD_EXPOSE="${HERMES_VPS_DASHBOARD_EXPOSE:-0}"
if [[ "$HERMES_VPS_DASHBOARD_EXPOSE" == "1" || "$HERMES_VPS_DASHBOARD_EXPOSE" == "true" ]]; then
    if [[ -z "${HERMES_VPS_DASHBOARD_HOSTNAME:-}" || -z "${HERMES_VPS_DASHBOARD_UPSTREAM:-}" ]]; then
        echo "    Skipped Hermes VPS dashboard block (hostname/upstream not set)"
    else
    HERMES_VPS_DASHBOARD_HOST_HEADER="${HERMES_VPS_DASHBOARD_HOST_HEADER:-${HERMES_VPS_DASHBOARD_UPSTREAM%%:*}}"
    cat >> "$CADDYFILE" <<CADDY_HERMES_VPS

# Hermes VPS dashboard — private remote agent UI.
# Browser access is gated by Blueprints dashboard-auth before proxying upstream.
https://${HERMES_VPS_DASHBOARD_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        forward_auth 127.0.0.1:8080 {
            uri /api/v1/dashboard-auth/hermes-vps/validate
            header_up X-Forwarded-Host {host}
            header_up X-Forwarded-Uri {uri}
            header_up X-Forwarded-Proto {scheme}
            header_up X-Forwarded-Method {method}
        }

        reverse_proxy ${HERMES_VPS_DASHBOARD_UPSTREAM} {
            header_up Host ${HERMES_VPS_DASHBOARD_HOST_HEADER}
            transport http {
                read_timeout 3600s
                write_timeout 3600s
            }
        }
    }

    respond 403
}

http://${HERMES_VPS_DASHBOARD_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_HERMES_VPS
    if [[ -n "${HERMES_VPS_DASHBOARD_ALIASES:-}" ]]; then
        IFS=',' read -ra HERMES_VPS_ALIASES <<< "$HERMES_VPS_DASHBOARD_ALIASES"
        for alias in "${HERMES_VPS_ALIASES[@]}"; do
            alias="${alias// /}"
            [[ -z "$alias" || "$alias" == "$HERMES_VPS_DASHBOARD_HOSTNAME" ]] && continue
            cat >> "$CADDYFILE" <<CADDY_HERMES_VPS_ALIAS

# Hermes VPS dashboard alias — redirect to preferred hostname.
https://${alias} {
    tls ${CERT_FILE} ${CERT_KEY}
    redir https://${HERMES_VPS_DASHBOARD_HOSTNAME}{uri} permanent
}

http://${alias} {
    redir https://${HERMES_VPS_DASHBOARD_HOSTNAME}{uri} permanent
}
CADDY_HERMES_VPS_ALIAS
            echo "    Appended Hermes VPS dashboard alias (https://${alias} -> https://${HERMES_VPS_DASHBOARD_HOSTNAME})"
        done
    fi
    echo "    Appended Hermes VPS dashboard block (https://${HERMES_VPS_DASHBOARD_HOSTNAME} -> ${HERMES_VPS_DASHBOARD_UPSTREAM})"
    fi
else
    echo "    Skipped Hermes VPS dashboard block (HERMES_VPS_DASHBOARD_EXPOSE not enabled)"
fi

# ── Optional shared/friends Matrix Synapse block ─────────────────────────────
# The shared VPS homeserver is reached over the generic VPS service bridge.
# Thunderbird-1 Caddy remains the browser/client-facing HTTPS boundary and
# keeps the same internal/private source policy as the local Synapse hostname.
if [[ -n "${MATRIX_SHARED_HOSTNAME:-}" && -n "${MATRIX_SHARED_UPSTREAM:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_MATRIX_SHARED

# Matrix Synapse shared/friends server — VPS backend over private bridge.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${MATRIX_SHARED_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_client_well_known {
        path /.well-known/matrix/client
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_federation {
        path /_matrix/federation/* /_matrix/key/*
    }

    handle @matrix_client_well_known {
        header Access-Control-Allow-Origin "*"
        respond \`{"m.homeserver":{"base_url":"https://${MATRIX_SHARED_HOSTNAME}"}}\` 200
    }

    handle @matrix_federation {
        respond 404
    }

    handle @xarta_internal {
        reverse_proxy ${MATRIX_SHARED_UPSTREAM}
    }

    respond 403
}

http://${MATRIX_SHARED_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_MATRIX_SHARED
    if [[ -n "${MATRIX_SHARED_ALIASES:-}" ]]; then
        IFS=',' read -ra MATRIX_SHARED_ALIAS_LIST <<< "$MATRIX_SHARED_ALIASES"
        for alias in "${MATRIX_SHARED_ALIAS_LIST[@]}"; do
            alias="${alias// /}"
            [[ -z "$alias" || "$alias" == "$MATRIX_SHARED_HOSTNAME" ]] && continue
            cat >> "$CADDYFILE" <<CADDY_MATRIX_SHARED_ALIAS

# Matrix Synapse shared/friends alias for private WebPKI/tailnet clients.
# Internal-only by source address: RFC1918 LAN/VLAN, Tailscale CGNAT, and local.
https://${alias} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_client_well_known {
        path /.well-known/matrix/client
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    @matrix_federation {
        path /_matrix/federation/* /_matrix/key/*
    }

    handle @matrix_client_well_known {
        header Access-Control-Allow-Origin "*"
        respond \`{"m.homeserver":{"base_url":"https://${alias}"}}\` 200
    }

    handle @matrix_federation {
        respond 404
    }

    handle @xarta_internal {
        reverse_proxy ${MATRIX_SHARED_UPSTREAM}
    }

    respond 403
}

http://${alias} {
    redir https://{host}{uri} permanent
}
CADDY_MATRIX_SHARED_ALIAS
            echo "    Appended shared Matrix Synapse alias block (https://${alias} -> ${MATRIX_SHARED_UPSTREAM})"
        done
    fi
    echo "    Appended shared Matrix Synapse block (https://${MATRIX_SHARED_HOSTNAME} -> ${MATRIX_SHARED_UPSTREAM})"
else
    echo "    Skipped shared Matrix Synapse block (MATRIX_SHARED_HOSTNAME/MATRIX_SHARED_UPSTREAM not set)"
fi

# ── Optional remote SearXNG block ────────────────────────────────────────────
# Site-specific hostnames and bridge-service addresses live in ignored/private
# config. The public script only appends this block when both values are set.
if [[ -n "${SEARXNG_VPS_HOSTNAME:-}" && -n "${SEARXNG_VPS_UPSTREAM:-}" ]]; then
    cat >> "$CADDYFILE" <<CADDY_SEARXNG_VPS

# Remote SearXNG — private web UI/API routed through a site-local bridge.
https://${SEARXNG_VPS_HOSTNAME} {
    tls ${CERT_FILE} ${CERT_KEY}

    @xarta_internal {
        remote_ip 127.0.0.1/32 ::1 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 100.64.0.0/10
    }

    handle @xarta_internal {
        reverse_proxy ${SEARXNG_VPS_UPSTREAM}
    }

    respond 403
}

http://${SEARXNG_VPS_HOSTNAME} {
    redir https://{host}{uri} permanent
}
CADDY_SEARXNG_VPS
    echo "    Appended remote SearXNG block (https://${SEARXNG_VPS_HOSTNAME} -> ${SEARXNG_VPS_UPSTREAM})"
else
    echo "    Skipped remote SearXNG block (SEARXNG_VPS_HOSTNAME/SEARXNG_VPS_UPSTREAM not set)"
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
    echo "    Appended Syncthing GUI block (https://${SYNCTHING_HOSTNAME})"
else
    echo "    Skipped Syncthing GUI block (SYNCTHING_HOSTNAME not set in .env)"
fi


}

if $RENDER_ONLY; then
    # This branch precedes every installer, live-file, service and cache write.
    [[ ! -e "$OUTPUT_FILE" && ! -L "$OUTPUT_FILE" ]] || { echo "Output already exists" >&2; exit 1; }
    (umask 077; render_config "$OUTPUT_FILE")
    exit 0
fi

if $EXISTING_ONLY; then
    command -v caddy >/dev/null
    for cert_path in "${CERT_FILE:-}" "${CERT_KEY:-}" "${SYNC_TLS_CA:-}" "${SYNC_TLS_CERT:-}" "${SYNC_TLS_KEY:-}"; do
        [[ -n "$cert_path" && -r "$cert_path" ]] || { echo "Missing configured certificate prerequisite" >&2; exit 1; }
    done
    [[ "$(systemctl show caddy --property=User --value)" == root && "$(systemctl show caddy --property=Group --value)" == root ]] || { echo "Unexpected Caddy identity" >&2; exit 1; }
    systemctl is-active --quiet caddy
    systemctl is-enabled --quiet caddy
else
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

fi

[[ -n "${REPO_CADDY_PATH:-}" && -d "$REPO_CADDY_PATH" ]] || { echo "Missing Caddy destination directory" >&2; exit 1; }
LIVE_CADDYFILE="$REPO_CADDY_PATH/Caddyfile"
SYSTEM_CADDYFILE="${CADDY_SYSTEM_CONFIG:-/etc/caddy/Caddyfile}"
if $EXISTING_ONLY; then
    [[ -f "$LIVE_CADDYFILE" && ! -L "$LIVE_CADDYFILE" && -L "$SYSTEM_CADDYFILE" && "$(readlink -f "$SYSTEM_CADDYFILE")" == "$(readlink -f "$LIVE_CADDYFILE")" ]] || { echo "Unexpected Caddyfile/symlink state" >&2; exit 1; }
fi
# Confirm the existing service's credential interface without reading it into the shell.
if [[ "${CADDY_MCP_ENABLED:-false}" == true ]]; then
    python3 - "$(systemctl show caddy --property=MainPID --value)" <<'PYMCP'
import pathlib, sys
pid = int(sys.argv[1])
if pid <= 1 or not any(v.startswith(b'MCP_BEARER_TOKEN=') and v.split(b'=', 1)[1].strip() for v in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')):
    raise SystemExit('Existing Caddy service has no nonempty MCP credential')
PYMCP
fi
CANDIDATE="$(mktemp --suffix=.caddy)"
trap 'rm -f -- "$CANDIDATE"' EXIT
render_config "$CANDIDATE"
echo "Validating complete staged Caddyfile..."
caddy validate --adapter caddyfile --config "$CANDIDATE"
# Preserve destination metadata (including ACL/xattrs), and atomically promote
# the exact validated bytes. No persistent recovery payload is created here.
python3 - "$CANDIDATE" "$LIVE_CADDYFILE" "$REPO_CADDY_PATH" <<'PYINSTALL'
import os, pathlib, shutil, tempfile, signal
def interrupted(signum, frame):
    raise SystemExit(128 + signum)
for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
    signal.signal(signum, interrupted)
candidate, target, owner_ref = map(pathlib.Path, __import__('sys').argv[1:])
if target.is_symlink():
    raise SystemExit('Refusing a symlink at the configured destination')
fd, staged = tempfile.mkstemp(prefix='.Caddyfile.install.', dir=target.parent)
try:
    os.close(fd)
    if target.exists():
        metadata = target.stat()
        shutil.copy2(target, staged)
    else:
        metadata = owner_ref.stat()
        os.chmod(staged, 0o644)
    os.chown(staged, metadata.st_uid, metadata.st_gid)
    with open(candidate, 'rb') as src, open(staged, 'wb') as dst:
        shutil.copyfileobj(src, dst)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(staged, target)
    parent_fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(parent_fd)
    finally:
        os.close(parent_fd)
finally:
    if os.path.exists(staged):
        os.unlink(staged)
PYINSTALL
CADDYFILE="$LIVE_CADDYFILE"
echo "    Written: $CADDYFILE"
echo ""

if [[ -L "$SYSTEM_CADDYFILE" ]]; then
    [[ "$(readlink -f "$SYSTEM_CADDYFILE")" == "$(readlink -f "$CADDYFILE")" ]] || { echo "Unexpected system Caddyfile target" >&2; exit 1; }
elif [[ -e "$SYSTEM_CADDYFILE" ]]; then
    echo "Existing system configuration requires an owner-managed recovery provision" >&2; exit 1
else
    mkdir -p "$(dirname "$SYSTEM_CADDYFILE")"
    ln -s "$CADDYFILE" "$SYSTEM_CADDYFILE"
fi

echo "Step 7: Enabling and starting Caddy..."
if ! $EXISTING_ONLY; then systemctl enable caddy; fi
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
