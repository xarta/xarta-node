#!/bin/bash

# setup-tailscale-up.sh
# Runs `tailscale up` using values from .env.
#
# Reads:
#   TAILSCALE_HOSTNAME       — hostname to advertise on the tailnet
#   TAILSCALE_ROUTES         — comma-separated CIDRs to advertise
#   TAILSCALE_EXIT_NODE      — true/false, whether to advertise as exit node
#   TAILSCALE_ACCEPT_DNS     — true/false, whether to accept DNS from control server
#   TAILSCALE_LOGIN_SERVER   — Headscale/custom control URL (blank = official Tailscale)
#   TAILSCALE_AUTH_KEY       — optional pre-auth key for unattended setup
#
# Idempotent — safe to re-run. If Tailscale is already up with the correct
# config, the `tailscale up` command is effectively a no-op.

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

echo "=== Tailscale up ==="
echo ""

# ── Preflight checks ──────────────────────────────────────────────────────────
if ! command -v tailscale >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} tailscale is not installed." >&2
    echo "  Run setup-lxc-failover.sh first to install it." >&2
    exit 1
fi

if [[ -z "${TAILSCALE_HOSTNAME:-}" ]]; then
    echo -e "${RED}Error:${NC} TAILSCALE_HOSTNAME is not set in .env." >&2
    exit 1
fi

if [[ -z "${TAILSCALE_ROUTES:-}" ]]; then
    echo -e "${YELLOW}Warning:${NC} TAILSCALE_ROUTES is not set — no subnet routes will be advertised."
    echo ""
fi

# ── Show current status ────────────────────────────────────────────────────────
echo "Current Tailscale status:"
tailscale status 2>/dev/null | sed 's/^/    /' || echo "    (not connected)"
echo ""

# ── Build tailscale up command ─────────────────────────────────────────────────
TS_ARGS=()
TS_ARGS+=("--hostname=${TAILSCALE_HOSTNAME}")

[[ -n "${TAILSCALE_ROUTES:-}" ]] && TS_ARGS+=("--advertise-routes=${TAILSCALE_ROUTES}")

if [[ "${TAILSCALE_EXIT_NODE:-true}" == "true" ]]; then
    TS_ARGS+=("--advertise-exit-node")
fi

TS_ARGS+=("--accept-dns=${TAILSCALE_ACCEPT_DNS:-false}")

[[ -n "${TAILSCALE_LOGIN_SERVER:-}" ]] && TS_ARGS+=("--login-server=${TAILSCALE_LOGIN_SERVER}")

[[ -n "${TAILSCALE_AUTH_KEY:-}" ]] && TS_ARGS+=("--authkey=${TAILSCALE_AUTH_KEY}")

echo "Running:"
echo -e "    ${CYAN}tailscale up ${TS_ARGS[*]}${NC}"
echo ""

# ── Run tailscale up ───────────────────────────────────────────────────────────
if tailscale up "${TS_ARGS[@]}"; then
    echo ""
    echo -e "${GREEN}Done.${NC}"
    echo ""
    echo "Tailscale status after:"
    tailscale status 2>/dev/null | sed 's/^/    /'
    echo ""
    if [[ -n "${TAILSCALE_ROUTES:-}" ]]; then
        echo -e "${YELLOW}Reminder:${NC} Advertised routes must be approved in your Tailscale/Headscale"
        echo "  admin panel before they become active on other nodes."
        [[ -n "${TAILSCALE_LOGIN_SERVER:-}" ]] && \
            echo "  Control server: ${TAILSCALE_LOGIN_SERVER}"
        echo ""
    fi
else
    EXIT=$?
    echo ""
    echo -e "${RED}tailscale up failed (exit ${EXIT}).${NC}" >&2
    echo "  If this is a new node, a browser login URL may have been printed above." >&2
    echo "  Open that URL to authenticate, then re-run this script." >&2
    exit $EXIT
fi
