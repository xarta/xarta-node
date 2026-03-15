#!/usr/bin/env bash
# setup-firewall.sh — configure iptables INPUT chain for xarta-node fleet LXCs.
#
# What this script does (idempotent):
#   1. Ensures nf_conntrack kernel module is loaded.
#   2. Creates a custom XARTA_INPUT chain in the filter table.
#   3. Populates that chain with the minimal allowed-inbound ruleset:
#        - loopback (lo) — unrestricted
#        - ESTABLISHED / RELATED return traffic
#        - TCP 22   (SSH)
#        - TCP 80   (Caddy HTTP → HTTPS redirect)
#        - TCP 443  (Caddy HTTPS)
#        - UDP 41641 (Tailscale / WireGuard direct connections)
#   4. Inserts a jump to XARTA_INPUT at position 1 of the INPUT chain
#      (if not already present), so our rules run before any other rules.
#   5. Sets the default INPUT policy to DROP.
#   6. Saves with netfilter-persistent (installed by setup-lxc-failover.sh).
#
# What this script deliberately does NOT change:
#   - FORWARD chain policy / rules  — left as-is so Tailscale exit node
#     NAT and subnet routing continue to work.
#   - nat table POSTROUTING rules   — managed by setup-lxc-failover.sh.
#   - mangle table MSS clamping     — managed by setup-lxc-failover.sh.
#   - IPv6 (ip6tables)              — not handled; see ASSUMPTIONS.md.
#
# Prerequisites:
#   - Run setup-lxc-failover.sh first (installs iptables-persistent /
#     netfilter-persistent and sets up IP forwarding + NAT).
#
# Safe to re-run.  The XARTA_INPUT chain is flushed and rebuilt each time.
# The INPUT jump and default policy are idempotent.
#
# CAUTION: After enabling the DROP policy, the node is no longer reachable
# on any port except those listed above.  Test connectivity (SSH + HTTPS)
# before saving / deploying fleet-wide.

set -euo pipefail

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

if ! command -v iptables >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} iptables not found." >&2
    echo "  Run setup-lxc-failover.sh first to install iptables-persistent." >&2
    exit 1
fi

if ! command -v netfilter-persistent >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} netfilter-persistent not found." >&2
    echo "  Run setup-lxc-failover.sh first to install iptables-persistent." >&2
    exit 1
fi

echo "=== Firewall setup ==="
echo ""

# ── Step 1 — Kernel module ────────────────────────────────────────────────────
echo "Step 1: Loading nf_conntrack kernel module..."
modprobe nf_conntrack 2>/dev/null || true
echo "    ok"
echo ""

# ── Step 2 — XARTA_INPUT chain ────────────────────────────────────────────────
echo "Step 2: Rebuilding XARTA_INPUT chain..."

# Create the chain if it doesn't exist yet; otherwise flush it for a clean rebuild.
if ! iptables -L XARTA_INPUT >/dev/null 2>&1; then
    iptables -N XARTA_INPUT
    echo -e "    ${CYAN}created${NC}: XARTA_INPUT chain"
else
    iptables -F XARTA_INPUT
    echo -e "    ${CYAN}flushed${NC}: XARTA_INPUT chain (rebuild)"
fi

# ── Inbound allow rules ───────────────────────────────────────────────────────
# Loopback — always allow all local traffic.
iptables -A XARTA_INPUT -i lo -j ACCEPT
echo "    added: lo → ACCEPT"

# Established / related — allow return traffic for outbound connections.
iptables -A XARTA_INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
echo "    added: ESTABLISHED,RELATED → ACCEPT"

# SSH — fleet management, git pull, node-to-node SSH.
iptables -A XARTA_INPUT -p tcp --dport 22 -j ACCEPT
echo "    added: TCP 22 (SSH) → ACCEPT"

# HTTP — Caddy redirect to HTTPS (port 80 → 443).
iptables -A XARTA_INPUT -p tcp --dport 80 -j ACCEPT
echo "    added: TCP 80 (HTTP/Caddy redirect) → ACCEPT"

# HTTPS — Caddy reverse proxy (Blueprints GUI + API).
iptables -A XARTA_INPUT -p tcp --dport 443 -j ACCEPT
echo "    added: TCP 443 (HTTPS/Caddy) → ACCEPT"

# Tailscale WireGuard — direct peer connections.
# Without this, Tailscale falls back to slower DERP relay.
# Essential for exit node performance.
iptables -A XARTA_INPUT -p udp --dport 41641 -j ACCEPT
echo "    added: UDP 41641 (Tailscale/WireGuard) → ACCEPT"
echo ""

# ── Step 3 — Jump from INPUT → XARTA_INPUT ────────────────────────────────────
echo "Step 3: Ensuring INPUT → XARTA_INPUT jump..."
if ! iptables -C INPUT -j XARTA_INPUT 2>/dev/null; then
    # Insert at position 1 so our rules run before anything else in INPUT
    # (Tailscale's ts-input chain will insert itself at position 1 on its own
    # startup, which is fine — it runs, returns for our allowed traffic, then
    # XARTA_INPUT handles port gating and the default DROP handles everything else).
    iptables -I INPUT 1 -j XARTA_INPUT
    echo -e "    ${CYAN}inserted${NC}: INPUT -j XARTA_INPUT (position 1)"
else
    echo "    already present: INPUT -j XARTA_INPUT"
fi
echo ""

# ── Step 4 — Default DROP policy ─────────────────────────────────────────────
echo "Step 4: Setting INPUT default policy to DROP..."
iptables -P INPUT DROP
echo -e "    ${GREEN}set${NC}: INPUT policy → DROP"
echo ""

# ── Step 5 — Save rules ───────────────────────────────────────────────────────
echo "Step 5: Saving rules with netfilter-persistent..."
netfilter-persistent save
echo -e "    ${GREEN}saved${NC}"
echo ""

# ── Summary ───────────────────────────────────────────────────────────────────
echo "=== Current INPUT chain ==="
iptables -L INPUT -n -v --line-numbers
echo ""
echo "=== Current XARTA_INPUT chain ==="
iptables -L XARTA_INPUT -n -v --line-numbers
echo ""
echo -e "${GREEN}Done.${NC}"
echo ""
echo -e "${YELLOW}IMPORTANT — test connectivity before deploying fleet-wide:${NC}"
echo "  1. Verify SSH still works from another terminal."
echo "  2. Verify HTTPS (Blueprints GUI) still reachable."
echo "  3. Verify Tailscale peers can still connect: tailscale ping <peer>"
echo "  4. Only then: commit, push, and run fleet-pull scripts."
