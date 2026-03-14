# .env Template for a New Blueprints Node

Replace every `<PLACEHOLDER>` with values extracted from the LXC config.
See `lxc-conf-guide.md` for the extraction rules.

```bash
cat > /root/xarta-node/.env << 'ENVEOF'
# -----------------------------------------------------------------------------
# Gateway Failover Configuration
# -----------------------------------------------------------------------------
IF_PRIMARY="<primary-vlan-interface>"
IF_SECONDARY="<secondary-vlan-interface>"
GW_PRIMARY="<WAN-A-GATEWAY>"
GW_SECONDARY="<WAN-B-GATEWAY>"
TEST_IP="<connectivity-test-ip>"

# -----------------------------------------------------------------------------
# Auto-update configuration
# -----------------------------------------------------------------------------
REPO_OUTER_PATH="/root/xarta-node"
REPO_INNER_PATH="/root/xarta-node/.xarta"
REPO_CADDY_PATH="/root/xarta-node/<node-local-caddy-repo>"  # each node has its own independent git repo here
GIT_TIMEOUT=5
AUTO_UPDATE_LOG="<log-file-path>"
SERVICE_RESTART_CMD="systemctl restart blueprints-app"

# -----------------------------------------------------------------------------
# SSH / Git Configuration
# -----------------------------------------------------------------------------
SSH_KEY_NAME="<deploy-key-filename>"
GIT_USER_NAME="xarta-node"
GIT_USER_EMAIL="xarta-node@example.com"

# -----------------------------------------------------------------------------
# Blueprints Node Configuration
# -----------------------------------------------------------------------------
# Only the node's own identity stays in .env.
# All fleet peer data (addresses, hostnames, etc.) is loaded from .nodes.json.
BLUEPRINTS_NODE_ID=<HOSTNAME>
BLUEPRINTS_INSTANCE=1
NODES_JSON_PATH=/root/xarta-node/.nodes.json

BLUEPRINTS_DB_DIR=/opt/blueprints/data/db
BLUEPRINTS_GUI_DIR=/root/xarta-node/.xarta/gui
BLUEPRINTS_BACKUP_DIR=/root/xarta-node/.xarta/db-backups

# -----------------------------------------------------------------------------
# TLS / Certificates
# -----------------------------------------------------------------------------
CERTS_DIR=/root/xarta-node/.xarta/.certs
CERT_FILE=/root/xarta-node/.xarta/.certs/<your-cert>.crt
CERT_KEY=/root/xarta-node/.xarta/.certs/<your-cert>.key
CERT_CA=/root/xarta-node/.xarta/.certs/<your-ca>.crt

# -----------------------------------------------------------------------------
# Tailscale
# -----------------------------------------------------------------------------
TAILSCALE_HOSTNAME=<HOSTNAME>
# All /24 subnets across every net interface — see lxc-conf-guide.md
TAILSCALE_ROUTES=<SUBNET-1>/24,<SUBNET-2>/24,<SUBNET-3>/24
TAILSCALE_EXIT_NODE=true
TAILSCALE_ACCEPT_DNS=false
TAILSCALE_LOGIN_SERVER=
TAILSCALE_AUTH_KEY=

# -----------------------------------------------------------------------------
# Caddy
# -----------------------------------------------------------------------------
# REPO_CADDY_PATH (above) must be set and that directory must be git-initialised
# before running setup-caddy.sh — it writes the Caddyfile into that repo.
CADDY_EXTRA_NAMES=<HOSTNAME>.<your-domain>
ENVEOF
```

## Verification

```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "grep -E 'BLUEPRINTS_NODE_ID|NODES_JSON_PATH|TAILSCALE_HOSTNAME' \
   /root/xarta-node/.env"
```

## Notes

- `BLUEPRINTS_NODE_ID` is the only identity key that stays in `.env` per node.
  All fleet peer data comes from `.nodes.json` (distributed via `bp-nodes-push.sh`).
- `CERT_FILE`/`CERT_KEY`/`CERT_CA` are set automatically by `setup-certificates.sh`
  if the correct certs are present in `.xarta/.certs/`. If missing, it falls back
  to a locally-generated self-signed cert — the browser will warn, which is expected.
- `TAILSCALE_ROUTES`: recalculate from the actual LXC conf — don't copy from another
  node unless the interface layout is identical.
