# .env Template for a New Blueprints Node

Replace every `<PLACEHOLDER>` with values extracted from the LXC config.
See `lxc-conf-guide.md` for the extraction rules.

```bash
cat > /root/xarta-node/.env << 'ENVEOF'
# -----------------------------------------------------------------------------
# Gateway Failover Configuration
# -----------------------------------------------------------------------------
IF_PRIMARY="net4"
IF_SECONDARY="net3"
GW_PRIMARY="<WAN-A-GATEWAY>"
GW_SECONDARY="<WAN-B-GATEWAY>"
TEST_IP="1.1.1.1"

# -----------------------------------------------------------------------------
# Auto-update configuration
# -----------------------------------------------------------------------------
REPO_OUTER_PATH="/root/xarta-node"
REPO_INNER_PATH="/root/xarta-node/.xarta"
GIT_TIMEOUT=5
AUTO_UPDATE_LOG="/var/log/auto-update.log"
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
BLUEPRINTS_NODE_ID=<HOSTNAME>
BLUEPRINTS_NODE_NAME=<HOSTNAME>
BLUEPRINTS_INSTANCE=1
BLUEPRINTS_HOST_MACHINE=<HOSTNAME>

BLUEPRINTS_DB_DIR=/opt/blueprints/data/db
BLUEPRINTS_GUI_DIR=/root/xarta-node/.xarta/gui
BLUEPRINTS_BACKUP_DIR=/root/xarta-node/.xarta/db-backups

# Browser-facing HTTPS URL for this node
BLUEPRINTS_UI_URL=https://<HOSTNAME>.<your-tailnet>.ts.net

# All fleet nodes — include the new node here too
BLUEPRINTS_CORS_ORIGINS=https://existing-node-1.<your-tailnet>.ts.net,https://existing-node-1.<your-domain>,https://existing-node-2.<your-tailnet>.ts.net,https://existing-node-2.<your-domain>,https://<HOSTNAME>.<your-tailnet>.ts.net,https://<HOSTNAME>.<your-domain>

# This node's own Tailscale IP:port — how peers reach us
BLUEPRINTS_SELF_ADDRESS=http://<TAILSCALE-IP>:8080

# One established fleet node as bootstrap peer — DB will sync automatically on first contact
# After onboarding, expand this to all fleet nodes
BLUEPRINTS_PEERS=http://<EXISTING-NODE-TS-IP>:8080

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
CADDY_EXTRA_NAMES=<HOSTNAME>.<your-domain>
ENVEOF
```

## Verification

```bash
ssh -i /root/.ssh/<deploy-key> root@<TARGET-IP> \
  "grep -E 'BLUEPRINTS_NODE_ID|BLUEPRINTS_SELF_ADDRESS|BLUEPRINTS_UI_URL|TAILSCALE_HOSTNAME' \
   /root/xarta-node/.env"
```

## Notes

- `BLUEPRINTS_PEERS` starts with one existing node's IP. After onboarding,
  update **all** nodes' PEERS to include the new node (see `fleet-registry.md`).
- `CERT_FILE`/`CERT_KEY`/`CERT_CA` are set automatically by `setup-certificates.sh`
  if the correct certs are present in `.xarta/.certs/`. If missing, it falls back
  to a locally-generated self-signed cert — the browser will warn, which is expected.
- `TAILSCALE_ROUTES`: recalculate from the actual LXC conf — don't copy from another
  node unless the interface layout is identical.
