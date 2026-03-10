"""
config.py — settings loaded from environment variables.

All per-node identity comes from env vars so the DB stays identity-agnostic
and can be restored to any node unchanged.
"""

import os

# ── Node identity ─────────────────────────────────────────────────────────────
NODE_ID: str = os.environ.get("BLUEPRINTS_NODE_ID", "blueprints-unknown")
NODE_NAME: str = os.environ.get("BLUEPRINTS_NODE_NAME", "unknown")
HOST_MACHINE: str = os.environ.get("BLUEPRINTS_HOST_MACHINE", "unknown")
INSTANCE: str = os.environ.get("BLUEPRINTS_INSTANCE", "1")

# ── Network ───────────────────────────────────────────────────────────────────
# Internal container port (always 8080 inside; host mapping is via compose.yaml)
APP_PORT: int = 8080

# Comma-separated peer base URLs, e.g. "http://node-b.example.com:8080,http://100.x.y.z:8080"
# Set by the operator in .xarta/deploy.env to tell this node about peers at startup.
# Each listed peer will be registered locally and receive a full DB backup on first contact.
_peers_raw: str = os.environ.get("BLUEPRINTS_PEERS", "")
PEER_URLS: list[str] = [p.strip() for p in _peers_raw.split(",") if p.strip()]

# Comma-separated list of origins (scheme+host+port) allowed to make cross-origin
# requests to this API from a browser.  Include every hostname that may serve a page
# with the node-selector embed widget.  Peer node ui_urls are also added automatically
# from the DB at runtime, so registering a new node covers it without editing this list.
# No trailing slashes. Example: "http://mynode.tailnet:8080,http://dashboard.lan"
_cors_raw: str = os.environ.get("BLUEPRINTS_CORS_ORIGINS", "")
CORS_ORIGINS: list[str] = [o.strip().rstrip("/") for o in _cors_raw.split(",") if o.strip()]

# External address other nodes use to reach THIS node's app (e.g. its Tailscale IP:port).
# Stored in the nodes table so peers can route sync traffic back to this node.
# For Docker nodes this defaults to localhost (Caddy handles external routing).
# For bare-systemd nodes set this to the TS IP or LAN IP, e.g. http://100.x.y.z:8080
SELF_ADDRESS: str = os.environ.get("BLUEPRINTS_SELF_ADDRESS", f"http://localhost:{8080}")

# ── Database ──────────────────────────────────────────────────────────────────
DB_DIR: str = os.environ.get("BLUEPRINTS_DB_DIR", "/data/db")
DB_PATH: str = os.path.join(DB_DIR, "blueprints.db")

# Directory where local DB backups are saved.  Empty string = feature disabled.
BACKUP_DIR: str = os.environ.get("BLUEPRINTS_BACKUP_DIR", "")

# ── GUI ───────────────────────────────────────────────────────────────────────
GUI_DIR: str = os.environ.get("BLUEPRINTS_GUI_DIR", "/data/gui")

# Browser-accessible URL for this node — advertised via the /health endpoint so
# the node-selector component can build clickable links without hard-coding URLs.
# Set to the MagicDNS or reverse-proxy URL that browsers use to reach this node.
UI_URL: str = os.environ.get("BLUEPRINTS_UI_URL", "")

# ── Sync ──────────────────────────────────────────────────────────────────────
SYNC_DRAIN_INTERVAL_MIN: int = 1    # seconds — minimum random delay per drain
SYNC_DRAIN_INTERVAL_MAX: int = 20   # seconds — maximum random delay per drain
SYNC_QUEUE_MAX_DEPTH: int = 1000    # per-peer queue overflow threshold
SYNC_BATCH_SIZE: int = 50           # actions posted per drain cycle

# ── Git repos (bare-systemd nodes only) ───────────────────────────────────────
# Paths on the local filesystem where the outer / inner repos are checked out.
# On bare-systemd nodes these point to the xarta-node
# clone and its .xarta private inner repo.
# On Docker nodes these are left empty — git-pull actions become no-ops.
REPO_OUTER_PATH: str = os.environ.get("REPO_OUTER_PATH", "")
REPO_INNER_PATH: str = os.environ.get("REPO_INNER_PATH", "")

# Shell command to restart the blueprints service after a git pull.
# e.g. "systemctl restart blueprints-app" on bare-systemd nodes.
# Leave empty on Docker nodes (image-based deploys — restart not applicable).
SERVICE_RESTART_CMD: str = os.environ.get("SERVICE_RESTART_CMD", "")
