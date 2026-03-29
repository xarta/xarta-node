"""
config.py — application settings.

Node identity and all fleet peer information are loaded from .nodes.json
(path in NODES_JSON_PATH env var). This file is gitignored and fleet-
distributed via bp-nodes-push.sh.

The only per-node key that stays in .env is BLUEPRINTS_NODE_ID — it tells
this instance which entry in .nodes.json is "self".

The app will not start if .nodes.json is missing or BLUEPRINTS_NODE_ID
does not match any entry in the nodes array.
"""

import json
import logging
import os

log = logging.getLogger(__name__)

# ── Node identity (stays in .env) ─────────────────────────────────────────────
NODE_ID: str = os.environ.get("BLUEPRINTS_NODE_ID", "")
if not NODE_ID:
    raise RuntimeError(
        "BLUEPRINTS_NODE_ID is not set — add it to .env before starting the app"
    )

INSTANCE: str = os.environ.get("BLUEPRINTS_INSTANCE", "1")

# ── Load .nodes.json ──────────────────────────────────────────────────────────
_default_json = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    ".nodes.json",
)
NODES_JSON_PATH: str = os.environ.get("NODES_JSON_PATH", _default_json)

if not os.path.isfile(NODES_JSON_PATH):
    raise RuntimeError(
        f".nodes.json not found at {NODES_JSON_PATH!r}. "
        "Create it (see bp-nodes-validate.sh) or set NODES_JSON_PATH in .env."
    )

with open(NODES_JSON_PATH) as _f:
    _raw: dict = json.load(_f)

NODES_DATA: list[dict] = _raw.get("nodes", [])

_self_node = next(
    (n for n in NODES_DATA if n.get("node_id") == NODE_ID),
    None,
)
if _self_node is None:
    raise RuntimeError(
        f"BLUEPRINTS_NODE_ID={NODE_ID!r} not found in {NODES_JSON_PATH!r}. "
        "Check the node_id values in .nodes.json match the .env on this node."
    )

# ── Derived identity (from self node in JSON) ─────────────────────────────────
NODE_NAME: str = _self_node["display_name"]
HOST_MACHINE: str = _self_node["host_machine"]
UI_URL: str = f"https://{_self_node['primary_hostname']}"
SELF_ADDRESS: str = f"http://{_self_node['primary_ip']}:{_self_node['sync_port']}"

# ── Derived fleet lists (from all active nodes in JSON) ───────────────────────
_active_nodes: list[dict] = [n for n in NODES_DATA if n.get("active", False)]
_peer_nodes: list[dict] = [n for n in _active_nodes if n["node_id"] != NODE_ID]


def _peer_sync_urls(peer: dict, self_node: dict) -> list[str]:
    """Return the ordered sync URL list for a peer.

    The primary LAN address (primary_ip) is always first when present — it is
    the direct on-premises LAN path and doesn't traverse any external network.

    Tailnet IP is appended only when both this node and the peer belong to the
    same tailnet (same string in the 'tailnet' field).  Nodes on different
    tailnets cannot reach each other's tailnet IPs directly.

    If a peer has no primary_ip (future remote VPS node) the tailnet URL is
    the only entry.  If neither address is available the list is empty and the
    drain will skip that peer until configuration is corrected.
    """
    scheme = peer.get("sync_scheme", "http")
    port   = peer.get("sync_port", 8080)
    urls: list[str] = []

    # Primary LAN address — always try first if the peer has one
    if peer.get("primary_ip"):
        urls.append(f"{scheme}://{peer['primary_ip']}:{port}")

    # Tailnet — only reachable when both nodes are on the same tailnet
    if (
        peer.get("tailnet_ip")
        and peer.get("tailnet")
        and peer["tailnet"] == self_node.get("tailnet")
    ):
        urls.append(f"{scheme}://{peer['tailnet_ip']}:{port}")

    return urls


# PEER_SYNC_URLS: per-peer ordered list of sync addresses (primary LAN first,
# tailnet fallback when both nodes share the same tailnet).
# drain.py iterates this list and stops at the first successful connection.
PEER_SYNC_URLS: dict[str, list[str]] = {
    n["node_id"]: _peer_sync_urls(n, _self_node)
    for n in _peer_nodes
}

# PEER_URLS: flat list of every configured sync address across all peers.
# Kept for backward-compatibility (trust checks, logging, fleet-peer detection).
# Primary address is always first per peer; tailnet address follows where applicable.
PEER_URLS: list[str] = [url for urls in PEER_SYNC_URLS.values() for url in urls]

# SELF_NODE / NODE_MAP / PEER_NODES — used by smart forwarding in Phase 2.
SELF_NODE: dict = _self_node
NODE_MAP: dict[str, dict] = {n["node_id"]: n for n in NODES_DATA}
PEER_NODES: list[dict] = _peer_nodes

# CORS: all active nodes' primary and secondary (tailnet) HTTPS URLs
CORS_ORIGINS: list[str] = (
    [f"https://{n['primary_hostname']}" for n in _active_nodes]
    + [f"https://{n['tailnet_hostname']}" for n in _active_nodes]
)

# Fleet node IDs — used by ssh-targets key routing
FLEET_LXC_NAMES: list[str] = [n["node_id"] for n in _active_nodes]

# ── mTLS sync transport ──────────────────────────────────────────────────────
# Paths to fleet CA cert, this node's cert, and this node's private key.
# Leave unset (or empty) to fall back to plain HTTP sync.
SYNC_TLS_CA:   str = os.environ.get("SYNC_TLS_CA",   "")
SYNC_TLS_CERT: str = os.environ.get("SYNC_TLS_CERT", "")
SYNC_TLS_KEY:  str = os.environ.get("SYNC_TLS_KEY",  "")

# ── Authentication ────────────────────────────────────────────────────────────
# 256-bit HMAC secrets (hex). Keep these in .env — never commit.
# GUI/REST token: generated by the browser from BLUEPRINTS_API_SECRET.
# Sync token: used by drain.py when calling peer /api/v1/sync/* endpoints.
API_SECRET:  str = os.environ.get("BLUEPRINTS_API_SECRET",  "")
SYNC_SECRET: str = os.environ.get("BLUEPRINTS_SYNC_SECRET", "")
# Comma-separated CIDRs allowed to reach the API.
# Empty default = allowlist disabled (all IPs pass through).
# Set in .env, e.g.: BLUEPRINTS_ALLOWED_NETWORKS=192.168.x.0/24,100.64.0.0/10
ALLOWED_NETWORKS_RAW: str = os.environ.get("BLUEPRINTS_ALLOWED_NETWORKS", "")

# ── Network ───────────────────────────────────────────────────────────────────
# Internal container port (always 8080 inside; host mapping is via Caddy)
APP_PORT: int = 8080

# ── Database ──────────────────────────────────────────────────────────────────
DB_DIR: str = os.environ.get("BLUEPRINTS_DB_DIR", "/data/db")
DB_PATH: str = os.path.join(DB_DIR, "blueprints.db")

# Directory where local DB backups are saved.  Empty string = feature disabled.
BACKUP_DIR: str = os.environ.get("BLUEPRINTS_BACKUP_DIR", "")

# ── GUI ───────────────────────────────────────────────────────────────────────
GUI_DIR: str = os.environ.get("BLUEPRINTS_GUI_DIR", "/data/gui")

# ── SeekDB (server mode) ─────────────────────────────────────────────────────
_required_seekdb_keys = (
    "SEEKDB_HOST",
    "SEEKDB_PORT",
    "SEEKDB_DB",
    "SEEKDB_USER",
    "SEEKDB_PASSWORD",
)
for _k in _required_seekdb_keys:
    if _k not in os.environ:
        raise RuntimeError(f"{_k} is not set — add it to .env before starting the app")

SEEKDB_HOST: str = os.environ["SEEKDB_HOST"]
try:
    SEEKDB_PORT: int = int(os.environ["SEEKDB_PORT"])
except ValueError as _exc:
    raise RuntimeError("SEEKDB_PORT must be an integer in .env") from _exc
SEEKDB_DB: str = os.environ["SEEKDB_DB"]
SEEKDB_USER: str = os.environ["SEEKDB_USER"]
SEEKDB_PASSWORD: str = os.environ["SEEKDB_PASSWORD"]

# ── Sync ──────────────────────────────────────────────────────────────────────
SYNC_DRAIN_INTERVAL_MIN: int = 1    # seconds — minimum random delay per drain
SYNC_DRAIN_INTERVAL_MAX: int = 20   # seconds — maximum random delay per drain
SYNC_QUEUE_MAX_DEPTH: int = 1000    # per-peer queue overflow threshold
SYNC_BATCH_SIZE: int = 50           # actions posted per drain cycle

# ── Git repos (bare-systemd nodes only) ───────────────────────────────────────
REPO_OUTER_PATH: str = os.environ.get("REPO_OUTER_PATH", "")
REPO_INNER_PATH: str = os.environ.get("REPO_INNER_PATH", "")
# Docs root: the directory whose sub-path 'docs/' holds markdown files.
# Defaults to REPO_INNER_PATH for backward compatibility.
# Point this at the node-local repo root so docs resolve outside the private git repo.
DOCS_ROOT: str = os.environ.get("DOCS_ROOT", "")

# Shell command to restart the blueprints service after a git pull.
SERVICE_RESTART_CMD: str = os.environ.get("SERVICE_RESTART_CMD", "")

# ── Git commit identity (computed once at import, cached for process lifetime) ─
# After git pull + restart, the new process will re-compute these values.
import subprocess as _sp

def _get_commit_info() -> tuple[str | None, int]:
    """Return (short_hash, unix_epoch) of HEAD in the outer repo.

    Falls back to (None, 0) if the repo is missing or git fails.
    """
    if not REPO_OUTER_PATH or not os.path.isdir(os.path.join(REPO_OUTER_PATH, ".git")):
        return (None, 0)
    try:
        h = _sp.check_output(
            ["git", "-C", REPO_OUTER_PATH, "rev-parse", "--short", "HEAD"],
            stderr=_sp.DEVNULL, text=True,
        ).strip()
        ts = int(_sp.check_output(
            ["git", "-C", REPO_OUTER_PATH, "log", "-1", "--format=%ct"],
            stderr=_sp.DEVNULL, text=True,
        ).strip())
        return (h, ts)
    except Exception:
        return (None, 0)

COMMIT_HASH, COMMIT_TS = _get_commit_info()
log.info("commit guard: hash=%s ts=%d", COMMIT_HASH, COMMIT_TS)

# ── SSH probe key configuration ────────────────────────────────────────────────
# Maps a stable id → human label + env-var that holds the key path.
# The API resolves actual paths from env vars at request time — clients never
# supply a path directly (prevents path-injection attacks).
KEY_CONFIGS: dict[str, dict] = {
    "xarta_node": {
        "label": "xarta-node Fleet LXCs",
        "env_var": "XARTA_NODE_SSH_KEY",
    },
    "lxc": {
        "label": "Other LXCs",
        "env_var": "LXC_SSH_KEY",
    },
    "vm": {
        "label": "QEMU VMs",
        "env_var": "VM_SSH_KEY",
    },
    "citadel": {
        "label": "Citadel VM",
        "env_var": "CITADEL_SSH_KEY",
    },
    "proxmox": {
        "label": "Proxmox PVE Hosts",
        "env_var": "PROXMOX_SSH_KEY",
    },
    "pfsense": {
        "label": "pfSense Firewall",
        "env_var": "PFSENSE_SSH_KEY",
    },
}

# ── Certificate configuration ──────────────────────────────────────────────────
# CERTS_DIR: where cert/key files live.  Defaults to REPO_INNER_PATH/.certs
# (set by setup-certificates.sh).  The directory is gitignored in the private
# inner repo — cert material never enters any public or private git repo.
CERTS_DIR: str = os.environ.get(
    "CERTS_DIR",
    os.path.join(REPO_INNER_PATH, ".certs") if REPO_INNER_PATH else "",
)

# Maps a stable id → metadata for each cert/key slot.
# - env_var:      env var that holds the absolute path to the file.
# - default_name: used when env var is not set; resolved relative to CERTS_DIR.
# - mode:         file permission bits (0o600 for keys, 0o644 for certs/CAs).
# - kind:         "cert" | "key" | "ca"
# - group:        "caddy" | "mtls" — used by the GUI for visual grouping.
# - description:  shown in the GUI.
CERT_CONFIGS: dict[str, dict] = {
    "cert_ca": {
        "label": "Caddy CA Certificate",
        "env_var": "CERT_CA",
        "default_name": "local-ca.crt",
        "mode": 0o644,
        "kind": "ca",
        "group": "caddy",
        "description": (
            "CA that signed the Caddy TLS certificate. "
            "Installed into the system trust store on upload."
        ),
    },
    "caddy_cert": {
        "label": "Caddy Server Cert",
        "env_var": "CERT_FILE",
        "default_name": "caddy-server.crt",
        "mode": 0o644,
        "kind": "cert",
        "group": "caddy",
        "description": (
            "TLS certificate served by Caddy on port 443. "
            "Replace to update the node HTTPS certificate."
        ),
    },
    "caddy_key": {
        "label": "Caddy Server Key",
        "env_var": "CERT_KEY",
        "default_name": "caddy-server.key",
        "mode": 0o600,
        "kind": "key",
        "group": "caddy",
        "description": "Private key for the Caddy TLS certificate.",
    },
    "sync_tls_ca": {
        "label": "Fleet CA (mTLS)",
        "env_var": "SYNC_TLS_CA",
        "default_name": "fleet-ca.crt",
        "mode": 0o644,
        "kind": "ca",
        "group": "mtls",
        "description": (
            "Fleet Certificate Authority for mTLS node-to-node sync (port 8443). "
            "Installed into the system trust store on upload."
        ),
    },
    "sync_tls_cert": {
        "label": "mTLS Node Cert",
        "env_var": "SYNC_TLS_CERT",
        "default_name": f"{NODE_ID}.crt",
        "mode": 0o644,
        "kind": "cert",
        "group": "mtls",
        "description": (
            "Node certificate for mTLS sync transport (port 8443). "
            "Must include primary LAN IP and Tailscale IP SANs."
        ),
    },
    "sync_tls_key": {
        "label": "mTLS Node Key",
        "env_var": "SYNC_TLS_KEY",
        "default_name": f"{NODE_ID}.key",
        "mode": 0o600,
        "kind": "key",
        "group": "mtls",
        "description": "Private key for the mTLS node certificate.",
    },
}

