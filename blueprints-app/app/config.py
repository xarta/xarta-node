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

# Sync peers: other nodes' primary (VLAN) sync addresses
PEER_URLS: list[str] = [
    f"http://{n['primary_ip']}:{n['sync_port']}" for n in _peer_nodes
]

# CORS: all active nodes' primary and secondary (tailnet) HTTPS URLs
CORS_ORIGINS: list[str] = (
    [f"https://{n['primary_hostname']}" for n in _active_nodes]
    + [f"https://{n['tailnet_hostname']}" for n in _active_nodes]
)

# Fleet node IDs — used by ssh-targets key routing
FLEET_LXC_NAMES: list[str] = [n["node_id"] for n in _active_nodes]

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

# ── Sync ──────────────────────────────────────────────────────────────────────
SYNC_DRAIN_INTERVAL_MIN: int = 1    # seconds — minimum random delay per drain
SYNC_DRAIN_INTERVAL_MAX: int = 20   # seconds — maximum random delay per drain
SYNC_QUEUE_MAX_DEPTH: int = 1000    # per-peer queue overflow threshold
SYNC_BATCH_SIZE: int = 50           # actions posted per drain cycle

# ── Git repos (bare-systemd nodes only) ───────────────────────────────────────
REPO_OUTER_PATH: str = os.environ.get("REPO_OUTER_PATH", "")
REPO_INNER_PATH: str = os.environ.get("REPO_INNER_PATH", "")

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

