---
name: blueprints
description: Work on the Blueprints distributed service index running on xarta-node LXC nodes — setup, deployment, sync protocol, onboarding new nodes, updating app code, and troubleshooting. Use when the user mentions blueprints, service index, node sync, boot catchup, gen guard, sync queue, git-sync actions, or onboarding a new node.
---

# Blueprints — Distributed Service Index (xarta-node)

See [references/architecture.md](references/architecture.md) for the full system design.

## Quick orientation

| What | Where |
|------|-------|
| App code | `blueprints-app/` (this repo, public) |
| Embed component | `gui-embed/` (this repo, public) |
| Dashboard GUI (primary) | `gui-fallback/` (this repo, public) |
| DB | `/opt/blueprints/data/db/` (not in git) |
| Venv | `/opt/blueprints/venv/` (not in git) |
| Node identity / peers / secrets | `.env` (gitignored) |
| Systemd service template | `blueprints-app/blueprints-app.service.template` |

## Setup scripts

| Script | Purpose |
|--------|---------|
| `setup-lxc-failover.sh` | Gateway failover, iptables, Tailscale install, SSH/git, auto-update |
| `setup-ssh-and-git.sh` | SSH keypair config, known_hosts, git identity |
| `setup-certificates.sh` | TLS cert detection/generation, CA system trust install |
| `setup-tailscale-up.sh` | Runs `tailscale up` from `.env` vars |
| `setup-caddy.sh` | Installs Caddy, writes Caddyfile from `.env`, symlinks, restarts |
| `setup-blueprints.sh` | Venv, symlinks, systemd service install + start |
| `bp-backup.sh` | Create a timestamped local DB backup (offline-safe; `--api` flag available) |
| `bp-restore.sh` | Restore a local backup interactively (`--force` bumps gen above peers) |

All scripts are idempotent and read from `.env` — no baked-in values.

## Onboarding a new node

```bash
git clone git@github.com:xarta/xarta-node.git
cd xarta-node
git clone <private-repo-url> .xarta   # needs SSH key with access
cp .env.example .env                  # edit with node identity + peers
bash setup-blueprints.sh
```

`setup-blueprints.sh` is idempotent. It creates `/opt/blueprints/data/db`, builds the venv, installs and starts the systemd service. No originator needed — runs entirely on the node.

After setup, introduce the node to a peer (operator step):
```bash
curl -X POST http://<peer-ip>:8080/api/v1/nodes \
     -H 'Content-Type: application/json' \
     -d '{"node_id":"<this-node-id>","display_name":"<name>","addresses":["http://<this-ts-ip>:8080"]}'
```
Boot-catchup runs automatically — the new node pulls the full DB from the peer.

## Distributing app updates

Push to GitHub, then update the fleet with the SSH-to-loopback scripts:
```bash
bash bp-nodes-push.sh
bash /root/xarta-node/.xarta/.claude/skills/fleet-pull/scripts/fleet-pull-public.sh
bash /root/xarta-node/.xarta/.claude/skills/fleet-pull/scripts/fleet-pull-non-root.sh
bash /root/xarta-node/.xarta/.claude/skills/fleet-pull/scripts/fleet-pull-private.sh
```
The scripts SSH to each node and trigger loopback-only `git-pull` actions safely.

For targeted operations, `POST /api/v1/sync/git-pull` still works when called on a local node via loopback. The API supports `outer`, `non_root`, `inner`, `both`, and `all`. The Fleet Nodes GUI uses staged pulls with a 10-second settle window, commit verification across all nodes, and one retry per repo stage.

## Sync protocol & commit guard

The sync system uses a persistent queue in `sync_queue` (SQLite). Each data write is enqueued for all peer nodes and drained in the background every 1–20 s.

### ⚠️ Blind upsert — no per-row recency check

`_apply_action()` in `routes_sync.py` applies an `INSERT ... ON CONFLICT DO UPDATE SET ...` with **no timestamp comparison**. The last write wins at row level. This is acceptable when only one node writes a given row — but if two nodes independently update the same row, there is no winner guarantee.

### Commit guard (added 2026-03-14)

`routes_sync.py` carries a coarse-payload guard:

- `config.py` caches `COMMIT_HASH` (short hash) and `COMMIT_TS` (unix epoch from `git log -1 --format=%ct`) **once at process startup**.
- Outgoing payloads (`drain.py`) include `source_commit_ts`.
- On `receive_actions()`, if `payload.source_commit_ts < cfg.COMMIT_TS`, all DB-write actions are **rejected with HTTP 409**.
- System actions (`sync_git_outer`, `sync_git_non_root`, `sync_git_inner`) are **always accepted** regardless of commit age — so a newer node can tell a stale peer to pull.
- When the drain loop receives a 409, it calls `purge_unsent_db_actions(peer_id)` — marks all unsent DB-write queue entries as sent (discards them). System actions in the queue are preserved.

**Why this matters:** during a rolling fleet update (T1 gets new code first, peers still on old), the old-code peers would otherwise push stale-schema data to T1 and overwrite correct rows. The commit guard stops this.

**Startup guarantee:** `_git_pull_and_restart` restarts the process after every git pull, so `COMMIT_TS` always reflects the running code. No cron needed.

### `nodes` table — excluded from sync

`"nodes"` is absent from `_ALLOWED_TABLES` in `routes_sync.py`. The nodes table is populated from `.nodes.json` at startup (`_load_nodes_from_json()`) and must never be overwritten by peer sync. Peer-synced `nodes` entries would carry stale addresses.

## Multi-address failover (Phase 1, 2026-03-16)

`drain.py` supports multiple addresses per peer — primary LAN first, tailnet as fallback. On each drain cycle it iterates the address list, stopping at the first successful POST. A `ConnectError` on one URL causes it to try the next; a 409 commit-guard rejection returns immediately (that's a code mismatch, not a connectivity issue).

### PEER_SYNC_URLS

`config.py` exports `PEER_SYNC_URLS: dict[str, list[str]]` — a per-peer ordered address list derived from the `nodes` table (populated from `.nodes.json` at startup). The helper `_peer_sync_urls(peer, self_node)` builds it:

1. **Primary LAN address** (`http://<primary_ip>:<sync_port>`) — always first
2. **Tailnet address** (`http://<tailnet_ip>:<sync_port>` or `http://<tailnet_hostname>:<sync_port>`) — appended only when `peer["tailnet"] == self_node["tailnet"]`

`PEER_URLS` (flat union of all addresses across all peers) is retained for backward-compatible trust checks in `boot_catchup` and `routes_nodes` fleet-peer detection.

### queue.py helpers

| Function | Returns | Used by |
|---|---|---|
| `get_peer_urls(node_id)` | `list[str]` — ordered URL list | `drain.py` multi-address loop |
| `get_peer_url(node_id)` | `str` — first URL only | Legacy callers (retained) |

### Failover probe diagnostic

`GET /api/v1/sync/failover-probe` — exercises the failover logic without a real secondary address. For each peer it builds a synthetic two-URL list: a guaranteed-dead port (refused immediately) followed by the real configured URL. Response per peer: `dead_status`, `dead_ms`, `real_status`, `real_ms`, `failover_ok`. Top-level `all_passed` flag.

Visible in the self-diagnostics GUI under **Sync — Failover Logic (simulated VPS probe)**.

## GUID dedup + smart forwarding (Phase 2, 2026-03-19)

Prevents duplicate action application when the same write is relayed via multiple paths (needed for cross-tailnet relay to future remote VPS nodes). Zero overhead on the current 6-node on-prem fleet.

### How it works

1. **GUID assigned at origin** — `enqueue_for_all_peers()` generates one `uuid4().hex` per write and stamps it on every per-peer queue row. `enqueue()` accepts an optional `guid` parameter; if omitted it generates a new one.
2. **GUID travels in the payload** — `drain.py` includes `guid` in each action dict sent to peers.
3. **Dedup at receive** — `receive_actions()` does `INSERT INTO sync_seen_guids` for each action. If `IntegrityError` (PRIMARY KEY collision) → action was already applied, silently skipped. Empty GUID = legacy pre-Phase-2 peer → dedup skipped, applied normally (backward compatible).
4. **Smart forwarding** — after applying, `_forward_actions()` re-enqueues the action (same GUID) for any peers the originator cannot reach but this node can. Currently a no-op for all 6 on-prem nodes (all share the LAN). Activates automatically when a remote VPS node is added.
5. **3-day sliding window** — `_maybe_cleanup_guids()` in `drain.py` deletes `sync_seen_guids` rows older than 3 days, rate-limited to once per hour.

### New schema

| Object | Change |
|---|---|
| `sync_seen_guids` | New table: `guid TEXT PRIMARY KEY`, `received_at INTEGER` |
| `sync_queue.guid` | New column: `TEXT DEFAULT ''` (migration in `_run_migrations`) |

### New config exports

`config.py` exports `SELF_NODE: dict`, `NODE_MAP: dict[str, dict]`, `PEER_NODES: list[dict]` — used by `_can_reach_directly()` and `_forward_actions()` in `routes_sync.py`.

### Reachability helper

```python
def _can_reach_directly(source: dict, target: dict) -> bool:
    # LAN: both have primary_ip  → share on-prem network
    if source.get("primary_ip") and target.get("primary_ip"):
        return True
    # Tailnet: both carry same non-empty tailnet string
    if source.get("tailnet") and target.get("tailnet") and source["tailnet"] == target["tailnet"]:
        return True
    return False
```

### GUID probe diagnostic

`GET /api/v1/sync/guid-probe` — three sub-tests:
1. **GUID dedup (DB layer)**: inserts a synthetic GUID twice; verifies second is deduplicated via IntegrityError.
2. **Fleet topology**: runs `_can_reach_directly()` on every peer; shows LAN/tailnet/unreachable per peer.
3. **Mock VPS relay**: simulates a phantom source with no `primary_ip`; reports which relay peers would be chosen.

Visible in the self-diagnostics GUI under **Sync — GUID Dedup & Forwarding (Phase 2 probe)**.

### Backward compatibility

| Scenario | Behaviour |
|---|---|
| New node → old node | Old node ignores `guid` field (extra JSON key) → applies normally |
| Old node → new node | `guid=""` → dedup skipped, applied normally (legacy path) |
| New node → new node | Full GUID dedup + smart forwarding active |

## Backup and restore

Local (per-node) DB backups are stored as `.db.tar.gz` files in `BLUEPRINTS_BACKUP_DIR` (typically `.xarta/db-backups/`, committed to the private repo). The `sync_queue` table is always stripped from backups.

```bash
bash bp-backup.sh              # create backup directly (works when app is down)
bash bp-backup.sh --api        # create backup via HTTP API
bash bp-restore.sh             # interactive selection + restore
bash bp-restore.sh <file> --force  # bump gen above all peers before restore
```

HTTP API:
```
GET  /api/v1/backup                      → list backups
POST /api/v1/backup                      → create backup
POST /api/v1/backup/restore/{filename}   → restore (add ?force=true to propagate to peers)
```

**Normal restore:** gen reverts to backup-time gen → lower than peers → peers push their current state back at next sync (overwriting the restore). Intentional — use for local recovery only.

**Force restore:** queries all peers for max gen, sets restored DB gen to max+1, so this node wins the gen guard and pushes the restored state to all peers. Disaster recovery only.

## Caddy (HTTPS reverse proxy)

`setup-caddy.sh` installs Caddy, writes `$REPO_CADDY_PATH/Caddyfile` (a **node-local** git repo, separate from the fleet's shared private repo) from `.env` values, symlinks it to `/etc/caddy/Caddyfile`, and restarts. The Caddyfile is node-specific (hostnames differ per node), so each node keeps its own independent repo. Key points:
- Uses `auto_https off` + `admin off` — always `systemctl restart caddy`, never `caddy reload`
- Caddy runs as root (drop-in at `/etc/systemd/system/caddy.service.d/run-as-root.conf`) to read certs from `/root/`
- Primary hostname comes from `BLUEPRINTS_UI_URL`; additional hostnames from `CADDY_EXTRA_NAMES` (comma-separated)
- `/ui/*` → uvicorn (blueprints-app) via `reverse_proxy localhost:8080`
- `/fallback-ui/*` → Caddy `file_server` direct from `gui-fallback/` (frozen public GUI copy, never changes)
- `/` → 301 to `/ui/`

Node-local Caddy and Dockge references:
- `/xarta-node/.lone-wolf/docs/caddy/CADDY.md` (infrastructure-level Caddy model)
- `/xarta-node/.lone-wolf/docs/web-design/CADDY.md` (GUI route ownership/cache context)
- `/xarta-node/.lone-wolf/docs/dockge/README.md` (per-stack docs index)
- `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md` (Open Notebook planning, Syncthing/Obsidian notes)

These are optional node-local references for deployed nodes and may be
unavailable in standalone/public-only checkouts.

## API Authentication (TOTP middleware)

`app/middleware_auth.py` — `AuthMiddleware` (Starlette `BaseHTTPMiddleware`) sits before CORS. Two protection layers:

1. **IP allowlist** — client IP (read from `X-Forwarded-For` set by Caddy, then raw socket) must fall within `BLUEPRINTS_ALLOWED_NETWORKS` (comma-separated CIDRs in `.env`).
2. **TOTP token** — `X-API-Token` header must be `HMAC-SHA256(secret_hex, str(unix_time // 5))`. 5-second windows, ±1 skew (~15 s total validity). Implemented in `app/auth.py`.

### Secret routing — two secrets with distinct scopes

| Secret env var | Used for | Called by |
|---|---|---|
| `BLUEPRINTS_SYNC_SECRET` | `POST /api/v1/sync/actions` and `POST /api/v1/sync/restore` **only** | `drain.py` node-to-node writes |
| `BLUEPRINTS_API_SECRET` | **All other routes** | Browser GUI, operator scripts, fleet-pull scripts |

**Fallback rule:** All routes *except* the two sync write endpoints also accept `SYNC_SECRET` as a valid token. This means `drain.py` only needs one secret regardless of which endpoint it hits.

### Exempt paths (no token required at all)

`/health`, `/ui/*`, `/favicon.ico` — always pass through, no token needed.

### Loopback exemption

`127.0.0.1` and `::1` bypass **all** checks unconditionally — needed for local `curl` calls from shell scripts like `bp-nodes-push.sh`.

### GUI token flow

- Browser stores `BLUEPRINTS_API_SECRET` in `localStorage['blueprints_api_secret']`
- `apiFetch()` wrapper in `index.html` computes a fresh TOTP via Web Crypto and injects `X-API-Token` on every request
- **Only the derived token travels on the wire** — the raw secret never leaves the browser
- On 401, `apiFetch()` opens the API key modal

### `blueprints-node-selector.js` embed

The embed uses `window.apiFetch || fetch` for `/api/v1/nodes` — picks up the parent page's `apiFetch` if available, falls back to raw `fetch` for standalone use.

### fleet-pull scripts

`fleet-pull-public.sh`, `fleet-pull-non-root.sh`, and `fleet-pull-private.sh` use SSH-to-loopback on each peer and call `http://127.0.0.1:8080/api/v1/sync/git-pull` or run the equivalent local git pull workflow. Because the sync API call is loopback-local on the peer, no TOTP token is required in the fleet-pull scripts.

### Onboarding new nodes

After generating secrets, SSH-append both to each node's `.env`:
```bash
ssh root@<node> "echo 'BLUEPRINTS_API_SECRET=<hex>' >> /root/xarta-node/.env"
ssh root@<node> "echo 'BLUEPRINTS_SYNC_SECRET=<hex>' >> /root/xarta-node/.env"
```
Secrets are **not** distributed by git — they must be pushed manually per node.

 — `receive_restore` rejects backups where `sender_gen <= my_gen` when `integrity_ok=true`. A healthy node with data must never be overwritten by a fresh empty node. The `X-Blueprints-Gen` header must be set on every restore POST.

**SELF_ADDRESS** — bare-systemd nodes need `BLUEPRINTS_SELF_ADDRESS=http://<ts-ip>:8080` in `.env`. Without it the node registers itself as `localhost:8080` and peers can't reach it.

**Re-self-register after restore** — `_self_register()` must be called after `_boot_catchup()` applies a restore, otherwise the node's own identity (especially `addresses`) is lost.

**GUI split** — Three GUI locations, all independent:
- `.xarta/gui/` (private) — active dashboard, served by uvicorn at `/ui/`. Will be overhauled.
- `gui-fallback/` (public) — frozen copy of the dashboard at the time of initial Caddy setup, served by Caddy `file_server` at `/fallback-ui/`. Not updated when the private GUI changes.
- `gui-embed/` (public) — node-selector web component, shared by both GUIs via symlink.

`setup-blueprints.sh` creates two symlinks: `$BLUEPRINTS_GUI_DIR/embed → gui-embed/` and `gui-fallback/embed → gui-embed/`. `StaticFiles` uses `follow_symlink=True`.

## .env required vars

```dotenv
# Node identity
BLUEPRINTS_NODE_ID=<unique-node-id>
BLUEPRINTS_NODE_NAME=<human-name>
BLUEPRINTS_INSTANCE=1
BLUEPRINTS_HOST_MACHINE=<host-machine>
BLUEPRINTS_SELF_ADDRESS=http://<tailscale-ip>:8080
BLUEPRINTS_UI_URL=https://<hostname>   # update to https:// once Caddy is set up
BLUEPRINTS_PEERS=http://<peer-ts-ip>:8080

# Paths
BLUEPRINTS_DB_DIR=/opt/blueprints/data/db
BLUEPRINTS_GUI_DIR=<path-to-xarta-node>/.xarta/gui
BLUEPRINTS_BACKUP_DIR=<path-to-xarta-node>/.xarta/db-backups
REPO_OUTER_PATH=<path-to-xarta-node>
REPO_INNER_PATH=<path-to-xarta-node>/.xarta
REPO_CADDY_PATH=<path-to-node-local-caddy-repo>  # node-specific repo; required by setup-caddy.sh

# Service restart (used by auto-update and git-sync)
SERVICE_RESTART_CMD=systemctl restart blueprints-app

# CORS — seed origins; peer node ui_urls are added automatically at runtime
BLUEPRINTS_CORS_ORIGINS=https://<node-1>,https://<node-2>

# TLS — populated by setup-certificates.sh
CERTS_DIR=<path-to-xarta-node>/.xarta/.certs
CERT_FILE=<path-to-cert.crt>
CERT_KEY=<path-to-cert.key>
CERT_CA=<path-to-ca.crt>

# Caddy — used by setup-caddy.sh
CADDY_EXTRA_NAMES=<extra-hostname-1>,<extra-hostname-2>  # optional additional hostnames

# API Authentication — 256-bit hex HMAC secrets (generate with: openssl rand -hex 32)
# Browser stores API_SECRET in localStorage; only derived TOTP tokens travel on wire.
# Secrets are NOT in git — push manually to each node's .env via SSH.
BLUEPRINTS_API_SECRET=<64-hex-chars>   # used by GUI and all non-sync-write routes
BLUEPRINTS_SYNC_SECRET=<64-hex-chars>  # used by drain.py for /sync/actions and /sync/restore
BLUEPRINTS_ALLOWED_NETWORKS=<cidr1>,<cidr2>,<cidr3>  # LAN VLANs + tailnet range

# Tailscale — used by setup-tailscale-up.sh
TAILSCALE_HOSTNAME=<node-hostname>
TAILSCALE_ROUTES=<cidr1>,<cidr2>
TAILSCALE_EXIT_NODE=true
TAILSCALE_ACCEPT_DNS=false
TAILSCALE_LOGIN_SERVER=<headscale-url-or-blank>
TAILSCALE_AUTH_KEY=
```

## ⛔ Public repo rules

This repo is public. Every file outside `.xarta/` must contain zero infrastructure-specific details — no IP addresses, hostnames, tailnet names, LXC IDs, port numbers, node names tied to real machines. Use placeholders in all examples.
