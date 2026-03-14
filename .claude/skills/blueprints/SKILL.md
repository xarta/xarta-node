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
| Dashboard GUI | `.xarta/gui/` (private inner repo) |
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

Push to GitHub → trigger git-sync on any live node:
```bash
curl -X POST http://<any-node>:8080/api/v1/sync/git-pull \
     -H 'Content-Type: application/json' \
     -d '{"scope":"outer"}'
```
All peers receive a `sync_git_outer` queue action → `git pull` → service restart (via `SERVICE_RESTART_CMD` in `.env`).

For GUI updates (`.xarta/gui/`), use `scope: "inner"` → `sync_git_inner`.

## Sync protocol & commit guard

The sync system uses a persistent queue in `sync_queue` (SQLite). Each data write is enqueued for all peer nodes and drained in the background every 1–20 s.

### ⚠️ Blind upsert — no per-row recency check

`_apply_action()` in `routes_sync.py` applies an `INSERT ... ON CONFLICT DO UPDATE SET ...` with **no timestamp comparison**. The last write wins at row level. This is acceptable when only one node writes a given row — but if two nodes independently update the same row, there is no winner guarantee.

### Commit guard (added 2026-03-14)

`routes_sync.py` carries a coarse-payload guard:

- `config.py` caches `COMMIT_HASH` (short hash) and `COMMIT_TS` (unix epoch from `git log -1 --format=%ct`) **once at process startup**.
- Outgoing payloads (`drain.py`) include `source_commit_ts`.
- On `receive_actions()`, if `payload.source_commit_ts < cfg.COMMIT_TS`, all DB-write actions are **rejected with HTTP 409**.
- System actions (`sync_git_outer`, `sync_git_inner`) are **always accepted** regardless of commit age — so a newer node can tell a stale peer to pull.
- When the drain loop receives a 409, it calls `purge_unsent_db_actions(peer_id)` — marks all unsent DB-write queue entries as sent (discards them). System actions in the queue are preserved.

**Why this matters:** during a rolling fleet update (T1 gets new code first, peers still on old), the old-code peers would otherwise push stale-schema data to T1 and overwrite correct rows. The commit guard stops this.

**Startup guarantee:** `_git_pull_and_restart` restarts the process after every git pull, so `COMMIT_TS` always reflects the running code. No cron needed.

### `nodes` table — excluded from sync

`"nodes"` is absent from `_ALLOWED_TABLES` in `routes_sync.py`. The nodes table is populated from `.nodes.json` at startup (`_load_nodes_from_json()`) and must never be overwritten by peer sync. Peer-synced `nodes` entries would carry stale addresses.

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
