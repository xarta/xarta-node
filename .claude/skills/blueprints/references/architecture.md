# Blueprints — Architecture Reference

## What it is

A distributed, peer-to-peer service index. Every node holds an identical SQLite database of services, machines, and infrastructure links. Nodes sync changes to each other via a FIFO action queue over Tailscale. There is no central controller and no originator — all nodes are equal.

## Node anatomy (bare-systemd LXC)

```
/root/xarta-node/             ← public repo (app code, setup scripts)
  blueprints-app/
    app/                      ← FastAPI application
    requirements.txt
    blueprints-app.service.template
  setup-blueprints.sh         ← one-time bootstrap
  auto-update.sh              ← cron/boot git pull

/root/xarta-node/.xarta/      ← private inner repo (gitignored in outer)
  gui/                        ← HTML/CSS/JS dashboard
  env.<node-id>               ← node identity (operator-managed)

/opt/blueprints/
  venv/                       ← Python virtualenv
  data/db/                    ← SQLite database (WAL mode)
  .env                        ← copied from .xarta/ env file by setup-blueprints.sh

/etc/systemd/system/
  blueprints-app.service      ← generated from template by setup-blueprints.sh
```

## Sync protocol

### Layer 1 — full backup (bootstrap)
- `GET  /api/v1/sync/export` — exports current DB as a zip
- `POST /api/v1/sync/restore` — receives and atomically applies a zip backup
- Used for: new node onboarding (boot-catchup), crash recovery, queue overflow

### Layer 2 — incremental actions
- `POST /api/v1/sync/actions` — receives a batch of INSERT/UPDATE/DELETE row actions
- Per-peer FIFO queue in SQLite, drained every 1–20s (random jitter)
- Batch size: 50 actions. Overflow threshold: 1000 pending → tries full backup first, then falls back to batched actions if restore is rejected (for example HTTP 409 generation guard)

### Generation counter
Every write increments a `gen` counter in the DB metadata. Used to:
- Detect which node has newer data during boot-catchup
- Guard against a fresh node overwriting an established one (HTTP 409 if `sender_gen <= my_gen` and node is healthy)

### Git-sync actions
Three system action types that flow through the same queue:
- `sync_git_outer` — triggers `git pull` on `REPO_OUTER_PATH` (app code)
- `sync_git_non_root` — triggers `git pull` on `REPO_NON_ROOT_PATH` (non-root public repo)
- `sync_git_inner` — triggers `git pull` on `REPO_INNER_PATH` (private repo)

Triggered via `POST /api/v1/sync/git-pull` with `{"scope": "outer"|"non_root"|"inner"|"both"|"all"}`. The receiving node runs the pull locally. `outer` and `inner` restart the service when configured; `non_root` updates in place without a service restart. The Fleet Nodes GUI orchestrates a staged `outer` → `non_root` → `inner` update with commit verification across all nodes; the private `inner` stage gets a longer settle window and multiple verification attempts because Blueprints is expected to restart there, and transient `HTTP 502`/`503` or fetch failures during that restart window are treated as timing noise unless commit mismatches persist. These actions are fire-and-forget at the sync layer — they don't touch the DB and don't re-propagate.

## Boot sequence

1. `_self_register()` — upserts own node_id, display_name, addresses into local DB
2. `_boot_catchup()` — queries all known peers for their gen; if any peer has higher gen (or local DB is degraded), pulls a full backup from the best peer and applies it
3. After restore: `_self_register()` runs again (the restored DB doesn't know about this node)
4. `_bootstrap_peers()` — contacts `BLUEPRINTS_PEERS` env-listed peers: registers them locally, sends them our full DB backup as their first copy
5. Queue drain loop starts

## Node introduction (operator action)

New nodes do not register themselves with peers. The operator does it:
```bash
# Tell peer-A about new-node (peer-A will push its DB to new-node):
curl -X POST http://<peer-a>:8080/api/v1/nodes \
     -H 'Content-Type: application/json' \
     -d '{"node_id":"<new-node-id>","display_name":"<name>","addresses":["http://<new-node-ts-ip>:8080"]}'
```
Or set `BLUEPRINTS_PEERS` in new-node's `.env` before running `setup-blueprints.sh` — the app contacts listed peers on startup.

## Auto-updates via cron / systemd timer

`auto-update.sh` (deployed to `/usr/local/bin/` by `setup-lxc-failover.sh`) runs on boot. It pulls both repos. If new commits land on the outer repo and `SERVICE_RESTART_CMD` is set, it restarts the service automatically.

## Docker nodes (transitional)

Docker stacks running the same app code with a Tailscale sidecar container + Caddy can participate in the same sync network as bare-systemd LXC nodes. `BLUEPRINTS_SELF_ADDRESS` defaults to `localhost` on Docker nodes (Caddy handles external routing). Docker nodes are transitional — the target architecture is all bare-systemd LXC nodes.
