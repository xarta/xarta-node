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
| GUI assets | `.xarta/gui/` (private inner repo) |
| DB | `/opt/blueprints/data/db/` (not in git) |
| Venv | `/opt/blueprints/venv/` (not in git) |
| Node identity / peers / secrets | `.xarta/` env files (gitignored) |
| Systemd service template | `blueprints-app/blueprints-app.service.template` |

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

## Key pitfalls (learned the hard way)

**Gen guard** — `receive_restore` rejects backups where `sender_gen <= my_gen` when `integrity_ok=true`. A healthy node with data must never be overwritten by a fresh empty node. The `X-Blueprints-Gen` header must be set on every restore POST.

**SELF_ADDRESS** — bare-systemd nodes need `BLUEPRINTS_SELF_ADDRESS=http://<ts-ip>:8080` in `.env`. Without it the node registers itself as `localhost:8080` and peers can't reach it.

**Re-self-register after restore** — `_self_register()` must be called after `_boot_catchup()` applies a restore, otherwise the node's own identity (especially `addresses`) is lost.

**GUI is private** — GUI assets live in `.xarta/gui/` (private inner repo). They are never committed to this public repo. Never.

## .env required vars

```dotenv
BLUEPRINTS_NODE_ID=<unique-node-id>
BLUEPRINTS_NODE_NAME=<human-name>
BLUEPRINTS_SELF_ADDRESS=http://<tailscale-ip>:8080
BLUEPRINTS_PEERS=http://<peer-ts-ip>:8080
BLUEPRINTS_DB_DIR=/opt/blueprints/data/db
BLUEPRINTS_GUI_DIR=<path-to-xarta-node>/.xarta/gui
REPO_OUTER_PATH=<path-to-xarta-node>
REPO_INNER_PATH=<path-to-xarta-node>/.xarta
SERVICE_RESTART_CMD=systemctl restart blueprints-app
```

## ⛔ Public repo rules

This repo is public. Every file outside `.xarta/` must contain zero infrastructure-specific details — no IP addresses, hostnames, tailnet names, LXC IDs, port numbers, node names tied to real machines. Use placeholders in all examples.
