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
| `setup-blueprints.sh` | Venv, symlinks, systemd service install + start |
| `setup-certificates.sh` | TLS cert detection/generation, CA system trust install |
| `setup-tailscale-up.sh` | Runs `tailscale up` from `.env` vars |

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

## Key pitfalls (learned the hard way)

**Gen guard** — `receive_restore` rejects backups where `sender_gen <= my_gen` when `integrity_ok=true`. A healthy node with data must never be overwritten by a fresh empty node. The `X-Blueprints-Gen` header must be set on every restore POST.

**SELF_ADDRESS** — bare-systemd nodes need `BLUEPRINTS_SELF_ADDRESS=http://<ts-ip>:8080` in `.env`. Without it the node registers itself as `localhost:8080` and peers can't reach it.

**Re-self-register after restore** — `_self_register()` must be called after `_boot_catchup()` applies a restore, otherwise the node's own identity (especially `addresses`) is lost.

**GUI split** — The embed component (`gui-embed/`) is in the **public** repo and served at `/ui/embed/...`. The dashboard (`index.html`) stays private in `.xarta/gui/`. `setup-blueprints.sh` creates the symlink `$BLUEPRINTS_GUI_DIR/embed → <repo>/gui-embed/` at deploy time. `StaticFiles` uses `follow_symlink=True`.

## .env required vars

```dotenv
# Node identity
BLUEPRINTS_NODE_ID=<unique-node-id>
BLUEPRINTS_NODE_NAME=<human-name>
BLUEPRINTS_INSTANCE=1
BLUEPRINTS_HOST_MACHINE=<host-machine>
BLUEPRINTS_SELF_ADDRESS=http://<tailscale-ip>:8080
BLUEPRINTS_UI_URL=http://<hostname-or-ip>:8080
BLUEPRINTS_PEERS=http://<peer-ts-ip>:8080

# Paths
BLUEPRINTS_DB_DIR=/opt/blueprints/data/db
BLUEPRINTS_GUI_DIR=<path-to-xarta-node>/.xarta/gui
REPO_OUTER_PATH=<path-to-xarta-node>
REPO_INNER_PATH=<path-to-xarta-node>/.xarta

# Service restart (used by auto-update and git-sync)
SERVICE_RESTART_CMD=systemctl restart blueprints-app

# CORS — seed origins; peer node ui_urls are added automatically at runtime
BLUEPRINTS_CORS_ORIGINS=http://<node-1>:8080,http://<node-2>:8080

# TLS — populated by setup-certificates.sh
CERTS_DIR=<path-to-xarta-node>/.xarta/.certs
CERT_FILE=<path-to-cert.crt>
CERT_KEY=<path-to-cert.key>
CERT_CA=<path-to-ca.crt>

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
