---
name: blueprints-nodes-json
description: Work with the .nodes.json single source of truth for the fleet node list — validate, load, push, delete, generate hosts, or onboard a new node. Use when the user wants to add/remove fleet nodes, distribute .nodes.json, update /etc/hosts, or understand the node config architecture.
---

# Blueprints — `.nodes.json` Node Configuration

## Overview

`.nodes.json` is the single source of truth for fleet node identity, network addresses,
and sync configuration.  It lives **outside git** (gitignored) in the public repo root:

```
/root/xarta-node/.nodes.json        ← on every fleet node (distributed manually)
```

Its path is stored in `.env` as `NODES_JSON_PATH` so the Python app and shell
scripts can find it without hardcoding.

The app reads this file **at startup** and on every `POST /api/v1/nodes/refresh`
call. The `nodes` SQLite table is derived from it — not the other way around.

---

## File format

```json
{
  "nodes": [
    {
      "node_id":          "<node-id>",
      "display_name":     "<Display Name>",
      "host_machine":     "<node-id>",
      "tailnet":          "<tailnet-name>.ts.net",
      "primary_ip":       "<VLAN42-IP>",
      "primary_hostname": "<node-id>.infra.<your-domain>",
      "tailnet_ip":       "<100.x.x.x>",
      "tailnet_hostname": "<node-id>.<tailnet-name>.ts.net",
      "sync_port":        8080,
      "active":           true
    }
  ]
}
```

### Field reference

| Field | Required | Notes |
|-------|----------|-------|
| `node_id` | ✅ | Matches `BLUEPRINTS_NODE_ID` in `.env`; unique key |
| `display_name` | ✅ | Human-readable label shown in the GUI |
| `host_machine` | ✅ | Usually the same as `node_id` unless virtualised differently |
| `tailnet` | ✅ | The Tailscale tailnet domain for this node |
| `primary_ip` | ✅ (if active) | VLAN42 / management IP — used for sync traffic |
| `primary_hostname` | ✅ (if active) | HTTPS-accessible hostname (e.g. Caddy TLS) |
| `tailnet_ip` | ✅ (if active) | Tailscale IP (100.x.x.x) |
| `tailnet_hostname` | ✅ (if active) | Full Tailscale hostname |
| `sync_port` | ✅ (if active) | Usually `8080` |
| `active` | ✅ | `true` = included in sync; `false` = node excluded but record preserved |

---

## Shell scripts

### Validate

```bash
bash bp-nodes-validate.sh [/path/to/.nodes.json]
```

Checks JSON syntax, required fields, no duplicate IDs/IPs, and that
`BLUEPRINTS_NODE_ID` in `.env` matches exactly one entry.

Exit 0 = valid, exit 1 = invalid (errors to stderr).

### Load into DB

```bash
bash bp-nodes-load.sh [/path/to/.nodes.json]
```

Validates then upserts all `active: true` nodes into the SQLite `nodes` table.
This is idempotent and safe to re-run.

Also called by the app at startup and by `POST /api/v1/nodes/refresh`.

### Push to fleet

```bash
bash bp-nodes-push.sh [/path/to/.nodes.json]
```

1. Validates locally.
2. SCPs the JSON to each active peer node (reads `NODES_JSON_PATH` from remote `.env`).
3. Calls `POST /api/v1/nodes/refresh` on each peer.

Requires SSH key configured: `XARTA_NODE_SSH_KEY` env var or `SSH_KEY_NAME` in `.env`.

### Mark a node inactive (soft-delete)

```bash
bash bp-nodes-delete.sh <node_id> [/path/to/.nodes.json]
```

Sets `active: false` for the node, then calls `bp-nodes-load.sh` to sync the DB.
Refuses to deactivate the node running the script (self-protection).

To fully propagate: run `bash bp-nodes-push.sh` afterwards.

### Generate /etc/hosts entries

```bash
bash bp-nodes-hosts.sh [/path/to/.nodes.json]
```

Writes `primary_ip → primary_hostname` and `tailnet_ip → tailnet_hostname`
for every active node into a managed block in `/etc/hosts`.
Idempotent — safe to re-run; replaces the old block cleanly.

Preferred over `setup-hosts.sh` on any node with `.nodes.json`.

---

## Python app integration

### `config.py`

`config.py` reads `.nodes.json` at import time and derives all fleet variables:

| `cfg.*` variable | Source |
|-----------------|--------|
| `NODE_ID` | `BLUEPRINTS_NODE_ID` env var |
| `NODE_NAME` | `display_name` of self entry |
| `HOST_MACHINE` | `host_machine` of self entry |
| `SELF_ADDRESS` | `http://{primary_ip}:{sync_port}` |
| `UI_URL` | `https://{primary_hostname}` |
| `PEER_URLS` | all active nodes except self, `http://{primary_ip}:{sync_port}` |
| `CORS_ORIGINS` | all active nodes, primary HTTPS + tailnet HTTPS |
| `FLEET_LXC_NAMES` | list of all active `node_id` values |
| `NODES_DATA` | full parsed JSON node list |

**Hard fails** (exit on startup) if:
- `NODES_JSON_PATH` file not found
- `BLUEPRINTS_NODE_ID` not present in the JSON

### `POST /api/v1/nodes/refresh`

Re-reads `.nodes.json` and upserts the DB.  Returns:
```json
{"status": "ok", "active_nodes": 6}
```

Use this after receiving a `bp-nodes-push.sh` distribution, or via the
GUI Refresh button (Settings → Nodes).

### `POST /api/v1/nodes` — returns 405

Programmatic node registration is no longer supported.  Edit `.nodes.json`
and distribute with `bp-nodes-push.sh` instead.

---

## `.env` required keys

On every fleet node, `.env` must contain:

```bash
BLUEPRINTS_NODE_ID=<node-id>         # the identity of THIS node
BLUEPRINTS_INSTANCE=1                # instance number (usually 1)
NODES_JSON_PATH=/root/xarta-node/.nodes.json
```

The following keys were removed in Phases 2–3 of the nodes-json migration
and must NOT be present (the app will ignore them, but they cause confusion):

```
BLUEPRINTS_NODE_NAME, BLUEPRINTS_HOST_MACHINE, BLUEPRINTS_UI_URL,
BLUEPRINTS_CORS_ORIGINS, BLUEPRINTS_SELF_ADDRESS, BLUEPRINTS_PEERS,
FLEET_LXC_NAMES
```

---

## Adding a new node

1. Add an entry to `.nodes.json` on the reference node (usually any active node).
2. Set `active: true`.
3. Run `bash bp-nodes-validate.sh` — must pass.
4. Run `bash bp-nodes-push.sh` — distributes to all currently-active peers.
5. On the new node: copy `.nodes.json` manually (e.g. SCP), add `BLUEPRINTS_NODE_ID`
   to `.env`, set `NODES_JSON_PATH` in `.env`.
6. Run `bash bp-nodes-load.sh` on the new node.
7. Restart the app on the new node.

See the `onboarding` skill for full new-node setup procedure.

---

## Removing a node

```bash
bash bp-nodes-delete.sh <node_id>   # marks active=false, reloads local DB
bash bp-nodes-push.sh               # distributes the updated JSON fleet-wide
```

The node entry is preserved in the JSON for audit purposes.
To permanently remove it, manually delete the entry and re-push.

---

## Distribution (gitignored)

`.nodes.json` is gitignored in the public repo. It is never committed.
Distribution is manual — via `bp-nodes-push.sh` over SSH.

Each node should have `.nodes.json` at the path in `NODES_JSON_PATH`.
If the file is absent on a node, the app **will not start**.

---

## Relationship to fleet-hosts.conf

`fleet-hosts.conf` (private repo `.xarta/`) is the legacy node list used by:
- old fleet-pull scripts (now updated to read `.nodes.json`)
- `setup-hosts.sh` (superseded by `bp-nodes-hosts.sh` on nodes with `.nodes.json`)

Keep `fleet-hosts.conf` in sync with `.nodes.json` until all nodes are migrated.
Long-term it can be retired.
