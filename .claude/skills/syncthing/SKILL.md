---
name: syncthing
description: Syncthing fleet asset distribution — manage binary icon and sound assets across all xarta-node fleet LXCs. Use when adding/checking asset files, monitoring sync health, managing the Syncthing service, onboarding a new node to the mesh, or debugging sync issues. Also use when setup-syncthing.sh is involved.
---

# Syncthing — Fleet Asset Distribution

Syncthing provides peer-to-peer fleet sync for binary assets that are unsuitable for git.

## What it syncs

| Folder ID | Local path | Contents |
|-----------|-----------|---------|
| `xarta-icons` | `gui-fallback/assets/icons/` | Nav item icon SVGs/PNGs/ICOs |
| `xarta-sounds` | `gui-fallback/assets/sounds/` | Nav item interaction sounds |

Both paths are gitignored (except `assets/icons/fallback.svg`). The `nav_items` DB table references these files by name; DB rows sync via Blueprints, asset files sync via Syncthing.

## Key facts

- **Service**: `syncthing@xarta.service` (not `syncthing.service`)
- **Config**: `/home/xarta/.local/state/syncthing/config.xml` (XDG state dir — Syncthing 1.30+)
- **Config is generated** by `setup-syncthing.sh` from `.nodes.json` + `.env`. Do not hand-edit — changes will be overwritten on next run.
- **GUI** bound to loopback only; exposed via Caddy reverse proxy using `SYNCTHING_HOSTNAME` from `.env`
- **API key** stored in `.env` as `SYNCTHING_API_KEY`; used for local curl calls to `127.0.0.1:8384`
- **Device IDs** stored in `.nodes.json` per node under `syncthing_device_id`
- **Transport** uses explicit fleet peer addresses from `.nodes.json`; local UDP discovery is disabled

## Two-pass rollout

New nodes require two passes of `setup-syncthing.sh`:

1. **Pass 1**: Generates device cert, reads device ID, writes to `.env`. Peer list is empty (IDs not yet known).
2. Record device ID in `.nodes.json`, run `bp-nodes-push.sh`.
3. **Pass 2**: Rebuilds `config.xml` with full peer list populated from `.nodes.json`.

When adding a new node to an existing mesh, pass 2 must also be re-run on all existing nodes to inject the new peer.

## Common operations

```bash
# Add an asset — copy to the folder on any node; propagates automatically
cp my-icon.svg /xarta-node/gui-fallback/assets/icons/

# Service status / restart
systemctl status syncthing@xarta.service
systemctl restart syncthing@xarta.service

# Live logs
journalctl -u syncthing@xarta -f

# Read own device ID
curl -sf -H "X-API-Key: $SYNCTHING_API_KEY" \
    http://127.0.0.1:8384/rest/system/status | python3 -c "import json,sys; print(json.load(sys.stdin)['myID'])"

# Check peer connections
curl -sf -H "X-API-Key: $SYNCTHING_API_KEY" \
    http://127.0.0.1:8384/rest/system/connections | python3 -m json.tool

# Folder sync state
curl -sf -H "X-API-Key: $SYNCTHING_API_KEY" \
    "http://127.0.0.1:8384/rest/db/status?folder=xarta-icons" | python3 -m json.tool

# Find conflict files
find /xarta-node/gui-fallback/assets/ -name '*.sync-conflict*'
```

## Detailed docs

This public skill is intentionally self-contained so it still works in a
standalone public checkout.

On deployed xarta-node fleet nodes, extra node-local operational notes may also
exist in the lone-wolf docs tree, but they are optional and must not be relied
on as the only instructions here.
