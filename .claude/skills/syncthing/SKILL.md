---
name: syncthing
description: Syncthing fleet asset distribution — manage binary icon and sound assets across all xarta-node fleet LXCs. Use when adding/checking asset files, monitoring sync health, managing the Syncthing service, onboarding a new node to the mesh, or debugging sync issues. Also use when setup-syncthing.sh is involved.
---

# Syncthing — Fleet Asset Distribution

Syncthing provides peer-to-peer fleet sync for binary assets that are unsuitable for git.

Scope note:
- This skill covers fleet asset sync (`gui-fallback/assets/*`) and Syncthing service operations.
- For docs-folder ingestion workflows (including select local Obsidian folders on this LXC), use the node-local Dockge docs as the planning source:
    - `/xarta-node/.lone-wolf/docs/dockge/README.md`
    - `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md`
    If node-local docs are unavailable on the current LXC/workspace, continue with this skill for Syncthing operations.

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

# Validate assets owner-guard + add/delete sync end to end
bash /root/xarta-node/.claude/skills/syncthing/scripts/validate-assets-owner-guard.sh
```

## Owner-Guard Validation

Use this validator when troubleshooting or proving that root-owned drift under
`assets/icons` does not break fleet sync.

- Script: `/root/xarta-node/.claude/skills/syncthing/scripts/validate-assets-owner-guard.sh`
- Requires root (it intentionally creates a root-owned temporary test file)
- Verifies:
    - owner-guard cron wiring exists
    - temporary root-owned file flips to `xarta:xarta` automatically
    - `xarta-icons` status has zero errors/needs
    - all peers report `completion=100` with file present and after file deletion
- Cleans up test file automatically

## Detailed docs

This public skill is intentionally self-contained so it still works in a
standalone public checkout.

On deployed xarta-node fleet nodes, extra node-local operational notes may also
exist in the lone-wolf docs tree, but they are optional and must not be relied
on as the only instructions here.

Relevant node-local docs for cross-tool document workflows:
- `/xarta-node/.lone-wolf/docs/dockge/README.md`
- `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md`

These references are optional and may not exist in standalone/public-only environments.

## MANDATORY - Embedded Menu DB Authority Contract (2026-04-08)

- Database is authoritative for embedded selector action pages in all contexts.
- `page_index` and `sort_order` from DB define order and slot positions.
- JS/runtime may insert placeholder circles only to preserve intentional DB slot gaps.
- Scarab paging control is always shown when multiple pages exist, except when touch ribbon mode is actively in use.
- Fallback is allowed only for embedded controls, and only when DB config fetch fails.
- Do not hardcode or merge local page layouts in a way that overrides DB-defined page order/positions.

## MANDATORY - App-Specific Selector Context Guardrail (2026-04-08)

- Never assume `menu_context='embed'` for new app work.
- Do not add or modify `embed_menu_items` rows in shared contexts (`embed`, `fallback-ui`, `db`) unless the user explicitly requests cross-app/shared rollout.
- Treat `embed` context as shared across all embed consumers (not app-local).
- For app-local selector behavior, require an app-specific context and explicit route-context wiring before any DB row additions.
- Default for new app work: no embed-menu DB writes unless explicitly requested.

The User insists on recognising that the menu system is database driven.  Never use language that suggests otherwise such as setting defaults in a file.  Word things carefully to always acknowledge that the menu system is database driven.  Changes to icons for example happen in the database as paths.  That is where to look.  Always confirm any possible exceptions, with careful diplomacy and tone, with the User, before assuming there are.

The User insists on recognising that the menu system is database driven.  Never use language that suggests otherwise such as setting defaults in a file.  Word things carefully to always acknowledge that the menu system is database driven.  Changes to icons for example happen in the database as paths.  That is where to look.  Always confirm any possible exceptions, with careful diplomacy and tone, with the User, before assuming there are.
