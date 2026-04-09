---
name: blueprints-dockge-stacks
description: Probe Dockge instances across the fleet via direct SSH (using ssh_targets) and populate dockge_stacks + dockge_stack_services tables. Detects parentage of each Dockge instance. Use when the user wants to enumerate stacks, discover compose definitions, show container status, or refresh Dockge inventory.
---

# Blueprints Dockge Stacks Probe

Discovers Docker Compose stacks managed by Dockge across all known Dockge hosts
(LXCs and VMs), then populates the `dockge_stacks` and `dockge_stack_services`
tables in the Blueprints database.

This skill is for fleet inventory/probing. Canonical per-stack runbooks and
planning notes are maintained in node-local docs under:

- `/xarta-node/.lone-wolf/docs/dockge/README.md`
- `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md`

If those node-local docs are unavailable on the current LXC/workspace, continue
with this skill for discovery/probe operations; treat the node-local docs as
optional supplemental context.

## Critical Copy Rule (Operator Intent)

When the user asks to "copy a Dockge stack" from another host/location, this
MUST be interpreted as copying both:

1. stack definitions (`compose.yaml`, `.env.example`, and local `.env` guidance)
2. required runtime data for functional parity (for example `data/`,
   `volumes/`, cache directories, model caches, vector caches, and other
   non-git runtime state the service requires)

Do not treat "copy stack" as "copy compose only" unless the user explicitly
asks for a compose-only scaffold.

Before declaring migration complete, explicitly report:

- which runtime paths were copied
- which runtime paths were intentionally excluded
- proof checks (file counts/sizes and at least one functional endpoint test)

## Architecture Overview

1. **Source of truth**: `proxmox_config.dockge_json` tells us which machines
   have Dockge and at which `stacks_dir` path(s). Populated by the Proxmox
   Config probe — run that first.
2. **SSH access**: `proxmox_nets` + `ssh_targets` gives us the right IP, key,
   and source VLAN for each machine. No more `pct exec` via PVE for every host.
3. **Relational services**: Each stack has N services in `dockge_stack_services`
   (one row per container), giving clean ports/state/image data per container.
4. **Parentage detection**: Docker `inspect` on the Dockge container reveals
   whether Dockge itself is running as a Dockge stack, plain docker-compose,
   docker run, or a Portainer stack.

## stack_id Format

`{pve_host_safe}_{vmid}_{stacks_dir_slug}_{stack_name}`

e.g. `pvehost_111_opt_stacks_dockge` for vmid=111 on a given PVE host, stacks at
`/opt/stacks`, stack named `dockge`.

This supports machines with **multiple Dockge instances** (different stacks_dirs).

## tables

### `dockge_stacks`

| Column | Type | Notes |
|---|---|---|
| `stack_id` | TEXT PK | `{pve_host_safe}_{vmid}_{stacks_dir_slug}_{stack_name}` |
| `pve_host` | TEXT | PVE host IP |
| `source_vmid` | INTEGER | VMID of machine running Dockge |
| `source_lxc_name` | TEXT | Name of VM/LXC |
| `stack_name` | TEXT | Stack directory name |
| `status` | TEXT | `running` \| `stopped` \| `partial` \| `unknown` |
| `compose_content` | TEXT | Full compose.yaml content |
| `services_json` | TEXT | JSON array of service names (summary) |
| `ports_json` | TEXT | JSON array of all port mappings (summary) |
| `volumes_json` | TEXT | JSON array of volume mounts |
| `env_file_exists` | INTEGER | 1 if .env present |
| `stacks_dir` | TEXT | Path to stacks directory |
| `vm_type` | TEXT | `lxc` or `qemu` |
| `ip_address` | TEXT | IP we SSH'd into for this probe |
| `parent_context` | TEXT | How Dockge itself is running: `dockge-stack` \| `docker-compose` \| `docker-run` \| `portainer-stack` \| `native` \| `unknown` |
| `parent_stack_name` | TEXT | If `parent_context=dockge-stack`, the stack name |
| `obsolete` | INTEGER | 1 = user-marked obsolete; probe never overwrites this |
| `notes` | TEXT | Free-text user notes (editable in GUI; probe never overwrites) |
| `last_probed` | TEXT | ISO timestamp |

### `dockge_stack_services`

One row per service (container) per stack.

| Column | Type | Notes |
|---|---|---|
| `service_id` | TEXT PK | `{stack_id}_{service_name}` |
| `stack_id` | TEXT | FK → `dockge_stacks.stack_id` |
| `service_name` | TEXT | Service name from compose.yaml |
| `image` | TEXT | Docker image |
| `ports_json` | TEXT | JSON array of "host:container/proto" |
| `volumes_json` | TEXT | JSON array of volume mounts |
| `container_state` | TEXT | `running` \| `exited` \| `restarting` etc |
| `container_id` | TEXT | Short Docker container ID |
| `last_probed` | TEXT | ISO timestamp |

## Prerequisites

Run the **Proxmox Config probe** first to populate `proxmox_config.dockge_json`.

SSH keys needed (pass whichever are configured):
- `VM_SSH_KEY` — for QEMU VMs
- `LXC_SSH_KEY` — for non-fleet LXCs
- `XARTA_NODE_SSH_KEY` — for fleet xarta-node LXCs
- `CITADEL_SSH_KEY` — for the dedicated citadel VM
- `PROXMOX_SSH_KEY` — fallback for LXC pct exec when no direct SSH target

If a machine has no `ssh_targets` entry:
- LXCs: falls back to `pct exec` via PVE host (requires `PROXMOX_SSH_KEY`)
- QEMUs: skipped with a warning — add the VM to `ssh_targets` to probe it

The Dockge instance must be **running**. Stopped instances are skipped gracefully.

## Running the Probe

Via the GUI: **Dockge Stacks** tab → **▶ Probe Dockge**.

Via the API:
```bash
# Status check
curl -s http://localhost:8080/api/v1/dockge-stacks/probe/status

# Run probe (may take 1-3 minutes depending on fleet size)
curl -s -X POST http://localhost:8080/api/v1/dockge-stacks/probe

# List stacks with new columns
curl -s http://localhost:8080/api/v1/dockge-stacks | python3 -m json.tool | head -60

# List services for a specific stack
curl -s "http://localhost:8080/api/v1/dockge-stacks/services?stack_id=pvehost_111_opt_stacks_dockge"
```

## Probe Output

stdout lines:
- `##ENTRIES## [...]` — JSON array of stack objects
- `##SERVICES## [...]` — JSON array of service objects
- `##STATS## {...}` — `{machines_probed, dockge_instances_probed, stacks_found, services_found}`

stderr: human-readable progress (safe to discard).

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `GET /api/v1/dockge-stacks` | GET | List all stacks |
| `POST /api/v1/dockge-stacks` | POST | Create one stack record |
| `POST /api/v1/dockge-stacks/bulk` | POST | Bulk upsert stacks |
| `GET /api/v1/dockge-stacks/services` | GET | List all services (optional `?stack_id=`) |
| `POST /api/v1/dockge-stacks/services/bulk` | POST | Bulk upsert services |
| `DELETE /api/v1/dockge-stacks/services/{service_id}` | DELETE | Remove a service row |
| `GET /api/v1/dockge-stacks/probe/status` | GET | Check SSH key availability |
| `POST /api/v1/dockge-stacks/probe` | POST | Run full probe |
| `GET /api/v1/dockge-stacks/{stack_id}` | GET | Single stack |
| `PUT /api/v1/dockge-stacks/{stack_id}` | PUT | Update stack |
| `DELETE /api/v1/dockge-stacks/{stack_id}` | DELETE | Delete stack + its services |

## Notes on Portainer

Portainer is tracked separately via `proxmox_config.portainer_json`. It does
**not** share this table. Portainer API access requires tokens and is
fundamentally different from Dockge's file-based approach. A dedicated
`portainer_stacks` table and probe are planned for the future.

## References

- `/xarta-node/.lone-wolf/docs/dockge/README.md`
- `/xarta-node/.lone-wolf/docs/dockge/OPEN-NOTEBOOK.md`
- `/xarta-node/.lone-wolf/docs/setup/DOCKGE.md`

These are optional node-local references and may not exist in standalone/public-only environments.

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
