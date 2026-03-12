````skill
---
name: blueprints-dockge-stacks
description: Probe Dockge instances across the fleet via pct exec and collect Docker Compose stack definitions into the Blueprints dockge_stacks table. Use when the user wants to enumerate stacks, discover compose definitions, or refresh Dockge inventory.
---

# Blueprints Dockge Stacks Probe

Discovers Docker Compose stacks managed by Dockge across all known Dockge LXCs, then populates the `dockge_stacks` table in the Blueprints database.

## What it does

For each Dockge instance (connected via `pct exec` on the PVE host):
- Lists directories under `/opt/stacks` (or `/opt/dockge/data/stacks`)
- Reads `compose.yaml` for each stack
- Checks for `.env` file presence
- Optionally queries `docker compose ps` for container status
- Extracts service names, port mappings, volume mounts

## Known Dockge Instances

> No instances are hardcoded. The probe reads all LXC VMIDs from the
> `proxmox_config` table (populated by `bp-proxmox-config-probe.sh`) and checks
> each one for a Dockge stacks directory. Run that probe first, then this one.

## table: `dockge_stacks`

| Column | Type | Notes |
|---|---|---|
| `stack_id` | TEXT PK | `{source_vmid}_{stack_name}` |
| `pve_host` | TEXT | PVE host IP |
| `source_vmid` | TEXT | VMID of Dockge LXC |
| `source_lxc_name` | TEXT | Hostname of LXC |
| `stack_name` | TEXT | Stack directory name |
| `status` | TEXT | Container status summary |
| `compose_content` | TEXT | Full compose.yaml content |
| `services_json` | TEXT | JSON array of service names |
| `ports_json` | TEXT | JSON array of port mappings |
| `volumes_json` | TEXT | JSON array of volume mounts |
| `env_file_exists` | INTEGER | 1 if .env present |
| `stacks_dir` | TEXT | Path to stacks directory on LXC |
| `last_probed` | TEXT | ISO timestamp of last probe |

## Running the Probe

Via the GUI: navigate to the **Dockge Stacks** tab and click **▶ Probe Dockge**.

Via the API:
```bash
# Check if probe is available on this node (requires PROXMOX_SSH_KEY)
curl -s http://localhost:8080/api/v1/dockge-stacks/probe/status

# Run the probe (takes 30-60s)
curl -s -X POST http://localhost:8080/api/v1/dockge-stacks/probe
```


## Script: `bp-dockge-stacks-probe.sh`

Location: the probe script (path configured in skill)

- SSH to each PVE host using `PROXMOX_SSH_KEY`
- Uses `pct exec $vmid -- ...` to run commands inside Dockge LXCs
- Outputs `##ENTRIES## [...]` and `##STATS## {...}`
- The FastAPI route (`routes_dockge_stacks.py`) parses these and upserts in-process

## Prerequisites

- `PROXMOX_SSH_KEY` must be set in `.env`
- The Dockge LXC must be **running** (pct exec requires running container)
- For Docker-based Dockge instances, the LXC must have Docker installed and running

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/dockge-stacks` | GET | List all stacks |
| `/api/v1/dockge-stacks` | POST | Create one stack record |
| `/api/v1/dockge-stacks/bulk` | POST | Bulk upsert |
| `/api/v1/dockge-stacks/probe/status` | GET | Check if probe key is available |
| `/api/v1/dockge-stacks/probe` | POST | Run probe and upsert results |
| `/api/v1/dockge-stacks/{stack_id}` | GET/PUT/DELETE | CRUD for one record |
````
