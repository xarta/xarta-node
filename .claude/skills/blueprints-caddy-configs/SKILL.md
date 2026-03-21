---
name: blueprints-caddy-configs
description: Discover Caddy reverse proxy configurations across all LXCs in the fleet via pct exec, extracting domain names and upstream targets into the Blueprints caddy_configs table. Use when the user wants to audit reverse proxy rules, find which domains map to which services, or refresh Caddy inventory.
---

# Blueprints Caddy Configs Probe

Dynamically discovers running LXCs on each Proxmox host, checks each for a Caddyfile at `/etc/caddy/Caddyfile` (or `/etc/caddy/caddy.conf`), and populates the `caddy_configs` table with domain/upstream data.

## What it does

For each running LXC on each PVE host (connected via `pct exec`):
- Checks for `/etc/caddy/Caddyfile` or `/etc/caddy/caddy.conf`
- Reads the main Caddyfile content
- Also reads fragment files from `/etc/caddy/` (`.caddy`, `.conf` extensions)
- Parses domain block headers (e.g. `hostname.domain.tld {`)
- Parses `reverse_proxy` upstreams
- One row per LXC that has Caddy

## Known Caddy LXCs (as of last discovery)

> The probe dynamically enumerates **all running LXCs** — no instances are hardcoded.
> Run the probe to populate this table from your own fleet.

## Table: `caddy_configs`

| Column | Type | Notes |
|---|---|---|
| `caddy_id` | TEXT PK | `{pve_name}_{vmid}` |
| `pve_host` | TEXT | PVE host IP |
| `source_vmid` | TEXT | VMID of LXC with Caddy |
| `source_lxc_name` | TEXT | Hostname of LXC |
| `caddyfile_path` | TEXT | Path to main Caddyfile |
| `caddyfile_content` | TEXT | Full Caddyfile content |
| `domains_json` | TEXT | JSON array of extracted domains |
| `upstreams_json` | TEXT | JSON array of reverse_proxy targets |
| `last_probed` | TEXT | ISO timestamp |

## Running the Probe

Via the GUI: navigate to the **Caddy Configs** tab and click **▶ Probe Caddy**.

Via the API:
```bash
# Check if probe is available (requires PROXMOX_SSH_KEY)
curl -s http://localhost:8080/api/v1/caddy-configs/probe/status

# Run the probe (takes 60-120s — iterates all running LXCs)
curl -s -X POST http://localhost:8080/api/v1/caddy-configs/probe
```

Directly for debugging:
```bash
PROXMOX_SSH_KEY=/root/.ssh/id_ed25519_proxmox \
  bash /root/xarta-node/.claude/skills/blueprints-caddy-configs/scripts/bp-caddy-configs-probe.sh
```

## Script: `bp-caddy-configs-probe.sh`

Location: `/root/xarta-node/.claude/skills/blueprints-caddy-configs/scripts/bp-caddy-configs-probe.sh`

- SSH to each PVE host using `PROXMOX_SSH_KEY`
- Runs remote Python that iterates `pct list` for running LXCs
- For each LXC: `pct exec $vmid -- cat /etc/caddy/Caddyfile`
- Domain regex: `^  (hostname.example.com) {`
- Upstream regex: `reverse_proxy <target>`
- Outputs `##ENTRIES## [...]` and `##STATS## {...}`

## Prerequisites

- `PROXMOX_SSH_KEY` set in `.env`
- Python 3 available on each PVE host (standard on Proxmox 7+)
- LXCs must be **running** for `pct exec` to work

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/caddy-configs` | GET | List all configs |
| `/api/v1/caddy-configs` | POST | Create one config record |
| `/api/v1/caddy-configs/bulk` | POST | Bulk upsert |
| `/api/v1/caddy-configs/probe/status` | GET | Check if probe key is available |
| `/api/v1/caddy-configs/probe` | POST | Run probe and upsert results |
| `/api/v1/caddy-configs/{caddy_id}` | GET/PUT/DELETE | CRUD for one record |
