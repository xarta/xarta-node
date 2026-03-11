---
name: blueprints-proxmox-discovery
description: Discover LXCs, VMs, and their IPs on Proxmox hosts via SSH, then create or update corresponding machine records in the Blueprints database. Use when the user wants to scan Proxmox infrastructure, refresh the machines table from live PVE hosts, or discover new containers/VMs.
---

# Blueprints Proxmox Discovery

Discover LXCs, VMs, and containers running on Proxmox VE hosts by SSH-ing into each host and using `pct list` / `qm list` / `pct exec <vmid> -- hostname -I`. Populate or update the Blueprints `machines` table with the results.

## Prerequisites

- **SSH agent forwarding must be enabled** on the session connecting to the Blueprints node. Without it, SSH hops from the Blueprints node to Proxmox hosts (and onward to nested VMs/LXCs) will fail with authentication errors.
  - If SSH from the Blueprints node to a Proxmox host fails, alert the user: _"SSH agent forwarding is required. Please reconnect with `ssh -A` or ensure `ForwardAgent yes` is set in your SSH config for this host."_
- The Blueprints API must be running locally (default `http://localhost:8080`).
- Proxmox machines must already exist in the `machines` table with `machine_kind = 'proxmox'` (or the user provides addresses to seed them).

## Workflow

### Step 1 — Identify Proxmox targets

Query the Blueprints database for machines that are Proxmox hosts:

```bash
python3 -c "
import sqlite3, json
conn = sqlite3.connect('/opt/blueprints/data/db/blueprints.db')
conn.row_factory = sqlite3.Row
rows = conn.execute(\"\"\"
    SELECT machine_id, name, ip_addresses, parent_machine_id
    FROM machines
    WHERE machine_kind = 'proxmox'
    ORDER BY parent_machine_id NULLS FIRST, machine_id
\"\"\").fetchall()
for r in rows:
    ips = json.loads(r['ip_addresses']) if r['ip_addresses'] else []
    parent = r['parent_machine_id'] or '(root)'
    print(f\"  {r['machine_id']:<16} parent={parent:<16} ips={ips}\")
"
```

If no Proxmox machines exist, ask the user:
> _"No Proxmox machines found in the database. Please provide the machine IDs and IP addresses of your Proxmox hosts so I can add them first."_

Create them via the API before proceeding:
```
POST /api/v1/machines
{
  "machine_id": "<pve-id>",
  "name": "<pve-id>",
  "type": "baremetal",        // or "vm" for nested Proxmox
  "machine_kind": "proxmox",
  "platform": "proxmox",
  "ip_addresses": ["<ip>"],
  "parent_machine_id": null,  // or parent PVE machine_id for nested
  "status": "active"
}
```

### Step 2 — SSH discovery on each Proxmox host

For each Proxmox machine, use the first listed IP address and run the discovery script:

```bash
bash /root/xarta-node/.claude/skills/blueprints-proxmox-discovery/scripts/bp-pve-discover.sh <ip-address>
```

This outputs JSON lines — one per LXC/VM — with fields: `vmid`, `name`, `status`, `type` (lxc/vm), `ips` (array), `mem_mb`, `disk_gb`.

Process hosts in **parent-first order** (baremetal before nested) so that parent machine_id references are valid when creating child records.

### Step 3 — Reconcile with database

For each discovered LXC/VM:

1. Compute a **host-qualified `machine_id`** using the convention `{type}-{pve-host}-{vmid}`:
   - LXC 100 on host-a → `lxc-host-a-100`
   - VM 200 on host-b → `vm-host-b-200`
   - This is **required** because the same VMID can exist on different Proxmox hosts (they are entirely different machines).
2. Check if a machine record exists for that `machine_id`.
3. **If missing** — `POST /api/v1/machines` to create it.
4. **If exists** — `PUT /api/v1/machines/<machine_id>` to update IP addresses, status, and description.

Machine type mapping:
| PVE type | `type` field | `machine_kind` | `machine_id` example |
|----------|-------------|-----------------|----------------------|
| LXC      | lxc         | lxc             | `lxc-<pve-host>-<vmid>`     |
| VM       | vm          | vm              | `vm-<pve-host>-<vmid>`      |
| Docker   | docker      | docker          | `docker-<pve-host>-<lxc-vmid>-<name>` |

Set `parent_machine_id` to the Proxmox host's `machine_id`.
Set `platform` to `proxmox` (or `docker` for containers).

### Step 4 — Deeper discovery (optional)

Once LXCs/VMs are known, the agent can optionally SSH into running containers to discover:

- **Docker containers**: `ssh root@<lxc-ip> docker ps --format '{{.Names}} {{.Image}} {{.Ports}} {{.Status}}'`
- **Listening services**: `ssh root@<lxc-ip> ss -tlnp` or `netstat -tlnp`
- **Caddy routes**: `ssh root@<lxc-ip> cat /etc/caddy/Caddyfile 2>/dev/null`
- **Documentation files**: look for README.md, docker-compose.yml, etc.

This deeper discovery may require SSH agent forwarding to chain through the Proxmox host to the LXC. If direct SSH fails, try hopping:

```bash
ssh -J root@<proxmox-ip> root@<lxc-ip> <command>
```

### Step 5 — Fleet node mapping

After discovery, cross-reference fleet nodes (from the `nodes` table) with discovered LXCs to set `machine_id` on the node record:

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/opt/blueprints/data/db/blueprints.db')
conn.row_factory = sqlite3.Row
for n in conn.execute('SELECT node_id, display_name, machine_id FROM nodes').fetchall():
    print(f\"  {n['node_id']:<20} machine_id={n['machine_id'] or 'NULL'}\")
"
```

Match nodes to LXCs by name correlation (e.g., a node named `thunderbird-1` may correspond to a Proxmox LXC also named `thunderbird-1`). Update via:
```
PUT /api/v1/nodes/<node-id>
{ "machine_id": "<lxc-machine-id>" }
```

## SSH agent forwarding troubleshooting

If SSH connections to Proxmox hosts fail:

1. **Check agent**: `ssh-add -l` — should list keys
2. **Check forwarding**: `echo $SSH_AUTH_SOCK` — must be set
3. **Test direct**: `ssh -v root@<proxmox-ip> hostname`
4. **Common fix**: User needs to reconnect to the Blueprints node with `ssh -A` or set `ForwardAgent yes` in their SSH config

## Output

After running, report:
- Number of machines created vs updated
- Any LXCs/VMs that couldn't be reached
- Fleet nodes that were linked to machine records
- Suggestions for deeper discovery on running containers
