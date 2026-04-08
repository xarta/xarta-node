---
name: blueprints-proxmox-discovery
description: Discover LXCs, VMs, and their IPs on Proxmox hosts via SSH, then create or update corresponding machine records in the Blueprints database. Use when the user wants to scan Proxmox infrastructure, refresh the machines table from live PVE hosts, or discover new containers/VMs.
---

# Blueprints Proxmox & Infrastructure Discovery

Multi-technique infrastructure discovery for Proxmox-based homelabs. This skill
chains together SSH probing, config file parsing, MAC cross-referencing, Docker
enumeration, Tailscale extraction, DNS forward/reverse sweeps, and Caddy domain
harvesting to build and enrich the Blueprints `machines` table.

## Prerequisites

- **SSH agent forwarding must be enabled** on the session connecting to the
  Blueprints node. Without it, SSH hops from the Blueprints node to Proxmox
  hosts will fail with authentication errors.
  - If SSH fails, alert the user: _"SSH agent forwarding is required. Please
    reconnect with `ssh -A` or ensure `ForwardAgent yes` is set."_
- The Blueprints API must be running locally (default `http://localhost:8080`).
- Proxmox machines must already exist in the `machines` table with
  `machine_kind = 'proxmox'` (or the user provides addresses to seed them).
- `dig` must be available on the local node (usually `/usr/bin/dig`; install
  via `apt install dnsutils` if missing).

## Discovery Phases

Run phases in order — earlier phases feed context to later ones.

---

### Phase 1 — Identify Proxmox targets

Query the Blueprints database for machines with `machine_kind = 'proxmox'`:

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

If no Proxmox machines exist, ask the user for host addresses and create them
via `POST /api/v1/machines` with `machine_kind: proxmox`, `type: baremetal`
(or `vm` for nested hosts).

Process hosts in **parent-first order** — baremetal Proxmox hosts before nested
ones, so that `parent_machine_id` references are valid when creating child records.

---

### Phase 2 — Basic SSH enumeration (pct list / qm list)

For each Proxmox host, run the discovery script:

```bash
bash /root/xarta-node/.claude/skills/blueprints-proxmox-discovery/scripts/bp-pve-discover.sh <proxmox-ip>
```

This outputs JSON lines with:
- `vmid`, `name`, `status`, `type` (lxc/vm)
- `ips` (array — from `hostname -I` for running LXCs)
- `mem_mb`, `disk_gb`
- `conf_ips` (array — from config file parsing)
- `gw` (gateway IP from config)
- `mac` (MAC address from config)
- `vlan` (VLAN tag from config)

**What the script does:**

1. `pct list` to enumerate LXCs; `pct exec <vmid> -- hostname -I` for running
   LXCs (filters out Docker bridge and IPv6 link-local addresses).
2. Reads `/etc/pve/lxc/<vmid>.conf` to extract static IPs, gateways, MAC
   addresses, and VLAN tags — works for **stopped LXCs** too.
3. `qm list` to enumerate VMs; reads `/etc/pve/qemu-server/<vmid>.conf` to
   get MAC addresses and VLAN tags.

**Host-qualified machine_id convention** — required because the same VMID can
exist on different Proxmox hosts:

| PVE type | `machine_id` format | Example |
|----------|---------------------|---------|
| LXC      | `lxc-<pve-host>-<vmid>` | `lxc-host-a-100` |
| VM       | `vm-<pve-host>-<vmid>`  | `vm-host-b-200` |
| Docker   | `docker-<pve-host>-<lxc-vmid>-<container-name>` | `docker-host-a-500-myapp` |

---

### Phase 3 — LXC config file parsing for stopped containers

Running LXCs report their IPs via `hostname -I`, but **stopped LXCs** have no
runtime. Their static IPs are reliably extracted from Proxmox config files:

```bash
# On the Proxmox host:
cat /etc/pve/lxc/<vmid>.conf
```

Parse the `net0:` (or `net1:`, etc.) lines to extract:
- **Static IP**: `ip=192.168.x.y/24` → strip the CIDR mask
- **Gateway**: `gw=192.168.x.1`
- **MAC address**: `hwaddr=XX:XX:XX:XX:XX:XX`
- **VLAN tag**: `tag=42`

The discovery script handles this automatically. For ad-hoc batch extraction:

```bash
ssh root@<pve-ip> 'for f in /etc/pve/lxc/*.conf; do
  vmid=$(basename "$f" .conf)
  echo "=== $vmid ==="
  cat "$f"
done'
```

---

### Phase 4 — VM IP resolution via MAC cross-referencing

VMs without QEMU guest agent cannot report their IPs directly. To find them:

1. **Get the VM's MAC address** from the Proxmox config:
   ```bash
   ssh root@<pve-ip> cat /etc/pve/qemu-server/<vmid>.conf | grep -oP 'virtio=\K[^,]+'
   ```

2. **Search ARP/neighbor tables** of known machines for that MAC:
   ```bash
   ssh root@<pve-ip> ip neigh show | grep -i "<mac-address>"
   ```

3. Also check other hosts on the same VLAN — the MAC may appear in their
   neighbor tables even if not on the Proxmox host itself.

This is particularly effective for Windows VMs and appliance VMs that don't
run guest agents.

---

### Phase 5 — Docker container discovery

LXCs that run Docker often contain many services. Discover them:

1. **List containers** (use `pct exec` if direct SSH isn't available):
   ```bash
   ssh root@<pve-ip> pct exec <lxc-vmid> -- docker ps \
     --format '{{.Names}} {{.Image}} {{.Ports}} {{.Status}}' 2>/dev/null
   ```

2. **Get container IPs** via Docker inspect:
   ```bash
   ssh root@<pve-ip> pct exec <lxc-vmid> -- docker inspect \
     --format '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
     $(docker ps -q)
   ```

3. Register each container as a machine with:
   - `machine_id`: `docker-<pve-host>-<lxc-vmid>-<container-name>`
   - `type`: `docker`, `machine_kind`: `docker`, `platform`: `docker`
   - `parent_machine_id`: the LXC's machine_id (e.g., `lxc-host-a-500`)

**Note**: Internal-only Docker containers (databases, sidecars) typically have
only Docker bridge IPs. Record as-is or leave `ip_addresses` null.

---

### Phase 6 — Tailscale IP extraction

Machines running Tailscale have overlay IPs (100.x.x.x range) that are often
the only routable addresses from outside the LAN.

```bash
# On a machine with Tailscale:
tailscale status
# or just the IPv4:
tailscale ip -4
```

To run inside an LXC from the Proxmox host:
```bash
ssh root@<pve-ip> pct exec <vmid> -- tailscale status 2>/dev/null
```

Docker containers with Tailscale sidecars:
```bash
ssh root@<pve-ip> pct exec <lxc-vmid> -- docker exec <name> tailscale ip -4
```

Record Tailscale IPs **alongside** LAN IPs in the `ip_addresses` array —
e.g., `["192.168.x.y", "100.x.y.z"]`.

---

### Phase 7 — Gateway and VLAN discovery

LXC config files reveal gateways and VLANs. Collect all unique gateways:

```bash
# Batch extract gateways from all LXC configs on a host:
ssh root@<pve-ip> 'grep -h "gw=" /etc/pve/lxc/*.conf 2>/dev/null \
  | grep -oP "gw=\K[0-9.]+"' | sort -u
```

**Gateways identify network segment boundaries:**
- Each unique gateway IP corresponds to a router/firewall interface
- The gateway's /24 subnet reveals a VLAN or network segment
- Gateways can be recorded as infrastructure machines (`type: network`,
  `machine_kind: router` or `firewall`)

Extract VLAN tags to understand network segmentation:
```bash
ssh root@<pve-ip> 'grep -h "tag=" /etc/pve/lxc/*.conf \
  /etc/pve/qemu-server/*.conf 2>/dev/null \
  | grep -oP "tag=\K[0-9]+"' | sort -un
```

---

### Phase 8 — DNS resolver and domain discovery

Discover what DNS infrastructure is available from the Blueprints node:

```bash
cat /etc/resolv.conf
```

This reveals:
- **Nameserver address(es)** — typically internal DNS (pfSense, Pi-hole, AD)
- **Search domain(s)** — the local domain suffix for short names

Test that DNS works:
```bash
dig +short -x <gateway-ip>    # Reverse lookup on a known gateway
```

If reverse DNS returns PTR records, the local DNS server has zone data that
can be swept for comprehensive discovery (see Phase 10).

---

### Phase 9 — Forward DNS hostname guessing

For VMs and machines whose IPs are unknown, try resolving their hostnames
against discovered domains:

```python
import socket

hostnames = ["vm-name-1", "vm-name-2"]  # From Phase 2 discovery
domains = ["infra.example.com", "example.com"]  # From Phase 8

for hostname in hostnames:
    for domain in domains:
        fqdn = f"{hostname}.{domain}"
        try:
            result = socket.getaddrinfo(fqdn, None, socket.AF_INET)
            ip = result[0][4][0]
            print(f"HIT: {fqdn} -> {ip}")
        except socket.gaierror:
            pass
```

Alternatively:
```bash
dig +short <hostname>.<search-domain>
```

**Strategy**: Try the VM/LXC name from `pct list`/`qm list` as-is, plus
lowercase and hyphenated variants.

---

### Phase 10 — Reverse DNS (PTR) sweep

If the DNS server supports reverse lookups (confirmed in Phase 8), sweep
entire subnets to discover ALL registered hosts:

```bash
# Sweep a /24 subnet:
for i in $(seq 1 254); do
    result=$(dig +short -x 192.168.X.$i 2>/dev/null)
    [ -n "$result" ] && echo "192.168.X.$i -> $result"
done
```

**Which subnets to sweep**: Use the gateway IPs from Phase 7 to determine
the /24 ranges. Each gateway typically sits at .1 or .254 of its subnet.
Sweep each unique subnet.

**What to do with results**:
1. **Match PTR names to existing machines** — cross-reference hostnames
   against known machine names to fill in missing IPs
2. **Discover infrastructure devices** — switches, cameras, NAS units,
   KVM-over-IP devices, routers that aren't in Proxmox
3. **Discover domain naming patterns** — PTR records reveal the local
   domain structure
4. **Register new machines** — Infrastructure devices found via rDNS can be
   added with appropriate types (`type: network`, `machine_kind: switch`, etc.)

Save sweep results to `/tmp/rdns_<subnet>.txt` for reference.

---

### Phase 11 — Caddy reverse proxy domain harvesting

Caddy servers (or nginx/HAProxy) act as reverse proxies and contain a complete
map of internal services and their backend addresses. Find and parse them:

```bash
# Check if Caddy is running on an LXC:
ssh root@<pve-ip> pct exec <vmid> -- which caddy 2>/dev/null
# Get the Caddyfile:
ssh root@<pve-ip> pct exec <vmid> -- cat /etc/caddy/Caddyfile 2>/dev/null
```

**What to extract**:
1. **Frontend domain names** — hostnames Caddy serves (public and internal)
2. **Backend addresses** — `reverse_proxy` targets reveal internal service
   IPs and ports
3. **Service topology** — which frontend maps to which backend

Parse backend addresses:
```bash
grep -oP 'reverse_proxy\s+\Khttps?://[^ }]+|reverse_proxy\s+\K[0-9.]+:[0-9]+' \
  /tmp/caddyfile.txt
```

This often reveals services not visible through any other technique.

---

### Phase 12 — Reconcile with database

For each discovered machine, reconcile against the Blueprints API:

1. **Compute the `machine_id`** using the host-qualified convention (Phase 2).
2. **GET** `/api/v1/machines/<machine_id>` — check if it exists.
3. **If missing** — `POST /api/v1/machines` to create.
4. **If exists** — `PUT /api/v1/machines/<machine_id>` to update IPs, status,
   and any new metadata.

When updating `ip_addresses`, **merge** not replace — a machine may have LAN
IPs, Tailscale IPs, and management IPs. Collect all unique addresses.

Use `urllib.request` (not `requests` — it's not installed) for API calls:
```python
import json, urllib.request

def api_put(machine_id, data):
    body = json.dumps(data).encode()
    req = urllib.request.Request(
        f"http://localhost:8080/api/v1/machines/{machine_id}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())
```

---

### Phase 13 — Fleet node mapping

Cross-reference fleet nodes with discovered LXCs to set `machine_id` on
node records:

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('/opt/blueprints/data/db/blueprints.db')
conn.row_factory = sqlite3.Row
for n in conn.execute('SELECT node_id, display_name, machine_id FROM nodes').fetchall():
    print(f\"  {n['node_id']:<20} machine_id={n['machine_id'] or 'NULL'}\")
"
```

Match nodes to LXCs by name correlation. Update via `PUT /api/v1/nodes/<node-id>`.

---

### Phase 14 — Service backfill

After machines are populated, cross-reference services. Newly discovered
services (from Caddy analysis, Docker inspection, listening ports) can be
registered.

For each running LXC/Docker container, check for listening services:
```bash
ssh root@<lxc-ip> ss -tlnp 2>/dev/null
# or via pct exec:
ssh root@<pve-ip> pct exec <vmid> -- ss -tlnp 2>/dev/null
```

---

## Alternative SSH patterns

If direct SSH to an LXC fails (no agent forwarding through to nested host):

```bash
# pct exec (runs directly on the PVE host, no SSH needed to the LXC):
ssh root@<pve-ip> pct exec <vmid> -- <command>

# SSH jump host:
ssh -J root@<pve-ip> root@<lxc-ip> <command>
```

`pct exec` is generally more reliable as it doesn't require SSH configuration
inside the LXC.

## SSH agent forwarding troubleshooting

1. **Check agent**: `ssh-add -l` — should list keys
2. **Check forwarding**: `echo $SSH_AUTH_SOCK` — must be set
3. **Test direct**: `ssh -v root@<proxmox-ip> hostname`
4. **Common fix**: Reconnect with `ssh -A` or set `ForwardAgent yes`

## Output

After running, report:
- Number of machines created vs updated
- IP coverage stats (how many have IPs vs missing)
- Any LXCs/VMs that couldn't be reached
- New infrastructure devices discovered via rDNS
- Fleet nodes that were linked to machine records
- Domains and services discovered via Caddy
- Suggestions for any remaining gaps

## MANDATORY - Embedded Menu DB Authority Contract (2026-04-08)

- Database is authoritative for embedded selector action pages in all contexts.
- `page_index` and `sort_order` from DB define order and slot positions.
- JS/runtime may insert placeholder circles only to preserve intentional DB slot gaps.
- Scarab paging control is always shown when multiple pages exist, except when touch ribbon mode is actively in use.
- Fallback is allowed only for embedded controls, and only when DB config fetch fails.
- Do not hardcode or merge local page layouts in a way that overrides DB-defined page order/positions.
