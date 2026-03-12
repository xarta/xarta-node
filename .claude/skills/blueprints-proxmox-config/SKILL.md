---
name: blueprints-proxmox-config
description: Work on the Blueprints Proxmox Config table — probing PVE hosts for VM/LXC network interface data, discovering IPs, enriching NIC records from pfSense, and managing the proxmox_config and proxmox_nets tables. Use when the user wants to probe Proxmox for network config, find missing IPs for VMs/LXCs, enrich NIC data from pfSense, or manage the Proxmox Config GUI tab.
---

# Blueprints Proxmox Config

The Proxmox Config subsystem discovers and stores **per-VM/LXC network interface
configuration** from Proxmox PVE hosts — MAC addresses, IPs, VLAN tags, bridge
names — in two linked tables: `proxmox_config` (one row per VM/LXC) and
`proxmox_nets` (one row per NIC).

This is distinct from the `machines` table (general infrastructure inventory).

---

## Tables

### `proxmox_config`

One row per VM or LXC container.

| Column | Notes |
|--------|-------|
| config_id | PK — `<pve-hostname>-<vmid>` |
| pve_host | Proxmox host address |
| vmid | Integer VM/LXC ID |
| name | VM/LXC name |
| vm_type | `lxc` or `qemu` |
| status | `running`, `stopped`, etc. |
| cores | CPU cores |
| memory_mb | RAM in MB |
| tags | Proxmox tag string |
| last_probed | ISO 8601 timestamp |

### `proxmox_nets`

One row per NIC on a VM/LXC.

| Column | Notes |
|--------|-------|
| net_id | PK — `<config_id>-<net_key>` |
| config_id | FK → proxmox_config |
| net_key | Interface key, e.g. `net0`, `net1` |
| mac_address | MAC from Proxmox config |
| bridge | Proxmox bridge name |
| model | NIC model (`virtio`, etc.) |
| vlan_tag | VLAN tag (integer, nullable) |
| ip_address | Discovered IP (nullable) |
| ip_source | How IP was found: `conf`, `arp`, `qemu-agent`, `pfsense-sweep`, `pve-arping` |

---

## Environment Variables

Required in `.env` (private — do not add to `.env.example`):

| Variable | Purpose |
|----------|---------|
| `PROXMOX_SSH_KEY` | Path to SSH key for Proxmox hosts |
| `PFSENSE_SSH_KEY` | Path to SSH key for pfSense hosts |
| `PFSENSE_SSH_TARGET` | `user@host` for primary pfSense |
| `PFSENSE_CLOUSEAU_SSH_TARGET` | `user@host` for a separate pfSense appliance (use for appliances not reachable as the primary firewall). Uses the same `PFSENSE_SSH_KEY`. || `VM_SSH_KEY` | Path to key for probe-services SSH into general VMs/LXCs |
| `CITADEL_SSH_KEY` | Path to key for the dedicated secure-host probe (see private skill for details) |
| Additional probe-services vars | Documented in the private skill (citadel identity, VLAN source map) |
---

## API Endpoints

### proxmox_config

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/proxmox-config` | List all entries |
| POST | `/api/v1/proxmox-config` | Create entry |
| POST | `/api/v1/proxmox-config/bulk` | Bulk upsert |
| GET | `/api/v1/proxmox-config/probe/status` | Last probe status |
| POST | `/api/v1/proxmox-config/probe` | Run full PVE probe |
| GET | `/api/v1/proxmox-config/{config_id}` | Get single entry |
| PUT | `/api/v1/proxmox-config/{config_id}` | Update entry |
| DELETE | `/api/v1/proxmox-config/{config_id}` | Delete entry |

### proxmox_nets

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/proxmox-nets` | List all NICs (optional `?config_id=`) |
| POST | `/api/v1/proxmox-nets/bulk` | Bulk upsert NICs |
| POST | `/api/v1/proxmox-nets/enrich-from-pfsense` | Fill IPs from pfSense DNS table |
| POST | `/api/v1/proxmox-nets/enrich-from-pfsense-arp` | Fill IPs from pfSense live ARP table |
| POST | `/api/v1/proxmox-nets/find-ips-via-pve` | arping from PVE hosts across VLANs |
| POST | `/api/v1/proxmox-nets/find-ips-by-arp` | arping from this node's local interfaces |
| POST | `/api/v1/proxmox-nets/find-ips-via-qemu-agent` | Query QEMU guest agent for running VM IPs |
| POST | `/api/v1/proxmox-nets/find-ips-via-pfsense-sweep` | PHP ping sweep via pfSense SSH, read ARP |
| PUT | `/api/v1/proxmox-nets/{net_id}` | Update a NIC record |
| DELETE | `/api/v1/proxmox-nets/{net_id}` | Delete a NIC record |

---

## Probe Flow

`POST /api/v1/proxmox-config/probe` SSHes to each registered PVE host
(from the `pve_hosts` table) using `PROXMOX_SSH_KEY`, reads LXC and QEMU
config files under `/etc/pve/`, and upserts `proxmox_config` + `proxmox_nets`
rows. After probe it calls `fill_vlan_tags_from_cidrs` to back-fill any
missing `vlan_tag` values.

---

## IP Discovery Methods

Run after a probe to fill in missing `ip_address` values on NIC rows. Use
multiple methods — each covers different VM types and network topologies.

### 1. Enrich from pfSense DNS (`enrich-from-pfsense`)

Cross-references `pfsense_dns` entries against MAC addresses in `proxmox_nets`.
Best for LXCs and VMs that are registered in pfSense DNS/DHCP.

Requires: `PFSENSE_SSH_TARGET`, `PFSENSE_SSH_KEY`

### 2. Enrich from pfSense ARP (`enrich-from-pfsense-arp`)

SSHes to pfSense and reads the live ARP table (`arp -an`). Matches MACs to
`proxmox_nets` rows. Only finds hosts that have recently communicated through
the firewall.

Requires: `PFSENSE_SSH_TARGET`, `PFSENSE_SSH_KEY`

### 3. Find IPs via QEMU Agent (`find-ips-via-qemu-agent`)

For **running QEMU VMs** with the guest agent installed. SSHes each PVE host
and calls:
```
qm agent <vmid> network-get-interfaces
```
Filters out loopback (127.x) and link-local (169.254.x) addresses. Best method
for running VMs — gets accurate live interface data without network scanning.

Requires: `PROXMOX_SSH_KEY`

### 4. Find IPs via pfSense Sweep (`find-ips-via-pfsense-sweep`)

The most powerful discovery method. SSHes pfSense (which has a leg on every
VLAN), runs a PHP script that:
1. Discovers all VLAN CIDRs from `ifconfig`
2. Background-pings each subnet to populate the ARP cache
3. Reads `arp -a` to collect IP↔MAC mappings

Returns all discovered IPs; seeds the `vlans` table with found CIDRs; then
calls `fill_vlan_tags_from_cidrs`.

**Important**: pfSense runs FreeBSD and has **no Python**. Remote scripts must
be written in PHP (or shell). The approach is:
1. Write the PHP script as a Python string
2. Base64-encode it: `base64.b64encode(script.encode()).decode()`
3. Send via SSH: `echo '<b64>' | b64decode -r | php`

Never try to run Python remotely on pfSense.

Requires: `PFSENSE_SSH_TARGET`, `PFSENSE_SSH_KEY`

### 5. Find IPs by ARP (`find-ips-by-arp`)

Runs `arping` locally from this node's network interfaces. Only reaches VLANs
that this node has a direct L3 interface on.

**Key rules for arping:**
- Timeout: `-w 0.3` (300ms) — do not use `-w 1` (too slow for fleet scanning)
- Always specify interface with `-I <iface>` — without it, arping may select
  loopback and find nothing
- Use `ip addr` to find the right interface for a given subnet

### 6. Find IPs via PVE (`find-ips-via-pve`)

SSHes each PVE host and runs arping from there. Only effective for VLANs where
the PVE host has a direct L3 interface. Same arping rules apply (`-w 0.3`,
specify `-I`).

Limited by PVE host topology — PVE hosts are typically on management VLANs
only. Use pfSense sweep for VLANs not reachable from PVE hosts.

---

## `fill_vlan_tags_from_cidrs` Helper

Called automatically after probe and after each IP discovery endpoint. For
every `proxmox_nets` row that has an `ip_address` but no `vlan_tag`, it:
1. Queries the `vlans` table for all rows with a CIDR
2. Checks if the IP falls within each CIDR using Python `ipaddress`
3. Updates `vlan_tag` and enqueues the change to peers

This means `vlan_tag` will auto-populate for any NIC as soon as its IP is
known, as long as the matching VLAN CIDR is in the `vlans` table.

The `vlans` table is seeded by the PVE probe and can also be enriched by
`find-ips-via-pfsense-sweep`.

---

## GUI — Proxmox Config Tab

The Proxmox Config tab in `.xarta/gui/index.html` renders:
- A searchable table of all VMs/LXCs with expand buttons
- Expandable NIC sub-rows showing: `net_key`, IP, MAC, VLAN, bridge, model
- **✕ delete button** inline on each NIC row — calls
  `DELETE /api/v1/proxmox-nets/{net_id}`. Use to remove NICs that have been
  deleted from Proxmox but still exist in Blueprints.
- Toolbar buttons for each IP discovery endpoint (some hidden until data loaded)

Show/hide logic: discovery buttons appear only when `proxmox_config` rows exist.

---

## Recommended Discovery Order

Run in this sequence for maximum coverage:

1. `POST /api/v1/proxmox-config/probe` — populate tables from PVE config files
2. `enrich-from-pfsense` — fast, uses existing DNS data
3. `enrich-from-pfsense-arp` — live ARP from firewall
4. `find-ips-via-qemu-agent` — running QEMU VMs (accurate, no scanning)
5. `find-ips-via-pfsense-sweep` — broadest coverage, pings all subnets
6. `find-ips-by-arp` / `find-ips-via-pve` — local/PVE arping for any gaps

After all methods, check for remaining NIC rows with `ip_address IS NULL` in
the DB — these are typically stopped VMs with no DHCP/DNS registration, or
CARP backup appliances that are intentionally silent on all VLANs.

---

## Debugging Tips

```bash
# NICs still missing IPs after all discovery:
sqlite3 /opt/blueprints/data/db/blueprints.db \
  "SELECT pm.name, pn.net_key, pn.mac_address, pn.vlan_tag
   FROM proxmox_nets pn
   JOIN proxmox_config pm ON pn.config_id = pm.config_id
   WHERE pn.ip_address IS NULL OR pn.ip_address = ''
   ORDER BY pm.name, pn.net_key;"

# NICs with IP but no vlan_tag (fill_vlan_tags_from_cidrs couldn't match):
sqlite3 /opt/blueprints/data/db/blueprints.db \
  "SELECT pm.name, pn.net_key, pn.ip_address
   FROM proxmox_nets pn
   JOIN proxmox_config pm ON pn.config_id = pm.config_id
   WHERE pn.ip_address IS NOT NULL AND pn.ip_address != 'dhcp'
     AND (pn.vlan_tag IS NULL OR pn.vlan_tag = 0);"
# → Add the missing CIDR to the vlans table then re-run enrich
```

## Fleet Distribution

Both `proxmox_config` and `proxmox_nets` are in `_ALLOWED_TABLES` and sync
automatically to all peers via the standard gen-based protocol. The probe only
needs to run on the node that has SSH access to PVE hosts (`PROXMOX_SSH_KEY`).
