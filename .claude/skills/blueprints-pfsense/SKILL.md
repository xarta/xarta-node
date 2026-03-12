---
name: blueprints-pfsense
description: Probe a pfSense firewall via SSH to harvest DNS resolver entries, ARP tables, DHCP leases, and other infrastructure data into Blueprints database tables. Use when the user wants to scan pfSense, refresh DNS entries, or discover network data from the firewall.
---

# Blueprints pfSense Integration

Probe a pfSense firewall over SSH to extract structured infrastructure data
and store it in Blueprints database tables. Currently supports DNS resolver
harvesting; designed to grow with additional pfSense features (ARP, DHCP,
firewall rules, etc.).

## Prerequisites

- **SSH access to the pfSense appliance** as a non-root user (root SSH is
  typically disabled). The user must provide the SSH username and host address.
- **Read-only operations only** — never write to pfSense config files, restart
  services, or modify the firewall state. All probes use `cat` or similar
  read-only commands.
- The Blueprints API must be running locally (default `http://localhost:8080`).
- The `pfsense_dns` table must exist in the schema (created automatically on
  app startup via `db.py` DDL).

## Safety Rules

1. **DO NOT** overwrite, edit, or lock any files on pfSense.
2. **DO NOT** restart services on pfSense.
3. **DO NOT** run `pfSsh.php` playback commands that modify config.
4. **DO** use `cat` to read config files; pipe output back to the local node
   for parsing.
5. **DO** check SSH connectivity before running probes:
   ```bash
   ssh -o ConnectTimeout=5 <user>@<pfSense-ip> "uname -a"
   ```
6. pfSense runs FreeBSD — standard GNU tools (`head`, `tail`, `awk`) may
   behave differently or be absent. Prefer `cat` piped to local processing.

## Feature 1 — DNS Resolver Harvest

Extracts DNS entries from the Unbound DNS resolver config on pfSense and
populates the `pfsense_dns` table via the bulk API.

### Data Sources on pfSense

| File | Contents |
|------|----------|
| `/var/unbound/host_entries.conf` | Static host overrides (A, AAAA, PTR records) |
| `/var/unbound/dhcpleases_entries.conf` | DHCP lease DNS registrations |
| `/var/unbound/domainoverrides.conf` | Domain-level forward overrides |

### Probe Script

Use the helper script to harvest DNS entries:

```bash
bash /root/xarta-node/.claude/skills/blueprints-pfsense/scripts/bp-pfsense-dns-probe.sh <user>@<pfSense-ip>
```

The script:
1. SSH to pfSense and `cat` each config file
2. Parse locally into structured records (IP, FQDN, record_type, source)
3. Classify entries: `host_override`, `host_override_alias`, `dhcp_lease`
4. POST to `/api/v1/pfsense-dns/bulk` for upsert

### Manual Probe Steps

If the script is unavailable, perform manually:

#### Step 1 — Fetch config files

```bash
ssh <user>@<pfSense-ip> "cat /var/unbound/host_entries.conf" > /tmp/pfsense_host_entries.conf
ssh <user>@<pfSense-ip> "cat /var/unbound/dhcpleases_entries.conf" > /tmp/pfsense_dhcpleases.conf
ssh <user>@<pfSense-ip> "cat /var/unbound/domainoverrides.conf" > /tmp/pfsense_domainoverrides.conf
```

#### Step 2 — Parse and classify

The `host_entries.conf` format uses Unbound `local-data:` directives:

```
local-data-ptr: "192.168.1.10 myhost.example.com"
local-data: "myhost.example.com A 192.168.1.10"
local-data: "alias.example.com A 192.168.1.10"
```

Classification logic:
- **A record with a matching PTR for the same IP+FQDN** → `host_override`
  (this is the canonical/primary DNS entry configured via Host Overrides)
- **A record without a matching PTR** → `host_override_alias`
  (additional hostname aliases pointing to the same IP)
- **Records from `dhcpleases_entries.conf`** → `dhcp_lease`

#### Step 3 — Build DNS entry ID

Use deterministic IDs: `dns-<record_type_letter>-<fqdn>`

- A records: `dns-a-<fqdn>`
- AAAA records: `dns-aaaa-<fqdn>`
- PTR records: `dns-ptr-<fqdn>` (where fqdn is the PTR value, not the
  in-addr.arpa form)

#### Step 4 — Bulk insert via API

```bash
curl -s -X POST http://localhost:8080/api/v1/pfsense-dns/bulk \
  -H 'Content-Type: application/json' \
  -d '{"entries": [...]}'
```

Each entry:
```json
{
  "dns_entry_id": "dns-a-myhost.example.com",
  "ip_address": "192.168.1.10",
  "fqdn": "myhost.example.com",
  "record_type": "A",
  "source": "host_override",
  "active": true,
  "last_seen": "2025-01-01T00:00:00"
}
```

### pfsense_dns Table Schema

| Column | Type | Notes |
|--------|------|-------|
| dns_entry_id | TEXT PK | `dns-<type>-<fqdn>` |
| ip_address | TEXT | IPv4 or IPv6 address |
| fqdn | TEXT | Fully qualified domain name |
| record_type | TEXT | `A`, `AAAA`, or `PTR` |
| source | TEXT | `host_override`, `host_override_alias`, or `dhcp_lease` |
| mac_address | TEXT | MAC if available (nullable) |
| active | BOOLEAN | 1 = active, 0 = stale |
| last_seen | TEXT | ISO 8601 timestamp of last probe |
| last_probed | TEXT | ISO 8601 timestamp of probe run |
| created_at | TEXT | Auto-set on insert |
| updated_at | TEXT | Auto-set on update |

Indexes: `ip_address`, `fqdn`

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/pfsense-dns` | List all DNS entries |
| POST | `/api/v1/pfsense-dns` | Create single entry |
| GET | `/api/v1/pfsense-dns/{id}` | Get entry by ID |
| PATCH | `/api/v1/pfsense-dns/{id}` | Update entry |
| DELETE | `/api/v1/pfsense-dns/{id}` | Delete entry |
| POST | `/api/v1/pfsense-dns/bulk` | Bulk upsert entries |

### GUI

The pfSense DNS tab is available in both the fallback UI (`gui-fallback/index.html`)
and private UI (`.xarta/gui/index.html`). It displays all columns with search
filtering.

## Future Features (Planned)

These are not yet implemented but the skill is designed to accommodate them:

- **ARP Table Harvest** — `arp -an` on pfSense to map IP↔MAC, could enrich
  `pfsense_dns.mac_address` or populate a new `pfsense_arp` table
- **DHCP Leases** — parse `/var/dhcpd/var/db/dhcpd.leases` for active leases
  with MAC, IP, hostname, expiry
- **Firewall Rules** — export active rules for documentation
- **Interface Status** — enumerate WAN/LAN/VLAN interfaces and their IPs

## Audit / Undo

All DNS entries can be removed with:
```bash
# Delete all pfsense_dns entries
python3 -c "
import sqlite3
conn = sqlite3.connect('/opt/blueprints/data/db/blueprints.db')
conn.execute('DELETE FROM pfsense_dns')
conn.commit()
print(f'Deleted {conn.total_changes} rows')
"
```

Or selectively by source:
```bash
python3 -c "
import sqlite3, sys
source = sys.argv[1]
conn = sqlite3.connect('/opt/blueprints/data/db/blueprints.db')
conn.execute('DELETE FROM pfsense_dns WHERE source = ?', (source,))
conn.commit()
print(f'Deleted {conn.total_changes} rows with source={source}')
" host_override_alias
```

The probe script also logs its actions to stdout for traceability.

## Fleet Distribution

The `pfsense_dns` table is included in `_ALLOWED_TABLES` in `routes_sync.py`,
so entries sync automatically to all fleet nodes via the standard gen-based
sync protocol. No additional fleet distribution steps needed.
