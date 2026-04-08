---
name: blueprints-network-discovery
description: Discover hosts and services on the local network using nmap and custom scripts, stage findings for review, and propose Blueprints database entries. Use when the user asks to scan the network, discover services, find new hosts, populate the database from the network, or check what is running on a subnet.
---

# Blueprints — Network Discovery

Scan reachable networks from this xarta-node, discover hosts and services,
and stage the results for review before committing to the Blueprints database.

## Prerequisites

```bash
# Must be installed on the scanning node
which nmap  || apt install -y nmap
which jq    || apt install -y jq
```

## Scanner coordination (single-scanner lock)

Only one fleet node should actively scan at a time to avoid duplicate
discoveries and conflicting proposals. The lock uses `sync_meta` keys
and is distributed via the normal sync engine.

### Acquire lock

```bash
bash /root/xarta-node/.claude/skills/blueprints-network-discovery/scripts/bp-scan-lock.sh acquire
```

### Release lock

```bash
bash /root/xarta-node/.claude/skills/blueprints-network-discovery/scripts/bp-scan-lock.sh release
```

### Check lock status

```bash
bash /root/xarta-node/.claude/skills/blueprints-network-discovery/scripts/bp-scan-lock.sh status
```

The lock has a configurable TTL (default: 15 minutes). If a node crashes
mid-scan, any other node can claim the expired lock.

## Quick-scan workflow

### 1. Acquire the scanner lock

```bash
bash scripts/bp-scan-lock.sh acquire
```

### 2. Run a scan

```bash
bash scripts/bp-scan.sh <cidr-or-target> [--ports <port-spec>] [--output <file>]
```

Examples:

```bash
# Scan a /24 subnet, common ports
bash scripts/bp-scan.sh 10.0.50.0/24

# Scan specific host, all ports
bash scripts/bp-scan.sh 10.0.50.1 --ports 1-65535

# Scan tailnet range
bash scripts/bp-scan.sh 100.64.0.0/10 --ports 80,443,8080,8443
```

Output is JSON, written to stdout and optionally to `--output <file>`.

### 3. Review results

The scan output is a JSON array of discovered host/port/service tuples.
Review before ingesting:

```bash
cat scan-results.json | jq '.'
```

### 4. Ingest to staging

```bash
bash scripts/bp-scan-ingest.sh scan-results.json
```

This proposes machines and service entries. It does NOT auto-commit —
it outputs proposed API calls for human review or Copilot enrichment.

### 5. Release the lock

```bash
bash scripts/bp-scan-lock.sh release
```

## Scan output format

```json
[
  {
    "ip": "10.0.50.100",
    "hostname": "pve-host",
    "ports": [
      {"port": 8006, "proto": "tcp", "state": "open", "service": "https", "banner": "Proxmox VE API"},
      {"port": 22, "proto": "tcp", "state": "open", "service": "ssh", "banner": "OpenSSH 9.2"}
    ],
    "os_guess": "Linux 6.x",
    "scan_time": "2026-03-11T12:00:00Z",
    "scanner_node": "<node-id>"
  }
]
```

## Deduplication rules

Before proposing a new database entry, the ingest script checks:

1. IP + port already in `service_endpoints` or `services.ports` → propose UPDATE.
2. Hostname matches existing `machines.name` → propose enrichment.
3. Service fingerprint matches existing `services.tags` → propose link.
4. No match → propose new `machines` + `services` entries.

## Node access considerations

Different fleet nodes sit on different VLANs and tailnets. The elected
scanner should ideally be the node with the broadest network access.
When choosing which node to scan from, consider:

- Which VLANs are routable from this node?
- Which tailnet(s) is this node connected to?
- Does this node have access to management networks?

The scanning node's network position determines what it can discover.
Run scans from nodes with appropriate access for the target network.

## Safety

- **Never auto-commit** discovered data to production tables.
- Always stage → review → approve.
- Respect rate limits: use `--max-rate` with nmap on production networks.
- Avoid scanning external/internet targets unless explicitly requested.
- The scanner lock prevents concurrent scans across the fleet.

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/bp-scan.sh` | Run nmap scan, output structured JSON |
| `scripts/bp-scan-ingest.sh` | Parse scan JSON → proposed API calls |
| `scripts/bp-scan-lock.sh` | Acquire/release/check scanner lock |

## Integration with Copilot enrichment

After scanning, use the `blueprints-copilot-enrichment` skill to have
Copilot review the findings, classify services, propose tags, suggest
dependencies, and generate API calls for the Blueprints database.

## MANDATORY - Embedded Menu DB Authority Contract (2026-04-08)

- Database is authoritative for embedded selector action pages in all contexts.
- `page_index` and `sort_order` from DB define order and slot positions.
- JS/runtime may insert placeholder circles only to preserve intentional DB slot gaps.
- Scarab paging control is always shown when multiple pages exist, except when touch ribbon mode is actively in use.
- Fallback is allowed only for embedded controls, and only when DB config fetch fails.
- Do not hardcode or merge local page layouts in a way that overrides DB-defined page order/positions.
