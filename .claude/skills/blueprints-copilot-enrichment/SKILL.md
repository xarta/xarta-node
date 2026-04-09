---
name: blueprints-copilot-enrichment
description: Enrich, classify, and fill in missing Blueprints database entries using Copilot's access to the codebase, network context, and the Blueprints REST API. Use when the user asks Copilot to populate, enhance, classify, tag, or review service/machine records, or to process network discovery results.
---

# Blueprints — Copilot Enrichment

Use this skill when the user asks you to enrich, classify, fill in, or
review Blueprints database records. This includes processing network
discovery scan results and proposing database updates.

## When to use

- User asks to "fill in" or "enrich" service/machine records.
- User asks to review network scan results and propose database entries.
- User asks to classify services (kind, exposure, tags).
- User asks to find and propose dependencies between services.
- User asks to populate machines from known infrastructure.

## API endpoints

All operations go through the Blueprints REST API at `http://localhost:8080`:

| Method | Endpoint | Purpose |
|--------|----------|---------|
| GET | `/api/v1/services` | List all services |
| POST | `/api/v1/services` | Create a service |
| PUT | `/api/v1/services/{id}` | Update a service |
| GET | `/api/v1/machines` | List all machines |
| POST | `/api/v1/machines` | Create a machine |
| PUT | `/api/v1/machines/{id}` | Update a machine |
| GET | `/api/v1/nodes` | List fleet nodes |
| GET | `/api/v1/schema` | Get live schema + relationships |
| GET | `/health` | Health check |

## Enrichment workflow

### Step 1: Gather current state

```bash
curl -s http://localhost:8080/api/v1/services | python3 -m json.tool
curl -s http://localhost:8080/api/v1/machines | python3 -m json.tool
```

### Step 2: Identify gaps

For each service, check:
- Is `host_machine_id` set? (Should point to a `machines` record.)
- Is `service_kind` meaningful? (Default is `app` — refine to `api`, `web`, `db`, `proxy`, `dns`, `infra-ui`.)
- Is `exposure_level` accurate? (`internal`, `tailnet`, `lan`, `public`, `restricted`.)
- Are `tags` descriptive? (Add technology, purpose, location tags.)
- Is `health_path` set for HTTP services?
- Are `dependencies` declared?

### Step 3: Propose updates

Generate `curl` commands or use the API directly to update records:

```bash
# Example: enrich a service with classification
curl -X PUT http://localhost:8080/api/v1/services/litellm \
  -H "Content-Type: application/json" \
  -d '{
    "service_kind": "api",
    "exposure_level": "tailnet",
    "health_path": "/health",
    "host_machine_id": "lxc-841"
  }'
```

### Step 4: Populate machines

Build the machine hierarchy from known infrastructure:

```bash
# Proxmox baremetal host
curl -X POST http://localhost:8080/api/v1/machines \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "<pve-id>",
    "name": "<pve-name>",
    "type": "baremetal",
    "machine_kind": "proxmox",
    "platform": "proxmox",
    "status": "active"
  }'

# LXC inside a Proxmox host
curl -X POST http://localhost:8080/api/v1/machines \
  -H "Content-Type: application/json" \
  -d '{
    "machine_id": "<lxc-id>",
    "name": "<lxc-name>",
    "type": "lxc",
    "machine_kind": "lxc",
    "parent_machine_id": "<pve-id>",
    "platform": "linux",
    "status": "active"
  }'
```

## Classification reference

### `service_kind` values

| Value | Use for |
|-------|---------|
| `app` | General purpose applications (default) |
| `api` | REST/gRPC API endpoints |
| `web` | Human-facing web UIs |
| `db` | Databases (PostgreSQL, SQLite, Redis, etc.) |
| `proxy` | Reverse proxies, load balancers (Caddy, nginx) |
| `dns` | DNS services (CoreDNS, Pi-hole, pfSense DNS) |
| `infra-ui` | Infrastructure management UIs (Proxmox, Portainer, PiKVM) |
| `llm` | LLM inference endpoints |
| `tts` | Text-to-speech services |
| `monitor` | Monitoring and observability |
| `storage` | File/object storage services |

### `exposure_level` values

| Value | Meaning |
|-------|---------|
| `internal` | Only reachable within the host or container (default) |
| `lan` | Reachable on the local VLAN/subnet |
| `tailnet` | Reachable via Tailscale/Headscale overlay |
| `public` | Exposed to the internet |
| `restricted` | Reachable only from specific networks/hosts |

### `machine_kind` values

| Value | Use for |
|-------|---------|
| `baremetal` | Physical server hardware |
| `proxmox` | Proxmox VE hypervisor (physical or nested) |
| `vm` | Virtual machine |
| `lxc` | LXC container |
| `docker` | Docker container host or container |
| `switch` | Network switch |
| `router` | Network router (pfSense, etc.) |
| `firewall` | Dedicated firewall appliance |
| `pikvm` | PiKVM device |

## Processing scan results

When given network scan output (from `bp-scan.sh`), follow this process:

1. **Read the scan JSON** — note IPs, ports, service fingerprints.
2. **Cross-reference with existing records** — query the API for current services and machines.
3. **Classify each discovery:**
   - Known service on known machine → propose UPDATE with new endpoint data.
   - Unknown service on known machine → propose CREATE service linked to that machine.
   - Unknown host entirely → propose CREATE machine + CREATE service(s).
4. **Infer relationships:**
   - Services on the same host → likely share machine dependencies.
   - Known service patterns (e.g. port 8006 = Proxmox) → set `service_kind: infra-ui`.
   - Database ports → look for services that might depend on them.
5. **Present proposals** as structured curl commands for human review.

## Important rules

- **Never auto-execute** proposed changes without user approval.
- **Always show the full proposed API call** before executing.
- **Check for duplicates** before proposing new entries.
- **Preserve existing data** — use PUT for updates, don't overwrite fields unnecessarily.
- **Use the schema endpoint** (`/api/v1/schema`) to verify current table structure.

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
