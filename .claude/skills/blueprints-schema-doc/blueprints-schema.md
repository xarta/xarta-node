# Blueprints Database Schema

*Generated: 2026-03-11 15:44:42*

---

## Relationships Summary

- `machines.parent_machine_id` → `machines.machine_id`
- `sync_queue.target_node_id` → `nodes.node_id`

---

## `machines`

| Column | Type | Null? | Default | Notes |
|--------|------|:-----:|---------|-------|
| `machine_id` | `TEXT` | ✓ | — | **PK** |
| `name` | `TEXT` | ✗ | — | — |
| `type` | `TEXT` | ✗ | — | — |
| `parent_machine_id` | `TEXT` | ✓ | — | → `machines(machine_id)` |
| `ip_addresses` | `TEXT` | ✓ | — | — |
| `description` | `TEXT` | ✓ | — | — |
| `created_at` | `TEXT` | ✓ | `datetime('now')` | — |
| `updated_at` | `TEXT` | ✓ | `datetime('now')` | — |

## `nodes`

| Column | Type | Null? | Default | Notes |
|--------|------|:-----:|---------|-------|
| `node_id` | `TEXT` | ✓ | — | **PK** |
| `display_name` | `TEXT` | ✗ | — | — |
| `host_machine` | `TEXT` | ✓ | — | — |
| `tailnet` | `TEXT` | ✓ | — | — |
| `addresses` | `TEXT` | ✓ | — | — |
| `ui_url` | `TEXT` | ✓ | — | — |
| `last_seen` | `TEXT` | ✓ | — | — |
| `created_at` | `TEXT` | ✓ | `datetime('now')` | — |

## `services`

| Column | Type | Null? | Default | Notes |
|--------|------|:-----:|---------|-------|
| `service_id` | `TEXT` | ✓ | — | **PK** |
| `name` | `TEXT` | ✗ | — | — |
| `description` | `TEXT` | ✓ | — | — |
| `host_machine` | `TEXT` | ✓ | — | — |
| `vm_or_lxc` | `TEXT` | ✓ | — | — |
| `ports` | `TEXT` | ✓ | — | — |
| `caddy_routes` | `TEXT` | ✓ | — | — |
| `dns_info` | `TEXT` | ✓ | — | — |
| `credential_hints` | `TEXT` | ✓ | — | — |
| `dependencies` | `TEXT` | ✓ | — | — |
| `project_status` | `TEXT` | ✓ | `'deployed'` | — |
| `tags` | `TEXT` | ✓ | — | — |
| `links` | `TEXT` | ✓ | — | — |
| `created_at` | `TEXT` | ✓ | `datetime('now')` | — |
| `updated_at` | `TEXT` | ✓ | `datetime('now')` | — |

## `sync_meta`

| Column | Type | Null? | Default | Notes |
|--------|------|:-----:|---------|-------|
| `key` | `TEXT` | ✓ | — | **PK** |
| `value` | `TEXT` | ✗ | — | — |

## `sync_queue`

| Column | Type | Null? | Default | Notes |
|--------|------|:-----:|---------|-------|
| `queue_id` | `INTEGER` | ✓ | — | **PK** |
| `target_node_id` | `TEXT` | ✗ | — | → `nodes(node_id)` |
| `action_type` | `TEXT` | ✗ | — | — |
| `table_name` | `TEXT` | ✗ | — | — |
| `row_id` | `TEXT` | ✗ | — | — |
| `row_data` | `TEXT` | ✓ | — | — |
| `gen` | `INTEGER` | ✗ | — | — |
| `created_at` | `TEXT` | ✓ | `datetime('now')` | — |
| `sent` | `INTEGER` | ✓ | `0` | — |

