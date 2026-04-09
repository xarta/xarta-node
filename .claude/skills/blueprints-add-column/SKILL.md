---
name: blueprints-add-column
description: Add a new column to a Blueprints app table (services, machines, or nodes) and safely distribute the schema change across the fleet. Use when the user wants to add a new field to the service index, extend a table, or asks about schema changes in Blueprints.
---

# Blueprints — Adding Columns

## Overview

Adding a column requires changes in exactly four files. The sync layer itself
(`routes_sync.py`) needs **no changes** — `_apply_action()` is fully dynamic
and builds SQL from `row_data` keys at runtime.

## The four touch-points

| File | What to change |
|------|----------------|
| `blueprints-app/app/db.py` | Add a tuple to `_run_migrations()` |
| `blueprints-app/app/models.py` | Add field to `*Create`, `*Update`, and `*Out` Pydantic classes |
| `blueprints-app/app/routes_<table>.py` | Add to `INSERT`/`UPDATE` SQL, `_row_to_out()`, and the `row_data` dict |
| `routes_sync.py` | No change needed |

---

## Step 1 — db.py: add the migration

`_run_migrations()` contains a list of `(table, column, col_type)` tuples.
Add your new column here. This runs idempotently on every app start via
`ALTER TABLE ADD COLUMN` guarded by `PRAGMA table_info`.

```python
# blueprints-app/app/db.py  — inside _run_migrations()
migrations = [
    ("nodes",    "ui_url",    "TEXT"),  # existing example
    ("services", "new_field", "TEXT"),  # add yours here
]
```

SQLite `ALTER TABLE ADD COLUMN` rules:
- Column is always appended (no position control).
- Default is `NULL` unless you specify `DEFAULT <value>`.
- Use `TEXT` for strings, JSON arrays/objects (stored as JSON text), and optional flags.
- Use `INTEGER` for numeric or boolean (0/1) values.
- `PRIMARY KEY` and `UNIQUE` constraints are not supported via ALTER — those need
  a new table migration. See the **blueprints-add-table** skill for that.

---

## Step 2 — models.py: extend Pydantic models

Add the field to all three model classes for the table. `Optional` fields with
a `None` default keep existing API clients working without changes.

```python
# blueprints-app/app/models.py

class ServiceCreate(BaseModel):
    # ...
    new_field: Optional[str] = None

class ServiceUpdate(BaseModel):
    # ...
    new_field: Optional[str] = None

class ServiceOut(BaseModel):
    # ...
    new_field: Optional[str] = None
```

Type reference:
- Plain text → `Optional[str] = None`
- JSON array of strings (like `tags`, `ports`) → `Optional[list[str]] = None`
- JSON array of dicts (like `links`) → `Optional[list[dict[str, str]]] = None`

---

## Step 3 — routes file: wire up SQL and serialisation

Update the relevant `routes_services.py`, `routes_machines.py`, etc.
Three sub-locations need editing:

### 3a. `_row_to_out()` — deserialise from DB row

```python
def _row_to_out(row) -> ServiceOut:
    return ServiceOut(
        # ...existing fields...
        new_field=row["new_field"],           # plain TEXT
        # new_field=_loads(row["new_field"]), # JSON column variant
    )
```

### 3b. `create_<entity>()` — INSERT

Add the column to the SQL column list, the values tuple, and the `row_data`
dict that gets enqueued for peers:

```python
conn.execute(
    """
    INSERT INTO services
        (..., new_field)
    VALUES (..., ?)
    """,
    (..., body.new_field),           # plain TEXT
    # (..., _dumps(body.new_field)), # JSON column variant
)
# Then in the row_data dict passed to enqueue_for_all_peers:
row_data = {
    # ...
    "new_field": row["new_field"],
}
```

### 3c. `update_<entity>()` — UPDATE

Include the column in the partial-update logic:

```python
if body.new_field is not None:
    updates["new_field"] = body.new_field
    # updates["new_field"] = _dumps(body.new_field)  # JSON variant
```

---

## Why `routes_sync.py` needs no changes

`_apply_action()` dynamically constructs SQL from whatever keys are present in
`row_data`. As long as the column exists in the peer's schema before the action
is applied, it just works. The migration in Step 1 ensures that.

---

## Distribution and rolling-restart safety

### Normal deployment sequence

```
1. Make all changes, git commit, git push
2. Trigger fleet pull from the originating node:

   curl -X POST http://localhost:8080/api/v1/sync/git-pull \
        -H 'Content-Type: application/json' \
        -d '{"scope":"outer"}'

3. This node restarts → init_db() runs → migration adds the column
4. A sync_git_outer action is queued for every peer in the DB
5. Drain loop delivers it → each peer git-pulls, restarts, migrates
6. Full propagation typically completes within ~30 seconds per hop
```

### What happens if a record with the new column reaches a peer before it has migrated

The drain loop sends the action to a peer whose column doesn't yet exist.
SQLite raises `OperationalError: table has no column named new_field`.
The peer returns HTTP 500. The drain loop logs a warning and does **not** call
`mark_sent()` — the action remains at the front of the FIFO queue and retries
on the next drain cycle (1–20 s jitter). Once the peer restarts and its
migration runs, the retry succeeds. **No data is lost.**

Overflow safety net: if retries accumulate to `SYNC_QUEUE_MAX_DEPTH`, the drain
switches to sending a full DB backup to that peer (which already contains the
migrated schema and all data).

Important (2026-04-07): restore can be rejected with HTTP 409 by the receiver's
generation guard (`sender_gen <= my_gen`). In that case drain must fall back to
batched `/api/v1/sync/actions` delivery and continue draining, not stay pinned
at overflow depth.

### Optional: wait for peers before writing new-column data

To avoid retry noise in the logs, check that all peers have caught up first:

```bash
curl http://<peer>/api/v1/sync/status
```

Verify `gen` matches the originating node's gen, then write freely.

Before any fleet-wide pull/update action, verify pending queue rows are trending
down (or fully drained) on the origin node:

```bash
sqlite3 /opt/blueprints/data/db/blueprints.db \
    "select target_node_id, count(*) from sync_queue where sent=0 group by target_node_id order by target_node_id;"
```

---

## Checklist

- [ ] `db.py` — tuple added to `_run_migrations()`
- [ ] `models.py` — field added to `*Create`, `*Update`, `*Out`
- [ ] `routes_<table>.py` — `INSERT`, `UPDATE`, `_row_to_out()`, `row_data` dict updated
- [ ] `routes_sync.py` — confirmed no changes needed
- [ ] Committed and pushed to GitHub
- [ ] `git-pull` triggered (`scope: "outer"`)
- [ ] Peers verified at `/api/v1/sync/status` (optional but tidy)

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

When asked to commit and push ALL repos always including the lone wolf repo.  Lone wolf repo is specific to each node and not distributed.  Sometimes you'll be asked to also commit and push each lone wolf repo on each node separately via ssh.  That is a separate concern to commit and push all repos.
