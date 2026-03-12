````skill
---
name: blueprints-add-table
description: Add a brand-new table to the Blueprints app with full CRUD API, fleet sync, and optional GUI tab. Use when the user wants to introduce a new data entity (not just a new column on an existing table).
---

# Blueprints — Adding a New Table

## Overview

Adding a new table requires changes in **six files** (plus an optional GUI
file). Missing any of them — especially the two entries in `routes_sync.py` —
will cause HTTP 500 sync errors across the fleet until the node is restarted
with the corrected code.

> **Lesson learned:** `arp_manual` was added with the correct `_ALLOWED_TABLES`
> entry but the `_pk_for_table` entry was omitted. The receiver fell through to
> the default PK `"id"`, generated `ON CONFLICT(id)` SQL, got a SQLite
> `OperationalError`, and returned HTTP 500 — blocking the drain queue for all
> fleet peers until the fix was deployed. **Always add both entries.**

---

## Touch-point checklist

| # | File | What to add |
|---|------|-------------|
| 1 | `blueprints-app/app/db.py` | `CREATE TABLE IF NOT EXISTS` + indexes in `_SCHEMA` |
| 2 | `blueprints-app/app/models.py` | `*Create`, `*Update`, `*Out` Pydantic models |
| 3 | `blueprints-app/app/routes_<table>.py` | New file — full CRUD endpoints |
| 4 | `blueprints-app/app/main.py` | Import router + `include_router(...)` |
| 5 | `blueprints-app/app/routes_sync.py` | **TWO entries** — see critical section below |
| 6 | `.xarta/gui/index.html` | Tab button + section + JS (if GUI tab needed) |

---

## Step 1 — db.py: DDL

Append the `CREATE TABLE IF NOT EXISTS` block and any indexes to the `_SCHEMA`
string (or the SQL block passed to `conn.executescript`). Use `TEXT PRIMARY KEY`
for UUID PKs.

```python
# Inside the schema string / executescript block in db.py
CREATE TABLE IF NOT EXISTS my_table (
    entry_id    TEXT PRIMARY KEY,
    some_field  TEXT NOT NULL,
    notes       TEXT,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_my_table_some_field ON my_table(some_field);
```

Naming convention:
- PK column: `<singular>_id` (e.g. `entry_id`, `stack_id`, `config_id`)
- Generate PKs with `str(uuid.uuid4())` in the create endpoint

---

## Step 2 — models.py: Pydantic models

Add three classes before the sync models section:

```python
class MyTableCreate(BaseModel):
    some_field: str
    notes: Optional[str] = None

class MyTableUpdate(BaseModel):
    some_field: Optional[str] = None
    notes: Optional[str] = None

class MyTableOut(BaseModel):
    entry_id: str
    some_field: str
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
```

---

## Step 3 — routes_<table>.py: CRUD endpoints

Create `blueprints-app/app/routes_my_table.py`. Required patterns:

```python
import uuid
from fastapi import APIRouter, HTTPException
from .db import get_conn
from .sync.queue import enqueue_for_all_peers
from .sync.gen import increment_gen
from .models import MyTableCreate, MyTableUpdate, MyTableOut

router = APIRouter(tags=["my-table"])

def _row_to_out(row) -> MyTableOut:
    return MyTableOut(
        entry_id=row["entry_id"],
        some_field=row["some_field"],
        notes=row["notes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )

@router.get("/my-table", response_model=list[MyTableOut])
async def list_my_table():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM my_table ORDER BY created_at").fetchall()
    return [_row_to_out(r) for r in rows]

@router.post("/my-table", response_model=MyTableOut, status_code=201)
async def create_my_table_entry(body: MyTableCreate):
    entry_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO my_table (entry_id, some_field, notes) VALUES (?, ?, ?)",
            (entry_id, body.some_field, body.notes),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM my_table WHERE entry_id=?", (entry_id,)).fetchone()
        row_data = {"entry_id": entry_id, "some_field": row["some_field"], "notes": row["notes"],
                    "created_at": row["created_at"], "updated_at": row["updated_at"]}
        enqueue_for_all_peers(conn, "INSERT", "my_table", entry_id, row_data, gen)
    return _row_to_out(row)

@router.get("/my-table/{entry_id}", response_model=MyTableOut)
async def get_my_table_entry(entry_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM my_table WHERE entry_id=?", (entry_id,)).fetchone()
    if not row:
        raise HTTPException(404, "entry not found")
    return _row_to_out(row)

@router.put("/my-table/{entry_id}", response_model=MyTableOut)
async def update_my_table_entry(entry_id: str, body: MyTableUpdate):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM my_table WHERE entry_id=?", (entry_id,)).fetchone()
        if not row:
            raise HTTPException(404, "entry not found")
        conn.execute(
            """UPDATE my_table SET
               some_field = COALESCE(?, some_field),
               notes      = COALESCE(?, notes),
               updated_at = datetime('now')
               WHERE entry_id = ?""",
            (body.some_field, body.notes, entry_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM my_table WHERE entry_id=?", (entry_id,)).fetchone()
        row_data = {"entry_id": entry_id, "some_field": row["some_field"], "notes": row["notes"],
                    "created_at": row["created_at"], "updated_at": row["updated_at"]}
        enqueue_for_all_peers(conn, "UPDATE", "my_table", entry_id, row_data, gen)
    return _row_to_out(row)

@router.delete("/my-table/{entry_id}", status_code=204)
async def delete_my_table_entry(entry_id: str):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM my_table WHERE entry_id=?", (entry_id,)).fetchone()
        if not row:
            raise HTTPException(404, "entry not found")
        conn.execute("DELETE FROM my_table WHERE entry_id=?", (entry_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "my_table", entry_id, {}, gen)
```

---

## Step 4 — main.py: register the router

```python
from .routes_my_table import router as my_table_router
# ...
application.include_router(my_table_router, prefix="/api/v1")
```

---

## Step 5 — routes_sync.py: TWO MANDATORY ENTRIES ⚠️

This is where the **`arp_manual` incident** happened. Both entries are required.
Missing either one causes HTTP 500 on all peer sync attempts:

### 5a. Add to `_ALLOWED_TABLES`

```python
_ALLOWED_TABLES = {
    "services", "machines", "nodes",
    "pfsense_dns",
    "proxmox_config", "proxmox_nets", "vlans", "dockge_stacks", "caddy_configs",
    "settings", "pve_hosts",
    "arp_manual",
    "my_table",   # ← add here
}
```

### 5b. Add to `_pk_for_table` ← THE ONE THAT'S EASY TO FORGET

```python
def _pk_for_table(table: str) -> str:
    pk_map = {
        "services":       "service_id",
        "machines":       "machine_id",
        "nodes":          "node_id",
        "pfsense_dns":    "dns_entry_id",
        "proxmox_config": "config_id",
        "proxmox_nets":   "net_id",
        "vlans":          "vlan_id",
        "dockge_stacks":  "stack_id",
        "caddy_configs":  "caddy_id",
        "settings":       "key",
        "pve_hosts":      "pve_id",
        "arp_manual":     "entry_id",
        "my_table":       "entry_id",  # ← add here — must match actual PK column name
    }
    return pk_map.get(table, "id")
```

> If the PK column name is wrong, the receiver generates SQL like
> `ON CONFLICT(wrong_col)` which raises `OperationalError` → HTTP 500 → drain
> retries forever. The default fallback is `"id"` which almost certainly does
> not exist on a Blueprints table.

---

## Step 6 — GUI tab (if needed)

In `.xarta/gui/index.html`:

1. Add tab button after the last existing tab:
   ```html
   <button onclick="switchTab('my-table')">My Table</button>
   ```

2. Add section:
   ```html
   <section id="tab-my-table" style="display:none">
     <h2>My Table</h2>
     <!-- table + add button -->
   </section>
   ```

3. Add state var: `let _myTable = [];`

4. Lazy-load in `switchTab`: `if (tab === 'my-table' && !_myTable.length) loadMyTable();`

5. Implement: `loadMyTable()`, `renderMyTable()`, `addMyTableEntry()`,
   `editMyTableEntry()`, `deleteMyTableEntry()`

---

## Deployment sequence

```
1. git commit -am "feat: add my_table — ..."
2. git push
3. Restart local service (picks up DDL, creates table):
   systemctl restart blueprints-app
4. Trigger fleet pull (outer):
   curl -X POST http://localhost:8080/api/v1/sync/git-pull \
        -H 'Content-Type: application/json' -d '{"scope":"outer"}'
5. Fleet nodes pull, restart, schema creates on each
6. Verify drain clears:
   sqlite3 /opt/blueprints/data/db/blueprints.db \
     "SELECT sent, COUNT(*) FROM sync_queue GROUP BY sent;"
   # All rows should be sent=1 within ~60 s
```

### What happens if you forget the `git-pull` broadcast

Fleet nodes won't have the new `_ALLOWED_TABLES` entry. When the drain tries
to sync new-table rows to a peer, the peer returns HTTP 500 (unknown table or
wrong PK). The drain retries every 1–20 s. **No data is lost** — items stay in
`sync_queue` with `sent=0`. Fix: SSH-restart each fleet node after pulling.

---

## Full checklist

- [ ] `db.py` — `CREATE TABLE IF NOT EXISTS` + indexes in schema
- [ ] `models.py` — `*Create`, `*Update`, `*Out` added
- [ ] `routes_<table>.py` — new file, CRUD, `enqueue_for_all_peers`, `increment_gen`
- [ ] `main.py` — router imported and registered
- [ ] `routes_sync.py` — **`_ALLOWED_TABLES`** entry added
- [ ] `routes_sync.py` — **`_pk_for_table`** entry added (correct PK column name!)
- [ ] `.xarta/gui/index.html` — tab + section + JS (if GUI needed) → commit to private repo
- [ ] Committed and pushed to public repo
- [ ] Local service restarted
- [ ] `git-pull` triggered (`scope: "outer"`) to fleet
- [ ] Drain verified clean (`sent=1` for all queue rows)
````
