"""routes_settings.py — CRUD for /api/v1/settings (key-value config store)"""

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen, set_setting
from .models import SettingOut, SettingUpsert
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/settings", tags=["settings"])


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[SettingOut])
async def list_settings() -> list[SettingOut]:
    """Return all settings ordered by key."""
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM settings ORDER BY key").fetchall()
    return [SettingOut(**dict(r)) for r in rows]


@router.get("/frontend-settings", response_model=dict[str, str])
async def get_frontend_settings() -> dict[str, str]:
    """Return all fe.* settings as a flat {key: value} map for JS clients (prefix stripped)."""
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT key, value FROM settings WHERE key LIKE 'fe.%' ORDER BY key"
        ).fetchall()
    return {row[0][3:]: row[1] for row in rows}


@router.put("/frontend-settings/{key}", response_model=SettingOut)
async def upsert_frontend_setting(key: str, body: SettingUpsert) -> SettingOut:
    """Create or update a frontend setting (stored as fe.<key>). Triggers fleet sync."""
    full_key = f"fe.{key}"
    with get_conn() as conn:
        gen = increment_gen(conn, f"settings-{full_key}")
        set_setting(conn, full_key, body.value, body.description)
        row = conn.execute("SELECT * FROM settings WHERE key=?", (full_key,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "settings", full_key, dict(row), gen)
    return SettingOut(**dict(row))


@router.get("/{key}", response_model=SettingOut)
async def get_one_setting(key: str) -> SettingOut:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM settings WHERE key=?", (key,)).fetchone()
    if not row:
        raise HTTPException(404, f"Setting '{key}' not found")
    return SettingOut(**dict(row))


@router.put("/{key}", response_model=SettingOut)
async def upsert_setting(key: str, body: SettingUpsert) -> SettingOut:
    """Create or update a setting.  Triggers fleet sync."""
    with get_conn() as conn:
        gen = increment_gen(conn, f"settings-{key}")
        set_setting(conn, key, body.value, body.description)
        row = conn.execute("SELECT * FROM settings WHERE key=?", (key,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "settings", key, dict(row), gen)
    return SettingOut(**dict(row))


@router.delete("/{key}", status_code=204)
async def delete_setting(key: str) -> None:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT key FROM settings WHERE key=?", (key,)
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"Setting '{key}' not found")
        gen = increment_gen(conn, f"settings-delete-{key}")
        conn.execute("DELETE FROM settings WHERE key=?", (key,))
        enqueue_for_all_peers(conn, "DELETE", "settings", key, None, gen)
