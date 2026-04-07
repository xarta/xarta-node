"""routes_embed_menu_items.py — CMS-driven embedded selector menu configuration.

GET    /api/v1/embed-menu-items                    → list[EmbedMenuItemOut]
GET    /api/v1/embed-menu-items?context=<ctx>      → filtered list
GET    /api/v1/embed-menu-items/config?context=<ctx> → sanitized selector pages payload
GET    /api/v1/embed-menu-items/{item_id}          → EmbedMenuItemOut
PUT    /api/v1/embed-menu-items/{item_id}          → EmbedMenuItemOut
POST   /api/v1/embed-menu-items/seed              → idempotent seed from defaults

menu_context values: 'embed' | 'fallback-ui' | 'db'
  embed       — buttons shown when the selector is embedded in external services
  fallback-ui — buttons shown when on the gui-fallback dashboard pages
  db          — buttons shown when on the database-tables/diagram pages

This table controls action-slot pages only. System controls (paging scarab and
origin button) are intentionally outside the table and remain fixed by selector
runtime rules.

All data writes call enqueue_for_all_peers() for fleet sync.
"""

import uuid

from fastapi import APIRouter, HTTPException, Query

from .db import get_conn, increment_gen
from .models import EmbedMenuItemOut, EmbedMenuItemUpdate
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/embed-menu-items", tags=["embed-menu-items"])

_VALID_CONTEXTS = {'embed', 'fallback-ui', 'db'}

_DEFAULT_SEED = [
    # ── context: embed ──────────────────────────────────────────────────────
    {
        "item_key": "embed-menu",
        "label": "Embed Menu",
        "icon_emoji": "🪲",
        "icon_asset": "icons/hieroglyphs/kheper-gold-inverted.svg",
        "page_index": 0,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "fallback-ui",
        "label": "Fallback UI",
        "icon_emoji": "🧰",
        "icon_asset": "icons/ui/house-gold.svg",
        "page_index": 0,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "ui",
        "label": "UI",
        "icon_emoji": "🏠",
        "icon_asset": "icons/ui/starfleet-gold.svg",
        "page_index": 0,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "database-tables",
        "label": "Database Tables",
        "icon_emoji": "🗂️",
        "icon_asset": "icons/ui/database-tables-gold.svg",
        "page_index": 1,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "database-diagram",
        "label": "Database Diagram",
        "icon_emoji": "🕸️",
        "icon_asset": "icons/ui/database-diagram-gold.svg",
        "page_index": 1,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "api-key",
        "label": "API Key",
        "icon_emoji": "🔑",
        "icon_asset": "icons/hieroglyphs/ankh-gold.svg",
        "page_index": 2,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "api-key-test",
        "label": "Test Embedded API Key Modal",
        "icon_emoji": "🗝️",
        "icon_asset": "icons/hieroglyphs/ankh-purple.svg",
        "page_index": 2,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "embed",
    },
    {
        "item_key": "cache-mode",
        "label": "Toggle Fallback Cache Mode",
        "icon_emoji": "♺",
        "icon_asset": "icons/ui/cache-mode-gold.svg",
        "page_index": 2,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "embed",
    },
    # ── context: fallback-ui ─────────────────────────────────────────────────
    # mirrors the hardcoded window.BLUEPRINTS_SELECTOR_BUTTONS in index.html
    {
        "item_key": "synthesis",
        "label": "Synthesis",
        "icon_emoji": "📋",
        "icon_asset": "icons/hieroglyphs/naos-shrine-gold.svg",
        "page_index": 0,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "probes",
        "label": "Probes",
        "icon_emoji": "📡",
        "icon_asset": "icons/hieroglyphs/was-scepter-gold.svg",
        "page_index": 0,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "settings",
        "label": "Settings",
        "icon_emoji": "⚙️",
        "icon_asset": "icons/hieroglyphs/djed-pillar-gold.svg",
        "page_index": 0,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "database-tables",
        "label": "Database Tables",
        "icon_emoji": "🗂️",
        "icon_asset": "icons/ui/database-tables-gold.svg",
        "page_index": 1,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "database-diagram",
        "label": "Database Diagram",
        "icon_emoji": "🕸️",
        "icon_asset": "icons/ui/database-diagram-gold.svg",
        "page_index": 1,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "diag-chip",
        "label": "Toggle Diagnostics Chip",
        "icon_emoji": "⎍",
        "icon_asset": "icons/hieroglyphs/eye-of-horus-gold.svg",
        "page_index": 1,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "api-key-test",
        "label": "Test Embedded API Key Modal",
        "icon_emoji": "🗝️",
        "icon_asset": "icons/hieroglyphs/ankh-purple.svg",
        "page_index": 2,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "cache-mode",
        "label": "Toggle Fallback Cache Mode",
        "icon_emoji": "♺",
        "icon_asset": "icons/ui/cache-mode-gold.svg",
        "page_index": 2,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "hard-refresh",
        "label": "Hard Refresh App Assets",
        "icon_emoji": "⟳",
        "icon_asset": "icons/hieroglyphs/shen-gold.svg",
        "page_index": 2,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "ui",
        "label": "UI",
        "icon_emoji": "🏠",
        "icon_asset": "icons/ui/starfleet-gold.svg",
        "page_index": 3,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    {
        "item_key": "api-key",
        "label": "API Key",
        "icon_emoji": "🔑",
        "icon_asset": "icons/hieroglyphs/ankh-gold.svg",
        "page_index": 4,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "fallback-ui",
    },
    # ── context: db ──────────────────────────────────────────────────────────
    # mirrors database-pages.config.js
    {
        "item_key": "fallback-ui",
        "label": "Fallback UI",
        "icon_emoji": "🧰",
        "icon_asset": "icons/ui/house-gold.svg",
        "page_index": 0,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "ui",
        "label": "UI",
        "icon_emoji": "🏠",
        "icon_asset": "icons/ui/starfleet-gold.svg",
        "page_index": 0,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "embed-menu",
        "label": "Embed Menu",
        "icon_emoji": "🪲",
        "icon_asset": "icons/hieroglyphs/kheper-gold-inverted.svg",
        "page_index": 0,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "database-tables",
        "label": "Database Tables",
        "icon_emoji": "🗂️",
        "icon_asset": "icons/ui/database-tables-gold.svg",
        "page_index": 1,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "database-diagram",
        "label": "Database Diagram",
        "icon_emoji": "🕸️",
        "icon_asset": "icons/ui/database-diagram-gold.svg",
        "page_index": 1,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "api-key",
        "label": "API Key",
        "icon_emoji": "🔑",
        "icon_asset": "icons/hieroglyphs/ankh-gold.svg",
        "page_index": 2,
        "sort_order": 0,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "api-key-test",
        "label": "Test Embedded API Key Modal",
        "icon_emoji": "🗝️",
        "icon_asset": "icons/hieroglyphs/ankh-purple.svg",
        "page_index": 2,
        "sort_order": 1,
        "enabled": 1,
        "menu_context": "db",
    },
    {
        "item_key": "cache-mode",
        "label": "Toggle Fallback Cache Mode",
        "icon_emoji": "♺",
        "icon_asset": "icons/ui/cache-mode-gold.svg",
        "page_index": 2,
        "sort_order": 2,
        "enabled": 1,
        "menu_context": "db",
    },
]

_ALLOWED_KEYS = {
    "fallback-ui",
    "ui",
    "synthesis",
    "probes",
    "settings",
    "database-tables",
    "database-diagram",
    "api-key",
    "api-key-test",
    "cache-mode",
    "hard-refresh",
    "diag-chip",
    "embed-menu",
}


def _row_to_out(row) -> EmbedMenuItemOut:
    return EmbedMenuItemOut(
        item_id=row["item_id"],
        item_key=row["item_key"],
        label=row["label"],
        icon_emoji=row["icon_emoji"],
        icon_asset=row["icon_asset"],
        sound_asset=row["sound_asset"],
        page_index=row["page_index"],
        sort_order=row["sort_order"],
        enabled=row["enabled"],
        menu_context=row["menu_context"] if "menu_context" in row.keys() else "embed",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_dict(row) -> dict:
    return {
        "item_id": row["item_id"],
        "item_key": row["item_key"],
        "label": row["label"],
        "icon_emoji": row["icon_emoji"],
        "icon_asset": row["icon_asset"],
        "sound_asset": row["sound_asset"],
        "page_index": row["page_index"],
        "sort_order": row["sort_order"],
        "enabled": row["enabled"],
        "menu_context": row["menu_context"] if "menu_context" in row.keys() else "embed",
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@router.get("", response_model=list[EmbedMenuItemOut])
async def list_embed_menu_items(
    context: str = Query(default=None, description="Filter by menu_context (embed/fallback-ui/db)")
) -> list[EmbedMenuItemOut]:
    with get_conn() as conn:
        if context and context in _VALID_CONTEXTS:
            rows = conn.execute(
                """
                SELECT * FROM embed_menu_items
                WHERE menu_context=?
                ORDER BY page_index, sort_order, item_key
                """,
                (context,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT * FROM embed_menu_items
                ORDER BY menu_context, page_index, sort_order, item_key
                """
            ).fetchall()
    return [_row_to_out(row) for row in rows]


@router.get("/config")
async def get_embed_menu_config(
    context: str = Query(default="embed", description="menu_context: embed / fallback-ui / db"),
) -> dict:
    """Return sanitized selector pages payload with icon/label metadata.

    Each page is a list of item objects:  {key, icon_asset?, label?}
    The selector applies icon_asset as --bp-ns-icon-asset CSS var and falls
    back to its hardcoded CSS data-URIs if this endpoint fails or the field is absent.
    context defaults to 'embed' for backward compatibility.
    """
    if context not in _VALID_CONTEXTS:
        context = "embed"
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT item_key, icon_asset, label, page_index, sort_order, updated_at
            FROM embed_menu_items
            WHERE enabled=1 AND menu_context=?
            ORDER BY page_index, sort_order, item_key
            """,
            (context,),
        ).fetchall()

    pages_map: dict[int, list[dict]] = {}
    last_updated = ""
    for row in rows:
        key = row["item_key"]
        if key not in _ALLOWED_KEYS:
            continue
        page_index = row["page_index"] if isinstance(row["page_index"], int) else 0
        if page_index < 0:
            continue
        item: dict = {"key": key}
        if row["icon_asset"]:
            item["icon_asset"] = row["icon_asset"]
        if row["label"]:
            item["label"] = row["label"]
        pages_map.setdefault(page_index, []).append(item)
        updated_at = row["updated_at"] or ""
        if updated_at > last_updated:
            last_updated = updated_at

    pages = [pages_map[i] for i in sorted(pages_map.keys()) if pages_map[i]]
    return {
        "pages": pages,
        "count": sum(len(page) for page in pages),
        "updated_at": last_updated,
    }


@router.get("/{item_id}", response_model=EmbedMenuItemOut)
async def get_embed_menu_item(item_id: str) -> EmbedMenuItemOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM embed_menu_items WHERE item_id=?",
            (item_id,),
        ).fetchone()
    if not row:
        raise HTTPException(404, "embed menu item not found")
    return _row_to_out(row)


@router.put("/{item_id}", response_model=EmbedMenuItemOut)
async def update_embed_menu_item(item_id: str, body: EmbedMenuItemUpdate) -> EmbedMenuItemOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM embed_menu_items WHERE item_id=?",
            (item_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, "embed menu item not found")

        next_page_index = body.page_index if body.page_index is not None else existing["page_index"]
        next_sort_order = body.sort_order if body.sort_order is not None else existing["sort_order"]
        next_enabled = body.enabled if body.enabled is not None else existing["enabled"]

        if next_page_index is not None and next_page_index < 0:
            raise HTTPException(400, "page_index must be >= 0")
        if next_sort_order is not None and next_sort_order < 0:
            raise HTTPException(400, "sort_order must be >= 0")
        if next_enabled not in (0, 1):
            raise HTTPException(400, "enabled must be 0 or 1")

        next_item_key = existing["item_key"]
        if next_item_key not in _ALLOWED_KEYS:
            raise HTTPException(400, f"unsupported item_key '{next_item_key}'")

        conn.execute(
            """
            UPDATE embed_menu_items SET
                label       = COALESCE(?, label),
                icon_emoji  = COALESCE(?, icon_emoji),
                icon_asset  = COALESCE(?, icon_asset),
                sound_asset = COALESCE(?, sound_asset),
                page_index  = COALESCE(?, page_index),
                sort_order  = COALESCE(?, sort_order),
                enabled     = COALESCE(?, enabled),
                updated_at  = datetime('now')
            WHERE item_id=?
            """,
            (
                body.label,
                body.icon_emoji,
                body.icon_asset,
                body.sound_asset,
                body.page_index,
                body.sort_order,
                body.enabled,
                item_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM embed_menu_items WHERE item_id=?",
            (item_id,),
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "embed_menu_items", item_id, _row_to_dict(row), gen)
    return _row_to_out(row)


@router.post("/seed")
async def seed_embed_menu_items() -> dict:
    inserted = 0
    skipped = 0
    with get_conn() as conn:
        for item in _DEFAULT_SEED:
            ctx = item.get("menu_context", "embed")
            exists = conn.execute(
                "SELECT item_id FROM embed_menu_items WHERE item_key=? AND menu_context=?",
                (item["item_key"], ctx),
            ).fetchone()
            if exists:
                skipped += 1
                continue

            item_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO embed_menu_items (
                    item_id, item_key, label, icon_emoji, icon_asset, sound_asset,
                    page_index, sort_order, enabled, menu_context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item_id,
                    item["item_key"],
                    item["label"],
                    item["icon_emoji"],
                    item.get("icon_asset"),
                    None,
                    item["page_index"],
                    item["sort_order"],
                    item["enabled"],
                    ctx,
                ),
            )
            gen = increment_gen(conn, "human")
            row = conn.execute(
                "SELECT * FROM embed_menu_items WHERE item_id=?",
                (item_id,),
            ).fetchone()
            enqueue_for_all_peers(conn, "INSERT", "embed_menu_items", item_id, _row_to_dict(row), gen)
            inserted += 1
    return {"inserted": inserted, "skipped": skipped}
