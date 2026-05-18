"""CRUD for Manual Links Page 4 category placements."""

import uuid

from fastapi import APIRouter, HTTPException

from .db import get_conn, increment_gen
from .models import (
    ManualLinkCategoryCreate,
    ManualLinkCategoryItemCreate,
    ManualLinkCategoryItemOut,
    ManualLinkCategoryItemUpdate,
    ManualLinkCategoryOut,
    ManualLinkCategoryPayload,
    ManualLinkCategoryUpdate,
    ManualLinkOut,
)
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/manual-link-categories", tags=["manual-link-categories"])

MAX_DEPTH = 12


def _repair_orphan_category_parents(conn) -> int:
    rows = conn.execute(
        """
        SELECT child.category_id
        FROM manual_link_categories child
        LEFT JOIN manual_link_categories parent
          ON parent.category_id = child.parent_category_id
        WHERE child.parent_category_id IS NOT NULL
          AND parent.category_id IS NULL
        """
    ).fetchall()
    page_rows = conn.execute(
        """
        SELECT category_id
        FROM manual_link_categories
        WHERE COALESCE(is_page, 0) = 1
          AND parent_category_id IS NOT NULL
        """
    ).fetchall()
    ids = [row["category_id"] for row in rows]
    ids.extend(row["category_id"] for row in page_rows if row["category_id"] not in ids)
    if not ids:
        return 0
    gen = increment_gen(conn, "system")
    for category_id in ids:
        conn.execute(
            """
            UPDATE manual_link_categories
            SET parent_category_id=NULL, updated_at=datetime('now')
            WHERE category_id=?
            """,
            (category_id,),
        )
        raw = conn.execute("SELECT * FROM manual_link_categories WHERE category_id=?", (category_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_link_categories", category_id, dict(raw), gen)
    return len(ids)


def _repair_orphan_mapping_parents(conn) -> int:
    rows = conn.execute(
        """
        SELECT child.mapping_id
        FROM manual_link_category_items child
        LEFT JOIN manual_link_category_items parent
          ON parent.mapping_id = child.parent_mapping_id
         AND parent.category_id = child.category_id
        WHERE child.parent_mapping_id IS NOT NULL
          AND parent.mapping_id IS NULL
        """
    ).fetchall()
    if not rows:
        return 0
    ids = [row["mapping_id"] for row in rows]
    gen = increment_gen(conn, "system")
    for mapping_id in ids:
        conn.execute(
            """
            UPDATE manual_link_category_items
            SET parent_mapping_id=NULL, updated_at=datetime('now')
            WHERE mapping_id=?
            """,
            (mapping_id,),
        )
        raw = conn.execute("SELECT * FROM manual_link_category_items WHERE mapping_id=?", (mapping_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_link_category_items", mapping_id, dict(raw), gen)
    return len(ids)


def _repair_promoted_placeholder_categories(conn) -> int:
    promoted_rows = conn.execute(
        """
        SELECT notes
        FROM manual_link_categories
        WHERE notes LIKE 'Promoted from Manual Links placement %'
        """
    ).fetchall()
    promoted_mapping_ids: set[str] = set()
    prefix = "Promoted from Manual Links placement "
    for row in promoted_rows:
        notes = (row["notes"] or "").strip()
        if notes.startswith(prefix):
            mapping_id = notes[len(prefix) :].strip().split()[0]
            if mapping_id:
                promoted_mapping_ids.add(mapping_id)
    if not promoted_mapping_ids:
        return 0

    deleted = 0
    gen = None
    for mapping_id in sorted(promoted_mapping_ids):
        row = conn.execute(
            """
            SELECT
                m.mapping_id, m.category_id, m.link_id,
                c.notes AS category_notes,
                COALESCE(c.is_page, 0) AS is_page,
                COALESCE(c.show_panel, 0) AS show_panel,
                l.vlan_ip, l.vlan_uri, l.tailnet_ip, l.tailnet_uri
            FROM manual_link_category_items m
            JOIN manual_link_categories c ON c.category_id = m.category_id
            LEFT JOIN manual_links l ON l.link_id = m.link_id
            WHERE m.mapping_id=?
            """,
            (mapping_id,),
        ).fetchone()
        if not row:
            continue
        has_route = any(row[field] for field in ("vlan_ip", "vlan_uri", "tailnet_ip", "tailnet_uri"))
        if has_route or row["is_page"] or row["show_panel"]:
            continue
        if not (row["category_notes"] or "").startswith("Seeded from manual_links.group_name"):
            continue
        category_id = row["category_id"]
        sibling_count = conn.execute(
            "SELECT COUNT(*) AS n FROM manual_link_category_items WHERE category_id=?",
            (category_id,),
        ).fetchone()["n"]
        child_count = conn.execute(
            "SELECT COUNT(*) AS n FROM manual_link_categories WHERE parent_category_id=?",
            (category_id,),
        ).fetchone()["n"]
        if sibling_count != 1 or child_count != 0:
            continue
        if gen is None:
            gen = increment_gen(conn, "system")
        conn.execute("DELETE FROM manual_link_category_items WHERE mapping_id=?", (mapping_id,))
        conn.execute("DELETE FROM manual_link_categories WHERE category_id=?", (category_id,))
        enqueue_for_all_peers(conn, "DELETE", "manual_link_category_items", mapping_id, {}, gen)
        enqueue_for_all_peers(conn, "DELETE", "manual_link_categories", category_id, {}, gen)
        deleted += 1
    return deleted


def _category_out(row) -> ManualLinkCategoryOut:
    return ManualLinkCategoryOut(**dict(row))


def _link_out(row) -> ManualLinkOut:
    return ManualLinkOut(
        link_id=row["linked_link_id"],
        vlan_ip=row["link_vlan_ip"],
        vlan_uri=row["link_vlan_uri"],
        tailnet_ip=row["link_tailnet_ip"],
        tailnet_uri=row["link_tailnet_uri"],
        label=row["link_label"],
        icon=row["link_icon"],
        group_name=row["link_group_name"],
        parent_id=row["link_parent_id"],
        sort_order=row["link_sort_order"] or 0,
        pve_host=row["link_pve_host"],
        is_internet=row["link_is_internet"] or 0,
        vm_id=row["link_vm_id"],
        vm_name=row["link_vm_name"],
        lxc_id=row["link_lxc_id"],
        lxc_name=row["link_lxc_name"],
        location=row["link_location"],
        notes=row["link_notes"],
        created_at=row["link_created_at"],
        updated_at=row["link_updated_at"],
    )


def _item_out(row) -> ManualLinkCategoryItemOut:
    data = {k: row[k] for k in ManualLinkCategoryItemOut.model_fields if k in row.keys() and k != "link"}
    link = _link_out(row) if row["link_created_at"] is not None else None
    return ManualLinkCategoryItemOut(**data, link=link)


def _category_depth(conn, category_id: str | None) -> int:
    depth = 0
    seen: set[str] = set()
    current = category_id
    while current:
        if current in seen:
            raise HTTPException(400, "category hierarchy contains a cycle")
        seen.add(current)
        row = conn.execute(
            "SELECT parent_category_id FROM manual_link_categories WHERE category_id=?",
            (current,),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"category {current!r} not found")
        depth += 1
        current = row["parent_category_id"]
        if depth > MAX_DEPTH:
            raise HTTPException(400, f"category hierarchy is deeper than {MAX_DEPTH}")
    return depth


def _assert_category_depth(conn, parent_category_id: str | None) -> None:
    if _category_depth(conn, parent_category_id) >= MAX_DEPTH:
        raise HTTPException(400, f"child categories are limited to depth {MAX_DEPTH}")


def _assert_link_exists(conn, link_id: str) -> None:
    row = conn.execute("SELECT link_id FROM manual_links WHERE link_id=?", (link_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"manual link {link_id!r} not found")


def _assert_mapping_parent(
    conn,
    *,
    category_id: str,
    parent_mapping_id: str | None,
    mapping_id: str | None = None,
) -> None:
    if not parent_mapping_id:
        return
    if parent_mapping_id == mapping_id:
        raise HTTPException(400, "mapping cannot be its own parent")
    depth = 0
    seen: set[str] = set()
    current = parent_mapping_id
    while current:
        if current in seen or current == mapping_id:
            raise HTTPException(400, "mapping hierarchy contains a cycle")
        seen.add(current)
        row = conn.execute(
            "SELECT category_id, parent_mapping_id FROM manual_link_category_items WHERE mapping_id=?",
            (current,),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"parent mapping {current!r} not found")
        if row["category_id"] != category_id:
            raise HTTPException(400, "parent mapping must be in the same category")
        depth += 1
        if depth >= MAX_DEPTH:
            raise HTTPException(400, f"mapping hierarchy is limited to depth {MAX_DEPTH}")
        current = row["parent_mapping_id"]


def _fetch_item(conn, mapping_id: str):
    return conn.execute(
        """
        SELECT
            m.mapping_id, m.category_id, m.link_id, m.parent_mapping_id, m.sort_order,
            m.label_override, m.notes, m.created_at, m.updated_at,
            l.link_id AS linked_link_id,
            l.vlan_ip AS link_vlan_ip, l.vlan_uri AS link_vlan_uri,
            l.tailnet_ip AS link_tailnet_ip, l.tailnet_uri AS link_tailnet_uri,
            l.label AS link_label, l.icon AS link_icon, l.group_name AS link_group_name,
            l.parent_id AS link_parent_id, l.sort_order AS link_sort_order,
            l.pve_host AS link_pve_host, l.is_internet AS link_is_internet,
            l.vm_id AS link_vm_id, l.vm_name AS link_vm_name,
            l.lxc_id AS link_lxc_id, l.lxc_name AS link_lxc_name,
            l.location AS link_location, l.notes AS link_notes, l.created_at AS link_created_at,
            l.updated_at AS link_updated_at
        FROM manual_link_category_items m
        LEFT JOIN manual_links l ON l.link_id = m.link_id
        WHERE m.mapping_id=?
        """,
        (mapping_id,),
    ).fetchone()


@router.get("", response_model=ManualLinkCategoryPayload)
async def list_manual_link_categories() -> ManualLinkCategoryPayload:
    with get_conn() as conn:
        _repair_orphan_category_parents(conn)
        _repair_orphan_mapping_parents(conn)
        _repair_promoted_placeholder_categories(conn)
        categories = conn.execute(
            "SELECT * FROM manual_link_categories ORDER BY parent_category_id, sort_order, label"
        ).fetchall()
        items = conn.execute(
            """
            SELECT
                m.mapping_id, m.category_id, m.link_id, m.parent_mapping_id, m.sort_order,
                m.label_override, m.notes, m.created_at, m.updated_at,
                l.link_id AS linked_link_id,
                l.vlan_ip AS link_vlan_ip, l.vlan_uri AS link_vlan_uri,
                l.tailnet_ip AS link_tailnet_ip, l.tailnet_uri AS link_tailnet_uri,
                l.label AS link_label, l.icon AS link_icon, l.group_name AS link_group_name,
                l.parent_id AS link_parent_id, l.sort_order AS link_sort_order,
                l.pve_host AS link_pve_host, l.is_internet AS link_is_internet,
                l.vm_id AS link_vm_id, l.vm_name AS link_vm_name,
                l.lxc_id AS link_lxc_id, l.lxc_name AS link_lxc_name,
                l.location AS link_location, l.notes AS link_notes, l.created_at AS link_created_at,
                l.updated_at AS link_updated_at
            FROM manual_link_category_items m
            LEFT JOIN manual_links l ON l.link_id = m.link_id
            ORDER BY m.category_id, m.sort_order
            """
        ).fetchall()
    return ManualLinkCategoryPayload(
        categories=[_category_out(row) for row in categories],
        items=[_item_out(row) for row in items],
    )


@router.post("", response_model=ManualLinkCategoryOut, status_code=201)
async def create_manual_link_category(body: ManualLinkCategoryCreate) -> ManualLinkCategoryOut:
    category_id = str(uuid.uuid4())
    with get_conn() as conn:
        _assert_category_depth(conn, body.parent_category_id)
        conn.execute(
            """
            INSERT INTO manual_link_categories
                (category_id, label, icon, parent_category_id, sort_order,
                 is_page, page_label, page_sort_order, show_panel, panel_color,
                 panel_background, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                category_id,
                body.label,
                body.icon,
                body.parent_category_id,
                body.sort_order if body.sort_order is not None else 0,
                int(bool(body.is_page)),
                body.page_label,
                body.page_sort_order if body.page_sort_order is not None else 0,
                int(bool(body.show_panel)),
                body.panel_color,
                body.panel_background,
                body.notes,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM manual_link_categories WHERE category_id=?", (category_id,)).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "manual_link_categories", category_id, dict(row), gen)
    return _category_out(row)


@router.put("/{category_id}", response_model=ManualLinkCategoryOut)
async def update_manual_link_category(category_id: str, body: ManualLinkCategoryUpdate) -> ManualLinkCategoryOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT category_id FROM manual_link_categories WHERE category_id=?",
            (category_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"category {category_id!r} not found")
        if body.parent_category_id == category_id:
            raise HTTPException(400, "category cannot be its own parent")
        fields_set = body.model_fields_set
        if "parent_category_id" in fields_set and body.parent_category_id is not None:
            _assert_category_depth(conn, body.parent_category_id)
        updates = []
        params = []
        for field in (
            "label",
            "icon",
            "parent_category_id",
            "sort_order",
            "is_page",
            "page_label",
            "page_sort_order",
            "show_panel",
            "panel_color",
            "panel_background",
            "notes",
        ):
            if field in fields_set:
                updates.append(f"{field}=?")
                value = getattr(body, field)
                if field in ("is_page", "show_panel"):
                    value = int(bool(value))
                elif field in ("sort_order", "page_sort_order") and value is None:
                    value = 0
                params.append(value)
        if updates:
            updates.append("updated_at=datetime('now')")
            params.append(category_id)
            conn.execute(
                f"UPDATE manual_link_categories SET {', '.join(updates)} WHERE category_id=?",
                params,
            )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM manual_link_categories WHERE category_id=?", (category_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_link_categories", category_id, dict(row), gen)
    return _category_out(row)


@router.delete("/{category_id}", status_code=204)
async def delete_manual_link_category(category_id: str, force: bool = False) -> None:
    with get_conn() as conn:
        category_rows = conn.execute(
            """
            WITH RECURSIVE category_tree(category_id, depth) AS (
                SELECT category_id, 0
                FROM manual_link_categories
                WHERE category_id=?
                UNION ALL
                SELECT child.category_id, parent.depth + 1
                FROM manual_link_categories child
                JOIN category_tree parent ON child.parent_category_id = parent.category_id
            )
            SELECT category_id, depth FROM category_tree
            ORDER BY depth DESC
            """,
            (category_id,),
        ).fetchall()
        if not category_rows:
            raise HTTPException(404, f"category {category_id!r} not found")

        category_ids = [row["category_id"] for row in category_rows]
        placeholders = ", ".join("?" for _ in category_ids)
        item_rows = conn.execute(
            f"""
            SELECT mapping_id
            FROM manual_link_category_items
            WHERE category_id IN ({placeholders})
            """,
            category_ids,
        ).fetchall()
        if (len(category_rows) > 1 or item_rows) and not force:
            raise HTTPException(400, "category is not empty; pass force=true to delete its child categories and mappings")

        item_ids = [row["mapping_id"] for row in item_rows]
        if item_ids:
            conn.executemany(
                "DELETE FROM manual_link_category_items WHERE mapping_id=?",
                [(item_id,) for item_id in item_ids],
            )
        conn.executemany(
            "DELETE FROM manual_link_categories WHERE category_id=?",
            [(cat_id,) for cat_id in category_ids],
        )
        gen = increment_gen(conn, "human")
        for item_id in item_ids:
            enqueue_for_all_peers(conn, "DELETE", "manual_link_category_items", item_id, {}, gen)
        for cat_id in category_ids:
            enqueue_for_all_peers(conn, "DELETE", "manual_link_categories", cat_id, {}, gen)


@router.post("/items", response_model=ManualLinkCategoryItemOut, status_code=201)
async def create_manual_link_category_item(body: ManualLinkCategoryItemCreate) -> ManualLinkCategoryItemOut:
    mapping_id = str(uuid.uuid4())
    with get_conn() as conn:
        _category_depth(conn, body.category_id)
        _assert_link_exists(conn, body.link_id)
        _assert_mapping_parent(conn, category_id=body.category_id, parent_mapping_id=body.parent_mapping_id)
        conn.execute(
            """
            INSERT INTO manual_link_category_items
                (mapping_id, category_id, link_id, parent_mapping_id, sort_order, label_override, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                mapping_id,
                body.category_id,
                body.link_id,
                body.parent_mapping_id,
                body.sort_order if body.sort_order is not None else 0,
                body.label_override,
                body.notes,
            ),
        )
        gen = increment_gen(conn, "human")
        raw = conn.execute("SELECT * FROM manual_link_category_items WHERE mapping_id=?", (mapping_id,)).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "manual_link_category_items", mapping_id, dict(raw), gen)
        row = _fetch_item(conn, mapping_id)
    return _item_out(row)


@router.put("/items/{mapping_id}", response_model=ManualLinkCategoryItemOut)
async def update_manual_link_category_item(mapping_id: str, body: ManualLinkCategoryItemUpdate) -> ManualLinkCategoryItemOut:
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT mapping_id, category_id, parent_mapping_id FROM manual_link_category_items WHERE mapping_id=?",
            (mapping_id,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"mapping {mapping_id!r} not found")
        fields_set = body.model_fields_set
        next_category_id = body.category_id if body.category_id is not None else existing["category_id"]
        if "category_id" in fields_set and body.category_id is not None:
            _category_depth(conn, body.category_id)
        if body.link_id is not None:
            _assert_link_exists(conn, body.link_id)
        if "parent_mapping_id" in fields_set:
            next_parent_mapping_id = body.parent_mapping_id
        elif "category_id" in fields_set:
            next_parent_mapping_id = None
        else:
            next_parent_mapping_id = existing["parent_mapping_id"]
        _assert_mapping_parent(
            conn,
            category_id=next_category_id,
            parent_mapping_id=next_parent_mapping_id,
            mapping_id=mapping_id,
        )
        updates = []
        params = []
        for field in ("category_id", "link_id", "parent_mapping_id", "sort_order", "label_override", "notes"):
            if field in fields_set:
                updates.append(f"{field}=?")
                params.append(getattr(body, field))
        if "category_id" in fields_set and "parent_mapping_id" not in fields_set:
            updates.append("parent_mapping_id=?")
            params.append(None)
        if updates:
            updates.append("updated_at=datetime('now')")
            params.append(mapping_id)
            conn.execute(
                f"UPDATE manual_link_category_items SET {', '.join(updates)} WHERE mapping_id=?",
                params,
            )
        gen = increment_gen(conn, "human")
        raw = conn.execute("SELECT * FROM manual_link_category_items WHERE mapping_id=?", (mapping_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "manual_link_category_items", mapping_id, dict(raw), gen)
        row = _fetch_item(conn, mapping_id)
    return _item_out(row)


@router.delete("/items/{mapping_id}", status_code=204)
async def delete_manual_link_category_item(mapping_id: str) -> None:
    with get_conn() as conn:
        rows = conn.execute(
            """
            WITH RECURSIVE subtree(mapping_id) AS (
                SELECT mapping_id FROM manual_link_category_items WHERE mapping_id=?
                UNION ALL
                SELECT child.mapping_id
                FROM manual_link_category_items child
                JOIN subtree parent ON child.parent_mapping_id = parent.mapping_id
            )
            SELECT mapping_id FROM subtree
            """,
            (mapping_id,),
        ).fetchall()
        if not rows:
            raise HTTPException(404, f"mapping {mapping_id!r} not found")
        ids = [row["mapping_id"] for row in rows]
        conn.executemany(
            "DELETE FROM manual_link_category_items WHERE mapping_id=?",
            [(item_id,) for item_id in ids],
        )
        gen = increment_gen(conn, "human")
        for item_id in ids:
            enqueue_for_all_peers(conn, "DELETE", "manual_link_category_items", item_id, {}, gen)
