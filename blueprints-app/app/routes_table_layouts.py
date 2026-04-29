"""Routes for frontend table layout catalog and FFFFFFFF-keyed layout buckets."""

from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Query

from .db import get_conn, increment_gen
from .models import (
    TableLayoutCatalogCreate,
    TableLayoutCatalogOut,
    TableLayoutCatalogUpdate,
    TableLayoutOut,
    TableLayoutResolveRequest,
    TableLayoutUpsert,
)
from .sync.queue import enqueue_for_all_peers
from .table_layouts import (
    TableLayoutError,
    build_fallback_layout,
    build_layout_key,
    choose_sibling_row,
    encode_bucket_code,
    normalize_hex_byte,
    normalize_layout_data,
    parse_json_text,
    seed_from_sibling,
    split_layout_key,
    validate_bucket_code,
    validate_reserved_code,
)

router = APIRouter(prefix="/table-layouts", tags=["table-layouts"])


def _catalog_row_to_out(row) -> TableLayoutCatalogOut:
    return TableLayoutCatalogOut(
        table_code=row["table_code"],
        table_name=row["table_name"],
        table_meta=parse_json_text(row["table_meta"], {}),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _layout_row_to_out(row) -> TableLayoutOut:
    return TableLayoutOut(
        layout_key=row["layout_key"],
        reserved_code=row["reserved_code"],
        user_code=row["user_code"],
        table_code=row["table_code"],
        bucket_code=row["bucket_code"],
        layout_data=parse_json_text(row["layout_data"], {}),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _http_400(exc: TableLayoutError) -> HTTPException:
    return HTTPException(400, str(exc))


def _resolve_table_code(conn, table_code: str | None, table_name: str | None) -> str:
    if table_code:
        code = normalize_hex_byte(table_code, "table_code")
        row = conn.execute(
            "SELECT table_code FROM table_layout_catalog WHERE table_code=?",
            (code,),
        ).fetchone()
        if not row:
            raise HTTPException(404, f"unknown table_code '{code}'")
        return code

    if not table_name:
        raise HTTPException(400, "table_name or table_code is required")

    row = conn.execute(
        "SELECT table_code FROM table_layout_catalog WHERE table_name=?",
        (table_name,),
    ).fetchone()
    if not row:
        raise HTTPException(404, f"unknown table_name '{table_name}'")
    return row["table_code"]


def _serialize_json(value: dict) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


@router.get("/catalog", response_model=list[TableLayoutCatalogOut])
async def list_table_layout_catalog() -> list[TableLayoutCatalogOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM table_layout_catalog ORDER BY table_code"
        ).fetchall()
    return [_catalog_row_to_out(row) for row in rows]


@router.get("/catalog/by-name/{table_name}", response_model=TableLayoutCatalogOut)
async def get_table_layout_catalog_by_name(table_name: str) -> TableLayoutCatalogOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM table_layout_catalog WHERE table_name=?",
            (table_name,),
        ).fetchone()
    if not row:
        raise HTTPException(404, f"table layout catalog '{table_name}' not found")
    return _catalog_row_to_out(row)


@router.get("/catalog/{table_code}", response_model=TableLayoutCatalogOut)
async def get_table_layout_catalog(table_code: str) -> TableLayoutCatalogOut:
    try:
        code = normalize_hex_byte(table_code, "table_code")
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM table_layout_catalog WHERE table_code=?",
            (code,),
        ).fetchone()
    if not row:
        raise HTTPException(404, f"table layout catalog '{code}' not found")
    return _catalog_row_to_out(row)


@router.post("/catalog", response_model=TableLayoutCatalogOut, status_code=201)
async def create_table_layout_catalog_entry(body: TableLayoutCatalogCreate) -> TableLayoutCatalogOut:
    try:
        table_code = normalize_hex_byte(body.table_code, "table_code")
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT table_code FROM table_layout_catalog WHERE table_code=? OR table_name=?",
            (table_code, body.table_name),
        ).fetchone()
        if existing:
            raise HTTPException(409, "table layout catalog entry already exists")

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO table_layout_catalog (table_code, table_name, table_meta)
            VALUES (?, ?, ?)
            """,
            (table_code, body.table_name, _serialize_json(body.table_meta)),
        )
        row = conn.execute(
            "SELECT * FROM table_layout_catalog WHERE table_code=?",
            (table_code,),
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "table_layout_catalog", table_code, dict(row), gen)
    return _catalog_row_to_out(row)


@router.put("/catalog/{table_code}", response_model=TableLayoutCatalogOut)
async def update_table_layout_catalog_entry(table_code: str, body: TableLayoutCatalogUpdate) -> TableLayoutCatalogOut:
    try:
        code = normalize_hex_byte(table_code, "table_code")
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT * FROM table_layout_catalog WHERE table_code=?",
            (code,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"table layout catalog '{code}' not found")

        next_name = body.table_name if body.table_name is not None else existing["table_name"]
        next_meta = body.table_meta if body.table_meta is not None else parse_json_text(existing["table_meta"], {})
        gen = increment_gen(conn, "human")
        conn.execute(
            """
            UPDATE table_layout_catalog
            SET table_name=?, table_meta=?, updated_at=datetime('now')
            WHERE table_code=?
            """,
            (next_name, _serialize_json(next_meta), code),
        )
        row = conn.execute(
            "SELECT * FROM table_layout_catalog WHERE table_code=?",
            (code,),
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "table_layout_catalog", code, dict(row), gen)
    return _catalog_row_to_out(row)


@router.delete("/catalog/{table_code}", status_code=204, response_model=None)
async def delete_table_layout_catalog_entry(table_code: str) -> None:
    try:
        code = normalize_hex_byte(table_code, "table_code")
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT table_code FROM table_layout_catalog WHERE table_code=?",
            (code,),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"table layout catalog '{code}' not found")
        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM table_layout_catalog WHERE table_code=?", (code,))
        enqueue_for_all_peers(conn, "DELETE", "table_layout_catalog", code, None, gen)


@router.get("", response_model=list[TableLayoutOut])
async def list_table_layouts(
    table_code: str | None = Query(default=None),
    user_code: str | None = Query(default=None),
) -> list[TableLayoutOut]:
    clauses = []
    params: list[str] = []
    if table_code:
        try:
            clauses.append("table_code=?")
            params.append(normalize_hex_byte(table_code, "table_code"))
        except TableLayoutError as exc:
            raise _http_400(exc) from exc
    if user_code:
        try:
            clauses.append("user_code=?")
            params.append(normalize_hex_byte(user_code, "user_code"))
        except TableLayoutError as exc:
            raise _http_400(exc) from exc

    sql = "SELECT * FROM table_layouts"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY table_code, user_code, bucket_code"

    with get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_layout_row_to_out(row) for row in rows]


@router.get("/{layout_key}", response_model=TableLayoutOut)
async def get_table_layout(layout_key: str) -> TableLayoutOut:
    try:
        parts = split_layout_key(layout_key)
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM table_layouts WHERE layout_key=?",
            (parts["layout_key"],),
        ).fetchone()
    if not row:
        raise HTTPException(404, f"table layout '{parts['layout_key']}' not found")
    return _layout_row_to_out(row)


@router.post("/resolve", response_model=TableLayoutOut)
async def resolve_table_layout(body: TableLayoutResolveRequest) -> TableLayoutOut:
    try:
        reserved_code = validate_reserved_code(body.reserved_code)
        user_code = normalize_hex_byte(body.user_code, "user_code")
        bucket_code = validate_bucket_code(body.bucket_code) if body.bucket_code else encode_bucket_code(
            body.bucket_bits.model_dump() if body.bucket_bits else {}
        )
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        table_code = _resolve_table_code(conn, body.table_code, body.table_name)
        layout_key = build_layout_key(reserved_code, user_code, table_code, bucket_code)
        existing = conn.execute(
            "SELECT * FROM table_layouts WHERE layout_key=?",
            (layout_key,),
        ).fetchone()
        if existing:
            return _layout_row_to_out(existing)

        sibling = choose_sibling_row(conn, reserved_code, user_code, table_code, bucket_code)
        requested_columns = [column.model_dump() for column in body.columns]
        if sibling:
            sibling_layout = parse_json_text(sibling["layout_data"], {})
            layout_data = seed_from_sibling(sibling_layout, requested_columns, bucket_code)
        else:
            layout_data = build_fallback_layout(requested_columns, bucket_code)

        gen = increment_gen(conn, "human")
        conn.execute(
            """
            INSERT INTO table_layouts (
                layout_key, reserved_code, user_code, table_code, bucket_code, layout_data
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                layout_key,
                reserved_code,
                user_code,
                table_code,
                bucket_code,
                _serialize_json(layout_data),
            ),
        )
        row = conn.execute(
            "SELECT * FROM table_layouts WHERE layout_key=?",
            (layout_key,),
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "table_layouts", layout_key, dict(row), gen)
    return _layout_row_to_out(row)


@router.put("/{layout_key}", response_model=TableLayoutOut)
async def upsert_table_layout(layout_key: str, body: TableLayoutUpsert) -> TableLayoutOut:
    try:
        parts = split_layout_key(layout_key)
        layout_data = normalize_layout_data(body.layout_data)
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT layout_key FROM table_layouts WHERE layout_key=?",
            (parts["layout_key"],),
        ).fetchone()
        gen = increment_gen(conn, "human")
        if existing:
            conn.execute(
                """
                UPDATE table_layouts
                SET layout_data=?, updated_at=datetime('now')
                WHERE layout_key=?
                """,
                (_serialize_json(layout_data), parts["layout_key"]),
            )
            action = "UPDATE"
        else:
            conn.execute(
                """
                INSERT INTO table_layouts (
                    layout_key, reserved_code, user_code, table_code, bucket_code, layout_data
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    parts["layout_key"],
                    parts["reserved_code"],
                    parts["user_code"],
                    parts["table_code"],
                    parts["bucket_code"],
                    _serialize_json(layout_data),
                ),
            )
            action = "INSERT"
        row = conn.execute(
            "SELECT * FROM table_layouts WHERE layout_key=?",
            (parts["layout_key"],),
        ).fetchone()
        enqueue_for_all_peers(conn, action, "table_layouts", parts["layout_key"], dict(row), gen)
    return _layout_row_to_out(row)


@router.delete("/{layout_key}", status_code=204, response_model=None)
async def delete_table_layout(layout_key: str) -> None:
    try:
        parts = split_layout_key(layout_key)
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        existing = conn.execute(
            "SELECT layout_key FROM table_layouts WHERE layout_key=?",
            (parts["layout_key"],),
        ).fetchone()
        if not existing:
            raise HTTPException(404, f"table layout '{parts['layout_key']}' not found")
        gen = increment_gen(conn, "human")
        conn.execute("DELETE FROM table_layouts WHERE layout_key=?", (parts["layout_key"],))
        enqueue_for_all_peers(conn, "DELETE", "table_layouts", parts["layout_key"], None, gen)
