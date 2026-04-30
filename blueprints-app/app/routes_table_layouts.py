"""Routes for frontend table layout catalog and FFFFFFFF-keyed layout buckets."""

from __future__ import annotations

import json
import os
import re

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from .ai_client import complete
from .db import get_conn, increment_gen
from .models import (
    TableLayoutAutoRequest,
    TableLayoutCatalogCreate,
    TableLayoutCatalogOut,
    TableLayoutCatalogUpdate,
    TableLayoutOut,
    TableLayoutResolveRequest,
    TableLayoutUpsert,
)
from .sync.queue import enqueue_for_all_peers
from .table_auto_layouts import build_auto_layout
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


_HYPHENATION_EXAMPLES = [
    {"header": "Pending", "header_label": "Pend-ing", "changed": True},
    {"header": "Hostnames", "header_label": "Host-names", "changed": True},
    {"header": "Filename", "header_label": "File-name", "changed": True},
    {"header": "Status", "header_label": "Status", "changed": False},
    {"header": "Commit", "header_label": "Commit", "changed": False},
    {"header": "Actions", "header_label": "Actions", "changed": False},
]
_HYPHENATION_MODEL_ENV = "TABLE_LAYOUT_HYPHENATION_LLM_MODEL"
_HYPHENATION_MODEL_FALLBACK_ENV = "DOC_SPEECH_LLM_MODEL"


class HeaderHyphenationExample(BaseModel):
    header: str
    header_label: str
    changed: bool = False


class HeaderHyphenationRequest(BaseModel):
    header: str = Field(..., min_length=1, max_length=80)
    table_name: str | None = Field(default=None, max_length=120)
    column_key: str | None = Field(default=None, max_length=120)
    examples: list[HeaderHyphenationExample] | None = None


class HeaderHyphenationResponse(BaseModel):
    header: str
    header_label: str | None = None
    changed: bool = False
    confidence: float = 0.0
    reason: str = ""
    used_llm: bool = False
    error: str | None = None


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


def _normalize_header_letters(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _valid_hyphenated_header_label(header: str, header_label: str | None) -> bool:
    if not header_label:
        return False
    label = str(header_label).strip()
    if not label or len(label) > 96:
        return False
    if "<" in label or ">" in label:
        return False
    if "\n" in label or "\r" in label:
        return False
    if _normalize_header_letters(header) != _normalize_header_letters(label):
        return False
    if label.count("-") > 1:
        return False
    return bool(re.match(r"^[A-Za-z0-9][A-Za-z0-9 -]*[A-Za-z0-9]$", label))


def _coerce_llm_json(raw: str) -> dict:
    text = str(raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    decoder = json.JSONDecoder()
    candidates: list[dict] = []
    for match in re.finditer(r"\{", text):
        try:
            data, _ = decoder.raw_decode(text[match.start():])
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            candidates.append(data)
            if "header" in data and "header_label" in data:
                return data
    data = candidates[0] if candidates else json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("LLM response was not a JSON object")
    return data


def _hyphenation_model_name() -> str | None:
    configured = (
        os.getenv(_HYPHENATION_MODEL_ENV)
        or os.getenv(_HYPHENATION_MODEL_FALLBACK_ENV)
        or ""
    ).strip()
    return configured or None


def _header_hyphenation_messages(body: HeaderHyphenationRequest) -> list[dict[str, str]]:
    examples = [example.model_dump() for example in (body.examples or [])] or _HYPHENATION_EXAMPLES
    payload = {
        "task": "suggest_visual_table_header_hyphenation",
        "header": body.header,
        "table_name": body.table_name,
        "column_key": body.column_key,
        "examples": examples,
        "response_shape": {
            "header": body.header,
            "header_label": "string or null",
            "changed": "boolean",
            "confidence": "number from 0 to 1",
            "reason": "short string",
        },
        "rules": [
            "Return JSON only, with no markdown or prose.",
            "This is a visual label only; do not rename the data column.",
            "Preserve the header letters and casing except for inserting at most one hyphen.",
            "Do not use <br>, soft hyphen, slash, underscore, or extra words.",
            "Prefer natural word joins, common suffix boundaries, and syllable boundaries.",
            "If there is no tasteful split, return the original header_label and changed=false.",
        ],
    }
    return [
        {
            "role": "system",
            "content": (
                "You are a compact UI table-header hyphenation helper. "
                "You return strict JSON only. Never include markdown."
            ),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=True, sort_keys=True)},
    ]


def _upsert_layout_row(conn, layout_key: str, layout_data: dict, *, source: str = "human"):
    parts = split_layout_key(layout_key)
    existing = conn.execute(
        "SELECT layout_key FROM table_layouts WHERE layout_key=?",
        (parts["layout_key"],),
    ).fetchone()
    gen = increment_gen(conn, source)
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
    return row


@router.get("/catalog", response_model=list[TableLayoutCatalogOut])
async def list_table_layout_catalog() -> list[TableLayoutCatalogOut]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM table_layout_catalog ORDER BY table_code").fetchall()
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
async def create_table_layout_catalog_entry(
    body: TableLayoutCatalogCreate,
) -> TableLayoutCatalogOut:
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
async def update_table_layout_catalog_entry(
    table_code: str, body: TableLayoutCatalogUpdate
) -> TableLayoutCatalogOut:
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
        next_meta = (
            body.table_meta
            if body.table_meta is not None
            else parse_json_text(existing["table_meta"], {})
        )
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


@router.post("/auto-layout")
async def auto_layout_table_bucket(body: TableLayoutAutoRequest) -> dict:
    try:
        reserved_code = validate_reserved_code(body.reserved_code)
        user_code = normalize_hex_byte(body.user_code, "user_code")
        bucket_code = (
            validate_bucket_code(body.bucket_code)
            if body.bucket_code
            else encode_bucket_code(body.bucket_bits.model_dump() if body.bucket_bits else {})
        )
        mode = str(body.mode or "preview").strip().lower()
        if mode not in {"preview", "apply", "replace_generated_only"}:
            raise TableLayoutError("mode must be preview, apply, or replace_generated_only")
    except TableLayoutError as exc:
        raise _http_400(exc) from exc

    with get_conn() as conn:
        table_code = _resolve_table_code(conn, body.table_code, body.table_name)
        catalog = conn.execute(
            "SELECT table_name FROM table_layout_catalog WHERE table_code=?",
            (table_code,),
        ).fetchone()
        table_name = body.table_name or (catalog["table_name"] if catalog else None)
        layout_key = build_layout_key(reserved_code, user_code, table_code, bucket_code)
        requested_columns = [column.model_dump() for column in body.columns]
        viewport = body.viewport.model_dump() if body.viewport else None
        try:
            layout_data, planner = build_auto_layout(
                requested_columns,
                bucket_code,
                table_name=table_name,
                viewport=viewport,
            )
            layout_data = normalize_layout_data(layout_data)
        except TableLayoutError as exc:
            raise _http_400(exc) from exc

        existing = conn.execute(
            "SELECT * FROM table_layouts WHERE layout_key=?",
            (layout_key,),
        ).fetchone()
        skipped = False
        if mode == "replace_generated_only" and existing:
            existing_layout = parse_json_text(existing["layout_data"], {})
            existing_origin = str(existing_layout.get("seed_origin") or "").lower()
            existing_algo = str(existing_layout.get("algorithm_version") or "").lower()
            generated = existing_origin in {
                "fallback",
                "sibling",
                "sibling-reapply",
                "auto-layout",
            } or existing_algo.startswith("auto-")
            if not generated:
                skipped = True

        if mode == "preview" or skipped:
            row_data = {
                "layout_key": layout_key,
                "reserved_code": reserved_code,
                "user_code": user_code,
                "table_code": table_code,
                "bucket_code": bucket_code,
                "layout_data": layout_data
                if not skipped
                else parse_json_text(existing["layout_data"], {}),
                "created_at": existing["created_at"] if existing else None,
                "updated_at": existing["updated_at"] if existing else None,
            }
        else:
            saved = _upsert_layout_row(
                conn,
                layout_key,
                layout_data,
                source="table-auto-layout",
            )
            row_data = _layout_row_to_out(saved).model_dump()

    row_data["planner"] = planner
    if skipped:
        row_data["planner"]["skipped"] = True
        row_data["planner"]["reason_codes"] = row_data["planner"].get("reason_codes", []) + [
            "manual_layout_preserved"
        ]
    return row_data


@router.post("/hyphenate-header", response_model=HeaderHyphenationResponse)
async def hyphenate_header(body: HeaderHyphenationRequest) -> HeaderHyphenationResponse:
    header = re.sub(r"\s+", " ", body.header or "").strip()
    if not header:
        return HeaderHyphenationResponse(header="", header_label=None, changed=False)

    try:
        raw = await complete(
            "browser-links",
            _header_hyphenation_messages(body),
            max_tokens=160,
            strip_think=True,
            no_think=True,
            model_name=_hyphenation_model_name(),
        )
        data = _coerce_llm_json(str(raw))
        header_label = str(data.get("header_label") or "").strip() or None
        changed = bool(data.get("changed"))
        if not _valid_hyphenated_header_label(header, header_label):
            return HeaderHyphenationResponse(
                header=header,
                header_label=None,
                changed=False,
                confidence=0.0,
                reason="LLM response did not pass header-label validation",
                used_llm=True,
            )
        try:
            confidence = max(0.0, min(1.0, float(data.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0
        return HeaderHyphenationResponse(
            header=header,
            header_label=header_label,
            changed=changed and header_label != header,
            confidence=confidence,
            reason=str(data.get("reason") or "")[:240],
            used_llm=True,
        )
    except Exception as exc:
        return HeaderHyphenationResponse(
            header=header,
            header_label=None,
            changed=False,
            confidence=0.0,
            reason="LLM hyphenation failed; caller should use fallback",
            used_llm=False,
            error=str(exc)[:240],
        )


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
        bucket_code = (
            validate_bucket_code(body.bucket_code)
            if body.bucket_code
            else encode_bucket_code(body.bucket_bits.model_dump() if body.bucket_bits else {})
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
