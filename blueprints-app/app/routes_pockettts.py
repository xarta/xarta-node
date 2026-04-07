"""PocketTTS-specific metadata routes (tags, voice meta, order, import/seed)."""

from __future__ import annotations

import re
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

from . import config as cfg
from .db import get_conn, increment_gen
from .models import (
    PocketttsImportRequest,
    PocketttsSeedDefaultsRequest,
    PocketttsTagCreate,
    PocketttsTagOrderUpdate,
    PocketttsTagOut,
    PocketttsTagUpdate,
    PocketttsVoiceMetaOut,
    PocketttsVoiceMetaPatch,
)
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/pockettts", tags=["pockettts"])

_DEFAULT_TAGS: list[dict[str, Any]] = [
    {"tag_id": "ptag-gender", "slug": "gender", "label": "gender", "parent_tag_id": None, "color_hex": "#8a0aa8", "sort_order": 10},
    {"tag_id": "ptag-gender-unknown", "slug": "unknown", "label": "unknown", "parent_tag_id": "ptag-gender", "color_hex": "#ff1f1f", "sort_order": 11},
    {"tag_id": "ptag-gender-male", "slug": "male", "label": "male", "parent_tag_id": "ptag-gender", "color_hex": "#ff1f1f", "sort_order": 12},
    {"tag_id": "ptag-gender-female", "slug": "female", "label": "female", "parent_tag_id": "ptag-gender", "color_hex": "#ff1f1f", "sort_order": 13},
    {"tag_id": "ptag-source", "slug": "source", "label": "source", "parent_tag_id": None, "color_hex": "#8a0aa8", "sort_order": 20},
    {"tag_id": "ptag-source-builtin", "slug": "built-in", "label": "built-in", "parent_tag_id": "ptag-source", "color_hex": "#1f40ff", "sort_order": 21},
    {"tag_id": "ptag-source-community", "slug": "community", "label": "community", "parent_tag_id": "ptag-source", "color_hex": "#1f40ff", "sort_order": 22},
    {"tag_id": "ptag-source-private", "slug": "private", "label": "private", "parent_tag_id": "ptag-source", "color_hex": "#1f40ff", "sort_order": 23},
    {"tag_id": "ptag-source-uploaded", "slug": "uploaded", "label": "uploaded", "parent_tag_id": "ptag-source", "color_hex": "#d97a00", "sort_order": 24},
    {"tag_id": "ptag-quality", "slug": "quality", "label": "quality", "parent_tag_id": None, "color_hex": "#8a0aa8", "sort_order": 30},
    {"tag_id": "ptag-quality-best", "slug": "best", "label": "best", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 31},
    {"tag_id": "ptag-quality-good", "slug": "good", "label": "good", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 32},
    {"tag_id": "ptag-quality-interesting", "slug": "interesting", "label": "interesting", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 33},
    {"tag_id": "ptag-quality-whisper", "slug": "whisper", "label": "whisper", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 34},
    {"tag_id": "ptag-quality-fair", "slug": "fair", "label": "fair", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 35},
    {"tag_id": "ptag-quality-glitchy", "slug": "glitchy", "label": "glitchy", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 36},
    {"tag_id": "ptag-quality-bad", "slug": "bad", "label": "bad", "parent_tag_id": "ptag-quality", "color_hex": "#d97a00", "sort_order": 37},
    {"tag_id": "ptag-accent", "slug": "accent", "label": "accent", "parent_tag_id": None, "color_hex": "#8a0aa8", "sort_order": 40},
    {"tag_id": "ptag-accent-unclassified", "slug": "accent-unclassified", "label": "accent-unclassified", "parent_tag_id": "ptag-accent", "color_hex": "#6d7dff", "sort_order": 50},
    {"tag_id": "ptag-accent-north-american", "slug": "north-american", "label": "north-american", "parent_tag_id": "ptag-accent", "color_hex": "#6d7dff", "sort_order": 51},
    {"tag_id": "ptag-accent-european-accented", "slug": "european-accented", "label": "european-accented", "parent_tag_id": "ptag-accent", "color_hex": "#6d7dff", "sort_order": 53},
    {"tag_id": "ptag-accent-australian-nz", "slug": "australian-nz", "label": "australian-nz", "parent_tag_id": "ptag-accent", "color_hex": "#6d7dff", "sort_order": 56},
    {"tag_id": "ptag-accent-latin-accented", "slug": "latin-accented", "label": "latin-accented", "parent_tag_id": "ptag-accent", "color_hex": "#6d7dff", "sort_order": 57},
    {"tag_id": "ptag-accent-midlands", "slug": "midlands", "label": "midlands", "parent_tag_id": "ptag-accent", "color_hex": "#0a8a0a", "sort_order": 43},
    {"tag_id": "ptag-accent-northern-english", "slug": "northern-english", "label": "northern-english", "parent_tag_id": "ptag-accent", "color_hex": "#0a8a0a", "sort_order": 41},
    {"tag_id": "ptag-accent-southern-english", "slug": "southern-english", "label": "southern-english", "parent_tag_id": "ptag-accent", "color_hex": "#0a8a0a", "sort_order": 44},
    {"tag_id": "ptag-accent-rp", "slug": "rp", "label": "rp", "parent_tag_id": "ptag-accent", "color_hex": "#0a8a0a", "sort_order": 45},
]

_DEFAULT_BUILTIN_ASSIGNMENTS: dict[str, list[str]] = {
    "alba": ["ptag-source-builtin", "ptag-gender-male", "ptag-quality-good", "ptag-accent-unclassified"],
    "azelma": ["ptag-source-builtin", "ptag-gender-female", "ptag-quality-fair", "ptag-accent-unclassified"],
    "cosette": ["ptag-source-builtin", "ptag-gender-female", "ptag-quality-good"],
    "eponine": ["ptag-source-builtin", "ptag-gender-female", "ptag-quality-good"],
    "fantine": ["ptag-source-builtin", "ptag-gender-female", "ptag-accent-northern-english"],
    "javert": ["ptag-source-builtin", "ptag-gender-male", "ptag-quality-good"],
    "marius": ["ptag-source-builtin", "ptag-gender-male", "ptag-quality-best", "ptag-accent-unclassified"],
}


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "tag"


def _tag_id_from_slug(slug: str) -> str:
    return f"ptag-{slug}"


def _assignment_id(voice_id: str, tag_id: str) -> str:
    return f"{voice_id}|{tag_id}"


def _order_id(scope_key: str, tag_id: str) -> str:
    return f"{scope_key}|{tag_id}"


def _resolve_import_base_url(base_url: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        raw = "/tts/pockettts"
    if raw.startswith("/"):
        return f"{cfg.UI_URL.rstrip('/')}{raw}"
    return raw


def _row_to_tag_out(row) -> PocketttsTagOut:
    voice_count = row["voice_count"] if "voice_count" in row.keys() else 0
    return PocketttsTagOut(
        tag_id=row["tag_id"],
        slug=row["slug"],
        label=row["label"],
        color_hex=row["color_hex"],
        parent_tag_id=row["parent_tag_id"],
        sort_order=row["sort_order"],
        is_seed_default=row["is_seed_default"],
        is_active=row["is_active"],
        voice_count=voice_count,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _ensure_voice(conn, voice_id: str, display_name: str | None, source_type: str | None, is_user_uploaded: int | None) -> None:
    conn.execute(
        """
        INSERT INTO pockettts_voices
            (voice_id, display_name, source_type, is_user_uploaded, is_active)
        VALUES (?, ?, COALESCE(?, 'unknown'), COALESCE(?, 0), 1)
        ON CONFLICT(voice_id) DO UPDATE SET
            display_name = COALESCE(excluded.display_name, pockettts_voices.display_name),
            source_type = COALESCE(excluded.source_type, pockettts_voices.source_type),
            is_user_uploaded = COALESCE(excluded.is_user_uploaded, pockettts_voices.is_user_uploaded),
            is_active = 1,
            last_seen_at = datetime('now'),
            updated_at = datetime('now')
        """,
        (voice_id, display_name, source_type, is_user_uploaded),
    )


def _fetch_full_dump(conn, scope_key: str = "global-default") -> dict[str, Any]:
    tag_rows = conn.execute(
        "SELECT * FROM pockettts_tags WHERE is_active=1 ORDER BY sort_order, label"
    ).fetchall()
    tags_dict = {
        row["tag_id"]: {
            "name": row["label"],
            "slug": row["slug"],
            "color": row["color_hex"],
            "parent": row["parent_tag_id"],
            "sort_order": row["sort_order"],
            "is_seed_default": row["is_seed_default"],
            "is_active": row["is_active"],
        }
        for row in tag_rows
    }

    voice_rows = conn.execute(
        """
        SELECT v.voice_id,
               COALESCE(m.hidden, 0) AS hidden,
               COALESCE(m.note, '')  AS note
        FROM pockettts_voices v
        LEFT JOIN pockettts_voice_meta m ON m.voice_id = v.voice_id
        WHERE v.is_active=1
        ORDER BY v.voice_id
        """
    ).fetchall()
    tag_rows_by_voice = conn.execute(
        "SELECT voice_id, tag_id FROM pockettts_voice_tags ORDER BY voice_id, tag_id"
    ).fetchall()
    voice_tags: dict[str, list[str]] = {}
    for row in tag_rows_by_voice:
        voice_tags.setdefault(row["voice_id"], []).append(row["tag_id"])

    voices_dict: dict[str, dict[str, Any]] = {}
    for row in voice_rows:
        voices_dict[row["voice_id"]] = {
            "hidden": bool(row["hidden"]),
            "tags": voice_tags.get(row["voice_id"], []),
            "note": row["note"] or "",
        }

    order_rows = conn.execute(
        "SELECT tag_id FROM pockettts_tag_order WHERE scope_key=? ORDER BY order_index",
        (scope_key,),
    ).fetchall()

    return {
        "version": "1",
        "tags": tags_dict,
        "voices": voices_dict,
        "tag_order": [row["tag_id"] for row in order_rows],
        "scope_key": scope_key,
    }


def _upsert_default_tags(conn, scope_key: str, include_builtin_assignments: bool) -> dict[str, int]:
    touched_tags: set[str] = set()
    touched_orders: set[str] = set()
    touched_voices: set[str] = set()
    touched_meta: set[str] = set()
    touched_assignments: set[str] = set()

    for item in _DEFAULT_TAGS:
        conn.execute(
            """
            INSERT INTO pockettts_tags
                (tag_id, slug, label, color_hex, parent_tag_id, sort_order, is_seed_default, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1, 1)
            ON CONFLICT(tag_id) DO UPDATE SET
                slug=excluded.slug,
                label=excluded.label,
                color_hex=excluded.color_hex,
                parent_tag_id=excluded.parent_tag_id,
                sort_order=excluded.sort_order,
                is_seed_default=1,
                is_active=1,
                updated_at=datetime('now')
            """,
            (
                item["tag_id"], item["slug"], item["label"], item["color_hex"],
                item["parent_tag_id"], item["sort_order"],
            ),
        )
        touched_tags.add(item["tag_id"])

    ordered_tag_ids = [item["tag_id"] for item in sorted(_DEFAULT_TAGS, key=lambda x: x["sort_order"])]
    for idx, tag_id in enumerate(ordered_tag_ids):
        oid = _order_id(scope_key, tag_id)
        conn.execute(
            """
            INSERT INTO pockettts_tag_order (order_id, scope_key, tag_id, order_index)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                order_index=excluded.order_index,
                updated_at=datetime('now')
            """,
            (oid, scope_key, tag_id, idx),
        )
        touched_orders.add(oid)

    if include_builtin_assignments:
        for voice_id, tags in _DEFAULT_BUILTIN_ASSIGNMENTS.items():
            _ensure_voice(conn, voice_id, voice_id.title(), "builtin", 0)
            touched_voices.add(voice_id)
            conn.execute(
                """
                INSERT INTO pockettts_voice_meta (voice_id, hidden, note)
                VALUES (?, 0, '')
                ON CONFLICT(voice_id) DO UPDATE SET updated_at=datetime('now')
                """,
                (voice_id,),
            )
            touched_meta.add(voice_id)
            for tag_id in tags:
                aid = _assignment_id(voice_id, tag_id)
                conn.execute(
                    """
                    INSERT INTO pockettts_voice_tags
                        (assignment_id, voice_id, tag_id, assignment_source, confidence)
                    VALUES (?, ?, ?, 'seed', 'high')
                    ON CONFLICT(assignment_id) DO UPDATE SET
                        assignment_source='seed',
                        confidence='high',
                        updated_at=datetime('now')
                    """,
                    (aid, voice_id, tag_id),
                )
                touched_assignments.add(aid)

    return {
        "tags": len(touched_tags),
        "orders": len(touched_orders),
        "voices": len(touched_voices),
        "meta": len(touched_meta),
        "assignments": len(touched_assignments),
    }


@router.get("/tags", response_model=list[PocketttsTagOut])
async def list_pockettts_tags() -> list[PocketttsTagOut]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT t.*, COALESCE(COUNT(vt.assignment_id), 0) AS voice_count
            FROM pockettts_tags t
            LEFT JOIN pockettts_voice_tags vt ON vt.tag_id = t.tag_id
            GROUP BY t.tag_id
            ORDER BY t.sort_order, t.label
            """
        ).fetchall()
    return [_row_to_tag_out(r) for r in rows]


@router.post("/tags", response_model=PocketttsTagOut, status_code=201)
async def create_pockettts_tag(body: PocketttsTagCreate) -> PocketttsTagOut:
    slug = body.slug.strip() if body.slug else _slugify(body.label)
    tag_id = body.tag_id.strip() if body.tag_id else _tag_id_from_slug(slug)

    with get_conn() as conn:
        exists = conn.execute("SELECT tag_id FROM pockettts_tags WHERE tag_id=?", (tag_id,)).fetchone()
        if exists:
            raise HTTPException(409, f"tag '{tag_id}' already exists")

        conn.execute(
            """
            INSERT INTO pockettts_tags
                (tag_id, slug, label, color_hex, parent_tag_id, sort_order, is_seed_default, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tag_id, slug, body.label.strip(), body.color_hex.strip(), body.parent_tag_id,
                body.sort_order, int(bool(body.is_seed_default)), int(bool(body.is_active)),
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM pockettts_tags WHERE tag_id=?", (tag_id,)).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "pockettts_tags", tag_id, dict(row), gen)

        row = conn.execute(
            """
            SELECT t.*, COALESCE(COUNT(vt.assignment_id), 0) AS voice_count
            FROM pockettts_tags t
            LEFT JOIN pockettts_voice_tags vt ON vt.tag_id = t.tag_id
            WHERE t.tag_id=?
            GROUP BY t.tag_id
            """,
            (tag_id,),
        ).fetchone()
    return _row_to_tag_out(row)


@router.patch("/tags/{tag_id}", response_model=PocketttsTagOut)
async def update_pockettts_tag(tag_id: str, body: PocketttsTagUpdate) -> PocketttsTagOut:
    with get_conn() as conn:
        current = conn.execute("SELECT * FROM pockettts_tags WHERE tag_id=?", (tag_id,)).fetchone()
        if not current:
            raise HTTPException(404, "tag not found")

        conn.execute(
            """
            UPDATE pockettts_tags SET
                slug=COALESCE(?, slug),
                label=COALESCE(?, label),
                color_hex=COALESCE(?, color_hex),
                parent_tag_id=COALESCE(?, parent_tag_id),
                sort_order=COALESCE(?, sort_order),
                is_seed_default=COALESCE(?, is_seed_default),
                is_active=COALESCE(?, is_active),
                updated_at=datetime('now')
            WHERE tag_id=?
            """,
            (
                body.slug,
                body.label,
                body.color_hex,
                body.parent_tag_id,
                body.sort_order,
                body.is_seed_default,
                body.is_active,
                tag_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM pockettts_tags WHERE tag_id=?", (tag_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "pockettts_tags", tag_id, dict(row), gen)

        row = conn.execute(
            """
            SELECT t.*, COALESCE(COUNT(vt.assignment_id), 0) AS voice_count
            FROM pockettts_tags t
            LEFT JOIN pockettts_voice_tags vt ON vt.tag_id = t.tag_id
            WHERE t.tag_id=?
            GROUP BY t.tag_id
            """,
            (tag_id,),
        ).fetchone()
    return _row_to_tag_out(row)


@router.delete("/tags/{tag_id}", status_code=204, response_model=None)
async def delete_pockettts_tag(tag_id: str) -> None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM pockettts_tags WHERE tag_id=?", (tag_id,)).fetchone()
        if not row:
            raise HTTPException(404, "tag not found")

        assignment_ids = [
            r["assignment_id"]
            for r in conn.execute(
                "SELECT assignment_id FROM pockettts_voice_tags WHERE tag_id=?",
                (tag_id,),
            ).fetchall()
        ]
        order_ids = [
            r["order_id"]
            for r in conn.execute(
                "SELECT order_id FROM pockettts_tag_order WHERE tag_id=?",
                (tag_id,),
            ).fetchall()
        ]

        conn.execute("DELETE FROM pockettts_voice_tags WHERE tag_id=?", (tag_id,))
        conn.execute("DELETE FROM pockettts_tag_order WHERE tag_id=?", (tag_id,))
        conn.execute(
            "UPDATE pockettts_tags SET parent_tag_id=NULL, updated_at=datetime('now') WHERE parent_tag_id=?",
            (tag_id,),
        )
        child_rows = conn.execute(
            "SELECT * FROM pockettts_tags WHERE parent_tag_id IS NULL AND tag_id != ?",
            (tag_id,),
        ).fetchall()
        conn.execute("DELETE FROM pockettts_tags WHERE tag_id=?", (tag_id,))

        gen = increment_gen(conn, "human")
        for assignment_id in assignment_ids:
            enqueue_for_all_peers(conn, "DELETE", "pockettts_voice_tags", assignment_id, {}, gen)
        for order_id in order_ids:
            enqueue_for_all_peers(conn, "DELETE", "pockettts_tag_order", order_id, {}, gen)
        for child in child_rows:
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tags", child["tag_id"], dict(child), gen)
        enqueue_for_all_peers(conn, "DELETE", "pockettts_tags", tag_id, {}, gen)


@router.get("/tags/order")
async def get_pockettts_tag_order(scope_key: str = "global-default") -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT tag_id FROM pockettts_tag_order WHERE scope_key=? ORDER BY order_index",
            (scope_key,),
        ).fetchall()
    return {"scope_key": scope_key, "order": [r["tag_id"] for r in rows]}


@router.put("/tags/order")
async def set_pockettts_tag_order(body: PocketttsTagOrderUpdate) -> dict[str, Any]:
    with get_conn() as conn:
        valid_rows = conn.execute("SELECT tag_id FROM pockettts_tags").fetchall()
        valid = {r["tag_id"] for r in valid_rows}
        ordered = [tag_id for tag_id in body.order if tag_id in valid]

        existing_rows = conn.execute(
            "SELECT order_id, tag_id FROM pockettts_tag_order WHERE scope_key=?",
            (body.scope_key,),
        ).fetchall()
        existing_by_tag = {r["tag_id"]: r["order_id"] for r in existing_rows}

        delete_ids = [oid for tid, oid in existing_by_tag.items() if tid not in set(ordered)]
        for oid in delete_ids:
            conn.execute("DELETE FROM pockettts_tag_order WHERE order_id=?", (oid,))

        upsert_ids: list[str] = []
        for idx, tag_id in enumerate(ordered):
            oid = _order_id(body.scope_key, tag_id)
            conn.execute(
                """
                INSERT INTO pockettts_tag_order (order_id, scope_key, tag_id, order_index)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    order_index=excluded.order_index,
                    updated_at=datetime('now')
                """,
                (oid, body.scope_key, tag_id, idx),
            )
            upsert_ids.append(oid)

        gen = increment_gen(conn, "human")
        for oid in delete_ids:
            enqueue_for_all_peers(conn, "DELETE", "pockettts_tag_order", oid, {}, gen)
        for oid in upsert_ids:
            row = conn.execute("SELECT * FROM pockettts_tag_order WHERE order_id=?", (oid,)).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tag_order", oid, dict(row), gen)

    return {"scope_key": body.scope_key, "order": ordered}


@router.get("/voices/meta")
async def get_pockettts_voice_meta_dump(scope_key: str = "global-default") -> dict[str, Any]:
    with get_conn() as conn:
        return _fetch_full_dump(conn, scope_key=scope_key)


@router.get("/voices/{voice_id}/meta", response_model=PocketttsVoiceMetaOut)
async def get_pockettts_single_voice_meta(voice_id: str) -> PocketttsVoiceMetaOut:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM pockettts_voice_meta WHERE voice_id=?", (voice_id,)).fetchone()
        tags = [
            r["tag_id"]
            for r in conn.execute(
                "SELECT tag_id FROM pockettts_voice_tags WHERE voice_id=? ORDER BY tag_id",
                (voice_id,),
            ).fetchall()
        ]
    return PocketttsVoiceMetaOut(
        voice_id=voice_id,
        hidden=bool(row["hidden"]) if row else False,
        note=(row["note"] or "") if row else "",
        tags=tags,
    )


@router.patch("/voices/{voice_id}/meta", response_model=PocketttsVoiceMetaOut)
async def patch_pockettts_voice_meta(voice_id: str, body: PocketttsVoiceMetaPatch) -> PocketttsVoiceMetaOut:
    with get_conn() as conn:
        _ensure_voice(
            conn,
            voice_id,
            body.display_name,
            body.source_type,
            int(body.is_user_uploaded) if body.is_user_uploaded is not None else None,
        )

        current = conn.execute(
            "SELECT hidden, note FROM pockettts_voice_meta WHERE voice_id=?",
            (voice_id,),
        ).fetchone()

        next_hidden = int(body.hidden) if body.hidden is not None else int(current["hidden"]) if current else 0
        next_note = body.note if body.note is not None else (current["note"] if current else "")

        conn.execute(
            """
            INSERT INTO pockettts_voice_meta (voice_id, hidden, note)
            VALUES (?, ?, ?)
            ON CONFLICT(voice_id) DO UPDATE SET
                hidden=excluded.hidden,
                note=excluded.note,
                updated_at=datetime('now')
            """,
            (voice_id, next_hidden, next_note),
        )

        delete_assignment_ids: list[str] = []
        upsert_assignment_ids: list[str] = []

        if body.tags is not None:
            valid_tags = {
                r["tag_id"]
                for r in conn.execute("SELECT tag_id FROM pockettts_tags WHERE is_active=1").fetchall()
            }
            requested = [tag_id for tag_id in body.tags if tag_id in valid_tags]

            existing_rows = conn.execute(
                "SELECT assignment_id, tag_id FROM pockettts_voice_tags WHERE voice_id=?",
                (voice_id,),
            ).fetchall()
            existing = {r["tag_id"]: r["assignment_id"] for r in existing_rows}

            requested_set = set(requested)
            for tag_id, assignment_id in existing.items():
                if tag_id not in requested_set:
                    conn.execute("DELETE FROM pockettts_voice_tags WHERE assignment_id=?", (assignment_id,))
                    delete_assignment_ids.append(assignment_id)

            for tag_id in requested:
                assignment_id = _assignment_id(voice_id, tag_id)
                conn.execute(
                    """
                    INSERT INTO pockettts_voice_tags (assignment_id, voice_id, tag_id, assignment_source, confidence)
                    VALUES (?, ?, ?, 'user', NULL)
                    ON CONFLICT(assignment_id) DO UPDATE SET
                        assignment_source='user',
                        updated_at=datetime('now')
                    """,
                    (assignment_id, voice_id, tag_id),
                )
                upsert_assignment_ids.append(assignment_id)

        gen = increment_gen(conn, "human")
        voice_row = conn.execute("SELECT * FROM pockettts_voices WHERE voice_id=?", (voice_id,)).fetchone()
        meta_row = conn.execute("SELECT * FROM pockettts_voice_meta WHERE voice_id=?", (voice_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "pockettts_voices", voice_id, dict(voice_row), gen)
        enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_meta", voice_id, dict(meta_row), gen)
        for aid in delete_assignment_ids:
            enqueue_for_all_peers(conn, "DELETE", "pockettts_voice_tags", aid, {}, gen)
        for aid in upsert_assignment_ids:
            row = conn.execute("SELECT * FROM pockettts_voice_tags WHERE assignment_id=?", (aid,)).fetchone()
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_tags", aid, dict(row), gen)

        tag_rows = conn.execute(
            "SELECT tag_id FROM pockettts_voice_tags WHERE voice_id=? ORDER BY tag_id",
            (voice_id,),
        ).fetchall()

    return PocketttsVoiceMetaOut(
        voice_id=voice_id,
        hidden=bool(meta_row["hidden"]),
        note=meta_row["note"] or "",
        tags=[r["tag_id"] for r in tag_rows],
    )


@router.post("/seed-defaults")
async def pockettts_seed_defaults(body: PocketttsSeedDefaultsRequest) -> dict[str, Any]:
    with get_conn() as conn:
        summary = _upsert_default_tags(conn, body.scope_key, body.include_builtin_assignments)
        gen = increment_gen(conn, "human")

        for row in conn.execute("SELECT * FROM pockettts_tags").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tags", row["tag_id"], dict(row), gen)
        for row in conn.execute("SELECT * FROM pockettts_tag_order WHERE scope_key=?", (body.scope_key,)).fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tag_order", row["order_id"], dict(row), gen)
        if body.include_builtin_assignments:
            for row in conn.execute("SELECT * FROM pockettts_voices WHERE source_type='builtin'").fetchall():
                enqueue_for_all_peers(conn, "UPDATE", "pockettts_voices", row["voice_id"], dict(row), gen)
            for row in conn.execute("SELECT * FROM pockettts_voice_meta WHERE voice_id IN ('alba','azelma','cosette','eponine','fantine','javert','marius')").fetchall():
                enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_meta", row["voice_id"], dict(row), gen)
            for row in conn.execute("SELECT * FROM pockettts_voice_tags WHERE voice_id IN ('alba','azelma','cosette','eponine','fantine','javert','marius')").fetchall():
                enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_tags", row["assignment_id"], dict(row), gen)

    return {
        "seeded": True,
        "scope_key": body.scope_key,
        **summary,
    }


@router.post("/import-pockettts")
async def pockettts_import_from_service(body: PocketttsImportRequest) -> dict[str, Any]:
    base = _resolve_import_base_url(body.base_url).rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=30.0, verify=body.verify_tls) as client:
            voices_resp = await client.get(f"{base}/v1/voices")
            voices_resp.raise_for_status()
            voices_payload = voices_resp.json()

            meta_payload = {"tags": {}, "voices": {}}
            if body.include_voice_meta:
                meta_resp = await client.get(f"{base}/v1/voice-meta")
                if meta_resp.status_code == 200:
                    meta_payload = meta_resp.json()
    except Exception as exc:
        raise HTTPException(502, f"failed to import from PocketTTS service: {exc}") from exc

    voices = voices_payload.get("data", []) if isinstance(voices_payload, dict) else []
    legacy_tags = meta_payload.get("tags", {}) if isinstance(meta_payload, dict) else {}
    legacy_voice_meta = meta_payload.get("voices", {}) if isinstance(meta_payload, dict) else {}

    upsert_counts = {
        "tags": 0,
        "voices": 0,
        "voice_meta": 0,
        "voice_tags": 0,
        "deleted_voice_tags": 0,
    }

    with get_conn() as conn:
        if body.include_seed_defaults:
            _upsert_default_tags(conn, "global-default", include_builtin_assignments=True)

        legacy_to_new_tag_id: dict[str, str] = {}
        for legacy_tag_id, t in legacy_tags.items():
            label = (t or {}).get("name") or str(legacy_tag_id)
            slug = _slugify(label)
            new_tag_id = _tag_id_from_slug(slug)
            color_hex = (t or {}).get("color") or "#5c6ef8"

            conn.execute(
                """
                INSERT INTO pockettts_tags
                    (tag_id, slug, label, color_hex, parent_tag_id, sort_order, is_seed_default, is_active)
                VALUES (?, ?, ?, ?, NULL, 1000, 0, 1)
                ON CONFLICT(tag_id) DO UPDATE SET
                    label=excluded.label,
                    color_hex=excluded.color_hex,
                    is_active=1,
                    updated_at=datetime('now')
                """,
                (new_tag_id, slug, label, color_hex),
            )
            legacy_to_new_tag_id[legacy_tag_id] = new_tag_id
            upsert_counts["tags"] += 1

        # parent links after all tags are known
        for legacy_tag_id, t in legacy_tags.items():
            parent_legacy = (t or {}).get("parent")
            if parent_legacy and parent_legacy in legacy_to_new_tag_id:
                conn.execute(
                    "UPDATE pockettts_tags SET parent_tag_id=?, updated_at=datetime('now') WHERE tag_id=?",
                    (legacy_to_new_tag_id[parent_legacy], legacy_to_new_tag_id[legacy_tag_id]),
                )

        for v in voices:
            voice_id = v.get("id")
            if not voice_id:
                continue
            display_name = v.get("name") or voice_id
            source_type = v.get("type") or ("uploaded" if v.get("user_uploaded") else "unknown")
            is_user_uploaded = 1 if v.get("user_uploaded") else 0

            _ensure_voice(conn, voice_id, display_name, source_type, is_user_uploaded)
            upsert_counts["voices"] += 1

            vm = legacy_voice_meta.get(voice_id, {}) if isinstance(legacy_voice_meta, dict) else {}
            hidden = int(bool(vm.get("hidden", v.get("hidden", False))))
            note = vm.get("note", v.get("note", "")) or ""

            conn.execute(
                """
                INSERT INTO pockettts_voice_meta (voice_id, hidden, note)
                VALUES (?, ?, ?)
                ON CONFLICT(voice_id) DO UPDATE SET
                    hidden=excluded.hidden,
                    note=excluded.note,
                    updated_at=datetime('now')
                """,
                (voice_id, hidden, note),
            )
            upsert_counts["voice_meta"] += 1

            tag_list = vm.get("tags") if isinstance(vm, dict) and isinstance(vm.get("tags"), list) else v.get("tags", [])
            mapped_tags = [legacy_to_new_tag_id.get(tag, tag) for tag in (tag_list or [])]
            mapped_tags = [tag for tag in mapped_tags if conn.execute("SELECT 1 FROM pockettts_tags WHERE tag_id=?", (tag,)).fetchone()]

            existing = conn.execute(
                "SELECT assignment_id, tag_id FROM pockettts_voice_tags WHERE voice_id=?",
                (voice_id,),
            ).fetchall()
            existing_by_tag = {r["tag_id"]: r["assignment_id"] for r in existing}

            mapped_set = set(mapped_tags)
            for tag_id, aid in existing_by_tag.items():
                if tag_id not in mapped_set:
                    conn.execute("DELETE FROM pockettts_voice_tags WHERE assignment_id=?", (aid,))
                    upsert_counts["deleted_voice_tags"] += 1

            for tag_id in mapped_tags:
                aid = _assignment_id(voice_id, tag_id)
                conn.execute(
                    """
                    INSERT INTO pockettts_voice_tags
                        (assignment_id, voice_id, tag_id, assignment_source, confidence)
                    VALUES (?, ?, ?, 'import', 'medium')
                    ON CONFLICT(assignment_id) DO UPDATE SET
                        assignment_source='import',
                        confidence='medium',
                        updated_at=datetime('now')
                    """,
                    (aid, voice_id, tag_id),
                )
                upsert_counts["voice_tags"] += 1

        gen = increment_gen(conn, "human")

        for row in conn.execute("SELECT * FROM pockettts_tags").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tags", row["tag_id"], dict(row), gen)
        for row in conn.execute("SELECT * FROM pockettts_voices").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_voices", row["voice_id"], dict(row), gen)
        for row in conn.execute("SELECT * FROM pockettts_voice_meta").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_meta", row["voice_id"], dict(row), gen)
        for row in conn.execute("SELECT * FROM pockettts_voice_tags").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_voice_tags", row["assignment_id"], dict(row), gen)
        for row in conn.execute("SELECT * FROM pockettts_tag_order").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", "pockettts_tag_order", row["order_id"], dict(row), gen)

        summary_dump = _fetch_full_dump(conn)

    return {
        "imported": True,
        "base_url": base,
        "requested_base_url": body.base_url,
        **upsert_counts,
        "tag_count": len(summary_dump.get("tags", {})),
        "voice_count": len(summary_dump.get("voices", {})),
    }
