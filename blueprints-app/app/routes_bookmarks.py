"""routes_bookmarks.py — browser-links CRUD, visits, search, diagnostics, and extension download."""

from __future__ import annotations

import io
import json
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from .ai_client import embed, rerank
from .db import get_conn, increment_gen
from .models import (
    BookmarkCreate,
    BookmarkImportRequest,
    BookmarkImportResult,
    BookmarkOut,
    BookmarkUpdate,
    VisitCreate,
    VisitOut,
)
from .seekdb import (
    keyword_search_bookmarks,
    keyword_search_visits,
    seekdb_counts,
    vector_search_bookmarks,
    vector_search_visits,
)
from .seekdb_sync import trigger_seekdb_sync
from .sync.queue import enqueue_for_all_peers

router = APIRouter(prefix="/bookmarks", tags=["bookmarks"])


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _normalize_url(url: str) -> str:
    p = urlparse((url or "").strip())
    path = p.path.rstrip("/") or "/"
    return urlunparse((p.scheme.lower(), p.netloc.lower(), path, p.params, p.query, ""))


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _tags_to_json(tags: list[str] | None) -> str:
    if not tags:
        return "[]"
    clean = [str(t).strip().lower() for t in tags if str(t).strip()]
    dedup = []
    seen = set()
    for t in clean:
        if t not in seen:
            seen.add(t)
            dedup.append(t)
    return json.dumps(dedup, ensure_ascii=True)


def _tags_from_json(raw: str | None) -> list[str]:
    try:
        val = json.loads(raw or "[]")
        if isinstance(val, list):
            return [str(x) for x in val]
    except (TypeError, json.JSONDecodeError):
        pass
    return []


def _row_to_bookmark_out(row) -> BookmarkOut:
    d = dict(row)
    d["tags"] = _tags_from_json(d.get("tags_json"))
    d["archived"] = bool(d.get("archived", 0))
    d.pop("tags_json", None)
    return BookmarkOut(**d)


def _row_to_visit_out(row) -> VisitOut:
    return VisitOut(**dict(row))


@router.get("", response_model=list[BookmarkOut])
async def list_bookmarks(
    archived: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
) -> list[BookmarkOut]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM bookmarks
            WHERE archived = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (1 if archived else 0, limit),
        ).fetchall()
    return [_row_to_bookmark_out(r) for r in rows]


# ── Static GET routes — MUST come before /{bookmark_id} ──────────────────────

@router.get("/health", response_model=dict)
async def bookmarks_health() -> dict:
    with get_conn() as conn:
        bookmark_count = int(conn.execute("SELECT COUNT(*) FROM bookmarks").fetchone()[0])
        visit_count = int(conn.execute("SELECT COUNT(*) FROM visits").fetchone()[0])
        last_sync = conn.execute(
            "SELECT value FROM settings WHERE key='seekdb_last_sync_ts'",
        ).fetchone()
        last_sync_ts = last_sync[0] if last_sync else ""

    counts = seekdb_counts()
    emb_ok = "ok"
    emb_err = ""
    try:
        vec = await embed("browser-links", ["health check"])
        if not vec or len(vec[0]) != 2048:
            emb_ok = "error"
            emb_err = "unexpected embedding dimensions"
    except Exception as exc:
        emb_ok = "error"
        emb_err = str(exc)

    status = "ok" if emb_ok == "ok" else "degraded"
    return {
        "status": status,
        "sqlite": "ok",
        "seekdb": "ok",
        "embedding": emb_ok,
        "embedding_error": emb_err,
        "bookmark_count": bookmark_count,
        "visit_count": visit_count,
        "seekdb_indexed": counts["bookmarks_indexed"],
        "seekdb_visits_indexed": counts["visits_indexed"],
        "last_seekdb_sync": last_sync_ts,
    }


@router.get("/tags", response_model=list[str])
async def list_tags() -> list[str]:
    with get_conn() as conn:
        rows = conn.execute("SELECT tags_json FROM bookmarks WHERE archived=0").fetchall()
    tags = set()
    for row in rows:
        for t in _tags_from_json(row["tags_json"]):
            tags.add(t)
    return sorted(tags)


@router.get("/visits", response_model=list[VisitOut])
async def list_visits(limit: int = Query(500, ge=1, le=5000)) -> list[VisitOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM visits ORDER BY visited_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [_row_to_visit_out(r) for r in rows]


@router.get("/search", response_model=dict)
async def search_bookmarks(
    q: str = Query(..., min_length=1),
    limit: int = Query(20, ge=1, le=100),
    include_visits: bool = Query(True),
) -> dict:
    query_embedding = (await embed("browser-links", [q]))[0]

    window = max(limit * 3, 30)
    bm_kw = keyword_search_bookmarks(q, window)
    bm_vec = vector_search_bookmarks(query_embedding, window)

    vis_kw = keyword_search_visits(q, window) if include_visits else []
    vis_vec = vector_search_visits(query_embedding, window) if include_visits else []

    rrf: dict[tuple[str, str], float] = {}
    payload: dict[tuple[str, str], dict] = {}
    k = 60

    def _accumulate(rows: list[dict], src: str) -> None:
        for rank, row in enumerate(rows):
            meta = row.get("metadata") or {}
            item_type = str(meta.get("item_type") or "bookmark")
            item_id = str(row.get("id"))
            key = (item_type, item_id)
            rrf[key] = rrf.get(key, 0.0) + (1.0 / (k + rank + 1))
            if key not in payload:
                payload[key] = {
                    "item_type": item_type,
                    "id": item_id,
                    "score_sources": [src],
                    "title": meta.get("title") or "",
                    "url": meta.get("url") or "",
                    "description": meta.get("description") or "",
                    "tags": _tags_from_json(meta.get("tags_json")),
                    "notes": meta.get("notes") or "",
                    "domain": meta.get("domain") or "",
                    "visited_at": meta.get("visited_at") or "",
                    "bookmark_id": meta.get("bookmark_id") or "",
                    "source": meta.get("source") or "",
                }
            else:
                payload[key]["score_sources"].append(src)

    _accumulate(bm_kw, "bookmark_keyword")
    _accumulate(bm_vec, "bookmark_vector")
    _accumulate(vis_kw, "visit_keyword")
    _accumulate(vis_vec, "visit_vector")

    top_keys = sorted(rrf, key=rrf.get, reverse=True)[:limit]
    results = [payload[k] | {"rrf_score": rrf[k]} for k in top_keys]

    if len(results) > 1:
        docs = [
            f"{r.get('title', '')}. {r.get('description', '')}. {r.get('notes', '')}".strip()
            for r in results
        ]
        ranked = await rerank("browser-links", q, docs, top_n=min(limit, len(docs)))
        results = [results[r["index"]] for r in ranked]

    return {
        "query": q,
        "count": len(results),
        "results": results,
    }


@router.get("/extension-download")
async def download_extension() -> StreamingResponse:
    """Serve the browser extension as a zip archive for manual load-unpacked install."""
    ext_dir = Path(__file__).parent.parent / "static" / "extension"
    if not ext_dir.is_dir():
        raise HTTPException(404, "Extension directory not found on this node")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fpath in sorted(ext_dir.rglob("*")):
            if fpath.is_file():
                zf.write(fpath, fpath.relative_to(ext_dir))
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="blueprints-bookmarks-extension.zip"'},
    )


# ── CRUD routes ───────────────────────────────────────────────────────────────

@router.post("", response_model=BookmarkOut, status_code=201)
async def create_bookmark(body: BookmarkCreate) -> BookmarkOut:
    bookmark_id = str(uuid.uuid4())
    normalized_url = _normalize_url(body.url)
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO bookmarks (
                bookmark_id, url, normalized_url, title, description,
                tags_json, folder, notes, favicon_url, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                bookmark_id,
                body.url,
                normalized_url,
                body.title,
                body.description,
                _tags_to_json(body.tags),
                body.folder,
                body.notes,
                body.favicon_url,
                body.source,
            ),
        )
        row = conn.execute("SELECT * FROM bookmarks WHERE bookmark_id=?", (bookmark_id,)).fetchone()
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "INSERT", "bookmarks", bookmark_id, dict(row), gen)
    trigger_seekdb_sync()
    return _row_to_bookmark_out(row)


@router.post("/import", response_model=BookmarkImportResult)
async def import_bookmarks(body: BookmarkImportRequest) -> BookmarkImportResult:
    imported = 0
    skipped = 0
    with get_conn() as conn:
        for bm in body.bookmarks:
            normalized_url = _normalize_url(bm.url)
            if body.skip_duplicates:
                existing = conn.execute(
                    "SELECT bookmark_id FROM bookmarks WHERE normalized_url=?",
                    (normalized_url,),
                ).fetchone()
                if existing:
                    skipped += 1
                    continue

            bookmark_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO bookmarks (
                    bookmark_id, url, normalized_url, title, description,
                    tags_json, folder, notes, favicon_url, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bookmark_id,
                    bm.url,
                    normalized_url,
                    bm.title,
                    bm.description,
                    _tags_to_json(bm.tags),
                    bm.folder,
                    bm.notes,
                    bm.favicon_url,
                    bm.source,
                ),
            )
            row = conn.execute("SELECT * FROM bookmarks WHERE bookmark_id=?", (bookmark_id,)).fetchone()
            gen = increment_gen(conn, "human")
            enqueue_for_all_peers(conn, "INSERT", "bookmarks", bookmark_id, dict(row), gen)
            imported += 1

    if imported:
        trigger_seekdb_sync()
    return BookmarkImportResult(imported=imported, skipped_duplicates=skipped)


@router.post("/visits", response_model=VisitOut, status_code=201)
async def create_visit(body: VisitCreate) -> VisitOut:
    visit_id = str(uuid.uuid4())
    normalized_url = _normalize_url(body.url)
    visited_at = body.visited_at or _now_iso()
    with get_conn() as conn:
        bm = conn.execute(
            "SELECT bookmark_id FROM bookmarks WHERE normalized_url=? LIMIT 1",
            (normalized_url,),
        ).fetchone()
        bookmark_id = bm["bookmark_id"] if bm else None
        conn.execute(
            """
            INSERT INTO visits (
                visit_id, url, normalized_url, domain, title, source,
                dwell_seconds, bookmark_id, visited_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                visit_id,
                body.url,
                normalized_url,
                _domain(body.url),
                body.title,
                body.source,
                body.dwell_seconds,
                bookmark_id,
                visited_at,
            ),
        )
        row = conn.execute("SELECT * FROM visits WHERE visit_id=?", (visit_id,)).fetchone()
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "INSERT", "visits", visit_id, dict(row), gen)

    trigger_seekdb_sync()
    return _row_to_visit_out(row)


@router.get("/{bookmark_id}", response_model=BookmarkOut)
async def get_bookmark(bookmark_id: str) -> BookmarkOut:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bookmarks WHERE bookmark_id=?", (bookmark_id,)).fetchone()
    if not row:
        raise HTTPException(404, f"Bookmark {bookmark_id!r} not found")
    return _row_to_bookmark_out(row)


@router.put("/{bookmark_id}", response_model=BookmarkOut)
async def update_bookmark(bookmark_id: str, body: BookmarkUpdate) -> BookmarkOut:
    with get_conn() as conn:
        current = conn.execute(
            "SELECT * FROM bookmarks WHERE bookmark_id=?",
            (bookmark_id,),
        ).fetchone()
        if not current:
            raise HTTPException(404, f"Bookmark {bookmark_id!r} not found")

        new_url = body.url if body.url is not None else current["url"]
        normalized_url = _normalize_url(new_url)
        tags_json = _tags_to_json(body.tags) if body.tags is not None else current["tags_json"]

        conn.execute(
            """
            UPDATE bookmarks SET
                url            = ?,
                normalized_url = ?,
                title          = COALESCE(?, title),
                description    = COALESCE(?, description),
                tags_json      = ?,
                folder         = COALESCE(?, folder),
                notes          = COALESCE(?, notes),
                favicon_url    = COALESCE(?, favicon_url),
                source         = COALESCE(?, source),
                archived       = COALESCE(?, archived),
                updated_at     = datetime('now')
            WHERE bookmark_id = ?
            """,
            (
                new_url,
                normalized_url,
                body.title,
                body.description,
                tags_json,
                body.folder,
                body.notes,
                body.favicon_url,
                body.source,
                None if body.archived is None else (1 if body.archived else 0),
                bookmark_id,
            ),
        )

        row = conn.execute("SELECT * FROM bookmarks WHERE bookmark_id=?", (bookmark_id,)).fetchone()
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "UPDATE", "bookmarks", bookmark_id, dict(row), gen)

    trigger_seekdb_sync()
    return _row_to_bookmark_out(row)


@router.delete("/{bookmark_id}", status_code=204, response_model=None)
async def delete_bookmark(bookmark_id: str) -> None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM bookmarks WHERE bookmark_id=?", (bookmark_id,)).fetchone()
        if not row:
            raise HTTPException(404, f"Bookmark {bookmark_id!r} not found")

        conn.execute("DELETE FROM bookmarks WHERE bookmark_id=?", (bookmark_id,))
        conn.execute(
            """
            INSERT INTO bookmark_deletions (bookmark_id, deleted_at)
            VALUES (?, datetime('now'))
            ON CONFLICT(bookmark_id) DO UPDATE SET deleted_at=datetime('now')
            """,
            (bookmark_id,),
        )

        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "bookmarks", bookmark_id, {}, gen)
        del_row = conn.execute(
            "SELECT * FROM bookmark_deletions WHERE bookmark_id=?",
            (bookmark_id,),
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "bookmark_deletions", bookmark_id, dict(del_row), gen)

    trigger_seekdb_sync()
