"""seekdb_sync.py — background SQLite -> SeekDB index sync for browser-links."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

from .ai_client import embed
from .db import get_conn, get_setting, set_setting
from .seekdb import (
    bookmark_embedding_by_normalized_url,
    delete_bookmark_index,
    init_seekdb,
    upsert_bookmark_index,
    upsert_visit_index,
    visit_embedding_by_normalized_url,
)

log = logging.getLogger(__name__)

SETTING_LAST_SYNC = "seekdb_last_sync_ts"
SYNC_INTERVAL_SECONDS = 60

_loop_started = False


def _now_iso() -> str:
    # Use SQLite-compatible format (space separator, no timezone) so the
    # string comparison `updated_at > last_sync` works correctly in SQLite.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_tags(tags_json: str) -> list[str]:
    try:
        tags = json.loads(tags_json or "[]")
        if isinstance(tags, list):
            return [str(x) for x in tags]
    except (TypeError, json.JSONDecodeError):
        pass
    return []


EMBED_BATCH_SIZE = 100


async def _embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    if len(texts) <= EMBED_BATCH_SIZE:
        return await embed("browser-links", texts)
    results: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        results.extend(await embed("browser-links", texts[i : i + EMBED_BATCH_SIZE]))
    return results


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


async def _sync_bookmarks_since(last_sync: str) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM bookmarks WHERE updated_at > ? ORDER BY updated_at ASC",
            (last_sync,),
        ).fetchall()

        if not rows:
            return 0

        row_dicts = [dict(r) for r in rows]
        texts = []
        for row in row_dicts:
            tags = _parse_tags(row.get("tags_json") or "[]")
            text = " ".join(
                [
                    row.get("title") or "",
                    row.get("description") or "",
                    " ".join(tags),
                    row.get("notes") or "",
                ]
            ).strip()
            texts.append(text)

    embeddings = await _embed_texts(texts)

    with get_conn() as conn:
        for idx, row in enumerate(row_dicts):
            stats = conn.execute(
                """
                SELECT COUNT(*) AS cnt, MAX(visited_at) AS last_v
                FROM visits
                WHERE normalized_url = ?
                """,
                (row["normalized_url"],),
            ).fetchone()
            visit_count = int(stats["cnt"] if stats else 0)
            last_visited = stats["last_v"] if stats else None
            upsert_bookmark_index(
                row=row,
                embedding=embeddings[idx],
                visit_count=visit_count,
                last_visited=last_visited,
            )

    return len(row_dicts)


async def _sync_visits_since(last_sync: str) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM visits WHERE updated_at > ? ORDER BY updated_at ASC",
            (last_sync,),
        ).fetchall()

    if not rows:
        return 0

    synced = 0
    pending_embed_rows = []
    pending_embed_texts = []
    prepared: list[tuple[dict, list[float]]] = []

    for row in rows:
        d = dict(row)
        norm = d.get("normalized_url") or ""

        embedding = bookmark_embedding_by_normalized_url(norm)
        if embedding is None:
            embedding = visit_embedding_by_normalized_url(norm)

        if embedding is None:
            domain = d.get("domain") or _domain_from_url(d.get("url") or "")
            d["domain"] = domain
            pending_embed_rows.append(d)
            pending_embed_texts.append(f"{d.get('title', '')} {domain}".strip())
        else:
            prepared.append((d, embedding))

    if pending_embed_rows:
        new_embeddings = await _embed_texts(pending_embed_texts)
        for idx, d in enumerate(pending_embed_rows):
            prepared.append((d, new_embeddings[idx]))

    for row, emb in prepared:
        upsert_visit_index(row, emb)
        synced += 1

    return synced


def _sync_bookmark_deletions_since(last_sync: str) -> int:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT bookmark_id FROM bookmark_deletions WHERE deleted_at > ?",
            (last_sync,),
        ).fetchall()

    deleted = 0
    for row in rows:
        delete_bookmark_index(row["bookmark_id"])
        deleted += 1
    return deleted


async def sync_once() -> dict[str, int]:
    init_seekdb()

    with get_conn() as conn:
        last_sync = get_setting(conn, SETTING_LAST_SYNC, "1970-01-01T00:00:00")

    bookmarks_synced = await _sync_bookmarks_since(last_sync)
    visits_synced = await _sync_visits_since(last_sync)
    bookmarks_deleted = _sync_bookmark_deletions_since(last_sync)

    now_ts = _now_iso()
    with get_conn() as conn:
        set_setting(
            conn,
            SETTING_LAST_SYNC,
            now_ts,
            description="Last successful SQLite->SeekDB sync timestamp",
        )

    return {
        "bookmarks_synced": bookmarks_synced,
        "visits_synced": visits_synced,
        "bookmarks_deleted": bookmarks_deleted,
    }


async def _sync_loop() -> None:
    while True:
        try:
            stats = await sync_once()
            if any(stats.values()):
                log.info("seekdb_sync: %s", stats)
        except Exception:
            log.exception("seekdb_sync: sync cycle failed")
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


def start_seekdb_sync_loop() -> None:
    global _loop_started
    if _loop_started:
        return
    _loop_started = True
    asyncio.create_task(_sync_loop())


def trigger_seekdb_sync() -> None:
    """Run a one-shot sync soon after a direct local write."""
    asyncio.create_task(sync_once())
