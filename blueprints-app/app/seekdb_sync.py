"""seekdb_sync.py — background SQLite -> SeekDB index sync for browser-links."""

from __future__ import annotations

import asyncio
import json
import logging
import random
from collections import Counter
from urllib.parse import urlparse

from .ai_client import embed
from .db import get_conn, get_setting, increment_gen, set_setting
from .seekdb import (
    bookmark_embedding_by_normalized_url,
    bookmarks_col,
    delete_bookmark_index,
    delete_visit_index,
    init_seekdb,
    upsert_bookmark_index,
    upsert_visit_index,
    visit_embedding_by_normalized_url,
    visits_col,
)
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

SETTING_EXCLUDED_TAGS = "embedding_excluded_tags"
SETTING_RARE_DOMAINS = "embedding_rare_domains"
SETTING_DOMAIN_THRESHOLD = "embedding_domain_threshold"
DEFAULT_EXCLUDED_TAGS = "favourites-bar,web,interests"
DEFAULT_DOMAIN_THRESHOLD = "3"

SYNC_INTERVAL_SECONDS = 60
SYNC_JITTER_SECONDS = 20  # each node sleeps 60 + randint(0, 20) between cycles
EMBED_BATCH_PAUSE_SECONDS = 2  # pause between LiteLLM batches to avoid thundering herd
SYNC_MAX_STALE_PER_CYCLE = (
    200  # max bookmarks/visits embedded per sync cycle; remainder deferred to next
)

_loop_started = False
_loop_task: asyncio.Task | None = None

# Progress state for full reindex operations (in-memory, resets on restart)
_reindex_state: dict = {"running": False, "done": 0, "total": 0, "error": None}


def get_reindex_state() -> dict:
    return dict(_reindex_state)


def _parse_tags(tags_json: str) -> list[str]:
    try:
        tags = json.loads(tags_json or "[]")
        if isinstance(tags, list):
            return [str(x) for x in tags]
    except (TypeError, json.JSONDecodeError):
        pass
    return []


def _get_excluded_tags() -> set[str]:
    with get_conn() as conn:
        val = get_setting(conn, SETTING_EXCLUDED_TAGS, DEFAULT_EXCLUDED_TAGS)
    if not val:
        return set()
    return {t.strip().lower() for t in val.split(",") if t.strip()}


def _get_rare_domains() -> set[str]:
    with get_conn() as conn:
        val = get_setting(conn, SETTING_RARE_DOMAINS, "[]")
    try:
        return set(json.loads(val or "[]"))
    except (json.JSONDecodeError, TypeError):
        return set()


def _sld_from_netloc(netloc: str) -> str:
    """Strip TLD from a hostname, returning subdomain + SLD only.

    Examples:
      'obscure-tool.io'      -> 'obscure-tool'
      'api.obscure-tool.io'  -> 'api.obscure-tool'
      'github.com'           -> 'github'
    Imperfect for multi-part TLDs (.co.uk) but good enough for semantic signal.
    """
    host = netloc.split(":")[0].lower()
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[:-1])
    return host


def _build_bookmark_text(row: dict, excluded_tags: set[str], rare_domains: set[str]) -> str:
    """Build the text that will be embedded for a bookmark."""
    tags = _parse_tags(row.get("tags_json") or "[]")
    filtered_tags = [t for t in tags if t.lower() not in excluded_tags]

    parts = [
        row.get("title") or "",
        row.get("description") or "",
        " ".join(filtered_tags),
        row.get("notes") or "",
    ]

    # Include the SLD for rare (high-signal) domains
    url = row.get("url") or ""
    if url and rare_domains:
        try:
            netloc = urlparse(url).netloc.lower()
            bare_host = netloc.split(":")[0]
            if bare_host in rare_domains:
                sld = _sld_from_netloc(netloc)
                if sld:
                    parts.append(sld)
        except Exception:
            pass

    return " ".join(parts).strip()


def analyze_domains(threshold: int | None = None) -> list[str]:
    """Compute rare domains (appearing <= threshold times) and persist to settings.

    Returns the sorted list of rare domain hostnames.
    """
    with get_conn() as conn:
        if threshold is None:
            threshold = int(
                get_setting(conn, SETTING_DOMAIN_THRESHOLD, DEFAULT_DOMAIN_THRESHOLD)
                or DEFAULT_DOMAIN_THRESHOLD
            )
        rows = conn.execute("SELECT url FROM bookmarks WHERE url IS NOT NULL").fetchall()

    counts: Counter = Counter()
    for (url,) in rows:
        try:
            netloc = urlparse(url).netloc.lower().split(":")[0]
            if netloc:
                counts[netloc] += 1
        except Exception:
            pass

    rare = sorted(domain for domain, cnt in counts.items() if cnt <= threshold)

    with get_conn() as conn:
        rare_json = json.dumps(rare)
        set_setting(
            conn,
            SETTING_RARE_DOMAINS,
            rare_json,
            description="Domains appearing <= domain_threshold times (auto-computed; included in embeddings)",
        )
        gen = increment_gen(conn)
        enqueue_for_all_peers(
            conn,
            "INSERT",
            "settings",
            SETTING_RARE_DOMAINS,
            {
                "key": SETTING_RARE_DOMAINS,
                "value": rare_json,
                "description": "Domains appearing <= domain_threshold times (auto-computed; included in embeddings)",
                "updated_at": None,
            },
            gen,
        )
        set_setting(
            conn,
            SETTING_DOMAIN_THRESHOLD,
            str(threshold),
            description="Max occurrences for a domain to be treated as rare (informative) in embeddings",
        )
        gen2 = increment_gen(conn)
        enqueue_for_all_peers(
            conn,
            "INSERT",
            "settings",
            SETTING_DOMAIN_THRESHOLD,
            {
                "key": SETTING_DOMAIN_THRESHOLD,
                "value": str(threshold),
                "description": "Max occurrences for a domain to be treated as rare (informative) in embeddings",
                "updated_at": None,
            },
            gen2,
        )

    log.info(
        "analyze_domains: threshold=%d, rare=%d of %d total domains",
        threshold,
        len(rare),
        len(counts),
    )
    return rare


async def reindex_all() -> None:
    """Re-embed every bookmark using current excluded_tags and rare_domains config.

    Updates _reindex_state continuously so the GUI can poll progress.
    The incremental sync will find nothing stale afterwards because all SeekDB
    metadata updated_at values will match the SQLite rows.
    """
    global _reindex_state
    _reindex_state = {"running": True, "done": 0, "total": 0, "error": None}
    try:
        excluded_tags = _get_excluded_tags()
        rare_domains = _get_rare_domains()

        with get_conn() as conn:
            rows = conn.execute("SELECT * FROM bookmarks ORDER BY updated_at ASC").fetchall()

        row_dicts = [dict(r) for r in rows]
        _reindex_state["total"] = len(row_dicts)
        log.info(
            "reindex_all: starting, %d bookmarks, excluded_tags=%r, rare_domains=%d",
            len(row_dicts),
            excluded_tags,
            len(rare_domains),
        )

        for i in range(0, len(row_dicts), EMBED_BATCH_SIZE):
            batch = row_dicts[i : i + EMBED_BATCH_SIZE]
            texts = [_build_bookmark_text(row, excluded_tags, rare_domains) for row in batch]
            embeddings = await _embed_texts(texts)

            with get_conn() as conn:
                for idx, row in enumerate(batch):
                    stats = conn.execute(
                        "SELECT COUNT(*) AS cnt, MAX(visited_at) AS last_v FROM visits WHERE normalized_url = ?",
                        (row["normalized_url"],),
                    ).fetchone()
                    visit_count = int(stats["cnt"] if stats else 0)
                    last_visited = stats["last_v"] if stats else None
                    upsert_bookmark_index(
                        row=row,
                        embedding=embeddings[idx],
                        visit_count=visit_count,
                        last_visited=last_visited,
                        document=texts[idx],
                    )

            _reindex_state["done"] = min(i + EMBED_BATCH_SIZE, len(row_dicts))

        _reindex_state["done"] = len(row_dicts)
        _reindex_state["running"] = False
        log.info("reindex_all: complete, %d bookmarks re-embedded", len(row_dicts))

    except Exception as exc:
        _reindex_state["running"] = False
        _reindex_state["error"] = str(exc)
        log.exception("reindex_all: failed")


EMBED_BATCH_SIZE = 100


async def _embed_texts(texts: list[str]) -> list[list[float]]:
    if not texts:
        return []
    if len(texts) <= EMBED_BATCH_SIZE:
        return await embed("browser-links", texts)
    results: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        if i > 0:
            await asyncio.sleep(EMBED_BATCH_PAUSE_SECONDS)
        results.extend(await embed("browser-links", texts[i : i + EMBED_BATCH_SIZE]))
    return results


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


async def _sync_bookmarks_stale() -> tuple[int, int]:
    """Upsert stale/missing bookmark embeddings; delete orphaned SeekDB entries.

    An embedding is stale when the SQLite row's updated_at is newer than the
    updated_at stored in SeekDB metadata at the time it was last embedded.
    Returns (embedded_count, deleted_count).
    """
    with get_conn() as conn:
        sqlite_rows = conn.execute("SELECT * FROM bookmarks").fetchall()
    sqlite_dict = {r["bookmark_id"]: dict(r) for r in sqlite_rows}

    try:
        col = bookmarks_col()
        raw = col.get(include=["metadatas"], limit=max(col.count(), 1))
    except Exception:
        log.exception("seekdb_sync: failed to read bookmark index — skipping")
        return 0, 0

    raw_ids = raw.get("ids") or []
    raw_metas = raw.get("metadatas") or []
    seekdb_map: dict[str, str] = {
        raw_ids[i]: (raw_metas[i] or {}).get("updated_at", "") for i in range(len(raw_ids))
    }

    # Delete SeekDB entries with no matching SQLite row (bookmark was deleted)
    deleted = 0
    for bid in list(seekdb_map):
        if bid not in sqlite_dict:
            delete_bookmark_index(bid)
            deleted += 1

    # Find bookmarks with missing or outdated embeddings
    stale = [
        row
        for bid, row in sqlite_dict.items()
        if bid not in seekdb_map or (row.get("updated_at") or "") > seekdb_map[bid]
    ]

    if not stale:
        return 0, deleted

    # Cap per cycle so a node catching up from zero doesn't flood LiteLLM.
    # Stale rows not processed here will be picked up next cycle.
    stale = stale[:SYNC_MAX_STALE_PER_CYCLE]

    excluded_tags = _get_excluded_tags()
    rare_domains = _get_rare_domains()
    texts = [_build_bookmark_text(row, excluded_tags, rare_domains) for row in stale]
    embeddings = await _embed_texts(texts)

    with get_conn() as conn:
        for idx, row in enumerate(stale):
            stats = conn.execute(
                "SELECT COUNT(*) AS cnt, MAX(visited_at) AS last_v FROM visits WHERE normalized_url = ?",
                (row["normalized_url"],),
            ).fetchone()
            upsert_bookmark_index(
                row=row,
                embedding=embeddings[idx],
                visit_count=int(stats["cnt"] if stats else 0),
                last_visited=stats["last_v"] if stats else None,
                document=texts[idx],
            )

    return len(stale), deleted


async def _sync_visits_stale() -> tuple[int, int]:
    """Upsert stale/missing visit embeddings; delete orphaned SeekDB visit entries.

    Reuses existing bookmark embeddings for visits whose URL is already bookmarked,
    avoiding an unnecessary LiteLLM call.
    Returns (embedded_count, deleted_count).
    """
    with get_conn() as conn:
        sqlite_rows = conn.execute("SELECT * FROM visits").fetchall()
    sqlite_dict = {r["visit_id"]: dict(r) for r in sqlite_rows}

    try:
        col = visits_col()
        raw = col.get(include=["metadatas"], limit=max(col.count(), 1))
    except Exception:
        log.exception("seekdb_sync: failed to read visit index — skipping")
        return 0, 0

    raw_ids = raw.get("ids") or []
    raw_metas = raw.get("metadatas") or []
    seekdb_map: dict[str, str] = {
        raw_ids[i]: (raw_metas[i] or {}).get("updated_at", "") for i in range(len(raw_ids))
    }

    # Delete SeekDB entries with no matching SQLite row (visit was deleted)
    deleted = 0
    for vid in list(seekdb_map):
        if vid not in sqlite_dict:
            delete_visit_index(vid)
            deleted += 1

    # Find visits with missing or outdated embeddings
    stale = [
        row
        for vid, row in sqlite_dict.items()
        if vid not in seekdb_map or (row.get("updated_at") or "") > seekdb_map[vid]
    ]

    if not stale:
        return 0, deleted

    # Cap per cycle — same reasoning as bookmarks.
    stale = stale[:SYNC_MAX_STALE_PER_CYCLE]

    pending_embed_rows: list[dict] = []
    pending_embed_texts: list[str] = []
    prepared: list[tuple[dict, list[float]]] = []

    for d in stale:
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

    return len(stale), deleted


async def sync_once() -> dict[str, int]:
    """One sync cycle: embed stale bookmarks/visits, delete orphans from SeekDB.

    Uses per-row updated_at comparison between SQLite and SeekDB metadata —
    no global timestamp state required.
    """
    if _reindex_state["running"]:
        log.debug("seekdb_sync: reindex in progress — skipping incremental sync cycle")
        return {
            "bookmarks_synced": 0,
            "bookmarks_deleted": 0,
            "visits_synced": 0,
            "visits_deleted": 0,
        }

    init_seekdb()

    bookmarks_synced = bookmarks_deleted = 0
    visits_synced = visits_deleted = 0

    try:
        bookmarks_synced, bookmarks_deleted = await _sync_bookmarks_stale()
    except Exception:
        log.exception("seekdb_sync: bookmark sync failed")

    try:
        visits_synced, visits_deleted = await _sync_visits_stale()
    except Exception:
        log.exception("seekdb_sync: visit sync failed")

    return {
        "bookmarks_synced": bookmarks_synced,
        "bookmarks_deleted": bookmarks_deleted,
        "visits_synced": visits_synced,
        "visits_deleted": visits_deleted,
    }


async def _sync_loop() -> None:
    # Initial random delay so nodes starting simultaneously don't all hit LiteLLM at once
    await asyncio.sleep(random.randint(0, SYNC_JITTER_SECONDS))
    while True:
        try:
            stats = await sync_once()
            if any(stats.values()):
                log.info("seekdb_sync: %s", stats)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("seekdb_sync: sync cycle failed")
        await asyncio.sleep(SYNC_INTERVAL_SECONDS + random.randint(0, SYNC_JITTER_SECONDS))


def start_seekdb_sync_loop() -> None:
    global _loop_started, _loop_task
    if _loop_started:
        return
    _loop_started = True
    _loop_task = asyncio.create_task(_sync_loop())


async def stop_seekdb_sync_loop() -> None:
    """Cancel the background SeekDB sync loop and wait for it to exit."""
    global _loop_started, _loop_task
    task = _loop_task
    _loop_started = False
    _loop_task = None
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        log.info("seekdb_sync: loop stopped")


def trigger_seekdb_sync() -> None:
    """Run a one-shot sync soon after a direct local write."""
    asyncio.create_task(sync_once())
