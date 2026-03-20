"""seekdb_sync.py — background SQLite -> SeekDB index sync for browser-links."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

from .ai_client import embed
from .db import get_conn, get_setting, set_setting, increment_gen
from .seekdb import (
    bookmark_embedding_by_normalized_url,
    delete_bookmark_index,
    init_seekdb,
    upsert_bookmark_index,
    upsert_visit_index,
    visit_embedding_by_normalized_url,
)
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

SETTING_LAST_SYNC = "seekdb_last_sync_ts"
SETTING_EXCLUDED_TAGS = "embedding_excluded_tags"
SETTING_RARE_DOMAINS = "embedding_rare_domains"
SETTING_DOMAIN_THRESHOLD = "embedding_domain_threshold"
DEFAULT_EXCLUDED_TAGS = "favourites-bar,web,interests"
DEFAULT_DOMAIN_THRESHOLD = "3"

SYNC_INTERVAL_SECONDS = 60

_loop_started = False

# Progress state for full reindex operations (in-memory, resets on restart)
_reindex_state: dict = {"running": False, "done": 0, "total": 0, "error": None}


def get_reindex_state() -> dict:
    return dict(_reindex_state)


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
        enqueue_for_all_peers(conn, "INSERT", "settings", SETTING_RARE_DOMAINS,
            {"key": SETTING_RARE_DOMAINS, "value": rare_json,
             "description": "Domains appearing <= domain_threshold times (auto-computed; included in embeddings)",
             "updated_at": None}, gen)
        set_setting(
            conn,
            SETTING_DOMAIN_THRESHOLD,
            str(threshold),
            description="Max occurrences for a domain to be treated as rare (informative) in embeddings",
        )
        gen2 = increment_gen(conn)
        enqueue_for_all_peers(conn, "INSERT", "settings", SETTING_DOMAIN_THRESHOLD,
            {"key": SETTING_DOMAIN_THRESHOLD, "value": str(threshold),
             "description": "Max occurrences for a domain to be treated as rare (informative) in embeddings",
             "updated_at": None}, gen2)

    log.info("analyze_domains: threshold=%d, rare=%d of %d total domains", threshold, len(rare), len(counts))
    return rare


async def reindex_all() -> None:
    """Re-embed every bookmark using current excluded_tags and rare_domains config.

    Updates _reindex_state continuously so the GUI can poll progress.
    Resets seekdb_last_sync_ts on completion so incremental sync picks up from now.
    """
    global _reindex_state
    _reindex_state = {"running": True, "done": 0, "total": 0, "error": None}
    try:
        excluded_tags = _get_excluded_tags()
        rare_domains = _get_rare_domains()

        with get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM bookmarks ORDER BY updated_at ASC"
            ).fetchall()

        row_dicts = [dict(r) for r in rows]
        _reindex_state["total"] = len(row_dicts)
        log.info("reindex_all: starting, %d bookmarks, excluded_tags=%r, rare_domains=%d",
                 len(row_dicts), excluded_tags, len(rare_domains))

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
                    )

            _reindex_state["done"] = min(i + EMBED_BATCH_SIZE, len(row_dicts))

        # Advance last_sync so the incremental loop doesn't redo everything
        with get_conn() as conn:
            set_setting(conn, SETTING_LAST_SYNC, _now_iso(),
                        description="Last successful SQLite->SeekDB sync timestamp")

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

    excluded_tags = _get_excluded_tags()
    rare_domains = _get_rare_domains()
    texts = [_build_bookmark_text(row, excluded_tags, rare_domains) for row in row_dicts]

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
    if _reindex_state["running"]:
        log.debug("seekdb_sync: reindex in progress — skipping incremental sync cycle")
        return {"bookmarks_synced": 0, "visits_synced": 0, "bookmarks_deleted": 0}

    init_seekdb()

    # Capture now BEFORE the queries so we can always advance SETTING_LAST_SYNC
    # even if embedding calls fail partway through.  A cycle that errors out
    # mid-way will leave some SeekDB entries stale until the next write touches
    # those rows, but it will NOT spin forever retrying the same bookmarks.
    now_ts = _now_iso()

    with get_conn() as conn:
        last_sync = get_setting(conn, SETTING_LAST_SYNC, "1970-01-01T00:00:00")

    bookmarks_synced = 0
    visits_synced = 0
    bookmarks_deleted = 0

    try:
        bookmarks_synced = await _sync_bookmarks_since(last_sync)
    except Exception:
        log.exception("seekdb_sync: bookmark sync failed — will advance last_sync anyway")

    try:
        visits_synced = await _sync_visits_since(last_sync)
    except Exception:
        log.exception("seekdb_sync: visit sync failed — will advance last_sync anyway")

    try:
        bookmarks_deleted = _sync_bookmark_deletions_since(last_sync)
    except Exception:
        log.exception("seekdb_sync: deletion sync failed — will advance last_sync anyway")

    # Always advance SETTING_LAST_SYNC regardless of per-step errors.
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
