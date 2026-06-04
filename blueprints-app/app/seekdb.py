"""seekdb.py — SeekDB access helpers for browser-links search indexes.

SeekDB is used as a per-node search index only.
Canonical bookmark/visit records remain in SQLite.
"""

from __future__ import annotations

import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any

import pyseekdb
from pyseekdb.client.configuration import Configuration, FulltextIndexConfig, HNSWConfiguration

from . import config as cfg

BOOKMARKS_COLLECTION = "bookmarks_index"
VISITS_COLLECTION = "visits_index"
VECTOR_DIM = 2048

_client: pyseekdb.Client | None = None
_bookmarks_col = None
_visits_col = None
_io_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="seekdb-io")
_io_lock = threading.RLock()

DEFAULT_SEEKDB_TIMEOUT_SECONDS = 5.0


def _client_instance() -> pyseekdb.Client:
    global _client
    if _client is None:
        _client = pyseekdb.Client(
            host=cfg.SEEKDB_HOST,
            port=cfg.SEEKDB_PORT,
            database=cfg.SEEKDB_DB,
            user=cfg.SEEKDB_USER,
            password=cfg.SEEKDB_PASSWORD,
        )
    return _client


def reset_seekdb_cache() -> None:
    """Drop cached client/collections after connection-level failures."""
    global _client, _bookmarks_col, _visits_col
    with _io_lock:
        _client = None
        _bookmarks_col = None
        _visits_col = None


def short_seekdb_error(exc: BaseException) -> str:
    """Return a compact non-secret error summary suitable for health payloads."""
    first_line = str(exc).replace("\r", " ").split("\n", 1)[0].strip()
    if not first_line:
        first_line = exc.__class__.__name__
    return first_line[:180]


def _blocking_call(fn, *args, **kwargs):
    with _io_lock:
        return fn(*args, **kwargs)


async def run_seekdb_blocking_async(
    fn,
    *args,
    timeout: float = DEFAULT_SEEKDB_TIMEOUT_SECONDS,
    reset_on_error: bool = True,
    **kwargs,
):
    """Run a pyseekdb operation in the dedicated single-worker executor."""
    loop = asyncio.get_running_loop()
    call = partial(_blocking_call, fn, *args, **kwargs)
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(_io_executor, call),
            timeout=timeout,
        )
    except Exception:
        if reset_on_error:
            reset_seekdb_cache()
        raise


def _collection_config() -> Configuration:
    return Configuration(
        hnsw=HNSWConfiguration(dimension=VECTOR_DIM, distance="cosine"),
        fulltext_config=FulltextIndexConfig(analyzer="space"),
    )


def init_seekdb() -> None:
    """Create/open required collections. Raises if SeekDB is not available."""
    global _bookmarks_col, _visits_col
    client = _client_instance()
    if _bookmarks_col is None:
        _bookmarks_col = client.get_or_create_collection(
            name=BOOKMARKS_COLLECTION,
            embedding_function=None,
            configuration=_collection_config(),
        )
    if _visits_col is None:
        _visits_col = client.get_or_create_collection(
            name=VISITS_COLLECTION,
            embedding_function=None,
            configuration=_collection_config(),
        )


async def init_seekdb_async(timeout: float = DEFAULT_SEEKDB_TIMEOUT_SECONDS) -> None:
    await run_seekdb_blocking_async(init_seekdb, timeout=timeout)


def bookmarks_col():
    init_seekdb()
    return _bookmarks_col


def visits_col():
    init_seekdb()
    return _visits_col


def _to_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True)


def bookmark_document(row: dict[str, Any]) -> str:
    tags = []
    try:
        tags = json.loads(row.get("tags_json") or "[]")
    except (TypeError, json.JSONDecodeError):
        tags = []
    return " ".join(
        [
            row.get("title") or "",
            row.get("description") or "",
            " ".join(tags),
            row.get("notes") or "",
            row.get("url") or "",
        ]
    ).strip()


def visit_document(row: dict[str, Any]) -> str:
    return " ".join(
        [
            row.get("title") or "",
            row.get("domain") or "",
            row.get("url") or "",
        ]
    ).strip()


def upsert_bookmark_index(
    row: dict[str, Any],
    embedding: list[float],
    visit_count: int,
    last_visited: str | None,
    document: str | None = None,
) -> None:
    # `document` should be the pre-filtered text (excluded tags stripped).  If
    # not supplied we fall back to the unfiltered bookmark_document(row).  Always
    # supply it from seekdb_sync so that excluded tags are absent from the stored
    # document and therefore invisible to keyword ($contains) searches.
    metadata = {
        "item_type": "bookmark",
        "bookmark_id": row["bookmark_id"],
        "url": row.get("url") or "",
        "normalized_url": row.get("normalized_url") or "",
        "title": row.get("title") or "",
        "description": row.get("description") or "",
        "tags_json": row.get("tags_json") or "[]",
        "folder": row.get("folder") or "",
        "notes": row.get("notes") or "",
        "source": row.get("source") or "manual",
        "archived": int(row.get("archived") or 0),
        "created_at": row.get("created_at") or "",
        "updated_at": row.get("updated_at") or "",
        "visit_count": int(visit_count),
        "last_visited": last_visited or "",
    }
    bookmarks_col().upsert(
        ids=[row["bookmark_id"]],
        embeddings=[embedding],
        documents=[document if document is not None else bookmark_document(row)],
        metadatas=[metadata],
    )


def upsert_visit_index(row: dict[str, Any], embedding: list[float]) -> None:
    metadata = {
        "item_type": "visit",
        "visit_id": row["visit_id"],
        "bookmark_id": row.get("bookmark_id") or "",
        "url": row.get("url") or "",
        "normalized_url": row.get("normalized_url") or "",
        "domain": row.get("domain") or "",
        "title": row.get("title") or "",
        "source": row.get("source") or "visit-recorder",
        "dwell_seconds": int(row.get("dwell_seconds") or 0),
        "visited_at": row.get("visited_at") or "",
        "updated_at": row.get("updated_at") or "",
    }
    visits_col().upsert(
        ids=[row["visit_id"]],
        embeddings=[embedding],
        documents=[visit_document(row)],
        metadatas=[metadata],
    )


def delete_bookmark_index(bookmark_id: str) -> None:
    bookmarks_col().delete(ids=[bookmark_id])


def delete_visit_index(visit_id: str) -> None:
    visits_col().delete(ids=[visit_id])


def _extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    # col.query() returns nested lists [[...]] (one per query embedding).
    # col.get()   returns flat lists   [...].
    # Detect by checking whether the first ids element is itself a list.
    raw_ids = payload.get("ids") or []
    nested = bool(raw_ids) and isinstance(raw_ids[0], list)

    if nested:
        ids = raw_ids[0]
        docs = (payload.get("documents") or [[]])[0]
        metas = (payload.get("metadatas") or [[]])[0]
        dists = (payload.get("distances") or [[]])[0]
    else:
        ids = raw_ids
        docs = payload.get("documents") or []
        metas = payload.get("metadatas") or []
        dists = payload.get("distances") or []

    rows: list[dict[str, Any]] = []
    for idx, item_id in enumerate(ids):
        meta = metas[idx] if idx < len(metas) else {}
        doc = docs[idx] if idx < len(docs) else ""
        dist = dists[idx] if idx < len(dists) else None
        rows.append(
            {
                "id": item_id,
                "document": doc,
                "metadata": meta or {},
                "distance": dist,
            }
        )
    return rows


def keyword_search_bookmarks(query: str, limit: int) -> list[dict[str, Any]]:
    res = bookmarks_col().get(
        where_document={"$contains": query},
        limit=limit,
        include=["documents", "metadatas"],
    )
    return _extract_rows(res)


def vector_search_bookmarks(query_embedding: list[float], limit: int) -> list[dict[str, Any]]:
    res = bookmarks_col().query(
        query_embeddings=[query_embedding],
        n_results=limit,
        include=["documents", "metadatas", "distances"],
    )
    return _extract_rows(res)


def keyword_search_visits(query: str, limit: int) -> list[dict[str, Any]]:
    res = visits_col().get(
        where_document={"$contains": query},
        limit=limit,
        include=["documents", "metadatas"],
    )
    return _extract_rows(res)


def vector_search_visits(query_embedding: list[float], limit: int) -> list[dict[str, Any]]:
    res = visits_col().query(
        query_embeddings=[query_embedding],
        n_results=limit,
        include=["documents", "metadatas", "distances"],
    )
    return _extract_rows(res)


def bookmark_embedding_by_normalized_url(normalized_url: str) -> list[float] | None:
    res = bookmarks_col().get(
        where={"normalized_url": {"$eq": normalized_url}},
        limit=1,
        include=["embeddings"],
    )
    emb = res.get("embeddings") or []
    return emb[0] if emb else None


def visit_embedding_by_normalized_url(normalized_url: str) -> list[float] | None:
    res = visits_col().get(
        where={"normalized_url": {"$eq": normalized_url}},
        limit=1,
        include=["embeddings"],
    )
    emb = res.get("embeddings") or []
    return emb[0] if emb else None


def seekdb_counts() -> dict[str, int]:
    return {
        "bookmarks_indexed": int(bookmarks_col().count()),
        "visits_indexed": int(visits_col().count()),
    }


async def seekdb_counts_async(timeout: float = DEFAULT_SEEKDB_TIMEOUT_SECONDS) -> dict[str, int]:
    return await run_seekdb_blocking_async(seekdb_counts, timeout=timeout)


async def keyword_search_bookmarks_async(query: str, limit: int) -> list[dict[str, Any]]:
    return await run_seekdb_blocking_async(keyword_search_bookmarks, query, limit)


async def vector_search_bookmarks_async(
    query_embedding: list[float],
    limit: int,
) -> list[dict[str, Any]]:
    return await run_seekdb_blocking_async(vector_search_bookmarks, query_embedding, limit)


async def keyword_search_visits_async(query: str, limit: int) -> list[dict[str, Any]]:
    return await run_seekdb_blocking_async(keyword_search_visits, query, limit)


async def vector_search_visits_async(
    query_embedding: list[float],
    limit: int,
) -> list[dict[str, Any]]:
    return await run_seekdb_blocking_async(vector_search_visits, query_embedding, limit)


async def bookmark_embedding_by_normalized_url_async(normalized_url: str) -> list[float] | None:
    return await run_seekdb_blocking_async(bookmark_embedding_by_normalized_url, normalized_url)


async def visit_embedding_by_normalized_url_async(normalized_url: str) -> list[float] | None:
    return await run_seekdb_blocking_async(visit_embedding_by_normalized_url, normalized_url)


async def upsert_bookmark_index_async(
    row: dict[str, Any],
    embedding: list[float],
    visit_count: int,
    last_visited: str | None,
    document: str | None = None,
) -> None:
    await run_seekdb_blocking_async(
        upsert_bookmark_index,
        row,
        embedding,
        visit_count,
        last_visited,
        document,
    )


async def upsert_visit_index_async(row: dict[str, Any], embedding: list[float]) -> None:
    await run_seekdb_blocking_async(upsert_visit_index, row, embedding)


async def delete_bookmark_index_async(bookmark_id: str) -> None:
    await run_seekdb_blocking_async(delete_bookmark_index, bookmark_id)


async def delete_visit_index_async(visit_id: str) -> None:
    await run_seekdb_blocking_async(delete_visit_index, visit_id)


def _bookmark_index_metadata_sync() -> tuple[list[str], list[dict[str, Any]]]:
    col = bookmarks_col()
    raw = col.get(include=["metadatas"], limit=max(col.count(), 1))
    return raw.get("ids") or [], raw.get("metadatas") or []


async def bookmark_index_metadata_async() -> tuple[list[str], list[dict[str, Any]]]:
    return await run_seekdb_blocking_async(_bookmark_index_metadata_sync)


def _visit_index_metadata_sync() -> tuple[list[str], list[dict[str, Any]]]:
    col = visits_col()
    raw = col.get(include=["metadatas"], limit=max(col.count(), 1))
    return raw.get("ids") or [], raw.get("metadatas") or []


async def visit_index_metadata_async() -> tuple[list[str], list[dict[str, Any]]]:
    return await run_seekdb_blocking_async(_visit_index_metadata_sync)
