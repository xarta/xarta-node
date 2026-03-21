"""routes_bookmarks.py — browser-links CRUD, visits, search, diagnostics, and extension download."""

from __future__ import annotations

import asyncio
import io
import json
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from .ai_client import embed, rerank, complete
from .db import get_conn, get_setting, set_setting, increment_gen
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
from .seekdb_sync import (
    trigger_seekdb_sync,
    reindex_all as _do_reindex_all,
    analyze_domains as _do_analyze_domains,
    get_reindex_state,
    SETTING_EXCLUDED_TAGS,
    SETTING_DOMAIN_THRESHOLD,
    SETTING_RARE_DOMAINS,
    DEFAULT_EXCLUDED_TAGS,
    DEFAULT_DOMAIN_THRESHOLD,
)
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
    limit: int = Query(500, ge=1, le=10000),
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

    counts = seekdb_counts()
    seekdb_indexed = counts["bookmarks_indexed"]
    visits_indexed = counts["visits_indexed"]
    # Stale = entries present in SQLite but absent from SeekDB.
    # Fast: two COUNTs already fetched above — just arithmetic.
    seekdb_stale = max(0, bookmark_count - seekdb_indexed)
    seekdb_visits_stale = max(0, visit_count - visits_indexed)

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
        "seekdb_indexed": seekdb_indexed,
        "seekdb_stale": seekdb_stale,
        "seekdb_visits_indexed": visits_indexed,
        "seekdb_visits_stale": seekdb_visits_stale,
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


@router.get("/tags-with-counts")
async def list_tags_with_counts() -> list[dict]:
    """Return all tags with active and archived bookmark counts."""
    with get_conn() as conn:
        rows = conn.execute("SELECT tags_json, archived FROM bookmarks").fetchall()
    counts: dict[str, dict] = {}
    for row in rows:
        arch = row["archived"]
        for t in _tags_from_json(row["tags_json"]):
            if t not in counts:
                counts[t] = {"tag": t, "active": 0, "archived": 0}
            if arch:
                counts[t]["archived"] += 1
            else:
                counts[t]["active"] += 1
    return sorted(counts.values(), key=lambda x: x["tag"])


# ── Embedding config & reindex ────────────────────────────────────────────────

@router.get("/embedding-config", response_model=dict)
async def get_embedding_config() -> dict:
    with get_conn() as conn:
        excluded_raw = get_setting(conn, SETTING_EXCLUDED_TAGS, DEFAULT_EXCLUDED_TAGS) or DEFAULT_EXCLUDED_TAGS
        threshold = int(get_setting(conn, SETTING_DOMAIN_THRESHOLD, DEFAULT_DOMAIN_THRESHOLD) or DEFAULT_DOMAIN_THRESHOLD)
        rare_raw = get_setting(conn, SETTING_RARE_DOMAINS, "[]") or "[]"
    excluded_tags = [t.strip() for t in excluded_raw.split(",") if t.strip()]
    try:
        rare_domains = json.loads(rare_raw)
    except Exception:
        rare_domains = []
    return {
        "excluded_tags": excluded_tags,
        "domain_threshold": threshold,
        "rare_domains_count": len(rare_domains),
        "rare_domains": rare_domains,
    }


@router.put("/embedding-config", response_model=dict)
async def put_embedding_config(body: dict) -> dict:
    with get_conn() as conn:
        if "excluded_tags" in body:
            tags = body["excluded_tags"]
            if isinstance(tags, list):
                val = ",".join(str(t).strip().lower() for t in tags if str(t).strip())
            else:
                val = str(tags)
            set_setting(conn, SETTING_EXCLUDED_TAGS, val,
                        description="Comma-separated tags to exclude from embeddings")
            gen = increment_gen(conn)
            row = {"key": SETTING_EXCLUDED_TAGS, "value": val,
                   "description": "Comma-separated tags to exclude from embeddings",
                   "updated_at": None}
            enqueue_for_all_peers(conn, "INSERT", "settings", SETTING_EXCLUDED_TAGS, row, gen)
        if "domain_threshold" in body:
            threshold = max(0, int(body["domain_threshold"]))
            val_t = str(threshold)
            set_setting(conn, SETTING_DOMAIN_THRESHOLD, val_t,
                        description="Max occurrences for a domain to be treated as rare (included in embeddings)")
            gen = increment_gen(conn)
            row = {"key": SETTING_DOMAIN_THRESHOLD, "value": val_t,
                   "description": "Max occurrences for a domain to be treated as rare (included in embeddings)",
                   "updated_at": None}
            enqueue_for_all_peers(conn, "INSERT", "settings", SETTING_DOMAIN_THRESHOLD, row, gen)
    return {"ok": True}


@router.post("/analyze-domains", response_model=dict)
async def post_analyze_domains(body: dict | None = None) -> dict:
    threshold: int | None = None
    if body and "domain_threshold" in body:
        threshold = max(0, int(body["domain_threshold"]))
    rare = _do_analyze_domains(threshold)
    with get_conn() as conn:
        used_threshold = int(get_setting(conn, SETTING_DOMAIN_THRESHOLD, DEFAULT_DOMAIN_THRESHOLD) or DEFAULT_DOMAIN_THRESHOLD)
    return {"rare_domains_count": len(rare), "threshold": used_threshold}


@router.post("/reindex", response_model=dict)
async def post_reindex() -> dict:
    state = get_reindex_state()
    if state["running"]:
        raise HTTPException(409, "Reindex already in progress")
    asyncio.create_task(_do_reindex_all())
    return {"ok": True, "message": "Reindex started"}


@router.get("/reindex-progress", response_model=dict)
async def get_reindex_progress() -> dict:
    return get_reindex_state()


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
    q_lower = q.lower()
    q_tokens = q_lower.split()  # individual words: ["github", "noise"]

    # Load excluded tags first — before any embedding or SeekDB call.
    with get_conn() as _conn:
        _excl_raw = get_setting(_conn, SETTING_EXCLUDED_TAGS, DEFAULT_EXCLUDED_TAGS)
    _excluded_tags: set[str] = {t.strip().lower() for t in (_excl_raw or "").split(",") if t.strip()}

    # If every token in the query is an excluded tag, return nothing immediately.
    # Searching for "favourites-bar" or "web" (nuisance tags) should produce no
    # results — not 50 vector results that happen to be semantically adjacent.
    if q_tokens and all(t in _excluded_tags for t in q_tokens):
        return {"query": q, "count": 0, "results": [], "excluded": True}

    query_embedding = (await embed("browser-links", [q]))[0]

    window = max(limit * 3, 30)
    bm_kw = keyword_search_bookmarks(q, window)
    bm_vec = vector_search_bookmarks(query_embedding, window)

    vis_kw = keyword_search_visits(q, window) if include_visits else []
    vis_vec = vector_search_visits(query_embedding, window) if include_visits else []

    def _searchable_tags(tags_json_str: str | None) -> str:
        """Return space-joined tag text with excluded tags removed."""
        try:
            all_tags = json.loads(tags_json_str or "[]")
        except Exception:
            all_tags = []
        return " ".join(t for t in (str(x) for x in all_tags) if t.lower() not in _excluded_tags)

    # Pre-rank keyword results by match location before RRF.
    # ChromaDB's $contains filter returns results in insertion order, not by
    # relevance. We sort so that the best substring matches get low rank
    # numbers → high RRF scores, dominating vector results that happen to be
    # semantically nearby but don't actually contain the query terms.
    #
    # Priority (lower number = better):
    #   0 — full phrase in title
    #   1 — full phrase in url
    #   2 — all tokens found anywhere across title+url+tags (cross-field)
    #   3 — full phrase matches any tag
    #   4 — at least one token in title
    #   5 — at least one token in url
    #   6 — at least one token in any tag
    #   7 — no substring match (document match only — e.g. description/notes)
    def _kw_rank(row: dict) -> int:
        meta = row.get("metadata") or {}
        title = (meta.get("title") or "").lower()
        url = (meta.get("url") or "").lower()
        combined = title + " " + url
        tags_text = _searchable_tags(meta.get("tags_json")).lower()
        all_text = combined + " " + tags_text
        if q_lower in title:
            return 0
        if q_lower in url:
            return 1
        if len(q_tokens) > 1 and all(t in all_text for t in q_tokens):
            return 2
        if q_lower in tags_text:
            return 3
        if any(t in title for t in q_tokens):
            return 4
        if any(t in url for t in q_tokens):
            return 5
        if any(t in tags_text for t in q_tokens):
            return 6
        return 7

    bm_kw.sort(key=_kw_rank)
    vis_kw.sort(key=_kw_rank)

    rrf: dict[tuple[str, str], float] = {}
    payload: dict[tuple[str, str], dict] = {}
    k = 60

    def _accumulate(rows: list[dict], src: str) -> None:
        is_kw = "keyword" in src
        is_vec = "vector" in src
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
                    "kw_tier": _kw_rank(row) if is_kw else None,
                    "cosine_distance": row.get("distance") if is_vec else None,
                    "title": meta.get("title") or "",
                    "url": meta.get("url") or "",
                    "description": meta.get("description") or "",
                    "tags": _tags_from_json(meta.get("tags_json")),
                    "notes": meta.get("notes") or "",
                    "domain": meta.get("domain") or "",
                    "visited_at": meta.get("visited_at") or "",
                    "bookmark_id": meta.get("bookmark_id") or "",
                    "source": meta.get("source") or "",
                    "created_at": meta.get("created_at") or "",
                }
            else:
                payload[key]["score_sources"].append(src)
                # Keep the best (lowest) keyword tier.
                if is_kw and payload[key]["kw_tier"] is None:
                    payload[key]["kw_tier"] = _kw_rank(row)
                elif is_kw:
                    payload[key]["kw_tier"] = min(payload[key]["kw_tier"], _kw_rank(row))
                # Keep the best (lowest) cosine distance.
                if is_vec:
                    dist = row.get("distance")
                    if dist is not None:
                        prev = payload[key]["cosine_distance"]
                        payload[key]["cosine_distance"] = dist if prev is None else min(prev, dist)

    _accumulate(bm_kw, "bookmark_keyword")
    _accumulate(bm_vec, "bookmark_vector")
    _accumulate(vis_kw, "visit_keyword")
    _accumulate(vis_vec, "visit_vector")

    top_keys = sorted(rrf, key=rrf.get, reverse=True)[:limit]
    results = [payload[k] | {"rrf_score": rrf[k]} for k in top_keys]

    if len(results) > 1:
        docs = [
            " ".join(filter(None, [
                r.get("title", ""),
                r.get("url", ""),
                r.get("description", ""),
                r.get("notes", ""),
            ])).strip()
            for r in results
        ]
        ranked = await rerank("browser-links", q, docs, top_n=min(limit, len(docs)))
        reranked = []
        for reranker_pos, r_item in enumerate(ranked):
            res = results[r_item["index"]]
            res["reranker_rank"] = reranker_pos + 1
            reranked.append(res)
        results = reranked
    else:
        for r in results:
            r["reranker_rank"] = 1

    # Post-reranker exact-match promotion.
    # Stable sort keeps the reranker's ordering within each tier.
    #
    # Tier 0 — full phrase found in title or url
    # Tier 1 — every token found across title+url+tags combined (cross-field)
    # Tier 2 — at least one token found in title or url
    # Tier 3 — phrase or token found in tags only
    # Tier 4 — no token match at all (pure embedding result)

    def _exact_tier(r: dict) -> int:
        title = (r.get("title") or "").lower()
        url = (r.get("url") or "").lower()
        combined = title + " " + url
        tags_text = " ".join(
            t for t in (r.get("tags") or []) if t.lower() not in _excluded_tags
        ).lower()
        all_text = combined + " " + tags_text
        if q_lower in combined:
            return 0
        if len(q_tokens) > 1 and all(t in all_text for t in q_tokens):
            return 1
        if any(t in combined for t in q_tokens):
            return 2
        if q_lower in tags_text or any(t in tags_text for t in q_tokens):
            return 3
        return 4

    # Compound sort: exact tier (keyword quality) primary; cosine distance secondary
    # (results that also appear in vector search are ordered by semantic similarity
    # within each tier); rrf_score as final tiebreaker.
    # null cosine_distance = float('inf') → keyword-only results sort after dual-match
    # results within the same tier, but still above pure-embedding (tier 4) results.
    results.sort(key=lambda r: (
        _exact_tier(r),
        r.get("cosine_distance") if r.get("cosine_distance") is not None else float("inf"),
        -(r.get("rrf_score") or 0.0),
    ))
    for r in results:
        r["exact_tier"] = _exact_tier(r)

    return {
        "query": q,
        "count": len(results),
        "results": results,
    }


# ── Score explanation endpoint ────────────────────────────────────────────────

_SCORE_PIPELINE_CONTEXT = """
You are a search ranking analyst explaining how a SINGLE bookmark result was scored in a hybrid search pipeline.

ACRONYMS AND TERMS:
- RRF = Reciprocal Rank Fusion — the score-merging formula that combines results from multiple ranking lists
- HNSW = Hierarchical Navigable Small World — the graph-based approximate nearest-neighbour index used for vector search
- KW = Keyword (the $contains full-text filter arm)
- VEC = Vector (the HNSW semantic embedding similarity arm)
- BM = Bookmark (a saved bookmark record)
- V = Visit (a browser history visit record)
- kw_tier = Keyword Tier — pre-RRF quality rank of where the query appears in this bookmark's text fields
- exact_tier = Exact Tier — post-reranker promotion tier based on literal token presence
- cosine_distance = geometric distance between query and bookmark in embedding space (0=identical, 1=orthogonal)
- score_sources = list of pipeline arms that returned this bookmark as a candidate
- reranker_rank = position assigned by the cross-encoder reranker (1=best)
- rrf_score = final Reciprocal Rank Fusion score (higher=better)

CRITICAL FRAMING — make this clear in your response:
- The pipeline always runs ALL four search arms (bookmark keyword, bookmark vector, visit keyword, visit vector) for the entire query.
- Each result's score_sources only lists which arms FOUND THAT SPECIFIC BOOKMARK in their top candidates.
- If score_sources shows only bookmark_keyword, it means the vector arm ran fine but this bookmark did not appear in its top candidates — NOT that vector search was skipped.
- Other results in the same search may have been found by vector search. Each result is independent.

The search pipeline (in order):
1. KEYWORD SEARCH — SeekDB $contains filter over a text document concatenating title+description+tags+notes+url. Results are pre-sorted by kw_tier before RRF: 0=full phrase in title, 1=full phrase in URL, 2=all tokens cross-field, 3=full phrase in tags, 4=any token in title, 5=any token in URL, 6=any token in tags, 7=document body only. Null kw_tier means this result was not a keyword match.
2. VECTOR SEARCH — Each bookmark has ONE embedding of its full concatenated document (title+description+tags+notes+url). Query is also embedded. Cosine HNSW nearest-neighbour search finds the 30 closest bookmarks. cosine_distance: 0=identical direction, 1=orthogonal. Null cosine_distance means this result was not a vector match (it appeared via keyword search only).
3. RRF (Reciprocal Rank Fusion) — Merges results from all contributing arms. score += 1/(60+rank+1) per arm. rrf_score is the sum — higher means better combined rank.
4. RERANKER — Cross-encoder rescores the top results by reading title+url+description+notes against the query. reranker_rank is 1-indexed position after this step.
5. EXACT TIER PROMOTION — Stable sort after reranking: 0=phrase in title/URL, 1=all tokens cross-field, 2=any token in title/URL, 3=tags only, 4=pure embedding.

The user's search query and full result JSON will be provided. Explain each metric clearly — what the value means specifically for this result and this query, and what it reveals about relevance. Use markdown with headings. Be concise and precise.
""".strip()

_FOCUS_CONTEXT = {
    "score_sources": ("Score Sources", "score_sources lists which of the 4 pipeline arms found THIS SPECIFIC BOOKMARK in their top candidates (bookmark_keyword, bookmark_vector, visit_keyword, visit_vector). IMPORTANT: all four arms always run for every search. An arm missing from score_sources means this bookmark was not in that arm's top candidates — not that the arm was skipped. Other results in the same search may have different sources. Each arm that finds a result adds 1/(60+rank+1) to the RRF score. Being found by multiple arms is a strong relevance signal."),
    "rrf_score": ("RRF Score", "rrf_score is the Reciprocal Rank Fusion total. For each source list a result appears in, the formula 1/(60+rank+1) is added, where rank is 0-indexed position in that list. Higher = better. Typical values: ~0.015 for a top result, ~0.005 for a lower one. The k=60 constant dampens rank differences — it prevents one very-top rank from dominating."),
    "kw_tier": ("Keyword Tier (kw_tier)", "kw_tier is the pre-RRF quality tier for keyword matches (0=best, 7=worst). It determines sort order within the keyword result list before RRF merging. 0: full phrase in title. 1: full phrase in URL. 2: all query tokens found across title+url+tags combined. 3: full phrase in tags. 4: any token in title. 5: any token in URL. 6: any token in any tag. 7: match only in description or notes (document body). Null means this result was NOT found by keyword search at all — it came from vector search only."),
    "cosine_distance": ("Cosine Distance", "cosine_distance is the geometric distance between the query embedding and the bookmark's embedding in the HNSW vector index. Both are computed from a concatenation of title+description+tags+notes+url. Scale: 0=identical direction, 1=orthogonal (no semantic overlap). Values below 0.3 indicate strong semantic match; 0.4-0.6 is moderate; above 0.6 is weak. Null means this result was NOT found by vector search — it appeared only via keyword match. The vector search ran but this bookmark was not in its top candidates."),
    "reranker_rank": ("Reranker Rank", "reranker_rank is the 1-indexed position assigned by the cross-encoder reranker. The reranker reads the bookmark's title+url+description+notes together with the query and produces a relevance score — it understands full sentences and context, unlike the embedding model which computes a single vector. rank=1 is best. A null value means only one result was returned (no reranking needed) or the result was below the top-N cutoff."),
    "exact_tier": ("Exact Tier", "exact_tier is the post-reranker promotion tier (stable sort, so reranker order is preserved within each tier). 0: full phrase in title or URL. 1: all query tokens found across title+url+tags. 2: any query token in title or URL. 3: phrase or token only in tags. 4: pure semantic match (no token overlap at all). This ensures that a result containing the literal query words is never buried below a purely semantic result, even if the reranker ranked it lower."),
}


@router.post("/score-explain", response_model=dict)
async def post_score_explain(body: dict) -> dict:
    """Call the browser-links LLM to explain how a search result was scored and ranked."""
    query: str = (body.get("query") or "").strip()
    result: dict = body.get("result") or {}
    focus: str | None = body.get("focus")  # None = overview; metric key = drill-down

    if focus and focus in _FOCUS_CONTEXT:
        metric_label, metric_context = _FOCUS_CONTEXT[focus]
        metric_value = result.get(focus)
        title = result.get("title") or result.get("url") or "this bookmark"
        system = (
            f"You are a search ranking analyst. Explain the '{metric_label}' metric in detail for one specific search result.\n\n"
            f"Metric context:\n{metric_context}\n\n"
            f"Be specific to the actual value and the bookmark's content. Use markdown. Be concise but complete."
        )
        user_content = (
            f"/no-think\n"
            f"Search query: \"{query}\"\n"
            f"Bookmark: \"{title}\"\n"
            f"Metric: {metric_label}\n"
            f"Value: {json.dumps(metric_value)}\n\n"
            f"Full result JSON for context:\n{json.dumps(result, indent=2, default=str)}\n\n"
            f"Explain specifically why {metric_label} has this value for this bookmark and this query. "
            f"What does it tell us about how relevant this bookmark is?"
        )
    else:
        title = result.get("title") or result.get("url") or "this bookmark"
        system = _SCORE_PIPELINE_CONTEXT
        user_content = (
            f"/no-think\n"
            f"Search query: \"{query}\"\n"
            f"Bookmark title: \"{title}\"\n\n"
            f"Full result JSON:\n{json.dumps(result, indent=2, default=str)}\n\n"
            f"Explain each scoring metric for this result. For each metric, say what the value means "
            f"specifically for this bookmark and this query. Does each metric suggest this result is relevant? "
            f"How did the pipeline steps combine to produce this final ranking?"
        )

    try:
        answer = await complete(
            "browser-links",
            [{"role": "system", "content": system}, {"role": "user", "content": user_content}],
            max_tokens=2000,
            strip_think=True,
            no_think=True,
        )
    except Exception as e:
        raise HTTPException(500, f"LLM error: {e}")

    return {"explanation": answer, "focus": focus}


_SORT_EXPLAIN_SYSTEM = """
You are a search ranking analyst explaining to a user exactly why a list of bookmark search results is ordered the way it is.

ACRONYMS AND TERMS:
- RRF = Reciprocal Rank Fusion — score-merging formula combining multiple result lists: score += 1/(60+rank+1) per arm
- HNSW = Hierarchical Navigable Small World — graph-based approximate nearest-neighbour index used for vector search
- KW = Keyword (the $contains full-text filter arm)
- VEC = Vector (the HNSW semantic embedding similarity arm)
- BM = Bookmark record; V = Visit (browser history) record
- kw_tier = Keyword Tier — pre-RRF quality rank of where the query appears in the bookmark's text (0=best, 7=worst, null=not a keyword match)
- exact_tier = Exact Tier — post-reranker promotion tier based on literal token presence (0=best, 4=worst)
- cosine_distance = geometric distance between query and bookmark embedding (0=identical direction, 1=orthogonal; null=not found by vector search)
- rrf_score = final merged score from all contributing arms (higher=better)
- reranker_rank = position assigned by cross-encoder reranker (1=best; may be overridden by compound sort)
- score_sources = which pipeline arms returned this bookmark as a candidate

THE COMPOUND SORT KEY (applied in this order of priority):
1. exact_tier (ascending — lower is better): coarse grouping by how well query tokens match title/URL/tags
   - 0: full query phrase in title or URL
   - 1: every token found across title+url+tags combined
   - 2: at least one token in title or URL
   - 3: phrase or token only in tags
   - 4: no token match — pure semantic/embedding result
2. cosine_distance (ascending — lower is better): within each exact_tier group, results also found by vector search are sorted by semantic closeness. Results NOT found by vector search (null cosine_distance) sort after all vector-matched results in the same tier.
3. rrf_score (descending — higher is better): final tiebreaker within the same tier and distance group.

The user will supply the search query, the top results in final sort order (as a JSON array with all scoring fields), and the current sort state if the user has clicked a column header.

Your job: explain clearly and concisely WHY each result is in its current position. Walk through the compound sort key. Identify which tier group(s) are present, point out what's driving the ordering within each group, and flag any interesting cases (e.g. a result with a great reranker_rank that is not at the top because exact_tier or cosine_distance pushed it down). Use markdown with clear sections.
""".strip()


@router.post("/sort-explain", response_model=dict)
async def post_sort_explain(body: dict) -> dict:
    """Call the browser-links LLM to explain the sort order of the current search results."""
    query: str = (body.get("query") or "").strip()
    results: list = body.get("results") or []
    sort_col: str = body.get("sort_col") or "compound"
    sort_dir: str = body.get("sort_dir") or "asc"

    if not results:
        raise HTTPException(400, "No results provided")

    # Strip large fields to keep the prompt tight but keep all scoring fields
    slim = []
    for i, r in enumerate(results):
        slim.append({
            "rank": i + 1,
            "title": (r.get("title") or "")[:120],
            "url": (r.get("url") or "")[:100],
            "score_sources": r.get("score_sources"),
            "kw_tier": r.get("kw_tier"),
            "cosine_distance": r.get("cosine_distance"),
            "rrf_score": r.get("rrf_score"),
            "reranker_rank": r.get("reranker_rank"),
            "exact_tier": r.get("exact_tier"),
            "item_type": r.get("item_type"),
        })

    sort_note = (
        f"NOTE: The user has manually re-sorted the table by column '{sort_col}' ({sort_dir}). "
        f"The compound sort above was the original order; the user-selected sort is now applied instead."
        if sort_col != "compound"
        else "The results are shown in the default compound sort order."
    )

    user_content = (
        f"/no-think\n"
        f"Search query: \"{query}\"\n"
        f"{sort_note}\n\n"
        f"Results in current display order (top {len(slim)}):\n"
        f"{json.dumps(slim, indent=2, default=str)}\n\n"
        f"Explain the sort order. Walk through each result group, what's driving position, "
        f"and point out any interesting or surprising placements."
    )

    try:
        answer = await complete(
            "browser-links",
            [{"role": "system", "content": _SORT_EXPLAIN_SYSTEM},
             {"role": "user", "content": user_content}],
            max_tokens=2500,
            strip_think=True,
            no_think=True,
        )
    except Exception as e:
        raise HTTPException(500, f"LLM error: {e}")

    return {"explanation": answer}


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


@router.get("/extension-version")
async def extension_version() -> dict:
    """Return the current extension version from manifest.json.  Auth-exempt, open CORS."""
    manifest_path = Path(__file__).parent.parent / "static" / "extension" / "manifest.json"
    try:
        data = json.loads(manifest_path.read_text())
        version = data.get("version", "unknown")
    except Exception:
        version = "unknown"
    return {"version": version, "download_path": "/api/v1/bookmarks/extension-download"}


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


@router.post("/check-dead-links", response_model=dict)
async def check_dead_links(
    timeout: float = Query(8.0, ge=1.0, le=30.0),
    concurrency: int = Query(50, ge=1, le=100),
) -> dict:
    """
    HEAD-check every non-archived bookmark URL in parallel.
    Any that return HTTP 404 or 410 are automatically archived and
    the change is enqueued for fleet sync.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT bookmark_id, url, title FROM bookmarks WHERE archived=0 LIMIT 5000"
        ).fetchall()
    bookmarks = [dict(r) for r in rows]

    sem = asyncio.Semaphore(concurrency)

    async def _check_one(client: httpx.AsyncClient, bm: dict) -> dict:
        async with sem:
            url = bm["url"]
            try:
                resp = await client.head(url, follow_redirects=True)
                status = resp.status_code
                dead = status in (404, 410)
                return {"bookmark_id": bm["bookmark_id"], "url": url,
                        "title": bm.get("title") or url, "status": status, "dead": dead}
            except Exception as exc:
                return {"bookmark_id": bm["bookmark_id"], "url": url,
                        "title": bm.get("title") or url, "status": None,
                        "dead": False, "error": str(exc)[:120]}

    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=10)
    async with httpx.AsyncClient(
        headers={"User-Agent": "BlueprintsLinkChecker/1.0"},
        timeout=timeout,
        limits=limits,
    ) as client:
        results = list(await asyncio.gather(*[_check_one(client, bm) for bm in bookmarks]))

    dead = [r for r in results if r.get("dead")]
    errors = [r for r in results if "error" in r]

    now = _now_iso()
    with get_conn() as conn:
        for r in dead:
            conn.execute(
                "UPDATE bookmarks SET archived=1, updated_at=? WHERE bookmark_id=?",
                (now, r["bookmark_id"]),
            )
            row = conn.execute(
                "SELECT * FROM bookmarks WHERE bookmark_id=?", (r["bookmark_id"],)
            ).fetchone()
            gen = increment_gen(conn, "human")
            enqueue_for_all_peers(conn, "UPDATE", "bookmarks", r["bookmark_id"], dict(row), gen)

    if dead:
        trigger_seekdb_sync()

    return {
        "checked": len(bookmarks),
        "archived": len(dead),
        "errors": len(errors),
        "dead": [
            {"bookmark_id": r["bookmark_id"], "url": r["url"],
             "title": r["title"], "status": r.get("status")}
            for r in dead
        ],
    }


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
