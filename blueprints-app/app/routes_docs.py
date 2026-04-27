"""routes_docs.py — CRUD for the docs table + file read/write.

GET    /api/v1/docs                      → list all doc metadata
GET    /api/v1/docs/{doc_id}             → metadata + file content
POST   /api/v1/docs                      → create doc record (creates file if not exists)
PUT    /api/v1/docs/{doc_id}             → update metadata only
PUT    /api/v1/docs/{doc_id}/content     → overwrite file content + touch updated_at
DELETE /api/v1/docs/{doc_id}            → delete record; ?delete_file=true also removes the file
"""

import logging
import os
import shlex
import shutil
import subprocess
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, increment_gen
from .models import DocContentBody, DocCreate, DocOut, DocUpdate, DocWithContent
from .nullclaw_docs_search import (
    SynthesisControls,
    blueprints_synthesis_response,
    ensure_succeeded,
    submit_query_synthesis,
    synthesis_display_block,
)
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/docs", tags=["docs"])

_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_DOCS_SENTINEL = _NODE_LOCAL_ROOT / ".docs-pending-commit"


class DocsSearchBody(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    mode: str = "hybrid"
    top_k: int = Field(default=8, ge=1, le=30)
    vector_k: int = Field(default=40, ge=1, le=120)
    keyword_k: int = Field(default=40, ge=1, le=120)
    rerank: bool = True


class DocsSearchExplainBody(SynthesisControls):
    explanation_mode: Literal["summary", "answer"] = "answer"


class DocsGroupFolderOpenBody(BaseModel):
    group_id: str | None = None


class DocsSearchSyncBody(BaseModel):
    force: bool = False
    paths: list[str] | None = None


class DocsGroupFolderTreeBody(BaseModel):
    group_id: str | None = None
    path: str | None = Field(default=None, max_length=2000)


def _touch_docs_sentinel() -> None:
    """Touch the sentinel file so the lone-wolf commit cron picks up the change."""
    try:
        _DOCS_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
        _DOCS_SENTINEL.touch()
    except Exception as exc:  # non-fatal — backup is best-effort
        log.warning("docs: could not touch sentinel %s: %s", _DOCS_SENTINEL, exc)


def _docs_root() -> Path:
    root = cfg.DOCS_ROOT or cfg.REPO_INNER_PATH
    if not root:
        raise HTTPException(503, "DOCS_ROOT (or REPO_INNER_PATH) not configured — cannot locate docs")
    return Path(root)


def _normalize_ownership(root: Path, target: Path) -> None:
    """Hand ownership of created doc paths back to the docs root owner."""
    try:
        owner = root.stat()
    except Exception as exc:
        log.warning("docs: could not stat docs root %s for ownership hand-back: %s", root, exc)
        return

    current = target
    while True:
        try:
            if current.exists():
                os.chown(current, owner.st_uid, owner.st_gid)
        except Exception as exc:
            log.warning("docs: could not normalize ownership on %s: %s", current, exc)
        if current == root or current.parent == current:
            break
        current = current.parent


def _safe_resolve(root: Path, rel_path: str) -> Path:
    """Resolve rel_path under root, raising 400 on path traversal."""
    resolved = (root / rel_path).resolve()
    root_resolved = str(root.resolve())
    if str(resolved).startswith(root_resolved + "/") or str(resolved) == root_resolved:
        return resolved
    raise HTTPException(400, "Path escapes docs root")


def _row_to_out(row) -> DocOut:
    cols = row.keys()
    return DocOut(
        doc_id=row["doc_id"],
        label=row["label"],
        description=row["description"],
        tags=row["tags"],
        path=row["path"],
        sort_order=row["sort_order"] if row["sort_order"] is not None else 0,
        group_id=row["group_id"] if "group_id" in cols else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _doc_path_candidates(doc_path: str) -> list[str]:
    clean = (doc_path or "").strip().lstrip("/")
    if not clean:
        return []
    candidates = [clean]
    if not clean.startswith("docs/"):
        candidates.append(f"docs/{clean}")
    else:
        candidates.append(clean.removeprefix("docs/"))
    seen: set[str] = set()
    ordered: list[str] = []
    for item in candidates:
        key = item.lower()
        if key and key not in seen:
            seen.add(key)
            ordered.append(item)
    return ordered


def _docs_folder_opener() -> list[str]:
    configured = os.environ.get("BLUEPRINTS_DOCS_FOLDER_OPEN_CMD", "").strip()
    if configured:
        return shlex.split(configured)
    for candidate in ("xdg-open", "gio", "open"):
        found = shutil.which(candidate)
        if not found:
            continue
        if candidate == "gio":
            return [found, "open"]
        return [found]
    return []


def _open_docs_folder(folder: Path) -> None:
    cmd = _docs_folder_opener()
    if not cmd:
        raise HTTPException(
            503,
            "No folder opener is available; set BLUEPRINTS_DOCS_FOLDER_OPEN_CMD",
        )
    try:
        proc = subprocess.Popen(
            [*cmd, str(folder)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            _, stderr = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            return
        if proc.returncode != 0:
            detail = (stderr or "").strip() or f"exit code {proc.returncode}"
            raise HTTPException(500, f"Could not open folder: {detail[:300]}")
    except OSError as exc:
        raise HTTPException(500, f"Could not open folder: {exc}") from exc


def _result_snippet(text: str, limit: int = 620) -> str:
    snippet = " ".join((text or "").split())
    if len(snippet) <= limit:
        return snippet
    return snippet[: limit - 1].rstrip() + "…"


def _docs_search_chunk_limit(document_count: int) -> int:
    """Fetch a wider chunk set so the UI can group by document."""
    doc_count = max(1, min(30, int(document_count or 8)))
    return min(120, max(doc_count * 5, doc_count + 20))


def _registered_docs_by_path() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM docs").fetchall()
    by_path: dict[str, Any] = {}
    for row in rows:
        path = (row["path"] or "").strip()
        if not path:
            continue
        by_path[path.lower()] = row
        if path.startswith("docs/"):
            by_path[path.removeprefix("docs/").lower()] = row
    return by_path


def _normalize_docs_rel(path: str) -> str:
    clean = (path or "").strip().replace("\\", "/").strip("/")
    return "." if clean in ("", ".") else clean


def _docs_rel_path(root: Path, path: Path) -> str:
    try:
        rel = path.resolve().relative_to(root.resolve())
    except ValueError:
        return str(path)
    rel_text = rel.as_posix()
    return "." if rel_text in ("", ".") else rel_text


def _docs_group_path_rows(group_id: str | None) -> list[Any]:
    if group_id:
        query = """
            SELECT path
            FROM docs
            WHERE group_id=?
            ORDER BY sort_order, label
        """
        params: tuple[Any, ...] = (group_id,)
    else:
        query = """
            SELECT path
            FROM docs
            WHERE group_id IS NULL OR group_id=''
            ORDER BY sort_order, label
        """
        params = ()

    with get_conn() as conn:
        return conn.execute(query, params).fetchall()


def _most_common_docs_folder(root: Path, rows: list[Any]) -> tuple[Path, Counter[Path]]:
    if not rows:
        raise HTTPException(404, "No documents in this group")

    folders: list[Path] = []
    first_seen: dict[Path, int] = {}
    for row in rows:
        doc_path = str(row["path"] or "").strip()
        if not doc_path:
            continue
        resolved = _safe_resolve(root, doc_path)
        folder = resolved if resolved.is_dir() else resolved.parent
        if folder not in first_seen:
            first_seen[folder] = len(folders)
        folders.append(folder)

    if not folders:
        raise HTTPException(404, "No document paths in this group")

    counts = Counter(folders)
    folder = max(counts, key=lambda item: (counts[item], -first_seen[item]))
    if not folder.exists():
        raise HTTPException(404, f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise HTTPException(400, f"Not a folder: {folder}")
    return folder, counts


def _registered_docs_tree_lookup() -> dict[str, dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT doc_id, label, description, tags, path, group_id
            FROM docs
            ORDER BY sort_order, label
            """
        ).fetchall()

    by_path: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _normalize_docs_rel(row["path"])
        lower_key = key.lower()
        if not key or lower_key in by_path:
            continue
        by_path[lower_key] = {
            "doc_id": row["doc_id"],
            "label": row["label"],
            "description": row["description"],
            "tags": row["tags"],
            "path": row["path"],
            "group_id": row["group_id"] if "group_id" in row.keys() else None,
        }
    return by_path


def _enrich_search_result(raw: dict[str, Any], docs_by_path: dict[str, Any], root: Path) -> dict[str, Any]:
    doc_path = str(raw.get("doc_path") or "").strip().lstrip("/")
    candidates = _doc_path_candidates(doc_path)
    row = next((docs_by_path.get(p.lower()) for p in candidates if p.lower() in docs_by_path), None)
    registered_path = row["path"] if row else None
    file_path = registered_path or (candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else ""))

    file_exists = False
    if file_path:
        try:
            file_exists = _safe_resolve(root, file_path).is_file()
        except HTTPException:
            file_exists = False

    doc_registered = row is not None
    openable = bool(doc_registered and file_exists)
    if openable:
        register_hint = "registered"
    elif file_exists:
        register_hint = "add_to_docs_viewer"
    else:
        register_hint = "stale_index"

    title = raw.get("title") or ""
    if row:
        title = row["label"] or title
    if not title and doc_path:
        title = Path(doc_path).stem.replace("-", " ").replace("_", " ").title()

    return {
        "doc_path": doc_path,
        "viewer_path": registered_path,
        "register_path": None if doc_registered else file_path,
        "title": title,
        "chunk_index": raw.get("chunk_index"),
        "snippet": _result_snippet(str(raw.get("text") or "")),
        "score": raw.get("score"),
        "rerank_score": raw.get("rerank_score"),
        "doc_registered": doc_registered,
        "doc_id": row["doc_id"] if row else None,
        "doc_group_id": row["group_id"] if row and "group_id" in row.keys() else None,
        "file_exists": file_exists,
        "openable": openable,
        "register_hint": register_hint,
        "match_sources": raw.get("match_sources") or (["vector"] if raw.get("vector_rank") is not None else []),
        "vector_rank": raw.get("vector_rank"),
        "vector_score": raw.get("vector_score"),
        "keyword_rank": raw.get("keyword_rank"),
        "keyword_score": raw.get("keyword_score"),
        "rrf_score": raw.get("rrf_score"),
        "keyword_terms": raw.get("keyword_terms") or [],
        "updated_at": raw.get("updated_at"),
        "handle": raw.get("handle"),
    }


# ── List ──────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[DocOut])
async def list_docs() -> list[DocOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM docs ORDER BY sort_order, label"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


# ── List unregistered files ───────────────────────────────────────────────────

@router.get("/unregistered", response_model=list[str])
async def list_unregistered_docs() -> list[str]:
    """Return relative paths of .md files inside DOCS_ROOT not yet in the docs table."""
    root = _docs_root()
    with get_conn() as conn:
        rows = conn.execute("SELECT path FROM docs").fetchall()
    registered = {row["path"] for row in rows}
    unregistered: list[str] = []
    for p in sorted(root.rglob("*.md")):
        parts = p.relative_to(root).parts
        # Skip anything inside hidden directories (e.g. .git)
        if any(part.startswith(".") for part in parts[:-1]):
            continue
        rel = "/".join(parts)
        if rel not in registered:
            unregistered.append(rel)
    return unregistered


# ── Search proxy ──────────────────────────────────────────────────────────────

@router.post("/search", response_model=dict)
async def search_docs(body: DocsSearchBody) -> dict:
    """Proxy node-local TurboVec Docs search and enrich results for the viewer."""
    mode = (body.mode or "hybrid").strip().lower()
    if mode not in {"vector", "hybrid", "keyword"}:
        raise HTTPException(400, "mode must be one of: vector, hybrid, keyword")

    base_url = cfg.TURBOVEC_DOCS_URL.rstrip("/")
    if not base_url:
        raise HTTPException(503, "TURBOVEC_DOCS_URL is not configured")

    chunk_limit = _docs_search_chunk_limit(body.top_k)
    if mode == "vector":
        endpoint = "/query"
        payload: dict[str, Any] = {
            "query": body.query,
            "top_k": chunk_limit,
            "candidate_k": max(body.vector_k, chunk_limit),
            "rerank": body.rerank,
        }
    else:
        endpoint = "/hybrid-query"
        payload = {
            "query": body.query,
            "top_k": chunk_limit,
            "vector_k": max(body.vector_k, chunk_limit),
            "keyword_k": max(body.keyword_k, chunk_limit),
            "rerank": body.rerank,
            "mode": mode,
        }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(cfg.TURBOVEC_DOCS_TIMEOUT)) as client:
            resp = await client.post(f"{base_url}{endpoint}", json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "TurboVec Docs search timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"TurboVec Docs unavailable: {exc}") from exc

    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"TurboVec Docs search failed: {detail}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise HTTPException(502, "TurboVec Docs returned invalid JSON") from exc

    docs_by_path = _registered_docs_by_path()
    root = _docs_root()
    raw_results = data.get("results") if isinstance(data, dict) else []
    if not isinstance(raw_results, list):
        raw_results = []

    results = [
        _enrich_search_result(r, docs_by_path, root)
        for r in raw_results
        if isinstance(r, dict)
    ]
    unique_documents = {
        str(r.get("doc_id") or r.get("viewer_path") or r.get("register_path") or r.get("doc_path") or "").lower()
        for r in results
    }
    unique_documents.discard("")
    return {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "mode": mode,
        "query": body.query,
        "rerank": body.rerank,
        "document_target": body.top_k,
        "chunk_candidate_limit": chunk_limit,
        "document_count": len(unique_documents),
        "result_count": len(results),
        "results": results,
        "upstream": {
            "endpoint": endpoint,
            "url": base_url,
            "result_count": len(raw_results),
        },
    }


@router.post("/search/explain", response_model=dict)
async def explain_docs_search(body: DocsSearchExplainBody) -> dict[str, Any]:
    """Return a grounded synthesis for a docs search query via nullclaw-docs-search."""
    task = await submit_query_synthesis(body, body.explanation_mode)
    ensure_succeeded(task)
    response = blueprints_synthesis_response(
        task,
        route="/api/v1/docs/search/explain",
        projection="explain",
    )
    response["display"] = synthesis_display_block(response)
    return response


@router.post("/search/sync", response_model=dict)
async def sync_docs_search_index(body: DocsSearchSyncBody | None = None) -> dict[str, Any]:
    """Proxy TurboVec Docs incremental index sync for updated Markdown files."""
    body = body or DocsSearchSyncBody()
    base_url = cfg.TURBOVEC_DOCS_URL.rstrip("/")
    if not base_url:
        raise HTTPException(503, "TURBOVEC_DOCS_URL is not configured")

    payload: dict[str, Any] = {"force": body.force}
    if body.paths:
        payload["paths"] = body.paths

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(max(cfg.TURBOVEC_DOCS_TIMEOUT, 60.0))) as client:
            resp = await client.post(f"{base_url}/index/sync", json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "TurboVec Docs index sync timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"TurboVec Docs unavailable: {exc}") from exc

    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"TurboVec Docs index sync failed: {detail}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise HTTPException(502, "TurboVec Docs returned invalid JSON") from exc

    return {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "force": body.force,
        "upstream": data,
    }


# ── Group folder opener ───────────────────────────────────────────────────────

@router.post("/group-folder/open", response_model=dict)
async def open_docs_group_folder(body: DocsGroupFolderOpenBody) -> dict[str, Any]:
    """Open the most common parent folder for the docs currently in a group."""
    root = _docs_root()
    group_id = body.group_id or None
    rows = _docs_group_path_rows(group_id)
    folder, counts = _most_common_docs_folder(root, rows)

    _open_docs_folder(folder)
    return {
        "ok": True,
        "folder": str(folder),
        "relative_folder": _docs_rel_path(root, folder),
        "document_count": len(rows),
        "folder_document_count": counts[folder],
    }


@router.post("/group-folder/tree", response_model=dict)
async def docs_group_folder_tree(body: DocsGroupFolderTreeBody) -> dict[str, Any]:
    """Return a browser-renderable tree view for a docs group folder."""
    root = _docs_root()
    group_id = body.group_id or None
    rows = _docs_group_path_rows(group_id)

    requested_path = _normalize_docs_rel(body.path or "")
    if requested_path != ".":
        folder = _safe_resolve(root, requested_path)
        if not folder.exists():
            raise HTTPException(404, f"Folder does not exist: {requested_path}")
        if not folder.is_dir():
            raise HTTPException(400, f"Not a folder: {requested_path}")
        _, counts = _most_common_docs_folder(root, rows)
    else:
        folder, counts = _most_common_docs_folder(root, rows)

    relative_folder = _docs_rel_path(root, folder)
    docs_by_path = _registered_docs_tree_lookup()
    current_doc = docs_by_path.get(relative_folder.lower())

    entries: list[dict[str, Any]] = []
    try:
        children = list(folder.iterdir())
    except OSError as exc:
        raise HTTPException(500, f"Could not list folder: {exc}") from exc

    for child in children:
        child_rel = _docs_rel_path(root, child)
        child_doc = docs_by_path.get(child_rel.lower())
        try:
            is_dir = child.is_dir()
            is_file = child.is_file()
        except OSError:
            continue
        if not is_dir and not is_file:
            continue
        entries.append(
            {
                "name": child.name,
                "path": child_rel,
                "type": "folder" if is_dir else "file",
                "registered_doc": child_doc,
            }
        )

    entries.sort(key=lambda item: (0 if item["type"] == "folder" else 1, item["name"].lower()))

    breadcrumbs = [{"label": "docs root", "path": "."}]
    if relative_folder != ".":
        running: list[str] = []
        for part in Path(relative_folder).parts:
            running.append(part)
            breadcrumbs.append({"label": part, "path": "/".join(running)})

    parent_path = None
    if relative_folder != ".":
        parent = folder.parent
        if str(parent.resolve()).startswith(str(root.resolve())):
            parent_path = _docs_rel_path(root, parent)

    return {
        "ok": True,
        "folder": str(folder),
        "relative_folder": relative_folder,
        "parent_path": parent_path,
        "breadcrumbs": breadcrumbs,
        "entries": entries,
        "current_doc": current_doc,
        "document_count": len(rows),
        "folder_document_count": counts.get(folder, 0),
    }


# ── Get with content ──────────────────────────────────────────────────────────

@router.get("/{doc_id}", response_model=DocWithContent)
async def get_doc(doc_id: str) -> DocWithContent:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, "doc not found")
    out = DocWithContent(**_row_to_out(row).model_dump())
    p = _safe_resolve(_docs_root(), row["path"])
    if p.exists():
        try:
            out.content = p.read_text(encoding="utf-8")
            out.file_exists = True
        except Exception as exc:
            log.error("docs: failed to read %s: %s", p, exc)
    return out


# ── Create ────────────────────────────────────────────────────────────────────

@router.post("", response_model=DocOut, status_code=201)
async def create_doc(body: DocCreate) -> DocOut:
    doc_id = str(uuid.uuid4())
    root = _docs_root()
    p = _safe_resolve(root, body.path)
    p.parent.mkdir(parents=True, exist_ok=True)
    _normalize_ownership(root, p.parent)
    if not p.exists() or body.initial_content is not None:
        content = body.initial_content if body.initial_content is not None else f"# {body.label}\n"
        p.write_text(content, encoding="utf-8")
        _normalize_ownership(root, p)
        log.info("docs: created file %s", p)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO docs (doc_id, label, description, tags, path, sort_order, group_id) VALUES (?,?,?,?,?,?,?)",
            (doc_id, body.label, body.description, body.tags, body.path, body.sort_order, body.group_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "docs", doc_id, dict(row), gen)
    return _row_to_out(row)


# ── Update metadata ───────────────────────────────────────────────────────────

@router.put("/{doc_id}", response_model=DocOut)
async def update_doc(doc_id: str, body: DocUpdate) -> DocOut:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        conn.execute(
            """UPDATE docs SET
               label       = COALESCE(?, label),
               description = COALESCE(?, description),
               tags        = COALESCE(?, tags),
               path        = COALESCE(?, path),
               sort_order  = COALESCE(?, sort_order),
               group_id    = CASE WHEN ? IS NOT NULL THEN NULLIF(?, '') ELSE group_id END,
               updated_at  = datetime('now')
               WHERE doc_id = ?""",
            (body.label, body.description, body.tags, body.path, body.sort_order,
             body.group_id, body.group_id, doc_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "docs", doc_id, dict(row), gen)
    return _row_to_out(row)


# ── Update file content ───────────────────────────────────────────────────────

@router.put("/{doc_id}/content", status_code=204)
async def update_doc_content(doc_id: str, body: DocContentBody) -> Response:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        path_str = row["path"]
    root = _docs_root()
    p = _safe_resolve(root, path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    _normalize_ownership(root, p.parent)
    try:
        p.write_text(body.content, encoding="utf-8")
        _normalize_ownership(root, p)
        log.info("docs: wrote %d chars to %s", len(body.content), p)
        _touch_docs_sentinel()
    except HTTPException:
        raise
    except Exception as exc:
        log.error("docs: failed to write %s: %s", p, exc)
        raise HTTPException(500, f"Failed to write file: {exc}") from exc
    # Touch updated_at and sync metadata to peers
    with get_conn() as conn:
        conn.execute("UPDATE docs SET updated_at = datetime('now') WHERE doc_id = ?", (doc_id,))
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "docs", doc_id, dict(row), gen)
    return Response(status_code=204)


# ── Delete ────────────────────────────────────────────────────────────────────

@router.delete("/{doc_id}", status_code=204)
async def delete_doc(
    doc_id: str,
    delete_file: bool = Query(default=False, description="Also delete the file from disk"),
) -> Response:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        path_str = row["path"]
        conn.execute("DELETE FROM docs WHERE doc_id=?", (doc_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "docs", doc_id, {}, gen)
    if delete_file:
        root = _docs_root()
        p = _safe_resolve(root, path_str)
        if p.exists():
            try:
                p.unlink()
                log.info("docs: deleted file %s", p)
            except Exception as exc:
                log.warning("docs: failed to delete file %s: %s", p, exc)
    return Response(status_code=204)
