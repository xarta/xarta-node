"""routes_docs.py — CRUD for the docs table + file read/write.

GET    /api/v1/docs                      → list all doc metadata
GET    /api/v1/docs/{doc_id}             → metadata + file content
POST   /api/v1/docs                      → create doc record (creates file if not exists)
PUT    /api/v1/docs/{doc_id}             → update metadata only
PUT    /api/v1/docs/{doc_id}/content     → overwrite file content + touch updated_at
DELETE /api/v1/docs/{doc_id}            → delete record; ?delete_file=true also removes the file
"""

import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, increment_gen
from .models import DocContentBody, DocCreate, DocOut, DocUpdate, DocWithContent
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/docs", tags=["docs"])


def _inner_root() -> Path:
    inner = cfg.REPO_INNER_PATH
    if not inner:
        raise HTTPException(503, "REPO_INNER_PATH not configured — cannot locate docs")
    return Path(inner)


def _safe_resolve(root: Path, rel_path: str) -> Path:
    """Resolve rel_path under root, raising 400 if traversal escapes root.

    The docs/ subdirectory may be a symlink to an external path (e.g. lone-wolf).
    Fully-resolved paths are checked against both the repo root and the resolved
    docs symlink target so that symlinked subtrees are allowed.
    """
    resolved = (root / rel_path).resolve()
    root_resolved = str(root.resolve())

    def _under(base: str) -> bool:
        return str(resolved).startswith(base + "/") or str(resolved) == base

    if _under(root_resolved):
        return resolved

    # Allow paths that resolve under the docs symlink target (if docs/ is a symlink)
    docs_link = root / "docs"
    if docs_link.is_symlink():
        docs_target = str(docs_link.resolve())
        if _under(docs_target):
            return resolved

    raise HTTPException(400, "Path escapes repository root")


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
    """Return relative paths of .md files inside REPO_INNER_PATH not yet in the docs table."""
    root = _inner_root()
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


# ── Get with content ──────────────────────────────────────────────────────────

@router.get("/{doc_id}", response_model=DocWithContent)
async def get_doc(doc_id: str) -> DocWithContent:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, "doc not found")
    out = DocWithContent(**_row_to_out(row).model_dump())
    p = _safe_resolve(_inner_root(), row["path"])
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
    root = _inner_root()
    p = _safe_resolve(root, body.path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not p.exists() or body.initial_content is not None:
        content = body.initial_content if body.initial_content is not None else f"# {body.label}\n"
        p.write_text(content, encoding="utf-8")
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
    root = _inner_root()
    p = _safe_resolve(root, path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        p.write_text(body.content, encoding="utf-8")
        log.info("docs: wrote %d chars to %s", len(body.content), p)
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
        root = _inner_root()
        p = _safe_resolve(root, path_str)
        if p.exists():
            try:
                p.unlink()
                log.info("docs: deleted file %s", p)
            except Exception as exc:
                log.warning("docs: failed to delete file %s: %s", p, exc)
    return Response(status_code=204)
