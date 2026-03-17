"""routes_doc_images.py — Upload, serve, and manage image files for docs.

GET    /api/v1/doc-images               → list[DocImageOut]; ?unused=true filters unreferenced
GET    /api/v1/doc-images/{id}/file     → serve image binary (auth-gated)
POST   /api/v1/doc-images               → upload image (multipart), create record
PUT    /api/v1/doc-images/{id}          → update description
DELETE /api/v1/doc-images/{id}          → delete record; ?delete_file=true also removes file

Images are stored under {REPO_INNER_PATH}/docs/images/ and distributed to the
fleet via fleet-pull-private (git). The DB records sync via the standard
gen-based peer protocol so all nodes know what images exist.
"""

import json
import logging
import uuid
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from starlette.responses import FileResponse, Response

from . import config as cfg
from .db import get_conn, increment_gen
from .models import DocImageOut, DocImageUpdate
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/doc-images", tags=["doc-images"])

_ALLOWED_EXTS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
_IMAGES_SUBDIR = "docs/images"   # relative to REPO_INNER_PATH


# ── Helpers ───────────────────────────────────────────────────────────────────

def _inner_root() -> Path:
    inner = cfg.REPO_INNER_PATH
    if not inner:
        raise HTTPException(503, "REPO_INNER_PATH not configured")
    return Path(inner)


def _images_dir() -> Path:
    d = _inner_root() / _IMAGES_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _row_to_out(row) -> DocImageOut:
    raw_tags = row["tags"]
    tags: list[str] | None = None
    if raw_tags:
        try:
            tags = json.loads(raw_tags)
        except Exception:
            tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    return DocImageOut(
        image_id=row["image_id"],
        filename=row["filename"],
        description=row["description"],
        tags=tags,
        file_size=row["file_size"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _get_used_image_ids(inner_root: Path) -> set[str]:
    """Return the set of image_ids that appear in any doc file content."""
    with get_conn() as conn:
        doc_rows = conn.execute("SELECT path FROM docs").fetchall()
        img_rows = conn.execute("SELECT image_id FROM doc_images").fetchall()
    all_ids = {r["image_id"] for r in img_rows}
    if not doc_rows or not all_ids:
        return set()
    used: set[str] = set()
    root_str = str(inner_root.resolve()) + "/"
    for row in doc_rows:
        p = (inner_root / row["path"]).resolve()
        if not str(p).startswith(root_str):
            continue  # path traversal guard
        if p.exists():
            try:
                content = p.read_text(encoding="utf-8", errors="replace")
                for img_id in all_ids:
                    if img_id in content:
                        used.add(img_id)
            except Exception:
                pass
    return used


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=list[DocImageOut])
async def list_doc_images(
    unused: bool = Query(default=False, description="Return only images not referenced in any doc"),
) -> list[DocImageOut]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM doc_images ORDER BY created_at DESC"
        ).fetchall()
    results = [_row_to_out(r) for r in rows]
    if unused:
        root = _inner_root()
        used = _get_used_image_ids(root)
        results = [r for r in results if r.image_id not in used]
    return results


@router.get("/{image_id}/file")
async def serve_doc_image(image_id: str) -> FileResponse:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM doc_images WHERE image_id=?", (image_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "image not found")
    p = _images_dir() / row["filename"]
    if not p.exists():
        raise HTTPException(404, "image file not present on this node (may appear after fleet-pull-private)")
    return FileResponse(str(p))


@router.post("", response_model=DocImageOut, status_code=201)
async def upload_doc_image(
    file: UploadFile = File(...),
    description: str = Form(default=""),
) -> DocImageOut:
    # Validate extension
    original_name = file.filename or "upload"
    ext = Path(original_name).suffix.lower()
    if ext not in _ALLOWED_EXTS:
        raise HTTPException(
            400,
            f"file type {ext!r} not allowed; permitted: {', '.join(sorted(_ALLOWED_EXTS))}",
        )

    # Sanitise filename — strip any path components, no dots at start
    safe_name = Path(original_name).name.replace(" ", "_")
    if not safe_name or safe_name.startswith(".") or "/" in safe_name or "\\" in safe_name:
        raise HTTPException(400, "invalid filename")

    images_dir = _images_dir()
    dest = images_dir / safe_name

    # Auto-rename on collision
    if dest.exists():
        stem = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        safe_name = f"{stem}_{uuid.uuid4().hex[:8]}{suffix}"
        dest = images_dir / safe_name

    content = await file.read()
    file_size = len(content)

    try:
        dest.write_bytes(content)
        log.info("doc_images: saved %s (%d bytes)", dest, file_size)
    except Exception as exc:
        log.error("doc_images: failed to write %s: %s", dest, exc)
        raise HTTPException(500, f"Failed to save file: {exc}") from exc

    image_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO doc_images (image_id, filename, description, file_size) VALUES (?,?,?,?)",
            (image_id, safe_name, description.strip() or None, file_size),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM doc_images WHERE image_id=?", (image_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "doc_images", image_id, dict(row), gen)
    return _row_to_out(row)


@router.put("/{image_id}", response_model=DocImageOut)
async def update_doc_image(image_id: str, body: DocImageUpdate) -> DocImageOut:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM doc_images WHERE image_id=?", (image_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "image not found")
        updates: list[str] = ["updated_at=datetime('now')"]
        params: list = []
        if body.description is not None:
            updates.append("description=?")
            params.append(body.description)
        if body.tags is not None:
            updates.append("tags=?")
            params.append(json.dumps(body.tags) if body.tags else None)
        params.append(image_id)
        conn.execute(
            f"UPDATE doc_images SET {', '.join(updates)} WHERE image_id=?",
            params,
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM doc_images WHERE image_id=?", (image_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "doc_images", image_id, dict(row), gen)
    return _row_to_out(row)


@router.delete("/{image_id}", status_code=204)
async def delete_doc_image(
    image_id: str,
    delete_file: bool = Query(default=False, description="Also delete the file from disk"),
) -> Response:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM doc_images WHERE image_id=?", (image_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "image not found")
        filename = row["filename"]
        conn.execute("DELETE FROM doc_images WHERE image_id=?", (image_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "doc_images", image_id, {}, gen)
    if delete_file:
        p = _images_dir() / filename
        if p.exists():
            try:
                p.unlink()
                log.info("doc_images: deleted file %s", p)
            except Exception as exc:
                log.warning("doc_images: failed to delete file %s: %s", p, exc)
    return Response(status_code=204)
