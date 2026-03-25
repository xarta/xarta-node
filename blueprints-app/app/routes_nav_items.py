"""routes_nav_items.py — CMS-driven navigation items: CRUD, seed, and asset management.

GET    /api/v1/nav-items                    → list[NavItemOut]  (optional ?group= filter)
GET    /api/v1/nav-items/{item_id}          → NavItemOut
POST   /api/v1/nav-items                    → NavItemOut  (201)
PUT    /api/v1/nav-items/{item_id}          → NavItemOut
DELETE /api/v1/nav-items/{item_id}          → 204
POST   /api/v1/nav-items/seed               → {"inserted": N, "skipped": M}  (idempotent)
POST   /api/v1/nav-items/upload-asset       → {"path": "icons/foo.svg", "item_id": "..."}
GET    /api/v1/nav-items/assets             → list of asset files  (?type=icons|sounds)
POST   /api/v1/nav-items/assign-asset       → assign an existing asset to a nav item
POST   /api/v1/nav-items/upload-bulk        → bulk extract a zip/tar/7z archive into assets dir

Assets are stored under {REPO_OUTER_PATH}/gui-fallback/assets/{icons|sounds}/ and served
by Caddy at /fallback-ui/assets/{icons|sounds}/  — no extra server config required.

All data writes call enqueue_for_all_peers() for fleet sync.
Asset files are NOT git-tracked but are deployed via Syncthing.
7-zip support requires p7zip-full to be installed (setup-blueprints.sh handles this).
"""

import io
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
import uuid
import zipfile
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

try:
    from PIL import Image as _PILImage
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, increment_gen
from .sync.queue import enqueue_for_all_peers
from .models import NavItemCreate, NavItemUpdate, NavItemOut

log = logging.getLogger(__name__)

router = APIRouter(prefix="/nav-items", tags=["nav-items"])

_ICON_ALLOWED_EXTS  = {".svg", ".png", ".ico", ".jpg", ".jpeg", ".webp"}
_SOUND_ALLOWED_EXTS = {".wav", ".mp3", ".ogg", ".flac", ".webm", ".m4a"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _outer_root() -> Path:
    outer = cfg.REPO_OUTER_PATH
    if not outer:
        raise HTTPException(503, "REPO_OUTER_PATH not configured")
    return Path(outer)


def _assets_dir(asset_type: str) -> Path:
    """Return (and create) the icon or sound assets directory."""
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")
    d = _outer_root() / "gui-fallback" / "assets" / asset_type
    d.mkdir(parents=True, exist_ok=True)
    return d


def _row_to_out(row) -> NavItemOut:
    return NavItemOut(
        item_id=row["item_id"],
        menu_group=row["menu_group"],
        item_key=row["item_key"],
        label=row["label"],
        page_label=row["page_label"],
        icon_emoji=row["icon_emoji"],
        icon_asset=row["icon_asset"],
        sound_asset=row["sound_asset"],
        parent_key=row["parent_key"],
        sort_order=row["sort_order"],
        is_fn=row["is_fn"],
        fn_key=row["fn_key"],
        active_on=row["active_on"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_dict(row) -> dict:
    return {
        "item_id":    row["item_id"],
        "menu_group": row["menu_group"],
        "item_key":   row["item_key"],
        "label":      row["label"],
        "page_label": row["page_label"],
        "icon_emoji": row["icon_emoji"],
        "icon_asset": row["icon_asset"],
        "sound_asset": row["sound_asset"],
        "parent_key": row["parent_key"],
        "sort_order": row["sort_order"],
        "is_fn":      row["is_fn"],
        "fn_key":     row["fn_key"],
        "active_on":  row["active_on"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.get("", response_model=list[NavItemOut])
async def list_nav_items(group: str | None = Query(default=None)):
    with get_conn() as conn:
        if group:
            rows = conn.execute(
                "SELECT * FROM nav_items WHERE menu_group=? ORDER BY sort_order, item_key",
                (group,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM nav_items ORDER BY menu_group, sort_order, item_key"
            ).fetchall()
    return [_row_to_out(r) for r in rows]


# ── Asset listing endpoint (must be before /{item_id} to avoid wildcard capture) ──

@router.get("/assets")
async def list_assets(type: Literal["icons", "sounds"] = Query(...)):
    """List all uploaded asset files for a given type.

    Returns a list of objects: {filename, path, size, url}
    where 'path' is the relative path stored in nav_items (e.g. 'icons/foo.svg')
    and 'url' is the Caddy-served URL for browser preview.
    """
    assets_dir = _assets_dir(type)
    allowed = _ICON_ALLOWED_EXTS if type == "icons" else _SOUND_ALLOWED_EXTS
    result = []
    for f in sorted(assets_dir.rglob('*')):
        if f.is_file() and f.suffix.lower() in allowed:
            rel = f.relative_to(assets_dir)  # e.g. 'hieroglyphs/ankh-blue.svg'
            result.append({
                "filename": f.name,
                "path":     f"{type}/{rel}",
                "size":     f.stat().st_size,
                "url":      f"/fallback-ui/assets/{type}/{rel}",
            })
    return result


@router.get("/{item_id}", response_model=NavItemOut)
async def get_nav_item(item_id: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "nav item not found")
    return _row_to_out(row)


@router.post("", response_model=NavItemOut, status_code=201)
async def create_nav_item(body: NavItemCreate):
    item_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO nav_items
               (item_id, menu_group, item_key, label, page_label,
                icon_emoji, icon_asset, sound_asset, parent_key,
                sort_order, is_fn, fn_key, active_on)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (item_id, body.menu_group, body.item_key, body.label,
             body.page_label, body.icon_emoji, body.icon_asset,
             body.sound_asset, body.parent_key, body.sort_order,
             body.is_fn, body.fn_key, body.active_on),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "nav_items", item_id, _row_to_dict(row), gen)
    return _row_to_out(row)


@router.put("/{item_id}", response_model=NavItemOut)
async def update_nav_item(item_id: str, body: NavItemUpdate):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "nav item not found")
        conn.execute(
            """UPDATE nav_items SET
               label      = COALESCE(?, label),
               page_label = COALESCE(?, page_label),
               icon_emoji = COALESCE(?, icon_emoji),
               icon_asset = COALESCE(?, icon_asset),
               sound_asset = COALESCE(?, sound_asset),
               parent_key  = COALESCE(?, parent_key),
               sort_order  = COALESCE(?, sort_order),
               is_fn       = COALESCE(?, is_fn),
               fn_key      = COALESCE(?, fn_key),
               active_on   = COALESCE(?, active_on),
               updated_at  = datetime('now')
               WHERE item_id=?""",
            (body.label, body.page_label, body.icon_emoji,
             body.icon_asset, body.sound_asset, body.parent_key,
             body.sort_order, body.is_fn, body.fn_key, body.active_on,
             item_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "nav_items", item_id, _row_to_dict(row), gen)
    return _row_to_out(row)


@router.delete("/{item_id}", status_code=204)
async def delete_nav_item(item_id: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "nav item not found")
        conn.execute("DELETE FROM nav_items WHERE item_id=?", (item_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "nav_items", item_id, {}, gen)
    return Response(status_code=204)


# ── Seed endpoint ─────────────────────────────────────────────────────────────

@router.post("/seed")
async def seed_nav_items(items: list[NavItemCreate]):
    """Bulk-insert nav items from JS defaults. Idempotent — skips existing (menu_group, item_key) pairs."""
    inserted = 0
    skipped = 0
    with get_conn() as conn:
        for body in items:
            existing = conn.execute(
                "SELECT item_id FROM nav_items WHERE menu_group=? AND item_key=?",
                (body.menu_group, body.item_key),
            ).fetchone()
            if existing:
                skipped += 1
                continue
            item_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO nav_items
                   (item_id, menu_group, item_key, label, page_label,
                    icon_emoji, icon_asset, sound_asset, parent_key,
                    sort_order, is_fn, fn_key, active_on)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (item_id, body.menu_group, body.item_key, body.label,
                 body.page_label, body.icon_emoji, body.icon_asset,
                 body.sound_asset, body.parent_key, body.sort_order,
                 body.is_fn, body.fn_key, body.active_on),
            )
            gen = increment_gen(conn, "human")
            row = conn.execute(
                "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
            ).fetchone()
            enqueue_for_all_peers(conn, "INSERT", "nav_items", item_id, _row_to_dict(row), gen)
            inserted += 1
    log.info("nav_items seed: inserted=%d skipped=%d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


# ── Icon pre-processing ──────────────────────────────────────────────────────

def _normalize_icon(data: bytes) -> tuple[bytes, str]:
    """Convert any raster icon to white-on-transparent PNG for CSS mask-image tinting.

    Returns (png_bytes, '.png').  If Pillow is unavailable the original data
    is returned unchanged with its original bytes (caller keeps original ext).
    """
    if not _PIL_AVAILABLE:
        return data, None
    try:
        img = _PILImage.open(io.BytesIO(data))
        img = img.convert('RGBA')
        r, g, b, a = img.split()
        white = _PILImage.new('L', img.size, 255)
        result = _PILImage.merge('RGBA', (white, white, white, a))
        # Fit into 64×64 box while preserving aspect ratio
        result.thumbnail((64, 64), _PILImage.LANCZOS)
        out = io.BytesIO()
        result.save(out, 'PNG', optimize=True)
        return out.getvalue(), '.png'
    except Exception as exc:
        log.warning('icon normalize failed (%s) — storing original', exc)
        return data, None


# ── Asset upload endpoint ─────────────────────────────────────────────────────

@router.post("/upload-asset")
async def upload_nav_asset(
    file: UploadFile = File(...),
    item_id: str = Form(...),
    asset_type: str = Form(...),   # 'icons' or 'sounds'
):
    """Upload an icon or sound asset, save to gui-fallback/assets/{type}/, update nav_items row."""
    # Validate asset_type
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")

    # Validate extension
    original_name = file.filename or "upload"
    ext = Path(original_name).suffix.lower()
    allowed = _ICON_ALLOWED_EXTS if asset_type == "icons" else _SOUND_ALLOWED_EXTS
    if ext not in allowed:
        raise HTTPException(
            400,
            f"file type {ext!r} not allowed for {asset_type}; "
            f"permitted: {', '.join(sorted(allowed))}",
        )

    # Sanitise filename
    safe_name = Path(original_name).name.replace(" ", "_")
    if not safe_name or safe_name.startswith(".") or "/" in safe_name or "\\" in safe_name:
        raise HTTPException(400, "invalid filename")

    content = await file.read()

    # Pre-process icon uploads (non-SVG): normalise to white-on-transparent PNG
    # so CSS mask-image + background-color:currentColor can tint them to match
    # the active/hover font colour with no additional state-specific assets.
    if asset_type == "icons" and ext not in (".svg",):
        normalised, new_ext = _normalize_icon(content)
        if new_ext:
            content = normalised
            stem = Path(safe_name).stem
            safe_name = stem + new_ext

    assets_dir = _assets_dir(asset_type)
    dest = assets_dir / safe_name

    # Auto-rename on collision
    if dest.exists():
        stem = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        safe_name = f"{stem}_{uuid.uuid4().hex[:8]}{suffix}"
        dest = assets_dir / safe_name

    try:
        dest.write_bytes(content)
        log.info("nav_items: saved asset %s (%d bytes)", dest, len(content))
    except Exception as exc:
        log.error("nav_items: failed to write asset %s: %s", dest, exc)
        raise HTTPException(500, f"Failed to save file: {exc}") from exc

    # Relative path stored in DB (relative to assets/)
    relative_path = f"{asset_type}/{safe_name}"
    asset_col = "icon_asset" if asset_type == "icons" else "sound_asset"

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        if not row:
            # File saved but row not found — not an error, caller may create row later
            log.warning("nav_items: item_id %s not found after upload", item_id)
            return {"path": relative_path, "item_id": item_id, "warning": "item not found in DB"}
        conn.execute(
            f"UPDATE nav_items SET {asset_col}=?, updated_at=datetime('now') WHERE item_id=?",
            (relative_path, item_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "nav_items", item_id, _row_to_dict(row), gen)

    return {"path": relative_path, "item_id": item_id}


# ── Assign existing asset endpoint ────────────────────────────────────────────

@router.post("/assign-asset")
async def assign_asset(
    item_id: str = Form(...),
    asset_path: str = Form(...),   # e.g. "icons/foo.svg" or "sounds/blip.wav"
    asset_type: str = Form(...),   # "icons" or "sounds"
):
    """Assign an already-uploaded asset file to a nav item (no file upload needed)."""
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")

    # Validate the path refers to an actually existing file within our assets dir
    assets_dir = _assets_dir(asset_type)
    # asset_path is relative to assets/ (e.g. "icons/foo.svg" or "icons/hieroglyphs/ankh-blue.svg")
    # Strip the type prefix to get the path within the assets_dir
    prefix = f"{asset_type}/"
    if asset_path.startswith(prefix):
        rel_within = asset_path[len(prefix):]
    else:
        rel_within = Path(asset_path).name
    candidate = assets_dir / rel_within

    # Prevent path traversal
    try:
        candidate.resolve().relative_to(assets_dir.resolve())
    except ValueError:
        raise HTTPException(400, "invalid asset path")

    if not candidate.exists() or not candidate.is_file():
        raise HTTPException(404, f"Asset file not found: {asset_path!r}")

    relative_path = f"{asset_type}/{rel_within}"
    asset_col = "icon_asset" if asset_type == "icons" else "sound_asset"

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "nav item not found")
        conn.execute(
            f"UPDATE nav_items SET {asset_col}=?, updated_at=datetime('now') WHERE item_id=?",
            (relative_path, item_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM nav_items WHERE item_id=?", (item_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "nav_items", item_id, _row_to_dict(row), gen)

    return {"path": relative_path, "item_id": item_id}


# ── Bulk archive upload endpoint ──────────────────────────────────────────────

_BULK_ARCHIVE_EXTS = {".zip", ".7z", ".tar", ".gz", ".bz2", ".xz", ".tgz", ".tbz2", ".txz"}


def _safe_extract_zip(archive_path: Path, dest: Path, allowed_exts: set) -> list[str]:
    """Extract a .zip file, skipping unsafe paths and non-allowed extensions. Returns list of extracted filenames."""
    extracted = []
    dest_resolved = dest.resolve()
    with zipfile.ZipFile(archive_path) as zf:
        for member in zf.infolist():
            # Skip directories and macOS metadata
            if member.is_dir() or member.filename.startswith("__MACOSX"):
                continue
            fname = Path(member.filename).name  # strip any directory components
            if not fname or fname.startswith("."):
                continue
            if Path(fname).suffix.lower() not in allowed_exts:
                continue
            dest_file = dest / fname
            # Zip-slip guard
            if not dest_file.resolve().is_relative_to(dest_resolved):
                log.warning("bulk upload: rejected zip-slip attempt: %s", member.filename)
                continue
            # Auto-rename on collision
            if dest_file.exists():
                stem, suffix = Path(fname).stem, Path(fname).suffix
                fname = f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
                dest_file = dest / fname
            with zf.open(member) as src, open(dest_file, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted.append(fname)
    return extracted


def _safe_extract_tar(archive_path: Path, dest: Path, allowed_exts: set) -> list[str]:
    """Extract a .tar.* file safely. Returns list of extracted filenames."""
    extracted = []
    dest_resolved = dest.resolve()
    with tarfile.open(archive_path) as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            fname = Path(member.name).name
            if not fname or fname.startswith(".") or fname.startswith("__MACOSX"):
                continue
            if Path(fname).suffix.lower() not in allowed_exts:
                continue
            dest_file = dest / fname
            if not dest_file.resolve().is_relative_to(dest_resolved):
                log.warning("bulk upload: rejected tar-slip attempt: %s", member.name)
                continue
            if dest_file.exists():
                stem, suffix = Path(fname).stem, Path(fname).suffix
                fname = f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
                dest_file = dest / fname
            member.name = fname  # flatten path for extraction
            tf.extract(member, dest, filter="data")
            extracted.append(fname)
    return extracted


def _safe_extract_7z(archive_path: Path, dest: Path, allowed_exts: set) -> list[str]:
    """Extract a .7z file using the 7z binary (p7zip-full). Returns list of extracted filenames."""
    sevenzip = shutil.which("7z") or shutil.which("7za")
    if not sevenzip:
        raise HTTPException(503, "7-zip not installed on this node; run setup-blueprints.sh to install p7zip-full")
    with tempfile.TemporaryDirectory() as tmpdir:
        result = subprocess.run(
            [sevenzip, "x", "-y", f"-o{tmpdir}", str(archive_path)],
            capture_output=True, text=True, timeout=60,
        )
        if result.returncode != 0:
            raise HTTPException(400, f"7z extraction failed: {result.stderr[:300]}")
        extracted = []
        dest_resolved = dest.resolve()
        for root, _, files in os.walk(tmpdir):
            for fname in files:
                if fname.startswith(".") or fname.startswith("__MACOSX"):
                    continue
                if Path(fname).suffix.lower() not in allowed_exts:
                    continue
                src_path = Path(root) / fname
                dest_file = dest / fname
                if not dest_file.resolve().is_relative_to(dest_resolved):
                    continue
                if dest_file.exists():
                    stem, suffix = Path(fname).stem, Path(fname).suffix
                    fname = f"{stem}_{uuid.uuid4().hex[:6]}{suffix}"
                    dest_file = dest / fname
                shutil.copy2(src_path, dest_file)
                extracted.append(fname)
    return extracted


@router.post("/upload-bulk")
async def upload_bulk_assets(
    file: UploadFile = File(...),
    asset_type: str = Form(...),   # "icons" or "sounds"
):
    """Upload a zip / tar.* / 7z archive and extract matching asset files into the assets directory.

    Only files with allowed extensions for the given asset_type are extracted.
    All paths are flattened (subdirectories inside the archive are ignored).
    Returns {"extracted": [...filenames...], "skipped": N}.
    """
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")

    original_name = file.filename or "archive"
    ext = "".join(Path(original_name).suffixes).lower()  # e.g. ".tar.gz"
    # Also check just the last suffix
    last_ext = Path(original_name).suffix.lower()

    if last_ext not in _BULK_ARCHIVE_EXTS:
        raise HTTPException(
            400,
            f"unsupported archive format {last_ext!r}; "
            f"supported: {', '.join(sorted(_BULK_ARCHIVE_EXTS))}",
        )

    allowed = _ICON_ALLOWED_EXTS if asset_type == "icons" else _SOUND_ALLOWED_EXTS
    assets_dir = _assets_dir(asset_type)

    # Write upload to a temp file for processing
    with tempfile.NamedTemporaryFile(suffix=last_ext, delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = Path(tmp.name)

    try:
        if last_ext == ".zip":
            extracted = _safe_extract_zip(tmp_path, assets_dir, allowed)
        elif last_ext == ".7z":
            extracted = _safe_extract_7z(tmp_path, assets_dir, allowed)
        else:
            # .tar, .gz, .bz2, .xz, .tgz, .tbz2, .txz
            extracted = _safe_extract_tar(tmp_path, assets_dir, allowed)
    except HTTPException:
        raise
    except Exception as exc:
        log.error("bulk upload: extraction failed: %s", exc)
        raise HTTPException(500, f"extraction failed: {exc}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)

    log.info("bulk upload: extracted %d files into %s", len(extracted), assets_dir)
    return {"extracted": extracted, "count": len(extracted)}
