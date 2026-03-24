"""routes_form_controls.py — CMS-driven form control sound/icon assignments.

GET    /api/v1/form-controls                   → list[FormControlOut]
GET    /api/v1/form-controls/assets            → list asset files (?type=icons|sounds)
GET    /api/v1/form-controls/{control_id}      → FormControlOut
POST   /api/v1/form-controls                   → FormControlOut  (201)
PUT    /api/v1/form-controls/{control_id}      → FormControlOut
DELETE /api/v1/form-controls/{control_id}      → 204
POST   /api/v1/form-controls/upload-asset      → {"path": "sounds/click.wav"}  (no row update)
POST   /api/v1/form-controls/assign-asset      → assign existing asset to a control
POST   /api/v1/form-controls/bulk-seed         → idempotent seed from JSON list

Assets live in the shared gui-fallback/assets/{icons|sounds}/ folder alongside nav_items assets.
Caddy serves them at /fallback-ui/assets/{icons|sounds}/filename.

All data writes call enqueue_for_all_peers() for fleet sync.
"""

import logging
import uuid
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, increment_gen
from .sync.queue import enqueue_for_all_peers
from .models import FormControlCreate, FormControlUpdate, FormControlOut

log = logging.getLogger(__name__)

router = APIRouter(prefix="/form-controls", tags=["form-controls"])

_ICON_ALLOWED_EXTS  = {".svg", ".png", ".ico", ".jpg", ".jpeg", ".webp"}
_SOUND_ALLOWED_EXTS = {".wav", ".mp3", ".ogg", ".flac", ".webm", ".m4a"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _outer_root() -> Path:
    outer = cfg.REPO_OUTER_PATH
    if not outer:
        raise HTTPException(503, "REPO_OUTER_PATH not configured")
    return Path(outer)


def _assets_dir(asset_type: str) -> Path:
    """Return (and create) the shared icon or sound assets directory."""
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")
    d = _outer_root() / "gui-fallback" / "assets" / asset_type
    d.mkdir(parents=True, exist_ok=True)
    return d


def _row_to_out(row) -> FormControlOut:
    return FormControlOut(
        control_id=row["control_id"],
        control_key=row["control_key"],
        label=row["label"],
        control_type=row["control_type"],
        context=row["context"],
        icon_asset=row["icon_asset"],
        sound_asset=row["sound_asset"],
        sound_asset_off=row["sound_asset_off"],
        notes=row["notes"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _row_to_dict(row) -> dict:
    return {
        "control_id":    row["control_id"],
        "control_key":   row["control_key"],
        "label":         row["label"],
        "control_type":  row["control_type"],
        "context":       row["context"],
        "icon_asset":    row["icon_asset"],
        "sound_asset":   row["sound_asset"],
        "sound_asset_off": row["sound_asset_off"],
        "notes":         row["notes"],
        "created_at":    row["created_at"],
        "updated_at":    row["updated_at"],
    }


# ── Asset listing (must be before /{control_id} to avoid wildcard capture) ───

@router.get("/assets")
async def list_fc_assets(type: Literal["icons", "sounds"] = Query(...)):
    """List all uploaded asset files for a given type.

    Returns list of {filename, path, size, url} — shared asset pool with nav_items.
    """
    assets_dir = _assets_dir(type)
    allowed = _ICON_ALLOWED_EXTS if type == "icons" else _SOUND_ALLOWED_EXTS
    result = []
    for f in sorted(assets_dir.iterdir()):
        if f.is_file() and f.suffix.lower() in allowed:
            result.append({
                "filename": f.name,
                "path":     f"{type}/{f.name}",
                "size":     f.stat().st_size,
                "url":      f"/fallback-ui/assets/{type}/{f.name}",
            })
    return result


# ── Bulk seed ─────────────────────────────────────────────────────────────────

@router.post("/bulk-seed")
async def bulk_seed_form_controls(items: list[FormControlCreate]):
    """Idempotent bulk-insert. Skips existing control_key entries."""
    inserted = 0
    skipped  = 0
    with get_conn() as conn:
        for body in items:
            existing = conn.execute(
                "SELECT control_id FROM form_controls WHERE control_key=?",
                (body.control_key,),
            ).fetchone()
            if existing:
                skipped += 1
                continue
            control_id = str(uuid.uuid4())
            conn.execute(
                """INSERT INTO form_controls
                   (control_id, control_key, label, control_type, context, icon_asset, sound_asset, sound_asset_off, notes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (control_id, body.control_key, body.label,
                 body.control_type, body.context,
                 body.icon_asset, body.sound_asset, body.sound_asset_off, body.notes),
            )
            gen = increment_gen(conn, "human")
            row = conn.execute(
                "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
            ).fetchone()
            enqueue_for_all_peers(conn, "INSERT", "form_controls", control_id, _row_to_dict(row), gen)
            inserted += 1
    log.info("form_controls bulk-seed: inserted=%d skipped=%d", inserted, skipped)
    return {"inserted": inserted, "skipped": skipped}


# ── Upload-asset (saves file, returns path — does NOT auto-update a row) ─────

@router.post("/upload-asset")
async def upload_fc_asset(
    file: UploadFile = File(...),
    asset_type: str  = Form(...),   # 'icons' or 'sounds'
):
    """Upload an icon or sound asset to the shared assets folder.

    Returns {"path": "sounds/click.wav"} — caller uses PUT to assign to a control.
    """
    if asset_type not in ("icons", "sounds"):
        raise HTTPException(400, "asset_type must be 'icons' or 'sounds'")

    original_name = file.filename or "upload"
    ext = Path(original_name).suffix.lower()
    allowed = _ICON_ALLOWED_EXTS if asset_type == "icons" else _SOUND_ALLOWED_EXTS
    if ext not in allowed:
        raise HTTPException(
            400,
            f"file type {ext!r} not allowed for {asset_type}; "
            f"permitted: {', '.join(sorted(allowed))}",
        )

    safe_name = Path(original_name).name.replace(" ", "_")
    if not safe_name or safe_name.startswith(".") or "/" in safe_name or "\\" in safe_name:
        raise HTTPException(400, "invalid filename")

    content = await file.read()
    assets_dir = _assets_dir(asset_type)
    dest = assets_dir / safe_name

    # Auto-rename on collision
    if dest.exists():
        stem   = Path(safe_name).stem
        suffix = Path(safe_name).suffix
        safe_name = f"{stem}_{uuid.uuid4().hex[:8]}{suffix}"
        dest = assets_dir / safe_name

    try:
        dest.write_bytes(content)
        log.info("form_controls: saved asset %s (%d bytes)", dest, len(content))
    except Exception as exc:
        log.error("form_controls: failed to write asset %s: %s", dest, exc)
        raise HTTPException(500, f"Failed to save file: {exc}") from exc

    return {"path": f"{asset_type}/{safe_name}"}


# ── Assign existing asset ─────────────────────────────────────────────────────

@router.post("/assign-asset")
async def assign_fc_asset(
    control_id: str = Form(...),
    asset_path: str = Form(...),   # e.g. "icons/foo.svg" or "sounds/blip.wav"
    asset_type: str = Form(...),   # "icons" or "sounds"
):
    """Assign an already-uploaded asset to a form control row."""
    if asset_type not in ("icons", "sounds", "sounds_off"):
        raise HTTPException(400, "asset_type must be 'icons', 'sounds', or 'sounds_off'")

    # For file lookup, map sounds_off → sounds folder
    folder_type = "icons" if asset_type == "icons" else "sounds"
    assets_dir = _assets_dir(folder_type)
    filename = Path(asset_path).name
    dest = assets_dir / filename
    if not dest.exists():
        raise HTTPException(404, f"asset file not found: {asset_path!r}")

    relative_path = f"{folder_type}/{filename}"
    asset_col = {"icons": "icon_asset", "sounds": "sound_asset", "sounds_off": "sound_asset_off"}[asset_type]

    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "form control not found")
        conn.execute(
            f"UPDATE form_controls SET {asset_col}=?, updated_at=datetime('now') WHERE control_id=?",
            (relative_path, control_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "form_controls", control_id, _row_to_dict(row), gen)

    return {"path": relative_path, "control_id": control_id}


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.get("", response_model=list[FormControlOut])
async def list_form_controls():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM form_controls ORDER BY label, control_key"
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.get("/{control_id}", response_model=FormControlOut)
async def get_form_control(control_id: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "form control not found")
    return _row_to_out(row)


@router.post("", response_model=FormControlOut, status_code=201)
async def create_form_control(body: FormControlCreate):
    control_id = str(uuid.uuid4())
    with get_conn() as conn:
        try:
            conn.execute(
                """INSERT INTO form_controls
                   (control_id, control_key, label, control_type, context, icon_asset, sound_asset, sound_asset_off, notes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (control_id, body.control_key, body.label,
                 body.control_type, body.context,
                 body.icon_asset, body.sound_asset, body.sound_asset_off, body.notes),
            )
        except Exception as exc:
            if "UNIQUE" in str(exc):
                raise HTTPException(409, f"control_key {body.control_key!r} already exists") from exc
            raise
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "form_controls", control_id, _row_to_dict(row), gen)
    return _row_to_out(row)


@router.put("/{control_id}", response_model=FormControlOut)
async def update_form_control(control_id: str, body: FormControlUpdate):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "form control not found")
        conn.execute(
            """UPDATE form_controls SET
               label           = COALESCE(?, label),
               control_type    = COALESCE(?, control_type),
               context         = COALESCE(?, context),
               icon_asset      = COALESCE(?, icon_asset),
               sound_asset     = COALESCE(?, sound_asset),
               sound_asset_off = COALESCE(?, sound_asset_off),
               notes           = COALESCE(?, notes),
               updated_at      = datetime('now')
               WHERE control_id=?""",
            (body.label, body.control_type, body.context,
             body.icon_asset, body.sound_asset, body.sound_asset_off, body.notes,
             control_id),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "form_controls", control_id, _row_to_dict(row), gen)
    return _row_to_out(row)


@router.delete("/{control_id}", status_code=204)
async def delete_form_control(control_id: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM form_controls WHERE control_id=?", (control_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "form control not found")
        conn.execute("DELETE FROM form_controls WHERE control_id=?", (control_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "form_controls", control_id, {}, gen)
    return Response(status_code=204)
