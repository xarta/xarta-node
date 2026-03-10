"""routes_gui_sync.py — GUI zip push/pull between Blueprints nodes.

GET  /api/v1/sync/gui/export   — serve current GUI as a zip (for a peer to pull)
POST /api/v1/sync/gui/receive  — accept a GUI zip from a peer; extract to GUI_DIR
POST /api/v1/sync/gui/push     — operator action: push GUI to all registered peers

The GUI lives in the Docker volume at cfg.GUI_DIR (/data/gui).
Zip contents are relative to GUI_DIR root.
SHA-256 checksum is carried in X-Blueprints-Checksum header — same convention
as the DB sync endpoints in routes_sync.py.
"""

import hashlib
import io
import json
import logging
import zipfile
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Request, Response

from . import config as cfg
from .db import get_conn

log = logging.getLogger(__name__)

router = APIRouter(prefix="/sync/gui", tags=["gui-sync"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_gui_zip() -> tuple[bytes, str]:
    """Zip the entire GUI_DIR and return (zip_bytes, sha256_hex)."""
    gui_path = Path(cfg.GUI_DIR)
    if not gui_path.exists() or not any(gui_path.iterdir()):
        raise FileNotFoundError(
            f"GUI directory is empty or missing: {cfg.GUI_DIR}"
        )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(gui_path.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(gui_path))
    zip_bytes = buf.getvalue()
    return zip_bytes, hashlib.sha256(zip_bytes).hexdigest()


def _extract_gui_zip(zip_bytes: bytes) -> None:
    """Extract a GUI zip into GUI_DIR, replacing existing files."""
    gui_path = Path(cfg.GUI_DIR)
    gui_path.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        zf.extractall(gui_path)


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/export")
async def export_gui() -> Response:
    """Serve the current GUI as a zip so a peer can pull it."""
    try:
        zip_bytes, sha256_hex = _make_gui_zip()
    except FileNotFoundError as exc:
        raise HTTPException(503, str(exc))
    except Exception:
        log.exception("export_gui: failed to create gui zip")
        raise HTTPException(500, "failed to create gui zip")

    log.info("exporting GUI zip (%d bytes)", len(zip_bytes))
    return Response(
        content=zip_bytes,
        media_type="application/octet-stream",
        headers={"X-Blueprints-Checksum": sha256_hex},
    )


@router.post("/receive", status_code=204)
async def receive_gui(request: Request) -> Response:
    """Accept a GUI zip from a peer and extract it to GUI_DIR."""
    sha256_hex = request.headers.get("x-blueprints-checksum", "")
    if not sha256_hex:
        raise HTTPException(400, "missing X-Blueprints-Checksum header")

    zip_bytes = await request.body()
    if not zip_bytes:
        raise HTTPException(400, "empty gui zip payload")

    actual = hashlib.sha256(zip_bytes).hexdigest()
    if actual != sha256_hex:
        raise HTTPException(
            422, f"checksum mismatch: expected {sha256_hex}, got {actual}"
        )

    try:
        _extract_gui_zip(zip_bytes)
    except Exception:
        log.exception("receive_gui: failed to extract gui zip")
        raise HTTPException(500, "failed to extract gui zip")

    log.info("GUI zip received and extracted (%d bytes)", len(zip_bytes))
    return Response(status_code=204)


@router.post("/push")
async def push_gui_to_peers() -> dict:
    """
    Operator action: push the current GUI zip to all registered peers.
    Returns a push-result summary per peer.
    """
    try:
        zip_bytes, sha256_hex = _make_gui_zip()
    except FileNotFoundError as exc:
        raise HTTPException(503, str(exc))
    except Exception:
        log.exception("push_gui: failed to create gui zip")
        raise HTTPException(500, "failed to create gui zip")

    with get_conn() as conn:
        peer_rows = conn.execute(
            "SELECT node_id, display_name, addresses FROM nodes WHERE node_id != ?",
            (cfg.NODE_ID,),
        ).fetchall()

    if not peer_rows:
        return {
            "pushed": 0, "total_peers": 0,
            "peers": {}, "message": "no peers registered",
        }

    results: dict[str, str] = {}
    pushed = 0
    for row in peer_rows:
        addresses = json.loads(row["addresses"]) if row["addresses"] else []
        success = False
        last_err = "no addresses configured"
        for addr in addresses:
            addr = addr.rstrip("/")
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    resp = await client.post(
                        f"{addr}/api/v1/sync/gui/receive",
                        content=zip_bytes,
                        headers={
                            "content-type": "application/octet-stream",
                            "x-blueprints-checksum": sha256_hex,
                        },
                    )
                if resp.status_code == 204:
                    success = True
                    break
                last_err = f"HTTP {resp.status_code}"
            except Exception as exc:
                last_err = str(exc)

        node_name = row["display_name"]
        if success:
            pushed += 1
            results[row["node_id"]] = "ok"
            log.info("push_gui: ✓ sent to %s (%s)", node_name, row["node_id"])
        else:
            results[row["node_id"]] = f"failed: {last_err}"
            log.warning(
                "push_gui: ✗ failed to send to %s: %s", node_name, last_err
            )

    return {
        "pushed": pushed,
        "total_peers": len(peer_rows),
        "zip_bytes": len(zip_bytes),
        "peers": results,
    }
