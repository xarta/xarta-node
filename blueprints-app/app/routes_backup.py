"""
routes_backup.py — local DB backup and restore endpoints.

GET  /api/v1/backup                          — list available backups
POST /api/v1/backup                          — create a new backup
POST /api/v1/backup/restore/{filename}       — restore a backup locally
    ?force=true                                — restore locally, then broadcast the
                                                                                             restored DB to all peers via the
                                                                                             full-restore endpoint

Backups are plain SQLite files (not zipped) saved to BLUEPRINTS_BACKUP_DIR.
The sync_queue table is always cleared in backups — there is no point
preserving stale outbound queue items from backup time.
"""

import logging
import os
import re
import sqlite3
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import Response

from . import config as cfg
from .auth import compute_token
from .db import get_conn, get_gen
from .sync.drain import _make_sync_client
from .sync.restore import make_full_backup, post_restore_housekeeping, validate_sqlite_file

log = logging.getLogger(__name__)

router = APIRouter(prefix="/backup", tags=["backup"])

_SAFE_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{6}-blueprints\.db\.tar\.gz$")
_FORCE_RESTORE_HEADER = "x-blueprints-force-restore"
_RESTORE_OP_HEADER = "x-blueprints-restore-op"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _backup_dir() -> Path:
    """Return the resolved backup directory, or raise 503 if not configured."""
    d = cfg.BACKUP_DIR
    if not d:
        raise HTTPException(
            status_code=503,
            detail="BLUEPRINTS_BACKUP_DIR is not configured on this node.",
        )
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _create_backup_file(dest_path: Path) -> None:
    """
    Clone the live DB via sqlite3 backup API, strip sync_queue, then
    compress to dest_path as a .db.tar.gz archive.  The intermediate raw
    DB is written to a temp file and deleted afterwards.
    """
    db_dir = dest_path.parent
    with tempfile.NamedTemporaryFile(
        dir=db_dir, suffix=".db.tmp", delete=False
    ) as tmp:
        tmp_db = Path(tmp.name)

    try:
        src = sqlite3.connect(cfg.DB_PATH)
        dst = sqlite3.connect(str(tmp_db))
        try:
            src.backup(dst)
        finally:
            src.close()
            dst.close()

        # Strip sync_queue from the backup copy
        conn = sqlite3.connect(str(tmp_db))
        try:
            conn.execute("DELETE FROM sync_queue")
            conn.commit()
        finally:
            conn.close()

        # Compress to dest_path
        with tarfile.open(str(dest_path), "w:gz") as tar:
            tar.add(str(tmp_db), arcname="blueprints.db")
    finally:
        try:
            os.unlink(str(tmp_db))
        except OSError:
            pass


def _extract_backup_to_temp(src_path: Path, tmp_path: Path) -> int:
    """Extract a backup archive to a temp DB file and return its generation."""
    with tarfile.open(str(src_path), "r:gz") as tar:
        member = tar.getmember("blueprints.db")
        with tar.extractfile(member) as f_in, open(str(tmp_path), "wb") as f_out:
            f_out.write(f_in.read())

    conn_tmp = sqlite3.connect(str(tmp_path))
    try:
        conn_tmp.execute("DELETE FROM sync_queue")
        conn_tmp.commit()
        row = conn_tmp.execute(
            "SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'"
        ).fetchone()
        return int(row[0]) if row else 0
    finally:
        conn_tmp.close()


class PeerRestoreResult(BaseModel):
    node_id: str
    ok: bool
    address: str | None = None
    detail: str = ""


async def _broadcast_live_db_to_peers(operation_id: str) -> list[PeerRestoreResult]:
    """Send the current local DB to every configured peer as an authoritative restore."""
    with get_conn() as conn:
        current_gen = get_gen(conn)

    zip_bytes, sha256_hex = make_full_backup()
    headers = {
        "content-type": "application/octet-stream",
        "x-blueprints-checksum": sha256_hex,
        "x-blueprints-gen": str(current_gen),
        _FORCE_RESTORE_HEADER: "true",
        _RESTORE_OP_HEADER: operation_id,
    }
    if cfg.SYNC_SECRET:
        headers["x-api-token"] = compute_token(cfg.SYNC_SECRET)

    results: list[PeerRestoreResult] = []
    async with _make_sync_client(60.0) as client:
        for node_id, peer_urls in cfg.PEER_SYNC_URLS.items():
            if not peer_urls:
                results.append(PeerRestoreResult(
                    node_id=node_id,
                    ok=False,
                    detail="no sync addresses configured",
                ))
                continue

            last_detail = "all configured addresses failed"
            last_address: str | None = None
            success = False

            for url in peer_urls:
                last_address = url
                try:
                    resp = await client.post(
                        f"{url}/api/v1/sync/restore",
                        content=zip_bytes,
                        headers=headers,
                    )
                    if resp.status_code == 204:
                        results.append(PeerRestoreResult(
                            node_id=node_id,
                            ok=True,
                            address=url,
                            detail="restore applied",
                        ))
                        success = True
                        break
                    body = resp.text.strip()
                    last_detail = f"HTTP {resp.status_code}"
                    if body:
                        last_detail = f"{last_detail} — {body}"
                except httpx.ConnectError:
                    last_detail = f"connect failed at {url}"
                except Exception as exc:
                    last_detail = f"{type(exc).__name__}: {exc}"

            if not success:
                results.append(PeerRestoreResult(
                    node_id=node_id,
                    ok=False,
                    address=last_address,
                    detail=last_detail,
                ))

    return results


# ── Models ────────────────────────────────────────────────────────────────────

class BackupEntry(BaseModel):
    filename: str
    size_bytes: int
    created_at: str  # ISO-8601 UTC


class BackupListResponse(BaseModel):
    backups: list[BackupEntry]
    backup_dir: str


class BackupCreatedResponse(BaseModel):
    filename: str
    size_bytes: int
    created_at: str


class RestoreResponse(BaseModel):
    restored_from: str
    force: bool
    gen_before: int
    gen_after: int
    warning: str | None = None
    fleet_success: bool | None = None
    peer_results: list[PeerRestoreResult] = Field(default_factory=list)


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("", response_model=BackupListResponse)
def list_backups() -> BackupListResponse:
    """List all available local backups, newest first."""
    bdir = _backup_dir()
    entries: list[BackupEntry] = []
    for p in sorted(bdir.glob("*-blueprints.db.tar.gz"), reverse=True):
        if not _SAFE_NAME.match(p.name):
            continue
        stat = p.stat()
        entries.append(BackupEntry(
            filename=p.name,
            size_bytes=stat.st_size,
            created_at=datetime.fromtimestamp(
                stat.st_mtime, tz=timezone.utc
            ).isoformat(),
        ))
    return BackupListResponse(backups=entries, backup_dir=str(bdir))


@router.post("", response_model=BackupCreatedResponse, status_code=201)
def create_backup() -> BackupCreatedResponse:
    """Create a timestamped backup of the current DB (sync_queue excluded)."""
    bdir = _backup_dir()
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    filename = f"{ts}-blueprints.db.tar.gz"
    dest = bdir / filename

    _create_backup_file(dest)

    stat = dest.stat()
    log.info("backup created: %s (%d bytes)", filename, stat.st_size)
    return BackupCreatedResponse(
        filename=filename,
        size_bytes=stat.st_size,
        created_at=datetime.fromtimestamp(
            stat.st_mtime, tz=timezone.utc
        ).isoformat(),
    )


@router.post("/restore/{filename}", response_model=RestoreResponse)
async def restore_backup(
    filename: str,
    force: bool = Query(default=False, description="Restore locally, then broadcast the restored DB to peers"),
) -> RestoreResponse:
    """
    Restore a local backup to the live DB.

    Normal restore applies only to this node.

    Force restore applies locally first, then sends the restored database to
    all configured peers via the Layer 1 full-restore endpoint.
    """
    if not _SAFE_NAME.match(filename):
        raise HTTPException(status_code=400, detail="Invalid backup filename.")

    bdir = _backup_dir()
    src_path = bdir / filename
    if not src_path.exists():
        raise HTTPException(status_code=404, detail=f"Backup not found: {filename}")

    # Read gen from live DB before restore
    with get_conn() as conn:
        gen_before = get_gen(conn)

    gen_after = gen_before  # updated below
    warning: str | None = None
    fleet_success: bool | None = None
    peer_results: list[PeerRestoreResult] = []

    # Extract the .tar.gz to a temp .db file alongside the live DB, then
    # atomically replace without touching the backup original.
    db_dir = Path(cfg.DB_PATH).parent
    with tempfile.NamedTemporaryFile(
        dir=db_dir, suffix=".db.tmp", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        gen_after = _extract_backup_to_temp(src_path, tmp_path)
        if not validate_sqlite_file(str(tmp_path)):
            raise HTTPException(
                status_code=422,
                detail="Restore failed — backup DB did not pass SQLite integrity_check.",
            )

        # Atomically replace the live DB
        os.replace(str(tmp_path), cfg.DB_PATH)
        if not post_restore_housekeeping():
            raise HTTPException(
                status_code=500,
                detail="Restore applied but post-restore integrity check failed.",
            )
        log.info(
            "restore applied: %s — gen %d → %d (force=%s)",
            filename, gen_before, gen_after, force,
        )

    except HTTPException:
        try:
            os.unlink(str(tmp_path))
        except OSError:
            pass
        raise

    except Exception:
        # Clean up temp file if replace failed
        try:
            os.unlink(str(tmp_path))
        except OSError:
            pass
        raise HTTPException(
            status_code=500, detail="Restore failed — live DB was not modified."
        )

    if force:
        operation_id = uuid.uuid4().hex
        if not cfg.PEER_SYNC_URLS:
            fleet_success = True
            warning = "Force restore applied locally, but no peers are configured on this node."
        else:
            try:
                peer_results = await _broadcast_live_db_to_peers(operation_id)
            except Exception as exc:
                log.exception("force restore broadcast failed unexpectedly")
                fleet_success = False
                warning = f"Force restore applied locally, but peer broadcast failed before completion: {exc}"
            else:
                fleet_success = all(result.ok for result in peer_results)
                if fleet_success:
                    warning = None
                else:
                    failed = ", ".join(
                        f"{result.node_id} ({result.detail})"
                        for result in peer_results
                        if not result.ok
                    )
                    warning = (
                        "Force restore applied locally, but some peers did not accept the restored DB: "
                        f"{failed}"
                    )

    return RestoreResponse(
        restored_from=filename,
        force=force,
        gen_before=gen_before,
        gen_after=gen_after,
        warning=warning,
        fleet_success=fleet_success,
        peer_results=peer_results,
    )


@router.delete("/{filename}", status_code=204)
def delete_backup(filename: str) -> Response:
    """Delete a local backup file."""
    if not _SAFE_NAME.match(filename):
        raise HTTPException(status_code=400, detail="Invalid backup filename.")
    bdir = _backup_dir()
    path = bdir / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Backup not found: {filename}")
    path.unlink()
    log.info("backup deleted: %s", filename)
    return Response(status_code=204)
