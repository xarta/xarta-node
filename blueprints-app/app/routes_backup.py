"""
routes_backup.py — local DB backup and restore endpoints.

GET  /api/v1/backup                          — list available backups
POST /api/v1/backup                          — create a new backup
POST /api/v1/backup/restore/{filename}       — restore a backup locally
  ?force=true                                — bump gen above all peers first

Backups are plain SQLite files (not zipped) saved to BLUEPRINTS_BACKUP_DIR.
The sync_queue table is always cleared in backups — there is no point
preserving stale outbound queue items from backup time.

⚠ RESTORE NOTE
Local restore reverts this node's gen to backup-time gen, which is below
current peer gens.  The gen guard will then cause peers to push their state
back to this node at the next drain cycle, overwriting the restore.

Use ?force=true to query peers for their max gen and bump the restored
DB's gen to max+1 before applying.  This causes THIS node to win the gen
guard check when it next syncs, propagating the restored state to all peers.
Only use this for disaster recovery / corruption fix scenarios.
"""

import io
import json
import logging
import os
import re
import sqlite3
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Query
from starlette.responses import Response
from pydantic import BaseModel

from . import config as cfg
from .db import get_conn, get_gen

log = logging.getLogger(__name__)

router = APIRouter(prefix="/backup", tags=["backup"])

_SAFE_NAME = re.compile(r"^\d{4}-\d{2}-\d{2}-\d{6}-blueprints\.db\.tar\.gz$")


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


def _peer_addresses() -> list[str]:
    """Return all known peer API base URLs from the nodes table."""
    addresses: list[str] = []
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT addresses FROM nodes WHERE node_id != ?", (cfg.NODE_ID,)
        ).fetchall()
    for row in rows:
        try:
            addrs = json.loads(row[0] or "[]")
            addresses.extend(addrs)
        except (json.JSONDecodeError, TypeError):
            pass
    return addresses


async def _fetch_peer_gen(address: str) -> int | None:
    """GET {address}/health and return the gen field, or None on failure."""
    url = address.rstrip("/") + "/health"
    try:
        async with httpx.AsyncClient(timeout=5.0, verify=False) as client:
            r = await client.get(url)
            if r.status_code == 200:
                return r.json().get("gen")
    except Exception as exc:
        log.debug("force-restore: could not reach %s: %s", url, exc)
    return None


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
    force: bool = Query(default=False, description="Bump gen above all peers before restoring"),
) -> RestoreResponse:
    """
    Restore a local backup to the live DB.

    ⚠ This only restores THIS node's DB.  Other nodes are NOT automatically
    updated.  Without ?force=true the restored gen will be below peer gens,
    so peers will push their current state back to this node at next sync,
    eventually overwriting the restore.

    Use ?force=true to query peers for their max gen and set the restored
    DB's gen to max+1.  This node will then win the gen guard on next sync
    and propagate the restored state to all peers.
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

    # If force: query peers for their max gen
    gen_after = gen_before  # updated below
    warning: str | None = None
    force_gen: int | None = None

    if force:
        peer_addresses = _peer_addresses()
        if not peer_addresses:
            warning = (
                "Force requested but no peers are registered — "
                "gen was not bumped.  Restore applied with original backup gen."
            )
        else:
            import asyncio
            peer_gens = await asyncio.gather(
                *[_fetch_peer_gen(addr) for addr in peer_addresses]
            )
            reachable = [g for g in peer_gens if g is not None]
            if reachable:
                force_gen = max(reachable) + 1
                log.info(
                    "force-restore: peer gens=%s, will set gen=%d in backup",
                    reachable, force_gen,
                )
            else:
                warning = (
                    "Force requested but could not reach any peers — "
                    "gen was not bumped.  Restore applied with original backup gen."
                )

    # Extract the .tar.gz to a temp .db file alongside the live DB, patch gen,
    # then atomically replace without touching the backup original.
    db_dir = Path(cfg.DB_PATH).parent
    with tempfile.NamedTemporaryFile(
        dir=db_dir, suffix=".db.tmp", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        # Extract blueprints.db from archive into tmp_path
        with tarfile.open(str(src_path), "r:gz") as tar:
            member = tar.getmember("blueprints.db")
            with tar.extractfile(member) as f_in, open(str(tmp_path), "wb") as f_out:
                f_out.write(f_in.read())

        # Clear sync_queue in the working copy (backup should already be clear,
        # but be defensive)
        conn_tmp = sqlite3.connect(str(tmp_path))
        try:
            conn_tmp.execute("DELETE FROM sync_queue")
            if force_gen is not None:
                conn_tmp.execute(
                    "UPDATE sync_meta SET value=? WHERE key='gen'",
                    (str(force_gen),),
                )
                conn_tmp.execute(
                    "UPDATE sync_meta SET value='false' WHERE key='integrity_ok'"
                )
            conn_tmp.commit()
            # Read the actual gen that will be applied
            row = conn_tmp.execute(
                "SELECT value FROM sync_meta WHERE key='gen'"
            ).fetchone()
            gen_after = int(row[0]) if row else gen_before
        finally:
            conn_tmp.close()

        # Atomically replace the live DB
        os.replace(str(tmp_path), cfg.DB_PATH)
        log.info(
            "restore applied: %s — gen %d → %d (force=%s)",
            filename, gen_before, gen_after, force,
        )

    except Exception:
        # Clean up temp file if replace failed
        try:
            os.unlink(str(tmp_path))
        except OSError:
            pass
        raise HTTPException(
            status_code=500, detail="Restore failed — live DB was not modified."
        )

    if force_gen is not None and warning is None:
        warning = (
            "FORCE RESTORE applied.  This node's gen is now "
            f"{gen_after}, above all known peers.  On next sync drain, "
            "this node will push the restored state to all peers, "
            "overwriting their current data.  This is intentional."
        )

    return RestoreResponse(
        restored_from=filename,
        force=force,
        gen_before=gen_before,
        gen_after=gen_after,
        warning=warning,
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
