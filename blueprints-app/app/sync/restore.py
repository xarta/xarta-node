"""
sync/restore.py — Layer 1 backup creation and restore.

make_full_backup(): zips the SQLite DB into a bytes object and returns it
with a SHA-256 hex checksum. Used by the drain loop when queue overflow occurs
or a new node is onboarded.

apply_restore(): receives the zip bytes + expected checksum, verifies integrity,
atomically replaces the local DB, and re-runs schema initialisation.

Note: apply_restore is async because the calling route is async, but the actual
SQLite operations are blocking and called via asyncio.to_thread().
"""

import asyncio
import hashlib
import io
import logging
import os
import shutil
import sqlite3
import zipfile

from .. import config as cfg

log = logging.getLogger(__name__)

_DB_ENTRY_NAME = "blueprints.db"


# ── Backup creation ───────────────────────────────────────────────────────────

def make_full_backup() -> tuple[bytes, str]:
    """
    Create an in-memory zip containing the current DB file.

    Returns (zip_bytes, sha256_hex).
    Raises if the DB file does not exist.
    """
    if not os.path.exists(cfg.DB_PATH):
        raise FileNotFoundError(f"DB not found at {cfg.DB_PATH}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(cfg.DB_PATH, arcname=_DB_ENTRY_NAME)
    zip_bytes = buf.getvalue()

    sha256_hex = hashlib.sha256(zip_bytes).hexdigest()
    log.debug(
        "created full backup: %d bytes, sha256=%s", len(zip_bytes), sha256_hex[:16]
    )
    return zip_bytes, sha256_hex


def validate_sqlite_file(path: str) -> bool:
    """Run PRAGMA integrity_check against a candidate SQLite file."""
    try:
        with sqlite3.connect(path) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        ok = bool(row and row[0] == "ok")
        if not ok:
            detail = row[0] if row else "no result"
            log.error("restore candidate integrity_check failed: %s", detail)
        return ok
    except Exception:
        log.exception("restore candidate integrity_check raised unexpectedly")
        return False


def post_restore_housekeeping() -> bool:
    """Re-initialise schema, restore local node records, and verify integrity."""
    try:
        from ..db import check_integrity, init_db
        from ..routes_nodes import _upsert_nodes_from_config

        init_db()
        _upsert_nodes_from_config()
        ok = check_integrity()
        if not ok:
            log.error("post-restore integrity check failed — node remains degraded")
        return ok
    except Exception:
        log.exception("post-restore housekeeping failed")
        return False


# ── Restore ───────────────────────────────────────────────────────────────────

async def apply_restore(zip_bytes: bytes, expected_sha256: str) -> bool:
    """
    Verify checksum, extract the DB from the zip, and atomically replace the
    local DB file. Re-initialises the schema in case any migrations are needed.

    Returns True on success, False on any failure.
    """
    return await asyncio.to_thread(_apply_restore_sync, zip_bytes, expected_sha256)


def _apply_restore_sync(zip_bytes: bytes, expected_sha256: str) -> bool:
    # 1. Verify checksum
    actual_sha256 = hashlib.sha256(zip_bytes).hexdigest()
    if actual_sha256 != expected_sha256:
        log.error(
            "restore checksum mismatch: expected=%s actual=%s",
            expected_sha256[:16],
            actual_sha256[:16],
        )
        return False

    # 2. Validate zip integrity and extract to a temp file
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            if _DB_ENTRY_NAME not in zf.namelist():
                log.error("restore zip does not contain '%s'", _DB_ENTRY_NAME)
                return False
            tmp_path = cfg.DB_PATH + ".restore_tmp"
            with zf.open(_DB_ENTRY_NAME) as src, open(tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
    except zipfile.BadZipFile:
        log.error("restore payload is not a valid zip file")
        return False
    except Exception:
        log.exception("unexpected error extracting restore zip")
        return False

    # 2b. Validate the extracted DB before replacing the live file
    if not validate_sqlite_file(tmp_path):
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return False

    # 3. Atomically swap the DB file
    try:
        os.replace(tmp_path, cfg.DB_PATH)
    except Exception:
        log.exception("failed to replace DB with restored copy")
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        return False

    # 4. Re-run schema init, reassert local nodes.json state, and verify integrity
    if not post_restore_housekeeping():
        return False

    log.info("DB restore applied successfully from %d-byte zip", len(zip_bytes))
    return True
