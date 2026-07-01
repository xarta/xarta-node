"""SQLite maintenance helpers shared by backup and sync routes."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def sqlite_file_stats(path: str | os.PathLike[str]) -> dict[str, Any]:
    db_path = Path(path)
    stats: dict[str, Any] = {
        "path": str(db_path),
        "exists": db_path.exists(),
        "size_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "page_size": 0,
        "page_count": 0,
        "freelist_count": 0,
    }
    if not db_path.exists():
        return stats
    with sqlite3.connect(str(db_path)) as conn:
        stats["page_size"] = int(conn.execute("PRAGMA page_size").fetchone()[0])
        stats["page_count"] = int(conn.execute("PRAGMA page_count").fetchone()[0])
        stats["freelist_count"] = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    return stats


def vacuum_sqlite_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Run VACUUM against a standalone SQLite file and return before/after stats."""
    before = sqlite_file_stats(path)
    with sqlite3.connect(str(path)) as conn:
        conn.execute("VACUUM")
    after = sqlite_file_stats(path)
    return {"before": before, "after": after}


def clone_without_sync_queue(
    source_path: str | os.PathLike[str],
    dest_path: str | os.PathLike[str],
    *,
    vacuum: bool = True,
) -> dict[str, Any]:
    """
    Clone a SQLite DB via sqlite3 backup API, clear sync_queue in the clone, and
    optionally VACUUM the clone so backup/export payloads do not preserve free pages.
    """
    source = str(source_path)
    dest = str(dest_path)
    src = sqlite3.connect(source)
    dst = sqlite3.connect(dest)
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()

    deleted_sync_queue_rows = 0
    with sqlite3.connect(dest) as conn:
        if table_exists(conn, "sync_queue"):
            row = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()
            deleted_sync_queue_rows = int(row[0]) if row else 0
            conn.execute("DELETE FROM sync_queue")
            conn.commit()

    vacuum_report = vacuum_sqlite_file(dest) if vacuum else None
    return {
        "source_path": source,
        "dest_path": dest,
        "deleted_sync_queue_rows": deleted_sync_queue_rows,
        "vacuum": vacuum_report,
        "stats": sqlite_file_stats(dest),
    }
