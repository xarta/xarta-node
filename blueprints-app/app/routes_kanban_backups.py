"""Kanban scoped import/export backups.

These backup packages are deliberately separate from the full Blueprints DB backups in
routes_backup.py.  A Kanban backup package stores only the Kanban DB tables plus the
file-backed Kanban Markdown/image tree under BLUEPRINTS_KANBAN_DIR.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import re
import shutil
import sqlite3
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from . import config as cfg
from .db import get_conn, get_gen, increment_gen
from .kanban_datastore import KANBAN_DATASTORE_TABLES
from .sync.queue import enqueue_for_all_peers

log = logging.getLogger(__name__)

router = APIRouter(prefix="/personal/kanban/backups", tags=["personal"])

BACKUP_SCHEMA = "xarta.kanban.backup.v1"
BACKUP_TABLE_SCHEMA = "xarta.kanban.backup.tables.v1"
_SAFE_BACKUP_NAME = re.compile(
    r"^\d{4}-\d{2}-\d{2}-\d{6}-kanban-backup-(manual|pre-import)-[a-f0-9]{8}\.tar\.gz$"
)
_TABLE_DATA_MEMBER = "data/kanban-tables.json"
_MANIFEST_MEMBER = "manifest.json"
_FILES_PREFIX = "files/kanban/"
_PRESERVE_KANBAN_ROOT_NAMES = {"backups", ".stfolder", ".stversions"}

KANBAN_BACKUP_TABLES: tuple[str, ...] = (*KANBAN_DATASTORE_TABLES,)
KANBAN_BACKUP_EXCLUDED_TABLES: tuple[str, ...] = ("sync_queue",)


class KanbanBackupEntry(BaseModel):
    filename: str
    size_bytes: int
    created_at: str
    backup_id: str = ""
    kind: str = ""
    db_gen: int | None = None
    table_counts: dict[str, int] = Field(default_factory=dict)
    file_count: int | None = None
    sha256: str = ""


class KanbanBackupListResponse(BaseModel):
    ok: bool
    backup_dir: str
    kanban_root: str
    backups: list[KanbanBackupEntry]


class KanbanBackupCreatedResponse(BaseModel):
    ok: bool
    backup: KanbanBackupEntry
    manifest: dict[str, Any]


class KanbanBackupValidationResponse(BaseModel):
    ok: bool
    filename: str
    manifest: dict[str, Any]
    table_counts: dict[str, int]
    file_count: int
    dry_run: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)


class KanbanBackupImportResponse(BaseModel):
    ok: bool
    filename: str
    applied: bool
    restored_files: bool
    gen_before: int
    gen_after: int
    table_counts: dict[str, int]
    file_count: int
    dry_run: dict[str, Any] = Field(default_factory=dict)
    pre_import_backup: str | None = None
    warnings: list[str] = Field(default_factory=list)


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _backup_dir() -> Path:
    path = Path(cfg.KANBAN_BACKUP_DIR).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _kanban_root() -> Path:
    path = Path(cfg.KANBAN_DIR).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_backup_path(filename: str) -> Path:
    clean = filename.strip()
    if not _SAFE_BACKUP_NAME.match(clean):
        raise HTTPException(status_code=400, detail="Invalid Kanban backup package filename.")
    path = (_backup_dir() / clean).resolve()
    if path.parent != _backup_dir():
        raise HTTPException(status_code=400, detail="Invalid Kanban backup package path.")
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Kanban backup package not found: {clean}")
    return path


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256_json(payload: Any) -> str:
    return _sha256_bytes(_json_bytes(payload))


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        raise HTTPException(status_code=500, detail=f"Kanban backup package table missing: {table}")
    return [str(row["name"]) for row in rows]


def _table_pk(conn: sqlite3.Connection, table: str) -> str:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    for row in rows:
        if int(row["pk"] or 0):
            return str(row["name"])
    raise HTTPException(
        status_code=500, detail=f"Kanban backup package table has no primary key: {table}"
    )


def _collect_table_data(conn: sqlite3.Connection) -> dict[str, Any]:
    tables: dict[str, Any] = {}
    for table in KANBAN_BACKUP_TABLES:
        columns = _table_columns(conn, table)
        pk = _table_pk(conn, table)
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY {pk}").fetchall()
        tables[table] = {
            "columns": columns,
            "primary_key": pk,
            "rows": [{col: row[col] for col in columns} for row in rows],
        }
    return {
        "schema": BACKUP_TABLE_SCHEMA,
        "excluded_tables": list(KANBAN_BACKUP_EXCLUDED_TABLES),
        "sync_queue_included": False,
        "tables": tables,
    }


def _iter_kanban_files(root: Path) -> list[Path]:
    files: list[Path] = []
    if not root.exists():
        return files
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] in _PRESERVE_KANBAN_ROOT_NAMES:
            continue
        files.append(path)
    return sorted(files)


def _file_manifest(root: Path, files: list[Path]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in files:
        stat = path.stat()
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size_bytes": stat.st_size,
                "sha256": _sha256_file(path),
            }
        )
    return entries


def _table_hashes(table_data: dict[str, Any]) -> dict[str, str]:
    tables = table_data.get("tables") or {}
    return {table: _sha256_json(tables[table]) for table in KANBAN_BACKUP_TABLES}


def _load_backup_manifest(path: Path) -> dict[str, Any]:
    try:
        with tarfile.open(path, "r:gz") as tar:
            try:
                manifest_file = tar.extractfile(_MANIFEST_MEMBER)
            except KeyError as exc:
                raise HTTPException(
                    status_code=422, detail=f"Backup package missing {exc.args[0]}"
                ) from exc
            if manifest_file is None:
                raise HTTPException(
                    status_code=422,
                    detail="Backup package manifest is not readable.",
                )
            manifest = json.loads(manifest_file.read().decode("utf-8"))
    except tarfile.TarError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid Kanban backup package: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid Kanban backup package JSON: {exc}"
        ) from exc

    if manifest.get("schema") != BACKUP_SCHEMA:
        raise HTTPException(status_code=422, detail="Kanban backup package schema mismatch.")
    return manifest


def _load_backup_package(path: Path) -> tuple[dict[str, Any], dict[str, Any], int]:
    warnings: list[str] = []
    try:
        with tarfile.open(path, "r:gz") as tar:
            try:
                manifest_file = tar.extractfile(_MANIFEST_MEMBER)
                table_file = tar.extractfile(_TABLE_DATA_MEMBER)
            except KeyError as exc:
                raise HTTPException(
                    status_code=422, detail=f"Backup package missing {exc.args[0]}"
                ) from exc
            if manifest_file is None or table_file is None:
                raise HTTPException(
                    status_code=422,
                    detail="Backup package manifest or data member is not readable.",
                )
            manifest = json.loads(manifest_file.read().decode("utf-8"))
            table_data = json.loads(table_file.read().decode("utf-8"))
            file_count = 0
            actual_file_hashes: dict[str, str] = {}
            for member in tar.getmembers():
                if member.name.startswith(_FILES_PREFIX) and member.isfile():
                    rel = _safe_package_member_rel(member.name).as_posix()
                    source = tar.extractfile(member)
                    if source is None:
                        raise HTTPException(
                            status_code=422,
                            detail=f"Unreadable backup package member: {member.name}",
                        )
                    actual_file_hashes[rel] = _sha256_bytes(source.read())
                    file_count += 1
                elif member.name.startswith(_FILES_PREFIX) and not member.isdir():
                    warnings.append(f"ignored non-file package member: {member.name}")
    except tarfile.TarError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid Kanban backup package: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=422, detail=f"Invalid Kanban backup package JSON: {exc}"
        ) from exc

    if manifest.get("schema") != BACKUP_SCHEMA:
        raise HTTPException(status_code=422, detail="Kanban backup package schema mismatch.")
    if table_data.get("schema") != BACKUP_TABLE_SCHEMA:
        raise HTTPException(status_code=422, detail="Kanban backup table schema mismatch.")
    missing = [table for table in KANBAN_BACKUP_TABLES if table not in table_data.get("tables", {})]
    if missing:
        raise HTTPException(
            status_code=422, detail=f"Backup package missing Kanban tables: {', '.join(missing)}"
        )
    forbidden = [
        table for table in KANBAN_BACKUP_EXCLUDED_TABLES if table in table_data.get("tables", {})
    ]
    if forbidden:
        raise HTTPException(
            status_code=422,
            detail=f"Backup package includes excluded tables: {', '.join(forbidden)}",
        )
    expected_table_hashes = manifest.get("table_hashes") or {}
    if expected_table_hashes:
        actual_table_hashes = _table_hashes(table_data)
        for table in KANBAN_BACKUP_TABLES:
            if str(expected_table_hashes.get(table) or "") != actual_table_hashes.get(table):
                warnings.append(f"manifest table hash mismatch: {table}")
    expected_table_data_hash = str(manifest.get("table_data_sha256") or "")
    if expected_table_data_hash and expected_table_data_hash != _sha256_json(table_data):
        warnings.append("manifest table_data_sha256 differs from backup table data")
    expected_file_hashes = manifest.get("file_hashes") or {}
    if expected_file_hashes:
        for rel, expected_hash in sorted(expected_file_hashes.items()):
            actual_hash = actual_file_hashes.get(str(rel))
            if actual_hash is None:
                warnings.append(f"manifest file missing from package: {rel}")
            elif str(expected_hash) != actual_hash:
                warnings.append(f"manifest file hash mismatch: {rel}")
        for rel in sorted(set(actual_file_hashes) - {str(key) for key in expected_file_hashes}):
            warnings.append(f"package file missing from manifest hashes: {rel}")
    if warnings:
        manifest.setdefault("warnings", []).extend(warnings)
    return manifest, table_data, file_count


def _safe_package_member_rel(member_name: str) -> PurePosixPath:
    if not member_name.startswith(_FILES_PREFIX):
        raise HTTPException(status_code=422, detail=f"Unexpected package member: {member_name}")
    rel = PurePosixPath(member_name[len(_FILES_PREFIX) :])
    if rel.is_absolute() or any(part in {"", ".", ".."} for part in rel.parts):
        raise HTTPException(status_code=422, detail=f"Unsafe package path: {member_name}")
    return rel


def _backup_entry(path: Path, *, include_sha256: bool = False) -> KanbanBackupEntry:
    stat = path.stat()
    backup_id = ""
    kind = ""
    db_gen: int | None = None
    table_counts: dict[str, int] = {}
    file_count: int | None = None
    sha256 = ""
    try:
        manifest = _load_backup_manifest(path)
        backup_id = str(manifest.get("backup_id") or "")
        kind = str(manifest.get("kind") or "")
        db_gen_raw = manifest.get("db_gen")
        db_gen = int(db_gen_raw) if db_gen_raw is not None else None
        table_counts = {str(k): int(v) for k, v in (manifest.get("table_counts") or {}).items()}
        file_count = int(manifest.get("file_count", 0))
        if include_sha256:
            sha256 = _sha256_file(path)
    except Exception:
        log.debug("could not read Kanban backup package manifest: %s", path.name, exc_info=True)
    return KanbanBackupEntry(
        filename=path.name,
        size_bytes=stat.st_size,
        created_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        backup_id=backup_id,
        kind=kind,
        db_gen=db_gen,
        table_counts=table_counts,
        file_count=file_count,
        sha256=sha256,
    )


def _create_backup_file(kind: str = "manual") -> KanbanBackupEntry:
    clean_kind = kind if kind in {"manual", "pre-import"} else "manual"
    backup_id = uuid.uuid4().hex
    ts = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    filename = f"{ts}-kanban-backup-{clean_kind}-{backup_id[:8]}.tar.gz"
    dest = _backup_dir() / filename
    root = _kanban_root()

    with get_conn() as conn:
        db_gen = get_gen(conn)
        table_data = _collect_table_data(conn)

    table_counts = {table: len(payload["rows"]) for table, payload in table_data["tables"].items()}
    files = _iter_kanban_files(root)
    file_manifest = _file_manifest(root, files)
    manifest = {
        "schema": BACKUP_SCHEMA,
        "backup_id": backup_id,
        "kind": clean_kind,
        "purpose": "kanban-datastore-migration-export",
        "created_at": _utc_now(),
        "node_id": cfg.NODE_ID,
        "node_name": cfg.NODE_NAME,
        "app_commit": getattr(cfg, "COMMIT_HASH", ""),
        "db_gen": db_gen,
        "kanban_root": str(root),
        "tables_schema": BACKUP_TABLE_SCHEMA,
        "included_tables": list(KANBAN_BACKUP_TABLES),
        "excluded_tables": list(KANBAN_BACKUP_EXCLUDED_TABLES),
        "sync_queue_included": False,
        "table_counts": table_counts,
        "table_hashes": _table_hashes(table_data),
        "table_data_sha256": _sha256_json(table_data),
        "file_count": len(files),
        "files": file_manifest,
        "file_hashes": {entry["path"]: entry["sha256"] for entry in file_manifest},
        "excluded_root_names": sorted(_PRESERVE_KANBAN_ROOT_NAMES),
    }

    with tarfile.open(dest, "w:gz") as tar:
        for member_name, payload in (
            (_MANIFEST_MEMBER, manifest),
            (_TABLE_DATA_MEMBER, table_data),
        ):
            data = json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True).encode("utf-8")
            info = tarfile.TarInfo(member_name)
            info.size = len(data)
            info.mtime = int(datetime.now(tz=timezone.utc).timestamp())
            tar.addfile(info, fileobj=io.BytesIO(data))
        for path in files:
            rel = path.relative_to(root).as_posix()
            tar.add(path, arcname=f"{_FILES_PREFIX}{rel}", recursive=False)

    return _backup_entry(dest)


def _table_counts(table_data: dict[str, Any]) -> dict[str, int]:
    return {
        table: len(payload.get("rows") or [])
        for table, payload in table_data.get("tables", {}).items()
    }


def _rows_by_primary_key(
    conn: sqlite3.Connection, table: str
) -> tuple[str, list[str], dict[str, dict[str, Any]]]:
    columns = _table_columns(conn, table)
    pk = _table_pk(conn, table)
    rows = {
        str(row[pk]): {col: row[col] for col in columns}
        for row in conn.execute(f"SELECT * FROM {table}").fetchall()
    }
    return pk, columns, rows


def _row_hash(row: dict[str, Any], columns: list[str]) -> str:
    return _sha256_json({col: row.get(col) for col in columns})


def _import_dry_run_report(
    conn: sqlite3.Connection,
    table_data: dict[str, Any],
) -> dict[str, Any]:
    table_reports: dict[str, Any] = {}
    totals = {
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "deleted": 0,
        "conflicts": 0,
    }
    for table in KANBAN_BACKUP_TABLES:
        _pk, db_columns, current_rows = _rows_by_primary_key(conn, table)
        payload = table_data["tables"][table]
        backup_columns = [col for col in payload.get("columns", []) if col in db_columns]
        backup_rows = {
            str(row.get(payload.get("primary_key") or _pk)): row
            for row in payload.get("rows") or []
        }
        inserted_ids = sorted(set(backup_rows) - set(current_rows))
        deleted_ids = sorted(set(current_rows) - set(backup_rows))
        shared_ids = sorted(set(current_rows) & set(backup_rows))
        updated_ids = [
            row_id
            for row_id in shared_ids
            if _row_hash(current_rows[row_id], backup_columns)
            != _row_hash(backup_rows[row_id], backup_columns)
        ]
        unchanged_ids = [row_id for row_id in shared_ids if row_id not in set(updated_ids)]
        conflicts = len(updated_ids) + len(deleted_ids)
        table_reports[table] = {
            "primary_key": payload.get("primary_key") or _pk,
            "inserted": len(inserted_ids),
            "updated": len(updated_ids),
            "unchanged": len(unchanged_ids),
            "deleted": len(deleted_ids),
            "conflicts": conflicts,
            "sample_inserted_ids": inserted_ids[:10],
            "sample_updated_ids": updated_ids[:10],
            "sample_deleted_ids": deleted_ids[:10],
        }
        totals["inserted"] += len(inserted_ids)
        totals["updated"] += len(updated_ids)
        totals["unchanged"] += len(unchanged_ids)
        totals["deleted"] += len(deleted_ids)
        totals["conflicts"] += conflicts

    return {
        "schema": "xarta.kanban.backup.import_preview.v1",
        "table_count": len(KANBAN_BACKUP_TABLES),
        "tables": table_reports,
        "totals": totals,
        "idempotent": not (totals["inserted"] or totals["updated"] or totals["deleted"]),
        "sync_queue_included": "sync_queue" in (table_data.get("tables") or {}),
        "sync_queue_rows_created": False,
        "excluded_tables": list(KANBAN_BACKUP_EXCLUDED_TABLES),
    }


def _extract_backup_files(path: Path, dest_root: Path) -> int:
    count = 0
    with tarfile.open(path, "r:gz") as tar:
        for member in tar.getmembers():
            if not member.name.startswith(_FILES_PREFIX):
                continue
            if member.isdir():
                continue
            if not member.isfile():
                raise HTTPException(
                    status_code=422, detail=f"Unsupported backup package member: {member.name}"
                )
            rel = _safe_package_member_rel(member.name)
            target = dest_root.joinpath(*rel.parts)
            target.parent.mkdir(parents=True, exist_ok=True)
            source = tar.extractfile(member)
            if source is None:
                raise HTTPException(
                    status_code=422, detail=f"Unreadable backup package member: {member.name}"
                )
            with target.open("wb") as out:
                shutil.copyfileobj(source, out)
            count += 1
    return count


def _replace_kanban_files_from_staged(tmp_root: Path) -> int:
    root = _kanban_root()
    file_count = 0
    for path in tmp_root.rglob("*"):
        if path.is_file():
            file_count += 1
    with tempfile.TemporaryDirectory(prefix=".kanban-restore-", dir=str(root.parent)) as backup_tmp:
        backup_root = Path(backup_tmp)
        moved: list[tuple[Path, Path]] = []
        try:
            for child in root.iterdir():
                if child.name in _PRESERVE_KANBAN_ROOT_NAMES:
                    continue
                backup_child = backup_root / child.name
                shutil.move(str(child), str(backup_child))
                moved.append((backup_child, child))
            for child in tmp_root.iterdir():
                dest = root / child.name
                if child.is_dir():
                    shutil.copytree(child, dest)
                else:
                    shutil.copy2(child, dest)
        except Exception:
            for child in root.iterdir():
                if child.name in _PRESERVE_KANBAN_ROOT_NAMES:
                    continue
                if child.is_dir():
                    shutil.rmtree(child)
                else:
                    child.unlink()
            for backup_child, original_child in moved:
                if backup_child.exists():
                    shutil.move(str(backup_child), str(original_child))
            raise
    return file_count


def _import_table_rows(
    conn: sqlite3.Connection,
    table_data: dict[str, Any],
    gen: int,
    audit_row: dict[str, Any],
) -> dict[str, int]:
    changed: dict[str, int] = {}
    existing_ids: dict[str, list[str]] = {}
    pks = {table: _table_pk(conn, table) for table in KANBAN_BACKUP_TABLES}

    for table in KANBAN_BACKUP_TABLES:
        pk = pks[table]
        existing_ids[table] = [
            str(row[pk]) for row in conn.execute(f"SELECT {pk} FROM {table}").fetchall()
        ]

    for table in reversed(KANBAN_BACKUP_TABLES):
        conn.execute(f"DELETE FROM {table}")

    for table in KANBAN_BACKUP_TABLES:
        payload = table_data["tables"][table]
        db_columns = _table_columns(conn, table)
        backup_columns = [col for col in payload.get("columns", []) if col in db_columns]
        rows = payload.get("rows") or []
        if rows and not backup_columns:
            raise HTTPException(
                status_code=422, detail=f"Backup table has no usable columns: {table}"
            )
        placeholders = ", ".join("?" for _ in backup_columns)
        quoted = ", ".join(backup_columns)
        sql = f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})"
        for row in rows:
            conn.execute(sql, [row.get(col) for col in backup_columns])
        changed[table] = len(rows)

    audit_id = audit_row["audit_id"]
    conn.execute(
        """
        INSERT INTO kanban_audit_log
            (audit_id, actor, source_surface, action, target_ref, item_id,
             parent_item_id, created_at, request_id, run_id, result,
             source_hash, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            audit_id,
            audit_row["actor"],
            audit_row["source_surface"],
            audit_row["action"],
            audit_row["target_ref"],
            audit_row["item_id"],
            audit_row["parent_item_id"],
            audit_row["created_at"],
            audit_row["request_id"],
            audit_row["run_id"],
            audit_row["result"],
            audit_row["source_hash"],
            audit_row["metadata_json"],
        ),
    )
    changed["kanban_audit_log"] = changed.get("kanban_audit_log", 0) + 1

    for table in KANBAN_BACKUP_TABLES:
        pk = pks[table]
        for row_id in existing_ids[table]:
            enqueue_for_all_peers(conn, "DELETE", table, row_id, {}, gen)
        for row in conn.execute(f"SELECT * FROM {table}").fetchall():
            enqueue_for_all_peers(conn, "UPDATE", table, str(row[pk]), dict(row), gen)
    return changed


@router.get("", response_model=KanbanBackupListResponse)
def list_kanban_backups(
    include_hashes: bool = False,
) -> KanbanBackupListResponse:
    backup_dir = _backup_dir()
    backups = [
        _backup_entry(path, include_sha256=include_hashes)
        for path in sorted(backup_dir.glob("*-kanban-*.tar.gz"), reverse=True)
        if _SAFE_BACKUP_NAME.match(path.name)
    ]
    return {
        "ok": True,
        "backup_dir": str(backup_dir),
        "kanban_root": str(_kanban_root()),
        "backups": backups,
    }


@router.post("", response_model=KanbanBackupCreatedResponse, status_code=201)
def create_kanban_backup(
    kind: str = Query(default="manual", pattern="^(manual|pre-import)$"),
) -> KanbanBackupCreatedResponse:
    backup = _create_backup_file(kind=kind)
    manifest, _table_data, _file_count = _load_backup_package(_safe_backup_path(backup.filename))
    return {"ok": True, "backup": backup, "manifest": manifest}


@router.get("/{filename}", include_in_schema=False)
def download_kanban_backup(filename: str) -> FileResponse:
    path = _safe_backup_path(filename)
    return FileResponse(path, filename=path.name, media_type="application/gzip")


@router.get("/{filename}/validate", response_model=KanbanBackupValidationResponse)
def validate_kanban_backup(filename: str) -> KanbanBackupValidationResponse:
    path = _safe_backup_path(filename)
    manifest, table_data, file_count = _load_backup_package(path)
    warnings = list(manifest.get("warnings") or [])
    manifest_counts = manifest.get("table_counts") or {}
    actual_counts = _table_counts(table_data)
    if manifest_counts and manifest_counts != actual_counts:
        warnings.append("manifest table counts differ from backup table data")
    if int(manifest.get("file_count", file_count)) != file_count:
        warnings.append("manifest file_count differs from backup package members")
    with get_conn() as conn:
        dry_run = _import_dry_run_report(conn, table_data)
    return {
        "ok": not warnings,
        "filename": path.name,
        "manifest": manifest,
        "table_counts": actual_counts,
        "file_count": file_count,
        "dry_run": dry_run,
        "warnings": warnings,
    }


@router.post("/{filename}/import", response_model=KanbanBackupImportResponse)
def import_kanban_backup(
    filename: str,
    apply: bool = Query(
        default=False, description="Import the backup package. False validates only."
    ),
    restore_files: bool = Query(
        default=True, description="Restore file-backed Kanban documents/images when applying."
    ),
    backup_before_import: bool = Query(
        default=True, description="Create a pre-import Kanban backup package first."
    ),
) -> KanbanBackupImportResponse:
    path = _safe_backup_path(filename)
    manifest, table_data, file_count = _load_backup_package(path)
    table_counts = _table_counts(table_data)
    warnings = list(manifest.get("warnings") or [])

    with get_conn() as conn:
        gen_before = get_gen(conn)
        dry_run = _import_dry_run_report(conn, table_data)
    gen_after = gen_before
    pre_import_backup: str | None = None
    applied_file_count = file_count

    if not apply:
        return {
            "ok": not warnings,
            "filename": path.name,
            "applied": False,
            "restored_files": False,
            "gen_before": gen_before,
            "gen_after": gen_after,
            "table_counts": table_counts,
            "file_count": file_count,
            "dry_run": dry_run,
            "pre_import_backup": None,
            "warnings": warnings,
        }

    staged_files: tempfile.TemporaryDirectory[str] | None = None
    staged_root: Path | None = None
    if restore_files:
        staged_files = tempfile.TemporaryDirectory(prefix="kanban-import-")
        staged_root = Path(staged_files.name)
        applied_file_count = _extract_backup_files(path, staged_root)

    try:
        if backup_before_import:
            pre_import_backup = _create_backup_file(kind="pre-import").filename

        audit_id = f"audit-{uuid.uuid4().hex}"
        request_id = f"kanban-backup-import-{uuid.uuid4().hex[:12]}"
        now = _utc_now()
        audit_row = {
            "audit_id": audit_id,
            "actor": "blueprints-api",
            "source_surface": "kanban-backups-api",
            "action": "import_kanban_backup",
            "target_ref": f"kanban_backup:{path.name}",
            "item_id": "",
            "parent_item_id": "",
            "created_at": now,
            "request_id": request_id,
            "run_id": request_id,
            "result": "ok",
            "source_hash": _sha256_file(path),
            "metadata_json": json.dumps(
                {
                    "filename": path.name,
                    "pre_import_backup": pre_import_backup,
                    "restore_files": restore_files,
                    "file_count": file_count,
                    "table_counts": table_counts,
                    "dry_run_totals": dry_run.get("totals", {}),
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
        }

        with get_conn() as conn:
            gen_before = get_gen(conn)
            gen_after = increment_gen(conn, "kanban-backup-import")
            table_counts = _import_table_rows(conn, table_data, gen_after, audit_row)

        if restore_files and staged_root is not None:
            applied_file_count = _replace_kanban_files_from_staged(staged_root)
    finally:
        if staged_files is not None:
            staged_files.cleanup()

    return {
        "ok": True,
        "filename": path.name,
        "applied": True,
        "restored_files": restore_files,
        "gen_before": gen_before,
        "gen_after": gen_after,
        "table_counts": table_counts,
        "file_count": applied_file_count if restore_files else file_count,
        "dry_run": dry_run,
        "pre_import_backup": pre_import_backup,
        "warnings": warnings,
    }
