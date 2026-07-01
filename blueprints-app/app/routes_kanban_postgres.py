"""Postgres-native Kanban backup, import, and distribution routes."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from . import config as cfg
from .kanban_datastore import (
    ACTIVE_STORE_POSTGRES,
    KANBAN_DATASTORE_TABLES,
    kanban_datastore_status,
)
from .kanban_postgres import KanbanPostgresError, postgres_candidate_connection

router = APIRouter(prefix="/personal/kanban/postgres", tags=["personal"])

POSTGRES_STATUS_SCHEMA = "xarta.kanban.postgres.status.v1"
POSTGRES_EXPORT_SCHEMA = "xarta.kanban.postgres.export.v1"
POSTGRES_EXPORT_VALIDATION_SCHEMA = "xarta.kanban.postgres.export.validation.v1"
POSTGRES_IMPORT_SCHEMA = "xarta.kanban.postgres.import.v1"
POSTGRES_DISTRIBUTION_REQUEST_SCHEMA = "xarta.kanban.postgres.distribution_request.v1"

_SAFE_EXPORT_NAME = re.compile(
    r"^\d{4}-\d{2}-\d{2}-\d{6}-kanban-postgres-(manual|pre-import)-[a-f0-9]{8}\.sql$"
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class KanbanPostgresExportEntry(BaseModel):
    filename: str
    size_bytes: int
    created_at: str
    export_id: str = ""
    kind: str = ""
    sha256: str = ""
    table_counts: dict[str, int] = Field(default_factory=dict)
    table_count: int = 0
    row_count: int = 0
    validation: dict[str, Any] = Field(default_factory=dict)


class KanbanPostgresExportCreatedResponse(BaseModel):
    ok: bool
    export: KanbanPostgresExportEntry
    manifest: dict[str, Any]
    warnings: list[str] = Field(default_factory=list)


class KanbanPostgresValidationResponse(BaseModel):
    ok: bool
    filename: str
    manifest: dict[str, Any]
    restored_table_counts: dict[str, int]
    row_count: int
    warnings: list[str] = Field(default_factory=list)


class KanbanPostgresImportResponse(BaseModel):
    ok: bool
    filename: str
    applied: bool
    validation: dict[str, Any]
    table_counts_before: dict[str, int]
    table_counts_after: dict[str, int]
    pre_import_export: str | None = None
    warnings: list[str] = Field(default_factory=list)


class KanbanPostgresDistributionRequest(BaseModel):
    target_node_id: str | None = None
    targets: str | None = None
    dry_run: bool = False


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _postgres_container() -> str:
    return os.environ.get("BLUEPRINTS_KANBAN_POSTGRES_CONTAINER", "blueprints-kanban-postgres")


def _postgres_user() -> str:
    return os.environ.get("BLUEPRINTS_KANBAN_POSTGRES_USER", "blueprints_kanban")


def _postgres_db() -> str:
    return os.environ.get("BLUEPRINTS_KANBAN_POSTGRES_DB", "blueprints_kanban")


def _distribution_helper() -> Path:
    return Path(
        os.environ.get(
            "BLUEPRINTS_KANBAN_POSTGRES_DISTRIBUTE_HELPER",
            "/root/xarta-node/.xarta/.agents/bin/xarta-kanban-postgres-distribute",
        )
    )


def _docker_exec_base(*, interactive: bool = False) -> list[str]:
    command = ["docker", "exec"]
    if interactive:
        command.append("-i")
    command.append(_postgres_container())
    return command


def _export_dir() -> Path:
    path = Path(cfg.KANBAN_POSTGRES_EXPORT_DIR).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_export_path(filename: str) -> Path:
    clean = filename.strip()
    if not _SAFE_EXPORT_NAME.match(clean):
        raise HTTPException(status_code=400, detail="Invalid Kanban Postgres export filename.")
    root = _export_dir()
    path = (root / clean).resolve()
    if path.parent != root:
        raise HTTPException(status_code=400, detail="Invalid Kanban Postgres export path.")
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"Kanban Postgres export not found: {clean}")
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise HTTPException(status_code=500, detail=f"Unsafe Kanban Postgres table name: {value}")
    return f'"{value}"'


def _run_command(
    command: list[str],
    *,
    timeout: int = 120,
    stdin_path: Path | None = None,
    stdout_path: Path | None = None,
) -> subprocess.CompletedProcess[bytes]:
    stdin_handle = stdin_path.open("rb") if stdin_path else None
    stdout_handle = stdout_path.open("wb") if stdout_path else subprocess.PIPE
    try:
        return subprocess.run(
            command,
            stdin=stdin_handle,
            stdout=stdout_handle,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    finally:
        if stdin_handle is not None:
            stdin_handle.close()
        if stdout_path is not None and stdout_handle is not None:
            stdout_handle.close()


def _result_text(result: subprocess.CompletedProcess[bytes], stream: str) -> str:
    value = result.stderr if stream == "stderr" else result.stdout
    if value is None:
        return ""
    return value.decode("utf-8", errors="replace").strip()


def _raise_command_error(
    result: subprocess.CompletedProcess[bytes],
    *,
    action: str,
    status_code: int = 502,
) -> None:
    if result.returncode == 0:
        return
    detail = {
        "action": action,
        "returncode": result.returncode,
        "stderr": _result_text(result, "stderr")[-4000:],
        "stdout": _result_text(result, "stdout")[-4000:],
    }
    raise HTTPException(status_code=status_code, detail=detail)


def _postgres_table_counts(database_url: str | None = None) -> dict[str, int]:
    url = database_url or cfg.KANBAN_DATASTORE_CONFIG.candidate_database_url
    if not url:
        raise HTTPException(
            status_code=503, detail="Kanban Postgres DATABASE_URL is not configured."
        )
    conn = None
    try:
        conn = postgres_candidate_connection(url)
        counts: dict[str, int] = {}
        for table in KANBAN_DATASTORE_TABLES:
            row = conn.execute(
                f"SELECT COUNT(*) AS row_count FROM {_quote_identifier(table)}"
            ).fetchone()
            counts[table] = int(row["row_count"] if row else 0)
        return counts
    except KanbanPostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Kanban Postgres count failed: {exc}") from exc
    finally:
        if conn is not None:
            conn.close()


def _temp_database_counts(database_name: str) -> dict[str, int]:
    union_parts = [
        f"SELECT '{table}' AS table_name, COUNT(*)::bigint AS row_count FROM {_quote_identifier(table)}"
        for table in KANBAN_DATASTORE_TABLES
    ]
    sql = " UNION ALL ".join(union_parts)
    result = _run_command(
        [
            *_docker_exec_base(),
            "psql",
            "-At",
            "-F",
            "\t",
            "-U",
            _postgres_user(),
            "-d",
            database_name,
            "-c",
            sql,
        ],
        timeout=60,
    )
    _raise_command_error(result, action="count temporary Kanban Postgres validation database")
    counts: dict[str, int] = {}
    for line in _result_text(result, "stdout").splitlines():
        if not line.strip():
            continue
        table, value = line.split("\t", 1)
        counts[table] = int(value)
    missing = [table for table in KANBAN_DATASTORE_TABLES if table not in counts]
    if missing:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Kanban Postgres export is missing datastore tables.",
                "missing": missing,
            },
        )
    return counts


def _load_manifest(path: Path) -> dict[str, Any]:
    manifest_path = path.with_suffix(path.suffix + ".json")
    if not manifest_path.exists():
        raise HTTPException(
            status_code=422, detail=f"Export manifest missing: {manifest_path.name}"
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=422, detail=f"Export manifest is invalid JSON: {exc}"
        ) from exc
    if manifest.get("schema") != POSTGRES_EXPORT_SCHEMA:
        raise HTTPException(
            status_code=422, detail="Kanban Postgres export manifest schema mismatch."
        )
    expected_sha = str(manifest.get("sha256") or "")
    actual_sha = _sha256_file(path)
    if expected_sha and expected_sha != actual_sha:
        raise HTTPException(
            status_code=422,
            detail={
                "message": "Kanban Postgres export checksum mismatch.",
                "expected_sha256": expected_sha,
                "actual_sha256": actual_sha,
            },
        )
    manifest["sha256"] = actual_sha
    return manifest


def _entry_from_manifest(
    path: Path, manifest: dict[str, Any] | None = None
) -> KanbanPostgresExportEntry:
    manifest = manifest or _load_manifest(path)
    counts = {
        str(key): int(value or 0) for key, value in (manifest.get("table_counts") or {}).items()
    }
    return KanbanPostgresExportEntry(
        filename=path.name,
        size_bytes=path.stat().st_size,
        created_at=str(manifest.get("created_at") or ""),
        export_id=str(manifest.get("export_id") or ""),
        kind=str(manifest.get("kind") or ""),
        sha256=str(manifest.get("sha256") or ""),
        table_counts=counts,
        table_count=len(counts),
        row_count=sum(counts.values()),
        validation=manifest.get("validation") or {},
    )


def _list_exports() -> list[KanbanPostgresExportEntry]:
    entries: list[KanbanPostgresExportEntry] = []
    for path in sorted(
        _export_dir().glob("*.sql"), key=lambda item: item.stat().st_mtime, reverse=True
    ):
        if not _SAFE_EXPORT_NAME.match(path.name):
            continue
        try:
            entries.append(_entry_from_manifest(path))
        except HTTPException:
            entries.append(
                KanbanPostgresExportEntry(
                    filename=path.name,
                    size_bytes=path.stat().st_size,
                    created_at=datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                    .replace(microsecond=0)
                    .isoformat(),
                    validation={"ok": False, "error": "manifest-invalid-or-missing"},
                )
            )
    return entries


def _health() -> dict[str, Any]:
    result = _run_command(
        [
            *_docker_exec_base(),
            "pg_isready",
            "-U",
            _postgres_user(),
            "-d",
            _postgres_db(),
        ],
        timeout=15,
    )
    return {
        "ok": result.returncode == 0,
        "container": _postgres_container(),
        "database": _postgres_db(),
        "user": _postgres_user(),
        "pg_isready_stdout": _result_text(result, "stdout"),
        "pg_isready_stderr": _result_text(result, "stderr"),
    }


def _owner_warning() -> str:
    return (
        "Postgres imports and distribution affect the dedicated "
        "blueprints-kanban-postgres database. Validate exports before import and "
        "use distribution only from the canonical owner."
    )


def _ensure_postgres_active() -> None:
    if cfg.KANBAN_DATASTORE_CONFIG.active_store != ACTIVE_STORE_POSTGRES:
        raise HTTPException(
            status_code=409,
            detail="Kanban Postgres export/import requires active_store=postgres.",
        )


def _ensure_owner() -> None:
    config = cfg.KANBAN_DATASTORE_CONFIG
    if (
        config.current_node_id
        and config.postgres_owner_node_id
        and (config.current_node_id != config.postgres_owner_node_id)
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "This node is a Kanban Postgres read replica. "
                f"Run imports/distribution from {config.postgres_owner_node_id}."
            ),
        )


def _node_options() -> list[dict[str, Any]]:
    nodes = []
    for node in getattr(cfg, "NODES_DATA", []) or []:
        if not node.get("active", False):
            continue
        node_id = str(node.get("node_id") or "")
        if not node_id:
            continue
        nodes.append(
            {
                "node_id": node_id,
                "display_name": str(node.get("display_name") or node_id),
                "is_current": node_id == cfg.KANBAN_DATASTORE_CONFIG.current_node_id,
                "is_owner": node_id == cfg.KANBAN_DATASTORE_CONFIG.postgres_owner_node_id,
            }
        )
    return nodes


@router.get("/status")
def get_kanban_postgres_status() -> dict[str, Any]:
    datastore = kanban_datastore_status(cfg.KANBAN_DATASTORE_CONFIG)
    health = _health()
    counts: dict[str, int] = {}
    count_error = ""
    try:
        counts = _postgres_table_counts()
    except HTTPException as exc:
        count_error = str(exc.detail)
    exports = _list_exports()
    return {
        "ok": health["ok"] and not count_error,
        "schema": POSTGRES_STATUS_SCHEMA,
        "datastore": datastore,
        "current_node_id": cfg.KANBAN_DATASTORE_CONFIG.current_node_id,
        "owner_node_id": cfg.KANBAN_DATASTORE_CONFIG.postgres_owner_node_id,
        "role": datastore.get("distribution", {}).get("this_node_role", ""),
        "health": health,
        "table_counts": counts,
        "table_count": len(counts),
        "row_count": sum(counts.values()),
        "count_error": count_error,
        "export_dir": str(_export_dir()),
        "exports": [entry.model_dump() for entry in exports],
        "latest_export": exports[0].model_dump() if exports else None,
        "nodes": _node_options(),
        "warnings": [_owner_warning()],
    }


@router.get("/exports")
def list_kanban_postgres_exports() -> dict[str, Any]:
    exports = _list_exports()
    return {
        "ok": True,
        "schema": "xarta.kanban.postgres.exports.v1",
        "export_dir": str(_export_dir()),
        "exports": [entry.model_dump() for entry in exports],
        "latest_export": exports[0].model_dump() if exports else None,
        "warnings": [_owner_warning()],
    }


@router.post("/exports")
def create_kanban_postgres_export(
    kind: str = Query(default="manual", pattern="^(manual|pre-import)$"),
) -> dict[str, Any]:
    _ensure_postgres_active()
    now = _utc_now()
    export_id = uuid.uuid4().hex
    filename = (
        f"{datetime.now(tz=timezone.utc).strftime('%Y-%m-%d-%H%M%S')}"
        f"-kanban-postgres-{kind}-{export_id[:8]}.sql"
    )
    path = (_export_dir() / filename).resolve()
    if path.parent != _export_dir():
        raise HTTPException(
            status_code=500, detail="Kanban Postgres export path escaped export dir."
        )

    counts_before = _postgres_table_counts()
    result = _run_command(
        [
            *_docker_exec_base(),
            "pg_dump",
            "--clean",
            "--if-exists",
            "--no-owner",
            "--no-privileges",
            "-U",
            _postgres_user(),
            "-d",
            _postgres_db(),
        ],
        timeout=180,
        stdout_path=path,
    )
    try:
        _raise_command_error(result, action="create Kanban Postgres export")
    except HTTPException:
        path.unlink(missing_ok=True)
        raise

    sha = _sha256_file(path)
    manifest = {
        "ok": True,
        "schema": POSTGRES_EXPORT_SCHEMA,
        "export_id": export_id,
        "kind": kind,
        "created_at": now,
        "filename": filename,
        "size_bytes": path.stat().st_size,
        "sha256": sha,
        "node_id": cfg.NODE_ID,
        "node_name": cfg.NODE_NAME,
        "owner_node_id": cfg.KANBAN_DATASTORE_CONFIG.postgres_owner_node_id,
        "current_node_role": kanban_datastore_status(cfg.KANBAN_DATASTORE_CONFIG)
        .get("distribution", {})
        .get("this_node_role", ""),
        "database": _postgres_db(),
        "container": _postgres_container(),
        "table_counts": counts_before,
        "row_count": sum(counts_before.values()),
        "included_tables": list(KANBAN_DATASTORE_TABLES),
        "storage": "postgres",
        "sqlite_kanban_rows_included": False,
        "sqlite_backup_package": False,
        "warning": _owner_warning(),
    }
    path.with_suffix(path.suffix + ".json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    entry = _entry_from_manifest(path, manifest)
    return KanbanPostgresExportCreatedResponse(
        ok=True,
        export=entry,
        manifest=manifest,
        warnings=[_owner_warning()],
    ).model_dump()


@router.get("/exports/{filename}", include_in_schema=False)
def download_kanban_postgres_export(filename: str) -> FileResponse:
    _ensure_postgres_active()
    path = _safe_export_path(filename)
    _load_manifest(path)
    return FileResponse(path, filename=path.name, media_type="application/sql")


def _validate_export_file(path: Path) -> dict[str, Any]:
    manifest = _load_manifest(path)
    temp_db = f"kanban_validate_{uuid.uuid4().hex[:16]}"
    created = False
    try:
        create_result = _run_command(
            [*_docker_exec_base(), "createdb", "-U", _postgres_user(), temp_db],
            timeout=60,
        )
        _raise_command_error(create_result, action="create Kanban Postgres validation database")
        created = True
        restore_result = _run_command(
            [
                *_docker_exec_base(interactive=True),
                "psql",
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                _postgres_user(),
                "-d",
                temp_db,
            ],
            timeout=300,
            stdin_path=path,
        )
        _raise_command_error(
            restore_result,
            action="restore Kanban Postgres export into validation database",
        )
        counts = _temp_database_counts(temp_db)
        payload = KanbanPostgresValidationResponse(
            ok=True,
            filename=path.name,
            manifest=manifest,
            restored_table_counts=counts,
            row_count=sum(counts.values()),
            warnings=[_owner_warning()],
        ).model_dump()
        payload["schema"] = POSTGRES_EXPORT_VALIDATION_SCHEMA
        return payload
    finally:
        if created:
            _run_command(
                [
                    *_docker_exec_base(),
                    "dropdb",
                    "--if-exists",
                    "-U",
                    _postgres_user(),
                    temp_db,
                ],
                timeout=60,
            )


@router.get("/exports/{filename}/validate")
def validate_kanban_postgres_export(filename: str) -> dict[str, Any]:
    _ensure_postgres_active()
    return _validate_export_file(_safe_export_path(filename))


@router.post("/exports/{filename}/import")
def import_kanban_postgres_export(
    filename: str,
    apply: bool = Query(default=False),
    backup_before_import: bool = Query(default=True),
) -> dict[str, Any]:
    _ensure_postgres_active()
    _ensure_owner()
    path = _safe_export_path(filename)
    before = _postgres_table_counts()
    validation = _validate_export_file(path)
    pre_import_export: str | None = None
    after = before
    if apply:
        if backup_before_import:
            created = create_kanban_postgres_export(kind="pre-import")
            pre_import_export = created.get("export", {}).get("filename")
        result = _run_command(
            [
                *_docker_exec_base(interactive=True),
                "psql",
                "--single-transaction",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                _postgres_user(),
                "-d",
                _postgres_db(),
            ],
            timeout=300,
            stdin_path=path,
        )
        _raise_command_error(result, action="import Kanban Postgres export")
        after = _postgres_table_counts()
    payload = KanbanPostgresImportResponse(
        ok=True,
        filename=filename,
        applied=apply,
        validation=validation,
        table_counts_before=before,
        table_counts_after=after,
        pre_import_export=pre_import_export,
        warnings=[_owner_warning()],
    ).model_dump()
    payload["schema"] = POSTGRES_IMPORT_SCHEMA
    return payload


@router.post("/distribute")
def distribute_kanban_postgres(request: KanbanPostgresDistributionRequest) -> dict[str, Any]:
    _ensure_postgres_active()
    _ensure_owner()
    target = (request.target_node_id or request.targets or "peers").strip()
    if not target:
        target = "peers"
    if target not in {"peers", "all"}:
        valid_nodes = {node["node_id"] for node in _node_options()}
        requested = {part.strip() for part in target.split(",") if part.strip()}
        unknown = sorted(requested - valid_nodes)
        if unknown:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Unknown Kanban Postgres distribution target.",
                    "unknown": unknown,
                },
            )
    helper = _distribution_helper()
    if not helper.exists():
        raise HTTPException(status_code=500, detail=f"Distribution helper is missing: {helper}")
    command = [
        str(helper),
        "--owner",
        cfg.KANBAN_DATASTORE_CONFIG.postgres_owner_node_id or cfg.NODE_ID,
        "--targets",
        target,
    ]
    if request.dry_run:
        command.append("--dry-run")
    result = _run_command(command, timeout=900)
    _raise_command_error(result, action="distribute Kanban Postgres snapshot", status_code=500)
    stdout = _result_text(result, "stdout")
    parsed: dict[str, Any] | None = None
    for index, char in enumerate(stdout):
        if char != "{":
            continue
        try:
            parsed = json.loads(stdout[index:])
            break
        except json.JSONDecodeError:
            continue
    return {
        "ok": True,
        "schema": POSTGRES_DISTRIBUTION_REQUEST_SCHEMA,
        "target": target,
        "dry_run": request.dry_run,
        "command": command,
        "result": parsed,
        "stdout": stdout[-12000:],
        "stderr": _result_text(result, "stderr")[-4000:],
        "warnings": [_owner_warning(), "Distribution is Postgres-to-Postgres only."],
    }
