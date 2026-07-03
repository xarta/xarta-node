#!/usr/bin/env python3
"""Audit or remove obsolete PIM Email local-corpus artifacts.

This script intentionally runs only inside the pim-email Dockge worker. It does
not touch raw originals, current sanitized-raw artifacts, or shared image assets.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCRIPT = Path(__file__).resolve()
REPO_ROOT = SCRIPT.parents[2]
APP_ROOT = REPO_ROOT / "blueprints-app"


def _require_stack_runner() -> None:
    if os.environ.get("BLUEPRINTS_EMAIL_STACK_RUNNER") == "1":
        return
    raise SystemExit(
        "PIM Email obsolete-artifact cleanup must run through the Dockge stack. "
        "Use /xarta-node/.lone-wolf/stacks/pim-email/scripts/run-cleanup.sh."
    )


_require_stack_runner()

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


def _load_env_file(path: Path) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip("\"'")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _log_event(event: str, **payload: Any) -> None:
    body = {"ts": _utc_now(), "event": event, **payload}
    print(json.dumps(_json_ready(body), sort_keys=True, separators=(",", ":")), flush=True)


def _safe_content_path(root: Path, relpath: str) -> Path:
    rel = Path(str(relpath or ""))
    if rel.is_absolute() or ".." in rel.parts or not rel.name:
        raise ValueError(f"unsafe content relpath: {relpath!r}")
    path = root / rel
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"content path escapes root: {relpath!r}") from exc
    return path


def _relpath_for(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def _prune_empty_dirs(start: Path, stop: Path) -> int:
    removed = 0
    current = start
    stop = stop.resolve()
    while current.resolve() != stop and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        removed += 1
        current = current.parent
    return removed


def _has_assets_parent(path: Path, root: Path) -> bool:
    try:
        rel = path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return "assets" in rel.parts[:-1]


async def _referenced_asset_relpaths(conn: Any) -> set[str]:
    rows = await conn.fetch(
        """
        SELECT storage_relpath FROM pim_email_transformed_assets
        WHERE storage_relpath <> ''
        UNION
        SELECT storage_relpath FROM pim_email_external_image_derivatives
        WHERE storage_relpath <> ''
        UNION
        SELECT storage_relpath FROM pim_email_shared_assets
        WHERE storage_relpath <> ''
        """
    )
    return {str(row["storage_relpath"]) for row in rows if str(row["storage_relpath"] or "")}


async def _cleanup_obsolete_sanitized_views(
    conn: Any,
    *,
    root: Path,
    active_transform_version: str,
    active_policy_version: str,
    limit: int | None,
    apply: bool,
    require_current_replacement: bool,
    prune_empty_dirs: bool,
) -> dict[str, Any]:
    query_limit = max(1, int(limit)) if limit else None
    rows = await conn.fetch(
        """
        SELECT
            a.artifact_uid,
            a.email_uid,
            a.input_raw_sha256,
            a.sanitizer_policy_version,
            a.transform_version,
            a.storage_relpath,
            a.encrypted_size,
            EXISTS (
                SELECT 1
                FROM pim_email_sanitized_view_artifacts current_a
                WHERE current_a.mailbox_id = a.mailbox_id
                  AND current_a.email_uid = a.email_uid
                  AND current_a.input_raw_sha256 = a.input_raw_sha256
                  AND current_a.sanitizer_policy_version = $1
                  AND current_a.transform_version = $2
            ) AS current_replacement_exists,
            EXISTS (
                SELECT 1
                FROM pim_email_messages m
                WHERE m.email_uid = a.email_uid
                  AND m.raw_sha256 = a.input_raw_sha256
                  AND m.storage_relpath <> ''
            ) AS raw_original_exists
        FROM pim_email_sanitized_view_artifacts a
        WHERE a.transform_version <> $2
        ORDER BY a.updated_at ASC, a.artifact_uid ASC
        LIMIT COALESCE($3::int, 2147483647)
        """,
        active_policy_version,
        active_transform_version,
        query_limit,
    )
    summary = {
        "schema": "xarta.pim_email.cleanup_obsolete_sanitized_views.v1",
        "apply": bool(apply),
        "active_transform_version": active_transform_version,
        "active_policy_version": active_policy_version,
        "planned_rows": len(rows),
        "current_replacement_exists": 0,
        "raw_original_exists": 0,
        "skipped_missing_raw_original": 0,
        "skipped_missing_current_replacement": 0,
        "missing_files": 0,
        "deleted_rows": 0,
        "deleted_files": 0,
        "deleted_bytes": 0,
        "pruned_empty_dirs": 0,
        "errors": [],
    }
    for row in rows:
        has_current = bool(row["current_replacement_exists"])
        has_raw = bool(row["raw_original_exists"])
        summary["current_replacement_exists"] += int(has_current)
        summary["raw_original_exists"] += int(has_raw)
        if not has_raw:
            summary["skipped_missing_raw_original"] += 1
            continue
        if require_current_replacement and not has_current:
            summary["skipped_missing_current_replacement"] += 1
            continue
        relpath = str(row["storage_relpath"] or "")
        try:
            path = _safe_content_path(root, relpath)
        except ValueError as exc:
            summary["errors"].append(
                {
                    "artifact_uid": str(row["artifact_uid"]),
                    "storage_relpath": relpath,
                    "error": str(exc),
                }
            )
            continue
        file_size = path.stat().st_size if path.exists() else 0
        if not path.exists():
            summary["missing_files"] += 1
        if not apply:
            summary["deleted_rows"] += 1
            summary["deleted_files"] += int(path.exists())
            summary["deleted_bytes"] += file_size
            continue
        if path.exists():
            path.unlink()
            summary["deleted_files"] += 1
            summary["deleted_bytes"] += file_size
            if prune_empty_dirs:
                summary["pruned_empty_dirs"] += _prune_empty_dirs(path.parent, root)
        result = await conn.execute(
            """
            DELETE FROM pim_email_sanitized_view_artifacts
            WHERE artifact_uid = $1
              AND transform_version <> $2
            """,
            str(row["artifact_uid"]),
            active_transform_version,
        )
        if result.endswith(" 1"):
            summary["deleted_rows"] += 1
    return summary


async def _cleanup_legacy_image_assets(
    conn: Any,
    *,
    root: Path,
    limit: int | None,
    apply: bool,
    prune_empty_dirs: bool,
) -> dict[str, Any]:
    referenced = await _referenced_asset_relpaths(conn)
    candidate_limit = max(1, int(limit)) if limit else None
    summary = {
        "schema": "xarta.pim_email.cleanup_legacy_image_assets.v1",
        "apply": bool(apply),
        "referenced_asset_paths": len(referenced),
        "scanned_files": 0,
        "referenced_legacy_files": 0,
        "obsolete_files": 0,
        "deleted_files": 0,
        "deleted_bytes": 0,
        "pruned_empty_dirs": 0,
        "errors": [],
    }
    shared_root = root / "assets"
    for path in sorted(root.rglob("*.enc")):
        if not path.is_file():
            continue
        summary["scanned_files"] += 1
        try:
            relpath = _relpath_for(root, path)
        except ValueError as exc:
            summary["errors"].append({"path": str(path), "error": str(exc)})
            continue
        if not _has_assets_parent(path, root):
            continue
        if path.resolve().is_relative_to(shared_root.resolve()):
            continue
        if relpath in referenced:
            summary["referenced_legacy_files"] += 1
            continue
        file_size = path.stat().st_size
        summary["obsolete_files"] += 1
        summary["deleted_bytes"] += file_size
        if not apply:
            summary["deleted_files"] += 1
        else:
            path.unlink()
            summary["deleted_files"] += 1
            if prune_empty_dirs:
                summary["pruned_empty_dirs"] += _prune_empty_dirs(path.parent, root)
        if candidate_limit and summary["obsolete_files"] >= candidate_limit:
            break
    return summary


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import (
        SANITIZED_VIEW_POLICY_VERSION,
        SANITIZED_VIEW_TRANSFORM_VERSION,
        PgEmailStore,
        _email_content_root,
    )

    store = PgEmailStore()
    root = _email_content_root()
    artifacts = set(args.artifact or [])
    result: dict[str, Any] = {
        "schema": "xarta.pim_email.cleanup_obsolete_artifacts.result.v1",
        "apply": bool(args.apply),
        "content_root": str(root),
        "artifacts": sorted(artifacts),
    }
    conn = await store._connect()
    try:
        if "obsolete-sanitized-views" in artifacts:
            result["obsolete_sanitized_views"] = await _cleanup_obsolete_sanitized_views(
                conn,
                root=root,
                active_transform_version=SANITIZED_VIEW_TRANSFORM_VERSION,
                active_policy_version=SANITIZED_VIEW_POLICY_VERSION,
                limit=args.limit,
                apply=bool(args.apply),
                require_current_replacement=bool(args.require_current_sanitized_replacement),
                prune_empty_dirs=bool(args.prune_empty_dirs),
            )
        if "legacy-image-assets" in artifacts:
            result["legacy_image_assets"] = await _cleanup_legacy_image_assets(
                conn,
                root=root,
                limit=args.limit,
                apply=bool(args.apply),
                prune_empty_dirs=bool(args.prune_empty_dirs),
            )
    finally:
        await conn.close()
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Blueprints .env file to load before connecting to PIM Email storage.",
    )
    parser.add_argument(
        "--artifact",
        action="append",
        choices=("obsolete-sanitized-views", "legacy-image-assets"),
        help="Artifact bucket to audit/remove. Defaults to both buckets.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--require-current-sanitized-replacement",
        action="store_true",
        help="For obsolete sanitized views, require a current sanitized-raw replacement before deletion.",
    )
    parser.add_argument("--prune-empty-dirs", action="store_true")
    args = parser.parse_args()
    if not args.artifact:
        args.artifact = ["obsolete-sanitized-views", "legacy-image-assets"]
    _load_env_file(Path(args.env_file))
    _log_event(
        "cleanup_obsolete_artifacts_start",
        artifacts=args.artifact,
        limit=args.limit,
        apply=bool(args.apply),
        require_current_sanitized_replacement=bool(args.require_current_sanitized_replacement),
    )
    result = asyncio.run(_run(args))
    _log_event("cleanup_obsolete_artifacts_complete", result=result)
    print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
    has_errors = any(
        isinstance(value, dict) and value.get("errors")
        for key, value in result.items()
        if key not in {"schema", "apply", "content_root", "artifacts"}
    )
    return 1 if has_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
