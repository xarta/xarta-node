#!/usr/bin/env python3
"""Run the safe PIM Email mailbox downloader."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
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
        "PIM Email downloader must run through the Dockge stack. "
        "Use /xarta-node/.lone-wolf/stacks/pim-email/scripts/run-download.sh."
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
    return value


def _log_event(event: str, **payload: Any) -> None:
    body = {"ts": _utc_now(), "event": event, **payload}
    print(json.dumps(_json_ready(body), sort_keys=True, separators=(",", ":")), flush=True)


def _parse_summary(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {"raw_summary": value}
        return loaded if isinstance(loaded, dict) else {"summary": loaded}
    return {}


def _compact_download_summary(summary: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "folders_seen",
        "folders_downloaded",
        "target_folder_ignored",
        "planned_messages",
        "processed_messages",
        "stored_messages",
        "security_completed",
        "security_incomplete",
        "sanitized_views_stored",
        "external_image_derivatives_stored",
        "external_image_derivatives_pending",
        "external_image_derivatives_unavailable",
        "external_image_derivatives_failed",
        "external_image_derivatives_blocked",
        "moved_messages",
        "move_not_allowed",
        "move_blocked",
        "move_refused",
        "failed_messages",
        "remote_image_sources_seen",
    )
    return {key: summary[key] for key in keys if key in summary}


async def _fetch_download_progress(store: Any, run_id: str) -> dict[str, Any]:
    conn = await store._connect()
    try:
        row = await conn.fetchrow(
            """
            SELECT status, started_at, finished_at, summary_json
            FROM pim_email_download_runs
            WHERE run_id = $1
            """,
            run_id,
        )
        event_counts = await conn.fetchrow(
            """
            SELECT
                count(*) AS total,
                count(*) FILTER (WHERE status = 'error') AS errors,
                count(*) FILTER (WHERE event_type = 'message-stored') AS stored,
                count(*) FILTER (WHERE event_type = 'message-failed') AS failed,
                count(*) FILTER (WHERE event_type = 'remote-move-gate-blocked') AS move_blocked,
                count(*) FILTER (WHERE event_type = 'remote-move-refused') AS move_refused
            FROM pim_email_download_events
            WHERE run_id = $1
            """,
            run_id,
        )
        latest_event = await conn.fetchrow(
            """
            SELECT event_type, status, message, error_class, folder_name, email_uid,
                   imap_uid, created_at
            FROM pim_email_download_events
            WHERE run_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            run_id,
        )
        batch_counts = await conn.fetchrow(
            """
            SELECT
                count(*) AS batches,
                coalesce(sum(planned_count), 0) AS planned,
                coalesce(sum(processed_count), 0) AS processed,
                coalesce(sum(moved_count), 0) AS moved,
                coalesce(sum(failed_count), 0) AS failed
            FROM pim_email_download_batches
            WHERE run_id = $1
            """,
            run_id,
        )
    finally:
        await conn.close()
    if not row:
        return {"run_id": run_id, "status": "not-created"}
    summary = _parse_summary(row["summary_json"])
    latest = dict(latest_event) if latest_event else {}
    return {
        "run_id": run_id,
        "status": str(row["status"] or ""),
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "summary": _compact_download_summary(summary),
        "events": dict(event_counts) if event_counts else {},
        "batches": dict(batch_counts) if batch_counts else {},
        "latest_event": latest,
    }


async def _monitor_download_progress(
    store: Any,
    run_id: str,
    stop: asyncio.Event,
    *,
    interval_seconds: float = 30.0,
) -> None:
    while not stop.is_set():
        try:
            _log_event("download_progress", **(await _fetch_download_progress(store, run_id)))
        except Exception as exc:  # pragma: no cover - defensive log path
            _log_event(
                "download_progress_monitor_error",
                run_id=run_id,
                error_class=exc.__class__.__name__,
                error_message=str(exc),
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import PgEmailStore, download_mailbox

    store = PgEmailStore()
    _log_event(
        "download_cli_start",
        run_id=args.run_id,
        mailbox_id=args.mailbox_id,
        apply_remote_moves=bool(args.apply_remote_moves),
        downloaded_folder=args.downloaded_folder,
        folder=args.folder,
        limit_per_folder=args.limit_per_folder,
        max_messages=args.max_messages,
        convergence_passes=args.convergence_passes,
    )
    active_download_run_ids = _active_download_run_ids()
    if args.run_id:
        active_download_run_ids.add(str(args.run_id))
    await store.reconcile_orphaned_download_runs(
        active_run_ids=active_download_run_ids,
        reason="stack_download_start_process_set_reconciliation",
        mailbox_id=args.mailbox_id,
    )
    mailbox = await store.get_mailbox(args.mailbox_id)
    stop = asyncio.Event()
    monitor_task: asyncio.Task[None] | None = None
    if args.run_id:
        monitor_task = asyncio.create_task(
            _monitor_download_progress(store, str(args.run_id), stop)
        )
    try:
        result = await download_mailbox(
            mailbox,
            store=store,
            run_id=args.run_id,
            apply_remote_moves=args.apply_remote_moves,
            downloaded_folder=args.downloaded_folder,
            folder_allowlist=args.folder,
            limit_per_folder=args.limit_per_folder,
            max_messages=args.max_messages,
            convergence_passes=args.convergence_passes,
            include_special_use=True,
            security_mode="run",
        )
        _log_event("download_cli_complete", result=result)
        return result
    finally:
        stop.set()
        if monitor_task is not None:
            await monitor_task


def _active_download_run_ids() -> set[str]:
    active: set[str] = set()
    for cmdline in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            raw = cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except Exception:
            continue
        if "pim_email_download_mailbox.py" not in raw:
            continue
        match = re.search(r"(?:^|\s)--run-id\s+(\S+)", raw)
        if match:
            active.add(match.group(1))
    return active


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Blueprints .env file to load before connecting to PIM Email storage.",
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--mailbox-id", default=None)
    parser.add_argument("--apply-remote-moves", action="store_true")
    parser.add_argument("--downloaded-folder", default=None)
    parser.add_argument("--folder", action="append", help="Folder allowlist entry; repeatable.")
    parser.add_argument("--limit-per-folder", type=int, default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--convergence-passes", type=int, default=1)
    args = parser.parse_args()
    _load_env_file(Path(args.env_file))
    result = asyncio.run(_run(args))
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
