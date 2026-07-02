#!/usr/bin/env python3
"""Run the durable PIM Email local-corpus backfill."""

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
        "PIM Email backfill must run through the Dockge stack. "
        "Use /xarta-node/.lone-wolf/stacks/pim-email/scripts/run-backfill.sh."
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


def _compact_backfill_summary(summary: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "artifact_types",
        "planned_messages",
        "processed_messages",
        "failed_messages",
        "raw_originals_verified",
        "raw_originals_failed",
        "security_completed",
        "security_already_completed",
        "security_failed",
        "sanitized_views_stored",
        "sanitized_views_already_current",
        "sanitized_views_failed",
        "external_images_captured",
        "external_images_stored",
        "external_images_pending",
        "external_images_unavailable",
        "external_images_failed",
        "external_images_blocked",
        "external_images_materialized_rows",
    )
    return {key: summary[key] for key in keys if key in summary}


async def _fetch_backfill_progress(store: Any, run_id: str) -> dict[str, Any]:
    conn = await store._connect()
    try:
        row = await conn.fetchrow(
            """
            SELECT status, processed_count, failed_count, started_at, finished_at, summary_json
            FROM pim_email_backfill_runs
            WHERE run_id = $1
            """,
            run_id,
        )
    finally:
        await conn.close()
    if not row:
        return {"run_id": run_id, "status": "not-created"}
    summary = _parse_summary(row["summary_json"])
    return {
        "run_id": run_id,
        "status": str(row["status"] or ""),
        "processed_count": int(row["processed_count"] or 0),
        "failed_count": int(row["failed_count"] or 0),
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "summary": _compact_backfill_summary(summary),
    }


async def _monitor_backfill_progress(
    store: Any,
    run_id: str,
    stop: asyncio.Event,
    *,
    interval_seconds: float = 30.0,
) -> None:
    while not stop.is_set():
        try:
            _log_event("backfill_progress", **(await _fetch_backfill_progress(store, run_id)))
        except Exception as exc:  # pragma: no cover - defensive log path
            _log_event(
                "backfill_progress_monitor_error",
                run_id=run_id,
                error_class=exc.__class__.__name__,
                error_message=str(exc),
            )
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import PgEmailStore

    store = PgEmailStore()
    _log_event(
        "backfill_cli_start",
        run_id=args.run_id,
        mailbox_id=args.mailbox_id,
        email_uid=args.email_uid,
        limit=args.limit,
        artifact=args.artifact,
        materialize_external_image_rows=bool(args.materialize_external_image_rows),
    )
    active_backfill_run_ids = _active_backfill_run_ids()
    if args.run_id:
        active_backfill_run_ids.add(str(args.run_id))
    await store.reconcile_orphaned_backfill_runs(
        active_run_ids=active_backfill_run_ids,
        reason="stack_backfill_start_process_set_reconciliation",
        mailbox_id=args.mailbox_id,
    )
    await store.reconcile_superseded_backfill_failures(mailbox_id=args.mailbox_id)
    stop = asyncio.Event()
    monitor_task: asyncio.Task[None] | None = None
    if args.run_id:
        monitor_task = asyncio.create_task(
            _monitor_backfill_progress(store, str(args.run_id), stop)
        )
    try:
        if args.materialize_external_image_rows:
            result = await store.materialize_external_image_derivative_rows(
                mailbox_id=args.mailbox_id,
                email_uid=args.email_uid,
                limit=args.limit,
                metadata={"source": "pim-email-backfill-cli-materialize"},
            )
            _log_event("backfill_materialize_complete", result=result)
            if not args.artifact:
                return {"ok": True, "materialize_external_image_rows": result}
        result = await store.run_backfill(
            mailbox_id=args.mailbox_id,
            email_uid=args.email_uid,
            limit=args.limit,
            artifact_types=args.artifact,
            run_id=args.run_id,
        )
        _log_event("backfill_cli_complete", result=result)
        return result
    finally:
        stop.set()
        if monitor_task is not None:
            await monitor_task


def _active_backfill_run_ids() -> set[str]:
    active: set[str] = set()
    for cmdline in Path("/proc").glob("[0-9]*/cmdline"):
        try:
            raw = cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except Exception:
            continue
        if "pim_email_backfill.py" not in raw:
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
    parser.add_argument("--mailbox-id", default=None)
    parser.add_argument("--email-uid", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--artifact",
        action="append",
        choices=["security", "sanitized_view", "external_images"],
        help="Artifact type to backfill; repeat for multiple. Defaults to all.",
    )
    parser.add_argument(
        "--materialize-external-image-rows",
        action="store_true",
        help="Create missing durable pending derivative rows for captured external image URLs.",
    )
    args = parser.parse_args()
    _load_env_file(Path(args.env_file))
    result = asyncio.run(_run(args))
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
