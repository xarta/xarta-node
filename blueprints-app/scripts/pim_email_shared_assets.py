#!/usr/bin/env python3
"""Migrate PIM Email transformed image derivatives into shared encrypted assets."""

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
        "PIM Email shared-asset migration must run through the Dockge stack. "
        "Use /xarta-node/.lone-wolf/stacks/pim-email/scripts/run-shared-assets.sh."
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


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import PgEmailStore

    store = PgEmailStore()
    batch_limit = max(1, min(int(args.batch_size or args.limit or 500), 5000))
    aggregate = {
        "schema": "xarta.pim_email.shared_asset_migration.aggregate.v1",
        "mailbox_id": args.mailbox_id,
        "batch_limit": batch_limit,
        "repeat_until_idle": bool(args.repeat_until_idle),
        "batches_completed": 0,
        "copied": 0,
        "updated_asset_refs": 0,
        "updated_derivative_refs": 0,
        "failed": 0,
        "stopped_reason": "",
    }
    _log_event("shared_asset_migration_start", aggregate=aggregate)
    while True:
        if args.max_batches is not None and aggregate["batches_completed"] >= max(
            1, int(args.max_batches)
        ):
            aggregate["stopped_reason"] = "max_batches"
            break
        result = await store.migrate_existing_transformed_assets_to_shared_store(
            mailbox_id=args.mailbox_id,
            limit=batch_limit,
        )
        aggregate["batches_completed"] += 1
        for key in ("copied", "updated_asset_refs", "updated_derivative_refs", "failed"):
            aggregate[key] += int(result.get(key) or 0)
        _log_event(
            "shared_asset_migration_batch_complete",
            batch_index=aggregate["batches_completed"],
            result=result,
            aggregate=aggregate,
        )
        if int(result.get("planned") or 0) <= 0:
            aggregate["stopped_reason"] = "idle"
            break
        if not args.repeat_until_idle:
            aggregate["stopped_reason"] = "single_batch"
            break
        if args.idle_sleep_seconds and float(args.idle_sleep_seconds) > 0:
            await asyncio.sleep(float(args.idle_sleep_seconds))
    return {"ok": aggregate["failed"] == 0, "aggregate": aggregate}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Blueprints .env file to load before connecting to PIM Email storage.",
    )
    parser.add_argument("--mailbox-id", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--repeat-until-idle", action="store_true")
    parser.add_argument("--idle-sleep-seconds", type=float, default=0.0)
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args()
    _load_env_file(Path(args.env_file))
    result = asyncio.run(_run(args))
    _log_event("shared_asset_migration_complete", result=result)
    print(json.dumps(_json_ready(result), indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
