#!/usr/bin/env python3
"""Run the durable PIM Email local-corpus backfill."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
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


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import PgEmailStore

    store = PgEmailStore()
    active_backfill_run_ids = _active_backfill_run_ids()
    if args.run_id:
        active_backfill_run_ids.add(str(args.run_id))
    await store.reconcile_orphaned_backfill_runs(
        active_run_ids=active_backfill_run_ids,
        reason="stack_backfill_start_process_set_reconciliation",
        mailbox_id=args.mailbox_id,
    )
    await store.reconcile_superseded_backfill_failures(mailbox_id=args.mailbox_id)
    if args.materialize_external_image_rows:
        result = await store.materialize_external_image_derivative_rows(
            mailbox_id=args.mailbox_id,
            email_uid=args.email_uid,
            limit=args.limit,
            metadata={"source": "pim-email-backfill-cli-materialize"},
        )
        if not args.artifact:
            return {"ok": True, "materialize_external_image_rows": result}
    return await store.run_backfill(
        mailbox_id=args.mailbox_id,
        email_uid=args.email_uid,
        limit=args.limit,
        artifact_types=args.artifact,
        run_id=args.run_id,
    )


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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
