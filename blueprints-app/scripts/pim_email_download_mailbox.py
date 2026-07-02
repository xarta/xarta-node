#!/usr/bin/env python3
"""Run the safe PIM Email mailbox downloader."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
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


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from app.pim_email import PgEmailStore, download_mailbox

    store = PgEmailStore()
    mailbox = await store.get_mailbox(args.mailbox_id)
    return await download_mailbox(
        mailbox,
        store=store,
        apply_remote_moves=args.apply_remote_moves,
        downloaded_folder=args.downloaded_folder,
        folder_allowlist=args.folder,
        limit_per_folder=args.limit_per_folder,
        max_messages=args.max_messages,
        convergence_passes=args.convergence_passes,
        include_special_use=True,
        security_mode="run",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Blueprints .env file to load before connecting to PIM Email storage.",
    )
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
