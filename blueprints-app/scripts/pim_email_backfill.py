#!/usr/bin/env python3
"""Run the durable PIM Email local-corpus backfill."""

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


def _reexec_repo_venv() -> None:
    venv_root = REPO_ROOT / ".venv"
    venv_python = REPO_ROOT / ".venv" / "bin" / "python"
    if not venv_python.exists():
        return
    try:
        current_prefix = Path(sys.prefix).resolve()
        target_prefix = venv_root.resolve()
    except OSError:
        current_prefix = Path(sys.prefix)
        target_prefix = venv_root
    if current_prefix != target_prefix:
        os.execv(str(venv_python), [str(venv_python), str(SCRIPT), *sys.argv[1:]])


_reexec_repo_venv()

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
    return await store.run_backfill(
        mailbox_id=args.mailbox_id,
        email_uid=args.email_uid,
        limit=args.limit,
        artifact_types=args.artifact,
        run_id=args.run_id,
    )


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
    args = parser.parse_args()
    _load_env_file(Path(args.env_file))
    result = asyncio.run(_run(args))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
