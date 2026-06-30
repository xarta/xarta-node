#!/usr/bin/env python3
"""Run the configured private PIM Email mailbox bootstrap helper."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = APP_ROOT.parent
HELPER_ENV = "BLUEPRINTS_EMAIL_BOOTSTRAP_HELPER"


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _helper_path() -> Path:
    raw = os.environ.get(HELPER_ENV, "").strip()
    if not raw:
        raise SystemExit(
            f"{HELPER_ENV} is not configured. Set it to an executable helper outside the public repo."
        )
    helper = Path(raw).expanduser()
    if not helper.exists():
        raise SystemExit(f"{HELPER_ENV} does not point to an existing helper.")
    if not os.access(helper, os.X_OK):
        raise SystemExit(f"{HELPER_ENV} does not point to an executable helper.")
    return helper


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        default=str(REPO_ROOT / ".env"),
        help="Blueprints .env file to load before resolving the private helper.",
    )
    args, helper_args = parser.parse_known_args()

    _load_env_file(Path(args.env_file))
    helper = _helper_path()
    result = subprocess.run([sys.executable, str(helper), *helper_args], check=False)
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
