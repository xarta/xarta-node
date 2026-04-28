#!/usr/bin/env python3
"""Run the local docs health gauntlet from one command."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path("/root/xarta-node")
DOCS_SCRIPTS = ROOT / ".xarta" / ".claude" / "skills" / "docs-operations" / "scripts"
BLUEPRINTS_SCRIPTS = ROOT / "blueprints-app" / "scripts"


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip("\"'")


def api_headers() -> dict[str, str]:
    secret = os.environ.get("BLUEPRINTS_API_SECRET", "").strip()
    if not secret:
        return {}
    import hashlib
    import hmac

    token = hmac.new(
        bytes.fromhex(secret),
        str(int(time.time()) // 5).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {"x-api-token": token}


def run_step(
    name: str,
    command: list[str],
    *,
    cwd: Path | None = None,
    allow_zero_error_warnings: bool = False,
) -> bool:
    print(f"\n== {name} ==")
    print(" ".join(command))
    proc = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=allow_zero_error_warnings,
    )
    if allow_zero_error_warnings:
        if proc.stdout:
            print(proc.stdout, end="")
        if proc.stderr:
            print(proc.stderr, end="", file=sys.stderr)
    if proc.returncode == 0:
        print(f"ok {name}")
        return True
    combined = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    if allow_zero_error_warnings and "0 errors" in combined:
        print(f"ok {name} with warnings")
        return True
    print(f"failed {name}: exit {proc.returncode}", file=sys.stderr)
    return False


def post_json(url: str, payload: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json", **api_headers()}
    request = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return False, {"error": str(exc)}
    return bool(body.get("ok", True)), body


def get_json(url: str) -> tuple[bool, dict[str, Any]]:
    request = urllib.request.Request(url, headers=api_headers(), method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return False, {"error": str(exc)}
    return bool(body.get("ok", True)), body


def api_step(name: str, method: str, url: str, payload: dict[str, Any] | None = None) -> bool:
    print(f"\n== {name} ==")
    print(f"{method} {url}")
    ok, body = post_json(url, payload or {}) if method == "POST" else get_json(url)
    if ok:
        print(
            json.dumps(
                {
                    k: body.get(k)
                    for k in ("ok", "status", "quality_status", "summary", "metrics")
                    if k in body
                },
                indent=2,
            )
        )
        print(f"ok {name}")
        return True
    print(json.dumps(body, indent=2), file=sys.stderr)
    print(f"failed {name}", file=sys.stderr)
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", default=os.getenv("BLUEPRINTS_API_URL", "http://127.0.0.1:8080")
    )
    parser.add_argument(
        "--turbovec-url", default=os.getenv("TURBOVEC_DOCS_URL", "http://127.0.0.1:19080")
    )
    parser.add_argument(
        "--worker-url", default=os.getenv("NULLCLAW_DOCS_SEARCH_URL", "http://127.0.0.1:19081")
    )
    parser.add_argument("--docs-scripts", default=str(DOCS_SCRIPTS))
    parser.add_argument("--skip-ui-contract", action="store_true")
    args = parser.parse_args()

    load_env(ROOT / ".env")
    docs_scripts = Path(args.docs_scripts)
    base_url = args.base_url.rstrip("/")
    failures = 0

    steps = [
        (
            "docs lint",
            [
                sys.executable,
                str(docs_scripts / "docs-lint.py"),
                "--scope",
                "lone-wolf-docs",
                "--checks",
                "all",
                "--summary",
            ],
        ),
        (
            "DB sync dry-run",
            [
                sys.executable,
                str(docs_scripts / "docs-sync-to-db.py"),
                "--dry-run",
            ],
        ),
        (
            "stale reference exceptions",
            [
                sys.executable,
                str(docs_scripts / "docs-stale-reference-check.py"),
                "--summary",
            ],
        ),
    ]
    for name, command in steps:
        failures += (
            0
            if run_step(
                name,
                command,
                cwd=ROOT,
                allow_zero_error_warnings=name == "docs lint",
            )
            else 1
        )

    failures += (
        0
        if api_step(
            "TurboVec sync", "POST", f"{base_url}/api/v1/docs/search/sync", {"force": False}
        )
        else 1
    )
    failures += (
        0
        if run_step(
            "graph smoke",
            [
                sys.executable,
                str(BLUEPRINTS_SCRIPTS / "smoke_nullclaw_docs_graph.py"),
                "--turbovec-url",
                args.turbovec_url,
                "--worker-url",
                args.worker_url,
            ],
            cwd=ROOT,
        )
        else 1
    )
    failures += (
        0 if api_step("status endpoint", "GET", f"{base_url}/api/v1/docs/search/status") else 1
    )
    failures += (
        0
        if api_step("quality endpoint", "GET", f"{base_url}/api/v1/docs/search/quality?limit=12")
        else 1
    )
    if not args.skip_ui_contract:
        failures += (
            0
            if run_step(
                "UI contract smoke",
                [
                    sys.executable,
                    str(BLUEPRINTS_SCRIPTS / "smoke_nullclaw_docs_ui_contract.py"),
                    "--base-url",
                    base_url,
                    "--gui-root",
                    "/xarta-node/gui-fallback",
                ],
                cwd=ROOT,
            )
            else 1
        )

    if failures:
        print(f"\ndocs health failed: {failures} step(s)", file=sys.stderr)
        return 1
    print("\ndocs health passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
