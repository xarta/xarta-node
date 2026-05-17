#!/usr/bin/env python3
"""Check whether a full URL already exists in manual_links.

This is the deterministic duplicate check used by Manual Links URL intake.
It checks exact URL values across the canonical route fields.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _default_db_path() -> Path:
    if os.environ.get("BLUEPRINTS_DB_DIR"):
        return Path(os.environ["BLUEPRINTS_DB_DIR"]) / "blueprints.db"
    env_file = ROOT.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("BLUEPRINTS_DB_DIR="):
                return Path(line.split("=", 1)[1].strip().strip('"').strip("'")) / "blueprints.db"
    return Path("/data/db") / "blueprints.db"


DEFAULT_DB_PATH = _default_db_path()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check Manual Links for an exact URL.")
    parser.add_argument("url", help="Full http(s) URL to check")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help=f"SQLite DB path (default: {DEFAULT_DB_PATH})")
    args = parser.parse_args()

    with sqlite3.connect(args.db) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT link_id, label, group_name, vlan_uri, tailnet_uri, vlan_ip, tailnet_ip
            FROM manual_links
            WHERE vlan_uri=? OR tailnet_uri=? OR vlan_ip=? OR tailnet_ip=?
            LIMIT 1
            """,
            (args.url, args.url, args.url, args.url),
        ).fetchone()

    result = {"exists": bool(row), "url": args.url, "match": dict(row) if row else None}
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if row else 1


if __name__ == "__main__":
    raise SystemExit(main())
