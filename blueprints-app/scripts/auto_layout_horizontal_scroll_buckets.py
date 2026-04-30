#!/usr/bin/env python3
"""Regenerate all horizontal-scroll-on table layout buckets for every catalog table."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

APP_ROOT = Path(__file__).resolve().parents[1]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


_load_env_file(Path("/root/xarta-node/.env"))
_load_env_file(Path("/xarta-node/.env"))

from app.db import increment_gen  # type: ignore  # noqa: E402
from app.sync.queue import enqueue_for_all_peers  # type: ignore  # noqa: E402
from app.table_auto_layouts import (  # type: ignore  # noqa: E402
    build_auto_layout,
    default_viewport_for_flags,
    display_name_from_key,
)
from app.table_layouts import (  # type: ignore  # noqa: E402
    build_layout_key,
    decode_bucket_code,
    normalize_column_seed,
    parse_json_text,
)

DEFAULT_DB = "/opt/blueprints/data/db/blueprints.db"
HORIZONTAL_SCROLL_BUCKETS = [f"{value:02X}" for value in range(0x20) if value & 0x02]


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _json(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _value_length(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return len(str(value))
    text = str(value)
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return len(text)
    if isinstance(parsed, list):
        return max((len(str(item)) for item in parsed), default=0)
    if isinstance(parsed, dict):
        return max((len(str(key)) + len(str(val)) + 2 for key, val in parsed.items()), default=0)
    return len(text)


def _max_sql_value_length(conn: sqlite3.Connection, sql_table: str, column_name: str) -> int:
    table_ident = _quote_ident(sql_table)
    col_ident = _quote_ident(column_name)
    max_len = 0
    try:
        rows = conn.execute(f"SELECT {col_ident} AS value FROM {table_ident} LIMIT 500").fetchall()
    except sqlite3.Error:
        return 0
    for row in rows:
        max_len = max(max_len, _value_length(row["value"]))
    return max_len


def _table_specific_lengths(conn: sqlite3.Connection, table_name: str) -> dict[str, int]:
    if table_name != "fleet-nodes":
        return {}
    rows = conn.execute(
        """
        SELECT display_name, addresses, primary_hostname, tailnet_hostname
        FROM nodes
        """
    ).fetchall()
    lengths = {
        "display_name": 0,
        "addresses": 0,
        "hostnames": 0,
        "gen": 6,
        "commit": 12,
        "commit_non_root": 12,
        "commit_inner": 12,
        "pending": 4,
    }
    for row in rows:
        lengths["display_name"] = max(lengths["display_name"], len(str(row["display_name"] or "")))
        lengths["addresses"] = max(lengths["addresses"], _value_length(row["addresses"]))
        lengths["hostnames"] = max(
            lengths["hostnames"],
            len(str(row["primary_hostname"] or "")),
            len(str(row["tailnet_hostname"] or "")),
        )
    return lengths


def _best_existing_seed(
    conn: sqlite3.Connection, table_code: str, user_code: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT layout_data FROM table_layouts
        WHERE table_code=? AND user_code=?
        ORDER BY updated_at DESC, created_at DESC
        """,
        (table_code, user_code),
    ).fetchall()
    best: list[dict[str, Any]] = []
    best_rank = (-1, -1)
    for row in rows:
        layout = parse_json_text(row["layout_data"], {})
        columns = layout.get("columns") if isinstance(layout, dict) else None
        if not isinstance(columns, list):
            continue
        normalized = []
        for idx, column in enumerate(columns):
            try:
                seed = normalize_column_seed(column, idx)
            except Exception:
                continue
            seed["sort_direction"] = None
            seed["sort_priority"] = None
            normalized.append(seed)
        visible_count = sum(1 for column in normalized if not column.get("hidden"))
        rank = (len(normalized), visible_count)
        if normalized and rank > best_rank:
            best = normalized
            best_rank = rank
    return best


def _refresh_seed_lengths(
    conn: sqlite3.Connection,
    seeds: list[dict[str, Any]],
    *,
    table_name: str,
    sql_table: str | None,
) -> list[dict[str, Any]]:
    specific = _table_specific_lengths(conn, table_name)
    for seed in seeds:
        key = str(seed.get("column_key") or "")
        sqlite_column = seed.get("sqlite_column")
        measured = specific.get(key, 0)
        if not measured and sql_table and sqlite_column:
            measured = _max_sql_value_length(conn, sql_table, str(sqlite_column))
        if measured:
            seed["sample_max_length"] = measured
    return seeds


def _infer_sql_seed(conn: sqlite3.Connection, sql_table: str | None) -> list[dict[str, Any]]:
    if not sql_table:
        return []
    info_rows = conn.execute(f"PRAGMA table_info({_quote_ident(sql_table)})").fetchall()
    if not info_rows:
        return []
    seeds = []
    for idx, row in enumerate(info_rows):
        name = row["name"]
        sample_max = _max_sql_value_length(conn, sql_table, name)
        seeds.append(
            {
                "column_key": name,
                "display_name": display_name_from_key(name),
                "sqlite_column": name,
                "width_px": None,
                "min_width_px": 40,
                "max_width_px": 900,
                "position": idx,
                "sort_direction": None,
                "sort_priority": None,
                "hidden": False,
                "data_type": row["type"] or None,
                "sample_max_length": sample_max,
            }
        )
    return seeds


def _catalog(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT table_code, table_name, table_meta FROM table_layout_catalog ORDER BY table_code"
    ).fetchall()


def _upsert(
    conn: sqlite3.Connection,
    layout_key: str,
    layout_data: dict[str, Any],
    *,
    dry_run: bool,
) -> str:
    parts = {
        "reserved_code": layout_key[0:2],
        "user_code": layout_key[2:4],
        "table_code": layout_key[4:6],
        "bucket_code": layout_key[6:8],
    }
    existing = conn.execute(
        "SELECT layout_key FROM table_layouts WHERE layout_key=?",
        (layout_key,),
    ).fetchone()
    if dry_run:
        return "update" if existing else "insert"

    gen = increment_gen(conn, "table-auto-layout")
    if existing:
        conn.execute(
            """
            UPDATE table_layouts
            SET layout_data=?, updated_at=datetime('now')
            WHERE layout_key=?
            """,
            (_json(layout_data), layout_key),
        )
        action = "UPDATE"
    else:
        conn.execute(
            """
            INSERT INTO table_layouts (
                layout_key, reserved_code, user_code, table_code, bucket_code, layout_data
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                layout_key,
                parts["reserved_code"],
                parts["user_code"],
                parts["table_code"],
                parts["bucket_code"],
                _json(layout_data),
            ),
        )
        action = "INSERT"
    saved = conn.execute(
        "SELECT * FROM table_layouts WHERE layout_key=?",
        (layout_key,),
    ).fetchone()
    enqueue_for_all_peers(conn, action, "table_layouts", layout_key, dict(saved), gen)
    return action.lower()


def regenerate(path: str, user_code: str, dry_run: bool) -> dict[str, Any]:
    summary = {
        "db": path,
        "dry_run": dry_run,
        "user_code": user_code,
        "tables": 0,
        "buckets": 0,
        "insert": 0,
        "update": 0,
        "skipped": [],
        "max_estimated_cell_lines": 0,
        "max_total_width_px": 0,
    }
    with _connect(path) as conn:
        for catalog_row in _catalog(conn):
            table_code = catalog_row["table_code"]
            table_name = catalog_row["table_name"]
            meta = parse_json_text(catalog_row["table_meta"], {})
            sql_table = meta.get("sql_table") if isinstance(meta, dict) else None
            seeds = _best_existing_seed(conn, table_code, user_code) or _infer_sql_seed(
                conn, sql_table
            )
            seeds = _refresh_seed_lengths(
                conn,
                seeds,
                table_name=table_name,
                sql_table=sql_table,
            )
            if not seeds:
                summary["skipped"].append({"table_code": table_code, "table_name": table_name})
                continue
            summary["tables"] += 1
            for bucket_code in HORIZONTAL_SCROLL_BUCKETS:
                flags = decode_bucket_code(bucket_code)
                viewport = default_viewport_for_flags(flags)
                layout_data, planner = build_auto_layout(
                    seeds,
                    bucket_code,
                    table_name=table_name,
                    viewport=viewport,
                )
                layout_key = build_layout_key("00", user_code, table_code, bucket_code)
                action = _upsert(conn, layout_key, layout_data, dry_run=dry_run)
                summary[action] = int(summary.get(action, 0)) + 1
                summary["buckets"] += 1
                summary["max_estimated_cell_lines"] = max(
                    int(summary["max_estimated_cell_lines"]),
                    int(planner.get("max_estimated_cell_lines") or 0),
                )
                summary["max_total_width_px"] = max(
                    int(summary["max_total_width_px"]),
                    int(planner.get("total_width_px") or 0),
                )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite DB path, default {DEFAULT_DB}")
    parser.add_argument("--user-code", default="00", help="Layout user byte, default 00")
    parser.add_argument("--apply", action="store_true", help="Write rows instead of dry-running")
    args = parser.parse_args()

    summary = regenerate(args.db, args.user_code.upper(), dry_run=not args.apply)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not summary["skipped"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
