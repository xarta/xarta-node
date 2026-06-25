#!/usr/bin/env python3
"""Deterministic Personal Time Activity automation runner."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

STATE_SCHEMA = "xarta.personal.automation.state.v1"
REPORT_SCHEMA = "xarta.personal.automation.report.v1"
DEFAULT_STATE_FILE = Path("/xarta-node/.lone-wolf/automation/personal-time-activity/state.json")
DEFAULT_REPORT_DIR = Path("/xarta-node/.lone-wolf/automation/personal-time-activity/reports")
DEFAULT_MINUTES_INDEX_PATH = Path("/xarta-node/.lone-wolf/state/hermes-stt/minutes/recent.jsonl")
DEFAULT_MINUTES_CONFIG_FILE = Path("/xarta-node/.lone-wolf/config/hermes-stt/minutes.json")
VOLATILE_KEYS = {
    "generated_at",
    "generated_at_utc",
    "timestamp",
    "updated_at_utc",
    "refreshed_at",
    "run_id",
    "request_id",
}
DEFAULT_JOBS = [
    "status-guards",
    "imports-dashboard",
    "diary-day",
    "minutes-projection",
    "browser-links-rollup",
    "git-rollup",
    "work-rollup",
    "search-sync",
    "graph-sync",
    "hot-cache-maintenance",
]


class AutomationError(RuntimeError):
    """Raised for expected automation command failures."""


def utc_now_text() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return fallback


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(tmp, path)


def load_state(path: Path) -> dict[str, Any]:
    state = read_json(path, {})
    if not isinstance(state, dict) or state.get("schema") != STATE_SCHEMA:
        return {"schema": STATE_SCHEMA, "jobs": {}}
    if not isinstance(state.get("jobs"), dict):
        state["jobs"] = {}
    return state


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def digest_value(value: Any) -> str:
    return "sha256:" + hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()


def strip_volatile(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: strip_volatile(raw)
            for key, raw in sorted(value.items())
            if key not in VOLATILE_KEYS
        }
    if isinstance(value, list):
        return [strip_volatile(item) for item in value]
    return value


def file_fingerprint(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        return {
            "status": "ok",
            "path": str(path),
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "sha256": f"sha256:{digest}",
        }
    except OSError as exc:
        return {
            "status": "source_unavailable",
            "path": str(path),
            "error": type(exc).__name__,
        }


def date_candidates(local_date: str) -> list[str]:
    base = date.fromisoformat(local_date)
    return [
        (base - timedelta(days=1)).isoformat(),
        base.isoformat(),
        (base + timedelta(days=1)).isoformat(),
    ]


def blueprints_db_path() -> Path:
    explicit = os.environ.get("BLUEPRINTS_DB_PATH", "").strip()
    if explicit:
        return Path(explicit)
    return Path(os.environ.get("BLUEPRINTS_DB_DIR", "/data/db")) / "blueprints.db"


def minutes_index_path() -> Path:
    explicit = os.environ.get("HERMES_MINUTES_LOCAL_INDEX_PATH", "").strip()
    if explicit:
        return Path(explicit)
    config_path = Path(
        os.environ.get("HERMES_MINUTES_CONFIG_FILE", str(DEFAULT_MINUTES_CONFIG_FILE))
    )
    config = read_json(config_path, {})
    if isinstance(config, dict) and str(config.get("local_index_path") or "").strip():
        return Path(str(config["local_index_path"]))
    return DEFAULT_MINUTES_INDEX_PATH


def minutes_signature(
    *, local_date: str, timezone_name: str, limit: int, ttl_seconds: float | None
) -> str:
    path = minutes_index_path()
    return digest_value(
        {
            "job": "minutes-projection",
            "local_date": local_date,
            "timezone": timezone_name,
            "limit": limit,
            "ttl_seconds": ttl_seconds,
            "source": file_fingerprint(path),
        }
    )


def sqlite_signature(*, local_date: str, kind: str) -> dict[str, Any]:
    path = blueprints_db_path()
    if not path.exists():
        return {"status": "source_unavailable", "path": str(path), "kind": kind}
    candidates = date_candidates(local_date)
    placeholders = ",".join("?" for _ in candidates)
    try:
        with sqlite3.connect(path) as conn:
            conn.row_factory = sqlite3.Row
            if kind == "browser-links":
                tables = {}
                for table, field, updated_field in (
                    ("bookmarks", "created_at", "updated_at"),
                    ("visits", "visited_at", "updated_at"),
                    ("visit_events", "visited_at", ""),
                ):
                    max_updated = (
                        f"MAX({updated_field}) AS max_updated_at"
                        if updated_field
                        else "'' AS max_updated_at"
                    )
                    row = conn.execute(
                        f"""
                        SELECT COUNT(*) AS count,
                               MAX({field}) AS max_source_at,
                               {max_updated}
                        FROM {table}
                        WHERE substr({field}, 1, 10) IN ({placeholders})
                        """,
                        candidates,
                    ).fetchone()
                    tables[table] = dict(row)
                return {"status": "ok", "path": str(path), "kind": kind, "tables": tables}
            tables = {}
            for table in (
                "personal_events",
                "personal_time_tasks",
                "kanban_items",
                "kanban_blockers",
                "kanban_discussions",
            ):
                row = conn.execute(
                    f"SELECT COUNT(*) AS count, MAX(updated_at) AS max_updated_at FROM {table}"
                ).fetchone()
                tables[table] = dict(row)
            return {"status": "ok", "path": str(path), "kind": kind, "tables": tables}
    except sqlite3.Error as exc:
        return {
            "status": "source_unavailable",
            "path": str(path),
            "kind": kind,
            "error": str(exc).splitlines()[0][:180],
        }


def source_signature(*, local_date: str, kind: str) -> str:
    return digest_value(sqlite_signature(local_date=local_date, kind=kind))


def request_json(
    *,
    base_url: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    timeout: float = 30.0,
    auth_token: str = "",
) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    headers = {"Accept": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if auth_token:
        headers["X-API-Token"] = auth_token
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:500]
        raise AutomationError(f"{method} {path} HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise AutomationError(f"{method} {path} failed: {exc.reason}") from exc
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise AutomationError(f"{method} {path} did not return JSON") from exc
    if not isinstance(parsed, dict):
        raise AutomationError(f"{method} {path} returned non-object JSON")
    return parsed


def get_json(args: argparse.Namespace, path: str) -> dict[str, Any]:
    return request_json(
        base_url=args.base_url,
        method="GET",
        path=path,
        timeout=args.timeout,
        auth_token=args.auth_token,
    )


def post_json(args: argparse.Namespace, path: str, body: dict[str, Any]) -> dict[str, Any]:
    return request_json(
        base_url=args.base_url,
        method="POST",
        path=path,
        body=body,
        timeout=args.timeout,
        auth_token=args.auth_token,
    )


def job_entry(
    name: str,
    *,
    status: str,
    mutated: bool = False,
    reason: str = "",
    signature: str = "",
    summary: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    return {
        "name": name,
        "status": status,
        "mutated": mutated,
        "reason": reason,
        "signature": signature,
        "summary": summary or {},
        "error": error,
    }


def job_key(name: str, local_date: str) -> str:
    return f"{name}:{local_date}"


def should_skip(
    state: dict[str, Any],
    *,
    name: str,
    local_date: str,
    signature: str,
    force: bool,
) -> bool:
    if force:
        return False
    previous = state.get("jobs", {}).get(job_key(name, local_date))
    return (
        isinstance(previous, dict)
        and previous.get("signature") == signature
        and previous.get("last_status") in {"ok", "degraded", "source_unavailable"}
    )


def record_state(
    state: dict[str, Any],
    *,
    name: str,
    local_date: str,
    signature: str,
    status: str,
    summary: dict[str, Any],
) -> None:
    state.setdefault("jobs", {})[job_key(name, local_date)] = {
        "signature": signature,
        "last_status": status,
        "last_summary_digest": digest_value(strip_volatile(summary)),
        "updated_at_utc": utc_now_text(),
    }


def run_health(args: argparse.Namespace) -> dict[str, Any]:
    payload = get_json(args, "/health")
    return job_entry(
        "health",
        status="ok" if payload.get("status") == "ok" else "degraded",
        summary={
            "status": payload.get("status"),
            "node_id": payload.get("node_id"),
            "gen": payload.get("gen"),
            "integrity_ok": payload.get("integrity_ok"),
            "commit": payload.get("commit"),
        },
    )


def run_imports_dashboard(args: argparse.Namespace) -> dict[str, Any]:
    payload = get_json(args, "/api/v1/personal/imports-dashboard")
    blockers = payload.get("blockers") if isinstance(payload.get("blockers"), list) else []
    git_activity = (
        payload.get("git_activity") if isinstance(payload.get("git_activity"), dict) else {}
    )
    return job_entry(
        "imports-dashboard",
        status="ok" if payload.get("status") == "ok" else "needs_review",
        summary={
            "status": payload.get("status"),
            "source_digest": payload.get("source_digest"),
            "blocker_count": len(blockers),
            "git_status": git_activity.get("status"),
            "git_repo_count": len(git_activity.get("watched_repos") or []),
        },
        signature=str(payload.get("source_digest") or digest_value(strip_volatile(payload))),
    )


def run_diary_day(args: argparse.Namespace) -> dict[str, Any]:
    query = urllib.parse.urlencode({"date": args.date})
    payload = get_json(args, f"/api/v1/personal/diary-day?{query}")
    return job_entry(
        "diary-day",
        status="ok"
        if payload.get("status") in {"ready", "ok"}
        else str(payload.get("status") or "ok"),
        summary={
            "status": payload.get("status"),
            "local_date": payload.get("local_date"),
            "source_moment_count": len(payload.get("source_moments") or []),
            "next_action_count": len(payload.get("next_actions") or []),
            "day_file_count": len(payload.get("day_files") or []),
        },
        signature=digest_value(strip_volatile(payload)),
    )


def run_guarded_post(
    args: argparse.Namespace,
    state: dict[str, Any],
    *,
    name: str,
    signature: str,
    path: str,
    body: dict[str, Any],
    summary_from_payload: Any,
) -> dict[str, Any]:
    if should_skip(state, name=name, local_date=args.date, signature=signature, force=args.force):
        return job_entry(
            name,
            status="skipped",
            mutated=False,
            reason="source_signature_unchanged",
            signature=signature,
        )
    if args.dry_run:
        return job_entry(
            name,
            status="would_run",
            mutated=False,
            reason="dry_run",
            signature=signature,
            summary={"path": path},
        )
    payload = post_json(args, path, body)
    summary = summary_from_payload(payload)
    status = str(summary.get("status") or payload.get("status") or "ok")
    record_state(
        state,
        name=name,
        local_date=args.date,
        signature=signature,
        status=status,
        summary=summary,
    )
    return job_entry(name, status=status, mutated=True, signature=signature, summary=summary)


def run_minutes_projection(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    signature = minutes_signature(
        local_date=args.date,
        timezone_name=args.timezone,
        limit=args.minutes_limit,
        ttl_seconds=args.minutes_ttl_seconds,
    )
    return run_guarded_post(
        args,
        state,
        name="minutes-projection",
        signature=signature,
        path="/api/v1/personal/diary-day/minutes-project",
        body={
            "local_date": args.date,
            "timezone": args.timezone,
            "limit": args.minutes_limit,
            "ttl_seconds": args.minutes_ttl_seconds,
            "actor": "blueprints-personal-automation",
            "source_surface": "personal-activity-automation",
            "request_id": f"automation-minutes-{args.date}-{signature[-12:]}",
            "run_id": args.run_id,
        },
        summary_from_payload=lambda payload: {
            "status": payload.get("status"),
            "source_available": payload.get("source_available"),
            "entry_count": (payload.get("projection") or {}).get("entry_count"),
            "source_hash": (payload.get("projection") or {}).get("source_hash"),
        },
    )


def run_browser_links_rollup(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    signature = source_signature(local_date=args.date, kind="browser-links")
    return run_guarded_post(
        args,
        state,
        name="browser-links-rollup",
        signature=signature,
        path="/api/v1/personal/diary-day/browser-links-project",
        body={
            "local_date": args.date,
            "timezone": args.timezone,
            "limit": args.browser_links_limit,
            "actor": "blueprints-personal-automation",
            "source_surface": "personal-activity-automation",
            "request_id": f"automation-browser-links-{args.date}-{signature[-12:]}",
            "run_id": args.run_id,
        },
        summary_from_payload=lambda payload: {
            "status": payload.get("status"),
            "source_available": payload.get("source_available"),
            "visit_event_count": (payload.get("projection") or {}).get("visit_event_count"),
            "bookmark_count": (payload.get("projection") or {}).get("bookmark_count"),
            "source_health_status": (payload.get("source_health") or {}).get("status"),
        },
    )


def run_git_rollup(args: argparse.Namespace) -> dict[str, Any]:
    payload = get_json(args, "/api/v1/personal/imports-dashboard")
    git_activity = (
        payload.get("git_activity") if isinstance(payload.get("git_activity"), dict) else {}
    )
    return job_entry(
        "git-rollup",
        status="ok" if git_activity.get("status") == "ok" else "needs_review",
        signature=digest_value(strip_volatile(git_activity)),
        summary={
            "status": git_activity.get("status"),
            "repo_count": len(git_activity.get("watched_repos") or []),
            "latest_commit_count": len(git_activity.get("latest_commits") or []),
            "error_count": len(git_activity.get("errors") or []),
        },
    )


def run_work_rollup(args: argparse.Namespace) -> dict[str, Any]:
    payload = get_json(args, "/api/v1/personal/kanban/board")
    board = payload.get("board") if isinstance(payload.get("board"), dict) else {}
    lanes = board.get("columns") if isinstance(board.get("columns"), list) else []
    item_count = 0
    for lane in lanes:
        if isinstance(lane, dict):
            item_count += len(lane.get("items") or [])
    return job_entry(
        "work-rollup",
        status="ok",
        signature=digest_value(strip_volatile(payload)),
        summary={"lane_count": len(lanes), "item_count": item_count},
    )


def run_search_sync(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    signature = digest_value(
        {
            "source": sqlite_signature(local_date=args.date, kind="personal-search"),
            "include_embeddings": args.include_embeddings,
            "force_embeddings": args.force_embeddings,
            "limit": args.search_limit,
        }
    )
    return run_guarded_post(
        args,
        state,
        name="search-sync",
        signature=signature,
        path="/api/v1/personal/search/sync",
        body={
            "include_embeddings": args.include_embeddings,
            "force_embeddings": args.force_embeddings,
            "limit": args.search_limit,
        },
        summary_from_payload=lambda payload: {
            "status": "ok" if payload.get("ok", True) is not False else "error",
            "documents": payload.get("documents"),
            "embeddings": payload.get("embeddings"),
            "vector_index": payload.get("vector_index"),
        },
    )


def run_graph_sync(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    signature = source_signature(local_date=args.date, kind="personal-graph")
    return run_guarded_post(
        args,
        state,
        name="graph-sync",
        signature=signature,
        path="/api/v1/personal/graph/sync",
        body={
            "actor": "blueprints-personal-automation",
            "request_id": f"automation-graph-{args.date}-{signature[-12:]}",
        },
        summary_from_payload=lambda payload: {
            "status": "ok" if payload.get("ok") is True else "error",
            "candidate_count": payload.get("candidate_count"),
            "links": payload.get("links"),
        },
    )


def run_hot_cache_maintenance(args: argparse.Namespace) -> dict[str, Any]:
    payload = post_json(
        args,
        "/api/v1/personal/projections/maintenance",
        {
            "retention_days": args.hot_cache_retention_days,
            "limit": args.hot_cache_limit,
            "dry_run": args.dry_run or args.hot_cache_dry_run,
        },
    )
    return job_entry(
        "hot-cache-maintenance",
        status="ok",
        mutated=bool(payload.get("trimmed_count")),
        summary={
            "dry_run": payload.get("dry_run"),
            "candidate_count": payload.get("candidate_count"),
            "trimmed_count": payload.get("trimmed_count"),
            "retention_days": payload.get("retention_days"),
        },
        signature=digest_value(strip_volatile(payload)),
    )


def run_status_guards(args: argparse.Namespace) -> dict[str, Any]:
    health = run_health(args)
    maintenance = post_json(
        args,
        "/api/v1/personal/projections/maintenance",
        {
            "retention_days": args.hot_cache_retention_days,
            "limit": args.hot_cache_limit,
            "dry_run": True,
        },
    )
    imports = run_imports_dashboard(args)
    status = "ok"
    if health["status"] != "ok" or imports["status"] != "ok":
        status = "needs_review"
    return job_entry(
        "status-guards",
        status=status,
        summary={
            "health": health["summary"],
            "imports": imports["summary"],
            "stale_projection_candidates": maintenance.get("candidate_count"),
            "permission": "loopback_or_token_accepted",
            "no_secret_posture": "checked_after_report_build",
        },
        signature=digest_value(
            {
                "health": health["summary"],
                "imports": imports["signature"],
                "stale_projection_candidates": maintenance.get("candidate_count"),
            }
        ),
    )


def secret_posture(value: Any) -> dict[str, Any]:
    hits: list[str] = []

    def walk(node: Any, path: str) -> None:
        if isinstance(node, dict):
            for key, raw in node.items():
                lowered = str(key).lower()
                next_path = f"{path}.{key}" if path else str(key)
                if lowered == "no_secret_posture":
                    continue
                if any(term in lowered for term in ("password", "secret", "api_key", "token")):
                    if raw not in ("", None, [], {}):
                        hits.append(next_path)
                walk(raw, next_path)
        elif isinstance(node, list):
            for index, item in enumerate(node[:200]):
                walk(item, f"{path}[{index}]")

    walk(value, "")
    return {
        "status": "ok" if not hits else "needs_review",
        "hit_count": len(hits),
        "hits": hits[:20],
    }


def run_job(name: str, args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    try:
        if name == "status-guards":
            return run_status_guards(args)
        if name == "imports-dashboard":
            return run_imports_dashboard(args)
        if name == "diary-day":
            return run_diary_day(args)
        if name == "minutes-projection":
            return run_minutes_projection(args, state)
        if name == "browser-links-rollup":
            return run_browser_links_rollup(args, state)
        if name == "git-rollup":
            return run_git_rollup(args)
        if name == "work-rollup":
            return run_work_rollup(args)
        if name == "search-sync":
            return run_search_sync(args, state)
        if name == "graph-sync":
            return run_graph_sync(args, state)
        if name == "hot-cache-maintenance":
            return run_hot_cache_maintenance(args)
        return job_entry(name, status="error", error=f"unknown job: {name}")
    except Exception as exc:
        return job_entry(name, status="error", error=str(exc))


def parse_jobs(raw: str) -> list[str]:
    if not raw.strip() or raw.strip() == "all":
        return list(DEFAULT_JOBS)
    seen = []
    for part in raw.split(","):
        job = part.strip()
        if job and job not in seen:
            seen.append(job)
    return seen


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", default=os.environ.get("BLUEPRINTS_BASE_URL", "http://127.0.0.1:8080")
    )
    parser.add_argument("--auth-token", default=os.environ.get("BLUEPRINTS_API_TOKEN", ""))
    parser.add_argument("--timeout", type=float, default=45.0)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument(
        "--timezone", default=os.environ.get("XARTA_DIARY_TIMEZONE", "Europe/London")
    )
    parser.add_argument("--jobs", default="all", help="Comma list or all")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--include-embeddings", action="store_true")
    parser.add_argument("--force-embeddings", action="store_true")
    parser.add_argument("--search-limit", type=int, default=200)
    parser.add_argument("--minutes-limit", type=int, default=200)
    parser.add_argument("--minutes-ttl-seconds", type=float, default=36 * 60 * 60)
    parser.add_argument("--browser-links-limit", type=int, default=1000)
    parser.add_argument("--hot-cache-retention-days", type=int, default=60)
    parser.add_argument("--hot-cache-limit", type=int, default=250)
    parser.add_argument("--hot-cache-dry-run", action="store_true")
    parser.add_argument("--run-id", default=f"personal-automation-{int(time.time())}")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    jobs = parse_jobs(args.jobs)
    state = load_state(args.state_file)
    report = {
        "schema": REPORT_SCHEMA,
        "generated_at_utc": utc_now_text(),
        "base_url": args.base_url,
        "date": args.date,
        "timezone": args.timezone,
        "dry_run": args.dry_run,
        "force": args.force,
        "run_id": args.run_id,
        "jobs": [],
    }
    for name in jobs:
        entry = run_job(name, args, state)
        report["jobs"].append(entry)
    posture = secret_posture(report)
    report["status_guards"] = {"no_secret_posture": posture}
    if posture["status"] != "ok":
        report["jobs"].append(
            job_entry(
                "no-secret-posture",
                status="needs_review",
                summary=posture,
                reason="report_contains_secret_shaped_fields",
            )
        )
    if not args.dry_run:
        write_json(args.state_file, state)
    report_path = args.report
    if report_path is None:
        DEFAULT_REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = DEFAULT_REPORT_DIR / f"personal-automation-{int(time.time())}.json"
    write_json(report_path, report)
    failed = [job for job in report["jobs"] if job.get("status") in {"error", "needs_review"}]
    print(
        json.dumps({"ok": not failed, "report": str(report_path), "failed": failed}, sort_keys=True)
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
