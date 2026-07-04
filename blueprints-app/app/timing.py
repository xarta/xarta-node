"""Small in-memory timing recorder for local API contention analysis."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import itertools
import json
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Generator
from concurrent.futures import Executor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import parse_qsl, urlencode

from .node_local_ownership import normalize_node_local_ownership as _normalize_node_local_ownership

T = TypeVar("T")

_DEFAULT_CAPACITY = 4096
_DEFAULT_LOOP_INTERVAL_MS = 100
_DEFAULT_LOOP_LAG_THRESHOLD_MS = 25
_DEFAULT_DISK_INTERVAL_SECONDS = 600
_DEFAULT_PRUNE_INTERVAL_SECONDS = 86400
_DEFAULT_LOG_ROOT = "/xarta-node/.lone-wolf/blueprints-event-loop-logs"
_LONE_WOLF_ROOT = Path("/xarta-node/.lone-wolf").resolve()
_SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "authorization",
    "key",
    "password",
    "secret",
    "token",
    "x-api-token",
}

_events: deque[dict[str, Any]] = deque(maxlen=_DEFAULT_CAPACITY)
_disk_pending_events: deque[dict[str, Any]] = deque()
_lock = threading.Lock()
_flush_lock = threading.Lock()
_sequence = itertools.count(1)
_trace_sequence = itertools.count(1)
_last_flushed_seq = 0
_current_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "blueprints_timing_trace_id",
    default="",
)


def enabled() -> bool:
    raw = os.getenv("BLUEPRINTS_TIMING_ENABLED", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _capacity() -> int:
    raw = os.getenv("BLUEPRINTS_TIMING_BUFFER_EVENTS", "").strip()
    try:
        return max(128, min(100_000, int(raw)))
    except (TypeError, ValueError):
        return _DEFAULT_CAPACITY


def _loop_interval_seconds() -> float:
    raw = os.getenv("BLUEPRINTS_TIMING_LOOP_INTERVAL_MS", "").strip()
    try:
        value = max(25, min(5000, int(raw)))
    except (TypeError, ValueError):
        value = _DEFAULT_LOOP_INTERVAL_MS
    return value / 1000.0


def _loop_lag_threshold_seconds() -> float:
    raw = os.getenv("BLUEPRINTS_TIMING_LOOP_LAG_THRESHOLD_MS", "").strip()
    try:
        value = max(1, min(60_000, int(raw)))
    except (TypeError, ValueError):
        value = _DEFAULT_LOOP_LAG_THRESHOLD_MS
    return value / 1000.0


def disk_logging_enabled() -> bool:
    raw = os.getenv("BLUEPRINTS_TIMING_DISK_ENABLED", "true").strip().lower()
    return enabled() and raw not in {"0", "false", "no", "off"}


def _disk_interval_seconds() -> int:
    raw = os.getenv("BLUEPRINTS_TIMING_DISK_INTERVAL_SECONDS", "").strip()
    try:
        return max(60, min(86400, int(raw)))
    except (TypeError, ValueError):
        return _DEFAULT_DISK_INTERVAL_SECONDS


def _prune_interval_seconds() -> int:
    raw = os.getenv("BLUEPRINTS_TIMING_PRUNE_INTERVAL_SECONDS", "").strip()
    try:
        return max(3600, min(7 * 86400, int(raw)))
    except (TypeError, ValueError):
        return _DEFAULT_PRUNE_INTERVAL_SECONDS


def _log_root() -> Path:
    raw = os.getenv("BLUEPRINTS_TIMING_LOG_ROOT", _DEFAULT_LOG_ROOT).strip()
    root = Path(raw or _DEFAULT_LOG_ROOT).resolve()
    try:
        root.relative_to(_LONE_WOLF_ROOT)
    except ValueError as exc:
        raise RuntimeError(f"timing log root must stay under {_LONE_WOLF_ROOT}") from exc
    return root


def sanitize_query(query: str) -> str:
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    clean = []
    for key, value in pairs:
        if key.strip().lower() in _SENSITIVE_QUERY_KEYS:
            clean.append((key, "[redacted]"))
        else:
            clean.append((key, value))
    return urlencode(clean, doseq=True)


def _trim_for_capacity() -> None:
    global _events
    capacity = _capacity()
    if _events.maxlen != capacity:
        preserved = list(_events)[-capacity:]
        _events = deque(preserved, maxlen=capacity)


def _clean_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:500]
    if isinstance(value, bytes):
        return value[:200].decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [_clean_value(item) for item in value[:50]]
    if isinstance(value, dict):
        return {str(key)[:100]: _clean_value(item) for key, item in list(value.items())[:80]}
    return str(value)[:500]


def new_trace_id() -> str:
    return f"timing-{next(_trace_sequence)}"


def current_trace_id() -> str:
    return _current_trace_id.get("")


def set_current_trace_id(trace_id: str) -> contextvars.Token[str]:
    return _current_trace_id.set(trace_id)


def reset_current_trace_id(token: contextvars.Token[str]) -> None:
    _current_trace_id.reset(token)


def record_event(event_type: str, **fields: Any) -> None:
    if not enabled():
        return
    event = {
        "schema": "xarta.blueprints.timing.event.v1",
        "seq": next(_sequence),
        "event": str(event_type),
        "recorded_at_ns": time.time_ns(),
        "recorded_perf_ns": time.perf_counter_ns(),
    }
    trace_id = str(fields.pop("trace_id", "") or current_trace_id())
    if trace_id:
        event["trace_id"] = trace_id
    for key, value in fields.items():
        event[str(key)] = _clean_value(value)
    with _lock:
        _trim_for_capacity()
        _events.append(event)
        if disk_logging_enabled():
            _disk_pending_events.append(event)


def record_span(
    event_type: str,
    *,
    start_perf_ns: int,
    end_perf_ns: int,
    start_time_ns: int | None = None,
    end_time_ns: int | None = None,
    **fields: Any,
) -> None:
    if not enabled():
        return
    duration_ms = max(0.0, (end_perf_ns - start_perf_ns) / 1_000_000)
    record_event(
        event_type,
        kind="span",
        start_perf_ns=start_perf_ns,
        end_perf_ns=end_perf_ns,
        start_time_ns=start_time_ns,
        end_time_ns=end_time_ns,
        duration_ms=round(duration_ms, 3),
        **fields,
    )


@contextlib.contextmanager
def span(event_type: str, **fields: Any) -> Generator[None, None, None]:
    started = time.perf_counter_ns()
    started_wall = time.time_ns()
    ok = True
    error_type = ""
    try:
        yield
    except Exception as exc:
        ok = False
        error_type = type(exc).__name__
        raise
    finally:
        finished = time.perf_counter_ns()
        record_span(
            event_type,
            start_perf_ns=started,
            end_perf_ns=finished,
            start_time_ns=started_wall,
            end_time_ns=time.time_ns(),
            **fields,
            ok=ok,
            error_type=error_type,
        )


async def to_thread(
    label: str,
    func: Callable[..., T],
    *args: Any,
    _executor: Executor | None = None,
    **kwargs: Any,
) -> T:
    submitted = time.perf_counter_ns()
    submitted_wall = time.time_ns()
    trace_id = current_trace_id()
    run_started = 0
    run_finished = 0
    run_started_wall = 0
    run_finished_wall = 0
    ok = True
    error_type = ""

    def runner() -> T:
        nonlocal run_started, run_finished, run_started_wall, run_finished_wall
        run_started = time.perf_counter_ns()
        run_started_wall = time.time_ns()
        try:
            return func(*args, **kwargs)
        finally:
            run_finished = time.perf_counter_ns()
            run_finished_wall = time.time_ns()

    try:
        if _executor is None:
            return await asyncio.to_thread(runner)
        loop = asyncio.get_running_loop()
        context = contextvars.copy_context()
        return await loop.run_in_executor(_executor, context.run, runner)
    except Exception as exc:
        ok = False
        error_type = type(exc).__name__
        raise
    finally:
        finished = time.perf_counter_ns()
        finished_wall = time.time_ns()
        record_span(
            "thread_work",
            start_perf_ns=submitted,
            end_perf_ns=finished,
            start_time_ns=submitted_wall,
            end_time_ns=finished_wall,
            trace_id=trace_id,
            label=label,
            ok=ok,
            error_type=error_type,
            submitted_perf_ns=submitted,
            run_start_perf_ns=run_started,
            run_end_perf_ns=run_finished,
            submitted_time_ns=submitted_wall,
            run_start_time_ns=run_started_wall,
            run_end_time_ns=run_finished_wall,
            queue_wait_ms=round(max(0, run_started - submitted) / 1_000_000, 3)
            if run_started
            else None,
            run_ms=round(max(0, run_finished - run_started) / 1_000_000, 3)
            if run_finished and run_started
            else None,
            await_ms=round(max(0, finished - submitted) / 1_000_000, 3),
        )


async def run_event_loop_lag_sampler() -> None:
    interval = _loop_interval_seconds()
    threshold = _loop_lag_threshold_seconds()
    loop = asyncio.get_running_loop()
    expected = loop.time() + interval
    while True:
        await asyncio.sleep(max(0.0, expected - loop.time()))
        observed = loop.time()
        lag = observed - expected
        if enabled() and lag >= threshold:
            observed_time_ns = time.time_ns()
            lag_ns = int(max(0.0, lag) * 1_000_000_000)
            record_span(
                "event_loop_lag",
                start_perf_ns=int(expected * 1_000_000_000),
                end_perf_ns=int(observed * 1_000_000_000),
                start_time_ns=observed_time_ns - lag_ns,
                end_time_ns=observed_time_ns,
                lag_ms=round(lag * 1000, 3),
                interval_ms=round(interval * 1000, 3),
                threshold_ms=round(threshold * 1000, 3),
                expected_loop_time=expected,
                observed_loop_time=observed,
            )
        interval = _loop_interval_seconds()
        threshold = _loop_lag_threshold_seconds()
        expected = max(expected + interval, observed + interval)


def snapshot(limit: int | None = None) -> list[dict[str, Any]]:
    with _lock:
        rows = list(_events)
    if limit is None:
        return rows
    clean_limit = max(1, min(int(limit), 100_000))
    return rows[-clean_limit:]


def snapshot_jsonl(limit: int | None = None) -> str:
    return "\n".join(
        json.dumps(event, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        for event in snapshot(limit)
    )


def _event_json(event: dict[str, Any]) -> str:
    return json.dumps(event, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _write_events_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    content = "\n".join(_event_json(event) for event in rows)
    if content:
        content += "\n"
    with tmp_path.open("w", encoding="utf-8") as handle:
        handle.write(content)
    os.chmod(tmp_path, 0o600)
    _normalize_node_local_ownership(tmp_path, root=_LONE_WOLF_ROOT)
    tmp_path.replace(path)
    _normalize_node_local_ownership(path, root=_LONE_WOLF_ROOT)


def _pending_disk_events() -> list[dict[str, Any]]:
    with _lock:
        return list(_disk_pending_events)


def _mark_disk_events_flushed(last_seq: int) -> None:
    with _lock:
        while _disk_pending_events and int(_disk_pending_events[0].get("seq") or 0) <= last_seq:
            _disk_pending_events.popleft()


def flush_disk_logs_once(now: datetime | None = None) -> dict[str, Any]:
    global _last_flushed_seq
    if not disk_logging_enabled():
        return {"ok": True, "enabled": False, "written": False, "count": 0}
    stamp = now or datetime.now(UTC)
    with _flush_lock:
        rows = _pending_disk_events()
        if not rows:
            return {"ok": True, "enabled": True, "written": False, "count": 0}
        root = _log_root()
        date_dir = root / f"{stamp:%Y}" / f"{stamp:%m}" / f"{stamp:%d}"
        path = date_dir / f"{stamp:%H%M%S}-blueprints-event-loop.jsonl"
        _write_events_jsonl(path, rows)
        _last_flushed_seq = max(int(row.get("seq") or 0) for row in rows)
        _mark_disk_events_flushed(_last_flushed_seq)
    return {
        "ok": True,
        "enabled": True,
        "written": True,
        "count": len(rows),
        "path": str(path),
        "last_seq": _last_flushed_seq,
    }


async def flush_disk_logs() -> dict[str, Any]:
    return await asyncio.to_thread(flush_disk_logs_once)


def _parse_log_path(path: Path) -> datetime | None:
    try:
        day = datetime(
            int(path.parent.parent.parent.name),
            int(path.parent.parent.name),
            int(path.parent.name),
            tzinfo=UTC,
        )
        prefix = path.name.split("-", 1)[0]
        if len(prefix) == 6 and prefix.isdigit():
            return day.replace(
                hour=int(prefix[0:2]),
                minute=int(prefix[2:4]),
                second=int(prefix[4:6]),
            )
    except (IndexError, TypeError, ValueError):
        return None
    return None


def _log_file_score(path: Path) -> tuple[float, float]:
    durations: list[float] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    value = json.loads(line).get("duration_ms")
                except (AttributeError, json.JSONDecodeError):
                    continue
                if isinstance(value, (int, float)):
                    durations.append(float(value))
    except OSError:
        return 0.0, 0.0
    if not durations:
        return 0.0, 0.0
    return max(durations), sum(durations) / len(durations)


def _hour_key(stamp: datetime) -> tuple[int, int, int, int]:
    return stamp.year, stamp.month, stamp.day, stamp.hour


def _select_high_low_hours(files: list[tuple[Path, datetime]]) -> set[tuple[int, int, int, int]]:
    scores: dict[tuple[int, int, int, int], dict[str, float]] = {}
    for path, stamp in files:
        key = _hour_key(stamp)
        max_ms, avg_ms = _log_file_score(path)
        bucket = scores.setdefault(key, {"max": 0.0, "avg_total": 0.0, "count": 0.0})
        bucket["max"] = max(bucket["max"], max_ms)
        bucket["avg_total"] += avg_ms
        bucket["count"] += 1.0
    if not scores:
        return set()
    high = max(scores, key=lambda key: (scores[key]["max"], key))
    low = min(
        scores,
        key=lambda key: (
            scores[key]["avg_total"] / max(1.0, scores[key]["count"]),
            key,
        ),
    )
    return {high, low}


def prune_disk_logs_once(now: datetime | None = None) -> dict[str, Any]:
    if not disk_logging_enabled():
        return {"ok": True, "enabled": False, "deleted": 0, "kept": 0}
    root = _log_root()
    if not root.exists():
        return {"ok": True, "enabled": True, "deleted": 0, "kept": 0}
    stamp = now or datetime.now(UTC)
    week_cutoff = stamp - timedelta(days=7)
    quarter_cutoff = stamp - timedelta(days=90)
    year_cutoff = stamp - timedelta(days=365)
    three_year_cutoff = stamp - timedelta(days=3 * 365)

    parsed: list[tuple[Path, datetime]] = []
    for path in root.glob("*/*/*/*.jsonl"):
        path_stamp = _parse_log_path(path)
        if path_stamp is not None:
            parsed.append((path, path_stamp))

    keep: set[Path] = set()
    delete: set[Path] = set()
    older_than_year: list[tuple[Path, datetime]] = []
    older_than_three_years: list[tuple[Path, datetime]] = []
    old_saturdays_by_day: dict[tuple[int, int, int], list[tuple[Path, datetime]]] = {}

    for path, path_stamp in parsed:
        if path_stamp >= week_cutoff:
            keep.add(path)
        elif path_stamp >= quarter_cutoff:
            (keep if path_stamp.weekday() == 5 else delete).add(path)
        elif path_stamp >= year_cutoff:
            if path_stamp.weekday() == 5:
                old_saturdays_by_day.setdefault(
                    (path_stamp.year, path_stamp.month, path_stamp.day),
                    [],
                ).append((path, path_stamp))
            else:
                delete.add(path)
        elif path_stamp >= three_year_cutoff:
            older_than_year.append((path, path_stamp))
        else:
            older_than_three_years.append((path, path_stamp))

    for files in old_saturdays_by_day.values():
        selected = _select_high_low_hours(files)
        for path, path_stamp in files:
            (keep if _hour_key(path_stamp) in selected else delete).add(path)

    by_month: dict[tuple[int, int], list[tuple[Path, datetime]]] = {}
    for path, path_stamp in older_than_year:
        by_month.setdefault((path_stamp.year, path_stamp.month), []).append((path, path_stamp))
    for files in by_month.values():
        selected = _select_high_low_hours(files)
        for path, path_stamp in files:
            (keep if _hour_key(path_stamp) in selected else delete).add(path)

    by_year: dict[int, list[tuple[Path, datetime]]] = {}
    for path, path_stamp in older_than_three_years:
        by_year.setdefault(path_stamp.year, []).append((path, path_stamp))
    for files in by_year.values():
        selected = _select_high_low_hours(files)
        for path, path_stamp in files:
            (keep if _hour_key(path_stamp) in selected else delete).add(path)

    deleted = 0
    for path in sorted(delete - keep):
        try:
            path.unlink()
            deleted += 1
        except FileNotFoundError:
            pass

    return {
        "ok": True,
        "enabled": True,
        "deleted": deleted,
        "kept": len(keep),
        "root": str(root),
    }


async def prune_disk_logs() -> dict[str, Any]:
    return await asyncio.to_thread(prune_disk_logs_once)


async def run_disk_log_writer_and_pruner() -> None:
    last_prune = 0.0
    while True:
        await asyncio.sleep(_disk_interval_seconds())
        try:
            await flush_disk_logs()
        except Exception as exc:
            record_event("timing_disk_flush_error", kind="instant", error_type=type(exc).__name__)
        now = time.monotonic()
        if now - last_prune < _prune_interval_seconds():
            continue
        last_prune = now
        try:
            await prune_disk_logs()
        except Exception as exc:
            record_event("timing_disk_prune_error", kind="instant", error_type=type(exc).__name__)


def clear() -> int:
    with _lock:
        count = len(_events)
        _events.clear()
        _disk_pending_events.clear()
    return count


def state() -> dict[str, Any]:
    with _lock:
        count = len(_events)
        pending_disk_count = len(_disk_pending_events)
    disk_log_root = ""
    disk_error = ""
    if disk_logging_enabled():
        try:
            disk_log_root = str(_log_root())
        except RuntimeError as exc:
            disk_error = str(exc)
    return {
        "ok": True,
        "schema": "xarta.blueprints.timing.state.v1",
        "enabled": enabled(),
        "count": count,
        "capacity": _capacity(),
        "event_loop_interval_ms": round(_loop_interval_seconds() * 1000, 3),
        "event_loop_lag_threshold_ms": round(_loop_lag_threshold_seconds() * 1000, 3),
        "disk_writes": disk_logging_enabled() and not disk_error,
        "disk_interval_seconds": _disk_interval_seconds(),
        "disk_log_root": disk_log_root,
        "disk_error": disk_error,
        "pending_disk_events": pending_disk_count,
    }


def reset_for_tests() -> None:
    global _last_flushed_seq
    clear()
    _last_flushed_seq = 0
