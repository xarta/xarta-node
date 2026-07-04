import asyncio
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import timing
from app.routes_timing import timing_jsonl


def test_timing_span_records_timeline_fields():
    timing.reset_for_tests()

    with timing.span("unit_span", route="test"):
        pass

    rows = timing.snapshot()
    assert len(rows) == 1
    row = rows[0]
    assert row["event"] == "unit_span"
    assert row["kind"] == "span"
    assert row["start_perf_ns"] <= row["end_perf_ns"]
    assert row["start_time_ns"] <= row["end_time_ns"]
    assert row["duration_ms"] >= 0


def test_timing_to_thread_records_queue_and_run_boundaries():
    timing.reset_for_tests()

    async def run():
        return await timing.to_thread("unit.work", lambda: "ok")

    assert asyncio.run(run()) == "ok"

    rows = timing.snapshot()
    assert len(rows) == 1
    row = rows[0]
    assert row["event"] == "thread_work"
    assert row["label"] == "unit.work"
    assert row["submitted_perf_ns"] <= row["run_start_perf_ns"] <= row["run_end_perf_ns"]
    assert row["queue_wait_ms"] >= 0
    assert row["run_ms"] >= 0


def test_timing_to_thread_custom_executor_preserves_trace_context():
    timing.reset_for_tests()

    async def run():
        with ThreadPoolExecutor(max_workers=1) as executor:
            token = timing.set_current_trace_id("unit-trace")
            try:
                return await timing.to_thread(
                    "unit.custom_executor",
                    timing.current_trace_id,
                    _executor=executor,
                )
            finally:
                timing.reset_current_trace_id(token)

    assert asyncio.run(run()) == "unit-trace"

    rows = timing.snapshot()
    assert len(rows) == 1
    row = rows[0]
    assert row["event"] == "thread_work"
    assert row["label"] == "unit.custom_executor"
    assert row["trace_id"] == "unit-trace"


def test_timing_jsonl_endpoint_returns_memory_buffer():
    timing.reset_for_tests()
    timing.record_event("unit_instant", kind="instant", detail="hello")

    response = asyncio.run(timing_jsonl(limit=10))
    lines = response.body.decode("utf-8").strip().splitlines()

    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "unit_instant"
    assert payload["detail"] == "hello"


def test_sanitize_query_redacts_sensitive_values():
    assert (
        timing.sanitize_query("token=secret&api_key=abc&q=health&empty=")
        == "token=%5Bredacted%5D&api_key=%5Bredacted%5D&q=health&empty="
    )


def test_flush_disk_logs_writes_lone_wolf_day_file(tmp_path, monkeypatch):
    timing.reset_for_tests()
    log_root = tmp_path / "blueprints-event-loop-logs"
    normalized = []
    monkeypatch.setattr(timing, "_LONE_WOLF_ROOT", tmp_path.resolve())
    monkeypatch.setattr(
        timing,
        "_normalize_node_local_ownership",
        lambda path, *, root=None: normalized.append((Path(path), root)),
    )
    monkeypatch.setenv("BLUEPRINTS_TIMING_LOG_ROOT", str(log_root))
    monkeypatch.setenv("BLUEPRINTS_TIMING_DISK_ENABLED", "true")

    timing.record_event("unit_instant", kind="instant", detail="hello")

    result = timing.flush_disk_logs_once(datetime(2026, 7, 3, 12, 34, 56, tzinfo=UTC))

    path = log_root / "2026" / "07" / "03" / "123456-blueprints-event-loop.jsonl"
    assert result["written"] is True
    assert result["path"] == str(path)
    payload = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["event"] == "unit_instant"
    assert payload["detail"] == "hello"
    assert stat_mode(path) == 0o600
    assert stat_mode(path.parent) == 0o700
    assert normalized == [
        (path.with_suffix(path.suffix + ".tmp"), tmp_path.resolve()),
        (path, tmp_path.resolve()),
    ]


def test_state_reports_guard_error_for_log_root_outside_lone_wolf(tmp_path, monkeypatch):
    timing.reset_for_tests()
    monkeypatch.setattr(timing, "_LONE_WOLF_ROOT", (tmp_path / "allowed").resolve())
    monkeypatch.setenv("BLUEPRINTS_TIMING_LOG_ROOT", str(tmp_path / "outside"))
    monkeypatch.setenv("BLUEPRINTS_TIMING_DISK_ENABLED", "true")

    state = timing.state()

    assert state["disk_writes"] is False
    assert "timing log root must stay under" in state["disk_error"]


def test_prune_keeps_high_and_low_hours_for_old_saturday(tmp_path, monkeypatch):
    timing.reset_for_tests()
    log_root = tmp_path / "blueprints-event-loop-logs"
    monkeypatch.setattr(timing, "_LONE_WOLF_ROOT", tmp_path.resolve())
    monkeypatch.setenv("BLUEPRINTS_TIMING_LOG_ROOT", str(log_root))
    monkeypatch.setenv("BLUEPRINTS_TIMING_DISK_ENABLED", "true")

    ordinary = write_log(log_root, "2026/03/07", "010000-blueprints-event-loop.jsonl", 25)
    high = write_log(log_root, "2026/03/07", "020000-blueprints-event-loop.jsonl", 900)
    low = write_log(log_root, "2026/03/07", "030000-blueprints-event-loop.jsonl", 1)

    result = timing.prune_disk_logs_once(datetime(2026, 7, 3, tzinfo=UTC))

    assert result["deleted"] == 1
    assert not ordinary.exists()
    assert high.exists()
    assert low.exists()


def stat_mode(path: Path) -> int:
    return os.stat(path).st_mode & 0o777


def write_log(root: Path, day: str, filename: str, duration_ms: float) -> Path:
    path = root / day / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"duration_ms": duration_ms}) + "\n", encoding="utf-8")
    return path
