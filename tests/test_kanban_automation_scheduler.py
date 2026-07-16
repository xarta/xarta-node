import asyncio
import functools
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import httpx
import pytest

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-kanban-scheduler-nodes.json"
NODES_JSON.write_text(
    json.dumps(
        {
            "nodes": [
                {
                    "node_id": "test-owner",
                    "display_name": "Thunderbird 1",
                    "host_machine": "test-host",
                    "primary_hostname": "test-owner.local",
                    "tailnet_hostname": "test-owner.tailnet",
                    "primary_ip": "127.0.0.1",
                    "sync_port": 8080,
                    "tailnet": "test",
                    "tailnet_ip": "100.64.0.1",
                    "active": True,
                }
            ]
        }
    ),
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-owner")
os.environ.setdefault("NODES_JSON_PATH", str(NODES_JSON))
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-kanban-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import kanban_automation_scheduler as scheduler  # noqa: E402
from app import routes_personal  # noqa: E402

TOKEN = "k" * 48


def async_test(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


def config(**overrides):
    values = {
        "base_url": "http://scheduler.test",
        "token": TOKEN,
        "heartbeat_interval": 0.01,
        "claim_interval": 0.01,
    }
    values.update(overrides)
    return scheduler.ProviderConfig(**values)


def client(handler):
    return httpx.AsyncClient(
        base_url="http://scheduler.test",
        transport=httpx.MockTransport(handler),
    )


def producer_config(mode):
    return {
        "producer_mode": mode,
        "owner_node_id": "test-owner",
        "scheduler_provider_effective_enabled": mode in {"scheduler_shadow", "scheduler"},
        "scheduler_mutations_enabled": mode == "scheduler",
    }


def claim(run_id="run-1"):
    return {
        "actionable": True,
        "run": {"run_id": run_id, "target_key": scheduler.TARGET_KEY},
        "target_config": {},
        "claim_token": "c" * 40,
        "fence_token": 7,
        "lease_until": "2999-01-01T00:00:00+00:00",
        "policies": {"lease_seconds": 900},
    }


def test_producer_modes_default_legacy_and_invalid_fail_closed(monkeypatch):
    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV, "test-owner")
    monkeypatch.delenv(routes_personal.KANBAN_AUTOMATION_PRODUCER_MODE_ENV, raising=False)
    legacy = routes_personal._work_automation_idle_worker_config()
    assert legacy["producer_mode"] == "legacy"
    assert legacy["legacy_loop_effective_enabled"] is True
    assert legacy["scheduler_provider_effective_enabled"] is False
    assert legacy["manual_recovery_enabled"] is True

    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_PRODUCER_MODE_ENV, "scheduler_shadow")
    shadow = routes_personal._work_automation_idle_worker_config()
    assert shadow["legacy_loop_effective_enabled"] is True
    assert shadow["scheduler_provider_effective_enabled"] is True
    assert shadow["scheduler_mutations_enabled"] is False

    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_PRODUCER_MODE_ENV, "scheduler")
    scheduled = routes_personal._work_automation_idle_worker_config()
    assert scheduled["legacy_loop_effective_enabled"] is False
    assert scheduled["scheduler_provider_effective_enabled"] is True
    assert scheduled["scheduler_mutations_enabled"] is True

    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_PRODUCER_MODE_ENV, "unexpected")
    fenced = routes_personal._work_automation_idle_worker_config()
    assert fenced["producer_mode"] == "fenced"
    assert fenced["producer_mode_valid"] is False
    assert fenced["legacy_loop_effective_enabled"] is False
    assert fenced["scheduler_provider_effective_enabled"] is False


@async_test
async def test_provider_does_not_start_in_legacy_mode(monkeypatch):
    async_client = client(lambda _request: pytest.fail("fenced provider must make no request"))
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("legacy"),
    )
    assert await instance.start() is False
    assert instance._loop_tasks == []
    await instance.stop()
    await async_client.aclose()


@async_test
async def test_provider_lifecycle_can_restart_after_clean_stop(monkeypatch):
    async_client = client(lambda _request: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("scheduler"),
    )
    starts = []

    async def idle_loop():
        starts.append(asyncio.current_task().get_name())
        await asyncio.Event().wait()

    monkeypatch.setattr(instance, "_heartbeat_loop", idle_loop)
    monkeypatch.setattr(instance, "_claim_loop", idle_loop)

    assert await instance.start() is True
    await asyncio.sleep(0)
    assert len(instance._loop_tasks) == 2
    await instance.stop()
    assert instance._loop_tasks == []

    assert await instance.start() is True
    await asyncio.sleep(0)
    assert len(instance._loop_tasks) == 2
    await instance.stop()
    assert len(starts) == 4
    await async_client.aclose()


@async_test
@pytest.mark.parametrize(
    "bad_claim",
    [
        {**claim(), "run": {"run_id": "run-1", "target_key": "arbitrary_url_v1"}},
        {**claim(), "target_config": {"url": "https://example.invalid"}},
        {**claim(), "target_config": None},
    ],
)
async def test_provider_rejects_non_exact_target_or_config(monkeypatch, bad_claim):
    async_client = client(lambda _request: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("scheduler"),
    )
    with pytest.raises(scheduler.TargetRejected):
        await instance._execute_target(bad_claim)
    await async_client.aclose()


@async_test
async def test_shadow_mode_is_read_only_and_reports_candidate_parity(monkeypatch):
    async_client = client(lambda _request: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("scheduler_shadow"),
    )
    called = []

    async def snapshot(**_kwargs):
        called.append("snapshot")
        return {
            "timeout_marker_ids": ["timeout-1"],
            "review_candidate_ids": ["review-1", "review-2"],
            "review_queue_item_ids": ["review-2"],
            "preprocessing_candidate_ids": ["prep-1"],
            "preprocessing_queue_item_ids": ["prep-1"],
            "claimable_marker_ids": ["marker-1"],
        }

    async def blocker(*, tick_id, worker_id, apply):
        called.append(("blocker", tick_id, worker_id, apply))
        return {"ok": True, "candidate_count": 3, "examined": 3, "processed": 0}

    async def forbidden_tick(**_kwargs):
        raise AssertionError("shadow provider must never call the mutating tick")

    monkeypatch.setattr(routes_personal, "work_kanban_automation_shadow_snapshot", snapshot)
    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", forbidden_tick)
    monkeypatch.setattr(scheduler, "_run_blocker_resolver", blocker)
    result = await instance._execute_target(claim("run-shadow"))
    assert result["outcome"] == "skipped"
    assert result["coverage_complete"] is True
    assert result["phase_counts"] == {
        "timeout_candidates": 1,
        "review_candidates": 2,
        "review_would_queue": 1,
        "preprocessing_candidates": 1,
        "preprocessing_would_queue": 1,
        "claimable_markers": 1,
        "blocker_candidates": 3,
        "blockers_examined": 3,
    }
    assert called == [
        "snapshot",
        (
            "blocker",
            "blueprints-kanban-automation:run-shadow",
            instance.worker_id,
            False,
        ),
    ]
    await async_client.aclose()


@async_test
async def test_shadow_mode_keeps_blocker_failures_visible(monkeypatch):
    async_client = client(lambda _request: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("scheduler_shadow"),
    )

    async def snapshot(**_kwargs):
        return {
            "timeout_marker_ids": [],
            "review_candidate_ids": [],
            "review_queue_item_ids": [],
            "preprocessing_candidate_ids": [],
            "preprocessing_queue_item_ids": [],
            "claimable_marker_ids": [],
        }

    async def blocker(*, tick_id, worker_id, apply):
        assert tick_id == "blueprints-kanban-automation:run-shadow-failure"
        assert worker_id == instance.worker_id
        assert apply is False
        return {
            "ok": False,
            "error": "blocker_resolver_timeout",
            "candidate_count": 3,
            "examined": 1,
            "processed": 0,
            "run_timeout_reached": True,
        }

    monkeypatch.setattr(routes_personal, "work_kanban_automation_shadow_snapshot", snapshot)
    monkeypatch.setattr(scheduler, "_run_blocker_resolver", blocker)
    result = await instance._execute_target(claim("run-shadow-failure"))
    assert result["outcome"] == "truncated"
    assert result["coverage_complete"] is False
    assert result["truncated"] is True
    assert result["error_count"] == 1
    assert result["failure_reasons"] == ["blocker_resolver_timeout"]
    await async_client.aclose()


@async_test
async def test_scheduler_tick_uses_stable_identity_and_durable_replay(monkeypatch):
    async_client = client(lambda _request: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: producer_config("scheduler"),
    )
    seen = {}
    receipt = {"value": None}

    async def get_receipt(tick_id):
        seen.setdefault("receipt_lookups", []).append(tick_id)
        return receipt["value"]

    async def tick(**kwargs):
        seen["tick"] = kwargs
        return {
            "lease_acquired": True,
            "eligible_marker_count": 1,
            "processed_count": 1,
            "processed_markers": [{"ok": True}],
            "timeout_requeue": {"requeued_count": 0},
            "review_scan": {"scanned_count": 2, "queued_count": 0},
            "preprocessing_scan": {"scanned_count": 3, "queued_count": 1},
        }

    async def blocker(*, tick_id, worker_id, apply):
        seen["blocker"] = (tick_id, worker_id, apply)
        return {
            "ok": True,
            "candidate_count": 1,
            "examined": 1,
            "processed": 1,
        }

    async def record(**kwargs):
        seen["record"] = kwargs
        receipt["value"] = kwargs["result"]
        return kwargs["result"]

    monkeypatch.setattr(routes_personal, "work_kanban_automation_tick_receipt", get_receipt)
    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", tick)
    monkeypatch.setattr(routes_personal, "record_work_kanban_automation_tick_receipt", record)
    monkeypatch.setattr(scheduler, "_run_blocker_resolver", blocker)

    first = await instance._execute_target(claim("run-stable"))
    assert first["scheduler_run_id"] == "run-stable"
    assert first["tick_id"] == "blueprints-kanban-automation:run-stable"
    assert first["producer_node_id"] == "test-owner"
    assert first["outcome"] == "completed"
    assert seen["tick"] == {
        "holder_id": instance.worker_id,
        "run_id": "blueprints-kanban-automation:run-stable",
        "actor": instance.worker_id,
        "source_surface": "xarta-scheduler-provider",
        "request_id": "blueprints-kanban-automation:run-stable",
        "producer_source": "scheduler",
    }
    assert seen["blocker"] == (
        "blueprints-kanban-automation:run-stable",
        instance.worker_id,
        True,
    )

    seen.pop("tick")
    seen.pop("blocker")
    second = await instance._execute_target(claim("run-stable"))
    assert second == first
    assert "tick" not in seen
    assert "blocker" not in seen
    await async_client.aclose()


@async_test
async def test_stale_scheduler_fence_is_rejected_before_target_execution(monkeypatch):
    def handler(request):
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(409, json={"accepted": False})
        raise AssertionError("stale fence must not reach another provider operation")

    async_client = client(handler)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    called = False

    async def target(_claim):
        nonlocal called
        called = True
        return {}

    monkeypatch.setattr(instance, "_execute_target", target)
    await instance._run_claim(claim("run-stale"))
    assert called is False
    assert instance._last_error == "claim:ClaimLost"
    await async_client.aclose()


@async_test
async def test_provider_timeout_cancels_target_and_reports_bounded_failure(monkeypatch):
    reports = []

    def handler(request):
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json={"accepted": True})
        if request.url.path.endswith("/fail"):
            reports.append(json.loads(request.content))
            return httpx.Response(200, json={"accepted": True})
        return httpx.Response(200, json={"accepted": True})

    async_client = client(handler)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    cancelled = asyncio.Event()

    async def target(_claim):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(instance, "_execute_target", target)
    monkeypatch.setattr(scheduler, "TARGET_TIMEOUT_SECONDS", 0.02)
    await instance._run_claim(claim("run-timeout"))
    assert cancelled.is_set()
    assert reports[0]["error"] == "TimeoutError: Kanban automation target failed"
    await async_client.aclose()


@async_test
async def test_manual_tick_preserves_request_identity(monkeypatch):
    captured = {}

    async def fake_tick(**kwargs):
        captured.update(kwargs)
        return {"ok": True}

    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", fake_tick)
    body = routes_personal.WorkAutomationIdleTickRequest(
        item_id="item-1",
        actor="codex",
        source_surface="manual-recovery-proof",
        request_id="request-1",
        run_id="run-1",
    )
    result = await routes_personal.trigger_work_automation_idle_worker_tick(body)
    assert result == {"ok": True}
    assert captured["run_id"] == "run-1"
    assert captured["actor"] == "codex"
    assert captured["source_surface"] == "manual-recovery-proof"
    assert captured["request_id"] == "request-1"
    assert captured["producer_source"] == "manual"


@async_test
async def test_blocker_resolver_command_is_fixed_and_bounded(monkeypatch):
    seen = {}

    class Process:
        returncode = 0

        async def communicate(self):
            return (
                json.dumps(
                    {
                        "ok": True,
                        "candidate_count": 0,
                        "examined": 0,
                        "processed": 0,
                    }
                ).encode(),
                b"",
            )

        def kill(self):
            raise AssertionError("bounded successful process must not be killed")

        async def wait(self):
            return 0

    async def create(*args, **kwargs):
        seen["args"] = args
        seen["kwargs"] = kwargs
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    report = await scheduler._run_blocker_resolver(
        tick_id="tick-1",
        worker_id="blueprints-kanban-automation:test-owner",
        apply=True,
    )
    assert report["ok"] is True
    args = seen["args"]
    assert args[:7] == (
        scheduler.DOCKER_BIN,
        "exec",
        "--user",
        scheduler.HERMES_CONTAINER_USER,
        scheduler.HERMES_CONTAINER,
        "python3",
        scheduler.HERMES_BLOCKER_SCRIPT,
    )
    assert "--apply" in args
    assert "--write-health" in args
    assert "--run-id" in args
    assert args[args.index("--run-id") + 1] == "tick-1:blockers"
    assert not any("http://" in str(value) or "https://" in str(value) for value in args)


def test_active_postgres_paths_use_transaction_helper_only():
    source = (APP_ROOT / "app/routes_personal.py").read_text(encoding="utf-8")
    assert source.count('conn.execute("BEGIN IMMEDIATE")') == 1
    for name in (
        "_claim_next_work_review_processor_marker_sync",
        "_complete_work_review_processor_marker_sync",
        "_requeue_timed_out_work_review_processor_markers_sync",
        "_acquire_work_review_processor_lease_sync",
        "_heartbeat_work_review_processor_lease_sync",
        "_release_work_review_processor_lease_sync",
    ):
        start = source.index(f"def {name}(")
        end = source.find("\ndef ", start + 10)
        fragment = source[start : end if end >= 0 else len(source)]
        assert "_kanban_begin_write_transaction(conn)" in fragment
        assert 'conn.execute("BEGIN IMMEDIATE")' not in fragment


def test_active_postgres_transaction_helper_never_touches_sqlite(monkeypatch):
    calls = []

    class Conn:
        def execute(self, statement):
            calls.append(statement)

    monkeypatch.setattr(routes_personal, "_kanban_active_store_is_postgres", lambda: True)
    routes_personal._kanban_begin_write_transaction(Conn())
    assert calls == []

    monkeypatch.setattr(routes_personal, "_kanban_active_store_is_postgres", lambda: False)
    routes_personal._kanban_begin_write_transaction(Conn())
    assert calls == ["BEGIN IMMEDIATE"]


@async_test
async def test_shadow_snapshot_blocking_database_boundary_stays_off_event_loop(monkeypatch):
    def slow_snapshot(_item_id, _max_scan_items):
        time.sleep(0.08)
        return {"schema": "xarta.kanban.automation.shadow_snapshot.v1"}

    monkeypatch.setattr(
        routes_personal,
        "_work_kanban_automation_shadow_snapshot_sync",
        slow_snapshot,
    )
    ticks = 0

    async def ticker():
        nonlocal ticks
        deadline = asyncio.get_running_loop().time() + 0.06
        while asyncio.get_running_loop().time() < deadline:
            ticks += 1
            await asyncio.sleep(0.005)

    snapshot, _ = await asyncio.gather(
        routes_personal.work_kanban_automation_shadow_snapshot(),
        ticker(),
    )
    assert snapshot["schema"] == "xarta.kanban.automation.shadow_snapshot.v1"
    assert ticks >= 5


def test_main_wires_fenced_provider_lifecycle_and_status_route():
    from app import main as app_main

    main_source = (APP_ROOT / "app/main.py").read_text(encoding="utf-8")
    assert 'kanban_automation_config["legacy_loop_effective_enabled"]' in main_source
    assert "await start_kanban_automation_scheduler()" in main_source
    assert "await stop_kanban_automation_scheduler()" in main_source
    assert "include_router(kanban_automation_scheduler_router" in main_source
    assert "/api/v1/personal/kanban/scheduler/status" in {
        route.path for route in app_main.app.routes
    }
