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
                    "node_id": "test-node",
                    "display_name": "Thunderbird 1",
                    "host_machine": "test-host",
                    "primary_hostname": "test-node.local",
                    "tailnet_hostname": "test-node.tailnet",
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
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
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
        "owner_node_id": "test-node",
        "scheduler_provider_effective_enabled": mode in {"scheduler_shadow", "scheduler"},
        "scheduler_mutations_enabled": mode == "scheduler",
    }


def claim(run_id="run-1", target_key=scheduler.AUTOMATION_TARGET_KEY):
    return {
        "actionable": True,
        "run": {"run_id": run_id, "target_key": target_key},
        "target_config": {},
        "claim_token": "c" * 40,
        "fence_token": 7,
        "lease_until": "2999-01-01T00:00:00+00:00",
        "policies": {"lease_seconds": 900},
    }


@async_test
async def test_restart_pause_waits_for_inflight_claim_materialization(monkeypatch):
    async_client = client(lambda _: None)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    instance.worker_id = "test-worker"
    entered = asyncio.Event()
    release_response = asyncio.Event()
    keep_target = asyncio.Event()
    request_count = 0

    async def request_json(method, path, **_kwargs):
        nonlocal request_count
        assert method == "POST"
        assert path.endswith("/claims/next")
        request_count += 1
        entered.set()
        await release_response.wait()
        return {"claim": claim("run-race")}

    async def run_claim(_claim):
        await keep_target.wait()

    async def current_producer():
        return {**producer_config("scheduler"), "legacy_loop_effective_enabled": False}

    monkeypatch.setattr(instance, "_request_json", request_json)
    monkeypatch.setattr(instance, "_run_claim", run_claim)
    monkeypatch.setattr(instance, "_producer_config", current_producer)
    claim_loop = asyncio.create_task(instance._claim_loop(), name="kanban-scheduler-claims")
    instance._loop_tasks = [claim_loop]
    await entered.wait()

    pause_task = asyncio.create_task(instance.pause_new_claims_for_restart())
    await asyncio.sleep(0)
    assert pause_task.done() is False
    release_response.set()
    paused = await asyncio.wait_for(pause_task, timeout=0.2)

    assert paused["active_run_ids"] == ["run-race"]
    assert request_count == 1
    await asyncio.sleep(0.03)
    assert request_count == 1
    assert await instance.resume_new_claims_after_restart_abort() is True
    await instance.stop()
    await async_client.aclose()


def test_producer_modes_default_legacy_and_invalid_fail_closed(monkeypatch):
    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV, "test-node")
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
async def test_lazy_provider_config_and_client_creation_stay_bounded_off_event_loop(
    monkeypatch,
):
    loaded = config()
    calls = []

    async def to_thread(func, *args, **kwargs):
        calls.append((func, args, kwargs))
        return func(*args, **kwargs)

    created = client(lambda _request: httpx.Response(200, json={"ok": True}))
    monkeypatch.setattr(asyncio, "to_thread", to_thread)
    monkeypatch.setattr(scheduler, "load_provider_config", lambda: loaded)
    monkeypatch.setattr(
        scheduler,
        "_scheduler_http_client",
        lambda received: calls.append(("client", received)) or created,
    )

    instance = scheduler.KanbanAutomationScheduler()
    response = await instance._request_json("GET", "/status")

    assert response == {"ok": True}
    assert calls[0][0] is scheduler.load_provider_config
    assert calls[1] == ("client", loaded)
    assert instance.config == loaded
    assert instance.client is created
    await instance.stop()


@async_test
async def test_producer_config_and_singleton_file_check_stay_off_event_loop(monkeypatch):
    loaded = producer_config("legacy")
    calls = []

    async def to_thread(func, *args, **kwargs):
        calls.append((func, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", to_thread)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: loaded,
    )
    instance = scheduler.KanbanAutomationScheduler(config=config())

    assert await instance._producer_config() is loaded
    assert calls == [
        (routes_personal._work_automation_idle_worker_config, (), {}),
    ]
    assert instance.worker_id == "blueprints-kanban-automation:test-node"


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

    async def forbidden_tick(**_kwargs):
        raise AssertionError("shadow provider must never call the mutating tick")

    monkeypatch.setattr(routes_personal, "work_kanban_automation_shadow_snapshot", snapshot)
    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", forbidden_tick)
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
    }
    assert called == ["snapshot"]
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

    async def blocker(*, tick_id, worker_id, apply):
        assert tick_id == "blueprints-kanban-automation:blocker:run-shadow-failure"
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

    monkeypatch.setattr(scheduler, "_run_blocker_resolver", blocker)
    result = await instance._execute_target(
        claim("run-shadow-failure", scheduler.BLOCKER_TARGET_KEY)
    )
    assert result["schema"] == scheduler.BLOCKER_RESULT_SCHEMA
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

    async def record(**kwargs):
        seen["record"] = kwargs
        receipt["value"] = kwargs["result"]
        return kwargs["result"]

    monkeypatch.setattr(routes_personal, "work_kanban_automation_tick_receipt", get_receipt)
    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", tick)
    monkeypatch.setattr(routes_personal, "record_work_kanban_automation_tick_receipt", record)

    first = await instance._execute_target(claim("run-stable"))
    assert first["scheduler_run_id"] == "run-stable"
    assert first["tick_id"] == "blueprints-kanban-automation:run-stable"
    assert first["producer_node_id"] == "test-node"
    assert first["outcome"] == "completed"
    assert seen["tick"] == {
        "holder_id": instance.worker_id,
        "run_id": "blueprints-kanban-automation:run-stable",
        "actor": instance.worker_id,
        "source_surface": "xarta-scheduler-provider",
        "request_id": "blueprints-kanban-automation:run-stable",
        "producer_source": "scheduler",
    }
    seen.pop("tick")
    second = await instance._execute_target(claim("run-stable"))
    assert second == first
    assert "tick" not in seen
    await async_client.aclose()


@async_test
async def test_blocker_tick_has_separate_identity_and_durable_replay(monkeypatch):
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
    monkeypatch.setattr(routes_personal, "record_work_kanban_automation_tick_receipt", record)
    monkeypatch.setattr(scheduler, "_run_blocker_resolver", blocker)

    target_claim = claim("run-blocker", scheduler.BLOCKER_TARGET_KEY)
    first = await instance._execute_target(target_claim)
    assert first["schema"] == scheduler.BLOCKER_RESULT_SCHEMA
    assert first["tick_id"] == "blueprints-kanban-automation:blocker:run-blocker"
    assert first["phase_counts"]["blockers_processed"] == 1
    assert seen["blocker"] == (
        "blueprints-kanban-automation:blocker:run-blocker",
        instance.worker_id,
        True,
    )

    seen.pop("blocker")
    second = await instance._execute_target(target_claim)
    assert second == first
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
async def test_claim_report_retry_is_bounded_and_does_not_create_hidden_retry_owner(
    monkeypatch,
):
    attempts = 0

    def handler(_request):
        nonlocal attempts
        attempts += 1
        return httpx.Response(503, json={"detail": "scheduler report unavailable"})

    async_client = client(handler)
    instance = scheduler.KanbanAutomationScheduler(config=config(), client=async_client)
    monkeypatch.setattr(scheduler, "REPORT_TIMEOUT_SECONDS", 0.03)
    lost = asyncio.Event()
    started = time.monotonic()

    accepted = await instance._report_claim(
        claim("run-report-bound"),
        "complete",
        {"result": {"schema": "bounded-test"}},
        lost,
    )

    assert accepted is False
    assert attempts >= 1
    assert time.monotonic() - started < 0.5
    assert lost.is_set() is False
    assert instance._last_error == "claim:complete_report_timeout"
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
async def test_operator_authorized_preprocessing_recovery_keeps_requester_separate(
    monkeypatch,
):
    captured = {}

    def worker_config():
        return {
            "enabled": True,
            "manual_recovery_enabled": True,
            "runs_on_this_node": True,
            "current_node_id": "test-node",
            "owner_node_id": "test-node",
            "producer_mode": "scheduler",
            "singleton_override": {},
        }

    async def fake_sync(func, *args, **kwargs):
        if func is routes_personal._work_operator_authorized_preprocessing_recovery_preflight_sync:
            assert args == ("leaf-1",)
            return {"ok": True, "reason": "initial_missing_or_queued"}
        if func is routes_personal._work_automation_processor_profile_drift:
            assert args == ("preprocessing",)
            return {"problems": [], "warnings": []}
        raise AssertionError(f"unexpected sync function: {func}")

    async def fake_tick(**kwargs):
        captured.update(kwargs)
        return {"ok": True, "effective_enabled": True, "processed_count": 1}

    monkeypatch.setattr(routes_personal, "_work_automation_idle_worker_config", worker_config)
    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", fake_sync)
    monkeypatch.setattr(routes_personal, "run_work_kanban_automation_idle_tick", fake_tick)

    result = await routes_personal.trigger_work_operator_authorized_preprocessing_recovery(
        routes_personal.WorkOperatorAuthorizedPreprocessingRecoveryRequest(
            item_id="leaf-1",
            operator_authorized=True,
            authorization_ref="operator-turn-2026-07-17",
            actor="codex",
            source_surface="blueprints-work-management",
            request_id="request-1",
            run_id="run-1",
        )
    )

    assert result["ok"] is True
    assert result["reason"] == "operator_authorized_preprocessing_triggered"
    assert result["requester"] == {
        "actor": "codex",
        "source_surface": "blueprints-work-management",
        "request_id": "request-1",
        "run_id": "run-1",
    }
    assert captured["item_id"] == "leaf-1"
    assert captured["max_scan_items"] == 1
    assert captured["max_process_items"] == 1
    assert captured["processor_kind"] == "preprocessing"
    assert captured["holder_id"] == "kanban-idle-worker"
    assert captured["actor"] == "kanban-idle-worker"
    assert captured["source_surface"] == "kanban-automation-idle-worker"
    initiation = captured["source_metadata_extra"]["operator_authorized_recovery"]
    assert initiation["authorization_ref"] == "operator-turn-2026-07-17"
    assert initiation["requester"] == result["requester"]


@async_test
async def test_operator_authorized_preprocessing_recovery_requires_explicit_authorization(
    monkeypatch,
):
    called = False

    def forbidden_preflight(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("preflight must not run without explicit authorization")

    monkeypatch.setattr(
        routes_personal,
        "_work_operator_authorized_preprocessing_recovery_preflight_sync",
        forbidden_preflight,
    )
    result = await routes_personal.trigger_work_operator_authorized_preprocessing_recovery(
        routes_personal.WorkOperatorAuthorizedPreprocessingRecoveryRequest(
            item_id="leaf-1",
            operator_authorized=False,
            authorization_ref="",
            actor="codex",
            source_surface="blueprints-work-management",
        )
    )

    assert result["ok"] is False
    assert result["reason"] == "operator_authorization_required"
    assert called is False


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
        worker_id="blueprints-kanban-automation:test-node",
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


@async_test
async def test_scheduler_status_coalesces_only_concurrent_fresh_builds(monkeypatch):
    instance = scheduler.KanbanAutomationScheduler(
        config=config(),
    )
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def build_status():
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"ok": True, "build": calls}

    monkeypatch.setattr(instance, "_build_status_payload", build_status)

    tasks = [asyncio.create_task(instance.status_payload()) for _ in range(4)]
    await started.wait()
    await asyncio.sleep(0)
    assert calls == 1

    release.set()
    results = await asyncio.gather(*tasks)
    assert results == [{"ok": True, "build": 1}] * 4

    release.clear()
    started.clear()
    next_task = asyncio.create_task(instance.status_payload())
    await started.wait()
    assert calls == 2
    release.set()
    assert await next_task == {"ok": True, "build": 2}


@async_test
async def test_scheduler_status_waiter_cancellation_does_not_cancel_shared_build(monkeypatch):
    instance = scheduler.KanbanAutomationScheduler(
        config=config(),
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def build_status():
        started.set()
        await release.wait()
        return {"ok": True}

    monkeypatch.setattr(instance, "_build_status_payload", build_status)

    first = asyncio.create_task(instance.status_payload())
    second = asyncio.create_task(instance.status_payload())
    await started.wait()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    assert instance._status_inflight_task is not None
    assert not instance._status_inflight_task.cancelled()
    release.set()
    assert await second == {"ok": True}


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
