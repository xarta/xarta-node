import asyncio
import functools
import json
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from fastapi import HTTPException

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-search-scheduler-nodes.json"
NODES_JSON.write_text(
    json.dumps(
        {
            "nodes": [
                {
                    "node_id": "test-node",
                    "display_name": "Test Node",
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-search-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import personal_search_scheduler as scheduler  # noqa: E402

TOKEN = "a" * 48


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


def status_payloads(*, schedules=None, provider=None):
    return {
        "/status": {
            "service": "xarta-scheduler",
            "version": "1.2.3",
            "health": {"ok": True, "database": "ok"},
        },
        f"/providers/{scheduler.PROVIDER_ID}/status": provider
        or {
            "provider_id": scheduler.PROVIDER_ID,
            "available": True,
            "stale_after_seconds": 12,
            "last_seen_at": "2026-07-15T12:00:00+00:00",
            "queued_runs": 0,
            "running_runs": 0,
            "latest_success": {
                "run_id": "run-old",
                "schedule_id": "schedule-1",
                "finished_at": "2026-07-15T11:59:59+00:00",
                "result": {"index_updated_at": "2026-07-15T11:59:58+00:00"},
            },
            "latest_failure": None,
            "source_of_truth": "xarta-scheduler-postgresql",
        },
        "/schedules": {
            "schedules": schedules
            if schedules is not None
            else [
                {
                    "schedule_id": "schedule-1",
                    "target_key": scheduler.TARGET_KEY,
                    "provider_id": scheduler.PROVIDER_ID,
                    "enabled": True,
                    "schedule": {"seconds": 1},
                    "next_run_at": "2026-07-15T12:00:01+00:00",
                    "archived_at": "",
                }
            ]
        },
        "/history": {
            "runs": [
                {
                    "run_id": "run-old",
                    "status": "completed",
                    "finished_at": "2026-07-15T11:59:59+00:00",
                }
            ]
        },
    }


def scheduler_handler(payloads, seen=None):
    def handler(request):
        path = request.url.path
        if seen is not None:
            seen.append((request.method, path, dict(request.headers), request.content))
        key = "/history" if path == "/history" else path
        payload = payloads.get(key)
        if payload is None:
            return httpx.Response(404, json={"detail": "missing"})
        return httpx.Response(200, json=payload)

    return handler


@async_test
async def test_restart_pause_waits_for_inflight_claim_materialization():
    instance = scheduler.PersonalSearchScheduler(config=config(), client=client(lambda _: None))
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
        return {
            "claim": {
                "actionable": True,
                "run": {"run_id": "run-race", "target_key": scheduler.TARGET_KEY},
                "target_config": {},
                "claim_token": "c" * 40,
                "fence_token": 7,
                "lease_until": "2999-01-01T00:00:00+00:00",
                "policies": {"lease_seconds": 60},
            }
        }

    async def run_claim(_claim):
        await keep_target.wait()

    instance._request_json = request_json
    instance._run_claim = run_claim
    claim_loop = asyncio.create_task(
        instance._claim_loop(), name="personal-search-scheduler-claims"
    )
    instance._loop_tasks = [claim_loop]
    await entered.wait()

    pause_task = asyncio.create_task(instance.pause_new_claims_for_restart())
    await asyncio.sleep(0)
    assert pause_task.done() is False
    release_response.set()
    paused = await asyncio.wait_for(pause_task, timeout=0.2)

    assert paused["provider_effective_enabled"] is True
    assert paused["active_run_ids"] == ["run-race"]
    assert request_count == 1
    await asyncio.sleep(0.03)
    assert request_count == 1
    assert await instance.resume_new_claims_after_restart_abort() is True
    await instance.stop()
    await instance.client.aclose()


def test_source_signature_covers_imports_and_discussions(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    for table in scheduler.SOURCE_TABLES:
        conn.execute(f"CREATE TABLE {table} (record_id TEXT, updated_at TEXT)")

    @contextmanager
    def fake_conn(**_kwargs):
        yield conn

    monkeypatch.setattr(scheduler.routes_personal, "_sqlite_get_read_conn", fake_conn)
    monkeypatch.setattr(
        scheduler.routes_personal,
        "_kanban_active_store_is_postgres",
        lambda: False,
    )
    first = scheduler._source_signature_sync()
    second = scheduler._source_signature_sync()
    assert first == second
    assert [item["table"] for item in first["tables"]] == list(scheduler.SOURCE_TABLES)

    conn.execute("INSERT INTO personal_import_batches VALUES ('import-1', '2026-07-15T12:00:00Z')")
    after_import = scheduler._source_signature_sync()
    assert after_import["source_signature"] != first["source_signature"]

    conn.execute("INSERT INTO kanban_discussions VALUES ('discussion-1', '2026-07-15T12:00:01Z')")
    after_discussion = scheduler._source_signature_sync()
    assert after_discussion["source_signature"] != after_import["source_signature"]


def test_source_signature_separates_sqlite_and_nontransactional_postgres(monkeypatch):
    sqlite_conn = sqlite3.connect(":memory:")
    sqlite_conn.row_factory = sqlite3.Row
    postgres_conn = sqlite3.connect(":memory:")
    postgres_conn.row_factory = sqlite3.Row
    for table in scheduler.PERSONAL_SOURCE_TABLES:
        sqlite_conn.execute(f"CREATE TABLE {table} (record_id TEXT, updated_at TEXT)")
        sqlite_conn.execute(
            f"INSERT INTO {table} VALUES (?, ?)",
            (f"{table}-row", "2026-07-16T15:00:00Z"),
        )
    for table in scheduler.KANBAN_SOURCE_TABLES:
        postgres_conn.execute(f"CREATE TABLE {table} (record_id TEXT, updated_at TEXT)")
        postgres_conn.execute(
            f"INSERT INTO {table} VALUES (?, ?)",
            (f"{table}-row", "2026-07-16T15:00:01Z"),
        )

    lifecycle = []

    @contextmanager
    def sqlite_read(**kwargs):
        assert kwargs == {
            "busy_timeout_ms": 100,
            "operation": "personal_search_source_signature",
        }
        lifecycle.append("sqlite-open")
        try:
            yield sqlite_conn
        finally:
            lifecycle.append("sqlite-close")

    @contextmanager
    def postgres_read(**kwargs):
        assert kwargs == {
            "operation": "personal_search_source_signature",
            "transactional": False,
        }
        lifecycle.append("postgres-open")
        try:
            yield postgres_conn
        finally:
            lifecycle.append("postgres-close")

    monkeypatch.setattr(scheduler.routes_personal, "_sqlite_get_read_conn", sqlite_read)
    monkeypatch.setattr(scheduler.routes_personal, "_kanban_postgres_get_conn", postgres_read)
    monkeypatch.setattr(
        scheduler.routes_personal,
        "_kanban_active_store_is_postgres",
        lambda: True,
    )

    signature = scheduler._source_signature_sync()

    assert [item["table"] for item in signature["tables"]] == list(scheduler.SOURCE_TABLES)
    assert signature["source_rows"] == len(scheduler.SOURCE_TABLES)
    assert lifecycle == [
        "sqlite-open",
        "sqlite-close",
        "postgres-open",
        "postgres-close",
    ]


@async_test
async def test_unchanged_signature_skips_and_retains_index_time(monkeypatch):
    instance = scheduler.PersonalSearchScheduler(config=config(), client=client(lambda _: None))
    previous = {
        "result": {
            "source_signature": "sha256:same",
            "index_updated_at": "2026-07-15T10:00:00+00:00",
            "documents": {"document_count": 4275},
        }
    }

    async def prior():
        return previous

    called = []

    async def fake_to_thread(label, func, *args, **kwargs):
        called.append(label)
        if "source_signature" in label:
            return {"source_signature": "sha256:same", "source_rows": 4275, "tables": []}
        raise AssertionError("full document sync must not run for an unchanged source signature")

    monkeypatch.setattr(instance, "_previous_success", prior)
    monkeypatch.setattr(scheduler.timing, "to_thread", fake_to_thread)
    result = await instance._execute_target(
        {"run": {"target_key": scheduler.TARGET_KEY}, "target_config": {}}
    )
    assert result == {
        "schema": scheduler.RESULT_SCHEMA,
        "skipped": True,
        "reason": "source_signature_unchanged",
        "source_signature": "sha256:same",
        "index_updated_at": "2026-07-15T10:00:00+00:00",
        "documents": {
            "document_count": 4275,
            "source_rows": 4275,
            "updated": 0,
            "deleted": 0,
            "unchanged": 4275,
        },
    }
    assert called == ["personal.search_scheduler.source_signature"]


@async_test
async def test_changed_signature_runs_full_sync_off_loop(monkeypatch):
    instance = scheduler.PersonalSearchScheduler(config=config(), client=client(lambda _: None))

    async def prior():
        return {"result": {"source_signature": "sha256:old", "index_updated_at": "old-time"}}

    labels = []

    async def fake_to_thread(label, func, *args, **kwargs):
        labels.append(label)
        if "source_signature" in label:
            return {"source_signature": "sha256:new", "source_rows": 4276, "tables": []}
        return {
            "generated_at": "2026-07-15T12:00:00+00:00",
            "source_signature": "sha256:must-not-certify-post-sync-state",
            "documents": {"document_count": 4276, "updated": 1},
        }

    monkeypatch.setattr(instance, "_previous_success", prior)
    monkeypatch.setattr(scheduler.timing, "to_thread", fake_to_thread)
    result = await instance._execute_target(
        {
            "run": {"target_key": scheduler.TARGET_KEY},
            "target_config": {"include_embeddings": False},
        }
    )
    assert result["skipped"] is False
    assert result["source_signature"] == "sha256:new"
    assert result["index_updated_at"] == "2026-07-15T12:00:00+00:00"
    assert labels == [
        "personal.search_scheduler.source_signature",
        "personal.search_scheduler.document_sync",
    ]


@async_test
@pytest.mark.parametrize(
    "claim",
    [
        {"run": {"target_key": "arbitrary_url_v1"}, "target_config": {}},
        {
            "run": {"target_key": scheduler.TARGET_KEY},
            "target_config": {"include_embeddings": True},
        },
        {
            "run": {"target_key": scheduler.TARGET_KEY},
            "target_config": {"shell": "anything"},
        },
    ],
)
async def test_provider_rejects_every_non_exact_or_embedding_target(claim):
    instance = scheduler.PersonalSearchScheduler(config=config(), client=client(lambda _: None))
    with pytest.raises(scheduler.TargetRejected):
        await instance._execute_target(claim)


@async_test
async def test_status_is_direct_raw_and_derives_thresholds():
    seen = []
    async_client = client(scheduler_handler(status_payloads(), seen))
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    payload = await instance.status_payload()
    assert payload["schema"] == scheduler.STATUS_SCHEMA
    assert payload["scheduler"] == {
        "available": True,
        "version": "1.2.3",
        "health": {"ok": True, "database": "ok"},
    }
    assert payload["provider"]["last_seen_at"] == "2026-07-15T12:00:00+00:00"
    assert payload["provider"]["latest_success"]["finished_at"] == ("2026-07-15T11:59:59+00:00")
    assert payload["schedule"]["schedule_definition"] == {"seconds": 1}
    assert payload["thresholds"] == {
        "heartbeat_stale_seconds": 12,
        "success_stale_seconds": 20,
    }
    assert [path for _method, path, _headers, _body in seen] == [
        "/status",
        f"/providers/{scheduler.PROVIDER_ID}/status",
        "/schedules",
        "/history",
    ]
    await async_client.aclose()


@async_test
async def test_status_down_is_structured_and_safe():
    def handler(_request):
        raise httpx.ConnectError("secret upstream detail")

    async_client = client(handler)
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    payload = await instance.status_payload()
    assert payload["scheduler"]["available"] is False
    assert payload["scheduler"]["health"] == {
        "ok": False,
        "classification": "ConnectError",
    }
    assert payload["provider"] is None
    assert "secret" not in json.dumps(payload)
    await async_client.aclose()


@async_test
async def test_status_duplicate_is_unhealthy_and_never_selects_silently():
    schedule = status_payloads()["/schedules"]["schedules"][0]
    payloads = status_payloads(schedules=[schedule, {**schedule, "schedule_id": "schedule-2"}])
    async_client = client(scheduler_handler(payloads))
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    payload = await instance.status_payload()
    assert payload["schedule"] is None
    assert payload["scheduler"]["health"]["ok"] is False
    assert payload["scheduler"]["health"]["classification"] == (
        "ambiguous_personal_search_schedule"
    )
    await async_client.aclose()


@async_test
@pytest.mark.parametrize(
    ("schedules", "expected_schedule"),
    [
        ([], None),
        ([{**status_payloads()["/schedules"]["schedules"][0], "enabled": False}], False),
    ],
)
async def test_status_represents_missing_and_disabled_schedule(schedules, expected_schedule):
    async_client = client(scheduler_handler(status_payloads(schedules=schedules)))
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    payload = await instance.status_payload()
    if expected_schedule is None:
        assert payload["schedule"] is None
    else:
        assert payload["schedule"]["enabled"] is expected_schedule
    assert payload["scheduler"]["available"] is True
    await async_client.aclose()


@async_test
@pytest.mark.parametrize(
    ("schedules", "expected_status"),
    [
        ([], 503),
        ([{**status_payloads()["/schedules"]["schedules"][0], "enabled": False}], 409),
        (
            [
                status_payloads()["/schedules"]["schedules"][0],
                {
                    **status_payloads()["/schedules"]["schedules"][0],
                    "schedule_id": "schedule-2",
                },
            ],
            409,
        ),
    ],
)
async def test_run_now_missing_disabled_and_duplicate_are_bounded(schedules, expected_status):
    async_client = client(scheduler_handler(status_payloads(schedules=schedules)))
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    with pytest.raises(HTTPException) as caught:
        await instance.run_now()
    assert caught.value.status_code == expected_status
    await async_client.aclose()


@async_test
async def test_run_now_generates_backend_provenance_and_returns_queue_ack():
    seen = []
    payloads = status_payloads()

    def handler(request):
        seen.append(request)
        if request.url.path == "/schedules/schedule-1/run-now":
            return httpx.Response(202, json={"run": {"run_id": "run-1", "status": "queued"}})
        return scheduler_handler(payloads)(request)

    async_client = client(handler)
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    result = await instance.run_now()
    assert result["run"] == {"run_id": "run-1", "status": "queued"}
    request = seen[-1]
    body = json.loads(request.content)
    assert body["actor"] == "blueprints-backend"
    assert body["source_surface"] == "personal-search"
    assert body["request_id"].startswith("blueprints-personal-search-run-now-")
    assert "authorization" not in request.headers
    await async_client.aclose()


@async_test
async def test_provider_heartbeat_continues_while_job_runs_and_stop_cancels(monkeypatch):
    heartbeats = []
    claimed = False
    started = asyncio.Event()
    cancelled = asyncio.Event()

    claim = {
        "actionable": True,
        "run": {"run_id": "run-long", "target_key": scheduler.TARGET_KEY},
        "target_config": {},
        "claim_token": "c" * 40,
        "fence_token": 7,
        "lease_until": "2999-01-01T00:00:00+00:00",
        "policies": {"lease_seconds": 5},
    }

    def handler(request):
        nonlocal claimed
        assert request.headers.get("Authorization") == f"Bearer {TOKEN}"
        if request.url.path.endswith("/heartbeat"):
            body = json.loads(request.content)
            if request.url.path.endswith("run-long/heartbeat"):
                assert body["claim_token"] == "c" * 40
                assert body["fence_token"] == 7
                return httpx.Response(200, json={"accepted": True})
            heartbeats.append(body)
            return httpx.Response(200, json={"accepted": True})
        if request.url.path.endswith("/claims/next"):
            if not claimed:
                claimed = True
                return httpx.Response(200, json={"claim": claim})
            return httpx.Response(200, json={"claim": {"actionable": False}})
        return httpx.Response(200, json={"accepted": True})

    async_client = client(handler)
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)

    async def long_target(_claim):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(instance, "_execute_target", long_target)
    assert await instance.start() is True
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.06)
    assert len(heartbeats) >= 3
    assert any(
        item["state"] == "running" and item["current_run_ids"] == ["run-long"]
        for item in heartbeats
    )
    await instance.stop()
    assert cancelled.is_set()
    await async_client.aclose()


@async_test
async def test_target_exception_is_bounded_and_next_claim_recovers(monkeypatch):
    reports = []

    def handler(request):
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json={"accepted": True})
        if request.url.path.endswith("/fail") or request.url.path.endswith("/complete"):
            reports.append((request.url.path, json.loads(request.content)))
            return httpx.Response(200, json={"accepted": True})
        return httpx.Response(200, json={})

    async_client = client(handler)
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    calls = 0

    async def target(_claim):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("secret target detail")
        return {"schema": scheduler.RESULT_SCHEMA, "skipped": True}

    monkeypatch.setattr(instance, "_execute_target", target)

    def claim(run_id):
        return {
            "actionable": True,
            "run": {"run_id": run_id, "target_key": scheduler.TARGET_KEY},
            "target_config": {},
            "claim_token": "d" * 40,
            "fence_token": 9,
            "lease_until": "2999-01-01T00:00:00+00:00",
            "policies": {"lease_seconds": 5},
        }

    await instance._run_claim(claim("run-fail"))
    await instance._run_claim(claim("run-recover"))
    assert reports[0][0].endswith("run-fail/fail")
    assert reports[0][1]["error"] == "RuntimeError: Personal Search target failed"
    assert "secret" not in json.dumps(reports)
    assert reports[1][0].endswith("run-recover/complete")
    assert instance._last_error == ""
    await async_client.aclose()


@async_test
async def test_claim_heartbeat_409_immediately_marks_fence_lost():
    def handler(_request):
        return httpx.Response(409, json={"accepted": False})

    async_client = client(handler)
    instance = scheduler.PersonalSearchScheduler(config=config(), client=async_client)
    stop = asyncio.Event()
    lost = asyncio.Event()
    claim = {
        "run": {"run_id": "run-fenced"},
        "claim_token": "e" * 40,
        "fence_token": 11,
        "lease_until": "2999-01-01T00:00:00+00:00",
        "policies": {"lease_seconds": 60},
    }
    await asyncio.wait_for(instance._claim_maintainer(claim, stop, lost), timeout=0.2)
    assert lost.is_set()
    await async_client.aclose()


def test_token_file_requires_mode_0600(monkeypatch, tmp_path):
    token_path = tmp_path / "provider-token"
    token_path.write_text(TOKEN, encoding="ascii")
    token_path.chmod(0o644)
    monkeypatch.delenv(scheduler.TOKEN_ENV, raising=False)
    monkeypatch.setenv(scheduler.TOKEN_FILE_ENV, str(token_path))
    with pytest.raises(ValueError, match="mode-0600"):
        scheduler._provider_token()
    token_path.chmod(0o600)
    assert scheduler._provider_token() == TOKEN


@async_test
async def test_provider_loops_do_not_start_without_token():
    async_client = client(lambda _request: pytest.fail("disabled provider must make no request"))
    instance = scheduler.PersonalSearchScheduler(
        config=config(token=""),
        client=async_client,
    )
    assert await instance.start() is False
    assert instance._loop_tasks == []
    await instance.stop()
    await async_client.aclose()


def test_main_wires_router_and_lifecycle_without_search_get_sync_regression():
    from app import main as app_main

    main_source = (APP_ROOT / "app/main.py").read_text(encoding="utf-8")
    routes_source = (APP_ROOT / "app/routes_personal.py").read_text(encoding="utf-8")
    assert "include_router(personal_search_scheduler_router" in main_source
    assert "await start_personal_search_scheduler()" in main_source
    assert "await stop_personal_search_scheduler()" in main_source
    assert "/api/v1/personal/search/scheduler/status" in {
        route.path for route in app_main.app.routes
    }
    marker = "async def search_personal_activity("
    search_signature = routes_source[
        routes_source.index(marker) : routes_source.index(marker) + 900
    ]
    assert "sync: bool = False" in search_signature
