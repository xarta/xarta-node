import asyncio
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from starlette.requests import Request
from starlette.testclient import TestClient

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-routes-sync-nodes.json"
NODES_JSON.write_text(
    """
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
          "active": true
        }
      ]
    }
    """,
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(NODES_JSON))
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-sync-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import db, routes_sync, timing  # noqa: E402
from app.models import GitPullRequest, SyncAction  # noqa: E402


def _sync_status_payload(gen: int = 1) -> routes_sync.SyncStatus:
    return routes_sync.SyncStatus(
        node_id="self",
        node_name="Self Node",
        gen=gen,
        integrity_ok=True,
        last_write_at="2026-07-04 14:00:00",
        last_write_by="test",
        queue_depths={"peer-a": 2},
        peer_count=1,
    )


def test_sync_status_reads_queue_depths_with_single_sqlite_connection(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE nodes (node_id TEXT PRIMARY KEY);
        CREATE TABLE sync_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_node_id TEXT NOT NULL,
            sent INTEGER DEFAULT 0
        );
        """
    )
    conn.executemany(
        "INSERT INTO sync_meta (key, value) VALUES (?, ?)",
        [
            ("gen", "42"),
            ("integrity_ok", "true"),
            ("last_write_at", "2026-07-04 14:00:00"),
            ("last_write_by", "unit"),
        ],
    )
    conn.executemany(
        "INSERT INTO nodes (node_id) VALUES (?)", [("self",), ("peer-a",), ("peer-b",)]
    )
    conn.executemany(
        "INSERT INTO sync_queue (target_node_id, sent) VALUES (?, ?)",
        [("peer-a", 0), ("peer-a", 0), ("peer-a", 1)],
    )
    conn.commit()
    opened = []

    @contextmanager
    def fake_get_read_conn(**kwargs):
        opened.append("open")
        assert kwargs == {"busy_timeout_ms": 100, "operation": "sync_status"}
        yield conn

    monkeypatch.setattr(routes_sync, "get_read_conn", fake_get_read_conn)
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "self")
    monkeypatch.setattr(routes_sync.cfg, "NODE_NAME", "Self Node")

    status = routes_sync._sync_status_sync()

    assert opened == ["open"]
    assert status.gen == 42
    assert status.queue_depths == {"peer-a": 2, "peer-b": 0}
    assert status.peer_count == 2
    assert conn.in_transaction is False


def test_read_only_sqlite_connection_records_open_setup_use_and_close_stages(
    tmp_path,
    monkeypatch,
):
    timing.reset_for_tests()
    db_path = tmp_path / "timing-read.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE sample (value TEXT NOT NULL)")
        conn.execute("INSERT INTO sample (value) VALUES ('ok')")
    monkeypatch.setattr(db.cfg, "DB_PATH", str(db_path))

    with db.get_read_conn(operation="unit_read") as conn:
        assert conn.execute("SELECT value FROM sample").fetchone()[0] == "ok"

    rows = timing.snapshot()
    assert [row["event"] for row in rows] == [
        "sqlite_connection.open",
        "sqlite_connection.setup",
        "sqlite_connection.use",
        "sqlite_connection.close",
        "sqlite_connection",
    ]
    assert all(row["operation"] == "unit_read" for row in rows)
    assert all(row["readonly"] is True for row in rows)


def test_sync_status_returns_retryable_503_when_sqlite_read_is_locked(monkeypatch):
    async def locked_status():
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(routes_sync, "_sync_status_coalesced", locked_status)

    try:
        asyncio.run(routes_sync.sync_status())
    except routes_sync.HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail == "database_locked"
    else:
        raise AssertionError("expected sync status to fail fast with HTTP 503")


def test_sync_status_coalesces_inflight_and_short_cache(monkeypatch):
    routes_sync._invalidate_sync_status_cache()
    monkeypatch.setattr(routes_sync, "_sync_status_inflight_task", None)
    calls = []

    async def fake_to_thread(label, func):
        calls.append((label, func))
        await asyncio.sleep(0.01)
        return _sync_status_payload(gen=len(calls))

    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)

    async def run_probe():
        first, second = await asyncio.gather(
            routes_sync._sync_status_coalesced(),
            routes_sync._sync_status_coalesced(),
        )
        third = await routes_sync._sync_status_coalesced()
        return first, second, third

    first, second, third = asyncio.run(run_probe())

    assert len(calls) == 1
    assert calls[0][0] == "sync.status"
    assert first.gen == second.gen == third.gen == 1
    assert first is not second
    assert second is not third


def test_git_pull_batch_skips_restart_when_heads_unchanged(monkeypatch):
    calls = []
    restarts = []

    async def fake_git_pull(repo_path, label):
        calls.append((repo_path, label))
        return False

    async def fake_restart():
        restarts.append("restart")

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {
            "outer": ("/repo/outer", True),
            "non_root": ("/repo/non-root", False),
            "inner": ("/repo/inner", True),
        },
    )

    asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["inner", "outer", "inner", "non_root"],
            source="test",
        )
    )

    assert calls == [
        ("/repo/outer", "outer"),
        ("/repo/non-root", "non_root"),
        ("/repo/inner", "inner"),
    ]
    assert restarts == []


def test_exact_git_pull_fetches_main_but_merges_only_reviewed_head(monkeypatch):
    commands = []
    expected = "b" * 40
    heads = iter(["a" * 40, expected])

    class Proc:
        returncode = 0

        async def communicate(self):
            return b"ok", b""

    async def create_subprocess(*args, **_kwargs):
        commands.append(args)
        return Proc()

    async def git_head(_repo_path, _label):
        return next(heads)

    monkeypatch.setattr(routes_sync.asyncio, "create_subprocess_exec", create_subprocess)
    monkeypatch.setattr(routes_sync, "_git_head", git_head)

    changed = asyncio.run(routes_sync._git_pull_exact("/repo/outer", "outer", expected))

    assert changed is True
    assert commands == [
        ("git", "-C", "/repo/outer", "fetch", "--no-tags", "origin", "main"),
        (
            "git",
            "-C",
            "/repo/outer",
            "merge-base",
            "--is-ancestor",
            expected,
            "origin/main",
        ),
        ("git", "-C", "/repo/outer", "merge", "--ff-only", expected),
    ]


def test_git_pull_batch_restarts_once_when_runtime_repo_changes(monkeypatch):
    calls = []
    restarts = []
    created_operations = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return label in {"outer", "non_root", "inner"}

    async def fake_restart(**kwargs):
        restarts.append(kwargs)
        return True

    async def fake_to_thread(_label, func, *args, **_kwargs):
        assert func is routes_sync._restart_operation_create_sync
        created_operations.append(args[0])
        return {"operation_id": args[0], "status": "queued"}

    async def runtime_heads():
        return {"outer": "a" * 40, "inner": "b" * 40}

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_runtime_heads", runtime_heads)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {
            "outer": ("/repo/outer", True),
            "non_root": ("/repo/non-root", False),
            "inner": ("/repo/inner", True),
        },
    )
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer", "non_root", "inner"]))

    assert calls == ["outer", "non_root", "inner"]
    assert len(created_operations) == 1
    assert restarts[0]["operation_id"] == created_operations[0]
    assert restarts[0]["expected_runtime_heads"] == {
        "outer": "a" * 40,
        "inner": "b" * 40,
    }


def test_git_pull_batch_restarts_when_runtime_process_is_stale(monkeypatch):
    calls = []
    restarts = []
    created_operations = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return False

    async def fake_runtime_repo_is_stale(repo_path, label):
        return label == "outer"

    async def fake_restart(**kwargs):
        restarts.append(kwargs)
        return True

    async def fake_to_thread(_label, func, *args, **_kwargs):
        assert func is routes_sync._restart_operation_create_sync
        created_operations.append(args[0])
        return {"operation_id": args[0], "status": "queued"}

    async def runtime_heads():
        return {"outer": "a" * 40}

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_runtime_repo_is_stale", fake_runtime_repo_is_stale)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_runtime_heads", runtime_heads)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {
            "outer": ("/repo/outer", True),
        },
    )
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer"]))

    assert calls == ["outer"]
    assert len(created_operations) == 1
    assert restarts[0]["operation_id"] == created_operations[0]


def test_manual_restart_snapshots_current_runtime_heads_before_dispatch(monkeypatch):
    created_operations = []
    restarts = []

    async def runtime_heads():
        raise AssertionError("route must not capture heads before the guarded restart")

    async def fake_to_thread(label, func, *args, **_kwargs):
        assert label == "sync.restart_operation_create"
        assert func is routes_sync._restart_operation_create_sync
        created_operations.append(args[0])
        return {"operation_id": args[0], "status": "queued"}

    async def guarded_restart(**kwargs):
        restarts.append(kwargs)

    async def exercise():
        response = await routes_sync.trigger_restart()
        await asyncio.sleep(0)
        return response

    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "restart-blueprints")
    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"outer": "a" * 40, "inner": "b" * 40},
    )
    monkeypatch.setattr(routes_sync, "_runtime_heads", runtime_heads)
    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_run_guarded_restart", guarded_restart)

    response = asyncio.run(exercise())

    assert response.status_code == 204
    assert len(created_operations) == 1
    assert restarts == [
        {
            "operation_id": created_operations[0],
            "capture_runtime_heads": True,
        }
    ]


def test_direct_restart_captures_runtime_heads_after_quiescence(monkeypatch):
    order = []
    receipt_updates = []

    async def no_sleep(_seconds):
        order.append("sleep")

    async def pause():
        order.append("pause")
        return []

    async def snapshot(_providers=None):
        order.append("snapshot")
        return {"queued_runs": 0, "running_runs": 0, "stale_running_runs": 0}

    async def runtime_heads():
        order.append("runtime_heads")
        return {"outer": "c" * 40, "inner": "d" * 40}

    async def update(operation_id, status, result, error_code=""):
        order.append("receipt")
        receipt_updates.append((operation_id, status, result, error_code))
        return {}

    class Proc:
        returncode = 0

        async def communicate(self):
            return b"dispatched", b""

    async def create_subprocess(*_args, **_kwargs):
        order.append("subprocess")
        return Proc()

    monkeypatch.setattr(routes_sync.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(routes_sync, "_pause_blueprints_provider_claims", pause)
    monkeypatch.setattr(routes_sync, "_scheduler_quiescence_snapshot", snapshot)
    monkeypatch.setattr(routes_sync, "_runtime_heads", runtime_heads)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)
    monkeypatch.setattr(routes_sync.asyncio, "create_subprocess_exec", create_subprocess)
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "restart-blueprints")
    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"outer": "a" * 40, "inner": "b" * 40},
    )

    restarted = asyncio.run(
        routes_sync._restart_service(
            operation_id="git-pull-" + "a" * 32,
            capture_runtime_heads=True,
        )
    )

    assert restarted is True
    assert order == ["sleep", "pause", "snapshot", "runtime_heads", "receipt", "subprocess"]
    assert receipt_updates[0][1] == "restart_requested"
    assert receipt_updates[0][2]["expected_runtime_heads"] == {
        "outer": "c" * 40,
        "inner": "d" * 40,
    }


def test_receive_actions_offloads_db_apply(monkeypatch):
    to_thread_calls = []
    applied = []

    async def fake_to_thread(func, /, *args, **kwargs):
        to_thread_calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    def fake_receive_db_actions_sync(payload, db_actions):
        applied.append((payload.source_node_id, len(db_actions)))
        return len(db_actions)

    monkeypatch.setattr(routes_sync, "_receive_actions_apply_lock", None)
    monkeypatch.setattr(routes_sync.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_receive_db_actions_sync", fake_receive_db_actions_sync)

    payload = routes_sync.SyncActionsPayload(
        source_node_id="peer-1",
        source_commit_ts=routes_sync.cfg.COMMIT_TS,
        actions=[
            SyncAction(
                action_type="UPDATE",
                table_name="settings",
                row_id="sync-proof",
                row_data={"key": "sync-proof", "value": "ok"},
                gen=1,
                source_node_id="peer-1",
                guid="guid-sync-proof",
            )
        ],
    )

    response = asyncio.run(routes_sync.receive_actions(payload))

    assert response.status_code == 204
    assert to_thread_calls == ["fake_receive_db_actions_sync"]
    assert applied == [("peer-1", 1)]


def test_runtime_stale_check_uses_scope_specific_running_head(monkeypatch):
    async def fake_git_head(repo_path, label):
        assert repo_path == "/repo/inner"
        assert label == "inner"
        return "inner-private-head"

    monkeypatch.setattr(routes_sync.os.path, "isdir", lambda path: path == "/repo/inner/.git")
    monkeypatch.setattr(routes_sync, "_git_head", fake_git_head)
    monkeypatch.setattr(routes_sync.cfg, "COMMIT_HASH", "outer-public-head")
    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"inner": "inner-private-head", "outer": "outer-public-head"},
    )

    assert asyncio.run(routes_sync._runtime_repo_is_stale("/repo/inner", "inner")) is False


def test_runtime_stale_check_detects_scope_specific_head_change(monkeypatch):
    async def fake_git_head(repo_path, label):
        assert repo_path == "/repo/inner"
        assert label == "inner"
        return "inner-new-head"

    monkeypatch.setattr(routes_sync.os.path, "isdir", lambda path: path == "/repo/inner/.git")
    monkeypatch.setattr(routes_sync, "_git_head", fake_git_head)
    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"inner": "inner-old-head"},
    )

    assert asyncio.run(routes_sync._runtime_repo_is_stale("/repo/inner", "inner")) is True


def test_git_pull_batch_skips_when_restart_is_pending(monkeypatch):
    calls = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return False

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", True)
    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {"outer": ("/repo/outer", True)},
    )

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer"]))

    assert calls == []


def test_git_pull_batch_does_not_restart_for_non_root_only(monkeypatch):
    restarts = []

    async def fake_git_pull(repo_path, label):
        return True

    async def fake_restart():
        restarts.append("restart")

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {"non_root": ("/repo/non-root", False)},
    )
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["non_root"]))

    assert restarts == []


def test_systemctl_restart_command_uses_transient_unit(monkeypatch):
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")
    monkeypatch.setattr(routes_sync.os, "getpid", lambda: 1234)
    monkeypatch.setattr(routes_sync.time, "time", lambda: 4567.89)

    assert routes_sync._restart_command_parts() == [
        "systemd-run",
        "--unit",
        "blueprints-app-self-restart-1234-4567890",
        "--collect",
        "/bin/systemctl",
        "restart",
        "blueprints-app",
    ]


def _request_from(host: str = "127.0.0.1") -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/sync/git-pull",
            "headers": [],
            "client": (host, 12345),
            "server": ("127.0.0.1", 8080),
            "scheme": "http",
            "query_string": b"",
        }
    )


def _git_pull_receipt_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sync_git_pull_operations (
            operation_id TEXT PRIMARY KEY,
            request_json TEXT NOT NULL,
            status TEXT NOT NULL,
            result_json TEXT NOT NULL DEFAULT '{}',
            error_code TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        );
        """
    )
    return conn


def test_git_pull_receipt_replay_is_idempotent_and_request_mismatch_conflicts(monkeypatch):
    conn = _git_pull_receipt_conn()

    @contextmanager
    def fake_conn():
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    monkeypatch.setattr(routes_sync, "get_conn", fake_conn)
    operation_id = "git-pull-" + "a" * 32
    expected = "b" * 40

    created = routes_sync._git_pull_operation_create_sync(operation_id, ["outer"], expected)
    replay = routes_sync._git_pull_operation_create_sync(operation_id, ["outer"], expected)

    assert created["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    with pytest.raises(routes_sync.GitPullOperationConflict):
        routes_sync._git_pull_operation_create_sync(operation_id, ["outer"], "c" * 40)


def test_git_pull_receipt_pruning_never_deletes_nonterminal_rows(monkeypatch):
    conn = _git_pull_receipt_conn()
    for index in range(routes_sync._GIT_PULL_RECEIPT_LIMIT + 3):
        conn.execute(
            """INSERT INTO sync_git_pull_operations(
                   operation_id,request_json,status,result_json,error_code,updated_at
               ) VALUES(?,?,?,?,?,?)""",
            (
                f"git-pull-{index:032x}",
                routes_sync._git_pull_request_json(["outer"], f"{index:040x}"),
                "pulling",
                "{}",
                "",
                f"2026-07-18T13:{index // 60:02d}:{index % 60:02d}.000Z",
            ),
        )
    conn.commit()

    @contextmanager
    def fake_conn():
        yield conn
        conn.commit()

    monkeypatch.setattr(routes_sync, "get_conn", fake_conn)
    routes_sync._git_pull_operation_create_sync(
        "git-pull-" + "f" * 32,
        ["outer"],
        "e" * 40,
    )

    assert (
        conn.execute(
            "SELECT count(*) FROM sync_git_pull_operations WHERE status='pulling'"
        ).fetchone()[0]
        == routes_sync._GIT_PULL_RECEIPT_LIMIT + 3
    )


def test_local_only_git_pull_replay_schedules_once_and_never_broadcasts(monkeypatch):
    creates = 0
    jobs = []

    async def fake_to_thread(_label, func, *args, **kwargs):
        return func(*args, **kwargs)

    def fake_create(operation_id, scopes, expected_head):
        nonlocal creates
        creates += 1
        return {
            "operation_id": operation_id,
            "request": {"scopes": scopes, "expected_head": expected_head},
            "status": "queued",
            "idempotent_replay": creates > 1,
        }

    async def fake_job(operation_id, scopes, expected_head):
        jobs.append((operation_id, scopes, expected_head))

    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_git_pull_operation_create_sync", fake_create)
    monkeypatch.setattr(routes_sync, "_run_tracked_git_pull_operation", fake_job)
    monkeypatch.setattr(routes_sync, "_ACTIVE_GIT_PULL_OPERATION_IDS", set())
    monkeypatch.setattr(
        routes_sync,
        "enqueue_for_all_peers",
        lambda *_args, **_kwargs: pytest.fail("local-only mode must not broadcast"),
    )
    payload = GitPullRequest(
        scope="outer",
        local_only=True,
        operation_id="git-pull-" + "a" * 32,
        expected_head="b" * 40,
    )

    async def run():
        first = await routes_sync.trigger_git_pull(payload, _request_from())
        await asyncio.sleep(0)
        second = await routes_sync.trigger_git_pull(payload, _request_from())
        await asyncio.sleep(0)
        return first, second

    first, second = asyncio.run(run())

    assert first.status_code == second.status_code == 202
    assert len(jobs) == 1


def test_local_only_git_pull_rejects_non_loopback_before_receipt_write(monkeypatch):
    calls = []

    async def fake_to_thread(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    payload = GitPullRequest(
        scope="outer",
        local_only=True,
        operation_id="git-pull-" + "a" * 32,
        expected_head="b" * 40,
    )

    with pytest.raises(routes_sync.HTTPException) as exc:
        asyncio.run(routes_sync.trigger_git_pull(payload, _request_from("192.0.2.10")))

    assert exc.value.status_code == 403
    assert calls == []


def test_legacy_git_pull_still_broadcasts_and_returns_204(monkeypatch):
    enqueued = []
    jobs = []

    async def fake_to_thread(_label, func, *args, **kwargs):
        return func(*args, **kwargs)

    def fake_enqueue(scopes):
        enqueued.append(scopes)

    async def fake_job(scopes, **kwargs):
        jobs.append((scopes, kwargs))

    monkeypatch.setattr(routes_sync.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(routes_sync, "_enqueue_git_pull_scopes_sync", fake_enqueue)
    monkeypatch.setattr(routes_sync, "_git_pull_scopes_and_maybe_restart", fake_job)

    async def run():
        response = await routes_sync.trigger_git_pull(
            GitPullRequest(scope="outer"), _request_from("192.0.2.10")
        )
        await asyncio.sleep(0)
        return response

    response = asyncio.run(run())

    assert response.status_code == 204
    assert enqueued == [["outer"]]
    assert jobs == [(["outer"], {"source": "local trigger"})]


def _restart_snapshot_payload(*, kanban_queued=0, pim_queued=4):
    kanban = "xarta-schedule-" + "a" * 24
    search = "xarta-schedule-" + "b" * 24
    pim = "xarta-schedule-" + "c" * 24
    return {
        "schema": "xarta.scheduler.restart_active_work_snapshot.v2",
        "captured_at": "2026-07-18T13:30:00+00:00",
        "snapshot_id": "1122542:1122542:",
        "generation": 1122542,
        "schedule_count": 3,
        "global_work": {
            "queued_runs": kanban_queued + pim_queued,
            "running_runs": 0,
            "stale_running_runs": 0,
        },
        "health": {
            "ok": True,
            "database": "available",
            "coordinator_available": True,
            "executor_available": True,
            "worker_stale_seconds": 10,
        },
        "schedules": [
            {
                "schedule_id": kanban,
                "execution_mode": "provider",
                "provider_id": routes_sync.KANBAN_PROVIDER_ID,
                "queued_runs": kanban_queued,
                "running_runs": 0,
                "stale_running_runs": 0,
                "owner_mismatch_runs": 0,
            },
            {
                "schedule_id": search,
                "execution_mode": "provider",
                "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
                "queued_runs": 0,
                "running_runs": 0,
                "stale_running_runs": 0,
                "owner_mismatch_runs": 0,
            },
            {
                "schedule_id": pim,
                "execution_mode": "provider",
                "provider_id": "pim-email",
                "queued_runs": pim_queued,
                "running_runs": 0,
                "stale_running_runs": 0,
                "owner_mismatch_runs": 0,
            },
        ],
    }


def test_scheduler_restart_snapshot_allows_only_non_blueprints_queued_work(monkeypatch):
    calls = []

    async def get_json(path):
        calls.append(path)
        return _restart_snapshot_payload()

    monkeypatch.setattr(routes_sync, "scheduler_local_get_json", get_json)

    snapshot = asyncio.run(routes_sync._scheduler_quiescence_snapshot())

    assert calls == ["/restart-snapshot"]
    assert snapshot["global_work"]["queued_runs"] == 4
    assert snapshot["generation"] == 1122542
    assert snapshot["non_blueprints_active_work"] == [
        {
            "schedule_id": "xarta-schedule-" + "c" * 24,
            "execution_mode": "provider",
            "provider_id": "pim-email",
            "queued_runs": 4,
            "running_runs": 0,
            "stale_running_runs": 0,
            "owner_mismatch_runs": 0,
        }
    ]


def test_scheduler_restart_snapshot_refuses_queued_blueprints_work(monkeypatch):
    async def get_json(_path):
        return _restart_snapshot_payload(kanban_queued=1, pim_queued=0)

    monkeypatch.setattr(routes_sync, "scheduler_local_get_json", get_json)

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(routes_sync._scheduler_quiescence_snapshot())

    assert exc.value.code == "blueprints_scheduler_not_quiescent"


def test_scheduler_restart_snapshot_allows_queued_owner_disabled_provider(monkeypatch):
    async def get_json(_path):
        return _restart_snapshot_payload(kanban_queued=1, pim_queued=0)

    monkeypatch.setattr(routes_sync, "scheduler_local_get_json", get_json)
    providers = [
        {
            "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
            "provider_effective_enabled": True,
        },
        {
            "provider_id": routes_sync.KANBAN_PROVIDER_ID,
            "provider_effective_enabled": False,
        },
    ]

    snapshot = asyncio.run(routes_sync._scheduler_quiescence_snapshot(providers))

    assert snapshot["global_work"]["queued_runs"] == 1
    assert snapshot["non_blueprints_active_work"] == []


def test_scheduler_restart_snapshot_refuses_running_owner_disabled_provider(monkeypatch):
    payload = _restart_snapshot_payload(kanban_queued=0, pim_queued=0)
    payload["schedules"][0]["running_runs"] = 1
    payload["global_work"]["running_runs"] = 1

    async def get_json(_path):
        return payload

    monkeypatch.setattr(routes_sync, "scheduler_local_get_json", get_json)
    providers = [
        {
            "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
            "provider_effective_enabled": True,
        },
        {
            "provider_id": routes_sync.KANBAN_PROVIDER_ID,
            "provider_effective_enabled": False,
        },
    ]

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(routes_sync._scheduler_quiescence_snapshot(providers))

    assert exc.value.code == "blueprints_scheduler_not_quiescent"


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (
            lambda p: p["global_work"].__setitem__("queued_runs", False),
            "scheduler_status_malformed",
        ),
        (lambda p: p["global_work"].__setitem__("queued_runs", 5), "scheduler_snapshot_malformed"),
        (lambda p: p["schedules"].pop(), "scheduler_snapshot_malformed"),
        (
            lambda p: p["schedules"][1].__setitem__("provider_id", "different-provider"),
            "scheduler_snapshot_malformed",
        ),
        (
            lambda p: p["schedules"][2].__setitem__("owner_mismatch_runs", 1),
            "scheduler_active_owner_mismatch",
        ),
    ],
)
def test_scheduler_restart_snapshot_rejects_malformed_or_incomplete_inventory(mutate, code):
    payload = _restart_snapshot_payload()
    mutate(payload)

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        routes_sync._strict_scheduler_restart_snapshot(payload)

    assert exc.value.code == code


def test_provider_claim_gate_requires_both_typed_provider_results(monkeypatch):
    resumes = []

    async def malformed_personal():
        return {
            "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
            "provider_effective_enabled": True,
            "claim_loop_paused": True,
            "active_run_ids": "not-a-list",
        }

    async def valid_kanban():
        return {
            "provider_id": routes_sync.KANBAN_PROVIDER_ID,
            "provider_effective_enabled": False,
            "claim_loop_paused": True,
            "active_run_ids": [],
            "legacy_loop_effective_enabled": False,
        }

    async def resume():
        resumes.append(True)

    monkeypatch.setattr(routes_sync, "pause_personal_search_claims_for_restart", malformed_personal)
    monkeypatch.setattr(routes_sync, "pause_kanban_claims_for_restart", valid_kanban)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(routes_sync._pause_blueprints_provider_claims())

    assert exc.value.code == "provider_claim_gate_malformed"
    assert resumes == [True]


def test_provider_claim_gate_requires_typed_effective_policy_and_resumes(monkeypatch):
    resumes = []

    async def personal_missing_policy():
        return {
            "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
            "claim_loop_paused": True,
            "active_run_ids": [],
        }

    async def valid_kanban():
        return {
            "provider_id": routes_sync.KANBAN_PROVIDER_ID,
            "provider_effective_enabled": False,
            "claim_loop_paused": True,
            "active_run_ids": [],
            "legacy_loop_effective_enabled": False,
        }

    async def resume():
        resumes.append(True)

    monkeypatch.setattr(
        routes_sync, "pause_personal_search_claims_for_restart", personal_missing_policy
    )
    monkeypatch.setattr(routes_sync, "pause_kanban_claims_for_restart", valid_kanban)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(routes_sync._pause_blueprints_provider_claims())

    assert exc.value.code == "provider_claim_gate_malformed"
    assert resumes == [True]


def test_local_pull_refuses_before_git_when_provider_guard_is_not_quiescent(monkeypatch):
    pulled = []
    updates = []

    async def refused():
        raise routes_sync.SchedulerRestartRefused(
            "blueprints_provider_not_quiescent", {"active_run_ids": {"provider": ["run-1"]}}
        )

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_pause_and_snapshot_scheduler", refused)
    monkeypatch.setattr(
        routes_sync,
        "_git_pull",
        lambda *_args: pulled.append(True),
    )
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)

    result = asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["outer"],
            operation_id="git-pull-" + "a" * 32,
            expected_head="b" * 40,
        )
    )

    assert result["code"] == "blueprints_provider_not_quiescent"
    assert pulled == []
    assert updates[-1][1:] == (
        "blocked",
        {
            "reason": "blueprints_provider_not_quiescent",
            "detail": {"active_run_ids": {"provider": ["run-1"]}},
        },
        "blueprints_provider_not_quiescent",
    )


def test_local_unchanged_pull_completes_receipt_and_resumes_claims(monkeypatch):
    updates = []
    resumes = []
    expected = "b" * 40
    heads = {"outer": expected, "inner": "c" * 40}

    async def pause():
        return {"providers": [], "scheduler": {"queued_runs": 0, "running_runs": 0}}

    async def pull(_repo_path, _label, exact_head):
        assert exact_head == expected
        return False

    async def stale(_repo_path, _label):
        return False

    async def runtime_heads():
        return heads

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))

    async def resume():
        resumes.append(True)

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_pause_and_snapshot_scheduler", pause)
    monkeypatch.setattr(routes_sync, "_git_pull_exact", pull)
    monkeypatch.setattr(routes_sync, "_runtime_repo_is_stale", stale)
    monkeypatch.setattr(routes_sync, "_runtime_heads", runtime_heads)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {"outer": ("/repo/outer", True)},
    )

    result = asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["outer"],
            operation_id="git-pull-" + "a" * 32,
            expected_head=expected,
        )
    )

    assert result["status"] == "completed"
    assert updates[-1][1] == "completed"
    assert updates[-1][2]["runtime_heads"] == heads
    assert resumes == [True]


def test_direct_restart_waits_for_git_pull_lock(monkeypatch):
    restarts = []
    held_lock = asyncio.Lock()

    async def restart(**_kwargs):
        restarts.append("restart")
        return True

    async def run():
        await held_lock.acquire()
        task = asyncio.create_task(routes_sync._run_guarded_restart())
        await asyncio.sleep(0)
        assert restarts == []
        held_lock.release()
        await task

    monkeypatch.setattr(routes_sync, "_GIT_PULL_LOCK", held_lock)
    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_restart_service", restart)

    asyncio.run(run())

    assert restarts == ["restart"]
    assert routes_sync._RESTART_PENDING is True


def test_git_pull_capability_is_typed_non_broadcast_and_node_bound(monkeypatch):
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "test-node")

    assert asyncio.run(routes_sync.git_pull_capabilities()) == {
        "schema": "xarta.blueprints.git_pull_capabilities.v1",
        "node_id": "test-node",
        "local_only": True,
        "broadcast": False,
        "operation_receipt_schema": "xarta.blueprints.git_pull_operation.v1",
        "supported_scopes": ["outer"],
        "restart_guard": "atomic-provider-scoped-snapshot-v5",
        "exact_expected_head": True,
    }


def test_git_pull_capability_real_route_is_not_consumed_by_receipt_route(monkeypatch):
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "test-node")
    app = FastAPI()
    app.include_router(routes_sync.router, prefix="/api/v1")

    response = TestClient(app).get("/api/v1/sync/git-pull-capabilities")

    assert response.status_code == 200
    assert response.json()["schema"] == "xarta.blueprints.git_pull_capabilities.v1"
    assert response.json()["node_id"] == "test-node"


def test_second_scheduler_snapshot_blocks_restart_and_resumes_claims(monkeypatch):
    resumes = []
    commands = []

    async def no_sleep(_seconds):
        return None

    async def second_snapshot(_providers=None):
        raise routes_sync.SchedulerRestartRefused(
            "scheduler_not_quiescent", {"queued_runs": 1, "running_runs": 0}
        )

    async def resume():
        resumes.append(True)

    async def create_subprocess(*args, **_kwargs):
        commands.append(args)
        raise AssertionError("restart command must not run")

    monkeypatch.setattr(routes_sync.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(routes_sync, "_scheduler_quiescence_snapshot", second_snapshot)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)
    monkeypatch.setattr(routes_sync.asyncio, "create_subprocess_exec", create_subprocess)
    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(
            routes_sync._restart_service(
                operation_id="git-pull-" + "a" * 32,
                expected_runtime_heads={"outer": "b" * 40},
                guard_before={"providers": [], "scheduler": {}},
                claims_already_paused=True,
            )
        )

    assert exc.value.code == "scheduler_not_quiescent"
    assert resumes == [True]
    assert commands == []


def test_direct_restart_subprocess_error_resumes_claim_gates(monkeypatch):
    resumes = []

    async def no_sleep(_seconds):
        return None

    async def pause():
        return [
            {
                "provider_id": routes_sync.PERSONAL_SEARCH_PROVIDER_ID,
                "provider_effective_enabled": True,
                "claim_loop_paused": True,
                "active_run_ids": [],
            },
            {
                "provider_id": routes_sync.KANBAN_PROVIDER_ID,
                "provider_effective_enabled": True,
                "claim_loop_paused": True,
                "active_run_ids": [],
                "legacy_loop_effective_enabled": False,
            },
        ]

    async def snapshot(_providers=None):
        return {"queued_runs": 0, "running_runs": 0, "stale_running_runs": 0}

    async def resume():
        resumes.append(True)

    async def fail_subprocess(*_args, **_kwargs):
        raise OSError("systemd unavailable")

    monkeypatch.setattr(routes_sync.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(routes_sync, "_pause_blueprints_provider_claims", pause)
    monkeypatch.setattr(routes_sync, "_scheduler_quiescence_snapshot", snapshot)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)
    monkeypatch.setattr(routes_sync.asyncio, "create_subprocess_exec", fail_subprocess)
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    with pytest.raises(OSError, match="systemd unavailable"):
        asyncio.run(routes_sync._restart_service())

    assert resumes == [True]


def test_restart_receipt_completes_only_in_new_process_with_exact_runtime_heads(monkeypatch):
    receipt = {
        "operation_id": "git-pull-" + "a" * 32,
        "status": "restart_requested",
        "created_at": "2026-07-18T13:00:00+00:00",
        "result": {
            "initiating_node_id": "test-node",
            "process_started_at": "2026-07-18T12:59:00+00:00",
            "expected_runtime_heads": {"outer": "a" * 40, "inner": "b" * 40},
        },
    }
    updates = []

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))
        return {**receipt, "status": status, "result": result}

    async def after_snapshot(_providers=None):
        return {
            "schema": "xarta.scheduler.restart_active_work_snapshot.v2",
            "captured_at": "2026-07-18T13:01:00+00:00",
            "generation": 1122542,
        }

    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(routes_sync, "_PROCESS_STARTED_AT", "2026-07-18T13:01:00+00:00")
    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"outer": "a" * 40, "inner": "wrong"},
    )
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)
    monkeypatch.setattr(routes_sync, "_scheduler_quiescence_snapshot", after_snapshot)
    unchanged = asyncio.run(routes_sync._reconcile_git_pull_operation(receipt))
    assert unchanged is receipt
    assert updates == []

    monkeypatch.setattr(
        routes_sync,
        "_RUNNING_RUNTIME_REPO_HEADS",
        {"outer": "a" * 40, "inner": "b" * 40},
    )
    completed = asyncio.run(routes_sync._reconcile_git_pull_operation(receipt))
    assert completed["status"] == "completed"
    assert completed["result"]["restart_observed"] is True
    assert (
        completed["result"]["guard_immediately_after_restart"]["scheduler"]["generation"] == 1122542
    )


def test_startup_reconciliation_persists_failed_after_snapshot_proof(monkeypatch):
    receipt = {
        "operation_id": "git-pull-" + "a" * 32,
        "status": "restart_requested",
        "result": {"expected_runtime_heads": {"outer": "a" * 40}},
    }
    updates = []

    async def to_thread(_label, _func, *_args):
        return [receipt]

    async def refuse(_receipt, **_kwargs):
        raise routes_sync.SchedulerRestartRefused("scheduler_unhealthy", {"field": "health.ok"})

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))
        return {**receipt, "status": status, "result": result, "error_code": error_code}

    monkeypatch.setattr(routes_sync.timing, "to_thread", to_thread)
    monkeypatch.setattr(routes_sync, "_reconcile_git_pull_operation", refuse)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)

    with pytest.raises(RuntimeError, match="post_restart_scheduler_proof_failed"):
        asyncio.run(routes_sync.reconcile_pending_restart_operations())

    assert updates[0][1] == "post_restart_proof_failed"
    assert updates[0][3] == "post_restart_scheduler_proof_failed"
    assert updates[0][2]["post_restart_proof_error"]["code"] == "scheduler_unhealthy"


@pytest.mark.parametrize(
    ("restart_pending", "restart_result", "restart_error", "expected_status", "expected_code"),
    [
        (True, True, None, "blocked", "restart_already_pending"),
        (False, False, None, "failed", "restart_dispatch_failed"),
        (False, None, OSError("systemd failed"), "failed", "restart_exception"),
    ],
)
def test_direct_restart_terminalizes_every_pre_dispatch_or_dispatch_failure(
    monkeypatch,
    restart_pending,
    restart_result,
    restart_error,
    expected_status,
    expected_code,
):
    updates = []

    async def restart(**_kwargs):
        if restart_error is not None:
            raise restart_error
        return restart_result

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))
        return {}

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", restart_pending)
    monkeypatch.setattr(routes_sync, "_restart_service", restart)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)

    asyncio.run(
        routes_sync._run_guarded_restart(
            operation_id="git-pull-" + "a" * 32,
            expected_runtime_heads={"outer": "b" * 40},
        )
    )

    assert updates[-1][1] == expected_status
    assert updates[-1][3] == expected_code


def test_startup_reconciliation_terminalizes_wrong_node_receipt(monkeypatch):
    receipt = {
        "operation_id": "git-pull-" + "a" * 32,
        "status": "restart_requested",
        "result": {
            "initiating_node_id": "different-node",
            "process_started_at": "2026-07-18T12:59:00+00:00",
            "expected_runtime_heads": {"outer": "a" * 40},
        },
    }
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "test-node")

    with pytest.raises(routes_sync.SchedulerRestartRefused) as exc:
        asyncio.run(
            routes_sync._reconcile_git_pull_operation(receipt, require_new_process_proof=True)
        )

    assert exc.value.code == "restart_receipt_wrong_node"


def test_startup_reconciliation_quarantines_permanent_receipt_failure(monkeypatch):
    receipt = {
        "operation_id": "git-pull-" + "a" * 32,
        "status": "restart_requested",
        "result": {"expected_runtime_heads": {"outer": "a" * 40}},
    }
    updates = []

    async def to_thread(_label, _func, *_args):
        return [receipt]

    async def refuse(_receipt, **_kwargs):
        raise routes_sync.SchedulerRestartRefused(
            "restart_receipt_wrong_node", {"initiating_node_id": "wrong"}
        )

    async def update(operation_id, status, result, error_code=""):
        updates.append((operation_id, status, result, error_code))
        return {**receipt, "status": status, "result": result, "error_code": error_code}

    monkeypatch.setattr(routes_sync.timing, "to_thread", to_thread)
    monkeypatch.setattr(routes_sync, "_reconcile_git_pull_operation", refuse)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)

    with pytest.raises(RuntimeError, match="post_restart_scheduler_proof_failed"):
        asyncio.run(routes_sync.reconcile_pending_restart_operations())

    assert updates[0][1] == "failed"
    assert updates[0][3] == "restart_receipt_wrong_node"


def test_direct_restart_receipt_creation_applies_bounded_retention(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE sync_git_pull_operations (
               operation_id TEXT PRIMARY KEY,
               request_json TEXT NOT NULL,
               status TEXT NOT NULL,
               result_json TEXT NOT NULL DEFAULT '{}',
               error_code TEXT NOT NULL DEFAULT '',
               created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
               updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
           )"""
    )
    for index in range(130):
        conn.execute(
            """INSERT INTO sync_git_pull_operations
               VALUES(?,?,?,'{}','',?,?)""",
            (
                f"git-pull-{index:032x}",
                "{}",
                "completed",
                f"2026-07-18T13:{index // 60:02d}:{index % 60:02d}Z",
                f"2026-07-18T13:{index // 60:02d}:{index % 60:02d}Z",
            ),
        )

    @contextmanager
    def get_conn():
        with conn:
            yield conn

    monkeypatch.setattr(routes_sync, "get_conn", get_conn)
    created = routes_sync._restart_operation_create_sync("git-pull-" + "f" * 32)

    assert created["status"] == "queued"
    assert conn.execute("SELECT count(*) FROM sync_git_pull_operations").fetchone()[0] == 128


def test_lifespan_captures_after_restart_proof_before_work_or_provider_startup():
    source = (APP_ROOT / "app" / "main.py").read_text(encoding="utf-8")
    lifespan = source[
        source.index("async def lifespan") : source.index("def _load_nodes_from_json")
    ]

    assert lifespan.index("db.init_db()") < lifespan.index(
        "await start_scheduler_coordination_bridge()"
    )
    assert lifespan.index("await reconcile_pending_restart_operations()") < lifespan.index(
        "_load_nodes_from_json()"
    )
    assert lifespan.index("await reconcile_pending_restart_operations()") < lifespan.index(
        "await start_drain_loop()"
    )
    assert lifespan.index("await reconcile_pending_restart_operations()") < lifespan.index(
        "await start_personal_search_scheduler()"
    )
    assert lifespan.index("await reconcile_pending_restart_operations()") < lifespan.index(
        "await start_kanban_automation_scheduler()"
    )


def test_restart_receipt_cannot_complete_in_initiating_process(monkeypatch):
    receipt = {
        "operation_id": "git-pull-" + "a" * 32,
        "status": "restart_requested",
        "created_at": "2026-07-18T12:00:00+00:00",
        "result": {
            "initiating_node_id": "test-node",
            "initiating_pid": 123,
            "process_started_at": "2026-07-18T13:01:00+00:00",
            "expected_runtime_heads": {"outer": "a" * 40},
        },
    }
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(routes_sync, "_PROCESS_STARTED_AT", "2026-07-18T13:01:00+00:00")
    monkeypatch.setattr(routes_sync, "_RUNNING_RUNTIME_REPO_HEADS", {"outer": "a" * 40})

    unchanged = asyncio.run(routes_sync._reconcile_git_pull_operation(receipt))

    assert unchanged is receipt


def _kanban_sync_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE kanban_items (
            item_id TEXT PRIMARY KEY,
            state_id TEXT,
            status TEXT,
            automation_excluded INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_items (
            item_id, state_id, status, automation_excluded, updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        ("work-git-github-activity", "done", "done", 1, "2026-06-27T20:10:00Z"),
    )
    return conn


def _personal_filter_sync_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE personal_events (
            event_id TEXT PRIMARY KEY,
            tags_json TEXT NOT NULL DEFAULT '[]'
        );
        CREATE TABLE personal_filter_meta_tags (
            meta_tag_id TEXT PRIMARY KEY,
            label TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE personal_filter_tags (
            tag_id TEXT PRIMARY KEY,
            label TEXT NOT NULL DEFAULT '',
            meta_tag_id TEXT NOT NULL DEFAULT ''
        );
        """
    )
    return conn


def test_assigned_personal_filter_tag_sync_delete_is_skipped():
    conn = _personal_filter_sync_conn()
    conn.execute(
        "INSERT INTO personal_filter_tags (tag_id, label) VALUES ('birthdays-friends', 'Birthdays Friends')"
    )
    conn.execute(
        "INSERT INTO personal_events (event_id, tags_json) VALUES ('birthday-event', '[\"birthdays-friends\"]')"
    )

    action = SimpleNamespace(
        action_type="DELETE",
        table_name="personal_filter_tags",
        row_id="birthdays-friends",
        row_data=None,
    )
    routes_sync._apply_action(conn, action)

    row = conn.execute(
        "SELECT tag_id FROM personal_filter_tags WHERE tag_id='birthdays-friends'"
    ).fetchone()
    assert row is not None


def test_assigned_personal_filter_meta_tag_sync_delete_is_skipped():
    conn = _personal_filter_sync_conn()
    conn.execute(
        "INSERT INTO personal_filter_meta_tags (meta_tag_id, label) VALUES ('important', 'Important')"
    )
    conn.execute(
        """
        INSERT INTO personal_filter_tags (tag_id, label, meta_tag_id)
        VALUES ('birthdays-friends', 'Birthdays Friends', 'important')
        """
    )

    action = SimpleNamespace(
        action_type="DELETE",
        table_name="personal_filter_meta_tags",
        row_id="important",
        row_data=None,
    )
    routes_sync._apply_action(conn, action)

    row = conn.execute(
        "SELECT meta_tag_id FROM personal_filter_meta_tags WHERE meta_tag_id='important'"
    ).fetchone()
    assert row is not None


def test_stale_kanban_item_sync_update_is_skipped():
    conn = _kanban_sync_conn()
    action = SimpleNamespace(
        action_type="UPDATE",
        table_name="kanban_items",
        row_id="work-git-github-activity",
        row_data={
            "item_id": "work-git-github-activity",
            "state_id": "doing",
            "status": "active",
            "automation_excluded": 0,
            "updated_at": "2026-06-25 18:19:57",
        },
    )

    assert routes_sync._should_skip_stale_kanban_item_upsert(conn, action) is True

    if not routes_sync._should_skip_stale_kanban_item_upsert(conn, action):
        routes_sync._apply_action(conn, action)

    row = conn.execute(
        "SELECT state_id, status, automation_excluded FROM kanban_items WHERE item_id=?",
        ("work-git-github-activity",),
    ).fetchone()
    assert dict(row) == {
        "state_id": "done",
        "status": "done",
        "automation_excluded": 1,
    }


def test_newer_kanban_item_sync_update_is_applied():
    conn = _kanban_sync_conn()
    action = SimpleNamespace(
        action_type="UPDATE",
        table_name="kanban_items",
        row_id="work-git-github-activity",
        row_data={
            "item_id": "work-git-github-activity",
            "state_id": "doing",
            "status": "active",
            "automation_excluded": 0,
            "updated_at": "2026-06-27T20:11:00Z",
        },
    )

    assert routes_sync._should_skip_stale_kanban_item_upsert(conn, action) is False
    routes_sync._apply_action(conn, action)

    row = conn.execute(
        "SELECT state_id, status, automation_excluded FROM kanban_items WHERE item_id=?",
        ("work-git-github-activity",),
    ).fetchone()
    assert dict(row) == {
        "state_id": "doing",
        "status": "active",
        "automation_excluded": 0,
    }
