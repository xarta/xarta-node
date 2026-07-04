import asyncio
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

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

from app import routes_sync  # noqa: E402
from app.models import SyncAction  # noqa: E402


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
    opened = []

    @contextmanager
    def fake_get_conn():
        opened.append("open")
        yield conn
        conn.commit()

    monkeypatch.setattr(routes_sync, "get_conn", fake_get_conn)
    monkeypatch.setattr(routes_sync.cfg, "NODE_ID", "self")
    monkeypatch.setattr(routes_sync.cfg, "NODE_NAME", "Self Node")

    status = routes_sync._sync_status_sync()

    assert opened == ["open"]
    assert status.gen == 42
    assert status.queue_depths == {"peer-a": 2, "peer-b": 0}
    assert status.peer_count == 2


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


def test_git_pull_batch_restarts_once_when_runtime_repo_changes(monkeypatch):
    calls = []
    restarts = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return label in {"outer", "non_root", "inner"}

    async def fake_restart():
        restarts.append("restart")
        return True

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
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
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer", "non_root", "inner"]))

    assert calls == ["outer", "non_root", "inner"]
    assert restarts == ["restart"]


def test_git_pull_batch_restarts_when_runtime_process_is_stale(monkeypatch):
    calls = []
    restarts = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return False

    async def fake_runtime_repo_is_stale(repo_path, label):
        return label == "outer"

    async def fake_restart():
        restarts.append("restart")
        return True

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_runtime_repo_is_stale", fake_runtime_repo_is_stale)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
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
    assert restarts == ["restart"]


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
