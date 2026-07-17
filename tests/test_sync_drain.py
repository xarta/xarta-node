import asyncio
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-sync-drain-nodes.json"
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-drain-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app.sync import drain  # noqa: E402


class _Response:
    def __init__(self, status_code=204):
        self.status_code = status_code


class _FakeClient:
    def __init__(self, posts, status_code=204):
        self._posts = posts
        self._status_code = status_code

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, **kwargs):
        self._posts.append((url, kwargs))
        return _Response(self._status_code)


def test_queue_overflow_continues_batched_row_drain(monkeypatch):
    action_posts = []
    marked_sent = []

    monkeypatch.setattr(drain.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(drain.cfg, "COMMIT_TS", 123)
    monkeypatch.setattr(drain.cfg, "SYNC_SECRET", "")
    monkeypatch.setattr(drain.cfg, "SYNC_QUEUE_MAX_DEPTH", 2)
    monkeypatch.setattr(drain.cfg, "SYNC_BATCH_SIZE", 1)
    monkeypatch.setattr(drain, "get_queue_depth", lambda node_id: 3)
    monkeypatch.setattr(
        drain,
        "get_pending_actions",
        lambda node_id, limit: [
            {
                "queue_id": 10,
                "action_type": "upsert",
                "table_name": "personal_git_commits",
                "row_id": "commit-1",
                "row_data": "{}",
                "gen": 1,
                "guid": "guid-1",
            }
        ],
    )
    monkeypatch.setattr(
        drain,
        "try_mark_sent",
        lambda queue_ids: not marked_sent.append(queue_ids),
    )
    monkeypatch.setattr(drain, "_make_sync_client", lambda timeout: _FakeClient(action_posts))

    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))
    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(action_posts) == 2
    assert marked_sent == [[10], [10]]


def test_action_commit_guard_rejection_keeps_actions_queued(monkeypatch):
    action_posts = []
    marked_sent = []

    monkeypatch.setattr(drain.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(drain.cfg, "COMMIT_TS", 123)
    monkeypatch.setattr(drain.cfg, "SYNC_SECRET", "")
    monkeypatch.setattr(drain.cfg, "SYNC_QUEUE_MAX_DEPTH", 1000)
    monkeypatch.setattr(drain.cfg, "SYNC_BATCH_SIZE", 1)
    monkeypatch.setattr(drain, "get_queue_depth", lambda node_id: 1)
    monkeypatch.setattr(
        drain,
        "get_pending_actions",
        lambda node_id, limit: [
            {
                "queue_id": 10,
                "action_type": "upsert",
                "table_name": "personal_git_commits",
                "row_id": "commit-1",
                "row_data": "{}",
                "gen": 1,
                "guid": "guid-1",
            }
        ],
    )
    monkeypatch.setattr(
        drain,
        "try_mark_sent",
        lambda queue_ids: not marked_sent.append(queue_ids),
    )
    monkeypatch.setattr(
        drain,
        "_make_sync_client",
        lambda timeout: _FakeClient(action_posts, status_code=409),
    )

    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(action_posts) == 1
    assert marked_sent == []


def test_successful_post_with_busy_sqlite_defers_mark_sent_without_retrying_urls(monkeypatch):
    action_posts = []

    monkeypatch.setattr(drain.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(drain.cfg, "COMMIT_TS", 123)
    monkeypatch.setattr(drain.cfg, "SYNC_SECRET", "")
    monkeypatch.setattr(drain.cfg, "SYNC_QUEUE_MAX_DEPTH", 1000)
    monkeypatch.setattr(drain.cfg, "SYNC_BATCH_SIZE", 1)
    monkeypatch.setattr(drain, "get_queue_depth", lambda node_id: 1)
    monkeypatch.setattr(
        drain,
        "get_pending_actions",
        lambda node_id, limit: [
            {
                "queue_id": 10,
                "action_type": "upsert",
                "table_name": "personal_git_commits",
                "row_id": "commit-1",
                "row_data": "{}",
                "gen": 1,
                "guid": "guid-1",
            }
        ],
    )
    monkeypatch.setattr(drain, "try_mark_sent", lambda queue_ids: False)
    monkeypatch.setattr(drain, "_make_sync_client", lambda timeout: _FakeClient(action_posts))

    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1", "http://peer-1-tail"]))

    assert len(action_posts) == 1
    assert action_posts[0][0] == "http://peer-1/api/v1/sync/actions"


def test_self_target_cleanup_defers_busy_sqlite_without_raising(monkeypatch, tmp_path):
    db_path = tmp_path / "drain-housekeeping.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE sync_queue (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                target_node_id TEXT NOT NULL,
                sent INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            "INSERT INTO sync_queue (target_node_id, sent) VALUES (?, 0)",
            ("test-node",),
        )
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(drain.cfg, "DB_PATH", str(db_path))
    monkeypatch.setattr(drain.cfg, "NODE_ID", "test-node")

    locker = sqlite3.connect(db_path, timeout=0, isolation_level=None)
    try:
        locker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()

        drain._discard_self_target_actions()
        assert time.monotonic() - started < 0.5
    finally:
        locker.execute("ROLLBACK")
        locker.close()

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT sent FROM sync_queue WHERE queue_id=1").fetchone()[0] == 0
    finally:
        conn.close()

    drain._discard_self_target_actions()

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT sent FROM sync_queue WHERE queue_id=1").fetchone()[0] == 1
    finally:
        conn.close()


def test_drain_integrity_read_is_read_only_and_does_not_change_journal_mode(monkeypatch, tmp_path):
    db_path = tmp_path / "drain-integrity.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0] == "delete"
        conn.execute("CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO sync_meta (key, value) VALUES ('integrity_ok', 'true')")
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(drain.cfg, "DB_PATH", str(db_path))

    assert drain._drain_integrity_ok_sync() is True
    with drain.get_read_conn(
        busy_timeout_ms=drain._DRAIN_SQLITE_BUSY_TIMEOUT_MS,
        operation="test_sync_drain_integrity",
    ) as read_conn:
        assert read_conn.in_transaction is False
        try:
            read_conn.execute("UPDATE sync_meta SET value='false' WHERE key='integrity_ok'")
        except sqlite3.OperationalError as exc:
            assert "readonly" in str(exc).lower()
        else:
            raise AssertionError("expected the sync integrity connection to reject writes")

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "delete"
    finally:
        conn.close()


def test_drain_integrity_read_fails_within_busy_bound(monkeypatch, tmp_path):
    db_path = tmp_path / "drain-integrity-locked.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        conn.execute("INSERT INTO sync_meta (key, value) VALUES ('integrity_ok', 'true')")
        conn.commit()
    finally:
        conn.close()

    monkeypatch.setattr(drain.cfg, "DB_PATH", str(db_path))
    locker = sqlite3.connect(db_path, timeout=0, isolation_level=None)
    try:
        locker.execute("BEGIN EXCLUSIVE")
        started = time.monotonic()
        try:
            drain._drain_integrity_ok_sync()
        except sqlite3.OperationalError as exc:
            assert "locked" in str(exc).lower()
        else:
            raise AssertionError("expected the bounded read to fail while SQLite is locked")
        assert time.monotonic() - started < 0.5
    finally:
        locker.execute("ROLLBACK")
        locker.close()
