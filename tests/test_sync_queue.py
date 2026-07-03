import contextlib
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-sync-queue-nodes.json"
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-queue-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app.sync import queue  # noqa: E402


def _make_queue_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE sync_queue (
            queue_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            target_node_id TEXT    NOT NULL,
            action_type    TEXT    NOT NULL,
            table_name     TEXT    NOT NULL,
            row_id         TEXT    NOT NULL,
            row_data       TEXT,
            gen            INTEGER NOT NULL,
            created_at     TEXT DEFAULT (datetime('now')),
            sent           INTEGER DEFAULT 0,
            sent_at        TEXT DEFAULT '',
            guid           TEXT DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE nodes (
            node_id TEXT PRIMARY KEY
        )
        """
    )
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-1')")
    return conn


def _patch_queue_db(monkeypatch, conn: sqlite3.Connection) -> None:
    @contextlib.contextmanager
    def fake_get_conn():
        yield conn
        conn.commit()

    monkeypatch.setattr(queue, "get_conn", fake_get_conn)


def _insert_action(
    conn: sqlite3.Connection,
    action_type: str,
    *,
    target_node_id: str = "peer-1",
) -> int:
    cur = conn.execute(
        """
        INSERT INTO sync_queue
            (target_node_id, action_type, table_name, row_id, row_data, gen, guid)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            target_node_id,
            action_type,
            "_system" if action_type.startswith("sync_git_") else "settings",
            action_type,
            None if action_type.startswith("sync_git_") else "{}",
            10,
            f"guid-{action_type}",
        ),
    )
    return int(cur.lastrowid)


def test_pending_actions_prioritise_git_system_actions(monkeypatch):
    conn = _make_queue_db()
    _patch_queue_db(monkeypatch, conn)
    update_1 = _insert_action(conn, "UPDATE")
    update_2 = _insert_action(conn, "UPDATE")
    git_outer = _insert_action(conn, "sync_git_outer")
    update_3 = _insert_action(conn, "UPDATE")
    git_non_root = _insert_action(conn, "sync_git_non_root")

    pending = queue.get_pending_actions("peer-1", limit=50)

    assert [row["queue_id"] for row in pending] == [git_outer, git_non_root]
    assert update_1 < update_2 < git_outer < update_3 < git_non_root


def test_pending_actions_return_fifo_db_rows_when_no_git_system_actions(monkeypatch):
    conn = _make_queue_db()
    _patch_queue_db(monkeypatch, conn)
    update_1 = _insert_action(conn, "UPDATE")
    delete_1 = _insert_action(conn, "DELETE")
    _insert_action(conn, "sync_git_inner", target_node_id="peer-2")

    pending = queue.get_pending_actions("peer-1", limit=50)

    assert [row["queue_id"] for row in pending] == [update_1, delete_1]


def test_purge_unsent_db_actions_preserves_all_git_system_actions(monkeypatch):
    conn = _make_queue_db()
    _patch_queue_db(monkeypatch, conn)
    update_id = _insert_action(conn, "UPDATE")
    outer_id = _insert_action(conn, "sync_git_outer")
    non_root_id = _insert_action(conn, "sync_git_non_root")
    inner_id = _insert_action(conn, "sync_git_inner")

    purged = queue.purge_unsent_db_actions("peer-1")

    rows = {
        row["queue_id"]: row["sent"]
        for row in conn.execute("SELECT queue_id, sent FROM sync_queue ORDER BY queue_id")
    }
    assert purged == 1
    assert rows[update_id] == 1
    assert rows[outer_id] == 0
    assert rows[non_root_id] == 0
    assert rows[inner_id] == 0


def test_mark_sent_stamps_sent_at(monkeypatch):
    conn = _make_queue_db()
    _patch_queue_db(monkeypatch, conn)
    action_id = _insert_action(conn, "UPDATE")

    queue.mark_sent([action_id])

    row = conn.execute(
        "SELECT sent, sent_at FROM sync_queue WHERE queue_id=?",
        (action_id,),
    ).fetchone()
    assert row["sent"] == 1
    assert row["sent_at"]


def test_try_mark_sent_returns_false_quickly_when_sqlite_writer_busy(monkeypatch, tmp_path):
    db_path = tmp_path / "sync-queue.sqlite"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE sync_queue (
                queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sent INTEGER DEFAULT 0,
                sent_at TEXT DEFAULT ''
            )
            """
        )
        conn.execute("INSERT INTO sync_queue (sent, sent_at) VALUES (0, '')")
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr(queue.cfg, "DB_PATH", str(db_path))

    locker = sqlite3.connect(db_path, timeout=0, isolation_level=None)
    try:
        locker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()

        assert queue.try_mark_sent([1]) is False
        assert time.monotonic() - started < 0.5
    finally:
        locker.execute("ROLLBACK")
        locker.close()

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT sent FROM sync_queue WHERE queue_id=1").fetchone()[0] == 0
    finally:
        conn.close()

    assert queue.try_mark_sent([1]) is True

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT sent, sent_at FROM sync_queue WHERE queue_id=1").fetchone()
        assert row[0] == 1
        assert row[1]
    finally:
        conn.close()


def test_sent_queue_retention_preview_and_apply_preserves_unsent(monkeypatch):
    conn = _make_queue_db()
    _patch_queue_db(monkeypatch, conn)
    old_sent_id = _insert_action(conn, "UPDATE")
    recent_sent_id = _insert_action(conn, "UPDATE")
    unsent_id = _insert_action(conn, "UPDATE")
    conn.execute(
        "UPDATE sync_queue SET sent=1, sent_at='2000-01-01 00:00:00' WHERE queue_id=?",
        (old_sent_id,),
    )
    conn.execute(
        "UPDATE sync_queue SET sent=1, sent_at='2099-01-01 00:00:00' WHERE queue_id=?",
        (recent_sent_id,),
    )

    preview = queue.get_sent_queue_retention_summary(older_than_hours=24)

    assert preview["eligible_sent_rows"] == 1
    assert preview["would_delete"] == 1
    assert preview["queue"]["unsent"] == 1
    assert preview["queue"]["sent"] == 2

    result = queue.prune_sent_actions(older_than_hours=0, limit=1, apply=True)

    remaining = {
        row["queue_id"]: row["sent"]
        for row in conn.execute("SELECT queue_id, sent FROM sync_queue ORDER BY queue_id")
    }
    assert result["deleted_rows"] == 1
    assert old_sent_id not in remaining
    assert remaining[recent_sent_id] == 1
    assert remaining[unsent_id] == 0
    assert result["after"]["queue"]["unsent"] == 1


def test_active_postgres_skips_kanban_table_enqueue(monkeypatch):
    conn = _make_queue_db()
    config = type("Config", (), {"active_store": "postgres"})()

    monkeypatch.setattr(queue.cfg, "KANBAN_DATASTORE_CONFIG", config)

    queue.enqueue_for_all_peers(
        conn,
        "UPDATE",
        "kanban_audit_log",
        "audit-1",
        {"audit_id": "audit-1"},
        10,
    )
    queue.enqueue_for_all_peers(
        conn,
        "UPDATE",
        "settings",
        "setting-1",
        {"key": "setting-1"},
        10,
    )

    rows = conn.execute("SELECT table_name, row_id FROM sync_queue ORDER BY queue_id").fetchall()
    assert [dict(row) for row in rows] == [{"table_name": "settings", "row_id": "setting-1"}]
