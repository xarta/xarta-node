import asyncio
import os
import sys
import tempfile
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


def test_full_backup_rejection_suppressed_until_queue_leaves_overflow(monkeypatch):
    backup_attempts = []
    action_posts = []
    marked_sent = []
    depth = {"value": 3}

    async def fake_send_full_backup(node_id, peer_urls):
        backup_attempts.append((node_id, tuple(peer_urls)))
        return False

    monkeypatch.setattr(drain, "_full_backup_rejected_overflow_peers", set())
    monkeypatch.setattr(drain.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(drain.cfg, "COMMIT_TS", 123)
    monkeypatch.setattr(drain.cfg, "SYNC_SECRET", "")
    monkeypatch.setattr(drain.cfg, "SYNC_QUEUE_MAX_DEPTH", 2)
    monkeypatch.setattr(drain.cfg, "SYNC_BATCH_SIZE", 1)
    monkeypatch.setattr(drain, "get_queue_depth", lambda node_id: depth["value"])
    monkeypatch.setattr(drain, "_send_full_backup", fake_send_full_backup)
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
    monkeypatch.setattr(drain, "mark_sent", lambda queue_ids: marked_sent.append(queue_ids))
    monkeypatch.setattr(drain, "_make_sync_client", lambda timeout: _FakeClient(action_posts))

    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))
    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(backup_attempts) == 1
    assert len(action_posts) == 2
    assert marked_sent == [[10], [10]]

    depth["value"] = 1
    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(backup_attempts) == 1
    assert len(action_posts) == 3

    depth["value"] = 3
    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(backup_attempts) == 2
    assert len(action_posts) == 4


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
    monkeypatch.setattr(drain, "mark_sent", lambda queue_ids: marked_sent.append(queue_ids))
    monkeypatch.setattr(
        drain,
        "_make_sync_client",
        lambda timeout: _FakeClient(action_posts, status_code=409),
    )

    asyncio.run(drain._drain_peer("peer-1", ["http://peer-1"]))

    assert len(action_posts) == 1
    assert marked_sent == []
