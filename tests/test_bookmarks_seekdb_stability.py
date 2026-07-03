import asyncio
import inspect
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-seekdb-stability-nodes.json"
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_bookmarks, routes_health, seekdb_sync  # noqa: E402
from app.models import BookmarkCreate  # noqa: E402


@contextmanager
def _bookmark_counts_conn(bookmarks: int = 3, visits: int = 2):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE bookmarks (bookmark_id TEXT PRIMARY KEY);
        CREATE TABLE visits (visit_id TEXT PRIMARY KEY);
        CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO sync_meta (key, value) VALUES ('gen', '7'), ('integrity_ok', 'true');
        CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT,
            severity TEXT,
            title TEXT,
            message TEXT,
            source TEXT,
            created_at REAL,
            payload_json TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO bookmarks (bookmark_id) VALUES (?)",
        [(f"b-{idx}",) for idx in range(bookmarks)],
    )
    conn.executemany(
        "INSERT INTO visits (visit_id) VALUES (?)",
        [(f"v-{idx}",) for idx in range(visits)],
    )
    try:
        yield conn
    finally:
        conn.close()


def test_bookmarks_health_degrades_when_seekdb_counts_fail(monkeypatch):
    async def run():
        async def fail_counts(*args, **kwargs):
            raise ConnectionError("connection refused\nTraceback: private stack")

        async def ok_embed(*args, **kwargs):
            return [[0.0] * 2048]

        monkeypatch.setattr(routes_bookmarks, "get_conn", _bookmark_counts_conn)
        monkeypatch.setattr(routes_bookmarks, "seekdb_counts_async", fail_counts)
        monkeypatch.setattr(routes_bookmarks, "embed", ok_embed)

        payload = await routes_bookmarks.bookmarks_health()

        assert payload["status"] == "degraded"
        assert payload["sqlite"] == "ok"
        assert payload["seekdb"] == "error"
        assert "connection refused" in payload["seekdb_error"]
        assert "Traceback" not in payload["seekdb_error"]
        assert payload["bookmark_count"] == 3
        assert payload["visit_count"] == 2
        assert payload["seekdb_stale"] == 3
        assert payload["seekdb_visits_stale"] == 2

    asyncio.run(run())


def test_normal_health_does_not_call_seekdb(monkeypatch):
    monkeypatch.setattr(routes_health, "get_conn", _bookmark_counts_conn)

    payload = routes_health.health()

    assert payload.status == "ok"
    assert payload.node_id == "test-node"
    assert payload.gen == 7
    assert payload.integrity_ok is True


def test_sync_single_flight_coalesces_followup(monkeypatch):
    async def run():
        seekdb_sync._reset_sync_controller_for_tests()
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def body():
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return {
                "bookmarks_synced": 1,
                "bookmarks_deleted": 0,
                "visits_synced": 0,
                "visits_deleted": 0,
            }

        monkeypatch.setattr(seekdb_sync, "_sync_once_body", body)

        first = asyncio.create_task(seekdb_sync.sync_once())
        await started.wait()
        second = asyncio.create_task(seekdb_sync.sync_once())
        third = asyncio.create_task(seekdb_sync.sync_once())
        await asyncio.sleep(0)

        assert calls == 1
        assert await second == {
            "bookmarks_synced": 0,
            "bookmarks_deleted": 0,
            "visits_synced": 0,
            "visits_deleted": 0,
            "coalesced": True,
        }
        assert await third == {
            "bookmarks_synced": 0,
            "bookmarks_deleted": 0,
            "visits_synced": 0,
            "visits_deleted": 0,
            "coalesced": True,
        }

        release.set()
        assert await first == {
            "bookmarks_synced": 2,
            "bookmarks_deleted": 0,
            "visits_synced": 0,
            "visits_deleted": 0,
        }
        assert calls == 2

    asyncio.run(run())


def test_sync_failure_backoff_and_recovery_events(monkeypatch):
    async def run():
        seekdb_sync._reset_sync_controller_for_tests()
        monkeypatch.setattr(seekdb_sync, "SYNC_BACKOFF_BASE_SECONDS", 0.25)
        monkeypatch.setattr(seekdb_sync, "SYNC_BACKOFF_MAX_SECONDS", 0.25)
        events = []

        async def capture_event(**kwargs):
            events.append(kwargs)

        async def failing_body():
            raise ConnectionError("connection refused\nTraceback: private stack")

        async def successful_body():
            return {
                "bookmarks_synced": 0,
                "bookmarks_deleted": 0,
                "visits_synced": 0,
                "visits_deleted": 0,
            }

        monkeypatch.setattr(seekdb_sync, "_publish_seekdb_status_event", capture_event)
        monkeypatch.setattr(seekdb_sync, "_sync_once_body", failing_body)

        failed = await seekdb_sync.sync_once()
        assert failed["skipped"] == "failure"
        state = seekdb_sync.get_sync_controller_state()
        assert state["failures"] == 1
        assert state["degraded"] is True
        assert state["backoff_remaining"] > 0
        assert [event["event_type"] for event in events] == ["browser_links.seekdb.degraded"]

        skipped = await seekdb_sync.sync_once()
        assert skipped["skipped"] == "backoff"
        assert [event["event_type"] for event in events] == ["browser_links.seekdb.degraded"]

        seekdb_sync._sync_state["backoff_until"] = 0.0
        monkeypatch.setattr(seekdb_sync, "_sync_once_body", successful_body)
        recovered = await seekdb_sync.sync_once()

        assert recovered["bookmarks_synced"] == 0
        assert seekdb_sync.get_sync_controller_state()["degraded"] is False
        assert [event["event_type"] for event in events] == [
            "browser_links.seekdb.degraded",
            "browser_links.seekdb.recovered",
        ]

    asyncio.run(run())


def test_async_paths_use_seekdb_isolation_wrappers():
    health_source = inspect.getsource(routes_bookmarks.bookmarks_health)
    search_source = inspect.getsource(routes_bookmarks.search_bookmarks)
    sync_source = inspect.getsource(seekdb_sync._sync_once_body)

    assert "await seekdb_counts_async" in health_source
    assert "seekdb_counts()" not in health_source
    assert "keyword_search_bookmarks_async" in search_source
    assert "vector_search_visits_async" in search_source
    assert "await init_seekdb_async" in sync_source


def test_bookmark_create_accepts_modal_null_fields():
    body = BookmarkCreate.model_validate(
        {
            "url": "https://example.test/null-fields",
            "title": "Null fields",
            "description": None,
            "tags": None,
            "folder": None,
            "notes": None,
            "favicon_url": None,
            "source": None,
        }
    )

    assert body.description == ""
    assert body.tags == []
    assert body.folder == ""
    assert body.notes == ""
    assert body.favicon_url == ""
    assert body.source == "manual"
