import asyncio
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-routes-alarms-nodes.json"
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-alarms-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_alarms  # noqa: E402


def _settings(*, enabled: bool = False, recurring: bool = True) -> dict:
    value = routes_alarms.default_server_alarm_settings()
    value["slots"][0]["enabled"] = enabled
    value["slots"][0]["recurring"] = recurring
    return value


def test_alarm_settings_read_uses_bounded_read_only_connection(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT, description TEXT)")
    conn.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?)",
        (
            routes_alarms._SERVER_SETTINGS_KEY,
            '{"timezone":"Europe/London","slots":[]}',
        ),
    )
    conn.commit()
    opened = []

    @contextmanager
    def fake_get_read_conn(**kwargs):
        opened.append(kwargs)
        yield conn

    monkeypatch.setattr(routes_alarms, "get_read_conn", fake_get_read_conn)

    settings = routes_alarms.load_server_alarm_settings()

    assert opened == [
        {
            "busy_timeout_ms": 100,
            "operation": "alarm_server_settings",
        }
    ]
    assert settings["timezone"] == "Europe/London"
    assert len(settings["slots"]) == 5
    assert conn.in_transaction is False


def test_alarm_settings_routes_offload_database_work(monkeypatch):
    calls = []
    loaded = _settings()
    saved = _settings(enabled=True)

    async def fake_to_thread(label, func, *args):
        calls.append((label, func, args))
        if func is routes_alarms.load_server_alarm_settings:
            return loaded
        return saved

    monkeypatch.setattr(routes_alarms.timing, "to_thread", fake_to_thread)

    read_result = asyncio.run(routes_alarms.get_server_settings())
    write_result = asyncio.run(
        routes_alarms.put_server_settings(routes_alarms.AlarmSettingsBody(settings=saved))
    )

    assert read_result == {"ok": True, "settings": loaded}
    assert write_result == {"ok": True, "settings": saved}
    assert [call[0] for call in calls] == [
        "alarms.server_settings.read",
        "alarms.server_settings.write",
    ]
    assert calls[0][1] is routes_alarms.load_server_alarm_settings
    assert calls[1][1] is routes_alarms.save_server_alarm_settings
    assert calls[1][2] == (saved,)


def test_alarm_settings_route_reports_locked_database_as_retryable_503(monkeypatch):
    async def locked_to_thread(label, func, *args):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(routes_alarms.timing, "to_thread", locked_to_thread)

    try:
        asyncio.run(routes_alarms.get_server_settings())
    except routes_alarms.HTTPException as exc:
        assert exc.status_code == 503
        assert exc.detail == "database_locked"
    else:
        raise AssertionError("expected alarm settings read to fail fast with HTTP 503")


def test_alarm_scheduler_tick_loads_and_saves_off_thread_without_spanning_publish(
    monkeypatch,
):
    calls = []
    settings = _settings(enabled=True, recurring=False)

    async def fake_to_thread(label, func, *args):
        calls.append(("thread", label))
        if func is routes_alarms.load_server_alarm_settings:
            return settings
        assert func is routes_alarms.save_server_alarm_settings
        assert args == (settings,)
        return settings

    async def fake_publish(slot, cycle_id, loaded_settings):
        calls.append(("publish", cycle_id))
        assert loaded_settings is settings
        assert slot["enabled"] is False
        await asyncio.sleep(0)

    monkeypatch.setattr(routes_alarms.timing, "to_thread", fake_to_thread)
    monkeypatch.setattr(
        routes_alarms,
        "_slot_due",
        lambda slot, now: "cycle-1" if slot["enabled"] else "",
    )
    monkeypatch.setattr(routes_alarms, "_publish_ring", fake_publish)

    result = asyncio.run(routes_alarms._run_alarm_scheduler_tick())

    assert result == {"dirty": True, "fired_count": 1}
    assert calls == [
        ("thread", "alarms.scheduler.load_settings"),
        ("publish", "cycle-1"),
        ("thread", "alarms.scheduler.save_settings"),
    ]
    assert settings["slots"][0]["enabled"] is False
    assert settings["slots"][0]["last_fired_cycle"] == "cycle-1"
    assert all(slot["last_fired_cycle"] == "" for slot in settings["slots"][1:])


def test_alarm_scheduler_blocking_load_does_not_stall_event_loop(monkeypatch):
    def slow_load():
        time.sleep(0.08)
        return _settings()

    monkeypatch.setattr(routes_alarms, "load_server_alarm_settings", slow_load)
    monkeypatch.setattr(routes_alarms, "_slot_due", lambda slot, now: "")

    async def probe():
        ticks = 0
        stop = asyncio.Event()

        async def ticker():
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0.002)

        ticker_task = asyncio.create_task(ticker())
        try:
            result = await routes_alarms._run_alarm_scheduler_tick()
        finally:
            stop.set()
            await ticker_task
        return result, ticks

    result, ticks = asyncio.run(probe())

    assert result == {"dirty": False, "fired_count": 0}
    assert ticks >= 10
