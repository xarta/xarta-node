import asyncio
import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

_NODES_JSON_PATH = Path(tempfile.gettempdir()) / "blueprints-test-nodes.json"
_NODES_JSON_PATH.write_text(
    json.dumps(
        {
            "nodes": [
                {
                    "node_id": "test-node",
                    "display_name": "Test Node",
                    "host_machine": "test-host",
                    "primary_hostname": "test.local",
                    "primary_ip": "127.0.0.1",
                    "tailnet": "",
                    "tailnet_hostname": "test-tailnet.local",
                    "tailnet_ip": "",
                    "sync_port": 8080,
                    "active": True,
                }
            ]
        }
    ),
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(_NODES_JSON_PATH))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "seekdb")
os.environ.setdefault("SEEKDB_USER", "seekdb")
os.environ.setdefault("SEEKDB_PASSWORD", "seekdb")


def app_event_modules():
    from app import routes_events
    from app.events import AppEvent

    return AppEvent, routes_events


def test_publish_event_default_requires_persistence(monkeypatch):
    AppEvent, routes_events = app_event_modules()
    event = AppEvent.create("unit.event", "Unit", "Unit event.")

    def locked(_event):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(routes_events, "_persist", locked)

    with pytest.raises(sqlite3.OperationalError):
        asyncio.run(routes_events.publish_event(event))


def test_publish_event_best_effort_still_publishes_when_sqlite_locked(monkeypatch):
    AppEvent, routes_events = app_event_modules()
    event = AppEvent.create("unit.event", "Unit", "Unit event.")
    published: list[str] = []

    def locked(_event):
        raise sqlite3.OperationalError("database is locked")

    async def fake_publish(app_event):
        published.append(app_event.event_id)

    monkeypatch.setattr(routes_events, "_persist", locked)
    monkeypatch.setattr(routes_events.bus, "publish", fake_publish)

    result = asyncio.run(routes_events.publish_event(event, persistence_required=False))

    assert result.event_id == event.event_id
    assert published == [event.event_id]
