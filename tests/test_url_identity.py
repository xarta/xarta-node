import asyncio
import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-nodes.json"
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

from app import routes_bookmarks  # noqa: E402
from app.db import _canonicalize_visit_history_urls, _dedup_visits  # noqa: E402
from app.url_identity import normalize_url_identity  # noqa: E402


def test_url_identity_removes_only_known_volatile_query_params():
    assert (
        normalize_url_identity(
            "https://Example-Node.test/ui?group=probes&tab=docs&_fresh=123"
        )
        == "https://example-node.test/ui?group=probes&tab=docs"
    )
    assert (
        normalize_url_identity("https://example.test/app?page=one&view=two")
        == "https://example.test/app?page=one&view=two"
    )


def test_visit_history_canonicalization_collapses_fresh_versions():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE visits (
            visit_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            visit_count INTEGER NOT NULL DEFAULT 1,
            visited_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE visit_events (
            event_id TEXT PRIMARY KEY,
            normalized_url TEXT NOT NULL,
            visited_at TEXT NOT NULL,
            dwell_seconds INTEGER
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO visits (visit_id, url, normalized_url, visit_count, visited_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (
                "old",
                "https://xarta.test/ui?doc=blueprints&_fresh=111",
                "https://xarta.test/ui?doc=blueprints&_fresh=111",
                2,
                "2026-05-20T08:00:00+00:00",
            ),
            (
                "new",
                "https://xarta.test/ui?doc=blueprints&_fresh=222",
                "https://xarta.test/ui?doc=blueprints&_fresh=222",
                3,
                "2026-05-20T09:00:00+00:00",
            ),
        ],
    )
    conn.executemany(
        "INSERT INTO visit_events (event_id, normalized_url, visited_at) VALUES (?, ?, ?)",
        [
            (
                "evt-old",
                "https://xarta.test/ui?doc=blueprints&_fresh=111",
                "2026-05-20T08:00:00+00:00",
            ),
            (
                "evt-new",
                "https://xarta.test/ui?doc=blueprints&_fresh=222",
                "2026-05-20T09:00:00+00:00",
            ),
        ],
    )

    _canonicalize_visit_history_urls(conn)
    _dedup_visits(conn)

    visits = conn.execute(
        "SELECT visit_id, normalized_url, visit_count, visited_at FROM visits"
    ).fetchall()
    assert visits == [
        (
            "new",
            "https://xarta.test/ui?doc=blueprints",
            5,
            "2026-05-20T09:00:00+00:00",
        )
    ]
    event_urls = conn.execute(
        "SELECT DISTINCT normalized_url FROM visit_events ORDER BY normalized_url"
    ).fetchall()
    assert event_urls == [("https://xarta.test/ui?doc=blueprints",)]


def test_visit_history_page_endpoint_filters_sorts_and_pages(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE visits (
            visit_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'visit-recorder',
            dwell_seconds INTEGER,
            bookmark_id TEXT,
            visited_at TEXT NOT NULL,
            visit_count INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO visits (visit_id, url, normalized_url, domain, title, visited_at, visit_count)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "v1",
                "https://xarta.test/one",
                "https://xarta.test/one",
                "xarta.test",
                "One",
                "2026-05-20T08:00:00+00:00",
                1,
            ),
            (
                "v2",
                "https://xarta.test/two",
                "https://xarta.test/two",
                "xarta.test",
                "Two",
                "2026-05-20T09:00:00+00:00",
                2,
            ),
            (
                "v3",
                "https://other.test/three",
                "https://other.test/three",
                "other.test",
                "Three",
                "2026-05-20T10:00:00+00:00",
                3,
            ),
        ],
    )

    @contextmanager
    def fake_get_conn():
        yield conn

    monkeypatch.setattr(routes_bookmarks, "get_conn", fake_get_conn)

    page = asyncio.run(
        routes_bookmarks.list_visits_page(
            q="xarta",
            saved="all",
            sort="visited_at",
            direction="desc",
            limit=1,
            offset=1,
        )
    )

    assert page.total == 2
    assert page.total_visit_count == 3
    assert page.limit == 1
    assert page.offset == 1
    assert [row.visit_id for row in page.items] == ["v1"]

    domains = asyncio.run(
        routes_bookmarks.list_visit_domains_page(
            q="",
            saved="all",
            sort="domain",
            direction="desc",
            limit=1,
            offset=0,
            expanded_domains=["xarta.test"],
        )
    )

    assert domains.total_domains == 2
    assert domains.total_urls == 3
    assert domains.total_visit_count == 6
    assert domains.limit == 1
    assert domains.groups[0].domain == "xarta.test"
    assert domains.groups[0].url_count == 2
    assert domains.groups[0].total_visit_count == 3
    assert [item.visit_id for item in domains.groups[0].items] == ["v2", "v1"]
