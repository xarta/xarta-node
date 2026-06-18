import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-personal-routes-nodes.json"
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

from app import routes_personal  # noqa: E402


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO sync_meta (key, value) VALUES ('gen', '0'), ('last_write_at', ''), ('last_write_by', '');
        CREATE TABLE nodes (node_id TEXT PRIMARY KEY);
        CREATE TABLE sync_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_node_id TEXT,
            action_type TEXT,
            table_name TEXT,
            row_id TEXT,
            row_data TEXT,
            gen INTEGER,
            guid TEXT,
            sent INTEGER DEFAULT 0
        );
        CREATE TABLE personal_events (
            event_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL DEFAULT 'manual',
            source_ref TEXT,
            source_hash TEXT,
            kind TEXT NOT NULL DEFAULT 'event',
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT,
            content_projection TEXT,
            start_at TEXT,
            end_at TEXT,
            local_date TEXT,
            timezone TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            priority TEXT,
            privacy_level TEXT NOT NULL DEFAULT 'normal',
            tags_json TEXT NOT NULL DEFAULT '[]',
            entities_json TEXT NOT NULL DEFAULT '[]',
            related_work_items_json TEXT NOT NULL DEFAULT '[]',
            related_tasks_json TEXT NOT NULL DEFAULT '[]',
            related_import_batches_json TEXT NOT NULL DEFAULT '[]',
            file_refs_json TEXT NOT NULL DEFAULT '[]',
            db_refs_json TEXT NOT NULL DEFAULT '[]',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            projection_state TEXT NOT NULL DEFAULT 'hot',
            provenance_state TEXT NOT NULL DEFAULT 'linked',
            last_rendered_at TEXT,
            projection_expires_at TEXT,
            retention_days INTEGER DEFAULT 60,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE personal_sources (
            source_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            label TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'unknown',
            last_seen_at TEXT,
            health_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE personal_import_batches (
            import_batch_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_ref TEXT,
            title TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending_review',
            local_date TEXT,
            started_at TEXT,
            completed_at TEXT,
            privacy_level TEXT NOT NULL DEFAULT 'normal',
            artifact_refs_json TEXT NOT NULL DEFAULT '[]',
            blocker_refs_json TEXT NOT NULL DEFAULT '[]',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE personal_time_audit (
            audit_id TEXT PRIMARY KEY,
            actor TEXT NOT NULL DEFAULT '',
            source_surface TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL DEFAULT '',
            target_ref TEXT NOT NULL DEFAULT '',
            file_ref TEXT NOT NULL DEFAULT '',
            db_ref TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            request_id TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL DEFAULT '',
            result TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    return conn


@contextmanager
def _conn_context(conn: sqlite3.Connection):
    yield conn
    conn.commit()


def _patch_conn(monkeypatch, conn: sqlite3.Connection) -> None:
    monkeypatch.setattr(routes_personal, "get_conn", lambda: _conn_context(conn))


def test_personal_events_filters_and_shape(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, kind, title, local_date, timezone,
            status, tags_json, related_work_items_json, related_import_batches_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-1",
            "manual",
            "10-20-personal-log.md",
            "entry",
            "Morning diary note",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["diary", "personal"]),
            json.dumps(["work-1"]),
            json.dumps(["batch-1"]),
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_events (event_id, source_type, kind, title, local_date, timezone, status, tags_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-2",
            "git",
            "git",
            "Other day",
            "2026-06-17",
            "Europe/London",
            "open",
            json.dumps(["git"]),
        ),
    )

    result = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-06-18",
            date_end="2026-06-18",
            tag="diary",
            related_work_item="work-1",
            limit=20,
            offset=0,
        )
    )

    assert result["pagination"]["count"] == 1
    item = result["items"][0]
    assert item["event_id"] == "evt-1"
    assert item["source"]["type"] == "manual"
    assert item["tags"] == ["diary", "personal"]
    assert item["related"]["work_items"] == ["work-1"]


def test_personal_import_batches_and_sources(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_sources (source_id, source_type, label, status, health_json)
        VALUES ('src-interests', 'interests-ingestion', 'Interests', 'ok', ?)
        """,
        (json.dumps({"last_run": "2026-06-18"}),),
    )
    conn.execute(
        """
        INSERT INTO personal_import_batches (
            import_batch_id, source_type, source_ref, title, status, local_date, artifact_refs_json
        )
        VALUES ('batch-1', 'interests-ingestion', 'run-1', 'Interests run', 'done', '2026-06-18', ?)
        """,
        (json.dumps(["docs/personal/interests-dashboard.md"]),),
    )

    sources = asyncio.run(routes_personal.list_personal_sources())
    batches = asyncio.run(
        routes_personal.list_personal_import_batches(
            date_start="2026-06-18",
            date_end="2026-06-18",
            source_type="interests-ingestion",
            limit=10,
            offset=0,
        )
    )

    assert sources["items"][0]["health"]["last_run"] == "2026-06-18"
    assert batches["items"][0]["import_batch_id"] == "batch-1"
    assert batches["items"][0]["artifact_refs"] == ["docs/personal/interests-dashboard.md"]


def test_personal_rehydrate_reads_file_ref(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)
    entry = tmp_path / "2026" / "06" / "18" / "10-20-personal-log.md"
    entry.parent.mkdir(parents=True)
    entry.write_text(
        "---\nschema: xarta.diary.entry.v1\n---\n\nRehydrated body\n", encoding="utf-8"
    )
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone,
            file_refs_json, projection_state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-slim",
            "diary-file",
            "entry",
            "Slim entry",
            "2026-06-18",
            "Europe/London",
            json.dumps(["2026/06/18/10-20-personal-log.md"]),
            "slim",
        ),
    )

    result = asyncio.run(
        routes_personal.rehydrate_personal_projection(
            routes_personal.PersonalRehydrateRequest(event_id="evt-slim")
        )
    )

    assert result["ok"] is True
    assert result["rehydrated"] is True
    assert result["event"]["projection_state"] == "hot"
    assert "Rehydrated body" in result["event"]["content_projection"]


def test_imports_dashboard_parses_interests_and_git(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    lone_wolf = tmp_path / "lone-wolf"
    dashboard = lone_wolf / "docs" / "interests" / "HERMES-INTERESTS-INGESTION-DASHBOARD.md"
    dashboard.parent.mkdir(parents=True)
    dashboard.write_text(
        """---
source_snapshot_at: 2026-06-18T12:00:00Z
source_digest: sha256:testdigest
---

# Hermes Interests Ingestion Dashboard

Overall: **OK**

- Source snapshot: `2026-06-18T12:00:00Z`
- Source digest: `sha256:testdigest`
- Pending review: `0`
- Actionable backlog: `0`

## Category Summary

| Category | Raw | Media | Extracted | Results | Wiki pages | Completed | Source unavailable | Pending | Latest proof artifact |
|---|---|---|---|---|---|---|---|---|---|
| `testing` | 1 | 2 | 3 | 4 | 5 | 6 | 0 | 0 | [proof.json](../../interests/testing/results/proof.json) |

## Input Health

| Input | State | Note | Generated | Evidence |
|---|---|---|---|---|
| Backlog | OK: no_actionable_dispatch_backlog | actionable=0 | 2026-06-18T12:00:00Z | [backlog.json](../../health/backlog.json) |

## Recent Completed Work

| When | Category | Work type | Status | Artifact |
|---|---|---|---|---|
| 2026-06-18T12:00:00Z | `testing` | `wiki_update` | `completed` | [proof](../../proof.json) |

## Source-Unavailable

| When | Category | Work type | Artifact |
|---|---|---|---|

## Pending And Blockers

No pending-review items.

No actionable backlog samples.

## Completion Blockers

- None reported by the latest final acceptance report.

## Rerun Status

The dashboard generator writes only when the source digest changes.
""",
        encoding="utf-8",
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init"], check=True, stdout=subprocess.PIPE)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.test"], check=True
    )
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test User"], check=True)
    (repo / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-m", "Initial"], check=True, stdout=subprocess.PIPE
    )

    monkeypatch.setattr(routes_personal, "LONE_WOLF_ROOT", lone_wolf)
    monkeypatch.setattr(
        routes_personal,
        "DEFAULT_PERSONAL_GIT_REPOS",
        (("test-repo", str(repo), "Test repo"),),
    )

    result = asyncio.run(routes_personal.get_imports_dashboard())

    assert result["status"] == "ok"
    assert result["interests"]["source_digest"] == "sha256:testdigest"
    assert result["interests"]["pending_review"] == 0
    assert result["interests"]["category_summary"][0]["Category"] == "testing"
    assert (
        result["interests"]["category_summary"][0]["Latest proof artifact_path"]
        == "interests/testing/results/proof.json"
    )
    assert result["proof_links"][0]["label"] == "Hermes Interests Ingestion Dashboard"
    assert "Personal Time Activity Step 8 proof" in [
        link["label"] for link in result["proof_links"]
    ]
    assert result["git_activity"]["status"] == "ok"
    assert result["git_activity"]["watched_repos"][0]["repo_id"] == "test-repo"
    assert result["git_activity"]["watched_repos"][0]["dirty_count"] == 0
    assert result["git_activity"]["latest_commits"][0]["subject"] == "Initial"
    assert result["source_digest"].startswith("sha256:")

    (repo / "README.md").write_text("hello again\n", encoding="utf-8")
    dirty = asyncio.run(routes_personal.get_imports_dashboard())

    assert dirty["status"] == "needs_review"
    assert dirty["git_activity"]["status"] == "needs_review"
    assert dirty["git_activity"]["watched_repos"][0]["dirty_count"] == 1
    assert dirty["git_activity"]["actionable_repos"][0]["repo_id"] == "test-repo"


def test_diary_day_read_model_hides_pin_events(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)
    day_dir = tmp_path / "2026" / "06" / "18"
    day_dir.mkdir(parents=True)
    (day_dir / "events-index.md").write_text("# index\n", encoding="utf-8")
    (day_dir / "source-ledger.json").write_text(
        json.dumps({"sources": [{"source_type": "manual"}]}), encoding="utf-8"
    )
    (day_dir / "day-manifest.json").write_text(
        json.dumps({"files": [{"path": "events-index.md"}]}), encoding="utf-8"
    )
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, privacy_level, status
        )
        VALUES ('evt-visible', 'manual', 'personal-log', 'Visible', '2026-06-18', 'Europe/London', 'normal', 'open')
        """
    )
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, privacy_level, status
        )
        VALUES ('evt-pin', 'manual', 'personal-log', 'Hidden', '2026-06-18', 'Europe/London', 'pin', 'open')
        """
    )

    result = asyncio.run(routes_personal.get_diary_day(date="2026-06-18"))

    assert result["status"] == "ready"
    assert [item["event_id"] for item in result["events"]] == ["evt-visible"]
    assert result["pin_hidden_count"] == 1
    assert result["files"]["source_ledger"]["source_count"] == 1
    assert result["summary"]["state"] == "summary_pending"


def test_diary_entry_write_projects_audit_and_rehydrates(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)

    created = asyncio.run(
        routes_personal.create_diary_day_entry(
            routes_personal.DiaryEntryCreateRequest(
                body="A focused test entry",
                local_date="2026-06-18",
                local_time="10:20",
                actor="codex-test",
                source_surface="pytest",
                request_id="req-test",
            )
        )
    )

    event = created["event"]
    file_ref = created["write"]["file_ref"]
    entry_path = tmp_path / file_ref
    assert created["ok"] is True
    assert entry_path.exists()
    assert "xarta.diary.personal_log.v1" in entry_path.read_text(encoding="utf-8")
    assert event["source"]["type"] == "manual"
    assert event["file_refs"] == [file_ref]
    assert created["audit"]["actor"] == "codex-test"
    audit_rows = conn.execute("SELECT * FROM personal_time_audit").fetchall()
    assert len(audit_rows) == 1
    assert audit_rows[0]["source_surface"] == "pytest"
    ledger = json.loads((tmp_path / "2026" / "06" / "18" / "source-ledger.json").read_text())
    assert ledger["sources"][0]["event_id"] == event["event_id"]

    conn.execute(
        """
        UPDATE personal_events
        SET content_projection='', body_excerpt='', projection_state='slim'
        WHERE event_id=?
        """,
        (event["event_id"],),
    )
    rehydrated = asyncio.run(
        routes_personal.rehydrate_personal_projection(
            routes_personal.PersonalRehydrateRequest(event_id=event["event_id"])
        )
    )
    assert rehydrated["ok"] is True
    assert rehydrated["rehydrated"] is True
    assert "A focused test entry" in rehydrated["event"]["content_projection"]

    linked = asyncio.run(
        routes_personal.link_personal_event_work_item(
            event["event_id"],
            routes_personal.DiaryWorkLinkRequest(
                work_item_ref="work:test-1",
                actor="codex-test",
                source_surface="pytest",
                request_id="link-test",
            ),
        )
    )
    assert linked["ok"] is True
    assert linked["event"]["related"]["work_items"] == ["work:test-1"]
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='link_work_item'"
        ).fetchone()["count"]
        == 1
    )


def test_diary_summary_generation_writes_file_and_audit(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, kind, title, local_date, timezone,
            privacy_level, status
        )
        VALUES ('evt-summary', 'manual', '2026/06/18/10-20-personal-log.md',
                'personal-log', 'Summary entry', '2026-06-18', 'Europe/London',
                'normal', 'open')
        """
    )

    result = asyncio.run(
        routes_personal.generate_diary_day_summary(
            routes_personal.DiarySummaryGenerateRequest(
                local_date="2026-06-18",
                actor="codex-test",
                source_surface="pytest",
                request_id="summary-test",
            )
        )
    )

    summary_path = tmp_path / result["summary"]["file_ref"]
    assert result["ok"] is True
    assert summary_path.exists()
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "xarta.diary.day_summary.v1" in summary_text
    assert "evt-summary" in summary_text
    assert result["day"]["summary"]["state"] == "ready"
    audit = conn.execute(
        "SELECT * FROM personal_time_audit WHERE action='generate_day_summary'"
    ).fetchone()
    assert audit["actor"] == "codex-test"
