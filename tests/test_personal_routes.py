import asyncio
import importlib.util
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

from app import routes_personal, routes_sync  # noqa: E402


def _minutes_turn_event(
    *,
    created_at: str,
    conversation_key: str = "matrix-bridge:tb1:room=!test:chat.example",
    matrix_event_id: str = "$source-event",
    raw_delivery_body: str = "RAW TRANSCRIPT BODY MUST NOT BE PROJECTED",
) -> dict:
    return {
        "schema": "xarta.hermes.minutes.event.v1",
        "event_kind": "turn_summary",
        "conversation_key": conversation_key,
        "created_at": created_at,
        "created_at_epoch": 1781800000.0,
        "payload": {
            "schema": "xarta.hermes.minutes.summary.v1",
            "conversation_key": conversation_key,
            "time": created_at,
            "route": "matrix_bridge",
            "route_status": "message_received",
            "route_profile": "matrix-bridge-operator",
            "operator_intent_summary": "Asked for Step 12 compact Minutes projection.",
            "assistant_action_summary": "Projected compact Minutes into diary provenance.",
            "result_summary": "Diary can show compact Minutes context without source copies.",
            "open_question": "",
            "entities": [{"name": "Step 12", "kind": "goal_step", "aliases": []}],
            "problems": [],
            "followup_affordances": ["verify_diary_projection"],
            "source_pointers": {
                "source_room_id": "!test:chat.example",
                "matrix_event_ids": [matrix_event_id],
                "tts_utterance_ids": [],
                "wake_route_record_ids": ["wake-route-test"],
            },
            "source_detail_available": True,
            "source_detail_policy": "Use source pointers; do not copy raw source material.",
            "delivery": {
                "server_id": "tb1",
                "room_id": "!test:chat.example",
                "event_id": matrix_event_id,
                "body": raw_delivery_body,
            },
            "confidence": 0.95,
        },
    }


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
        CREATE TABLE personal_time_tasks (
            task_id TEXT PRIMARY KEY,
            source_type TEXT NOT NULL DEFAULT 'manual-task',
            source_ref TEXT,
            source_hash TEXT,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            mode TEXT NOT NULL DEFAULT 'personal',
            priority TEXT,
            due_at TEXT,
            local_date TEXT,
            timezone TEXT,
            privacy_level TEXT NOT NULL DEFAULT 'normal',
            tags_json TEXT NOT NULL DEFAULT '[]',
            related_work_items_json TEXT NOT NULL DEFAULT '[]',
            related_tasks_json TEXT NOT NULL DEFAULT '[]',
            related_import_batches_json TEXT NOT NULL DEFAULT '[]',
            file_refs_json TEXT NOT NULL DEFAULT '[]',
            db_refs_json TEXT NOT NULL DEFAULT '[]',
            event_id TEXT,
            provenance_json TEXT NOT NULL DEFAULT '{}',
            completed_at TEXT,
            archived_at TEXT,
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
        CREATE TABLE personal_search_documents (
            document_id TEXT PRIMARY KEY,
            record_type TEXT NOT NULL DEFAULT '',
            record_table TEXT NOT NULL DEFAULT '',
            record_id TEXT NOT NULL DEFAULT '',
            source_type TEXT NOT NULL DEFAULT '',
            source_ref TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            body TEXT NOT NULL DEFAULT '',
            search_text TEXT NOT NULL DEFAULT '',
            local_date TEXT,
            status TEXT NOT NULL DEFAULT '',
            mode TEXT NOT NULL DEFAULT '',
            privacy_level TEXT NOT NULL DEFAULT 'normal',
            tags_json TEXT NOT NULL DEFAULT '[]',
            related_refs_json TEXT NOT NULL DEFAULT '[]',
            page_ref_json TEXT NOT NULL DEFAULT '{}',
            source_refs_json TEXT NOT NULL DEFAULT '[]',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            score_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            vector_index_status TEXT NOT NULL DEFAULT 'pending',
            vector_index_updated_at TEXT,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE VIRTUAL TABLE personal_search_fts USING fts5(
            document_id UNINDEXED,
            title,
            body,
            search_text,
            tags,
            source_type,
            record_type,
            tokenize='porter unicode61'
        );
        CREATE TABLE personal_graph_links (
            link_id TEXT PRIMARY KEY,
            source_ref TEXT NOT NULL,
            source_table TEXT NOT NULL DEFAULT '',
            source_id TEXT NOT NULL DEFAULT '',
            target_ref TEXT NOT NULL,
            target_table TEXT NOT NULL DEFAULT '',
            target_id TEXT NOT NULL DEFAULT '',
            link_type TEXT NOT NULL DEFAULT 'relates_to',
            link_state TEXT NOT NULL DEFAULT 'declared',
            risk_level TEXT NOT NULL DEFAULT 'normal',
            title TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_by TEXT NOT NULL DEFAULT '',
            request_id TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_item_states (
            state_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            lane_key TEXT NOT NULL,
            status_category TEXT NOT NULL DEFAULT 'open',
            sort_order INTEGER NOT NULL DEFAULT 0,
            is_terminal INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_item_priorities (
            priority_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_items (
            item_id TEXT PRIMARY KEY,
            parent_item_id TEXT,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            item_type TEXT NOT NULL DEFAULT 'work',
            state_id TEXT NOT NULL DEFAULT 'todo',
            priority_id TEXT NOT NULL DEFAULT 'medium',
            depth INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'open',
            archived_at TEXT,
            promoted_from_ref TEXT,
            source_type TEXT NOT NULL DEFAULT 'manual-work',
            source_ref TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            tags_json TEXT NOT NULL DEFAULT '[]',
            related_event_ids_json TEXT NOT NULL DEFAULT '[]',
            related_task_ids_json TEXT NOT NULL DEFAULT '[]',
            related_issue_ids_json TEXT NOT NULL DEFAULT '[]',
            search_text TEXT NOT NULL DEFAULT '',
            search_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_item_links (
            link_id TEXT PRIMARY KEY,
            source_item_id TEXT NOT NULL,
            target_item_id TEXT NOT NULL,
            link_type TEXT NOT NULL DEFAULT 'related',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_issues (
            issue_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            priority_id TEXT NOT NULL DEFAULT 'medium',
            source_ref TEXT NOT NULL DEFAULT '',
            related_task_id TEXT NOT NULL DEFAULT '',
            search_text TEXT NOT NULL DEFAULT '',
            search_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_todos (
            todo_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            priority_id TEXT NOT NULL DEFAULT 'medium',
            due_at TEXT,
            related_task_id TEXT NOT NULL DEFAULT '',
            search_text TEXT NOT NULL DEFAULT '',
            search_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_blockers (
            blocker_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            blocked_by_ref TEXT NOT NULL DEFAULT '',
            search_text TEXT NOT NULL DEFAULT '',
            search_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_discussions (
            discussion_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            author TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            search_text TEXT NOT NULL DEFAULT '',
            search_metadata_json TEXT NOT NULL DEFAULT '{}',
            embedding_ref TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedding_updated_at TEXT,
            vector_index_key TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE work_audit_log (
            audit_id TEXT PRIMARY KEY,
            actor TEXT NOT NULL DEFAULT '',
            source_surface TEXT NOT NULL DEFAULT '',
            action TEXT NOT NULL DEFAULT '',
            target_ref TEXT NOT NULL DEFAULT '',
            item_id TEXT NOT NULL DEFAULT '',
            parent_item_id TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            request_id TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL DEFAULT '',
            result TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        INSERT INTO work_item_states (
            state_id, label, lane_key, status_category, sort_order, is_terminal
        ) VALUES
            ('backlog', 'Backlog', 'backlog', 'open', 10, 0),
            ('todo', 'To Do', 'todo', 'open', 20, 0),
            ('doing', 'Doing', 'doing', 'active', 30, 0),
            ('blocked', 'Blocked', 'blocked', 'blocked', 40, 0),
            ('done', 'Done', 'done', 'done', 50, 1);
        INSERT INTO work_item_priorities (
            priority_id, label, weight, sort_order
        ) VALUES
            ('low', 'Low', 10, 10),
            ('medium', 'Medium', 50, 20),
            ('high', 'High', 80, 30),
            ('critical', 'Critical', 100, 40);
        CREATE TABLE bookmarks (
            bookmark_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            description TEXT NOT NULL DEFAULT '',
            tags_json TEXT NOT NULL DEFAULT '[]',
            folder TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            favicon_url TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'manual',
            archived INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE visits (
            visit_id TEXT PRIMARY KEY,
            url TEXT NOT NULL,
            normalized_url TEXT NOT NULL,
            domain TEXT NOT NULL DEFAULT '',
            title TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT 'visit-recorder',
            dwell_seconds INTEGER,
            bookmark_id TEXT,
            visited_at TEXT NOT NULL DEFAULT (datetime('now')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            visit_count INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE visit_events (
            event_id TEXT PRIMARY KEY,
            normalized_url TEXT NOT NULL,
            visited_at TEXT NOT NULL DEFAULT (datetime('now')),
            dwell_seconds INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
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


def _disable_import_status_sync(monkeypatch) -> None:
    monkeypatch.setattr(
        routes_personal,
        "_sync_personal_import_status_batches",
        lambda conn, now: {"inserted": 0, "updated": 0, "unchanged": 0},
    )


def _load_personal_automation_module():
    script = APP_ROOT / "scripts" / "personal_activity_automation.py"
    spec = importlib.util.spec_from_file_location("personal_activity_automation", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


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


def test_projection_maintenance_trims_hot_cache_and_rehydrates(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)
    source = tmp_path / "2026" / "06" / "18" / "10-20-personal-log.md"
    source.parent.mkdir(parents=True)
    source.write_text("Restored projection body\n", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, body_excerpt, content_projection,
            local_date, timezone, file_refs_json, db_refs_json, projection_state,
            provenance_json, last_rendered_at, projection_expires_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-hot-cache",
            "diary-file",
            "entry",
            "Hot cache entry",
            "Body excerpt survives trim",
            "Bulky cached projection",
            "2026-06-18",
            "Europe/London",
            json.dumps(["2026/06/18/10-20-personal-log.md"]),
            json.dumps(["personal_events:evt-hot-cache"]),
            "hot",
            json.dumps({"projection_file": "2026/06/18/10-20-personal-log.md"}),
            "2026-05-01T10:00:00Z",
            "2026-05-15T10:00:00Z",
        ),
    )

    dry_run = asyncio.run(
        routes_personal.maintain_personal_projections(
            routes_personal.PersonalProjectionMaintenanceRequest(
                retention_days=7,
                dry_run=True,
                now="2026-06-18T12:00:00Z",
            )
        )
    )

    assert dry_run["ok"] is True
    assert dry_run["candidate_count"] == 1
    assert dry_run["trimmed_count"] == 0
    before = conn.execute(
        "SELECT content_projection, projection_state FROM personal_events WHERE event_id='evt-hot-cache'"
    ).fetchone()
    assert before["content_projection"] == "Bulky cached projection"
    assert before["projection_state"] == "hot"

    applied = asyncio.run(
        routes_personal.maintain_personal_projections(
            routes_personal.PersonalProjectionMaintenanceRequest(
                retention_days=7,
                dry_run=False,
                now="2026-06-18T12:00:00Z",
            )
        )
    )

    assert applied["trimmed_count"] == 1
    trimmed = conn.execute(
        """
        SELECT content_projection, body_excerpt, file_refs_json, projection_state, provenance_json
        FROM personal_events
        WHERE event_id='evt-hot-cache'
        """
    ).fetchone()
    assert trimmed["content_projection"] == ""
    assert trimmed["body_excerpt"] == "Body excerpt survives trim"
    assert json.loads(trimmed["file_refs_json"]) == ["2026/06/18/10-20-personal-log.md"]
    assert trimmed["projection_state"] == "needs_rehydrate"
    assert json.loads(trimmed["provenance_json"])["hot_cache_maintenance"][
        "preserved_file_refs"
    ] == ["2026/06/18/10-20-personal-log.md"]

    rehydrated = asyncio.run(
        routes_personal.rehydrate_personal_projection(
            routes_personal.PersonalRehydrateRequest(event_id="evt-hot-cache")
        )
    )

    assert rehydrated["ok"] is True
    assert rehydrated["rehydrated"] is True
    assert rehydrated["event"]["projection_state"] == "hot"
    assert "Restored projection body" in rehydrated["event"]["content_projection"]


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


def test_calendar_event_create_and_edit_use_shared_events(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)

    created = asyncio.run(
        routes_personal.create_calendar_event(
            routes_personal.CalendarEventUpsertRequest(
                title="Dentist",
                body="Bring form",
                local_date="2026-06-18",
                start_time="09:30",
                end_time="10:15",
                timezone="Europe/London",
                actor="codex-test",
                source_surface="pytest",
                request_id="calendar-create-test",
            )
        )
    )

    event = created["event"]
    assert created["ok"] is True
    assert event["source"]["type"] == "manual-calendar"
    assert event["kind"] == "calendar-event"
    assert event["title"] == "Dentist"
    assert event["body_excerpt"] == "Bring form"
    assert event["start_at"] == "2026-06-18T08:30:00Z"
    assert event["end_at"] == "2026-06-18T09:15:00Z"
    assert "calendar" in event["tags"]
    assert "timed" in event["tags"]
    assert event["provenance"]["calendar"]["local_start_time"] == "09:30"

    listed = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-06-18",
            date_end="2026-06-18",
            source_type="manual-calendar",
            limit=10,
            offset=0,
        )
    )
    assert [item["event_id"] for item in listed["items"]] == [event["event_id"]]

    updated = asyncio.run(
        routes_personal.update_calendar_event(
            event["event_id"],
            routes_personal.CalendarEventUpsertRequest(
                title="Dentist moved",
                body="Bring updated form",
                local_date="2026-06-18",
                start_time="11:00",
                end_time="11:30",
                timezone="Europe/London",
                all_day=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="calendar-update-test",
            ),
        )
    )

    assert updated["event"]["event_id"] == event["event_id"]
    assert updated["event"]["title"] == "Dentist moved"
    assert updated["event"]["start_at"] == "2026-06-18T10:00:00Z"
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE source_type='manual-calendar'"
        ).fetchone()["count"]
        == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='create_calendar_event'"
        ).fetchone()["count"]
        == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='update_calendar_event'"
        ).fetchone()["count"]
        == 1
    )
    source = conn.execute(
        "SELECT * FROM personal_sources WHERE source_id='manual-calendar'"
    ).fetchone()
    assert source["status"] == "ok"


def test_calendar_event_rejects_end_before_start(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)

    try:
        asyncio.run(
            routes_personal.create_calendar_event(
                routes_personal.CalendarEventUpsertRequest(
                    title="Bad slot",
                    local_date="2026-06-18",
                    start_time="15:00",
                    end_time="14:00",
                    timezone="Europe/London",
                )
            )
        )
    except routes_personal.HTTPException as exc:
        assert exc.status_code == 400
        assert "end time" in exc.detail
    else:
        raise AssertionError("calendar event with end before start must fail")


def test_personal_task_create_edit_complete_archive_projects_to_events(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)

    created = asyncio.run(
        routes_personal.create_personal_task(
            routes_personal.PersonalTaskUpsertRequest(
                title="Step 15 backend task",
                body="Prove task write path",
                mode="personal",
                due_date="2026-06-18",
                due_time="16:45",
                timezone="Europe/London",
                priority="high",
                tags=["proof"],
                actor="codex-test",
                source_surface="pytest",
                request_id="task-create-test",
            )
        )
    )

    task = created["task"]
    event = created["event"]
    assert created["ok"] is True
    assert task["source"]["type"] == "manual-task"
    assert task["status"] == "open"
    assert task["mode"] == "personal"
    assert task["due_at"] == "2026-06-18T15:45:00Z"
    assert event["kind"] == "task"
    assert event["source"]["ref"] == f"personal_time_tasks:{task['task_id']}"
    assert event["related"]["tasks"][0] == task["task_id"]
    assert "todo" in task["tags"]
    assert "proof" in task["tags"]
    assert (tmp_path / task["file_refs"][0]).exists()
    assert (tmp_path / task["file_refs"][1]).exists()
    assert "xarta.todo.task.v1" in (tmp_path / task["file_refs"][0]).read_text(encoding="utf-8")

    calendar_visible = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-06-18",
            date_end="2026-06-18",
            kind="task",
            limit=20,
            offset=0,
        )
    )
    assert [item["event_id"] for item in calendar_visible["items"]] == [task["event_id"]]

    updated = asyncio.run(
        routes_personal.update_personal_task(
            task["task_id"],
            routes_personal.PersonalTaskUpsertRequest(
                title="Step 15 backend task updated",
                body="Edited task body",
                mode="work",
                due_date="2026-06-18",
                due_time="17:05",
                timezone="Europe/London",
                priority="medium",
                related_work_items=["work:item-1"],
                actor="codex-test",
                source_surface="pytest",
                request_id="task-update-test",
            ),
        )
    )
    assert updated["task"]["title"] == "Step 15 backend task updated"
    assert updated["task"]["mode"] == "work"
    assert updated["task"]["related"]["work_items"] == ["work:item-1"]
    assert updated["event"]["start_at"] == "2026-06-18T16:05:00Z"

    work_list = asyncio.run(routes_personal.list_personal_tasks(mode="work", limit=20, offset=0))
    assert [item["task_id"] for item in work_list["items"]] == [task["task_id"]]

    completed = asyncio.run(
        routes_personal.complete_personal_task(
            task["task_id"],
            routes_personal.PersonalTaskActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="task-complete-test",
            ),
        )
    )
    assert completed["task"]["status"] == "done"
    assert completed["task"]["completed_at"]

    archived = asyncio.run(
        routes_personal.archive_personal_task(
            task["task_id"],
            routes_personal.PersonalTaskActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="task-archive-test",
            ),
        )
    )
    assert archived["task"]["status"] == "archived"
    assert archived["task"]["archived_at"]
    assert (
        conn.execute("SELECT COUNT(*) AS count FROM personal_time_tasks").fetchone()["count"] == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE source_type='manual-task'"
        ).fetchone()["count"]
        == 1
    )
    assert {
        row["action"]
        for row in conn.execute("SELECT action FROM personal_time_audit").fetchall()
        if row["action"].endswith("_task") or row["action"] == "create_task"
    } == {"create_task", "update_task", "complete_task", "archive_task"}


def test_personal_tasks_list_includes_event_sourced_next_actions(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, status,
            tags_json, related_work_items_json, provenance_state
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-reminder",
            "hermes-minutes",
            "reminder",
            "Review projected reminder",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["todo", "review"]),
            json.dumps([]),
            "linked",
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, status,
            tags_json, related_work_items_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-work",
            "work-management",
            "todo",
            "Work source task",
            "2026-06-18",
            "Europe/London",
            "blocked",
            json.dumps(["todo", "work"]),
            json.dumps(["work:item-9"]),
        ),
    )

    personal = asyncio.run(routes_personal.list_personal_tasks(mode="personal", limit=20, offset=0))
    work = asyncio.run(routes_personal.list_personal_tasks(mode="work", limit=20, offset=0))
    blocked = asyncio.run(routes_personal.list_personal_tasks(mode="blocked", limit=20, offset=0))

    assert [item["task_id"] for item in personal["items"]] == ["evt-reminder"]
    assert personal["items"][0]["source"]["authority"] == "event"
    assert [item["task_id"] for item in work["items"]] == ["evt-work"]
    assert [item["task_id"] for item in blocked["items"]] == ["evt-work"]


def test_personal_search_sync_exact_fts_and_filters(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _disable_import_status_sync(monkeypatch)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title,
            body_excerpt, content_projection, local_date, timezone, status,
            tags_json, related_work_items_json, provenance_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-diary-search",
            "manual",
            "2026/06/18/10-20-personal-log.md",
            "sha256:diary-search",
            "personal-log",
            "Morning source moment",
            "Needle phrase visible in a diary body",
            "Needle phrase visible in a diary body",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["diary", "proof"]),
            json.dumps(["work-search"]),
            json.dumps({"file_ref": "2026/06/18/10-20-personal-log.md"}),
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_import_batches (
            import_batch_id, source_type, source_ref, title, status, local_date,
            artifact_refs_json, blocker_refs_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "batch-search",
            "interests-ingestion",
            "docs/interests/source.json",
            "Interests import alpha",
            "pending_review",
            "2026-06-18",
            json.dumps(["docs/interests/dashboard.md"]),
            json.dumps(["missing source"]),
        ),
    )

    sync = asyncio.run(
        routes_personal.sync_personal_search(
            routes_personal.PersonalSearchSyncRequest(include_embeddings=False)
        )
    )
    assert sync["documents"]["document_count"] == 2
    assert sync["vector"]["status"] == "skipped"

    needle = asyncio.run(
        routes_personal.search_personal_activity(
            q="Needle",
            date_start="2026-06-18",
            date_end="2026-06-18",
            include_vector=False,
            rerank_results=False,
            sync=False,
            limit=10,
        )
    )
    assert needle["subsystems"]["fts"]["candidate_count"] >= 1
    assert needle["results"][0]["document_id"] == "personal_events:evt-diary-search"
    assert {"exact", "fts_bm25"}.issubset(set(needle["results"][0]["score"]["score_sources"]))

    imports = asyncio.run(
        routes_personal.search_personal_activity(
            q="source",
            mode="imports",
            include_vector=False,
            rerank_results=False,
            sync=False,
            limit=10,
        )
    )
    assert [item["record_type"] for item in imports["results"]] == ["import"]
    assert imports["results"][0]["page_ref"]["tab"] == "imports"


def test_personal_search_sync_projects_import_status_rows(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)

    monkeypatch.setattr(
        routes_personal,
        "_parse_interests_dashboard",
        lambda: {
            "status": "ok",
            "doc_path": "docs/interests/dashboard.md",
            "source_digest": "sha256:interests",
            "snapshot_at": "2026-06-18T10:00:00Z",
            "pending_review": 2,
            "actionable_backlog": 1,
            "blockers": [],
            "proof_links": [
                {
                    "label": "Interests proof",
                    "path": "docs/interests/proof.md",
                }
            ],
        },
    )

    def fake_git_activity(counts: dict):
        assert "import_batches" in counts
        return {
            "status": "needs_review",
            "watched_repos": [
                {
                    "repo_id": "p300",
                    "path": "/xarta-node",
                    "head": "abc123",
                    "dirty_count": 1,
                    "untracked_count": 0,
                    "error": "",
                    "daily_commit_count": 2,
                }
            ],
            "latest_commits": [
                {
                    "repo_id": "p300",
                    "sha": "abcdef",
                    "author_date": "2026-06-18T10:00:00Z",
                    "subject": "Import dashboard proof",
                }
            ],
            "errors": [],
            "actionable_repos": [{"repo_id": "p300", "actions": ["review uncommitted changes"]}],
        }

    monkeypatch.setattr(routes_personal, "_git_activity_dashboard", fake_git_activity)

    sync = asyncio.run(
        routes_personal.sync_personal_search(
            routes_personal.PersonalSearchSyncRequest(include_embeddings=False)
        )
    )

    assert sync["documents"]["import_status"] == {
        "inserted": 2,
        "updated": 0,
        "unchanged": 0,
    }
    assert sync["documents"]["document_count"] == 2
    interests = conn.execute(
        "SELECT * FROM personal_import_batches WHERE import_batch_id='status-interests-ingestion'"
    ).fetchone()
    assert interests["source_type"] == "interests-ingestion"
    assert interests["status"] == "ok"

    result = asyncio.run(
        routes_personal.search_personal_activity(
            q="Hermes Interests",
            mode="imports",
            include_vector=False,
            rerank_results=False,
            sync=False,
            limit=10,
        )
    )
    assert result["count"] == 1
    assert result["results"][0]["record_type"] == "import"
    assert result["results"][0]["record_id"] == "status-interests-ingestion"

    second_sync = asyncio.run(
        routes_personal.sync_personal_search(
            routes_personal.PersonalSearchSyncRequest(include_embeddings=False)
        )
    )
    assert second_sync["documents"]["import_status"] == {
        "inserted": 0,
        "updated": 0,
        "unchanged": 2,
    }


def test_personal_search_vector_sync_does_not_hold_sqlite_across_await(monkeypatch):
    conn = _make_conn()
    active_contexts = 0

    @contextmanager
    def tracked_conn():
        nonlocal active_contexts
        active_contexts += 1
        try:
            yield conn
            conn.commit()
        finally:
            active_contexts -= 1

    monkeypatch.setattr(routes_personal, "get_conn", lambda: tracked_conn())
    _disable_import_status_sync(monkeypatch)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title,
            body_excerpt, content_projection, local_date, timezone, status, tags_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-vector-sync",
            "manual",
            "2026/06/18/vector.md",
            "sha256:vector-sync",
            "personal-log",
            "Vector sync proof",
            "Async vector indexing should not hold SQLite open",
            "Async vector indexing should not hold SQLite open",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["diary"]),
        ),
    )

    from app import ai_client, seekdb

    async def fake_embed(project_name: str, texts: list[str]):
        assert project_name == "personal-time-activity"
        assert active_contexts == 0
        return [[0.11] * routes_personal.PERSONAL_SEARCH_VECTOR_DIM for _ in texts]

    indexed = []

    async def fake_upsert(row: dict, vector: list[float]):
        assert active_contexts == 0
        assert len(vector) == routes_personal.PERSONAL_SEARCH_VECTOR_DIM
        indexed.append(row["document_id"])

    monkeypatch.setattr(
        ai_client, "_get_provider", lambda project, role: {"model_name": "test-emb"}
    )
    monkeypatch.setattr(ai_client, "embed", fake_embed)
    monkeypatch.setattr(seekdb, "upsert_personal_index_async", fake_upsert)

    sync = asyncio.run(
        routes_personal.sync_personal_search(
            routes_personal.PersonalSearchSyncRequest(include_embeddings=True)
        )
    )

    assert sync["vector"]["status"] == "ok"
    assert sync["vector"]["indexed"] == 1
    assert indexed == ["personal_events:evt-vector-sync"]
    row = conn.execute(
        "SELECT embedding_model, vector_index_status FROM personal_search_documents "
        "WHERE document_id='personal_events:evt-vector-sync'"
    ).fetchone()
    assert row["embedding_model"] == "test-emb"
    assert row["vector_index_status"] == "indexed"


def test_personal_search_vector_only_candidate_and_reranker(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _disable_import_status_sync(monkeypatch)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title,
            body_excerpt, content_projection, local_date, timezone, status, tags_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-literal",
            "manual",
            "2026/06/18/literal.md",
            "sha256:literal",
            "personal-log",
            "semantic query literal",
            "literal keyword candidate",
            "literal keyword candidate",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["diary"]),
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_import_batches (
            import_batch_id, source_type, source_ref, title, status, local_date,
            artifact_refs_json, blocker_refs_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "batch-semantic",
            "interests-ingestion",
            "docs/interests/conceptual.json",
            "Conceptual import row",
            "pending_review",
            "2026-06-18",
            json.dumps(["docs/interests/conceptual-dashboard.md"]),
            json.dumps([]),
        ),
    )
    asyncio.run(
        routes_personal.sync_personal_search(
            routes_personal.PersonalSearchSyncRequest(include_embeddings=False)
        )
    )

    async def fake_vector_candidates(q: str, *, limit: int):
        return (
            [
                {
                    "id": "personal_import_batches:batch-semantic",
                    "metadata": {"document_id": "personal_import_batches:batch-semantic"},
                    "distance": 0.18,
                }
            ],
            {"status": "ok", "error": "", "candidate_count": 1},
        )

    from app import ai_client

    async def fake_rerank(
        project_name: str, query: str, documents: list[str], top_n: int | None = None
    ):
        assert project_name == "personal-time-activity"
        assert query == "semantic query"
        assert len(documents) == 2
        return [
            {"index": 1, "relevance_score": 0.91, "document": {"text": documents[1]}},
            {"index": 0, "relevance_score": 0.42, "document": {"text": documents[0]}},
        ]

    monkeypatch.setattr(routes_personal, "_personal_vector_candidates", fake_vector_candidates)
    monkeypatch.setattr(ai_client, "rerank", fake_rerank)

    result = asyncio.run(
        routes_personal.search_personal_activity(
            q="semantic query",
            include_vector=True,
            rerank_results=True,
            sync=False,
            limit=10,
        )
    )
    assert result["subsystems"]["vector"]["status"] == "ok"
    assert result["subsystems"]["rerank"]["status"] == "ok"
    assert result["results"][0]["document_id"] == "personal_import_batches:batch-semantic"
    assert result["results"][0]["score"]["score_sources"] == ["vector"]
    assert result["results"][0]["score"]["reranker_rank"] == 1
    assert result["results"][0]["score"]["components"]["vector"]["cosine_distance"] == 0.18


def test_personal_graph_sync_projects_explicit_provenance_links(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title,
            body_excerpt, content_projection, local_date, timezone, status,
            tags_json, related_work_items_json, related_tasks_json,
            related_import_batches_json, file_refs_json, db_refs_json,
            provenance_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-graph",
            "git",
            "abc123def456",
            "sha256:event-graph",
            "personal-log",
            "Graph source moment",
            "Graph source moment body",
            "Graph source moment body",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["diary", "graph"]),
            json.dumps(["work:work-graph"]),
            json.dumps(["task:task-graph"]),
            json.dumps(["import:batch-graph"]),
            json.dumps(["browser_link:visit-graph"]),
            json.dumps(["manual_links:manual-graph"]),
            json.dumps(
                {
                    "source_pointers": {
                        "conversation_key": "matrix-bridge:tb1:room=!test:chat.example",
                        "matrix_event_ids": ["$matrix-graph"],
                    }
                }
            ),
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_time_tasks (
            task_id, source_type, title, local_date, timezone, status,
            related_work_items_json, event_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "task-graph",
            "manual-task",
            "Graph task",
            "2026-06-18",
            "Europe/London",
            "open",
            json.dumps(["work:work-graph"]),
            "evt-graph",
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_import_batches (
            import_batch_id, source_type, source_ref, title, status, local_date,
            artifact_refs_json, provenance_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "batch-graph",
            "interests-ingestion",
            "docs/interests/source.json",
            "Graph import",
            "pending_review",
            "2026-06-18",
            json.dumps(["docs/interests/artifact.json"]),
            json.dumps(
                {
                    "proof_links": [
                        {
                            "label": "Import proof",
                            "path": "docs/personal/time-activity-goal/import-proof.md",
                        }
                    ]
                }
            ),
        ),
    )
    conn.execute(
        """
        INSERT INTO work_items (
            item_id, title, state_id, status, promoted_from_ref,
            related_event_ids_json, related_task_ids_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "work-graph",
            "Graph work item",
            "todo",
            "open",
            "personal_events:evt-graph",
            json.dumps(["evt-graph"]),
            json.dumps(["task-graph"]),
        ),
    )
    conn.execute(
        """
        INSERT INTO work_items (item_id, title, state_id, status)
        VALUES (?, ?, ?, ?)
        """,
        ("work-other", "Graph dependency", "todo", "open"),
    )
    conn.execute(
        """
        INSERT INTO work_item_links (link_id, source_item_id, target_item_id, link_type)
        VALUES (?, ?, ?, ?)
        """,
        ("wil-graph", "work-graph", "work-other", "depends_on"),
    )
    conn.execute(
        """
        INSERT INTO work_issues (
            issue_id, item_id, title, status, source_ref, related_task_id
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            "issue-graph",
            "work-graph",
            "Graph issue",
            "open",
            "github:issue-123",
            "task-graph",
        ),
    )
    conn.execute(
        """
        INSERT INTO work_blockers (
            blocker_id, item_id, title, status, blocked_by_ref
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            "blocker-graph",
            "work-graph",
            "Graph blocker",
            "open",
            "personal_events:evt-graph",
        ),
    )

    sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="graph-sync-test",
            )
        )
    )

    assert sync["ok"] is True
    assert sync["candidate_count"] >= 14
    assert sync["links"]["inserted"] == sync["candidate_count"]

    event_links = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref="personal_events:evt-graph",
            sync=False,
            limit=80,
        )
    )
    event_targets = {
        (link["link_type"], link["target_ref"], link["link_state"]) for link in event_links["links"]
    }
    assert ("source_for", "git_commit:abc123def456", "accepted") in event_targets
    assert ("source_for", "browser_links:visit-graph", "accepted") in event_targets
    assert ("evidence_for", "manual_links:manual-graph", "accepted") in event_targets
    assert ("relates_to", "work_items:work-graph", "accepted") in event_targets
    assert ("relates_to", "personal_time_tasks:task-graph", "accepted") in event_targets
    assert ("created_from", "personal_import_batches:batch-graph", "accepted") in event_targets
    assert ("same_day_as", "diary_day:2026-06-18", "accepted") in event_targets
    assert ("source_for", "matrix_event:$matrix-graph", "accepted") in event_targets
    assert (
        "source_for",
        "matrix_minutes:matrix-bridge:tb1:room=!test:chat.example",
        "accepted",
    ) in event_targets
    git_link = next(
        link for link in event_links["links"] if link["target_ref"] == "git_commit:abc123def456"
    )
    assert git_link["provenance"]["source_hash"] == "sha256:event-graph"
    assert git_link["provenance"]["provenance_state"] == "linked"

    work_links = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref="work_items:work-graph",
            sync=False,
            limit=80,
        )
    )
    work_targets = {
        (link["link_type"], link["target_ref"], link["link_state"]) for link in work_links["links"]
    }
    assert ("evidence_for", "personal_events:evt-graph", "accepted") in work_targets
    assert ("evidence_for", "personal_time_tasks:task-graph", "accepted") in work_targets
    assert ("promoted_from", "personal_events:evt-graph", "accepted") in work_targets
    assert ("depends_on", "work_items:work-other", "accepted") in work_targets

    import_links = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref="personal_import_batches:batch-graph",
            sync=False,
            limit=80,
        )
    )
    import_targets = {(link["link_type"], link["target_ref"]) for link in import_links["links"]}
    assert ("evidence_for", "files:docs/interests/artifact.json") in import_targets
    assert (
        "documents",
        "docs:docs/personal/time-activity-goal/import-proof.md",
    ) in import_targets

    second_sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="graph-sync-test",
            )
        )
    )
    assert second_sync["links"]["unchanged"] == sync["candidate_count"]


def test_personal_graph_sync_default_request_id_is_stable(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title,
            body_excerpt, content_projection, local_date, timezone, status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-default-sync",
            "git",
            "feedface1234",
            "sha256:default-sync",
            "personal-log",
            "Default graph sync",
            "Default graph sync body",
            "Default graph sync body",
            "2026-06-18",
            "Europe/London",
            "open",
        ),
    )

    first_sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(actor="codex-test")
        )
    )
    second_sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(actor="codex-test")
        )
    )

    assert first_sync["ok"] is True
    assert first_sync["candidate_count"] >= 2
    assert first_sync["links"]["inserted"] == first_sync["candidate_count"]
    assert second_sync["links"]["unchanged"] == first_sync["candidate_count"]
    assert second_sync["links"]["updated"] == 0

    listed = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref="personal_events:evt-default-sync",
            sync=False,
            limit=20,
        )
    )
    assert {link["request_id"] for link in listed["links"]} == {"personal-graph-sync"}


def test_personal_graph_declared_link_keeps_inferred_under_review(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    created = asyncio.run(
        routes_personal.create_personal_graph_link(
            routes_personal.PersonalGraphLinkCreateRequest(
                source_ref="personal_events:evt-declared",
                target_ref="docs:docs/personal/proof.md",
                link_type="documents",
                link_state="inferred",
                risk_level="review",
                title="Declared proof link",
                metadata={"origin": "pytest"},
                provenance={"evidence": "explicit operator action"},
                actor="codex-test",
                source_surface="pytest",
                request_id="graph-create-test",
            )
        )
    )

    assert created["result"] == "inserted"
    assert created["link"]["link_state"] == "needs_review"
    assert created["link"]["risk_level"] == "review"
    assert created["link"]["provenance"]["declared_by"] == "codex-test"
    assert "inferred input" in created["link"]["provenance"]["guard"]
    assert conn.execute("SELECT COUNT(*) AS count FROM sync_queue").fetchone()["count"] == 1


def test_work_kanban_schema_api_depth_audit_sync_and_promote(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    config = asyncio.run(routes_personal.get_work_config())
    assert config["depth_limit"] == 12
    assert [state["state_id"] for state in config["states"]] == [
        "backlog",
        "todo",
        "doing",
        "blocked",
        "done",
    ]
    assert "work_items" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("work_items") == "item_id"

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-root",
                title="Step 16 root board item",
                body="Root board proof",
                state_id="todo",
                priority_id="high",
                tags=["proof"],
                actor="codex-test",
                source_surface="pytest",
                request_id="work-root-create",
            )
        )
    )
    root = created["item"]
    assert root["item_id"] == "work-root"
    assert root["depth"] == 0
    assert root["state_id"] == "todo"
    assert root["search"]["metadata"]["vector"]["turbo_vec_ready"] is True
    assert root["vector"]["index_key"] == "work_items:work-root"

    board = asyncio.run(routes_personal.get_work_root_board())
    todo_column = next(
        column for column in board["board"]["columns"] if column["state"]["state_id"] == "todo"
    )
    assert [item["item_id"] for item in todo_column["items"]] == ["work-root"]

    child = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-child",
                parent_item_id="work-root",
                title="Step 16 child card",
                body="Child board proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="work-child-create",
            )
        )
    )["item"]
    assert child["depth"] == 1

    parent_id = "work-child"
    for depth in range(2, 13):
        item = asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=f"work-depth-{depth}",
                    parent_item_id=parent_id,
                    title=f"Depth {depth}",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id=f"work-depth-{depth}",
                )
            )
        )["item"]
        assert item["depth"] == depth
        parent_id = item["item_id"]

    try:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id="work-depth-too-far",
                    parent_item_id=parent_id,
                    title="Depth too far",
                )
            )
        )
    except routes_personal.HTTPException as exc:
        assert exc.status_code == 400
        assert "depth" in exc.detail
    else:
        raise AssertionError("work item depth guard must reject depth 13")

    try:
        asyncio.run(
            routes_personal.move_work_item(
                "work-root",
                routes_personal.WorkItemMoveRequest(parent_item_id="work-depth-12"),
            )
        )
    except routes_personal.HTTPException as exc:
        assert exc.status_code == 400
        assert "descendant" in exc.detail
    else:
        raise AssertionError("work item cycle guard must reject moving under a descendant")

    moved = asyncio.run(
        routes_personal.move_work_item(
            "work-child",
            routes_personal.WorkItemMoveRequest(
                parent_item_id=None,
                state_id="doing",
                sort_order=4,
                actor="codex-test",
                source_surface="pytest",
                request_id="work-child-move",
            ),
        )
    )["item"]
    assert moved["parent_item_id"] is None
    assert moved["state_id"] == "doing"
    assert moved["status"] == "active"
    assert moved["depth"] == 0

    issue = asyncio.run(
        routes_personal.create_work_issue(
            routes_personal.WorkIssueUpsertRequest(
                issue_id="issue-step16",
                item_id="work-root",
                title="Step 16 issue",
                body="Issue proof",
                severity_id="high",
                source_ref="docs:step-16",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-create",
            )
        )
    )["issue"]
    assert issue["vector"]["index_key"] == "work_issues:issue-step16"
    assert issue["severity_id"] == "high"

    child_issue = asyncio.run(
        routes_personal.create_work_issue(
            routes_personal.WorkIssueUpsertRequest(
                issue_id="issue-step19-child",
                item_id="work-depth-2",
                title="Step 19 child issue",
                body="Scoped issue proof",
                severity_id="critical",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-step19-child-create",
            )
        )
    )["issue"]
    assert child_issue["severity_id"] == "critical"

    grandchild_issue = asyncio.run(
        routes_personal.create_work_issue(
            routes_personal.WorkIssueUpsertRequest(
                issue_id="issue-step19-grandchild",
                item_id="work-depth-3",
                title="Step 19 grandchild issue",
                body="Two-level scoped issue proof",
                severity_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-step19-grandchild-create",
            )
        )
    )["issue"]
    assert grandchild_issue["item_id"] == "work-depth-3"

    todo = asyncio.run(
        routes_personal.create_work_todo(
            routes_personal.WorkTodoUpsertRequest(
                todo_id="todo-step16",
                item_id="work-root",
                title="Step 16 todo",
                body="Todo proof",
                priority_id="medium",
                related_task_id="task-step16",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-create",
            )
        )
    )["todo"]
    assert todo["related_task_id"] == "task-step16"

    scoped_todo = asyncio.run(
        routes_personal.create_work_todo(
            routes_personal.WorkTodoUpsertRequest(
                todo_id="todo-step19-grandchild",
                item_id="work-depth-3",
                title="Step 19 grandchild todo",
                body="Two-level scoped todo proof",
                priority_id="high",
                related_task_id="task-step19",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-step19-grandchild-create",
            )
        )
    )["todo"]
    assert scoped_todo["item_id"] == "work-depth-3"
    scoped_todo = asyncio.run(
        routes_personal.update_work_todo(
            "todo-step19-grandchild",
            routes_personal.WorkTodoUpsertRequest(
                item_id="work-depth-3",
                title="Step 19 grandchild todo",
                body="Two-level scoped todo proof updated",
                status="active",
                priority_id="critical",
                related_task_id="task-step19",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-step19-grandchild-update",
            ),
        )
    )["todo"]
    assert scoped_todo["status"] == "active"
    assert scoped_todo["priority_id"] == "critical"

    promoted = asyncio.run(
        routes_personal.promote_work_item(
            routes_personal.WorkPromoteRequest(
                source_ref="work_todos:todo-step16",
                title="Promoted Step 16 todo",
                parent_item_id="work-root",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-promote",
            )
        )
    )["item"]
    assert promoted["promoted_from_ref"] == "work_todos:todo-step16"
    assert promoted["related"]["tasks"] == ["task-step16"]
    assert (
        conn.execute("SELECT status FROM work_todos WHERE todo_id='todo-step16'").fetchone()[0]
        == "promoted"
    )

    promoted_issue = asyncio.run(
        routes_personal.promote_work_item(
            routes_personal.WorkPromoteRequest(
                source_ref="work_issues:issue-step19-child",
                title="Promoted Step 19 issue",
                parent_item_id="work-depth-2",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-promote",
            )
        )
    )["item"]
    assert promoted_issue["promoted_from_ref"] == "work_issues:issue-step19-child"
    assert promoted_issue["related"]["issues"] == ["issue-step19-child"]
    assert (
        conn.execute(
            "SELECT status FROM work_issues WHERE issue_id='issue-step19-child'"
        ).fetchone()[0]
        == "promoted"
    )

    local_issues = asyncio.run(
        routes_personal.list_work_item_issues("work-root", scope="local", view="flat")
    )
    assert [row["issue_id"] for row in local_issues["items"]] == ["issue-step16"]
    assert local_issues["counts"]["descendant_items"] == 0

    descendant_issues = asyncio.run(
        routes_personal.list_work_item_issues("work-child", scope="descendants", view="grouped")
    )
    descendant_issue_ids = {row["issue_id"] for row in descendant_issues["items"]}
    assert {"issue-step19-child", "issue-step19-grandchild"}.issubset(descendant_issue_ids)
    assert descendant_issues["counts"]["descendant_items"] >= 2
    assert {group["scope"]["depth_offset"] for group in descendant_issues["groups"]}.issuperset(
        {1, 2}
    )

    descendant_todos = asyncio.run(
        routes_personal.list_work_item_todos("work-child", scope="descendants", view="tree")
    )
    assert [row["todo_id"] for row in descendant_todos["items"]] == ["todo-step19-grandchild"]
    assert descendant_todos["groups"][0]["scope"]["relation"] == "self"

    work_tasks = asyncio.run(routes_personal.list_personal_tasks(mode="work", limit=200))
    work_task_refs = {
        item["source"]["ref"]
        for item in work_tasks["items"]
        if item["source"]["type"] == "work-todo"
    }
    assert {"work_todos:todo-step16", "work_todos:todo-step19-grandchild"}.issubset(work_task_refs)

    child_board = asyncio.run(routes_personal.get_work_child_board("work-root"))
    assert child_board["board"]["parent"]["item_id"] == "work-root"
    assert [item["item_id"] for item in child_board["board"]["breadcrumbs"]] == ["work-root"]
    assert child_board["board"]["remaining_depth"] == 12

    link = asyncio.run(
        routes_personal.create_work_item_link(
            "work-root",
            routes_personal.WorkItemLinkCreateRequest(
                target_item_id=promoted["item_id"],
                link_type="related",
                metadata={"proof_step": 18},
                actor="codex-test",
                source_surface="pytest",
                request_id="link-create",
            ),
        )
    )["link"]
    assert link["source_item_id"] == "work-root"
    assert link["target_item_id"] == promoted["item_id"]
    assert link["metadata"]["proof_step"] == 18

    blocker = asyncio.run(
        routes_personal.create_work_blocker(
            routes_personal.WorkBlockerUpsertRequest(
                blocker_id="blocker-step18",
                item_id="work-root",
                title="Step 18 blocker",
                body="Blocker proof",
                blocked_by_ref=f"work_items:{promoted['item_id']}",
                actor="codex-test",
                source_surface="pytest",
                request_id="blocker-create",
            )
        )
    )["blocker"]
    assert blocker["vector"]["index_key"] == "work_blockers:blocker-step18"
    assert blocker["blocked_by_ref"] == f"work_items:{promoted['item_id']}"

    detail = asyncio.run(routes_personal.get_work_item_detail("work-root"))
    assert detail["rollup"]["items"]["total"] >= 2
    assert [item["item_id"] for item in detail["breadcrumbs"]] == ["work-root"]
    assert detail["remaining_depth"] == 12
    assert detail["issues"][0]["issue_id"] == "issue-step16"
    assert detail["todos"][0]["todo_id"] == "todo-step16"
    assert detail["links"][0]["link_id"] == link["link_id"]
    assert detail["blockers"][0]["blocker_id"] == "blocker-step18"
    assert detail["counts"]["links"] == 1
    assert detail["counts"]["blockers"] == 1

    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM work_audit_log").fetchall()
    }
    assert {
        "create_work_item",
        "move_work_item",
        "create_work_issue",
        "create_work_todo",
        "promote_work_item",
        "create_work_item_link",
        "create_work_blocker",
    }.issubset(audit_actions)
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {
        "work_items",
        "work_item_links",
        "work_issues",
        "work_todos",
        "work_blockers",
        "work_audit_log",
    }.issubset(sync_tables)


def test_minutes_projection_writes_compact_day_file_events_and_ledger(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path / "diary")
    minutes_file = tmp_path / "minutes" / "recent.jsonl"
    minutes_file.parent.mkdir(parents=True)
    raw_body = "RAW TRANSCRIPT BODY MUST NOT BE PROJECTED"
    events = [
        _minutes_turn_event(
            created_at="2026-06-18T10:15:00Z",
            matrix_event_id="$minutes-source-1",
            raw_delivery_body=raw_body,
        ),
        _minutes_turn_event(
            created_at="2026-06-17T10:15:00Z",
            matrix_event_id="$minutes-source-previous-day",
            raw_delivery_body="PREVIOUS DAY RAW BODY",
        ),
    ]
    minutes_file.write_text(
        "\n".join(json.dumps(event, sort_keys=True) for event in events) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_MINUTES_LOCAL_INDEX_PATH", str(minutes_file))
    monkeypatch.setenv("HERMES_MINUTES_CONFIG_FILE", str(tmp_path / "missing-config.json"))

    result = asyncio.run(
        routes_personal.project_diary_day_minutes(
            routes_personal.DiaryMinutesProjectRequest(
                local_date="2026-06-18",
                ttl_seconds=10**12,
                actor="codex-test",
                source_surface="pytest",
                request_id="minutes-test",
            )
        )
    )

    projection_path = tmp_path / "diary" / result["projection"]["file_ref"]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    projection_text = projection_path.read_text(encoding="utf-8")
    assert result["ok"] is True
    assert result["source_available"] is True
    assert result["status"] == "ok"
    assert projection["schema"] == "xarta.diary.hermes_minutes_projection.v1"
    assert projection["entry_count"] == 1
    assert projection["entries"][0]["source_pointers"]["matrix_event_ids"] == ["$minutes-source-1"]
    assert projection["entries"][0]["source_support"]["matrix_source_pointer"] == "supported"
    assert raw_body not in projection_text
    assert "PREVIOUS DAY RAW BODY" not in projection_text

    event_rows = conn.execute(
        "SELECT * FROM personal_events WHERE source_type='hermes-minutes'"
    ).fetchall()
    assert len(event_rows) == 1
    assert event_rows[0]["status"] == "open"
    assert event_rows[0]["kind"] == "hermes-minutes"
    assert raw_body not in event_rows[0]["content_projection"]

    ledger = json.loads(
        (tmp_path / "diary" / "2026" / "06" / "18" / "source-ledger.json").read_text(
            encoding="utf-8"
        )
    )
    minutes_ledger = [
        item
        for item in ledger["sources"]
        if str(item.get("ledger_entry_id", "")).startswith("hermes-minutes:")
    ]
    assert len(minutes_ledger) == 1
    assert minutes_ledger[0]["matrix_event_ids"] == ["$minutes-source-1"]
    manifest = json.loads(
        (tmp_path / "diary" / "2026" / "06" / "18" / "day-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert "hermes-minutes.json" in {item["path"] for item in manifest["files"]}

    rerun = asyncio.run(
        routes_personal.project_diary_day_minutes(
            routes_personal.DiaryMinutesProjectRequest(
                local_date="2026-06-18",
                ttl_seconds=10**12,
                actor="codex-test",
                source_surface="pytest",
                request_id="minutes-test-rerun",
            )
        )
    )
    assert rerun["projection"]["skipped_existing_event_count"] == 1
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE source_type='hermes-minutes'"
        ).fetchone()["count"]
        == 1
    )
    rerun_ledger = json.loads(
        (tmp_path / "diary" / "2026" / "06" / "18" / "source-ledger.json").read_text(
            encoding="utf-8"
        )
    )
    assert (
        len(
            [
                item
                for item in rerun_ledger["sources"]
                if str(item.get("ledger_entry_id", "")).startswith("hermes-minutes:")
            ]
        )
        == 1
    )


def test_minutes_projection_records_source_unavailable_status(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path / "diary")
    missing_minutes = tmp_path / "minutes" / "missing.jsonl"
    monkeypatch.setenv("HERMES_MINUTES_LOCAL_INDEX_PATH", str(missing_minutes))
    monkeypatch.setenv("HERMES_MINUTES_CONFIG_FILE", str(tmp_path / "missing-config.json"))

    result = asyncio.run(
        routes_personal.project_diary_day_minutes(
            routes_personal.DiaryMinutesProjectRequest(
                local_date="2026-06-18",
                actor="codex-test",
                source_surface="pytest",
                request_id="minutes-missing-test",
            )
        )
    )

    assert result["ok"] is True
    assert result["source_available"] is False
    assert result["status"] == "source_unavailable"
    projection_path = tmp_path / "diary" / result["projection"]["file_ref"]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["status"] == "source_unavailable"
    assert projection["entries"] == []
    event = conn.execute(
        "SELECT * FROM personal_events WHERE source_type='hermes-minutes'"
    ).fetchone()
    assert event["event_id"] == "minutes-2026-06-18-source-unavailable"
    assert event["status"] == "source_unavailable"
    source = conn.execute(
        "SELECT * FROM personal_sources WHERE source_id='hermes-minutes'"
    ).fetchone()
    assert source["status"] == "source_unavailable"


def _insert_browser_link_fixture_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO visits (
            visit_id, url, normalized_url, domain, title, source, dwell_seconds,
            bookmark_id, visited_at, visit_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "visit-1",
            "https://example.test/page",
            "https://example.test/page",
            "example.test",
            "Example Page",
            "visit-recorder",
            30,
            "bookmark-today",
            "2026-06-18T10:05:00+00:00",
            2,
        ),
    )
    conn.execute(
        """
        INSERT INTO visits (
            visit_id, url, normalized_url, domain, title, source, dwell_seconds,
            bookmark_id, visited_at, visit_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "visit-2",
            "https://docs.example.test/reference",
            "https://docs.example.test/reference",
            "docs.example.test",
            "Docs Reference",
            "visit-recorder",
            45,
            None,
            "2026-06-18T11:20:00+00:00",
            1,
        ),
    )
    conn.execute(
        """
        INSERT INTO visits (
            visit_id, url, normalized_url, domain, title, source, visited_at, visit_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "visit-old",
            "https://old.example.test/history",
            "https://old.example.test/history",
            "old.example.test",
            "Old History",
            "visit-recorder",
            "2026-06-17T08:00:00+00:00",
            1,
        ),
    )
    conn.executemany(
        """
        INSERT INTO visit_events (event_id, normalized_url, visited_at, dwell_seconds)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("ve-1", "https://example.test/page", "2026-06-18T10:05:00+00:00", 30),
            ("ve-2", "https://example.test/page", "2026-06-18T10:30:00+00:00", 15),
            ("ve-3", "https://docs.example.test/reference", "2026-06-18T11:20:00+00:00", 45),
            ("ve-old", "https://old.example.test/history", "2026-06-17T08:00:00+00:00", 5),
        ],
    )
    conn.executemany(
        """
        INSERT INTO bookmarks (
            bookmark_id, url, normalized_url, title, description, tags_json,
            folder, notes, source, archived, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "bookmark-today",
                "https://example.test/page",
                "https://example.test/page",
                "Example Page",
                "description must stay out of browser projection",
                json.dumps(["docs", "daily"]),
                "research",
                "notes must stay out of browser projection",
                "manual",
                0,
                "2026-06-18T10:00:00+00:00",
                "2026-06-18T10:01:00+00:00",
            ),
            (
                "bookmark-old",
                "https://old.example.test/history",
                "https://old.example.test/history",
                "Old History",
                "old description",
                json.dumps(["old"]),
                "archive",
                "old notes",
                "manual",
                0,
                "2026-06-17T09:00:00+00:00",
                "2026-06-17T09:01:00+00:00",
            ),
        ],
    )


def test_browser_links_projection_writes_day_file_events_ledger_and_initiation(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path / "diary")
    _insert_browser_link_fixture_rows(conn)

    async def ok_health(sqlite_health):
        return {
            **sqlite_health,
            "status": "ok",
            "seekdb": "ok",
            "seekdb_error": "",
            "embedding": "ok",
            "embedding_error": "",
            "seekdb_indexed": sqlite_health["bookmark_count"],
            "seekdb_stale": 0,
            "seekdb_visits_indexed": sqlite_health["visit_count"],
            "seekdb_visits_stale": 0,
        }

    monkeypatch.setattr(routes_personal, "_browser_links_search_health", ok_health)

    result = asyncio.run(
        routes_personal.project_diary_day_browser_links(
            routes_personal.DiaryBrowserLinksProjectRequest(
                local_date="2026-06-18",
                actor="codex-test",
                source_surface="pytest",
                request_id="browser-links-test",
            )
        )
    )

    assert result["ok"] is True
    assert result["status"] == "ok"
    assert result["source_available"] is True
    assert result["projection"]["visit_event_count"] == 3
    assert result["projection"]["visited_page_count"] == 2
    assert result["projection"]["bookmark_count"] == 1
    assert set(result["projection"]["projected_event_ids"]) == {
        "browser-links-2026-06-18-visits",
        "browser-links-2026-06-18-bookmarks",
    }

    projection_path = tmp_path / "diary" / result["projection"]["file_ref"]
    projection_text = projection_path.read_text(encoding="utf-8")
    projection = json.loads(projection_text)
    assert projection["schema"] == "xarta.diary.browser_links.v1"
    assert projection["summary"]["visit_event_count"] == 3
    assert len(projection["visits"]) == 2
    assert projection["bookmarks"][0]["bookmark_id"] == "bookmark-today"
    assert projection["visits"][0]["url_hash"].startswith("sha256:")
    assert "description must stay out" not in projection_text
    assert "notes must stay out" not in projection_text
    assert "old.example.test/history" not in projection_text
    assert projection["initiation_backfill"]["bookmarks_existing_count"] == 1
    assert projection["initiation_backfill"]["visit_events_existing_count"] == 1

    initiation_dir = tmp_path / "diary" / "_initiation" / "2026-06-18" / "browser-links"
    assert (initiation_dir / "initiation-index.md").exists()
    assert (initiation_dir / "bookmarks-existing.json").exists()
    assert (initiation_dir / "visits-existing-summary.json").exists()

    event_rows = conn.execute(
        "SELECT * FROM personal_events WHERE source_type='browser-links' ORDER BY event_id"
    ).fetchall()
    assert [row["event_id"] for row in event_rows] == [
        "browser-links-2026-06-18-bookmarks",
        "browser-links-2026-06-18-visits",
    ]
    assert all(row["status"] == "open" for row in event_rows)
    assert "old.example.test" not in "\n".join(row["content_projection"] for row in event_rows)

    ledger = json.loads(
        (tmp_path / "diary" / "2026" / "06" / "18" / "source-ledger.json").read_text(
            encoding="utf-8"
        )
    )
    browser_ledger = [
        item
        for item in ledger["sources"]
        if str(item.get("ledger_entry_id", "")).startswith("browser-links:")
    ]
    assert len(browser_ledger) == 3
    assert all(item.get("url_hash", "").startswith("sha256:") for item in browser_ledger)
    manifest = json.loads(
        (tmp_path / "diary" / "2026" / "06" / "18" / "day-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert "browser-links-visits.json" in {item["path"] for item in manifest["files"]}

    rerun = asyncio.run(
        routes_personal.project_diary_day_browser_links(
            routes_personal.DiaryBrowserLinksProjectRequest(
                local_date="2026-06-18",
                actor="codex-test",
                source_surface="pytest",
                request_id="browser-links-test-rerun",
            )
        )
    )
    assert rerun["projection"]["skipped_existing_event_count"] == 2
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE source_type='browser-links'"
        ).fetchone()["count"]
        == 2
    )


def test_browser_links_projection_records_source_unavailable_status(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path / "diary")
    conn.executescript("DROP TABLE bookmarks; DROP TABLE visits; DROP TABLE visit_events;")

    result = asyncio.run(
        routes_personal.project_diary_day_browser_links(
            routes_personal.DiaryBrowserLinksProjectRequest(
                local_date="2026-06-18",
                actor="codex-test",
                source_surface="pytest",
                request_id="browser-links-missing-test",
            )
        )
    )

    assert result["ok"] is True
    assert result["source_available"] is False
    assert result["status"] == "source_unavailable"
    projection_path = tmp_path / "diary" / result["projection"]["file_ref"]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["status"] == "source_unavailable"
    assert projection["visits"] == []
    assert projection["bookmarks"] == []
    event = conn.execute(
        "SELECT * FROM personal_events WHERE source_type='browser-links'"
    ).fetchone()
    assert event["event_id"] == "browser-links-2026-06-18-source-status"
    assert event["status"] == "source_unavailable"
    source = conn.execute(
        "SELECT * FROM personal_sources WHERE source_id='browser-links'"
    ).fetchone()
    assert source["status"] == "source_unavailable"


def test_personal_automation_signatures_and_skip_gate(monkeypatch, tmp_path):
    automation = _load_personal_automation_module()
    db_path = tmp_path / "blueprints.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE bookmarks (
                bookmark_id TEXT PRIMARY KEY,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE visits (
                visit_id TEXT PRIMARY KEY,
                visited_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE visit_events (
                event_id TEXT PRIMARY KEY,
                visited_at TEXT
            );
            CREATE TABLE personal_events (
                event_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE personal_time_tasks (
                task_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE work_items (
                item_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE work_issues (
                issue_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE work_todos (
                todo_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE work_blockers (
                blocker_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE work_discussions (
                discussion_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            """
        )
        conn.execute(
            "INSERT INTO visit_events (event_id, visited_at) VALUES (?, ?)",
            ("ve-1", "2026-06-18T10:00:00Z"),
        )
        conn.execute(
            "INSERT INTO bookmarks (bookmark_id, created_at, updated_at) VALUES (?, ?, ?)",
            ("bm-1", "2026-06-18T11:00:00Z", "2026-06-18T11:00:00Z"),
        )

    monkeypatch.setenv("BLUEPRINTS_DB_PATH", str(db_path))
    signature_one = automation.source_signature(local_date="2026-06-18", kind="browser-links")
    signature_two = automation.source_signature(local_date="2026-06-18", kind="browser-links")
    assert signature_one == signature_two

    state = {"schema": automation.STATE_SCHEMA, "jobs": {}}
    automation.record_state(
        state,
        name="browser-links-rollup",
        local_date="2026-06-18",
        signature=signature_one,
        status="ok",
        summary={"status": "ok"},
    )
    assert automation.should_skip(
        state,
        name="browser-links-rollup",
        local_date="2026-06-18",
        signature=signature_one,
        force=False,
    )
    assert not automation.should_skip(
        state,
        name="browser-links-rollup",
        local_date="2026-06-18",
        signature=signature_one,
        force=True,
    )

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO visit_events (event_id, visited_at) VALUES (?, ?)",
            ("ve-2", "2026-06-18T12:00:00Z"),
        )
    signature_three = automation.source_signature(local_date="2026-06-18", kind="browser-links")
    assert signature_three != signature_one
