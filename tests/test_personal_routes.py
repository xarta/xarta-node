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

import pytest

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
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            description TEXT,
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
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
            related_kanban_items_json TEXT NOT NULL DEFAULT '[]',
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
            related_kanban_items_json TEXT NOT NULL DEFAULT '[]',
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
        CREATE TABLE personal_filter_meta_tags (
            meta_tag_id TEXT PRIMARY KEY,
            label TEXT NOT NULL DEFAULT '',
            color TEXT NOT NULL DEFAULT 'blue',
            priority INTEGER NOT NULL DEFAULT 0,
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE personal_filter_tags (
            tag_id TEXT PRIMARY KEY,
            label TEXT NOT NULL DEFAULT '',
            color TEXT NOT NULL DEFAULT 'blue',
            shape TEXT NOT NULL DEFAULT 'circle',
            fill TEXT NOT NULL DEFAULT 'outline',
            meta_tag_id TEXT NOT NULL DEFAULT '',
            builtin INTEGER NOT NULL DEFAULT 0,
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
        CREATE TABLE personal_git_commits (
            commit_id TEXT PRIMARY KEY,
            repo_full_name TEXT NOT NULL,
            sha TEXT NOT NULL,
            short_sha TEXT NOT NULL DEFAULT '',
            html_url TEXT NOT NULL DEFAULT '',
            author_login TEXT NOT NULL DEFAULT '',
            author_name TEXT NOT NULL DEFAULT '',
            committed_at TEXT NOT NULL DEFAULT '',
            local_date TEXT NOT NULL DEFAULT '',
            message_subject TEXT NOT NULL DEFAULT '',
            message_body TEXT NOT NULL DEFAULT '',
            branches_json TEXT NOT NULL DEFAULT '[]',
            pr_refs_json TEXT NOT NULL DEFAULT '[]',
            issue_refs_json TEXT NOT NULL DEFAULT '[]',
            feature_key TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            UNIQUE(repo_full_name, sha)
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
        CREATE TABLE kanban_item_states (
            state_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            lane_key TEXT NOT NULL,
            status_category TEXT NOT NULL DEFAULT 'open',
            sort_order INTEGER NOT NULL DEFAULT 0,
            is_terminal INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_item_priorities (
            priority_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            weight INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_items (
            item_id TEXT PRIMARY KEY,
            parent_item_id TEXT,
            title TEXT NOT NULL DEFAULT '',
            body_excerpt TEXT NOT NULL DEFAULT '',
            item_type TEXT NOT NULL DEFAULT 'item',
            state_id TEXT NOT NULL DEFAULT 'todo',
            priority_id TEXT NOT NULL DEFAULT 'medium',
            depth INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'open',
            goal_flag INTEGER NOT NULL DEFAULT 0,
            archived_at TEXT,
            promoted_from_ref TEXT,
            source_type TEXT NOT NULL DEFAULT 'manual-kanban',
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
        CREATE TABLE kanban_item_order_edges (
            edge_id TEXT PRIMARY KEY,
            parent_item_id TEXT NOT NULL DEFAULT '',
            state_id TEXT NOT NULL,
            priority_id TEXT NOT NULL,
            before_item_id TEXT NOT NULL,
            after_item_id TEXT NOT NULL,
            source_hash TEXT NOT NULL DEFAULT '',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_item_links (
            link_id TEXT PRIMARY KEY,
            source_item_id TEXT NOT NULL,
            target_item_id TEXT NOT NULL,
            link_type TEXT NOT NULL DEFAULT 'related',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_item_commits (
            commit_link_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            repo_full_name TEXT NOT NULL DEFAULT '',
            sha TEXT NOT NULL DEFAULT '',
            short_sha TEXT NOT NULL DEFAULT '',
            html_url TEXT NOT NULL DEFAULT '',
            author_login TEXT NOT NULL DEFAULT '',
            author_name TEXT NOT NULL DEFAULT '',
            committed_at TEXT NOT NULL DEFAULT '',
            message_subject TEXT NOT NULL DEFAULT '',
            message_body TEXT NOT NULL DEFAULT '',
            branch TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            UNIQUE(item_id, repo_full_name, sha)
        );
        CREATE TABLE kanban_review_decisions (
            decision_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            processor_kind TEXT NOT NULL DEFAULT 'review',
            decision_type TEXT NOT NULL DEFAULT 'decision',
            title TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            rationale TEXT NOT NULL DEFAULT '',
            affected_refs_json TEXT NOT NULL DEFAULT '[]',
            confidence TEXT NOT NULL DEFAULT '',
            uncertainty TEXT NOT NULL DEFAULT '',
            proof_refs_json TEXT NOT NULL DEFAULT '[]',
            commit_link_ids_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'recorded',
            provider_mode TEXT NOT NULL DEFAULT 'cloud-first',
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_review_processor_leases (
            lease_id TEXT PRIMARY KEY,
            processor_kind TEXT NOT NULL DEFAULT 'review',
            holder_id TEXT NOT NULL DEFAULT '',
            lease_token TEXT NOT NULL DEFAULT '',
            item_id TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'released',
            acquired_at TEXT NOT NULL DEFAULT '',
            heartbeat_at TEXT NOT NULL DEFAULT '',
            expires_at TEXT NOT NULL DEFAULT '',
            timeout_seconds INTEGER NOT NULL DEFAULT 1200,
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_agent_hints (
            hint_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL UNIQUE,
            required_skills_json TEXT NOT NULL DEFAULT '[]',
            routing_notes TEXT NOT NULL DEFAULT '',
            commit_attribution_json TEXT NOT NULL DEFAULT '{}',
            visibility TEXT NOT NULL DEFAULT 'agent',
            status TEXT NOT NULL DEFAULT 'active',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_agent_sessions (
            session_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            agent_id TEXT NOT NULL DEFAULT '',
            node_id TEXT NOT NULL DEFAULT '',
            worktree_path TEXT NOT NULL DEFAULT '',
            repo_full_name TEXT NOT NULL DEFAULT '',
            branch TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            started_at TEXT NOT NULL DEFAULT '',
            ended_at TEXT NOT NULL DEFAULT '',
            last_seen_at TEXT NOT NULL DEFAULT '',
            request_hash TEXT NOT NULL DEFAULT '',
            source_surface TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_blockers (
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
        CREATE TABLE kanban_discussions (
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
        CREATE TABLE kanban_audit_log (
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
        INSERT INTO kanban_item_states (
            state_id, label, lane_key, status_category, sort_order, is_terminal
        ) VALUES
            ('backlog', 'Backlog', 'backlog', 'open', 10, 0),
            ('todo', 'To Do', 'todo', 'open', 20, 0),
            ('doing', 'Doing', 'doing', 'active', 30, 0),
            ('blocked', 'Blocked', 'blocked', 'blocked', 40, 0),
            ('done', 'Done', 'done', 'done', 50, 1);
        INSERT INTO kanban_item_priorities (
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
            status, tags_json, related_kanban_items_json, related_import_batches_json
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
            related_kanban_item="work-1",
            limit=20,
            offset=0,
        )
    )

    assert result["pagination"]["count"] == 1
    item = result["items"][0]
    assert item["event_id"] == "evt-1"
    assert item["source"]["type"] == "manual"
    assert item["tags"] == ["diary", "personal"]
    assert item["related"]["kanban_items"] == ["work-1"]


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
- Traceability proof: [21 Jun Games Wordle trace](../../interests/games/results/trace-2026-06-21-games-wordle.json)

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
    trace = lone_wolf / "interests" / "games" / "results" / "trace-2026-06-21-games-wordle.json"
    trace.parent.mkdir(parents=True)
    raw = lone_wolf / "interests" / "games" / "raw" / "2026-06-22" / "wordle.json"
    raw.parent.mkdir(parents=True)
    raw.write_text("{}", encoding="utf-8")
    trace.write_text(
        json.dumps(
            {
                "schema": "xarta.interests.ingestion.traceability.v1",
                "ok": True,
                "generated_at": "2026-06-22T10:00:00Z",
                "selectors": {"event_ids": ["$wordle"], "urls": []},
                "summary": {
                    "categories": ["games"],
                    "completed_work_types": ["game_parse", "wiki_update"],
                    "game_types": ["wordle"],
                    "results": 2,
                    "wiki_pages": 1,
                },
                "raw_records": [
                    {
                        "category": "games",
                        "event_timestamp": "2026-06-21T22:12:10Z",
                        "path": str(raw),
                        "source_event_id": "$wordle",
                        "source_room_id": "!games:example.test",
                    }
                ],
                "categories": {
                    "games": {
                        "extracted": [
                            {
                                "parsed_candidates": [
                                    {
                                        "game_type": "wordle",
                                        "target_word": "ALIBI",
                                        "score": "4/6",
                                        "attempts": 4,
                                        "status": "win",
                                        "parser": "private_vision_wordle_screenshot_v2",
                                    }
                                ]
                            }
                        ],
                        "results": [{"completed_at": "2026-06-22T10:04:28Z"}],
                    }
                },
                "operator_surfaces": {
                    "raw_records": [str(raw)],
                    "visible_results": [
                        str(lone_wolf / "interests" / "games" / "results" / "wordle-result.json")
                    ],
                    "wiki_pages": [
                        str(lone_wolf / "interests" / "games" / "queries" / "wordle.md")
                    ],
                },
            },
            ensure_ascii=True,
        ),
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
    assert any(link["label"].startswith("Traceability proof:") for link in result["proof_links"])
    assert result["interests"]["recent_submissions"][0]["title"] == "Wordle screenshot: ALIBI"
    assert result["interests"]["recent_submissions"][0]["status"] == "processed"
    assert result["interests"]["recent_submissions"][0]["outcome"] == "4/6, ALIBI"
    assert (
        result["interests"]["recent_submissions"][0]["artifacts"]["trace"][0]["path"]
        == "interests/games/results/trace-2026-06-21-games-wordle.json"
    )
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


def test_imports_artifact_preview_is_allowlisted(monkeypatch, tmp_path):
    lone_wolf = tmp_path / "lone-wolf"
    artifact = lone_wolf / "interests" / "games" / "results" / "trace-wordle.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text('{"game_type":"wordle","score":"4/6"}\n', encoding="utf-8")
    monkeypatch.setattr(routes_personal, "LONE_WOLF_ROOT", lone_wolf)

    result = asyncio.run(
        routes_personal.get_imports_artifact(path="interests/games/results/trace-wordle.json")
    )

    assert result["ok"] is True
    assert result["path"] == "interests/games/results/trace-wordle.json"
    assert result["name"] == "trace-wordle.json"
    assert result["truncated"] is False
    assert result["sha256"].startswith("sha256:")
    assert '"game_type":"wordle"' in result["preview"]


def test_imports_artifact_preview_blocks_path_escape(monkeypatch, tmp_path):
    lone_wolf = tmp_path / "lone-wolf"
    lone_wolf.mkdir()
    monkeypatch.setattr(routes_personal, "LONE_WOLF_ROOT", lone_wolf)

    with pytest.raises(routes_personal.HTTPException) as error:
        asyncio.run(routes_personal.get_imports_artifact(path="../.env"))

    assert error.value.status_code == 400


def test_openclaw_ai_domain_audit_flags_missing_or_misfiled_urls(monkeypatch, tmp_path):
    lone_wolf = tmp_path / "lone-wolf"
    candidates = (
        lone_wolf
        / "runtime"
        / "openclaw-migration"
        / "2026-06-12-vm720"
        / "derived"
        / "bookmark_candidates.jsonl"
    )
    candidates.parent.mkdir(parents=True)
    marktechpost_url = "https://www.marktechpost.com/2026/02/21/example-ai-research/"
    venturebeat_url = "https://venturebeat.com/ai/example-model-news/"
    candidates.write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-02-22T22:40:20Z", "url": marktechpost_url}),
                json.dumps({"timestamp": "2026-02-23T09:00:00Z", "url": venturebeat_url}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    software_result = lone_wolf / "interests" / "software" / "results" / "misfiled.json"
    software_result.parent.mkdir(parents=True)
    software_result.write_text(json.dumps({"url": marktechpost_url}), encoding="utf-8")
    monkeypatch.setattr(routes_personal, "LONE_WOLF_ROOT", lone_wolf)

    result = routes_personal._openclaw_candidate_audit(
        {
            "category_summary": [
                {"Category": "ai-developments", "Raw": "0", "Results": "0", "Wiki pages": "0"}
            ]
        }
    )

    domains = {row["domain"]: row for row in result["ai_development_domains"]}
    assert result["status"] == "needs_review"
    assert domains["marktechpost.com"]["in_other_category"] == 1
    assert domains["marktechpost.com"]["examples"][0]["categories"] == ["software"]
    assert domains["venturebeat.com"]["missing_from_interests"] == 1


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

    shared_events = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-06-18",
            date_end="2026-06-18",
            limit=20,
            offset=0,
        )
    )
    pin_events = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-06-18",
            date_end="2026-06-18",
            privacy_level="pin",
            limit=20,
            offset=0,
        )
    )

    assert [item["event_id"] for item in shared_events["items"]] == ["evt-visible"]
    assert pin_events["items"] == []


def test_personal_list_routes_hide_pin_records_until_unlock(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_time_tasks (
            task_id, source_type, title, status, mode, local_date, timezone, privacy_level
        )
        VALUES
            ('task-visible', 'manual-task', 'Visible task', 'open', 'personal',
             '2026-06-18', 'Europe/London', 'normal'),
            ('task-pin', 'manual-task', 'Hidden task', 'open', 'personal',
             '2026-06-18', 'Europe/London', 'pin')
        """
    )
    conn.execute(
        """
        INSERT INTO personal_import_batches (
            import_batch_id, source_type, source_ref, title, status, local_date,
            privacy_level
        )
        VALUES
            ('batch-visible', 'interests-ingestion', 'run-visible', 'Visible import',
             'pending_review', '2026-06-18', 'normal'),
            ('batch-pin', 'interests-ingestion', 'run-pin', 'Hidden import',
             'pending_review', '2026-06-18', 'pin')
        """
    )

    tasks = asyncio.run(routes_personal.list_personal_tasks(mode="personal", limit=20, offset=0))
    pin_tasks = asyncio.run(
        routes_personal.list_personal_tasks(
            mode="personal",
            privacy_level="pin",
            limit=20,
            offset=0,
        )
    )
    imports = asyncio.run(
        routes_personal.list_personal_import_batches(
            date_start="2026-06-18",
            date_end="2026-06-18",
            limit=20,
            offset=0,
        )
    )
    pin_imports = asyncio.run(
        routes_personal.list_personal_import_batches(
            date_start="2026-06-18",
            date_end="2026-06-18",
            privacy_level="pin",
            limit=20,
            offset=0,
        )
    )

    assert [item["task_id"] for item in tasks["items"]] == ["task-visible"]
    assert tasks["counts"]["modes"]["personal"] == 1
    assert tasks["counts"]["total"] == 1
    assert pin_tasks["items"] == []
    assert [item["import_batch_id"] for item in imports["items"]] == ["batch-visible"]
    assert pin_imports["items"] == []


def test_diary_entry_write_projects_audit_and_rehydrates(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
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
            routes_personal.DiaryKanbanLinkRequest(
                kanban_item_ref="test-1",
                actor="codex-test",
                source_surface="pytest",
                request_id="link-test",
            ),
        )
    )
    assert linked["ok"] is True
    assert linked["event"]["related"]["kanban_items"] == ["test-1"]
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='link_kanban_item'"
        ).fetchone()["count"]
        == 1
    )

    deleted = asyncio.run(
        routes_personal.delete_diary_day_entry(
            event["event_id"],
            routes_personal.PersonalEventDeleteRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="diary-delete-test",
            ),
        )
    )
    assert deleted["ok"] is True
    assert deleted["deleted_event"]["event_id"] == event["event_id"]
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE event_id=?",
            (event["event_id"],),
        ).fetchone()["count"]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='delete_diary_entry'"
        ).fetchone()["count"]
        == 1
    )
    delete_sync = conn.execute(
        """
        SELECT * FROM sync_queue
        WHERE action_type='DELETE' AND table_name='personal_events' AND row_id=?
        """,
        (event["event_id"],),
    ).fetchone()
    assert delete_sync is not None


def test_diary_entry_write_classifies_automation_proof_outside_personal_log(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)

    created = asyncio.run(
        routes_personal.create_diary_day_entry(
            routes_personal.DiaryEntryCreateRequest(
                body="Step 10 Playwright browser quick-entry proof from Codex",
                local_date="2026-06-18",
                actor="codex",
                source_surface="playwright-proof",
                request_id="codex-live-proof-test",
            )
        )
    )

    event = created["event"]
    tags = event["tags"]
    assert event["kind"] == "automation-proof"
    assert "automation-proof" in tags
    assert "quick-entry" in tags
    assert "personal-log" not in tags

    row = conn.execute(
        "SELECT kind, tags_json FROM personal_events WHERE event_id=?",
        (event["event_id"],),
    ).fetchone()
    assert row["kind"] == "automation-proof"
    assert "personal-log" not in json.loads(row["tags_json"])


def test_diary_edit_allows_operator_to_update_task_backed_event_tags(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)

    created = asyncio.run(
        routes_personal.create_personal_task(
            routes_personal.PersonalTaskUpsertRequest(
                title="Step 15 Playwright ToDo proof",
                body="Original proof body",
                mode="kanban",
                status="open",
                due_date="2026-06-18",
                tags=["work", "diary", routes_personal.KANBAN_AGENT_WORKING_OUT_TAG],
                related_kanban_items=["kanban-proof"],
                actor="codex-test",
                source_surface="pytest",
                request_id="operator-task-create",
            )
        )
    )

    event_id = created["event"]["event_id"]
    assert "work" in created["task"]["tags"]
    assert "work" in created["event"]["tags"]

    updated = asyncio.run(
        routes_personal.update_diary_day_entry(
            event_id,
            routes_personal.DiaryEntryUpdateRequest(
                body="Step 15 Playwright ToDo proof edited\n\nEdited from Diary.",
                local_date="2026-06-18",
                all_day=True,
                tags=[
                    "todo",
                    "task",
                    "due",
                    routes_personal.KANBAN_AGENT_WORKING_OUT_TAG,
                    "kanban",
                    "diary",
                ],
                actor="operator",
                source_surface="diary-page",
                request_id="operator-task-edit",
            ),
        )
    )

    assert updated["ok"] is True
    assert updated["event"]["event_id"] == event_id
    assert updated["task"]["title"] == "Step 15 Playwright ToDo proof edited"
    assert "work" not in updated["task"]["tags"]
    assert "work" not in updated["event"]["tags"]
    task_row = conn.execute(
        "SELECT tags_json FROM personal_time_tasks WHERE task_id=?", (event_id,)
    ).fetchone()
    event_row = conn.execute(
        "SELECT tags_json FROM personal_events WHERE event_id=?", (event_id,)
    ).fetchone()
    assert "work" not in json.loads(task_row["tags_json"])
    assert "work" not in json.loads(event_row["tags_json"])
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='update_diary_task_event'"
        ).fetchone()["count"]
        == 1
    )

    deleted = asyncio.run(
        routes_personal.delete_diary_day_entry(
            event_id,
            routes_personal.PersonalEventDeleteRequest(
                actor="operator",
                source_surface="diary-page",
                request_id="operator-task-delete",
            ),
        )
    )
    assert deleted["ok"] is True
    assert deleted["deleted_task"]["task_id"] == event_id
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_tasks WHERE task_id=?", (event_id,)
        ).fetchone()["count"]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE event_id=?", (event_id,)
        ).fetchone()["count"]
        == 0
    )
    delete_tables = {
        row["table_name"]
        for row in conn.execute(
            "SELECT table_name FROM sync_queue WHERE action_type='DELETE'"
        ).fetchall()
    }
    assert {"personal_time_tasks", "personal_events"}.issubset(delete_tables)


def test_diary_edit_allows_operator_to_update_source_owned_event(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, source_ref, source_hash, kind, title, body_excerpt,
            content_projection, start_at, local_date, timezone, status, privacy_level,
            tags_json, provenance_json, last_rendered_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "github-proof-event",
            "git",
            "git:test",
            "old-hash",
            "git-activity",
            "Git activity summary",
            "Old body",
            "Old body",
            "2026-06-18T00:00:00Z",
            "2026-06-18",
            "Europe/London",
            "open",
            "normal",
            json.dumps(["github", "work"]),
            json.dumps({"source": "test"}),
            "2026-06-18T10:00:00Z",
        ),
    )

    updated = asyncio.run(
        routes_personal.update_diary_day_entry(
            "github-proof-event",
            routes_personal.DiaryEntryUpdateRequest(
                body="Git activity summary edited\n\nOperator changed tags.",
                local_date="2026-06-18",
                all_day=True,
                tags=["github"],
                actor="operator",
                source_surface="diary-page",
                request_id="operator-source-edit",
            ),
        )
    )

    assert updated["event"]["source"]["type"] == "git"
    assert updated["event"]["title"] == "Git activity summary edited"
    assert updated["event"]["tags"] == ["github", "all-day"]
    assert updated["event"]["provenance"]["operator_edit"]["preserved_source_type"] == "git"
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='update_diary_source_event'"
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
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

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

    deleted = asyncio.run(
        routes_personal.delete_calendar_event(
            event["event_id"],
            routes_personal.PersonalEventDeleteRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="calendar-delete-test",
            ),
        )
    )
    assert deleted["ok"] is True
    assert deleted["deleted_event"]["title"] == "Dentist moved"
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_events WHERE event_id=?",
            (event["event_id"],),
        ).fetchone()["count"]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_time_audit WHERE action='delete_calendar_event'"
        ).fetchone()["count"]
        == 1
    )
    delete_sync = conn.execute(
        """
        SELECT * FROM sync_queue
        WHERE action_type='DELETE' AND table_name='personal_events' AND row_id=?
        """,
        (event["event_id"],),
    ).fetchone()
    assert delete_sync is not None


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


def test_personal_filter_tags_and_meta_tags_are_server_backed_and_synced(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    meta = asyncio.run(
        routes_personal.upsert_personal_filter_meta_tag(
            routes_personal.PersonalFilterMetaTagUpsertRequest(
                meta_tag_id="calendar",
                label="Calendar",
                color="blue",
                priority=250,
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-meta-upsert-test",
            )
        )
    )
    assert meta["ok"] is True
    assert meta["meta_tag"]["meta_tag_id"] == "calendar"
    assert meta["meta_tag"]["color"] == "blue"

    tag = asyncio.run(
        routes_personal.upsert_personal_filter_tag(
            routes_personal.PersonalFilterTagUpsertRequest(
                tag_id="national-holiday",
                label="National Holiday",
                color="red",
                shape="star",
                fill="outline",
                meta_tag_id="calendar",
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-tag-upsert-test",
            )
        )
    )
    assert tag["ok"] is True
    assert tag["tag"]["tag_id"] == "national-holiday"
    assert tag["tag"]["meta_tag_id"] == "calendar"

    created = asyncio.run(
        routes_personal.create_calendar_event(
            routes_personal.CalendarEventUpsertRequest(
                event_id="uk-bank-holiday-test",
                title="UK Bank Holiday",
                body="Server-backed filter tag proof",
                local_date="2026-06-18",
                timezone="Europe/London",
                all_day=True,
                tags=["national-holiday"],
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-tag-event-test",
            )
        )
    )
    assert "national-holiday" in created["event"]["tags"]

    registry = asyncio.run(routes_personal.list_personal_filters())
    meta_by_id = {item["meta_tag_id"]: item for item in registry["meta_tags"]}
    tags_by_id = {item["tag_id"]: item for item in registry["tags"]}
    assert meta_by_id["calendar"]["color"] == "blue"
    assert tags_by_id["national-holiday"]["meta_tag_id"] == "calendar"
    assert tags_by_id["national-holiday"]["usage_count"] == 1

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "personal_filter_meta_tags" in sync_tables
    assert "personal_filter_tags" in sync_tables

    try:
        asyncio.run(
            routes_personal.delete_personal_filter_tag(
                "national-holiday",
                routes_personal.PersonalFilterDeleteRequest(
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="filter-tag-delete-assigned-test",
                ),
            )
        )
    except routes_personal.HTTPException as exc:
        assert exc.status_code == 400
        assert "assigned" in exc.detail
    else:
        raise AssertionError("assigned filter tag delete must be gated")

    temp_tag = asyncio.run(
        routes_personal.upsert_personal_filter_tag(
            routes_personal.PersonalFilterTagUpsertRequest(
                tag_id="temporary-proof",
                label="Temporary Proof",
                color="gold",
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-tag-unused-test",
            )
        )
    )
    assert temp_tag["ok"] is True
    deleted = asyncio.run(
        routes_personal.delete_personal_filter_tag(
            "temporary-proof",
            routes_personal.PersonalFilterDeleteRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-tag-delete-unused-test",
            ),
        )
    )
    assert deleted["ok"] is True
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM personal_filter_tags WHERE tag_id='temporary-proof'"
        ).fetchone()["count"]
        == 0
    )


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
                mode="kanban",
                due_date="2026-06-18",
                due_time="17:05",
                timezone="Europe/London",
                priority="medium",
                tags=["kanban"],
                related_kanban_items=["item-1"],
                actor="codex-test",
                source_surface="pytest",
                request_id="task-update-test",
            ),
        )
    )
    assert updated["task"]["title"] == "Step 15 backend task updated"
    assert updated["task"]["mode"] == "kanban"
    assert updated["task"]["related"]["kanban_items"] == ["item-1"]
    assert "kanban" in updated["task"]["tags"]
    assert updated["event"]["start_at"] == "2026-06-18T16:05:00Z"

    kanban_list = asyncio.run(
        routes_personal.list_personal_tasks(mode="kanban", limit=20, offset=0)
    )
    assert [item["task_id"] for item in kanban_list["items"]] == [task["task_id"]]

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
            tags_json, related_kanban_items_json, provenance_state
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
            tags_json, related_kanban_items_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-work",
            "kanban",
            "todo",
            "Work source task",
            "2026-06-18",
            "Europe/London",
            "blocked",
            json.dumps(["todo", "kanban"]),
            json.dumps(["item-9"]),
        ),
    )

    personal = asyncio.run(routes_personal.list_personal_tasks(mode="personal", limit=20, offset=0))
    kanban = asyncio.run(routes_personal.list_personal_tasks(mode="kanban", limit=20, offset=0))
    blocked = asyncio.run(routes_personal.list_personal_tasks(mode="blocked", limit=20, offset=0))

    assert [item["task_id"] for item in personal["items"]] == ["evt-reminder"]
    assert personal["items"][0]["source"]["authority"] == "event"
    assert [item["task_id"] for item in kanban["items"]] == ["evt-work"]
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
            tags_json, related_kanban_items_json, provenance_json
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
            tags_json, related_kanban_items_json, related_tasks_json,
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
            json.dumps(["work-graph"]),
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
            related_kanban_items_json, event_id
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
            json.dumps(["work-graph"]),
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
        INSERT INTO kanban_items (
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
        INSERT INTO kanban_items (item_id, title, state_id, status)
        VALUES (?, ?, ?, ?)
        """,
        ("work-other", "Graph dependency", "todo", "open"),
    )
    conn.execute(
        """
        INSERT INTO kanban_item_links (link_id, source_item_id, target_item_id, link_type)
        VALUES (?, ?, ?, ?)
        """,
        ("wil-graph", "work-graph", "work-other", "depends_on"),
    )
    conn.execute(
        """
        INSERT INTO kanban_items (
            item_id, parent_item_id, title, item_type, state_id, status,
            source_type, source_ref, related_task_ids_json, tags_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "issue-graph",
            "work-graph",
            "Graph issue",
            "issue",
            "todo",
            "open",
            "kanban-issue",
            "kanban_items:issue-graph",
            json.dumps(["task-graph"]),
            json.dumps(["issue", "kanban"]),
        ),
    )
    conn.execute(
        """
        INSERT INTO kanban_blockers (
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
    assert ("relates_to", "kanban_items:work-graph", "accepted") in event_targets
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

    kanban_links = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref="kanban_items:work-graph",
            sync=False,
            limit=80,
        )
    )
    work_targets = {
        (link["link_type"], link["target_ref"], link["link_state"])
        for link in kanban_links["links"]
    }
    assert ("evidence_for", "personal_events:evt-graph", "accepted") in work_targets
    assert ("evidence_for", "personal_time_tasks:task-graph", "accepted") in work_targets
    assert ("promoted_from", "personal_events:evt-graph", "accepted") in work_targets
    assert ("depends_on", "kanban_items:work-other", "accepted") in work_targets

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
    metadata_only_sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(
                actor="codex-other",
                source_surface="pytest",
                request_id="different-proof-request",
            )
        )
    )

    assert first_sync["ok"] is True
    assert first_sync["candidate_count"] >= 2
    assert first_sync["links"]["inserted"] == first_sync["candidate_count"]
    assert second_sync["links"]["unchanged"] == first_sync["candidate_count"]
    assert second_sync["links"]["updated"] == 0
    assert metadata_only_sync["links"]["unchanged"] == first_sync["candidate_count"]
    assert metadata_only_sync["links"]["updated"] == 0

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


def test_work_kanban_schema_api_depth_audit_sync_and_promote(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
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
    assert "kanban_items" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_items") == "item_id"
    assert "kanban_item_commits" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_item_commits") == "commit_link_id"
    assert "kanban_review_decisions" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_review_decisions") == "decision_id"
    assert "kanban_review_processor_leases" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_review_processor_leases") == "lease_id"
    assert "kanban_agent_hints" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_agent_hints") == "hint_id"
    assert "kanban_agent_sessions" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_agent_sessions") == "session_id"

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-root",
                title="Step 16 root board item",
                body="Root board proof\n\nSecond paragraph",
                state_id="todo",
                priority_id="high",
                goal_flag=True,
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
    assert root["goal_flag"] is True
    assert root["body_excerpt"] == "Root board proof\n\nSecond paragraph"
    assert root["search"]["metadata"]["vector"]["turbo_vec_ready"] is True
    assert root["vector"]["index_key"] == "kanban_items:work-root"

    board = asyncio.run(routes_personal.get_work_root_board())
    todo_column = next(
        column for column in board["board"]["columns"] if column["state"]["state_id"] == "todo"
    )
    assert [item["item_id"] for item in todo_column["items"]] == ["work-root"]
    assert todo_column["items"][0]["goal_flag"] is True

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
    assert child["goal_flag"] is False

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
    assert issue["vector"]["index_key"] == "kanban_items:issue-step16"
    assert issue["severity_id"] == "high"
    issue_card = conn.execute("SELECT * FROM kanban_items WHERE item_id='issue-step16'").fetchone()
    assert issue_card["item_type"] == "issue"
    assert issue_card["parent_item_id"] == "work-root"
    assert issue_card["state_id"] == "todo"
    assert issue_card["source_ref"] == "kanban_items:issue-step16"
    assert json.loads(issue_card["tags_json"]) == ["issue", "kanban"]
    root_rollup_with_open_issue = asyncio.run(routes_personal.get_work_item_rollup("work-root"))[
        "rollup"
    ]
    assert root_rollup_with_open_issue["issues"]["open"] == 1

    done_issue = asyncio.run(
        routes_personal.update_work_issue(
            "issue-step16",
            routes_personal.WorkIssueUpsertRequest(
                item_id="work-root",
                title="Step 16 issue",
                body="Issue proof",
                status="done",
                severity_id="high",
                source_ref="docs:step-16",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-done",
            ),
        )
    )["issue"]
    assert done_issue["status"] == "done"
    root_rollup_with_done_issue = asyncio.run(routes_personal.get_work_item_rollup("work-root"))[
        "rollup"
    ]
    assert root_rollup_with_done_issue["issues"]["open"] == 0

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
    assert grandchild_issue["item_id"] == "issue-step19-grandchild"
    assert grandchild_issue["parent_item_id"] == "work-depth-3"

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
    todo_card = conn.execute("SELECT * FROM kanban_items WHERE item_id='todo-step16'").fetchone()
    assert todo_card["item_type"] == "item"
    assert todo_card["parent_item_id"] == "work-root"
    assert todo_card["state_id"] == "todo"
    assert todo_card["source_ref"] == "kanban_items:todo-step16"
    assert json.loads(todo_card["related_task_ids_json"]) == ["task-step16"]
    assert "todo" in json.loads(todo_card["tags_json"])
    root_rollup_with_todo_lane_leaf = asyncio.run(
        routes_personal.get_work_item_rollup("work-root")
    )["rollup"]
    assert root_rollup_with_todo_lane_leaf["todos"]["open"] == 1

    tagged_todo_filter_item = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-filter-tag-todo",
                parent_item_id="work-root",
                title="Filter tag ToDo item",
                body="This appears on the ToDo page by tag, not in Kanban ToDo rollups.",
                item_type="item",
                state_id="doing",
                priority_id="medium",
                tags=["ToDo"],
                actor="codex-test",
                source_surface="pytest",
                request_id="filter-tag-todo-create",
            )
        )
    )["item"]
    assert tagged_todo_filter_item["item_type"] == "item"
    assert "todo" in {tag.lower() for tag in tagged_todo_filter_item["tags"]}
    root_rollup_with_todo_filter_tag = asyncio.run(
        routes_personal.get_work_item_rollup("work-root")
    )["rollup"]
    assert root_rollup_with_todo_filter_tag["todos"]["open"] == 1
    assert root_rollup_with_todo_filter_tag["items"]["leaf_metrics"]["active"] == 2
    assert root_rollup_with_todo_filter_tag["items"]["leaf_metrics"]["active_doing"] == 1
    assert root_rollup_with_todo_filter_tag["issues"]["leaf_metrics"]["done"] == 1

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
    assert scoped_todo["item_id"] == "todo-step19-grandchild"
    assert scoped_todo["parent_item_id"] == "work-depth-3"
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
    scoped_todo_card = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='todo-step19-grandchild'"
    ).fetchone()
    assert scoped_todo_card["item_type"] == "item"
    assert scoped_todo_card["parent_item_id"] == "work-depth-3"
    assert scoped_todo_card["state_id"] == "doing"
    assert scoped_todo_card["priority_id"] == "critical"
    assert "todo" in json.loads(scoped_todo_card["tags_json"])
    assert (
        conn.execute("SELECT COUNT(*) FROM kanban_items WHERE item_type='todo'").fetchone()[0] == 0
    )

    promoted = asyncio.run(
        routes_personal.promote_work_item(
            routes_personal.WorkPromoteRequest(
                source_ref="kanban_items:todo-step16",
                title="Promoted Step 16 todo",
                parent_item_id="work-root",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-promote",
            )
        )
    )["item"]
    assert promoted["item_id"] == "todo-step16"
    assert promoted["item_type"] == "item"
    assert promoted["promoted_from_ref"] == "kanban_items:todo-step16"
    assert promoted["related"]["tasks"] == ["task-step16"]

    promoted_issue = asyncio.run(
        routes_personal.promote_work_item(
            routes_personal.WorkPromoteRequest(
                source_ref="kanban_items:issue-step19-child",
                title="Promoted Step 19 issue",
                parent_item_id="work-depth-2",
                actor="codex-test",
                source_surface="pytest",
                request_id="issue-promote",
            )
        )
    )["item"]
    assert promoted_issue["item_id"] == "issue-step19-child"
    assert promoted_issue["item_type"] == "item"
    assert promoted_issue["promoted_from_ref"] == "kanban_items:issue-step19-child"
    assert promoted_issue["related"]["issues"] == ["issue-step19-child"]

    local_issues = asyncio.run(
        routes_personal.list_work_item_issues("work-root", scope="local", view="flat")
    )
    assert [row["issue_id"] for row in local_issues["items"]] == ["issue-step16"]
    assert local_issues["counts"]["descendant_items"] == 0

    descendant_issues = asyncio.run(
        routes_personal.list_work_item_issues("work-child", scope="descendants", view="grouped")
    )
    descendant_issue_ids = {row["issue_id"] for row in descendant_issues["items"]}
    assert descendant_issue_ids == {"issue-step19-grandchild"}
    assert descendant_issues["counts"]["descendant_items"] >= 2
    assert {group["scope"]["depth_offset"] for group in descendant_issues["groups"]}.issuperset(
        {1, 2}
    )

    descendant_todos = asyncio.run(
        routes_personal.list_work_item_todos("work-child", scope="descendants", view="tree")
    )
    descendant_todo_ids = {row["todo_id"] for row in descendant_todos["items"]}
    assert "todo-step19-grandchild" not in descendant_todo_ids
    assert descendant_todo_ids == {"issue-step19-child", "work-depth-12"}
    assert all(row["item_card"]["item_type"] == "item" for row in descendant_todos["items"])
    assert all(row["item_card"]["state_id"] == "todo" for row in descendant_todos["items"])
    assert descendant_todos["groups"][0]["scope"]["relation"] == "self"

    issue_direct = asyncio.run(routes_personal.get_work_issue("issue-step19-child"))
    assert issue_direct["issue"]["body_excerpt"] == "Scoped issue proof"
    assert issue_direct["item"]["item_id"] == "work-depth-2"
    assert issue_direct["item_card"]["item_id"] == "issue-step19-child"
    assert issue_direct["item_card"]["item_type"] == "item"
    assert [item["item_id"] for item in issue_direct["breadcrumbs"]] == [
        "work-child",
        "work-depth-2",
    ]
    issue_bundle = asyncio.run(
        routes_personal.get_rich_doc_bundle("kanban", "issue", "issue-step19-child")
    )
    assert issue_bundle["document"]["document_type"] == "issue"
    assert issue_bundle["document"]["body"] == "Scoped issue proof"

    todo_direct = asyncio.run(routes_personal.get_work_todo("todo-step19-grandchild"))
    assert todo_direct["todo"]["body_excerpt"] == "Two-level scoped todo proof updated"
    assert todo_direct["item"]["item_id"] == "work-depth-3"
    assert todo_direct["item_card"]["item_id"] == "todo-step19-grandchild"
    assert todo_direct["item_card"]["item_type"] == "item"
    assert todo_direct["item_card"]["state_id"] == "doing"
    todo_bundle = asyncio.run(
        routes_personal.get_rich_doc_bundle("kanban", "todo", "todo-step19-grandchild")
    )
    assert todo_bundle["document"]["document_type"] == "todo"
    assert todo_bundle["document"]["body"] == "Two-level scoped todo proof updated"

    work_tasks = asyncio.run(routes_personal.list_personal_tasks(mode="kanban", limit=200))
    work_task_refs = {
        item["source"]["ref"]
        for item in work_tasks["items"]
        if item["source"]["type"] == "kanban-todo"
    }
    assert work_task_refs == {
        "kanban_items:todo-step16",
        "kanban_items:todo-step19-grandchild",
        "kanban_items:work-filter-tag-todo",
    }
    assert len(work_task_refs) == len(
        [item for item in work_tasks["items"] if item["source"]["type"] == "kanban-todo"]
    )
    child_board = asyncio.run(routes_personal.get_work_child_board("work-root"))
    assert child_board["board"]["parent"]["item_id"] == "work-root"
    assert [item["item_id"] for item in child_board["board"]["breadcrumbs"]] == ["work-root"]
    assert child_board["board"]["remaining_depth"] == 12
    root_child_cards = {
        item["item_id"]: item
        for column in child_board["board"]["columns"]
        for item in column["items"]
    }
    assert root_child_cards["issue-step16"]["item_type"] == "issue"
    assert root_child_cards["todo-step16"]["item_type"] == "item"

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

    share_code_link = asyncio.run(
        routes_personal.create_work_item_link(
            "work-child",
            routes_personal.WorkItemLinkCreateRequest(
                target_item_id=f"xarta-kanban:item:{promoted['item_id']}",
                link_type="depends_on",
                metadata={"proof_step": "share-code-ref"},
                actor="codex-test",
                source_surface="pytest",
                request_id="link-create-share-code",
            ),
        )
    )["link"]
    assert share_code_link["source_item_id"] == "work-child"
    assert share_code_link["target_item_id"] == promoted["item_id"]
    assert share_code_link["metadata"]["proof_step"] == "share-code-ref"

    blocker = asyncio.run(
        routes_personal.create_work_blocker(
            routes_personal.WorkBlockerUpsertRequest(
                blocker_id="blocker-step18",
                item_id="work-root",
                title="Step 18 blocker",
                body="Blocker proof",
                blocked_by_ref=f"kanban_items:{promoted['item_id']}",
                actor="codex-test",
                source_surface="pytest",
                request_id="blocker-create",
            )
        )
    )["blocker"]
    assert blocker["vector"]["index_key"] == "kanban_blockers:blocker-step18"
    assert blocker["blocked_by_ref"] == f"kanban_items:{promoted['item_id']}"

    share_code_blocker = asyncio.run(
        routes_personal.create_work_blocker(
            routes_personal.WorkBlockerUpsertRequest(
                blocker_id="blocker-share-code",
                item_id="work-child",
                title="Share code blocker",
                body="Blocker proof with pasted share code",
                blocked_by_ref=f"xarta-kanban:todo:{todo['item_id']}",
                actor="codex-test",
                source_surface="pytest",
                request_id="blocker-create-share-code",
            )
        )
    )["blocker"]
    assert share_code_blocker["blocked_by_ref"] == f"kanban_items:{todo['item_id']}"

    detail_body = "# Work Root Detail\n\nLine one\nLine two"
    detail_doc = asyncio.run(
        routes_personal.update_work_item_detail_document(
            "work-root",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=detail_body,
                actor="codex-test",
                source_surface="pytest",
                request_id="detail-doc-update",
            ),
        )
    )["detail_document"]
    assert detail_doc["body"] == detail_body
    assert detail_doc["file_ref"]["path"] == "step-16-root-board-item/items/work-root/detail.md"
    assert (tmp_path / "kanban" / detail_doc["file_ref"]["path"]).exists()

    review_body = "## Review\n\nNegative learning stays available for future agents."
    review_doc = asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-root",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=review_body,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-doc-update",
            ),
        )
    )["review_document"]
    assert review_doc["body"] == review_body
    assert review_doc["file_ref"]["path"] == "step-16-root-board-item/items/work-root/review.md"
    assert (tmp_path / "kanban" / review_doc["file_ref"]["path"]).exists()
    review_bundle = asyncio.run(
        routes_personal.get_rich_doc_bundle("kanban", "item-review", "work-root")
    )
    assert review_bundle["document"]["document_type"] == "item-review"
    assert review_bundle["document"]["body"] == review_body

    discussion_body = "First discussion line\n\n- markdown survives"
    discussion = asyncio.run(
        routes_personal.create_work_discussion(
            "work-root",
            routes_personal.WorkDiscussionCreateRequest(
                discussion_id="discussion-step18",
                body=discussion_body,
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-create",
            ),
        )
    )["discussion"]
    assert discussion["body"] == discussion_body
    assert discussion["document"]["file_ref"]["path"] == (
        "step-16-root-board-item/items/work-root/discussions/discussion-step18.md"
    )

    updated_discussion_body = "Edited discussion\n\n```text\nkeeps newlines\n```"
    updated_discussion = asyncio.run(
        routes_personal.update_work_discussion(
            "discussion-step18",
            routes_personal.WorkDiscussionUpdateRequest(
                body=updated_discussion_body,
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-update",
            ),
        )
    )["discussion"]
    assert updated_discussion["body"] == updated_discussion_body
    assert updated_discussion["body_excerpt"] == updated_discussion_body

    renamed_root = asyncio.run(
        routes_personal.update_work_item(
            "work-root",
            routes_personal.WorkItemUpdateRequest(
                title="Step 16 Root Board Renamed",
                body="Root board proof\n\nRenamed paragraph",
                goal_flag=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="work-root-rename",
            ),
        )
    )["item"]
    assert renamed_root["title"] == "Step 16 Root Board Renamed"
    assert renamed_root["goal_flag"] is False
    assert renamed_root["body_excerpt"] == "Root board proof\n\nRenamed paragraph"
    assert (
        conn.execute("SELECT goal_flag FROM kanban_items WHERE item_id='work-root'").fetchone()[
            "goal_flag"
        ]
        == 0
    )
    manifest = json.loads((tmp_path / "kanban" / "projects.json").read_text(encoding="utf-8"))
    assert manifest["projects"]["work-root"]["title"] == "Step 16 Root Board Renamed"
    assert manifest["projects"]["work-root"]["folder"] == "step-16-root-board-renamed"
    assert manifest["projects"]["work-root"]["pending"] is None
    assert not (tmp_path / "kanban" / "step-16-root-board-item").exists()
    assert (tmp_path / "kanban" / "step-16-root-board-renamed").exists()

    detail = asyncio.run(routes_personal.get_work_item_detail("work-root"))
    assert detail["rollup"]["items"]["total"] >= 2
    assert [item["item_id"] for item in detail["breadcrumbs"]] == ["work-root"]
    assert detail["remaining_depth"] == 12
    assert detail["detail_document"]["body"] == detail_body
    assert detail["detail_document"]["file_ref"]["path"] == (
        "step-16-root-board-renamed/items/work-root/detail.md"
    )
    assert detail["review_document"]["body"] == review_body
    assert detail["review_document"]["file_ref"]["path"] == (
        "step-16-root-board-renamed/items/work-root/review.md"
    )
    assert {issue["issue_id"] for issue in detail["issues"]} == {"issue-step16"}
    assert {todo["todo_id"] for todo in detail["todos"]} == {"todo-step16"}
    assert detail["todos"][0]["item_card"]["item_type"] == "item"
    assert detail["todos"][0]["item_card"]["state_id"] == "todo"
    assert detail["links"][0]["link_id"] == link["link_id"]
    assert detail["blockers"][0]["blocker_id"] == "blocker-step18"
    assert detail["discussions"][0]["body"] == updated_discussion_body
    assert detail["counts"]["links"] == 1
    assert detail["counts"]["blockers"] == 1
    assert detail["counts"]["discussions"] == 1
    assert detail["counts"]["review"] == 1

    deleted_discussion = asyncio.run(
        routes_personal.delete_work_discussion(
            "discussion-step18",
            routes_personal.WorkItemActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-delete",
            ),
        )
    )
    assert deleted_discussion["ok"] is True
    assert not (
        tmp_path
        / "kanban"
        / "step-16-root-board-renamed/items/work-root/discussions/discussion-step18.md"
    ).exists()
    detail_after_delete = asyncio.run(routes_personal.get_work_item_detail("work-root"))
    assert detail_after_delete["counts"]["discussions"] == 0

    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert {
        "create_work_item",
        "move_work_item",
        "create_work_issue",
        "create_work_todo",
        "promote_work_item",
        "create_work_item_link",
        "create_work_blocker",
        "update_work_item_detail",
        "update_work_item_review",
        "create_work_discussion",
        "update_work_discussion",
        "delete_work_discussion",
    }.issubset(audit_actions)
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {
        "kanban_items",
        "kanban_item_links",
        "kanban_blockers",
        "kanban_discussions",
        "kanban_audit_log",
    }.issubset(sync_tables)


def test_work_kanban_commit_associations_are_item_scoped(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    sha_one = "a" * 40
    sha_two = "b" * 40
    conn.execute(
        """
        INSERT INTO personal_git_commits (
            commit_id, repo_full_name, sha, short_sha, html_url, author_login,
            author_name, committed_at, local_date, message_subject, message_body
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "ghc-one",
            "xarta/xarta-node",
            sha_one,
            sha_one[:7],
            f"https://github.com/xarta/xarta-node/commit/{sha_one}",
            "codex",
            "Codex",
            "2026-06-25T01:30:00Z",
            "2026-06-25",
            "Add commit association support",
            "Body from git import",
        ),
    )
    for item_id, title in (("work-commit-a", "Commit item A"), ("work-commit-b", "Commit item B")):
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    title=title,
                    actor="codex-test",
                    source_surface="pytest",
                    request_id=f"{item_id}-create",
                )
            )
        )

    first = asyncio.run(
        routes_personal.record_work_item_commit(
            "work-commit-a",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha_one,
                actor="codex-test",
                source_surface="pytest",
                request_id="commit-one",
            ),
        )
    )
    assert first["commit"]["message_subject"] == "Add commit association support"
    assert first["commit"]["commit_ref"] == f"git_commit:xarta/xarta-node@{sha_one}"

    asyncio.run(
        routes_personal.record_work_item_commit(
            "work-commit-a",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha_two,
                message_subject="Second commit",
                branch="main",
                metadata={"note": "second"},
                actor="codex-test",
                source_surface="pytest",
                request_id="commit-two",
            ),
        )
    )
    updated = asyncio.run(
        routes_personal.record_work_item_commit(
            "work-commit-a",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha_two,
                message_subject="Second commit amended metadata",
                branch="main",
                metadata={"note": "updated"},
                actor="codex-test",
                source_surface="pytest",
                request_id="commit-two-update",
            ),
        )
    )
    assert updated["commit"]["message_subject"] == "Second commit amended metadata"

    asyncio.run(
        routes_personal.record_work_item_commit(
            "work-commit-b",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha_one,
                message_subject="Same commit explicitly linked to B",
                actor="codex-test",
                source_surface="pytest",
                request_id="commit-one-b",
            ),
        )
    )

    detail_a = asyncio.run(routes_personal.get_work_item_detail("work-commit-a"))
    detail_b = asyncio.run(routes_personal.get_work_item_detail("work-commit-b"))
    assert detail_a["counts"]["commits"] == 2
    assert {row["sha"] for row in detail_a["commits"]} == {sha_one, sha_two}
    assert all(row["item_id"] == "work-commit-a" for row in detail_a["commits"])
    assert detail_b["counts"]["commits"] == 1
    assert detail_b["commits"][0]["sha"] == sha_one
    assert detail_b["commits"][0]["item_id"] == "work-commit-b"

    list_a = asyncio.run(routes_personal.list_work_item_commits("work-commit-a"))
    assert list_a["count"] == 2
    assert conn.execute("SELECT COUNT(*) FROM kanban_item_commits").fetchone()[0] == 3
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_item_commits" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "record_work_commit" in audit_actions

    sync = asyncio.run(
        routes_personal.sync_personal_graph_links(
            routes_personal.PersonalGraphSyncRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="commit-graph-sync",
            )
        )
    )
    assert sync["ok"] is True
    commit_links = asyncio.run(
        routes_personal.list_personal_graph_links(
            source_ref=f"git_commit:xarta/xarta-node@{sha_one}",
            sync=False,
            limit=10,
        )
    )
    targets = {link["target_ref"] for link in commit_links["links"]}
    assert {"kanban_items:work-commit-a", "kanban_items:work-commit-b"}.issubset(targets)


def test_work_kanban_review_decision_ledger_links_commits_and_status(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    sha = "c" * 40
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-decision-ledger",
                title="Decision ledger item",
                body="Decision ledger proof item",
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
                request_id="decision-ledger-item-create",
            )
        )
    )
    commit = asyncio.run(
        routes_personal.record_work_item_commit(
            "work-decision-ledger",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha,
                message_subject="Add decision ledger contract",
                branch="main",
                actor="codex-test",
                source_surface="pytest",
                request_id="decision-ledger-commit",
            ),
        )
    )["commit"]
    conn.execute("DELETE FROM sync_queue")

    created = asyncio.run(
        routes_personal.record_work_item_review_decision(
            "work-decision-ledger",
            routes_personal.WorkReviewDecisionCreateRequest(
                decision_id="decision-ledger-proof",
                title="Use cloud-first decision ledger",
                summary=(
                    "Decided to record autonomous Review Processor actions as "
                    "natural-language Kanban decision rows before queue code."
                ),
                rationale="The operator needs reconstructable decisions and explicit commit provenance.",
                affected_refs=["xarta-kanban:item:work-decision-ledger"],
                confidence="high",
                uncertainty="Queue lease implementation is intentionally outside this slice.",
                proof_refs=[
                    "pytest:test_work_kanban_review_decision_ledger_links_commits_and_status"
                ],
                commit_link_ids=[commit["commit_link_id"]],
                provider_mode="cloud-first",
                metadata={"hook_status": "passed"},
                actor="codex-test",
                source_surface="pytest",
                request_id="decision-ledger-record",
            ),
        )
    )
    decision = created["decision"]
    assert decision["decision_id"] == "decision-ledger-proof"
    assert decision["summary"].startswith("Decided to record autonomous Review Processor")
    assert decision["affected_refs"] == [
        "kanban_items:work-decision-ledger",
        "xarta-kanban:item:work-decision-ledger",
    ]
    assert decision["commit_link_ids"] == [commit["commit_link_id"]]
    assert decision["commits"][0]["sha"] == sha
    assert decision["provider_mode"] == "cloud-first"

    listed = asyncio.run(routes_personal.list_work_item_review_decisions("work-decision-ledger"))
    assert listed["count"] == 1
    assert listed["commit_link_health"]["ok"] is True
    assert listed["decisions"][0]["commits"][0]["message_subject"] == "Add decision ledger contract"

    status = asyncio.run(routes_personal.get_work_automation_status(item_id="work-decision-ledger"))
    assert status["provider_mode"]["active"] == "cloud-first"
    assert status["provider_mode"]["planned"] == "planned-gated"
    assert status["provider_mode"]["local_processing_gate"] == "structured-job-packets-required"
    assert status["provider_mode"]["automatic_switch"] is False
    assert (
        status["processing_policy"]["schema"]
        == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    )
    assert status["processing_policy"]["active_mode"] == "cloud-first"
    assert status["processing_policy"]["local_processing"]["state"] == "planned-gated"
    assert status["review_processor"]["status"] == "decision-ledger-ready"
    assert (
        status["review_processor"]["lease"]["schema"] == routes_personal.KANBAN_REVIEW_LEASE_SCHEMA
    )
    assert status["review_processor"]["lease"]["exists"] is False
    assert (
        status["output_contract"]["schema"] == routes_personal.KANBAN_REVIEW_OUTPUT_CONTRACT_SCHEMA
    )
    assert status["output_contract"]["decision_record_schema"] == "xarta.kanban.review_decision.v1"
    assert {output_type["type"] for output_type in status["output_contract"]["output_types"]} == {
        "lesson",
        "prompt_change",
        "contradiction_check",
        "follow_up_card",
    }
    assert status["decisions"]["count"] == 1
    assert status["decisions"]["recent"][0]["decision_id"] == "decision-ledger-proof"
    assert status["commit_link_health"]["decisions_with_commits"] == 1
    assert status["commit_link_health"]["missing_commit_link_count"] == 0

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_decisions" in sync_tables
    assert "kanban_audit_log" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "record_review_processor_decision" in audit_actions


def test_work_review_processor_output_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_review_processor_output_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_REVIEW_OUTPUT_CONTRACT_SCHEMA
    assert (
        contract["processing_policy_schema"]
        == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    )
    assert contract["provider_mode"]["active"] == "cloud-first"
    assert contract["provider_mode"]["local_processing_gate"] == "structured-job-packets-required"
    assert contract["provider_mode"]["automatic_switch"] is False
    assert "metadata.output_payload" in contract["minimum_decision_fields"]
    output_types = {output["type"]: output for output in contract["output_types"]}
    assert set(output_types) == {
        "lesson",
        "prompt_change",
        "contradiction_check",
        "follow_up_card",
    }
    assert "current_behavior" in output_types["prompt_change"]["required_payload_fields"]
    assert "source_refs" in output_types["contradiction_check"]["required_payload_fields"]


def test_work_review_processor_processing_policy_endpoint():
    result = asyncio.run(routes_personal.get_work_review_processor_processing_policy())
    policy = result["policy"]
    assert result["ok"] is True
    assert policy["schema"] == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    assert policy["active_mode"] == "cloud-first"
    assert policy["applies_to"] == ["review_processor", "preprocessing"]
    assert policy["cloud_processing"]["state"] == "active"
    assert policy["local_processing"]["state"] == "planned-gated"
    assert policy["local_processing"]["gate"] == "structured-job-packets-required"
    assert policy["local_processing"]["automatic_switch"] is False
    assert "structured_job_packet_schema" in policy["local_processing"]["switch_requires"]
    assert policy["provider_choice"]["default_mode"] == "cloud-first"
    assert "local" in policy["provider_choice"]["blocked_until_gate"]
    assert any("Provider mode must be explicit" in rule for rule in policy["routing_rules"])


def test_work_review_processor_lease_acquire_conflict_heartbeat_release(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-lease",
                title="Review Processor lease item",
                body="Lease proof item",
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-lease-item-create",
            )
        )
    )
    conn.execute("DELETE FROM sync_queue")
    conn.commit()

    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-a",
                item_id="work-review-lease",
                session_id="kanban-agent-session-lease-proof",
                ttl_seconds=300,
                metadata={"slice": "queue-lease"},
                actor="codex-test",
                source_surface="pytest",
                request_id="review-lease-acquire",
            )
        )
    )
    assert acquired["ok"] is True
    assert acquired["acquired"] is True
    assert acquired["lease"]["schema"] == routes_personal.KANBAN_REVIEW_LEASE_SCHEMA
    assert acquired["lease"]["holder_id"] == "codex-a"
    assert acquired["lease"]["item_id"] == "work-review-lease"
    assert acquired["lease"]["active"] is True
    assert acquired["lease"]["timeout_seconds"] == 300
    assert acquired["lease"]["metadata"]["slice"] == "queue-lease"
    token = acquired["lease"]["lease_token"]
    assert token.startswith("lease-")

    status = asyncio.run(routes_personal.get_work_automation_status(item_id="work-review-lease"))
    assert status["review_processor"]["status"] == "lease-active"
    assert status["review_processor"]["active_item_id"] == "work-review-lease"
    assert status["review_processor"]["lease_owner"] == "codex-a"
    assert status["review_processor"]["lease"]["active"] is True
    assert status["review_processor"]["lease"]["holder_id"] == "codex-a"
    assert "lease_token" not in status["review_processor"]["lease"]

    readback = asyncio.run(routes_personal.get_work_review_processor_lease())
    assert readback["lease"]["active"] is True
    assert "lease_token" not in readback["lease"]

    blocked = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-b",
                item_id="work-review-lease",
                ttl_seconds=300,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-lease-blocked-acquire",
            )
        )
    )
    assert blocked["ok"] is True
    assert blocked["acquired"] is False
    assert blocked["reason"] == "active_lease"
    assert blocked["lease"]["holder_id"] == "codex-a"
    assert "lease_token" not in blocked["lease"]

    heartbeat = asyncio.run(
        routes_personal.heartbeat_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-a",
                lease_token=token,
                ttl_seconds=600,
                metadata={"heartbeat": "tested"},
                actor="codex-test",
                source_surface="pytest",
                request_id="review-lease-heartbeat",
            )
        )
    )
    assert heartbeat["ok"] is True
    assert heartbeat["heartbeated"] is True
    assert heartbeat["lease"]["active"] is True
    assert heartbeat["lease"]["timeout_seconds"] == 600
    assert heartbeat["lease"]["metadata"]["heartbeat"] == "tested"
    assert "lease_token" not in heartbeat["lease"]

    released = asyncio.run(
        routes_personal.release_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-a",
                lease_token=token,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-lease-release",
            )
        )
    )
    assert released["ok"] is True
    assert released["released"] is True
    assert released["lease"]["status"] == "released"
    assert released["lease"]["active"] is False

    status_after = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-lease")
    )
    assert status_after["review_processor"]["status"] == "decision-ledger-ready"
    assert status_after["review_processor"]["lease"]["status"] == "released"
    assert status_after["review_processor"]["lease"]["active"] is False

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_leases" in sync_tables
    assert "kanban_audit_log" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "acquire_review_processor_lease" in audit_actions
    assert "heartbeat_review_processor_lease" in audit_actions
    assert "release_review_processor_lease" in audit_actions


def test_work_kanban_agent_hints_hidden_api(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-agent-hints",
                title="Agent hints proof",
                body="Visible card text",
                state_id="todo",
                priority_id="high",
                tags=["agent-ledger"],
                actor="codex-test",
                source_surface="pytest",
                request_id="agent-hints-create",
            )
        )
    )
    assert created["item"]["item_id"] == "work-agent-hints"
    conn.execute("DELETE FROM sync_queue")

    empty = asyncio.run(routes_personal.get_work_item_agent_hints("work-agent-hints"))
    assert empty["agent_hints"]["exists"] is False
    assert empty["agent_hints"]["required_skills"] == []

    detail_before = asyncio.run(routes_personal.get_work_item_detail("work-agent-hints"))
    assert "agent_hints" not in detail_before
    board = asyncio.run(routes_personal.get_work_root_board())
    board_item = next(
        item
        for column in board["board"]["columns"]
        for item in column["items"]
        if item["item_id"] == "work-agent-hints"
    )
    assert "agent_hints" not in board_item

    updated = asyncio.run(
        routes_personal.update_work_item_agent_hints(
            "work-agent-hints",
            routes_personal.WorkAgentHintsUpdateRequest(
                required_skills=[
                    "blueprints-work-management",
                    "git-operations",
                    "blueprints-work-management",
                ],
                routing_notes="Use the Kanban helper before committing.",
                commit_attribution={"mode": "explicit_item", "require_commit_link": True},
                metadata={"slice": "hints-schema"},
                actor="codex-test",
                source_surface="pytest",
                request_id="agent-hints-update",
            ),
        )
    )
    hints = updated["agent_hints"]
    assert hints["exists"] is True
    assert hints["visibility"] == "agent"
    assert hints["required_skills"] == ["blueprints-work-management", "git-operations"]
    assert hints["commit_attribution"]["require_commit_link"] is True
    assert hints["metadata"]["slice"] == "hints-schema"
    assert hints["provenance"]["recorded_by"] == "codex-test"

    detail_after = asyncio.run(routes_personal.get_work_item_detail("work-agent-hints"))
    assert "agent_hints" not in detail_after
    row = conn.execute(
        "SELECT * FROM kanban_agent_hints WHERE item_id='work-agent-hints'"
    ).fetchone()
    assert row is not None
    assert row["visibility"] == "agent"
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_agent_hints" in sync_tables
    assert "kanban_audit_log" in sync_tables


def test_work_kanban_agent_sessions_api(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-agent-session",
                title="Agent session proof",
                body="Visible card text",
                state_id="doing",
                priority_id="high",
                tags=["agent-ledger"],
                actor="codex-test",
                source_surface="pytest",
                request_id="agent-session-item-create",
            )
        )
    )
    conn.execute("DELETE FROM sync_queue")

    empty = asyncio.run(routes_personal.list_work_item_agent_sessions("work-agent-session"))
    assert empty["count"] == 0

    created = asyncio.run(
        routes_personal.create_work_item_agent_session(
            "work-agent-session",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="session-proof-1",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                request_hash="sha256:test-request",
                source_surface="pytest-session",
                summary="Started schema/API work",
                metadata={"slice": "sessions-api"},
                actor="codex-test",
                request_id="agent-session-create",
            ),
        )
    )
    session = created["agent_session"]
    assert session["session_id"] == "session-proof-1"
    assert session["item_id"] == "work-agent-session"
    assert session["agent_id"] == "codex"
    assert session["node_id"] == "test-node"
    assert session["repo_full_name"] == "xarta/xarta-node"
    assert session["status"] == "active"
    assert session["metadata"]["slice"] == "sessions-api"

    updated = asyncio.run(
        routes_personal.update_work_agent_session(
            "session-proof-1",
            routes_personal.WorkAgentSessionUpdateRequest(
                status="done",
                summary="Completed schema/API work",
                metadata={"slice": "sessions-api", "result": "done"},
                actor="codex-test",
                source_surface="pytest-session",
                request_id="agent-session-update",
            ),
        )
    )["agent_session"]
    assert updated["status"] == "done"
    assert updated["ended_at"]
    assert updated["metadata"]["result"] == "done"

    listed = asyncio.run(routes_personal.list_work_item_agent_sessions("work-agent-session"))
    assert listed["count"] == 1
    assert listed["agent_sessions"][0]["session_id"] == "session-proof-1"
    detail = asyncio.run(routes_personal.get_work_item_detail("work-agent-session"))
    assert "agent_sessions" not in detail
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_agent_sessions" in sync_tables
    assert "kanban_audit_log" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "record_work_agent_session" in audit_actions
    assert "update_work_agent_session" in audit_actions


def test_work_kanban_test_entry_visibility_preference_filters_board(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    user_item = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-user-visible",
                title="User visible item",
                body="Normal work survives the test-entry filter.",
                state_id="todo",
                priority_id="medium",
                tags=["planning"],
                actor="codex-test",
                source_surface="pytest",
                request_id="work-user-visible-create",
            )
        )
    )["item"]
    agent_item = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-agent-hidden",
                title="Agent working-out item",
                body="Automation test entry should be hideable.",
                state_id="todo",
                priority_id="high",
                tags=["proof", "step-19"],
                actor="active-browser",
                source_surface="kanban-active-browser-proof",
                request_id="work-agent-hidden-create",
            )
        )
    )["item"]
    assert "kanban" in user_item["tags"]
    assert "kanban" in agent_item["tags"]
    assert "agent-working-out" not in user_item["tags"]
    assert "agent-working-out" in agent_item["tags"]

    default_config = asyncio.run(routes_personal.get_work_config())
    assert default_config["preferences"]["show_test_entries"] is True
    default_board = asyncio.run(routes_personal.get_work_root_board())
    todo_default = next(
        column
        for column in default_board["board"]["columns"]
        if column["state"]["state_id"] == "todo"
    )
    assert {item["item_id"] for item in todo_default["items"]} == {
        "work-user-visible",
        "work-agent-hidden",
    }
    assert default_board["board"]["preferences"]["show_test_entries"] is True

    asyncio.run(
        routes_personal.create_work_todo(
            routes_personal.WorkTodoUpsertRequest(
                todo_id="todo-user-visible",
                item_id="work-user-visible",
                title="User visible todo",
                body="Normal Kanban todo survives the test-entry filter.",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-user-visible-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_todo(
            routes_personal.WorkTodoUpsertRequest(
                todo_id="todo-agent-hidden",
                item_id="work-agent-hidden",
                title="Agent working-out todo",
                body="Automation test-entry todo should be hideable.",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="todo-agent-hidden-create",
            )
        )
    )
    user_todo_filter_tags = json.loads(
        conn.execute(
            "SELECT tags_json FROM kanban_items WHERE item_id='todo-user-visible'"
        ).fetchone()[0]
    )
    agent_todo_filter_tags = json.loads(
        conn.execute(
            "SELECT tags_json FROM kanban_items WHERE item_id='todo-agent-hidden'"
        ).fetchone()[0]
    )
    assert (
        conn.execute(
            "SELECT item_type FROM kanban_items WHERE item_id='todo-user-visible'"
        ).fetchone()[0]
        == "item"
    )
    assert (
        conn.execute(
            "SELECT item_type FROM kanban_items WHERE item_id='todo-agent-hidden'"
        ).fetchone()[0]
        == "item"
    )
    assert "agent-working-out" not in user_todo_filter_tags
    assert "agent-working-out" in agent_todo_filter_tags
    assert "todo" in user_todo_filter_tags
    assert "todo" in agent_todo_filter_tags
    asyncio.run(
        routes_personal.create_personal_task(
            routes_personal.PersonalTaskUpsertRequest(
                task_id="task-kanban-proof-visible",
                title="User visible legacy Kanban proof task",
                body="Normal manual Kanban-linked task survives the test-entry filter.",
                mode="kanban",
                status="archived",
                due_date="2026-06-18",
                tags=["proof"],
                related_kanban_items=["STEP15-PW-visible"],
                actor="codex-test",
                source_surface="pytest",
                request_id="task-kanban-proof-visible-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_personal_task(
            routes_personal.PersonalTaskUpsertRequest(
                task_id="task-kanban-proof-agent",
                title="Agent working-out legacy Kanban proof task",
                body="Older manual ToDo proof rows should be hideable by persisted tag.",
                mode="kanban",
                status="archived",
                due_date="2026-06-18",
                tags=["proof", routes_personal.KANBAN_AGENT_WORKING_OUT_TAG],
                related_kanban_items=["STEP15-PW-agent"],
                actor="codex-test",
                source_surface="pytest",
                request_id="task-kanban-proof-agent-create",
            )
        )
    )
    default_tasks = asyncio.run(routes_personal.list_personal_tasks(mode="kanban", limit=50))
    default_refs = {
        item["source"]["ref"]
        for item in default_tasks["items"]
        if item["source"]["type"] == "kanban-todo"
    }
    assert default_refs == {"kanban_items:todo-user-visible", "kanban_items:todo-agent-hidden"}
    default_manual_refs = {
        item["source"]["ref"]
        for item in default_tasks["items"]
        if item["source"]["type"] == "manual-task"
    }
    assert default_manual_refs == {
        "personal_time_tasks:task-kanban-proof-visible",
        "personal_time_tasks:task-kanban-proof-agent",
    }
    assert default_tasks["kanban_preferences"]["show_test_entries"] is True

    hidden_pref = asyncio.run(
        routes_personal.update_kanban_preferences(
            routes_personal.WorkPreferencesUpdateRequest(
                show_test_entries=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="hide-test-entries",
            )
        )
    )
    assert hidden_pref["preferences"]["show_test_entries"] is False
    assert (
        conn.execute(
            "SELECT value FROM settings WHERE key=?",
            (routes_personal.KANBAN_SHOW_TEST_ENTRIES_SETTING,),
        ).fetchone()["value"]
        == "false"
    )

    hidden_board = asyncio.run(routes_personal.get_work_root_board())
    todo_hidden = next(
        column
        for column in hidden_board["board"]["columns"]
        if column["state"]["state_id"] == "todo"
    )
    assert [item["item_id"] for item in todo_hidden["items"]] == ["work-user-visible"]
    assert all("kanban" in item["tags"] for item in todo_hidden["items"])
    assert all("agent-working-out" not in item["tags"] for item in todo_hidden["items"])
    assert hidden_board["board"]["rollup"]["items"]["total"] == 2
    assert hidden_board["board"]["hidden_test_items"] == 1
    hidden_tasks = asyncio.run(routes_personal.list_personal_tasks(mode="kanban", limit=50))
    hidden_refs = {
        item["source"]["ref"]
        for item in hidden_tasks["items"]
        if item["source"]["type"] == "kanban-todo"
    }
    assert hidden_refs == {"kanban_items:todo-user-visible"}
    hidden_manual_refs = {
        item["source"]["ref"]
        for item in hidden_tasks["items"]
        if item["source"]["type"] == "manual-task"
    }
    assert hidden_manual_refs == {"personal_time_tasks:task-kanban-proof-visible"}
    assert hidden_tasks["kanban_preferences"]["show_test_entries"] is False
    assert hidden_tasks["test_entries"]["hidden_kanban_todos"] == 1
    assert hidden_tasks["test_entries"]["hidden_personal_tasks"] == 1

    shown_pref = asyncio.run(
        routes_personal.update_kanban_preferences(
            routes_personal.WorkPreferencesUpdateRequest(
                show_test_entries=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="show-test-entries",
            )
        )
    )
    assert shown_pref["preferences"]["show_test_entries"] is True
    shown_board = asyncio.run(routes_personal.get_work_root_board())
    todo_shown = next(
        column
        for column in shown_board["board"]["columns"]
        if column["state"]["state_id"] == "todo"
    )
    assert {item["item_id"] for item in todo_shown["items"]} == {
        "work-user-visible",
        "work-agent-hidden",
    }
    shown_tasks = asyncio.run(routes_personal.list_personal_tasks(mode="kanban", limit=50))
    shown_refs = {
        item["source"]["ref"]
        for item in shown_tasks["items"]
        if item["source"]["type"] == "kanban-todo"
    }
    assert shown_refs == {"kanban_items:todo-user-visible", "kanban_items:todo-agent-hidden"}
    shown_manual_refs = {
        item["source"]["ref"]
        for item in shown_tasks["items"]
        if item["source"]["type"] == "manual-task"
    }
    assert shown_manual_refs == {
        "personal_time_tasks:task-kanban-proof-visible",
        "personal_time_tasks:task-kanban-proof-agent",
    }
    assert shown_tasks["kanban_preferences"]["show_test_entries"] is True
    assert shown_tasks["test_entries"]["hidden_kanban_todos"] == 0
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "settings" in sync_tables


def test_work_item_lane_order_uses_priority_then_relative_edges(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    def create_item(item_id: str, title: str, state_id: str, priority_id: str) -> dict:
        return asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    title=title,
                    body=f"{title} body",
                    state_id=state_id,
                    priority_id=priority_id,
                    actor="codex-test",
                    source_surface="pytest",
                    request_id=f"{item_id}-create",
                )
            )
        )["item"]

    create_item("kanban-order-medium", "Medium Doing", "doing", "medium")
    create_item("kanban-order-high-a", "High Doing A", "doing", "high")
    create_item("kanban-order-high-b", "High Doing B", "doing", "high")
    create_item("kanban-order-critical", "Critical Doing", "doing", "critical")
    create_item("kanban-order-blocked-a", "Blocked Medium A", "blocked", "medium")
    create_item("kanban-order-blocked-b", "Blocked Medium B", "blocked", "medium")

    def lane_ids(state_id: str) -> list[str]:
        board = asyncio.run(routes_personal.get_work_root_board())
        column = next(
            column
            for column in board["board"]["columns"]
            if column["state"]["state_id"] == state_id
        )
        return [item["item_id"] for item in column["items"]]

    assert lane_ids("doing")[:4] == [
        "kanban-order-critical",
        "kanban-order-high-a",
        "kanban-order-high-b",
        "kanban-order-medium",
    ]

    ordered = asyncio.run(
        routes_personal.order_work_item(
            "kanban-order-high-b",
            routes_personal.WorkItemOrderRequest(
                direction="up",
                actor="codex-test",
                source_surface="pytest",
                request_id="order-high-b-up",
            ),
        )
    )
    assert ordered["changed"] is True
    assert ordered["lane_order"] == ["kanban-order-high-b", "kanban-order-high-a"]
    assert lane_ids("doing")[:4] == [
        "kanban-order-critical",
        "kanban-order-high-b",
        "kanban-order-high-a",
        "kanban-order-medium",
    ]

    edge = conn.execute(
        """
        SELECT * FROM kanban_item_order_edges
        WHERE state_id='doing' AND priority_id='high'
        """
    ).fetchone()
    assert edge["before_item_id"] == "kanban-order-high-b"
    assert edge["after_item_id"] == "kanban-order-high-a"

    ordered_down = asyncio.run(
        routes_personal.order_work_item(
            "kanban-order-high-b",
            routes_personal.WorkItemOrderRequest(
                direction="down",
                actor="codex-test",
                source_surface="pytest",
                request_id="order-high-b-down",
            ),
        )
    )
    assert ordered_down["lane_order"] == ["kanban-order-high-a", "kanban-order-high-b"]
    assert lane_ids("doing")[:4] == [
        "kanban-order-critical",
        "kanban-order-high-a",
        "kanban-order-high-b",
        "kanban-order-medium",
    ]

    moved_to_blocked = asyncio.run(
        routes_personal.move_work_item(
            "kanban-order-medium",
            routes_personal.WorkItemMoveRequest(
                parent_item_id=None,
                state_id="blocked",
                actor="codex-test",
                source_surface="pytest",
                request_id="medium-to-blocked",
            ),
        )
    )
    assert moved_to_blocked["item"]["state_id"] == "blocked"
    assert lane_ids("blocked")[:3] == [
        "kanban-order-medium",
        "kanban-order-blocked-a",
        "kanban-order-blocked-b",
    ]

    blocked_edges = conn.execute(
        """
        SELECT before_item_id, after_item_id FROM kanban_item_order_edges
        WHERE state_id='blocked' AND priority_id='medium'
        ORDER BY before_item_id, after_item_id
        """
    ).fetchall()
    assert {tuple(edge) for edge in blocked_edges} == {
        ("kanban-order-blocked-a", "kanban-order-blocked-b"),
        ("kanban-order-medium", "kanban-order-blocked-a"),
    }

    asyncio.run(
        routes_personal.move_work_item(
            "kanban-order-medium",
            routes_personal.WorkItemMoveRequest(
                parent_item_id=None,
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
                request_id="medium-back-to-doing",
            ),
        )
    )
    asyncio.run(
        routes_personal.move_work_item(
            "kanban-order-medium",
            routes_personal.WorkItemMoveRequest(
                parent_item_id=None,
                state_id="blocked",
                actor="codex-test",
                source_surface="pytest",
                request_id="medium-back-to-blocked",
            ),
        )
    )
    assert lane_ids("blocked")[:3] == [
        "kanban-order-medium",
        "kanban-order-blocked-a",
        "kanban-order-blocked-b",
    ]

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_item_order_edges" in sync_tables
    assert routes_sync._pk_for_table("kanban_item_order_edges") == "edge_id"


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
            CREATE TABLE kanban_items (
                item_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE kanban_blockers (
                blocker_id TEXT PRIMARY KEY,
                updated_at TEXT
            );
            CREATE TABLE kanban_discussions (
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
