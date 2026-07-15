import asyncio
import hashlib
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

from app import db as app_db  # noqa: E402
from app import (  # noqa: E402
    kanban_parity,
    kanban_postgres,
    routes_kanban_backups,
    routes_kanban_postgres,
    routes_personal,
    routes_sync,
)
from app.kanban_datastore import (  # noqa: E402
    KanbanDatastoreConfigError,
    load_kanban_datastore_config,
)

VERBATIM_OPERATOR_REQUEST_FIXTURE = "I want you to set-up a subagent to check and if necessary update the pre-processing (Kanban) process so that whenever I ask you to do something and put it in the kanban cards, you'll include a verbatim copy of my request(s).  That should be in your kanban skill and the pre-processing to make sure that's the case.  And part of the implementation plans put into the kanban cards should always including checking outcomes against original verbatim requests.  While deviations might be allowed as implementations evolve, there should always be very very good reasons that do not violate the intent of the original request (verbatim) without Operator approval or necessity from because of contradictions or impossibilities etc. in the original request discovered during implementation phases if not caught during pre-processing seeking operator approval then.  This should be a relatively small change the subagent can test and verify and commit and push.  It should not take hours.  The operator will be away so you'll have to pose as the Operator for testing."


def _preprocessing_contract_fields(
    *, recommended_mode: str = "serial", unit_title: str = "Implement the scoped leaf"
) -> dict:
    return {
        "proposal_entries": [],
        "execution_directive": {
            "recommended_mode": recommended_mode,
            "rationale": (
                "The whole bounded fixture has one owner and no independent conflicting write scope."
            ),
            "work_units": [
                {
                    "unit_id": "scoped-unit",
                    "title": unit_title,
                    "scope": "Complete only the current fixture and stop when its asserted proof passes.",
                    "depends_on": [],
                    "repo_paths": [],
                    "write_scopes": [],
                    "required_skills": [],
                    "proof_expected": ["Focused behavioral assertion passes"],
                }
            ],
            "isolation_requirements": [],
            "candidate_nodes": [],
            "resource_requirements": [],
            "required_context": [],
            "merge_reconciliation_owner": "owning test agent",
            "timeout_checkin_failure_behavior": "Stop and report a failing assertion.",
            "cleanup_criteria": [],
            "evidence": ["bounded fixture state"],
        },
    }


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
            automation_excluded INTEGER NOT NULL DEFAULT 0,
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
        CREATE TABLE kanban_priority_recommendations (
            recommendation_id TEXT PRIMARY KEY,
            scope_id TEXT NOT NULL DEFAULT 'kanban',
            rank INTEGER NOT NULL DEFAULT 0,
            item_id TEXT NOT NULL,
            title TEXT NOT NULL DEFAULT '',
            summary TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            priority_id TEXT NOT NULL DEFAULT 'medium',
            state_id TEXT NOT NULL DEFAULT '',
            score REAL NOT NULL DEFAULT 0,
            strategy_version TEXT NOT NULL DEFAULT 'skill-managed-v1',
            source_surface TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            generated_at TEXT NOT NULL DEFAULT '',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            UNIQUE(scope_id, rank)
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
            provider_mode TEXT NOT NULL DEFAULT 'local',
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
        CREATE TABLE kanban_review_processor_markers (
            marker_id TEXT PRIMARY KEY,
            item_id TEXT NOT NULL,
            processor_kind TEXT NOT NULL DEFAULT 'review',
            document_type TEXT NOT NULL DEFAULT 'review',
            document_ref TEXT NOT NULL DEFAULT '',
            document_updated_at TEXT NOT NULL DEFAULT '',
            document_source_hash TEXT NOT NULL DEFAULT '',
            processed_document_updated_at TEXT NOT NULL DEFAULT '',
            processed_source_hash TEXT NOT NULL DEFAULT '',
            processed_at TEXT NOT NULL DEFAULT '',
            queued_at TEXT NOT NULL DEFAULT '',
            last_seen_at TEXT NOT NULL DEFAULT '',
            processing_started_at TEXT NOT NULL DEFAULT '',
            processing_expires_at TEXT NOT NULL DEFAULT '',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            next_retry_at TEXT NOT NULL DEFAULT '',
            retry_after_seconds INTEGER NOT NULL DEFAULT 0,
            retry_attempt_count INTEGER NOT NULL DEFAULT 0,
            last_successful_source_hash TEXT NOT NULL DEFAULT '',
            last_failure_event_id TEXT NOT NULL DEFAULT '',
            last_failure_source_hash TEXT NOT NULL DEFAULT '',
            last_error_class TEXT NOT NULL DEFAULT '',
            retry_policy_version TEXT NOT NULL DEFAULT '',
            superseded_at TEXT NOT NULL DEFAULT '',
            superseded_by_source_hash TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'queued',
            provider_mode TEXT NOT NULL DEFAULT 'local',
            decision_id TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}',
            provenance_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT DEFAULT '2026-06-18T10:00:00Z',
            updated_at TEXT DEFAULT '2026-06-18T10:00:00Z'
        );
        CREATE TABLE kanban_review_processor_failure_events (
            failure_event_id TEXT PRIMARY KEY,
            marker_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            processor_kind TEXT NOT NULL DEFAULT 'review',
            document_type TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL DEFAULT '',
            error_class TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            provider_mode TEXT NOT NULL DEFAULT 'local',
            model_alias TEXT NOT NULL DEFAULT '',
            attempt_number INTEGER NOT NULL DEFAULT 0,
            failed_at TEXT NOT NULL DEFAULT '',
            next_retry_at TEXT NOT NULL DEFAULT '',
            retry_after_seconds INTEGER NOT NULL DEFAULT 0,
            retry_policy_version TEXT NOT NULL DEFAULT '',
            retryable INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL DEFAULT 'retry_waiting',
            event_hash TEXT NOT NULL DEFAULT '',
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


def test_init_db_does_not_create_kanban_sqlite_tables(monkeypatch, tmp_path):
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    db_path = db_dir / "blueprints.db"

    monkeypatch.setattr(app_db.cfg, "DB_DIR", str(db_dir))
    monkeypatch.setattr(app_db.cfg, "DB_PATH", str(db_path))

    app_db.init_db()

    with sqlite3.connect(db_path) as conn:
        kanban_table_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type='table'
              AND name LIKE 'kanban\\_%' ESCAPE '\\'
            """
        ).fetchone()[0]

    assert kanban_table_count == 0


def test_kanban_ref_rewrite_records_completion_marker():
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE sync_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE personal_search_documents (
            document_id TEXT,
            record_type TEXT,
            record_table TEXT,
            record_id TEXT,
            source_type TEXT,
            source_ref TEXT,
            search_text TEXT,
            related_refs_json TEXT,
            page_ref_json TEXT,
            source_refs_json TEXT,
            provenance_json TEXT,
            vector_index_key TEXT
        );
        INSERT INTO personal_search_documents (
            document_id, record_type, record_table, record_id, source_type,
            source_ref, search_text, related_refs_json, page_ref_json,
            source_refs_json, provenance_json, vector_index_key
        )
        VALUES (
            'work_items:legacy', 'work_item', 'work_items', 'legacy',
            'manual-work', 'work_items:legacy', 'work_items:legacy',
            '["work_items:legacy"]', '{}', '["work_items:legacy"]',
            '{"table": "work_items"}', 'work_items:legacy'
        );
        """
    )

    app_db._rewrite_kanban_ref_text(conn)

    marker = conn.execute(
        "SELECT value FROM sync_meta WHERE key=?",
        (app_db._KANBAN_REF_REWRITE_MARKER,),
    ).fetchone()
    rewritten = conn.execute("SELECT source_ref FROM personal_search_documents").fetchone()[0]
    assert marker == ("complete",)
    assert rewritten == "kanban_items:legacy"

    conn.execute("UPDATE personal_search_documents SET source_ref='work_items:after-marker'")
    app_db._rewrite_kanban_ref_text(conn)

    skipped = conn.execute("SELECT source_ref FROM personal_search_documents").fetchone()[0]
    assert skipped == "work_items:after-marker"


@contextmanager
def _conn_context(conn: sqlite3.Connection):
    yield conn
    conn.commit()


def _patch_conn(monkeypatch, conn: sqlite3.Connection) -> None:
    monkeypatch.setattr(routes_personal, "get_conn", lambda: _conn_context(conn))
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV,
        routes_personal._work_automation_current_node_id(),
    )


def _create_proposal_surface_fixture() -> None:
    items = [
        (
            routes_personal.KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID,
            None,
            "Proposal surfaces",
            "doing",
        ),
        (
            routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID,
            routes_personal.KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID,
            "INBOX",
            "doing",
        ),
        (
            routes_personal.KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID,
            routes_personal.KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID,
            "OUTBOX",
            "doing",
        ),
        ("work-proposal-semantic-owner", None, "Semantic implementation owner", "doing"),
    ]
    for item_id, parent_item_id, title, state_id in items:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    parent_item_id=parent_item_id,
                    title=title,
                    body=f"Durable fixture for {title}.",
                    state_id=state_id,
                    actor="codex-test",
                    source_surface="pytest",
                )
            )
        )


def _proposal_request(
    *, lifecycle_required: bool = True, **overrides
) -> routes_personal.WorkProposalInboxCreateRequest:
    values = {
        "entry_id": ("proposal-fixture-entry" if lifecycle_required else "proposal-fixture-note"),
        "entry_type": "approval_request",
        "title": "Choose the bounded release behavior",
        "summary": "The implementation has two safe behaviors with different rollout costs.",
        "rationale": "Current evidence does not grant the agent authority to select the rollout.",
        "requested_operator_action": "Approve one behavior or authorize bounded best judgment.",
        "exact_decision_needed": "Choose staged rollout, immediate rollout, or defer.",
        "source_item_refs": ["xarta-kanban:item:work-proposal-semantic-owner"],
        "semantic_owner_item_ref": "xarta-kanban:item:work-proposal-semantic-owner",
        "proof_refs": ["pytest:proposal-surface"],
        "lifecycle_required": lifecycle_required,
        "actor": "codex-test",
        "source_surface": "pytest",
    }
    values.update(overrides)
    return routes_personal.WorkProposalInboxCreateRequest(**values)


def _patch_kanban_backup_env(monkeypatch, tmp_path: Path, conn: sqlite3.Connection) -> Path:
    kanban_root = tmp_path / "kanban"
    backup_dir = kanban_root / "backups"
    backup_dir.mkdir(parents=True)
    monkeypatch.setattr(routes_kanban_backups, "get_conn", lambda: _conn_context(conn))
    monkeypatch.setattr(routes_kanban_backups.cfg, "KANBAN_DIR", str(kanban_root))
    monkeypatch.setattr(routes_kanban_backups.cfg, "KANBAN_BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(routes_kanban_backups.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(routes_kanban_backups.cfg, "NODE_NAME", "Test Node")
    return kanban_root


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


def test_personal_event_date_filters_include_calendar_spans(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute(
        """
        INSERT INTO personal_events (
            event_id, source_type, kind, title, local_date, timezone, status,
            tags_json, provenance_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "evt-span",
            "manual",
            "personal-log",
            "Liz visiting Hilary",
            "2026-07-23",
            "Europe/London",
            "open",
            json.dumps(["diary", "all-day"]),
            json.dumps(
                {
                    "calendar": {
                        "all_day": True,
                        "local_start_time": "",
                        "local_end_time": "",
                        "local_end_date": "2026-07-29",
                        "timezone": "Europe/London",
                    }
                }
            ),
        ),
    )

    visible = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-07-26",
            date_end="2026-07-26",
            tag="diary",
            limit=20,
            offset=0,
        )
    )
    outside = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-07-30",
            date_end="2026-07-30",
            tag="diary",
            limit=20,
            offset=0,
        )
    )
    day_events, pin_hidden, source_counts = routes_personal._visible_day_events("2026-07-26")

    assert [item["event_id"] for item in visible["items"]] == ["evt-span"]
    assert outside["items"] == []
    assert [item["event_id"] for item in day_events] == ["evt-span"]
    assert pin_hidden == 0
    assert source_counts["manual"] == 1


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


def test_diary_entry_write_keeps_date_range_as_one_event(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    monkeypatch.setattr(routes_personal, "DIARY_ROOT", tmp_path)

    created = asyncio.run(
        routes_personal.create_diary_day_entry(
            routes_personal.DiaryEntryCreateRequest(
                body="Liz visiting Hilary",
                local_date="2026-07-24",
                range_start_date="2026-07-23",
                range_end_date="2026-07-29",
                all_day=True,
                tags=["liz-away"],
                actor="codex-test",
                source_surface="pytest",
                request_id="range-write-test",
            )
        )
    )

    event = created["event"]
    rows = conn.execute(
        "SELECT event_id, local_date, provenance_json FROM personal_events"
    ).fetchall()
    day_events, _, source_counts = routes_personal._visible_day_events("2026-07-26")

    assert created["ok"] is True
    assert event["local_date"] == "2026-07-23"
    assert event["provenance"]["calendar"]["local_end_date"] == "2026-07-29"
    assert "liz-away" in event["tags"]
    assert len(rows) == 1
    assert rows[0]["event_id"] == event["event_id"]
    assert rows[0]["local_date"] == "2026-07-23"
    assert json.loads(rows[0]["provenance_json"])["calendar"]["local_end_date"] == "2026-07-29"
    assert [item["event_id"] for item in day_events] == [event["event_id"]]
    assert source_counts["manual"] == 1


def test_calendar_event_write_keeps_date_range_as_one_event(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    created = asyncio.run(
        routes_personal.create_calendar_event(
            routes_personal.CalendarEventUpsertRequest(
                title="Liz visiting Hilary",
                body="Away",
                local_date="2026-07-24",
                range_start_date="2026-07-23",
                range_end_date="2026-07-29",
                all_day=True,
                tags=["liz-away"],
                actor="codex-test",
                source_surface="pytest",
                request_id="calendar-range-write-test",
            )
        )
    )

    event = created["event"]
    rows = conn.execute(
        "SELECT event_id, local_date, provenance_json FROM personal_events"
    ).fetchall()
    visible = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-07-26",
            date_end="2026-07-26",
            source_type="manual-calendar",
            limit=20,
            offset=0,
        )
    )
    outside = asyncio.run(
        routes_personal.list_personal_events(
            date_start="2026-07-30",
            date_end="2026-07-30",
            source_type="manual-calendar",
            limit=20,
            offset=0,
        )
    )

    assert created["ok"] is True
    assert event["local_date"] == "2026-07-23"
    assert event["provenance"]["calendar"]["local_end_date"] == "2026-07-29"
    assert "liz-away" in event["tags"]
    assert len(rows) == 1
    assert rows[0]["event_id"] == event["event_id"]
    assert rows[0]["local_date"] == "2026-07-23"
    assert json.loads(rows[0]["provenance_json"])["calendar"]["local_end_date"] == "2026-07-29"
    assert [item["event_id"] for item in visible["items"]] == [event["event_id"]]
    assert outside["items"] == []


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

    conn.execute(
        """
        INSERT INTO personal_events (event_id, title, local_date, tags_json)
        VALUES ('friend-birthday-test', 'Friend birthday', '2026-06-27', '["birthdays-friends"]')
        """
    )
    conn.execute(
        """
        INSERT INTO personal_filter_tags (
            tag_id, label, color, shape, fill, meta_tag_id, builtin
        )
        VALUES ('orphaned-meta-tag-proof', 'Orphaned Meta Tag Proof', 'gold', 'circle', 'filled', 'important', 0)
        """
    )

    registry = asyncio.run(routes_personal.list_personal_filters())
    meta_by_id = {item["meta_tag_id"]: item for item in registry["meta_tags"]}
    tags_by_id = {item["tag_id"]: item for item in registry["tags"]}
    assert meta_by_id["calendar"]["color"] == "blue"
    assert meta_by_id["important"]["source"] == "orphaned-assignment"
    assert tags_by_id["national-holiday"]["meta_tag_id"] == "calendar"
    assert tags_by_id["national-holiday"]["usage_count"] == 1
    assert tags_by_id["birthdays-friends"]["source"] == "discovered"
    assert tags_by_id["birthdays-friends"]["usage_count"] == 1
    assert "birthdays-friends" in registry["integrity"]["discovered_tag_ids"]
    assert "important" in registry["integrity"]["orphan_meta_tag_ids"]

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
            json.dumps(
                {
                    "file_ref": "2026/06/18/10-20-personal-log.md",
                    "calendar": {"local_end_date": "2026-06-20"},
                }
            ),
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
    assert needle["results"][0]["date_span"] == {
        "start": "2026-06-18",
        "end": "2026-06-20",
        "is_range": True,
        "label": "2026-06-18 to 2026-06-20",
    }
    assert {"exact", "fts_bm25"}.issubset(set(needle["results"][0]["score"]["score_sources"]))

    overlap = asyncio.run(
        routes_personal.search_personal_activity(
            q="Needle",
            date_start="2026-06-19",
            date_end="2026-06-19",
            include_vector=False,
            rerank_results=False,
            sync=False,
            limit=10,
        )
    )
    assert overlap["results"][0]["document_id"] == "personal_events:evt-diary-search"

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


def test_personal_search_get_sync_skips_embedding_indexing_but_runs_requested_vector(monkeypatch):
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
            "evt-fast-search-sync",
            "manual-calendar",
            "calendar:fast-search-sync",
            "sha256:fast-search-sync",
            "calendar-event",
            "Dentist proof",
            "Search should not rebuild embeddings interactively",
            "Search should not rebuild embeddings interactively",
            "2026-07-09",
            "Europe/London",
            "open",
            json.dumps(["health"]),
        ),
    )

    async def fail_vector_sync(**_kwargs):
        raise AssertionError("GET /personal/search must not perform embedding sync")

    vector_called = False

    async def fake_vector_candidates(q: str, *, limit: int):
        nonlocal vector_called
        vector_called = True
        return (
            [
                {
                    "id": "personal_events:evt-fast-search-sync",
                    "metadata": {"document_id": "personal_events:evt-fast-search-sync"},
                    "distance": 0.07,
                }
            ],
            {"status": "ok", "error": "", "candidate_count": 1},
        )

    monkeypatch.setattr(routes_personal, "_sync_personal_search_vectors", fail_vector_sync)
    monkeypatch.setattr(routes_personal, "_personal_vector_candidates", fake_vector_candidates)

    result = asyncio.run(
        routes_personal.search_personal_activity(
            q="dentist",
            date_start="2026-07-06",
            date_end="2026-07-12",
            include_vector=True,
            rerank_results=True,
            sync=True,
            limit=10,
        )
    )

    assert result["subsystems"]["sync"]["documents"]["document_count"] == 1
    assert result["subsystems"]["sync"]["vector"]["status"] == "skipped"
    assert result["subsystems"]["vector"]["status"] == "ok"
    assert result["subsystems"]["rerank"]["status"] == "skipped"
    assert vector_called is True
    assert result["results"][0]["document_id"] == "personal_events:evt-fast-search-sync"


def test_personal_search_get_defaults_to_no_sync(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _disable_import_status_sync(monkeypatch)
    conn.execute(
        """
        INSERT INTO personal_search_documents (
            document_id, record_type, record_table, record_id, source_type,
            source_ref, source_hash, title, body, search_text, local_date,
            status, mode, privacy_level, tags_json, related_refs_json,
            page_ref_json, source_refs_json, provenance_json, score_metadata_json,
            vector_index_key
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "personal_events:evt-default-nosync",
            "calendar",
            "personal_events",
            "evt-default-nosync",
            "manual-calendar",
            "calendar:default-nosync",
            "sha256:default-nosync",
            "Dentist default proof",
            "Existing indexed document",
            "Dentist default proof Existing indexed document",
            "2026-07-09",
            "open",
            "calendar",
            "normal",
            json.dumps(["health"]),
            "[]",
            json.dumps({"group": "dave", "tab": "calendar", "date": "2026-07-09"}),
            json.dumps(["personal_events:evt-default-nosync"]),
            "{}",
            "{}",
            "personal_events:evt-default-nosync",
        ),
    )
    conn.execute(
        """
        INSERT INTO personal_search_fts (
            document_id, title, body, search_text, tags, source_type, record_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "personal_events:evt-default-nosync",
            "Dentist default proof",
            "Existing indexed document",
            "Dentist default proof Existing indexed document",
            "health",
            "manual-calendar",
            "calendar",
        ),
    )

    async def fail_search_sync(**_kwargs):
        raise AssertionError("GET /personal/search should not sync by default")

    monkeypatch.setattr(routes_personal, "_sync_personal_search_index", fail_search_sync)

    result = asyncio.run(
        routes_personal.search_personal_activity(
            q="dentist",
            date_start="2026-07-06",
            date_end="2026-07-12",
            include_vector=False,
            rerank_results=False,
            limit=10,
        )
    )

    assert result["subsystems"]["sync"] is None
    assert result["results"][0]["document_id"] == "personal_events:evt-default-nosync"


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
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "personal_graph_links" in sync_tables


def test_full_db_restore_policy_is_recovery_or_force_only():
    assert routes_sync._full_restore_allowed(force_restore=False, integrity_ok=True) is False
    assert routes_sync._full_restore_allowed(force_restore=False, integrity_ok=False) is True
    assert routes_sync._full_restore_allowed(force_restore=True, integrity_ok=True) is True


def test_kanban_idle_worker_no_root_scans_automatic_todo_leaves(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv("BLUEPRINTS_KANBAN_AUTOMATION_IDLE_WORKER", "1")
    monkeypatch.delenv("BLUEPRINTS_KANBAN_AUTOMATION_ROOT_ITEM_ID", raising=False)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-auto-root",
                title="Automatic root",
                body="Root item should not be a preprocessing leaf once it has children.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-auto-todo-leaf",
                parent_item_id="work-auto-root",
                title="Automatic ToDo leaf",
                body="This ToDo leaf needs preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )

    async def fake_local_ai_json_completion(*, messages, run_id, processor_kind=""):
        assert "work-auto-todo-leaf" in messages[1]["content"]
        return {
            "model_alias": "TEST-KANBAN-LOCAL-AI",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                **_preprocessing_contract_fields(unit_title="Process the automatic ToDo leaf"),
                "ready": True,
                "title": "Context ready",
                "summary": "The automatic ToDo leaf was preprocessed.",
                "rationale": "The card has enough context for this proof.",
                "confidence": "high",
                "uncertainty": "",
                "blocking_codes": [],
                "recommended_next_actions": [],
                "decomposition_items": [],
                "affected_refs": ["xarta-kanban:item:work-auto-todo-leaf"],
                "proof_refs": ["kanban_items:work-auto-todo-leaf:body"],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_local_ai_json_completion,
    )

    result = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )

    assert result["ok"] is True
    assert result["item_id"] == ""
    assert result["lease_acquired"] is True
    assert result["preprocessing_scan"]["queued_count"] == 1
    assert result["eligible_marker_count"] == 1
    assert result["processed_count"] == 1
    assert result["processed_markers"][0]["processor_kind"] == "preprocessing"
    assert result["processed_markers"][0]["item_id"] == "work-auto-todo-leaf"

    marker = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE item_id='work-auto-todo-leaf'"
    ).fetchone()
    assert marker["status"] == "processed"


def test_kanban_idle_worker_skips_non_owner_node(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV,
        "owner-node",
    )
    monkeypatch.delenv(routes_personal.KANBAN_AUTOMATION_SINGLETON_OVERRIDE_ENV, raising=False)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH_ENV,
        str(tmp_path / "missing-kanban-automation-override"),
    )
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )

    assert tick["ok"] is True
    assert tick["enabled"] is True
    assert tick["effective_enabled"] is False
    assert tick["runs_on_this_node"] is False
    assert tick["current_node_id"] == "test-node"
    assert tick["owner_node_id"] == "owner-node"
    assert tick["reason"] == "idle_worker_not_owner_node"
    assert tick["lease_acquired"] is False
    assert tick["processed_count"] == 0
    assert tick["eligible_marker_count"] == 0


def test_kanban_idle_worker_singleton_override_allows_non_owner_node(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    override_path = tmp_path / "kanban-automation-override"
    override_path.write_text("operator approved failover\n", encoding="utf-8")
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV,
        "owner-node",
    )
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_SINGLETON_OVERRIDE_PATH_ENV,
        str(override_path),
    )
    monkeypatch.delenv(routes_personal.KANBAN_AUTOMATION_SINGLETON_OVERRIDE_ENV, raising=False)

    config = routes_personal._work_automation_idle_worker_config()

    assert config["current_node_id"] == "test-node"
    assert config["owner_node_id"] == "owner-node"
    assert config["singleton_owner_match"] is False
    assert config["singleton_override"]["file"]["exists"] is True
    assert config["singleton_override"]["active"] is True
    assert config["runs_on_this_node"] is True
    assert config["effective_enabled"] is True


def test_kanban_idle_worker_max_scan_items_range_metadata(monkeypatch):
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV,
        routes_personal._work_automation_current_node_id(),
    )
    monkeypatch.setenv("BLUEPRINTS_KANBAN_AUTOMATION_MAX_SCAN_ITEMS", "250")

    config = routes_personal._work_automation_idle_worker_config()

    range_config = config["range_config"]["max_scan_items"]
    assert config["max_scan_items"] == routes_personal.KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP
    assert range_config["env_name"] == "BLUEPRINTS_KANBAN_AUTOMATION_MAX_SCAN_ITEMS"
    assert range_config["raw_value"] == "250"
    assert range_config["default"] == routes_personal.KANBAN_AUTOMATION_DEFAULT_MAX_SCAN_ITEMS
    assert range_config["min"] == 1
    assert range_config["max"] == routes_personal.KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP
    assert range_config["effective"] == routes_personal.KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP
    assert range_config["source"] == "env"
    assert range_config["state"] == "clamped"
    assert range_config["valid"] is False
    assert range_config["clamped"] is True
    assert range_config["error"] == "above_max"

    monkeypatch.setenv("BLUEPRINTS_KANBAN_AUTOMATION_MAX_SCAN_ITEMS", "not-an-int")
    error_config = routes_personal._work_automation_idle_worker_config()
    error_range = error_config["range_config"]["max_scan_items"]
    assert (
        error_config["max_scan_items"] == routes_personal.KANBAN_AUTOMATION_DEFAULT_MAX_SCAN_ITEMS
    )
    assert error_range["state"] == "error"
    assert error_range["error"] == "not_an_integer"
    assert error_range["raw_value"] == "not-an-int"


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
    assert "kanban_review_processor_markers" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_review_processor_markers") == "marker_id"
    assert "kanban_review_processor_failure_events" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_review_processor_failure_events") == "failure_event_id"
    assert "kanban_agent_hints" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_agent_hints") == "hint_id"
    assert "kanban_agent_sessions" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_agent_sessions") == "session_id"
    assert "kanban_priority_recommendations" in routes_sync._ALLOWED_TABLES
    assert routes_sync._pk_for_table("kanban_priority_recommendations") == "recommendation_id"

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-root",
                title="Step 16 root board item",
                body="Root board proof\n\nSecond paragraph",
                state_id="todo",
                priority_id="high",
                goal_flag=True,
                automation_excluded=True,
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
    assert root["automation_excluded"] is True
    assert root["body_excerpt"] == "Root board proof\n\nSecond paragraph"
    assert root["search"]["metadata"]["vector"]["turbo_vec_ready"] is True
    assert root["vector"]["index_key"] == "kanban_items:work-root"

    empty_priorities = asyncio.run(routes_personal.get_work_priorities())
    assert empty_priorities["source"] == "empty"
    assert empty_priorities["scope_id"] == "kanban"
    assert empty_priorities["recommendations"] == []

    saved_priorities = asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[
                    routes_personal.WorkPriorityRecommendationInput(
                        item_id="work-root",
                        title="Board priority proof",
                        summary="Use managed recommendations only.",
                        reason="Priority recommendations are skill/profile-managed, not computed as a substitute manager decision.",
                        score=98.0,
                        metadata={"proof": "pytest"},
                    )
                ],
                strategy_version="codex-skill-managed-test-v1",
                generated_at="2026-06-18T10:15:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="priority-replace",
            ),
        )
    )
    assert saved_priorities["source"] == "managed"
    assert saved_priorities["scope_id"] == "kanban"
    assert saved_priorities["count"] == 1
    assert saved_priorities["recommendations"][0]["canonical_code"] == (
        "xarta-kanban:item:work-root"
    )
    assert saved_priorities["recommendations"][0]["metadata"] == {"proof": "pytest"}
    root_detail_after_priorities = asyncio.run(routes_personal.get_work_item_detail("work-root"))
    assert "priorities" not in root_detail_after_priorities
    assert "priorities" not in root_detail_after_priorities["counts"]

    board = asyncio.run(routes_personal.get_work_root_board())
    todo_column = next(
        column for column in board["board"]["columns"] if column["state"]["state_id"] == "todo"
    )
    assert [item["item_id"] for item in todo_column["items"]] == ["work-root"]
    assert todo_column["items"][0]["goal_flag"] is True
    assert todo_column["items"][0]["automation_excluded"] is True

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

    worker_parent = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-worker-parent",
                title="Worker parent guard parent",
                actor="codex-test",
                source_surface="pytest",
                request_id="worker-parent-create",
            )
        )
    )["item"]
    worker_target_parent = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-worker-target-parent",
                title="Worker parent guard target",
                actor="codex-test",
                source_surface="pytest",
                request_id="worker-target-parent-create",
            )
        )
    )["item"]
    worker_child = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-worker-child",
                parent_item_id=worker_parent["item_id"],
                title="Worker parent guard child",
                actor="codex-test",
                source_surface="pytest",
                request_id="worker-child-create",
            )
        )
    )["item"]
    with pytest.raises(routes_personal.HTTPException) as worker_blocked_without_blocker:
        asyncio.run(
            routes_personal.move_work_item(
                worker_child["item_id"],
                routes_personal.WorkItemMoveRequest(
                    state_id="blocked",
                    actor="kanban-idle-worker",
                    source_surface="kanban-automation-idle-worker",
                    request_id="worker-child-lane-move-without-blocker",
                    run_id="worker-parent-guard",
                ),
            )
        )
    assert worker_blocked_without_blocker.value.status_code == 409
    assert worker_blocked_without_blocker.value.detail["error"] == (
        "kanban_automation_blocked_leaf_requires_blocker"
    )

    worker_lane_move_result = asyncio.run(
        routes_personal.move_work_item(
            worker_child["item_id"],
            routes_personal.WorkItemMoveRequest(
                state_id="blocked",
                blocker_title="Worker lane move blocker",
                blocker_body="Worker blocked moves must leave visible blocker detail.",
                actor="kanban-idle-worker",
                source_surface="kanban-automation-idle-worker",
                request_id="worker-child-lane-move",
                run_id="worker-parent-guard",
            ),
        )
    )
    worker_lane_move = worker_lane_move_result["item"]
    assert worker_lane_move["parent_item_id"] == worker_parent["item_id"]
    assert worker_lane_move["state_id"] == "blocked"
    assert worker_lane_move_result["created_blocker"]["status"] == "open"

    with pytest.raises(routes_personal.HTTPException) as worker_reparent_blocked:
        asyncio.run(
            routes_personal.move_work_item(
                worker_child["item_id"],
                routes_personal.WorkItemMoveRequest(
                    parent_item_id=worker_target_parent["item_id"],
                    state_id="blocked",
                    actor="kanban-idle-worker",
                    source_surface="kanban-automation-idle-worker",
                    request_id="worker-child-reparent-blocked",
                    run_id="worker-parent-guard",
                ),
            )
        )
    assert worker_reparent_blocked.value.status_code == 403
    assert worker_reparent_blocked.value.detail["error"] == (
        "kanban_idle_worker_parent_change_forbidden"
    )

    operator_reparent = asyncio.run(
        routes_personal.move_work_item(
            worker_child["item_id"],
            routes_personal.WorkItemMoveRequest(
                parent_item_id=worker_target_parent["item_id"],
                state_id="blocked",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="worker-child-operator-reparent",
            ),
        )
    )["item"]
    assert operator_reparent["parent_item_id"] == worker_target_parent["item_id"]

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
    root_rollup_with_open_issue_payload = asyncio.run(
        routes_personal.get_work_item_rollup("work-root")
    )
    root_rollup_with_open_issue = root_rollup_with_open_issue_payload["rollup"]
    assert root_rollup_with_open_issue["issues"]["open"] == 1
    root_and_child_rollups = asyncio.run(
        routes_personal.get_work_item_rollups(item_id=["work-root", "work-child", "work-root"])
    )
    assert root_and_child_rollups["count"] == 2
    assert set(root_and_child_rollups["rollups"]) == {"work-root", "work-child"}
    assert root_and_child_rollups["rollups"]["work-root"] == root_rollup_with_open_issue
    allowed_visible_card_ids = [f"visible-card-{index}" for index in range(200)]
    assert len(routes_personal._clean_work_rollup_item_ids(allowed_visible_card_ids)) == 200
    with pytest.raises(routes_personal.HTTPException) as exc_info:
        routes_personal._clean_work_rollup_item_ids(
            [f"visible-card-{index}" for index in range(201)]
        )
    assert exc_info.value.status_code == 400
    assert "200 item_id values" in exc_info.value.detail

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

    with pytest.raises(routes_personal.HTTPException) as duplicate_discussion:
        asyncio.run(
            routes_personal.create_work_discussion(
                "work-root",
                routes_personal.WorkDiscussionCreateRequest(
                    discussion_id="discussion-step18",
                    body=discussion_body,
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="discussion-create-replay",
                ),
            )
        )
    assert duplicate_discussion.value.status_code == 409

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
                automation_excluded=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="work-root-rename",
            ),
        )
    )["item"]
    assert renamed_root["title"] == "Step 16 Root Board Renamed"
    assert renamed_root["goal_flag"] is False
    assert renamed_root["automation_excluded"] is False
    assert renamed_root["body_excerpt"] == "Root board proof\n\nRenamed paragraph"
    assert (
        conn.execute("SELECT goal_flag FROM kanban_items WHERE item_id='work-root'").fetchone()[
            "goal_flag"
        ]
        == 0
    )
    assert (
        conn.execute(
            "SELECT automation_excluded FROM kanban_items WHERE item_id='work-root'"
        ).fetchone()["automation_excluded"]
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
        "replace_priority_recommendations",
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
        "kanban_priority_recommendations",
    }.issubset(sync_tables)


def test_work_kanban_read_routes_delegate_to_kanban_store_boundary(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="store-root",
                title="Store root",
                body="Store boundary route proof",
                state_id="todo",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="store-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="store-child",
                parent_item_id="store-root",
                title="Store child",
                body="Child board proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="store-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[
                    routes_personal.WorkPriorityRecommendationInput(
                        item_id="store-root",
                        title="Store priority",
                        summary="Managed recommendation survives the store boundary.",
                        reason="The route should hydrate recommendations through the store.",
                        score=88.0,
                    )
                ],
                strategy_version="store-boundary-test-v1",
                generated_at="2026-07-01T08:30:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="store-priorities-replace",
            )
        )
    )

    calls = {
        "config": 0,
        "board": 0,
        "item_detail": 0,
        "priority_recommendations": 0,
        "rollup": 0,
        "rollups": 0,
        "multi_rollups": 0,
    }
    original_config = routes_personal.KanbanStore.config
    original_board = routes_personal.KanbanStore.board
    original_item_detail = routes_personal.KanbanStore.item_detail
    original_priority_recommendations = routes_personal.KanbanStore.priority_recommendations
    original_rollup = routes_personal.KanbanStore.rollup
    original_rollups = routes_personal.KanbanStore.rollups

    def spy_config(self):
        calls["config"] += 1
        return original_config(self)

    def spy_board(self, *args, **kwargs):
        calls["board"] += 1
        return original_board(self, *args, **kwargs)

    def spy_item_detail(self, *args, **kwargs):
        calls["item_detail"] += 1
        return original_item_detail(self, *args, **kwargs)

    def spy_priority_recommendations(self, *args, **kwargs):
        calls["priority_recommendations"] += 1
        return original_priority_recommendations(self, *args, **kwargs)

    def spy_rollup(self, *args, **kwargs):
        calls["rollup"] += 1
        return original_rollup(self, *args, **kwargs)

    def spy_rollups(self, *args, **kwargs):
        calls["rollups"] += 1
        if args and len(args[0]) > 1:
            calls["multi_rollups"] += 1
        return original_rollups(self, *args, **kwargs)

    monkeypatch.setattr(routes_personal.KanbanStore, "config", spy_config)
    monkeypatch.setattr(routes_personal.KanbanStore, "board", spy_board)
    monkeypatch.setattr(routes_personal.KanbanStore, "item_detail", spy_item_detail)
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "priority_recommendations",
        spy_priority_recommendations,
    )
    monkeypatch.setattr(routes_personal.KanbanStore, "rollup", spy_rollup)
    monkeypatch.setattr(routes_personal.KanbanStore, "rollups", spy_rollups)

    config = asyncio.run(routes_personal.get_work_config())
    board = asyncio.run(routes_personal.get_work_root_board())
    child_board = asyncio.run(routes_personal.get_work_child_board("store-root"))
    detail = asyncio.run(routes_personal.get_work_item_detail("store-root"))
    priorities = asyncio.run(routes_personal.get_work_priorities())
    single_rollup = asyncio.run(routes_personal.get_work_item_rollup("store-root"))
    batch_rollups = asyncio.run(
        routes_personal.get_work_item_rollups(item_id=["store-root", "store-child"])
    )

    assert [state["state_id"] for state in config["states"]] == [
        "backlog",
        "todo",
        "doing",
        "blocked",
        "done",
    ]
    root_todo_items = next(
        column for column in board["board"]["columns"] if column["state"]["state_id"] == "todo"
    )["items"]
    assert [item["item_id"] for item in root_todo_items] == ["store-root"]
    assert child_board["board"]["parent"]["item_id"] == "store-root"
    assert child_board["board"]["breadcrumbs"][0]["item_id"] == "store-root"
    assert detail["counts"]["children"] == 1
    assert detail["counts"]["todos"] == 1
    assert priorities["recommendations"][0]["canonical_code"] == "xarta-kanban:item:store-root"
    assert single_rollup["rollup"]["items"]["total"] == 2
    assert batch_rollups["rollups"]["store-root"]["items"]["total"] == 2
    assert batch_rollups["rollups"]["store-child"]["items"]["total"] == 1
    assert calls == {
        "config": 1,
        "board": 2,
        "item_detail": 1,
        "priority_recommendations": 1,
        "rollup": 4,
        "rollups": 4,
        "multi_rollups": 1,
    }


def test_work_kanban_core_write_routes_delegate_to_kanban_store_boundary(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    calls = {
        "insert_item_row": 0,
        "update_item_row": 0,
        "move_item_row": 0,
        "archive_item_row": 0,
        "priority_recommendation_rows": 0,
        "upsert_priority_recommendation": 0,
        "delete_priority_recommendation": 0,
    }
    original_insert = routes_personal.KanbanStore.insert_item_row
    original_update = routes_personal.KanbanStore.update_item_row
    original_move = routes_personal.KanbanStore.move_item_row
    original_archive = routes_personal.KanbanStore.archive_item_row
    original_priority_rows = routes_personal.KanbanStore.priority_recommendation_rows
    original_upsert_priority = routes_personal.KanbanStore.upsert_priority_recommendation
    original_delete_priority = routes_personal.KanbanStore.delete_priority_recommendation

    def spy_insert(self, *args, **kwargs):
        calls["insert_item_row"] += 1
        return original_insert(self, *args, **kwargs)

    def spy_update(self, *args, **kwargs):
        calls["update_item_row"] += 1
        return original_update(self, *args, **kwargs)

    def spy_move(self, *args, **kwargs):
        calls["move_item_row"] += 1
        return original_move(self, *args, **kwargs)

    def spy_archive(self, *args, **kwargs):
        calls["archive_item_row"] += 1
        return original_archive(self, *args, **kwargs)

    def spy_priority_rows(self, *args, **kwargs):
        calls["priority_recommendation_rows"] += 1
        return original_priority_rows(self, *args, **kwargs)

    def spy_upsert_priority(self, *args, **kwargs):
        calls["upsert_priority_recommendation"] += 1
        return original_upsert_priority(self, *args, **kwargs)

    def spy_delete_priority(self, *args, **kwargs):
        calls["delete_priority_recommendation"] += 1
        return original_delete_priority(self, *args, **kwargs)

    monkeypatch.setattr(routes_personal.KanbanStore, "insert_item_row", spy_insert)
    monkeypatch.setattr(routes_personal.KanbanStore, "update_item_row", spy_update)
    monkeypatch.setattr(routes_personal.KanbanStore, "move_item_row", spy_move)
    monkeypatch.setattr(routes_personal.KanbanStore, "archive_item_row", spy_archive)
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "priority_recommendation_rows",
        spy_priority_rows,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "upsert_priority_recommendation",
        spy_upsert_priority,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "delete_priority_recommendation",
        spy_delete_priority,
    )

    sync_before = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]
    create_payload = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="write-boundary-root",
                title="Write boundary root",
                body="Core write boundary proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-create",
            )
        )
    )
    assert create_payload["item"]["item_id"] == "write-boundary-root"

    update_payload = asyncio.run(
        routes_personal.update_work_item(
            "write-boundary-root",
            routes_personal.WorkItemUpdateRequest(
                title="Write boundary root updated",
                body="Updated through the core write boundary.",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-update",
            ),
        )
    )
    assert update_payload["item"]["title"] == "Write boundary root updated"
    assert update_payload["item"]["priority_id"] == "high"

    move_payload = asyncio.run(
        routes_personal.move_work_item(
            "write-boundary-root",
            routes_personal.WorkItemMoveRequest(
                state_id="backlog",
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-move",
            ),
        )
    )
    assert move_payload["item"]["state_id"] == "backlog"

    priorities_payload = asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[
                    routes_personal.WorkPriorityRecommendationInput(
                        item_id="write-boundary-root",
                        title="Write boundary priority",
                        summary="Priority writes should pass through the store.",
                        reason="Store-boundary proof",
                        score=91.0,
                    )
                ],
                strategy_version="write-boundary-test-v1",
                generated_at="2026-07-01T10:45:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-priority-upsert",
            )
        )
    )
    assert priorities_payload["recommendations"][0]["item_id"] == "write-boundary-root"
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_priority_recommendations WHERE item_id=?",
            ("write-boundary-root",),
        ).fetchone()[0]
        == 1
    )

    empty_priorities_payload = asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[],
                strategy_version="write-boundary-test-v1",
                generated_at="2026-07-01T10:46:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-priority-delete",
            )
        )
    )
    assert empty_priorities_payload["recommendations"] == []
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_priority_recommendations WHERE item_id=?",
            ("write-boundary-root",),
        ).fetchone()[0]
        == 0
    )

    archive_payload = asyncio.run(
        routes_personal.archive_work_item(
            "write-boundary-root",
            routes_personal.WorkItemActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="write-boundary-archive",
            ),
        )
    )
    assert archive_payload["item"]["status"] == "archived"
    assert calls == {
        "insert_item_row": 1,
        "update_item_row": 1,
        "move_item_row": 1,
        "archive_item_row": 1,
        "priority_recommendation_rows": 2,
        "upsert_priority_recommendation": 1,
        "delete_priority_recommendation": 1,
    }
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] > sync_before
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {"kanban_items", "kanban_priority_recommendations", "kanban_audit_log"}.issubset(
        sync_tables
    )


def test_work_kanban_mutation_routes_offload_blocking_work(monkeypatch):
    calls = []

    async def fake_run_personal_sync_work(func, *args, **kwargs):
        calls.append((func.__name__, args, kwargs))
        return {"ok": True, "worker": func.__name__}

    monkeypatch.setattr(
        routes_personal,
        "_run_personal_sync_work",
        fake_run_personal_sync_work,
    )

    create_body = routes_personal.WorkItemCreateRequest(
        item_id="offload-create",
        title="Offload create",
        actor="codex-test",
        source_surface="pytest",
    )
    update_body = routes_personal.WorkItemUpdateRequest(
        title="Offload update",
        actor="codex-test",
        source_surface="pytest",
    )
    detail_body = routes_personal.WorkItemDetailDocumentUpdateRequest(
        body="# Offload detail",
        actor="codex-test",
        source_surface="pytest",
    )
    review_body = routes_personal.WorkItemDetailDocumentUpdateRequest(
        body="## Offload review",
        actor="codex-test",
        source_surface="pytest",
    )
    archive_body = routes_personal.WorkItemActionRequest(
        actor="codex-test",
        source_surface="pytest",
    )

    assert asyncio.run(routes_personal.create_work_item(create_body)) == {
        "ok": True,
        "worker": "_create_work_item_sync",
    }
    assert asyncio.run(routes_personal.update_work_item("offload-create", update_body)) == {
        "ok": True,
        "worker": "_update_work_item_sync",
    }
    assert asyncio.run(
        routes_personal.update_work_item_detail_document("offload-create", detail_body)
    ) == {
        "ok": True,
        "worker": "_update_work_item_detail_document_sync",
    }
    assert asyncio.run(
        routes_personal.update_work_item_review_document("offload-create", review_body)
    ) == {
        "ok": True,
        "worker": "_update_work_item_review_document_sync",
    }
    assert asyncio.run(routes_personal.archive_work_item("offload-create", archive_body)) == {
        "ok": True,
        "worker": "_archive_work_item_sync",
    }
    assert calls == [
        ("_create_work_item_sync", (create_body,), {}),
        ("_update_work_item_sync", ("offload-create", update_body), {}),
        ("_update_work_item_detail_document_sync", ("offload-create", detail_body), {}),
        ("_update_work_item_review_document_sync", ("offload-create", review_body), {}),
        ("_archive_work_item_sync", ("offload-create", archive_body), {}),
    ]


def test_work_kanban_discussion_writes_delegate_to_kanban_store_boundary(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    calls = {
        "create_discussion_row": 0,
        "update_discussion_row": 0,
        "update_discussion_provenance": 0,
        "delete_discussion_row": 0,
    }
    original_create = routes_personal.KanbanStore.create_discussion_row
    original_update = routes_personal.KanbanStore.update_discussion_row
    original_update_provenance = routes_personal.KanbanStore.update_discussion_provenance
    original_delete = routes_personal.KanbanStore.delete_discussion_row

    def spy_create(self, *args, **kwargs):
        calls["create_discussion_row"] += 1
        return original_create(self, *args, **kwargs)

    def spy_update(self, *args, **kwargs):
        calls["update_discussion_row"] += 1
        return original_update(self, *args, **kwargs)

    def spy_update_provenance(self, *args, **kwargs):
        calls["update_discussion_provenance"] += 1
        return original_update_provenance(self, *args, **kwargs)

    def spy_delete(self, *args, **kwargs):
        calls["delete_discussion_row"] += 1
        return original_delete(self, *args, **kwargs)

    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "create_discussion_row",
        spy_create,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "update_discussion_row",
        spy_update,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "update_discussion_provenance",
        spy_update_provenance,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "delete_discussion_row",
        spy_delete,
    )

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="discussion-boundary-root",
                title="Discussion boundary root",
                body="Discussion write boundary proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-boundary-root-create",
            )
        )
    )

    sync_before = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]
    created = asyncio.run(
        routes_personal.create_work_discussion(
            "discussion-boundary-root",
            routes_personal.WorkDiscussionCreateRequest(
                discussion_id="discussion-boundary-proof",
                body="Initial discussion body\n\n- keep markdown",
                author="codex-test",
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-boundary-create",
            ),
        )
    )
    assert created["discussion"]["body"] == "Initial discussion body\n\n- keep markdown"
    assert created["discussion"]["document"]["file_ref"]["path"].endswith(
        "/discussions/discussion-boundary-proof.md"
    )

    updated = asyncio.run(
        routes_personal.update_work_discussion(
            "discussion-boundary-proof",
            routes_personal.WorkDiscussionUpdateRequest(
                body="Edited discussion body\n\n```text\nstill markdown\n```",
                status="done",
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-boundary-update",
            ),
        )
    )
    assert updated["discussion"]["status"] == "done"
    assert "still markdown" in updated["discussion"]["body"]

    deleted = asyncio.run(
        routes_personal.delete_work_discussion(
            "discussion-boundary-proof",
            routes_personal.WorkItemActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="discussion-boundary-delete",
            ),
        )
    )
    assert deleted["ok"] is True
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_discussions WHERE discussion_id=?",
            ("discussion-boundary-proof",),
        ).fetchone()[0]
        == 0
    )
    assert not (
        tmp_path
        / "kanban"
        / "discussion-boundary-root/items/discussion-boundary-root/discussions/discussion-boundary-proof.md"
    ).exists()
    assert calls == {
        "create_discussion_row": 1,
        "update_discussion_row": 1,
        "update_discussion_provenance": 2,
        "delete_discussion_row": 1,
    }
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] > sync_before
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {"kanban_discussions", "kanban_audit_log"}.issubset(sync_tables)


def test_work_kanban_detail_review_documents_delegate_to_kanban_store_boundary(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    calls = {
        "item_detail_document": 0,
        "item_review_document": 0,
        "write_item_detail_document": 0,
        "write_item_review_document": 0,
    }
    original_detail = routes_personal.KanbanStore.item_detail_document
    original_review = routes_personal.KanbanStore.item_review_document
    original_write_detail = routes_personal.KanbanStore.write_item_detail_document
    original_write_review = routes_personal.KanbanStore.write_item_review_document

    def spy_detail(self, *args, **kwargs):
        calls["item_detail_document"] += 1
        return original_detail(self, *args, **kwargs)

    def spy_review(self, *args, **kwargs):
        calls["item_review_document"] += 1
        return original_review(self, *args, **kwargs)

    def spy_write_detail(self, *args, **kwargs):
        calls["write_item_detail_document"] += 1
        return original_write_detail(self, *args, **kwargs)

    def spy_write_review(self, *args, **kwargs):
        calls["write_item_review_document"] += 1
        return original_write_review(self, *args, **kwargs)

    monkeypatch.setattr(routes_personal.KanbanStore, "item_detail_document", spy_detail)
    monkeypatch.setattr(routes_personal.KanbanStore, "item_review_document", spy_review)
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "write_item_detail_document",
        spy_write_detail,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "write_item_review_document",
        spy_write_review,
    )

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="doc-boundary-root",
                title="Document boundary root",
                body="Document boundary proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="doc-boundary-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item_agent_session(
            "doc-boundary-root",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="kanban-agent-session-doc-boundary",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                source_surface="pytest-session",
                summary="Document boundary feedback proof",
                actor="codex-test",
                request_id="doc-boundary-session-create",
            ),
        )
    )

    sync_before = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]
    detail = asyncio.run(
        routes_personal.update_work_item_detail_document(
            "doc-boundary-root",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="# Detail boundary\n\nStored as file-backed markdown.",
                actor="codex-test",
                source_surface="pytest",
                request_id="doc-boundary-detail-update",
            ),
        )
    )
    assert detail["detail_document"]["body"].startswith("# Detail boundary")

    review = asyncio.run(
        routes_personal.update_work_item_review_document(
            "doc-boundary-root",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="## Review boundary\n\nInitial review text.",
                actor="codex-test",
                source_surface="pytest",
                request_id="doc-boundary-review-update",
            ),
        )
    )
    assert review["review_document"]["metadata"]["body_hash"].startswith("sha256:")

    item_detail = asyncio.run(routes_personal.get_work_item_detail("doc-boundary-root"))
    assert item_detail["detail_document"]["body"].startswith("# Detail boundary")
    assert item_detail["review_document"]["body"].startswith("## Review boundary")

    feedback = asyncio.run(
        routes_personal.append_work_item_review_feedback(
            "doc-boundary-root",
            routes_personal.WorkReviewFeedbackCaptureRequest(
                feedback_id="kanban-feedback-doc-boundary",
                feedback="Document boundary feedback should keep review scheduling intact.",
                session_id="kanban-agent-session-doc-boundary",
                capture_source="explicit_command",
                actor="codex-test",
                source_surface="pytest",
                request_id="doc-boundary-feedback-capture",
            ),
        )
    )
    assert "Document boundary feedback" in feedback["review_document"]["body"]
    assert feedback["review_processor"]["queued"] is True

    assert calls == {
        "item_detail_document": 1,
        "item_review_document": 2,
        "write_item_detail_document": 1,
        "write_item_review_document": 2,
    }
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] > sync_before
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {"kanban_audit_log", "kanban_review_processor_markers"}.issubset(sync_tables)


def test_work_kanban_relationship_writes_delegate_to_kanban_store_boundary(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    calls = {
        "create_item_link_row": 0,
        "blocker_row": 0,
        "upsert_blocker_row": 0,
        "upsert_item_commit_row": 0,
    }
    original_create_link = routes_personal.KanbanStore.create_item_link_row
    original_blocker_row = routes_personal.KanbanStore.blocker_row
    original_upsert_blocker = routes_personal.KanbanStore.upsert_blocker_row
    original_upsert_commit = routes_personal.KanbanStore.upsert_item_commit_row

    def spy_create_link(self, *args, **kwargs):
        calls["create_item_link_row"] += 1
        return original_create_link(self, *args, **kwargs)

    def spy_blocker_row(self, *args, **kwargs):
        calls["blocker_row"] += 1
        return original_blocker_row(self, *args, **kwargs)

    def spy_upsert_blocker(self, *args, **kwargs):
        calls["upsert_blocker_row"] += 1
        return original_upsert_blocker(self, *args, **kwargs)

    def spy_upsert_commit(self, *args, **kwargs):
        calls["upsert_item_commit_row"] += 1
        return original_upsert_commit(self, *args, **kwargs)

    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "create_item_link_row",
        spy_create_link,
    )
    monkeypatch.setattr(routes_personal.KanbanStore, "blocker_row", spy_blocker_row)
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "upsert_blocker_row",
        spy_upsert_blocker,
    )
    monkeypatch.setattr(
        routes_personal.KanbanStore,
        "upsert_item_commit_row",
        spy_upsert_commit,
    )

    for item_id, title in (
        ("relationship-boundary-source", "Relationship boundary source"),
        ("relationship-boundary-target", "Relationship boundary target"),
    ):
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    title=title,
                    body="Relationship write boundary proof.",
                    state_id="todo",
                    priority_id="medium",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id=f"{item_id}-create",
                )
            )
        )

    sync_before = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]
    link = asyncio.run(
        routes_personal.create_work_item_link(
            "relationship-boundary-source",
            routes_personal.WorkItemLinkCreateRequest(
                target_item_id="relationship-boundary-target",
                link_type="depends_on",
                metadata={"link_id": "kanban-link-relationship-boundary"},
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-link-create",
            ),
        )
    )["link"]
    assert link["link_id"] == "kanban-link-relationship-boundary"

    replay = asyncio.run(
        routes_personal.create_work_item_link(
            "relationship-boundary-source",
            routes_personal.WorkItemLinkCreateRequest(
                target_item_id="relationship-boundary-target",
                link_type="depends_on",
                metadata={"proof": "same semantic typed link"},
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-link-replay-first",
            ),
        )
    )
    replay_again = asyncio.run(
        routes_personal.create_work_item_link(
            "relationship-boundary-source",
            routes_personal.WorkItemLinkCreateRequest(
                target_item_id="relationship-boundary-target",
                link_type="depends_on",
                metadata={"proof": "same semantic typed link"},
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-link-replay-second",
            ),
        )
    )
    assert replay["idempotent_replay"] is False
    assert replay_again["idempotent_replay"] is True
    assert replay_again["link"]["link_id"] == replay["link"]["link_id"]
    assert (
        conn.execute(
            """
            SELECT COUNT(*) FROM kanban_item_links
            WHERE source_item_id='relationship-boundary-source'
              AND target_item_id='relationship-boundary-target'
              AND link_type='depends_on'
            """
        ).fetchone()[0]
        == 2
    )

    blocker = asyncio.run(
        routes_personal.create_work_blocker(
            routes_personal.WorkBlockerUpsertRequest(
                blocker_id="blocker-relationship-boundary",
                item_id="relationship-boundary-source",
                title="Relationship boundary blocker",
                body="Explicit blocker row should pass through the store.",
                blocked_by_ref="kanban_items:relationship-boundary-target",
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-blocker-create",
            )
        )
    )["blocker"]
    assert blocker["status"] == "open"

    resolved = asyncio.run(
        routes_personal.update_work_blocker(
            "blocker-relationship-boundary",
            routes_personal.WorkBlockerUpsertRequest(
                item_id="relationship-boundary-source",
                title="Relationship boundary blocker resolved",
                body="Resolved through the same store row method.",
                status="resolved",
                blocked_by_ref="kanban_items:relationship-boundary-target",
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-blocker-update",
            ),
        )
    )["blocker"]
    assert resolved["status"] == "resolved"

    blocked_leaf = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="relationship-boundary-blocked-leaf",
                title="Relationship boundary blocked leaf",
                body="Automation blocked leaf with explicit blocker payload.",
                state_id="blocked",
                blocker_title="Blocked relationship proof",
                blocker_body="Guard-created blocker row stays visible.",
                blocked_by_ref="kanban_items:relationship-boundary-source",
                actor="kanban-idle-worker",
                source_surface="kanban-automation-idle-worker",
                request_id="relationship-boundary-blocked-leaf-create",
                run_id="relationship-boundary-blocked-leaf-run",
            )
        )
    )
    assert blocked_leaf["created_blocker"]["status"] == "open"

    sha = "c" * 40
    commit = asyncio.run(
        routes_personal.record_work_item_commit(
            "relationship-boundary-source",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha,
                message_subject="Relationship boundary commit",
                branch="main",
                metadata={"relationship_boundary": "insert"},
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-commit-create",
            ),
        )
    )["commit"]
    assert commit["sha"] == sha

    updated_commit = asyncio.run(
        routes_personal.record_work_item_commit(
            "relationship-boundary-source",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha,
                message_subject="Relationship boundary commit updated",
                branch="main",
                metadata={"relationship_boundary": "updated"},
                actor="codex-test",
                source_surface="pytest",
                request_id="relationship-boundary-commit-update",
            ),
        )
    )["commit"]
    assert updated_commit["message_subject"] == "Relationship boundary commit updated"
    assert updated_commit["metadata"]["relationship_boundary"] == "updated"

    assert calls == {
        "create_item_link_row": 2,
        "blocker_row": 6,
        "upsert_blocker_row": 3,
        "upsert_item_commit_row": 2,
    }
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] > sync_before
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {
        "kanban_item_links",
        "kanban_blockers",
        "kanban_item_commits",
        "kanban_audit_log",
    }.issubset(sync_tables)


def test_work_kanban_read_selector_shadow_candidate_parity_and_rollback(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="selector-root",
                title="Selector Root",
                body="Read selector root proof",
                state_id="doing",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="selector-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="selector-child",
                parent_item_id="selector-root",
                title="Selector Child",
                body="Read selector child proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="selector-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[
                    routes_personal.WorkPriorityRecommendationInput(
                        item_id="selector-root",
                        title="Selector priority",
                        summary="Read selector should preserve priority payloads.",
                        reason="Candidate-shadow reads should match SQLite reads.",
                        score=87.0,
                    )
                ],
                strategy_version="read-selector-test-v1",
                generated_at="2026-07-01T10:30:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="selector-priorities-replace",
            )
        )
    )

    sqlite_config = load_kanban_datastore_config({})
    shadow_config = load_kanban_datastore_config(
        {"BLUEPRINTS_KANBAN_READ_STORE": "candidate-shadow"}
    )
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", sqlite_config)
    sqlite_payloads = {
        "config": asyncio.run(routes_personal.get_work_config()),
        "root_board": asyncio.run(routes_personal.get_work_root_board()),
        "child_board": asyncio.run(routes_personal.get_work_child_board("selector-root")),
        "detail": asyncio.run(routes_personal.get_work_item_detail("selector-root")),
        "priorities": asyncio.run(routes_personal.get_work_priorities()),
    }

    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", shadow_config)
    status = asyncio.run(routes_personal.get_work_kanban_datastore_status())
    assert status["reads"]["store"] == "candidate-shadow"
    assert status["reads"]["candidate_enabled"] is True
    assert status["reads"]["candidate_mode"] == "sqlite-shadow"
    assert status["writes"]["store"] == "sqlite"
    assert status["writes"]["candidate_enabled"] is False
    shadow_payloads = {
        "config": asyncio.run(routes_personal.get_work_config()),
        "root_board": asyncio.run(routes_personal.get_work_root_board()),
        "child_board": asyncio.run(routes_personal.get_work_child_board("selector-root")),
        "detail": asyncio.run(routes_personal.get_work_item_detail("selector-root")),
        "priorities": asyncio.run(routes_personal.get_work_priorities()),
    }
    assert shadow_payloads == sqlite_payloads

    before_sync_rows = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="selector-post-shadow-write",
                parent_item_id="selector-root",
                title="Selector Post Shadow Write",
                body="Writes still go to SQLite while candidate-shadow reads are enabled.",
                state_id="todo",
                priority_id="low",
                actor="codex-test",
                source_surface="pytest",
                request_id="selector-post-shadow-write",
            )
        )
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_items WHERE item_id='selector-post-shadow-write'"
        ).fetchone()[0]
        == 1
    )
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] > before_sync_rows
    shadow_after_write = asyncio.run(routes_personal.get_work_child_board("selector-root"))

    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", sqlite_config)
    sqlite_after_rollback = asyncio.run(routes_personal.get_work_child_board("selector-root"))
    assert shadow_after_write == sqlite_after_rollback
    child_ids = [
        item["item_id"]
        for column in sqlite_after_rollback["board"]["columns"]
        for item in column["items"]
    ]
    assert "selector-post-shadow-write" in child_ids


def test_work_kanban_datastore_status_and_bootstrap_are_disabled_by_default(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("CREATE INDEX idx_kanban_items_pytest ON kanban_items(state_id)")

    before_sync_rows = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]

    status = asyncio.run(routes_personal.get_work_kanban_datastore_status())
    assert status["ok"] is True
    assert status["active_store"] == "sqlite"
    assert status["reads"] == {
        "store": "sqlite",
        "candidate_enabled": False,
        "candidate_mode": "disabled",
        "read_store_env": "BLUEPRINTS_KANBAN_READ_STORE",
    }
    assert status["writes"]["store"] == "sqlite"
    assert status["writes"]["candidate_enabled"] is False
    assert status["writes"]["local_writes_allowed"] is True
    assert status["writes"]["replica_write_policy"] == "reject"
    assert status["candidate"]["backend"] == "postgres"
    assert status["candidate"]["bootstrap_dry_run_supported"] is True
    assert status["candidate"]["bootstrap_apply_supported"] is False
    assert status["candidate"]["read_shadow_supported"] is True
    assert status["candidate"]["read_shadow_persistent"] is False
    assert status["candidate"]["read_postgres_supported"] is True
    assert status["candidate"]["read_postgres_persistent"] is True
    assert status["safety"]["sqlite_rows_retained"] is True
    assert status["distribution"]["schema"] == "xarta.kanban.postgres_distribution.v1"
    assert status["distribution"]["current_node_id"] == "test-node"
    assert status["distribution"]["owner_node_id"] == ""
    assert status["distribution"]["this_node_role"] == "sqlite-peer"
    assert status["distribution"]["authority"]["multi_writer_supported"] is False
    assert status["distribution"]["authority"]["writes_authoritative_postgres"] is False
    assert (
        status["distribution"]["fleet"]["kanban_sqlite_row_sync"]
        == "normal-sqlite-sync-queue-while-sqlite-active"
    )
    assert status["distribution"]["fleet"]["expected_peer_active_store"] == "sqlite"
    assert status["distribution"]["operator_safety"]["old_sqlite_rows_deletion_allowed"] is False
    assert status["distribution"]["operator_safety"]["sqlite_distribution_allowed"] is False
    assert {
        "kanban_items",
        "kanban_priority_recommendations",
        "kanban_review_processor_markers",
        "kanban_review_processor_failure_events",
        "kanban_agent_sessions",
    }.issubset(set(status["tables"]))

    plan = asyncio.run(
        routes_personal.bootstrap_work_kanban_datastore(
            routes_personal.WorkDatastoreBootstrapRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="kanban-datastore-bootstrap-dry-run",
            )
        )
    )
    assert plan["ok"] is True
    assert plan["dry_run"] is True
    assert plan["applied"] is False
    assert plan["active_store"] == "sqlite"
    assert plan["candidate_backend"] == "postgres"
    assert plan["apply_supported"] is False
    assert plan["support_tables"] == ["settings"]
    assert plan["statement_count"] > len(plan["tables"])
    assert plan["safety"]["live_reads_changed"] is False
    assert plan["safety"]["live_writes_changed"] is False
    assert plan["safety"]["sync_queue_rows_created"] is False
    assert {
        "kanban_items",
        "kanban_priority_recommendations",
        "kanban_review_processor_markers",
        "kanban_review_processor_failure_events",
        "kanban_agent_sessions",
    }.issubset(set(plan["tables"]))
    statement_by_name = {statement["name"]: statement for statement in plan["statements"]}
    assert "CREATE TABLE IF NOT EXISTS kanban_items" in statement_by_name["kanban_items"]["sql"]
    assert statement_by_name["idx_kanban_items_pytest"]["type"] == "index"
    assert (
        "CREATE TABLE IF NOT EXISTS kanban_review_processor_markers"
        in statement_by_name["kanban_review_processor_markers"]["sql"]
    )
    assert plan["audit"]["actor"] == "codex-test"
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] == before_sync_rows

    with pytest.raises(routes_personal.HTTPException) as apply_error:
        asyncio.run(
            routes_personal.bootstrap_work_kanban_datastore(
                routes_personal.WorkDatastoreBootstrapRequest(
                    apply=True,
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="kanban-datastore-bootstrap-apply-rejected",
                )
            )
        )
    assert apply_error.value.status_code == 400
    assert "is not configured" in str(apply_error.value.detail)


def test_work_kanban_datastore_config_rejects_unsafe_modes():
    assert load_kanban_datastore_config({}).active_store == "sqlite"
    assert load_kanban_datastore_config({}).read_store == "sqlite"
    assert (
        load_kanban_datastore_config(
            {"BLUEPRINTS_KANBAN_READ_STORE": "candidate-shadow"}
        ).read_store
        == "candidate-shadow"
    )
    postgres_candidate = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_READ_STORE": "candidate-postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
        }
    )
    assert postgres_candidate.read_store == "candidate-postgres"
    assert postgres_candidate.candidate_database_url_configured is True

    active_postgres = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
        }
    )
    assert active_postgres.active_store == "postgres"
    assert active_postgres.read_store == "postgres"
    assert active_postgres.candidate_database_url_configured is True
    assert active_postgres.postgres_replica_write_policy == "reject"

    with pytest.raises(KanbanDatastoreConfigError, match="required"):
        load_kanban_datastore_config({"BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres"})

    with pytest.raises(KanbanDatastoreConfigError, match="read stores"):
        load_kanban_datastore_config({"BLUEPRINTS_KANBAN_READ_STORE": "mongo"})

    with pytest.raises(KanbanDatastoreConfigError, match="invalid"):
        load_kanban_datastore_config({"BLUEPRINTS_KANBAN_CANDIDATE_STORE_BACKEND": "mongo"})

    with pytest.raises(KanbanDatastoreConfigError, match="replica write policies"):
        load_kanban_datastore_config(
            {"BLUEPRINTS_KANBAN_POSTGRES_REPLICA_WRITE_POLICY": "multi-writer"}
        )


def test_work_kanban_datastore_status_reports_postgres_read_replica(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    replica_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_NODE_ID": "peer-node",
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
            "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID": "owner-node",
        }
    )
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", replica_config)

    status = asyncio.run(routes_personal.get_work_kanban_datastore_status())

    assert status["active_store"] == "postgres"
    assert status["reads"]["store"] == "postgres"
    assert status["writes"]["store"] == "postgres"
    assert status["writes"]["local_writes_allowed"] is False
    assert status["writes"]["replica_write_policy"] == "reject"
    assert status["writes"]["write_authority"] == "postgres-read-replica-local-writes-rejected"
    assert status["distribution"]["current_node_id"] == "peer-node"
    assert status["distribution"]["owner_node_id"] == "owner-node"
    assert status["distribution"]["this_node_role"] == "postgres-read-replica"
    assert status["distribution"]["authority"]["this_node_is_owner"] is False
    assert status["distribution"]["authority"]["reads_authoritative_postgres"] is True
    assert status["distribution"]["authority"]["writes_authoritative_postgres"] is False
    assert (
        status["distribution"]["authority"]["write_authority"]
        == "postgres-read-replica-local-writes-rejected"
    )
    assert status["distribution"]["authority"]["replica_local_writes_rejected"] is True
    assert status["distribution"]["fleet"]["expected_peer_active_store"] == "postgres"
    assert status["distribution"]["fleet"]["peer_postgres_required_now"] is True
    assert (
        "SQLite is not a distribution mechanism"
        in status["distribution"]["fleet"]["data_distribution"]
    )
    assert status["distribution"]["operator_safety"]["sqlite_distribution_allowed"] is False


def test_work_kanban_active_postgres_write_through_and_read_preference(monkeypatch, tmp_path):
    conn = _make_conn()
    postgres_conn = _make_conn()
    conn.executemany(
        "INSERT INTO nodes (node_id) VALUES (?)",
        [("peer-a",), ("peer-b",)],
    )
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(conn))
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    active_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
            "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID": "test-node",
        }
    )
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", active_config)

    class FakePostgres:
        def __init__(self, sqlite_conn):
            self.conn = sqlite_conn
            self.begun = False
            self.committed = False

        def begin(self):
            self.begun = True

        def execute(self, sql, params=None):
            if params is None:
                return self.conn.execute(sql)
            return self.conn.execute(sql, params)

        def executemany(self, sql, seq_of_params):
            return self.conn.executemany(sql, list(seq_of_params))

        def commit(self):
            self.committed = True
            self.conn.commit()

        def rollback(self):
            self.conn.rollback()

        def close(self):
            return None

    fake_connections: list[FakePostgres] = []

    def fake_postgres_connection(_database_url):
        fake = FakePostgres(postgres_conn)
        fake_connections.append(fake)
        return fake

    monkeypatch.setattr(routes_personal, "postgres_candidate_connection", fake_postgres_connection)

    status = asyncio.run(routes_personal.get_work_kanban_datastore_status())
    assert status["active_store"] == "postgres"
    assert status["reads"]["store"] == "postgres"
    assert status["writes"]["store"] == "postgres"
    assert status["writes"]["local_writes_allowed"] is True
    assert status["writes"]["write_authority"] == "owner-local-postgres"
    assert status["safety"]["sqlite_writes_retained"] is False
    assert status["safety"]["sqlite_archive_mirror_retained"] is False
    assert status["distribution"]["owner_node_id"] == "test-node"
    assert status["distribution"]["this_node_role"] == "postgres-owner"
    assert status["distribution"]["authority"]["this_node_is_owner"] is True
    assert status["distribution"]["authority"]["reads_authoritative_postgres"] is True
    assert status["distribution"]["authority"]["writes_authoritative_postgres"] is True
    assert (
        status["distribution"]["fleet"]["kanban_sqlite_row_sync"]
        == "disabled-for-kanban-tables-while-owner-postgres-active"
    )
    assert status["distribution"]["fleet"]["expected_peer_active_store"] == "postgres"
    assert status["distribution"]["fleet"]["peer_postgres_required_now"] is True
    assert status["distribution"]["rollback"]["sqlite_archive_mirror_retained"] is False
    assert status["distribution"]["operator_safety"]["old_sqlite_rows_deletion_allowed"] is False

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="active-postgres-write",
                title="Active Postgres Write",
                body="This write must land in Postgres.",
                state_id="todo",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="active-postgres-write",
            )
        )
    )
    assert fake_connections and fake_connections[-1].committed is True
    assert (
        postgres_conn.execute(
            "SELECT COUNT(*) FROM kanban_items WHERE item_id='active-postgres-write'"
        ).fetchone()[0]
        == 1
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_items WHERE item_id='active-postgres-write'"
        ).fetchone()[0]
        == 0
    )
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] == 0

    postgres_conn.execute(
        "UPDATE kanban_items SET title='Postgres authoritative title' WHERE item_id='active-postgres-write'"
    )
    board = asyncio.run(routes_personal.get_work_root_board())
    root_items = [
        item
        for column in board["board"]["columns"]
        for item in column["items"]
        if item["item_id"] == "active-postgres-write"
    ]
    assert root_items[0]["title"] == "Postgres authoritative title"


def test_settings_upsert_sql_is_unambiguous_for_kanban_postgres_support_setting():
    class CaptureConn:
        def __init__(self):
            self.sql = ""
            self.params = ()

        def execute(self, sql, params=None):
            self.sql = sql
            self.params = params or ()

    capture = CaptureConn()
    app_db.set_setting(
        capture,
        routes_personal.KANBAN_SHOW_TEST_ENTRIES_SETTING,
        "true",
        None,
    )

    statement, _args = kanban_postgres.prepare_sqlite_query_for_postgres(
        capture.sql,
        capture.params,
    )

    assert "COALESCE(excluded.description, settings.description)" in statement


def test_work_kanban_postgres_read_replica_rejects_local_writes(monkeypatch, tmp_path):
    conn = _make_conn()
    postgres_conn = _make_conn()
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(conn))
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    replica_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_NODE_ID": "peer-node",
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
            "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID": "owner-node",
        }
    )
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", replica_config)
    postgres_opened = False

    class FakePostgres:
        def __init__(self, sqlite_conn):
            self.conn = sqlite_conn

        def begin(self):
            return None

        def execute(self, sql, params=None):
            if params is None:
                return self.conn.execute(sql)
            return self.conn.execute(sql, params)

        def executemany(self, sql, seq_of_params):
            return self.conn.executemany(sql, list(seq_of_params))

        def commit(self):
            self.conn.commit()

        def rollback(self):
            self.conn.rollback()

        def close(self):
            return None

    def fake_postgres_connection(_database_url):
        nonlocal postgres_opened
        postgres_opened = True
        return FakePostgres(postgres_conn)

    monkeypatch.setattr(routes_personal, "postgres_candidate_connection", fake_postgres_connection)

    with pytest.raises(routes_personal.HTTPException) as exc:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id="replica-write-rejected",
                    title="Replica Write Rejected",
                    body="This write must not land on a non-owner Postgres node.",
                    state_id="todo",
                    priority_id="high",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="replica-write-rejected",
                )
            )
        )

    assert exc.value.status_code == 409
    assert "Postgres Kanban read replica" in str(exc.value.detail)
    assert "owner-node" in str(exc.value.detail)
    assert postgres_opened is True
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_items WHERE item_id='replica-write-rejected'"
        ).fetchone()[0]
        == 0
    )


def test_work_kanban_datastore_parity_uses_postgres_report_for_active_postgres(monkeypatch):
    conn = _make_conn()
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(conn))
    active_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
        }
    )
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", active_config)
    called: dict[str, bool] = {}

    def fake_postgres_report(conn_arg, **_kwargs):
        called["postgres_report"] = True
        return {
            "ok": True,
            "candidate": {
                "live_reads_enabled": True,
                "live_writes_enabled": True,
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "kanban_postgres_parity_report",
        fake_postgres_report,
    )

    report = asyncio.run(routes_personal.get_work_kanban_datastore_parity(sample_limit=1))

    assert called["postgres_report"] is True
    assert report["candidate"]["live_reads_enabled"] is True
    assert report["candidate"]["live_writes_enabled"] is True


def _seed_kanban_shadow_parity_dataset(monkeypatch, tmp_path) -> tuple[sqlite3.Connection, Path]:
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    kanban_root = _patch_kanban_backup_env(monkeypatch, tmp_path, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", kanban_root)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="parity-root",
                title="Parity Root",
                body="Root parity proof",
                state_id="doing",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
                request_id="parity-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="parity-child",
                parent_item_id="parity-root",
                title="Parity Child",
                body="Child board parity proof",
                state_id="todo",
                priority_id="medium",
                actor="codex-test",
                source_surface="pytest",
                request_id="parity-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_discussion(
            "parity-root",
            routes_personal.WorkDiscussionCreateRequest(
                discussion_id="discussion-parity-root",
                body="Discussion parity proof",
                author="codex-test",
                actor="codex-test",
                source_surface="pytest",
                request_id="parity-discussion-create",
            ),
        )
    )
    asyncio.run(
        routes_personal.replace_work_priorities(
            routes_personal.WorkPriorityRecommendationsReplaceRequest(
                recommendations=[
                    routes_personal.WorkPriorityRecommendationInput(
                        item_id="parity-root",
                        title="Parity priority",
                        summary="Priority parity proof",
                        reason="Shadow store should hydrate the recommendation item.",
                        score=91.0,
                    )
                ],
                strategy_version="shadow-parity-test-v1",
                generated_at="2026-07-01T10:00:00Z",
                actor="codex-test",
                source_surface="pytest",
                request_id="parity-priorities-replace",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item_agent_session(
            "parity-root",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="session-parity-root",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                source_surface="pytest-session",
                summary="Parity session proof",
                actor="codex-test",
                request_id="parity-session-create",
            ),
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_review_decisions (
            decision_id, item_id, processor_kind, decision_type, title, summary,
            status, provider_mode
        )
        VALUES (
            'decision-parity-root', 'parity-root', 'review', 'decision',
            'Review parity', 'Decision parity proof', 'recorded', 'local'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_markers (
            marker_id, item_id, processor_kind, document_type, document_ref,
            status, provider_mode, decision_id, source_hash
        )
        VALUES (
            'marker-parity-root', 'parity-root', 'preprocessing', 'item-body',
            'kanban_items:parity-root', 'ready', 'local',
            'decision-parity-root', 'sha256:parity-marker'
        )
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_failure_events (
            failure_event_id, marker_id, item_id, processor_kind, document_type,
            error_class, error_message, provider_mode, status
        )
        VALUES (
            'failure-parity-root', 'marker-parity-root', 'parity-root',
            'preprocessing', 'item-body', 'TransientParityError',
            'covered by shadow parity', 'local', 'superseded'
        )
        """
    )
    routes_kanban_backups.create_kanban_backup(kind="manual")
    return conn, kanban_root


def test_work_kanban_datastore_shadow_parity_reports_api_shapes(monkeypatch, tmp_path):
    conn, kanban_root = _seed_kanban_shadow_parity_dataset(monkeypatch, tmp_path)
    before_sync_rows = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]

    report = asyncio.run(routes_personal.get_work_kanban_datastore_parity(sample_limit=5))

    assert report["ok"] is True, {
        "failed_comparisons": report["failed_comparisons"],
        "hash_mismatches": report["tables"]["hash_mismatches"],
        "postload_totals": report["migration"]["postload_preview"]["totals"],
        "safety": report["safety"],
    }
    assert report["candidate"]["shadow_backend"] == "sqlite-shadow"
    assert report["candidate"]["live_reads_enabled"] is False
    assert report["candidate"]["live_writes_enabled"] is False
    assert report["safety"]["live_reads_changed"] is False
    assert report["safety"]["live_writes_changed"] is False
    assert report["safety"]["sync_queue_rows_created"] is False
    assert report["safety"]["sync_queue_count_before"] == before_sync_rows
    assert report["safety"]["sync_queue_count_after"] == before_sync_rows
    assert report["migration"]["preload_preview"]["totals"]["inserted"] == sum(
        report["tables"]["live_counts"].values()
    )
    assert report["migration"]["preload_preview"]["totals"]["conflicts"] == 0
    assert report["migration"]["postload_preview"]["idempotent"] is True
    assert report["migration"]["postload_preview"]["totals"]["unchanged"] == sum(
        report["tables"]["live_counts"].values()
    )
    assert report["tables"]["hash_mismatches"] == []
    assert "sync_queue" not in report["tables"]["included"]
    assert report["tables"]["live_counts"]["kanban_review_processor_markers"] == 1
    assert report["tables"]["live_counts"]["kanban_review_processor_failure_events"] == 1
    assert report["tables"]["live_counts"]["kanban_agent_sessions"] == 1
    assert report["coverage"]["backup_package_count"] == 1
    assert report["coverage"]["kanban_file_count"] >= 1
    assert report["coverage"]["automation_marker_count"] == 1
    assert report["coverage"]["automation_failure_event_count"] == 1
    assert report["coverage"]["agent_session_count"] == 1
    assert report["coverage"]["review_decision_count"] == 1
    assert "parity-root" in report["samples"]["item_ids"]
    assert report["samples"]["child_board_parent_id"] == "parity-root"
    assert set(report["coverage"]["api_shapes"]).issuperset(
        {
            "config",
            "root_board",
            "child_board",
            "item_detail:parity-root",
            "priorities",
            "automation_markers",
            "automation_failure_events",
            "agent_sessions",
            "review_decisions",
            "backup_packages",
            "file_backed_docs",
        }
    )
    assert all(comparison["ok"] for comparison in report["api_comparisons"])
    assert kanban_root.exists()


def test_work_kanban_postgres_parity_reports_active_writes(monkeypatch, tmp_path):
    conn, kanban_root = _seed_kanban_shadow_parity_dataset(monkeypatch, tmp_path)
    candidate = kanban_parity.kanban_shadow_candidate_connection(
        conn,
        support_setting_keys=(routes_personal.KANBAN_SHOW_TEST_ENTRIES_SETTING,),
    )
    active_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
        }
    )
    monkeypatch.setattr(kanban_parity, "postgres_candidate_connection", lambda _url: candidate)

    report = kanban_parity.kanban_postgres_parity_report(
        conn,
        depth_limit=routes_personal.KANBAN_DEPTH_LIMIT,
        show_test_entries_setting=routes_personal.KANBAN_SHOW_TEST_ENTRIES_SETTING,
        agent_working_out_tag=routes_personal.KANBAN_AGENT_WORKING_OUT_TAG,
        kanban_root=kanban_root,
        backup_dir=Path(routes_kanban_backups.cfg.KANBAN_BACKUP_DIR),
        datastore_config=active_config,
        sample_limit=5,
    )

    assert report["ok"] is True
    assert report["candidate"]["live_reads_enabled"] is True
    assert report["candidate"]["live_writes_enabled"] is True
    assert report["safety"]["live_reads_enabled"] is True
    assert report["safety"]["live_writes_enabled"] is True


def test_work_kanban_datastore_shadow_parity_detects_candidate_drift(monkeypatch, tmp_path):
    conn, kanban_root = _seed_kanban_shadow_parity_dataset(monkeypatch, tmp_path)

    def drift_candidate(candidate: sqlite3.Connection) -> None:
        candidate.execute(
            "UPDATE kanban_items SET title='Candidate Drift' WHERE item_id='parity-root'"
        )

    report = kanban_parity.kanban_shadow_parity_report(
        conn,
        depth_limit=routes_personal.KANBAN_DEPTH_LIMIT,
        show_test_entries_setting=routes_personal.KANBAN_SHOW_TEST_ENTRIES_SETTING,
        agent_working_out_tag=routes_personal.KANBAN_AGENT_WORKING_OUT_TAG,
        kanban_root=kanban_root,
        backup_dir=Path(routes_personal.cfg.KANBAN_BACKUP_DIR),
        candidate_backend=routes_personal.cfg.KANBAN_DATASTORE_CONFIG.candidate_backend,
        sample_limit=5,
        candidate_mutator=drift_candidate,
    )

    assert report["ok"] is False
    assert "kanban_items" in report["tables"]["hash_mismatches"]
    assert report["migration"]["postload_preview"]["tables"]["kanban_items"]["updated"] == 1
    assert report["migration"]["postload_preview"]["totals"]["conflicts"] == 1
    assert "root_board" in report["failed_comparisons"]
    assert "item_detail:parity-root" in report["failed_comparisons"]
    assert report["safety"]["live_reads_changed"] is False
    assert report["safety"]["live_writes_changed"] is False
    assert report["safety"]["sync_queue_rows_created"] is False


def test_work_kanban_backup_package_covers_datastore_tables_and_file_hashes(monkeypatch, tmp_path):
    conn = _make_conn()
    kanban_root = _patch_kanban_backup_env(monkeypatch, tmp_path, conn)
    detail_path = kanban_root / "database-topic-ancestor" / "items" / "backup-item" / "detail.md"
    detail_path.parent.mkdir(parents=True)
    detail_path.write_text("# Backup Item\n\nProof file.\n", encoding="utf-8")
    (kanban_root / "backups" / "ignored.txt").write_text("exclude me", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO kanban_items (item_id, title, state_id, status)
        VALUES ('backup-item', 'Backup Item', 'todo', 'open')
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_failure_events (
            failure_event_id, marker_id, item_id, error_class, error_message
        )
        VALUES ('failure-1', 'marker-1', 'backup-item', 'test', 'covered')
        """
    )

    created = routes_kanban_backups.create_kanban_backup(kind="manual")
    manifest = created["manifest"]

    assert manifest["purpose"] == "kanban-datastore-migration-export"
    assert manifest["sync_queue_included"] is False
    assert "sync_queue" not in manifest["included_tables"]
    assert "kanban_review_processor_failure_events" in manifest["included_tables"]
    assert manifest["table_counts"]["kanban_review_processor_failure_events"] == 1
    assert manifest["table_data_sha256"]
    assert manifest["table_hashes"]["kanban_review_processor_failure_events"]
    assert manifest["file_count"] == 1
    rel_detail = "database-topic-ancestor/items/backup-item/detail.md"
    assert (
        manifest["file_hashes"][rel_detail] == hashlib.sha256(detail_path.read_bytes()).hexdigest()
    )

    listed = routes_kanban_backups.list_kanban_backups()
    assert listed["backups"][0].filename == created["backup"].filename
    assert listed["backups"][0].sha256 == ""
    listed_with_hashes = routes_kanban_backups.list_kanban_backups(include_hashes=True)
    assert listed_with_hashes["backups"][0].sha256

    validation = routes_kanban_backups.validate_kanban_backup(created["backup"].filename)
    assert validation["ok"] is True
    assert validation["dry_run"]["idempotent"] is True
    assert validation["dry_run"]["sync_queue_included"] is False


def test_work_kanban_backup_import_dry_run_reports_conflicts_and_restore(monkeypatch, tmp_path):
    conn = _make_conn()
    kanban_root = _patch_kanban_backup_env(monkeypatch, tmp_path, conn)
    detail_path = kanban_root / "database-topic-ancestor" / "items" / "backup-item" / "detail.md"
    detail_path.parent.mkdir(parents=True)
    detail_path.write_text("original detail\n", encoding="utf-8")
    conn.execute(
        """
        INSERT INTO kanban_items (item_id, title, state_id, status)
        VALUES ('backup-item', 'Original Title', 'todo', 'open')
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_failure_events (
            failure_event_id, marker_id, item_id, error_class, error_message
        )
        VALUES ('failure-1', 'marker-1', 'backup-item', 'test', 'covered')
        """
    )
    created = routes_kanban_backups.create_kanban_backup(kind="manual")
    filename = created["backup"].filename

    detail_path.write_text("changed detail\n", encoding="utf-8")
    conn.execute("UPDATE kanban_items SET title='Changed Title' WHERE item_id='backup-item'")
    conn.execute(
        """
        INSERT INTO kanban_items (item_id, title, state_id, status)
        VALUES ('post-export-item', 'Post Export', 'todo', 'open')
        """
    )
    conn.execute(
        "DELETE FROM kanban_review_processor_failure_events WHERE failure_event_id='failure-1'"
    )
    before_sync_rows = conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0]

    dry_run = routes_kanban_backups.import_kanban_backup(filename, apply=False)
    assert dry_run["applied"] is False
    assert dry_run["dry_run"]["tables"]["kanban_items"]["updated"] == 1
    assert dry_run["dry_run"]["tables"]["kanban_items"]["deleted"] == 1
    assert dry_run["dry_run"]["tables"]["kanban_items"]["conflicts"] == 2
    assert dry_run["dry_run"]["tables"]["kanban_review_processor_failure_events"]["inserted"] == 1
    assert dry_run["dry_run"]["sync_queue_rows_created"] is False
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] == before_sync_rows

    applied = routes_kanban_backups.import_kanban_backup(
        filename,
        apply=True,
        restore_files=True,
        backup_before_import=False,
    )
    assert applied["ok"] is True
    assert applied["applied"] is True
    assert applied["dry_run"]["tables"]["kanban_items"]["conflicts"] == 2
    restored = conn.execute("SELECT title FROM kanban_items WHERE item_id='backup-item'").fetchone()
    assert restored["title"] == "Original Title"
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_items WHERE item_id='post-export-item'"
        ).fetchone()[0]
        == 0
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM kanban_review_processor_failure_events "
            "WHERE failure_event_id='failure-1'"
        ).fetchone()[0]
        == 1
    )
    assert detail_path.read_text(encoding="utf-8") == "original detail\n"


def test_work_kanban_backup_package_routes_retire_when_postgres_active(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_kanban_backup_env(monkeypatch, tmp_path, conn)
    active_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
            "BLUEPRINTS_NODE_ID": "test-node",
            "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID": "test-node",
        }
    )
    monkeypatch.setattr(routes_kanban_backups.cfg, "KANBAN_DATASTORE_CONFIG", active_config)

    with pytest.raises(Exception) as exc:
        routes_kanban_backups.list_kanban_backups()

    assert getattr(exc.value, "status_code", None) == 410
    assert exc.value.detail["replacement_api"] == "/api/v1/personal/kanban/postgres"
    assert exc.value.detail["sqlite_kanban_storage_reintroduced"] is False


def test_work_kanban_postgres_export_validate_import_and_distribute(monkeypatch, tmp_path):
    export_dir = tmp_path / "postgres-exports"
    helper = tmp_path / "xarta-kanban-postgres-distribute"
    helper.write_text("#!/bin/sh\n", encoding="utf-8")
    counts = {table: 0 for table in routes_kanban_postgres.KANBAN_DATASTORE_TABLES}
    counts["kanban_items"] = 2
    counts["kanban_priority_recommendations"] = 1
    calls: list[dict[str, object]] = []
    active_config = load_kanban_datastore_config(
        {
            "BLUEPRINTS_KANBAN_DATASTORE_MODE": "postgres",
            "BLUEPRINTS_KANBAN_CANDIDATE_DATABASE_URL": "postgresql://example.invalid/db",
            "BLUEPRINTS_NODE_ID": "test-node",
            "BLUEPRINTS_KANBAN_POSTGRES_OWNER_NODE_ID": "test-node",
        }
    )

    def fake_counts(database_url: str | None = None) -> dict[str, int]:
        assert database_url is None
        return dict(counts)

    def fake_run_command(
        command: list[str],
        *,
        timeout: int = 120,
        stdin_path: Path | None = None,
        stdout_path: Path | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        calls.append(
            {
                "command": command,
                "stdin_path": str(stdin_path or ""),
                "stdout_path": str(stdout_path or ""),
                "timeout": timeout,
            }
        )
        if "pg_dump" in command and stdout_path is not None:
            stdout_path.write_text("-- kanban postgres dump\n", encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if "pg_isready" in command:
            return subprocess.CompletedProcess(command, 0, b"accepting connections\n", b"")
        if "createdb" in command or "dropdb" in command:
            return subprocess.CompletedProcess(command, 0, b"", b"")
        if "psql" in command and "-c" in command:
            stdout = "\n".join(f"{table}\t{count}" for table, count in counts.items()) + "\n"
            return subprocess.CompletedProcess(command, 0, stdout.encode("utf-8"), b"")
        if "psql" in command and stdin_path is not None:
            assert stdin_path.exists()
            return subprocess.CompletedProcess(command, 0, b"restored\n", b"")
        if command and command[0] == str(helper):
            return subprocess.CompletedProcess(
                command,
                0,
                b'{"schema":"xarta.kanban.postgres_fleet_distribution.v1","ok":true}\n',
                b"",
            )
        return subprocess.CompletedProcess(command, 0, b"", b"")

    monkeypatch.setattr(routes_kanban_postgres.cfg, "KANBAN_POSTGRES_EXPORT_DIR", str(export_dir))
    monkeypatch.setattr(routes_kanban_postgres.cfg, "KANBAN_DATASTORE_CONFIG", active_config)
    monkeypatch.setattr(routes_kanban_postgres.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(routes_kanban_postgres.cfg, "NODE_NAME", "Test Node")
    monkeypatch.setattr(
        routes_kanban_postgres.cfg,
        "NODES_DATA",
        [
            {"node_id": "test-node", "display_name": "Test Node", "active": True},
            {"node_id": "peer-node", "display_name": "Peer Node", "active": True},
        ],
    )
    monkeypatch.setattr(routes_kanban_postgres, "_postgres_table_counts", fake_counts)
    monkeypatch.setattr(routes_kanban_postgres, "_run_command", fake_run_command)
    monkeypatch.setattr(routes_kanban_postgres, "_distribution_helper", lambda: helper)

    created = routes_kanban_postgres.create_kanban_postgres_export(kind="manual")
    filename = created["export"]["filename"]

    assert created["manifest"]["storage"] == "postgres"
    assert created["manifest"]["sqlite_backup_package"] is False
    assert created["manifest"]["sqlite_kanban_rows_included"] is False
    assert (export_dir / filename).exists()
    assert (export_dir / f"{filename}.json").exists()

    status = routes_kanban_postgres.get_kanban_postgres_status()
    assert status["ok"] is True
    assert status["role"] == "postgres-owner"
    assert status["latest_export"]["filename"] == filename
    assert status["table_counts"]["kanban_priority_recommendations"] == 1

    validation = routes_kanban_postgres.validate_kanban_postgres_export(filename)
    assert validation["ok"] is True
    assert validation["restored_table_counts"]["kanban_items"] == 2

    dry_run = routes_kanban_postgres.import_kanban_postgres_export(
        filename,
        apply=False,
        backup_before_import=False,
    )
    assert dry_run["applied"] is False
    assert dry_run["table_counts_before"] == dry_run["table_counts_after"]

    applied = routes_kanban_postgres.import_kanban_postgres_export(
        filename,
        apply=True,
        backup_before_import=False,
    )
    assert applied["applied"] is True
    assert applied["table_counts_after"]["kanban_items"] == 2

    distribution = routes_kanban_postgres.distribute_kanban_postgres(
        routes_kanban_postgres.KanbanPostgresDistributionRequest(
            target_node_id="peer-node",
            dry_run=True,
        )
    )
    assert distribution["ok"] is True
    assert distribution["target"] == "peer-node"
    assert distribution["result"]["ok"] is True
    assert all("kanban-backup" not in str(call["command"]) for call in calls)


def test_work_preprocessing_scoped_decomposition_reparent_guard(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    for item_id, parent_item_id, title, body in [
        (
            "work-scoped-source",
            None,
            "Scoped source",
            "Concrete implementation request. Proof path: scoped reparent stays inside subtree.",
        ),
        ("work-scoped-a", "work-scoped-source", "Scoped A", "Child A."),
        ("work-scoped-b", "work-scoped-source", "Scoped B", "Child B."),
        ("work-scoped-outside", None, "Outside", "Outside target."),
    ]:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    parent_item_id=parent_item_id,
                    title=title,
                    body=body,
                    state_id="todo",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id=f"{item_id}-create",
                )
            )
        )

    result = routes_personal._work_preprocessing_scoped_decomposition_reparent(
        item_id="work-scoped-b",
        target_parent_item_id="work-scoped-a",
        source_item_id="work-scoped-source",
        marker_id="marker-scoped-source",
        actor="kanban-idle-worker",
        request_id="scoped-reparent-allowed",
        run_id="pytest-scoped-reparent",
        reason="move child under the more precise decomposition parent",
        operation_kind="preprocessing_reparent_child",
    )
    assert result["ok"] is True
    assert result["item"]["parent_item_id"] == "work-scoped-a"
    assert result["item"]["depth"] == 2

    with pytest.raises(routes_personal.HTTPException) as outside_target:
        routes_personal._work_preprocessing_scoped_decomposition_reparent(
            item_id="work-scoped-b",
            target_parent_item_id="work-scoped-outside",
            source_item_id="work-scoped-source",
            marker_id="marker-scoped-source",
            actor="kanban-idle-worker",
            request_id="scoped-reparent-outside",
            run_id="pytest-scoped-reparent",
            reason="try to move outside source subtree",
            operation_kind="preprocessing_reparent_child",
        )
    assert outside_target.value.status_code == 403
    assert outside_target.value.detail["error"] == (
        "kanban_preprocessing_scoped_move_outside_source_subtree"
    )

    with pytest.raises(routes_personal.HTTPException) as source_move:
        routes_personal._work_preprocessing_scoped_decomposition_reparent(
            item_id="work-scoped-source",
            target_parent_item_id="work-scoped-a",
            source_item_id="work-scoped-source",
            marker_id="marker-scoped-source",
            actor="kanban-idle-worker",
            request_id="scoped-reparent-source",
            run_id="pytest-scoped-reparent",
            reason="try to move the source item itself",
            operation_kind="preprocessing_reparent_child",
        )
    assert source_move.value.status_code == 403
    assert source_move.value.detail["error"] == "kanban_preprocessing_cannot_move_source_item"

    audit = asyncio.run(
        routes_personal.list_work_audit_log(
            item_id="work-scoped-b",
            action="preprocessing_decomposition_reparent_item",
        )
    )
    assert audit["count"] == 1
    metadata = audit["audit"][0]["metadata"]
    assert metadata["schema"] == routes_personal.KANBAN_PREPROCESSING_DECOMPOSITION_MOVE_SCHEMA
    assert metadata["marker_id"] == "marker-scoped-source"
    assert audit["audit"][0]["rollback"]["operation"] == "move_item"


def test_agent_completion_move_blocks_outstanding_work_but_operator_can_override(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-completion-parent",
                title="Completion parent",
                body="Parent with outstanding work",
                state_id="doing",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="completion-parent-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-completion-child",
                parent_item_id="work-completion-parent",
                title="Open child",
                body="Still open",
                state_id="todo",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="completion-child-create",
            )
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_blockers (blocker_id, item_id, title, status)
        VALUES ('blocker-completion', 'work-completion-parent', 'Blocked proof', 'open')
        """
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_markers (
            marker_id, item_id, processor_kind, document_type, status, queued_at
        )
        VALUES (
            'marker-completion', 'work-completion-child', 'review', 'review',
            'queued', '2026-06-27T10:00:00Z'
        )
        """
    )
    conn.commit()

    with pytest.raises(routes_personal.HTTPException) as raised:
        asyncio.run(
            routes_personal.move_work_item(
                "work-completion-parent",
                routes_personal.WorkItemMoveRequest(
                    state_id="done",
                    actor="codex",
                    source_surface="xarta-kanban-work",
                    request_id="agent-finish-parent",
                ),
            )
        )
    assert raised.value.status_code == 409
    detail = raised.value.detail
    assert detail["error"] == "kanban_agent_completion_blocked"
    assert {blocker["code"] for blocker in detail["blockers"]} == {
        "open_descendants",
        "open_blockers",
        "pending_processor_markers",
    }
    still_open = conn.execute(
        "SELECT state_id, status FROM kanban_items WHERE item_id='work-completion-parent'"
    ).fetchone()
    assert dict(still_open) == {"state_id": "doing", "status": "active"}

    operator_done = asyncio.run(
        routes_personal.move_work_item(
            "work-completion-parent",
            routes_personal.WorkItemMoveRequest(
                state_id="done",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="operator-finish-parent",
            ),
        )
    )["item"]
    assert operator_done["state_id"] == "done"
    assert operator_done["status"] == "done"


def test_agent_completion_update_blocks_pending_processor_markers(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-completion-marker",
                title="Completion marker",
                body="Pending marker should block agent Done",
                state_id="doing",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="completion-marker-create",
            )
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_review_processor_markers (
            marker_id, item_id, processor_kind, document_type, status, queued_at
        )
        VALUES (
            'marker-preprocess-completion', 'work-completion-marker',
            'preprocessing', 'context', 'queued', '2026-06-27T10:15:00Z'
        )
        """
    )
    conn.commit()

    with pytest.raises(routes_personal.HTTPException) as raised:
        asyncio.run(
            routes_personal.update_work_item(
                "work-completion-marker",
                routes_personal.WorkItemUpdateRequest(
                    state_id="done",
                    actor="codex",
                    source_surface="blueprints-work-management-skill",
                    request_id="agent-update-done",
                ),
            )
        )
    assert raised.value.status_code == 409
    detail = raised.value.detail
    assert detail["error"] == "kanban_agent_completion_blocked"
    assert [blocker["code"] for blocker in detail["blockers"]] == ["pending_processor_markers"]
    assert detail["blockers"][0]["items"][0]["processor_kind"] == "preprocessing"
    still_open = conn.execute(
        "SELECT state_id, status FROM kanban_items WHERE item_id='work-completion-marker'"
    ).fetchone()
    assert dict(still_open) == {"state_id": "doing", "status": "active"}

    operator_done = asyncio.run(
        routes_personal.update_work_item(
            "work-completion-marker",
            routes_personal.WorkItemUpdateRequest(
                state_id="done",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="operator-update-done",
            ),
        )
    )["item"]
    assert operator_done["state_id"] == "done"
    assert operator_done["status"] == "done"


def test_work_review_document_hash_only_updates_on_body_change(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    timestamps = iter(
        [
            "2026-06-27T03:00:00Z",
            "2026-06-27T03:01:00Z",
            "2026-06-27T03:02:00Z",
            "2026-06-27T03:03:00Z",
        ]
    )
    monkeypatch.setattr(
        routes_personal,
        "_utc_now_iso",
        lambda: next(timestamps, "2026-06-27T03:04:00Z"),
    )

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-hash",
                title="Review hash item",
                body="Review hash proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-hash-item-create",
            )
        )
    )
    review_body = "Operator Review: only changed content should bump the timestamp."
    first = asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-hash",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=review_body,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-hash-write-first",
            ),
        )
    )["review_document"]
    first_source = routes_personal._review_document_source(first)
    first_body_hash = first["metadata"]["body_hash"]
    assert first["updated_at"] == "2026-06-27T03:01:00Z"
    assert first_body_hash.startswith("sha256:")

    same = asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-hash",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=review_body,
                actor="codex-other",
                source_surface="pytest",
                request_id="review-hash-write-same",
            ),
        )
    )["review_document"]
    same_source = routes_personal._review_document_source(same)
    assert same["updated_at"] == first["updated_at"]
    assert same["metadata"]["body_hash"] == first_body_hash
    assert same["metadata"]["actor"] == "codex-test"
    assert same_source["document_source_hash"] == first_source["document_source_hash"]

    changed = asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-hash",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=f"{review_body}\n\nA new correction changes the review source.",
                actor="codex-other",
                source_surface="pytest",
                request_id="review-hash-write-changed",
            ),
        )
    )["review_document"]
    changed_source = routes_personal._review_document_source(changed)
    assert changed["updated_at"] > first["updated_at"]
    assert changed["metadata"]["body_hash"] != first_body_hash
    assert changed["metadata"]["actor"] == "codex-other"
    assert changed_source["document_source_hash"] != first_source["document_source_hash"]


def test_work_review_feedback_capture_appends_markdown_and_metadata(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    timestamps = iter(
        [
            "2026-06-27T04:00:00Z",
            "2026-06-27T04:01:00Z",
            "2026-06-27T04:02:00Z",
            "2026-06-27T04:03:00Z",
            "2026-06-27T04:04:00Z",
            "2026-06-27T04:05:00Z",
            "2026-06-27T04:06:00Z",
            "2026-06-27T04:07:00Z",
            "2026-06-27T04:08:00Z",
            "2026-06-27T04:09:00Z",
            "2026-06-27T04:10:00Z",
        ]
    )
    monkeypatch.setattr(
        routes_personal,
        "_utc_now_iso",
        lambda: next(timestamps, "2026-06-27T04:04:00Z"),
    )

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-feedback",
                title="Review feedback item",
                body="Feedback capture proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-item-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-feedback-child",
                parent_item_id="work-review-feedback",
                title="Review feedback child",
                body="Child card proof",
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item_agent_session(
            "work-review-feedback-child",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="kanban-agent-session-test",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                source_surface="pytest-session",
                summary="Started explicit feedback attribution proof",
                metadata={"slice": "feedback-session-attribution"},
                actor="codex-test",
                request_id="review-feedback-session-create",
            ),
        )
    )

    first = asyncio.run(
        routes_personal.append_work_item_review_feedback(
            "work-review-feedback",
            routes_personal.WorkReviewFeedbackCaptureRequest(
                feedback_id="kanban-feedback-one",
                feedback="Proceed with confidence and do not add a fallback path.",
                session_id="kanban-agent-session-test",
                capture_source="explicit_command",
                source_ref="discussion:operator-command",
                related_refs=["xarta-kanban:item:work-parent"],
                child_item_id="work-review-feedback-child",
                proof_refs=["git_commit:xarta/xarta-node@abcdef1", "discussion:proof-one"],
                outcome_ref="discussion:outcome-one",
                outcome_summary="Outcome accepted with tests, hooks, and linked commits.",
                metadata={"operator_intent": "durable-review-input"},
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-capture-one",
            ),
        )
    )
    first_doc = first["review_document"]
    assert first["feedback_entry"]["schema"] == routes_personal.KANBAN_REVIEW_FEEDBACK_SCHEMA
    assert first["feedback_entry"]["feedback_id"] == "kanban-feedback-one"
    assert first_doc["body"].startswith("## Operator Feedback")
    assert "### 2026-06-27T04:03:00Z - codex-test" in first_doc["body"]
    assert "> Proceed with confidence and do not add a fallback path." in first_doc["body"]
    assert "- Child card: `xarta-kanban:item:work-review-feedback-child`" in first_doc["body"]
    assert "- Session item: `xarta-kanban:item:work-review-feedback-child`" in first_doc["body"]
    assert "`git_commit:xarta/xarta-node@abcdef1`" in first_doc["body"]
    assert "- Outcome ref: `discussion:outcome-one`" in first_doc["body"]
    assert (
        "- Outcome summary: Outcome accepted with tests, hooks, and linked commits."
        in first_doc["body"]
    )
    operator_feedback = first_doc["metadata"]["operator_feedback"]
    assert operator_feedback["schema"] == routes_personal.KANBAN_REVIEW_FEEDBACK_COLLECTION_SCHEMA
    assert operator_feedback["count"] == 1
    first_entry = operator_feedback["entries"][0]
    assert first_entry["session_id"] == "kanban-agent-session-test"
    assert first_entry["capture_source"] == "explicit_command"
    assert first_entry["affected_item_id"] == "work-review-feedback"
    assert "feedback" not in first_entry
    assert first_entry["metadata"]["operator_intent"] == "durable-review-input"
    assert "kanban_agent_sessions:kanban-agent-session-test" in first_entry["affected_refs"]
    assert "xarta-kanban:item:work-review-feedback-child" in first_entry["affected_refs"]
    assert "git_commit:xarta/xarta-node@abcdef1" in first_entry["affected_refs"]
    attribution = first_entry["attribution"]
    assert attribution["schema"] == routes_personal.KANBAN_REVIEW_FEEDBACK_ATTRIBUTION_SCHEMA
    assert attribution["session_ref"] == "kanban_agent_sessions:kanban-agent-session-test"
    assert attribution["child_item_id"] == "work-review-feedback-child"
    assert attribution["proof_refs"] == [
        "git_commit:xarta/xarta-node@abcdef1",
        "discussion:proof-one",
    ]
    assert attribution["outcome_ref"] == "discussion:outcome-one"
    assert attribution["agent_session"]["item_id"] == "work-review-feedback-child"
    assert attribution["agent_session"]["metadata"]["slice"] == "feedback-session-attribution"
    first_processor = first["review_processor"]
    assert first_processor["schema"] == routes_personal.KANBAN_REVIEW_SCHEDULER_SCHEMA
    assert first_processor["action"] == "queued"
    assert first_processor["queued"] is True
    first_marker = first_processor["marker"]
    assert first_marker["status"] == "queued"
    assert first_marker["item_id"] == "work-review-feedback"
    assert first_marker["metadata"]["reason"] == "operator_feedback_captured"
    assert first_marker["metadata"]["feedback_id"] == "kanban-feedback-one"

    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-feedback",
                item_id="work-review-feedback",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-processor-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-feedback",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-feedback",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-processor-claim",
            )
        )
    )
    assert claimed["claimed"] is True
    assert claimed["marker"]["status"] == "processing"

    second = asyncio.run(
        routes_personal.append_work_item_review_feedback(
            "work-review-feedback",
            routes_personal.WorkReviewFeedbackCaptureRequest(
                feedback_id="kanban-feedback-two",
                feedback="Discussion-selected feedback also belongs in Review.",
                session_id="kanban-agent-session-test",
                capture_source="explicit_discussion",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-feedback-capture-two",
            ),
        )
    )
    second_doc = second["review_document"]
    entries = second_doc["metadata"]["operator_feedback"]["entries"]
    assert second_doc["metadata"]["operator_feedback"]["count"] == 2
    assert [entry["feedback_id"] for entry in entries] == [
        "kanban-feedback-one",
        "kanban-feedback-two",
    ]
    assert "Discussion-selected feedback also belongs in Review." in second_doc["body"]
    second_processor = second["review_processor"]
    assert second_processor["action"] == "queued"
    assert second_processor["queued"] is True
    second_marker = second_processor["marker"]
    assert second_marker["status"] == "queued"
    assert second_marker["attempt_count"] == 1
    assert second_marker["last_error"] == "review_changed_during_processing"
    assert second_marker["superseded_at"]
    assert second_marker["superseded_by_source_hash"] == second_marker["document_source_hash"]
    assert second_marker["metadata"]["superseded_processing_attempt"] is True
    assert second_marker["metadata"]["feedback_id"] == "kanban-feedback-two"
    assert second_marker["document_source_hash"] != first_marker["document_source_hash"]

    preserved = asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-feedback",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=second_doc["body"],
                actor="codex-other",
                source_surface="pytest",
                request_id="review-feedback-preserve-metadata",
            ),
        )
    )["review_document"]
    assert preserved["metadata"]["operator_feedback"]["count"] == 2
    assert preserved["metadata"]["actor"] == "codex-test"

    with pytest.raises(routes_personal.HTTPException) as excinfo:
        asyncio.run(
            routes_personal.append_work_item_review_feedback(
                "work-review-feedback",
                routes_personal.WorkReviewFeedbackCaptureRequest(
                    feedback="Sentiment alone must not become Review input.",
                    session_id="kanban-agent-session-test",
                    capture_source="sentiment",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="review-feedback-invalid-source",
                ),
            )
        )
    assert excinfo.value.status_code == 400

    with pytest.raises(routes_personal.HTTPException) as missing_session:
        asyncio.run(
            routes_personal.append_work_item_review_feedback(
                "work-review-feedback",
                routes_personal.WorkReviewFeedbackCaptureRequest(
                    feedback="A session label that does not exist must not be accepted.",
                    session_id="kanban-agent-session-missing",
                    capture_source="explicit_command",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="review-feedback-missing-session",
                ),
            )
        )
    assert missing_session.value.status_code == 404

    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "append_work_item_review_feedback" in audit_actions
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_markers" in sync_tables
    assert "kanban_audit_log" in sync_tables


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


def test_postgres_active_kanban_commit_link_skips_sqlite_sync_bookkeeping(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    conn.execute(
        """
        INSERT INTO kanban_items (item_id, title, body_excerpt, state_id, priority_id, tags_json)
        VALUES (
            'work-postgres-commit-link',
            'Postgres commit link',
            'Proof card',
            'doing',
            'high',
            '["kanban"]'
        )
        """
    )
    config = type("Config", (), {"active_store": "postgres"})()
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", config)

    def fail_increment_gen(*args, **kwargs):
        raise AssertionError("Postgres-active Kanban commit links must not increment sync_meta")

    monkeypatch.setattr(routes_personal, "increment_gen", fail_increment_gen)
    gen_before = conn.execute("SELECT value FROM sync_meta WHERE key='gen'").fetchone()[0]
    last_write_by_before = conn.execute(
        "SELECT value FROM sync_meta WHERE key='last_write_by'"
    ).fetchone()[0]

    sha = "d" * 40
    result = asyncio.run(
        routes_personal.record_work_item_commit(
            "work-postgres-commit-link",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha,
                message_subject="Postgres-active commit link proof",
                actor="codex-test",
                source_surface="pytest",
                request_id="postgres-active-commit-link",
            ),
        )
    )

    assert result["commit"]["sha"] == sha
    assert conn.execute("SELECT COUNT(*) FROM kanban_item_commits").fetchone()[0] == 1
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "record_work_commit" in audit_actions
    assert conn.execute("SELECT value FROM sync_meta WHERE key='gen'").fetchone()[0] == gen_before
    assert (
        conn.execute("SELECT value FROM sync_meta WHERE key='last_write_by'").fetchone()[0]
        == last_write_by_before
    )
    assert conn.execute("SELECT COUNT(*) FROM sync_queue").fetchone()[0] == 0


def test_sqlite_kanban_commit_link_keeps_generation_and_fanout(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    conn.execute(
        """
        INSERT INTO kanban_items (item_id, title, body_excerpt, state_id, priority_id, tags_json)
        VALUES (
            'work-sqlite-commit-link',
            'SQLite commit link',
            'Proof card',
            'doing',
            'high',
            '["kanban"]'
        )
        """
    )
    config = type("Config", (), {"active_store": "sqlite"})()
    monkeypatch.setattr(routes_personal.cfg, "KANBAN_DATASTORE_CONFIG", config)
    original_increment_gen = routes_personal.increment_gen
    gen_sources: list[str] = []

    def spy_increment_gen(conn_arg, source="human"):
        gen_sources.append(source)
        return original_increment_gen(conn_arg, source)

    monkeypatch.setattr(routes_personal, "increment_gen", spy_increment_gen)

    sha = "e" * 40
    result = asyncio.run(
        routes_personal.record_work_item_commit(
            "work-sqlite-commit-link",
            routes_personal.WorkItemCommitCreateRequest(
                repo_full_name="xarta/xarta-node",
                sha=sha,
                message_subject="SQLite commit link proof",
                actor="codex-test",
                source_surface="pytest",
                request_id="sqlite-commit-link",
            ),
        )
    )

    assert result["commit"]["sha"] == sha
    assert gen_sources == ["kanban-item-commit"]
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert {"kanban_item_commits", "kanban_audit_log"}.issubset(sync_tables)
    assert conn.execute("SELECT value FROM sync_meta WHERE key='gen'").fetchone()[0] == "1"


def test_work_automation_status_delegates_sync_payload_off_event_loop(monkeypatch):
    calls = []

    def fake_status(clean_item_id, limit, **kwargs):
        return {
            "ok": True,
            "schema": "xarta.kanban.automation_status.v1",
            "item_id": clean_item_id,
            "limit": limit,
            "kwargs": kwargs,
        }

    async def fake_to_thread(label, func, *args, **kwargs):
        calls.append((label, func.__name__, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(routes_personal, "_get_work_automation_status_sync", fake_status)
    monkeypatch.setattr(routes_personal.timing, "to_thread", fake_to_thread)

    status = asyncio.run(
        routes_personal.get_work_automation_status(
            item_id="slow-route",
            limit=3,
            include_contracts=False,
            metrics=True,
        )
    )

    assert calls[0][0] == "personal.run_status_sync"
    assert calls[0][1] == "run_status_sync"
    assert calls[0][2] == ()
    assert calls[0][3] == {}
    assert status["ok"] is True
    assert status["item_id"] == "slow-route"
    assert status["limit"] == 3
    assert status["kwargs"]["include_contracts"] is False
    assert status["server_metrics"]["schema"] == "xarta.kanban.automation_status.metrics.v1"
    assert status["server_metrics"]["status_thread_seconds"] >= 0


def test_work_kanban_review_decision_ledger_links_commits_and_status(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
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
                title="Use local decision ledger",
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
                provider_mode="local",
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
    assert decision["provider_mode"] == "local"

    listed = asyncio.run(routes_personal.list_work_item_review_decisions("work-decision-ledger"))
    assert listed["count"] == 1
    assert listed["commit_link_health"]["ok"] is True
    assert listed["decisions"][0]["commits"][0]["message_subject"] == "Add decision ledger contract"

    status = asyncio.run(routes_personal.get_work_automation_status(item_id="work-decision-ledger"))
    assert (
        status["provider_mode"]["active"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    assert status["provider_mode"]["planned"] == "active"
    assert (
        status["provider_mode"]["local_processing_gate"]
        == "hermes_profile_configured_fallback_only"
    )
    assert status["provider_mode"]["automatic_switch"] is False
    assert (
        status["provider_mode"]["profile_processing"]["routes"]["review"]["profile"]
        == "hermes-kanban-review-processor"
    )
    assert status["idle_worker"]["local_ai_model_alias"] == "TEST-KANBAN-LOCAL-AI"
    assert status["idle_worker"]["current_node_id"] == "test-node"
    assert status["idle_worker"]["owner_node_id"] == "test-node"
    assert status["idle_worker"]["owner_node_source"] == "owner_node_env"
    assert status["idle_worker"]["runs_on_this_node"] is True
    assert status["idle_worker"]["effective_enabled"] is True
    assert (
        status["idle_worker_contract"]["schema"]
        == routes_personal.KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA
    )
    assert status["idle_worker_contract"]["source_of_truth"] is True
    assert status["idle_worker_contract"]["scope_model"]["automatic_by_default"] is True
    assert status["idle_worker_contract"]["scope_model"]["background_root_scope_required"] is False
    assert (
        status["idle_worker_contract"]["preprocessing_processor"]["candidate_rule"]["state_id"]
        == "todo"
    )
    assert (
        status["idle_worker_contract"]["preprocessing_processor"]["candidate_rule"]["leaf_required"]
        is True
    )
    assert (
        status["processing_policy"]["schema"]
        == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    )
    assert (
        status["metadata_contract"]["schema"]
        == routes_personal.KANBAN_REVIEW_METADATA_CONTRACT_SCHEMA
    )
    assert (
        status["preprocessing_contract"]["schema"]
        == routes_personal.KANBAN_PREPROCESSING_READINESS_CONTRACT_SCHEMA
    )
    assert status["preprocessing"]["status"] == "readiness-contract-ready"
    assert (
        status["preprocessing"]["readiness_contract"]["marker_storage"]
        == "kanban_agent_hints.metadata.context_readiness_marker"
    )
    assert (
        status["proposal_surfaces"]["schema"]
        == routes_personal.KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA
    )
    assert (
        status["proposal_surfaces"]["inbox"]["item_id"]
        == routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID
    )
    assert (
        status["proposal_surfaces"]["outbox"]["item_id"]
        == routes_personal.KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID
    )
    assert (
        status["processing_policy"]["active_mode"]
        == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    assert status["processing_policy"]["local_processing"]["state"] == "fallback-model-only"
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


def test_work_review_provider_mode_preserves_hermes_kanban_llm_route():
    assert (
        routes_personal._clean_review_provider_mode("required-hermes-kanban-llm")
        == "required-hermes-kanban-llm"
    )
    assert (
        routes_personal._clean_review_provider_mode("required_hermes_kanban_llm")
        == "required-hermes-kanban-llm"
    )


def test_work_review_processor_output_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_review_processor_output_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_REVIEW_OUTPUT_CONTRACT_SCHEMA
    assert (
        contract["processing_policy_schema"]
        == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    )
    assert (
        contract["metadata_contract_schema"]
        == routes_personal.KANBAN_REVIEW_METADATA_CONTRACT_SCHEMA
    )
    assert (
        contract["provider_mode"]["active"]
        == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    assert (
        contract["provider_mode"]["local_processing_gate"]
        == "hermes_profile_configured_fallback_only"
    )
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


def test_work_automation_idle_worker_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_automation_idle_worker_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA
    assert contract["source_of_truth"] is True
    assert contract["singleton_guard"]["owner_node_env"] == (
        routes_personal.KANBAN_AUTOMATION_OWNER_NODE_ID_ENV
    )
    assert contract["singleton_guard"]["default_owner_source"] == "primary_flag_env"
    assert contract["singleton_guard"]["primary_flag_env"] == (
        routes_personal.KANBAN_AUTOMATION_PRIMARY_FLAG_ENV
    )
    assert contract["scope_model"]["automatic_by_default"] is True
    assert contract["scope_model"]["background_root_scope_required"] is False
    assert contract["scope_model"]["exclusion_field"] == "kanban_items.automation_excluded"
    assert contract["preprocessing_processor"]["candidate_rule"]["state_id"] == "todo"
    assert contract["preprocessing_processor"]["candidate_rule"]["leaf_required"] is True
    assert "flowchart TD" in contract["flowchart_mermaid"]
    assert any("No root env var set" in test for test in contract["must_pass_tests"])


def test_work_review_processor_metadata_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_review_processor_metadata_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_REVIEW_METADATA_CONTRACT_SCHEMA
    assert contract["review_document_schema"] == routes_personal.KANBAN_ITEM_REVIEW_SCHEMA
    assert contract["marker_schema"] == routes_personal.KANBAN_REVIEW_MARKER_SCHEMA
    assert (
        contract["provider_mode"]["active"]
        == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    fields = {field["field"]: field for field in contract["required_fields"]}
    assert fields["body_hash"]["scope"] == "review_document.metadata"
    assert fields["updated_at"]["alias"] == "review_updated_at"
    assert fields["operator_feedback.entries"]["entry_schema"] == (
        routes_personal.KANBAN_REVIEW_FEEDBACK_SCHEMA
    )
    assert fields["processed_at"]["alias"] == "last_processed_at"
    assert "Retryable failed outcomes clear this field" in fields["processed_at"]["updates_when"]
    assert (
        "failures cannot masquerade as successful processing"
        in fields["processed_source_hash"]["updates_when"]
    )
    assert fields["last_successful_source_hash"]["scope"] == "kanban_review_processor_markers"
    assert fields["next_retry_at"]["scope"] == "kanban_review_processor_markers"
    assert fields["retry_attempt_count"]["scope"] == "kanban_review_processor_markers"
    assert contract["failure_event_schema"] == routes_personal.KANBAN_REVIEW_FAILURE_EVENT_SCHEMA
    assert contract["retry_policy_version"] == routes_personal.KANBAN_REVIEW_RETRY_POLICY_VERSION
    assert fields["status"]["allowed_values"] == [
        "queued",
        "processing",
        "processed",
        "failed",
        "skipped",
        "cancelled",
    ]
    assert fields["run_id"]["scope"] == "marker.provenance"
    assert fields["last_error"]["scope"] == "kanban_review_processor_markers"
    assert fields["last_outcome_at"]["scope"] == "marker.metadata"
    assert fields["last_outcome_status"]["scope"] == "marker.metadata"
    assert "metadata.cancelled_previous_status" in contract["cancellation_fields"]
    assert any("body_hash is unchanged" in rule for rule in contract["transition_rules"])
    assert any("review_document_deleted" in rule for rule in contract["transition_rules"])


def test_work_review_processor_retry_backoff_schedule_and_cap():
    expected = [
        5 * 60,
        20 * 60,
        60 * 60,
        4 * 60 * 60,
        12 * 60 * 60,
        24 * 60 * 60,
        2 * 24 * 60 * 60,
        4 * 24 * 60 * 60,
        6 * 24 * 60 * 60,
    ]
    assert [
        routes_personal._work_review_retry_after_seconds(attempt)
        for attempt in range(1, len(expected) + 1)
    ] == expected
    assert routes_personal._work_review_retry_after_seconds(99) == 6 * 24 * 60 * 60
    assert routes_personal._work_review_retry_after_seconds(99) < 7 * 24 * 60 * 60
    assert routes_personal._work_review_retry_next_at("2026-06-28T10:00:00Z", 1) == (
        "2026-06-28T10:05:00Z"
    )
    assert routes_personal._work_review_retry_next_at("2026-06-28T10:00:00Z", 9) == (
        "2026-07-04T10:00:00Z"
    )


def test_work_preprocessing_readiness_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_preprocessing_readiness_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_PREPROCESSING_READINESS_CONTRACT_SCHEMA
    assert contract["context_packet_schema"] == "xarta.kanban.context_packet.v1"
    assert contract["readiness_marker_schema"] == "xarta.kanban.context_readiness_marker.v1"
    assert contract["readiness_check_schema"] == "xarta.kanban.context_readiness_check.v1"
    assert contract["preprocessing_request_schema"] == "xarta.kanban.preprocessing_time_request.v1"
    assert contract["queue_schema"] == routes_personal.KANBAN_PREPROCESSING_QUEUE_SCHEMA
    assert contract["marker_storage"] == "kanban_agent_hints.metadata.context_readiness_marker"
    assert (
        contract["provider_mode"]["active"]
        == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    fields = {field["field"]: field for field in contract["required_fields"]}
    assert fields["context_hash"]["scope"] == "context_readiness_marker"
    assert fields["marked_at"]["alias"] == "last_preprocessed_at"
    assert fields["ready"]["alias"] == "readiness_state"
    assert fields["drift_components"]["alias"] == "stale_markers"
    assert "open_questions" in fields
    assert "links" in fields
    assert "blockers" in fields
    assert "preprocessing_request" in fields
    assert "readiness_marker_stale" in contract["readiness_states"]
    assert "workspace_orientation" in contract["packet_inputs"]
    assert "commits" in contract["packet_inputs"]
    assert any("Implementation starts only" in rule for rule in contract["transition_rules"])
    assert any(
        "preprocessing_request.request_text" in rule for rule in contract["transition_rules"]
    )


def test_work_automation_file_backed_contract_routes_use_measured_sync_boundary(monkeypatch):
    calls = []

    async def run_sync(func, *args, **kwargs):
        calls.append((func, args, kwargs))
        return {"worker": func.__name__}

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)

    results = [
        asyncio.run(routes_personal.get_work_review_processor_output_contract()),
        asyncio.run(routes_personal.get_work_review_processor_processing_policy()),
        asyncio.run(routes_personal.get_work_automation_idle_worker_contract()),
        asyncio.run(routes_personal.get_work_review_processor_metadata_contract()),
        asyncio.run(routes_personal.get_work_preprocessing_readiness_contract()),
    ]

    assert [call[0] for call in calls] == [
        routes_personal._work_review_processor_output_contract,
        routes_personal._work_review_processing_policy,
        routes_personal._work_automation_idle_worker_contract,
        routes_personal._work_review_processing_metadata_contract,
        routes_personal._work_preprocessing_readiness_contract,
    ]
    assert all(call[1:] == ((), {}) for call in calls)
    assert all(result["ok"] is True for result in results)
    assert results[1]["policy"] == {"worker": "_work_review_processing_policy"}
    assert all(
        result["contract"] == {"worker": calls[index][0].__name__}
        for index, result in enumerate(results)
        if index != 1
    )


def test_work_proposal_surfaces_contract_endpoint():
    result = asyncio.run(routes_personal.get_work_proposal_surfaces_contract())
    contract = result["contract"]
    assert result["ok"] is True
    assert contract["schema"] == routes_personal.KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA
    assert (
        contract["surface_root"]["item_id"]
        == routes_personal.KANBAN_OPERATOR_PROPOSAL_SURFACE_ITEM_ID
    )
    assert (
        contract["workstream"]["item_id"]
        == routes_personal.KANBAN_AGENT_PROPOSAL_WORKSTREAM_ITEM_ID
    )
    assert contract["inbox"]["item_id"] == routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID
    assert contract["inbox"]["uri"] == "xarta-kanban:item:kanban-203acef17b12"
    assert "approval_request" in contract["inbox"]["accepted_entry_types"]
    assert "requested_operator_action" in contract["inbox"]["required_fields"]
    assert any(
        "not treat INBOX as the implementation card" in rule
        for rule in contract["inbox"]["placement_rules"]
    )
    assert contract["outbox"]["item_id"] == routes_personal.KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID
    assert "completed_decision" in contract["outbox"]["accepted_entry_types"]
    assert "commit_link_ids" in contract["outbox"]["required_fields"]
    assert any(
        "explicit commit associations" in rule for rule in contract["outbox"]["placement_rules"]
    )
    assert contract["status_integration"]["automation_status_field"] == "proposal_surfaces"
    assert any(
        "not substitutes for implementation workstream cards" in rule
        for rule in contract["global_rules"]
    )


def test_work_proposal_inbox_producer_is_replay_safe_and_uses_discussion_for_small_note(
    monkeypatch,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()

    created = asyncio.run(routes_personal.create_work_proposal_inbox(_proposal_request()))
    replay = asyncio.run(routes_personal.create_work_proposal_inbox(_proposal_request()))
    assert created["kind"] == "item"
    assert created["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert (
        created["item"]["parent_item_id"] == routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID
    )
    assert created["item"]["state_id"] == "todo"
    assert {link["metadata"]["role"] for link in created["links"]} == {
        "source_item",
        "semantic_owner",
    }
    proposal = json.loads(
        conn.execute(
            "SELECT provenance_json FROM kanban_items WHERE item_id='proposal-fixture-entry'"
        ).fetchone()["provenance_json"]
    )["proposal_surface"]
    assert proposal["status"] == "pending"
    assert proposal["exact_decision_needed"].startswith("Choose staged rollout")

    review_follow_up = asyncio.run(
        routes_personal.create_work_proposal_inbox(
            _proposal_request(
                entry_id="proposal-review-follow-up-alias",
                entry_type="review-follow-up",
            )
        )
    )
    review_follow_up_provenance = json.loads(
        conn.execute(
            "SELECT provenance_json FROM kanban_items WHERE item_id=?",
            (review_follow_up["item"]["item_id"],),
        ).fetchone()["provenance_json"]
    )["proposal_surface"]
    assert review_follow_up_provenance["entry_type"] == "review_processor_follow_up"

    note = asyncio.run(
        routes_personal.create_work_proposal_inbox(_proposal_request(lifecycle_required=False))
    )
    note_replay = asyncio.run(
        routes_personal.create_work_proposal_inbox(_proposal_request(lifecycle_required=False))
    )
    assert note["kind"] == "discussion"
    assert note_replay["idempotent_replay"] is True
    assert (
        conn.execute(
            "SELECT item_id FROM kanban_discussions WHERE discussion_id='proposal-fixture-note'"
        ).fetchone()["item_id"]
        == routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM kanban_items WHERE parent_item_id=?",
            (routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID,),
        ).fetchone()["count"]
        == 2
    )

    status = asyncio.run(routes_personal.get_work_proposal_surfaces_status())["proposal_surfaces"]
    assert status["inbox"]["count"] == 2
    assert status["inbox"]["open_count"] == 2
    assert status["inbox"]["entries"][0]["requested_operator_action"].startswith("Approve")


def test_work_proposal_operator_response_creates_outcome_and_owned_follow_up(
    monkeypatch,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()
    asyncio.run(routes_personal.create_work_proposal_inbox(_proposal_request()))

    async def fake_classifier(**kwargs):
        assert kwargs["processor_kind"] == "preprocessing"
        prompt = json.loads(kwargs["messages"][1]["content"])
        assert prompt["task_kind"] == "proposal_response_classification"
        if prompt["operator_response"].startswith("Withdraw the staged approval"):
            return {
                "payload": {
                    "outcome_type": "rejected",
                    "title": "Staged rollout withdrawn",
                    "summary": "The operator superseded the earlier approval.",
                    "rationale": "The later whole response explicitly withdraws the prior choice.",
                    "confidence": "high",
                    "uncertainty": "",
                    "selected_choices": [
                        {"choice": "withdraw staged rollout", "evidence": "later response"}
                    ],
                    "best_judgment_authorized": False,
                    "agent_choices": [],
                    "remaining_questions": [],
                    "implementation_actions": [],
                    "affected_item_refs": ["xarta-kanban:item:work-proposal-semantic-owner"],
                    "proof_refs": ["pytest:superseding-response"],
                },
                "model_alias": "TEST-HERMES",
                "model_attempts": [{"route_id": "semantic-test", "status": "success"}],
                "chosen_route_id": "semantic-test",
                "chosen_model": "test-model",
            }
        assert prompt["operator_response"].startswith("Approve the staged path")
        return {
            "payload": {
                "outcome_type": "accepted",
                "title": "Staged rollout accepted",
                "summary": "The operator approved the staged path and authorized bounded details.",
                "rationale": "The whole response explicitly selects staged rollout and delegates safe details.",
                "confidence": "high",
                "uncertainty": "",
                "selected_choices": [
                    {"choice": "staged rollout", "evidence": "Approve the staged path"}
                ],
                "best_judgment_authorized": True,
                "agent_choices": [
                    {"choice": "use the smaller pilot", "evidence": "use your best judgment"}
                ],
                "remaining_questions": [],
                "implementation_actions": [
                    {
                        "title": "Implement the staged pilot",
                        "body": "Implement the bounded pilot and attach focused proof.",
                        "requires_new_card": True,
                        "priority_id": "high",
                    }
                ],
                "affected_item_refs": ["xarta-kanban:item:work-proposal-semantic-owner"],
                "proof_refs": ["pytest:classified-whole-response"],
            },
            "model_alias": "TEST-HERMES",
            "model_attempts": [{"route_id": "semantic-test", "status": "success"}],
            "chosen_route_id": "semantic-test",
            "chosen_model": "test-model",
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_classifier,
    )
    body = routes_personal.WorkProposalResponseRequest(
        response_text=(
            "Approve the staged path. Use your best judgment for the smaller pilot details, "
            "within the proposal's existing authority."
        ),
        actor="operator-ui-audit",
        source_surface="kanban-automation-status",
    )
    result = asyncio.run(
        routes_personal.process_work_proposal_response("proposal-fixture-entry", body)
    )
    replay = asyncio.run(
        routes_personal.process_work_proposal_response("proposal-fixture-entry", body)
    )

    assert result["processed"] is True
    assert result["outcome_type"] == "accepted"
    assert (
        result["outbox_item"]["parent_item_id"]
        == routes_personal.KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID
    )
    assert result["outbox_item"]["state_id"] == "done"
    assert len(result["implementation_items"]) == 1
    assert result["implementation_items"][0]["parent_item_id"] == "work-proposal-semantic-owner"
    assert replay["idempotent_replay"] is True
    assert (
        conn.execute(
            "SELECT state_id FROM kanban_items WHERE item_id='proposal-fixture-entry'"
        ).fetchone()["state_id"]
        == "done"
    )
    decision = conn.execute(
        "SELECT * FROM kanban_review_decisions WHERE item_id='proposal-fixture-entry' "
        "AND processor_kind='proposal_response'"
    ).fetchone()
    metadata = json.loads(decision["metadata_json"])
    assert metadata["authority"]["kind"] == "operator"
    assert metadata["authority"]["actor_is_audit_identity_only"] is True
    assert metadata["best_judgment_authorized"] is True
    assert metadata["agent_choices"][0]["choice"] == "use the smaller pilot"
    discussion = conn.execute(
        "SELECT body_excerpt FROM kanban_discussions WHERE discussion_id=?",
        (metadata["response_discussion_id"],),
    ).fetchone()
    assert "Use your best judgment" in discussion["body_excerpt"]
    assert result["proposal_surfaces"]["inbox"]["processed_count"] == 1
    assert result["proposal_surfaces"]["outbox"]["processed_count"] == 1
    processed_entry = next(
        entry
        for entry in result["proposal_surfaces"]["inbox"]["entries"]
        if entry["item_id"] == "proposal-fixture-entry"
    )
    assert processed_entry["proposal_status"] == "processed"
    proposal_lifecycle = json.loads(
        conn.execute(
            "SELECT provenance_json FROM kanban_items WHERE item_id='proposal-fixture-entry'"
        ).fetchone()["provenance_json"]
    )["proposal_surface"]
    assert proposal_lifecycle["status"] == "processed"
    assert proposal_lifecycle["response_id"] == result["response_id"]
    assert proposal_lifecycle["outcome_type"] == "accepted"

    superseding = asyncio.run(
        routes_personal.process_work_proposal_response(
            "proposal-fixture-entry",
            routes_personal.WorkProposalResponseRequest(
                response_text=(
                    "Withdraw the staged approval. Reject this proposal based on the newer proof."
                )
            ),
        )
    )
    assert superseding["outcome_type"] == "rejected"
    assert superseding["superseded_decision_ids"] == [decision["decision_id"]]
    assert superseding["superseded_outbox_item_ids"] == [result["outbox_item"]["item_id"]]
    assert (
        conn.execute(
            "SELECT status FROM kanban_review_decisions WHERE decision_id=?",
            (decision["decision_id"],),
        ).fetchone()["status"]
        == "superseded"
    )
    assert superseding["proposal_surfaces"]["outbox"]["count"] == 2
    previous_outbox = next(
        entry
        for entry in superseding["proposal_surfaces"]["outbox"]["entries"]
        if entry["item_id"] == result["outbox_item"]["item_id"]
    )
    assert previous_outbox["proposal_status"] == "superseded"
    assert previous_outbox["superseded"] is True
    assert previous_outbox["superseded_by_response_id"] == superseding["response_id"]


def test_work_proposal_response_durably_queues_material_context_refresh(monkeypatch):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-proposal-refresh-leaf",
                title="Refresh proposal-owned implementation context",
                body="A bounded Todo leaf whose implementation context depends on operator input.",
                state_id="todo",
                priority_id="high",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_proposal_inbox(
            _proposal_request(
                entry_id="proposal-refresh-entry",
                source_item_refs=["xarta-kanban:item:work-proposal-refresh-leaf"],
                semantic_owner_item_ref="xarta-kanban:item:work-proposal-refresh-leaf",
            )
        )
    )

    async def fake_classifier(**kwargs):
        return {
            "payload": {
                "outcome_type": "accepted",
                "title": "Bounded choice accepted",
                "summary": "The operator selected the bounded choice for this leaf.",
                "rationale": "The complete response supplies the missing implementation context.",
                "confidence": "high",
                "uncertainty": "",
                "selected_choices": [{"choice": "bounded path", "evidence": "whole response"}],
                "best_judgment_authorized": True,
                "agent_choices": [
                    {"choice": "smallest safe batch", "evidence": "bounded authority"}
                ],
                "remaining_questions": [],
                "implementation_actions": [],
                "affected_item_refs": ["xarta-kanban:item:work-proposal-refresh-leaf"],
                "proof_refs": ["pytest:proposal-refresh"],
            },
            "model_alias": "TEST-HERMES",
            "model_attempts": [{"route_id": "semantic-test", "status": "success"}],
            "chosen_route_id": "semantic-test",
            "chosen_model": "test-model",
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_classifier,
    )
    body = routes_personal.WorkProposalResponseRequest(
        response_text="Approve the bounded path and use best judgment only inside this leaf.",
        response_id="proposal-response-refresh-fixed",
    )
    result = asyncio.run(
        routes_personal.process_work_proposal_response("proposal-refresh-entry", body)
    )
    replay = asyncio.run(
        routes_personal.process_work_proposal_response("proposal-refresh-entry", body)
    )

    assert result["readiness_refresh_requested"] is True
    assert result["readiness_refresh"]["state"] == "queued"
    assert result["readiness_refresh"]["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True
    assert replay["readiness_refresh_requested"] is True
    assert replay["readiness_refresh"]["state"] == "queued"
    assert replay["readiness_refresh"]["idempotent_replay"] is True
    marker = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE item_id=? "
        "AND processor_kind='preprocessing'",
        ("work-proposal-refresh-leaf",),
    ).fetchone()
    marker_metadata = json.loads(marker["metadata_json"])
    assert marker["status"] == "queued"
    assert marker_metadata["reason"] == "proposal_response_material_context_change"
    assert marker_metadata["proposal_response_id"] == "proposal-response-refresh-fixed"
    assert marker_metadata["queue_source"] == "proposal_response"


@pytest.mark.parametrize(
    ("outcome_type", "remaining_questions", "implementation_actions", "processed"),
    [
        (
            "partial",
            ["Confirm whether the second bounded phase may start."],
            [
                {
                    "title": "Record the approved first phase",
                    "body": "Record the bounded first-phase decision without expanding scope.",
                    "requires_new_card": False,
                    "priority_id": "medium",
                }
            ],
            True,
        ),
        (
            "follow_up",
            ["Choose the remaining bounded proof window."],
            [
                {
                    "title": "Prepare the accepted proof context",
                    "body": "Prepare only the accepted context while the final window remains open.",
                    "requires_new_card": False,
                    "priority_id": "medium",
                }
            ],
            True,
        ),
        ("deferred", ["Revisit after the named dependency is proven."], [], False),
    ],
)
def test_work_proposal_response_materializes_partial_deferred_and_follow_up(
    monkeypatch,
    outcome_type,
    remaining_questions,
    implementation_actions,
    processed,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()
    asyncio.run(routes_personal.create_work_proposal_inbox(_proposal_request()))

    async def fake_classifier(**kwargs):
        return {
            "payload": {
                "outcome_type": outcome_type,
                "title": f"Proposal response: {outcome_type}",
                "summary": f"The whole response produced the {outcome_type} outcome.",
                "rationale": "The bounded current state and complete operator response support it.",
                "confidence": "high",
                "uncertainty": "",
                "selected_choices": [],
                "best_judgment_authorized": False,
                "agent_choices": [],
                "remaining_questions": remaining_questions,
                "implementation_actions": implementation_actions,
                "affected_item_refs": ["xarta-kanban:item:work-proposal-semantic-owner"],
                "proof_refs": [f"pytest:{outcome_type}"],
            },
            "model_alias": "TEST-HERMES",
            "model_attempts": [{"route_id": "semantic-test", "status": "success"}],
            "chosen_route_id": "semantic-test",
            "chosen_model": "test-model",
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_classifier,
    )
    result = asyncio.run(
        routes_personal.process_work_proposal_response(
            "proposal-fixture-entry",
            routes_personal.WorkProposalResponseRequest(
                response_text=f"Whole semantic response for {outcome_type}."
            ),
        )
    )

    assert result["outcome_type"] == outcome_type
    assert result["processed"] is processed
    assert result["decision"]["decision_type"] == outcome_type
    assert result["outbox_item"]["state_id"] == "done"
    assert conn.execute(
        "SELECT state_id FROM kanban_items WHERE item_id='proposal-fixture-entry'"
    ).fetchone()["state_id"] == ("done" if processed else "todo")
    if outcome_type in {"partial", "follow_up"}:
        assert len(result["follow_up_inbox_items"]) == 1
        assert (
            result["follow_up_inbox_items"][0]["parent_item_id"]
            == routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID
        )
    else:
        assert result["follow_up_inbox_items"] == []


def test_work_proposal_response_failure_is_retryable_without_invented_semantics(
    monkeypatch,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()
    asyncio.run(routes_personal.create_work_proposal_inbox(_proposal_request()))

    async def invalid_classifier(**kwargs):
        return {"payload": {"summary": "No semantic outcome type was returned."}}

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        invalid_classifier,
    )
    body = routes_personal.WorkProposalResponseRequest(
        response_text="I approve only the first choice; hold the second until the dependency is proven.",
        response_id="proposal-response-fixed-retry",
    )
    with pytest.raises(routes_personal.HTTPException) as exc_info:
        asyncio.run(routes_personal.process_work_proposal_response("proposal-fixture-entry", body))
    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["retryable"] is True
    row = conn.execute(
        "SELECT * FROM kanban_review_decisions WHERE item_id='proposal-fixture-entry' "
        "AND processor_kind='proposal_response'"
    ).fetchone()
    assert row["status"] == "failed"
    metadata = json.loads(row["metadata_json"])
    assert metadata["response_state"]["retry_state"] == "retryable"
    assert metadata["authority"]["kind"] == "operator"
    assert (
        conn.execute(
            "SELECT state_id FROM kanban_items WHERE item_id='proposal-fixture-entry'"
        ).fetchone()["state_id"]
        == "todo"
    )
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM kanban_items WHERE parent_item_id=?",
            (routes_personal.KANBAN_OPERATOR_PROPOSAL_OUTBOX_ITEM_ID,),
        ).fetchone()["count"]
        == 0
    )
    status = asyncio.run(routes_personal.get_work_proposal_surfaces_status())["proposal_surfaces"]
    assert status["inbox"]["failed_count"] == 1
    assert status["inbox"]["retry_count"] == 1
    conflicting = routes_personal.WorkProposalResponseRequest(
        response_text="A different response must not reuse the same durable response identity.",
        response_id="proposal-response-fixed-retry",
    )
    with pytest.raises(routes_personal.HTTPException) as conflict_info:
        asyncio.run(
            routes_personal.process_work_proposal_response("proposal-fixture-entry", conflicting)
        )
    assert conflict_info.value.status_code == 409


def test_work_review_processor_processing_policy_endpoint(monkeypatch):
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    result = asyncio.run(routes_personal.get_work_review_processor_processing_policy())
    policy = result["policy"]
    assert result["ok"] is True
    assert policy["schema"] == routes_personal.KANBAN_REVIEW_PROCESSING_POLICY_SCHEMA
    assert policy["active_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert policy["applies_to"] == [
        "review_processor",
        "preprocessing",
        "blocker_processor",
    ]
    assert policy["profile_processing"]["state"] == "active"
    assert policy["profile_processing"]["routes"]["review"]["profile"] == (
        "hermes-kanban-review-processor"
    )
    assert policy["profile_processing"]["routes"]["preprocessing"]["profile"] == (
        "hermes-kanban-preprocessor"
    )
    assert policy["profile_processing"]["routes"]["blocker"]["profile"] == (
        "hermes-kanban-blocker-processor"
    )
    assert policy["local_processing"]["state"] == "fallback-model-only"
    assert policy["local_processing"]["gate"] == "hermes_profile_configured_fallback_only"
    assert policy["local_processing"]["substitute_decisions_allowed"] is False
    assert policy["provider_choice"]["default_mode"] == (
        routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    )
    assert any("Provider mode must be explicit" in rule for rule in policy["routing_rules"])


def test_work_review_processor_processing_policy_uses_configured_local_model(monkeypatch):
    monkeypatch.setenv(routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV, "test-local-model")

    result = asyncio.run(routes_personal.get_work_review_processor_processing_policy())

    assert result["ok"] is True
    assert result["policy"]["local_processing"]["fallback_model"] == (
        "PRIMARY-LOCAL-PRIVATE-NO-PROTECTION"
    )


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


def test_work_review_processor_idle_scan_queues_changed_reviews(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-scan-root",
                title="Review scan root",
                body="Root item for review scan proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-scan-child",
                parent_item_id="work-review-scan-root",
                title="Review scan child",
                body="Child item with Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-scan-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Operator Review: proceed confidently with the queue trigger.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-review-write",
            ),
        )
    )
    conn.execute("DELETE FROM sync_queue")
    conn.commit()

    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-scan-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-trigger",
            )
        )
    )
    assert scan["ok"] is True
    assert scan["schema"] == routes_personal.KANBAN_REVIEW_SCHEDULER_SCHEMA
    assert scan["scanned_count"] == 2
    assert scan["eligible_review_count"] == 1
    assert scan["queued_count"] == 1
    assert scan["skipped_empty_count"] == 1
    marker = scan["queued_markers"][0]
    assert marker["schema"] == routes_personal.KANBAN_REVIEW_MARKER_SCHEMA
    assert marker["item_id"] == "work-review-scan-child"
    assert marker["status"] == "queued"
    assert marker["provider_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert marker["processed_source_hash"] == ""
    assert marker["metadata"]["reason"] == "new_review_document"

    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-scan-root")
    )
    assert status["review_processor"]["queue_length"] == 1
    assert status["review_processor"]["scheduler"]["queue_length"] == 1
    assert (
        status["review_processor"]["scheduler"]["recent_markers"][0]["item_id"]
        == "work-review-scan-child"
    )

    same_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-scan-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-trigger-again",
            )
        )
    )
    assert same_scan["queued_count"] == 0
    assert same_scan["unchanged_pending_count"] == 1
    assert (
        conn.execute("SELECT COUNT(*) AS count FROM kanban_review_processor_markers").fetchone()[
            "count"
        ]
        == 1
    )

    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-scan-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Operator Review: proceed confidently, and queue the changed Review doc.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-review-change",
            ),
        )
    )
    changed_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-scan-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-trigger-changed",
            )
        )
    )
    assert changed_scan["queued_count"] == 1
    assert changed_scan["queued_markers"][0]["metadata"]["reason"] == "review_document_changed"
    assert (
        changed_scan["queued_markers"][0]["document_source_hash"] != marker["document_source_hash"]
    )

    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-scan-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-review-delete",
            ),
        )
    )
    deleted_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-scan-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-scan-trigger-deleted",
            )
        )
    )
    assert deleted_scan["queued_count"] == 0
    assert deleted_scan["cancelled_deleted_count"] == 1
    cancelled = deleted_scan["cancelled_markers"][0]
    assert cancelled["status"] == "cancelled"
    assert cancelled["last_error"] == "review_document_deleted"
    assert cancelled["metadata"]["cancelled_previous_status"] == "queued"
    assert cancelled["metadata"]["document_exists"] is True
    assert cancelled["metadata"]["body_bytes"] == 0

    deleted_status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-scan-root")
    )
    deleted_scheduler = deleted_status["review_processor"]["scheduler"]
    assert deleted_status["review_processor"]["queue_length"] == 0
    assert deleted_scheduler["queue_length"] == 0
    assert deleted_scheduler["active_count"] == 0
    assert deleted_scheduler["pending_count"] == 0
    assert deleted_scheduler["by_status"]["cancelled"] == 1
    assert deleted_status["review_processor"]["review_markers"][0]["status"] == "cancelled"

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_markers" in sync_tables
    assert "kanban_audit_log" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "trigger_review_processor_idle_scan" in audit_actions


def test_work_review_processor_claim_empty_eligible_ids_uses_boolean_false(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-empty-eligible-root",
                title="Empty eligible root",
                body="Root item for empty eligible marker claim proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-empty-eligible-child",
                parent_item_id="work-review-empty-eligible-root",
                title="Empty eligible child",
                body="Child item with Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-empty-eligible-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Operator Review: queue this marker but filter it out during claim.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-empty-eligible-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-scan",
            )
        )
    )
    marker_id = scan["queued_markers"][0]["marker_id"]
    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-empty-eligible",
                item_id="work-review-empty-eligible-root",
                lease_token="empty-eligible-token",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-lease",
            )
        )
    )
    assert acquired["acquired"] is True

    class CapturingConnection:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.sql = []

        def execute(self, sql, *args):
            if isinstance(sql, str):
                self.sql.append(sql)
            return self.wrapped.execute(sql, *args)

        def commit(self):
            return self.wrapped.commit()

        def rollback(self):
            return self.wrapped.rollback()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

    capture = CapturingConnection(conn)
    monkeypatch.setattr(routes_personal, "get_conn", lambda: _conn_context(capture))
    claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-empty-eligible",
                lease_token="empty-eligible-token",
                item_id="work-review-empty-eligible-root",
                eligible_marker_ids=[],
                actor="codex-test",
                source_surface="pytest",
                request_id="review-empty-eligible-claim",
            )
        )
    )
    assert claim["claimed"] is False
    assert claim["reason"] == "no_queued_marker"
    marker_queries = [
        sql
        for sql in capture.sql
        if "SELECT marker.* FROM kanban_review_processor_markers marker" in sql
        and "CASE marker.processor_kind" in sql
    ]
    assert marker_queries
    assert "AND FALSE" in marker_queries[-1]
    assert "AND 0" not in marker_queries[-1]
    marker_status = conn.execute(
        "SELECT status FROM kanban_review_processor_markers WHERE marker_id=?",
        (marker_id,),
    ).fetchone()["status"]
    assert marker_status == "queued"


def test_work_automation_exclusion_skips_review_scan_and_marker_claim(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-excluded-root",
                title="Review excluded root",
                body="Root import bucket for review exclusion proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-excluded-child",
                parent_item_id="work-review-excluded-root",
                title="Review excluded child",
                body="Child item with Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-excluded-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Operator Review: this would normally queue.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-review-write",
            ),
        )
    )

    queued_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-excluded-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-scan-before",
            )
        )
    )
    assert queued_scan["queued_count"] == 1
    marker_id = queued_scan["queued_markers"][0]["marker_id"]

    excluded = asyncio.run(
        routes_personal.update_work_item(
            "work-review-excluded-root",
            routes_personal.WorkItemUpdateRequest(
                automation_excluded=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-root-update",
            ),
        )
    )["item"]
    assert excluded["automation_excluded"] is True

    excluded_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-excluded-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-scan-after",
            )
        )
    )
    assert excluded_scan["scanned_count"] == 0
    assert excluded_scan["queued_count"] == 0
    assert excluded_scan["scheduler"]["queue_length"] == 0

    scope_ids = routes_personal._work_scope_item_ids(conn, "work-review-excluded-root")
    assert routes_personal._work_queued_processor_marker_ids(conn, scope_ids) == []

    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-excluded-root")
    )
    assert status["automation_exclusions"]["count"] == 1
    assert (
        status["automation_exclusions"]["recent_items"][0]["item_id"] == "work-review-excluded-root"
    )
    assert status["review_processor"]["queue_length"] == 0

    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-exclusion",
                item_id="work-review-excluded-root",
                lease_token="exclusion-token",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-lease",
            )
        )
    )
    assert acquired["acquired"] is True
    claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-exclusion",
                lease_token="exclusion-token",
                item_id="work-review-excluded-root",
                eligible_marker_ids=[marker_id],
                actor="codex-test",
                source_surface="pytest",
                request_id="review-excluded-claim",
            )
        )
    )
    assert claim["claimed"] is False
    assert claim["reason"] == "no_queued_marker"


def test_work_review_processor_completion_cancels_claimed_marker_after_exclusion(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-claimed-excluded-root",
                title="Claimed excluded root",
                body="Root that becomes excluded after a marker is claimed.",
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-claimed-excluded-child",
                parent_item_id="work-review-claimed-excluded-root",
                title="Claimed excluded child",
                body="Child with review text.",
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-claimed-excluded-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Operator Review: claimed before exclusion.",
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-claimed-excluded-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    lease = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-claimed-excluded",
                item_id="work-review-claimed-excluded-root",
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-claimed-excluded",
                lease_token=lease["lease"]["lease_token"],
                item_id="work-review-claimed-excluded-root",
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-claim",
            )
        )
    )
    assert claimed["claimed"] is True

    asyncio.run(
        routes_personal.update_work_item(
            "work-review-claimed-excluded-root",
            routes_personal.WorkItemUpdateRequest(
                automation_excluded=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="claimed-excluded-root-exclude",
            ),
        )
    )

    completed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            claimed["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-claimed-excluded",
                lease_token=lease["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="processed",
                actor="kanban-idle-worker",
                source_surface="kanban-automation-idle-worker",
                request_id="claimed-excluded-complete",
            ),
        )
    )
    assert completed["completed"] is False
    assert completed["reason"] == "automation_excluded"
    assert completed["marker"]["status"] == "cancelled"
    assert completed["marker"]["last_error"] == "automation_excluded"


def test_work_automation_idle_tick_processes_review_with_profile_llm(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    _create_proposal_surface_fixture()
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-local-ai-root",
                title="Profile LLM root",
                body="Root item for profile-backed worker proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="local-ai-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-local-ai-review",
                parent_item_id="work-local-ai-root",
                title="Profile Review child",
                body="Child item with Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="local-ai-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-local-ai-review",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body=(
                    "Operator Review: missing required provider wiring must be raised "
                    "as a blocker/question, not worked around."
                ),
                actor="codex-test",
                source_surface="pytest",
                request_id="local-ai-review-write",
            ),
        )
    )

    async def fake_local_ai_json_completion(*, messages, run_id, processor_kind=""):
        assert "missing required provider wiring" in messages[1]["content"]
        return {
            "model_alias": "TEST-KANBAN-LOCAL-AI",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                "title": "Record blocker/question guidance",
                "summary": "Processed the operator Review as guidance against workarounds.",
                "rationale": (
                    "The Review states missing required provider wiring should produce "
                    "a blocker or question instead of substitute behavior."
                ),
                "decision_type": "follow_up_card",
                "confidence": "high",
                "uncertainty": "",
                "status": "recorded",
                "affected_refs": ["xarta-kanban:item:work-local-ai-review"],
                "proof_refs": ["kanban_items:work-local-ai-review:review"],
                "output_payload": {
                    "title": "Implement the confirmed provider wiring",
                    "body": "Implement only the provider path selected through the Review evidence.",
                    "parent_ref": "xarta-kanban:item:work-local-ai-review",
                    "lane": "todo",
                    "priority": "high",
                    "reason": "The Review identified a bounded implementation follow-up.",
                },
                "proposal_entries": [
                    {
                        "entry_type": "review_processor_follow_up",
                        "title": "Confirm required provider wiring",
                        "summary": "The Review requires an operator-visible provider decision.",
                        "rationale": "The Review explicitly forbids substituting another workflow.",
                        "requested_operator_action": "Confirm the required provider wiring.",
                        "exact_decision_needed": "Select the authorized provider path or defer.",
                        "lifecycle_required": True,
                        "proof_refs": ["kanban_items:work-local-ai-review:review"],
                    }
                ],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_local_ai_json_completion,
    )
    conn.execute("DELETE FROM sync_queue")
    conn.commit()

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-local-ai-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    assert tick["ok"] is True
    assert tick["lease_acquired"] is True
    assert tick["processed_count"] == 1
    processed = tick["processed_markers"][0]
    assert processed["processor_kind"] == "review"
    assert processed["provider_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert processed["profile"] == "hermes-kanban-review-processor"
    assert processed["model_alias"] == "TEST-KANBAN-LOCAL-AI"

    row = conn.execute(
        "SELECT * FROM kanban_review_decisions WHERE item_id='work-local-ai-review'"
    ).fetchone()
    assert row["provider_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert row["decision_type"] == "follow_up_card"
    marker = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE item_id='work-local-ai-review'"
    ).fetchone()
    assert marker["status"] == "processed"
    assert marker["decision_id"] == row["decision_id"]
    proposal = conn.execute(
        "SELECT * FROM kanban_items WHERE parent_item_id=? AND title=?",
        (
            routes_personal.KANBAN_OPERATOR_PROPOSAL_INBOX_ITEM_ID,
            "Confirm required provider wiring",
        ),
    ).fetchone()
    assert proposal is not None
    assert proposal["state_id"] == "todo"
    decision_metadata = json.loads(row["metadata_json"])
    assert decision_metadata["proposal_results"][0]["item"]["item_id"] == proposal["item_id"]
    follow_up = conn.execute(
        "SELECT * FROM kanban_items WHERE parent_item_id='work-local-ai-review' "
        "AND title='Implement the confirmed provider wiring'"
    ).fetchone()
    assert follow_up is not None
    assert follow_up["state_id"] == "todo"
    assert decision_metadata["follow_up_card"]["item"]["item_id"] == follow_up["item_id"]


def test_work_automation_preprocessing_distinguishes_marker_staleness_from_blocker(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-ai-root",
                title="Preprocessing root",
                body="Root item for preprocessing worker proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-ai-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-ai-child",
                parent_item_id="work-preprocess-ai-root",
                title="Preprocessing child",
                body="Child needs current preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-ai-child-create",
            )
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_review_decisions (
            decision_id, item_id, processor_kind, decision_type, title, summary,
            rationale, affected_refs_json, confidence, uncertainty, proof_refs_json,
            commit_link_ids_json, status, provider_mode, source_hash, metadata_json,
            provenance_json, created_at, updated_at
        )
        VALUES (?, ?, 'preprocessing', 'preprocessing_blocker_or_question', ?, ?, ?,
                '[]', 'high', '', '[]', '[]', 'failed', 'local', ?, ?, '{}',
                '2026-06-27T04:00:00Z', '2026-06-27T04:00:00Z')
        """,
        (
            "kanban-decision-stale-preprocess-failure",
            "work-preprocess-ai-child",
            "Blocked stale parent context",
            "Old source said parent context was missing.",
            "This stale failed decision should not be replayed as current evidence.",
            "sha256:stale-decision-row",
            json.dumps(
                {"document_source_hash": "sha256:old-pre-ancestor-context"},
                ensure_ascii=True,
                sort_keys=True,
            ),
        ),
    )
    conn.commit()

    async def fake_local_ai_json_completion(*, messages, run_id, processor_kind=""):
        context = json.loads(messages[1]["content"])
        assert context["queue_source"]["reason"] == "missing_readiness_marker"
        assert "parent_body" in {ref["name"] for ref in context["queue_source"]["source_refs"]}
        ancestor_context = context["evidence"]["ancestor_context"]
        assert ancestor_context["ancestors"][0]["item"]["item_id"] == ("work-preprocess-ai-root")
        assert (
            ancestor_context["ancestors"][0]["documents"]["body_excerpt"]
            == "Root item for preprocessing worker proof"
        )
        assert "kanban-decision-stale-preprocess-failure" not in {
            decision["decision_id"] for decision in context["evidence"]["recent_decisions"]
        }
        assert "scheduling reason for this preprocessing pass" in messages[1]["content"]
        assert "scheduling reason, not a failure" in messages[0]["content"]
        return {
            "model_alias": "TEST-KANBAN-LOCAL-AI",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                **_preprocessing_contract_fields(unit_title="Create the missing proof artifact"),
                "ready": False,
                "summary": "Current evidence is not enough yet.",
                "rationale": "The card needs proof before implementation can start.",
                "confidence": "high",
                "uncertainty": "",
                "blocking_codes": ["missing_proof"],
                "recommended_next_actions": ["Add proof."],
                "selected_guidance_ids": ["permission-and-ownership-guards"],
                "guidance_rationales": {
                    "permission-and-ownership-guards": (
                        "The proof artifact is written by a privileged helper into an xarta-owned tree."
                    )
                },
                "decomposition_items": [
                    {
                        "title": "Add proof.",
                        "body": "Create the missing proof artifact.",
                        "state_id": "todo",
                        "priority_id": "medium",
                        "proof_path": "Proof artifact exists.",
                        "guidance_ids": ["permission-and-ownership-guards"],
                    }
                ],
                "affected_refs": ["xarta-kanban:item:work-preprocess-ai-child"],
                "proof_refs": ["kanban_items:work-preprocess-ai-child:body"],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_local_ai_json_completion,
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-preprocess-ai-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )

    assert tick["ok"] is True
    assert tick["lease_acquired"] is True
    assert tick["processed_count"] == 1
    processed = tick["processed_markers"][0]
    assert processed["processor_kind"] == "preprocessing"
    assert processed["provider_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert processed["status"] == "processed"
    assert processed["decomposition"]["total_count"] == 1
    assert processed["decomposition"]["created_count"] == 1

    marker = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE item_id='work-preprocess-ai-child'"
    ).fetchone()
    assert marker["status"] == "processed"
    assert marker["last_error"] == ""
    child = conn.execute(
        """
        SELECT * FROM kanban_items
        WHERE parent_item_id='work-preprocess-ai-child'
          AND title='Add proof.'
        """
    ).fetchone()
    assert child is not None
    assert child["parent_item_id"] == "work-preprocess-ai-child"
    assert child["depth"] == 2
    assert child["state_id"] == "todo"
    parent = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-ai-child'"
    ).fetchone()
    assert parent["state_id"] == "doing"
    decision = conn.execute(
        """
        SELECT * FROM kanban_review_decisions
        WHERE item_id='work-preprocess-ai-child'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    assert decision["decision_type"] == "preprocessing_decomposition"
    assert decision["status"] == "accepted"
    assert decision["title"] == "Preprocessing child"
    decision_metadata = json.loads(decision["metadata_json"])
    assert marker["document_source_hash"] == marker["processed_source_hash"]
    assert (
        marker["document_source_hash"]
        == (decision_metadata["source_after_decomposition"]["document_source_hash"])
    )
    assert (
        marker["document_source_hash"]
        != (decision_metadata["source_before_decomposition"]["document_source_hash"])
    )
    assert decision_metadata["engineering_guidance"]["selected_guidance_ids"] == [
        "permission-and-ownership-guards"
    ]
    assert (
        decision_metadata["readiness_marker"]["engineering_guidance"]
        == (decision_metadata["engineering_guidance"])
    )
    assert "permission-and-ownership-guards" in child["body_excerpt"]


def test_work_preprocessing_decomposition_child_ids_retry_collisions_and_reuse_siblings(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-collision-root",
                title="Collision root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-collision-parent",
                parent_item_id="work-preprocess-collision-root",
                title="Collision parent",
                body="Parent item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    title = "Create API proof"
    colliding_id = routes_personal._work_preprocessing_child_id(
        "work-preprocess-collision-parent",
        title,
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id=colliding_id,
                title="Unrelated item using the first generated id",
                body="This unrelated root forces an item_id retry.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )

    parent_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-collision-parent'"
    ).fetchone()
    first = asyncio.run(
        routes_personal._work_preprocessing_create_decomposition_children(
            parent_item=parent_row,
            payload={
                "decomposition_items": [
                    {
                        "title": title,
                        "body": "Create an API proof child.",
                        "state_id": "todo",
                    }
                ]
            },
            holder_id="codex-test",
            run_id="pytest-preprocess-collision",
            marker_id="marker-preprocess-collision",
        )
    )
    assert first["created_count"] == 1
    child = first["created_items"][0]
    assert child["item_id"] != colliding_id
    assert child["parent_item_id"] == "work-preprocess-collision-parent"
    assert child["depth"] == 2

    second = asyncio.run(
        routes_personal._work_preprocessing_create_decomposition_children(
            parent_item=parent_row,
            payload={
                "decomposition_items": [
                    {
                        "title": title.upper(),
                        "body": "Same semantic child, different case.",
                        "state_id": "todo",
                    }
                ]
            },
            holder_id="codex-test",
            run_id="pytest-preprocess-collision-repeat",
            marker_id="marker-preprocess-collision-repeat",
        )
    )
    assert second["created_count"] == 0
    assert second["existing_count"] == 1
    assert second["existing_items"][0]["item_id"] == child["item_id"]


def test_work_preprocessing_blocked_child_materializes_visible_blocker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-blocked-root",
                title="Blocked child root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-blocked-parent",
                parent_item_id="work-preprocess-blocked-root",
                title="Blocked child parent",
                body="Concrete implementation request. Proof path: child blocker row exists.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    parent_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-blocked-parent'"
    ).fetchone()

    result = asyncio.run(
        routes_personal._work_preprocessing_create_decomposition_children(
            parent_item=parent_row,
            payload={
                "decomposition_items": [
                    {
                        "title": "Ask operator for API route",
                        "body": "Need a route decision before implementation.",
                        "state_id": "blocked",
                        "blocked_reason": "Operator must choose the API route.",
                        "proof_path": "Visible blocker row records the question.",
                    }
                ]
            },
            holder_id="kanban-idle-worker",
            run_id="pytest-preprocess-blocked-child",
            marker_id="marker-preprocess-blocked-child",
        )
    )

    assert result["created_count"] == 1
    child = result["created_items"][0]
    assert child["state_id"] == "blocked"
    blockers = conn.execute(
        "SELECT * FROM kanban_blockers WHERE item_id=?",
        (child["item_id"],),
    ).fetchall()
    assert len(blockers) == 1
    assert blockers[0]["status"] == "open"
    assert blockers[0]["blocked_by_ref"] == (
        "kanban_review_processor_markers:marker-preprocess-blocked-child"
    )
    provenance = json.loads(blockers[0]["provenance_json"])
    assert provenance["schema"] == routes_personal.KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "create_work_item_blocker" in audit_actions


def test_work_preprocessing_scan_resolves_satisfied_parent_context_blocker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-parent-context-root",
                title="Parent context root",
                body="Parent body now supplied to child preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-parent-context-child",
                parent_item_id="work-preprocess-parent-context-root",
                title="Retrieve parent item content",
                body="Fetch the parent item content before implementation.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_blockers (
            blocker_id, item_id, title, body_excerpt, status, blocked_by_ref,
            provenance_json
        )
        VALUES (?, ?, ?, ?, 'open', ?, ?)
        """,
        (
            "kanban-blocker-parent-context-satisfied",
            "work-preprocess-parent-context-child",
            "Preprocessing blocker/question: Fetch parent item content",
            "Parent item content not available in current context; requires fetch.",
            "kanban_review_processor_markers:marker-parent-context-old",
            json.dumps(
                {
                    "schema": routes_personal.KANBAN_PREPROCESSING_BLOCKER_PROVENANCE_SCHEMA,
                    "marker_id": "marker-parent-context-old",
                    "reason": "Parent item content not available in current context.",
                    "source_item_id": "work-preprocess-parent-context-root",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
        ),
    )
    conn.commit()

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-parent-context-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-parent-context-scan",
            )
        )
    )

    assert scan["satisfied_parent_context_blocker_resolved_count"] == 1
    assert scan["queued_count"] == 1
    blocker = conn.execute(
        """
        SELECT * FROM kanban_blockers
        WHERE blocker_id='kanban-blocker-parent-context-satisfied'
        """
    ).fetchone()
    assert blocker["status"] == "resolved"
    assert "ancestor_context" in blocker["body_excerpt"]
    marker = conn.execute(
        """
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id='work-preprocess-parent-context-child'
        """
    ).fetchone()
    assert marker["status"] == "queued"
    source = routes_personal._work_preprocessing_context_source(
        conn,
        conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE item_id='work-preprocess-parent-context-child'
            """
        ).fetchone(),
    )
    assert source["counts"]["blocker_count"] == 0
    assert "parent_body" in {ref["name"] for ref in source["source_refs"]}
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "resolve_satisfied_preprocessing_parent_context_blocker" in audit_actions


def test_work_preprocessing_duplicate_decomposition_titles_fail_without_children(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-duplicate-parent",
                title="Duplicate parent",
                body="Concrete implementation request. Proof path: no partial mutation.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    parent_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-duplicate-parent'"
    ).fetchone()

    with pytest.raises(ValueError) as duplicate:
        asyncio.run(
            routes_personal._work_preprocessing_create_decomposition_children(
                parent_item=parent_row,
                payload={
                    "decomposition_items": [
                        {"title": "Same child", "body": "First."},
                        {"title": " same   child ", "body": "Duplicate."},
                    ]
                },
                holder_id="kanban-idle-worker",
                run_id="pytest-preprocess-duplicate",
                marker_id="marker-preprocess-duplicate",
            )
        )
    assert "duplicates another child title" in str(duplicate.value)
    child_count = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_items
        WHERE parent_item_id='work-preprocess-duplicate-parent'
        """
    ).fetchone()["count"]
    assert child_count == 0


def test_work_blocked_leaf_invariant_audit_and_repair_creates_blocker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-legacy-blocked-leaf",
                title="Resolve Dependency: Legacy blocker repair",
                body=(
                    "Preprocessing child of work-legacy-parent.\n\n"
                    "Blocked reason/question: Operator must choose the dependency source."
                ),
                state_id="blocked",
                actor="codex-test",
                source_surface="pytest",
                request_id="legacy-blocked-leaf-create",
            )
        )
    )

    audit = asyncio.run(
        routes_personal.audit_work_blocked_leaf_invariant(
            item_id="work-legacy-blocked-leaf",
            include_test_entries=True,
            limit=20,
        )
    )
    assert audit["count"] == 1
    assert audit["findings"][0]["classification"] == "preprocessing_blocked_child"
    assert audit["findings"][0]["action"] == "create_blocker"

    dry_run = asyncio.run(
        routes_personal.repair_work_blocked_leaf_invariant(
            routes_personal.WorkBlockedLeafInvariantRepairRequest(
                item_id="work-legacy-blocked-leaf",
                apply=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="legacy-blocked-leaf-dry-run",
                run_id="pytest-legacy-blocked-leaf",
            )
        )
    )
    assert dry_run["applied"] is False
    assert dry_run["count"] == 1

    repair = asyncio.run(
        routes_personal.repair_work_blocked_leaf_invariant(
            routes_personal.WorkBlockedLeafInvariantRepairRequest(
                item_id="work-legacy-blocked-leaf",
                apply=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="legacy-blocked-leaf-repair",
                run_id="pytest-legacy-blocked-leaf",
            )
        )
    )
    assert repair["applied"] is True
    assert repair["before_count"] == 1
    assert repair["after_count"] == 0
    assert repair["repaired_count"] == 1

    blockers = conn.execute(
        "SELECT * FROM kanban_blockers WHERE item_id='work-legacy-blocked-leaf'"
    ).fetchall()
    assert len(blockers) == 1
    assert blockers[0]["status"] == "open"
    assert blockers[0]["title"].startswith("Preprocessing blocker/question:")
    provenance = json.loads(blockers[0]["provenance_json"])
    assert provenance["schema"] == routes_personal.KANBAN_BLOCKED_LEAF_INVARIANT_SCHEMA
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "repair_blocked_leaf_missing_blocker" in audit_actions

    repaired_audit = asyncio.run(
        routes_personal.audit_work_blocked_leaf_invariant(
            item_id="work-legacy-blocked-leaf",
            include_test_entries=True,
            limit=20,
        )
    )
    assert repaired_audit["count"] == 0


def test_work_automation_cannot_resolve_last_blocker_on_blocked_leaf(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-automation-blocked-leaf",
                title="Automation blocked leaf",
                body="Blocked reason/question: Needs an operator answer.",
                state_id="blocked",
                blocker_title="Operator answer required",
                blocker_body="Needs an operator answer.",
                actor="kanban-idle-worker",
                source_surface="kanban-automation-idle-worker",
                request_id="automation-blocked-leaf-create",
                run_id="pytest-automation-blocked-leaf",
            )
        )
    )
    blocker = created["created_blocker"]
    assert blocker["status"] == "open"

    with pytest.raises(routes_personal.HTTPException) as blocked_resolve:
        asyncio.run(
            routes_personal.update_work_blocker(
                blocker["blocker_id"],
                routes_personal.WorkBlockerUpsertRequest(
                    item_id="work-automation-blocked-leaf",
                    title=blocker["title"],
                    body=blocker["body_excerpt"],
                    status="resolved",
                    actor="kanban-idle-worker",
                    source_surface="kanban-automation-idle-worker",
                    request_id="automation-blocked-leaf-resolve",
                    run_id="pytest-automation-blocked-leaf",
                ),
            )
        )
    assert blocked_resolve.value.status_code == 409
    assert blocked_resolve.value.detail["error"] == (
        "kanban_automation_cannot_resolve_last_blocker_on_blocked_leaf"
    )
    row = conn.execute(
        "SELECT * FROM kanban_blockers WHERE blocker_id=?",
        (blocker["blocker_id"],),
    ).fetchone()
    assert row["status"] == "open"


def test_work_automation_preprocessing_malformed_decomposition_fails_without_children(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-malformed-root",
                title="Malformed root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-malformed-child",
                parent_item_id="work-preprocess-malformed-root",
                title="Malformed child",
                body="This item will receive malformed decomposition output.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )

    async def fake_local_ai_json_completion(*, messages, run_id, processor_kind=""):
        return {
            "model_alias": "TEST-KANBAN-LOCAL-AI",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                **_preprocessing_contract_fields(unit_title="Reject malformed decomposition"),
                "ready": False,
                "title": "Malformed decomposition",
                "summary": "The model returned one malformed child.",
                "rationale": "The output is intentionally malformed for the test.",
                "confidence": "medium",
                "uncertainty": "",
                "decomposition_items": [
                    {"title": "Valid first child", "body": "This must not be created."},
                    {"body": "Missing title should fail the whole mutation."},
                ],
                "affected_refs": ["xarta-kanban:item:work-preprocess-malformed-child"],
                "proof_refs": ["kanban_items:work-preprocess-malformed-child:body"],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_local_ai_json_completion,
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-preprocess-malformed-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    assert tick["processed_count"] == 1
    processed = tick["processed_markers"][0]
    assert processed["ok"] is False
    assert processed["status"] == "failed"
    assert "missing required field: title" in processed["error"]
    child_count = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_items
        WHERE parent_item_id='work-preprocess-malformed-child'
        """
    ).fetchone()["count"]
    assert child_count == 0
    marker = conn.execute(
        """
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id='work-preprocess-malformed-child'
        """
    ).fetchone()
    assert marker["status"] == "failed"
    assert marker["processed_source_hash"] == ""
    assert marker["processed_at"] == ""
    assert marker["retry_attempt_count"] == 1
    assert marker["retry_after_seconds"] == 5 * 60
    assert marker["next_retry_at"]
    assert marker["last_error_class"] == "llm_response_validation"
    event = conn.execute(
        """
        SELECT * FROM kanban_review_processor_failure_events
        WHERE marker_id=?
        """,
        (marker["marker_id"],),
    ).fetchone()
    assert event["processor_kind"] == "preprocessing"
    assert event["source_hash"] == marker["document_source_hash"]
    assert event["error_class"] == "llm_response_validation"
    assert event["attempt_number"] == 1
    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-preprocess-malformed-root")
    )
    assert status["preprocessing"]["retry_waiting_count"] == 1
    assert status["preprocessing"]["failure_aggregates"][0]["processor_kind"] == "preprocessing"
    assert status["failures"]["recent_events"][0]["processor_kind"] == "preprocessing"
    blocker = conn.execute(
        "SELECT * FROM kanban_blockers WHERE item_id='work-preprocess-malformed-child'"
    ).fetchone()
    assert blocker["status"] == "open"


def test_work_preprocessing_actionable_leaf_without_decomposition_marks_ready(
    monkeypatch,
    tmp_path,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        routes_personal.KANBAN_AUTOMATION_LOCAL_AI_MODEL_ENV,
        "TEST-KANBAN-LOCAL-AI",
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-actionable-root",
                title="Actionable root",
                body="Root item.",
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-actionable-child",
                parent_item_id="work-preprocess-actionable-root",
                title="Audit report files",
                body=(
                    "Investigate docs/reports/structure-audit to identify target files. "
                    "Proof: list candidate files and confirm active usage."
                ),
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )

    async def fake_local_ai_json_completion(*, messages, run_id, processor_kind=""):
        return {
            "model_alias": "TEST-KANBAN-LOCAL-AI",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                **_preprocessing_contract_fields(unit_title="Inspect the audit proof path"),
                "ready": True,
                "title": "Audit report files",
                "summary": "The next action is to inspect the proof path and list files.",
                "rationale": (
                    "No operator question or external blocker exists; this is actionable "
                    "investigation work for the current leaf."
                ),
                "confidence": "high",
                "uncertainty": "",
                "decomposition_items": [],
                "recommended_next_actions": [
                    "Scan docs/reports/structure-audit and list candidate files."
                ],
                "proof_refs": ["docs/reports/structure-audit"],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        fake_local_ai_json_completion,
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-preprocess-actionable-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    assert tick["processed_count"] == 1
    processed = tick["processed_markers"][0]
    assert processed["ok"] is True
    assert processed["status"] == "processed"
    marker = conn.execute(
        """
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id='work-preprocess-actionable-child'
        """
    ).fetchone()
    assert marker["status"] == "processed"
    assert marker["last_error"] == ""
    assert marker["processed_source_hash"] == marker["document_source_hash"]
    failure_count = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_review_processor_failure_events
        WHERE marker_id=?
        """,
        (marker["marker_id"],),
    ).fetchone()["count"]
    assert failure_count == 0
    blocker_count = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_blockers
        WHERE item_id='work-preprocess-actionable-child' AND status='open'
        """
    ).fetchone()["count"]
    assert blocker_count == 0
    hints = asyncio.run(
        routes_personal.get_work_item_agent_hints("work-preprocess-actionable-child")
    )
    readiness_marker = hints["agent_hints"]["metadata"]["context_readiness_marker"]
    assert readiness_marker["preprocessing_outcome"] == "ready"
    assert readiness_marker["implementation_scope"] == "current_item"
    assert "Scan docs/reports/structure-audit" in readiness_marker["recommended_next_actions"][0]
    decision = conn.execute(
        """
        SELECT * FROM kanban_review_decisions
        WHERE item_id='work-preprocess-actionable-child'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    metadata = json.loads(decision["metadata_json"])
    assert metadata["readiness_normalization"] is None
    assert metadata["execution_directive"]["recommended_mode"] == "serial"
    assert metadata["llm_payload"]["ready"] is True


def test_work_preprocessing_duplicate_outcome_needs_no_invented_children(
    monkeypatch,
    tmp_path,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    for item_id, parent_id, title, state_id in (
        ("work-duplicate-root", None, "Duplicate root", "doing"),
        ("work-canonical-owner", "work-duplicate-root", "Canonical owner", "done"),
        ("work-duplicate-leaf", "work-duplicate-root", "Duplicate leaf", "todo"),
    ):
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    parent_item_id=parent_id,
                    title=title,
                    body=f"{title} body and proof scope.",
                    state_id=state_id,
                    actor="codex-test",
                    source_surface="pytest",
                )
            )
        )

    async def duplicate_completion(*, messages, run_id, processor_kind=""):
        return {
            "model_alias": "TEST-KANBAN-DUPLICATE",
            "run_id": run_id,
            "content_excerpt": "{}",
            "payload": {
                **_preprocessing_contract_fields(unit_title="Confirm the canonical owner"),
                "ready": False,
                "outcome_type": "duplicate",
                "canonical_item_ref": "xarta-kanban:item:work-canonical-owner",
                "title": "Duplicate leaf",
                "summary": "The canonical completed card already owns this work.",
                "rationale": "No new implementation or decomposition is required.",
                "confidence": "high",
                "uncertainty": "",
                "blocking_codes": [],
                "recommended_next_actions": [],
                "decomposition_items": [],
                "affected_refs": ["xarta-kanban:item:work-canonical-owner"],
                "proof_refs": ["kanban_items:work-canonical-owner"],
            },
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_local_ai_json_completion",
        duplicate_completion,
    )
    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-duplicate-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    processed = tick["processed_markers"][0]
    assert processed["ok"] is True
    assert processed["status"] == "processed"
    assert processed["outcome_type"] == "duplicate"
    assert processed["canonical_item_ref"] == "xarta-kanban:item:work-canonical-owner"
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM kanban_items WHERE parent_item_id='work-duplicate-leaf'"
        ).fetchone()["count"]
        == 0
    )
    decision = conn.execute(
        "SELECT * FROM kanban_review_decisions WHERE item_id='work-duplicate-leaf'"
    ).fetchone()
    assert decision["decision_type"] == "preprocessing_duplicate"
    assert decision["status"] == "accepted"
    decision_metadata = json.loads(decision["metadata_json"])
    assert decision_metadata["canonical_item_ref"] == ("xarta-kanban:item:work-canonical-owner")
    hints = asyncio.run(routes_personal.get_work_item_agent_hints("work-duplicate-leaf"))
    readiness = hints["agent_hints"]["metadata"]["context_readiness_marker"]
    assert readiness["preprocessing_outcome"] == "duplicate"


def test_work_done_rich_document_updates_do_not_requeue_without_explicit_reprocess(
    monkeypatch,
    tmp_path,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-done-proof-update",
                title="Done proof update",
                body="Completed implementation whose final proof is being recorded.",
                state_id="done",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-done-feedback-capture",
                title="Done feedback capture",
                body="Completed implementation receiving final operator feedback.",
                state_id="done",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item_agent_session(
            "work-done-feedback-capture",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="kanban-agent-session-done-feedback",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                status="done",
                summary="Completed implementation feedback proof",
                actor="codex-test",
                source_surface="pytest-session",
            ),
        )
    )
    asyncio.run(
        routes_personal.update_work_item_detail_document(
            "work-done-proof-update",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Final implementation proof added after completion.",
                actor="codex-test",
                source_surface="pytest",
            ),
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-done-proof-update",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Final review proof; no new unresolved requirement.",
                actor="codex-test",
                source_surface="pytest",
            ),
        )
    )
    review_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-done-proof-update",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    preprocessing_scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-done-proof-update",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    assert review_scan["queued_count"] == 0
    assert preprocessing_scan["queued_count"] == 0
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM kanban_review_processor_markers "
            "WHERE item_id='work-done-proof-update'"
        ).fetchone()["count"]
        == 0
    )

    captured_without_reprocess = asyncio.run(
        routes_personal.append_work_item_review_feedback(
            "work-done-feedback-capture",
            routes_personal.WorkReviewFeedbackCaptureRequest(
                feedback_id="kanban-feedback-done-proof-only",
                feedback="Final proof is accepted; there is no unresolved requirement.",
                session_id="kanban-agent-session-done-feedback",
                outcome_summary="Accepted and implemented.",
                actor="codex-test",
                source_surface="pytest",
            ),
        )
    )
    assert captured_without_reprocess["review_processor"]["action"] == (
        "skipped_completed_no_reprocess"
    )
    assert captured_without_reprocess["review_processor"]["queued"] is False
    assert "Final proof is accepted" in captured_without_reprocess["review_document"]["body"]
    assert (
        conn.execute(
            "SELECT COUNT(*) AS count FROM kanban_review_processor_markers "
            "WHERE item_id='work-done-feedback-capture'"
        ).fetchone()["count"]
        == 0
    )

    captured_with_reprocess = asyncio.run(
        routes_personal.append_work_item_review_feedback(
            "work-done-feedback-capture",
            routes_personal.WorkReviewFeedbackCaptureRequest(
                feedback_id="kanban-feedback-done-unresolved",
                feedback="A new unresolved requirement was explicitly identified.",
                session_id="kanban-agent-session-done-feedback",
                reprocess_completed=True,
                actor="codex-test",
                source_surface="pytest",
            ),
        )
    )
    assert captured_with_reprocess["review_processor"]["action"] == "queued"
    assert captured_with_reprocess["review_processor"]["queued"] is True
    assert (
        captured_with_reprocess["review_processor"]["marker"]["metadata"]["reprocess_completed"]
        is True
    )

    explicit_review_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-done-proof-update",
                actor="codex-test",
                source_surface="pytest",
                reprocess_completed=True,
            )
        )
    )
    assert explicit_review_scan["queued_count"] == 1


def test_work_review_feedback_capture_delegates_whole_boundary_off_event_loop(monkeypatch):
    calls = []

    async def run_sync(func, *args):
        calls.append((func, args))
        return {"ok": True, "item_id": args[0]}

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)
    request = routes_personal.WorkReviewFeedbackCaptureRequest(
        feedback="Final accepted proof.",
        session_id="kanban-agent-session-off-loop",
    )
    result = asyncio.run(
        routes_personal.append_work_item_review_feedback("work-off-loop-feedback", request)
    )
    assert result == {"ok": True, "item_id": "work-off-loop-feedback"}
    assert calls == [
        (
            routes_personal._append_work_item_review_feedback_sync,
            ("work-off-loop-feedback", request),
        )
    ]


def test_work_item_link_creation_delegates_whole_boundary_off_event_loop(monkeypatch):
    calls = []

    async def run_sync(func, *args):
        calls.append((func, args))
        return {"ok": True, "link": {"link_id": "kanban-link-off-loop"}}

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)
    request = routes_personal.WorkItemLinkCreateRequest(
        target_item_id="work-off-loop-target",
        link_type="split_from",
        metadata={"proof": "event-loop-boundary"},
    )
    result = asyncio.run(routes_personal.create_work_item_link("work-off-loop-source", request))
    assert result["link"]["link_id"] == "kanban-link-off-loop"
    assert calls == [
        (
            routes_personal._create_work_item_link_sync,
            ("work-off-loop-source", request),
        )
    ]


def test_work_proposal_routes_delegate_postgres_boundaries_off_event_loop(monkeypatch):
    calls = []

    async def run_sync(func, *args, **kwargs):
        calls.append((func, args, kwargs))
        if func is routes_personal._work_proposal_surfaces_status_sync:
            return {"schema": routes_personal.KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA}
        return {"ok": True, "kind": "item"}

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)
    request = _proposal_request()
    created = asyncio.run(routes_personal.create_work_proposal_inbox(request))
    status = asyncio.run(routes_personal.get_work_proposal_surfaces_status(limit=7))
    assert created == {"ok": True, "kind": "item"}
    assert status["proposal_surfaces"]["schema"] == (
        routes_personal.KANBAN_PROPOSAL_SURFACES_CONTRACT_SCHEMA
    )
    assert calls == [
        (routes_personal._create_work_proposal_inbox_sync, (request,), {}),
        (routes_personal._work_proposal_surfaces_status_sync, (7,), {}),
    ]


def test_work_discussion_and_decision_writes_delegate_off_event_loop(monkeypatch):
    calls = []

    async def run_sync(func, *args):
        calls.append((func, args))
        return {"ok": True, "worker": func.__name__}

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)
    discussion = routes_personal.WorkDiscussionCreateRequest(
        discussion_id="discussion-off-loop",
        body="Bounded Discussion write.",
    )
    decision = routes_personal.WorkReviewDecisionCreateRequest(
        summary="Bounded durable decision.",
    )
    assert (
        asyncio.run(routes_personal.create_work_discussion("work-off-loop", discussion))["worker"]
        == "_create_work_discussion_sync"
    )
    assert (
        asyncio.run(routes_personal.record_work_item_review_decision("work-off-loop", decision))[
            "worker"
        ]
        == "_record_work_item_review_decision_sync"
    )
    assert calls == [
        (routes_personal._create_work_discussion_sync, ("work-off-loop", discussion)),
        (
            routes_personal._record_work_item_review_decision_sync,
            ("work-off-loop", decision),
        ),
    ]


def test_work_automation_missing_profile_api_key_is_retryable_failure(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setenv(
        "BLUEPRINTS_KANBAN_REVIEW_PROCESSOR_API_KEY_FILE",
        str(tmp_path / "missing-review-profile.env"),
    )
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-missing-model-root",
                title="Missing model root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-missing-model-child",
                parent_item_id="work-review-missing-model-root",
                title="Missing profile key child",
                body="This item has Review data but no profile API key.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-missing-model-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review work requiring the Hermes profile route.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-missing-model-doc",
            ),
        )
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-review-missing-model-root",
            max_scan_items=20,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    assert tick["processed_count"] == 1
    processed = tick["processed_markers"][0]
    assert processed["ok"] is False
    assert processed["status"] == "failed"
    assert "api_server_key" in processed["error"].lower()
    marker = conn.execute(
        """
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id='work-review-missing-model-child'
        """
    ).fetchone()
    assert marker["status"] == "failed"
    assert marker["processed_source_hash"] == ""
    assert marker["processed_at"] == ""
    assert marker["retry_attempt_count"] == 1
    assert marker["next_retry_at"]
    assert marker["last_error_class"] == "hermes_profile_configuration"
    event = conn.execute(
        """
        SELECT * FROM kanban_review_processor_failure_events
        WHERE marker_id=?
        """,
        (marker["marker_id"],),
    ).fetchone()
    assert event["error_class"] == "hermes_profile_configuration"
    assert event["provider_mode"] == routes_personal.KANBAN_AUTOMATION_PROFILE_PROVIDER_MODE
    assert event["model_alias"] == ("hermes-kanban-review-processor:openai-codex/gpt-5.5")
    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-missing-model-root")
    )
    assert status["failures"]["recent_events"][0]["error_class"] == ("hermes_profile_configuration")
    assert status["review_processor"]["retry_waiting_count"] == 1


def test_work_automation_idle_tick_does_not_claim_markers_queued_mid_tick(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-snapshot-root",
                title="Snapshot root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    for item_id, title in [
        ("work-preprocess-snapshot-first", "First child"),
        ("work-preprocess-snapshot-second", "Second child"),
    ]:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    parent_item_id="work-preprocess-snapshot-root",
                    title=title,
                    body=f"{title} body.",
                    state_id="todo",
                    actor="codex-test",
                    source_surface="pytest",
                )
            )
        )

    second_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-snapshot-second'"
    ).fetchone()
    second_source = routes_personal._work_preprocessing_context_source(conn, second_row)
    current_marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "context_packet_schema": "xarta.kanban.context_packet.v1",
        "item_id": "work-preprocess-snapshot-second",
        "canonical_code": "xarta-kanban:item:work-preprocess-snapshot-second",
        "marked_at": "2026-06-27T04:40:00Z",
        "marked_by": "codex-test",
        "context_hash": second_source["document_source_hash"],
        "component_hashes": {},
        "counts": second_source["counts"],
        "source_refs": second_source["source_refs"],
    }
    asyncio.run(
        routes_personal.update_work_item_agent_hints(
            "work-preprocess-snapshot-second",
            routes_personal.WorkAgentHintsUpdateRequest(
                metadata={"context_readiness_marker": current_marker},
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-snapshot-second-current",
            ),
        )
    )

    async def fake_process_claimed_marker(marker, *, holder_id, lease_token, run_id):
        assert marker["item_id"] == "work-preprocess-snapshot-first"
        row = conn.execute(
            "SELECT * FROM kanban_items WHERE item_id='work-preprocess-snapshot-second'"
        ).fetchone()
        source = routes_personal._work_preprocessing_context_source(conn, row)
        queued_row = routes_personal._work_preprocessing_marker_row(
            existing=None,
            item_id="work-preprocess-snapshot-second",
            source=source,
            meta={
                "actor": holder_id,
                "source_surface": "pytest",
                "request_id": "preprocess-snapshot-mid-tick",
                "run_id": run_id,
            },
            now="2026-06-27T04:41:00Z",
            reason="queued_mid_tick",
            scan_metadata={},
        )
        routes_personal._write_work_review_processor_marker(conn, queued_row)
        conn.commit()
        return {
            "ok": True,
            "processor_kind": marker["processor_kind"],
            "item_id": marker["item_id"],
            "marker_id": marker["marker_id"],
            "status": "synthetic-processed",
        }

    monkeypatch.setattr(
        routes_personal,
        "_process_work_automation_claimed_marker",
        fake_process_claimed_marker,
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            item_id="work-preprocess-snapshot-root",
            max_scan_items=20,
            max_process_items=2,
            holder_id="codex-test",
        )
    )
    assert tick["processed_count"] == 1
    assert tick["eligible_marker_count"] == 1
    assert tick["claim_results"][0]["claimed"] is True
    assert tick["claim_results"][1]["claimed"] is False
    assert tick["claim_results"][1]["reason"] == "no_queued_marker"
    mid_tick_marker = conn.execute(
        """
        SELECT * FROM kanban_review_processor_markers
        WHERE item_id='work-preprocess-snapshot-second'
        """
    ).fetchone()
    assert mid_tick_marker["status"] == "queued"


def test_work_automation_idle_tick_handles_claim_lease_race(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    conn.commit()

    async def fake_claim_next_work_review_processor_marker(body):
        raise routes_personal.HTTPException(409, "Review Processor lease is not active")

    monkeypatch.setattr(
        routes_personal,
        "claim_next_work_review_processor_marker",
        fake_claim_next_work_review_processor_marker,
    )

    tick = asyncio.run(
        routes_personal.run_work_kanban_automation_idle_tick(
            max_scan_items=1,
            max_process_items=1,
            holder_id="codex-test",
        )
    )
    assert tick["ok"] is True
    assert tick["lease_acquired"] is True
    assert tick["processed_count"] == 0
    assert tick["claim_results"] == [
        {
            "claimed": False,
            "reason": "lease_not_active_during_claim",
            "detail": "Review Processor lease is not active",
            "marker_id": "",
            "processor_kind": "",
        }
    ]
    assert tick["release"]["released"] is True


def test_work_preprocessing_context_ignores_processor_marker_blockers(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-blocker-source",
                title="Preprocessing blocker source",
                body="Synthetic processor marker blockers must not block their own preprocessing.",
                state_id="blocked",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-blocker-source-create",
            )
        )
    )
    conn.execute(
        """
        INSERT INTO kanban_blockers (
            blocker_id, item_id, title, status, blocked_by_ref, provenance_json
        )
        VALUES (?, ?, ?, 'open', ?, ?)
        """,
        (
            "kanban-blocker-processor-self-loop",
            "work-preprocess-blocker-source",
            "Preprocessing context readiness marker failed",
            "kanban_review_processor_markers:marker-preprocess-self-loop",
            json.dumps(
                {
                    "schema": routes_personal.KANBAN_PROCESSOR_MARKER_BLOCKER_PROVENANCE_SCHEMA,
                    "marker_id": "marker-preprocess-self-loop",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
        ),
    )
    conn.commit()

    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-blocker-source'"
    ).fetchone()
    source = routes_personal._work_preprocessing_context_source(conn, item_row)
    assert source["counts"]["blocker_count"] == 0

    conn.execute(
        """
        INSERT INTO kanban_blockers (
            blocker_id, item_id, title, status, blocked_by_ref, provenance_json
        )
        VALUES (?, ?, ?, 'open', '', '{}')
        """,
        (
            "kanban-blocker-real-open",
            "work-preprocess-blocker-source",
            "Real operator blocker",
        ),
    )
    conn.commit()

    source = routes_personal._work_preprocessing_context_source(conn, item_row)
    assert source["counts"]["blocker_count"] == 1


def test_work_preprocessing_idle_scan_resolves_stale_self_marker_blocker_only(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-stale-marker-blocker",
                title="Stale marker blocker",
                body="A stale processor marker blocker should be cleared safely.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-stale-marker-blocker-create",
            )
        )
    )
    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-stale-marker-blocker'"
    ).fetchone()
    source = routes_personal._work_preprocessing_context_source(conn, item_row)
    meta = {
        "actor": "codex-test",
        "source_surface": "pytest",
        "request_id": "preprocess-stale-marker-row",
        "run_id": "pytest-preprocess-stale-marker-row",
    }
    failed_row = routes_personal._work_preprocessing_marker_row(
        existing=None,
        item_id="work-preprocess-stale-marker-blocker",
        source=source,
        meta=meta,
        now="2026-06-27T04:31:00Z",
        reason="test_failed_marker",
        scan_metadata={},
    )
    failed_row["status"] = "failed"
    failed_row["last_error"] = "open_blockers;llm_reported_not_ready"
    saved_failed = routes_personal._write_work_review_processor_marker(conn, failed_row)
    marker_blocker = routes_personal._upsert_work_processor_marker_blocker(
        conn,
        saved_failed,
        meta=meta,
        now="2026-06-27T04:31:00Z",
    )
    assert marker_blocker is not None
    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET status='processed', processed_source_hash=document_source_hash
        WHERE marker_id=?
        """,
        (saved_failed["marker_id"],),
    )
    conn.execute(
        """
        INSERT INTO kanban_blockers (
            blocker_id, item_id, title, status, blocked_by_ref, provenance_json
        )
        VALUES (?, ?, ?, 'open', '', '{}')
        """,
        (
            "kanban-blocker-real-stays-open",
            "work-preprocess-stale-marker-blocker",
            "Real blocker stays open",
        ),
    )
    conn.commit()

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-stale-marker-blocker",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-stale-marker-scan",
            )
        )
    )
    assert scan["stale_marker_blocker_resolved_count"] == 1
    stale_blocker = conn.execute(
        "SELECT * FROM kanban_blockers WHERE blocker_id=?",
        (marker_blocker["blocker_row"]["blocker_id"],),
    ).fetchone()
    real_blocker = conn.execute(
        "SELECT * FROM kanban_blockers WHERE blocker_id='kanban-blocker-real-stays-open'"
    ).fetchone()
    assert stale_blocker["status"] == "resolved"
    assert real_blocker["status"] == "open"
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "resolve_stale_processor_marker_blocker" in audit_actions


def test_work_preprocessing_idle_scan_resolves_current_failed_marker_blocker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-current-failed",
                title="Current failed preprocessing marker",
                body="Readiness is current, so a stale failed marker should not keep blocking.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-current-failed-create",
            )
        )
    )

    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-current-failed'"
    ).fetchone()
    initial_source = routes_personal._work_preprocessing_context_source(conn, item_row)
    current_marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "context_packet_schema": "xarta.kanban.context_packet.v1",
        "item_id": "work-preprocess-current-failed",
        "canonical_code": "xarta-kanban:item:work-preprocess-current-failed",
        "marked_at": "2026-06-27T04:20:00Z",
        "marked_by": "codex-test",
        "context_hash": initial_source["document_source_hash"],
        "component_hashes": {},
        "counts": initial_source["counts"],
        "source_refs": initial_source["source_refs"],
    }
    asyncio.run(
        routes_personal.update_work_item_agent_hints(
            "work-preprocess-current-failed",
            routes_personal.WorkAgentHintsUpdateRequest(
                metadata={"context_readiness_marker": current_marker},
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-current-failed-marker",
            ),
        )
    )

    meta = {
        "actor": "codex-test",
        "source_surface": "pytest",
        "request_id": "preprocess-current-failed-row",
        "run_id": "pytest-preprocess-current-failed-row",
    }
    failed_row = routes_personal._work_preprocessing_marker_row(
        existing=None,
        item_id="work-preprocess-current-failed",
        source=initial_source,
        meta=meta,
        now="2026-06-27T04:21:00Z",
        reason="test_failed_marker",
        scan_metadata={},
    )
    failed_row["status"] = "failed"
    failed_row["last_error"] = "open_blockers;llm_reported_not_ready"
    saved_failed = routes_personal._write_work_review_processor_marker(conn, failed_row)
    marker_blocker = routes_personal._upsert_work_processor_marker_blocker(
        conn,
        saved_failed,
        meta=meta,
        now="2026-06-27T04:21:00Z",
    )
    assert marker_blocker is not None
    assert marker_blocker["blocker_row"]["status"] == "open"
    conn.commit()

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-current-failed",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-current-failed-scan",
            )
        )
    )
    assert scan["queued_count"] == 0
    assert scan["current_ready_count"] == 1
    assert scan["cancelled_current_count"] == 1
    cancelled = scan["cancelled_markers"][0]
    assert cancelled["status"] == "cancelled"
    assert cancelled["last_error"] == "preprocessing_current"
    assert cancelled["metadata"]["cancelled_previous_status"] == "failed"

    blocker_row = conn.execute(
        "SELECT * FROM kanban_blockers WHERE blocker_id=?",
        (marker_blocker["blocker_row"]["blocker_id"],),
    ).fetchone()
    assert blocker_row["status"] == "resolved"
    assert "no longer blocks agent completion" in blocker_row["body_excerpt"]

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_markers" in sync_tables
    assert "kanban_blockers" in sync_tables


def test_work_preprocessing_idle_scan_queues_missing_readiness(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-root",
                title="Preprocess root",
                body="Root item for preprocessing queue proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-missing",
                parent_item_id="work-preprocess-root",
                title="Missing readiness child",
                body="This child needs preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-missing-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-current",
                parent_item_id="work-preprocess-root",
                title="Current readiness child",
                body="This child already has current preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-current-create",
            )
        )
    )

    current_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-current'"
    ).fetchone()
    current_source = routes_personal._work_preprocessing_context_source(conn, current_row)
    current_marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "context_packet_schema": "xarta.kanban.context_packet.v1",
        "item_id": "work-preprocess-current",
        "canonical_code": "xarta-kanban:item:work-preprocess-current",
        "marked_at": "2026-06-27T04:00:00Z",
        "marked_by": "codex-test",
        "context_hash": current_source["document_source_hash"],
        "component_hashes": {},
        "counts": current_source["counts"],
        "source_refs": current_source["source_refs"],
    }
    asyncio.run(
        routes_personal.update_work_item_agent_hints(
            "work-preprocess-current",
            routes_personal.WorkAgentHintsUpdateRequest(
                metadata={"context_readiness_marker": current_marker},
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-current-marker",
            ),
        )
    )
    conn.execute("DELETE FROM sync_queue")
    conn.commit()

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-scan-trigger",
            )
        )
    )
    assert scan["ok"] is True
    assert scan["schema"] == routes_personal.KANBAN_PREPROCESSING_QUEUE_SCHEMA
    assert scan["idle"] is True
    assert scan["scanned_count"] == 2
    assert scan["eligible_preprocessing_count"] == 1
    assert scan["current_ready_count"] == 1
    assert scan["queued_count"] == 1
    marker = scan["queued_markers"][0]
    assert marker["processor_kind"] == "preprocessing"
    assert marker["document_type"] == "context_readiness"
    assert marker["item_id"] == "work-preprocess-missing"
    assert marker["status"] == "queued"
    assert marker["metadata"]["reason"] == "missing_readiness_marker"
    assert marker["metadata"]["readiness_reason"] == "missing_readiness_marker"
    assert scan["scheduler"]["queue_length"] == 1

    status = asyncio.run(routes_personal.get_work_automation_status(item_id="work-preprocess-root"))
    max_scan_range = status["idle_worker"]["range_config"]["max_scan_items"]
    assert status["idle_worker"]["max_scan_items"] == max_scan_range["effective"]
    assert max_scan_range["env_name"] == "BLUEPRINTS_KANBAN_AUTOMATION_MAX_SCAN_ITEMS"
    assert max_scan_range["default"] == routes_personal.KANBAN_AUTOMATION_DEFAULT_MAX_SCAN_ITEMS
    assert max_scan_range["min"] == 1
    assert max_scan_range["max"] == routes_personal.KANBAN_AUTOMATION_MAX_SCAN_ITEMS_CAP
    assert status["preprocessing"]["queue_length"] == 1
    assert status["preprocessing"]["scheduler"]["queue_length"] == 1
    assert status["preprocessing"]["markers"][0]["processor_kind"] == "preprocessing"
    assert status["review_processor"]["queue_length"] == 0

    same_scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-scan-trigger-again",
            )
        )
    )
    assert same_scan["queued_count"] == 0
    assert same_scan["unchanged_pending_count"] == 1

    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET status='processing',
            processing_started_at='2026-06-27T04:05:00Z',
            processing_expires_at='2999-01-01T00:00:00Z'
        WHERE marker_id=?
        """,
        (marker["marker_id"],),
    )
    conn.commit()
    active_scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-scan-active",
            )
        )
    )
    assert active_scan["idle"] is False
    assert active_scan["reason"] == "active_preprocessing"
    assert active_scan["blocked_by_active_count"] == 1
    assert active_scan["queued_count"] == 0

    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET status='queued', processing_started_at='', processing_expires_at=''
        WHERE marker_id=?
        """,
        (marker["marker_id"],),
    )
    missing_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-missing'"
    ).fetchone()
    missing_source = routes_personal._work_preprocessing_context_source(conn, missing_row)
    missing_marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "context_packet_schema": "xarta.kanban.context_packet.v1",
        "item_id": "work-preprocess-missing",
        "canonical_code": "xarta-kanban:item:work-preprocess-missing",
        "marked_at": "2026-06-27T04:10:00Z",
        "marked_by": "codex-test",
        "context_hash": missing_source["document_source_hash"],
        "component_hashes": {},
        "counts": missing_source["counts"],
        "source_refs": missing_source["source_refs"],
    }
    asyncio.run(
        routes_personal.update_work_item_agent_hints(
            "work-preprocess-missing",
            routes_personal.WorkAgentHintsUpdateRequest(
                metadata={"context_readiness_marker": missing_marker},
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-missing-marker",
            ),
        )
    )
    cancel_scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-scan-current",
            )
        )
    )
    assert cancel_scan["queued_count"] == 0
    assert cancel_scan["cancelled_current_count"] == 1
    assert cancel_scan["scheduler"]["queue_length"] == 0
    cancelled = cancel_scan["cancelled_markers"][0]
    assert cancelled["status"] == "cancelled"
    assert cancelled["last_error"] == "preprocessing_current"
    assert cancelled["metadata"]["cancelled_previous_status"] == "queued"

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_markers" in sync_tables
    assert "kanban_audit_log" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "trigger_preprocessing_idle_scan" in audit_actions


def test_work_queued_processor_markers_prioritize_item_urgency_before_queue_age(
    monkeypatch,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    fixtures = [
        ("work-old-low-review", "low", "review", "2026-07-15T08:00:00Z"),
        ("work-new-critical-preprocess", "critical", "preprocessing", "2026-07-15T11:00:00Z"),
        ("work-new-critical-review", "critical", "review", "2026-07-15T11:01:00Z"),
    ]
    for item_id, priority_id, processor_kind, queued_at in fixtures:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id=item_id,
                    title=item_id,
                    body="Bounded queue-order fixture.",
                    state_id="todo",
                    priority_id=priority_id,
                    actor="codex-test",
                    source_surface="pytest",
                )
            )
        )
        conn.execute(
            """
            INSERT INTO kanban_review_processor_markers (
                marker_id, item_id, processor_kind, document_type,
                document_source_hash, queued_at, status, provider_mode
            ) VALUES (?, ?, ?, ?, ?, ?, 'queued', 'required-hermes-kanban-llm')
            """,
            (
                f"marker-{item_id}",
                item_id,
                processor_kind,
                processor_kind,
                f"sha256:{item_id}",
                queued_at,
            ),
        )
    conn.commit()

    assert routes_personal._work_queued_processor_marker_ids(conn, []) == [
        "marker-work-new-critical-review",
        "marker-work-new-critical-preprocess",
        "marker-work-old-low-review",
    ]


def test_work_preprocessing_idle_scan_cancels_non_todo_pending_marker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-old-backlog",
                title="Old queued backlog preprocessing",
                body="This old pending preprocessing marker is no longer eligible.",
                state_id="backlog",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-old-backlog-create",
            )
        )
    )

    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-old-backlog'"
    ).fetchone()
    source = routes_personal._work_preprocessing_context_source(conn, item_row)
    marker_row = routes_personal._work_preprocessing_marker_row(
        existing=None,
        item_id="work-preprocess-old-backlog",
        source=source,
        meta={
            "actor": "codex-test",
            "source_surface": "pytest",
            "request_id": "preprocess-old-backlog-marker",
            "run_id": "pytest-preprocess-old-backlog-marker",
        },
        now="2026-06-28T08:01:17Z",
        reason="old_global_preprocessing_run",
        scan_metadata={"worker_schema": routes_personal.KANBAN_AUTOMATION_IDLE_WORKER_SCHEMA},
    )
    saved_marker = routes_personal._write_work_review_processor_marker(conn, marker_row)
    conn.commit()

    assert saved_marker["status"] == "queued"
    assert routes_personal._work_queued_processor_marker_ids(conn, []) == []

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-old-backlog-scan",
            )
        )
    )

    assert scan["ok"] is True
    assert scan["cancelled_invalid_count"] == 1
    assert scan["queued_count"] == 0
    assert scan["scheduler"]["queue_length"] == 0
    cancelled = scan["cancelled_invalid_markers"][0]
    assert cancelled["marker_id"] == saved_marker["marker_id"]
    assert cancelled["status"] == "cancelled"
    assert cancelled["last_error"] == "preprocessing_not_todo"
    assert cancelled["metadata"]["contract_schema"] == (
        routes_personal.KANBAN_AUTOMATION_IDLE_WORKER_CONTRACT_SCHEMA
    )

    marker = conn.execute(
        "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
        (saved_marker["marker_id"],),
    ).fetchone()
    assert marker["status"] == "cancelled"
    assert marker["last_error"] == "preprocessing_not_todo"
    assert routes_personal._work_queued_processor_marker_ids(conn, []) == []


def test_work_preprocessing_idle_scan_skips_topic_container_without_marker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-topic-container",
                title="Topic Container",
                body=(
                    "Topic/container holding area for related ideas. "
                    "Broad topic; collect related work here."
                ),
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-topic-container-create",
            )
        )
    )
    conn.execute(
        """
        UPDATE kanban_items
        SET provenance_json=?
        WHERE item_id='work-preprocess-topic-container'
        """,
        (
            json.dumps(
                {
                    "preprocessing": {
                        "classification": "topic_container",
                        "eligible": False,
                    }
                }
            ),
        ),
    )
    conn.commit()

    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-preprocess-topic-container'"
    ).fetchone()
    source = routes_personal._work_preprocessing_context_source(conn, item_row)
    assert source["classification"]["classification"] == "topic_container"
    assert source["needs_preprocessing"] is False
    assert source["reason"] == "preprocessing_skipped_topic_container"

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-topic-container",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-topic-container-scan",
            )
        )
    )
    assert scan["scanned_count"] == 1
    assert scan["eligible_preprocessing_count"] == 0
    assert scan["current_ready_count"] == 1
    assert scan["queued_count"] == 0
    marker_count = conn.execute(
        "SELECT COUNT(*) AS count FROM kanban_review_processor_markers"
    ).fetchone()["count"]
    assert marker_count == 0


def test_work_preprocessing_idle_scan_skips_automation_excluded_subtree(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-excluded-root",
                title="Preprocessing excluded root",
                body="Import or topic branch that automation must not decompose.",
                state_id="todo",
                automation_excluded=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-excluded-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-excluded-child",
                parent_item_id="work-preprocess-excluded-root",
                title="Preprocessing excluded child",
                body="Leaf card that would otherwise be considered for preprocessing.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-excluded-child-create",
            )
        )
    )

    scan = asyncio.run(
        routes_personal.trigger_work_preprocessing_idle_scan(
            routes_personal.WorkPreprocessingIdleScanRequest(
                item_id="work-preprocess-excluded-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="preprocess-excluded-scan",
            )
        )
    )
    assert scan["scanned_count"] == 0
    assert scan["eligible_preprocessing_count"] == 0
    assert scan["queued_count"] == 0
    assert scan["scheduler"]["queue_length"] == 0
    marker_count = conn.execute(
        "SELECT COUNT(*) AS count FROM kanban_review_processor_markers"
    ).fetchone()["count"]
    assert marker_count == 0


def test_work_preprocessing_create_child_skips_automation_excluded_parent(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-direct-excluded-root",
                title="Direct excluded root",
                body="Excluded branch.",
                automation_excluded=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="direct-excluded-root-create",
            )
        )
    )
    parent = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-preprocess-direct-excluded-parent",
                parent_item_id="work-preprocess-direct-excluded-root",
                title="Direct excluded parent",
                body="Idle worker must not decompose this card.",
                actor="codex-test",
                source_surface="pytest",
                request_id="direct-excluded-parent-create",
            )
        )
    )["item"]
    parent_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id=?", (parent["item_id"],)
    ).fetchone()
    result = asyncio.run(
        routes_personal._work_preprocessing_create_decomposition_children(
            parent_item=parent_row,
            payload={"decomposition_items": [{"title": "Generated child", "body": "Nope."}]},
            holder_id="kanban-idle-worker",
            run_id="direct-excluded-run",
            marker_id="direct-excluded-marker",
        )
    )
    assert result["skipped_reason"] == "automation_excluded"
    assert result["total_count"] == 0
    child_count = conn.execute(
        """
        SELECT COUNT(*) AS count FROM kanban_items
        WHERE parent_item_id='work-preprocess-direct-excluded-parent'
        """
    ).fetchone()["count"]
    assert child_count == 0

    with pytest.raises(routes_personal.HTTPException) as blocked_create:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id="work-preprocess-direct-excluded-child",
                    parent_item_id="work-preprocess-direct-excluded-parent",
                    title="Directly generated child",
                    actor="kanban-idle-worker",
                    source_surface="kanban-automation-idle-worker",
                    request_id="direct-excluded-child-create",
                )
            )
        )
    assert blocked_create.value.status_code == 409
    assert blocked_create.value.detail["error"] == "kanban_automation_excluded_branch"


def test_work_review_processor_marker_lifecycle_timeout_and_supersede(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-timeout-root",
                title="Review timeout root",
                body="Root item for timeout proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-timeout-child",
                parent_item_id="work-review-timeout-root",
                title="Review timeout child",
                body="Child item with Review lifecycle data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-timeout-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review lifecycle pass one.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-timeout-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]

    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-timeout",
                item_id="work-review-timeout-child",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-lease",
            )
        )
    )
    token = acquired["lease"]["lease_token"]

    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-timeout",
                lease_token=token,
                item_id="work-review-timeout-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-claim",
            )
        )
    )
    assert claimed["claimed"] is True
    assert claimed["marker"]["status"] == "processing"
    assert claimed["marker"]["attempt_count"] == 1
    assert claimed["marker"]["processing_expires_at"]

    status_claimed = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-timeout-root")
    )
    claimed_scheduler = status_claimed["review_processor"]["scheduler"]
    assert status_claimed["review_processor"]["queue_length"] == 0
    assert claimed_scheduler["active_count"] == 1
    assert claimed_scheduler["pending_count"] == 1
    assert claimed_scheduler["by_status"]["processing"] == 1

    with pytest.raises(routes_personal.HTTPException) as wrong_worker:
        asyncio.run(
            routes_personal.claim_next_work_review_processor_marker(
                routes_personal.WorkReviewProcessorMarkerClaimRequest(
                    holder_id="codex-other",
                    lease_token=token,
                    item_id="work-review-timeout-root",
                    timeout_seconds=120,
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="review-timeout-claim-wrong-worker",
                )
            )
        )
    assert wrong_worker.value.status_code == 409
    conn.rollback()

    duplicate_claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-timeout",
                lease_token=token,
                item_id="work-review-timeout-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-claim-duplicate",
            )
        )
    )
    assert duplicate_claim["claimed"] is False
    assert duplicate_claim["reason"] == "no_queued_marker"

    completed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            claimed["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-timeout",
                lease_token=token,
                document_source_hash=marker["document_source_hash"],
                decision_id="kanban-decision-timeout-proof",
                status="processed",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-complete",
            ),
        )
    )
    assert completed["completed"] is True
    assert completed["marker"]["status"] == "processed"
    assert completed["marker"]["processed_source_hash"] == marker["document_source_hash"]
    assert completed["marker"]["processed_at"]
    processed_same_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-timeout-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-scan-processed-same",
            )
        )
    )
    assert processed_same_scan["queued_count"] == 0
    assert processed_same_scan["unchanged_current_count"] == 1

    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-timeout-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review lifecycle pass two, after processed marker.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-review-change",
            ),
        )
    )
    changed_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-timeout-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-scan-changed",
            )
        )
    )
    assert changed_scan["queued_count"] == 1
    assert changed_scan["queued_markers"][0]["status"] == "queued"
    assert (
        changed_scan["queued_markers"][0]["processed_source_hash"] == marker["document_source_hash"]
    )

    claimed_again = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-timeout",
                lease_token=token,
                item_id="work-review-timeout-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-claim-again",
            )
        )
    )
    assert claimed_again["claimed"] is True
    assert claimed_again["marker"]["attempt_count"] == 2
    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET processing_expires_at='2026-06-27T04:00:00+01:00'
        WHERE marker_id=?
        """,
        (claimed_again["marker"]["marker_id"],),
    )
    conn.commit()
    real_utc_now_iso = routes_personal._utc_now_iso
    monkeypatch.setattr(routes_personal, "_utc_now_iso", lambda: "2026-06-27T03:30:00Z")
    assert routes_personal._parse_utc_datetime("2026-06-27T04:00:00+01:00") == (
        routes_personal._parse_utc_datetime("2026-06-27T03:00:00Z")
    )
    timed_out = asyncio.run(
        routes_personal.requeue_timed_out_work_review_processor_markers(
            routes_personal.WorkReviewProcessorTimeoutRequeueRequest(
                item_id="work-review-timeout-root",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-requeue",
            )
        )
    )
    assert timed_out["requeued_count"] == 1
    assert timed_out["requeued_markers"][0]["status"] == "queued"
    assert timed_out["requeued_markers"][0]["last_error"] == "processing_timeout"
    assert timed_out["requeued_markers"][0]["metadata"]["last_outcome_status"] == (
        "timeout_requeued"
    )
    assert timed_out["requeued_markers"][0]["metadata"]["last_error"] == "processing_timeout"
    monkeypatch.setattr(routes_personal, "_utc_now_iso", real_utc_now_iso)

    claimed_third = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-timeout",
                lease_token=token,
                item_id="work-review-timeout-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-claim-third",
            )
        )
    )
    assert claimed_third["marker"]["status"] == "processing"
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-timeout-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review lifecycle pass three, changed during processing.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-review-supersede",
            ),
        )
    )
    supersede_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-timeout-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-timeout-scan-supersede",
            )
        )
    )
    assert supersede_scan["queued_count"] == 1
    superseded = supersede_scan["queued_markers"][0]
    assert superseded["status"] == "queued"
    assert superseded["last_error"] == "review_changed_during_processing"
    assert superseded["superseded_at"]
    assert superseded["superseded_by_source_hash"] == superseded["document_source_hash"]
    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-timeout-root")
    )
    assert status["review_processor"]["scheduler"]["timeout_count"] == 0
    assert status["review_processor"]["scheduler"]["superseded_count"] == 1

    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "claim_review_processor_marker" in audit_actions
    assert "complete_review_processor_marker" in audit_actions
    assert "requeue_review_processor_timeouts" in audit_actions


def test_work_review_processor_completion_requeues_superseded_processing_marker(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-superseded-complete",
                title="Superseded completion",
                body="Root item for superseded completion proof.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-superseded-complete",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review text before claim.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete-review",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-superseded-complete",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-superseded-complete",
                item_id="work-review-superseded-complete",
                ttl_seconds=300,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-superseded-complete",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-superseded-complete",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete-claim",
            )
        )
    )
    assert claimed["claimed"] is True
    replacement_hash = "sha256:" + ("a" * 64)
    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET document_source_hash=?, source_hash=?
        WHERE marker_id=?
        """,
        (replacement_hash, replacement_hash, marker["marker_id"]),
    )
    conn.commit()

    completed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            marker["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-superseded-complete",
                lease_token=acquired["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="processed",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-superseded-complete",
            ),
        )
    )
    assert completed["completed"] is False
    assert completed["reason"] == "superseded_source_hash"
    assert completed["marker"]["status"] == "queued"
    assert completed["marker"]["last_error"] == "superseded_source_hash"
    assert completed["marker"]["processing_started_at"] == ""
    assert completed["marker"]["processing_expires_at"] == ""
    assert completed["marker"]["superseded_at"]
    assert completed["marker"]["superseded_by_source_hash"] == replacement_hash
    assert completed["audit"]["result"] == "superseded_source_hash"


def test_work_review_processor_failed_completion_records_outcome(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-failed-root",
                title="Review failed root",
                body="Root item for failed completion proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-failed-child",
                parent_item_id="work-review-failed-root",
                title="Review failed child",
                body="Child item with failed Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-failed-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review failed completion pass one.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-failed-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-failed",
                item_id="work-review-failed-child",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-failed",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-failed-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-claim",
            )
        )
    )
    completed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            claimed["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-failed",
                lease_token=acquired["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="failed",
                error="provider_failed",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-complete",
            ),
        )
    )
    failed_marker = completed["marker"]
    assert failed_marker["status"] == "failed"
    assert failed_marker["last_error"] == "provider_failed"
    assert failed_marker["processed_at"] == ""
    assert failed_marker["processed_source_hash"] == ""
    assert failed_marker["processed_document_updated_at"] == ""
    assert failed_marker["last_successful_source_hash"] == ""
    assert failed_marker["retry_state"] == "retry_waiting"
    assert failed_marker["retry_waiting"] is True
    assert failed_marker["retry_attempt_count"] == 1
    assert failed_marker["retry_after_seconds"] == 5 * 60
    assert failed_marker["next_retry_at"]
    assert failed_marker["last_failure_event_id"]
    assert failed_marker["last_failure_source_hash"] == marker["document_source_hash"]
    assert (
        failed_marker["retry_policy_version"] == routes_personal.KANBAN_REVIEW_RETRY_POLICY_VERSION
    )
    assert failed_marker["metadata"]["last_outcome_status"] == "failed"
    assert failed_marker["metadata"]["retryable"] is True
    assert failed_marker["metadata"]["failure_attempt"] == 1
    failure_event = completed["failure_event"]
    assert failure_event["failure_event_id"] == failed_marker["last_failure_event_id"]
    assert failure_event["item_id"] == "work-review-failed-child"
    assert failure_event["marker_id"] == failed_marker["marker_id"]
    assert failure_event["processor_kind"] == "review"
    assert failure_event["source_hash"] == marker["document_source_hash"]
    assert failure_event["error_message"] == "provider_failed"
    assert failure_event["attempt_number"] == 1
    assert failure_event["retry_after_seconds"] == 5 * 60
    assert failure_event["next_retry_at"] == failed_marker["next_retry_at"]
    assert (
        failure_event["retry_policy_version"] == routes_personal.KANBAN_REVIEW_RETRY_POLICY_VERSION
    )
    processor_blocker = completed["processor_blocker"]
    assert processor_blocker["item_id"] == "work-review-failed-child"
    assert processor_blocker["status"] == "open"
    assert (
        processor_blocker["blocked_by_ref"]
        == f"kanban_review_processor_markers:{failed_marker['marker_id']}"
    )
    assert "provider_failed" in processor_blocker["body_excerpt"]

    detail = asyncio.run(routes_personal.get_work_item_detail("work-review-failed-child"))
    assert detail["counts"]["blockers"] == 1
    assert detail["blockers"][0]["blocker_id"] == processor_blocker["blocker_id"]

    same_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-failed-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-scan-again",
            )
        )
    )
    assert same_scan["queued_count"] == 0
    assert same_scan["unchanged_failed_count"] == 1

    early_claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-failed",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-failed-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-early-retry-claim",
            )
        )
    )
    assert early_claim["claimed"] is False
    assert early_claim["reason"] == "no_queued_marker"

    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-failed-root")
    )
    assert status["review_processor"]["retry_waiting_count"] == 1
    assert status["review_processor"]["scheduler"]["failure_event_count"] == 1
    assert (
        status["failures"]["recent_events"][0]["failure_event_id"]
        == failure_event["failure_event_id"]
    )
    aggregate = status["failures"]["aggregates"][0]
    assert aggregate["item_title"] == "Review failed child"
    assert aggregate["processor_kind"] == "review"
    assert aggregate["attempt_count"] == 1
    assert aggregate["last_error"] == "provider_failed"
    assert aggregate["retry_waiting"] is True
    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_failure_events" in sync_tables

    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET next_retry_at='2000-01-01T00:00:00Z'
        WHERE marker_id=?
        """,
        (failed_marker["marker_id"],),
    )
    conn.commit()
    retry_claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-failed",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-failed-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-retry-claim",
            )
        )
    )
    assert retry_claim["claimed"] is True
    retry_complete = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            retry_claim["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-failed",
                lease_token=acquired["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="failed",
                error="provider_failed_again",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-retry-complete",
            ),
        )
    )
    assert retry_complete["marker"]["processed_source_hash"] == ""
    assert retry_complete["marker"]["retry_attempt_count"] == 2
    assert retry_complete["marker"]["retry_after_seconds"] == 20 * 60
    assert retry_complete["failure_event"]["attempt_number"] == 2

    repeated_status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-failed-root")
    )
    assert repeated_status["failures"]["repeated_failure_count"] == 1
    repeated_aggregate = repeated_status["failures"]["aggregates"][0]
    assert repeated_aggregate["attempt_count"] == 2
    assert repeated_aggregate["last_error"] == "provider_failed_again"

    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-failed-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review failed completion pass two supersedes retry wait.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-review-change",
            ),
        )
    )
    changed_scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-failed-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-failed-scan-changed",
            )
        )
    )
    assert changed_scan["queued_count"] == 1
    changed_marker = changed_scan["queued_markers"][0]
    assert changed_marker["status"] == "queued"
    assert changed_marker["document_source_hash"] != marker["document_source_hash"]
    assert changed_marker["processed_source_hash"] == ""
    assert changed_marker["next_retry_at"] == ""
    assert changed_marker["retry_attempt_count"] == 0
    assert changed_marker["last_failure_event_id"] == ""


def test_work_review_processor_status_clears_active_retry_summary_after_success(
    monkeypatch,
    tmp_path,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-recovered-root",
                title="Recovered retry root",
                body="Root item.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-recovered-child",
                parent_item_id="work-review-recovered-root",
                title="Recovered retry child",
                body="Child item with transient Review failure.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-recovered-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review content that first fails and then succeeds.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-doc",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-recovered-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    lease = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-recovered",
                item_id="work-review-recovered-child",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-lease",
            )
        )
    )
    first_claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-recovered",
                lease_token=lease["lease"]["lease_token"],
                item_id="work-review-recovered-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-first-claim",
            )
        )
    )
    failed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            first_claim["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-recovered",
                lease_token=lease["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="failed",
                error="provider_failed",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-fail",
            ),
        )
    )
    failure_event = failed["failure_event"]
    active_prune = asyncio.run(
        routes_personal.prune_work_automation_failure_events(
            routes_personal.WorkAutomationFailurePruneRequest(
                item_id="work-review-recovered-root",
                apply=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-active-prune",
            )
        )
    )
    assert active_prune["deleted_count"] == 0
    assert active_prune["matched_count"] == 0
    assert active_prune["skipped_active_count"] == 1
    assert active_prune["skipped_active_event_ids"] == [failure_event["failure_event_id"]]
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS count FROM kanban_review_processor_failure_events
            WHERE failure_event_id=?
            """,
            (failure_event["failure_event_id"],),
        ).fetchone()["count"]
        == 1
    )
    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET next_retry_at='2000-01-01T00:00:00Z'
        WHERE marker_id=?
        """,
        (failed["marker"]["marker_id"],),
    )
    conn.commit()
    retry_claim = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-recovered",
                lease_token=lease["lease"]["lease_token"],
                item_id="work-review-recovered-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-retry-claim",
            )
        )
    )
    recovered = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            retry_claim["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-recovered",
                lease_token=lease["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="processed",
                decision_id="kanban-decision-recovered",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-success",
            ),
        )
    )
    assert recovered["marker"]["status"] == "processed"
    assert recovered["marker"]["last_error"] == ""
    assert recovered["marker"]["next_retry_at"] == ""
    assert recovered["marker"]["last_successful_source_hash"] == marker["document_source_hash"]

    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-recovered-root")
    )
    assert status["failures"]["event_count"] == 1
    assert status["failures"]["active_failure_count"] == 0
    assert status["failures"]["retry_waiting_count"] == 0
    assert status["failures"]["retry_due_count"] == 0
    assert status["failures"]["last_error"] == ""
    assert status["failures"]["historical_last_error"] == "provider_failed"
    assert status["review_processor"]["retry_waiting_count"] == 0
    assert status["review_processor"]["retry_due_count"] == 0
    assert status["review_processor"]["last_error"] == ""
    assert status["review_processor"]["next_retry_at"] == ""
    aggregate = status["failures"]["aggregates"][0]
    assert aggregate["last_error"] == "provider_failed"
    assert aggregate["retry_state"] == "processed"
    assert aggregate["retry_waiting"] is False
    assert aggregate["next_retry_at"] == ""
    assert aggregate["scheduled_retry_at"] == failure_event["next_retry_at"]

    prune_preview = asyncio.run(
        routes_personal.prune_work_automation_failure_events(
            routes_personal.WorkAutomationFailurePruneRequest(
                item_id="work-review-recovered-root",
                apply=False,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-prune-preview",
            )
        )
    )
    assert prune_preview["apply"] is False
    assert prune_preview["matched_count"] == 1
    assert prune_preview["deleted_count"] == 0
    assert prune_preview["skipped_active_count"] == 0
    assert prune_preview["events"][0]["failure_event_id"] == failure_event["failure_event_id"]

    conn.execute("DELETE FROM sync_queue")
    prune_apply = asyncio.run(
        routes_personal.prune_work_automation_failure_events(
            routes_personal.WorkAutomationFailurePruneRequest(
                item_id="work-review-recovered-root",
                apply=True,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-recovered-prune-apply",
            )
        )
    )
    assert prune_apply["apply"] is True
    assert prune_apply["matched_count"] == 1
    assert prune_apply["deleted_count"] == 1
    assert prune_apply["pruned_event_ids"] == [failure_event["failure_event_id"]]
    assert (
        conn.execute(
            """
            SELECT COUNT(*) AS count FROM kanban_review_processor_failure_events
            WHERE failure_event_id=?
            """,
            (failure_event["failure_event_id"],),
        ).fetchone()["count"]
        == 0
    )
    pruned_status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-recovered-root")
    )
    assert pruned_status["failures"]["event_count"] == 0
    assert pruned_status["failures"]["aggregates"] == []
    delete_sync_tables = {
        row["table_name"]
        for row in conn.execute(
            "SELECT table_name FROM sync_queue WHERE action_type='DELETE'"
        ).fetchall()
    }
    assert "kanban_review_processor_failure_events" in delete_sync_tables


def test_work_review_processor_failed_completion_clears_legacy_processed_hash(
    monkeypatch,
    tmp_path,
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-legacy-failed-root",
                title="Review legacy failed root",
                body="Root item for legacy processed hash failure proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-legacy-failed-child",
                parent_item_id="work-review-legacy-failed-root",
                title="Review legacy failed child",
                body="Child item with Review data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-legacy-failed-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review legacy processed hash should not survive retry failure.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-legacy-failed-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-legacy-failed",
                item_id="work-review-legacy-failed-child",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-legacy-failed",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-legacy-failed-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-claim",
            )
        )
    )
    conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET processed_document_updated_at=document_updated_at,
            processed_source_hash=document_source_hash,
            processed_at='2026-06-28T10:00:00Z',
            last_successful_source_hash=''
        WHERE marker_id=?
        """,
        (claimed["marker"]["marker_id"],),
    )
    conn.commit()

    completed = asyncio.run(
        routes_personal.complete_work_review_processor_marker(
            claimed["marker"]["marker_id"],
            routes_personal.WorkReviewProcessorMarkerCompleteRequest(
                holder_id="codex-legacy-failed",
                lease_token=acquired["lease"]["lease_token"],
                document_source_hash=marker["document_source_hash"],
                status="failed",
                error="provider_failed",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-legacy-failed-complete",
            ),
        )
    )

    failed_marker = completed["marker"]
    assert failed_marker["status"] == "failed"
    assert failed_marker["processed_document_updated_at"] == ""
    assert failed_marker["processed_source_hash"] == ""
    assert failed_marker["processed_at"] == ""
    assert failed_marker["last_successful_source_hash"] == ""
    assert failed_marker["retry_attempt_count"] == 1
    assert failed_marker["last_failure_source_hash"] == marker["document_source_hash"]


def test_work_review_processor_archive_cancels_active_marker(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-archive-root",
                title="Review archive root",
                body="Root item for archive cancellation proof",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-root-create",
            )
        )
    )
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-review-archive-child",
                parent_item_id="work-review-archive-root",
                title="Review archive child",
                body="Child item with Review cancellation data",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-child-create",
            )
        )
    )
    asyncio.run(
        routes_personal.update_work_item_review_document(
            "work-review-archive-child",
            routes_personal.WorkItemDetailDocumentUpdateRequest(
                body="Review archive cancellation pass one.",
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-review-write",
            ),
        )
    )
    scan = asyncio.run(
        routes_personal.trigger_work_review_processor_idle_scan(
            routes_personal.WorkReviewProcessorIdleScanRequest(
                item_id="work-review-archive-root",
                max_items=20,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-scan",
            )
        )
    )
    marker = scan["queued_markers"][0]
    acquired = asyncio.run(
        routes_personal.acquire_work_review_processor_lease(
            routes_personal.WorkReviewProcessorLeaseRequest(
                holder_id="codex-archive",
                item_id="work-review-archive-child",
                ttl_seconds=600,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-lease",
            )
        )
    )
    claimed = asyncio.run(
        routes_personal.claim_next_work_review_processor_marker(
            routes_personal.WorkReviewProcessorMarkerClaimRequest(
                holder_id="codex-archive",
                lease_token=acquired["lease"]["lease_token"],
                item_id="work-review-archive-root",
                timeout_seconds=120,
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-claim",
            )
        )
    )
    assert claimed["marker"]["status"] == "processing"

    archived = asyncio.run(
        routes_personal.archive_work_item(
            "work-review-archive-child",
            routes_personal.WorkItemActionRequest(
                actor="codex-test",
                source_surface="pytest",
                request_id="review-archive-child-archive",
            ),
        )
    )
    assert archived["item"]["status"] == "archived"
    assert len(archived["cancelled_review_markers"]) == 1
    cancelled = archived["cancelled_review_markers"][0]
    assert cancelled["marker_id"] == marker["marker_id"]
    assert cancelled["status"] == "cancelled"
    assert cancelled["last_error"] == "item_archived"
    assert cancelled["metadata"]["cancelled_previous_status"] == "processing"
    assert cancelled["metadata"]["archived_item_id"] == "work-review-archive-child"
    assert cancelled["processing_expires_at"] == ""

    status = asyncio.run(
        routes_personal.get_work_automation_status(item_id="work-review-archive-child")
    )
    scheduler = status["review_processor"]["scheduler"]
    assert scheduler["queue_length"] == 0
    assert scheduler["active_count"] == 0
    assert scheduler["pending_count"] == 0
    assert scheduler["by_status"]["cancelled"] == 1

    sync_tables = {
        row["table_name"] for row in conn.execute("SELECT table_name FROM sync_queue").fetchall()
    }
    assert "kanban_review_processor_markers" in sync_tables
    audit_actions = {
        row["action"] for row in conn.execute("SELECT action FROM kanban_audit_log").fetchall()
    }
    assert "archive_work_item" in audit_actions


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


def test_work_kanban_agent_leaf_doing_requires_active_session(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")

    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-leaf-doing-guard",
                title="Leaf Doing guard proof",
                body="Leaf should not be Doing without an active session.",
                state_id="todo",
                actor="codex-test",
                source_surface="pytest",
                request_id="leaf-doing-create",
            )
        )
    )

    with pytest.raises(routes_personal.HTTPException) as blocked:
        asyncio.run(
            routes_personal.move_work_item(
                "work-leaf-doing-guard",
                routes_personal.WorkItemMoveRequest(
                    state_id="doing",
                    actor="codex-test",
                    source_surface="pytest",
                    request_id="leaf-doing-agent-blocked",
                ),
            )
        )
    assert blocked.value.status_code == 409
    assert blocked.value.detail["error"] == "kanban_agent_leaf_doing_without_active_session"

    operator_move = asyncio.run(
        routes_personal.move_work_item(
            "work-leaf-doing-guard",
            routes_personal.WorkItemMoveRequest(
                state_id="doing",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="leaf-doing-operator-override",
            ),
        )
    )["item"]
    assert operator_move["state_id"] == "doing"

    asyncio.run(
        routes_personal.move_work_item(
            "work-leaf-doing-guard",
            routes_personal.WorkItemMoveRequest(
                state_id="todo",
                actor="blueprints-ui",
                source_surface="kanban-board",
                request_id="leaf-doing-reset",
            ),
        )
    )
    asyncio.run(
        routes_personal.create_work_item_agent_session(
            "work-leaf-doing-guard",
            routes_personal.WorkAgentSessionCreateRequest(
                session_id="leaf-doing-session",
                agent_id="codex",
                node_id="test-node",
                worktree_path="/root/xarta-node",
                repo_full_name="xarta/xarta-node",
                branch="main",
                actor="codex-test",
                source_surface="pytest-session",
                request_id="leaf-doing-session-create",
            ),
        )
    )
    agent_move = asyncio.run(
        routes_personal.move_work_item(
            "work-leaf-doing-guard",
            routes_personal.WorkItemMoveRequest(
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
                request_id="leaf-doing-agent-allowed",
            ),
        )
    )["item"]
    assert agent_move["state_id"] == "doing"


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
                actor="blueprints-ui",
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


def _processor_registry_for_test(_processor_kind, *, probe=True):
    return [
        {
            "route_id": route_id,
            "label": routes_personal.KANBAN_PROCESSOR_MODEL_REGISTRY[route_id]["label"],
            "description": "pytest allowlisted route",
            "available": True,
            "availability_state": "available",
            "availability_classification": (
                "available_subscription_model"
                if route_id.startswith("chatgpt-")
                else "available_private_local_model"
            ),
        }
        for route_id in routes_personal.KANBAN_PROCESSOR_ROUTE_IDS
    ]


def _processor_model_definitions_for_test(_processor_kind):
    return {
        route_id: {
            **routes_personal.KANBAN_PROCESSOR_MODEL_REGISTRY[route_id],
            "route_id": route_id,
            "provider": "test-subscription-provider",
            "model": f"test-{route_id}",
            "catalog_model_id": f"test-{route_id}",
        }
        for route_id in routes_personal.KANBAN_PROCESSOR_ROUTE_IDS
    }


def test_work_processor_public_registry_separates_private_route_configuration() -> None:
    assert all(
        not {"provider", "model", "catalog_model_id"}.intersection(definition)
        for definition in routes_personal.KANBAN_PROCESSOR_MODEL_REGISTRY.values()
    )
    definitions = routes_personal._work_processor_model_definitions(
        "preprocessing",
        profile_config={
            "xarta_kanban_model_routes": {
                route_id: {
                    "provider": "provider-from-private-config",
                    "model": f"model-from-private-config-{route_id}",
                }
                for route_id in routes_personal.KANBAN_PROCESSOR_ROUTE_IDS
            }
        },
    )
    assert definitions["chatgpt-5-6-sol"]["label"] == "ChatGPT 5.6 Sol"
    assert definitions["chatgpt-5-6-sol"]["provider"] == "provider-from-private-config"
    assert definitions["private-local-thinking"]["model"].endswith("private-local-thinking")


def test_work_processor_routing_settings_persist_reset_and_reject_stale_writes(monkeypatch):
    conn = _make_conn()
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(conn))
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_registry_snapshot",
        _processor_registry_for_test,
    )

    initial = routes_personal._get_work_processor_routing_settings_sync(probe=False)
    preprocessing = initial["settings"]["preprocessing"]
    review = initial["settings"]["review"]
    blocker = initial["settings"]["blocker"]
    assert preprocessing["route_ids"] == list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS)
    assert review["processor_kind"] == "review"
    assert blocker["processor_kind"] == "blocker"
    assert blocker["route_ids"] == list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS)
    reversed_ids = list(reversed(preprocessing["route_ids"]))

    saved = routes_personal._update_work_processor_routing_settings_sync(
        "preprocessing",
        routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
            route_ids=reversed_ids,
            expected_revision=preprocessing["revision"],
            actor="codex-test",
            source_surface="pytest",
            request_id="processor-routing-save",
        ),
    )
    assert saved["settings"]["route_ids"] == reversed_ids
    assert saved["settings"]["stored"] is True
    assert saved["settings"]["revision"] != preprocessing["revision"]
    assert routes_personal._get_work_processor_routing_settings_sync(False)["settings"]["review"][
        "route_ids"
    ] == list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS)

    with pytest.raises(routes_personal.HTTPException) as stale:
        routes_personal._update_work_processor_routing_settings_sync(
            "preprocessing",
            routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
                route_ids=list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS),
                expected_revision=preprocessing["revision"],
            ),
        )
    assert stale.value.status_code == 409
    assert stale.value.detail["error"] == "stale_processor_routing_settings"
    assert stale.value.detail["current"]["route_ids"] == reversed_ids

    reset = routes_personal._update_work_processor_routing_settings_sync(
        "preprocessing",
        routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
            expected_revision=saved["settings"]["revision"],
            reset=True,
        ),
    )
    assert reset["settings"]["route_ids"] == list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS)
    assert reset["audit"]["action"] == "reset_processor_model_priority"

    blocker_saved = routes_personal._update_work_processor_routing_settings_sync(
        "blocker",
        routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
            route_ids=reversed_ids,
            expected_revision=blocker["revision"],
            actor="codex-test",
            source_surface="pytest",
            request_id="blocker-routing-save",
        ),
    )
    assert blocker_saved["settings"]["route_ids"] == reversed_ids
    with pytest.raises(routes_personal.HTTPException) as blocker_stale:
        routes_personal._update_work_processor_routing_settings_sync(
            "blocker",
            routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
                route_ids=list(routes_personal.KANBAN_PROCESSOR_ROUTE_IDS),
                expected_revision=blocker["revision"],
            ),
        )
    assert blocker_stale.value.status_code == 409
    assert blocker_stale.value.detail["current"]["route_ids"] == reversed_ids

    with pytest.raises(Exception):
        routes_personal.WorkProcessorRoutingSettingsUpdateRequest(
            expected_revision=reset["settings"]["revision"],
            provider="operator-controlled-provider",
            model="operator-controlled-model",
            endpoint="https://untrusted.example",
        )


def test_work_processor_model_failover_is_bounded_and_records_attempts(monkeypatch):
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_route",
        lambda kind: {
            "profile": f"hermes-kanban-{kind}",
            "api_base": "http://127.0.0.1:9999",
            "api_key_file": "/tmp/not-read-by-test",
            "fallback_provider": "",
            "fallback_model": "",
        },
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_drift",
        lambda kind: {"ok": True, "problems": []},
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: {"local_ai_max_tokens": 1200},
    )
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(_make_conn()))
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_model_definitions",
        _processor_model_definitions_for_test,
    )
    routes = _processor_registry_for_test("preprocessing")[:2]
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_routing_snapshot",
        lambda conn, kind, probe=True: {
            "revision": "sha256:routing-test",
            "routes": routes,
        },
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_prompt_variant",
        lambda kind, route_id, messages: (
            messages,
            {
                "request_sha256": f"sha256:{route_id}",
                "selection": "model_variant",
                "route_id": route_id,
            },
        ),
    )
    calls = []

    def complete(**kwargs):
        calls.append(kwargs)
        if kwargs["route_id"] == "chatgpt-5-6-sol":
            raise ConnectionError("subscription route temporarily unavailable")
        return {
            "model": "gateway-profile",
            "choices": [{"message": {"content": '{"ready": true}'}}],
        }

    monkeypatch.setattr(routes_personal, "_work_automation_profile_completion_sync", complete)
    result = asyncio.run(
        routes_personal._work_automation_processor_profile_json_completion(
            messages=[{"role": "user", "content": "bounded failover proof"}],
            run_id="pytest-route-failover",
            processor_kind="blocker",
            source_hash="sha256:blocker-source",
            state_identity={"state_key": "blocker:test"},
        )
    )
    assert [call["route_id"] for call in calls] == [
        "chatgpt-5-6-sol",
        "chatgpt-5-6-terra",
    ]
    assert calls[0]["idempotency_key"] != calls[1]["idempotency_key"]
    assert result["chosen_route_id"] == "chatgpt-5-6-terra"
    assert result["final_outcome"] == "completed"
    assert result["fallback_reason"] == "chatgpt-5-6-sol:transport_unavailable"
    assert [attempt["outcome"] for attempt in result["model_attempts"]] == [
        "failed",
        "chosen",
    ]
    assert all(
        attempt["source_hash"] == "sha256:blocker-source"
        and attempt["state_identity"]["state_key"] == "blocker:test"
        for attempt in result["model_attempts"]
    )


def test_work_processor_all_unavailable_retains_retryable_attempt_evidence(monkeypatch):
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_route",
        lambda kind: {
            "profile": f"hermes-kanban-{kind}",
            "api_base": "http://127.0.0.1:9999",
            "api_key_file": "/tmp/not-read-by-test",
            "fallback_provider": "",
            "fallback_model": "",
        },
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_drift",
        lambda kind: {"ok": True, "problems": []},
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_idle_worker_config",
        lambda: {"local_ai_max_tokens": 1200},
    )
    monkeypatch.setattr(routes_personal, "_sqlite_get_conn", lambda: _conn_context(_make_conn()))
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_model_definitions",
        _processor_model_definitions_for_test,
    )
    unavailable = _processor_registry_for_test("review")[:3]
    for route in unavailable:
        route.update(
            available=False,
            availability_state="unavailable",
            availability_classification="model_not_in_subscription_cache",
        )
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_routing_snapshot",
        lambda conn, kind, probe=True: {"revision": "sha256:all-down", "routes": unavailable},
    )
    monkeypatch.setattr(
        routes_personal,
        "_work_processor_prompt_variant",
        lambda kind, route_id, messages: (
            messages,
            {"request_sha256": f"sha256:{route_id}", "route_id": route_id},
        ),
    )

    with pytest.raises(routes_personal.WorkProcessorRoutesExhausted) as exhausted:
        asyncio.run(
            routes_personal._work_automation_processor_profile_json_completion(
                messages=[{"role": "user", "content": "all unavailable proof"}],
                run_id="pytest-all-unavailable",
                processor_kind="review",
            )
        )
    assert len(exhausted.value.attempts) == 3
    assert {attempt["outcome"] for attempt in exhausted.value.attempts} == {"unavailable"}
    assert all(attempt["latency_ms"] == 0 for attempt in exhausted.value.attempts)


def test_blocker_processor_completion_uses_structured_bounded_blueprints_routing(monkeypatch):
    captured = {}

    async def complete(**kwargs):
        captured.update(kwargs)
        return {
            "payload": {
                "decision_type": "needs_operator",
                "rationale": "Current authorization is genuinely absent.",
                "confidence": "high",
                "limitations": "Operator approval remains required.",
                "evidence_refs": ["kanban_blockers:blocker-structured"],
                "proof_refs": [],
            },
            "response_id": "response-structured",
            "response_model": "profile-response",
            "finish_reason": "stop",
            "usage": {"total_tokens": 42},
            "content_excerpt": '{"decision_type":"needs_operator"}',
            "routing_schema": routes_personal.KANBAN_PROCESSOR_ROUTING_ATTEMPTS_SCHEMA,
            "routing_revision": "sha256:blocker-routing",
            "model_attempts": [
                {
                    "route_id": "chatgpt-5-6-sol",
                    "outcome": "chosen",
                    "prompt_variant": {
                        "soul_prompt_id": "kanban-blocker-chatgpt-5-6-sol-soul",
                        "system_prompt_id": "kanban-blocker-chatgpt-5-6-sol-system",
                        "soul_sha256": "sha256:soul",
                        "system_sha256": "sha256:system",
                    },
                }
            ],
            "chosen_route_id": "chatgpt-5-6-sol",
            "chosen_provider": "openai-codex",
            "chosen_model": "gpt-5.6-sol",
            "latency_ms": 12.5,
            "fallback_reason": "",
            "source_hash": "sha256:structured-source",
            "state_identity": {"state_key": "blocker:blocker-structured"},
            "final_outcome": "completed",
        }

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_json_completion",
        complete,
    )
    response = asyncio.run(
        routes_personal.complete_work_blocker_processor(
            routes_personal.WorkBlockerProcessorCompletionRequest(
                candidate_ref="kanban_blockers:blocker-structured",
                source_hash="sha256:structured-source",
                source_context={
                    "blockers": [{"blocker_id": "blocker-structured", "status": "open"}],
                    "blocker_history": [{"blocker_id": "old", "status": "resolved"}],
                },
                evidence_refs=["kanban_blockers:blocker-structured"],
                state_identity={"state_key": "blocker:blocker-structured"},
                request_id="blocker-structured-request",
                run_id="blocker-structured-run",
            )
        )
    )
    assert response["ok"] is True
    assert response["decision"]["decision_type"] == "needs_operator"
    assert response["model_routing"]["chosen_route_id"] == "chatgpt-5-6-sol"
    assert response["model_routing"]["source_hash"] == "sha256:structured-source"
    assert captured["processor_kind"] == "blocker"
    assert captured["state_identity"]["state_key"] == "blocker:blocker-structured"
    assert "file" not in captured
    assert "terminal" not in captured
    assert "provider" not in captured
    assert "model" not in captured


def test_blocker_processor_completion_returns_retryable_all_route_evidence(monkeypatch):
    attempts = [
        {
            "route_id": route_id,
            "label": routes_personal.KANBAN_PROCESSOR_MODEL_REGISTRY[route_id]["label"],
            "provider": "server-provider",
            "model": f"server-model-{route_id}",
            "availability_classification": "gateway_unavailable",
            "error_classification": "gateway_unavailable",
            "latency_ms": 0,
            "fallback_reason": "",
            "prompt_variant": {
                "soul_prompt_id": f"kanban-blocker-{route_id}-soul",
                "system_prompt_id": f"kanban-blocker-{route_id}-system",
            },
            "source_hash": "sha256:all-routes-source",
            "run_id": "all-routes-run",
            "state_identity": {"state_key": "blocker:all-routes"},
            "outcome": "unavailable",
        }
        for route_id in routes_personal.KANBAN_PROCESSOR_ROUTE_IDS
    ]

    async def exhausted(**_kwargs):
        raise routes_personal.WorkProcessorRoutesExhausted("blocker", attempts)

    monkeypatch.setattr(
        routes_personal,
        "_work_automation_processor_profile_json_completion",
        exhausted,
    )
    response = asyncio.run(
        routes_personal.complete_work_blocker_processor(
            routes_personal.WorkBlockerProcessorCompletionRequest(
                candidate_ref="kanban_blockers:all-routes",
                source_hash="sha256:all-routes-source",
                source_context={"blockers": [{"status": "open"}]},
                state_identity={"state_key": "blocker:all-routes"},
                run_id="all-routes-run",
            )
        )
    )
    assert response["ok"] is False
    assert response["retryable"] is True
    assert response["model_routing"]["final_outcome"] == "all_routes_failed"
    assert len(response["model_routing"]["attempts"]) == len(
        routes_personal.KANBAN_PROCESSOR_ROUTE_IDS
    )


def test_work_processor_prompt_variant_selects_model_files_and_generic_fallback(
    monkeypatch, tmp_path
):
    prompt_root = tmp_path / "prompts"
    profile_root = tmp_path / "config/profiles"
    variant = prompt_root / "kanban-model-variants/preprocessing/chatgpt-5-6-sol"
    variant.mkdir(parents=True)
    (variant / "soul.md").write_text("Sol-specific SOUL", encoding="utf-8")
    (variant / "system.md").write_text("Sol-specific system contract", encoding="utf-8")
    generic = prompt_root / "kanban-preprocessing-system.md"
    generic.write_text("Generic preprocessing contract", encoding="utf-8")
    profile = profile_root / "hermes-kanban-preprocessor"
    profile.mkdir(parents=True)
    (profile / "SOUL.md").write_text("Generic processor SOUL", encoding="utf-8")
    monkeypatch.setattr(
        routes_personal, "KANBAN_PROCESSOR_MODEL_PROMPT_ROOT", prompt_root / "kanban-model-variants"
    )
    monkeypatch.setattr(routes_personal, "PREPROCESSING_SYSTEM_PROMPT_PATH", generic)
    monkeypatch.setattr(routes_personal, "HERMES_LOCAL_STACK_ROOT", tmp_path)

    selected, evidence = routes_personal._work_processor_prompt_variant(
        "preprocessing",
        "chatgpt-5-6-sol",
        [{"role": "user", "content": "prompt variant proof"}],
    )
    assert selected[0]["role"] == "system"
    assert "Sol-specific SOUL" in selected[0]["content"]
    assert "Sol-specific system contract" in selected[0]["content"]
    assert evidence["selection"] == "model_variant"
    assert evidence["soul_prompt_id"] == "kanban-preprocessing-chatgpt-5-6-sol-soul"
    assert evidence["request_sha256"].startswith("sha256:")

    fallback, fallback_evidence = routes_personal._work_processor_prompt_variant(
        "preprocessing",
        "chatgpt-5-6-terra",
        [{"role": "user", "content": "generic fallback proof"}],
    )
    assert "Generic preprocessing contract" in fallback[0]["content"]
    assert fallback_evidence["selection"] == "generic_fallback"


def test_blocker_processor_prompt_variant_records_route_pair_and_generic_pair(
    monkeypatch, tmp_path
):
    prompt_root = tmp_path / "prompts"
    variant = prompt_root / "kanban-model-variants/blocker/private-local-thinking"
    variant.mkdir(parents=True)
    (variant / "soul.md").write_text("Blocker thinking SOUL", encoding="utf-8")
    (variant / "system.md").write_text("Blocker thinking contract", encoding="utf-8")
    generic_soul = prompt_root / "kanban-blocker-processor-soul.md"
    generic_system = prompt_root / "kanban-blocker-processor-system.md"
    generic_soul.write_text("Generic blocker SOUL", encoding="utf-8")
    generic_system.write_text("Generic blocker contract", encoding="utf-8")
    monkeypatch.setattr(
        routes_personal,
        "KANBAN_PROCESSOR_MODEL_PROMPT_ROOT",
        prompt_root / "kanban-model-variants",
    )
    monkeypatch.setattr(routes_personal, "BLOCKER_PROCESSOR_SOUL_PROMPT_PATH", generic_soul)
    monkeypatch.setattr(routes_personal, "BLOCKER_PROCESSOR_SYSTEM_PROMPT_PATH", generic_system)

    selected, evidence = routes_personal._work_processor_prompt_variant(
        "blocker",
        "private-local-thinking",
        [{"role": "user", "content": "structured blocker context"}],
    )
    assert "Blocker thinking SOUL" in selected[0]["content"]
    assert evidence["soul_prompt_id"] == "kanban-blocker-private-local-thinking-soul"
    assert evidence["system_prompt_id"] == "kanban-blocker-private-local-thinking-system"
    assert evidence["soul_sha256"].startswith("sha256:")
    assert evidence["system_sha256"].startswith("sha256:")

    fallback, fallback_evidence = routes_personal._work_processor_prompt_variant(
        "blocker",
        "chatgpt-5-6-luna",
        [{"role": "user", "content": "generic blocker context"}],
    )
    assert "Generic blocker SOUL" in fallback[0]["content"]
    assert "Generic blocker contract" in fallback[0]["content"]
    assert fallback_evidence["soul_prompt_id"] == "kanban-blocker-generic-soul"
    assert fallback_evidence["system_prompt_id"] == "kanban-blocker-generic-system"


def test_private_no_think_preprocessing_prompt_forbids_skill_and_tool_access() -> None:
    path = (
        routes_personal.KANBAN_PROCESSOR_MODEL_PROMPT_ROOT
        / "preprocessing/private-local-no-think/system.md"
    )
    prompt = path.read_text(encoding="utf-8")
    assert "Do not call tools, read files, browse skills" in prompt
    assert "exact ID only" in prompt
    assert prompt.rstrip().endswith("/no_think")


def test_work_preprocessing_prompt_is_domain_neutral(monkeypatch):
    monkeypatch.setattr(
        routes_personal,
        "_row_to_work_item",
        lambda item: {"item_id": item["item_id"], "title": "Unrelated service work"},
    )
    messages = routes_personal._work_preprocessing_local_ai_messages(
        item={
            "item_id": "work-unrelated-service",
            "body_excerpt": "Add a bounded cache to an unrelated service.",
        },
        source={},
        detail_document={},
        review_document={},
        discussions=[],
        recent_commits=[],
        recent_decisions=[],
        ancestor_context={},
        marker={"marker_id": "marker-domain-neutral"},
    )
    payload = json.loads(messages[1]["content"])
    rules = " ".join(payload["hard_rules"])
    full_prompt = "\n".join(message["content"] for message in messages)
    assert "typed named ordered collections that execute as one target" in rules
    assert "Task-specific regression fixtures" in rules
    assert "PIM" not in full_prompt
    assert "zero-path recovery" not in full_prompt
    assert "one retained schedule" not in full_prompt
    assert "GPT-5.6 Sol or Terra" in rules


def test_work_preprocessing_prompt_keeps_regression_fixture_in_scoped_evidence_only(
    monkeypatch,
):
    correction = (
        "For this PIM Email Scheduler card only: preserve a general scheduler, "
        "one typed named ordered rule-set target executed atomically, separate "
        "zero-path recovery, operator UI, coverage/fencing proof, and an explicit "
        "production-configuration question."
    )
    monkeypatch.setattr(
        routes_personal,
        "_row_to_work_item",
        lambda item: {"item_id": item["item_id"], "title": "Scheduler regression"},
    )
    messages = routes_personal._work_preprocessing_local_ai_messages(
        item={"item_id": "kanban-002305ddba25", "body_excerpt": "Scheduler work"},
        source={},
        detail_document={},
        review_document={"body": correction},
        discussions=[],
        recent_commits=[],
        recent_decisions=[],
        ancestor_context={},
        marker={"marker_id": "marker-scheduler-regression"},
    )
    payload = json.loads(messages[1]["content"])
    rules = " ".join(payload["hard_rules"])
    assert payload["documents"]["review_excerpt"] == correction
    assert "PIM Email Scheduler" not in messages[0]["content"]
    assert "zero-path recovery" not in rules


def test_work_processor_message_builders_do_not_read_prompt_files(monkeypatch):
    monkeypatch.setattr(
        routes_personal,
        "_load_work_prompt",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected file read")),
    )
    monkeypatch.setattr(
        routes_personal,
        "_row_to_work_item",
        lambda item: {"item_id": item["item_id"], "title": "Prompt boundary"},
    )
    preprocessing = routes_personal._work_preprocessing_local_ai_messages(
        item={"item_id": "work-prompt-boundary", "body_excerpt": "Prompt boundary"},
        source={},
        detail_document={},
        review_document={},
        discussions=[],
        recent_commits=[],
        recent_decisions=[],
        ancestor_context={},
        marker={"marker_id": "marker-prompt-boundary"},
    )
    review = routes_personal._work_review_processor_local_ai_messages(
        item={"item_id": "work-prompt-boundary"},
        review_document={},
        marker={"marker_id": "marker-review-prompt-boundary"},
    )
    assert preprocessing[0]["content"] == routes_personal.PREPROCESSING_SYSTEM_PROMPT
    assert review[0]["content"] == routes_personal.REVIEW_PROCESSOR_SYSTEM_PROMPT


def test_work_preprocessing_prompt_supplies_bounded_engineering_guidance(monkeypatch):
    monkeypatch.setattr(
        routes_personal,
        "_row_to_work_item",
        lambda item: {"item_id": item["item_id"], "title": "Async route work"},
    )
    messages = routes_personal._work_preprocessing_local_ai_messages(
        item={"item_id": "work-guidance-prompt", "body_excerpt": "Change an async route."},
        source={},
        detail_document={},
        review_document={},
        discussions=[],
        recent_commits=[],
        recent_decisions=[],
        ancestor_context={},
        marker={"marker_id": "marker-guidance-prompt"},
    )
    payload = json.loads(messages[1]["content"])
    catalog = payload["engineering_guidance_catalog"]
    assert catalog["direct_skill_tools_required"] is False
    assert {entry["guidance_id"] for entry in catalog["entries"]} == {
        "blueprints-event-loop-isolation",
        "database-connection-lifecycle",
        "long-lived-http-clients",
        "permission-and-ownership-guards",
    }
    assert "selected_guidance_ids" in payload["required_output"]
    assert any("must not browse arbitrary skills" in rule for rule in payload["hard_rules"])


def test_work_preprocessing_guidance_selection_rejects_unknown_and_dedupes() -> None:
    selection = routes_personal._work_preprocessing_guidance_selection(
        {
            "selected_guidance_ids": [
                "long-lived-http-clients",
                "invented-general-best-practice",
                "long-lived-http-clients",
                "blueprints-event-loop-isolation",
            ],
            "guidance_rationales": {
                "long-lived-http-clients": "This leaf probes the same gateway repeatedly.",
                "blueprints-event-loop-isolation": "The probe starts in an async route.",
            },
        }
    )
    assert selection["selected_guidance_ids"] == [
        "long-lived-http-clients",
        "blueprints-event-loop-isolation",
    ]
    assert selection["rejected_unknown_ids"] == ["invented-general-best-practice"]
    assert [entry["guidance_id"] for entry in selection["selections"]] == selection[
        "selected_guidance_ids"
    ]
    assert all(entry["source_refs"] for entry in selection["selections"])


def test_work_preprocessing_decomposition_guidance_is_allowlisted_and_child_scoped() -> None:
    items = routes_personal._work_preprocessing_normalise_decomposition_items(
        {
            "decomposition_items": [
                {
                    "title": "Move route work off the event loop",
                    "body": "Repair the async request boundary.",
                    "guidance_ids": [
                        "blueprints-event-loop-isolation",
                        "unknown-guidance",
                    ],
                },
                {
                    "title": "Update the operator copy",
                    "body": "Explain the visible outcome.",
                    "guidance_ids": [],
                },
            ]
        },
        parent_item={
            "item_id": "work-guidance-parent",
            "priority_id": "high",
            "tags_json": '["kanban"]',
        },
    )
    assert items[0]["guidance_ids"] == ["blueprints-event-loop-isolation"]
    assert "whole blocking SQLite" in items[0]["body"]
    assert "unknown-guidance" not in items[0]["body"]
    assert items[1]["guidance_ids"] == []
    assert "Applicable implementation patterns" not in items[1]["body"]


def test_work_processor_request_planning_runs_through_measured_sync_boundary(monkeypatch):
    route_id = "chatgpt-5-6-sol"
    calls = []

    async def run_sync(func, *args, **kwargs):
        calls.append((func, args, kwargs))
        return {
            "config": {"local_ai_max_tokens": 800},
            "processor_kind": "preprocessing",
            "route": {
                "profile": "hermes-kanban-preprocessor",
                "api_base": "http://127.0.0.1:8649",
                "api_key_file": "/tmp/not-read-by-test",
                "fallback_provider": "",
                "fallback_model": "",
            },
            "routing": {
                "revision": "sha256:request-plan",
                "routes": [
                    {
                        "route_id": route_id,
                        "available": True,
                        "availability_state": "available",
                        "availability_classification": "available_subscription_model",
                    }
                ],
            },
            "route_definitions": {
                route_id: _processor_model_definitions_for_test("preprocessing")[route_id]
            },
            "prompt_variants": {
                route_id: (
                    [{"role": "user", "content": "planned away from event loop"}],
                    {"request_sha256": "sha256:planned", "route_id": route_id},
                )
            },
        }

    monkeypatch.setattr(routes_personal, "_run_personal_sync_work", run_sync)
    monkeypatch.setattr(
        routes_personal,
        "_work_automation_profile_completion_sync",
        lambda **kwargs: {
            "model": "hermes-kanban-preprocessor",
            "choices": [{"message": {"content": '{"ready": true}'}}],
        },
    )
    result = asyncio.run(
        routes_personal._work_automation_processor_profile_json_completion(
            messages=[{"role": "user", "content": "request plan proof"}],
            run_id="pytest-request-plan",
            processor_kind="preprocessing",
        )
    )
    assert calls[0][0] is routes_personal._work_automation_processor_request_plan_sync
    assert result["chosen_route_id"] == route_id


def test_work_processor_http_client_is_reused_per_origin_and_closed() -> None:
    asyncio.run(routes_personal._close_work_processor_http_clients())
    first = routes_personal._work_processor_http_client("http://processor-a.invalid/health")
    second = routes_personal._work_processor_http_client(
        "http://processor-a.invalid/v1/chat/completions"
    )
    other = routes_personal._work_processor_http_client("http://processor-b.invalid/health")
    assert first is second
    assert other is not first
    asyncio.run(routes_personal._close_work_processor_http_clients())
    assert first.is_closed
    assert other.is_closed
    assert routes_personal._WORK_PROCESSOR_HTTP_CLIENTS == {}


def test_work_normalized_context_manifest_filters_resolved_blockers_and_tracks_deltas(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('peer-node')")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-manifest-blockers",
                title="Normalized manifest blocker proof",
                body="Only current blockers belong in readiness.",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    conn.executemany(
        """
        INSERT INTO kanban_blockers (
            blocker_id, item_id, title, body_excerpt, status, blocked_by_ref,
            provenance_json
        ) VALUES (?, 'work-manifest-blockers', ?, '', ?, '', '{}')
        """,
        [
            ("blocker-resolved", "Historical resolved blocker", "resolved"),
            ("blocker-open", "Current open blocker", "open"),
        ],
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-manifest-blockers'"
    ).fetchone()
    first = routes_personal._work_preprocessing_context_source(conn, row)
    manifest = first["context_manifest"]
    assert manifest["schema"] == "xarta.kanban.normalized_context_manifest.v1"
    assert first["document_source_hash"] == manifest["fingerprint"]
    assert first["counts"]["blocker_count"] == 1
    assert [entry["ref"] for entry in manifest["components"]["blocker"]] == [
        "kanban_blockers:blocker-open"
    ]

    marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "item_id": "work-manifest-blockers",
        "context_hash": manifest["fingerprint"],
        "counts": first["counts"],
        "source_refs": first["source_refs"],
        "context_manifest": manifest,
    }
    conn.execute(
        """
        INSERT INTO kanban_agent_hints (
            hint_id, item_id, required_skills_json, metadata_json
        ) VALUES ('hint-work-manifest-blockers', ?, '[]', ?)
        """,
        ("work-manifest-blockers", json.dumps({"context_readiness_marker": marker})),
    )
    conn.execute("UPDATE kanban_blockers SET status='resolved' WHERE blocker_id='blocker-open'")
    conn.commit()
    second = routes_personal._work_preprocessing_context_source(conn, row)
    assert second["reason"] == "readiness_marker_stale"
    assert second["manifest_delta"]["changed_components"] == ["blocker"]
    assert second["manifest_delta"]["delta_only"] is True


def test_work_normalized_context_manifest_does_not_self_invalidate_on_preprocessing_decision(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-manifest-preprocessing-decision",
                title="Manifest excludes its own processor output",
                body="A processed readiness decision must not queue itself again.",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id='work-manifest-preprocessing-decision'"
    ).fetchone()
    before = routes_personal._work_preprocessing_context_source(conn, row)
    conn.execute(
        """
        INSERT INTO kanban_review_decisions (
            decision_id, item_id, processor_kind, decision_type, summary,
            proof_refs_json, status
        ) VALUES (
            'decision-preprocessing-self-output',
            'work-manifest-preprocessing-decision',
            'preprocessing',
            'context_readiness_marked',
            'The leaf is ready.',
            '["proof:self-output"]',
            'accepted'
        )
        """
    )
    conn.commit()

    after = routes_personal._work_preprocessing_context_source(conn, row)

    assert before["context_manifest"]["fingerprint"] == after["context_manifest"]["fingerprint"]
    assert after["context_manifest"]["components"]["decision"] == []
    assert after["context_manifest"]["components"]["proof"]["refs"] == []


def test_work_context_manifest_read_detects_current_drift_while_item_is_doing(
    monkeypatch, tmp_path
):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="work-manifest-read",
                title="Read current manifest",
                body="Readiness drift must be visible outside the ToDo queue.",
                state_id="doing",
                actor="codex-test",
                source_surface="pytest",
            )
        )
    )
    baseline = routes_personal._get_work_item_context_manifest_sync("work-manifest-read")
    marker = {
        "schema": "xarta.kanban.context_readiness_marker.v1",
        "item_id": "work-manifest-read",
        "context_hash": baseline["source_hash"],
        "counts": baseline["counts"],
        "source_refs": baseline["source_refs"],
        "context_manifest": baseline["context_manifest"],
    }
    conn.execute(
        """
        INSERT INTO kanban_agent_hints (
            hint_id, item_id, required_skills_json, metadata_json
        ) VALUES ('hint-work-manifest-read', ?, '[]', ?)
        """,
        ("work-manifest-read", json.dumps({"context_readiness_marker": marker})),
    )
    conn.execute(
        """
        INSERT INTO kanban_discussions (
            discussion_id, item_id, author, body_excerpt, status,
            search_text, search_metadata_json, provenance_json,
            created_at, updated_at
        ) VALUES (
            'discussion-manifest-read', 'work-manifest-read', 'codex-test',
            'New implementation evidence.', 'open', '', '{}', '{}',
            '2026-07-14T13:00:00Z', '2026-07-14T13:00:00Z'
        )
        """
    )
    conn.commit()

    current = asyncio.run(routes_personal.get_work_item_context_manifest("work-manifest-read"))

    assert current["schema"] == "xarta.kanban.context_manifest_read.v1"
    assert current["source_hash"] != baseline["source_hash"]
    assert current["reason"] == "readiness_marker_stale"
    assert "discussion" in current["manifest_delta"]["changed_components"]


def test_work_automation_status_compact_bounds_refresh_payload(monkeypatch):
    payload = {
        "ok": True,
        "generated_at": "2026-07-13T00:00:00Z",
        "item": {"item_id": "work-compact"},
        "provider_mode": {"active": "required-hermes-kanban-llm"},
        "idle_worker": {"enabled": True},
        "automation_exclusions": {
            "count": 30,
            "recent_items": [{"item_id": f"excluded-{index}"} for index in range(30)],
        },
        "review_processor": {
            "status": "decision-ledger-ready",
            "queue_length": 30,
            "review_markers": [
                {"marker_id": f"review-{index}", "source_hash": f"sha256:{index}"}
                for index in range(30)
            ],
        },
        "preprocessing": {
            "status": "queued",
            "queue_length": 30,
            "markers": [{"marker_id": f"pre-{index}"} for index in range(30)],
        },
        "decisions": {
            "count": 30,
            "by_status": {"recorded": 30},
            "recent": [{"decision_id": f"decision-{index}"} for index in range(30)],
        },
        "commit_link_health": {"ok": True},
        "output_contract": {"huge": "x" * 10000},
        "failures": {"recent_events": [{"huge": "y" * 10000}]},
    }
    compact = routes_personal._compact_work_automation_status_response(payload)
    assert compact["schema"] == "xarta.kanban.automation_status.compact.v1"
    assert len(compact["review_processor"]["recent_markers"]) == 5
    assert len(compact["preprocessing"]["recent_markers"]) == 5
    assert len(compact["decisions"]["recent"]) == 5
    assert len(compact["automation_exclusions"]["recent_items"]) == 5
    assert "output_contract" not in compact
    assert "failures" not in compact
    assert len(json.dumps(compact)) < 10000


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


def test_preprocessing_preserves_tagged_operator_requests_and_steers_exactly(monkeypatch):
    first_steer = "  Keep  two spaces.\nDo not censor FUCKING language.  "
    second_steer = "Second steer\n\nwith a blank line."
    item_payload = {
        "item_id": "verbatim-fixture",
        "tags": ["kanban", "operator-request-verbatim-v1"],
        "created_at": "2026-07-15T10:00:00Z",
        "updated_at": "2026-07-15T10:00:00Z",
    }
    monkeypatch.setattr(routes_personal, "_row_to_work_item", lambda item: item_payload)
    item = {
        "item_id": "verbatim-fixture",
        "body_excerpt": VERBATIM_OPERATOR_REQUEST_FIXTURE,
    }
    discussions = [
        {
            "discussion_id": "operator-request-verbatim-003-second",
            "created_at": "2026-07-15T10:03:00Z",
            "author": "codex-test-fixture",
            "document": {"body": second_steer},
        },
        {
            "discussion_id": "operator-request-verbatim-002-first",
            "created_at": "2026-07-15T10:01:00Z",
            "author": "codex-test-fixture",
            "document": {"body": first_steer},
        },
    ]

    record = routes_personal._work_preprocessing_original_operator_requests(
        item,
        discussions,
    )

    assert [entry["sequence"] for entry in record["records"]] == [1, 2, 3]
    assert [entry["text"] for entry in record["records"]] == [
        VERBATIM_OPERATOR_REQUEST_FIXTURE,
        first_steer,
        second_steer,
    ]
    assert record["records"][0]["text"].encode() == VERBATIM_OPERATOR_REQUEST_FIXTURE.encode()
    assert record["records"][1]["text"].encode() == first_steer.encode()

    messages = routes_personal._work_preprocessing_local_ai_messages(
        item=item,
        source={"schema": "test-source"},
        detail_document={"body": "Test-only implementation plan."},
        review_document={"body": ""},
        discussions=discussions,
        recent_commits=[],
        recent_decisions=[],
        ancestor_context={},
        marker={"marker_id": "test-marker", "document_source_hash": "test-hash"},
    )
    provider_payload = json.loads(messages[1]["content"])
    assert [
        entry["text"] for entry in provider_payload["original_operator_requests"]["records"]
    ] == [
        VERBATIM_OPERATOR_REQUEST_FIXTURE,
        first_steer,
        second_steer,
    ]
    assert (
        provider_payload["original_operator_requests"]["records"][1]["text"].encode()
        == first_steer.encode()
    )


def test_verbatim_operator_request_storage_preserves_long_text_and_crlf(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")

    primary_request = "  primary\r\n" + ("Long request text.  " * 260) + "\r\nend  "
    steering_request = "\tsteer\r\n" + ("Do exactly this.  " * 280) + "\r\n  "
    assert len(primary_request) > 4000
    assert len(steering_request) > 4000

    created = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="verbatim-long-storage",
                title="Verbatim long storage fixture",
                body=primary_request,
                tags=["kanban", "operator-request-verbatim-v1"],
                actor="codex-test-fixture",
                source_surface="pytest",
            )
        )
    )["item"]
    assert created["body_excerpt"] == primary_request

    discussion = asyncio.run(
        routes_personal.create_work_discussion(
            created["item_id"],
            routes_personal.WorkDiscussionCreateRequest(
                discussion_id="operator-request-verbatim-002-long-crlf",
                body=steering_request,
                author="codex-test-fixture",
                actor="codex-test-fixture",
                source_surface="pytest",
            ),
        )
    )["discussion"]
    assert discussion["body_excerpt"] == steering_request
    # Markdown rendering has its own LF convention; the database field is authoritative.
    assert discussion["document"]["body"] == steering_request.replace("\r\n", "\n")

    item_row = conn.execute(
        "SELECT * FROM kanban_items WHERE item_id=?",
        (created["item_id"],),
    ).fetchone()
    discussion_row = conn.execute(
        "SELECT * FROM kanban_discussions WHERE discussion_id=?",
        (discussion["discussion_id"],),
    ).fetchone()
    assert item_row["body_excerpt"] == primary_request
    assert discussion_row["body_excerpt"] == steering_request

    requests = routes_personal._work_preprocessing_original_operator_requests(
        item_row,
        [routes_personal._row_to_work_discussion(discussion_row, conn)],
    )
    assert [record["text"] for record in requests["records"]] == [
        primary_request,
        steering_request,
    ]


def test_verbatim_operator_request_storage_rejects_oversize_records(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    oversized = "x" * (routes_personal.KANBAN_VERBATIM_OPERATOR_REQUEST_MAX_CODEPOINTS + 1)

    with pytest.raises(routes_personal.HTTPException) as item_error:
        asyncio.run(
            routes_personal.create_work_item(
                routes_personal.WorkItemCreateRequest(
                    item_id="verbatim-oversize-rejected",
                    title="Verbatim oversize fixture",
                    body=oversized,
                    tags=["operator-request-verbatim-v1"],
                    actor="codex-test-fixture",
                    source_surface="pytest",
                )
            )
        )
    assert item_error.value.status_code == 413
    assert "65536 Unicode-codepoint" in item_error.value.detail

    parent = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="verbatim-oversize-parent",
                title="Verbatim oversize parent fixture",
                body="Within the explicit limit.",
                tags=["operator-request-verbatim-v1"],
                actor="codex-test-fixture",
                source_surface="pytest",
            )
        )
    )["item"]
    with pytest.raises(routes_personal.HTTPException) as discussion_error:
        asyncio.run(
            routes_personal.create_work_discussion(
                parent["item_id"],
                routes_personal.WorkDiscussionCreateRequest(
                    discussion_id="operator-request-verbatim-002-oversize",
                    body=oversized,
                    actor="codex-test-fixture",
                    source_surface="pytest",
                ),
            )
        )
    assert discussion_error.value.status_code == 413
    assert "65536 Unicode-codepoint" in discussion_error.value.detail


def test_adding_verbatim_tag_requires_body_in_same_update(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    item = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="verbatim-tag-transition",
                title="Verbatim tag transition fixture",
                body="An ordinary body cannot later be certified without resubmission.",
                actor="codex-test-fixture",
                source_surface="pytest",
            )
        )
    )["item"]

    with pytest.raises(routes_personal.HTTPException) as missing_body_error:
        asyncio.run(
            routes_personal.update_work_item(
                item["item_id"],
                routes_personal.WorkItemUpdateRequest(
                    tags=["operator-request-verbatim-v1"],
                    actor="codex-test-fixture",
                    source_surface="pytest",
                ),
            )
        )
    assert missing_body_error.value.status_code == 400
    assert "complete Operator request" in missing_body_error.value.detail

    exact = "  resubmitted\r\nrequest  "
    updated = asyncio.run(
        routes_personal.update_work_item(
            item["item_id"],
            routes_personal.WorkItemUpdateRequest(
                body=exact,
                tags=["operator-request-verbatim-v1"],
                actor="codex-test-fixture",
                source_surface="pytest",
            ),
        )
    )["item"]
    assert updated["body_excerpt"] == exact


def test_verbatim_operator_request_storage_rejects_oversize_card_total(monkeypatch, tmp_path):
    conn = _make_conn()
    _patch_conn(monkeypatch, conn)
    monkeypatch.setattr(routes_personal, "KANBAN_ROOT", tmp_path / "kanban")
    conn.execute("INSERT INTO nodes (node_id) VALUES ('test-node')")
    item = asyncio.run(
        routes_personal.create_work_item(
            routes_personal.WorkItemCreateRequest(
                item_id="verbatim-total-limit",
                title="Verbatim total limit fixture",
                body="p" * 40_000,
                tags=["operator-request-verbatim-v1"],
                actor="codex-test-fixture",
                source_surface="pytest",
            )
        )
    )["item"]

    with pytest.raises(routes_personal.HTTPException) as total_error:
        asyncio.run(
            routes_personal.create_work_discussion(
                item["item_id"],
                routes_personal.WorkDiscussionCreateRequest(
                    discussion_id="operator-request-verbatim-002-total-limit",
                    body="s" * 30_000,
                    actor="codex-test-fixture",
                    source_surface="pytest",
                ),
            )
        )
    assert total_error.value.status_code == 413
    assert "total limit" in total_error.value.detail


def test_preprocessing_operator_request_assessment_accepts_aligned_comparison():
    original = {
        "required": True,
        "records": [
            {"source_ref": "kanban_items:verbatim-fixture:body"},
            {"source_ref": "kanban_discussions:operator-request-verbatim-002-first"},
        ],
    }

    assessment = routes_personal._work_preprocessing_operator_request_assessment(
        {
            "operator_request_assessment": {
                "record_refs_in_order": [
                    "kanban_items:verbatim-fixture:body",
                    "kanban_discussions:operator-request-verbatim-002-first",
                ],
                "alignment": "aligned",
                "comparison_summary": "Implementation and proof cover both exact requests.",
                "deviations": [],
            }
        },
        original,
    )

    assert assessment["alignment"] == "aligned"
    assert assessment["requires_operator_input"] is False


def test_preprocessing_rejects_unjustified_operator_request_deviation():
    original = {
        "required": True,
        "records": [{"source_ref": "kanban_items:verbatim-fixture:body"}],
    }

    with pytest.raises(ValueError, match="lacks a valid request ref, description, or reason"):
        routes_personal._work_preprocessing_operator_request_assessment(
            {
                "operator_request_assessment": {
                    "record_refs_in_order": ["kanban_items:verbatim-fixture:body"],
                    "alignment": "deviation_requires_approval",
                    "comparison_summary": "A different implementation is proposed.",
                    "deviations": [
                        {
                            "affected_request_ref": "kanban_items:verbatim-fixture:body",
                            "description": "Replace the request with a summary.",
                            "reason": "",
                            "operator_approval_ref": "",
                            "awaiting_operator": False,
                        }
                    ],
                }
            },
            original,
        )


def test_preprocessing_routes_discovered_impossibility_for_operator_approval():
    original = {
        "required": True,
        "records": [{"source_ref": "kanban_items:verbatim-fixture:body"}],
    }

    assessment = routes_personal._work_preprocessing_operator_request_assessment(
        {
            "operator_request_assessment": {
                "record_refs_in_order": ["kanban_items:verbatim-fixture:body"],
                "alignment": "impossibility_requires_approval",
                "comparison_summary": "The API cannot preserve the requested storage form.",
                "deviations": [
                    {
                        "affected_request_ref": "kanban_items:verbatim-fixture:body",
                        "description": "Use the only storage form accepted by the API.",
                        "reason": "The requested form is rejected by the authoritative API schema.",
                        "operator_approval_ref": "",
                        "awaiting_operator": True,
                    }
                ],
            }
        },
        original,
    )

    assert assessment["requires_operator_input"] is True
    assert assessment["deviations"][0]["awaiting_operator"] is True
