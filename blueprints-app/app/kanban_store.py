"""Kanban storage boundary for the current SQLite-backed implementation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from .db import get_setting


class KanbanStoreError(Exception):
    """Base class for Kanban store errors."""


class KanbanItemNotFound(KanbanStoreError):
    """Raised when a Kanban item cannot be resolved."""


class KanbanItemCycleError(KanbanStoreError):
    """Raised when Kanban parent links contain a cycle."""


@dataclass(frozen=True)
class KanbanConfigRead:
    states: list[Any]
    priorities: list[Any]
    preferences: dict[str, Any]
    depth_limit: int


@dataclass(frozen=True)
class KanbanBoardRead:
    parent: Any | None
    states: list[Any]
    items_by_state: dict[str, list[Any]]
    breadcrumbs: list[Any]
    preferences: dict[str, Any]
    remaining_depth: int
    rollup: dict[str, Any]
    hidden_test_items: int
    show_test_entries: bool


@dataclass(frozen=True)
class KanbanItemDetailRead:
    item: Any
    children: list[Any]
    issues: list[Any]
    todos: list[Any]
    blockers: list[Any]
    discussions: list[Any]
    links: list[Any]
    commits: list[Any]
    audit: list[Any]
    breadcrumbs: list[Any]
    remaining_depth: int
    rollup: dict[str, Any]
    depth_limit: int


@dataclass(frozen=True)
class KanbanPriorityRecommendationRead:
    recommendation: Any
    item: Any | None
    breadcrumbs: list[Any]


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return value


def _bool_setting_value(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "show"}:
        return True
    if text in {"0", "false", "no", "off", "hide"}:
        return False
    return default


class SQLiteKanbanStore:
    """Repository facade for Kanban reads and writes backed by SQLite."""

    def __init__(
        self,
        conn: Any,
        *,
        depth_limit: int,
        show_test_entries_setting: str,
        agent_working_out_tag: str,
        item_detail_document_reader: Callable[[Any, str], dict[str, Any]] | None = None,
        item_review_document_reader: Callable[[Any, str], dict[str, Any]] | None = None,
        item_detail_document_writer: Callable[..., dict[str, Any]] | None = None,
        item_review_document_writer: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self.conn = conn
        self.depth_limit = depth_limit
        self.show_test_entries_setting = show_test_entries_setting
        self.agent_working_out_tag = agent_working_out_tag
        self._item_detail_document_reader = item_detail_document_reader
        self._item_review_document_reader = item_review_document_reader
        self._item_detail_document_writer = item_detail_document_writer
        self._item_review_document_writer = item_review_document_writer

    def config(self) -> KanbanConfigRead:
        return KanbanConfigRead(
            states=self.conn.execute(
                "SELECT * FROM kanban_item_states ORDER BY sort_order, state_id"
            ).fetchall(),
            priorities=self.conn.execute(
                "SELECT * FROM kanban_item_priorities ORDER BY sort_order, priority_id"
            ).fetchall(),
            preferences=self.preferences(),
            depth_limit=self.depth_limit,
        )

    def preferences(self) -> dict[str, Any]:
        return {
            "show_test_entries": _bool_setting_value(
                get_setting(self.conn, self.show_test_entries_setting),
                default=True,
            )
        }

    def board(
        self,
        parent_item_id: str | None = None,
        *,
        show_test_entries: bool | None = None,
    ) -> KanbanBoardRead:
        parent_id = self._clean_id(parent_item_id) or None
        parent = self.item_or_raise(parent_id) if parent_id else None
        preferences = self.preferences()
        if show_test_entries is None:
            effective_show_test_entries = bool(preferences["show_test_entries"])
        else:
            effective_show_test_entries = bool(show_test_entries)
            preferences["show_test_entries"] = effective_show_test_entries

        breadcrumbs = self.breadcrumbs(parent_id)
        remaining_depth = (
            max(0, self.depth_limit - int(parent["depth"]))
            if parent is not None
            else self.depth_limit
        )
        states = self.conn.execute(
            "SELECT * FROM kanban_item_states ORDER BY sort_order, state_id"
        ).fetchall()
        if parent_id:
            rows = self.conn.execute(
                """
                SELECT * FROM kanban_items
                WHERE parent_item_id=? AND status != 'archived'
                ORDER BY state_id, priority_id, sort_order, updated_at DESC, item_id
                """,
                (parent_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT * FROM kanban_items
                WHERE parent_item_id IS NULL AND status != 'archived'
                ORDER BY state_id, priority_id, sort_order, updated_at DESC, item_id
                """
            ).fetchall()

        rows, hidden_test_items = self._filter_test_rows(rows, effective_show_test_entries)
        raw_items_by_state: dict[str, list[Any]] = {row["state_id"]: [] for row in states}
        for row in rows:
            raw_items_by_state.setdefault(row["state_id"], []).append(row)
        items_by_state = {
            state_id: self.sort_items_for_lane(state_rows)
            for state_id, state_rows in raw_items_by_state.items()
        }
        return KanbanBoardRead(
            parent=parent,
            states=states,
            items_by_state=items_by_state,
            breadcrumbs=breadcrumbs,
            preferences=preferences,
            remaining_depth=remaining_depth,
            rollup=self.rollup(parent_id, show_test_entries=effective_show_test_entries),
            hidden_test_items=hidden_test_items,
            show_test_entries=effective_show_test_entries,
        )

    def item_detail(self, item_id: str) -> KanbanItemDetailRead:
        clean_item_id = self._clean_id(item_id)
        item = self.item_or_raise(clean_item_id)
        children = self.conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND status != 'archived'
            ORDER BY state_id, sort_order, updated_at DESC, item_id
            """,
            (clean_item_id,),
        ).fetchall()
        issues = self.conn.execute(
            """
            SELECT * FROM kanban_items
            WHERE parent_item_id=? AND item_type='issue' AND status != 'archived'
            ORDER BY updated_at DESC, item_id
            """,
            (clean_item_id,),
        ).fetchall()
        todos = self.conn.execute(
            """
            SELECT w.* FROM kanban_items w
            WHERE w.parent_item_id=?
              AND w.state_id='todo'
              AND w.status != 'archived'
              AND w.item_type != 'issue'
              AND NOT EXISTS (
                  SELECT 1 FROM kanban_items child
                  WHERE child.parent_item_id=w.item_id
                    AND child.status != 'archived'
              )
            ORDER BY COALESCE(json_extract(w.provenance_json, '$.todo.due_at'), w.updated_at),
                     w.item_id
            """,
            (clean_item_id,),
        ).fetchall()
        blockers = self.conn.execute(
            "SELECT * FROM kanban_blockers WHERE item_id=? ORDER BY updated_at DESC, blocker_id",
            (clean_item_id,),
        ).fetchall()
        discussions = self.conn.execute(
            "SELECT * FROM kanban_discussions WHERE item_id=? ORDER BY created_at ASC, discussion_id",
            (clean_item_id,),
        ).fetchall()
        links = self.conn.execute(
            """
            SELECT * FROM kanban_item_links
            WHERE source_item_id=? OR target_item_id=?
            ORDER BY link_type, updated_at DESC, link_id
            """,
            (clean_item_id, clean_item_id),
        ).fetchall()
        commits = self.conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=?
            ORDER BY COALESCE(NULLIF(committed_at, ''), updated_at) DESC,
                     repo_full_name, sha
            """,
            (clean_item_id,),
        ).fetchall()
        audit = self.conn.execute(
            """
            SELECT * FROM kanban_audit_log
            WHERE item_id=?
            ORDER BY created_at DESC, audit_id
            LIMIT 20
            """,
            (clean_item_id,),
        ).fetchall()
        return KanbanItemDetailRead(
            item=item,
            children=children,
            issues=issues,
            todos=todos,
            blockers=blockers,
            discussions=discussions,
            links=links,
            commits=commits,
            audit=audit,
            breadcrumbs=self.breadcrumbs(clean_item_id),
            remaining_depth=max(0, self.depth_limit - int(item["depth"])),
            rollup=self.rollup(clean_item_id),
            depth_limit=self.depth_limit,
        )

    def priority_recommendations(
        self,
        *,
        scope_id: str,
        limit: int,
    ) -> list[KanbanPriorityRecommendationRead]:
        clean_scope_id = self._clean_id(scope_id, fallback="kanban", limit=120)
        clean_limit = max(1, min(int(limit or 10), 50))
        rows = self.conn.execute(
            """
            SELECT * FROM kanban_priority_recommendations
            WHERE scope_id=?
            ORDER BY rank ASC, updated_at DESC, recommendation_id
            LIMIT ?
            """,
            (clean_scope_id, clean_limit),
        ).fetchall()
        reads: list[KanbanPriorityRecommendationRead] = []
        for row in rows:
            item = self.item(row["item_id"])
            reads.append(
                KanbanPriorityRecommendationRead(
                    recommendation=row,
                    item=item,
                    breadcrumbs=self.breadcrumbs(row["item_id"]) if item is not None else [],
                )
            )
        return reads

    def priority_recommendation_rows(self, *, scope_id: str) -> list[Any]:
        clean_scope_id = self._clean_id(scope_id, fallback="kanban", limit=120)
        return self.conn.execute(
            "SELECT * FROM kanban_priority_recommendations WHERE scope_id=?",
            (clean_scope_id,),
        ).fetchall()

    def upsert_priority_recommendation(
        self,
        payload: dict[str, Any],
        *,
        now: str,
    ) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_priority_recommendations (
                recommendation_id, scope_id, rank, item_id, title, summary,
                reason, priority_id, state_id, score, strategy_version,
                source_surface, source_hash, metadata_json, provenance_json,
                generated_at, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(recommendation_id) DO UPDATE SET
                scope_id=excluded.scope_id,
                rank=excluded.rank,
                item_id=excluded.item_id,
                title=excluded.title,
                summary=excluded.summary,
                reason=excluded.reason,
                priority_id=excluded.priority_id,
                state_id=excluded.state_id,
                score=excluded.score,
                strategy_version=excluded.strategy_version,
                source_surface=excluded.source_surface,
                source_hash=excluded.source_hash,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                generated_at=excluded.generated_at,
                updated_at=excluded.updated_at
            """,
            (
                payload["recommendation_id"],
                payload["scope_id"],
                payload["rank"],
                payload["item_id"],
                payload["title"],
                payload["summary"],
                payload["reason"],
                payload["priority_id"],
                payload["state_id"],
                float(payload["score"] or 0),
                payload["strategy_version"],
                payload["source_surface"],
                payload["source_hash"],
                json.dumps(payload["metadata"], ensure_ascii=True, sort_keys=True),
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                payload["generated_at"],
                now,
                now,
            ),
        )
        return self.conn.execute(
            "SELECT * FROM kanban_priority_recommendations WHERE recommendation_id=?",
            (payload["recommendation_id"],),
        ).fetchone()

    def delete_priority_recommendation(self, recommendation_id: str) -> None:
        self.conn.execute(
            "DELETE FROM kanban_priority_recommendations WHERE recommendation_id=?",
            (recommendation_id,),
        )

    def insert_item_row(self, payload: dict[str, Any], *, now: str) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_items (
                item_id, parent_item_id, title, body_excerpt, item_type, state_id,
                priority_id, depth, sort_order, status, goal_flag, automation_excluded,
                archived_at, promoted_from_ref,
                source_type, source_ref, source_hash, tags_json, related_event_ids_json,
                related_task_ids_json, related_issue_ids_json, search_text,
                search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
                vector_index_key, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '',
                    NULL, ?, ?, ?, ?)
            """,
            (
                payload["item_id"],
                payload["parent_item_id"],
                payload["title"],
                payload["body_excerpt"],
                payload["item_type"],
                payload["state_id"],
                payload["priority_id"],
                payload["depth"],
                payload["sort_order"],
                payload["status"],
                int(payload["goal_flag"]),
                int(payload["automation_excluded"]),
                payload["promoted_from_ref"],
                payload["source_type"],
                payload["source_ref"],
                payload["source_hash"],
                json.dumps(payload["tags"], ensure_ascii=True),
                json.dumps(payload["related_event_ids"], ensure_ascii=True),
                json.dumps(payload["related_task_ids"], ensure_ascii=True),
                json.dumps(payload["related_issue_ids"], ensure_ascii=True),
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                now,
                now,
            ),
        )
        return self.item_or_raise(payload["item_id"])

    def update_item_row(self, item_id: str, payload: dict[str, Any], *, now: str) -> Any:
        self.conn.execute(
            """
            UPDATE kanban_items
            SET title=?, body_excerpt=?, item_type=?, state_id=?, priority_id=?,
                sort_order=?, status=?, goal_flag=?, automation_excluded=?, source_hash=?, tags_json=?,
                related_event_ids_json=?, related_task_ids_json=?,
                related_issue_ids_json=?, search_text=?, search_metadata_json=?,
                vector_index_key=?, provenance_json=?, updated_at=?
            WHERE item_id=?
            """,
            (
                payload["title"],
                payload["body_excerpt"],
                payload["item_type"],
                payload["state_id"],
                payload["priority_id"],
                payload["sort_order"],
                payload["status"],
                int(payload["goal_flag"]),
                int(payload["automation_excluded"]),
                payload["source_hash"],
                json.dumps(payload["tags"], ensure_ascii=True),
                json.dumps(payload["related_event_ids"], ensure_ascii=True),
                json.dumps(payload["related_task_ids"], ensure_ascii=True),
                json.dumps(payload["related_issue_ids"], ensure_ascii=True),
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                now,
                item_id,
            ),
        )
        return self.item_or_raise(item_id)

    def move_item_row(
        self,
        item_id: str,
        *,
        parent_item_id: str | None,
        state_id: str,
        status: str,
        depth: int,
        sort_order: int,
        now: str,
    ) -> Any:
        self.conn.execute(
            """
            UPDATE kanban_items
            SET parent_item_id=?, state_id=?, status=?, depth=?, sort_order=?, updated_at=?
            WHERE item_id=?
            """,
            (
                parent_item_id,
                state_id,
                status,
                depth,
                sort_order,
                now,
                item_id,
            ),
        )
        return self.item_or_raise(item_id)

    def archive_item_row(self, item_id: str, *, archived_at: str) -> Any:
        self.conn.execute(
            "UPDATE kanban_items SET status='archived', archived_at=?, updated_at=? WHERE item_id=?",
            (archived_at, archived_at, item_id),
        )
        return self.item_or_raise(item_id)

    def discussion_row(self, discussion_id: str) -> Any | None:
        clean_discussion_id = self._clean_id(discussion_id)
        if not clean_discussion_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_discussions WHERE discussion_id=?",
            (clean_discussion_id,),
        ).fetchone()

    def create_discussion_row(self, payload: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_discussions (
                discussion_id, item_id, author, body_excerpt, status, search_text,
                search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
                vector_index_key, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, ?)
            """,
            (
                payload["discussion_id"],
                payload["item_id"],
                payload["author"],
                payload["body_excerpt"],
                payload["status"],
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        return self.discussion_row(payload["discussion_id"])

    def update_discussion_row(
        self,
        discussion_id: str,
        payload: dict[str, Any],
        *,
        now: str,
    ) -> Any:
        self.conn.execute(
            """
            UPDATE kanban_discussions
            SET author=?, body_excerpt=?, status=?, search_text=?, search_metadata_json=?,
                vector_index_key=?, provenance_json=?, updated_at=?
            WHERE discussion_id=?
            """,
            (
                payload["author"],
                payload["body_excerpt"],
                payload["status"],
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                now,
                discussion_id,
            ),
        )
        return self.discussion_row(discussion_id)

    def update_discussion_provenance(
        self,
        discussion_id: str,
        *,
        provenance: dict[str, Any],
    ) -> Any:
        self.conn.execute(
            "UPDATE kanban_discussions SET provenance_json=? WHERE discussion_id=?",
            (json.dumps(provenance, ensure_ascii=True, sort_keys=True), discussion_id),
        )
        return self.discussion_row(discussion_id)

    def delete_discussion_row(self, discussion_id: str) -> None:
        self.conn.execute(
            "DELETE FROM kanban_discussions WHERE discussion_id=?",
            (discussion_id,),
        )

    def item_detail_document(self, item_id: str) -> dict[str, Any]:
        reader = self._require_document_reader(
            self._item_detail_document_reader, "item detail document reader"
        )
        clean_item_id = self._clean_id(item_id)
        self.item_or_raise(clean_item_id)
        return reader(self.conn, clean_item_id)

    def item_review_document(self, item_id: str) -> dict[str, Any]:
        reader = self._require_document_reader(
            self._item_review_document_reader, "item review document reader"
        )
        clean_item_id = self._clean_id(item_id)
        self.item_or_raise(clean_item_id)
        return reader(self.conn, clean_item_id)

    def write_item_detail_document(
        self,
        item_id: str,
        body: str,
        *,
        actor: str,
        now: str,
    ) -> dict[str, Any]:
        writer = self._require_document_writer(
            self._item_detail_document_writer, "item detail document writer"
        )
        clean_item_id = self._clean_id(item_id)
        self.item_or_raise(clean_item_id)
        return writer(self.conn, clean_item_id, body, actor=actor, now=now)

    def write_item_review_document(
        self,
        item_id: str,
        body: str,
        *,
        actor: str,
        now: str,
        metadata_extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        writer = self._require_document_writer(
            self._item_review_document_writer, "item review document writer"
        )
        clean_item_id = self._clean_id(item_id)
        self.item_or_raise(clean_item_id)
        return writer(
            self.conn,
            clean_item_id,
            body,
            actor=actor,
            now=now,
            metadata_extra=metadata_extra,
        )

    def create_item_link_row(self, payload: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_item_links (
                link_id, source_item_id, target_item_id, link_type, metadata_json,
                created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["link_id"],
                payload["source_item_id"],
                payload["target_item_id"],
                payload["link_type"],
                json.dumps(payload["metadata"], ensure_ascii=True, sort_keys=True),
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        return self.conn.execute(
            "SELECT * FROM kanban_item_links WHERE link_id=?",
            (payload["link_id"],),
        ).fetchone()

    def blocker_row(self, blocker_id: str) -> Any | None:
        clean_blocker_id = self._clean_id(blocker_id)
        if not clean_blocker_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_blockers WHERE blocker_id=?",
            (clean_blocker_id,),
        ).fetchone()

    def upsert_blocker_row(self, payload: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_blockers (
                blocker_id, item_id, title, body_excerpt, status, blocked_by_ref,
                search_text, search_metadata_json, embedding_ref, embedding_model,
                embedding_updated_at, vector_index_key, provenance_json, created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?, ?, ?)
            ON CONFLICT(blocker_id) DO UPDATE SET
                item_id=excluded.item_id,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                status=excluded.status,
                blocked_by_ref=excluded.blocked_by_ref,
                search_text=excluded.search_text,
                search_metadata_json=excluded.search_metadata_json,
                vector_index_key=excluded.vector_index_key,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                payload["blocker_id"],
                payload["item_id"],
                payload["title"],
                payload["body_excerpt"],
                payload["status"],
                payload["blocked_by_ref"],
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        return self.blocker_row(payload["blocker_id"])

    def update_blocker_row(self, blocker_id: str, payload: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            UPDATE kanban_blockers
            SET title=?, body_excerpt=?, status=?, search_text=?,
                search_metadata_json=?, vector_index_key=?, provenance_json=?,
                updated_at=?
            WHERE blocker_id=?
            """,
            (
                payload["title"],
                payload["body_excerpt"],
                payload["status"],
                payload["search_text"],
                json.dumps(payload["search_metadata"], ensure_ascii=True, sort_keys=True),
                payload["vector_index_key"],
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                payload["updated_at"],
                blocker_id,
            ),
        )
        return self.blocker_row(blocker_id)

    def review_processor_lease_row(self, lease_id: str) -> Any | None:
        clean_lease_id = self._clean_id(lease_id)
        if not clean_lease_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_review_processor_leases WHERE lease_id=?",
            (clean_lease_id,),
        ).fetchone()

    def upsert_review_processor_lease_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_review_processor_leases (
                lease_id, processor_kind, holder_id, lease_token, item_id, session_id,
                status, acquired_at, heartbeat_at, expires_at, timeout_seconds,
                source_hash, metadata_json, provenance_json, created_at, updated_at
            )
            VALUES (
                :lease_id, :processor_kind, :holder_id, :lease_token, :item_id,
                :session_id, :status, :acquired_at, :heartbeat_at, :expires_at,
                :timeout_seconds, :source_hash, :metadata_json, :provenance_json,
                :created_at, :updated_at
            )
            ON CONFLICT(lease_id) DO UPDATE SET
                processor_kind=excluded.processor_kind,
                holder_id=excluded.holder_id,
                lease_token=excluded.lease_token,
                item_id=excluded.item_id,
                session_id=excluded.session_id,
                status=excluded.status,
                acquired_at=excluded.acquired_at,
                heartbeat_at=excluded.heartbeat_at,
                expires_at=excluded.expires_at,
                timeout_seconds=excluded.timeout_seconds,
                source_hash=excluded.source_hash,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.review_processor_lease_row(row["lease_id"])

    def review_processor_marker_row(self, marker_id: str) -> Any | None:
        clean_marker_id = self._clean_id(marker_id)
        if not clean_marker_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_review_processor_markers WHERE marker_id=?",
            (clean_marker_id,),
        ).fetchone()

    def upsert_review_processor_marker_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_review_processor_markers (
                marker_id, item_id, processor_kind, document_type, document_ref,
                document_updated_at, document_source_hash, processed_document_updated_at,
                processed_source_hash, processed_at, queued_at, last_seen_at,
                processing_started_at, processing_expires_at, attempt_count, last_error,
                next_retry_at, retry_after_seconds, retry_attempt_count,
                last_successful_source_hash, last_failure_event_id,
                last_failure_source_hash, last_error_class, retry_policy_version,
                superseded_at, superseded_by_source_hash, status, provider_mode,
                decision_id, source_hash, metadata_json, provenance_json, created_at,
                updated_at
            )
            VALUES (
                :marker_id, :item_id, :processor_kind, :document_type, :document_ref,
                :document_updated_at, :document_source_hash, :processed_document_updated_at,
                :processed_source_hash, :processed_at, :queued_at, :last_seen_at,
                :processing_started_at, :processing_expires_at, :attempt_count,
                :last_error, :next_retry_at, :retry_after_seconds,
                :retry_attempt_count, :last_successful_source_hash,
                :last_failure_event_id, :last_failure_source_hash, :last_error_class,
                :retry_policy_version, :superseded_at, :superseded_by_source_hash,
                :status, :provider_mode, :decision_id, :source_hash, :metadata_json,
                :provenance_json, :created_at, :updated_at
            )
            ON CONFLICT(marker_id) DO UPDATE SET
                item_id=excluded.item_id,
                processor_kind=excluded.processor_kind,
                document_type=excluded.document_type,
                document_ref=excluded.document_ref,
                document_updated_at=excluded.document_updated_at,
                document_source_hash=excluded.document_source_hash,
                processed_document_updated_at=excluded.processed_document_updated_at,
                processed_source_hash=excluded.processed_source_hash,
                processed_at=excluded.processed_at,
                queued_at=excluded.queued_at,
                last_seen_at=excluded.last_seen_at,
                processing_started_at=excluded.processing_started_at,
                processing_expires_at=excluded.processing_expires_at,
                attempt_count=excluded.attempt_count,
                last_error=excluded.last_error,
                next_retry_at=excluded.next_retry_at,
                retry_after_seconds=excluded.retry_after_seconds,
                retry_attempt_count=excluded.retry_attempt_count,
                last_successful_source_hash=excluded.last_successful_source_hash,
                last_failure_event_id=excluded.last_failure_event_id,
                last_failure_source_hash=excluded.last_failure_source_hash,
                last_error_class=excluded.last_error_class,
                retry_policy_version=excluded.retry_policy_version,
                superseded_at=excluded.superseded_at,
                superseded_by_source_hash=excluded.superseded_by_source_hash,
                status=excluded.status,
                provider_mode=excluded.provider_mode,
                decision_id=excluded.decision_id,
                source_hash=excluded.source_hash,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.review_processor_marker_row(row["marker_id"])

    def review_failure_event_row(self, failure_event_id: str) -> Any | None:
        clean_failure_event_id = self._clean_id(failure_event_id)
        if not clean_failure_event_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_review_processor_failure_events WHERE failure_event_id=?",
            (clean_failure_event_id,),
        ).fetchone()

    def upsert_review_failure_event_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_review_processor_failure_events (
                failure_event_id, marker_id, item_id, processor_kind, document_type,
                source_hash, error_class, error_message, provider_mode, model_alias,
                attempt_number, failed_at, next_retry_at, retry_after_seconds,
                retry_policy_version, retryable, status, event_hash, metadata_json,
                provenance_json, created_at, updated_at
            )
            VALUES (
                :failure_event_id, :marker_id, :item_id, :processor_kind,
                :document_type, :source_hash, :error_class, :error_message,
                :provider_mode, :model_alias, :attempt_number, :failed_at,
                :next_retry_at, :retry_after_seconds, :retry_policy_version,
                :retryable, :status, :event_hash, :metadata_json, :provenance_json,
                :created_at, :updated_at
            )
            ON CONFLICT(failure_event_id) DO UPDATE SET
                marker_id=excluded.marker_id,
                item_id=excluded.item_id,
                processor_kind=excluded.processor_kind,
                document_type=excluded.document_type,
                source_hash=excluded.source_hash,
                error_class=excluded.error_class,
                error_message=excluded.error_message,
                provider_mode=excluded.provider_mode,
                model_alias=excluded.model_alias,
                attempt_number=excluded.attempt_number,
                failed_at=excluded.failed_at,
                next_retry_at=excluded.next_retry_at,
                retry_after_seconds=excluded.retry_after_seconds,
                retry_policy_version=excluded.retry_policy_version,
                retryable=excluded.retryable,
                status=excluded.status,
                event_hash=excluded.event_hash,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.review_failure_event_row(row["failure_event_id"])

    def delete_review_failure_event_row(self, failure_event_id: str) -> None:
        self.conn.execute(
            "DELETE FROM kanban_review_processor_failure_events WHERE failure_event_id=?",
            (failure_event_id,),
        )

    def review_decision_row(self, decision_id: str) -> Any | None:
        clean_decision_id = self._clean_id(decision_id)
        if not clean_decision_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_review_decisions WHERE decision_id=?",
            (clean_decision_id,),
        ).fetchone()

    def upsert_review_decision_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_review_decisions (
                decision_id, item_id, processor_kind, decision_type, title, summary,
                rationale, affected_refs_json, confidence, uncertainty, proof_refs_json,
                commit_link_ids_json, status, provider_mode, source_hash, metadata_json,
                provenance_json, created_at, updated_at
            )
            VALUES (
                :decision_id, :item_id, :processor_kind, :decision_type, :title,
                :summary, :rationale, :affected_refs_json, :confidence,
                :uncertainty, :proof_refs_json, :commit_link_ids_json, :status,
                :provider_mode, :source_hash, :metadata_json, :provenance_json,
                :created_at, :updated_at
            )
            ON CONFLICT(decision_id) DO UPDATE SET
                item_id=excluded.item_id,
                processor_kind=excluded.processor_kind,
                decision_type=excluded.decision_type,
                title=excluded.title,
                summary=excluded.summary,
                rationale=excluded.rationale,
                affected_refs_json=excluded.affected_refs_json,
                confidence=excluded.confidence,
                uncertainty=excluded.uncertainty,
                proof_refs_json=excluded.proof_refs_json,
                commit_link_ids_json=excluded.commit_link_ids_json,
                status=excluded.status,
                provider_mode=excluded.provider_mode,
                source_hash=excluded.source_hash,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.review_decision_row(row["decision_id"])

    def agent_hints_row_for_item(self, item_id: str) -> Any | None:
        clean_item_id = self._clean_id(item_id)
        if not clean_item_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_agent_hints WHERE item_id=?",
            (clean_item_id,),
        ).fetchone()

    def upsert_agent_hints_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_agent_hints (
                hint_id, item_id, required_skills_json, routing_notes,
                commit_attribution_json, visibility, status, metadata_json,
                provenance_json, created_at, updated_at
            )
            VALUES (
                :hint_id, :item_id, :required_skills_json, :routing_notes,
                :commit_attribution_json, :visibility, :status, :metadata_json,
                :provenance_json, :created_at, :updated_at
            )
            ON CONFLICT(item_id) DO UPDATE SET
                required_skills_json=excluded.required_skills_json,
                routing_notes=excluded.routing_notes,
                commit_attribution_json=excluded.commit_attribution_json,
                visibility=excluded.visibility,
                status=excluded.status,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.agent_hints_row_for_item(row["item_id"])

    def agent_session_row(self, session_id: str) -> Any | None:
        clean_session_id = self._clean_id(session_id)
        if not clean_session_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_agent_sessions WHERE session_id=?",
            (clean_session_id,),
        ).fetchone()

    def upsert_agent_session_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            INSERT INTO kanban_agent_sessions (
                session_id, item_id, agent_id, node_id, worktree_path,
                repo_full_name, branch, status, started_at, ended_at,
                last_seen_at, request_hash, source_surface, summary,
                metadata_json, provenance_json, created_at, updated_at
            )
            VALUES (
                :session_id, :item_id, :agent_id, :node_id, :worktree_path,
                :repo_full_name, :branch, :status, :started_at, :ended_at,
                :last_seen_at, :request_hash, :source_surface, :summary,
                :metadata_json, :provenance_json, :created_at, :updated_at
            )
            ON CONFLICT(session_id) DO UPDATE SET
                agent_id=excluded.agent_id,
                node_id=excluded.node_id,
                worktree_path=excluded.worktree_path,
                repo_full_name=excluded.repo_full_name,
                branch=excluded.branch,
                status=excluded.status,
                started_at=excluded.started_at,
                ended_at=excluded.ended_at,
                last_seen_at=excluded.last_seen_at,
                request_hash=excluded.request_hash,
                source_surface=excluded.source_surface,
                summary=excluded.summary,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            row,
        )
        return self.agent_session_row(row["session_id"])

    def update_agent_session_row(self, row: dict[str, Any]) -> Any:
        self.conn.execute(
            """
            UPDATE kanban_agent_sessions SET
                agent_id=:agent_id,
                node_id=:node_id,
                worktree_path=:worktree_path,
                repo_full_name=:repo_full_name,
                branch=:branch,
                status=:status,
                started_at=:started_at,
                ended_at=:ended_at,
                last_seen_at=:last_seen_at,
                request_hash=:request_hash,
                source_surface=:source_surface,
                summary=:summary,
                metadata_json=:metadata_json,
                provenance_json=:provenance_json,
                updated_at=:updated_at
            WHERE session_id=:session_id
            """,
            row,
        )
        return self.agent_session_row(row["session_id"])

    def item_commit_row_for_ref(
        self,
        *,
        item_id: str,
        repo_full_name: str,
        sha: str,
    ) -> Any | None:
        return self.conn.execute(
            """
            SELECT * FROM kanban_item_commits
            WHERE item_id=? AND repo_full_name=? AND sha=?
            """,
            (item_id, repo_full_name, sha),
        ).fetchone()

    def upsert_item_commit_row(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        existing = self.item_commit_row_for_ref(
            item_id=payload["item_id"],
            repo_full_name=payload["repo_full_name"],
            sha=payload["sha"],
        )
        self.conn.execute(
            """
            INSERT INTO kanban_item_commits (
                commit_link_id, item_id, repo_full_name, sha, short_sha, html_url,
                author_login, author_name, committed_at, message_subject, message_body,
                branch, metadata_json, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(item_id, repo_full_name, sha) DO UPDATE SET
                short_sha=excluded.short_sha,
                html_url=excluded.html_url,
                author_login=excluded.author_login,
                author_name=excluded.author_name,
                committed_at=excluded.committed_at,
                message_subject=excluded.message_subject,
                message_body=excluded.message_body,
                branch=excluded.branch,
                metadata_json=excluded.metadata_json,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                payload["commit_link_id"],
                payload["item_id"],
                payload["repo_full_name"],
                payload["sha"],
                payload["short_sha"],
                payload["html_url"],
                payload["author_login"],
                payload["author_name"],
                payload["committed_at"],
                payload["message_subject"],
                payload["message_body"],
                payload["branch"],
                json.dumps(payload["metadata"], ensure_ascii=True, sort_keys=True),
                json.dumps(payload["provenance"], ensure_ascii=True, sort_keys=True),
                payload["created_at"],
                payload["updated_at"],
            ),
        )
        commit_link_id = existing["commit_link_id"] if existing else payload["commit_link_id"]
        row = self.conn.execute(
            "SELECT * FROM kanban_item_commits WHERE commit_link_id=?",
            (commit_link_id,),
        ).fetchone()
        return row, existing is not None

    def item(self, item_id: str | None) -> Any | None:
        clean_item_id = self._clean_id(item_id)
        if not clean_item_id:
            return None
        return self.conn.execute(
            "SELECT * FROM kanban_items WHERE item_id=?",
            (clean_item_id,),
        ).fetchone()

    def item_or_raise(self, item_id: str | None) -> Any:
        row = self.item(item_id)
        if row is None:
            raise KanbanItemNotFound("Kanban item not found")
        return row

    def breadcrumbs(self, item_id: str | None) -> list[Any]:
        current = self._clean_id(item_id)
        if not current:
            return []
        rows: list[Any] = []
        seen: set[str] = set()
        while current:
            if current in seen:
                raise KanbanItemCycleError("Kanban item parent cycle detected")
            seen.add(current)
            row = self.item_or_raise(current)
            rows.append(row)
            current = row["parent_item_id"]
            if len(rows) > self.depth_limit + 2:
                raise KanbanItemCycleError("Kanban item parent cycle detected")
        return list(reversed(rows))

    def rollup(
        self,
        item_id: str | None = None,
        *,
        show_test_entries: bool = True,
    ) -> dict[str, Any]:
        item_ids = self.scope_item_ids(item_id, show_test_entries=show_test_entries)
        if not item_ids:
            return {
                "items": {
                    "total": 0,
                    "by_state": {},
                    "by_status": {},
                    "leaf_metrics": self._leaf_metrics([]),
                },
                "issues": {
                    "open": 0,
                    "leaf_metrics": self._leaf_metrics([]),
                },
                "todos": {"open": 0},
                "blockers": {"open": 0},
                "depth_limit": self.depth_limit,
            }
        placeholders = ",".join("?" for _ in item_ids)
        state_rows = self.conn.execute(
            f"SELECT state_id, COUNT(*) AS count FROM kanban_items WHERE item_id IN ({placeholders}) "
            "GROUP BY state_id",
            item_ids,
        ).fetchall()
        status_rows = self.conn.execute(
            f"SELECT status, COUNT(*) AS count FROM kanban_items WHERE item_id IN ({placeholders}) "
            "GROUP BY status",
            item_ids,
        ).fetchall()
        descendant_item_ids = [
            scope_item_id for scope_item_id in item_ids if not item_id or scope_item_id != item_id
        ]
        if descendant_item_ids:
            descendant_placeholders = ",".join("?" for _ in descendant_item_ids)
            issue_open = self.conn.execute(
                f"SELECT COUNT(*) AS count FROM kanban_items WHERE item_id IN ({descendant_placeholders}) "
                "AND item_type='issue' AND status NOT IN ('done', 'closed', 'archived')",
                descendant_item_ids,
            ).fetchone()
            todo_open = self.conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM kanban_items w
                WHERE w.item_id IN ({descendant_placeholders})
                  AND w.state_id='todo'
                  AND w.status != 'archived'
                  AND w.item_type != 'issue'
                  AND NOT EXISTS (
                      SELECT 1 FROM kanban_items child
                      WHERE child.parent_item_id=w.item_id
                        AND child.status != 'archived'
                  )
                """,
                descendant_item_ids,
            ).fetchone()
        else:
            issue_open = {"count": 0}
            todo_open = {"count": 0}
        item_leaf_metrics = self._rollup_leaf_metrics(descendant_item_ids, issue_cards=False)
        issue_leaf_metrics = self._rollup_leaf_metrics(descendant_item_ids, issue_cards=True)
        blocker_open = self.conn.execute(
            f"SELECT COUNT(*) AS count FROM kanban_blockers WHERE item_id IN ({placeholders}) "
            "AND status NOT IN ('resolved', 'archived')",
            item_ids,
        ).fetchone()
        return {
            "items": {
                "total": len(item_ids),
                "by_state": {row["state_id"]: row["count"] for row in state_rows},
                "by_status": {row["status"]: row["count"] for row in status_rows},
                "leaf_metrics": item_leaf_metrics,
            },
            "issues": {
                "open": int(issue_open["count"] if issue_open else 0),
                "leaf_metrics": issue_leaf_metrics,
            },
            "todos": {"open": int(todo_open["count"] if todo_open else 0)},
            "blockers": {"open": int(blocker_open["count"] if blocker_open else 0)},
            "depth_limit": self.depth_limit,
        }

    def scope_item_ids(
        self,
        item_id: str | None,
        *,
        show_test_entries: bool = True,
    ) -> list[str]:
        clean_item_id = self._clean_id(item_id) or None
        if not clean_item_id:
            rows = self.conn.execute(
                "SELECT * FROM kanban_items WHERE status != 'archived'"
            ).fetchall()
            rows, _hidden = self._filter_test_rows(rows, show_test_entries)
            return [row["item_id"] for row in rows]
        self.item_or_raise(clean_item_id)
        rows = self.conn.execute(
            """
            WITH RECURSIVE descendants(item_id) AS (
                SELECT item_id FROM kanban_items WHERE item_id=?
                UNION ALL
                SELECT w.item_id
                FROM kanban_items w
                JOIN descendants ON w.parent_item_id = descendants.item_id
                WHERE w.status != 'archived'
            )
            SELECT item_id FROM descendants
            """,
            (clean_item_id,),
        ).fetchall()
        item_ids = [row["item_id"] for row in rows]
        if show_test_entries or not item_ids:
            return item_ids
        placeholders = ",".join("?" for _ in item_ids)
        scoped_rows = self.conn.execute(
            f"SELECT * FROM kanban_items WHERE item_id IN ({placeholders})",
            item_ids,
        ).fetchall()
        scoped_rows, _hidden = self._filter_test_rows(scoped_rows, show_test_entries)
        return [row["item_id"] for row in scoped_rows]

    def sort_items_for_lane(self, rows: list[Any]) -> list[Any]:
        if len(rows) <= 1:
            return list(rows)
        priorities = self._priority_sort_map()
        groups: dict[str, list[Any]] = {}
        for row in rows:
            groups.setdefault(row["priority_id"] or "medium", []).append(row)
        priority_ids = sorted(
            groups,
            key=lambda priority_id: (
                -priorities.get(priority_id, {}).get("weight", 0),
                -priorities.get(priority_id, {}).get("sort_order", 0),
                priority_id,
            ),
        )
        ordered: list[Any] = []
        for priority_id in priority_ids:
            ordered.extend(self._order_priority_group(groups[priority_id]))
        return ordered

    def _rollup_leaf_metrics(
        self,
        descendant_item_ids: list[str],
        *,
        issue_cards: bool,
    ) -> dict[str, Any]:
        if not descendant_item_ids:
            return self._leaf_metrics([])
        descendant_placeholders = ",".join("?" for _ in descendant_item_ids)
        issue_predicate = "w.item_type='issue'" if issue_cards else "w.item_type!='issue'"
        rows = self.conn.execute(
            f"""
            SELECT w.*
            FROM kanban_items w
            WHERE w.item_id IN ({descendant_placeholders})
              AND w.status != 'archived'
              AND {issue_predicate}
              AND NOT EXISTS (
                  SELECT 1 FROM kanban_items child
                  WHERE child.parent_item_id=w.item_id
                    AND child.status != 'archived'
              )
            """,
            descendant_item_ids,
        ).fetchall()
        return self._leaf_metrics(rows)

    def _leaf_metrics(self, rows: list[Any]) -> dict[str, Any]:
        by_state: dict[str, int] = {}
        by_status: dict[str, int] = {}
        active = 0
        active_doing = 0
        blocked = 0
        done = 0
        for row in rows:
            state_id = str(row["state_id"] or "")
            status = str(row["status"] or "")
            by_state[state_id] = by_state.get(state_id, 0) + 1
            by_status[status] = by_status.get(status, 0) + 1
            if state_id in {"backlog", "todo", "doing"}:
                active += 1
            if state_id == "doing":
                active_doing += 1
            if state_id == "blocked" or status == "blocked":
                blocked += 1
            if state_id == "done" or status in {"done", "closed", "promoted"}:
                done += 1
        return {
            "total": len(rows),
            "active": active,
            "active_doing": active_doing,
            "blocked": blocked,
            "done": done,
            "by_state": by_state,
            "by_status": by_status,
        }

    def _priority_sort_map(self) -> dict[str, dict[str, int]]:
        rows = self.conn.execute(
            "SELECT priority_id, weight, sort_order FROM kanban_item_priorities"
        ).fetchall()
        return {
            row["priority_id"]: {
                "weight": int(row["weight"] or 0),
                "sort_order": int(row["sort_order"] or 0),
            }
            for row in rows
        }

    def _order_priority_group(self, rows: list[Any]) -> list[Any]:
        if len(rows) <= 1:
            return list(rows)
        fallback = sorted(
            rows,
            key=lambda row: (
                int(row["sort_order"] or 0),
                str(row["created_at"] or ""),
                row["item_id"],
            ),
        )
        fallback_index = {row["item_id"]: index for index, row in enumerate(fallback)}
        row_by_id = {row["item_id"]: row for row in fallback}
        item_ids = set(row_by_id)
        first = fallback[0]
        edges = self._order_edges_for_group(
            first["parent_item_id"],
            first["state_id"],
            first["priority_id"],
            item_ids,
        )
        if not edges:
            return fallback
        adjacency: dict[str, set[str]] = {item_id: set() for item_id in item_ids}
        indegree: dict[str, int] = {item_id: 0 for item_id in item_ids}
        seen_pairs: set[tuple[str, str]] = set()
        for edge in edges:
            before = edge["before_item_id"]
            after = edge["after_item_id"]
            pair = (before, after)
            if pair in seen_pairs or after in adjacency[before]:
                continue
            seen_pairs.add(pair)
            adjacency[before].add(after)
            indegree[after] += 1
        ready = sorted(
            [item_id for item_id, value in indegree.items() if value == 0],
            key=lambda item_id: fallback_index[item_id],
        )
        ordered_ids: list[str] = []
        while ready:
            item_id = ready.pop(0)
            ordered_ids.append(item_id)
            for after in sorted(adjacency[item_id], key=lambda value: fallback_index[value]):
                indegree[after] -= 1
                if indegree[after] == 0:
                    ready.append(after)
                    ready.sort(key=lambda value: fallback_index[value])
        if len(ordered_ids) < len(item_ids):
            ordered_ids.extend(
                row["item_id"] for row in fallback if row["item_id"] not in ordered_ids
            )
        return [row_by_id[item_id] for item_id in ordered_ids]

    def _order_edges_for_group(
        self,
        parent_item_id: str | None,
        state_id: str,
        priority_id: str,
        item_ids: set[str],
    ) -> list[Any]:
        if not item_ids:
            return []
        rows = self.conn.execute(
            """
            SELECT * FROM kanban_item_order_edges
            WHERE parent_item_id=? AND state_id=? AND priority_id=?
            ORDER BY updated_at DESC, edge_id
            """,
            (self._lane_parent_key(parent_item_id), state_id, priority_id),
        ).fetchall()
        return [
            row
            for row in rows
            if row["before_item_id"] in item_ids
            and row["after_item_id"] in item_ids
            and row["before_item_id"] != row["after_item_id"]
        ]

    def _filter_test_rows(self, rows: list[Any], show_test_entries: bool) -> tuple[list[Any], int]:
        if show_test_entries:
            return list(rows), 0
        visible: list[Any] = []
        hidden = 0
        for row in rows:
            if self._is_test_entry(row):
                hidden += 1
            else:
                visible.append(row)
        return visible, hidden

    def _is_test_entry(self, row: Any) -> bool:
        tags = {
            str(tag).strip().lower()
            for tag in _json_value(row["tags_json"], [])
            if str(tag).strip()
        }
        return self.agent_working_out_tag in tags

    def _lane_parent_key(self, parent_item_id: str | None) -> str:
        return self._clean_id(parent_item_id)

    @staticmethod
    def _clean_id(value: str | None, fallback: str = "", limit: int = 180) -> str:
        text = str(value or fallback or "").strip()
        if not text:
            return ""
        return text[:limit]

    @staticmethod
    def _require_document_reader(
        callback: Callable[[Any, str], dict[str, Any]] | None,
        label: str,
    ) -> Callable[[Any, str], dict[str, Any]]:
        if callback is None:
            raise KanbanStoreError(f"Kanban store missing {label}")
        return callback

    @staticmethod
    def _require_document_writer(
        callback: Callable[..., dict[str, Any]] | None,
        label: str,
    ) -> Callable[..., dict[str, Any]]:
        if callback is None:
            raise KanbanStoreError(f"Kanban store missing {label}")
        return callback
