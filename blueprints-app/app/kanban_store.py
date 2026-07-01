"""Kanban storage boundary for the current SQLite-backed implementation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

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
    """Repository facade for Kanban reads backed by the Blueprints SQLite DB."""

    def __init__(
        self,
        conn: Any,
        *,
        depth_limit: int,
        show_test_entries_setting: str,
        agent_working_out_tag: str,
    ) -> None:
        self.conn = conn
        self.depth_limit = depth_limit
        self.show_test_entries_setting = show_test_entries_setting
        self.agent_working_out_tag = agent_working_out_tag

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
            ORDER BY created_at DESC
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
