"""Persistent Postgres candidate store helpers for Kanban.

The live store remains SQLite in this slice.  These helpers let Blueprints
bootstrap a node-local Postgres candidate, load current Kanban rows into it,
and run read-only API paths through that candidate without changing write
semantics.
"""

from __future__ import annotations

import asyncio
import re
import sqlite3
import threading
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import asyncpg

from .kanban_datastore import KANBAN_DATASTORE_TABLES

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_JSON_EXTRACT_RE = re.compile(
    r"json_extract\((?P<expr>[^,]+),\s*'\$\.(?P<path>[A-Za-z0-9_.-]+)'\)",
    flags=re.IGNORECASE,
)
_PRAGMA_TABLE_INFO_RE = re.compile(
    r"^\s*PRAGMA\s+table_info\((?P<table>\"[^\"]+\"|[A-Za-z_][A-Za-z0-9_]*)\)\s*$",
    flags=re.IGNORECASE,
)


class KanbanPostgresError(RuntimeError):
    """Raised when the Postgres candidate cannot be safely used."""


class PostgresRow(dict[str, Any]):
    """Small row object compatible with sqlite3.Row's mapping/index access."""

    def __init__(self, columns: Sequence[str], values: Sequence[Any]) -> None:
        super().__init__(zip(columns, values))
        self._columns = tuple(columns)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return super().__getitem__(self._columns[key])
        return super().__getitem__(key)


class PostgresCursor:
    def __init__(self, rows: Iterable[PostgresRow] = ()) -> None:
        self._rows = list(rows)

    def __iter__(self):
        return iter(self._rows)

    def fetchall(self) -> list[PostgresRow]:
        return list(self._rows)

    def fetchone(self) -> PostgresRow | None:
        return self._rows[0] if self._rows else None


class _AsyncpgRunner:
    def __init__(self) -> None:
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._ready.wait(timeout=10)
        if not hasattr(self, "_loop"):
            raise KanbanPostgresError("Postgres candidate event loop did not start")

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()

    def run(self, coro: Any, *, timeout: float = 60.0) -> Any:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)


_RUNNER: _AsyncpgRunner | None = None
_RUNNER_LOCK = threading.Lock()


def _runner() -> _AsyncpgRunner:
    global _RUNNER
    with _RUNNER_LOCK:
        if _RUNNER is None:
            _RUNNER = _AsyncpgRunner()
        return _RUNNER


def _quote_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise KanbanPostgresError(f"unsafe Postgres identifier: {value!r}")
    return f'"{value}"'


def _sqlite_identifier(value: str) -> str:
    if not _IDENTIFIER_RE.match(value):
        raise KanbanPostgresError(f"unsafe SQLite identifier: {value!r}")
    return f'"{value}"'


def _row_get(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        return getattr(row, key)


def _sqlite_table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({_sqlite_identifier(table)})").fetchall()
    if not rows:
        raise KanbanPostgresError(f"SQLite source table missing: {table}")
    return [str(row["name"]) for row in rows]


def _sqlite_table_pk(conn: sqlite3.Connection, table: str) -> str:
    rows = conn.execute(f"PRAGMA table_info({_sqlite_identifier(table)})").fetchall()
    for row in rows:
        if int(row["pk"] or 0):
            return str(row["name"])
    raise KanbanPostgresError(f"SQLite source table has no primary key: {table}")


def _sqlite_collect_table_data(
    conn: sqlite3.Connection,
    tables: Sequence[str],
    *,
    settings_keys: Sequence[str] = (),
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for table in tables:
        columns = _sqlite_table_columns(conn, table)
        pk = _sqlite_table_pk(conn, table)
        quoted_table = _sqlite_identifier(table)
        if table == "settings" and settings_keys:
            placeholders = ",".join("?" for _ in settings_keys)
            rows = conn.execute(
                f"SELECT * FROM {quoted_table} WHERE key IN ({placeholders}) "
                f"ORDER BY {_sqlite_identifier(pk)}",
                tuple(settings_keys),
            ).fetchall()
        else:
            rows = conn.execute(
                f"SELECT * FROM {quoted_table} ORDER BY {_sqlite_identifier(pk)}"
            ).fetchall()
        payloads[table] = {
            "columns": columns,
            "primary_key": pk,
            "rows": [{column: row[column] for column in columns} for row in rows],
        }
    return {
        "schema": "xarta.kanban.datastore.postgres.table_data.v1",
        "tables": payloads,
        "excluded_tables": ["sync_queue"],
        "sync_queue_included": False,
    }


def _translate_json_extract(sql: str) -> str:
    def repl(match: re.Match[str]) -> str:
        expr = match.group("expr").strip()
        path = ",".join(part for part in match.group("path").split(".") if part)
        return f"(({expr})::jsonb #>> '{{{path}}}')"

    return _JSON_EXTRACT_RE.sub(repl, sql)


def _translate_placeholders(sql: str) -> str:
    result: list[str] = []
    index = 1
    in_single_quote = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'":
            result.append(char)
            if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                result.append(sql[i + 1])
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue
        if char == "?" and not in_single_quote:
            result.append(f"${index}")
            index += 1
            i += 1
            continue
        result.append(char)
        i += 1
    return "".join(result)


def _translate_named_placeholders(
    sql: str, params: Mapping[str, Any]
) -> tuple[str, tuple[Any, ...]]:
    result: list[str] = []
    ordered_names: list[str] = []
    name_to_index: dict[str, int] = {}
    in_single_quote = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'":
            result.append(char)
            if in_single_quote and i + 1 < len(sql) and sql[i + 1] == "'":
                result.append(sql[i + 1])
                i += 2
                continue
            in_single_quote = not in_single_quote
            i += 1
            continue
        if (
            char == ":"
            and not in_single_quote
            and (i == 0 or sql[i - 1] != ":")
            and i + 1 < len(sql)
            and (sql[i + 1].isalpha() or sql[i + 1] == "_")
        ):
            j = i + 2
            while j < len(sql) and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            name = sql[i + 1 : j]
            if name not in params:
                raise KanbanPostgresError(f"missing named Postgres parameter: {name}")
            if name not in name_to_index:
                ordered_names.append(name)
                name_to_index[name] = len(ordered_names)
            result.append(f"${name_to_index[name]}")
            i = j
            continue
        result.append(char)
        i += 1
    return "".join(result), tuple(params[name] for name in ordered_names)


def translate_sqlite_query_to_postgres(sql: str) -> str:
    statement = _translate_json_extract(sql)
    statement = re.sub(
        r"datetime\('now'\)",
        "CURRENT_TIMESTAMP::text",
        statement,
        flags=re.IGNORECASE,
    )
    return _translate_placeholders(statement)


def prepare_sqlite_query_for_postgres(
    sql: str,
    params: Sequence[Any] | Mapping[str, Any] | None = None,
) -> tuple[str, tuple[Any, ...]]:
    statement = _translate_json_extract(sql)
    statement = re.sub(
        r"datetime\('now'\)",
        "CURRENT_TIMESTAMP::text",
        statement,
        flags=re.IGNORECASE,
    )
    if isinstance(params, Mapping):
        return _translate_named_placeholders(statement, params)
    return _translate_placeholders(statement), tuple(params or ())


def _record_to_row(record: asyncpg.Record) -> PostgresRow:
    columns = list(record.keys())
    return PostgresRow(columns, [record[column] for column in columns])


@dataclass(frozen=True)
class PostgresTableInfo:
    name: str
    primary_key: str
    columns: tuple[str, ...]


class PostgresSyncConnection:
    """Synchronous DB-API-ish wrapper over asyncpg for existing Kanban reads."""

    def __init__(self, database_url: str) -> None:
        if not database_url.strip():
            raise KanbanPostgresError("Postgres candidate database URL is not configured")
        self.database_url = database_url
        self._runner = _runner()
        self._conn = self._runner.run(
            asyncpg.connect(database_url, timeout=10, statement_cache_size=0)
        )
        self._transaction: asyncpg.transaction.Transaction | None = None

    def close(self) -> None:
        if self._conn is not None:
            if self._transaction is not None:
                with suppress(Exception):
                    self.rollback()
            self._runner.run(self._conn.close())
            self._conn = None

    def begin(self) -> None:
        if self._transaction is not None:
            return

        async def start_transaction() -> asyncpg.transaction.Transaction:
            transaction = self._conn.transaction()
            await transaction.start()
            return transaction

        self._transaction = self._runner.run(start_transaction())

    def commit(self) -> None:
        if self._transaction is None:
            return None
        self._runner.run(self._transaction.commit())
        self._transaction = None
        return None

    def rollback(self) -> None:
        if self._transaction is None:
            return None
        self._runner.run(self._transaction.rollback())
        self._transaction = None
        return None

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
    ) -> PostgresCursor:
        pragma = _PRAGMA_TABLE_INFO_RE.match(sql.strip())
        if pragma:
            table = pragma.group("table").strip('"')
            return PostgresCursor(self._pragma_table_info(table))
        statement, args = prepare_sqlite_query_for_postgres(sql, params)
        first = statement.lstrip().split(None, 1)[0].lower() if statement.strip() else ""
        if first in {"select", "with", "show"}:
            records = self._runner.run(self._conn.fetch(statement, *args))
            return PostgresCursor(_record_to_row(record) for record in records)
        self._runner.run(self._conn.execute(statement, *args))
        return PostgresCursor()

    def executemany(
        self,
        sql: str,
        seq_of_params: Iterable[Sequence[Any] | Mapping[str, Any]],
    ) -> None:
        prepared_rows = [prepare_sqlite_query_for_postgres(sql, params) for params in seq_of_params]
        if not prepared_rows:
            return None
        statements = {statement for statement, _args in prepared_rows}
        if len(statements) != 1:
            raise KanbanPostgresError("executemany produced inconsistent Postgres statements")
        statement = prepared_rows[0][0]
        self._runner.run(
            self._conn.executemany(statement, [args for _statement, args in prepared_rows])
        )

    def _pragma_table_info(self, table: str) -> list[PostgresRow]:
        info = _postgres_table_info(self._conn, table, runner=self._runner)
        rows: list[PostgresRow] = []
        for index, column in enumerate(info.columns):
            rows.append(
                PostgresRow(
                    ("cid", "name", "type", "notnull", "dflt_value", "pk"),
                    (index, column, "", 0, None, 1 if column == info.primary_key else 0),
                )
            )
        return rows


def postgres_candidate_connection(database_url: str) -> PostgresSyncConnection:
    return PostgresSyncConnection(database_url)


def _postgres_table_info(
    conn: asyncpg.Connection,
    table: str,
    *,
    runner: _AsyncpgRunner,
) -> PostgresTableInfo:
    if not _IDENTIFIER_RE.match(table):
        raise KanbanPostgresError(f"unsafe Postgres table name: {table!r}")

    async def fetch_info() -> tuple[list[str], str | None]:
        columns = await conn.fetch(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=$1
            ORDER BY ordinal_position
            """,
            table,
        )
        pk = await conn.fetchval(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema='public'
              AND tc.table_name=$1
              AND tc.constraint_type='PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            LIMIT 1
            """,
            table,
        )
        return [str(row["column_name"]) for row in columns], str(pk) if pk else None

    columns, primary_key = runner.run(fetch_info())
    if not columns:
        raise KanbanPostgresError(f"Postgres candidate table missing: {table}")
    if not primary_key:
        raise KanbanPostgresError(f"Postgres candidate table has no primary key: {table}")
    return PostgresTableInfo(name=table, primary_key=primary_key, columns=tuple(columns))


def _table_counts(table_data: dict[str, Any]) -> dict[str, int]:
    return {
        table: len(payload.get("rows") or [])
        for table, payload in (table_data.get("tables") or {}).items()
    }


def _schema_statements(statements: Sequence[dict[str, Any]]) -> list[str]:
    return [str(statement["sql"]).strip().rstrip(";") + ";" for statement in statements]


def _insert_sql(table: str, columns: Sequence[str]) -> str:
    quoted_table = _quote_identifier(table)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    return f"INSERT INTO {quoted_table} ({quoted_columns}) VALUES ({placeholders})"


def bootstrap_postgres_candidate(
    sqlite_conn: sqlite3.Connection,
    *,
    database_url: str,
    statements: Sequence[dict[str, Any]],
    support_setting_keys: Sequence[str] = (),
) -> dict[str, Any]:
    """Apply schema and replace candidate data with current SQLite Kanban rows."""

    if not database_url.strip():
        raise KanbanPostgresError("Postgres candidate database URL is not configured")

    tables = (*KANBAN_DATASTORE_TABLES, "settings")
    table_data = _sqlite_collect_table_data(
        sqlite_conn,
        tables,
        settings_keys=tuple(support_setting_keys),
    )
    ddl = _schema_statements(statements)
    runner = _runner()

    async def apply() -> dict[str, Any]:
        conn = await asyncpg.connect(database_url, timeout=10, statement_cache_size=0)
        try:
            async with conn.transaction():
                for statement in ddl:
                    await conn.execute(statement)
                for table in reversed(tables):
                    if table == "settings" and support_setting_keys:
                        await conn.execute(
                            f"DELETE FROM {_quote_identifier(table)} WHERE key = ANY($1::text[])",
                            list(support_setting_keys),
                        )
                    else:
                        await conn.execute(f"DELETE FROM {_quote_identifier(table)}")
                loaded_rows = 0
                for table in tables:
                    payload = table_data["tables"][table]
                    columns = list(payload.get("columns") or [])
                    rows = payload.get("rows") or []
                    if not columns or not rows:
                        continue
                    values = [[row.get(column) for column in columns] for row in rows]
                    await conn.executemany(_insert_sql(table, columns), values)
                    loaded_rows += len(rows)
                counts = {
                    table: int(
                        await conn.fetchval(f"SELECT COUNT(*) FROM {_quote_identifier(table)}") or 0
                    )
                    for table in tables
                }
            return {"loaded_rows": loaded_rows, "postgres_counts": counts}
        finally:
            await conn.close()

    result = runner.run(apply(), timeout=180)
    sqlite_counts = _table_counts(table_data)
    return {
        "schema": "xarta.kanban.datastore.postgres.bootstrap_result.v1",
        "storage": "postgres",
        "schema_statement_count": len(ddl),
        "loaded_rows": int(result["loaded_rows"]),
        "sqlite_counts": sqlite_counts,
        "postgres_counts": result["postgres_counts"],
        "support_settings_copied": sqlite_counts.get("settings", 0),
        "included_tables": list(KANBAN_DATASTORE_TABLES),
        "support_tables": ["settings"],
        "excluded_tables": ["sync_queue"],
    }
