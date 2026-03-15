"""routes_schema.py — live SQLite schema documentation endpoints."""

from __future__ import annotations

from fastapi import APIRouter

from .db import get_conn

router = APIRouter(prefix="/schema", tags=["schema"])


def _list_tables(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()
    return [r[0] for r in rows]


def _pluralize(word: str) -> str:
    if word.endswith("y") and len(word) > 1 and word[-2] not in "aeiou":
        return f"{word[:-1]}ies"
    if word.endswith("s"):
        return word
    return f"{word}s"


def _infer_relationships(tables: list[str], table_columns: dict[str, list[dict]]) -> list[dict]:
    relationships: list[dict] = []
    table_set = set(tables)

    for source_table, columns in table_columns.items():
        column_names = {col["name"] for col in columns}
        for column in columns:
            name = column["name"]
            if not name.endswith("_id"):
                continue
            if name == f"{source_table.rstrip('s')}_id":
                continue

            target_table = None
            target_column = None

            if name.startswith("parent_"):
                stem = name[len("parent_"):-len("_id")]
                candidate = _pluralize(stem)
                if candidate in table_set:
                    target_table = candidate
                    target_column = name[len("parent_"):]

            if target_table is None:
                stem = name[:-len("_id")]
                for candidate in (_pluralize(stem), stem):
                    candidate_column = f"{stem}_id"
                    if candidate in table_set and candidate_column in {
                        c["name"] for c in table_columns[candidate]
                    }:
                        target_table = candidate
                        target_column = candidate_column
                        break

            if target_table is None:
                continue

            relationships.append(
                {
                    "source_table": source_table,
                    "source_column": name,
                    "target_table": target_table,
                    "target_column": target_column,
                    "kind": "inferred",
                }
            )

        if source_table in {"nodes", "services"} and "host_machine" in column_names and "machines" in table_set:
            relationships.append(
                {
                    "source_table": source_table,
                    "source_column": "host_machine",
                    "target_table": "machines",
                    "target_column": "machine_id",
                    "kind": "logical",
                }
            )

    unique = []
    seen = set()
    for rel in relationships:
        key = (
            rel["source_table"],
            rel["source_column"],
            rel["target_table"],
            rel["target_column"],
            rel["kind"],
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(rel)

    return unique


def _build_mermaid(tables: list[str], table_columns: dict[str, list[dict]], relationships: list[dict]) -> str:
    lines: list[str] = ["erDiagram"]

    for table in tables:
        lines.append(f"    {table.upper()} {{")
        for column in table_columns[table]:
            col_type = column["type"] or "TEXT"
            is_pk = column["pk"]
            is_fk = any(
                rel["source_table"] == table and rel["source_column"] == column["name"]
                for rel in relationships
            )
            # Mermaid erDiagram only accepts one key attribute per column.
            # PK takes precedence; FK alone is valid; PK+FK together is not.
            if is_pk:
                suffix = " PK"
            elif is_fk:
                suffix = " FK"
            else:
                suffix = ""
            lines.append(f"        {col_type} {column['name']}{suffix}")
        lines.append("    }")
        lines.append("")

    for rel in relationships:
        note = f"{rel['source_column']} -> {rel['target_column']} ({rel['kind']})"
        lines.append(
            f"    {rel['target_table'].upper()} ||--o{{ {rel['source_table'].upper()} : \"{note}\""
        )

    return "\n".join(lines).strip()


@router.get("")
async def get_schema() -> dict:
    with get_conn() as conn:
        tables = _list_tables(conn)
        table_columns: dict[str, list[dict]] = {}
        declared_fk_map: dict[str, dict[str, dict]] = {}

        for table in tables:
            columns = []
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall():
                columns.append(
                    {
                        "name": row[1],
                        "type": row[2] or "TEXT",
                        "notnull": bool(row[3]),
                        "default": row[4],
                        "pk": bool(row[5]),
                    }
                )
            table_columns[table] = columns

            declared_fk_map[table] = {}
            for row in conn.execute(f"PRAGMA foreign_key_list({table})").fetchall():
                declared_fk_map[table][row[3]] = {
                    "target_table": row[2],
                    "target_column": row[4],
                    "kind": "declared",
                }

    inferred_relationships = _infer_relationships(tables, table_columns)

    relationships = []
    for table, fk_map in declared_fk_map.items():
        for source_column, fk in fk_map.items():
            relationships.append(
                {
                    "source_table": table,
                    "source_column": source_column,
                    "target_table": fk["target_table"],
                    "target_column": fk["target_column"],
                    "kind": fk["kind"],
                }
            )

    existing_keys = {
        (r["source_table"], r["source_column"], r["target_table"], r["target_column"]) for r in relationships
    }
    for rel in inferred_relationships:
        key = (rel["source_table"], rel["source_column"], rel["target_table"], rel["target_column"])
        if key not in existing_keys:
            relationships.append(rel)

    tables_payload = []
    rel_by_source = {}
    for rel in relationships:
        rel_by_source.setdefault((rel["source_table"], rel["source_column"]), []).append(rel)

    for table in tables:
        cols_payload = []
        for column in table_columns[table]:
            notes = []
            if column["pk"]:
                notes.append("PK")
            for rel in rel_by_source.get((table, column["name"]), []):
                notes.append(
                    f"-> {rel['target_table']}.{rel['target_column']} ({rel['kind']})"
                )
            cols_payload.append(
                {
                    "column": column["name"],
                    "type": column["type"],
                    "nullable": not column["notnull"],
                    "default": column["default"],
                    "notes": notes,
                }
            )
        tables_payload.append({"table": table, "columns": cols_payload})

    return {
        "tables": tables_payload,
        "relationships": relationships,
        "mermaid": _build_mermaid(tables, table_columns, relationships),
    }
