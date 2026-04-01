"""Helpers for table layout keying, validation, sibling seeding, and fallback generation."""

from __future__ import annotations

import copy
import json
import re
from typing import Any


_HEX_RE = re.compile(r"^[0-9A-F]+$")
_BUCKET_MASK = 0x1F
_MIN_WIDTH = 56
_MAX_WIDTH = 2400


class TableLayoutError(ValueError):
    """Raised when a table layout request or stored payload is invalid."""


def normalize_hex_byte(value: str | int, field_name: str) -> str:
    if isinstance(value, int):
        if value < 0 or value > 0xFF:
            raise TableLayoutError(f"{field_name} must be between 00 and FF")
        return f"{value:02X}"

    raw = str(value or "").strip().upper()
    if raw.startswith("0X"):
        raw = raw[2:]
    if len(raw) == 1:
        raw = f"0{raw}"
    if len(raw) != 2 or not _HEX_RE.match(raw):
        raise TableLayoutError(f"{field_name} must be a 2-digit uppercase hex byte")
    return raw


def validate_reserved_code(reserved_code: str) -> str:
    code = normalize_hex_byte(reserved_code, "reserved_code")
    if code != "00":
        raise TableLayoutError("reserved_code is currently reserved and must remain 00")
    return code


def validate_bucket_code(bucket_code: str) -> str:
    code = normalize_hex_byte(bucket_code, "bucket_code")
    if int(code, 16) & ~_BUCKET_MASK:
        raise TableLayoutError("bucket_code currently supports only bits 0-4 (00-1F)")
    return code


def encode_bucket_code(bits: dict[str, bool] | None) -> str:
    flags = bits or {}
    value = 0
    if flags.get("shade_up"):
        value |= 0x01
    if flags.get("horizontal_scroll"):
        value |= 0x02
    if flags.get("mobile"):
        value |= 0x04
    if flags.get("portrait"):
        value |= 0x08
    if flags.get("wide"):
        value |= 0x10
    return f"{value:02X}"


def decode_bucket_code(bucket_code: str) -> dict[str, bool]:
    code = validate_bucket_code(bucket_code)
    value = int(code, 16)
    return {
        "shade_up": bool(value & 0x01),
        "horizontal_scroll": bool(value & 0x02),
        "mobile": bool(value & 0x04),
        "portrait": bool(value & 0x08),
        "wide": bool(value & 0x10),
    }


def build_layout_key(reserved_code: str, user_code: str, table_code: str, bucket_code: str) -> str:
    reserved = validate_reserved_code(reserved_code)
    user = normalize_hex_byte(user_code, "user_code")
    table = normalize_hex_byte(table_code, "table_code")
    bucket = validate_bucket_code(bucket_code)
    return f"{reserved}{user}{table}{bucket}"


def split_layout_key(layout_key: str) -> dict[str, str]:
    raw = str(layout_key or "").strip().upper()
    if raw.startswith("0X"):
        raw = raw[2:]
    if len(raw) != 8 or not _HEX_RE.match(raw):
        raise TableLayoutError("layout_key must be an 8-digit uppercase hex key")
    parts = {
        "layout_key": raw,
        "reserved_code": raw[0:2],
        "user_code": raw[2:4],
        "table_code": raw[4:6],
        "bucket_code": raw[6:8],
    }
    validate_reserved_code(parts["reserved_code"])
    validate_bucket_code(parts["bucket_code"])
    return parts


def parse_json_text(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _sanitize_column_key(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned)
    return cleaned or "column"


def _clamp_width(value: int | None, fallback: int) -> int:
    if value is None:
        return fallback
    try:
        width = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(_MIN_WIDTH, min(_MAX_WIDTH, width))


def normalize_column_seed(seed: dict[str, Any], fallback_position: int = 0) -> dict[str, Any]:
    display_name = str(seed.get("display_name") or seed.get("label") or "").strip()
    if not display_name:
        raise TableLayoutError("each layout column requires display_name")

    sqlite_column = seed.get("sqlite_column")
    column_key = seed.get("column_key") or sqlite_column or display_name
    sort_direction = seed.get("sort_direction")
    if sort_direction not in (None, "asc", "desc"):
        raise TableLayoutError("sort_direction must be 'asc', 'desc', or null")

    position = seed.get("position")
    if position is None:
        position = fallback_position
    try:
        position = max(0, int(position))
    except (TypeError, ValueError) as exc:
        raise TableLayoutError("column position must be an integer") from exc

    sort_priority = seed.get("sort_priority")
    if sort_priority is not None:
        try:
            sort_priority = max(0, int(sort_priority))
        except (TypeError, ValueError) as exc:
            raise TableLayoutError("sort_priority must be an integer") from exc

    min_width = _clamp_width(seed.get("min_width_px"), _MIN_WIDTH)
    max_width = _clamp_width(seed.get("max_width_px"), max(min_width, 900))
    if max_width < min_width:
        max_width = min_width
    width = _clamp_width(seed.get("width_px"), max(min_width, 160))
    width = max(min_width, min(max_width, width))

    sample_max_length = seed.get("sample_max_length")
    if sample_max_length is not None:
        try:
            sample_max_length = max(0, int(sample_max_length))
        except (TypeError, ValueError) as exc:
            raise TableLayoutError("sample_max_length must be an integer") from exc

    return {
        "column_key": _sanitize_column_key(column_key),
        "display_name": display_name,
        "sqlite_column": str(sqlite_column).strip() if sqlite_column else None,
        "width_px": width,
        "min_width_px": min_width,
        "max_width_px": max_width,
        "position": position,
        "sort_direction": sort_direction,
        "sort_priority": sort_priority,
        "hidden": bool(seed.get("hidden", False)),
        "data_type": str(seed.get("data_type") or "").strip() or None,
        "sample_max_length": sample_max_length,
    }


def normalize_layout_data(layout_data: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(layout_data, dict):
        raise TableLayoutError("layout_data must be a JSON object")

    columns_raw = layout_data.get("columns")
    if not isinstance(columns_raw, list) or not columns_raw:
        raise TableLayoutError("layout_data.columns must contain at least one column")

    columns = [normalize_column_seed(col, idx) for idx, col in enumerate(columns_raw)]
    normalized = {
        "version": int(layout_data.get("version") or 1),
        "columns": columns,
    }

    if "seed_origin" in layout_data:
        normalized["seed_origin"] = str(layout_data.get("seed_origin") or "manual")
    if "algorithm_version" in layout_data:
        normalized["algorithm_version"] = str(layout_data.get("algorithm_version") or "v1")
    if "bucket_flags" in layout_data and isinstance(layout_data["bucket_flags"], dict):
        normalized["bucket_flags"] = {
            key: bool(layout_data["bucket_flags"].get(key, False))
            for key in ("shade_up", "horizontal_scroll", "mobile", "portrait", "wide")
        }
    return normalized


def choose_sibling_row(conn, reserved_code: str, user_code: str, table_code: str, bucket_code: str):
    target = int(bucket_code, 16)
    candidate_sets = [
        (reserved_code, user_code, table_code),
    ]
    if user_code != "00":
        candidate_sets.append((reserved_code, "00", table_code))

    best_row = None
    best_rank = None
    for reserved, user, table in candidate_sets:
        rows = conn.execute(
            """
            SELECT * FROM table_layouts
            WHERE reserved_code=? AND user_code=? AND table_code=? AND bucket_code != ?
            ORDER BY updated_at DESC, created_at DESC
            """,
            (reserved, user, table, bucket_code),
        ).fetchall()
        for row in rows:
            row_bucket = int(row["bucket_code"], 16)
            rank = ((row_bucket ^ target).bit_count(), abs(row_bucket - target))
            if best_rank is None or rank < best_rank:
                best_rank = rank
                best_row = row
        if best_row is not None:
            break
    return best_row


def _estimate_width(column: dict[str, Any], flags: dict[str, bool]) -> int:
    label_len = len(column["display_name"])
    sample_len = min(column.get("sample_max_length") or 0, 72)
    data_type = (column.get("data_type") or "").lower()

    width = 96 + (label_len * 8)
    if any(token in data_type for token in ("bool", "flag")):
        width = max(width, 84)
    elif any(token in data_type for token in ("int", "real", "numeric", "count")):
        width = max(width, 104)
    elif any(token in data_type for token in ("date", "time")):
        width = max(width, 152)
    elif any(token in data_type for token in ("json", "text", "notes", "description")):
        width = max(width, 220)
    else:
        width = max(width, 110 + (sample_len * 6))

    if flags.get("mobile"):
        width = int(width * 0.82)
    if flags.get("portrait"):
        width = int(width * 0.94)
    if flags.get("horizontal_scroll"):
        width = int(width * 1.08)
    if flags.get("wide"):
        width = int(width * 1.12)

    width = max(column["min_width_px"], min(column["max_width_px"], width))
    return _clamp_width(width, width)


def _target_total_width(flags: dict[str, bool]) -> int:
    if flags.get("mobile"):
        base = 420 if flags.get("portrait") else 760
    else:
        base = 1180 if flags.get("portrait") else 1480
    if flags.get("wide"):
        base = int(base * 1.25)
    if flags.get("horizontal_scroll"):
        base = int(base * 1.18)
    return base


def _shrink_columns_to_target(columns: list[dict[str, Any]], target_total: int) -> None:
    total = sum(col["width_px"] for col in columns)
    if total <= target_total:
        return

    adjustable = [col for col in columns if col["width_px"] > col["min_width_px"]]
    while total > target_total and adjustable:
        excess = total - target_total
        reducible = sum(col["width_px"] - col["min_width_px"] for col in adjustable)
        if reducible <= 0:
            break
        for col in list(adjustable):
            current = col["width_px"]
            spare = current - col["min_width_px"]
            if spare <= 0:
                adjustable.remove(col)
                continue
            cut = max(1, round(excess * (spare / reducible)))
            next_width = max(col["min_width_px"], current - cut)
            total -= current - next_width
            col["width_px"] = next_width
        adjustable = [col for col in columns if col["width_px"] > col["min_width_px"]]


def build_fallback_layout(columns: list[dict[str, Any]], bucket_code: str) -> dict[str, Any]:
    if not columns:
        raise TableLayoutError("cannot build fallback layout without requested column metadata")

    flags = decode_bucket_code(bucket_code)
    resolved = [normalize_column_seed(column, idx) for idx, column in enumerate(columns)]
    for column in resolved:
        column["width_px"] = _estimate_width(column, flags)

    if not flags["horizontal_scroll"]:
        _shrink_columns_to_target(resolved, _target_total_width(flags))

    return {
        "version": 1,
        "seed_origin": "fallback",
        "algorithm_version": "v1",
        "bucket_flags": flags,
        "columns": resolved,
    }


def seed_from_sibling(sibling_layout: dict[str, Any], requested_columns: list[dict[str, Any]], bucket_code: str) -> dict[str, Any]:
    sibling_columns = sibling_layout.get("columns") if isinstance(sibling_layout, dict) else None
    if not isinstance(sibling_columns, list) or not sibling_columns:
        return build_fallback_layout(requested_columns, bucket_code)

    if not requested_columns:
        seeded = normalize_layout_data(copy.deepcopy(sibling_layout))
        seeded["seed_origin"] = "sibling"
        seeded["bucket_flags"] = decode_bucket_code(bucket_code)
        return seeded

    normalized_requested = [normalize_column_seed(column, idx) for idx, column in enumerate(requested_columns)]
    sibling_map: dict[str, dict[str, Any]] = {}
    for entry in sibling_columns:
        normalized = normalize_column_seed(entry, entry.get("position", 0))
        sibling_map[normalized["column_key"]] = normalized
        if normalized.get("sqlite_column"):
            sibling_map[normalized["sqlite_column"]] = normalized

    merged: list[dict[str, Any]] = []
    fallback = build_fallback_layout(requested_columns, bucket_code)
    fallback_map = {col["column_key"]: col for col in fallback["columns"]}
    for idx, column in enumerate(normalized_requested):
        match = sibling_map.get(column["column_key"]) or sibling_map.get(column.get("sqlite_column") or "")
        base = copy.deepcopy(match) if match else copy.deepcopy(fallback_map[column["column_key"]])
        base["display_name"] = column["display_name"]
        base["sqlite_column"] = column.get("sqlite_column")
        base["position"] = idx
        base["hidden"] = column.get("hidden", base.get("hidden", False))
        base["min_width_px"] = column["min_width_px"]
        base["max_width_px"] = column["max_width_px"]
        base["width_px"] = max(base["min_width_px"], min(base["max_width_px"], base["width_px"]))
        merged.append(base)

    return {
        "version": int(sibling_layout.get("version") or 1),
        "seed_origin": "sibling",
        "algorithm_version": str(sibling_layout.get("algorithm_version") or "v1"),
        "bucket_flags": decode_bucket_code(bucket_code),
        "columns": merged,
    }