"""Deterministic auto-layout heuristics for shared table layout buckets."""

from __future__ import annotations

import math
import re
import time
from typing import Any

from .table_layouts import (
    TableLayoutError,
    decode_bucket_code,
    normalize_column_seed,
    validate_bucket_code,
)

_AUTO_VERSION = "auto-horizontal-v1"
_LABEL_CHAR_PX = 6.6
_DATA_CHAR_PX = 6.6
_MONO_CHAR_PX = 8.1
_HEADER_RESERVE_PX = 42
_CELL_RESERVE_PX = 24


def _clamp(value: float, low: int, high: int) -> int:
    return max(low, min(high, int(round(value))))


def _norm_text(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())


def _title_from_key(key: str) -> str:
    return " ".join(part.capitalize() for part in _norm_text(key).split("_") if part) or key


def _tokens(column: dict[str, Any]) -> set[str]:
    text = _norm_text(
        " ".join(
            str(column.get(name) or "") for name in ("column_key", "display_name", "sqlite_column")
        )
    )
    return {token for token in text.split("_") if token}


def classify_column(column: dict[str, Any], table_name: str | None = None) -> str:
    key = _norm_text(column.get("column_key") or column.get("sqlite_column"))
    token_set = _tokens(column)
    table = _norm_text(table_name)
    data_type = _norm_text(column.get("data_type"))

    if key.startswith("_") or "action" in token_set or "edit" in token_set:
        return "actions"
    if token_set & {"status", "state", "health", "pending", "obsolete"}:
        return "status"
    if token_set & {
        "created",
        "updated",
        "scanned",
        "visited",
        "detected",
        "probed",
        "timestamp",
        "time",
        "date",
    } or data_type in {"date", "datetime", "timestamp"}:
        return "timestamp"
    if token_set & {
        "url",
        "uri",
        "domain",
        "domains",
        "address",
        "addresses",
        "ip",
        "mac",
        "cidr",
        "tailnet",
    }:
        return "address"
    if (
        token_set & {"json", "metadata", "networks", "upstreams", "payload", "blob"}
        or data_type == "json"
    ):
        return "json"
    if token_set & {"path", "file", "filename", "caddyfile", "asset", "sound"}:
        return "path"
    if token_set & {"icon", "glyph", "present", "enabled", "active", "ok"}:
        return "indicator"
    if token_set & {"description", "notes", "note", "detail", "details", "text"}:
        return "long_text"
    if token_set & {
        "id",
        "vmid",
        "guid",
        "uuid",
        "hash",
        "sha",
        "key",
        "env",
        "var",
        "token",
        "commit",
    }:
        return "code"
    if token_set & {
        "count",
        "size",
        "bytes",
        "priority",
        "order",
        "score",
        "dimensions",
        "port",
        "gen",
    }:
        return "metric"
    if token_set & {
        "host",
        "hostname",
        "hostnames",
        "parent",
        "source",
        "group",
        "project",
        "role",
        "type",
        "kind",
    }:
        return "relationship"
    if token_set & {"name", "label", "title", "display", "stack", "service", "model"}:
        return "primary"
    if table and any(part in key for part in table.split("_") if part):
        return "primary"
    return "medium"


def _role_bounds(
    role: str, flags: dict[str, bool], viewport: dict[str, int], column_count: int
) -> tuple[int, int]:
    mobile = flags.get("mobile")
    portrait = flags.get("portrait")
    width = viewport.get("available_table_width_px") or viewport.get("width_px") or 390
    phone_column_cap = max(132, int(width * (0.62 if column_count >= 8 else 0.66)))

    desktop_bounds = {
        "actions": (72, 136),
        "indicator": (64, 118),
        "status": (84, 150),
        "timestamp": (132, 190),
        "address": (148, 380),
        "json": (176, 620),
        "path": (158, 520),
        "long_text": (152, 480),
        "code": (116, 330),
        "metric": (78, 142),
        "relationship": (112, 260),
        "primary": (130, 360),
        "medium": (106, 240),
    }
    mobile_bounds = {
        "actions": (64, 112),
        "indicator": (56, 96),
        "status": (72, 124),
        "timestamp": (116, 166),
        "address": (132, min(phone_column_cap, 310)),
        "json": (148, min(phone_column_cap, 340)),
        "path": (136, min(phone_column_cap, 320)),
        "long_text": (130, min(phone_column_cap, 300)),
        "code": (104, min(phone_column_cap, 250)),
        "metric": (70, 124),
        "relationship": (100, min(phone_column_cap, 220)),
        "primary": (122, min(phone_column_cap, 260)),
        "medium": (96, min(phone_column_cap, 210)),
    }

    low, high = (mobile_bounds if mobile else desktop_bounds).get(role, (96, 240))
    if mobile and portrait and role not in {"actions", "indicator", "metric"}:
        high = min(high, phone_column_cap)
    if flags.get("wide") and not mobile:
        high = int(high * 1.16)
    return low, max(low, high)


def _density_factor(flags: dict[str, bool], column_count: int) -> float:
    factor = 1.0
    if column_count >= 14:
        factor *= 0.78 if flags.get("mobile") else 0.86
    elif column_count >= 10:
        factor *= 0.84 if flags.get("mobile") else 0.92
    elif column_count <= 4:
        factor *= 1.08

    if flags.get("mobile") and flags.get("portrait"):
        factor *= 0.9
    elif flags.get("mobile"):
        factor *= 0.96
    if flags.get("wide") and not flags.get("mobile"):
        factor *= 1.08
    return factor


def _sample_chars(column: dict[str, Any], role: str) -> int:
    sample = column.get("sample_max_length")
    try:
        sample_len = int(sample) if sample is not None else 0
    except (TypeError, ValueError):
        sample_len = 0
    label_len = len(str(column.get("display_name") or ""))

    defaults = {
        "actions": 8,
        "indicator": 6,
        "status": 12,
        "timestamp": 19,
        "address": 34,
        "json": 58,
        "path": 52,
        "long_text": 48,
        "code": 34,
        "metric": 10,
        "relationship": 24,
        "primary": 28,
        "medium": 22,
    }
    caps = {
        "json": 88,
        "actions": 4,
        "path": 76,
        "long_text": 72,
        "address": 64,
        "code": 52,
        "metric": 14,
        "status": 18,
        "indicator": 8,
        "relationship": 34,
        "primary": 40,
        "medium": 32,
    }
    if sample_len > 0:
        useful = max(sample_len, label_len)
    else:
        useful = max(defaults.get(role, 22), label_len)
    return min(useful, caps.get(role, 44))


def _header_width(column: dict[str, Any], role: str) -> int:
    if role in {"actions", "indicator"}:
        return _role_base_width(role)
    label = str(column.get("display_name") or column.get("column_key") or "")
    return int(math.ceil((len(label) * _LABEL_CHAR_PX) + _HEADER_RESERVE_PX))


def _role_base_width(role: str) -> int:
    return {
        "actions": 92,
        "indicator": 76,
        "status": 108,
        "timestamp": 152,
        "address": 184,
        "json": 220,
        "path": 196,
        "long_text": 188,
        "code": 154,
        "metric": 96,
        "relationship": 148,
        "primary": 172,
        "medium": 138,
    }.get(role, 138)


def _data_width(column: dict[str, Any], role: str, flags: dict[str, bool]) -> int:
    chars = _sample_chars(column, role)
    char_px = _MONO_CHAR_PX if role in {"code", "path", "json", "address"} else _DATA_CHAR_PX

    if flags.get("mobile") and flags.get("portrait"):
        desired_chars = {
            "json": 30,
            "path": 28,
            "long_text": 28,
            "address": 28,
            "code": 22,
        }.get(role, chars)
        chars = min(chars, desired_chars)
    elif flags.get("mobile"):
        chars = min(chars, 42 if role in {"json", "path", "long_text", "address"} else chars)
    elif role in {"json", "path", "long_text", "address"}:
        chars = min(chars, 68)

    return int(math.ceil((chars * char_px) + _CELL_RESERVE_PX))


def _estimated_lines(column: dict[str, Any], role: str, width_px: int) -> int:
    chars = _sample_chars(column, role)
    char_px = _MONO_CHAR_PX if role in {"code", "path", "json", "address"} else _DATA_CHAR_PX
    usable = max(1, width_px - _CELL_RESERVE_PX)
    chars_per_line = max(4, int(usable / char_px))
    return max(1, int(math.ceil(chars / chars_per_line)))


def _sort_default_column(columns: list[dict[str, Any]], table_name: str | None) -> str | None:
    lowered_table = _norm_text(table_name)
    if any(token in lowered_table for token in ("visit", "backup", "history", "search")):
        for column in columns:
            if classify_column(column, table_name) == "timestamp":
                return column["column_key"]
    for role in ("primary", "address", "relationship", "code"):
        for column in columns:
            if classify_column(column, table_name) == role:
                return column["column_key"]
    return columns[0]["column_key"] if columns else None


def default_viewport_for_flags(flags: dict[str, bool]) -> dict[str, int]:
    if flags.get("mobile"):
        if flags.get("portrait"):
            return {"width_px": 360, "height_px": 800, "available_table_width_px": 360}
        return {"width_px": 800, "height_px": 360, "available_table_width_px": 800}
    if flags.get("wide"):
        return {"width_px": 1920, "height_px": 1080, "available_table_width_px": 1840}
    if flags.get("portrait"):
        return {"width_px": 900, "height_px": 1280, "available_table_width_px": 860}
    return {"width_px": 1366, "height_px": 768, "available_table_width_px": 1280}


def _normalize_viewport(viewport: dict[str, Any] | None, flags: dict[str, bool]) -> dict[str, int]:
    fallback = default_viewport_for_flags(flags)
    raw = viewport or {}
    result = {}
    for key, fallback_value in fallback.items():
        try:
            value = int(raw.get(key) or fallback_value)
        except (TypeError, ValueError):
            value = fallback_value
        result[key] = max(1, value)
    return result


def build_auto_layout(
    columns: list[dict[str, Any]],
    bucket_code: str,
    *,
    table_name: str | None = None,
    viewport: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not columns:
        raise TableLayoutError("cannot build auto-layout without requested column metadata")

    started = time.perf_counter()
    bucket = validate_bucket_code(bucket_code)
    flags = decode_bucket_code(bucket)
    normalized_viewport = _normalize_viewport(viewport, flags)
    resolved = [normalize_column_seed(column, idx) for idx, column in enumerate(columns)]
    column_count = len(resolved)
    density = _density_factor(flags, column_count)
    reason_codes: list[str] = []

    if flags.get("horizontal_scroll"):
        reason_codes.append("horizontal_scroll_all_columns")
    if flags.get("mobile") and flags.get("portrait"):
        reason_codes.append("mobile_portrait_compact")
    if column_count >= 10:
        reason_codes.append("many_columns_density")
    elif column_count <= 4:
        reason_codes.append("few_columns_breathing_room")

    role_by_key: dict[str, str] = {}
    line_depths: list[int] = []
    for column in resolved:
        role = classify_column(column, table_name)
        role_by_key[column["column_key"]] = role
        min_bound, max_bound = _role_bounds(role, flags, normalized_viewport, column_count)
        min_bound = max(column["min_width_px"], min_bound)
        max_bound = min(column["max_width_px"], max(max_bound, min_bound))

        natural_parts = [_header_width(column, role), _data_width(column, role, flags)]
        if not flags.get("horizontal_scroll"):
            natural_parts.append(_role_base_width(role))
        natural = max(natural_parts)
        width = _clamp(natural * density, min_bound, max_bound)
        if flags.get("horizontal_scroll") and flags.get("mobile") and flags.get("portrait"):
            readable_floor = {
                "address": 176,
                "json": 188,
                "path": 180,
                "long_text": 188,
                "primary": 164,
                "medium": 150,
            }.get(role)
            if readable_floor and _sample_chars(column, role) >= 34:
                width = max(width, min(max_bound, readable_floor))
        if _estimated_lines(column, role, width) > (5 if flags.get("mobile") else 3):
            width = _clamp(width * 1.12, min_bound, max_bound)
        column["width_px"] = width
        column["hidden"] = False if flags.get("horizontal_scroll") else bool(column.get("hidden"))
        column["position"] = int(column.get("position") or 0)
        column["sort_direction"] = None
        column["sort_priority"] = None
        line_depths.append(_estimated_lines(column, role, width))

    if flags.get("horizontal_scroll"):
        all_columns_visible = all(not column["hidden"] for column in resolved)
        if all_columns_visible:
            reason_codes.append("visible_all_columns")

    available_width = normalized_viewport["available_table_width_px"]
    total_width = sum(column["width_px"] for column in resolved)
    can_fill_slack = (
        not flags.get("horizontal_scroll")
        or column_count <= 4
        or total_width < int(available_width * 0.72)
    )
    if total_width < available_width and resolved and can_fill_slack:
        elastic_roles = {
            "primary",
            "long_text",
            "address",
            "json",
            "path",
            "relationship",
            "medium",
        }
        elastic = [
            column for column in resolved if role_by_key.get(column["column_key"]) in elastic_roles
        ]
        if not elastic:
            elastic = resolved
        slack = available_width - total_width
        for column in elastic:
            if slack <= 0:
                break
            role = role_by_key.get(column["column_key"], "medium")
            _, max_bound = _role_bounds(role, flags, normalized_viewport, column_count)
            max_bound = min(column["max_width_px"], max_bound)
            add = min(slack, max(0, max_bound - column["width_px"]))
            if add <= 0:
                continue
            column["width_px"] += add
            slack -= add
        if slack < available_width - total_width:
            reason_codes.append("filled_available_width")

    default_sort = _sort_default_column(resolved, table_name)
    if default_sort:
        for column in resolved:
            if column["column_key"] != default_sort:
                continue
            role = role_by_key.get(column["column_key"], "medium")
            column["sort_direction"] = "desc" if role == "timestamp" else "asc"
            column["sort_priority"] = 0
            reason_codes.append("default_sort_applied")
            break

    planner = {
        "algorithm_version": _AUTO_VERSION,
        "elapsed_ms": int(round((time.perf_counter() - started) * 1000)),
        "target_width_px": available_width,
        "total_width_px": sum(column["width_px"] for column in resolved),
        "visible_count": sum(1 for column in resolved if not column["hidden"]),
        "hidden_count": sum(1 for column in resolved if column["hidden"]),
        "max_estimated_cell_lines": max(line_depths) if line_depths else 0,
        "density_factor": round(density, 3),
        "reason_codes": reason_codes,
    }

    layout_data = {
        "version": 1,
        "seed_origin": "auto-layout",
        "algorithm_version": _AUTO_VERSION,
        "bucket_flags": flags,
        "columns": resolved,
    }
    return layout_data, planner


def display_name_from_key(key: str) -> str:
    return _title_from_key(key)
