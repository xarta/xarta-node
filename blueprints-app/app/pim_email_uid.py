"""Folder-independent PIM email identity UID helper."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from email import policy
from email.header import decode_header, make_header
from email.message import Message
from email.parser import BytesParser, Parser
from email.utils import parsedate_to_datetime
from typing import Any

SCHEMA = "xarta.pim_email.email_uid.v1"
VERSION = 1
HASH_ALG = "sha256-160"
HASH_HEX_LENGTH = 40

IDENTITY_HEADER_ORDER = (
    "Message-ID",
    "Date",
    "From",
    "Sender",
    "Reply-To",
    "To",
    "Cc",
    "Subject",
    "In-Reply-To",
    "References",
)

HIGH_CONFIDENCE_HEADERS = {"Message-ID", "Date", "From"}

FALLBACK_EXCLUDED_HEADER_NAMES = {
    "authentication-results",
    "arc-authentication-results",
    "delivered-to",
    "received",
    "received-spf",
    "return-path",
}


def generate_email_uid_info(source: bytes | str | Message | None) -> dict[str, Any]:
    """Generate a stable header-derived email UID schema from raw headers/message bytes."""
    message = _coerce_header_message(source)
    source_fields = _identity_source_fields(message)
    date_yyyymmdd, date_source, date_warnings = _date_prefix(message)

    present_names = {field["name"] for field in source_fields if field.get("values")}
    missing_high_confidence = [
        name
        for name in IDENTITY_HEADER_ORDER
        if name in HIGH_CONFIDENCE_HEADERS and name not in present_names
    ]
    if "Date" in present_names and date_source != "date":
        missing_high_confidence.append("Date")
    warnings = list(date_warnings)
    for name in missing_high_confidence:
        suffix = "missing" if name not in present_names else "unusable"
        _append_warning(warnings, f"{_warning_name(name)}_{suffix}")

    if "Message-ID" not in present_names:
        confidence = "low"
    elif missing_high_confidence:
        confidence = "medium"
    else:
        confidence = "high"

    hash_fields = list(source_fields)
    if missing_high_confidence:
        fallback = _header_fallback_component(message)
        hash_fields.append(
            {
                "name": "header-fallback",
                "hash_alg": "sha256",
                "hash_hex": fallback["hash_hex"],
                "header_count": fallback["header_count"],
            }
        )
        _append_warning(warnings, "header_fallback_used")

    digest_payload = {
        "schema": SCHEMA,
        "version": VERSION,
        "source_fields": hash_fields,
    }
    digest = hashlib.sha256(
        json.dumps(
            digest_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    hash_hex = digest[:HASH_HEX_LENGTH]
    email_uid = f"{date_yyyymmdd}-{hash_hex}"

    return {
        "schema": SCHEMA,
        "version": VERSION,
        "email_uid": email_uid,
        "date_yyyymmdd": date_yyyymmdd,
        "date_source": date_source,
        "hash_alg": HASH_ALG,
        "hash_hex": hash_hex,
        "source_fields": hash_fields,
        "confidence": confidence,
        "warnings": warnings,
        "storage_relpath": storage_relpath_for_email_uid(email_uid, date_yyyymmdd),
    }


def storage_relpath_for_email_uid(email_uid: str, date_yyyymmdd: str) -> str:
    if re.fullmatch(r"\d{8}", date_yyyymmdd or "") and date_yyyymmdd != "00000000":
        year = date_yyyymmdd[:4]
        month = date_yyyymmdd[4:6]
        day = date_yyyymmdd[6:8]
        return f"{year}/{month}/{day}/{email_uid}.eml.enc"
    return f"undated/{email_uid}.eml.enc"


def _coerce_header_message(source: bytes | str | Message | None) -> Message:
    if isinstance(source, Message):
        return source
    if isinstance(source, str):
        return Parser(policy=policy.default).parsestr(source, headersonly=True)
    return BytesParser(policy=policy.default).parsebytes(bytes(source or b""), headersonly=True)


def _identity_source_fields(message: Message) -> list[dict[str, Any]]:
    fields: list[dict[str, Any]] = []
    for name in IDENTITY_HEADER_ORDER:
        values = [
            normalized
            for value in message.get_all(name, [])
            if (normalized := _normalize_header_value(name, value))
        ]
        if values:
            fields.append({"name": name, "values": values})
    return fields


def _date_prefix(message: Message) -> tuple[str, str, list[str]]:
    warnings: list[str] = []
    raw_dates = message.get_all("Date", [])
    for raw_date in raw_dates:
        parsed = _parse_header_datetime(raw_date)
        if parsed is not None:
            return parsed.strftime("%Y%m%d"), "date", warnings
    if raw_dates:
        warnings.append("date_unparseable")

    for received in message.get_all("Received", []):
        parsed = _parse_received_datetime(received)
        if parsed is not None:
            _append_warning(warnings, "date_from_received_fallback")
            return parsed.strftime("%Y%m%d"), "received", warnings

    _append_warning(warnings, "date_missing")
    return "00000000", "missing", warnings


def _parse_header_datetime(value: Any) -> datetime | None:
    clean = _decode_header_value(value)
    if not clean:
        return None
    try:
        parsed = parsedate_to_datetime(clean)
    except (TypeError, ValueError, IndexError, AttributeError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _parse_received_datetime(value: Any) -> datetime | None:
    clean = _collapse_whitespace(_decode_header_value(value))
    if ";" not in clean:
        return None
    return _parse_header_datetime(clean.rsplit(";", 1)[1].strip())


def _normalize_header_value(name: str, value: Any) -> str:
    if name.lower() == "date":
        parsed = _parse_header_datetime(value)
        if parsed is not None:
            return parsed.isoformat().replace("+00:00", "Z")
    return _collapse_whitespace(_decode_header_value(value))


def _decode_header_value(value: Any) -> str:
    try:
        return str(make_header(decode_header(str(value or ""))))
    except Exception:
        return str(value or "")


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\r", "\n")).strip()


def _header_fallback_component(message: Message) -> dict[str, Any]:
    headers: list[dict[str, str]] = []
    for name, value in message.raw_items():
        clean_name = str(name or "").strip().lower()
        if not clean_name or _is_fallback_excluded(clean_name):
            continue
        normalized = _normalize_header_value(clean_name, value)
        if normalized:
            headers.append({"name": clean_name, "value": normalized})
    headers.sort(key=lambda item: (item["name"], item["value"]))
    payload = json.dumps(headers, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return {
        "hash_hex": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "header_count": len(headers),
    }


def _is_fallback_excluded(name: str) -> bool:
    return name in FALLBACK_EXCLUDED_HEADER_NAMES or name.startswith("x-")


def _append_warning(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def _warning_name(header_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", header_name.lower()).strip("_")
