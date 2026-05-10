"""Local LLM event helpers for Blueprints push notifications.

These helpers are intentionally best-effort. A failed notification must never
turn a model failure into a second application failure.
"""

from __future__ import annotations

import json
import logging
import time
from urllib.parse import urlsplit, urlunsplit

from .db import get_conn
from .events import AppEvent, bus

log = logging.getLogger(__name__)

_OFFLINE_DEDUPE_SECONDS = 300.0
_last_offline_notice: dict[str, float] = {}


def _sanitize_base_url(value: str | None) -> str:
    """Return a URL safe for local event payloads and diagnostics."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
        host = parts.hostname or ""
        if parts.port:
            host = f"{host}:{parts.port}"
        return urlunsplit((parts.scheme, host, parts.path.rstrip("/"), "", ""))
    except Exception:
        return raw.split("?", 1)[0]


def _looks_like_local_primary(model: str | None) -> bool:
    alias = str(model or "").strip().upper()
    return alias.startswith("PRIMARY-LOCAL")


def _looks_like_offline_failure(status_code: int | None, detail: str | None) -> bool:
    if status_code is None:
        return True
    if status_code >= 500:
        return True
    text = str(detail or "").lower()
    return any(
        marker in text
        for marker in (
            "cannot connect",
            "connect call failed",
            "connection refused",
            "connection reset",
            "hosted_vllmexception",
            "endpoint unavailable",
            "timed out",
            "timeout",
        )
    )


def _persist(event: AppEvent) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO events
              (event_id, event_type, severity, title, message, source, created_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.event_type,
                event.severity,
                event.title,
                event.message,
                event.source,
                event.created_at,
                json.dumps(event.payload),
            ),
        )


async def publish_local_llm_offline_event(
    *,
    operation: str,
    model: str | None,
    base_url: str | None,
    status_code: int | None = None,
    detail: str | None = None,
) -> None:
    """Publish a deduped local-LLM-offline event for SSE/TTS subscribers.

    Only PRIMARY-LOCAL aliases are treated as the main LLM for this warning.
    Embedding/reranker/TTS failures are intentionally not folded into this
    operator alert.
    """
    if not _looks_like_local_primary(model):
        return
    if not _looks_like_offline_failure(status_code, detail):
        return

    safe_base = _sanitize_base_url(base_url)
    dedupe_key = f"{operation}|{model}|{safe_base}"
    now = time.monotonic()
    last = _last_offline_notice.get(dedupe_key, 0.0)
    if now - last < _OFFLINE_DEDUPE_SECONDS:
        return
    _last_offline_notice[dedupe_key] = now

    event = AppEvent.create(
        event_type="local.llm.offline",
        severity="error",
        title="Local LLM Offline",
        message="Local Large Language Model is offline.",
        source="blueprints-local-llm",
        payload={
            "operation": operation,
            "model": model or "",
            "base_url": safe_base,
            "status_code": status_code,
            "detail": str(detail or "")[:1000],
            "dedupe_seconds": _OFFLINE_DEDUPE_SECONDS,
        },
    )
    try:
        _persist(event)
        await bus.publish(event)
    except Exception as exc:  # noqa: BLE001
        log.warning("local LLM offline event publish failed: %s", exc)
