"""routes_events.py — Blueprints push-notification event stream.

Routes
------
GET  /api/v1/events/stream   long-lived SSE stream  (use ``EventSource``)
GET  /api/v1/events/recent   JSON history for client bootstrap
POST /api/v1/events          insert an event (loopback-only; no extra auth needed)

The ``events`` table is intentionally node-local and excluded from fleet sync.
Each node publishes its own events (LiteLLM model changes, probe outcomes,
backup completions, etc.).

SSE authentication
------------------
``EventSource`` cannot set custom request headers, so the TOTP token must be
sent via the ``token`` query parameter.  The auth middleware accepts this
parameter as a fallback for any route where the ``X-API-Token`` header is
absent.

Reconnect / catch-up
--------------------
The client should send ``Last-Event-ID`` on reconnect.  The generator will
replay all events persisted after that event's timestamp before streaming
live events, preventing gaps during brief disconnects.

Backpressure
------------
Each subscriber gets a bounded asyncio.Queue (128 items).  If a slow client
fills its queue, the current event is dropped *for that client only*.  On
reconnect with a valid ``Last-Event-ID``, the client will catch up from the
persisted history.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator

from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .db import get_conn
from .events import AppEvent, bus

log = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])

_RECENT_DEFAULT = 50
_RECENT_MAX = 200
_EVENT_TTL_SECS = 7 * 24 * 3600  # prune events older than 7 days
_KEEPALIVE_SECS = 15.0  # SSE heartbeat comment interval


# ── Pydantic models ───────────────────────────────────────────────────────────


class EventIn(BaseModel):
    event_type: str
    severity: str = "info"
    title: str
    message: str
    source: str = "blueprints-app"
    payload: dict[str, Any] | None = None
    event_id: str | None = None


class EventOut(BaseModel):
    event_id: str
    event_type: str
    severity: str
    title: str
    message: str
    source: str
    created_at: float
    payload: dict[str, Any]


# ── DB helpers ────────────────────────────────────────────────────────────────


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
        conn.execute(
            "DELETE FROM events WHERE created_at < ?",
            (time.time() - _EVENT_TTL_SECS,),
        )


def _ts_for_event_id(event_id: str) -> float | None:
    """Return created_at for a known event_id, or None if not found."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT created_at FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
    return float(row["created_at"]) if row else None


def _load_events(
    limit: int = _RECENT_DEFAULT,
    after_ts: float | None = None,
) -> list[EventOut]:
    with get_conn() as conn:
        if after_ts is not None:
            rows = conn.execute(
                "SELECT * FROM events WHERE created_at > ? ORDER BY created_at DESC LIMIT ?",
                (after_ts, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
    return [
        EventOut(
            event_id=r["event_id"],
            event_type=r["event_type"],
            severity=r["severity"],
            title=r["title"],
            message=r["message"],
            source=r["source"],
            created_at=float(r["created_at"]),
            payload=json.loads(r["payload_json"] or "{}"),
        )
        for r in rows
    ]


# ── SSE stream generator ──────────────────────────────────────────────────────


async def _sse_generator(
    request: Request,
    last_event_id: str | None,
) -> AsyncIterator[str]:
    """Async generator for the SSE response body.

    1. Replays persisted events that arrived after ``last_event_id`` so a
       reconnecting client closes any gap without duplicating already-seen
       events.
    2. Forwards live events from the bus queue as they arrive.
    3. Emits a comment keepalive every ``_KEEPALIVE_SECS`` of silence to
       prevent intermediaries (Caddy, Tailscale relay, mobile TCP stacks)
       from closing idle connections.
    4. Terminates cleanly when the client disconnects (``is_disconnected()``)
       or a shutdown sentinel (``None``) arrives on the queue.
    """
    sub_id, q = await bus.subscribe()
    try:
        # ── Catch-up replay ───────────────────────────────────────────────────
        if last_event_id:
            after_ts = _ts_for_event_id(last_event_id)
            if after_ts is not None:
                replays = _load_events(_RECENT_MAX, after_ts=after_ts)
                for ev in reversed(replays):  # chronological order for the client
                    yield ev.to_sse()

        # ── Live event loop ───────────────────────────────────────────────────
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(q.get(), timeout=_KEEPALIVE_SECS)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            if event is None:
                # Shutdown sentinel from bus.close_all() during app shutdown
                break
            yield event.to_sse()
    finally:
        await bus.unsubscribe(sub_id)


# ── Routes ────────────────────────────────────────────────────────────────────


@router.get("/stream")
async def sse_stream(request: Request) -> StreamingResponse:
    """Long-lived SSE stream.

    Clients should pass the TOTP token via the ``token`` query parameter
    because ``EventSource`` cannot set custom headers.  The ``Last-Event-ID``
    header is honoured for seamless reconnect catch-up.
    """
    last_event_id = request.headers.get("last-event-id")
    return StreamingResponse(
        _sse_generator(request, last_event_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Accel-Buffering": "no",  # disable Nginx/Caddy response buffering
            "Connection": "keep-alive",
        },
    )


@router.get("/recent", response_model=list[EventOut])
async def get_recent_events(
    limit: int = Query(default=_RECENT_DEFAULT, ge=1, le=_RECENT_MAX),
    since_id: str | None = Query(default=None),
) -> list[EventOut]:
    """Return recent events (newest first) for client bootstrap.

    Pass ``since_id`` to receive only events that occurred after a known
    event id.
    """
    after_ts: float | None = None
    if since_id:
        after_ts = _ts_for_event_id(since_id)
    return _load_events(limit=limit, after_ts=after_ts)


@router.post("", response_model=EventOut, status_code=201)
async def create_event(body: EventIn) -> EventOut:
    """Insert a new event and fan out to all connected SSE subscribers.

    Intended for internal use (the LiteLLM hook-sync script POSTing from
    localhost).  Loopback callers bypass all auth middleware checks so no
    additional enforcement is needed in this handler.
    """
    event = AppEvent.create(
        event_type=body.event_type,
        title=body.title,
        message=body.message,
        severity=body.severity,
        source=body.source,
        payload=body.payload or {},
        event_id=body.event_id,
    )
    _persist(event)
    await bus.publish(event)
    return EventOut(
        event_id=event.event_id,
        event_type=event.event_type,
        severity=event.severity,
        title=event.title,
        message=event.message,
        source=event.source,
        created_at=event.created_at,
        payload=event.payload,
    )
