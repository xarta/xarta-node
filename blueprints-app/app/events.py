"""events.py — In-process async fan-out event bus for the Blueprints SSE stream.

Usage::

    from .events import bus, AppEvent

    async def my_route():
        event = AppEvent.create("model.changed", "Model Changed", "Primary → Gemma3")
        await bus.publish(event)

The bus is a module-level singleton. SSE clients subscribe via ``bus.subscribe()``
and consume from the returned asyncio.Queue.

Each subscriber queue is bounded (``_QUEUE_MAX``). If a slow consumer fills its
queue an event is silently dropped for that client; the client catches up from
``GET /api/v1/events/recent`` after reconnecting with a ``Last-Event-ID`` header.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

_QUEUE_MAX = 128


# ── Event data model ──────────────────────────────────────────────────────────


@dataclass
class AppEvent:
    event_id: str
    event_type: str  # e.g. "model.changed", "alias.tests.completed"
    severity: str  # "info" | "warn" | "error"
    title: str
    message: str
    source: str  # e.g. "litellm-hook-sync", "blueprints-app"
    created_at: float  # unix epoch
    payload: dict[str, Any] = field(default_factory=dict)

    def to_sse(self) -> str:
        """Render as SSE wire format.

        We intentionally omit the ``event:`` line so all messages arrive on
        the EventSource ``onmessage`` handler — avoiding the need for the
        client to register per-type listeners in advance.  The event_type is
        carried inside the JSON data payload.
        """
        data = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "severity": self.severity,
            "title": self.title,
            "message": self.message,
            "source": self.source,
            "created_at": self.created_at,
            "payload": self.payload,
        }
        return f"id:{self.event_id}\ndata:{json.dumps(data)}\n\n"

    @classmethod
    def create(
        cls,
        event_type: str,
        title: str,
        message: str,
        *,
        severity: str = "info",
        source: str = "blueprints-app",
        payload: dict[str, Any] | None = None,
        event_id: str | None = None,
    ) -> "AppEvent":
        return cls(
            event_id=event_id or uuid.uuid4().hex,
            event_type=event_type,
            severity=severity,
            title=title,
            message=message,
            source=source,
            created_at=time.time(),
            payload=payload or {},
        )


# ── Event bus ────────────────────────────────────────────────────────────────


class EventBus:
    """Async fan-out event bus for SSE subscribers.

    Thread safety: all methods that touch ``_queues`` acquire ``_lock``.
    The lock is an asyncio.Lock and must be awaited inside a running event loop.

    The lock is created lazily on first use to avoid attaching it to the wrong
    event loop when the module is imported at startup before uvicorn's loop
    is running.
    """

    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None
        self._queues: dict[str, asyncio.Queue[AppEvent | None]] = {}

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def subscribe(self) -> tuple[str, asyncio.Queue[AppEvent | None]]:
        """Register a new SSE client.  Returns (sub_id, queue)."""
        async with self._get_lock():
            sub_id = uuid.uuid4().hex
            q: asyncio.Queue[AppEvent | None] = asyncio.Queue(maxsize=_QUEUE_MAX)
            self._queues[sub_id] = q
            log.debug("events: client %s subscribed (total=%d)", sub_id, len(self._queues))
            return sub_id, q

    async def unsubscribe(self, sub_id: str) -> None:
        """Deregister a subscriber.  Safe to call if already gone."""
        async with self._get_lock():
            self._queues.pop(sub_id, None)
            log.debug("events: client %s unsubscribed (total=%d)", sub_id, len(self._queues))

    async def publish(self, event: AppEvent) -> None:
        """Fan out an event to all connected subscribers.

        The lock is held only briefly to snapshot the queue reference list;
        actual puts happen outside the lock so slow clients don't block
        the publisher.  Full queues are silently skipped — those clients catch
        up on the next reconnect via ``Last-Event-ID`` history replay.
        """
        async with self._get_lock():
            queues = list(self._queues.values())
        dropped = 0
        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dropped += 1
        if dropped:
            log.warning(
                "events: event %r dropped for %d slow SSE client(s)",
                event.event_id,
                dropped,
            )

    async def close_all(self) -> None:
        """Send shutdown sentinel (None) to all subscribers and clear the registry."""
        async with self._get_lock():
            for q in self._queues.values():
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    pass
            self._queues.clear()

    @property
    def subscriber_count(self) -> int:
        return len(self._queues)


# ── Module-level singleton ───────────────────────────────────────────────────

bus = EventBus()
