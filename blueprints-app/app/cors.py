"""
cors.py — Dynamic CORS middleware for Blueprints.

Allowed origins = static seeds from BLUEPRINTS_CORS_ORIGINS (in .env)
                + ui_url of every registered peer node in the DB.

The DB-derived set is re-queried at most once per CACHE_TTL seconds, so a
newly-registered peer node becomes an allowed origin within that window
without requiring a restart.

Why not allow_origins=["*"]?
  - With *, malware served from your LAN could — when visited by your browser
    while you're on the tailnet — silently read your full service inventory,
    node addresses, and sync state via the Blueprints API.
  - Restricting to known origins means the browser blocks such reads; the
    server still receives the request but the browser withholds the response
    from the attacking page.
  - Preflight OPTIONS requests for POST/PATCH also fail, preventing writes
    from unlisted origins.
"""

import json
import logging
import time
from typing import Iterable

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from . import config as cfg
from . import db

log = logging.getLogger(__name__)

# Re-query the DB for node ui_urls this often (seconds).
CACHE_TTL = 60

# Methods the component actually needs.
ALLOWED_METHODS = "GET, POST, OPTIONS"

# Headers the component sends.
ALLOWED_HEADERS = "Content-Type"


class DynamicCORSMiddleware(BaseHTTPMiddleware):
    """
    CORS middleware that merges a static seed list with live node ui_urls from
    the DB.  The allowed set is refreshed every CACHE_TTL seconds.
    """

    def __init__(self, app):
        super().__init__(app)
        # Static seeds from config — never expire.
        self._seeds: frozenset[str] = frozenset(cfg.CORS_ORIGINS)
        # DB-derived origins, rebuilt periodically.
        self._db_origins: frozenset[str] = frozenset()
        self._last_refresh: float = 0.0

        if self._seeds:
            log.info("CORS seed origins (%d): %s", len(self._seeds), sorted(self._seeds))
        else:
            log.warning(
                "CORS: BLUEPRINTS_CORS_ORIGINS is empty — "
                "only peer node ui_urls from the DB will be allowed. "
                "Set BLUEPRINTS_CORS_ORIGINS in .env to allow embed pages."
            )

    # ── Origin resolution ──────────────────────────────────────────────────

    def _refresh_db_origins(self) -> None:
        """Pull ui_url (and first address) for every registered node."""
        now = time.monotonic()
        if now - self._last_refresh < CACHE_TTL:
            return
        try:
            origins: set[str] = set()
            with db.get_conn() as conn:
                rows = conn.execute("SELECT ui_url, addresses FROM nodes").fetchall()
            for ui_url, addresses_json in rows:
                if ui_url:
                    origins.add(ui_url.rstrip("/"))
                # addresses is a JSON array of sync URLs — include them too so
                # a node can call its own API via the sync address.
                try:
                    for addr in json.loads(addresses_json or "[]"):
                        if addr:
                            origins.add(addr.rstrip("/"))
                except (json.JSONDecodeError, TypeError):
                    pass
            self._db_origins = frozenset(origins)
            self._last_refresh = now
        except Exception:
            log.exception("CORS: failed to refresh node origins from DB")

    def _allowed_origins(self) -> frozenset[str]:
        self._refresh_db_origins()
        return self._seeds | self._db_origins

    def _is_allowed(self, origin: str) -> bool:
        if not origin or origin == "null":
            return False
        return origin.rstrip("/") in self._allowed_origins()

    # ── Middleware ─────────────────────────────────────────────────────────

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        origin = request.headers.get("origin", "")

        # No Origin header — not a cross-origin request, pass straight through.
        if not origin:
            return await call_next(request)

        allowed = self._is_allowed(origin)

        # Preflight OPTIONS — respond immediately; never forward to the app.
        if request.method == "OPTIONS":
            if allowed:
                return Response(
                    status_code=204,
                    headers=_cors_headers(origin),
                )
            # Origin not allowed — return without CORS headers; browser blocks.
            return Response(status_code=204)

        # Regular cross-origin request — let the app handle it, then annotate.
        response = await call_next(request)
        if allowed:
            for k, v in _cors_headers(origin).items():
                response.headers[k] = v
        return response


# ── Helpers ────────────────────────────────────────────────────────────────

def _cors_headers(origin: str) -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin":  origin,
        "Access-Control-Allow-Methods": ALLOWED_METHODS,
        "Access-Control-Allow-Headers": ALLOWED_HEADERS,
        "Access-Control-Max-Age":       "600",
        "Vary":                         "Origin",
    }
