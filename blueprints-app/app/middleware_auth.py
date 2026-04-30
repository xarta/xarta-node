"""middleware_auth.py — IP allowlist + TOTP token authentication middleware.

Two protection layers applied in order to every inbound request:

1. IP allowlist — client IP must fall within BLUEPRINTS_ALLOWED_NETWORKS
   (comma-separated CIDRs).  Caddy sets X-Forwarded-For; we read that first,
   then fall back to the raw connection IP.

2. TOTP token — X-API-Token header must match the current (±1 window)
   HMAC-SHA256 token derived from the appropriate secret:
     • /api/v1/sync/* routes  →  BLUEPRINTS_SYNC_SECRET
     • all other routes        →  BLUEPRINTS_API_SECRET

Exempt paths (no token required): /health, anything under /ui, and narrow
read/search endpoints intended for local AI agents. The IP allowlist still
applies to token-exempt non-loopback requests.

If the relevant secret is empty (initial deploy before .env is configured),
the token check is skipped with a debug log so the app still starts.
"""

import ipaddress
import logging

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

from . import config as cfg
from .auth import verify_token

log = logging.getLogger(__name__)

# ── Parse CIDR allowlist once at import time ──────────────────────────────────
_allowed_networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
for _cidr in cfg.ALLOWED_NETWORKS_RAW.split(","):
    _cidr = _cidr.strip()
    if not _cidr:
        continue
    try:
        _allowed_networks.append(ipaddress.ip_network(_cidr, strict=False))
    except ValueError:
        log.warning("middleware_auth: ignoring invalid CIDR %r", _cidr)

# Prefixes that require NO token (IP allowlist still applies)
_TOKEN_EXEMPT_PREFIXES = ("/health", "/ui", "/favicon.ico", "/api/v1/pwa/manifest")
# Exact API paths that require NO token (IP allowlist still applies). Keep this
# intentionally narrow: these are lookup/research surfaces advertised to local
# AI clients through LiteLLM workspace context.
_TOKEN_EXEMPT_PATHS = frozenset(
    {
        "/api/v1/docs/search",
        "/api/v1/docs/search/explain",
        "/api/v1/docs/search/status",
        "/api/v1/docs/search/quality",
        "/api/v1/web-research/health",
        "/api/v1/web-research/egress-ip",
        "/api/v1/web-research/privacy-doc",
        "/api/v1/web-research/query",
    }
)
# Routes that use SYNC_SECRET instead of API_SECRET
_SYNC_PREFIX = "/api/v1/sync/"
# Sync write endpoints used exclusively by node-to-node drain: require SYNC_SECRET only.
# All other sync routes (status, git-pull, gui/*) are browser-accessible and accept either secret.
_SYNC_WRITE_PATHS = ("/api/v1/sync/actions", "/api/v1/sync/restore")
# Bookmarks endpoints that are auth-exempt (aggregate/non-sensitive data, open CORS for extension).
_BOOKMARKS_OPEN_PATHS = frozenset(
    {
        "/api/v1/bookmarks/health",
        "/api/v1/bookmarks/extension-version",
    }
)


def _client_ip(request: Request) -> str:
    """Return the real client IP, honouring X-Forwarded-For (set by Caddy)."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


_LOOPBACK = frozenset({"127.0.0.1", "::1"})


def _is_token_exempt_path(path: str) -> bool:
    return path in _TOKEN_EXEMPT_PATHS or any(
        path.startswith(prefix) for prefix in _TOKEN_EXEMPT_PREFIXES
    )


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ip_str = _client_ip(request)

        # Loopback (localhost curl calls from shell scripts) — always trusted.
        if ip_str in _LOOPBACK:
            return await call_next(request)

        # ── 1. IP allowlist ───────────────────────────────────────────────────
        if _allowed_networks:
            try:
                ip_obj = ipaddress.ip_address(ip_str)
                if not any(ip_obj in net for net in _allowed_networks):
                    log.warning("auth: blocked request from %s — not in allowlist", ip_str)
                    return JSONResponse({"detail": "Forbidden"}, status_code=403)
            except ValueError:
                log.warning("auth: could not parse client IP %r — blocking", ip_str)
                return JSONResponse({"detail": "Forbidden"}, status_code=403)

        # ── 2. Token check (skip for exempt paths) ────────────────────────────
        path = request.url.path
        if path in _BOOKMARKS_OPEN_PATHS:
            return await call_next(request)
        if not _is_token_exempt_path(path):
            # Prefer the header; fall back to query param so EventSource (which
            # cannot set custom headers) can pass its TOTP token in the URL.
            token = request.headers.get("x-api-token", "")
            if not token:
                token = request.query_params.get("token", "")

            if any(path.startswith(p) for p in _SYNC_WRITE_PATHS):
                # Node-to-node sync writes: SYNC_SECRET only.
                if not cfg.SYNC_SECRET:
                    log.debug("auth: SYNC_SECRET not set — skipping token check for %s", path)
                elif not verify_token(cfg.SYNC_SECRET, token):
                    log.warning("auth: invalid SYNC token from %s for %s", ip_str, path)
                    return JSONResponse({"detail": "Unauthorized"}, status_code=401)
            else:
                # All other routes (including browser-facing sync routes):
                # accept API_SECRET or SYNC_SECRET — whichever the caller has.
                if cfg.API_SECRET or cfg.SYNC_SECRET:
                    valid = (cfg.API_SECRET and verify_token(cfg.API_SECRET, token)) or (
                        cfg.SYNC_SECRET and verify_token(cfg.SYNC_SECRET, token)
                    )
                    if not valid:
                        log.warning("auth: invalid token from %s for %s", ip_str, path)
                        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
                else:
                    log.debug("auth: no secrets set — skipping token check for %s", path)

        return await call_next(request)
