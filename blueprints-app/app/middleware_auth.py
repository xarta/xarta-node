"""middleware_auth.py — IP allowlist + TOTP token authentication middleware.

Two protection layers applied in order to every inbound request:

1. IP allowlist — client IP must fall within BLUEPRINTS_ALLOWED_NETWORKS
   (comma-separated CIDRs).  Caddy sets X-Forwarded-For; we read that first,
   then fall back to the raw connection IP.

2. TOTP token — X-API-Token header must match the current (±1 window)
   HMAC-SHA256 token derived from the appropriate secret:
     • /api/v1/sync/* routes  →  BLUEPRINTS_SYNC_SECRET
     • all other routes        →  BLUEPRINTS_API_SECRET

Exempt paths (no token required): /health, and anything under /ui.

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

# Paths that require NO token (IP allowlist still applies)
_TOKEN_EXEMPT_PREFIXES = ("/health", "/ui")
# Routes that use SYNC_SECRET instead of API_SECRET
_SYNC_PREFIX = "/api/v1/sync/"


def _client_ip(request: Request) -> str:
    """Return the real client IP, honouring X-Forwarded-For (set by Caddy)."""
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        ip_str = _client_ip(request)

        # ── 1. IP allowlist ───────────────────────────────────────────────────
        if _allowed_networks:
            try:
                ip_obj = ipaddress.ip_address(ip_str)
                if not any(ip_obj in net for net in _allowed_networks):
                    log.warning(
                        "auth: blocked request from %s — not in allowlist", ip_str
                    )
                    return JSONResponse({"detail": "Forbidden"}, status_code=403)
            except ValueError:
                log.warning("auth: could not parse client IP %r — blocking", ip_str)
                return JSONResponse({"detail": "Forbidden"}, status_code=403)

        # ── 2. Token check (skip for exempt paths) ────────────────────────────
        path = request.url.path
        if not any(path.startswith(p) for p in _TOKEN_EXEMPT_PREFIXES):
            if path.startswith(_SYNC_PREFIX):
                secret = cfg.SYNC_SECRET
                secret_name = "SYNC"
            else:
                secret = cfg.API_SECRET
                secret_name = "API"

            if not secret:
                log.debug(
                    "auth: %s_SECRET not set — skipping token check for %s",
                    secret_name,
                    path,
                )
            else:
                token = request.headers.get("x-api-token", "")
                if not verify_token(secret, token):
                    log.warning(
                        "auth: invalid %s token from %s for %s",
                        secret_name,
                        ip_str,
                        path,
                    )
                    return JSONResponse(
                        {"detail": "Unauthorized"}, status_code=401
                    )

        return await call_next(request)
