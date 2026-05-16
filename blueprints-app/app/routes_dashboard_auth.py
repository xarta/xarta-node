"""Blueprints-backed browser sessions for Caddy-protected dashboards."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, RedirectResponse

from . import config as cfg

router = APIRouter(prefix="/dashboard-auth", tags=["dashboard-auth"])

_COOKIE_NAME = "bp_hermes_local_session"
_AUDIENCE = "hermes-local-dashboard"


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _signing_key() -> bytes | None:
    secret_hex = cfg.API_SECRET or cfg.SYNC_SECRET
    if not secret_hex:
        return None
    try:
        return bytes.fromhex(secret_hex)
    except ValueError:
        return None


def _sign(payload_b64: str, key: bytes) -> str:
    return _b64encode(
        hmac.new(key, payload_b64.encode("ascii"), hashlib.sha256).digest()
    )


def _make_session_value(now: int | None = None) -> tuple[str, int]:
    key = _signing_key()
    if key is None:
        raise RuntimeError("dashboard auth signing key is not configured")
    issued_at = int(now or time.time())
    ttl = max(60, int(cfg.DASHBOARD_AUTH_SESSION_SECONDS or 3600))
    expires_at = issued_at + ttl
    payload = {
        "aud": _AUDIENCE,
        "iat": issued_at,
        "exp": expires_at,
    }
    payload_b64 = _b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    return f"{payload_b64}.{_sign(payload_b64, key)}", expires_at


def _verify_session_value(value: str, now: int | None = None) -> bool:
    key = _signing_key()
    if key is None or not value or "." not in value:
        return False
    payload_b64, sig_b64 = value.rsplit(".", 1)
    expected = _sign(payload_b64, key)
    if not hmac.compare_digest(sig_b64.encode(), expected.encode()):
        return False
    try:
        payload = json.loads(_b64decode(payload_b64).decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return False
    if payload.get("aud") != _AUDIENCE:
        return False
    return int(payload.get("exp") or 0) > int(now or time.time())


def _login_url() -> str:
    if cfg.DASHBOARD_AUTH_LOGIN_URL:
        return cfg.DASHBOARD_AUTH_LOGIN_URL
    return f"{cfg.UI_URL}/fallback-ui/?group=settings&tab=hermes-local"


def _unauthorized_response() -> Response:
    return RedirectResponse(_login_url(), status_code=302, headers={"Cache-Control": "no-store"})


@router.post("/hermes-local/session")
def establish_hermes_local_session(response: Response) -> dict[str, int | str | bool | None]:
    """Issue a short-lived HttpOnly cookie after normal Blueprints TOTP auth."""
    try:
        session_value, expires_at = _make_session_value()
    except RuntimeError:
        return JSONResponse(
            {"ok": False, "detail": "dashboard auth signing key is not configured"},
            status_code=503,
        )

    cookie_domain = cfg.DASHBOARD_AUTH_COOKIE_DOMAIN or None
    max_age = max(60, int(cfg.DASHBOARD_AUTH_SESSION_SECONDS or 3600))
    response.set_cookie(
        _COOKIE_NAME,
        session_value,
        max_age=max_age,
        expires=max_age,
        path="/",
        domain=cookie_domain,
        secure=True,
        httponly=True,
        samesite="lax",
    )
    response.headers["Cache-Control"] = "no-store"
    return {
        "ok": True,
        "expires_at": expires_at,
        "cookie_name": _COOKIE_NAME,
        "cookie_domain": cookie_domain,
    }


@router.get("/hermes-local/validate")
def validate_hermes_local_session(request: Request) -> Response:
    """Caddy forward_auth endpoint for the standalone Hermes dashboard host."""
    if _verify_session_value(request.cookies.get(_COOKIE_NAME, "")):
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return _unauthorized_response()
