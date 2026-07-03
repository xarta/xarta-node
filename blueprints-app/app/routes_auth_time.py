import time
from datetime import UTC, datetime

from fastapi import APIRouter

from .auth import DEFAULT_SKEW_WINDOWS, TOKEN_WINDOW_SECONDS

router = APIRouter()


@router.get("/auth/time")
async def auth_time() -> dict[str, int | str]:
    """
    Return the server clock used for TOTP-style Blueprints API tokens.

    This route intentionally returns no secret material. It is token-exempt so
    browsers can correct local clock drift before deriving X-API-Token, while
    still sitting behind the normal IP allowlist for non-loopback callers.
    """
    epoch_ms = time.time_ns() // 1_000_000
    return {
        "server_epoch_ms": epoch_ms,
        "server_epoch_seconds": epoch_ms // 1000,
        "server_iso": datetime.fromtimestamp(epoch_ms / 1000, UTC).isoformat(),
        "token_window_seconds": TOKEN_WINDOW_SECONDS,
        "accepted_skew_windows": DEFAULT_SKEW_WINDOWS,
    }
