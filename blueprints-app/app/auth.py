"""auth.py — Time-based HMAC token authentication.

Shared 256-bit secrets live in .env (never transmitted on the wire).
Token = HMAC-SHA256(secret_bytes, str(unix_time // 5).encode())
A skew of ±1 window gives ~15-second effective validity.
"""
import hmac
import hashlib
import time


def _compute(secret_hex: str, window: int) -> str:
    return hmac.new(
        bytes.fromhex(secret_hex),
        str(window).encode(),
        hashlib.sha256,
    ).hexdigest()


def compute_token(secret_hex: str) -> str:
    """Return the current HMAC token for this 5-second window."""
    return _compute(secret_hex, int(time.time()) // 5)


def verify_token(secret_hex: str, token: str, skew: int = 1) -> bool:
    """Return True if *token* matches any window within ±skew of now."""
    if not secret_hex or not token:
        return False
    window = int(time.time()) // 5
    for delta in range(-skew, skew + 1):
        if hmac.compare_digest(_compute(secret_hex, window + delta), token):
            return True
    return False
