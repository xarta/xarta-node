import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_auth_time
from app.auth import DEFAULT_SKEW_WINDOWS, TOKEN_WINDOW_SECONDS


def test_auth_time_returns_token_clock_metadata():
    data = routes_auth_time.auth_time()

    assert isinstance(data["server_epoch_ms"], int)
    assert isinstance(data["server_epoch_seconds"], int)
    assert data["server_epoch_ms"] // 1000 == data["server_epoch_seconds"]
    assert data["server_iso"].endswith("+00:00")
    assert data["token_window_seconds"] == TOKEN_WINDOW_SECONDS
    assert data["accepted_skew_windows"] == DEFAULT_SKEW_WINDOWS
