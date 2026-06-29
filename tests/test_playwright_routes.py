import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import routes_playwright  # noqa: E402


def test_unreachable_playwright_health_reports_autostartable_stack(
    monkeypatch,
    tmp_path,
):
    helper = tmp_path / "ensure-running.sh"
    helper.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    helper.chmod(0o755)

    monkeypatch.setattr(routes_playwright, "_START_HELPER", helper)

    payload = routes_playwright._playwright_unreachable_payload(
        "http://localhost:18932",
        "All connection attempts failed",
    )

    assert payload["present"] is True
    assert payload["reachable"] is False
    assert payload["autostart_available"] is True
    assert payload["lifecycle"] == "stopped_or_unreachable"


def test_unreachable_playwright_health_reports_unavailable_without_helper(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(routes_playwright, "_START_HELPER", tmp_path / "missing.sh")

    payload = routes_playwright._playwright_unreachable_payload(
        "http://localhost:18932",
        "All connection attempts failed",
    )

    assert payload["present"] is False
    assert payload["reachable"] is False
    assert payload["autostart_available"] is False
    assert payload["lifecycle"] == "unavailable"
