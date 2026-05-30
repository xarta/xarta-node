import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_notifier_dnd as dnd


def _request(ip: str):
    return SimpleNamespace(client=SimpleNamespace(host=ip))


def _claim(listener_id: str, kind: str, event_id: str, ip: str, os_key: str = "linux"):
    body = dnd.SpeechClaimRequest(
        listener_id=listener_id,
        kind=kind,
        event_id=event_id,
        os_key=os_key,
    )
    return asyncio.run(dnd.claim_notifier_speech(body, _request(ip)))


def _reset_runtime(monkeypatch):
    dnd._listener_heartbeats.clear()
    dnd._speech_claims.clear()
    monkeypatch.setattr(
        dnd,
        "_read_config",
        lambda: dnd.NotifierDndConfig(
            listener_policy=dnd.ListenerPolicy(
                phone_wins=True,
                desktop_one_per_os_ip=True,
            )
        ),
    )


def test_phone_listener_does_not_block_desktop_speech(monkeypatch):
    _reset_runtime(monkeypatch)

    phone = _claim("phone-listener-01", "phone", "event-1", "10.0.0.10", "android")
    desktop = _claim("desktop-listener-01", "desktop", "event-1", "10.0.0.20", "linux")

    assert phone.allowed is True
    assert desktop.allowed is True


def test_phone_listeners_are_not_deduped_by_ip(monkeypatch):
    _reset_runtime(monkeypatch)

    first = _claim("phone-listener-01", "phone", "event-1", "10.0.0.10", "android")
    second = _claim("phone-listener-02", "phone", "event-1", "10.0.0.10", "android")

    assert first.allowed is True
    assert second.allowed is True


def test_desktop_speech_dedupes_by_client_ip_and_event(monkeypatch):
    _reset_runtime(monkeypatch)

    first = _claim("desktop-listener-01", "desktop", "event-1", "10.0.0.20", "linux")
    same_ip_same_event = _claim("desktop-listener-02", "desktop", "event-1", "10.0.0.20", "linux")
    same_ip_next_event = _claim("desktop-listener-02", "desktop", "event-2", "10.0.0.20", "linux")
    other_ip_same_event = _claim("desktop-listener-03", "desktop", "event-1", "10.0.0.30", "linux")

    assert first.allowed is True
    assert same_ip_same_event.allowed is False
    assert same_ip_same_event.reason == "desktop_claim_exists"
    assert same_ip_next_event.allowed is True
    assert other_ip_same_event.allowed is True


def test_legacy_phone_wins_config_is_forced_to_phone_always_speaks(tmp_path, monkeypatch):
    config_path = tmp_path / "system-bridge-notifier-dnd.json"
    config_path.write_text(
        json.dumps({"listener_policy": {"phone_wins": False, "desktop_one_per_os_ip": True}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(dnd, "_CONFIG_PATH", config_path)

    config = dnd._read_config()
    assert config.listener_policy.phone_wins is True

    config.listener_policy.phone_wins = False
    written = dnd._write_config(config)
    saved = json.loads(config_path.read_text(encoding="utf-8"))

    assert written.listener_policy.phone_wins is True
    assert saved["listener_policy"]["phone_wins"] is True


def test_default_toast_policy_enables_all_known_categories():
    policy = dnd._default_config().toast_policy.model_dump()

    assert policy == {
        "model_alias": True,
        "system_health": True,
        "security": True,
        "notification_tests": True,
        "hermes_speech": True,
        "active_browser_state": True,
        "active_browser_commands": True,
        "matrix_chat": True,
        "unknown_other": True,
    }


def test_toast_policy_round_trips_through_config_file(tmp_path, monkeypatch):
    config_path = tmp_path / "system-bridge-notifier-dnd.json"
    monkeypatch.setattr(dnd, "_CONFIG_PATH", config_path)
    config = dnd._default_config()
    config.toast_policy.active_browser_commands = False
    config.toast_policy.active_browser_state = False

    dnd._write_config(config)
    loaded = dnd._read_config()

    assert loaded.toast_policy.active_browser_commands is False
    assert loaded.toast_policy.active_browser_state is False
    assert loaded.toast_policy.unknown_other is True


def test_missing_toast_policy_reads_as_backward_compatible_defaults(tmp_path, monkeypatch):
    config_path = tmp_path / "system-bridge-notifier-dnd.json"
    config_path.write_text(json.dumps({"mode": "default"}), encoding="utf-8")
    monkeypatch.setattr(dnd, "_CONFIG_PATH", config_path)

    config = dnd._read_config()

    assert config.toast_policy.model_alias is True
    assert config.toast_policy.unknown_other is True


def test_unknown_toast_policy_categories_are_normalized_away(tmp_path, monkeypatch):
    config_path = tmp_path / "system-bridge-notifier-dnd.json"
    config_path.write_text(
        json.dumps(
            {
                "toast_policy": {
                    "model_alias": False,
                    "unknown_future_category": False,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dnd, "_CONFIG_PATH", config_path)

    config = dnd._read_config()
    dumped = config.toast_policy.model_dump()

    assert config.toast_policy.model_alias is False
    assert "unknown_future_category" not in dumped
