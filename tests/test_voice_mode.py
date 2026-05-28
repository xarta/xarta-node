import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_voice_mode as voice_mode


def test_voice_mode_wake_settings_defaults_and_bounds_are_sanitized():
    policy = voice_mode._clean_wake_to_talk_policy(
        {
            "instances": {
                "local": {
                    "enabled": False,
                    "matrix_server": "tb1",
                    "wake_word": "Computer",
                    "post_wake_pause_ms": 513,
                    "initial_silence_cancel_ms": 999,
                    "pause_reset_seconds": 33,
                    "auto_execute_silence_ms": 175,
                    "commands": {"execute": "execute"},
                },
                "vps": {
                    "matrix_server": "not-valid",
                    "wake_word": "Mini-Me",
                    "auto_execute_silence_ms": 3601,
                },
            }
        }
    )

    local = policy["instances"]["local"]
    assert local["enabled"] is True
    assert local["post_wake_pause_ms"] == 500
    assert local["initial_silence_cancel_ms"] == 1000
    assert local["pause_reset_seconds"] == 35
    assert local["auto_execute_silence_ms"] == 300
    assert local["commands"]["execute"] == "execute"
    assert local["commands"]["pause"] == "pause-dictation"
    assert local["hermes_prefix"] == "hermes: "

    vps = policy["instances"]["vps"]
    assert vps["matrix_server"] == "vps"
    assert vps["auto_execute_silence_ms"] == 3000
    assert vps["hermes_prefix"] == "hermes-vps: "
    assert "mini me" in vps["wake_aliases"]
    assert "minime" in vps["wake_aliases"]


def test_voice_mode_stt_policy_sanitizes_aggregation_timeout():
    assert voice_mode._clean_stt_policy({"speech_aggregation_timeout_ms": 83}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 300,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"speech_aggregation_timeout_ms": 999}) == {
        "speech_aggregation_timeout_ms": 300,
        "vad_reset_timeout_ms": 300,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 1}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 0,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 126}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 150,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 9999}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 500,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"silence_reset_timeout_ms": 2000}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 300,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"silence_reset_timeout_ms": 1}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 300,
        "silence_reset_timeout_ms": 0,
    }


def test_voice_mode_aggregation_proxy_payload_uses_seconds_for_pipecat():
    assert voice_mode._aggregation_timeout_payload(83) == {
        "aggregation_timeout": 0.08,
        "aggregation_timeout_ms": 80,
    }
    assert voice_mode._aggregation_timeout_payload(301) == {
        "aggregation_timeout": 0.3,
        "aggregation_timeout_ms": 300,
    }


def test_voice_mode_wake_debug_prefers_active_browser_report():
    state = {
        "active": {
            "browser_id": "active-browser",
            "browser_label": "Browser on Win32",
            "stt_enabled": True,
            "stt_mode": "wake_to_talk",
            "tts_enabled": True,
        }
    }
    debug = {
        "reports": {
            "other-browser": {"browser_id": "other-browser", "fsm_state": "ARMED_IDLE", "reported_at": 1},
            "active-browser": {
                "browser_id": "active-browser",
                "fsm_state": "CAPTURING",
                "queues": {"input_queue": [], "message_queue": [{"text": "hello"}]},
                "reported_at": 2,
            },
        }
    }

    public = voice_mode._public_wake_debug(state, debug)

    assert public["ok"] is True
    assert public["has_debug"] is True
    assert public["debug"]["browser_id"] == "active-browser"
    assert public["debug"]["fsm_state"] == "CAPTURING"
    assert public["debug"]["queues"]["message_queue"][0]["text"] == "hello"


def test_voice_mode_wake_debug_report_cannot_override_authoritative_active_wake_status():
    state = {
        "active": {
            "browser_id": "active-browser",
            "browser_label": "Browser on Win32",
            "stt_enabled": True,
            "stt_mode": "wake_to_talk",
            "tts_enabled": True,
        }
    }
    debug = {
        "reports": {
            "active-browser": {
                "browser_id": "active-browser",
                "running": False,
                "starting": False,
                "fsm_state": "DISABLED",
                "reason": "Wake to Talk is not selected.",
                "reported_at": 2,
            },
        }
    }

    status = voice_mode._public_state(state, debug)
    wake_debug = voice_mode._public_wake_debug(state, debug)

    assert status["active"]["stt_enabled"] is True
    assert status["active"]["stt_mode"] == "wake_to_talk"
    assert wake_debug["active"]["stt_enabled"] is True
    assert wake_debug["active"]["stt_mode"] == "wake_to_talk"
    assert wake_debug["debug"]["authoritative_browser_active"] is True


def test_voice_mode_wake_debug_does_not_treat_stt_mode_as_a_separate_activation():
    state = {
        "active": {
            "browser_id": "active-browser",
            "browser_label": "Browser on Win32",
            "stt_enabled": False,
            "stt_mode": "",
            "tts_enabled": True,
        }
    }
    debug = {
        "reports": {
            "active-browser": {
                "browser_id": "active-browser",
                "running": True,
                "starting": False,
                "fsm_state": "ARMED_IDLE",
                "reason": "",
                "audio_frames_sent": 30027,
                "reported_at": 2,
            },
        }
    }

    wake_debug = voice_mode._public_wake_debug(state, debug)

    assert wake_debug["active"]["stt_enabled"] is False
    assert wake_debug["active"]["stt_mode"] == ""
    assert wake_debug["debug"]["authoritative_browser_active"] is True
    assert wake_debug["debug"]["running"] is True
    assert wake_debug["debug"]["fsm_state"] == "ARMED_IDLE"


def test_voice_mode_wake_debug_masks_report_from_non_activated_browser():
    state = {
        "active": None
    }
    debug = {
        "reports": {
            "other-browser": {
                "browser_id": "other-browser",
                "running": True,
                "starting": False,
                "fsm_state": "ARMED_IDLE",
                "reason": "",
                "audio_frames_sent": 30027,
                "reported_at": 2,
            },
        }
    }

    wake_debug = voice_mode._public_wake_debug(state, debug)

    assert wake_debug["debug"]["authoritative_browser_active"] is False
    assert wake_debug["debug"]["running"] is False
    assert wake_debug["debug"]["starting"] is False
    assert wake_debug["debug"]["fsm_state"] == "SELECTED_INACTIVE"
    assert wake_debug["debug"]["reason"] == "This browser is not activated for Voice Mode."


def test_voice_mode_activation_fsm_replaces_existing_activated_browser():
    state = {
        "active": {
            "browser_id": "old-browser",
            "browser_label": "Old Browser",
            "stt_enabled": True,
            "stt_mode": "push_to_talk",
            "tts_enabled": True,
            "activated_at": 10,
        },
        "revision": 10,
        "updated_at": 10,
    }
    body = voice_mode.BrowserVoiceState(
        browser_id="new-browser",
        browser_label="New Browser",
        stt_enabled=True,
        stt_mode="wake_to_talk",
        tts_enabled=False,
    )
    activated = voice_mode._activated_browser_from_body(body, 20)

    result = voice_mode._VoiceModeActivationFsm(state).dispatch(
        voice_mode._VoiceModeActivationFsm.INPUT_ACTIVATE_REQUEST,
        browser_id="new-browser",
        activated_browser=activated,
        now=20,
    )

    assert result["changed"] is True
    assert result["from"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert result["to"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "new-browser"
    assert state["active"]["stt_mode"] == "wake_to_talk"
    assert state["active"]["tts_enabled"] is False
    assert state["revision"] == 20
    assert state["updated_at"] == 20


def test_voice_mode_activation_fsm_activates_from_idle():
    state = {"active": None, "revision": 0, "updated_at": 0}
    body = voice_mode.BrowserVoiceState(
        browser_id="new-browser",
        browser_label="New Browser",
        stt_enabled=False,
        stt_mode="",
        tts_enabled=True,
    )
    activated = voice_mode._activated_browser_from_body(body, 20)

    result = voice_mode._VoiceModeActivationFsm(state).dispatch(
        voice_mode._VoiceModeActivationFsm.INPUT_ACTIVATE_REQUEST,
        browser_id="new-browser",
        activated_browser=activated,
        now=20,
    )

    assert result["changed"] is True
    assert result["from"] == voice_mode._VoiceModeActivationFsm.STATE_IDLE
    assert result["to"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "new-browser"
    assert state["active"]["tts_enabled"] is True


def test_voice_mode_activation_fsm_only_deactivates_current_browser():
    state = {
        "active": {
            "browser_id": "current-browser",
            "browser_label": "Current Browser",
            "stt_enabled": True,
            "stt_mode": "wake_to_talk",
            "tts_enabled": True,
            "activated_at": 10,
        },
        "revision": 10,
        "updated_at": 10,
    }

    ignored = voice_mode._VoiceModeActivationFsm(state).dispatch(
        voice_mode._VoiceModeActivationFsm.INPUT_DEACTIVATE_REQUEST,
        browser_id="other-browser",
        now=20,
    )
    assert ignored["changed"] is False
    assert ignored["from"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert ignored["to"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "current-browser"
    assert state["revision"] == 10

    result = voice_mode._VoiceModeActivationFsm(state).dispatch(
        voice_mode._VoiceModeActivationFsm.INPUT_DEACTIVATE_REQUEST,
        browser_id="current-browser",
        now=30,
    )
    assert result["changed"] is True
    assert result["from"] == voice_mode._VoiceModeActivationFsm.STATE_ACTIVATED
    assert result["to"] == voice_mode._VoiceModeActivationFsm.STATE_IDLE
    assert state["active"] is None
    assert state["revision"] == 30
