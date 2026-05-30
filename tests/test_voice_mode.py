import asyncio
import json
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
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 400}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 400,
        "silence_reset_timeout_ms": 2100,
    }
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 9999}) == {
        "speech_aggregation_timeout_ms": 80,
        "vad_reset_timeout_ms": 2000,
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


def test_active_browser_command_action_aliases_are_sanitized():
    assert voice_mode._clean_active_browser_command_action("refresh") == "hard_refresh"
    assert voice_mode._clean_active_browser_command_action("hard-refresh") == "hard_refresh"
    assert voice_mode._clean_active_browser_command_action("app refresh") == "hard_refresh"
    assert voice_mode._clean_active_browser_command_action("vad-dev") == "open_vad_dev"
    assert voice_mode._clean_active_browser_command_action("close-vad") == "close_vad_dev"
    assert voice_mode._clean_active_browser_command_action("modal close") == "close_modal"
    assert voice_mode._clean_active_browser_command_action("page") == "open_page"
    assert voice_mode._clean_active_browser_command_action("open tab") == "open_page"
    assert voice_mode._clean_active_browser_command_action("modal") == "open_modal"
    assert voice_mode._clean_active_browser_command_action("fn") == "menu_function"
    assert voice_mode._clean_active_browser_command_action("menu-fn") == "menu_function"
    assert voice_mode._clean_active_browser_command_action("synthesis") == "open_synthesis"
    assert voice_mode._clean_active_browser_command_action("probes") == "open_probes"
    assert voice_mode._clean_active_browser_command_action("settings") == "open_settings"
    assert voice_mode._clean_active_browser_command_action("selector") == "selector_action"


def test_voice_dev_vad_detector_actions_are_allowed():
    assert voice_mode._clean_dev_command_action("set silero vad") == "set_silero_vad"
    assert voice_mode._clean_dev_command_action("set-vad-detector") == "set_vad_detector"
    assert voice_mode._clean_dev_command_action("set auto pre roll") == "set_auto_pre_roll"
    assert voice_mode._clean_dev_command_action("set-noise-threshold-db") == "set_noise_threshold_db"
    assert "set_silero_vad" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_vad_detector" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_auto_pre_roll" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_noise_threshold" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_noise_threshold_db" in voice_mode._DEV_COMMAND_ACTIONS


def test_active_browser_command_parameters_are_sanitized():
    assert voice_mode._clean_active_browser_event_kind("tap") == "click"
    assert voice_mode._clean_active_browser_event_kind("double tap") == "double_click"
    assert voice_mode._clean_active_browser_event_kind("long-press") == "long_press"
    assert voice_mode._clean_active_browser_event_kind("something else") == "click"
    assert voice_mode._clean_active_browser_modal_id("vad-dev-modal<script>") == "vad-dev-modalscript"
    assert voice_mode._clean_active_browser_selector_action("API Key") == "api-key"
    assert voice_mode._clean_active_browser_group("Settings Panel!") == "settings-panel"
    assert voice_mode._clean_active_browser_page_id("manual-links-page:ABC_123<script>") == "manual-links-page:ABC_123script"
    assert voice_mode._clean_active_browser_menu_item_id("chat-fn-vad-dev<script>") == "chat-fn-vad-devscript"
    assert voice_mode._clean_active_browser_fn_key("nod.backupColumns<script>") == "nod.backupColumnsscript"
    assert len(voice_mode._clean_active_browser_group("A" * 200)) == 80
    assert len(voice_mode._clean_active_browser_page_id("p" * 220)) == 160
    assert len(voice_mode._clean_active_browser_menu_item_id("m" * 220)) == 160
    assert len(voice_mode._clean_active_browser_fn_key("f" * 220)) == 160


def test_active_browser_command_rejects_unsupported_actions():
    response = asyncio.run(
        voice_mode.active_browser_command(
            voice_mode.ActiveBrowserCommandBody(action="delete everything")
        )
    )

    assert response.status_code == 400
    assert json.loads(response.body)["detail"] == "Unsupported active browser action: delete_everything"


def test_active_browser_view_report_updates_active_tab_and_page():
    state = {
        "active": {
            "browser_id": "active-browser",
            "browser_label": "Browser on Win32",
            "stt_enabled": False,
            "stt_mode": "",
            "tts_enabled": True,
            "activated_at": 10,
        },
        "browser_views": {},
        "revision": 10,
        "updated_at": 10,
    }
    body = voice_mode.BrowserViewBody(
        browser_id="active-browser",
        browser_label="Browser on Win32",
        tab_id="tab-1",
        page={
            "group": "settings",
            "tab": "matrix-chat",
            "loading": False,
            "ready": True,
            "api_in_flight": "0",
            "api_quiet_for_ms": 1200,
            "api_sequence": 42,
        },
        modals=[{"id": "vad-dev-modal", "label": "VAD Dev", "open": True}],
        visibility_state="visible",
        has_focus=True,
        voice={"stt_enabled": True, "stt_mode": "push_to_talk", "tts_enabled": True},
        viewport={
            "innerWidth": 1280,
            "innerHeight": 720,
            "devicePixelRatio": 1,
            "screen": {"width": 1920, "height": 1080},
            "orientation": {"type": "landscape-primary", "angle": 0},
            "visualViewport": {"width": 1280, "height": 720, "scale": 1},
            "pointer": {"primary": "fine", "any": "fine", "coarse": False, "fine": True, "maxTouchPoints": 0},
        },
        frontend={"app": "fallback-ui", "asset_version": "dev-test"},
        automation={
            "current_group": "settings",
            "current_page_id": "matrix-chat",
            "menus": [
                {
                    "group": "settings",
                    "active_id": "matrix-chat",
                    "layout_item_id": "settings-layout",
                    "pages": [
                        {
                            "id": "matrix-chat",
                            "label": "Chat",
                            "page_label": "Chat",
                            "parent": "agent-pages",
                            "target_id": "matrix-chat",
                            "current": True,
                            "visible": True,
                            "invokable": True,
                        }
                    ],
                    "function_items": [
                        {
                            "id": "chat-fn-vad-dev",
                            "label": "VAD Dev",
                            "fn": "chat.vadDev",
                            "active_on": ["matrix-chat"],
                            "current_context": True,
                            "visible": True,
                            "registered": True,
                            "invokable": True,
                        }
                    ],
                    "current_functions": [
                        {
                            "id": "chat-fn-vad-dev",
                            "label": "VAD Dev",
                            "fn": "chat.vadDev",
                            "active_on": ["matrix-chat"],
                            "current_context": True,
                            "visible": True,
                            "registered": True,
                            "invokable": True,
                        }
                    ],
                }
            ],
            "selector_actions": [{"action": "settings", "label": "Settings", "bridge_group": "settings"}],
        },
    )
    report = voice_mode._clean_browser_view_report(body, 20)

    changed = voice_mode._store_browser_view_report_unlocked(state, report, 20)
    view = voice_mode._selected_active_browser_view(state)

    assert changed is True
    assert state["active"]["tab_id"] == "tab-1"
    assert state["active"]["last_view_page"]["tab"] == "matrix-chat"
    assert state["active"]["last_view_page"]["ready"] is True
    assert state["active"]["last_view_page"]["api_quiet_for_ms"] == 1200
    assert view["tab_id"] == "tab-1"
    assert view["modals"][0]["id"] == "vad-dev-modal"
    assert view["frontend"]["asset_version"] == "dev-test"
    assert view["voice"] == {
        "stt_enabled": True,
        "stt_mode": "push_to_talk",
        "tts_enabled": True,
    }
    assert view["viewport"]["innerWidth"] == 1280
    assert view["viewport_class"] == "landscape_1080p_like"
    assert view["viewport_flags"]["standard_landscape"] is True
    assert view["automation"]["current_group"] == "settings"
    assert view["automation"]["menus"][0]["pages"][0]["id"] == "matrix-chat"
    assert view["automation"]["menus"][0]["function_items"][0]["fn"] == "chat.vadDev"
    assert view["automation"]["selector_actions"][0]["action"] == "settings"


def test_active_browser_view_exposes_automation_defaults():
    public = voice_mode._public_active_browser_view({"active": None, "browser_views": {}})

    assert public["automation"]["default_step_timeout_seconds"] == 10
    assert public["automation"]["minimum_step_timeout_seconds"] == 1
    assert public["automation"]["maximum_step_timeout_seconds"] == 120


def test_active_browser_viewport_classification_flags_are_provisional():
    mobile = voice_mode._clean_browser_view_report(
        voice_mode.BrowserViewBody(
            browser_id="phone",
            viewport={
                "innerWidth": 390,
                "innerHeight": 844,
                "screen": {"width": 390, "height": 844},
                "pointer": {"coarse": True, "touch": True, "maxTouchPoints": 5},
            },
        ),
        20,
    )
    wide = voice_mode._clean_browser_view_report(
        voice_mode.BrowserViewBody(
            browser_id="wide",
            viewport={
                "innerWidth": 2560,
                "innerHeight": 1080,
                "screen": {"width": 2560, "height": 1080},
                "pointer": {"fine": True, "maxTouchPoints": 0},
            },
        ),
        20,
    )

    assert mobile["viewport_class"] == "mobile_portrait"
    assert mobile["viewport_flags"]["mobile_portrait"] is True
    assert wide["viewport_class"] == "widescreen"
    assert wide["viewport_flags"]["widescreen"] is True
    assert wide["viewport_classification"]["provisional"] is True


def test_active_browser_client_inventory_marks_active_fresh_and_stale():
    state = {
        "active": {"browser_id": "active-browser", "tab_id": "tab-1", "stt_enabled": False},
        "browser_views": {
            "active-browser::tab-1": {
                "browser_id": "active-browser",
                "browser_label": "Active",
                "tab_id": "tab-1",
                "reported_at": 95,
                "visibility_state": "visible",
                "has_focus": True,
                "frontend": {"asset_version": "v1"},
            },
            "other-browser::tab-2": {
                "browser_id": "other-browser",
                "browser_label": "Other",
                "tab_id": "tab-2",
                "reported_at": 50,
                "visibility_state": "hidden",
                "has_focus": False,
                "frontend": {"asset_version": "v0"},
            },
        },
    }

    clients = voice_mode._browser_client_inventory(state, now=100, max_age_seconds=30)

    assert clients[0]["browser_id"] == "active-browser"
    assert clients[0]["active_tab"] is True
    assert clients[0]["fresh"] is True
    assert clients[0]["age_seconds"] == 5
    assert clients[1]["browser_id"] == "other-browser"
    assert clients[1]["stale"] is True


def test_active_browser_client_lookup_rejects_missing_and_stale_reports():
    state = {
        "active": None,
        "browser_views": {
            "stale-browser::tab-1": {
                "browser_id": "stale-browser",
                "tab_id": "tab-1",
                "reported_at": 10,
            }
        },
    }

    stale, stale_reason = voice_mode._find_browser_client_report(
        state,
        browser_id="stale-browser",
        tab_id="tab-1",
        now=100,
        max_age_seconds=30,
    )
    missing, missing_reason = voice_mode._find_browser_client_report(
        state,
        browser_id="missing-browser",
        now=100,
        max_age_seconds=30,
    )

    assert stale["browser_id"] == "stale-browser"
    assert "stale" in stale_reason
    assert missing is None
    assert missing_reason == "Browser client was not found"


def test_active_browser_from_client_report_preserves_existing_voice_flags_for_same_browser():
    report = {
        "browser_id": "active-browser",
        "browser_label": "Active",
        "tab_id": "tab-2",
    }
    current_active = {
        "browser_id": "active-browser",
        "stt_enabled": True,
        "stt_mode": "wake_to_talk",
        "tts_enabled": True,
    }

    active = voice_mode._active_browser_from_client_report(
        report,
        25,
        current_active=current_active,
    )
    fresh = voice_mode._active_browser_from_client_report(
        {"browser_id": "other-browser", "browser_label": "Other", "tab_id": "tab-3"},
        30,
    )

    assert active["browser_id"] == "active-browser"
    assert active["tab_id"] == "tab-2"
    assert active["stt_mode"] == "wake_to_talk"
    assert active["tts_enabled"] is True
    assert fresh["stt_enabled"] is False
    assert fresh["tts_enabled"] is False


def test_active_browser_from_client_report_uses_reported_voice_state_for_new_browser():
    active = voice_mode._active_browser_from_client_report(
        {
            "browser_id": "phone-browser",
            "browser_label": "Phone",
            "tab_id": "phone-tab",
            "voice": {
                "stt_enabled": True,
                "stt_mode": "wake_to_talk",
                "tts_enabled": True,
            },
        },
        40,
    )

    assert active["browser_id"] == "phone-browser"
    assert active["tab_id"] == "phone-tab"
    assert active["stt_enabled"] is True
    assert active["stt_mode"] == "wake_to_talk"
    assert active["tts_enabled"] is True


def test_active_browser_from_client_report_allows_explicit_voice_override():
    body = voice_mode.BrowserClientSelectionBody(
        browser_id="phone-browser",
        stt_enabled=False,
        stt_mode="",
        tts_enabled=False,
    )

    active = voice_mode._active_browser_from_client_report(
        {
            "browser_id": "phone-browser",
            "browser_label": "Phone",
            "tab_id": "phone-tab",
            "voice": {
                "stt_enabled": True,
                "stt_mode": "wake_to_talk",
                "tts_enabled": True,
            },
        },
        40,
        body=body,
    )

    assert active["stt_enabled"] is False
    assert active["stt_mode"] == ""
    assert active["tts_enabled"] is False


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


def test_voice_mode_wake_debug_masks_report_from_non_active_browser():
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
    assert wake_debug["debug"]["reason"] == "This browser is not the Active Browser."


def test_active_browser_activation_fsm_replaces_existing_active_browser():
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
    active_browser = voice_mode._active_browser_from_body(body, 20)

    result = voice_mode._ActiveBrowserActivationFsm(state).dispatch(
        voice_mode._ActiveBrowserActivationFsm.INPUT_ACTIVATE_REQUEST,
        browser_id="new-browser",
        active_browser=active_browser,
        now=20,
    )

    assert result["changed"] is True
    assert result["from"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert result["to"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "new-browser"
    assert state["active"]["stt_mode"] == "wake_to_talk"
    assert state["active"]["tts_enabled"] is False
    assert state["revision"] == 20
    assert state["updated_at"] == 20


def test_active_browser_activation_fsm_activates_from_idle():
    state = {"active": None, "revision": 0, "updated_at": 0}
    body = voice_mode.BrowserVoiceState(
        browser_id="new-browser",
        browser_label="New Browser",
        tab_id="new-tab",
        stt_enabled=False,
        stt_mode="",
        tts_enabled=True,
    )
    active_browser = voice_mode._active_browser_from_body(body, 20)

    result = voice_mode._ActiveBrowserActivationFsm(state).dispatch(
        voice_mode._ActiveBrowserActivationFsm.INPUT_ACTIVATE_REQUEST,
        browser_id="new-browser",
        active_browser=active_browser,
        now=20,
    )

    assert result["changed"] is True
    assert result["from"] == voice_mode._ActiveBrowserActivationFsm.STATE_IDLE
    assert result["to"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "new-browser"
    assert state["active"]["tab_id"] == "new-tab"
    assert state["active"]["tts_enabled"] is True


def test_active_browser_activation_fsm_only_deactivates_current_browser():
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

    ignored = voice_mode._ActiveBrowserActivationFsm(state).dispatch(
        voice_mode._ActiveBrowserActivationFsm.INPUT_DEACTIVATE_REQUEST,
        browser_id="other-browser",
        now=20,
    )
    assert ignored["changed"] is False
    assert ignored["from"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert ignored["to"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert state["active"]["browser_id"] == "current-browser"
    assert state["revision"] == 10

    result = voice_mode._ActiveBrowserActivationFsm(state).dispatch(
        voice_mode._ActiveBrowserActivationFsm.INPUT_DEACTIVATE_REQUEST,
        browser_id="current-browser",
        now=30,
    )
    assert result["changed"] is True
    assert result["from"] == voice_mode._ActiveBrowserActivationFsm.STATE_ACTIVATED
    assert result["to"] == voice_mode._ActiveBrowserActivationFsm.STATE_IDLE
    assert state["active"] is None
    assert state["revision"] == 30
