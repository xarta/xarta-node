import asyncio
import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_voice_mode as voice_mode


@pytest.fixture(autouse=True)
def _isolate_wake_stt_direct_route_env(tmp_path, monkeypatch):
    # Keep unit-test defaults independent from TB1's live Wake direct-route config.
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_INSTANCES_FILE", str(tmp_path / "instances.json"))
    monkeypatch.delenv("HERMES_STT_INSTANCES_FILE", raising=False)
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", raising=False)
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_VPS_DIRECT_ROUTE_ENABLED", raising=False)


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
                    "execute_cancel_ms": 999,
                    "partial_settle_ms": 1,
                    "commands": {"execute": "execute"},
                },
                "vps": {
                    "matrix_server": "not-valid",
                    "wake_word": "Mini-Me",
                    "auto_execute_silence_ms": 3601,
                    "partial_settle_timeout_ms": 1201,
                },
            }
        }
    )

    local = policy["instances"]["local"]
    assert local["enabled"] is True
    assert "post_wake_pause_ms" not in local
    assert "initial_silence_cancel_ms" not in local
    assert "pause_reset_seconds" not in local
    assert local["auto_execute_silence_ms"] == 300
    assert local["execute_cancel_ms"] == 900
    assert local["partial_settle_ms"] == 300
    assert local["delivery_mode"] == "matrix"
    assert local["direct_available"] is True
    assert local["direct_enabled"] is False
    assert local["direct_route_enabled"] is False
    assert local["direct_rollback_applied"] is False
    assert local["commands"]["execute"] == "execute"
    assert local["commands"]["pause"] == "pause-dictation"
    assert local["hermes_prefix"] == "hermes: "

    vps = policy["instances"]["vps"]
    assert vps["matrix_server"] == "vps"
    assert vps["auto_execute_silence_ms"] == 3000
    assert vps["execute_cancel_ms"] == 0
    assert vps["partial_settle_ms"] == 1200
    assert vps["delivery_mode"] == "matrix"
    assert vps["direct_available"] is False
    assert vps["direct_enabled"] is False
    assert vps["hermes_prefix"] == "hermes-vps: "
    assert "mini me" in vps["wake_aliases"]
    assert "minime" in vps["wake_aliases"]


def test_voice_mode_wake_direct_delivery_rolls_back_until_route_enabled(monkeypatch):
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", raising=False)

    policy = voice_mode._clean_wake_to_talk_policy(
        {
            "instances": {
                "local": {
                    "delivery_mode": "direct-hermes",
                    "direct_enabled": True,
                },
                "vps": {
                    "delivery_mode": "direct-hermes",
                    "direct_enabled": True,
                },
            }
        }
    )

    local = policy["instances"]["local"]
    assert local["delivery_mode"] == "matrix"
    assert local["direct_available"] is True
    assert local["direct_enabled"] is False
    assert local["direct_requested"] is True
    assert local["direct_status"] == "rollback_disabled"
    assert local["direct_rollback_applied"] is True
    assert local["direct_rollback_reason"] == "direct_route_disabled"

    vps = policy["instances"]["vps"]
    assert vps["delivery_mode"] == "matrix"
    assert vps["direct_available"] is False
    assert vps["direct_enabled"] is False
    assert vps["direct_status"] == "not_available"
    assert vps["direct_rollback_reason"] == "direct_not_available"


def test_voice_mode_wake_direct_delivery_can_survive_when_route_enabled(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "true")

    policy = voice_mode._clean_wake_to_talk_policy(
        {
            "instances": {
                "local": {
                    "delivery_mode": "direct-hermes",
                    "direct_enabled": True,
                },
            }
        }
    )

    local = policy["instances"]["local"]
    assert local["delivery_mode"] == "direct_local"
    assert local["direct_available"] is True
    assert local["direct_enabled"] is True
    assert local["direct_status"] == "enabled"
    assert local["direct_route_enabled"] is True
    assert local["direct_rollback_applied"] is False


def test_voice_mode_wake_vps_direct_uses_instance_config_and_rollout_env(tmp_path, monkeypatch):
    instances_file = tmp_path / "instances.json"
    instances_file.write_text(
        json.dumps(
            {
                "schema": "xarta.wake-stt.instances.v1",
                "instances": {
                    "vps": {
                        "direct_available": True,
                        "delivery_mode": "direct_vps",
                        "route_enabled_env": "BLUEPRINTS_WAKE_STT_VPS_DIRECT_ROUTE_ENABLED",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_INSTANCES_FILE", str(instances_file))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_VPS_DIRECT_ROUTE_ENABLED", "1")

    policy = voice_mode._clean_wake_to_talk_policy(
        {
            "instances": {
                "vps": {
                    "delivery_mode": "direct-hermes",
                    "direct_enabled": True,
                },
            }
        }
    )

    vps = policy["instances"]["vps"]
    assert vps["delivery_mode"] == "direct_vps"
    assert vps["direct_available"] is True
    assert vps["direct_enabled"] is True
    assert vps["direct_route_enabled"] is True
    assert vps["direct_status"] == "enabled"


def test_voice_mode_stt_policy_sanitizes_aggregation_timeout():
    def expected(**overrides):
        base = {
            "speech_aggregation_timeout_ms": 80,
            "vad_reset_timeout_ms": 300,
            "pre_roll_frames": 1,
            "silero_vad_enabled": False,
            "vad_interrupt_tts_enabled": False,
            "word_detection_match_interrupt_tts_enabled": False,
            "word_detection_prefix_partial_interrupt_tts_enabled": False,
            "word_detection_prefix_final_interrupt_tts_enabled": False,
            "word_detection_payload0_timeout_ms": 0,
            "word_detection_match_cue_enabled": False,
            "word_detection_match_cue_sound": "",
            "word_detection_payload0_timeout_cue_enabled": False,
            "word_detection_payload0_timeout_cue_sound": "",
            "word_detection_agent_candidate_cue_enabled": False,
            "word_detection_agent_candidate_cue_sound": "",
            "always_pre_roll_enabled": False,
            "silence_reset_timeout_ms": 2100,
        }
        base.update(overrides)
        return base

    assert voice_mode._clean_stt_policy({"speech_aggregation_timeout_ms": 83}) == expected()
    assert voice_mode._clean_stt_policy({"speech_aggregation_timeout_ms": 999}) == expected(
        speech_aggregation_timeout_ms=300
    )
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 1}) == expected(
        vad_reset_timeout_ms=0
    )
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 126}) == expected(
        vad_reset_timeout_ms=150
    )
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 400}) == expected(
        vad_reset_timeout_ms=400
    )
    assert voice_mode._clean_stt_policy({"vad_reset_timeout_ms": 9999}) == expected(
        vad_reset_timeout_ms=2000
    )
    assert voice_mode._clean_stt_policy({"pre_roll_frames": 3}) == expected(pre_roll_frames=3)
    assert voice_mode._clean_stt_policy({"num_pre_roll_frames": 99}) == expected(pre_roll_frames=4)
    assert voice_mode._clean_stt_policy({"num_pre_roll": 0}) == expected()
    assert voice_mode._clean_stt_policy({"silence_reset_timeout_ms": 2000}) == expected()
    assert voice_mode._clean_stt_policy({"silence_reset_timeout_ms": 1}) == expected(
        silence_reset_timeout_ms=0
    )
    assert voice_mode._clean_stt_policy(
        {
            "word_detection_match_cue_enabled": "yes",
            "word_detection_match_cue_sound": "sounds/high.mp3",
            "word_detection_payload0_timeout_sound_enabled": "1",
            "word_detection_payload0_timeout_sound_path": "sounds/low.mp3",
            "word_detection_agent_candidate_sound_enabled": True,
            "word_detection_agent_candidate_sound_path": "sounds/candidate.mp3",
        }
    ) == expected(
        word_detection_match_cue_enabled=True,
        word_detection_match_cue_sound="sounds/high.mp3",
        word_detection_payload0_timeout_cue_enabled=True,
        word_detection_payload0_timeout_cue_sound="sounds/low.mp3",
        word_detection_agent_candidate_cue_enabled=True,
        word_detection_agent_candidate_cue_sound="sounds/candidate.mp3",
    )
    assert voice_mode._clean_stt_policy({"silero_vad_enabled": True})["silero_vad_enabled"] is True
    assert voice_mode._clean_stt_policy({"silero_enabled": "yes"})["silero_vad_enabled"] is True
    assert (
        voice_mode._clean_stt_policy({"always_pre_roll_enabled": True})["always_pre_roll_enabled"]
        is True
    )
    assert voice_mode._clean_stt_policy({"always_pre_roll": "1"})["always_pre_roll_enabled"] is True


def test_voice_mode_stt_policy_update_preserves_unspecified_values():
    current = voice_mode._clean_stt_policy(
        {
            "word_detection_payload0_timeout_ms": 2100,
            "silero_vad_enabled": True,
            "word_detection_match_cue_enabled": True,
            "word_detection_match_cue_sound": "sounds/tos_chirp_5.wav",
        }
    )

    updated = voice_mode._clean_stt_policy_update(
        current,
        {
            "vad_reset_timeout_ms": 126,
        },
    )

    assert updated["vad_reset_timeout_ms"] == 150
    assert updated["word_detection_payload0_timeout_ms"] == 2100
    assert updated["silero_vad_enabled"] is True
    assert updated["word_detection_match_cue_enabled"] is True
    assert updated["word_detection_match_cue_sound"] == "sounds/tos_chirp_5.wav"


def test_voice_mode_default_stt_reset_payload_detects_stale_combined_wake_save():
    current = voice_mode._clean_stt_policy(
        {
            "word_detection_payload0_timeout_ms": 2100,
            "silero_vad_enabled": True,
        }
    )

    assert voice_mode._is_default_stt_reset_payload(voice_mode._clean_stt_policy({}), current)
    assert not voice_mode._is_default_stt_reset_payload(
        {"word_detection_payload0_timeout_ms": 0},
        voice_mode._clean_stt_policy({}),
    )
    assert voice_mode._is_default_stt_reset_payload(
        {"word_detection_payload0_timeout_ms": 0},
        current,
    )


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
    assert voice_mode._clean_active_browser_command_action("chat room") == "open_matrix_chat_room"
    assert (
        voice_mode._clean_active_browser_command_action("matrix-chat-room")
        == "open_matrix_chat_room"
    )
    assert voice_mode._clean_active_browser_command_action("modal") == "open_modal"
    assert voice_mode._clean_active_browser_command_action("doc") == "open_doc"
    assert voice_mode._clean_active_browser_command_action("document") == "open_doc"
    assert voice_mode._clean_active_browser_command_action("fn") == "menu_function"
    assert voice_mode._clean_active_browser_command_action("menu-fn") == "menu_function"
    assert voice_mode._clean_active_browser_command_action("synthesis") == "open_synthesis"
    assert voice_mode._clean_active_browser_command_action("probes") == "open_probes"
    assert voice_mode._clean_active_browser_command_action("settings") == "open_settings"
    assert voice_mode._clean_active_browser_command_action("selector") == "selector_action"
    assert voice_mode._clean_active_browser_command_action("body shade") == "set_body_shade"
    assert voice_mode._clean_active_browser_command_action("shade-up") == "set_body_shade"
    assert (
        voice_mode._clean_active_browser_command_action("kanban lane update")
        == "kanban_external_refresh"
    )


def test_active_browser_kanban_snapshot_exposes_automation_policy_fields():
    cleaned = voice_mode._clean_active_browser_kanban(
        {
            "automation_status_loaded": True,
            "automation_status_loading": False,
            "automation_status_error": "ok",
            "automation_recent_decisions": 3,
            "automation_review_processor_status": "decision-ledger-ready",
            "automation_review_queue_length": 7,
            "automation_review_active_count": 2,
            "automation_review_timeout_count": 1,
            "automation_review_superseded_count": 4,
            "automation_review_marker_count": 9,
            "automation_busy_action": "scan-reviews",
            "automation_last_result": "Review scan complete.",
            "automation_commit_link_health_ok": True,
            "automation_output_contract_schema": "xarta.kanban.review_processor.output_contract.v1",
            "automation_output_contract_types": 4,
            "automation_processing_policy_schema": "xarta.kanban.review_processor.policy.v1",
            "automation_processing_policy_active_mode": "cloud-first",
            "automation_processing_policy_local_gate": "structured-job-packets-required",
        }
    )
    assert cleaned["automation_status_loaded"] is True
    assert cleaned["automation_status_loading"] is False
    assert cleaned["automation_status_error"] == "ok"
    assert cleaned["automation_recent_decisions"] == 3
    assert cleaned["automation_review_processor_status"] == "decision-ledger-ready"
    assert cleaned["automation_review_queue_length"] == 7
    assert cleaned["automation_review_active_count"] == 2
    assert cleaned["automation_review_timeout_count"] == 1
    assert cleaned["automation_review_superseded_count"] == 4
    assert cleaned["automation_review_marker_count"] == 9
    assert cleaned["automation_busy_action"] == "scan-reviews"
    assert cleaned["automation_last_result"] == "Review scan complete."
    assert cleaned["automation_commit_link_health_ok"] is True
    assert (
        cleaned["automation_output_contract_schema"]
        == "xarta.kanban.review_processor.output_contract.v1"
    )
    assert cleaned["automation_output_contract_types"] == 4
    assert (
        cleaned["automation_processing_policy_schema"] == "xarta.kanban.review_processor.policy.v1"
    )
    assert cleaned["automation_processing_policy_active_mode"] == "cloud-first"
    assert cleaned["automation_processing_policy_local_gate"] == "structured-job-packets-required"


def test_voice_dev_vad_detector_actions_are_allowed():
    assert voice_mode._clean_dev_command_action("set silero vad") == "set_silero_vad"
    assert voice_mode._clean_dev_command_action("set-vad-detector") == "set_vad_detector"
    assert voice_mode._clean_dev_command_action("set auto pre roll") == "set_auto_pre_roll"
    assert voice_mode._clean_dev_command_action("set always pre roll") == "set_always_pre_roll"
    assert voice_mode._clean_dev_command_action("set num pre roll") == "set_num_pre_roll"
    assert (
        voice_mode._clean_dev_command_action("set-noise-threshold-db") == "set_noise_threshold_db"
    )
    assert voice_mode._clean_dev_command_action("set vad pre roll db") == "set_vad_pre_roll_db"
    assert (
        voice_mode._clean_dev_command_action("set word detection match cue")
        == "set_word_detection_match_cue"
    )
    assert "set_silero_vad" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_vad_detector" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_auto_pre_roll" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_always_pre_roll" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_pre_roll_frames" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_num_pre_roll" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_num_pre_roll_frames" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_noise_threshold" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_noise_threshold_db" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_vad_pre_roll" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_vad_pre_roll_db" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_vad_pre_roll_threshold" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_word_detection_match_cue" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_word_detection_payload0_timeout_cue" in voice_mode._DEV_COMMAND_ACTIONS
    assert "set_word_detection_agent_candidate_cue" in voice_mode._DEV_COMMAND_ACTIONS


def test_active_browser_command_parameters_are_sanitized():
    assert voice_mode._clean_active_browser_event_kind("tap") == "click"
    assert voice_mode._clean_active_browser_event_kind("double tap") == "double_click"
    assert voice_mode._clean_active_browser_event_kind("long-press") == "long_press"
    assert voice_mode._clean_active_browser_event_kind("something else") == "click"
    assert voice_mode._clean_active_browser_body_shade("raise") == "up"
    assert voice_mode._clean_active_browser_body_shade("lower") == "down"
    assert voice_mode._clean_active_browser_body_shade("flip") == "toggle"
    assert voice_mode._clean_active_browser_body_shade("unexpected") == "up"
    assert voice_mode._clean_active_browser_body_shade(None) == ""
    assert (
        voice_mode._clean_active_browser_modal_id("vad-dev-modal<script>") == "vad-dev-modalscript"
    )
    assert voice_mode._clean_active_browser_selector_action("API Key") == "api-key"
    assert voice_mode._clean_active_browser_group("Settings Panel!") == "settings-panel"
    assert (
        voice_mode._clean_active_browser_page_id("manual-links-page:ABC_123<script>")
        == "manual-links-page:ABC_123script"
    )
    assert (
        voice_mode._clean_active_browser_menu_item_id("chat-fn-vad-dev<script>")
        == "chat-fn-vad-devscript"
    )
    assert (
        voice_mode._clean_active_browser_fn_key("nod.backupColumns<script>")
        == "nod.backupColumnsscript"
    )
    assert voice_mode._clean_active_browser_diagnostic_sources("search gpu provenance") == [
        "personal_search",
        "gpu_activity_sound",
        "personal_graph",
    ]
    assert len(voice_mode._clean_active_browser_group("A" * 200)) == 80
    assert len(voice_mode._clean_active_browser_page_id("p" * 220)) == 160
    assert len(voice_mode._clean_active_browser_menu_item_id("m" * 220)) == 160
    assert len(voice_mode._clean_active_browser_fn_key("f" * 220)) == 160


def test_active_browser_automation_does_not_fabricate_empty_last_command():
    report = voice_mode._clean_active_browser_automation_report({"last_command": {}})

    assert report["last_command"] == {}


def test_active_browser_automation_preserves_personal_search_graph_surfaces():
    report = voice_mode._clean_active_browser_automation_report(
        {
            "surfaces": {
                "personal_search": {
                    "surfaces": {
                        "diary": {
                            "query": "step",
                            "mode": "personal",
                            "record_type": "diary",
                            "loading": False,
                            "error": "",
                            "result_count": 8,
                            "first_result": "personal_events:evt-proof",
                            "subsystems": {"vector": {"status": "ok"}},
                        }
                    },
                    "graph": {
                        "open": True,
                        "source_ref": "personal_events:evt-proof",
                        "title": "Proof event",
                        "link_count": 4,
                        "first_link": "git_commit:abc123",
                    },
                },
                "personal_graph": {
                    "open": True,
                    "source_ref": "personal_events:evt-proof",
                    "title": "Proof event",
                    "link_count": 4,
                    "first_link": "git_commit:abc123",
                },
            }
        }
    )

    personal_search = report["surfaces"]["personal_search"]
    assert personal_search["surfaces"]["diary"]["result_count"] == 8
    assert personal_search["surfaces"]["diary"]["subsystems"]["vector"]["status"] == "ok"
    assert personal_search["graph"]["link_count"] == 4
    assert report["surfaces"]["personal_graph"]["first_link"] == "git_commit:abc123"


def test_active_browser_automation_compacts_menu_lists_and_preserves_matrix_metrics():
    report = voice_mode._clean_active_browser_automation_report(
        {
            "menus": [
                {
                    "group": "settings",
                    "pages": [
                        {"id": f"page-{index}", "label": f"Page {index}"} for index in range(30)
                    ],
                    "function_items": [
                        {"id": f"fn-{index}", "fn": f"settings.fn{index}"} for index in range(30)
                    ],
                }
            ],
            "current_menu": {
                "group": "settings",
                "active_id": "matrix-chat",
                "pages": [{"id": "matrix-chat", "label": "Chat"}],
                "current_functions": [{"id": "chat-fn-refresh", "fn": "chat.refresh"}],
            },
            "surfaces": {
                "matrix_chat": {
                    "server_id": "tb1",
                    "loading": False,
                    "active_room_id": "!room:matrix.example.test",
                    "known_message_count": 60,
                    "last_refresh": {
                        "server_id": "tb1",
                        "total_ms": 1200,
                        "status_ms": 40,
                        "rooms_ms": 80,
                        "messages_ms": 900,
                        "ok": True,
                    },
                    "last_message_load": {
                        "server_id": "tb1",
                        "room_id": "!room:matrix.example.test",
                        "total_ms": 900,
                        "visible_message_count": 60,
                        "ok": True,
                    },
                    "inline_images": {
                        "scheduled": 24,
                        "requested": 3,
                        "loaded": 3,
                        "dom_total": 24,
                    },
                }
            },
        }
    )

    assert report["menus"][0]["page_count"] == 30
    assert report["menus"][0]["function_count"] == 30
    assert report["menus"][0]["pages"] == []
    assert report["menus"][0]["function_items"] == []
    assert report["current_menu"]["pages"][0]["id"] == "matrix-chat"
    assert report["current_menu"]["current_functions"][0]["fn"] == "chat.refresh"
    matrix_chat = report["surfaces"]["matrix_chat"]
    assert matrix_chat["server_id"] == "tb1"
    assert matrix_chat["last_refresh"]["messages_ms"] == 900
    assert matrix_chat["last_message_load"]["visible_message_count"] == 60
    assert matrix_chat["inline_images"]["requested"] == 3


def test_active_browser_view_compacts_history_clients_and_reports():
    heavy_automation = {
        "menus": [
            {
                "group": "settings",
                "pages": [{"id": f"page-{index}", "label": f"Page {index}"} for index in range(80)],
                "function_items": [
                    {"id": f"fn-{index}", "fn": f"settings.fn{index}"} for index in range(80)
                ],
            }
        ]
    }
    state = {
        "active": {"browser_id": "browser-a", "tab_id": "tab-a"},
        "browser_views": {
            "browser-a::tab-a": {
                "browser_id": "browser-a",
                "browser_label": "Active",
                "tab_id": "tab-a",
                "reported_at": 100,
                "page": {"group": "settings", "tab": "matrix-chat", "ready": True},
                "frontend": {"asset_version": "dev-test"},
                "automation": heavy_automation,
            },
            "browser-b::tab-b": {
                "browser_id": "browser-b",
                "browser_label": "Other",
                "tab_id": "tab-b",
                "reported_at": 90,
                "page": {"group": "kanban", "tab": "kanban", "ready": True},
                "frontend": {"asset_version": "old"},
                "automation": heavy_automation,
            },
        },
    }

    public = voice_mode._public_active_browser_view(state)

    assert public["view"]["automation"]["menus"][0]["pages"] == []
    assert "automation" not in public["clients"][0]
    assert "automation" not in public["reports"][0]
    assert public["clients"][0]["browser_id"] == "browser-a"
    assert public["reports"][0]["page"]["tab"] == "matrix-chat"
    assert len(json.dumps(public).encode("utf-8")) < 50000


def test_active_browser_command_rejects_unsupported_actions():
    response = asyncio.run(
        voice_mode.active_browser_command(
            voice_mode.ActiveBrowserCommandBody(action="delete everything")
        )
    )

    assert response.status_code == 400
    assert (
        json.loads(response.body)["detail"]
        == "Unsupported active browser action: delete_everything"
    )


def test_active_browser_command_accepts_matrix_chat_room_payload(tmp_path, monkeypatch):
    state_path = tmp_path / "blueprints-voice-mode.json"
    state_path.write_text(
        json.dumps(
            {
                "active": {
                    "browser_id": "browser-a",
                    "tab_id": "tab-a",
                    "activated_at": 10,
                },
                "browser_views": {},
                "revision": 1,
                "updated_at": 10,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(voice_mode, "_STATE_PATH", state_path)
    monkeypatch.setattr(voice_mode, "_STATE_CACHE", None)
    captured = {}

    class Published:
        def model_dump(self):
            return {"event_id": "active-browser-command-test"}

    async def fake_publish(event, **kwargs):
        captured["event"] = event
        return Published()

    monkeypatch.setattr(voice_mode, "publish_event", fake_publish)

    result = asyncio.run(
        voice_mode.active_browser_command(
            voice_mode.ActiveBrowserCommandBody(
                action="chat room",
                group="settings",
                page_id="matrix-chat",
                server_id="VPS",
                room_hint="Shared Bridge",
            )
        )
    )

    assert result["ok"] is True
    assert result["payload"]["action"] == "open_matrix_chat_room"
    assert result["payload"]["target_browser_id"] == "browser-a"
    assert result["payload"]["target_tab_id"] == "tab-a"
    assert result["payload"]["server_id"] == "vps"
    assert result["payload"]["room_hint"] == "Shared Bridge"


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
            "pointer": {
                "primary": "fine",
                "any": "fine",
                "coarse": False,
                "fine": True,
                "maxTouchPoints": 0,
            },
        },
        frontend={"app": "fallback-ui", "asset_version": "dev-test"},
        body_shade={
            "available": True,
            "is_up": True,
            "active_panel_id": "tab-docs",
            "handle_present": True,
        },
        layout={
            "active_panel_id": "tab-diary",
            "root": {
                "scroll_element": "HTML",
                "html_overflow_y": "hidden",
                "body_overflow_y": "hidden",
                "html_has_managed_scroll_tab": True,
                "body_has_managed_scroll_tab": True,
                "window_scroll_y": 0,
                "scroll_height": 640,
                "client_height": 640,
                "body_scroll_height": 640,
                "html_scroll_height": 640,
                "ignored": "raw",
            },
            "rects": {
                "main": {
                    "left": 0,
                    "top": 0,
                    "right": 1024,
                    "bottom": 640,
                    "width": 1024,
                    "height": 640,
                },
                "menu_nav": {
                    "left": 16,
                    "top": 77,
                    "right": 1008,
                    "bottom": 121,
                    "width": 992,
                    "height": 44,
                },
                "panel": {
                    "left": 16,
                    "top": 137,
                    "right": 1008,
                    "bottom": 640,
                    "width": 992,
                    "height": 503,
                },
                "handle": {
                    "left": 0,
                    "top": 566.25,
                    "right": 1024,
                    "bottom": 586.25,
                    "width": 1024,
                    "height": 20,
                },
                "shell": {
                    "left": 16,
                    "top": 586.25,
                    "right": 1008,
                    "bottom": 640,
                    "width": 992,
                    "height": 53.75,
                },
                "ignored": {"left": 1},
            },
            "shell": {
                "overflow_y": "auto",
                "client_height": 54,
                "scroll_height": 480,
                "scrollbar_active": True,
                "ignored": "raw",
            },
            "alignment": {
                "panel_left_delta_from_menu": 0,
                "panel_right_delta_from_menu": 0,
                "ignored": 99,
            },
            "ignored": {"raw": True},
        },
        tts={
            "client_available": True,
            "client": {
                "status": "playing",
                "utterance_id": "utt-live-proof",
                "audio_context_state": "running",
            },
            "announcer_available": True,
            "announcer": {
                "state": "ANNOUNCING",
                "last_speech": {
                    "status": "speaking",
                    "event": {"utterance_id": "utt-live-proof"},
                },
            },
        },
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
            "selector_actions": [
                {"action": "settings", "label": "Settings", "bridge_group": "settings"}
            ],
            "surfaces": {
                "calendar": {
                    "loaded": True,
                    "loading": False,
                    "status": "ready",
                    "local_date": "2026-06-18",
                    "range_start": "2026-06-16",
                    "range_end": "2026-06-22",
                    "mode": "week",
                    "source_filter": "calendar",
                    "event_count": 3,
                    "total_count": 8,
                    "manual_calendar_count": 2,
                    "selection_type": "event",
                    "selection_label": "Planning block",
                    "last_write_event_id": "calendar-2026-06-18-proof",
                    "error": "",
                    "ignored": {"nested": "raw"},
                },
                "todo": {
                    "loaded": True,
                    "loading": False,
                    "status": "ready",
                    "mode": "work",
                    "task_count": 4,
                    "total_count": 6,
                    "open_count": 2,
                    "blocked_count": 1,
                    "done_count": 1,
                    "source_counts": {"manual-task": 3, "work-management": 1},
                    "selection_status": "open",
                    "selection_label": "Task proof",
                    "last_write_task_id": "task-2026-06-18-proof",
                    "error": "",
                    "ignored": "raw",
                },
                "kanban": {
                    "loaded": True,
                    "loading": False,
                    "status": "ready",
                    "current_parent_id": "",
                    "breadcrumb_depth": 2,
                    "breadcrumb_labels": ["Root", "Proof child", "ignored extra"],
                    "column_count": 5,
                    "item_count": 1,
                    "selected_item_id": "work-abc",
                    "selected_state": "todo",
                    "detail_open": True,
                    "detail_item_id": "work-abc",
                    "detail_state": "todo",
                    "depth_remaining": 10,
                    "child_count": 2,
                    "link_count": 1,
                    "blocker_count": 1,
                    "scoped_open": True,
                    "scoped_kind": "issues",
                    "scoped_scope": "descendants",
                    "scoped_view": "grouped",
                    "scoped_item_id": "work-abc",
                    "scoped_count": 3,
                    "scoped_group_count": 2,
                    "last_write_item_id": "work-last",
                    "last_write_issue_id": "issue-last",
                    "last_write_todo_id": "todo-last",
                    "last_promoted_issue_item_id": "work-promoted-issue",
                    "last_promoted_todo_item_id": "work-promoted-todo",
                    "last_write_link_id": "work-link-last",
                    "last_write_blocker_id": "blocker-last",
                    "card_fsm": {
                        "state": "idle",
                        "lastEvent": "openRootBoard",
                        "itemId": "work-abc",
                        "raw": "ignored",
                    },
                    "rollup_total": 1,
                    "issue_count": 1,
                    "todo_count": 1,
                    "backups_loaded": True,
                    "backup_count": 2,
                    "backups_loading": False,
                    "backups_error": "",
                    "backup_busy_action": "validate",
                    "backup_importing_filename": "",
                    "backup_last_result": "Backup package is valid.",
                    "error": "",
                    "ignored": "raw",
                },
            },
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
    assert view["automation"]["menus"][0]["page_count"] == 1
    assert view["automation"]["menus"][0]["function_count"] == 1
    assert view["automation"]["menus"][0]["pages"] == []
    assert view["automation"]["menus"][0]["function_items"] == []
    assert view["automation"]["selector_actions"][0]["action"] == "settings"
    assert view["automation"]["surfaces"]["calendar"] == {
        "loaded": True,
        "loading": False,
        "status": "ready",
        "local_date": "2026-06-18",
        "range_start": "2026-06-16",
        "range_end": "2026-06-22",
        "mode": "week",
        "source_filter": "calendar",
        "event_count": 3,
        "total_count": 8,
        "manual_calendar_count": 2,
        "selection_type": "event",
        "selection_label": "Planning block",
        "last_write_event_id": "calendar-2026-06-18-proof",
        "error": "",
    }
    assert view["automation"]["surfaces"]["todo"] == {
        "loaded": True,
        "loading": False,
        "status": "ready",
        "mode": "work",
        "task_count": 4,
        "total_count": 6,
        "open_count": 2,
        "blocked_count": 1,
        "done_count": 1,
        "source_counts": {"manual-task": 3, "work-management": 1},
        "selection_status": "open",
        "selection_label": "Task proof",
        "last_write_task_id": "task-2026-06-18-proof",
        "error": "",
    }
    assert view["automation"]["surfaces"]["kanban"] == {
        "loaded": True,
        "loading": False,
        "status": "ready",
        "current_parent_id": "",
        "breadcrumb_depth": 2,
        "breadcrumb_labels": ["Root", "Proof child", "ignored extra"],
        "column_count": 5,
        "item_count": 1,
        "selected_item_id": "work-abc",
        "selected_state": "todo",
        "detail_open": True,
        "detail_panel_open": False,
        "detail_item_id": "work-abc",
        "detail_state": "todo",
        "editing": False,
        "draft_dirty": False,
        "external_refresh_skipped": False,
        "external_refresh_skipped_reason": "",
        "depth_remaining": 10,
        "child_count": 2,
        "link_count": 1,
        "blocker_count": 1,
        "scoped_open": True,
        "scoped_kind": "issues",
        "scoped_scope": "descendants",
        "scoped_view": "grouped",
        "scoped_item_id": "work-abc",
        "scoped_count": 3,
        "scoped_group_count": 2,
        "last_write_item_id": "work-last",
        "last_write_issue_id": "issue-last",
        "last_write_todo_id": "todo-last",
        "last_promoted_issue_item_id": "work-promoted-issue",
        "last_promoted_todo_item_id": "work-promoted-todo",
        "last_write_link_id": "work-link-last",
        "last_write_blocker_id": "blocker-last",
        "card_fsm": {
            "state": "idle",
            "lastEvent": "openRootBoard",
            "itemId": "work-abc",
        },
        "rollup_total": 1,
        "issue_count": 1,
        "todo_count": 1,
        "postgres_loaded": False,
        "postgres_export_count": 0,
        "postgres_loading": False,
        "postgres_error": "",
        "postgres_busy_action": "",
        "postgres_importing_filename": "",
        "postgres_last_result": "",
        "postgres_role": "",
        "postgres_row_count": 0,
        "automation_status_loaded": False,
        "automation_status_loading": False,
        "automation_status_error": "",
        "automation_recent_decisions": 0,
        "automation_review_processor_status": "",
        "automation_review_queue_length": 0,
        "automation_review_active_count": 0,
        "automation_review_timeout_count": 0,
        "automation_review_superseded_count": 0,
        "automation_review_marker_count": 0,
        "automation_busy_action": "",
        "automation_last_result": "",
        "automation_commit_link_health_ok": True,
        "automation_output_contract_schema": "",
        "automation_output_contract_types": 0,
        "automation_processing_policy_schema": "",
        "automation_processing_policy_active_mode": "",
        "automation_processing_policy_local_gate": "",
        "error": "",
    }
    assert view["body_shade"] == {
        "available": True,
        "is_up": True,
        "state": "up",
        "active_panel_id": "tab-docs",
        "handle_present": True,
    }
    assert view["layout"]["active_panel_id"] == "tab-diary"
    assert view["layout"]["root"] == {
        "scroll_element": "HTML",
        "html_overflow_y": "hidden",
        "body_overflow_y": "hidden",
        "html_has_managed_scroll_tab": True,
        "body_has_managed_scroll_tab": True,
        "window_scroll_y": 0.0,
        "scroll_height": 640,
        "client_height": 640,
        "body_scroll_height": 640,
        "html_scroll_height": 640,
    }
    assert view["layout"]["rects"]["menu_nav"]["left"] == 16.0
    assert view["layout"]["rects"]["panel"]["right"] == 1008.0
    assert view["layout"]["shell"] == {
        "overflow_y": "auto",
        "client_height": 54,
        "scroll_height": 480,
        "scrollbar_active": True,
    }
    assert view["layout"]["alignment"] == {
        "panel_left_delta_from_menu": 0.0,
        "panel_right_delta_from_menu": 0.0,
    }
    assert view["tts"]["client_available"] is True
    assert view["tts"]["client"]["status"] == "playing"
    assert view["tts"]["client"]["utterance_id"] == "utt-live-proof"
    assert view["tts"]["announcer"]["last_speech"]["event"]["utterance_id"] == "utt-live-proof"


def test_active_browser_view_exposes_automation_defaults():
    public = voice_mode._public_active_browser_view({"active": None, "browser_views": {}})

    assert public["automation"]["default_step_timeout_seconds"] == 10
    assert public["automation"]["minimum_step_timeout_seconds"] == 1
    assert public["automation"]["maximum_step_timeout_seconds"] == 120


def test_active_browser_view_offloads_payload_build_and_reports_metrics(tmp_path, monkeypatch):
    state_path = tmp_path / "blueprints-voice-mode.json"
    monkeypatch.setattr(voice_mode, "_STATE_PATH", state_path)
    monkeypatch.setattr(voice_mode, "_STATE_CACHE", None)
    state_path.write_text(
        json.dumps(
            {
                "active": {"browser_id": "browser-a", "tab_id": "tab-1"},
                "browser_views": {
                    "browser-a::tab-1": {
                        "browser_id": "browser-a",
                        "tab_id": "tab-1",
                        "reported_at": 100,
                        "page": {"group": "settings", "tab": "matrix-chat", "ready": True},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    calls = []

    async def fake_to_thread(func, *args, **kwargs):
        calls.append((func.__name__, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(voice_mode.asyncio, "to_thread", fake_to_thread)

    public = asyncio.run(voice_mode.active_browser_view(metrics=True))

    assert calls[0][0] == "_active_browser_view_locked_sync"
    assert public["view"]["page"]["tab"] == "matrix-chat"
    assert public["server_metrics"]["schema"] == "xarta.active_browser_view.metrics.v1"
    assert public["server_metrics"]["state_read_and_payload_seconds"] >= 0


def test_browser_view_post_returns_small_ack_and_coalesces_disk_persistence(tmp_path, monkeypatch):
    state_path = tmp_path / "blueprints-voice-mode.json"
    debug_path = tmp_path / "blueprints-wake-dev-debug.json"
    monkeypatch.setattr(voice_mode, "_STATE_PATH", state_path)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_PATH", debug_path)
    monkeypatch.setattr(voice_mode, "_STATE_CACHE", None)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_CACHE", None)
    monkeypatch.setattr(voice_mode, "_STATE_LAST_PERSISTED_AT", 0.0)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_LAST_PERSISTED_AT", 0.0)
    monkeypatch.setattr(voice_mode, "_BROWSER_VIEW_TELEMETRY_PERSIST_INTERVAL_SECONDS", 60.0)
    monkeypatch.setattr(voice_mode, "_VOICE_MODE_HOT_POST_FULL_RESPONSE", False)

    first = asyncio.run(
        voice_mode.update_browser_view(
            voice_mode.BrowserViewBody(
                browser_id="browser-a",
                tab_id="tab-1",
                page={"group": "settings", "tab": "matrix-chat", "ready": True},
            )
        )
    )

    assert first["ok"] is True
    assert first["stored"] is True
    assert first["persisted"] is True
    assert "reports" not in first

    second = asyncio.run(
        voice_mode.update_browser_view(
            voice_mode.BrowserViewBody(
                browser_id="browser-a",
                tab_id="tab-1",
                page={"group": "settings", "tab": "agents", "ready": True},
            )
        )
    )

    assert second["ok"] is True
    assert second["persisted"] is False
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    persisted_report = next(iter(persisted["browser_views"].values()))
    assert persisted_report["page"]["tab"] == "matrix-chat"

    live = asyncio.run(voice_mode.active_browser_view())
    assert live["view"]["page"]["tab"] == "agents"


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


def test_publish_kanban_external_refresh_targets_safe_kanban_tabs(tmp_path, monkeypatch):
    state_path = tmp_path / "blueprints-voice-mode.json"
    state_path.write_text(
        json.dumps(
            {
                "active": {"browser_id": "safe-browser", "tab_id": "tab-1"},
                "browser_views": {
                    "safe-browser::tab-1": {
                        "browser_id": "safe-browser",
                        "tab_id": "tab-1",
                        "reported_at": 95,
                        "page": {"group": "kanban", "tab": "kanban"},
                        "automation": {
                            "surfaces": {
                                "kanban": {
                                    "loaded": True,
                                    "editing": False,
                                    "draft_dirty": False,
                                    "scoped_open": False,
                                }
                            }
                        },
                    },
                    "dirty-browser::tab-2": {
                        "browser_id": "dirty-browser",
                        "tab_id": "tab-2",
                        "reported_at": 95,
                        "page": {"group": "kanban", "tab": "kanban"},
                        "automation": {
                            "surfaces": {
                                "kanban": {
                                    "loaded": True,
                                    "editing": True,
                                    "draft_dirty": True,
                                    "scoped_open": False,
                                }
                            }
                        },
                    },
                    "chat-browser::tab-3": {
                        "browser_id": "chat-browser",
                        "tab_id": "tab-3",
                        "reported_at": 95,
                        "page": {"group": "settings", "tab": "matrix-chat"},
                        "automation": {"surfaces": {"kanban": {"loaded": False}}},
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(voice_mode, "_STATE_PATH", state_path)
    monkeypatch.setattr(voice_mode, "_STATE_CACHE", None)
    monkeypatch.setattr(voice_mode.time, "time", lambda: 100.0)
    published = []

    async def fake_publish(event, **kwargs):
        published.append(event.payload)
        return event

    monkeypatch.setattr(voice_mode, "publish_event", fake_publish)

    result = asyncio.run(
        voice_mode.publish_kanban_external_refresh_commands(
            item_id="item-1",
            parent_item_id="parent-1",
            state_id="done",
            actor="test",
            source_surface="test-suite",
            max_age_seconds=30,
        )
    )

    assert result["published_count"] == 1
    assert result["skipped_count"] == 2
    assert {item["reason"] for item in result["skipped"]} == {"editing", "not-kanban"}
    assert published[0]["action"] == "kanban_external_refresh"
    assert published[0]["target_browser_id"] == "safe-browser"
    assert published[0]["target_tab_id"] == "tab-1"
    assert published[0]["item_id"] == "item-1"
    assert published[0]["parent_item_id"] == "parent-1"
    assert published[0]["state_id"] == "done"


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


def test_voice_mode_wake_dev_debug_prefers_active_browser_report():
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
            "other-browser": {
                "browser_id": "other-browser",
                "fsm_state": "ARMED_IDLE",
                "reported_at": 1,
            },
            "active-browser": {
                "browser_id": "active-browser",
                "fsm_state": "CAPTURING",
                "queues": {"input_queue": [], "message_queue": [{"text": "hello"}]},
                "reported_at": 2,
            },
        }
    }

    public = voice_mode._public_wake_dev_debug(state, debug)

    assert public["ok"] is True
    assert public["has_debug"] is True
    assert public["debug"]["browser_id"] == "active-browser"
    assert public["debug"]["fsm_state"] == "CAPTURING"
    assert public["debug"]["queues"]["message_queue"][0]["text"] == "hello"


def test_voice_mode_dev_status_surface_prefers_active_browser_matching_surface():
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
            "active-browser:vad_dev": {
                "browser_id": "active-browser",
                "surface": "vad_dev",
                "status": "active VAD report",
                "reported_at": 20,
            },
            "active-browser:wake_dev": {
                "browser_id": "active-browser",
                "surface": "wake_dev",
                "status": "active Wake report",
                "reported_at": 10,
            },
            "other-browser:wake_dev": {
                "browser_id": "other-browser",
                "surface": "wake_dev",
                "status": "newer stale Wake report",
                "reported_at": 30,
            },
        }
    }

    public = voice_mode._public_wake_dev_debug(state, debug, surface="wake_dev")

    assert public["ok"] is True
    assert public["has_debug"] is True
    assert public["debug"]["browser_id"] == "active-browser"
    assert public["debug"]["surface"] == "wake_dev"
    assert public["debug"]["status"] == "active Wake report"
    assert public["debug"]["authoritative_browser_active"] is True


def test_voice_mode_dev_status_surface_can_select_browser_specific_report():
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
            "active-browser:wake_dev": {
                "browser_id": "active-browser",
                "surface": "wake_dev",
                "status": "active Wake report",
                "reported_at": 10,
            },
            "other-browser:wake_dev": {
                "browser_id": "other-browser",
                "surface": "wake_dev",
                "status": "requested Wake report",
                "reported_at": 20,
            },
        }
    }

    public = voice_mode._public_wake_dev_debug(
        state,
        debug,
        surface="wake_dev",
        browser_id="other-browser",
    )

    assert public["debug"]["browser_id"] == "other-browser"
    assert public["debug"]["status"] == "requested Wake report"
    assert public["debug"]["authoritative_browser_active"] is False


def test_voice_mode_dev_status_surface_prefers_active_tab_report():
    state = {
        "active": {
            "browser_id": "active-browser",
            "tab_id": "tab-1",
            "browser_label": "Browser on Win32",
            "stt_enabled": True,
            "stt_mode": "wake_to_talk",
            "tts_enabled": True,
        }
    }
    debug = {
        "reports": {
            "active-browser:tab-2:wake_dev": {
                "browser_id": "active-browser",
                "tab_id": "tab-2",
                "surface": "wake_dev",
                "status": "newer wrong tab",
                "reported_at": 30,
            },
            "active-browser:tab-1:wake_dev": {
                "browser_id": "active-browser",
                "tab_id": "tab-1",
                "surface": "wake_dev",
                "status": "active tab",
                "reported_at": 20,
            },
        }
    }

    public = voice_mode._public_wake_dev_debug(state, debug, surface="wake_dev")

    assert public["debug"]["tab_id"] == "tab-1"
    assert public["debug"]["status"] == "active tab"


def test_voice_mode_dev_status_post_returns_small_ack_and_coalesces_disk_persistence(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "blueprints-voice-mode.json"
    debug_path = tmp_path / "blueprints-wake-dev-debug.json"
    monkeypatch.setattr(voice_mode, "_STATE_PATH", state_path)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_PATH", debug_path)
    monkeypatch.setattr(voice_mode, "_STATE_CACHE", None)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_CACHE", None)
    monkeypatch.setattr(voice_mode, "_STATE_LAST_PERSISTED_AT", 0.0)
    monkeypatch.setattr(voice_mode, "_WAKE_DEV_DEBUG_LAST_PERSISTED_AT", 0.0)
    monkeypatch.setattr(voice_mode, "_DEV_STATUS_TELEMETRY_PERSIST_INTERVAL_SECONDS", 60.0)
    monkeypatch.setattr(voice_mode, "_VOICE_MODE_HOT_POST_FULL_RESPONSE", False)

    first = asyncio.run(
        voice_mode.voice_mode_update_dev_status(
            voice_mode.WakeDevDebugBody(
                browser_id="browser-a",
                tab_id="tab-1",
                surface="vad_dev",
                mode="vad_rearm",
                status="first",
                transcript="Computer",
                snapshot={"fsm_state": "VAD_REARM_STT_ARMED"},
            )
        )
    )

    assert first["ok"] is True
    assert first["stored"] is True
    assert first["persisted"] is True
    assert first["surface"] == "vad_dev"
    assert "debug" not in first
    assert (
        voice_mode._dev_debug_report_key(
            {"browser_id": "browser-a", "tab_id": "tab-1", "surface": "vad_dev"}
        )
        == "browser-a:tab-1:vad_dev"
    )

    second = asyncio.run(
        voice_mode.voice_mode_update_dev_status(
            voice_mode.WakeDevDebugBody(
                browser_id="browser-a",
                tab_id="tab-1",
                surface="vad_dev",
                mode="vad_rearm",
                status="second",
                transcript="Computer again",
                snapshot={"fsm_state": "VAD_REARM_STT_FINALIZING"},
            )
        )
    )

    assert second["ok"] is True
    assert second["persisted"] is False
    persisted = json.loads(debug_path.read_text(encoding="utf-8"))
    persisted_report = persisted["reports"]["browser-a:tab-1:vad_dev"]
    assert persisted_report["status"] == "first"

    live = asyncio.run(voice_mode.voice_mode_dev_status(surface="vad_dev", browser_id="browser-a"))
    assert live["debug"]["status"] == "second"
    assert live["debug"]["snapshot"]["fsm_state"] == "VAD_REARM_STT_FINALIZING"


def test_voice_mode_dev_status_update_runs_off_event_loop(monkeypatch):
    async def run():
        def slow_update_sync(*args, **kwargs):
            time.sleep(0.15)
            return {"ok": True, "stored": True}

        monkeypatch.setattr(
            voice_mode,
            "_voice_mode_update_dev_status_locked_sync",
            slow_update_sync,
        )

        started = time.perf_counter()
        task = asyncio.create_task(
            voice_mode.voice_mode_update_dev_status(
                voice_mode.WakeDevDebugBody(
                    browser_id="browser-a",
                    surface="vad_dev",
                    status="slow",
                    snapshot={"fsm_state": "VAD_REARM_STT_ARMED"},
                )
            )
        )
        await asyncio.sleep(0.02)

        assert time.perf_counter() - started < 0.08
        assert not task.done()
        assert await task == {"ok": True, "stored": True}

    asyncio.run(run())


def test_voice_mode_wake_dev_debug_report_cannot_override_authoritative_active_wake_status():
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
    wake_debug = voice_mode._public_wake_dev_debug(state, debug)

    assert status["active"]["stt_enabled"] is True
    assert status["active"]["stt_mode"] == "wake_to_talk"
    assert wake_debug["active"]["stt_enabled"] is True
    assert wake_debug["active"]["stt_mode"] == "wake_to_talk"
    assert wake_debug["debug"]["authoritative_browser_active"] is True


def test_voice_mode_wake_dev_debug_does_not_treat_stt_mode_as_a_separate_activation():
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

    wake_debug = voice_mode._public_wake_dev_debug(state, debug)

    assert wake_debug["active"]["stt_enabled"] is False
    assert wake_debug["active"]["stt_mode"] == ""
    assert wake_debug["debug"]["authoritative_browser_active"] is True
    assert wake_debug["debug"]["running"] is True
    assert wake_debug["debug"]["fsm_state"] == "ARMED_IDLE"


def test_voice_mode_wake_dev_debug_reports_non_active_browser_as_non_authoritative():
    state = {"active": None}
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

    wake_debug = voice_mode._public_wake_dev_debug(state, debug)

    assert wake_debug["active"] is None
    assert wake_debug["debug"]["authoritative_browser_active"] is False
    assert wake_debug["debug"]["running"] is True
    assert wake_debug["debug"]["starting"] is False
    assert wake_debug["debug"]["fsm_state"] == "ARMED_IDLE"


def test_voice_mode_wake_dev_debug_flags_auto_execute_policy_mismatch():
    state = {
        "active": {
            "browser_id": "active-browser",
            "browser_label": "Browser on Win32",
            "stt_enabled": True,
            "stt_mode": "wake_to_talk",
            "tts_enabled": True,
        },
        "policy": {
            "wake_to_talk": {
                "instances": {
                    "local": {
                        "auto_execute_silence_ms": 900,
                        "execute_cancel_ms": 3000,
                        "partial_settle_ms": 2100,
                    }
                }
            }
        },
    }
    debug = {
        "reports": {
            "active-browser:wake_dev": {
                "browser_id": "active-browser",
                "surface": "wake_dev",
                "snapshot": {
                    "downstream": {
                        "instances": {
                            "local": {
                                "auto_execute_enabled": False,
                            }
                        }
                    }
                },
                "reported_at": 2,
            },
        }
    }

    wake_debug = voice_mode._public_wake_dev_debug(state, debug, surface="wake_dev")

    guard = wake_debug["debug"]["auto_execute_guard"]
    assert wake_debug["debug"]["authoritative_browser_active"] is True
    assert guard["ok"] is False
    assert guard["mismatch"] is True
    assert guard["policy_auto_execute_silence_ms"] == 900
    assert guard["reported_auto_execute_enabled"] is False


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
