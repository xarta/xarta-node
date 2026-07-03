import asyncio
import io
import json
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_matrix_chat as matrix_chat


def model_minutes_summary_for_packet(packet, *, result=None):
    operator = str(packet.get("operator_text") or "").strip()
    speech = str(packet.get("assistant_speech") or "").strip()
    pointers = (
        packet.get("source_pointers") if isinstance(packet.get("source_pointers"), dict) else {}
    )
    return {
        "schema": matrix_chat.hermes_minutes.MINUTES_SUMMARY_SCHEMA,
        "conversation_key": packet.get("conversation_key"),
        "time": "2026-06-13T00:00:00Z",
        "route": packet.get("route"),
        "route_status": packet.get("route_status"),
        "route_profile": packet.get("route_profile"),
        "operator_intent_summary": (
            f"Operator asked: {operator}" if operator else "Hermes sent a Bridge response."
        ),
        "assistant_action_summary": (
            "The system delivered a response."
            if speech
            else "The system recorded the operator turn."
        ),
        "result_summary": result or speech or "The Matrix Bridge message was recorded.",
        "open_question": "",
        "entities": [],
        "problems": [],
        "followup_affordances": [],
        "source_pointers": {
            "source_room_id": pointers.get("source_room_id") or "",
            "matrix_event_ids": pointers.get("matrix_event_ids") or [],
            "tts_utterance_ids": pointers.get("tts_utterance_ids") or [],
        },
        "source_detail_available": bool(
            (packet.get("source_material") or {}).get("matrix_detail_excerpt_for_model_only")
        ),
        "source_detail_policy": (
            "Minutes are model-written compact routing context, not source copies. "
            "Use source_pointers only when a later bounded source-check decision needs originals."
        ),
        "delivery": packet.get("delivery") if isinstance(packet.get("delivery"), dict) else {},
        "confidence": 0.82,
    }


@pytest.fixture(autouse=True)
def _default_wake_stt_profile_classifier(monkeypatch):
    async def fake_classifier(*_args, **_kwargs):
        return matrix_chat.wake_stt_direct.WakeSttProfileRoutingResult(
            target_profile="hermes-stt",
            requires_command_code=False,
            complex=False,
            risk_class="safe_short_answer",
            confidence=0.95,
            reason="test default base profile",
            speech_if_pending="",
            status="classified",
        )

    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fake_classifier,
    )


def test_matrix_chat_reads_private_env_without_exposing_token(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=secret-token-value",
                "MATRIX_HERMES_SMOKE_ROOM_ID=!room:test.example",
                "MATRIX_HERMES_USER_ID=@hermes:test.example",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))
    monkeypatch.delenv("MATRIX_CHAT_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MATRIX_CHAT_USER_ID", raising=False)

    settings = matrix_chat._settings()
    status = {
        "configured": bool(settings["user_id"] and settings["access_token"]),
        "homeserver_url": settings["public_homeserver"],
        "user_id": settings["user_id"],
        "default_room_id": settings["smoke_room_id"],
        "hermes_user_id": settings["hermes_user_id"],
    }

    assert settings["access_token"] == "secret-token-value"
    assert status == {
        "configured": True,
        "homeserver_url": "https://matrix.local",
        "user_id": "@codex:test.example",
        "default_room_id": "!room:test.example",
        "hermes_user_id": "@hermes:test.example",
    }
    assert "secret-token-value" not in repr(status)


def test_matrix_chat_reads_private_stt_noise_reduction_settings(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CHAT_STT_WS_URL=ws://stt.example.test:8765",
                "MATRIX_CHAT_STT_NOISE_REDUCTION_ENABLED=true",
                "MATRIX_CHAT_STT_NOISE_DFN_WS_URL=ws://filter.example.test:18760",
                "MATRIX_CHAT_STT_NOISE_STREAM_TEST_WS_URL=ws://filter.example.test:18761",
                "MATRIX_CHAT_STT_NOISE_ATTEN_LIM_DB=6.5",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))

    settings = matrix_chat._settings()

    assert settings["stt_ws_url"] == "ws://stt.example.test:8765"
    assert settings["stt_noise_reduction_enabled"] == "true"
    assert settings["stt_noise_dfn_ws_url"] == "ws://filter.example.test:18760"
    assert settings["stt_noise_stream_test_ws_url"] == "ws://filter.example.test:18761"
    assert settings["stt_noise_atten_lim_db"] == "6.5"


def test_wake_stt_pending_command_key_is_scoped_by_instance():
    local_key = matrix_chat._wake_stt_pending_command_key("!bridge:test.example", "local")
    vps_key = matrix_chat._wake_stt_pending_command_key("!bridge:test.example", "vps")

    assert local_key == "!bridge:test.example::local"
    assert vps_key == "!bridge:test.example::vps"
    assert local_key != vps_key


def test_wake_stt_builtin_control_fast_routes_do_not_need_config():
    stop = matrix_chat._wake_stt_fast_route_decision(
        "Computer stop",
        base_session_id="wake-stt-local",
    )
    clear = matrix_chat._wake_stt_fast_route_decision(
        "clear-house",
        base_session_id="wake-stt-local",
    )

    assert stop is not None
    assert stop.action == matrix_chat._WAKE_STT_FAST_ACTION_VOICE_STOP_CONTROL
    assert stop.persist_session is False
    assert clear is not None
    assert clear.action == matrix_chat._WAKE_STT_FAST_ACTION_CLEAR_HOUSE_CONTROL
    assert clear.persist_session is False


def test_wake_stt_voice_stop_publishes_silent_stop_event(monkeypatch):
    published = []

    async def fake_publish(event):
        published.append(event)

    monkeypatch.setattr(matrix_chat.events_bus, "publish", fake_publish)

    result = asyncio.run(matrix_chat._wake_stt_voice_stop_response_fields(reason="abort"))

    assert result["speech"] == ""
    assert result["status"] == "voice_stop_requested"
    assert published[0].event_type == "tts.stop.requested"
    assert published[0].payload["interrupt_active"] is True
    assert published[0].payload["clear_queues"] is True


def test_wake_stt_active_delivery_cancel_tracks_control_cancel():
    async def run():
        key = matrix_chat._wake_stt_active_delivery_key("!room:test", "local")
        task = asyncio.create_task(asyncio.sleep(60))
        try:
            matrix_chat._wake_stt_track_active_delivery_task(key, task)
            cancelled = matrix_chat._wake_stt_cancel_active_delivery_tasks(key)
            assert cancelled == 1
            assert id(task) in matrix_chat._WAKE_STT_CONTROL_CANCELLED_TASK_IDS
            try:
                await task
            except asyncio.CancelledError:
                pass
            assert key not in matrix_chat._WAKE_STT_ACTIVE_DELIVERY_TASKS
            assert id(task) not in matrix_chat._WAKE_STT_CONTROL_CANCELLED_TASK_IDS
        finally:
            task.cancel()
            matrix_chat._WAKE_STT_ACTIVE_DELIVERY_TASKS.clear()
            matrix_chat._WAKE_STT_CONTROL_CANCELLED_TASK_IDS.clear()

    asyncio.run(run())


def test_matrix_chat_noise_relay_waits_for_stt_final_after_filter_closes():
    async def run():
        done = asyncio.Event()
        final_requested = asyncio.Event()
        stt_end_sent = asyncio.Event()
        final_requested.set()
        stt_end_sent.set()

        async def wait_for_done():
            await done.wait()

        async def filter_done():
            return "filter-drained"

        async def stt_final():
            await asyncio.sleep(0.01)
            done.set()
            return "stt-final"

        browser_task = asyncio.create_task(wait_for_done())
        filter_task = asyncio.create_task(filter_done())
        stt_task = asyncio.create_task(stt_final())
        timeout_task = asyncio.create_task(wait_for_done())
        done_task = asyncio.create_task(done.wait())
        tasks = {browser_task, filter_task, stt_task, timeout_task, done_task}
        try:
            await matrix_chat._wait_for_matrix_stt_noise_relay_completion(
                browser_task=browser_task,
                filter_task=filter_task,
                stt_task=stt_task,
                timeout_task=timeout_task,
                done_task=done_task,
                done=done,
                final_requested=final_requested,
                stt_end_sent=stt_end_sent,
            )

            assert done.is_set()
            assert stt_task.done()
            assert stt_task.result() == "stt-final"
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())


def test_matrix_chat_noise_relay_treats_late_client_close_after_final_request_as_expected():
    async def run():
        done = asyncio.Event()
        final_requested = asyncio.Event()
        stt_end_sent = asyncio.Event()
        client_closed_before_final = asyncio.Event()
        final_requested.set()
        stt_end_sent.set()

        async def wait_for_done():
            await done.wait()

        async def late_partial_send_failure():
            await asyncio.sleep(0.01)
            raise matrix_chat.WebSocketDisconnect(code=1000)

        browser_task = asyncio.create_task(wait_for_done(), name="browser")
        filter_task = asyncio.create_task(wait_for_done(), name="filter")
        stt_task = asyncio.create_task(late_partial_send_failure(), name="stt")
        timeout_task = asyncio.create_task(wait_for_done(), name="timeout")
        done_task = asyncio.create_task(done.wait(), name="done")
        tasks = {browser_task, filter_task, stt_task, timeout_task, done_task}
        try:
            await matrix_chat._wait_for_matrix_stt_noise_relay_completion(
                browser_task=browser_task,
                filter_task=filter_task,
                stt_task=stt_task,
                timeout_task=timeout_task,
                done_task=done_task,
                done=done,
                final_requested=final_requested,
                stt_end_sent=stt_end_sent,
                client_closed_before_final=client_closed_before_final,
                log_room="!room:test",
            )

            assert done.is_set()
            assert client_closed_before_final.is_set()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(run())


def test_matrix_chat_wake_stt_direct_pre_roll_delay_defaults_to_three_seconds(monkeypatch):
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_PRE_ROLL_CONFIG_FILE",
        "/tmp/xarta-missing-wake-stt-pre-roll-test.json",
    )
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", raising=False)

    assert matrix_chat._wake_stt_direct_pre_roll_delay_seconds() == 3.0


def test_matrix_chat_wake_stt_pre_roll_config_sets_delay_and_shrinking_pool(monkeypatch, tmp_path):
    config = tmp_path / "wake-stt-pre-roll.json"
    config.write_text(
        json.dumps({"delay_ms": 1234, "utterances": ["Alpha.", "Beta."]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_PRE_ROLL_CONFIG_FILE", str(config))
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", raising=False)
    matrix_chat._wake_stt_clear_pre_roll_pool_state_for_tests()

    assert matrix_chat._wake_stt_direct_pre_roll_delay_seconds() == 1.234
    first = matrix_chat._wake_stt_select_pre_roll_utterance()[0]
    second = matrix_chat._wake_stt_select_pre_roll_utterance()[0]
    third = matrix_chat._wake_stt_select_pre_roll_utterance()[0]

    assert {first, second} == {"Alpha.", "Beta."}
    assert first != second
    assert third in {"Alpha.", "Beta."}


def test_matrix_chat_hermes_matrix_patch_status_reduces_report(tmp_path):
    report_path = tmp_path / "matrix_platform_patch.json"
    report_path.write_text(
        json.dumps(
            {
                "ok": False,
                "generated_at_epoch": 1779292232,
                "checks": [
                    {"id": "alias_mentions_leading_at_only", "message": "alias guard", "ok": False},
                    {"id": "env_allowed_rooms_set", "message": "rooms set", "ok": True},
                ],
                "env": {"MATRIX_ACCESS_TOKEN": "must-not-leak"},
            }
        ),
        encoding="utf-8",
    )

    status = matrix_chat._hermes_matrix_patch_status(str(report_path))
    rendered = repr(status)

    assert status == {
        "available": True,
        "ok": False,
        "generated_at_epoch": 1779292232,
        "failed_checks": [{"id": "alias_mentions_leading_at_only", "message": "alias guard"}],
        "error": "",
    }
    assert "must-not-leak" not in rendered


def test_matrix_chat_hermes_matrix_patch_status_handles_missing_report(tmp_path):
    status = matrix_chat._hermes_matrix_patch_status(str(tmp_path / "missing.json"))

    assert status == {
        "available": False,
        "ok": None,
        "generated_at_epoch": None,
        "failed_checks": [],
        "error": "report not found",
    }


def test_matrix_chat_message_content_adds_explicit_mxid_mentions():
    content = matrix_chat._matrix_message_content(
        "Hello @hermes:test.example and @operator:test.example"
    )

    assert content == {
        "msgtype": "m.text",
        "body": "Hello @hermes:test.example and @operator:test.example",
        "m.mentions": {
            "user_ids": ["@hermes:test.example", "@operator:test.example"],
        },
    }


def test_matrix_chat_stt_transcript_body_marks_voice_source():
    assert (
        matrix_chat._stt_transcript_body(server_id="tb1", transcript="hello world")
        == f"hermes: {matrix_chat._STT_TRANSCRIPT_PREFIX} hello world"
    )


def test_matrix_chat_wake_stt_transcript_redacts_auth_prefix_spans():
    body = matrix_chat._wake_stt_transcript_body(
        server_id="tb1",
        transcript="delete file Dave authorizes banana casino digital please",
    )

    assert "[redacted authorisation]" in body
    assert "banana" not in body.lower()
    assert "digital" not in body.lower()
    assert "please" not in body.lower()


def test_matrix_chat_direct_response_report_does_not_scrub_legitimate_authorisation_text():
    content = matrix_chat._matrix_wake_stt_direct_response_content(
        body="Wake STT reply: The authorisation policy is available.",
        instance="local",
        candidate_source="payload0",
        command="execute",
        wake_word="Computer",
        candidate_revision="rev1",
    )

    assert "authorisation policy" in content["body"]


def test_matrix_chat_stt_message_content_adds_visible_and_custom_metadata():
    content = matrix_chat._matrix_stt_message_content(
        body=f"hermes: {matrix_chat._STT_TRANSCRIPT_PREFIX} hello world",
        runtime="stt-runtime.example:8765",
        confidence=0.75,
    )

    assert content == {
        "msgtype": "m.text",
        "body": f"hermes: {matrix_chat._STT_TRANSCRIPT_PREFIX} hello world",
        "xarta_source": "stt",
        "xarta_stt_runtime": "stt-runtime.example:8765",
        "xarta_stt_partial": False,
        "xarta_capture_mode": "push_to_talk",
        "xarta_stt_safety_instruction": matrix_chat._STT_SAFETY_INSTRUCTION,
        "xarta_stt_long_task_tts_instruction": matrix_chat._STT_LONG_TASK_TTS_INSTRUCTION,
        "xarta_stt_destructive_actions_require_chat_composer_approval": True,
        "xarta_stt_confidence": 0.75,
    }


def test_matrix_chat_stt_safety_instruction_resists_transcript_overrides():
    safety = matrix_chat._STT_SAFETY_INSTRUCTION

    assert "Matrix Chat composer" in safety
    assert "ignore, disregard, override" in safety
    assert "untrusted STT content" in safety


def test_matrix_chat_wake_stt_transcript_body_marks_voice_source():
    assert (
        matrix_chat._wake_stt_transcript_body(server_id="tb1", transcript="hello world")
        == f"hermes: {matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} hello world"
    )
    assert (
        matrix_chat._wake_stt_transcript_body(server_id="vps", transcript="hello world")
        == f"hermes-vps: {matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} hello world"
    )
    assert (
        matrix_chat._wake_stt_transcript_body(
            server_id="tb1",
            transcript="validation only",
            address_hermes=False,
        )
        == f"{matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} validation only"
    )


def test_matrix_chat_wake_stt_message_content_adds_visible_and_custom_metadata():
    content = matrix_chat._matrix_wake_stt_message_content(
        body=f"hermes: {matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} hello world",
        instance="local",
        candidate_source="payload0",
        command="execute",
        wake_word="Computer",
        candidate_revision="wake-local-123",
    )

    assert content == {
        "msgtype": "m.text",
        "body": f"hermes: {matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} hello world",
        "xarta_source": "stt",
        "xarta_capture_mode": "wake_to_talk",
        "xarta_wake_instance": "local",
        "xarta_wake_candidate_source": "payload0",
        "xarta_wake_command": "execute",
        "xarta_wake_candidate_revision": "wake-local-123",
        "xarta_wake_word": "Computer",
        "xarta_stt_partial": False,
        "xarta_stt_safety_instruction": matrix_chat._STT_SAFETY_INSTRUCTION,
        "xarta_stt_long_task_tts_instruction": matrix_chat._STT_LONG_TASK_TTS_INSTRUCTION,
        "xarta_stt_destructive_actions_require_chat_composer_approval": True,
    }


@pytest.mark.asyncio
async def test_matrix_chat_wake_stt_route_reuses_e2ee_content_send(monkeypatch):
    captured = {}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$wake-stt"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    token = matrix_chat._CURRENT_MATRIX_SERVER.set("vps")
    try:
        result = await matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="What is the time?",
                instance="vps",
                candidate_source="payload2",
                command="auto_execute",
                wake_word="Mini-Me",
                candidate_revision="wake-vps-123",
            ),
        )
    finally:
        matrix_chat._CURRENT_MATRIX_SERVER.reset(token)

    assert captured["room_id"] == "!bridge:test.example"
    assert (
        captured["content"]["body"]
        == f"hermes-vps: {matrix_chat._WAKE_STT_TRANSCRIPT_PREFIX} What is the time?"
    )
    assert captured["content"]["xarta_capture_mode"] == "wake_to_talk"
    assert captured["content"]["xarta_wake_instance"] == "vps"
    assert captured["content"]["xarta_wake_candidate_source"] == "payload2"
    assert captured["content"]["xarta_wake_command"] == "auto_execute"
    assert captured["content"]["xarta_wake_candidate_revision"] == "wake-vps-123"
    assert result == {
        "room_id": "!bridge:test.example",
        "event_id": "$wake-stt",
        "body": captured["content"]["body"],
        "server_id": "vps",
        "xarta_source": "stt",
        "xarta_capture_mode": "wake_to_talk",
        "xarta_wake_instance": "vps",
        "xarta_wake_candidate_source": "payload2",
        "xarta_wake_command": "auto_execute",
        "xarta_wake_candidate_revision": "wake-vps-123",
    }


def test_matrix_chat_wake_stt_direct_diagnostic_content_is_not_addressed():
    content = matrix_chat._matrix_wake_stt_direct_diagnostic_content(
        body="Wake STT: What is the time?",
        instance="local",
        candidate_source="payload0",
        command="execute",
        wake_word="Computer",
        candidate_revision="wake-local-456",
    )

    assert content["body"] == "Wake STT: What is the time?"
    assert not content["body"].lower().startswith("hermes:")
    assert "authorised" not in content["body"].lower()
    assert content["xarta_source"] == "wake_stt_direct_observation"
    assert content["xarta_capture_mode"] == "wake_to_talk"
    assert content["xarta_suppress_speech"] is True
    assert content["suppress_speech"] is True
    assert "m.mentions" not in content


def test_matrix_chat_wake_stt_direct_response_content_is_speech_suppressed():
    content = matrix_chat._matrix_wake_stt_direct_response_content(
        body="Wake STT reply: I sent the rest to Matrix.",
        instance="local",
        candidate_source="payload0",
        command="execute",
        wake_word="Computer",
        candidate_revision="wake-local-response",
        tts_status="streamed",
    )

    assert content["body"] == "Wake STT reply: I sent the rest to Matrix."
    assert not content["body"].lower().startswith("hermes:")
    assert content["xarta_source"] == "wake_stt_direct_response"
    assert content["xarta_tts_companion_copy"] is True
    assert content["xarta_tts_status"] == "streamed"
    assert content["xarta_suppress_speech"] is True
    assert content["suppress_speech"] is True
    assert "m.mentions" not in content


def test_matrix_chat_direct_wrapper_failure_does_not_post_matrix_fallback(monkeypatch):
    captured = {"called": False}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["called"] = True
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$unexpected"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    async def fake_submit(text, *, codes=None, **_kwargs):
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=False,
            status="request_error",
            gate=gate,
            attempted=True,
            fallback_required=False,
            error="connection refused",
        )

    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="authorisation alpha one seven This command is authorised. What is the time?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-789",
            ),
            direct_enabled=True,
        )
    )

    assert result.ok is False
    assert result.route == "direct_local"
    assert result.status == "request_error"
    assert result.fallback_reason == "request_error"
    assert captured["called"] is False


def test_matrix_chat_direct_wrapper_failure_can_post_explicit_non_addressed_diagnostic(
    monkeypatch,
):
    captured = {}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$diag"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    async def fake_submit(text, *, codes=None, **_kwargs):
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=False,
            status="request_error",
            gate=gate,
            attempted=True,
            fallback_required=False,
            error="connection refused",
        )

    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="authorisation alpha one seven This command is authorised. What is the time?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-789",
            ),
            direct_enabled=True,
            diagnostic_enabled=True,
            await_diagnostic=True,
        )
    )

    assert result.ok is False
    assert result.route == "direct_local"
    assert result.diagnostic and result.diagnostic["event_id"] == "$diag"
    assert captured["room_id"] == "!bridge:test.example"
    assert captured["content"]["body"] == "Wake STT: what is the time?"
    assert not captured["content"]["body"].lower().startswith("hermes:")
    assert "alpha one seven" not in captured["content"]["body"].lower()
    assert "authorised" not in captured["content"]["body"].lower()
    assert captured["content"]["xarta_source"] == "wake_stt_direct_observation"


def test_matrix_chat_direct_wrapper_uses_active_session_without_auto_rotation(
    monkeypatch, tmp_path
):
    captured = {}
    active_session = tmp_path / "active-session.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-kept"}),
        encoding="utf-8",
    )

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            return {"room_id": room_id, "event_id": "$fallback"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    async def fake_submit(text, *, config=None, codes=None, **_kwargs):
        captured["session_id"] = config.session_id
        captured["tool_surface"] = config.tool_surface
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=False,
            status="request_error",
            gate=gate,
            attempted=True,
            fallback_required=False,
            error="connection refused",
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="What is five times seven?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-kept",
            ),
            direct_enabled=True,
        )
    )

    assert captured["session_id"] == "wake-stt-local-operator-kept"
    assert json.loads(active_session.read_text(encoding="utf-8"))["session_id"] == (
        "wake-stt-local-operator-kept"
    )


def test_matrix_chat_direct_wrapper_uses_compact_session_for_time_lookup(monkeypatch, tmp_path):
    captured = {}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-heavy"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_fast",
                        "action": "time_fast_session",
                        "match": {
                            "kind": "exact",
                            "phrases": ["what's the time", "what is the time"],
                        },
                        "session": {
                            "mode": "ephemeral",
                            "prefix": "time-fast",
                            "persist_session": False,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fake_submit(text, *, config=None, codes=None, **_kwargs):
        captured["session_id"] = config.session_id
        captured["tool_surface"] = config.tool_surface
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text='{"speech":"ten oh five","matrix_detail":"time","status":"ok"}',
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="ten oh five",
                matrix_detail="time",
                status="ok",
                structured=True,
                raw_assistant_text=(
                    '{"speech":"ten oh five","matrix_detail":"time","status":"ok"}'
                ),
            ),
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="what is the time",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-time",
            ),
            direct_enabled=True,
        )
    )

    assert captured["session_id"].startswith("wake-stt-local-time-fast-")
    assert captured["tool_surface"] == "xarta_time_lookup_only"
    assert json.loads(active_session.read_text(encoding="utf-8"))["session_id"] == (
        "wake-stt-local-operator-heavy"
    )


def test_matrix_chat_direct_wrapper_uses_deterministic_action_for_exact_current_time(
    monkeypatch, tmp_path
):
    captured = {"fallback_calls": 0, "helper_texts": []}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-heavy"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_current_exact",
                        "action": "time_current_deterministic_response",
                        "match": {
                            "kind": "exact",
                            "phrases": [
                                "what's the time",
                                "whats the time",
                                "rot's the time",
                                "rots the time",
                                "what time is it",
                                "what is the time",
                                "time please",
                            ],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fail_delivery(*_args, **_kwargs):
        captured["fallback_calls"] += 1
        raise AssertionError("deterministic current-time action must not call Hermes")

    async def fail_classifier(*_args, **_kwargs):
        raise AssertionError("deterministic current-time action must not classify")

    def fake_time_tool_response_fields(*, text, route):
        captured["helper_texts"].append(text)
        assert route.action == "time_current_deterministic_response"
        return {
            "speech": "twenty-one fifteen",
            "matrix_detail": "Local time (Europe/London, BST): 2026-06-06 21:15.",
            "status": "ok",
            "kind": "time",
            "timezone": "Europe/London",
            "time_24h": "21:15",
            "helper_elapsed_ms": "1.2",
        }

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fail_delivery,
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fail_classifier,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_time_tool_response_fields",
        fake_time_tool_response_fields,
    )

    for phrase in (
        "what's the time",
        "whats the time",
        "rot's the time",
        "rots the time",
        "what time is it",
        "what is the time",
        "time please",
    ):
        result = asyncio.run(
            matrix_chat._deliver_wake_stt_with_direct_fallback(
                room_id="!bridge:test.example",
                body=matrix_chat._WakeSttMessageBody(
                    text=phrase,
                    instance="local",
                    candidate_source="payload0",
                    command="execute",
                    wake_word="Computer",
                    candidate_revision="wake-local-time",
                ),
                direct_enabled=True,
            )
        )
        public = result.public_dict()
        companion = public["direct"]["companion"]
        assert public["ok"] is True
        assert public["route"] == "direct_local"
        assert public["direct"]["attempted"] is False
        assert public["direct"]["status"] == "time_current_deterministic_response"
        assert companion["status"] == "ok"
        assert companion["speech"] == "twenty-one fifteen"
        assert companion["matrix_detail"].startswith("Local time (Europe/London")

    assert captured["fallback_calls"] == 0
    assert captured["helper_texts"] == [
        "what's the time",
        "whats the time",
        "rot's the time",
        "rots the time",
        "what time is it",
        "what is the time",
        "time please",
    ]
    assert json.loads(active_session.read_text(encoding="utf-8"))["session_id"] == (
        "wake-stt-local-operator-heavy"
    )


def test_matrix_chat_direct_wrapper_uses_deterministic_alarm_controls(monkeypatch, tmp_path):
    captured = {"fallback_calls": 0, "classifier_calls": 0, "actions": []}
    fast_routes = tmp_path / "fast-routes.json"
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "alarm_dismiss_exact",
                        "action": "alarm_dismiss_control",
                        "match": {
                            "kind": "exact",
                            "phrases": [
                                "dismiss",
                                "dismiss alarm",
                                "computer alarm dismiss",
                            ],
                        },
                    },
                    {
                        "id": "alarm_snooze_exact",
                        "action": "alarm_snooze_control",
                        "match": {
                            "kind": "exact",
                            "phrases": [
                                "snooze",
                                "snooze alarm",
                                "computer alarm snooze",
                            ],
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fail_delivery(*_args, **_kwargs):
        captured["fallback_calls"] += 1
        raise AssertionError("deterministic alarm controls must not call Hermes")

    async def fail_classifier(*_args, **_kwargs):
        captured["classifier_calls"] += 1
        raise AssertionError("deterministic alarm controls must not classify")

    async def fake_alarm_control_response_fields(*, action, route):
        captured["actions"].append((action, route.route_id))
        return {
            "speech": "Alarm snoozed." if action == "alarm_snooze_control" else "Alarm dismissed.",
            "matrix_detail": f"alarm control {action}",
            "status": action,
            "helper_elapsed_ms": "0",
        }

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fail_delivery,
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fail_classifier,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_alarm_control_response_fields",
        fake_alarm_control_response_fields,
    )

    cases = [
        ("dismiss", "alarm_dismiss_control", "Alarm dismissed."),
        ("dismiss alarm", "alarm_dismiss_control", "Alarm dismissed."),
        ("computer alarm dismiss", "alarm_dismiss_control", "Alarm dismissed."),
        ("snooze", "alarm_snooze_control", "Alarm snoozed."),
        ("snooze alarm", "alarm_snooze_control", "Alarm snoozed."),
        ("computer alarm snooze", "alarm_snooze_control", "Alarm snoozed."),
    ]
    for phrase, status, speech in cases:
        result = asyncio.run(
            matrix_chat._deliver_wake_stt_with_direct_fallback(
                room_id="!bridge:test.example",
                body=matrix_chat._WakeSttMessageBody(
                    text=phrase,
                    instance="local",
                    candidate_source="payload0",
                    command="execute",
                    wake_word="Computer",
                    candidate_revision="wake-local-alarm",
                ),
                direct_enabled=True,
            )
        )
        public = result.public_dict()
        companion = public["direct"]["companion"]
        assert public["ok"] is True
        assert public["route"] == "direct_local"
        assert public["direct"]["attempted"] is False
        assert public["direct"]["status"] == status
        assert companion["status"] == status
        assert companion["speech"] == speech

    assert captured == {
        "fallback_calls": 0,
        "classifier_calls": 0,
        "actions": [
            ("alarm_dismiss_control", "alarm_dismiss_exact"),
            ("alarm_dismiss_control", "alarm_dismiss_exact"),
            ("alarm_dismiss_control", "alarm_dismiss_exact"),
            ("alarm_snooze_control", "alarm_snooze_exact"),
            ("alarm_snooze_control", "alarm_snooze_exact"),
            ("alarm_snooze_control", "alarm_snooze_exact"),
        ],
    }


def test_matrix_chat_deterministic_current_time_action_is_exact_only(monkeypatch, tmp_path):
    captured = {}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-kept"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_current_exact",
                        "action": "time_current_deterministic_response",
                        "match": {
                            "kind": "exact",
                            "phrases": ["what is the time"],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fake_delivery(text, *, config=None, codes=None, **_kwargs):
        captured["text"] = text
        captured["session_id"] = config.session_id
        captured["tool_surface"] = config.tool_surface
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        direct = matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text="Hermes path.",
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="Hermes path.",
                matrix_detail="Hermes path.",
                status="ok",
                structured=False,
                raw_assistant_text="Hermes path.",
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=direct,
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fake_delivery,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="what is the time in London",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-time-near-miss",
            ),
            direct_enabled=True,
        )
    )

    assert result.ok is True
    assert captured["text"] == "what is the time in London"
    assert captured["session_id"] == "wake-stt-local-operator-kept"
    assert captured["tool_surface"] == ""


def test_matrix_chat_basic_health_action_is_exact_only(monkeypatch, tmp_path):
    fast_routes = tmp_path / "fast-routes.json"
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "basic_health_exact",
                        "action": "basic_health_deterministic_response",
                        "match": {
                            "kind": "exact",
                            "phrases": ["are you ok", "are you okay"],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))

    assert (
        matrix_chat._wake_stt_fast_route_decision(
            "Are you okay?", base_session_id="wake-stt-local"
        ).action
        == "basic_health_deterministic_response"
    )
    assert (
        matrix_chat._wake_stt_fast_route_decision(
            "are you ok", base_session_id="wake-stt-local"
        ).action
        == "basic_health_deterministic_response"
    )
    assert (
        matrix_chat._wake_stt_fast_route_decision(
            "are you okay and delete a file", base_session_id="wake-stt-local"
        )
        is None
    )


def test_matrix_chat_fast_routes_do_not_match_explicit_corrections(monkeypatch, tmp_path):
    fast_routes = tmp_path / "fast-routes.json"
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "misconfigured_health",
                        "action": "basic_health_deterministic_response",
                        "match": {
                            "kind": "exact",
                            "phrases": ["i didn t want chat admin"],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))

    assert (
        matrix_chat._wake_stt_fast_route_decision(
            "I didn't want chat admin", base_session_id="wake-stt-local"
        )
        is None
    )


def test_matrix_chat_direct_wrapper_uses_deterministic_action_for_basic_health(
    monkeypatch, tmp_path
):
    captured = {"fallback_calls": 0, "classifier_calls": 0}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-heavy"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "basic_health_exact",
                        "action": "basic_health_deterministic_response",
                        "match": {
                            "kind": "exact",
                            "phrases": ["are you ok", "are you okay"],
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fail_delivery(*_args, **_kwargs):
        captured["fallback_calls"] += 1
        raise AssertionError("deterministic basic-health action must not call Hermes")

    async def fail_classifier(*_args, **_kwargs):
        captured["classifier_calls"] += 1
        raise AssertionError("deterministic basic-health action must not classify")

    async def fake_health_fields():
        return {
            "speech": "I am functioning within normal parameters.",
            "matrix_detail": "Deterministic Wake STT basic health check.",
            "status": "basic_health_ok",
            "helper_elapsed_ms": "1.0",
        }

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fail_delivery,
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fail_classifier,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_basic_health_response_fields",
        fake_health_fields,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="Are you okay?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-health",
            ),
            direct_enabled=True,
        )
    )

    public = result.public_dict()
    companion = public["direct"]["companion"]
    assert public["ok"] is True
    assert public["route"] == "direct_local"
    assert public["direct"]["attempted"] is False
    assert public["direct"]["status"] == "basic_health_deterministic_response"
    assert companion["speech"] == "I am functioning within normal parameters."
    assert captured == {"fallback_calls": 0, "classifier_calls": 0}


def test_matrix_chat_basic_health_reads_private_check_config(monkeypatch, tmp_path):
    checks_file = tmp_path / "basic-health-checks.json"
    checks_file.write_text(
        json.dumps(
            {
                "checks": [
                    {"label": "local model", "host": "127.0.0.1", "port": 4000},
                    {"label": "test service", "host": "127.0.0.1", "port": 443},
                ]
            }
        ),
        encoding="utf-8",
    )
    seen = []

    def fake_probe(host, port, *, timeout_seconds=0.35):
        seen.append((host, port, timeout_seconds))
        return True, ""

    monkeypatch.setenv(
        "BLUEPRINTS_PVE_FAST_HEALTH_CONFIG_FILE",
        str(tmp_path / "missing-pve-fast-health.json"),
    )
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_BASIC_HEALTH_CHECKS_FILE", str(checks_file))
    monkeypatch.setattr(matrix_chat, "_wake_stt_tcp_probe", fake_probe)

    fields = asyncio.run(matrix_chat._wake_stt_basic_health_response_fields())

    assert fields["speech"] == "I am functioning within normal parameters."
    assert fields["status"] == "basic_health_ok"
    assert "Config: configured." in fields["matrix_detail"]
    assert "- local model: ok (127.0.0.1:4000)" in fields["matrix_detail"]
    assert "- test service: ok (127.0.0.1:443)" in fields["matrix_detail"]
    assert seen == [("127.0.0.1", 4000, 0.35), ("127.0.0.1", 443, 0.35)]


def test_wake_stt_fast_routes_file_uses_lone_wolf_config_default(monkeypatch, tmp_path):
    fast_routes = tmp_path / "wake-stt-fast-routes.json"
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", raising=False)
    monkeypatch.setattr(
        matrix_chat,
        "_DEFAULT_WAKE_STT_FAST_ROUTES_FILE",
        str(fast_routes),
    )

    assert matrix_chat._wake_stt_fast_routes_file() == fast_routes


def test_matrix_chat_direct_wrapper_uses_prefix_fast_route_from_config(monkeypatch, tmp_path):
    captured = {}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-heavy"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_fast",
                        "action": "time_fast_session",
                        "match": {
                            "kind": "prefix",
                            "phrases": ["what will be the time"],
                        },
                        "session": {
                            "mode": "ephemeral",
                            "prefix": "time-fast",
                            "persist_session": False,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fake_submit(text, *, config=None, codes=None, **_kwargs):
        captured["session_id"] = config.session_id
        captured["tool_surface"] = config.tool_surface
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text='{"speech":"ten oh seven","matrix_detail":"time","status":"ok"}',
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="ten oh seven",
                matrix_detail="time",
                status="ok",
                structured=True,
                raw_assistant_text=(
                    '{"speech":"ten oh seven","matrix_detail":"time","status":"ok"}'
                ),
            ),
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="what will be the time in two minutes",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-time-prefix",
            ),
            direct_enabled=True,
        )
    )

    assert captured["session_id"].startswith("wake-stt-local-time-fast-")
    assert captured["tool_surface"] == "xarta_time_lookup_only"


def test_matrix_chat_direct_wrapper_does_not_treat_times_arithmetic_as_time_lookup(
    monkeypatch, tmp_path
):
    captured = {}
    active_session = tmp_path / "active-session.json"
    fast_routes = tmp_path / "fast-routes.json"
    active_session.write_text(
        json.dumps({"session_id": "wake-stt-local-operator-kept"}),
        encoding="utf-8",
    )
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_fast",
                        "action": "time_fast_session",
                        "match": {
                            "kind": "exact",
                            "phrases": ["what is the time"],
                        },
                        "session": {
                            "mode": "ephemeral",
                            "prefix": "time-fast",
                            "persist_session": False,
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fake_submit(text, *, config=None, codes=None, **_kwargs):
        captured["session_id"] = config.session_id
        captured["tool_surface"] = config.tool_surface
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text="35",
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="35",
                matrix_detail="35",
                status="ok",
                structured=False,
                raw_assistant_text="35",
            ),
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="What is five times seven?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-math",
            ),
            direct_enabled=True,
        )
    )

    assert captured["session_id"] == "wake-stt-local-operator-kept"
    assert captured["tool_surface"] == ""


def test_matrix_chat_vps_exact_time_is_deterministic(monkeypatch, tmp_path):
    captured = {"fallback_calls": 0, "classifier_calls": 0, "helper_texts": []}
    fast_routes = tmp_path / "fast-routes.json"
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "time_current_exact",
                        "action": "time_current_deterministic_response",
                        "match": {"kind": "exact", "phrases": ["what is the time"]},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fail_delivery(*_args, **_kwargs):
        captured["fallback_calls"] += 1
        raise AssertionError("exact VPS current-time must not call Hermes or Matrix fallback")

    async def fail_classifier(*_args, **_kwargs):
        captured["classifier_calls"] += 1
        raise AssertionError("exact VPS current-time must not classify")

    def fake_time_tool_response_fields(*, text, route):
        captured["helper_texts"].append(text)
        assert route.action == "time_current_deterministic_response"
        return {
            "speech": "ten oh five",
            "matrix_detail": "Local time (Europe/London, BST): 2026-06-10 10:05.",
            "status": "ok",
            "kind": "time",
            "timezone": "Europe/London",
            "time_24h": "10:05",
            "helper_elapsed_ms": "1.1",
        }

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_instance_config",
        lambda instance: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://10.253.2.99:8648",
            api_key="secret",
            model="example-vps-stt",
            session_id="wake-stt-vps",
            allow_non_loopback=True,
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fail_delivery,
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fail_classifier,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_time_tool_response_fields",
        fake_time_tool_response_fields,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="what is the time",
                instance="vps",
                candidate_source="payload0",
                command="execute",
                wake_word="Dave",
                candidate_revision="wake-vps-time",
            ),
            direct_enabled=True,
        )
    )

    public = result.public_dict()
    companion = public["direct"]["companion"]
    assert public["ok"] is True
    assert public["route"] == "direct_vps"
    assert public["direct"]["attempted"] is False
    assert public["direct"]["status"] == "time_current_deterministic_response"
    assert companion["speech"] == "ten oh five"
    assert captured == {
        "fallback_calls": 0,
        "classifier_calls": 0,
        "helper_texts": ["what is the time"],
    }


def test_matrix_chat_vps_basic_health_exact_is_deterministic(monkeypatch, tmp_path):
    captured = {"fallback_calls": 0, "classifier_calls": 0, "health_calls": 0}
    fast_routes = tmp_path / "fast-routes.json"
    fast_routes.write_text(
        json.dumps(
            {
                "routes": [
                    {
                        "id": "basic_health_exact",
                        "action": "basic_health_deterministic_response",
                        "match": {"kind": "exact", "phrases": ["are you okay"]},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    async def fail_delivery(*_args, **_kwargs):
        captured["fallback_calls"] += 1
        raise AssertionError("exact VPS health must not call Hermes or Matrix fallback")

    async def fail_classifier(*_args, **_kwargs):
        captured["classifier_calls"] += 1
        raise AssertionError("exact VPS health must not classify")

    async def fake_vps_health_fields():
        captured["health_calls"] += 1
        return {
            "speech": "I'm okay. Hermes VPS is online and ready.",
            "matrix_detail": "Deterministic VPS Wake STT health check passed.",
            "status": "vps_health_ok",
            "helper_elapsed_ms": "1.0",
        }

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_FAST_ROUTES_FILE", str(fast_routes))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_instance_config",
        lambda instance: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://10.253.2.99:8648",
            api_key="secret",
            model="example-vps-stt",
            session_id="wake-stt-vps",
            allow_non_loopback=True,
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "deliver_wake_stt_with_matrix_fallback",
        fail_delivery,
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "classify_wake_stt_profile",
        fail_classifier,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_vps_basic_health_response_fields",
        fake_vps_health_fields,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="are you okay",
                instance="vps",
                candidate_source="payload0",
                command="execute",
                wake_word="Dave",
                candidate_revision="wake-vps-health",
            ),
            direct_enabled=True,
        )
    )

    public = result.public_dict()
    companion = public["direct"]["companion"]
    assert public["ok"] is True
    assert public["route"] == "direct_vps"
    assert public["direct"]["attempted"] is False
    assert public["direct"]["status"] == "basic_health_deterministic_response"
    assert companion["speech"] == "I'm okay. Hermes VPS is online and ready."
    assert captured == {"fallback_calls": 0, "classifier_calls": 0, "health_calls": 1}


def test_matrix_chat_direct_wrapper_rotates_session_only_on_operator_request(monkeypatch, tmp_path):
    captured = {"session_ids": []}
    active_session = tmp_path / "active-session.json"

    async def fake_submit(text, *, config=None, codes=None, **_kwargs):
        captured["session_ids"].append(config.session_id)
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text="Session handled.",
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="Session handled.",
                matrix_detail="Session handled.",
                status="ok",
                structured=False,
                raw_assistant_text="Session handled.",
            ),
        )

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ACTIVE_SESSION_FILE", str(active_session))
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "load_hermes_stt_config",
        lambda: matrix_chat.wake_stt_direct.HermesSttConfig(
            api_base="http://127.0.0.1:8643",
            api_key="secret",
            session_id="wake-stt-local",
        ),
    )
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    async def run_turn(text: str):
        return await matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text=text,
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-reset",
            ),
            direct_enabled=True,
        )

    asyncio.run(run_turn("Please start a new session."))
    stored = json.loads(active_session.read_text(encoding="utf-8"))["session_id"]
    asyncio.run(run_turn("What is five times seven?"))

    assert stored.startswith("wake-stt-local-operator-")
    assert captured["session_ids"] == [stored, stored]


def test_matrix_chat_direct_wrapper_posts_redacted_diagnostic_on_success(monkeypatch):
    captured = {}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$diag"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    async def fake_submit(text, *, codes=None, **_kwargs):
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            assistant_text="ok",
            companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                speech="ok",
                matrix_detail="ok",
                status="ok",
                structured=True,
                raw_assistant_text='{"speech":"ok","matrix_detail":"ok","status":"ok"}',
            ),
        )

    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"bravo","aliases":["bravo two cedar"]}]}',
    )
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_to_hermes",
        fake_submit,
    )

    result = asyncio.run(
        matrix_chat._deliver_wake_stt_with_direct_fallback(
            room_id="!bridge:test.example",
            body=matrix_chat._WakeSttMessageBody(
                text="authorisation bravo two cedar This command is authorised. Please check status.",
                instance="local",
                candidate_source="payload1",
                command="auto_execute",
                wake_word="Computer",
                candidate_revision="wake-local-999",
            ),
            direct_enabled=True,
            diagnostic_enabled=True,
            await_diagnostic=True,
        )
    )

    assert result.ok is True
    assert result.route == "direct_local"
    assert captured["content"]["body"] == "Wake STT: please check status."
    assert not captured["content"]["body"].lower().startswith("hermes:")
    assert "bravo" not in captured["content"]["body"].lower()
    assert "authorised" not in captured["content"]["body"].lower()
    assert captured["content"]["xarta_source"] == "wake_stt_direct_observation"


def test_matrix_chat_wake_stt_command_code_retry_authorises_only_pending_request(
    monkeypatch,
):
    calls = []
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        trusted = bool(kwargs.get("trusted_authorised"))
        calls.append({"text": body.text, "trusted": trusted})
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=trusted,
        )
        status = "ok" if trusted else "command_code_required"
        speech = "authorised retry ok" if trusted else "internal diagnostic should not leak"
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech=speech,
            matrix_detail=speech,
            status=status,
            structured=True,
            raw_assistant_text=json.dumps(
                {"speech": speech, "matrix_detail": speech, "status": status},
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create a new file called Dave",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    second = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    first_delivery = first["delivery"]
    second_delivery = second["delivery"]

    assert first_delivery["status"] == "command_code_required"
    assert first_delivery["command_code_pending"] == {"held": True, "scope": "next_wake_turn"}
    assert first_delivery["tts"]["status"] == "queued"
    assert first_delivery["direct"]["companion"]["speech"] == (
        "Authorisation Command Code required."
    )
    assert second_delivery["ok"] is True
    assert second_delivery["direct"]["authorised"] is True
    assert calls == [
        {"text": "create a new file called Dave", "trusted": False},
        {"text": "create a new file called Dave", "trusted": True},
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}
    assert "alpha one seven" not in json.dumps(first).lower()
    assert "alpha one seven" not in json.dumps(second).lower()
    assert matrix_chat.wake_stt_direct.AUTHORISED_PHRASE not in json.dumps(second)


def test_matrix_chat_wake_stt_pending_command_correction_repairs_bounded_navigation(
    monkeypatch,
    tmp_path,
):
    room_id = "!bridge:test.example"
    calls: list[dict[str, object]] = []
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    context_file = tmp_path / "blueprints-nav-context.json"
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_BLUEPRINTS_NAV_CONTEXT_FILE", str(context_file))
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        calls.append(
            {
                "text": body.text,
                "trusted": bool(kwargs.get("trusted_authorised")),
                "conversation_key": kwargs.get("conversation_key", ""),
            }
        )
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=bool(kwargs.get("trusted_authorised")),
        )
        requires_code = "create a new file" in body.text
        status = "command_code_required" if requires_code else "blueprints_nav_dispatched"
        speech = "needs code" if requires_code else "Opening the VPS Shared Bridge room."
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech=speech,
            matrix_detail=speech,
            status=status,
            structured=True,
            raw_assistant_text=json.dumps(
                {"speech": speech, "matrix_detail": speech, "status": status},
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=not requires_code,
            status="delivered" if not requires_code else "command_code_required",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=not requires_code,
                status=status,
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            room_id,
            matrix_chat._WakeSttMessageBody(
                text="create a new file called Dave",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    conversation_key = matrix_chat.wake_stt_direct.wake_stt_conversation_key(
        room_id=room_id,
        instance="local",
    )
    matrix_chat.wake_stt_direct._write_wake_stt_blueprints_nav_context(
        request_text="open the vps chat",
        status="blueprints_nav_dispatched",
        decision={
            "action": "dispatch",
            "candidate_id": "page:settings.matrix-chat-admin",
            "confidence": 0.86,
            "ambiguous": False,
            "reason": "wrong nearby page",
            "speech": "Opening Chat Admin.",
            "candidate": {
                "id": "page:settings.matrix-chat-admin",
                "kind": "open_page",
                "label": "Chat Admin",
                "group": "settings",
                "page_id": "matrix-chat-admin",
            },
        },
        candidates=[
            {
                "id": "matrix_chat_room:vps.shared-bridge",
                "kind": "open_matrix_chat_room",
                "label": "Matrix Chat - VPS - Shared Bridge",
                "group": "settings",
                "page_id": "matrix-chat",
                "server_id": "vps",
                "room_hint": "Shared Bridge",
            },
            {
                "id": "page:settings.matrix-chat-admin",
                "kind": "open_page",
                "label": "Chat Admin",
                "group": "settings",
                "page_id": "matrix-chat-admin",
            },
        ],
        conversation_key=conversation_key,
        context_kind="last_navigation_action",
    )
    second = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            room_id,
            matrix_chat._WakeSttMessageBody(
                text=(
                    "I didn't want chat admin, I wanted the shared bridge room, "
                    "I think that's the VPS chat"
                ),
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert second["delivery"]["status"] == "delivered"
    assert "command_code_pending" not in second["delivery"]
    assert calls == [
        {
            "text": "create a new file called Dave",
            "trusted": False,
            "conversation_key": conversation_key,
        },
        {
            "text": (
                "I didn't want chat admin, I wanted the shared bridge room, "
                "I think that's the VPS chat"
            ),
            "trusted": False,
            "conversation_key": conversation_key,
        },
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_pending_non_code_followup_routes_current_turn(monkeypatch):
    room_id = "!bridge:test.example"
    calls: list[dict[str, object]] = []
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        calls.append(
            {
                "text": body.text,
                "trusted": bool(kwargs.get("trusted_authorised")),
                "conversation_key": kwargs.get("conversation_key", ""),
            }
        )
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=bool(kwargs.get("trusted_authorised")),
        )
        requires_code = "create a new file" in body.text
        status = "command_code_required" if requires_code else "delivered"
        speech = "needs code" if requires_code else "follow-up answered"
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech=speech,
            matrix_detail=speech,
            status=status,
            structured=True,
            raw_assistant_text=json.dumps(
                {"speech": speech, "matrix_detail": speech, "status": status},
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=not requires_code,
            status="command_code_required" if requires_code else "delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=not requires_code,
                status=status,
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            room_id,
            matrix_chat._WakeSttMessageBody(
                text="create a new file called Dave",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    second = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            room_id,
            matrix_chat._WakeSttMessageBody(
                text="Peter K work with him as well then",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    conversation_key = matrix_chat.wake_stt_direct.wake_stt_conversation_key(
        room_id=room_id,
        instance="local",
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert second["delivery"]["status"] == "delivered"
    assert "command_code_pending" not in second["delivery"]
    assert calls == [
        {
            "text": "create a new file called Dave",
            "trusted": False,
            "conversation_key": conversation_key,
        },
        {
            "text": "Peter K work with him as well then",
            "trusted": False,
            "conversation_key": conversation_key,
        },
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_pending_reuses_profile_routing(monkeypatch):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    classifier_calls = {"count": 0}
    handoff_calls: list[dict[str, object]] = []
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_classifier(*_args, **_kwargs):
        classifier_calls["count"] += 1
        return matrix_chat.wake_stt_direct.WakeSttProfileRoutingResult(
            target_profile="hermes-stt-smart",
            requires_command_code=True,
            complex=True,
            risk_class="scripting",
            confidence=0.96,
            reason="script building",
            speech_if_pending="Authorisation Command Code required.",
            status="classified",
        )

    async def fake_base_submit(text, *, codes=None, **_kwargs):
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(text, codes or [])
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="base should be ignored",
            matrix_detail="base should be ignored",
            status="ok",
            structured=True,
            raw_assistant_text=(
                '{"speech":"base should be ignored","matrix_detail":"base should be ignored",'
                '"status":"ok"}'
            ),
        )
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            companion=companion,
        )

    async def fake_handoff(text, *, profile_routing, trusted_authorised=False, **_kwargs):
        handoff_calls.append(
            {
                "text": text,
                "target_profile": profile_routing.target_profile,
                "trusted_authorised": trusted_authorised,
            }
        )
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=trusted_authorised,
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="smart handoff ok",
            matrix_detail="smart handoff detail",
            status="ok",
            structured=True,
            raw_assistant_text=(
                '{"speech":"smart handoff ok","matrix_detail":"smart handoff detail","status":"ok"}'
            ),
        )
        return matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            companion=companion,
            target_profile=profile_routing.target_profile,
            profile_routing=profile_routing,
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat.wake_stt_direct, "classify_wake_stt_profile", fake_classifier)
    monkeypatch.setattr(matrix_chat.wake_stt_direct, "submit_wake_stt_to_hermes", fake_base_submit)
    monkeypatch.setattr(
        matrix_chat.wake_stt_direct,
        "submit_wake_stt_profile_handoff",
        fake_handoff,
    )
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="build a script to validate Hermes STT",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    second = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert first["delivery"]["command_code_pending"]["target_profile"] == "hermes-stt-smart"
    assert second["delivery"]["ok"] is True
    assert second["delivery"]["direct"]["target_profile"] == "hermes-stt-smart"
    assert classifier_calls["count"] == 1
    assert handoff_calls == [
        {
            "text": "build a script to validate Hermes STT",
            "target_profile": "hermes-stt-smart",
            "trusted_authorised": True,
        }
    ]


def test_matrix_chat_wake_stt_direct_writes_minutes_and_schedules_post(monkeypatch, tmp_path):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    minutes_file = tmp_path / "minutes.jsonl"
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv("HERMES_MINUTES_LOCAL_INDEX_PATH", str(minutes_file))
    captured: dict[str, object] = {}

    async def fake_deliver(*, room_id, body, timing, conversation_key, **_kwargs):
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        routing = matrix_chat.wake_stt_direct.WakeSttProfileRoutingResult(
            target_profile=matrix_chat.wake_stt_direct.WAKE_STT_BLUEPRINTS_NAV_PROFILE,
            requires_command_code=False,
            complex=False,
            risk_class="blueprints_navigation",
            confidence=0.94,
            reason="bounded active browser navigation",
            speech_if_pending="",
            status="classified",
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="Opening the VPS Shared Bridge room.",
            matrix_detail="Wake STT bounded Blueprints navigation\nStatus: blueprints_nav_dispatched",
            status="blueprints_nav_dispatched",
            structured=True,
            raw_assistant_text=json.dumps(
                {
                    "speech": "Opening the VPS Shared Bridge room.",
                    "matrix_detail": (
                        "Wake STT bounded Blueprints navigation\nStatus: blueprints_nav_dispatched"
                    ),
                    "status": "blueprints_nav_dispatched",
                }
            ),
        )
        direct = matrix_chat.wake_stt_direct.HermesSttSubmitResult(
            ok=True,
            status="blueprints_nav_dispatched",
            gate=gate,
            attempted=True,
            fallback_required=False,
            companion=companion,
            target_profile=matrix_chat.wake_stt_direct.WAKE_STT_BLUEPRINTS_NAV_PROFILE,
            profile_routing=routing,
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=direct,
            timing=timing,
        )

    async def fake_publish(payload):
        captured["tts_payload"] = payload
        return {
            "ok": True,
            "status": "queued",
            "event": {"event_id": "$tts-test"},
            "payload": payload,
        }

    async def fake_report(**_kwargs):
        return {"ok": True}

    async def fake_minutes_post(summary):
        captured["minutes_summary"] = summary
        return {"ok": True, "room_id": "!minutes:test.example", "event_id": "$minutes"}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)
    monkeypatch.setattr(matrix_chat, "_post_wake_stt_minutes_summary_safely", fake_minutes_post)
    monkeypatch.setattr(
        matrix_chat.hermes_minutes,
        "summarize_turn_packet_with_model",
        lambda packet, **_kwargs: (model_minutes_summary_for_packet(packet), ""),
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="open the chat room for shared bridge please",
                delivery_mode="direct_local",
                direct_enabled=True,
                instance="local",
            ),
        )
    )

    assert result["delivery"]["ok"] is True
    assert result["delivery"]["minutes"]["ok"] is True
    assert result["delivery"]["minutes"]["matrix_post_scheduled"] is True
    entries = [
        json.loads(line)
        for line in minutes_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert entries[-1]["event_kind"] == "turn_summary"
    assert "shared bridge" in entries[-1]["payload"]["operator_intent_summary"].lower()
    assert entries[-1]["payload"]["schema"] == matrix_chat.hermes_minutes.MINUTES_SUMMARY_SCHEMA
    assert entries[-1]["payload"]["source_pointers"]["tts_utterance_ids"] == [
        captured["tts_payload"]["utterance_id"]
    ]


def test_matrix_chat_sync_records_bridge_messages_to_minutes(monkeypatch, tmp_path):
    matrix_chat._BRIDGE_MINUTES_SEEN_EVENT_IDS.clear()
    minutes_file = tmp_path / "minutes.jsonl"
    monkeypatch.setenv("HERMES_MINUTES_LOCAL_INDEX_PATH", str(minutes_file))
    monkeypatch.setenv("HERMES_MINUTES_ROOM_ID", "!minutes:test.example")
    captured_posts: list[dict[str, object]] = []

    async def fake_minutes_post(summary):
        captured_posts.append(summary)
        return {"ok": True, "room_id": "!minutes:test.example", "event_id": "$minutes"}

    monkeypatch.setattr(matrix_chat, "_post_wake_stt_minutes_summary_safely", fake_minutes_post)
    monkeypatch.setattr(
        matrix_chat.hermes_minutes,
        "summarize_turn_packet_with_model",
        lambda packet, **_kwargs: (
            model_minutes_summary_for_packet(
                packet,
                result=(
                    "Hermes said it should preserve the prior turn order."
                    if packet.get("route_profile") == "matrix-bridge-hermes"
                    else "The operator emphasized paying attention to turn order."
                ),
            ),
            "",
        ),
    )
    settings = {
        "server_id": "tb1",
        "server_label": "TB1",
        "smoke_room_id": "!bridge:test.example",
        "user_id": "@davros-proxy-tb1:test.example",
        "operator_user_id": "",
        "admin_user_id": "",
        "hermes_user_id": "@hermes-local-20260518:test.example",
    }
    payload = {
        "server_id": "tb1",
        "room_updates": [
            {
                "room_id": "!bridge:test.example",
                "messages": [
                    {
                        "event_id": "$operator-1",
                        "room_id": "!bridge:test.example",
                        "sender": "@davros-proxy-tb1:test.example",
                        "origin_server_ts": 1781347040000,
                        "msgtype": "m.text",
                        "body": ("THE CLEAR THING IS TO PAY ATTENTION TO THE ORDER OF TURNS"),
                    },
                    {
                        "event_id": "$hermes-1",
                        "room_id": "!bridge:test.example",
                        "sender": "@hermes-local-20260518:test.example",
                        "origin_server_ts": 1781347050000,
                        "msgtype": "m.text",
                        "body": "I understand; I should preserve the prior turn order.",
                    },
                ],
            },
            {
                "room_id": "!minutes:test.example",
                "messages": [
                    {
                        "event_id": "$minutes-loop",
                        "room_id": "!minutes:test.example",
                        "sender": "@davros-proxy-tb1:test.example",
                        "origin_server_ts": 1781347060000,
                        "msgtype": "m.notice",
                        "body": "Hermes Minutes",
                    }
                ],
            },
        ],
    }

    async def run_record():
        first = matrix_chat._record_matrix_bridge_minutes_from_payload(
            settings=settings,
            payload=payload,
            snapshot=False,
        )
        duplicate = matrix_chat._record_matrix_bridge_minutes_from_payload(
            settings=settings,
            payload=payload,
            snapshot=False,
        )
        await asyncio.sleep(0)
        return first, duplicate

    first, duplicate = asyncio.run(run_record())

    assert first["recorded"] == 2
    assert first["matrix_post_scheduled"] == 2
    assert duplicate["recorded"] == 0
    assert len(captured_posts) == 2
    entries = [
        json.loads(line)
        for line in minutes_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(entries) == 2
    assert entries[0]["conversation_key"] == "matrix-bridge:tb1:room=!bridge:test.example"
    assert entries[0]["payload"]["route_profile"] == "matrix-bridge-operator"
    assert "ORDER OF TURNS" in entries[0]["payload"]["operator_intent_summary"]
    assert entries[1]["payload"]["route_profile"] == "matrix-bridge-hermes"
    assert "prior turn order" in entries[1]["payload"]["result_summary"]


def test_matrix_chat_rooms_merges_raw_sync_when_e2ee_room_list_lags(monkeypatch, tmp_path):
    bridge_id = "!bridge:test.example"
    minutes_id = "!minutes:test.example"

    def room_state(name: str, *, encrypted: bool = True) -> dict[str, object]:
        events = [
            {
                "type": "m.room.name",
                "content": {"name": name},
                "origin_server_ts": 1000,
            }
        ]
        if encrypted:
            events.append(
                {
                    "type": "m.room.encryption",
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                    "origin_server_ts": 1001,
                }
            )
        return {"state": {"events": events}, "timeline": {"events": []}}

    e2ee_sync = {"rooms": {"join": {bridge_id: room_state("Bridge")}}, "next_batch": "e2ee"}
    raw_sync = {
        "rooms": {
            "join": {
                bridge_id: room_state("Bridge"),
                minutes_id: room_state("Minutes"),
            }
        },
        "next_batch": "raw",
    }

    monkeypatch.setattr(
        matrix_chat,
        "_settings",
        lambda: {
            "server_id": "tb1",
            "server_label": "TB1",
            "room_settings_file": str(tmp_path / "room-settings.json"),
            "admin_access_token": "",
        },
    )

    async def fake_sync_for_chat(**_kwargs):
        return e2ee_sync, object()

    async def fake_sync(**_kwargs):
        return raw_sync

    monkeypatch.setattr(matrix_chat, "_sync_for_chat", fake_sync_for_chat)
    monkeypatch.setattr(matrix_chat, "_sync", fake_sync)

    result = asyncio.run(matrix_chat.matrix_chat_rooms())

    titles = [room["display_name"] for room in result["joined"]]
    assert "Bridge" in titles
    assert "Minutes" in titles
    minutes = [room for room in result["joined"] if room["room_id"] == minutes_id][0]
    assert minutes["encrypted"] is True


def test_minutes_config_reads_matrix_targets(tmp_path, monkeypatch):
    config_path = tmp_path / "minutes.json"
    config_path.write_text(
        json.dumps(
            {
                "schema": matrix_chat.hermes_minutes.MINUTES_CONFIG_SCHEMA,
                "enabled": True,
                "local_enabled": True,
                "matrix_targets": {
                    "tb1": {
                        "server_id": "tb1",
                        "room_id": "!tb1-minutes:test.example",
                        "require_e2ee": True,
                    },
                    "vps": {
                        "server_id": "vps",
                        "room_id": "!vps-minutes:test.example",
                        "room_name": "Minutes",
                        "require_e2ee": True,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HERMES_MINUTES_CONFIG_FILE", str(config_path))

    config = matrix_chat.hermes_minutes.read_minutes_config()

    assert config["matrix_post_enabled"] is True
    assert config["matrix_targets"]["tb1"]["room_id"] == "!tb1-minutes:test.example"
    assert config["matrix_targets"]["vps"] == {
        "server_id": "vps",
        "room_id": "!vps-minutes:test.example",
        "room_name": "Minutes",
        "matrix_post_enabled": True,
        "require_e2ee": True,
    }


def test_matrix_chat_minutes_post_routes_vps_summary_to_vps_target(monkeypatch):
    captured = {}
    config = {
        "enabled": True,
        "matrix_post_enabled": True,
        "server_id": "tb1",
        "room_id": "!tb1-minutes:test.example",
        "require_e2ee": True,
        "matrix_targets": {
            "vps": {
                "server_id": "vps",
                "room_id": "!vps-minutes:test.example",
                "room_name": "Minutes",
                "matrix_post_enabled": True,
                "require_e2ee": True,
            }
        },
    }

    monkeypatch.setattr(matrix_chat.hermes_minutes, "read_minutes_config", lambda: config)

    async def fake_room_is_encrypted(room_id):
        captured["encrypted_room_id"] = room_id
        captured["encrypted_server"] = matrix_chat._CURRENT_MATRIX_SERVER.get()
        return True

    async def fake_get_e2ee_client():
        return None

    async def fake_matrix_request(method, path, *, json_body=None, expected=None, **_kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        captured["expected"] = expected
        captured["send_server"] = matrix_chat._CURRENT_MATRIX_SERVER.get()
        return {"event_id": "$vps-minutes"}

    monkeypatch.setattr(matrix_chat, "_matrix_room_is_encrypted", fake_room_is_encrypted)
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)
    monkeypatch.setattr(matrix_chat, "_matrix_request", fake_matrix_request)

    result = asyncio.run(
        matrix_chat._post_wake_stt_minutes_summary_message(
            {
                "schema": matrix_chat.hermes_minutes.MINUTES_SUMMARY_SCHEMA,
                "conversation_key": "wake-stt:vps:room=!shared:test.example",
                "time": "2026-06-13T00:00:00Z",
                "route": "direct_vps",
                "route_status": "delivered",
                "route_profile": "vps-direct-profile",
                "operator_intent_summary": "Operator asked for a VPS check.",
                "assistant_action_summary": "Hermes VPS answered.",
                "result_summary": "The VPS route responded.",
                "open_question": "",
                "entities": [],
                "problems": [],
                "followup_affordances": [],
                "source_pointers": {},
                "delivery": {"server_id": "vps"},
                "confidence": 0.8,
            }
        )
    )

    assert result["ok"] is True
    assert result["server_id"] == "vps"
    assert result["room_id"] == "!vps-minutes:test.example"
    assert result["target_key"] == "vps"
    assert captured["encrypted_room_id"] == "!vps-minutes:test.example"
    assert captured["encrypted_server"] == "vps"
    assert captured["send_server"] == "vps"
    assert captured["method"] == "PUT"
    assert "%21vps-minutes%3Atest.example" in captured["path"]
    assert captured["json_body"]["org.xarta.system_message"]["kind"] == "hermes_minutes_summary"


def test_matrix_chat_wake_stt_handoff_assignment_is_info_only_and_speech_suppressed():
    body = matrix_chat._wake_stt_handoff_assignment_body(
        {
            "target_profile": "hermes-stt-local",
            "request_text": "authorisation alpha one seven create a file called Dave Computer",
            "reason": "filesystem mutation",
        }
    )
    content = matrix_chat._matrix_wake_stt_handoff_assignment_content(
        body=body,
        target_profile="hermes-stt-local",
        instance="local",
        candidate_source="payload2",
        command="execute",
        wake_word="Computer",
        candidate_revision="rev-1",
    )

    assert body.startswith("Wake STT handoff assigned: hermes-stt-local")
    assert not body.lower().startswith("hermes:")
    assert "alpha one seven" not in body.lower()
    assert content["xarta_source"] == "wake_stt_handoff_assignment"
    assert content["xarta_handoff_target_profile"] == "hermes-stt-local"
    assert content["xarta_suppress_speech"] is True
    assert content["suppress_speech"] is True


def test_matrix_chat_wake_stt_pre_roll_uses_command_code_accepted_message(monkeypatch, tmp_path):
    published: list[dict[str, object]] = []
    room_id = "!bridge:test.example"
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    matrix_chat._wake_stt_clear_pre_roll_pool_state_for_tests()
    config = tmp_path / "wake-stt-pre-roll.json"
    config.write_text(
        json.dumps(
            {
                "delay_ms": 1,
                "utterances": ["Default wait."],
                "special_utterances": {"command_code_accepted": ["Command Codes accepted."]},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_PRE_ROLL_CONFIG_FILE", str(config))
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", raising=False)
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    pending_key = matrix_chat._wake_stt_pending_command_key(room_id, "local")
    matrix_chat._wake_stt_store_pending_command(
        pending_key,
        "create a new file called Dave",
    )

    async def fake_deliver(**kwargs):
        await asyncio.sleep(0.07)
        body = kwargs["body"]
        trusted = bool(kwargs.get("trusted_authorised"))
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=trusted,
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="authorised retry ok",
            matrix_detail="authorised retry ok",
            status="ok",
            structured=True,
            raw_assistant_text=json.dumps(
                {
                    "speech": "authorised retry ok",
                    "matrix_detail": "authorised retry ok",
                    "status": "ok",
                },
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish_tts_payload(payload):
        published.append(payload)
        return {
            "ok": True,
            "event": {"event_id": f"tts-{len(published)}"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            room_id,
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["ok"] is True
    assert result["delivery"]["pre_roll"]["queued"] is True
    assert result["delivery"]["pre_roll"]["reason"] == "command_code_accepted"
    assert result["delivery"]["pre_roll"]["speech"] == "Command Codes accepted."
    assert published[0]["text"] == "Command Codes accepted."
    assert published[0]["metadata"]["pre_roll"] is True
    assert published[0]["metadata"]["pre_roll_reason"] == "command_code_accepted"
    assert published[1]["text"] == "authorised retry ok"


def test_matrix_chat_wake_stt_pre_roll_uses_inline_command_code_message(monkeypatch, tmp_path):
    published: list[dict[str, object]] = []
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    matrix_chat._wake_stt_clear_pre_roll_pool_state_for_tests()
    config = tmp_path / "wake-stt-pre-roll.json"
    config.write_text(
        json.dumps(
            {
                "delay_ms": 1,
                "utterances": ["Default wait."],
                "special_utterances": {
                    "command_code_accepted": ["Command Codes accepted."],
                    "command_code_inline_accepted": ["OK. Processing."],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_PRE_ROLL_CONFIG_FILE", str(config))
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", raising=False)
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_deliver(**kwargs):
        await asyncio.sleep(0.07)
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="inline authorised ok",
            matrix_detail="inline authorised ok",
            status="ok",
            structured=True,
            raw_assistant_text=json.dumps(
                {
                    "speech": "inline authorised ok",
                    "matrix_detail": "inline authorised ok",
                    "status": "ok",
                },
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish_tts_payload(payload):
        published.append(payload)
        return {
            "ok": True,
            "event": {"event_id": f"tts-{len(published)}"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create a new file called Dave authorisation alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["ok"] is True
    assert result["delivery"]["pre_roll"]["queued"] is True
    assert result["delivery"]["pre_roll"]["reason"] == "command_code_inline_accepted"
    assert result["delivery"]["pre_roll"]["speech"] == "OK. Processing."
    assert published[0]["text"] == "OK. Processing."
    assert published[0]["metadata"]["pre_roll_reason"] == "command_code_inline_accepted"
    assert published[1]["text"] == "inline authorised ok"


def test_matrix_chat_wake_stt_terminal_gate_failure_becomes_command_code_challenge(
    monkeypatch,
):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="I could not safely verify that delegated work, so I did not start it.",
            matrix_detail="Delegation gate failed closed before delegated work started.",
            status="delegation_gate_failed_closed",
            structured=True,
            raw_assistant_text=json.dumps(
                {
                    "speech": (
                        "I could not safely verify that delegated work, so I did not start it."
                    ),
                    "matrix_detail": (
                        "Delegation gate failed closed before delegated work started."
                    ),
                    "status": "delegation_gate_failed_closed",
                },
                sort_keys=True,
            ),
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    async def fake_report(**_kwargs):
        return {"ok": True}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(matrix_chat, "_send_wake_stt_direct_response_report_safely", fake_report)

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create a new file called Dave6",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    delivery = result["delivery"]
    assert delivery["status"] == "command_code_required"
    assert delivery["command_code_pending"] == {"held": True, "scope": "next_wake_turn"}
    assert delivery["direct"]["companion"]["speech"] == ("Authorisation Command Code required.")
    assert "Delegation gate failed closed" not in json.dumps(delivery)
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS


def test_matrix_chat_wake_stt_wrong_malformed_extra_and_stale_codes_do_not_retry(
    monkeypatch,
):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    calls = []

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        calls.append(body.text)
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="needs code",
            matrix_detail="needs code",
            status="command_code_required",
            structured=True,
            raw_assistant_text='{"speech":"needs code","matrix_detail":"needs code","status":"command_code_required"}',
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)

    async def challenge_then(reply: str):
        matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
        await matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="delete the file called Dave",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
        return await matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text=reply,
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )

    wrong = asyncio.run(challenge_then("authorize wrong code words"))
    malformed = asyncio.run(challenge_then("authorisation alpha one"))
    extra = asyncio.run(challenge_then("authorisation alpha one seven please"))
    new_request = asyncio.run(challenge_then("what time is it?"))
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    stale = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    wrong_delivery = wrong["delivery"]
    malformed_delivery = malformed["delivery"]
    extra_delivery = extra["delivery"]
    new_request_delivery = new_request["delivery"]
    stale_delivery = stale["delivery"]

    assert wrong_delivery["status"] == "command_code_aborted"
    assert malformed_delivery["status"] == "command_code_aborted"
    assert extra_delivery["status"] == "command_code_aborted"
    assert new_request_delivery["status"] == "command_code_required"
    assert stale_delivery["status"] == "command_code_stale"
    assert all(
        item["direct"]["authorised"] is False
        for item in (
            wrong_delivery,
            malformed_delivery,
            extra_delivery,
            new_request_delivery,
            stale_delivery,
        )
    )
    assert calls == [
        "delete the file called Dave",
        "delete the file called Dave",
        "delete the file called Dave",
        "delete the file called Dave",
        "what time is it?",
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_intervening_turn_preserves_pending_across_instances(
    monkeypatch,
):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    calls = []

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        calls.append({"text": body.text, "trusted": bool(kwargs.get("trusted_authorised"))})
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=bool(kwargs.get("trusted_authorised")),
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="needs code",
            matrix_detail="needs code",
            status="command_code_required",
            structured=True,
            raw_assistant_text='{"speech":"needs code","matrix_detail":"needs code","status":"command_code_required"}',
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create file Dave 10",
                instance="local",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    intervening = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="what time is it",
                instance="vps",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    later_code = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                instance="local",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert intervening["delivery"]["status"] == "command_code_required"
    assert later_code["delivery"]["ok"] is True
    assert later_code["delivery"]["direct"]["authorised"] is True
    assert calls == [
        {"text": "create file Dave 10", "trusted": False},
        {"text": "what time is it", "trusted": False},
        {"text": "create file Dave 10", "trusted": True},
    ]
    local_key = matrix_chat._wake_stt_pending_command_key("!bridge:test.example", "local")
    vps_key = matrix_chat._wake_stt_pending_command_key("!bridge:test.example", "vps")
    assert local_key not in matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS
    assert vps_key in matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS


def test_matrix_chat_wake_stt_authorised_retry_reports_profile_transport_error(
    monkeypatch,
):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    calls: list[dict[str, object]] = []
    published: list[dict[str, object]] = []
    reports: list[dict[str, object]] = []

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        trusted = bool(kwargs.get("trusted_authorised"))
        calls.append({"text": body.text, "trusted": trusted})
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
            trusted_authorised=trusted,
        )
        if trusted:
            return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
                ok=False,
                status="request_error",
                route="direct_local",
                gate=gate,
                direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                    ok=False,
                    status="request_error",
                    gate=gate,
                    attempted=True,
                    fallback_required=True,
                    error="ConnectError",
                    target_profile="hermes-stt-local",
                ),
                fallback_reason="request_error",
            )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="needs code",
            matrix_detail="needs code",
            status="command_code_required",
            structured=True,
            raw_assistant_text='{"speech":"needs code","matrix_detail":"needs code","status":"command_code_required"}',
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        published.append(payload)
        return {
            "ok": True,
            "event": {"event_id": f"$tts-{len(published)}"},
            "payload": payload,
        }

    async def fake_report(**kwargs):
        reports.append(kwargs)
        return {"ok": True, "event_id": f"$report-{len(reports)}"}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create file Dave 10",
                instance="local",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    accepted_error = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="authorize alpha one seven",
                instance="local",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert accepted_error["delivery"]["status"] == "request_error"
    assert accepted_error["delivery"]["direct"]["authorised"] is True
    assert accepted_error["delivery"]["direct"]["companion"]["speech"] == (
        "Command Code accepted, but the local Hermes profile did not respond."
    )
    assert (
        "selected profile `hermes-stt-local` returned `request_error`"
        in reports[-1]["matrix_detail"]
    )
    assert published[-1]["text"] == (
        "Command Code accepted, but the local Hermes profile did not respond."
    )
    stages = [mark["stage"] for mark in accepted_error["delivery"]["timing"]["marks"]]
    assert "command_code_authorised_failure_companion" in stages
    assert calls == [
        {"text": "create file Dave 10", "trusted": False},
        {"text": "create file Dave 10", "trusted": True},
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_bare_code_words_do_not_authorise_pending(monkeypatch):
    matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS.clear()
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", "0")
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"alpha","aliases":["alpha one seven"]}]}',
    )
    calls = []

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        calls.append({"text": body.text, "trusted": bool(kwargs.get("trusted_authorised"))})
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(
            body.text,
            matrix_chat.wake_stt_direct.command_codes_from_env(),
        )
        companion = matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
            speech="needs code",
            matrix_detail="needs code",
            status="command_code_required",
            structured=True,
            raw_assistant_text='{"speech":"needs code","matrix_detail":"needs code","status":"command_code_required"}',
        )
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=companion.raw_assistant_text,
                companion=companion,
            ),
        )

    async def fake_publish(payload):
        return {"ok": True, "event": {"event_id": "$tts"}, "payload": payload}

    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish)

    first = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="create file Dave 11",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )
    bare_code = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="alpha one seven",
                delivery_mode="direct_local",
                direct_enabled=True,
            ),
        )
    )

    assert first["delivery"]["status"] == "command_code_required"
    assert bare_code["delivery"]["status"] == "command_code_aborted"
    assert bare_code["delivery"]["direct"]["authorised"] is False
    assert calls == [{"text": "create file Dave 11", "trusted": False}]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_direct_route_queues_tts(monkeypatch):
    captured = {}

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=(
                    '{"speech":"I am okay.","matrix_detail":"I am okay in detail.","status":"ok"}'
                ),
                companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                    speech="I am okay.",
                    matrix_detail="I am okay in detail.",
                    status="ok",
                    structured=True,
                    raw_assistant_text=(
                        '{"speech":"I am okay.","matrix_detail":"I am okay in detail.",'
                        '"status":"ok"}'
                    ),
                ),
            ),
        )

    async def fake_publish_tts_payload(payload):
        captured["text"] = payload["text"]
        captured["source"] = payload["source"]
        captured["agent_id"] = payload["agent_id"]
        captured["metadata"] = payload["metadata"]
        captured["interrupt"] = payload["interrupt"]
        return {
            "ok": True,
            "event": {"event_id": "tts-wake-direct"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**kwargs):
        captured["report"] = kwargs
        return {"ok": True, "event_id": "$response-copy"}

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="Are you okay?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-tts",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["route"] == "direct_local"
    assert result["delivery"]["direct"]["companion"]["speech"] == "I am okay."
    assert result["delivery"]["pre_roll"]["queued"] is False
    assert result["delivery"]["pre_roll"]["pending_after_threshold"] is False
    assert result["delivery"]["pre_roll"]["direct_receipt_status"] == "delivered"
    assert result["delivery"]["tts"]["ok"] is True
    assert result["delivery"]["tts"]["event_id"] == "tts-wake-direct"
    stages = [mark["stage"] for mark in result["delivery"]["timing"]["marks"]]
    assert stages[0] == "stt_final_transcript_received"
    assert "blueprints_delivery_task_created" in stages
    assert "tts_queued" in stages
    assert "matrix_detail_scheduled" in stages
    assert stages[-1] == "route_response"
    assert captured["text"] == "I am okay."
    assert captured["source"] == "hermes-stt"
    assert captured["agent_id"] == "hermes-stt"
    assert captured["interrupt"] is True
    assert captured["metadata"]["schema"] == "xarta.wake-stt.direct-response.v1"
    assert captured["metadata"]["speech_elected_by"] == "hermes-stt"
    assert captured["metadata"]["tts_queue_policy"] == "hermes_priority_stream"
    assert captured["metadata"]["tts_priority"] == 100
    assert captured["report"]["matrix_detail"] == "I am okay in detail."
    assert result["delivery"]["assistant_report_scheduled"] is True
    assert result["delivery"]["tts_elected_by_hermes"] is True
    assert "api_server_key" not in str(captured).lower()
    assert "secret" not in str(captured).lower()


def test_matrix_chat_wake_stt_vps_direct_route_queues_tts(monkeypatch):
    captured = {}

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_vps",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=(
                    '{"speech":"sixteen thirty-one","matrix_detail":"VPS time check completed.",'
                    '"status":"ok"}'
                ),
                companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                    speech="sixteen thirty-one",
                    matrix_detail="VPS time check completed.",
                    status="ok",
                    structured=True,
                    raw_assistant_text=(
                        '{"speech":"sixteen thirty-one","matrix_detail":"VPS time check completed.",'
                        '"status":"ok"}'
                    ),
                ),
            ),
        )

    async def fake_publish_tts_payload(payload):
        captured["text"] = payload["text"]
        captured["source"] = payload["source"]
        captured["agent_id"] = payload["agent_id"]
        captured["client_id"] = payload["client_id"]
        captured["voice"] = payload["voice"]
        captured["metadata"] = payload["metadata"]
        captured["interrupt"] = payload["interrupt"]
        return {
            "ok": True,
            "event": {"event_id": "tts-wake-vps-direct"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**kwargs):
        captured["report"] = kwargs
        return {"ok": True, "event_id": "$vps-response-copy"}

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_HERMES_STT_VPS_MODEL", "vps-test-agent")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_VPS_TTS_VOICE", "vps-test-voice.wav")
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!vps:test.example",
            matrix_chat._WakeSttMessageBody(
                text="What's the time?",
                instance="vps",
                candidate_source="payload0",
                command="execute",
                wake_word="Mini-Me",
                candidate_revision="wake-vps-time",
                delivery_mode="direct_vps",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["route"] == "direct_vps"
    assert result["delivery"]["direct"]["companion"]["speech"] == "sixteen thirty-one"
    assert result["delivery"]["pre_roll"]["direct_receipt_status"] == "delivered"
    assert result["delivery"]["tts"]["ok"] is True
    assert result["delivery"]["tts"]["event_id"] == "tts-wake-vps-direct"
    assert result["delivery"]["tts"]["voice_set"] is True
    assert captured["text"] == "sixteen thirty-one"
    assert captured["source"] == "vps-test-agent"
    assert captured["agent_id"] == "vps-test-agent"
    assert captured["client_id"] == "vps-test-agent:wake-to-talk"
    assert captured["voice"] == "vps-test-voice.wav"
    assert captured["interrupt"] is True
    assert captured["metadata"]["route"] == "direct_vps"
    assert captured["metadata"]["wake_instance"] == "vps"
    assert captured["metadata"]["hermes_instance"] == "vps-test-agent"
    assert captured["metadata"]["speech_elected_by"] == "vps-test-agent"
    assert captured["report"]["matrix_detail"] == "VPS time check completed."
    assert result["delivery"]["assistant_report_scheduled"] is True
    assert result["delivery"]["tts_elected_by_hermes"] is True
    assert "api_server_key" not in str(captured).lower()
    assert "secret" not in str(captured).lower()


def test_matrix_chat_wake_stt_direct_route_pre_rolls_then_speaks_final(monkeypatch, tmp_path):
    published: list[dict[str, object]] = []
    pre_roll_config = tmp_path / "wake-stt-pre-roll.json"
    pre_roll_config.write_text(
        json.dumps({"delay_ms": 1, "utterances": ["I heard you."]}),
        encoding="utf-8",
    )

    async def fake_deliver(**kwargs):
        await asyncio.sleep(0.02)
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=(
                    '{"speech":"I am online.","matrix_detail":"Wake STT check completed.",'
                    '"status":"ok"}'
                ),
                companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                    speech="I am online.",
                    matrix_detail="Wake STT check completed.",
                    status="ok",
                    structured=True,
                    raw_assistant_text="{}",
                ),
            ),
        )

    async def fake_publish_tts_payload(payload):
        published.append(payload)
        return {
            "ok": True,
            "event": {"event_id": f"tts-{len(published)}"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**kwargs):
        return {"ok": True, "event_id": "$response-copy"}

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_PRE_ROLL_CONFIG_FILE", str(pre_roll_config))
    matrix_chat._wake_stt_clear_pre_roll_pool_state_for_tests()
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_wake_stt_direct_pre_roll_delay_seconds",
        lambda: 0.001,
    )
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="Are you okay?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-pre-roll",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["route"] == "direct_local"
    assert result["delivery"]["pre_roll_tts"]["event_id"] == "tts-1"
    assert result["delivery"]["pre_roll"]["queued"] is True
    assert result["delivery"]["pre_roll"]["pending_after_threshold"] is True
    assert result["delivery"]["pre_roll"]["direct_receipt_status"] == "delivered"
    assert result["delivery"]["pre_roll"]["meaning"] == (
        "pending_direct_task_ack_not_hermes_receipt"
    )
    assert result["delivery"]["tts"]["event_id"] == "tts-2"
    assert [payload["text"] for payload in published] == ["I heard you.", "I am online."]
    assert published[0]["interrupt"] is True
    assert published[0]["metadata"]["pre_roll"] is True
    assert published[0]["metadata"]["speech_elected_by"] == "blueprints_transport_ack"
    assert published[0]["priority"] == 100
    assert published[0]["queue_policy"] == "hermes_priority_stream"
    assert published[1]["metadata"]["pre_roll"] is False
    assert published[1]["metadata"]["speech_elected_by"] == "hermes-stt"
    assert published[1]["priority"] == 100
    assert published[1]["queue_policy"] == "hermes_priority_stream"


def test_matrix_chat_wake_stt_direct_route_speaks_short_unstructured_response(monkeypatch):
    captured = {}

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text="Plain raw assistant text should not be auto-spoken.",
                companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                    speech="Plain raw assistant text should be spoken as fallback.",
                    matrix_detail="Plain raw assistant text should not be auto-spoken.",
                    status="unstructured_speech_fallback",
                    structured=False,
                    raw_assistant_text="Plain raw assistant text should not be auto-spoken.",
                ),
            ),
        )

    async def fake_publish_tts_payload(payload):
        captured["text"] = payload["text"]
        return {
            "ok": True,
            "event": {"event_id": "tts-unstructured-fallback"},
            "payload": {
                "utterance_id": payload["utterance_id"],
                "source": payload["source"],
                "agent_id": payload["agent_id"],
            },
        }

    async def fake_report(**kwargs):
        captured["report"] = kwargs
        return {"ok": True, "event_id": "$response-copy"}

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="Are you okay?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-tts-stream",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["route"] == "direct_local"
    assert result["delivery"]["tts"]["status"] == "queued"
    assert result["delivery"]["tts"]["event_id"] == "tts-unstructured-fallback"
    assert result["delivery"]["tts_elected_by_hermes"] is True
    assert captured["text"] == "Plain raw assistant text should be spoken as fallback."
    assert (
        captured["report"]["matrix_detail"] == "Plain raw assistant text should not be auto-spoken."
    )
    assert result["delivery"]["assistant_report_scheduled"] is True


def test_matrix_chat_wake_stt_direct_route_preserves_long_elected_speech(monkeypatch):
    captured = {}
    long_speech = " ".join(["computer"] * 360)

    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=True,
            status="delivered",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=True,
                status="delivered",
                gate=gate,
                attempted=True,
                fallback_required=False,
                assistant_text=json.dumps(
                    {
                        "speech": long_speech,
                        "matrix_detail": "Long spoken essay requested and elected.",
                        "status": "long_speech",
                    }
                ),
                companion=matrix_chat.wake_stt_direct.HermesSttCompanionOutput(
                    speech=long_speech,
                    matrix_detail="Long spoken essay requested and elected.",
                    status="long_speech",
                    structured=True,
                    raw_assistant_text="{}",
                ),
            ),
        )

    async def fake_publish_tts_payload(payload):
        captured["text"] = payload["text"]
        return {
            "ok": True,
            "event": {"event_id": "tts-long"},
            "payload": {"utterance_id": payload["utterance_id"]},
        }

    async def fake_report(**kwargs):
        captured["report"] = kwargs
        return {"ok": True, "event_id": "$response-copy"}

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)
    monkeypatch.setattr(
        matrix_chat,
        "_send_wake_stt_direct_response_report_safely",
        fake_report,
    )

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="Read a long essay.",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-long-speech",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    assert result["delivery"]["route"] == "direct_local"
    assert captured["text"] == long_speech
    assert "I sent the rest to Matrix" not in captured["text"]
    assert result["delivery"]["tts"]["event_id"] == "tts-long"


def test_matrix_chat_wake_stt_direct_route_failure_has_receipt_without_matrix_post(monkeypatch):
    async def fake_deliver(**kwargs):
        body = kwargs["body"]
        gate = matrix_chat.wake_stt_direct.apply_command_code_gate(body.text, [])
        return matrix_chat.wake_stt_direct.WakeSttDeliveryResult(
            ok=False,
            status="request_error",
            route="direct_local",
            gate=gate,
            direct=matrix_chat.wake_stt_direct.HermesSttSubmitResult(
                ok=False,
                status="request_error",
                gate=gate,
                attempted=True,
                fallback_required=False,
                error="connection refused",
            ),
            fallback_reason="request_error",
        )

    async def fake_publish_tts_payload(payload):
        raise AssertionError(f"TTS should not queue without elected speech: {payload}")

    monkeypatch.setenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", "1")
    monkeypatch.setattr(matrix_chat, "_deliver_wake_stt_with_direct_fallback", fake_deliver)
    monkeypatch.setattr(matrix_chat, "_publish_tts_utterance_payload", fake_publish_tts_payload)

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="Are you okay?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-direct-failure",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    delivery = result["delivery"]
    assert result["event_id"] is None
    assert delivery["ok"] is False
    assert delivery["route"] == "direct_local"
    assert delivery["status"] == "request_error"
    assert delivery["fallback_reason"] == "request_error"
    assert delivery["matrix"] == {}
    assert delivery["direct"]["fallback_required"] is False
    assert delivery["pre_roll"]["queued"] is False
    assert delivery["pre_roll"]["pending_after_threshold"] is False
    assert delivery["pre_roll"]["direct_receipt_status"] == "failed"
    assert delivery["pre_roll"]["failure_status"] == "request_error"
    assert delivery["tts"]["reason"] == "no_hermes_elected_speech"


def test_matrix_chat_wake_stt_route_explicit_matrix_mode_posts_addressed_transcript(monkeypatch):
    captured = {}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$explicit-matrix"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text="What is the time?",
                instance="local",
                candidate_source="payload0",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-matrix",
                delivery_mode="matrix",
                direct_enabled=False,
            ),
        )
    )

    assert result["event_id"] == "$explicit-matrix"
    assert captured["room_id"] == "!bridge:test.example"
    assert captured["content"]["body"].startswith("hermes: ")
    assert "What is the time?" in captured["content"]["body"]
    assert captured["content"]["xarta_capture_mode"] == "wake_to_talk"


def test_matrix_chat_wake_stt_route_rolls_direct_request_back_to_matrix(monkeypatch):
    captured = {}

    class FakeE2EEClient:
        async def send_message_content(self, room_id, content):
            captured["room_id"] = room_id
            captured["content"] = content
            return {"room_id": room_id, "event_id": "$route-rollback"}

    async def fake_get_e2ee_client(settings=None):
        return FakeE2EEClient()

    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_ROUTE_ENABLED", raising=False)
    monkeypatch.setenv(
        "BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON",
        '{"command_codes":[{"id":"route","aliases":["route seven pine"]}]}',
    )
    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)

    result = asyncio.run(
        matrix_chat.matrix_chat_send_wake_stt(
            "!bridge:test.example",
            matrix_chat._WakeSttMessageBody(
                text=(
                    "authorisation route seven pine This command is authorised. "
                    "Please check status."
                ),
                instance="local",
                candidate_source="payload2",
                command="execute",
                wake_word="Computer",
                candidate_revision="wake-local-route",
                delivery_mode="direct-hermes",
                direct_enabled=True,
            ),
        )
    )

    assert result["event_id"] == "$route-rollback"
    assert result["delivery"]["route"] == "matrix"
    assert result["delivery"]["readback"]["rollback_reason"] == "direct_route_disabled"
    assert captured["content"]["body"].startswith("hermes: ")
    assert "please check status." in captured["content"]["body"]
    assert "route seven pine" not in captured["content"]["body"].lower()
    assert "authorised" not in captured["content"]["body"].lower()


def test_matrix_chat_audio_message_content_uses_matrix_audio_shape():
    content = matrix_chat._audio_message_content(
        content_uri="mxc://example.org/audio123",
        filename="voice-note.webm",
        mimetype="audio/webm",
        size=12345,
        duration_ms=987,
    )

    assert content == {
        "msgtype": "m.audio",
        "body": "voice-note.webm",
        "filename": "voice-note.webm",
        "url": "mxc://example.org/audio123",
        "info": {
            "mimetype": "audio/webm",
            "size": 12345,
            "duration": 987,
        },
    }


def test_matrix_chat_media_message_content_uses_encrypted_file_shape():
    encrypted_file = {
        "v": "v2",
        "url": "mxc://example.org/encrypted123",
        "key": {"kty": "oct", "k": "not-a-real-key"},
        "hashes": {"sha256": "not-a-real-hash"},
        "iv": "not-a-real-iv",
    }

    content = matrix_chat._media_message_content(
        content_uri="mxc://example.org/encrypted123",
        filename="proof.png",
        mimetype="image/png",
        size=67,
        encrypted_file=encrypted_file,
    )

    assert content == {
        "msgtype": "m.image",
        "body": "proof.png",
        "filename": "proof.png",
        "file": encrypted_file,
        "info": {
            "mimetype": "image/png",
            "size": 67,
        },
    }
    assert "url" not in content


def test_matrix_chat_send_attachment_route_returns_upload_contract(monkeypatch):
    captured = {}

    class FakeUpload:
        filename = "proof.png"
        content_type = "image/png"

        async def read(self, _limit):
            return b"plain-image-bytes"

    async def fake_media_event_content(**kwargs):
        captured["media"] = kwargs
        filename = kwargs["filename"]
        mimetype = kwargs["mimetype"]
        return (
            {
                "msgtype": "m.image",
                "body": filename,
                "filename": filename,
                "file": {
                    "v": "v2",
                    "url": "mxc://example.org/encrypted-image",
                },
                "info": {
                    "mimetype": mimetype,
                    "size": len(kwargs["content"]),
                },
            },
            "mxc://example.org/encrypted-image",
            True,
            True,
        )

    async def fake_send_room_message_content(**kwargs):
        captured["send"] = kwargs
        return {
            "room_id": kwargs["room_id"],
            "event_id": "$image-event:test.example",
        }

    monkeypatch.setattr(matrix_chat, "_matrix_media_event_content", fake_media_event_content)
    monkeypatch.setattr(matrix_chat, "_send_room_message_content", fake_send_room_message_content)

    response = asyncio.run(
        matrix_chat.matrix_chat_send_attachment("!room:test.example", FakeUpload())
    )

    assert captured["media"] == {
        "room_id": "!room:test.example",
        "content": b"plain-image-bytes",
        "filename": "proof.png",
        "mimetype": "image/png",
    }
    assert captured["send"]["txn_prefix"] == "bp-attachment"
    assert captured["send"]["content"]["file"]["url"] == "mxc://example.org/encrypted-image"
    assert response == {
        "room_id": "!room:test.example",
        "event_id": "$image-event:test.example",
        "content_uri": "mxc://example.org/encrypted-image",
        "filename": "proof.png",
        "mimetype": "image/png",
        "size": len(b"plain-image-bytes"),
        "msgtype": "m.image",
        "encrypted_room": True,
        "encrypted_attachment": True,
    }


def test_matrix_chat_send_audio_route_returns_duration_upload_contract(monkeypatch):
    captured = {}

    class FakeUpload:
        filename = "voice-note.wav"
        content_type = "audio/wav"

        async def read(self, _limit):
            return b"RIFF-audio-bytes"

    async def fake_media_event_content(**kwargs):
        captured["media"] = kwargs
        return (
            {
                "msgtype": "m.audio",
                "body": kwargs["filename"],
                "filename": kwargs["filename"],
                "url": "mxc://example.org/audio",
                "info": {
                    "mimetype": kwargs["mimetype"],
                    "size": len(kwargs["content"]),
                    "duration": kwargs["duration_ms"],
                },
            },
            "mxc://example.org/audio",
            False,
            False,
        )

    async def fake_send_room_message_content(**kwargs):
        captured["send"] = kwargs
        return {
            "room_id": kwargs["room_id"],
            "event_id": "$audio-event:test.example",
        }

    monkeypatch.setattr(matrix_chat, "_matrix_media_event_content", fake_media_event_content)
    monkeypatch.setattr(matrix_chat, "_send_room_message_content", fake_send_room_message_content)

    response = asyncio.run(
        matrix_chat.matrix_chat_send_audio(
            "!room:test.example",
            FakeUpload(),
            duration_ms=1234,
        )
    )

    assert captured["media"] == {
        "room_id": "!room:test.example",
        "content": b"RIFF-audio-bytes",
        "filename": "voice-note.wav",
        "mimetype": "audio/wav",
        "duration_ms": 1234,
    }
    assert captured["send"]["txn_prefix"] == "bp-audio"
    assert captured["send"]["content"]["url"] == "mxc://example.org/audio"
    assert response == {
        "room_id": "!room:test.example",
        "event_id": "$audio-event:test.example",
        "content_uri": "mxc://example.org/audio",
        "filename": "voice-note.wav",
        "mimetype": "audio/wav",
        "size": len(b"RIFF-audio-bytes"),
        "msgtype": "m.audio",
        "encrypted_room": False,
        "encrypted_attachment": False,
    }


def test_matrix_chat_media_fields_reduce_attachment_metadata():
    content = {
        "msgtype": "m.image",
        "body": "proof.png",
        "filename": "proof.png",
        "file": {
            "v": "v2",
            "url": "mxc://example.org/encrypted123",
            "key": {"kty": "oct", "k": "not-a-real-key"},
            "hashes": {"sha256": "not-a-real-hash"},
            "iv": "not-a-real-iv",
        },
        "info": {
            "mimetype": "image/png",
            "size": 67,
            "w": 640,
            "h": 480,
        },
    }

    media = matrix_chat._media_fields_from_content(content)

    assert media == {
        "msgtype": "m.image",
        "filename": "proof.png",
        "mimetype": "image/png",
        "size": 67,
        "content_uri": "mxc://example.org/encrypted123",
        "encrypted_file": True,
        "width": 640,
        "height": 480,
    }
    assert "key" not in media
    assert "hashes" not in media
    assert "iv" not in media


def _minimal_docx(text: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as zf:
        zf.writestr(
            "word/document.xml",
            (
                '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
                "<w:body><w:p><w:r><w:t>" + text + "</w:t></w:r></w:p></w:body></w:document>"
            ),
        )
    return output.getvalue()


def _minimal_xlsx(values):
    output = io.BytesIO()
    shared = "".join(f"<si><t>{value}</t></si>" for value in values)
    with zipfile.ZipFile(output, "w") as zf:
        zf.writestr(
            "xl/sharedStrings.xml",
            (
                '<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                + shared
                + "</sst>"
            ),
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            (
                '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                '<sheetData><row><c t="s"><v>0</v></c><c t="s"><v>1</v></c></row></sheetData>'
                "</worksheet>"
            ),
        )
    return output.getvalue()


def test_matrix_chat_attachment_preview_payload_handles_text_and_office_formats():
    text_payload = {
        "data": b"# Attachment\n\nReadable text",
        "room_id": "!room:test.example",
        "event_id": "$text:test.example",
        "filename": "note.md",
        "mimetype": "text/markdown",
        "size": 26,
        "msgtype": "m.file",
    }
    docx_payload = {
        **text_payload,
        "data": _minimal_docx("Docx extracted text"),
        "event_id": "$docx:test.example",
        "filename": "note.docx",
        "mimetype": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }
    xlsx_payload = {
        **text_payload,
        "data": _minimal_xlsx(["Header", "Value"]),
        "event_id": "$xlsx:test.example",
        "filename": "sheet.xlsx",
        "mimetype": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    }

    markdown_preview = matrix_chat._attachment_preview_payload(text_payload)
    docx_preview = matrix_chat._attachment_preview_payload(docx_payload)
    xlsx_preview = matrix_chat._attachment_preview_payload(xlsx_payload)

    assert markdown_preview["preview_kind"] == "markdown"
    assert markdown_preview["text"].startswith("# Attachment")
    assert docx_preview["preview_kind"] == "text"
    assert "Docx extracted text" in docx_preview["text"]
    assert xlsx_preview["preview_kind"] == "markdown"
    assert "Header | Value" in xlsx_preview["text"]
    for preview in (markdown_preview, docx_preview, xlsx_preview):
        assert preview["download"] == {"kind": "decrypted_attachment", "available": True}
        assert "key" not in preview
        assert "iv" not in preview
        assert "hash" not in preview


def test_matrix_chat_attachment_download_route_returns_plain_bytes(monkeypatch):
    class FakeE2EEClient:
        async def download_attachment_event(self, room_id, event_id):
            return {
                "data": b"plain image bytes",
                "room_id": room_id,
                "event_id": event_id,
                "content_uri": "mxc://example.org/encrypted123",
                "filename": "proof.png",
                "mimetype": "image/png",
                "size": 17,
                "msgtype": "m.image",
                "encrypted_event": True,
                "encrypted_attachment": True,
            }

    async def fake_get_e2ee_client(*_args, **_kwargs):
        return FakeE2EEClient()

    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)

    response = asyncio.run(
        matrix_chat.matrix_chat_download_attachment("!room:test.example", "$event:test.example")
    )

    assert response.body == b"plain image bytes"
    assert response.media_type == "image/png"
    assert response.headers["x-matrix-media-mxc"] == "mxc://example.org/encrypted123"
    assert response.headers["x-matrix-encrypted-event"] == "true"
    assert response.headers["x-matrix-encrypted-attachment"] == "true"
    assert "key" not in response.headers
    assert "iv" not in response.headers
    assert "hash" not in response.headers


def test_matrix_chat_attachment_preview_route_returns_reduced_plaintext_preview(monkeypatch):
    class FakeE2EEClient:
        async def download_attachment_event(self, room_id, event_id):
            return {
                "data": b"plain preview text",
                "room_id": room_id,
                "event_id": event_id,
                "content_uri": "mxc://example.org/text123",
                "filename": "proof.txt",
                "mimetype": "text/plain",
                "size": 18,
                "msgtype": "m.file",
                "encrypted_event": True,
                "encrypted_attachment": True,
            }

    async def fake_get_e2ee_client(*_args, **_kwargs):
        return FakeE2EEClient()

    monkeypatch.setattr(matrix_chat, "_get_e2ee_client", fake_get_e2ee_client)

    response = asyncio.run(
        matrix_chat.matrix_chat_preview_attachment("!room:test.example", "$event:test.example")
    )

    assert response["preview_kind"] == "text"
    assert response["text"] == "plain preview text"
    assert response["download"]["kind"] == "decrypted_attachment"
    assert response["encrypted_event"] is True
    assert "content_uri" not in response


def test_matrix_chat_audio_filename_and_mimetype_are_normalized():
    assert matrix_chat._safe_media_filename("../../voice?.mp3") == "voice_.mp3"
    assert matrix_chat._safe_media_filename("") == "voice-message.webm"
    assert matrix_chat._guess_audio_mimetype("clip.mp3", "") == "audio/mpeg"
    assert matrix_chat._guess_audio_mimetype("clip.wav", None) == "audio/wav"
    assert matrix_chat._guess_audio_mimetype("clip.bin", "audio/ogg") == "audio/ogg"


def test_matrix_chat_auto_prefixes_local_bridge_without_member_mention():
    body = matrix_chat._auto_hermes_prefix_body_for_state(
        server_id="tb1",
        body="status please",
        events=[
            {"type": "m.room.name", "content": {"name": "Bridge"}},
            {
                "type": "m.room.member",
                "state_key": "@operator:test.example",
                "content": {"membership": "join"},
            },
        ],
    )

    assert body == "hermes: status please"


def test_matrix_chat_auto_prefix_skips_existing_room_member_mention():
    body = matrix_chat._auto_hermes_prefix_body_for_state(
        server_id="tb1",
        body="hello @operator:test.example",
        events=[
            {"type": "m.room.name", "content": {"name": "Bridge"}},
            {
                "type": "m.room.member",
                "state_key": "@operator:test.example",
                "content": {"membership": "join"},
            },
        ],
    )

    assert body == "hello @operator:test.example"


def test_matrix_chat_auto_prefix_skips_non_bridge_rooms():
    body = matrix_chat._auto_hermes_prefix_body_for_state(
        server_id="tb1",
        body="status please",
        events=[{"type": "m.room.name", "content": {"name": "Ops"}}],
    )

    assert body == "status please"


def test_matrix_chat_auto_prefixes_vps_shared_bridge():
    body = matrix_chat._auto_hermes_prefix_body_for_state(
        server_id="vps",
        body="status please",
        events=[{"type": "m.room.name", "content": {"name": "Shared Bridge"}}],
    )

    assert body == "hermes-vps: status please"


def test_matrix_chat_auto_prefix_skips_existing_hermes_alias():
    body = matrix_chat._auto_hermes_prefix_body_for_state(
        server_id="tb1",
        body="h: status please",
        events=[{"type": "m.room.name", "content": {"name": "Bridge"}}],
    )

    assert body == "h: status please"


def test_matrix_chat_room_mention_candidates_from_state_excludes_self():
    users = matrix_chat._room_mention_candidates_from_state(
        [
            {
                "type": "m.room.member",
                "state_key": "@codex:test.example",
                "content": {"membership": "join", "displayname": "AI-Admin"},
            },
            {
                "type": "m.room.member",
                "state_key": "@hermes:test.example",
                "content": {"membership": "join", "displayname": "Hermes-TB1"},
            },
            {
                "type": "m.room.member",
                "state_key": "@operator:test.example",
                "content": {"membership": "leave", "displayname": "Davros"},
            },
        ],
        current_user_id="@codex:test.example",
        query="herm",
    )

    assert users == [{"user_id": "@hermes:test.example", "display_name": "Hermes-TB1"}]


def test_matrix_chat_hermes_command_catalog_reduces_subprocess_output(monkeypatch):
    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "commands": [
                    {
                        "name": "/help",
                        "insert": "/help",
                        "description": "Show help",
                        "category": "Info",
                        "source": "core",
                        "aliases": ["/h"],
                        "requires_argument": False,
                    },
                    {
                        "name": "not-a-command",
                        "description": "drop me",
                    },
                ]
            }
        )
        stderr = ""

    captured = {}

    def fake_run(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return Result()

    monkeypatch.setattr(matrix_chat.subprocess, "run", fake_run)

    catalogue = matrix_chat._load_hermes_command_catalog(
        {
            "hermes_command_container": "hermes-local",
            "hermes_command_python": "/opt/hermes/.venv/bin/python",
        }
    )

    assert captured["args"][:4] == [
        "docker",
        "exec",
        "hermes-local",
        "/opt/hermes/.venv/bin/python",
    ]
    assert captured["kwargs"]["timeout"] == matrix_chat._HERMES_COMMAND_CATALOG_TIMEOUT
    assert catalogue == {
        "source": "hermes",
        "commands": [
            {
                "name": "/help",
                "insert": "/help",
                "description": "Show help",
                "category": "Info",
                "source": "core",
                "args_hint": "",
                "aliases": ["/h"],
                "requires_argument": False,
            }
        ],
        "total": 1,
    }


def test_matrix_chat_hermes_command_catalog_can_probe_over_ssh(monkeypatch):
    class Result:
        returncode = 0
        stdout = json.dumps(
            {
                "commands": [
                    {
                        "name": "/help",
                        "insert": "/help",
                        "description": "Show help",
                        "category": "Info",
                        "source": "core",
                    }
                ]
            }
        )
        stderr = ""

    captured = {}

    def fake_run(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return Result()

    monkeypatch.setattr(matrix_chat.subprocess, "run", fake_run)

    catalogue = matrix_chat._load_hermes_command_catalog(
        {
            "hermes_command_container": "hermes",
            "hermes_command_python": "/opt/hermes/.venv/bin/python",
            "hermes_command_ssh_host": "203.0.113.10",
            "hermes_command_ssh_user": "root",
            "hermes_command_ssh_key": "/tmp/xarta-test-ssh-key",
        }
    )

    assert captured["args"][:7] == [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-i",
        "/tmp/xarta-test-ssh-key",
    ]
    assert captured["args"][7] == "root@203.0.113.10"
    assert captured["args"][8].startswith("docker exec hermes /opt/hermes/.venv/bin/python -c ")
    assert "from hermes_cli.commands import" in captured["args"][8]
    assert "COMMAND_REGISTRY" in captured["args"][8]
    assert captured["kwargs"]["timeout"] == matrix_chat._HERMES_COMMAND_CATALOG_TIMEOUT
    assert catalogue["commands"][0]["name"] == "/help"
    assert catalogue["source"] == "hermes"


def test_matrix_chat_room_settings_default_off_and_persist(tmp_path):
    settings = {
        "server_id": "vps",
        "room_settings_file": str(tmp_path / "room-settings.json"),
        "admin_access_token": "admin-token-secret",
    }

    assert matrix_chat._room_settings_payload(settings, "!shared:test.example") == {
        "server_id": "vps",
        "room_id": "!shared:test.example",
        "hermes_command_catalog": False,
        "hide_system_messages": False,
        "system_message_min_level": "information",
        "admin_available": True,
    }

    updated = matrix_chat._set_room_settings(
        settings,
        "!shared:test.example",
        matrix_chat._RoomSettingsBody(hermes_command_catalog=True),
    )

    assert updated["hermes_command_catalog"] is True
    assert (
        matrix_chat._room_settings_payload(
            settings,
            "!shared:test.example",
        )["hermes_command_catalog"]
        is True
    )
    assert "admin-token-secret" not in (tmp_path / "room-settings.json").read_text(encoding="utf-8")


def test_matrix_chat_room_settings_update_requires_admin_token(tmp_path):
    settings = {
        "server_id": "tb1",
        "room_settings_file": str(tmp_path / "room-settings.json"),
        "admin_access_token": "",
    }

    with pytest.raises(matrix_chat.HTTPException) as exc:
        matrix_chat._set_room_settings(
            settings,
            "!bridge:test.example",
            matrix_chat._RoomSettingsBody(hermes_command_catalog=True),
        )

    assert exc.value.status_code == 503
    assert not (tmp_path / "room-settings.json").exists()


@pytest.mark.asyncio
async def test_matrix_chat_hermes_commands_refuses_disabled_room_before_probe(
    tmp_path, monkeypatch
):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=chat-token",
                f"MATRIX_CHAT_ROOM_SETTINGS_FILE={tmp_path / 'room-settings.json'}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))

    def fail_probe(_settings):
        raise AssertionError("Hermes catalogue probe should not run")

    monkeypatch.setattr(matrix_chat, "_load_hermes_command_catalog", fail_probe)

    with pytest.raises(matrix_chat.HTTPException) as exc:
        await matrix_chat.matrix_chat_hermes_commands(room_id="!plain:test.example")

    assert exc.value.status_code == 403
    assert "disabled" in exc.value.detail.lower()


@pytest.mark.asyncio
async def test_matrix_chat_hermes_commands_allows_enabled_room(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    settings_file = tmp_path / "room-settings.json"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=chat-token",
                f"MATRIX_CHAT_ROOM_SETTINGS_FILE={settings_file}",
            ]
        ),
        encoding="utf-8",
    )
    settings_file.write_text(
        json.dumps(
            {
                "servers": {
                    "tb1": {"rooms": {"!bridge:test.example": {"hermes_command_catalog": True}}}
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))

    def fake_probe(_settings):
        return {
            "source": "hermes",
            "total": 1,
            "commands": [
                {
                    "name": "/help",
                    "insert": "/help",
                    "description": "Show help",
                    "category": "Info",
                    "source": "core",
                    "args_hint": "",
                    "aliases": [],
                    "requires_argument": False,
                }
            ],
        }

    monkeypatch.setattr(matrix_chat, "_load_hermes_command_catalog", fake_probe)

    response = await matrix_chat.matrix_chat_hermes_commands(
        q="/he",
        room_id="!bridge:test.example",
    )

    assert response["commands"][0]["name"] == "/help"


def test_matrix_chat_room_and_message_mapping_do_not_return_credentials():
    sync = {
        "next_batch": "s123",
        "rooms": {
            "join": {
                "!room:test.example": {
                    "summary": {"m.joined_member_count": 2},
                    "state": {
                        "events": [
                            {
                                "type": "m.room.name",
                                "content": {"name": "Hermes Local Smoke"},
                            }
                        ]
                    },
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "event_id": "$event1",
                                "sender": "@hermes:test.example",
                                "origin_server_ts": 1710000000000,
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "brief reply",
                                },
                            }
                        ]
                    },
                }
            },
            "invite": {},
        },
    }

    joined, invited = matrix_chat._rooms_from_sync(sync)
    message = matrix_chat._message_from_event(
        sync["rooms"]["join"]["!room:test.example"]["timeline"]["events"][0],
        "!room:test.example",
    )
    rendered = repr({"joined": joined, "invited": invited, "message": message})

    assert joined[0]["name"] == "Hermes Local Smoke"
    assert joined[0]["display_name"] == "Hermes Local Smoke"
    assert joined[0]["name_source"] == "m.room.name"
    assert joined[0]["last_preview"] == "brief reply"
    assert invited == []
    assert message["body"] == "brief reply"
    assert "access_token" not in rendered.lower()
    assert "password" not in rendered.lower()
    assert "authorization" not in rendered.lower()


def test_matrix_chat_drops_redacted_target_before_message_mapping():
    events = [
        {
            "type": "m.room.encrypted",
            "event_id": "$bad",
            "sender": "@old:test.example",
            "origin_server_ts": 1710000000000,
            "content": {"algorithm": "m.megolm.v1.aes-sha2"},
        },
        {
            "type": "m.room.redaction",
            "event_id": "$redaction",
            "sender": "@admin:test.example",
            "origin_server_ts": 1710000000001,
            "redacts": "$bad",
            "content": {"reason": "cleanup"},
        },
        {
            "type": "m.room.message",
            "event_id": "$good",
            "sender": "@hermes:test.example",
            "origin_server_ts": 1710000000002,
            "content": {"msgtype": "m.text", "body": "still here"},
        },
    ]

    filtered = matrix_chat._events_without_redacted_targets(events)
    messages = [matrix_chat._message_from_event(event, "!room:test.example") for event in filtered]
    messages = [message for message in messages if message]

    assert [event["event_id"] for event in filtered] == ["$redaction", "$good"]
    assert [message["event_id"] for message in messages] == ["$good"]


def test_matrix_chat_strips_redacted_targets_from_sync_before_crypto_handling():
    sync = {
        "rooms": {
            "join": {
                "!room:test.example": {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.encrypted",
                                "event_id": "$bad",
                                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                            },
                            {
                                "type": "m.room.redaction",
                                "event_id": "$redaction",
                                "redacts": "$bad",
                                "content": {"reason": "cleanup"},
                            },
                        ]
                    }
                }
            }
        }
    }

    stripped = matrix_chat._sync_without_redacted_targets(sync)
    events = stripped["rooms"]["join"]["!room:test.example"]["timeline"]["events"]

    assert [event["event_id"] for event in events] == ["$redaction"]


def test_matrix_chat_room_mapping_marks_missing_names_as_fallback():
    sync = {
        "rooms": {
            "join": {
                "!roomwithnoname:test.example": {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "event_id": "$event2",
                                "sender": "@hermes:test.example",
                                "origin_server_ts": 1710000000001,
                                "content": {"msgtype": "m.text", "body": "hello"},
                            }
                        ]
                    }
                }
            }
        }
    }

    joined, _ = matrix_chat._rooms_from_sync(sync)

    assert joined[0]["name"] == "!roomwithnoname:test.example"
    assert joined[0]["display_name"].startswith("Unnamed room (")
    assert joined[0]["name_source"] == "fallback_room_id"


def test_matrix_chat_room_mapping_infers_encryption_from_encrypted_timeline_event():
    sync = {
        "rooms": {
            "join": {
                "!bridge:test.example": {
                    "state": {"events": []},
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.encrypted",
                                "event_id": "$encrypted",
                                "sender": "@hermes:test.example",
                                "origin_server_ts": 1710000000002,
                                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                            }
                        ]
                    },
                }
            }
        }
    }

    joined, _ = matrix_chat._rooms_from_sync(sync)

    assert joined[0]["encrypted"] is True
    assert joined[0]["last_preview"] == "[encrypted event]"


def test_matrix_chat_invite_candidate_filter_excludes_members_self_and_admin():
    candidates = [
        {
            "user_id": "@codex:test.example",
            "display_name": "codex",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@hermes:test.example",
            "display_name": "hermes",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@operator:test.example",
            "display_name": "operator",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@admin:test.example",
            "display_name": "admin",
            "is_admin": True,
            "deactivated": False,
        },
        {
            "user_id": "@old:test.example",
            "display_name": "old",
            "is_admin": False,
            "deactivated": True,
        },
    ]

    filtered = matrix_chat._filter_invite_candidates(
        candidates,
        excluded_user_ids={"@hermes:test.example"},
        current_user_id="@codex:test.example",
        query="@",
    )

    assert filtered == [{"user_id": "@operator:test.example", "display_name": "operator"}]


def test_matrix_chat_invite_candidate_filter_applies_query():
    candidates = [
        {
            "user_id": "@xarta-operator:test.example",
            "display_name": "xarta-operator",
            "is_admin": False,
            "deactivated": False,
        },
        {
            "user_id": "@hermes:test.example",
            "display_name": "hermes",
            "is_admin": False,
            "deactivated": False,
        },
    ]

    filtered = matrix_chat._filter_invite_candidates(
        candidates,
        excluded_user_ids=set(),
        current_user_id="@codex:test.example",
        query="oper",
    )

    assert filtered == [
        {
            "user_id": "@xarta-operator:test.example",
            "display_name": "xarta-operator",
        }
    ]


def test_matrix_chat_admin_status_does_not_expose_token():
    settings = {
        "public_homeserver": "https://chat.test.example",
        "admin_user_id": "@synapse-admin:test.example",
        "admin_access_token": "admin-token-secret",
    }

    status = matrix_chat._admin_status_payload(settings, reachable=True, health="ok")
    rendered = repr(status)

    assert status["configured"] is True
    assert status["admin_configured"] is True
    assert status["admin_user_id"] == "@synapse-admin:test.example"
    assert status["features"] == {
        "generic_admin_proxy": False,
        "destructive_actions": False,
        "room_settings": True,
    }
    assert "admin-token-secret" not in rendered
    assert "admin_access_token" not in rendered
    assert "access_token" not in rendered


def test_matrix_chat_admin_user_dto_drops_secret_material():
    user = matrix_chat._normalize_admin_user(
        {
            "name": "@operator:test.example",
            "displayname": "operator",
            "admin": False,
            "deactivated": False,
            "is_guest": False,
            "creation_ts": 1770000000000,
            "access_token": "token-secret",
            "password": "password-secret",
            "pusher": "pusher-secret",
            "topic": "topic-secret",
            "recovery_key": "recovery-secret",
        }
    )
    rendered = repr(user)

    assert user == {
        "user_id": "@operator:test.example",
        "display_name": "operator",
        "is_admin": False,
        "deactivated": False,
        "is_guest": False,
        "creation_ts": 1770000000000,
    }
    for forbidden in (
        "token-secret",
        "password-secret",
        "pusher-secret",
        "topic-secret",
        "recovery-secret",
        "access_token",
        "password",
        "pusher",
        "topic",
        "recovery",
    ):
        assert forbidden not in rendered


def test_matrix_chat_admin_room_dto_handles_missing_name_and_drops_topic():
    room = matrix_chat._normalize_admin_room(
        {
            "room_id": "!room:test.example",
            "joined_members": "3",
            "joined_local_members": 2,
            "version": 10,
            "federatable": "false",
            "public": None,
            "topic": "secret-topic",
        }
    )
    rendered = repr(room)

    assert room == {
        "room_id": "!room:test.example",
        "name": "",
        "canonical_alias": "",
        "joined_members": 3,
        "joined_local_members": 2,
        "version": "10",
        "encrypted": False,
        "public": False,
        "federatable": False,
    }
    assert "secret-topic" not in rendered
    assert "topic" not in rendered


@pytest.mark.asyncio
async def test_matrix_chat_admin_endpoints_fail_when_admin_token_missing(tmp_path, monkeypatch):
    env_file = tmp_path / "matrix.env"
    env_file.write_text(
        "\n".join(
            [
                "MATRIX_CODEX_USER_ID=@codex:test.example",
                "MATRIX_CODEX_ACCESS_TOKEN=chat-token",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", str(env_file))
    monkeypatch.delenv("MATRIX_CHAT_ADMIN_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MATRIX_ADMIN_ACCESS_TOKEN", raising=False)

    with pytest.raises(matrix_chat.HTTPException) as exc:
        await matrix_chat.matrix_chat_admin_users()

    assert exc.value.status_code == 503
    assert "admin token" in exc.value.detail.lower()


def test_matrix_chat_admin_member_reduction_includes_power_without_raw_state():
    state_rows = matrix_chat._room_member_rows_from_state(
        [
            {
                "type": "m.room.power_levels",
                "content": {"users": {"@admin:test.example": 100}},
            },
            {
                "type": "m.room.member",
                "state_key": "@admin:test.example",
                "content": {"membership": "join", "displayname": "Synapse Admin"},
            },
        ]
    )
    member = matrix_chat._normalize_admin_member("@admin:test.example", state_rows)

    assert member == {
        "user_id": "@admin:test.example",
        "membership": "join",
        "display_name": "Synapse Admin",
        "power_level": 100,
    }
    assert "content" not in repr(member)
    assert "state_key" not in repr(member)


def test_matrix_chat_admin_member_rows_include_invited_state_members():
    state_rows = matrix_chat._room_member_rows_from_state(
        [
            {
                "type": "m.room.power_levels",
                "content": {"users": {"@owner:test.example": 100, "@admin:test.example": 50}},
            },
            {
                "type": "m.room.member",
                "state_key": "@owner:test.example",
                "content": {"membership": "join", "displayname": "Owner"},
            },
            {
                "type": "m.room.member",
                "state_key": "@invitee:test.example",
                "content": {"membership": "invite", "displayname": "Invited User"},
            },
        ]
    )

    joined = matrix_chat._normalize_admin_member("@owner:test.example", state_rows)
    invited = state_rows["@invitee:test.example"]

    assert joined == {
        "user_id": "@owner:test.example",
        "membership": "join",
        "display_name": "Owner",
        "power_level": 100,
    }
    assert invited == {
        "user_id": "@invitee:test.example",
        "membership": "invite",
        "display_name": "Invited User",
        "power_level": None,
    }


@pytest.mark.asyncio
async def test_matrix_chat_create_room_can_request_encryption(monkeypatch):
    captured = {}

    async def fake_matrix_request(method, path, *, json_body=None, **_kwargs):
        captured["method"] = method
        captured["path"] = path
        captured["json_body"] = json_body
        return {"room_id": "!encrypted:test.example"}

    monkeypatch.setattr(matrix_chat, "_matrix_request", fake_matrix_request)

    result = await matrix_chat.matrix_chat_create_room(
        matrix_chat._CreateRoomBody(name="Encrypted Ops", encrypted=True)
    )

    assert result == {"room_id": "!encrypted:test.example"}
    assert captured["method"] == "POST"
    assert captured["path"] == "/createRoom"
    assert captured["json_body"]["visibility"] == "private"
    assert captured["json_body"]["initial_state"] == [
        {
            "type": "m.room.encryption",
            "state_key": "",
            "content": {"algorithm": "m.megolm.v1.aes-sha2"},
        }
    ]


@pytest.mark.asyncio
async def test_matrix_chat_e2ee_messages_treat_missing_end_as_start_of_history(monkeypatch):
    captured = {}

    class FakeAPI:
        async def request(self, method, path, *, query_params=None, metrics_method=None):
            captured["query_params"] = query_params
            captured["metrics_method"] = metrics_method
            return {"chunk": [], "start": "t1-start"}

    class FakeClient:
        api = FakeAPI()

    client = matrix_chat._MatrixChatE2EEClient(
        {
            "crypto_store_dir": "/tmp/unused",
            "upstream": "https://matrix.test",
            "user_id": "@codex:test",
            "access_token": "token",
        }
    )
    client._started = True
    client._client = FakeClient()

    async def fake_messages_from_raw_events(room_id, events):
        return []

    monkeypatch.setattr(client, "messages_from_raw_events", fake_messages_from_raw_events)

    result = await client.messages("!room:test", limit=60, from_token="t1-start")

    assert captured["query_params"] == {"dir": "b", "from": "t1-start", "limit": "60"}
    assert captured["metrics_method"] == "getMessages"
    assert result == {
        "room_id": "!room:test",
        "messages": [],
        "start": "t1-start",
        "end": None,
        "at_start": True,
    }


class _FakeDecryptedContent:
    body = "decrypted text"
    msgtype = "m.text"

    def serialize(self):
        return {"msgtype": self.msgtype, "body": self.body}


class _FakeDecryptedEvent:
    def __init__(self, event_id="$decrypted", sender="@alice:test", timestamp=1710000000000):
        from mautrix.types import EventType

        self.type = EventType.ROOM_MESSAGE
        self.event_id = event_id
        self.sender = sender
        self.timestamp = timestamp
        self.content = _FakeDecryptedContent()


def _encrypted_raw_event(event_id: str) -> dict[str, object]:
    return {
        "type": "m.room.encrypted",
        "event_id": event_id,
        "room_id": "!room:test",
        "sender": "@alice:test",
        "origin_server_ts": 1710000000000,
        "content": {
            "algorithm": "m.megolm.v1.aes-sha2",
            "ciphertext": "abc",
            "device_id": "DEVICE",
            "sender_key": "key",
            "session_id": "session",
        },
    }


def test_matrix_chat_e2ee_messages_explicit_timeout_returns_undecrypted_placeholder():
    class SlowCrypto:
        async def decrypt_megolm_event(self, _event):
            await asyncio.sleep(1)

    class FakeClient:
        crypto = SlowCrypto()

    client = matrix_chat._MatrixChatE2EEClient(
        {
            "crypto_store_dir": "/tmp/unused",
            "upstream": "https://matrix.test",
            "user_id": "@codex:test",
            "access_token": "token",
        }
    )
    client._started = True
    client._client = FakeClient()

    messages = asyncio.run(
        client.messages_from_raw_events(
            "!room:test",
            [_encrypted_raw_event("$slow")],
            total_timeout_seconds=0.05,
        )
    )

    assert messages == [
        {
            "event_id": "$slow",
            "room_id": "!room:test",
            "sender": "@alice:test",
            "origin_server_ts": 1710000000000,
            "msgtype": "m.encrypted",
            "body": "[unable to decrypt encrypted event]",
            "relates_to": None,
            "system_message": None,
            "media": None,
            "encrypted": True,
            "decrypted": False,
        }
    ]


def test_matrix_chat_e2ee_messages_waits_for_decryptable_events_by_default():
    class SlowCrypto:
        async def decrypt_megolm_event(self, event):
            await asyncio.sleep(0.02)
            return _FakeDecryptedEvent(event_id=str(event.event_id))

    class FakeClient:
        crypto = SlowCrypto()

    client = matrix_chat._MatrixChatE2EEClient(
        {
            "crypto_store_dir": "/tmp/unused",
            "upstream": "https://matrix.test",
            "user_id": "@codex:test",
            "access_token": "token",
        }
    )
    client._started = True
    client._client = FakeClient()

    messages = asyncio.run(
        client.messages_from_raw_events("!room:test", [_encrypted_raw_event("$ok")])
    )

    assert messages[0]["event_id"] == "$ok"
    assert messages[0]["body"] == "decrypted text"
    assert messages[0]["encrypted"] is True
    assert messages[0]["decrypted"] is True


def test_matrix_chat_e2ee_messages_serializes_crypto_store_access():
    active = 0
    max_active = 0

    class SlowCrypto:
        async def decrypt_megolm_event(self, event):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.02)
            active -= 1
            return _FakeDecryptedEvent(event_id=str(event.event_id))

    class FakeClient:
        crypto = SlowCrypto()

    client = matrix_chat._MatrixChatE2EEClient(
        {
            "crypto_store_dir": "/tmp/unused",
            "upstream": "https://matrix.test",
            "user_id": "@codex:test",
            "access_token": "token",
        }
    )
    client._started = True
    client._client = FakeClient()

    async def run_concurrently():
        return await asyncio.gather(
            client.messages_from_raw_events("!room:test", [_encrypted_raw_event("$one")]),
            client.messages_from_raw_events("!room:test", [_encrypted_raw_event("$two")]),
        )

    first, second = asyncio.run(run_concurrently())

    assert max_active == 1
    assert first[0]["body"] == "decrypted text"
    assert second[0]["body"] == "decrypted text"
