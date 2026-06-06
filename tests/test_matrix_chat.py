import asyncio
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_matrix_chat as matrix_chat


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
    monkeypatch.delenv("BLUEPRINTS_WAKE_STT_DIRECT_PRE_ROLL_AFTER_MS", raising=False)

    assert matrix_chat._wake_stt_direct_pre_roll_delay_seconds() == 3.0


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
    assert json.loads(active_session.read_text(encoding="utf-8"))["session_id"] == (
        "wake-stt-local-operator-heavy"
    )


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
        "Command Code required. Please say the Command Code now."
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
    assert delivery["direct"]["companion"]["speech"] == (
        "Command Code required. Please say the Command Code now."
    )
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
    assert new_request_delivery["status"] == "command_code_aborted"
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
    ]
    assert matrix_chat._WAKE_STT_PENDING_COMMAND_CODE_REQUESTS == {}


def test_matrix_chat_wake_stt_intervening_turn_clears_pending_across_instances(
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
    assert intervening["delivery"]["status"] == "command_code_aborted"
    assert later_code["delivery"]["status"] == "command_code_stale"
    assert calls == [{"text": "create file Dave 10", "trusted": False}]
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


def test_matrix_chat_wake_stt_direct_route_pre_rolls_then_speaks_final(monkeypatch):
    published: list[dict[str, object]] = []

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
