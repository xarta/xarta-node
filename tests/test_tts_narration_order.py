from pathlib import Path

WORKSPACE_ROOT = Path("/xarta-node")


def _assert_pre_sanitized_speak_call(source: str, function_name: str) -> None:
    start = source.find(f"async function {function_name}")
    assert start != -1, f"{function_name} not found"
    speak_start = source.find("BlueprintsTtsClient.speak({", start)
    assert speak_start != -1, f"{function_name} does not call BlueprintsTtsClient.speak"
    speak_end = source.find("});", speak_start)
    assert speak_end != -1, f"{function_name} speak call is incomplete"
    call = source[speak_start:speak_end]
    assert "sanitizeText: false" in call
    assert "transformProfile: 'none'" in call
    assert "sanitizeText: true" not in call
    assert "transformProfile: 'speech'" not in call


def test_cached_docs_narration_is_not_sanitized_again_before_tts():
    source = (WORKSPACE_ROOT / "gui-fallback/js/settings/docs.js").read_text(encoding="utf-8")

    _assert_pre_sanitized_speak_call(source, "_docsDocSpeechStart")


def test_cached_web_research_narration_is_not_sanitized_again_before_tts():
    source = (WORKSPACE_ROOT / "gui-fallback/js/probes/web-research.js").read_text(encoding="utf-8")

    _assert_pre_sanitized_speak_call(source, "_webResearchPrivacySpeechStart")
    _assert_pre_sanitized_speak_call(source, "_webResearchSpeechStart")


def test_cached_local_dockge_narration_is_not_sanitized_again_before_tts():
    source = (WORKSPACE_ROOT / "gui-fallback/js/settings/local-dockge.js").read_text(encoding="utf-8")

    _assert_pre_sanitized_speak_call(source, "_localDockgeNarrationStart")


def test_cached_vps_dockge_narration_is_not_sanitized_again_before_tts():
    source = (WORKSPACE_ROOT / "gui-fallback/js/settings/vps-dockge.js").read_text(encoding="utf-8")

    _assert_pre_sanitized_speak_call(source, "_vpsDockgeNarrationStart")


def test_blueprints_wrapper_suppresses_upstream_pockettts_sanitizer():
    source = Path("/root/xarta-node/blueprints-app/app/routes_tts.py").read_text(encoding="utf-8")
    model_field = source.find('"model": model_name')
    assert model_field != -1
    payload_start = source.rfind("payload = {", 0, model_field)
    assert payload_start != -1
    payload_end = source.find("}", model_field)
    assert payload_end != -1
    payload = source[payload_start:payload_end]
    assert '"sanitize_text": False' in payload
    assert '"transform_profile": "none"' in payload


def test_blueprints_does_not_carry_a_second_tts_sanitizer_implementation():
    assert not Path("/root/xarta-node/blueprints-app/app/tts_sanitizer.py").exists()


def test_web_research_speech_cache_write_does_not_resanitize():
    source = Path("/root/xarta-node/blueprints-app/app/routes_web_research.py").read_text(encoding="utf-8")
    start = source.find("def _write_speech_cache")
    assert start != -1
    end = source.find("def ", start + 1)
    assert end != -1
    function_body = source[start:end]
    assert "sanitize_tts_text(" not in function_body
