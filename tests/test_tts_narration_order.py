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
