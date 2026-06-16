from pathlib import Path


def test_browser_announcer_honors_suppress_speech_payload():
    announcer = Path("/xarta-node/gui-fallback/js/model-change-announcer.js").read_text(
        encoding="utf-8"
    )

    assert "function _isSpeechSuppressedEvent" in announcer
    assert "payload.suppress_speech === true" in announcer
    assert "payload.xarta_suppress_speech === true" in announcer
    assert "_emitSpeechSuppressed('payload_suppress_speech'" in announcer


def test_browser_replay_ignores_speech_suppressed_model_change_events():
    announcer = Path("/xarta-node/gui-fallback/js/model-change-announcer.js").read_text(
        encoding="utf-8"
    )

    replay_filter = announcer[announcer.index("const missed = events") :]
    assert "!_isSpeechSuppressedEvent(e)" in replay_filter


def test_browser_suppresses_only_repeated_full_model_change_signature():
    announcer = Path("/xarta-node/gui-fallback/js/model-change-announcer.js").read_text(
        encoding="utf-8"
    )

    assert "_FULL_MODEL_SIGNATURE_KEY" in announcer
    assert "items.every(_isAliasReconcileEvent)" in announcer
    assert "duplicate_model_change_signature" in announcer
    assert "_recordFullModelSignature(fullSignature)" in announcer


def test_browser_skips_stale_full_model_change_sse_replay_without_blocking_alias_reconcile():
    announcer = Path("/xarta-node/gui-fallback/js/model-change-announcer.js").read_text(
        encoding="utf-8"
    )

    assert "_MODEL_CHANGE_REPLAY_FRESHNESS_SECONDS = _REPLAY_LOOKBACK" in announcer
    assert "function _isStaleModelChangeReplay" in announcer
    assert "stale_model_change_replay" in announcer
    assert "!_isAliasReconcileEvent(evt) && _isStaleModelChangeReplay(evt)" in announcer
