from app.tts_sanitizer import sanitize_tts_text


def test_sanitize_tts_text_projects_markdown_headings_and_source_refs():
    raw = """**Progress So Far**
As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories [S1]. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage [S1]. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance [S2], [S4].

**Current Challenges**
Despite the progress, there are a few areas."""

    result = sanitize_tts_text(raw)

    assert result.text == """Progress So Far.

As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance.

Current Challenges.

Despite the progress, there are a few areas."""
    assert list(result.transforms) == [
        "normalize_newlines",
        "strip_source_refs",
        "project_markdown_headings",
        "strip_inline_markdown_emphasis",
        "normalize_spacing",
    ]


def test_sanitize_tts_text_handles_hash_headings_and_inline_emphasis():
    result = sanitize_tts_text("# Background\nThe **xarta-node** docs are _indexed_ [S12].")

    assert result.text == "Background.\n\nThe xarta-node docs are indexed."
