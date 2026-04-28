from app.tts_sanitizer import prepare_tts_markdown_for_llm, sanitize_tts_text


def test_sanitize_tts_text_projects_markdown_headings_and_source_refs():
    raw = """**Progress So Far**
As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories [S1]. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage [S1]. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance [S2], [S4].

**Current Challenges**
Despite the progress, there are a few areas."""

    result = sanitize_tts_text(raw)

    assert result.text == """Progress So Far.

As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non root repositories. The backend now proxies search requests through the Blueprints ay pee eye, and the frontend supports multiple search modes with persistent state in local storage. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance.

Current Challenges.

Despite the progress, there are a few areas."""
    assert list(result.transforms) == [
        "normalize_newlines",
        "strip_top_backlink_line",
        "strip_source_refs",
        "project_markdown_headings",
        "strip_inline_code_ticks",
        "strip_inline_markdown_emphasis",
        "speak_known_attribute_names",
        "speak_tts_identifiers",
        "speak_tts_acronyms",
        "normalize_spacing",
    ]


def test_sanitize_tts_text_handles_hash_headings_and_inline_emphasis():
    result = sanitize_tts_text("# Background\nThe **xarta-node** docs are _indexed_ [S12].")

    assert result.text == "Background.\n\nThe xarta node docs are indexed."


def test_sanitize_tts_text_speaks_data_fc_key_attribute():
    result = sanitize_tts_text(
        "Blueprints GUI uses a `data-fc-key` HTML attribute, `data-fc-event`, and stray `ticks."
    )

    assert (
        result.text
        == "Blueprints gooey uses a data eff sea key aitch tee em ell attribute, data eff sea event, and stray ticks."
    )
    assert "`" not in result.text


def test_sanitize_tts_text_speaks_snake_case_and_kebab_case_identifiers():
    result = sanitize_tts_text(
        "The `form_controls` table maps table_layout_catalog rows for SOUND-MANAGER."
    )

    assert result.text == "The form controls table maps table layout catalog rows for SOUND MANAGER."


def test_prepare_tts_markdown_for_llm_projects_inline_code_identifiers():
    raw = "Use `form_controls`, `data-fc-key`, and table_layouts in the narration source."

    assert (
        prepare_tts_markdown_for_llm(raw)
        == "Use form controls, data eff sea key, and table layouts in the narration source."
    )


def test_sanitize_tts_text_speaks_common_technical_acronyms():
    raw = "LED SVG png jpg VM LXC805 AI API GUI DNS HTTPS mTLS IPv6 UUID SQLite pfSense CI/CD"

    assert (
        sanitize_tts_text(raw).text
        == (
            "ell ee dee ess vee gee pee enn gee jay peg vee em ell ex sea 805 "
            "ay eye ay pee eye gooey dee enn ess aitch tee tee pee ess em tee ell ess "
            "eye pee vee six you you eye dee sequel lite pee eff sense see eye, see dee"
        )
    )


def test_sanitize_tts_text_removes_only_top_backlink():
    raw = """[<- web-design README](README.md)

# FORM-CONTROLS

← [README](README.md)

The page body remains."""

    result = sanitize_tts_text(raw)

    assert result.text == "FORM CONTROLS.\n\n← [README](README.md)\n\nThe page body remains."
