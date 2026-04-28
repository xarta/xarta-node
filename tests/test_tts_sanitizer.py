from app.tts_sanitizer import prepare_tts_markdown_for_llm, sanitize_tts_text


def test_sanitize_tts_text_projects_markdown_headings_and_source_refs():
    raw = """**Progress So Far**
As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories [S1]. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage [S1]. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance [S2], [S4].

**Current Challenges**
Despite the progress, there are a few areas."""

    result = sanitize_tts_text(raw)

    assert result.text == """Progress So Far.

As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non root repositories. The backend now proxies search requests through the Blueprints A pee eye, and the frontend supports multiple search modes with persistent state in local storage. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance.

Current Challenges.

Despite the progress, there are a few areas."""
    assert list(result.transforms) == [
        "normalize_newlines",
        "strip_top_backlink_line",
        "strip_source_refs",
        "project_markdown_headings",
        "summarize_fenced_code_blocks",
        "summarize_markdown_tables",
        "summarize_endpoint_list_blocks",
        "strip_inline_code_ticks",
        "strip_inline_markdown_emphasis",
        "strip_markdown_list_markers",
        "speak_known_attribute_names",
        "speak_tts_known_terms",
        "speak_tts_file_extensions",
        "speak_tts_identifiers",
        "speak_tts_acronyms",
        "speak_remaining_pipes",
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
        == "Blueprints gooey uses a data eff sea key H tee em ell attribute, data eff sea event, and stray ticks."
    )
    assert "`" not in result.text


def test_sanitize_tts_text_speaks_snake_case_and_kebab_case_identifiers():
    result = sanitize_tts_text("The `form_controls` table maps table_layout_catalog rows for NAV-ITEMS.")

    assert result.text == "The form controls table maps table layout catalog rows for NAV ITEMS."


def test_prepare_tts_markdown_for_llm_projects_inline_code_identifiers():
    raw = "Use `form_controls`, `data-fc-key`, and table_layouts in the narration source."

    assert (
        prepare_tts_markdown_for_llm(raw)
        == "Use form controls, data eff sea key, and table layouts in the narration source."
    )


def test_prepare_tts_markdown_for_llm_preserves_fenced_code_blocks():
    raw = """Use `form_controls` and this example:

```html
<input type="text" data-fc-key="bookmarks.filter.search" />
```

Then mention SVG."""

    assert prepare_tts_markdown_for_llm(raw) == """Use form controls and this example:

```html
<input type="text" data-fc-key="bookmarks.filter.search" />
```

Then mention ess vee gee."""


def test_sanitize_tts_text_summarizes_fenced_code_blocks():
    raw = """Example:

```html
<input type="text" data-fc-key="bookmarks.filter.search" />
```

Done."""

    assert sanitize_tts_text(raw).text == "Example:\n\nThere is an H tee em ell example here.\n\nDone."


def test_sanitize_tts_text_speaks_common_technical_acronyms():
    raw = "LED SVG png jpg VM LXC805 AI API GUI DNS HTTPS mTLS IPv6 UUID SQLite pfSense CI/CD js html"

    assert (
        sanitize_tts_text(raw).text
        == (
            "ell ee dee ess vee gee pee enn gee jay peg vee em ell ex sea 805 "
            "ay eye A pee eye gooey dee enn ess aitch tee tee pee ess em tee ell ess "
            "eye pee vee six you you eye dee sequel lite pee eff sense see eye, see dee "
            "JavaScript H tee em ell"
        )
    )


def test_sanitize_tts_text_speaks_file_extensions_differently_from_acronyms():
    raw = "Open `form-controls.js`, icons.svg, page.HTML, config.env, table_layout_catalog.json, and .svg."

    assert (
        sanitize_tts_text(raw).text
        == (
            "Open form controls dot jay ess, icons dot ess vee gee, page dot H tee em ell, "
            "config dot ee enn vee, table layout catalog dot jay son, and dot ess vee gee."
        )
    )


def test_sanitize_tts_text_summarizes_markdown_tables_without_pipes():
    raw = """## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | /api/v1/form-controls | List all |
| POST | /api/v1/form-controls | Create |
| DELETE | /api/v1/form-controls/{id} | Delete |

Next."""

    result = sanitize_tts_text(raw).text

    assert result == (
        "A pee eye Endpoints.\n\n"
        "There is a table with 3 rows covering Method, Path, and Description.\n\n"
        "Next."
    )
    assert "|" not in result


def test_sanitize_tts_text_speaks_textarea_iife_and_dom_terms():
    result = sanitize_tts_text("textarea textareas IIFE DOM click|change|focus").text

    assert result == "text area text areas eye eye eff ee dom click or change or focus"


def test_sanitize_tts_text_summarizes_endpoint_bullet_blocks():
    raw = """The system provides the following API methods:
- **GET** /api/v1/form-controls to list all controls.
- **GET** /api/v1/form-controls/assets to list asset files.
- **GET** /api/v1/form-controls/discover-keys to discover literal data-fc-key usages.
- **POST** /api/v1/form-controls to create a new entry.
- **PUT** /api/v1/form-controls/{id} to update an entry.
- **DELETE** /api/v1/form-controls/{id} to remove an entry.

Done."""

    result = sanitize_tts_text(raw).text

    assert result == (
        "The system provides the following A pee eye methods:\n\n"
        "There is an A pee eye endpoint list with 6 endpoints using GET, POST, PUT, and DELETE. "
        "It is summarized here rather than read row by row.\n\n"
        "Done."
    )
    assert "**" not in result
    assert "/api/" not in result


def test_sanitize_tts_text_removes_only_top_backlink():
    raw = """[<- web-design README](README.md)

# FORM-CONTROLS

← [README](README.md)

The page body remains."""

    result = sanitize_tts_text(raw)

    assert result.text == "FORM CONTROLS.\n\n← [README](README dot em dee)\n\nThe page body remains."
