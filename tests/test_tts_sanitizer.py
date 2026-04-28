from app.tts_sanitizer import prepare_tts_markdown_for_llm, sanitize_tts_text


def test_sanitize_tts_text_projects_markdown_headings_and_source_refs():
    raw = """**Progress So Far**
As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories [S1]. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage [S1]. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance [S2], [S4].

**Current Challenges**
Despite the progress, there are a few areas."""

    result = sanitize_tts_text(raw)

    assert result.text == """Progress So Far.

As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non root repositories. The backend now proxies search requests through the Blueprints A pee eye, and the frontend supports multiple search modes with persistent state in local storage. Additionally, the turbo veck Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance.

Current Challenges.

Despite the progress, there are a few areas."""
    assert list(result.transforms) == [
        "normalize_newlines",
        "strip_top_backlink_line",
        "strip_source_refs",
        "redact_tts_secret_material",
        "project_markdown_headings",
        "summarize_fenced_code_blocks",
        "summarize_markdown_tables",
        "summarize_endpoint_list_blocks",
        "strip_inline_code_ticks",
        "strip_inline_markdown_emphasis",
        "strip_markdown_list_markers",
        "speak_known_attribute_names",
        "speak_tts_compound_tokens",
        "speak_legacy_letter_names",
        "speak_tts_known_terms",
        "speak_tts_file_extensions",
        "speak_legacy_letter_names_after_file_extensions",
        "speak_tts_identifiers",
        "speak_tts_acronyms",
        "redact_tts_secret_material",
        "speak_remaining_pipes",
        "speak_tts_punctuation",
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
        == "Blueprints goo ee uses a data eff sea key HTML attribute, data eff sea event, and stray ticks."
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

    assert sanitize_tts_text(raw).text == "Example:\n\nThere is an HTML example here.\n\nDone."


def test_sanitize_tts_text_speaks_common_technical_acronyms():
    raw = "LED SVG png jpg VM LXC805 AI API GUI DNS HTTPS mTLS IPv6 UUID SQLite pfSense CI/CD js html"

    assert (
        sanitize_tts_text(raw).text
        == (
            "LED ess vee gee pee enn gee jay peg vee em LXC 805 "
            "A eye A pee eye goo ee dee enn ess aitch tee tee pee ess mTLS "
            "eye pee vee six you you eye dee sequel lite pee eff sense see eye, see dee "
            "JavaScript HTML"
        )
    )


def test_sanitize_tts_text_speaks_file_extensions_differently_from_acronyms():
    raw = "Open `form-controls.js`, icons.svg, page.HTML, config.env, table_layout_catalog.json, and .svg."

    assert (
        sanitize_tts_text(raw).text
        == (
            "Open form controls dot jay ess, icons dot ess vee gee, page dot HTML, "
            "config dot ee en vee, table layout catalog dot Jason, and dot ess vee gee."
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


def test_sanitize_tts_text_handles_requested_doc_speech_vocabulary():
    raw = (
        "LiteLLM postgres fleet CA public CA "
        "https://127.0.0.1:4000 ../foo/bar C:\\Temp @ eth0 eth1 rtx env .env "
        "gitignored .gitignored <think></think> OOM vmid seekdb certs mcp "
        "dockge xmemory pipecat livecat vllm moe LLMClient.chat openclaw .claude "
        "byok nullclaw AI pockettts playwright websocket clonedrepos localstorage "
        "sessionstorage zai z.ai vscodium vscode totp RAG turbovec taliscale tailscale "
        "vps dns -cli crawl4ai cHTP01 liteparse markitdown scrapling searxng vikunja "
        "... path/to/file.json"
    )

    assert sanitize_tts_text(raw).text == (
        "light L.L.M post gress fleet Certificate Authority public certificate authority url 127 dot 0 dot 0 dot 1 colon 4000 "
        "parent of foo slash bar C: back slash Temp at network port eff 0 network port eff 1 are tee ex dot ee en vee "
        "dot ee en vee dot git ignored dot git ignored think tags Out Of Memory Error Virtual Machine eye dee "
        "seek dee bee certificates em see pee Dockage ex memory pipe cat live cat V L.L.M Mixture of Experts "
        "L.L.M client dot chat open claw dot claude Bring Your Own Key null claw A eye pocket tee tee ess "
        "play wright web socket cloned repos local storage session storage zed A eye zed A eye vee ess code ee um "
        "vee ess code tee oh tee pee rag turbo veck tail scale tail scale vee pee ess dee enn ess CLI "
        "crawl for A eye chat private zero one light parse mark it down scrape ling seer ex next generation "
        "vee coon ee yah ellipses path slash to slash file dot Jason"
    )


def test_sanitize_tts_text_redacts_secret_like_keys_before_caching():
    raw = (
        "The virtual key for fleet use is sk-EXAMPLEVIRTUALKEY000000000000, "
        "and Authorization: Bearer EXAMPLETOKENVALUE000000000000."
    )

    result = sanitize_tts_text(raw).text

    assert result == (
        "The virtual key for fleet use is redacted key, and Authorization: Bearer redacted key."
    )
    assert "EXAMPLEVIRTUALKEY" not in result
    assert "EXAMPLETOKENVALUE" not in result


def test_sanitize_tts_text_speaks_colons_only_when_structural():
    raw = "status: ok\nstatus1: ok\nstatus1:status2\nstatus3 : status4\n: leading"

    assert sanitize_tts_text(raw).text == (
        "status: okay\n"
        "status1: okay\n"
        "status1 colon status2\n"
        "status3 colon status4\n"
        "colon leading"
    )


def test_sanitize_tts_text_cleans_legacy_letter_names_and_pve_forms():
    raw = (
        "LIGHTL.L.M, LITE.L.M, light.LM, light dot l dot m, "
        "light ell ell em, vee ell ell em, ell ell em client, L dot L dot M, "
        "H tee em ell, ell ex sea 805, tee ell ess, PVee999, pee vee ee 998"
    )

    assert (
        sanitize_tts_text(raw).text
        == (
            "light L.L.M, light L.L.M, light L.L.M, light L.L.M, light L.L.M, "
            "V L.L.M, L.L.M client, L.L.M, HTML, LXC 805, TLS, PVE999, PVE998"
        )
    )


def test_sanitize_tts_text_preserves_llm_pronunciation_in_paths():
    result = sanitize_tts_text("LiteLLM/config.yaml and light L.L.M/config.yaml").text

    assert result == "light L.L.M slash config dot yammel and light L.L.M slash config dot yammel"


def test_prepare_tts_markdown_for_llm_redacts_secret_like_keys():
    raw = "The virtual key for fleet use is sk-EXAMPLEVIRTUALKEY000000000000."

    result = prepare_tts_markdown_for_llm(raw)

    assert result == "The virtual key for fleet use is redacted key."
    assert "EXAMPLEVIRTUALKEY" not in result


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
