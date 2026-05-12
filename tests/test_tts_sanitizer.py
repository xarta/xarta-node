import importlib.util
import json
import re
import sys
from pathlib import Path

_SERVICE_SANITIZER = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/app/services/tts_sanitizer.py"
)
_HYHEN_AUTO_PRESERVE_SCRIPT = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/scripts/auto_preserve_hyphenated_terms.py"
)
_UNKNOWN_COUPLET_SUGGEST_SCRIPT = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/scripts/suggest_unknown_couplet_transforms.py"
)
_HYPHEN_INVENTORY_SCRIPT = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/scripts/inventory_hyphenated_terms.py"
)
_HYPHEN_RUNTIME_POLICY = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/app/services/tts_hyphenation_policy.runtime.json"
)
_HYPHEN_UNKNOWN_COUPLETS = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/app/services/tts_hyphenation_unknown_couplets.json"
)
_UNKNOWN_COUPLET_TRANSFORMS = Path(
    "/xarta-node/.lone-wolf/stacks/pockettts-openai/app/services/tts_unknown_couplet_transforms.json"
)
_SPEC = importlib.util.spec_from_file_location("pockettts_service_tts_sanitizer", _SERVICE_SANITIZER)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
_AUTO_SPEC = importlib.util.spec_from_file_location(
    "pockettts_auto_preserve_hyphenated_terms", _HYHEN_AUTO_PRESERVE_SCRIPT
)
assert _AUTO_SPEC is not None and _AUTO_SPEC.loader is not None
sys.path.insert(0, str(_HYHEN_AUTO_PRESERVE_SCRIPT.parent))
_AUTO_MODULE = importlib.util.module_from_spec(_AUTO_SPEC)
sys.modules[_AUTO_SPEC.name] = _AUTO_MODULE
_AUTO_SPEC.loader.exec_module(_AUTO_MODULE)
_SUGGEST_SPEC = importlib.util.spec_from_file_location(
    "pockettts_suggest_unknown_couplet_transforms", _UNKNOWN_COUPLET_SUGGEST_SCRIPT
)
assert _SUGGEST_SPEC is not None and _SUGGEST_SPEC.loader is not None
_SUGGEST_MODULE = importlib.util.module_from_spec(_SUGGEST_SPEC)
sys.modules[_SUGGEST_SPEC.name] = _SUGGEST_MODULE
_SUGGEST_SPEC.loader.exec_module(_SUGGEST_MODULE)
_INVENTORY_SPEC = importlib.util.spec_from_file_location(
    "pockettts_inventory_hyphenated_terms", _HYPHEN_INVENTORY_SCRIPT
)
assert _INVENTORY_SPEC is not None and _INVENTORY_SPEC.loader is not None
_INVENTORY_MODULE = importlib.util.module_from_spec(_INVENTORY_SPEC)
sys.modules[_INVENTORY_SPEC.name] = _INVENTORY_MODULE
_INVENTORY_SPEC.loader.exec_module(_INVENTORY_MODULE)

prepare_tts_markdown_for_llm = _MODULE.prepare_tts_markdown_for_llm
sanitize_tts_text = _MODULE.sanitize_tts_text
terminate_tts_line_endings = _MODULE.terminate_tts_line_endings


def test_sanitize_tts_text_projects_markdown_headings_and_source_refs():
    raw = """**Progress So Far**
As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories [S1]. The backend now proxies search requests through the Blueprints API, and the frontend supports multiple search modes with persistent state in local storage [S1]. Additionally, the TurboVec Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance [S2], [S4].

**Current Challenges**
Despite the progress, there are a few areas."""

    result = sanitize_tts_text(raw)

    assert result.text == """Progress So Far.

As of late April 2026, the first pass of the Blueprints integration has been successfully deployed across public root and non-root repositories. The backend now proxies search requests through the Blueprints application programming interface, and the frontend supports multiple search modes with persistent state in local storage. Additionally, the turbo-veck Docs stack is fully operational, with a complete corpus index and successful smoke tests confirming health and performance.

Current Challenges.

Despite the progress, there are a few areas."""
    assert list(result.transforms) == [
        "normalize_newlines",
        "strip_top_backlink_line",
        "strip_source_refs",
        "redact_tts_secret_material",
        "project_markdown_headings",
        "speak_storage_pcie_terms",
        "summarize_fenced_code_blocks",
        "summarize_markdown_tables",
        "summarize_endpoint_list_blocks",
        "strip_inline_code_ticks",
        "strip_inline_markdown_emphasis",
        "strip_markdown_list_markers",
        "speak_known_attribute_names",
        "speak_tts_compound_tokens",
        "speak_http_status_codes",
        "speak_domain_suffixes",
        "speak_legacy_letter_names",
        "shield_litellm_aliases",
        "speak_tts_file_extensions",
        "speak_legacy_letter_names_after_file_extensions",
        "speak_env_key_names",
        "speak_tts_identifiers",
        "speak_unknown_couplet_terms",
        "speak_tts_known_terms",
        "speak_litellm_aliases",
        "speak_tts_known_terms_after_litellm_aliases",
        "speak_tts_acronyms",
        "speak_tts_product_terms",
        "redact_tts_secret_material",
        "speak_remaining_pipes",
        "speak_tts_punctuation",
        "speak_port_and_colon_numbers",
        "clean_spoken_url_artifacts",
        "normalize_spacing",
    ]


def test_sanitize_tts_text_handles_hash_headings_and_inline_emphasis():
    result = sanitize_tts_text("# Background\nThe **xarta-node** docs are _indexed_ [S12].")

    assert result.text == "Background.\n\nThe zarta node docs are indexed."


def test_sanitize_tts_text_speaks_data_fc_key_attribute():
    result = sanitize_tts_text(
        "Blueprints GUI uses a `data-fc-key` HTML attribute, `data-fc-event`, and stray `ticks."
    )

    assert (
        result.text
        == "Blueprints GUI uses a data eff sea key HTML attribute, data eff sea event, and stray ticks."
    )
    assert "`" not in result.text


def test_sanitize_tts_text_speaks_snake_case_and_kebab_case_identifiers():
    result = sanitize_tts_text("The `form_controls` table maps table_layout_catalog rows for NAV-ITEMS.")

    assert result.text == "The form controls table maps table layout catalog rows for NAV ITEMS."


def test_sanitize_tts_text_preserves_safe_two_word_terms_and_transforms_nav_items():
    result = sanitize_tts_text("Keep purpose-built user-facing copy, but NAV-ITEMS splits.").text

    assert result == "Keep purpose-built user-facing copy, but NAV ITEMS splits."


def test_tts_hyphen_auto_preserve_blocks_sanitizer_terms():
    tokens = _AUTO_MODULE.sanitizer_transform_tokens(_SERVICE_SANITIZER)
    runtime_policy = json.loads(_HYPHEN_RUNTIME_POLICY.read_text(encoding="utf-8"))

    assert "auth" in tokens
    assert "webauthn" in tokens
    assert "yubikey" in tokens
    assert "repo" in tokens
    assert "env" in tokens
    assert "auth-exempt" not in runtime_policy["p"]
    assert "auth-token" not in runtime_policy["p"]
    assert "webauthn-backed" not in runtime_policy["p"]
    assert "yubikey-derived" not in runtime_policy["p"]
    assert "yubikey-protected" not in runtime_policy["p"]
    assert "a-eye-embeddings" not in runtime_policy["p"]
    assert "a-eye-cheap" not in runtime_policy["p"]
    assert "primary-open" not in runtime_policy["p"]
    assert "purpose-built" in runtime_policy["p"]
    assert "non-root" in runtime_policy["p"]


def test_tts_hyphen_auto_preserve_requires_dictionary_words():
    source_policy = {
        "entries": {
            "purpose-built": {"source_count": 2},
            "madeup-widget": {"source_count": 3},
            "auth-token": {"source_count": 4},
            "nav-items": {"source_count": 5},
        }
    }
    dictionary_words = {"purpose", "built", "widget", "token", "items"}
    sanitizer_tokens = {"auth"}
    force_transform_terms = {"nav-items"}

    policy, _stats = _AUTO_MODULE.classify_policy(
        source_policy,
        {},
        sanitizer_tokens,
        force_transform_terms,
        dictionary_words,
    )
    unknown = _AUTO_MODULE.build_unknown_couplets_report(
        source_policy,
        sanitizer_tokens,
        force_transform_terms,
        dictionary_words,
        Path("/tmp/test-dictionary"),
    )

    assert policy["entries"]["purpose-built"]["dehyphenate"] is False
    assert policy["entries"]["madeup-widget"]["dehyphenate"] is True
    assert policy["entries"]["auth-token"]["dehyphenate"] is True
    assert policy["entries"]["nav-items"]["dehyphenate"] is True
    assert unknown["e"] == {
        "madeup-widget": {
            "m": ["madeup"],
            "c": 3,
        }
    }


def test_tts_hyphen_unknown_couplets_report_excludes_known_transform_terms():
    unknown = json.loads(_HYPHEN_UNKNOWN_COUPLETS.read_text(encoding="utf-8"))

    assert "purpose-built" not in unknown["e"]
    assert "auth-token" not in unknown["e"]
    assert "nav-items" not in unknown["e"]
    assert "webauthn-backed" not in unknown["e"]
    assert "yubikey-derived" not in unknown["e"]
    assert "private-pki" not in unknown["e"]
    assert "presidio-pii" not in unknown["e"]
    assert "nvidia-cuda" not in unknown["e"]
    assert "oom-killed" not in unknown["e"]
    assert "aes-gcm" in unknown["e"]


def test_unknown_couplet_transform_suggestions_cover_common_patterns():
    transforms = json.loads(_UNKNOWN_COUPLET_TRANSFORMS.read_text(encoding="utf-8"))

    assert transforms["c"]["costoverhead assessment"] == "cost-overhead assessment"
    assert transforms["c"]["couchdb based"] == "couch DB based"
    assert transforms["c"]["crawlerrunconfig compatible"] == "crawler run config compatible"
    assert transforms["c"]["backup tbody"] == "backup tee body"
    assert transforms["c"]["badge btn"] == "badge button"
    assert transforms["c"]["apitotp authentication"] == "API TOTP authentication"
    assert transforms["c"]["answerability threshold"] == "answer-ability threshold"
    assert transforms["c"]["aip type"] == "AIP type"
    assert transforms["c"]["aria haspopup"] == "aria has-popup"
    assert transforms["c"]["endpoint list"] == "endpoint list"
    assert transforms["c"]["azagent ubuntu"] == "a-zed agent ubuntu"
    assert transforms["c"]["behaviour reference"] == "behaviour reference"
    assert transforms["c"]["carnice like"] == "carnice-like"
    assert transforms["c"]["anthropic first"] == "Anthropic-first"
    assert transforms["c"]["breakpoint driven"] == "breakpoint-driven"
    assert transforms["c"]["blueprints keystore"] == "Blueprints key-store"
    assert transforms["c"]["pkg config"] == "package-config"
    assert transforms["c"]["bookmarks embeddings"] == "bookmarks embeddings"
    assert transforms["c"]["pre signoff"] == "pre-sign-off"
    assert transforms["c"]["post indexsync"] == "post-index-sync"
    assert transforms["c"]["print errorlogs"] == "print error-logs"
    assert transforms["c"]["reranker health"] == "re-ranker health"
    assert transforms["c"]["non allowlisted"] == "non-allow-listed"
    assert transforms["c"]["non autoregressive"] == "non-auto-regressive"
    assert transforms["c"]["non ipool"] == "non-eye-pool"
    assert transforms["c"]["a za"] == "a zed a"
    assert transforms["c"]["agentic chunker"] == "a gent-ick chucker"
    assert transforms["c"]["anthropic passthrough"] == "Anthropic pass-through"
    assert transforms["c"]["arping sweep"] == "ar-ping sweep"
    assert transforms["c"]["ata backed"] == "ATA backed"
    assert transforms["c"]["back compat"] == "backward compatible"
    assert transforms["c"]["bare systemd"] == "bare system-dee"
    assert transforms["c"]["bg ctrl"] == "background control"
    assert transforms["c"]["blueprints appappaiclientpy"] == "Blueprints app-app a-eye client-pie"
    assert transforms["c"]["blueprints observability"] == "Blueprints observe-ability"
    assert transforms["c"]["browser wasm"] == "browser whasm"
    assert transforms["c"]["build meson"] == "build meezon"
    assert transforms["c"]["bulk precreate"] == "bulk pre-create"
    assert transforms["c"]["capabilities dict"] == "capabilities dictionary"
    assert transforms["c"]["carnace hermes"] == "carnice Hermes"
    assert transforms["c"]["carnice style"] == "carnice-style"
    assert transforms["c"]["cgroup based"] == "sea-group based"
    assert transforms["c"]["chatgpt plan"] == "chat GPT plan"
    assert transforms["c"]["checkbox list"] == "check-box list"
    assert transforms["c"]["checksum member"] == "check-sum member"
    assert transforms["c"]["chipset style"] == "chip-set style"
    assert transforms["c"]["chromadb replacement"] == "chroma DB replacement"
    assert transforms["c"]["chu tianxiang"] == "Chu Tianxiang"
    assert transforms["c"]["ciphertext only"] == "cipher-text only"
    assert transforms["c"]["clockhtml internals"] == "clock HTML internals"
    assert transforms["c"]["cloud fallback"] == "cloud-fallback"
    assert transforms["c"]["cloud init"] == "cloud-initialisation"
    assert transforms["c"]["cmd shell"] == "command-shell"
    assert transforms["c"]["cms ifying"] == "CMS iffy-ing"
    assert transforms["c"]["code workspace"] == "code work-space"
    assert transforms["c"]["codium server"] == "Codium Server"
    assert transforms["c"]["column chooser"] == "column chooser"
    assert transforms["c"]["composeyaml draft"] == "compose yammel draft"
    assert transforms["c"]["configyaml entry"] == "config-yammel entry"
    assert transforms["c"]["configyml reference"] == "config yammel reference"
    assert transforms["c"]["connect timeout"] == "connect time-out"
    assert transforms["c"]["connectivity testkit"] == "connectivity test-kit"
    assert transforms["c"]["copypaste prompt"] == "copy-paste prompt"
    assert transforms["c"]["cors bypass"] == "cross-origin resource sharing bypass"
    assert transforms["c"]["crawlerpool section"] == "crawler-pool section"
    assert transforms["c"]["csp safe"] == "CSP Safe"
    assert transforms["c"]["current tailnets"] == "current tail-nets"
    assert transforms["c"]["cyan gradient"] == "cyan gradient"
    assert transforms["c"]["data dir"] == "data directory"
    assert transforms["c"]["data filename"] == "data file-name"
    assert transforms["c"]["data fn"] == "data function"
    assert transforms["c"]["data gkey"] == "data gee-key"
    assert transforms["c"]["data idx"] == "data IDX"
    assert transforms["c"]["dataset level"] == "dataset level"
    assert transforms["c"]["deep agentic"] == "deep a gent-ick"
    assert transforms["c"]["defrag relevant"] == "defragment relevant"
    assert transforms["c"]["descrip tion"] == "description"
    assert transforms["c"]["diag chip"] == "diagnostic chip"
    assert transforms["c"]["diag modal"] == "diagnostic modal"
    assert transforms["c"]["direct loopback"] == "direct loop-back"
    assert transforms["c"]["distribute nodesjson"] == "distribute nodes dot json"
    assert transforms["c"]["distributed failover"] == "distributed fail-over"
    assert transforms["c"]["docker backed"] == "docker-backed"
    assert transforms["c"]["docker capable"] == "docker-capable"
    assert transforms["c"]["docker compose"] == "docker-compose"
    assert transforms["c"]["docker enabled"] == "docker-enabled"
    assert transforms["c"]["docker enabling"] == "docker enabling"
    assert transforms["c"]["docker level"] == "docker-level"
    assert transforms["c"]["docker network"] == "docker-network"
    assert transforms["c"]["docker oriented"] == "docker-oriented"
    assert transforms["c"]["docker published"] == "docker-published"
    assert transforms["c"]["docker registry"] == "docker-registry"
    assert transforms["c"]["docker related"] == "docker-related"
    assert transforms["c"]["docker run"] == "docker-run"
    assert transforms["c"]["docker specific"] == "docker-specific"
    assert transforms["c"]["docker stacks"] == "docker-stacks"
    assert transforms["c"]["docs centric"] == "documents-centric"
    assert transforms["c"]["docs summarizer"] == "documents-summarizer"
    assert (
        transforms["c"]["docsdockgeclaude codemd"]
        == "mark-down document for Dockge Claude Code container"
    )
    assert (
        transforms["c"]["docsdockgenullclaw basicsmd"]
        == "mark-down document for Dockge Null-Claw Basics container"
    )
    assert (
        transforms["c"]["docsdockgepockettts openaimd"]
        == "mark-down document for Dockge Pocket-TTS Open a-eye container"
    )
    assert (
        transforms["c"]["docsdockgeturbovec docsmd"]
        == "mark-down document for Dockge turbo-veck container"
    )
    assert transforms["c"]["dot namespaced"] == "dot name-spaced"
    assert "private pki" not in transforms["c"]
    assert "presidio pii" not in transforms["c"]
    assert "nvidia cuda" not in transforms["c"]
    assert "oom killed" not in transforms["c"]


def test_unknown_couplet_transform_suggestions_cover_reviewed_e_to_i_batch():
    transforms = json.loads(_UNKNOWN_COUPLET_TRANSFORMS.read_text(encoding="utf-8"))

    expected = {
        "dotenv webpack": "dot ENV webpack",
        "double scrollbar": "double scroll-bar",
        "drag resize": "drag resize",
        "drive scsin": "drive SCS in",
        "dropdown arrow": "dropdown arrow",
        "dropdown open": "dropdown open",
        "en us": "English US",
        "error exitcode": "error exit-code",
        "ev ec": "EV EC",
        "export preflight": "export pre-flight",
        "failover architecture": "failover-architecture",
        "failover observed": "failover-observed",
        "failover probe": "failover-probe",
        "failover topology": "failover-topology",
        "fallback capable": "fallback-capable",
        "fallback only": "fallback-only",
        "fallback page": "fallback-page",
        "fallback playback": "fallback play-back",
        "fast ttfa": "fast Time To First Audio",
        "fc container": "FC Container",
        "fc status": "FC Status",
        "fc table": "FC Table",
        "fc tbody": "FC Tee Body",
        "ff only": "FF Only",
        "ffffffff keyed": "Hex eff eight times, like four Gibi-bytes, keyed",
        "fgtinn ebiv": "FGT inn E Biv",
        "filesystem isolation": "file-system isolation",
        "filesystem layout": "file-system layout",
        "filesystem level": "file-system level",
        "fleet replicable": "fleet replicable",
        "footer pager": "footer pager",
        "fpool origin": "F-pool origin",
        "frontmatter aware": "front-matter aware",
        "full reindex": "full re-index",
        "fusion mpt": "fusion MPT",
        "fx kvbaxjgismb": "FX KV Bax JGI SMB",
        "get tasksid": "get tasks ID",
        "gfm style": "GFM style",
        "gh copilot": "GH Co-pilot",
        "git based": "git based",
        "git clones": "git clones",
        "git credentials": "git credentials",
        "git ignored": "git ignored",
        "git pull": "git pull",
        "git relative": "git relative",
        "git tracked": "git tracked",
        "githookspre commit": "git-hooks pre-commit",
        "gytyhvqh e": "GYT YH VGH",
        "handoff template": "hand-off template",
        "hangwinmcp chrome": "hang-win MCP Chrome",
        "harddrive problems": "hard-drive problems",
        "headscale provided": "head-scale provided",
        "heretic abliterated": "heretic Abliterated",
        "hermes handoff": "Hermes hand-off",
        "hermes maint": "Hermes maintenance",
        "hmac secret": "Hash-based Message Authentication Code Secret",
        "hookscript before": "hook-script before",
        "horiz scroll": "horizontal scroll",
        "hu xiaobai": "Hu Xiaobai",
        "huggingface token": "Hugging Face token",
        "hw address": "Hardware Address",
        "hw type": "Hardware Type",
        "iana managed": "Internet Assigned Numbers Authority Managed",
        "icann operated": "Internet Corporation for Assigned Names and Numbers operated",
        "ihor sokoliuk": "Ihor Sokoliuk",
        "im alive": "IM Alive",
        "image searchable": "Image Searchable",
        "importapply strategy": "import-apply strategy",
        "infra internal": "infra-internal",
        "infra specific": "infra-specific",
        "integration roadmap": "integration road-map",
        "intra table": "infra-table",
        "invalid settingsyml": "invalid settings-yammel",
        "iptables persistent": "IP tables persistent",
        "iputils arping": "IP Utilities Ar-ping",
        "jq based": "JQ based",
        "kanban style": "Kanban-Style",
        "kokoro backed": "Kokoro backed",
        "kv cache": "KV Cache",
        "lakera guard": "Lak-era Guard",
        "later todos": "Later To-Doo's",
        "layout nav": "Layout Nav",
        "lifecycle metadata": "Lifecycle meta-data",
        "llmclient constructor": "Large Language Models client constructor",
        "loopback bound": "loop-back bound",
        "loopback only": "loop-back only",
        "loopback trust": "loop-back trust",
        "low mem": "low-memory",
        "lower vram": "lower vee-Ram",
        "maint read": "maintenance read",
        "majel older": "older Majel",
        "manage apikey": "Manage Application Programming Interface key",
        "maskable friendly": "mask-able friendly",
        "matrix continuwuity": "matrix continuity with a w",
        "matrix tuwunel": "matrix tunnel thing with a w",
        "maxtravel calculation": "max-travel calculation",
        "measured footer": "measured-footer",
        "mega prompt": "mega-promp",
        "mega wrapper": "mega-wrapper",
        "meghrfajazdavkwwwdrlur ojowt": "unpronouncable-words",
        "metaslab note": "meta-slab note",
        "micro blog": "micro-blog",
        "minimax oriented": "Mini-Max oriented",
        "mistral common": "Mistral common",
        "mnt models": "Mount Models",
        "mtp preserved": "MTP preserved",
        "multi toolchain": "multi tool-chain",
        "must revalidate": "must re-validate",
        "mutationobserver sync": "mutation observer-sync",
        "mysql compat": "my-sequel compatibility",
        "name asc": "name ascending",
        "name desc": "name descending",
        "nav layout": "Nav lay-out",
        "nested virtualization": "nested virtualization",
        "netcat traditional": "net-cat traditional",
        "nn gen": "en en generation",
        "nn integrity": "en en integrity",
        "nn keys": "en en keys",
        "nn peers": "en en peers",
        "no cert": "no certificate",
        "no cors": "no cross-origin resource sharing",
        "no fallback": "no fallback",
        "no trunc": "no truncation",
        "node themed": "node-themed",
        "nordvpn backed": "Nord VPN backed",
        "npm based": "NPM based",
        "nryd cy": "en RYD CY",
        "offline uncorrectable": "offline un-correct-able",
        "oidc based": "Open ID Connect",
        "ollama compatible": "O'Llama compatible",
        "omi pu": "uncertain pronounceable words: oh-my poo, maybe poo means pull-up or pop-up,",
        "on prem": "on-premises",
        "open webui": "open web UI",
        "openmoss team": "Open Moss team",
        "openssh client": "Open SSH client",
        "operational runbooks": "operational run-books",
        "operational takeaway": "operational take-away",
        "operator resized": "operator re-sized",
        "over permissioning": "over permission-ing",
        "pa dmrkiui": "uncertain pronounceable words: pa dee mark-ee UI",
        "paperclip app": "paper-clip application",
        "paperclip managed": "paper-clip managed",
        "passphrase source": "pass-phrase source",
        "passphrase sources": "pass-phrase sources",
        "per dataset": "per data-set",
        "pilot runbook": "pilot run-book",
        "plex config": "Plex configuration",
        "plex transcode": "Plex trans-code",
        "pnpm lock": "PNP em lock",
        "pointerup drag": "pointer-up",
        "portainer ce": "Portainer CE",
        "portainer ee": "Portainer E E",
        "portainer stack": "Portainer stack",
        "preflight checklist": "pre-flight checklist",
        "preflight checks": "pre-flight checks",
        "private runbook": "private run-book",
        "prod mtime": "production em-time",
        "proposed hostname": "proposed host-name",
        "proxmox config": "Proxmox configuration",
        "proxmox host": "Proxmox host",
        "proxmox managed": "Proxmox managed",
        "proxmox nets": "Proxmox nets",
        "proxmox probe": "Proxmox probe",
        "proxmox side": "Proxmox side",
        "proxmox style": "Proxmox style",
        "ptag accent": "pee-tag accent",
        "ptag gender": "pee-tag gender",
        "ptag quality": "pee-tag quality",
        "ptag source": "pee-tag source",
        "ptr derived": "pointer-derived",
        "pydantic models": "pie-dant-ick models",
        "qwen backed": "Qwen backed",
    }

    for source, replacement in expected.items():
        assert transforms["c"][source] == replacement
        assert source not in transforms["u"]


def test_unknown_couplet_suggestion_builder_preserves_existing_choices():
    unknown = {"e": {"badge-btn": {"m": ["btn"], "c": 1}, "custom-token": {"m": ["custom"], "c": 2}}}
    existing = {"c": {"badge btn": "badge control"}, "u": {"custom token": "custom token"}}

    suggestions = _SUGGEST_MODULE.build_transform_suggestions(
        unknown,
        {"badge", "custom", "token"},
        set(),
        existing,
    )

    assert suggestions["c"]["badge btn"] == "badge control"
    assert suggestions["u"]["custom token"] == "custom token"


def test_hyphen_inventory_skips_generated_chunk_boundary_fragments(tmp_path):
    db_path = tmp_path / "chunks.sqlite3"

    import sqlite3

    connection = sqlite3.connect(db_path)
    try:
        connection.execute("CREATE TABLE chunks (chunk_index INTEGER NOT NULL, text TEXT NOT NULL)")
        connection.execute("CREATE TABLE chunks_fts_content (c2 TEXT)")
        connection.execute(
            "INSERT INTO chunks (chunk_index, text) VALUES (?, ?)",
            (0, "request-time at the start of a real document"),
        )
        connection.execute(
            "INSERT INTO chunks (chunk_index, text) VALUES (?, ?)",
            (3, "equest" + "-time chunk boundary fragment"),
        )
        connection.execute(
            "INSERT INTO chunks (chunk_index, text) VALUES (?, ?)",
            (3, "middle request-time remains a real full token"),
        )
        connection.execute(
            "INSERT INTO chunks_fts_content (c2) VALUES (?)",
            ("generated" + "-fts" + "-table should not be scanned",),
        )
        connection.commit()
    finally:
        connection.close()

    original_find_sqlite_files = _INVENTORY_MODULE.find_sqlite_files
    try:
        _INVENTORY_MODULE.find_sqlite_files = lambda _roots: [db_path]
        inventory = {}
        scanned = _INVENTORY_MODULE.scan_sqlite_tables(inventory, (tmp_path,))
    finally:
        _INVENTORY_MODULE.find_sqlite_files = original_find_sqlite_files

    assert scanned == 1
    assert "request-time" in inventory
    assert "equest" + "-time" not in inventory
    assert "generated" + "-fts-table" not in inventory


def test_hyphen_inventory_drops_stale_unreviewed_policy_entries():
    policy = _INVENTORY_MODULE.build_policy(
        {"request-time": _INVENTORY_MODULE._empty_record()},
        {
            "entries": {
                "equest" + "-time": {
                    "dehyphenate": True,
                    "reason": "Inventory default: remove the hyphen unless this term is reviewed for preservation.",
                    "source_count": 3,
                },
                "chat-private": {
                    "dehyphenate": False,
                    "reviewed": True,
                    "reason": "Reviewed speech form; preserve the hyphen for clearer TTS.",
                },
            }
        },
        {},
    )

    assert "request-time" in policy["entries"]
    assert "chat-private" in policy["entries"]
    assert "equest" + "-time" not in policy["entries"]


def test_sanitize_tts_text_speaks_confident_unknown_couplet_transforms():
    result = sanitize_tts_text(
        "Use badge-btn, backup-tbody, couchdb-based, apitotp-authentication, "
        "answerability-threshold, aria-haspopup, crawlerrunconfig-compatible, "
        "azagent-ubuntu, behaviour-reference, carnice-like, blueprints-keystore, "
        "pkg-config, bookmarks-embeddings, private-pki, presidio-pii, nvidia-cuda, nvidia-smi, "
        "oom-killed, pre-signoff, post-indexsync, print-errorlogs, reranker-health, "
        "non-allowlisted, non-autoregressive, non-ipool, a-za, agentic-chunker, "
        "anthropic-passthrough, arping-sweep, ata-backed, back-compat, bare-systemd, "
        "bg-ctrl, blueprints-appappaiclientpy, blueprints-observability, browser-wasm, "
        "build-meson, bulk-precreate, capabilities-dict, carnice-style, cgroup-based, "
        "chatgpt-plan, checkbox-list, checksum-member, chipset-style, chromadb-replacement, "
        "chu-tianxiang, ciphertext-only, clockhtml-internals, cloud-fallback, cloud-init, "
        "cmd-shell, cms-ifying, code-workspace, codium-server, column-chooser, composeyaml-draft, "
        "configyaml-entry, configyml-reference, connect-timeout, connectivity-testkit, "
        "copypaste-prompt, cors-bypass, crawlerpool-section, csp-safe, current-tailnets, "
        "cyan-gradient, data-dir, data-filename, data-fn, data-gkey, data-idx, dataset-level, "
        "deep-agentic, defrag-relevant, descrip-tion, diag-chip, diag-modal, direct-loopback, "
        "distribute-nodesjson, distributed-failover, docker-backed, docker-capable, docker-compose, "
        "docker-enabled, docker-enabling, docker-level, docker-network, docker-oriented, "
        "docker-published, docker-registry, docker-related, docker-run, docker-specific, "
        "docker-stacks, docs-centric, docs-summarizer, docsdockgeclaude-codemd, "
        "docsdockgenullclaw-basicsmd, docsdockgepockettts-openaimd, "
        "docsdockgeturbovec-docsmd, and dot-namespaced."
    ).text

    assert result == (
        "Use badge button, backup tee body, couch dee bee based, "
        "application programming interface tee oh tee pee authentication, "
        "answer-ability threshold, aria has-popup, crawler run config compatible, "
        "a-zed agent ubuntu, behaviour reference, carnice-like, Blueprints key-store, "
        "package-config, bookmarks embeddings, private PKI, Presidio P two, "
        "en-vid ee-ah cue-dah, en-vid ee-ah SMI, "
        "Out Of Memory killed, pre-sign-off, post-index-sync, print error-logs, re-ranker health, "
        "non-allow-listed, non-auto-regressive, non-eye-pool, a zed a, a gent-ick chucker, "
        "Anthropic pass-through, ar-ping sweep, ATA backed, backward compatible, bare system-dee, "
        "background control, Blueprints app-app a-eye client-pie, Blueprints observe-ability, browser whasm, "
        "build meezon, bulk pre-create, capabilities dictionary, carnice-style, sea-group based, "
        "chat GPT plan, check-box list, check-sum member, chip-set style, chroma dee bee replacement, "
        "Chu Tianxiang, cipher-text only, clock HTML internals, cloud-fallback, cloud-initialisation, "
        "command-shell, CMS iffy-ing, code work-space, Codium Server, column chooser, compose yammel draft, "
        "config-yammel entry, config yammel reference, connect time-out, connectivity test-kit, "
        "copy-paste prompt, cross-origin resource sharing bypass, crawler-pool section, CSP Safe, current tail-nets, "
        "cyan gradient, data directory, data file-name, data function, data gee-key, data IDX, dataset level, "
        "deep a gent-ick, defragment relevant, description, diagnostic chip, diagnostic modal, direct loop-back, "
        "distribute nodes dot Jason, distributed fail-over, docker-backed, docker-capable, docker-compose, "
        "docker-enabled, docker enabling, docker-level, docker-network, docker-oriented, "
        "docker-published, docker-registry, docker-related, docker-run, docker-specific, "
        "docker-stacks, documents-centric, documents-summarizer, "
        "mark-down document for Dockage Claude Code container, "
        "mark-down document for Dockage null-claw Basics container, "
        "mark-down document for Dockage pocket-TTS open a-eye container, "
        "mark-down document for Dockage turbo-veck container, and dot name-spaced."
    )


def test_sanitize_tts_text_speaks_reviewed_e_to_i_unknown_couplets():
    result = sanitize_tts_text(
        "dotenv-webpack double-scrollbar drag-resize drive-scsin dropdown-arrow dropdown-open "
        "en-us error-exitcode ev-ec export-preflight failover-architecture failover-observed "
        "failover-probe failover-topology fallback-capable fallback-only fallback-page fallback-playback "
        "fast-ttfa fc-container fc-status fc-table fc-tbody ff-only ffffffff-keyed fgtinn-ebiv "
        "filesystem-isolation filesystem-layout filesystem-level fleet-replicable footer-pager "
        "fpool-origin frontmatter-aware full-reindex fusion-mpt fx-kvbaxjgismb get-tasksid "
        "gfm-style gh-copilot git-based git-clones git-credentials git-ignored git-pull "
        "git-relative git-tracked githookspre-commit gytyhvqh-e handoff-template hangwinmcp-chrome "
        "harddrive-problems headscale-provided heretic-abliterated hermes-handoff hermes-maint "
        "hmac-secret hookscript-before horiz-scroll hu-xiaobai huggingface-token hw-address hw-type "
        "iana-managed icann-operated ihor-sokoliuk im-alive image-searchable importapply-strategy "
        "infra-internal infra-specific integration-roadmap intra-table invalid-settingsyml iptables-persistent "
        "iputils-arping jq-based kanban-style kokoro-backed kv-cache lakera-guard later-todos layout-nav "
        "lifecycle-metadata llmclient-constructor loopback-bound loopback-only loopback-trust low-mem "
        "lower-vram maint-read majel-older manage-apikey maskable-friendly matrix-continuwuity matrix-tuwunel "
        "maxtravel-calculation measured-footer mega-prompt mega-wrapper meghrfajazdavkwwwdrlur-ojowt "
        "metaslab-note micro-blog minimax-oriented mistral-common mnt-models mtp-preserved multi-toolchain "
        "must-revalidate mutationobserver-sync mysql-compat name-asc name-desc nav-layout nested-virtualization "
        "netcat-traditional nn-gen nn-integrity nn-keys nn-peers no-cert no-cors no-fallback "
        "no-trunc node-themed nordvpn-backed npm-based nryd-cy offline-uncorrectable oidc-based "
        "ollama-compatible omi-pu on-prem open-webui openmoss-team openssh-client operational-runbooks "
        "operational-takeaway operator-resized over-permissioning pa-dmrkiui paperclip-app paperclip-managed "
        "passphrase-source passphrase-sources per-dataset pilot-runbook plex-config plex-transcode pnpm-lock "
        "pointerup-drag portainer-ce portainer-ee portainer-stack preflight-checklist preflight-checks "
        "private-runbook prod-mtime proposed-hostname proxmox-config proxmox-host proxmox-managed proxmox-nets "
        "proxmox-probe proxmox-side proxmox-style ptag-accent ptag-gender ptag-quality ptag-source "
        "ptr-derived pydantic-models qwen-backed"
    ).text

    assert result == (
        "dot ee en vee webpack double scroll-bar drag resize drive SCS in dropdown arrow dropdown open "
        "English US error exit-code EV EC export pre-flight failover-architecture failover-observed "
        "failover-probe failover-topology fallback-capable fallback-only fallback-page fallback play-back "
        "fast Time To First Audio FC Container FC Status FC Table FC Tee Body FF Only "
        "Hex eff eight times, like four Gibi-bytes, keyed FGT inn E Biv file-system isolation "
        "file-system layout file-system level fleet replicable footer pager F-pool origin "
        "front-matter aware full re-index fusion MPT FX KV Bax JGI ess em bee get tasks eye dee "
        "GFM style GH Co-pilot git based git clones git credentials git ignored git pull git relative "
        "git tracked git-hooks pre-commit GYT YH VGH hand-off template hang-win em see pee Chrome "
        "hard-drive problems head-scale provided heretic Abliterated Hermes hand-off Hermes maintenance "
        "Hash-based Message Authentication Code Secret hook-script before horizontal scroll Hu Xiaobai "
        "Hugging Face token Hardware Address Hardware Type Internet Assigned Numbers Authority Managed "
        "Internet Corporation for Assigned Names and Numbers operated Ihor Sokoliuk IM Alive Image Searchable "
        "import-apply strategy infra-internal infra-specific integration road-map infra-table invalid settings-yammel "
        "eye pee tables persistent eye pee Utilities Ar-ping JQ based Kanban-Style Kokoro backed KV Cache "
        "Lak-era Guard Later To-Doo's Layout Nav Lifecycle meta-data Large Language Models client constructor "
        "loop-back bound loop-back only loop-back trust low-memory lower vee-ram maintenance read older Majel "
        "Manage Application Programming Interface key mask-able friendly matrix continuity with a w "
        "matrix tunnel thing with a w max-travel calculation measured-footer mega-promp mega-wrapper "
        "unpronouncable-words meta-slab note micro-blog Mini-Max oriented Mistral common Mount Models "
        "MTP preserved multi tool-chain must re-validate mutation observer-sync my-sequel compatibility "
        "name ascending name descending Nav lay-out nested virtualization net-cat traditional en en generation "
        "en en integrity en en keys en en peers no certificate no cross-origin resource sharing no fallback "
        "no truncation node-themed Nord vee pee enn backed NPM based en RYD CY offline un-correct-able "
        "Open eye dee Connect O'Llama compatible uncertain pronounceable words: oh-my poo, maybe poo means "
        "pull-up or pop-up, on-premises open web you eye Open Moss team Open SSH client operational run-books "
        "operational take-away operator re-sized over permission-ing uncertain pronounceable words: pa dee "
        "mark-ee you eye paper-clip application paper-clip managed pass-phrase source pass-phrase sources "
        "per data-set pilot run-book Plex configuration Plex trans-code PNP em lock pointer-up Portainer CE "
        "Portainer E E Portainer stack pre-flight checklist pre-flight checks private run-book production "
        "em-time proposed host-name Proxmox configuration Proxmox host Proxmox managed Proxmox nets Proxmox "
        "probe Proxmox side Proxmox style pee-tag accent pee-tag gender pee-tag quality pee-tag source "
        "pointer-derived pie-dant-ick models Qwen backed"
    )


def test_tts_hyphen_auto_preserve_has_no_generated_sanitizer_token_conflicts():
    tokens = _AUTO_MODULE.sanitizer_transform_tokens(_SERVICE_SANITIZER)
    force_transform_terms = _AUTO_MODULE.load_force_transform_terms(
        Path("/xarta-node/.lone-wolf/stacks/pockettts-openai/app/services/tts_hyphenation_transform_terms.json")
    )
    dictionary_words = _AUTO_MODULE.load_dictionary_words(Path("/usr/share/dict/american-english"))
    runtime_policy = json.loads(_HYPHEN_RUNTIME_POLICY.read_text(encoding="utf-8"))

    assert _AUTO_MODULE.validate_runtime_policy(runtime_policy, tokens, force_transform_terms, dictionary_words) == []


def test_sanitize_tts_text_speaks_environment_and_nodes_keys():
    result = sanitize_tts_text(
        "CHTP01_AUTH_SECRET POSTGRES_PASSWORD TS_AUTHKEY CHTP01_VLLM_API_KEY "
        "OPENAI_BASE_URL SYNCTHING_DEVICE_ID pwa_icon_192 tailnet_ip better-auth."
    ).text

    assert result == (
        "chat-private zero one authorisation secret post gress password tee ess authorisation key "
        "chat-private zero one V L-LM application programming interface key open a-eye base you are ell "
        "sync-thing device eye dee pee double you ay icon 192 tail-net eye pee better authorisation."
    )


def test_sanitize_tts_text_speaks_webauthn_compounds_before_auth():
    result = sanitize_tts_text("WebAuthn-backed auth-token, yubikey-derived, and WebAuthn auth.").text

    assert result == "web orff en backed authorisation token, Yubi-key derived, and web orff en authorisation."


def test_sanitize_tts_text_speaks_known_joined_policy_terms():
    result = sanitize_tts_text("The allowlist, blocklist, denylist, and safelist are configured.")

    assert result.text == "The allow list, block list, deny list, and safe list are configured."


def test_prepare_tts_markdown_for_llm_preserves_markdown_for_model_prompt():
    raw = "Use `form_controls`, `data-fc-key`, and table_layouts in the narration source."

    assert (
        prepare_tts_markdown_for_llm(raw)
        == "Use `form_controls`, `data-fc-key`, and table_layouts in the narration source."
    )


def test_prepare_tts_markdown_for_llm_does_not_apply_speech_transforms():
    raw = "LiteLLM Remote-SSH `form_controls.js` SVG API L.L.M."

    prepared = prepare_tts_markdown_for_llm(raw)

    assert prepared == raw
    assert "light LLM" not in prepared
    assert "Remote SSH" not in prepared
    assert "form controls" not in prepared
    assert "ess vee gee" not in prepared
    assert "A pee eye" not in prepared


def test_prepare_tts_markdown_for_llm_preserves_fenced_code_blocks():
    raw = """Use `form_controls` and this example:

```html
<input type="text" data-fc-key="bookmarks.filter.search" />
```

Then mention SVG."""

    assert prepare_tts_markdown_for_llm(raw) == """Use `form_controls` and this example:

```html
<input type="text" data-fc-key="bookmarks.filter.search" />
```

Then mention SVG."""


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
            "LED ess vee gee pee enn gee jay peg vee em LXC eight zero five "
            "artificial intelligence application programming interface GUI domain name system H tee tee pee ess mTLS "
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


def test_sanitize_tts_text_avoids_double_dot_file_pronunciation():
    result = sanitize_tts_text(
        "Use dot env, dot .env, dot dot env, dot dot .env, .env, env, config.env, "
        "dot .json, dot dot json, dot md, dot .claude, dot dot claude, dot .ssh, "
        "dot dot ssh, .ssh, dot .config, dot dot config, .config, and dot dot gitignored."
    ).text

    assert result == (
        "Use dot ee en vee, dot ee en vee, dot ee en vee, dot ee en vee, "
        "dot ee en vee, dot ee-en-vee, config dot ee en vee, dot Jason, dot Jason, "
        "dot em dee, dot claude, dot claude, dot SSH, dot SSH, dot SSH, dot config, "
        "dot config, dot config, and dot git ignored."
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
        "application programming interface Endpoints.\n\n"
        "There is a table with 3 rows covering Method, Path, and Description.\n\n"
        "Next."
    )
    assert "|" not in result


def test_sanitize_tts_text_speaks_textarea_iife_and_dom_terms():
    result = sanitize_tts_text("textarea textareas IIFE DOM click|change|focus").text

    assert result == "text-area text-areas eye eye eff ee dom click or change or focus"


def test_sanitize_tts_text_handles_requested_doc_speech_vocabulary():
    raw = (
        "LiteLLM postgres Redis fleet CA public CA "
        "https://127.0.0.1:4000 ../foo/bar C:\\Temp @ eth0 eth1 rtx env .env "
        "gitignored .gitignored <think></think> OOM vmid seekdb certs mcp "
        "dockge xmemory pipecat livecat vllm moe LLMClient.chat openclaw .claude "
        "byok nullclaw AI pockettts playwright websocket clonedrepos localstorage "
        "sessionstorage zai z.ai vscodium vscode totp RAG turbovec taliscale tailscale "
        "vps dns -cli crawl4ai cHTP01 liteparse markitdown scrapling searxng vikunja "
        "... path/to/file.json"
    )

    assert sanitize_tts_text(raw).text == (
        "light L-LM post gress red-is fleet Certificate Authority public certificate authority you are ell 127 dot 0 dot 0 dot 1 colon four zero zero zero "
        "parent of foo slash bar C: back slash Temp at network port eff 0 network port eff 1 are tee ex dot ee-en-vee "
        "dot ee en vee dot git ignored dot git ignored think tags Out Of Memory Virtual Machine eye dee "
        "seek dee bee certificates em see pee Dockage ex memory pipe-cat lithe-cat V L-LM Mixture of Experts "
        "L-LM client dot chat open-claw dot claude Bring Your Own Key null-claw artificial intelligence pocket-TTS "
        "play-wright web-socket cloned repositories local-storage session-storage zed a-eye zed a-eye vee ess code ee um "
        "vee-ess code tee oh tee pee rag turbo-veck tail scale tail-scale vee pee ess domain name system CLI "
        "crawl for a-eye chat-private zero one light-parse mark-it-down scrape-ling seer ex next generation "
        "vee coon ee yah ellipses path slash to slash file dot Jason"
    )


def test_sanitize_tts_text_redacts_secret_like_keys_before_caching():
    raw = (
        "The virtual key for fleet use is sk-EXAMPLEVIRTUALKEY000000000000, "
        "and Authorization: Bearer EXAMPLETOKENVALUE000000000000."
    )

    result = sanitize_tts_text(raw).text

    assert result == (
        "The virtual key for fleet use is redacted key, and authorisation: Bearer redacted key."
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
            "light L-LM, light L-LM, light L-LM, light L-LM, light L-LM, "
            "V L-LM, L-LM client, L-LM, HTML, LXC eight zero five, TLS, "
            "PVE nine nine nine, PVE nine nine eight"
        )
    )


def test_sanitize_tts_text_preserves_llm_pronunciation_in_paths():
    result = sanitize_tts_text("LiteLLM/config.yaml and light L.L.M/config.yaml").text

    assert result == "light L-LM slash config dot yammel and light L-LM slash config dot yammel"


def test_sanitize_tts_text_speaks_infra_ids_digit_by_digit():
    result = sanitize_tts_text("PVE987 lxc654 LXC 805 pve-998 paths00 paths10 paths 07 paths00sub2").text

    assert result == (
        "PVE nine eight seven LXC six five four LXC eight zero five PVE nine nine eight "
        "paths zero zero paths one zero paths zero seven paths zero zero sub two"
    )


def test_sanitize_tts_text_speaks_partial_ip_patterns():
    result = sanitize_tts_text("Use 203.0.113.x, 198.x.2.3, and x.x.x.x. Keep 203.0.113.19.").text

    assert result == (
        "Use 203 dot 0 dot 113 dot X, 198 dot X dot 2 dot 3, and X dot X dot X dot X. "
        "Keep 203 dot 0 dot 113 dot 19."
    )


def test_sanitize_tts_text_speaks_live_as_lithe_only_in_technical_contexts():
    technical = sanitize_tts_text(
        "Validate the corrected cases live on the target nodes. Live local model tests passed. A live run updated it."
    ).text
    ordinary = sanitize_tts_text("I live on the target node during tests.").text

    assert technical == (
        "Validate the corrected cases lithe on the target nodes. "
        "lithe local model tests passed. A lithe run updated it."
    )
    assert ordinary == "I live on the target node during tests."


def test_sanitize_tts_text_speaks_web_status_codes_and_url_acronym():
    result = sanitize_tts_text(
        "The URL returned HTTP 503. Status code 404 and response 500 were seen. PostgreSQL stayed up."
    ).text

    assert result == (
        "The you are ell returned H tee tee pee five oh three. "
        "Status code four oh four and response five oh oh were seen. post gress sequel stayed up."
    )


def test_sanitize_tts_text_is_idempotent_for_speech_ready_url_terms():
    raw = (
        "The web UI is exposed at URL chat-private-01.example.local/. "
        "For diagnostics use URL localhost:18443, port 5432, or ports 18081 and 18443. "
        "YubiKey WebAuthn auth."
    )

    once = sanitize_tts_text(raw).text
    twice = sanitize_tts_text(once).text

    assert once == (
        "The web you eye is exposed at you are ell chat-private zero one dot example dot local slash. "
        "For diagnostics use you are ell local-host colon one eight four four three, port five four three two, "
        "or ports one eight zero eight one and one eight four four three. "
        "Yubi-key web orff en authorisation."
    )
    assert twice == once


def test_sanitize_tts_text_speaks_plural_port_lists_digit_by_digit():
    result = sanitize_tts_text(
        "Local diagnostics are available on loopback at ports 18081 and 18443. "
        "Fallback ports 18884, 19000, and 11235 remain local."
    ).text

    assert result == (
        "Local diagnostics are available on loopback at ports one eight zero eight one "
        "and one eight four four three. Fallback ports one eight eight eight four, "
        "one nine zero zero zero, and one one two three five remain local."
    )


def test_sanitize_tts_text_speaks_colon_port_mappings_digit_by_digit():
    result = sanitize_tts_text(
        "loopback: 18081 maps to the app HTTP port and 18443 maps to the HTTPS port."
    ).text

    assert result == (
        "loopback colon one eight zero eight one maps to the app H tee tee pee port "
        "and one eight four four three maps to the H tee tee pee ess port."
    )


def test_sanitize_tts_text_preserves_gui_and_hyphenates_localhost():
    result = sanitize_tts_text("gui GUI localhost URL localhost:18443").text

    assert result == "GUI GUI local-host you are ell local-host colon one eight four four three"


def test_sanitize_tts_text_speaks_xarta_repos_and_domain_suffixes():
    result = sanitize_tts_text(
        "Open repo, repos, repo's, and clonedrepos at https://xarta.local/foo or example.co.uk/xarta-node."
    ).text

    assert result == (
        "Open repository, repositories, repositories, and cloned repositories at you are ell zarta dot local slash foo "
        "or example dot koh dot UK slash zarta node."
    )


def test_sanitize_tts_text_formats_litellm_aliases_for_speech():
    assert (
        sanitize_tts_text("PRIMARY-LOCAL-PRIVATE-NO-PROTECTION model").text
        == "Primary-Local private No-Protection model"
    )
    assert (
        sanitize_tts_text("PRIMARY LOCAL PRIVATE NO PROTECTION model").text
        == "Primary-Local private No-Protection model"
    )
    assert (
        sanitize_tts_text("OPENROUTER-CHEAP-US-NO-PROTECTION model").text
        == "Open-Router-Cheap-US No-Protection model"
    )


def test_sanitize_tts_text_formats_all_litellm_aliases_from_raw_input():
    aliases = sorted(_MODULE._LITELLM_ALIAS_NAMES)
    raw_alias_word_re = re.compile(
        r"\b(?:ANTHROPIC|CHEAP|CHINA|CODING|EMBEDDINGS|EXPENSIVE|FLASH|FREE|GEMINI|LOCAL|"
        r"MEDIUM|MINIMAX|OPENAI|OPENROUTER|PRESERVE|PRIMARY|PRIVATE|PROTECTION|QWEN36|"
        r"RERANKER|SECONDARY|THINKING|VISION|WHISPER|ZAI)\b"
    )

    assert len(aliases) == 72
    for alias in aliases:
        result = sanitize_tts_text(f"{alias} model alias").text
        assert not raw_alias_word_re.search(result)


def test_prepare_tts_markdown_for_llm_preserves_ssh_for_model_prompt():
    result = prepare_tts_markdown_for_llm("Remote-SSH/Roo traffic and SSH target.")

    assert result == "Remote-SSH/Roo traffic and SSH target."


def test_terminate_tts_line_endings_adds_pause_punctuation():
    result = terminate_tts_line_endings("Implementation tracking append\n\nDone in this session:\nAlready done.")

    assert result == "Implementation tracking append.\n\nDone in this session.\nAlready done."


def test_sanitize_tts_text_speaks_subagent_with_hyphen():
    result = sanitize_tts_text("Use a subagent or multiple subagents.").text

    assert result == "Use a sub-agent or multiple sub-agents."


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
        "The system provides the following application programming interface methods:\n\n"
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
