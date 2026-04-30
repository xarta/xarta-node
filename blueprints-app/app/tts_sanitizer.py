"""Voice-focused text transforms for the Blueprints TTS wrapper."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class TtsTextTransform:
    name: str
    project: Callable[[str], str]


@dataclass(frozen=True)
class TtsSanitizeResult:
    text: str
    transforms: tuple[str, ...]


_SOURCE_REF_RE = re.compile(r"(?:[\s,;]*\[S\d+\])+", re.IGNORECASE)
_BOLD_HEADING_RE = re.compile(r"^\s*\*\*(?P<title>[^*\n][^*\n]*?)\*\*\s*$")
_MARKDOWN_HEADING_RE = re.compile(r"^\s*#{1,6}\s+(?P<title>.*?)\s*#*\s*$")
_MARKDOWN_TABLE_SEPARATOR_RE = re.compile(r"^\s*\|?[\s:|-]+\|[\s:|-]+\|?\s*$")
_ENDPOINT_LIST_ITEM_RE = re.compile(
    r"^\s*[-*]\s+(?:\*\*)?(?P<method>GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)(?:\*\*)?\s+"
    r"(?P<rest>\S.*)$",
    re.IGNORECASE,
)
_LIST_MARKER_RE = re.compile(r"(?m)^\s*(?:[-*+]\s+|\d+[.)]\s+)")
_TERMINAL_PUNCT_RE = re.compile(r"[.!?:;]$")
_SPEECH_LINE_TERMINAL_RE = re.compile(r"[.!?]$")
_BACKLINK_PREFIXES = ("<-", "←", "&larr;", "[<-", "[←", "[&larr;")
_INLINE_CODE_RE = re.compile(r"`([^`\n]+?)`")
_FENCED_CODE_BLOCK_RE = re.compile(r"^```(?P<lang>[^\n`]*)\n(?P<body>.*?)(?:^```\s*$|\Z)", re.MULTILINE | re.DOTALL)
_IDENTIFIER_WORD_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)+\b")
_FILE_EXTENSION_SPEECH: tuple[tuple[str, str], ...] = (
    ("cjs", "dot see jay ess"),
    ("css", "dot see ess ess"),
    ("csv", "dot see ess vee"),
    ("env", "dot ee en vee"),
    ("gif", "dot gif"),
    ("htm", "dot HTML"),
    ("html", "dot HTML"),
    ("jpeg", "dot jay peg"),
    ("jpg", "dot jay peg"),
    ("js", "dot jay ess"),
    ("json", "dot Jason"),
    ("jsx", "dot jay ess ex"),
    ("md", "dot em dee"),
    ("mjs", "dot em jay ess"),
    ("pdf", "dot pee dee eff"),
    ("png", "dot pee enn gee"),
    ("py", "dot pee why"),
    ("sh", "dot shell"),
    ("sqlite", "dot sequel lite"),
    ("sqlite3", "dot sequel lite three"),
    ("svg", "dot ess vee gee"),
    ("ts", "dot tee ess"),
    ("tsx", "dot tee ess ex"),
    ("txt", "dot tee ex tee"),
    ("webp", "dot web pee"),
    ("yaml", "dot yammel"),
    ("yml", "dot yammel"),
)
_FILE_EXTENSION_LOOKUP = dict(_FILE_EXTENSION_SPEECH)
_FILE_EXTENSION_RE = re.compile(
    r"(?<![A-Za-z0-9])(?P<path>[A-Za-z0-9][A-Za-z0-9_./-]*\.(?P<ext>"
    + "|".join(re.escape(ext) for ext, _spoken in _FILE_EXTENSION_SPEECH)
    + r"))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_BARE_FILE_EXTENSION_RE = re.compile(
    r"(?<![A-Za-z0-9/])\.(?P<ext>"
    + "|".join(re.escape(ext) for ext, _spoken in _FILE_EXTENSION_SPEECH)
    + r")(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_IP_ADDRESS_RE = re.compile(
    r"\b(?P<ip>(?:25[0-5]|2[0-4]\d|1?\d?\d)(?:\.(?:25[0-5]|2[0-4]\d|1?\d?\d)){3})(?::(?P<port>\d{1,5}))?\b"
)
_IP_PATTERN_RE = re.compile(
    r"\b(?P<ip>(?:(?:25[0-5]|2[0-4]\d|1?\d?\d|x)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d|x))\b",
    re.IGNORECASE,
)
_THINK_PAIR_RE = re.compile(r"<think>\s*</think>", re.IGNORECASE)
_THINK_TAG_RE = re.compile(r"</?think>", re.IGNORECASE)
_ETH_PORT_RE = re.compile(r"\beth(?P<number>\d+)\b", re.IGNORECASE)
_PVE_NODE_RE = re.compile(r"\bpve(?P<number>\d+)\b", re.IGNORECASE)
_INFRA_ID_WITH_NUMBER_RE = re.compile(
    r"\b(?P<prefix>PVE|LXC|paths)(?:[\s-]?)(?P<number>\d{1,5})(?:sub(?P<subnumber>\d{1,5}))?\b",
    re.IGNORECASE,
)
_LIVE_BEFORE_TECH_NOUN_RE = re.compile(
    r"\blive(?=\s+(?:local|model|response|test|tests|validation|probe|probes|"
    r"endpoint|route|stack|service|surface|alias|traffic|request|requests|"
    r"run|runs|full|regeneration)\b)",
    re.IGNORECASE,
)
_LIVE_AFTER_TEST_CONTEXT_RE = re.compile(
    r"\b(?P<prefix>(?:validat(?:e|es|ed|ing|ion)|verif(?:y|ies|ied|ying)|test(?:s|ed|ing)?|probe(?:s|d|ing)?|smoke(?:s|d|ing)?|check(?:s|ed|ing)?|cases?)\b(?:(?![.!?]\s).){0,120}?)\blive(?=\s+(?:on|against|in|at|with|via|through)\b)",
    re.IGNORECASE,
)
_OOM_RE = re.compile(r"\boom(?:\s+error)?\b", re.IGNORECASE)
_LITELLM_CLIENT_CHAT_RE = re.compile(r"\bLLMClient\.chat\b", re.IGNORECASE)
_PARENT_DIR_RE = re.compile(r"\.\./")
_COLON_SPEECH_RE = re.compile(r"(?<![A-Za-z0-9]):|:(?!\s)")
_SECRET_CONTEXT_LABEL_PATTERN = (
    r"password|passwd|pwd|secret|token|api[_\s-]?key|private[_\s-]?key|credential|"
    r"virtual[_\s-]?key|master[_\s-]?key|x-api-key|authorization|bearer"
)
_OPENAI_STYLE_SECRET_RE = re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{16,}\b")
_SPOKEN_OPENAI_STYLE_SECRET_RE = re.compile(r"\bsk\s+[A-Za-z0-9][A-Za-z0-9_-]{16,}\b", re.IGNORECASE)
_CONTEXTUAL_SECRET_VALUE_RE = re.compile(
    rf"\b(?P<label>{_SECRET_CONTEXT_LABEL_PATTERN})\b"
    r"(?P<middle>[^.\n]{0,180}?\b(?:is|=|:|Bearer)\s+[`'\"]?)"
    r"(?P<value>(?:sk[-\s]+)?[A-Za-z0-9][A-Za-z0-9_+=.-]{15,})",
    re.IGNORECASE,
)
_BEARER_SECRET_RE = re.compile(
    r"\b(?P<prefix>Bearer\s+)(?P<value>(?:sk[-\s]+)?[A-Za-z0-9][A-Za-z0-9_+=.-]{15,})",
    re.IGNORECASE,
)
_X_API_KEY_SECRET_RE = re.compile(
    r"\b(?P<prefix>X-API-Key\s*:\s*)(?P<value>[A-Za-z0-9][A-Za-z0-9_+=.-]{15,})",
    re.IGNORECASE,
)
_ACRONYM_SPEECH: tuple[tuple[str, str], ...] = (
    ("CI/CD", "see eye, see dee"),
    ("SQLite", "sequel lite"),
    ("pfSense", "pee eff sense"),
    ("IPv4", "eye pee vee four"),
    ("IPv6", "eye pee vee six"),
    ("mTLS", "mTLS"),
    ("VMID", "Virtual Machine eye dee"),
    ("VLAN", "vee lan"),
    ("VXLAN", "vee ex lan"),
    ("WLAN", "double you lan"),
    ("LAN", "lan"),
    ("WAN", "wan"),
    ("VPN", "vee pee enn"),
    ("DNS", "dee enn ess"),
    ("mDNS", "em dee enn ess"),
    ("DHCP", "dee aitch see pee"),
    ("NTP", "enn tee pee"),
    ("TCP", "tee see pee"),
    ("UDP", "you dee pee"),
    ("CIDR", "sigh der"),
    ("NAT", "nat"),
    ("IP", "eye pee"),
    ("MAC", "mack"),
    ("NIC", "enn eye sea"),
    ("MTU", "em tee you"),
    ("HTTP", "aitch tee tee pee"),
    ("HTTPS", "aitch tee tee pee ess"),
    ("SSH", "SSH"),
    ("SSL", "SSL"),
    ("TLS", "TLS"),
    ("URL", "url"),
    ("URI", "you are eye"),
    ("API", "A pee eye"),
    ("REST", "rest"),
    ("JSON", "Jason"),
    ("YAML", "yammel"),
    ("XML", "XML"),
    ("HTML", "HTML"),
    ("CSS", "see ess ess"),
    ("SVG", "ess vee gee"),
    ("PNG", "pee enn gee"),
    ("JPG", "jay peg"),
    ("JPEG", "jay peg"),
    ("GIF", "gee eye eff"),
    ("PDF", "pee dee eff"),
    ("CSV", "see ess vee"),
    ("IIFE", "eye eye eff ee"),
    ("RTX", "are tee ex"),
    ("JS", "JavaScript"),
    ("TS", "tee ess"),
    ("DOM", "dom"),
    ("PWA", "pee double you ay"),
    ("UI", "you eye"),
    ("UX", "you ex"),
    ("GUI", "goo ee"),
    ("CLI", "CLI"),
    ("IDE", "eye dee ee"),
    ("SDK", "ess dee kay"),
    ("MCP", "em see pee"),
    ("CI", "see eye"),
    ("CD", "see dee"),
    ("DB", "dee bee"),
    ("SQL", "sequel"),
    ("ORM", "oh are em"),
    ("CRUD", "crud"),
    ("AI", "A eye"),
    ("LLM", "L-LM"),
    ("ML", "ML"),
    ("NLP", "NLP"),
    ("RAG", "rag"),
    ("TTS", "tee tee ess"),
    ("TOTP", "tee oh tee pee"),
    ("STT", "ess tee tee"),
    ("ASR", "ay ess are"),
    ("OCR", "oh see are"),
    ("CPU", "see pee you"),
    ("GPU", "gee pee you"),
    ("RAM", "ram"),
    ("ROM", "rom"),
    ("ECC", "ee see see"),
    ("LED", "LED"),
    ("OLED", "oh led"),
    ("HDMI", "aitch dee em eye"),
    ("USB", "you ess bee"),
    ("PCIe", "pee see eye ee"),
    ("PCI", "pee see eye"),
    ("NVMe", "enn vee em ee"),
    ("SSD", "ess ess dee"),
    ("HDD", "aitch dee dee"),
    ("PSU", "pee ess you"),
    ("UPS", "you pee ess"),
    ("NAS", "naz"),
    ("NFS", "enn eff ess"),
    ("SMB", "ess em bee"),
    ("ZFS", "zee eff ess"),
    ("LVM", "LVM"),
    ("VM", "vee em"),
    ("KVM", "kay vee em"),
    ("QEMU", "queue em you"),
    ("LXC", "LXC"),
    ("PVE", "PVE"),
    ("VPS", "vee pee ess"),
    ("OS", "oh ess"),
    ("UID", "you eye dee"),
    ("UUID", "you you eye dee"),
    ("GUID", "gee you eye dee"),
    ("ID", "eye dee"),
    ("OK", "okay"),
)
_KNOWN_TERM_SPEECH: tuple[tuple[str, str], ...] = (
    (r"\bfleet\s+CA\b", "fleet Certificate Authority"),
    (r"\bpublic\s+CA\b", "public certificate authority"),
    (r"\bLiteLLM\b", "light LLM"),
    (r"\bpostgres\b", "post gress"),
    (r"\bbyok\b", "Bring Your Own Key"),
    (r"\bz\.ai\b", "zed A eye"),
    (r"\bzai\b", "zed A eye"),
    (r"\bseekdb\b", "seek dee bee"),
    (r"\bcerts\b", "certificates"),
    (r"\bdockge\b", "Dockage"),
    (r"\bxmemory\b", "ex memory"),
    (r"\bpipecat\b", "pipe cat"),
    (r"\blivecat\b", "live cat"),
    (r"\bvllm\b", "V LLM"),
    (r"\ballowlist\b", "allow list"),
    (r"\bblocklist\b", "block list"),
    (r"\bdenylist\b", "deny list"),
    (r"\bsafelist\b", "safe list"),
    (r"\bsubagents\b", "sub-agents"),
    (r"\bsubagent\b", "sub-agent"),
    (r"\bmoe\b", "Mixture of Experts"),
    (r"\bopenclaw\b", "open claw"),
    (r"\bnullclaw\b", "null claw"),
    (r"\bpockettts\b", "pocket TTS"),
    (r"\bplaywright\b", "play wright"),
    (r"\bwebsocket\b", "web socket"),
    (r"\bclonedrepos\b", "cloned repos"),
    (r"\blocalstorage\b", "local storage"),
    (r"\bsessionstorage\b", "session storage"),
    (r"\bvscodium\b", "vee ess code ee um"),
    (r"\bvscode\b", "vee ess code"),
    (r"\bturbovec\b", "turbo veck"),
    (r"\btailscale\b", "tail scale"),
    (r"\btaliscale\b", "tail scale"),
    (r"\bcrawl4ai\b", "crawl for A eye"),
    (r"\bchtp01\b", "chat private zero one"),
    (r"\bliteparse\b", "light parse"),
    (r"\bmarkitdown\b", "mark it down"),
    (r"\bscrapling\b", "scrape ling"),
    (r"\bsearxng\b", "seer ex next generation"),
    (r"\bvikunja\b", "vee coon ee yah"),
    (r"\btextareas\b", "text areas"),
    (r"\btextarea\b", "text area"),
    (r"(?<!\.)\benv\b", "dot ee en vee"),
)
_LEGACY_LETTER_NAME_SPEECH: tuple[tuple[str, str], ...] = (
    (r"\blight\s*L\.L\.M\b", "light LLM"),
    (r"\blite\.L\.M\b", "light LLM"),
    (r"\blight\.LM\b", "light LLM"),
    (r"\blight\s+dot\s+l\s+dot\s+m\b", "light LLM"),
    (r"\blight\s+ell\s+ell\s+em\b", "light LLM"),
    (r"\blight\s+LLM\b", "light LLM"),
    (r"\bvee\s+ell\s+ell\s+em\b", "V LLM"),
    (r"\bV\s+LLM\b", "V LLM"),
    (r"\bL\.L\.M\b", "LLM"),
    (r"\bL\s+dot\s+L\s+dot\s+M\b", "LLM"),
    (r"\bell\s+ell\s+em\b", "LLM"),
    (r"\bLLM\b", "LLM"),
    (r"(?<!-)\bLM\b", "LLM"),
    (r"\bPVee(\d+)\b", r"PVE\1"),
    (r"\bpee\s+vee\s+ee\s+(\d+)\b", r"PVE\1"),
    (r"\bH\s+tee\s+em\s+ell\b", "HTML"),
    (r"\bex\s+em\s+ell\b", "XML"),
    (r"\bem\s+tee\s+ell\s+ess\b", "mTLS"),
    (r"\btee\s+ell\s+ess\b", "TLS"),
    (r"\bess\s+ess\s+ell\b", "SSL"),
    (r"\bsee\s+ell\s+eye\b", "CLI"),
    (r"\bem\s+ell\b", "ML"),
    (r"\benn\s+ell\s+pee\b", "NLP"),
    (r"\bell\s+ee\s+dee\b", "LED"),
    (r"\bell\s+vee\s+em\b", "LVM"),
    (r"\bell\s+ex\s+sea\b", "LXC"),
)


def _normalize_newlines(text: str) -> str:
    return str(text or "").replace("\r\n", "\n").replace("\r", "\n")


def _is_top_backlink_line(line: str) -> bool:
    clean = line.strip()
    if not clean or len(clean) > 220:
        return False
    if not clean.lower().startswith(_BACKLINK_PREFIXES):
        return False
    lowered = clean.lower()
    if "](" in clean or "readme" in lowered:
        return True
    return bool(re.match(r"^(?:<-|←|&larr;)\s+[\w ./_-]{1,160}$", clean, flags=re.IGNORECASE))


def strip_top_backlink_line(text: str) -> str:
    lines = _normalize_newlines(text).split("\n")
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        if not _is_top_backlink_line(line):
            return "\n".join(lines)
        del lines[index]
        while index < len(lines) and not lines[index].strip():
            del lines[index]
        return "\n".join(lines)
    return "\n".join(lines)


def _strip_source_refs(text: str) -> str:
    text = _SOURCE_REF_RE.sub("", text)
    text = re.sub(r"\s+([,;!?])", r"\1", text)
    text = re.sub(r"([([])\s+", r"\1", text)
    text = re.sub(r"\s+([])])", r"\1", text)
    return text


def redact_tts_secret_material(text: str) -> str:
    """Remove secret-looking values before narration text can be cached or spoken."""

    def redacted_value(value: str) -> str:
        trailing = ""
        clean = str(value or "")
        while clean and clean[-1] in ".,;:!?":
            trailing = clean[-1] + trailing
            clean = clean[:-1]
        return f"redacted key{trailing}"

    def redact_contextual(match: re.Match[str]) -> str:
        return f"{match.group('label')}{match.group('middle')}{redacted_value(match.group('value'))}"

    projected = str(text or "")
    projected = _OPENAI_STYLE_SECRET_RE.sub("redacted key", projected)
    projected = _X_API_KEY_SECRET_RE.sub(lambda match: f"{match.group('prefix')}{redacted_value(match.group('value'))}", projected)
    projected = _BEARER_SECRET_RE.sub(lambda match: f"{match.group('prefix')}{redacted_value(match.group('value'))}", projected)
    projected = _CONTEXTUAL_SECRET_VALUE_RE.sub(redact_contextual, projected)

    lines: list[str] = []
    for line in projected.split("\n"):
        if re.search(_SECRET_CONTEXT_LABEL_PATTERN, line, flags=re.IGNORECASE):
            line = _SPOKEN_OPENAI_STYLE_SECRET_RE.sub("redacted key", line)
        lines.append(line)
    return "\n".join(lines)


def _clean_heading_text(value: str) -> str:
    value = re.sub(r"[*_`]+", "", value).strip()
    return value if _TERMINAL_PUNCT_RE.search(value) else f"{value}."


def _project_markdown_heading_lines(text: str) -> str:
    projected: list[str] = []
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        heading_match = _MARKDOWN_HEADING_RE.match(line) or _BOLD_HEADING_RE.match(line)
        if heading_match:
            if projected and projected[-1] != "":
                projected.append("")
            projected.append(_clean_heading_text(heading_match.group("title")))
            projected.append("")
        else:
            projected.append(raw_line)
    return "\n".join(projected)


def _strip_inline_markdown_emphasis(text: str) -> str:
    text = re.sub(r"\*\*([^*\n]+?)\*\*", r"\1", text)
    text = re.sub(r"__([^_\n]+?)__", r"\1", text)
    text = re.sub(r"(?<!\*)\*([^*\n]+?)\*(?!\*)", r"\1", text)
    text = re.sub(r"(?<![A-Za-z0-9_])_([^_\n]+?)_(?![A-Za-z0-9_])", r"\1", text)
    return text


def _transform_outside_fenced_code(text: str, project: Callable[[str], str]) -> str:
    source = str(text or "")
    projected: list[str] = []
    last_end = 0
    for match in _FENCED_CODE_BLOCK_RE.finditer(source):
        projected.append(project(source[last_end : match.start()]))
        projected.append(match.group(0))
        last_end = match.end()
    projected.append(project(source[last_end:]))
    return "".join(projected)


def summarize_fenced_code_blocks(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        lang = match.group("lang").strip().lower()
        label = {
            "html": "HTML",
            "xml": "XML",
            "svg": "SVG",
            "js": "JavaScript",
            "javascript": "JavaScript",
            "ts": "TypeScript",
            "typescript": "TypeScript",
            "py": "Python",
            "python": "Python",
            "bash": "shell",
            "sh": "shell",
            "shell": "shell",
            "json": "JSON",
            "yaml": "YAML",
            "yml": "YAML",
            "css": "CSS",
        }.get(lang, "code")
        article = "an" if label in {"HTML", "XML", "SVG"} else "a"
        return f"\nThere is {article} {label} example here.\n"

    return _FENCED_CODE_BLOCK_RE.sub(replace, text)


def _is_table_row_line(line: str) -> bool:
    stripped = line.strip()
    return stripped.count("|") >= 2 and not stripped.startswith("```")


def _is_table_separator_line(line: str) -> bool:
    return bool(_MARKDOWN_TABLE_SEPARATOR_RE.match(line))


def _clean_table_cell(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(value or ""))
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", cleaned)
    cleaned = re.sub(r"[*_`]+", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -:")
    return cleaned


def _split_table_cells(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [_clean_table_cell(cell) for cell in stripped.split("|")]


def _join_spoken_list(items: list[str]) -> str:
    clean = [item for item in items if item]
    if not clean:
        return ""
    if len(clean) == 1:
        return clean[0]
    if len(clean) == 2:
        return f"{clean[0]} and {clean[1]}"
    return f"{', '.join(clean[:-1])}, and {clean[-1]}"


def _summarize_table_block(lines: list[str]) -> str:
    header_line = next((line for line in lines if _is_table_row_line(line) and not _is_table_separator_line(line)), "")
    header_cells = _split_table_cells(header_line) if header_line else []
    row_count = sum(1 for line in lines if _is_table_row_line(line) and not _is_table_separator_line(line))
    if header_cells and row_count:
        row_count = max(0, row_count - 1)
    row_word = "row" if row_count == 1 else "rows"
    columns = _join_spoken_list(header_cells[:6])
    extra_columns = len(header_cells) - 6
    if extra_columns > 0:
        columns = f"{columns}, plus {extra_columns} more"
    if columns and row_count:
        return f"There is a table with {row_count} {row_word} covering {columns}."
    if columns:
        return f"There is a table covering {columns}."
    if row_count:
        return f"There is a table with {row_count} {row_word}."
    return "There is a table here."


def summarize_markdown_tables(text: str) -> str:
    lines = _normalize_newlines(text).split("\n")
    projected: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        next_line = lines[index + 1] if index + 1 < len(lines) else ""
        if _is_table_row_line(line) and _is_table_separator_line(next_line):
            table_lines = [line, next_line]
            index += 2
            while index < len(lines) and _is_table_row_line(lines[index]):
                table_lines.append(lines[index])
                index += 1
            if projected and projected[-1].strip():
                projected.append("")
            projected.append(_summarize_table_block(table_lines))
            if index < len(lines) and lines[index].strip():
                projected.append("")
            continue
        projected.append(line)
        index += 1
    return "\n".join(projected)


def _summarize_endpoint_list_block(lines: list[str]) -> str:
    methods: list[str] = []
    for line in lines:
        match = _ENDPOINT_LIST_ITEM_RE.match(line)
        if not match:
            continue
        method = match.group("method").upper()
        if method not in methods:
            methods.append(method)
    methods_text = _join_spoken_list(methods)
    endpoint_word = "endpoint" if len(lines) == 1 else "endpoints"
    if methods_text:
        return (
            f"There is an A pee eye endpoint list with {len(lines)} {endpoint_word} "
            f"using {methods_text}. It is summarized here rather than read row by row."
        )
    return f"The A pee eye includes {len(lines)} endpoints."


def summarize_endpoint_list_blocks(text: str) -> str:
    lines = _normalize_newlines(text).split("\n")
    projected: list[str] = []
    index = 0
    while index < len(lines):
        line = lines[index]
        if not _ENDPOINT_LIST_ITEM_RE.match(line):
            projected.append(line)
            index += 1
            continue
        block: list[str] = []
        while index < len(lines) and _ENDPOINT_LIST_ITEM_RE.match(lines[index]):
            block.append(lines[index])
            index += 1
        if len(block) >= 4:
            if projected and projected[-1].strip():
                projected.append("")
            projected.append(_summarize_endpoint_list_block(block))
            if index < len(lines) and lines[index].strip():
                projected.append("")
        else:
            projected.extend(block)
    return "\n".join(projected)


def _speak_inline_code_token(value: str) -> str:
    spoken = _speak_known_attribute_names(value)
    if spoken != value:
        return spoken
    spoken = speak_tts_file_extensions(value)
    if spoken != value:
        return speak_tts_acronyms(spoken)
    return speak_tts_acronyms(_speak_identifier_token(value))


def _strip_inline_code_ticks(text: str) -> str:
    text = _INLINE_CODE_RE.sub(lambda match: _speak_inline_code_token(match.group(1)), text)
    return text.replace("`", "")


def _speak_known_attribute_names(text: str) -> str:
    text = re.sub(
        r"\bdata-fc-([A-Za-z0-9_-]+)\b",
        lambda match: f"data eff sea {_speak_identifier_token(match.group(1))}",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"\bdata-fc\b", "data eff sea", text, flags=re.IGNORECASE)
    return text


def _speak_ip_address(match: re.Match[str]) -> str:
    ip = match.group("ip")
    port = match.group("port")
    spoken = " dot ".join(ip.split("."))
    if port:
        spoken = f"{spoken} colon {port}"
    return spoken


def _speak_ip_pattern(match: re.Match[str]) -> str:
    return " dot ".join("X" if part.lower() == "x" else part for part in match.group("ip").split("."))


_DIGIT_WORDS = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


def _speak_digit_code(match: re.Match[str]) -> str:
    raw_prefix = match.group("prefix")
    prefix = "paths" if raw_prefix.lower() == "paths" else raw_prefix.upper()
    digits = " ".join(_DIGIT_WORDS[digit] for digit in match.group("number"))
    subnumber = match.group("subnumber")
    if subnumber:
        subdigits = " ".join(_DIGIT_WORDS[digit] for digit in subnumber)
        return f"{prefix} {digits} sub {subdigits}"
    return f"{prefix} {digits}"


def _speak_live_pronunciation(text: str) -> str:
    spoken = _LIVE_BEFORE_TECH_NOUN_RE.sub("lithe", str(text or ""))
    return _LIVE_AFTER_TEST_CONTEXT_RE.sub(lambda match: f"{match.group('prefix')}lithe", spoken)


def speak_tts_compound_tokens(text: str) -> str:
    spoken = str(text or "")
    spoken = _THINK_PAIR_RE.sub("think tags", spoken)
    spoken = _THINK_TAG_RE.sub("think tag", spoken)
    spoken = re.sub(r"\.\.\.", " ellipses ", spoken)
    spoken = re.sub(r"https?://", " url ", spoken, flags=re.IGNORECASE)
    spoken = _IP_ADDRESS_RE.sub(_speak_ip_address, spoken)
    spoken = _IP_PATTERN_RE.sub(_speak_ip_pattern, spoken)
    spoken = _LITELLM_CLIENT_CHAT_RE.sub("LLM client dot chat", spoken)
    spoken = re.sub(r"-cli\b", " CLI", spoken, flags=re.IGNORECASE)
    spoken = _PARENT_DIR_RE.sub("parent of ", spoken)
    spoken = re.sub(r"(?<![A-Za-z0-9])\.claude\b", "dot claude", spoken, flags=re.IGNORECASE)
    spoken = re.sub(r"(?<![A-Za-z0-9])\.env\b", "dot ee en vee", spoken, flags=re.IGNORECASE)
    spoken = re.sub(r"(?<![A-Za-z0-9])\.gitignored\b", "dot git ignored", spoken, flags=re.IGNORECASE)
    spoken = re.sub(r"\bgitignored\b", "dot git ignored", spoken, flags=re.IGNORECASE)
    spoken = _ETH_PORT_RE.sub(lambda match: f"network port eff {match.group('number')}", spoken)
    spoken = _PVE_NODE_RE.sub(lambda match: f"PVE{match.group('number')}", spoken)
    spoken = _OOM_RE.sub("Out Of Memory Error", spoken)
    return spoken


def speak_legacy_letter_names(text: str) -> str:
    spoken = str(text or "")
    for pattern, replacement in _LEGACY_LETTER_NAME_SPEECH:
        spoken = re.sub(pattern, replacement, spoken, flags=re.IGNORECASE)
    return spoken


def speak_tts_known_terms(text: str) -> str:
    spoken = str(text or "")
    for pattern, replacement in _KNOWN_TERM_SPEECH:
        spoken = re.sub(pattern, replacement, spoken, flags=re.IGNORECASE)
    return spoken


def _speak_identifier_token(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return token
    return re.sub(r"(?<=[A-Za-z0-9])[-_]+(?=[A-Za-z0-9])", " ", token)


def _speak_file_stem(value: str) -> str:
    parts = str(value or "").strip().strip("/").split("/")
    spoken_parts: list[str] = []
    for part in parts:
        clean = part.strip()
        if not clean:
            continue
        dotted_parts = [_speak_identifier_token(piece) for piece in clean.split(".") if piece]
        spoken_parts.append(" dot ".join(dotted_parts))
    return " slash ".join(spoken_parts)


def speak_tts_file_extensions(text: str) -> str:
    def replace_path(match: re.Match[str]) -> str:
        path = match.group("path")
        ext = match.group("ext").lower()
        extension = _FILE_EXTENSION_LOOKUP.get(ext)
        if not extension:
            return path
        stem = path[: -(len(ext) + 1)]
        spoken_stem = _speak_file_stem(stem)
        return f"{spoken_stem} {extension}" if spoken_stem else extension

    def replace_bare(match: re.Match[str]) -> str:
        extension = _FILE_EXTENSION_LOOKUP.get(match.group("ext").lower(), match.group(0))
        previous = match.string[match.start() - 1] if match.start() > 0 else ""
        return f" {extension}" if previous in {",", ";", ":"} else extension

    spoken = _FILE_EXTENSION_RE.sub(replace_path, str(text or ""))
    return _BARE_FILE_EXTENSION_RE.sub(replace_bare, spoken)


def speak_tts_identifiers(text: str) -> str:
    return _IDENTIFIER_WORD_RE.sub(lambda match: _speak_identifier_token(match.group(0)), text)


def speak_tts_acronyms(text: str) -> str:
    spoken = str(text or "")
    for acronym, replacement in _ACRONYM_SPEECH:
        spoken = re.sub(
            rf"\b{re.escape(acronym)}(?=\d)",
            f"{replacement} ",
            spoken,
            flags=re.IGNORECASE,
        )
        spoken = re.sub(
            rf"\b{re.escape(acronym)}\b",
            replacement,
            spoken,
            flags=re.IGNORECASE,
        )
    spoken = re.sub(r"\bsub\s+agents\b", "sub-agents", spoken, flags=re.IGNORECASE)
    spoken = re.sub(r"\bsub\s+agent\b", "sub-agent", spoken, flags=re.IGNORECASE)
    spoken = _INFRA_ID_WITH_NUMBER_RE.sub(_speak_digit_code, spoken)
    spoken = _speak_live_pronunciation(spoken)
    return re.sub(r"\bL\.L\.M\.?", "L-LM", spoken)


def prepare_tts_markdown_for_llm(markdown: str) -> str:
    """Prepare source Markdown for the narration model without speech transforms.

    Keep this pre-LLM stage boring: normalize newlines and redact secret-like
    material only. Pronunciation, identifier splitting, acronym handling, and
    line-ending pauses belong after the model has produced narration text, just
    before the speech cache is written.
    """
    return redact_tts_secret_material(_normalize_newlines(markdown))


def terminate_tts_line_endings(text: str) -> str:
    """Add a spoken pause to plain-text narration lines that lack punctuation."""
    normalized = _normalize_newlines(text)
    if "\n" not in normalized:
        return normalized.strip()

    lines: list[str] = []
    for raw_line in normalized.split("\n"):
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if not line:
            lines.append("")
            continue
        if _SPEECH_LINE_TERMINAL_RE.search(line):
            lines.append(line)
            continue
        lines.append(re.sub(r"[:;]+$", "", line).rstrip() + ".")
    return "\n".join(lines).strip()


def _normalize_spacing(text: str) -> str:
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def speak_remaining_pipes(text: str) -> str:
    return re.sub(r"\s*\|\s*", " or ", str(text or ""))


def speak_tts_punctuation(text: str) -> str:
    spoken = str(text or "")
    spoken = spoken.replace("@", " at ")
    spoken = spoken.replace("\\", " back slash ")
    spoken = spoken.replace("//", " slash slash ")
    spoken = spoken.replace("/", " slash ")
    spoken = _COLON_SPEECH_RE.sub(" colon ", spoken)
    return spoken


def strip_markdown_list_markers(text: str) -> str:
    return _LIST_MARKER_RE.sub("", str(text or ""))


TTS_TEXT_TRANSFORMS: tuple[TtsTextTransform, ...] = (
    TtsTextTransform("normalize_newlines", _normalize_newlines),
    TtsTextTransform("strip_top_backlink_line", strip_top_backlink_line),
    TtsTextTransform("strip_source_refs", _strip_source_refs),
    TtsTextTransform("redact_tts_secret_material", redact_tts_secret_material),
    TtsTextTransform("project_markdown_headings", _project_markdown_heading_lines),
    TtsTextTransform("summarize_fenced_code_blocks", summarize_fenced_code_blocks),
    TtsTextTransform("summarize_markdown_tables", summarize_markdown_tables),
    TtsTextTransform("summarize_endpoint_list_blocks", summarize_endpoint_list_blocks),
    TtsTextTransform("strip_inline_code_ticks", _strip_inline_code_ticks),
    TtsTextTransform("strip_inline_markdown_emphasis", _strip_inline_markdown_emphasis),
    TtsTextTransform("strip_markdown_list_markers", strip_markdown_list_markers),
    TtsTextTransform("speak_known_attribute_names", _speak_known_attribute_names),
    TtsTextTransform("speak_tts_compound_tokens", speak_tts_compound_tokens),
    TtsTextTransform("speak_legacy_letter_names", speak_legacy_letter_names),
    TtsTextTransform("speak_tts_known_terms", speak_tts_known_terms),
    TtsTextTransform("speak_tts_file_extensions", speak_tts_file_extensions),
    TtsTextTransform("speak_legacy_letter_names_after_file_extensions", speak_legacy_letter_names),
    TtsTextTransform("speak_tts_identifiers", speak_tts_identifiers),
    TtsTextTransform("speak_tts_acronyms", speak_tts_acronyms),
    TtsTextTransform("redact_tts_secret_material", redact_tts_secret_material),
    TtsTextTransform("speak_remaining_pipes", speak_remaining_pipes),
    TtsTextTransform("speak_tts_punctuation", speak_tts_punctuation),
    TtsTextTransform("normalize_spacing", _normalize_spacing),
)


def sanitize_tts_text(
    text: str,
    transforms: Iterable[TtsTextTransform] = TTS_TEXT_TRANSFORMS,
) -> TtsSanitizeResult:
    projected = str(text or "")
    names: list[str] = []
    for transform in transforms:
        projected = transform.project(projected)
        names.append(transform.name)
    return TtsSanitizeResult(text=projected, transforms=tuple(names))
