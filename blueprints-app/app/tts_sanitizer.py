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
_TERMINAL_PUNCT_RE = re.compile(r"[.!?:;]$")
_BACKLINK_PREFIXES = ("<-", "←", "&larr;", "[<-", "[←", "[&larr;")
_INLINE_CODE_RE = re.compile(r"`([^`\n]+?)`")
_IDENTIFIER_WORD_RE = re.compile(r"\b[A-Za-z][A-Za-z0-9]*(?:[-_][A-Za-z0-9]+)+\b")
_ACRONYM_SPEECH: tuple[tuple[str, str], ...] = (
    ("CI/CD", "see eye, see dee"),
    ("SQLite", "sequel lite"),
    ("pfSense", "pee eff sense"),
    ("IPv4", "eye pee vee four"),
    ("IPv6", "eye pee vee six"),
    ("mTLS", "em tee ell ess"),
    ("VMID", "vee em eye dee"),
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
    ("SSH", "ess ess aitch"),
    ("SSL", "ess ess ell"),
    ("TLS", "tee ell ess"),
    ("URL", "you are ell"),
    ("URI", "you are eye"),
    ("API", "ay pee eye"),
    ("REST", "rest"),
    ("JSON", "jay son"),
    ("YAML", "yammel"),
    ("XML", "ex em ell"),
    ("HTML", "aitch tee em ell"),
    ("CSS", "see ess ess"),
    ("SVG", "ess vee gee"),
    ("PNG", "pee enn gee"),
    ("JPG", "jay peg"),
    ("JPEG", "jay peg"),
    ("GIF", "gee eye eff"),
    ("PDF", "pee dee eff"),
    ("CSV", "see ess vee"),
    ("JS", "jay ess"),
    ("TS", "tee ess"),
    ("DOM", "dee oh em"),
    ("PWA", "pee double you ay"),
    ("UI", "you eye"),
    ("UX", "you ex"),
    ("GUI", "gooey"),
    ("CLI", "see ell eye"),
    ("IDE", "eye dee ee"),
    ("SDK", "ess dee kay"),
    ("CI", "see eye"),
    ("CD", "see dee"),
    ("DB", "dee bee"),
    ("SQL", "sequel"),
    ("ORM", "oh are em"),
    ("CRUD", "crud"),
    ("AI", "ay eye"),
    ("LLM", "ell ell em"),
    ("ML", "em ell"),
    ("NLP", "enn ell pee"),
    ("RAG", "rag"),
    ("TTS", "tee tee ess"),
    ("STT", "ess tee tee"),
    ("ASR", "ay ess are"),
    ("OCR", "oh see are"),
    ("CPU", "see pee you"),
    ("GPU", "gee pee you"),
    ("RAM", "ram"),
    ("ROM", "rom"),
    ("ECC", "ee see see"),
    ("LED", "ell ee dee"),
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
    ("LVM", "ell vee em"),
    ("VM", "vee em"),
    ("KVM", "kay vee em"),
    ("QEMU", "queue em you"),
    ("LXC", "ell ex sea"),
    ("PVE", "pee vee ee"),
    ("VPS", "vee pee ess"),
    ("OS", "oh ess"),
    ("UID", "you eye dee"),
    ("UUID", "you you eye dee"),
    ("GUID", "gee you eye dee"),
    ("ID", "eye dee"),
    ("OK", "okay"),
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
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"([([])\s+", r"\1", text)
    text = re.sub(r"\s+([])])", r"\1", text)
    return text


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


def _speak_inline_code_token(value: str) -> str:
    spoken = _speak_known_attribute_names(value)
    if spoken != value:
        return spoken
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


def _speak_identifier_token(value: str) -> str:
    token = str(value or "").strip()
    if not token:
        return token
    return re.sub(r"(?<=[A-Za-z0-9])[-_]+(?=[A-Za-z0-9])", " ", token)


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
    return spoken


def prepare_tts_markdown_for_llm(markdown: str) -> str:
    text = _normalize_newlines(markdown)
    text = _INLINE_CODE_RE.sub(lambda match: _speak_inline_code_token(match.group(1)), text)
    text = _speak_known_attribute_names(text)
    text = speak_tts_identifiers(text)
    return speak_tts_acronyms(text)


def _normalize_spacing(text: str) -> str:
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


TTS_TEXT_TRANSFORMS: tuple[TtsTextTransform, ...] = (
    TtsTextTransform("normalize_newlines", _normalize_newlines),
    TtsTextTransform("strip_top_backlink_line", strip_top_backlink_line),
    TtsTextTransform("strip_source_refs", _strip_source_refs),
    TtsTextTransform("project_markdown_headings", _project_markdown_heading_lines),
    TtsTextTransform("strip_inline_code_ticks", _strip_inline_code_ticks),
    TtsTextTransform("strip_inline_markdown_emphasis", _strip_inline_markdown_emphasis),
    TtsTextTransform("speak_known_attribute_names", _speak_known_attribute_names),
    TtsTextTransform("speak_tts_identifiers", speak_tts_identifiers),
    TtsTextTransform("speak_tts_acronyms", speak_tts_acronyms),
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
