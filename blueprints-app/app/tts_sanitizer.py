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
    text = re.sub(r"(?<!_)_([^_\n]+?)_(?!_)", r"\1", text)
    return text


def _strip_inline_code_ticks(text: str) -> str:
    text = re.sub(r"`([^`\n]+?)`", r"\1", text)
    return text.replace("`", "")


def _speak_known_attribute_names(text: str) -> str:
    text = re.sub(r"\bdata-fc-key\b", "data eff sea key", text, flags=re.IGNORECASE)
    text = re.sub(r"\bdata-fc\b", "data eff sea", text, flags=re.IGNORECASE)
    return text


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
    TtsTextTransform("strip_inline_markdown_emphasis", _strip_inline_markdown_emphasis),
    TtsTextTransform("strip_inline_code_ticks", _strip_inline_code_ticks),
    TtsTextTransform("speak_known_attribute_names", _speak_known_attribute_names),
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
