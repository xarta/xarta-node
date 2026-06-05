"""Deterministic helpers for the planned direct Wake STT Hermes route."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

AUTHORISED_PHRASE = "This command is authorised"
_AUTHORISED_SOURCE_RE = re.compile(
    r"\bthis\s+command\s+is\s+authorised\b[\s.!?]*",
    re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class CommandCode:
    code_id: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class CommandCodeGateResult:
    authorised: bool
    matched_code_id: str
    meat: str
    hermes_text: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "authorised": self.authorised,
            "matched_code_id": self.matched_code_id,
            "meat": self.meat,
            "hermes_text": self.hermes_text,
        }


def _clean_code_id(value: Any) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", "."})
    return clean[:80]


def _clean_alias(value: Any) -> str:
    text = _SPACE_RE.sub(" ", str(value or "").strip())
    return text[:160]


def command_codes_from_config(value: Any) -> list[CommandCode]:
    """Read up to 100 private Command Code entries without exposing aliases."""
    raw_entries = value if isinstance(value, list) else []
    codes: list[CommandCode] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_entries, 1):
        if len(codes) >= 100:
            break
        if not isinstance(raw, dict):
            continue
        code_id = _clean_code_id(raw.get("id") or raw.get("code_id") or f"code_{index}")
        if not code_id or code_id in seen_ids:
            continue
        aliases_raw = raw.get("aliases")
        aliases_list = aliases_raw if isinstance(aliases_raw, list) else []
        aliases = tuple(alias for alias in (_clean_alias(item) for item in aliases_list) if alias)[
            :20
        ]
        if not aliases:
            continue
        seen_ids.add(code_id)
        codes.append(CommandCode(code_id=code_id, aliases=aliases))
    return codes


def _alias_regex(alias: str) -> re.Pattern[str]:
    words = [re.escape(part) for part in re.split(r"[\s\-_]+", alias.strip()) if part]
    if not words:
        return re.compile(r"(?!x)x")
    separator = r"[\s\-_]+"
    return re.compile(
        rf"(?<!\w){separator.join(words)}(?!\w)[\s.!?,;:]*",
        re.IGNORECASE,
    )


def apply_command_code_gate(text: str, codes: list[CommandCode]) -> CommandCodeGateResult:
    """Strip spoken codes/authorisation claims and inject the canonical phrase once.

    Raw Command Code aliases must stay private. Callers should log only the returned
    code id, boolean authorisation state, and redacted text.
    """
    meat = _AUTHORISED_SOURCE_RE.sub(" ", str(text or ""))
    matched_code_id = ""
    for code in codes[:100]:
        for alias in code.aliases:
            pattern = _alias_regex(alias)
            if not pattern.search(meat):
                continue
            meat = pattern.sub(" ", meat)
            matched_code_id = code.code_id
            break
        if matched_code_id:
            break
    meat = _SPACE_RE.sub(" ", meat).strip()
    authorised = bool(matched_code_id)
    hermes_text = meat
    if authorised:
        hermes_text = f"{AUTHORISED_PHRASE}\n\n{meat}".strip()
    return CommandCodeGateResult(
        authorised=authorised,
        matched_code_id=matched_code_id,
        meat=meat,
        hermes_text=hermes_text,
    )


def strip_direct_wake_diagnostic(text: str, codes: list[CommandCode]) -> str:
    """Return Bridge-observable request text without codes or authorisation claims."""
    return apply_command_code_gate(text, codes).meat
