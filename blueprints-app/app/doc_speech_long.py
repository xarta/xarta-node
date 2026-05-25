"""Deterministic long-document sectioning for document speech summaries."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable


@dataclass
class SectionRecord:
    section_id: str
    parent_id: str | None
    title: str
    start: int
    end: int
    heading_depth: int | None
    byte_count: int
    char_count: int
    token_count: int
    text_path: str
    summary_path: str


def source_fingerprint(text: str) -> str:
    return hashlib.sha256(str(text or "").encode("utf-8", "replace")).hexdigest()[:20]


def _line_spans(text: str) -> list[tuple[int, int, str]]:
    spans: list[tuple[int, int, str]] = []
    offset = 0
    for line in str(text or "").splitlines(keepends=True):
        end = offset + len(line)
        spans.append((offset, end, line))
        offset = end
    if offset < len(text):
        spans.append((offset, len(text), text[offset:]))
    return spans


def _heading_for_line(line: str, next_line: str | None = None) -> tuple[int, str] | None:
    atx = re.match(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$", line.rstrip("\n"))
    if atx:
        return len(atx.group(1)), atx.group(2).strip()
    if next_line and line.strip() and re.match(r"^\s{0,3}(=+|-+)\s*$", next_line.rstrip("\n")):
        marker = next_line.strip()[0]
        return (1 if marker == "=" else 2), line.strip()
    return None


def _heading_starts(text: str) -> list[tuple[int, int, str]]:
    spans = _line_spans(text)
    starts: list[tuple[int, int, str]] = []
    in_fence = False
    fence_marker = ""
    for index, (start, _end, line) in enumerate(spans):
        stripped = line.lstrip()
        fence = re.match(r"^(```+|~~~+)", stripped)
        if fence:
            marker = fence.group(1)[:3]
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
                fence_marker = ""
            continue
        if in_fence:
            continue
        next_line = spans[index + 1][2] if index + 1 < len(spans) else None
        heading = _heading_for_line(line, next_line)
        if heading:
            starts.append((start, heading[0], heading[1]))
    return starts


def _paragraph_block_spans(text: str) -> list[tuple[int, int]]:
    spans = _line_spans(text)
    blocks: list[tuple[int, int]] = []
    block_start: int | None = None
    block_end: int | None = None
    in_fence = False
    fence_marker = ""
    for start, end, line in spans:
        stripped = line.lstrip()
        fence = re.match(r"^(```+|~~~+)", stripped)
        if fence:
            marker = fence.group(1)[:3]
            if block_start is None:
                block_start = start
            block_end = end
            if not in_fence:
                in_fence = True
                fence_marker = marker
            elif marker == fence_marker:
                in_fence = False
                fence_marker = ""
                blocks.append((block_start, block_end))
                block_start = None
                block_end = None
            continue
        if in_fence:
            block_end = end
            continue
        if not line.strip():
            if block_start is not None and block_end is not None:
                blocks.append((block_start, block_end))
            block_start = None
            block_end = None
            continue
        if block_start is None:
            block_start = start
        block_end = end
    if block_start is not None and block_end is not None:
        blocks.append((block_start, block_end))
    return blocks


def _write_section(
    *,
    text: str,
    work_dir: Path,
    section_id: str,
    parent_id: str | None,
    title: str,
    start: int,
    end: int,
    heading_depth: int | None,
    count_tokens: Callable[[str], int],
) -> SectionRecord:
    section_text = text[start:end].strip()
    section_dir = work_dir / "sections"
    summary_dir = work_dir / "summaries"
    section_dir.mkdir(parents=True, exist_ok=True)
    summary_dir.mkdir(parents=True, exist_ok=True)
    text_path = section_dir / f"{section_id}.md"
    summary_path = summary_dir / f"{section_id}.txt"
    text_path.write_text(section_text + "\n", encoding="utf-8")
    return SectionRecord(
        section_id=section_id,
        parent_id=parent_id,
        title=title,
        start=start,
        end=end,
        heading_depth=heading_depth,
        byte_count=len(section_text.encode("utf-8", "replace")),
        char_count=len(section_text),
        token_count=count_tokens(section_text),
        text_path=str(text_path),
        summary_path=str(summary_path),
    )


def split_sections(
    text: str,
    *,
    work_dir: Path,
    count_tokens: Callable[[str], int],
    fallback_chunk_tokens: int,
    max_heading_sections: int = 80,
) -> list[SectionRecord]:
    text = str(text or "")
    work_dir.mkdir(parents=True, exist_ok=True)
    headings = _heading_starts(text)
    records: list[SectionRecord] = []
    if headings and len(headings) <= max_heading_sections:
        if headings[0][0] > 0 and text[: headings[0][0]].strip():
            records.append(
                _write_section(
                    text=text,
                    work_dir=work_dir,
                    section_id="section-0000",
                    parent_id=None,
                    title="Opening context",
                    start=0,
                    end=headings[0][0],
                    heading_depth=None,
                    count_tokens=count_tokens,
                )
            )
        for index, (start, depth, title) in enumerate(headings, start=1):
            end = headings[index][0] if index < len(headings) else len(text)
            if not text[start:end].strip():
                continue
            records.append(
                _write_section(
                    text=text,
                    work_dir=work_dir,
                    section_id=f"section-{index:04d}",
                    parent_id=None,
                    title=title or f"Section {index}",
                    start=start,
                    end=end,
                    heading_depth=depth,
                    count_tokens=count_tokens,
                )
            )
    elif headings:
        char_budget = max(1000, int(fallback_chunk_tokens * 2.0))
        spans: list[tuple[int, int, int | None, str]] = []
        if headings[0][0] > 0 and text[: headings[0][0]].strip():
            spans.append((0, headings[0][0], None, "Opening context"))
        for index, (start, depth, title) in enumerate(headings):
            end = headings[index + 1][0] if index + 1 < len(headings) else len(text)
            if text[start:end].strip():
                spans.append((start, end, depth, title or f"Section {index + 1}"))

        group_start: int | None = None
        group_end: int | None = None
        group_titles: list[str] = []
        group_depth: int | None = None
        group_chars = 0
        for start, end, depth, title in spans:
            span_chars = end - start
            if group_start is not None and group_chars + span_chars > char_budget:
                section_index = len(records) + 1
                title_text = group_titles[0] if len(group_titles) == 1 else f"{group_titles[0]} through {group_titles[-1]}"
                records.append(
                    _write_section(
                        text=text,
                        work_dir=work_dir,
                        section_id=f"section-{section_index:04d}",
                        parent_id=None,
                        title=title_text,
                        start=group_start,
                        end=group_end if group_end is not None else start,
                        heading_depth=group_depth,
                        count_tokens=count_tokens,
                    )
                )
                group_start = None
                group_end = None
                group_titles = []
                group_depth = None
                group_chars = 0
            if group_start is None:
                group_start = start
                group_depth = depth
            group_end = end
            if len(group_titles) < 2:
                group_titles.append(title)
            elif len(group_titles) == 2:
                group_titles[1] = title
            group_chars += span_chars
        if group_start is not None and group_end is not None:
            section_index = len(records) + 1
            title_text = group_titles[0] if len(group_titles) == 1 else f"{group_titles[0]} through {group_titles[-1]}"
            records.append(
                _write_section(
                    text=text,
                    work_dir=work_dir,
                    section_id=f"section-{section_index:04d}",
                    parent_id=None,
                    title=title_text,
                    start=group_start,
                    end=group_end,
                    heading_depth=group_depth,
                    count_tokens=count_tokens,
                )
            )
    else:
        records = split_text_to_records(
            text,
            work_dir=work_dir,
            count_tokens=count_tokens,
            max_tokens=max(256, fallback_chunk_tokens),
            parent_id=None,
            title_prefix="Part",
        )

    metadata_path = work_dir / "sections.json"
    metadata_path.write_text(
        json.dumps([asdict(record) for record in records], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return records


def split_text_to_records(
    text: str,
    *,
    work_dir: Path,
    count_tokens: Callable[[str], int],
    max_tokens: int,
    parent_id: str | None,
    title_prefix: str,
) -> list[SectionRecord]:
    text = str(text or "")
    blocks = _paragraph_block_spans(text) or [(0, len(text))]
    records: list[SectionRecord] = []
    group_start: int | None = None
    group_end: int | None = None
    group_text_parts: list[str] = []
    group_tokens = 0
    for block_start, block_end in blocks:
        block_text = text[block_start:block_end]
        block_tokens = count_tokens(block_text)
        if group_start is not None and group_tokens + block_tokens > max_tokens:
            section_id = f"{parent_id or 'chunk'}-{len(records) + 1:04d}"
            records.append(
                _write_section(
                    text=text,
                    work_dir=work_dir,
                    section_id=section_id,
                    parent_id=parent_id,
                    title=f"{title_prefix} {len(records) + 1}",
                    start=group_start,
                    end=group_end if group_end is not None else block_start,
                    heading_depth=None,
                    count_tokens=count_tokens,
                )
            )
            group_start = None
            group_end = None
            group_text_parts = []
            group_tokens = 0
        if group_start is None:
            group_start = block_start
        group_end = block_end
        group_text_parts.append(block_text)
        group_tokens += block_tokens
    if group_start is not None and group_end is not None:
        section_id = f"{parent_id or 'chunk'}-{len(records) + 1:04d}"
        records.append(
            _write_section(
                text=text,
                work_dir=work_dir,
                section_id=section_id,
                parent_id=parent_id,
                title=f"{title_prefix} {len(records) + 1}",
                start=group_start,
                end=group_end,
                heading_depth=None,
                count_tokens=count_tokens,
            )
        )
    return records


def allocate_word_targets(
    sections: list[SectionRecord],
    *,
    target_words: int,
    reserve_words: int = 90,
    floor_words: int = 35,
    cap_ratio: float = 0.28,
) -> dict[str, int]:
    if not sections:
        return {}
    available = max(len(sections) * 10, target_words - max(0, reserve_words))
    total_tokens = sum(max(1, section.token_count) for section in sections)
    cap_words = max(floor_words, int(target_words * cap_ratio))
    allocations: dict[str, int] = {}
    for section in sections:
        proportional = round(available * (max(1, section.token_count) / total_tokens))
        allocations[section.section_id] = max(floor_words, min(cap_words, proportional))
    overflow = sum(allocations.values()) - available
    if overflow <= 0:
        return allocations
    ordered = sorted(sections, key=lambda section: allocations[section.section_id], reverse=True)
    while overflow > 0:
        changed = False
        for section in ordered:
            current = allocations[section.section_id]
            if current <= 10:
                continue
            decrement = min(overflow, max(1, current - 10))
            allocations[section.section_id] = current - decrement
            overflow -= decrement
            changed = True
            if overflow <= 0:
                break
        if not changed:
            break
    return allocations
