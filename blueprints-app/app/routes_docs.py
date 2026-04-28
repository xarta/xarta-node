"""routes_docs.py — CRUD for the docs table + file read/write.

GET    /api/v1/docs                      → list all doc metadata
GET    /api/v1/docs/{doc_id}             → metadata + file content
POST   /api/v1/docs                      → create doc record (creates file if not exists)
PUT    /api/v1/docs/{doc_id}             → update metadata only
PUT    /api/v1/docs/{doc_id}/content     → overwrite file content + touch updated_at
DELETE /api/v1/docs/{doc_id}            → delete record; ?delete_file=true also removes the file
"""

import json
import logging
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import uuid
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import Response

from . import config as cfg
from .db import get_conn, increment_gen
from .models import DocContentBody, DocCreate, DocOut, DocUpdate, DocWithContent
from .nullclaw_docs_search import (
    SynthesisControls,
    blueprints_synthesis_response,
    ensure_succeeded,
    submit_query_synthesis,
    synthesis_display_block,
)
from .sync.queue import enqueue_for_all_peers
from .tts_sanitizer import (
    prepare_tts_markdown_for_llm,
    sanitize_tts_text,
    strip_top_backlink_line,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/docs", tags=["docs"])

_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_DOCS_SENTINEL = _NODE_LOCAL_ROOT / ".docs-pending-commit"
_DEFAULT_TURBOVEC_DB_PATH = (
    _NODE_LOCAL_ROOT / "stacks" / "turbovec-docs" / "data" / "index" / "chunks.sqlite3"
)
_DEFAULT_NULLCLAW_TASKS_DIR = (
    _NODE_LOCAL_ROOT / "stacks" / "nullclaw-docs-search" / "data" / "tasks"
)
_DOC_SPEECH_CACHE_ROOT = _NODE_LOCAL_ROOT / "doc-speech-cache"
_METADATA_BACKLOG_LIMIT = 40
_PATH_LIKE_KEYS = {
    "path",
    "doc_path",
    "viewer_path",
    "register_path",
    "expanded_path",
    "seed_path",
    "from_path",
    "to_path",
    "graph_from_path",
}


class DocsSearchBody(BaseModel):
    query: str = Field(..., min_length=1, max_length=1000)
    mode: str = "hybrid"
    top_k: int = Field(default=8, ge=1, le=30)
    vector_k: int = Field(default=40, ge=1, le=120)
    keyword_k: int = Field(default=40, ge=1, le=120)
    rerank: bool = True
    folder: str | None = Field(default=None, max_length=2000)
    allowed_paths: list[str] = Field(default_factory=list)
    current_only: bool = False
    include_plans: bool = True
    include_research: bool = True
    include_history: bool = False
    include_unknown: bool = True


class DocsSearchExplainBody(SynthesisControls):
    explanation_mode: Literal["summary", "answer"] = "answer"


class DocSpeechBody(BaseModel):
    force: bool = False


class DocsGroupFolderOpenBody(BaseModel):
    group_id: str | None = None


class DocsSearchSyncBody(BaseModel):
    force: bool = False
    paths: list[str] | None = None


class DocsGroupFolderTreeBody(BaseModel):
    group_id: str | None = None
    path: str | None = Field(default=None, max_length=2000)


def _touch_docs_sentinel() -> None:
    """Touch the sentinel file so the lone-wolf commit cron picks up the change."""
    try:
        _DOCS_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
        _DOCS_SENTINEL.touch()
    except Exception as exc:  # non-fatal — backup is best-effort
        log.warning("docs: could not touch sentinel %s: %s", _DOCS_SENTINEL, exc)


def _docs_root() -> Path:
    root = cfg.DOCS_ROOT or cfg.REPO_INNER_PATH
    if not root:
        raise HTTPException(
            503, "DOCS_ROOT (or REPO_INNER_PATH) not configured — cannot locate docs"
        )
    return Path(root)


def _normalize_ownership(root: Path, target: Path) -> None:
    """Hand ownership of created doc paths back to the docs root owner."""
    try:
        owner = root.stat()
    except Exception as exc:
        log.warning("docs: could not stat docs root %s for ownership hand-back: %s", root, exc)
        return

    current = target
    while True:
        try:
            if current.exists():
                os.chown(current, owner.st_uid, owner.st_gid)
        except Exception as exc:
            log.warning("docs: could not normalize ownership on %s: %s", current, exc)
        if current == root or current.parent == current:
            break
        current = current.parent


def _normalize_node_local_ownership(target: Path) -> None:
    _normalize_ownership(_NODE_LOCAL_ROOT, target)


def _safe_resolve(root: Path, rel_path: str) -> Path:
    """Resolve rel_path under root, raising 400 on path traversal."""
    resolved = (root / rel_path).resolve()
    root_resolved = str(root.resolve())
    if str(resolved).startswith(root_resolved + "/") or str(resolved) == root_resolved:
        return resolved
    raise HTTPException(400, "Path escapes docs root")


def _source_timestamp_slug(path: Path) -> str:
    try:
        mtime = path.stat().st_mtime
    except OSError as exc:
        raise HTTPException(404, "doc file not found") from exc
    return datetime.fromtimestamp(mtime).strftime("%Y%m%d-%H%M%S")


def _now_timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _safe_cache_rel_parent(doc_path: str) -> Path:
    clean = _normalize_docs_rel(doc_path)
    rel_parent = Path(clean).parent
    if str(rel_parent) == ".":
        return Path()
    if any(part in {"", ".", ".."} or part.startswith(".") for part in rel_parent.parts):
        raise HTTPException(400, "Invalid document path for speech cache")
    return rel_parent


def _doc_speech_cache_dir_and_name(doc_path: str) -> tuple[Path, str]:
    name = Path(_normalize_docs_rel(doc_path)).name
    if not name.endswith(".md"):
        name = f"{name}.md"
    return _DOC_SPEECH_CACHE_ROOT / _safe_cache_rel_parent(doc_path), name


def _doc_speech_cache_candidates(doc_path: str) -> list[Path]:
    cache_dir, name = _doc_speech_cache_dir_and_name(doc_path)
    if not cache_dir.is_dir():
        return []
    candidates = [p for p in cache_dir.iterdir() if p.is_file() and p.name.endswith(f"--{name}")]
    return sorted(candidates, key=lambda p: (p.stat().st_mtime, p.name), reverse=True)


def _valid_doc_speech_cache_path(doc_path: str, source_path: Path) -> Path | None:
    try:
        source_mtime = source_path.stat().st_mtime
    except OSError as exc:
        raise HTTPException(404, "doc file not found") from exc
    for candidate in _doc_speech_cache_candidates(doc_path):
        try:
            if candidate.stat().st_mtime >= source_mtime:
                return candidate
        except OSError:
            continue
    return None


def _new_doc_speech_cache_path(doc_path: str) -> Path:
    cache_dir, name = _doc_speech_cache_dir_and_name(doc_path)
    base = cache_dir / f"{_now_timestamp_slug()}--{name}"
    if not base.exists():
        return base
    for index in range(1, 100):
        candidate = cache_dir / f"{_now_timestamp_slug()}-{index:02d}--{name}"
        if not candidate.exists():
            return candidate
    raise HTTPException(500, "Could not allocate a unique speech cache path")


def _invalidate_doc_speech_cache(doc_path: str | None) -> None:
    if not doc_path:
        return
    try:
        for cached in _doc_speech_cache_candidates(doc_path):
            try:
                cached.unlink()
            except OSError as exc:
                log.warning("docs: could not remove stale speech cache %s: %s", cached, exc)
    except Exception as exc:
        log.warning("docs: speech cache invalidation failed for %s: %s", doc_path, exc)


def _strip_frontmatter(markdown: str) -> str:
    text = str(markdown or "").replace("\r\n", "\n").replace("\r", "\n")
    start = 1 if text.startswith("\ufeff") else 0
    if text[start : start + 3] != "---":
        return text
    first_line_end = text.find("\n", start)
    first_line = text[start : len(text) if first_line_end == -1 else first_line_end].strip()
    if first_line != "---":
        return text
    line_start = len(text) if first_line_end == -1 else first_line_end + 1
    while line_start < len(text):
        line_end = text.find("\n", line_start)
        if line_end == -1:
            line_end = len(text)
        if text[line_start:line_end].strip() == "---":
            return text[line_end + 1 :].lstrip()
        line_start = line_end + 1
    return text


def _clamp_source_markdown(markdown: str, limit: int = 28000) -> str:
    text = str(markdown or "")
    if len(text) <= limit:
        return text
    clipped = text[:limit].rsplit("\n## ", 1)[0].strip()
    if len(clipped) < limit * 0.5:
        clipped = text[:limit].rsplit("\n", 1)[0].strip()
    return (
        clipped
        + "\n\n[Document continues beyond this prompt window. Preserve the visible details, "
        "then say that the remaining lower sections should be read from the page.]"
    )


def _clean_doc_speech_markdown(text: str) -> str:
    cleaned = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    cleaned = _strip_frontmatter(cleaned)
    return sanitize_tts_text(cleaned).text


_DOC_SPEECH_SYSTEM_PROMPT = """
You write narration scripts for local documentation pages.

Convert the supplied Markdown document into a plain text narration script for TTS.

Rules:
- Preserve the document's real details, statuses, warnings, dates, names, paths, commands, and relationships.
- Output plain text only. Do not use Markdown headings, bullets, numbered lists, bold markers, code fences, tables, YAML front matter, or link syntax.
- Do not recite raw Markdown syntax, table pipes, link URLs, or every repetitive table cell.
- For links, say their human meaning, such as "link to the responsive header notes".
- For tables, read every row for understanding, then output only a prose summary. Never output Markdown table pipes or row-by-row table text.
- For endpoint tables or method lists, summarize the A pee eye surface in prose. Mention the main capabilities, not every GET, POST, PUT, or DELETE row.
- For file lists, summarize the implementation areas in prose. Mention important files only when they explain the architecture.
- For code or commands, mention the command or path only when it is important. Keep punctuation speakable.
- Preserve fenced code blocks as examples; summarize what they illustrate instead of reading raw tags, attributes, or source lines.
- For inline code identifiers, prefer speech-friendly words: form_controls becomes "form controls"; data-fc-key becomes "data eff sea key".
- Spell important acronyms phonetically where it helps narration: LXC becomes "ell ex sea"; SVG becomes "ess vee gee"; AI becomes "ay eye".
- Use short paragraph breaks for pacing. If a section title helps, write it as a plain sentence with a full stop.
- Do not add citations, source labels, or commentary about being an AI.
- Output only the narration text.
""".strip()


def _strip_think_blocks(text: str) -> str:
    return re.sub(r"\s*<think>.*?</think>\s*", "", str(text or ""), flags=re.DOTALL).strip()


async def _complete_doc_speech_local(messages: list[dict[str, str]]) -> str:
    base_url = (os.environ.get("LITELLM_BASE_URL") or "").strip().rstrip("/")
    api_key = (os.environ.get("LITELLM_API_KEY") or "").strip()
    model = (os.environ.get("DOC_SPEECH_LLM_MODEL") or "").strip()
    if not base_url:
        raise HTTPException(503, "LITELLM_BASE_URL is not configured for local doc narration")
    if not api_key:
        raise HTTPException(503, "LITELLM_API_KEY is not configured for local doc narration")
    if not model:
        raise HTTPException(503, "DOC_SPEECH_LLM_MODEL is not configured for local doc narration")

    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": 4200,
    }
    headers = {"Authorization": f"Bearer {api_key}"}
    timeout = float(os.environ.get("DOC_SPEECH_LLM_TIMEOUT", "75"))
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            resp = await client.post(f"{base_url}/v1/chat/completions", headers=headers, json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "Local LLM narration generation timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"Local LLM narration endpoint unavailable: {exc}") from exc
    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"Local LLM narration generation failed: {detail}")
    try:
        data = resp.json()
        answer = data["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(502, "Local LLM returned an invalid narration response") from exc
    return _strip_think_blocks(str(answer or ""))


async def _generate_doc_speech_markdown(doc: Any, source_markdown: str) -> str:
    title = str(doc["label"] or Path(doc["path"]).stem.replace("-", " ").replace("_", " ")).strip()
    description = str(doc["description"] or "").strip()
    doc_path = str(doc["path"] or "").strip()
    speech_source = _clamp_source_markdown(
        prepare_tts_markdown_for_llm(strip_top_backlink_line(_strip_frontmatter(source_markdown)))
    )
    user_prompt = (
        "/no-think\n"
        f"Document title: {title}\n"
        f"Document path: {doc_path}\n"
        f"Description: {description or 'None'}\n\n"
        "Rewrite this document as a plain text narrated version for TTS playback:\n\n"
        f"{speech_source}"
    )
    answer = await _complete_doc_speech_local(
        [
            {"role": "system", "content": _DOC_SPEECH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
    )
    speech = _clean_doc_speech_markdown(str(answer or ""))
    if not speech:
        raise HTTPException(502, "Local LLM returned an empty narration")
    return speech


def _row_to_out(row) -> DocOut:
    cols = row.keys()
    return DocOut(
        doc_id=row["doc_id"],
        label=row["label"],
        description=row["description"],
        tags=row["tags"],
        path=row["path"],
        sort_order=row["sort_order"] if row["sort_order"] is not None else 0,
        group_id=row["group_id"] if "group_id" in cols else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _doc_path_candidates(doc_path: str) -> list[str]:
    clean = (doc_path or "").strip().lstrip("/")
    if not clean:
        return []
    candidates = [clean]
    if not clean.startswith("docs/"):
        candidates.append(f"docs/{clean}")
    else:
        candidates.append(clean.removeprefix("docs/"))
    seen: set[str] = set()
    ordered: list[str] = []
    for item in candidates:
        key = item.lower()
        if key and key not in seen:
            seen.add(key)
            ordered.append(item)
    return ordered


def _docs_folder_opener() -> list[str]:
    configured = os.environ.get("BLUEPRINTS_DOCS_FOLDER_OPEN_CMD", "").strip()
    if configured:
        return shlex.split(configured)
    for candidate in ("xdg-open", "gio", "open"):
        found = shutil.which(candidate)
        if not found:
            continue
        if candidate == "gio":
            return [found, "open"]
        return [found]
    return []


def _open_docs_folder(folder: Path) -> None:
    cmd = _docs_folder_opener()
    if not cmd:
        raise HTTPException(
            503,
            "No folder opener is available; set BLUEPRINTS_DOCS_FOLDER_OPEN_CMD",
        )
    try:
        proc = subprocess.Popen(
            [*cmd, str(folder)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            _, stderr = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            return
        if proc.returncode != 0:
            detail = (stderr or "").strip() or f"exit code {proc.returncode}"
            raise HTTPException(500, f"Could not open folder: {detail[:300]}")
    except OSError as exc:
        raise HTTPException(500, f"Could not open folder: {exc}") from exc


def _result_snippet(text: str, limit: int = 620) -> str:
    snippet = " ".join((text or "").split())
    if len(snippet) <= limit:
        return snippet
    return snippet[: limit - 1].rstrip() + "…"


def _docs_search_chunk_limit(document_count: int) -> int:
    """Fetch a wider chunk set so the UI can group by document."""
    doc_count = max(1, min(30, int(document_count or 8)))
    return min(120, max(doc_count * 5, doc_count + 20))


def _docs_search_allowed_paths(folder: str | None, allowed_paths: list[str] | None) -> list[str]:
    raw_paths = [*(allowed_paths or [])]
    if folder:
        raw_paths.append(folder)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in raw_paths:
        value = str(raw or "").strip().replace("\\", "/").lstrip("/")
        if not value or value in {".", ".."}:
            continue
        if value == "docs" or value.startswith("docs/"):
            value = value.removeprefix("docs").lstrip("/")
            if not value:
                continue
        parts = Path(value).parts
        if any(part in {"", ".", ".."} or part.startswith(".") for part in parts):
            continue
        if not value.endswith(".md") and not value.endswith("/"):
            value = f"{value.rstrip('/')}/"
        if value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _registered_docs_by_path() -> dict[str, Any]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM docs").fetchall()
    by_path: dict[str, Any] = {}
    for row in rows:
        path = (row["path"] or "").strip()
        if not path:
            continue
        by_path[path.lower()] = row
        if path.startswith("docs/"):
            by_path[path.removeprefix("docs/").lower()] = row
    return by_path


def _normalize_docs_rel(path: str) -> str:
    clean = (path or "").strip().replace("\\", "/").strip("/")
    return "." if clean in ("", ".") else clean


def _docs_rel_path(root: Path, path: Path) -> str:
    try:
        rel = path.resolve().relative_to(root.resolve())
    except ValueError:
        return str(path)
    rel_text = rel.as_posix()
    return "." if rel_text in ("", ".") else rel_text


def _docs_group_path_rows(group_id: str | None) -> list[Any]:
    if group_id:
        query = """
            SELECT path
            FROM docs
            WHERE group_id=?
            ORDER BY sort_order, label
        """
        params: tuple[Any, ...] = (group_id,)
    else:
        query = """
            SELECT path
            FROM docs
            WHERE group_id IS NULL OR group_id=''
            ORDER BY sort_order, label
        """
        params = ()

    with get_conn() as conn:
        return conn.execute(query, params).fetchall()


def _most_common_docs_folder(root: Path, rows: list[Any]) -> tuple[Path, Counter[Path]]:
    if not rows:
        raise HTTPException(404, "No documents in this group")

    folders: list[Path] = []
    first_seen: dict[Path, int] = {}
    for row in rows:
        doc_path = str(row["path"] or "").strip()
        if not doc_path:
            continue
        resolved = _safe_resolve(root, doc_path)
        folder = resolved if resolved.is_dir() else resolved.parent
        if folder not in first_seen:
            first_seen[folder] = len(folders)
        folders.append(folder)

    if not folders:
        raise HTTPException(404, "No document paths in this group")

    counts = Counter(folders)
    folder = max(counts, key=lambda item: (counts[item], -first_seen[item]))
    if not folder.exists():
        raise HTTPException(404, f"Folder does not exist: {folder}")
    if not folder.is_dir():
        raise HTTPException(400, f"Not a folder: {folder}")
    return folder, counts


def _registered_docs_tree_lookup() -> dict[str, dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT doc_id, label, description, tags, path, group_id
            FROM docs
            ORDER BY sort_order, label
            """
        ).fetchall()

    by_path: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _normalize_docs_rel(row["path"])
        lower_key = key.lower()
        if not key or lower_key in by_path:
            continue
        by_path[lower_key] = {
            "doc_id": row["doc_id"],
            "label": row["label"],
            "description": row["description"],
            "tags": row["tags"],
            "path": row["path"],
            "group_id": row["group_id"] if "group_id" in row.keys() else None,
        }
    return by_path


def _count_docs_files(root: Path) -> int:
    docs_root = root / "docs" if (root / "docs").is_dir() else root
    count = 0
    for path in docs_root.rglob("*.md"):
        try:
            rel_parts = path.relative_to(docs_root).parts
        except ValueError:
            continue
        if any(part.startswith(".") for part in rel_parts[:-1]):
            continue
        count += 1
    return count


async def _docs_status_get_json(
    client: httpx.AsyncClient, url: str
) -> tuple[bool, dict[str, Any] | None, str | None]:
    try:
        resp = await client.get(url)
        if resp.status_code >= 400:
            return False, None, f"HTTP {resp.status_code}"
        data = resp.json()
    except httpx.TimeoutException:
        return False, None, "timeout"
    except httpx.RequestError as exc:
        return False, None, str(exc)
    except ValueError:
        return False, None, "invalid JSON"
    if not isinstance(data, dict):
        return False, None, "non-object response"
    return True, data, None


async def _docs_status_model_ids(
    client: httpx.AsyncClient, base_url: str | None
) -> tuple[bool, set[str], str | None]:
    if not base_url:
        return False, set(), "not configured"
    headers: dict[str, str] = {}
    api_key = os.environ.get("LITELLM_API_KEY", "").strip()
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        resp = await client.get(f"{base_url.rstrip('/')}/v1/models", headers=headers)
        if resp.status_code >= 400:
            return False, set(), f"HTTP {resp.status_code}"
        data = resp.json()
    except httpx.TimeoutException:
        return False, set(), "timeout"
    except httpx.RequestError as exc:
        return False, set(), str(exc)
    except ValueError:
        return False, set(), "invalid JSON"
    models = data.get("data") if isinstance(data, dict) else []
    ids = {str(item.get("id")) for item in models if isinstance(item, dict) and item.get("id")}
    if not ids:
        return False, set(), "empty model list"
    return True, ids, None


def _docs_status_check(
    name: str,
    label: str,
    ok: bool,
    detail: str,
    *,
    critical: bool = False,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "ok": bool(ok),
        "status": "ok" if ok else ("fail" if critical else "warn"),
        "critical": bool(critical),
        "detail": detail,
        "meta": meta or {},
    }


def _traffic_status(has_critical_failure: bool, has_warning: bool) -> str:
    if has_critical_failure:
        return "red"
    if has_warning:
        return "amber"
    return "green"


def _traffic_summary(status: str, *, subject: str) -> str:
    summaries = {
        "health": {
            "green": "Docs search runtime is healthy.",
            "amber": "Docs search runtime is usable, with non-critical warnings.",
            "red": "Docs search runtime has a critical failure.",
        },
        "quality": {
            "green": "Docs corpus quality is healthy.",
            "amber": "Docs corpus quality has review backlog.",
            "red": "Docs corpus quality could not be assessed.",
        },
    }
    return summaries.get(subject, summaries["health"]).get(status, "Status unknown.")


def _normalize_index_doc_path(path: str | None) -> str:
    value = str(path or "").strip().replace("\\", "/").lstrip("/")
    if value == "docs" or value.startswith("docs/"):
        value = value.removeprefix("docs").lstrip("/")
    return value.strip("/")


def _turbovec_db_path() -> Path:
    configured = os.environ.get("TURBOVEC_DOCS_DB_PATH", "").strip()
    return Path(configured) if configured else _DEFAULT_TURBOVEC_DB_PATH


def _nullclaw_tasks_dir() -> Path:
    configured = os.environ.get("NULLCLAW_DOCS_SEARCH_TASKS_DIR", "").strip()
    return Path(configured) if configured else _DEFAULT_NULLCLAW_TASKS_DIR


def _collect_known_doc_paths(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT DISTINCT doc_path FROM doc_lifecycle").fetchall()
    return {path for row in rows if (path := _normalize_index_doc_path(row["doc_path"]))}


def _collect_paths_from_json(value: Any, known_paths: set[str], found: set[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key in _PATH_LIKE_KEYS and isinstance(child, str):
                path = _normalize_index_doc_path(child)
                if path in known_paths:
                    found.add(path)
            else:
                _collect_paths_from_json(child, known_paths, found)
    elif isinstance(value, list):
        for child in value:
            _collect_paths_from_json(child, known_paths, found)


def _retrieval_counts_from_tasks(
    tasks_dir: Path, known_paths: set[str]
) -> tuple[Counter[str], dict[str, Any]]:
    counts: Counter[str] = Counter()
    scanned = 0
    unreadable = 0
    if not tasks_dir.is_dir():
        return counts, {"tasks_dir": str(tasks_dir), "scanned_tasks": 0, "unreadable_tasks": 0}
    for path in sorted(tasks_dir.glob("task_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            unreadable += 1
            continue
        scanned += 1
        found: set[str] = set()
        _collect_paths_from_json(
            data.get("result") if isinstance(data, dict) else data, known_paths, found
        )
        for doc_path in found:
            counts[doc_path] += 1
    return counts, {
        "tasks_dir": str(tasks_dir),
        "scanned_tasks": scanned,
        "unreadable_tasks": unreadable,
    }


def _metadata_backlog_report(limit: int = _METADATA_BACKLOG_LIMIT) -> dict[str, Any]:
    db_path = _turbovec_db_path()
    if not db_path.is_file():
        return {
            "ok": False,
            "status": "red",
            "summary": "TurboVec metadata database is unavailable.",
            "metrics": {"unknown_lifecycle_source_docs": None},
            "items": [],
            "error": f"not found: {db_path}",
        }

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        known_paths = _collect_known_doc_paths(conn)
        retrieval_counts, retrieval_meta = _retrieval_counts_from_tasks(
            _nullclaw_tasks_dir(), known_paths
        )
        folder_rows = conn.execute(
            """
            SELECT substr(doc_path, 1, instr(doc_path || '/', '/') - 1) AS folder,
                   COUNT(DISTINCT doc_path) AS docs,
                   COUNT(*) AS chunks
            FROM chunks
            GROUP BY folder
            ORDER BY chunks DESC, folder ASC
            """
        ).fetchall()
        folder_count = max(len(folder_rows), 1)
        folder_importance = {
            row["folder"]: {
                "rank": index + 1,
                "score": folder_count - index,
                "docs": int(row["docs"] or 0),
                "chunks": int(row["chunks"] or 0),
            }
            for index, row in enumerate(folder_rows)
        }
        inbound_rows = conn.execute(
            """
            SELECT to_path AS doc_path, COUNT(*) AS inbound
            FROM doc_edges
            GROUP BY to_path
            """
        ).fetchall()
        inbound_counts = {
            _normalize_index_doc_path(row["doc_path"]): int(row["inbound"] or 0)
            for row in inbound_rows
        }
        rows = conn.execute(
            """
            SELECT dl.doc_path, dl.lifecycle, dl.source_type, dl.authority,
                   dl.confidence_band, dl.verified_at, dl.freshness_risk,
                   COALESCE(node.title, '') AS title,
                   COALESCE(node.heading_count, 0) AS heading_count,
                   COUNT(DISTINCT chunks.handle) AS chunks
            FROM doc_lifecycle AS dl
            LEFT JOIN doc_nodes AS node ON node.doc_path = dl.doc_path
            LEFT JOIN chunks ON chunks.doc_path = dl.doc_path
            WHERE dl.lifecycle = 'unknown' AND dl.source_type = 'unknown'
            GROUP BY dl.doc_path
            """
        ).fetchall()
    finally:
        conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        doc_path = _normalize_index_doc_path(row["doc_path"])
        folder = doc_path.split("/", 1)[0] if "/" in doc_path else doc_path
        folder_meta = folder_importance.get(
            folder,
            {"rank": folder_count + 1, "score": 0, "docs": 0, "chunks": 0},
        )
        retrieval_count = int(retrieval_counts.get(doc_path, 0))
        inbound = int(inbound_counts.get(doc_path, 0))
        importance = int(folder_meta["score"])
        priority_score = retrieval_count * 1000 + inbound * 10 + importance
        items.append(
            {
                "path": doc_path,
                "title": row["title"]
                or Path(doc_path).stem.replace("-", " ").replace("_", " ").title(),
                "folder": folder,
                "priority_score": priority_score,
                "retrieval_frequency": retrieval_count,
                "inbound_graph_links": inbound,
                "folder_importance": importance,
                "folder_rank": folder_meta["rank"],
                "folder_docs": folder_meta["docs"],
                "folder_chunks": folder_meta["chunks"],
                "chunk_count": int(row["chunks"] or 0),
                "heading_count": int(row["heading_count"] or 0),
                "lifecycle": row["lifecycle"],
                "source_type": row["source_type"],
                "authority": row["authority"],
                "confidence_band": row["confidence_band"],
                "verified_at": row["verified_at"],
                "freshness_risk": row["freshness_risk"],
            }
        )
    items.sort(
        key=lambda item: (
            -int(item["priority_score"]),
            -int(item["retrieval_frequency"]),
            -int(item["inbound_graph_links"]),
            -int(item["folder_importance"]),
            item["path"],
        )
    )
    unknown_count = len(items)
    return {
        "ok": True,
        "status": "green" if unknown_count == 0 else "amber",
        "summary": (
            "All indexed docs have lifecycle/source metadata."
            if unknown_count == 0
            else f"{unknown_count} indexed docs still need lifecycle/source metadata review."
        ),
        "ordering": [
            "retrieval_frequency",
            "inbound_graph_links",
            "folder_importance",
        ],
        "metrics": {
            "unknown_lifecycle_source_docs": unknown_count,
            "backlog_items_returned": min(limit, unknown_count),
            "retrieval_tasks_scanned": retrieval_meta["scanned_tasks"],
            "retrieval_task_read_errors": retrieval_meta["unreadable_tasks"],
            "folders_ranked": len(folder_importance),
        },
        "items": items[: max(0, limit)],
        "sources": {
            "turbovec_db_path": str(db_path),
            **retrieval_meta,
        },
    }


def _docs_status_model_check(
    name: str,
    label: str,
    model: str | None,
    model_ids: set[str],
    models_ok: bool,
    models_error: str | None,
    *,
    critical: bool = False,
) -> dict[str, Any]:
    model_name = str(model or "").strip()
    ok = bool(model_name and models_ok and model_name in model_ids)
    if ok:
        detail = f"{model_name} listed by LiteLLM"
    elif not model_name:
        detail = "No model alias configured"
    elif not models_ok:
        detail = f"{model_name}; model list unavailable: {models_error or 'unknown error'}"
    else:
        detail = f"{model_name} is not listed by LiteLLM"
    return _docs_status_check(
        name,
        label,
        ok,
        detail,
        critical=critical,
        meta={
            "model": model_name or None,
            "model_list_available": models_ok,
            "model_list_error": models_error,
        },
    )


def _enrich_search_result(
    raw: dict[str, Any], docs_by_path: dict[str, Any], root: Path
) -> dict[str, Any]:
    doc_path = str(raw.get("doc_path") or "").strip().lstrip("/")
    candidates = _doc_path_candidates(doc_path)
    row = next((docs_by_path.get(p.lower()) for p in candidates if p.lower() in docs_by_path), None)
    registered_path = row["path"] if row else None
    file_path = registered_path or (
        candidates[1] if len(candidates) > 1 else (candidates[0] if candidates else "")
    )

    file_exists = False
    if file_path:
        try:
            file_exists = _safe_resolve(root, file_path).is_file()
        except HTTPException:
            file_exists = False

    doc_registered = row is not None
    openable = bool(doc_registered and file_exists)
    if openable:
        register_hint = "registered"
    elif file_exists:
        register_hint = "add_to_docs_viewer"
    else:
        register_hint = "stale_index"

    title = raw.get("title") or ""
    if row:
        title = row["label"] or title
    if not title and doc_path:
        title = Path(doc_path).stem.replace("-", " ").replace("_", " ").title()

    return {
        "doc_path": doc_path,
        "viewer_path": registered_path,
        "register_path": None if doc_registered else file_path,
        "title": title,
        "chunk_index": raw.get("chunk_index"),
        "snippet": _result_snippet(str(raw.get("text") or "")),
        "score": raw.get("score"),
        "rerank_score": raw.get("rerank_score"),
        "doc_registered": doc_registered,
        "doc_id": row["doc_id"] if row else None,
        "doc_group_id": row["group_id"] if row and "group_id" in row.keys() else None,
        "file_exists": file_exists,
        "openable": openable,
        "register_hint": register_hint,
        "match_sources": raw.get("match_sources")
        or (["vector"] if raw.get("vector_rank") is not None else []),
        "vector_rank": raw.get("vector_rank"),
        "vector_score": raw.get("vector_score"),
        "keyword_rank": raw.get("keyword_rank"),
        "keyword_score": raw.get("keyword_score"),
        "rrf_score": raw.get("rrf_score"),
        "keyword_terms": raw.get("keyword_terms") or [],
        "updated_at": raw.get("updated_at"),
        "handle": raw.get("handle"),
        "lifecycle": raw.get("lifecycle") or "unknown",
        "source_type": raw.get("source_type") or "unknown",
        "authority": raw.get("authority") or "unknown",
        "confidence_band": raw.get("confidence_band") or "unknown",
        "verified_at": raw.get("verified_at"),
        "freshness_risk": raw.get("freshness_risk") or "unknown",
        "lifecycle_metadata": raw.get("lifecycle_metadata")
        if isinstance(raw.get("lifecycle_metadata"), dict)
        else {},
    }


# ── List ──────────────────────────────────────────────────────────────────────


@router.get("", response_model=list[DocOut])
async def list_docs() -> list[DocOut]:
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM docs ORDER BY sort_order, label").fetchall()
    return [_row_to_out(r) for r in rows]


# ── List unregistered files ───────────────────────────────────────────────────


@router.get("/unregistered", response_model=list[str])
async def list_unregistered_docs() -> list[str]:
    """Return relative paths of .md files inside DOCS_ROOT not yet in the docs table."""
    root = _docs_root()
    with get_conn() as conn:
        rows = conn.execute("SELECT path FROM docs").fetchall()
    registered = {row["path"] for row in rows}
    unregistered: list[str] = []
    for p in sorted(root.rglob("*.md")):
        parts = p.relative_to(root).parts
        # Skip anything inside hidden directories (e.g. .git)
        if any(part.startswith(".") for part in parts[:-1]):
            continue
        rel = "/".join(parts)
        if rel not in registered:
            unregistered.append(rel)
    return unregistered


# ── Search proxy ──────────────────────────────────────────────────────────────


@router.post("/search", response_model=dict)
async def search_docs(body: DocsSearchBody) -> dict:
    """Proxy node-local TurboVec Docs search and enrich results for the viewer."""
    mode = (body.mode or "hybrid").strip().lower()
    if mode not in {"vector", "hybrid", "keyword"}:
        raise HTTPException(400, "mode must be one of: vector, hybrid, keyword")

    base_url = cfg.TURBOVEC_DOCS_URL.rstrip("/")
    if not base_url:
        raise HTTPException(503, "TURBOVEC_DOCS_URL is not configured")

    chunk_limit = _docs_search_chunk_limit(body.top_k)
    allowed_paths = _docs_search_allowed_paths(body.folder, body.allowed_paths)
    scope_payload: dict[str, Any] = {
        "allowed_paths": allowed_paths,
        "current_only": body.current_only,
        "include_plans": body.include_plans,
        "include_research": body.include_research,
        "include_history": body.include_history,
        "include_unknown": body.include_unknown,
    }
    if mode == "vector":
        endpoint = "/query"
        payload: dict[str, Any] = {
            "query": body.query,
            "top_k": chunk_limit,
            "candidate_k": max(body.vector_k, chunk_limit),
            "rerank": body.rerank,
            **scope_payload,
        }
    else:
        endpoint = "/hybrid-query"
        payload = {
            "query": body.query,
            "top_k": chunk_limit,
            "vector_k": max(body.vector_k, chunk_limit),
            "keyword_k": max(body.keyword_k, chunk_limit),
            "rerank": body.rerank,
            "mode": mode,
            **scope_payload,
        }

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(cfg.TURBOVEC_DOCS_TIMEOUT)) as client:
            resp = await client.post(f"{base_url}{endpoint}", json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "TurboVec Docs search timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"TurboVec Docs unavailable: {exc}") from exc

    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"TurboVec Docs search failed: {detail}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise HTTPException(502, "TurboVec Docs returned invalid JSON") from exc

    docs_by_path = _registered_docs_by_path()
    root = _docs_root()
    raw_results = data.get("results") if isinstance(data, dict) else []
    if not isinstance(raw_results, list):
        raw_results = []

    results = [
        _enrich_search_result(r, docs_by_path, root) for r in raw_results if isinstance(r, dict)
    ]
    unique_documents = {
        str(
            r.get("doc_id")
            or r.get("viewer_path")
            or r.get("register_path")
            or r.get("doc_path")
            or ""
        ).lower()
        for r in results
    }
    unique_documents.discard("")
    return {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "mode": mode,
        "query": body.query,
        "rerank": body.rerank,
        "document_target": body.top_k,
        "chunk_candidate_limit": chunk_limit,
        "scope": {
            "folder": body.folder,
            "allowed_paths": allowed_paths,
            "current_only": body.current_only,
            "include_plans": body.include_plans,
            "include_research": body.include_research,
            "include_history": body.include_history,
            "include_unknown": body.include_unknown,
        },
        "document_count": len(unique_documents),
        "result_count": len(results),
        "results": results,
        "upstream": {
            "endpoint": endpoint,
            "url": base_url,
            "result_count": len(raw_results),
        },
    }


@router.post("/search/explain", response_model=dict)
async def explain_docs_search(body: DocsSearchExplainBody) -> dict[str, Any]:
    """Return a grounded synthesis for a docs search query via nullclaw-docs-search."""
    task = await submit_query_synthesis(body, body.explanation_mode)
    ensure_succeeded(task)
    response = blueprints_synthesis_response(
        task,
        route="/api/v1/docs/search/explain",
        projection="explain",
    )
    response["display"] = synthesis_display_block(response)
    return response


@router.post("/search/sync", response_model=dict)
async def sync_docs_search_index(body: DocsSearchSyncBody | None = None) -> dict[str, Any]:
    """Proxy TurboVec Docs incremental index sync for updated Markdown files."""
    body = body or DocsSearchSyncBody()
    base_url = cfg.TURBOVEC_DOCS_URL.rstrip("/")
    if not base_url:
        raise HTTPException(503, "TURBOVEC_DOCS_URL is not configured")

    payload: dict[str, Any] = {"force": body.force}
    if body.paths:
        payload["paths"] = body.paths

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(max(cfg.TURBOVEC_DOCS_TIMEOUT, 60.0))
        ) as client:
            resp = await client.post(f"{base_url}/index/sync", json=payload)
    except httpx.TimeoutException as exc:
        raise HTTPException(504, "TurboVec Docs index sync timed out") from exc
    except httpx.RequestError as exc:
        raise HTTPException(503, f"TurboVec Docs unavailable: {exc}") from exc

    if resp.status_code >= 400:
        detail = resp.text[:500] if resp.text else f"HTTP {resp.status_code}"
        raise HTTPException(502, f"TurboVec Docs index sync failed: {detail}")

    try:
        data = resp.json()
    except ValueError as exc:
        raise HTTPException(502, "TurboVec Docs returned invalid JSON") from exc

    return {
        "ok": bool(data.get("ok", True)) if isinstance(data, dict) else True,
        "force": body.force,
        "upstream": data,
    }


@router.get("/search/status", response_model=dict)
async def docs_search_status() -> dict[str, Any]:
    """Return compact health and corpus metadata for the docs search dashboard."""
    root = _docs_root()
    with get_conn() as conn:
        registered_docs = conn.execute("SELECT COUNT(*) AS n FROM docs").fetchone()["n"]
        doc_groups = conn.execute("SELECT COUNT(*) AS n FROM doc_groups").fetchone()["n"]

    on_disk_markdown = _count_docs_files(root)
    checks: list[dict[str, Any]] = [
        _docs_status_check(
            "blueprints_docs",
            "Blueprints Docs",
            root.exists() and registered_docs > 0,
            f"{registered_docs} registered docs; {on_disk_markdown} Markdown files on disk",
            critical=True,
            meta={
                "docs_root": str(root),
                "registered_docs": registered_docs,
                "on_disk_markdown": on_disk_markdown,
                "doc_groups": doc_groups,
            },
        )
    ]

    turbovec_base = cfg.TURBOVEC_DOCS_URL.rstrip("/")
    nullclaw_base = cfg.NULLCLAW_DOCS_SEARCH_URL.rstrip("/")
    turbovec_health: dict[str, Any] = {}
    turbovec_stats: dict[str, Any] = {}
    nullclaw_health: dict[str, Any] = {}
    models_ok = False
    model_ids: set[str] = set()
    models_error: str | None = None

    timeout = httpx.Timeout(max(cfg.TURBOVEC_DOCS_TIMEOUT, cfg.NULLCLAW_DOCS_SEARCH_TIMEOUT, 12.0))
    async with httpx.AsyncClient(timeout=timeout) as client:
        tv_ok, tv_data, tv_err = (
            await _docs_status_get_json(client, f"{turbovec_base}/health")
            if turbovec_base
            else (False, None, "not configured")
        )
        if tv_data:
            turbovec_health = tv_data
        checks.append(
            _docs_status_check(
                "turbovec_docs",
                "TurboVec Docs",
                tv_ok
                and bool((tv_data or {}).get("ok"))
                and bool((tv_data or {}).get("index_exists")),
                (
                    f"{(tv_data or {}).get('documents', 0)} docs; {(tv_data or {}).get('chunks', 0)} chunks"
                    if tv_data
                    else f"Unavailable: {tv_err}"
                ),
                critical=True,
                meta=tv_data or {"error": tv_err},
            )
        )

        stats_ok, stats_data, stats_err = (
            await _docs_status_get_json(client, f"{turbovec_base}/stats")
            if turbovec_base
            else (False, None, "not configured")
        )
        if stats_data:
            turbovec_stats = stats_data
        graph = (stats_data or {}).get("graph") if isinstance(stats_data, dict) else {}
        graph_ok = (
            stats_ok
            and isinstance(graph, dict)
            and int(graph.get("nodes") or 0) > 0
            and int(graph.get("edges") or 0) > 0
        )
        checks.append(
            _docs_status_check(
                "graph_sidecar",
                "Docs Graph",
                graph_ok,
                (
                    f"{int((graph or {}).get('nodes') or 0)} nodes; "
                    f"{int((graph or {}).get('edges') or 0)} edges; "
                    f"{int((graph or {}).get('headings') or 0)} headings"
                    if stats_ok
                    else f"Stats unavailable: {stats_err}"
                ),
                meta=graph if isinstance(graph, dict) else {"error": stats_err},
            )
        )

        nc_ok, nc_data, nc_err = (
            await _docs_status_get_json(client, f"{nullclaw_base}/health")
            if nullclaw_base
            else (False, None, "not configured")
        )
        if nc_data:
            nullclaw_health = nc_data
        checks.append(
            _docs_status_check(
                "nullclaw_docs_search",
                "Synthesis Worker",
                nc_ok and bool((nc_data or {}).get("ok")) and bool((nc_data or {}).get("ready")),
                (
                    f"{(nc_data or {}).get('task_count', 0)} stored tasks; worker ready"
                    if nc_data
                    else f"Unavailable: {nc_err}"
                ),
                critical=True,
                meta=nc_data or {"error": nc_err},
            )
        )
        deps_for_models = (
            (nc_data or {}).get("dependencies")
            if isinstance((nc_data or {}).get("dependencies"), dict)
            else {}
        )
        local_ai_for_models = (
            deps_for_models.get("local_ai")
            if isinstance(deps_for_models.get("local_ai"), dict)
            else {}
        )
        model_base_url = local_ai_for_models.get("base_url") or os.environ.get(
            "LITELLM_BASE_URL", ""
        )
        models_ok, model_ids, models_error = await _docs_status_model_ids(client, model_base_url)

    deps = (
        nullclaw_health.get("dependencies")
        if isinstance(nullclaw_health.get("dependencies"), dict)
        else {}
    )
    local_ai = deps.get("local_ai") if isinstance(deps.get("local_ai"), dict) else {}
    checks.append(
        _docs_status_check(
            "local_ai",
            "Local AI",
            bool(local_ai.get("ok")),
            (
                f"{local_ai.get('model') or 'configured model'} via {local_ai.get('base_url') or 'local endpoint'}"
                if local_ai
                else "No local AI health result from synthesis worker"
            ),
            critical=True,
            meta=local_ai or {},
        )
    )
    checks.extend(
        [
            _docs_status_model_check(
                "docs_embeddings_model",
                "Embeddings",
                turbovec_health.get("embedding_model"),
                model_ids,
                models_ok,
                models_error,
                critical=True,
            ),
            _docs_status_model_check(
                "docs_reranker_model",
                "Reranker",
                turbovec_health.get("reranker_model"),
                model_ids,
                models_ok,
                models_error,
            ),
            _docs_status_model_check(
                "turbovec_llm_model",
                "TurboVec LLM",
                turbovec_health.get("llm_model"),
                model_ids,
                models_ok,
                models_error,
            ),
            _docs_status_model_check(
                "synthesis_llm_model",
                "Synthesis LLM",
                local_ai.get("model"),
                model_ids,
                models_ok,
                models_error,
                critical=True,
            ),
        ]
    )

    lifecycle_counts = (
        turbovec_stats.get("lifecycle_counts")
        if isinstance(turbovec_stats.get("lifecycle_counts"), list)
        else []
    )
    unknown_lifecycle = sum(
        int(row.get("n") or 0)
        for row in lifecycle_counts
        if isinstance(row, dict)
        and str(row.get("lifecycle") or "unknown") == "unknown"
        and str(row.get("source_type") or "unknown") == "unknown"
    )
    quality_check = _docs_status_check(
        "metadata_coverage",
        "Metadata Coverage",
        unknown_lifecycle == 0,
        (
            "All indexed docs have lifecycle/source metadata"
            if unknown_lifecycle == 0
            else f"{unknown_lifecycle} indexed docs still have unknown lifecycle/source metadata"
        ),
        meta={"unknown_lifecycle_source_docs": unknown_lifecycle},
    )
    quality_checks = [quality_check]

    has_critical_failure = any(check["critical"] and not check["ok"] for check in checks)
    has_warning = any(not check["critical"] and not check["ok"] for check in checks)
    status = _traffic_status(has_critical_failure, has_warning)
    quality_status = _traffic_status(False, any(not check["ok"] for check in quality_checks))
    graph = turbovec_stats.get("graph") if isinstance(turbovec_stats.get("graph"), dict) else {}
    metrics = {
        "registered_docs": registered_docs,
        "on_disk_markdown": on_disk_markdown,
        "doc_groups": doc_groups,
        "turbovec_documents": turbovec_health.get("documents"),
        "turbovec_chunks": turbovec_health.get("chunks"),
        "graph_nodes": graph.get("nodes"),
        "graph_edges": graph.get("edges"),
        "graph_headings": graph.get("headings"),
        "unknown_lifecycle_source_docs": unknown_lifecycle,
        "nullclaw_task_count": nullclaw_health.get("task_count"),
        "available_local_models": len(model_ids) if models_ok else None,
    }
    quality = {
        "status": quality_status,
        "summary": _traffic_summary(quality_status, subject="quality"),
        "metrics": {
            "unknown_lifecycle_source_docs": unknown_lifecycle,
            "proposed_edges": graph.get("proposed_edges"),
        },
        "checks": quality_checks,
        "backlog_endpoint": "/api/v1/docs/search/quality",
    }
    return {
        "ok": status != "red",
        "status": status,
        "quality_status": quality_status,
        "summary": _traffic_summary(status, subject="health"),
        "metrics": metrics,
        "checks": checks,
        "quality": quality,
        "upstream": {
            "turbovec_docs": {
                "health": turbovec_health,
                "stats": turbovec_stats,
            },
            "nullclaw_docs_search": nullclaw_health,
        },
    }


@router.get("/search/quality", response_model=dict)
async def docs_search_quality(
    limit: int = Query(_METADATA_BACKLOG_LIMIT, ge=1, le=200),
) -> dict[str, Any]:
    """Return metadata backlog ordered by retrieval pressure and graph importance."""
    return _metadata_backlog_report(limit)


# ── Group folder opener ───────────────────────────────────────────────────────


@router.post("/group-folder/open", response_model=dict)
async def open_docs_group_folder(body: DocsGroupFolderOpenBody) -> dict[str, Any]:
    """Open the most common parent folder for the docs currently in a group."""
    root = _docs_root()
    group_id = body.group_id or None
    rows = _docs_group_path_rows(group_id)
    folder, counts = _most_common_docs_folder(root, rows)

    _open_docs_folder(folder)
    return {
        "ok": True,
        "folder": str(folder),
        "relative_folder": _docs_rel_path(root, folder),
        "document_count": len(rows),
        "folder_document_count": counts[folder],
    }


@router.post("/group-folder/tree", response_model=dict)
async def docs_group_folder_tree(body: DocsGroupFolderTreeBody) -> dict[str, Any]:
    """Return a browser-renderable tree view for a docs group folder."""
    root = _docs_root()
    group_id = body.group_id or None
    rows = _docs_group_path_rows(group_id)

    requested_path = _normalize_docs_rel(body.path or "")
    if requested_path != ".":
        folder = _safe_resolve(root, requested_path)
        if not folder.exists():
            raise HTTPException(404, f"Folder does not exist: {requested_path}")
        if not folder.is_dir():
            raise HTTPException(400, f"Not a folder: {requested_path}")
        _, counts = _most_common_docs_folder(root, rows)
    else:
        folder, counts = _most_common_docs_folder(root, rows)

    relative_folder = _docs_rel_path(root, folder)
    docs_by_path = _registered_docs_tree_lookup()
    current_doc = docs_by_path.get(relative_folder.lower())

    entries: list[dict[str, Any]] = []
    try:
        children = list(folder.iterdir())
    except OSError as exc:
        raise HTTPException(500, f"Could not list folder: {exc}") from exc

    for child in children:
        child_rel = _docs_rel_path(root, child)
        child_doc = docs_by_path.get(child_rel.lower())
        try:
            is_dir = child.is_dir()
            is_file = child.is_file()
        except OSError:
            continue
        if not is_dir and not is_file:
            continue
        entries.append(
            {
                "name": child.name,
                "path": child_rel,
                "type": "folder" if is_dir else "file",
                "registered_doc": child_doc,
            }
        )

    entries.sort(key=lambda item: (0 if item["type"] == "folder" else 1, item["name"].lower()))

    breadcrumbs = [{"label": "docs root", "path": "."}]
    if relative_folder != ".":
        running: list[str] = []
        for part in Path(relative_folder).parts:
            running.append(part)
            breadcrumbs.append({"label": part, "path": "/".join(running)})

    parent_path = None
    if relative_folder != ".":
        parent = folder.parent
        if str(parent.resolve()).startswith(str(root.resolve())):
            parent_path = _docs_rel_path(root, parent)

    return {
        "ok": True,
        "folder": str(folder),
        "relative_folder": relative_folder,
        "parent_path": parent_path,
        "breadcrumbs": breadcrumbs,
        "entries": entries,
        "current_doc": current_doc,
        "document_count": len(rows),
        "folder_document_count": counts.get(folder, 0),
    }


# ── Get with content ──────────────────────────────────────────────────────────


@router.get("/{doc_id}", response_model=DocWithContent)
async def get_doc(doc_id: str) -> DocWithContent:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, "doc not found")
    out = DocWithContent(**_row_to_out(row).model_dump())
    p = _safe_resolve(_docs_root(), row["path"])
    if p.exists():
        try:
            out.content = p.read_text(encoding="utf-8")
            out.file_exists = True
        except Exception as exc:
            log.error("docs: failed to read %s: %s", p, exc)
    return out


@router.post("/{doc_id}/speech", response_model=dict)
async def doc_speech(doc_id: str, body: DocSpeechBody | None = None) -> dict[str, Any]:
    """Return cached or freshly generated narration Markdown for a document."""
    force = bool(body.force) if body else False
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
    if not row:
        raise HTTPException(404, "doc not found")

    root = _docs_root()
    source_path = _safe_resolve(root, row["path"])
    if not source_path.is_file():
        _invalidate_doc_speech_cache(row["path"])
        raise HTTPException(404, "doc file not found")

    if force:
        _invalidate_doc_speech_cache(row["path"])
    elif cache_path := _valid_doc_speech_cache_path(row["path"], source_path):
        try:
            speech = cache_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(500, f"Could not read speech cache: {exc}") from exc
        return {
            "ok": True,
            "doc_id": doc_id,
            "doc_path": row["path"],
            "cache": "hit",
            "speech_path": str(cache_path),
            "source_version": _source_timestamp_slug(source_path),
            "generated_at": cache_path.name.split("--", 1)[0],
            "markdown": speech,
        }
    else:
        _invalidate_doc_speech_cache(row["path"])

    try:
        source_markdown = source_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise HTTPException(500, f"Could not read document: {exc}") from exc

    speech = await _generate_doc_speech_markdown(row, source_markdown)
    cache_path = _new_doc_speech_cache_path(row["path"])
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(speech + "\n", encoding="utf-8")
        _normalize_node_local_ownership(cache_path.parent)
        _normalize_node_local_ownership(cache_path)
    except OSError as exc:
        raise HTTPException(500, f"Could not write speech cache: {exc}") from exc

    return {
        "ok": True,
        "doc_id": doc_id,
        "doc_path": row["path"],
        "cache": "regenerated" if force else "miss",
        "speech_path": str(cache_path),
        "source_version": _source_timestamp_slug(source_path),
        "generated_at": cache_path.name.split("--", 1)[0],
        "markdown": speech,
    }


# ── Create ────────────────────────────────────────────────────────────────────


@router.post("", response_model=DocOut, status_code=201)
async def create_doc(body: DocCreate) -> DocOut:
    doc_id = str(uuid.uuid4())
    root = _docs_root()
    p = _safe_resolve(root, body.path)
    p.parent.mkdir(parents=True, exist_ok=True)
    _normalize_ownership(root, p.parent)
    if not p.exists() or body.initial_content is not None:
        content = body.initial_content if body.initial_content is not None else f"# {body.label}\n"
        p.write_text(content, encoding="utf-8")
        _normalize_ownership(root, p)
        log.info("docs: created file %s", p)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO docs (doc_id, label, description, tags, path, sort_order, group_id) VALUES (?,?,?,?,?,?,?)",
            (
                doc_id,
                body.label,
                body.description,
                body.tags,
                body.path,
                body.sort_order,
                body.group_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "INSERT", "docs", doc_id, dict(row), gen)
    return _row_to_out(row)


# ── Update metadata ───────────────────────────────────────────────────────────


@router.put("/{doc_id}", response_model=DocOut)
async def update_doc(doc_id: str, body: DocUpdate) -> DocOut:
    old_path: str | None = None
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        old_path = row["path"]
        conn.execute(
            """UPDATE docs SET
               label       = COALESCE(?, label),
               description = COALESCE(?, description),
               tags        = COALESCE(?, tags),
               path        = COALESCE(?, path),
               sort_order  = COALESCE(?, sort_order),
               group_id    = CASE WHEN ? IS NOT NULL THEN NULLIF(?, '') ELSE group_id END,
               updated_at  = datetime('now')
               WHERE doc_id = ?""",
            (
                body.label,
                body.description,
                body.tags,
                body.path,
                body.sort_order,
                body.group_id,
                body.group_id,
                doc_id,
            ),
        )
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "docs", doc_id, dict(row), gen)
    _invalidate_doc_speech_cache(old_path)
    if row["path"] != old_path:
        _invalidate_doc_speech_cache(row["path"])
    return _row_to_out(row)


# ── Update file content ───────────────────────────────────────────────────────


@router.put("/{doc_id}/content", status_code=204)
async def update_doc_content(doc_id: str, body: DocContentBody) -> Response:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        path_str = row["path"]
    root = _docs_root()
    p = _safe_resolve(root, path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    _normalize_ownership(root, p.parent)
    try:
        p.write_text(body.content, encoding="utf-8")
        _normalize_ownership(root, p)
        log.info("docs: wrote %d chars to %s", len(body.content), p)
        _touch_docs_sentinel()
        _invalidate_doc_speech_cache(path_str)
    except HTTPException:
        raise
    except Exception as exc:
        log.error("docs: failed to write %s: %s", p, exc)
        raise HTTPException(500, f"Failed to write file: {exc}") from exc
    # Touch updated_at and sync metadata to peers
    with get_conn() as conn:
        conn.execute("UPDATE docs SET updated_at = datetime('now') WHERE doc_id = ?", (doc_id,))
        gen = increment_gen(conn, "human")
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        enqueue_for_all_peers(conn, "UPDATE", "docs", doc_id, dict(row), gen)
    return Response(status_code=204)


# ── Delete ────────────────────────────────────────────────────────────────────


@router.delete("/{doc_id}", status_code=204)
async def delete_doc(
    doc_id: str,
    delete_file: bool = Query(default=False, description="Also delete the file from disk"),
) -> Response:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM docs WHERE doc_id=?", (doc_id,)).fetchone()
        if not row:
            raise HTTPException(404, "doc not found")
        path_str = row["path"]
        conn.execute("DELETE FROM docs WHERE doc_id=?", (doc_id,))
        gen = increment_gen(conn, "human")
        enqueue_for_all_peers(conn, "DELETE", "docs", doc_id, {}, gen)
    if delete_file:
        root = _docs_root()
        p = _safe_resolve(root, path_str)
        if p.exists():
            try:
                p.unlink()
                log.info("docs: deleted file %s", p)
            except Exception as exc:
                log.warning("docs: failed to delete file %s: %s", p, exc)
    _invalidate_doc_speech_cache(path_str)
    return Response(status_code=204)
