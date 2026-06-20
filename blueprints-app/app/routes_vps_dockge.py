"""routes_vps_dockge.py - VPS Dockge stack status/control endpoints.

This mirrors the Local Dockge API shape, but runs the compose operations over
SSH. The SSH host list is ordered by the private service environment, so a
tailnet address can be preferred with a public address used only as failover.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shlex
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

import httpx
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg
from .routes_docs import (
    _clamp_source_markdown,
    _clean_doc_speech_markdown,
    _complete_doc_speech_local,
    _normalize_node_local_ownership,
)
from .routes_local_dockge import (
    _COMPOSE_FILENAMES,
    _DOCS_CANDIDATES,
    _EXPOSURE_FILENAMES,
    _OPENAPI_CANDIDATES,
    _VALID_ACTIONS,
    _apply_manifest_exposures,
    _base_exposures,
    _candidate_url,
    _field,
    _parse_compose_ps,
    _parse_labels,
    _ports_from_publishers,
    _ports_from_string,
    _service_configs,
    _summarize_openapi,
    _summarize_status,
)
from .tts_sanitizer_client import TtsSanitizerUnavailable, prepare_tts_markdown_for_llm_via_service

log = logging.getLogger(__name__)

router = APIRouter(prefix="/vps-dockge", tags=["vps-dockge"])

_COMPOSE_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<op>[^}]*)?\}")
_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_VPS_DOCKGE_SPEECH_CACHE_ROOT = _NODE_LOCAL_ROOT / "doc-speech-vps-dockge-cache"
_VPS_DOCKGE_SPEECH_PROMPT_VERSION = "20260514-central-doc-context-v1"
_VPS_DOCKGE_SPEECH_FILE_LIMIT = 24000
_VPS_DOCKGE_SPEECH_SOURCE_LIMIT = 180000
_VPS_DOCKGE_METRICS_MIN_INTERVAL = 0.9
_VPS_DOCKGE_SOURCE_SUFFIXES = {
    ".conf",
    ".cfg",
    ".ini",
    ".json",
    ".md",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
_VPS_DOCKGE_REQUIRED_ENV = (
    "VPS_DOCKGE_BASE_URL",
    "VPS_DOCKGE_STACKS_DIR",
    "VPS_DOCKGE_SSH_USER",
    "VPS_DOCKGE_SSH_HOSTS",
    "VPS_DOCKGE_SSH_KEY",
    "VPS_DOCKGE_PROBE_ALLOWED_HOST_SUFFIXES",
)
_VPS_DOCKGE_METRICS_LOCK = threading.Lock()
_VPS_DOCKGE_METRICS_CACHE: dict[str, Any] = {"monotonic": 0.0, "payload": None}


class VpsDockgeAction(BaseModel):
    action: str


class VpsDockgeSpeechBody(BaseModel):
    force: bool = False


def _safe_stack_cache_name(stack_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", stack_name.strip()).strip(".-") or "stack"


def _ensure_vps_dockge_config() -> None:
    missing = [name for name in _VPS_DOCKGE_REQUIRED_ENV if not getattr(cfg, name, "").strip()]
    if missing:
        raise HTTPException(
            503,
            "VPS Dockge is not configured on this node; set required env vars: "
            + ", ".join(missing),
        )


def _vps_dockge_speech_cache_dir(stack_name: str) -> Path:
    return _VPS_DOCKGE_SPEECH_CACHE_ROOT / _safe_stack_cache_name(stack_name)


def _vps_dockge_speech_cache_candidates(stack_name: str) -> list[Path]:
    cache_dir = _vps_dockge_speech_cache_dir(stack_name)
    if not cache_dir.is_dir():
        return []
    candidates = [
        path for path in cache_dir.iterdir() if path.is_file() and path.name.endswith(".txt")
    ]
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name), reverse=True)


def _vps_dockge_speech_cache_meta_path(cache_path: Path) -> Path:
    return cache_path.with_name(f"{cache_path.name}.meta.json")


def _new_vps_dockge_speech_cache_path(stack_name: str) -> Path:
    cache_dir = _vps_dockge_speech_cache_dir(stack_name)
    base = (
        cache_dir
        / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}--{_safe_stack_cache_name(stack_name)}.txt"
    )
    if not base.exists():
        return base
    for index in range(1, 100):
        candidate = (
            cache_dir
            / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{index:02d}--{_safe_stack_cache_name(stack_name)}.txt"
        )
        if not candidate.exists():
            return candidate
    raise HTTPException(500, "Could not allocate a unique VPS Dockge narration cache path")


def _invalidate_vps_dockge_speech_cache(stack_name: str) -> None:
    for cached in _vps_dockge_speech_cache_candidates(stack_name):
        try:
            cached.unlink()
        except OSError as exc:
            log.warning("VPS Dockge narration: could not remove stale cache %s: %s", cached, exc)
        meta_path = _vps_dockge_speech_cache_meta_path(cached)
        try:
            if meta_path.exists():
                meta_path.unlink()
        except OSError as exc:
            log.warning(
                "VPS Dockge narration: could not remove stale cache metadata %s: %s", meta_path, exc
            )


def _vps_dockge_speech_condition(stack: dict) -> dict:
    clean = json.loads(json.dumps(stack, default=str))
    clean.pop("updated_at", None)
    clean.pop("path", None)
    clean.pop("connection_host", None)
    return clean


def _vps_dockge_speech_env_int(
    name: str, default: int, minimum: int = 0, maximum: int = 1000000
) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("VPS Dockge narration: invalid %s=%r; using %s", name, raw, default)
        return default
    return max(minimum, min(maximum, value))


def _vps_dockge_speech_fingerprint(stack: dict, source_items: list[dict]) -> str:
    payload = {
        "prompt_version": _VPS_DOCKGE_SPEECH_PROMPT_VERSION,
        "stack": _vps_dockge_speech_condition(stack),
        "sources": [
            {
                "path": item.get("path"),
                "kind": item.get("kind"),
                "size": item.get("size"),
                "mtime_ns": item.get("mtime_ns"),
                "sha256": item.get("sha256"),
                "clipped": item.get("clipped"),
            }
            for item in source_items
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _valid_vps_dockge_speech_cache_path(stack_name: str, fingerprint: str) -> Path | None:
    for candidate in _vps_dockge_speech_cache_candidates(stack_name):
        meta_path = _vps_dockge_speech_cache_meta_path(candidate)
        if not meta_path.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if meta.get("fingerprint") == fingerprint:
            return candidate
    return None


def _add_vps_dockge_source_text(
    *,
    path: str,
    kind: str,
    text: str,
    source_items: list[dict],
    seen: set[str],
    mtime_ns: int | None = None,
) -> None:
    source_key = f"{kind}:{path}"
    if source_key in seen:
        return
    if PurePosixPath(path).name == ".env":
        return
    suffix = Path(path).suffix.lower()
    if suffix and suffix not in _VPS_DOCKGE_SOURCE_SUFFIXES and not path.endswith("/.env.example"):
        return
    seen.add(source_key)
    clipped = len(text) > _VPS_DOCKGE_SPEECH_FILE_LIMIT
    source_items.append(
        {
            "path": path,
            "kind": kind,
            "size": len(text.encode("utf-8", errors="replace")),
            "mtime_ns": mtime_ns,
            "sha256": hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest(),
            "clipped": clipped,
            "text": text[:_VPS_DOCKGE_SPEECH_FILE_LIMIT],
        }
    )


def _read_vps_dockge_local_source(
    path: Path, kind: str, source_items: list[dict], seen: set[str]
) -> None:
    try:
        resolved = path.resolve()
    except OSError:
        return
    if not resolved.is_file() or resolved.name == ".env":
        return
    if resolved.suffix.lower() not in _VPS_DOCKGE_SOURCE_SUFFIXES:
        return
    try:
        stat = resolved.stat()
        text = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("VPS Dockge narration: could not read source %s: %s", resolved, exc)
        return
    _add_vps_dockge_source_text(
        path=str(resolved),
        kind=kind,
        text=text,
        source_items=source_items,
        seen=seen,
        mtime_ns=stat.st_mtime_ns,
    )


def _read_vps_dockge_remote_source(
    stack_name: str,
    rel_name: str,
    kind: str,
    source_items: list[dict],
    seen: set[str],
) -> None:
    if rel_name == ".env":
        return
    text = _remote_read_file(stack_name, rel_name)
    if text is None:
        return
    remote_path = str(PurePosixPath(_stack_dir(stack_name)) / rel_name)
    _add_vps_dockge_source_text(
        path=f"vps:{remote_path}",
        kind=kind,
        text=text,
        source_items=source_items,
        seen=seen,
    )


def _vps_stack_doc_names(stack_name: str) -> list[str]:
    upper_dash = re.sub(r"[^A-Za-z0-9]+", "-", stack_name).strip("-").upper()
    upper_underscore = re.sub(r"[^A-Za-z0-9]+", "_", stack_name).strip("_").upper()
    return list(
        dict.fromkeys(
            [
                f"{upper_dash}.md",
                f"{upper_underscore}.md",
                f"{stack_name}.md",
                f"{stack_name.upper()}.md",
            ]
        )
    )


def _gather_vps_dockge_speech_sources(stack_name: str, compose_name: str) -> list[dict]:
    source_items: list[dict] = []
    seen: set[str] = set()

    _read_vps_dockge_remote_source(
        stack_name, compose_name, "vps-stack-compose", source_items, seen
    )
    for name in _EXPOSURE_FILENAMES:
        _read_vps_dockge_remote_source(
            stack_name, name, "vps-stack-exposure-manifest", source_items, seen
        )
    for name in ("README.md", "NOTES.md", "OPERATIONS.md", ".env.example"):
        _read_vps_dockge_remote_source(stack_name, name, "vps-stack-note", source_items, seen)

    vps_docs = _NODE_LOCAL_ROOT / "docs" / "vps" / "my-vps" / "dockge"
    for name in ("README.md", *_vps_stack_doc_names(stack_name)):
        _read_vps_dockge_local_source(vps_docs / name, "central-vps-dockge-doc", source_items, seen)

    dockge_control_doc = _NODE_LOCAL_ROOT / "docs" / "dockge" / "VPS-DOCKGE-BLUEPRINTS-CONTROL.md"
    _read_vps_dockge_local_source(
        dockge_control_doc, "central-blueprints-control-doc", source_items, seen
    )
    for path in (
        _NODE_LOCAL_ROOT / "docs" / "vps" / "my-vps" / "README.md",
        _NODE_LOCAL_ROOT / "docs" / "vps" / "README.md",
    ):
        _read_vps_dockge_local_source(path, "central-vps-doc", source_items, seen)

    return source_items


def _vps_dockge_source_markdown(stack: dict, source_items: list[dict]) -> str:
    parts = [
        "# VPS Dockge stack condition",
        "",
        "Live stack inspection JSON:",
        "```json",
        json.dumps(_vps_dockge_speech_condition(stack), indent=2, sort_keys=True, default=str),
        "```",
    ]
    for item in source_items:
        parts.extend(
            [
                "",
                f"## Source: {item['kind']} - {item['path']}",
                "",
                item["text"],
            ]
        )
    return "\n".join(parts)


_VPS_DOCKGE_SPEECH_SYSTEM_PROMPT = """
You write concise spoken condition reports for the VPS Dockge page.

Use the supplied live inspection JSON first. Use the central xarta docs, safe VPS stack files, and compose notes as supporting context.

Rules:
- Output plain text only. No Markdown headings, bullets, tables, code fences, citations, or source labels.
- Start with the stack name and its current observed status and health.
- Explain non-ideal conditions when present: mixed health, unknown health, stopped containers, starting containers, exited containers, compose errors, public exposure concerns, tailnet access concerns, or missing runtime information.
- Be grounded and honest. Distinguish observed facts from likely causes. If the context does not prove why something happened, say that it is not clear from the available evidence.
- Mention useful next checks only when they follow directly from the supplied docs or status data.
- Do not reveal secrets or environment values. If a missing environment variable is named in an error, mention only the variable name and the effect.
- Prefer service names and human descriptions over raw URLs. Mention a hostname, port, path, tailnet IP, or status code only when it explains the condition or a useful next check.
- Preserve acronyms, product names, status codes, and identifiers as ordinary text. The final speech sanitizer handles pronunciation for URL, API, HTTP, OpenAPI, PostgreSQL, IP addresses, CIDR ranges, status codes, and joined tokens.
- Do not pre-pronounce technical punctuation. Write `.env`, paths, hostnames, URLs, IPs, CIDR ranges, and `host:port` values as ordinary text when they are important; never write forms like "dot env", "colon 8080", or hand-spelled URL punctuation.
- After the condition report, add a concise closing description of what the stack is meant to do. Cover its stand-alone purpose first, then its role in the wider VPS, private tailnet, Headscale, Traefik, Blueprints, Dockge, Hermes, or network-control setup when the supplied context supports it.
- If the supplied docs and stack files do not explain the stack's purpose or wider role, say that the available central context does not describe its intended role clearly.
- Aim for roughly 180 to 420 spoken words total. It can be longer for complex or unhealthy stacks, but stay concise enough for row-level playback and MP3 generation.
""".strip()


async def _generate_vps_dockge_speech_markdown(
    stack: dict, source_items: list[dict]
) -> tuple[str, dict[str, Any]]:
    source_limit = _vps_dockge_speech_env_int(
        "VPS_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT",
        _VPS_DOCKGE_SPEECH_SOURCE_LIMIT,
        minimum=0,
        maximum=1000000,
    )
    try:
        prepared_source = await prepare_tts_markdown_for_llm_via_service(
            _vps_dockge_source_markdown(stack, source_items)
        )
    except TtsSanitizerUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    speech_source, source_meta = _clamp_source_markdown(prepared_source, limit=source_limit)
    if source_meta.get("source_clipped"):
        raise HTTPException(
            413,
            (
                "VPS Dockge narration source exceeded VPS_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT "
                f"({source_meta.get('source_chars')} chars > {source_meta.get('source_char_limit')})."
            ),
        )
    user_prompt = (
        "/no-think\n"
        f"Stack: {stack.get('stack_name')}\n\n"
        "Write a spoken condition report from this gathered VPS Dockge context:\n\n"
        f"{speech_source}"
    )
    answer, llm_meta = await _complete_doc_speech_local(
        [
            {"role": "system", "content": _VPS_DOCKGE_SPEECH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        operation="vps-dockge:narration",
    )
    generation_meta = {**source_meta, **llm_meta}
    if generation_meta.get("finish_reason") == "length":
        raise HTTPException(
            502,
            "VPS Dockge narration hit DOC_SPEECH_LLM_MAX_TOKENS before completing. Increase the limit and regenerate.",
        )
    speech = await _clean_doc_speech_markdown(str(answer or ""))
    if not speech:
        raise HTTPException(502, "Local LLM returned an empty VPS Dockge narration")
    generation_meta.update(
        {
            "speech_chars": len(speech),
            "speech_words": len(speech.split()),
            "source_count": len(source_items),
            "prompt_version": _VPS_DOCKGE_SPEECH_PROMPT_VERSION,
        }
    )
    return speech, generation_meta


def _stacks_root() -> str:
    return str(PurePosixPath(cfg.VPS_DOCKGE_STACKS_DIR))


def _ssh_hosts() -> list[str]:
    hosts: list[str] = []
    for raw in cfg.VPS_DOCKGE_SSH_HOSTS.split(","):
        host = raw.strip()
        if host and host not in hosts:
            hosts.append(host)
    if not hosts:
        raise HTTPException(500, "VPS_DOCKGE_SSH_HOSTS is empty")
    return hosts


def _ssh_base_args(host: str, *, connect_timeout: int = 8) -> list[str]:
    key = cfg.VPS_DOCKGE_SSH_KEY.strip()
    if not key or not os.path.isfile(key):
        raise HTTPException(500, f"VPS_DOCKGE_SSH_KEY not found: {key or '(empty)'}")
    return [
        "ssh",
        "-i",
        key,
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        f"{cfg.VPS_DOCKGE_SSH_USER}@{host}",
    ]


def _run_remote(script: str, *, timeout: int = 30) -> tuple[subprocess.CompletedProcess[str], str]:
    errors: list[str] = []
    for host in _ssh_hosts():
        try:
            result = subprocess.run(
                _ssh_base_args(host) + [script],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            errors.append(f"{host}: SSH command timed out")
            continue
        if result.returncode == 255:
            errors.append(
                f"{host}: {result.stderr.strip() or result.stdout.strip() or 'SSH failed'}"
            )
            continue
        return result, host
    raise HTTPException(
        504, f"Could not reach VPS over SSH: {'; '.join(errors) or 'all hosts failed'}"
    )


def _remote_metrics_script(root: str, source: str) -> str:
    remote_py = r"""
import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def emit(payload):
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True))


def field(item, *names):
    for name in names:
        value = item.get(name)
        if value is not None:
            return str(value)
    return ""


def parse_json_lines(text):
    stripped = (text or "").strip()
    if not stripped:
        return []
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass
    rows = []
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def run_json(args, timeout):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "docker command failed")
    return parse_json_lines(result.stdout)


def parse_labels(value):
    if isinstance(value, dict):
        return {str(key): str(val) for key, val in value.items()}
    labels = {}
    for part in str(value or "").split(","):
        if "=" not in part:
            continue
        key, val = part.split("=", 1)
        labels[key.strip()] = val.strip()
    return labels


def parse_size_bytes(value):
    match = re.match(r"^\s*(?P<num>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>[A-Za-z]+)?", value or "")
    if not match:
        return 0
    number = float(match.group("num"))
    unit = (match.group("unit") or "B").lower()
    multipliers = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": 1024**3,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    return int(number * multipliers.get(unit, 1))


def parse_percent(value):
    try:
        return float(str(value or "0").strip().rstrip("%"))
    except ValueError:
        return 0.0


def memory_usage_bytes(value):
    return parse_size_bytes(str(value or "").split("/", 1)[0].strip())


def read_cgroup_number(path):
    try:
        raw = Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw or raw == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def available_memory_bytes():
    cgroup_limit = read_cgroup_number("/sys/fs/cgroup/memory.max")
    if cgroup_limit:
        return cgroup_limit
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024
    except OSError:
        pass
    return 0


def parse_cpuset_count(value):
    total = 0
    for part in (value or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            try:
                total += max(0, int(end) - int(start) + 1)
            except ValueError:
                continue
        else:
            try:
                int(part)
            except ValueError:
                continue
            total += 1
    return total


def available_cpu_units():
    try:
        quota_raw, period_raw, *_ = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").strip().split()
        if quota_raw != "max":
            quota = float(quota_raw)
            period = float(period_raw)
            if quota > 0 and period > 0:
                return max(0.01, quota / period)
    except (OSError, ValueError):
        pass
    try:
        cpuset_count = parse_cpuset_count(Path("/sys/fs/cgroup/cpuset.cpus.effective").read_text(encoding="utf-8").strip())
        if cpuset_count:
            return float(cpuset_count)
    except OSError:
        pass
    try:
        affinity = os.sched_getaffinity(0)
        if affinity:
            return float(len(affinity))
    except (AttributeError, OSError):
        pass
    return float(os.cpu_count() or 1)


def stack_dirs(root):
    try:
        root_path = Path(root).expanduser().resolve()
        names = {path.name for path in root_path.iterdir() if path.is_dir()}
    except OSError:
        root_path = Path(root).expanduser()
        names = set()
    return root_path, names


def stack_name_from_labels(labels, root_path, known_stacks):
    working_dir = (labels.get("com.docker.compose.project.working_dir") or "").strip()
    if working_dir:
        try:
            resolved = Path(working_dir).expanduser().resolve()
            if resolved == root_path or root_path in resolved.parents:
                return resolved.name
        except OSError:
            pass
    project = (labels.get("com.docker.compose.project") or "").strip()
    if project and (not known_stacks or project in known_stacks):
        return project
    return ""


try:
    stacks_root = os.environ.get("BP_STACKS_ROOT", "")
    root_path, known_stacks = stack_dirs(stacks_root)
    rows = run_json(["docker", "ps", "--all", "--no-trunc", "--format", "json"], timeout=5)
    by_ref = {}
    by_stack = {}
    for row in rows:
        labels = parse_labels(row.get("Labels") if "Labels" in row else row.get("labels"))
        stack_name = stack_name_from_labels(labels, root_path, known_stacks)
        if not stack_name:
            continue
        container_id = field(row, "ID", "Id", "id")
        name = field(row, "Names", "Name", "name")
        state = field(row, "State", "state").lower()
        meta = {
            "id": container_id,
            "name": name,
            "stack_name": stack_name,
            "service": labels.get("com.docker.compose.service", ""),
            "state": state,
        }
        if container_id:
            by_ref[container_id] = meta
            by_ref[container_id[:12]] = meta
        if name:
            by_ref[name] = meta
        by_stack.setdefault(stack_name, []).append(meta)

    running_ids = sorted({meta["id"] for meta in by_ref.values() if meta.get("id") and meta.get("state") == "running"})
    stat_rows = run_json(["docker", "stats", "--no-stream", "--format", "json", *running_ids], timeout=8) if running_ids else []
    cpu_units = available_cpu_units()
    memory_total = available_memory_bytes()
    stacks = {
        stack_name: {
            "stack_name": stack_name,
            "cpu_percent": 0.0,
            "cpu_docker_percent": 0.0,
            "memory_percent": 0.0,
            "memory_bytes": 0,
            "containers": [],
        }
        for stack_name in by_stack
    }

    for row in stat_rows:
        container_ref = field(row, "Container", "ID", "Name")
        name = field(row, "Name")
        meta = by_ref.get(container_ref) or by_ref.get(container_ref[:12]) or by_ref.get(name)
        if not meta:
            continue
        stack_name = meta["stack_name"]
        stack = stacks.setdefault(stack_name, {
            "stack_name": stack_name,
            "cpu_percent": 0.0,
            "cpu_docker_percent": 0.0,
            "memory_percent": 0.0,
            "memory_bytes": 0,
            "containers": [],
        })
        docker_cpu = parse_percent(field(row, "CPUPerc"))
        memory_bytes = memory_usage_bytes(field(row, "MemUsage"))
        stack["cpu_docker_percent"] += docker_cpu
        stack["memory_bytes"] += memory_bytes
        stack["containers"].append({
            "id": meta.get("id", "")[:12],
            "name": meta.get("name") or name,
            "service": meta.get("service", ""),
            "cpu_docker_percent": round(docker_cpu, 3),
            "memory_bytes": memory_bytes,
        })

    for stack in stacks.values():
        stack["cpu_percent"] = round(min(100.0, stack["cpu_docker_percent"] / max(cpu_units, 0.01)), 3)
        stack["cpu_docker_percent"] = round(stack["cpu_docker_percent"], 3)
        stack["memory_percent"] = round(
            min(100.0, (stack["memory_bytes"] / memory_total) * 100.0) if memory_total else 0.0,
            3,
        )

    emit({
        "ok": True,
        "source": os.environ.get("BP_METRICS_SOURCE", "docker-stats-ssh"),
        "capacity": {
            "cpu_units": round(cpu_units, 3),
            "memory_bytes": memory_total,
        },
        "interval_seconds": 1,
        "window_seconds": 10,
        "stacks": sorted(stacks.values(), key=lambda item: item["stack_name"].lower()),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
except Exception as exc:
    emit({"ok": False, "detail": str(exc), "updated_at": datetime.now(timezone.utc).isoformat()})
"""
    return (
        f"BP_STACKS_ROOT={shlex.quote(root)} "
        f"BP_METRICS_SOURCE={shlex.quote(source)} "
        f"python3 - <<'PY'\n{remote_py}\nPY"
    )


def _vps_dockge_metrics_sync() -> dict:
    _ensure_vps_dockge_config()
    now = time.monotonic()
    with _VPS_DOCKGE_METRICS_LOCK:
        cached_at = float(_VPS_DOCKGE_METRICS_CACHE.get("monotonic") or 0.0)
        cached_payload = _VPS_DOCKGE_METRICS_CACHE.get("payload")
        if cached_payload and now - cached_at < _VPS_DOCKGE_METRICS_MIN_INTERVAL:
            return {**cached_payload, "cache": "hit"}

        result, host = _run_remote(
            _remote_metrics_script(_stacks_root(), "docker-stats-ssh"),
            timeout=12,
        )
        if result.returncode != 0:
            raise HTTPException(
                502,
                result.stderr.strip() or result.stdout.strip() or "VPS Dockge metrics read failed",
            )
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise HTTPException(502, f"VPS Dockge metrics returned invalid JSON: {exc}") from exc
        if not payload.get("ok"):
            raise HTTPException(502, payload.get("detail") or "VPS Dockge metrics read failed")
        payload = {**payload, "ssh_host": host}
        _VPS_DOCKGE_METRICS_CACHE["monotonic"] = time.monotonic()
        _VPS_DOCKGE_METRICS_CACHE["payload"] = payload
        return {**payload, "cache": "miss"}


def _valid_stack_name(stack_name: str) -> str:
    name = (stack_name or "").strip()
    if not name or "/" in name or "\\" in name or name in {".", ".."}:
        raise HTTPException(400, "invalid stack name")
    return name


def _stack_dir(stack_name: str) -> str:
    return str(PurePosixPath(_stacks_root()) / _valid_stack_name(stack_name))


def _remote_compose_name(stack_name: str) -> str:
    stack_dir = _stack_dir(stack_name)
    checks = "\n".join(
        f'test -f "$dir/{name}" && printf %s {shlex.quote(name)} && exit 0'
        for name in _COMPOSE_FILENAMES
    )
    script = f'dir={shlex.quote(stack_dir)}\ntest -d "$dir" || exit 43\n{checks}\nexit 44'
    result, _host = _run_remote(script, timeout=12)
    if result.returncode == 43:
        raise HTTPException(404, f"stack '{stack_name}' not found")
    if result.returncode == 44:
        raise HTTPException(404, f"stack '{stack_name}' has no compose file")
    if result.returncode != 0:
        raise HTTPException(
            500, result.stderr.strip() or result.stdout.strip() or "compose lookup failed"
        )
    return result.stdout.strip()


def _run_compose(
    stack_name: str,
    compose_name: str,
    args: list[str],
    *,
    timeout: int = 30,
) -> tuple[subprocess.CompletedProcess[str], str]:
    stack_dir = _stack_dir(stack_name)
    quoted_args = " ".join(shlex.quote(arg) for arg in args)
    script = (
        f"cd {shlex.quote(stack_dir)} && "
        f"docker compose -f {shlex.quote(compose_name)} "
        f"--project-directory {shlex.quote(stack_dir)} {quoted_args}"
    )
    return _run_remote(script, timeout=timeout)


def _compose_config(stack_name: str, compose_name: str) -> tuple[dict, str | None]:
    result, _host = _run_compose(
        stack_name, compose_name, ["config", "--format", "json"], timeout=25
    )
    if result.returncode != 0:
        return {}, result.stderr.strip() or result.stdout.strip() or "docker compose config failed"
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"docker compose config returned invalid JSON: {exc}"
    return parsed if isinstance(parsed, dict) else {}, None


def _remote_read_file(stack_name: str, rel_name: str, *, timeout: int = 12) -> str | None:
    if "/" in rel_name or "\\" in rel_name or rel_name in {"", ".", ".."}:
        return None
    path = str(PurePosixPath(_stack_dir(stack_name)) / rel_name)
    script = f"test -f {shlex.quote(path)} && cat {shlex.quote(path)}"
    result, _host = _run_remote(script, timeout=timeout)
    if result.returncode != 0:
        return None
    return result.stdout


def _read_exposure_manifest(stack_name: str) -> dict:
    for name in _EXPOSURE_FILENAMES:
        text = _remote_read_file(stack_name, name)
        if text is None:
            continue
        try:
            if name.endswith(".json"):
                parsed = json.loads(text)
            else:
                parsed = yaml.safe_load(text)
        except Exception as exc:
            return {"_error": f"{name}: {exc}"}
        return parsed if isinstance(parsed, dict) else {"_error": f"{name}: expected mapping"}
    return {}


def _remote_env_requirements(stack_name: str, compose_name: str) -> dict:
    compose_text = _remote_read_file(stack_name, compose_name) or ""
    env_text = _remote_read_file(stack_name, ".env") or ""
    referenced = set()
    required = set()
    for match in _COMPOSE_ENV_PATTERN.finditer(compose_text):
        name = match.group("name")
        op = match.group("op") or ""
        referenced.add(name)
        if op.startswith(":?") or op.startswith("?"):
            required.add(name)
    present: dict[str, bool] = {}
    for line in env_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            present[key] = bool(value.strip().strip('"').strip("'"))
    return {
        "referenced": sorted(referenced),
        "required": sorted(required),
        "missing_required": [name for name in sorted(required) if not present.get(name, False)],
    }


def _normalize_stack(
    stack_name: str,
    compose_name: str,
    ps_items: list[dict],
    compose_config: dict | None = None,
    connection_host: str | None = None,
) -> dict:
    containers = []
    services = []
    for item in ps_items:
        service = _field(item, "Service", "service")
        if service:
            services.append(service)
        labels = _parse_labels(_field(item, "Labels", "labels"))
        ports = _ports_from_publishers(item) or _ports_from_string(_field(item, "Ports", "ports"))
        containers.append(
            {
                "id": _field(item, "ID", "Id", "id"),
                "name": _field(item, "Name", "Names", "name"),
                "service": service,
                "image": _field(item, "Image", "image"),
                "state": _field(item, "State", "state").lower() or "unknown",
                "status": _field(item, "Status", "status"),
                "health": _field(item, "Health", "health").lower(),
                "ports": _field(item, "Ports", "ports"),
                "ports_structured": ports,
                "labels": labels,
                "exit_code": item.get("ExitCode", item.get("exit_code")),
            }
        )

    compose_services = _service_configs(compose_config or {})
    for service in compose_services:
        if service not in services:
            services.append(service)

    services_sorted = sorted(set(services))
    exposures = _base_exposures(services_sorted, containers, compose_services, [])
    exposures, manifest_error = _apply_manifest_exposures(
        exposures, _read_exposure_manifest(stack_name)
    )
    status, health, running, total = _summarize_status(containers)
    stack = {
        "stack_name": stack_name,
        "path": _stack_dir(stack_name),
        "compose_file": compose_name,
        "status": status,
        "health": health,
        "running": running,
        "total": total,
        "services": services_sorted,
        "service_exposures": exposures,
        "containers": containers,
        "connection_host": connection_host,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if manifest_error:
        stack["exposure_manifest_error"] = manifest_error
    return stack


def _inspect_stack(stack_name: str) -> dict:
    name = _valid_stack_name(stack_name)
    compose_name = _remote_compose_name(name)
    result, host = _run_compose(name, compose_name, ["ps", "--all", "--format", "json"], timeout=25)
    if result.returncode != 0:
        raise HTTPException(
            500,
            f"docker compose ps failed for {name}: {result.stderr.strip() or result.stdout.strip() or '(no output)'}",
        )
    compose_config, config_error = _compose_config(name, compose_name)
    stack = _normalize_stack(
        name, compose_name, _parse_compose_ps(result.stdout), compose_config, host
    )
    stack["env_requirements"] = _remote_env_requirements(name, compose_name)
    if config_error:
        stack["compose_config_error"] = config_error
    return stack


def _inspect_stack_lenient(stack_name: str) -> dict:
    name = _valid_stack_name(stack_name)
    compose_name = _remote_compose_name(name)
    result, host = _run_compose(name, compose_name, ["ps", "--all", "--format", "json"], timeout=25)
    compose_config, config_error = _compose_config(name, compose_name)
    if result.returncode == 0:
        stack = _normalize_stack(
            name, compose_name, _parse_compose_ps(result.stdout), compose_config, host
        )
    else:
        services = sorted(_service_configs(compose_config))
        exposures, manifest_error = _apply_manifest_exposures(
            _base_exposures(services, [], _service_configs(compose_config), []),
            _read_exposure_manifest(name),
        )
        stack = {
            "stack_name": name,
            "path": _stack_dir(name),
            "compose_file": compose_name,
            "status": "unknown",
            "health": "unknown",
            "running": 0,
            "total": 0,
            "services": services,
            "service_exposures": exposures,
            "containers": [],
            "connection_host": host,
            "error": result.stderr.strip() or result.stdout.strip() or "docker compose ps failed",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        if manifest_error:
            stack["exposure_manifest_error"] = manifest_error
    stack["env_requirements"] = _remote_env_requirements(name, compose_name)
    if config_error:
        stack["compose_config_error"] = config_error
    return stack


def _is_probe_url_allowed(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        return True
    suffixes = [
        item.strip().lower()
        for item in cfg.VPS_DOCKGE_PROBE_ALLOWED_HOST_SUFFIXES.split(",")
        if item.strip()
    ]
    return any(host.endswith(suffix) for suffix in suffixes)


async def _probe_text(client: httpx.AsyncClient, url: str, accept: str = "*/*") -> dict:
    if not _is_probe_url_allowed(url):
        return {"url": url, "ok": False, "status": None, "error": "probe URL not allowed"}
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() in {"localhost", "127.0.0.1", "::1"}:
        header = f"Accept: {accept}"
        marker = "__BP_VPS_DOCKGE_CURL__"
        script = (
            "curl -skL --max-time 3 "
            f"-H {shlex.quote(header)} "
            "-w "
            f"{shlex.quote(chr(10) + marker + 'STATUS:%{http_code}' + chr(10) + marker + 'TYPE:%{content_type}' + chr(10) + marker + 'URL:%{url_effective}' + chr(10))} "
            f"{shlex.quote(url)}"
        )
        result, _host = _run_remote(script, timeout=8)
        text = result.stdout
        status = None
        content_type = ""
        effective_url = url
        body = text
        marker_index = text.find(marker + "STATUS:")
        if marker_index >= 0:
            body = text[:marker_index].strip()
            trailer = text[marker_index:].splitlines()
            for line in trailer:
                if line.startswith(marker + "STATUS:"):
                    try:
                        status = int(line.split(":", 1)[1])
                    except ValueError:
                        status = None
                elif line.startswith(marker + "TYPE:"):
                    content_type = line.split(":", 1)[1]
                elif line.startswith(marker + "URL:"):
                    effective_url = line.split(":", 1)[1] or url
        return {
            "url": effective_url,
            "ok": bool(status and 200 <= status < 400),
            "status": status,
            "content_type": content_type,
            "text": body[:200000],
            "error": "" if result.returncode == 0 else (result.stderr.strip() or "curl failed"),
        }
    try:
        resp = await client.get(url, headers={"Accept": accept})
    except Exception as exc:
        return {"url": url, "ok": False, "status": None, "error": str(exc)}
    return {
        "url": str(resp.url),
        "ok": 200 <= resp.status_code < 400,
        "status": resp.status_code,
        "content_type": resp.headers.get("content-type", ""),
        "text": resp.text[:200000],
    }


def _find_service_exposure(stack_name: str, service_name: str) -> tuple[dict, dict]:
    stack = _inspect_stack(stack_name)
    exposure = (stack.get("service_exposures") or {}).get(service_name)
    if not exposure:
        raise HTTPException(404, f"service '{service_name}' not found in stack '{stack_name}'")
    return stack, exposure


@router.get("/stacks", status_code=200)
async def list_vps_dockge_stacks() -> dict:
    return await asyncio.to_thread(_list_vps_dockge_stacks_sync)


@router.get("/metrics", status_code=200)
async def vps_dockge_metrics() -> dict:
    return await asyncio.to_thread(_vps_dockge_metrics_sync)


def _list_vps_dockge_stacks_sync() -> dict:
    _ensure_vps_dockge_config()
    root = _stacks_root()
    result, host = _run_remote(
        f'root={shlex.quote(root)}; test -d "$root" || exit 43; '
        "find \"$root\" -mindepth 1 -maxdepth 1 -type d -printf '%f\\n' | sort",
        timeout=15,
    )
    if result.returncode == 43:
        raise HTTPException(404, f"VPS Dockge stacks directory not found: {root}")
    if result.returncode != 0:
        raise HTTPException(
            500, result.stderr.strip() or result.stdout.strip() or "stack listing failed"
        )

    stacks = []
    for name in [line.strip() for line in result.stdout.splitlines() if line.strip()]:
        try:
            compose_name = _remote_compose_name(name)
        except HTTPException:
            continue
        ps_result, ps_host = _run_compose(
            name, compose_name, ["ps", "--all", "--format", "json"], timeout=25
        )
        compose_config, config_error = _compose_config(name, compose_name)
        if ps_result.returncode != 0:
            stack = {
                "stack_name": name,
                "path": _stack_dir(name),
                "compose_file": compose_name,
                "status": "unknown",
                "health": "unknown",
                "running": 0,
                "total": 0,
                "services": sorted(_service_configs(compose_config)),
                "service_exposures": {},
                "containers": [],
                "connection_host": ps_host,
                "error": ps_result.stderr.strip()
                or ps_result.stdout.strip()
                or "docker compose ps failed",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            stack = _normalize_stack(
                name, compose_name, _parse_compose_ps(ps_result.stdout), compose_config, ps_host
            )
        stack["env_requirements"] = _remote_env_requirements(name, compose_name)
        if config_error:
            stack["compose_config_error"] = config_error
        stacks.append(stack)

    return {
        "ok": True,
        "dockge_url": cfg.VPS_DOCKGE_BASE_URL,
        "stacks_dir": root,
        "ssh_host": host,
        "ssh_hosts": _ssh_hosts(),
        "stacks": stacks,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stacks/{stack_name}/services/{service_name}/info", status_code=200)
async def vps_dockge_service_info(stack_name: str, service_name: str) -> dict:
    _ensure_vps_dockge_config()
    stack, exposure = await asyncio.to_thread(_find_service_exposure, stack_name, service_name)
    base_url = exposure.get("url") or ""
    openapi_candidates = []
    if exposure.get("openapi_url"):
        openapi_candidates.append(exposure["openapi_url"])
    if base_url:
        openapi_candidates.extend(
            _candidate_url(base_url, suffix) for suffix in _OPENAPI_CANDIDATES
        )

    docs_candidates = []
    if exposure.get("docs_url"):
        docs_candidates.append(exposure["docs_url"])
    if base_url:
        docs_candidates.extend(_candidate_url(base_url, suffix) for suffix in _DOCS_CANDIDATES)

    openapi_candidates = list(dict.fromkeys(openapi_candidates))
    docs_candidates = list(dict.fromkeys(docs_candidates))
    openapi_summary = None
    openapi_checks = []
    docs_checks = []
    home_check = None
    async with httpx.AsyncClient(timeout=3.0, follow_redirects=True, verify=False) as client:
        if base_url:
            home_check = await _probe_text(client, base_url)
            home_check.pop("text", None)
        for url in openapi_candidates:
            check = await _probe_text(client, url, accept="application/json")
            text = check.pop("text", "")
            openapi_checks.append(check)
            if check.get("ok") and not openapi_summary:
                try:
                    spec = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(spec, dict) and "openapi" in spec:
                    openapi_summary = _summarize_openapi(spec, check["url"])
                    break
        for url in docs_candidates:
            check = await _probe_text(client, url)
            check.pop("text", None)
            docs_checks.append(check)

    return {
        "ok": True,
        "stack_name": stack["stack_name"],
        "service": service_name,
        "exposure": exposure,
        "home_check": home_check,
        "openapi": openapi_summary,
        "openapi_checks": openapi_checks,
        "docs_checks": docs_checks,
        "tests": {
            "status": "todo",
            "detail": "VPS service-specific smoke tests can be recorded in xarta-service-exposure.yaml.",
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/stacks/{stack_name}/speech", status_code=200)
async def vps_dockge_stack_speech(stack_name: str, body: VpsDockgeSpeechBody | None = None) -> dict:
    _ensure_vps_dockge_config()
    force = bool(body.force) if body else False
    stack = _inspect_stack_lenient(stack_name)
    source_items = _gather_vps_dockge_speech_sources(stack["stack_name"], stack["compose_file"])
    fingerprint = _vps_dockge_speech_fingerprint(stack, source_items)

    if force:
        _invalidate_vps_dockge_speech_cache(stack["stack_name"])
    elif cache_path := _valid_vps_dockge_speech_cache_path(stack["stack_name"], fingerprint):
        try:
            speech = cache_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(500, f"Could not read VPS Dockge narration cache: {exc}") from exc
        speech_meta = None
        meta_path = _vps_dockge_speech_cache_meta_path(cache_path)
        if meta_path.is_file():
            try:
                speech_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.warning(
                    "VPS Dockge narration: could not read cache metadata %s: %s", meta_path, exc
                )
        return {
            "ok": True,
            "stack": stack["stack_name"],
            "cache": "hit",
            "speech_path": str(cache_path),
            "generated_at": cache_path.name.split("--", 1)[0],
            "generation": speech_meta,
            "markdown": speech,
        }

    markdown, speech_meta = await _generate_vps_dockge_speech_markdown(stack, source_items)
    cache_path = _new_vps_dockge_speech_cache_path(stack["stack_name"])
    source_summary = [
        {
            "path": item.get("path"),
            "kind": item.get("kind"),
            "size": item.get("size"),
            "clipped": item.get("clipped"),
        }
        for item in source_items
    ]
    speech_meta = {
        **speech_meta,
        "stack": stack["stack_name"],
        "fingerprint": fingerprint,
        "prompt_version": _VPS_DOCKGE_SPEECH_PROMPT_VERSION,
        "generated_at": cache_path.name.split("--", 1)[0],
        "sources": source_summary,
        "condition": _vps_dockge_speech_condition(stack),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(markdown + "\n", encoding="utf-8")
        _vps_dockge_speech_cache_meta_path(cache_path).write_text(
            json.dumps(speech_meta, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _normalize_node_local_ownership(cache_path.parent)
        _normalize_node_local_ownership(cache_path)
        _normalize_node_local_ownership(_vps_dockge_speech_cache_meta_path(cache_path))
    except OSError as exc:
        raise HTTPException(500, f"Could not write VPS Dockge narration cache: {exc}") from exc

    return {
        "ok": True,
        "stack": stack["stack_name"],
        "cache": "regenerated" if force else "miss",
        "speech_path": str(cache_path),
        "generated_at": cache_path.name.split("--", 1)[0],
        "generation": speech_meta,
        "markdown": markdown,
    }


@router.post("/stacks/{stack_name}/action", status_code=200)
async def vps_dockge_stack_action(stack_name: str, body: VpsDockgeAction) -> dict:
    _ensure_vps_dockge_config()
    action = body.action.strip().lower()
    if action not in _VALID_ACTIONS:
        raise HTTPException(400, f"invalid action '{action}'; must be start, stop, or restart")
    return await asyncio.to_thread(_vps_dockge_stack_action_sync, stack_name, action)


def _vps_dockge_stack_action_sync(stack_name: str, action: str) -> dict:
    name = _valid_stack_name(stack_name)
    compose_name = _remote_compose_name(name)
    args = ["up", "-d"] if action == "start" else [action]
    result, host = _run_compose(name, compose_name, args, timeout=70)
    if result.returncode != 0:
        raise HTTPException(
            500,
            f"docker compose {action} failed for {name}: {result.stderr.strip() or result.stdout.strip() or '(no output)'}",
        )
    log.info("VPS Dockge stack %s: %s succeeded via %s", name, action, host)
    return {
        "ok": True,
        "action": action,
        "stack": name,
        "ssh_host": host,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "result": _inspect_stack(name),
    }
