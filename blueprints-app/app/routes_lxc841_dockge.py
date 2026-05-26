"""routes_lxc841_dockge.py - LXC841 Dockge stack status/control endpoints.

This mirrors the Local/VPS Dockge API shape, but runs compose operations inside
remote LXC 841 through a configured Proxmox SSH + pct exec hop.
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

router = APIRouter(prefix="/lxc841-dockge", tags=["lxc841-dockge"])

_COMPOSE_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<op>[^}]*)?\}")
_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_LXC841_DOCKGE_SPEECH_CACHE_ROOT = _NODE_LOCAL_ROOT / "doc-speech-lxc841-dockge-cache"
_LXC841_DOCKGE_SPEECH_PROMPT_VERSION = "20260514-central-doc-context-v1"
_LXC841_DOCKGE_SPEECH_FILE_LIMIT = 24000
_LXC841_DOCKGE_SPEECH_SOURCE_LIMIT = 180000
_LXC841_DOCKGE_SOURCE_SUFFIXES = {
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
_LXC841_DOCKGE_REQUIRED_ENV = (
    "LXC841_DOCKGE_BASE_URL",
    "LXC841_DOCKGE_STACKS_DIR",
    "LXC841_DOCKGE_SSH_USER",
    "LXC841_DOCKGE_SSH_HOSTS",
    "LXC841_DOCKGE_SSH_KEY",
    "LXC841_DOCKGE_PVE_VMID",
    "LXC841_DOCKGE_LXC_IP",
    "LXC841_DOCKGE_CADDY_VMID",
    "LXC841_DOCKGE_CADDYFILE",
    "LXC841_DOCKGE_PROBE_ALLOWED_HOST_SUFFIXES",
)
_LXC841_CADDY_ROUTES_CACHE: tuple[float, list[dict]] | None = None


class Lxc841DockgeAction(BaseModel):
    action: str


class Lxc841DockgeSpeechBody(BaseModel):
    force: bool = False


def _safe_stack_cache_name(stack_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", stack_name.strip()).strip(".-") or "stack"


def _ensure_lxc841_dockge_config() -> None:
    missing = [name for name in _LXC841_DOCKGE_REQUIRED_ENV if not getattr(cfg, name, "").strip()]
    if missing:
        raise HTTPException(
            503,
            "LXC841 Dockge is not configured on this node; set required env vars: "
            + ", ".join(missing),
        )


def _lxc841_dockge_speech_cache_dir(stack_name: str) -> Path:
    return _LXC841_DOCKGE_SPEECH_CACHE_ROOT / _safe_stack_cache_name(stack_name)


def _lxc841_dockge_speech_cache_candidates(stack_name: str) -> list[Path]:
    cache_dir = _lxc841_dockge_speech_cache_dir(stack_name)
    if not cache_dir.is_dir():
        return []
    candidates = [path for path in cache_dir.iterdir() if path.is_file() and path.name.endswith(".txt")]
    return sorted(candidates, key=lambda path: (path.stat().st_mtime, path.name), reverse=True)


def _lxc841_dockge_speech_cache_meta_path(cache_path: Path) -> Path:
    return cache_path.with_name(f"{cache_path.name}.meta.json")


def _new_lxc841_dockge_speech_cache_path(stack_name: str) -> Path:
    cache_dir = _lxc841_dockge_speech_cache_dir(stack_name)
    base = cache_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}--{_safe_stack_cache_name(stack_name)}.txt"
    if not base.exists():
        return base
    for index in range(1, 100):
        candidate = cache_dir / f"{datetime.now().strftime('%Y%m%d-%H%M%S')}-{index:02d}--{_safe_stack_cache_name(stack_name)}.txt"
        if not candidate.exists():
            return candidate
    raise HTTPException(500, "Could not allocate a unique LXC841 Dockge narration cache path")


def _invalidate_lxc841_dockge_speech_cache(stack_name: str) -> None:
    for cached in _lxc841_dockge_speech_cache_candidates(stack_name):
        try:
            cached.unlink()
        except OSError as exc:
            log.warning("LXC841 Dockge narration: could not remove stale cache %s: %s", cached, exc)
        meta_path = _lxc841_dockge_speech_cache_meta_path(cached)
        try:
            if meta_path.exists():
                meta_path.unlink()
        except OSError as exc:
            log.warning("LXC841 Dockge narration: could not remove stale cache metadata %s: %s", meta_path, exc)


def _lxc841_dockge_speech_condition(stack: dict) -> dict:
    clean = json.loads(json.dumps(stack, default=str))
    clean.pop("updated_at", None)
    clean.pop("path", None)
    clean.pop("connection_host", None)
    return clean


def _lxc841_dockge_speech_env_int(name: str, default: int, minimum: int = 0, maximum: int = 1000000) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("LXC841 Dockge narration: invalid %s=%r; using %s", name, raw, default)
        return default
    return max(minimum, min(maximum, value))


def _lxc841_dockge_speech_fingerprint(stack: dict, source_items: list[dict]) -> str:
    payload = {
        "prompt_version": _LXC841_DOCKGE_SPEECH_PROMPT_VERSION,
        "stack": _lxc841_dockge_speech_condition(stack),
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


def _valid_lxc841_dockge_speech_cache_path(stack_name: str, fingerprint: str) -> Path | None:
    for candidate in _lxc841_dockge_speech_cache_candidates(stack_name):
        meta_path = _lxc841_dockge_speech_cache_meta_path(candidate)
        if not meta_path.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if meta.get("fingerprint") == fingerprint:
            return candidate
    return None


def _add_lxc841_dockge_source_text(
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
    if suffix and suffix not in _LXC841_DOCKGE_SOURCE_SUFFIXES and not path.endswith("/.env.example"):
        return
    seen.add(source_key)
    clipped = len(text) > _LXC841_DOCKGE_SPEECH_FILE_LIMIT
    source_items.append({
        "path": path,
        "kind": kind,
        "size": len(text.encode("utf-8", errors="replace")),
        "mtime_ns": mtime_ns,
        "sha256": hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest(),
        "clipped": clipped,
        "text": text[:_LXC841_DOCKGE_SPEECH_FILE_LIMIT],
    })


def _read_lxc841_dockge_local_source(path: Path, kind: str, source_items: list[dict], seen: set[str]) -> None:
    try:
        resolved = path.resolve()
    except OSError:
        return
    if not resolved.is_file() or resolved.name == ".env":
        return
    if resolved.suffix.lower() not in _LXC841_DOCKGE_SOURCE_SUFFIXES:
        return
    try:
        stat = resolved.stat()
        text = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("LXC841 Dockge narration: could not read source %s: %s", resolved, exc)
        return
    _add_lxc841_dockge_source_text(
        path=str(resolved),
        kind=kind,
        text=text,
        source_items=source_items,
        seen=seen,
        mtime_ns=stat.st_mtime_ns,
    )


def _read_lxc841_dockge_remote_source(
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
    _add_lxc841_dockge_source_text(
        path=f"lxc841:{remote_path}",
        kind=kind,
        text=text,
        source_items=source_items,
        seen=seen,
    )


def _lxc841_stack_doc_names(stack_name: str) -> list[str]:
    upper_dash = re.sub(r"[^A-Za-z0-9]+", "-", stack_name).strip("-").upper()
    upper_underscore = re.sub(r"[^A-Za-z0-9]+", "_", stack_name).strip("_").upper()
    return list(dict.fromkeys([
        f"{upper_dash}.md",
        f"{upper_underscore}.md",
        f"{stack_name}.md",
        f"{stack_name.upper()}.md",
    ]))


def _central_lxc841_dockge_docs_root() -> Path | None:
    rel = cfg.LXC841_DOCKGE_DOCS_REL_ROOT.strip().strip("/")
    if not rel or ".." in Path(rel).parts:
        return None
    return _NODE_LOCAL_ROOT / "docs" / rel


def _gather_lxc841_dockge_speech_sources(stack_name: str, compose_name: str) -> list[dict]:
    source_items: list[dict] = []
    seen: set[str] = set()

    _read_lxc841_dockge_remote_source(stack_name, compose_name, "lxc841-stack-compose", source_items, seen)
    for name in _EXPOSURE_FILENAMES:
        _read_lxc841_dockge_remote_source(stack_name, name, "lxc841-stack-exposure-manifest", source_items, seen)
    for name in ("README.md", "NOTES.md", "OPERATIONS.md", ".env.example"):
        _read_lxc841_dockge_remote_source(stack_name, name, "lxc841-stack-note", source_items, seen)

    lxc841_docs = _central_lxc841_dockge_docs_root()
    if lxc841_docs:
        for name in ("README.md", *_lxc841_stack_doc_names(stack_name)):
            _read_lxc841_dockge_local_source(lxc841_docs / name, "central-lxc841-dockge-doc", source_items, seen)

    dockge_control_doc = _NODE_LOCAL_ROOT / "docs" / "dockge" / "LXC841-DOCKGE-BLUEPRINTS-CONTROL.md"
    _read_lxc841_dockge_local_source(dockge_control_doc, "central-blueprints-control-doc", source_items, seen)
    if lxc841_docs:
        host_docs = lxc841_docs.parent.parent
        for path in (
            lxc841_docs.parent / "README.md",
            host_docs / "README.md",
            host_docs / "SERVICES.md",
            host_docs / "LXC-CONTROL-API.md",
        ):
            _read_lxc841_dockge_local_source(path, "central-remote-host-doc", source_items, seen)

    return source_items


def _lxc841_dockge_source_markdown(stack: dict, source_items: list[dict]) -> str:
    parts = [
        "# LXC841 Dockge stack condition",
        "",
        "Live stack inspection JSON:",
        "```json",
        json.dumps(_lxc841_dockge_speech_condition(stack), indent=2, sort_keys=True, default=str),
        "```",
    ]
    for item in source_items:
        parts.extend([
            "",
            f"## Source: {item['kind']} - {item['path']}",
            "",
            item["text"],
        ])
    return "\n".join(parts)


_LXC841_DOCKGE_SPEECH_SYSTEM_PROMPT = """
You write concise spoken condition reports for the LXC841 Dockge page.

Use the supplied live inspection JSON first. Use central xarta docs before safe LXC841 stack files and compose notes.

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
- After the condition report, add a concise closing description of what the stack is meant to do. Cover its stand-alone purpose first, then its role in the remote Proxmox host, LXC 841, Caddy, Blueprints, Dockge, local AI, GPU, TTS, STT, or network-control setup when the supplied context supports it.
- If the supplied docs and stack files do not explain the stack's purpose or wider role, say that the available central context does not describe its intended role clearly.
- Aim for roughly 180 to 420 spoken words total. It can be longer for complex or unhealthy stacks, but stay concise enough for row-level playback and MP3 generation.
""".strip()


async def _generate_lxc841_dockge_speech_markdown(stack: dict, source_items: list[dict]) -> tuple[str, dict[str, Any]]:
    source_limit = _lxc841_dockge_speech_env_int(
        "LXC841_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT",
        _LXC841_DOCKGE_SPEECH_SOURCE_LIMIT,
        minimum=0,
        maximum=1000000,
    )
    try:
        prepared_source = await prepare_tts_markdown_for_llm_via_service(
            _lxc841_dockge_source_markdown(stack, source_items)
        )
    except TtsSanitizerUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    speech_source, source_meta = _clamp_source_markdown(prepared_source, limit=source_limit)
    if source_meta.get("source_clipped"):
        raise HTTPException(
            413,
            (
                "LXC841 Dockge narration source exceeded LXC841_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT "
                f"({source_meta.get('source_chars')} chars > {source_meta.get('source_char_limit')})."
            ),
        )
    user_prompt = (
        "/no-think\n"
        f"Stack: {stack.get('stack_name')}\n\n"
        "Write a spoken condition report from this gathered LXC841 Dockge context:\n\n"
        f"{speech_source}"
    )
    answer, llm_meta = await _complete_doc_speech_local(
        [
            {"role": "system", "content": _LXC841_DOCKGE_SPEECH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        operation="lxc841-dockge:narration",
    )
    generation_meta = {**source_meta, **llm_meta}
    if generation_meta.get("finish_reason") == "length":
        raise HTTPException(
            502,
            "LXC841 Dockge narration hit DOC_SPEECH_LLM_MAX_TOKENS before completing. Increase the limit and regenerate.",
        )
    speech = await _clean_doc_speech_markdown(str(answer or ""))
    if not speech:
        raise HTTPException(502, "Local LLM returned an empty LXC841 Dockge narration")
    generation_meta.update({
        "speech_chars": len(speech),
        "speech_words": len(speech.split()),
        "source_count": len(source_items),
        "prompt_version": _LXC841_DOCKGE_SPEECH_PROMPT_VERSION,
    })
    return speech, generation_meta


def _stacks_root() -> str:
    return str(PurePosixPath(cfg.LXC841_DOCKGE_STACKS_DIR))


def _ssh_hosts() -> list[str]:
    hosts: list[str] = []
    for raw in cfg.LXC841_DOCKGE_SSH_HOSTS.split(","):
        host = raw.strip()
        if host and host not in hosts:
            hosts.append(host)
    if not hosts:
        raise HTTPException(500, "LXC841_DOCKGE_SSH_HOSTS is empty")
    return hosts


def _ssh_base_args(host: str, *, connect_timeout: int = 8) -> list[str]:
    key = cfg.LXC841_DOCKGE_SSH_KEY.strip()
    if not key or not os.path.isfile(key):
        raise HTTPException(500, f"LXC841_DOCKGE_SSH_KEY not found: {key or '(empty)'}")
    args = [
        "ssh",
        "-i",
        key,
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"ConnectTimeout={connect_timeout}",
    ]
    bind_addr = cfg.LXC841_DOCKGE_PVE_BIND_ADDR.strip()
    if bind_addr:
        args.extend(["-b", bind_addr])
    args.append(f"{cfg.LXC841_DOCKGE_SSH_USER}@{host}")
    return args


def _run_pve(script: str, *, timeout: int = 30) -> tuple[subprocess.CompletedProcess[str], str]:
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
            errors.append(f"{host}: {result.stderr.strip() or result.stdout.strip() or 'SSH failed'}")
            continue
        return result, host
    raise HTTPException(504, f"Could not reach the configured Proxmox host over SSH for LXC841 Dockge: {'; '.join(errors) or 'all hosts failed'}")


def _run_remote(script: str, *, timeout: int = 30) -> tuple[subprocess.CompletedProcess[str], str]:
    vmid = cfg.LXC841_DOCKGE_PVE_VMID.strip()
    wrapped = f"pct exec {shlex.quote(vmid)} -- bash -lc {shlex.quote(script)}"
    return _run_pve(wrapped, timeout=timeout)


def _route_url(host: str, path: str) -> str:
    if host.startswith("http://") or host.startswith("https://"):
        base = host.rstrip("/")
    elif host.startswith(":"):
        base = f"http://{cfg.LXC841_DOCKGE_LXC_IP}{host}"
    else:
        base = f"https://{host.strip(',')}"
    clean_path = (path or "/").replace("*", "").rstrip("/")
    if not clean_path or clean_path == "/":
        return f"{base}/"
    return f"{base}{clean_path}/"


def _split_caddy_hosts(value: str) -> list[str]:
    hosts: list[str] = []
    for item in re.split(r"[,\s]+", value.strip()):
        host = item.strip().strip(",")
        if not host or host.startswith("@"):
            continue
        if host in {"log", "route", "handle", "handle_path", "transport"}:
            continue
        hosts.append(host)
    return hosts


def _parse_lxc841_caddy_routes() -> list[dict]:
    global _LXC841_CADDY_ROUTES_CACHE
    now = datetime.now(timezone.utc).timestamp()
    if _LXC841_CADDY_ROUTES_CACHE and now - _LXC841_CADDY_ROUTES_CACHE[0] < 30:
        return [dict(route) for route in _LXC841_CADDY_ROUTES_CACHE[1]]

    caddy_vmid = cfg.LXC841_DOCKGE_CADDY_VMID.strip()
    caddyfile = cfg.LXC841_DOCKGE_CADDYFILE.strip()
    lxc_ip = cfg.LXC841_DOCKGE_LXC_IP.strip()
    if not caddy_vmid or not lxc_ip:
        _LXC841_CADDY_ROUTES_CACHE = (now, [])
        return []
    result, _host = _run_pve(
        f"pct exec {shlex.quote(caddy_vmid)} -- cat {shlex.quote(caddyfile)}",
        timeout=12,
    )
    if result.returncode != 0:
        log.warning("LXC841 Dockge: could not read Caddyfile from LXC %s: %s", caddy_vmid, result.stderr.strip())
        _LXC841_CADDY_ROUTES_CACHE = (now, [])
        return []

    routes = []
    current_hosts: list[str] = []
    current_path = "/"
    block_depth = 0
    pending_site_hosts: list[str] = []
    pending_handle_path = ""

    site_re = re.compile(r"^(?P<hosts>[^{}#]+?)\s*\{")
    handle_re = re.compile(r"^handle(?:_path)?\s+([^\s{]+)")
    proxy_re = re.compile(r"reverse_proxy\s+(?:https?://)?(?P<host>[A-Za-z0-9._-]+|\[[^\]]+\]):(?P<port>\d+)")
    ignored_block_heads = {
        "log",
        "header",
        "transport",
        "handle",
        "handle_path",
        "route",
        "respond",
        "reverse_proxy",
        "tls",
        "@options",
    }

    for raw_line in result.stdout.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        site_match = site_re.match(line)
        if site_match and block_depth == 0:
            first = site_match.group("hosts").strip().split(None, 1)[0]
            if first not in ignored_block_heads:
                pending_site_hosts = _split_caddy_hosts(site_match.group("hosts"))

        handle_match = handle_re.match(line)
        if handle_match and current_hosts:
            handle_target = handle_match.group(1)
            pending_handle_path = "/" if handle_target.startswith("@") else handle_target

        proxy_match = proxy_re.search(line)
        if proxy_match and current_hosts and proxy_match.group("host").strip("[]") == lxc_ip:
            port = int(proxy_match.group("port"))
            for host in current_hosts:
                routes.append({
                    "host": urlparse(host).netloc or host,
                    "site": host,
                    "path": current_path,
                    "url": _route_url(host, current_path),
                    "upstream": f"{lxc_ip}:{port}",
                    "upstream_port": port,
                    "source": f"remote-caddy-lxc{caddy_vmid}",
                })

        opens = line.count("{")
        closes = line.count("}")
        if opens:
            if pending_site_hosts and block_depth == 0:
                current_hosts = pending_site_hosts
                current_path = "/"
                pending_site_hosts = []
            elif pending_handle_path:
                current_path = pending_handle_path
                pending_handle_path = ""
            block_depth += opens
        if closes:
            block_depth = max(0, block_depth - closes)
            if block_depth <= 1:
                current_path = "/"
            if block_depth == 0:
                current_hosts = []
                current_path = "/"
    _LXC841_CADDY_ROUTES_CACHE = (now, routes)
    return [dict(route) for route in routes]


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
    script = f"dir={shlex.quote(stack_dir)}\ntest -d \"$dir\" || exit 43\n{checks}\nexit 44"
    result, _host = _run_remote(script, timeout=12)
    if result.returncode == 43:
        raise HTTPException(404, f"stack '{stack_name}' not found")
    if result.returncode == 44:
        raise HTTPException(404, f"stack '{stack_name}' has no compose file")
    if result.returncode != 0:
        raise HTTPException(500, result.stderr.strip() or result.stdout.strip() or "compose lookup failed")
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
    result, _host = _run_compose(stack_name, compose_name, ["config", "--format", "json"], timeout=25)
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
    return _env_requirements_from_text(compose_text, env_text)


def _env_requirements_from_text(compose_text: str, env_text: str) -> dict:
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


def _manifest_from_snapshot(name: str | None, text: str | None) -> dict:
    if not name or text is None:
        return {}
    try:
        if name.endswith(".json"):
            parsed = json.loads(text)
        else:
            parsed = yaml.safe_load(text)
    except Exception as exc:
        return {"_error": f"{name}: {exc}"}
    return parsed if isinstance(parsed, dict) else {"_error": f"{name}: expected mapping"}


def _normalize_stack(
    stack_name: str,
    compose_name: str,
    ps_items: list[dict],
    compose_config: dict | None = None,
    connection_host: str | None = None,
    exposure_manifest: dict | None = None,
    caddy_routes: list[dict] | None = None,
) -> dict:
    containers = []
    services = []
    for item in ps_items:
        service = _field(item, "Service", "service")
        if service:
            services.append(service)
        labels = _parse_labels(_field(item, "Labels", "labels"))
        ports = _ports_from_publishers(item) or _ports_from_string(_field(item, "Ports", "ports"))
        containers.append({
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
        })

    compose_services = _service_configs(compose_config or {})
    for service in compose_services:
        if service not in services:
            services.append(service)

    services_sorted = sorted(set(services))
    exposures = _base_exposures(
        services_sorted,
        containers,
        compose_services,
        caddy_routes if caddy_routes is not None else _parse_lxc841_caddy_routes(),
    )
    exposures, manifest_error = _apply_manifest_exposures(
        exposures,
        exposure_manifest if exposure_manifest is not None else _read_exposure_manifest(stack_name),
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
    stack = _normalize_stack(name, compose_name, _parse_compose_ps(result.stdout), compose_config, host)
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
        stack = _normalize_stack(name, compose_name, _parse_compose_ps(result.stdout), compose_config, host)
    else:
        services = sorted(_service_configs(compose_config))
        exposures, manifest_error = _apply_manifest_exposures(
            _base_exposures(services, [], _service_configs(compose_config), _parse_lxc841_caddy_routes()),
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
        for item in cfg.LXC841_DOCKGE_PROBE_ALLOWED_HOST_SUFFIXES.split(",")
        if item.strip()
    ]
    return any(host.endswith(suffix) for suffix in suffixes)


async def _probe_text(client: httpx.AsyncClient, url: str, accept: str = "*/*") -> dict:
    if not _is_probe_url_allowed(url):
        return {"url": url, "ok": False, "status": None, "error": "probe URL not allowed"}
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() in {"localhost", "127.0.0.1", "::1"}:
        header = f"Accept: {accept}"
        marker = "__BP_LXC841_DOCKGE_CURL__"
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


def _remote_stack_snapshots(root: str) -> tuple[list[dict], str]:
    exposure_names = json.dumps(list(_EXPOSURE_FILENAMES))
    compose_names = json.dumps(list(_COMPOSE_FILENAMES))
    remote_py = f"""
import json
import os
import subprocess
from pathlib import Path

root = Path(os.environ["BP_STACKS_ROOT"])
compose_names = {compose_names}
exposure_names = {exposure_names}
snapshots = []
if not root.is_dir():
    print(json.dumps({{"missing_root": str(root), "snapshots": []}}))
    raise SystemExit(0)
for stack_dir in sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name.lower()):
    compose = next((stack_dir / name for name in compose_names if (stack_dir / name).is_file()), None)
    if compose is None:
        continue
    def run_compose(args, timeout=12):
        try:
            result = subprocess.run(
                ["docker", "compose", "-f", compose.name, "--project-directory", str(stack_dir), *args],
                cwd=str(stack_dir),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return {{
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }}
        except subprocess.TimeoutExpired as exc:
            return {{
                "returncode": 124,
                "stdout": exc.stdout or "",
                "stderr": "docker compose command timed out",
            }}
    manifest_name = None
    manifest_text = None
    for exposure_name in exposure_names:
        path = stack_dir / exposure_name
        if path.is_file():
            manifest_name = exposure_name
            manifest_text = path.read_text(encoding="utf-8", errors="replace")
            break
    env_path = stack_dir / ".env"
    snapshots.append({{
        "stack_name": stack_dir.name,
        "path": str(stack_dir),
        "compose_file": compose.name,
        "compose_text": compose.read_text(encoding="utf-8", errors="replace"),
        "env_text": env_path.read_text(encoding="utf-8", errors="replace") if env_path.is_file() else "",
        "manifest_name": manifest_name,
        "manifest_text": manifest_text,
        "ps": run_compose(["ps", "--all", "--format", "json"], timeout=12),
        "config": run_compose(["config", "--format", "json"], timeout=12),
    }})
print(json.dumps({{"missing_root": None, "snapshots": snapshots}}))
""".strip()
    script = f"BP_STACKS_ROOT={shlex.quote(root)} python3 - <<'PY'\n{remote_py}\nPY"
    result, host = _run_remote(script, timeout=120)
    if result.returncode != 0:
        raise HTTPException(500, result.stderr.strip() or result.stdout.strip() or "stack snapshot failed")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HTTPException(500, f"stack snapshot returned invalid JSON: {exc}") from exc
    if payload.get("missing_root"):
        raise HTTPException(404, f"LXC841 Dockge stacks directory not found: {payload['missing_root']}")
    snapshots = payload.get("snapshots") if isinstance(payload, dict) else []
    return [item for item in snapshots if isinstance(item, dict)], host


@router.get("/stacks", status_code=200)
async def list_lxc841_dockge_stacks() -> dict:
    return await asyncio.to_thread(_list_lxc841_dockge_stacks_sync)


def _list_lxc841_dockge_stacks_sync() -> dict:
    _ensure_lxc841_dockge_config()
    root = _stacks_root()
    snapshots, host = _remote_stack_snapshots(root)
    caddy_routes = _parse_lxc841_caddy_routes()
    stacks = []
    for snapshot in snapshots:
        name = str(snapshot.get("stack_name") or "").strip()
        compose_name = str(snapshot.get("compose_file") or "").strip()
        if not name or not compose_name:
            continue
        ps_result = snapshot.get("ps") if isinstance(snapshot.get("ps"), dict) else {}
        config_result = snapshot.get("config") if isinstance(snapshot.get("config"), dict) else {}
        compose_config: dict = {}
        config_error = None
        if config_result.get("returncode") == 0:
            try:
                parsed = json.loads(config_result.get("stdout") or "{}")
                compose_config = parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError as exc:
                config_error = f"docker compose config returned invalid JSON: {exc}"
        else:
            config_error = config_result.get("stderr") or config_result.get("stdout") or "docker compose config failed"
        if ps_result.get("returncode") != 0:
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
                "connection_host": host,
                "error": ps_result.get("stderr") or ps_result.get("stdout") or "docker compose ps failed",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        else:
            stack = _normalize_stack(
                name,
                compose_name,
                _parse_compose_ps(ps_result.get("stdout") or ""),
                compose_config,
                host,
                exposure_manifest=_manifest_from_snapshot(snapshot.get("manifest_name"), snapshot.get("manifest_text")),
                caddy_routes=caddy_routes,
            )
        stack["env_requirements"] = _env_requirements_from_text(
            str(snapshot.get("compose_text") or ""),
            str(snapshot.get("env_text") or ""),
        )
        if config_error:
            stack["compose_config_error"] = config_error
        stacks.append(stack)

    return {
        "ok": True,
        "dockge_url": cfg.LXC841_DOCKGE_BASE_URL,
        "stacks_dir": root,
        "ssh_host": host,
        "ssh_hosts": _ssh_hosts(),
        "stacks": stacks,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stacks/{stack_name}/services/{service_name}/info", status_code=200)
async def lxc841_dockge_service_info(stack_name: str, service_name: str) -> dict:
    _ensure_lxc841_dockge_config()
    stack, exposure = await asyncio.to_thread(_find_service_exposure, stack_name, service_name)
    base_url = exposure.get("url") or ""
    openapi_candidates = []
    if exposure.get("openapi_url"):
        openapi_candidates.append(exposure["openapi_url"])
    if base_url:
        openapi_candidates.extend(_candidate_url(base_url, suffix) for suffix in _OPENAPI_CANDIDATES)

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
            "detail": "LXC841 service-specific smoke tests can be recorded in xarta-service-exposure.yaml.",
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/stacks/{stack_name}/speech", status_code=200)
async def lxc841_dockge_stack_speech(stack_name: str, body: Lxc841DockgeSpeechBody | None = None) -> dict:
    _ensure_lxc841_dockge_config()
    force = bool(body.force) if body else False
    stack = _inspect_stack_lenient(stack_name)
    source_items = _gather_lxc841_dockge_speech_sources(stack["stack_name"], stack["compose_file"])
    fingerprint = _lxc841_dockge_speech_fingerprint(stack, source_items)

    if force:
        _invalidate_lxc841_dockge_speech_cache(stack["stack_name"])
    elif cache_path := _valid_lxc841_dockge_speech_cache_path(stack["stack_name"], fingerprint):
        try:
            speech = cache_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(500, f"Could not read LXC841 Dockge narration cache: {exc}") from exc
        speech_meta = None
        meta_path = _lxc841_dockge_speech_cache_meta_path(cache_path)
        if meta_path.is_file():
            try:
                speech_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.warning("LXC841 Dockge narration: could not read cache metadata %s: %s", meta_path, exc)
        return {
            "ok": True,
            "stack": stack["stack_name"],
            "cache": "hit",
            "speech_path": str(cache_path),
            "generated_at": cache_path.name.split("--", 1)[0],
            "generation": speech_meta,
            "markdown": speech,
        }

    markdown, speech_meta = await _generate_lxc841_dockge_speech_markdown(stack, source_items)
    cache_path = _new_lxc841_dockge_speech_cache_path(stack["stack_name"])
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
        "prompt_version": _LXC841_DOCKGE_SPEECH_PROMPT_VERSION,
        "generated_at": cache_path.name.split("--", 1)[0],
        "sources": source_summary,
        "condition": _lxc841_dockge_speech_condition(stack),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(markdown + "\n", encoding="utf-8")
        _lxc841_dockge_speech_cache_meta_path(cache_path).write_text(
            json.dumps(speech_meta, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _normalize_node_local_ownership(cache_path.parent)
        _normalize_node_local_ownership(cache_path)
        _normalize_node_local_ownership(_lxc841_dockge_speech_cache_meta_path(cache_path))
    except OSError as exc:
        raise HTTPException(500, f"Could not write LXC841 Dockge narration cache: {exc}") from exc

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
async def lxc841_dockge_stack_action(stack_name: str, body: Lxc841DockgeAction) -> dict:
    _ensure_lxc841_dockge_config()
    action = body.action.strip().lower()
    if action not in _VALID_ACTIONS:
        raise HTTPException(400, f"invalid action '{action}'; must be start, stop, or restart")
    return await asyncio.to_thread(_lxc841_dockge_stack_action_sync, stack_name, action)


def _lxc841_dockge_stack_action_sync(stack_name: str, action: str) -> dict:
    name = _valid_stack_name(stack_name)
    compose_name = _remote_compose_name(name)
    args = ["up", "-d"] if action == "start" else [action]
    result, host = _run_compose(name, compose_name, args, timeout=70)
    if result.returncode != 0:
        raise HTTPException(
            500,
            f"docker compose {action} failed for {name}: {result.stderr.strip() or result.stdout.strip() or '(no output)'}",
        )
    log.info("LXC841 Dockge stack %s: %s succeeded via %s", name, action, host)
    return {
        "ok": True,
        "action": action,
        "stack": name,
        "ssh_host": host,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "result": _inspect_stack(name),
    }
