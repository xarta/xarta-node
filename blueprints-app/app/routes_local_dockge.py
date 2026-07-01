"""routes_local_dockge.py — local Dockge stack status/control endpoints.

These routes intentionally expose a narrow Blueprints-controlled surface over
the node-local Dockge stacks directory. Browsers call this through the normal
Caddy-served Blueprints API instead of talking to Dockge's loopback-only port.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

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
from .tts_sanitizer_client import TtsSanitizerUnavailable, prepare_tts_markdown_for_llm_via_service

log = logging.getLogger(__name__)

router = APIRouter(prefix="/local-dockge", tags=["local-dockge"])

_COMPOSE_FILENAMES = ("compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml")
_EXPOSURE_FILENAMES = (
    "xarta-service-exposure.yaml",
    "xarta-service-exposure.yml",
    "xarta-service-exposure.json",
)
_VALID_ACTIONS = {"start", "stop", "restart"}
_OPENAPI_CANDIDATES = ("openapi.json", "swagger.json", "api/openapi.json", "api/swagger.json")
_DOCS_CANDIDATES = ("docs", "redoc", "swagger", "api/docs")
_HTTP_PROBE_KINDS = {
    "caddy-api",
    "caddy-web",
    "localhost-api",
    "localhost-web",
    "tailnet-api",
    "tailnet-web",
}
_NON_HTTP_SERVICE_TOKENS = {
    "database",
    "db",
    "mariadb",
    "mongo",
    "mongodb",
    "mysql",
    "postgres",
    "postgresql",
    "rabbitmq",
    "redis",
    "valkey",
}
_NODE_LOCAL_ROOT = Path("/xarta-node") / ".lone-wolf"
_LOCAL_DOCKGE_SPEECH_CACHE_ROOT = _NODE_LOCAL_ROOT / "doc-speech-local-dockge-cache"
_LOCAL_DOCKGE_SPEECH_PROMPT_VERSION = "20260510-purpose-context-speech-v2"
_LOCAL_DOCKGE_SPEECH_FILE_LIMIT = 24000
_LOCAL_DOCKGE_SPEECH_SOURCE_LIMIT = 180000
_LOCAL_DOCKGE_METRICS_MAP_TTL = 20.0
_LOCAL_DOCKGE_METRICS_PS_TTL = 10.0
_LOCAL_DOCKGE_METRICS_STREAM_IDLE_TIMEOUT = 15.0
_LOCAL_DOCKGE_METRICS_STREAM_WAIT_TIMEOUT = 2.5
_LOCAL_DOCKGE_SOURCE_SUFFIXES = {
    ".caddyfile",
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
_COMPOSE_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?P<op>[^}]*)?\}")
_LOCAL_DOCKGE_METRICS_PS_CACHE: dict[str, Any] = {"monotonic": 0.0, "rows": []}
_LOCAL_DOCKGE_METRICS_STREAM_CONDITION = threading.Condition()
_LOCAL_DOCKGE_METRICS_STREAM: dict[str, Any] = {
    "thread": None,
    "payload": None,
    "payload_at": 0.0,
    "last_access": 0.0,
    "error": None,
}


class LocalDockgeAction(BaseModel):
    action: str


class LocalDockgeSpeechBody(BaseModel):
    force: bool = False


def _stacks_root() -> Path:
    return Path(cfg.LOCAL_DOCKGE_STACKS_DIR).expanduser().resolve()


def _compose_file(stack_dir: Path) -> Path | None:
    for name in _COMPOSE_FILENAMES:
        path = stack_dir / name
        if path.is_file():
            return path
    return None


def _safe_stack_dir(stack_name: str) -> tuple[Path, Path]:
    name = (stack_name or "").strip()
    if not name or "/" in name or "\\" in name or name in {".", ".."}:
        raise HTTPException(400, "invalid stack name")

    root = _stacks_root()
    stack_dir = (root / name).resolve()
    if root != stack_dir and root not in stack_dir.parents:
        raise HTTPException(400, "stack path escapes the configured stacks directory")
    if not stack_dir.is_dir():
        raise HTTPException(404, f"stack '{name}' not found")

    compose = _compose_file(stack_dir)
    if not compose:
        raise HTTPException(404, f"stack '{name}' has no compose file")
    return stack_dir, compose


def _run_compose(
    stack_dir: Path, compose: Path, args: list[str], timeout: int = 30
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(compose), "--project-directory", str(stack_dir)] + args
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "docker compose command timed out") from exc


def _compose_config(stack_dir: Path, compose: Path) -> tuple[dict, str | None]:
    result = _run_compose(stack_dir, compose, ["config", "--format", "json"], timeout=20)
    if result.returncode != 0:
        return {}, result.stderr.strip() or result.stdout.strip() or "docker compose config failed"
    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return {}, f"docker compose config returned invalid JSON: {exc}"
    return parsed if isinstance(parsed, dict) else {}, None


def _parse_labels(value: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    for part in (value or "").split(","):
        if "=" not in part:
            continue
        key, raw = part.split("=", 1)
        labels[key.strip()] = raw.strip()
    return labels


def _normalize_port(value: object) -> int | None:
    if value is None:
        return None
    try:
        port = int(str(value))
    except ValueError:
        return None
    return port if 0 < port < 65536 else None


def _ports_from_publishers(item: dict) -> list[dict]:
    ports = []
    for publisher in item.get("Publishers") or item.get("publishers") or []:
        if not isinstance(publisher, dict):
            continue
        published = _normalize_port(
            publisher.get("PublishedPort") or publisher.get("published_port")
        )
        target = _normalize_port(publisher.get("TargetPort") or publisher.get("target_port"))
        if not published and not target:
            continue
        ports.append(
            {
                "host_ip": str(publisher.get("URL") or publisher.get("url") or ""),
                "published": published,
                "target": target,
                "protocol": str(publisher.get("Protocol") or publisher.get("protocol") or "tcp"),
            }
        )
    return ports


def _ports_from_string(value: str) -> list[dict]:
    ports = []
    pattern = re.compile(
        r"(?:(?P<host_ip>127\.0\.0\.1|0\.0\.0\.0|\[::\]|::|localhost):)?"
        r"(?P<published>\d+)->(?P<target>\d+)/(?P<protocol>\w+)"
    )
    for match in pattern.finditer(value or ""):
        ports.append(
            {
                "host_ip": match.group("host_ip") or "",
                "published": _normalize_port(match.group("published")),
                "target": _normalize_port(match.group("target")),
                "protocol": match.group("protocol") or "tcp",
            }
        )
    return ports


def _ports_from_compose_service(service_config: dict) -> list[dict]:
    ports = []
    for item in service_config.get("ports") or []:
        if isinstance(item, dict):
            published = _normalize_port(item.get("published"))
            target = _normalize_port(item.get("target"))
            if not published and not target:
                continue
            ports.append(
                {
                    "host_ip": str(item.get("host_ip") or ""),
                    "published": published,
                    "target": target,
                    "protocol": str(item.get("protocol") or "tcp"),
                }
            )
            continue
        if isinstance(item, str):
            ports.extend(_ports_from_string(item))
    return ports


def _service_configs(compose_config: dict) -> dict:
    services = compose_config.get("services")
    return services if isinstance(services, dict) else {}


def _route_url(host_url: str, path: str) -> str:
    base = host_url.rstrip("/")
    clean_path = (path or "/").replace("*", "").rstrip("/")
    if not clean_path or clean_path == "/":
        return f"{base}/"
    return f"{base}{clean_path}/"


def _parse_caddy_routes() -> list[dict]:
    caddyfile = Path(cfg.LOCAL_DOCKGE_CADDYFILE)
    if not caddyfile.is_file():
        return []
    routes = []
    current_hosts: list[str] = []
    current_path = "/"
    block_depth = 0
    pending_site_hosts: list[str] = []
    pending_handle_path = ""

    site_re = re.compile(r"^(https?://[^\s{]+(?:\s+https?://[^\s{]+)*)\s*\{")
    handle_re = re.compile(r"^handle(?:_path)?\s+([^\s{]+)")
    proxy_re = re.compile(
        r"reverse_proxy\s+(?:https?://)?(?:localhost|127\.0\.0\.1|\[::1\]):(?P<port>\d+)"
    )

    for raw_line in caddyfile.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        site_match = site_re.match(line)
        if site_match and block_depth == 0:
            pending_site_hosts = site_match.group(1).split()

        handle_match = handle_re.match(line)
        if handle_match and current_hosts:
            handle_target = handle_match.group(1)
            pending_handle_path = "/" if handle_target.startswith("@") else handle_target

        proxy_match = proxy_re.search(line)
        if proxy_match and current_hosts:
            port = _normalize_port(proxy_match.group("port"))
            if port:
                for host in current_hosts:
                    routes.append(
                        {
                            "host": urlparse(host).netloc or host,
                            "site": host,
                            "path": current_path,
                            "url": _route_url(host, current_path),
                            "upstream": f"localhost:{port}",
                            "upstream_port": port,
                            "source": "caddy",
                        }
                    )

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
    return routes


def _read_exposure_manifest(stack_dir: Path) -> dict:
    for name in _EXPOSURE_FILENAMES:
        path = stack_dir / name
        if not path.is_file():
            continue
        try:
            if path.suffix == ".json":
                parsed = json.loads(path.read_text(encoding="utf-8"))
            else:
                parsed = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            return {"_error": f"{name}: {exc}"}
        return parsed if isinstance(parsed, dict) else {"_error": f"{name}: expected mapping"}
    return {}


def _service_ports(service: str, container: dict | None, compose_services: dict) -> list[dict]:
    ports: list[dict] = []
    if container:
        ports.extend(container.get("ports_structured") or [])
    config = compose_services.get(service)
    if isinstance(config, dict):
        ports.extend(_ports_from_compose_service(config))

    seen = set()
    unique = []
    for item in ports:
        key = (item.get("host_ip"), item.get("published"), item.get("target"), item.get("protocol"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _route_for_ports(ports: list[dict], caddy_routes: list[dict]) -> dict | None:
    published_ports = {port.get("published") for port in ports if port.get("published")}
    for route in caddy_routes:
        if route.get("upstream_port") in published_ports:
            return route
    return None


def _url_with_path(base_url: str, value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if not base_url:
        return None
    return urljoin(base_url.rstrip("/") + "/", value.lstrip("/"))


def _infer_kind(service: str, route: dict | None, ports: list[dict]) -> str:
    service_l = service.lower()
    if route:
        if "api" in service_l or route.get("path", "").lower().startswith("/api"):
            return "caddy-api"
        return "caddy-web"
    service_tokens = {token for token in re.split(r"[^a-z0-9]+", service_l) if token}
    if service_tokens & _NON_HTTP_SERVICE_TOKENS:
        return "internal"
    host_ports = [port for port in ports if port.get("published")]
    if host_ports:
        return "localhost-api" if "api" in service_l else "localhost-web"
    return "internal"


def _kind_supports_http_probes(kind: str | None) -> bool:
    return str(kind or "").strip().lower() in _HTTP_PROBE_KINDS


def _http_url(value: str | None) -> str:
    url = str(value or "").strip()
    parsed = urlparse(url)
    return url if parsed.scheme in {"http", "https"} and parsed.netloc else ""


def _base_exposures(
    services: list[str],
    containers: list[dict],
    compose_services: dict,
    caddy_routes: list[dict],
) -> dict[str, dict]:
    by_service = {
        container.get("service"): container for container in containers if container.get("service")
    }
    exposures: dict[str, dict] = {}
    for service in services:
        container = by_service.get(service)
        ports = _service_ports(service, container, compose_services)
        route = _route_for_ports(ports, caddy_routes)
        kind = _infer_kind(service, route, ports)
        url = route.get("url") if route else None
        if not url and _kind_supports_http_probes(kind) and ports:
            first = next((port for port in ports if port.get("published")), None)
            if first:
                url = f"http://127.0.0.1:{first['published']}/"
        exposures[service] = {
            "service": service,
            "label": service,
            "kind": kind,
            "source": route.get("source") if route else ("compose" if ports else "internal"),
            "url": url,
            "open_url": url if kind in {"caddy-web", "caddy-api"} else None,
            "route": route,
            "ports": ports,
            "description": "",
            "notes": "",
            "tests_todo": "Service-specific tests can be added in xarta-service-exposure.yaml.",
        }
    return exposures


def _apply_manifest_exposures(
    exposures: dict[str, dict], manifest: dict
) -> tuple[dict[str, dict], str | None]:
    if not manifest:
        return exposures, None
    manifest_error = manifest.get("_error")
    services = manifest.get("services") if isinstance(manifest.get("services"), dict) else {}
    for service, raw in services.items():
        if not isinstance(raw, dict):
            continue
        base = exposures.setdefault(
            str(service),
            {
                "service": str(service),
                "label": str(service),
                "kind": "internal",
                "source": "manifest",
                "url": None,
                "open_url": None,
                "ports": [],
                "description": "",
                "notes": "",
                "tests_todo": "Service-specific tests can be added in xarta-service-exposure.yaml.",
            },
        )
        base_url = raw.get("url") or base.get("url") or ""
        for key in ("label", "kind", "description", "notes", "tests_todo"):
            if raw.get(key) is not None:
                base[key] = raw.get(key)
        if raw.get("url") is not None:
            base["url"] = raw.get("url")
        base["open_url"] = raw.get("open_url") or (
            base.get("url") if base.get("kind") in {"caddy-web", "caddy-api"} else None
        )
        base["docs_url"] = _url_with_path(base_url, raw.get("docs") or raw.get("docs_url"))
        base["openapi_url"] = _url_with_path(base_url, raw.get("openapi") or raw.get("openapi_url"))
        base["source"] = "manifest"
    return exposures, manifest_error


def _parse_compose_ps(stdout: str) -> list[dict]:
    items: list[dict] = []
    text = stdout.strip()
    if not text:
        return items

    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            items.append(parsed)
    return items


def _run_docker_json(args: list[str], timeout: int = 8) -> list[dict]:
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise HTTPException(
            502, result.stderr.strip() or result.stdout.strip() or "docker command failed"
        )
    return _parse_compose_ps(result.stdout)


def _parse_size_bytes(value: str) -> int:
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


def _parse_percent(value: str) -> float:
    try:
        return float(str(value or "0").strip().rstrip("%"))
    except ValueError:
        return 0.0


def _memory_usage_bytes(value: str) -> int:
    return _parse_size_bytes(str(value or "").split("/", 1)[0].strip())


def _read_cgroup_number(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw or raw == "max":
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value > 0 else None


def _available_memory_bytes() -> int:
    cgroup_limit = _read_cgroup_number(Path("/sys/fs/cgroup/memory.max"))
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


def _parse_cpuset_count(value: str) -> int:
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


def _available_cpu_units() -> float:
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    try:
        quota_raw, period_raw, *_ = cpu_max.read_text(encoding="utf-8").strip().split()
        if quota_raw != "max":
            quota = float(quota_raw)
            period = float(period_raw)
            if quota > 0 and period > 0:
                return max(0.01, quota / period)
    except (OSError, ValueError):
        pass

    cpuset_path = Path("/sys/fs/cgroup/cpuset.cpus.effective")
    try:
        cpuset_count = _parse_cpuset_count(cpuset_path.read_text(encoding="utf-8").strip())
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


def _local_dockge_container_rows() -> list[dict]:
    now = time.monotonic()
    cached_at = float(_LOCAL_DOCKGE_METRICS_PS_CACHE.get("monotonic") or 0.0)
    cached_rows = _LOCAL_DOCKGE_METRICS_PS_CACHE.get("rows") or []
    if cached_rows and now - cached_at < _LOCAL_DOCKGE_METRICS_PS_TTL:
        return cached_rows
    rows = _run_docker_json(["ps", "--all", "--no-trunc", "--format", "json"], timeout=5)
    _LOCAL_DOCKGE_METRICS_PS_CACHE["monotonic"] = now
    _LOCAL_DOCKGE_METRICS_PS_CACHE["rows"] = rows
    return rows


def _local_dockge_stack_name_from_labels(labels: dict[str, str]) -> str:
    working_dir = (labels.get("com.docker.compose.project.working_dir") or "").strip()
    if working_dir:
        try:
            root = _stacks_root()
            resolved = Path(working_dir).expanduser().resolve()
            if resolved == root or root in resolved.parents:
                return resolved.name
        except OSError:
            pass
    return (labels.get("com.docker.compose.project") or "").strip()


def _local_dockge_container_map() -> tuple[dict[str, dict], dict[str, list[dict]]]:
    by_ref: dict[str, dict] = {}
    by_stack: dict[str, list[dict]] = {}
    for row in _local_dockge_container_rows():
        labels = _parse_labels(_field(row, "Labels", "labels"))
        stack_name = _local_dockge_stack_name_from_labels(labels)
        if not stack_name:
            continue
        container_id = _field(row, "ID", "Id", "id")
        name = _field(row, "Names", "Name", "name")
        state = _field(row, "State", "state").lower()
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
    return by_ref, by_stack


def _local_dockge_container_list() -> list[dict]:
    _by_ref, by_stack = _local_dockge_container_map()
    containers: list[dict] = []
    seen: set[str] = set()
    for stack_name, items in by_stack.items():
        for item in items:
            container_id = str(item.get("id") or "")
            if not container_id or container_id in seen:
                continue
            seen.add(container_id)
            containers.append(
                {
                    "id": container_id,
                    "name": item.get("name") or "",
                    "stack_name": stack_name,
                    "service": item.get("service") or "",
                    "state": item.get("state") or "",
                }
            )
    return sorted(containers, key=lambda item: (item["stack_name"].lower(), item.get("name", "")))


def _proc_meminfo() -> dict[str, int]:
    info: dict[str, int] = {}
    try:
        lines = Path("/proc/meminfo").read_text(encoding="utf-8").splitlines()
    except OSError:
        return info
    for line in lines:
        if ":" not in line:
            continue
        key, raw = line.split(":", 1)
        if key in {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}:
            parts = raw.split()
            if parts:
                info[key] = int(parts[0]) * 1024
    return info


def _proc_stat_cpu() -> dict[str, int]:
    try:
        parts = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0].split()
    except (OSError, IndexError):
        return {"total": 0, "idle": 0}
    values = [int(part) for part in parts[1:] if part.isdigit()]
    idle = values[3] + values[4] if len(values) > 4 else (values[3] if len(values) > 3 else 0)
    return {"total": sum(values), "idle": idle}


def _interface_speed_mbps(name: str) -> int:
    try:
        value = int((Path("/sys/class/net") / name / "speed").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return 0
    return value if value > 0 else 0


def _proc_netdev() -> list[dict]:
    try:
        lines = Path("/proc/net/dev").read_text(encoding="utf-8").splitlines()[2:]
    except OSError:
        return []
    rows = []
    for line in lines:
        if ":" not in line:
            continue
        iface, raw = line.split(":", 1)
        parts = raw.split()
        if len(parts) < 16:
            continue
        name = iface.strip()
        external = name.startswith(
            ("en", "eth", "wl", "ww", "tailscale", "wg")
        ) and not name.startswith("veth")
        rows.append(
            {
                "name": name,
                "rx_bytes": int(parts[0]),
                "tx_bytes": int(parts[8]),
                "external": external,
                "speed_mbps": _interface_speed_mbps(name) if external else 0,
            }
        )
    return rows


def _local_dockge_cgroup_dir(container_id: str) -> Path | None:
    candidates = [
        Path(f"/sys/fs/cgroup/system.slice/docker-{container_id}.scope"),
        Path(f"/sys/fs/cgroup/docker/{container_id}"),
        Path(f"/sys/fs/cgroup/docker-{container_id}.scope"),
        Path(f"/sys/fs/cgroup/system.slice/docker.service/docker/{container_id}"),
    ]
    for current in candidates:
        if (current / "cpu.stat").exists() or (current / "memory.current").exists():
            return current
    return None


def _local_dockge_cpu_usage_usec(cgroup: Path | None) -> int | None:
    if cgroup is None:
        return None
    try:
        for line in (cgroup / "cpu.stat").read_text(encoding="utf-8").splitlines():
            if line.startswith("usage_usec "):
                return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return None


def _local_dockge_sample_raw(containers: list[dict]) -> dict:
    meminfo = _proc_meminfo()
    sampled = []
    for item in containers:
        container_id = str(item.get("id") or "")
        cgroup = _local_dockge_cgroup_dir(container_id)
        memory_bytes = _read_cgroup_number(cgroup / "memory.current") if cgroup else 0
        sampled.append(
            {
                **item,
                "cgroup_found": cgroup is not None,
                "cpu_usage_usec": _local_dockge_cpu_usage_usec(cgroup),
                "memory_bytes": memory_bytes or 0,
            }
        )
    return {
        "monotonic_ns": time.monotonic_ns(),
        "capacity": {
            "cpu_units": round(_available_cpu_units(), 3),
            "memory_bytes": meminfo.get("MemTotal", 0),
        },
        "host": {
            "cpu": _proc_stat_cpu(),
            "memory": {
                "total_bytes": meminfo.get("MemTotal", 0),
                "available_bytes": meminfo.get("MemAvailable", 0),
                "swap_total_bytes": meminfo.get("SwapTotal", 0),
                "swap_free_bytes": meminfo.get("SwapFree", 0),
            },
            "network_interfaces": _proc_netdev(),
        },
        "containers": sampled,
    }


def _local_dockge_stack_metrics_from_counters(current: dict, previous: dict | None) -> dict:
    elapsed_seconds = 0.0
    if previous:
        elapsed_seconds = max(
            0.0,
            (int(current.get("monotonic_ns") or 0) - int(previous.get("monotonic_ns") or 0))
            / 1_000_000_000,
        )
    sample_ready = elapsed_seconds > 0
    elapsed_usec = elapsed_seconds * 1_000_000
    prev_by_id = {
        str(item.get("id") or ""): item
        for item in (previous or {}).get("containers", [])
        if item.get("id")
    }
    memory_total = int((current.get("capacity") or {}).get("memory_bytes") or 0)
    cpu_units = float((current.get("capacity") or {}).get("cpu_units") or 1.0)
    stacks: dict[str, dict[str, Any]] = {}
    for item in current.get("containers") or []:
        stack_name = str(item.get("stack_name") or "")
        if not stack_name:
            continue
        stack = stacks.setdefault(
            stack_name,
            {
                "stack_name": stack_name,
                "cpu_percent": 0.0,
                "cpu_docker_percent": 0.0,
                "memory_percent": 0.0,
                "memory_bytes": 0,
                "containers": [],
            },
        )
        container_id = str(item.get("id") or "")
        current_usage = item.get("cpu_usage_usec")
        previous_usage = (prev_by_id.get(container_id) or {}).get("cpu_usage_usec")
        cpu_cores = 0.0
        if (
            sample_ready
            and isinstance(current_usage, int)
            and isinstance(previous_usage, int)
            and elapsed_usec > 0
        ):
            cpu_cores = max(0.0, (current_usage - previous_usage) / elapsed_usec)
        docker_cpu = cpu_cores * 100.0
        memory_bytes = max(0, int(item.get("memory_bytes") or 0))
        stack["cpu_docker_percent"] += docker_cpu
        stack["memory_bytes"] += memory_bytes
        stack["containers"].append(
            {
                "id": container_id[:12],
                "name": item.get("name") or "",
                "service": item.get("service") or "",
                "state": item.get("state")
                or ("running" if item.get("cgroup_found") else "stopped"),
                "cgroup_found": bool(item.get("cgroup_found")),
                "cpu_docker_percent": round(docker_cpu, 3),
                "memory_bytes": memory_bytes,
            }
        )
    for stack in stacks.values():
        stack["cpu_docker_percent"] = round(stack["cpu_docker_percent"], 3)
        stack["cpu_percent"] = round(
            min(100.0, stack["cpu_docker_percent"] / max(cpu_units, 0.01)),
            3,
        )
        stack["memory_percent"] = round(
            min(100.0, (stack["memory_bytes"] / memory_total) * 100.0) if memory_total else 0.0,
            3,
        )

    host = current.get("host") or {}
    host_memory = host.get("memory") or {}
    host_cpu_percent = 0.0
    if previous:
        current_cpu = host.get("cpu") or {}
        previous_cpu = (previous.get("host") or {}).get("cpu") or {}
        total_delta = int(current_cpu.get("total") or 0) - int(previous_cpu.get("total") or 0)
        idle_delta = int(current_cpu.get("idle") or 0) - int(previous_cpu.get("idle") or 0)
        if total_delta > 0:
            host_cpu_percent = max(0.0, min(100.0, (1.0 - idle_delta / total_delta) * 100.0))

    previous_net = {
        item.get("name"): item
        for item in (((previous or {}).get("host") or {}).get("network_interfaces") or [])
        if item.get("name")
    }
    interfaces = []
    total_rx_bps = 0.0
    total_tx_bps = 0.0
    total_capacity_bps = 0.0
    for item in host.get("network_interfaces") or []:
        name = item.get("name")
        prev = previous_net.get(name)
        rx_bps = 0.0
        tx_bps = 0.0
        speed_mbps = max(0.0, float(item.get("speed_mbps") or 0))
        capacity_bps = (speed_mbps * 1_000_000) / 8 if speed_mbps > 0 else 0.0
        if sample_ready and prev:
            rx_bps = max(
                0.0,
                (int(item.get("rx_bytes") or 0) - int(prev.get("rx_bytes") or 0)) / elapsed_seconds,
            )
            tx_bps = max(
                0.0,
                (int(item.get("tx_bytes") or 0) - int(prev.get("tx_bytes") or 0)) / elapsed_seconds,
            )
        if item.get("external"):
            total_rx_bps += rx_bps
            total_tx_bps += tx_bps
            total_capacity_bps += capacity_bps
        interfaces.append(
            {
                "name": name,
                "rx_bytes": int(item.get("rx_bytes") or 0),
                "tx_bytes": int(item.get("tx_bytes") or 0),
                "rx_bytes_per_second": round(rx_bps, 1),
                "tx_bytes_per_second": round(tx_bps, 1),
                "external": bool(item.get("external")),
                "speed_mbps": round(speed_mbps, 3),
                "capacity_bytes_per_second": round(capacity_bps, 1),
            }
        )

    host_total = int(host_memory.get("total_bytes") or memory_total or 0)
    host_available = int(host_memory.get("available_bytes") or 0)
    host_memory_used = max(0, host_total - host_available)
    return {
        "sample_ready": sample_ready,
        "sample_elapsed_seconds": round(elapsed_seconds, 3) if sample_ready else 0,
        "host": {
            "cpu_percent": round(host_cpu_percent, 3),
            "memory_total_bytes": host_total,
            "memory_available_bytes": host_available,
            "memory_used_bytes": host_memory_used,
            "memory_percent": round(
                min(100.0, (host_memory_used / host_total) * 100.0) if host_total else 0.0,
                3,
            ),
            "network_external_rx_bytes_per_second": round(total_rx_bps, 1),
            "network_external_tx_bytes_per_second": round(total_tx_bps, 1),
            "network_external_capacity_bytes_per_second": round(total_capacity_bps, 1),
            "network_external_percent": round(
                min(100.0, ((total_rx_bps + total_tx_bps) / total_capacity_bps) * 100.0)
                if total_capacity_bps
                else 0.0,
                3,
            ),
            "network_interfaces": interfaces,
        },
        "stacks": sorted(stacks.values(), key=lambda item: item["stack_name"].lower()),
    }


def _local_dockge_metrics_stream_worker() -> None:
    current_thread = threading.current_thread()
    containers: list[dict] = []
    last_map_at = 0.0
    previous_sample: dict | None = None
    try:
        while True:
            tick_started = time.monotonic()
            last_access = float(_LOCAL_DOCKGE_METRICS_STREAM.get("last_access") or 0.0)
            if (
                last_access
                and tick_started - last_access > _LOCAL_DOCKGE_METRICS_STREAM_IDLE_TIMEOUT
            ):
                return
            refreshed = False
            try:
                if not containers or tick_started - last_map_at > _LOCAL_DOCKGE_METRICS_MAP_TTL:
                    containers = _local_dockge_container_list()
                    last_map_at = tick_started
                    refreshed = True
                read_started = time.perf_counter()
                current_sample = _local_dockge_sample_raw(containers)
                probe_elapsed_ms = (time.perf_counter() - read_started) * 1000
                metrics = _local_dockge_stack_metrics_from_counters(
                    current_sample,
                    previous_sample,
                )
                payload = {
                    "ok": True,
                    "source": "cgroup-stream-local",
                    "sample_kind": "cgroup-counters-stream",
                    "probe_elapsed_ms": round(probe_elapsed_ms, 3),
                    "container_map_refreshed": refreshed,
                    "capacity": current_sample["capacity"],
                    "interval_seconds": 1,
                    "window_seconds": 10,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    **metrics,
                }
                previous_sample = current_sample
                with _LOCAL_DOCKGE_METRICS_STREAM_CONDITION:
                    now = time.monotonic()
                    _LOCAL_DOCKGE_METRICS_STREAM["payload"] = {
                        **payload,
                        "cache": "stream",
                    }
                    _LOCAL_DOCKGE_METRICS_STREAM["payload_at"] = now
                    _LOCAL_DOCKGE_METRICS_STREAM["error"] = None
                    _LOCAL_DOCKGE_METRICS_STREAM_CONDITION.notify_all()
                    idle_for = now - float(_LOCAL_DOCKGE_METRICS_STREAM.get("last_access") or 0.0)
                if idle_for > _LOCAL_DOCKGE_METRICS_STREAM_IDLE_TIMEOUT:
                    return
            except Exception as exc:
                with _LOCAL_DOCKGE_METRICS_STREAM_CONDITION:
                    _LOCAL_DOCKGE_METRICS_STREAM["error"] = str(exc)
                    _LOCAL_DOCKGE_METRICS_STREAM_CONDITION.notify_all()
            time.sleep(max(0.0, 1.0 - (time.monotonic() - tick_started)))
    finally:
        with _LOCAL_DOCKGE_METRICS_STREAM_CONDITION:
            if _LOCAL_DOCKGE_METRICS_STREAM.get("thread") is current_thread:
                _LOCAL_DOCKGE_METRICS_STREAM["thread"] = None
            _LOCAL_DOCKGE_METRICS_STREAM_CONDITION.notify_all()


def _ensure_local_dockge_metrics_stream_locked() -> None:
    thread = _LOCAL_DOCKGE_METRICS_STREAM.get("thread")
    if isinstance(thread, threading.Thread) and thread.is_alive():
        return
    _LOCAL_DOCKGE_METRICS_STREAM["error"] = None
    new_thread = threading.Thread(
        target=_local_dockge_metrics_stream_worker,
        name="local-dockge-metrics-stream",
        daemon=True,
    )
    _LOCAL_DOCKGE_METRICS_STREAM["thread"] = new_thread
    new_thread.start()


def _local_dockge_metrics_sync() -> dict:
    deadline = time.monotonic() + _LOCAL_DOCKGE_METRICS_STREAM_WAIT_TIMEOUT
    with _LOCAL_DOCKGE_METRICS_STREAM_CONDITION:
        _LOCAL_DOCKGE_METRICS_STREAM["last_access"] = time.monotonic()
        _ensure_local_dockge_metrics_stream_locked()
        while True:
            now = time.monotonic()
            payload = _LOCAL_DOCKGE_METRICS_STREAM.get("payload")
            payload_at = float(_LOCAL_DOCKGE_METRICS_STREAM.get("payload_at") or 0.0)
            stream_age = now - payload_at if payload_at > 0 else 0.0
            if (
                isinstance(payload, dict)
                and payload_at > 0
                and stream_age <= _LOCAL_DOCKGE_METRICS_STREAM_WAIT_TIMEOUT
            ):
                stream_age_ms = round(stream_age * 1000, 1)
                return {**payload, "stream_age_ms": stream_age_ms}

            remaining = deadline - now
            if remaining <= 0:
                detail = (
                    _LOCAL_DOCKGE_METRICS_STREAM.get("error")
                    or "metrics stream has not produced a sample yet"
                )
                raise HTTPException(504, f"Local Dockge metrics unavailable: {detail}")
            _LOCAL_DOCKGE_METRICS_STREAM_CONDITION.wait(timeout=min(0.1, remaining))


def _field(item: dict, *names: str) -> str:
    for name in names:
        value = item.get(name)
        if value is not None:
            return str(value)
    return ""


def _summarize_status(containers: list[dict]) -> tuple[str, str, int, int]:
    total = len(containers)
    running = sum(1 for item in containers if _field(item, "State", "state").lower() == "running")
    if total == 0:
        status = "stopped"
    elif running == total:
        status = "running"
    elif running == 0:
        status = "stopped"
    else:
        status = "partial"

    health_values = [
        _field(item, "Health", "health").lower()
        for item in containers
        if _field(item, "Health", "health").strip()
    ]
    if not health_values:
        health = "none"
    elif len(set(health_values)) == 1:
        health = health_values[0]
    else:
        health = "mixed"
    return status, health, running, total


def _normalize_stack(
    stack_dir: Path,
    compose: Path,
    ps_items: list[dict],
    compose_config: dict | None = None,
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
    exposures = _base_exposures(services_sorted, containers, compose_services, caddy_routes or [])
    exposures, manifest_error = _apply_manifest_exposures(
        exposures, _read_exposure_manifest(stack_dir)
    )
    status, health, running, total = _summarize_status(containers)
    stack = {
        "stack_name": stack_dir.name,
        "path": str(stack_dir),
        "compose_file": compose.name,
        "status": status,
        "health": health,
        "running": running,
        "total": total,
        "services": services_sorted,
        "service_exposures": exposures,
        "containers": containers,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if manifest_error:
        stack["exposure_manifest_error"] = manifest_error
    return stack


def _inspect_stack(stack_name: str) -> dict:
    stack_dir, compose = _safe_stack_dir(stack_name)
    result = _run_compose(stack_dir, compose, ["ps", "--all", "--format", "json"], timeout=20)
    if result.returncode != 0:
        raise HTTPException(
            500,
            f"docker compose ps failed for {stack_name}: {result.stderr.strip() or result.stdout.strip() or '(no output)'}",
        )
    compose_config, _ = _compose_config(stack_dir, compose)
    return _normalize_stack(
        stack_dir, compose, _parse_compose_ps(result.stdout), compose_config, _parse_caddy_routes()
    )


def _compose_env_references(compose: Path) -> dict:
    try:
        text = compose.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {"referenced": [], "required": []}
    referenced = set()
    required = set()
    for match in _COMPOSE_ENV_PATTERN.finditer(text):
        name = match.group("name")
        op = match.group("op") or ""
        referenced.add(name)
        if op.startswith(":?") or op.startswith("?"):
            required.add(name)
    return {"referenced": sorted(referenced), "required": sorted(required)}


def _env_file_presence(stack_dir: Path) -> dict[str, bool]:
    env_path = stack_dir / ".env"
    if not env_path.is_file():
        return {}
    presence: dict[str, bool] = {}
    try:
        lines = env_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return presence
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            presence[key] = bool(value.strip().strip('"').strip("'"))
    return presence


def _stack_env_requirements(stack_dir: Path, compose: Path) -> dict:
    refs = _compose_env_references(compose)
    presence = _env_file_presence(stack_dir)
    required = refs["required"]
    return {
        "referenced": refs["referenced"],
        "required": required,
        "missing_required": [name for name in required if not presence.get(name, False)],
    }


def _inspect_stack_lenient(stack_name: str) -> tuple[dict, Path, Path]:
    stack_dir, compose = _safe_stack_dir(stack_name)
    caddy_routes = _parse_caddy_routes()
    result = _run_compose(stack_dir, compose, ["ps", "--all", "--format", "json"], timeout=20)
    compose_config, config_error = _compose_config(stack_dir, compose)
    if result.returncode == 0:
        stack = _normalize_stack(
            stack_dir, compose, _parse_compose_ps(result.stdout), compose_config, caddy_routes
        )
        stack["env_requirements"] = _stack_env_requirements(stack_dir, compose)
        if config_error:
            stack["compose_config_error"] = config_error
        return stack, stack_dir, compose

    compose_services = _service_configs(compose_config)
    services = sorted(compose_services)
    exposures = _base_exposures(services, [], compose_services, caddy_routes)
    exposures, manifest_error = _apply_manifest_exposures(
        exposures, _read_exposure_manifest(stack_dir)
    )
    stack = {
        "stack_name": stack_dir.name,
        "path": str(stack_dir),
        "compose_file": compose.name,
        "status": "unknown",
        "health": "unknown",
        "running": 0,
        "total": 0,
        "services": services,
        "service_exposures": exposures,
        "containers": [],
        "error": result.stderr.strip() or result.stdout.strip() or "docker compose ps failed",
        "compose_config_error": config_error,
        "env_requirements": _stack_env_requirements(stack_dir, compose),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if manifest_error:
        stack["exposure_manifest_error"] = manifest_error
    return stack, stack_dir, compose


def _local_dockge_speech_env_int(
    name: str, default: int, minimum: int = 0, maximum: int = 1000000
) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("local Dockge narration: invalid %s=%r; using %s", name, raw, default)
        return default
    return max(minimum, min(maximum, value))


def _safe_stack_cache_name(stack_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", stack_name.strip()).strip(".-") or "stack"


def _local_dockge_speech_cache_dir(stack_name: str) -> Path:
    return _LOCAL_DOCKGE_SPEECH_CACHE_ROOT / _safe_stack_cache_name(stack_name)


def _local_dockge_speech_cache_candidates(stack_name: str) -> list[Path]:
    cache_dir = _local_dockge_speech_cache_dir(stack_name)
    if not cache_dir.is_dir():
        return []
    candidates = [p for p in cache_dir.iterdir() if p.is_file() and p.name.endswith(".txt")]
    return sorted(candidates, key=lambda p: (p.stat().st_mtime, p.name), reverse=True)


def _local_dockge_speech_cache_meta_path(cache_path: Path) -> Path:
    return cache_path.with_name(f"{cache_path.name}.meta.json")


def _new_local_dockge_speech_cache_path(stack_name: str) -> Path:
    cache_dir = _local_dockge_speech_cache_dir(stack_name)
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
    raise HTTPException(500, "Could not allocate a unique local Dockge narration cache path")


def _invalidate_local_dockge_speech_cache(stack_name: str) -> None:
    for cached in _local_dockge_speech_cache_candidates(stack_name):
        try:
            cached.unlink()
        except OSError as exc:
            log.warning("local Dockge narration: could not remove stale cache %s: %s", cached, exc)
        meta_path = _local_dockge_speech_cache_meta_path(cached)
        try:
            if meta_path.exists():
                meta_path.unlink()
        except OSError as exc:
            log.warning(
                "local Dockge narration: could not remove stale cache metadata %s: %s",
                meta_path,
                exc,
            )


def _read_local_dockge_source(
    path: Path, kind: str, source_items: list[dict], seen: set[Path]
) -> None:
    try:
        resolved = path.resolve()
    except OSError:
        return
    if resolved in seen or not resolved.is_file():
        return
    if resolved.name == ".env":
        return
    if (
        resolved.suffix.lower() not in _LOCAL_DOCKGE_SOURCE_SUFFIXES
        and resolved.name.lower() != "caddyfile"
    ):
        return
    try:
        stat = resolved.stat()
        text = resolved.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        log.warning("local Dockge narration: could not read source %s: %s", resolved, exc)
        return
    seen.add(resolved)
    clipped = len(text) > _LOCAL_DOCKGE_SPEECH_FILE_LIMIT
    source_items.append(
        {
            "path": str(resolved),
            "kind": kind,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "clipped": clipped,
            "text": text[:_LOCAL_DOCKGE_SPEECH_FILE_LIMIT],
        }
    )


def _dockge_skill_roots() -> list[Path]:
    roots = []
    for raw_root in (
        cfg.REPO_OUTER_PATH,
        cfg.REPO_INNER_PATH,
        cfg.REPO_NON_ROOT_PATH,
        str(_NODE_LOCAL_ROOT),
    ):
        if not raw_root:
            continue
        skill_root = Path(raw_root).expanduser() / ".claude" / "skills"
        if skill_root.is_dir():
            roots.append(skill_root)
    return roots


def _gather_local_dockge_speech_sources(
    stack_name: str, stack_dir: Path, compose: Path
) -> list[dict]:
    source_items: list[dict] = []
    seen: set[Path] = set()
    stack_l = stack_name.lower()

    _read_local_dockge_source(compose, "stack-compose", source_items, seen)
    for name in _EXPOSURE_FILENAMES:
        _read_local_dockge_source(stack_dir / name, "stack-exposure-manifest", source_items, seen)
    for name in ("README.md", "NOTES.md", "OPERATIONS.md"):
        _read_local_dockge_source(stack_dir / name, "stack-note", source_items, seen)
    docs_dir = stack_dir / "docs"
    if docs_dir.is_dir():
        for path in sorted(docs_dir.rglob("*"))[:24]:
            _read_local_dockge_source(path, "stack-doc", source_items, seen)
    stack_skill_root = stack_dir / ".claude" / "skills"
    if stack_skill_root.is_dir():
        for path in sorted(stack_skill_root.rglob("*"))[:32]:
            if path.name == "SKILL.md" or path.parent.name == "docs":
                _read_local_dockge_source(path, "stack-skill", source_items, seen)

    dockge_docs = _NODE_LOCAL_ROOT / "docs" / "dockge"
    for name in (
        "LOCAL-DOCKGE-BLUEPRINTS-CONTROL.md",
        f"{stack_name}.md",
        f"{stack_name.upper()}.md",
        f"{stack_name.replace('-', '_')}.md",
    ):
        _read_local_dockge_source(dockge_docs / name, "node-local-dockge-doc", source_items, seen)
    if dockge_docs.is_dir():
        for path in sorted(dockge_docs.glob("*.md")):
            if stack_l in path.stem.lower():
                _read_local_dockge_source(path, "node-local-dockge-doc", source_items, seen)

    for skill_root in _dockge_skill_roots():
        for skill_dir in sorted(
            (p for p in skill_root.iterdir() if p.is_dir()), key=lambda p: p.name.lower()
        ):
            name_l = skill_dir.name.lower()
            if "dockge" not in name_l and stack_l not in name_l:
                continue
            _read_local_dockge_source(skill_dir / "SKILL.md", "workspace-skill", source_items, seen)
            if len(source_items) >= 48:
                break

    caddyfile = Path(cfg.LOCAL_DOCKGE_CADDYFILE)
    _read_local_dockge_source(caddyfile, "local-caddy-config", source_items, seen)
    return source_items


def _stack_condition_for_cache(stack: dict) -> dict:
    clean = json.loads(json.dumps(stack, default=str))
    clean.pop("updated_at", None)
    clean.pop("path", None)
    clean.pop("error", None)
    clean.pop("compose_config_error", None)
    for exposure in (clean.get("service_exposures") or {}).values():
        if isinstance(exposure, dict):
            route = exposure.get("route")
            if isinstance(route, dict):
                route.pop("site", None)
    return clean


def _local_dockge_speech_fingerprint(stack: dict, source_items: list[dict]) -> str:
    payload = {
        "prompt_version": _LOCAL_DOCKGE_SPEECH_PROMPT_VERSION,
        "stack": _stack_condition_for_cache(stack),
        "sources": [
            {
                "path": item.get("path"),
                "kind": item.get("kind"),
                "size": item.get("size"),
                "mtime_ns": item.get("mtime_ns"),
                "clipped": item.get("clipped"),
            }
            for item in source_items
        ],
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _valid_local_dockge_speech_cache_path(stack_name: str, fingerprint: str) -> Path | None:
    for candidate in _local_dockge_speech_cache_candidates(stack_name):
        meta_path = _local_dockge_speech_cache_meta_path(candidate)
        if not meta_path.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if meta.get("fingerprint") == fingerprint:
            return candidate
    return None


def _local_dockge_source_markdown(stack: dict, source_items: list[dict]) -> str:
    parts = [
        "# Local Dockge stack condition",
        "",
        "Live stack inspection JSON:",
        "```json",
        json.dumps(_stack_condition_for_cache(stack), indent=2, sort_keys=True, default=str),
        "```",
    ]
    for item in source_items:
        parts.extend(
            [
                "",
                f"## Source: {item['kind']} — {item['path']}",
                "",
                item["text"],
            ]
        )
    return "\n".join(parts)


_LOCAL_DOCKGE_SPEECH_SYSTEM_PROMPT = """
You write concise spoken condition reports for a local Dockge stacks page.

Use the supplied live inspection JSON first. Use the docs, skills, compose files, Caddy config, and stack notes only as supporting context.

Rules:
- Output plain text only. No Markdown headings, bullets, tables, code fences, citations, or source labels.
- Start with the stack name and its current observed status and health.
- Explain non-ideal conditions when present: mixed health, unknown health, stopped containers, starting containers, exited containers, compose errors, Caddy exposure issues, or missing runtime information.
- Be grounded and honest. Distinguish observed facts from likely causes. If the context does not prove why something happened, say that it is not clear from the available evidence.
- Mention useful next checks only when they follow directly from the supplied docs or status data.
- Do not reveal secrets or environment values. If a missing environment variable is named in an error, mention only the variable name and the effect.
- Prefer service names and human descriptions over raw URLs. Mention a hostname, port, path, or status code only when it explains the condition or a useful next check.
- Preserve acronyms, product names, status codes, and identifiers as ordinary text. The final speech sanitizer handles pronunciation for URL, API, HTTP, OpenAPI, PostgreSQL, IP addresses, status codes, and joined tokens.
- Do not pre-pronounce technical punctuation. Write `.env`, `.gitignored`, paths, hostnames, URLs, and `host:port` values as ordinary text when they are important; never write forms like "dot env", "dot dot env", "colon 8080", or hand-spelled URL punctuation.
- Keep the existing condition-report style: precise, operational, and useful. Do not replace it with a generic service description.
- After the condition report, add a concise closing description of what the stack is meant to do. Cover its stand-alone purpose first, then its role in the wider xarta-node or Blueprints local systems. Mention the main web/API/user-facing capability and any important relationship to docs, search, AI, TTS, agents, Caddy, tailnet, or other stacks when the supplied context supports it.
- If the supplied docs and skills do not explain the stack's purpose or wider role, say that the available local context does not describe its intended role clearly.
- Aim for roughly 180 to 420 spoken words total. It can be longer for complex or unhealthy stacks, but stay concise enough for row-level playback and MP3 generation.
""".strip()


async def _generate_local_dockge_speech_markdown(
    stack: dict, source_items: list[dict]
) -> tuple[str, dict[str, Any]]:
    source_limit = _local_dockge_speech_env_int(
        "LOCAL_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT",
        _LOCAL_DOCKGE_SPEECH_SOURCE_LIMIT,
        minimum=0,
        maximum=1000000,
    )
    try:
        prepared_source = await prepare_tts_markdown_for_llm_via_service(
            _local_dockge_source_markdown(stack, source_items)
        )
    except TtsSanitizerUnavailable as exc:
        raise HTTPException(503, str(exc)) from exc
    speech_source, source_meta = _clamp_source_markdown(prepared_source, limit=source_limit)
    if source_meta.get("source_clipped"):
        raise HTTPException(
            413,
            (
                "Local Dockge narration source exceeded LOCAL_DOCKGE_SPEECH_SOURCE_CHAR_LIMIT "
                f"({source_meta.get('source_chars')} chars > {source_meta.get('source_char_limit')})."
            ),
        )
    user_prompt = (
        "/no-think\n"
        f"Stack: {stack.get('stack_name')}\n\n"
        "Write a spoken condition report from this gathered local context:\n\n"
        f"{speech_source}"
    )
    answer, llm_meta = await _complete_doc_speech_local(
        [
            {"role": "system", "content": _LOCAL_DOCKGE_SPEECH_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        operation="local-dockge:narration",
    )
    generation_meta = {**source_meta, **llm_meta}
    if generation_meta.get("finish_reason") == "length":
        raise HTTPException(
            502,
            "Local Dockge narration hit DOC_SPEECH_LLM_MAX_TOKENS before completing. Increase the limit and regenerate.",
        )
    speech = await _clean_doc_speech_markdown(str(answer or ""))
    if not speech:
        raise HTTPException(502, "Local LLM returned an empty local Dockge narration")
    generation_meta.update(
        {
            "speech_chars": len(speech),
            "speech_words": len(speech.split()),
            "source_count": len(source_items),
            "prompt_version": _LOCAL_DOCKGE_SPEECH_PROMPT_VERSION,
        }
    )
    return speech, generation_meta


def _find_service_exposure(stack_name: str, service_name: str) -> tuple[dict, dict]:
    stack = _inspect_stack(stack_name)
    exposures = stack.get("service_exposures") or {}
    exposure = exposures.get(service_name)
    if not exposure:
        raise HTTPException(404, f"service '{service_name}' not found in stack '{stack_name}'")
    return stack, exposure


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
        for item in cfg.LOCAL_DOCKGE_PROBE_ALLOWED_HOST_SUFFIXES.split(",")
        if item.strip()
    ]
    return any(host.endswith(suffix) for suffix in suffixes)


def _candidate_url(base_url: str, suffix: str) -> str:
    return urljoin(base_url.rstrip("/") + "/", suffix)


async def _probe_text(client: httpx.AsyncClient, url: str, accept: str = "*/*") -> dict:
    if not _is_probe_url_allowed(url):
        return {"url": url, "ok": False, "status": None, "error": "probe URL not allowed"}
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


def _summarize_openapi(spec: dict, url: str) -> dict:
    info = spec.get("info") if isinstance(spec.get("info"), dict) else {}
    paths = spec.get("paths") if isinstance(spec.get("paths"), dict) else {}
    path_rows = []
    for path, methods in sorted(paths.items())[:80]:
        if isinstance(methods, dict):
            method_list = [
                method.upper()
                for method in methods
                if method.lower() in {"get", "post", "put", "patch", "delete"}
            ]
        else:
            method_list = []
        path_rows.append({"path": path, "methods": method_list})
    return {
        "url": url,
        "title": info.get("title") or "OpenAPI",
        "version": info.get("version") or "",
        "description": (info.get("description") or "")[:1200],
        "path_count": len(paths),
        "paths": path_rows,
    }


@router.get("/stacks", status_code=200)
async def list_local_dockge_stacks() -> dict:
    return await asyncio.to_thread(_list_local_dockge_stacks_sync)


@router.get("/metrics", status_code=200)
async def local_dockge_metrics() -> dict:
    return await asyncio.to_thread(_local_dockge_metrics_sync)


def _list_local_dockge_stacks_sync() -> dict:
    root = _stacks_root()
    if not root.is_dir():
        raise HTTPException(404, f"local Dockge stacks directory not found: {root}")

    caddy_routes = _parse_caddy_routes()
    stacks = []
    for stack_dir in sorted(
        (p for p in root.iterdir() if p.is_dir()), key=lambda p: p.name.lower()
    ):
        compose = _compose_file(stack_dir)
        if not compose:
            continue
        result = _run_compose(stack_dir, compose, ["ps", "--all", "--format", "json"], timeout=20)
        compose_config, config_error = _compose_config(stack_dir, compose)
        if result.returncode != 0:
            stacks.append(
                {
                    "stack_name": stack_dir.name,
                    "path": str(stack_dir),
                    "compose_file": compose.name,
                    "status": "unknown",
                    "health": "unknown",
                    "running": 0,
                    "total": 0,
                    "services": [],
                    "service_exposures": {},
                    "containers": [],
                    "error": result.stderr.strip()
                    or result.stdout.strip()
                    or "docker compose ps failed",
                    "compose_config_error": config_error,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            continue
        stack = _normalize_stack(
            stack_dir, compose, _parse_compose_ps(result.stdout), compose_config, caddy_routes
        )
        if config_error:
            stack["compose_config_error"] = config_error
        stacks.append(stack)

    return {
        "ok": True,
        "dockge_url": cfg.LOCAL_DOCKGE_BASE_URL,
        "stacks_dir": str(root),
        "caddyfile": cfg.LOCAL_DOCKGE_CADDYFILE,
        "caddy_routes": caddy_routes,
        "stacks": stacks,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/stacks/{stack_name}/services/{service_name}/info", status_code=200)
async def local_dockge_service_info(stack_name: str, service_name: str) -> dict:
    stack, exposure = await asyncio.to_thread(_find_service_exposure, stack_name, service_name)
    probe_http = _kind_supports_http_probes(exposure.get("kind"))
    base_url = _http_url(exposure.get("url")) if probe_http else ""
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

    seen = set()
    openapi_candidates = [url for url in openapi_candidates if not (url in seen or seen.add(url))]
    seen = set()
    docs_candidates = [url for url in docs_candidates if not (url in seen or seen.add(url))]

    openapi_summary = None
    openapi_checks = []
    docs_checks = []
    home_check = None
    if probe_http:
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
                        exposure["kind"] = (
                            "caddy-api"
                            if exposure.get("kind") == "caddy-web"
                            else exposure.get("kind")
                        )
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
            "status": "todo" if probe_http else "skipped",
            "detail": "Per-stack smoke tests can be recorded in xarta-service-exposure.yaml and surfaced here later."
            if probe_http
            else "HTTP, OpenAPI, and docs probes skipped because this service is declared non-HTTP.",
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/stacks/{stack_name}/speech", status_code=200)
async def local_dockge_stack_speech(
    stack_name: str, body: LocalDockgeSpeechBody | None = None
) -> dict:
    force = bool(body.force) if body else False
    stack, stack_dir, compose = _inspect_stack_lenient(stack_name)
    source_items = _gather_local_dockge_speech_sources(stack["stack_name"], stack_dir, compose)
    fingerprint = _local_dockge_speech_fingerprint(stack, source_items)

    if force:
        _invalidate_local_dockge_speech_cache(stack["stack_name"])
    elif cache_path := _valid_local_dockge_speech_cache_path(stack["stack_name"], fingerprint):
        try:
            speech = cache_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise HTTPException(500, f"Could not read local Dockge narration cache: {exc}") from exc
        speech_meta = None
        meta_path = _local_dockge_speech_cache_meta_path(cache_path)
        if meta_path.is_file():
            try:
                speech_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                log.warning(
                    "local Dockge narration: could not read cache metadata %s: %s", meta_path, exc
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

    speech, speech_meta = await _generate_local_dockge_speech_markdown(stack, source_items)
    cache_path = _new_local_dockge_speech_cache_path(stack["stack_name"])
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
        "prompt_version": _LOCAL_DOCKGE_SPEECH_PROMPT_VERSION,
        "generated_at": cache_path.name.split("--", 1)[0],
        "sources": source_summary,
        "condition": _stack_condition_for_cache(stack),
    }
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(speech + "\n", encoding="utf-8")
        _local_dockge_speech_cache_meta_path(cache_path).write_text(
            json.dumps(speech_meta, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _normalize_node_local_ownership(cache_path.parent)
        _normalize_node_local_ownership(cache_path)
        _normalize_node_local_ownership(_local_dockge_speech_cache_meta_path(cache_path))
    except OSError as exc:
        raise HTTPException(500, f"Could not write local Dockge narration cache: {exc}") from exc

    return {
        "ok": True,
        "stack": stack["stack_name"],
        "cache": "regenerated" if force else "miss",
        "speech_path": str(cache_path),
        "generated_at": cache_path.name.split("--", 1)[0],
        "generation": speech_meta,
        "markdown": speech,
    }


@router.post("/stacks/{stack_name}/action", status_code=200)
async def local_dockge_stack_action(stack_name: str, body: LocalDockgeAction) -> dict:
    action = body.action.strip().lower()
    if action not in _VALID_ACTIONS:
        raise HTTPException(400, f"invalid action '{action}'; must be start, stop, or restart")
    return await asyncio.to_thread(_local_dockge_stack_action_sync, stack_name, action)


def _local_dockge_stack_action_sync(stack_name: str, action: str) -> dict:
    stack_dir, compose = _safe_stack_dir(stack_name)
    if action == "start":
        args = ["up", "-d"]
    elif action == "stop":
        args = ["stop"]
    else:
        args = ["restart"]

    result = _run_compose(stack_dir, compose, args, timeout=60)
    if result.returncode != 0:
        raise HTTPException(
            500,
            f"docker compose {action} failed for {stack_name}: {result.stderr.strip() or result.stdout.strip() or '(no output)'}",
        )

    log.info("local Dockge stack %s: %s succeeded", stack_name, action)
    return {
        "ok": True,
        "action": action,
        "stack": stack_name,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
        "result": _inspect_stack(stack_name),
    }
