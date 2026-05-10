"""routes_local_dockge.py — local Dockge stack status/control endpoints.

These routes intentionally expose a narrow Blueprints-controlled surface over
the node-local Dockge stacks directory. Browsers call this through the normal
Caddy-served Blueprints API instead of talking to Dockge's loopback-only port.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx
import yaml
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg

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


class LocalDockgeAction(BaseModel):
    action: str


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


def _run_compose(stack_dir: Path, compose: Path, args: list[str], timeout: int = 30) -> subprocess.CompletedProcess[str]:
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
        published = _normalize_port(publisher.get("PublishedPort") or publisher.get("published_port"))
        target = _normalize_port(publisher.get("TargetPort") or publisher.get("target_port"))
        if not published and not target:
            continue
        ports.append({
            "host_ip": str(publisher.get("URL") or publisher.get("url") or ""),
            "published": published,
            "target": target,
            "protocol": str(publisher.get("Protocol") or publisher.get("protocol") or "tcp"),
        })
    return ports


def _ports_from_string(value: str) -> list[dict]:
    ports = []
    pattern = re.compile(
        r"(?:(?P<host_ip>127\.0\.0\.1|0\.0\.0\.0|\[::\]|::|localhost):)?"
        r"(?P<published>\d+)->(?P<target>\d+)/(?P<protocol>\w+)"
    )
    for match in pattern.finditer(value or ""):
        ports.append({
            "host_ip": match.group("host_ip") or "",
            "published": _normalize_port(match.group("published")),
            "target": _normalize_port(match.group("target")),
            "protocol": match.group("protocol") or "tcp",
        })
    return ports


def _ports_from_compose_service(service_config: dict) -> list[dict]:
    ports = []
    for item in service_config.get("ports") or []:
        if isinstance(item, dict):
            published = _normalize_port(item.get("published"))
            target = _normalize_port(item.get("target"))
            if not published and not target:
                continue
            ports.append({
                "host_ip": str(item.get("host_ip") or ""),
                "published": published,
                "target": target,
                "protocol": str(item.get("protocol") or "tcp"),
            })
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
    proxy_re = re.compile(r"reverse_proxy\s+(?:https?://)?(?:localhost|127\.0\.0\.1|\[::1\]):(?P<port>\d+)")

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
                    routes.append({
                        "host": urlparse(host).netloc or host,
                        "site": host,
                        "path": current_path,
                        "url": _route_url(host, current_path),
                        "upstream": f"localhost:{port}",
                        "upstream_port": port,
                        "source": "caddy",
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
    host_ports = [port for port in ports if port.get("published")]
    if host_ports:
        return "localhost-api" if "api" in service_l else "localhost-web"
    return "internal"


def _base_exposures(
    services: list[str],
    containers: list[dict],
    compose_services: dict,
    caddy_routes: list[dict],
) -> dict[str, dict]:
    by_service = {container.get("service"): container for container in containers if container.get("service")}
    exposures: dict[str, dict] = {}
    for service in services:
        container = by_service.get(service)
        ports = _service_ports(service, container, compose_services)
        route = _route_for_ports(ports, caddy_routes)
        kind = _infer_kind(service, route, ports)
        url = route.get("url") if route else None
        if not url and ports:
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


def _apply_manifest_exposures(exposures: dict[str, dict], manifest: dict) -> tuple[dict[str, dict], str | None]:
    if not manifest:
        return exposures, None
    manifest_error = manifest.get("_error")
    services = manifest.get("services") if isinstance(manifest.get("services"), dict) else {}
    for service, raw in services.items():
        if not isinstance(raw, dict):
            continue
        base = exposures.setdefault(str(service), {
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
        })
        base_url = raw.get("url") or base.get("url") or ""
        for key in ("label", "kind", "description", "notes", "tests_todo"):
            if raw.get(key) is not None:
                base[key] = raw.get(key)
        if raw.get("url") is not None:
            base["url"] = raw.get("url")
        base["open_url"] = raw.get("open_url") or (base.get("url") if base.get("kind") in {"caddy-web", "caddy-api"} else None)
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
    exposures = _base_exposures(services_sorted, containers, compose_services, caddy_routes or [])
    exposures, manifest_error = _apply_manifest_exposures(exposures, _read_exposure_manifest(stack_dir))
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
    return _normalize_stack(stack_dir, compose, _parse_compose_ps(result.stdout), compose_config, _parse_caddy_routes())


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
            method_list = [method.upper() for method in methods if method.lower() in {"get", "post", "put", "patch", "delete"}]
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
    root = _stacks_root()
    if not root.is_dir():
        raise HTTPException(404, f"local Dockge stacks directory not found: {root}")

    caddy_routes = _parse_caddy_routes()
    stacks = []
    for stack_dir in sorted((p for p in root.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        compose = _compose_file(stack_dir)
        if not compose:
            continue
        result = _run_compose(stack_dir, compose, ["ps", "--all", "--format", "json"], timeout=20)
        compose_config, config_error = _compose_config(stack_dir, compose)
        if result.returncode != 0:
            stacks.append({
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
                "error": result.stderr.strip() or result.stdout.strip() or "docker compose ps failed",
                "compose_config_error": config_error,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            continue
        stack = _normalize_stack(stack_dir, compose, _parse_compose_ps(result.stdout), compose_config, caddy_routes)
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
    stack, exposure = _find_service_exposure(stack_name, service_name)
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

    seen = set()
    openapi_candidates = [url for url in openapi_candidates if not (url in seen or seen.add(url))]
    seen = set()
    docs_candidates = [url for url in docs_candidates if not (url in seen or seen.add(url))]

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
                    exposure["kind"] = "caddy-api" if exposure.get("kind") == "caddy-web" else exposure.get("kind")
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
            "detail": "Per-stack smoke tests can be recorded in xarta-service-exposure.yaml and surfaced here later.",
        },
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/stacks/{stack_name}/action", status_code=200)
async def local_dockge_stack_action(stack_name: str, body: LocalDockgeAction) -> dict:
    action = body.action.strip().lower()
    if action not in _VALID_ACTIONS:
        raise HTTPException(400, f"invalid action '{action}'; must be start, stop, or restart")

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
