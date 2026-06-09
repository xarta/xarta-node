"""PVE fast-health aggregator for deterministic Wake STT health answers."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import ipaddress
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx

CONFIG_SCHEMA = "xarta.pve-fast-health.hosts.v1"
RESULT_SCHEMA = "xarta.pve-fast-health.result.v1"
DEFAULT_CONFIG_FILE = "/xarta-node/.lone-wolf/config/pve-fast-health/hosts.json"
OK_SPEECH = "I am functioning within normal parameters."


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def elapsed_ms(started: float) -> int:
    return int(round((time.perf_counter() - started) * 1000))


def config_file() -> Path:
    raw = os.getenv("BLUEPRINTS_PVE_FAST_HEALTH_CONFIG_FILE", DEFAULT_CONFIG_FILE)
    return Path(str(raw).strip() or DEFAULT_CONFIG_FILE)


def _clamp_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _valid_ip(value: Any) -> str:
    text = _safe_str(value)
    ipaddress.ip_address(text)
    return text


def load_config(path: Path | None = None) -> tuple[dict[str, Any] | None, str, str]:
    cfg_path = path or config_file()
    try:
        raw = json.loads(cfg_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None, "missing", str(cfg_path)
    except (OSError, json.JSONDecodeError) as exc:
        return {"error": str(exc)}, "invalid", str(cfg_path)
    if not isinstance(raw, dict):
        return {"error": "config must be a JSON object"}, "invalid", str(cfg_path)
    if raw.get("schema") not in {CONFIG_SCHEMA, None}:
        return {"error": f"unsupported schema: {raw.get('schema')}"}, "invalid", str(cfg_path)
    raw.setdefault("schema", CONFIG_SCHEMA)
    return raw, "configured", str(cfg_path)


async def aggregate_fast_health(
    *,
    intent: str = "operator_query",
    config_path: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    config, config_status, path_text = load_config(config_path)
    if config_status != "configured" or not config:
        result = {
            "schema": RESULT_SCHEMA,
            "ok": False,
            "status": "unknown",
            "checked_at": utc_now(),
            "duration_ms": elapsed_ms(started),
            "deadline_ms": 0,
            "intent": intent,
            "config_status": config_status,
            "config_path": path_text,
            "hosts": [],
            "isp_routes": [],
        }
        if isinstance(config, dict) and config.get("error"):
            result["error"] = config["error"]
        result["speech"] = speech_for_result(result)
        result["matrix_detail"] = matrix_detail_for_result(result)
        return result

    deadline_ms = _clamp_int(config.get("timeout_ms"), default=2000, minimum=200, maximum=2000)
    pve_timeout_ms = _clamp_int(
        config.get("pve_check_timeout_ms"),
        default=min(1800, deadline_ms),
        minimum=100,
        maximum=min(1800, deadline_ms),
    )
    source_ip = _source_ip(config)
    endpoint_path = _endpoint_path(config.get("pve_endpoint_path"))
    tasks: dict[asyncio.Task[dict[str, Any]], tuple[str, dict[str, Any]]] = {}
    for host in _enabled_items(config.get("hosts")):
        task = asyncio.create_task(
            _probe_host(
                host,
                source_ip=source_ip,
                endpoint_path=_endpoint_path(host.get("endpoint_path") or endpoint_path),
                timeout_ms=pve_timeout_ms,
                intent=intent,
            )
        )
        tasks[task] = ("host", host)
    for route in _enabled_items(config.get("isp_routes")):
        task = asyncio.create_task(_probe_isp_route(route, source_ip=source_ip))
        tasks[task] = ("route", route)

    hosts: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    if tasks:
        done, pending = await asyncio.wait(
            set(tasks),
            timeout=max(0.05, (deadline_ms - elapsed_ms(started)) / 1000),
        )
        for task in done:
            kind, item = tasks[task]
            try:
                result = task.result()
            except Exception as exc:  # noqa: BLE001
                result = _failed_task_result(kind, item, exc, deadline_ms)
            if kind == "host":
                hosts.append(result)
            else:
                routes.append(result)
        for task in pending:
            task.cancel()
            kind, item = tasks[task]
            result = _timeout_task_result(kind, item, deadline_ms)
            if kind == "host":
                hosts.append(result)
            else:
                routes.append(result)
    status = aggregate_status(hosts, routes)
    result = {
        "schema": RESULT_SCHEMA,
        "ok": status == "ok",
        "status": status,
        "checked_at": utc_now(),
        "duration_ms": elapsed_ms(started),
        "deadline_ms": deadline_ms,
        "pve_check_timeout_ms": pve_timeout_ms,
        "intent": intent,
        "speech_allowed": intent != "poll",
        "notifier_policy": "direct_response" if intent != "poll" else "suppress_speech",
        "config_status": config_status,
        "config_path": path_text,
        "hosts": sorted(hosts, key=lambda item: str(item.get("id") or "")),
        "isp_routes": sorted(routes, key=lambda item: str(item.get("id") or "")),
    }
    result["speech"] = speech_for_result(result)
    result["matrix_detail"] = matrix_detail_for_result(result)
    return result


def _source_ip(config: dict[str, Any]) -> str | None:
    raw = _safe_str(config.get("source_ip"))
    if not raw:
        return None
    try:
        return _valid_ip(raw)
    except ValueError:
        return None


def _endpoint_path(value: Any) -> str:
    text = _safe_str(value) or "/health"
    if not text.startswith("/"):
        text = "/" + text
    if "?" in text or "#" in text:
        text = text.split("?", 1)[0].split("#", 1)[0]
    return text or "/health"


def _enabled_items(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    return [
        item for item in raw if isinstance(item, dict) and item.get("enabled", True) is not False
    ]


async def _probe_host(
    host: dict[str, Any],
    *,
    source_ip: str | None,
    endpoint_path: str,
    timeout_ms: int,
    intent: str,
) -> dict[str, Any]:
    started = time.perf_counter()
    host_id = _safe_str(host.get("id")) or "unknown-host"
    try:
        ip = _valid_ip(host.get("ip"))
        port = _clamp_int(host.get("port"), default=17871, minimum=1, maximum=65535)
    except ValueError as exc:
        return {
            "id": host_id,
            "ip": _safe_str(host.get("ip")),
            "status": "fail",
            "ok": False,
            "duration_ms": elapsed_ms(started),
            "failure_kind": "config",
            "error": str(exc),
        }
    params = urlencode({"intent": intent, "deadline_ms": timeout_ms})
    url = f"http://{ip}:{port}{endpoint_path}?{params}"
    timeout_s = max(0.05, timeout_ms / 1000)
    try:
        async with _http_client(timeout_s=timeout_s, source_ip=source_ip) as client:
            response = await client.get(url)
            body = response.json()
    except (httpx.TimeoutException, TimeoutError) as exc:
        return _host_failure(host_id, ip, elapsed_ms(started), "timeout", str(exc), timeout_ms)
    except Exception as exc:  # noqa: BLE001
        return _host_failure(host_id, ip, elapsed_ms(started), "connection", str(exc), timeout_ms)
    if not isinstance(body, dict):
        return _host_failure(
            host_id, ip, elapsed_ms(started), "invalid_json", "body was not an object", timeout_ms
        )
    status = _normal_status(body.get("status"))
    if response.status_code != 200:
        status = "fail"
    return {
        "id": host_id,
        "ip": ip,
        "status": status,
        "ok": status == "ok",
        "duration_ms": elapsed_ms(started),
        "http_status": response.status_code,
        "checks": body.get("checks") if isinstance(body.get("checks"), dict) else {},
        "pve_duration_ms": body.get("duration_ms"),
        "checked_at": body.get("checked_at"),
    }


def _http_client(*, timeout_s: float, source_ip: str | None) -> httpx.AsyncClient:
    timeout = httpx.Timeout(timeout_s, connect=min(0.45, timeout_s))
    if source_ip:
        transport = httpx.AsyncHTTPTransport(local_address=source_ip, retries=0)
        return httpx.AsyncClient(timeout=timeout, transport=transport, trust_env=False)
    return httpx.AsyncClient(timeout=timeout, trust_env=False)


def _host_failure(
    host_id: str,
    ip: str,
    duration_ms: int,
    failure_kind: str,
    error: str,
    timeout_ms: int,
) -> dict[str, Any]:
    status = "timeout" if failure_kind == "timeout" else "fail"
    message = (
        f"did not respond within {timeout_ms} ms" if failure_kind == "timeout" else error[:240]
    )
    return {
        "id": host_id,
        "ip": ip,
        "status": status,
        "ok": False,
        "duration_ms": duration_ms,
        "failure_kind": failure_kind,
        "error": message,
    }


async def _probe_isp_route(route: dict[str, Any], *, source_ip: str | None) -> dict[str, Any]:
    started = time.perf_counter()
    route_id = _safe_str(route.get("id")) or "unknown-route"
    method = _safe_str(route.get("method")) or "tcp_connect"
    timeout_ms = _clamp_int(route.get("timeout_ms"), default=700, minimum=100, maximum=1800)
    required = route.get("required", True) is not False
    try:
        target_ip = _valid_ip(route.get("target_ip"))
        port = _clamp_int(route.get("port"), default=443, minimum=1, maximum=65535)
    except ValueError as exc:
        return {
            "id": route_id,
            "status": "fail",
            "ok": False,
            "required": required,
            "duration_ms": elapsed_ms(started),
            "failure_kind": "config",
            "error": str(exc),
        }
    if method != "tcp_connect":
        return {
            "id": route_id,
            "target_ip": target_ip,
            "port": port,
            "status": "not_configured",
            "ok": False,
            "required": required,
            "duration_ms": elapsed_ms(started),
            "error": f"unsupported method {method}",
        }
    local_source = _safe_str(route.get("source_ip")) or source_ip
    local_addr = None
    if local_source:
        with contextlib.suppress(ValueError):
            local_addr = (_valid_ip(local_source), 0)
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(target_ip, port, local_addr=local_addr),
            timeout=timeout_ms / 1000,
        )
        del reader
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        status = "ok"
        error = ""
    except asyncio.TimeoutError:
        status = "timeout"
        error = f"TCP connect exceeded {timeout_ms} ms"
    except Exception as exc:  # noqa: BLE001
        status = "fail"
        error = str(exc)[:240]
    return {
        "id": route_id,
        "target_ip": target_ip,
        "port": port,
        "method": method,
        "required": required,
        "status": status,
        "ok": status == "ok",
        "duration_ms": elapsed_ms(started),
        "failure_kind": "" if status == "ok" else status,
        "error": error,
    }


def _failed_task_result(
    kind: str,
    item: dict[str, Any],
    exc: Exception,
    deadline_ms: int,
) -> dict[str, Any]:
    if kind == "host":
        return _host_failure(
            _safe_str(item.get("id")),
            _safe_str(item.get("ip")),
            deadline_ms,
            "exception",
            str(exc),
            deadline_ms,
        )
    return {
        "id": _safe_str(item.get("id")),
        "target_ip": _safe_str(item.get("target_ip")),
        "status": "fail",
        "ok": False,
        "required": item.get("required", True) is not False,
        "duration_ms": deadline_ms,
        "failure_kind": "exception",
        "error": str(exc)[:240],
    }


def _timeout_task_result(kind: str, item: dict[str, Any], deadline_ms: int) -> dict[str, Any]:
    if kind == "host":
        return _host_failure(
            _safe_str(item.get("id")),
            _safe_str(item.get("ip")),
            deadline_ms,
            "timeout",
            "",
            deadline_ms,
        )
    return {
        "id": _safe_str(item.get("id")),
        "target_ip": _safe_str(item.get("target_ip")),
        "status": "timeout",
        "ok": False,
        "required": item.get("required", True) is not False,
        "duration_ms": deadline_ms,
        "failure_kind": "timeout",
        "error": f"route check exceeded {deadline_ms} ms",
    }


def _normal_status(value: Any) -> str:
    status = _safe_str(value).lower()
    if status in {"ok", "warn", "fail", "timeout", "unknown", "not_configured"}:
        return status
    return "unknown"


def aggregate_status(hosts: list[dict[str, Any]], routes: list[dict[str, Any]]) -> str:
    if not hosts and not routes:
        return "unknown"
    saw_warn = False
    saw_unknown = False
    for host in hosts:
        status = _normal_status(host.get("status"))
        if status in {"fail", "timeout"}:
            return "fail"
        if status == "warn":
            saw_warn = True
        if status in {"unknown", "not_configured"}:
            saw_unknown = True
    for route in routes:
        status = _normal_status(route.get("status"))
        required = route.get("required", True) is not False
        if required and status in {"fail", "timeout"}:
            return "fail"
        if status in {"fail", "timeout", "warn"}:
            saw_warn = True
        if status in {"unknown", "not_configured"}:
            saw_unknown = True
    if saw_warn:
        return "warn"
    if saw_unknown:
        return "unknown"
    return "ok"


def speech_for_result(result: dict[str, Any]) -> str:
    if result.get("status") == "ok":
        return OK_SPEECH
    if result.get("config_status") == "missing":
        return "I could not complete the health check because the PVE health config is missing."
    if result.get("config_status") == "invalid":
        return "I could not complete the health check because the PVE health config is invalid."
    hosts = result.get("hosts") if isinstance(result.get("hosts"), list) else []
    routes = result.get("isp_routes") if isinstance(result.get("isp_routes"), list) else []
    for host in hosts:
        if host.get("failure_kind") == "timeout" or host.get("status") == "timeout":
            return f"I have a health check failure. {host.get('id')} did not respond within two seconds."
    for host in hosts:
        zfs = _host_check(host, "zfs")
        if _normal_status(zfs.get("status")) in {"fail", "timeout"}:
            return f"I have a storage warning. {host.get('id')} reports a ZFS problem."
    for host in hosts:
        cpu = _host_check(host, "cpu")
        if _normal_status(cpu.get("status")) in {"warn", "fail", "timeout"}:
            return f"I have a performance warning. {host.get('id')} is under high CPU pressure."
    for host in hosts:
        ram = _host_check(host, "ram")
        if _normal_status(ram.get("status")) in {"warn", "fail", "timeout"}:
            used = ram.get("mem_used_pct")
            if isinstance(used, (int, float)):
                return f"I have a memory warning. {host.get('id')} RAM is at {used:g} percent utilization."
            return f"I have a memory warning. {host.get('id')} is under memory pressure."
    for route in routes:
        if _normal_status(route.get("status")) in {"fail", "timeout"}:
            return "I have a network warning. One ISP route did not pass the fast check."
    for host in hosts:
        if _normal_status(host.get("status")) in {"fail", "warn", "unknown", "not_configured"}:
            return (
                f"I have a health check warning. {host.get('id')} is not reporting normal health."
            )
    return "I could not determine full system health from the fast checks."


def _host_check(host: dict[str, Any], check_id: str) -> dict[str, Any]:
    checks = host.get("checks") if isinstance(host.get("checks"), dict) else {}
    item = checks.get(check_id)
    return item if isinstance(item, dict) else {}


def matrix_detail_for_result(result: dict[str, Any]) -> str:
    lines = [
        "Deterministic Wake STT PVE fast health check.",
        "",
        _markdown_table(
            ["Field", "Value"],
            [
                ["Intent", result.get("intent")],
                ["Status", result.get("status")],
                [
                    "Elapsed",
                    f"{result.get('duration_ms')} ms / deadline {result.get('deadline_ms')} ms",
                ],
                ["Config", f"{result.get('config_status')} ({result.get('config_path')})"],
                ["Notifier policy", result.get("notifier_policy", "direct_response")],
            ],
        ),
    ]
    hosts = result.get("hosts") if isinstance(result.get("hosts"), list) else []
    routes = result.get("isp_routes") if isinstance(result.get("isp_routes"), list) else []
    if hosts:
        lines.extend(["", "### PVE Host Metrics", _host_metrics_table(hosts)])
    else:
        lines.extend(["", "No PVE hosts were configured for this check."])
    if routes:
        lines.extend(["", "### ISP Routes", _route_metrics_table(routes)])
    return "\n".join(lines).rstrip()


def _markdown_table(headers: list[Any], rows: list[list[Any]]) -> str:
    table_lines = [
        "| " + " | ".join(_markdown_cell(header) for header in headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        cells = list(row[: len(headers)])
        if len(cells) < len(headers):
            cells.extend([""] * (len(headers) - len(cells)))
        table_lines.append("| " + " | ".join(_markdown_cell(cell) for cell in cells) + " |")
    return "\n".join(table_lines)


def _markdown_cell(value: Any) -> str:
    text = _safe_str(value) or "n/a"
    return " ".join(text.replace("|", "/").split())


def _host_metrics_table(hosts: list[dict[str, Any]]) -> str:
    host_headers = [_safe_str(host.get("id")) or "unknown-host" for host in hosts]
    rows: list[list[Any]] = [
        ["Host status", *[_host_status_cell(host) for host in hosts]],
        ["CPU", *[_check_summary_cell(host, "cpu") for host in hosts]],
        ["RAM", *[_check_summary_cell(host, "ram") for host in hosts]],
        ["ZFS", *[_check_summary_cell(host, "zfs") for host in hosts]],
        ["GPU", *[_check_summary_cell(host, "gpu") for host in hosts]],
        ["Thunderbolt", *[_check_summary_cell(host, "thunderbolt") for host in hosts]],
    ]
    return _markdown_table(["Metric", *host_headers], rows)


def _route_metrics_table(routes: list[dict[str, Any]]) -> str:
    rows = []
    for route in routes:
        if not isinstance(route, dict):
            continue
        target = f"{route.get('target_ip')}:{route.get('port')}"
        rows.append(
            [
                route.get("id"),
                route.get("status"),
                _duration_cell(route),
                target,
                "yes" if route.get("required", True) is not False else "no",
                _status_detail(route),
            ]
        )
    return _markdown_table(["Route", "Status", "Duration", "Target", "Required", "Detail"], rows)


def _host_status_cell(host: dict[str, Any]) -> str:
    parts = [
        _safe_str(host.get("status")) or "unknown",
        _duration_cell(host),
        _safe_str(host.get("ip")),
    ]
    detail = _status_detail(host)
    if detail != "n/a":
        parts.append(detail)
    return "; ".join(part for part in parts if part)


def _check_summary_cell(host: dict[str, Any], check_id: str) -> str:
    check = _host_check(host, check_id)
    if not check:
        return "not checked"
    prefix = _check_prefix(check)
    if check_id == "cpu":
        return _join_summary(prefix, _cpu_summary(check), _status_detail(check))
    if check_id == "ram":
        return _join_summary(prefix, _ram_summary(check), _status_detail(check))
    if check_id == "zfs":
        return _join_summary(prefix, _pools_summary(check), _status_detail(check))
    if check_id == "gpu":
        return _join_summary(prefix, _gpu_summary(check), _status_detail(check))
    if check_id == "thunderbolt":
        return _join_summary(prefix, _pools_summary(check), _status_detail(check))
    return _join_summary(prefix, _status_detail(check))


def _check_prefix(check: dict[str, Any]) -> str:
    status = _safe_str(check.get("status")) or "unknown"
    duration = _duration_cell(check)
    return f"{status} ({duration})" if duration != "n/a" else status


def _duration_cell(item: dict[str, Any]) -> str:
    duration = item.get("duration_ms")
    if isinstance(duration, (int, float)):
        return f"{duration:g} ms"
    return "n/a"


def _status_detail(item: dict[str, Any]) -> str:
    for key in ("message", "error", "failure_kind"):
        text = _safe_str(item.get(key))
        if text:
            return text[:180]
    return "n/a"


def _join_summary(*parts: Any) -> str:
    clean = [_safe_str(part) for part in parts if _safe_str(part) and _safe_str(part) != "n/a"]
    return "; ".join(clean) if clean else "n/a"


def _cpu_summary(check: dict[str, Any]) -> str:
    parts = []
    cpu_util = _pct("CPU", check.get("cpu_util_pct"))
    if cpu_util:
        parts.append(cpu_util)
    load1 = check.get("load1")
    if isinstance(load1, (int, float)):
        load_text = f"load1 {load1:g}"
        load_per_cpu = check.get("load1_per_cpu_pct")
        if isinstance(load_per_cpu, (int, float)):
            load_text += f" ({load_per_cpu:g}%/cpu)"
        parts.append(load_text)
    psi = _pct("PSI some avg10", check.get("psi_some_avg10"))
    if psi:
        parts.append(psi)
    return "; ".join(parts)


def _ram_summary(check: dict[str, Any]) -> str:
    parts = []
    used = _pct("used", check.get("mem_used_pct"))
    if used:
        parts.append(used)
    available = _pct("available", check.get("mem_available_pct"))
    if available:
        parts.append(available)
    swap = _pct("swap", check.get("swap_used_pct"))
    if swap:
        parts.append(swap)
    psi = _pct("PSI some avg10", check.get("psi_some_avg10"))
    if psi:
        parts.append(psi)
    return "; ".join(parts)


def _pct(label: str, value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{label} {value:g}%"
    return ""


def _pools_summary(check: dict[str, Any]) -> str:
    pools = check.get("pools") if isinstance(check.get("pools"), list) else []
    items = []
    for pool in pools[:4]:
        if not isinstance(pool, dict):
            continue
        name = _safe_str(pool.get("name")) or "pool"
        health = _safe_str(pool.get("health")) or "unknown"
        cap = pool.get("capacity_pct")
        if isinstance(cap, (int, float)):
            items.append(f"{name} {health} {cap:g}%")
        else:
            items.append(f"{name} {health}")
    if len(pools) > 4:
        items.append(f"+{len(pools) - 4} more")
    return ", ".join(items)


def _gpu_summary(check: dict[str, Any]) -> str:
    gpus = check.get("gpus") if isinstance(check.get("gpus"), list) else []
    items = []
    for gpu in gpus[:4]:
        if not isinstance(gpu, dict):
            continue
        index = gpu.get("index")
        label = f"GPU{index}" if index is not None else _safe_str(gpu.get("name")) or "GPU"
        memory = gpu.get("memory_percent")
        if isinstance(memory, (int, float)):
            items.append(f"{label} {memory:g}%")
        else:
            items.append(label)
    if len(gpus) > 4:
        items.append(f"+{len(gpus) - 4} more")
    return ", ".join(items)


def response_fields_from_result(result: dict[str, Any]) -> dict[str, str]:
    status = _safe_str(result.get("status")) or "unknown"
    return {
        "speech": _safe_str(result.get("speech")) or speech_for_result(result),
        "matrix_detail": _safe_str(result.get("matrix_detail")) or matrix_detail_for_result(result),
        "status": f"pve_fast_health_{status}",
        "helper_elapsed_ms": str(result.get("duration_ms") or ""),
    }
