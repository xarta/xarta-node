import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import pve_fast_health


async def _start_http_json_server(payload, *, delay_seconds=0.0, status=200):
    async def handler(reader, writer):
        try:
            await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=1)
            if delay_seconds:
                await asyncio.sleep(delay_seconds)
            body = json.dumps(payload).encode("utf-8")
            writer.write(
                "\r\n".join(
                    [
                        f"HTTP/1.1 {status} OK",
                        "Content-Type: application/json",
                        f"Content-Length: {len(body)}",
                        "Connection: close",
                        "",
                        "",
                    ]
                ).encode("ascii")
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def _start_tcp_server():
    async def handler(_reader, writer):
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


def _write_config(
    tmp_path, *, host_port=None, route_port=None, pve_timeout_ms=500, route_enabled=True
):
    config = {
        "schema": "xarta.pve-fast-health.hosts.v1",
        "timeout_ms": 900,
        "pve_check_timeout_ms": pve_timeout_ms,
        "hosts": [],
        "isp_routes": [],
    }
    if host_port:
        config["hosts"].append(
            {"id": "pve-test", "ip": "127.0.0.1", "port": host_port, "enabled": True}
        )
    if route_enabled:
        config["isp_routes"].append(
            {
                "id": "test-route",
                "target_ip": "127.0.0.1",
                "port": route_port or 9,
                "method": "tcp_connect",
                "timeout_ms": 200,
                "required": True,
                "enabled": True,
            }
        )
    path = tmp_path / "pve-fast-health.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def test_pve_fast_health_all_ok(tmp_path):
    async def run():
        http_server, http_port = await _start_http_json_server(
            {
                "status": "ok",
                "duration_ms": 12,
                "checks": {
                    "cpu": {"status": "ok", "duration_ms": 3},
                    "ram": {"status": "ok", "duration_ms": 2},
                    "zfs": {"status": "ok", "duration_ms": 7},
                },
            }
        )
        tcp_server, tcp_port = await _start_tcp_server()
        try:
            config_path = _write_config(tmp_path, host_port=http_port, route_port=tcp_port)
            return await pve_fast_health.aggregate_fast_health(config_path=config_path)
        finally:
            http_server.close()
            tcp_server.close()
            await http_server.wait_closed()
            await tcp_server.wait_closed()

    result = asyncio.run(run())

    assert result["ok"] is True
    assert result["status"] == "ok"
    assert result["speech"] == "I am functioning within normal parameters."
    assert result["hosts"][0]["checks"]["zfs"]["status"] == "ok"
    assert "| Metric | pve-test |" in result["matrix_detail"]
    assert "| CPU | ok" in result["matrix_detail"]
    assert "| Route | Status | Duration | Target | Required | Detail |" in result["matrix_detail"]
    assert "Structured detail:" not in result["matrix_detail"]
    assert '{"schema"' not in result["matrix_detail"]


def test_pve_fast_health_host_timeout_is_bounded(tmp_path):
    async def run():
        http_server, http_port = await _start_http_json_server(
            {"status": "ok", "checks": {}},
            delay_seconds=2.5,
        )
        try:
            config_path = _write_config(
                tmp_path,
                host_port=http_port,
                pve_timeout_ms=250,
                route_enabled=False,
            )
            return await pve_fast_health.aggregate_fast_health(config_path=config_path)
        finally:
            http_server.close()
            await http_server.wait_closed()

    result = asyncio.run(run())

    assert result["status"] == "fail"
    assert result["duration_ms"] < 900
    assert result["hosts"][0]["failure_kind"] == "timeout"
    assert (
        result["speech"]
        == "I have a health check failure. pve-test did not respond within two seconds."
    )


def test_pve_fast_health_zfs_failure_speech(tmp_path):
    async def run():
        http_server, http_port = await _start_http_json_server(
            {
                "status": "fail",
                "duration_ms": 20,
                "checks": {
                    "cpu": {"status": "ok", "duration_ms": 2},
                    "ram": {"status": "ok", "duration_ms": 2},
                    "zfs": {"status": "fail", "duration_ms": 15},
                },
            }
        )
        try:
            config_path = _write_config(tmp_path, host_port=http_port, route_enabled=False)
            return await pve_fast_health.aggregate_fast_health(config_path=config_path)
        finally:
            http_server.close()
            await http_server.wait_closed()

    result = asyncio.run(run())

    assert result["status"] == "fail"
    assert result["speech"] == "I have a storage warning. pve-test reports a ZFS problem."


def test_pve_fast_health_ram_warning_speech(tmp_path):
    async def run():
        http_server, http_port = await _start_http_json_server(
            {
                "status": "warn",
                "duration_ms": 18,
                "checks": {
                    "cpu": {"status": "ok", "duration_ms": 2},
                    "ram": {"status": "warn", "mem_used_pct": 96.5, "duration_ms": 2},
                    "zfs": {"status": "ok", "duration_ms": 12},
                },
            }
        )
        try:
            config_path = _write_config(tmp_path, host_port=http_port, route_enabled=False)
            return await pve_fast_health.aggregate_fast_health(config_path=config_path)
        finally:
            http_server.close()
            await http_server.wait_closed()

    result = asyncio.run(run())

    assert result["status"] == "warn"
    assert (
        result["speech"] == "I have a memory warning. pve-test RAM is at 96.5 percent utilization."
    )


def test_pve_fast_health_required_route_failure_speech(tmp_path):
    config_path = _write_config(tmp_path, host_port=None, route_port=9)

    result = asyncio.run(pve_fast_health.aggregate_fast_health(config_path=config_path))

    assert result["status"] == "fail"
    assert result["hosts"] == []
    assert result["isp_routes"][0]["status"] == "fail"
    assert (
        result["speech"] == "I have a network warning. One ISP route did not pass the fast check."
    )


def test_pve_fast_health_matrix_detail_uses_host_columns():
    detail = pve_fast_health.matrix_detail_for_result(
        {
            "intent": "operator_query",
            "status": "warn",
            "duration_ms": 241,
            "deadline_ms": 2000,
            "config_status": "configured",
            "config_path": "/tmp/hosts.json",
            "notifier_policy": "direct_response",
            "hosts": [
                {
                    "id": "host-a",
                    "ip": "127.0.0.1",
                    "status": "ok",
                    "duration_ms": 101,
                    "checks": {
                        "cpu": {"status": "ok", "duration_ms": 100, "cpu_util_pct": 12.5},
                        "ram": {"status": "ok", "duration_ms": 1, "mem_used_pct": 64.2},
                        "zfs": {
                            "status": "ok",
                            "duration_ms": 8,
                            "pools": [{"name": "rpool", "health": "ONLINE", "capacity_pct": 31}],
                        },
                    },
                },
                {
                    "id": "host-gpu",
                    "ip": "127.0.0.2",
                    "status": "warn",
                    "duration_ms": 112,
                    "checks": {
                        "cpu": {"status": "ok", "duration_ms": 100, "cpu_util_pct": 20.0},
                        "ram": {"status": "ok", "duration_ms": 1, "mem_used_pct": 88.1},
                        "zfs": {
                            "status": "ok",
                            "duration_ms": 11,
                            "pools": [{"name": "tank", "health": "ONLINE", "capacity_pct": 55}],
                        },
                        "gpu": {
                            "status": "warn",
                            "duration_ms": 49,
                            "message": "high GPU memory GPU0:99%",
                            "gpus": [{"index": 0, "memory_percent": 99.0}],
                        },
                    },
                },
            ],
            "isp_routes": [
                {
                    "id": "primary-wan",
                    "status": "ok",
                    "duration_ms": 10,
                    "target_ip": "1.1.1.1",
                    "port": 443,
                    "required": True,
                }
            ],
        }
    )

    assert "| Metric | host-a | host-gpu |" in detail
    assert "| ZFS | ok (8 ms); rpool ONLINE 31% | ok (11 ms); tank ONLINE 55% |" in detail
    assert "| GPU | not checked | warn (49 ms); GPU0 99%; high GPU memory GPU0:99% |" in detail
    assert "| primary-wan | ok | 10 ms | 1.1.1.1:443 | yes | n/a |" in detail
