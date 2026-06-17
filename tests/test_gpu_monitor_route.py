import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import routes_gpu_monitor


def test_gpu_monitor_calls_are_coalesced(monkeypatch):
    calls = 0

    async def fake_fetch():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return {
            "timestamp": "2026-06-16T00:00:00Z",
            "gpus": [
                {
                    "index": 0,
                    "name": "Test GPU",
                    "power_draw_w": 123.0,
                    "power_limit_w": 500.0,
                }
            ],
        }

    async def run():
        routes_gpu_monitor._reset_cache_for_tests()
        monkeypatch.setattr(routes_gpu_monitor, "_fetch_monitor_source", fake_fetch)
        first, second = await asyncio.gather(
            routes_gpu_monitor._coalesced_monitor(),
            routes_gpu_monitor._coalesced_monitor(),
        )
        third = await routes_gpu_monitor._coalesced_monitor()
        return first, second, third

    first, second, third = asyncio.run(run())

    assert calls == 1
    assert first[0] == second[0] == third[0]
    assert first[1]["cached"] is False
    assert second[1]["cached"] is False
    assert third[1]["cached"] is True


def test_gpu_monitor_returns_stale_cache_after_source_failure(monkeypatch):
    async def fake_fetch_ok():
        return {"timestamp": "ok", "gpus": [{"index": 0, "power_draw_w": 10.0}]}

    async def fake_fetch_fail():
        raise RuntimeError("source down")

    async def run():
        routes_gpu_monitor._reset_cache_for_tests()
        monkeypatch.setattr(routes_gpu_monitor, "_fetch_monitor_source", fake_fetch_ok)
        await routes_gpu_monitor._coalesced_monitor(force=True)
        monkeypatch.setattr(routes_gpu_monitor, "_fetch_monitor_source", fake_fetch_fail)
        return await routes_gpu_monitor._coalesced_monitor(force=True)

    payload, meta = asyncio.run(run())

    assert payload["timestamp"] == "ok"
    assert meta["cached"] is True
    assert meta["stale"] is True
