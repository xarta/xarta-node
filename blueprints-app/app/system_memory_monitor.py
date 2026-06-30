"""Low-overhead system RAM monitor for Blueprints SSE/TTS warnings."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

from . import config as cfg
from .events import AppEvent
from .routes_events import publish_event
from .system_notifier import notifier_primary_enabled, post_notifier_event

log = logging.getLogger(__name__)

_GIB = 1024**3
_DEFAULT_CHECK_INTERVAL_SECS = 15.0
_DEFAULT_WARN_GIB = 28.0
_DEFAULT_CRITICAL_GIB = 31.0
_DEFAULT_WARN_REPEAT_SECS = 15 * 60.0
_DEFAULT_CRITICAL_REPEAT_SECS = 5 * 60.0


@dataclass(frozen=True)
class MemorySample:
    total_bytes: int
    available_bytes: int

    @property
    def used_bytes(self) -> int:
        return max(0, self.total_bytes - self.available_bytes)

    @property
    def used_gib(self) -> float:
        return self.used_bytes / _GIB

    @property
    def total_gib(self) -> float:
        return self.total_bytes / _GIB

    @property
    def available_gib(self) -> float:
        return self.available_bytes / _GIB


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        log.warning("memory monitor: invalid %s=%r, using %.1f", name, raw, default)
        return default
    return value if value > 0 else default


def _node_speech_name() -> str:
    node_id = cfg.NODE_ID or cfg.NODE_NAME or "this xarta-node"
    return "-".join(part[:1].upper() + part[1:] for part in str(node_id).split("-") if part)


def read_memory_sample() -> MemorySample | None:
    values: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                key, _, rest = line.partition(":")
                if key not in {"MemTotal", "MemAvailable"}:
                    continue
                amount = rest.strip().split()[0]
                values[key] = int(amount) * 1024
    except Exception:
        log.exception("memory monitor: failed reading /proc/meminfo")
        return None

    total = values.get("MemTotal", 0)
    available = values.get("MemAvailable", 0)
    if total <= 0 or available < 0:
        return None
    return MemorySample(total_bytes=total, available_bytes=available)


def _level_for(sample: MemorySample, warn_gib: float, critical_gib: float) -> str:
    used = sample.used_gib
    if used >= critical_gib:
        return "critical"
    if used >= warn_gib:
        return "warn"
    return "ok"


async def _publish_memory_warning(level: str, sample: MemorySample) -> None:
    node_name = _node_speech_name()
    critical = level == "critical"
    notification_key = f"system.memory.warning:{level}"
    message = (
        f"Warning: {node_name} likely crashing due to insufficient RAM"
        if critical
        else f"Warning: {node_name} RAM usage exceeding safe parameters"
    )
    event_type = "system.memory.warning"
    severity = "error" if critical else "warn"
    payload = {
        "notification_key": notification_key,
        "toast_dedupe_key": notification_key,
        "dedupe_key": notification_key,
        "node_id": cfg.NODE_ID,
        "node_name": cfg.NODE_NAME,
        "level": level,
        "used_gib": round(sample.used_gib, 2),
        "available_gib": round(sample.available_gib, 2),
        "total_gib": round(sample.total_gib, 2),
        "speech": message,
    }
    notifier_ok = await post_notifier_event(
        event_type=event_type,
        title="RAM Critical" if critical else "RAM Warning",
        message=message,
        severity=severity,
        source_component="blueprints-memory-monitor",
        tags=["blueprints", "system", "memory"],
        data=payload,
        importance="neutral",
        dedupe_key=notification_key,
    )
    if notifier_primary_enabled() and notifier_ok:
        log.warning(
            "memory monitor: %s used=%.2fGiB available=%.2fGiB total=%.2fGiB",
            level,
            sample.used_gib,
            sample.available_gib,
            sample.total_gib,
        )
        return

    event = AppEvent.create(
        event_type="system.memory.warning",
        title="RAM Critical" if critical else "RAM Warning",
        message=message,
        severity=severity,
        source="blueprints-memory-monitor",
        payload=payload,
    )
    await publish_event(event)
    log.warning(
        "memory monitor: %s used=%.2fGiB available=%.2fGiB total=%.2fGiB",
        level,
        sample.used_gib,
        sample.available_gib,
        sample.total_gib,
    )


async def run_memory_monitor() -> None:
    interval = _env_float("BLUEPRINTS_RAM_MONITOR_INTERVAL_SECS", _DEFAULT_CHECK_INTERVAL_SECS)
    warn_gib = _env_float("BLUEPRINTS_RAM_WARN_GIB", _DEFAULT_WARN_GIB)
    critical_gib = _env_float("BLUEPRINTS_RAM_CRITICAL_GIB", _DEFAULT_CRITICAL_GIB)
    warn_repeat = _env_float("BLUEPRINTS_RAM_WARN_REPEAT_SECS", _DEFAULT_WARN_REPEAT_SECS)
    critical_repeat = _env_float(
        "BLUEPRINTS_RAM_CRITICAL_REPEAT_SECS", _DEFAULT_CRITICAL_REPEAT_SECS
    )

    last_level = "ok"
    last_sent_at: dict[str, float] = {"warn": 0.0, "critical": 0.0}
    loop = asyncio.get_running_loop()

    log.info(
        "memory monitor started: warn>=%.1fGiB critical>=%.1fGiB interval=%.1fs",
        warn_gib,
        critical_gib,
        interval,
    )

    while True:
        sample = read_memory_sample()
        if sample is not None:
            level = _level_for(sample, warn_gib, critical_gib)
            now = loop.time()
            repeat_after = critical_repeat if level == "critical" else warn_repeat
            should_send = level in {"warn", "critical"} and (
                level != last_level or now - last_sent_at.get(level, 0.0) >= repeat_after
            )
            if should_send:
                await _publish_memory_warning(level, sample)
                last_sent_at[level] = now
            last_level = level

        await asyncio.sleep(interval)
