"""routes_ui_cache.py — node-local cache controls for the fallback UI."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg

router = APIRouter(prefix="/ui-cache", tags=["ui-cache"])

_PRODUCTION = "production"
_DEVELOPMENT = "development"
_CACHE_STATE_FILE = "fallback-ui-cache-state.json"


class FallbackCacheModeUpdate(BaseModel):
    mode: Literal["production", "development"]


class FallbackCacheStatus(BaseModel):
    desired_mode: Literal["production", "development"]
    current_mode: Literal["production", "development"]
    asset_version: str
    fallback_root: str
    state_file: str
    last_applied_at: str | None = None
    last_apply_ok: bool | None = None


def _fallback_root() -> str:
    configured = os.environ.get("BLUEPRINTS_FALLBACK_GUI_DIR", "").strip()
    if configured:
        return configured
    base = cfg.REPO_NON_ROOT_PATH or "/xarta-node"
    return os.path.join(base, "gui-fallback")


def _state_path() -> Path:
    return Path(cfg.DB_DIR) / _CACHE_STATE_FILE


def _production_asset_version() -> str:
    repo = (cfg.REPO_NON_ROOT_PATH or "").strip()
    if repo and os.path.isdir(os.path.join(repo, ".git")):
        try:
            head = subprocess.check_output(
                ["git", "-C", repo, "rev-parse", "--short", "HEAD"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            ts = subprocess.check_output(
                ["git", "-C", repo, "log", "-1", "--format=%ct"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            dirty = subprocess.check_output(
                ["git", "-C", repo, "status", "--porcelain"],
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
            suffix = "-dirty" if dirty else ""
            return f"prod-{head}-{ts}{suffix}"
        except Exception:
            pass

    fallback_root = Path(_fallback_root())
    latest_mtime = 0
    if fallback_root.exists():
        try:
            latest_mtime = int(fallback_root.stat().st_mtime)
        except OSError:
            latest_mtime = 0
        for path in fallback_root.rglob("*"):
            if not path.is_file():
                continue
            try:
                latest_mtime = max(latest_mtime, int(path.stat().st_mtime))
            except OSError:
                continue
    return f"prod-mtime-{latest_mtime}"


def _default_status() -> dict:
    return {
        "desired_mode": _PRODUCTION,
        "current_mode": _PRODUCTION,
        "asset_version": _production_asset_version(),
        "fallback_root": _fallback_root(),
        "state_file": str(_state_path()),
        "last_applied_at": None,
        "last_apply_ok": None,
    }


def _read_status() -> FallbackCacheStatus:
    data = _default_status()
    path = _state_path()
    if path.is_file():
        try:
            stored = json.loads(path.read_text())
            if isinstance(stored, dict):
                data.update(stored)
        except Exception:
            pass

    for key in ("desired_mode", "current_mode"):
        if data.get(key) not in {_PRODUCTION, _DEVELOPMENT}:
            data[key] = _PRODUCTION

    if data["current_mode"] == _PRODUCTION and not str(data.get("asset_version") or "").strip():
        data["asset_version"] = _production_asset_version()

    data["fallback_root"] = _fallback_root()
    data["state_file"] = str(path)
    return FallbackCacheStatus(**data)


def _store_desired_mode(mode: str) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    data = _read_status().model_dump()
    data["desired_mode"] = mode
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


async def _run_setup_caddy() -> None:
    script = os.path.join(cfg.REPO_OUTER_PATH or "/root/xarta-node", "setup-caddy.sh")
    if not os.path.isfile(script):
        raise HTTPException(500, "setup-caddy.sh not found")

    def _run() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", script],
            capture_output=True,
            text=True,
            timeout=180,
        )

    try:
        result = await asyncio.to_thread(_run)
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "setup-caddy.sh timed out") from exc

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "setup-caddy.sh failed").strip()
        raise HTTPException(500, detail.splitlines()[-1])


@router.get("/fallback", response_model=FallbackCacheStatus)
async def get_fallback_cache_status() -> FallbackCacheStatus:
    return _read_status()


@router.put("/fallback", response_model=FallbackCacheStatus)
async def set_fallback_cache_mode(body: FallbackCacheModeUpdate) -> FallbackCacheStatus:
    _store_desired_mode(body.mode)
    await _run_setup_caddy()
    return _read_status()
