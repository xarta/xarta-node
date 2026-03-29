"""routes_health.py — GET /health"""

import os
import subprocess

from fastapi import APIRouter

from . import config as cfg
from .db import get_conn, get_gen, get_meta
from .models import HealthOut, RepoVersionOut, RepoVersionsOut

router = APIRouter(tags=["health"])


def _repo_version(path: str, label: str) -> RepoVersionOut:
    if not path or not os.path.isdir(os.path.join(path, ".git")):
        return RepoVersionOut(label=label, path=path or "", exists=False)
    try:
        branch = subprocess.check_output(
            ["git", "-C", path, "rev-parse", "--abbrev-ref", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        commit = subprocess.check_output(
            ["git", "-C", path, "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        commit_ts = int(subprocess.check_output(
            ["git", "-C", path, "log", "-1", "--format=%ct"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip())
        dirty = bool(subprocess.check_output(
            ["git", "-C", path, "status", "--short"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip())
        return RepoVersionOut(
            label=label,
            path=path,
            exists=True,
            branch=branch,
            commit=commit,
            commit_ts=commit_ts,
            dirty=dirty,
        )
    except Exception:
        return RepoVersionOut(label=label, path=path, exists=True)


@router.get("/health", response_model=HealthOut)
async def health() -> HealthOut:
    with get_conn() as conn:
        gen = get_gen(conn)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
    return HealthOut(
        status="ok",
        node_id=cfg.NODE_ID,
        node_name=cfg.NODE_NAME,
        gen=gen,
        integrity_ok=integrity_ok,
        ui_url=cfg.UI_URL or None,
        commit=cfg.COMMIT_HASH,
        commit_ts=cfg.COMMIT_TS,
    )


@router.get("/health/repos", response_model=RepoVersionsOut)
async def repo_versions() -> RepoVersionsOut:
    return RepoVersionsOut(
        node_id=cfg.NODE_ID,
        outer=_repo_version(cfg.REPO_OUTER_PATH, "outer"),
        inner=_repo_version(cfg.REPO_INNER_PATH, "inner"),
        non_root=_repo_version(cfg.REPO_NON_ROOT_PATH, "non_root"),
    )
