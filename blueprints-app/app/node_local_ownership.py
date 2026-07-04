"""Ownership hand-back helpers for root-run writes into node-local repo paths."""

from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

NODE_LOCAL_ROOT = Path("/xarta-node/.lone-wolf")


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def normalize_node_local_ownership(target: Path, *, root: Path | None = None) -> bool:
    """Hand a node-local path and any newly-created parents back to the repo owner."""
    root_path = Path(root or NODE_LOCAL_ROOT)
    target_path = Path(target)
    try:
        root_abs = root_path.resolve(strict=True)
    except OSError as exc:
        log.warning("node-local ownership: could not stat root %s: %s", root_path, exc)
        return False

    target_abs = target_path if target_path.is_absolute() else target_path.absolute()
    if not _is_under(target_abs, root_abs):
        return False

    try:
        owner = root_abs.stat()
    except OSError as exc:
        log.warning("node-local ownership: could not read owner for %s: %s", root_abs, exc)
        return False

    changed = False
    current = target_path
    while True:
        try:
            exists = current.exists() or current.is_symlink()
            if exists:
                if current.is_symlink():
                    os.lchown(current, owner.st_uid, owner.st_gid)
                else:
                    os.chown(current, owner.st_uid, owner.st_gid)
                changed = True
        except OSError as exc:
            log.warning("node-local ownership: could not normalize %s: %s", current, exc)

        current_abs = current if current.is_absolute() else current.absolute()
        if current_abs == root_abs:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent

    return changed
