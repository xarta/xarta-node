import os
import pwd
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app import node_local_ownership
from app.node_local_ownership import (
    normalize_node_local_ownership,
    read_node_local_text,
    write_node_local_text_atomic,
)


def test_normalize_node_local_ownership_fchowns_bound_target_and_parents(tmp_path):
    if os.geteuid() != 0:
        pytest.skip("ownership transition proof requires root")
    root = tmp_path / "lone-wolf"
    target = root / "state" / "blueprints-active-browser-runtime.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")
    xarta_uid = pwd.getpwnam("xarta").pw_uid
    os.chown(target.parent, xarta_uid, xarta_uid)
    os.chown(target, xarta_uid, xarta_uid)

    assert normalize_node_local_ownership(target, root=root) is True

    owner = root.stat()
    assert (target.stat().st_uid, target.stat().st_gid) == (owner.st_uid, owner.st_gid)
    assert (target.parent.stat().st_uid, target.parent.stat().st_gid) == (
        owner.st_uid,
        owner.st_gid,
    )


def test_normalize_node_local_ownership_ignores_paths_outside_root(tmp_path, monkeypatch):
    root = tmp_path / "lone-wolf"
    outside = tmp_path / "outside.json"
    root.mkdir()
    outside.write_text("{}\n", encoding="utf-8")
    calls = []
    monkeypatch.setattr(node_local_ownership.os, "fchown", lambda fd, uid, gid: calls.append(fd))

    assert normalize_node_local_ownership(outside, root=root) is False

    assert calls == []


def test_normalize_rejects_symlink_parent_without_touching_destination(tmp_path):
    root = tmp_path / "lone-wolf"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    destination = outside / "state.json"
    destination.write_text("outside\n", encoding="utf-8")
    (root / "state").symlink_to(outside, target_is_directory=True)
    before = os.lstat(destination)

    assert normalize_node_local_ownership(root / "state" / "state.json", root=root) is False

    after = os.lstat(destination)
    assert (after.st_uid, after.st_gid, after.st_ino, after.st_ctime_ns) == (
        before.st_uid,
        before.st_gid,
        before.st_ino,
        before.st_ctime_ns,
    )


def test_normalize_rejects_multiply_linked_regular_file(tmp_path):
    root = tmp_path / "lone-wolf"
    root.mkdir()
    target = root / "state.json"
    alias = root / "state-alias.json"
    target.write_text("state\n", encoding="utf-8")
    os.link(target, alias)
    before = os.lstat(target)

    assert normalize_node_local_ownership(target, root=root) is False

    after = os.lstat(target)
    assert (after.st_uid, after.st_gid, after.st_ino, after.st_ctime_ns) == (
        before.st_uid,
        before.st_gid,
        before.st_ino,
        before.st_ctime_ns,
    )


def test_atomic_write_replaces_final_symlink_without_following_it(tmp_path):
    root = tmp_path / "lone-wolf"
    cache = root / "cache"
    outside = tmp_path / "outside.json"
    cache.mkdir(parents=True)
    outside.write_text("outside\n", encoding="utf-8")
    target = cache / "entry.json"
    target.symlink_to(outside)

    written = write_node_local_text_atomic(target, "inside\n", root=root)

    assert written == target
    assert target.is_file() and not target.is_symlink()
    assert target.read_text(encoding="utf-8") == "inside\n"
    assert outside.read_text(encoding="utf-8") == "outside\n"


def test_atomic_write_rejects_symlink_parent(tmp_path):
    root = tmp_path / "lone-wolf"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "cache").symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        write_node_local_text_atomic(root / "cache" / "entry.json", "unsafe\n", root=root)

    assert not (outside / "entry.json").exists()


def test_read_rejects_final_symlink(tmp_path):
    root = tmp_path / "lone-wolf"
    cache = root / "cache"
    outside = tmp_path / "outside.json"
    cache.mkdir(parents=True)
    outside.write_text("outside\n", encoding="utf-8")
    target = cache / "entry.json"
    target.symlink_to(outside)

    with pytest.raises(OSError):
        read_node_local_text(target, root=root)
