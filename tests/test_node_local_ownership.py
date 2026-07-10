import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

from app.node_local_ownership import normalize_node_local_ownership


def test_normalize_node_local_ownership_chowns_target_and_parents(tmp_path, monkeypatch):
    root = tmp_path / "lone-wolf"
    target = root / "state" / "blueprints-active-browser-runtime.json"
    target.parent.mkdir(parents=True)
    target.write_text("{}\n", encoding="utf-8")
    calls = []

    def fake_chown(path, uid, gid):
        calls.append((Path(path), uid, gid))

    monkeypatch.setattr(os, "chown", fake_chown)

    assert normalize_node_local_ownership(target, root=root) is True

    owner = root.stat()
    assert calls == [
        (target, owner.st_uid, owner.st_gid),
        (target.parent, owner.st_uid, owner.st_gid),
        (root, owner.st_uid, owner.st_gid),
    ]


def test_normalize_node_local_ownership_ignores_paths_outside_root(tmp_path, monkeypatch):
    root = tmp_path / "lone-wolf"
    outside = tmp_path / "outside.json"
    root.mkdir()
    outside.write_text("{}\n", encoding="utf-8")
    calls = []
    monkeypatch.setattr(os, "chown", lambda path, uid, gid: calls.append(path))

    assert normalize_node_local_ownership(outside, root=root) is False

    assert calls == []
