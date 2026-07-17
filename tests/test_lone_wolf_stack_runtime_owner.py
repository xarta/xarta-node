from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]
GUARD = REPO_ROOT / "blueprints-app" / "scripts" / "lone-wolf-stack-runtime-fix-owner.sh"


def run_guard(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(GUARD), *args],
        env={**os.environ, "LONE_WOLF_ROOT": str(root), "STACKS_DIR": str(root / "stacks")},
        capture_output=True,
        text=True,
        check=False,
    )


def postgres_fixture(root: Path) -> tuple[Path, Path]:
    stack = root / "stacks" / "postgres-fixture"
    data = stack / "data" / "postgres"
    data.mkdir(parents=True)
    (stack / "compose.yaml").write_text(
        "services:\n  db:\n    image: postgres:16-alpine\n"
        "    volumes:\n      - ./data/postgres:/var/lib/postgresql/data\n",
        encoding="utf-8",
    )
    child = data / "database-file"
    child.write_text("service state\n", encoding="utf-8")
    return data, child


@pytest.mark.skipif(os.geteuid() != 0, reason="numeric ownership fixture needs root")
def test_default_is_bounded_read_only_and_expanded_is_explicit(tmp_path: Path):
    root = tmp_path / ".lone-wolf"
    data, child = postgres_fixture(root)
    os.chown(data, 70, 70)
    os.chown(child, 12345, 12346)
    child_before = os.lstat(child)

    default = run_guard(root, "--check", "--verbose")
    assert default.returncode == 0, default.stdout + default.stderr
    assert "audit_level=default" in default.stdout
    assert os.lstat(child).st_uid == child_before.st_uid
    assert os.lstat(child).st_gid == child_before.st_gid

    expanded = run_guard(root, "--check", "--expanded", "--verbose")
    assert expanded.returncode == 1
    assert "DRIFT: postgres-fixture postgres-alpine" in expanded.stdout
    assert run_guard(root, "--apply", "--expanded").returncode == 2


@pytest.mark.skipif(os.geteuid() != 0, reason="numeric ownership fixture needs root")
def test_apply_repairs_only_known_sentinel_and_preserves_syncthing_identity(tmp_path: Path):
    root = tmp_path / ".lone-wolf"
    data, child = postgres_fixture(root)
    syncthing = root / "syncthing" / "vault"
    syncthing.mkdir(parents=True)
    synced = syncthing / "document.md"
    synced.write_text("synced\n", encoding="utf-8")
    os.chown(data, 0, 0)
    os.chown(child, 999, 999)
    os.chown(syncthing, 10000, 1000)
    os.chown(synced, 10000, 1000)

    applied = run_guard(root, "--apply", "--verbose")
    assert applied.returncode == 0, applied.stdout + applied.stderr
    assert (os.lstat(data).st_uid, os.lstat(data).st_gid) == (70, 70)
    assert (os.lstat(child).st_uid, os.lstat(child).st_gid) == (999, 999)
    assert (os.lstat(syncthing).st_uid, os.lstat(syncthing).st_gid) == (10000, 1000)
    assert (os.lstat(synced).st_uid, os.lstat(synced).st_gid) == (10000, 1000)
    assert "changed=1" in applied.stdout


def test_guard_contract_has_no_recursive_generic_repair():
    source = GUARD.read_text(encoding="utf-8")
    assert 'MODE="check"' in source
    assert 'AUDIT_LEVEL="default"' in source
    assert "--expanded" in source
    assert "-xdev" in source
    assert "chown -R" not in source
    assert "-exec chown" not in source
    assert 'chown --no-dereference "$owner" "$path"' in source
