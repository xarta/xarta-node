"""Preserve divergent history and keep workload admission fenced on failure."""

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))
from app import public_checkout_recovery as recovery


def git(repo, *args):
    return (
        subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.DEVNULL)
        .decode()
        .strip()
    )


def configure(repo):
    git(repo, "config", "user.email", "test@example.test")
    git(repo, "config", "user.name", "Test")


@pytest.fixture
def histories(tmp_path):
    origin, writer, peer = (tmp_path / name for name in ("origin", "writer", "peer"))
    subprocess.run(["git", "init", "--bare", str(origin)], check=True, capture_output=True)
    subprocess.run(["git", "init", "-b", "main", str(writer)], check=True, capture_output=True)
    configure(writer)
    (writer / ".gitignore").write_text(".env\n.nodes.json\n.xarta/\n.lone-wolf/\n")
    (writer / "blueprints-app/app").mkdir(parents=True)
    (writer / recovery.BOOTSTRAP_PATHS[0]).write_text("old protocol")
    (writer / "source").write_text("base")
    git(writer, "add", ".gitignore", "source", recovery.BOOTSTRAP_PATHS[0])
    git(writer, "commit", "-m", "base")
    git(writer, "remote", "add", "origin", str(origin))
    git(writer, "push", "-u", "origin", "main")
    subprocess.run(
        ["git", "clone", "-b", "main", str(origin), str(peer)], check=True, capture_output=True
    )
    configure(peer)
    (peer / "source").write_text("old divergent version")
    git(peer, "add", "source")
    git(peer, "commit", "-m", "old lineage")
    old = git(peer, "rev-parse", "HEAD")
    (writer / "source").write_text("reviewed canonical version")
    (writer / "new.txt").write_text("new tracked path")
    for path in recovery.BOOTSTRAP_PATHS:
        (writer / path).write_text("published protocol: " + path)
    git(writer, "add", "source", "new.txt", *recovery.BOOTSTRAP_PATHS)
    git(writer, "commit", "-m", "canonical lineage")
    git(writer, "push", "origin", "main")
    target = git(writer, "rev-parse", "HEAD")
    for name in (".env", ".nodes.json"):
        (peer / name).write_text("local identity")
    for name in (".xarta", ".lone-wolf"):
        (peer / name).mkdir()
        (peer / name / "local").write_text("independent state")
    request = dict(
        node_id="test-node",
        expected_old_head=old,
        expected_head=target,
        expected_origin=str(origin),
        recovery_branch="codex/recovery-before-test",
        mode="recover",
        bootstrap=False,
    )
    return peer, request


def test_recovery_preserves_history_and_private_state(histories):
    peer, request = histories
    before = recovery.protected_snapshot(peer)
    result = recovery.execute(peer, request)
    assert git(peer, "rev-parse", "HEAD") == request["expected_head"]
    assert git(peer, "rev-parse", request["recovery_branch"]) == request["expected_old_head"]
    assert git(peer, "branch", "--show-current") == "main"
    assert git(peer, "rev-list", "--left-right", "--count", "HEAD...origin/main").split() == [
        "0",
        "0",
    ]
    assert recovery.protected_snapshot(peer) == before
    assert result["changed_paths"] == 4
    rollback = dict(
        request,
        mode="rollback",
        expected_old_head=request["expected_head"],
        expected_head=request["expected_old_head"],
    )
    recovery.execute(peer, rollback)
    assert git(peer, "rev-parse", "HEAD") == request["expected_old_head"]
    assert git(peer, "rev-parse", "main") == request["expected_head"]


def test_ignored_collision_preserves_tip_and_file(histories):
    peer, request = histories
    (peer / ".git/info/exclude").write_text("new.txt\n")
    (peer / "new.txt").write_text("ignored local state")
    with pytest.raises(RuntimeError, match="collision"):
        recovery.execute(peer, request)
    assert git(peer, "rev-parse", "main") == request["expected_old_head"]
    assert (peer / "new.txt").read_text() == "ignored local state"


def test_checkout_failure_keeps_recovery_branch(histories, monkeypatch):
    peer, request = histories
    actual = recovery.git

    def fail(repo, *args, **kwargs):
        if args[0] == "switch":
            raise RuntimeError("synthetic checkout failure")
        return actual(repo, *args, **kwargs)

    monkeypatch.setattr(recovery, "git", fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        recovery.execute(peer, request)
    assert git(peer, "rev-parse", request["recovery_branch"]) == request["expected_old_head"]


@pytest.mark.parametrize("failure", ["checkout", "restart"])
def test_recovery_failure_keeps_provider_fence(monkeypatch, failure):
    from app import routes_sync

    events = []
    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_PUBLIC_RECOVERY_HELD", False)
    monkeypatch.setattr(routes_sync, "_RUNNING_RUNTIME_REPO_HEADS", {"outer": "a" * 40})

    async def pause():
        events.append("pause-and-settle")
        return {"providers": [], "scheduler": {}}

    def execute(*args):
        assert events == ["pause-and-settle"]
        events.append("checkout")
        if failure == "checkout":
            raise RuntimeError("synthetic checkout failure")
        return {}

    async def update(*args):
        pass

    async def stale(*args):
        return True

    async def heads():
        return {"outer": "b" * 40}

    async def restart(**kwargs):
        assert kwargs["hold_claims_on_failure"] is True
        assert kwargs["claims_already_paused"] is True
        events.append("restart")
        return False

    async def resume():
        events.append("UNSAFE_RESUME")

    monkeypatch.setattr(routes_sync, "_pause_and_snapshot_scheduler", pause)
    monkeypatch.setattr(recovery, "execute", execute)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)
    monkeypatch.setattr(routes_sync, "_runtime_repo_is_stale", stale)
    monkeypatch.setattr(routes_sync, "_runtime_heads", heads)
    monkeypatch.setattr(routes_sync, "_restart_service", restart)
    monkeypatch.setattr(routes_sync, "_resume_blueprints_provider_claims", resume)
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart example")
    result = asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["outer"],
            operation_id="git-pull-" + "1" * 32,
            expected_head="b" * 40,
            recovery={"mode": "recover"},
        )
    )
    assert result["ok"] is False
    assert "UNSAFE_RESUME" not in events
    assert routes_sync._RESTART_PENDING and routes_sync._PUBLIC_RECOVERY_HELD


def test_work_arriving_during_admission_prevents_git_mutation(monkeypatch):
    from app import routes_sync

    events = []
    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", False)
    monkeypatch.setattr(routes_sync, "_PUBLIC_RECOVERY_HELD", False)

    async def pause():
        events.append("claims-fenced")
        await asyncio.sleep(0)
        events.append("inflight-claim-registered")
        raise routes_sync.SchedulerRestartRefused(
            "blueprints_provider_not_quiescent", {"active_run_ids": ["arriving-work"]}
        )

    async def update(*args):
        pass

    def execute(*args):
        pytest.fail("Git mutation raced admission")

    monkeypatch.setattr(routes_sync, "_pause_and_snapshot_scheduler", pause)
    monkeypatch.setattr(routes_sync, "_update_git_pull_operation", update)
    monkeypatch.setattr(recovery, "execute", execute)
    result = asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["outer"],
            operation_id="git-pull-" + "2" * 32,
            expected_head="b" * 40,
            recovery={"mode": "recover"},
        )
    )
    assert result["code"] == "blueprints_provider_not_quiescent"
    assert events == ["claims-fenced", "inflight-claim-registered"]


def test_recovery_receipt_request_binds_recovery_authority():
    from app import routes_sync

    base = {
        "expected_old_head": "a" * 40,
        "recovery_branch": "codex/recovery-test",
        "expected_origin": "reviewed-origin",
    }
    original = routes_sync._git_pull_request_json(["outer"], "b" * 40, base)
    for key in base:
        changed = dict(base)
        changed[key] += "changed"
        assert routes_sync._git_pull_request_json(["outer"], "b" * 40, changed) != original


def test_only_exact_published_bootstrap_is_admitted(histories):
    peer, request = histories
    git(peer, "fetch", "origin", "main")
    for path in recovery.BOOTSTRAP_PATHS:
        (peer / path).write_bytes(recovery.git(peer, "show", request["expected_head"] + ":" + path))
    request["bootstrap"] = True
    recovery.execute(peer, request)
    assert git(peer, "status", "--porcelain=v1") == ""
    assert git(peer, "rev-parse", request["recovery_branch"]) == request["expected_old_head"]
