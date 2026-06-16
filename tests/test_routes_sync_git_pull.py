import asyncio
import os
import sys
import tempfile
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-routes-sync-nodes.json"
NODES_JSON.write_text(
    """
    {
      "nodes": [
        {
          "node_id": "test-node",
          "display_name": "Test Node",
          "host_machine": "test-host",
          "primary_hostname": "test-node.local",
          "tailnet_hostname": "test-node.tailnet",
          "primary_ip": "127.0.0.1",
          "sync_port": 8080,
          "tailnet": "test",
          "tailnet_ip": "100.64.0.1",
          "active": true
        }
      ]
    }
    """,
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(NODES_JSON))
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-sync-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_sync  # noqa: E402


def test_git_pull_batch_skips_restart_when_heads_unchanged(monkeypatch):
    calls = []
    restarts = []

    async def fake_git_pull(repo_path, label):
        calls.append((repo_path, label))
        return False

    async def fake_restart():
        restarts.append("restart")

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {
            "outer": ("/repo/outer", True),
            "non_root": ("/repo/non-root", False),
            "inner": ("/repo/inner", True),
        },
    )

    asyncio.run(
        routes_sync._git_pull_scopes_and_maybe_restart(
            ["inner", "outer", "inner", "non_root"],
            source="test",
        )
    )

    assert calls == [
        ("/repo/outer", "outer"),
        ("/repo/non-root", "non_root"),
        ("/repo/inner", "inner"),
    ]
    assert restarts == []


def test_git_pull_batch_restarts_once_when_runtime_repo_changes(monkeypatch):
    calls = []
    restarts = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return label in {"outer", "non_root", "inner"}

    async def fake_restart():
        restarts.append("restart")
        return True

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {
            "outer": ("/repo/outer", True),
            "non_root": ("/repo/non-root", False),
            "inner": ("/repo/inner", True),
        },
    )
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer", "non_root", "inner"]))

    assert calls == ["outer", "non_root", "inner"]
    assert restarts == ["restart"]


def test_git_pull_batch_skips_when_restart_is_pending(monkeypatch):
    calls = []

    async def fake_git_pull(repo_path, label):
        calls.append(label)
        return False

    monkeypatch.setattr(routes_sync, "_RESTART_PENDING", True)
    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {"outer": ("/repo/outer", True)},
    )

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["outer"]))

    assert calls == []


def test_git_pull_batch_does_not_restart_for_non_root_only(monkeypatch):
    restarts = []

    async def fake_git_pull(repo_path, label):
        return True

    async def fake_restart():
        restarts.append("restart")

    monkeypatch.setattr(routes_sync, "_git_pull", fake_git_pull)
    monkeypatch.setattr(routes_sync, "_restart_service", fake_restart)
    monkeypatch.setattr(
        routes_sync,
        "_repo_pull_targets",
        lambda: {"non_root": ("/repo/non-root", False)},
    )
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")

    asyncio.run(routes_sync._git_pull_scopes_and_maybe_restart(["non_root"]))

    assert restarts == []


def test_systemctl_restart_command_uses_transient_unit(monkeypatch):
    monkeypatch.setattr(routes_sync.cfg, "SERVICE_RESTART_CMD", "systemctl restart blueprints-app")
    monkeypatch.setattr(routes_sync.os, "getpid", lambda: 1234)
    monkeypatch.setattr(routes_sync.time, "time", lambda: 4567.89)

    assert routes_sync._restart_command_parts() == [
        "systemd-run",
        "--unit",
        "blueprints-app-self-restart-1234-4567890",
        "--on-active=1",
        "--collect",
        "/bin/systemctl",
        "restart",
        "blueprints-app",
    ]
