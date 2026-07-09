import asyncio
import copy
import os
import sys
import tempfile
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-routes-nodes.json"
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
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-nodes-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_health, routes_nodes  # noqa: E402
from app.models import RepoVersionOut, RepoVersionsOut  # noqa: E402


def test_self_repo_versions_offloads_sync_health_route(monkeypatch):
    calls = []

    def fake_repo_versions():
        calls.append("repo_versions")
        return RepoVersionsOut(
            node_id="test-node",
            outer=RepoVersionOut(label="outer", path="/repo/outer", exists=True, commit="outer1"),
            inner=RepoVersionOut(label="inner", path="/repo/inner", exists=True, commit="inner1"),
            non_root=RepoVersionOut(
                label="non_root", path="/repo/non-root", exists=True, commit="nonroot1"
            ),
        )

    async def fake_to_thread(func, /, *args, **kwargs):
        calls.append(("to_thread", func.__name__))
        return func(*args, **kwargs)

    monkeypatch.setattr(routes_nodes.cfg, "NODE_ID", "test-node")
    monkeypatch.setattr(routes_health, "repo_versions", fake_repo_versions)
    monkeypatch.setattr(routes_nodes.asyncio, "to_thread", fake_to_thread)

    result = asyncio.run(routes_nodes.proxy_node_repo_versions("test-node"))

    assert result.node_id == "test-node"
    assert result.outer.commit == "outer1"
    assert result.inner.commit == "inner1"
    assert result.non_root.commit == "nonroot1"
    assert calls == [("to_thread", "fake_repo_versions"), "repo_versions"]


def _fleet_health_seekdb_restart_report(restarts: str = "1") -> dict:
    return {
        "ok": False,
        "source": "manual",
        "generated_at": "2026-07-09T17:22:04Z",
        "summary": {
            "nodes_targeted": 1,
            "nodes_checked": 1,
            "nodes_not_checked": [],
            "problems_found": 1,
            "checks_not_run": 0,
        },
        "reports": [
            {
                "node_id": "node-with-old-warning",
                "target_ip": "test-target",
                "status": "warn",
                "problem_count": 1,
                "checks_not_run": 0,
                "checks": [
                    {
                        "name": "seekdb_systemd",
                        "status": "warn",
                        "detail": (
                            "RestartUSec=10s TimeoutStopUSec=30s "
                            f"NRestarts={restarts} ExecStartPre=/bin/sleep"
                        ),
                        "metrics": {
                            "ActiveState": "active",
                            "SubState": "running",
                            "NRestarts": restarts,
                            "ExecStartPre": "start_time=[Tue 2026-07-07 00:19:51 UTC]",
                        },
                    }
                ],
            }
        ],
        "harness": {"returncode": 0},
    }


def test_fleet_health_clear_hides_only_exact_issue_fingerprint(monkeypatch, tmp_path):
    monkeypatch.setenv("XARTA_FLEET_HEALTH_ACK_PATH", str(tmp_path / "acknowledged.json"))

    first = routes_nodes._apply_fleet_health_acknowledgements(
        copy.deepcopy(_fleet_health_seekdb_restart_report("1"))
    )
    assert first["summary"]["problems_found"] == 1
    assert first["fleet_health_issues"]
    issue = first["fleet_health_issues"][0]

    clear_result = routes_nodes._clear_fleet_health_issues_sync([issue])
    assert clear_result["acknowledged"] == 1

    same = routes_nodes._apply_fleet_health_acknowledgements(
        copy.deepcopy(_fleet_health_seekdb_restart_report("1"))
    )
    assert same["ok"] is True
    assert same["summary"]["problems_found"] == 0
    assert same["summary"]["acknowledged_problems_hidden"] == 1
    assert same["reports"][0]["status"] == "ok"
    assert same["reports"][0]["problem_count"] == 0
    assert "node-with-old-warning: OK problems=0 blocked_checks=0" in same["text_report"]

    changed = routes_nodes._apply_fleet_health_acknowledgements(
        copy.deepcopy(_fleet_health_seekdb_restart_report("2"))
    )
    assert changed["ok"] is False
    assert changed["summary"]["problems_found"] == 1
    assert changed["summary"]["acknowledged_problems_hidden"] == 0
    assert changed["fleet_health_issues"][0]["fingerprint"] != issue["fingerprint"]
    assert "WARN seekdb_systemd" in changed["text_report"]
