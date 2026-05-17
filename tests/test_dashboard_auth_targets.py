import json
import os
import subprocess
import sys
from pathlib import Path


def test_dashboard_auth_targets_have_separate_cookies_audiences_and_redirects(tmp_path):
    nodes_path = tmp_path / "nodes.json"
    nodes_path.write_text(
        json.dumps(
            {
                "nodes": [
                    {
                        "node_id": "test-node",
                        "display_name": "Test Node",
                        "host_machine": "test-node",
                        "primary_hostname": "test-node.example.invalid",
                        "tailnet_hostname": "test-node.tailnet.example.invalid",
                        "primary_ip": "203.0.113.10",
                        "tailnet_ip": "198.51.100.10",
                        "tailnet": "test-tailnet",
                        "sync_port": 8080,
                        "active": True,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    repo = Path(__file__).resolve().parents[1]
    env = dict(os.environ)
    env.update(
        {
            "BLUEPRINTS_NODE_ID": "test-node",
            "NODES_JSON_PATH": str(nodes_path),
            "PYTHONPATH": str(repo / "blueprints-app"),
            "SEEKDB_HOST": "127.0.0.1",
            "SEEKDB_PORT": "5432",
            "SEEKDB_DB": "blueprints_test",
            "SEEKDB_USER": "blueprints_test",
            "SEEKDB_PASSWORD": "blueprints_test",
            "BLUEPRINTS_API_SECRET": "11" * 32,
            "BLUEPRINTS_DASHBOARD_AUTH_SESSION_SECONDS": "3600",
        }
    )

    script = r"""
from app import routes_dashboard_auth as r

local = r._TARGETS["hermes-local"]
vps = r._TARGETS["hermes-vps"]

local_value, _ = r._make_session_value(local, now=1000)
vps_value, _ = r._make_session_value(vps, now=1000)

assert local.cookie_name != vps.cookie_name
assert local.audience != vps.audience
assert r._verify_session_value(local, local_value, now=1001)
assert r._verify_session_value(vps, vps_value, now=1001)
assert not r._verify_session_value(local, vps_value, now=1001)
assert not r._verify_session_value(vps, local_value, now=1001)
assert r._login_url(local).endswith("tab=hermes-local")
assert r._login_url(vps).endswith("tab=hermes-vps")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
