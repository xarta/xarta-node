import json
import os
import subprocess
import sys
from pathlib import Path


def test_config_import_does_not_require_vps_dockge_env(tmp_path):
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
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("VPS_DOCKGE_")
    }
    env.update(
        {
            "BLUEPRINTS_NODE_ID": "test-node",
            "NODES_JSON_PATH": str(nodes_path),
            "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "blueprints-app"),
            "SEEKDB_HOST": "127.0.0.1",
            "SEEKDB_PORT": "5432",
            "SEEKDB_DB": "blueprints_test",
            "SEEKDB_USER": "blueprints_test",
            "SEEKDB_PASSWORD": "blueprints_test",
        }
    )

    script = "from app import config; assert config.VPS_DOCKGE_BASE_URL == ''"
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
