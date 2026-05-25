import os
from pathlib import Path

TEST_NODES_JSON = Path("/tmp/xarta-node-test-local-llm-events-nodes.json")
TEST_NODES_JSON.write_text(
    """
{
  "nodes": [
    {
      "node_id": "test-node",
      "display_name": "Test Node",
      "host_machine": "test-host",
      "primary_hostname": "test.local",
      "tailnet_hostname": "test-tailnet.local",
      "primary_ip": "203.0.113.10",
      "tailnet_ip": "198.51.100.10",
      "tailnet": "test-tailnet",
      "sync_port": 8080,
      "active": true
    }
  ]
}
""".strip(),
    encoding="utf-8",
)

os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(TEST_NODES_JSON))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app.local_llm_events import _looks_like_offline_failure  # noqa: E402


def test_context_window_errors_are_not_reported_as_offline():
    detail = (
        "litellm.BadRequestError: Hosted_vllmException - "
        "ContextWindowExceededError: This model's maximum context length is 204800 tokens. "
        "However, you requested 204801 tokens (156801 in the messages, 48000 in completion)."
    )

    assert _looks_like_offline_failure(400, detail) is False


def test_request_errors_without_status_are_reported_as_offline():
    assert _looks_like_offline_failure(None, "connect call failed") is True


def test_server_errors_are_reported_as_offline():
    assert _looks_like_offline_failure(503, "upstream endpoint unavailable") is True


def test_transport_errors_in_body_are_reported_as_offline():
    assert _looks_like_offline_failure(400, "connection refused by upstream") is True
