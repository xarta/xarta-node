import os
from pathlib import Path

import pytest
from fastapi import HTTPException

TEST_NODES_JSON = Path("/tmp/xarta-node-test-doc-speech-nodes.json")
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

from app.routes_docs import _assert_complete_doc_speech, _strip_frontmatter  # noqa: E402


def test_strip_frontmatter_accepts_backlink_on_closing_delimiter():
    markdown = (
        "---\n"
        "lifecycle: current\n"
        "source_type: implementation\n"
        "--- [<- LiteLLM README](README.md)\n\n"
        "# LiteLLM Workspace Context And Indexing\n\n"
        "## Purpose\n\n"
        "Body.\n\n"
        "---\n\n"
        "## Implementation tracking append\n"
    )

    stripped = _strip_frontmatter(markdown)

    assert stripped.startswith("# LiteLLM Workspace Context And Indexing")
    assert "## Purpose" in stripped
    assert "## Implementation tracking append" in stripped


def test_doc_speech_rejects_known_truncated_generation_metadata():
    with pytest.raises(HTTPException) as exc:
        _assert_complete_doc_speech(
            {
                "source_clipped": False,
                "finish_reason": "length",
            }
        )

    assert exc.value.status_code == 502
