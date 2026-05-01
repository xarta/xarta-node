import os
from pathlib import Path

import pytest

TEST_NODES_JSON = Path("/tmp/xarta-node-test-web-research-nodes.json")
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
os.environ.setdefault("DOC_SPEECH_LLM_MODEL", "TEST-QUERY-NORMALIZER-MODEL")

from app import routes_web_research  # noqa: E402
from app.routes_web_research import (  # noqa: E402
    _fallback_normalize_web_research_query,
    _normalize_web_research_query,
    _web_research_query_normalizer_model,
)


@pytest.mark.asyncio
async def test_normalize_web_research_query_uses_local_llm_json_result(monkeypatch):
    async def fake_normalize(query: str) -> str:
        assert query == "latest dr who news april 2026 and bbc ai"
        return "Latest Doctor Who news April 2026 and BBC AI"

    monkeypatch.setattr(
        routes_web_research,
        "_complete_web_research_query_normalization_local",
        fake_normalize,
    )
    assert (
        await _normalize_web_research_query(" latest dr who news april 2026 and bbc ai ")
        == "Latest Doctor Who news April 2026 and BBC AI"
    )


@pytest.mark.asyncio
async def test_normalize_web_research_query_falls_back_to_whitespace_only(monkeypatch):
    async def fake_unavailable(query: str) -> None:
        return None

    monkeypatch.setattr(
        routes_web_research,
        "_complete_web_research_query_normalization_local",
        fake_unavailable,
    )
    assert await _normalize_web_research_query("  keep   user casing  ") == "keep user casing"


def test_fallback_normalize_web_research_query_does_not_guess_names():
    assert _fallback_normalize_web_research_query(" april dr who bbc ai ") == "april dr who bbc ai"


def test_query_normalizer_uses_env_configured_model(monkeypatch):
    monkeypatch.delenv("WEB_RESEARCH_QUERY_NORMALIZER_MODEL", raising=False)
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-QUERY-NORMALIZER-MODEL")
    assert _web_research_query_normalizer_model() == "TEST-QUERY-NORMALIZER-MODEL"


def test_query_normalizer_model_env_override_wins(monkeypatch):
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-DOC-MODEL")
    monkeypatch.setenv("WEB_RESEARCH_QUERY_NORMALIZER_MODEL", "TEST-WEB-QUERY-MODEL")
    assert _web_research_query_normalizer_model() == "TEST-WEB-QUERY-MODEL"
