import os
from pathlib import Path

import pytest
from fastapi import HTTPException

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
    _validate_public_query,
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


def test_validate_public_query_allows_non_secret_token_language():
    _validate_public_query("Qwen3 RTX 5090 benchmark tokens per second vLLM")
    _validate_public_query("token budget benchmark for local LLM inference")


def test_validate_public_query_rejects_secret_token_material():
    with pytest.raises(HTTPException):
        _validate_public_query("Authorization bearer token for example service")
    with pytest.raises(HTTPException):
        _validate_public_query("token=abc123456789")


def test_query_normalizer_uses_env_configured_model(monkeypatch):
    monkeypatch.delenv("WEB_RESEARCH_QUERY_NORMALIZER_MODEL", raising=False)
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-QUERY-NORMALIZER-MODEL")
    assert _web_research_query_normalizer_model() == "TEST-QUERY-NORMALIZER-MODEL"


def test_query_normalizer_model_env_override_wins(monkeypatch):
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-DOC-MODEL")
    monkeypatch.setenv("WEB_RESEARCH_QUERY_NORMALIZER_MODEL", "TEST-WEB-QUERY-MODEL")
    assert _web_research_query_normalizer_model() == "TEST-WEB-QUERY-MODEL"


def test_web_research_egress_profile_aliases(monkeypatch):
    monkeypatch.delenv("WEB_RESEARCH_SEARXNG_PROFILE", raising=False)
    monkeypatch.setattr(
        routes_web_research,
        "_read_egress_profile_config",
        lambda: {
            "default_profile": "normal-egress",
            "profiles": {
                "normal-egress": {},
                "vpn-egress": {},
            },
            "aliases": {
                "normal": "normal-egress",
                "vpn": "vpn-egress",
                "nordvpn": "vpn-egress",
            },
        },
    )

    assert routes_web_research._searxng_profile(None) == "normal-egress"
    assert routes_web_research._searxng_profile("default") == "normal-egress"
    assert routes_web_research._searxng_profile("vpn") == "vpn-egress"
    assert routes_web_research._searxng_profile("nordvpn") == "vpn-egress"
    assert routes_web_research._searxng_profile("normal") == "normal-egress"
    assert routes_web_research._searxng_profile("normal-egress") == "normal-egress"


def test_web_research_default_profile_comes_from_node_local_config(monkeypatch):
    monkeypatch.delenv("WEB_RESEARCH_SEARXNG_PROFILE", raising=False)
    monkeypatch.setattr(
        routes_web_research,
        "_read_egress_profile_config",
        lambda: {"default_profile": "normal-egress", "profiles": {"normal-egress": {}}},
    )

    assert routes_web_research._searxng_profile("default") == "normal-egress"
