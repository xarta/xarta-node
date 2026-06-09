import asyncio
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

from app import local_llm_events  # noqa: E402
from app.local_llm_events import (  # noqa: E402
    _dedupe_key,
    _last_offline_notice,
    _looks_like_offline_failure,
    publish_local_llm_recovered_event,
)


def setup_function():
    _last_offline_notice.clear()


def teardown_function():
    _last_offline_notice.clear()


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


def test_recovered_event_is_not_published_without_prior_offline(monkeypatch):
    calls = []

    async def fake_post_notifier_event(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(local_llm_events, "post_notifier_event", fake_post_notifier_event)

    asyncio.run(
        publish_local_llm_recovered_event(
            operation="docs:speech",
            model="PRIMARY-LOCAL-test",
            base_url="https://local-llm.example.test:9443/v1?secret=redacted",
        )
    )

    assert calls == []


def test_recovered_event_publishes_once_after_prior_offline(monkeypatch):
    calls = []
    operation = "web-research:narration"
    model = "PRIMARY-LOCAL-test"
    base_url = "https://local-llm.example.test:9443/v1?secret=redacted"
    dedupe_key = _dedupe_key(operation, model, base_url)
    _last_offline_notice[dedupe_key] = 123.0

    async def fake_post_notifier_event(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(local_llm_events, "post_notifier_event", fake_post_notifier_event)
    monkeypatch.setattr(local_llm_events, "notifier_primary_enabled", lambda: True)

    asyncio.run(
        publish_local_llm_recovered_event(
            operation=operation,
            model=model,
            base_url=base_url,
        )
    )

    assert dedupe_key not in _last_offline_notice
    assert len(calls) == 1
    assert calls[0]["event_type"] == "local.llm.recovered"
    assert calls[0]["severity"] == "info"
    assert calls[0]["importance"] == "neutral"
    assert calls[0]["recovery"] is True
    assert calls[0]["data"]["base_url"] == "https://local-llm.example.test:9443/v1"

    asyncio.run(
        publish_local_llm_recovered_event(
            operation=operation,
            model=model,
            base_url=base_url,
        )
    )
    assert len(calls) == 1


def test_recovered_event_ignores_non_primary_models(monkeypatch):
    calls = []
    operation = "docs:speech"
    model = "SECONDARY-LOCAL-test"
    base_url = "https://local-llm.example.test:9443"
    _last_offline_notice[_dedupe_key(operation, model, base_url)] = 123.0

    async def fake_post_notifier_event(**kwargs):
        calls.append(kwargs)
        return True

    monkeypatch.setattr(local_llm_events, "post_notifier_event", fake_post_notifier_event)

    asyncio.run(
        publish_local_llm_recovered_event(
            operation=operation,
            model=model,
            base_url=base_url,
        )
    )

    assert calls == []
