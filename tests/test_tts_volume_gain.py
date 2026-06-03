import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

TEST_NODES_JSON = Path("/tmp/xarta-node-test-tts-volume-gain-nodes.json")
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "blueprints-app"))

os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(TEST_NODES_JSON))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import routes_tts  # noqa: E402


def _tts_settings(volume: str = "0.85") -> dict[str, str]:
    return {
        "tts.enabled": "true",
        "tts.default_voice": "alloy",
        "tts.default_message": "Hello.",
        "tts.default_mode": "stream",
        "tts.stream_word_threshold": "3",
        "tts.local_probe_url": "http://pockettts.test/health",
        "tts.local_speech_url": "http://pockettts.test/v1/audio/speech",
        "tts.timeout_ms": "120000",
        "tts.volume": volume,
        "tts.interrupt_default": "true",
        "tts.fallback.enabled": "false",
        "tts.fallback.positive_sound_path": "sounds/positive.mp3",
        "tts.fallback.negative_sound_path": "sounds/negative.mp3",
        "tts.fallback.volume": "0.70",
    }


def test_tts_utterance_event_preserves_volume_gain(monkeypatch):
    class PublishedEvent:
        def __init__(self, event):
            self.event = event

        def model_dump(self):
            return {
                "event_id": self.event.event_id,
                "event_type": self.event.event_type,
                "severity": self.event.severity,
                "title": self.event.title,
                "message": self.event.message,
                "source": self.event.source,
                "created_at": self.event.created_at,
                "payload": self.event.payload,
            }

    async def fake_publish(event):
        return PublishedEvent(event)

    monkeypatch.setattr(routes_tts, "publish_event", fake_publish)
    monkeypatch.setattr(routes_tts, "_resolve_settings", lambda: (_tts_settings(), []))

    async def run():
        return await routes_tts.tts_create_utterance(
            routes_tts.UtteranceRequest(
                utterance_id="volgain-test",
                source="hermes-local",
                agent_id="computer",
                text="Volume gain event test.",
                voice="Majel_1.wav",
                volume=0.85,
                volume_gain=1.5,
            )
        )

    response = asyncio.run(run())

    assert response["payload"]["volume"] == 0.85
    assert response["payload"]["volume_gain"] == 1.5
    assert response["event"]["payload"]["volume_gain"] == 1.5


def test_tts_utterance_event_defaults_volume_gain_from_shared_volume(monkeypatch):
    class PublishedEvent:
        def __init__(self, event):
            self.event = event

        def model_dump(self):
            return {
                "event_id": self.event.event_id,
                "event_type": self.event.event_type,
                "severity": self.event.severity,
                "title": self.event.title,
                "message": self.event.message,
                "source": self.event.source,
                "created_at": self.event.created_at,
                "payload": self.event.payload,
            }

    async def fake_publish(event):
        return PublishedEvent(event)

    monkeypatch.setattr(routes_tts, "publish_event", fake_publish)
    monkeypatch.setattr(routes_tts, "_resolve_settings", lambda: (_tts_settings("1.5"), []))

    async def run():
        return await routes_tts.tts_create_utterance(
            routes_tts.UtteranceRequest(
                utterance_id="volgain-default-test",
                source="codex",
                agent_id="codex",
                text="Volume gain default event test.",
                voice="The_other_brother.wav",
                volume=0.85,
            )
        )

    response = asyncio.run(run())

    assert response["payload"]["volume"] == 1.0
    assert response["payload"]["volume_gain"] == 1.5
    assert response["event"]["payload"]["volume_gain"] == 1.5


def test_tts_speak_forwards_volume_gain_to_pockettts(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_available(*_args, **_kwargs):
        return True

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "audio/wav"}

        async def aiter_bytes(self):
            yield b"RIFF"

        async def aread(self):
            return b""

        async def aclose(self):
            captured["response_closed"] = True

    class FakeAsyncClient:
        def __init__(self, timeout):
            captured["timeout"] = timeout

        def build_request(self, method, url, json):
            captured["method"] = method
            captured["url"] = url
            captured["json"] = json
            return SimpleNamespace(method=method, url=url, json=json)

        async def send(self, request, stream):
            captured["stream"] = stream
            return FakeResponse()

        async def aclose(self):
            captured["client_closed"] = True

    monkeypatch.setattr(routes_tts, "_resolve_settings", lambda: (_tts_settings(), []))
    monkeypatch.setattr(routes_tts, "_is_local_tts_available", fake_available)
    monkeypatch.setattr(routes_tts.httpx, "AsyncClient", FakeAsyncClient)

    async def run():
        request = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))
        response = await routes_tts.tts_speak(
            routes_tts.SpeakRequest(
                text="Volume gain upstream test.",
                voice="Majel_1.wav",
                mode="stream",
                format="wav",
                sanitize_text=False,
                transform_profile="none",
                volume_gain=1.5,
            ),
            request,
        )

        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())

    assert chunks == [b"RIFF"]
    assert captured["json"]["volume_gain"] == 1.5
    assert captured["json"]["sanitize_text"] is False
    assert captured["json"]["transform_profile"] == "none"
    assert captured["client_closed"] is True
    assert captured["response_closed"] is True


def test_tts_speak_defaults_volume_gain_from_shared_volume(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_available(*_args, **_kwargs):
        return True

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "audio/wav"}

        async def aiter_bytes(self):
            yield b"RIFF"

        async def aread(self):
            return b""

        async def aclose(self):
            captured["response_closed"] = True

    class FakeAsyncClient:
        def __init__(self, timeout):
            captured["timeout"] = timeout

        def build_request(self, method, url, json):
            captured["json"] = json
            return SimpleNamespace(method=method, url=url, json=json)

        async def send(self, request, stream):
            captured["stream"] = stream
            return FakeResponse()

        async def aclose(self):
            captured["client_closed"] = True

    monkeypatch.setattr(routes_tts, "_resolve_settings", lambda: (_tts_settings("1.5"), []))
    monkeypatch.setattr(routes_tts, "_is_local_tts_available", fake_available)
    monkeypatch.setattr(routes_tts.httpx, "AsyncClient", FakeAsyncClient)

    async def run():
        request = SimpleNamespace(headers={}, client=SimpleNamespace(host="127.0.0.1"))
        response = await routes_tts.tts_speak(
            routes_tts.SpeakRequest(
                text="Volume gain shared setting test.",
                voice="Majel_1.wav",
                mode="stream",
                format="wav",
                sanitize_text=False,
                transform_profile="none",
            ),
            request,
        )

        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk)
        return chunks

    chunks = asyncio.run(run())

    assert chunks == [b"RIFF"]
    assert captured["json"]["volume_gain"] == 1.5
    assert captured["client_closed"] is True
    assert captured["response_closed"] is True


def test_browser_hermes_tts_path_forwards_volume_gain():
    workspace = Path("/xarta-node")
    announcer = (workspace / "gui-fallback/js/model-change-announcer.js").read_text(
        encoding="utf-8"
    )
    client = (workspace / "gui-fallback/js/tts-wrapper-client.js").read_text(encoding="utf-8")

    assert "payload.volume_gain" in announcer
    assert "volumeGain:" in announcer
    assert "volume_gain:" in client
    assert "opts.volumeGain" in client
    assert "getTtsVolumeGain()" in client
