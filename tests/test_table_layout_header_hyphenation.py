import asyncio
import json
import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import routes_table_layouts as routes  # noqa: E402
from app.routes_table_layouts import (  # noqa: E402
    HeaderHyphenationRequest,
    _coerce_llm_json,
    _header_hyphenation_messages,
    _hyphenation_model_name,
    _valid_hyphenated_header_label,
    hyphenate_header,
)


def test_header_hyphenation_prompt_contains_examples_and_json_shape():
    messages = _header_hyphenation_messages(
        HeaderHyphenationRequest(
            header="Pending",
            table_name="fleet-nodes",
            column_key="pending",
        )
    )

    payload = json.loads(messages[-1]["content"])
    assert payload["task"] == "suggest_visual_table_header_hyphenation"
    assert payload["header"] == "Pending"
    assert payload["response_shape"]["header_label"] == "string or null"
    assert {"header": "Pending", "header_label": "Pend-ing", "changed": True} in payload[
        "examples"
    ]


def test_header_hyphenation_json_parsing_and_validation():
    data = _coerce_llm_json(
        '```json\n{"header":"Pending","header_label":"Pend-ing","changed":true}\n```'
    )

    assert data["header_label"] == "Pend-ing"
    assert _valid_hyphenated_header_label("Pending", "Pend-ing")
    assert _valid_hyphenated_header_label("Hostnames", "Host-names")
    assert not _valid_hyphenated_header_label("Pending", "Pend-ing-now")
    assert not _valid_hyphenated_header_label("Pending", "Pend<br>ing")


def test_header_hyphenation_json_parsing_uses_response_object():
    data = _coerce_llm_json(
        'Prompt echo: {"response_shape":{"header_label":"string or null"}}\n'
        '{"header":"Pending","header_label":"Pend-ing","changed":true}'
    )

    assert data["header"] == "Pending"
    assert data["header_label"] == "Pend-ing"


def test_header_hyphenation_model_comes_from_environment(monkeypatch):
    monkeypatch.setenv("TABLE_LAYOUT_HYPHENATION_LLM_MODEL", "TEST-HYPHENATION-MODEL")
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-FALLBACK-MODEL")

    assert _hyphenation_model_name() == "TEST-HYPHENATION-MODEL"


def test_header_hyphenation_endpoint_accepts_valid_llm_json(monkeypatch):
    monkeypatch.setenv("TABLE_LAYOUT_HYPHENATION_LLM_MODEL", "TEST-HYPHENATION-MODEL")

    async def fake_complete(*args, **kwargs):
        assert args[0] == "browser-links"
        assert kwargs["max_tokens"] == 160
        assert kwargs["strip_think"] is True
        assert kwargs["no_think"] is True
        assert kwargs["model_name"] == "TEST-HYPHENATION-MODEL"
        return json.dumps(
            {
                "header": "Pending",
                "header_label": "Pend-ing",
                "changed": True,
                "confidence": 0.91,
                "reason": "common suffix split",
            }
        )

    monkeypatch.setattr(routes, "complete", fake_complete)

    response = asyncio.run(hyphenate_header(HeaderHyphenationRequest(header="Pending")))

    assert response.header == "Pending"
    assert response.header_label == "Pend-ing"
    assert response.changed is True
    assert response.used_llm is True
    assert response.confidence == 0.91


def test_header_hyphenation_endpoint_rejects_invalid_llm_json(monkeypatch):
    async def fake_complete(*args, **kwargs):
        return '{"header":"Pending","header_label":"Pend-ing-now","changed":true}'

    monkeypatch.setattr(routes, "complete", fake_complete)

    response = asyncio.run(hyphenate_header(HeaderHyphenationRequest(header="Pending")))

    assert response.header == "Pending"
    assert response.header_label is None
    assert response.changed is False
    assert response.used_llm is True
    assert "validation" in response.reason
