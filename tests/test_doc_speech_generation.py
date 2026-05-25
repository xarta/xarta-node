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

from app import routes_docs  # noqa: E402
from app.doc_speech_budget import ModelBudget, TokenCount, read_model_budget  # noqa: E402
from app.doc_speech_long import allocate_word_targets, split_sections  # noqa: E402
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


def test_doc_speech_budget_reads_litellm_model_info(tmp_path, monkeypatch):
    try:
        import yaml  # noqa: F401
    except Exception:
        pytest.skip("PyYAML is not installed in this environment")

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
model_list:
  - model_name: TEST-DOC-SPEECH
    model_info:
      max_input_tokens: 131584
      max_output_tokens: 65536
      xarta_total_context_tokens: 204800
      xarta_context_window_buffer_tokens: 256
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("DOC_SPEECH_LITELLM_CONFIG_PATH", str(config_path))

    budget = read_model_budget("TEST-DOC-SPEECH")

    assert budget.source == f"litellm_config:{config_path}"
    assert budget.max_input_tokens == 131584
    assert budget.max_output_tokens == 65536
    assert budget.total_context_tokens == 204800
    assert budget.context_buffer_tokens == 256


@pytest.mark.asyncio
async def test_doc_speech_over_size_refusal_does_not_call_llm(monkeypatch):
    monkeypatch.setenv("DOC_SPEECH_MAX_SOURCE_BYTES", "10")
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-DOC-SPEECH")
    monkeypatch.setenv("DOC_SPEECH_LITELLM_CONFIG_PATH", "/does/not/exist.yaml")

    async def fail_prepare(*_args, **_kwargs):
        raise AssertionError("sanitizer should not run for over-size refusal")

    async def fail_complete(*_args, **_kwargs):
        raise AssertionError("LLM should not run for over-size refusal")

    monkeypatch.setattr(routes_docs, "prepare_tts_markdown_for_llm_via_service", fail_prepare)
    monkeypatch.setattr(routes_docs, "_complete_doc_speech_local", fail_complete)

    speech, meta = await routes_docs._generate_doc_speech_markdown(
        {"doc_id": "doc-too-large", "path": "docs/huge.md", "label": "Huge", "description": ""},
        "x" * 20,
    )

    assert "too large" in speech
    assert meta["path_taken"] == "too_large"
    assert meta["llm_call_count"] == 0
    assert meta["source_bytes"] == 20


@pytest.mark.asyncio
async def test_doc_speech_direct_path_for_under_threshold(monkeypatch):
    monkeypatch.setenv("DOC_SPEECH_MAX_SOURCE_BYTES", "1000000")
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-DOC-SPEECH")
    monkeypatch.setenv("DOC_SPEECH_LLM_MAX_TOKENS", "100")
    budget = ModelBudget(
        model="TEST-DOC-SPEECH",
        source="test",
        max_input_tokens=1000,
        max_output_tokens=1000,
        total_context_tokens=4000,
        context_buffer_tokens=0,
        metadata={},
    )

    async def fake_prepare(text, **_kwargs):
        return text

    async def fake_complete(*_args, **kwargs):
        return "Draft narration", {
            "llm_model": "TEST-DOC-SPEECH",
            "max_tokens": kwargs.get("max_tokens") or 1000,
            "finish_reason": "stop",
            "usage": {},
        }

    async def fake_review(**_kwargs):
        return "Reviewed narration", {
            "llm_model": "TEST-DOC-SPEECH",
            "max_tokens": 1000,
            "finish_reason": "stop",
            "usage": {},
        }

    async def fake_clean(text):
        return text

    monkeypatch.setattr(routes_docs, "read_model_budget", lambda _model: budget)
    monkeypatch.setattr(routes_docs, "count_text_tokens", lambda _text: TokenCount(10, "test"))
    monkeypatch.setattr(routes_docs, "prepare_tts_markdown_for_llm_via_service", fake_prepare)
    monkeypatch.setattr(routes_docs, "_complete_doc_speech_local", fake_complete)
    monkeypatch.setattr(routes_docs, "_review_doc_speech_narration", fake_review)
    monkeypatch.setattr(routes_docs, "_clean_doc_speech_markdown", fake_clean)

    speech, meta = await routes_docs._generate_doc_speech_markdown(
        {"doc_id": "doc-small", "path": "docs/small.md", "label": "Small", "description": ""},
        "# Small\n\nBody.",
    )

    assert speech == "Reviewed narration"
    assert meta["path_taken"] == "direct"
    assert meta["llm_call_count"] == 2
    assert meta["final_speech_words"] == 2


@pytest.mark.asyncio
async def test_doc_speech_uses_long_summary_over_threshold(monkeypatch):
    monkeypatch.setenv("DOC_SPEECH_MAX_SOURCE_BYTES", "1000000")
    monkeypatch.setenv("DOC_SPEECH_LLM_MODEL", "TEST-DOC-SPEECH")
    budget = ModelBudget(
        model="TEST-DOC-SPEECH",
        source="test",
        max_input_tokens=100,
        max_output_tokens=1000,
        total_context_tokens=4000,
        context_buffer_tokens=0,
        metadata={},
    )

    async def fake_prepare(text, **_kwargs):
        return text

    async def fake_long(**_kwargs):
        return "Long summary speech", {
            "work_dir": "/tmp/work",
            "llm_calls": [],
            "llm_call_count": 0,
            "speech_words": 3,
        }

    monkeypatch.setattr(routes_docs, "read_model_budget", lambda _model: budget)
    monkeypatch.setattr(routes_docs, "count_text_tokens", lambda _text: TokenCount(100, "test"))
    monkeypatch.setattr(routes_docs, "prepare_tts_markdown_for_llm_via_service", fake_prepare)
    monkeypatch.setattr(routes_docs, "_generate_long_doc_speech", fake_long)

    speech, meta = await routes_docs._generate_doc_speech_markdown(
        {"doc_id": "doc-long", "path": "docs/long.md", "label": "Long", "description": ""},
        "# Long\n\nBody.",
    )

    assert speech == "Long summary speech"
    assert meta["path_taken"] == "long_summary"
    assert meta["work_dir"] == "/tmp/work"
    assert meta["final_speech_words"] == 3


@pytest.mark.asyncio
async def test_long_doc_generation_reuses_existing_section_summary(tmp_path, monkeypatch):
    section_path = tmp_path / "sections" / "section-0001.md"
    summary_path = tmp_path / "summaries" / "section-0001.txt"
    section_path.parent.mkdir(parents=True)
    summary_path.parent.mkdir(parents=True)
    section_path.write_text("# One\n\nBody", encoding="utf-8")
    summary_path.write_text("Existing summary.\n", encoding="utf-8")

    section = routes_docs.SectionRecord(
        section_id="section-0001",
        parent_id=None,
        title="One",
        start=0,
        end=11,
        heading_depth=1,
        byte_count=11,
        char_count=11,
        token_count=3,
        text_path=str(section_path),
        summary_path=str(summary_path),
    )
    budget = ModelBudget(
        model="TEST-DOC-SPEECH",
        source="test",
        max_input_tokens=1000,
        max_output_tokens=1000,
        total_context_tokens=4000,
        context_buffer_tokens=0,
        metadata={},
    )

    monkeypatch.setattr(routes_docs, "_DOC_SPEECH_WORK_ROOT", tmp_path)
    monkeypatch.setattr(routes_docs, "source_fingerprint", lambda _text: "fingerprint")
    monkeypatch.setattr(routes_docs, "split_sections", lambda *_args, **_kwargs: [section])
    monkeypatch.setattr(routes_docs, "count_text_tokens", lambda _text: TokenCount(10, "test"))

    async def fail_section_summary(**_kwargs):
        raise AssertionError("existing summary should be reused")

    async def fake_complete(*_args, **kwargs):
        return "Cohesive speech.", {
            "llm_model": "TEST-DOC-SPEECH",
            "max_tokens": kwargs.get("max_tokens") or 1000,
            "finish_reason": "stop",
            "usage": {},
        }

    async def fake_review(**_kwargs):
        return "Reviewed cohesive speech.", {
            "llm_model": "TEST-DOC-SPEECH",
            "max_tokens": 1000,
            "finish_reason": "stop",
            "usage": {},
        }

    async def fake_clean(text):
        return text

    monkeypatch.setattr(routes_docs, "_summarize_long_text_piece", fail_section_summary)
    monkeypatch.setattr(routes_docs, "_complete_doc_speech_local", fake_complete)
    monkeypatch.setattr(routes_docs, "_review_doc_speech_narration", fake_review)
    monkeypatch.setattr(routes_docs, "_clean_doc_speech_markdown", fake_clean)

    speech, meta = await routes_docs._generate_long_doc_speech(
        doc={"doc_id": "doc", "path": "docs/doc.md", "label": "Doc"},
        prepared_source="# One\n\nBody",
        model_budget=budget,
        target_words=750,
        max_words=900,
    )

    assert speech == "Reviewed cohesive speech."
    assert meta["resumed_summary_count"] == 1


def test_long_doc_split_respects_headings_and_fenced_code(tmp_path):
    text = "# Real heading\n\nIntro.\n\n```md\n# Not a heading\n```\n\n## Next heading\n\nBody.\n"

    sections = split_sections(
        text,
        work_dir=tmp_path,
        count_tokens=lambda value: max(1, len(value.split())),
        fallback_chunk_tokens=100,
    )

    assert [section.title for section in sections] == ["Real heading", "Next heading"]
    assert "# Not a heading" in Path(sections[0].text_path).read_text(encoding="utf-8")


def test_long_doc_split_groups_weak_heading_text_by_paragraph(tmp_path):
    text = "\n\n".join(f"Paragraph {index} with enough words to count as a chunkable unit." for index in range(80))

    sections = split_sections(
        text,
        work_dir=tmp_path,
        count_tokens=lambda value: max(1, len(value.split())),
        fallback_chunk_tokens=10,
    )

    assert len(sections) > 1
    assert all(section.title.startswith("Part ") for section in sections)


def test_long_doc_split_groups_many_heading_sections(tmp_path):
    text = "\n\n".join(f"## Heading {index}\n\nBody {index} " + ("word " * 20) for index in range(120))

    sections = split_sections(
        text,
        work_dir=tmp_path,
        count_tokens=lambda value: max(1, len(value.split())),
        fallback_chunk_tokens=400,
        max_heading_sections=20,
    )

    assert 1 < len(sections) < 120
    assert any("through" in section.title for section in sections)
    assert all(section.token_count <= 1200 for section in sections)


def test_long_doc_word_allocation_respects_floor_and_cap(tmp_path):
    text = "# A\n\nsmall\n\n# B\n\n" + ("large " * 400)
    sections = split_sections(
        text,
        work_dir=tmp_path,
        count_tokens=lambda value: max(1, len(value.split())),
        fallback_chunk_tokens=1000,
    )

    allocations = allocate_word_targets(sections, target_words=200, floor_words=35, cap_ratio=0.3)

    assert min(allocations.values()) >= 10
    assert max(allocations.values()) <= 60
