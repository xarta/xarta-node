"""Budget helpers for Blueprints document speech generation."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

DEFAULT_QWEN_TOKENIZER_MODEL = "Qwen/Qwen3.6-35B-A3B"
DEFAULT_MODEL_CONFIG_PATH = Path("/xarta-node/.lone-wolf/stacks/litellm/config.yaml")
DEFAULT_MAX_SOURCE_BYTES = 5 * 1024 * 1024
DEFAULT_SAFE_INPUT_TOKENS = 32768
DEFAULT_OUTPUT_TOKENS = 4096
DEFAULT_TOTAL_CONTEXT_TOKENS = 32768
DEFAULT_CONTEXT_BUFFER_TOKENS = 512
DEFAULT_BUDGET_THRESHOLD_RATIO = 0.9
DEFAULT_TARGET_SPOKEN_WORDS = 750
DEFAULT_MAX_SPOKEN_WORDS = 900


@dataclass(frozen=True)
class TokenCount:
    tokens: int
    method: str
    warning: str | None = None


@dataclass(frozen=True)
class ModelBudget:
    model: str
    source: str
    max_input_tokens: int
    max_output_tokens: int
    total_context_tokens: int
    context_buffer_tokens: int
    metadata: dict[str, Any]
    warning: str | None = None

    @property
    def safe_input_tokens(self) -> int:
        return max(1, int(self.max_input_tokens) - max(0, int(self.context_buffer_tokens)))


def env_int(name: str, default: int, *, minimum: int = 0, maximum: int | None = None) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            value = default
    value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def doc_speech_max_source_bytes() -> int:
    return env_int(
        "DOC_SPEECH_MAX_SOURCE_BYTES",
        DEFAULT_MAX_SOURCE_BYTES,
        minimum=0,
        maximum=50 * 1024 * 1024,
    )


def doc_speech_budget_threshold_ratio() -> float:
    raw = (os.environ.get("DOC_SPEECH_BUDGET_THRESHOLD_RATIO") or "").strip()
    try:
        value = float(raw) if raw else DEFAULT_BUDGET_THRESHOLD_RATIO
    except ValueError:
        value = DEFAULT_BUDGET_THRESHOLD_RATIO
    return max(0.1, min(0.98, value))


def doc_speech_target_words() -> int:
    return env_int("DOC_SPEECH_TARGET_SPOKEN_WORDS", DEFAULT_TARGET_SPOKEN_WORDS, minimum=100, maximum=5000)


def doc_speech_max_words() -> int:
    target = doc_speech_target_words()
    return env_int("DOC_SPEECH_MAX_SPOKEN_WORDS", DEFAULT_MAX_SPOKEN_WORDS, minimum=target, maximum=6000)


def approx_output_tokens_for_words(words: int) -> int:
    return max(256, math.ceil(max(1, words) * 1.8))


def heuristic_token_count(text: str) -> int:
    return max(1, math.ceil(len(str(text or "")) / 3.4))


@lru_cache(maxsize=4)
def _load_qwen_tokenizer(model: str, local_files_only: bool) -> Any:
    from huggingface_hub import hf_hub_download
    from tokenizers import Tokenizer

    local_path = hf_hub_download(repo_id=model, filename="tokenizer.json", local_files_only=local_files_only)
    return Tokenizer.from_file(local_path)


def count_text_tokens(text: str) -> TokenCount:
    tokenizer_model = (os.environ.get("QWEN_TOKENIZER_MODEL") or DEFAULT_QWEN_TOKENIZER_MODEL).strip()
    local_only_raw = (os.environ.get("DOC_SPEECH_TOKENIZER_LOCAL_FILES_ONLY") or "1").strip().lower()
    local_files_only = local_only_raw not in {"0", "false", "no"}
    try:
        tokenizer = _load_qwen_tokenizer(tokenizer_model, local_files_only)
        return TokenCount(tokens=len(tokenizer.encode(str(text or "")).ids), method="qwen-tokenizer")
    except Exception as exc:
        return TokenCount(
            tokens=heuristic_token_count(text),
            method="heuristic-chars-div-3.4",
            warning=f"{type(exc).__name__}: {exc}",
        )


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _extract_model_info_from_yaml(config_path: Path, model: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        import yaml
    except Exception as exc:
        return None, f"PyYAML unavailable: {exc}"
    try:
        data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return None, f"could not read {config_path}: {exc}"
    for entry in data.get("model_list") or []:
        if not isinstance(entry, dict) or str(entry.get("model_name") or "") != model:
            continue
        info = entry.get("model_info") if isinstance(entry.get("model_info"), dict) else {}
        return dict(info), None
    return None, f"model {model!r} not found in {config_path}"


def read_model_budget(model: str) -> ModelBudget:
    model = str(model or "").strip()
    config_path = Path(
        os.environ.get("DOC_SPEECH_LITELLM_CONFIG_PATH")
        or os.environ.get("LITELLM_CONFIG_PATH")
        or DEFAULT_MODEL_CONFIG_PATH
    )
    metadata: dict[str, Any] | None = None
    warning: str | None = None
    if model and config_path.is_file():
        metadata, warning = _extract_model_info_from_yaml(config_path, model)
    elif not config_path.is_file():
        warning = f"model config path not found: {config_path}"

    if metadata:
        max_input = _coerce_int(metadata.get("max_input_tokens"))
        max_output = _coerce_int(metadata.get("max_output_tokens"))
        total_context = _coerce_int(metadata.get("xarta_total_context_tokens"))
        buffer_tokens = _coerce_int(metadata.get("xarta_context_window_buffer_tokens"))
        return ModelBudget(
            model=model,
            source=f"litellm_config:{config_path}",
            max_input_tokens=max_input or DEFAULT_SAFE_INPUT_TOKENS,
            max_output_tokens=max_output or DEFAULT_OUTPUT_TOKENS,
            total_context_tokens=total_context or max(max_input or 0, DEFAULT_TOTAL_CONTEXT_TOKENS),
            context_buffer_tokens=buffer_tokens if buffer_tokens is not None else DEFAULT_CONTEXT_BUFFER_TOKENS,
            metadata=metadata,
            warning=None if max_input else "model_info missing max_input_tokens; used conservative fallback",
        )

    return ModelBudget(
        model=model,
        source="fallback",
        max_input_tokens=DEFAULT_SAFE_INPUT_TOKENS,
        max_output_tokens=DEFAULT_OUTPUT_TOKENS,
        total_context_tokens=DEFAULT_TOTAL_CONTEXT_TOKENS,
        context_buffer_tokens=DEFAULT_CONTEXT_BUFFER_TOKENS,
        metadata={},
        warning=warning or "model metadata unavailable; used conservative fallback",
    )
