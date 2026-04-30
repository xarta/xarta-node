---
name: qwen-token-budget
description: Count or estimate prompt/output token budgets for local Qwen3.6-family LiteLLM/vLLM aliases, especially before adding source clamps, max_tokens caps, or long-context document workflows.
---

# Qwen Token Budget

Use this before changing context limits for local Qwen3.6-family workflows.

Default tokenizer:

```text
Qwen/Qwen3.6-35B-A3B
```

That is the canonical tokenizer for the local primary model family in the LiteLLM config. The local served alias may use a quantized or abliterated derivative, but token counting should follow the base tokenizer unless the deployed tokenizer is known to differ.

## Workflow

1. Prefer exact counting with `scripts/count_tokens.py`.
2. Count the actual text after only unavoidable pre-model redaction/normalization.
3. Count prompt and expected completion separately when diagnosing truncation.
4. Treat heuristic estimates as rough planning only; do not use them to justify hard clamps.

Examples:

```bash
/root/xarta-node/.venv/bin/python /root/xarta-node/.claude/skills/qwen-token-budget/scripts/count_tokens.py /path/to/file.md
/root/xarta-node/.venv/bin/python /root/xarta-node/.claude/skills/qwen-token-budget/scripts/count_tokens.py --text "paths00 token budget check"
/root/xarta-node/.venv/bin/python /root/xarta-node/.claude/skills/qwen-token-budget/scripts/count_tokens.py --json /path/to/file.md
```

The script uses Hugging Face `tokenizer.json` through the `tokenizers` package. If the tokenizer cannot be downloaded or loaded, it prints a conservative heuristic so the caller can still see the uncertainty.

## Guardrail

Never add silent source clipping because a count is large. If a workflow needs a cap, make it configurable, report the input size, report the model finish reason, and fail visibly when a generation is incomplete.
