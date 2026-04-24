---
name: cost-efficient-subagents
description: Use configured local or lower-cost model helpers with fresh context for many independent docs, files, repos, nodes, scan findings, or records. Use when a public skill should fan out semantic analysis while keeping deterministic discovery, validation, and writes in the main orchestrator.
---

# Cost-Efficient Fresh-Context Subagents

Use this public-safe pattern when a workflow has many independent items and
each item benefits from a clean, narrow model context.

Examples:

- one document, file, repo, node, stack, or scan finding per helper call
- one git diff or leak-scan report per helper call
- one GUI file, service record, inventory report, or API response per helper call
- small aggregation batches of helper outputs before the final summary

## Core Shape

The main skill remains the orchestrator:

- discover items deterministically
- prepare compact per-item input
- call the helper model with fresh context
- aggregate structured results
- validate output shape
- make final write, commit, API, or remediation decisions

The helper is narrow:

- read one item or one compact manifest
- make one semantic judgement
- return concise structured output
- avoid hidden side effects

## Model Routing

Model choice must be configurable. Public skills should describe this contract,
not a specific deployment.

Prefer environment-driven routing through a LiteLLM-compatible proxy:

- `LITELLM_API_BASE`
- `LITELLM_API_KEY` or another deployment-specific secret variable
- `SUBAGENT_MODEL` or a skill-specific model variable
- `SUBAGENT_RUNTIME`, `SUBAGENT_WORKERS`, and `SUBAGENT_BATCH_SIZE`

The configured alias may point to a local model, a lower-cost cloud model, or a
stronger model selected by the operator for a sensitive task. Do not hardcode
private endpoints, keys, hostnames, IP addresses, node names, or model aliases
in public skill files.

## Concurrency

Use bounded concurrency only when each prompt is compact and the configured
endpoint can handle it. A safe starting point is one or two workers. Four workers
can work well for small payloads on endpoints configured for concurrent
sequences.

Aggregate in small batches, usually 10 to 20 helper outputs at a time, so the
aggregator also stays inside a clean and manageable context.

## When To Use

Use this pattern when:

- items are mostly independent
- deterministic scripts can gather the raw facts
- the expensive part is repeated semantic classification, summarization, or
  proposal generation
- clean per-item context improves quality or reduces main-context bloat

Avoid it when:

- the task needs broad cross-item reasoning from the beginning
- a deterministic script can solve the problem safely
- the helper would need write access or broad credentials
- orchestration overhead is larger than the model savings

## Safety Rule

Helpers can propose. The orchestrator decides. Keep API writes, filesystem
edits, fleet operations, commits, pushes, and remediation behind deterministic
checks and explicit workflow rules.
