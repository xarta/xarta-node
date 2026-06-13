# Wake STT Follow-Up Routing Plan

Date: 2026-06-13

## Problem

Wake STT contextual follow-ups can still fall through to `hermes-stt-smart` after the profile classifier times out or fails closed. In a recent public research thread, compact Minutes correctly recorded that the prior turn likely meant Enrico Caruso, but the follow-up "the person you said" still went to a broad profile and revived stale Peter Kay context.

## Design

1. Add a bounded per-entry follow-up classifier fan-out before the broad profile classifier fallback.
2. Give each classifier only the current utterance plus one previous Minutes entry and any bounded source pointer summary for that entry.
3. Run a small number of per-entry classifiers in parallel, ordered by recency.
4. If the most recent entry returns a strong affirmative result, accept it and cancel the remaining per-entry classifiers.
5. Otherwise wait for all per-entry classifiers up to a bounded timeout and choose the highest weighted affirmative result.
6. Weight results by both model confidence and timeliness, with recency acting as a fallible prior rather than a deterministic route.
7. If all per-entry classifiers fail, time out, or return negative/weak results, continue to the existing profile classifier flow.
8. If the flow still falls back to `hermes-stt-smart`, pass the same compact, separated, caution-labelled follow-up context into that profile so it gets one more chance to resolve the reference without treating the context as proof.

## Scoring

Use a combined score:

- `combined_score = confidence * 0.75 + time_association_prior * 0.25`
- Missing timeliness prior counts as `0.0`.
- Strong affirmative threshold: `combined_score >= 0.82` and `confidence >= 0.78`.
- Minimum affirmative threshold: `combined_score >= 0.70` and `confidence >= 0.70`.

The scorer must reject semantic mismatch, fresh-topic language, and safety uncertainty even when an entry is recent.

## Initial Scope

- Target safe public research follow-ups first, especially entity-resolution repair turns like "the person you mentioned" after a bounded NullClaw result.
- Route accepted safe public research continuations to `hermes-stt-nullclaw` with `risk_class="web_research"`, `complex=false`, and no Command Code.
- Preserve Command Code requirements for filesystem mutation, terminal, SSH, Docker, service control, credentials/access, destructive action, or uncertain high-impact work.
- Keep existing source-check classifier behavior available, but reduce pressure on the single broad classifier by adding the per-entry fast lane first.

## Tests

- A Caruso-style prior Minutes entry with "Did the operator mean Enrico Caruso?" should make "I did mean the person you said. Can you look into that one please?" route to `hermes-stt-nullclaw` without Command Code.
- If the most recent per-entry classifier is strongly affirmative, later/stale classifier tasks should be cancellable and not override it.
- If only stale/weak entries return affirmative, scoring should prefer the highest combined score and avoid stale Peter Kay context winning over a newer Caruso entry.
- If per-entry classifiers time out or return only negative results, the fallback profile prompt should include separated follow-up context with timeliness metadata.
