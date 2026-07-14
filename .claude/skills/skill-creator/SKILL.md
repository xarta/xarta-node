---
name: skill-creator
description: Create or refactor public p201 xarta-node skills with public-safe metadata, provider-neutral workflows, deterministic helpers, focused tests, and generated-catalog validation.
---

# Public Skill Creator

Use this skill only for `/root/xarta-node/.claude/skills` in the public root repository.

## Invariants

- Public skill files contain no real infrastructure addresses, hostnames, node identities, private topology, credentials, private endpoints, or deployment-specific model aliases. Use placeholders and configured environment values.
- `.claude/skills` is the workflow source. Provider-specific directories are thin adapters, not policy forks.
- Frontmatter `name` and `description` are always-visible routing metadata; make the description selective and include concrete triggers.
- Keep active text concise: outcome, current invariants, fast path, mutation boundaries, evidence, stop conditions, proof, and conditional references.
- Move long examples, deep troubleshooting, migrations, and provider-specific background into narrow one-level references.
- Use deterministic code only for mechanical operations. Exact words/regex/provider names may report evidence but must not decide semantic intent.
- Helpers expose `--help`, bounded non-interactive operation, stable secret-safe output/error codes, and focused tests.
- Preserve unrelated work and run the public leak check before commit. Commit/push requires current authorization.

## Workflow

1. Read the existing skill and affected resources completely. Inspect public/private ownership, current scripts/APIs, adapters, catalogs, and tests.
2. Define concrete trigger examples and overlap boundaries before editing metadata.
3. Implement the smallest provider-neutral workflow. Put repeated reliable mechanics in `scripts/`; add references/assets only when used.
4. Validate frontmatter, links, command existence/`--help`, output bounds, tests, normal/conditional routes, and metadata overlap.
5. Run:

   ```bash
   bash /root/xarta-node/.claude/skills/skill-creator/scripts/audit-skills.sh \
     /root/xarta-node/.claude/skills
   /root/xarta-node/check-for-leaks.sh
   ```

6. Refresh generated discovery only through its owner and inspect the resulting diff.

For a public-safe fresh-context helper pattern, read `../cost-efficient-subagents/SKILL.md`. Model routing remains configurable; writes and remediation stay with the orchestrator after deterministic checks.

## Public repository boundary

Keep tracked content and examples free of real infrastructure details; use placeholders and run `/root/xarta-node/check-for-leaks.sh` before commit.
