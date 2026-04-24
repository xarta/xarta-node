---
name: skill-creator
description: Create new skills and refactor existing skills in the public xarta-node repo, then validate conformance (frontmatter, structure, and script/path references). Use whenever a user asks to add, improve, migrate, or standardize skills under .claude/skills.
---

# Skill Creator (Public)

Use this skill to create or refactor skills in `/root/xarta-node/.claude/skills`.

## Skill structure

```
.claude/skills/
└── <skill-name>/
    ├── SKILL.md         ← required: YAML frontmatter (name, description) + instructions
    ├── scripts/         ← executable helper scripts for deterministic/repetitive tasks
    ├── references/      ← large docs loaded on demand
    └── assets/          ← templates, static files
```

Keep `SKILL.md` concise (target under 500 lines). Move heavy guidance to `references/`.

## Rules for this repo (PUBLIC — read before writing anything)

- `.claude/skills/` is in the **public** xarta-node repo
- **Zero infrastructure-specific details** in any skill file:
  - No IP addresses, hostnames, LXC IDs, port numbers, tailnet names, auth keys
  - No node names assigned to specific machines
  - Use generic placeholders (`<node-id>`, `<peer-ip>`, `my-node`) in examples
- Node-specific config and secrets → `.xarta/.secrets/` (gitignored, private inner repo)

## Workflow

1. Capture intent: what the skill does, and when it should trigger.
2. Choose the execution shape: deterministic script, single-agent workflow, or
   fresh-context helper fan-out.
3. Write or refactor `SKILL.md` with strong trigger description.
4. Place deterministic helpers in `scripts/` and reference them from `SKILL.md`.
5. Move long guidance/examples into `references/`.
6. Run conformance audit before finalizing.

If the skill governs GUI, modal, or reusable component work, encode the shared-first principle
directly in the skill text: prefer one named shared module plus small page-specific adapters over
duplicated per-page implementations.

## Fresh-context helper pattern

When a skill loops over many independent docs, files, repos, nodes, stack
reports, leak-scan findings, git diffs, or API responses, consider a
fresh-context helper pattern instead of one large prompt. The orchestrator
should run deterministic discovery and validation, then pass one compact item at
a time to a configured cost-efficient model helper.

For public skills, describe model routing generically:

- use LiteLLM-compatible aliases from environment variables
- allow local or lower-cost cloud models depending on the operator's `.env`
- never hardcode private endpoints, keys, hostnames, IP addresses, node names,
  or deployment-specific model aliases
- keep filesystem writes, API writes, commits, pushes, and remediation in the
  orchestrator after deterministic checks

See `/root/xarta-node/.claude/skills/cost-efficient-subagents/SKILL.md` for the
reusable public-safe pattern.

## Conformance audit script

Use:

```bash
bash /root/xarta-node/.claude/skills/skill-creator/scripts/audit-skills.sh /root/xarta-node/.claude/skills
```

The audit checks:

- `SKILL.md` has YAML frontmatter with `name` and `description`
- line count target (`<= 500`)
- absolute filesystem paths referenced in `SKILL.md` exist

## References

- `references/skill-patterns.md` for recommended anatomy and migration guidance.
- `../cost-efficient-subagents/SKILL.md` for public-safe fresh-context helper
  fan-out using configured local or lower-cost models.

## Updating an existing skill

Read current files first, then make targeted edits. Preserve behavior unless the user asked for structural change.

After edits, verify the result is safe for a public repository and run the audit script.

When asked to commit and push ALL repos always including the lone wolf repo.  Lone wolf repo is specific to each node and not distributed.  Sometimes you'll be asked to also commit and push each lone wolf repo on each node separately via ssh.  That is a separate concern to commit and push all repos.
