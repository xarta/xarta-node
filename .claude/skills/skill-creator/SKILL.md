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
2. Write or refactor `SKILL.md` with strong trigger description.
3. Place deterministic helpers in `scripts/` and reference them from `SKILL.md`.
4. Move long guidance/examples into `references/`.
5. Run conformance audit before finalizing.

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

## Updating an existing skill

Read current files first, then make targeted edits. Preserve behavior unless the user asked for structural change.

After edits, verify the result is safe for a public repository and run the audit script.
