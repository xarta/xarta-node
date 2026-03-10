---
name: skill-creator
description: Create, edit, and improve skills in this xarta-node repo. Use when the user wants to add a new skill, update an existing skill, or improve how a skill triggers. Skills live in .claude/skills/ (public repo — no secrets). Node-specific config and secrets belong in .xarta/ (gitignored private inner repo).
---

# Skill Creator

## Skill structure

```
.claude/skills/
└── <skill-name>/
    ├── SKILL.md         ← required: YAML frontmatter (name, description) + instructions
    ├── references/      ← large docs loaded on demand
    └── assets/          ← templates, static files
```

Skills are auto-discovered by Claude — no registration needed.

## Rules for this repo (PUBLIC — read before writing anything)

- `.claude/skills/` is in the **public** xarta-node repo
- **Zero infrastructure-specific details** in any skill file:
  - No IP addresses, hostnames, LXC IDs, port numbers, tailnet names, auth keys
  - No node names assigned to specific machines
  - Use generic placeholders (`<node-id>`, `<peer-ip>`, `my-node`) in examples
- Node-specific config and secrets → `.xarta/` (gitignored, private inner repo)

## Writing a skill

1. **SKILL.md frontmatter** — `name` + `description` (description drives when the skill triggers — make it specific)
2. **Body under 500 lines** — move large reference material to `references/`
3. **Reference files** — link clearly from SKILL.md with a note on when to read them
4. **Keep generic** — skills describe *how* to work with the system, not *this specific deployment's* values

## Updating an existing skill

Read the current SKILL.md first. Make targeted edits — don't rewrite unless asked. After editing, check: would this file be safe in a public GitHub repo right now?
