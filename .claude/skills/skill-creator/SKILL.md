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

If the skill governs GUI, modal, or reusable component work, encode the shared-first principle
directly in the skill text: prefer one named shared module plus small page-specific adapters over
duplicated per-page implementations.

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

## MANDATORY - Embedded Menu DB Authority Contract (2026-04-08)

- Database is authoritative for embedded selector action pages in all contexts.
- `page_index` and `sort_order` from DB define order and slot positions.
- JS/runtime may insert placeholder circles only to preserve intentional DB slot gaps.
- Scarab paging control is always shown when multiple pages exist, except when touch ribbon mode is actively in use.
- Fallback is allowed only for embedded controls, and only when DB config fetch fails.
- Do not hardcode or merge local page layouts in a way that overrides DB-defined page order/positions.

## MANDATORY - App-Specific Selector Context Guardrail (2026-04-08)

- Never assume `menu_context='embed'` for new app work.
- Do not add or modify `embed_menu_items` rows in shared contexts (`embed`, `fallback-ui`, `db`) unless the user explicitly requests cross-app/shared rollout.
- Treat `embed` context as shared across all embed consumers (not app-local).
- For app-local selector behavior, require an app-specific context and explicit route-context wiring before any DB row additions.
- Default for new app work: no embed-menu DB writes unless explicitly requested.

The User insists on recognising that the menu system is database driven.  Never use language that suggests otherwise such as setting defaults in a file.  Word things carefully to always acknowledge that the menu system is database driven.  Changes to icons for example happen in the database as paths.  That is where to look.  Always confirm any possible exceptions, with careful diplomacy and tone, with the User, before assuming there are.

The User insists on recognising that the menu system is database driven.  Never use language that suggests otherwise such as setting defaults in a file.  Word things carefully to always acknowledge that the menu system is database driven.  Changes to icons for example happen in the database as paths.  That is where to look.  Always confirm any possible exceptions, with careful diplomacy and tone, with the User, before assuming there are.
