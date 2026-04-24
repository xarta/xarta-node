---
name: skill-discovery
description: Catalog and search all available Claude skills across the workspace. Use when you need to find a specific skill for a task or want to explore available capabilities.
---

# Skill Discovery

Use this skill to quickly find and identify the right Claude skill for your current task.

## When to use
- When you are unsure if a skill exists for a specific task.
- When you want to explore the available capabilities across the workspace.
- When you need to find the path to a specific skill's `SKILL.md` or helper scripts.

## Workflow

1. **Search the index**: Read `references/SKILL-INDEX.md` to find relevant skills.
2. **Refresh the index** (if needed): If you suspect new skills have been added, execute the catalog script to update the index.
3. **Navigate**: Use the path provided in the index to locate the skill directory.

## Commands

### Refresh the skill index
```bash
bash /root/xarta-node/.roo/skills/skill-discovery/scripts/catalog-skills.sh /root/xarta-node text > /root/xarta-node/.roo/skills/skill-discovery/references/SKILL-INDEX.md
```

## References
- `references/SKILL-INDEX.md`: The current catalog of all discovered skills.
