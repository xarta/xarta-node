# Skill patterns (public)

Use this quick reference when creating or refactoring skills.

## Recommended anatomy

```
<skill-name>/
├── SKILL.md
├── scripts/        # optional
├── references/     # optional
└── assets/         # optional
```

## `SKILL.md` minimum requirements

1. YAML frontmatter with:
   - `name`
   - `description`
2. Trigger-focused description (what + when to use)
3. Clear operational steps
4. Pointers to `scripts/` and `references/` when applicable

## Migration guidance

- Keep behavior stable while refactoring structure.
- If scripts move, update every path in `SKILL.md` examples.
- Keep examples public-safe in this repo (no real infra details).
- Prefer concise `SKILL.md`; move long material to `references/`.

## Practical checklist

- Frontmatter present and valid
- `SKILL.md` target length under 500 lines
- Script paths resolve
- Referenced files exist
- Public/privacy boundaries respected