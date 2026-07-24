---
name: skill-discovery
description: Use at the start of xarta-node repository, docs, stack, UI, provider, fleet, recovery, or development work in Roo. Discover Syncthing-backed common skills, declared local overlays, and shared helpers through orientation and the maintained catalog adapter.
---

# Roo Skill Discovery Adapter

The common workflow system is `/xarta-node/.lone-wolf/skills`. This Roo adapter owns no duplicate skill policy, private catalog, or semantic routing. Read `shared-governance/SKILL.md` before managing or selecting shared skills.

1. Open `/root/xarta-node/.xarta/.agent/orientation.md`.
2. Use the relevant `paths01.md` through `paths10.md` bucket; avoid `paths00.md` unless exhaustive lookup is necessary.
3. Resolve the selected common `SKILL.md` under `/xarta-node/.lone-wolf/skills` first. Use p101, p201, p301, p401, or a maintained stack-local `.claude/skills` path only for a declared local/repository overlay or an unreconciled skill, and read the selected files completely.
4. Use p103 `/root/xarta-node/.xarta/.agents` for shared executable helpers. Treat `.roo` and `.codex` copies as adapters only.
5. List current canonical skills with
   `bash /root/xarta-node/.roo/skills/skill-discovery/scripts/catalog-skills.sh`.
   Use `--name <exact-name>` to inspect same-name variants. The script delegates
   to the maintained p103 inventory; it does not own a Roo-only catalog.
6. If metadata appears stale, inspect the raw audit path reported by the script
   and refresh the owning canonical catalog or adapter. Do not hand-edit a
   static Roo skill index.
7. The always-on Roo rule at `/root/.roo/rules/xarta-session-finish.md` owns the
   turn boundary and points to the canonical `agent-session-finish` skill; do
   not duplicate its TTS call from a subtask.

Metadata/path evidence narrows candidates; the agent decides semantic fit from the full request and state. Preserve the canonical skill’s authorization and proof boundaries across providers.
