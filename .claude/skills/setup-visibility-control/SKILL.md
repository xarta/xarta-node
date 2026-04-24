---
name: setup-visibility-control
description: Audit what setup capabilities are installed on a node and run controlled, profile-based setup remediation with dry-run support. Use when you need visibility into setup drift or want deterministic auto-install flows.
---

# Setup Visibility + Control

Use this skill when you want deterministic visibility into what setup scripts have been applied, what tools/services are actually present, and to remediate missing setup states with explicit operator control.

## Why this exists

The repository has many `setup-*.sh` scripts, but not all are intended to run in every environment. This skill provides:

- A single inventory report of script presence plus real install status probes
- Controlled remediation by profile (`baseline`, `network`, `ops`, `desktop`, `containers`, `sync`)
- Dry-run first, apply only when explicitly requested

## Scripts

- `/root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-inventory.sh`
  - Read-only inventory report (no changes)
  - Checks script existence and probe status for each setup capability

- `/root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-remediate.sh`
  - Controlled install/remediation runner
  - Supports `--dry-run` (default) and `--apply`
  - Supports profiles and explicit include/exclude controls

## Quick usage

Inventory only:

```bash
bash /root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-inventory.sh
```

Dry-run baseline remediation:

```bash
bash /root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-remediate.sh --profile baseline
```

Apply baseline remediation:

```bash
bash /root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-remediate.sh --profile baseline --apply
```

Apply custom set:

```bash
bash /root/xarta-node/.claude/skills/setup-visibility-control/scripts/setup-remediate.sh \
  --include python-dev-tools,github-cli,shellcheck \
  --exclude firewall \
  --apply
```

## Profiles

- `baseline`: ssh-and-git, certificates, blueprints, caddy, python-dev-tools
- `network`: lxc-failover, tailscale-up, firewall
- `ops`: shellcheck, rsync, github-cli
- `desktop`: user-xarta, ssh-and-git-xarta, xfce-xrdp, desktop-apps
- `containers`: docker, dockge
- `sync`: syncthing

## Notes

- `setup-remediate.sh` requires root and is intentionally conservative.
- `--apply` is explicit; default mode is dry-run.
- Network-affecting scripts are excluded from baseline profile by default.
- For fleet-wide execution, pair these scripts with the private fleet-node-harness skill.

## Related references

If available in your context, these can provide deeper operational patterns:

- `/root/xarta-node/.xarta/.claude/skills/fleet-node-harness/SKILL.md`
- `/root/xarta-node/.xarta/.claude/skills/fleet-audit/SKILL.md`
