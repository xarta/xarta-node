---
name: python-dev-tools
description: Use uv and ruff to manage Python tooling, lint and format the blueprints-app Python codebase, and keep the VS Code ruff extension working correctly. Use when running lint checks, formatting code, running Python scripts ephemerally, or setting up Python tooling on a new node.
---

# Python Dev Tools (uv + ruff)

Both tools are from [Astral](https://astral.sh) and installed system-wide on each xarta-node LXC.

## Installation

```bash
bash setup-python-dev-tools.sh
```

Run as root. Idempotent — safe to re-run. Installs `uv`, `uvx`, and `ruff` to `/usr/local/bin`.

## Linting the public blueprints-app Python code

```bash
bash .claude/skills/python-dev-tools/scripts/lint-public-python.sh
```

Runs `ruff check` (lint) and `ruff format --check` (format diff) over `blueprints-app/`.  
No files are modified. Add `--fix` to auto-repair safe lint issues.

**Baseline state (2026-04-12, first run):**
- 26 lint issues — mostly unused imports (F401), all fixable
- 53 files would be reformatted by `ruff format`

Config lives at `ruff.toml` in the repo root (picked up automatically).

## ruff — quick commands

```bash
# Check (lint only, no changes)
ruff check blueprints-app/

# Check with auto-fix (safe fixes only)
ruff check --fix blueprints-app/

# Format check (diff, no changes)
ruff format --check blueprints-app/

# Apply formatting (deliberate, one-time pass)
ruff format blueprints-app/

# Check a single file
ruff check blueprints-app/app/models.py

# Explain a rule
ruff rule F401
```

### VS Code extension

The [Ruff VS Code extension](https://marketplace.visualstudio.com/items?itemName=charliermarsh.ruff)
automatically uses the locally installed `ruff` binary at `/usr/local/bin/ruff`.

- Lint errors appear inline as you type
- On-save fixing is configurable via VS Code settings (see ruff.md in node-local docs if available)
- Format-on-save can be enabled via `"[python]": { "editor.defaultFormatter": "charliermarsh.ruff" }`

## uv — quick commands

```bash
# Run a tool ephemerally without installing it system-wide
uvx ruff check .
uvx black --check .
uvx httpie http https://example.com

# Run a Python script (auto-manages its own venv)
uv run my-script.py

# Run a Python script with inline dependencies (PEP 723)
# Add to top of script: # /// script
# //  dependencies = ["requests"]
# /// 
uv run my-script.py

# pip-compatible interface (10-100x faster than pip)
uv pip install <package>
uv pip install -r requirements.txt
uv pip list

# Manage Python versions
uv python list
uv python install 3.12

# Update uv itself
uv self update
```

## uv and the blueprints venv

The Blueprints app venv at `/opt/blueprints/venv` is managed by pip via `setup-blueprints.sh`.
It is **not** a uv-managed project. `uv pip` can be used as a drop-in replacement for pip operations
against it if desired:

```bash
uv pip install --python /opt/blueprints/venv/bin/python -r blueprints-app/requirements.txt
```

Do not use `uv sync` or `uv add` against the Blueprints venv — those are for uv-managed projects
with `pyproject.toml` + lockfiles, not the existing setup.

## Running utility scripts ephemerally

For one-off analysis or helper scripts that need packages not in the Blueprints venv:

```bash
uvx --from httpx python -c "import httpx; print(httpx.get('http://127.0.0.1:8080/health').json())"
```

Or write a script with inline metadata:

```python
#!/usr/bin/env -S uv run
# /// script
# dependencies = ["httpx", "rich"]
# ///
import httpx
from rich import print
print(httpx.get("http://127.0.0.1:8080/health").json())
```

Then: `uv run health-check.py` — no venv setup needed.

## References

If this file is loaded in a context with access to node-local docs:
- `docs/python/uv.md` — comprehensive uv examples and LXC-specific context
- `docs/python/ruff.md` — comprehensive ruff config, rules, and VS Code integration

If `/ClonedRepos/uv` and `/ClonedRepos/ruff` are available:
- `/ClonedRepos/uv/docs/guides/` — official uv guides (tools, scripts, projects)
- `/ClonedRepos/ruff/docs/configuration.md` — full ruff config reference
- `/ClonedRepos/ruff/docs/tutorial.md` — ruff + uv workflow tutorial
- `/ClonedRepos/ruff/docs/editors/setup.md` — VS Code extension setup

## Updating tools

```bash
# Update uv + uvx (self-managed binary)
uv self update

# Update ruff (installed via pip3)
pip3 install --break-system-packages --upgrade ruff
```
