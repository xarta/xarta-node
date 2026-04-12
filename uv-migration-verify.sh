#!/usr/bin/env bash
# uv-migration-verify.sh — Quick verification that migration is working
# Usage: bash uv-migration-verify.sh

set -euo pipefail

echo "=== uv Migration Verification ==="
echo ""

# Check 1: Files exist
echo "✓ Checking migration files..."
[[ -f pyproject.toml ]] && echo "  ✓ pyproject.toml" || echo "  ✗ pyproject.toml MISSING"
[[ -f uv.lock ]] && echo "  ✓ uv.lock ($(wc -l < uv.lock) lines)" || echo "  ✗ uv.lock MISSING"
[[ -x setup-blueprints-uv.sh ]] && echo "  ✓ setup-blueprints-uv.sh" || echo "  ✗ setup-blueprints-uv.sh MISSING"
[[ -f blueprints-app/blueprints-app-uv.service.template ]] && echo "  ✓ blueprints-app-uv.service.template" || echo "  ✗ template MISSING"
[[ -f UV_MIGRATION_GUIDE.md ]] && echo "  ✓ UV_MIGRATION_GUIDE.md" || echo "  ✗ guide MISSING"
[[ -f UV_MIGRATION_SUMMARY.md ]] && echo "  ✓ UV_MIGRATION_SUMMARY.md" || echo "  ✗ summary MISSING"

echo ""

# Check 2: uv.lock integrity
echo "✓ Checking uv.lock integrity..."
if uv lock --check >/dev/null 2>&1; then
    echo "  ✓ uv.lock is valid"
else
    echo "  ✗ uv.lock validation failed"
    exit 1
fi

echo ""

# Check 3: venv exists and has packages
echo "✓ Checking venv..."
if [[ -d .venv ]]; then
    PACKAGE_COUNT=$(ls .venv/lib/python3.11/site-packages/ 2>/dev/null | wc -l)
    echo "  ✓ .venv exists ($PACKAGE_COUNT items installed)"
else
    echo "  ✗ .venv not found (run setup-blueprints-uv.sh)"
    exit 1
fi

echo ""

# Check 4: Dependencies importable
echo "✓ Checking dependencies..."
IMPORT_TEST=$(.venv/bin/python -c "import fastapi, uvicorn, httpx, pydantic; print('OK')" 2>&1)
if [[ "$IMPORT_TEST" == "OK" ]]; then
    echo "  ✓ All core dependencies importable"
    FASTAPI_VERSION=$(.venv/bin/python -c "import fastapi; print(fastapi.__version__)")
    echo "  ✓ FastAPI version: $FASTAPI_VERSION"
else
    echo "  ✗ Dependency import failed: $IMPORT_TEST"
    exit 1
fi

echo ""

# Check 5: App health
echo "✓ Checking app health..."
if curl -sf http://127.0.0.1:8080/health >/dev/null 2>&1; then
    NODE_ID=$(curl -s http://127.0.0.1:8080/health | grep -o '"node_id":"[^"]*' | cut -d'"' -f4)
    echo "  ✓ Health endpoint responds (node: $NODE_ID)"
else
    echo "  ✗ Health endpoint not responding (app may not be running)"
fi

echo ""

# Check 6: Git status
echo "✓ Checking git status..."
UNTRACKED=$(git ls-files --others --exclude-standard | wc -l)
if [[ $UNTRACKED -eq 6 ]]; then
    echo "  ✓ 6 new untracked files (not committed)"
else
    echo "  ⚠ Expected 6 untracked, found $UNTRACKED"
fi

echo ""
echo "=== Verification Complete ==="
echo ""
echo "Next steps:"
echo "  1. Review: cat UV_MIGRATION_SUMMARY.md"
echo "  2. Decide: Which migration path (A, B, or C)?"
echo "  3. Plan: When to deploy to fleet"
echo "  4. Commit: When ready (git add + git commit)"
echo ""
