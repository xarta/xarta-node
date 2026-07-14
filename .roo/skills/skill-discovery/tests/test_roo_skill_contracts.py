import json
import subprocess
from pathlib import Path

ROOT = Path("/root/xarta-node/.roo/skills/skill-discovery")
INSTALLED = Path("/root/.roo/skills/roo-skill-discovery/SKILL.md")


def test_roo_adapter_metadata_matches_directory_and_routes_canonical() -> None:
    text = (ROOT / "SKILL.md").read_text(encoding="utf-8")
    assert "name: skill-discovery" in text
    assert "authoritative .claude skills" in text
    assert "catalog-skills.sh" in text
    assert INSTALLED.is_file()
    installed = INSTALLED.read_text(encoding="utf-8")
    assert "name: roo-skill-discovery" in installed
    assert str(ROOT / "SKILL.md") in installed


def test_catalog_is_dynamic_bounded_and_uses_shared_helper() -> None:
    script = ROOT / "scripts/catalog-skills.sh"
    source = script.read_text(encoding="utf-8")
    assert "/.xarta/.agents/bin/xarta-skill-audit" in source
    assert "SKILL-INDEX.md" not in source
    help_result = subprocess.run(
        ["bash", str(script), "--help"],
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
    )
    assert help_result.returncode == 0
    result = subprocess.run(
        ["bash", str(script), "--name", "docs-operations"],
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    assert rows
    assert {row["name"] for row in rows} == {"docs-operations"}
    assert all("api_key" not in json.dumps(row).lower() for row in rows)
