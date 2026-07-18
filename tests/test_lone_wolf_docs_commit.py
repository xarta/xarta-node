from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "blueprints-app" / "scripts" / "lone-wolf-docs-commit.sh"


def test_docs_backup_commit_uses_guarded_publication_boundary():
    source = SCRIPT.read_text(encoding="utf-8")
    executable = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "xarta-lone-wolf-publish" in source
    assert "publish" in source and "--path docs" in source
    assert "git commit" not in executable
    assert "git push" not in executable
    assert "git add" not in executable


def test_docs_backup_keeps_role_boundary_and_debounce():
    source = SCRIPT.read_text(encoding="utf-8")
    assert "THIS_NODE_DOCS_BACKUP:-false" in source
    assert '== "true"' in source
    assert "SENTINEL" in source
    assert "DELAY=300" in source
    assert "age=$(( now - mtime ))" in source


def test_docs_backup_has_no_service_restart_database_or_event_loop_work():
    source = SCRIPT.read_text(encoding="utf-8")
    for forbidden in (
        "systemctl",
        "docker",
        "sqlite",
        "postgres",
        "curl",
        "requests",
        "asyncio",
    ):
        assert forbidden not in source
