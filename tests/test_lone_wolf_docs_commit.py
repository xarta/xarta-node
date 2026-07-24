from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "blueprints-app" / "scripts" / "lone-wolf-docs-commit.sh"
SKILLS_SCRIPT = (
    Path(__file__).parents[1] / "blueprints-app" / "scripts" / "lone-wolf-skills-commit.sh"
)
SETUP_SCRIPT = Path(__file__).parents[1] / "setup-lone-wolf.sh"


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


def test_skills_backup_commit_uses_guarded_scoped_publication():
    source = SKILLS_SCRIPT.read_text(encoding="utf-8")
    executable = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("#")
    )
    assert "THIS_NODE_SKILLS_BACKUP:-false" in source
    assert "xarta-lone-wolf-publish" in source
    assert "publish" in source and "--path skills" in source
    assert "DELAY=300" in source
    assert "tree_fingerprint" in source
    assert "git commit" not in executable
    assert "git push" not in executable
    assert "git add" not in executable


def test_skills_backup_has_no_service_restart_database_or_network_work():
    source = SKILLS_SCRIPT.read_text(encoding="utf-8")
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


def test_setup_keeps_docs_and_skills_authority_independent():
    source = SETUP_SCRIPT.read_text(encoding="utf-8")
    assert 'DOCS_BACKUP="${THIS_NODE_DOCS_BACKUP:-false}"' in source
    assert 'SKILLS_BACKUP="${THIS_NODE_SKILLS_BACKUP:-false}"' in source
    assert 'if [[ "$DOCS_BACKUP" == "true" ]]' in source
    assert 'if [[ "$SKILLS_BACKUP" == "true" ]]' in source
    assert "remove_gitignore_line 'skills'" in source
    assert "ensure_gitignore_line 'skills'" in source
    assert "/etc/cron.d/lone-wolf-skills" in source


def test_gitignore_policy_uses_guarded_scoped_publication():
    source = SETUP_SCRIPT.read_text(encoding="utf-8")
    assert '"$PUBLISH_HELPER" publish --message "$message" --path .gitignore' in source
    assert "GIT_OWNER_HELPER" not in source
