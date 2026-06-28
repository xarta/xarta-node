import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import routes_personal_prompts as prompts  # noqa: E402


def configure_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    apply_strategy: str = "write-file",
) -> tuple[Path, prompts.PromptSpec]:
    root = tmp_path / "stack" / "config"
    prompt_path = root / "prompts" / "test-prompt.md"
    prompt_path.parent.mkdir(parents=True)
    prompt_path.write_text("original prompt\n", encoding="utf-8")
    spec = prompts.PromptSpec(
        prompt_id="test-prompt",
        label="Test Prompt",
        surface="kanban",
        group="Tests",
        description="Prompt used by tests.",
        path=prompt_path,
        apply_strategy=apply_strategy,
    )
    monkeypatch.setattr(prompts, "ALLOWED_PROMPT_ROOTS", (root,))
    monkeypatch.setattr(prompts, "PROMPT_REGISTRY", {"test-prompt": spec})
    return prompt_path, spec


def test_list_and_get_prompt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    configure_registry(monkeypatch, tmp_path)

    listed = prompts.list_prompts()
    assert listed["ok"] is True
    assert listed["prompts"][0]["id"] == "test-prompt"

    fetched = prompts.get_prompt("test-prompt")
    assert fetched["ok"] is True
    assert fetched["prompt"]["content"] == "original prompt\n"
    assert fetched["prompt"]["sha256"].startswith("sha256:")


def test_apply_prompt_writes_tracked_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    prompt_path, _spec = configure_registry(monkeypatch, tmp_path)

    result = prompts.apply_prompt(
        "test-prompt",
        prompts.PromptApplyRequest(content="updated prompt\n", restart=False),
    )

    assert result["ok"] is True
    assert result["changed"] is True
    assert result["actions"] == []
    assert prompt_path.read_text(encoding="utf-8") == "updated prompt\n"


def test_hermes_profile_apply_runs_installer_and_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configure_registry(monkeypatch, tmp_path, apply_strategy="hermes-kanban-profile")
    calls: list[tuple[list[str], int]] = []

    def fake_run(command: list[str], timeout: int = 60) -> dict:
        calls.append((command, timeout))
        return {"ok": True, "command": command, "timeout": timeout}

    monkeypatch.setattr(prompts, "_run_command", fake_run)

    result = prompts.apply_prompt(
        "test-prompt",
        prompts.PromptApplyRequest(content="profile prompt\n", restart=True),
    )

    assert result["ok"] is True
    assert len(calls) == 2
    assert calls[0][0][:5] == [
        "docker",
        "exec",
        "hermes-local",
        "/opt/data/scripts/install_hermes_kanban_profile.py",
        "--force",
    ]
    assert calls[1][0][-2:] == ["-t", "/run/service/gateway-hermes-kanban"]


def test_unknown_prompt_returns_404(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    configure_registry(monkeypatch, tmp_path)

    with pytest.raises(HTTPException) as exc:
        prompts.get_prompt("missing")

    assert exc.value.status_code == 404
