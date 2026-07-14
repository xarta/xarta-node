"""Allowlisted prompt editor API for Personal/Kanban LLM workflows."""

from __future__ import annotations

import contextlib
import hashlib
import os
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/personal/prompts", tags=["personal-prompts"])

HERMES_LOCAL_ROOT = Path(
    os.environ.get("BLUEPRINTS_HERMES_LOCAL_STACK", "/xarta-node/.lone-wolf/stacks/hermes-local")
)
ALLOWED_PROMPT_ROOTS = (HERMES_LOCAL_ROOT / "config",)
MAX_PROMPT_BYTES = 512 * 1024
HERMES_PROFILE_APPLY_PREFIX = "hermes-profile:"


@dataclass(frozen=True)
class PromptSpec:
    prompt_id: str
    label: str
    surface: str
    group: str
    description: str
    path: Path
    apply_strategy: str = "write-file"
    live_path: Path | None = None
    restart_label: str = ""
    processor_kind: str = ""
    model_route_id: str = ""
    prompt_role: str = ""


PROMPT_REGISTRY: dict[str, PromptSpec] = {
    "hermes-kanban-soul": PromptSpec(
        prompt_id="hermes-kanban-soul",
        label="Blocker Resolver SOUL.md",
        surface="kanban",
        group="Hermes Kanban",
        description="Profile-level SOUL prompt for the hermes-kanban blocker resolver gateway.",
        path=HERMES_LOCAL_ROOT / "config/profiles/hermes-kanban/SOUL.md",
        apply_strategy="hermes-kanban-profile",
        live_path=HERMES_LOCAL_ROOT / "data/profiles/hermes-kanban/SOUL.md",
        restart_label="Refresh hermes-kanban profile gateway",
    ),
    "hermes-kanban-blocker-resolver-system": PromptSpec(
        prompt_id="hermes-kanban-blocker-resolver-system",
        label="Blocker Resolver system prompt",
        surface="kanban",
        group="Hermes Kanban",
        description="System prompt read by the scheduled/manual Kanban blocker resolver script.",
        path=HERMES_LOCAL_ROOT / "config/prompts/hermes-kanban-blocker-resolver-system.md",
        apply_strategy="write-file",
        restart_label="No restart required",
    ),
    "hermes-kanban-preprocessor-soul": PromptSpec(
        prompt_id="hermes-kanban-preprocessor-soul",
        label="Preprocessor SOUL.md",
        surface="kanban",
        group="Hermes Kanban Processors",
        description="Profile-level SOUL prompt for the hermes-kanban-preprocessor gateway.",
        path=HERMES_LOCAL_ROOT / "config/profiles/hermes-kanban-preprocessor/SOUL.md",
        apply_strategy="hermes-profile:hermes-kanban-preprocessor",
        live_path=HERMES_LOCAL_ROOT / "data/profiles/hermes-kanban-preprocessor/SOUL.md",
        restart_label="Refresh hermes-kanban-preprocessor profile gateway",
    ),
    "hermes-kanban-review-processor-soul": PromptSpec(
        prompt_id="hermes-kanban-review-processor-soul",
        label="Review Processor SOUL.md",
        surface="kanban",
        group="Hermes Kanban Processors",
        description="Profile-level SOUL prompt for the hermes-kanban-review-processor gateway.",
        path=HERMES_LOCAL_ROOT / "config/profiles/hermes-kanban-review-processor/SOUL.md",
        apply_strategy="hermes-profile:hermes-kanban-review-processor",
        live_path=HERMES_LOCAL_ROOT / "data/profiles/hermes-kanban-review-processor/SOUL.md",
        restart_label="Refresh hermes-kanban-review-processor profile gateway",
    ),
    "kanban-review-processor-system": PromptSpec(
        prompt_id="kanban-review-processor-system",
        label="Review Processor system prompt",
        surface="kanban",
        group="Hermes Kanban Processors",
        description="Runtime system prompt read by the profile-backed Kanban Review Processor marker worker.",
        path=HERMES_LOCAL_ROOT / "config/prompts/kanban-review-processor-system.md",
        apply_strategy="write-file",
        restart_label="No restart required; next marker uses latest prompt",
    ),
    "kanban-preprocessing-system": PromptSpec(
        prompt_id="kanban-preprocessing-system",
        label="Preprocessor system prompt",
        surface="kanban",
        group="Hermes Kanban Processors",
        description="Runtime system prompt read by the profile-backed Kanban ToDo-leaf preprocessing worker.",
        path=HERMES_LOCAL_ROOT / "config/prompts/kanban-preprocessing-system.md",
        apply_strategy="write-file",
        restart_label="No restart required; next marker uses latest prompt",
    ),
}

KANBAN_PROCESSOR_PROMPT_ROUTE_LABELS = {
    "chatgpt-5-6-sol": "ChatGPT 5.6 Sol",
    "chatgpt-5-6-terra": "ChatGPT 5.6 Terra",
    "chatgpt-5-6-luna": "ChatGPT 5.6 Luna",
    "chatgpt-5-5": "ChatGPT 5.5",
    "private-local-no-think": "Private local Qwen no-think",
    "private-local-thinking": "Private local Qwen thinking",
}

for _processor_kind, _processor_label in (
    ("preprocessing", "Preprocessor"),
    ("review", "Review Processor"),
):
    for _route_id, _route_label in KANBAN_PROCESSOR_PROMPT_ROUTE_LABELS.items():
        for _prompt_role, _role_label, _filename in (
            ("soul", "SOUL overlay", "soul.md"),
            ("system", "system prompt", "system.md"),
        ):
            _prompt_id = f"kanban-{_processor_kind}-{_route_id}-{_prompt_role}"
            PROMPT_REGISTRY[_prompt_id] = PromptSpec(
                prompt_id=_prompt_id,
                label=f"{_processor_label} · {_route_label} · {_role_label}",
                surface="kanban",
                group=f"{_processor_label} model variants",
                description=(
                    f"Automatically selected {_prompt_role} instructions for the "
                    f"allowlisted {_route_label} route."
                ),
                path=(
                    HERMES_LOCAL_ROOT
                    / "config/prompts/kanban-model-variants"
                    / _processor_kind
                    / _route_id
                    / _filename
                ),
                apply_strategy="write-file",
                restart_label="No restart required; next model attempt uses latest prompt",
                processor_kind=_processor_kind,
                model_route_id=_route_id,
                prompt_role=_prompt_role,
            )


class PromptApplyRequest(BaseModel):
    content: str
    actor: str = "blueprints-ui"
    source_surface: str = "personal-prompts"
    restart: bool = True


def _sha256_text(content: str) -> str:
    return "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()


def _tail(text: str, limit: int = 2000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _display_command(command: list[str]) -> list[str]:
    if len(command) <= 4:
        return command
    return [*command[:4], "..."]


def _run_command(command: list[str], timeout: int = 60) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "command": _display_command(command),
            "error": str(exc),
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "command": _display_command(command),
            "error": f"timed out after {timeout}s",
            "stdout_preview": _tail(exc.stdout or ""),
            "stderr_preview": _tail(exc.stderr or ""),
        }
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "command": _display_command(command),
        "stdout_preview": _tail(proc.stdout),
        "stderr_preview": _tail(proc.stderr),
    }


def _path_allowed(path: Path) -> bool:
    resolved = path.resolve(strict=False)
    for root in ALLOWED_PROMPT_ROOTS:
        try:
            if resolved.is_relative_to(root.resolve(strict=False)):
                return True
        except OSError:
            continue
    return False


def _prompt_spec(prompt_id: str) -> PromptSpec:
    spec = PROMPT_REGISTRY.get(prompt_id)
    if not spec:
        raise HTTPException(status_code=404, detail="Unknown prompt source")
    if not _path_allowed(spec.path):
        raise HTTPException(status_code=500, detail="Prompt source is outside allowed roots")
    return spec


def _read_prompt(spec: PromptSpec) -> str:
    try:
        return spec.path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Prompt source file is missing") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"Unable to read prompt source: {exc}") from exc


def _write_prompt(spec: PromptSpec, content: str) -> bool:
    if len(content.encode("utf-8")) > MAX_PROMPT_BYTES:
        raise HTTPException(status_code=413, detail="Prompt content is too large")
    previous = spec.path.read_text(encoding="utf-8") if spec.path.exists() else ""
    if previous == content:
        return False

    existing_stat = spec.path.stat() if spec.path.exists() else None
    owner_source = spec.path.parent
    missing_parents: list[Path] = []
    while not owner_source.exists():
        missing_parents.append(owner_source)
        owner_source = owner_source.parent
    owner_stat = existing_stat or owner_source.stat()
    spec.path.parent.mkdir(parents=True, exist_ok=True)
    for directory in reversed(missing_parents):
        directory.chmod(0o755)
        if os.geteuid() == 0:
            os.chown(directory, owner_stat.st_uid, owner_stat.st_gid)

    target_mode = stat.S_IMODE(existing_stat.st_mode) if existing_stat else 0o644
    fd, temp_path = tempfile.mkstemp(prefix=f".{spec.path.name}.", dir=spec.path.parent)
    try:
        os.fchmod(fd, target_mode)
        if os.geteuid() == 0:
            os.fchown(fd, owner_stat.st_uid, owner_stat.st_gid)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        os.replace(temp_path, spec.path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(temp_path)
        raise
    return True


def _prompt_summary(spec: PromptSpec) -> dict[str, Any]:
    stat = spec.path.stat() if spec.path.exists() else None
    return {
        "id": spec.prompt_id,
        "label": spec.label,
        "surface": spec.surface,
        "group": spec.group,
        "description": spec.description,
        "path": str(spec.path),
        "live_path": str(spec.live_path) if spec.live_path else "",
        "apply_strategy": spec.apply_strategy,
        "restart_label": spec.restart_label,
        "processor_kind": spec.processor_kind,
        "model_route_id": spec.model_route_id,
        "prompt_role": spec.prompt_role,
        "exists": bool(stat),
        "updated_at": stat.st_mtime if stat else None,
        "size": stat.st_size if stat else 0,
    }


def _apply_prompt(spec: PromptSpec, restart: bool) -> list[dict[str, Any]]:
    profile = ""
    if spec.apply_strategy == "hermes-kanban-profile":
        profile = "hermes-kanban"
    elif spec.apply_strategy.startswith(HERMES_PROFILE_APPLY_PREFIX):
        profile = spec.apply_strategy.removeprefix(HERMES_PROFILE_APPLY_PREFIX).strip()
    if not profile:
        return []

    install_command = [
        "docker",
        "exec",
        "hermes-local",
        "/opt/data/scripts/install_hermes_kanban_profile.py",
        "--force",
        "--json",
    ]
    if profile != "hermes-kanban":
        install_command[4:4] = ["--profile", profile]
    actions = [_run_command(install_command, timeout=90)]
    if restart and actions[-1].get("ok"):
        actions.append(
            _run_command(
                [
                    "docker",
                    "exec",
                    "hermes-local",
                    "/command/s6-svc",
                    "-t",
                    f"/run/service/gateway-{profile}",
                ],
                timeout=20,
            )
        )
    return actions


@router.get("")
def list_prompts() -> dict[str, Any]:
    return {"ok": True, "prompts": [_prompt_summary(spec) for spec in PROMPT_REGISTRY.values()]}


@router.get("/{prompt_id}")
def get_prompt(prompt_id: str) -> dict[str, Any]:
    spec = _prompt_spec(prompt_id)
    content = _read_prompt(spec)
    return {
        "ok": True,
        "prompt": {
            **_prompt_summary(spec),
            "content": content,
            "sha256": _sha256_text(content),
        },
    }


@router.post("/{prompt_id}/apply")
def apply_prompt(prompt_id: str, request: PromptApplyRequest) -> dict[str, Any]:
    spec = _prompt_spec(prompt_id)
    changed = _write_prompt(spec, request.content)
    actions = _apply_prompt(spec, request.restart)
    ok = all(action.get("ok") for action in actions) if actions else True
    return {
        "ok": ok,
        "changed": changed,
        "actions": actions,
        "prompt": {
            **_prompt_summary(spec),
            "content": request.content,
            "sha256": _sha256_text(request.content),
        },
        "actor": request.actor,
        "source_surface": request.source_surface,
    }
