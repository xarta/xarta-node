"""Thin factory layer for node-local AI observability backends."""

from __future__ import annotations

from dataclasses import dataclass
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any, Protocol


class AiObservabilityBackend(Protocol):
    backend_name: str

    async def describe(self, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        ...

    async def test_alias(self, alias: str, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        ...

    async def propose_db_links(self, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        ...


@dataclass
class _UnavailableBackend:
    backend_name: str = "none"
    reason: str = "No supported local AI observability backend is available on this node."
    present: bool = False

    async def describe(self, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "panel_visible": bool(self.present),
            "available": False,
            "backend": self.backend_name,
            "error": self.reason,
            "stack": {
                "name": "litellm",
                "present": bool(self.present),
                "running": False,
                "message": self.reason,
            },
            "models": [],
            "counts": {"aliases": 0, "db_linked": 0},
        }

    async def test_alias(self, alias: str, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "ok": False,
            "alias": alias,
            "status": "backend_unavailable",
            "detail": self.reason,
            "backend": self.backend_name,
        }

    async def propose_db_links(self, db_providers: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            "ok": False,
            "status": "backend_unavailable",
            "detail": self.reason,
            "backend": self.backend_name,
        }


_CANDIDATE_BACKENDS: tuple[tuple[str, Path], ...] = (
    (
        "litellm-local",
        Path("/xarta-node/.lone-wolf/stacks/litellm/observability_backend.py"),
    ),
)


def get_ai_observability_backend() -> AiObservabilityBackend:
    for backend_name, path in _CANDIDATE_BACKENDS:
        if not path.is_file():
            continue
        try:
            spec = spec_from_file_location(f"xarta_{backend_name}_observability", path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"Could not load backend module from {path}")
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
            if hasattr(module, "get_backend"):
                backend = module.get_backend()
            elif hasattr(module, "LiteLLMObservabilityBackend"):
                backend = module.LiteLLMObservabilityBackend()
            else:
                raise RuntimeError("Backend module does not expose get_backend()")
            return backend
        except Exception as exc:
            return _UnavailableBackend(
                backend_name=backend_name,
                reason=f"{backend_name} backend failed to load: {exc}",
                present=True,
            )
    return _UnavailableBackend()
