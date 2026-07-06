"""Compatibility shim for stack-owned PIM Email security checks.

The PIM Email deterministic and LLM security implementation lives in the
node-local Dockge stack:

    /xarta-node/.lone-wolf/stacks/pim-email/pim_email_core

Blueprints keeps this module only for older imports. New backend/security work
belongs in the stack copy so the app process remains an API/control-plane proxy.
"""

from __future__ import annotations

import sys
from pathlib import Path

PIM_EMAIL_STACK_ROOT = Path("/xarta-node/.lone-wolf/stacks/pim-email")

if str(PIM_EMAIL_STACK_ROOT) not in sys.path:
    sys.path.insert(0, str(PIM_EMAIL_STACK_ROOT))

try:
    from pim_email_core.pim_email_security import *  # noqa: F401,F403
except ModuleNotFoundError as exc:  # pragma: no cover - startup configuration failure
    _IMPORT_ERROR = exc

    class EmailSecurityUnavailableError(RuntimeError):
        """Raised when stack-owned security checks are unavailable."""

    def _stack_security_unavailable() -> EmailSecurityUnavailableError:
        return EmailSecurityUnavailableError(
            "PIM Email security core is not available on this node. Expected "
            f"stack-owned code under {PIM_EMAIL_STACK_ROOT / 'pim_email_core'}."
        )

    def check_email_security_sync(*args, **kwargs):
        raise _stack_security_unavailable() from _IMPORT_ERROR

    async def check_email_security(*args, **kwargs):
        raise _stack_security_unavailable() from _IMPORT_ERROR

    def security_status(*args, **kwargs):
        return {
            "available": False,
            "error": str(_IMPORT_ERROR),
            "stack_root": str(PIM_EMAIL_STACK_ROOT),
        }
