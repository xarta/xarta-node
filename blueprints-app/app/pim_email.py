"""Compatibility shim for the stack-owned PIM Email backend core.

The PIM Email backend/store implementation lives in the node-local Dockge stack:

    /xarta-node/.lone-wolf/stacks/pim-email/pim_email_core

Blueprints should treat PIM Email as a stack-owned service surface. This shim is
kept only so older imports fail toward the stack copy instead of carrying SQL
and mailbox/backend logic in the public app package.
"""

from __future__ import annotations

import sys
from pathlib import Path

PIM_EMAIL_STACK_ROOT = Path("/xarta-node/.lone-wolf/stacks/pim-email")

if str(PIM_EMAIL_STACK_ROOT) not in sys.path:
    sys.path.insert(0, str(PIM_EMAIL_STACK_ROOT))

try:
    from pim_email_core.pim_email import *  # noqa: F401,F403
except ModuleNotFoundError as exc:  # pragma: no cover - startup configuration failure
    _IMPORT_ERROR = exc
    DEFAULT_DOWNLOADED_FOLDER = "Downloaded"

    class EmailConfigError(RuntimeError):
        pass

    class EmailCredentialError(RuntimeError):
        pass

    class EmailOperationError(RuntimeError):
        pass

    def _stack_core_unavailable() -> EmailConfigError:
        return EmailConfigError(
            "PIM Email backend core is not available on this node. Expected "
            f"stack-owned code under {PIM_EMAIL_STACK_ROOT / 'pim_email_core'}."
        )

    class PgEmailStore:
        def __init__(self, *args, **kwargs) -> None:
            raise _stack_core_unavailable() from _IMPORT_ERROR

    async def list_folders(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR

    async def list_inbox(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR

    async def list_folder_messages(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR

    async def fetch_message(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR

    async def fetch_message_security(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR

    async def smtp_self_send(*args, **kwargs):
        raise _stack_core_unavailable() from _IMPORT_ERROR
