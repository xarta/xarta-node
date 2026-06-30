"""Blueprints PIM Email API routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from .pim_email import (
    EmailConfigError,
    EmailCredentialError,
    EmailOperationError,
    PgEmailStore,
    fetch_message,
    list_folders,
    list_inbox,
    smtp_self_send,
)

router = APIRouter(prefix="/personal/email", tags=["personal-email"])


class SmtpSelfTestRequest(BaseModel):
    recipient: str = Field(..., min_length=3, max_length=254)


def _store() -> PgEmailStore:
    return PgEmailStore()


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, EmailConfigError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, EmailCredentialError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, EmailOperationError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=502, detail="Email middleware operation failed")


@router.get("/status")
async def email_status() -> dict[str, Any]:
    try:
        store = _store()
        await store.ensure_schema()
        mailboxes = await store.public_mailboxes()
        return {
            "ok": True,
            "storage": "postgres",
            "mailboxes": mailboxes,
            "capabilities": {
                "imap_read": True,
                "smtp_self_test": True,
                "smtp_general_send": False,
                "delete": False,
                "ai_send": False,
            },
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/folders")
async def email_folders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        folders = await list_folders(mailbox)
        return {"ok": True, "mailbox": mailbox.public_dict(), "folders": folders}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/inbox")
async def email_inbox(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        messages = await list_inbox(mailbox, limit=limit)
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": "INBOX",
            "messages": messages,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/messages/{uid}")
async def email_message(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        message = await fetch_message(mailbox, folder=folder, uid=uid)
        return {"ok": True, "mailbox": mailbox.public_dict(), "message": message}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/smtp-self-test")
async def email_smtp_self_test(body: SmtpSelfTestRequest) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox()
        proof = await smtp_self_send(mailbox, recipient=body.recipient)
        return {"ok": True, "proof": proof}
    except Exception as exc:
        raise _http_error(exc) from exc
