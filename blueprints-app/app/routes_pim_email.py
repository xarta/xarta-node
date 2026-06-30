"""Blueprints PIM Email API routes."""

from __future__ import annotations

import asyncio
import re
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel, Field

from .events import AppEvent
from .pim_email import (
    EmailConfigError,
    EmailCredentialError,
    EmailOperationError,
    PgEmailStore,
    fetch_message,
    fetch_message_security,
    fetch_remote_image_as_jpeg,
    list_folder_messages,
    list_folders,
    list_inbox,
    smtp_self_send,
    verify_email_image_signature,
)
from .pim_email_security import security_status

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


def _clean_security_run_id(value: str | None = None) -> str:
    clean = str(value or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_.:-]{8,120}", clean):
        return clean
    return uuid.uuid4().hex


def _event_severity_for_tone(tone: str) -> str:
    if str(tone or "").lower() == "red":
        return "error"
    if str(tone or "").lower() == "amber":
        return "warn"
    return "info"


def _security_progress_emitter(
    *,
    loop: asyncio.AbstractEventLoop,
    run_id: str,
    mailbox_id: str,
    folder: str,
    uid: str,
) -> Any:
    def emit(update: dict[str, Any]) -> None:
        payload = {
            **(update or {}),
            "run_id": run_id,
            "mailbox_id": mailbox_id,
            "folder": folder,
            "uid": uid,
        }
        event = AppEvent.create(
            "pim.email.security.progress",
            "Email Security Progress",
            "Email security check progress updated.",
            severity=_event_severity_for_tone(str(payload.get("tone") or "")),
            source="pim-email",
            payload=payload,
        )
        try:
            from .routes_events import publish_event

            asyncio.run_coroutine_threadsafe(publish_event(event), loop)
        except RuntimeError:
            return

    return emit


def _attach_security_run_id(security: dict[str, Any], run_id: str) -> None:
    progress = security.get("progress") if isinstance(security, dict) else None
    if not isinstance(progress, dict):
        security["progress"] = {}
        progress = security["progress"]
    progress["run_id"] = run_id


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
                "security_checks": security_status(),
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


@router.get("/folder-messages")
async def email_folder_messages(
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        messages = await list_folder_messages(mailbox, folder=folder, limit=limit)
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": folder,
            "messages": messages,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/messages/{uid}")
async def email_message(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        run_id = _clean_security_run_id(security_run_id)
        message = await fetch_message(
            mailbox,
            folder=folder,
            uid=uid,
            security_progress_callback=_security_progress_emitter(
                loop=asyncio.get_running_loop(),
                run_id=run_id,
                mailbox_id=mailbox.mailbox_id,
                folder=folder,
                uid=uid,
            ),
        )
        if isinstance(message.get("security"), dict):
            _attach_security_run_id(message["security"], run_id)
        await store.record_security_result(message, mailbox_id=mailbox.mailbox_id)
        return {"ok": True, "mailbox": mailbox.public_dict(), "message": message}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/messages/{uid}/security")
async def email_message_security(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        run_id = _clean_security_run_id(security_run_id)
        security = await fetch_message_security(
            mailbox,
            folder=folder,
            uid=uid,
            security_progress_callback=_security_progress_emitter(
                loop=asyncio.get_running_loop(),
                run_id=run_id,
                mailbox_id=mailbox.mailbox_id,
                folder=folder,
                uid=uid,
            ),
        )
        _attach_security_run_id(security, run_id)
        await store.record_security_result(
            {
                "uid": uid,
                "folder": folder,
                "headers": {"message_id": security.get("context", {}).get("message_id", "")},
                "security": security,
            },
            mailbox_id=mailbox.mailbox_id,
        )
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": folder,
            "uid": uid,
            "security": security,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/image-proxy")
async def email_image_proxy(
    src: str = Query(..., min_length=8, max_length=4096),
    sig: str = Query(..., min_length=32, max_length=128),
) -> Response:
    if not verify_email_image_signature(src, sig):
        raise HTTPException(status_code=403, detail="image proxy signature is invalid")
    try:
        jpeg = await fetch_remote_image_as_jpeg(src)
        return Response(
            content=jpeg,
            media_type="image/jpeg",
            headers={
                "Cache-Control": "private, max-age=86400",
                "X-Content-Type-Options": "nosniff",
            },
        )
    except EmailOperationError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/smtp-self-test")
async def email_smtp_self_test(body: SmtpSelfTestRequest) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox()
        proof = await smtp_self_send(mailbox, recipient=body.recipient)
        return {"ok": True, "proof": proof}
    except Exception as exc:
        raise _http_error(exc) from exc
