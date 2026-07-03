"""Blueprints PIM Email API routes."""

from __future__ import annotations

import asyncio
import base64
import hmac
import os
import re
import uuid
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field

from .events import AppEvent
from .pim_email import (
    DEFAULT_DOWNLOADED_FOLDER,
    EmailConfigError,
    EmailCredentialError,
    EmailOperationError,
    PgEmailStore,
    download_mailbox,
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


class DownloadMailboxRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    apply_remote_moves: bool = False
    downloaded_folder: str = Field(DEFAULT_DOWNLOADED_FOLDER, min_length=1, max_length=180)
    folder_allowlist: list[str] | None = None
    limit_per_folder: int | None = Field(None, ge=1, le=5000)
    max_messages: int | None = Field(None, ge=1, le=1000000)
    convergence_passes: int = Field(2, ge=1, le=5)


class ExternalImageAssignmentClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    run_id: str = Field("", max_length=180)
    limit: int = Field(1000, ge=1, le=5000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalImageAssignmentHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)


class ExternalImageAssignmentReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    reason: str = Field("worker_released_assignment", max_length=1000)


class ExternalImageAssignmentCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    transformed_image_base64: str = Field(..., min_length=1)
    raw_image_sha256: str = Field(..., pattern=r"^[0-9a-fA-F]{64}$")
    transformed_sha256: str = Field(..., pattern=r"^[0-9a-fA-F]{64}$")
    width: int = Field(..., ge=1, le=1800)
    height: int = Field(..., ge=1, le=2400)
    transform_version: str = Field("jpeg-v1", min_length=1, max_length=80)
    fetched_content_type: str = Field("", max_length=180)
    fetched_final_url: str = Field("", max_length=4096)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalImageAssignmentFailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    status: str = Field(..., min_length=3, max_length=40)
    reason: str = Field(..., min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)


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


def _email_worker_secret() -> str:
    return (
        os.environ.get("BLUEPRINTS_PIM_EMAIL_WORKER_SECRET")
        or os.environ.get("BLUEPRINTS_EMAIL_WORKER_SECRET")
        or ""
    ).strip()


def _require_email_worker_token(token: str | None) -> None:
    secret = _email_worker_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="PIM Email worker auth is not configured")
    if not token or not hmac.compare_digest(str(token), secret):
        raise HTTPException(status_code=401, detail="PIM Email worker token is invalid")


def _worker_safe_assignment(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item or {})
    public.pop("assignment_token", None)
    public.pop("email_uid", None)
    public.pop("input_raw_sha256", None)
    return public


def _worker_safe_shared_asset(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item or {})
    public.pop("storage_relpath", None)
    public.pop("encryption", None)
    return public


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
        if hasattr(store, "local_corpus_status"):
            local = await store.local_corpus_status()
        else:
            local = {"available": False, "message_count": 0}
        return {
            "ok": True,
            "storage": "postgres",
            "mailboxes": mailboxes,
            "local_corpus": local,
            "capabilities": {
                "imap_read": True,
                "local_corpus_read": True,
                "safe_local_download": True,
                "imap_uid_move_after_local_commit": True,
                "smtp_self_test": True,
                "smtp_general_send": False,
                "delete": False,
                "ai_send": False,
                "security_checks": security_status(),
            },
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/local/status")
async def email_local_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        status = await store.local_corpus_status(mailbox_id=mailbox_id)
        return {"ok": True, "status": status}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/workers/external-images/status")
async def email_external_image_worker_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_derivatives: bool = Query(False),
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        store = _store()
        assignments = await store.external_image_url_assignment_status_counts(mailbox_id=mailbox_id)
        response = {
            "ok": True,
            "url_assignments": assignments,
        }
        if include_derivatives:
            local = await store.local_corpus_status(mailbox_id=mailbox_id)
            response["external_image_derivatives"] = local.get("external_image_derivatives", {})
        return response
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/claim")
async def email_external_image_worker_claim_assignments(
    body: ExternalImageAssignmentClaimRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        result = await _store().claim_external_image_url_assignment_block(
            mailbox_id=body.mailbox_id,
            worker_id=body.worker_id,
            run_id=body.run_id,
            limit=body.limit,
            metadata={
                **body.metadata,
                "source_surface": "pim-email-worker-api",
            },
        )
        return {
            "ok": True,
            "schema": "xarta.pim_email.external_image_url_assignment.block.v1",
            "mailbox_id": result.get("mailbox_id"),
            "assignment_batch_id": result.get("assignment_batch_id"),
            "assignment_token": result.get("assignment_token"),
            "worker_id": result.get("worker_id"),
            "run_id": result.get("run_id"),
            "claimed": result.get("claimed"),
            "items": [_worker_safe_assignment(item) for item in result.get("items", [])],
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/heartbeat")
async def email_external_image_worker_heartbeat_assignments(
    body: ExternalImageAssignmentHeartbeatRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        result = await _store().heartbeat_external_image_url_assignment_block(
            assignment_batch_id=body.assignment_batch_id,
            worker_id=body.worker_id,
            assignment_token=body.assignment_token,
        )
        return {"ok": True, "result": result}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/release")
async def email_external_image_worker_release_assignments(
    body: ExternalImageAssignmentReleaseRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        result = await _store().release_external_image_url_assignment_block(
            assignment_batch_id=body.assignment_batch_id,
            worker_id=body.worker_id,
            assignment_token=body.assignment_token,
            reason=body.reason,
        )
        return {"ok": True, "result": result}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/{canonical_url_digest}/complete")
async def email_external_image_worker_complete_assignment(
    canonical_url_digest: str,
    body: ExternalImageAssignmentCompleteRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        try:
            transformed_content = base64.b64decode(
                body.transformed_image_base64,
                validate=True,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="transformed_image_base64 is invalid",
            ) from exc
        result = await _store().complete_external_image_url_assignment_with_transformed_payload(
            mailbox_id=body.mailbox_id,
            canonical_url_digest=canonical_url_digest,
            worker_id=body.worker_id,
            assignment_token=body.assignment_token,
            transformed_content=transformed_content,
            raw_image_sha256=body.raw_image_sha256,
            transformed_sha256=body.transformed_sha256,
            width=body.width,
            height=body.height,
            transform_version=body.transform_version,
            fetched_content_type=body.fetched_content_type,
            fetched_final_url=body.fetched_final_url,
            metadata={
                **body.metadata,
                "source_surface": "pim-email-worker-api",
            },
        )
        if "assignment" in result:
            result["assignment"] = _worker_safe_assignment(result.pop("assignment"))
        if "shared_asset" in result:
            result["shared_asset"] = _worker_safe_shared_asset(result["shared_asset"])
        return {"ok": bool(result.get("ok", True)), "result": result}
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/{canonical_url_digest}/fail")
async def email_external_image_worker_fail_assignment(
    canonical_url_digest: str,
    body: ExternalImageAssignmentFailRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
) -> dict[str, Any]:
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        result = await _store().fail_external_image_url_assignment(
            mailbox_id=body.mailbox_id,
            canonical_url_digest=canonical_url_digest,
            worker_id=body.worker_id,
            assignment_token=body.assignment_token,
            status=body.status,
            reason=body.reason,
            metadata={
                **body.metadata,
                "source_surface": "pim-email-worker-api",
            },
        )
        if "assignment" in result:
            result["assignment"] = _worker_safe_assignment(result.pop("assignment"))
        return {"ok": True, "result": result}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/local/folders")
async def email_local_folders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        folders = await store.local_folders(mailbox_id=mailbox.mailbox_id)
        return {"ok": True, "mailbox": mailbox.public_dict(), "folders": folders}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/local/folder-messages")
async def email_local_folder_messages(
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=200),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        messages = await store.local_folder_messages(
            mailbox_id=mailbox.mailbox_id,
            folder=folder,
            limit=limit,
            ensure_schema=False,
        )
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": folder,
            "messages": messages,
            "source": "local-corpus",
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/local/messages/{email_uid}")
async def email_local_message(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        message = await store.read_local_message(
            email_uid,
            mailbox_id=mailbox.mailbox_id,
            ensure_schema=False,
        )
        return {"ok": True, "mailbox": mailbox.public_dict(), "message": message}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/local/messages/{email_uid}/security")
async def email_local_message_security(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        run_id = _clean_security_run_id(security_run_id)
        result = await store.run_local_security_check(
            email_uid,
            mailbox_id=mailbox.mailbox_id,
            progress_callback=_security_progress_emitter(
                loop=asyncio.get_running_loop(),
                run_id=run_id,
                mailbox_id=mailbox.mailbox_id,
                folder="local-corpus",
                uid=email_uid,
            ),
        )
        _attach_security_run_id(result["security"], run_id)
        return {"ok": True, "mailbox": mailbox.public_dict(), **result}
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


@router.post("/download/run")
async def email_download_run(body: DownloadMailboxRequest) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(body.mailbox_id)
        result = await download_mailbox(
            mailbox,
            store=store,
            apply_remote_moves=body.apply_remote_moves,
            downloaded_folder=body.downloaded_folder,
            folder_allowlist=body.folder_allowlist,
            limit_per_folder=body.limit_per_folder,
            max_messages=body.max_messages,
            convergence_passes=body.convergence_passes,
            include_special_use=True,
            security_mode="run",
        )
        return {"ok": True, "result": result}
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
