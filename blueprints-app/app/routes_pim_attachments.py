"""Same-origin Operator transport for the checked PIM attachment API."""

from __future__ import annotations

import hmac
import re
from pathlib import Path

import anyio
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from . import config as cfg
from .auth import verify_token
from .routes_pim_email import PIM_EMAIL_STACK_API_BASE, _stack_http_client

PREFIX = "/personal/email/local/messages/{email_uid}/attachments"
router = APIRouter(prefix=PREFIX, tags=["personal-email-attachments"])
CREDENTIAL = Path("/var/lib/xarta/pim-attachments/proxy-key")
ID = re.compile(r"[A-Za-z0-9._-]{1,120}")
REF = re.compile(r"[0-9a-f]{64}")
CHECK = re.compile(r"[0-9a-f]{32}")
CREDENTIAL_FORMAT = REF
NO_STORE = {"Cache-Control": "private, no-store"}
JSON_LIMIT = 65536
DOWNLOAD_LIMIT = 15 * 1024 * 1024
BINARY_HEADERS = ("content-disposition", "content-type", "content-length", "x-content-type-options")
TIMEOUT = httpx.Timeout(15, connect=2, pool=2)


class Prepare(BaseModel):
    model_config = ConfigDict(extra="forbid")
    message_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    idempotency_key: str = Field(pattern=r"^[A-Za-z0-9._~-]{1,80}$")


def error(status: int, reason: str, action: str = "none") -> HTTPException:
    return HTTPException(
        status,
        {"reason_code": reason, "permitted_action": action, "retryable": action != "none"},
        headers={**NO_STORE, **({"Retry-After": "2"} if status == 503 else {})},
    )


def authorize(request: Request):
    token = request.headers.get("x-api-token", "")
    try:
        operator = bytes.fromhex(cfg.API_SECRET)
        sync = bytes.fromhex(cfg.SYNC_SECRET)
    except ValueError:
        raise error(503, "operator_auth_unavailable", "retry") from None
    if not operator or hmac.compare_digest(operator, sync):
        raise error(503, "operator_auth_unavailable", "retry")
    if not REF.fullmatch(token) or not verify_token(cfg.API_SECRET, token):
        raise error(401, "operator_auth_required")
    if request.url.scheme != "https":
        raise error(403, "https_required")
    try:
        service_key = CREDENTIAL.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        raise error(503, "attachment_service_unavailable", "retry") from None
    if not CREDENTIAL_FORMAT.fullmatch(service_key):
        raise error(503, "attachment_service_unavailable", "retry")
    headers = {"X-PIM-Attachment-Key": service_key, "X-PIM-Attachment-Scope": "blueprints-operator"}
    for name in ("x-attachment-flow", "range"):
        if name in request.headers:
            headers[name] = request.headers[name]
    return headers


class ProxyDownload(StreamingResponse):
    def __init__(self, upstream: httpx.Response):
        self.upstream = upstream
        super().__init__(
            upstream.aiter_raw(),
            headers={**NO_STORE, **{name: upstream.headers[name] for name in BINARY_HEADERS}},
        )

    async def __call__(self, scope, receive, send):
        try:
            with anyio.fail_after(65):
                await super().__call__(scope, receive, send)
        finally:
            with anyio.CancelScope(shield=True):
                await self.upstream.aclose()


async def proxy(
    request: Request,
    headers: dict,
    email_uid: str,
    mailbox_id: str | None,
    attachment_ref: str | None = None,
    check_id: str | None = None,
    operation: str = "",
    body: Prepare | None = None,
):
    if (
        not ID.fullmatch(email_uid)
        or (mailbox_id is not None and not ID.fullmatch(mailbox_id))
        or (attachment_ref is not None and not REF.fullmatch(attachment_ref))
        or (check_id is not None and not CHECK.fullmatch(check_id))
    ):
        raise error(404, "not_found")
    path = f"/internal/attachments/messages/{email_uid}"
    if attachment_ref:
        path += f"/{attachment_ref}"
    if operation:
        path += "/checks"
    if check_id:
        path += f"/{check_id}"
    if operation == "download":
        path += "/download"
    client = await _stack_http_client("foreground")
    upstream = None
    handed_off = False
    try:
        outbound = client.build_request(
            request.method,
            f"{PIM_EMAIL_STACK_API_BASE}{path}",
            params={"mailbox_id": mailbox_id} if mailbox_id is not None else None,
            headers=headers,
            json=body.model_dump() if body is not None else None,
            timeout=TIMEOUT,
        )
        upstream = await client.send(outbound, stream=True, follow_redirects=False)
        if operation == "download" and upstream.status_code == 200:
            try:
                valid = (
                    set(BINARY_HEADERS) <= upstream.headers.keys()
                    and upstream.headers["content-type"] == "application/octet-stream"
                    and upstream.headers["content-disposition"].startswith("attachment; ")
                    and upstream.headers["x-content-type-options"] == "nosniff"
                    and "content-encoding" not in upstream.headers
                    and 0 < int(upstream.headers["content-length"]) <= DOWNLOAD_LIMIT
                )
            except ValueError:
                valid = False
            if not valid:
                raise error(502, "invalid_download_response", "recheck")
            result = ProxyDownload(upstream)
            handed_off = True
            return result
        if not upstream.headers.get("content-type", "").startswith("application/json"):
            raise error(502, "invalid_attachment_response", "retry")
        data = bytearray()
        async for chunk in upstream.aiter_bytes(chunk_size=4096):
            data.extend(chunk)
            if len(data) > JSON_LIMIT:
                raise error(502, "invalid_attachment_response", "retry")
        response_headers = dict(NO_STORE)
        if upstream.headers.get("retry-after") == "2":
            response_headers["Retry-After"] = "2"
        return Response(bytes(data), upstream.status_code, response_headers, "application/json")
    except httpx.HTTPError:
        raise error(
            503, "attachment_service_unavailable", "recheck" if operation == "download" else "retry"
        ) from None
    finally:
        if upstream is not None and not handed_off:
            with anyio.CancelScope(shield=True):
                await upstream.aclose()


@router.get("")
async def list_attachments(
    request: Request,
    email_uid: str,
    headers: dict = Depends(authorize),
    mailbox_id: str | None = None,
):
    return await proxy(request, headers, email_uid, mailbox_id)


@router.get("/{attachment_ref}")
async def properties(
    request: Request,
    email_uid: str,
    attachment_ref: str,
    headers: dict = Depends(authorize),
    mailbox_id: str | None = None,
):
    return await proxy(request, headers, email_uid, mailbox_id, attachment_ref)


@router.post(
    "/{attachment_ref}/checks",
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": Prepare.model_json_schema()}},
        }
    },
)
async def prepare(
    request: Request,
    email_uid: str,
    attachment_ref: str,
    headers: dict = Depends(authorize),
    mailbox_id: str | None = None,
):
    if request.headers.get("content-type", "").split(";", 1)[0].lower() != "application/json":
        raise error(415, "json_required")
    data = bytearray()
    try:
        with anyio.fail_after(5):
            async for chunk in request.stream():
                if len(data) + len(chunk) > 512:
                    raise error(413, "request_body_limit")
                data.extend(chunk)
    except TimeoutError:
        raise error(408, "request_timeout", "retry") from None
    try:
        body = Prepare.model_validate_json(data)
    except ValidationError:
        raise error(422, "invalid_prepare_request") from None
    return await proxy(
        request, headers, email_uid, mailbox_id, attachment_ref, operation="prepare", body=body
    )


@router.get("/{attachment_ref}/checks/{check_id}")
async def poll(
    request: Request,
    email_uid: str,
    attachment_ref: str,
    check_id: str,
    headers: dict = Depends(authorize),
    mailbox_id: str | None = None,
):
    return await proxy(request, headers, email_uid, mailbox_id, attachment_ref, check_id, "poll")


@router.post("/{attachment_ref}/checks/{check_id}/download")
async def download(
    request: Request,
    email_uid: str,
    attachment_ref: str,
    check_id: str,
    headers: dict = Depends(authorize),
    mailbox_id: str | None = None,
):
    return await proxy(
        request, headers, email_uid, mailbox_id, attachment_ref, check_id, "download"
    )
