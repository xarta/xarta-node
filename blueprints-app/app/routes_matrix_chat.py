"""routes_matrix_chat.py - Narrow Blueprints proxy for Matrix/Synapse chat.

This route intentionally exposes only the chat operations needed by the
Blueprints Settings -> Agents -> Chat page. Matrix credentials stay server-side
in ignored/private env files; browser responses are reduced DTOs, never raw
Matrix credentials or generic Matrix API proxy output.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field


async def _require_matrix_chat_auth(request: Request) -> None:
    """Require Blueprints token auth even on loopback for Matrix chat routes."""
    from . import config as cfg
    from .auth import verify_token

    if not (cfg.API_SECRET or cfg.SYNC_SECRET):
        return
    token = request.headers.get("x-api-token", "") or request.query_params.get("token", "")
    valid = (cfg.API_SECRET and verify_token(cfg.API_SECRET, token)) or (
        cfg.SYNC_SECRET and verify_token(cfg.SYNC_SECRET, token)
    )
    if not valid:
        raise HTTPException(status_code=401, detail="Unauthorized")


router = APIRouter(
    prefix="/matrix-chat",
    tags=["matrix-chat"],
    dependencies=[Depends(_require_matrix_chat_auth)],
)

_DEFAULT_ENV_FILE = "/xarta-node/.lone-wolf/stacks/matrix-synapse/.env"
_DEFAULT_UPSTREAM = "http://127.0.0.1:8008"
_DEFAULT_PUBLIC_HOMESERVER = "https://matrix.local"
_DEFAULT_HERMES_USER_ID = ""
_DEFAULT_SMOKE_ROOM_ID = ""
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 20.0
_MAX_MESSAGE_LIMIT = 100
_MAX_SYNC_TIMEOUT_MS = 30_000


class _CreateRoomBody(BaseModel):
    name: str = Field(default="Blueprints Chat", min_length=1, max_length=120)
    topic: str | None = Field(default=None, max_length=400)
    invite: list[str] = Field(default_factory=list, max_length=20)


class _JoinRoomBody(BaseModel):
    room_id_or_alias: str = Field(min_length=1, max_length=255)


class _InviteBody(BaseModel):
    user_id: str = Field(min_length=1, max_length=255)


class _SendMessageBody(BaseModel):
    body: str = Field(min_length=1, max_length=8000)


def _read_env_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = Path(path)
    if not env_path.is_file():
        return values
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]
            if key:
                values[key] = value
    except OSError:
        return {}
    return values


def _settings() -> dict[str, str]:
    env_file = os.getenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", _DEFAULT_ENV_FILE)
    file_values = _read_env_file(env_file)

    def pick(*names: str, default: str = "") -> str:
        for name in names:
            value = os.getenv(name)
            if value:
                return value.strip()
            value = file_values.get(name)
            if value:
                return value.strip()
        return default

    upstream = pick(
        "BLUEPRINTS_MATRIX_CHAT_UPSTREAM",
        "MATRIX_CHAT_UPSTREAM",
        default=os.getenv("MATRIX_SYNAPSE_UPSTREAM", _DEFAULT_UPSTREAM),
    )
    if not upstream.startswith(("http://", "https://")):
        upstream = f"http://{upstream}"

    public_homeserver = pick(
        "BLUEPRINTS_MATRIX_CHAT_HOMESERVER",
        "MATRIX_CHAT_HOMESERVER",
        default=os.getenv("MATRIX_SYNAPSE_HOSTNAME", ""),
    )
    if public_homeserver and not public_homeserver.startswith(("http://", "https://")):
        public_homeserver = f"https://{public_homeserver}"
    if not public_homeserver:
        public_homeserver = _DEFAULT_PUBLIC_HOMESERVER

    user_id = pick("MATRIX_CHAT_USER_ID", "MATRIX_CODEX_USER_ID")
    access_token = pick("MATRIX_CHAT_ACCESS_TOKEN", "MATRIX_CODEX_ACCESS_TOKEN")

    return {
        "env_file": env_file,
        "upstream": upstream.rstrip("/"),
        "public_homeserver": public_homeserver.rstrip("/"),
        "user_id": user_id,
        "access_token": access_token,
        "smoke_room_id": pick(
            "MATRIX_CHAT_DEFAULT_ROOM_ID",
            "MATRIX_HERMES_SMOKE_ROOM_ID",
            default=_DEFAULT_SMOKE_ROOM_ID,
        ),
        "hermes_user_id": pick(
            "MATRIX_CHAT_HERMES_USER_ID",
            "MATRIX_HERMES_USER_ID",
            default=_DEFAULT_HERMES_USER_ID,
        ),
    }


def _require_credentials() -> dict[str, str]:
    settings = _settings()
    if not settings["user_id"] or not settings["access_token"]:
        raise HTTPException(
            status_code=503,
            detail="Matrix chat credentials are not configured on the Blueprints server",
        )
    return settings


def _headers(settings: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {settings['access_token']}"}


def _matrix_path(path: str) -> str:
    return f"/_matrix/client/v3{path}"


async def _matrix_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
) -> dict[str, Any]:
    settings = _require_credentials()
    timeout = httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT)
    try:
        async with httpx.AsyncClient(base_url=settings["upstream"], timeout=timeout) as client:
            response = await client.request(
                method,
                _matrix_path(path),
                params=params,
                json=json_body,
                headers=_headers(settings),
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="Matrix homeserver is not reachable") from exc

    if response.status_code not in expected:
        if response.status_code in {401, 403}:
            detail = "Matrix chat credential rejected by homeserver"
        else:
            detail = f"Matrix request failed with HTTP {response.status_code}"
        raise HTTPException(status_code=502, detail=detail)

    if not response.content:
        return {}
    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Matrix homeserver returned invalid JSON") from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Matrix homeserver returned unexpected JSON")
    return data


def _event_content(event: dict[str, Any]) -> dict[str, Any]:
    content = event.get("content")
    return content if isinstance(content, dict) else {}


def _event_ts(event: dict[str, Any]) -> int | None:
    ts = event.get("origin_server_ts")
    return ts if isinstance(ts, int) else None


def _message_from_event(event: dict[str, Any], room_id: str) -> dict[str, Any] | None:
    event_type = event.get("type")
    content = _event_content(event)
    if event_type == "m.room.encrypted":
        body = "[encrypted event]"
        msgtype = "m.encrypted"
    elif event_type == "m.room.message":
        body = content.get("body")
        msgtype = content.get("msgtype") or "m.text"
        if not isinstance(body, str):
            return None
    else:
        return None

    relates_to = content.get("m.relates_to")
    return {
        "event_id": event.get("event_id") or "",
        "room_id": room_id,
        "sender": event.get("sender") or "",
        "origin_server_ts": _event_ts(event),
        "msgtype": msgtype,
        "body": body,
        "relates_to": relates_to if isinstance(relates_to, dict) else None,
    }


def _state_events(room: dict[str, Any], invite: bool = False) -> list[dict[str, Any]]:
    if invite:
        events = room.get("invite_state", {}).get("events", [])
    else:
        events = room.get("state", {}).get("events", [])
    return events if isinstance(events, list) else []


def _timeline_events(room: dict[str, Any]) -> list[dict[str, Any]]:
    events = room.get("timeline", {}).get("events", [])
    return events if isinstance(events, list) else []


def _is_room_id(value: str | None) -> bool:
    return bool(value and value.startswith("!") and ":" in value)


def _short_room_id(room_id: str) -> str:
    if not room_id:
        return ""
    if ":" not in room_id:
        return room_id
    local, server = room_id.split(":", 1)
    if len(local) <= 10:
        return f"{local}:{server}"
    return f"{local[:6]}...{local[-4:]}:{server}"


def _room_display_name(room_id: str, name: str, canonical_alias: str | None) -> str:
    if name and not _is_room_id(name):
        return name
    if canonical_alias:
        return canonical_alias
    return f"Unnamed room ({_short_room_id(room_id)})"


def _room_name_from_events(events: list[dict[str, Any]]) -> tuple[str, str | None, bool, str]:
    name = ""
    canonical_alias: str | None = None
    encrypted = False
    member_names: list[str] = []
    name_source = "missing"
    for event in events:
        event_type = event.get("type")
        content = _event_content(event)
        if event_type == "m.room.name" and isinstance(content.get("name"), str) and content["name"].strip():
            name = content["name"].strip()
            name_source = "m.room.name"
        elif event_type == "m.room.canonical_alias" and isinstance(content.get("alias"), str):
            canonical_alias = content["alias"]
        elif event_type == "m.room.encryption":
            encrypted = True
        elif event_type == "m.room.member" and content.get("membership") in {"join", "invite"}:
            display = content.get("displayname")
            if isinstance(display, str) and display and display not in member_names:
                member_names.append(display)
    if not name and canonical_alias:
        name = canonical_alias
        name_source = "m.room.canonical_alias"
    if not name and member_names:
        name = ", ".join(member_names[:3])
        name_source = "m.room.member"
    if name and _is_room_id(name):
        name_source = "fallback_room_id"
    return name, canonical_alias, encrypted, name_source


def _room_summary(room_id: str, room: dict[str, Any], *, invite: bool = False) -> dict[str, Any]:
    events = _state_events(room, invite=invite) + _timeline_events(room)
    name, canonical_alias, encrypted, name_source = _room_name_from_events(events)
    messages = [_message_from_event(event, room_id) for event in _timeline_events(room)]
    messages = [message for message in messages if message]
    last_message = messages[-1] if messages else None
    summary = room.get("summary") if isinstance(room.get("summary"), dict) else {}
    joined_count = summary.get("m.joined_member_count")
    invited_count = summary.get("m.invited_member_count")
    return {
        "room_id": room_id,
        "name": name or room_id,
        "display_name": _room_display_name(room_id, name, canonical_alias),
        "name_source": name_source if name else "fallback_room_id",
        "canonical_alias": canonical_alias,
        "joined_member_count": joined_count if isinstance(joined_count, int) else None,
        "invited_member_count": invited_count if isinstance(invited_count, int) else None,
        "invited": invite,
        "encrypted": encrypted,
        "last_event_ts": last_message["origin_server_ts"] if last_message else None,
        "last_preview": last_message["body"][:180] if last_message else "",
    }


def _rooms_from_sync(sync: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rooms = sync.get("rooms") if isinstance(sync.get("rooms"), dict) else {}
    joined_raw = rooms.get("join") if isinstance(rooms.get("join"), dict) else {}
    invited_raw = rooms.get("invite") if isinstance(rooms.get("invite"), dict) else {}
    joined = [
        _room_summary(room_id, room, invite=False)
        for room_id, room in joined_raw.items()
        if isinstance(room, dict)
    ]
    invited = [
        _room_summary(room_id, room, invite=True)
        for room_id, room in invited_raw.items()
        if isinstance(room, dict)
    ]
    joined.sort(key=lambda room: room.get("last_event_ts") or 0, reverse=True)
    invited.sort(key=lambda room: room.get("name") or room.get("room_id"))
    return joined, invited


async def _sync(
    *,
    since: str | None = None,
    timeout_ms: int = 0,
    full_state: bool = False,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "timeout": max(0, min(timeout_ms, _MAX_SYNC_TIMEOUT_MS)),
        "full_state": "true" if full_state else "false",
    }
    if since:
        params["since"] = since
    return await _matrix_request("GET", "/sync", params=params)


@router.get("/status")
async def matrix_chat_status() -> dict[str, Any]:
    settings = _settings()
    reachable = False
    health = ""
    try:
        async with httpx.AsyncClient(
            base_url=settings["upstream"],
            timeout=httpx.Timeout(_CONNECT_TIMEOUT),
        ) as client:
            response = await client.get("/health")
        reachable = response.status_code < 500
        health = response.text[:80]
    except httpx.RequestError:
        reachable = False

    return {
        "configured": bool(settings["user_id"] and settings["access_token"]),
        "reachable": reachable,
        "health": health,
        "homeserver_url": settings["public_homeserver"],
        "user_id": settings["user_id"] or None,
        "default_room_id": settings["smoke_room_id"],
        "hermes_user_id": settings["hermes_user_id"],
        "features": {
            "e2ee": False,
            "push_notifications": False,
            "generic_matrix_proxy": False,
        },
    }


@router.get("/rooms")
async def matrix_chat_rooms() -> dict[str, Any]:
    sync = await _sync(timeout_ms=0, full_state=True)
    joined, invited = _rooms_from_sync(sync)
    return {
        "next_batch": sync.get("next_batch") if isinstance(sync.get("next_batch"), str) else None,
        "joined": joined,
        "invites": invited,
    }


@router.post("/rooms")
async def matrix_chat_create_room(body: _CreateRoomBody) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": body.name.strip(),
        "preset": "private_chat",
        "visibility": "private",
    }
    if body.topic:
        payload["topic"] = body.topic.strip()
    clean_invites = [user.strip() for user in body.invite if user and user.strip()]
    if clean_invites:
        payload["invite"] = clean_invites
    data = await _matrix_request("POST", "/createRoom", json_body=payload)
    return {"room_id": data.get("room_id")}


@router.post("/rooms/join")
async def matrix_chat_join_room(body: _JoinRoomBody) -> dict[str, Any]:
    target = quote(body.room_id_or_alias.strip(), safe="")
    data = await _matrix_request("POST", f"/join/{target}", json_body={})
    return {"room_id": data.get("room_id")}


@router.post("/rooms/{room_id}/invite")
async def matrix_chat_invite(room_id: str, body: _InviteBody) -> dict[str, Any]:
    encoded_room = quote(room_id, safe="")
    await _matrix_request(
        "POST",
        f"/rooms/{encoded_room}/invite",
        json_body={"user_id": body.user_id.strip()},
        expected=(200,),
    )
    return {"ok": True}


@router.get("/rooms/{room_id}/messages")
async def matrix_chat_messages(
    room_id: str,
    limit: int = Query(default=50, ge=1, le=_MAX_MESSAGE_LIMIT),
    from_token: str | None = Query(default=None, alias="from"),
) -> dict[str, Any]:
    encoded_room = quote(room_id, safe="")
    params: dict[str, Any] = {"dir": "b", "limit": limit}
    if from_token:
        params["from"] = from_token
    data = await _matrix_request("GET", f"/rooms/{encoded_room}/messages", params=params)
    chunk = data.get("chunk") if isinstance(data.get("chunk"), list) else []
    messages = [_message_from_event(event, room_id) for event in chunk if isinstance(event, dict)]
    messages = [message for message in messages if message]
    messages.reverse()
    return {
        "room_id": room_id,
        "messages": messages,
        "start": data.get("start") if isinstance(data.get("start"), str) else None,
        "end": data.get("end") if isinstance(data.get("end"), str) else None,
    }


@router.post("/rooms/{room_id}/messages")
async def matrix_chat_send_message(room_id: str, body: _SendMessageBody) -> dict[str, Any]:
    encoded_room = quote(room_id, safe="")
    txn_id = f"bp-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
    encoded_txn = quote(txn_id, safe="")
    data = await _matrix_request(
        "PUT",
        f"/rooms/{encoded_room}/send/m.room.message/{encoded_txn}",
        json_body={"msgtype": "m.text", "body": body.body},
        expected=(200,),
    )
    return {
        "room_id": room_id,
        "event_id": data.get("event_id"),
    }


@router.get("/sync")
async def matrix_chat_sync(
    since: str | None = None,
    timeout_ms: int = Query(default=0, ge=0, le=_MAX_SYNC_TIMEOUT_MS),
) -> dict[str, Any]:
    sync = await _sync(since=since, timeout_ms=timeout_ms, full_state=False)
    joined, invited = _rooms_from_sync(sync)
    rooms = sync.get("rooms") if isinstance(sync.get("rooms"), dict) else {}
    joined_raw = rooms.get("join") if isinstance(rooms.get("join"), dict) else {}
    room_updates = []
    for room_id, room in joined_raw.items():
        if not isinstance(room, dict):
            continue
        messages = [
            _message_from_event(event, room_id)
            for event in _timeline_events(room)
            if isinstance(event, dict)
        ]
        room_updates.append(
            {
                "room_id": room_id,
                "messages": [message for message in messages if message],
                "limited": bool(room.get("timeline", {}).get("limited"))
                if isinstance(room.get("timeline"), dict)
                else False,
            }
        )
    return {
        "next_batch": sync.get("next_batch") if isinstance(sync.get("next_batch"), str) else None,
        "joined": joined,
        "invites": invited,
        "room_updates": room_updates,
    }
