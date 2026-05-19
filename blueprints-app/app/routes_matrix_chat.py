"""routes_matrix_chat.py - Narrow Blueprints proxy for Matrix/Synapse chat.

This route intentionally exposes only the chat operations needed by the
Blueprints Settings -> Agents -> Chat page. Matrix credentials stay server-side
in ignored/private env files; browser responses are reduced DTOs, never raw
Matrix credentials or generic Matrix API proxy output.
"""

from __future__ import annotations

import asyncio
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
_DEFAULT_CRYPTO_STORE_DIR = (
    "/xarta-node/.lone-wolf/stacks/matrix-synapse/data/blueprints-chat/crypto-store"
)


class _CreateRoomBody(BaseModel):
    name: str = Field(default="Blueprints Chat", min_length=1, max_length=120)
    topic: str | None = Field(default=None, max_length=400)
    invite: list[str] = Field(default_factory=list, max_length=20)
    encrypted: bool = False


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
        "operator_user_id": pick("MATRIX_CHAT_OPERATOR_USER_ID", "MATRIX_OPERATOR_USER_ID"),
        "admin_user_id": pick("MATRIX_CHAT_ADMIN_USER_ID", "MATRIX_ADMIN_USER_ID"),
        "admin_access_token": pick(
            "MATRIX_CHAT_ADMIN_ACCESS_TOKEN",
            "MATRIX_ADMIN_ACCESS_TOKEN",
        ),
        "encryption": pick("MATRIX_CHAT_ENCRYPTION", "BLUEPRINTS_MATRIX_CHAT_ENCRYPTION"),
        "device_id": pick("MATRIX_CHAT_DEVICE_ID", "BLUEPRINTS_MATRIX_CHAT_DEVICE_ID"),
        "crypto_store_dir": pick(
            "MATRIX_CHAT_CRYPTO_STORE_DIR",
            "BLUEPRINTS_MATRIX_CHAT_CRYPTO_STORE_DIR",
            default=_DEFAULT_CRYPTO_STORE_DIR,
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


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes", "on"}


def _e2ee_requested(settings: dict[str, str]) -> bool:
    return _truthy(settings.get("encryption"))


def _check_e2ee_deps() -> tuple[bool, str]:
    try:
        import aiosqlite  # noqa: F401
        import asyncpg  # noqa: F401
        import olm  # noqa: F401
        from mautrix.crypto import OlmMachine  # noqa: F401
        from mautrix.crypto.store.asyncpg import PgCryptoStore  # noqa: F401
        from mautrix.util.async_db import Database  # noqa: F401

        return True, ""
    except Exception as exc:  # pragma: no cover - depends on optional runtime deps
        return False, f"{type(exc).__name__}: {exc}"


def _secure_crypto_store(store_dir: Path) -> None:
    store_dir.mkdir(parents=True, exist_ok=True)
    try:
        store_dir.chmod(0o700)
    except OSError:
        pass
    for path in store_dir.glob("crypto.db*"):
        if path.is_file():
            try:
                path.chmod(0o600)
            except OSError:
                pass


class _MatrixCryptoStateStore:
    """Minimal mautrix crypto state-store adapter for the Blueprints chat client."""

    def __init__(self, client_state_store: Any, joined_rooms: set[str]):
        self._state_store = client_state_store
        self._joined_rooms = joined_rooms

    async def is_encrypted(self, room_id: str) -> bool:
        return (await self.get_encryption_info(room_id)) is not None

    async def get_encryption_info(self, room_id: str) -> Any:
        if hasattr(self._state_store, "get_encryption_info"):
            return await self._state_store.get_encryption_info(room_id)
        return None

    async def find_shared_rooms(self, user_id: str) -> list[str]:
        return list(self._joined_rooms)


class _MatrixChatE2EEClient:
    """Small server-side Matrix client with persistent E2EE state.

    The browser still talks only to Blueprints. Matrix access tokens and crypto
    state remain on the server.
    """

    def __init__(self, settings: dict[str, str]):
        self._settings = dict(settings)
        self._store_dir = Path(settings["crypto_store_dir"])
        self._crypto_db_path = self._store_dir / "crypto.db"
        self._client: Any = None
        self._api: Any = None
        self._crypto_db: Any = None
        self._joined_rooms: set[str] = set()
        self._started = False
        self._lock = asyncio.Lock()

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self._settings.get("upstream", ""),
            self._settings.get("user_id", ""),
            self._settings.get("access_token", ""),
            self._settings.get("crypto_store_dir", ""),
        )

    async def ensure_started(self) -> None:
        if self._started:
            return
        async with self._lock:
            if self._started:
                return
            await self._start()

    async def _start(self) -> None:
        ok, detail = _check_e2ee_deps()
        if not ok:
            raise HTTPException(
                status_code=503,
                detail=f"Matrix E2EE dependencies are not installed: {detail}",
            )

        from mautrix.api import HTTPAPI
        from mautrix.client import Client
        from mautrix.client.state_store import MemoryStateStore, MemorySyncStore
        from mautrix.crypto import OlmMachine
        from mautrix.crypto.store.asyncpg import PgCryptoStore
        from mautrix.types import TrustState, UserID
        from mautrix.util.async_db import Database

        _secure_crypto_store(self._store_dir)

        self._api = HTTPAPI(
            base_url=self._settings["upstream"],
            token=self._settings["access_token"],
        )
        state_store = MemoryStateStore()
        sync_store = MemorySyncStore()
        self._client = Client(
            mxid=UserID(self._settings["user_id"]),
            device_id=self._settings.get("device_id") or None,
            api=self._api,
            state_store=state_store,
            sync_store=sync_store,
        )

        whoami = await self._client.whoami()
        resolved_user_id = getattr(whoami, "user_id", "") or self._settings["user_id"]
        resolved_device_id = self._settings.get("device_id") or getattr(whoami, "device_id", "")
        self._settings["user_id"] = str(resolved_user_id)
        self._client.mxid = UserID(self._settings["user_id"])
        if resolved_device_id:
            self._client.device_id = str(resolved_device_id)

        self._crypto_db = Database.create(
            f"sqlite:///{self._crypto_db_path}",
            upgrade_table=PgCryptoStore.upgrade_table,
        )
        await self._crypto_db.start()
        _secure_crypto_store(self._store_dir)

        account_id = self._settings["user_id"] or "blueprints-chat"
        pickle_key = f"{account_id}:{self._client.device_id or 'default'}"
        crypto_store = PgCryptoStore(account_id=account_id, pickle_key=pickle_key, db=self._crypto_db)
        await crypto_store.open()
        if self._client.device_id:
            await crypto_store.put_device_id(self._client.device_id)

        olm = OlmMachine(
            self._client,
            crypto_store,
            _MatrixCryptoStateStore(state_store, self._joined_rooms),
        )
        olm.share_keys_min_trust = TrustState.UNVERIFIED
        olm.send_keys_min_trust = TrustState.UNVERIFIED
        await olm.load()
        await olm.share_keys()
        self._client.crypto = olm

        data = await self._client.sync(timeout=1000, full_state=True)
        await self._handle_sync_data(data if isinstance(data, dict) else {})
        self._started = True
        _secure_crypto_store(self._store_dir)

    async def _handle_sync_data(self, data: dict[str, Any]) -> None:
        rooms_join = data.get("rooms", {}).get("join", {})
        if isinstance(rooms_join, dict):
            self._joined_rooms.update(str(room_id) for room_id in rooms_join)
        next_batch = data.get("next_batch")
        if next_batch:
            await self._client.sync_store.put_next_batch(next_batch)
        tasks = self._client.handle_sync(data)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def sync(
        self,
        *,
        since: str | None = None,
        timeout_ms: int = 0,
        full_state: bool = False,
    ) -> dict[str, Any]:
        await self.ensure_started() if not self._started else None
        data = await self._client.sync(
            since=since,
            timeout=max(0, min(timeout_ms, _MAX_SYNC_TIMEOUT_MS)),
            full_state=full_state,
        )
        if isinstance(data, dict):
            await self._handle_sync_data(data)
        return data if isinstance(data, dict) else {}

    async def messages(
        self,
        room_id: str,
        *,
        limit: int,
        from_token: str | None = None,
    ) -> dict[str, Any]:
        from mautrix.types import PaginationDirection, RoomID, SyncToken

        await self.ensure_started()
        data = await self._client.get_messages(
            RoomID(room_id),
            PaginationDirection.BACKWARD,
            from_token=SyncToken(from_token) if from_token else None,
            limit=limit,
        )
        events = getattr(data, "events", [])
        messages = [await self.message_from_event(event, room_id) for event in events]
        messages = [message for message in messages if message]
        messages.reverse()
        return {
            "room_id": room_id,
            "messages": messages,
            "start": str(data.start) if data.start else None,
            "end": str(data.end) if data.end else None,
        }

    async def send_message(self, room_id: str, body: str) -> dict[str, Any]:
        from mautrix.types import EventType, RoomID

        await self.ensure_started()
        event_id = await self._client.send_message_event(
            RoomID(room_id),
            EventType.ROOM_MESSAGE,
            {"msgtype": "m.text", "body": body},
        )
        _secure_crypto_store(self._store_dir)
        return {"room_id": room_id, "event_id": str(event_id)}

    async def message_from_event(self, event: Any, room_id: str) -> dict[str, Any] | None:
        from mautrix.types import EventType

        encrypted = str(getattr(event, "type", "")) == str(EventType.ROOM_ENCRYPTED)
        if encrypted:
            try:
                event = await self._client.crypto.decrypt_megolm_event(event)
            except Exception:
                return _message_from_parts(
                    event_id=str(getattr(event, "event_id", "") or ""),
                    room_id=room_id,
                    sender=str(getattr(event, "sender", "") or ""),
                    origin_server_ts=getattr(event, "timestamp", None),
                    msgtype="m.encrypted",
                    body="[unable to decrypt encrypted event]",
                    encrypted=True,
                    decrypted=False,
                )

        if str(getattr(event, "type", "")) != str(EventType.ROOM_MESSAGE):
            return None
        content = getattr(event, "content", None)
        body = getattr(content, "body", None)
        msgtype = getattr(content, "msgtype", None) or "m.text"
        if not isinstance(body, str):
            return None
        source_content = content.serialize() if hasattr(content, "serialize") else {}
        relates_to = source_content.get("m.relates_to") if isinstance(source_content, dict) else None
        return _message_from_parts(
            event_id=str(getattr(event, "event_id", "") or ""),
            room_id=room_id,
            sender=str(getattr(event, "sender", "") or ""),
            origin_server_ts=getattr(event, "timestamp", None),
            msgtype=str(msgtype),
            body=body,
            relates_to=relates_to if isinstance(relates_to, dict) else None,
            encrypted=encrypted,
            decrypted=encrypted,
        )

    async def messages_from_raw_events(
        self,
        room_id: str,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        from mautrix.types import Event

        messages: list[dict[str, Any]] = []
        for raw in events:
            try:
                event = Event.deserialize(raw)
            except Exception:
                fallback = _message_from_event(raw, room_id)
                if fallback:
                    messages.append(fallback)
                continue
            message = await self.message_from_event(event, room_id)
            if message:
                messages.append(message)
        return messages


_e2ee_client: _MatrixChatE2EEClient | None = None
_e2ee_client_key: tuple[str, str, str, str] | None = None
_e2ee_client_lock = asyncio.Lock()


async def _get_e2ee_client(settings: dict[str, str] | None = None) -> _MatrixChatE2EEClient | None:
    global _e2ee_client, _e2ee_client_key
    settings = settings or _require_credentials()
    if not _e2ee_requested(settings):
        return None
    candidate = _MatrixChatE2EEClient(settings)
    async with _e2ee_client_lock:
        if _e2ee_client is None or _e2ee_client_key != candidate.key:
            _e2ee_client = candidate
            _e2ee_client_key = candidate.key
    await _e2ee_client.ensure_started()
    return _e2ee_client


async def _matrix_request_any(
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
    return data


async def _matrix_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
) -> dict[str, Any]:
    data = await _matrix_request_any(
        method,
        path,
        params=params,
        json_body=json_body,
        expected=expected,
    )
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Matrix homeserver returned unexpected JSON")
    return data


async def _synapse_admin_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
) -> dict[str, Any]:
    settings = _require_credentials()
    admin_token = settings.get("admin_access_token") or ""
    if not admin_token:
        raise HTTPException(status_code=503, detail="Matrix admin token is not configured")
    timeout = httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT)
    try:
        async with httpx.AsyncClient(base_url=settings["upstream"], timeout=timeout) as client:
            response = await client.request(
                method,
                f"/_synapse/admin/v2{path}",
                params=params,
                headers={"Authorization": f"Bearer {admin_token}"},
            )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="Matrix homeserver is not reachable") from exc

    if response.status_code not in expected:
        if response.status_code in {401, 403}:
            detail = "Matrix admin credential rejected by homeserver"
        else:
            detail = f"Matrix admin request failed with HTTP {response.status_code}"
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


def _message_from_parts(
    *,
    event_id: str,
    room_id: str,
    sender: str,
    origin_server_ts: int | None,
    msgtype: str,
    body: str,
    relates_to: dict[str, Any] | None = None,
    encrypted: bool = False,
    decrypted: bool = False,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "room_id": room_id,
        "sender": sender,
        "origin_server_ts": origin_server_ts,
        "msgtype": msgtype,
        "body": body,
        "relates_to": relates_to if isinstance(relates_to, dict) else None,
        "encrypted": encrypted,
        "decrypted": decrypted,
    }


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
    return _message_from_parts(
        event_id=event.get("event_id") or "",
        room_id=room_id,
        sender=event.get("sender") or "",
        origin_server_ts=_event_ts(event),
        msgtype=msgtype,
        body=body,
        relates_to=relates_to if isinstance(relates_to, dict) else None,
        encrypted=event_type == "m.room.encrypted",
        decrypted=False,
    )


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


def _matrix_localpart(user_id: str) -> str:
    if user_id.startswith("@") and ":" in user_id:
        return user_id[1:].split(":", 1)[0]
    return user_id


def _user_display_name(user_id: str, display_name: str | None = None) -> str:
    if display_name and display_name.strip():
        return display_name.strip()
    return _matrix_localpart(user_id)


def _normalize_user_candidate(raw: dict[str, Any], *, source: str) -> dict[str, Any] | None:
    user_id = raw.get("name") or raw.get("user_id")
    if not isinstance(user_id, str) or not user_id.startswith("@") or ":" not in user_id:
        return None
    deactivated = bool(raw.get("deactivated"))
    is_admin = bool(raw.get("admin"))
    display = raw.get("displayname") or raw.get("display_name")
    return {
        "user_id": user_id,
        "display_name": _user_display_name(user_id, display if isinstance(display, str) else None),
        "is_admin": is_admin,
        "deactivated": deactivated,
        "source": source,
    }


def _candidate_matches_query(candidate: dict[str, Any], query: str) -> bool:
    needle = query.strip().lower()
    if needle in {"", "@"}:
        return True
    return needle.lstrip("@") in (
        f"{candidate.get('user_id', '')} {candidate.get('display_name', '')}"
    ).lower()


def _filter_invite_candidates(
    candidates: list[dict[str, Any]],
    *,
    excluded_user_ids: set[str],
    current_user_id: str,
    query: str,
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    filtered: list[dict[str, Any]] = []
    for candidate in candidates:
        user_id = str(candidate.get("user_id") or "")
        if not user_id or user_id in seen:
            continue
        seen.add(user_id)
        if user_id in excluded_user_ids or user_id == current_user_id:
            continue
        if candidate.get("deactivated") or candidate.get("is_admin"):
            continue
        if not _candidate_matches_query(candidate, query):
            continue
        filtered.append(
            {
                "user_id": user_id,
                "display_name": candidate.get("display_name") or _matrix_localpart(user_id),
            }
        )
    filtered.sort(key=lambda item: (str(item.get("display_name") or "").lower(), item["user_id"]))
    return filtered


def _configured_user_candidates(settings: dict[str, str]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for key in ("user_id", "hermes_user_id", "operator_user_id", "admin_user_id"):
        user_id = settings.get(key) or ""
        if not user_id:
            continue
        candidates.append(
            {
                "user_id": user_id,
                "display_name": _matrix_localpart(user_id),
                "is_admin": key == "admin_user_id",
                "deactivated": False,
                "source": "config",
            }
        )
    return candidates


async def _room_member_user_ids(room_id: str) -> set[str]:
    encoded_room = quote(room_id, safe="")
    data = await _matrix_request_any("GET", f"/rooms/{encoded_room}/state")
    events = data if isinstance(data, list) else []
    members: set[str] = set()
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "m.room.member":
            continue
        content = _event_content(event)
        if content.get("membership") not in {"join", "invite"}:
            continue
        state_key = event.get("state_key")
        if isinstance(state_key, str) and state_key.startswith("@"):
            members.add(state_key)
    return members


async def _admin_user_candidates() -> list[dict[str, Any]]:
    data = await _synapse_admin_request(
        "GET",
        "/users",
        params={"from": 0, "limit": 100, "guests": "false", "deactivated": "false"},
    )
    users = data.get("users") if isinstance(data.get("users"), list) else []
    candidates = [
        _normalize_user_candidate(user, source="synapse_admin")
        for user in users
        if isinstance(user, dict)
    ]
    return [candidate for candidate in candidates if candidate]


async def _directory_user_candidates(query: str) -> list[dict[str, Any]]:
    terms = [query.strip()]
    if query.strip() in {"", "@"}:
        terms = ["xarta", "operator", "hermes", "codex"]
    candidates: list[dict[str, Any]] = []
    for term in terms:
        if not term:
            continue
        data = await _matrix_request(
            "POST",
            "/user_directory/search",
            json_body={"search_term": term, "limit": 50},
        )
        results = data.get("results") if isinstance(data.get("results"), list) else []
        candidates.extend(
            candidate
            for candidate in (
                _normalize_user_candidate(result, source="user_directory")
                for result in results
                if isinstance(result, dict)
            )
            if candidate
        )
    return candidates


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


async def _sync_for_chat(
    *,
    since: str | None = None,
    timeout_ms: int = 0,
    full_state: bool = False,
) -> tuple[dict[str, Any], _MatrixChatE2EEClient | None]:
    settings = _require_credentials()
    e2ee_client = await _get_e2ee_client(settings)
    if e2ee_client:
        return (
            await e2ee_client.sync(
                since=since,
                timeout_ms=timeout_ms,
                full_state=full_state,
            ),
            e2ee_client,
        )
    return await _sync(since=since, timeout_ms=timeout_ms, full_state=full_state), None


@router.get("/status")
async def matrix_chat_status() -> dict[str, Any]:
    settings = _settings()
    reachable = False
    health = ""
    e2ee_deps_ok, e2ee_deps_error = _check_e2ee_deps()
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
            "e2ee": _e2ee_requested(settings) and e2ee_deps_ok,
            "e2ee_requested": _e2ee_requested(settings),
            "e2ee_dependencies": e2ee_deps_ok,
            "e2ee_dependency_error": e2ee_deps_error if _e2ee_requested(settings) and not e2ee_deps_ok else "",
            "push_notifications": False,
            "generic_matrix_proxy": False,
        },
    }


@router.get("/rooms")
async def matrix_chat_rooms() -> dict[str, Any]:
    sync, _e2ee_client = await _sync_for_chat(timeout_ms=0, full_state=True)
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
    if body.encrypted:
        payload["initial_state"] = [
            {
                "type": "m.room.encryption",
                "state_key": "",
                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
            }
        ]
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


@router.get("/rooms/{room_id}/invite-candidates")
async def matrix_chat_invite_candidates(
    room_id: str,
    q: str = Query(default="", max_length=80),
) -> dict[str, Any]:
    settings = _require_credentials()
    excluded = await _room_member_user_ids(room_id)
    source = "synapse_admin"
    try:
        candidates = await _admin_user_candidates()
    except HTTPException:
        source = "user_directory"
        candidates = await _directory_user_candidates(q)
        candidates.extend(_configured_user_candidates(settings))
    users = _filter_invite_candidates(
        candidates,
        excluded_user_ids=excluded,
        current_user_id=settings["user_id"],
        query=q,
    )
    return {
        "room_id": room_id,
        "query": q,
        "source": source,
        "users": users[:50],
    }


@router.get("/rooms/{room_id}/messages")
async def matrix_chat_messages(
    room_id: str,
    limit: int = Query(default=50, ge=1, le=_MAX_MESSAGE_LIMIT),
    from_token: str | None = Query(default=None, alias="from"),
) -> dict[str, Any]:
    e2ee_client = await _get_e2ee_client()
    if e2ee_client:
        return await e2ee_client.messages(room_id, limit=limit, from_token=from_token)

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
    e2ee_client = await _get_e2ee_client()
    if e2ee_client:
        return await e2ee_client.send_message(room_id, body.body)

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
    sync, e2ee_client = await _sync_for_chat(since=since, timeout_ms=timeout_ms, full_state=False)
    joined, invited = _rooms_from_sync(sync)
    rooms = sync.get("rooms") if isinstance(sync.get("rooms"), dict) else {}
    joined_raw = rooms.get("join") if isinstance(rooms.get("join"), dict) else {}
    room_updates = []
    for room_id, room in joined_raw.items():
        if not isinstance(room, dict):
            continue
        raw_events = [event for event in _timeline_events(room) if isinstance(event, dict)]
        if e2ee_client:
            messages = await e2ee_client.messages_from_raw_events(room_id, raw_events)
        else:
            messages = [_message_from_event(event, room_id) for event in raw_events]
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
