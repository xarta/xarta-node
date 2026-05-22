"""routes_matrix_chat.py - Narrow Blueprints proxy for Matrix/Synapse chat.

This route intentionally exposes only the chat operations needed by the
Blueprints Settings -> Agents -> Chat page. Matrix credentials stay server-side
in ignored/private env files; browser responses are reduced DTOs, never raw
Matrix credentials or generic Matrix API proxy output.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import re
import shlex
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


_MATRIX_SERVER_LABELS = {
    "tb1": "TB1",
    "vps": "VPS",
}
_CURRENT_MATRIX_SERVER = contextvars.ContextVar("matrix_chat_server", default="tb1")


def _normalize_server_id(value: str | None) -> str:
    server_id = (value or "tb1").strip().lower()
    if server_id not in _MATRIX_SERVER_LABELS:
        raise HTTPException(status_code=400, detail="Unsupported Matrix chat server")
    return server_id


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


async def _select_matrix_server(request: Request) -> None:
    _CURRENT_MATRIX_SERVER.set(_normalize_server_id(request.query_params.get("server")))


router = APIRouter(
    prefix="/matrix-chat",
    tags=["matrix-chat"],
    dependencies=[Depends(_require_matrix_chat_auth), Depends(_select_matrix_server)],
)

_DEFAULT_ENV_FILE = "/xarta-node/.lone-wolf/stacks/matrix-synapse/.env"
_DEFAULT_VPS_ENV_FILE = "/xarta-node/.lone-wolf/stacks/matrix-synapse-vps/.env"
_DEFAULT_UPSTREAM = "http://127.0.0.1:8008"
_DEFAULT_PUBLIC_HOMESERVER = "https://matrix.local"
_DEFAULT_HERMES_USER_ID = ""
_DEFAULT_SMOKE_ROOM_ID = ""
_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 20.0
_MAX_MESSAGE_LIMIT = 100
_MAX_REDACTION_SCAN_LIMIT = 20_000
_REDACTION_MAX_RETRIES = 6
_REDACTION_RETRY_FLOOR_SECONDS = 5.0
_REDACTION_PACE_SECONDS = 0.15
_MAX_SYNC_TIMEOUT_MS = 30_000
_runtime_access_tokens: dict[tuple[str, str, str, str], str] = {}
_runtime_access_token_lock = asyncio.Lock()
_DEFAULT_CRYPTO_STORE_DIR = (
    "/xarta-node/.lone-wolf/stacks/matrix-synapse/data/blueprints-chat/crypto-store"
)
_DEFAULT_HERMES_MATRIX_PATCH_REPORT = (
    "/xarta-node/.lone-wolf/stacks/hermes-local/data/health/matrix_platform_patch.json"
)
_DEFAULT_HERMES_COMMAND_CONTAINER = "hermes-local"
_DEFAULT_HERMES_COMMAND_PYTHON = "/opt/hermes/.venv/bin/python"
_DEFAULT_ROOM_SETTINGS_FILE = "/xarta-node/.lone-wolf/stacks/matrix-chat/data/room-settings.json"
_HERMES_COMMAND_CATALOG_TIMEOUT = 8
_MXID_MENTION_RE = re.compile(r"(?<![\w/])(@[0-9A-Za-z._=/-]+:[0-9A-Za-z.-]+(?::\d+)?)")
_HERMES_COMMAND_CATALOG_SCRIPT = r"""
import json

from hermes_cli.commands import (
    COMMAND_REGISTRY,
    _is_gateway_available,
    _iter_plugin_command_entries,
    _requires_argument,
    _resolve_config_gates,
)


def item(name, description, category, source, args_hint="", aliases=None):
    insert = f"/{name}"
    if args_hint:
        insert += " "
    return {
        "name": f"/{name}",
        "insert": insert,
        "description": description,
        "category": category,
        "source": source,
        "args_hint": args_hint,
        "aliases": [f"/{alias}" for alias in aliases or []],
        "requires_argument": _requires_argument(args_hint),
    }


commands = []
overrides = _resolve_config_gates()
for cmd in COMMAND_REGISTRY:
    if not _is_gateway_available(cmd, overrides):
        continue
    commands.append(
        item(
            cmd.name,
            cmd.description,
            cmd.category,
            "core",
            cmd.args_hint,
            cmd.aliases,
        )
    )

for name, description, args_hint in _iter_plugin_command_entries():
    commands.append(item(name, description, "Plugins", "plugin", args_hint))

try:
    from agent.skill_commands import get_skill_commands

    for cmd_key, info in sorted(get_skill_commands().items()):
        name = str(cmd_key).lstrip("/")
        if not name:
            continue
        commands.append(
            item(
                name,
                str(info.get("description") or f"Load {name} skill"),
                "Skills",
                "skill",
                "<instruction>",
            )
        )
except Exception:
    pass

print(json.dumps({"commands": commands}, ensure_ascii=True))
"""


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


class _RoomSettingsBody(BaseModel):
    hermes_command_catalog: bool = False
    hide_system_messages: bool = False
    system_message_min_level: str = "information"


class _RedactMessagesBody(BaseModel):
    mode: str = Field(default="events", pattern="^(events|undecryptable|system_before)$")
    event_ids: list[str] = Field(default_factory=list, max_length=500)
    before_ts: int | None = None
    limit: int = Field(default=500, ge=1, le=_MAX_REDACTION_SCAN_LIMIT)
    scan_all: bool = False
    reason: str = Field(default="Blueprints Matrix Chat delete", max_length=240)


class _TestDecryptionMessagesBody(BaseModel):
    decryptable_count: int = Field(default=2, ge=0, le=5)
    undecryptable_count: int = Field(default=2, ge=0, le=5)


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


def _server_env_name(name: str, server_id: str) -> str:
    return f"{name}_{server_id.upper()}"


def _server_prefixed_env_name(name: str, server_id: str) -> str:
    marker = "MATRIX_CHAT_"
    if name.startswith(f"BLUEPRINTS_{marker}"):
        return name.replace(f"BLUEPRINTS_{marker}", f"BLUEPRINTS_{marker}{server_id.upper()}_", 1)
    if name.startswith(marker):
        return name.replace(marker, f"{marker}{server_id.upper()}_", 1)
    return _server_env_name(name, server_id)


def _runtime_token_key(settings: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        settings.get("server_id", ""),
        settings.get("upstream", ""),
        settings.get("user_id", ""),
        settings.get("device_id", ""),
    )


def _settings(server_id: str | None = None) -> dict[str, str]:
    server_id = _normalize_server_id(server_id or _CURRENT_MATRIX_SERVER.get())
    env_file = (
        os.getenv(_server_env_name("BLUEPRINTS_MATRIX_CHAT_ENV_FILE", server_id))
        or os.getenv(f"BLUEPRINTS_MATRIX_CHAT_{server_id.upper()}_ENV_FILE")
        or os.getenv("BLUEPRINTS_MATRIX_CHAT_ENV_FILE")
        or (_DEFAULT_VPS_ENV_FILE if server_id == "vps" else _DEFAULT_ENV_FILE)
    )
    file_values = _read_env_file(env_file)

    def pick(*names: str, default: str = "") -> str:
        for name in names:
            server_names = (
                _server_prefixed_env_name(name, server_id),
                _server_env_name(name, server_id),
                name,
            )
            for candidate in server_names:
                value = os.getenv(candidate)
                if value:
                    return value.strip()
            for candidate in server_names:
                value = file_values.get(candidate)
                if value:
                    return value.strip()
        return default

    def pick_global(*names: str, default: str = "") -> str:
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
        default=pick_global("MATRIX_SYNAPSE_UPSTREAM", default=_DEFAULT_UPSTREAM),
    )
    if not upstream.startswith(("http://", "https://")):
        upstream = f"http://{upstream}"

    public_homeserver = pick(
        "BLUEPRINTS_MATRIX_CHAT_HOMESERVER",
        "MATRIX_CHAT_HOMESERVER",
        default=pick_global("MATRIX_SYNAPSE_HOSTNAME"),
    )
    if public_homeserver and not public_homeserver.startswith(("http://", "https://")):
        public_homeserver = f"https://{public_homeserver}"
    if not public_homeserver:
        public_homeserver = _DEFAULT_PUBLIC_HOMESERVER

    user_id = pick(
        "MATRIX_CHAT_DAVROS_USER_ID",
        "MATRIX_DAVROS_USER_ID",
        "MATRIX_CHAT_OPERATOR_USER_ID",
        "MATRIX_OPERATOR_USER_ID",
        "MATRIX_CHAT_USER_ID",
        "MATRIX_CODEX_USER_ID",
    )
    access_token = pick(
        "MATRIX_CHAT_DAVROS_ACCESS_TOKEN",
        "MATRIX_DAVROS_ACCESS_TOKEN",
        "MATRIX_CHAT_OPERATOR_ACCESS_TOKEN",
        "MATRIX_OPERATOR_ACCESS_TOKEN",
        "MATRIX_CHAT_ACCESS_TOKEN",
        "MATRIX_CODEX_ACCESS_TOKEN",
    )

    settings = {
        "server_id": server_id,
        "server_label": _MATRIX_SERVER_LABELS[server_id],
        "env_file": env_file,
        "upstream": upstream.rstrip("/"),
        "public_homeserver": public_homeserver.rstrip("/"),
        "user_id": user_id,
        "access_token": access_token,
        "password": pick(
            "MATRIX_CHAT_DAVROS_PASSWORD",
            "MATRIX_DAVROS_PASSWORD",
            "MATRIX_CHAT_OPERATOR_PASSWORD",
            "MATRIX_OPERATOR_PASSWORD",
            "MATRIX_CHAT_PASSWORD",
            "MATRIX_CODEX_PASSWORD",
        ),
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
        "device_id": pick(
            "MATRIX_CHAT_DAVROS_DEVICE_ID",
            "MATRIX_DAVROS_DEVICE_ID",
            "MATRIX_CHAT_OPERATOR_DEVICE_ID",
            "MATRIX_OPERATOR_DEVICE_ID",
            "MATRIX_CHAT_DEVICE_ID",
            "BLUEPRINTS_MATRIX_CHAT_DEVICE_ID",
            "MATRIX_CODEX_DEVICE_ID",
        ),
        "recovery_key": pick(
            "MATRIX_CHAT_DAVROS_RECOVERY_KEY",
            "MATRIX_DAVROS_RECOVERY_KEY",
            "MATRIX_CHAT_OPERATOR_RECOVERY_KEY",
            "MATRIX_OPERATOR_RECOVERY_KEY",
            "MATRIX_CHAT_RECOVERY_KEY",
            "BLUEPRINTS_MATRIX_CHAT_RECOVERY_KEY",
        ),
        "crypto_store_dir": pick(
            "MATRIX_CHAT_DAVROS_CRYPTO_STORE_DIR",
            "MATRIX_DAVROS_CRYPTO_STORE_DIR",
            "MATRIX_CHAT_OPERATOR_CRYPTO_STORE_DIR",
            "MATRIX_OPERATOR_CRYPTO_STORE_DIR",
            "MATRIX_CHAT_CRYPTO_STORE_DIR",
            "BLUEPRINTS_MATRIX_CHAT_CRYPTO_STORE_DIR",
            default=_DEFAULT_CRYPTO_STORE_DIR,
        ),
        "hermes_matrix_patch_report": pick(
            "MATRIX_CHAT_HERMES_PATCH_REPORT",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_PATCH_REPORT",
            default="" if server_id == "vps" else _DEFAULT_HERMES_MATRIX_PATCH_REPORT,
        ),
        "hermes_command_container": pick(
            "MATRIX_CHAT_HERMES_COMMAND_CONTAINER",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_COMMAND_CONTAINER",
            default="" if server_id == "vps" else _DEFAULT_HERMES_COMMAND_CONTAINER,
        ),
        "hermes_command_python": pick(
            "MATRIX_CHAT_HERMES_COMMAND_PYTHON",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_COMMAND_PYTHON",
            default=_DEFAULT_HERMES_COMMAND_PYTHON,
        ),
        "hermes_command_ssh_host": pick(
            "MATRIX_CHAT_HERMES_COMMAND_SSH_HOST",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_COMMAND_SSH_HOST",
        ),
        "hermes_command_ssh_key": pick(
            "MATRIX_CHAT_HERMES_COMMAND_SSH_KEY",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_COMMAND_SSH_KEY",
        ),
        "hermes_command_ssh_user": pick(
            "MATRIX_CHAT_HERMES_COMMAND_SSH_USER",
            "BLUEPRINTS_MATRIX_CHAT_HERMES_COMMAND_SSH_USER",
            default="root",
        ),
        "room_settings_file": pick(
            "MATRIX_CHAT_ROOM_SETTINGS_FILE",
            "BLUEPRINTS_MATRIX_CHAT_ROOM_SETTINGS_FILE",
            default=_DEFAULT_ROOM_SETTINGS_FILE,
        ),
    }
    cached_token = _runtime_access_tokens.get(_runtime_token_key(settings))
    if cached_token:
        settings["access_token"] = cached_token
    return settings


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


async def _refresh_chat_access_token(settings: dict[str, str]) -> dict[str, str]:
    if not settings.get("password"):
        raise HTTPException(
            status_code=502,
            detail="Matrix chat credential rejected by homeserver and password fallback is not configured",
        )

    async with _runtime_access_token_lock:
        cached_token = _runtime_access_tokens.get(_runtime_token_key(settings))
        if cached_token and cached_token != settings.get("access_token"):
            settings["access_token"] = cached_token
            return settings

        payload: dict[str, Any] = {
            "type": "m.login.password",
            "identifier": {"type": "m.id.user", "user": settings["user_id"]},
            "password": settings["password"],
            "initial_device_display_name": "Blueprints Matrix Chat",
        }
        if settings.get("device_id"):
            payload["device_id"] = settings["device_id"]

        timeout = httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT)
        try:
            async with httpx.AsyncClient(base_url=settings["upstream"], timeout=timeout) as client:
                response = await client.post(_matrix_path("/login"), json=payload)
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="Matrix homeserver is not reachable") from exc

        if response.status_code not in {200, 201}:
            raise HTTPException(
                status_code=502,
                detail="Matrix chat credential rejected and password fallback login failed",
            )
        try:
            data = response.json()
        except ValueError as exc:
            raise HTTPException(status_code=502, detail="Matrix homeserver returned invalid JSON") from exc

        token = str(data.get("access_token") or "")
        resolved_user_id = str(data.get("user_id") or "")
        resolved_device_id = str(data.get("device_id") or settings.get("device_id") or "")
        if not token or resolved_user_id != settings["user_id"]:
            raise HTTPException(
                status_code=502,
                detail="Matrix chat password fallback returned an unexpected identity",
            )

        settings["access_token"] = token
        if resolved_device_id:
            settings["device_id"] = resolved_device_id
        _runtime_access_tokens[_runtime_token_key(settings)] = token
        log.warning(
            "Matrix chat access token refreshed from password fallback for %s on %s",
            settings["user_id"],
            settings["server_label"],
        )
        return settings


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
    recovery_path = store_dir / "recovery-key.txt"
    if recovery_path.is_file():
        try:
            recovery_path.chmod(0o600)
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
        self._recovery_key_path = self._store_dir / "recovery-key.txt"
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

        try:
            whoami = await self._client.whoami()
        except Exception as exc:
            if exc.__class__.__name__ != "MUnknownToken":
                raise
            refreshed = await _refresh_chat_access_token(self._settings)
            self._settings.update(refreshed)
            self._api.token = self._settings["access_token"]
            whoami = await self._client.whoami()
        resolved_user_id = getattr(whoami, "user_id", "") or self._settings["user_id"]
        resolved_device_id = getattr(whoami, "device_id", "") or self._settings.get("device_id") or ""
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
        await self._ensure_cross_signing(olm)
        self._client.crypto = olm

        data = await self._client.sync(timeout=1000, full_state=True)
        await self._handle_sync_data(data if isinstance(data, dict) else {})
        self._started = True
        _secure_crypto_store(self._store_dir)

    async def _ensure_cross_signing(self, olm: Any) -> None:
        """Bootstrap or restore cross-signing for the server-side Matrix device."""
        recovery_key = self._settings.get("recovery_key", "").strip()
        if not recovery_key and self._recovery_key_path.is_file():
            try:
                recovery_key = self._recovery_key_path.read_text(encoding="utf-8").strip()
                if recovery_key:
                    log.info(
                        "Matrix chat E2EE: loaded recovery key from private store file %s",
                        self._recovery_key_path,
                    )
            except OSError as exc:
                log.warning(
                    "Matrix chat E2EE: could not read private recovery key file %s: %s",
                    self._recovery_key_path,
                    exc,
                )

        if recovery_key:
            try:
                await olm.verify_with_recovery_key(recovery_key)
                log.info("Matrix chat E2EE: cross-signing verified via recovery key")
                return
            except Exception as exc:
                log.warning("Matrix chat E2EE: recovery key verification failed: %s", exc)

        try:
            own_xsign = await olm.get_own_cross_signing_public_keys()
        except Exception as exc:
            own_xsign = None
            log.warning("Matrix chat E2EE: cross-signing key lookup failed: %s", exc)

        if own_xsign:
            return

        try:
            new_recovery_key = await olm.generate_recovery_key()
            self._recovery_key_path.parent.mkdir(parents=True, exist_ok=True)
            self._recovery_key_path.write_text(new_recovery_key + "\n", encoding="utf-8")
            self._recovery_key_path.chmod(0o600)
            log.warning(
                "Matrix chat E2EE: bootstrapped cross-signing for %s. "
                "Recovery key was written to private store file %s. "
                "Move it into private secret storage and set MATRIX_CHAT_RECOVERY_KEY "
                "for future restarts if desired.",
                self._settings.get("user_id") or "(unknown user)",
                self._recovery_key_path,
            )
        except Exception as exc:
            log.warning(
                "Matrix chat E2EE: cross-signing bootstrap failed "
                "(non-fatal; service device remains unverified): %s",
                exc,
            )
        finally:
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
        from mautrix.api import Method, Path

        await self.ensure_started()
        query_params = {
            "dir": "b",
            "limit": str(limit),
        }
        if from_token:
            query_params["from"] = from_token
        data = await self._client.api.request(
            Method.GET,
            Path.v3.rooms[room_id].messages,
            query_params=query_params,
            metrics_method="getMessages",
        )
        events = data.get("chunk") if isinstance(data, dict) and isinstance(data.get("chunk"), list) else []
        messages = await self.messages_from_raw_events(room_id, events)
        messages.reverse()
        end = data.get("end") if isinstance(data, dict) and isinstance(data.get("end"), str) else None
        start = data.get("start") if isinstance(data, dict) and isinstance(data.get("start"), str) else from_token
        return {
            "room_id": room_id,
            "messages": messages,
            "start": start,
            "end": end,
            "at_start": not bool(end),
        }

    async def send_message(self, room_id: str, body: str) -> dict[str, Any]:
        from mautrix.types import EventType, RoomID

        await self.ensure_started()
        event_id = await self._client.send_message_event(
            RoomID(room_id),
            EventType.ROOM_MESSAGE,
            _matrix_message_content(body),
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
        system_message = (
            source_content.get("org.xarta.system_message") if isinstance(source_content, dict) else None
        )
        return _message_from_parts(
            event_id=str(getattr(event, "event_id", "") or ""),
            room_id=room_id,
            sender=str(getattr(event, "sender", "") or ""),
            origin_server_ts=getattr(event, "timestamp", None),
            msgtype=str(msgtype),
            body=body,
            relates_to=relates_to if isinstance(relates_to, dict) else None,
            system_message=system_message if isinstance(system_message, dict) else None,
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
            if _event_is_redacted(raw):
                continue
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
            if response.status_code == 401:
                settings = await _refresh_chat_access_token(settings)
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


def _matrix_retry_after_seconds(response: httpx.Response) -> float:
    try:
        data = response.json()
    except ValueError:
        data = {}
    retry_after_ms = data.get("retry_after_ms") if isinstance(data, dict) else None
    if isinstance(retry_after_ms, int | float) and retry_after_ms > 0:
        return max(_REDACTION_RETRY_FLOOR_SECONDS, min(float(retry_after_ms) / 1000.0, 30.0))
    return _REDACTION_RETRY_FLOOR_SECONDS


async def _redact_matrix_event(room_id: str, event_id: str, reason: str) -> dict[str, Any]:
    if not event_id:
        raise HTTPException(status_code=400, detail="Matrix event id is required")
    settings = _require_credentials()
    encoded_room = quote(room_id, safe="")
    encoded_event = quote(event_id, safe="")
    txn_id = f"bp-redact-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
    encoded_txn = quote(txn_id, safe="")
    path = _matrix_path(f"/rooms/{encoded_room}/redact/{encoded_event}/{encoded_txn}")
    timeout = httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT)
    async with httpx.AsyncClient(base_url=settings["upstream"], timeout=timeout) as client:
        for attempt in range(_REDACTION_MAX_RETRIES + 1):
            try:
                response = await client.put(path, json={"reason": reason}, headers=_headers(settings))
                if response.status_code == 401:
                    settings = await _refresh_chat_access_token(settings)
                    response = await client.put(path, json={"reason": reason}, headers=_headers(settings))
            except httpx.RequestError as exc:
                raise HTTPException(status_code=502, detail="Matrix homeserver is not reachable") from exc

            if response.status_code == 200:
                try:
                    data = response.json() if response.content else {}
                except ValueError as exc:
                    raise HTTPException(
                        status_code=502,
                        detail="Matrix homeserver returned invalid JSON",
                    ) from exc
                break
            if response.status_code == 429 and attempt < _REDACTION_MAX_RETRIES:
                await asyncio.sleep(_matrix_retry_after_seconds(response))
                continue
            if response.status_code in {401, 403}:
                detail = "Matrix chat credential rejected by homeserver"
            elif response.status_code == 429:
                detail = "Matrix homeserver rate limited redaction; try again in a moment"
            else:
                detail = f"Matrix redaction failed with HTTP {response.status_code}"
            raise HTTPException(status_code=502, detail=detail)
    return {
        "event_id": event_id,
        "redaction_event_id": data.get("event_id"),
    }


async def _load_room_messages_for_redaction(
    room_id: str,
    limit: int,
    *,
    scan_all: bool = False,
) -> tuple[list[dict[str, Any]], bool]:
    messages: list[dict[str, Any]] = []
    from_token: str | None = None
    target = _MAX_REDACTION_SCAN_LIMIT if scan_all else max(1, min(limit, _MAX_REDACTION_SCAN_LIMIT))
    remaining = target
    at_start = False

    while remaining > 0:
        batch_size = min(_MAX_MESSAGE_LIMIT, remaining)
        e2ee_client = await _get_e2ee_client()
        if e2ee_client:
            data = await e2ee_client.messages(room_id, limit=batch_size, from_token=from_token)
        else:
            encoded_room = quote(room_id, safe="")
            params: dict[str, Any] = {"dir": "b", "limit": batch_size}
            if from_token:
                params["from"] = from_token
            raw = await _matrix_request("GET", f"/rooms/{encoded_room}/messages", params=params)
            chunk = raw.get("chunk") if isinstance(raw.get("chunk"), list) else []
            batch_messages = [
                _message_from_event(event, room_id) for event in chunk if isinstance(event, dict)
            ]
            batch_messages = [message for message in batch_messages if message]
            batch_messages.reverse()
            data = {
                "messages": batch_messages,
                "end": raw.get("end") if isinstance(raw.get("end"), str) else None,
            }

        batch = data.get("messages") if isinstance(data.get("messages"), list) else []
        messages.extend(message for message in batch if isinstance(message, dict))
        remaining -= batch_size
        from_token = data.get("end") if isinstance(data.get("end"), str) else None
        if not from_token or not batch:
            at_start = True
            break

    return messages[-target:], at_start


async def _synapse_admin_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    expected: tuple[int, ...] = (200,),
    admin_api_version: str = "v2",
) -> dict[str, Any]:
    if admin_api_version not in {"v1", "v2"}:
        raise HTTPException(status_code=500, detail="Unsupported Matrix admin API version")
    settings = _settings()
    admin_token = settings.get("admin_access_token") or ""
    if not admin_token:
        raise HTTPException(status_code=503, detail="Matrix admin token is not configured")
    timeout = httpx.Timeout(_READ_TIMEOUT, connect=_CONNECT_TIMEOUT)
    try:
        async with httpx.AsyncClient(base_url=settings["upstream"], timeout=timeout) as client:
            response = await client.request(
                method,
                f"/_synapse/admin/{admin_api_version}{path}",
                params=params,
                json=json_body,
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


async def _set_bulk_redaction_ratelimit_override(
    settings: dict[str, str],
) -> tuple[bool, dict[str, Any]]:
    """Temporarily raise the Matrix chat user's send/redaction rate limit."""
    if not settings.get("admin_access_token") or not settings.get("user_id"):
        return False, {}
    encoded_user = quote(settings["user_id"], safe="")
    path = f"/users/{encoded_user}/override_ratelimit"
    try:
        prior = await _synapse_admin_request("GET", path, admin_api_version="v1")
        await _synapse_admin_request(
            "POST",
            path,
            json_body={"messages_per_second": 50, "burst_count": 500},
            admin_api_version="v1",
        )
    except HTTPException as exc:
        log.warning("Matrix bulk redaction: could not set temporary ratelimit override: %s", exc.detail)
        return False, {}
    return True, prior


async def _restore_bulk_redaction_ratelimit_override(
    settings: dict[str, str],
    prior: dict[str, Any],
) -> None:
    if not settings.get("admin_access_token") or not settings.get("user_id"):
        return
    encoded_user = quote(settings["user_id"], safe="")
    path = f"/users/{encoded_user}/override_ratelimit"
    try:
        if prior:
            await _synapse_admin_request(
                "POST",
                path,
                json_body={
                    "messages_per_second": int(prior.get("messages_per_second") or 0),
                    "burst_count": int(prior.get("burst_count") or 0),
                },
                admin_api_version="v1",
            )
        else:
            await _synapse_admin_request("DELETE", path, admin_api_version="v1")
    except HTTPException as exc:
        log.warning("Matrix bulk redaction: could not restore ratelimit override: %s", exc.detail)


def _safe_str(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _safe_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default


def _normalize_admin_user(raw: dict[str, Any]) -> dict[str, Any] | None:
    user_id = _safe_str(raw.get("name") or raw.get("user_id"))
    if not user_id.startswith("@") or ":" not in user_id:
        return None
    display = _safe_str(raw.get("displayname") or raw.get("display_name"))
    return {
        "user_id": user_id,
        "display_name": _user_display_name(user_id, display),
        "is_admin": _safe_bool(raw.get("admin") if "admin" in raw else raw.get("is_admin")),
        "deactivated": _safe_bool(raw.get("deactivated")),
        "is_guest": _safe_bool(raw.get("is_guest") if "is_guest" in raw else raw.get("guest")),
        "creation_ts": _safe_int(raw.get("creation_ts")),
    }


def _normalize_admin_room(raw: dict[str, Any]) -> dict[str, Any] | None:
    room_id = _safe_str(raw.get("room_id"))
    if not room_id:
        return None
    encryption = raw.get("encryption")
    encrypted = _safe_bool(raw.get("encrypted"), default=bool(encryption))
    version = raw.get("version") if raw.get("version") is not None else raw.get("room_version")
    return {
        "room_id": room_id,
        "name": _safe_str(raw.get("name")),
        "canonical_alias": _safe_str(raw.get("canonical_alias")),
        "joined_members": _safe_int(raw.get("joined_members")),
        "joined_local_members": _safe_int(raw.get("joined_local_members")),
        "version": str(version) if version is not None else None,
        "encrypted": encrypted,
        "public": _safe_bool(raw.get("public") if "public" in raw else raw.get("is_public")),
        "federatable": _safe_bool(raw.get("federatable")),
    }


def _state_content_for(events: list[dict[str, Any]], event_type: str) -> dict[str, Any]:
    for event in events:
        if not isinstance(event, dict) or event.get("type") != event_type:
            continue
        content = _event_content(event)
        if content:
            return content
    return {}


def _reduced_power_levels(content: dict[str, Any]) -> dict[str, int | None]:
    fields = ("users_default", "events_default", "state_default", "redact", "ban", "kick")
    return {field: _safe_int(content.get(field)) for field in fields}


async def _synapse_admin_room_state(room_id: str) -> list[dict[str, Any]]:
    encoded_room = quote(room_id, safe="")
    data = await _synapse_admin_request(
        "GET",
        f"/rooms/{encoded_room}/state",
        admin_api_version="v1",
    )
    events = data.get("state") if isinstance(data.get("state"), list) else []
    return [event for event in events if isinstance(event, dict)]


def _room_member_rows_from_state(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    power_content = _state_content_for(events, "m.room.power_levels")
    power_users = power_content.get("users") if isinstance(power_content.get("users"), dict) else {}
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "m.room.member":
            continue
        user_id = _safe_str(event.get("state_key"))
        if not user_id:
            continue
        content = _event_content(event)
        rows[user_id] = {
            "user_id": user_id,
            "membership": _safe_str(content.get("membership")) or "join",
            "display_name": _user_display_name(user_id, _safe_str(content.get("displayname"))),
            "power_level": _safe_int(power_users.get(user_id)) if isinstance(power_users, dict) else None,
        }
    return rows


def _normalize_admin_member(raw: Any, state_rows: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    if isinstance(raw, str):
        user_id = raw
        raw_dict: dict[str, Any] = {}
    elif isinstance(raw, dict):
        user_id = _safe_str(raw.get("user_id") or raw.get("name"))
        raw_dict = raw
    else:
        return None
    if not user_id.startswith("@") or ":" not in user_id:
        return None
    state_row = state_rows.get(user_id, {})
    display = _safe_str(raw_dict.get("displayname") or raw_dict.get("display_name"))
    return {
        "user_id": user_id,
        "membership": _safe_str(raw_dict.get("membership")) or state_row.get("membership") or "join",
        "display_name": _user_display_name(user_id, display or state_row.get("display_name")),
        "power_level": _safe_int(state_row.get("power_level")),
    }


def _admin_status_payload(
    settings: dict[str, str],
    *,
    reachable: bool,
    health: str = "",
) -> dict[str, Any]:
    return {
        "server_id": settings.get("server_id") or "tb1",
        "server_label": settings.get("server_label") or "TB1",
        "configured": bool(settings.get("admin_access_token")),
        "reachable": reachable,
        "health": health,
        "homeserver_url": settings["public_homeserver"],
        "admin_configured": bool(settings.get("admin_access_token")),
        "admin_user_id": settings.get("admin_user_id") or None,
        "features": {
            "generic_admin_proxy": False,
            "destructive_actions": False,
            "room_settings": bool(settings.get("admin_access_token")),
        },
    }


def _room_settings_path(settings: dict[str, str] | None = None) -> Path:
    raw_path = (settings or {}).get("room_settings_file") or _DEFAULT_ROOM_SETTINGS_FILE
    return Path(raw_path)


def _read_room_settings(settings: dict[str, str] | None = None) -> dict[str, Any]:
    path = _room_settings_path(settings)
    if not path.is_file():
        return {"servers": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"servers": {}}
    if not isinstance(data, dict):
        return {"servers": {}}
    servers = data.get("servers")
    if not isinstance(servers, dict):
        data["servers"] = {}
    return data


def _write_room_settings(settings: dict[str, str], data: dict[str, Any]) -> None:
    path = _room_settings_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(tmp_path, 0o600)
    os.replace(tmp_path, path)


def _room_settings_for(data: dict[str, Any], server_id: str, room_id: str) -> dict[str, Any]:
    servers = data.get("servers") if isinstance(data.get("servers"), dict) else {}
    server = servers.get(server_id) if isinstance(servers.get(server_id), dict) else {}
    rooms = server.get("rooms") if isinstance(server.get("rooms"), dict) else {}
    room = rooms.get(room_id) if isinstance(rooms.get(room_id), dict) else {}
    system_level = _safe_str(room.get("system_message_min_level")).lower() or "information"
    if system_level not in {"debug", "information", "warning", "error"}:
        system_level = "information"
    return {
        "hermes_command_catalog": _safe_bool(room.get("hermes_command_catalog")),
        "hide_system_messages": _safe_bool(room.get("hide_system_messages")),
        "system_message_min_level": system_level,
    }


def _room_settings_payload(settings: dict[str, str], room_id: str) -> dict[str, Any]:
    room = _room_settings_for(
        _read_room_settings(settings),
        settings.get("server_id") or "tb1",
        room_id,
    )
    return {
        "server_id": settings.get("server_id") or "tb1",
        "room_id": room_id,
        "hermes_command_catalog": bool(room["hermes_command_catalog"]),
        "hide_system_messages": bool(room["hide_system_messages"]),
        "system_message_min_level": room["system_message_min_level"],
        "admin_available": bool(settings.get("admin_access_token")),
    }


def _set_room_settings(settings: dict[str, str], room_id: str, patch: _RoomSettingsBody) -> dict[str, Any]:
    if not settings.get("admin_access_token"):
        raise HTTPException(status_code=503, detail="Matrix admin token is not configured")
    server_id = settings.get("server_id") or "tb1"
    data = _read_room_settings(settings)
    servers = data.setdefault("servers", {})
    if not isinstance(servers, dict):
        servers = {}
        data["servers"] = servers
    server = servers.setdefault(server_id, {})
    if not isinstance(server, dict):
        server = {}
        servers[server_id] = server
    rooms = server.setdefault("rooms", {})
    if not isinstance(rooms, dict):
        rooms = {}
        server["rooms"] = rooms
    rooms[room_id] = {
        "hermes_command_catalog": bool(patch.hermes_command_catalog),
        "hide_system_messages": bool(patch.hide_system_messages),
        "system_message_min_level": _safe_str(patch.system_message_min_level).lower()
        if _safe_str(patch.system_message_min_level).lower()
        in {"debug", "information", "warning", "error"}
        else "information",
    }
    _write_room_settings(settings, data)
    return _room_settings_payload(settings, room_id)


def _annotate_room_settings(settings: dict[str, str], rooms: list[dict[str, Any]]) -> None:
    data = _read_room_settings(settings)
    server_id = settings.get("server_id") or "tb1"
    for room in rooms:
        room_id = _safe_str(room.get("room_id"))
        if not room_id:
            continue
        room_settings = _room_settings_for(data, server_id, room_id)
        room["hermes_command_catalog"] = bool(room_settings["hermes_command_catalog"])
        room["hide_system_messages"] = bool(room_settings["hide_system_messages"])
        room["system_message_min_level"] = room_settings["system_message_min_level"]


def _hermes_matrix_patch_status(path: str) -> dict[str, Any]:
    report_path = Path(path)
    if not report_path.is_file():
        return {
            "available": False,
            "ok": None,
            "generated_at_epoch": None,
            "failed_checks": [],
            "error": "report not found",
        }
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {
            "available": False,
            "ok": None,
            "generated_at_epoch": None,
            "failed_checks": [],
            "error": f"{type(exc).__name__}: {exc}",
        }

    checks = report.get("checks") if isinstance(report, dict) else []
    failed = []
    if isinstance(checks, list):
        for item in checks:
            if not isinstance(item, dict) or item.get("ok") is True:
                continue
            failed.append(
                {
                    "id": _safe_str(item.get("id")),
                    "message": _safe_str(item.get("message")),
                }
            )
    return {
        "available": True,
        "ok": bool(report.get("ok")) if isinstance(report, dict) else False,
        "generated_at_epoch": _safe_int(report.get("generated_at_epoch"))
        if isinstance(report, dict)
        else None,
        "failed_checks": failed[:20],
        "error": "",
    }


def _mentions_from_body(body: str) -> list[str]:
    seen: set[str] = set()
    mentions: list[str] = []
    for match in _MXID_MENTION_RE.finditer(body or ""):
        user_id = match.group(1)
        if user_id in seen:
            continue
        seen.add(user_id)
        mentions.append(user_id)
    return mentions[:20]


def _matrix_message_content(body: str) -> dict[str, Any]:
    content: dict[str, Any] = {"msgtype": "m.text", "body": body}
    mentions = _mentions_from_body(body)
    if mentions:
        content["m.mentions"] = {"user_ids": mentions}
    return content


async def _send_bogus_encrypted_test_event(
    room_id: str,
    *,
    label: str,
    sequence: int,
) -> dict[str, Any]:
    settings = _require_credentials()
    encoded_room = quote(room_id, safe="")
    txn_id = f"bp-test-undec-{int(time.time() * 1000)}-{uuid.uuid4().hex[:12]}"
    encoded_txn = quote(txn_id, safe="")
    marker = f"{label}-u{sequence}"
    data = await _matrix_request(
        "PUT",
        f"/rooms/{encoded_room}/send/m.room.encrypted/{encoded_txn}",
        json_body={
            "algorithm": "m.megolm.v1.aes-sha2",
            "sender_key": f"blueprints-test-sender-key-{uuid.uuid4().hex}",
            "session_id": f"blueprints-test-session-{uuid.uuid4().hex}",
            "device_id": settings.get("device_id") or "BLUEPRINTS_TEST",
            "ciphertext": f"not-a-valid-megolm-payload:{marker}",
            "org.xarta.test_message": {
                "kind": "deliberately_undecryptable",
                "label": label,
                "sequence": sequence,
            },
        },
        expected=(200,),
    )
    return {
        "event_id": data.get("event_id"),
        "kind": "undecryptable",
        "label": marker,
    }


def _normalize_hermes_command(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    name = _safe_str(raw.get("name"))
    insert = _safe_str(raw.get("insert")) or name
    if not name.startswith("/") or not insert.startswith("/"):
        return None
    aliases_raw = raw.get("aliases") if isinstance(raw.get("aliases"), list) else []
    aliases = [_safe_str(alias) for alias in aliases_raw if _safe_str(alias).startswith("/")]
    return {
        "name": name[:80],
        "insert": insert[:120],
        "description": _safe_str(raw.get("description"))[:240],
        "category": _safe_str(raw.get("category"))[:80] or "Commands",
        "source": _safe_str(raw.get("source"))[:40] or "core",
        "args_hint": _safe_str(raw.get("args_hint"))[:120],
        "aliases": aliases[:12],
        "requires_argument": bool(raw.get("requires_argument")),
    }


def _filter_hermes_commands(commands: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    needle = query.strip().lower().lstrip("/")
    if not needle:
        return commands[:120]
    filtered = []
    for command in commands:
        haystack = " ".join(
            [
                str(command.get("name") or ""),
                str(command.get("description") or ""),
                str(command.get("category") or ""),
                " ".join(str(alias) for alias in command.get("aliases") or []),
            ]
        ).lower()
        if needle in haystack.lstrip("/"):
            filtered.append(command)
    return filtered[:120]


def _load_hermes_command_catalog(settings: dict[str, str]) -> dict[str, Any]:
    container = settings.get("hermes_command_container") or ""
    python_bin = settings.get("hermes_command_python") or _DEFAULT_HERMES_COMMAND_PYTHON
    ssh_host = settings.get("hermes_command_ssh_host") or ""
    ssh_key = settings.get("hermes_command_ssh_key") or ""
    ssh_user = settings.get("hermes_command_ssh_user") or "root"
    if not container:
        raise HTTPException(
            status_code=503,
            detail="Hermes command catalogue is not configured for this Matrix server",
        )
    if ssh_host:
        target = ssh_host if "@" in ssh_host else f"{ssh_user}@{ssh_host}"
        remote_command = " ".join(
            [
                "docker",
                "exec",
                shlex.quote(container),
                shlex.quote(python_bin),
                "-c",
                shlex.quote(_HERMES_COMMAND_CATALOG_SCRIPT),
            ]
        )
        args = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
        ]
        if ssh_key:
            args.extend(["-i", ssh_key])
        args.extend([target, remote_command])
    else:
        args = ["docker", "exec", container, python_bin, "-c", _HERMES_COMMAND_CATALOG_SCRIPT]
    try:
        proc = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=_HERMES_COMMAND_CATALOG_TIMEOUT,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Hermes command catalogue unavailable: {type(exc).__name__}",
        ) from exc

    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "Hermes command catalogue command failed").strip()
        raise HTTPException(status_code=503, detail=detail[:240])

    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=503, detail="Hermes command catalogue returned invalid JSON") from exc
    raw_commands = data.get("commands") if isinstance(data, dict) else []
    commands = [
        command
        for command in (_normalize_hermes_command(raw) for raw in raw_commands if isinstance(raw, dict))
        if command
    ]
    commands.sort(
        key=lambda item: (
            {"core": 0, "plugin": 1, "skill": 2}.get(str(item.get("source") or ""), 9),
            str(item.get("name") or "").lower(),
        )
    )
    return {
        "source": "hermes",
        "commands": commands,
        "total": len(commands),
    }


def _event_content(event: dict[str, Any]) -> dict[str, Any]:
    content = event.get("content")
    return content if isinstance(content, dict) else {}


def _event_ts(event: dict[str, Any]) -> int | None:
    ts = event.get("origin_server_ts")
    return ts if isinstance(ts, int) else None


def _event_is_redacted(event: dict[str, Any]) -> bool:
    unsigned = event.get("unsigned")
    if isinstance(unsigned, dict) and isinstance(unsigned.get("redacted_because"), dict):
        return True
    return bool(event.get("redacted"))


def _message_from_parts(
    *,
    event_id: str,
    room_id: str,
    sender: str,
    origin_server_ts: int | None,
    msgtype: str,
    body: str,
    relates_to: dict[str, Any] | None = None,
    system_message: dict[str, Any] | None = None,
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
        "system_message": system_message if isinstance(system_message, dict) else None,
        "encrypted": encrypted,
        "decrypted": decrypted,
    }


def _message_from_event(event: dict[str, Any], room_id: str) -> dict[str, Any] | None:
    if _event_is_redacted(event):
        return None
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
    system_message = content.get("org.xarta.system_message")
    return _message_from_parts(
        event_id=event.get("event_id") or "",
        room_id=room_id,
        sender=event.get("sender") or "",
        origin_server_ts=_event_ts(event),
        msgtype=msgtype,
        body=body,
        relates_to=relates_to if isinstance(relates_to, dict) else None,
        system_message=system_message if isinstance(system_message, dict) else None,
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


def _redacted_event_ids_from_events(events: list[dict[str, Any]]) -> list[str]:
    redacted: list[str] = []
    for event in events:
        if _event_is_redacted(event):
            event_id = _safe_str(event.get("event_id"))
            if event_id:
                redacted.append(event_id)
        if event.get("type") != "m.room.redaction":
            continue
        content = _event_content(event)
        target = _safe_str(event.get("redacts") or content.get("redacts"))
        if target:
            redacted.append(target)
    return redacted


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
        elif event_type in {"m.room.encryption", "m.room.encrypted"}:
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


def _room_mention_candidates_from_state(
    events: list[dict[str, Any]],
    *,
    current_user_id: str,
    query: str,
) -> list[dict[str, Any]]:
    rows = _room_member_rows_from_state(events)
    candidates: list[dict[str, Any]] = []
    for row in rows.values():
        if row.get("membership") not in {"join", "invite"}:
            continue
        user_id = str(row.get("user_id") or "")
        if not user_id or user_id == current_user_id:
            continue
        candidate = {
            "user_id": user_id,
            "display_name": row.get("display_name") or _matrix_localpart(user_id),
            "is_admin": False,
            "deactivated": False,
        }
        if _candidate_matches_query(candidate, query):
            candidates.append(
                {
                    "user_id": user_id,
                    "display_name": str(candidate["display_name"]),
                }
            )
    candidates.sort(key=lambda item: (item["display_name"].lower(), item["user_id"]))
    return candidates[:50]


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
        "server_id": settings["server_id"],
        "server_label": settings["server_label"],
        "servers": [
            {"id": server_id, "label": label}
            for server_id, label in _MATRIX_SERVER_LABELS.items()
        ],
        "configured": bool(settings["user_id"] and settings["access_token"]),
        "reachable": reachable,
        "health": health,
        "homeserver_url": settings["public_homeserver"],
        "user_id": settings["user_id"] or None,
        "default_room_id": settings["smoke_room_id"],
        "hermes_user_id": settings["hermes_user_id"],
        "hermes_matrix_patch": _hermes_matrix_patch_status(
            settings["hermes_matrix_patch_report"]
        ),
        "features": {
            "e2ee": _e2ee_requested(settings) and e2ee_deps_ok,
            "e2ee_requested": _e2ee_requested(settings),
            "e2ee_dependencies": e2ee_deps_ok,
            "e2ee_dependency_error": e2ee_deps_error if _e2ee_requested(settings) and not e2ee_deps_ok else "",
            "push_notifications": False,
            "generic_matrix_proxy": False,
            "room_settings": bool(settings.get("admin_access_token")),
        },
    }


@router.get("/admin/status")
async def matrix_chat_admin_status() -> dict[str, Any]:
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
    return _admin_status_payload(settings, reachable=reachable, health=health)


@router.get("/admin/users")
async def matrix_chat_admin_users() -> dict[str, Any]:
    data = await _synapse_admin_request(
        "GET",
        "/users",
        params={"from": 0, "limit": 500, "guests": "true", "deactivated": "true"},
    )
    raw_users = data.get("users") if isinstance(data.get("users"), list) else []
    users = [
        user
        for user in (_normalize_admin_user(raw) for raw in raw_users if isinstance(raw, dict))
        if user
    ]
    users.sort(key=lambda item: (str(item.get("user_id") or "").lower()))
    total = _safe_int(data.get("total")) or len(users)
    return {"users": users, "total": total}


@router.get("/admin/rooms")
async def matrix_chat_admin_rooms() -> dict[str, Any]:
    data = await _synapse_admin_request(
        "GET",
        "/rooms",
        params={"from": 0, "limit": 500},
        admin_api_version="v1",
    )
    raw_rooms = data.get("rooms") if isinstance(data.get("rooms"), list) else []
    rooms = [
        room
        for room in (_normalize_admin_room(raw) for raw in raw_rooms if isinstance(raw, dict))
        if room
    ]
    rooms.sort(key=lambda item: (str(item.get("name") or item.get("room_id") or "").lower()))
    total = _safe_int(data.get("total_rooms")) or _safe_int(data.get("total")) or len(rooms)
    return {"rooms": rooms, "total": total}


@router.get("/admin/rooms/{room_id}")
async def matrix_chat_admin_room_detail(room_id: str) -> dict[str, Any]:
    encoded_room = quote(room_id, safe="")
    data = await _synapse_admin_request(
        "GET",
        f"/rooms/{encoded_room}",
        admin_api_version="v1",
    )
    room = _normalize_admin_room({**data, "room_id": data.get("room_id") or room_id}) or {
        "room_id": room_id,
        "name": "",
        "canonical_alias": "",
        "joined_members": None,
        "joined_local_members": None,
        "version": None,
        "encrypted": False,
        "public": False,
        "federatable": False,
    }
    events: list[dict[str, Any]] = []
    try:
        events = await _synapse_admin_room_state(room_id)
    except HTTPException:
        events = []
    encryption = _state_content_for(events, "m.room.encryption")
    power_levels = _state_content_for(events, "m.room.power_levels")
    if encryption:
        room["encrypted"] = True
    room["encryption_algorithm"] = _safe_str(encryption.get("algorithm")) if encryption else ""
    room["power_levels"] = _reduced_power_levels(power_levels) if power_levels else {}
    return room


@router.get("/admin/rooms/{room_id}/members")
async def matrix_chat_admin_room_members(room_id: str) -> dict[str, Any]:
    encoded_room = quote(room_id, safe="")
    state_events: list[dict[str, Any]] = []
    try:
        state_events = await _synapse_admin_room_state(room_id)
    except HTTPException:
        state_events = []
    state_rows = _room_member_rows_from_state(state_events)
    data = await _synapse_admin_request(
        "GET",
        f"/rooms/{encoded_room}/members",
        admin_api_version="v1",
    )
    raw_members = data.get("members") if isinstance(data.get("members"), list) else []
    members = [
        member
        for member in (_normalize_admin_member(raw, state_rows) for raw in raw_members)
        if member
    ]
    seen_members = {member["user_id"] for member in members}
    for user_id, member in state_rows.items():
        if user_id not in seen_members:
            members.append(member)
    members.sort(key=lambda item: (str(item.get("user_id") or "").lower()))
    return {"room_id": room_id, "members": members}


@router.get("/rooms")
async def matrix_chat_rooms() -> dict[str, Any]:
    settings = _settings()
    sync, _e2ee_client = await _sync_for_chat(timeout_ms=0, full_state=True)
    joined, invited = _rooms_from_sync(sync)
    _annotate_room_settings(settings, joined)
    return {
        "next_batch": sync.get("next_batch") if isinstance(sync.get("next_batch"), str) else None,
        "joined": joined,
        "invites": invited,
    }


@router.post("/rooms")
async def matrix_chat_create_room(body: _CreateRoomBody) -> dict[str, Any]:
    settings = _settings()
    force_encrypted = settings["server_id"] == "vps" and _e2ee_requested(settings)
    payload: dict[str, Any] = {
        "name": body.name.strip(),
        "preset": "private_chat",
        "visibility": "private",
    }
    if body.encrypted or force_encrypted:
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


@router.get("/rooms/{room_id}/mention-candidates")
async def matrix_chat_mention_candidates(
    room_id: str,
    q: str = Query(default="", max_length=80),
) -> dict[str, Any]:
    settings = _require_credentials()
    encoded_room = quote(room_id, safe="")
    data = await _matrix_request_any("GET", f"/rooms/{encoded_room}/state")
    events = data if isinstance(data, list) else []
    return {
        "room_id": room_id,
        "query": q,
        "users": _room_mention_candidates_from_state(
            events,
            current_user_id=settings["user_id"],
            query=q,
        ),
    }


@router.get("/rooms/{room_id}/settings")
async def matrix_chat_room_settings(room_id: str) -> dict[str, Any]:
    settings = _settings()
    return _room_settings_payload(settings, room_id)


@router.patch("/rooms/{room_id}/settings")
async def matrix_chat_update_room_settings(
    room_id: str,
    body: _RoomSettingsBody,
) -> dict[str, Any]:
    settings = _settings()
    return _set_room_settings(settings, room_id, body)


@router.get("/hermes/commands")
async def matrix_chat_hermes_commands(
    q: str = Query(default="", max_length=80),
    room_id: str = Query(default="", max_length=255),
) -> dict[str, Any]:
    settings = _settings()
    if not room_id:
        raise HTTPException(status_code=403, detail="Hermes command catalogue is disabled for this room")
    room_settings = _room_settings_payload(settings, room_id)
    if not room_settings["hermes_command_catalog"]:
        raise HTTPException(status_code=403, detail="Hermes command catalogue is disabled for this room")
    catalogue = await asyncio.to_thread(_load_hermes_command_catalog, settings)
    commands = catalogue.get("commands") if isinstance(catalogue.get("commands"), list) else []
    filtered = _filter_hermes_commands(commands, q)
    return {
        "source": catalogue.get("source") or "hermes",
        "query": q,
        "total": catalogue.get("total") if isinstance(catalogue.get("total"), int) else len(commands),
        "commands": filtered,
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
        "at_start": not isinstance(data.get("end"), str),
    }


@router.post("/rooms/{room_id}/redactions")
async def matrix_chat_redact_messages(
    room_id: str,
    body: _RedactMessagesBody,
) -> dict[str, Any]:
    targets: list[str] = []
    if body.mode == "events":
        targets = [event_id.strip() for event_id in body.event_ids if event_id.strip()]
        scanned_count = 0
        scan_exhausted = True
    elif body.mode == "undecryptable":
        targets = [event_id.strip() for event_id in body.event_ids if event_id.strip()]
        if not targets:
            raise HTTPException(
                status_code=400,
                detail="Undecryptable cleanup requires explicit event ids from the visible client view",
            )
        scanned_count = 0
        scan_exhausted = True
    else:
        messages, scan_exhausted = await _load_room_messages_for_redaction(
            room_id,
            body.limit,
            scan_all=body.scan_all,
        )
        scanned_count = len(messages)
        if body.mode == "system_before":
            if body.before_ts is None:
                raise HTTPException(status_code=400, detail="before_ts is required")
            targets = [
                str(message.get("event_id") or "")
                for message in messages
                if isinstance(message.get("system_message"), dict)
                and isinstance(message.get("origin_server_ts"), int)
                and int(message["origin_server_ts"]) < body.before_ts
            ]

    deduped: list[str] = []
    seen: set[str] = set()
    for event_id in targets:
        if not event_id or event_id in seen:
            continue
        seen.add(event_id)
        deduped.append(event_id)

    redacted: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    settings = _settings()
    override_applied = False
    override_prior: dict[str, Any] = {}
    if body.mode != "events" and deduped:
        override_applied, override_prior = await _set_bulk_redaction_ratelimit_override(settings)
    try:
        for index, event_id in enumerate(deduped):
            try:
                redacted.append(await _redact_matrix_event(room_id, event_id, body.reason))
            except HTTPException as exc:
                errors.append({"event_id": event_id, "detail": exc.detail})
            if index < len(deduped) - 1 and _REDACTION_PACE_SECONDS > 0:
                await asyncio.sleep(_REDACTION_PACE_SECONDS)
    finally:
        if override_applied:
            await _restore_bulk_redaction_ratelimit_override(settings, override_prior)

    return {
        "room_id": room_id,
        "mode": body.mode,
        "matched": len(deduped),
        "scanned_count": scanned_count,
        "scan_exhausted": scan_exhausted,
        "scan_limit": _MAX_REDACTION_SCAN_LIMIT if body.scan_all else body.limit,
        "redacted": redacted,
        "redacted_count": len(redacted),
        "errors": errors,
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
        json_body=_matrix_message_content(body.body),
        expected=(200,),
    )
    return {
        "room_id": room_id,
        "event_id": data.get("event_id"),
    }


@router.post("/rooms/{room_id}/test/decryption-mix")
async def matrix_chat_seed_decryption_mix(
    room_id: str,
    body: _TestDecryptionMessagesBody,
) -> dict[str, Any]:
    label = f"bp-decryption-test-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    events: list[dict[str, Any]] = []
    e2ee_client = await _get_e2ee_client()
    for index in range(body.decryptable_count):
        text = (
            f"[{label}-d{index + 1}] decryptable Matrix chat cleanup test. "
            "This one should remain after deleting loaded undecryptable messages."
        )
        if e2ee_client:
            sent = await e2ee_client.send_message(room_id, text)
            events.append(
                {
                    "event_id": sent.get("event_id"),
                    "kind": "decryptable",
                    "label": f"{label}-d{index + 1}",
                }
            )
        else:
            sent = await matrix_chat_send_message(room_id, _SendMessageBody(body=text))
            events.append(
                {
                    "event_id": sent.get("event_id"),
                    "kind": "decryptable",
                    "label": f"{label}-d{index + 1}",
                }
            )
    for index in range(body.undecryptable_count):
        events.append(
            await _send_bogus_encrypted_test_event(
                room_id,
                label=label,
                sequence=index + 1,
            )
        )
    return {
        "room_id": room_id,
        "label": label,
        "events": events,
    }


@router.get("/sync")
async def matrix_chat_sync(
    since: str | None = None,
    timeout_ms: int = Query(default=0, ge=0, le=_MAX_SYNC_TIMEOUT_MS),
) -> dict[str, Any]:
    settings = _settings()
    sync, e2ee_client = await _sync_for_chat(since=since, timeout_ms=timeout_ms, full_state=False)
    joined, invited = _rooms_from_sync(sync)
    _annotate_room_settings(settings, joined)
    rooms = sync.get("rooms") if isinstance(sync.get("rooms"), dict) else {}
    joined_raw = rooms.get("join") if isinstance(rooms.get("join"), dict) else {}
    room_updates = []
    for room_id, room in joined_raw.items():
        if not isinstance(room, dict):
            continue
        raw_events = [event for event in _timeline_events(room) if isinstance(event, dict)]
        redacted_event_ids = _redacted_event_ids_from_events(raw_events)
        redacted_event_id_set = set(redacted_event_ids)
        if e2ee_client:
            messages = await e2ee_client.messages_from_raw_events(room_id, raw_events)
        else:
            messages = [_message_from_event(event, room_id) for event in raw_events]
        messages = [
            message
            for message in messages
            if message and message.get("event_id") not in redacted_event_id_set
        ]
        room_updates.append(
            {
                "room_id": room_id,
                "messages": messages,
                "redacted_event_ids": redacted_event_ids,
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
