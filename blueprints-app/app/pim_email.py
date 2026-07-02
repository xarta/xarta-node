"""PIM email helpers for IMAP read views and guarded SMTP proof."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html
import imaplib
import ipaddress
import json
import os
import queue
import re
import secrets
import smtplib
import socket
import ssl
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, ClassVar
from urllib.parse import quote, unquote, urljoin, urlparse, urlunparse

import asyncpg
import httpx
from PIL import Image, ImageOps, UnidentifiedImageError

from .pim_email_security import (
    SCHEMA as SECURITY_CHECK_SCHEMA,
)
from .pim_email_security import (
    EmailSecurityUnavailableError,
    check_email_security_deterministic_sync,
    check_email_security_sync,
    complete_email_security_with_llm_sync,
)
from .pim_email_uid import generate_email_uid_info

DEFAULT_MAILBOX_ID = "default"
DEFAULT_IMAP_PORT = 993
DEFAULT_SMTP_PORT = 465
DEFAULT_IMAP_TIMEOUT_SECONDS = 60.0
DEFAULT_IMAP_CALL_TIMEOUT_SECONDS = 60.0
DEFAULT_IMAP_FETCH_CHUNK_BYTES = 1024 * 1024
ENVELOPE_PURPOSE = b"xarta-pim-email-password-v1"
CONTENT_PURPOSE = b"xarta-pim-email-content-v1"
ASSET_PURPOSE = b"xarta-pim-email-asset-v1"
SANITIZED_VIEW_PURPOSE = b"xarta-pim-email-sanitized-view-v1"
SANITIZED_VIEW_POLICY_VERSION = "sanitized-view-v1"
SANITIZED_VIEW_TRANSFORM_VERSION = "email-sanitized-raw-v1"
SECURITY_POLICY_VERSION = "pim-email-security-v1"
EXTERNAL_IMAGE_DERIVATIVE_VERSION = "external-image-derivative-v1"
EXTERNAL_IMAGE_RETRY_DELAY_SECONDS = 15 * 60
PIM_EMAIL_SCHEMA_LOCK_ID = 917_202_607_020_001
MAX_INLINE_IMAGE_BYTES = 2 * 1024 * 1024
DEFAULT_REMOTE_IMAGE_MAX_BYTES = 25 * 1024 * 1024
MAX_REMOTE_IMAGE_BYTES = DEFAULT_REMOTE_IMAGE_MAX_BYTES
DEFAULT_REMOTE_IMAGE_MAX_REDIRECTS = 20
MAX_IMAGE_PIXELS = 12_000_000
MAX_IMAGE_DIMENSIONS = (1800, 2400)
MAX_RAW_VIEW_TEXT_CHARS = 200_000
EMAIL_IMAGE_PROXY_PATH = "/api/v1/personal/email/image-proxy"
DEFAULT_EMAIL_CONTENT_ROOT = "/xarta-node/.lone-wolf/email"
DEFAULT_DOWNLOADED_FOLDER = "Downloaded"
SPECIAL_USE_MOVE_SKIP_ROLES = {"drafts", "sent", "trash", "junk", "spam", "archive"}
SAFE_INLINE_IMAGE_TYPES = {
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
}

Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS


class EmailConfigError(RuntimeError):
    pass


class EmailCredentialError(RuntimeError):
    pass


class EmailOperationError(RuntimeError):
    pass


class EmailImapTimeoutError(EmailOperationError):
    pass


@dataclass(frozen=True)
class EmailMailbox:
    mailbox_id: str
    email_address: str
    imap_host: str
    imap_port: int
    imap_ssl: bool
    smtp_host: str
    smtp_port: int
    smtp_ssl: bool
    smtp_starttls: bool
    password: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "mailbox_id": self.mailbox_id,
            "email_address": self.email_address,
            "imap": {
                "host": self.imap_host,
                "port": self.imap_port,
                "ssl": self.imap_ssl,
            },
            "smtp": {
                "host": self.smtp_host,
                "port": self.smtp_port,
                "ssl": self.smtp_ssl,
                "starttls": self.smtp_starttls,
            },
        }


@dataclass
class EmailHtmlSanitizeResult:
    html: str
    remote_images_blocked: int = 0
    remote_images_proxied: int = 0
    tracking_images_blocked: int = 0
    inline_images_rendered: int = 0
    inline_images_blocked: int = 0
    active_content_blocked: int = 0
    unsafe_links_blocked: int = 0
    allowed_links: int = 0

    def public_dict(self) -> dict[str, Any]:
        return {
            "remote_images_blocked": self.remote_images_blocked,
            "remote_images_proxied": self.remote_images_proxied,
            "tracking_images_blocked": self.tracking_images_blocked,
            "inline_images_rendered": self.inline_images_rendered,
            "inline_images_blocked": self.inline_images_blocked,
            "active_content_blocked": self.active_content_blocked,
            "unsafe_links_blocked": self.unsafe_links_blocked,
            "allowed_links": self.allowed_links,
            "image_proxy": "same-site-jpeg-transform",
            "sandbox": "srcdoc-no-scripts-no-same-origin",
        }


def _b64_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64_decode(value: str) -> bytes:
    clean = value.strip()
    return base64.urlsafe_b64decode(clean + ("=" * (-len(clean) % 4)))


def generate_credential_key() -> str:
    return _b64_encode(secrets.token_bytes(32))


def _credential_key_bytes(raw_key: str | None = None) -> bytes:
    raw = (
        raw_key if raw_key is not None else os.environ.get("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", "")
    ).strip()
    if not raw:
        raise EmailConfigError("BLUEPRINTS_EMAIL_CREDENTIAL_KEY is not configured")
    try:
        decoded = _b64_decode(raw)
    except Exception:
        decoded = b""
    if len(decoded) >= 32:
        return decoded[:32]
    return hashlib.sha256(raw.encode("utf-8")).digest()


def _keystream(key: bytes, nonce: bytes, length: int) -> bytes:
    chunks: list[bytes] = []
    counter = 0
    produced = 0
    while produced < length:
        counter_bytes = counter.to_bytes(4, "big")
        chunk = hashlib.sha256(key + nonce + counter_bytes).digest()
        chunks.append(chunk)
        produced += len(chunk)
        counter += 1
    return b"".join(chunks)[:length]


def _json_safe(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode("utf-8", "replace").decode("utf-8")
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if isinstance(value, dict):
        return {_json_safe(str(key)): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return value


def _json_dumps(
    value: Any,
    *,
    sort_keys: bool = True,
    separators: tuple[str, str] | None = (",", ":"),
    **kwargs: Any,
) -> str:
    return json.dumps(
        _json_safe(value),
        sort_keys=sort_keys,
        separators=separators,
        **kwargs,
    )


def _json_loads_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return loaded if isinstance(loaded, dict) else {}
    return {}


def _external_image_canonical_digest(canonical_url: str) -> str:
    # Index key for long URLs; security decisions use the raw/transformed hashes.
    return hashlib.md5(str(canonical_url or "").encode("utf-8")).hexdigest()


def _external_image_canonical_lock_key(mailbox_id: str, canonical_url: str) -> int:
    digest = hashlib.sha256(
        f"xarta-pim-email-external-image\0{mailbox_id}\0{canonical_url}".encode("utf-8")
    ).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    if value >= 2**63:
        value -= 2**64
    return value


def encrypt_password(password: str, *, key: str | None = None) -> str:
    key_bytes = _credential_key_bytes(key)
    nonce = secrets.token_bytes(16)
    plaintext = password.encode("utf-8")
    stream = _keystream(key_bytes, nonce, len(plaintext))
    ciphertext = bytes(a ^ b for a, b in zip(plaintext, stream))
    mac = hmac.new(key_bytes, ENVELOPE_PURPOSE + nonce + ciphertext, hashlib.sha256).digest()
    return _json_dumps(
        {
            "v": 1,
            "alg": "xarta-secretbox-sha256-stream-hmac",
            "nonce": _b64_encode(nonce),
            "ciphertext": _b64_encode(ciphertext),
            "mac": _b64_encode(mac),
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def decrypt_password(envelope: str, *, key: str | None = None) -> str:
    key_bytes = _credential_key_bytes(key)
    try:
        payload = json.loads(envelope)
        nonce = _b64_decode(str(payload["nonce"]))
        ciphertext = _b64_decode(str(payload["ciphertext"]))
        expected_mac = _b64_decode(str(payload["mac"]))
    except Exception as exc:
        raise EmailCredentialError("Invalid encrypted mailbox password envelope") from exc
    actual_mac = hmac.new(key_bytes, ENVELOPE_PURPOSE + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(actual_mac, expected_mac):
        raise EmailCredentialError("Encrypted mailbox password authentication failed")
    stream = _keystream(key_bytes, nonce, len(ciphertext))
    plaintext = bytes(a ^ b for a, b in zip(ciphertext, stream))
    return plaintext.decode("utf-8")


def _content_key_bytes(raw_key: str | None = None) -> bytes:
    raw = raw_key if raw_key is not None else os.environ.get("BLUEPRINTS_EMAIL_CONTENT_KEY", "")
    raw = str(raw or "").strip()
    if raw:
        try:
            decoded = _b64_decode(raw)
        except Exception:
            decoded = b""
        if len(decoded) >= 32:
            return decoded[:32]
        return hashlib.sha256(raw.encode("utf-8")).digest()
    credential_key = _credential_key_bytes()
    return hmac.new(credential_key, CONTENT_PURPOSE, hashlib.sha256).digest()


def _email_content_root(root: str | None = None) -> Path:
    configured = (
        root
        if root is not None
        else os.environ.get("BLUEPRINTS_EMAIL_CONTENT_ROOT", DEFAULT_EMAIL_CONTENT_ROOT)
    )
    return Path(str(configured or DEFAULT_EMAIL_CONTENT_ROOT)).expanduser()


def _assert_safe_relpath(relpath: str) -> Path:
    path = Path(str(relpath or ""))
    if path.is_absolute() or ".." in path.parts or not path.name:
        raise EmailOperationError("Unsafe email content storage path")
    return path


def encrypt_bytes_envelope(
    content: bytes,
    *,
    purpose: bytes = CONTENT_PURPOSE,
    key: str | None = None,
) -> dict[str, Any]:
    key_bytes = _content_key_bytes(key)
    nonce = secrets.token_bytes(16)
    plaintext = bytes(content or b"")
    stream = _keystream(key_bytes, nonce, len(plaintext))
    ciphertext = bytes(a ^ b for a, b in zip(plaintext, stream))
    mac = hmac.new(key_bytes, purpose + nonce + ciphertext, hashlib.sha256).digest()
    return {
        "v": 1,
        "alg": "xarta-secretbox-sha256-stream-hmac",
        "purpose": purpose.decode("ascii", "replace"),
        "nonce": _b64_encode(nonce),
        "ciphertext": _b64_encode(ciphertext),
        "mac": _b64_encode(mac),
    }


def decrypt_bytes_envelope(
    envelope: dict[str, Any] | str | bytes,
    *,
    purpose: bytes = CONTENT_PURPOSE,
    key: str | None = None,
) -> bytes:
    key_bytes = _content_key_bytes(key)
    try:
        payload = json.loads(envelope) if isinstance(envelope, (str, bytes)) else dict(envelope)
        nonce = _b64_decode(str(payload["nonce"]))
        ciphertext = _b64_decode(str(payload["ciphertext"]))
        expected_mac = _b64_decode(str(payload["mac"]))
    except Exception as exc:
        raise EmailCredentialError("Invalid encrypted email content envelope") from exc
    actual_mac = hmac.new(key_bytes, purpose + nonce + ciphertext, hashlib.sha256).digest()
    if not hmac.compare_digest(actual_mac, expected_mac):
        raise EmailCredentialError("Encrypted email content authentication failed")
    stream = _keystream(key_bytes, nonce, len(ciphertext))
    return bytes(a ^ b for a, b in zip(ciphertext, stream))


def write_encrypted_bytes_atomic(
    *,
    relpath: str,
    content: bytes,
    purpose: bytes = CONTENT_PURPOSE,
    root: str | None = None,
) -> dict[str, Any]:
    safe_relpath = _assert_safe_relpath(relpath)
    root_path = _email_content_root(root)
    target = root_path / safe_relpath
    target.parent.mkdir(parents=True, exist_ok=True)
    envelope = encrypt_bytes_envelope(content, purpose=purpose)
    encoded = _json_dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    tmp = target.with_name(f".{target.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_bytes(encoded)
    os.replace(tmp, target)
    decrypted = decrypt_bytes_envelope(encoded, purpose=purpose)
    raw_sha256 = hashlib.sha256(bytes(content or b"")).hexdigest()
    verified_sha256 = hashlib.sha256(decrypted).hexdigest()
    if verified_sha256 != raw_sha256:
        raise EmailOperationError("Encrypted email content verification failed")
    return {
        "storage_relpath": str(safe_relpath),
        "storage_abspath": str(target),
        "encrypted_size": len(encoded),
        "raw_sha256": raw_sha256,
        "verified": True,
        "encryption": {
            "schema": "xarta.pim_email.encrypted_file.v1",
            "alg": envelope["alg"],
            "purpose": envelope["purpose"],
            "nonce": envelope["nonce"],
            "mac": envelope["mac"],
        },
    }


def read_encrypted_bytes(
    relpath: str,
    *,
    purpose: bytes = CONTENT_PURPOSE,
    root: str | None = None,
) -> bytes:
    safe_relpath = _assert_safe_relpath(relpath)
    path = _email_content_root(root) / safe_relpath
    try:
        encoded = path.read_bytes()
    except FileNotFoundError as exc:
        raise EmailOperationError("Encrypted email content file is missing") from exc
    return decrypt_bytes_envelope(encoded, purpose=purpose)


class PgEmailStore:
    _schema_ready_dsns: ClassVar[set[str]] = set()
    _schema_ready_lock: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = (
            dsn if dsn is not None else os.environ.get("BLUEPRINTS_EMAIL_POSTGRES_DSN", "")
        ).strip()
        if not self.dsn:
            raise EmailConfigError("BLUEPRINTS_EMAIL_POSTGRES_DSN is not configured")

    async def _connect(self) -> asyncpg.Connection:
        return await asyncpg.connect(self.dsn)

    async def ensure_schema(self) -> None:
        schema_cache_key = str(getattr(self, "dsn", "") or "")
        if schema_cache_key:
            with self._schema_ready_lock:
                if schema_cache_key in self._schema_ready_dsns:
                    return
        conn = await self._connect()
        schema_lock_acquired = False
        try:
            await conn.execute("SELECT pg_advisory_lock($1)", PIM_EMAIL_SCHEMA_LOCK_ID)
            schema_lock_acquired = True
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_mailboxes (
                    mailbox_id TEXT PRIMARY KEY,
                    email_address TEXT NOT NULL UNIQUE,
                    imap_host TEXT NOT NULL,
                    imap_port INTEGER NOT NULL,
                    imap_ssl BOOLEAN NOT NULL DEFAULT true,
                    smtp_host TEXT NOT NULL,
                    smtp_port INTEGER NOT NULL,
                    smtp_ssl BOOLEAN NOT NULL DEFAULT true,
                    smtp_starttls BOOLEAN NOT NULL DEFAULT false,
                    encrypted_password TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_security_checks (
                    security_check_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    folder TEXT NOT NULL,
                    uid TEXT NOT NULL,
                    message_id TEXT NOT NULL DEFAULT '',
                    raw_sha256 TEXT NOT NULL,
                    aggregate_status TEXT NOT NULL,
                    aggregate_score INTEGER NOT NULL DEFAULT 0,
                    llm_called BOOLEAN NOT NULL DEFAULT false,
                    result_json JSONB NOT NULL,
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (mailbox_id, folder, uid, raw_sha256)
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_security_checks_message
                ON pim_email_security_checks(mailbox_id, folder, uid, checked_at DESC);
                """
            )
            await conn.execute(
                """
                ALTER TABLE pim_email_security_checks
                ADD COLUMN IF NOT EXISTS email_uid TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS security_status TEXT NOT NULL DEFAULT 'stored',
                ADD COLUMN IF NOT EXISTS error_message TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS checker_versions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb;
                """
            )
            await conn.execute(
                """
                DELETE FROM pim_email_security_checks
                WHERE security_status <> 'stored'
                   OR aggregate_status IN ('queued', 'pending')
                   OR COALESCE(result_json->>'available', 'false') <> 'true'
                   OR COALESCE(result_json->>'queued', 'false') = 'true'
                   OR COALESCE(result_json->>'placeholder', 'false') = 'true'
                   OR COALESCE(result_json->>'incomplete', 'false') = 'true';
                """
            )
            await conn.execute(
                """
                DROP INDEX IF EXISTS idx_pim_email_security_email_uid_raw;
                CREATE INDEX IF NOT EXISTS idx_pim_email_security_email_uid_raw
                ON pim_email_security_checks(email_uid, raw_sha256)
                WHERE email_uid <> '' AND raw_sha256 <> '';
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_security_phases (
                    phase_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    email_uid TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    phase_status TEXT NOT NULL,
                    phase_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    checker_versions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    error_class TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    policy_version TEXT NOT NULL DEFAULT '',
                    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (mailbox_id, email_uid, raw_sha256, phase, policy_version)
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_security_phases_lookup
                ON pim_email_security_phases(
                    mailbox_id, email_uid, raw_sha256, phase, phase_status, updated_at DESC
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_local_folders (
                    folder_uid TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    parent_folder_uid TEXT NOT NULL DEFAULT '',
                    folder_name TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    delimiter TEXT NOT NULL DEFAULT '/',
                    special_use_role TEXT NOT NULL DEFAULT '',
                    flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    tombstoned_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (mailbox_id, folder_name)
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_folder_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    folder_uid TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    delimiter TEXT NOT NULL DEFAULT '/',
                    special_use_role TEXT NOT NULL DEFAULT '',
                    flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    uidvalidity TEXT NOT NULL DEFAULT '',
                    uidnext TEXT NOT NULL DEFAULT '',
                    messages_count INTEGER NOT NULL DEFAULT 0,
                    unseen_count INTEGER NOT NULL DEFAULT 0,
                    status_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    captured_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_folder_snapshots_folder
                ON pim_email_folder_snapshots(mailbox_id, folder_uid, captured_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_messages (
                    email_uid TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL,
                    message_id TEXT NOT NULL DEFAULT '',
                    subject TEXT NOT NULL DEFAULT '',
                    from_addr TEXT NOT NULL DEFAULT '',
                    to_addr TEXT NOT NULL DEFAULT '',
                    date_header TEXT NOT NULL DEFAULT '',
                    uid_info_json JSONB NOT NULL,
                    headers_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    storage_relpath TEXT NOT NULL,
                    encrypted_size BIGINT NOT NULL DEFAULT 0,
                    encryption_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    first_downloaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_messages_mailbox_updated
                ON pim_email_messages(mailbox_id, updated_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_folder_memberships (
                    membership_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    folder_uid TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    email_uid TEXT NOT NULL REFERENCES pim_email_messages(email_uid) ON DELETE CASCADE,
                    imap_uid TEXT NOT NULL,
                    uidvalidity TEXT NOT NULL DEFAULT '',
                    flags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    source_snapshot_id TEXT NOT NULL DEFAULT '',
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    remote_moved_at TIMESTAMPTZ,
                    remote_move_target TEXT NOT NULL DEFAULT '',
                    UNIQUE (mailbox_id, folder_uid, uidvalidity, imap_uid)
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_memberships_email_uid
                ON pim_email_folder_memberships(email_uid, last_seen_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_download_runs (
                    run_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'running',
                    apply_remote_moves BOOLEAN NOT NULL DEFAULT false,
                    downloaded_folder TEXT NOT NULL DEFAULT '',
                    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_download_batches (
                    batch_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES pim_email_download_runs(run_id) ON DELETE CASCADE,
                    mailbox_id TEXT NOT NULL,
                    folder_uid TEXT NOT NULL,
                    folder_name TEXT NOT NULL,
                    uidvalidity TEXT NOT NULL DEFAULT '',
                    uid_min TEXT NOT NULL DEFAULT '',
                    uid_max TEXT NOT NULL DEFAULT '',
                    planned_count INTEGER NOT NULL DEFAULT 0,
                    processed_count INTEGER NOT NULL DEFAULT 0,
                    skipped_count INTEGER NOT NULL DEFAULT 0,
                    moved_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_download_events (
                    event_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL DEFAULT '',
                    mailbox_id TEXT NOT NULL DEFAULT '',
                    folder_uid TEXT NOT NULL DEFAULT '',
                    folder_name TEXT NOT NULL DEFAULT '',
                    email_uid TEXT NOT NULL DEFAULT '',
                    imap_uid TEXT NOT NULL DEFAULT '',
                    uidvalidity TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    error_class TEXT NOT NULL DEFAULT '',
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_download_events_run
                ON pim_email_download_events(run_id, created_at);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_transformed_assets (
                    asset_uid TEXT PRIMARY KEY,
                    email_uid TEXT NOT NULL REFERENCES pim_email_messages(email_uid) ON DELETE CASCADE,
                    mailbox_id TEXT NOT NULL,
                    source_url TEXT NOT NULL DEFAULT '',
                    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
                    raw_sha256 TEXT NOT NULL DEFAULT '',
                    transformed_sha256 TEXT NOT NULL,
                    storage_relpath TEXT NOT NULL,
                    encrypted_size BIGINT NOT NULL DEFAULT 0,
                    width INTEGER NOT NULL DEFAULT 0,
                    height INTEGER NOT NULL DEFAULT 0,
                    transform_version TEXT NOT NULL DEFAULT 'jpeg-v1',
                    encryption_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_shared_assets (
                    shared_asset_uid TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    canonical_url TEXT NOT NULL DEFAULT '',
                    canonical_url_digest TEXT NOT NULL DEFAULT '',
                    content_type TEXT NOT NULL DEFAULT 'image/jpeg',
                    raw_image_sha256 TEXT NOT NULL DEFAULT '',
                    transformed_sha256 TEXT NOT NULL,
                    storage_relpath TEXT NOT NULL,
                    encrypted_size BIGINT NOT NULL DEFAULT 0,
                    width INTEGER NOT NULL DEFAULT 0,
                    height INTEGER NOT NULL DEFAULT 0,
                    transform_version TEXT NOT NULL DEFAULT 'jpeg-v1',
                    encryption_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    reference_count INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (
                        mailbox_id, canonical_url_digest, transformed_sha256, transform_version
                    )
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_shared_assets_url
                ON pim_email_shared_assets(
                    mailbox_id, canonical_url_digest, updated_at DESC
                );
                """
            )
            await conn.execute(
                """
                ALTER TABLE pim_email_transformed_assets
                ADD COLUMN IF NOT EXISTS shared_asset_uid TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS canonical_url_digest TEXT NOT NULL DEFAULT '';
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_assets_email_uid
                ON pim_email_transformed_assets(email_uid, updated_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_sanitized_view_artifacts (
                    artifact_uid TEXT PRIMARY KEY,
                    email_uid TEXT NOT NULL REFERENCES pim_email_messages(email_uid) ON DELETE CASCADE,
                    mailbox_id TEXT NOT NULL,
                    input_raw_sha256 TEXT NOT NULL,
                    sanitizer_policy_version TEXT NOT NULL,
                    transform_version TEXT NOT NULL,
                    output_sha256 TEXT NOT NULL,
                    storage_relpath TEXT NOT NULL,
                    encrypted_size BIGINT NOT NULL DEFAULT 0,
                    views_available_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    safety_counts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    derivation_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    encryption_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (
                        mailbox_id, email_uid, input_raw_sha256,
                        sanitizer_policy_version, transform_version
                    )
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_sanitized_views_current
                ON pim_email_sanitized_view_artifacts(
                    mailbox_id, email_uid, input_raw_sha256,
                    sanitizer_policy_version, transform_version, updated_at DESC
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_external_image_derivatives (
                    derivative_id TEXT PRIMARY KEY,
                    email_uid TEXT NOT NULL REFERENCES pim_email_messages(email_uid) ON DELETE CASCADE,
                    mailbox_id TEXT NOT NULL,
                    input_raw_sha256 TEXT NOT NULL DEFAULT '',
                    source_url TEXT NOT NULL,
                    canonical_url TEXT NOT NULL DEFAULT '',
                    canonical_url_digest TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL DEFAULT '',
                    safety_decision TEXT NOT NULL DEFAULT '',
                    transform_version TEXT NOT NULL DEFAULT '',
                    raw_image_sha256 TEXT NOT NULL DEFAULT '',
                    transformed_sha256 TEXT NOT NULL DEFAULT '',
                    storage_relpath TEXT NOT NULL DEFAULT '',
                    encrypted_size BIGINT NOT NULL DEFAULT 0,
                    content_type TEXT NOT NULL DEFAULT '',
                    width INTEGER NOT NULL DEFAULT 0,
                    height INTEGER NOT NULL DEFAULT 0,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    fetched_at TIMESTAMPTZ,
                    transformed_at TIMESTAMPTZ,
                    stored_at TIMESTAMPTZ,
                    next_retry_at TIMESTAMPTZ,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (mailbox_id, email_uid, input_raw_sha256, canonical_url_digest)
                );
                """
            )
            await conn.execute(
                """
                ALTER TABLE pim_email_external_image_derivatives
                ADD COLUMN IF NOT EXISTS canonical_url_digest TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS shared_asset_uid TEXT NOT NULL DEFAULT '';
                """
            )
            await conn.execute(
                """
                UPDATE pim_email_external_image_derivatives
                SET canonical_url_digest = md5(canonical_url)
                WHERE canonical_url_digest = '' AND canonical_url <> '';
                """
            )
            await conn.execute(
                """
                ALTER TABLE pim_email_external_image_derivatives
                DROP CONSTRAINT IF EXISTS
                  pim_email_external_image_deri_mailbox_id_email_uid_input_ra_key;
                """
            )
            await conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_pim_email_external_images_identity_digest
                ON pim_email_external_image_derivatives(
                    mailbox_id, email_uid, input_raw_sha256, canonical_url_digest
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_external_images_status
                ON pim_email_external_image_derivatives(mailbox_id, status, updated_at DESC);
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_external_images_digest_status
                ON pim_email_external_image_derivatives(
                    mailbox_id, canonical_url_digest, status, updated_at DESC
                );
                """
            )
            await conn.execute(
                """
                UPDATE pim_email_external_image_derivatives
                SET status = 'pending',
                    reason = 'legacy_non_downloaded_state_reset_for_real_download',
                    safety_decision = 'pending_real_download',
                    updated_at = now()
                WHERE status = 'skipped';
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_backfill_runs (
                    run_id TEXT PRIMARY KEY,
                    mailbox_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    requested_limit INTEGER,
                    artifact_types_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    processed_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_backfill_batches (
                    batch_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES pim_email_backfill_runs(run_id) ON DELETE CASCADE,
                    mailbox_id TEXT NOT NULL,
                    artifact_types_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                    planned_count INTEGER NOT NULL DEFAULT 0,
                    processed_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pim_email_backfill_items (
                    item_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES pim_email_backfill_runs(run_id) ON DELETE CASCADE,
                    batch_id TEXT NOT NULL DEFAULT '',
                    mailbox_id TEXT NOT NULL,
                    email_uid TEXT NOT NULL,
                    raw_sha256 TEXT NOT NULL DEFAULT '',
                    artifact_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error_class TEXT NOT NULL DEFAULT '',
                    error_message TEXT NOT NULL DEFAULT '',
                    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                    started_at TIMESTAMPTZ,
                    finished_at TIMESTAMPTZ,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (run_id, email_uid, raw_sha256, artifact_type)
                );
                """
            )
            await conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pim_email_backfill_items_status
                ON pim_email_backfill_items(mailbox_id, artifact_type, status, updated_at DESC);
                """
            )
            if schema_cache_key:
                with self._schema_ready_lock:
                    self._schema_ready_dsns.add(schema_cache_key)
        finally:
            if schema_lock_acquired:
                try:
                    await conn.execute(
                        "SELECT pg_advisory_unlock($1)",
                        PIM_EMAIL_SCHEMA_LOCK_ID,
                    )
                except Exception:
                    pass
            await conn.close()

    async def upsert_mailbox(self, mailbox: EmailMailbox) -> dict[str, Any]:
        encrypted = encrypt_password(mailbox.password)
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_mailboxes (
                    mailbox_id, email_address, imap_host, imap_port, imap_ssl,
                    smtp_host, smtp_port, smtp_ssl, smtp_starttls, encrypted_password
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (mailbox_id) DO UPDATE SET
                    email_address = EXCLUDED.email_address,
                    imap_host = EXCLUDED.imap_host,
                    imap_port = EXCLUDED.imap_port,
                    imap_ssl = EXCLUDED.imap_ssl,
                    smtp_host = EXCLUDED.smtp_host,
                    smtp_port = EXCLUDED.smtp_port,
                    smtp_ssl = EXCLUDED.smtp_ssl,
                    smtp_starttls = EXCLUDED.smtp_starttls,
                    encrypted_password = EXCLUDED.encrypted_password,
                    updated_at = now();
                """,
                mailbox.mailbox_id,
                mailbox.email_address,
                mailbox.imap_host,
                mailbox.imap_port,
                mailbox.imap_ssl,
                mailbox.smtp_host,
                mailbox.smtp_port,
                mailbox.smtp_ssl,
                mailbox.smtp_starttls,
                encrypted,
            )
            row = await conn.fetchrow(
                """
                SELECT mailbox_id, email_address, imap_host, imap_port, imap_ssl,
                       smtp_host, smtp_port, smtp_ssl, smtp_starttls, updated_at,
                       encrypted_password
                FROM pim_email_mailboxes
                WHERE mailbox_id = $1
                """,
                mailbox.mailbox_id,
            )
        finally:
            await conn.close()
        if row is None:
            raise EmailCredentialError("Mailbox upsert did not return a row")
        return _mailbox_row_public(row)

    async def get_mailbox(self, mailbox_id: str | None = None) -> EmailMailbox:
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT mailbox_id, email_address, imap_host, imap_port, imap_ssl,
                       smtp_host, smtp_port, smtp_ssl, smtp_starttls, encrypted_password
                FROM pim_email_mailboxes
                WHERE mailbox_id = $1
                """,
                configured_mailbox_id,
            )
        finally:
            await conn.close()
        if row is None:
            raise EmailCredentialError(f"Mailbox {configured_mailbox_id!r} is not configured")
        return EmailMailbox(
            mailbox_id=str(row["mailbox_id"]),
            email_address=str(row["email_address"]),
            imap_host=str(row["imap_host"]),
            imap_port=int(row["imap_port"]),
            imap_ssl=bool(row["imap_ssl"]),
            smtp_host=str(row["smtp_host"]),
            smtp_port=int(row["smtp_port"]),
            smtp_ssl=bool(row["smtp_ssl"]),
            smtp_starttls=bool(row["smtp_starttls"]),
            password=decrypt_password(str(row["encrypted_password"])),
        )

    async def public_mailboxes(self) -> list[dict[str, Any]]:
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT mailbox_id, email_address, imap_host, imap_port, imap_ssl,
                       smtp_host, smtp_port, smtp_ssl, smtp_starttls, updated_at
                FROM pim_email_mailboxes
                ORDER BY email_address
                """
            )
        finally:
            await conn.close()
        return [_mailbox_row_public(row) for row in rows]

    async def record_security_result(self, message: dict[str, Any], *, mailbox_id: str) -> None:
        security = message.get("security") if isinstance(message, dict) else None
        if not isinstance(security, dict) or not security.get("available"):
            raise EmailOperationError(
                "Email security result is missing and message view is blocked"
            )
        await self.ensure_schema()
        aggregate = security.get("aggregate") if isinstance(security.get("aggregate"), dict) else {}
        raw_sha256 = str(security.get("raw_sha256") or "")
        folder = clean_folder_name(str(message.get("folder") or "INBOX"))
        uid = clean_uid_value(str(message.get("uid") or ""))
        email_uid = str(message.get("email_uid") or "")
        message_id = str((message.get("headers") or {}).get("message_id") or "")
        storage_security = _security_result_for_storage(
            security,
            email_uid=email_uid,
            raw_sha256=raw_sha256,
            parsed=message,
        )
        check_id = _stable_id("email-security", email_uid, raw_sha256)
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_security_checks (
                    security_check_id, mailbox_id, folder, uid, message_id, raw_sha256,
                    aggregate_status, aggregate_score, llm_called, result_json, checked_at,
                    email_uid, security_status, checker_versions_json, metadata_json
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb, now(),$11,$12,$13::jsonb,$14::jsonb)
                ON CONFLICT (security_check_id) DO UPDATE SET
                    mailbox_id = EXCLUDED.mailbox_id,
                    folder = EXCLUDED.folder,
                    uid = EXCLUDED.uid,
                    message_id = EXCLUDED.message_id,
                    raw_sha256 = EXCLUDED.raw_sha256,
                    aggregate_status = EXCLUDED.aggregate_status,
                    aggregate_score = EXCLUDED.aggregate_score,
                    llm_called = EXCLUDED.llm_called,
                    email_uid = EXCLUDED.email_uid,
                    security_status = EXCLUDED.security_status,
                    checker_versions_json = EXCLUDED.checker_versions_json,
                    metadata_json = EXCLUDED.metadata_json,
                    result_json = EXCLUDED.result_json,
                    checked_at = now();
                """,
                check_id,
                mailbox_id,
                folder,
                uid,
                message_id,
                raw_sha256,
                str(aggregate.get("status") or "amber"),
                int(aggregate.get("score") or aggregate.get("risk_score") or 0),
                bool(aggregate.get("llm_called") or (security.get("llm") or {}).get("called")),
                _json_dumps(storage_security, sort_keys=True, separators=(",", ":")),
                email_uid,
                "stored",
                _json_dumps(
                    _security_checker_versions(storage_security),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                _json_dumps(
                    {
                        "schema": "xarta.pim_email.security_result.metadata.v1",
                        "email_uid": email_uid,
                        "raw_sha256": raw_sha256,
                        "policy_version": SECURITY_POLICY_VERSION,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        finally:
            await conn.close()

    async def record_security_phase_result(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        raw_sha256: str,
        phase: str,
        phase_status: str,
        phase_result: dict[str, Any],
        error_class: str = "",
        error_message: str = "",
        ensure_schema: bool = True,
    ) -> dict[str, Any]:
        if ensure_schema:
            await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        clean_phase = str(phase or "").strip().lower()
        clean_status = str(phase_status or "").strip().lower()
        if clean_phase not in {"deterministic", "llm"}:
            raise EmailOperationError("Invalid security phase")
        if clean_status not in {"running", "complete", "failed_retryable"}:
            raise EmailOperationError("Invalid security phase status")
        phase_id = _stable_id(
            "email-security-phase",
            mailbox_id,
            clean_uid,
            raw_sha256,
            clean_phase,
            SECURITY_POLICY_VERSION,
        )
        versions = _security_checker_versions(
            phase_result if isinstance(phase_result, dict) else {}
        )
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO pim_email_security_phases (
                    phase_id, mailbox_id, email_uid, raw_sha256, phase, phase_status,
                    phase_json, checker_versions_json, error_class, error_message,
                    policy_version, checked_at, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,now(),now())
                ON CONFLICT (mailbox_id, email_uid, raw_sha256, phase, policy_version)
                DO UPDATE SET
                    phase_status = EXCLUDED.phase_status,
                    phase_json = EXCLUDED.phase_json,
                    checker_versions_json = EXCLUDED.checker_versions_json,
                    error_class = EXCLUDED.error_class,
                    error_message = EXCLUDED.error_message,
                    checked_at = now(),
                    updated_at = now()
                RETURNING phase_id, phase, phase_status, error_class, error_message, updated_at;
                """,
                phase_id,
                mailbox_id,
                clean_uid,
                str(raw_sha256 or ""),
                clean_phase,
                clean_status,
                _json_dumps(phase_result or {}, sort_keys=True, separators=(",", ":")),
                _json_dumps(versions, sort_keys=True, separators=(",", ":")),
                str(error_class or "")[:200],
                str(error_message or "")[:1000],
                SECURITY_POLICY_VERSION,
            )
        finally:
            await conn.close()
        return {
            "phase_id": str(_row_get(row, "phase_id", "")),
            "phase": str(_row_get(row, "phase", "")),
            "phase_status": str(_row_get(row, "phase_status", "")),
            "error_class": str(_row_get(row, "error_class", "")),
            "error_message": str(_row_get(row, "error_message", "")),
            "updated_at": _iso_datetime(_row_get(row, "updated_at")),
        }

    async def record_security_incomplete_result(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        raw_sha256: str,
        folder: str,
        uid: str,
        message_id: str,
        phase: str,
        error_class: str,
        error_message: str,
        deterministic_result: dict[str, Any] | None = None,
    ) -> None:
        await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        result = {
            "schema": SECURITY_CHECK_SCHEMA,
            "available": False,
            "incomplete": True,
            "email_uid": clean_uid,
            "raw_sha256": str(raw_sha256 or ""),
            "checked_at": _now_iso(),
            "checker_versions": _security_checker_versions(deterministic_result or {}),
            "failed_phase": str(phase or ""),
            "deterministic_phase_available": bool(
                (deterministic_result or {}).get("deterministic_complete")
            ),
            "error": {
                "class": str(error_class or "")[:200],
                "message": str(error_message or "")[:1000],
            },
        }
        if deterministic_result:
            result["deterministic"] = {
                "schema": deterministic_result.get("schema", ""),
                "checked_at": deterministic_result.get("checked_at", ""),
                "finding_count": len(deterministic_result.get("findings") or []),
                "dkim": deterministic_result.get("dkim") or {},
                "spf": deterministic_result.get("spf") or {},
                "dmarc": deterministic_result.get("dmarc") or {},
            }
        check_id = _stable_id("email-security", clean_uid, raw_sha256)
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_security_checks (
                    security_check_id, mailbox_id, folder, uid, message_id, raw_sha256,
                    aggregate_status, aggregate_score, llm_called, result_json, checked_at,
                    email_uid, security_status, error_message, checker_versions_json, metadata_json
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,'incomplete',0,false,$7::jsonb,now(),
                    $8,'failed_retryable',$9,$10::jsonb,$11::jsonb
                )
                ON CONFLICT (security_check_id) DO UPDATE SET
                    mailbox_id = EXCLUDED.mailbox_id,
                    folder = EXCLUDED.folder,
                    uid = EXCLUDED.uid,
                    message_id = EXCLUDED.message_id,
                    raw_sha256 = EXCLUDED.raw_sha256,
                    aggregate_status = EXCLUDED.aggregate_status,
                    aggregate_score = EXCLUDED.aggregate_score,
                    llm_called = EXCLUDED.llm_called,
                    result_json = EXCLUDED.result_json,
                    email_uid = EXCLUDED.email_uid,
                    security_status = EXCLUDED.security_status,
                    error_message = EXCLUDED.error_message,
                    checker_versions_json = EXCLUDED.checker_versions_json,
                    metadata_json = EXCLUDED.metadata_json,
                    checked_at = now();
                """,
                check_id,
                mailbox_id,
                clean_folder_name(folder or "local"),
                str(uid or clean_uid),
                str(message_id or ""),
                str(raw_sha256 or ""),
                _json_dumps(result, sort_keys=True, separators=(",", ":")),
                clean_uid,
                str(error_message or "")[:1000],
                _json_dumps(result["checker_versions"], sort_keys=True, separators=(",", ":")),
                _json_dumps(
                    {
                        "schema": "xarta.pim_email.security_result.metadata.v1",
                        "email_uid": clean_uid,
                        "raw_sha256": str(raw_sha256 or ""),
                        "policy_version": SECURITY_POLICY_VERSION,
                        "source": "split-security-phase",
                        "failed_phase": str(phase or ""),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        finally:
            await conn.close()

    async def local_corpus_status(self, *, mailbox_id: str | None = None) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        download_orphan_reconcile = await self.reconcile_orphaned_download_runs(
            active_run_ids=_active_download_run_ids_from_proc(),
            reason="local_corpus_status_process_set_reconciliation",
            mailbox_id=configured_mailbox_id,
        )
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM pim_email_messages WHERE mailbox_id = $1) AS messages,
                  (SELECT count(*) FROM pim_email_local_folders WHERE mailbox_id = $1 AND tombstoned_at IS NULL) AS folders,
                  (SELECT count(*) FROM pim_email_folder_memberships WHERE mailbox_id = $1) AS memberships,
                  (SELECT count(*) FROM pim_email_transformed_assets WHERE mailbox_id = $1) AS transformed_assets,
                  (SELECT count(*) FROM pim_email_shared_assets WHERE mailbox_id = $1) AS shared_assets,
                  (SELECT count(*) FROM pim_email_messages WHERE mailbox_id = $1 AND storage_relpath <> '') AS raw_originals_stored
                """,
                configured_mailbox_id,
            )
            security_counts = await conn.fetchrow(
                """
                WITH latest_current AS (
                    SELECT DISTINCT ON (m.email_uid)
                           m.email_uid,
                           m.raw_sha256 AS message_raw_sha256,
                           s.raw_sha256 AS security_raw_sha256,
                           s.security_status,
                           s.aggregate_status,
                           s.error_message,
                           s.result_json,
                           s.checked_at
                    FROM pim_email_messages m
                    LEFT JOIN pim_email_security_checks s
                      ON s.email_uid = m.email_uid AND s.raw_sha256 = m.raw_sha256
                    WHERE m.mailbox_id = $1
                    ORDER BY m.email_uid, s.checked_at DESC NULLS LAST
                ), classified AS (
                    SELECT
                        email_uid,
                        CASE
                            WHEN security_raw_sha256 IS NULL THEN 'missing'
                            WHEN security_status = 'stored'
                              AND result_json->>'available' = 'true'
                              AND COALESCE(result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(result_json->>'placeholder', 'false') <> 'true'
                              AND result_json->>'email_uid' = email_uid
                              AND result_json->>'raw_sha256' = message_raw_sha256
                              AND COALESCE(result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(result_json->'checker_versions') = 'object'
                            THEN 'completed'
                            WHEN security_status IN ('pending_retryable', 'failed_retryable')
                            THEN 'pending_retryable'
                            WHEN COALESCE(result_json->>'queued', 'false') = 'true'
                              OR COALESCE(result_json->>'placeholder', 'false') = 'true'
                              OR result_json->>'available' = 'false'
                              OR aggregate_status = 'queued'
                            THEN 'pending'
                            WHEN security_status LIKE 'failed%' OR COALESCE(error_message, '') <> ''
                            THEN 'failed'
                            ELSE 'pending'
                        END AS state
                    FROM latest_current
                ), stale AS (
                    SELECT count(DISTINCT m.email_uid) AS stale_hash
                    FROM pim_email_messages m
                    JOIN pim_email_security_checks s
                      ON s.email_uid = m.email_uid AND s.raw_sha256 <> m.raw_sha256
                    WHERE m.mailbox_id = $1
                      AND NOT EXISTS (
                          SELECT 1
                          FROM pim_email_security_checks current_s
                          WHERE current_s.email_uid = m.email_uid
                            AND current_s.raw_sha256 = m.raw_sha256
                      )
                )
                SELECT
                    count(*) FILTER (WHERE state = 'completed') AS completed,
                    count(*) FILTER (WHERE state IN ('pending', 'pending_retryable')) AS pending,
                    count(*) FILTER (WHERE state = 'pending_retryable') AS pending_retryable,
                    count(*) FILTER (WHERE state = 'failed') AS failed,
                    count(*) FILTER (WHERE state = 'missing') AS missing,
                    (SELECT stale_hash FROM stale) AS stale_hash
                FROM classified
                """,
                configured_mailbox_id,
            )
            security_phase_rows = await conn.fetch(
                """
                SELECT phase, phase_status, count(*) AS row_count
                FROM pim_email_security_phases
                WHERE mailbox_id = $1
                GROUP BY phase, phase_status
                """,
                configured_mailbox_id,
            )
            sanitized_counts = await conn.fetchrow(
                """
                WITH current_sanitized AS (
                    SELECT DISTINCT m.email_uid
                    FROM pim_email_messages m
                    JOIN pim_email_sanitized_view_artifacts a
                      ON a.mailbox_id = m.mailbox_id
                     AND a.email_uid = m.email_uid
                     AND a.input_raw_sha256 = m.raw_sha256
                     AND a.sanitizer_policy_version = $2
                     AND a.transform_version = $3
                    WHERE m.mailbox_id = $1
                ), latest_failures AS (
                    SELECT DISTINCT ON (email_uid, raw_sha256)
                           email_uid, raw_sha256, status
                    FROM pim_email_backfill_items
                    WHERE mailbox_id = $1 AND artifact_type = 'sanitized_view'
                    ORDER BY email_uid, raw_sha256, updated_at DESC
                )
                SELECT
                    (SELECT count(*) FROM current_sanitized) AS completed,
                    count(*) FILTER (
                        WHERE m.email_uid NOT IN (SELECT email_uid FROM current_sanitized)
                          AND f.status = 'failed'
                    ) AS failed,
                    count(*) FILTER (
                        WHERE m.email_uid NOT IN (SELECT email_uid FROM current_sanitized)
                          AND COALESCE(f.status, '') <> 'failed'
                    ) AS pending
                FROM pim_email_messages m
                LEFT JOIN latest_failures f
                  ON f.email_uid = m.email_uid AND f.raw_sha256 = m.raw_sha256
                WHERE m.mailbox_id = $1
                """,
                configured_mailbox_id,
                SANITIZED_VIEW_POLICY_VERSION,
                SANITIZED_VIEW_TRANSFORM_VERSION,
            )
            external_counts = await conn.fetchrow(
                """
                WITH captured AS (
                    SELECT COALESCE(sum(
                        CASE
                          WHEN jsonb_typeof(metadata_json->'remote_image_sources') = 'array'
                          THEN jsonb_array_length(metadata_json->'remote_image_sources')
                          ELSE 0
                        END
                    ), 0) AS captured_count
                    FROM pim_email_messages
                    WHERE mailbox_id = $1
                ), shared_digest AS (
                    SELECT DISTINCT mailbox_id, canonical_url_digest
                    FROM pim_email_shared_assets
                    WHERE mailbox_id = $1 AND canonical_url_digest <> ''
                ), derivatives AS (
                    SELECT
                        d.mailbox_id,
                        d.status,
                        d.canonical_url_digest,
                        d.shared_asset_uid,
                        sd.canonical_url_digest IS NOT NULL AS has_shared_asset
                    FROM pim_email_external_image_derivatives
                    d
                    LEFT JOIN shared_digest sd
                      ON sd.mailbox_id = d.mailbox_id
                     AND sd.canonical_url_digest = d.canonical_url_digest
                    WHERE d.mailbox_id = $1
                ), rows AS (
                    SELECT status, count(*) AS row_count
                    FROM derivatives
                    GROUP BY status
                ), totals AS (
                    SELECT
                        count(*) AS recorded_count,
                        count(DISTINCT canonical_url_digest) FILTER (
                            WHERE canonical_url_digest <> ''
                        ) AS recorded_unique_canonical_urls,
                        count(DISTINCT canonical_url_digest) FILTER (
                            WHERE canonical_url_digest <> '' AND status = 'stored'
                        ) AS stored_unique_canonical_urls,
                        count(DISTINCT canonical_url_digest) FILTER (
                            WHERE canonical_url_digest <> ''
                              AND status IN ('pending','fetched','transformed')
                        ) AS pending_recorded_unique_canonical_urls,
                        count(*) FILTER (
                            WHERE status IN ('pending','fetched','transformed')
                              AND canonical_url_digest <> ''
                              AND has_shared_asset
                        ) AS pending_reference_rows_with_shared_asset,
                        count(DISTINCT canonical_url_digest) FILTER (
                            WHERE canonical_url_digest <> ''
                              AND status IN ('pending','fetched','transformed')
                              AND has_shared_asset
                        ) AS pending_unique_canonical_urls_with_shared_asset,
                        count(*) FILTER (
                            WHERE status IN ('pending','fetched','transformed')
                              AND canonical_url_digest <> ''
                              AND NOT has_shared_asset
                        ) AS pending_reference_rows_needing_fetch,
                        count(DISTINCT canonical_url_digest) FILTER (
                            WHERE canonical_url_digest <> ''
                              AND status IN ('pending','fetched','transformed')
                              AND NOT has_shared_asset
                        ) AS pending_unique_canonical_urls_needing_fetch,
                        count(*) FILTER (
                            WHERE status = 'stored' AND shared_asset_uid <> ''
                        ) AS stored_shared_asset_links,
                        count(*) FILTER (
                            WHERE status = 'stored' AND shared_asset_uid = ''
                        ) AS unlinked_stored_reference_rows
                    FROM derivatives
                ), shared AS (
                    SELECT
                        count(*) AS shared_assets_stored,
                        COALESCE(sum(encrypted_size), 0) AS shared_asset_encrypted_bytes
                    FROM pim_email_shared_assets
                    WHERE mailbox_id = $1
                )
                SELECT
                    (SELECT captured_count FROM captured) AS captured,
                    (SELECT recorded_count FROM totals) AS recorded_reference_rows,
                    COALESCE((SELECT row_count FROM rows WHERE status = 'stored'), 0) AS stored,
                    COALESCE((SELECT row_count FROM rows WHERE status = 'blocked'), 0) AS blocked,
                    COALESCE((SELECT row_count FROM rows WHERE status = 'failed'), 0) AS failed,
                    COALESCE((SELECT row_count FROM rows WHERE status = 'unavailable'), 0) AS unavailable,
                    COALESCE((SELECT sum(row_count) FROM rows WHERE status IN ('pending','fetched','transformed')), 0)
                      + GREATEST((SELECT captured_count FROM captured) - (SELECT recorded_count FROM totals), 0) AS pending,
                    (SELECT recorded_unique_canonical_urls FROM totals) AS recorded_unique_canonical_urls,
                    (SELECT stored_unique_canonical_urls FROM totals) AS stored_unique_canonical_urls,
                    (SELECT pending_recorded_unique_canonical_urls FROM totals)
                        AS pending_recorded_unique_canonical_urls,
                    (SELECT pending_reference_rows_with_shared_asset FROM totals)
                        AS pending_reference_rows_with_shared_asset,
                    (SELECT pending_unique_canonical_urls_with_shared_asset FROM totals)
                        AS pending_unique_canonical_urls_with_shared_asset,
                    (SELECT pending_reference_rows_needing_fetch FROM totals)
                        AS pending_reference_rows_needing_fetch,
                    (SELECT pending_unique_canonical_urls_needing_fetch FROM totals)
                        AS pending_unique_canonical_urls_needing_fetch,
                    GREATEST(
                        (SELECT recorded_count FROM totals)
                        - (SELECT recorded_unique_canonical_urls FROM totals),
                        0
                    ) AS recorded_duplicate_reference_rows,
                    (SELECT stored_shared_asset_links FROM totals) AS stored_shared_asset_links,
                    (SELECT unlinked_stored_reference_rows FROM totals)
                        AS unlinked_stored_reference_rows,
                    (SELECT shared_assets_stored FROM shared) AS shared_assets_stored,
                    (SELECT shared_asset_encrypted_bytes FROM shared)
                        AS shared_asset_encrypted_bytes
                """,
                configured_mailbox_id,
            )
            folder_counts = await conn.fetchrow(
                """
                WITH folder_effective AS (
                    SELECT
                        fm.folder_name,
                        fm.remote_moved_at,
                        CASE
                            WHEN f.special_use_role IN ('archive','drafts','sent','trash','junk','spam')
                            THEN f.special_use_role
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 IN ('archive','archives','archived')
                            THEN 'archive'
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 IN ('draft','drafts')
                            THEN 'drafts'
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 IN ('sent','sentmail','sentmessages','sentitems')
                            THEN 'sent'
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 IN ('trash','rubbish','bin','deleted','deleteditems')
                            THEN 'trash'
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 = 'junk'
                            THEN 'junk'
                            WHEN regexp_replace(lower(split_part(f.folder_name, '/', 1)), '[^a-z0-9]+', '', 'g')
                                 = 'spam'
                            THEN 'spam'
                            ELSE ''
                        END AS effective_special_use_role
                    FROM pim_email_folder_memberships fm
                    JOIN pim_email_local_folders f
                      ON f.mailbox_id = fm.mailbox_id AND f.folder_uid = fm.folder_uid
                    WHERE fm.mailbox_id = $1
                )
                SELECT
                    count(*) FILTER (
                        WHERE effective_special_use_role IN ('archive','drafts','sent','trash','junk','spam')
                    ) AS special_use_downloaded,
                    count(*) FILTER (
                        WHERE effective_special_use_role IN ('archive','drafts','sent','trash','junk','spam')
                          AND remote_moved_at IS NULL
                    ) AS special_use_unmoved,
                    count(*) FILTER (
                        WHERE effective_special_use_role IN ('archive','drafts','sent','trash','junk','spam')
                          AND remote_moved_at IS NOT NULL
                    ) AS special_use_moved,
                    count(*) FILTER (
                        WHERE (folder_name = 'INBOX' OR folder_name LIKE 'INBOX/%')
                          AND remote_moved_at IS NOT NULL
                    ) AS inbox_subfolders_moved
                FROM folder_effective
                """,
                configured_mailbox_id,
            )
            last_run = await conn.fetchrow(
                """
                SELECT run_id, status, started_at, finished_at, summary_json
                FROM pim_email_download_runs
                WHERE mailbox_id = $1
                ORDER BY started_at DESC
                LIMIT 1
                """,
                configured_mailbox_id,
            )
        finally:
            await conn.close()
        message_count = int(row["messages"] or 0) if row else 0
        security_completed = int(_row_get(security_counts, "completed", 0) or 0)
        sanitized_completed = int(_row_get(sanitized_counts, "completed", 0) or 0)
        security_phase_counts: dict[str, int] = {}
        for phase_row in security_phase_rows:
            key = (
                f"{str(_row_get(phase_row, 'phase', '') or '')}_"
                f"{str(_row_get(phase_row, 'phase_status', '') or '')}"
            ).strip("_")
            if key:
                security_phase_counts[key] = int(_row_get(phase_row, "row_count", 0) or 0)
        return {
            "schema": "xarta.pim_email.local_corpus.status.v1",
            "mailbox_id": configured_mailbox_id,
            "message_count": message_count,
            "folder_count": int(row["folders"] or 0) if row else 0,
            "membership_count": int(row["memberships"] or 0) if row else 0,
            "transformed_asset_count": int(row["transformed_assets"] or 0) if row else 0,
            "shared_asset_count": int(row["shared_assets"] or 0) if row else 0,
            "raw_originals": {
                "stored": int(row["raw_originals_stored"] or 0) if row else 0,
                "messages": message_count,
            },
            "security_results": {
                "completed": security_completed,
                "pending": int(_row_get(security_counts, "pending", 0) or 0),
                "pending_retryable": int(_row_get(security_counts, "pending_retryable", 0) or 0),
                "failed": int(_row_get(security_counts, "failed", 0) or 0),
                "missing": int(_row_get(security_counts, "missing", 0) or 0),
                "stale_hash": int(_row_get(security_counts, "stale_hash", 0) or 0),
            },
            "security_phases": {
                "deterministic_complete": security_phase_counts.get("deterministic_complete", 0),
                "deterministic_failed_retryable": security_phase_counts.get(
                    "deterministic_failed_retryable", 0
                ),
                "llm_complete": security_phase_counts.get("llm_complete", 0),
                "llm_failed_retryable": security_phase_counts.get("llm_failed_retryable", 0),
            },
            "sanitized_derivatives": {
                "completed": sanitized_completed,
                "pending": int(_row_get(sanitized_counts, "pending", 0) or 0),
                "failed": int(_row_get(sanitized_counts, "failed", 0) or 0),
                "policy_version": SANITIZED_VIEW_POLICY_VERSION,
                "transform_version": SANITIZED_VIEW_TRANSFORM_VERSION,
            },
            "external_image_derivatives": {
                "captured": int(_row_get(external_counts, "captured", 0) or 0),
                "recorded_reference_rows": int(
                    _row_get(external_counts, "recorded_reference_rows", 0) or 0
                ),
                "stored": int(_row_get(external_counts, "stored", 0) or 0),
                "stored_reference_rows": int(_row_get(external_counts, "stored", 0) or 0),
                "blocked": int(_row_get(external_counts, "blocked", 0) or 0),
                "failed": int(_row_get(external_counts, "failed", 0) or 0),
                "unavailable": int(_row_get(external_counts, "unavailable", 0) or 0),
                "pending": int(_row_get(external_counts, "pending", 0) or 0),
                "pending_reference_rows": int(_row_get(external_counts, "pending", 0) or 0),
                "recorded_unique_canonical_urls": int(
                    _row_get(external_counts, "recorded_unique_canonical_urls", 0) or 0
                ),
                "stored_unique_canonical_urls": int(
                    _row_get(external_counts, "stored_unique_canonical_urls", 0) or 0
                ),
                "pending_recorded_unique_canonical_urls": int(
                    _row_get(external_counts, "pending_recorded_unique_canonical_urls", 0) or 0
                ),
                "pending_reference_rows_with_shared_asset": int(
                    _row_get(external_counts, "pending_reference_rows_with_shared_asset", 0) or 0
                ),
                "pending_unique_canonical_urls_with_shared_asset": int(
                    _row_get(external_counts, "pending_unique_canonical_urls_with_shared_asset", 0)
                    or 0
                ),
                "pending_reference_rows_needing_fetch": int(
                    _row_get(external_counts, "pending_reference_rows_needing_fetch", 0) or 0
                ),
                "pending_unique_canonical_urls_needing_fetch": int(
                    _row_get(external_counts, "pending_unique_canonical_urls_needing_fetch", 0) or 0
                ),
                "recorded_duplicate_reference_rows": int(
                    _row_get(external_counts, "recorded_duplicate_reference_rows", 0) or 0
                ),
                "stored_shared_asset_links": int(
                    _row_get(external_counts, "stored_shared_asset_links", 0) or 0
                ),
                "unlinked_stored_reference_rows": int(
                    _row_get(external_counts, "unlinked_stored_reference_rows", 0) or 0
                ),
                "shared_assets_stored": int(
                    _row_get(external_counts, "shared_assets_stored", 0) or 0
                ),
                "shared_asset_encrypted_bytes": int(
                    _row_get(external_counts, "shared_asset_encrypted_bytes", 0) or 0
                ),
            },
            "special_use_folders": {
                "downloaded_memberships": int(
                    _row_get(folder_counts, "special_use_downloaded", 0) or 0
                ),
                "unmoved_memberships": int(_row_get(folder_counts, "special_use_unmoved", 0) or 0),
                "moved_memberships": int(_row_get(folder_counts, "special_use_moved", 0) or 0),
            },
            "inbox_subfolders": {
                "moved_memberships": int(_row_get(folder_counts, "inbox_subfolders_moved", 0) or 0),
            },
            "render_gate": {
                "blocked_security_incomplete": max(message_count - security_completed, 0),
                "blocked_sanitized_missing": max(security_completed - sanitized_completed, 0),
            },
            "available": bool(row and message_count > 0),
            "last_run": _download_run_public(last_run) if last_run else None,
            "download_orphan_reconcile": download_orphan_reconcile,
        }

    async def completed_security_result(
        self,
        *,
        email_uid: str,
        raw_sha256: str,
    ) -> dict[str, Any] | None:
        await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT result_json, security_status, error_message, checked_at, raw_sha256
                FROM pim_email_security_checks
                WHERE email_uid = $1 AND raw_sha256 = $2
                ORDER BY checked_at DESC
                LIMIT 1
                """,
                clean_uid,
                str(raw_sha256 or ""),
            )
        finally:
            await conn.close()
        return _completed_security_result_from_row(
            row,
            email_uid=clean_uid,
            raw_sha256=str(raw_sha256 or ""),
        )

    async def local_folders(self, *, mailbox_id: str | None = None) -> list[dict[str, Any]]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT f.folder_uid, f.parent_folder_uid, f.folder_name, f.display_name,
                       f.delimiter, f.special_use_role, f.flags_json, f.metadata_json,
                       count(m.email_uid) AS message_count
                FROM pim_email_local_folders f
                LEFT JOIN pim_email_folder_memberships m
                  ON m.mailbox_id = f.mailbox_id AND m.folder_uid = f.folder_uid
                WHERE f.mailbox_id = $1 AND f.tombstoned_at IS NULL
                GROUP BY f.folder_uid, f.parent_folder_uid, f.folder_name, f.display_name,
                         f.delimiter, f.special_use_role, f.flags_json, f.metadata_json
                ORDER BY CASE WHEN upper(f.folder_name) = 'INBOX' THEN 0 ELSE 1 END, lower(f.folder_name)
                """,
                configured_mailbox_id,
            )
        finally:
            await conn.close()
        return [_folder_row_public(row) for row in rows]

    async def local_folder_messages(
        self,
        *,
        folder: str = "INBOX",
        mailbox_id: str | None = None,
        limit: int = 25,
        ensure_schema: bool = True,
    ) -> list[dict[str, Any]]:
        if ensure_schema:
            await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_folder = clean_folder_name(folder)
        safe_limit = max(1, min(int(limit), 200))
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT m.email_uid, m.raw_sha256, m.message_id, m.subject, m.from_addr,
                       m.to_addr, m.date_header, m.uid_info_json, m.metadata_json,
                       fm.folder_name, fm.imap_uid, fm.uidvalidity, fm.flags_json,
                       fm.last_seen_at, m.updated_at
                FROM pim_email_folder_memberships fm
                JOIN pim_email_messages m ON m.email_uid = fm.email_uid
                WHERE fm.mailbox_id = $1 AND fm.folder_name = $2
                ORDER BY fm.last_seen_at DESC, m.updated_at DESC
                LIMIT $3
                """,
                configured_mailbox_id,
                clean_folder,
                safe_limit,
            )
        finally:
            await conn.close()
        return [_local_message_row_public(row) for row in rows]

    async def read_local_message(
        self,
        email_uid: str,
        *,
        mailbox_id: str | None = None,
        ensure_schema: bool = True,
    ) -> dict[str, Any]:
        if ensure_schema:
            await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_uid = clean_email_uid(email_uid)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM pim_email_messages
                WHERE mailbox_id = $1 AND email_uid = $2
                """,
                configured_mailbox_id,
                clean_uid,
            )
            memberships = await conn.fetch(
                """
                SELECT folder_name, folder_uid, imap_uid, uidvalidity, flags_json, last_seen_at,
                       remote_moved_at, remote_move_target
                FROM pim_email_folder_memberships
                WHERE mailbox_id = $1 AND email_uid = $2
                ORDER BY last_seen_at DESC
                """,
                configured_mailbox_id,
                clean_uid,
            )
            assets = await conn.fetch(
                """
                SELECT asset_uid, shared_asset_uid, source_url, content_type, transformed_sha256,
                       storage_relpath, width, height, transform_version, metadata_json,
                       canonical_url_digest, updated_at
                FROM pim_email_transformed_assets
                WHERE mailbox_id = $1 AND email_uid = $2
                ORDER BY updated_at DESC
                """,
                configured_mailbox_id,
                clean_uid,
            )
            stored_hash = str(_row_get(row, "raw_sha256", "")) if row is not None else ""
            security = await conn.fetchrow(
                """
                SELECT result_json, security_status, error_message, checked_at, raw_sha256
                FROM pim_email_security_checks
                WHERE email_uid = $1 AND raw_sha256 = $2
                ORDER BY checked_at DESC
                LIMIT 1
                """,
                clean_uid,
                stored_hash,
            )
            sanitized = await conn.fetchrow(
                """
                SELECT artifact_uid, email_uid, input_raw_sha256, sanitizer_policy_version,
                       transform_version, output_sha256, storage_relpath, encrypted_size,
                       views_available_json, safety_counts_json, derivation_json,
                       generated_at, updated_at
                FROM pim_email_sanitized_view_artifacts
                WHERE mailbox_id = $1
                  AND email_uid = $2
                  AND input_raw_sha256 = $3
                  AND sanitizer_policy_version = $4
                  AND transform_version = $5
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                configured_mailbox_id,
                clean_uid,
                stored_hash,
                SANITIZED_VIEW_POLICY_VERSION,
                SANITIZED_VIEW_TRANSFORM_VERSION,
            )
        finally:
            await conn.close()
        if row is None:
            raise EmailOperationError("Local email message is not stored")
        raw = read_encrypted_bytes(str(row["storage_relpath"]))
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        if raw_sha256 != str(row["raw_sha256"]):
            raise EmailOperationError("Local email content hash verification failed")
        completed_security = _completed_security_result_from_row(
            security,
            email_uid=clean_uid,
            raw_sha256=raw_sha256,
        )
        if completed_security is None:
            message = _local_message_envelope(
                row,
                memberships=list(memberships),
                raw_sha256=raw_sha256,
                security=_security_block_payload(
                    email_uid=clean_uid,
                    raw_sha256=raw_sha256,
                    reason="completed_security_result_missing",
                    row=security,
                ),
                body_blocked=True,
            )
            message["stored"]["transformed_assets"] = [_asset_row_public(item) for item in assets]
            return message
        if sanitized is None:
            message = _local_message_envelope(
                row,
                memberships=list(memberships),
                raw_sha256=raw_sha256,
                security=completed_security,
                body_blocked=True,
            )
            message["blocked_reason"] = "sanitized_derivative_missing"
            message["stored"]["transformed_assets"] = [_asset_row_public(item) for item in assets]
            return message
        try:
            sanitized_payload = read_sanitized_view_artifact(sanitized)
        except Exception as exc:
            message = _local_message_envelope(
                row,
                memberships=list(memberships),
                raw_sha256=raw_sha256,
                security=completed_security,
                body_blocked=True,
            )
            message["blocked_reason"] = "sanitized_derivative_unreadable"
            message["blocked_error"] = exc.__class__.__name__
            message["stored"]["transformed_assets"] = [_asset_row_public(item) for item in assets]
            return message
        message = _local_message_envelope(
            row,
            memberships=list(memberships),
            raw_sha256=raw_sha256,
            security=completed_security,
            body_blocked=False,
            sanitized_artifact=sanitized_payload,
        )
        message["stored"]["transformed_assets"] = [_asset_row_public(item) for item in assets]
        return message

    async def record_download_run_start(
        self,
        *,
        run_id: str,
        mailbox_id: str,
        apply_remote_moves: bool,
        downloaded_folder: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.ensure_schema()
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_download_runs (
                    run_id, mailbox_id, status, apply_remote_moves, downloaded_folder, metadata_json
                )
                VALUES ($1,$2,'running',$3,$4,$5::jsonb)
                ON CONFLICT (run_id) DO UPDATE SET
                    started_at = now(),
                    finished_at = NULL,
                    status = 'running',
                    apply_remote_moves = EXCLUDED.apply_remote_moves,
                    downloaded_folder = EXCLUDED.downloaded_folder,
                    summary_json = '{}'::jsonb,
                    metadata_json = EXCLUDED.metadata_json;
                """,
                run_id,
                mailbox_id,
                apply_remote_moves,
                downloaded_folder,
                _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()

    async def reconcile_orphaned_download_runs(
        self,
        *,
        active_run_ids: set[str] | list[str] | tuple[str, ...],
        reason: str,
        mailbox_id: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_active = sorted({str(run_id or "").strip() for run_id in active_run_ids if run_id})
        metadata = {
            "schema": "xarta.pim_email.download_orphan_reconcile.v1",
            "reason": str(reason or "stack_process_not_active"),
            "active_run_ids": clean_active,
        }
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                UPDATE pim_email_download_runs
                SET status = 'interrupted-orphaned',
                    finished_at = COALESCE(finished_at, now()),
                    metadata_json = metadata_json || jsonb_build_object(
                        'orphan_reconcile', $3::jsonb
                    )
                WHERE mailbox_id = $1
                  AND status = 'running'
                  AND NOT (run_id = ANY($2::text[]))
                RETURNING run_id
                """,
                configured_mailbox_id,
                clean_active,
                _json_dumps(metadata, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return {
            "schema": "xarta.pim_email.download_orphan_reconcile.result.v1",
            "mailbox_id": configured_mailbox_id,
            "active_run_ids": clean_active,
            "marked_orphaned": [str(row["run_id"]) for row in rows],
            "marked_count": len(rows),
        }

    async def record_download_run_finish(
        self,
        *,
        run_id: str,
        status: str,
        summary: dict[str, Any],
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_download_runs
                SET status = $2, finished_at = now(), summary_json = $3::jsonb
                WHERE run_id = $1
                """,
                run_id,
                status,
                _json_dumps(summary, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()

    async def record_download_event(
        self,
        *,
        run_id: str,
        event_type: str,
        status: str = "",
        message: str = "",
        mailbox_id: str = "",
        batch_id: str = "",
        folder_uid: str = "",
        folder_name: str = "",
        email_uid: str = "",
        imap_uid: str = "",
        uidvalidity: str = "",
        error_class: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_download_events (
                    event_id, run_id, batch_id, mailbox_id, folder_uid, folder_name,
                    email_uid, imap_uid, uidvalidity, event_type, status, message,
                    error_class, metadata_json
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
                """,
                _stable_id(
                    "email-event", run_id, event_type, folder_name, imap_uid, str(time.time_ns())
                ),
                run_id,
                batch_id,
                mailbox_id,
                folder_uid,
                folder_name,
                email_uid,
                imap_uid,
                uidvalidity,
                event_type,
                status,
                message[:1000],
                error_class[:200],
                _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()

    async def record_download_batch_start(
        self,
        *,
        batch_id: str,
        run_id: str,
        mailbox_id: str,
        folder_snapshot: dict[str, Any],
        uids: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        safe_uids = [clean_uid_value(uid) for uid in uids]
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_download_batches (
                    batch_id, run_id, mailbox_id, folder_uid, folder_name, uidvalidity,
                    uid_min, uid_max, planned_count, metadata_json, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,now())
                ON CONFLICT (batch_id) DO UPDATE SET
                    uid_min = EXCLUDED.uid_min,
                    uid_max = EXCLUDED.uid_max,
                    planned_count = EXCLUDED.planned_count,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = now();
                """,
                batch_id,
                run_id,
                mailbox_id,
                str(folder_snapshot.get("folder_uid") or ""),
                str(folder_snapshot.get("folder_name") or ""),
                str(folder_snapshot.get("uidvalidity") or ""),
                min(safe_uids, key=int) if safe_uids else "",
                max(safe_uids, key=int) if safe_uids else "",
                len(safe_uids),
                _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()

    async def record_download_batch_finish(
        self,
        *,
        batch_id: str,
        processed_count: int = 0,
        skipped_count: int = 0,
        moved_count: int = 0,
        failed_count: int = 0,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_download_batches
                SET processed_count = $2,
                    skipped_count = $3,
                    moved_count = $4,
                    failed_count = $5,
                    metadata_json = COALESCE($6::jsonb, metadata_json),
                    updated_at = now()
                WHERE batch_id = $1
                """,
                batch_id,
                int(processed_count),
                int(skipped_count),
                int(moved_count),
                int(failed_count),
                _json_dumps(metadata, sort_keys=True, separators=(",", ":"))
                if metadata is not None
                else None,
            )
        finally:
            await conn.close()

    async def save_folder_snapshot(
        self,
        *,
        mailbox_id: str,
        folder: dict[str, Any],
        status: dict[str, Any],
    ) -> dict[str, Any]:
        await self.ensure_schema()
        folder_name = clean_folder_name(str(folder.get("name") or "INBOX"))
        delimiter = str(folder.get("delimiter") or "/") or "/"
        flags = [str(item).lower() for item in folder.get("flags") or []]
        role = special_use_role(folder_name, flags)
        folder_uid = folder_uid_for(mailbox_id, folder_name)
        parent_uid = parent_folder_uid_for(mailbox_id, folder_name, delimiter)
        snapshot_id = _stable_id(
            "email-folder-snapshot",
            mailbox_id,
            folder_uid,
            str(time.time_ns()),
        )
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_local_folders (
                    folder_uid, mailbox_id, parent_folder_uid, folder_name, display_name,
                    delimiter, special_use_role, flags_json, metadata_json, tombstoned_at, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,NULL,now())
                ON CONFLICT (mailbox_id, folder_name) DO UPDATE SET
                    parent_folder_uid = EXCLUDED.parent_folder_uid,
                    display_name = EXCLUDED.display_name,
                    delimiter = EXCLUDED.delimiter,
                    special_use_role = EXCLUDED.special_use_role,
                    flags_json = EXCLUDED.flags_json,
                    metadata_json = EXCLUDED.metadata_json,
                    tombstoned_at = NULL,
                    updated_at = now();
                """,
                folder_uid,
                mailbox_id,
                parent_uid,
                folder_name,
                folder_name.rsplit(delimiter, 1)[-1] if delimiter in folder_name else folder_name,
                delimiter,
                role,
                _json_dumps(flags, sort_keys=True, separators=(",", ":")),
                _json_dumps(
                    {
                        "schema": "xarta.pim_email.local_folder.metadata.v1",
                        "raw_folder": folder,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            await conn.execute(
                """
                INSERT INTO pim_email_folder_snapshots (
                    snapshot_id, mailbox_id, folder_uid, folder_name, delimiter,
                    special_use_role, flags_json, uidvalidity, uidnext, messages_count,
                    unseen_count, status_json
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12::jsonb)
                """,
                snapshot_id,
                mailbox_id,
                folder_uid,
                folder_name,
                delimiter,
                role,
                _json_dumps(flags, sort_keys=True, separators=(",", ":")),
                str(status.get("UIDVALIDITY") or status.get("uidvalidity") or ""),
                str(status.get("UIDNEXT") or status.get("uidnext") or ""),
                int(status.get("MESSAGES") or status.get("messages") or 0),
                int(status.get("UNSEEN") or status.get("unseen") or 0),
                _json_dumps(status, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return {
            "snapshot_id": snapshot_id,
            "folder_uid": folder_uid,
            "folder_name": folder_name,
            "delimiter": delimiter,
            "flags": flags,
            "special_use_role": role,
            "uidvalidity": str(status.get("UIDVALIDITY") or status.get("uidvalidity") or ""),
            "uidnext": str(status.get("UIDNEXT") or status.get("uidnext") or ""),
            "messages_count": int(status.get("MESSAGES") or status.get("messages") or 0),
        }

    async def save_downloaded_email(
        self,
        *,
        mailbox_id: str,
        folder_snapshot: dict[str, Any],
        imap_uid: str,
        flags: list[str],
        raw: bytes,
        parsed: dict[str, Any],
        storage: dict[str, Any],
        security: dict[str, Any] | None,
        security_error: str = "",
        transformed_assets: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.ensure_schema()
        email_uid = clean_email_uid(str(parsed.get("email_uid") or ""))
        headers = parsed.get("headers") if isinstance(parsed.get("headers"), dict) else {}
        email_uid_info = (
            parsed.get("email_uid_info") if isinstance(parsed.get("email_uid_info"), dict) else {}
        )
        uidvalidity = str(folder_snapshot.get("uidvalidity") or "")
        folder_uid = str(folder_snapshot.get("folder_uid") or "")
        folder_name = str(folder_snapshot.get("folder_name") or "")
        membership_id = _stable_id(
            "email-membership",
            mailbox_id,
            folder_uid,
            uidvalidity,
            imap_uid,
        )
        raw_sha256 = str(storage.get("raw_sha256") or hashlib.sha256(raw).hexdigest())
        conn = await self._connect()
        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO pim_email_messages (
                        email_uid, mailbox_id, raw_sha256, message_id, subject, from_addr,
                        to_addr, date_header, uid_info_json, headers_json, metadata_json,
                        storage_relpath, encrypted_size, encryption_json, updated_at
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13,$14::jsonb,now())
                    ON CONFLICT (email_uid) DO UPDATE SET
                        mailbox_id = EXCLUDED.mailbox_id,
                        raw_sha256 = EXCLUDED.raw_sha256,
                        message_id = EXCLUDED.message_id,
                        subject = EXCLUDED.subject,
                        from_addr = EXCLUDED.from_addr,
                        to_addr = EXCLUDED.to_addr,
                        date_header = EXCLUDED.date_header,
                        uid_info_json = EXCLUDED.uid_info_json,
                        headers_json = EXCLUDED.headers_json,
                        metadata_json = EXCLUDED.metadata_json,
                        storage_relpath = EXCLUDED.storage_relpath,
                        encrypted_size = EXCLUDED.encrypted_size,
                        encryption_json = EXCLUDED.encryption_json,
                        updated_at = now();
                    """,
                    email_uid,
                    mailbox_id,
                    raw_sha256,
                    str(headers.get("message_id") or ""),
                    str(headers.get("subject") or ""),
                    str(headers.get("from") or ""),
                    str(headers.get("to") or ""),
                    str(headers.get("date") or ""),
                    _json_dumps(email_uid_info, sort_keys=True, separators=(",", ":")),
                    _json_dumps(headers, sort_keys=True, separators=(",", ":")),
                    _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
                    str(storage.get("storage_relpath") or ""),
                    int(storage.get("encrypted_size") or 0),
                    _json_dumps(
                        storage.get("encryption") or {}, sort_keys=True, separators=(",", ":")
                    ),
                )
                await conn.execute(
                    """
                    INSERT INTO pim_email_folder_memberships (
                        membership_id, mailbox_id, folder_uid, folder_name, email_uid,
                        imap_uid, uidvalidity, flags_json, source_snapshot_id,
                        metadata_json, last_seen_at
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10::jsonb,now())
                    ON CONFLICT (mailbox_id, folder_uid, uidvalidity, imap_uid) DO UPDATE SET
                        email_uid = EXCLUDED.email_uid,
                        folder_name = EXCLUDED.folder_name,
                        flags_json = EXCLUDED.flags_json,
                        source_snapshot_id = EXCLUDED.source_snapshot_id,
                        metadata_json = EXCLUDED.metadata_json,
                        last_seen_at = now();
                    """,
                    membership_id,
                    mailbox_id,
                    folder_uid,
                    folder_name,
                    email_uid,
                    imap_uid,
                    uidvalidity,
                    _json_dumps(flags, sort_keys=True, separators=(",", ":")),
                    str(folder_snapshot.get("snapshot_id") or ""),
                    _json_dumps(
                        {
                            "schema": "xarta.pim_email.folder_membership.metadata.v1",
                            "source": "imap-download",
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
                if security and security.get("available"):
                    aggregate = (
                        security.get("aggregate")
                        if isinstance(security.get("aggregate"), dict)
                        else {}
                    )
                    storage_security = _security_result_for_storage(
                        security,
                        email_uid=email_uid,
                        raw_sha256=raw_sha256,
                        parsed=parsed,
                    )
                    security_check_id = _stable_id("email-security", email_uid, raw_sha256)
                    await conn.execute(
                        """
                        INSERT INTO pim_email_security_checks (
                            security_check_id, mailbox_id, folder, uid, message_id, raw_sha256,
                            aggregate_status, aggregate_score, llm_called, result_json, checked_at,
                            email_uid, security_status, checker_versions_json, metadata_json
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,now(),$11,'stored',$12::jsonb,$13::jsonb)
                        ON CONFLICT (mailbox_id, folder, uid, raw_sha256) DO UPDATE SET
                            message_id = EXCLUDED.message_id,
                            aggregate_status = EXCLUDED.aggregate_status,
                            aggregate_score = EXCLUDED.aggregate_score,
                            llm_called = EXCLUDED.llm_called,
                            result_json = EXCLUDED.result_json,
                            email_uid = EXCLUDED.email_uid,
                            security_status = EXCLUDED.security_status,
                            checker_versions_json = EXCLUDED.checker_versions_json,
                            metadata_json = EXCLUDED.metadata_json,
                            checked_at = now();
                        """,
                        security_check_id,
                        mailbox_id,
                        folder_name,
                        imap_uid,
                        str(headers.get("message_id") or ""),
                        raw_sha256,
                        str(aggregate.get("status") or "amber"),
                        int(aggregate.get("score") or aggregate.get("risk_score") or 0),
                        bool(
                            aggregate.get("llm_called") or (security.get("llm") or {}).get("called")
                        ),
                        _json_dumps(storage_security, sort_keys=True, separators=(",", ":")),
                        email_uid,
                        _json_dumps(
                            _security_checker_versions(storage_security),
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                        _json_dumps(
                            {
                                "email_uid": email_uid,
                                "raw_sha256": raw_sha256,
                                "policy_version": SECURITY_POLICY_VERSION,
                            },
                            sort_keys=True,
                            separators=(",", ":"),
                        ),
                    )
                for asset in transformed_assets or []:
                    await conn.execute(
                        """
                        INSERT INTO pim_email_transformed_assets (
                            asset_uid, email_uid, mailbox_id, source_url, content_type,
                            raw_sha256, transformed_sha256, storage_relpath, encrypted_size,
                            width, height, transform_version, encryption_json, metadata_json,
                            shared_asset_uid, canonical_url_digest, updated_at
                        )
                        VALUES (
                            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,
                            $15,$16,now()
                        )
                        ON CONFLICT (asset_uid) DO UPDATE SET
                            source_url = EXCLUDED.source_url,
                            content_type = EXCLUDED.content_type,
                            raw_sha256 = EXCLUDED.raw_sha256,
                            transformed_sha256 = EXCLUDED.transformed_sha256,
                            storage_relpath = EXCLUDED.storage_relpath,
                            encrypted_size = EXCLUDED.encrypted_size,
                            width = EXCLUDED.width,
                            height = EXCLUDED.height,
                            transform_version = EXCLUDED.transform_version,
                            encryption_json = EXCLUDED.encryption_json,
                            metadata_json = EXCLUDED.metadata_json,
                            shared_asset_uid = EXCLUDED.shared_asset_uid,
                            canonical_url_digest = EXCLUDED.canonical_url_digest,
                            updated_at = now();
                        """,
                        str(asset["asset_uid"]),
                        email_uid,
                        mailbox_id,
                        str(asset.get("source_url") or ""),
                        str(asset.get("content_type") or "image/jpeg"),
                        str(asset.get("raw_sha256") or ""),
                        str(asset.get("transformed_sha256") or ""),
                        str(asset.get("storage_relpath") or ""),
                        int(asset.get("encrypted_size") or 0),
                        int(asset.get("width") or 0),
                        int(asset.get("height") or 0),
                        str(asset.get("transform_version") or "jpeg-v1"),
                        _json_dumps(
                            asset.get("encryption") or {}, sort_keys=True, separators=(",", ":")
                        ),
                        _json_dumps(
                            asset.get("metadata") or {}, sort_keys=True, separators=(",", ":")
                        ),
                        str(asset.get("shared_asset_uid") or ""),
                        str(
                            asset.get("canonical_url_digest")
                            or _external_image_canonical_digest(str(asset.get("source_url") or ""))
                        ),
                    )
        finally:
            await conn.close()

    async def mark_remote_moved(
        self,
        *,
        mailbox_id: str,
        folder_uid: str,
        uidvalidity: str,
        imap_uid: str,
        target_folder: str,
    ) -> None:
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_folder_memberships
                SET remote_moved_at = now(), remote_move_target = $5
                WHERE mailbox_id = $1 AND folder_uid = $2 AND uidvalidity = $3 AND imap_uid = $4
                """,
                mailbox_id,
                folder_uid,
                uidvalidity,
                imap_uid,
                target_folder,
            )
        finally:
            await conn.close()

    async def store_transformed_asset(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        asset: dict[str, Any],
        ensure_schema: bool = True,
        update_shared_reference_count: bool = True,
    ) -> dict[str, Any]:
        if ensure_schema:
            await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO pim_email_transformed_assets (
                    asset_uid, email_uid, mailbox_id, source_url, content_type,
                    raw_sha256, transformed_sha256, storage_relpath, encrypted_size,
                    width, height, transform_version, encryption_json, metadata_json,
                    shared_asset_uid, canonical_url_digest, updated_at
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,$16,now()
                )
                ON CONFLICT (asset_uid) DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    content_type = EXCLUDED.content_type,
                    raw_sha256 = EXCLUDED.raw_sha256,
                    transformed_sha256 = EXCLUDED.transformed_sha256,
                    storage_relpath = EXCLUDED.storage_relpath,
                    encrypted_size = EXCLUDED.encrypted_size,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    transform_version = EXCLUDED.transform_version,
                    encryption_json = EXCLUDED.encryption_json,
                    metadata_json = EXCLUDED.metadata_json,
                    shared_asset_uid = EXCLUDED.shared_asset_uid,
                    canonical_url_digest = EXCLUDED.canonical_url_digest,
                    updated_at = now()
                RETURNING asset_uid, shared_asset_uid, source_url, content_type, transformed_sha256,
                          storage_relpath, width, height, transform_version,
                          metadata_json, canonical_url_digest, updated_at;
                """,
                str(asset["asset_uid"]),
                clean_uid,
                mailbox_id,
                str(asset.get("source_url") or ""),
                str(asset.get("content_type") or "image/jpeg"),
                str(asset.get("raw_sha256") or ""),
                str(asset.get("transformed_sha256") or ""),
                str(asset.get("storage_relpath") or ""),
                int(asset.get("encrypted_size") or 0),
                int(asset.get("width") or 0),
                int(asset.get("height") or 0),
                str(asset.get("transform_version") or "jpeg-v1"),
                _json_dumps(asset.get("encryption") or {}, sort_keys=True, separators=(",", ":")),
                _json_dumps(asset.get("metadata") or {}, sort_keys=True, separators=(",", ":")),
                str(asset.get("shared_asset_uid") or ""),
                str(
                    asset.get("canonical_url_digest")
                    or _external_image_canonical_digest(str(asset.get("source_url") or ""))
                ),
            )
            shared_asset_uid = str(asset.get("shared_asset_uid") or "")
            if shared_asset_uid and update_shared_reference_count:
                await conn.execute(
                    """
                    UPDATE pim_email_shared_assets
                    SET reference_count = (
                            SELECT count(*)
                            FROM pim_email_transformed_assets
                            WHERE shared_asset_uid = $1
                        ),
                        updated_at = now()
                    WHERE shared_asset_uid = $1
                    """,
                    shared_asset_uid,
                )
        finally:
            await conn.close()
        return _asset_row_public(row)

    async def store_shared_asset(
        self,
        *,
        asset: dict[str, Any],
        ensure_schema: bool = True,
    ) -> dict[str, Any]:
        if ensure_schema:
            await self.ensure_schema()
        shared_uid = str(asset.get("shared_asset_uid") or "")
        if not shared_uid:
            raise EmailOperationError("Shared asset UID is missing")
        storage_relpath = str(asset.get("storage_relpath") or "")
        transformed_sha256 = str(asset.get("transformed_sha256") or "")
        if storage_relpath and transformed_sha256:
            stored = read_encrypted_bytes(storage_relpath, purpose=ASSET_PURPOSE)
            if hashlib.sha256(stored).hexdigest() != transformed_sha256:
                raise EmailOperationError("Shared external image asset hash verification failed")
        canonical = _canonical_remote_image_url(str(asset.get("source_url") or "")) or str(
            asset.get("source_url") or ""
        )
        canonical_digest = str(
            asset.get("canonical_url_digest") or _external_image_canonical_digest(canonical)
        )
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO pim_email_shared_assets (
                    shared_asset_uid, mailbox_id, canonical_url, canonical_url_digest,
                    content_type, raw_image_sha256, transformed_sha256, storage_relpath,
                    encrypted_size, width, height, transform_version, encryption_json,
                    metadata_json, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,now())
                ON CONFLICT (
                    mailbox_id, canonical_url_digest, transformed_sha256, transform_version
                ) DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    content_type = EXCLUDED.content_type,
                    raw_image_sha256 = EXCLUDED.raw_image_sha256,
                    storage_relpath = EXCLUDED.storage_relpath,
                    encrypted_size = EXCLUDED.encrypted_size,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    encryption_json = EXCLUDED.encryption_json,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = now()
                RETURNING shared_asset_uid, mailbox_id, canonical_url, canonical_url_digest,
                          content_type, raw_image_sha256, transformed_sha256, storage_relpath,
                          encrypted_size, width, height, transform_version, encryption_json,
                          metadata_json, reference_count, created_at, updated_at;
                """,
                shared_uid,
                str(asset.get("mailbox_id") or ""),
                canonical,
                canonical_digest,
                str(asset.get("content_type") or "image/jpeg"),
                str(asset.get("raw_image_sha256") or asset.get("raw_sha256") or ""),
                transformed_sha256,
                storage_relpath,
                int(asset.get("encrypted_size") or 0),
                int(asset.get("width") or 0),
                int(asset.get("height") or 0),
                str(asset.get("transform_version") or "jpeg-v1"),
                _json_dumps(asset.get("encryption") or {}, sort_keys=True, separators=(",", ":")),
                _json_dumps(asset.get("metadata") or {}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return _shared_asset_row_public(row)

    async def find_shared_asset_for_url(
        self,
        *,
        mailbox_id: str,
        source_url: str,
        ensure_schema: bool = True,
    ) -> dict[str, Any] | None:
        if ensure_schema:
            await self.ensure_schema()
        canonical = _canonical_remote_image_url(source_url) or str(source_url or "")
        if not canonical:
            return None
        canonical_digest = _external_image_canonical_digest(canonical)
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT shared_asset_uid, mailbox_id, canonical_url, canonical_url_digest,
                       content_type, raw_image_sha256, transformed_sha256, storage_relpath,
                       encrypted_size, width, height, transform_version, encryption_json,
                       metadata_json, reference_count, created_at, updated_at
                FROM pim_email_shared_assets
                WHERE mailbox_id = $1
                  AND canonical_url_digest = $2
                  AND transform_version = 'jpeg-v1'
                ORDER BY reference_count DESC, updated_at DESC
                LIMIT 3
                """,
                mailbox_id,
                canonical_digest,
            )
        finally:
            await conn.close()
        for row in rows:
            public = _shared_asset_row_public(row)
            try:
                stored = read_encrypted_bytes(public["storage_relpath"], purpose=ASSET_PURPOSE)
                if hashlib.sha256(stored).hexdigest() == public["transformed_sha256"]:
                    return public
            except Exception:
                continue
        return None

    async def find_external_image_canonical_terminal_state(
        self,
        *,
        mailbox_id: str,
        source_url: str,
        ensure_schema: bool = True,
    ) -> dict[str, Any] | None:
        if ensure_schema:
            await self.ensure_schema()
        canonical = _canonical_remote_image_url(source_url) or str(source_url or "")
        if not canonical:
            return None
        canonical_digest = _external_image_canonical_digest(canonical)
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT derivative_id, canonical_url, canonical_url_digest, status, reason,
                       safety_decision, transform_version, raw_image_sha256,
                       transformed_sha256, storage_relpath, shared_asset_uid,
                       encrypted_size, content_type, width, height, updated_at
                FROM pim_email_external_image_derivatives
                WHERE mailbox_id = $1
                  AND canonical_url_digest = $2
                  AND status IN ('blocked','unavailable')
                ORDER BY
                  CASE WHEN status = 'blocked' THEN 0 ELSE 1 END,
                  updated_at DESC
                LIMIT 20
                """,
                mailbox_id,
                canonical_digest,
            )
        finally:
            await conn.close()
        for row in rows:
            status = str(_row_get(row, "status", "") or "")
            reason = str(_row_get(row, "reason", "") or "")
            if not _external_image_existing_state_is_terminal(status, reason):
                continue
            return {
                "derivative_id": str(_row_get(row, "derivative_id", "")),
                "canonical_url": str(_row_get(row, "canonical_url", canonical) or canonical),
                "canonical_url_digest": str(
                    _row_get(row, "canonical_url_digest", canonical_digest) or canonical_digest
                ),
                "status": status,
                "reason": reason,
                "safety_decision": str(_row_get(row, "safety_decision", "") or ""),
                "transform_version": str(_row_get(row, "transform_version", "") or ""),
                "raw_image_sha256": str(_row_get(row, "raw_image_sha256", "") or ""),
                "transformed_sha256": str(_row_get(row, "transformed_sha256", "") or ""),
                "storage_relpath": str(_row_get(row, "storage_relpath", "") or ""),
                "shared_asset_uid": str(_row_get(row, "shared_asset_uid", "") or ""),
                "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
                "content_type": str(_row_get(row, "content_type", "") or ""),
                "width": int(_row_get(row, "width", 0) or 0),
                "height": int(_row_get(row, "height", 0) or 0),
                "updated_at": _iso_datetime(_row_get(row, "updated_at")),
            }
        return None

    async def _acquire_external_image_canonical_lock(
        self,
        *,
        mailbox_id: str,
        canonical_url: str,
    ) -> tuple[Any, int]:
        lock_key = _external_image_canonical_lock_key(mailbox_id, canonical_url)
        conn = await self._connect()
        try:
            await conn.execute("SELECT pg_advisory_lock($1::bigint)", lock_key)
        except Exception:
            await conn.close()
            raise
        return conn, lock_key

    async def _release_external_image_canonical_lock(self, conn: Any, lock_key: int) -> None:
        try:
            await conn.execute("SELECT pg_advisory_unlock($1::bigint)", lock_key)
        finally:
            await conn.close()

    async def link_external_image_references_from_shared_assets(
        self,
        *,
        mailbox_id: str | None = None,
        limit: int = 500,
        canonical_url_digest: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        safe_limit = max(1, min(int(limit or 1), 5000))
        clean_digest = str(canonical_url_digest or "").strip()
        link_metadata = dict(metadata or {})
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT d.derivative_id, d.email_uid, d.mailbox_id, d.input_raw_sha256,
                       d.source_url, d.canonical_url, d.canonical_url_digest,
                       s.shared_asset_uid, s.content_type, s.raw_image_sha256,
                       s.transformed_sha256, s.storage_relpath, s.encrypted_size,
                       s.width, s.height, s.transform_version, s.encryption_json,
                       s.metadata_json
                FROM pim_email_external_image_derivatives d
                JOIN LATERAL (
                    SELECT shared_asset_uid, content_type, raw_image_sha256,
                           transformed_sha256, storage_relpath, encrypted_size,
                           width, height, transform_version, encryption_json,
                           metadata_json
                    FROM pim_email_shared_assets s
                    WHERE s.mailbox_id = d.mailbox_id
                      AND s.canonical_url_digest = d.canonical_url_digest
                      AND s.transform_version = 'jpeg-v1'
                    ORDER BY s.reference_count DESC, s.updated_at DESC
                    LIMIT 1
                ) s ON true
                WHERE d.mailbox_id = $1
                  AND d.canonical_url_digest <> ''
                  AND ($3::text = '' OR d.canonical_url_digest = $3)
                  AND d.status IN ('pending','fetched','transformed','failed')
                ORDER BY d.updated_at DESC
                LIMIT $2
                """,
                configured_mailbox_id,
                safe_limit,
                clean_digest,
            )
        finally:
            await conn.close()

        summary = {
            "schema": "xarta.pim_email.external_image_shared_asset_link.summary.v1",
            "mailbox_id": configured_mailbox_id,
            "canonical_url_digest": clean_digest,
            "planned": len(rows),
            "linked": 0,
            "failed": 0,
            "failures": [],
        }
        touched_shared_uids: set[str] = set()
        for row in rows:
            source_url = str(_row_get(row, "source_url", "") or "")
            canonical = str(_row_get(row, "canonical_url", "") or source_url)
            shared = {
                "shared_asset_uid": str(_row_get(row, "shared_asset_uid", "") or ""),
                "mailbox_id": configured_mailbox_id,
                "source_url": canonical,
                "canonical_url": canonical,
                "canonical_url_digest": str(_row_get(row, "canonical_url_digest", "") or ""),
                "content_type": str(_row_get(row, "content_type", "image/jpeg") or "image/jpeg"),
                "raw_image_sha256": str(_row_get(row, "raw_image_sha256", "") or ""),
                "transformed_sha256": str(_row_get(row, "transformed_sha256", "") or ""),
                "storage_relpath": str(_row_get(row, "storage_relpath", "") or ""),
                "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
                "width": int(_row_get(row, "width", 0) or 0),
                "height": int(_row_get(row, "height", 0) or 0),
                "transform_version": str(
                    _row_get(row, "transform_version", "jpeg-v1") or "jpeg-v1"
                ),
                "encryption": _json_loads_obj(_row_get(row, "encryption_json", {})),
                "metadata": _json_loads_obj(_row_get(row, "metadata_json", {})),
            }
            try:
                stored = read_encrypted_bytes(shared["storage_relpath"], purpose=ASSET_PURPOSE)
                if hashlib.sha256(stored).hexdigest() != shared["transformed_sha256"]:
                    raise EmailOperationError("Shared external image asset hash mismatch")
                reference = _asset_reference_for_email(
                    mailbox_id=configured_mailbox_id,
                    email_uid=str(_row_get(row, "email_uid", "")),
                    input_raw_sha256=str(_row_get(row, "input_raw_sha256", "")),
                    canonical_url=canonical,
                    shared_asset=shared,
                    metadata={
                        "schema": "xarta.pim_email.external_image_shared_asset_link.v1",
                        "source_url": source_url,
                        "canonical_url": canonical,
                        "bulk_linked_from_shared_asset": True,
                        **link_metadata,
                    },
                )
                await self.store_transformed_asset(
                    mailbox_id=configured_mailbox_id,
                    email_uid=str(_row_get(row, "email_uid", "")),
                    asset=reference,
                    ensure_schema=False,
                    update_shared_reference_count=False,
                )
                await self.record_external_image_derivative_state(
                    mailbox_id=configured_mailbox_id,
                    email_uid=str(_row_get(row, "email_uid", "")),
                    input_raw_sha256=str(_row_get(row, "input_raw_sha256", "")),
                    source_url=source_url,
                    status="stored",
                    reason="",
                    safety_decision="reused_verified_shared_encrypted_asset_bulk_link",
                    transform_version=shared["transform_version"],
                    raw_image_sha256=shared["raw_image_sha256"],
                    transformed_sha256=shared["transformed_sha256"],
                    storage_relpath=shared["storage_relpath"],
                    shared_asset_uid=shared["shared_asset_uid"],
                    encrypted_size=shared["encrypted_size"],
                    content_type=shared["content_type"],
                    width=shared["width"],
                    height=shared["height"],
                    metadata={
                        "schema": "xarta.pim_email.external_image_derivative.metadata.v1",
                        "completion_kind": "verified-shared-asset-bulk-linked",
                        "source_url": source_url,
                        "canonical_url": canonical,
                        "shared_asset_uid": shared["shared_asset_uid"],
                        **link_metadata,
                    },
                    ensure_schema=False,
                )
                summary["linked"] += 1
                touched_shared_uids.add(shared["shared_asset_uid"])
            except Exception as exc:
                summary["failed"] += 1
                if len(summary["failures"]) < 20:
                    summary["failures"].append(
                        {
                            "derivative_id": str(_row_get(row, "derivative_id", "")),
                            "source_url": source_url,
                            "error_class": exc.__class__.__name__,
                            "error_message": str(exc)[:300],
                        }
                    )
        if touched_shared_uids:
            conn = await self._connect()
            try:
                await conn.execute(
                    """
                    UPDATE pim_email_shared_assets s
                    SET reference_count = counts.reference_count,
                        updated_at = now()
                    FROM (
                        SELECT shared_asset_uid, count(*)::integer AS reference_count
                        FROM pim_email_transformed_assets
                        WHERE shared_asset_uid = ANY($1::text[])
                        GROUP BY shared_asset_uid
                    ) counts
                    WHERE s.shared_asset_uid = counts.shared_asset_uid
                    """,
                    sorted(touched_shared_uids),
                )
            finally:
                await conn.close()
        return summary

    async def record_external_image_canonical_terminal_state(
        self,
        *,
        mailbox_id: str,
        canonical_url_digest: str,
        status: str,
        reason: str,
        safety_decision: str,
        transform_version: str = "",
        metadata: dict[str, Any] | None = None,
        ensure_schema: bool = True,
    ) -> int:
        if ensure_schema:
            await self.ensure_schema()
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"blocked", "unavailable"}:
            raise EmailOperationError("Canonical external image outcome must be terminal")
        clean_digest = str(canonical_url_digest or "").strip()
        if not clean_digest:
            return 0
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                UPDATE pim_email_external_image_derivatives
                SET status = $3,
                    reason = $4,
                    safety_decision = $5,
                    transform_version = $6,
                    metadata_json = metadata_json || jsonb_build_object(
                        'canonical_terminal_outcome', $7::jsonb
                    ),
                    updated_at = now()
                WHERE mailbox_id = $1
                  AND canonical_url_digest = $2
                  AND status IN ('pending','fetched','transformed','failed')
                RETURNING derivative_id
                """,
                mailbox_id,
                clean_digest,
                clean_status,
                str(reason or "")[:1000],
                str(safety_decision or ""),
                str(transform_version or ""),
                _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return len(rows)

    async def process_external_image_unique_canonical_assets(
        self,
        *,
        mailbox_id: str | None = None,
        limit: int = 50,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        safe_limit = max(1, min(int(limit or 1), 500))
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                WITH shared_digest AS (
                    SELECT DISTINCT mailbox_id, canonical_url_digest
                    FROM pim_email_shared_assets
                    WHERE mailbox_id = $1 AND canonical_url_digest <> ''
                )
                SELECT DISTINCT ON (d.canonical_url_digest)
                       d.canonical_url_digest,
                       d.canonical_url,
                       d.source_url,
                       d.email_uid,
                       d.input_raw_sha256,
                       d.updated_at
                FROM pim_email_external_image_derivatives d
                LEFT JOIN shared_digest sd
                  ON sd.mailbox_id = d.mailbox_id
                 AND sd.canonical_url_digest = d.canonical_url_digest
                WHERE d.mailbox_id = $1
                  AND d.canonical_url_digest <> ''
                  AND d.status IN ('pending','fetched','transformed','failed')
                  AND sd.canonical_url_digest IS NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pim_email_external_image_derivatives waiting
                      WHERE waiting.mailbox_id = d.mailbox_id
                        AND waiting.canonical_url_digest = d.canonical_url_digest
                        AND waiting.status = 'pending'
                        AND COALESCE(waiting.reason, '') <> 'captured_waiting_for_real_download'
                        AND waiting.next_retry_at IS NOT NULL
                        AND waiting.next_retry_at > now()
                  )
                ORDER BY d.canonical_url_digest, d.email_uid DESC, d.updated_at DESC
                LIMIT $2
                """,
                configured_mailbox_id,
                safe_limit,
            )
        finally:
            await conn.close()

        summary: dict[str, Any] = {
            "schema": "xarta.pim_email.external_image_unique_asset.summary.v1",
            "mailbox_id": configured_mailbox_id,
            "planned_unique_urls": len(rows),
            "attempted_unique_urls": 0,
            "stored_unique_urls": 0,
            "already_stored_unique_urls": 0,
            "unavailable_unique_urls": 0,
            "blocked_unique_urls": 0,
            "pending_retryable_unique_urls": 0,
            "failed_unique_urls": 0,
            "references_linked": 0,
            "references_link_failed": 0,
            "references_terminal": 0,
            "retryable": [],
            "failures": [],
        }

        async def reuse_existing_canonical_work(canonical: str) -> dict[str, Any] | None:
            shared = await self.find_shared_asset_for_url(
                mailbox_id=configured_mailbox_id,
                source_url=canonical,
                ensure_schema=False,
            )
            if shared:
                return {"status": "stored", "shared": shared}
            outcome = await self.find_external_image_canonical_terminal_state(
                mailbox_id=configured_mailbox_id,
                source_url=canonical,
                ensure_schema=False,
            )
            if outcome:
                return {"status": str(outcome.get("status") or ""), "outcome": outcome}
            return None

        for row in rows:
            canonical = str(_row_get(row, "canonical_url", "") or _row_get(row, "source_url", ""))
            canonical = _canonical_remote_image_url(canonical) or canonical
            source = str(_row_get(row, "source_url", "") or canonical)
            canonical_digest = str(_row_get(row, "canonical_url_digest", "") or "")
            clean_uid = clean_email_uid(str(_row_get(row, "email_uid", "")))
            raw_sha256 = str(_row_get(row, "input_raw_sha256", "") or "")
            if not canonical or not canonical_digest:
                continue

            lock_conn, lock_key = await self._acquire_external_image_canonical_lock(
                mailbox_id=configured_mailbox_id,
                canonical_url=canonical,
            )
            try:
                reused = await reuse_existing_canonical_work(canonical)
                if reused:
                    status = str(reused.get("status") or "")
                    if status == "stored":
                        summary["already_stored_unique_urls"] += 1
                        link_result = await self.link_external_image_references_from_shared_assets(
                            mailbox_id=configured_mailbox_id,
                            canonical_url_digest=canonical_digest,
                            limit=5000,
                            metadata={
                                **(metadata or {}),
                                "phase": "external-image-unique-asset-reuse-link",
                                "canonical_url": canonical,
                            },
                        )
                        summary["references_linked"] += int(link_result.get("linked") or 0)
                        summary["references_link_failed"] += int(link_result.get("failed") or 0)
                    elif status in {"blocked", "unavailable"}:
                        terminal = int(
                            await self.record_external_image_canonical_terminal_state(
                                mailbox_id=configured_mailbox_id,
                                canonical_url_digest=canonical_digest,
                                status=status,
                                reason=str((reused.get("outcome") or {}).get("reason") or ""),
                                safety_decision=f"reused_canonical_{status}_external_image_outcome",
                                transform_version=str(
                                    (reused.get("outcome") or {}).get("transform_version") or ""
                                ),
                                metadata={
                                    **(metadata or {}),
                                    "schema": "xarta.pim_email.external_image_unique_asset.reuse.v1",
                                    "canonical_url": canonical,
                                },
                                ensure_schema=False,
                            )
                        )
                        summary[f"{status}_unique_urls"] += 1
                        summary["references_terminal"] += terminal
                    continue

                summary["attempted_unique_urls"] += 1
                fetched = await fetch_remote_image_bytes(source)
                raw_content = bytes(fetched.get("content") or b"")
                asset = build_transformed_external_image_asset(
                    mailbox_id=configured_mailbox_id,
                    email_uid=clean_uid,
                    source_url=canonical,
                    content=raw_content,
                    metadata={
                        **(metadata or {}),
                        "input_raw_sha256": raw_sha256,
                        "source_url": source,
                        "canonical_url": canonical,
                        "fetched_content_type": str(fetched.get("content_type") or ""),
                        "fetched_final_url": str(fetched.get("final_url") or ""),
                        "unique_canonical_asset_fetch": True,
                    },
                )
                await self.store_shared_asset(asset=asset, ensure_schema=False)
                summary["stored_unique_urls"] += 1
                link_result = await self.link_external_image_references_from_shared_assets(
                    mailbox_id=configured_mailbox_id,
                    canonical_url_digest=canonical_digest,
                    limit=5000,
                    metadata={
                        **(metadata or {}),
                        "phase": "external-image-unique-asset-store-link",
                        "canonical_url": canonical,
                        "shared_asset_uid": str(asset.get("shared_asset_uid") or ""),
                    },
                )
                summary["references_linked"] += int(link_result.get("linked") or 0)
                summary["references_link_failed"] += int(link_result.get("failed") or 0)
            except Exception as exc:
                state = _external_image_error_status(exc)
                if state in {"blocked", "unavailable"}:
                    terminal_count = await self.record_external_image_canonical_terminal_state(
                        mailbox_id=configured_mailbox_id,
                        canonical_url_digest=canonical_digest,
                        status=state,
                        reason=str(exc),
                        safety_decision=f"{state}_during_unique_external_image_download_or_transform",
                        transform_version=EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                        metadata={
                            **(metadata or {}),
                            "schema": "xarta.pim_email.external_image_unique_asset.terminal.v1",
                            "canonical_url": canonical,
                            "source_url": source,
                            "error_class": exc.__class__.__name__,
                        },
                        ensure_schema=False,
                    )
                    summary[f"{state}_unique_urls"] += 1
                    summary["references_terminal"] += terminal_count
                elif state == "pending":
                    await self.record_external_image_derivative_state(
                        mailbox_id=configured_mailbox_id,
                        email_uid=clean_uid,
                        input_raw_sha256=raw_sha256,
                        source_url=source,
                        status=state,
                        reason=str(exc),
                        safety_decision=f"{state}_during_unique_external_image_download_or_transform",
                        transform_version=EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                        metadata={
                            **(metadata or {}),
                            "schema": "xarta.pim_email.external_image_unique_asset.failure.v1",
                            "canonical_url": canonical,
                            "source_url": source,
                            "error_class": exc.__class__.__name__,
                        },
                        ensure_schema=False,
                    )
                    summary["pending_retryable_unique_urls"] += 1
                    if len(summary["retryable"]) < 20:
                        summary["retryable"].append(
                            {
                                "canonical_url_digest": canonical_digest,
                                "canonical_url": canonical,
                                "error_class": exc.__class__.__name__,
                                "error_message": str(exc)[:300],
                            }
                        )
                else:
                    await self.record_external_image_derivative_state(
                        mailbox_id=configured_mailbox_id,
                        email_uid=clean_uid,
                        input_raw_sha256=raw_sha256,
                        source_url=source,
                        status=state,
                        reason=str(exc),
                        safety_decision=f"{state}_during_unique_external_image_download_or_transform",
                        transform_version=EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                        metadata={
                            **(metadata or {}),
                            "schema": "xarta.pim_email.external_image_unique_asset.failure.v1",
                            "canonical_url": canonical,
                            "source_url": source,
                            "error_class": exc.__class__.__name__,
                        },
                        ensure_schema=False,
                    )
                    summary["failed_unique_urls"] += 1
                    if len(summary["failures"]) < 20:
                        summary["failures"].append(
                            {
                                "canonical_url_digest": canonical_digest,
                                "canonical_url": canonical,
                                "error_class": exc.__class__.__name__,
                                "error_message": str(exc)[:300],
                            }
                        )
            finally:
                await self._release_external_image_canonical_lock(lock_conn, lock_key)
        return summary

    async def migrate_existing_transformed_assets_to_shared_store(
        self,
        *,
        mailbox_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        safe_limit = max(1, min(int(limit or 1), 5000))
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT asset_uid, email_uid, mailbox_id, source_url, content_type,
                       raw_sha256, transformed_sha256, storage_relpath, encrypted_size,
                       width, height, transform_version, encryption_json, metadata_json,
                       shared_asset_uid, canonical_url_digest
                FROM pim_email_transformed_assets
                WHERE mailbox_id = $1
                  AND source_url <> ''
                  AND transformed_sha256 <> ''
                  AND storage_relpath <> ''
                  AND (
                    shared_asset_uid = ''
                    OR canonical_url_digest = ''
                    OR storage_relpath NOT LIKE 'assets/%'
                  )
                ORDER BY updated_at DESC
                LIMIT $2
                """,
                configured_mailbox_id,
                safe_limit,
            )
        finally:
            await conn.close()
        summary = {
            "schema": "xarta.pim_email.shared_asset_migration.summary.v1",
            "mailbox_id": configured_mailbox_id,
            "planned": len(rows),
            "copied": 0,
            "updated_asset_refs": 0,
            "updated_derivative_refs": 0,
            "failed": 0,
            "failures": [],
        }
        for row in rows:
            asset_uid = str(_row_get(row, "asset_uid", ""))
            old_relpath = str(_row_get(row, "storage_relpath", ""))
            transformed_sha256 = str(_row_get(row, "transformed_sha256", ""))
            canonical = _canonical_remote_image_url(str(_row_get(row, "source_url", ""))) or str(
                _row_get(row, "source_url", "")
            )
            try:
                content = read_encrypted_bytes(old_relpath, purpose=ASSET_PURPOSE)
                if hashlib.sha256(content).hexdigest() != transformed_sha256:
                    raise EmailOperationError("Existing transformed asset hash mismatch")
                canonical_digest = _external_image_canonical_digest(canonical)
                shared_uid = _stable_id(
                    "email-shared-asset",
                    configured_mailbox_id,
                    canonical_digest,
                    transformed_sha256,
                    str(_row_get(row, "transform_version", "jpeg-v1") or "jpeg-v1"),
                )
                storage = write_encrypted_bytes_atomic(
                    relpath=_shared_email_asset_relpath(canonical_digest, shared_uid),
                    content=content,
                    purpose=ASSET_PURPOSE,
                )
                if (
                    hashlib.sha256(
                        read_encrypted_bytes(storage["storage_relpath"], purpose=ASSET_PURPOSE)
                    ).hexdigest()
                    != transformed_sha256
                ):
                    raise EmailOperationError("Shared transformed asset copy verification failed")
                shared = await self.store_shared_asset(
                    asset={
                        "shared_asset_uid": shared_uid,
                        "mailbox_id": configured_mailbox_id,
                        "source_url": canonical,
                        "canonical_url_digest": canonical_digest,
                        "content_type": str(_row_get(row, "content_type", "image/jpeg")),
                        "raw_image_sha256": str(_row_get(row, "raw_sha256", "")),
                        "transformed_sha256": transformed_sha256,
                        "storage_relpath": storage["storage_relpath"],
                        "encrypted_size": int(storage.get("encrypted_size") or 0),
                        "width": int(_row_get(row, "width", 0) or 0),
                        "height": int(_row_get(row, "height", 0) or 0),
                        "transform_version": str(
                            _row_get(row, "transform_version", "jpeg-v1") or "jpeg-v1"
                        ),
                        "encryption": storage.get("encryption") or {},
                        "metadata": {
                            "schema": "xarta.pim_email.shared_asset.migration.v1",
                            "source_asset_uid": asset_uid,
                            "old_storage_relpath": old_relpath,
                            "copy_before_reference_update": True,
                        },
                    },
                    ensure_schema=False,
                )
                migration = {
                    "schema": "xarta.pim_email.shared_asset_reference.migration.v1",
                    "shared_asset_uid": shared["shared_asset_uid"],
                    "old_storage_relpath": old_relpath,
                    "new_storage_relpath": shared["storage_relpath"],
                    "copied_at": _now_iso(),
                }
                conn = await self._connect()
                try:
                    async with conn.transaction():
                        await conn.execute(
                            """
                            UPDATE pim_email_transformed_assets
                            SET shared_asset_uid = $2,
                                canonical_url_digest = $3,
                                storage_relpath = $4,
                                encrypted_size = $5,
                                metadata_json = metadata_json || jsonb_build_object(
                                    'shared_asset_migration', $6::jsonb
                                ),
                                updated_at = now()
                            WHERE asset_uid = $1
                            """,
                            asset_uid,
                            shared["shared_asset_uid"],
                            canonical_digest,
                            shared["storage_relpath"],
                            int(shared.get("encrypted_size") or 0),
                            _json_dumps(migration, sort_keys=True, separators=(",", ":")),
                        )
                        derivative_rows = await conn.fetch(
                            """
                            UPDATE pim_email_external_image_derivatives
                            SET shared_asset_uid = $1,
                                canonical_url_digest = CASE
                                    WHEN canonical_url_digest = '' THEN $2
                                    ELSE canonical_url_digest
                                END,
                                storage_relpath = $3,
                                encrypted_size = $4,
                                metadata_json = metadata_json || jsonb_build_object(
                                    'shared_asset_migration', $5::jsonb
                                ),
                                updated_at = now()
                            WHERE mailbox_id = $6
                              AND email_uid = $7
                              AND transformed_sha256 = $8
                              AND (storage_relpath = $9 OR canonical_url_digest = $2)
                            RETURNING derivative_id
                            """,
                            shared["shared_asset_uid"],
                            canonical_digest,
                            shared["storage_relpath"],
                            int(shared.get("encrypted_size") or 0),
                            _json_dumps(migration, sort_keys=True, separators=(",", ":")),
                            configured_mailbox_id,
                            str(_row_get(row, "email_uid", "")),
                            transformed_sha256,
                            old_relpath,
                        )
                        await conn.execute(
                            """
                            UPDATE pim_email_shared_assets
                            SET reference_count = (
                                    SELECT count(*)
                                    FROM pim_email_transformed_assets
                                    WHERE shared_asset_uid = $1
                                ),
                                updated_at = now()
                            WHERE shared_asset_uid = $1
                            """,
                            shared["shared_asset_uid"],
                        )
                finally:
                    await conn.close()
                summary["copied"] += 1
                summary["updated_asset_refs"] += 1
                summary["updated_derivative_refs"] += len(derivative_rows)
            except Exception as exc:
                summary["failed"] += 1
                if len(summary["failures"]) < 20:
                    summary["failures"].append(
                        {
                            "asset_uid": asset_uid,
                            "storage_relpath": old_relpath,
                            "error_class": exc.__class__.__name__,
                            "error_message": str(exc)[:300],
                        }
                    )
        return summary

    async def store_sanitized_view_artifact(
        self,
        *,
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        await self.ensure_schema()
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO pim_email_sanitized_view_artifacts (
                    artifact_uid, email_uid, mailbox_id, input_raw_sha256,
                    sanitizer_policy_version, transform_version, output_sha256,
                    storage_relpath, encrypted_size, views_available_json,
                    safety_counts_json, derivation_json, encryption_json, updated_at
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb,now())
                ON CONFLICT (
                    mailbox_id, email_uid, input_raw_sha256,
                    sanitizer_policy_version, transform_version
                ) DO UPDATE SET
                    output_sha256 = EXCLUDED.output_sha256,
                    storage_relpath = EXCLUDED.storage_relpath,
                    encrypted_size = EXCLUDED.encrypted_size,
                    views_available_json = EXCLUDED.views_available_json,
                    safety_counts_json = EXCLUDED.safety_counts_json,
                    derivation_json = EXCLUDED.derivation_json,
                    encryption_json = EXCLUDED.encryption_json,
                    updated_at = now()
                RETURNING artifact_uid, email_uid, input_raw_sha256, sanitizer_policy_version,
                          transform_version, output_sha256, storage_relpath, encrypted_size,
                          views_available_json, safety_counts_json, derivation_json,
                          generated_at, updated_at;
                """,
                str(artifact["artifact_uid"]),
                clean_email_uid(str(artifact["email_uid"])),
                str(artifact["mailbox_id"]),
                str(artifact["input_raw_sha256"]),
                str(artifact["sanitizer_policy_version"]),
                str(artifact["transform_version"]),
                str(artifact["output_sha256"]),
                str(artifact["storage_relpath"]),
                int(artifact.get("encrypted_size") or 0),
                _json_dumps(
                    artifact.get("views_available") or {}, sort_keys=True, separators=(",", ":")
                ),
                _json_dumps(
                    artifact.get("safety_counts") or {}, sort_keys=True, separators=(",", ":")
                ),
                _json_dumps(
                    artifact.get("derivation") or {}, sort_keys=True, separators=(",", ":")
                ),
                _json_dumps(
                    artifact.get("encryption") or {}, sort_keys=True, separators=(",", ":")
                ),
            )
        finally:
            await conn.close()
        return _sanitized_artifact_row_public(row)

    async def record_external_image_derivative_state(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        input_raw_sha256: str,
        source_url: str,
        status: str,
        reason: str = "",
        safety_decision: str = "",
        transform_version: str = "",
        raw_image_sha256: str = "",
        transformed_sha256: str = "",
        storage_relpath: str = "",
        shared_asset_uid: str = "",
        encrypted_size: int = 0,
        content_type: str = "",
        width: int = 0,
        height: int = 0,
        metadata: dict[str, Any] | None = None,
        ensure_schema: bool = True,
    ) -> dict[str, Any]:
        if ensure_schema:
            await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        canonical = _canonical_remote_image_url(source_url) or str(source_url or "")
        canonical_digest = _external_image_canonical_digest(canonical)
        clean_status = str(status or "").strip().lower()
        if clean_status not in {
            "pending",
            "fetched",
            "transformed",
            "stored",
            "blocked",
            "failed",
            "unavailable",
        }:
            raise EmailOperationError("Invalid external image derivative status")
        derivative_id = _stable_id(
            "email-image-derivative",
            mailbox_id,
            clean_uid,
            input_raw_sha256,
            canonical,
        )
        clean_reason = str(reason or "")[:1000]
        retryable_pending = (
            clean_status == "pending"
            and bool(clean_reason)
            and clean_reason != "captured_waiting_for_real_download"
        )
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                INSERT INTO pim_email_external_image_derivatives (
                    derivative_id, email_uid, mailbox_id, input_raw_sha256,
                    source_url, canonical_url, canonical_url_digest, status, reason, safety_decision,
                    transform_version, raw_image_sha256, transformed_sha256,
                    storage_relpath, shared_asset_uid, encrypted_size, content_type, width, height,
                    retry_count, last_error, next_retry_at, metadata_json,
                    fetched_at, transformed_at, stored_at, updated_at
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                    CASE WHEN $21::boolean THEN 1 ELSE 0 END,
                    CASE WHEN $21::boolean THEN $9 ELSE '' END,
                    CASE WHEN $21::boolean THEN now() + ($22::integer * interval '1 second') ELSE NULL END,
                    $20::jsonb,
                    CASE WHEN $8 IN ('fetched','transformed','stored') THEN now() ELSE NULL END,
                    CASE WHEN $8 IN ('transformed','stored') THEN now() ELSE NULL END,
                    CASE WHEN $8 = 'stored' THEN now() ELSE NULL END,
                    now()
                )
                ON CONFLICT (mailbox_id, email_uid, input_raw_sha256, canonical_url_digest) DO UPDATE SET
                    status = EXCLUDED.status,
                    reason = EXCLUDED.reason,
                    safety_decision = EXCLUDED.safety_decision,
                    transform_version = EXCLUDED.transform_version,
                    raw_image_sha256 = EXCLUDED.raw_image_sha256,
                    transformed_sha256 = EXCLUDED.transformed_sha256,
                    storage_relpath = EXCLUDED.storage_relpath,
                    shared_asset_uid = EXCLUDED.shared_asset_uid,
                    encrypted_size = EXCLUDED.encrypted_size,
                    content_type = EXCLUDED.content_type,
                    width = EXCLUDED.width,
                    height = EXCLUDED.height,
                    retry_count = CASE
                        WHEN $21::boolean THEN pim_email_external_image_derivatives.retry_count + 1
                        ELSE pim_email_external_image_derivatives.retry_count
                    END,
                    last_error = CASE
                        WHEN $21::boolean THEN EXCLUDED.last_error
                        WHEN EXCLUDED.status IN ('stored','blocked','unavailable') THEN ''
                        ELSE pim_email_external_image_derivatives.last_error
                    END,
                    next_retry_at = CASE
                        WHEN $21::boolean THEN now() + ($22::integer * interval '1 second')
                        WHEN EXCLUDED.status IN ('stored','blocked','unavailable') THEN NULL
                        ELSE pim_email_external_image_derivatives.next_retry_at
                    END,
                    metadata_json = EXCLUDED.metadata_json,
                    fetched_at = COALESCE(EXCLUDED.fetched_at, pim_email_external_image_derivatives.fetched_at),
                    transformed_at = COALESCE(EXCLUDED.transformed_at, pim_email_external_image_derivatives.transformed_at),
                    stored_at = COALESCE(EXCLUDED.stored_at, pim_email_external_image_derivatives.stored_at),
                    updated_at = now()
                RETURNING derivative_id, email_uid, input_raw_sha256, source_url,
                          canonical_url, status, reason, safety_decision,
                          transform_version, raw_image_sha256, transformed_sha256,
                          storage_relpath, shared_asset_uid, encrypted_size, content_type, width,
                          height, retry_count, last_error, next_retry_at, created_at, updated_at;
                """,
                derivative_id,
                clean_uid,
                mailbox_id,
                str(input_raw_sha256 or ""),
                str(source_url or ""),
                canonical,
                canonical_digest,
                clean_status,
                clean_reason,
                str(safety_decision or ""),
                str(transform_version or ""),
                str(raw_image_sha256 or ""),
                str(transformed_sha256 or ""),
                str(storage_relpath or ""),
                str(shared_asset_uid or ""),
                int(encrypted_size or 0),
                str(content_type or ""),
                int(width or 0),
                int(height or 0),
                _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
                retryable_pending,
                EXTERNAL_IMAGE_RETRY_DELAY_SECONDS,
            )
        finally:
            await conn.close()
        return {
            "derivative_id": str(_row_get(row, "derivative_id", "")),
            "email_uid": str(_row_get(row, "email_uid", "")),
            "input_raw_sha256": str(_row_get(row, "input_raw_sha256", "")),
            "source_url": str(_row_get(row, "source_url", "")),
            "canonical_url": str(_row_get(row, "canonical_url", "")),
            "status": str(_row_get(row, "status", "")),
            "reason": str(_row_get(row, "reason", "")),
            "safety_decision": str(_row_get(row, "safety_decision", "")),
            "transform_version": str(_row_get(row, "transform_version", "")),
            "raw_image_sha256": str(_row_get(row, "raw_image_sha256", "")),
            "transformed_sha256": str(_row_get(row, "transformed_sha256", "")),
            "storage_relpath": str(_row_get(row, "storage_relpath", "")),
            "shared_asset_uid": str(_row_get(row, "shared_asset_uid", "")),
            "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
            "content_type": str(_row_get(row, "content_type", "")),
            "width": int(_row_get(row, "width", 0) or 0),
            "height": int(_row_get(row, "height", 0) or 0),
            "retry_count": int(_row_get(row, "retry_count", 0) or 0),
            "last_error": str(_row_get(row, "last_error", "")),
            "next_retry_at": _iso_datetime(_row_get(row, "next_retry_at")),
            "updated_at": _iso_datetime(_row_get(row, "updated_at")),
        }

    async def materialize_external_image_derivative_rows(
        self,
        *,
        mailbox_id: str | None = None,
        email_uid: str | None = None,
        limit: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_email_filter = clean_email_uid(email_uid) if email_uid else ""
        safe_limit = None if limit is None else max(1, int(limit))
        params: list[Any] = [configured_mailbox_id]
        where = "WHERE mailbox_id = $1"
        if clean_email_filter:
            params.append(clean_email_filter)
            where += f" AND email_uid = ${len(params)}"
        limit_clause = ""
        if safe_limit is not None:
            params.append(safe_limit)
            limit_clause = f"LIMIT ${len(params)}"
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                f"""
                WITH selected_messages AS (
                    SELECT email_uid, mailbox_id, raw_sha256, metadata_json
                    FROM pim_email_messages
                    {where}
                      AND jsonb_typeof(metadata_json->'remote_image_sources') = 'array'
                      AND jsonb_array_length(metadata_json->'remote_image_sources') > 0
                      AND (
                          SELECT count(DISTINCT d.canonical_url_digest)
                          FROM pim_email_external_image_derivatives d
                          WHERE d.mailbox_id = pim_email_messages.mailbox_id
                            AND d.email_uid = pim_email_messages.email_uid
                            AND d.input_raw_sha256 = pim_email_messages.raw_sha256
                            AND d.canonical_url_digest <> ''
                      ) < jsonb_array_length(metadata_json->'remote_image_sources')
                    ORDER BY email_uid DESC, updated_at DESC
                    {limit_clause}
                )
                SELECT email_uid, mailbox_id, raw_sha256, source.value #>> '{{}}' AS source_url
                FROM selected_messages,
                     LATERAL jsonb_array_elements(
                       COALESCE(metadata_json->'remote_image_sources', '[]'::jsonb)
                     ) AS source(value)
                """,
                *params,
            )
        finally:
            await conn.close()

        records: list[tuple[Any, ...]] = []
        seen: set[tuple[str, str, str, str]] = set()
        for row in rows:
            clean_uid = clean_email_uid(str(row["email_uid"] or ""))
            raw_sha256 = str(row["raw_sha256"] or "")
            source_url = str(row["source_url"] or "")
            canonical = _canonical_remote_image_url(source_url) or source_url
            if not canonical:
                continue
            key = (configured_mailbox_id, clean_uid, raw_sha256, canonical)
            if key in seen:
                continue
            seen.add(key)
            records.append(
                (
                    _stable_id(
                        "email-image-derivative",
                        configured_mailbox_id,
                        clean_uid,
                        raw_sha256,
                        canonical,
                    ),
                    clean_uid,
                    configured_mailbox_id,
                    raw_sha256,
                    source_url,
                    canonical,
                    _external_image_canonical_digest(canonical),
                    "pending",
                    "captured_waiting_for_real_download",
                    "pending_real_download",
                    EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                    _json_dumps(
                        {
                            **(metadata or {}),
                            "schema": "xarta.pim_email.external_image_derivative.metadata.v1",
                            "completion_kind": "pending_real_download",
                            "source_url": source_url,
                            "canonical_url": canonical,
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            )
        if not records:
            return {
                "captured_sources": len(rows),
                "candidate_rows": 0,
                "materialized_rows": 0,
            }

        inserted = 0
        conn = await self._connect()
        try:
            for offset in range(0, len(records), 1000):
                chunk = records[offset : offset + 1000]
                result = await conn.fetch(
                    """
                    INSERT INTO pim_email_external_image_derivatives (
                        derivative_id, email_uid, mailbox_id, input_raw_sha256,
                        source_url, canonical_url, canonical_url_digest, status, reason, safety_decision,
                        transform_version, metadata_json
                    )
                    SELECT *
                    FROM unnest(
                        $1::text[], $2::text[], $3::text[], $4::text[],
                        $5::text[], $6::text[], $7::text[], $8::text[],
                        $9::text[], $10::text[], $11::text[], $12::jsonb[]
                    ) AS input(
                        derivative_id, email_uid, mailbox_id, input_raw_sha256,
                        source_url, canonical_url, canonical_url_digest, status, reason, safety_decision,
                        transform_version, metadata_json
                    )
                    ON CONFLICT (mailbox_id, email_uid, input_raw_sha256, canonical_url_digest)
                    DO NOTHING
                    RETURNING derivative_id
                    """,
                    [item[0] for item in chunk],
                    [item[1] for item in chunk],
                    [item[2] for item in chunk],
                    [item[3] for item in chunk],
                    [item[4] for item in chunk],
                    [item[5] for item in chunk],
                    [item[6] for item in chunk],
                    [item[7] for item in chunk],
                    [item[8] for item in chunk],
                    [item[9] for item in chunk],
                    [item[10] for item in chunk],
                    [item[11] for item in chunk],
                )
                inserted += len(result)
        finally:
            await conn.close()
        return {
            "captured_sources": len(rows),
            "candidate_rows": len(records),
            "materialized_rows": inserted,
        }

    async def process_external_image_derivatives(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        input_raw_sha256: str,
        source_urls: list[str],
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        unique_sources: list[tuple[str, str]] = []
        seen: set[str] = set()
        for source in source_urls or []:
            canonical = _canonical_remote_image_url(source) or str(source or "")
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            unique_sources.append((str(source or ""), canonical))
        counts = {
            "stored": 0,
            "blocked": 0,
            "failed": 0,
            "unavailable": 0,
            "pending": 0,
            "attempted": 0,
            "already_stored": 0,
            "already_blocked": 0,
            "already_unavailable": 0,
        }
        if not unique_sources:
            return counts
        conn = await self._connect()
        try:
            existing_rows = await conn.fetch(
                """
                SELECT canonical_url, status, reason
                FROM pim_email_external_image_derivatives
                WHERE mailbox_id = $1
                  AND email_uid = $2
                  AND input_raw_sha256 = $3
                  AND canonical_url = ANY($4::text[])
                """,
                mailbox_id,
                clean_uid,
                str(input_raw_sha256 or ""),
                [canonical for _, canonical in unique_sources],
            )
            existing = {
                str(_row_get(row, "canonical_url", "")): {
                    "status": str(_row_get(row, "status", "") or ""),
                    "reason": str(_row_get(row, "reason", "") or ""),
                }
                for row in existing_rows
            }
        finally:
            await conn.close()

        async def store_shared_reference(
            *,
            source: str,
            canonical: str,
            shared: dict[str, Any],
            completion_kind: str,
            safety_decision: str,
            reference_metadata: dict[str, Any] | None = None,
        ) -> None:
            reference = _asset_reference_for_email(
                mailbox_id=mailbox_id,
                email_uid=clean_uid,
                input_raw_sha256=input_raw_sha256,
                canonical_url=canonical,
                shared_asset=shared,
                metadata={
                    **(metadata or {}),
                    **(reference_metadata or {}),
                    "source_url": source,
                    "canonical_url": canonical,
                },
            )
            await self.store_transformed_asset(
                mailbox_id=mailbox_id,
                email_uid=clean_uid,
                asset=reference,
                ensure_schema=False,
            )
            await self.record_external_image_derivative_state(
                mailbox_id=mailbox_id,
                email_uid=clean_uid,
                input_raw_sha256=input_raw_sha256,
                source_url=source,
                status="stored",
                reason="",
                safety_decision=safety_decision,
                transform_version=str(shared.get("transform_version") or ""),
                raw_image_sha256=str(shared.get("raw_image_sha256") or ""),
                transformed_sha256=str(shared.get("transformed_sha256") or ""),
                storage_relpath=str(shared.get("storage_relpath") or ""),
                shared_asset_uid=str(shared.get("shared_asset_uid") or ""),
                encrypted_size=int(shared.get("encrypted_size") or 0),
                content_type=str(shared.get("content_type") or ""),
                width=int(shared.get("width") or 0),
                height=int(shared.get("height") or 0),
                metadata={
                    **(metadata or {}),
                    "schema": "xarta.pim_email.external_image_derivative.metadata.v1",
                    "completion_kind": completion_kind,
                    "source_url": source,
                    "canonical_url": canonical,
                    "shared_asset_uid": str(shared.get("shared_asset_uid") or ""),
                },
                ensure_schema=False,
            )

        async def reuse_canonical_terminal_state(
            *,
            source: str,
            canonical: str,
            outcome: dict[str, Any],
        ) -> str:
            status = str(outcome.get("status") or "")
            await self.record_external_image_derivative_state(
                mailbox_id=mailbox_id,
                email_uid=clean_uid,
                input_raw_sha256=input_raw_sha256,
                source_url=source,
                status=status,
                reason=str(outcome.get("reason") or ""),
                safety_decision=f"reused_canonical_{status}_external_image_outcome",
                transform_version=str(outcome.get("transform_version") or ""),
                raw_image_sha256=str(outcome.get("raw_image_sha256") or ""),
                transformed_sha256=str(outcome.get("transformed_sha256") or ""),
                storage_relpath=str(outcome.get("storage_relpath") or ""),
                shared_asset_uid=str(outcome.get("shared_asset_uid") or ""),
                encrypted_size=int(outcome.get("encrypted_size") or 0),
                content_type=str(outcome.get("content_type") or ""),
                width=int(outcome.get("width") or 0),
                height=int(outcome.get("height") or 0),
                metadata={
                    **(metadata or {}),
                    "schema": "xarta.pim_email.external_image_derivative.metadata.v1",
                    "completion_kind": f"reused-canonical-{status}-outcome",
                    "source_url": source,
                    "canonical_url": canonical,
                    "outcome_derivative_id": str(outcome.get("derivative_id") or ""),
                },
                ensure_schema=False,
            )
            return status

        async def reuse_existing_canonical_work(source: str, canonical: str) -> str:
            shared = await self.find_shared_asset_for_url(
                mailbox_id=mailbox_id,
                source_url=canonical,
                ensure_schema=False,
            )
            if shared:
                await store_shared_reference(
                    source=source,
                    canonical=canonical,
                    shared=shared,
                    completion_kind="verified-shared-asset-reused",
                    safety_decision="reused_verified_shared_encrypted_asset",
                )
                return "stored"
            outcome = await self.find_external_image_canonical_terminal_state(
                mailbox_id=mailbox_id,
                source_url=canonical,
                ensure_schema=False,
            )
            if outcome:
                return await reuse_canonical_terminal_state(
                    source=source,
                    canonical=canonical,
                    outcome=outcome,
                )
            return ""

        for source, canonical in unique_sources:
            existing_state = existing.get(canonical, {})
            status = str(existing_state.get("status") or "")
            reason = str(existing_state.get("reason") or "")
            if _external_image_existing_state_is_terminal(status, reason):
                if status == "stored":
                    counts["already_stored"] += 1
                elif status == "blocked":
                    counts["already_blocked"] += 1
                elif status == "unavailable":
                    counts["already_unavailable"] += 1
                continue
            reused_state = await reuse_existing_canonical_work(source, canonical)
            if reused_state:
                counts[reused_state] += 1
                continue
            lock_conn, lock_key = await self._acquire_external_image_canonical_lock(
                mailbox_id=mailbox_id,
                canonical_url=canonical,
            )
            try:
                reused_state = await reuse_existing_canonical_work(source, canonical)
                if reused_state:
                    counts[reused_state] += 1
                    continue
                counts["attempted"] += 1
                fetched = await fetch_remote_image_bytes(source)
                raw_content = bytes(fetched.get("content") or b"")
                asset = build_transformed_external_image_asset(
                    mailbox_id=mailbox_id,
                    email_uid=clean_uid,
                    source_url=canonical,
                    content=raw_content,
                    metadata={
                        **(metadata or {}),
                        "input_raw_sha256": str(input_raw_sha256 or ""),
                        "source_url": source,
                        "canonical_url": canonical,
                        "fetched_content_type": str(fetched.get("content_type") or ""),
                        "fetched_final_url": str(fetched.get("final_url") or ""),
                    },
                )
                shared = await self.store_shared_asset(asset=asset, ensure_schema=False)
                await store_shared_reference(
                    source=source,
                    canonical=canonical,
                    shared=shared,
                    completion_kind="downloaded-transformed-encrypted-stored",
                    safety_decision="fetched_transformed_encrypted_stored",
                    reference_metadata=asset.get("metadata") or {},
                )
                counts["stored"] += 1
            except Exception as exc:
                state = _external_image_error_status(exc)
                await self.record_external_image_derivative_state(
                    mailbox_id=mailbox_id,
                    email_uid=clean_uid,
                    input_raw_sha256=input_raw_sha256,
                    source_url=source,
                    status=state,
                    reason=str(exc),
                    safety_decision=f"{state}_during_external_image_download_or_transform",
                    transform_version=EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                    metadata={
                        **(metadata or {}),
                        "schema": "xarta.pim_email.external_image_derivative.metadata.v1",
                        "completion_kind": state,
                        "source_url": source,
                        "canonical_url": canonical,
                        "error_class": exc.__class__.__name__,
                    },
                    ensure_schema=False,
                )
                counts[state] += 1
            finally:
                await self._release_external_image_canonical_lock(lock_conn, lock_key)
        return counts

    async def reset_legacy_non_downloaded_external_image_derivatives(
        self,
        *,
        mailbox_id: str | None = None,
    ) -> int:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        conn = await self._connect()
        try:
            return int(
                await conn.fetchval(
                    """
                    WITH updated AS (
                        UPDATE pim_email_external_image_derivatives
                        SET status = 'pending',
                            reason = 'legacy_non_downloaded_state_reset_for_real_download',
                            safety_decision = 'pending_real_download',
                            updated_at = now()
                        WHERE mailbox_id = $1
                          AND status = 'skipped'
                        RETURNING 1
                    )
                    SELECT count(*) FROM updated
                    """,
                    configured_mailbox_id,
                )
                or 0
            )
        finally:
            await conn.close()

    async def current_sanitized_view_artifact(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        raw_sha256: str,
    ) -> dict[str, Any] | None:
        await self.ensure_schema()
        clean_uid = clean_email_uid(email_uid)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT artifact_uid, email_uid, input_raw_sha256, sanitizer_policy_version,
                       transform_version, output_sha256, storage_relpath, encrypted_size,
                       views_available_json, safety_counts_json, derivation_json,
                       generated_at, updated_at
                FROM pim_email_sanitized_view_artifacts
                WHERE mailbox_id = $1
                  AND email_uid = $2
                  AND input_raw_sha256 = $3
                  AND sanitizer_policy_version = $4
                  AND transform_version = $5
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                mailbox_id,
                clean_uid,
                str(raw_sha256 or ""),
                SANITIZED_VIEW_POLICY_VERSION,
                SANITIZED_VIEW_TRANSFORM_VERSION,
            )
        finally:
            await conn.close()
        return _sanitized_artifact_row_public(row) if row else None

    async def _load_local_security_source(
        self,
        email_uid: str,
        *,
        mailbox_id: str,
    ) -> dict[str, Any]:
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_uid = clean_email_uid(email_uid)
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT *
                FROM pim_email_messages
                WHERE mailbox_id = $1 AND email_uid = $2
                """,
                configured_mailbox_id,
                clean_uid,
            )
            membership = await conn.fetchrow(
                """
                SELECT folder_name, imap_uid
                FROM pim_email_folder_memberships
                WHERE mailbox_id = $1 AND email_uid = $2
                ORDER BY last_seen_at DESC
                LIMIT 1
                """,
                configured_mailbox_id,
                clean_uid,
            )
        finally:
            await conn.close()
        if row is None:
            raise EmailOperationError("Local email message is not stored")
        raw = read_encrypted_bytes(str(row["storage_relpath"]))
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        if raw_sha256 != str(row["raw_sha256"]):
            raise EmailOperationError("Local email content hash verification failed")
        parsed = parse_message(
            raw,
            folder=str(_row_get(membership, "folder_name", "")) if membership else "",
            uid=str(_row_get(membership, "imap_uid", "")) if membership else "",
        )
        parsed["email_uid"] = clean_uid
        parsed["raw_sha256"] = raw_sha256
        return {
            "mailbox_id": configured_mailbox_id,
            "email_uid": clean_uid,
            "row": row,
            "membership": membership,
            "raw": raw,
            "raw_sha256": raw_sha256,
            "parsed": parsed,
        }

    async def completed_security_phase_result(
        self,
        *,
        mailbox_id: str,
        email_uid: str,
        raw_sha256: str,
        phase: str,
    ) -> dict[str, Any] | None:
        await self.ensure_schema()
        clean_phase = str(phase or "").strip().lower()
        if clean_phase not in {"deterministic", "llm"}:
            raise EmailOperationError("Invalid security phase")
        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                """
                SELECT phase_status, phase_json
                FROM pim_email_security_phases
                WHERE mailbox_id = $1
                  AND email_uid = $2
                  AND raw_sha256 = $3
                  AND phase = $4
                  AND policy_version = $5
                """,
                _configured_mailbox_id(mailbox_id),
                clean_email_uid(email_uid),
                str(raw_sha256 or ""),
                clean_phase,
                SECURITY_POLICY_VERSION,
            )
        finally:
            await conn.close()
        if not row or str(_row_get(row, "phase_status", "")) != "complete":
            return None
        result = _json_value(_row_get(row, "phase_json"), {})
        return result if isinstance(result, dict) else None

    async def run_local_security_deterministic_phase(
        self,
        email_uid: str,
        *,
        mailbox_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        source = await self._load_local_security_source(
            email_uid,
            mailbox_id=configured_mailbox_id,
        )
        raw = bytes(source["raw"])
        raw_sha256 = str(source["raw_sha256"])
        deterministic = check_email_security_deterministic_sync(
            raw,
            progress_callback=progress_callback,
        )
        await self.record_security_phase_result(
            mailbox_id=configured_mailbox_id,
            email_uid=str(source["email_uid"]),
            raw_sha256=raw_sha256,
            phase="deterministic",
            phase_status="complete",
            phase_result=deterministic,
            ensure_schema=False,
        )
        return {
            "schema": "xarta.pim_email.local_security_deterministic_phase.v1",
            "email_uid": str(source["email_uid"]),
            "raw_sha256": raw_sha256,
            "deterministic": deterministic,
        }

    async def run_local_security_llm_phase(
        self,
        email_uid: str,
        *,
        mailbox_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        source = await self._load_local_security_source(
            email_uid,
            mailbox_id=configured_mailbox_id,
        )
        clean_uid = str(source["email_uid"])
        raw = bytes(source["raw"])
        raw_sha256 = str(source["raw_sha256"])
        parsed = source["parsed"]
        membership = source["membership"]
        deterministic = await self.completed_security_phase_result(
            mailbox_id=configured_mailbox_id,
            email_uid=clean_uid,
            raw_sha256=raw_sha256,
            phase="deterministic",
        )
        if deterministic is None:
            raise EmailOperationError("Deterministic security phase is not complete")
        try:
            security = complete_email_security_with_llm_sync(
                raw,
                deterministic=deterministic,
                body_text=str((parsed.get("views") or {}).get("plain") or ""),
                progress_callback=progress_callback,
            )
        except Exception as exc:
            await self.record_security_phase_result(
                mailbox_id=configured_mailbox_id,
                email_uid=clean_uid,
                raw_sha256=raw_sha256,
                phase="llm",
                phase_status="failed_retryable",
                phase_result={
                    "schema": "xarta.pim_email.security_llm_phase.v1",
                    "available": False,
                    "status": "failed_retryable",
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc),
                },
                error_class=exc.__class__.__name__,
                error_message=str(exc),
                ensure_schema=False,
            )
            folder = clean_folder_name(str(_row_get(membership, "folder_name", "local") or "local"))
            uid = str(_row_get(membership, "imap_uid", clean_uid) if membership else clean_uid)
            message_id = str((parsed.get("headers") or {}).get("message_id") or "")
            await self.record_security_incomplete_result(
                mailbox_id=configured_mailbox_id,
                email_uid=clean_uid,
                raw_sha256=raw_sha256,
                folder=folder,
                uid=uid,
                message_id=message_id,
                phase="llm",
                error_class=exc.__class__.__name__,
                error_message=str(exc),
                deterministic_result=deterministic,
            )
            raise
        await self.record_security_phase_result(
            mailbox_id=configured_mailbox_id,
            email_uid=clean_uid,
            raw_sha256=raw_sha256,
            phase="llm",
            phase_status="complete",
            phase_result=security.get("llm") or {},
            ensure_schema=False,
        )
        storage_security = _security_result_for_storage(
            security,
            email_uid=clean_uid,
            raw_sha256=raw_sha256,
            parsed=parsed,
        )
        aggregate = (
            storage_security.get("aggregate")
            if isinstance(storage_security.get("aggregate"), dict)
            else {}
        )
        folder = clean_folder_name(str(_row_get(membership, "folder_name", "local") or "local"))
        uid = str(_row_get(membership, "imap_uid", clean_uid) if membership else clean_uid)
        message_id = str((parsed.get("headers") or {}).get("message_id") or "")
        security_check_id = _stable_id("email-security", clean_uid, raw_sha256)
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_security_checks (
                    security_check_id, mailbox_id, folder, uid, message_id, raw_sha256,
                    aggregate_status, aggregate_score, llm_called, result_json, checked_at,
                    email_uid, security_status, checker_versions_json, metadata_json
                )
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,now(),$11,'stored',$12::jsonb,$13::jsonb)
                ON CONFLICT (security_check_id) DO UPDATE SET
                    mailbox_id = EXCLUDED.mailbox_id,
                    folder = EXCLUDED.folder,
                    uid = EXCLUDED.uid,
                    message_id = EXCLUDED.message_id,
                    raw_sha256 = EXCLUDED.raw_sha256,
                    aggregate_status = EXCLUDED.aggregate_status,
                    aggregate_score = EXCLUDED.aggregate_score,
                    llm_called = EXCLUDED.llm_called,
                    result_json = EXCLUDED.result_json,
                    email_uid = EXCLUDED.email_uid,
                    security_status = EXCLUDED.security_status,
                    checker_versions_json = EXCLUDED.checker_versions_json,
                    metadata_json = EXCLUDED.metadata_json,
                    error_message = '',
                    checked_at = now();
                """,
                security_check_id,
                configured_mailbox_id,
                folder,
                uid,
                message_id,
                raw_sha256,
                str(aggregate.get("status") or "amber"),
                int(aggregate.get("score") or aggregate.get("risk_score") or 0),
                bool(
                    aggregate.get("llm_called") or (storage_security.get("llm") or {}).get("called")
                ),
                _json_dumps(storage_security, sort_keys=True, separators=(",", ":")),
                clean_uid,
                _json_dumps(
                    _security_checker_versions(storage_security),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                _json_dumps(
                    {
                        "schema": "xarta.pim_email.security_result.metadata.v1",
                        "email_uid": clean_uid,
                        "raw_sha256": raw_sha256,
                        "policy_version": SECURITY_POLICY_VERSION,
                        "source": "local-email-uid-security-action",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        finally:
            await conn.close()
        sanitized_artifact = build_sanitized_view_artifact(
            mailbox_id=configured_mailbox_id,
            email_uid=clean_uid,
            raw=raw,
            raw_sha256=raw_sha256,
        )
        artifact_public = await self.store_sanitized_view_artifact(artifact=sanitized_artifact)
        return {
            "schema": "xarta.pim_email.local_security_result.v1",
            "email_uid": clean_uid,
            "raw_sha256": raw_sha256,
            "security": storage_security,
            "sanitized_view": artifact_public,
        }

    async def run_local_security_check(
        self,
        email_uid: str,
        *,
        mailbox_id: str | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        await self.run_local_security_deterministic_phase(
            email_uid,
            mailbox_id=configured_mailbox_id,
            progress_callback=progress_callback,
        )
        return await self.run_local_security_llm_phase(
            email_uid,
            mailbox_id=configured_mailbox_id,
            progress_callback=progress_callback,
        )

    async def _record_backfill_item(
        self,
        *,
        run_id: str,
        batch_id: str,
        mailbox_id: str,
        email_uid: str,
        raw_sha256: str,
        artifact_type: str,
        status: str,
        error_class: str = "",
        error_message: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        clean_status = str(status or "").strip().lower()
        if clean_status not in {"running", "completed", "failed", "superseded"}:
            raise EmailOperationError("Invalid backfill item status")
        clean_artifact = str(artifact_type or "").strip().lower()
        if not clean_artifact:
            raise EmailOperationError("Invalid backfill artifact type")
        clean_uid = clean_email_uid(email_uid)
        clean_hash = str(raw_sha256 or "")
        item_id = _stable_id(
            "email-backfill-item",
            run_id,
            clean_uid,
            clean_hash,
            clean_artifact,
        )
        finished = clean_status in {"completed", "failed", "superseded"}
        conn = await self._connect()
        try:
            async with conn.transaction():
                if clean_status == "running":
                    claim_key = "\n".join((mailbox_id, clean_uid, clean_hash, clean_artifact))
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        claim_key,
                    )
                    claimed_by = await conn.fetchval(
                        """
                        SELECT i.run_id
                        FROM pim_email_backfill_items i
                        JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                        WHERE i.mailbox_id = $1
                          AND i.email_uid = $2
                          AND i.raw_sha256 = $3
                          AND i.artifact_type = $4
                          AND i.status = 'running'
                          AND r.status = 'running'
                          AND i.run_id <> $5
                        LIMIT 1
                        """,
                        mailbox_id,
                        clean_uid,
                        clean_hash,
                        clean_artifact,
                        run_id,
                    )
                    if claimed_by:
                        return False
                    already_converged = await conn.fetchval(
                        f"""
                        SELECT CASE
                          WHEN $4 = 'security' THEN EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = $2
                              AND s.raw_sha256 = $3
                              AND s.security_status = 'stored'
                              AND s.result_json->>'available' = 'true'
                              AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                              AND s.result_json->>'email_uid' = $2
                              AND s.result_json->>'raw_sha256' = $3
                              AND COALESCE(s.result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                          )
                          WHEN $4 = 'security_deterministic' THEN (
                            EXISTS (
                              SELECT 1
                              FROM pim_email_security_checks s
                              WHERE s.email_uid = $2
                                AND s.raw_sha256 = $3
                                AND s.security_status = 'stored'
                                AND s.result_json->>'available' = 'true'
                                AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                                AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                                AND s.result_json->>'email_uid' = $2
                                AND s.result_json->>'raw_sha256' = $3
                                AND COALESCE(s.result_json->>'checked_at', '') <> ''
                                AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                            )
                            OR EXISTS (
                              SELECT 1
                              FROM pim_email_security_phases p
                              WHERE p.mailbox_id = $1
                                AND p.email_uid = $2
                                AND p.raw_sha256 = $3
                                AND p.phase = 'deterministic'
                                AND p.phase_status = 'complete'
                                AND p.policy_version = '{SECURITY_POLICY_VERSION}'
                            )
                          )
                          WHEN $4 = 'security_llm' THEN EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = $2
                              AND s.raw_sha256 = $3
                              AND s.security_status = 'stored'
                              AND s.result_json->>'available' = 'true'
                              AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                              AND s.result_json->>'email_uid' = $2
                              AND s.result_json->>'raw_sha256' = $3
                              AND COALESCE(s.result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                          )
                          WHEN $4 = 'sanitized_view' THEN EXISTS (
                            SELECT 1
                            FROM pim_email_sanitized_view_artifacts a
                            WHERE a.mailbox_id = $1
                              AND a.email_uid = $2
                              AND a.input_raw_sha256 = $3
                              AND a.sanitizer_policy_version = '{SANITIZED_VIEW_POLICY_VERSION}'
                              AND a.transform_version = '{SANITIZED_VIEW_TRANSFORM_VERSION}'
                          )
                          WHEN $4 = 'external_images' THEN NOT EXISTS (
                            SELECT 1
                            FROM pim_email_external_image_derivatives d
                            WHERE d.mailbox_id = $1
                              AND d.email_uid = $2
                              AND d.input_raw_sha256 = $3
                              AND d.status IN ('pending','fetched','transformed','failed')
                          )
                          ELSE FALSE
                        END
                        """,
                        mailbox_id,
                        clean_uid,
                        clean_hash,
                        clean_artifact,
                    )
                    if already_converged:
                        return False
                await conn.execute(
                    """
                    INSERT INTO pim_email_backfill_items (
                        item_id, run_id, batch_id, mailbox_id, email_uid, raw_sha256,
                        artifact_type, status, attempts, error_class, error_message,
                        metadata_json, started_at, finished_at, updated_at
                    )
                    VALUES (
                        $1,$2,$3,$4,$5,$6,$7,$8,
                        CASE WHEN $8 = 'running' THEN 1 ELSE 0 END,
                        $9,$10,$11::jsonb,
                        CASE WHEN $8 = 'running' THEN now() ELSE NULL END,
                        CASE WHEN $12 THEN now() ELSE NULL END,
                        now()
                    )
                    ON CONFLICT (run_id, email_uid, raw_sha256, artifact_type)
                    DO UPDATE SET
                        batch_id = EXCLUDED.batch_id,
                        status = EXCLUDED.status,
                        attempts = pim_email_backfill_items.attempts
                          + CASE WHEN EXCLUDED.status = 'running' THEN 1 ELSE 0 END,
                        error_class = EXCLUDED.error_class,
                        error_message = EXCLUDED.error_message,
                        metadata_json = EXCLUDED.metadata_json,
                        started_at = COALESCE(
                            pim_email_backfill_items.started_at,
                            EXCLUDED.started_at
                        ),
                        finished_at = CASE
                            WHEN $12 THEN now()
                            ELSE pim_email_backfill_items.finished_at
                        END,
                        updated_at = now()
                    """,
                    item_id,
                    run_id,
                    batch_id,
                    mailbox_id,
                    clean_uid,
                    clean_hash,
                    clean_artifact,
                    clean_status,
                    str(error_class or "")[:200],
                    str(error_message or "")[:1000],
                    _json_dumps(metadata or {}, sort_keys=True, separators=(",", ":")),
                    finished,
                )
        finally:
            await conn.close()
        return True

    async def reconcile_superseded_backfill_failures(
        self,
        *,
        mailbox_id: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        metadata = {
            "schema": "xarta.pim_email.backfill_item_superseded.v1",
            "reason": "artifact_converged_after_failed_attempt",
        }
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                f"""
                WITH failed AS (
                    SELECT i.*, m.metadata_json AS message_metadata
                    FROM pim_email_backfill_items i
                    LEFT JOIN pim_email_messages m
                      ON m.mailbox_id = i.mailbox_id
                     AND m.email_uid = i.email_uid
                     AND m.raw_sha256 = i.raw_sha256
                    WHERE i.mailbox_id = $1
                      AND i.status = 'failed'
                ), converged AS (
                    SELECT f.item_id
                    FROM failed f
                    WHERE (
                        f.artifact_type = 'security'
                        AND EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = f.email_uid
                              AND s.raw_sha256 = f.raw_sha256
                              AND s.security_status = 'stored'
                              AND s.result_json->>'available' = 'true'
                              AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                              AND s.result_json->>'email_uid' = f.email_uid
                              AND s.result_json->>'raw_sha256' = f.raw_sha256
                              AND COALESCE(s.result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                        )
                    ) OR (
                        f.artifact_type = 'security_deterministic'
                        AND EXISTS (
                            SELECT 1
                            FROM pim_email_security_phases p
                            WHERE p.mailbox_id = f.mailbox_id
                              AND p.email_uid = f.email_uid
                              AND p.raw_sha256 = f.raw_sha256
                              AND p.phase = 'deterministic'
                              AND p.phase_status = 'complete'
                              AND p.policy_version = '{SECURITY_POLICY_VERSION}'
                        )
                    ) OR (
                        f.artifact_type = 'security_llm'
                        AND EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = f.email_uid
                              AND s.raw_sha256 = f.raw_sha256
                              AND s.security_status = 'stored'
                              AND s.result_json->>'available' = 'true'
                              AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                              AND s.result_json->>'email_uid' = f.email_uid
                              AND s.result_json->>'raw_sha256' = f.raw_sha256
                              AND COALESCE(s.result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                        )
                    ) OR (
                        f.artifact_type = 'sanitized_view'
                        AND EXISTS (
                            SELECT 1
                            FROM pim_email_sanitized_view_artifacts a
                            WHERE a.mailbox_id = f.mailbox_id
                              AND a.email_uid = f.email_uid
                              AND a.input_raw_sha256 = f.raw_sha256
                              AND a.sanitizer_policy_version = '{SANITIZED_VIEW_POLICY_VERSION}'
                              AND a.transform_version = '{SANITIZED_VIEW_TRANSFORM_VERSION}'
                        )
                    ) OR (
                        f.artifact_type = 'external_images'
                        AND NOT EXISTS (
                            SELECT 1
                            FROM pim_email_external_image_derivatives d
                            WHERE d.mailbox_id = f.mailbox_id
                              AND d.email_uid = f.email_uid
                              AND d.input_raw_sha256 = f.raw_sha256
                              AND d.status IN ('pending','fetched','transformed','failed')
                        )
                        AND (
                            CASE
                              WHEN jsonb_typeof(f.message_metadata->'remote_image_sources') = 'array'
                              THEN jsonb_array_length(f.message_metadata->'remote_image_sources')
                              ELSE 0
                            END = 0
                            OR EXISTS (
                                SELECT 1
                                FROM pim_email_external_image_derivatives terminal_d
                                WHERE terminal_d.mailbox_id = f.mailbox_id
                                  AND terminal_d.email_uid = f.email_uid
                                  AND terminal_d.input_raw_sha256 = f.raw_sha256
                                  AND terminal_d.status IN ('stored','unavailable','blocked')
                            )
                        )
                    )
                ), updated AS (
                    UPDATE pim_email_backfill_items i
                    SET status = 'superseded',
                        metadata_json = i.metadata_json || jsonb_build_object(
                            'superseded_by_convergence', $2::jsonb
                        ),
                        finished_at = COALESCE(i.finished_at, now()),
                        updated_at = now()
                    FROM converged c
                    WHERE i.item_id = c.item_id
                    RETURNING i.item_id, i.artifact_type
                )
                SELECT artifact_type, count(*) AS marked_count
                FROM updated
                GROUP BY artifact_type
                ORDER BY artifact_type
                """,
                configured_mailbox_id,
                _json_dumps(metadata, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        by_artifact = {str(row["artifact_type"]): int(row["marked_count"] or 0) for row in rows}
        return {
            "schema": "xarta.pim_email.backfill_item_superseded.result.v1",
            "mailbox_id": configured_mailbox_id,
            "marked_count": sum(by_artifact.values()),
            "by_artifact": by_artifact,
        }

    async def backfill_item_status_counts(self, *, run_id: str) -> dict[str, Any]:
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return {
                "schema": "xarta.pim_email.backfill_item_status_counts.v1",
                "run_id": "",
                "by_artifact": {},
            }
        await self.ensure_schema()
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                SELECT artifact_type, status, count(*) AS item_count
                FROM pim_email_backfill_items
                WHERE run_id = $1
                GROUP BY artifact_type, status
                ORDER BY artifact_type, status
                """,
                clean_run_id,
            )
        finally:
            await conn.close()
        by_artifact: dict[str, dict[str, int]] = {}
        for row in rows:
            artifact = str(row["artifact_type"] or "")
            status = str(row["status"] or "")
            by_artifact.setdefault(artifact, {})[status] = int(row["item_count"] or 0)
        return {
            "schema": "xarta.pim_email.backfill_item_status_counts.v1",
            "run_id": clean_run_id,
            "by_artifact": by_artifact,
        }

    async def reconcile_orphaned_backfill_runs(
        self,
        *,
        active_run_ids: set[str] | list[str] | tuple[str, ...],
        reason: str,
        mailbox_id: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_active = sorted({str(run_id or "").strip() for run_id in active_run_ids if run_id})
        metadata = {
            "schema": "xarta.pim_email.backfill_orphan_reconcile.v1",
            "reason": str(reason or "stack_process_not_active"),
            "active_run_ids": clean_active,
        }
        conn = await self._connect()
        try:
            rows = await conn.fetch(
                """
                UPDATE pim_email_backfill_runs
                SET status = 'interrupted-orphaned',
                    finished_at = COALESCE(finished_at, now()),
                    updated_at = now(),
                    metadata_json = metadata_json || jsonb_build_object(
                        'orphan_reconcile', $3::jsonb
                    )
                WHERE mailbox_id = $1
                  AND status = 'running'
                  AND NOT (run_id = ANY($2::text[]))
                RETURNING run_id
                """,
                configured_mailbox_id,
                clean_active,
                _json_dumps(metadata, sort_keys=True, separators=(",", ":")),
            )
            item_rows = await conn.fetch(
                """
                UPDATE pim_email_backfill_items i
                SET status = 'interrupted-orphaned',
                    finished_at = COALESCE(i.finished_at, now()),
                    updated_at = now(),
                    metadata_json = i.metadata_json || jsonb_build_object(
                        'orphan_reconcile', $2::jsonb
                    )
                FROM pim_email_backfill_runs r
                WHERE i.run_id = r.run_id
                  AND i.mailbox_id = $1
                  AND i.status = 'running'
                  AND r.status <> 'running'
                RETURNING i.run_id
                """,
                configured_mailbox_id,
                _json_dumps(metadata, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return {
            "schema": "xarta.pim_email.backfill_orphan_reconcile.result.v1",
            "mailbox_id": configured_mailbox_id,
            "active_run_ids": clean_active,
            "marked_orphaned": [str(row["run_id"]) for row in rows],
            "marked_count": len(rows),
            "marked_item_count": len(item_rows),
        }

    async def start_backfill_auxiliary_batch(
        self,
        *,
        run_id: str | None = None,
        mailbox_id: str | None = None,
        artifact_types: list[str],
        requested_limit: int | None = None,
        batch_index: int = 1,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        clean_artifacts = [
            str(item or "").strip().lower().replace("-", "_")
            for item in artifact_types
            if str(item or "").strip()
        ]
        if not clean_artifacts:
            raise EmailOperationError("No valid auxiliary backfill artifact types requested")
        safe_limit = None if requested_limit is None else max(1, int(requested_limit))
        actual_run_id = str(
            run_id
            or _stable_id(
                "email-backfill-run",
                configured_mailbox_id,
                ",".join(clean_artifacts),
                str(time.time_ns()),
            )
        )
        batch_id = _stable_id(
            "email-backfill-auxiliary-batch",
            actual_run_id,
            configured_mailbox_id,
            ",".join(clean_artifacts),
            str(max(1, int(batch_index or 1))),
            str(time.time_ns()),
        )
        run_metadata = {
            "schema": "xarta.pim_email.backfill_run.metadata.v1",
            "auxiliary_backfill": True,
            "candidate_order": "email_uid_desc_then_artifact_gap_priority",
            **(metadata or {}),
        }
        batch_metadata = {
            "schema": "xarta.pim_email.backfill_batch.metadata.v1",
            "auxiliary_backfill": True,
            "batch_index": max(1, int(batch_index or 1)),
            **(metadata or {}),
        }
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_backfill_runs (
                    run_id, mailbox_id, status, requested_limit, artifact_types_json,
                    metadata_json, finished_at, updated_at
                )
                VALUES ($1,$2,'running',$3,$4::jsonb,$5::jsonb,NULL,now())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'running',
                    requested_limit = EXCLUDED.requested_limit,
                    artifact_types_json = EXCLUDED.artifact_types_json,
                    metadata_json = pim_email_backfill_runs.metadata_json
                      || EXCLUDED.metadata_json,
                    finished_at = NULL,
                    updated_at = now()
                """,
                actual_run_id,
                configured_mailbox_id,
                safe_limit,
                _json_dumps(clean_artifacts, sort_keys=True, separators=(",", ":")),
                _json_dumps(run_metadata, sort_keys=True, separators=(",", ":")),
            )
            await conn.execute(
                """
                INSERT INTO pim_email_backfill_batches (
                    batch_id, run_id, mailbox_id, artifact_types_json,
                    planned_count, metadata_json, updated_at
                )
                VALUES ($1,$2,$3,$4::jsonb,0,$5::jsonb,now())
                ON CONFLICT (batch_id) DO UPDATE SET
                    artifact_types_json = EXCLUDED.artifact_types_json,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = now()
                """,
                batch_id,
                actual_run_id,
                configured_mailbox_id,
                _json_dumps(clean_artifacts, sort_keys=True, separators=(",", ":")),
                _json_dumps(batch_metadata, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return {
            "schema": "xarta.pim_email.backfill_auxiliary_batch.v1",
            "run_id": actual_run_id,
            "batch_id": batch_id,
            "mailbox_id": configured_mailbox_id,
            "artifact_types": clean_artifacts,
        }

    async def update_backfill_auxiliary_batch(
        self,
        *,
        run_id: str,
        batch_id: str,
        processed_count: int,
        failed_count: int,
        summary: dict[str, Any],
        aggregate: dict[str, Any] | None = None,
        final: bool = False,
    ) -> None:
        clean_status = (
            "completed-with-errors"
            if final and int(failed_count or 0) > 0
            else ("completed" if final else "running")
        )
        metadata = {
            "schema": "xarta.pim_email.backfill_batch.update.v1",
            "summary": summary,
            "aggregate": aggregate or {},
        }
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_backfill_runs
                SET status = $2,
                    processed_count = $3,
                    failed_count = $4,
                    summary_json = $5::jsonb,
                    finished_at = CASE WHEN $6 THEN now() ELSE NULL END,
                    updated_at = now()
                WHERE run_id = $1
                """,
                run_id,
                clean_status,
                int(processed_count or 0),
                int(failed_count or 0),
                _json_dumps(aggregate or summary, sort_keys=True, separators=(",", ":")),
                bool(final),
            )
            if batch_id:
                await conn.execute(
                    """
                    UPDATE pim_email_backfill_batches
                    SET planned_count = $2,
                        processed_count = $3,
                        failed_count = $4,
                        metadata_json = $5::jsonb,
                        updated_at = now()
                    WHERE batch_id = $1
                    """,
                    batch_id,
                    int(summary.get("planned") or 0),
                    int(summary.get("linked") or summary.get("processed_messages") or 0),
                    int(summary.get("failed") or summary.get("failed_messages") or 0),
                    _json_dumps(metadata, sort_keys=True, separators=(",", ":")),
                )
        finally:
            await conn.close()

    async def update_backfill_run_summary(
        self,
        *,
        run_id: str,
        processed_count: int,
        failed_count: int,
        summary: dict[str, Any],
        final: bool = False,
    ) -> None:
        clean_status = (
            "completed-with-errors"
            if final and int(failed_count or 0) > 0
            else ("completed" if final else "running")
        )
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_backfill_runs
                SET status = $2,
                    processed_count = $3,
                    failed_count = $4,
                    summary_json = $5::jsonb,
                    finished_at = CASE WHEN $6 THEN now() ELSE NULL END,
                    updated_at = now()
                WHERE run_id = $1
                """,
                run_id,
                clean_status,
                int(processed_count or 0),
                int(failed_count or 0),
                _json_dumps(summary, sort_keys=True, separators=(",", ":")),
                bool(final),
            )
        finally:
            await conn.close()

    async def run_backfill(
        self,
        *,
        mailbox_id: str | None = None,
        email_uid: str | None = None,
        limit: int | None = None,
        artifact_types: list[str] | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        await self.ensure_schema()
        configured_mailbox_id = _configured_mailbox_id(mailbox_id)
        allowed_artifacts = {
            "security",
            "security_deterministic",
            "security_llm",
            "sanitized_view",
            "external_images",
        }
        requested_artifacts = [
            str(item or "").strip().lower().replace("-", "_")
            for item in (artifact_types or ["security", "sanitized_view", "external_images"])
        ]
        clean_artifacts = [item for item in requested_artifacts if item in allowed_artifacts]
        if not clean_artifacts:
            raise EmailOperationError("No valid PIM email backfill artifact types requested")
        clean_email_filter = clean_email_uid(email_uid) if email_uid else ""
        safe_limit = None if limit is None else max(1, int(limit))
        actual_run_id = str(
            run_id
            or _stable_id(
                "email-backfill-run",
                configured_mailbox_id,
                ",".join(clean_artifacts),
                str(time.time_ns()),
            )
        )
        conn = await self._connect()
        try:
            params: list[Any] = [configured_mailbox_id]
            where = "WHERE mailbox_id = $1"
            if clean_email_filter:
                params.append(clean_email_filter)
                where += f" AND email_uid = ${len(params)}"
            limit_clause = ""
            if safe_limit is not None:
                params.append(safe_limit)
                limit_clause = f"LIMIT ${len(params)}"
            security_requested = "TRUE" if "security" in clean_artifacts else "FALSE"
            security_deterministic_requested = (
                "TRUE" if "security_deterministic" in clean_artifacts else "FALSE"
            )
            security_llm_requested = "TRUE" if "security_llm" in clean_artifacts else "FALSE"
            sanitized_requested = "TRUE" if "sanitized_view" in clean_artifacts else "FALSE"
            external_requested = "TRUE" if "external_images" in clean_artifacts else "FALSE"
            rows = await conn.fetch(
                f"""
                WITH candidates AS (
                    SELECT
                        m.email_uid,
                        m.mailbox_id,
                        m.raw_sha256,
                        m.storage_relpath,
                        m.updated_at,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = m.email_uid
                              AND s.raw_sha256 = m.raw_sha256
                        ) AS security_result_present,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_security_checks s
                            WHERE s.email_uid = m.email_uid
                              AND s.raw_sha256 = m.raw_sha256
                              AND s.security_status = 'stored'
                              AND s.result_json->>'available' = 'true'
                              AND COALESCE(s.result_json->>'queued', 'false') <> 'true'
                              AND COALESCE(s.result_json->>'placeholder', 'false') <> 'true'
                              AND s.result_json->>'email_uid' = m.email_uid
                              AND s.result_json->>'raw_sha256' = m.raw_sha256
                              AND COALESCE(s.result_json->>'checked_at', '') <> ''
                              AND jsonb_typeof(s.result_json->'checker_versions') = 'object'
                        ) AS security_complete,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_security_phases p
                            WHERE p.mailbox_id = m.mailbox_id
                              AND p.email_uid = m.email_uid
                              AND p.raw_sha256 = m.raw_sha256
                              AND p.phase = 'deterministic'
                              AND p.phase_status = 'complete'
                              AND p.policy_version = '{SECURITY_POLICY_VERSION}'
                        ) AS deterministic_complete,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_security_phases p
                            WHERE p.mailbox_id = m.mailbox_id
                              AND p.email_uid = m.email_uid
                              AND p.raw_sha256 = m.raw_sha256
                              AND p.phase = 'llm'
                              AND p.phase_status = 'complete'
                              AND p.policy_version = '{SECURITY_POLICY_VERSION}'
                        ) AS llm_complete,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_sanitized_view_artifacts a
                            WHERE a.mailbox_id = m.mailbox_id
                              AND a.email_uid = m.email_uid
                              AND a.input_raw_sha256 = m.raw_sha256
                              AND a.sanitizer_policy_version = '{SANITIZED_VIEW_POLICY_VERSION}'
                              AND a.transform_version = '{SANITIZED_VIEW_TRANSFORM_VERSION}'
                        ) AS sanitized_complete,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_external_image_derivatives d
                            WHERE d.mailbox_id = m.mailbox_id
                              AND d.email_uid = m.email_uid
                              AND d.input_raw_sha256 = m.raw_sha256
                              AND d.status IN ('pending','fetched','transformed','failed')
                        ) AS external_pending,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_backfill_items i
                            JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                            WHERE i.mailbox_id = m.mailbox_id
                              AND i.email_uid = m.email_uid
                              AND i.raw_sha256 = m.raw_sha256
                              AND i.artifact_type = 'security'
                              AND i.status = 'running'
                              AND r.status = 'running'
                        ) AS security_running,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_backfill_items i
                            JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                            WHERE i.mailbox_id = m.mailbox_id
                              AND i.email_uid = m.email_uid
                              AND i.raw_sha256 = m.raw_sha256
                              AND i.artifact_type = 'security_deterministic'
                              AND i.status = 'running'
                              AND r.status = 'running'
                        ) AS security_deterministic_running,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_backfill_items i
                            JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                            WHERE i.mailbox_id = m.mailbox_id
                              AND i.email_uid = m.email_uid
                              AND i.raw_sha256 = m.raw_sha256
                              AND i.artifact_type = 'security_llm'
                              AND i.status = 'running'
                              AND r.status = 'running'
                        ) AS security_llm_running,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_backfill_items i
                            JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                            WHERE i.mailbox_id = m.mailbox_id
                              AND i.email_uid = m.email_uid
                              AND i.raw_sha256 = m.raw_sha256
                              AND i.artifact_type = 'sanitized_view'
                              AND i.status = 'running'
                              AND r.status = 'running'
                        ) AS sanitized_running,
                        EXISTS (
                            SELECT 1
                            FROM pim_email_backfill_items i
                            JOIN pim_email_backfill_runs r ON r.run_id = i.run_id
                            WHERE i.mailbox_id = m.mailbox_id
                              AND i.email_uid = m.email_uid
                              AND i.raw_sha256 = m.raw_sha256
                              AND i.artifact_type = 'external_images'
                              AND i.status = 'running'
                              AND r.status = 'running'
                        ) AS external_running
                    FROM pim_email_messages m
                    {where}
                )
                SELECT email_uid, mailbox_id, raw_sha256, storage_relpath, updated_at
                FROM candidates
                WHERE NOT (
                    ({security_requested} AND security_running)
                    OR ({security_deterministic_requested} AND (
                        security_deterministic_running OR security_running
                    ))
                    OR ({security_llm_requested} AND (security_llm_running OR security_running))
                    OR ({sanitized_requested} AND sanitized_running)
                    OR ({external_requested} AND external_running)
                )
                AND (
                    ({security_requested} AND NOT security_complete)
                    OR (
                        {security_deterministic_requested}
                        AND NOT security_complete
                        AND NOT deterministic_complete
                    )
                    OR (
                        {security_llm_requested}
                        AND deterministic_complete
                        AND NOT security_complete
                    )
                    OR ({sanitized_requested} AND NOT sanitized_complete)
                    OR ({external_requested} AND external_pending)
                )
                ORDER BY
                  email_uid DESC,
                  CASE
                    WHEN {security_llm_requested} AND deterministic_complete AND NOT security_complete THEN 0
                    WHEN {security_requested} AND security_result_present AND NOT security_complete THEN 1
                    WHEN {security_deterministic_requested} AND NOT deterministic_complete THEN 1
                    WHEN {security_requested} AND NOT security_complete THEN 2
                    WHEN {sanitized_requested} AND NOT sanitized_complete THEN 2
                    WHEN {external_requested} AND external_pending THEN 2
                    ELSE 2
                  END,
                  updated_at DESC
                {limit_clause}
                """,
                *params,
            )
            await conn.execute(
                """
                INSERT INTO pim_email_backfill_runs (
                    run_id, mailbox_id, status, requested_limit, artifact_types_json,
                    metadata_json, updated_at
                )
                VALUES ($1,$2,'running',$3,$4::jsonb,$5::jsonb,now())
                ON CONFLICT (run_id) DO UPDATE SET
                    status = 'running',
                    requested_limit = EXCLUDED.requested_limit,
                    artifact_types_json = EXCLUDED.artifact_types_json,
                    metadata_json = EXCLUDED.metadata_json,
                    finished_at = NULL,
                    updated_at = now()
                """,
                actual_run_id,
                configured_mailbox_id,
                safe_limit,
                _json_dumps(clean_artifacts, sort_keys=True, separators=(",", ":")),
                _json_dumps(
                    {
                        "schema": "xarta.pim_email.backfill_run.metadata.v1",
                        "email_uid": clean_email_filter,
                        "security_policy_version": SECURITY_POLICY_VERSION,
                        "sanitizer_policy_version": SANITIZED_VIEW_POLICY_VERSION,
                        "transform_version": SANITIZED_VIEW_TRANSFORM_VERSION,
                        "external_image_derivative_version": EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                        "candidate_order": "email_uid_desc_then_artifact_gap_priority",
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        finally:
            await conn.close()
        batch_id = _stable_id(
            "email-backfill-batch",
            actual_run_id,
            configured_mailbox_id,
            str(len(rows)),
            str(time.time_ns()),
        )
        conn = await self._connect()
        try:
            await conn.execute(
                """
                INSERT INTO pim_email_backfill_batches (
                    batch_id, run_id, mailbox_id, artifact_types_json,
                    planned_count, metadata_json, updated_at
                )
                VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,now())
                ON CONFLICT (batch_id) DO UPDATE SET
                    planned_count = EXCLUDED.planned_count,
                    artifact_types_json = EXCLUDED.artifact_types_json,
                    metadata_json = EXCLUDED.metadata_json,
                    updated_at = now()
                """,
                batch_id,
                actual_run_id,
                configured_mailbox_id,
                _json_dumps(clean_artifacts, sort_keys=True, separators=(",", ":")),
                len(rows),
                _json_dumps(
                    {"schema": "xarta.pim_email.backfill_batch.metadata.v1"},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
        finally:
            await conn.close()
        summary: dict[str, Any] = {
            "schema": "xarta.pim_email.backfill_run.summary.v1",
            "mailbox_id": configured_mailbox_id,
            "artifact_types": clean_artifacts,
            "planned_messages": len(rows),
            "processed_messages": 0,
            "raw_originals_verified": 0,
            "raw_originals_failed": 0,
            "security_deterministic_completed": 0,
            "security_deterministic_already_completed": 0,
            "security_deterministic_failed": 0,
            "security_llm_completed": 0,
            "security_llm_already_completed": 0,
            "security_llm_failed": 0,
            "security_completed": 0,
            "security_already_completed": 0,
            "security_failed": 0,
            "sanitized_views_stored": 0,
            "sanitized_views_already_current": 0,
            "sanitized_views_failed": 0,
            "external_images_stored": 0,
            "external_images_blocked": 0,
            "external_images_failed": 0,
            "external_images_unavailable": 0,
            "external_images_pending": 0,
            "external_images_already_stored": 0,
            "external_images_already_blocked": 0,
            "external_images_already_unavailable": 0,
            "external_images_captured": 0,
            "external_images_materialize_candidates": 0,
            "external_images_materialized_rows": 0,
            "failed_messages": 0,
        }
        run_status = "completed"
        if "external_images" in clean_artifacts:
            materialized = await self.materialize_external_image_derivative_rows(
                mailbox_id=configured_mailbox_id,
                email_uid=clean_email_filter or None,
                limit=safe_limit,
                metadata={
                    "run_id": actual_run_id,
                    "batch_id": batch_id,
                    "phase": "external-images-materialize",
                },
            )
            summary["external_images_materialize_candidates"] = int(
                materialized.get("candidate_rows") or 0
            )
            summary["external_images_materialized_rows"] = int(
                materialized.get("materialized_rows") or 0
            )
            conn = await self._connect()
            try:
                await conn.execute(
                    """
                    UPDATE pim_email_backfill_runs
                    SET summary_json = $2::jsonb,
                        updated_at = now()
                    WHERE run_id = $1
                    """,
                    actual_run_id,
                    _json_dumps(summary, sort_keys=True, separators=(",", ":")),
                )
                await conn.execute(
                    """
                    UPDATE pim_email_backfill_batches
                    SET metadata_json = $2::jsonb,
                        updated_at = now()
                    WHERE batch_id = $1
                    """,
                    batch_id,
                    _json_dumps({"summary": summary}, sort_keys=True, separators=(",", ":")),
                )
            finally:
                await conn.close()
        for row in rows:
            email_uid = clean_email_uid(str(row["email_uid"]))
            expected_hash = str(row["raw_sha256"] or "")
            claimed_artifacts: list[str] = []
            artifact_phases = {
                "security": "security",
                "security_deterministic": "security-deterministic",
                "security_llm": "security-llm-final",
                "sanitized_view": "sanitized-view",
                "external_images": "external-images",
            }
            for artifact in clean_artifacts:
                claimed = await self._record_backfill_item(
                    run_id=actual_run_id,
                    batch_id=batch_id,
                    mailbox_id=configured_mailbox_id,
                    email_uid=email_uid,
                    raw_sha256=expected_hash,
                    artifact_type=artifact,
                    status="running",
                    metadata={"phase": artifact_phases.get(artifact, artifact)},
                )
                if claimed:
                    claimed_artifacts.append(artifact)
            if not claimed_artifacts:
                continue
            raw = b""
            try:
                raw = read_encrypted_bytes(str(row["storage_relpath"]))
                actual_hash = hashlib.sha256(raw).hexdigest()
                if actual_hash != expected_hash:
                    raise EmailOperationError("Backfill raw hash verification failed")
                summary["raw_originals_verified"] += 1
            except Exception as exc:
                run_status = "completed-with-errors"
                summary["raw_originals_failed"] += 1
                summary["failed_messages"] += 1
                for artifact in claimed_artifacts:
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type=artifact,
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "raw-verification"},
                    )
                continue

            if "security_deterministic" in claimed_artifacts:
                try:
                    completed = await self.completed_security_result(
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                    )
                    deterministic = await self.completed_security_phase_result(
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        phase="deterministic",
                    )
                    if completed is None and deterministic is None:
                        await self.run_local_security_deterministic_phase(
                            email_uid,
                            mailbox_id=configured_mailbox_id,
                        )
                        summary["security_deterministic_completed"] += 1
                    else:
                        summary["security_deterministic_already_completed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security_deterministic",
                        status="completed",
                        metadata={"phase": "security-deterministic"},
                    )
                except Exception as exc:
                    run_status = "completed-with-errors"
                    summary["security_deterministic_failed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security_deterministic",
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "security-deterministic"},
                    )

            if "security_llm" in claimed_artifacts:
                try:
                    completed = await self.completed_security_result(
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                    )
                    if completed is None:
                        await self.run_local_security_llm_phase(
                            email_uid,
                            mailbox_id=configured_mailbox_id,
                        )
                        summary["security_llm_completed"] += 1
                    else:
                        summary["security_llm_already_completed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security_llm",
                        status="completed",
                        metadata={"phase": "security-llm-final"},
                    )
                except Exception as exc:
                    run_status = "completed-with-errors"
                    summary["security_llm_failed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security_llm",
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "security-llm-final"},
                    )

            if "security" in claimed_artifacts:
                try:
                    completed = await self.completed_security_result(
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                    )
                    if completed is None:
                        deterministic = await self.completed_security_phase_result(
                            mailbox_id=configured_mailbox_id,
                            email_uid=email_uid,
                            raw_sha256=expected_hash,
                            phase="deterministic",
                        )
                        if deterministic is None:
                            await self.run_local_security_check(
                                email_uid,
                                mailbox_id=configured_mailbox_id,
                            )
                        else:
                            await self.run_local_security_llm_phase(
                                email_uid,
                                mailbox_id=configured_mailbox_id,
                            )
                        summary["security_completed"] += 1
                    else:
                        summary["security_already_completed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security",
                        status="completed",
                        metadata={"phase": "security"},
                    )
                except Exception as exc:
                    run_status = "completed-with-errors"
                    summary["security_failed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="security",
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "security"},
                    )

            if "sanitized_view" in claimed_artifacts:
                try:
                    current = await self.current_sanitized_view_artifact(
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                    )
                    if current is None:
                        artifact = build_sanitized_view_artifact(
                            mailbox_id=configured_mailbox_id,
                            email_uid=email_uid,
                            raw=raw,
                            raw_sha256=expected_hash,
                        )
                        await self.store_sanitized_view_artifact(artifact=artifact)
                        summary["sanitized_views_stored"] += 1
                    else:
                        summary["sanitized_views_already_current"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="sanitized_view",
                        status="completed",
                        metadata={"phase": "sanitized-view"},
                    )
                except Exception as exc:
                    run_status = "completed-with-errors"
                    summary["sanitized_views_failed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="sanitized_view",
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "sanitized-view"},
                    )

            if "external_images" in claimed_artifacts:
                try:
                    sources = remote_image_sources_from_raw(raw)
                    summary["external_images_captured"] += len(sources)
                    counts = await self.process_external_image_derivatives(
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        input_raw_sha256=expected_hash,
                        source_urls=sources,
                        metadata={"run_id": actual_run_id, "batch_id": batch_id},
                    )
                    summary["external_images_stored"] += counts["stored"]
                    summary["external_images_blocked"] += counts["blocked"]
                    summary["external_images_failed"] += counts["failed"]
                    summary["external_images_unavailable"] += counts["unavailable"]
                    summary["external_images_pending"] += counts["pending"]
                    summary["external_images_already_stored"] += counts["already_stored"]
                    summary["external_images_already_blocked"] += counts["already_blocked"]
                    summary["external_images_already_unavailable"] += counts["already_unavailable"]
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="external_images",
                        status="completed",
                        metadata={"phase": "external-images", "captured": len(sources)},
                    )
                except Exception as exc:
                    run_status = "completed-with-errors"
                    summary["external_images_failed"] += 1
                    await self._record_backfill_item(
                        run_id=actual_run_id,
                        batch_id=batch_id,
                        mailbox_id=configured_mailbox_id,
                        email_uid=email_uid,
                        raw_sha256=expected_hash,
                        artifact_type="external_images",
                        status="failed",
                        error_class=exc.__class__.__name__,
                        error_message=str(exc),
                        metadata={"phase": "external-images"},
                    )
            summary["processed_messages"] += 1
            conn = await self._connect()
            try:
                await conn.execute(
                    """
                    UPDATE pim_email_backfill_runs
                    SET status = 'running',
                        processed_count = $2,
                        failed_count = $3,
                        summary_json = $4::jsonb,
                        finished_at = NULL,
                        updated_at = now()
                    WHERE run_id = $1
                    """,
                    actual_run_id,
                    int(summary["processed_messages"]),
                    int(summary["failed_messages"]),
                    _json_dumps(summary, sort_keys=True, separators=(",", ":")),
                )
                await conn.execute(
                    """
                    UPDATE pim_email_backfill_batches
                    SET processed_count = $2,
                        failed_count = $3,
                        metadata_json = $4::jsonb,
                        updated_at = now()
                    WHERE batch_id = $1
                    """,
                    batch_id,
                    int(summary["processed_messages"]),
                    int(summary["failed_messages"]),
                    _json_dumps({"summary": summary}, sort_keys=True, separators=(",", ":")),
                )
            finally:
                await conn.close()
        conn = await self._connect()
        try:
            await conn.execute(
                """
                UPDATE pim_email_backfill_runs
                SET status = $2,
                    processed_count = $3,
                    failed_count = $4,
                    summary_json = $5::jsonb,
                    finished_at = now(),
                    updated_at = now()
                WHERE run_id = $1
                """,
                actual_run_id,
                run_status,
                int(summary["processed_messages"]),
                int(summary["failed_messages"]),
                _json_dumps(summary, sort_keys=True, separators=(",", ":")),
            )
            await conn.execute(
                """
                UPDATE pim_email_backfill_batches
                SET processed_count = $2,
                    failed_count = $3,
                    metadata_json = $4::jsonb,
                    updated_at = now()
                WHERE batch_id = $1
                """,
                batch_id,
                int(summary["processed_messages"]),
                int(summary["failed_messages"]),
                _json_dumps({"summary": summary}, sort_keys=True, separators=(",", ":")),
            )
        finally:
            await conn.close()
        return {
            "ok": run_status != "failed",
            "run_id": actual_run_id,
            "batch_id": batch_id,
            "status": run_status,
            "summary": summary,
        }


def _mailbox_row_public(row: Any) -> dict[str, Any]:
    updated = _row_get(row, "updated_at")
    return {
        "mailbox_id": str(row["mailbox_id"]),
        "email_address": str(row["email_address"]),
        "imap": {
            "host": str(row["imap_host"]),
            "port": int(row["imap_port"]),
            "ssl": bool(row["imap_ssl"]),
        },
        "smtp": {
            "host": str(row["smtp_host"]),
            "port": int(row["smtp_port"]),
            "ssl": bool(row["smtp_ssl"]),
            "starttls": bool(row["smtp_starttls"]),
        },
        "updated_at": updated.isoformat() if hasattr(updated, "isoformat") else str(updated or ""),
    }


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if hasattr(row, "get"):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, bytes):
        try:
            return json.loads(value.decode("utf-8"))
        except Exception:
            return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _iso_datetime(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value or "")


def _download_run_public(row: Any) -> dict[str, Any]:
    return {
        "run_id": str(_row_get(row, "run_id", "")),
        "status": str(_row_get(row, "status", "")),
        "started_at": _iso_datetime(_row_get(row, "started_at")),
        "finished_at": _iso_datetime(_row_get(row, "finished_at")),
        "summary": _json_value(_row_get(row, "summary_json"), {}),
    }


def _active_download_run_ids_from_proc(proc_root: str | Path = "/proc") -> set[str]:
    active: set[str] = set()
    root = Path(proc_root)
    try:
        cmdlines = root.glob("[0-9]*/cmdline")
    except Exception:
        return active
    for cmdline in cmdlines:
        try:
            raw = cmdline.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except Exception:
            continue
        if "pim_email_download_mailbox.py" not in raw or "--run-id" not in raw:
            continue
        match = re.search(r"(?:^|\s)--run-id\s+(\S+)", raw)
        if match:
            active.add(match.group(1))
    return active


def _folder_row_public(row: Any) -> dict[str, Any]:
    flags = _json_value(_row_get(row, "flags_json"), [])
    metadata = _json_value(_row_get(row, "metadata_json"), {})
    folder_name = str(_row_get(row, "folder_name", ""))
    return {
        "folder_uid": str(_row_get(row, "folder_uid", "")),
        "parent_folder_uid": str(_row_get(row, "parent_folder_uid", "")),
        "name": folder_name,
        "path": folder_name,
        "display_name": str(_row_get(row, "display_name", folder_name)),
        "delimiter": str(_row_get(row, "delimiter", "/") or "/"),
        "flags": flags if isinstance(flags, list) else [],
        "special_use_role": str(_row_get(row, "special_use_role", "")),
        "message_count": int(_row_get(row, "message_count", 0) or 0),
        "metadata": metadata if isinstance(metadata, dict) else {},
    }


def _local_message_row_public(row: Any) -> dict[str, Any]:
    metadata = _json_value(_row_get(row, "metadata_json"), {})
    flags = _json_value(_row_get(row, "flags_json"), [])
    return {
        "uid": str(_row_get(row, "imap_uid", "")),
        "folder": str(_row_get(row, "folder_name", "")),
        "email_uid": str(_row_get(row, "email_uid", "")),
        "raw_sha256": str(_row_get(row, "raw_sha256", "")),
        "subject": str(_row_get(row, "subject", "")),
        "from": str(_row_get(row, "from_addr", "")),
        "to": str(_row_get(row, "to_addr", "")),
        "date": str(_row_get(row, "date_header", "")),
        "message_id": str(_row_get(row, "message_id", "")),
        "uidvalidity": str(_row_get(row, "uidvalidity", "")),
        "flags": flags if isinstance(flags, list) else [],
        "metadata": metadata if isinstance(metadata, dict) else {},
        "source": "local-corpus",
        "updated_at": _iso_datetime(_row_get(row, "updated_at")),
        "last_seen_at": _iso_datetime(_row_get(row, "last_seen_at")),
    }


def _public_email_uid_info(value: Any) -> dict[str, Any]:
    info = _json_value(value, {})
    if not isinstance(info, dict):
        return {}
    public = dict(info)
    for key in (
        "storage_relpath",
        "raw_storage_relpath",
        "content_relpath",
        "path",
        "file_path",
        "filesystem_path",
    ):
        public.pop(key, None)
    return public


def _membership_row_public(row: Any) -> dict[str, Any]:
    flags = _json_value(_row_get(row, "flags_json"), [])
    return {
        "folder_name": str(_row_get(row, "folder_name", "")),
        "folder_uid": str(_row_get(row, "folder_uid", "")),
        "imap_uid": str(_row_get(row, "imap_uid", "")),
        "uidvalidity": str(_row_get(row, "uidvalidity", "")),
        "flags": flags if isinstance(flags, list) else [],
        "last_seen_at": _iso_datetime(_row_get(row, "last_seen_at")),
        "remote_moved_at": _iso_datetime(_row_get(row, "remote_moved_at")),
        "remote_move_target": str(_row_get(row, "remote_move_target", "")),
    }


def _asset_row_public(row: Any) -> dict[str, Any]:
    metadata = _json_value(_row_get(row, "metadata_json"), {})
    return {
        "asset_uid": str(_row_get(row, "asset_uid", "")),
        "shared_asset_uid": str(_row_get(row, "shared_asset_uid", "")),
        "source_url": str(_row_get(row, "source_url", "")),
        "canonical_url_digest": str(_row_get(row, "canonical_url_digest", "")),
        "content_type": str(_row_get(row, "content_type", "image/jpeg")),
        "transformed_sha256": str(_row_get(row, "transformed_sha256", "")),
        "storage_relpath": str(_row_get(row, "storage_relpath", "")),
        "width": int(_row_get(row, "width", 0) or 0),
        "height": int(_row_get(row, "height", 0) or 0),
        "transform_version": str(_row_get(row, "transform_version", "jpeg-v1")),
        "metadata": metadata if isinstance(metadata, dict) else {},
        "updated_at": _iso_datetime(_row_get(row, "updated_at")),
    }


def _shared_asset_row_public(row: Any) -> dict[str, Any]:
    metadata = _json_value(_row_get(row, "metadata_json"), {})
    encryption = _json_value(_row_get(row, "encryption_json"), {})
    return {
        "shared_asset_uid": str(_row_get(row, "shared_asset_uid", "")),
        "mailbox_id": str(_row_get(row, "mailbox_id", "")),
        "source_url": str(_row_get(row, "canonical_url", "")),
        "canonical_url": str(_row_get(row, "canonical_url", "")),
        "canonical_url_digest": str(_row_get(row, "canonical_url_digest", "")),
        "content_type": str(_row_get(row, "content_type", "image/jpeg")),
        "raw_image_sha256": str(_row_get(row, "raw_image_sha256", "")),
        "transformed_sha256": str(_row_get(row, "transformed_sha256", "")),
        "storage_relpath": str(_row_get(row, "storage_relpath", "")),
        "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
        "width": int(_row_get(row, "width", 0) or 0),
        "height": int(_row_get(row, "height", 0) or 0),
        "transform_version": str(_row_get(row, "transform_version", "jpeg-v1")),
        "encryption": encryption if isinstance(encryption, dict) else {},
        "metadata": metadata if isinstance(metadata, dict) else {},
        "reference_count": int(_row_get(row, "reference_count", 0) or 0),
        "created_at": _iso_datetime(_row_get(row, "created_at")),
        "updated_at": _iso_datetime(_row_get(row, "updated_at")),
    }


def _sanitized_artifact_row_public(row: Any) -> dict[str, Any]:
    safety_counts = _json_value(_row_get(row, "safety_counts_json"), {})
    derivation = _json_value(_row_get(row, "derivation_json"), {})
    views_available = _json_value(_row_get(row, "views_available_json"), {})
    return {
        "artifact_uid": str(_row_get(row, "artifact_uid", "")),
        "email_uid": str(_row_get(row, "email_uid", "")),
        "input_raw_sha256": str(_row_get(row, "input_raw_sha256", "")),
        "sanitizer_policy_version": str(_row_get(row, "sanitizer_policy_version", "")),
        "transform_version": str(_row_get(row, "transform_version", "")),
        "output_sha256": str(_row_get(row, "output_sha256", "")),
        "storage_relpath": str(_row_get(row, "storage_relpath", "")),
        "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
        "views_available": views_available if isinstance(views_available, dict) else {},
        "safety_counts": safety_counts if isinstance(safety_counts, dict) else {},
        "derivation": derivation if isinstance(derivation, dict) else {},
        "generated_at": _iso_datetime(_row_get(row, "generated_at")),
        "updated_at": _iso_datetime(_row_get(row, "updated_at")),
    }


def clean_email_uid(value: str) -> str:
    clean = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9]{8}-[0-9a-f]{40}", clean):
        raise EmailOperationError("Invalid local email UID")
    return clean


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("\n".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:32]}"


def _security_checker_versions(security: dict[str, Any]) -> dict[str, Any]:
    progress = security.get("progress") if isinstance(security.get("progress"), dict) else {}
    llm = security.get("llm") if isinstance(security.get("llm"), dict) else {}
    return {
        "schema": "xarta.pim_email.security_checker_versions.v1",
        "security_schema": str(security.get("schema") or ""),
        "progress_schema": str(progress.get("schema") or ""),
        "llm_model": str(llm.get("model") or ""),
        "llm_tools": "disabled",
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sanitized_view_relpath(email_uid: str, artifact_uid: str) -> str:
    clean_uid = clean_email_uid(email_uid)
    prefix = clean_uid.split("-", 1)[0]
    if prefix == "00000000":
        return f"undated/views/{clean_uid}/{artifact_uid}.json.enc"
    return f"{prefix[0:4]}/{prefix[4:6]}/{prefix[6:8]}/views/{clean_uid}/{artifact_uid}.json.enc"


def _sanitizer_safety_counts(parsed: dict[str, Any]) -> dict[str, Any]:
    html_security = (
        parsed.get("html_security") if isinstance(parsed.get("html_security"), dict) else {}
    )
    return {
        "schema": "xarta.pim_email.sanitizer_safety_counts.v1",
        "remote_images_blocked": int(html_security.get("remote_images_blocked") or 0),
        "remote_images_proxied": int(html_security.get("remote_images_proxied") or 0),
        "tracking_images_blocked": int(html_security.get("tracking_images_blocked") or 0),
        "inline_images_rendered": int(html_security.get("inline_images_rendered") or 0),
        "inline_images_blocked": int(html_security.get("inline_images_blocked") or 0),
        "active_content_blocked": int(html_security.get("active_content_blocked") or 0),
        "unsafe_links_blocked": int(html_security.get("unsafe_links_blocked") or 0),
        "allowed_links": int(html_security.get("allowed_links") or 0),
    }


def _security_counts(parsed: dict[str, Any]) -> dict[str, Any]:
    counts = _sanitizer_safety_counts(parsed)
    return {
        "schema": "xarta.pim_email.security_counts.v1",
        "url_link_count": counts["allowed_links"] + counts["unsafe_links_blocked"],
        "image_count": (
            counts["remote_images_blocked"]
            + counts["remote_images_proxied"]
            + counts["tracking_images_blocked"]
            + counts["inline_images_rendered"]
            + counts["inline_images_blocked"]
        ),
        "tracking_image_count": counts["tracking_images_blocked"],
        "active_content_count": counts["active_content_blocked"],
        "sanitizer_safety_counts": counts,
    }


def _security_result_for_storage(
    security: dict[str, Any],
    *,
    email_uid: str,
    raw_sha256: str,
    parsed: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = dict(_json_safe(security))
    result["schema"] = str(result.get("schema") or SECURITY_CHECK_SCHEMA)
    result["available"] = bool(result.get("available"))
    result["email_uid"] = clean_email_uid(email_uid)
    result["raw_sha256"] = str(raw_sha256)
    result["policy_version"] = SECURITY_POLICY_VERSION
    result["sanitizer_policy_version"] = SANITIZED_VIEW_POLICY_VERSION
    result["checker_versions"] = _security_checker_versions(result)
    if parsed is not None:
        result["message_counts"] = _security_counts(parsed)
    result.setdefault("checked_at", _now_iso())
    return result


def _completed_security_result_from_row(
    row: Any,
    *,
    email_uid: str,
    raw_sha256: str,
) -> dict[str, Any] | None:
    if row is None:
        return None
    result = _json_value(_row_get(row, "result_json"), {})
    if not isinstance(result, dict):
        return None
    if str(_row_get(row, "security_status", "")) != "stored":
        return None
    if str(_row_get(row, "raw_sha256", "")) != str(raw_sha256):
        return None
    if result.get("available") is not True:
        return None
    if result.get("queued") or result.get("placeholder"):
        return None
    if str(result.get("email_uid") or "") != clean_email_uid(email_uid):
        return None
    if str(result.get("raw_sha256") or "") != str(raw_sha256):
        return None
    if not str(result.get("checked_at") or ""):
        return None
    if not isinstance(result.get("checker_versions"), dict):
        return None
    return result


def _security_block_payload(
    *,
    email_uid: str,
    raw_sha256: str,
    reason: str,
    row: Any | None = None,
) -> dict[str, Any]:
    status = str(_row_get(row, "security_status", "missing") if row is not None else "missing")
    return {
        "schema": "xarta.pim_email.security_gate.v1",
        "available": False,
        "completed": False,
        "email_uid": clean_email_uid(email_uid),
        "raw_sha256": str(raw_sha256),
        "security_status": status or "missing",
        "blocked_reason": reason,
        "checked_at": _iso_datetime(_row_get(row, "checked_at")) if row is not None else "",
        "error_message": str(_row_get(row, "error_message", "") if row is not None else ""),
    }


def _local_message_envelope(
    row: Any,
    *,
    memberships: list[Any],
    raw_sha256: str,
    security: dict[str, Any],
    body_blocked: bool,
    sanitized_artifact: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_uid = clean_email_uid(str(_row_get(row, "email_uid", "")))
    headers = {
        "subject": str(_row_get(row, "subject", "")),
        "from": str(_row_get(row, "from_addr", "")),
        "to": str(_row_get(row, "to_addr", "")),
        "date": str(_row_get(row, "date_header", "")),
        "message_id": str(_row_get(row, "message_id", "")),
    }
    first_membership = memberships[0] if memberships else None
    message: dict[str, Any] = {
        "uid": str(_row_get(first_membership, "imap_uid", "")) if first_membership else "",
        "folder": str(_row_get(first_membership, "folder_name", "")) if first_membership else "",
        "email_uid": clean_uid,
        "email_uid_info": _public_email_uid_info(_row_get(row, "uid_info_json")),
        "raw_sha256": str(raw_sha256),
        "headers": headers,
        "source": "local-corpus",
        "security": security,
        "body_blocked": bool(body_blocked),
        "views": {},
        "views_available": {"plain": False, "html": False, "markdown": False, "raw": False},
        "stored": {
            "email_uid": clean_uid,
            "raw_sha256": str(raw_sha256),
            "encrypted_size": int(_row_get(row, "encrypted_size", 0) or 0),
            "verified": True,
            "memberships": [_membership_row_public(item) for item in memberships],
            "raw_original_access": "blocked",
        },
    }
    if sanitized_artifact:
        message.update(
            {
                "views": sanitized_artifact.get("views") or {},
                "views_available": sanitized_artifact.get("views_available") or {},
                "html_security": sanitized_artifact.get("html_security") or {},
                "attachments": sanitized_artifact.get("attachments") or [],
            }
        )
        message["stored"]["sanitized_view"] = sanitized_artifact.get("artifact") or {}
    return message


def build_sanitized_view_artifact(
    *,
    mailbox_id: str,
    email_uid: str,
    raw: bytes,
    raw_sha256: str | None = None,
) -> dict[str, Any]:
    clean_uid = clean_email_uid(email_uid)
    input_hash = str(raw_sha256 or hashlib.sha256(bytes(raw or b"")).hexdigest())
    parsed = parse_message(bytes(raw or b""), folder="", uid="")
    sanitized_raw = safe_raw_email_view(bytes(raw or b""))
    sanitized_raw_bytes = sanitized_raw.encode("utf-8", "replace")
    derived = parse_message(sanitized_raw_bytes, folder="", uid="")
    payload = {
        "schema": "xarta.pim_email.sanitized_raw_artifact.v1",
        "email_uid": clean_uid,
        "input_raw_sha256": input_hash,
        "sanitized_raw_sha256": hashlib.sha256(sanitized_raw_bytes).hexdigest(),
        "sanitizer_policy_version": SANITIZED_VIEW_POLICY_VERSION,
        "transform_version": SANITIZED_VIEW_TRANSFORM_VERSION,
        "headers": parsed.get("headers") or {},
        "sanitized_raw": sanitized_raw,
        "views_available": {
            "plain": bool((derived.get("views") or {}).get("plain")),
            "html": bool((derived.get("views") or {}).get("html")),
            "markdown": bool((derived.get("views") or {}).get("markdown")),
            "raw": bool(sanitized_raw),
        },
        "html_security": parsed.get("html_security") or {},
        "attachments": parsed.get("attachments") or [],
        "safety_counts": _sanitizer_safety_counts(parsed),
        "derivation": {
            "schema": "xarta.pim_email.sanitized_view.derivation.v1",
            "input": "encrypted-raw-eml",
            "input_raw_sha256": input_hash,
            "durable_body": "sanitized-raw-message",
            "view_bodies_persisted": False,
            "view_source": "derived-from-sanitized-raw",
            "raw_original_exposed": False,
        },
    }
    encoded = _json_dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    output_sha256 = hashlib.sha256(encoded).hexdigest()
    artifact_uid = _stable_id(
        "email-sanitized-view",
        mailbox_id,
        clean_uid,
        input_hash,
        SANITIZED_VIEW_POLICY_VERSION,
        SANITIZED_VIEW_TRANSFORM_VERSION,
    )
    storage = write_encrypted_bytes_atomic(
        relpath=_sanitized_view_relpath(clean_uid, artifact_uid),
        content=encoded,
        purpose=SANITIZED_VIEW_PURPOSE,
    )
    return {
        "artifact_uid": artifact_uid,
        "email_uid": clean_uid,
        "mailbox_id": mailbox_id,
        "input_raw_sha256": input_hash,
        "sanitizer_policy_version": SANITIZED_VIEW_POLICY_VERSION,
        "transform_version": SANITIZED_VIEW_TRANSFORM_VERSION,
        "output_sha256": output_sha256,
        "storage_relpath": storage["storage_relpath"],
        "encrypted_size": storage["encrypted_size"],
        "views_available": payload["views_available"],
        "safety_counts": payload["safety_counts"],
        "derivation": payload["derivation"],
        "encryption": storage["encryption"],
        "payload": payload,
    }


def read_sanitized_view_artifact(row: Any) -> dict[str, Any]:
    output_sha256 = str(_row_get(row, "output_sha256", ""))
    relpath = str(_row_get(row, "storage_relpath", ""))
    encoded = read_encrypted_bytes(relpath, purpose=SANITIZED_VIEW_PURPOSE)
    actual = hashlib.sha256(encoded).hexdigest()
    if output_sha256 and actual != output_sha256:
        raise EmailOperationError("Sanitized email view artifact hash verification failed")
    payload = _json_value(encoded, {})
    if not isinstance(payload, dict):
        raise EmailOperationError("Sanitized email view artifact payload is invalid")
    sanitized_raw = str(payload.get("sanitized_raw") or "")
    if not sanitized_raw:
        raise EmailOperationError("Sanitized email raw artifact payload is missing")
    sanitized_raw_sha256 = str(payload.get("sanitized_raw_sha256") or "")
    actual_sanitized_raw_sha256 = hashlib.sha256(
        sanitized_raw.encode("utf-8", "replace")
    ).hexdigest()
    if sanitized_raw_sha256 and sanitized_raw_sha256 != actual_sanitized_raw_sha256:
        raise EmailOperationError("Sanitized email raw artifact hash verification failed")
    derived = parse_message(sanitized_raw.encode("utf-8", "replace"), folder="", uid="")
    views = dict(derived.get("views") or {})
    views["raw"] = sanitized_raw
    payload["views"] = views
    payload["views_available"] = {
        "plain": bool(views.get("plain")),
        "html": bool(views.get("html")),
        "markdown": bool(views.get("markdown")),
        "raw": bool(sanitized_raw),
    }
    payload["html_security"] = derived.get("html_security") or {}
    payload["attachments"] = derived.get("attachments") or payload.get("attachments") or []
    payload["artifact"] = _sanitized_artifact_row_public(row)
    return payload


def special_use_role(folder_name: str, flags: list[str] | None = None) -> str:
    clean_flags = {str(flag or "").lower().lstrip("\\") for flag in (flags or [])}
    for role in ("drafts", "sent", "trash", "junk", "archive"):
        if role in clean_flags:
            return role
    if "spam" in clean_flags:
        return "spam"
    leaf = re.sub(r"[^a-z0-9]+", "", str(folder_name or "").rsplit("/", 1)[-1].lower())
    if leaf in {"draft", "drafts"}:
        return "drafts"
    if leaf in {"sent", "sentmail", "sentmessages", "sentitems"}:
        return "sent"
    if leaf in {"trash", "rubbish", "bin", "deleted", "deleteditems"}:
        return "trash"
    if leaf in {"junk", "spam"}:
        return "junk" if leaf == "junk" else "spam"
    if leaf in {"archive", "archives", "archived"}:
        return "archive"
    if leaf == "inbox":
        return "inbox"
    return ""


def folder_uid_for(mailbox_id: str, folder_name: str) -> str:
    return _stable_id("email-folder", mailbox_id, clean_folder_name(folder_name))


def parent_folder_uid_for(mailbox_id: str, folder_name: str, delimiter: str = "/") -> str:
    clean = clean_folder_name(folder_name)
    delim = delimiter or "/"
    if delim not in clean:
        return ""
    parent = clean.rsplit(delim, 1)[0]
    return folder_uid_for(mailbox_id, parent) if parent else ""


def _email_asset_relpath(email_uid: str, asset_uid: str) -> str:
    clean_uid = clean_email_uid(email_uid)
    prefix = clean_uid.split("-", 1)[0]
    if prefix == "00000000":
        return f"undated/assets/{clean_uid}/{asset_uid}.jpg.enc"
    return f"{prefix[0:4]}/{prefix[4:6]}/{prefix[6:8]}/assets/{clean_uid}/{asset_uid}.jpg.enc"


def _shared_email_asset_relpath(canonical_url_digest: str, shared_asset_uid: str) -> str:
    digest = re.sub(r"[^0-9a-f]+", "", str(canonical_url_digest or "").lower())[:64]
    if not digest:
        digest = hashlib.sha256(str(shared_asset_uid or "").encode("utf-8")).hexdigest()
    return f"assets/{digest[:2]}/{digest}/{shared_asset_uid}.jpg.enc"


def _asset_reference_for_email(
    *,
    mailbox_id: str,
    email_uid: str,
    input_raw_sha256: str,
    canonical_url: str,
    shared_asset: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_uid = clean_email_uid(email_uid)
    canonical = _canonical_remote_image_url(canonical_url) or str(canonical_url or "")
    canonical_digest = _external_image_canonical_digest(canonical)
    shared_uid = str(shared_asset.get("shared_asset_uid") or shared_asset.get("asset_uid") or "")
    asset_uid = _stable_id(
        "email-asset-ref",
        mailbox_id,
        clean_uid,
        input_raw_sha256,
        canonical,
        shared_uid,
    )
    return {
        "asset_uid": asset_uid,
        "shared_asset_uid": shared_uid,
        "email_uid": clean_uid,
        "mailbox_id": mailbox_id,
        "source_url": canonical,
        "canonical_url_digest": canonical_digest,
        "content_type": str(shared_asset.get("content_type") or "image/jpeg"),
        "raw_sha256": str(
            shared_asset.get("raw_image_sha256") or shared_asset.get("raw_sha256") or ""
        ),
        "transformed_sha256": str(shared_asset.get("transformed_sha256") or ""),
        "storage_relpath": str(shared_asset.get("storage_relpath") or ""),
        "encrypted_size": int(shared_asset.get("encrypted_size") or 0),
        "width": int(shared_asset.get("width") or 0),
        "height": int(shared_asset.get("height") or 0),
        "transform_version": str(shared_asset.get("transform_version") or "jpeg-v1"),
        "encryption": shared_asset.get("encryption") or {},
        "metadata": {
            "schema": "xarta.pim_email.transformed_external_image.reference.v1",
            "shared_asset_uid": shared_uid,
            "input_raw_sha256": str(input_raw_sha256 or ""),
            "storage_kind": "shared-encrypted-asset",
            **(metadata or {}),
        },
    }


def build_transformed_external_image_asset(
    *,
    mailbox_id: str,
    email_uid: str,
    source_url: str,
    content: bytes,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    clean_uid = clean_email_uid(email_uid)
    canonical = _canonical_remote_image_url(source_url) or str(source_url or "")
    raw_sha256 = hashlib.sha256(bytes(content or b"")).hexdigest()
    jpeg = transform_image_to_jpeg(content)
    transformed_sha256 = hashlib.sha256(jpeg).hexdigest()
    canonical_digest = _external_image_canonical_digest(canonical)
    shared_asset_uid = _stable_id(
        "email-shared-asset",
        mailbox_id,
        canonical_digest,
        transformed_sha256,
        "jpeg-v1",
    )
    asset_uid = _stable_id(
        "email-asset-ref",
        mailbox_id,
        clean_uid,
        str((metadata or {}).get("input_raw_sha256") or ""),
        canonical,
        shared_asset_uid,
    )
    with Image.open(BytesIO(jpeg)) as image:
        width, height = image.size
    storage = write_encrypted_bytes_atomic(
        relpath=_shared_email_asset_relpath(canonical_digest, shared_asset_uid),
        content=jpeg,
        purpose=ASSET_PURPOSE,
    )
    return {
        "asset_uid": asset_uid,
        "shared_asset_uid": shared_asset_uid,
        "email_uid": clean_uid,
        "mailbox_id": mailbox_id,
        "source_url": canonical,
        "canonical_url_digest": canonical_digest,
        "content_type": "image/jpeg",
        "raw_sha256": raw_sha256,
        "raw_image_sha256": raw_sha256,
        "transformed_sha256": transformed_sha256,
        "storage_relpath": storage["storage_relpath"],
        "encrypted_size": storage["encrypted_size"],
        "width": int(width),
        "height": int(height),
        "transform_version": "jpeg-v1",
        "encryption": storage["encryption"],
        "metadata": {
            "schema": "xarta.pim_email.transformed_external_image.metadata.v1",
            "verified": storage["verified"],
            "shared_asset_uid": shared_asset_uid,
            "storage_kind": "shared-encrypted-asset",
            **(metadata or {}),
        },
    }


def mailbox_from_env_password(password: str) -> EmailMailbox:
    return EmailMailbox(
        mailbox_id=_configured_mailbox_id(),
        email_address=_env_required("BLUEPRINTS_EMAIL_ADDRESS"),
        imap_host=_env_required("BLUEPRINTS_EMAIL_IMAP_HOST"),
        imap_port=int(os.environ.get("BLUEPRINTS_EMAIL_IMAP_PORT", str(DEFAULT_IMAP_PORT))),
        imap_ssl=_env_bool("BLUEPRINTS_EMAIL_IMAP_SSL", True),
        smtp_host=_env_required("BLUEPRINTS_EMAIL_SMTP_HOST"),
        smtp_port=int(os.environ.get("BLUEPRINTS_EMAIL_SMTP_PORT", str(DEFAULT_SMTP_PORT))),
        smtp_ssl=_env_bool("BLUEPRINTS_EMAIL_SMTP_SSL", True),
        smtp_starttls=_env_bool("BLUEPRINTS_EMAIL_SMTP_STARTTLS", False),
        password=password,
    )


def _configured_mailbox_id(mailbox_id: str | None = None) -> str:
    return (
        mailbox_id or os.environ.get("BLUEPRINTS_EMAIL_MAILBOX_ID", DEFAULT_MAILBOX_ID)
    ).strip() or DEFAULT_MAILBOX_ID


def _env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EmailConfigError(f"{name} is not configured")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_imap_list_line(line: bytes | str) -> dict[str, Any]:
    text = line.decode("utf-8", "replace") if isinstance(line, bytes) else str(line)
    flags_match = re.match(r"\((?P<flags>.*?)\)\s+\"?(?P<delimiter>.*?)\"?\s+(?P<name>.*)$", text)
    if not flags_match:
        return {"name": text.strip('" '), "delimiter": "/", "flags": []}
    raw_name = flags_match.group("name").strip()
    if raw_name.startswith('"') and raw_name.endswith('"'):
        raw_name = raw_name[1:-1]
    flags = [
        item.lstrip("\\").lower() for item in flags_match.group("flags").split() if item.strip()
    ]
    delimiter = flags_match.group("delimiter") or "/"
    return {"name": raw_name, "delimiter": delimiter, "flags": flags}


def _connect_imap(mailbox: EmailMailbox) -> imaplib.IMAP4:
    if not mailbox.imap_ssl:
        raise EmailConfigError("Only SSL/TLS IMAP is enabled for this MVP")
    timeout = float(
        os.environ.get("BLUEPRINTS_EMAIL_IMAP_TIMEOUT_SECONDS", DEFAULT_IMAP_TIMEOUT_SECONDS)
    )
    client = imaplib.IMAP4_SSL(mailbox.imap_host, mailbox.imap_port, timeout=timeout)
    sock = getattr(client, "sock", None)
    if sock is not None and hasattr(sock, "settimeout"):
        sock.settimeout(timeout)
    client.login(mailbox.email_address, mailbox.password)
    return client


def _logout_imap(client: imaplib.IMAP4) -> None:
    try:
        client.logout()
    except Exception:
        pass


def list_folders_sync(mailbox: EmailMailbox) -> list[dict[str, Any]]:
    client = _connect_imap(mailbox)
    try:
        status, rows = client.list()
        if status != "OK":
            raise EmailOperationError("IMAP folder listing failed")
        return [parse_imap_list_line(row) for row in rows or []]
    finally:
        _logout_imap(client)


def list_folder_messages_sync(
    mailbox: EmailMailbox, *, folder: str = "INBOX", limit: int = 25
) -> list[dict[str, Any]]:
    clean_folder = clean_folder_name(folder)
    safe_limit = max(1, min(int(limit), 100))
    client = _connect_imap(mailbox)
    try:
        _select_imap_folder(client, clean_folder)
        status, search_data = client.uid("search", None, "ALL")
        if status != "OK" or not search_data:
            raise EmailOperationError("IMAP folder search failed")
        uids = search_data[0].split()[-safe_limit:]
        messages: list[dict[str, Any]] = []
        for uid in reversed(uids):
            fetch_status, fetch_data = client.uid("fetch", uid, "(RFC822.HEADER RFC822.SIZE FLAGS)")
            if fetch_status != "OK":
                continue
            header_bytes = _first_fetch_bytes(fetch_data)
            msg = BytesParser(policy=policy.default).parsebytes(header_bytes or b"")
            email_uid_info = generate_email_uid_info(header_bytes)
            messages.append(
                {
                    "uid": uid.decode("ascii", "replace"),
                    "folder": clean_folder,
                    "email_uid": email_uid_info["email_uid"],
                    "email_uid_info": email_uid_info,
                    "subject": _message_header_value(msg, "subject"),
                    "from": _message_header_value(msg, "from"),
                    "date": _message_header_value(msg, "date"),
                    "message_id": _message_header_value(msg, "message-id"),
                }
            )
        return messages
    finally:
        _logout_imap(client)


def list_inbox_sync(mailbox: EmailMailbox, *, limit: int = 25) -> list[dict[str, Any]]:
    return list_folder_messages_sync(mailbox, folder="INBOX", limit=limit)


def _run_store(coro: Any) -> Any:
    return asyncio.run(coro)


def _imap_list_folders(client: imaplib.IMAP4) -> list[dict[str, Any]]:
    status, rows = client.list()
    if status != "OK":
        raise EmailOperationError("IMAP folder listing failed")
    return [parse_imap_list_line(row) for row in rows or []]


def _parse_imap_status_rows(rows: Any) -> dict[str, Any]:
    text = " ".join(
        row.decode("utf-8", "replace") if isinstance(row, bytes) else str(row)
        for row in (rows or [])
    )
    status: dict[str, Any] = {}
    for key, value in re.findall(r"\b([A-Z][A-Z0-9-]*)\s+([0-9]+)", text):
        status[key] = value
    return status


def _imap_folder_status(client: imaplib.IMAP4, folder: str) -> dict[str, Any]:
    try:
        status, rows = client.status(
            _imap_mailbox_select_arg(folder), "(UIDVALIDITY UIDNEXT MESSAGES UNSEEN)"
        )
    except Exception:
        return {}
    if status != "OK":
        return {}
    return _parse_imap_status_rows(rows)


def _imap_uid_search_all(client: imaplib.IMAP4) -> list[str]:
    status, search_data = client.uid("search", None, "ALL")
    if status != "OK" or not search_data:
        raise EmailOperationError("IMAP folder search failed")
    return [
        clean_uid_value(uid.decode("ascii", "replace") if isinstance(uid, bytes) else str(uid))
        for uid in search_data[0].split()
    ]


def _imap_fetch_flags(fetch_data: Any) -> list[str]:
    text = " ".join(
        part.decode("utf-8", "replace") if isinstance(part, bytes) else str(part)
        for item in (fetch_data or [])
        for part in (item if isinstance(item, tuple) else (item,))
    )
    match = re.search(r"FLAGS\s+\(([^)]*)\)", text, re.I)
    if not match:
        return []
    return [flag.strip().lstrip("\\").lower() for flag in match.group(1).split() if flag.strip()]


def _imap_call_timeout_seconds() -> float:
    return max(
        1.0,
        float(
            os.environ.get(
                "BLUEPRINTS_EMAIL_IMAP_CALL_TIMEOUT_SECONDS",
                DEFAULT_IMAP_CALL_TIMEOUT_SECONDS,
            )
        ),
    )


def _imap_fetch_chunk_bytes() -> int:
    return max(
        64 * 1024,
        int(
            os.environ.get(
                "BLUEPRINTS_EMAIL_IMAP_FETCH_CHUNK_BYTES",
                DEFAULT_IMAP_FETCH_CHUNK_BYTES,
            )
        ),
    )


def _close_imap_socket(client: imaplib.IMAP4) -> None:
    for attr in ("sock", "sslobj"):
        sock = getattr(client, attr, None)
        if sock is None:
            continue
        try:
            if hasattr(sock, "settimeout"):
                sock.settimeout(0)
            sock.close()
        except Exception:
            pass


def _imap_uid_fetch_with_timeout(
    client: imaplib.IMAP4,
    uid: str,
    query: str,
    *,
    timeout: float | None = None,
) -> Any:
    clean_uid = clean_uid_value(uid)
    timeout_seconds = timeout if timeout is not None else _imap_call_timeout_seconds()
    results: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

    def run_fetch() -> None:
        try:
            results.put(("ok", client.uid("fetch", clean_uid, query)))
        except BaseException as exc:
            results.put(("error", exc))

    thread = threading.Thread(
        target=run_fetch,
        name=f"pim-email-imap-fetch-{clean_uid}",
        daemon=True,
    )
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        _close_imap_socket(client)
        raise EmailImapTimeoutError(
            f"IMAP UID FETCH timed out after {timeout_seconds:.0f}s for UID {clean_uid}"
        )
    try:
        kind, payload = results.get_nowait()
    except queue.Empty as exc:
        raise EmailOperationError("IMAP fetch worker returned no result") from exc
    if kind == "error":
        raise payload
    return payload


def _imap_fetch_size(fetch_data: Any) -> int | None:
    text = " ".join(
        part.decode("utf-8", "replace") if isinstance(part, bytes) else str(part)
        for item in (fetch_data or [])
        for part in (item if isinstance(item, tuple) else (item,))
    )
    match = re.search(r"RFC822\.SIZE\s+([0-9]+)", text, re.I)
    if not match:
        return None
    return max(0, int(match.group(1)))


def _is_imap_connection_error(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            EmailImapTimeoutError,
            imaplib.IMAP4.abort,
            ssl.SSLError,
            ConnectionError,
            TimeoutError,
        ),
    )


def _imap_fetch_raw_for_uid(client: imaplib.IMAP4, uid: str) -> tuple[bytes, list[str]]:
    clean_uid = clean_uid_value(uid)
    fetch_status, fetch_data = _imap_uid_fetch_with_timeout(
        client,
        clean_uid,
        "(RFC822.SIZE FLAGS)",
    )
    flags = _imap_fetch_flags(fetch_data)
    raw_size = _imap_fetch_size(fetch_data)
    if fetch_status == "OK" and raw_size is not None:
        chunks: list[bytes] = []
        offset = 0
        chunk_size = _imap_fetch_chunk_bytes()
        while offset < raw_size:
            requested = min(chunk_size, raw_size - offset)
            chunk_status, chunk_data = _imap_uid_fetch_with_timeout(
                client,
                clean_uid,
                f"(BODY.PEEK[]<{offset}.{requested}>)",
            )
            if chunk_status != "OK":
                raise EmailOperationError("IMAP message chunk fetch failed")
            chunk = _first_fetch_bytes(chunk_data)
            if not chunk:
                raise EmailOperationError("IMAP message chunk was empty")
            chunks.append(chunk)
            offset += len(chunk)
        raw = b"".join(chunks)
    else:
        fetch_status, fetch_data = _imap_uid_fetch_with_timeout(
            client,
            clean_uid,
            "(RFC822 FLAGS)",
        )
        if fetch_status != "OK":
            raise EmailOperationError("IMAP message fetch failed")
        raw = _first_fetch_bytes(fetch_data)
        flags = _imap_fetch_flags(fetch_data)
    if not raw:
        raise EmailOperationError("IMAP message body was empty")
    return raw, flags


def _folder_is_download_target(folder_name: str, downloaded_folder: str) -> bool:
    return clean_folder_name(folder_name).lower() == clean_folder_name(downloaded_folder).lower()


def _folder_name_move_skip_role(folder_name: str) -> str:
    clean = clean_folder_name(folder_name)
    first_part = re.sub(r"[^a-z0-9]+", "", clean.split("/", 1)[0].lower())
    if first_part in {"draft", "drafts"}:
        return "drafts"
    if first_part in {"sent", "sentmail", "sentmessages", "sentitems"}:
        return "sent"
    if first_part in {"trash", "rubbish", "bin", "deleted", "deleteditems"}:
        return "trash"
    if first_part in {"junk", "spam"}:
        return "junk" if first_part == "junk" else "spam"
    if first_part in {"archive", "archives", "archived"}:
        return "archive"
    return ""


def _folder_move_allowed(folder_snapshot: dict[str, Any], downloaded_folder: str) -> bool:
    role = str(folder_snapshot.get("special_use_role") or "").lower()
    folder_name = str(folder_snapshot.get("folder_name") or "")
    if _folder_is_download_target(folder_name, downloaded_folder):
        return False
    if _folder_name_move_skip_role(folder_name) in SPECIAL_USE_MOVE_SKIP_ROLES:
        return False
    return role not in SPECIAL_USE_MOVE_SKIP_ROLES


def _ensure_downloaded_folder(
    client: imaplib.IMAP4,
    folders: list[dict[str, Any]],
    downloaded_folder: str,
) -> bool:
    target = clean_folder_name(downloaded_folder)
    if any(_folder_is_download_target(str(folder.get("name") or ""), target) for folder in folders):
        return True
    try:
        status, _ = client.create(_imap_mailbox_select_arg(target))
    except Exception:
        return False
    return status == "OK"


def _imap_move_uid(client: imaplib.IMAP4, uid: str, target_folder: str) -> bool:
    status, _ = client.uid("MOVE", clean_uid_value(uid), _imap_mailbox_select_arg(target_folder))
    return status == "OK"


def _all_headers_json(raw: bytes) -> dict[str, Any]:
    msg = BytesParser(policy=policy.default).parsebytes(bytes(raw or b""))
    values: dict[str, list[str]] = {}
    for name, value in msg.raw_items():
        values.setdefault(str(name), []).append(_decode_header_value(str(value or "")))
    return {
        "schema": "xarta.pim_email.headers.full_capture.v1",
        "headers": values,
        "header_count": sum(len(items) for items in values.values()),
    }


class _RemoteImageSourceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.sources: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "img":
            return
        attrs_by_name = {str(name or "").lower(): str(value or "") for name, value in attrs}
        source = _canonical_remote_image_url(attrs_by_name.get("src", ""))
        if source and source not in self.sources:
            self.sources.append(source)


def remote_image_sources_from_raw(raw: bytes) -> list[str]:
    msg = BytesParser(policy=policy.default).parsebytes(bytes(raw or b""))
    html_parts: list[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            if part.is_multipart() or part.get_content_maintype() != "text":
                continue
            if part.get_content_subtype() != "html":
                continue
            try:
                html_part = str(part.get_content())
            except Exception:
                payload = part.get_payload(decode=True) or b""
                html_part = payload.decode(part.get_content_charset() or "utf-8", "replace")
            html_parts.append(html_part)
    elif msg.get_content_maintype() == "text" and msg.get_content_subtype() == "html":
        try:
            html_parts.append(str(msg.get_content()))
        except Exception:
            payload = msg.get_payload(decode=True) or b""
            html_parts.append(payload.decode(msg.get_content_charset() or "utf-8", "replace"))
    parser = _RemoteImageSourceParser()
    for html_part in html_parts:
        parser.feed(html_part)
    parser.close()
    return parser.sources


def _download_relpath_for_message(parsed: dict[str, Any]) -> str:
    info = parsed.get("email_uid_info") if isinstance(parsed.get("email_uid_info"), dict) else {}
    relpath = str(info.get("storage_relpath") or "")
    if not relpath:
        email_uid = clean_email_uid(str(parsed.get("email_uid") or ""))
        prefix = email_uid.split("-", 1)[0]
        relpath = (
            f"undated/{email_uid}.eml.enc"
            if prefix == "00000000"
            else f"{prefix[0:4]}/{prefix[4:6]}/{prefix[6:8]}/{email_uid}.eml.enc"
        )
    return relpath


def _download_security_for_message(
    raw: bytes,
    parsed: dict[str, Any],
    *,
    security_mode: str,
) -> tuple[dict[str, Any] | None, str]:
    clean_mode = str(security_mode or "run").strip().lower()
    if clean_mode not in {"run", "require", "required", "sync"}:
        raise EmailConfigError(
            "Email downloader security_mode must run the checker; queue/defer modes are disabled."
        )
    try:
        security = check_email_security_sync(
            raw,
            body_text=str((parsed.get("views") or {}).get("plain") or ""),
        )
        if not security or not security.get("available"):
            raise EmailOperationError("Email security checker did not produce a completed result")
        return security, ""
    except EmailSecurityUnavailableError as exc:
        raise EmailConfigError("Email security checks are unavailable") from exc


def download_mailbox_sync(
    mailbox: EmailMailbox,
    *,
    store: PgEmailStore | None = None,
    run_id: str | None = None,
    apply_remote_moves: bool = False,
    downloaded_folder: str | None = None,
    folder_allowlist: list[str] | None = None,
    limit_per_folder: int | None = None,
    max_messages: int | None = None,
    convergence_passes: int = 2,
    include_special_use: bool = True,
    security_mode: str = "run",
) -> dict[str, Any]:
    store = store or PgEmailStore()
    clean_security_mode = str(security_mode or "run").strip().lower()
    if clean_security_mode not in {"run", "require", "required", "sync"}:
        raise EmailConfigError(
            "Email downloader security_mode must run the checker; queue/defer modes are disabled."
        )
    if not include_special_use:
        raise EmailConfigError(
            "Special-use folders must be downloaded; include_special_use cannot be false"
        )
    target_folder = clean_folder_name(
        downloaded_folder
        or os.environ.get("BLUEPRINTS_EMAIL_DOWNLOADED_FOLDER", DEFAULT_DOWNLOADED_FOLDER)
    )
    allowed_folders = {
        clean_folder_name(folder).lower()
        for folder in (folder_allowlist or [])
        if str(folder or "").strip()
    }
    run_id = str(run_id or "").strip() or _stable_id(
        "email-download-run", mailbox.mailbox_id, str(time.time_ns())
    )
    summary: dict[str, Any] = {
        "schema": "xarta.pim_email.download_run.summary.v1",
        "mailbox_id": mailbox.mailbox_id,
        "apply_remote_moves": bool(apply_remote_moves),
        "downloaded_folder": target_folder,
        "convergence_passes": max(1, int(convergence_passes or 1)),
        "folders_seen": 0,
        "folders_downloaded": 0,
        "target_folder_ignored": 0,
        "planned_messages": 0,
        "processed_messages": 0,
        "stored_messages": 0,
        "security_completed": 0,
        "security_incomplete": 0,
        "sanitized_views_stored": 0,
        "external_image_derivatives_stored": 0,
        "external_image_derivatives_blocked": 0,
        "external_image_derivatives_unavailable": 0,
        "external_image_derivatives_failed": 0,
        "external_image_derivatives_pending": 0,
        "external_image_derivatives_already_stored": 0,
        "external_image_derivatives_already_blocked": 0,
        "external_image_derivatives_already_unavailable": 0,
        "moved_messages": 0,
        "move_not_allowed": 0,
        "move_blocked": 0,
        "move_refused": 0,
        "failed_messages": 0,
        "remote_image_sources_seen": 0,
    }
    _run_store(
        store.record_download_run_start(
            run_id=run_id,
            mailbox_id=mailbox.mailbox_id,
            apply_remote_moves=bool(apply_remote_moves),
            downloaded_folder=target_folder,
            metadata={
                "schema": "xarta.pim_email.download_run.metadata.v1",
                "security_mode": clean_security_mode,
                "include_special_use": include_special_use,
                "folder_allowlist": sorted(allowed_folders),
                "limit_per_folder": limit_per_folder,
                "max_messages": max_messages,
            },
        )
    )
    client = _connect_imap(mailbox)
    status = "completed"
    try:
        folders = _imap_list_folders(client)
        summary["folders_seen"] = len(folders)
        target_ready = True
        if apply_remote_moves:
            target_ready = _ensure_downloaded_folder(client, folders, target_folder)
            if not target_ready:
                _run_store(
                    store.record_download_event(
                        run_id=run_id,
                        event_type="move-target-unavailable",
                        status="warn",
                        mailbox_id=mailbox.mailbox_id,
                        message="Downloaded folder could not be created or verified; moves disabled for this run.",
                        metadata={"target_folder": target_folder},
                    )
                )
        for pass_no in range(1, max(1, int(convergence_passes or 1)) + 1):
            if max_messages is not None and summary["processed_messages"] >= max_messages:
                break
            for folder in folders:
                folder_name = clean_folder_name(str(folder.get("name") or "INBOX"))
                if allowed_folders and folder_name.lower() not in allowed_folders:
                    continue
                folder_status = _imap_folder_status(client, folder_name)
                folder_snapshot = _run_store(
                    store.save_folder_snapshot(
                        mailbox_id=mailbox.mailbox_id,
                        folder=folder,
                        status=folder_status,
                    )
                )
                if _folder_is_download_target(folder_name, target_folder):
                    summary["target_folder_ignored"] += 1
                    _run_store(
                        store.record_download_event(
                            run_id=run_id,
                            event_type="folder-target-ignored",
                            status="ignored",
                            mailbox_id=mailbox.mailbox_id,
                            folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                            folder_name=folder_name,
                            metadata={"pass": pass_no, "target_folder": target_folder},
                        )
                    )
                    continue
                summary["folders_downloaded"] += 1
                move_allowed = bool(
                    apply_remote_moves
                    and target_ready
                    and _folder_move_allowed(folder_snapshot, target_folder)
                )
                _select_imap_folder(client, folder_name, readonly=not move_allowed)
                uids = _imap_uids_newest_first(_imap_uid_search_all(client))
                if limit_per_folder is not None:
                    uids = uids[: max(0, int(limit_per_folder))]
                if max_messages is not None:
                    remaining = max(0, int(max_messages) - int(summary["processed_messages"]))
                    uids = uids[:remaining]
                summary["planned_messages"] += len(uids)
                batch_id = _stable_id(
                    "email-download-batch",
                    run_id,
                    pass_no,
                    folder_snapshot.get("folder_uid"),
                    folder_snapshot.get("uidvalidity"),
                    ",".join(uids),
                )
                _run_store(
                    store.record_download_batch_start(
                        batch_id=batch_id,
                        run_id=run_id,
                        mailbox_id=mailbox.mailbox_id,
                        folder_snapshot=folder_snapshot,
                        uids=uids,
                        metadata={
                            "schema": "xarta.pim_email.download_batch.metadata.v1",
                            "pass": pass_no,
                            "folder_status": folder_status,
                            "move_allowed": move_allowed,
                        },
                    )
                )
                batch_processed = batch_moved = batch_failed = 0
                for uid in uids:
                    try:
                        _run_store(
                            store.record_download_event(
                                run_id=run_id,
                                batch_id=batch_id,
                                mailbox_id=mailbox.mailbox_id,
                                folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                folder_name=folder_name,
                                imap_uid=uid,
                                uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                event_type="message-fetch-start",
                                status="started",
                                message="Starting bounded IMAP fetch for email download.",
                                metadata={
                                    "pass": pass_no,
                                    "move_allowed": move_allowed,
                                    "timeout_seconds": _imap_call_timeout_seconds(),
                                },
                            )
                        )
                        raw, flags = _imap_fetch_raw_for_uid(client, uid)
                        parsed = parse_message(raw, folder=folder_name, uid=uid)
                        relpath = _download_relpath_for_message(parsed)
                        storage = write_encrypted_bytes_atomic(relpath=relpath, content=raw)
                        security, security_error = _download_security_for_message(
                            raw,
                            parsed,
                            security_mode=clean_security_mode,
                        )
                        remote_sources = remote_image_sources_from_raw(raw)
                        summary["remote_image_sources_seen"] += len(remote_sources)
                        metadata = {
                            "schema": "xarta.pim_email.message_download.metadata.v1",
                            "run_id": run_id,
                            "batch_id": batch_id,
                            "pass": pass_no,
                            "raw_size_bytes": len(raw),
                            "flags": flags,
                            "headers": _all_headers_json(raw),
                            "remote_image_sources": remote_sources,
                            "folder_snapshot": folder_snapshot,
                        }
                        _run_store(
                            store.save_downloaded_email(
                                mailbox_id=mailbox.mailbox_id,
                                folder_snapshot=folder_snapshot,
                                imap_uid=uid,
                                flags=flags,
                                raw=raw,
                                parsed=parsed,
                                storage=storage,
                                security=security,
                                security_error=security_error,
                                transformed_assets=[],
                                metadata=metadata,
                            )
                        )
                        email_uid = clean_email_uid(str(parsed.get("email_uid") or ""))
                        raw_sha256 = str(storage.get("raw_sha256") or "")
                        if isinstance(store, PgEmailStore):
                            verified_raw = read_encrypted_bytes(
                                str(storage.get("storage_relpath") or "")
                            )
                            verified_hash = hashlib.sha256(verified_raw).hexdigest()
                        else:
                            verified = _run_store(
                                store.read_local_message(
                                    str(parsed.get("email_uid") or ""),
                                    mailbox_id=mailbox.mailbox_id,
                                )
                            )
                            verified_hash = str(
                                ((verified.get("stored") or {}).get("raw_sha256")) or ""
                            )
                        if verified_hash != str(storage.get("raw_sha256") or ""):
                            raise EmailOperationError("Post-commit local verification failed")
                        summary["stored_messages"] += 1
                        security_complete = False
                        if security and security.get("available"):
                            if not hasattr(store, "completed_security_result"):
                                raise EmailOperationError(
                                    "Store cannot verify completed security result after download"
                                )
                            security_complete = bool(
                                _run_store(
                                    store.completed_security_result(
                                        email_uid=email_uid,
                                        raw_sha256=raw_sha256,
                                    )
                                )
                            )
                            if security_complete:
                                summary["security_completed"] += 1
                            else:
                                summary["security_incomplete"] += 1
                        else:
                            raise EmailOperationError(
                                "Email security checker did not produce a completed result"
                            )
                        sanitized_complete = False
                        if hasattr(store, "current_sanitized_view_artifact") and hasattr(
                            store, "store_sanitized_view_artifact"
                        ):
                            current_sanitized = _run_store(
                                store.current_sanitized_view_artifact(
                                    mailbox_id=mailbox.mailbox_id,
                                    email_uid=email_uid,
                                    raw_sha256=raw_sha256,
                                )
                            )
                            if current_sanitized:
                                sanitized_complete = True
                            else:
                                sanitized_artifact = build_sanitized_view_artifact(
                                    mailbox_id=mailbox.mailbox_id,
                                    email_uid=email_uid,
                                    raw=raw,
                                    raw_sha256=raw_sha256,
                                )
                                _run_store(
                                    store.store_sanitized_view_artifact(artifact=sanitized_artifact)
                                )
                                summary["sanitized_views_stored"] += 1
                                sanitized_complete = True
                        external_derivatives_complete = len(remote_sources) == 0
                        if remote_sources:
                            if not hasattr(store, "process_external_image_derivatives"):
                                raise EmailOperationError(
                                    "Store cannot process external image derivatives"
                                )
                            external_counts = _run_store(
                                store.process_external_image_derivatives(
                                    mailbox_id=mailbox.mailbox_id,
                                    email_uid=email_uid,
                                    input_raw_sha256=raw_sha256,
                                    source_urls=remote_sources,
                                    metadata={"run_id": run_id, "batch_id": batch_id},
                                )
                            )
                            summary["external_image_derivatives_stored"] += int(
                                (external_counts or {}).get("stored") or 0
                            )
                            summary["external_image_derivatives_blocked"] += int(
                                (external_counts or {}).get("blocked") or 0
                            )
                            summary["external_image_derivatives_unavailable"] += int(
                                (external_counts or {}).get("unavailable") or 0
                            )
                            summary["external_image_derivatives_failed"] += int(
                                (external_counts or {}).get("failed") or 0
                            )
                            summary["external_image_derivatives_pending"] += int(
                                (external_counts or {}).get("pending") or 0
                            )
                            summary["external_image_derivatives_already_stored"] += int(
                                (external_counts or {}).get("already_stored") or 0
                            )
                            summary["external_image_derivatives_already_blocked"] += int(
                                (external_counts or {}).get("already_blocked") or 0
                            )
                            summary["external_image_derivatives_already_unavailable"] += int(
                                (external_counts or {}).get("already_unavailable") or 0
                            )
                            handled_external = sum(
                                int((external_counts or {}).get(key) or 0)
                                for key in (
                                    "stored",
                                    "blocked",
                                    "unavailable",
                                    "already_stored",
                                    "already_blocked",
                                    "already_unavailable",
                                )
                            )
                            unique_remote_sources = len(
                                {
                                    _canonical_remote_image_url(source) or str(source or "")
                                    for source in remote_sources
                                }
                                - {""}
                            )
                            external_derivatives_complete = (
                                handled_external >= unique_remote_sources
                            )
                        move_gate = {
                            "raw_encrypted_verified": True,
                            "db_committed": True,
                            "security_completed": security_complete,
                            "sanitized_view_persisted": sanitized_complete,
                            "external_image_derivatives_handled": external_derivatives_complete,
                        }
                        move_ready = move_allowed and all(move_gate.values())
                        moved = False
                        if move_ready:
                            moved = _imap_move_uid(client, uid, target_folder)
                            if moved:
                                _run_store(
                                    store.mark_remote_moved(
                                        mailbox_id=mailbox.mailbox_id,
                                        folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                        uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                        imap_uid=uid,
                                        target_folder=target_folder,
                                    )
                                )
                                summary["moved_messages"] += 1
                                batch_moved += 1
                            else:
                                summary["move_refused"] += 1
                                _run_store(
                                    store.record_download_event(
                                        run_id=run_id,
                                        batch_id=batch_id,
                                        mailbox_id=mailbox.mailbox_id,
                                        folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                        folder_name=folder_name,
                                        email_uid=str(parsed.get("email_uid") or ""),
                                        imap_uid=uid,
                                        uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                        event_type="remote-move-refused",
                                        status="warn",
                                        message="IMAP UID MOVE did not return OK after local verification.",
                                        metadata={"target_folder": target_folder},
                                    )
                                )
                        elif move_allowed:
                            summary["move_blocked"] += 1
                            _run_store(
                                store.record_download_event(
                                    run_id=run_id,
                                    batch_id=batch_id,
                                    mailbox_id=mailbox.mailbox_id,
                                    folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                    folder_name=folder_name,
                                    email_uid=email_uid,
                                    imap_uid=uid,
                                    uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                    event_type="remote-move-gate-blocked",
                                    status="blocked",
                                    message=(
                                        "Remote move blocked until raw, security, sanitized, "
                                        "and external image derivative gates are complete."
                                    ),
                                    metadata={
                                        "target_folder": target_folder,
                                        "move_gate": move_gate,
                                    },
                                )
                            )
                        else:
                            summary["move_not_allowed"] += 1
                        summary["processed_messages"] += 1
                        batch_processed += 1
                        _run_store(
                            store.record_download_event(
                                run_id=run_id,
                                batch_id=batch_id,
                                mailbox_id=mailbox.mailbox_id,
                                folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                folder_name=folder_name,
                                email_uid=str(parsed.get("email_uid") or ""),
                                imap_uid=uid,
                                uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                event_type="message-stored",
                                status="moved" if moved else "stored",
                                message="Email durably stored locally and verified.",
                                metadata={
                                    "raw_sha256": storage.get("raw_sha256"),
                                    "encrypted_size": storage.get("encrypted_size"),
                                    "security_status": "completed"
                                    if security_complete
                                    else "incomplete",
                                    "move_allowed": move_allowed,
                                    "move_ready": move_ready,
                                    "move_gate": move_gate,
                                },
                            )
                        )
                    except Exception as exc:
                        summary["failed_messages"] += 1
                        batch_failed += 1
                        status = "completed-with-errors"
                        _run_store(
                            store.record_download_event(
                                run_id=run_id,
                                batch_id=batch_id,
                                mailbox_id=mailbox.mailbox_id,
                                folder_uid=str(folder_snapshot.get("folder_uid") or ""),
                                folder_name=folder_name,
                                imap_uid=str(uid),
                                uidvalidity=str(folder_snapshot.get("uidvalidity") or ""),
                                event_type="message-failed",
                                status="error",
                                message=str(exc),
                                error_class=exc.__class__.__name__,
                            )
                        )
                        if _is_imap_connection_error(exc):
                            _logout_imap(client)
                            client = _connect_imap(mailbox)
                            _select_imap_folder(client, folder_name, readonly=not move_allowed)
                _run_store(
                    store.record_download_batch_finish(
                        batch_id=batch_id,
                        processed_count=batch_processed,
                        skipped_count=0,
                        moved_count=batch_moved,
                        failed_count=batch_failed,
                        metadata={"pass": pass_no},
                    )
                )
    except Exception:
        status = "failed"
        raise
    finally:
        _logout_imap(client)
        _run_store(store.record_download_run_finish(run_id=run_id, status=status, summary=summary))
    return {"ok": status != "failed", "run_id": run_id, "status": status, "summary": summary}


def fetch_message_sync(
    mailbox: EmailMailbox,
    *,
    folder: str,
    uid: str,
    security_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    clean_folder = clean_folder_name(folder)
    clean_uid = clean_uid_value(uid)
    client = _connect_imap(mailbox)
    try:
        _select_imap_folder(client, clean_folder)
        status, fetch_data = client.uid("fetch", clean_uid, "(RFC822)")
        if status != "OK":
            raise EmailOperationError("IMAP message fetch failed")
        raw = _first_fetch_bytes(fetch_data)
        if not raw:
            raise EmailOperationError("IMAP message body was empty")
        message = parse_message(raw, folder=clean_folder, uid=clean_uid)
        try:
            message["security"] = check_email_security_sync(
                raw,
                body_text=str((message.get("views") or {}).get("plain") or ""),
                progress_callback=security_progress_callback,
            )
        except EmailSecurityUnavailableError as exc:
            raise EmailConfigError(
                "Email security checks are unavailable, so message viewing is blocked"
            ) from exc
        return message
    finally:
        _logout_imap(client)


def fetch_message_security_sync(
    mailbox: EmailMailbox,
    *,
    folder: str,
    uid: str,
    security_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    clean_folder = clean_folder_name(folder)
    clean_uid = clean_uid_value(uid)
    client = _connect_imap(mailbox)
    try:
        _select_imap_folder(client, clean_folder)
        status, fetch_data = client.uid("fetch", clean_uid, "(RFC822)")
        if status != "OK":
            raise EmailOperationError("IMAP message fetch failed")
        raw = _first_fetch_bytes(fetch_data)
        if not raw:
            raise EmailOperationError("IMAP message body was empty")
        parsed = parse_message(raw, folder=clean_folder, uid=clean_uid)
        try:
            return check_email_security_sync(
                raw,
                body_text=str((parsed.get("views") or {}).get("plain") or ""),
                progress_callback=security_progress_callback,
            )
        except EmailSecurityUnavailableError as exc:
            raise EmailConfigError(
                "Email security checks are unavailable, so message viewing is blocked"
            ) from exc
    finally:
        _logout_imap(client)


def clean_folder_name(value: str) -> str:
    clean = str(value or "INBOX").strip() or "INBOX"
    if len(clean) > 180 or any(ch in clean for ch in ("\r", "\n", "\x00")):
        raise EmailOperationError("Invalid IMAP folder name")
    return clean


def _imap_mailbox_select_arg(folder: str) -> str:
    clean = clean_folder_name(folder)
    if re.fullmatch(r"[A-Za-z0-9._/-]+", clean):
        return clean
    escaped = clean.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _select_imap_folder(client: imaplib.IMAP4, folder: str, *, readonly: bool = True) -> None:
    try:
        status, _ = client.select(_imap_mailbox_select_arg(folder), readonly=readonly)
    except imaplib.IMAP4.error as exc:
        raise EmailOperationError("IMAP folder select failed") from exc
    if status != "OK":
        raise EmailOperationError("IMAP folder select failed")


def clean_uid_value(value: str) -> str:
    clean = str(value or "").strip()
    if not re.fullmatch(r"[0-9]+", clean):
        raise EmailOperationError("Invalid IMAP UID")
    return clean


def _imap_uids_newest_first(uids: list[str]) -> list[str]:
    return sorted((clean_uid_value(uid) for uid in uids), key=int, reverse=True)


def _first_fetch_bytes(fetch_data: Any) -> bytes:
    for item in fetch_data or []:
        if isinstance(item, tuple):
            for part in item:
                if isinstance(part, bytes) and b"\r\n" in part:
                    return part
        if isinstance(item, bytes) and b"\r\n" in item:
            return item
    return b""


def parse_message(raw: bytes, *, folder: str = "INBOX", uid: str = "") -> dict[str, Any]:
    msg = BytesParser(policy=policy.default).parsebytes(raw)
    email_uid_info = generate_email_uid_info(raw)
    plain = _first_text_part(msg, "plain")
    raw_markdown = _first_text_part(msg, "markdown")
    raw_html = _first_text_part(msg, "html")
    html_result = (
        sanitize_email_html_with_report(raw_html, inline_images=_inline_image_sources(msg))
        if raw_html
        else EmailHtmlSanitizeResult("")
    )
    markdown_text = text_to_markdown(raw_markdown) if raw_markdown.strip() else ""
    default_text = (
        plain.strip() if plain.strip() else markdown_text or html_to_text(html_result.html)
    )
    markdown = markdown_text if markdown_text else text_to_markdown(default_text)
    return {
        "uid": uid,
        "folder": folder,
        "email_uid": email_uid_info["email_uid"],
        "email_uid_info": email_uid_info,
        "headers": {
            "subject": _message_header_value(msg, "subject"),
            "from": _message_header_value(msg, "from"),
            "to": _message_header_value(msg, "to"),
            "date": _message_header_value(msg, "date"),
            "message_id": _message_header_value(msg, "message-id"),
        },
        "views": {
            "plain": default_text,
            "html": html_result.html,
            "markdown": markdown,
            "raw": safe_raw_email_view(raw),
        },
        "views_available": {
            "plain": bool(default_text),
            "html": bool(html_result.html),
            "markdown": bool(markdown_text),
            "raw": bool(raw),
        },
        "html_security": html_result.public_dict(),
        "attachments": _attachment_summaries(msg),
    }


def _decode_header_value(value: str | None) -> str:
    try:
        decoded = str(make_header(decode_header(value or "")))
    except Exception:
        decoded = str(value or "")
    return decoded.encode("utf-8", "replace").decode("utf-8")


def _raw_message_header_values(message: Message, name: str) -> list[str]:
    clean = str(name or "").lower()
    return [
        str(value or "")
        for header_name, value in message.raw_items()
        if str(header_name or "").lower() == clean
    ]


def _message_header_value(message: Message, name: str) -> str:
    values = [
        decoded
        for value in _raw_message_header_values(message, name)
        if (decoded := _decode_header_value(value))
    ]
    return ", ".join(values)


def _first_text_part(message: Message, subtype: str) -> str:
    if message.is_multipart():
        for part in message.walk():
            if part.is_multipart():
                continue
            disposition = (part.get_content_disposition() or "").lower()
            if disposition == "attachment":
                continue
            if part.get_content_maintype() == "text" and part.get_content_subtype() == subtype:
                try:
                    return str(part.get_content())
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, "replace")
        return ""
    if message.get_content_maintype() == "text" and message.get_content_subtype() == subtype:
        try:
            return str(message.get_content())
        except Exception:
            payload = message.get_payload(decode=True) or b""
            return payload.decode(message.get_content_charset() or "utf-8", "replace")
    return ""


def safe_raw_email_view(raw: bytes) -> str:
    """Return a persisted-safe source view with HTML sanitized and binary bodies omitted."""
    msg = BytesParser(policy=policy.default).parsebytes(bytes(raw or b""))
    lines: list[str] = []
    _append_raw_headers(lines, msg)
    lines.append("")
    if msg.is_multipart():
        _append_raw_multipart_payload(lines, msg)
    else:
        _append_raw_part_payload(lines, msg)
    return "\n".join(lines).replace("\r\n", "\n").replace("\r", "\n").strip("\n")


def _append_raw_headers(lines: list[str], message: Message) -> None:
    for name, value in message.raw_items():
        clean_value = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
        value_lines = clean_value.split("\n") or [""]
        lines.append(f"{name}: {value_lines[0]}")
        lines.extend(f" {line}" for line in value_lines[1:])


def _message_parts(message: Message) -> list[Message]:
    payload = message.get_payload()
    return list(payload) if isinstance(payload, list) else []


def _append_raw_multipart_payload(lines: list[str], message: Message) -> None:
    boundary = message.get_boundary() or "xarta-safe-raw-boundary"
    for part in _message_parts(message):
        lines.append(f"--{boundary}")
        _append_raw_headers(lines, part)
        lines.append("")
        if part.is_multipart():
            _append_raw_multipart_payload(lines, part)
        else:
            _append_raw_part_payload(lines, part)
    lines.append(f"--{boundary}--")


def _append_raw_part_payload(lines: list[str], part: Message) -> None:
    if not _raw_view_part_can_show_payload(part):
        lines.append(_raw_view_omitted_part_marker(part))
        return
    text = _raw_view_text_payload(part)
    if part.get_content_type().lower() == "text/html":
        text = sanitize_email_html_with_report(text).html
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if len(text) > MAX_RAW_VIEW_TEXT_CHARS:
        text = (
            text[:MAX_RAW_VIEW_TEXT_CHARS]
            + f"\n[xarta raw view truncated text part at {MAX_RAW_VIEW_TEXT_CHARS} characters]"
        )
    lines.extend(text.split("\n"))


def _raw_view_text_payload(part: Message) -> str:
    try:
        return str(part.get_content())
    except Exception:
        payload = part.get_payload(decode=True)
        if payload is None:
            payload = part.get_payload(decode=False)
        if isinstance(payload, bytes):
            return payload.decode(part.get_content_charset() or "utf-8", "replace")
        return str(payload or "")


def _raw_view_part_can_show_payload(part: Message) -> bool:
    disposition = (part.get_content_disposition() or "").lower()
    if disposition == "attachment" or part.get_filename():
        return False
    return part.get_content_maintype() == "text"


def _raw_view_payload_size(part: Message) -> int:
    decoded = part.get_payload(decode=True)
    if decoded is not None:
        return len(decoded)
    payload = part.get_payload(decode=False)
    if isinstance(payload, bytes):
        return len(payload)
    return len(str(payload or "").encode("utf-8", "replace"))


def _raw_view_omitted_part_marker(part: Message) -> str:
    filename = _decode_header_value(part.get_filename("") or "")
    disposition = (part.get_content_disposition() or "").lower() or "inline"
    fields = [
        f"content_type={part.get_content_type()}",
        f"disposition={disposition}",
        f"bytes={_raw_view_payload_size(part)}",
    ]
    if filename:
        fields.append(f"filename={filename}")
    return f"[xarta raw view omitted MIME part body: {'; '.join(fields)}]"


def _attachment_summaries(message: Message) -> list[dict[str, str]]:
    attachments: list[dict[str, str]] = []
    for part in message.walk() if message.is_multipart() else []:
        if (part.get_content_disposition() or "").lower() != "attachment":
            continue
        filename = _decode_header_value(part.get_filename("") or "")
        attachments.append(
            {
                "filename": filename,
                "content_type": part.get_content_type(),
            }
        )
    return attachments


def _normalize_cid(value: str | None) -> str:
    clean = str(value or "").strip().strip("<>").strip()
    if clean.lower().startswith("cid:"):
        clean = clean[4:]
    return unquote(clean).strip().lower()


def _inline_image_sources(message: Message) -> dict[str, str]:
    sources: dict[str, str] = {}
    parts = message.walk() if message.is_multipart() else [message]
    for part in parts:
        if part.is_multipart() or part.get_content_maintype() != "image":
            continue
        content_type = str(part.get_content_type() or "").lower()
        if content_type not in SAFE_INLINE_IMAGE_TYPES:
            continue
        cid = _normalize_cid(part.get("content-id", ""))
        if not cid:
            continue
        payload = part.get_payload(decode=True) or b""
        if not payload or len(payload) > MAX_INLINE_IMAGE_BYTES:
            continue
        try:
            jpeg = transform_image_to_jpeg(payload)
        except EmailOperationError:
            continue
        sources[cid] = f"data:image/jpeg;base64,{base64.b64encode(jpeg).decode('ascii')}"
    return sources


def _image_proxy_secret() -> bytes:
    secret = (
        os.environ.get("BLUEPRINTS_EMAIL_IMAGE_PROXY_SECRET", "")
        or os.environ.get("BLUEPRINTS_API_SECRET", "")
        or os.environ.get("BLUEPRINTS_SYNC_SECRET", "")
    ).strip()
    return secret.encode("utf-8")


def _canonical_remote_image_url(source: str) -> str:
    clean = str(source or "").strip()
    if not clean or len(clean) > 4096 or re.search(r"[\x00-\x20]", clean):
        return ""
    if clean.startswith("//"):
        clean = f"https:{clean}"
    parsed = urlparse(clean)
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    if parsed.username or parsed.password or not parsed.hostname:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and port not in {80, 443}:
        return ""
    try:
        host = parsed.hostname.encode("idna").decode("ascii").lower()
    except UnicodeError:
        return ""
    scheme = parsed.scheme.lower()
    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    path = parsed.path or "/"
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def sign_email_image_url(source: str) -> str:
    canonical = _canonical_remote_image_url(source)
    secret = _image_proxy_secret()
    if not canonical or not secret:
        return ""
    return hmac.new(secret, canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_email_image_signature(source: str, signature: str) -> bool:
    expected = sign_email_image_url(source)
    clean = str(signature or "").strip()
    return bool(expected and clean and hmac.compare_digest(expected, clean))


def email_image_proxy_path(source: str) -> str:
    canonical = _canonical_remote_image_url(source)
    signature = sign_email_image_url(canonical)
    if not canonical or not signature:
        return ""
    return f"{EMAIL_IMAGE_PROXY_PATH}?src={quote(canonical, safe='')}&sig={signature}"


def _blocked_proxy_ip(address: str) -> bool:
    try:
        ip = ipaddress.ip_address(address)
    except ValueError:
        return True
    return any(
        (
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        )
    )


def _assert_public_remote_image_url(source: str) -> str:
    canonical = _canonical_remote_image_url(source)
    if not canonical:
        raise EmailOperationError("image URL is not an allowed http(s) URL")
    parsed = urlparse(canonical)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = socket.getaddrinfo(parsed.hostname, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise EmailOperationError("image host could not be resolved") from exc
    resolved = {item[4][0] for item in addresses}
    if not resolved or any(_blocked_proxy_ip(address) for address in resolved):
        raise EmailOperationError("image host resolved to a private or unsafe address")
    return canonical


def _remote_image_max_bytes() -> int:
    raw_value = os.environ.get("BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_BYTES", "").strip()
    if not raw_value:
        return DEFAULT_REMOTE_IMAGE_MAX_BYTES
    try:
        parsed = int(raw_value)
    except ValueError:
        return DEFAULT_REMOTE_IMAGE_MAX_BYTES
    return max(1 * 1024 * 1024, parsed)


def transform_image_to_jpeg(content: bytes) -> bytes:
    if not content or len(content) > _remote_image_max_bytes():
        raise EmailOperationError("image payload is empty or too large")
    try:
        with Image.open(BytesIO(content)) as opened:
            opened.seek(0)
            image = ImageOps.exif_transpose(opened)
            if image.mode in {"RGBA", "LA", "P"}:
                rgba = image.convert("RGBA")
                flattened = Image.new("RGB", rgba.size, (255, 255, 255))
                flattened.paste(rgba, mask=rgba.getchannel("A"))
                image = flattened
            elif image.mode != "RGB":
                image = image.convert("RGB")
            image.thumbnail(MAX_IMAGE_DIMENSIONS, Image.Resampling.LANCZOS)
            output = BytesIO()
            image.save(output, format="JPEG", quality=85, optimize=True, progressive=True)
            return output.getvalue()
    except (OSError, UnidentifiedImageError, ValueError, Image.DecompressionBombError) as exc:
        raise EmailOperationError("image could not be decoded safely") from exc


def fetch_remote_image_as_jpeg_sync(source: str) -> bytes:
    fetched = fetch_remote_image_bytes_sync(source)
    return transform_image_to_jpeg(bytes(fetched["content"]))


def fetch_remote_image_bytes_sync(source: str) -> dict[str, Any]:
    current = _assert_public_remote_image_url(source)
    max_image_bytes = _remote_image_max_bytes()
    max_redirects = max(
        1,
        int(
            os.environ.get(
                "BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_REDIRECTS",
                str(DEFAULT_REMOTE_IMAGE_MAX_REDIRECTS),
            )
        ),
    )
    headers = {
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/gif,image/*;q=0.8",
        "User-Agent": "BlueprintsEmailImageProxy/1.0",
    }
    try:
        with httpx.Client(
            follow_redirects=False, timeout=httpx.Timeout(8.0, connect=4.0)
        ) as client:
            for _ in range(max_redirects):
                current = _assert_public_remote_image_url(current)
                with client.stream("GET", current, headers=headers) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        location = response.headers.get("location", "").strip()
                        if not location:
                            raise EmailOperationError("image redirect was missing a location")
                        current = urljoin(current, location)
                        continue
                    if response.status_code >= 400:
                        raise EmailOperationError(f"image unavailable: HTTP {response.status_code}")
                    content_type = (
                        response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                    )
                    if content_type and not content_type.startswith("image/"):
                        raise EmailOperationError("image fetch did not return an image")
                    chunks: list[bytes] = []
                    total = 0
                    for chunk in response.iter_bytes():
                        total += len(chunk)
                        if total > max_image_bytes:
                            raise EmailOperationError("image payload is too large")
                        chunks.append(chunk)
                    return {
                        "content": b"".join(chunks),
                        "content_type": content_type or "image/*",
                        "final_url": current,
                    }
            raise EmailOperationError(
                f"image unavailable: redirect chain exceeded {max_redirects} redirects"
            )
    except httpx.RequestError as exc:
        raise EmailOperationError(f"image unavailable: {exc.__class__.__name__}") from exc


def _external_image_error_status(exc: BaseException) -> str:
    text = str(exc).lower()
    if (
        "private or unsafe" in text
        or "not an allowed http" in text
        or "could not be decoded safely" in text
        or "payload is empty or too large" in text
        or "payload is too large" in text
        or "decompression" in text
    ):
        return "blocked"
    if (
        "timeout" in text
        or "connect" in text
        or "http 429" in text
        or re.search(r"http 5\d\d", text)
    ):
        return "pending"
    if (
        "unavailable" in text
        or "could not be resolved" in text
        or "http 404" in text
        or "http 410" in text
        or "redirect chain" in text
        or "did not return an image" in text
    ):
        return "unavailable"
    return "failed"


def _external_image_existing_state_is_terminal(status: str, reason: str = "") -> bool:
    clean_status = str(status or "").strip().lower()
    if clean_status in {"stored", "blocked"}:
        return True
    if clean_status == "unavailable":
        return _external_image_error_status(EmailOperationError(reason)) != "pending"
    return False


async def fetch_remote_image_as_jpeg(source: str) -> bytes:
    return await asyncio.to_thread(fetch_remote_image_as_jpeg_sync, source)


async def fetch_remote_image_bytes(source: str) -> dict[str, Any]:
    return await asyncio.to_thread(fetch_remote_image_bytes_sync, source)


class _SafeHtmlParser(HTMLParser):
    allowed_tags = {
        "a",
        "p",
        "div",
        "span",
        "br",
        "blockquote",
        "ul",
        "ol",
        "li",
        "strong",
        "b",
        "em",
        "i",
        "u",
        "pre",
        "code",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
        "img",
    }
    skip_tags = {"script", "style", "iframe", "object", "embed", "svg", "math"}

    def __init__(self, *, inline_images: dict[str, str] | None = None) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0
        self.inline_images = inline_images or {}
        self.remote_images_blocked = 0
        self.remote_images_proxied = 0
        self.tracking_images_blocked = 0
        self.inline_images_rendered = 0
        self.inline_images_blocked = 0
        self.active_content_blocked = 0
        self.unsafe_links_blocked = 0
        self.allowed_links = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in self.skip_tags:
            self.skip_depth += 1
            self.active_content_blocked += 1
            return
        if self.skip_depth or tag not in self.allowed_tags:
            return
        if tag == "img":
            self._append_img(attrs)
            return
        safe_attrs = self._safe_attrs(tag, attrs)
        attr_text = "".join(
            f' {name}="{html.escape(value, quote=True)}"' for name, value in safe_attrs
        )
        self.parts.append(f"<{tag}{attr_text}>")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in self.skip_tags and self.skip_depth:
            self.skip_depth -= 1
            return
        if self.skip_depth or tag not in self.allowed_tags or tag in {"br", "img"}:
            return
        self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self.skip_depth:
            return
        self.parts.append(html.escape(data, quote=False))

    def handle_entityref(self, name: str) -> None:
        if not self.skip_depth:
            self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if not self.skip_depth:
            self.parts.append(f"&#{name};")

    def _safe_attrs(self, tag: str, attrs: list[tuple[str, str | None]]) -> list[tuple[str, str]]:
        safe: list[tuple[str, str]] = []
        for name, value in attrs:
            clean_name = str(name or "").lower()
            clean_value = str(value or "")
            if clean_name.startswith("on") or clean_name in {"style", "srcset", "src"}:
                continue
            if tag == "a" and clean_name == "href":
                lowered = clean_value.strip().lower()
                if lowered.startswith(("https://", "http://", "mailto:")):
                    safe.append(("href", clean_value.strip()))
                    safe.append(("rel", "noreferrer noopener"))
                    safe.append(("target", "_blank"))
                    self.allowed_links += 1
                else:
                    self.unsafe_links_blocked += 1
                continue
            if clean_name in {"title", "colspan", "rowspan"}:
                safe.append((clean_name, clean_value[:160]))
        return safe

    def _append_img(self, attrs: list[tuple[str, str | None]]) -> None:
        attrs_by_name = {str(name or "").lower(): str(value or "") for name, value in attrs}
        source = attrs_by_name.get("src", "").strip()
        alt = attrs_by_name.get("alt", "").strip()[:160]
        title = attrs_by_name.get("title", "").strip()[:160]
        original = _canonical_remote_image_url(source)
        safe_source = self._safe_image_source(source, attrs_by_name)
        if not safe_source:
            label = alt or "Remote image blocked"
            self.parts.append(
                '<span class="email-image-blocked" title="Remote image blocked">'
                f"{html.escape(label, quote=False)}</span>"
            )
            return
        safe_attrs: list[tuple[str, str]] = [("src", safe_source), ("alt", alt)]
        if title:
            safe_attrs.append(("title", title))
        for dim in ("width", "height"):
            value = attrs_by_name.get(dim, "").strip()
            if re.fullmatch(r"[0-9]{1,4}", value):
                safe_attrs.append((dim, value))
        attr_text = "".join(
            f' {name}="{html.escape(value, quote=True)}"' for name, value in safe_attrs
        )
        image_html = f"<img{attr_text}>"
        if original:
            self.parts.append(
                '<span class="email-image-wrap">'
                f"{image_html}"
                f'<a class="email-image-original" href="{html.escape(original, quote=True)}" '
                'rel="noreferrer noopener">original</a>'
                "</span>"
            )
        else:
            self.parts.append(image_html)

    def _safe_image_source(self, source: str, attrs: dict[str, str]) -> str:
        clean = str(source or "").strip()
        lowered = clean.lower()
        if not clean:
            self.inline_images_blocked += 1
            return ""
        if lowered.startswith(("http://", "https://", "//")):
            if _looks_like_tracking_image(clean, attrs):
                self.tracking_images_blocked += 1
            proxy = email_image_proxy_path(clean)
            if proxy:
                self.remote_images_proxied += 1
                return proxy
            self.remote_images_blocked += 1
            return ""
        if lowered.startswith("cid:"):
            resolved = self.inline_images.get(_normalize_cid(clean), "")
            if resolved:
                self.inline_images_rendered += 1
                return resolved
            self.inline_images_blocked += 1
            return ""
        if _is_safe_data_image(clean):
            self.inline_images_rendered += 1
            return clean
        self.inline_images_blocked += 1
        return ""


def _looks_like_tracking_image(source: str, attrs: dict[str, str]) -> bool:
    parsed = urlparse(source if not source.startswith("//") else f"https:{source}")
    haystack = " ".join(
        [
            parsed.netloc,
            parsed.path,
            parsed.query,
            attrs.get("alt", ""),
            attrs.get("title", ""),
        ]
    ).lower()
    if any(
        term in haystack
        for term in ("track", "tracker", "pixel", "beacon", "open", "analytics", "click")
    ):
        return True
    return attrs.get("width", "").strip() == "1" and attrs.get("height", "").strip() == "1"


def _is_safe_data_image(source: str) -> bool:
    match = re.match(r"^data:(image/[a-z0-9.+-]+);base64,(.*)$", source, re.I | re.S)
    if not match:
        return False
    content_type = match.group(1).lower()
    if content_type == "image/jpg":
        content_type = "image/jpeg"
    if content_type not in SAFE_INLINE_IMAGE_TYPES:
        return False
    payload = re.sub(r"\s+", "", match.group(2))
    return bool(payload) and re.fullmatch(r"[A-Za-z0-9+/=]+", payload) is not None


def sanitize_email_html_with_report(
    value: str,
    *,
    inline_images: dict[str, str] | None = None,
) -> EmailHtmlSanitizeResult:
    parser = _SafeHtmlParser(inline_images=inline_images)
    parser.feed(value or "")
    parser.close()
    return EmailHtmlSanitizeResult(
        html="".join(parser.parts).strip(),
        remote_images_blocked=parser.remote_images_blocked,
        remote_images_proxied=parser.remote_images_proxied,
        tracking_images_blocked=parser.tracking_images_blocked,
        inline_images_rendered=parser.inline_images_rendered,
        inline_images_blocked=parser.inline_images_blocked,
        active_content_blocked=parser.active_content_blocked,
        unsafe_links_blocked=parser.unsafe_links_blocked,
        allowed_links=parser.allowed_links,
    )


def sanitize_email_html(value: str) -> str:
    return sanitize_email_html_with_report(value).html


class _HtmlTextParser(HTMLParser):
    block_tags = {"p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_text_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if self.skip_text_depth:
            self.skip_text_depth += 1
            return
        attrs_by_name = {str(name or "").lower(): str(value or "") for name, value in attrs}
        class_names = set(attrs_by_name.get("class", "").lower().split())
        if tag == "a" and "email-image-original" in class_names:
            self.skip_text_depth = 1
            return
        if tag == "li":
            self.parts.append("\n- ")

    def handle_endtag(self, tag: str) -> None:
        if self.skip_text_depth:
            self.skip_text_depth -= 1
            return
        if tag.lower() in self.block_tags:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self.skip_text_depth:
            return
        self.parts.append(data)


def html_to_text(value: str) -> str:
    parser = _HtmlTextParser()
    parser.feed(value or "")
    parser.close()
    text = "".join(parser.parts)
    text = re.sub(r"[ \t]+", " ", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def text_to_markdown(value: str) -> str:
    text = (value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return ""
    lines = [line.rstrip() for line in text.split("\n")]
    return "\n".join(lines).strip()


async def list_folders(mailbox: EmailMailbox) -> list[dict[str, Any]]:
    return await asyncio.to_thread(list_folders_sync, mailbox)


async def list_inbox(mailbox: EmailMailbox, *, limit: int = 25) -> list[dict[str, Any]]:
    return await asyncio.to_thread(list_inbox_sync, mailbox, limit=limit)


async def list_folder_messages(
    mailbox: EmailMailbox, *, folder: str = "INBOX", limit: int = 25
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(list_folder_messages_sync, mailbox, folder=folder, limit=limit)


async def fetch_message(
    mailbox: EmailMailbox,
    *,
    folder: str,
    uid: str,
    security_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        fetch_message_sync,
        mailbox,
        folder=folder,
        uid=uid,
        security_progress_callback=security_progress_callback,
    )


async def fetch_message_security(
    mailbox: EmailMailbox,
    *,
    folder: str,
    uid: str,
    security_progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        fetch_message_security_sync,
        mailbox,
        folder=folder,
        uid=uid,
        security_progress_callback=security_progress_callback,
    )


async def download_mailbox(
    mailbox: EmailMailbox,
    *,
    store: PgEmailStore | None = None,
    run_id: str | None = None,
    apply_remote_moves: bool = False,
    downloaded_folder: str | None = None,
    folder_allowlist: list[str] | None = None,
    limit_per_folder: int | None = None,
    max_messages: int | None = None,
    convergence_passes: int = 2,
    include_special_use: bool = True,
    security_mode: str = "run",
) -> dict[str, Any]:
    return await asyncio.to_thread(
        download_mailbox_sync,
        mailbox,
        store=store,
        run_id=run_id,
        apply_remote_moves=apply_remote_moves,
        downloaded_folder=downloaded_folder,
        folder_allowlist=folder_allowlist,
        limit_per_folder=limit_per_folder,
        max_messages=max_messages,
        convergence_passes=convergence_passes,
        include_special_use=include_special_use,
        security_mode=security_mode,
    )


def smtp_self_send_sync(mailbox: EmailMailbox, *, recipient: str) -> dict[str, Any]:
    clean_recipient = str(recipient or "").strip().lower()
    account = mailbox.email_address.strip().lower()
    if clean_recipient != account:
        raise EmailOperationError("SMTP proof is limited to self-send for this mailbox")
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    msg = EmailMessage()
    msg["From"] = mailbox.email_address
    msg["To"] = mailbox.email_address
    msg["Subject"] = f"Blueprints PIM Email SMTP self-test {now}"
    msg.set_content(
        "Blueprints PIM Email SMTP self-test.\n"
        "This route only sends from the configured mailbox to itself.\n"
    )
    context = ssl.create_default_context()
    if mailbox.smtp_ssl:
        with smtplib.SMTP_SSL(
            mailbox.smtp_host, mailbox.smtp_port, context=context, timeout=30
        ) as client:
            client.login(mailbox.email_address, mailbox.password)
            client.send_message(msg)
    else:
        with smtplib.SMTP(mailbox.smtp_host, mailbox.smtp_port, timeout=30) as client:
            if mailbox.smtp_starttls:
                client.starttls(context=context)
            client.login(mailbox.email_address, mailbox.password)
            client.send_message(msg)
    return {"ok": True, "recipient": mailbox.email_address, "sent_at": now}


async def smtp_self_send(mailbox: EmailMailbox, *, recipient: str) -> dict[str, Any]:
    return await asyncio.to_thread(smtp_self_send_sync, mailbox, recipient=recipient)
