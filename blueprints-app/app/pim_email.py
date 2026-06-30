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
import re
import secrets
import smtplib
import socket
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser
from html.parser import HTMLParser
from io import BytesIO
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlparse, urlunparse

import asyncpg
import httpx
from PIL import Image, ImageOps, UnidentifiedImageError

DEFAULT_MAILBOX_ID = "default"
DEFAULT_IMAP_PORT = 993
DEFAULT_SMTP_PORT = 465
ENVELOPE_PURPOSE = b"xarta-pim-email-password-v1"
MAX_INLINE_IMAGE_BYTES = 2 * 1024 * 1024
MAX_REMOTE_IMAGE_BYTES = 5 * 1024 * 1024
MAX_IMAGE_PIXELS = 12_000_000
MAX_IMAGE_DIMENSIONS = (1800, 2400)
EMAIL_IMAGE_PROXY_PATH = "/api/v1/personal/email/image-proxy"
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
    while sum(len(chunk) for chunk in chunks) < length:
        counter_bytes = counter.to_bytes(4, "big")
        chunks.append(hashlib.sha256(key + nonce + counter_bytes).digest())
        counter += 1
    return b"".join(chunks)[:length]


def encrypt_password(password: str, *, key: str | None = None) -> str:
    key_bytes = _credential_key_bytes(key)
    nonce = secrets.token_bytes(16)
    plaintext = password.encode("utf-8")
    stream = _keystream(key_bytes, nonce, len(plaintext))
    ciphertext = bytes(a ^ b for a, b in zip(plaintext, stream))
    mac = hmac.new(key_bytes, ENVELOPE_PURPOSE + nonce + ciphertext, hashlib.sha256).digest()
    return json.dumps(
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


class PgEmailStore:
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = (
            dsn if dsn is not None else os.environ.get("BLUEPRINTS_EMAIL_POSTGRES_DSN", "")
        ).strip()
        if not self.dsn:
            raise EmailConfigError("BLUEPRINTS_EMAIL_POSTGRES_DSN is not configured")

    async def _connect(self) -> asyncpg.Connection:
        return await asyncpg.connect(self.dsn)

    async def ensure_schema(self) -> None:
        conn = await self._connect()
        try:
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
        finally:
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


def _mailbox_row_public(row: Any) -> dict[str, Any]:
    updated = row.get("updated_at") if hasattr(row, "get") else row["updated_at"]
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
    client = imaplib.IMAP4_SSL(mailbox.imap_host, mailbox.imap_port)
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
        status, _ = client.select(clean_folder, readonly=True)
        if status != "OK":
            raise EmailOperationError("IMAP folder select failed")
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
            messages.append(
                {
                    "uid": uid.decode("ascii", "replace"),
                    "folder": clean_folder,
                    "subject": _decode_header_value(msg.get("subject", "")),
                    "from": _decode_header_value(msg.get("from", "")),
                    "date": _decode_header_value(msg.get("date", "")),
                    "message_id": _decode_header_value(msg.get("message-id", "")),
                }
            )
        return messages
    finally:
        _logout_imap(client)


def list_inbox_sync(mailbox: EmailMailbox, *, limit: int = 25) -> list[dict[str, Any]]:
    return list_folder_messages_sync(mailbox, folder="INBOX", limit=limit)


def fetch_message_sync(mailbox: EmailMailbox, *, folder: str, uid: str) -> dict[str, Any]:
    clean_folder = clean_folder_name(folder)
    clean_uid = clean_uid_value(uid)
    client = _connect_imap(mailbox)
    try:
        status, _ = client.select(clean_folder, readonly=True)
        if status != "OK":
            raise EmailOperationError("IMAP folder select failed")
        status, fetch_data = client.uid("fetch", clean_uid, "(RFC822)")
        if status != "OK":
            raise EmailOperationError("IMAP message fetch failed")
        raw = _first_fetch_bytes(fetch_data)
        if not raw:
            raise EmailOperationError("IMAP message body was empty")
        return parse_message(raw, folder=clean_folder, uid=clean_uid)
    finally:
        _logout_imap(client)


def clean_folder_name(value: str) -> str:
    clean = str(value or "INBOX").strip() or "INBOX"
    if len(clean) > 180 or any(ch in clean for ch in ("\r", "\n", "\x00")):
        raise EmailOperationError("Invalid IMAP folder name")
    return clean


def clean_uid_value(value: str) -> str:
    clean = str(value or "").strip()
    if not re.fullmatch(r"[0-9]+", clean):
        raise EmailOperationError("Invalid IMAP UID")
    return clean


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
    plain = _first_text_part(msg, "plain")
    raw_html = _first_text_part(msg, "html")
    html_result = (
        sanitize_email_html_with_report(raw_html, inline_images=_inline_image_sources(msg))
        if raw_html
        else EmailHtmlSanitizeResult("")
    )
    default_text = plain.strip() if plain.strip() else html_to_text(html_result.html)
    markdown = text_to_markdown(default_text)
    return {
        "uid": uid,
        "folder": folder,
        "headers": {
            "subject": _decode_header_value(msg.get("subject", "")),
            "from": _decode_header_value(msg.get("from", "")),
            "to": _decode_header_value(msg.get("to", "")),
            "date": _decode_header_value(msg.get("date", "")),
            "message_id": _decode_header_value(msg.get("message-id", "")),
        },
        "views": {
            "plain": default_text,
            "html": html_result.html,
            "markdown": markdown,
        },
        "html_security": html_result.public_dict(),
        "attachments": _attachment_summaries(msg),
    }


def _decode_header_value(value: str | None) -> str:
    try:
        return str(make_header(decode_header(value or "")))
    except Exception:
        return str(value or "")


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


def transform_image_to_jpeg(content: bytes) -> bytes:
    if not content or len(content) > MAX_REMOTE_IMAGE_BYTES:
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
    current = _assert_public_remote_image_url(source)
    headers = {
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/gif,image/*;q=0.8",
        "User-Agent": "BlueprintsEmailImageProxy/1.0",
    }
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(8.0, connect=4.0)) as client:
        for _ in range(4):
            current = _assert_public_remote_image_url(current)
            with client.stream("GET", current, headers=headers) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("location", "").strip()
                    if not location:
                        raise EmailOperationError("image redirect was missing a location")
                    current = urljoin(current, location)
                    continue
                if response.status_code >= 400:
                    raise EmailOperationError(
                        f"image fetch failed with HTTP {response.status_code}"
                    )
                content_type = (
                    response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                )
                if content_type and not content_type.startswith("image/"):
                    raise EmailOperationError("image fetch did not return an image")
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    total += len(chunk)
                    if total > MAX_REMOTE_IMAGE_BYTES:
                        raise EmailOperationError("image payload is too large")
                    chunks.append(chunk)
                return transform_image_to_jpeg(b"".join(chunks))
        raise EmailOperationError("image redirect chain is too long")


async def fetch_remote_image_as_jpeg(source: str) -> bytes:
    return await asyncio.to_thread(fetch_remote_image_as_jpeg_sync, source)


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

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "li":
            self.parts.append("\n- ")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self.block_tags:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def html_to_text(value: str) -> str:
    parser = _HtmlTextParser()
    parser.feed(value or "")
    parser.close()
    text = "".join(parser.parts)
    text = re.sub(r"[ \t]+", " ", text)
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


async def fetch_message(mailbox: EmailMailbox, *, folder: str, uid: str) -> dict[str, Any]:
    return await asyncio.to_thread(fetch_message_sync, mailbox, folder=folder, uid=uid)


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
