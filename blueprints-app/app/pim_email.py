"""PIM email helpers for IMAP read views and guarded SMTP proof."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import html
import imaplib
import json
import os
import re
import secrets
import smtplib
import ssl
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage, Message
from email.parser import BytesParser
from html.parser import HTMLParser
from typing import Any

import asyncpg

DEFAULT_MAILBOX_ID = "default"
DEFAULT_IMAP_PORT = 993
DEFAULT_SMTP_PORT = 465
ENVELOPE_PURPOSE = b"xarta-pim-email-password-v1"


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


def list_inbox_sync(mailbox: EmailMailbox, *, limit: int = 25) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 100))
    client = _connect_imap(mailbox)
    try:
        status, _ = client.select("INBOX", readonly=True)
        if status != "OK":
            raise EmailOperationError("IMAP INBOX select failed")
        status, search_data = client.uid("search", None, "ALL")
        if status != "OK" or not search_data:
            raise EmailOperationError("IMAP INBOX search failed")
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
                    "folder": "INBOX",
                    "subject": _decode_header_value(msg.get("subject", "")),
                    "from": _decode_header_value(msg.get("from", "")),
                    "date": _decode_header_value(msg.get("date", "")),
                    "message_id": _decode_header_value(msg.get("message-id", "")),
                }
            )
        return messages
    finally:
        _logout_imap(client)


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
    sanitized_html = sanitize_email_html(raw_html) if raw_html else ""
    default_text = plain.strip() if plain.strip() else html_to_text(sanitized_html)
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
            "html": sanitized_html,
            "markdown": markdown,
        },
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
    }
    skip_tags = {"script", "style", "iframe", "object", "embed", "svg", "math"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in self.skip_tags:
            self.skip_depth += 1
            return
        if self.skip_depth or tag not in self.allowed_tags:
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
        if self.skip_depth or tag not in self.allowed_tags or tag == "br":
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
                continue
            if clean_name in {"title", "colspan", "rowspan"}:
                safe.append((clean_name, clean_value[:160]))
        return safe


def sanitize_email_html(value: str) -> str:
    parser = _SafeHtmlParser()
    parser.feed(value or "")
    parser.close()
    return "".join(parser.parts).strip()


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
