import asyncio
import sys
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import pim_email, routes_pim_email  # noqa: E402


def _mailbox(password: str = "test-password-123") -> pim_email.EmailMailbox:
    return pim_email.EmailMailbox(
        mailbox_id="test-mailbox",
        email_address="user@example.test",
        imap_host="imap.example.test",
        imap_port=993,
        imap_ssl=True,
        smtp_host="smtp.example.test",
        smtp_port=465,
        smtp_ssl=True,
        smtp_starttls=False,
        password=password,
    )


def test_password_envelope_is_encrypted_and_authenticated(monkeypatch):
    key = pim_email.generate_credential_key()
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", key)

    envelope = pim_email.encrypt_password("test-password-123")

    assert "test-password-123" not in envelope
    assert pim_email.decrypt_password(envelope) == "test-password-123"
    tampered = envelope.replace("ciphertext", "ciphertexu")
    with pytest.raises(pim_email.EmailCredentialError):
        pim_email.decrypt_password(tampered)


def test_email_html_sanitizer_removes_active_and_proxies_remote_content(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_API_SECRET", "test-secret")
    result = pim_email.sanitize_email_html_with_report(
        """
        <div onclick="steal()">Hello<script>alert(1)</script>
        <img src="https://tracker.example/pixel.png">
        <img src="cid:hero-image" alt="Hero">
        <a href="javascript:alert(1)">bad</a>
        <a href="https://example.test/page" style="color:red">good</a></div>
        """,
        inline_images={"hero-image": "data:image/jpeg;base64,abc"},
    )
    sanitized = result.html

    assert "script" not in sanitized.lower()
    assert "onclick" not in sanitized.lower()
    assert "javascript:" not in sanitized.lower()
    assert 'src="/api/v1/personal/email/image-proxy?' in sanitized
    assert 'class="email-image-original" href="https://tracker.example/pixel.png"' in sanitized
    assert '<img src="data:image/jpeg;base64,abc" alt="Hero">' in sanitized
    assert 'href="https://example.test/page"' in sanitized
    assert "rel=" in sanitized
    assert result.remote_images_proxied == 1
    assert result.remote_images_blocked == 0
    assert result.tracking_images_blocked == 1
    assert result.inline_images_rendered == 1
    assert result.active_content_blocked == 1
    assert result.unsafe_links_blocked == 1


def test_image_transform_reencodes_inline_images_to_jpeg():
    source = BytesIO()
    Image.new("RGBA", (1, 1), (255, 0, 0, 128)).save(source, format="PNG")

    jpeg = pim_email.transform_image_to_jpeg(source.getvalue())

    assert jpeg.startswith(b"\xff\xd8\xff")
    assert len(jpeg) > 20


def test_parse_message_returns_plain_sanitized_html_and_markdown_views():
    raw = (
        b"Subject: =?utf-8?q?Hello_=E2=9C=93?=\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: user@example.test\r\n"
        b"Message-ID: <m1@example.test>\r\n"
        b"Content-Type: multipart/alternative; boundary=x\r\n\r\n"
        b"--x\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nPlain body\r\n"
        b"--x\r\nContent-Type: text/html; charset=utf-8\r\n\r\n"
        b"<p>HTML body</p><script>bad()</script>\r\n--x--\r\n"
    )

    parsed = pim_email.parse_message(raw, folder="INBOX", uid="42")

    assert parsed["headers"]["subject"] == "Hello ✓"
    assert parsed["views"]["plain"] == "Plain body"
    assert parsed["views"]["markdown"] == "Plain body"
    assert "<script>" not in parsed["views"]["html"]
    assert "<p>HTML body</p>" in parsed["views"]["html"]
    assert parsed["html_security"]["sandbox"] == "srcdoc-no-scripts-no-same-origin"
    assert parsed["html_security"]["image_proxy"] == "same-site-jpeg-transform"


class FakeIMAP:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.logged_in = False

    def login(self, user, password):
        assert user == "user@example.test"
        assert password == "test-password-123"
        self.logged_in = True
        return "OK", [b"logged in"]

    def list(self):
        return "OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren) "/" "Archive"',
        ]

    def select(self, folder, readonly=False):
        assert readonly is True
        assert folder in {"INBOX", "Archive"}
        return "OK", [b"2"]

    def uid(self, command, *args):
        if command == "search":
            return "OK", [b"41 42"]
        if command == "fetch" and args[1] == "(RFC822.HEADER RFC822.SIZE FLAGS)":
            uid = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
            header = (
                f"Subject: Message {uid}\r\n"
                "From: Sender <sender@example.test>\r\n"
                "Date: Tue, 30 Jun 2026 12:00:00 +0000\r\n"
                f"Message-ID: <{uid}@example.test>\r\n\r\n"
            ).encode()
            return "OK", [(b"HEADER", header)]
        if command == "fetch" and args[1] == "(RFC822)":
            return "OK", [
                (
                    b"RFC822",
                    b"Subject: Opened\r\nFrom: Sender <sender@example.test>\r\n"
                    b"Content-Type: text/plain; charset=utf-8\r\n\r\nOpened body",
                )
            ]
        return "NO", []

    def logout(self):
        return "OK", [b"bye"]


def test_imap_folder_inbox_and_message_paths_use_configured_mailbox(monkeypatch):
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", FakeIMAP)
    mailbox = _mailbox()

    folders = pim_email.list_folders_sync(mailbox)
    inbox = pim_email.list_inbox_sync(mailbox, limit=2)
    archive = pim_email.list_folder_messages_sync(mailbox, folder="Archive", limit=1)
    message = pim_email.fetch_message_sync(mailbox, folder="INBOX", uid="42")

    assert [folder["name"] for folder in folders] == ["INBOX", "Archive"]
    assert [row["uid"] for row in inbox] == ["42", "41"]
    assert inbox[0]["subject"] == "Message 42"
    assert [row["folder"] for row in archive] == ["Archive"]
    assert [row["uid"] for row in archive] == ["42"]
    assert message["views"]["plain"] == "Opened body"


class FakeSMTP:
    sent_messages = []

    def __init__(self, host, port, context=None, timeout=None):
        self.host = host
        self.port = port

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def login(self, user, password):
        assert user == "user@example.test"
        assert password == "test-password-123"

    def send_message(self, message):
        self.sent_messages.append(message)


def test_smtp_self_send_gate_rejects_all_other_recipients(monkeypatch):
    FakeSMTP.sent_messages = []
    monkeypatch.setattr(pim_email.smtplib, "SMTP_SSL", FakeSMTP)
    mailbox = _mailbox()

    with pytest.raises(pim_email.EmailOperationError):
        pim_email.smtp_self_send_sync(mailbox, recipient="other@example.test")

    proof = pim_email.smtp_self_send_sync(mailbox, recipient="user@example.test")

    assert proof["ok"] is True
    assert proof["recipient"] == "user@example.test"
    assert len(FakeSMTP.sent_messages) == 1
    assert FakeSMTP.sent_messages[0]["To"] == "user@example.test"


def test_router_exposes_no_delete_or_general_send_capability():
    routes = {
        (method, route.path)
        for route in routes_pim_email.router.routes
        for method in getattr(route, "methods", set())
    }

    assert not any(method == "DELETE" for method, _ in routes)
    assert not any(path.endswith("/send") for _, path in routes)
    assert ("GET", "/personal/email/folder-messages") in routes
    assert ("GET", "/personal/email/image-proxy") in routes
    assert ("POST", "/personal/email/smtp-self-test") in routes


def test_email_image_proxy_signature_is_required(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_API_SECRET", "test-secret")
    source = "https://images.example.test/banner.png?track=1"

    signature = pim_email.sign_email_image_url(source)

    assert pim_email.verify_email_image_signature(source, signature)
    assert not pim_email.verify_email_image_signature(source, "bad")


def test_status_route_reports_disabled_send_delete_capabilities(monkeypatch):
    class FakeStore:
        async def ensure_schema(self):
            return None

        async def public_mailboxes(self):
            return [_mailbox().public_dict()]

    monkeypatch.setattr(routes_pim_email, "_store", lambda: FakeStore())

    response = asyncio.run(routes_pim_email.email_status())

    assert response["storage"] == "postgres"
    assert response["capabilities"]["imap_read"] is True
    assert response["capabilities"]["smtp_self_test"] is True
    assert response["capabilities"]["smtp_general_send"] is False
    assert response["capabilities"]["delete"] is False
    assert response["capabilities"]["ai_send"] is False
