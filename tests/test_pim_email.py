import asyncio
import json
import sys
from io import BytesIO
from pathlib import Path

import pytest
from PIL import Image

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app import pim_email, pim_email_security, pim_email_uid, routes_pim_email  # noqa: E402


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


def _security_result(status: str = "green") -> dict:
    return {
        "schema": "xarta.pim_email.security_check.v1",
        "available": True,
        "raw_sha256": "sha256-test",
        "aggregate": {
            "status": status,
            "score": 0,
            "risk_score": 0,
            "summary": "test security result",
            "llm_called": True,
        },
        "llm": {"called": True, "model": "PRIMARY-LOCAL-TEST"},
        "dkim": {"signature_count": 1},
        "spf": {"result": "pass"},
        "dmarc": {"result": "pass"},
        "findings": [],
    }


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


def test_html_to_text_skips_inserted_image_original_helpers_and_compacts_gaps():
    text = pim_email.html_to_text(
        """
        <p>Intro</p>
        <span class="email-image-wrap">
          <img src="/api/v1/personal/email/image-proxy?source=x" alt="">
          <a class="email-image-original" href="https://example.test/image.png">original</a>
        </span>


        <div>
          Useful body
        </div>
        <p>Done</p>
        """
    )

    assert text == "Intro\n\nUseful body\n\nDone"
    assert "original" not in text


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
    assert parsed["views_available"] == {"plain": True, "html": True, "markdown": False}
    assert "Subject: =?utf-8?q?Hello_=E2=9C=93?=" in parsed["views"]["raw"]
    assert "<script>" not in parsed["views"]["html"]
    assert "<p>HTML body</p>" in parsed["views"]["html"]
    assert parsed["html_security"]["sandbox"] == "srcdoc-no-scripts-no-same-origin"
    assert parsed["html_security"]["image_proxy"] == "same-site-jpeg-transform"
    assert parsed["email_uid"].startswith("00000000-")
    assert parsed["email_uid_info"]["schema"] == pim_email_uid.SCHEMA
    assert parsed["email_uid_info"]["confidence"] == "medium"


def test_email_uid_same_headers_ignore_folder_and_imap_uid():
    raw = (
        b"Subject: Stable identity\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <stable@example.test>\r\n\r\n"
        b"Body text\r\n"
    )

    inbox = pim_email.parse_message(raw, folder="INBOX", uid="42")
    archive = pim_email.parse_message(raw, folder="Archive", uid="777")

    assert inbox["folder"] == "INBOX"
    assert archive["folder"] == "Archive"
    assert inbox["uid"] == "42"
    assert archive["uid"] == "777"
    assert inbox["email_uid"] == archive["email_uid"]
    assert inbox["email_uid_info"] == archive["email_uid_info"]


def test_email_uid_header_only_and_full_raw_generation_match():
    headers = (
        b"Subject: Header-only proof\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <header-only@example.test>\r\n\r\n"
    )
    full_raw = headers + b"Content-Type: text/plain; charset=utf-8\r\n\r\nBody text\r\n"

    header_info = pim_email_uid.generate_email_uid_info(headers)
    full_info = pim_email_uid.generate_email_uid_info(full_raw)

    assert header_info["email_uid"] == full_info["email_uid"]
    assert header_info["hash_hex"] == full_info["hash_hex"]
    assert header_info["storage_relpath"] == full_info["storage_relpath"]


def test_email_uid_date_timezone_normalizes_to_utc_prefix():
    raw = (
        b"Subject: Timezone proof\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 00:30:00 +0200\r\n"
        b"Message-ID: <timezone@example.test>\r\n\r\n"
    )

    info = pim_email_uid.generate_email_uid_info(raw)

    assert info["date_yyyymmdd"] == "20260630"
    assert info["date_source"] == "date"
    assert info["email_uid"].startswith("20260630-")


def test_email_uid_uses_received_only_as_date_prefix_fallback():
    raw = (
        b"Received: from mx.example.test by mailbox.example.test; "
        b"Wed, 01 Jul 2026 10:30:00 +0000\r\n"
        b"Subject: Received fallback\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Message-ID: <received-fallback@example.test>\r\n\r\n"
    )

    info = pim_email_uid.generate_email_uid_info(raw)
    field_names = {field["name"] for field in info["source_fields"]}

    assert info["date_yyyymmdd"] == "20260701"
    assert info["date_source"] == "received"
    assert info["email_uid"].startswith("20260701-")
    assert "Received" not in field_names


def test_email_uid_excludes_folder_and_delivery_noise_headers_from_identity_hash():
    base = (
        b"Subject: Noise proof\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <noise@example.test>\r\n\r\n"
    )
    noisy = (
        b"Return-Path: <bounce@example.test>\r\n"
        b"Delivered-To: user@example.test\r\n"
        b"Authentication-Results: mx.example.test; dkim=pass\r\n"
        b"Received-SPF: pass\r\n"
        b"Received: from mx.example.test; Wed, 01 Jul 2026 09:15:02 +0000\r\n"
        b"X-Folder: Archive\r\n"
    ) + base

    assert (
        pim_email_uid.generate_email_uid_info(base)["email_uid"]
        == (pim_email_uid.generate_email_uid_info(noisy)["email_uid"])
    )


def test_email_uid_missing_message_id_is_deterministic_lower_confidence():
    raw = (
        b"Subject: Missing message id\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
    )

    first = pim_email_uid.generate_email_uid_info(raw)
    second = pim_email_uid.generate_email_uid_info(raw)

    assert first["email_uid"] == second["email_uid"]
    assert first["confidence"] == "low"
    assert "message_id_missing" in first["warnings"]
    assert "header_fallback_used" in first["warnings"]
    assert any(field["name"] == "header-fallback" for field in first["source_fields"])


def test_email_uid_storage_relpath_derives_from_date_prefix_and_undated_bucket():
    dated = (
        b"Subject: Storage path\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <storage@example.test>\r\n\r\n"
    )
    undated = (
        b"Subject: Storage path\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"Message-ID: <storage-undated@example.test>\r\n\r\n"
    )

    dated_info = pim_email_uid.generate_email_uid_info(dated)
    undated_info = pim_email_uid.generate_email_uid_info(undated)

    assert dated_info["storage_relpath"] == f"2026/07/01/{dated_info['email_uid']}.eml.enc"
    assert undated_info["date_yyyymmdd"] == "00000000"
    assert undated_info["storage_relpath"] == f"undated/{undated_info['email_uid']}.eml.enc"


def test_parse_message_marks_real_markdown_view_available():
    raw = (
        b"Subject: Markdown\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: user@example.test\r\n"
        b"Message-ID: <m-markdown@example.test>\r\n"
        b"Content-Type: text/markdown; charset=utf-8\r\n\r\n"
        b"# Heading\r\n\r\nMarkdown body\r\n"
    )

    parsed = pim_email.parse_message(raw, folder="INBOX", uid="43")

    assert parsed["views"]["plain"] == "# Heading\n\nMarkdown body"
    assert parsed["views"]["markdown"] == "# Heading\n\nMarkdown body"
    assert parsed["views_available"] == {"plain": True, "html": False, "markdown": True}


def test_parse_message_raw_view_omits_attachment_payloads_but_keeps_security_headers():
    attachment_payload = b"UEsDBAoAAAAAAGF0dGFjaG1lbnQtYnl0ZXM="
    raw = (
        b"Subject: Raw proof\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: user@example.test\r\n"
        b"Authentication-Results: mx.example.test; dkim=pass header.d=example.test; spf=pass smtp.mailfrom=example.test; dmarc=pass\r\n"
        b"Received-SPF: pass (example.test: domain designates 203.0.113.8 as permitted sender)\r\n"
        b"DKIM-Signature: v=1; a=rsa-sha256; d=example.test; s=s1; bh=abc; b=def\r\n"
        b"Content-Type: multipart/mixed; boundary=outer\r\n\r\n"
        b"--outer\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Visible body\r\n"
        b"--outer\r\n"
        b"Content-Type: application/pdf; name=invoice.pdf\r\n"
        b"Content-Disposition: attachment; filename=invoice.pdf\r\n"
        b"Content-Transfer-Encoding: base64\r\n\r\n" + attachment_payload + b"\r\n--outer--\r\n"
    )

    parsed = pim_email.parse_message(raw, folder="INBOX", uid="99")
    raw_view = parsed["views"]["raw"]

    assert "Authentication-Results: mx.example.test;" in raw_view
    assert "Received-SPF: pass" in raw_view
    assert "DKIM-Signature: v=1;" in raw_view
    assert "Visible body" in raw_view
    assert "filename=invoice.pdf" in raw_view
    assert "omitted MIME part body" in raw_view
    assert attachment_payload.decode() not in raw_view


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
    monkeypatch.setattr(
        pim_email, "check_email_security_sync", lambda *args, **kwargs: _security_result()
    )
    mailbox = _mailbox()

    folders = pim_email.list_folders_sync(mailbox)
    inbox = pim_email.list_inbox_sync(mailbox, limit=2)
    archive = pim_email.list_folder_messages_sync(mailbox, folder="Archive", limit=1)
    message = pim_email.fetch_message_sync(mailbox, folder="INBOX", uid="42")

    assert [folder["name"] for folder in folders] == ["INBOX", "Archive"]
    assert [row["uid"] for row in inbox] == ["42", "41"]
    assert inbox[0]["subject"] == "Message 42"
    assert inbox[0]["email_uid"].startswith("20260630-")
    assert inbox[0]["email_uid_info"]["confidence"] == "high"
    assert inbox[0]["email_uid_info"]["date_source"] == "date"
    assert [row["folder"] for row in archive] == ["Archive"]
    assert [row["uid"] for row in archive] == ["42"]
    assert archive[0]["email_uid"] == inbox[0]["email_uid"]
    assert message["views"]["plain"] == "Opened body"
    assert message["email_uid_info"]["schema"] == pim_email_uid.SCHEMA
    assert message["security"]["aggregate"]["status"] == "green"


def test_fast_folder_listing_uid_generation_does_not_call_security(monkeypatch):
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", FakeIMAP)

    def fail_security(*args, **kwargs):
        raise AssertionError("UID generation must not call email security checks")

    monkeypatch.setattr(pim_email, "check_email_security_sync", fail_security)

    rows = pim_email.list_folder_messages_sync(_mailbox(), folder="INBOX", limit=2)

    assert [row["uid"] for row in rows] == ["42", "41"]
    assert all(row["email_uid_info"]["schema"] == pim_email_uid.SCHEMA for row in rows)


def test_message_open_blocks_when_security_service_is_unavailable(monkeypatch):
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", FakeIMAP)

    def offline(*args, **kwargs):
        raise pim_email.EmailSecurityUnavailableError("security deps missing")

    monkeypatch.setattr(pim_email, "check_email_security_sync", offline)

    with pytest.raises(pim_email.EmailConfigError):
        pim_email.fetch_message_sync(_mailbox(), folder="INBOX", uid="42")


def test_imap_folder_select_quotes_mailbox_names_with_spaces(monkeypatch):
    class SpaceFolderIMAP(FakeIMAP):
        selected_args = []

        def select(self, folder, readonly=False):
            assert readonly is True
            self.__class__.selected_args.append(folder)
            return "OK", [b"17"]

    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", SpaceFolderIMAP)
    monkeypatch.setattr(
        pim_email, "check_email_security_sync", lambda *args, **kwargs: _security_result()
    )
    mailbox = _mailbox()

    rows = pim_email.list_folder_messages_sync(
        mailbox,
        folder="INBOX/__ MORE 01/ethosdentalcare",
        limit=1,
    )

    assert SpaceFolderIMAP.selected_args == ['"INBOX/__ MORE 01/ethosdentalcare"']
    assert [row["folder"] for row in rows] == ["INBOX/__ MORE 01/ethosdentalcare"]


def test_imap_folder_select_arg_escapes_quoted_mailbox_names():
    assert (
        pim_email._imap_mailbox_select_arg('INBOX/Needs "quotes" and \\ slash')
        == '"INBOX/Needs \\"quotes\\" and \\\\ slash"'
    )


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
    assert ("GET", "/personal/email/messages/{uid}/security") in routes
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
    assert response["capabilities"]["security_checks"]["message_view_requires_security"] is True


def test_security_service_calls_local_llm_with_json_contract(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", "http://local-email-test.invalid")
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", "LOCAL-EMAIL-TEST")
    calls = []
    progress = []

    def fake_llm(payload):
        calls.append(payload)
        return json.dumps(
            {
                "verdict": "safe",
                "confidence": 0.8,
                "risk_score": 4,
                "scam_traits": [],
                "rationale": "Routine message shape.",
                "needs_human_review": False,
            }
        )

    raw = (
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Subject: Hello\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Hello from the test mailbox.\r\n"
    )

    result = pim_email_security.check_email_security_sync(
        raw,
        body_text="Hello from the test mailbox.",
        llm_client=fake_llm,
        dns_txt_lookup=lambda name: [],
        progress_callback=progress.append,
    )

    assert result["llm"]["called"] is True
    assert calls, "the local LLM client must be called"
    assert calls[0]["messages"][1]["content"].startswith("/no-think\n")
    assert "tools" not in calls[0]
    assert any(item["code"] == "LLM_SCAM_TRAITS_CLEAR" for item in result["findings"])
    assert result["progress"]["schema"] == pim_email_security.SECURITY_PROGRESS_SCHEMA
    assert [segment["id"] for segment in result["progress"]["segments"]] == [
        "service",
        "parse",
        "authres_provider",
        "dkim_crypto",
        "spf_protocol",
        "dmarc_policy",
        "llm_input",
        "llm_json",
        "llm_judgement",
        "aggregate",
    ]
    assert {item["stage_id"] for item in progress} >= {
        "service",
        "parse",
        "authres_provider",
        "dkim_crypto",
        "spf_protocol",
        "dmarc_policy",
        "llm_input",
        "llm_json",
        "llm_judgement",
        "aggregate",
    }
    assert progress[-1]["stage_id"] == "aggregate"
    assert progress[-1]["segments"][-1]["tone"] == result["aggregate"]["status"]


def test_security_service_gates_invalid_llm_json_as_suspicious(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", "http://local-email-test.invalid")
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", "LOCAL-EMAIL-TEST")
    raw = (
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Subject: Bad JSON proof\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Plain body\r\n"
    )

    result = pim_email_security.check_email_security_sync(
        raw,
        body_text="Plain body",
        llm_client=lambda payload: "I refuse to return JSON",
        dns_txt_lookup=lambda name: [],
    )

    codes = {item["code"] for item in result["findings"]}
    assert "LLM_JSON_INVALID" in codes
    assert result["aggregate"]["status"] == "red"
    assert result["llm"]["called"] is True


def test_spf_source_ip_uses_newest_public_received_hop_for_srs_forwarding():
    headers = [
        (
            "from mout.kundenserver.de ([217.72.192.73]) by mx.kundenserver.de "
            "(mxeue003 [212.227.15.41]) with ESMTPS for <user@example.test>"
        ),
        (
            "from c132-110.smtp-out.eu-west-2.amazonses.com ([76.223.132.110]) "
            "by mx.kundenserver.de (mxeue101 [217.72.192.67]) with ESMTPS"
        ),
    ]

    source = pim_email_security._extract_source_ip(headers)

    assert source == {"ip": "217.72.192.73", "helo": "mout.kundenserver.de"}
