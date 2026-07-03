import asyncio
import importlib.util
import inspect
import json
import sys
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image
from pydantic import ValidationError

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


def test_encrypted_content_and_transformed_external_asset_round_trip(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    raw = b"Subject: Stored\r\n\r\nBody that must not appear in ciphertext.\r\n"

    storage = pim_email.write_encrypted_bytes_atomic(
        relpath="2026/07/01/test-message.eml.enc",
        content=raw,
    )

    encrypted = (tmp_path / storage["storage_relpath"]).read_bytes()
    assert raw not in encrypted
    assert pim_email.read_encrypted_bytes(storage["storage_relpath"]) == raw
    assert storage["raw_sha256"] == pim_email.hashlib.sha256(raw).hexdigest()

    image_source = BytesIO()
    Image.new("RGBA", (3, 2), (0, 160, 240, 255)).save(image_source, format="PNG")
    asset = pim_email.build_transformed_external_image_asset(
        mailbox_id="test-mailbox",
        email_uid="20260701-0123456789abcdef0123456789abcdef01234567",
        source_url="https://images.example.test/banner.png?utm=1",
        content=image_source.getvalue(),
        metadata={"proof": "unit"},
    )

    assert asset["content_type"] == "image/jpeg"
    assert asset["width"] == 3
    assert asset["height"] == 2
    assert asset["shared_asset_uid"].startswith("email-shared-asset-")
    assert asset["storage_relpath"].startswith("assets/")
    asset_plain = pim_email.read_encrypted_bytes(
        asset["storage_relpath"],
        purpose=pim_email.ASSET_PURPOSE,
    )
    assert asset_plain.startswith(b"\xff\xd8\xff")
    assert b"PNG" not in (tmp_path / asset["storage_relpath"]).read_bytes()


def test_worker_transformed_external_asset_round_trip_and_rejects_mismatch(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    image_source = BytesIO()
    Image.new("RGBA", (3, 2), (0, 160, 240, 255)).save(image_source, format="PNG")
    raw = image_source.getvalue()
    transformed = pim_email.transform_image_to_jpeg(raw)
    width, height = pim_email.jpeg_dimensions(transformed)

    asset = pim_email.build_transformed_external_image_asset_from_worker_payload(
        mailbox_id="test-mailbox",
        email_uid="20260701-0123456789abcdef0123456789abcdef01234567",
        source_url="https://images.example.test/banner.png?utm=1",
        transformed_content=transformed,
        raw_image_sha256=pim_email.hashlib.sha256(raw).hexdigest(),
        transformed_sha256=pim_email.hashlib.sha256(transformed).hexdigest(),
        width=width,
        height=height,
        transform_version="jpeg-v1",
        metadata={"proof": "worker"},
    )

    stored = pim_email.read_encrypted_bytes(
        asset["storage_relpath"],
        purpose=pim_email.ASSET_PURPOSE,
    )
    assert stored == transformed
    assert asset["metadata"]["worker_transformed"] is True
    assert asset["transform_version"] == "jpeg-v1"

    with pytest.raises(pim_email.EmailOperationError, match="transformed_sha256"):
        pim_email.build_transformed_external_image_asset_from_worker_payload(
            mailbox_id="test-mailbox",
            email_uid="20260701-0123456789abcdef0123456789abcdef01234567",
            source_url="https://images.example.test/banner.png?utm=1",
            transformed_content=transformed,
            raw_image_sha256=pim_email.hashlib.sha256(raw).hexdigest(),
            transformed_sha256="0" * 64,
            width=width,
            height=height,
            transform_version="jpeg-v1",
        )

    with pytest.raises(pim_email.EmailOperationError, match="dimensions"):
        pim_email.build_transformed_external_image_asset_from_worker_payload(
            mailbox_id="test-mailbox",
            email_uid="20260701-0123456789abcdef0123456789abcdef01234567",
            source_url="https://images.example.test/banner.png?utm=1",
            transformed_content=transformed,
            raw_image_sha256=pim_email.hashlib.sha256(raw).hexdigest(),
            transformed_sha256=pim_email.hashlib.sha256(transformed).hexdigest(),
            width=width + 1,
            height=height,
            transform_version="jpeg-v1",
        )


def test_completed_security_contract_rejects_placeholders_and_requires_exact_hash():
    email_uid = "20260701-0123456789abcdef0123456789abcdef01234567"
    raw_sha256 = "a" * 64
    valid = {
        "result_json": json.dumps(
            {
                "available": True,
                "email_uid": email_uid,
                "raw_sha256": raw_sha256,
                "checked_at": "2026-07-02T00:00:00Z",
                "checker_versions": {"schema": "test"},
            }
        ),
        "security_status": "stored",
        "raw_sha256": raw_sha256,
    }
    queued = {
        **valid,
        "result_json": json.dumps(
            {
                "available": False,
                "queued": True,
                "email_uid": email_uid,
                "raw_sha256": raw_sha256,
                "checked_at": "2026-07-02T00:00:00Z",
                "checker_versions": {"schema": "test"},
            }
        ),
        "security_status": "queued",
    }
    placeholder = {
        **valid,
        "result_json": json.dumps(
            {
                "available": True,
                "placeholder": True,
                "email_uid": email_uid,
                "raw_sha256": raw_sha256,
                "checked_at": "2026-07-02T00:00:00Z",
                "checker_versions": {"schema": "test"},
            }
        ),
    }

    assert pim_email._completed_security_result_from_row(
        valid,
        email_uid=email_uid,
        raw_sha256=raw_sha256,
    )
    assert (
        pim_email._completed_security_result_from_row(
            queued,
            email_uid=email_uid,
            raw_sha256=raw_sha256,
        )
        is None
    )
    assert (
        pim_email._completed_security_result_from_row(
            placeholder,
            email_uid=email_uid,
            raw_sha256=raw_sha256,
        )
        is None
    )
    assert (
        pim_email._completed_security_result_from_row(
            valid,
            email_uid=email_uid,
            raw_sha256="b" * 64,
        )
        is None
    )


def test_split_security_phase_only_result_does_not_unlock_render(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", "http://local-email-test.invalid")
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", "LOCAL-EMAIL-TEST")
    raw = (
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Subject: Split security\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Deterministic evidence should persist before local AI.\r\n"
    )
    deterministic = pim_email_security.check_email_security_deterministic_sync(
        raw,
        dns_txt_lookup=lambda name: [],
    )

    assert deterministic["schema"] == pim_email_security.DETERMINISTIC_SCHEMA
    assert deterministic["deterministic_complete"] is True
    assert deterministic["available"] is False
    assert deterministic["llm"]["status"] == "pending"

    row = {
        "result_json": json.dumps(
            {
                **deterministic,
                "email_uid": "20260701-" + "c" * 40,
                "checker_versions": {"schema": "test"},
            }
        ),
        "security_status": "failed_retryable",
        "raw_sha256": deterministic["raw_sha256"],
    }
    assert (
        pim_email._completed_security_result_from_row(
            row,
            email_uid="20260701-" + "c" * 40,
            raw_sha256=deterministic["raw_sha256"],
        )
        is None
    )

    def fake_llm(payload):
        return json.dumps(
            {
                "verdict": "safe",
                "confidence": 0.9,
                "risk_score": 1,
                "scam_traits": [],
                "rationale": "Routine message.",
                "needs_human_review": False,
            }
        )

    full = pim_email_security.complete_email_security_with_llm_sync(
        raw,
        deterministic=deterministic,
        body_text="Deterministic evidence should persist before local AI.",
        llm_client=fake_llm,
    )

    assert full["available"] is True
    assert full["phases"]["deterministic"]["status"] == "complete"
    assert full["phases"]["llm"]["status"] == "complete"
    assert full["llm"]["valid_json"] is True


def test_deterministic_security_handles_malformed_message_id_header(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", "http://local-email-test.invalid")
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", "LOCAL-EMAIL-TEST")
    raw = (
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Subject: Malformed message id\r\n"
        b"Message-ID: <[bad-local-part@example.test]>\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Malformed headers are evidence; they must not make the checker fail.\r\n"
    )

    deterministic = pim_email_security.check_email_security_deterministic_sync(
        raw,
        dns_txt_lookup=lambda name: [],
    )

    assert deterministic["deterministic_complete"] is True
    assert deterministic["raw_sha256"] == pim_email.hashlib.sha256(raw).hexdigest()
    assert deterministic["context"]["message_id"] == "<[bad-local-part@example.test]>"


def test_security_handles_surrogate_escaped_headers_as_evidence(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", "http://local-email-test.invalid")
    monkeypatch.setenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", "LOCAL-EMAIL-TEST")
    raw = (
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Subject: bad \xff\xfe here\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Bad header bytes are durable evidence, not a checker failure.\r\n"
    )

    deterministic = pim_email_security.check_email_security_deterministic_sync(
        raw,
        dns_txt_lookup=lambda name: [],
    )

    safe_subject = "bad \\udcff\\udcfe here"
    assert deterministic["deterministic_complete"] is True
    assert deterministic["raw_sha256"] == pim_email.hashlib.sha256(raw).hexdigest()
    assert (
        deterministic["context"]["subject_sha256"]
        == pim_email.hashlib.sha256(safe_subject.encode("utf-8")).hexdigest()
    )

    payloads: list[dict] = []

    def fake_llm(payload):
        payload["messages"][1]["content"].encode("utf-8")
        payloads.append(payload)
        return json.dumps(
            {
                "verdict": "safe",
                "confidence": 0.9,
                "risk_score": 1,
                "scam_traits": [],
                "rationale": "Malformed header evidence was normalized.",
                "needs_human_review": False,
            }
        )

    full = pim_email_security.complete_email_security_with_llm_sync(
        raw,
        deterministic=deterministic,
        body_text="Bad header bytes are durable evidence, not a checker failure.",
        llm_client=fake_llm,
    )

    assert full["available"] is True
    user_payload = json.loads(payloads[0]["messages"][1]["content"].split("\n", 1)[1])
    assert user_payload["headers"]["subject"] == safe_subject


def test_security_llm_phase_requires_prior_deterministic_phase():
    email_uid = "20260701-" + "d" * 40
    raw = b"Subject: Split stage\r\n\r\nLLM must not run before deterministic.\r\n"
    raw_sha256 = pim_email.hashlib.sha256(raw).hexdigest()

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            pass

        async def ensure_schema(self):
            return None

        async def _load_local_security_source(self, email_uid_arg, *, mailbox_id):
            assert email_uid_arg == email_uid
            assert mailbox_id == "test-mailbox"
            return {
                "mailbox_id": mailbox_id,
                "email_uid": email_uid,
                "membership": None,
                "raw": raw,
                "raw_sha256": raw_sha256,
                "parsed": {"views": {"plain": "LLM must not run before deterministic."}},
            }

        async def completed_security_phase_result(self, **kwargs):
            assert kwargs["phase"] == "deterministic"
            return None

    with pytest.raises(pim_email.EmailOperationError, match="Deterministic security phase"):
        asyncio.run(
            FakeStore().run_local_security_llm_phase(
                email_uid,
                mailbox_id="test-mailbox",
            )
        )


def test_backfill_cli_exposes_independent_security_phase_artifacts():
    source = (APP_ROOT / "app" / "pim_email.py").read_text(encoding="utf-8")
    cli = (APP_ROOT / "scripts" / "pim_email_backfill.py").read_text(encoding="utf-8")

    assert '"security_deterministic"' in source
    assert '"security_llm"' in source
    assert '"security_deterministic"' in cli
    assert '"security_llm"' in cli
    assert "run_local_security_deterministic_phase" in source
    assert "run_local_security_llm_phase" in source


def test_security_result_upserts_are_keyed_by_email_uid_raw_hash_contract():
    source = (APP_ROOT / "app" / "pim_email.py").read_text(encoding="utf-8")

    assert source.count("ON CONFLICT (security_check_id)") >= 3
    assert "ON CONFLICT (mailbox_id, folder, uid, raw_sha256) DO UPDATE SET" in source
    assert "security_check_id = EXCLUDED.security_check_id" not in source


def test_security_llm_openai_env_accepts_v1_base_url(monkeypatch):
    monkeypatch.delenv("BLUEPRINTS_EMAIL_SECURITY_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("BLUEPRINTS_EMAIL_SECURITY_LLM_API_KEY", raising=False)
    monkeypatch.delenv("BLUEPRINTS_EMAIL_SECURITY_LLM_MODEL", raising=False)
    monkeypatch.setenv("OPENAI_BASE_URL", "http://127.0.0.1:8651/v1")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setenv("OPENAI_MODEL", "hermes-sparky-toothless")
    monkeypatch.setenv("LITELLM_BASE_URL", "http://litellm-fallback.example.invalid")
    monkeypatch.setenv("LITELLM_API_KEY", "test-litellm-key")
    monkeypatch.setenv("BLUEPRINTS_KANBAN_AUTOMATION_LOCAL_AI_MODEL", "local-default")

    assert pim_email_security._llm_config() == (
        "http://127.0.0.1:8651/v1",
        "test-openai-key",
        "hermes-sparky-toothless",
    )
    assert (
        pim_email_security._openai_chat_completions_url("http://127.0.0.1:8651/v1")
        == "http://127.0.0.1:8651/v1/chat/completions"
    )


def test_email_store_schema_is_process_cached_per_dsn(monkeypatch):
    dsn = "postgresql://pim-email-cache-test"
    pim_email.PgEmailStore._schema_ready_dsns.discard(dsn)
    connections = []

    class FakeConnection:
        def __init__(self):
            self.statements = []
            self.closed = False

        async def execute(self, statement, *args):
            self.statements.append(str(statement))
            return "OK"

        async def close(self):
            self.closed = True

    async def fake_connect(connect_dsn):
        assert connect_dsn == dsn
        conn = FakeConnection()
        connections.append(conn)
        return conn

    monkeypatch.setattr(pim_email.asyncpg, "connect", fake_connect)
    try:
        store = pim_email.PgEmailStore(dsn=dsn)

        asyncio.run(store.ensure_schema())
        asyncio.run(store.ensure_schema())

        assert len(connections) == 1
        assert connections[0].closed
        assert any(
            "CREATE TABLE IF NOT EXISTS pim_email_mailboxes" in statement
            for statement in connections[0].statements
        )
    finally:
        pim_email.PgEmailStore._schema_ready_dsns.discard(dsn)


def test_sanitized_view_artifact_persists_encrypted_sanitized_raw_view(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    email_uid = "20260701-abcdefabcdefabcdefabcdefabcdefabcdefabcd"
    raw = (
        b"Subject: Sanitized artifact\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <sanitized-artifact@example.test>\r\n"
        b"Content-Type: multipart/mixed; boundary=artifact-boundary\r\n\r\n"
        b"--artifact-boundary\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n\r\n"
        b"<p>Visible safe body</p><script>bad()</script>\r\n"
        b"--artifact-boundary\r\n"
        b"Content-Type: application/octet-stream; name=secret.bin\r\n"
        b"Content-Disposition: attachment; filename=secret.bin\r\n\r\n"
        b"secret-bytes-must-not-render\r\n"
        b"--artifact-boundary--\r\n"
    )
    raw_sha256 = pim_email.hashlib.sha256(raw).hexdigest()

    artifact = pim_email.build_sanitized_view_artifact(
        mailbox_id="test-mailbox",
        email_uid=email_uid,
        raw=raw,
        raw_sha256=raw_sha256,
    )
    encrypted = (tmp_path / artifact["storage_relpath"]).read_bytes()

    assert b"Visible safe body" not in encrypted
    assert b"<script>" not in encrypted
    assert b"secret-bytes-must-not-render" not in encrypted
    assert artifact["input_raw_sha256"] == raw_sha256
    assert artifact["transform_version"] == "email-sanitized-raw-v1"
    assert artifact["views_available"]["raw"] is True
    assert artifact["derivation"]["durable_body"] == "sanitized-raw-message"
    assert artifact["derivation"]["view_bodies_persisted"] is False

    stored_payload = json.loads(
        pim_email.read_encrypted_bytes(
            artifact["storage_relpath"],
            purpose=pim_email.SANITIZED_VIEW_PURPOSE,
        )
    )
    assert stored_payload["schema"] == "xarta.pim_email.sanitized_raw_artifact.v1"
    assert "views" not in stored_payload
    assert "plain" not in stored_payload
    assert "markdown" not in stored_payload
    assert "html" not in stored_payload
    assert "Subject: Sanitized artifact" in stored_payload["sanitized_raw"]
    assert "Visible safe body" in stored_payload["sanitized_raw"]
    assert "<script>" not in stored_payload["sanitized_raw"]
    assert "[xarta raw view omitted MIME part body:" in stored_payload["sanitized_raw"]
    assert "secret-bytes-must-not-render" not in stored_payload["sanitized_raw"]

    row = {
        "artifact_uid": artifact["artifact_uid"],
        "email_uid": email_uid,
        "input_raw_sha256": raw_sha256,
        "sanitizer_policy_version": artifact["sanitizer_policy_version"],
        "transform_version": artifact["transform_version"],
        "output_sha256": artifact["output_sha256"],
        "storage_relpath": artifact["storage_relpath"],
        "encrypted_size": artifact["encrypted_size"],
        "views_available_json": json.dumps(artifact["views_available"]),
        "safety_counts_json": json.dumps(artifact["safety_counts"]),
        "derivation_json": json.dumps(artifact["derivation"]),
        "generated_at": "",
        "updated_at": "",
    }
    payload = pim_email.read_sanitized_view_artifact(row)

    assert payload["sanitized_raw"] == stored_payload["sanitized_raw"]
    assert payload["views"]["plain"] == "Visible safe body"
    assert "<script>" not in payload["views"]["html"]
    assert payload["views_available"]["raw"] is True
    assert "Subject: Sanitized artifact" in payload["views"]["raw"]
    assert "Visible safe body" in payload["views"]["raw"]
    assert "<script>" not in payload["views"]["raw"]
    assert "[xarta raw view omitted MIME part body:" in payload["views"]["raw"]
    assert "secret-bytes-must-not-render" not in payload["views"]["raw"]


def test_read_local_message_uses_stored_uid_as_authoritative_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    raw = (
        b"Subject: Reparsed identity\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Wed, 01 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <reparsed@example.test>\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Body\r\n"
    )
    reparsed = pim_email.parse_message(raw, folder="INBOX", uid="42")
    stored_uid = "20260701-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    assert reparsed["email_uid"] != stored_uid
    storage = pim_email.write_encrypted_bytes_atomic(
        relpath=f"2026/07/01/{stored_uid}.eml.enc",
        content=raw,
    )

    message_row = {
        "email_uid": stored_uid,
        "mailbox_id": "test-mailbox",
        "raw_sha256": storage["raw_sha256"],
        "message_id": "<stored@example.test>",
        "subject": "Stored identity subject",
        "from_addr": "Stored Sender <stored@example.test>",
        "to_addr": "User <user@example.test>",
        "date_header": "Wed, 01 Jul 2026 09:15:00 +0000",
        "uid_info_json": json.dumps({**reparsed["email_uid_info"], "email_uid": stored_uid}),
        "headers_json": json.dumps({"subject": "Stale parsed subject"}),
        "metadata_json": "{}",
        "storage_relpath": storage["storage_relpath"],
        "encrypted_size": storage["encrypted_size"],
        "encryption_json": "{}",
    }
    membership_rows = [
        {
            "folder_name": "INBOX",
            "folder_uid": "folder-inbox",
            "imap_uid": "42",
            "uidvalidity": "777",
            "flags_json": json.dumps(["seen"]),
            "last_seen_at": "",
            "remote_moved_at": "",
            "remote_move_target": "Downloaded",
        }
    ]

    class FakeConnection:
        async def fetchrow(self, query, *args):
            assert args[0] == "test-mailbox" or args[0] == stored_uid
            if "FROM pim_email_messages" in query:
                assert args == ("test-mailbox", stored_uid)
                return message_row
            if "FROM pim_email_security_checks" in query:
                assert args == (stored_uid, storage["raw_sha256"])
                return None
            if "FROM pim_email_sanitized_view_artifacts" in query:
                assert args[:3] == ("test-mailbox", stored_uid, storage["raw_sha256"])
                return None
            raise AssertionError(query)

        async def fetch(self, query, *args):
            assert args == ("test-mailbox", stored_uid)
            if "FROM pim_email_folder_memberships" in query:
                return membership_rows
            if "FROM pim_email_transformed_assets" in query:
                return []
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    message = asyncio.run(FakeStore().read_local_message(stored_uid, mailbox_id="test-mailbox"))

    assert message["email_uid"] == stored_uid
    assert message["email_uid_info"]["email_uid"] == stored_uid
    assert message["raw_sha256"] == storage["raw_sha256"]
    assert message["headers"]["subject"] == "Stored identity subject"
    assert message["stored"]["verified"] is True
    assert message["stored"]["raw_original_access"] == "blocked"
    assert message["body_blocked"] is True
    assert message["views"] == {}
    assert message["views_available"] == {
        "plain": False,
        "html": False,
        "markdown": False,
        "raw": False,
    }
    assert message["security"]["security_status"] == "missing"
    assert message["security"]["blocked_reason"] == "completed_security_result_missing"


def test_read_local_message_uses_persisted_sanitized_raw_view(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    stored_uid = "20260702-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    raw = (
        b"Subject: Safe raw proof\r\n"
        b"From: Sender <sender@example.test>\r\n"
        b"To: User <user@example.test>\r\n"
        b"Date: Thu, 02 Jul 2026 09:15:00 +0000\r\n"
        b"Message-ID: <safe-raw@example.test>\r\n"
        b"Content-Type: multipart/mixed; boundary=proof-boundary\r\n\r\n"
        b"--proof-boundary\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Hello safe raw body.\r\n"
        b"--proof-boundary\r\n"
        b"Content-Type: application/octet-stream; name=secret.bin\r\n"
        b"Content-Disposition: attachment; filename=secret.bin\r\n\r\n"
        b"secret-bytes-must-not-render\r\n"
        b"--proof-boundary--\r\n"
    )
    storage = pim_email.write_encrypted_bytes_atomic(
        relpath=f"2026/07/02/{stored_uid}.eml.enc",
        content=raw,
    )
    artifact = pim_email.build_sanitized_view_artifact(
        mailbox_id="test-mailbox",
        email_uid=stored_uid,
        raw=raw,
        raw_sha256=storage["raw_sha256"],
    )
    message_row = {
        "email_uid": stored_uid,
        "mailbox_id": "test-mailbox",
        "raw_sha256": storage["raw_sha256"],
        "message_id": "<safe-raw@example.test>",
        "subject": "Safe raw proof",
        "from_addr": "Sender <sender@example.test>",
        "to_addr": "User <user@example.test>",
        "date_header": "Thu, 02 Jul 2026 09:15:00 +0000",
        "uid_info_json": json.dumps(
            {
                "email_uid": stored_uid,
                "storage_relpath": "2026/07/02/original.eml.enc",
                "path": "/unsafe/original.eml.enc",
            }
        ),
        "headers_json": "{}",
        "metadata_json": "{}",
        "storage_relpath": storage["storage_relpath"],
        "encrypted_size": storage["encrypted_size"],
        "encryption_json": "{}",
    }
    membership_rows = [
        {
            "folder_name": "INBOX",
            "folder_uid": "folder-inbox",
            "imap_uid": "42",
            "uidvalidity": "777",
            "flags_json": "[]",
            "last_seen_at": "",
            "remote_moved_at": "",
            "remote_move_target": "",
        }
    ]
    security_result = {
        **_security_result("green"),
        "email_uid": stored_uid,
        "raw_sha256": storage["raw_sha256"],
        "checked_at": "2026-07-02T09:30:00Z",
        "checker_versions": {"schema": "test"},
    }
    security_row = {
        "result_json": json.dumps(security_result),
        "security_status": "stored",
        "error_message": "",
        "checked_at": "2026-07-02T09:30:00Z",
        "raw_sha256": storage["raw_sha256"],
    }
    sanitized_row = {
        "artifact_uid": artifact["artifact_uid"],
        "email_uid": stored_uid,
        "input_raw_sha256": artifact["input_raw_sha256"],
        "sanitizer_policy_version": artifact["sanitizer_policy_version"],
        "transform_version": artifact["transform_version"],
        "output_sha256": artifact["output_sha256"],
        "storage_relpath": artifact["storage_relpath"],
        "encrypted_size": artifact["encrypted_size"],
        "views_available_json": json.dumps(artifact["views_available"]),
        "safety_counts_json": json.dumps(artifact["safety_counts"]),
        "derivation_json": json.dumps(artifact["derivation"]),
        "generated_at": "",
        "updated_at": "",
    }

    class FakeConnection:
        async def fetchrow(self, query, *args):
            if "FROM pim_email_messages" in query:
                assert args == ("test-mailbox", stored_uid)
                return message_row
            if "FROM pim_email_security_checks" in query:
                assert args == (stored_uid, storage["raw_sha256"])
                return security_row
            if "FROM pim_email_sanitized_view_artifacts" in query:
                assert args[:3] == ("test-mailbox", stored_uid, storage["raw_sha256"])
                return sanitized_row
            raise AssertionError(query)

        async def fetch(self, query, *args):
            assert args == ("test-mailbox", stored_uid)
            if "FROM pim_email_folder_memberships" in query:
                return membership_rows
            if "FROM pim_email_transformed_assets" in query:
                return []
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    message = asyncio.run(FakeStore().read_local_message(stored_uid, mailbox_id="test-mailbox"))

    assert message["body_blocked"] is False
    safe_raw = message["views"]["raw"]
    assert message["views_available"]["raw"] is True
    assert message["stored"]["raw_original_access"] == "blocked"
    assert message["stored"]["sanitized_view"]["transform_version"] == "email-sanitized-raw-v1"
    assert "storage_relpath" not in message["email_uid_info"]
    assert "path" not in message["email_uid_info"]
    assert "Subject: Safe raw proof" in safe_raw
    assert "Hello safe raw body." in safe_raw
    assert "[xarta raw view omitted MIME part body:" in safe_raw
    assert "secret-bytes-must-not-render" not in safe_raw


def test_local_corpus_status_reports_missing_security_without_placeholder_results():
    queries = []

    class FakeConnection:
        async def fetchrow(self, query, *args):
            queries.append(query)
            assert args == ("test-mailbox",) or args[:1] == ("test-mailbox",)
            if "raw_originals_stored" in query:
                return {
                    "messages": 33,
                    "folders": 4,
                    "memberships": 35,
                    "transformed_assets": 0,
                    "shared_assets": 0,
                    "raw_originals_stored": 33,
                }
            if "WITH latest_current" in query:
                return {
                    "completed": 2,
                    "pending": 0,
                    "pending_retryable": 0,
                    "failed": 0,
                    "missing": 31,
                    "stale_hash": 0,
                }
            if "WITH current_sanitized" in query:
                return {"completed": 2, "pending": 31, "failed": 0}
            if "WITH captured" in query:
                return {
                    "captured": 9,
                    "recorded_reference_rows": 7,
                    "stored": 0,
                    "blocked": 0,
                    "failed": 0,
                    "unavailable": 3,
                    "pending": 6,
                    "recorded_unique_canonical_urls": 4,
                    "stored_unique_canonical_urls": 0,
                    "pending_recorded_unique_canonical_urls": 3,
                    "pending_reference_rows_with_shared_asset": 2,
                    "pending_unique_canonical_urls_with_shared_asset": 1,
                    "pending_reference_rows_needing_fetch": 4,
                    "pending_unique_canonical_urls_needing_fetch": 2,
                    "recorded_duplicate_reference_rows": 3,
                    "stored_shared_asset_links": 0,
                    "unlinked_stored_reference_rows": 0,
                    "shared_assets_stored": 0,
                    "shared_asset_encrypted_bytes": 0,
                }
            if "special_use_downloaded" in query:
                return {
                    "special_use_downloaded": 7,
                    "special_use_unmoved": 5,
                    "special_use_moved": 2,
                    "inbox_subfolders_moved": 5,
                }
            if "FROM pim_email_download_runs" in query:
                return None
            raise AssertionError(query)

        async def fetch(self, query, *args):
            if "UPDATE pim_email_download_runs" in query:
                return []
            if "FROM pim_email_security_phases" in query:
                return []
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    status = asyncio.run(FakeStore().local_corpus_status(mailbox_id="test-mailbox"))

    assert status["download_orphan_reconcile"]["marked_count"] == 0
    assert status["security_results"] == {
        "completed": 2,
        "pending": 0,
        "pending_retryable": 0,
        "failed": 0,
        "missing": 31,
        "stale_hash": 0,
    }
    assert status["render_gate"]["blocked_security_incomplete"] == 31
    assert status["external_image_derivatives"] == {
        "captured": 9,
        "recorded_reference_rows": 7,
        "stored": 0,
        "stored_reference_rows": 0,
        "blocked": 0,
        "failed": 0,
        "unavailable": 3,
        "pending": 6,
        "pending_reference_rows": 6,
        "recorded_unique_canonical_urls": 4,
        "stored_unique_canonical_urls": 0,
        "pending_recorded_unique_canonical_urls": 3,
        "pending_reference_rows_with_shared_asset": 2,
        "pending_unique_canonical_urls_with_shared_asset": 1,
        "pending_reference_rows_needing_fetch": 4,
        "pending_unique_canonical_urls_needing_fetch": 2,
        "recorded_duplicate_reference_rows": 3,
        "stored_shared_asset_links": 0,
        "unlinked_stored_reference_rows": 0,
        "shared_assets_stored": 0,
        "shared_asset_encrypted_bytes": 0,
    }
    assert status["special_use_folders"] == {
        "downloaded_memberships": 7,
        "unmoved_memberships": 5,
        "moved_memberships": 2,
    }
    external_query = next(query for query in queries if "recorded_reference_rows" in query)
    assert "recorded_unique_canonical_urls" in external_query
    assert "pending_unique_canonical_urls_needing_fetch" in external_query
    assert "stored_shared_asset_links" in external_query
    assert "unlinked_stored_reference_rows" in external_query
    folder_query = next(query for query in queries if "folder_effective" in query)
    assert "effective_special_use_role" in folder_query
    assert "'rubbish'" in folder_query
    assert "'sentitems'" in folder_query
    assert "'archived'" in folder_query


def test_ensure_schema_purges_incomplete_security_placeholders():
    queries = []

    class FakeConnection:
        async def execute(self, query, *args):
            queries.append(query)
            return None

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    asyncio.run(FakeStore().ensure_schema())

    cleanup_query = next(
        query for query in queries if "DELETE FROM pim_email_security_checks" in query
    )
    assert "security_status <> 'stored'" in cleanup_query
    assert "result_json->>'queued'" in cleanup_query
    assert "result_json->>'placeholder'" in cleanup_query
    assert "result_json->>'incomplete'" in cleanup_query


def test_ensure_schema_serializes_schema_ddl_with_advisory_lock():
    queries = []

    class FakeConnection:
        async def execute(self, query, *args):
            queries.append((query, args))
            return None

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    asyncio.run(FakeStore().ensure_schema())

    assert queries[0] == ("SELECT pg_advisory_lock($1)", (pim_email.PIM_EMAIL_SCHEMA_LOCK_ID,))
    assert queries[-1] == (
        "SELECT pg_advisory_unlock($1)",
        (pim_email.PIM_EMAIL_SCHEMA_LOCK_ID,),
    )


def test_ensure_schema_current_sentinel_skips_advisory_lock_and_ddl():
    calls = []

    class FakeConnection:
        async def fetchval(self, query, *args):
            calls.append(("fetchval", query, args))
            assert "information_schema.columns" in query
            return True

        async def execute(self, query, *args):
            calls.append(("execute", query, args))
            return None

        async def close(self):
            calls.append(("close", "", ()))
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    asyncio.run(FakeStore().ensure_schema())

    assert calls[0][0] == "fetchval"
    assert not any(call[0] == "execute" and "pg_advisory_lock" in call[1] for call in calls)
    assert not any(call[0] == "execute" and "ALTER TABLE" in call[1] for call in calls)


def test_backfill_orphan_reconcile_marks_only_non_active_running_runs():
    calls = []

    class FakeConnection:
        async def fetch(self, query, *args):
            calls.append((query, args))
            assert "status = 'interrupted-orphaned'" in query
            assert args[0] == "test-mailbox"
            if "UPDATE pim_email_backfill_runs" in query:
                assert args[1] == ["active-run"]
                metadata = json.loads(args[2])
                result = [{"run_id": "old-run"}]
            else:
                assert "UPDATE pim_email_backfill_items" in query
                metadata = json.loads(args[1])
                result = [{"run_id": "old-run"}]
            assert metadata["reason"] == "process-gone"
            assert metadata["active_run_ids"] == ["active-run"]
            return result

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(
        FakeStore().reconcile_orphaned_backfill_runs(
            active_run_ids={"active-run"},
            reason="process-gone",
            mailbox_id="test-mailbox",
        )
    )

    assert result["marked_orphaned"] == ["old-run"]
    assert result["marked_count"] == 1
    assert result["marked_item_count"] == 1
    assert len(calls) == 2


def test_auxiliary_backfill_batch_ledger_records_running_and_final():
    executed = []

    class FakeConnection:
        async def execute(self, query, *args):
            executed.append((query, args))
            return "UPDATE 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    store = FakeStore()
    batch = asyncio.run(
        store.start_backfill_auxiliary_batch(
            run_id="shared-link-run",
            mailbox_id="test-mailbox",
            artifact_types=["external_image_shared_asset_links"],
            requested_limit=1000,
            batch_index=7,
            metadata={"source": "test"},
        )
    )

    assert batch["run_id"] == "shared-link-run"
    assert batch["mailbox_id"] == "test-mailbox"
    assert batch["artifact_types"] == ["external_image_shared_asset_links"]
    assert batch["batch_id"].startswith("email-backfill-auxiliary-batch-")
    assert "INSERT INTO pim_email_backfill_runs" in executed[0][0]
    assert executed[0][1][0] == "shared-link-run"
    assert json.loads(executed[0][1][3]) == ["external_image_shared_asset_links"]
    assert json.loads(executed[0][1][4])["auxiliary_backfill"] is True
    assert "INSERT INTO pim_email_backfill_batches" in executed[1][0]

    asyncio.run(
        store.update_backfill_auxiliary_batch(
            run_id="shared-link-run",
            batch_id=batch["batch_id"],
            processed_count=23,
            failed_count=0,
            summary={"planned": 23, "linked": 23, "failed": 0},
            aggregate={"external_images_shared_asset_links": 23},
            final=True,
        )
    )

    run_update = executed[2]
    assert "UPDATE pim_email_backfill_runs" in run_update[0]
    assert run_update[1][1] == "completed"
    assert run_update[1][2] == 23
    assert run_update[1][3] == 0
    assert run_update[1][5] is True
    batch_update = executed[3]
    assert "UPDATE pim_email_backfill_batches" in batch_update[0]
    assert batch_update[1][1] == 23
    assert batch_update[1][2] == 23
    assert batch_update[1][3] == 0


def test_download_orphan_reconcile_marks_only_non_active_running_runs():
    calls = []

    class FakeConnection:
        async def fetch(self, query, *args):
            calls.append((query, args))
            assert "status = 'interrupted-orphaned'" in query
            assert "pim_email_download_runs" in query
            assert args[0] == "test-mailbox"
            assert args[1] == ["active-download"]
            metadata = json.loads(args[2])
            assert metadata["reason"] == "process-gone"
            assert metadata["active_run_ids"] == ["active-download"]
            return [{"run_id": "old-download"}]

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(
        FakeStore().reconcile_orphaned_download_runs(
            active_run_ids={"active-download"},
            reason="process-gone",
            mailbox_id="test-mailbox",
        )
    )

    assert result["marked_orphaned"] == ["old-download"]
    assert result["marked_count"] == 1
    assert calls


def test_active_download_run_ids_from_proc_parses_only_download_script_run_ids(tmp_path):
    active_dir = tmp_path / "123"
    active_dir.mkdir()
    (active_dir / "cmdline").write_bytes(
        b"python\0blueprints-app/scripts/pim_email_download_mailbox.py\0--run-id\0active-run\0"
    )
    ignored_dir = tmp_path / "456"
    ignored_dir.mkdir()
    (ignored_dir / "cmdline").write_bytes(
        b"python\0blueprints-app/scripts/pim_email_backfill.py\0--run-id\0wrong-run\0"
    )

    assert pim_email._active_download_run_ids_from_proc(tmp_path) == {"active-run"}


def test_record_download_run_start_resets_reused_run_terminal_fields():
    calls = []

    class FakeConnection:
        async def execute(self, query, *args):
            calls.append((query, args))
            assert "finished_at = NULL" in query
            assert "started_at = now()" in query
            assert "summary_json = '{}'::jsonb" in query
            return None

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    asyncio.run(
        FakeStore().record_download_run_start(
            run_id="download-run",
            mailbox_id="test-mailbox",
            apply_remote_moves=True,
            downloaded_folder="Downloaded",
            metadata={"schema": "test"},
        )
    )

    assert calls


def test_backfill_superseded_reconcile_marks_converged_failed_items():
    calls = []

    class FakeConnection:
        async def fetch(self, query, *args):
            calls.append((query, args))
            assert "status = 'superseded'" in query
            assert "artifact_converged_after_failed_attempt" in args[1]
            assert "pim_email_external_image_derivatives" in query
            assert "pim_email_security_checks" in query
            return [
                {"artifact_type": "external_images", "marked_count": 71},
                {"artifact_type": "security", "marked_count": 2},
            ]

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(
        FakeStore().reconcile_superseded_backfill_failures(mailbox_id="test-mailbox")
    )

    assert result["marked_count"] == 73
    assert result["by_artifact"] == {"external_images": 71, "security": 2}
    assert calls


def test_backfill_item_status_counts_groups_by_artifact_and_status():
    calls = []

    class FakeConnection:
        async def fetch(self, query, *args):
            calls.append((query, args))
            assert "GROUP BY artifact_type, status" in query
            assert args == ("run-1",)
            return [
                {"artifact_type": "security", "status": "superseded", "item_count": 1},
                {"artifact_type": "security", "status": "completed", "item_count": 7},
                {"artifact_type": "security_llm", "status": "failed", "item_count": 2},
            ]

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(FakeStore().backfill_item_status_counts(run_id="run-1"))

    assert result["by_artifact"]["security"] == {"superseded": 1, "completed": 7}
    assert result["by_artifact"]["security_llm"] == {"failed": 2}
    assert calls


def test_backfill_prioritizes_contract_incomplete_security_before_missing_security():
    queries = []

    class FakeConnection:
        async def fetch(self, query, *args):
            queries.append(query)
            if "WITH candidates AS" in query:
                return []
            return []

        async def execute(self, query, *args):
            queries.append(query)
            return None

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(
        FakeStore().run_backfill(
            mailbox_id="test-mailbox",
            artifact_types=["security"],
            limit=25,
            run_id="priority-test",
        )
    )

    candidate_query = next(query for query in queries if "WITH candidates AS" in query)
    run_upsert_query = next(
        query for query in queries if "INSERT INTO pim_email_backfill_runs" in query
    )
    assert result["ok"] is True
    assert "finished_at = NULL" in run_upsert_query
    assert "AS security_result_present" in candidate_query
    assert "security_result_present AND NOT security_complete THEN 1" in candidate_query
    assert "NOT security_complete THEN 2" in candidate_query
    assert "AS security_running" in candidate_query
    assert "AS sanitized_running" in candidate_query
    assert "AS external_running" in candidate_query
    assert "AND security_running" in candidate_query
    assert "AND (\n                    (TRUE AND NOT security_complete)" in candidate_query
    assert "OR (FALSE AND NOT sanitized_complete)" in candidate_query
    assert "OR (FALSE AND external_pending)" in candidate_query
    assert "d.status IN ('fetched','transformed','failed')" in candidate_query
    assert "d.status = 'pending'" in candidate_query
    assert "d.next_retry_at <= now()" in candidate_query
    assert "captured_waiting_for_real_download" in candidate_query
    assert "email_uid DESC" in candidate_query
    assert "updated_at ASC" not in candidate_query


def test_backfill_progress_updates_clear_terminal_run_fields():
    source = inspect.getsource(pim_email.PgEmailStore.run_backfill)

    assert "UPDATE pim_email_backfill_runs" in source
    assert "SET status = 'running'," in source
    assert "finished_at = NULL" in source


def test_backfill_cli_generated_external_rows_are_not_idle(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_STACK_RUNNER", "1")
    script_path = APP_ROOT / "scripts" / "pim_email_backfill.py"
    spec = importlib.util.spec_from_file_location("pim_email_backfill_test_module", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    result = {
        "summary": {
            "planned_messages": 0,
            "external_images_materialized_rows": 7,
        }
    }

    assert module._planned_messages(result) == 0
    assert module._generated_work_rows(result) == 7


def test_backfill_cli_retryable_external_image_urls_are_not_idle(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_STACK_RUNNER", "1")
    script_path = APP_ROOT / "scripts" / "pim_email_backfill.py"
    spec = importlib.util.spec_from_file_location("pim_email_backfill_test_module", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    result = {
        "summary": {
            "planned_messages": 0,
            "external_image_unique_urls_pending_retryable": 20,
        }
    }

    assert module._planned_messages(result) == 0
    assert module._generated_work_rows(result) == 20


def test_backfill_cli_superseded_items_do_not_count_as_failed(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_STACK_RUNNER", "1")
    script_path = APP_ROOT / "scripts" / "pim_email_backfill.py"
    spec = importlib.util.spec_from_file_location("pim_email_backfill_test_module", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    aggregate = {
        "failed_messages": 0,
        "security_failed": 9,
        "security_llm_failed": 0,
        "security_deterministic_failed": 0,
        "sanitized_views_failed": 0,
        "external_images_failed": 0,
        "external_images_shared_asset_link_failed": 0,
        "external_image_unique_urls_failed": 0,
        "external_image_unique_references_link_failed": 0,
    }
    module._sync_aggregate_item_status_counts(
        aggregate,
        {
            "by_artifact": {
                "security": {"completed": 12, "superseded": 1},
                "security_llm": {"failed": 2},
            }
        },
    )

    assert aggregate["security_failed"] == 0
    assert aggregate["security_superseded"] == 1
    assert aggregate["security_llm_failed"] == 2
    assert module._aggregate_failed_count(aggregate) == 2


def test_backfill_cli_repeat_artifact_keeps_run_active_until_loop_exit(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_STACK_RUNNER", "1")
    script_path = APP_ROOT / "scripts" / "pim_email_backfill.py"
    spec = importlib.util.spec_from_file_location("pim_email_backfill_test_module", script_path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    class FakeStore:
        def __init__(self):
            self.updates = []

        async def reconcile_orphaned_backfill_runs(self, **kwargs):
            return {}

        async def reconcile_superseded_backfill_failures(self, **kwargs):
            return {}

        async def run_backfill(self, **kwargs):
            return {
                "ok": True,
                "run_id": kwargs["run_id"],
                "status": "completed",
                "summary": {
                    "planned_messages": 1,
                    "processed_messages": 1,
                    "failed_messages": 0,
                    "raw_originals_verified": 1,
                    "security_completed": 1,
                },
            }

        async def update_backfill_run_summary(self, **kwargs):
            self.updates.append(kwargs)

    async def quiet_monitor(store, run_id, stop, *, interval_seconds=30.0):
        await stop.wait()

    store = FakeStore()
    monkeypatch.setattr(pim_email, "PgEmailStore", lambda: store)
    monkeypatch.setattr(module, "_monitor_backfill_progress", quiet_monitor)

    result = asyncio.run(
        module._run(
            SimpleNamespace(
                run_id="repeat-run",
                mailbox_id=None,
                email_uid=None,
                limit=None,
                batch_size=1,
                repeat_until_idle=True,
                idle_sleep_seconds=0,
                max_batches=1,
                artifact=["security"],
                materialize_external_image_rows=False,
                link_shared_external_image_assets=False,
            )
        )
    )

    assert result["ok"] is True
    assert [update["final"] for update in store.updates] == [False, True]
    assert store.updates[0]["processed_count"] == 1
    assert store.updates[0]["summary"]["security_completed"] == 1
    assert store.updates[1]["summary"]["stopped_reason"] == "max_batches"


def test_backfill_running_item_claim_is_atomic_and_refuses_live_duplicate():
    queries = []

    class FakeTransaction:
        async def __aenter__(self):
            queries.append(("transaction-enter", ()))
            return self

        async def __aexit__(self, exc_type, exc, tb):
            queries.append(("transaction-exit", (exc_type,)))
            return False

    class FakeConnection:
        def transaction(self):
            return FakeTransaction()

        async def execute(self, query, *args):
            queries.append((query, args))
            return None

        async def fetchval(self, query, *args):
            queries.append((query, args))
            assert "pim_email_backfill_items" in query
            assert args == (
                "test-mailbox",
                "20260702-" + "e" * 40,
                "raw-hash",
                "security",
                "run-a",
            )
            return "run-b"

        async def close(self):
            queries.append(("close", ()))
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    claimed = asyncio.run(
        FakeStore()._record_backfill_item(
            run_id="run-a",
            batch_id="batch-a",
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "e" * 40,
            raw_sha256="raw-hash",
            artifact_type="security",
            status="running",
            metadata={"phase": "security"},
        )
    )

    assert claimed is False
    assert any("pg_advisory_xact_lock" in item[0] for item in queries)
    assert not any("INSERT INTO pim_email_backfill_items" in item[0] for item in queries)
    assert queries[-1] == ("close", ())


def test_backfill_running_item_claim_refuses_already_converged_artifact():
    queries = []
    fetchval_calls = 0

    class FakeTransaction:
        async def __aenter__(self):
            queries.append(("transaction-enter", ()))
            return self

        async def __aexit__(self, exc_type, exc, tb):
            queries.append(("transaction-exit", (exc_type,)))
            return False

    class FakeConnection:
        def transaction(self):
            return FakeTransaction()

        async def execute(self, query, *args):
            queries.append((query, args))
            return None

        async def fetchval(self, query, *args):
            nonlocal fetchval_calls
            fetchval_calls += 1
            queries.append((query, args))
            if fetchval_calls == 1:
                assert "pim_email_backfill_items" in query
                return None
            assert "already" not in query.lower()
            assert "pim_email_security_checks" in query
            assert args == (
                "test-mailbox",
                "20260702-" + "f" * 40,
                "raw-hash",
                "security_llm",
            )
            return True

        async def close(self):
            queries.append(("close", ()))
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    claimed = asyncio.run(
        FakeStore()._record_backfill_item(
            run_id="run-a",
            batch_id="batch-a",
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "f" * 40,
            raw_sha256="raw-hash",
            artifact_type="security_llm",
            status="running",
            metadata={"phase": "security-llm-final"},
        )
    )

    assert claimed is False
    assert fetchval_calls == 2
    assert any("pg_advisory_xact_lock" in item[0] for item in queries)
    assert not any("INSERT INTO pim_email_backfill_items" in item[0] for item in queries)
    assert queries[-1] == ("close", ())


def test_backfill_skips_raw_decrypt_when_stale_row_cannot_be_claimed(monkeypatch):
    queries = []
    record_calls = []
    uid = "20260702-" + "a" * 40

    def fail_read_encrypted_bytes(path):
        raise AssertionError(f"raw decrypt should not run for unclaimed stale row: {path}")

    monkeypatch.setattr(pim_email, "read_encrypted_bytes", fail_read_encrypted_bytes)

    class FakeConnection:
        async def fetch(self, query, *args):
            queries.append((query, args))
            if "WITH candidates AS" in query:
                return [
                    {
                        "email_uid": uid,
                        "mailbox_id": "test-mailbox",
                        "raw_sha256": "raw-hash",
                        "storage_relpath": "stale/message.eml.enc",
                        "updated_at": None,
                    }
                ]
            return []

        async def execute(self, query, *args):
            queries.append((query, args))
            return None

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def _record_backfill_item(self, **kwargs):
            record_calls.append(kwargs)
            return False

    result = asyncio.run(
        FakeStore().run_backfill(
            mailbox_id="test-mailbox",
            artifact_types=["security_llm"],
            limit=1,
            run_id="stale-row-skip",
        )
    )

    assert result["ok"] is True
    assert result["summary"]["planned_messages"] == 1
    assert result["summary"]["processed_messages"] == 0
    assert result["summary"]["raw_originals_verified"] == 0
    assert record_calls == [
        {
            "run_id": "stale-row-skip",
            "batch_id": result["batch_id"],
            "mailbox_id": "test-mailbox",
            "email_uid": uid,
            "raw_sha256": "raw-hash",
            "artifact_type": "security_llm",
            "status": "running",
            "metadata": {"phase": "security-llm-final"},
        }
    ]


def test_external_image_materializer_creates_missing_rows_without_overwriting_existing():
    uid_a = "20260702-" + "a" * 40
    uid_b = "20260702-" + "b" * 40
    uid_existing = "20260702-" + "c" * 40
    queries = []
    existing = {
        (
            "test-mailbox",
            uid_existing,
            "raw-existing",
            pim_email._external_image_canonical_digest("https://cdn.example.test/stored.png"),
        )
    }
    inserted = []

    class FakeConnection:
        async def fetch(self, query, *args):
            queries.append(query)
            if "selected_messages" in query:
                assert args == ("test-mailbox",)
                return [
                    {
                        "email_uid": uid_a,
                        "mailbox_id": "test-mailbox",
                        "raw_sha256": "raw-a",
                        "source_url": "https://cdn.example.test/a.png",
                    },
                    {
                        "email_uid": uid_a,
                        "mailbox_id": "test-mailbox",
                        "raw_sha256": "raw-a",
                        "source_url": "https://cdn.example.test/a.png",
                    },
                    {
                        "email_uid": uid_existing,
                        "mailbox_id": "test-mailbox",
                        "raw_sha256": "raw-existing",
                        "source_url": "https://cdn.example.test/stored.png",
                    },
                    {
                        "email_uid": uid_b,
                        "mailbox_id": "test-mailbox",
                        "raw_sha256": "raw-b",
                        "source_url": "https://cdn.example.test/b.png",
                    },
                ]
            if "INSERT INTO pim_email_external_image_derivatives" in query:
                rows = []
                for index, digest in enumerate(args[6]):
                    key = (args[2][index], args[1][index], args[3][index], digest)
                    if key in existing:
                        continue
                    existing.add(key)
                    inserted.append(
                        {
                            "status": args[7][index],
                            "reason": args[8][index],
                            "safety_decision": args[9][index],
                            "transform_version": args[10][index],
                            "metadata_json": args[11][index],
                        }
                    )
                    rows.append({"derivative_id": args[0][index]})
                return rows
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

    result = asyncio.run(
        FakeStore().materialize_external_image_derivative_rows(mailbox_id="test-mailbox")
    )

    assert result == {
        "captured_sources": 4,
        "candidate_rows": 3,
        "materialized_rows": 2,
    }
    selected_query = next(query for query in queries if "selected_messages" in query)
    assert "count(DISTINCT d.canonical_url_digest)" in selected_query
    assert "pim_email_external_image_derivatives" in selected_query
    assert "ORDER BY email_uid DESC" in selected_query
    assert {item["status"] for item in inserted} == {"pending"}
    assert {item["safety_decision"] for item in inserted} == {"pending_real_download"}
    assert {item["reason"] for item in inserted} == {"captured_waiting_for_real_download"}


def test_external_image_failed_rows_are_requeued_as_worker_candidates(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    failed_url = "https://cdn.example.test/retry.png"
    transient_url = "https://cdn.example.test/transient.png"
    gone_url = "https://cdn.example.test/gone.png"
    stored_url = "https://cdn.example.test/already.jpg"
    recorded = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives" in query:
                return [
                    {"canonical_url": failed_url, "status": "failed"},
                    {
                        "canonical_url": transient_url,
                        "status": "unavailable",
                        "reason": "image unavailable: ConnectError",
                    },
                    {
                        "canonical_url": gone_url,
                        "status": "unavailable",
                        "reason": "image unavailable: HTTP 404",
                    },
                    {"canonical_url": stored_url, "status": "stored"},
                ]
            raise AssertionError(query)

        async def execute(self, query, *args):
            assert "pg_advisory_" in query
            return "SELECT 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def store_transformed_asset(self, **kwargs):
            return {"ok": True, "asset": kwargs["asset"]}

        async def store_shared_asset(self, **kwargs):
            asset = dict(kwargs["asset"])
            asset.setdefault("shared_asset_uid", "email-shared-asset-test")
            asset.setdefault("source_url", "https://cdn.example.test/image.png")
            asset.setdefault("canonical_url_digest", "digest-test")
            asset.setdefault("raw_image_sha256", asset.get("raw_sha256", "raw-image-hash"))
            return asset

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

        async def find_shared_asset_for_url(self, **kwargs):
            return None

        async def find_external_image_canonical_terminal_state(self, **kwargs):
            return None

    async def fail_fetch(source):
        raise AssertionError("message-level image handling must not fetch directly")

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", fail_fetch)

    counts = asyncio.run(
        FakeStore().process_external_image_derivatives(
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "d" * 40,
            input_raw_sha256="raw",
            source_urls=[failed_url, transient_url, gone_url, stored_url],
            metadata={"proof": "retry"},
        )
    )

    assert counts["attempted"] == 0
    assert counts["pending"] == 2
    assert counts["stored"] == 0
    assert counts["unavailable"] == 0
    assert counts["already_stored"] == 1
    assert counts["already_unavailable"] == 1
    assert counts["failed"] == 0
    assert {item["source_url"] for item in recorded} == {failed_url, transient_url}
    assert {item["status"] for item in recorded} == {"pending"}
    assert {item["safety_decision"] for item in recorded} == {"pending_real_download"}
    assert {item["reason"] for item in recorded} == {"captured_waiting_for_real_download"}


def test_external_image_derivative_reuses_verified_shared_asset_without_refetch(monkeypatch):
    stored = []
    recorded = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives" in query:
                return []
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def find_shared_asset_for_url(self, **kwargs):
            return {
                "shared_asset_uid": "email-shared-asset-existing",
                "source_url": kwargs["source_url"],
                "canonical_url": kwargs["source_url"],
                "canonical_url_digest": pim_email._external_image_canonical_digest(
                    kwargs["source_url"]
                ),
                "content_type": "image/jpeg",
                "raw_image_sha256": "raw-image-hash",
                "transformed_sha256": "transformed-image-hash",
                "storage_relpath": "assets/aa/existing/image.jpg.enc",
                "encrypted_size": 456,
                "width": 12,
                "height": 9,
                "transform_version": "jpeg-v1",
                "encryption": {"alg": "test"},
            }

        async def store_transformed_asset(self, **kwargs):
            stored.append(kwargs["asset"])
            return kwargs["asset"]

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

    async def fail_fetch(source):
        raise AssertionError("duplicate URL should reuse verified shared asset")

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", fail_fetch)

    counts = asyncio.run(
        FakeStore().process_external_image_derivatives(
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "e" * 40,
            input_raw_sha256="raw",
            source_urls=["https://cdn.example.test/dup.png"],
            metadata={"proof": "reuse"},
        )
    )

    assert counts["stored"] == 1
    assert counts["already_stored"] == 0
    assert counts["attempted"] == 0
    assert stored[0]["shared_asset_uid"] == "email-shared-asset-existing"
    assert stored[0]["storage_relpath"].startswith("assets/")
    assert recorded[0]["safety_decision"] == "reused_verified_shared_encrypted_asset"
    assert recorded[0]["shared_asset_uid"] == "email-shared-asset-existing"


def test_external_image_derivative_records_pending_without_url_lock_or_fetch(monkeypatch):
    stored = []
    recorded = []
    executed = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives" in query:
                return []
            raise AssertionError(query)

        async def execute(self, query, *args):
            executed.append(query)
            return "SELECT 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def find_shared_asset_for_url(self, **kwargs):
            return None

        async def find_external_image_canonical_terminal_state(self, **kwargs):
            return None

        async def store_transformed_asset(self, **kwargs):
            stored.append(kwargs["asset"])
            return kwargs["asset"]

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

    async def fail_fetch(source):
        raise AssertionError("message-level image handling must not fetch directly")

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", fail_fetch)

    counts = asyncio.run(
        FakeStore().process_external_image_derivatives(
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "f" * 40,
            input_raw_sha256="raw",
            source_urls=["https://cdn.example.test/race.png"],
            metadata={"proof": "lock-recheck"},
        )
    )

    assert counts["pending"] == 1
    assert counts["stored"] == 0
    assert counts["attempted"] == 0
    assert executed == []
    assert stored == []
    assert recorded[0]["safety_decision"] == "pending_real_download"
    assert recorded[0]["reason"] == "captured_waiting_for_real_download"


def test_external_image_derivative_reuses_canonical_unavailable_outcome(monkeypatch):
    recorded = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives" in query:
                return []
            raise AssertionError(query)

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def find_shared_asset_for_url(self, **kwargs):
            return None

        async def find_external_image_canonical_terminal_state(self, **kwargs):
            return {
                "derivative_id": "prior-outcome",
                "status": "unavailable",
                "reason": "image unavailable: HTTP 404",
            }

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

    async def fail_fetch(source):
        raise AssertionError("canonical unavailable outcome should prevent refetch")

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", fail_fetch)

    counts = asyncio.run(
        FakeStore().process_external_image_derivatives(
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "a" * 40,
            input_raw_sha256="raw",
            source_urls=["https://cdn.example.test/gone.png"],
            metadata={"proof": "canonical-unavailable"},
        )
    )

    assert counts["unavailable"] == 1
    assert counts["attempted"] == 0
    assert recorded[0]["status"] == "unavailable"
    assert recorded[0]["reason"] == "image unavailable: HTTP 404"
    assert recorded[0]["safety_decision"] == "reused_canonical_unavailable_external_image_outcome"


def test_link_external_image_references_from_shared_assets_without_network(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    transformed = b"jpeg-ish-shared-bytes"
    transformed_hash = pim_email.hashlib.sha256(transformed).hexdigest()
    storage = pim_email.write_encrypted_bytes_atomic(
        relpath="assets/aa/shared/image.jpg.enc",
        content=transformed,
        purpose=pim_email.ASSET_PURPOSE,
    )
    stored = []
    recorded = []
    executed = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives d" in query:
                assert "'failed'" in query
                return [
                    {
                        "derivative_id": "derivative-1",
                        "email_uid": "20260702-" + "b" * 40,
                        "mailbox_id": "test-mailbox",
                        "input_raw_sha256": "raw",
                        "source_url": "https://cdn.example.test/shared.png",
                        "canonical_url": "https://cdn.example.test/shared.png",
                        "canonical_url_digest": pim_email._external_image_canonical_digest(
                            "https://cdn.example.test/shared.png"
                        ),
                        "shared_asset_uid": "email-shared-asset-test",
                        "content_type": "image/jpeg",
                        "raw_image_sha256": "raw-image-hash",
                        "transformed_sha256": transformed_hash,
                        "storage_relpath": storage["storage_relpath"],
                        "encrypted_size": storage["encrypted_size"],
                        "width": 2,
                        "height": 1,
                        "transform_version": "jpeg-v1",
                        "encryption_json": storage["encryption"],
                        "metadata_json": {"proof": "shared"},
                    }
                ]
            raise AssertionError(query)

        async def execute(self, query, *args):
            executed.append((query, args))
            return "UPDATE 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def store_transformed_asset(self, **kwargs):
            stored.append(kwargs["asset"])
            return kwargs["asset"]

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

    summary = asyncio.run(
        FakeStore().link_external_image_references_from_shared_assets(
            mailbox_id="test-mailbox",
            limit=10,
            metadata={"run_id": "shared-link-run", "batch_id": "shared-link-batch"},
        )
    )

    assert summary["planned"] == 1
    assert summary["linked"] == 1
    assert summary["failed"] == 0
    assert stored[0]["storage_relpath"] == storage["storage_relpath"]
    assert stored[0]["metadata"]["storage_kind"] == "shared-encrypted-asset"
    assert recorded[0]["status"] == "stored"
    assert recorded[0]["safety_decision"] == "reused_verified_shared_encrypted_asset_bulk_link"
    assert stored[0]["shared_asset_uid"] == "email-shared-asset-test"
    assert stored[0]["metadata"]["run_id"] == "shared-link-run"
    assert stored[0]["metadata"]["batch_id"] == "shared-link-batch"
    assert recorded[0]["metadata"]["run_id"] == "shared-link-run"
    assert recorded[0]["metadata"]["batch_id"] == "shared-link-batch"
    assert any("UPDATE pim_email_shared_assets" in query for query, _ in executed)


def test_unique_external_image_asset_fetches_once_and_links_all_references(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    canonical = "https://cdn.example.test/unique.png"
    digest = pim_email._external_image_canonical_digest(canonical)
    transformed = b"shared-jpeg-bytes"
    transformed_hash = pim_email.hashlib.sha256(transformed).hexdigest()
    storage = pim_email.write_encrypted_bytes_atomic(
        relpath=f"assets/{digest[:2]}/{digest}/shared.jpg.enc",
        content=transformed,
        purpose=pim_email.ASSET_PURPOSE,
    )
    fetches = []
    shared_assets = []
    stored_refs = []
    recorded_refs = []
    finished_assignments = []

    class FakeConnection:
        async def fetch(self, query, *args):
            if "FROM pim_email_external_image_derivatives d" in query:
                assert args[2] == digest
                return [
                    {
                        "derivative_id": "derivative-1",
                        "email_uid": "20260702-" + "c" * 40,
                        "mailbox_id": "test-mailbox",
                        "input_raw_sha256": "raw-latest",
                        "source_url": canonical + "?utm=proof",
                        "canonical_url": canonical,
                        "canonical_url_digest": digest,
                        "shared_asset_uid": "email-shared-asset-unique",
                        "content_type": "image/jpeg",
                        "raw_image_sha256": "raw-image-hash",
                        "transformed_sha256": transformed_hash,
                        "storage_relpath": storage["storage_relpath"],
                        "encrypted_size": storage["encrypted_size"],
                        "width": 2,
                        "height": 1,
                        "transform_version": "jpeg-v1",
                        "encryption_json": storage["encryption"],
                        "metadata_json": {"proof": "unique"},
                    },
                    {
                        "derivative_id": "derivative-2",
                        "email_uid": "20260702-" + "d" * 40,
                        "mailbox_id": "test-mailbox",
                        "input_raw_sha256": "raw-older",
                        "source_url": canonical,
                        "canonical_url": canonical,
                        "canonical_url_digest": digest,
                        "shared_asset_uid": "email-shared-asset-unique",
                        "content_type": "image/jpeg",
                        "raw_image_sha256": "raw-image-hash",
                        "transformed_sha256": transformed_hash,
                        "storage_relpath": storage["storage_relpath"],
                        "encrypted_size": storage["encrypted_size"],
                        "width": 2,
                        "height": 1,
                        "transform_version": "jpeg-v1",
                        "encryption_json": storage["encryption"],
                        "metadata_json": {"proof": "unique"},
                    },
                ]
            raise AssertionError(query)

        async def execute(self, query, *args):
            return "SELECT 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def find_shared_asset_for_url(self, **kwargs):
            return None

        async def find_external_image_canonical_terminal_state(self, **kwargs):
            return None

        async def claim_external_image_url_assignment_block(self, **kwargs):
            assert kwargs["limit"] == 10
            return {
                "assignment_batch_id": "assignment-batch-test",
                "assignment_token": "assignment-token-test",
                "items": [
                    {
                        "canonical_url_digest": digest,
                        "canonical_url": canonical,
                        "source_url": canonical + "?utm=proof",
                        "email_uid": "20260702-" + "c" * 40,
                        "input_raw_sha256": "raw-latest",
                    }
                ],
            }

        async def _finish_external_image_url_assignment(self, **kwargs):
            finished_assignments.append(kwargs)
            return kwargs

        async def store_shared_asset(self, **kwargs):
            shared_assets.append(kwargs["asset"])
            return kwargs["asset"]

        async def store_transformed_asset(self, **kwargs):
            stored_refs.append(kwargs["asset"])
            return kwargs["asset"]

        async def record_external_image_derivative_state(self, **kwargs):
            recorded_refs.append(kwargs)
            return kwargs

    async def fake_fetch(source):
        fetches.append(source)
        return {"content": b"image-bytes", "content_type": "image/png", "final_url": source}

    def fake_asset(**kwargs):
        assert kwargs["source_url"] == canonical
        return {
            "shared_asset_uid": "email-shared-asset-unique",
            "mailbox_id": kwargs["mailbox_id"],
            "source_url": canonical,
            "canonical_url_digest": digest,
            "content_type": "image/jpeg",
            "raw_sha256": "raw-image-hash",
            "raw_image_sha256": "raw-image-hash",
            "transformed_sha256": transformed_hash,
            "storage_relpath": storage["storage_relpath"],
            "encrypted_size": storage["encrypted_size"],
            "width": 2,
            "height": 1,
            "transform_version": "jpeg-v1",
            "encryption": storage["encryption"],
            "metadata": kwargs["metadata"],
        }

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", fake_fetch)
    monkeypatch.setattr(pim_email, "build_transformed_external_image_asset", fake_asset)

    summary = asyncio.run(
        FakeStore().process_external_image_unique_canonical_assets(
            mailbox_id="test-mailbox",
            limit=10,
            metadata={"run_id": "unique-run", "batch_id": "unique-batch"},
            allow_coordinator_transform_fallback=True,
        )
    )

    assert fetches == [canonical + "?utm=proof"]
    assert len(shared_assets) == 1
    assert summary["planned_unique_urls"] == 1
    assert summary["attempted_unique_urls"] == 1
    assert summary["stored_unique_urls"] == 1
    assert summary["references_linked"] == 2
    assert summary["failed_unique_urls"] == 0
    assert len(stored_refs) == 2
    assert {item["shared_asset_uid"] for item in stored_refs} == {"email-shared-asset-unique"}
    assert {item["status"] for item in recorded_refs} == {"stored"}
    assert finished_assignments
    assert finished_assignments[0]["assignment_status"] == "completed"
    assert finished_assignments[0]["result_status"] == "stored"


def test_unique_external_image_asset_timeout_is_pending_retryable(monkeypatch):
    canonical = "https://cdn.example.test/timeout.png"
    digest = pim_email._external_image_canonical_digest(canonical)
    recorded = []
    finished_assignments = []

    class FakeConnection:
        async def fetch(self, query, *args):
            raise AssertionError(query)

        async def execute(self, query, *args):
            return "SELECT 1"

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return self.connection

        async def find_shared_asset_for_url(self, **kwargs):
            return None

        async def find_external_image_canonical_terminal_state(self, **kwargs):
            return None

        async def claim_external_image_url_assignment_block(self, **kwargs):
            return {
                "assignment_batch_id": "assignment-batch-test",
                "assignment_token": "assignment-token-test",
                "items": [
                    {
                        "canonical_url_digest": digest,
                        "canonical_url": canonical,
                        "source_url": canonical,
                        "email_uid": "20260702-" + "e" * 40,
                        "input_raw_sha256": "raw-timeout",
                    }
                ],
            }

        async def _finish_external_image_url_assignment(self, **kwargs):
            finished_assignments.append(kwargs)
            return kwargs

        async def record_external_image_derivative_state(self, **kwargs):
            recorded.append(kwargs)
            return kwargs

    async def timeout_fetch(source):
        raise pim_email.EmailOperationError("image unavailable: ReadTimeout")

    monkeypatch.setattr(pim_email, "fetch_remote_image_bytes", timeout_fetch)

    summary = asyncio.run(
        FakeStore().process_external_image_unique_canonical_assets(
            mailbox_id="test-mailbox",
            limit=10,
            metadata={"run_id": "unique-run"},
            allow_coordinator_transform_fallback=True,
        )
    )

    assert summary["attempted_unique_urls"] == 1
    assert summary["pending_retryable_unique_urls"] == 1
    assert summary["failed_unique_urls"] == 0
    assert summary["stored_unique_urls"] == 0
    assert summary["retryable"][0]["canonical_url_digest"] == digest
    assert recorded[0]["status"] == "pending"
    assert (
        recorded[0]["safety_decision"]
        == "pending_during_unique_external_image_download_or_transform"
    )
    assert finished_assignments
    assert finished_assignments[0]["assignment_status"] == "retryable"
    assert finished_assignments[0]["result_status"] == "pending"


def test_unique_external_image_asset_fallback_is_explicit(monkeypatch):
    monkeypatch.delenv(
        "BLUEPRINTS_EMAIL_ALLOW_COORDINATOR_IMAGE_TRANSFORM_FALLBACK",
        raising=False,
    )

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            return None

        async def ensure_schema(self):
            return None

        async def claim_external_image_url_assignment_block(self, **kwargs):
            raise AssertionError("fallback guard should fire before assignment claim")

    with pytest.raises(pim_email.EmailOperationError, match="fallback is disabled"):
        asyncio.run(
            FakeStore().process_external_image_unique_canonical_assets(
                mailbox_id="test-mailbox",
                limit=10,
            )
        )


def test_message_external_image_path_feeds_assignment_flow_without_direct_download():
    source = inspect.getsource(pim_email.PgEmailStore.process_external_image_derivatives)

    assert "fetch_remote_image_bytes" not in source
    assert "build_transformed_external_image_asset" not in source
    assert "captured_waiting_for_real_download" in source
    assert "pending_real_download" in source


def test_external_image_worker_asset_file_work_is_off_event_loop():
    complete_source = inspect.getsource(
        pim_email.PgEmailStore.complete_external_image_url_assignment_with_transformed_payload
    )
    store_shared_source = inspect.getsource(pim_email.PgEmailStore.store_shared_asset)
    link_source = inspect.getsource(
        pim_email.PgEmailStore.link_external_image_references_from_shared_assets
    )

    assert "await asyncio.to_thread(" in complete_source
    assert "build_transformed_external_image_asset_from_worker_payload" in complete_source
    assert "await asyncio.to_thread(" in store_shared_source
    assert "_verify_encrypted_asset_sha256" in store_shared_source
    assert "await asyncio.to_thread(" in link_source
    assert "_verify_encrypted_asset_sha256" in link_source


def test_external_image_url_assignment_block_claim_uses_unique_resumable_assignments():
    canonical = "https://cdn.example.test/assignment.png"
    digest = pim_email._external_image_canonical_digest(canonical)
    queries = []

    class FakeConnection:
        async def fetch(self, query, *args):
            queries.append(query)
            if "worker_id = $2" in query and "run_id = $3" in query:
                return []
            assert "SELECT DISTINCT ON (d.canonical_url_digest)" in query
            assert "ON CONFLICT (mailbox_id, canonical_url_digest, transform_version)" in query
            assert "waiting.next_retry_at > now()" in query
            assert "captured_waiting_for_real_download" in query
            assert "l.assignment_status = 'unassigned'" in query
            assert "l.assignment_status IN ('unassigned','retryable','failed')" not in query
            assert "l.result_status IN ('blocked','unavailable')" not in query
            assert "pim_email_external_image_url_assignments.expires_at <= now()" not in query
            return [
                {
                    "assignment_id": "assignment-1",
                    "assignment_batch_id": "assignment-batch-1",
                    "mailbox_id": "test-mailbox",
                    "canonical_url_digest": digest,
                    "canonical_url": canonical,
                    "source_url": canonical + "?utm=proof",
                    "email_uid": "20260702-" + "f" * 40,
                    "input_raw_sha256": "raw-assignment",
                    "transform_version": "jpeg-v1",
                    "assignment_status": "assigned",
                    "worker_id": "tb3-worker-01",
                    "assignment_token": "secret-token",
                    "run_id": "stable-run-1",
                    "attempts": 1,
                    "result_status": "",
                    "reason": "",
                    "metadata_json": {},
                    "claimed_at": None,
                    "renewed_at": None,
                    "expires_at": None,
                    "next_retry_at": None,
                    "completed_at": None,
                    "updated_at": None,
                }
            ]

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            return None

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return FakeConnection()

    result = asyncio.run(
        FakeStore().claim_external_image_url_assignment_block(
            mailbox_id="test-mailbox",
            worker_id="tb3-worker-01",
            run_id="stable-run-1",
            limit=1000,
        )
    )

    assert result["claimed"] == 1
    assert result["items"][0]["canonical_url_digest"] == digest
    assert "assignment_token" not in result["items"][0]
    assert len(queries) == 2


def test_external_image_url_assignment_block_claim_resumes_same_worker_run_before_new_scan():
    digest = pim_email._external_image_canonical_digest("https://cdn.example.test/resume.png")
    queries = []

    class FakeConnection:
        async def fetch(self, query, *args):
            queries.append(query)
            assert "worker_id = $2" in query and "run_id = $3" in query
            assert "assignment_status = 'assigned'" in query
            assert "assignment_status = 'retryable'" not in query
            return [
                {
                    "assignment_id": "assignment-resume",
                    "assignment_batch_id": "assignment-batch-resume",
                    "mailbox_id": "test-mailbox",
                    "canonical_url_digest": digest,
                    "canonical_url": "https://cdn.example.test/resume.png",
                    "source_url": "https://cdn.example.test/resume.png",
                    "email_uid": "20260702-" + "a" * 40,
                    "input_raw_sha256": "raw-resume",
                    "transform_version": "jpeg-v1",
                    "assignment_status": "assigned",
                    "worker_id": "tb3-worker-01",
                    "assignment_token": "secret-token",
                    "run_id": "stable-run-1",
                    "attempts": 2,
                    "result_status": "",
                    "reason": "",
                    "metadata_json": {},
                    "claimed_at": None,
                    "renewed_at": None,
                    "expires_at": None,
                    "next_retry_at": None,
                    "completed_at": None,
                    "updated_at": None,
                }
            ]

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            return None

        async def ensure_schema(self):
            return None

        async def _connect(self):
            return FakeConnection()

    result = asyncio.run(
        FakeStore().claim_external_image_url_assignment_block(
            mailbox_id="test-mailbox",
            worker_id="tb3-worker-01",
            run_id="stable-run-1",
            limit=1000,
        )
    )

    assert result["claimed"] == 1
    assert result["items"][0]["canonical_url_digest"] == digest
    assert len(queries) == 1


def test_record_external_image_pending_retryable_sets_retry_metadata():
    canonical = "https://cdn.example.test/retry.png"
    captured = {}

    class FakeConnection:
        async def fetchrow(self, query, *args):
            captured["query"] = query
            captured["args"] = args
            assert "retry_count" in query
            assert "next_retry_at" in query
            assert args[20] is True
            assert args[21] == pim_email.EXTERNAL_IMAGE_RETRY_DELAY_SECONDS
            return {
                "derivative_id": "email-image-derivative-test",
                "email_uid": "20260702-" + "f" * 40,
                "input_raw_sha256": "raw-retry",
                "source_url": canonical,
                "canonical_url": canonical,
                "status": "pending",
                "reason": "image unavailable: ReadTimeout",
                "safety_decision": "pending_during_unique_external_image_download_or_transform",
                "transform_version": pim_email.EXTERNAL_IMAGE_DERIVATIVE_VERSION,
                "raw_image_sha256": "",
                "transformed_sha256": "",
                "storage_relpath": "",
                "shared_asset_uid": "",
                "encrypted_size": 0,
                "content_type": "",
                "width": 0,
                "height": 0,
                "retry_count": 3,
                "last_error": "image unavailable: ReadTimeout",
                "next_retry_at": "2026-07-02T19:30:00+00:00",
                "updated_at": "2026-07-02T19:15:00+00:00",
            }

        async def close(self):
            return None

    class FakeStore(pim_email.PgEmailStore):
        def __init__(self):
            self.connection = FakeConnection()

        async def _connect(self):
            return self.connection

    row = asyncio.run(
        FakeStore().record_external_image_derivative_state(
            mailbox_id="test-mailbox",
            email_uid="20260702-" + "f" * 40,
            input_raw_sha256="raw-retry",
            source_url=canonical,
            status="pending",
            reason="image unavailable: ReadTimeout",
            safety_decision="pending_during_unique_external_image_download_or_transform",
            transform_version=pim_email.EXTERNAL_IMAGE_DERIVATIVE_VERSION,
            ensure_schema=False,
        )
    )

    assert "pim_email_external_image_derivatives.retry_count + 1" in captured["query"]
    assert row["retry_count"] == 3
    assert row["last_error"] == "image unavailable: ReadTimeout"
    assert row["next_retry_at"] == "2026-07-02T19:30:00+00:00"


def test_external_image_error_classification_never_uses_skipped():
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image host resolved to a private or unsafe address")
        )
        == "blocked"
    )
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image could not be decoded safely")
        )
        == "blocked"
    )
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image payload is too large")
        )
        == "blocked"
    )
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image unavailable: HTTP 404")
        )
        == "unavailable"
    )
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image unavailable: redirect chain exceeded 20 redirects")
        )
        == "unavailable"
    )
    assert (
        pim_email._external_image_error_status(
            pim_email.EmailOperationError("image unavailable: ReadTimeout")
        )
        == "pending"
    )


def test_external_image_max_bytes_defaults_above_observed_corpus_and_is_configurable(
    monkeypatch,
):
    monkeypatch.delenv("BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_BYTES", raising=False)

    assert pim_email._remote_image_max_bytes() == 25 * 1024 * 1024

    monkeypatch.setenv("BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_BYTES", str(32 * 1024 * 1024))
    assert pim_email._remote_image_max_bytes() == 32 * 1024 * 1024

    monkeypatch.setenv("BLUEPRINTS_EMAIL_REMOTE_IMAGE_MAX_BYTES", "invalid")
    assert pim_email._remote_image_max_bytes() == 25 * 1024 * 1024


def test_oversize_security_llm_state_is_scored_not_marked_skipped(monkeypatch):
    body = "ab" * ((pim_email_security.MAX_LLM_CHARS // 2) + 1)
    findings = []

    state = pim_email_security._llm_findings(None, body, findings)

    assert state["called"] is False
    assert state["not_called_reason"] == "oversize_deterministic_risk_result"
    assert "skipped_reason" not in state
    assert any(item["code"] == "LLM_BODY_OVERSIZE" for item in findings)


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
    assert parsed["views_available"] == {
        "plain": True,
        "html": True,
        "markdown": False,
        "raw": True,
    }
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
    assert parsed["views_available"] == {
        "plain": True,
        "html": False,
        "markdown": True,
        "raw": True,
    }


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
    def __init__(self, host, port, *, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
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


def _download_raw(uid: str, *, subject: str | None = None) -> bytes:
    subject = subject or f"Download {uid}"
    return (
        f"Subject: {subject}\r\n"
        "From: Sender <sender@example.test>\r\n"
        "To: User <user@example.test>\r\n"
        f"Date: Wed, 01 Jul 2026 09:{int(uid) % 60:02d}:00 +0000\r\n"
        f"Message-ID: <download-{uid}@example.test>\r\n"
        "Content-Type: multipart/alternative; boundary=x\r\n\r\n"
        "--x\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n"
        f"Plain body {uid}\r\n"
        "--x\r\nContent-Type: text/html; charset=utf-8\r\n\r\n"
        f'<p>HTML {uid}</p><img src="https://images.example.test/{uid}.png?track=1">\r\n'
        "--x--\r\n"
    ).encode()


class DownloadFakeIMAP:
    instances = []

    def __init__(self, host, port, *, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.current_folder = ""
        self.search_calls = {"INBOX": 0, "Archive": 0, "Drafts": 0}
        self.messages = {
            "INBOX": {"41": _download_raw("41"), "42": _download_raw("42")},
            "Archive": {"9": _download_raw("9", subject="Archive message")},
            "Drafts": {"8": _download_raw("8", subject="Draft message")},
        }
        self.moves = []
        self.selected = []
        self.created = []
        self.__class__.instances.append(self)

    def login(self, user, password):
        return "OK", [b"logged in"]

    def list(self):
        return "OK", [
            b'(\\HasNoChildren) "/" "INBOX"',
            b'(\\HasNoChildren \\Archive) "/" "Archive"',
            b'(\\HasNoChildren \\Drafts) "/" "Drafts"',
        ]

    def create(self, folder):
        self.created.append(str(folder).strip('"'))
        return "OK", [b"created"]

    def status(self, folder, query):
        clean = str(folder).strip('"')
        count = len(self.messages.get(clean, {}))
        return "OK", [f"{clean} (MESSAGES {count} UIDNEXT 100 UIDVALIDITY 777 UNSEEN 0)".encode()]

    def select(self, folder, readonly=False):
        self.current_folder = str(folder).strip('"')
        self.selected.append((self.current_folder, readonly))
        return "OK", [str(len(self.messages.get(self.current_folder, {}))).encode()]

    def uid(self, command, *args):
        if command == "search":
            self.search_calls[self.current_folder] += 1
            if self.current_folder == "INBOX" and self.search_calls[self.current_folder] == 1:
                return "OK", [b"41"]
            uids = " ".join(sorted(self.messages.get(self.current_folder, {}), key=int)).encode()
            return "OK", [uids]
        if command == "fetch":
            uid = args[0].decode() if isinstance(args[0], bytes) else str(args[0])
            raw = self.messages.get(self.current_folder, {}).get(uid)
            if not raw:
                return "NO", []
            prefix = f"1 (UID {uid} FLAGS (\\Seen) RFC822 {{{len(raw)}}}".encode()
            return "OK", [(prefix, raw)]
        if command == "MOVE":
            uid = str(args[0])
            target = str(args[1]).strip('"')
            self.moves.append((self.current_folder, uid, target))
            self.messages.get(self.current_folder, {}).pop(uid, None)
            return "OK", [b"moved"]
        return "NO", []

    def logout(self):
        return "OK", [b"bye"]


class CaptureDownloadStore:
    def __init__(self):
        self.snapshots = []
        self.saved = []
        self.sanitized = {}
        self.external_derivatives = []
        self.verified = set()
        self.moved = []
        self.events = []
        self.batches = []
        self.run_finish = None

    async def ensure_schema(self):
        return None

    async def record_download_run_start(self, **kwargs):
        self.run_start = kwargs

    async def record_download_run_finish(self, **kwargs):
        self.run_finish = kwargs

    async def record_download_event(self, **kwargs):
        self.events.append(kwargs)

    async def record_download_batch_start(self, **kwargs):
        self.batches.append(("start", kwargs))

    async def record_download_batch_finish(self, **kwargs):
        self.batches.append(("finish", kwargs))

    async def save_folder_snapshot(self, *, mailbox_id, folder, status):
        flags = [str(item).lower() for item in folder.get("flags") or []]
        folder_name = pim_email.clean_folder_name(folder["name"])
        snapshot = {
            "snapshot_id": f"snapshot-{folder_name}",
            "folder_uid": pim_email.folder_uid_for(mailbox_id, folder_name),
            "folder_name": folder_name,
            "delimiter": folder.get("delimiter", "/"),
            "flags": flags,
            "special_use_role": pim_email.special_use_role(folder_name, flags),
            "uidvalidity": str(status.get("UIDVALIDITY", "")),
            "uidnext": str(status.get("UIDNEXT", "")),
            "messages_count": int(status.get("MESSAGES", 0)),
        }
        self.snapshots.append(snapshot)
        return snapshot

    async def save_downloaded_email(self, **kwargs):
        self.saved.append(kwargs)

    async def read_local_message(self, email_uid, *, mailbox_id=None, ensure_schema=True):
        match = next(
            item for item in reversed(self.saved) if item["parsed"]["email_uid"] == email_uid
        )
        raw = pim_email.read_encrypted_bytes(match["storage"]["storage_relpath"])
        raw_sha256 = pim_email.hashlib.sha256(raw).hexdigest()
        self.verified.add(match["imap_uid"])
        return {"stored": {"raw_sha256": raw_sha256}}

    async def completed_security_result(self, *, email_uid, raw_sha256):
        match = next(
            item for item in reversed(self.saved) if item["parsed"]["email_uid"] == email_uid
        )
        security = match.get("security")
        if security and security.get("available"):
            return {
                **security,
                "available": True,
                "email_uid": email_uid,
                "raw_sha256": raw_sha256,
                "checked_at": "2026-07-02T00:00:00Z",
                "checker_versions": {"schema": "test"},
            }
        return None

    async def current_sanitized_view_artifact(self, *, mailbox_id, email_uid, raw_sha256):
        return self.sanitized.get((mailbox_id, email_uid, raw_sha256))

    async def store_sanitized_view_artifact(self, *, artifact):
        key = (artifact["mailbox_id"], artifact["email_uid"], artifact["input_raw_sha256"])
        self.sanitized[key] = artifact
        return artifact

    async def process_external_image_derivatives(
        self,
        *,
        mailbox_id,
        email_uid,
        input_raw_sha256,
        source_urls,
        metadata=None,
    ):
        unique = {
            pim_email._canonical_remote_image_url(source) or str(source or "")
            for source in source_urls
        } - {""}
        for source in sorted(unique):
            self.external_derivatives.append(
                {
                    "mailbox_id": mailbox_id,
                    "email_uid": email_uid,
                    "input_raw_sha256": input_raw_sha256,
                    "source_url": source,
                    "status": "pending",
                    "reason": "captured_waiting_for_real_download",
                    "safety_decision": "pending_real_download",
                    "metadata": metadata or {},
                }
            )
        return {
            "stored": 0,
            "blocked": 0,
            "failed": 0,
            "unavailable": 0,
            "pending": len(unique),
            "attempted": 0,
            "already_stored": 0,
            "already_blocked": 0,
            "already_unavailable": 0,
        }

    async def mark_remote_moved(self, **kwargs):
        self.moved.append(kwargs)


def test_safe_downloader_converges_stores_verifies_and_enqueues_image_candidates(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)
    monkeypatch.setattr(
        pim_email,
        "check_email_security_sync",
        lambda raw, **kwargs: {
            **_security_result(),
            "raw_sha256": pim_email.hashlib.sha256(raw).hexdigest(),
        },
    )
    store = CaptureDownloadStore()
    real_move = pim_email._imap_move_uid

    def guarded_move(client, uid, target):
        assert uid in store.verified
        return real_move(client, uid, target)

    monkeypatch.setattr(pim_email, "_imap_move_uid", guarded_move)

    result = pim_email.download_mailbox_sync(
        _mailbox(),
        store=store,
        run_id="test-download-run",
        apply_remote_moves=True,
        convergence_passes=1,
        security_mode="run",
    )

    instance = DownloadFakeIMAP.instances[0]
    assert result["ok"] is True
    assert result["run_id"] == "test-download-run"
    assert store.run_start["run_id"] == "test-download-run"
    assert result["summary"]["stored_messages"] == 3
    assert result["summary"]["moved_messages"] == 0
    assert result["summary"]["move_blocked"] == 1
    assert result["summary"]["sanitized_views_stored"] >= 3
    assert result["summary"]["external_image_derivatives_stored"] == 0
    assert result["summary"]["external_image_derivatives_pending"] >= 3
    assert {item["imap_uid"] for item in store.saved} == {"8", "9", "41"}
    assert {item["folder_snapshot"]["folder_name"] for item in store.saved} == {
        "INBOX",
        "Archive",
        "Drafts",
    }
    assert instance.moves == []
    assert not any(folder == "Archive" for folder, _, _ in instance.moves)
    assert not any(folder == "Drafts" for folder, _, _ in instance.moves)
    assert all(item["storage"]["verified"] for item in store.saved)
    assert all(item["metadata"]["remote_image_sources"] for item in store.saved)
    assert store.external_derivatives
    assert {item["status"] for item in store.external_derivatives} == {"pending"}
    assert {item["safety_decision"] for item in store.external_derivatives} == {
        "pending_real_download"
    }
    assert all((tmp_path / item["storage"]["storage_relpath"]).exists() for item in store.saved)
    assert not any(item["event_type"] == "folder-skip-special-use" for item in store.events)
    assert store.run_finish["status"] == "completed"


def test_downloader_does_not_move_before_persisted_completed_security(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)
    monkeypatch.setattr(
        pim_email,
        "check_email_security_sync",
        lambda raw, **kwargs: {
            **_security_result(),
            "raw_sha256": pim_email.hashlib.sha256(raw).hexdigest(),
        },
    )

    class MissingPersistedSecurityStore(CaptureDownloadStore):
        async def completed_security_result(self, *, email_uid, raw_sha256):
            return None

    store = MissingPersistedSecurityStore()

    result = pim_email.download_mailbox_sync(
        _mailbox(),
        store=store,
        apply_remote_moves=True,
        convergence_passes=1,
        folder_allowlist=["INBOX"],
        limit_per_folder=1,
        max_messages=1,
        security_mode="run",
    )

    assert result["summary"]["stored_messages"] == 1
    assert result["summary"]["security_incomplete"] == 1
    assert result["summary"]["moved_messages"] == 0
    assert result["summary"]["move_blocked"] == 1
    assert DownloadFakeIMAP.instances[0].moves == []
    blocked = [item for item in store.events if item["event_type"] == "remote-move-gate-blocked"]
    assert blocked
    assert blocked[0]["metadata"]["move_gate"]["security_completed"] is False


def test_downloader_does_not_move_when_external_image_processing_failed(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)
    monkeypatch.setattr(
        pim_email,
        "check_email_security_sync",
        lambda raw, **kwargs: {
            **_security_result(),
            "raw_sha256": pim_email.hashlib.sha256(raw).hexdigest(),
        },
    )

    class FailedExternalImageStore(CaptureDownloadStore):
        async def process_external_image_derivatives(self, **kwargs):
            return {
                "stored": 0,
                "blocked": 0,
                "failed": 1,
                "unavailable": 0,
                "pending": 0,
                "attempted": 1,
                "already_stored": 0,
                "already_blocked": 0,
                "already_unavailable": 0,
            }

    store = FailedExternalImageStore()

    result = pim_email.download_mailbox_sync(
        _mailbox(),
        store=store,
        apply_remote_moves=True,
        convergence_passes=1,
        folder_allowlist=["INBOX"],
        limit_per_folder=1,
        max_messages=1,
        security_mode="run",
    )

    assert result["summary"]["stored_messages"] == 1
    assert result["summary"]["external_image_derivatives_failed"] == 1
    assert result["summary"]["moved_messages"] == 0
    assert result["summary"]["move_blocked"] == 1
    assert DownloadFakeIMAP.instances[0].moves == []
    blocked = [item for item in store.events if item["event_type"] == "remote-move-gate-blocked"]
    assert blocked
    assert blocked[0]["metadata"]["move_gate"]["external_image_derivatives_handled"] is False


def test_downloader_moves_when_external_image_is_proven_unavailable(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)
    monkeypatch.setattr(
        pim_email,
        "check_email_security_sync",
        lambda raw, **kwargs: {
            **_security_result(),
            "raw_sha256": pim_email.hashlib.sha256(raw).hexdigest(),
        },
    )

    class UnavailableExternalImageStore(CaptureDownloadStore):
        async def process_external_image_derivatives(self, **kwargs):
            return {
                "stored": 0,
                "blocked": 0,
                "failed": 0,
                "unavailable": 1,
                "pending": 0,
                "attempted": 1,
                "already_stored": 0,
                "already_blocked": 0,
                "already_unavailable": 0,
            }

    store = UnavailableExternalImageStore()

    result = pim_email.download_mailbox_sync(
        _mailbox(),
        store=store,
        apply_remote_moves=True,
        convergence_passes=1,
        folder_allowlist=["INBOX"],
        limit_per_folder=1,
        max_messages=1,
        security_mode="run",
    )

    assert result["summary"]["stored_messages"] == 1
    assert result["summary"]["external_image_derivatives_unavailable"] == 1
    assert result["summary"]["external_image_derivatives_failed"] == 0
    assert result["summary"]["moved_messages"] == 1
    assert result["summary"]["move_blocked"] == 0
    assert DownloadFakeIMAP.instances[0].moves == [("INBOX", "41", "Downloaded")]
    stored = [item for item in store.events if item["event_type"] == "message-stored"]
    assert stored
    assert stored[0]["status"] == "moved"
    assert stored[0]["metadata"]["move_gate"]["external_image_derivatives_handled"] is True


def test_special_use_descendants_do_not_move_but_inbox_subfolders_can_move():
    target = "Downloaded"

    assert not pim_email._folder_move_allowed(
        {"folder_name": "Archive/2023", "special_use_role": ""},
        target,
    )
    assert not pim_email._folder_move_allowed(
        {"folder_name": "Sent/Receipts", "special_use_role": ""},
        target,
    )
    assert pim_email._folder_move_allowed(
        {"folder_name": "INBOX", "special_use_role": "inbox"},
        target,
    )
    assert pim_email._folder_move_allowed(
        {"folder_name": "INBOX/Receipts", "special_use_role": ""},
        target,
    )


def test_imap_uids_newest_first_uses_numeric_uid_descending():
    assert pim_email._imap_uids_newest_first(["2", "10", "1"]) == ["10", "2", "1"]


def test_safe_downloader_resume_idempotence_keeps_duplicate_identity_singleton(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)
    monkeypatch.setattr(
        pim_email, "check_email_security_sync", lambda raw, **kwargs: _security_result()
    )
    store = CaptureDownloadStore()

    result = pim_email.download_mailbox_sync(
        _mailbox(),
        store=store,
        apply_remote_moves=False,
        convergence_passes=2,
        security_mode="run",
    )

    email_uids = [item["parsed"]["email_uid"] for item in store.saved]
    assert result["summary"]["moved_messages"] == 0
    assert len(store.saved) == 7
    assert len(set(email_uids)) == 4
    assert result["summary"]["security_completed"] == 7


def test_downloader_rejects_security_queue_modes(tmp_path, monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CREDENTIAL_KEY", pim_email.generate_credential_key())
    monkeypatch.setenv("BLUEPRINTS_EMAIL_CONTENT_ROOT", str(tmp_path))
    DownloadFakeIMAP.instances = []
    monkeypatch.setattr(pim_email.imaplib, "IMAP4_SSL", DownloadFakeIMAP)

    with pytest.raises(pim_email.EmailConfigError, match="must run the checker"):
        pim_email.download_mailbox_sync(
            _mailbox(),
            store=CaptureDownloadStore(),
            folder_allowlist=["INBOX"],
            limit_per_folder=1,
            max_messages=1,
            security_mode="queue",
        )


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
    assert ("GET", "/personal/email/local/folder-messages") in routes
    assert ("GET", "/personal/email/local/messages/{email_uid}") in routes
    assert ("POST", "/personal/email/local/messages/{email_uid}/security") in routes
    assert ("POST", "/personal/email/download/run") in routes
    assert ("GET", "/personal/email/image-proxy") in routes
    assert ("GET", "/personal/email/messages/{uid}/security") in routes
    assert ("POST", "/personal/email/smtp-self-test") in routes


def test_download_run_request_has_no_security_or_special_use_shortcuts():
    request = routes_pim_email.DownloadMailboxRequest(max_messages=1)

    assert request.max_messages == 1
    assert not hasattr(request, "security_mode")
    assert not hasattr(request, "include_special_use")
    with pytest.raises(ValidationError):
        routes_pim_email.DownloadMailboxRequest(security_mode="queue")
    with pytest.raises(ValidationError):
        routes_pim_email.DownloadMailboxRequest(include_special_use=False)


def test_email_image_proxy_signature_is_required(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_API_SECRET", "test-secret")
    source = "https://images.example.test/banner.png?track=1"

    signature = pim_email.sign_email_image_url(source)

    assert pim_email.verify_email_image_signature(source, signature)
    assert not pim_email.verify_email_image_signature(source, "bad")


def test_external_image_worker_api_auth_and_private_field_scrubbing(monkeypatch):
    monkeypatch.setenv("BLUEPRINTS_PIM_EMAIL_WORKER_SECRET", "worker-secret-123456")
    digest = pim_email._external_image_canonical_digest("https://cdn.example.test/api.png")
    to_thread_calls = []

    async def fake_to_thread(func, /, *args, **kwargs):
        to_thread_calls.append(getattr(func, "__name__", repr(func)))
        return func(*args, **kwargs)

    class FakeStore:
        async def claim_external_image_url_assignment_block(self, **kwargs):
            return {
                "schema": "xarta.pim_email.external_image_url_assignment.block.v1",
                "mailbox_id": "test-mailbox",
                "assignment_batch_id": "assignment-batch-api",
                "assignment_token": "assignment-token-worker-needs",
                "worker_id": kwargs["worker_id"],
                "run_id": kwargs["run_id"],
                "claimed": 1,
                "items": [
                    {
                        "canonical_url_digest": digest,
                        "canonical_url": "https://cdn.example.test/api.png",
                        "source_url": "https://cdn.example.test/api.png?track=1",
                        "email_uid": "20260702-" + "b" * 40,
                        "input_raw_sha256": "raw-api-secret",
                        "assignment_token": "row-token-secret",
                    }
                ],
            }

        async def complete_external_image_url_assignment_with_transformed_payload(self, **kwargs):
            assert kwargs["transformed_content"] == b"jpeg-bytes"
            assert kwargs["raw_image_sha256"] == "a" * 64
            assert kwargs["transformed_sha256"] == "b" * 64
            assert kwargs["width"] == 1
            assert kwargs["height"] == 1
            assert kwargs["transform_version"] == "jpeg-v1"
            return {
                "ok": True,
                "assignment": {
                    "canonical_url_digest": digest,
                    "email_uid": "20260702-" + "b" * 40,
                    "input_raw_sha256": "raw-api-secret",
                    "assignment_token": "row-token-secret",
                },
                "shared_asset": {
                    "shared_asset_uid": "email-shared-asset-api",
                    "canonical_url_digest": digest,
                    "storage_relpath": "assets/plain/path/hidden.enc",
                    "encryption": {"cipher": "secret"},
                },
                "references_linked": 2,
                "references_link_failed": 0,
            }

    monkeypatch.setattr(routes_pim_email, "_store", lambda: FakeStore())
    monkeypatch.setattr(routes_pim_email.asyncio, "to_thread", fake_to_thread)

    with pytest.raises(routes_pim_email.HTTPException) as exc_info:
        asyncio.run(
            routes_pim_email.email_external_image_worker_claim_assignments(
                routes_pim_email.ExternalImageAssignmentClaimRequest(
                    worker_id="tb3-worker-01",
                    run_id="stable-run-1",
                    limit=1000,
                ),
                x_pim_email_worker_token="wrong-token",
            )
        )
    assert exc_info.value.status_code == 401

    claim = asyncio.run(
        routes_pim_email.email_external_image_worker_claim_assignments(
            routes_pim_email.ExternalImageAssignmentClaimRequest(
                worker_id="tb3-worker-01",
                run_id="stable-run-1",
                limit=1000,
            ),
            x_pim_email_worker_token="worker-secret-123456",
        )
    )

    assert claim["assignment_token"] == "assignment-token-worker-needs"
    assert claim["items"][0]["canonical_url_digest"] == digest
    assert "assignment_token" not in claim["items"][0]
    assert "assignment_token" not in claim["items"][0]
    assert "email_uid" not in claim["items"][0]
    assert "input_raw_sha256" not in claim["items"][0]
    assert "raw-api-secret" not in json.dumps(claim)

    complete = asyncio.run(
        routes_pim_email.email_external_image_worker_complete_assignment(
            digest,
            routes_pim_email.ExternalImageAssignmentCompleteRequest(
                worker_id="tb3-worker-01",
                assignment_token="assignment-token-worker-needs",
                transformed_image_base64="anBlZy1ieXRlcw==",
                raw_image_sha256="a" * 64,
                transformed_sha256="b" * 64,
                width=1,
                height=1,
                transform_version="jpeg-v1",
            ),
            x_pim_email_worker_token="worker-secret-123456",
        )
    )

    assert complete["ok"] is True
    assert "email_uid" not in complete["result"]["assignment"]
    assert "input_raw_sha256" not in complete["result"]["assignment"]
    assert "assignment_token" not in complete["result"]["assignment"]
    assert "assignment_token" not in complete["result"]["assignment"]
    assert "storage_relpath" not in complete["result"]["shared_asset"]
    assert "encryption" not in complete["result"]["shared_asset"]
    assert "_decode_transformed_image_base64" in to_thread_calls

    with pytest.raises(routes_pim_email.HTTPException) as bad_base64:
        asyncio.run(
            routes_pim_email.email_external_image_worker_complete_assignment(
                digest,
                routes_pim_email.ExternalImageAssignmentCompleteRequest(
                    worker_id="tb3-worker-01",
                    assignment_token="assignment-token-worker-needs",
                    transformed_image_base64="not-base64!!!",
                    raw_image_sha256="a" * 64,
                    transformed_sha256="b" * 64,
                    width=1,
                    height=1,
                    transform_version="jpeg-v1",
                ),
                x_pim_email_worker_token="worker-secret-123456",
            )
        )
    assert bad_base64.value.status_code == 400

    with pytest.raises(ValidationError):
        routes_pim_email.ExternalImageAssignmentCompleteRequest(
            worker_id="tb3-worker-01",
            assignment_token="assignment-token-worker-needs",
            transformed_image_base64="not-base64",
            raw_image_sha256="not-a-sha",
            transformed_sha256="b" * 64,
            width=1,
            height=1,
        )


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
    assert response["capabilities"]["local_corpus_read"] is True
    assert response["capabilities"]["safe_local_download"] is True
    assert response["capabilities"]["smtp_self_test"] is True
    assert response["capabilities"]["smtp_general_send"] is False
    assert response["capabilities"]["delete"] is False
    assert response["capabilities"]["ai_send"] is False
    assert response["capabilities"]["security_checks"]["message_view_requires_security"] is True


def test_local_corpus_routes_read_stored_message_without_live_imap(monkeypatch):
    email_uid = "20260701-0123456789abcdef0123456789abcdef01234567"

    class FakeLocalStore:
        async def get_mailbox(self, mailbox_id=None):
            return _mailbox()

        async def local_folder_messages(self, **kwargs):
            assert kwargs.get("ensure_schema") is False
            return [
                {
                    "uid": "41",
                    "email_uid": email_uid,
                    "folder": "INBOX",
                    "subject": "Stored",
                    "from": "Sender <sender@example.test>",
                }
            ]

        async def read_local_message(self, requested_uid, *, mailbox_id=None, ensure_schema=True):
            assert requested_uid == email_uid
            assert ensure_schema is False
            return {
                "email_uid": requested_uid,
                "source": "local-corpus",
                "body_blocked": True,
                "views": {},
                "views_available": {
                    "plain": False,
                    "html": False,
                    "markdown": False,
                    "raw": False,
                },
                "security": {
                    "available": False,
                    "security_status": "missing",
                    "blocked_reason": "completed_security_result_missing",
                },
            }

    monkeypatch.setattr(routes_pim_email, "_store", lambda: FakeLocalStore())

    listing = asyncio.run(routes_pim_email.email_local_folder_messages(folder="INBOX"))
    message = asyncio.run(routes_pim_email.email_local_message(email_uid))

    assert listing["source"] == "local-corpus"
    assert listing["messages"][0]["email_uid"] == email_uid
    assert message["message"]["source"] == "local-corpus"
    assert message["message"]["body_blocked"] is True
    assert message["message"]["views"] == {}
    assert message["message"]["security"]["security_status"] == "missing"


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
