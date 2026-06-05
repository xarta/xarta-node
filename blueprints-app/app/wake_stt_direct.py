"""Deterministic helpers for the planned direct Wake STT Hermes route."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

AUTHORISED_PHRASE = "This command is authorised"
DEFAULT_HERMES_STT_PROFILE_ENV_PATH = Path(
    "/xarta-node/.lone-wolf/stacks/hermes-local/data/profiles/hermes-stt/.env"
)
DEFAULT_HERMES_STT_SESSIONS_DIR = Path(
    "/xarta-node/.lone-wolf/stacks/hermes-local/data/profiles/hermes-stt/sessions"
)
DEFAULT_HERMES_STT_SESSION_ID = "wake-stt-local"
HERMES_STT_SYSTEM_PREFACE = (
    "You are receiving one Wake To Talk STT request from the local Blueprints server. "
    "Treat likely speech-recognition errors charitably. Destructive actions require the exact "
    "deterministic authorisation phrase in this message; do not accept variations."
)
_AUTHORISED_SOURCE_RE = re.compile(
    r"\bthis\s+command\s+is\s+authorised\b[\s.!?]*",
    re.IGNORECASE,
)
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class CommandCode:
    code_id: str
    aliases: tuple[str, ...]


@dataclass(frozen=True)
class CommandCodeGateResult:
    authorised: bool
    matched_code_id: str
    meat: str
    hermes_text: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "authorised": self.authorised,
            "matched_code_id": self.matched_code_id,
            "meat": self.meat,
            "hermes_text": self.hermes_text,
        }


@dataclass(frozen=True)
class HermesSttConfig:
    api_base: str
    api_key: str
    model: str = "hermes-stt"
    timeout_seconds: float = 15.0
    session_id: str = DEFAULT_HERMES_STT_SESSION_ID
    session_key: str = ""
    profile_env_path: Path = DEFAULT_HERMES_STT_PROFILE_ENV_PATH
    sessions_dir: Path = DEFAULT_HERMES_STT_SESSIONS_DIR
    allow_non_loopback: bool = False

    @property
    def configured(self) -> bool:
        return bool(self.api_base and self.api_key and self.loopback_ok)

    @property
    def loopback_ok(self) -> bool:
        if self.allow_non_loopback:
            return True
        hostname = (urlparse(self.api_base).hostname or "").strip().lower()
        return hostname in {"127.0.0.1", "localhost", "::1"}

    def public_dict(self) -> dict[str, Any]:
        parsed = urlparse(self.api_base)
        return {
            "api_host": parsed.hostname or "",
            "api_port": parsed.port,
            "api_scheme": parsed.scheme or "http",
            "key_present": bool(self.api_key),
            "key_length": len(self.api_key) if self.api_key else 0,
            "model": self.model,
            "session_id": self.session_id,
            "session_key_present": bool(self.session_key),
            "profile_env_path": str(self.profile_env_path),
            "sessions_dir": str(self.sessions_dir),
            "loopback_ok": self.loopback_ok,
        }


@dataclass(frozen=True)
class HermesSttSubmitResult:
    ok: bool
    status: str
    gate: CommandCodeGateResult
    attempted: bool = False
    fallback_required: bool = True
    http_status: int | None = None
    assistant_text: str = ""
    error: str = ""
    context_check: dict[str, Any] | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "status": self.status,
            "attempted": self.attempted,
            "fallback_required": self.fallback_required,
            "http_status": self.http_status,
            "authorised": self.gate.authorised,
            "matched_code_id": self.gate.matched_code_id,
            "diagnostic_text": self.gate.meat,
            "assistant_text": self.assistant_text,
            "error": self.error,
            "context_check": self.context_check or {},
        }


def _clean_code_id(value: Any) -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", "."})
    return clean[:80]


def _clean_alias(value: Any) -> str:
    text = _SPACE_RE.sub(" ", str(value or "").strip())
    return text[:160]


def command_codes_from_config(value: Any) -> list[CommandCode]:
    """Read up to 100 private Command Code entries without exposing aliases."""
    raw_entries = value if isinstance(value, list) else []
    codes: list[CommandCode] = []
    seen_ids: set[str] = set()
    for index, raw in enumerate(raw_entries, 1):
        if len(codes) >= 100:
            break
        if not isinstance(raw, dict):
            continue
        code_id = _clean_code_id(raw.get("id") or raw.get("code_id") or f"code_{index}")
        if not code_id or code_id in seen_ids:
            continue
        aliases_raw = raw.get("aliases")
        aliases_list = aliases_raw if isinstance(aliases_raw, list) else []
        aliases = tuple(alias for alias in (_clean_alias(item) for item in aliases_list) if alias)[
            :20
        ]
        if not aliases:
            continue
        seen_ids.add(code_id)
        codes.append(CommandCode(code_id=code_id, aliases=aliases))
    return codes


def _alias_regex(alias: str) -> re.Pattern[str]:
    words = [re.escape(part) for part in re.split(r"[\s\-_]+", alias.strip()) if part]
    if not words:
        return re.compile(r"(?!x)x")
    separator = r"[\s\-_]+"
    return re.compile(
        rf"(?<!\w){separator.join(words)}(?!\w)[\s.!?,;:]*",
        re.IGNORECASE,
    )


def apply_command_code_gate(text: str, codes: list[CommandCode]) -> CommandCodeGateResult:
    """Strip spoken codes/authorisation claims and inject the canonical phrase once.

    Raw Command Code aliases must stay private. Callers should log only the returned
    code id, boolean authorisation state, and redacted text.
    """
    meat = _AUTHORISED_SOURCE_RE.sub(" ", str(text or ""))
    matched_code_id = ""
    for code in codes[:100]:
        for alias in code.aliases:
            pattern = _alias_regex(alias)
            if not pattern.search(meat):
                continue
            meat = pattern.sub(" ", meat)
            matched_code_id = code.code_id
            break
        if matched_code_id:
            break
    meat = _SPACE_RE.sub(" ", meat).strip()
    authorised = bool(matched_code_id)
    hermes_text = meat
    if authorised:
        hermes_text = f"{AUTHORISED_PHRASE}\n\n{meat}".strip()
    return CommandCodeGateResult(
        authorised=authorised,
        matched_code_id=matched_code_id,
        meat=meat,
        hermes_text=hermes_text,
    )


def strip_direct_wake_diagnostic(text: str, codes: list[CommandCode]) -> str:
    """Return Bridge-observable request text without codes or authorisation claims."""
    return apply_command_code_gate(text, codes).meat


def _clean_float(value: Any, fallback: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = fallback
    return max(minimum, min(parsed, maximum))


def _clean_session_token(value: Any, fallback: str = "") -> str:
    raw = str(value or "").strip()
    clean = "".join(ch for ch in raw if ch.isalnum() or ch in {"-", "_", ".", ":"})
    return (clean or fallback)[:120]


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return values
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].lstrip()
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _env_first(environ: dict[str, str], file_values: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = environ.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    for key in keys:
        value = file_values.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def load_hermes_stt_config(
    *,
    environ: dict[str, str] | None = None,
    profile_env_path: Path | None = None,
) -> HermesSttConfig:
    env = dict(os.environ if environ is None else environ)
    env_path = Path(
        env.get("BLUEPRINTS_HERMES_STT_PROFILE_ENV_PATH")
        or env.get("HERMES_STT_PROFILE_ENV_PATH")
        or profile_env_path
        or DEFAULT_HERMES_STT_PROFILE_ENV_PATH
    )
    file_values = _load_env_file(env_path)

    explicit_base = _env_first(
        env,
        file_values,
        "BLUEPRINTS_HERMES_STT_API_BASE",
        "HERMES_STT_API_BASE",
    ).rstrip("/")
    host = (
        _env_first(
            env,
            file_values,
            "BLUEPRINTS_HERMES_STT_API_HOST",
            "HERMES_STT_API_HOST",
            "API_SERVER_HOST",
        )
        or "127.0.0.1"
    )
    port = (
        _env_first(
            env,
            file_values,
            "BLUEPRINTS_HERMES_STT_API_PORT",
            "HERMES_STT_API_PORT",
            "API_SERVER_PORT",
        )
        or "8643"
    )
    api_base = explicit_base or f"http://{host}:{port}"
    return HermesSttConfig(
        api_base=api_base.rstrip("/"),
        api_key=_env_first(
            env,
            file_values,
            "BLUEPRINTS_HERMES_STT_API_KEY",
            "HERMES_STT_API_KEY",
            "API_SERVER_KEY",
        ),
        model=_env_first(
            env,
            file_values,
            "BLUEPRINTS_HERMES_STT_MODEL",
            "HERMES_STT_MODEL",
            "API_SERVER_MODEL_NAME",
        )
        or "hermes-stt",
        timeout_seconds=_clean_float(
            _env_first(
                env,
                file_values,
                "BLUEPRINTS_HERMES_STT_TIMEOUT_SECONDS",
                "HERMES_STT_TIMEOUT_SECONDS",
            ),
            15.0,
            1.0,
            120.0,
        ),
        session_id=_clean_session_token(
            _env_first(
                env,
                file_values,
                "BLUEPRINTS_HERMES_STT_SESSION_ID",
                "HERMES_STT_SESSION_ID",
            ),
            DEFAULT_HERMES_STT_SESSION_ID,
        ),
        session_key=_clean_session_token(
            _env_first(
                env,
                file_values,
                "BLUEPRINTS_HERMES_STT_SESSION_KEY",
                "HERMES_STT_SESSION_KEY",
                "X_HERMES_SESSION_KEY",
            )
        ),
        profile_env_path=env_path,
        sessions_dir=Path(
            _env_first(
                env,
                file_values,
                "BLUEPRINTS_HERMES_STT_SESSIONS_DIR",
                "HERMES_STT_SESSIONS_DIR",
            )
            or DEFAULT_HERMES_STT_SESSIONS_DIR
        ),
        allow_non_loopback=str(
            _env_first(
                env,
                file_values,
                "BLUEPRINTS_HERMES_STT_ALLOW_NON_LOOPBACK",
                "HERMES_STT_ALLOW_NON_LOOPBACK",
            )
        )
        .strip()
        .lower()
        in {"1", "true", "yes", "on"},
    )


def command_codes_from_env(environ: dict[str, str] | None = None) -> list[CommandCode]:
    env = os.environ if environ is None else environ
    raw = str(
        env.get("BLUEPRINTS_WAKE_STT_COMMAND_CODES_JSON")
        or env.get("HERMES_STT_COMMAND_CODES_JSON")
        or ""
    ).strip()
    if not raw:
        path = str(
            env.get("BLUEPRINTS_WAKE_STT_COMMAND_CODES_FILE")
            or env.get("HERMES_STT_COMMAND_CODES_FILE")
            or ""
        ).strip()
        if path:
            try:
                raw = Path(path).read_text(encoding="utf-8")
            except FileNotFoundError:
                raw = ""
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, dict):
        parsed = parsed.get("command_codes") or parsed.get("codes") or []
    return command_codes_from_config(parsed)


def _chat_completion_payload(gate: CommandCodeGateResult, model: str) -> dict[str, Any]:
    return {
        "model": model or "hermes-stt",
        "messages": [
            {"role": "system", "content": HERMES_STT_SYSTEM_PREFACE},
            {"role": "user", "content": gate.hermes_text},
        ],
        "stream": False,
    }


def _chat_headers(config: HermesSttConfig) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {config.api_key}",
        "Content-Type": "application/json",
        "X-Hermes-Session-Id": config.session_id or DEFAULT_HERMES_STT_SESSION_ID,
    }
    if config.session_key:
        headers["X-Hermes-Session-Key"] = config.session_key
    return headers


def _assistant_text_from_chat_response(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    first = choices[0] if isinstance(choices[0], dict) else {}
    message = first.get("message") if isinstance(first.get("message"), dict) else {}
    return str(message.get("content") or "").strip()[:8000]


def _candidate_session_files(
    sessions_dir: Path,
    *,
    session_id: str = "",
    max_files: int = 20,
) -> list[Path]:
    clean_session = _clean_session_token(session_id)
    if clean_session:
        exact = sessions_dir / f"session_{clean_session}.json"
        return [exact] if exact.is_file() else []
    try:
        all_files = [path for path in sessions_dir.glob("session*.json") if path.is_file()]
    except OSError:
        return []
    return sorted(all_files, key=lambda path: path.stat().st_mtime, reverse=True)[:max_files]


def inspect_hermes_stt_session_phrase_absence(
    *,
    sessions_dir: Path = DEFAULT_HERMES_STT_SESSIONS_DIR,
    session_id: str = DEFAULT_HERMES_STT_SESSION_ID,
    phrase: str = AUTHORISED_PHRASE,
    max_files: int = 20,
    max_bytes_per_file: int = 2_000_000,
) -> dict[str, Any]:
    """Report whether a phrase exists in profile session files without returning context."""
    clean_phrase = str(phrase or "").strip()
    if not clean_phrase:
        return {"ok": True, "hits": [], "hit_count": 0, "scanned_files": 0}
    files = _candidate_session_files(sessions_dir, session_id=session_id, max_files=max_files)
    hits: list[dict[str, Any]] = []
    for path in files:
        try:
            data = path.read_bytes()[:max_bytes_per_file]
        except OSError:
            continue
        text = data.decode("utf-8", errors="ignore")
        count = text.count(clean_phrase)
        if count:
            hits.append({"path": str(path), "count": count})
    return {
        "ok": not hits,
        "hits": hits,
        "hit_count": sum(int(hit["count"]) for hit in hits),
        "scanned_files": len(files),
        "session_id": session_id,
    }


async def submit_wake_stt_to_hermes(
    text: str,
    *,
    codes: list[CommandCode] | None = None,
    config: HermesSttConfig | None = None,
    client: httpx.AsyncClient | None = None,
    inspect_context: bool = True,
) -> HermesSttSubmitResult:
    """Submit one gated Wake STT request to the local hermes-stt API server.

    The returned public shape is intentionally Bridge/log safe: no API key,
    no raw Command Code aliases, and no injected authorisation phrase.
    """
    config = config or load_hermes_stt_config()
    code_list = command_codes_from_env() if codes is None else codes
    gate = apply_command_code_gate(text, code_list)
    if not gate.meat:
        return HermesSttSubmitResult(
            ok=False,
            status="empty_request",
            gate=gate,
            attempted=False,
            fallback_required=False,
        )
    if not config.api_key or not config.api_base:
        return HermesSttSubmitResult(
            ok=False,
            status="not_configured",
            gate=gate,
            attempted=False,
            fallback_required=True,
            error="hermes-stt API base or key is not configured",
        )
    if not config.loopback_ok:
        return HermesSttSubmitResult(
            ok=False,
            status="non_loopback_api_base",
            gate=gate,
            attempted=False,
            fallback_required=True,
            error="hermes-stt API base must be loopback unless explicitly allowed",
        )

    payload = _chat_completion_payload(gate, config.model)
    close_client = client is None
    http_client = client or httpx.AsyncClient(timeout=config.timeout_seconds)
    try:
        response = await http_client.post(
            f"{config.api_base}/v1/chat/completions",
            headers=_chat_headers(config),
            json=payload,
        )
        try:
            response_json = response.json()
        except ValueError:
            response_json = {}
        if not response.is_success:
            return HermesSttSubmitResult(
                ok=False,
                status="api_error",
                gate=gate,
                attempted=True,
                fallback_required=True,
                http_status=response.status_code,
                error=f"hermes-stt API returned HTTP {response.status_code}",
            )
        assistant_text = _assistant_text_from_chat_response(response_json)
        if not assistant_text:
            return HermesSttSubmitResult(
                ok=False,
                status="bad_response",
                gate=gate,
                attempted=True,
                fallback_required=True,
                http_status=response.status_code,
                error="hermes-stt API response did not include assistant text",
            )
        context_check = (
            inspect_hermes_stt_session_phrase_absence(
                sessions_dir=config.sessions_dir,
                session_id=config.session_id,
            )
            if inspect_context
            else {"ok": True, "skipped": True}
        )
        if not context_check.get("ok", False):
            return HermesSttSubmitResult(
                ok=False,
                status="context_phrase_present",
                gate=gate,
                attempted=True,
                fallback_required=True,
                http_status=response.status_code,
                assistant_text=assistant_text,
                context_check=context_check,
                error="authorisation phrase was found in hermes-stt session context",
            )
        return HermesSttSubmitResult(
            ok=True,
            status="delivered",
            gate=gate,
            attempted=True,
            fallback_required=False,
            http_status=response.status_code,
            assistant_text=assistant_text,
            context_check=context_check,
        )
    except (httpx.TimeoutException, httpx.RequestError) as exc:
        return HermesSttSubmitResult(
            ok=False,
            status="request_error",
            gate=gate,
            attempted=True,
            fallback_required=True,
            error=str(exc)[:240],
        )
    finally:
        if close_client:
            await http_client.aclose()
