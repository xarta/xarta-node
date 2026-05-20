"""HTTP client for the node-local PocketTTS text sanitizer service."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import httpx

from .db import get_conn, get_setting

DEFAULT_TTS_SANITIZER_URL = "http://127.0.0.1:18884/v1/text/sanitize"


@dataclass(frozen=True)
class TtsSanitizeResult:
    text: str
    transforms: tuple[str, ...]


class TtsSanitizerUnavailable(RuntimeError):
    pass


def resolve_tts_sanitizer_url(settings: Mapping[str, str] | None = None) -> str:
    configured = ""
    if settings is not None:
        configured = str(settings.get("tts.sanitizer_url") or "").strip()
    else:
        with get_conn() as conn:
            configured = str(get_setting(conn, "tts.sanitizer_url") or "").strip()
    return configured or DEFAULT_TTS_SANITIZER_URL


def _resolve_transform_url(settings: Mapping[str, str] | None = None) -> str:
    url = resolve_tts_sanitizer_url(settings)
    if url.endswith("/v1/text/sanitize"):
        return url[: -len("/sanitize")] + "/transform"
    if url.endswith("/v1/tts/sanitize"):
        return url[: -len("/v1/tts/sanitize")] + "/v1/text/transform"
    return url.rstrip("/").rsplit("/", 1)[0] + "/transform"


async def _post_text_service(
    url: str,
    payload: dict[str, object],
    *,
    timeout_ms: int,
) -> tuple[str, tuple[str, ...]]:
    try:
        async with httpx.AsyncClient(timeout=timeout_ms / 1000.0) as client:
            resp = await client.post(url, json=payload)
    except Exception as exc:
        raise TtsSanitizerUnavailable("Local TTS sanitizer is unavailable") from exc
    if resp.status_code >= 400:
        raise TtsSanitizerUnavailable(f"Local TTS sanitizer returned HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise TtsSanitizerUnavailable("Local TTS sanitizer returned invalid JSON") from exc
    if not data.get("ok", False) or not isinstance(data.get("text"), str):
        raise TtsSanitizerUnavailable("Local TTS sanitizer returned an invalid response")
    transforms = data.get("transforms")
    if not isinstance(transforms, list):
        transforms = []
    return str(data["text"]), tuple(str(item) for item in transforms)


async def sanitize_tts_text_via_service(
    text: str,
    *,
    settings: Mapping[str, str] | None = None,
    timeout_ms: int,
    transform_profile: str = "speech",
    allow_llm_sanitizer: bool = False,
) -> TtsSanitizeResult:
    result_text, transforms = await _post_text_service(
        resolve_tts_sanitizer_url(settings),
        {
            "text": text,
            "sanitize_text": transform_profile != "none",
            "transform_profile": transform_profile or "speech",
            "allow_llm_sanitizer": allow_llm_sanitizer,
        },
        timeout_ms=timeout_ms,
    )
    return TtsSanitizeResult(text=result_text, transforms=transforms)


async def transform_tts_text_via_service(
    text: str,
    *,
    operation: str,
    settings: Mapping[str, str] | None = None,
    timeout_ms: int = 12000,
    **options: object,
) -> TtsSanitizeResult:
    payload: dict[str, object] = {
        "text": text,
        "operation": operation,
    }
    payload.update(options)
    result_text, transforms = await _post_text_service(
        _resolve_transform_url(settings),
        payload,
        timeout_ms=timeout_ms,
    )
    return TtsSanitizeResult(text=result_text, transforms=transforms)


async def prepare_tts_markdown_for_llm_via_service(
    markdown: str,
    *,
    strip_top_backlink: bool = False,
    timeout_ms: int = 12000,
) -> str:
    result = await transform_tts_text_via_service(
        markdown,
        operation="prepare_tts_markdown_for_llm",
        timeout_ms=timeout_ms,
        strip_top_backlink_line=strip_top_backlink,
    )
    return result.text


async def clean_tts_markdown_via_service(text: str, *, timeout_ms: int = 12000) -> TtsSanitizeResult:
    return await transform_tts_text_via_service(
        text,
        operation="clean_tts_markdown",
        timeout_ms=timeout_ms,
    )


async def prepare_and_sanitize_tts_markdown_via_service(
    markdown: str,
    *,
    timeout_ms: int = 12000,
) -> TtsSanitizeResult:
    return await transform_tts_text_via_service(
        markdown,
        operation="prepare_and_sanitize_tts_markdown",
        timeout_ms=timeout_ms,
    )
