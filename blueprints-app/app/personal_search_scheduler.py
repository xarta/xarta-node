"""Typed xarta-scheduler provider and UI status API for Personal Search."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import socket
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from . import routes_personal, timing

log = logging.getLogger(__name__)

PROVIDER_ID = "blueprints-personal-search"
TARGET_KEY = "blueprints_personal_search_sync_v1"
STATUS_SCHEMA = "xarta.personal.search.scheduler-status.v1"
RESULT_SCHEMA = "xarta.personal.search.scheduler-result.v1"
DEFAULT_SCHEDULER_URL = "http://127.0.0.1:18111"
TOKEN_ENV = "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_PERSONAL_SEARCH_TOKEN"
TOKEN_FILE_ENV = "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_PERSONAL_SEARCH_TOKEN_FILE"
PERSONAL_SOURCE_TABLES = (
    "personal_events",
    "personal_time_tasks",
    "personal_import_batches",
)
KANBAN_SOURCE_TABLES = (
    "kanban_items",
    "kanban_blockers",
    "kanban_discussions",
)
SOURCE_TABLES = (*PERSONAL_SOURCE_TABLES, *KANBAN_SOURCE_TABLES)
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 30 * 1024
REQUEST_TIMEOUT_SECONDS = 3.0
FULL_SYNC_HEADROOM_SECONDS = 17
DEFAULT_HEARTBEAT_STALE_SECONDS = 15
DEFAULT_SUCCESS_STALE_SECONDS = 60

router = APIRouter(prefix="/personal/search/scheduler", tags=["personal-search-scheduler"])


class EmptyRunNowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SchedulerProtocolError(RuntimeError):
    def __init__(self, classification: str, *, status_code: int = 0) -> None:
        super().__init__(classification)
        self.classification = classification
        self.status_code = status_code


class ClaimLost(RuntimeError):
    pass


class TargetRejected(RuntimeError):
    pass


@dataclass(frozen=True)
class ProviderConfig:
    base_url: str
    token: str
    heartbeat_interval: float
    claim_interval: float


def _bounded_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(value, maximum))


def _scheduler_base_url() -> str:
    value = os.environ.get("XARTA_SCHEDULER_URL", DEFAULT_SCHEDULER_URL).strip().rstrip("/")
    parsed = urlparse(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("XARTA_SCHEDULER_URL is not a bounded HTTP origin")
    return value


def _valid_token(value: str) -> bool:
    return bool(40 <= len(value) <= 200 and re.fullmatch(r"[A-Za-z0-9_-]+", value))


def _provider_token() -> str:
    direct = os.environ.get(TOKEN_ENV, "").strip()
    token_file = os.environ.get(TOKEN_FILE_ENV, "").strip()
    if direct and token_file:
        raise ValueError("configure either the provider token or token file, not both")
    if direct:
        if not _valid_token(direct):
            raise ValueError("provider token is malformed")
        return direct
    if not token_file:
        return ""
    path = Path(token_file)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ValueError("provider token file must be a mode-0600 regular file")
    token = path.read_text(encoding="ascii").strip()
    if not _valid_token(token):
        raise ValueError("provider token file is malformed")
    return token


def load_provider_config() -> ProviderConfig:
    return ProviderConfig(
        base_url=_scheduler_base_url(),
        token=_provider_token(),
        heartbeat_interval=_bounded_float(
            "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_PERSONAL_SEARCH_HEARTBEAT_SECONDS",
            2.0,
            1.0,
            30.0,
        ),
        claim_interval=_bounded_float(
            "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_PERSONAL_SEARCH_CLAIM_SECONDS",
            1.0,
            0.25,
            10.0,
        ),
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _source_signature_sync() -> dict[str, Any]:
    tables: list[dict[str, Any]] = []
    postgres_active = routes_personal._kanban_active_store_is_postgres()

    def append_table_signatures(conn: Any, source_tables: tuple[str, ...]) -> None:
        for table in source_tables:
            row = conn.execute(
                f"SELECT COUNT(*) AS row_count, COALESCE(MAX(updated_at), '') AS max_updated_at "
                f"FROM {table}"
            ).fetchone()
            tables.append(
                {
                    "table": table,
                    "row_count": int(row["row_count"] or 0),
                    "max_updated_at": str(row["max_updated_at"] or ""),
                }
            )

    with routes_personal._sqlite_get_read_conn(
        busy_timeout_ms=100,
        operation="personal_search_source_signature",
    ) as sqlite_conn:
        append_table_signatures(sqlite_conn, PERSONAL_SOURCE_TABLES)
        if not postgres_active:
            append_table_signatures(sqlite_conn, KANBAN_SOURCE_TABLES)
    if postgres_active:
        with routes_personal._kanban_postgres_get_conn(
            operation="personal_search_source_signature",
            transactional=False,
        ) as kanban_conn:
            append_table_signatures(kanban_conn, KANBAN_SOURCE_TABLES)
    encoded = json.dumps(tables, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {
        "source_signature": f"sha256:{hashlib.sha256(encoded).hexdigest()}",
        "source_rows": sum(item["row_count"] for item in tables),
        "tables": tables,
    }


def _full_document_sync_sync() -> dict[str, Any]:
    generated_at = _utc_now()
    documents = routes_personal._sync_personal_search_documents_isolated(generated_at)
    return {"generated_at": generated_at, "documents": documents}


def _bounded_result(result: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str).encode()
    if len(encoded) > MAX_RESULT_BYTES:
        raise RuntimeError("Personal Search scheduler result exceeded its bound")
    return result


def _parse_iso(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _safe_failure(exc: BaseException) -> str:
    if isinstance(exc, TargetRejected):
        return f"TargetRejected: {str(exc)[:240]}"
    return f"{type(exc).__name__}: Personal Search target failed"


class PersonalSearchScheduler:
    def __init__(
        self,
        *,
        config: ProviderConfig | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.client = client
        self._owns_client = client is None
        self.worker_id = f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:12]}"
        self._loop_tasks: list[asyncio.Task[Any]] = []
        self._active_runs: dict[str, asyncio.Task[Any]] = {}
        self._claim_attempt_lock: asyncio.Lock | None = None
        self._claims_paused_for_restart = False
        self._closing = False
        self._last_error = ""
        self.config_error = ""

    async def start(self) -> bool:
        if self._loop_tasks:
            return True
        try:
            self.config = self.config or load_provider_config()
        except (OSError, UnicodeError, ValueError) as exc:
            self.config_error = type(exc).__name__
            log.warning("Personal Search scheduler provider disabled: invalid configuration")
            return False
        if self.client is None:
            self.client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=1.0),
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
                trust_env=False,
            )
        if not self.config.token:
            log.info("Personal Search scheduler provider disabled: token is not configured")
            return False
        self._closing = False
        self._claims_paused_for_restart = False
        self._loop_tasks = [
            asyncio.create_task(self._heartbeat_loop(), name="personal-search-scheduler-heartbeat"),
            asyncio.create_task(self._claim_loop(), name="personal-search-scheduler-claims"),
        ]
        return True

    async def stop(self) -> None:
        self._closing = True
        tasks = [*self._loop_tasks, *self._active_runs.values()]
        self._loop_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active_runs.clear()
        if self._owns_client and self.client is not None:
            with suppress(Exception):
                await self.client.aclose()
            self.client = None

    async def pause_new_claims_for_restart(self) -> dict[str, Any]:
        """Fence new claims without cancelling an in-flight or retained claim.

        Existing target tasks and their claim maintainers are deliberately left
        running.  The restart owner must refuse while ``active_run_ids`` is
        non-empty; it must never cancel retained scheduler work to make a
        deployment appear quiescent.
        """
        provider_effective_enabled = any(not task.done() for task in self._loop_tasks)
        self._claims_paused_for_restart = True
        async with self._get_claim_attempt_lock():
            # Acquiring the barrier proves any /claims/next response has already
            # been fully materialized into _active_runs.
            pass
        await asyncio.sleep(0)
        return {
            "provider_id": PROVIDER_ID,
            "provider_effective_enabled": provider_effective_enabled,
            "claim_loop_paused": True,
            "active_run_ids": sorted(
                run_id for run_id, task in self._active_runs.items() if not task.done()
            ),
        }

    async def resume_new_claims_after_restart_abort(self) -> bool:
        """Resume a previously paused claim loop when the restart is refused."""
        if self._closing:
            return False
        self._claims_paused_for_restart = False
        return True

    def _get_claim_attempt_lock(self) -> asyncio.Lock:
        if self._claim_attempt_lock is None:
            self._claim_attempt_lock = asyncio.Lock()
        return self._claim_attempt_lock

    def _auth_headers(self) -> dict[str, str]:
        if not self.config or not self.config.token:
            raise SchedulerProtocolError("provider authentication is not configured")
        return {"Authorization": f"Bearer {self.config.token}"}

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        body: dict[str, Any] | None = None,
        authenticated: bool = False,
    ) -> dict[str, Any]:
        if self.client is None:
            config = self.config or load_provider_config()
            self.config = config
            self.client = httpx.AsyncClient(
                base_url=config.base_url,
                timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=1.0),
                trust_env=False,
            )
        response = await self.client.request(
            method,
            path,
            json=body,
            headers=self._auth_headers() if authenticated else None,
        )
        if response.status_code >= 400:
            raise SchedulerProtocolError(
                f"scheduler_http_{response.status_code}", status_code=response.status_code
            )
        if len(response.content) > MAX_RESPONSE_BYTES:
            raise SchedulerProtocolError("scheduler_response_too_large")
        try:
            payload = response.json()
        except ValueError as exc:
            raise SchedulerProtocolError("scheduler_invalid_json") from exc
        if not isinstance(payload, dict):
            raise SchedulerProtocolError("scheduler_non_object_response")
        return payload

    async def _heartbeat_loop(self) -> None:
        assert self.config is not None
        while not self._closing:
            run_ids = list(self._active_runs)[:32]
            state = "running" if run_ids else ("degraded" if self._last_error else "idle")
            try:
                await self._request_json(
                    "POST",
                    f"/providers/{PROVIDER_ID}/heartbeat",
                    body={
                        "worker_id": self.worker_id,
                        "state": state,
                        "current_run_ids": run_ids,
                        "metadata": {"protocol": "blueprints-personal-search-provider-v1"},
                        "last_error": self._last_error,
                    },
                    authenticated=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = f"heartbeat:{type(exc).__name__}"[:240]
            await asyncio.sleep(self.config.heartbeat_interval)

    async def _claim_loop(self) -> None:
        assert self.config is not None
        backoff = self.config.claim_interval
        while not self._closing:
            try:
                if self._active_runs or self._claims_paused_for_restart:
                    await asyncio.sleep(self.config.claim_interval)
                    continue
                async with self._get_claim_attempt_lock():
                    if not self._claims_paused_for_restart:
                        response = await self._request_json(
                            "POST",
                            f"/providers/{PROVIDER_ID}/claims/next",
                            body={
                                "worker_id": self.worker_id,
                                "request_id": f"blueprints-search-claim-{uuid.uuid4().hex}",
                            },
                            authenticated=True,
                        )
                        claim = response.get("claim")
                        if isinstance(claim, dict) and claim.get("actionable"):
                            run = claim.get("run") if isinstance(claim.get("run"), dict) else {}
                            run_id = str(run.get("run_id") or "")
                            if (
                                not run_id
                                or not str(claim.get("claim_token") or "")
                                or not str(claim.get("fence_token") or "")
                                or _parse_iso(claim.get("lease_until")) is None
                            ):
                                raise SchedulerProtocolError(
                                    "actionable claim omitted fencing data"
                                )
                            task = asyncio.create_task(
                                self._run_claim(claim), name=f"search-sync-{run_id}"
                            )
                            self._active_runs[run_id] = task
                            task.add_done_callback(
                                lambda done, rid=run_id: self._claim_done(rid, done)
                            )
                        else:
                            self._last_error = ""
                backoff = self.config.claim_interval
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = f"claim:{type(exc).__name__}"[:240]
                backoff = min(max(self.config.claim_interval, backoff * 2), 10.0)
            await asyncio.sleep(backoff)

    def _claim_done(self, run_id: str, task: asyncio.Task[Any]) -> None:
        self._active_runs.pop(run_id, None)
        with suppress(asyncio.CancelledError, Exception):
            task.result()

    async def _claim_maintainer(
        self,
        claim: dict[str, Any],
        stop: asyncio.Event,
        lost: asyncio.Event,
    ) -> None:
        run = claim["run"]
        lease_until = _parse_iso(claim.get("lease_until"))
        policies = claim.get("policies") if isinstance(claim.get("policies"), dict) else {}
        try:
            lease_seconds = float(policies.get("lease_seconds") or 60.0)
        except (TypeError, ValueError):
            lease_seconds = 60.0
        lease_seconds = max(5.0, min(lease_seconds, 3600.0))
        interval = max(1.0, min(lease_seconds / 3.0, 10.0))
        while not stop.is_set():
            if lease_until and (lease_until - datetime.now(UTC)).total_seconds() <= 1.0:
                lost.set()
                return
            try:
                response = await self._request_json(
                    "POST",
                    f"/providers/{PROVIDER_ID}/claims/{run['run_id']}/heartbeat",
                    body={
                        "worker_id": self.worker_id,
                        "claim_token": claim["claim_token"],
                        "fence_token": claim["fence_token"],
                    },
                    authenticated=True,
                )
                if response.get("accepted") is False:
                    lost.set()
                    return
                refreshed = _parse_iso(response.get("lease_until"))
                if refreshed:
                    lease_until = refreshed
            except asyncio.CancelledError:
                raise
            except SchedulerProtocolError as exc:
                if exc.status_code == 409:
                    lost.set()
                    return
                if lease_until and (lease_until - datetime.now(UTC)).total_seconds() <= 1.0:
                    lost.set()
                    return
            except Exception:
                if lease_until and (lease_until - datetime.now(UTC)).total_seconds() <= 1.0:
                    lost.set()
                    return
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def _previous_success(self) -> dict[str, Any] | None:
        provider = await self._request_json("GET", f"/providers/{PROVIDER_ID}/status")
        success = provider.get("latest_success")
        if isinstance(success, dict):
            return success
        history = await self._request_json("GET", f"/history?provider_id={PROVIDER_ID}&limit=25")
        for run in history.get("runs", []):
            if isinstance(run, dict) and run.get("status") in {
                "completed",
                "completed_with_errors",
            }:
                return run
        return None

    async def _execute_target(self, claim: dict[str, Any]) -> dict[str, Any]:
        run = claim.get("run") if isinstance(claim.get("run"), dict) else {}
        if run.get("target_key") != TARGET_KEY:
            raise TargetRejected("claim target is outside the Personal Search allowlist")
        target_config = claim.get("target_config")
        target_config = target_config if isinstance(target_config, dict) else {}
        embedding_request = target_config.get("include_embeddings", False)
        if set(target_config) - {"include_embeddings"} or embedding_request is not False:
            raise TargetRejected("Personal Search scheduled sync does not permit embeddings")
        prior = await self._previous_success()
        signature = await timing.to_thread(
            "personal.search_scheduler.source_signature", _source_signature_sync
        )
        prior_result = prior.get("result") if isinstance(prior, dict) else None
        prior_result = prior_result if isinstance(prior_result, dict) else {}
        prior_index_at = str(prior_result.get("index_updated_at") or "")
        if (
            prior_result.get("source_signature") == signature["source_signature"]
            and _parse_iso(prior_index_at) is not None
        ):
            prior_documents = prior_result.get("documents")
            prior_documents = prior_documents if isinstance(prior_documents, dict) else {}
            document_count = int(
                prior_documents.get("document_count") or signature["source_rows"] or 0
            )
            documents = {
                "document_count": document_count,
                "source_rows": signature["source_rows"],
                "updated": 0,
                "deleted": 0,
                "unchanged": document_count,
            }
            return _bounded_result(
                {
                    "schema": RESULT_SCHEMA,
                    "skipped": True,
                    "reason": "source_signature_unchanged",
                    "source_signature": signature["source_signature"],
                    "index_updated_at": prior_index_at,
                    "documents": documents,
                }
            )
        synced = await timing.to_thread(
            "personal.search_scheduler.document_sync", _full_document_sync_sync
        )
        return _bounded_result(
            {
                "schema": RESULT_SCHEMA,
                "skipped": False,
                # Retain the signature measured before synchronization. If a
                # source changes during or after the index transaction, the
                # next occurrence must see a mismatch and sync again; never
                # certify a change that may not have reached the index.
                "source_signature": signature["source_signature"],
                "index_updated_at": synced["generated_at"],
                "documents": synced["documents"],
            }
        )

    async def _report_claim(
        self,
        claim: dict[str, Any],
        action: str,
        value: dict[str, Any],
        lost: asyncio.Event,
    ) -> bool:
        run_id = claim["run"]["run_id"]
        body = {
            "worker_id": self.worker_id,
            "claim_token": claim["claim_token"],
            "fence_token": claim["fence_token"],
            **value,
        }
        while not lost.is_set() and not self._closing:
            try:
                response = await self._request_json(
                    "POST",
                    f"/providers/{PROVIDER_ID}/claims/{run_id}/{action}",
                    body=body,
                    authenticated=True,
                )
                accepted = response.get("accepted") is True
                if not accepted:
                    lost.set()
                return accepted
            except asyncio.CancelledError:
                raise
            except SchedulerProtocolError as exc:
                if exc.status_code == 409:
                    lost.set()
                    return False
                try:
                    await asyncio.wait_for(lost.wait(), timeout=1.0)
                except TimeoutError:
                    pass
            except Exception:
                try:
                    await asyncio.wait_for(lost.wait(), timeout=1.0)
                except TimeoutError:
                    pass
        return False

    async def _run_claim(self, claim: dict[str, Any]) -> None:
        stop = asyncio.Event()
        lost = asyncio.Event()
        maintainer = asyncio.create_task(self._claim_maintainer(claim, stop, lost))
        try:
            try:
                result = await self._execute_target(claim)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = f"target:{type(exc).__name__}"[:240]
                if not lost.is_set():
                    await self._report_claim(claim, "fail", {"error": _safe_failure(exc)}, lost)
            else:
                if lost.is_set():
                    raise ClaimLost("claim lease was lost before completion")
                accepted = await self._report_claim(claim, "complete", {"result": result}, lost)
                if not accepted:
                    self._last_error = "claim:ClaimLost"
                    raise ClaimLost("claim completion was not accepted")
                self._last_error = ""
        finally:
            stop.set()
            maintainer.cancel()
            await asyncio.gather(maintainer, return_exceptions=True)

    async def _status_components(self) -> tuple[dict[str, Any], dict[str, Any], list[Any], dict]:
        status, provider, schedules, history = await asyncio.gather(
            self._request_json("GET", "/status"),
            self._request_json("GET", f"/providers/{PROVIDER_ID}/status"),
            self._request_json("GET", "/schedules"),
            self._request_json("GET", f"/history?provider_id={PROVIDER_ID}&limit=40"),
        )
        schedule_rows = schedules.get("schedules")
        history_rows = history.get("runs")
        if not isinstance(schedule_rows, list) or not isinstance(history_rows, list):
            raise SchedulerProtocolError("scheduler list response is malformed")
        return status, provider, schedule_rows, {"runs": history_rows}

    @staticmethod
    def _matching_schedules(rows: list[Any]) -> list[dict[str, Any]]:
        return [
            item
            for item in rows
            if isinstance(item, dict)
            and item.get("target_key") == TARGET_KEY
            and item.get("provider_id") == PROVIDER_ID
            and not item.get("archived_at")
        ]

    @staticmethod
    def _thresholds(provider: dict[str, Any] | None, schedule: dict[str, Any] | None) -> dict:
        try:
            heartbeat = int((provider or {}).get("stale_after_seconds") or 0)
        except (TypeError, ValueError):
            heartbeat = 0
        heartbeat = max(5, min(heartbeat or DEFAULT_HEARTBEAT_STALE_SECONDS, 3600))
        definition = (schedule or {}).get("schedule_definition")
        definition = definition if isinstance(definition, dict) else {}
        try:
            interval = int(definition.get("seconds") or 0)
        except (TypeError, ValueError):
            interval = 0
        success = DEFAULT_SUCCESS_STALE_SECONDS
        if interval > 0:
            success = max(20, min(interval * 3 + FULL_SYNC_HEADROOM_SECONDS, 86400))
        return {
            "heartbeat_stale_seconds": heartbeat,
            "success_stale_seconds": success,
        }

    async def status_payload(self) -> dict[str, Any]:
        checked_at = _utc_now()
        try:
            status, provider, schedule_rows, history = await self._status_components()
        except Exception as exc:
            return {
                "schema": STATUS_SCHEMA,
                "checked_at": checked_at,
                "scheduler": {
                    "available": False,
                    "version": "",
                    "health": {
                        "ok": False,
                        "classification": exc.classification
                        if isinstance(exc, SchedulerProtocolError)
                        else type(exc).__name__,
                    },
                },
                "provider": None,
                "schedule": None,
                "history": {"runs": []},
                "thresholds": self._thresholds(None, None),
            }
        matches = self._matching_schedules(schedule_rows)
        schedule = None
        health = status.get("health") if isinstance(status.get("health"), dict) else {}
        health = dict(health)
        if len(matches) == 1:
            schedule = {**matches[0], "schedule_definition": matches[0].get("schedule") or {}}
        elif len(matches) > 1:
            health.update({"ok": False, "classification": "ambiguous_personal_search_schedule"})
        return {
            "schema": STATUS_SCHEMA,
            "checked_at": checked_at,
            "scheduler": {
                "available": True,
                "version": str(status.get("version") or ""),
                "health": health,
            },
            "provider": provider,
            "schedule": schedule,
            "history": history,
            "thresholds": self._thresholds(provider, schedule),
        }

    async def run_now(self) -> dict[str, Any]:
        try:
            _status, _provider, rows, _history = await self._status_components()
        except Exception as exc:
            raise HTTPException(503, "Personal Search scheduler is unavailable") from exc
        matches = self._matching_schedules(rows)
        if not matches:
            raise HTTPException(503, "Personal Search sync schedule is missing")
        if len(matches) > 1:
            raise HTTPException(409, "Personal Search sync schedule is ambiguous")
        schedule = matches[0]
        if schedule.get("enabled") is not True:
            raise HTTPException(409, "Personal Search sync schedule is disabled")
        try:
            result = await self._request_json(
                "POST",
                f"/schedules/{schedule['schedule_id']}/run-now",
                body={
                    "actor": "blueprints-backend",
                    "source_surface": "personal-search",
                    "request_id": f"blueprints-personal-search-run-now-{uuid.uuid4().hex}",
                },
            )
        except Exception as exc:
            raise HTTPException(503, "Personal Search sync could not be queued") from exc
        run = result.get("run")
        if not isinstance(run, dict) or not run.get("run_id") or not run.get("status"):
            raise HTTPException(503, "Personal Search scheduler returned no queue acknowledgement")
        return result


service = PersonalSearchScheduler()


async def start_personal_search_scheduler() -> bool:
    return await service.start()


async def stop_personal_search_scheduler() -> None:
    await service.stop()


async def pause_personal_search_claims_for_restart() -> dict[str, Any]:
    return await service.pause_new_claims_for_restart()


async def resume_personal_search_claims_after_restart_abort() -> bool:
    return await service.resume_new_claims_after_restart_abort()


@router.get("/status")
async def personal_search_scheduler_status() -> dict[str, Any]:
    return await service.status_payload()


@router.post("/run-now")
async def personal_search_scheduler_run_now(_body: EmptyRunNowRequest) -> dict[str, Any]:
    return await service.run_now()
