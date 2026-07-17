"""Typed xarta-scheduler provider for one bounded Kanban automation tick."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter

from . import routes_personal

log = logging.getLogger(__name__)

PROVIDER_ID = "blueprints-kanban-automation"
AUTOMATION_TARGET_KEY = "blueprints_kanban_automation_tick_v1"
BLOCKER_TARGET_KEY = "blueprints_kanban_blocker_tick_v1"
TARGET_KEYS = frozenset({AUTOMATION_TARGET_KEY, BLOCKER_TARGET_KEY})
TARGET_KEY = AUTOMATION_TARGET_KEY
STATUS_SCHEMA = "xarta.blueprints.kanban_automation.scheduler_status.v1"
RESULT_SCHEMA = "xarta.blueprints.kanban_automation_tick.result.v1"
BLOCKER_RESULT_SCHEMA = "xarta.blueprints.kanban_blocker_tick.result.v1"
DEFAULT_SCHEDULER_URL = "http://127.0.0.1:18111"
TOKEN_ENV = "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_KANBAN_AUTOMATION_TOKEN"
TOKEN_FILE_ENV = "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_KANBAN_AUTOMATION_TOKEN_FILE"
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 30 * 1024
REQUEST_TIMEOUT_SECONDS = 3.0
REPORT_TIMEOUT_SECONDS = 15.0
TARGET_TIMEOUT_SECONDS = 840.0
BLOCKER_TIMEOUT_SECONDS = 330.0
BLOCKER_MAX_OUTPUT_BYTES = 1024 * 1024
DOCKER_BIN = "/usr/bin/docker"
HERMES_CONTAINER = "hermes-local"
HERMES_CONTAINER_USER = "10000:10000"
HERMES_BLOCKER_SCRIPT = "/opt/data/scripts/run_hermes_kanban_blocker_resolver.py"
HERMES_BLOCKER_STATE = "/opt/data/kanban-blocker-processor/state.json"
HERMES_BLOCKER_HEALTH = "/opt/data/health/hermes-kanban-blocker-processor.json"
HERMES_BLOCKER_LOCK = "/opt/data/locks/hermes-kanban-blocker-processor.lock"

router = APIRouter(
    prefix="/personal/kanban/scheduler",
    tags=["personal-kanban-scheduler"],
)


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
            "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_KANBAN_AUTOMATION_HEARTBEAT_SECONDS",
            30.0,
            1.0,
            60.0,
        ),
        claim_interval=_bounded_float(
            "XARTA_SCHEDULER_PROVIDER_BLUEPRINTS_KANBAN_AUTOMATION_CLAIM_SECONDS",
            1.0,
            0.25,
            10.0,
        ),
    )


def _scheduler_http_client(config: ProviderConfig) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=config.base_url,
        timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=1.0),
        limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
        trust_env=False,
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _provider_worker_id(producer: dict[str, Any]) -> str:
    owner_node_id = str(producer.get("owner_node_id") or "unconfigured-owner")
    return f"{PROVIDER_ID}:{owner_node_id}"


def _parse_iso(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _bounded_result(result: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), default=str).encode()
    if len(encoded) > MAX_RESULT_BYTES:
        raise RuntimeError("Kanban scheduler result exceeded its bound")
    return result


def _safe_failure(exc: BaseException) -> str:
    if isinstance(exc, TargetRejected):
        return f"TargetRejected: {str(exc)[:240]}"
    return f"{type(exc).__name__}: Kanban automation target failed"


def _failure_reasons(
    tick: dict[str, Any] | None = None,
    blocker: dict[str, Any] | None = None,
) -> list[str]:
    tick = tick or {}
    blocker = blocker or {}
    reasons: list[str] = []
    for marker in tick.get("processed_markers") or []:
        if isinstance(marker, dict) and marker.get("ok") is False:
            reason = str(marker.get("reason") or marker.get("error") or "marker_processing_failed")
            if reason and reason not in reasons:
                reasons.append(reason[:500])
    if blocker.get("ok") is False:
        reason = str(blocker.get("error") or "blocker_resolver_failed")
        if reason not in reasons:
            reasons.append(reason[:500])
    return reasons[:20]


def _automation_phase_counts(tick: dict[str, Any]) -> dict[str, int]:
    timeout = tick.get("timeout_requeue") if isinstance(tick.get("timeout_requeue"), dict) else {}
    review = tick.get("review_scan") if isinstance(tick.get("review_scan"), dict) else {}
    preprocessing = (
        tick.get("preprocessing_scan") if isinstance(tick.get("preprocessing_scan"), dict) else {}
    )
    return {
        "timeout_requeued": int(timeout.get("requeued_count") or 0),
        "review_scanned": int(review.get("scanned_count") or 0),
        "review_queued": int(review.get("queued_count") or 0),
        "preprocessing_scanned": int(preprocessing.get("scanned_count") or 0),
        "preprocessing_queued": int(preprocessing.get("queued_count") or 0),
        "eligible_markers": int(tick.get("eligible_marker_count") or 0),
        "processed_markers": int(tick.get("processed_count") or 0),
    }


def _automation_shadow_phase_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    return {
        "timeout_candidates": len(snapshot.get("timeout_marker_ids") or []),
        "review_candidates": len(snapshot.get("review_candidate_ids") or []),
        "review_would_queue": len(snapshot.get("review_queue_item_ids") or []),
        "preprocessing_candidates": len(snapshot.get("preprocessing_candidate_ids") or []),
        "preprocessing_would_queue": len(snapshot.get("preprocessing_queue_item_ids") or []),
        "claimable_markers": len(snapshot.get("claimable_marker_ids") or []),
    }


def _blocker_phase_counts(blocker: dict[str, Any]) -> dict[str, int]:
    return {
        "blocker_candidates": int(blocker.get("candidate_count") or 0),
        "blockers_examined": int(blocker.get("examined") or 0),
        "blockers_processed": int(blocker.get("processed") or 0),
    }


async def _run_blocker_resolver(
    *,
    tick_id: str,
    worker_id: str,
    apply: bool,
) -> dict[str, Any]:
    mode = "--apply" if apply else "--dry-run"
    command = [
        DOCKER_BIN,
        "exec",
        "--user",
        HERMES_CONTAINER_USER,
        HERMES_CONTAINER,
        "python3",
        HERMES_BLOCKER_SCRIPT,
        mode,
        "--limit",
        "5",
        "--scan-item-limit",
        "250",
        "--llm-timeout",
        "240",
        "--run-timeout",
        "300",
        "--state-path",
        HERMES_BLOCKER_STATE,
        "--health-path",
        HERMES_BLOCKER_HEALTH,
        "--processor-lock-path",
        HERMES_BLOCKER_LOCK,
        "--actor",
        worker_id,
        "--run-id",
        f"{tick_id}:blockers",
        "--json",
    ]
    if apply:
        command.append("--write-health")
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(),
            timeout=BLOCKER_TIMEOUT_SECONDS,
        )
    except asyncio.CancelledError:
        process.kill()
        await process.wait()
        raise
    except TimeoutError:
        process.kill()
        await process.wait()
        return {"ok": False, "error": "blocker_resolver_timeout"}
    if len(stdout) > BLOCKER_MAX_OUTPUT_BYTES or len(stderr) > BLOCKER_MAX_OUTPUT_BYTES:
        return {"ok": False, "error": "blocker_resolver_output_too_large"}
    try:
        payload = json.loads(stdout.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        return {
            "ok": False,
            "error": f"blocker_resolver_invalid_output_exit_{process.returncode}",
        }
    if not isinstance(payload, dict):
        return {"ok": False, "error": "blocker_resolver_non_object_output"}
    if process.returncode != 0:
        payload["ok"] = False
        payload.setdefault("error", f"blocker_resolver_exit_{process.returncode}")
    return payload


class KanbanAutomationScheduler:
    def __init__(
        self,
        *,
        config: ProviderConfig | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.client = client
        self._owns_client = client is None
        self.worker_id = ""
        self._loop_tasks: list[asyncio.Task[Any]] = []
        self._active_runs: dict[str, asyncio.Task[Any]] = {}
        self._status_lock: asyncio.Lock | None = None
        self._status_inflight_task: asyncio.Task[dict[str, Any]] | None = None
        self._closing = False
        self._last_error = ""
        self.config_error = ""

    def _get_status_lock(self) -> asyncio.Lock:
        if self._status_lock is None:
            self._status_lock = asyncio.Lock()
        return self._status_lock

    async def _producer_config(self) -> dict[str, Any]:
        producer = await asyncio.to_thread(routes_personal._work_automation_idle_worker_config)
        if not self.worker_id:
            self.worker_id = _provider_worker_id(producer)
        return producer

    async def start(self) -> bool:
        if self._loop_tasks:
            return True
        producer = await self._producer_config()
        if not producer["scheduler_provider_effective_enabled"]:
            log.info(
                "Kanban scheduler provider disabled by producer mode %s",
                producer["producer_mode"],
            )
            return False
        try:
            self.config = self.config or await asyncio.to_thread(load_provider_config)
        except (OSError, UnicodeError, ValueError) as exc:
            self.config_error = type(exc).__name__
            log.warning("Kanban scheduler provider disabled: invalid configuration")
            return False
        if self.client is None:
            self.client = _scheduler_http_client(self.config)
        if not self.config.token:
            log.info("Kanban scheduler provider disabled: token is not configured")
            return False
        self._closing = False
        self._loop_tasks = [
            asyncio.create_task(self._heartbeat_loop(), name="kanban-scheduler-heartbeat"),
            asyncio.create_task(self._claim_loop(), name="kanban-scheduler-claims"),
        ]
        return True

    async def stop(self) -> None:
        self._closing = True
        tasks = [*self._loop_tasks, *self._active_runs.values()]
        if self._status_inflight_task is not None and not self._status_inflight_task.done():
            tasks.append(self._status_inflight_task)
        self._status_inflight_task = None
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
            config = self.config or await asyncio.to_thread(load_provider_config)
            self.config = config
            self.client = _scheduler_http_client(config)
        response = await self.client.request(
            method,
            path,
            json=body,
            headers=self._auth_headers() if authenticated else None,
        )
        if response.status_code >= 400:
            raise SchedulerProtocolError(
                f"scheduler_http_{response.status_code}",
                status_code=response.status_code,
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
                producer = await self._producer_config()
                await self._request_json(
                    "POST",
                    f"/providers/{PROVIDER_ID}/heartbeat",
                    body={
                        "worker_id": self.worker_id,
                        "state": state,
                        "current_run_ids": run_ids,
                        "metadata": {
                            "protocol": "blueprints-kanban-automation-provider-v1",
                            "producer_mode": producer["producer_mode"],
                        },
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
                if self._active_runs:
                    await asyncio.sleep(self.config.claim_interval)
                    continue
                response = await self._request_json(
                    "POST",
                    f"/providers/{PROVIDER_ID}/claims/next",
                    body={
                        "worker_id": self.worker_id,
                        "request_id": f"blueprints-kanban-claim-{uuid.uuid4().hex}",
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
                        raise SchedulerProtocolError("actionable claim omitted fencing data")
                    task = asyncio.create_task(self._run_claim(claim), name=f"kanban-{run_id}")
                    self._active_runs[run_id] = task
                    task.add_done_callback(lambda done, rid=run_id: self._claim_done(rid, done))
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
            lease_seconds = float(policies.get("lease_seconds") or 900.0)
        except (TypeError, ValueError):
            lease_seconds = 900.0
        lease_seconds = max(5.0, min(lease_seconds, 3600.0))
        interval = max(1.0, min(lease_seconds / 3.0, 30.0))
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

    async def _assert_claim_current(self, claim: dict[str, Any]) -> None:
        run = claim["run"]
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
        if response.get("accepted") is not True:
            raise ClaimLost("claim fence was rejected before target execution")

    async def _execute_target(self, claim: dict[str, Any]) -> dict[str, Any]:
        run = claim.get("run") if isinstance(claim.get("run"), dict) else {}
        scheduler_run_id = str(run.get("run_id") or "")
        target_key = str(run.get("target_key") or "")
        if target_key not in TARGET_KEYS or not scheduler_run_id:
            raise TargetRejected("claim target is outside the Kanban automation allowlist")
        target_config = claim.get("target_config")
        if not isinstance(target_config, dict) or target_config:
            raise TargetRejected("Kanban automation target config must be exactly empty")
        tick_id = (
            f"{PROVIDER_ID}:{scheduler_run_id}"
            if target_key == AUTOMATION_TARGET_KEY
            else f"{PROVIDER_ID}:blocker:{scheduler_run_id}"
        )
        producer = await self._producer_config()
        if not producer["scheduler_provider_effective_enabled"]:
            raise TargetRejected("Kanban scheduler provider is fenced on this node")

        if producer["producer_mode"] == "scheduler_shadow":
            if target_key == AUTOMATION_TARGET_KEY:
                snapshot = await routes_personal.work_kanban_automation_shadow_snapshot()
                result = {
                    "schema": RESULT_SCHEMA,
                    "scheduler_run_id": scheduler_run_id,
                    "tick_id": tick_id,
                    "producer_node_id": str(producer.get("owner_node_id") or ""),
                    "outcome": "skipped",
                    "coverage_complete": True,
                    "truncated": False,
                    "error_count": 0,
                    "phase_counts": _automation_shadow_phase_counts(snapshot),
                    "failure_reasons": [],
                }
                return _bounded_result(result)
            blocker = await _run_blocker_resolver(
                tick_id=tick_id,
                worker_id=self.worker_id,
                apply=False,
            )
            reasons = _failure_reasons(blocker=blocker)
            blocker_candidates = int(blocker.get("candidate_count") or 0)
            blocker_examined = int(blocker.get("examined") or 0)
            truncated = bool(
                blocker.get("run_timeout_reached") or blocker_candidates > blocker_examined
            )
            result = {
                "schema": BLOCKER_RESULT_SCHEMA,
                "scheduler_run_id": scheduler_run_id,
                "tick_id": tick_id,
                "producer_node_id": str(producer.get("owner_node_id") or ""),
                "outcome": (
                    "truncated"
                    if truncated
                    else ("completed_with_errors" if reasons else "skipped")
                ),
                "coverage_complete": not truncated,
                "truncated": truncated,
                "error_count": len(reasons),
                "phase_counts": _blocker_phase_counts(blocker),
                "failure_reasons": reasons,
            }
            return _bounded_result(result)

        if producer["producer_mode"] != "scheduler":
            raise TargetRejected("Kanban scheduler mutations are not enabled")
        replay = await routes_personal.work_kanban_automation_tick_receipt(tick_id)
        if replay is not None:
            return _bounded_result(replay)

        if target_key == AUTOMATION_TARGET_KEY:
            tick = await routes_personal.run_work_kanban_automation_idle_tick(
                holder_id=self.worker_id,
                run_id=tick_id,
                actor=self.worker_id,
                source_surface="xarta-scheduler-provider",
                request_id=tick_id,
                producer_source="scheduler",
            )
            reasons = _failure_reasons(tick=tick)
            eligible = int(tick.get("eligible_marker_count") or 0)
            processed = int(tick.get("processed_count") or 0)
            truncated = eligible > processed
            skipped = tick.get("lease_acquired") is False and not reasons and not truncated
            outcome = (
                "truncated"
                if truncated
                else (
                    "completed_with_errors" if reasons else ("skipped" if skipped else "completed")
                )
            )
            result = {
                "schema": RESULT_SCHEMA,
                "scheduler_run_id": scheduler_run_id,
                "tick_id": tick_id,
                "producer_node_id": str(producer.get("owner_node_id") or ""),
                "outcome": outcome,
                "coverage_complete": not truncated,
                "truncated": truncated,
                "error_count": len(reasons),
                "phase_counts": _automation_phase_counts(tick),
                "failure_reasons": reasons,
            }
        else:
            blocker = await _run_blocker_resolver(
                tick_id=tick_id,
                worker_id=self.worker_id,
                apply=True,
            )
            reasons = _failure_reasons(blocker=blocker)
            blocker_candidates = int(blocker.get("candidate_count") or 0)
            blocker_examined = int(blocker.get("examined") or 0)
            truncated = bool(
                blocker.get("run_timeout_reached") or blocker_candidates > blocker_examined
            )
            result = {
                "schema": BLOCKER_RESULT_SCHEMA,
                "scheduler_run_id": scheduler_run_id,
                "tick_id": tick_id,
                "producer_node_id": str(producer.get("owner_node_id") or ""),
                "outcome": (
                    "truncated"
                    if truncated
                    else ("completed_with_errors" if reasons else "completed")
                ),
                "coverage_complete": not truncated,
                "truncated": truncated,
                "error_count": len(reasons),
                "phase_counts": _blocker_phase_counts(blocker),
                "failure_reasons": reasons,
            }
        result = _bounded_result(result)
        return await routes_personal.record_work_kanban_automation_tick_receipt(
            tick_id=tick_id,
            scheduler_run_id=scheduler_run_id,
            result=result,
        )

    async def _report_claim(
        self,
        claim: dict[str, Any],
        action: str,
        value: dict[str, Any],
        lost: asyncio.Event,
    ) -> bool:
        run_id = claim["run"]["run_id"]
        deadline = asyncio.get_running_loop().time() + REPORT_TIMEOUT_SECONDS
        body = {
            "worker_id": self.worker_id,
            "claim_token": claim["claim_token"],
            "fence_token": claim["fence_token"],
            **value,
        }
        while not lost.is_set() and not self._closing:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                self._last_error = f"claim:{action}_report_timeout"
                return False
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
            except Exception:
                pass
            try:
                await asyncio.wait_for(lost.wait(), timeout=min(1.0, remaining))
            except TimeoutError:
                pass
        return False

    async def _run_claim(self, claim: dict[str, Any]) -> None:
        try:
            await self._assert_claim_current(claim)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._last_error = "claim:ClaimLost"
            return
        stop = asyncio.Event()
        lost = asyncio.Event()
        maintainer = asyncio.create_task(self._claim_maintainer(claim, stop, lost))
        try:
            try:
                result = await asyncio.wait_for(
                    self._execute_target(claim),
                    timeout=TARGET_TIMEOUT_SECONDS,
                )
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
                    if not self._last_error.startswith("claim:complete_report_timeout"):
                        self._last_error = "claim:ClaimLost"
                    raise ClaimLost("claim completion was not accepted")
                self._last_error = ""
        finally:
            stop.set()
            maintainer.cancel()
            await asyncio.gather(maintainer, return_exceptions=True)

    async def status_payload(self) -> dict[str, Any]:
        lock = self._get_status_lock()
        async with lock:
            if self._status_inflight_task is None or self._status_inflight_task.done():
                self._status_inflight_task = asyncio.create_task(
                    self._build_status_payload(),
                    name="kanban-scheduler-status",
                )
            task = self._status_inflight_task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done():
                async with lock:
                    if self._status_inflight_task is task:
                        self._status_inflight_task = None

    async def _build_status_payload(self) -> dict[str, Any]:
        producer = await self._producer_config()
        try:
            automation = await routes_personal.get_work_automation_status(
                item_id=None,
                limit=5,
                include_contracts=False,
                include_auth_drift=False,
                include_decision_metadata=False,
                metrics=False,
                compact=True,
            )
        except Exception as exc:
            automation = {
                "ok": False,
                "schema": "xarta.kanban.automation_status.compact.v1",
                "error_classification": type(exc).__name__,
            }
        payload: dict[str, Any] = {
            "schema": STATUS_SCHEMA,
            "checked_at": _utc_now(),
            "producer": {
                key: producer.get(key)
                for key in (
                    "producer_mode",
                    "producer_mode_valid",
                    "current_node_id",
                    "owner_node_id",
                    "singleton_owner_match",
                    "legacy_loop_effective_enabled",
                    "scheduler_provider_effective_enabled",
                    "scheduler_mutations_enabled",
                    "manual_recovery_enabled",
                )
            },
            "worker_id": self.worker_id,
            "active_run_ids": list(self._active_runs),
            "last_error": self._last_error,
            "config_error": self.config_error,
            "automation": automation,
            "scheduler": {"available": False},
            "provider": None,
            "schedule": None,
            "blocker_schedule": None,
            "schedules": {},
            "history": {"runs": []},
        }
        try:
            status, provider, schedules, history = await asyncio.gather(
                self._request_json("GET", "/status"),
                self._request_json("GET", f"/providers/{PROVIDER_ID}/status"),
                self._request_json("GET", "/schedules"),
                self._request_json("GET", f"/history?provider_id={PROVIDER_ID}&limit=20"),
            )
            schedule_rows = schedules.get("schedules")
            history_rows = history.get("runs")
            if not isinstance(schedule_rows, list) or not isinstance(history_rows, list):
                raise SchedulerProtocolError("scheduler list response is malformed")
            matches_by_target = {
                target_key: [
                    item
                    for item in schedule_rows
                    if isinstance(item, dict)
                    and item.get("target_key") == target_key
                    and item.get("provider_id") == PROVIDER_ID
                    and not item.get("archived_at")
                ]
                for target_key in TARGET_KEYS
            }
            health = status.get("health") if isinstance(status.get("health"), dict) else {}
            ambiguous_targets = [
                target_key for target_key, matches in matches_by_target.items() if len(matches) > 1
            ]
            if ambiguous_targets:
                health = {
                    **health,
                    "ok": False,
                    "classification": "ambiguous_kanban_automation_schedule",
                    "ambiguous_target_keys": sorted(ambiguous_targets),
                }
            payload["scheduler"] = {
                "available": True,
                "version": str(status.get("version") or ""),
                "health": health,
            }
            payload["provider"] = provider
            payload["schedules"] = {
                target_key: matches[0] if len(matches) == 1 else None
                for target_key, matches in matches_by_target.items()
            }
            payload["schedule"] = payload["schedules"][AUTOMATION_TARGET_KEY]
            payload["blocker_schedule"] = payload["schedules"][BLOCKER_TARGET_KEY]
            payload["history"] = {"runs": history_rows}
        except Exception as exc:
            payload["scheduler"] = {
                "available": False,
                "health": {
                    "ok": False,
                    "classification": (
                        exc.classification
                        if isinstance(exc, SchedulerProtocolError)
                        else type(exc).__name__
                    ),
                },
            }
        return payload


service = KanbanAutomationScheduler()


async def start_kanban_automation_scheduler() -> bool:
    return await service.start()


async def stop_kanban_automation_scheduler() -> None:
    await service.stop()


@router.get("/status")
async def kanban_automation_scheduler_status() -> dict[str, Any]:
    return await service.status_payload()
