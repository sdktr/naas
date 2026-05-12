"""NATS JetStream-backed queue compatibility layer.

This module provides a minimal RQ-compatible API used by NAAS resources while
routing transport via NATS JetStream. It keeps an in-memory state cache so API
status endpoints continue to work even when JetStream is temporarily
unavailable.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import socket
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from socket import gethostname
from typing import Any
from uuid import uuid4

from nats import connect as nats_connect

logger = logging.getLogger(__name__)


class BackendUnavailableError(RuntimeError):
    """Raised when the queue backend is unavailable."""


class NoSuchJobError(LookupError):
    """Raised when a job cannot be found."""


class JobStatus(StrEnum):
    QUEUED = "queued"
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass
class Retry:
    max: int = 0
    interval: list[int] = field(default_factory=list)


@dataclass
class Callback:
    func: Callable[..., Any]


@dataclass
class _WorkerState:
    name: str
    hostname: str
    queues: list[str]
    last_heartbeat: datetime | None = None
    current_job_id: str | None = None


class _Store:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.jobs: dict[str, Job] = {}
        self.queues: dict[str, list[str]] = {}
        self.worker_states: dict[str, _WorkerState] = {}
        self.sorted_sets: dict[str, list[float]] = {}
        self.kv: dict[str, bytes] = {}
        self.hashes: dict[str, dict[str, bytes]] = {}
        self.sets: dict[str, set[str]] = {}


_STORE = _Store()


class RedisLikeKV:
    """Small Redis-like API used by auth/dedup/idempotency/api-key helpers."""

    def ping(self) -> bool:
        return True

    def get(self, key: str) -> bytes | None:
        with _STORE.lock:
            return _STORE.kv.get(key)

    def set(self, key: str, value: str | bytes, ex: int | None = None, nx: bool = False) -> bool:
        _ = ex
        with _STORE.lock:
            if nx and key in _STORE.kv:
                return False
            if isinstance(value, str):
                _STORE.kv[key] = value.encode()
            else:
                _STORE.kv[key] = value
            return True

    def setnx(self, key: str, value: str | bytes) -> bool:
        return self.set(key, value, nx=True)

    def delete(self, key: str) -> int:
        with _STORE.lock:
            existed = key in _STORE.kv
            _STORE.kv.pop(key, None)
            return 1 if existed else 0

    def expire(self, key: str, ttl: int) -> bool:
        _ = ttl
        with _STORE.lock:
            return key in _STORE.kv or key in _STORE.hashes

    def zremrangebyscore(self, key: str, minimum: float, maximum: float) -> int:
        with _STORE.lock:
            vals = _STORE.sorted_sets.get(key, [])
            keep = [v for v in vals if not (minimum <= v <= maximum)]
            removed = len(vals) - len(keep)
            _STORE.sorted_sets[key] = keep
            return removed

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        with _STORE.lock:
            vals = _STORE.sorted_sets.setdefault(key, [])
            vals.extend(mapping.values())
            return len(mapping)

    def zcard(self, key: str) -> int:
        with _STORE.lock:
            return len(_STORE.sorted_sets.get(key, []))

    def hset(self, key: str, field: str | None = None, value: str | bytes | None = None, mapping: dict | None = None) -> int:
        with _STORE.lock:
            h = _STORE.hashes.setdefault(key, {})
            if mapping is not None:
                for k, v in mapping.items():
                    h[str(k)] = v.encode() if isinstance(v, str) else bytes(v)
                return len(mapping)
            if field is None or value is None:
                return 0
            h[str(field)] = value.encode() if isinstance(value, str) else bytes(value)
            return 1

    def hget(self, key: str, field: str) -> bytes | None:
        with _STORE.lock:
            return _STORE.hashes.get(key, {}).get(field)

    def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        with _STORE.lock:
            h = _STORE.hashes.setdefault(key, {})
            cur = int((h.get(field) or b"0").decode())
            nxt = cur + amount
            h[field] = str(nxt).encode()
            return nxt

    def hgetall(self, key: str) -> dict[bytes, bytes]:
        with _STORE.lock:
            h = _STORE.hashes.get(key, {})
            return {k.encode(): v for k, v in h.items()}

    def keys(self, pattern: str) -> list[bytes]:
        # supports prefix* patterns used in code
        prefix = pattern[:-1] if pattern.endswith("*") else pattern
        with _STORE.lock:
            keys = [k.encode() for k in _STORE.hashes if k.startswith(prefix)]
            return keys

    def sismember(self, key: str, member: str) -> bool:
        with _STORE.lock:
            return member in _STORE.sets.get(key, set())

    def sadd(self, key: str, member: str) -> int:
        with _STORE.lock:
            s = _STORE.sets.setdefault(key, set())
            before = len(s)
            s.add(member)
            return 1 if len(s) > before else 0

    def exists(self, key: str) -> bool:
        with _STORE.lock:
            return key in _STORE.hashes or key in _STORE.kv


class Job:
    def __init__(
        self,
        *,
        queue_name: str,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        connection: RedisLikeKV,
        job_id: str,
        meta: dict[str, Any] | None = None,
        on_success: Callback | None = None,
        on_failure: Callback | None = None,
    ) -> None:
        self.id = job_id
        self.queue_name = queue_name
        self.func = func
        self.func_name = f"{func.__module__}.{getattr(func, '__name__', 'task')}"
        self.args = args
        self.kwargs = kwargs
        self.connection = connection
        self.meta = meta or {}
        self.on_success = on_success
        self.on_failure = on_failure
        self.created_at = datetime.now(tz=UTC)
        self.enqueued_at = self.created_at
        self.started_at: datetime | None = None
        self.ended_at: datetime | None = None
        self.result: Any = None
        self.exc_info: str | None = None
        self.worker_name: str | None = None
        self._status = JobStatus.QUEUED

    def get_status(self) -> JobStatus:
        return self._status

    def set_status(self, status: JobStatus) -> None:
        self._status = status

    def save_meta(self) -> None:
        with _STORE.lock:
            if self.id in _STORE.jobs:
                _STORE.jobs[self.id].meta = dict(self.meta)

    def cancel(self) -> None:
        with _STORE.lock:
            self._status = JobStatus.CANCELED
            q = _STORE.queues.setdefault(self.queue_name, [])
            if self.id in q:
                q.remove(self.id)

    def delete(self) -> None:
        with _STORE.lock:
            _STORE.jobs.pop(self.id, None)
            for q in _STORE.queues.values():
                with contextlib.suppress(ValueError):
                    q.remove(self.id)

    @classmethod
    def fetch(cls, job_id: str, connection: RedisLikeKV) -> Job:
        _ = connection
        with _STORE.lock:
            job = _STORE.jobs.get(job_id)
            if job is None:
                raise NoSuchJobError(job_id)
            return job

    @classmethod
    def fetch_many(cls, job_ids: list[str], connection: RedisLikeKV) -> list[Job | None]:
        _ = connection
        with _STORE.lock:
            return [_STORE.jobs.get(job_id) for job_id in job_ids]


class Queue:
    def __init__(self, name: str, connection: RedisLikeKV) -> None:
        self.name = name
        self.connection = connection
        with _STORE.lock:
            _STORE.queues.setdefault(name, [])

    @property
    def job_ids(self) -> list[str]:
        with _STORE.lock:
            return list(_STORE.queues.get(self.name, []))

    def __len__(self) -> int:
        return len(self.job_ids)

    def get_job_ids(self, offset: int = 0, length: int | None = None) -> list[str]:
        ids = self.job_ids
        if length is None:
            return ids[offset:]
        return ids[offset : offset + length]

    def fetch_job(self, job_id: str) -> Job | None:
        with _STORE.lock:
            return _STORE.jobs.get(job_id)

    def enqueue(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Job:
        job_id = kwargs.pop("job_id", str(uuid4()))
        _ = kwargs.pop("job_timeout", None)
        _ = kwargs.pop("result_ttl", None)
        _ = kwargs.pop("failure_ttl", None)
        _ = kwargs.pop("retry", None)
        on_success = kwargs.pop("on_success", None)
        on_failure = kwargs.pop("on_failure", None)
        meta = kwargs.pop("meta", None)

        job = Job(
            queue_name=self.name,
            func=func,
            args=args,
            kwargs=kwargs,
            connection=self.connection,
            job_id=job_id,
            meta=meta,
            on_success=on_success,
            on_failure=on_failure,
        )
        with _STORE.lock:
            _STORE.jobs[job_id] = job
            _STORE.queues.setdefault(self.name, []).append(job_id)

        publish_job(self.name, job)
        return job


class Worker:
    def __init__(self, queues: list[Queue], name: str, connection: RedisLikeKV) -> None:
        self._queues = queues
        self.name = name
        self.connection = connection
        self.hostname = gethostname()
        self.last_heartbeat: datetime | None = None
        self._current_job_id: str | None = None

    @classmethod
    def all(cls, connection: RedisLikeKV) -> list[Worker]:
        _ = connection
        with _STORE.lock:
            workers = []
            for state in _STORE.worker_states.values():
                w = Worker(
                    [Queue(q, connection) for q in state.queues],
                    state.name,
                    connection,
                )
                w.hostname = state.hostname
                w.last_heartbeat = state.last_heartbeat
                w._current_job_id = state.current_job_id
                workers.append(w)
            return workers

    def queue_names(self) -> list[str]:
        return [q.name for q in self._queues]

    def get_current_job(self) -> Job | None:
        if self._current_job_id is None:
            return None
        with _STORE.lock:
            return _STORE.jobs.get(self._current_job_id)

    def request_stop(self, signum, frame) -> None:
        _ = signum, frame


class BaseWorker(Worker):
    pass


class _Registry:
    status: JobStatus

    def __init__(self, queue: Queue | None = None, connection: RedisLikeKV | None = None) -> None:
        self.queue = queue
        self.connection = connection

    @property
    def count(self) -> int:
        return len(self.get_job_ids())

    def __len__(self) -> int:
        return self.count

    def get_job_ids(self, start: int = 0, end: int = -1) -> list[str]:
        with _STORE.lock:
            ids = [jid for jid, job in _STORE.jobs.items() if job.get_status() == self.status]
            if self.queue is not None:
                ids = [jid for jid in ids if _STORE.jobs[jid].queue_name == self.queue.name]
            if end < 0:
                return ids[start:]
            return ids[start : end + 1]

    def add(self, job: Job, exc_string: str = "") -> None:
        _ = exc_string
        job.set_status(self.status)

    def remove(self, job: Job) -> None:
        _ = job


class FinishedJobRegistry(_Registry):
    status = JobStatus.FINISHED


class FailedJobRegistry(_Registry):
    status = JobStatus.FAILED


class StartedJobRegistry(_Registry):
    status = JobStatus.STARTED


# NATS transport configuration
NATS_SERVERS = "nats://localhost:4222"
TASK_SUBJECT_PREFIX = "naas.jobs"
DEVICE_DETAILS_SUBJECT_PREFIX = "naas.devices"


def configure_nats(*, servers: str | None = None) -> None:
    global NATS_SERVERS
    if servers:
        NATS_SERVERS = servers


async def _publish_job_async(subject: str, payload: dict[str, Any]) -> None:
    nc = await nats_connect(servers=[s.strip() for s in NATS_SERVERS.split(",") if s.strip()])
    try:
        await nc.publish(subject, json.dumps(payload, default=str).encode())
        await nc.flush(timeout=1)
    finally:
        await nc.close()


def publish_job(queue_name: str, job: Job) -> None:
    payload = {
        "job_id": job.id,
        "queue": queue_name,
        "func": job.func_name,
        "kwargs": job.kwargs,
        "meta": job.meta,
        "enqueued_at": job.enqueued_at.isoformat(),
    }
    subject = f"{TASK_SUBJECT_PREFIX}.{queue_name}"
    servers = [s.strip() for s in NATS_SERVERS.split(",") if s.strip()]

    for server in servers:
        with contextlib.suppress(Exception):
            host_port = server.split("://", 1)[-1]
            host = host_port.rsplit(":", 1)[0]
            socket.getaddrinfo(host, None)
            break
    else:
        logger.warning("Skipping NATS publish for %s: none of NATS_SERVERS are resolvable (%s)", job.id, NATS_SERVERS)
        return

    def _run() -> None:
        with contextlib.suppress(Exception):
            asyncio.run(_publish_job_async(subject, payload))

    threading.Thread(target=_run, daemon=True).start()


async def fetch_device_details(host: str, context: str = "default") -> dict[str, Any] | None:
    subject = f"{DEVICE_DETAILS_SUBJECT_PREFIX}.{context}.get"
    nc = await nats_connect(servers=[s.strip() for s in NATS_SERVERS.split(",") if s.strip()])
    try:
        msg = await nc.request(subject, json.dumps({"host": host}).encode(), timeout=1.0)
    except Exception:
        await nc.close()
        return None
    await nc.close()
    with contextlib.suppress(Exception):
        return json.loads(msg.data.decode())
    return None


def register_worker_heartbeat(name: str, queues: list[str], current_job_id: str | None = None) -> None:
    with _STORE.lock:
        _STORE.worker_states[name] = _WorkerState(
            name=name,
            hostname=gethostname(),
            queues=queues,
            last_heartbeat=datetime.now(tz=UTC),
            current_job_id=current_job_id,
        )


def set_job_started(job_id: str, worker_name: str) -> Job | None:
    with _STORE.lock:
        job = _STORE.jobs.get(job_id)
        if job is None:
            return None
        job.started_at = datetime.now(tz=UTC)
        job.worker_name = worker_name
        job.set_status(JobStatus.STARTED)
        q = _STORE.queues.get(job.queue_name, [])
        with contextlib.suppress(ValueError):
            q.remove(job_id)
        return job


def set_job_result(job_id: str, *, result: Any = None, error: str | None = None) -> Job | None:
    with _STORE.lock:
        job = _STORE.jobs.get(job_id)
        if job is None:
            return None
        job.ended_at = datetime.now(tz=UTC)
        if error:
            job.exc_info = error
            job.result = None
            job.set_status(JobStatus.FAILED)
        else:
            job.result = result
            job.exc_info = None
            job.set_status(JobStatus.FINISHED)
        return job


def iter_jobs(status: str | None = None) -> list[Job]:
    with _STORE.lock:
        jobs = list(_STORE.jobs.values())
    if status:
        jobs = [j for j in jobs if j.get_status().value == status]
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return jobs
