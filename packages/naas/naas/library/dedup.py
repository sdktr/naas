"""
dedup.py
Server-side automatic job deduplication based on content hash.
Prevents duplicate in-flight jobs for the same host+platform+commands+user.
"""

import hashlib
import json
from typing import TYPE_CHECKING

from naas.config import JOB_DEDUP_ENABLED, JOB_TIMEOUT

if TYPE_CHECKING:
    from naas.library.nats_queue import KVStore

_DEDUP_TTL = JOB_TIMEOUT + 60  # Safety net: expires slightly after job timeout


def _dedup_key(host: str, platform: str, commands: list[str], username: str) -> str:
    """Build a KVStore key from the content hash of the job parameters."""
    content = json.dumps(
        {"host": host, "platform": platform, "commands": sorted(commands), "username": username},
        sort_keys=True,
    )
    digest = hashlib.sha256(content.encode()).hexdigest()
    return f"naas:dedup:{digest}"


def get_duplicate_job_id(host: str, platform: str, commands: list[str], username: str, kv_store: "KVStore") -> str | None:
    """
    Return an existing in-flight job_id for this host+platform+commands+user, or None.

    Args:
        host: Target device host
        platform: Netmiko platform
        commands: Command list
        username: Requesting username
        kv_store: KVStore connection

    Returns:
        Existing job_id if a duplicate is in-flight, else None
    """
    if not JOB_DEDUP_ENABLED:
        return None
    stored = kv_store.get(_dedup_key(host, platform, commands, username))
    if stored is None:
        return None
    return stored.decode() if isinstance(stored, bytes) else str(stored)


def register_dedup_key(
    host: str, platform: str, commands: list[str], username: str, job_id: str, kv_store: "KVStore"
) -> str:
    """
    Register a dedup key for this job. Returns the raw KVStore key for storage in job.meta.

    Args:
        host: Target device host
        platform: Netmiko platform
        commands: Command list
        username: Requesting username
        job_id: Job ID to associate
        kv_store: KVStore connection

    Returns:
        The KVStore key (stored in job.meta for cleanup by worker/reaper)
    """
    if not JOB_DEDUP_ENABLED:
        return ""
    key = _dedup_key(host, platform, commands, username)
    kv_store.set(key, job_id, ex=_DEDUP_TTL, nx=True)
    return key


def clear_dedup_key(kv_key: str, kv_store: "KVStore") -> None:
    """
    Delete a dedup key. Called by worker on job completion/failure.

    Args:
        kv_key: The KVStore key stored in job.meta["dedup_key"]
        kv_store: KVStore connection
    """
    if kv_key:
        kv_store.delete(kv_key)
