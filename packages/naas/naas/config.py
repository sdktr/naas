#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
config.py
Author: Brett Lykins (lykinsbd@gmail.com)
Description: Configure NAAS API
"""

import os
import random
import string

from naas.library.nats_queue import KVStore, Queue, configure_nats

# Cert/Key File Locations
CERT_KEY_FILE = "/tmp/key.pem"
CERT_FILE = "/tmp/cert.pem"
CERT_BUNDLE_FILE = "/tmp/bundle.crt"

# NATS config
NATS_SERVERS = os.environ.get("NATS_SERVERS", "nats://nats:4222")

# Job TTL config (seconds)
JOB_TTL_SUCCESS = int(os.environ.get("JOB_TTL_SUCCESS", 86400))  # 24h
JOB_TTL_FAILED = int(os.environ.get("JOB_TTL_FAILED", 604800))  # 7 days
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", 120))  # 2 minutes; covers delay_factor=1 + buffer

# Circuit breaker config
CIRCUIT_BREAKER_ENABLED = os.environ.get("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"
CIRCUIT_BREAKER_THRESHOLD = int(os.environ.get("CIRCUIT_BREAKER_THRESHOLD", 5))
CIRCUIT_BREAKER_TIMEOUT = int(os.environ.get("CIRCUIT_BREAKER_TIMEOUT", 300))  # 5 minutes

# Graceful shutdown config (seconds)
SHUTDOWN_TIMEOUT = int(os.environ.get("SHUTDOWN_TIMEOUT", 30))  # 30s

# Connection pool config
CONNECTION_POOL_ENABLED = os.environ.get("CONNECTION_POOL_ENABLED", "true").lower() == "true"
CONNECTION_POOL_MAX_SIZE = int(os.environ.get("CONNECTION_POOL_MAX_SIZE", 10))
CONNECTION_POOL_IDLE_TIMEOUT = int(os.environ.get("CONNECTION_POOL_IDLE_TIMEOUT", 300))  # 5 minutes
CONNECTION_POOL_MAX_AGE = int(os.environ.get("CONNECTION_POOL_MAX_AGE", 3600))  # 1 hour
CONNECTION_POOL_KEEPALIVE = int(os.environ.get("CONNECTION_POOL_KEEPALIVE", 60))  # seconds
CONNECTION_POOL_EXCLUDE: frozenset[str] = frozenset(
    e.strip() for e in os.environ.get("CONNECTION_POOL_EXCLUDE", "").split(",") if e.strip()
)

# Context routing config
NAAS_CONTEXTS: frozenset[str] = frozenset(
    c.strip() for c in os.environ.get("NAAS_CONTEXTS", "default").split(",") if c.strip()
)
WORKER_CONTEXTS: list[str] = [c.strip() for c in os.environ.get("WORKER_CONTEXTS", "default").split(",") if c.strip()]

# Queue depth limit (0 = disabled)
MAX_QUEUE_DEPTH: int = int(os.environ.get("MAX_QUEUE_DEPTH", 0))

# Idempotency key TTL in seconds (24h default)
IDEMPOTENCY_TTL: int = int(os.environ.get("IDEMPOTENCY_TTL", 86400))

# Job deduplication (enabled by default)
JOB_DEDUP_ENABLED: bool = os.environ.get("JOB_DEDUP_ENABLED", "true").lower() == "true"

# Dead letter queue
FAILED_JOB_MAX_RETAIN: int = int(os.environ.get("FAILED_JOB_MAX_RETAIN", 500))

# Webhook config
WEBHOOK_ALLOW_HTTP: bool = os.environ.get("WEBHOOK_ALLOW_HTTP", "false").lower() == "true"
WEBHOOK_MAX_RETRIES: int = int(os.environ.get("WEBHOOK_MAX_RETRIES", "4"))
WEBHOOK_TIMEOUT: int = int(os.environ.get("WEBHOOK_TIMEOUT", "10"))

# Job reaper config
JOB_REAPER_ENABLED: bool = os.environ.get("JOB_REAPER_ENABLED", "true").lower() == "true"
JOB_REAPER_INTERVAL: int = int(os.environ.get("JOB_REAPER_INTERVAL", 60))
WORKER_STALE_THRESHOLD: int = int(os.environ.get("WORKER_STALE_THRESHOLD", 120))

# API key config
API_KEY_DEFAULT_TTL: int = int(os.environ.get("API_KEY_DEFAULT_TTL", 7776000))  # 90 days
API_KEY_MAX_TTL: int = int(os.environ.get("API_KEY_MAX_TTL", 0))  # 0 = unlimited
NAAS_ADMIN_SECRET: str = os.environ.get("NAAS_ADMIN_SECRET", "")
CREDENTIAL_ENCRYPTION_ENABLED: bool = os.environ.get("CREDENTIAL_ENCRYPTION_ENABLED", "true").lower() == "true"


def app_configure(app):
    # Configure our environment
    app_environment = os.environ.get("APP_ENVIRONMENT", "dev")

    # Default set the env to "dev" if something invalid is specified
    if app_environment.lower() not in ["dev", "staging", "production"]:
        app_environment = "dev"

    app.config["APP_ENVIRONMENT"] = app_environment

    # Disable flask debugger
    app.config["DEBUG"] = False

    if "dev" in app.config["APP_ENVIRONMENT"]:
        app.config["LOG_LEVEL"] = os.environ.get("LOG_LEVEL", "DEBUG")
    else:
        app.config["LOG_LEVEL"] = os.environ.get("LOG_LEVEL", "INFO")

    # Push our log level up to an environment variable.
    os.environ["LOG_LEVEL"] = app.config["LOG_LEVEL"]

    # Configure environment specific variables
    if (
        app.config["APP_ENVIRONMENT"].lower() == "dev"
        or app.config["APP_ENVIRONMENT"].lower() == "staging"
        or app.config["APP_ENVIRONMENT"].lower() == "production"
    ):
        # Today we're not differentiating on environment...
        pass

    # Turn off JSON Key sorting
    app.config["JSON_SORT_KEYS"] = False

    # Initialize NATS transport config and a shared KV state store used by auth/idempotency helpers
    configure_nats(servers=NATS_SERVERS)
    kv_store = KVStore()
    kv_store.ping()
    app.config["kv_store"] = kv_store

    # Create a random string to use as a Salt for the UN/PW hashes, stash it in KV store.
    # Use setnx so the salt persists across API restarts — overwriting it would invalidate
    # all connection pool keys and in-flight job auth checks.
    kv_store.setnx("naas_cred_salt", "".join(random.choice(string.ascii_lowercase) for _ in range(10)))

    # Initialize default queue facade
    q = Queue("default", connection=kv_store)
    app.config["q"] = q

    # Initialize secrets backend
    from naas.library.secrets import get_secrets_backend

    app.config["secrets"] = get_secrets_backend()

    # Warn if credential encryption is disabled
    if not CREDENTIAL_ENCRYPTION_ENABLED:
        import logging

        logging.getLogger("NAAS").warning(
            "CREDENTIAL_ENCRYPTION_ENABLED=false — credentials stored in plaintext in job payloads"
        )
