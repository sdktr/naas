#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""NATS JetStream worker launcher using nats-py-worker."""

from __future__ import annotations

import json
import os
import signal
from argparse import ArgumentParser, Namespace
from logging import basicConfig, getLogger
from socket import gethostname
from threading import Event, Thread
from time import sleep

from nats_worker import Worker as NATSWorker

from naas.config import WORKER_CONTEXTS
from naas.library.nats_queue import (
    Job,
    KVStore,
    NoSuchJobError,
    register_worker_heartbeat,
    set_job_result,
    set_job_started,
    task_subject_pattern,
)
from naas.library.netmiko_lib import netmiko_send_command, netmiko_send_command_structured, netmiko_send_config

logger = getLogger("naas_worker")
WORKER_HEARTBEAT_INTERVAL = int(os.environ.get("WORKER_HEARTBEAT_INTERVAL", "10"))
WORKER_ACK_WAIT_SECONDS = int(os.environ.get("WORKER_ACK_WAIT_SECONDS", "60"))


def _authorize_before_execution(job: Job, payload_meta: dict) -> bool:
    """AuthZ check closest to execution time on worker side."""
    owner_hash = payload_meta.get("hash") or job.meta.get("hash")
    if not owner_hash:
        logger.warning("%s: missing owner hash, rejecting execution", job.id)
        return False
    if job.meta.get("hash") and owner_hash != job.meta.get("hash"):
        logger.warning("%s: owner hash mismatch, rejecting execution", job.id)
        return False
    return True


def _dispatch_function(func_name: str):
    mapping = {
        "naas.library.netmiko_lib.netmiko_send_command": netmiko_send_command,
        "naas.library.netmiko_lib.netmiko_send_config": netmiko_send_config,
        "naas.library.netmiko_lib.netmiko_send_command_structured": netmiko_send_command_structured,
    }
    return mapping.get(func_name)


def _heartbeat_loop(stop_event: Event, worker_name: str, queues: list[str]) -> None:
    while not stop_event.is_set():
        register_worker_heartbeat(worker_name, queues)
        stop_event.wait(WORKER_HEARTBEAT_INTERVAL)


def run_worker(name: str, queues: list[str], nats_servers: str) -> None:
    basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="[%(asctime)s] [%(process)d] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %z",
    )

    stop_event = Event()
    heartbeat = Thread(target=_heartbeat_loop, args=(stop_event, name, queues), daemon=True)
    heartbeat.start()

    worker = NATSWorker(name=name, servers=nats_servers)

    @worker.background_consumer(
        name="tasks",
        subject=task_subject_pattern(),
        batch_size=1,
        ack_wait=WORKER_ACK_WAIT_SECONDS,
    )
    async def _consume(msg, **kwargs):
        _ = kwargs
        payload = json.loads(msg.data.decode())
        job_id = payload.get("job_id", "")
        func_name = payload.get("func", "")
        task_func = _dispatch_function(func_name)
        if task_func is None:
            set_job_result(job_id, error=f"Unsupported task function: {func_name}")
            await msg.ack()
            return

        try:
            job = Job.fetch(job_id, connection=KVStore())
        except NoSuchJobError:
            await msg.ack()
            return

        if not _authorize_before_execution(job, payload.get("meta", {})):
            set_job_result(job_id, error="Unauthorized at execution time")
            await msg.ack()
            return

        set_job_started(job_id, name)
        register_worker_heartbeat(name, queues, current_job_id=job_id)

        try:
            result = task_func(**job.kwargs)
            set_job_result(job_id, result=result)
        except Exception as exc:  # pragma: no cover
            set_job_result(job_id, error=str(exc))
        finally:
            register_worker_heartbeat(name, queues)
            await msg.ack()

    def _stop(signum, frame):
        _ = signum, frame
        stop_event.set()

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    worker.start_as_app()


def arg_parsing() -> Namespace:
    argparser = ArgumentParser(description="NATS JetStream worker launcher")
    argparser.add_argument(
        "workers",
        type=int,
        nargs="?",
        default=1,
        help="Requested worker process count (nats-py-worker currently supports one process per container)",
    )
    argparser.add_argument(
        "-q",
        "--queues",
        type=str,
        nargs="+",
        default=list(WORKER_CONTEXTS),
        help=f"Queue(s) to watch. Default from WORKER_CONTEXTS ({WORKER_CONTEXTS})",
    )
    argparser.add_argument(
        "-n",
        "--nats-servers",
        type=str,
        default=os.environ.get("NATS_SERVERS", "nats://nats:4222"),
        help="Comma-separated NATS server URLs",
    )
    argparser.add_argument("-s", "--sleep", type=int, default=3, nargs="?", help="Startup delay in seconds")
    return argparser.parse_args()


def main() -> None:
    args = arg_parsing()
    sleep(args.sleep)

    hostname = gethostname()
    worker_name = f"naas_{hostname}_1"
    if args.workers > 1:
        logger.warning(
            "nats-py-worker mode currently runs one process per container; requested workers=%s, starting one worker",
            args.workers,
        )
    logger.info("Starting worker %s for queues=%s", worker_name, args.queues)
    run_worker(worker_name, args.queues, args.nats_servers)


if __name__ == "__main__":
    main()
