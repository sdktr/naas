# API Resources

import time

from flask import current_app
from flask_restful import Resource
from spectree import Response

from naas import __version__
from naas.library.nats_queue import BackendUnavailableError as BackendError
from naas.library.nats_queue import FailedJobRegistry
from naas.library.worker_cache import get_cached_workers
from naas.models import HealthCheckResponse
from naas.spec import spec

_START_TIME = time.time()


class HealthCheck(Resource):
    @spec.validate(resp=Response(HTTP_200=HealthCheckResponse))
    def get(self):
        """Return detailed health status including component checks.

        Returns:
            dict: Health status with the following structure:
                {
                    "status": str,  # "healthy", "degraded", or "no_workers"
                    "version": str,  # NAAS version
                    "uptime_seconds": int,  # Seconds since API start
                    "components": {
                        "kv_store": {"status": str},  # "healthy" or "unhealthy"
                        "queue": {"status": str, "depth": int},  # Queue status and job count
                        "workers": {
                            "status": str,  # "healthy" or "no_workers"
                            "count": int,  # Number of worker pods/hosts
                            "active_jobs": int  # Jobs currently processing
                        }
                    }
                }
        """
        kv_store = current_app.config["kv_store"]
        q = current_app.config["q"]

        # Check KVStore connectivity
        try:
            kv_store.ping()
            kv_status = "healthy"
        except BackendError:
            kv_status = "unhealthy"

        # Check workers — count unique hostnames (pods/hosts), not individual processes
        workers = get_cached_workers(kv_store) if kv_status == "healthy" else []
        worker_count = len({w.hostname for w in workers})
        active_jobs = sum(1 for w in workers if w.get_current_job() is not None)
        worker_status = "healthy" if worker_count > 0 else "no_workers"

        if kv_status != "healthy":
            overall = "degraded"
        elif worker_count == 0:
            overall = "no_workers"
        else:
            overall = "healthy"

        return {
            "status": overall,
            "version": __version__,
            "uptime_seconds": int(time.time() - _START_TIME),
            "components": {
                "kv_store": {"status": kv_status},
                "queue": {"status": "healthy", "depth": len(q)},
                "workers": {"status": worker_status, "count": worker_count, "active_jobs": active_jobs},
                "failed_jobs": len(FailedJobRegistry(connection=kv_store)) if kv_status == "healthy" else 0,
            },
        }
