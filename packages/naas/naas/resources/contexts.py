# API Resource for listing active routing contexts

from flask import current_app
from flask_restful import Resource
from spectree import Response

from naas.config import NAAS_CONTEXTS
from naas.library.auth import require_role
from naas.library.nats_queue import Queue, Worker
from naas.models import ContextInfo, ContextsResponse
from naas.spec import spec


class Contexts(Resource):
    @require_role("viewer")
    @spec.validate(resp=Response(HTTP_200=ContextsResponse))
    def get(self):
        """
        List all configured contexts with active worker counts and queue depths.

        :return: ContextsResponse with per-context status
        """
        kv_store = current_app.config["kv_store"]
        all_workers = Worker.all(connection=kv_store)

        contexts = []
        for context_name in sorted(NAAS_CONTEXTS):
            queue_name = context_name
            q = Queue(queue_name, connection=kv_store)
            worker_count = sum(1 for w in all_workers if queue_name in w.queue_names())
            contexts.append(
                ContextInfo(
                    name=context_name,
                    workers=worker_count,
                    queue_depth=len(q),
                ).model_dump()
            )

        return ContextsResponse(contexts=contexts).model_dump(), 200
