"""
context.py
Context routing helpers for multi-segment worker environments.
"""

from naas.config import MAX_QUEUE_DEPTH, NAAS_CONTEXTS
from naas.library.errorhandlers import InvalidContext, NoWorkersForContext, QueueFull
from naas.library.nats_queue import Queue, Worker


def get_queue_for_context(context: str, kv_store: object) -> Queue:
    """
    Return the queue for the given context, validating it first.

    Args:
        context: Context name from request
        kv_store: Shared KV store connection

    Returns:
        Queue for the context

    Raises:
        InvalidContext: If context is not in NAAS_CONTEXTS
        NoWorkersForContext: If no active workers serve this context
    """
    if context not in NAAS_CONTEXTS:
        raise InvalidContext

    queue_name = context
    q = Queue(queue_name, connection=kv_store)  # type: ignore[arg-type]

    # Check for active workers serving this context
    active_workers = [w for w in Worker.all(connection=kv_store) if queue_name in w.queue_names()]  # type: ignore[arg-type]
    if not active_workers:
        raise NoWorkersForContext

    if MAX_QUEUE_DEPTH > 0 and len(q) >= MAX_QUEUE_DEPTH:
        raise QueueFull

    return q
