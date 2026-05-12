from naas.library.nats_queue import (
    Queue,
    RedisLikeKV,
    Worker,
    register_worker_heartbeat,
    set_job_result,
    set_job_started,
)


def _demo_task(value: str):
    return {"echo": value}


def test_queue_enqueue_and_job_lifecycle():
    q = Queue("naas-default", connection=RedisLikeKV())
    job = q.enqueue(_demo_task, value="ok", job_id="job-1", meta={"hash": "owner-hash"})

    assert job.id == "job-1"
    assert "job-1" in q.job_ids

    fetched = q.fetch_job("job-1")
    assert fetched.get_status().value == "queued"

    set_job_started("job-1", "worker-1")
    set_job_result("job-1", result={"done": True})

    fetched = q.fetch_job("job-1")
    assert fetched.get_status().value == "finished"
    assert fetched.result == {"done": True}


def test_worker_registry_heartbeat_visibility():
    register_worker_heartbeat("naas-test-worker", ["naas-default"])  # nosec B106
    workers = Worker.all(connection=RedisLikeKV())
    names = [w.name for w in workers]
    assert "naas-test-worker" in names
