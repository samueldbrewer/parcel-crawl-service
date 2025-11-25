from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, Future
from threading import Lock

from api.models import JobRecord
from worker.run_job import JobExecutionError, run_job

LOG = logging.getLogger(__name__)
POOL = ThreadPoolExecutor(max_workers=int(os.getenv("API_JOB_WORKERS", "2")))
_ACTIVE: dict[str, Future] = {}
_CANCELLED: set[str] = set()
_LOCK = Lock()


def enqueue(job: JobRecord) -> None:
    LOG.info("Queueing job %s", job.id)
    future = POOL.submit(_process_job, job)
    with _LOCK:
        _ACTIVE[job.id] = future


def cancel(job_id: str) -> dict[str, bool]:
    with _LOCK:
        future = _ACTIVE.get(job_id)
        _CANCELLED.add(job_id)
        if future and future.cancel():
            _ACTIVE.pop(job_id, None)
            return {"accepted": True, "was_running": False}
        was_running = future is not None
    return {"accepted": True, "was_running": was_running}


def is_cancelled(job_id: str) -> bool:
    with _LOCK:
        return job_id in _CANCELLED


def cleanup(job_id: str, *, drop_cancel_flag: bool = True) -> None:
    with _LOCK:
        _ACTIVE.pop(job_id, None)
        if drop_cancel_flag:
            _CANCELLED.discard(job_id)


def _process_job(job: JobRecord) -> None:
    from api.routes import jobs as job_routes  # avoid circular dependency at import time

    job_id = job.id
    if is_cancelled(job_id):
        LOG.info("Job %s cancelled before start.", job_id)
        job_routes.update_job_status(job_id, "cancelled", error=job.error or "Job cancelled.")
        cleanup(job_id)
        return

    job_routes.update_job_status(job.id, "running")
    payload = job.model_dump()
    try:
        result = run_job(payload, should_cancel=lambda: is_cancelled(job_id))
    except JobExecutionError as exc:
        error_message = _format_error(exc)
        if is_cancelled(job_id):
            LOG.info("Job %s cancelled during execution.", job_id)
            job_routes.update_job_status(job_id, "cancelled", error=job.error or "Job cancelled.")
        else:
            LOG.exception("Job %s failed: %s", job.id, error_message)
            job_routes.update_job_status(job_id, "failed", error=error_message)
    except Exception as exc:  # noqa: BLE001
        if is_cancelled(job_id):
            LOG.info("Job %s cancelled during execution.", job_id)
            job_routes.update_job_status(job_id, "cancelled", error=job.error or "Job cancelled.")
        else:
            LOG.exception("Job %s crashed: %s", job_id, exc)
            job_routes.update_job_status(job_id, "failed", error=str(exc))
    else:
        if is_cancelled(job_id):
            LOG.info("Job %s completed but marked as cancelled; discarding result.", job_id)
            job_routes.update_job_status(job_id, "cancelled", error=job.error or "Job cancelled.")
        else:
            LOG.info("Job %s completed successfully", job_id)
            manifest = result.get("manifest_path")
            job_routes.update_job_status(job_id, "completed", result=result, result_url=manifest)
    finally:
        cleanup(job_id)


def _format_error(exc: JobExecutionError) -> str:
    if not exc.context:
        return str(exc)
    try:
        context = json.dumps(exc.context, ensure_ascii=False)
    except TypeError:
        context = str(exc.context)
    return f"{exc} | context={context}"
