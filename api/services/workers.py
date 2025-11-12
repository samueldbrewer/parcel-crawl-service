from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from api.models import JobRecord
from worker.run_job import JobExecutionError, run_job

LOG = logging.getLogger(__name__)
POOL = ThreadPoolExecutor(max_workers=int(os.getenv("API_JOB_WORKERS", "2")))


def enqueue(job: JobRecord) -> None:
    LOG.info("Queueing job %s", job.id)
    POOL.submit(_process_job, job)


def _process_job(job: JobRecord) -> None:
    from api.routes import jobs as job_routes  # avoid circular dependency at import time

    job_routes.update_job_status(job.id, "running")
    payload = job.model_dump()
    try:
        result = run_job(payload)
    except JobExecutionError as exc:
        error_message = _format_error(exc)
        LOG.exception("Job %s failed: %s", job.id, error_message)
        job_routes.update_job_status(job.id, "failed", error=error_message)
    except Exception as exc:  # noqa: BLE001
        LOG.exception("Job %s crashed: %s", job.id, exc)
        job_routes.update_job_status(job.id, "failed", error=str(exc))
    else:
        LOG.info("Job %s completed successfully", job.id)
        manifest = result.get("manifest_path")
        job_routes.update_job_status(job.id, "completed", result=result, result_url=manifest)


def _format_error(exc: JobExecutionError) -> str:
    if not exc.context:
        return str(exc)
    try:
        context = json.dumps(exc.context, ensure_ascii=False)
    except TypeError:
        context = str(exc.context)
    return f"{exc} | context={context}"
