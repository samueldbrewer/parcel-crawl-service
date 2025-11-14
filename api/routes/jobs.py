from __future__ import annotations
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from api import models
from api.services import workers
from worker.run_job import JOB_STORAGE, build_output_snapshot, read_log_tail

router = APIRouter()

# naive in-memory job store for the milestone
JOBS: dict[str, models.JobRecord] = {}


@router.post("/", response_model=models.JobStatus)
async def create_job(payload: models.JobCreate) -> models.JobStatus:
    return await _create_job(payload)


@router.post("", response_model=models.JobStatus)
async def create_job_no_slash(payload: models.JobCreate) -> models.JobStatus:
    return await _create_job(payload)


async def _create_job(payload: models.JobCreate) -> models.JobStatus:
    job_id = uuid4().hex
    config = dict(payload.config or {})
    if payload.footprint_points:
        config["footprint_points"] = payload.footprint_points
    if payload.front_direction:
        config["front_direction"] = payload.front_direction

    record = models.JobRecord(
        id=job_id,
        status="queued",
        created_at=datetime.utcnow(),
        address=payload.address,
        dxf_url=payload.dxf_url,
        config=config,
        footprint_points=payload.footprint_points,
        front_direction=payload.front_direction,
    )
    JOBS[job_id] = record
    workers.enqueue(record)
    return models.JobStatus(id=job_id, status=record.status)


@router.get("/{job_id}", response_model=models.JobRecord)
async def read_job(job_id: str) -> models.JobRecord:
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/{job_id}/logs")
async def read_job_logs(job_id: str, lines: int = 200) -> dict[str, object]:
    log_path = JOB_STORAGE / job_id / "crawl.log"
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log not available yet.")
    limit = max(1, min(lines, 2000))
    return {
        "job_id": job_id,
        "lines": limit,
        "log_tail": read_log_tail(log_path, limit),
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/{job_id}/artifacts")
async def read_job_artifacts(job_id: str) -> dict[str, object]:
    workspace = JOB_STORAGE / job_id
    output_dir = workspace / "outputs"
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="Artifacts not available yet.")
    snapshot = build_output_snapshot(output_dir)
    return {
        "job_id": job_id,
        "artifacts": snapshot.get("artifacts", {}),
        "cycle_summaries": snapshot.get("cycle_summaries", []),
        "output_dir": snapshot.get("output_dir"),
    }


def update_job_status(
    job_id: str,
    status: str,
    *,
    result_url: str | None = None,
    error: str | None = None,
    result: dict[str, Any] | None = None,
) -> None:
    job = JOBS.get(job_id)
    if not job:
        return
    job.status = status
    if result_url:
        job.result_url = result_url  # type: ignore[assignment]
    if error:
        job.error = error
    if result is not None:
        job.result = result
