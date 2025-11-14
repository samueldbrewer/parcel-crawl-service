from __future__ import annotations
from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException

from api import models
from api.services import workers

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
