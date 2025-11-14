from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from worker.run_job import JOB_STORAGE

router = APIRouter()


@router.get("/{job_id}/files/{full_path:path}", response_class=FileResponse)
async def proxy_job_file(job_id: str, full_path: str) -> FileResponse:
    base_dir = JOB_STORAGE / job_id
    target = base_dir / full_path
    try:
        target = target.resolve(strict=True)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Artifact not found")

    try:
        target.relative_to(base_dir)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Path outside workspace") from exc

    if not target.exists():
        raise HTTPException(status_code=404, detail="Artifact not found")

    return FileResponse(target, filename=target.name)
