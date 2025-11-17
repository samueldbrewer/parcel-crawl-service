from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, HTTPException

from worker.run_job import JOB_STORAGE, build_output_snapshot

router = APIRouter()


@router.get("/{job_id}/outputs")
async def fetch_job_outputs(job_id: str) -> dict[str, object]:
    """Return the raw outputs snapshot (paths + summaries) to aid debugging/overlays."""
    workspace = JOB_STORAGE / job_id
    output_dir = workspace / "outputs"
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="Outputs not available yet.")
    snapshot = build_output_snapshot(output_dir)
    # Include placements.json contents if present for convenience
    parcels_dir = Path(snapshot["artifacts"].get("parcels_dir") or "")
    sample_payload = None
    if parcels_dir.exists():
        placements = next(parcels_dir.glob("*/placements.json"), None)
        if placements and placements.exists():
            try:
                sample_payload = json.loads(placements.read_text())
            except json.JSONDecodeError:
                sample_payload = None
    if sample_payload:
        snapshot["sample_placements"] = sample_payload
    return snapshot
