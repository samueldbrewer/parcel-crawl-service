from __future__ import annotations
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from starlette.routing import NoMatchFound

from api import models
from api.services import workers
from worker.run_job import JOB_STORAGE, build_output_snapshot, read_log_tail, LOG_TAIL_LINES

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


@router.post("/{job_id}/cancel")
async def cancel_job(job_id: str, payload: models.JobCancelRequest | None = None) -> dict[str, object]:
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status in {"completed", "failed", "cancelled"}:
        return {
            "cancelled": False,
            "was_running": False,
            "status": job.status,
            "message": f"Job already {job.status}.",
        }

    reason = (payload.reason if payload and payload.reason else "Job cancelled via API.").strip()
    result = workers.cancel(job_id)

    if not result["was_running"]:
        job.status = "cancelled"
        job.error = reason
        job.result = None
        workers.cleanup(job_id)
        message = "Job cancelled before start."
    else:
        job.status = "cancelling"
        job.error = reason
        message = "Cancellation requested; worker will stop when safe."

    return {
        "cancelled": True,
        "was_running": result["was_running"],
        "status": job.status,
        "message": message,
    }


@router.get("/{job_id}", response_model=models.JobRecord)
async def read_job(job_id: str) -> models.JobRecord:
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    log_path = JOB_STORAGE / job_id / "crawl.log"
    log_exists = log_path.exists()
    job.log_available = log_exists
    job.log_tail = read_log_tail(log_path, LOG_TAIL_LINES) if log_exists else ""
    return job


@router.get("/", response_model=list[models.JobRecord])
async def list_jobs() -> list[models.JobRecord]:
    return list(JOBS.values())


@router.get("", response_model=list[models.JobRecord])
async def list_jobs_no_slash() -> list[models.JobRecord]:
    return list(JOBS.values())


@router.get("/{job_id}/logs")
async def read_job_logs(job_id: str, lines: int = 200) -> dict[str, object]:
    log_path = JOB_STORAGE / job_id / "crawl.log"
    if not log_path.exists():
        return {
            "job_id": job_id,
            "lines": 0,
            "log_tail": "",
            "available": False,
            "detail": "Log not available yet.",
            "updated_at": datetime.utcnow().isoformat() + "Z",
        }
    limit = max(1, min(lines, 2000))
    return {
        "job_id": job_id,
        "lines": limit,
        "log_tail": read_log_tail(log_path, limit),
        "available": True,
        "updated_at": datetime.utcnow().isoformat() + "Z",
    }


@router.get("/{job_id}/artifacts")
async def read_job_artifacts(job_id: str, request: Request) -> dict[str, object]:
    workspace = JOB_STORAGE / job_id
    output_dir = workspace / "outputs"
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="Artifacts not available yet.")
    snapshot = build_output_snapshot(output_dir)
    artifacts = snapshot.get("artifacts", {})
    add_download_urls(job_id, artifacts, request)
    return {
        "job_id": job_id,
        "artifacts": artifacts,
        "cycle_summaries": snapshot.get("cycle_summaries", []),
        "output_dir": snapshot.get("output_dir"),
    }


def add_download_urls(job_id: str, artifacts: dict[str, object], request: Request) -> None:
    """Attach downloadable URLs for known artifact paths."""
    workspace_root = JOB_STORAGE / job_id
    files_root = Path("/data")

    def to_url(path_str: str) -> str | None:
        path = Path(path_str)
        try:
            rel = path.relative_to(workspace_root)
            try:
                return str(request.url_for("proxy_job_file", job_id=job_id, full_path=str(rel)))
            except NoMatchFound:
                return None
        except ValueError:
            pass

        try:
            rel = path.relative_to(files_root)
            try:
                return str(request.url_for("download_uploaded_file", filename=rel.name))
            except NoMatchFound:
                return None
        except ValueError:
            return None

    def inject(obj: object):
        if isinstance(obj, dict):
            for key, value in list(obj.items()):
                if isinstance(value, str) and value.startswith("/"):
                    url = to_url(value)
                    if url:
                        obj[f"{key}_url"] = url
                else:
                    inject(value)
        elif isinstance(obj, list):
            for item in obj:
                inject(item)

    inject(artifacts)


@router.get("/{job_id}/geo")
async def read_job_geo(job_id: str, request: Request) -> dict[str, object]:
    """Return GeoJSON features for parcel best footprints and basic progress."""
    workspace = JOB_STORAGE / job_id
    output_dir = workspace / "outputs"
    job = JOBS.get(job_id)
    if not output_dir.exists():
        return {
            "type": "FeatureCollection",
            "features": [],
            "progress": {
                "completed": 0,
                "total": 0,
                "extra": {
                    "outputs_available": False,
                    "job_status": job.status if job else None,
                    "log_available": (JOB_STORAGE / job_id / "crawl.log").exists(),
                },
            },
        }
    snapshot = build_output_snapshot(output_dir)
    artifacts = snapshot.get("artifacts", {})
    parcels = artifacts.get("parcels") or []
    features: list[dict[str, object]] = []
    total_parcels = len(parcels)
    for parcel in parcels:
        placements_path = Path(parcel.get("placements_json") or "")
        geom = None
        props: dict[str, object] = {"parcel_id": parcel.get("parcel_id")}
        if placements_path.exists():
            try:
                data = json.loads(placements_path.read_text())
                geom = data.get("best_footprint_geojson")
                # include top summary if present
                if data.get("summary"):
                    props.update(data["summary"])
            except json.JSONDecodeError:
                geom = None
        if geom:
            features.append({"type": "Feature", "geometry": geom, "properties": props})
    return {
        "type": "FeatureCollection",
        "features": features,
        "progress": {
            "completed": len(features),
            "total": total_parcels,
            "extra": {
                "outputs_available": True,
                "job_status": job.status if job else None,
                "log_available": (JOB_STORAGE / job_id / "crawl.log").exists(),
            },
        },
    }


@router.get("/{job_id}/events")
async def read_job_events(job_id: str, cursor: int = 0, max_bytes: int = 262144) -> dict[str, object]:
    """Return NDJSON events produced during the crawl starting from a byte cursor."""
    if cursor < 0:
        cursor = 0
    max_bytes = max(1024, min(max_bytes, 1_048_576))
    event_path = JOB_STORAGE / job_id / "outputs" / "events.ndjson"
    if not event_path.exists():
        raise HTTPException(status_code=404, detail="Events not available yet.")

    events: list[dict[str, object]] = []
    new_cursor = cursor
    truncated = False

    try:
        with event_path.open("r", encoding="utf-8") as stream:
            stream.seek(cursor)
            while True:
                pos_before = stream.tell()
                line = stream.readline()
                if not line:
                    break
                pos_after = stream.tell()
                if events and (pos_after - cursor) > max_bytes:
                    stream.seek(pos_before)
                    truncated = True
                    break
                new_cursor = pos_after
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    # Encountered partial line; rewind and return so the client can retry later.
                    stream.seek(pos_before)
                    new_cursor = pos_before
                    truncated = True
                    break
    except OSError as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"Failed to read events: {exc}") from exc

    has_more = truncated or event_path.stat().st_size > new_cursor
    return {
        "cursor": new_cursor,
        "events": events,
        "has_more": has_more,
    }


@router.get("/{job_id}/overlay")
async def read_job_overlay(job_id: str) -> dict[str, object]:
    overlay_path = JOB_STORAGE / job_id / "outputs" / "overlay.json"
    if not overlay_path.exists():
        raise HTTPException(status_code=404, detail="Overlay not available yet.")
    try:
        overlay = json.loads(overlay_path.read_text())
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="Overlay snapshot is corrupted.") from exc

    def _fc(items: Iterable[dict[str, object]]) -> dict[str, object]:
        return {"type": "FeatureCollection", "features": list(items)}

    parcels = overlay.get("parcels", {})
    placements = overlay.get("placements", {})
    best = overlay.get("best", {})
    shadows = overlay.get("shadows", {})

    return {
        "updated_at": overlay.get("updated_at"),
        "parcels": _fc(parcels.values() if isinstance(parcels, dict) else []),
        "placements": _fc(
            feat
            for plist in (placements.values() if isinstance(placements, dict) else [])
            for feat in plist
        ),
        "best": _fc(best.values() if isinstance(best, dict) else []),
        "shadows": _fc(
            feat
            for plist in (shadows.values() if isinstance(shadows, dict) else [])
            for feat in plist
        ),
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
