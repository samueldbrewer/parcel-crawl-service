from __future__ import annotations

import logging
import os
import re
import shutil
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote
from uuid import uuid4
from zipfile import BadZipFile, ZipFile

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from starlette.requests import Request

from api import models
from parcel_crawl_demo_v4 import prepare_footprint

UPLOAD_ROOT = Path(os.getenv("DXF_UPLOAD_ROOT", "/data")).resolve()
router = APIRouter()


def _ensure_upload_dir() -> None:
    try:
        UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        raise HTTPException(status_code=500, detail=f"Upload directory not writable: {UPLOAD_ROOT}") from exc


def _sanitize_filename(filename: Optional[str]) -> str:
    candidate = (filename or "").strip()
    if not candidate:
        candidate = f"{uuid4().hex}.dxf"
    candidate = Path(candidate).name  # strip directories
    candidate = re.sub(r"[^A-Za-z0-9._-]", "_", candidate)
    if not candidate:
        candidate = f"{uuid4().hex}.dxf"
    return candidate


def _reserve_path(name: str) -> Path:
    target = UPLOAD_ROOT / name
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix or ".dxf"
    counter = 1
    while True:
        candidate = UPLOAD_ROOT / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _build_file_url(path: Path) -> str:
    return f"file://{quote(str(path), safe='/')}"


def _build_download_url(request: Request, path: Path) -> str:
    return str(request.url_for("download_uploaded_file", filename=path.name))


@router.post("/", response_model=models.FileUploadResponse)
async def upload_dxf(
    request: Request,
    file: UploadFile = File(...),
    filename: Optional[str] = None,
) -> models.FileUploadResponse:
    return await _handle_upload(request, file, filename)


@router.post("", response_model=models.FileUploadResponse)
async def upload_dxf_no_slash(
    request: Request,
    file: UploadFile = File(...),
    filename: Optional[str] = None,
) -> models.FileUploadResponse:
    return await _handle_upload(request, file, filename)


async def _handle_upload(request: Request, file: UploadFile, filename: Optional[str]) -> models.FileUploadResponse:
    _log_upload_start(request, file)
    _ensure_upload_dir()
    desired_name = filename or file.filename
    safe_name = _sanitize_filename(desired_name)
    destination = _reserve_path(safe_name)

    try:
        with destination.open("wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)
    except PermissionError as exc:
        raise HTTPException(status_code=500, detail="Failed to write uploaded file.") from exc
    finally:
        await file.close()

    file_url = _build_file_url(destination)
    download_url = _build_download_url(request, destination)
    extracted = _maybe_extract_archive(destination, request)

    response = models.FileUploadResponse(
        filename=destination.name,
        stored_path=str(destination),
        file_url=file_url,
        download_url=download_url,
        extracted_files=extracted,
    )
    _log_upload_complete(request, destination, response)
    return response


@router.get("/", response_model=List[models.FileArtifact])
async def list_uploaded_files(request: Request) -> List[models.FileArtifact]:
    _ensure_upload_dir()
    artifacts: List[models.FileArtifact] = []
    for entry in sorted(UPLOAD_ROOT.iterdir()):
        if entry.is_file():
            artifacts.append(
                models.FileArtifact(
                    filename=entry.name,
                    stored_path=str(entry),
                    file_url=_build_file_url(entry),
                    download_url=_build_download_url(request, entry),
                )
            )
    return artifacts


@router.get("", response_model=List[models.FileArtifact])
async def list_uploaded_files_no_slash(request: Request) -> List[models.FileArtifact]:
    return await list_uploaded_files(request)


@router.get("/{filename}", response_class=FileResponse, name="download_uploaded_file")
async def download_uploaded_file(filename: str) -> FileResponse:
    target = (UPLOAD_ROOT / filename).resolve()
    try:
        target.relative_to(UPLOAD_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="File not found.") from exc
    if not target.exists():
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(
        target,
        filename=target.name,
        media_type="application/octet-stream",
    )


@router.get("/{filename}/preview")
async def preview_footprint(filename: str) -> dict[str, object]:
    target = (UPLOAD_ROOT / filename).resolve()
    try:
        target.relative_to(UPLOAD_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="File not found.") from exc
    if not target.exists():
        raise HTTPException(status_code=404, detail="File not found.")
    try:
        profile, front_vec = prepare_footprint(
            target,
            auto_front=True,
            front_angle=None,
            front_vector_override=None,
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"Failed to prepare footprint: {exc}") from exc
    coords = list(profile.geometry.exterior.coords)
    if coords and coords[0] == coords[-1]:
        coords = coords[:-1]
    footprint_points = [[round(float(x), 3), round(float(y), 3)] for x, y in coords]
    centroid = profile.geometry.centroid
    if front_vec is None:
        front_vec = (1.0, 0.0)
    return {
        "footprint_points": footprint_points,
        "front_direction": [round(float(front_vec[0]), 4), round(float(front_vec[1]), 4)],
        "front_origin": [round(float(centroid.x), 3), round(float(centroid.y), 3)],
        "area": round(float(profile.area), 3),
    }


def describe_upload_target() -> dict[str, object]:
    exists = UPLOAD_ROOT.exists()
    writable = os.access(UPLOAD_ROOT, os.W_OK) if exists else False
    return {
        "path": str(UPLOAD_ROOT),
        "exists": exists,
        "writable": writable,
    }


def _maybe_extract_archive(path: Path, request: Request) -> List[models.FileArtifact]:
    suffix = path.suffix.lower()
    artifacts: List[models.FileArtifact] = []
    if suffix != ".zip":
        return artifacts

    try:
        with ZipFile(path) as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                member_name = _sanitize_filename(member.filename)
                target = _reserve_path(member_name)
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, target.open("wb") as destination:
                    shutil.copyfileobj(source, destination)
                artifacts.append(
                    models.FileArtifact(
                        filename=target.name,
                        stored_path=str(target),
                        file_url=_build_file_url(target),
                        download_url=_build_download_url(request, target),
                    )
                )
    except BadZipFile as exc:
        logging.warning("Failed to extract zip %s: %s", path, exc)

    return artifacts


def _log_upload_start(request: Request, file: UploadFile) -> None:
    client = request.client.host if request.client else "unknown"
    headers = dict(request.headers)
    logging.info(
        "Upload started from %s | filename=%s | content_length=%s",
        client,
        file.filename,
        headers.get("content-length"),
    )


def _log_upload_complete(request: Request, destination: Path, payload: models.FileUploadResponse) -> None:
    client = request.client.host if request.client else "unknown"
    logging.info(
        "Upload completed from %s | stored=%s | file_url=%s | extracted=%d",
        client,
        destination,
        payload.file_url,
        len(payload.extracted_files),
    )
