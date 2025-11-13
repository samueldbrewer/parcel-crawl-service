from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from uuid import uuid4
import logging

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from starlette.requests import Request

from api import models

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

    file_url = f"file://{quote(str(destination), safe='/')}"
    download_url = str(request.url_for("download_uploaded_file", filename=destination.name))

    response = models.FileUploadResponse(
        filename=destination.name,
        stored_path=str(destination),
        file_url=file_url,
        download_url=download_url,
    )
    _log_upload_complete(request, destination, response)
    return response


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


def describe_upload_target() -> dict[str, object]:
    exists = UPLOAD_ROOT.exists()
    writable = os.access(UPLOAD_ROOT, os.W_OK) if exists else False
    return {
        "path": str(UPLOAD_ROOT),
        "exists": exists,
        "writable": writable,
    }


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
        "Upload completed from %s | stored=%s | file_url=%s",
        client,
        destination,
        payload.file_url,
    )
