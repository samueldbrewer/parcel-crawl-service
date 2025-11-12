from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from uuid import uuid4

from fastapi import APIRouter, File, HTTPException, UploadFile

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
async def upload_dxf(file: UploadFile = File(...), filename: Optional[str] = None) -> models.FileUploadResponse:
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
    return models.FileUploadResponse(
        filename=destination.name,
        stored_path=str(destination),
        file_url=file_url,
    )


def describe_upload_target() -> dict[str, object]:
    exists = UPLOAD_ROOT.exists()
    writable = os.access(UPLOAD_ROOT, os.W_OK) if exists else False
    return {
        "path": str(UPLOAD_ROOT),
        "exists": exists,
        "writable": writable,
    }
