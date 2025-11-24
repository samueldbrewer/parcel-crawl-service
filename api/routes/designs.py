from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException

import os

DESIGN_ROOT = Path(os.getenv("DESIGN_STORAGE_ROOT", "/data/designs"))
DESIGN_ROOT.mkdir(parents=True, exist_ok=True)

router = APIRouter()


def _slugify(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", name.strip()).strip("-")
    return slug or datetime.utcnow().strftime("%Y%m%d%H%M%S")


@router.get("/", response_model=list[dict[str, object]])
async def list_designs() -> list[dict[str, object]]:
    designs: list[dict[str, object]] = []
    for path in sorted(DESIGN_ROOT.glob("*.json")):
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            continue
        payload["slug"] = path.stem
        designs.append(payload)
    return designs


@router.get("", response_model=list[dict[str, object]])
async def list_designs_no_slash() -> list[dict[str, object]]:
    return await list_designs()


@router.post("/", response_model=dict[str, object])
async def save_design(payload: dict[str, object]) -> dict[str, object]:
    name = (payload.get("name") or "").strip()
    dxf_url = payload.get("dxf_url")
    footprint = payload.get("footprint_points")
    front = payload.get("front_direction")
    if not name:
        raise HTTPException(status_code=400, detail="Design name is required.")
    if not dxf_url:
        raise HTTPException(status_code=400, detail="dxf_url is required.")
    if not footprint or not isinstance(footprint, list):
        raise HTTPException(status_code=400, detail="footprint_points are required.")
    if not front:
        raise HTTPException(status_code=400, detail="front_direction is required.")

    slug = _slugify(name)
    record = {
        "name": name,
        "slug": slug,
        "dxf_url": dxf_url,
        "footprint_points": footprint,
        "front_direction": front,
        "saved_at": datetime.utcnow().isoformat() + "Z",
    }
    target = DESIGN_ROOT / f"{slug}.json"
    target.write_text(json.dumps(record, indent=2))
    return record


@router.post("", response_model=dict[str, object])
async def save_design_no_slash(payload: dict[str, object]) -> dict[str, object]:
    return await save_design(payload)


@router.get("/{slug}", response_model=dict[str, object])
async def read_design(slug: str) -> dict[str, object]:
    target = DESIGN_ROOT / f"{slug}.json"
    if not target.exists():
        raise HTTPException(status_code=404, detail="Design not found.")
    try:
        return json.loads(target.read_text())
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Design file is corrupted.")
