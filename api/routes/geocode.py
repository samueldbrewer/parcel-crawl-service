from __future__ import annotations

import logging
from typing import Optional

import requests
from fastapi import APIRouter, HTTPException

LOG = logging.getLogger(__name__)
router = APIRouter()

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
HEADERS = {"User-Agent": "landlens/1.0"}


@router.get("/reverse")
async def reverse_geocode(lat: float, lon: float) -> dict[str, Optional[str]]:
    """Reverse geocode lat/lon to a human-readable address."""
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"format": "json", "lat": lat, "lon": lon, "zoom": 18, "addressdetails": 1},
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "label": data.get("display_name"),
            "lat": str(lat),
            "lon": str(lon),
        }
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Reverse geocode failed for %s,%s: %s", lat, lon, exc)
        raise HTTPException(status_code=502, detail="Reverse geocoding failed.") from exc
