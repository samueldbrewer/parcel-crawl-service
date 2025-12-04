#!/usr/bin/env python3
"""Lookup City of Atlanta parcel data for an address and render a parcel map.

Workflow:
    1. Geocode an input address with the ArcGIS World Geocoder.
    2. Project the coordinate to Web Mercator (wkid 102100) used by the parcel layer.
    3. Query the City of Atlanta tax parcel feature service for the parcel that
       contains the address point.
    4. Query surrounding parcels inside a configurable buffer so we can show
       adjacent owners/addresses.
    5. Generate a simple map tile (PNG) with the target parcel highlighted and
       neighboring parcel outlines.

You need network access and the following Python packages installed:
    pip install requests shapely matplotlib numpy pillow

Authenticating against the parcel layer:
    The parcel feature service enforces token-based access. The script will try
    to scrape the public ArcGIS API key from https://gis.atlantaga.gov/propinfo/.
    You can override this by supplying --token or the ATL_ARCGIS_TOKEN
    environment variable. If a new key is issued and scraping fails, provide it
    explicitly.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import urljoin

from io import BytesIO

import matplotlib.pyplot as plt
import requests
import numpy as np
from PIL import Image
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from shapely.geometry import Polygon, shape, mapping
from shapely.geometry.base import BaseGeometry

GEOCODE_URL = "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
PARCEL_SERVICE = "https://services5.arcgis.com/5RxyIIJ9boPdptdo/arcgis/rest/services/coa_tax_parcels/FeatureServer/0"
TOKEN_SOURCES = [
    "https://gis.atlantaga.gov/propinfo/main.js",
    "https://gis.atlantaga.gov/propinfo/app.js",
    "https://gis.atlantaga.gov/propinfo/",
]
PROPINFO_BASE = "https://gis.atlantaga.gov/propinfo/"
DOCUMENT_ARCHIVE_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/DocumentArchive/Layers/MapServer/3"
OFFICIAL_ZONING_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/LandUsePlanning/LandUsePlanning/MapServer/0"
ZONING_OVERLAY_LAYERS = [
    {
        "service": "https://gis.atlantaga.gov/dpcd/rest/services/LandUsePlanning/LandUsePlanning/MapServer/2",
        "name_field": "NAME",
        "description_field": "DESCRIPTION",
        "fields": ["NAME", "DESCRIPTION", "PDF_LINK"],
    },
    {
        "service": "https://gis.atlantaga.gov/dpcd/rest/services/LandUsePlanning/IncentiveZone/MapServer/0",
        "name_field": "ZONEDESC",
        "description_field": "ZONEDESC",
        "fields": ["ZONEDESC", "NAME", "URL"],
    },
]
DEVELOPMENT_PATTERN_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/LandUsePlanning/LandUsePlanning/MapServer/8"
LAND_LOT_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/AdministrativeArea/GeopoliticalArea/MapServer/3"
COUNCIL_DISTRICT_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/AdministrativeArea/GeopoliticalArea/MapServer/1"
NPU_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/AdministrativeArea/GeopoliticalArea/MapServer/2"
NEIGHBORHOOD_LAYER = "https://gis.atlantaga.gov/dpcd/rest/services/AdministrativeArea/GeopoliticalArea/MapServer/4"
DEFAULT_OUT_FIELDS = ",".join(
    [
        "OBJECTID",
        "PARCELID",
        "LOWPARCELID",
        "OWNERNME1",
        "OWNERNME2",
        "SITEADDRESS",
        "SCHLDSCRP",
        "TAXYEAR",
    ]
)

ORIGIN_SHIFT = 20037508.342789244
TILE_SIZE = 256
MAPTILER_TILE_URL = "https://api.maptiler.com/maps/streets/{z}/{x}/{y}.png?key={key}"

HTTP_SESSION = requests.Session()
BASE_HEADERS = {
    "Accept": "application/json",
    "Origin": "https://gis.atlantaga.gov",
    "Referer": "https://gis.atlantaga.gov/propinfo/",
    "User-Agent": "parcel-lookup/1.0",
}


@dataclass
class ParcelFeature:
    """Typed wrapper for parcel feature JSON."""

    object_id: int
    attributes: Dict[str, object]
    geometry: Polygon

    @property
    def address(self) -> str:
        for key in ("SITEADDRESS", "SITE_ADDR", "ADDRESS"):
            value = self.attributes.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return ""

    @property
    def parcel_id(self) -> str:
        for key in ("LOWPARCELID", "PARCELID"):
            value = self.attributes.get(key)
            if isinstance(value, str) and value.strip():
                return value
        return str(self.object_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch parcel data and map neighbors for a given address.")
    parser.add_argument("address", help="Street address to geocode (free-form string).")
    parser.add_argument(
        "--buffer",
        type=float,
        default=75.0,
        help="Neighbor search buffer in meters around the subject parcel bounds (default: %(default)s).",
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.getenv("ATL_ARCGIS_TOKEN"),
        help="ArcGIS token for the parcel feature service. Overrides ATL_ARCGIS_TOKEN env var.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("parcel_map.png"),
        help="Output PNG path for the parcel map (default: %(default)s).",
    )
    parser.add_argument(
        "--max-neighbors",
        type=int,
        default=25,
        help="Upper bound on number of neighbor parcels to fetch (default: %(default)s).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose HTTP logging.",
    )
    parser.add_argument(
        "--maptiler-key",
        type=str,
        default=os.getenv("MAPTILER_KEY", "D7muCTicYZmoW1fGqAk0"),
        help="MapTiler API key for basemap rendering (default: MAPTILER_KEY env var).",
    )
    parser.add_argument(
        "--maptiler-zoom",
        type=int,
        default=18,
        help="MapTiler zoom level for basemap imagery (default: %(default)s).",
    )
    return parser.parse_args()


def setup_logging(debug: bool) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")


def extract_token_from_text(text: str) -> Optional[str]:
    """Locate an ArcGIS API key within raw text."""
    import re

    # Primary explicit markers we have seen in captured assets.
    markers = [
        'token":"',
        "token=\"",
        "arcgisApiKey\":\"",
        "arcgis_apikey\":\"",
    ]
    for marker in markers:
        idx = text.find(marker)
        if idx == -1:
            continue
        start = idx + len(marker)
        end = text.find('"', start)
        if end == -1:
            continue
        candidate = text[start:end]
        if candidate.startswith("AAPK"):
            return candidate

    match = re.search(r"AAPK[0-9A-Za-z_\-]+", text)
    if match:
        return match.group(0)
    return None


def fetch_arcgis_token() -> Optional[str]:
    """Best-effort fetch of the public ArcGIS API key from the PropInfo site."""
    session = requests.Session()
    headers = {
        "User-Agent": "parcel-lookup/1.0",
        "Referer": "https://gis.atlantaga.gov/propinfo/",
        "Accept": "text/html,application/javascript;q=0.9,*/*;q=0.8",
    }
    discovered_sources: List[str] = []
    try:
        resp = session.get(PROPINFO_BASE, headers=headers, timeout=10)
        resp.raise_for_status()
        html = resp.text
        token = extract_token_from_text(html)
        if token:
            logging.info("Discovered ArcGIS token from %s", PROPINFO_BASE)
            return token
        # find script tags
        import re

        for match in re.findall(r'src=["\']([^"\']+)["\']', html):
            url = match.strip()
            if not url:
                continue
            normalized_for_suffix = url.split("?", 1)[0]
            if not normalized_for_suffix.lower().endswith(".js"):
                continue
            if url.startswith("http"):
                full_url = url
            elif url.startswith("//"):
                full_url = f"https:{url}"
            else:
                full_url = urljoin(PROPINFO_BASE, url)
            discovered_sources.append(full_url)
    except requests.RequestException as exc:
        logging.debug("Failed to fetch propinfo landing page: %s", exc)

    candidate_sources = TOKEN_SOURCES + discovered_sources
    seen: set[str] = set()
    for url in candidate_sources:
        if url in seen:
            continue
        seen.add(url)
        try:
            logging.debug("Attempting to resolve ArcGIS token from %s", url)
            resp = session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logging.debug("Request to %s failed: %s", url, exc)
            continue
        token = extract_token_from_text(resp.text)
        if token:
            logging.info("Discovered ArcGIS token from %s", url)
            return token
    logging.warning("Unable to discover ArcGIS API token automatically.")
    return None


def _arcgis_request(
    full_url: str,
    params: Dict[str, Union[str, int, float]],
    token: Optional[str],
    *,
    require_token: bool = False,
) -> Dict[str, object]:
    attempts: List[Optional[str]]
    if require_token:
        if not token:
            raise PermissionError(f"{full_url} requires an ArcGIS token but none was provided.")
        attempts = [token]
    else:
        attempts = []
        if token:
            attempts.append(token)
        attempts.append(None)

    last_error: Optional[str] = None
    for idx, attempt_token in enumerate(attempts):
        payload = dict(params)
        if attempt_token:
            payload["token"] = attempt_token
        logging.debug("ArcGIS request %s payload=%s", full_url, payload)
        response = HTTP_SESSION.post(full_url, data=payload, headers=BASE_HEADERS, timeout=20)
        if response.status_code == 499 and idx < len(attempts) - 1:
            last_error = "Invalid or expired token"
            logging.debug("Token rejected for %s, retrying without token", full_url)
            continue
        response.raise_for_status()
        payload_json = response.json()
        if "error" in payload_json:
            error = payload_json["error"]
            error_code = error.get("code")
            message = error.get("message", "Unknown ArcGIS error")
            details = error.get("details") or []
            if details:
                message = f"{message} Details: {'; '.join(details)}"
            if error_code in (498, 499) and idx < len(attempts) - 1:
                last_error = message
                logging.debug("ArcGIS token error for %s, retrying.", full_url)
                continue
            raise RuntimeError(f"ArcGIS request failed for {full_url}: {message}")
        return payload_json

    if last_error:
        raise RuntimeError(f"ArcGIS request failed for {full_url}: {last_error}")
    raise RuntimeError(f"ArcGIS request failed for {full_url}: unknown error")


def normalize_out_fields(out_fields: Union[str, Sequence[str], None]) -> str:
    if isinstance(out_fields, str):
        return out_fields or "*"
    if out_fields:
        return ",".join(out_fields)
    return "*"


def execute_arcgis_query(
    service_url: str,
    *,
    geometry: Dict[str, object],
    geometry_type: str,
    out_fields: Union[str, Sequence[str], None],
    token: Optional[str],
    where: str = "1=1",
    return_geometry: bool = False,
    result_record_count: int = 100,
    require_token: bool = False,
) -> Dict[str, object]:
    params = {
        "f": "json",
        "where": where,
        "geometryType": geometry_type,
        "inSR": 102100,
        "outSR": 102100,
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": normalize_out_fields(out_fields),
        "returnGeometry": json.dumps(return_geometry).lower(),
        "resultRecordCount": result_record_count,
        "geometry": json.dumps(geometry),
    }
    return _arcgis_request(f"{service_url}/query", params, token, require_token=require_token)


def geocode_address(address: str) -> Dict[str, object]:
    params = {
        "SingleLine": address,
        "maxLocations": 5,
        "outFields": "*",
        "f": "json",
        "outSR": 4326,
    }
    logging.info("Geocoding address: %s", address)
    response = requests.get(GEOCODE_URL, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    candidates: Sequence[Dict[str, object]] = data.get("candidates", [])
    if not candidates:
        raise ValueError(f"No geocoding candidates found for address: {address}")
    candidate = max(candidates, key=lambda x: x.get("score", 0))
    logging.info("Selected candidate '%s' (score=%s)", candidate.get("address"), candidate.get("score"))
    return candidate


def wgs84_to_web_mercator(lon: float, lat: float) -> tuple[float, float]:
    """Project WGS84 coordinates to Web Mercator (EPSG:3857 / WKID 102100)."""
    if abs(lat) > 89.5:  # clamp to avoid projection blow-up
        lat = math.copysign(89.5, lat)
    origin_shift = 2 * math.pi * 6378137 / 2.0
    mx = lon * origin_shift / 180.0
    my = math.log(math.tan((90 + lat) * math.pi / 360.0)) * 6378137
    return mx, my


def web_mercator_to_wgs84(mx: float, my: float) -> tuple[float, float]:
    lon = (mx / ORIGIN_SHIFT) * 180.0
    lat = (my / ORIGIN_SHIFT) * 180.0
    lat = 180.0 / math.pi * (2 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lon, lat


def lonlat_to_tile(lon: float, lat: float, zoom: int) -> tuple[int, int]:
    lat = max(min(lat, 85.05112878), -85.05112878)
    n = 2 ** zoom
    xtile = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + 1 / math.cos(lat_rad)) / math.pi) / 2.0 * n)
    return xtile, ytile


def tile_bounds_webmerc(x: int, y: int, zoom: int) -> tuple[float, float, float, float]:
    n = 2 ** zoom
    lon_min = x / n * 360.0 - 180.0
    lon_max = (x + 1) / n * 360.0 - 180.0
    lat_rad_max = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat_rad_min = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
    lat_max = math.degrees(lat_rad_max)
    lat_min = math.degrees(lat_rad_min)
    minx, miny = wgs84_to_web_mercator(lon_min, lat_min)
    maxx, maxy = wgs84_to_web_mercator(lon_max, lat_max)
    return minx, miny, maxx, maxy


def query_parcels(
    *,
    geometry: Dict[str, object],
    geometry_type: str,
    out_fields: str,
    token: Optional[str],
    where: str = "1=1",
    return_geometry: bool = True,
    result_record_count: int = 100,
) -> Dict[str, object]:
    return execute_arcgis_query(
        PARCEL_SERVICE,
        geometry=geometry,
        geometry_type=geometry_type,
        out_fields=out_fields,
        token=token,
        where=where,
        return_geometry=return_geometry,
        result_record_count=result_record_count,
        require_token=True,
    )


def arcgis_polygon_to_shapely(polygon_json: Dict[str, object]) -> Polygon:
    rings: Iterable[Iterable[Sequence[float]]] = polygon_json.get("rings", [])
    if not rings:
        raise ValueError("Parcel geometry missing rings.")
    exterior = rings[0]
    interiors = list(rings[1:])
    poly = Polygon(exterior, interiors)
    if not poly.is_valid and poly.area > 0:
        poly = poly.buffer(0)
    if not poly.is_valid:
        raise ValueError("Failed to build a valid polygon from parcel geometry.")
    return poly


def to_parcel_features(features: Iterable[Dict[str, object]]) -> List[ParcelFeature]:
    parcels: List[ParcelFeature] = []
    for feature in features:
        geom_json = feature.get("geometry")
        attrs = feature.get("attributes", {})
        if not geom_json:
            continue
        try:
            geom = arcgis_polygon_to_shapely(geom_json)
        except ValueError as exc:
            logging.warning("Skipping feature missing geometry: %s", exc)
            continue
        object_id = int(attrs.get("OBJECTID", len(parcels)))
        parcels.append(ParcelFeature(object_id=object_id, attributes=attrs, geometry=geom))
    return parcels


def pick_primary_parcel(parcels: Sequence[ParcelFeature], point: BaseGeometry) -> ParcelFeature:
    if not parcels:
        raise ValueError("No candidate parcels returned for the address point.")
    intersecting = [p for p in parcels if p.geometry.contains(point) or p.geometry.touches(point)]
    if intersecting:
        parcels = intersecting
    return max(parcels, key=lambda p: p.geometry.area)


def fetch_target_parcel(x_merc: float, y_merc: float, token: Optional[str]) -> ParcelFeature:
    geometry = {
        "x": x_merc,
        "y": y_merc,
        "spatialReference": {"wkid": 102100},
    }
    search_buffers = [0, 5, 15, 30]
    point = shape({"type": "Point", "coordinates": (x_merc, y_merc)})

    for buffer_meters in search_buffers:
        if buffer_meters == 0:
            geom = geometry
            geom_type = "esriGeometryPoint"
        else:
            geom = {
                "xmin": x_merc - buffer_meters,
                "ymin": y_merc - buffer_meters,
                "xmax": x_merc + buffer_meters,
                "ymax": y_merc + buffer_meters,
                "spatialReference": {"wkid": 102100},
            }
            geom_type = "esriGeometryEnvelope"

        try:
            data = query_parcels(
                geometry=geom,
                geometry_type=geom_type,
                out_fields=DEFAULT_OUT_FIELDS,
                token=token,
                result_record_count=25,
            )
        except Exception as exc:  # noqa: BLE001
            logging.debug("Parcel query attempt failed (buffer %s m): %s", buffer_meters, exc)
            continue

        parcels = to_parcel_features(data.get("features", []))
        if parcels:
            try:
                return pick_primary_parcel(parcels, point)
            except ValueError:
                continue

    # identify fallback on parcel service layer 0
    if "MapServer" in PARCEL_SERVICE:
        try:
            identify_results = identify_layer_attributes(
                PARCEL_SERVICE,
                [0],
                x_merc=x_merc,
                y_merc=y_merc,
                token=token,
                max_features=10,
            )
        except Exception as exc:  # noqa: BLE001
            logging.debug("Parcel identify fallback failed: %s", exc)
            identify_results = []

        if identify_results:
            features = []
            for attrs in identify_results:
                geometry_data = attrs.get("__geometry__") or attrs.get("geometry")
                if geometry_data:
                    try:
                        polygon = arcgis_polygon_to_shapely(geometry_data)
                    except Exception:  # noqa: BLE001
                        continue
                    clean_attrs = dict(attrs)
                    clean_attrs.pop("__geometry__", None)
                    clean_attrs.pop("geometry", None)
                    object_id = int(clean_attrs.get("OBJECTID", clean_attrs.get("ObjectId", 0)))
                    features.append(ParcelFeature(object_id=object_id, attributes=clean_attrs, geometry=polygon))
            if features:
                try:
                    return pick_primary_parcel(features, point)
                except ValueError:
                    pass

    raise ValueError("No candidate parcels returned for the address point.")


def fetch_neighbor_parcels(
    target: ParcelFeature,
    buffer_meters: float,
    token: Optional[str],
    max_neighbors: int,
    include_target: bool = True,
) -> List[ParcelFeature]:
    minx, miny, maxx, maxy = target.geometry.bounds
    enlarged = {
        "xmin": minx - buffer_meters,
        "ymin": miny - buffer_meters,
        "xmax": maxx + buffer_meters,
        "ymax": maxy + buffer_meters,
        "spatialReference": {"wkid": 102100},
    }
    data = query_parcels(
        geometry=enlarged,
        geometry_type="esriGeometryEnvelope",
        out_fields=DEFAULT_OUT_FIELDS,
        token=token,
        result_record_count=max_neighbors,
    )
    parcels = to_parcel_features(data.get("features", []))
    if not include_target:
        parcels = [p for p in parcels if p.object_id != target.object_id]
    return parcels


def query_layer_attributes_by_point(
    service_url: str,
    x_merc: float,
    y_merc: float,
    out_fields: Union[str, Sequence[str], None],
    token: Optional[str],
    max_features: int = 10,
    where: str = "1=1",
    envelope_buffer: float = 5.0,
) -> List[Dict[str, object]]:
    geometry = {
        "x": x_merc,
        "y": y_merc,
        "spatialReference": {"wkid": 102100},
    }
    data = execute_arcgis_query(
        service_url,
        geometry=geometry,
        geometry_type="esriGeometryPoint",
        out_fields=out_fields,
        token=token,
        where=where,
        return_geometry=False,
        result_record_count=max_features,
    )
    features = data.get("features", [])
    if not features and envelope_buffer > 0:
        envelope = {
            "xmin": x_merc - envelope_buffer,
            "ymin": y_merc - envelope_buffer,
            "xmax": x_merc + envelope_buffer,
            "ymax": y_merc + envelope_buffer,
            "spatialReference": {"wkid": 102100},
        }
        data = execute_arcgis_query(
            service_url,
            geometry=envelope,
            geometry_type="esriGeometryEnvelope",
            out_fields=out_fields,
            token=token,
            where=where,
            return_geometry=False,
            result_record_count=max_features,
        )
        features = data.get("features", [])
    return [feature.get("attributes", {}) for feature in features]


def split_service_layer(service_url: str) -> Tuple[str, Optional[int]]:
    cleaned = service_url.rstrip("/")
    if not cleaned:
        return service_url, None
    parts = cleaned.rsplit("/", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], int(parts[1])
    return cleaned, None


def identify_layer_attributes(
    service_url: str,
    layer_ids: Sequence[int],
    x_merc: float,
    y_merc: float,
    token: Optional[str],
    max_features: int = 10,
    tolerance: float = 5.0,
) -> List[Dict[str, object]]:
    if not layer_ids:
        return []
    base_url, _ = split_service_layer(service_url)
    geometry = {
        "x": x_merc,
        "y": y_merc,
        "spatialReference": {"wkid": 102100},
    }
    extent = {
        "xmin": x_merc - 25,
        "ymin": y_merc - 25,
        "xmax": x_merc + 25,
        "ymax": y_merc + 25,
        "spatialReference": {"wkid": 102100},
    }
    params = {
        "f": "json",
        "geometry": json.dumps(geometry),
        "geometryType": "esriGeometryPoint",
        "sr": 102100,
        "tolerance": max(int(tolerance), 2),
        "mapExtent": json.dumps(extent),
        "imageDisplay": "800,600,96",
        "returnGeometry": "false",
        "layers": "all:" + ",".join(str(i) for i in layer_ids),
        "maxAllowableOffset": "",
        "time": "",
        "maxRecordCountFactor": "",
    }
    payload = _arcgis_request(f"{base_url}/identify", params, token, require_token=False)
    results = payload.get("results", [])
    filtered: List[Dict[str, object]] = []
    layer_id_set = set(layer_ids)
    for result in results:
        if result.get("layerId") in layer_id_set and "attributes" in result:
            attrs = dict(result["attributes"])
            if "geometry" in result and result["geometry"]:
                attrs["__geometry__"] = result["geometry"]
            filtered.append(attrs)
            if len(filtered) >= max_features:
                break
    return filtered


def format_arcgis_timestamp(value: object) -> Optional[str]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            dt = datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        elif isinstance(value, str) and value.isdigit():
            dt = datetime.fromtimestamp(int(value) / 1000.0, tz=timezone.utc)
        else:
            return None
        local_dt = dt.astimezone()
        time_part = local_dt.strftime("%I:%M %p").lstrip("0")
        return f"{local_dt.month}/{local_dt.day}/{local_dt.year}, {time_part}"
    except Exception as exc:  # noqa: BLE001 - formatting best effort
        logging.debug("Failed to parse timestamp %s: %s", value, exc)
    return None


def build_tax_assessor_link(parcel_id: str) -> str:
    normalized = parcel_id.replace(" ", "")
    return f"https://www.qpublic.net/ga/fulton/parcel.php?parcel={normalized}"


def normalize_link(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    lower = value.lower()
    if lower.startswith("http://") or lower.startswith("https://"):
        return value
    if lower.startswith("//"):
        return f"https:{value}"
    return urljoin(PROPINFO_BASE, value.lstrip("/"))


def fetch_maptiler_basemap(
    bounds: tuple[float, float, float, float],
    zoom: int,
    api_key: str,
) -> tuple[np.ndarray, tuple[float, float, float, float]]:
    minx, miny, maxx, maxy = bounds
    min_lon, min_lat = web_mercator_to_wgs84(minx, miny)
    max_lon, max_lat = web_mercator_to_wgs84(maxx, maxy)

    lon_min = min(min_lon, max_lon)
    lon_max = max(min_lon, max_lon)
    lat_min = min(min_lat, max_lat)
    lat_max = max(min_lat, max_lat)

    corners = [
        lonlat_to_tile(lon_min, lat_min, zoom),
        lonlat_to_tile(lon_min, lat_max, zoom),
        lonlat_to_tile(lon_max, lat_min, zoom),
        lonlat_to_tile(lon_max, lat_max, zoom),
    ]
    x_tiles = [tile[0] for tile in corners]
    y_tiles = [tile[1] for tile in corners]
    x_min_tile, x_max_tile = min(x_tiles), max(x_tiles)
    y_min_tile, y_max_tile = min(y_tiles), max(y_tiles)

    tile_arrays: List[np.ndarray] = []
    extent_calculated = [float("inf"), float("inf"), float("-inf"), float("-inf")]  # minx, miny, maxx, maxy

    for y in range(y_min_tile, y_max_tile + 1):
        row_arrays: List[np.ndarray] = []
        for x in range(x_min_tile, x_max_tile + 1):
            url = MAPTILER_TILE_URL.format(z=zoom, x=x, y=y, key=api_key)
            response = HTTP_SESSION.get(url, headers={"User-Agent": BASE_HEADERS["User-Agent"]}, timeout=20)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert("RGBA")
            row_arrays.append(np.array(img))

            tile_minx, tile_miny, tile_maxx, tile_maxy = tile_bounds_webmerc(x, y, zoom)
            extent_calculated[0] = min(extent_calculated[0], tile_minx)
            extent_calculated[1] = min(extent_calculated[1], tile_miny)
            extent_calculated[2] = max(extent_calculated[2], tile_maxx)
            extent_calculated[3] = max(extent_calculated[3], tile_maxy)

        if row_arrays:
            row_stack = np.hstack(row_arrays)
            tile_arrays.append(row_stack)

    if not tile_arrays:
        raise RuntimeError("Failed to download MapTiler tiles for the requested bounds.")

    mosaic = np.vstack(tile_arrays)
    return mosaic, (extent_calculated[0], extent_calculated[2], extent_calculated[1], extent_calculated[3])


FAILED_SERVICE_ATTEMPTS: Dict[str, int] = {}
MAX_SERVICE_FAILURES = 3


def safe_point_query(
    service_url: str,
    fields: Union[str, Sequence[str], None],
    label: str,
    *,
    x_merc: float,
    y_merc: float,
    token: Optional[str],
    max_features: int = 5,
    where: str = "1=1",
    layer_ids: Optional[Sequence[int]] = None,
) -> Optional[List[Dict[str, object]]]:
    """Query an ArcGIS layer for attributes near a point, falling back to identify.

    Returns:
        - list of attribute dicts when the request succeeds,
        - empty list when the request succeeds but yields no matches,
        - None when the request fails (network/server error).
    """
    cache_key = f"{service_url}::{label}"
    if FAILED_SERVICE_ATTEMPTS.get(cache_key, 0) >= MAX_SERVICE_FAILURES:
        logging.debug("Skipping %s after repeated failures", label)
        return []

    query_error: Optional[Exception] = None
    had_error = False
    try:
        results = query_layer_attributes_by_point(
            service_url,
            x_merc=x_merc,
            y_merc=y_merc,
            out_fields=fields,
            token=token,
            max_features=max_features,
            where=where,
        )
        if results:
            for item in results:
                item.pop("__geometry__", None)
            return results
    except Exception as exc:  # noqa: BLE001
        query_error = exc
        had_error = True

    base_url, default_layer = split_service_layer(service_url)
    ids = list(layer_ids) if layer_ids else []
    if not ids and default_layer is not None:
        ids.append(default_layer)

    if ids:
        try:
            identify_results = identify_layer_attributes(
                base_url,
                ids,
                x_merc=x_merc,
                y_merc=y_merc,
                token=token,
                max_features=max_features,
            )
            if identify_results:
                for item in identify_results:
                    item.pop("__geometry__", None)
                return identify_results
        except Exception as identify_exc:  # noqa: BLE001
            failures = FAILED_SERVICE_ATTEMPTS.get(cache_key, 0) + 1
            FAILED_SERVICE_ATTEMPTS[cache_key] = failures
            level = logging.WARNING if failures <= 1 else logging.DEBUG
            logging.log(level, "Failed to query %s via identify: %s", label, identify_exc)
            if query_error:
                logging.debug("Original query error for %s: %s", label, query_error)
            return None

    if query_error or had_error:
        failures = FAILED_SERVICE_ATTEMPTS.get(cache_key, 0) + 1
        FAILED_SERVICE_ATTEMPTS[cache_key] = failures
        level = logging.WARNING if failures <= 1 else logging.DEBUG
        logging.log(level, "Failed to query %s: %s", label, query_error)
        if failures >= MAX_SERVICE_FAILURES:
            logging.debug("Suppressing further attempts to query %s after %d failures.", label, failures)
        return None

    return []


def classify_neighbors(target: ParcelFeature, neighbors: Iterable[ParcelFeature]) -> Dict[str, List[ParcelFeature]]:
    adjacent: List[ParcelFeature] = []
    overlapping: List[ParcelFeature] = []
    others: List[ParcelFeature] = []
    target_boundary = target.geometry.boundary
    for parcel in neighbors:
        if parcel.object_id == target.object_id:
            continue
        if parcel.geometry.equals(target.geometry):
            overlapping.append(parcel)
            continue
        if not parcel.geometry.intersects(target.geometry):
            # keep anything intersecting the buffer but not touching; these are near-by
            others.append(parcel)
            continue
        intersection = target_boundary.intersection(parcel.geometry.boundary)
        if intersection.length > 0:
            adjacent.append(parcel)
        else:
            overlapping.append(parcel)
    return {
        "adjacent": adjacent,
        "overlapping": overlapping,
        "others": others,
    }


def render_map(
    target: ParcelFeature,
    neighbors: Iterable[ParcelFeature],
    buffer_meters: float,
    output_path: Path,
    *,
    bounds: Optional[tuple[float, float, float, float]] = None,
    basemap: Optional[tuple[np.ndarray, tuple[float, float, float, float]]] = None,
) -> None:
    fig: Figure
    ax: Axes
    fig, ax = plt.subplots(figsize=(8, 8))
    if bounds is None:
        all_geoms = [target.geometry] + [p.geometry for p in neighbors]
        minx, miny, maxx, maxy = unary_bounds(all_geoms, pad=buffer_meters / 2)
    else:
        minx, miny, maxx, maxy = bounds

    if basemap:
        basemap_image, basemap_extent = basemap
        ax.imshow(
            basemap_image,
            extent=basemap_extent,
            origin="upper",
            interpolation="bilinear",
        )

    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    ax.set_aspect("equal")
    ax.set_title(f"Parcel map around {target.address or target.parcel_id}")

    for parcel in neighbors:
        x, y = parcel.geometry.exterior.xy
        ax.plot(x, y, color="#555555", linewidth=1.0, alpha=0.7)

    tx, ty = target.geometry.exterior.xy
    ax.fill(tx, ty, color="#ffcc66", alpha=0.5, label="Subject parcel")
    ax.plot(tx, ty, color="#cc6600", linewidth=2.0)

    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    ax.set_xlabel("Web Mercator X (m)")
    ax.set_ylabel("Web Mercator Y (m)")
    ax.legend(loc="upper right")
    fig.tight_layout()
    fig.savefig(output_path, dpi=200)
    plt.close(fig)
    logging.info("Map tile saved to %s", output_path)


def unary_bounds(geoms: Sequence[BaseGeometry], pad: float = 0.0) -> tuple[float, float, float, float]:
    minx = min(g.bounds[0] for g in geoms)
    miny = min(g.bounds[1] for g in geoms)
    maxx = max(g.bounds[2] for g in geoms)
    maxy = max(g.bounds[3] for g in geoms)
    return minx - pad, miny - pad, maxx + pad, maxy + pad


def parcel_detail_record(parcel: ParcelFeature, extra: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    bounds = parcel.geometry.bounds
    detail = dict(parcel.attributes)
    detail.setdefault("OBJECTID", parcel.object_id)
    detail["_area_sq_m"] = round(parcel.geometry.area, 2)
    detail["_perimeter_m"] = round(parcel.geometry.length, 2)
    detail["_bounds"] = {
        "xmin": bounds[0],
        "ymin": bounds[1],
        "xmax": bounds[2],
        "ymax": bounds[3],
    }
    detail["geometry"] = mapping(parcel.geometry)
    if extra:
        detail.update(extra)
    return detail


def log_parcel_attributes(title: str, parcel: ParcelFeature, extra: Optional[Dict[str, object]] = None) -> None:
    record = parcel_detail_record(parcel, extra=extra)
    logging.info("%s\n%s", title, json.dumps(record, indent=2, default=str))


def fetch_property_info(
    parcel: ParcelFeature,
    *,
    token: Optional[str],
    reference_point: Optional[Tuple[float, float]] = None,
    fallback_address: Optional[str] = None,
    geocoded_address: Optional[str] = None,
) -> Dict[str, object]:
    """Collect property metadata and zoning context for a parcel."""
    centroid = parcel.geometry.centroid
    if reference_point:
        x_merc, y_merc = reference_point
    else:
        x_merc, y_merc = float(centroid.x), float(centroid.y)

    owner_1 = (parcel.attributes.get("OWNERNME1") or "").strip() or None
    owner_2 = (parcel.attributes.get("OWNERNME2") or "").strip() or None
    tax_year_attr = parcel.attributes.get("TAXYEAR") or parcel.attributes.get("TaxYear")
    assessor_parcel = parcel.attributes.get("PARCELID") or parcel.parcel_id

    property_info: Dict[str, object] = {
        "official_address": parcel.address or geocoded_address or fallback_address,
        "tax_parcel": parcel.parcel_id,
        "owner_1": owner_1,
        "owner_2": owner_2,
        "county_address": (parcel.attributes.get("SITEADDRESS") or "").strip()
        or parcel.address
        or geocoded_address
        or fallback_address,
        "tax_year": tax_year_attr,
        "tax_assessor_link": build_tax_assessor_link(str(assessor_parcel)),
    }

    doc_layer_id = split_service_layer(DOCUMENT_ARCHIVE_LAYER)[1]
    document_records = safe_point_query(
        DOCUMENT_ARCHIVE_LAYER,
        ["DOC_NAME", "DOC_LINK", "PLAT_NUM", "RECORD_DATE", "TOTAL_SHEETS"],
        "document archive",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[doc_layer_id] if doc_layer_id is not None else None,
    )
    if document_records is None:
        document_records = []
    document_record = document_records[0] if document_records else {}
    if document_record:
        document_record.pop("__geometry__", None)
        document_record = {k: v for k, v in document_record.items() if v not in ("", None)}
        document_record["formatted_record_date"] = format_arcgis_timestamp(document_record.get("RECORD_DATE"))
        if document_record.get("DOC_LINK"):
            document_record["DOC_LINK"] = normalize_link(document_record.get("DOC_LINK"))
    property_info["document"] = document_record

    zoning_layer_id = split_service_layer(OFFICIAL_ZONING_LAYER)[1]
    zoning_attrs = safe_point_query(
        OFFICIAL_ZONING_LAYER,
        ["ZONECLASS", "ZONEDESC", "ZONETYPE", "ZONE_NAME", "ZONECLASSIFICATION"],
        "official zoning",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=5,
        layer_ids=[zoning_layer_id] if zoning_layer_id is not None else None,
    )
    if zoning_attrs is None:
        zoning_attrs = []
    if zoning_attrs:
        chosen = zoning_attrs[0]
        property_info["official_zoning"] = (
            chosen.get("ZONECLASS")
            or chosen.get("ZONE_NAME")
            or chosen.get("ZONING")
            or chosen.get("ZONINGCODE")
        )
        property_info["zoning_classification"] = (
            chosen.get("ZONEDESC")
            or chosen.get("ZONE_NAME")
            or chosen.get("ZONING")
            or chosen.get("ZONECLASSIFICATION")
            or chosen.get("ZONECLASS")
            or chosen.get("ZONINGCODE")
        )
        property_info["official_zoning_attrs"] = {
            k: v for k, v in chosen.items() if v not in (None, "", " ")
        }
        pdf_link = None
        for key, value in chosen.items():
            if not isinstance(value, str):
                continue
            if "pdf" in key.lower() or value.lower().endswith(".pdf"):
                pdf_link = normalize_link(value)
                if pdf_link:
                    break
        if pdf_link:
            property_info["official_zoning_pdf"] = pdf_link

    overlays: List[Dict[str, object]] = []
    seen_overlay_names: set[str] = set()
    for config in ZONING_OVERLAY_LAYERS:
        fields = config.get("fields") or [config["name_field"], config.get("description_field")]
        overlay_layer_id = split_service_layer(config["service"])[1]
        overlay_attrs = safe_point_query(
            config["service"],
            fields,
            f"zoning overlay {config['service']}",
            x_merc=x_merc,
            y_merc=y_merc,
            token=token,
            max_features=10,
            layer_ids=[overlay_layer_id] if overlay_layer_id is not None else None,
        )
        if overlay_attrs is None:
            continue
        for attrs in overlay_attrs:
            name = attrs.get(config["name_field"])
            if not name or name in seen_overlay_names:
                continue
            seen_overlay_names.add(name)
            description = attrs.get(config.get("description_field", config["name_field"])) or name
            link = None
            for key in ("PDF_LINK", "URL", "LINK", "WEB_URL"):
                if attrs.get(key):
                    link = normalize_link(attrs.get(key))
                    break
            clean_attrs = {k: v for k, v in attrs.items() if v not in (None, "", " ")}
            overlays.append({"name": name, "description": description, "link": link, "attributes": clean_attrs})
    property_info["overlays"] = overlays
    property_info["overlay_names"] = [ov["name"] for ov in overlays if ov.get("name")]

    development_layer_id = split_service_layer(DEVELOPMENT_PATTERN_LAYER)[1]
    development_attrs = safe_point_query(
        DEVELOPMENT_PATTERN_LAYER,
        ["DP_NAME", "NAME", "DESCRIPTION"],
        "development pattern",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[development_layer_id] if development_layer_id is not None else None,
    )
    if development_attrs is None:
        development_attrs = []
    if development_attrs:
        dev = development_attrs[0]
        dev_name = dev.get("DP_NAME") or dev.get("NAME")
        property_info["future_land_use"] = dev_name
        property_info["development_pattern"] = dev.get("DESCRIPTION") or dev_name

    land_lot_layer_id = split_service_layer(LAND_LOT_LAYER)[1]
    land_lot_attrs = safe_point_query(
        LAND_LOT_LAYER,
        ["DIST_PAGE", "ZONINGMYLARLINK2", "PDF_LINK"],
        "land lot index",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[land_lot_layer_id] if land_lot_layer_id is not None else None,
    )
    if land_lot_attrs is None:
        land_lot_attrs = []
    if land_lot_attrs:
        lot = land_lot_attrs[0]
        property_info["land_lot_page"] = lot.get("DIST_PAGE")
        property_info["land_lot_link"] = normalize_link(lot.get("ZONINGMYLARLINK2") or lot.get("PDF_LINK"))

    council_layer_id = split_service_layer(COUNCIL_DISTRICT_LAYER)[1]
    council_attrs = safe_point_query(
        COUNCIL_DISTRICT_LAYER,
        ["NAME", "LINK", "URL", "WEBSITE"],
        "city council district",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[council_layer_id] if council_layer_id is not None else None,
    )
    if council_attrs is None:
        council_attrs = []
    if council_attrs:
        council = council_attrs[0]
        property_info["council_district"] = council.get("NAME")
        property_info["council_link"] = normalize_link(council.get("LINK") or council.get("URL") or council.get("WEBSITE"))

    npu_layer_id = split_service_layer(NPU_LAYER)[1]
    npu_attrs = safe_point_query(
        NPU_LAYER,
        ["NAME", "NPU", "URL"],
        "neighborhood planning unit",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[npu_layer_id] if npu_layer_id is not None else None,
    )
    if npu_attrs is None:
        npu_attrs = []
    if npu_attrs:
        npu = npu_attrs[0]
        property_info["npu"] = npu.get("NAME") or npu.get("NPU")
        if npu.get("URL"):
            property_info["npu_link"] = normalize_link(npu.get("URL"))

    neighborhood_layer_id = split_service_layer(NEIGHBORHOOD_LAYER)[1]
    nb_attrs = safe_point_query(
        NEIGHBORHOOD_LAYER,
        ["NAME", "NEIGHBORHOOD"],
        "neighborhood",
        x_merc=x_merc,
        y_merc=y_merc,
        token=token,
        max_features=3,
        layer_ids=[neighborhood_layer_id] if neighborhood_layer_id is not None else None,
    )
    if nb_attrs is None:
        nb_attrs = []
    if nb_attrs:
        nb = nb_attrs[0]
        property_info["neighborhood"] = nb.get("NAME") or nb.get("NEIGHBORHOOD")

    property_info["_query_point"] = {"x": x_merc, "y": y_merc}
    return property_info


def main() -> None:
    args = parse_args()
    setup_logging(args.debug)

    token = args.token or fetch_arcgis_token()
    if not token:
        logging.warning("No ArcGIS token resolved. Parcel queries may fail with HTTP 499.")
    else:
        logging.debug("Using ArcGIS token starting with %s…", token[:8])

    maptiler_key = (args.maptiler_key or "").strip()

    geocode = geocode_address(args.address)
    location = geocode.get("location", {})
    lon = float(location["x"])
    lat = float(location["y"])
    x_merc, y_merc = wgs84_to_web_mercator(lon, lat)
    logging.info("Geocoded location: lon=%s lat=%s (WebMercator x=%s, y=%s)", lon, lat, x_merc, y_merc)

    target = fetch_target_parcel(x_merc, y_merc, token=token)
    logging.info("Target parcel %s at %s", target.parcel_id, target.address)

    neighbor_candidates = fetch_neighbor_parcels(
        target,
        buffer_meters=args.buffer,
        token=token,
        max_neighbors=args.max_neighbors,
        include_target=True,
    )
    logging.info("Fetched %d nearby parcel candidates", len(neighbor_candidates))
    plot_bounds = unary_bounds(
        [target.geometry] + [p.geometry for p in neighbor_candidates],
        pad=args.buffer / 2,
    )
    classes = classify_neighbors(target, neighbor_candidates)
    adjacent = classes["adjacent"]
    logging.info("Identified %d adjacent parcels", len(adjacent))

    def basemap_output_path(base: Path) -> Path:
        if base.suffix:
            return base.with_name(f"{base.stem}_basemap{base.suffix}")
        return base.with_name(base.name + "_basemap.png")

    property_info = fetch_property_info(
        target,
        token=token,
        reference_point=(x_merc, y_merc),
        fallback_address=args.address,
        geocoded_address=geocode.get("address"),
    )
    document_record = property_info.get("document") or {}
    overlays = property_info.get("overlays") or []
    logging.info("Property summary:\n%s", json.dumps(property_info, indent=2, default=str))

    print("\nProperty Information")
    print(f"Official Address: {property_info.get('official_address') or 'Unavailable'}")
    print()
    print(f"Tax Parcel: {property_info.get('tax_parcel') or 'Unavailable'}")
    print(f"First Owner Name: {property_info.get('owner_1') or 'Unavailable'}")
    second_owner = property_info.get("owner_2") or ""
    print(f"Second Owner Name: {second_owner}")
    print(f"Unofficial County Address: {property_info.get('county_address') or 'Unavailable'}")
    print(f"Tax Year: {property_info.get('tax_year') or 'Unavailable'}")
    print(f"Link to Fulton County Tax Assessor: {property_info.get('tax_assessor_link') or 'Unavailable'}")

    if document_record:
        print("\nDocument Archive")
        print(f"PLAT_NUM: {document_record.get('PLAT_NUM', 'Unavailable')}")
        print(f"RECORD_DATE: {document_record.get('formatted_record_date') or 'Unavailable'}")
        print(f"TOTAL_SHEETS: {document_record.get('TOTAL_SHEETS', 'Unavailable')}")
        print(f"DOC_NAME: {document_record.get('DOC_NAME', 'Unavailable')}")
        if document_record.get("DOC_LINK"):
            print(f"Link to Plat PDF: {document_record['DOC_LINK']}")

    if property_info.get("official_zoning") or property_info.get("zoning_classification"):
        print()
        print(f"Official Zoning: {property_info.get('official_zoning') or 'Unavailable'}")
        print(f"Zoning Classification: {property_info.get('zoning_classification') or 'Unavailable'}")
        if property_info.get("official_zoning_pdf"):
            print("\nView official zoning PDF")
            print(f"Official Zoning Map PDF: {property_info['official_zoning_pdf']}")

    if property_info.get("land_lot_link"):
        print()
        print("Original mylar zoning maps for research:")
        print(f"    {property_info['land_lot_link']}")

    if overlays:
        print()
        for index, overlay in enumerate(overlays, start=1):
            print(f"Zoning Overlay: {overlay.get('name')}")
            print(f"Overlay Description: {overlay.get('description')}")
            if overlay.get("link"):
                print(f"Overlay Link: {overlay['link']}")
            if index < len(overlays):
                print()
        print()

    if property_info.get("future_land_use") or property_info.get("development_pattern"):
        print()
        print("Development Patterns - Future Land Use: {}".format(property_info.get("future_land_use") or "Unavailable"))
        print("Development Pattern: {}".format(property_info.get("development_pattern") or "Unavailable"))

    if property_info.get("land_lot_page"):
        print("\nLand Lot Index")
        print(f"Land Lot District + Page: {property_info['land_lot_page']}")
        if property_info.get("land_lot_link"):
            print(f"Link to cadastral PDF map: {property_info['land_lot_link']}")

    if property_info.get("neighborhood"):
        print("\nNeighborhood")
        print(f"Neighborhood: {property_info['neighborhood']}")

    if property_info.get("council_district"):
        print("\nCity Council Districts")
        print(f"City Council Districts: {property_info['council_district']}")
        if property_info.get("council_link"):
            print(f"Link to council webpage: {property_info['council_link']}")

    if property_info.get("npu"):
        print("\nNPU")
        print(f"NPU: {property_info['npu']}")

    render_map(
        target,
        neighbor_candidates,
        buffer_meters=args.buffer,
        output_path=args.output,
        bounds=plot_bounds,
    )

    if maptiler_key:
        basemap_path = basemap_output_path(args.output)
        basemap_rendered = False
        zoom_candidates = [args.maptiler_zoom, args.maptiler_zoom - 1, args.maptiler_zoom - 2]
        for zoom_level in zoom_candidates:
            if zoom_level < 0 or basemap_rendered:
                continue
            try:
                basemap_image, basemap_extent = fetch_maptiler_basemap(plot_bounds, zoom_level, maptiler_key)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed to retrieve MapTiler basemap (zoom %s): %s", zoom_level, exc)
                continue
            render_map(
                target,
                neighbor_candidates,
                buffer_meters=args.buffer,
                output_path=basemap_path,
                bounds=plot_bounds,
                basemap=(basemap_image, basemap_extent),
            )
            logging.info("Map tile with basemap saved to %s", basemap_path)
            basemap_rendered = True
        if not basemap_rendered:
            logging.warning("Unable to render MapTiler basemap after trying zoom levels %s", zoom_candidates)

    log_parcel_attributes("Subject parcel attributes:", target)
    print("Subject parcel attributes:")
    print(json.dumps(parcel_detail_record(target), indent=2, default=str))

    relation_index = {}
    for relation, parcels in classes.items():
        for parcel in parcels:
            relation_index[parcel.object_id] = relation

    neighbors_only = [p for p in neighbor_candidates if p.object_id != target.object_id]
    if neighbors_only:
        print("\nNeighbor parcel attributes (within buffer):")
        for parcel in neighbors_only:
            relation = relation_index.get(parcel.object_id, "nearby")
            extra = {"_relation": relation}
            log_parcel_attributes(
                f"Neighbor parcel {parcel.parcel_id} ({relation}) attributes:", parcel, extra=extra
            )
            print(json.dumps(parcel_detail_record(parcel, extra=extra), indent=2, default=str))
            print()
    else:
        logging.info("No neighbor parcels identified within buffer %.2f m", args.buffer)


if __name__ == "__main__":
    main()
