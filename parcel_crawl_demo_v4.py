#!/usr/bin/env python3
"""V4 parcel crawl – GUI-assisted DXF footprint sweep with per-parcel fit scoring."""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import threading
import warnings
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import ezdxf
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from matplotlib.patches import Patch
from shapely import affinity
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon, mapping
from shapely.geometry.base import BaseGeometry
from shapely.ops import polygonize, unary_union
from shapely.prepared import prep

import parcel_lookup
from parcel_lookup import (
    ParcelFeature,
    fetch_arcgis_token,
    fetch_neighbor_parcels,
    fetch_property_info,
    fetch_target_parcel,
    geocode_address,
    parcel_detail_record,
    unary_bounds,
    web_mercator_to_wgs84,
    wgs84_to_web_mercator,
)

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, simpledialog
    from tkinter import ttk
except Exception:  # pragma: no cover - headless environments
    tk = None

try:
    from PIL import Image, ImageTk  # type: ignore
except Exception:
    Image = None
    ImageTk = None


INSUNITS_METERS_PER_UNIT: Dict[int, Optional[float]] = {
    0: None,
    1: 0.0254,
    2: 0.3048,
    3: 1609.344,
    4: 0.001,
    5: 0.01,
    6: 1.0,
    7: 1000.0,
    8: 0.0254e-6,
    9: 0.0000254,
    10: 0.9144,
    11: 1e-10,
    12: 1e-9,
    13: 1e-6,
    14: 0.1,
    15: 10.0,
    16: 100.0,
    17: 1e9,
    18: 149_597_870_700.0,
    19: 9.4607304725808e15,
    20: 3.08567758149137e16,
    21: 1200.0 / 3937.0,
}

OVERPASS_URLS: List[str] = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/cgi/interpreter",
]
ORIENTED_WARNING_KEY = "oriented_envelope"
WARNING_COUNTS: Dict[str, int] = {ORIENTED_WARNING_KEY: 0}
_ORIGINAL_SHOWWARNING = None

REQUEST_SESSION = requests.Session()
REQUEST_SESSION.mount("https://", HTTPAdapter(pool_connections=24, pool_maxsize=48))
REQUEST_SESSION.mount("http://", HTTPAdapter(pool_connections=24, pool_maxsize=48))

OVERPASS_INDEX = 0
LAST_ROAD_FETCH = 0.0
ROAD_FAILURE_COUNT = 0
ROAD_BACKOFF_UNTIL = 0.0
ROAD_MASTER_LINES: List[LineString] = []
ROAD_MASTER_BOUNDS: Optional[Tuple[float, float, float, float]] = None


def bounds_contains(outer: Tuple[float, float, float, float], inner: Tuple[float, float, float, float]) -> bool:
    return (
        inner[0] >= outer[0]
        and inner[1] >= outer[1]
        and inner[2] <= outer[2]
        and inner[3] <= outer[3]
    )


def expand_bounds(bounds: Tuple[float, float, float, float], pad: float) -> Tuple[float, float, float, float]:
    return (bounds[0] - pad, bounds[1] - pad, bounds[2] + pad, bounds[3] + pad)


def merge_bounds(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> Tuple[float, float, float, float]:
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))


def _fetch_roads_from_bounds(bounds: Tuple[float, float, float, float]) -> List[LineString]:
    global ROAD_FAILURE_COUNT, ROAD_BACKOFF_UNTIL, LAST_ROAD_FETCH, OVERPASS_INDEX, ROAD_MASTER_LINES, ROAD_MASTER_BOUNDS

    now = time.monotonic()
    if ROAD_BACKOFF_UNTIL and now < ROAD_BACKOFF_UNTIL:
        remaining = ROAD_BACKOFF_UNTIL - now
        logging.debug("Skipping road fetch (backing off for %.1fs).", remaining)
        return []

    minx, miny, maxx, maxy = bounds
    corners = [
        (minx, miny),
        (minx, maxy),
        (maxx, miny),
        (maxx, maxy),
    ]
    lons: List[float] = []
    lats: List[float] = []
    for xw, yw in corners:
        lon, lat = web_mercator_to_wgs84(xw, yw)
        lons.append(lon)
        lats.append(lat)
    south = min(lats)
    north = max(lats)
    west = min(lons)
    east = max(lons)

    query = (
        "[out:json][timeout:30];"
        f"(way['highway'~'^(motorway|trunk|primary|secondary|tertiary|residential|service|unclassified)$']"
        f"({south},{west},{north},{east}););"
        "out geom;"
    )

    wait = 1.0 - (now - LAST_ROAD_FETCH)
    if wait > 0:
        time.sleep(min(wait, 1.5))

    payload = None
    for attempt in range(len(OVERPASS_URLS)):
        url_index = (OVERPASS_INDEX + attempt) % len(OVERPASS_URLS)
        endpoint = OVERPASS_URLS[url_index]
        try:
            response = REQUEST_SESSION.post(endpoint, data={"data": query}, timeout=35)
            response.raise_for_status()
            payload = response.json()
            OVERPASS_INDEX = url_index
            LAST_ROAD_FETCH = time.monotonic()
            ROAD_FAILURE_COUNT = 0
            ROAD_BACKOFF_UNTIL = 0.0
            break
        except requests.RequestException as exc:
            ROAD_FAILURE_COUNT += 1
            logging.warning("Failed to fetch roads via %s: %s", endpoint, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            ROAD_FAILURE_COUNT += 1
            logging.warning("Failed to fetch roads via %s: %s", endpoint, exc)
            continue
    if payload is None:
        base_delay = 7.0
        backoff_seconds = min(180.0, base_delay * max(1, ROAD_FAILURE_COUNT))
        ROAD_BACKOFF_UNTIL = time.monotonic() + backoff_seconds
        logging.warning("Backing off road fetches for %.0f seconds after repeated failures.", backoff_seconds)
        return []

    new_lines: List[LineString] = []
    for element in payload.get("elements", []):
        geometry = element.get("geometry")
        if not geometry:
            continue
        points: List[Tuple[float, float]] = []
        for node in geometry:
            lon = node.get("lon")
            lat = node.get("lat")
            if lon is None or lat is None:
                continue
            x_merc, y_merc = wgs84_to_web_mercator(float(lon), float(lat))
            points.append((x_merc, y_merc))
        if len(points) >= 2:
            new_lines.append(LineString(points))

    if new_lines:
        if ROAD_MASTER_LINES and ROAD_MASTER_BOUNDS:
            ROAD_MASTER_LINES.extend(new_lines)
            ROAD_MASTER_BOUNDS = merge_bounds(ROAD_MASTER_BOUNDS, bounds)
        else:
            ROAD_MASTER_LINES = list(new_lines)
            ROAD_MASTER_BOUNDS = bounds

    return new_lines
@dataclass
class FootprintProfile:
    geometry: Polygon
    centroid: Tuple[float, float]
    area: float
    span: float


@dataclass
class RotatedFootprint:
    angle: float
    geometry: Polygon
    centroid: Tuple[float, float]
    bounds: Tuple[float, float, float, float]


@dataclass
class ParcelEvaluationResult:
    parcel: ParcelFeature
    placements: List[Dict[str, object]]
    summary: Dict[str, object]
    best_placement: Optional[Dict[str, object]]
    best_geometry: Optional[Polygon]
    buildable: Polygon
    roads: List[LineString]
    disqualified: bool


@dataclass
class RotationCacheEntry:
    wkb: bytes
    centroid: Tuple[float, float]
    bounds: Tuple[float, float, float, float]


WORKER_CONTEXT: Dict[str, object] = {}


class EventRecorder:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, event_type: str, payload: Dict[str, object]) -> None:
        event = {
            "type": event_type,
            "timestamp": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
        }
        event.update(payload)
        line = json.dumps(event, default=str)
        with self.lock:
            with self.path.open("a", encoding="utf-8") as stream:
                stream.write(line + "\n")
def install_warning_capture() -> None:
    global _ORIGINAL_SHOWWARNING
    if _ORIGINAL_SHOWWARNING is not None:
        return

    def _showwarning(message, category, filename, lineno, file=None, line=None):  # type: ignore[override]
        text = str(message)
        if category is RuntimeWarning and ORIENTED_WARNING_KEY in text:
            WARNING_COUNTS[ORIENTED_WARNING_KEY] = WARNING_COUNTS.get(ORIENTED_WARNING_KEY, 0) + 1
            return
        if _ORIGINAL_SHOWWARNING:
            _ORIGINAL_SHOWWARNING(message, category, filename, lineno, file=file, line=line)

    _ORIGINAL_SHOWWARNING = warnings.showwarning
    warnings.simplefilter("always", RuntimeWarning)
    warnings.showwarning = _showwarning


def placement_to_geometry(
    placement: Dict[str, object],
    footprint_profile: FootprintProfile,
    parcel_geom: Polygon,
) -> Polygon:
    angle = placement["rotation_deg"]
    dx = placement["offset_x_m"]
    dy = placement["offset_y_m"]
    rotated = affinity.rotate(footprint_profile.geometry, angle, origin=footprint_profile.centroid)
    centroid = parcel_geom.centroid
    transformed = affinity.translate(
        rotated,
        xoff=centroid.x + dx - rotated.centroid.x,
        yoff=centroid.y + dy - rotated.centroid.y,
    )
    if not transformed.is_valid:
        transformed = transformed.buffer(0)
    return transformed


def iter_polygons(geom: BaseGeometry) -> Iterable[Polygon]:
    if isinstance(geom, Polygon):
        if not geom.is_empty:
            yield geom
    elif isinstance(geom, MultiPolygon):
        for sub in geom.geoms:
            yield from iter_polygons(sub)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parcel crawl with DXF footprint scoring (v4).")
    parser.add_argument("--cycles", type=int, default=6, help="Crawl waves to execute (default 6).")
    parser.add_argument("--buffer", type=float, default=80.0, help="Neighbor query buffer in meters.")
    parser.add_argument("--max-neighbors", type=int, default=50, help="Max parcels requested per neighbor query.")
    parser.add_argument("--workers", type=int, default=6, help="Neighbor fetch concurrency (default 6).")
    parser.add_argument("--score-workers", type=int, default=1, help="Placement evaluation workers (default 1).")
    parser.add_argument("--rotation-step", type=float, default=15.0, help="Rotation increment in degrees.")
    parser.add_argument(
        "--offset-step-scale",
        type=float,
        default=0.2,
        help="Footprint span multiplier for offset spacing (default 0.2).",
    )
    parser.add_argument("--offset-step", type=float, default=None, help="Explicit offset grid step in meters (overrides scale).")
    parser.add_argument("--offset-range", type=float, default=None, help="Explicit offset sweep range in meters (requires --no-auto-offset).")
    parser.add_argument(
        "--auto-offset-scale",
        type=float,
        default=2.0,
        help="Footprint span multiplier for offset radius (default 2.0).",
    )
    parser.add_argument("--auto-offset", dest="auto_offset", action="store_true", help="Derive offset range automatically from parcel bounds (default).")
    parser.add_argument("--no-auto-offset", dest="auto_offset", action="store_false", help="Use manual offset range values.")
    parser.set_defaults(auto_offset=True)
    parser.add_argument("--setback", type=float, default=3.0, help="Uniform setback to carve buildable envelope.")
    parser.add_argument("--full-rotation", action="store_true", help="Sweep the full 0-360° rotation range (default halves at 180°).")
    parser.add_argument("--min-composite", type=float, default=0.0, help="Minimum composite score required to retain a placement (default 0).")
    parser.add_argument("--no-render-cycle", dest="render_cycle", action="store_false", help="Skip writing cycle PNGs to speed up runs.")
    parser.add_argument("--no-render-best", dest="render_best", action="store_false", help="Skip writing per-parcel best-fit PNGs.")
    parser.add_argument("--no-render-composite", dest="render_composite", action="store_false", help="Skip writing per-parcel composite overlays.")
    parser.add_argument("--skip-roads", action="store_true", help="Skip Overpass road fetches (faster but removes road-based scoring).")
    parser.add_argument("--auto-front", dest="auto_front", action="store_true", help="Derive frontage vector automatically from the DXF footprint.")
    parser.add_argument("--no-auto-front", dest="auto_front", action="store_false", help="Force interactive frontage selection when possible.")
    parser.add_argument("--front-angle", type=float, default=None, help="Explicit frontage heading in degrees (0° = +X axis).")
    parser.add_argument(
        "--front-vector",
        type=float,
        nargs=2,
        metavar=("X", "Y"),
        default=None,
        help="Explicit frontage direction vector components.",
    )
    parser.add_argument(
        "--footprint-json",
        type=Path,
        default=None,
        help="JSON file containing footprint points (when running headless)",
    )
    parser.set_defaults(render_cycle=True, render_best=True, render_composite=True, skip_roads=False, auto_front=False)
    parser.add_argument(
        "--frontage-perpendicular",
        action="store_true",
        help="Treat selected frontage vector as perpendicular instead of parallel.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("parcel_crawl_v4"), help="Output directory.")
    parser.add_argument("--token", type=str, default=None, help="Optional ArcGIS token.")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--address", type=str, default=None, help="Seed address (if omitted GUI wizard prompts).")
    parser.add_argument("--dxf", type=Path, default=None, help="DXF design file (if omitted GUI wizard prompts).")
    return parser.parse_args()


def setup_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s [%(levelname)s] %(message)s")


def slugify(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "-._" else "_" for ch in value)
    safe = safe.strip("._")
    return safe or "parcel"


def select_dxf_path(initial: Optional[Path] = None) -> Path:
    if initial:
        return initial.expanduser().resolve()
    if tk is None:
        raise RuntimeError("Tkinter not available; please provide --dxf.")
    root = tk.Tk()
    root.withdraw()
    path = filedialog.askopenfilename(
        parent=root,
        title="Select DXF footprint",
        filetypes=[("DXF files", "*.dxf"), ("All files", "*.*")],
    )
    root.destroy()
    if not path:
        raise RuntimeError("DXF selection cancelled.")
    return Path(path).expanduser().resolve()


def prompt_address(initial: Optional[str] = None) -> str:
    if initial:
        return initial
    if tk is None:
        raise RuntimeError("Tkinter not available; please provide --address.")
    root = tk.Tk()
    root.withdraw()
    addr = simpledialog.askstring("Seed address", "Enter crawl starting address:", parent=root)  # type: ignore[attr-defined]
    root.destroy()
    if not addr:
        raise RuntimeError("Address entry cancelled.")
    return addr


def calculate_unit_scale(units_code: int) -> float:
    meters_per_unit = INSUNITS_METERS_PER_UNIT.get(units_code, None)
    if not meters_per_unit or meters_per_unit <= 0:
        logging.warning("DXF units unknown or zero. Assuming units are meters.")
        return 1.0
    return meters_per_unit


def load_dxf_polygons(
    dxf_path: Path,
) -> Tuple[List[Polygon], int, Optional[Tuple[float, float, float, float]], List[List[Tuple[float, float]]], List[LineString]]:
    doc = ezdxf.readfile(str(dxf_path))
    msp = doc.modelspace()
    polygons: List[Polygon] = []
    coord_points: List[Tuple[float, float]] = []
    paths: List[List[Tuple[float, float]]] = []
    raw_lines: List[LineString] = []

    def to_polygon(points: Sequence[Tuple[float, float]]) -> Optional[Polygon]:
        if len(points) < 3:
            return None
        poly = Polygon(points)
        if not poly.is_valid:
            poly = poly.buffer(0)
        if not poly.is_valid or poly.area <= 0:
            return None
        return poly

    for entity in msp:
        dxftype = entity.dxftype()
        if dxftype == "LWPOLYLINE" and entity.closed:
            pts = [(pt[0], pt[1]) for pt in entity.get_points("xy")]
            poly = to_polygon(pts)
            if poly:
                polygons.append(poly)
                coord_points.extend(pts)
            if pts:
                paths.append(pts + [pts[0]])
            raw_lines.extend(LineString([pts[i], pts[(i + 1) % len(pts)]]) for i in range(len(pts)))
        elif dxftype == "POLYLINE":
            pts = [(v.dxf.location[0], v.dxf.location[1]) for v in entity.vertices()]
            poly = to_polygon(pts)
            if poly:
                polygons.append(poly)
                coord_points.extend(pts)
            if entity.is_closed and pts:
                paths.append(pts + [pts[0]])
                raw_lines.extend(LineString([pts[i], pts[(i + 1) % len(pts)]]) for i in range(len(pts)))
                raw_lines.append(LineString([pts[-1], pts[0]]))
            elif pts:
                paths.append(pts)
                raw_lines.extend(LineString([pts[i], pts[i + 1]]) for i in range(len(pts) - 1))
        elif dxftype == "LINE":
            start = entity.dxf.start
            end = entity.dxf.end
            coord_points.append((start[0], start[1]))
            coord_points.append((end[0], end[1]))
            paths.append([(start[0], start[1]), (end[0], end[1])])
            raw_lines.append(LineString([(start[0], start[1]), (end[0], end[1])]))
        elif dxftype == "CIRCLE":
            center = entity.dxf.center
            radius = entity.dxf.radius
            samples = [
                (center[0] + radius * math.cos(theta), center[1] + radius * math.sin(theta))
                for theta in np.linspace(0, 2 * math.pi, 64, endpoint=True)
            ]
            paths.append(samples)
            coord_points.extend(samples)
            raw_lines.append(LineString(samples))
        elif dxftype == "ARC":
            center = entity.dxf.center
            radius = entity.dxf.radius
            start_angle = math.radians(entity.dxf.start_angle)
            end_angle = math.radians(entity.dxf.end_angle)
            points = [
                (center[0] + radius * math.cos(theta), center[1] + radius * math.sin(theta))
                for theta in np.linspace(start_angle, end_angle, 64)
            ]
            paths.append(points)
            coord_points.extend(points)
            raw_lines.append(LineString(points))

    units_code = int(doc.header.get("$INSUNITS", 0) or 0)
    extents: Optional[Tuple[float, float, float, float]] = None
    if coord_points:
        xs, ys = zip(*coord_points)
        extents = (min(xs), min(ys), max(xs), max(ys))
    return polygons, units_code, extents, paths, raw_lines


def normalize_polygons(polygons: Iterable[Polygon], scale_m_per_unit: float) -> List[Polygon]:
    return [affinity.scale(poly, scale_m_per_unit, scale_m_per_unit, origin=(0, 0)) for poly in polygons]


def normalize_paths(paths: Iterable[List[Tuple[float, float]]], scale_m_per_unit: float) -> List[List[Tuple[float, float]]]:
    return [[(x * scale_m_per_unit, y * scale_m_per_unit) for x, y in path] for path in paths]


def normalize_lines(lines: Iterable[LineString], scale_m_per_unit: float) -> List[LineString]:
    return [affinity.scale(ln, scale_m_per_unit, scale_m_per_unit, origin=(0, 0)) for ln in lines]


def shrinkwrap_polygon(rect: Polygon, base_lines: List[LineString]) -> Polygon:
    if not base_lines:
        return rect
    try:
        union_lines = unary_union(base_lines)
    except Exception:
        return rect
    box = rect.bounds
    span = max(box[2] - box[0], box[3] - box[1])
    buffer_eps = max(0.05, span * 0.02)

    try:
        candidate_polys = list(polygonize(union_lines))
    except Exception:
        candidate_polys = []

    if candidate_polys:
        candidate_union = unary_union(candidate_polys)
        if candidate_union.is_empty:
            candidate_union = union_lines.buffer(buffer_eps)
    else:
        candidate_union = union_lines.buffer(buffer_eps)

    candidate_union = candidate_union.buffer(buffer_eps)
    if candidate_union.is_empty:
        return rect

    shrinked = rect.intersection(candidate_union)
    if shrinked.is_empty:
        return rect
    if isinstance(shrinked, Polygon):
        return shrinked
    if hasattr(shrinked, "geoms"):
        polys = [geom for geom in shrinked.geoms if isinstance(geom, Polygon) and geom.area > 0]
        if polys:
            return max(polys, key=lambda g: g.area)
    return rect


def prompt_front_direction(footprint: Polygon) -> Tuple[float, float]:
    import matplotlib.pyplot as plt  # local import to avoid backend issues at module import time

    fig, ax = plt.subplots(figsize=(7, 7))
    xs, ys = footprint.exterior.xy
    ax.fill(xs, ys, color="#60a5fa", alpha=0.5)
    ax.plot(xs, ys, color="#1d4ed8", linewidth=1.6)
    ax.set_aspect("equal")
    ax.set_title(
        "Select frontage direction:\n"
        "1) Click along the front edge. 2) Click outward to indicate facing direction.\n"
        "Press Enter to confirm or R to reset."
    )

    clicks: List[Tuple[float, float]] = []
    markers: List[object] = []
    arrow_artist = None

    def clear_preview() -> None:
        nonlocal arrow_artist
        while markers:
            artist = markers.pop()
            try:
                artist.remove()
            except Exception:
                pass
        if arrow_artist is not None:
            try:
                arrow_artist.remove()
            except Exception:
                pass
            arrow_artist = None

    def update_preview() -> None:
        nonlocal arrow_artist
        clear_preview()
        for idx, point in enumerate(clicks):
            color = "#fb923c" if idx == 0 else "#f97316"
            marker = ax.scatter(point[0], point[1], s=48, color=color, zorder=5)
            markers.append(marker)
        if len(clicks) == 2:
            arrow_artist = ax.annotate(
                "",
                xy=clicks[1],
                xytext=clicks[0],
                arrowprops=dict(arrowstyle="->", linewidth=2.2, color="#ea580c"),
            )
        fig.canvas.draw_idle()

    def on_click(event) -> None:
        if event.button != 1 or not event.inaxes or len(clicks) >= 2:
            return
        clicks.append((event.xdata, event.ydata))
        update_preview()

    def on_key(event) -> None:
        if event.key in {"enter", "return"} and len(clicks) == 2:
            plt.close(fig)
        elif event.key in {"r", "escape"}:
            clicks.clear()
            update_preview()

    fig.canvas.mpl_connect("button_press_event", on_click)
    fig.canvas.mpl_connect("key_press_event", on_key)
    plt.show(block=True)

    if len(clicks) < 2:
        logging.warning("Front direction selection skipped; defaulting to +X axis.")
        return (1.0, 0.0)
    clear_preview()

    vec = (clicks[1][0] - clicks[0][0], clicks[1][1] - clicks[0][1])
    length = math.hypot(vec[0], vec[1])
    if length <= 1e-9:
        return (1.0, 0.0)
    normalized = (vec[0] / length, vec[1] / length)
    logging.info("Front direction captured with heading %.1f°.", math.degrees(math.atan2(normalized[1], normalized[0])))
    return normalized


def interactive_bounds_selector(paths: List[List[Tuple[float, float]]], base_lines: List[LineString]) -> Tuple[Polygon, Tuple[float, float]]:
    import matplotlib.pyplot as plt  # local import for GUI contexts

    points: List[Tuple[float, float]] = []
    result: Dict[str, object] = {}
    preview_patch = None
    shrink_patch = None

    fig, ax = plt.subplots(figsize=(8, 8))
    for path in paths:
        if len(path) < 2:
            continue
        xs, ys = zip(*path)
        ax.plot(xs, ys, color="#0f172a", linewidth=0.8)

    ax.set_aspect("equal")
    ax.set_title(
        "Click 3 points: A (corner), B (width direction), C (height direction).\n"
        "A red outline shows your box; the teal fill is the shrink-wrap result.\n"
        "Press Enter to accept or R to retry."
    )
    info_text = ax.text(
        0.02,
        0.02,
        "Awaiting point A",
        transform=ax.transAxes,
        fontsize=9,
        ha="left",
        va="bottom",
        bbox=dict(facecolor="white", alpha=0.8, edgecolor="none"),
    )

    def update_text() -> None:
        labels = ["A", "B", "C"]
        if len(points) < 3:
            info_text.set_text(f"Selected {len(points)} point(s). Awaiting point {labels[len(points)]}.")
        else:
            info_text.set_text("Points selected. Press Enter to confirm or R to reset.")
        fig.canvas.draw_idle()

    def reset() -> None:
        nonlocal preview_patch, shrink_patch
        points.clear()
        if preview_patch is not None:
            preview_patch.remove()
            preview_patch = None
        if shrink_patch is not None:
            shrink_patch.remove()
            shrink_patch = None
        update_text()

    def compute_rectangle() -> Optional[Polygon]:
        nonlocal preview_patch, shrink_patch
        if len(points) < 3:
            return None
        A = np.array(points[0])
        B = np.array(points[1])
        C = np.array(points[2])
        width_vec = B - A
        width = np.linalg.norm(width_vec)
        if width <= 1e-6:
            return None
        width_unit = width_vec / width
        height_vec = C - A
        height_proj = height_vec - np.dot(height_vec, width_unit) * width_unit
        height = np.linalg.norm(height_proj)
        if height <= 1e-6:
            return None
        height_unit = height_proj / height
        corner_A = A
        corner_B = A + width_unit * width
        corner_C = corner_B + height_unit * height
        corner_D = A + height_unit * height
        polygon_pts = [tuple(corner_A), tuple(corner_B), tuple(corner_C), tuple(corner_D)]
        rect_poly = Polygon(polygon_pts)
        if preview_patch is not None:
            preview_patch.remove()
        preview_patch = plt.Polygon(polygon_pts, closed=True, fill=False, linewidth=1.5, edgecolor="#ef4444")
        ax.add_patch(preview_patch)
        shrinked = shrinkwrap_polygon(rect_poly, base_lines)
        if shrink_patch is not None:
            shrink_patch.remove()
        if shrinked and shrinked.is_valid and not shrinked.is_empty:
            sx, sy = shrinked.exterior.xy
            shrink_patch = ax.fill(sx, sy, color="#22c55e", alpha=0.25, zorder=0)[0]
            result["polygon"] = shrinked
        else:
            result.pop("polygon", None)
        fig.canvas.draw_idle()
        return rect_poly

    def on_click(event) -> None:
        if event.button != 1 or not event.inaxes:
            return
        if len(points) >= 3:
            return
        points.append((event.xdata, event.ydata))
        ax.plot(event.xdata, event.ydata, marker="o", color="#f97316")
        if len(points) == 3:
            compute_rectangle()
        update_text()

    def on_key(event) -> None:
        if event.key in {"enter", "return"} and "polygon" in result:
            plt.close(fig)
        elif event.key in {"r", "escape"}:
            reset()

    fig.canvas.mpl_connect("button_press_event", on_click)
    fig.canvas.mpl_connect("key_press_event", on_key)

    plt.show(block=True)

    shrinked = result.get("polygon")
    if not shrinked or not isinstance(shrinked, Polygon):
        raise RuntimeError("Footprint selection cancelled.")
    if not shrinked.is_valid or shrinked.area <= 0:
        raise RuntimeError("Generated footprint is invalid.")

    if tk is not None:
        try:
            parent = tk._default_root  # type: ignore[attr-defined]
        except Exception:
            parent = None
        temp_root = None
        try:
            if parent is None:
                temp_root = tk.Tk()
                temp_root.withdraw()
                parent = temp_root
            messagebox.showinfo(
                "Footprint Captured",
                "Shrink-wrap applied successfully.\nSelect frontage direction next.",
                parent=parent,
            )
        except Exception:
            pass
        finally:
            if temp_root is not None:
                temp_root.destroy()

    front_vec = prompt_front_direction(shrinked)
    return shrinked, front_vec


def normalize_vector(vec: Tuple[float, float]) -> Tuple[float, float]:
    length = math.hypot(vec[0], vec[1])
    if length <= 1e-9:
        return (1.0, 0.0)
    return (vec[0] / length, vec[1] / length)


def vector_from_angle(angle_deg: float) -> Tuple[float, float]:
    rad = math.radians(angle_deg)
    return (math.cos(rad), math.sin(rad))


def perpendicular(vec: Tuple[float, float]) -> Tuple[float, float]:
    return (-vec[1], vec[0])


def rotate_vector(vec: Tuple[float, float], angle_deg: float) -> Tuple[float, float]:
    rad = math.radians(angle_deg)
    cos_a = math.cos(rad)
    sin_a = math.sin(rad)
    return (vec[0] * cos_a - vec[1] * sin_a, vec[0] * sin_a + vec[1] * cos_a)


def normalize_angle(angle: float) -> float:
    value = angle % 360.0
    if value < 0:
        value += 360.0
    return round(value, 6)


def major_axis_angle(rect: Polygon) -> float:
    coords = list(rect.exterior.coords)
    if len(coords) < 4:
        return 0.0
    edge1 = np.array(coords[1]) - np.array(coords[0])
    edge2 = np.array(coords[2]) - np.array(coords[1])
    length1 = np.linalg.norm(edge1)
    length2 = np.linalg.norm(edge2)
    major = edge1 if length1 >= length2 else edge2
    angle = math.degrees(math.atan2(major[1], major[0]))
    return angle


def bounds_overlap(
    outer: Tuple[float, float, float, float],
    inner: Tuple[float, float, float, float],
    margin: float = 0.0,
) -> bool:
    return not (
        inner[2] < outer[0] - margin
        or inner[0] > outer[2] + margin
        or inner[3] < outer[1] - margin
        or inner[1] > outer[3] + margin
    )


def _init_worker(context: Dict[str, object]) -> None:
    from shapely import wkb as _wkb

    install_warning_capture()

    parcel_geom = _wkb.loads(context["parcel_wkb"])
    buildable_geom = (
        _wkb.loads(context["buildable_wkb"])
        if context.get("buildable_wkb") is not None
        else parcel_geom
    )
    roads_geom = (
        _wkb.loads(context["roads_geom_wkb"])
        if context.get("roads_geom_wkb") is not None
        else None
    )
    roads_raw = [_wkb.loads(item) for item in context.get("roads_wkbs", [])]
    footprint_base = _wkb.loads(context["footprint_wkb"])

    global WORKER_CONTEXT
    WORKER_CONTEXT = {
        "footprint_base": footprint_base,
        "footprint_base_centroid": tuple(context["footprint_centroid"]),
        "parcel_centroid": tuple(context["parcel_centroid"]),
        "parcel_geom": parcel_geom,
        "parcel_prepared": prep(parcel_geom),
        "parcel_area": float(context["parcel_area"]),
        "parcel_bounds": tuple(context["parcel_bounds"]),
        "bounds_margin": float(context["bounds_margin"]),
        "buildable": buildable_geom,
        "buildable_prepared": prep(buildable_geom) if not buildable_geom.is_empty else None,
        "roads_geom": roads_geom,
        "roads_raw": roads_raw,
        "parcel_info": context["parcel_info"],
        "front_vector_base": normalize_vector(tuple(context["front_vector"])),
        "parcel_major_angle": float(context["parcel_major_angle"]),
        "min_composite": float(context["min_composite"]),
        "rotation_cache": {},
    }


def _evaluate_pose_process(task: Tuple[float, float, float]) -> Optional[Dict[str, object]]:
    from shapely import affinity as _affinity, wkb as _wkb

    angle_value, dx_value, dy_value = task
    ctx = WORKER_CONTEXT
    rotation_cache: Dict[float, RotationCacheEntry] = ctx["rotation_cache"]

    angle_norm = normalize_angle(angle_value)
    entry = rotation_cache.get(angle_norm)
    if entry is None:
        rotated_geom = _affinity.rotate(
            ctx["footprint_base"],
            angle_norm,
            origin=ctx["footprint_base_centroid"],
        )
        centroid_local = rotated_geom.centroid
        entry = RotationCacheEntry(
            wkb=rotated_geom.wkb,
            centroid=(centroid_local.x, centroid_local.y),
            bounds=rotated_geom.bounds,
        )
        rotation_cache[angle_norm] = entry
    else:
        rotated_geom = _wkb.loads(entry.wkb)

    offset_x = ctx["parcel_centroid"][0] + dx_value - entry.centroid[0]
    offset_y = ctx["parcel_centroid"][1] + dy_value - entry.centroid[1]
    translated_bounds = (
        entry.bounds[0] + offset_x,
        entry.bounds[1] + offset_y,
        entry.bounds[2] + offset_x,
        entry.bounds[3] + offset_y,
    )

    if not bounds_overlap(ctx["parcel_bounds"], translated_bounds, ctx["bounds_margin"]):
        return None

    footprint_translated = _affinity.translate(rotated_geom, xoff=offset_x, yoff=offset_y)
    if footprint_translated.is_empty:
        return None
    if not footprint_translated.is_valid:
        footprint_translated = footprint_translated.buffer(0)
    if footprint_translated.is_empty:
        return None

    parcel_prepared = ctx["parcel_prepared"]
    if parcel_prepared and not parcel_prepared.intersects(footprint_translated):
        return None

    rotated_front = rotate_vector(ctx["front_vector_base"], angle_norm)

    scores = compute_scores(
        ctx["parcel_geom"],
        footprint_translated,
        buildable=ctx["buildable"],
        buildable_prepared=ctx["buildable_prepared"],
        parcel_area=ctx["parcel_area"],
        roads_geom=ctx["roads_geom"],
        roads_raw=ctx["roads_raw"],
        parcel_info=ctx["parcel_info"],
        front_vector=rotated_front,
        parcel_major_angle=ctx["parcel_major_angle"],
    )

    if scores.get("disqualified"):
        return None

    composite = float(scores.get("composite_score", 0.0))
    if composite < ctx["min_composite"]:
        return None

    placement = {
        "rotation_deg": round(angle_norm, 3),
        "offset_x_m": round(float(dx_value), 3),
        "offset_y_m": round(float(dy_value), 3),
        "footprint_area_sqm": round(footprint_translated.area, 2),
        "scores": scores,
    }
    return placement


def fetch_roads(bounds: Tuple[float, float, float, float]) -> List[LineString]:
    global ROAD_FAILURE_COUNT, ROAD_BACKOFF_UNTIL, LAST_ROAD_FETCH, OVERPASS_INDEX, ROAD_MASTER_LINES, ROAD_MASTER_BOUNDS
    now = time.monotonic()
    if ROAD_BACKOFF_UNTIL and now < ROAD_BACKOFF_UNTIL:
        remaining = ROAD_BACKOFF_UNTIL - now
        logging.debug("Skipping road fetch (backing off for %.1fs).", remaining)
        return []
    if ROAD_MASTER_LINES and ROAD_MASTER_BOUNDS and bounds_contains(ROAD_MASTER_BOUNDS, bounds):
        return [
            line
            for line in ROAD_MASTER_LINES
            if bounds_overlap(bounds, line.bounds, margin=0.0)
        ]
    fetch_bounds = expand_bounds(bounds, pad=120.0)
    if ROAD_MASTER_BOUNDS:
        union_bounds = merge_bounds(ROAD_MASTER_BOUNDS, fetch_bounds)
        span_x = union_bounds[2] - union_bounds[0]
        span_y = union_bounds[3] - union_bounds[1]
        if span_x <= 6000 and span_y <= 6000:
            fetch_bounds = union_bounds
    minx, miny, maxx, maxy = fetch_bounds
    corners = [
        (minx, miny),
        (minx, maxy),
        (maxx, miny),
        (maxx, maxy),
    ]
    lons: List[float] = []
    lats: List[float] = []
    for xw, yw in corners:
        lon, lat = web_mercator_to_wgs84(xw, yw)
        lons.append(lon)
        lats.append(lat)
    south = min(lats)
    north = max(lats)
    west = min(lons)
    east = max(lons)
    query = (
        "[out:json][timeout:25];"
        f"(way['highway'~'^(motorway|trunk|primary|secondary|tertiary|residential|service|unclassified)$']"
        f"({south},{west},{north},{east}););"
        "out geom;"
    )

    wait = 1.0 - (now - LAST_ROAD_FETCH)
    if wait > 0:
        time.sleep(min(wait, 1.5))

    for attempt in range(len(OVERPASS_URLS)):
        url_index = (OVERPASS_INDEX + attempt) % len(OVERPASS_URLS)
        endpoint = OVERPASS_URLS[url_index]
        try:
            response = REQUEST_SESSION.post(endpoint, data={"data": query}, timeout=28)
            response.raise_for_status()
            payload = response.json()
            OVERPASS_INDEX = url_index
            LAST_ROAD_FETCH = time.monotonic()
            ROAD_FAILURE_COUNT = 0
            ROAD_BACKOFF_UNTIL = 0.0
            break
        except requests.RequestException as exc:
            ROAD_FAILURE_COUNT += 1
            logging.warning("Failed to fetch roads via %s: %s", endpoint, exc)
            continue
        except Exception as exc:  # noqa: BLE001
            ROAD_FAILURE_COUNT += 1
            logging.warning("Failed to fetch roads via %s: %s", endpoint, exc)
            continue
    else:
        backoff_seconds = min(60.0, 5.0 * max(1, ROAD_FAILURE_COUNT - ROAD_FAILURE_LIMIT + 1))
        if ROAD_FAILURE_COUNT >= ROAD_FAILURE_LIMIT:
            ROAD_BACKOFF_UNTIL = time.monotonic() + backoff_seconds
            logging.warning("Backing off road fetches for %.0f seconds after repeated failures.", backoff_seconds)
        return []

    lines: List[LineString] = []
    new_lines: List[LineString] = []
    for element in payload.get("elements", []):
        geometry = element.get("geometry")
        if not geometry:
            continue
        points: List[Tuple[float, float]] = []
        for node in geometry:
            lon = node.get("lon")
            lat = node.get("lat")
            if lon is None or lat is None:
                continue
            x_merc, y_merc = wgs84_to_web_mercator(float(lon), float(lat))
            points.append((x_merc, y_merc))
        if len(points) >= 2:
            new_lines.append(LineString(points))

    if new_lines:
        if ROAD_MASTER_LINES and ROAD_MASTER_BOUNDS:
            ROAD_MASTER_LINES.extend(new_lines)
            ROAD_MASTER_BOUNDS = merge_bounds(ROAD_MASTER_BOUNDS, fetch_bounds)
        else:
            ROAD_MASTER_LINES = list(new_lines)
            ROAD_MASTER_BOUNDS = fetch_bounds

    lines = [
        line
        for line in ROAD_MASTER_LINES
        if bounds_overlap(bounds, line.bounds, margin=0.0)
    ]
    return list(lines)


def compute_scores(
    parcel_geom: Polygon,
    footprint: Polygon,
    *,
    buildable: Polygon,
    buildable_prepared,
    parcel_area: float,
    roads_geom: Optional[MultiLineString],
    roads_raw: Sequence[LineString],
    parcel_info: Dict[str, object],
    front_vector: Tuple[float, float],
    parcel_major_angle: float,
) -> Dict[str, object]:
    scores: Dict[str, object] = {}
    footprint_area = footprint.area or 1.0

    outside_area = 0.0
    if buildable_prepared and not buildable_prepared.contains(footprint):
        try:
            outside_area = footprint.difference(buildable).area
        except Exception:
            outside_area = max(0.0, footprint_area - buildable.intersection(footprint).area)

    envelope_score = max(0.0, 1.0 - outside_area / footprint_area) * 100.0
    envelope_ok = outside_area <= 0.05
    scores["envelope_fit"] = 100.0 if envelope_ok else 0.0
    scores["envelope_outside_area_sqm"] = round(outside_area, 2)

    area_ratio = footprint_area / (parcel_area or 1.0)
    if 0.35 <= area_ratio <= 0.65:
        area_score = 100.0
    else:
        deviation = abs(area_ratio - 0.5)
        area_score = max(0.0, 100.0 - deviation * 200.0)
    scores["area_efficiency"] = round(area_score, 1)
    scores["area_ratio"] = round(area_ratio, 3)

    candidate_list: List[LineString] = []
    for road in roads_raw:
        if road is None or road.is_empty:
            continue
        try:
            if road.crosses(parcel_geom) or road.within(parcel_geom):
                continue
            if road.intersects(parcel_geom) and not road.touches(parcel_geom):
                continue
        except Exception:
            if road.intersects(parcel_geom) and not road.touches(parcel_geom):
                continue
        candidate_list.append(road)
    roads_union = unary_union(candidate_list) if candidate_list else None
    centroid = footprint.centroid

    if candidate_list:
        distance = min(footprint.boundary.distance(road) for road in candidate_list)
        if math.isinf(distance):
            distance = footprint.boundary.distance(roads_union) if roads_union else 0.0
        access_score = max(0.0, 100.0 - (distance * 5.0))
        scores["access_alignment"] = round(access_score, 1)
        scores["access_distance_m"] = round(distance, 2)
        scores["road_segments_considered"] = len(candidate_list)
    elif roads_union and not roads_union.is_empty:
        distance = footprint.boundary.distance(roads_union)
        access_score = max(0.0, 100.0 - (distance * 5.0))
        scores["access_alignment"] = round(access_score, 1)
        scores["access_distance_m"] = round(distance, 2)
        scores["road_segments_considered"] = len(getattr(roads_union, "geoms", []))
    else:
        scores["access_alignment"] = 50.0
        scores["access_distance_m"] = None
        scores["road_segments_considered"] = 0

    bbox_parcel = parcel_geom.minimum_rotated_rectangle
    bbox_footprint = footprint.minimum_rotated_rectangle

    footprint_major_angle = major_axis_angle(bbox_footprint)

    def angle_diff_deg(a: float, b: float) -> float:
        diff = abs(a - b) % 360
        if diff > 180:
            diff = 360 - diff
        return diff

    def symmetric_diff(a: float, b: float) -> float:
        diff = angle_diff_deg(a, b)
        if diff > 90:
            diff = 180 - diff
        return diff

    front_normal = normalize_vector(front_vector)
    front_tangent = normalize_vector(perpendicular(front_normal))
    front_tangent_angle = math.degrees(math.atan2(front_tangent[1], front_tangent[0]))
    front_normal_angle = math.degrees(math.atan2(front_normal[1], front_normal[0]))
    parcel_angle = parcel_major_angle
    footprint_diff = symmetric_diff(footprint_major_angle, parcel_angle)
    orientation_score = max(0.0, 100.0 - footprint_diff * (100.0 / 90.0))
    scores["orientation_alignment"] = round(orientation_score, 1)
    scores["orientation_delta_deg"] = round(footprint_diff, 1)

    shape_diff = symmetric_diff(front_tangent_angle, parcel_angle)
    shape_score = max(0.0, 100.0 - shape_diff * (100.0 / 90.0))
    scores["front_parcel_alignment"] = round(shape_score, 1)
    scores["front_parcel_delta_deg"] = round(shape_diff, 1)

    nearest_road_line: Optional[LineString] = None
    if candidate_list:
        nearest_road_line = min(candidate_list, key=lambda ln: footprint.distance(ln))

    front_road_orientation_score = 50.0
    front_visibility_score = 50.0
    front_road_segment = None
    road_vector = None
    road_normal = None
    visibility_vector = None
    if nearest_road_line is not None and nearest_road_line.length > 0:
        coords = list(nearest_road_line.coords)
        if len(coords) >= 2:
            best_segment = None
            min_seg_dist = float("inf")
            for i in range(len(coords) - 1):
                segment = LineString([coords[i], coords[i + 1]])
                dist = footprint.distance(segment)
                if dist < min_seg_dist:
                    min_seg_dist = dist
                    best_segment = segment
            if best_segment is None:
                best_segment = nearest_road_line
            seg_coords = list(best_segment.coords)
            road_vec = normalize_vector((seg_coords[-1][0] - seg_coords[0][0], seg_coords[-1][1] - seg_coords[0][1]))
            road_vector = road_vec
            road_angle = math.degrees(math.atan2(road_vec[1], road_vec[0]))
            road_diff = symmetric_diff(front_tangent_angle, road_angle)
            front_road_orientation_score = max(0.0, 100.0 - road_diff * (100.0 / 90.0))
            front_road_segment = [
                [float(seg_coords[0][0]), float(seg_coords[0][1])],
                [float(seg_coords[-1][0]), float(seg_coords[-1][1])],
            ]
            road_normal = normalize_vector(perpendicular(road_vec))

        nearest_point_on_road = nearest_road_line.interpolate(nearest_road_line.project(centroid))
        vector_to_road = normalize_vector((nearest_point_on_road.x - centroid.x, nearest_point_on_road.y - centroid.y))
        visibility_vector = vector_to_road
        road_facing_diff = symmetric_diff(
            front_normal_angle,
            math.degrees(math.atan2(vector_to_road[1], vector_to_road[0])),
        )
        front_visibility_score = max(0.0, 100.0 - road_facing_diff * (100.0 / 90.0))

    scores["front_road_alignment"] = round(front_road_orientation_score, 1)
    scores["front_visibility"] = round(front_visibility_score, 1)
    scores["front_parallel_vector"] = [round(front_tangent[0], 4), round(front_tangent[1], 4)]
    scores["front_outward_vector"] = [round(front_normal[0], 4), round(front_normal[1], 4)]
    scores["front_reference_point"] = [round(centroid.x, 3), round(centroid.y, 3)]
    if front_road_segment:
        scores["front_road_segment"] = front_road_segment
    if road_vector:
        scores["front_road_direction"] = [round(road_vector[0], 4), round(road_vector[1], 4)]
    if visibility_vector:
        scores["front_visibility_vector"] = [round(visibility_vector[0], 4), round(visibility_vector[1], 4)]
    if road_normal:
        scores["front_road_normal"] = [round(road_normal[0], 4), round(road_normal[1], 4)]

    zoning = str(parcel_info.get("official_zoning") or "").upper().strip()
    parcel_use_match = 100.0 if zoning and zoning.startswith("C") else 75.0
    if zoning.startswith("R"):
        parcel_use_match = 90.0
    if zoning:
        scores["zoning_compatibility"] = round(parcel_use_match, 1)
        scores["zoning_code"] = zoning

    composite = (
        area_score * 0.25
        + scores["access_alignment"] * 0.2
        + front_road_orientation_score * 0.2
        + front_visibility_score * 0.2
        + shape_score * 0.15
    )

    disqualifiers: List[str] = []
    if not envelope_ok:
        composite = 0.0
        disqualifiers.append("envelope_violation")
        scores.pop("envelope_fit", None)
    scores["composite_score"] = round(composite, 1)
    scores["disqualified"] = ",".join(disqualifiers) if disqualifiers else None

    return scores


def prepare_footprint(
    dxf_path: Path,
    *,
    auto_front: bool = False,
    front_angle: Optional[float] = None,
    front_vector_override: Optional[Tuple[float, float]] = None,
) -> Tuple[FootprintProfile, Optional[Tuple[float, float]]]:
    polygons_raw, units_code, extents, paths_raw, raw_lines = load_dxf_polygons(dxf_path)
    scale_m_per_unit = calculate_unit_scale(units_code)
    polygons_scaled = normalize_polygons(polygons_raw, scale_m_per_unit)
    paths_scaled = normalize_paths(paths_raw, scale_m_per_unit)
    lines_scaled = normalize_lines(raw_lines, scale_m_per_unit)

    if polygons_scaled:
        base_candidate = max(polygons_scaled, key=lambda p: p.area)
        shrinked = shrinkwrap_polygon(base_candidate, lines_scaled)
        if not shrinked.is_valid:
            shrinked = shrinked.buffer(0)
        if shrinked.is_empty or shrinked.area <= 0:
            raise RuntimeError("Shrink-wrapped footprint is empty.")
        logging.info("Using largest closed DXF footprint (%.2f m²).", shrinked.area)
        front_choice: Optional[Tuple[float, float]] = None
        if front_vector_override is not None:
            front_choice = normalize_vector(front_vector_override)
        elif front_angle is not None:
            front_choice = vector_from_angle(front_angle)
        elif auto_front:
            auto_angle = major_axis_angle(shrinked)
            front_choice = vector_from_angle(auto_angle)

        if front_choice is not None:
            front_vector = front_choice
        elif tk is not None and tk._default_root is not None:  # type: ignore[attr-defined]
            # GUI mode: defer interactive prompts to the main thread via the app
            front_vector = None
        else:
            front_vector = prompt_front_direction(shrinked)
    elif paths_scaled:
        logging.info("Launching interactive DXF bounds selector.")
        shrinked, front_vector = interactive_bounds_selector(paths_scaled, lines_scaled)
        logging.info("Interactive footprint confirmed (%.2f m²).", shrinked.area)
    else:
        raise RuntimeError(
            "DXF does not contain closed polygons or drawable segments; unable to derive footprint."
        )

    centroid = shrinked.centroid
    bounds = shrinked.bounds
    span = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
    profile = FootprintProfile(
        geometry=shrinked,
        centroid=(centroid.x, centroid.y),
        area=shrinked.area,
        span=span,
    )
    return profile, front_vector


def load_footprint_from_json(json_path: Path) -> FootprintProfile:
    data = json.loads(json_path.read_text())
    points = data.get("points")
    if not points or len(points) < 3:
        raise ValueError("Footprint JSON must provide at least three points.")
    polygon = Polygon(points)
    if not polygon.is_valid:
        polygon = polygon.buffer(0)
    if polygon.is_empty:
        raise ValueError("Footprint polygon is empty after cleaning.")
    centroid = polygon.centroid
    bounds = polygon.bounds
    span = max(bounds[2] - bounds[0], bounds[3] - bounds[1])
    return FootprintProfile(
        geometry=polygon,
        centroid=(centroid.x, centroid.y),
        area=polygon.area,
        span=span,
    )


def prepare_rotations(profile: FootprintProfile, rotation_step: float, full_rotation: bool) -> List[RotatedFootprint]:
    if rotation_step <= 0:
        raise ValueError("Rotation step must be positive.")
    limit = 360.0 if full_rotation else 180.0
    angles = np.arange(0.0, limit, rotation_step)
    rotations: List[RotatedFootprint] = []
    for angle in angles:
        rotated = affinity.rotate(profile.geometry, float(angle), origin=profile.centroid)
        centroid = rotated.centroid
        rotations.append(
            RotatedFootprint(
                angle=float(angle),
                geometry=rotated,
                centroid=(centroid.x, centroid.y),
                bounds=rotated.bounds,
            )
        )
    return rotations


def compute_offset_config(
    profile: FootprintProfile,
    buildable: Polygon,
    *,
    offset_step_scale: float,
    auto_offset_scale: float,
    offset_step_value: Optional[float],
    offset_range_value: Optional[float],
    auto_offset_enabled: bool,
) -> Tuple[List[float], float, float, float]:
    footprint_span = max(profile.span, 1.0)
    raw_step = offset_step_value if offset_step_value and offset_step_value > 0 else footprint_span * offset_step_scale
    offset_step = max(0.5, raw_step)
    buildable_bounds = buildable.bounds
    buildable_width = max(0.1, buildable_bounds[2] - buildable_bounds[0])
    buildable_height = max(0.1, buildable_bounds[3] - buildable_bounds[1])
    max_extent = max(buildable_width, buildable_height)
    if auto_offset_enabled:
        offset_range = min(max_extent / 2.0 + offset_step, footprint_span * auto_offset_scale)
    else:
        if offset_range_value is None or offset_range_value <= 0:
            offset_range = max(footprint_span, offset_step * 4.0)
        else:
            offset_range = offset_range_value
    offset_range = max(offset_range, offset_step)
    offset_values = np.arange(-offset_range, offset_range + offset_step, offset_step)
    offsets = sorted({round(float(val), 3) for val in offset_values})
    if not offsets:
        offsets = [0.0]
    if 0.0 not in offsets:
        offsets.append(0.0)
    offsets = sorted(offsets)
    bounds_margin = max(offset_step * 2.0, offset_range * 0.3, 3.0)
    return offsets, offset_step, offset_range, bounds_margin


def evaluate_parcel(
    parcel: ParcelFeature,
    parcel_info: Dict[str, object],
    footprint_profile: FootprintProfile,
    rotations: Sequence[RotatedFootprint],
    front_vector: Tuple[float, float],
    *,
    setback: float,
    offset_step_scale: float,
    auto_offset_scale: float,
    offset_step_value: Optional[float],
    offset_range_value: Optional[float],
    auto_offset_enabled: bool,
    min_composite: float,
    road_fetcher: Optional[Callable[[Tuple[float, float, float, float]], List[LineString]]] = None,
    skip_roads: bool = False,
    score_workers: int = 1,
    progress_writer: Optional[
        Callable[[Dict[str, object], Optional[Dict[str, object]], List[Dict[str, object]]], None]
    ] = None,
    event_recorder: Optional[EventRecorder] = None,
) -> ParcelEvaluationResult:
    WARNING_COUNTS[ORIENTED_WARNING_KEY] = 0
    install_warning_capture()

    parcel_geom = parcel.geometry
    parcel_area = parcel_geom.area or 1.0
    parcel_centroid = parcel_geom.centroid
    buildable = parcel_geom
    if setback > 0:
        try:
            candidate = parcel_geom.buffer(-setback)
            if not candidate.is_empty and candidate.area > 0:
                buildable = candidate
        except ValueError:
            pass
    parcel_major_angle = major_axis_angle(parcel_geom.minimum_rotated_rectangle)

    offsets, offset_step, offset_range, bounds_margin = compute_offset_config(
        footprint_profile,
        buildable,
        offset_step_scale=offset_step_scale,
        auto_offset_scale=auto_offset_scale,
        offset_step_value=offset_step_value,
        offset_range_value=offset_range_value,
        auto_offset_enabled=auto_offset_enabled,
    )

    road_pad = offset_range + offset_step + 40.0
    roads: List[LineString] = []
    roads_geom = None
    if not skip_roads:
        road_bounds = unary_bounds([parcel_geom], pad=road_pad)
        fetch_cb = road_fetcher or fetch_roads
        try:
            roads = fetch_cb(road_bounds)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Road fetch failed for %s: %s", parcel.parcel_id, exc)
            roads = []
        roads_geom = unary_union(roads) if roads else None
        if isinstance(roads_geom, LineString):
            roads_geom = MultiLineString([roads_geom])

    angles = [normalize_angle(rotation.angle) for rotation in rotations]
    tasks: List[Tuple[float, float, float]] = [
        (angle, dx, dy) for angle in angles for dx in offsets for dy in offsets
    ]

    buildable_wkb = buildable.wkb if not buildable.is_empty else None
    roads_geom_wkb = None
    if roads_geom is not None and not roads_geom.is_empty:
        roads_geom_wkb = roads_geom.wkb

    context_payload = {
        "parcel_wkb": parcel_geom.wkb,
        "parcel_centroid": (parcel_centroid.x, parcel_centroid.y),
        "parcel_area": parcel_area,
        "parcel_bounds": parcel_geom.bounds,
        "bounds_margin": bounds_margin,
        "buildable_wkb": buildable_wkb,
        "roads_geom_wkb": roads_geom_wkb,
        "roads_wkbs": [road.wkb for road in roads],
        "footprint_wkb": footprint_profile.geometry.wkb,
        "footprint_centroid": footprint_profile.centroid,
        "parcel_info": parcel_info,
        "front_vector": front_vector,
        "parcel_major_angle": parcel_major_angle,
        "min_composite": min_composite,
    }

    placements: List[Dict[str, object]] = []
    best_placement: Optional[Dict[str, object]] = None
    best_composite = -math.inf
    best_geometry: Optional[Polygon] = None

    def emit_progress() -> None:
        if not progress_writer:
            return
        try:
            summary_snapshot = summarize_parcel_result(
                parcel,
                placements,
                offset_step,
                offset_range,
                best_placement,
            )
            best_geojson = mapping(best_geometry) if best_geometry is not None else None
            progress_writer(summary_snapshot, best_geojson, placements)
        except Exception as exc:  # noqa: BLE001
            logging.debug("Progress writer failed for %s: %s", parcel.parcel_id, exc)

    placement_sequence = 0

    def record_placement(placement: Dict[str, object]) -> None:
        nonlocal best_placement, best_composite, best_geometry, placement_sequence
        geometry: Optional[Polygon] = None
        try:
            geometry = placement_to_geometry(placement, footprint_profile, parcel_geom)
        except Exception as exc:  # noqa: BLE001
            logging.debug("Failed to derive placement geometry for %s: %s", parcel.parcel_id, exc)
        if geometry is None:
            geometry = buildable
        placement["footprint_geojson"] = mapping(geometry)
        placements.append(placement)
        placement_sequence += 1
        composite_value = float(placement["scores"].get("composite_score", 0.0))
        is_best = False
        if composite_value > best_composite:
            best_composite = composite_value
            best_placement = placement
            best_geometry = geometry
            is_best = True
        if event_recorder:
            event_payload = {
                "parcel_id": parcel.parcel_id,
                "index": placement_sequence,
                "rotation_deg": placement.get("rotation_deg"),
                "offset_x_m": placement.get("offset_x_m"),
                "offset_y_m": placement.get("offset_y_m"),
                "composite_score": placement["scores"].get("composite_score"),
                "is_best": is_best,
                "footprint_geojson": placement["footprint_geojson"],
            }
            event_recorder.emit("placement_scored", event_payload)
            if is_best:
                best_payload = {
                    "parcel_id": parcel.parcel_id,
                    "index": placement_sequence,
                    "composite_score": placement["scores"].get("composite_score"),
                    "footprint_geojson": placement["footprint_geojson"],
                }
                event_recorder.emit("best_updated", best_payload)
        emit_progress()

    use_pool = score_workers > 1 and len(tasks) > 0
    if use_pool:
        logging.info(
            "Evaluating %d candidate poses for %s using %d worker%s.",
            len(tasks),
            parcel.parcel_id,
            score_workers,
            "" if score_workers == 1 else "s",
        )
        try:
            with ProcessPoolExecutor(
                max_workers=score_workers,
                initializer=_init_worker,
                initargs=(context_payload,),
            ) as executor:
                futures = {executor.submit(_evaluate_pose_process, task): task for task in tasks}
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        placement = future.result()
                    except Exception as exc:  # noqa: BLE001
                        logging.error(
                            "Pose evaluation failed for %s at angle %.2f°, dx %.2f, dy %.2f: %s",
                            parcel.parcel_id,
                            task[0],
                            task[1],
                            task[2],
                            exc,
                        )
                        continue
                    if placement:
                        record_placement(placement)
        except Exception as exc:  # noqa: BLE001
            logging.error(
                "Process pool evaluation for %s failed (%s); retrying with a single worker.",
                parcel.parcel_id,
                exc,
            )
            use_pool = False

    if not use_pool and tasks:
        _init_worker(context_payload)
        try:
            for task in tasks:
                placement = _evaluate_pose_process(task)
                if placement:
                    record_placement(placement)
        finally:
            WORKER_CONTEXT.clear()

    if best_placement and best_geometry is None:
        try:
            best_geometry = placement_to_geometry(best_placement, footprint_profile, parcel_geom)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to reconstruct best geometry for %s (%s).", parcel.parcel_id, exc)
            best_geometry = None

    summary = summarize_parcel_result(parcel, placements, offset_step, offset_range, best_placement)

    disqualified = not placements
    return ParcelEvaluationResult(
        parcel=parcel,
        placements=placements,
        summary=summary,
        best_placement=best_placement,
        best_geometry=best_geometry,
        buildable=buildable,
        roads=list(roads),
        disqualified=disqualified,
    )


def summarize_parcel_result(
    parcel: ParcelFeature,
    placements: List[Dict[str, object]],
    offset_step: float,
    offset_range: float,
    best_placement: Optional[Dict[str, object]],
) -> Dict[str, object]:
    summary: Dict[str, object] = {
        "parcel_id": parcel.parcel_id,
        "address": parcel.address,
        "placements_evaluated": len(placements),
        "offset_step_m": round(offset_step, 3),
        "offset_range_m": round(offset_range, 3),
        "viable_count": len(placements),
    }
    composites = [float(p["scores"].get("composite_score", 0.0)) for p in placements]
    if composites:
        summary["average_composite"] = round(sum(composites) / len(composites), 1)
        summary["max_composite"] = round(max(composites), 1)
    else:
        summary["average_composite"] = 0.0
        summary["max_composite"] = 0.0
    if best_placement:
        summary["top_rotation_deg"] = best_placement["rotation_deg"]
        summary["top_offset_x_m"] = best_placement["offset_x_m"]
        summary["top_offset_y_m"] = best_placement["offset_y_m"]
        summary["top_composite"] = best_placement["scores"]["composite_score"]
    else:
        summary["top_composite"] = 0.0
    return summary


def plot_best_fit(
    result: ParcelEvaluationResult,
    parcel_info: Dict[str, object],
    output_path: Path,
) -> None:
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg

    fig = Figure(figsize=(7.5, 7.5))
    ax = fig.add_subplot(111)

    parcel_geom = result.parcel.geometry
    used_labels: set[str] = set()
    for poly in iter_polygons(parcel_geom):
        px, py = poly.exterior.xy
        label = "Parcel" if "Parcel" not in used_labels else None
        ax.fill(px, py, color="#e2e8f0", alpha=0.6, label=label)
        ax.plot(px, py, color="#64748b", linewidth=1.1)
        if label:
            used_labels.add(label)

    if not result.buildable.equals(parcel_geom):
        for poly in iter_polygons(result.buildable):
            bx, by = poly.exterior.xy
            label = "Buildable" if "Buildable" not in used_labels else None
            ax.plot(bx, by, color="#0ea5e9", linestyle="--", linewidth=1.0, alpha=0.7, label=label)
            if label:
                used_labels.add(label)

    if result.roads:
        for road in result.roads:
            rx, ry = road.xy
            ax.plot(rx, ry, color="#94a3b8", linewidth=0.8, alpha=0.6)

    if result.best_geometry is not None:
        for poly in iter_polygons(result.best_geometry):
            fx, fy = poly.exterior.xy
            label = "Best placement" if "Best placement" not in used_labels else None
            ax.fill(fx, fy, color="#f97316", alpha=0.35, label=label)
            ax.plot(fx, fy, color="#c2410c", linewidth=1.6)
            if label:
                used_labels.add(label)

    bounds = unary_bounds([parcel_geom] + ([result.best_geometry] if result.best_geometry else []), pad=5.0)
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect("equal")
    ax.set_xlabel("X (Web Mercator m)")
    ax.set_ylabel("Y (Web Mercator m)")
    ax.set_title(f"Best fit – {result.parcel.parcel_id}")

    summary = result.summary
    info_lines = [
        f"Placements: {summary['placements_evaluated']}",
        f"Avg composite: {summary.get('average_composite')}",
        f"Top composite: {summary.get('max_composite')}",
        f"Offset step: {summary['offset_step_m']} m",
        f"Offset range: {summary['offset_range_m']} m",
    ]
    zone = parcel_info.get("official_zoning")
    if zone:
        info_lines.append(f"Zoning: {zone}")
    ax.text(
        0.02,
        0.98,
        "\n".join(str(item) for item in info_lines),
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        bbox=dict(facecolor="white", alpha=0.8, edgecolor="none"),
    )

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(loc="lower right", fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    FigureCanvasAgg(fig).draw()
    fig.savefig(output_path, dpi=220)
    logging.info("Saved parcel snapshot to %s", output_path)


def plot_composite_overlay(
    result: ParcelEvaluationResult,
    footprint_profile: FootprintProfile,
    output_path: Path,
) -> None:
    if not result.placements:
        logging.info("Skipping composite overlay for %s (no placements).", result.parcel.parcel_id)
        return

    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg

    fig = Figure(figsize=(7.5, 7.5))
    ax = fig.add_subplot(111)

    parcel_geom = result.parcel.geometry
    for poly in iter_polygons(parcel_geom):
        px, py = poly.exterior.xy
        ax.fill(px, py, color="#e5e7eb", alpha=0.6)
        ax.plot(px, py, color="#9ca3af", linewidth=1.0)

    for placement in result.placements:
        transformed = placement_to_geometry(placement, footprint_profile, parcel_geom)
        for poly in iter_polygons(transformed):
            tx, ty = poly.exterior.xy
            ax.fill(tx, ty, color="#4b5563", alpha=0.12)
            ax.plot(tx, ty, color="#4b5563", linewidth=0.5, alpha=0.35)

    bounds = unary_bounds([parcel_geom], pad=result.summary["offset_range_m"] + 5.0)
    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect("equal")
    ax.set_title(f"Composite placements – {result.parcel.parcel_id}")
    ax.set_xlabel("X (Web Mercator m)")
    ax.set_ylabel("Y (Web Mercator m)")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    FigureCanvasAgg(fig).draw()
    fig.savefig(output_path, dpi=220)
    logging.info("Saved composite overlay to %s", output_path)


def write_parcel_outputs(
    result: ParcelEvaluationResult,
    parcel_info: Dict[str, object],
    footprint_profile: FootprintProfile,
    output_root: Path,
    parcel_callback: Optional[Callable[[ParcelEvaluationResult, Path], None]] = None,
    *,
    render_best: bool,
    render_composite: bool,
) -> Path:
    parcel_slug = slugify(result.parcel.parcel_id)
    parcel_dir = output_root / "parcels" / parcel_slug
    parcel_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "parcel": parcel_detail_record(result.parcel, parcel_info),
        "summary": result.summary,
        "placements": result.placements,
    }
    if result.best_geometry is not None:
        payload["best_footprint_geojson"] = mapping(result.best_geometry)

    json_path = parcel_dir / "placements.json"
    with json_path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
    logging.info("Wrote placement JSON to %s", json_path)

    if render_best and result.best_geometry is not None:
        plot_best_fit(result, parcel_info, parcel_dir / "best.png")
    if render_composite:
        plot_composite_overlay(result, footprint_profile, parcel_dir / "composite.png")
    if parcel_callback is not None and render_composite:
        try:
            parcel_callback(result, parcel_dir)
        except Exception as exc:  # noqa: BLE001
            logging.debug("Parcel callback failed for %s: %s", result.parcel.parcel_id, exc)
    return parcel_dir


def write_best_parcels_snapshot(parcels_root: Path, results: Dict[str, ParcelEvaluationResult]) -> None:
    parcels_root.mkdir(parents=True, exist_ok=True)
    best_path = parcels_root / "best_parcels.json"
    entries: List[Dict[str, object]] = []
    for result in results.values():
        summary = result.summary
        entries.append(
            {
                "parcel_id": result.parcel.parcel_id,
                "address": summary.get("address") or result.parcel.address,
                "average_composite": float(summary.get("average_composite") or 0.0),
                "max_composite": float(summary.get("max_composite") or 0.0),
                "viable_count": int(summary.get("viable_count") or 0),
                "top_rotation_deg": summary.get("top_rotation_deg"),
                "top_offset_x_m": summary.get("top_offset_x_m"),
                "top_offset_y_m": summary.get("top_offset_y_m"),
            }
        )
    entries.sort(key=lambda item: item.get("average_composite", 0.0), reverse=True)
    with best_path.open("w", encoding="utf-8") as stream:
        json.dump(entries, stream, indent=2)


def plot_cycle(
    cycle_index: int,
    target: ParcelFeature,
    visited: Sequence[ParcelFeature],
    newest: Sequence[ParcelFeature],
    results: Dict[str, ParcelEvaluationResult],
    roads: Sequence[LineString],
    footprint_profile: FootprintProfile,
    output_dir: Path,
) -> Path:
    avg_scores: Dict[str, float] = {}
    max_avg = 0.0
    min_avg = float("inf")
    for parcel in visited:
        result = results.get(parcel.parcel_id)
        avg_val = 0.0
        if result:
            avg_raw = result.summary.get("average_composite")
            if avg_raw is not None:
                avg_val = float(avg_raw)
        avg_scores[parcel.parcel_id] = avg_val
        if avg_val > max_avg:
            max_avg = avg_val
        if 0.0 < avg_val < min_avg:
            min_avg = avg_val

    if min_avg == float("inf") or min_avg >= max_avg:
        min_avg = 0.0

    def color_for_parcel(parcel_id: str) -> str:
        base = (209, 213, 219)  # #d1d5db
        peak = (20, 83, 45)  # #14532d
        avg_val = max(0.0, avg_scores.get(parcel_id, 0.0))
        if max_avg <= 0 or avg_val <= 0:
            r, g, b = base
        else:
            denom = max(max_avg - min_avg, 1e-6)
            t = min(1.0, max(0.0, (avg_val - min_avg) / denom))
            r = round(base[0] + (peak[0] - base[0]) * t)
            g = round(base[1] + (peak[1] - base[1]) * t)
            b = round(base[2] + (peak[2] - base[2]) * t)
        return f"#{r:02x}{g:02x}{b:02x}"

    geoms: List[BaseGeometry] = [target.geometry] + [p.geometry for p in visited]
    bounds = unary_bounds(geoms, pad=20.0)
    minx, miny, maxx, maxy = bounds

    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg

    fig = Figure(figsize=(8, 8))
    ax = fig.add_subplot(111)

    try:
        import contextily as ctx

        try:
            ctx.add_basemap(
                ax,
                crs="EPSG:3857",
                source=ctx.providers.OpenStreetMap.Mapnik,
                attribution_size=6,
            )
        except Exception as exc:  # noqa: BLE001
            logging.debug("Basemap overlay failed: %s", exc)
    except Exception:
        logging.debug("Contextily not available; skipping basemap overlay.")

    for segment in roads:
        xs, ys = segment.xy
        ax.plot(xs, ys, color="#b0b0b0", linewidth=0.8, alpha=0.75)

    for parcel in visited:
        color = color_for_parcel(parcel.parcel_id)
        for poly in iter_polygons(parcel.geometry):
            x, y = poly.exterior.xy
            ax.fill(x, y, color=color, alpha=0.6)
            ax.plot(x, y, color=color, linewidth=1.0)

    for parcel in newest:
        x, y = parcel.geometry.exterior.xy
        ax.plot(x, y, color="#ea580c", linewidth=1.6)

    target_color = color_for_parcel(target.parcel_id)
    for poly in iter_polygons(target.geometry):
        tx, ty = poly.exterior.xy
        ax.fill(tx, ty, color=target_color, alpha=0.65)
        ax.plot(tx, ty, color="#1e293b", linewidth=2.4)

    for parcel in visited:
        centroid = parcel.geometry.centroid
        result = results.get(parcel.parcel_id)
        if result:
            for placement in result.placements:
                geometry = placement_to_geometry(placement, footprint_profile, parcel.geometry)
                for poly in iter_polygons(geometry):
                    px_foot, py_foot = poly.exterior.xy
                    ax.fill(px_foot, py_foot, color="#475569", alpha=0.12)
                    ax.plot(px_foot, py_foot, color="#475569", linewidth=0.6, alpha=0.4)
        if result:
            viable = len(result.placements)
            top_score = result.summary.get("max_composite")
            top_value = 0.0 if top_score is None else float(top_score)
            avg_score = float(result.summary.get("average_composite") or 0.0)
            label = f"{viable}, {top_value:.1f}, {avg_score:.1f}"
        else:
            label = "0, 0.0, 0.0"
        ax.text(
            centroid.x,
            centroid.y,
            label,
            fontsize=7,
            color="#111827",
            ha="center",
            va="center",
            bbox=dict(boxstyle="round,pad=0.15", facecolor="#ffffffcc", edgecolor="none"),
        )
        if result and result.best_geometry is not None:
            for poly in iter_polygons(result.best_geometry):
                fx, fy = poly.exterior.xy
                ax.plot(fx, fy, color="#ef4444", linewidth=1.2, alpha=0.9)

    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    ax.set_aspect("equal")
    ax.set_title(f"Crawl cycle {cycle_index}")
    ax.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)

    legend_handles = [
        Patch(facecolor="#d1d5db", edgecolor="#9ca3af", label="Lower average"),
        Patch(facecolor="#14532d", edgecolor="#14532d", label="Higher average"),
        Patch(facecolor="none", edgecolor="#ea580c", linewidth=1.6, label="Current wave"),
        Patch(facecolor="none", edgecolor="#ef4444", linewidth=1.2, label="Top placement"),
        Patch(facecolor="none", edgecolor="#1e293b", linewidth=2.4, label="Subject outline"),
    ]
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8)

    output_dir.mkdir(parents=True, exist_ok=True)
    cycle_path = output_dir / f"cycle_{cycle_index:03d}.png"
    fig.tight_layout()
    FigureCanvasAgg(fig).draw()
    fig.savefig(cycle_path, dpi=200)
    logging.info("Saved cycle %d snapshot to %s", cycle_index, cycle_path)
    return cycle_path


def write_cycle_json(
    cycle_index: int,
    visited: Sequence[ParcelFeature],
    results: Dict[str, ParcelEvaluationResult],
    output_dir: Path,
) -> None:
    entries: List[Dict[str, object]] = []
    for parcel in visited:
        result = results.get(parcel.parcel_id)
        entry: Dict[str, object] = {
            "parcel_id": parcel.parcel_id,
            "address": parcel.address,
        }
        if result:
            entry["summary"] = result.summary
        entries.append(entry)

    payload = {
        "cycle": cycle_index,
        "parcels": entries,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"cycle_{cycle_index:03d}.json"
    with json_path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
    logging.info("Saved cycle %d data to %s", cycle_index, json_path)


def _process_seed(
    seed: ParcelFeature,
    buffer_meters: float,
    max_neighbors: int,
    token: Optional[str],
    visited_snapshot: set[str],
) -> Tuple[ParcelFeature, List[ParcelFeature], int, float, float]:
    seed_centroid = seed.geometry.centroid
    candidate_ids: Dict[str, Tuple[float, ParcelFeature]] = {}
    attempts = 0
    current_buffer = buffer_meters
    total_raw_candidates = 0

    while attempts < 4 and len(candidate_ids) < max_neighbors:
        try:
            neighbors = fetch_neighbor_parcels(
                seed,
                buffer_meters=current_buffer,
                token=token,
                max_neighbors=max_neighbors,
                include_target=False,
            )
        except Exception as exc:  # noqa: BLE001
            logging.warning("Neighbor fetch failed for %s: %s", seed.parcel_id, exc)
            break
        total_raw_candidates += len(neighbors)
        for neighbor in neighbors:
            neighbor_key = neighbor.parcel_id
            if neighbor_key in visited_snapshot or neighbor_key == seed.parcel_id:
                continue
            if neighbor_key in candidate_ids:
                continue
            distance = seed_centroid.distance(neighbor.geometry.centroid)
            candidate_ids[neighbor_key] = (distance, neighbor)
        if len(candidate_ids) < 2:
            current_buffer *= 1.75
            attempts += 1
        else:
            break

    sorted_candidates = [parcel for _, parcel in sorted(candidate_ids.values(), key=lambda item: item[0])]
    return seed, sorted_candidates, total_raw_candidates, buffer_meters, current_buffer


def evaluate_and_record(
    parcel: ParcelFeature,
    parcel_info: Dict[str, object],
    *,
    footprint_profile: FootprintProfile,
    rotations: Sequence[RotatedFootprint],
    front_vector: Tuple[float, float],
    output_root: Path,
    results: Dict[str, ParcelEvaluationResult],
    setback: float,
    offset_step_scale: float,
    auto_offset_scale: float,
    offset_step_value: Optional[float],
    offset_range_value: Optional[float],
    auto_offset_enabled: bool,
    min_composite: float,
    parcel_callback: Optional[Callable[[ParcelEvaluationResult, Path], None]],
    render_best: bool,
    render_composite: bool,
    road_fetcher: Optional[Callable[[Tuple[float, float, float, float]], List[LineString]]],
    skip_roads: bool,
    score_workers: int,
    event_recorder: Optional[EventRecorder] = None,
) -> None:
    parcel_slug = slugify(parcel.parcel_id)
    parcel_dir = output_root / "parcels" / parcel_slug
    parcel_dir.mkdir(parents=True, exist_ok=True)
    parcel_detail = parcel_detail_record(parcel, parcel_info)

    def write_progress(
        summary: Dict[str, object],
        best_geojson: Optional[Dict[str, object]],
        placements: List[Dict[str, object]],
    ) -> None:
        payload = {
            "parcel": parcel_detail,
            "summary": summary,
            "placements": placements,
        }
        if best_geojson:
            payload["best_footprint_geojson"] = best_geojson
        tmp_path = parcel_dir / "placements.partial.json"
        final_path = parcel_dir / "placements.json"
        with tmp_path.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2)
        tmp_path.replace(final_path)
        if event_recorder:
            event_recorder.emit(
                "parcel_progress",
                {
                    "parcel_id": parcel.parcel_id,
                    "placements": len(placements),
                    "best_composite": summary.get("top_composite"),
                },
            )

    # seed a stub so the parcel boundary appears immediately
    write_progress(
        {
            "parcel_id": parcel.parcel_id,
            "address": parcel.address,
            "placements_evaluated": 0,
            "offset_step_m": None,
            "offset_range_m": None,
            "viable_count": 0,
            "average_composite": 0.0,
            "max_composite": 0.0,
            "top_composite": 0.0,
        },
        None,
        [],
    )
    if event_recorder:
        event_recorder.emit(
            "parcel_started",
            {
                "parcel_id": parcel.parcel_id,
                "parcel": parcel_detail,
            },
        )
    try:
        result = evaluate_parcel(
            parcel,
            parcel_info,
            footprint_profile,
            rotations,
            front_vector,
            setback=setback,
            offset_step_scale=offset_step_scale,
            auto_offset_scale=auto_offset_scale,
            offset_step_value=offset_step_value,
            offset_range_value=offset_range_value,
            auto_offset_enabled=auto_offset_enabled,
            min_composite=min_composite,
            road_fetcher=road_fetcher,
            skip_roads=skip_roads,
            score_workers=score_workers,
            progress_writer=write_progress,
            event_recorder=event_recorder,
        )
    except Exception as exc:  # noqa: BLE001
        logging.error("Evaluation failed for %s: %s", parcel.parcel_id, exc)
        if event_recorder:
            event_recorder.emit("parcel_failed", {"parcel_id": parcel.parcel_id, "error": str(exc)})
        return

    results[parcel.parcel_id] = result
    parcel_dir = write_parcel_outputs(
        result,
        parcel_info,
        footprint_profile,
        output_root,
        parcel_callback=parcel_callback,
        render_best=render_best,
        render_composite=render_composite,
    )
    if event_recorder:
        event_recorder.emit(
            "parcel_completed",
            {
                "parcel_id": parcel.parcel_id,
                "placements": len(result.placements),
                "top_composite": result.summary.get("top_composite"),
            },
        )


def crawl_parcels(
    address: str,
    *,
    footprint_profile: FootprintProfile,
    rotations: Sequence[RotatedFootprint],
    front_vector: Tuple[float, float],
    max_cycles: int,
    buffer_meters: float,
    max_neighbors: int,
    workers: int,
    output_dir: Path,
    token: Optional[str],
    setback: float,
    offset_step_scale: float,
    auto_offset_scale: float,
    offset_step_value: Optional[float],
    offset_range_value: Optional[float],
    auto_offset_enabled: bool,
    min_composite: float,
    cycle_callback: Optional[Callable[[int, Path, int], None]] = None,
    progress_callback: Optional[Callable[[str, Dict[str, int]], None]] = None,
    parcel_callback: Optional[Callable[[ParcelEvaluationResult, Path], None]] = None,
    render_cycle: bool = True,
    render_best: bool = True,
    render_composite: bool = True,
    skip_roads: bool = False,
    score_workers: int = 1,
) -> None:
    global ROAD_FAILURE_COUNT, ROAD_BACKOFF_UNTIL, LAST_ROAD_FETCH, OVERPASS_INDEX, ROAD_MASTER_LINES, ROAD_MASTER_BOUNDS
    ROAD_FAILURE_COUNT = 0
    ROAD_BACKOFF_UNTIL = 0.0
    LAST_ROAD_FETCH = 0.0
    OVERPASS_INDEX = 0
    ROAD_MASTER_LINES = []
    ROAD_MASTER_BOUNDS = None
    output_dir.mkdir(parents=True, exist_ok=True)
    cycles_output = output_dir / "cycles"
    cycles_output.mkdir(exist_ok=True)
    parcels_output = output_dir / "parcels"
    parcels_output.mkdir(exist_ok=True)
    event_recorder = EventRecorder(output_dir / "events.ndjson")

    if max_cycles > 100:
        logging.warning("Cycle count capped to 100 (requested %d).", max_cycles)
        max_cycles = 100

    if not token:
        token = fetch_arcgis_token()
        if token:
            logging.debug("Discovered ArcGIS token starting with %s…", token[:8])
        else:
            logging.warning("Proceeding without ArcGIS token (queries may fail).")

    geocode = geocode_address(address)
    location = geocode.get("location", {})
    lon = float(location["x"])
    lat = float(location["y"])
    logging.info("Geocoded '%s' to lon=%s lat=%s", geocode.get("address"), lon, lat)
    x_merc, y_merc = wgs84_to_web_mercator(lon, lat)

    target = fetch_target_parcel(x_merc, y_merc, token=token)
    logging.info("Subject parcel %s (%s)", target.parcel_id, target.address or "no site address")

    try:
        target_info = fetch_property_info(
            target,
            token=token,
            reference_point=(x_merc, y_merc),
            fallback_address=address,
            geocoded_address=geocode.get("address"),
        )
    except Exception as exc:  # noqa: BLE001
        logging.warning("Failed to fetch property info for subject parcel: %s", exc)
        target_info = {}

    results: Dict[str, ParcelEvaluationResult] = {}
    parcel_infos: Dict[str, Dict[str, object]] = {target.parcel_id: target_info}
    if skip_roads:
        def road_fetcher(_bounds: Tuple[float, float, float, float]) -> List[LineString]:
            return []
    else:
        road_cache: Dict[Tuple[float, float, float, float], List[LineString]] = {}

        def road_fetcher(bounds: Tuple[float, float, float, float]) -> List[LineString]:
            key = tuple(round(b, 2) for b in bounds)
            cached = road_cache.get(key)
            if cached is not None:
                return cached
            roads = fetch_roads(bounds)
            road_cache[key] = roads
            return roads

    evaluate_and_record(
        target,
        target_info,
        footprint_profile=footprint_profile,
        rotations=rotations,
        front_vector=front_vector,
        output_root=output_dir,
        results=results,
        setback=setback,
        offset_step_scale=offset_step_scale,
        auto_offset_scale=auto_offset_scale,
        offset_step_value=offset_step_value,
        offset_range_value=offset_range_value,
        auto_offset_enabled=auto_offset_enabled,
        min_composite=min_composite,
        parcel_callback=parcel_callback,
        render_best=render_best,
        render_composite=render_composite,
        road_fetcher=road_fetcher,
        skip_roads=skip_roads,
        score_workers=score_workers,
        event_recorder=event_recorder,
    )

    visited_ids: set[str] = {target.parcel_id}
    visited_parcels: List[ParcelFeature] = [target]
    frontier: List[ParcelFeature] = [target]
    completed_cycles = 0

    if progress_callback is not None:
        try:
            progress_callback("overall", {"current": 0, "total": max_cycles})
        except Exception:
            logging.debug("Overall progress callback failed during init.")

    for cycle in range(1, max_cycles + 1):
        logging.info("--- Cycle %d ---", cycle)
        unique_frontier: List[ParcelFeature] = []
        seen_frontier: set[str] = set()
        for parcel in frontier:
            if parcel.parcel_id in seen_frontier:
                continue
            seen_frontier.add(parcel.parcel_id)
            unique_frontier.append(parcel)
        frontier = unique_frontier

        next_frontier: List[ParcelFeature] = []
        next_ids: set[str] = set()
        total_seeds = max(1, len(frontier))
        processed_seeds = 0

        if progress_callback is not None:
            try:
                progress_callback("cycle", {"cycle": cycle, "processed": 0, "total": total_seeds})
            except Exception:
                logging.debug("Cycle progress callback failed during init.")

        futures = []
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            for seed in frontier:
                futures.append(
                    executor.submit(
                        _process_seed,
                        seed,
                        buffer_meters,
                        max_neighbors,
                        token,
                        visited_ids.copy(),
                    )
                )

            for future in as_completed(futures):
                seed, candidates, total_raw_candidates, start_buffer, final_buffer = future.result()
                picked: List[ParcelFeature] = []
                for neighbor in candidates:
                    if neighbor.parcel_id in visited_ids or neighbor.parcel_id in next_ids:
                        continue
                    geom = neighbor.geometry
                    if geom.area < footprint_profile.area * 0.6:
                        continue
                    nb_bounds = geom.bounds
                    width = nb_bounds[2] - nb_bounds[0]
                    height = nb_bounds[3] - nb_bounds[1]
                    span = footprint_profile.span
                    if width < span * 0.6 and height < span * 0.6:
                        continue
                    try:
                        neighbor_info = parcel_infos.get(neighbor.parcel_id)
                        if neighbor_info is None:
                            neighbor_info = fetch_property_info(neighbor, token=token)
                    except Exception as exc:  # noqa: BLE001
                        logging.warning("Failed to fetch property info for %s: %s", neighbor.parcel_id, exc)
                        neighbor_info = {}
                    parcel_infos[neighbor.parcel_id] = neighbor_info
                    visited_ids.add(neighbor.parcel_id)
                    visited_parcels.append(neighbor)
                    next_frontier.append(neighbor)
                    next_ids.add(neighbor.parcel_id)
                    evaluate_and_record(
                        neighbor,
                        neighbor_info,
                        footprint_profile=footprint_profile,
                        rotations=rotations,
                        front_vector=front_vector,
                        output_root=output_dir,
                        results=results,
                        setback=setback,
                        offset_step_scale=offset_step_scale,
                        auto_offset_scale=auto_offset_scale,
                        offset_step_value=offset_step_value,
                        offset_range_value=offset_range_value,
                        auto_offset_enabled=auto_offset_enabled,
                        min_composite=min_composite,
                        parcel_callback=parcel_callback,
                        render_best=render_best,
                    render_composite=render_composite,
                    road_fetcher=road_fetcher,
                    skip_roads=skip_roads,
                    score_workers=score_workers,
                    event_recorder=event_recorder,
                )
                    picked.append(neighbor)
                    if len(picked) >= 2:
                        break
                logging.info(
                    "Seed parcel %s examined %d candidates (buffer %.1f m -> %.1f m), selected %d",
                    seed.parcel_id,
                    total_raw_candidates,
                    start_buffer,
                    final_buffer,
                    len(picked),
                )
                processed_seeds += 1
                if progress_callback is not None:
                    try:
                        progress_callback(
                            "cycle",
                            {
                                "cycle": cycle,
                                "processed": min(processed_seeds, total_seeds),
                                "total": total_seeds,
                            },
                        )
                    except Exception:
                        logging.debug("Cycle progress callback failed while updating.")

        if not next_frontier:
            logging.info("No new parcels discovered. Crawl halted.")
            break

        geoms = [target.geometry] + [p.geometry for p in visited_parcels]
        cycle_path = None
        if render_cycle:
            if skip_roads:
                cycle_roads = []
            else:
                cycle_bounds = unary_bounds(geoms, pad=max(10.0, buffer_meters * 0.8))
                cycle_roads = road_fetcher(cycle_bounds)
            cycle_path = plot_cycle(
                cycle,
                target,
                visited_parcels,
                next_frontier,
                results,
                cycle_roads,
                footprint_profile,
                cycles_output,
            )
        if cycle_callback is not None and cycle_path is not None:
            try:
                cycle_callback(cycle, cycle_path, max_cycles)
            except Exception as exc:  # noqa: BLE001
                logging.debug("Cycle callback failed: %s", exc)
        write_cycle_json(cycle, visited_parcels, results, cycles_output)
        write_best_parcels_snapshot(parcels_output, results)
        frontier = next_frontier
        completed_cycles = cycle

        if progress_callback is not None:
            try:
                progress_callback("overall", {"current": cycle, "total": max_cycles})
            except Exception:
                logging.debug("Overall progress callback failed while updating.")

    logging.info(
        "Crawl finished with %d parcels discovered across %d cycles.",
        len(visited_ids),
        completed_cycles,
    )
    write_best_parcels_snapshot(parcels_output, results)
    if progress_callback is not None:
        try:
            progress_callback("overall", {"current": completed_cycles, "total": max_cycles})
        except Exception:
            logging.debug("Final overall progress callback failed.")


class TkLogHandler(logging.Handler):
    def __init__(self, gui: "CrawlApp") -> None:
        super().__init__()
        self.gui = gui

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        try:
            message = self.format(record)
        except Exception:  # pragma: no cover - defensive
            message = record.getMessage()
        self.gui.enqueue(("log", message))


class CrawlApp:
    def __init__(self, args: argparse.Namespace, *, initial_address: Optional[str], initial_dxf: Optional[Path]) -> None:
        if tk is None:
            raise RuntimeError("Tkinter is unavailable on this system.")

        self.args = args
        self.root = tk.Tk()
        self.root.title("Parcel Crawl v4")
        self.root.geometry("980x740")
        self.root.minsize(880, 620)

        self.queue: Queue[Tuple[str, object]] = Queue()
        self.running = False
        self.worker: Optional[threading.Thread] = None
        self.output_dir: Optional[Path] = None
        self._config_cycles = int(args.cycles)
        self._preview_photo = None
        self._preview_image_id: Optional[int] = None
        self._preview_image_size: Tuple[int, int] = (0, 0)
        self._preview_pan_start: Optional[Tuple[int, int]] = None
        self._preview_origin: Tuple[float, float] = (0.0, 0.0)
        self._preview_user_moved = False
        self._latest_photo = None
        self._best_average = 0.0
        self._latest_path: Optional[Path] = None
        self._latest_caption: str = "No parcel rendered yet."
        self._cycle_path: Optional[Path] = None
        self.preview_window: Optional[tk.Toplevel] = None
        self.preview_canvas: Optional[tk.Canvas] = None
        self.latest_canvas: Optional[tk.Canvas] = None
        self.rank_canvases: List[tk.Canvas] = []
        self.rank_caption_vars: List[tk.StringVar] = []
        self.rank_entries: List[Tuple[str, float, Path]] = []
        self.rank_photo_attrs: List[str] = [f"_rank_photo_{idx}" for idx in range(5)]
        for attr in self.rank_photo_attrs:
            setattr(self, attr, None)

        address_value = initial_address or (args.address or "")
        dxf_value = str(initial_dxf) if initial_dxf else (str(args.dxf) if args.dxf else "")

        self.address_var = tk.StringVar(value=address_value)
        self.dxf_var = tk.StringVar(value=dxf_value)
        self.output_var = tk.StringVar(value=str(args.output_dir))
        self.cycles_var = tk.StringVar(value=str(args.cycles))
        self.buffer_var = tk.StringVar(value=str(args.buffer))
        self.rotation_var = tk.StringVar(value=str(args.rotation_step))
        self.full_rotation_var = tk.BooleanVar(value=bool(args.full_rotation))
        self.offset_step_scale_var = tk.StringVar(value=str(args.offset_step_scale))
        self.offset_step_var = tk.StringVar(value="" if args.offset_step is None else str(args.offset_step))
        self.auto_offset_scale_var = tk.StringVar(value=str(args.auto_offset_scale))
        self.auto_offset_enabled_var = tk.BooleanVar(value=bool(args.auto_offset))
        self.offset_range_var = tk.StringVar(value="" if args.offset_range is None else str(args.offset_range))
        self.setback_var = tk.StringVar(value=str(args.setback))
        self.min_composite_var = tk.StringVar(value=str(args.min_composite))
        self.workers_var = tk.StringVar(value=str(args.workers))
        self.max_neighbors_var = tk.StringVar(value=str(args.max_neighbors))
        self.perpendicular_var = tk.BooleanVar(value=bool(args.frontage_perpendicular))
        self.token_var = tk.StringVar(value=args.token or "")
        self.render_cycle_var = tk.BooleanVar(value=bool(args.render_cycle))
        self.render_best_var = tk.BooleanVar(value=bool(args.render_best))
        self.render_composite_var = tk.BooleanVar(value=bool(args.render_composite))
        self.skip_roads_var = tk.BooleanVar(value=bool(args.skip_roads))
        self.score_workers_var = tk.StringVar(value=str(max(1, getattr(args, "score_workers", 1))))

        self.status_var = tk.StringVar(value="Idle")
        self.preview_caption_var = tk.StringVar(value="Cycle preview will appear here.")
        self.latest_caption_var = tk.StringVar(value="No parcel rendered yet.")

        self._build_layout()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(150, self._process_queue)

    def _build_layout(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill="both", expand=True)

        config_frame = ttk.LabelFrame(main, text="Configuration")
        config_frame.grid(row=0, column=0, columnspan=3, sticky="nsew", pady=(0, 8))

        ttk.Label(config_frame, text="Seed address:").grid(row=0, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.address_var, width=48).grid(row=0, column=1, columnspan=3, sticky="ew", pady=2)

        ttk.Label(config_frame, text="DXF file:").grid(row=1, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.dxf_var, width=40).grid(row=1, column=1, columnspan=2, sticky="ew", pady=2)
        ttk.Button(config_frame, text="Browse…", command=self._pick_dxf).grid(row=1, column=3, sticky="ew", padx=(6, 0))

        ttk.Label(config_frame, text="Output folder:").grid(row=2, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.output_var, width=40).grid(row=2, column=1, columnspan=2, sticky="ew", pady=2)
        ttk.Button(config_frame, text="Browse…", command=self._pick_output).grid(row=2, column=3, sticky="ew", padx=(6, 0))

        ttk.Label(config_frame, text="Cycles").grid(row=3, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.cycles_var, width=10).grid(row=3, column=1, sticky="w")
        ttk.Label(config_frame, text="Buffer (m)").grid(row=3, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.buffer_var, width=10).grid(row=3, column=3, sticky="w")

        ttk.Label(config_frame, text="Workers").grid(row=4, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.workers_var, width=10).grid(row=4, column=1, sticky="w")
        ttk.Label(config_frame, text="Max neighbors").grid(row=4, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.max_neighbors_var, width=10).grid(row=4, column=3, sticky="w")

        ttk.Label(config_frame, text="Score workers").grid(row=5, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.score_workers_var, width=10).grid(row=5, column=1, sticky="w")

        ttk.Label(config_frame, text="Rotation step (°)").grid(row=6, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.rotation_var, width=10).grid(row=6, column=1, sticky="w")
        ttk.Checkbutton(config_frame, text="Full rotation", variable=self.full_rotation_var).grid(row=6, column=2, columnspan=2, sticky="w")

        ttk.Label(config_frame, text="Offset step scale").grid(row=7, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.offset_step_scale_var, width=10).grid(row=7, column=1, sticky="w")
        ttk.Label(config_frame, text="Offset step (m)").grid(row=7, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.offset_step_var, width=10).grid(row=7, column=3, sticky="w")

        ttk.Label(config_frame, text="Auto offset scale").grid(row=8, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.auto_offset_scale_var, width=10).grid(row=8, column=1, sticky="w")
        ttk.Label(config_frame, text="Offset range (m)").grid(row=8, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.offset_range_var, width=10).grid(row=8, column=3, sticky="w")

        ttk.Checkbutton(config_frame, text="Auto offset", variable=self.auto_offset_enabled_var).grid(row=9, column=0, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Checkbutton(config_frame, text="Frontage vector perpendicular", variable=self.perpendicular_var).grid(row=9, column=2, columnspan=2, sticky="w", pady=(4, 0))

        ttk.Label(
            config_frame,
            text="Fast scan suggestion: rotation 20-30°, offset scale 0.25, auto-offset scale 1.5.",
            foreground="#475569",
        ).grid(row=10, column=0, columnspan=4, sticky="w", pady=(2, 6))

        ttk.Label(config_frame, text="Setback (m)").grid(row=11, column=0, sticky="w")
        ttk.Entry(config_frame, textvariable=self.setback_var, width=10).grid(row=11, column=1, sticky="w")
        ttk.Label(config_frame, text="Min composite").grid(row=11, column=2, sticky="w")
        ttk.Entry(config_frame, textvariable=self.min_composite_var, width=10).grid(row=11, column=3, sticky="w")

        ttk.Checkbutton(config_frame, text="Render cycle PNGs", variable=self.render_cycle_var).grid(row=12, column=0, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Checkbutton(config_frame, text="Render best-fit PNGs", variable=self.render_best_var).grid(row=12, column=2, columnspan=2, sticky="w", pady=(4, 0))
        ttk.Checkbutton(config_frame, text="Render composite PNGs", variable=self.render_composite_var).grid(row=13, column=0, columnspan=2, sticky="w")
        ttk.Checkbutton(config_frame, text="Skip road fetches", variable=self.skip_roads_var).grid(row=13, column=2, columnspan=2, sticky="w")

        ttk.Label(config_frame, text="ArcGIS token (optional):").grid(row=14, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(config_frame, textvariable=self.token_var, width=48).grid(row=14, column=1, columnspan=3, sticky="ew", pady=(6, 0))

        button_bar = ttk.Frame(config_frame)
        button_bar.grid(row=0, column=4, rowspan=15, padx=(12, 0), sticky="ns")
        self.start_btn = ttk.Button(button_bar, text="Start Crawl", command=self.start_crawl)
        self.start_btn.pack(fill="x")
        ttk.Button(button_bar, text="Quit", command=self.root.destroy).pack(fill="x", pady=(6, 0))

        config_frame.columnconfigure(1, weight=1)
        config_frame.columnconfigure(2, weight=1)
        config_frame.columnconfigure(3, weight=1)

        preview_frame = ttk.LabelFrame(main, text="Crawl Progress")
        preview_frame.grid(row=1, column=0, sticky="nsew")

        progress_section = ttk.Frame(preview_frame)
        progress_section.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Label(progress_section, text="Overall progress:").grid(row=0, column=0, sticky="w")
        self.overall_progress_var = tk.DoubleVar(value=0.0)
        self.overall_progress_bar = ttk.Progressbar(progress_section, maximum=100.0, variable=self.overall_progress_var)
        self.overall_progress_bar.grid(row=0, column=1, sticky="ew", padx=(6, 0))
        self.overall_progress_text = ttk.Label(progress_section, text="0 / 0")
        self.overall_progress_text.grid(row=0, column=2, sticky="e", padx=(6, 0))

        ttk.Label(progress_section, text="Cycle progress:").grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.cycle_progress_var = tk.DoubleVar(value=0.0)
        self.cycle_progress_bar = ttk.Progressbar(progress_section, maximum=100.0, variable=self.cycle_progress_var)
        self.cycle_progress_bar.grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(4, 0))
        self.cycle_progress_text = ttk.Label(progress_section, text="Cycle 0: 0/0")
        self.cycle_progress_text.grid(row=1, column=2, sticky="e", padx=(6, 0), pady=(4, 0))
        progress_section.columnconfigure(1, weight=1)

        ttk.Label(
            preview_frame,
            text="Image previews will open in a separate window once the crawl begins so they can render larger.",
            wraplength=320,
            justify="left",
        ).pack(fill="x", padx=6, pady=(0, 6))
        ttk.Label(preview_frame, textvariable=self.preview_caption_var).pack(fill="x", padx=6, pady=(0, 4))

        log_frame = ttk.LabelFrame(main, text="Activity Log")
        log_frame.grid(row=1, column=1, sticky="nsew", padx=(12, 0))
        self.log_text = tk.Text(log_frame, width=48, height=10, state=tk.DISABLED, wrap="word")
        self.log_text.pack(fill="both", expand=True, padx=6, pady=6)

        status_bar = ttk.Label(main, textvariable=self.status_var, relief="groove", anchor="w")
        status_bar.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        main.columnconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(1, weight=1)

    def enqueue(self, item: Tuple[str, object]) -> None:
        self.queue.put(item)

    def shutdown(self, *, confirm: bool = False) -> None:
        if confirm and self.running:
            proceed = messagebox.askyesno(
                "Crawl running",
                "A crawl is still running. Quit anyway?",
                parent=self.root,
            )
            if not proceed:
                return
        self.running = False
        if self.preview_window is not None and self.preview_window.winfo_exists():
            try:
                self.preview_window.destroy()
            except Exception:
                pass
            finally:
                self.preview_window = None
                self.preview_canvas = None
                self.latest_canvas = None
                self.rank_canvases = []
                self.rank_caption_vars = []
        try:
            if self.root.winfo_exists():
                self.root.quit()
        except Exception:
            pass
        try:
            if self.root.winfo_exists():
                self.root.destroy()
        except Exception:
            pass

    def _ensure_preview_window(self) -> None:
        if self.preview_window is not None and self.preview_window.winfo_exists():
            self.preview_window.deiconify()
            self.preview_window.lift()
            return

        self.preview_window = tk.Toplevel(self.root)
        self.preview_window.title("Crawl Previews")
        self.preview_window.geometry("1320x840")
        self.preview_window.minsize(960, 640)
        self.preview_window.protocol("WM_DELETE_WINDOW", self._on_preview_window_close)

        self.rank_canvases = []
        self.rank_caption_vars = []
        for attr in self.rank_photo_attrs:
            setattr(self, attr, None)

        container = ttk.Frame(self.preview_window, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=7)
        container.columnconfigure(1, weight=3)
        container.rowconfigure(0, weight=6)
        container.rowconfigure(1, weight=4)

        left_frame = ttk.Frame(container)
        left_frame.grid(row=0, column=0, rowspan=2, sticky="nsew")
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(0, weight=3)
        left_frame.rowconfigure(1, weight=2)

        cycle_frame = ttk.LabelFrame(left_frame, text="Latest Cycle")
        cycle_frame.grid(row=0, column=0, sticky="nsew")
        cycle_frame.columnconfigure(0, weight=1)
        cycle_frame.rowconfigure(0, weight=1)

        self.preview_canvas = tk.Canvas(cycle_frame, background="#0f172a", highlightthickness=0)
        self.preview_canvas.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        self.preview_canvas.bind("<ButtonPress-1>", self._on_preview_press)
        self.preview_canvas.bind("<B1-Motion>", self._on_preview_drag)
        self.preview_canvas.bind("<ButtonRelease-1>", self._on_preview_release)
        self.preview_canvas.bind("<Configure>", self._on_preview_configure)
        ttk.Label(cycle_frame, textvariable=self.preview_caption_var).grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))

        latest_frame = ttk.LabelFrame(left_frame, text="Latest Parcel")
        latest_frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        latest_frame.columnconfigure(0, weight=1)
        latest_frame.rowconfigure(0, weight=1)

        self.latest_canvas = tk.Canvas(latest_frame, background="#f8fafc", highlightthickness=0)
        self.latest_canvas.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        ttk.Label(latest_frame, textvariable=self.latest_caption_var).grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))

        leaderboard = ttk.LabelFrame(container, text="Top Parcels")
        leaderboard.grid(row=0, column=1, rowspan=2, sticky="nsew", padx=(12, 0))
        leaderboard.columnconfigure(0, weight=1)
        for idx in range(5):
            leaderboard.rowconfigure(idx, weight=1)
            slot = ttk.Frame(leaderboard)
            slot.grid(row=idx, column=0, sticky="nsew", pady=(0 if idx == 0 else 12, 0))
            slot.columnconfigure(0, weight=1)
            slot.rowconfigure(0, weight=1)
            canvas = tk.Canvas(slot, background="#f8fafc", highlightthickness=0)
            canvas.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
            caption = tk.StringVar(value=f"Rank {idx + 1}: pending")
            ttk.Label(slot, textvariable=caption).grid(row=1, column=0, sticky="ew", padx=8, pady=(0, 6))
            self.rank_canvases.append(canvas)
            self.rank_caption_vars.append(caption)

        self._set_canvas_placeholder(self.preview_canvas, "Cycle preview will appear here.", fill="#f1f5f9")
        self._set_canvas_placeholder(self.latest_canvas, "Latest composite pending")
        for idx, canvas in enumerate(self.rank_canvases):
            self._set_canvas_placeholder(canvas, "No parcel yet.")
            self.rank_caption_vars[idx].set(f"Rank {idx + 1}: pending")

        self._restore_preview_content()

    def _restore_preview_content(self) -> None:
        if Image is None or ImageTk is None:
            return
        if self.preview_canvas is not None and self._cycle_path and self._cycle_path.exists():
            try:
                with Image.open(self._cycle_path) as img:
                    pil = img.convert("RGBA")
                self._set_preview_image(pil)
            except Exception:
                pass
        if self.latest_canvas is not None and self._latest_path and self._latest_path.exists():
            self.latest_caption_var.set(self._latest_caption)
            self._display_static_image(
                self.latest_canvas,
                "_latest_photo",
                self._latest_path,
                self.latest_caption_var,
                self._latest_caption,
            )
        self._refresh_leaderboard_display()

    def _refresh_leaderboard_display(self) -> None:
        if not self.rank_canvases:
            return
        for idx, canvas in enumerate(self.rank_canvases):
            caption_var = self.rank_caption_vars[idx]
            if idx < len(self.rank_entries):
                parcel_id, avg_score, path = self.rank_entries[idx]
                caption = f"{idx + 1}. {parcel_id}\nAvg composite {avg_score:.1f}"
                if path.exists():
                    self._display_static_image(canvas, self.rank_photo_attrs[idx], path, caption_var, caption)
                else:
                    caption_var.set(f"{idx + 1}. {parcel_id}\nImage missing")
                    self._set_canvas_placeholder(canvas, "Image missing.")
                    setattr(self, self.rank_photo_attrs[idx], None)
            else:
                caption_var.set(f"Rank {idx + 1}: pending")
                self._set_canvas_placeholder(canvas, "No parcel yet.")
                setattr(self, self.rank_photo_attrs[idx], None)


    def _on_preview_window_close(self) -> None:
        if self.preview_window is None:
            return
        try:
            self.preview_window.destroy()
        finally:
            self.preview_window = None
            self.preview_canvas = None
            self.latest_canvas = None
            self.rank_canvases = []
            self.rank_caption_vars = []

    def _pick_dxf(self) -> None:
        path = filedialog.askopenfilename(
            parent=self.root,
            title="Select DXF footprint",
            filetypes=[("DXF files", "*.dxf"), ("All files", "*.*")],
        )
        if path:
            self.dxf_var.set(path)

    def _pick_output(self) -> None:
        directory = filedialog.askdirectory(parent=self.root, title="Select output folder")
        if directory:
            self.output_var.set(directory)

    def start_crawl(self) -> None:
        if self.running:
            return
        try:
            config = self._build_config()
        except ValueError as exc:
            messagebox.showerror("Invalid configuration", str(exc), parent=self.root)
            return

        self.status_var.set("Preparing crawl…")
        self.start_btn.state(["disabled"])
        self._ensure_preview_window()
        self.preview_caption_var.set("Cycle preview will update as results arrive.")
        if self.preview_canvas is not None:
            self.preview_canvas.delete("all")
            preview_message = "Deriving footprint…"
            if not config["render_cycle"]:
                preview_message = "Cycle rendering disabled."
            self._set_canvas_placeholder(self.preview_canvas, preview_message, fill="#f1f5f9")
        self._preview_photo = None
        self._preview_image_id = None
        self._preview_image_size = (0, 0)
        self._preview_pan_start = None
        self._preview_origin = (0.0, 0.0)
        self._preview_user_moved = False
        self.overall_progress_var.set(0.0)
        self.cycle_progress_var.set(0.0)
        self.overall_progress_text.configure(text="0 / 0")
        self.cycle_progress_text.configure(text="Cycle 0: 0/0")
        if self.latest_canvas is not None:
            self.latest_canvas.delete("all")
        for idx, canvas in enumerate(self.rank_canvases):
            canvas.delete("all")
            setattr(self, self.rank_photo_attrs[idx], None)
        if config["render_composite"]:
            if self.latest_canvas is not None:
                self._set_canvas_placeholder(self.latest_canvas, "Latest composite pending")
            self.latest_caption_var.set("No parcel rendered yet.")
            self._latest_caption = "No parcel rendered yet."
            for idx, canvas in enumerate(self.rank_canvases):
                self.rank_caption_vars[idx].set(f"Rank {idx + 1}: pending")
                self._set_canvas_placeholder(canvas, "No parcel yet.")
        else:
            disabled_msg = "Composite rendering disabled."
            self.latest_caption_var.set(disabled_msg)
            self._latest_caption = disabled_msg
            if self.latest_canvas is not None:
                self._set_canvas_placeholder(self.latest_canvas, disabled_msg)
            for idx, canvas in enumerate(self.rank_canvases):
                self.rank_caption_vars[idx].set(disabled_msg)
                self._set_canvas_placeholder(canvas, disabled_msg)
        self._latest_photo = None
        self._latest_path = None
        self._cycle_path = None
        self._best_average = 0.0
        self.rank_entries.clear()

        try:
            footprint_profile, front_vector = prepare_footprint(
                config["dxf"],
                auto_front=bool(config.get("auto_front")),
                front_angle=config.get("front_angle"),
            )
            if front_vector is None:
                front_vector = prompt_front_direction(footprint_profile.geometry)
            if config["frontage_perpendicular"]:
                front_vector = perpendicular(front_vector)
            front_vector = normalize_vector(front_vector)
        except Exception as exc:
            self.start_btn.state(["!disabled"])
            self.status_var.set("Footprint preparation failed.")
            messagebox.showerror("Footprint error", str(exc), parent=self.root)
            return

        config["footprint_profile"] = footprint_profile
        config["front_vector"] = front_vector

        self.status_var.set("Starting crawl…")
        self.running = True
        self.output_dir = config["output_dir"]
        self._config_cycles = int(config["cycles"])
        self.worker = threading.Thread(target=self._run_crawl, args=(config,), daemon=True)
        self.worker.start()

    def _build_config(self) -> Dict[str, object]:
        address = self.address_var.get().strip()
        if not address:
            raise ValueError("Seed address is required.")

        dxf_raw = self.dxf_var.get().strip()
        if not dxf_raw:
            raise ValueError("DXF file path is required.")
        dxf_path = Path(dxf_raw).expanduser().resolve()
        if not dxf_path.exists():
            raise ValueError(f"DXF file not found: {dxf_path}")

        output_raw = self.output_var.get().strip() or "parcel_crawl_v4"
        output_dir = Path(output_raw).expanduser().resolve()

        try:
            cycles = max(1, int(float(self.cycles_var.get())))
        except ValueError as exc:  # noqa: B902
            raise ValueError("Cycles must be an integer.") from exc
        try:
            buffer_m = float(self.buffer_var.get())
            rotation = float(self.rotation_var.get())
            offset_scale = float(self.offset_step_scale_var.get())
            auto_offset_scale = float(self.auto_offset_scale_var.get())
            setback = float(self.setback_var.get())
        except ValueError as exc:  # noqa: B902
            raise ValueError("Numeric fields contain invalid values.") from exc

        try:
            workers = max(1, int(float(self.workers_var.get())))
        except ValueError as exc:  # noqa: B902
            raise ValueError("Workers must be an integer.") from exc
        try:
            max_neighbors = max(1, int(float(self.max_neighbors_var.get())))
        except ValueError as exc:  # noqa: B902
            raise ValueError("Max neighbors must be an integer.") from exc

        offset_step_str = self.offset_step_var.get().strip()
        offset_step_value = float(offset_step_str) if offset_step_str else None
        offset_range_str = self.offset_range_var.get().strip()
        offset_range_value = float(offset_range_str) if offset_range_str else None

        try:
            min_composite = float(self.min_composite_var.get())
        except ValueError as exc:  # noqa: B902
            raise ValueError("Min composite must be numeric.") from exc

        try:
            score_workers = max(1, int(float(self.score_workers_var.get())))
        except ValueError as exc:  # noqa: B902
            raise ValueError("Score workers must be an integer.") from exc

        token_value = self.token_var.get().strip() or None

        render_cycle = bool(self.render_cycle_var.get())
        render_best = bool(self.render_best_var.get())
        render_composite = bool(self.render_composite_var.get())
        skip_roads = bool(self.skip_roads_var.get())

        return {
            "address": address,
            "dxf": dxf_path,
            "output_dir": output_dir,
            "cycles": cycles,
            "buffer": buffer_m,
            "rotation_step": rotation,
            "offset_step_scale": offset_scale,
            "auto_offset_scale": auto_offset_scale,
            "setback": setback,
            "workers": workers,
            "max_neighbors": max_neighbors,
            "offset_step": offset_step_value,
            "offset_range": offset_range_value,
            "auto_offset_enabled": bool(self.auto_offset_enabled_var.get()),
            "min_composite": min_composite,
            "full_rotation": bool(self.full_rotation_var.get()),
            "frontage_perpendicular": bool(self.perpendicular_var.get()),
            "token": token_value,
            "render_cycle": render_cycle,
            "render_best": render_best,
            "render_composite": render_composite,
            "skip_roads": skip_roads,
            "score_workers": score_workers,
        }

    def _run_crawl(self, config: Dict[str, object]) -> None:
        handler = TkLogHandler(self)
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger = logging.getLogger()
        logger.addHandler(handler)
        try:
            footprint_profile: FootprintProfile = config["footprint_profile"]
            front_vector: Tuple[float, float] = config["front_vector"]

            self.enqueue(("status", "Preparing rotations…"))
            rotations = prepare_rotations(footprint_profile, config["rotation_step"], config["full_rotation"])
            self.enqueue(("status", f"Starting crawl with {len(rotations)} rotation samples."))

            crawl_parcels(
                config["address"],
                footprint_profile=footprint_profile,
                rotations=rotations,
                front_vector=front_vector,
                max_cycles=config["cycles"],
                buffer_meters=config["buffer"],
                max_neighbors=config["max_neighbors"],
                workers=config["workers"],
                output_dir=config["output_dir"],
                token=config["token"],
                setback=config["setback"],
                offset_step_scale=config["offset_step_scale"],
                auto_offset_scale=config["auto_offset_scale"],
                offset_step_value=config["offset_step"],
                offset_range_value=config["offset_range"],
                auto_offset_enabled=config["auto_offset_enabled"],
                min_composite=config["min_composite"],
                cycle_callback=self._cycle_callback,
                progress_callback=self._progress_callback,
                parcel_callback=self._parcel_callback,
                render_cycle=config["render_cycle"],
                render_best=config["render_best"],
                render_composite=config["render_composite"],
                skip_roads=config["skip_roads"],
                score_workers=config["score_workers"],
            )
            self.enqueue(("status", "Crawl completed."))
        except Exception as exc:  # noqa: BLE001
            logger.exception("Crawl failed: %s", exc)
            self.enqueue(("error", str(exc)))
        finally:
            logger.removeHandler(handler)
            self.enqueue(("done", None))

    def _cycle_callback(self, cycle_index: int, cycle_path: Path, total_cycles: int) -> None:
        self.enqueue(("cycle", cycle_index, str(cycle_path), total_cycles))

    def _progress_callback(self, kind: str, payload: Dict[str, int]) -> None:
        self.enqueue(("progress", kind, payload))

    def _parcel_callback(self, result: ParcelEvaluationResult, parcel_dir: Path) -> None:
        composite_path = parcel_dir / "composite.png"
        avg_score = float(result.summary.get("average_composite") or 0.0)
        if not composite_path.exists():
            return
        self.enqueue(("parcel", result.parcel.parcel_id, str(composite_path), avg_score))

    def _process_queue(self) -> None:
        try:
            while True:
                message = self.queue.get_nowait()
                kind = message[0]
                if kind == "log":
                    self._append_log(str(message[1]))
                elif kind == "status":
                    self.status_var.set(str(message[1]))
                elif kind == "cycle":
                    _, cycle_idx, cycle_path, total_cycles = message
                    self._update_cycle_preview(int(cycle_idx), Path(str(cycle_path)))
                    self._update_overall_progress(int(cycle_idx), int(total_cycles))
                elif kind == "parcel":
                    _, parcel_id, composite_path, avg_score = message
                    avg_score = float(avg_score)
                    comp_path = Path(str(composite_path))
                    self._update_latest_parcel(parcel_id, comp_path, avg_score)
                    self._update_leaderboard(parcel_id, comp_path, avg_score)
                elif kind == "progress":
                    _, prog_kind, payload = message
                    if prog_kind == "cycle":
                        self._update_cycle_progress(payload)
                    elif prog_kind == "overall":
                        self._update_overall_progress(payload.get("current", 0), payload.get("total", 0))
                elif kind == "error":
                    self.status_var.set(f"Error: {message[1]}")
                    messagebox.showerror("Crawl failed", str(message[1]), parent=self.root)
                elif kind == "done":
                    self.running = False
                    self.start_btn.state(["!disabled"])
        except Empty:
            pass
        finally:
            self.root.after(200, self._process_queue)

    def _append_log(self, text: str) -> None:
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, text + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _update_cycle_preview(self, cycle_idx: int, path: Path) -> None:
        self.preview_caption_var.set(f"Cycle {cycle_idx} – {path.name}")
        self._cycle_path = path
        if self.preview_canvas is None:
            self._ensure_preview_window()
        if self.preview_canvas is None:
            return
        def attempt(retry: int = 0) -> None:
            if Image is None or ImageTk is None:
                self._set_canvas_placeholder(
                    self.preview_canvas,
                    "Install Pillow to view cycle previews.",
                    fill="#f1f5f9",
                )
                return
            try:
                with Image.open(path) as img:
                    pil = img.convert("RGBA")
            except FileNotFoundError:
                self._set_canvas_placeholder(
                    self.preview_canvas,
                    f"Cycle image missing:\n{path.name}",
                    fill="#f1f5f9",
                )
                return
            except Exception as exc:  # noqa: BLE001
                if retry < 3:
                    self.root.after(200, lambda: attempt(retry + 1))
                else:
                    self._set_canvas_placeholder(
                        self.preview_canvas,
                        f"Unable to load cycle image:\n{exc}",
                        fill="#f1f5f9",
                    )
                return
            self._set_preview_image(pil)

        attempt()

    def _set_preview_image(self, pil_image):
        if ImageTk is None:
            return
        canvas = self.preview_canvas
        if canvas is None:
            return
        canvas.update_idletasks()
        max_w = max(canvas.winfo_width() - 8, 60)
        max_h = max(canvas.winfo_height() - 8, 60)
        image = pil_image.copy()
        image.thumbnail((max_w, max_h), Image.LANCZOS)
        canvas.delete("all")
        photo = ImageTk.PhotoImage(image)
        self._preview_photo = photo
        self._preview_image_size = image.size
        self._preview_image_id = canvas.create_image(0, 0, image=photo, anchor="nw")
        canvas.image = photo
        canvas.config(scrollregion=(0, 0, image.width, image.height))
        self._preview_user_moved = False
        self._preview_origin = (0.0, 0.0)
        self._center_preview_image(force=True)

    def _center_preview_image(self, force: bool = False) -> None:
        if self.preview_canvas is None or self._preview_image_id is None or not self._preview_image_size:
            return
        if not force and self._preview_user_moved:
            return
        self.preview_canvas.update_idletasks()
        canvas_w = self.preview_canvas.winfo_width()
        canvas_h = self.preview_canvas.winfo_height()
        if canvas_w <= 1 or canvas_h <= 1:
            return
        img_w, img_h = self._preview_image_size
        x = max((canvas_w - img_w) / 2, 0)
        y = max((canvas_h - img_h) / 2, 0)
        self.preview_canvas.coords(self._preview_image_id, x, y)
        self._preview_origin = (x, y)

    def _on_preview_press(self, event) -> None:
        if self.preview_canvas is None or self._preview_image_id is None:
            return
        self.preview_canvas.focus_set()
        self._preview_pan_start = (event.x, event.y)
        coords = self.preview_canvas.coords(self._preview_image_id)
        if coords:
            self._preview_origin = (coords[0], coords[1])

    def _on_preview_drag(self, event) -> None:
        if self.preview_canvas is None or self._preview_image_id is None or self._preview_pan_start is None:
            return
        dx = event.x - self._preview_pan_start[0]
        dy = event.y - self._preview_pan_start[1]
        new_x = self._preview_origin[0] + dx
        new_y = self._preview_origin[1] + dy
        self.preview_canvas.coords(self._preview_image_id, new_x, new_y)
        self._preview_user_moved = True

    def _on_preview_release(self, _event) -> None:
        if self.preview_canvas is None:
            self._preview_pan_start = None
            return
        self._preview_pan_start = None
        coords = self.preview_canvas.coords(self._preview_image_id) if self._preview_image_id else None
        if coords:
            self._preview_origin = (coords[0], coords[1])

    def _on_preview_configure(self, _event) -> None:
        self._center_preview_image()

    def _set_canvas_placeholder(self, canvas: Optional[tk.Canvas], message: str, *, fill: str = "#475569") -> None:
        if canvas is None:
            return
        canvas.delete("all")
        canvas.update_idletasks()
        width = max(canvas.winfo_width(), 120)
        height = max(canvas.winfo_height(), 120)
        canvas.image = None
        canvas.create_text(
            width / 2,
            height / 2,
            text=message,
            fill=fill,
            font=("TkDefaultFont", 9),
            justify="center",
            width=width - 16,
        )

    def _display_static_image(
        self,
        canvas: Optional[tk.Canvas],
        photo_attr: str,
        path: Path,
        caption_var: tk.StringVar,
        caption_text: str,
        *,
        retry: int = 0,
    ) -> None:
        if canvas is None:
            return
        path = Path(path)
        if Image is None or ImageTk is None:
            caption_var.set("Install Pillow to view imagery.")
            self._set_canvas_placeholder(canvas, "Install Pillow to view imagery.")
            return
        try:
            with Image.open(path) as img:
                pil = img.convert("RGBA")
        except FileNotFoundError:
            caption_var.set(f"Image missing: {path.name}")
            self._set_canvas_placeholder(canvas, f"Image missing:\n{path.name}")
            return
        except Exception as exc:  # noqa: BLE001
            if retry < 3:
                self.root.after(
                    200,
                    lambda: self._display_static_image(canvas, photo_attr, path, caption_var, caption_text, retry=retry + 1),
                )
            else:
                caption_var.set(f"Failed to load image: {exc}")
                self._set_canvas_placeholder(canvas, f"Failed to load image:\n{exc}")
            return

        canvas.update_idletasks()
        max_w = max(canvas.winfo_width() - 8, 20)
        max_h = max(canvas.winfo_height() - 8, 20)
        pil = pil.copy()
        pil.thumbnail((max_w, max_h), Image.LANCZOS)
        photo = ImageTk.PhotoImage(pil)
        setattr(self, photo_attr, photo)
        canvas.delete("all")
        canvas.create_image(canvas.winfo_width() / 2, canvas.winfo_height() / 2, image=photo, anchor="center")
        canvas.image = photo
        caption_var.set(caption_text)

    def _update_latest_parcel(self, parcel_id: str, path: Path, avg_score: float) -> None:
        if self.latest_canvas is None:
            self._ensure_preview_window()
        if self.latest_canvas is None:
            return
        if not path.exists():
            self.latest_caption_var.set("Latest composite unavailable.")
            self._set_canvas_placeholder(self.latest_canvas, "Latest composite unavailable.")
            self._latest_path = None
            self._latest_caption = "Latest composite unavailable."
            return
        caption = f"{parcel_id}\nAvg composite {avg_score:.1f}"
        self._latest_caption = caption
        self._latest_path = path
        self._display_static_image(self.latest_canvas, "_latest_photo", path, self.latest_caption_var, caption)

    def _update_leaderboard(self, parcel_id: str, path: Path, avg_score: float) -> None:
        path = Path(path)
        if not path.exists():
            return
        self.rank_entries = [entry for entry in self.rank_entries if entry[0] != parcel_id]
        self.rank_entries.append((parcel_id, float(avg_score), path))
        self.rank_entries.sort(key=lambda item: item[1], reverse=True)
        if len(self.rank_entries) > 5:
            self.rank_entries = self.rank_entries[:5]
        self._best_average = self.rank_entries[0][1] if self.rank_entries else 0.0
        if not self.rank_canvases:
            self._ensure_preview_window()
        self._refresh_leaderboard_display()

    def _update_cycle_progress(self, payload: Dict[str, int]) -> None:
        cycle_idx = int(payload.get("cycle", 0))
        processed = int(payload.get("processed", 0))
        total = max(1, int(payload.get("total", 1)))
        percent = min(100.0, max(0.0, processed / total * 100.0))
        self.cycle_progress_var.set(percent)
        self.cycle_progress_text.configure(text=f"Cycle {cycle_idx}: {processed}/{total}")

    def _update_overall_progress(self, current: int, total: int) -> None:
        total = max(1, total)
        current = max(0, min(current, total))
        percent = current / total * 100.0
        self.overall_progress_var.set(percent)
        self.overall_progress_text.configure(text=f"{current} / {total}")

    def _on_close(self) -> None:
        self.shutdown(confirm=True)

    def run(self) -> None:
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            logging.info("GUI interrupted; shutting down.")
            self.shutdown(confirm=False)

def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    address = (args.address or "").strip() or None
    dxf_path: Optional[Path] = args.dxf
    if dxf_path is not None:
        dxf_path = dxf_path.expanduser().resolve()

    args.output_dir = args.output_dir.expanduser().resolve()

    footprint_profile: Optional[FootprintProfile] = None
    front_vector: Optional[Tuple[float, float]] = None
    front_hint = tuple(args.front_vector) if args.front_vector else None

    if args.footprint_json:
        footprint_profile = load_footprint_from_json(args.footprint_json)
        if front_hint is not None:
            front_vector = front_hint
        elif args.front_angle is not None:
            front_vector = vector_from_angle(args.front_angle)
        else:
            raise RuntimeError("Provide --front-vector or --front-angle when using --footprint-json.")

    if address and dxf_path:
        detected_front: Optional[Tuple[float, float]] = None
        if footprint_profile is None:
            logging.info("Loading DXF footprint from %s", dxf_path)
            footprint_profile, detected_front = prepare_footprint(
                dxf_path,
                auto_front=args.auto_front,
                front_angle=args.front_angle,
                front_vector_override=front_hint,
            )
        else:
            logging.info("Using precomputed footprint from %s", args.footprint_json)

        active_front = front_vector or front_hint or detected_front
        if active_front is None:
            active_front = prompt_front_direction(footprint_profile.geometry)
        if args.frontage_perpendicular:
            active_front = perpendicular(active_front)
        front_vector = normalize_vector(active_front)

        rotations = prepare_rotations(footprint_profile, args.rotation_step, args.full_rotation)
        logging.info("Prepared %d rotation samples.", len(rotations))

        crawl_parcels(
            address,
            footprint_profile=footprint_profile,
            rotations=rotations,
            front_vector=front_vector,
            max_cycles=args.cycles,
            buffer_meters=args.buffer,
            max_neighbors=args.max_neighbors,
            workers=args.workers,
            output_dir=args.output_dir,
            token=args.token,
            setback=args.setback,
            offset_step_scale=args.offset_step_scale,
            auto_offset_scale=args.auto_offset_scale,
            offset_step_value=args.offset_step,
            offset_range_value=args.offset_range,
            auto_offset_enabled=args.auto_offset,
            min_composite=args.min_composite,
            render_cycle=args.render_cycle,
            render_best=args.render_best,
            render_composite=args.render_composite,
            skip_roads=args.skip_roads,
            score_workers=args.score_workers,
        )
        return

    if tk is None:
        raise RuntimeError(
            "Tkinter is not available; supply --address and --dxf to run headless or install Tk."
        )

    app = CrawlApp(args, initial_address=address, initial_dxf=dxf_path)
    try:
        app.run()
    except KeyboardInterrupt:
        logging.info("Interrupted by user; closing GUI.")
        app.shutdown(confirm=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Crawl aborted by user.")
def bounds_contains(outer: Tuple[float, float, float, float], inner: Tuple[float, float, float, float]) -> bool:
    return (
        inner[0] >= outer[0]
        and inner[1] >= outer[1]
        and inner[2] <= outer[2]
        and inner[3] <= outer[3]
    )


def expand_bounds(bounds: Tuple[float, float, float, float], pad: float) -> Tuple[float, float, float, float]:
    return (bounds[0] - pad, bounds[1] - pad, bounds[2] + pad, bounds[3] + pad)


def merge_bounds(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> Tuple[float, float, float, float]:
    return (min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3]))
