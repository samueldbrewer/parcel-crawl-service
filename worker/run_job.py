"""Worker entry point that executes the parcel crawl script for a single job."""
from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

LOG = logging.getLogger(__name__)

SCRIPT_PATH = Path(
    os.getenv("PARCEL_CRAWL_SCRIPT", Path(__file__).resolve().parents[1] / "parcel_crawl_demo_v4.py")
).resolve()
JOB_STORAGE = Path(os.getenv("JOB_STORAGE_ROOT", Path("storage") / "jobs")).resolve()
JOB_STORAGE.mkdir(parents=True, exist_ok=True)
DXF_TIMEOUT = int(os.getenv("DXF_DOWNLOAD_TIMEOUT", "120"))
LOG_TAIL_LINES = int(os.getenv("JOB_LOG_TAIL_LINES", "200"))

NUMERIC_FLAGS: Dict[str, str] = {
    "cycles": "--cycles",
    "buffer": "--buffer",
    "max_neighbors": "--max-neighbors",
    "workers": "--workers",
    "score_workers": "--score-workers",
    "rotation_step": "--rotation-step",
    "offset_step_scale": "--offset-step-scale",
    "offset_step": "--offset-step",
    "offset_range": "--offset-range",
    "auto_offset_scale": "--auto-offset-scale",
    "setback": "--setback",
    "min_composite": "--min-composite",
}

POSITIVE_FLAGS: Dict[str, str] = {
    "full_rotation": "--full-rotation",
    "skip_roads": "--skip-roads",
    "frontage_perpendicular": "--frontage-perpendicular",
}

NEGATED_FLAGS: Dict[str, str] = {
    "render_cycle": "--no-render-cycle",
    "render_best": "--no-render-best",
    "render_composite": "--no-render-composite",
}

AUTO_OFFSET_FLAGS = ("--auto-offset", "--no-auto-offset")


class JobExecutionError(RuntimeError):
    """Raised when the crawl pipeline fails for a job."""

    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.context = context or {}


def run_job(job: Dict[str, Any]) -> Dict[str, Any]:
    """Execute the crawl script for the supplied job payload."""
    job_id = job["id"]
    workspace = JOB_STORAGE / job_id
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)

    dxf_path = workspace / "footprint.dxf"
    download_dxf(job["dxf_url"], dxf_path)

    output_dir = workspace / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    command = build_command(job, dxf_path, output_dir, workspace)
    log_path = workspace / "crawl.log"
    exit_code = execute(command, log_path)

    if exit_code != 0:
        context = {
            "workspace": str(workspace),
            "command": format_command(command),
            "log_tail": read_log_tail(log_path),
        }
        raise JobExecutionError(f"Crawler exited with status {exit_code}.", context)

    result = collect_summary(output_dir)
    result.update(
        {
            "workspace": str(workspace),
            "command": format_command(command),
            "log_path": str(log_path),
            "log_tail": read_log_tail(log_path),
        }
    )
    manifest_path = workspace / "result.json"
    result["manifest_path"] = str(manifest_path)
    manifest_path.write_text(json.dumps(result, indent=2))
    return result


def download_dxf(url: str, dest: Path) -> None:
    """Download the DXF footprint for the job."""
    if url.startswith("file://"):
        src = Path(url[7:])
        if not src.exists():
            raise JobExecutionError("DXF path does not exist.", {"path": url})
        shutil.copyfile(src, dest)
        return

    if url.startswith("/") or url.startswith("~"):
        src = Path(url).expanduser()
        if not src.exists():
            raise JobExecutionError("DXF path does not exist.", {"path": str(src)})
        shutil.copyfile(src, dest)
        return

    LOG.info("Downloading DXF from %s", url)
    try:
        with requests.get(url, stream=True, timeout=DXF_TIMEOUT) as response:
            response.raise_for_status()
            with dest.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        handle.write(chunk)
    except requests.RequestException as exc:  # pragma: no cover - network errors are runtime issues
        raise JobExecutionError("Failed to download DXF.", {"url": url, "error": str(exc)}) from exc


def build_command(job: Dict[str, Any], dxf_path: Path, output_dir: Path, workspace: Path) -> List[str]:
    if not SCRIPT_PATH.exists():
        raise JobExecutionError("parcel_crawl_demo_v4.py is missing inside the image.", {"script_path": str(SCRIPT_PATH)})

    address = job["address"]
    config: Dict[str, Any] = dict(job.get("config") or {})
    if "auto_front" not in config:
        config["auto_front"] = os.getenv("CRAWL_AUTO_FRONT", "1") == "1"
    job["config"] = config

    command: List[str] = [
        sys.executable,
        str(SCRIPT_PATH),
        "--address",
        address,
        "--dxf",
        str(dxf_path),
        "--output-dir",
        str(output_dir),
    ]

    log_level = str(config.get("log_level") or os.getenv("CRAWL_LOG_LEVEL") or "INFO").upper()
    command += ["--log-level", log_level]

    token = config.get("token") or os.getenv("ATL_ARCGIS_TOKEN")
    if token:
        command += ["--token", token]

    for key, flag in NUMERIC_FLAGS.items():
        if key not in config or config[key] is None:
            continue
        command += [flag, str(config[key])]

    for key, flag in POSITIVE_FLAGS.items():
        if config.get(key):
            command.append(flag)

    auto_front = config.get("auto_front")
    if auto_front is True:
        command.append("--auto-front")
    elif auto_front is False:
        command.append("--no-auto-front")

    if "auto_offset" in config:
        command.append(AUTO_OFFSET_FLAGS[0 if config["auto_offset"] else 1])

    if config.get("front_angle") is not None:
        command += ["--front-angle", str(config["front_angle"])]

    direction = config.get("front_direction") or config.get("front_vector")
    if direction:
        if not isinstance(direction, (list, tuple)) or len(direction) != 2:
            raise JobExecutionError("front_direction config must be a 2-element list.", {"front_direction": direction})
        command += ["--front-vector", str(direction[0]), str(direction[1])]

    footprint_points = config.get("footprint_points")
    if footprint_points:
        footprint_json = workspace / "footprint.json"
        footprint_json.write_text(json.dumps({"points": footprint_points}))
        command += ["--footprint-json", str(footprint_json)]

    for key, flag in NEGATED_FLAGS.items():
        if key in config and config[key] is False:
            command.append(flag)

    return command


def execute(command: List[str], log_path: Path) -> int:
    LOG.info("Starting crawl: %s", format_command(command))
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.run(
            command,
            cwd=SCRIPT_PATH.parent,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=os.environ.copy(),
        )
    return process.returncode


def collect_summary(output_dir: Path) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "output_dir": str(output_dir),
        "best_parcels": [],
        "cycle_summaries": [],
        "artifacts": {
            "best_parcels": None,
            "cycle_json": [],
            "cycle_png": [],
            "parcels_dir": None,
        },
    }

    parcels_dir = output_dir / "parcels"
    cycles_dir = output_dir / "cycles"

    best_path = parcels_dir / "best_parcels.json"
    if best_path.exists():
        summary["best_parcels"] = json.loads(best_path.read_text())
        summary["artifacts"]["best_parcels"] = str(best_path)

    if parcels_dir.exists():
        summary["artifacts"]["parcels_dir"] = str(parcels_dir)

    if cycles_dir.exists():
        for json_path in sorted(cycles_dir.glob("cycle_*.json")):
            try:
                payload = json.loads(json_path.read_text())
            except json.JSONDecodeError:
                continue
            summary["cycle_summaries"].append(_summarize_cycle(payload))
            summary["artifacts"]["cycle_json"].append(str(json_path))
        summary["artifacts"]["cycle_png"] = [str(p) for p in sorted(cycles_dir.glob("cycle_*.png"))]

    return summary


def _summarize_cycle(payload: Dict[str, Any]) -> Dict[str, Any]:
    parcels = payload.get("parcels") or []
    ranked = sorted(
        (
            {
                "parcel_id": parcel.get("parcel_id"),
                "address": parcel.get("address"),
                "max_composite": (parcel.get("summary") or {}).get("max_composite"),
                "viable_count": (parcel.get("summary") or {}).get("viable_count"),
            }
            for parcel in parcels
        ),
        key=lambda item: item.get("max_composite") or 0.0,
        reverse=True,
    )

    return {
        "cycle": payload.get("cycle"),
        "parcels_evaluated": len(parcels),
        "top_parcels": ranked[:3],
    }


def read_log_tail(log_path: Path) -> str:
    if not log_path.exists():
        return ""
    with log_path.open("r", encoding="utf-8", errors="ignore") as stream:
        tail = deque(stream, maxlen=LOG_TAIL_LINES)
    return "".join(tail)


def format_command(parts: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker/run_job.py job_payload.json")
        sys.exit(1)

    payload_path = Path(sys.argv[1])
    payload = json.loads(payload_path.read_text())
    result = run_job(payload)
    print(json.dumps(result, indent=2))
