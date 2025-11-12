"""Worker entry point placeholder.

Later this module will wrap parcel_crawl_demo_v4.py to execute real crawls.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Dict

STORAGE_DIR = Path("storage")
STORAGE_DIR.mkdir(exist_ok=True)


def run_job(job: Dict[str, Any]) -> Path:
    job_id = job["id"]
    output = {
        "job": job,
        "message": "Worker execution not implemented yet",
        "parcels": [],
    }
    time.sleep(2)
    path = STORAGE_DIR / f"{job_id}.json"
    path.write_text(json.dumps(output, indent=2))
    return path


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python worker/run_job.py job.json", file=sys.stderr)
        sys.exit(1)
    payload = json.loads(Path(sys.argv[1]).read_text())
    artifact = run_job(payload)
    print(f"Wrote {artifact}")
