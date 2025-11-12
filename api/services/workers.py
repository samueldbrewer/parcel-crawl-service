import json
import threading
import time
from pathlib import Path

from api.models import JobRecord

OUTPUT_DIR = Path("storage")
OUTPUT_DIR.mkdir(exist_ok=True)


def enqueue(job: JobRecord) -> None:
    thread = threading.Thread(target=_process_job, args=(job,), daemon=True)
    thread.start()


def _process_job(job: JobRecord) -> None:
    from api.routes import jobs as job_routes  # avoid circular import at module load

    job_routes.update_job_status(job.id, "running")
    time.sleep(1.5)  # placeholder for actual crawl start
    output_path = OUTPUT_DIR / f"{job.id}.json"
    payload = {
        "job_id": job.id,
        "address": job.address,
        "dxf_url": str(job.dxf_url),
        "config": job.config,
        "message": "Crawler execution is not wired yet.",
        "parcels": [],
    }
    output_path.write_text(json.dumps(payload, indent=2))
    job_routes.update_job_status(job.id, "completed", result_url=str(output_path.resolve()))
