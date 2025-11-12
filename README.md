# Parcel Crawl Service

FastAPI wrapper for the `parcel_crawl_demo_v4.py` engine. Jobs submitted to `/jobs`
download the referenced DXF, run the full crawl headlessly (with frontage auto-derived),
and return JSON summaries plus filesystem paths to detailed artifacts (PNG/JSON outputs).

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api.main:app --reload
```

Submit a crawl job (HTTP or `file://` DXF URL supported):

```bash
curl -s -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "address": "3419 Northside Pkwy NW, Atlanta, GA 30327",
    "dxf_url": "https://example.com/sample.dxf",
    "config": {
      "cycles": 3,
      "score_workers": 4,
      "render_composite": true
    }
  }'
```

Look up the job:

```bash
curl -s http://localhost:8000/jobs/<job_id> | jq
```

`result.artifacts` lists the manifest, cycle PNG/JSON files, and parcel directories under
`storage/jobs/<job_id>/outputs`.

## Environment variables

| Variable | Purpose |
| --- | --- |
| `ATL_ARCGIS_TOKEN` | Overrides token discovery for the City of Atlanta ArcGIS endpoints. |
| `CRAWL_LOG_LEVEL` | Default log level passed to the crawler (`INFO` if unset). |
| `CRAWL_AUTO_FRONT` | `1` (default) auto-derives frontage headings; set `0` to re-enable interactive prompts. |
| `PARCEL_CRAWL_SCRIPT` | Path to `parcel_crawl_demo_v4.py` if you relocate it. |
| `JOB_STORAGE_ROOT` | Root directory for job workspaces (defaults to `storage/jobs`). |
| `DXF_DOWNLOAD_TIMEOUT` | DXF download timeout in seconds (default 120). |

## Storage layout

Each job gets its own workspace:

```
storage/jobs/<job_id>/
├── crawl.log             # stdout/stderr from the crawl run
├── footprint.dxf         # downloaded DXF input
├── outputs/              # native outputs from parcel_crawl_demo_v4
└── result.json           # summarized manifest returned via the API
```

Add an object-store or volume mount in Railway/production to persist `storage/` across deploys.

## Configuration passthrough

Values inside the `config` object map to CLI flags from `parcel_crawl_demo_v4.py`.
Examples:

```json
"config": {
  "cycles": 4,
  "score_workers": 8,
  "rotation_step": 10,
  "auto_offset": false,
  "offset_range": 60,
  "render_cycle": false,
  "skip_roads": true
}
```

- Boolean flags that disable rendering (`render_cycle`, `render_best`, `render_composite`)
  become `--no-render-*`.
- `score_workers`, `rotation_step`, etc. map directly to their CLI equivalents.
- If `auto_front` isn’t provided, the worker forces `--auto-front` so the crawl can run headlessly.
- Provide `front_angle` or `front_vector` to override the auto-detected frontage.

## Deployment notes

The Dockerfile installs all crawler dependencies, copies both `parcel_crawl_demo_v4.py`
and `parcel_lookup.py`, and exposes the FastAPI app on port 8000. Railway builds can use the
detected Dockerfile; just make sure the service has enough CPU/RAM for Shapely + multiprocessing
and that `storage/` persists (Railway volume or object store sync). Add a background worker service
if you later move job execution outside the API container.
