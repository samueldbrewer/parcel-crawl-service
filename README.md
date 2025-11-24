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

## Uploading DXFs to the server

If your Railway deployment mounts a volume at `/data`, you can push DXFs directly through the API and
reference them later via `file://` URLs:

```bash
# Upload from local disk
curl -s -X POST https://landlens.up.railway.app/files \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/P14_LE_BASE_FOOTPRINT.dxf" \
  | jq

# Response snippet
{
  "filename": "P14_LE_BASE_FOOTPRINT.dxf",
  "stored_path": "/data/P14_LE_BASE_FOOTPRINT.dxf",
  "file_url": "file:///data/P14_LE_BASE_FOOTPRINT.dxf",
  "download_url": "https://landlens.up.railway.app/files/P14_LE_BASE_FOOTPRINT.dxf"
}
```

Use the returned `file_url` (e.g., `file:///data/P14_LE_BASE_FOOTPRINT.dxf`) as the `dxf_url` when
submitting `/jobs` requests. The `download_url` is a public HTTPS endpoint that streams the stored file
back, making it easy to verify uploads or share them with other services. Override the upload directory
with `DXF_UPLOAD_ROOT` if your volume is mounted somewhere else. The uploader automatically extracts
`.zip` archives into `/data` and lists the extracted artifacts (each with its own download URL) so you
can upload compressed DXFs. Set `UPLOAD_TIMEOUT` (default 900 seconds) if you need more time for larger
files to stream.

List everything currently staged on the volume:

```bash
curl -s https://landlens.up.railway.app/files | jq
```

Download any stored artifact via the public link:

```bash
curl -O https://landlens.up.railway.app/files/<filename>
```

Delete a stored asset (removes it from `/data`):

```bash
curl -X DELETE https://landlens.up.railway.app/files/<filename>
```

Fetch raw DXF polylines for custom previews:

```bash
curl -s https://landlens.up.railway.app/files/<filename>/geometry | jq
```

Run shrink-wrap remotely from client-provided points:

```bash
curl -s -X POST https://landlens.up.railway.app/files/<filename>/shrinkwrap \
  -H "Content-Type: application/json" \
  -d '{
        "rectangle_points": [[0,0],[30,0],[0,20]],
        "front_points": [[5,5],[10,5]]
      }' | jq
```

## Environment variables

| Variable | Purpose |
| --- | --- |
| `ATL_ARCGIS_TOKEN` | Overrides token discovery for the City of Atlanta ArcGIS endpoints. |
| `CRAWL_LOG_LEVEL` | Default log level passed to the crawler (`INFO` if unset). |
| `CRAWL_AUTO_FRONT` | `1` (default) auto-derives frontage headings; set `0` to re-enable interactive prompts. |
| `PARCEL_CRAWL_SCRIPT` | Path to `parcel_crawl_demo_v4.py` if you relocate it. |
| `JOB_STORAGE_ROOT` | Root directory for job workspaces (defaults to `storage/jobs`). |
| `DXF_DOWNLOAD_TIMEOUT` | DXF download timeout in seconds (default 120). |
| `DXF_UPLOAD_ROOT` | Directory where `/files` uploads will be stored (default `/data`). |
| `DESIGN_STORAGE_ROOT` | Directory for saved designs (default `/data/designs`). |
| `UPLOAD_TIMEOUT` | Client-side upload timeout used by `remote_client_gui.py` (default 900 seconds). |
| `CORS_ALLOWED_ORIGINS` | Comma-separated origins allowed via CORS middleware (defaults to Railway + localhost). |

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
- When calling `/jobs`, you can now supply `footprint_points` (list of `[x, y]`) and `front_direction`
  (two floats) to reuse the footprint captured locally. These become `--footprint-json` and
  `--front-vector` when the worker launches the crawler.
- On the web UI, the **Start Crawl** button remains disabled until shrink-wrap succeeds; once the modal
  reports “Shrink-wrap captured,” the button flips to “Start Crawl” and the job payload is ready.

### Remote GUI workflow

`remote_client_gui.py` is a lightweight desktop helper that mirrors the classic shrink-wrap flow:

1. Select the DXF locally and click **Capture Footprint** to pick the bounding box + frontage direction.
2. Refresh the **Remote Files** list to see everything already staged on the server.
3. Click **Upload DXF** (or pick an existing file). The capture modal renders the DXF geometry via
   `/files/<name>/geometry`; choose three rectangle points and two frontage points, apply shrink-wrap
   (the UI calls `/files/<name>/shrinkwrap`), then submit the crawl. The response panel shows the job ID
   and updates as the job runs.

Run it with `python3 remote_client_gui.py` (requires the same dependencies as the crawler). Configure the
API base URL if you’re targeting a different Railway environment.

## Live service URL and endpoints

- Production base: `https://landlens.up.railway.app`
- Local dev base: `http://localhost:8000`
- OpenAPI docs: `/docs`

Primary routes:

- `POST /jobs` – submit crawl job
- `GET /jobs/{job_id}` – job status/result
- `GET /jobs/{job_id}/log` – crawl log stream
- `GET /jobs/{job_id}/outputs/{path}` – download artifacts
- `POST /files` – upload DXF (multipart `file`)
- `GET /files` – list uploads
- `GET /files/{filename}` – download file
- `DELETE /files/{filename}` – delete file
- `GET /files/{filename}/geometry` – DXF polylines JSON
- `POST /files/{filename}/shrinkwrap` – run shrink-wrap
- `GET /designs` / `GET /designs/{id}` – design metadata
- `GET /geocode/reverse?lat=&lon=` – reverse geocode
- `GET /health` – service health
- `GET /debug/env` – environment debug

## Deployment notes

The Dockerfile installs all crawler dependencies, copies both `parcel_crawl_demo_v4.py`
and `parcel_lookup.py`, and exposes the FastAPI app on port 8000. Railway builds can use the
detected Dockerfile; just make sure the service has enough CPU/RAM for Shapely + multiprocessing
and that `storage/` persists (Railway volume or object store sync). Add a background worker service
if you later move job execution outside the API container.
