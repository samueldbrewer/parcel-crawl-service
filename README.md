# Parcel Crawl Service

Backend scaffold for orchestrating parcel crawl jobs. This milestone exposes a FastAPI service with `/jobs` endpoints and an in-memory worker simulator. Future work will plug in the real parcel crawler, persistent storage, and a job queue.

## Running locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api.main:app --reload
```

Submit a job:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{
        "address": "3419 Northside Pkwy NW, Atlanta, GA",
        "dxf_url": "https://example.com/design.dxf"
      }'
```

The worker simulator writes JSON artifacts under `storage/`.
