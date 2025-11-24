import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import jobs, uploads, downloads, designs, geocode, debug

app = FastAPI(title="Parcel Crawl API", version="0.1.0")

default_origins = [
    "https://landlens-production.up.railway.app",
    "https://landlens.up.railway.app",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:8000",
]
cors_origins = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOWED_ORIGINS", ",".join(default_origins)).split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, object]:
    return {
        "service": "parcel-crawl",
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
async def health() -> dict[str, object]:
    return {
        "status": "ok",
        "upload": uploads.describe_upload_target(),
    }


app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
app.include_router(uploads.router, prefix="/files", tags=["files"])
app.include_router(downloads.router, prefix="/jobs", tags=["job-files"])
app.include_router(designs.router, prefix="/designs", tags=["designs"])
app.include_router(geocode.router, prefix="/geocode", tags=["geocode"])
app.include_router(debug.router, prefix="/debug", tags=["debug"])
