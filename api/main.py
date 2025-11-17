from fastapi import FastAPI

from api.routes import jobs, uploads, downloads, designs, geocode, debug

app = FastAPI(title="Parcel Crawl API", version="0.1.0")


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
