import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.routes import jobs, uploads, downloads, designs, geocode, debug

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Parcel Crawl API", version="0.1.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _page_ctx(request: Request, page: str, title: str, subtitle: str) -> dict[str, object]:
    return {
        "request": request,
        "maptiler_key": os.getenv("MAPTILER_KEY", ""),
        "page": page,
        "title": title,
        "subtitle": subtitle,
    }


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> HTMLResponse:
    return HTMLResponse("<!DOCTYPE html><html><body><p>blank</p></body></html>")


@app.get("/architecture", response_class=HTMLResponse)
async def architecture(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("architecture.html", {"request": request})


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
