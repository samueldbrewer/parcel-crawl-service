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
    return templates.TemplateResponse(
        "designs.html",
        _page_ctx(
            request,
            page="designs",
            title="Design, save, and run parcel crawls.",
            subtitle="Capture shrink-wrap footprints, reuse saved designs, and start crawls.",
        ),
    )


@app.get("/designs", response_class=HTMLResponse)
async def designs_page(request: Request) -> HTMLResponse:
    return await root(request)


@app.get("/map", response_class=HTMLResponse)
async def map_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "map.html",
        _page_ctx(
            request,
            page="map",
            title="Run crawls from the map.",
            subtitle="Select a saved design, drop a pin, and watch overlays in real time.",
        ),
    )


@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "config.html",
        _page_ctx(request, page="config", title="Configure crawl defaults.", subtitle=""),
    )


@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "jobs.html",
        _page_ctx(request, page="jobs", title="Jobs dashboard.", subtitle="View current and recent jobs."),
    )


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
