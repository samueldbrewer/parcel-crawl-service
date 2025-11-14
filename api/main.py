from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from api.routes import jobs, uploads, downloads

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="Parcel Crawl API", version="0.1.0")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/health")
async def health() -> dict[str, object]:
    return {
        "status": "ok",
        "upload": uploads.describe_upload_target(),
    }


app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
app.include_router(uploads.router, prefix="/files", tags=["files"])
app.include_router(downloads.router, prefix="/jobs", tags=["job-files"])
