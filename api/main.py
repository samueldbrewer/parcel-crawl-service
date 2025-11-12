from fastapi import FastAPI

from api.routes import jobs, uploads

app = FastAPI(title="Parcel Crawl API", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
app.include_router(uploads.router, prefix="/files", tags=["files"])
