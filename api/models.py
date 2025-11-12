from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import AnyUrl, BaseModel, Field


class JobCreate(BaseModel):
    address: str = Field(..., description="Seed address for the crawl")
    dxf_url: AnyUrl = Field(..., description="Location of the DXF footprint (http(s) or file URL)")
    config: Dict[str, Any] | None = Field(
        default=None,
        description="Optional overrides for crawl parameters",
    )


class JobStatus(BaseModel):
    id: str
    status: str
    result_url: Optional[str] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None


class JobRecord(JobStatus):
    created_at: datetime
    address: str
    dxf_url: AnyUrl
    config: Dict[str, Any] = Field(default_factory=dict)


class FileUploadResponse(BaseModel):
    filename: str
    stored_path: str
    file_url: str
