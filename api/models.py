from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, HttpUrl


class JobCreate(BaseModel):
    address: str = Field(..., description="Seed address for the crawl")
    dxf_url: HttpUrl = Field(..., description="Location of the DXF footprint")
    config: Dict[str, Any] | None = Field(
        default=None,
        description="Optional overrides for crawl parameters",
    )


class JobStatus(BaseModel):
    id: str
    status: str
    result_url: Optional[HttpUrl] = None
    error: Optional[str] = None


class JobRecord(JobStatus):
    created_at: datetime
    address: str
    dxf_url: HttpUrl
    config: Dict[str, Any] = Field(default_factory=dict)
