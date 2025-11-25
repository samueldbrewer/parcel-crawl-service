from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import AnyUrl, BaseModel, Field


class JobCreate(BaseModel):
    address: str = Field(..., description="Seed address for the crawl")
    dxf_url: AnyUrl = Field(..., description="Location of the DXF footprint (http(s) or file URL)")
    config: Dict[str, Any] | None = Field(
        default=None,
        description="Optional overrides for crawl parameters",
    )
    footprint_points: List[List[float]] | None = Field(
        default=None,
        description="Optional footprint polygon coordinates (meters)",
    )
    front_direction: List[float] | None = Field(
        default=None,
        description="Optional frontage direction vector [x, y]",
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
    footprint_points: List[List[float]] | None = None
    front_direction: List[float] | None = None
    log_tail: Optional[str] = None
    log_available: bool = False


class JobCancelRequest(BaseModel):
    reason: Optional[str] = Field(
        default="Job cancelled via API.",
        description="Optional cancellation reason stored on the job record.",
    )


class FileUploadResponse(BaseModel):
    filename: str
    stored_path: str
    file_url: str
    download_url: Optional[str] = None
    extracted_files: List["FileArtifact"] = Field(default_factory=list)


class FileArtifact(BaseModel):
    filename: str
    stored_path: str
    file_url: str
    download_url: str


FileUploadResponse.model_rebuild()


class ShrinkwrapRequest(BaseModel):
    rectangle_points: List[List[float]] = Field(..., min_length=3)
    front_points: List[List[float]] = Field(..., min_length=2, max_length=2)


class ShrinkwrapResponse(BaseModel):
    footprint_points: List[List[float]]
    front_direction: List[float]
    front_origin: List[float]
    area: float
