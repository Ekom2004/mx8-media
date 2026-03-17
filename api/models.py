from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class TransformSpec(BaseModel):
    type: str
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_supported_transforms(self) -> "TransformSpec":
        if self.type == "image.resize":
            allowed = {"width", "height", "maintain_aspect"}
            width = self.params.get("width")
            height = self.params.get("height")
            if not isinstance(width, int) or width <= 0:
                raise ValueError("image.resize requires width > 0")
            if not isinstance(height, int) or height <= 0:
                raise ValueError("image.resize requires height > 0")
            if not set(self.params).issubset(allowed):
                raise ValueError("image.resize only accepts width, height, maintain_aspect")
        elif self.type == "image.crop":
            allowed = {"width", "height"}
            width = self.params.get("width")
            height = self.params.get("height")
            if not isinstance(width, int) or width <= 0:
                raise ValueError("image.crop requires width > 0")
            if not isinstance(height, int) or height <= 0:
                raise ValueError("image.crop requires height > 0")
            if not set(self.params).issubset(allowed):
                raise ValueError("image.crop only accepts width and height")
        elif self.type == "image.convert":
            allowed = {"format", "quality"}
            format_value = self.params.get("format")
            quality = self.params.get("quality", 85)
            if not isinstance(format_value, str):
                raise ValueError("image.convert requires format")
            normalized = format_value.strip().lower()
            if normalized == "jpeg":
                normalized = "jpg"
            if normalized not in {"jpg", "png", "webp"}:
                raise ValueError("image.convert format must be one of jpg, png, webp")
            if not isinstance(quality, int) or not 1 <= quality <= 100:
                raise ValueError("image.convert quality must be in 1..=100")
            if not set(self.params).issubset(allowed):
                raise ValueError("image.convert only accepts format and quality")
            self.params["format"] = normalized
            self.params["quality"] = quality
        return self


class CreateJobRequest(BaseModel):
    source: str
    sink: str
    transforms: list[TransformSpec]

    @model_validator(mode="after")
    def validate_job(self) -> "CreateJobRequest":
        if not self.source.strip():
            raise ValueError("source must be non-empty")
        if not self.sink.strip():
            raise ValueError("sink must be non-empty")
        if not self.transforms:
            raise ValueError("transforms must be non-empty")
        return self


class JobRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.PENDING
    source: str
    sink: str
    transforms: list[TransformSpec]
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class JobStatusUpdate(BaseModel):
    status: JobStatus


class JobCompletionWebhook(BaseModel):
    job_id: str
    total_bytes_processed: int = 0
