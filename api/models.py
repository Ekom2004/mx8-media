from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobStatus(str, Enum):
    FINDING = "FINDING"
    PLANNED = "PLANNED"
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
        elif self.type == "video.transcode":
            allowed = {"codec", "crf"}
            codec = self.params.get("codec")
            crf = self.params.get("crf", 23)
            if not isinstance(codec, str):
                raise ValueError("video.transcode requires codec")
            normalized_codec = codec.strip().lower()
            if normalized_codec == "hevc":
                normalized_codec = "h265"
            if normalized_codec not in {"h264", "h265", "av1"}:
                raise ValueError("video.transcode codec must be one of h264, h265, av1")
            if not isinstance(crf, int) or not 0 <= crf <= 51:
                raise ValueError("video.transcode crf must be in 0..=51")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.transcode only accepts codec and crf")
            self.params["codec"] = normalized_codec
            self.params["crf"] = crf
        elif self.type == "video.resize":
            allowed = {"width", "height", "maintain_aspect"}
            width = self.params.get("width")
            height = self.params.get("height")
            if not isinstance(width, int) or width <= 0:
                raise ValueError("video.resize requires width > 0")
            if not isinstance(height, int) or height <= 0:
                raise ValueError("video.resize requires height > 0")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.resize only accepts width, height, maintain_aspect")
        elif self.type == "video.extract_audio":
            allowed = {"format", "bitrate"}
            format_value = self.params.get("format")
            bitrate = self.params.get("bitrate", "128k")
            if not isinstance(format_value, str):
                raise ValueError("video.extract_audio requires format")
            normalized = format_value.strip().lower()
            if normalized not in {"mp3", "wav", "flac"}:
                raise ValueError("video.extract_audio format must be one of mp3, wav, flac")
            if not isinstance(bitrate, str) or not bitrate.strip():
                raise ValueError("video.extract_audio bitrate must be non-empty")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.extract_audio only accepts format and bitrate")
            self.params["format"] = normalized
            self.params["bitrate"] = bitrate.strip()
        elif self.type == "video.extract_frames":
            allowed = {"fps", "format"}
            fps = self.params.get("fps")
            format_value = self.params.get("format")
            if not isinstance(fps, (int, float)) or fps <= 0:
                raise ValueError("video.extract_frames fps must be > 0")
            if not isinstance(format_value, str):
                raise ValueError("video.extract_frames requires format")
            normalized = format_value.strip().lower()
            if normalized == "jpeg":
                normalized = "jpg"
            if normalized not in {"jpg", "png"}:
                raise ValueError("video.extract_frames format must be one of jpg, png")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.extract_frames only accepts fps and format")
            self.params["fps"] = float(fps)
            self.params["format"] = normalized
        elif self.type == "audio.resample":
            allowed = {"rate", "channels"}
            rate = self.params.get("rate")
            channels = self.params.get("channels", 1)
            if not isinstance(rate, int) or rate <= 0:
                raise ValueError("audio.resample requires rate > 0")
            if not isinstance(channels, int) or channels <= 0:
                raise ValueError("audio.resample requires channels > 0")
            if not set(self.params).issubset(allowed):
                raise ValueError("audio.resample only accepts rate and channels")
            self.params["channels"] = channels
        elif self.type == "audio.normalize":
            allowed = {"loudness"}
            loudness = self.params.get("loudness", -14.0)
            if not isinstance(loudness, (int, float)) or not math.isfinite(loudness):
                raise ValueError("audio.normalize loudness must be finite")
            if not set(self.params).issubset(allowed):
                raise ValueError("audio.normalize only accepts loudness")
            self.params["loudness"] = float(loudness)
        return self


class CreateJobRequest(BaseModel):
    source: str
    sink: str
    find: Optional[str] = None
    transforms: list[TransformSpec]

    @field_validator("find")
    @classmethod
    def normalize_find(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("find must be non-empty")
        return normalized

    @model_validator(mode="after")
    def validate_job(self) -> "CreateJobRequest":
        if not self.source.strip():
            raise ValueError("source must be non-empty")
        if not self.sink.strip():
            raise ValueError("sink must be non-empty")
        if not self.transforms:
            raise ValueError("transforms must be non-empty")
        if self.find is not None:
            if self.transforms[0].type != "video.extract_frames":
                raise ValueError("find currently requires video.extract_frames as the first transform")
            if not all(transform.type.startswith("image.") for transform in self.transforms[1:]):
                raise ValueError(
                    "find currently supports only image transforms after video.extract_frames"
                )
        return self


class JobRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    status: JobStatus = JobStatus.PENDING
    source: str
    sink: str
    find: Optional[str] = None
    transforms: list[TransformSpec]
    region: Optional[str] = None
    instance_type: Optional[str] = None
    manifest_hash: Optional[str] = None
    matched_assets: Optional[int] = None
    matched_segments: Optional[int] = None
    total_objects: Optional[int] = None
    total_bytes: Optional[int] = None
    completed_objects: int = 0
    completed_bytes: int = 0
    current_workers: int = 0
    desired_workers: int = 0
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class JobStatusUpdate(BaseModel):
    status: JobStatus


class JobProgressUpdate(BaseModel):
    job_id: str
    status: Optional[JobStatus] = None
    manifest_hash: Optional[str] = None
    matched_assets: Optional[int] = None
    matched_segments: Optional[int] = None
    completed_objects: Optional[int] = None
    completed_bytes: Optional[int] = None
    current_workers: Optional[int] = None
    desired_workers: Optional[int] = None
    region: Optional[str] = None
    instance_type: Optional[str] = None
    total_objects: Optional[int] = None
    total_bytes: Optional[int] = None


class JobCompletionWebhook(BaseModel):
    job_id: str
    total_bytes_processed: int = 0
    total_objects_processed: int = 0
