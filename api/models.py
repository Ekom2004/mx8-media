from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import math
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


VIDEO_PRESETS = {
    "ultrafast",
    "superfast",
    "veryfast",
    "faster",
    "fast",
    "medium",
    "slow",
    "slower",
    "veryslow",
}


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


class JobStage(str, Enum):
    SUBMITTED = "SUBMITTED"
    PLANNING = "PLANNING"
    PLANNED = "PLANNED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class JobFailureCategory(str, Enum):
    INPUT_ERROR = "input_error"
    AUTH_ERROR = "auth_error"
    PLANNING_ERROR = "planning_error"
    CAPACITY_ERROR = "capacity_error"
    WORKER_ERROR = "worker_error"
    SINK_ERROR = "sink_error"
    TIMEOUT = "timeout"
    INTERNAL_ERROR = "internal_error"


class AccountRole(str, Enum):
    CUSTOMER = "customer"
    OPERATOR = "operator"


class AuthPrincipal(BaseModel):
    account_id: str
    role: AccountRole = AccountRole.CUSTOMER


def default_stage_for_request(has_find: bool) -> JobStage:
    if has_find:
        return JobStage.PLANNING
    return JobStage.SUBMITTED


def default_stage_for_status(status: JobStatus) -> JobStage:
    if status == JobStatus.FINDING:
        return JobStage.PLANNING
    if status == JobStatus.PLANNED:
        return JobStage.PLANNED
    if status in {JobStatus.PENDING, JobStatus.QUEUED}:
        return JobStage.QUEUED
    if status == JobStatus.RUNNING:
        return JobStage.RUNNING
    if status == JobStatus.COMPLETE:
        return JobStage.COMPLETE
    return JobStage.FAILED


class TransformSpec(BaseModel):
    type: str
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_supported_transforms(self) -> "TransformSpec":
        if self.type == "convert":
            self.type = "image.convert"
        elif self.type == "crop":
            self.type = "image.crop"
        elif self.type == "extract_audio":
            self.type = "video.extract_audio"
        elif self.type == "clip":
            self.type = "video.transcode"
            self.params.setdefault("codec", "h264")
            self.params.setdefault("crf", 23)
        elif self.type == "proxy":
            self.type = "video.transcode"
            self.params.setdefault("codec", "h264")
            self.params.setdefault("crf", 28)
            self.params.setdefault("preset", "veryfast")
        elif self.type == "remux":
            self.type = "video.remux"
        elif self.type == "extract_frames":
            self.type = "video.extract_frames"
        elif self.type == "develop_raw":
            self.type = "image.develop_raw"
        elif self.type == "remove_background":
            self.type = "image.remove_background"
        elif self.type == "normalize":
            self.type = "audio.normalize"
        elif self.type == "resample":
            self.type = "audio.resample"
        elif self.type == "resize":
            media = self.params.get("media")
            if not isinstance(media, str) or not media.strip():
                raise ValueError("resize requires media")
            normalized_media = media.strip().lower()
            if normalized_media == "video":
                self.type = "video.resize"
            elif normalized_media == "image":
                self.type = "image.resize"
            else:
                raise ValueError("resize media must be one of: video, image")
            self.params = {key: value for key, value in self.params.items() if key != "media"}
        elif self.type == "filter":
            media = self.params.get("media")
            if not isinstance(media, str) or not media.strip():
                raise ValueError("filter requires media")
            normalized_media = media.strip().lower()
            if normalized_media == "video":
                self.type = "video.filter"
            elif normalized_media == "image":
                self.type = "image.filter"
            elif normalized_media == "audio":
                self.type = "audio.filter"
            else:
                raise ValueError("filter media must be one of: video, image, audio")
            self.params = {key: value for key, value in self.params.items() if key != "media"}
        elif self.type == "transcode":
            if "codec" in self.params:
                self.type = "video.transcode"
            elif "format" in self.params:
                self.type = "audio.transcode"
            else:
                raise ValueError("transcode requires codec for video or format for audio")

        if self.type == "image.develop_raw":
            if self.params:
                raise ValueError("image.develop_raw does not accept params")
        elif self.type == "image.remove_background":
            if self.params:
                raise ValueError("image.remove_background does not accept params")
        elif self.type == "image.resize":
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
            allowed = {"codec", "crf", "preset"}
            codec = self.params.get("codec")
            crf = self.params.get("crf", 23)
            preset = self.params.get("preset")
            if not isinstance(codec, str):
                raise ValueError("video.transcode requires codec")
            normalized_codec = codec.strip().lower()
            if normalized_codec == "hevc":
                normalized_codec = "h265"
            if normalized_codec not in {"h264", "h265", "av1"}:
                raise ValueError("video.transcode codec must be one of h264, h265, av1")
            if not isinstance(crf, int) or not 0 <= crf <= 51:
                raise ValueError("video.transcode crf must be in 0..=51")
            normalized_preset = None
            if preset is not None:
                if not isinstance(preset, str) or not preset.strip():
                    raise ValueError("video.transcode preset must be non-empty when set")
                normalized_preset = preset.strip().lower()
                if normalized_preset not in VIDEO_PRESETS:
                    raise ValueError("video.transcode preset must be one of ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow")
                if normalized_codec == "av1":
                    raise ValueError("video.transcode preset is only supported for h264 and h265")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.transcode only accepts codec, crf, and preset")
            self.params["codec"] = normalized_codec
            self.params["crf"] = crf
            if normalized_preset is None:
                self.params.pop("preset", None)
            else:
                self.params["preset"] = normalized_preset
        elif self.type == "video.remux":
            allowed = {"container"}
            container = self.params.get("container", "mp4")
            if not isinstance(container, str):
                raise ValueError("video.remux requires container")
            normalized_container = container.strip().lower()
            if normalized_container not in {"mp4"}:
                raise ValueError("video.remux container must be one of mp4")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.remux only accepts container")
            self.params["container"] = normalized_container
        elif self.type == "audio.transcode":
            allowed = {"format", "bitrate"}
            format_value = self.params.get("format")
            bitrate = self.params.get("bitrate", "128k")
            if not isinstance(format_value, str):
                raise ValueError("audio.transcode requires format")
            normalized = format_value.strip().lower()
            if normalized not in {"mp3", "wav", "flac"}:
                raise ValueError("audio.transcode format must be one of mp3, wav, flac")
            if not isinstance(bitrate, str) or not bitrate.strip():
                raise ValueError("audio.transcode bitrate must be non-empty")
            if not set(self.params).issubset(allowed):
                raise ValueError("audio.transcode only accepts format and bitrate")
            self.params["format"] = normalized
            self.params["bitrate"] = bitrate.strip()
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
        elif self.type == "video.filter":
            allowed = {"expr"}
            expr = self.params.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                raise ValueError("video.filter requires expr")
            if not set(self.params).issubset(allowed):
                raise ValueError("video.filter only accepts expr")
            self.params["expr"] = expr.strip()
        elif self.type == "audio.filter":
            allowed = {"expr"}
            expr = self.params.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                raise ValueError("audio.filter requires expr")
            if not set(self.params).issubset(allowed):
                raise ValueError("audio.filter only accepts expr")
            self.params["expr"] = expr.strip()
        elif self.type == "image.filter":
            allowed = {"expr"}
            expr = self.params.get("expr")
            if not isinstance(expr, str) or not expr.strip():
                raise ValueError("image.filter requires expr")
            if not set(self.params).issubset(allowed):
                raise ValueError("image.filter only accepts expr")
            self.params["expr"] = expr.strip()
        return self


class WorkSpec(BaseModel):
    type: str
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_supported_work(self) -> "WorkSpec":
        if self.type == "find":
            allowed = {"query"}
            query = self.params.get("query")
            if not isinstance(query, str):
                raise ValueError("find requires query")
            normalized = query.strip()
            if not normalized:
                raise ValueError("find query must be non-empty")
            if not set(self.params).issubset(allowed):
                raise ValueError("find only accepts query")
            self.params = {"query": normalized}
            return self

        normalized_transform = TransformSpec.model_validate(
            {
                "type": self.type,
                "params": self.params,
            }
        )
        self.type = normalized_transform.type
        self.params = normalized_transform.params
        return self


class CreateJobRequest(BaseModel):
    account_id: str = "local-dev"
    source: str
    sink: str
    find: Optional[str] = None
    transforms: list[TransformSpec] = Field(default_factory=list)
    max_outputs: Optional[int] = None

    @field_validator("find")
    @classmethod
    def normalize_find(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("find must be non-empty")
        return normalized

    @field_validator("max_outputs")
    @classmethod
    def validate_max_outputs(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("max must be > 0")
        return value

    @model_validator(mode="after")
    def validate_job(self) -> "CreateJobRequest":
        if not self.source.strip():
            raise ValueError("source must be non-empty")
        if not self.sink.strip():
            raise ValueError("sink must be non-empty")
        if not self.transforms and self.find is None:
            raise ValueError("job must contain a find operation or at least one transform")
        if self.find is not None:
            if self.transforms and not _is_find_compatible_transform_chain(self.transforms):
                raise ValueError(
                    "find requires a visual-compatible transform chain: "
                    "image transforms, video transforms, video.extract_frames followed by image "
                    "transforms, or video.extract_audio followed by audio transforms"
                )
        return self


def _is_find_compatible_transform_chain(transforms: list[TransformSpec]) -> bool:
    if not transforms:
        return False

    transform_types = [transform.type for transform in transforms]
    if "video.remux" in transform_types:
        return False
    if all(transform_type.startswith("image.") for transform_type in transform_types):
        return True
    head = transform_types[0]
    tail = transform_types[1:]
    if head == "video.extract_frames":
        return all(transform_type.startswith("image.") for transform_type in tail)
    if head == "video.extract_audio":
        return all(transform_type.startswith("audio.") for transform_type in tail)
    if all(transform_type.startswith("video.") for transform_type in transform_types):
        return True
    return False


class SubmitJobRequest(BaseModel):
    input: str
    output: str
    work: list[WorkSpec]
    max: Optional[int] = None

    @model_validator(mode="before")
    @classmethod
    def normalize_request_shape(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        using_canonical = any(key in data for key in ("input", "output", "work"))
        using_legacy = any(key in data for key in ("source", "sink", "find", "transforms"))
        if using_canonical and using_legacy:
            raise ValueError(
                "use either canonical input/work/output fields or legacy source/find/transforms/sink fields"
            )
        if using_canonical:
            return data

        work: list[object] = []
        find_query = data.get("find")
        if find_query is not None:
            work.append({"type": "find", "params": {"query": find_query}})
        work.extend(list(data.get("transforms") or []))
        return {
            "input": data.get("source"),
            "output": data.get("sink"),
            "work": work,
            "max": data.get("max"),
        }

    @field_validator("input", "output")
    @classmethod
    def require_non_empty_path(cls, value: str, info: Any) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{info.field_name} must be non-empty")
        return normalized

    @field_validator("max")
    @classmethod
    def validate_max(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("max must be > 0")
        return value

    @model_validator(mode="after")
    def validate_job(self) -> "SubmitJobRequest":
        self.to_internal()
        return self

    def to_internal(self) -> CreateJobRequest:
        if not self.work:
            raise ValueError("work must contain at least one operation")

        find_query: str | None = None
        transforms: list[TransformSpec] = []
        for index, item in enumerate(self.work):
            if item.type == "find":
                if find_query is not None:
                    raise ValueError("work currently supports at most one find operation")
                if index != 0:
                    raise ValueError("find must be the first work item")
                find_query = str(item.params["query"])
                continue
            transforms.append(TransformSpec(type=item.type, params=dict(item.params)))

        if not transforms and find_query is None:
            raise ValueError("work must contain at least one operation")

        return CreateJobRequest(
            source=self.input,
            sink=self.output,
            find=find_query,
            transforms=transforms,
            max_outputs=self.max,
        )


class JobRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    account_id: str = "local-dev"
    status: JobStatus = JobStatus.PENDING
    stage: JobStage = JobStage.SUBMITTED
    source: str
    sink: str
    find: Optional[str] = None
    transforms: list[TransformSpec]
    max_outputs: Optional[int] = None
    region: Optional[str] = None
    instance_type: Optional[str] = None
    manifest_hash: Optional[str] = None
    matched_assets: Optional[int] = None
    matched_segments: Optional[int] = None
    total_objects: Optional[int] = None
    total_bytes: Optional[int] = None
    completed_objects: int = 0
    completed_bytes: int = 0
    outputs_written: int = 0
    current_workers: int = 0
    desired_workers: int = 0
    worker_pool: Optional[str] = None
    failure_category: Optional[JobFailureCategory] = None
    failure_message: Optional[str] = None
    events: list["JobEvent"] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class JobView(BaseModel):
    id: str
    account_id: str
    status: JobStatus
    stage: JobStage
    input: str
    output: str
    work: list[WorkSpec]
    max: Optional[int] = None
    region: Optional[str] = None
    instance_type: Optional[str] = None
    manifest_hash: Optional[str] = None
    matched_assets: Optional[int] = None
    matched_segments: Optional[int] = None
    total_objects: Optional[int] = None
    total_bytes: Optional[int] = None
    completed_objects: int = 0
    completed_bytes: int = 0
    outputs_written: int = 0
    current_workers: int = 0
    desired_workers: int = 0
    worker_pool: Optional[str] = None
    failure_category: Optional[JobFailureCategory] = None
    failure_message: Optional[str] = None
    events: list["JobEvent"] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_record(cls, record: JobRecord) -> "JobView":
        work: list[WorkSpec] = []
        if record.find is not None:
            work.append(WorkSpec(type="find", params={"query": record.find}))
        work.extend(
            WorkSpec.model_validate(transform.model_dump(mode="json"))
            for transform in record.transforms
        )
        return cls(
            id=record.id,
            account_id=record.account_id,
            status=record.status,
            stage=record.stage,
            input=record.source,
            output=record.sink,
            work=work,
            max=record.max_outputs,
            region=record.region,
            instance_type=record.instance_type,
            manifest_hash=record.manifest_hash,
            matched_assets=record.matched_assets,
            matched_segments=record.matched_segments,
            total_objects=record.total_objects,
            total_bytes=record.total_bytes,
            completed_objects=record.completed_objects,
            completed_bytes=record.completed_bytes,
            outputs_written=record.outputs_written,
            current_workers=record.current_workers,
            desired_workers=record.desired_workers,
            worker_pool=record.worker_pool,
            failure_category=record.failure_category,
            failure_message=record.failure_message,
            events=record.events,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


class JobEvent(BaseModel):
    at: datetime = Field(default_factory=utc_now)
    type: str
    status: Optional[JobStatus] = None
    stage: Optional[JobStage] = None
    message: Optional[str] = None
    failure_category: Optional[JobFailureCategory] = None
    failure_message: Optional[str] = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class JobStatusUpdate(BaseModel):
    status: JobStatus
    stage: Optional[JobStage] = None
    failure_category: Optional[JobFailureCategory] = None
    failure_message: Optional[str] = None
    worker_pool: Optional[str] = None
    event_type: Optional[str] = None
    event_message: Optional[str] = None
    event_metadata: dict[str, Any] = Field(default_factory=dict)


class JobProgressUpdate(BaseModel):
    job_id: str
    status: Optional[JobStatus] = None
    stage: Optional[JobStage] = None
    manifest_hash: Optional[str] = None
    matched_assets: Optional[int] = None
    matched_segments: Optional[int] = None
    completed_objects: Optional[int] = None
    completed_bytes: Optional[int] = None
    outputs_written: Optional[int] = None
    current_workers: Optional[int] = None
    desired_workers: Optional[int] = None
    worker_pool: Optional[str] = None
    region: Optional[str] = None
    instance_type: Optional[str] = None
    total_objects: Optional[int] = None
    total_bytes: Optional[int] = None
    failure_category: Optional[JobFailureCategory] = None
    failure_message: Optional[str] = None
    clear_failure: bool = False
    event_type: Optional[str] = None
    event_message: Optional[str] = None
    event_metadata: dict[str, Any] = Field(default_factory=dict)


class JobCompletionWebhook(BaseModel):
    job_id: str
    total_bytes_processed: int = 0
    total_objects_processed: int = 0
