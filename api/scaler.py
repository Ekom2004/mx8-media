from __future__ import annotations

import math
import os
import threading
from dataclasses import dataclass

from .launcher import CoordinatorLauncher
from .models import JobProgressUpdate, JobRecord, JobStatus
from .storage import JobStore

# `FINDING` jobs are planner-owned. `PLANNED` remains a transient planner state but
# matched jobs should normally advance into the active scaler path immediately after
# planning succeeds.
ACTIVE_JOB_STATUSES = {JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING}
TERMINAL_JOB_STATUSES = {JobStatus.COMPLETE, JobStatus.FAILED}

TRANSFORM_WEIGHTS: dict[str, float] = {
    "image.resize": 1.0,
    "image.crop": 1.0,
    "image.convert": 1.0,
    "audio.resample": 3.0,
    "audio.normalize": 3.0,
    "video.resize": 6.0,
    "video.extract_audio": 8.0,
    "video.transcode": 12.0,
    "video.extract_frames": 16.0,
}

PER_OBJECT_OVERHEAD: dict[str, float] = {
    "image.resize": 0.02,
    "image.crop": 0.02,
    "image.convert": 0.02,
    "audio.resample": 0.05,
    "audio.normalize": 0.05,
    "video.resize": 0.2,
    "video.extract_audio": 0.25,
    "video.transcode": 0.3,
    "video.extract_frames": 0.5,
}

WORKER_THROUGHPUT: dict[str, float] = {
    "image": 25.0,
    "audio": 15.0,
    "video": 6.0,
    "mixed": 6.0,
}


@dataclass(frozen=True)
class FleetPlan:
    worker_count: int
    region: str
    instance_type: str


class JobScaler:
    def __init__(self, store: JobStore, launcher: CoordinatorLauncher) -> None:
        self._store = store
        self._launcher = launcher
        self._poll_secs = max(1.0, float(os.getenv("MX8_SCALER_POLL_SECS", "2")))
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self, api_base_url: str) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(api_base_url,),
            name="mx8-media-scaler",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None

    def wake(self) -> None:
        self._wake_event.set()

    def _run(self, api_base_url: str) -> None:
        while not self._stop_event.is_set():
            self._reconcile(api_base_url)
            self._wake_event.wait(self._poll_secs)
            self._wake_event.clear()

    def _reconcile(self, api_base_url: str) -> None:
        for record in self._store.list_jobs():
            if record.status in TERMINAL_JOB_STATUSES:
                self._launcher.terminate_job(record.id)
                if record.current_workers != 0 or record.desired_workers != 0:
                    self._store.update_job_progress(
                        JobProgressUpdate(
                            job_id=record.id,
                            current_workers=0,
                            desired_workers=0,
                        )
                    )
                continue
            if record.status not in ACTIVE_JOB_STATUSES:
                continue
            record = self._ensure_launch(record, api_base_url)
            if record is None:
                continue
            plan = self._plan(record)
            if (
                record.desired_workers != plan.worker_count
                or record.region != plan.region
                or record.instance_type != plan.instance_type
            ):
                updated = self._store.update_job_progress(
                    JobProgressUpdate(
                        job_id=record.id,
                        desired_workers=plan.worker_count,
                        region=plan.region,
                        instance_type=plan.instance_type,
                    )
                )
                if updated is not None:
                    record = updated
            self._launcher.scale_workers(
                record,
                api_base_url,
                plan.worker_count,
                plan.region,
                plan.instance_type,
            )

    def _ensure_launch(self, record: JobRecord, api_base_url: str) -> JobRecord | None:
        if record.status == JobStatus.PENDING:
            queued = self._store.update_job_status(record.id, JobStatus.QUEUED)
            if queued is None:
                return None
            record = queued
        try:
            self._launcher.launch(record, api_base_url)
        except Exception:
            self._store.update_job_progress(
                JobProgressUpdate(
                    job_id=record.id,
                    status=JobStatus.FAILED,
                    current_workers=0,
                    desired_workers=0,
                )
            )
            self._launcher.terminate_job(record.id)
            return None
        return record

    def _plan(self, record: JobRecord) -> FleetPlan:
        media_family = self._media_family(record)
        total_objects = max(0, record.total_objects or 0)
        total_bytes = max(0, record.total_bytes or 0)
        remaining_objects = max(0, total_objects - max(0, record.completed_objects))
        remaining_bytes = max(0, total_bytes - max(0, record.completed_bytes))
        if remaining_objects == 0 and total_objects == 0:
            remaining_objects = 1

        estimated_work = self._estimated_work(record, remaining_objects, remaining_bytes)
        worker_throughput = WORKER_THROUGHPUT[media_family]
        required_workers = max(1, math.ceil(estimated_work / worker_throughput))
        worker_count = min(self._max_workers(), required_workers)
        return FleetPlan(
            worker_count=worker_count,
            region=self._region(record),
            instance_type=self._instance_type(media_family, record),
        )

    def _estimated_work(
        self,
        record: JobRecord,
        remaining_objects: int,
        remaining_bytes: int,
    ) -> float:
        total_weight = 0.0
        total_object_overhead = 0.0
        for transform in record.transforms:
            total_weight += TRANSFORM_WEIGHTS.get(transform.type, 1.0)
            total_object_overhead += PER_OBJECT_OVERHEAD.get(transform.type, 0.05)
        if total_weight <= 0:
            total_weight = 1.0
        remaining_gb = remaining_bytes / float(1024**3)
        return (remaining_gb * total_weight) + (remaining_objects * total_object_overhead)

    def _media_family(self, record: JobRecord) -> str:
        families = {transform.type.split(".", 1)[0] for transform in record.transforms}
        if len(families) == 1:
            family = next(iter(families))
            if family in WORKER_THROUGHPUT:
                return family
        return "mixed"

    def _region(self, record: JobRecord) -> str:
        if record.source.startswith("file://") or record.source.startswith("/"):
            return "local"
        return os.getenv("MX8_AWS_REGION", "us-east-1")

    def _instance_type(self, media_family: str, record: JobRecord) -> str:
        if self._region(record) == "local":
            return f"local.{media_family}"
        if media_family == "video":
            return os.getenv("MX8_VIDEO_INSTANCE_TYPE", "g4dn.xlarge")
        if media_family == "audio":
            return os.getenv("MX8_AUDIO_INSTANCE_TYPE", "c7i.xlarge")
        return os.getenv("MX8_IMAGE_INSTANCE_TYPE", "c7i.xlarge")

    def _max_workers(self) -> int:
        return max(1, int(os.getenv("MX8_SCALE_MAX_WORKERS", "8")))
