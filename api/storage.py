from __future__ import annotations

import os
import threading
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .models import (
    CreateJobRequest,
    JobEvent,
    JobFailureCategory,
    JobProgressUpdate,
    JobRecord,
    JobStage,
    JobStatus,
    TransformSpec,
    default_stage_for_request,
    default_stage_for_status,
    utc_now,
)

MAX_JOB_EVENTS = 50


class JobStore(Protocol):
    def create_job(self, request: CreateJobRequest) -> JobRecord: ...

    def get_job(self, job_id: str, *, account_id: str | None = None) -> JobRecord | None: ...

    def list_jobs(self, *, account_id: str | None = None) -> Iterable[JobRecord]: ...

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None: ...

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None: ...


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        record = JobRecord(
            account_id=request.account_id,
            status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
            stage=default_stage_for_request(request.find is not None),
            source=request.source,
            sink=request.sink,
            find=request.find,
            transforms=request.transforms,
            max_outputs=request.max_outputs,
            events=[
                JobEvent(
                    type="job_submitted",
                    status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
                    stage=default_stage_for_request(request.find is not None),
                    message="Job submitted",
                    metadata={"source": request.source, "sink": request.sink},
                )
            ],
        )
        with self._lock:
            self._jobs[record.id] = record
        return record

    def get_job(self, job_id: str, *, account_id: str | None = None) -> JobRecord | None:
        with self._lock:
            record = self._jobs.get(job_id)
        if record is None:
            return None
        if account_id is not None and record.account_id != account_id:
            return None
        return record

    def list_jobs(self, *, account_id: str | None = None) -> list[JobRecord]:
        with self._lock:
            jobs = list(self._jobs.values())
        if account_id is not None:
            jobs = [job for job in jobs if job.account_id == account_id]
        return sorted(jobs, key=lambda job: job.created_at, reverse=True)

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                return None
            updated = _apply_progress_update(
                record,
                JobProgressUpdate(job_id=job_id, status=status),
            )
            self._jobs[job_id] = updated
        return updated

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None:
        with self._lock:
            record = self._jobs.get(payload.job_id)
            if record is None:
                return None
            updated = _apply_progress_update(record, payload)
            self._jobs[payload.job_id] = updated
        return updated


class PostgresJobStore:
    def __init__(self, dsn: str, schema_dsn: str | None = None) -> None:
        self._dsn = _normalize_postgres_dsn(dsn)
        self._schema_dsn = _normalize_postgres_dsn(schema_dsn or dsn)
        self._connect, self._dict_row, self._jsonb = _load_psycopg()
        self._ensure_schema()

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        record = JobRecord(
            account_id=request.account_id,
            status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
            stage=default_stage_for_request(request.find is not None),
            source=request.source,
            sink=request.sink,
            find=request.find,
            transforms=request.transforms,
            max_outputs=request.max_outputs,
            events=[
                JobEvent(
                    type="job_submitted",
                    status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
                    stage=default_stage_for_request(request.find is not None),
                    message="Job submitted",
                    metadata={"source": request.source, "sink": request.sink},
                )
            ],
        )
        transforms = [transform.model_dump(mode="json") for transform in record.transforms]
        events = [event.model_dump(mode="json") for event in record.events]
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    insert into jobs (
                        id,
                        account_id,
                        status,
                        stage,
                        source,
                        sink,
                        find_query,
                        transforms,
                        max_outputs,
                        region,
                        instance_type,
                        manifest_hash,
                        matched_assets,
                        matched_segments,
                        total_objects,
                        total_bytes,
                        completed_objects,
                        completed_bytes,
                        outputs_written,
                        current_workers,
                        desired_workers,
                        worker_pool,
                        failure_category,
                        failure_message,
                        events,
                        created_at,
                        updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning
                        id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                        worker_pool, failure_category, failure_message, events,
                        created_at, updated_at
                    """,
                    (
                        record.id,
                        record.account_id,
                        record.status.value,
                        record.stage.value,
                        record.source,
                        record.sink,
                        record.find,
                        self._jsonb(transforms),
                        record.max_outputs,
                        record.region,
                        record.instance_type,
                        record.manifest_hash,
                        record.matched_assets,
                        record.matched_segments,
                        record.total_objects,
                        record.total_bytes,
                        record.completed_objects,
                        record.completed_bytes,
                        record.outputs_written,
                        record.current_workers,
                        record.desired_workers,
                        record.worker_pool,
                        record.failure_category.value if record.failure_category is not None else None,
                        record.failure_message,
                        self._jsonb(events),
                        record.created_at,
                        record.updated_at,
                    ),
                )
                row = cur.fetchone()
        return self._row_to_job(row)

    def get_job(self, job_id: str, *, account_id: str | None = None) -> JobRecord | None:
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                if account_id is None:
                    cur.execute(
                        """
                        select
                            id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                            manifest_hash,
                            matched_assets, matched_segments, total_objects, total_bytes,
                            completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                            worker_pool, failure_category, failure_message, events,
                            created_at, updated_at
                        from jobs
                        where id = %s
                        """,
                        (job_id,),
                    )
                else:
                    cur.execute(
                        """
                        select
                            id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                            manifest_hash,
                            matched_assets, matched_segments, total_objects, total_bytes,
                            completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                            worker_pool, failure_category, failure_message, events,
                            created_at, updated_at
                        from jobs
                        where id = %s and account_id = %s
                        """,
                        (job_id, account_id),
                    )
                row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def list_jobs(self, *, account_id: str | None = None) -> list[JobRecord]:
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                if account_id is None:
                    cur.execute(
                        """
                        select
                            id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                            manifest_hash,
                            matched_assets, matched_segments, total_objects, total_bytes,
                            completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                            worker_pool, failure_category, failure_message, events,
                            created_at, updated_at
                        from jobs
                        order by created_at desc
                        """
                    )
                else:
                    cur.execute(
                        """
                        select
                            id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                            manifest_hash,
                            matched_assets, matched_segments, total_objects, total_bytes,
                            completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                            worker_pool, failure_category, failure_message, events,
                            created_at, updated_at
                        from jobs
                        where account_id = %s
                        order by created_at desc
                        """,
                        (account_id,),
                    )
                rows = cur.fetchall()
        return [self._row_to_job(row) for row in rows]

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None:
        record = self.get_job(job_id)
        if record is None:
            return None
        updated_record = _apply_progress_update(
            record,
            JobProgressUpdate(job_id=job_id, status=status),
        )
        return self._persist_job(updated_record)

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None:
        record = self.get_job(payload.job_id)
        if record is None:
            return None
        updated_record = _apply_progress_update(record, payload)
        return self._persist_job(updated_record)

    def _ensure_schema(self) -> None:
        try:
            self._ensure_schema_with_dsn(self._schema_dsn)
        except Exception:
            if self._schema_dsn == self._dsn:
                raise
            self._ensure_schema_with_dsn(self._dsn)

    def _ensure_schema_with_dsn(self, dsn: str) -> None:
        with self._connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    create table if not exists jobs (
                        id text primary key,
                        account_id text not null,
                        status text not null,
                        stage text not null,
                        source text not null,
                        sink text not null,
                        find_query text,
                        transforms jsonb not null,
                        max_outputs bigint,
                        region text,
                        instance_type text,
                        manifest_hash text,
                        matched_assets bigint,
                        matched_segments bigint,
                        total_objects bigint,
                        total_bytes bigint,
                        completed_objects bigint not null default 0,
                        completed_bytes bigint not null default 0,
                        outputs_written bigint not null default 0,
                        current_workers integer not null default 0,
                        desired_workers integer not null default 0,
                        worker_pool text,
                        failure_category text,
                        failure_message text,
                        events jsonb not null default '[]'::jsonb,
                        created_at timestamptz not null,
                        updated_at timestamptz not null
                    )
                    """
                )
                cur.execute("alter table jobs add column if not exists stage text")
                cur.execute("alter table jobs add column if not exists account_id text")
                cur.execute("alter table jobs add column if not exists region text")
                cur.execute("alter table jobs add column if not exists instance_type text")
                cur.execute("alter table jobs add column if not exists find_query text")
                cur.execute("alter table jobs add column if not exists max_outputs bigint")
                cur.execute("alter table jobs add column if not exists manifest_hash text")
                cur.execute("alter table jobs add column if not exists matched_assets bigint")
                cur.execute("alter table jobs add column if not exists matched_segments bigint")
                cur.execute("alter table jobs add column if not exists total_objects bigint")
                cur.execute("alter table jobs add column if not exists total_bytes bigint")
                cur.execute(
                    "alter table jobs add column if not exists completed_objects bigint not null default 0"
                )
                cur.execute(
                    "alter table jobs add column if not exists completed_bytes bigint not null default 0"
                )
                cur.execute(
                    "alter table jobs add column if not exists outputs_written bigint not null default 0"
                )
                cur.execute(
                    "alter table jobs add column if not exists current_workers integer not null default 0"
                )
                cur.execute(
                    "alter table jobs add column if not exists desired_workers integer not null default 0"
                )
                cur.execute("alter table jobs add column if not exists worker_pool text")
                cur.execute("alter table jobs add column if not exists failure_category text")
                cur.execute("alter table jobs add column if not exists failure_message text")
                cur.execute(
                    "alter table jobs add column if not exists events jsonb not null default '[]'::jsonb"
                )
                cur.execute(
                    "update jobs set stage = status where stage is null and status in ('PLANNED', 'RUNNING', 'COMPLETE', 'FAILED')"
                )
                cur.execute(
                    "update jobs set stage = 'PLANNING' where stage is null and status = 'FINDING'"
                )
                cur.execute(
                    "update jobs set stage = 'QUEUED' where stage is null and status in ('PENDING', 'QUEUED')"
                )

    def _row_to_job(self, row: object) -> JobRecord:
        if row is None:
            raise ValueError("row must not be None")
        data = cast(dict[str, object], row)
        transforms = [
            TransformSpec.model_validate(transform)
            for transform in cast(list[object], data["transforms"])
        ]
        raw_events = cast(list[object] | None, data.get("events")) or []
        events = [JobEvent.model_validate(event) for event in raw_events]
        return JobRecord(
            id=str(data["id"]),
            account_id=str(data.get("account_id") or "local-dev"),
            status=JobStatus(str(data["status"])),
            stage=JobStage(str(data.get("stage") or default_stage_for_status(JobStatus(str(data["status"]))).value)),
            source=str(data["source"]),
            sink=str(data["sink"]),
            find=cast(str | None, data.get("find_query")),
            transforms=transforms,
            max_outputs=cast(int | None, data.get("max_outputs")),
            region=cast(str | None, data.get("region")),
            instance_type=cast(str | None, data.get("instance_type")),
            manifest_hash=cast(str | None, data.get("manifest_hash")),
            matched_assets=cast(int | None, data.get("matched_assets")),
            matched_segments=cast(int | None, data.get("matched_segments")),
            total_objects=cast(int | None, data.get("total_objects")),
            total_bytes=cast(int | None, data.get("total_bytes")),
            completed_objects=int(cast(int | None, data.get("completed_objects")) or 0),
            completed_bytes=int(cast(int | None, data.get("completed_bytes")) or 0),
            outputs_written=int(cast(int | None, data.get("outputs_written")) or 0),
            current_workers=int(cast(int | None, data.get("current_workers")) or 0),
            desired_workers=int(cast(int | None, data.get("desired_workers")) or 0),
            worker_pool=cast(str | None, data.get("worker_pool")),
            failure_category=(
                JobFailureCategory(str(data["failure_category"]))
                if data.get("failure_category") is not None
                else None
            ),
            failure_message=cast(str | None, data.get("failure_message")),
            events=events,
            created_at=cast(datetime, data["created_at"]),
            updated_at=cast(datetime, data["updated_at"]),
        )

    def _persist_job(self, record: JobRecord) -> JobRecord:
        transforms = [transform.model_dump(mode="json") for transform in record.transforms]
        events = [event.model_dump(mode="json") for event in record.events]
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    update jobs
                    set
                        status = %s,
                        account_id = %s,
                        stage = %s,
                        source = %s,
                        sink = %s,
                        find_query = %s,
                        transforms = %s,
                        max_outputs = %s,
                        region = %s,
                        instance_type = %s,
                        manifest_hash = %s,
                        matched_assets = %s,
                        matched_segments = %s,
                        total_objects = %s,
                        total_bytes = %s,
                        completed_objects = %s,
                        completed_bytes = %s,
                        outputs_written = %s,
                        current_workers = %s,
                        desired_workers = %s,
                        worker_pool = %s,
                        failure_category = %s,
                        failure_message = %s,
                        events = %s,
                        updated_at = %s
                    where id = %s
                    returning
                        id, account_id, status, stage, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, outputs_written, current_workers, desired_workers,
                        worker_pool, failure_category, failure_message, events,
                        created_at, updated_at
                    """,
                    (
                        record.status.value,
                        record.account_id,
                        record.stage.value,
                        record.source,
                        record.sink,
                        record.find,
                        self._jsonb(transforms),
                        record.max_outputs,
                        record.region,
                        record.instance_type,
                        record.manifest_hash,
                        record.matched_assets,
                        record.matched_segments,
                        record.total_objects,
                        record.total_bytes,
                        record.completed_objects,
                        record.completed_bytes,
                        record.outputs_written,
                        record.current_workers,
                        record.desired_workers,
                        record.worker_pool,
                        record.failure_category.value if record.failure_category is not None else None,
                        record.failure_message,
                        self._jsonb(events),
                        record.updated_at,
                        record.id,
                    ),
                )
                row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"job disappeared during update: {record.id}")
        return self._row_to_job(row)


def build_job_store() -> JobStore:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        direct_url = os.getenv("DIRECT_URL", "").strip() or None
        return PostgresJobStore(database_url, schema_dsn=direct_url)
    return InMemoryJobStore()


def _load_psycopg() -> tuple[object, object, object]:
    try:
        from psycopg import connect
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as err:
        raise RuntimeError(
            "psycopg is required for Postgres-backed job storage. Install api dependencies first."
        ) from err
    return connect, dict_row, Jsonb


def _normalize_postgres_dsn(dsn: str) -> str:
    parts = urlsplit(dsn)
    filtered_query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() != "pgbouncer"
    ]
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(filtered_query), parts.fragment)
    )


def _apply_progress_update(record: JobRecord, payload: JobProgressUpdate) -> JobRecord:
    updated_at = utc_now()
    next_status = payload.status or record.status
    next_stage = payload.stage or (
        default_stage_for_status(next_status) if payload.status is not None else record.stage
    )
    update: dict[str, object] = {
        "status": next_status,
        "stage": next_stage,
        "updated_at": updated_at,
    }
    if payload.manifest_hash is not None:
        update["manifest_hash"] = payload.manifest_hash
    if payload.matched_assets is not None:
        update["matched_assets"] = payload.matched_assets
    if payload.matched_segments is not None:
        update["matched_segments"] = payload.matched_segments
    if payload.completed_objects is not None:
        update["completed_objects"] = payload.completed_objects
        if payload.outputs_written is None:
            update["outputs_written"] = payload.completed_objects
    if payload.completed_bytes is not None:
        update["completed_bytes"] = payload.completed_bytes
    if payload.outputs_written is not None:
        update["outputs_written"] = payload.outputs_written
    if payload.current_workers is not None:
        update["current_workers"] = payload.current_workers
    if payload.desired_workers is not None:
        update["desired_workers"] = payload.desired_workers
    if payload.worker_pool is not None:
        update["worker_pool"] = payload.worker_pool
    if payload.region is not None:
        update["region"] = payload.region
    if payload.instance_type is not None:
        update["instance_type"] = payload.instance_type
    if payload.total_objects is not None:
        update["total_objects"] = payload.total_objects
    if payload.total_bytes is not None:
        update["total_bytes"] = payload.total_bytes
    if payload.clear_failure:
        update["failure_category"] = None
        update["failure_message"] = None
    if payload.failure_category is not None:
        update["failure_category"] = payload.failure_category
    if payload.failure_message is not None:
        update["failure_message"] = payload.failure_message

    next_record = record.model_copy(update=update)
    next_record = next_record.model_copy(
        update={"events": _next_events(record, next_record, payload, updated_at)}
    )
    return next_record


def _next_events(
    previous: JobRecord,
    current: JobRecord,
    payload: JobProgressUpdate,
    event_at: datetime,
) -> list[JobEvent]:
    events = list(previous.events)
    if payload.event_type is not None:
        events.append(
            JobEvent(
                at=event_at,
                type=payload.event_type,
                status=current.status,
                stage=current.stage,
                message=payload.event_message,
                failure_category=current.failure_category,
                failure_message=current.failure_message,
                metadata=dict(payload.event_metadata),
            )
        )
        return events[-MAX_JOB_EVENTS:]

    if current.status != previous.status:
        events.append(_default_status_event(current, event_at))
    elif current.stage != previous.stage:
        events.append(
            JobEvent(
                at=event_at,
                type="job_stage_updated",
                status=current.status,
                stage=current.stage,
                message=f"Job stage is now {current.stage.value}",
            )
        )
    elif (
        current.failure_category != previous.failure_category
        or current.failure_message != previous.failure_message
    ) and (current.failure_category is not None or current.failure_message is not None):
        events.append(
            JobEvent(
                at=event_at,
                type="job_failed",
                status=current.status,
                stage=current.stage,
                message=current.failure_message or "Job failed",
                failure_category=current.failure_category,
                failure_message=current.failure_message,
            )
        )
    return events[-MAX_JOB_EVENTS:]


def _default_status_event(record: JobRecord, event_at: datetime) -> JobEvent:
    message_map = {
        JobStatus.FINDING: "Planner started preparing the job",
        JobStatus.PLANNED: "Planner finished preparing the job",
        JobStatus.PENDING: "Job is waiting for scheduler handoff",
        JobStatus.QUEUED: "Job is queued for workers",
        JobStatus.RUNNING: "Workers are running transforms",
        JobStatus.COMPLETE: "Job completed successfully",
        JobStatus.FAILED: record.failure_message or "Job failed",
    }
    event_type_map = {
        JobStatus.FINDING: "planner_started",
        JobStatus.PLANNED: "planner_completed",
        JobStatus.PENDING: "job_pending",
        JobStatus.QUEUED: "job_queued",
        JobStatus.RUNNING: "job_running",
        JobStatus.COMPLETE: "job_completed",
        JobStatus.FAILED: "job_failed",
    }
    return JobEvent(
        at=event_at,
        type=event_type_map[record.status],
        status=record.status,
        stage=record.stage,
        message=message_map[record.status],
        failure_category=record.failure_category,
        failure_message=record.failure_message,
    )
