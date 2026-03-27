from __future__ import annotations

import os
import threading
from collections.abc import Iterable
from datetime import datetime
from typing import Protocol, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .models import (
    CreateJobRequest,
    JobProgressUpdate,
    JobRecord,
    JobStatus,
    TransformSpec,
    utc_now,
)


class JobStore(Protocol):
    def create_job(self, request: CreateJobRequest) -> JobRecord: ...

    def get_job(self, job_id: str) -> JobRecord | None: ...

    def list_jobs(self) -> Iterable[JobRecord]: ...

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None: ...

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None: ...


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.Lock()

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        record = JobRecord(
            status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
            source=request.source,
            sink=request.sink,
            find=request.find,
            transforms=request.transforms,
            max_outputs=request.max_outputs,
        )
        with self._lock:
            self._jobs[record.id] = record
        return record

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobRecord]:
        with self._lock:
            jobs = list(self._jobs.values())
        return sorted(jobs, key=lambda job: job.created_at, reverse=True)

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None:
        with self._lock:
            record = self._jobs.get(job_id)
            if record is None:
                return None
            updated = record.model_copy(
                update={
                    "status": status,
                    "updated_at": utc_now(),
                }
            )
            self._jobs[job_id] = updated
        return updated

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None:
        with self._lock:
            record = self._jobs.get(payload.job_id)
            if record is None:
                return None
            update: dict[str, object] = {"updated_at": utc_now()}
            if payload.status is not None:
                update["status"] = payload.status
            if payload.manifest_hash is not None:
                update["manifest_hash"] = payload.manifest_hash
            if payload.matched_assets is not None:
                update["matched_assets"] = payload.matched_assets
            if payload.matched_segments is not None:
                update["matched_segments"] = payload.matched_segments
            if payload.completed_objects is not None:
                update["completed_objects"] = payload.completed_objects
            if payload.completed_bytes is not None:
                update["completed_bytes"] = payload.completed_bytes
            if payload.current_workers is not None:
                update["current_workers"] = payload.current_workers
            if payload.desired_workers is not None:
                update["desired_workers"] = payload.desired_workers
            if payload.region is not None:
                update["region"] = payload.region
            if payload.instance_type is not None:
                update["instance_type"] = payload.instance_type
            if payload.total_objects is not None:
                update["total_objects"] = payload.total_objects
            if payload.total_bytes is not None:
                update["total_bytes"] = payload.total_bytes
            updated = record.model_copy(update=update)
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
            status=JobStatus.FINDING if request.find is not None else JobStatus.PENDING,
            source=request.source,
            sink=request.sink,
            find=request.find,
            transforms=request.transforms,
            max_outputs=request.max_outputs,
        )
        transforms = [transform.model_dump(mode="json") for transform in record.transforms]
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    insert into jobs (
                        id,
                        status,
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
                        current_workers,
                        desired_workers,
                        created_at,
                        updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    returning
                        id, status, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, current_workers, desired_workers,
                        created_at, updated_at
                    """,
                    (
                        record.id,
                        record.status.value,
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
                        record.current_workers,
                        record.desired_workers,
                        record.created_at,
                        record.updated_at,
                    ),
                )
                row = cur.fetchone()
        return self._row_to_job(row)

    def get_job(self, job_id: str) -> JobRecord | None:
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    select
                        id, status, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, current_workers, desired_workers,
                        created_at, updated_at
                    from jobs
                    where id = %s
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def list_jobs(self) -> list[JobRecord]:
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    select
                        id, status, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, current_workers, desired_workers,
                        created_at, updated_at
                    from jobs
                    order by created_at desc
                    """
                )
                rows = cur.fetchall()
        return [self._row_to_job(row) for row in rows]

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None:
        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    """
                    update jobs
                    set status = %s, updated_at = %s
                    where id = %s
                    returning
                        id, status, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, current_workers, desired_workers,
                        created_at, updated_at
                    """,
                    (status.value, utc_now(), job_id),
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def update_job_progress(self, payload: JobProgressUpdate) -> JobRecord | None:
        update_fields: list[str] = ["updated_at = %s"]
        params: list[object] = [utc_now()]
        if payload.status is not None:
            update_fields.append("status = %s")
            params.append(payload.status.value)
        if payload.manifest_hash is not None:
            update_fields.append("manifest_hash = %s")
            params.append(payload.manifest_hash)
        if payload.matched_assets is not None:
            update_fields.append("matched_assets = %s")
            params.append(payload.matched_assets)
        if payload.matched_segments is not None:
            update_fields.append("matched_segments = %s")
            params.append(payload.matched_segments)
        if payload.completed_objects is not None:
            update_fields.append("completed_objects = %s")
            params.append(payload.completed_objects)
        if payload.completed_bytes is not None:
            update_fields.append("completed_bytes = %s")
            params.append(payload.completed_bytes)
        if payload.current_workers is not None:
            update_fields.append("current_workers = %s")
            params.append(payload.current_workers)
        if payload.desired_workers is not None:
            update_fields.append("desired_workers = %s")
            params.append(payload.desired_workers)
        if payload.region is not None:
            update_fields.append("region = %s")
            params.append(payload.region)
        if payload.instance_type is not None:
            update_fields.append("instance_type = %s")
            params.append(payload.instance_type)
        if payload.total_objects is not None:
            update_fields.append("total_objects = %s")
            params.append(payload.total_objects)
        if payload.total_bytes is not None:
            update_fields.append("total_bytes = %s")
            params.append(payload.total_bytes)
        params.append(payload.job_id)

        with self._connect(self._dsn, autocommit=True) as conn:
            with conn.cursor(row_factory=self._dict_row) as cur:
                cur.execute(
                    f"""
                    update jobs
                    set {", ".join(update_fields)}
                    where id = %s
                    returning
                        id, status, source, sink, find_query, transforms, max_outputs, region, instance_type,
                        manifest_hash,
                        matched_assets, matched_segments, total_objects, total_bytes,
                        completed_objects, completed_bytes, current_workers, desired_workers,
                        created_at, updated_at
                    """,
                    params,
                )
                row = cur.fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

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
                        status text not null,
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
                        current_workers integer not null default 0,
                        desired_workers integer not null default 0,
                        created_at timestamptz not null,
                        updated_at timestamptz not null
                    )
                    """
                )
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
                    "alter table jobs add column if not exists current_workers integer not null default 0"
                )
                cur.execute(
                    "alter table jobs add column if not exists desired_workers integer not null default 0"
                )

    def _row_to_job(self, row: object) -> JobRecord:
        if row is None:
            raise ValueError("row must not be None")
        data = cast(dict[str, object], row)
        transforms = [
            TransformSpec.model_validate(transform)
            for transform in cast(list[object], data["transforms"])
        ]
        return JobRecord(
            id=str(data["id"]),
            status=JobStatus(str(data["status"])),
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
            current_workers=int(cast(int | None, data.get("current_workers")) or 0),
            desired_workers=int(cast(int | None, data.get("desired_workers")) or 0),
            created_at=cast(datetime, data["created_at"]),
            updated_at=cast(datetime, data["updated_at"]),
        )


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
