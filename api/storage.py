from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import datetime
from typing import Protocol, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .models import CreateJobRequest, JobRecord, JobStatus, TransformSpec, utc_now


class JobStore(Protocol):
    def create_job(self, request: CreateJobRequest) -> JobRecord: ...

    def get_job(self, job_id: str) -> JobRecord | None: ...

    def list_jobs(self) -> Iterable[JobRecord]: ...

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None: ...


class InMemoryJobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        record = JobRecord(
            source=request.source,
            sink=request.sink,
            transforms=request.transforms,
        )
        self._jobs[record.id] = record
        return record

    def get_job(self, job_id: str) -> JobRecord | None:
        return self._jobs.get(job_id)

    def list_jobs(self) -> list[JobRecord]:
        return sorted(
            self._jobs.values(),
            key=lambda job: job.created_at,
            reverse=True,
        )

    def update_job_status(self, job_id: str, status: JobStatus) -> JobRecord | None:
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


class PostgresJobStore:
    def __init__(self, dsn: str, schema_dsn: str | None = None) -> None:
        self._dsn = _normalize_postgres_dsn(dsn)
        self._schema_dsn = _normalize_postgres_dsn(schema_dsn or dsn)
        self._connect, self._dict_row, self._jsonb = _load_psycopg()
        self._ensure_schema()

    def create_job(self, request: CreateJobRequest) -> JobRecord:
        record = JobRecord(
            source=request.source,
            sink=request.sink,
            transforms=request.transforms,
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
                        transforms,
                        created_at,
                        updated_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    returning id, status, source, sink, transforms, created_at, updated_at
                    """,
                    (
                        record.id,
                        record.status.value,
                        record.source,
                        record.sink,
                        self._jsonb(transforms),
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
                    select id, status, source, sink, transforms, created_at, updated_at
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
                    select id, status, source, sink, transforms, created_at, updated_at
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
                    returning id, status, source, sink, transforms, created_at, updated_at
                    """,
                    (status.value, utc_now(), job_id),
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
                        transforms jsonb not null,
                        created_at timestamptz not null,
                        updated_at timestamptz not null
                    )
                    """
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
            transforms=transforms,
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
