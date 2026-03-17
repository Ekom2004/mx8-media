from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from ..models import CreateJobRequest, JobRecord, JobStatus

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobRecord, status_code=status.HTTP_201_CREATED)
def create_job(payload: CreateJobRequest, request: Request) -> JobRecord:
    store = request.app.state.store
    launcher = request.app.state.launcher
    record = store.create_job(payload)
    try:
        launcher.launch(record, str(request.base_url).rstrip("/"))
    except Exception as err:
        failed = store.update_job_status(record.id, JobStatus.FAILED)
        detail = str(err) or "failed to launch coordinator"
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail) from err
    queued = store.update_job_status(record.id, JobStatus.QUEUED)
    if queued is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="job launched but failed to persist queued status",
        )
    return queued


@router.get("", response_model=list[JobRecord])
def list_jobs(request: Request) -> list[JobRecord]:
    store = request.app.state.store
    return list(store.list_jobs())


@router.get("/{job_id}", response_model=JobRecord)
def get_job(job_id: str, request: Request) -> JobRecord:
    store = request.app.state.store
    record = store.get_job(job_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return record
