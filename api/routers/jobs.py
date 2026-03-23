from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status

from ..models import CreateJobRequest, JobRecord

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobRecord, status_code=status.HTTP_201_CREATED)
def create_job(payload: CreateJobRequest, request: Request) -> JobRecord:
    store = request.app.state.store
    finder = request.app.state.finder
    scaler = request.app.state.scaler
    record = store.create_job(payload)
    finder.wake()
    scaler.wake()
    return record


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
