from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ..auth import resolve_principal
from ..models import AccountRole, AuthPrincipal, JobView, SubmitJobRequest

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


@router.post("", response_model=JobView, status_code=status.HTTP_201_CREATED)
def create_job(
    payload: SubmitJobRequest,
    request: Request,
    principal: AuthPrincipal = Depends(resolve_principal),
) -> JobView:
    store = request.app.state.store
    finder = request.app.state.finder
    scaler = request.app.state.scaler
    internal = payload.to_internal()
    internal.account_id = principal.account_id
    record = store.create_job(internal)
    finder.wake()
    scaler.wake()
    return JobView.from_record(record)


@router.get("", response_model=list[JobView])
def list_jobs(
    request: Request,
    principal: AuthPrincipal = Depends(resolve_principal),
) -> list[JobView]:
    store = request.app.state.store
    account_id = None if principal.role == AccountRole.OPERATOR else principal.account_id
    return [JobView.from_record(record) for record in store.list_jobs(account_id=account_id)]


@router.get("/{job_id}", response_model=JobView)
def get_job(
    job_id: str,
    request: Request,
    principal: AuthPrincipal = Depends(resolve_principal),
) -> JobView:
    store = request.app.state.store
    account_id = None if principal.role == AccountRole.OPERATOR else principal.account_id
    record = store.get_job(job_id, account_id=account_id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
    return JobView.from_record(record)
