from __future__ import annotations

from fastapi import FastAPI

from .launcher import CoordinatorLauncher
from .routers import jobs, webhooks
from .storage import build_job_store


def create_app() -> FastAPI:
    app = FastAPI(title="mx8-media api", version="0.1.0")
    app.state.store = build_job_store()
    app.state.launcher = CoordinatorLauncher()
    app.include_router(jobs.router)
    app.include_router(webhooks.router)

    @app.get("/healthz", tags=["system"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.on_event("shutdown")
    def shutdown_launcher() -> None:
        app.state.launcher.terminate_all()

    return app


app = create_app()
