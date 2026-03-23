from __future__ import annotations

import os

from fastapi import FastAPI

from .finder import JobFinder
from .launcher import CoordinatorLauncher
from .routers import jobs, webhooks
from .scaler import JobScaler
from .storage import build_job_store


def create_app() -> FastAPI:
    app = FastAPI(title="mx8-media api", version="0.1.0")
    app.state.store = build_job_store()
    app.state.launcher = CoordinatorLauncher()
    app.state.scaler = JobScaler(app.state.store, app.state.launcher)
    app.state.finder = JobFinder(app.state.store, wake_scaler=app.state.scaler.wake)
    app.include_router(jobs.router)
    app.include_router(webhooks.router)

    @app.get("/healthz", tags=["system"])
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.on_event("startup")
    def startup_background_loops() -> None:
        app.state.finder.start()
        app.state.scaler.start(os.getenv("MX8_API_BASE_URL", "http://127.0.0.1:8000"))

    @app.on_event("shutdown")
    def shutdown_launcher() -> None:
        app.state.finder.stop()
        app.state.scaler.stop()
        app.state.launcher.terminate_all()

    return app


app = create_app()
