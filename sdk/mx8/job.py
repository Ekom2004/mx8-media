from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from time import sleep
from typing import TYPE_CHECKING, Any


TERMINAL_STATUSES = {"COMPLETE", "FAILED"}


@dataclass(slots=True)
class Job:
    client: "MX8Client"
    id: str
    status: str
    stage: str | None
    input: str
    output: str
    work: Sequence[dict[str, Any]] = ()
    find: str | None = None
    max: int | None = None
    matched_assets: int | None = None
    matched_segments: int | None = None
    outputs_written: int | None = None
    worker_pool: str | None = None
    failure_category: str | None = None
    failure_message: str | None = None
    events: Sequence[dict[str, Any]] = ()

    def poll(self) -> "Job":
        latest = self.client.get_job(self.id)
        self.status = latest.status
        self.stage = latest.stage
        self.input = latest.input
        self.output = latest.output
        self.work = latest.work
        self.find = latest.find
        self.max = latest.max
        self.matched_assets = latest.matched_assets
        self.matched_segments = latest.matched_segments
        self.outputs_written = latest.outputs_written
        self.worker_pool = latest.worker_pool
        self.failure_category = latest.failure_category
        self.failure_message = latest.failure_message
        self.events = latest.events
        return self

    def wait(self, *, poll_interval_secs: float = 2.0) -> "Job":
        while self.status not in TERMINAL_STATUSES:
            sleep(poll_interval_secs)
            self.poll()
        return self

    @property
    def source(self) -> str:
        return self.input

    @property
    def sink(self) -> str:
        return self.output


if TYPE_CHECKING:
    from .client import MX8Client
