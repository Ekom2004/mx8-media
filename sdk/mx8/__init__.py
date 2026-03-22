from __future__ import annotations

from collections.abc import Sequence

from . import transforms as transforms
from .client import MX8Client, default_client
from .job import Job
from .transforms import Transform, audio, image, video


def run(
    source: str,
    *,
    transform: Transform | Sequence[Transform],
    sink: str,
    find: str | None = None,
    client: MX8Client | None = None,
) -> Job:
    active_client = client or default_client()
    return active_client.submit_job(source=source, transform=transform, sink=sink, find=find)


__all__ = [
    "MX8Client",
    "Job",
    "Transform",
    "audio",
    "image",
    "run",
    "video",
    "transforms",
]
