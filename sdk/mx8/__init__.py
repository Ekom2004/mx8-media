from __future__ import annotations

from typing import Sequence

from . import transforms as transforms
from .client import MX8Client, default_client
from .job import Job
from .transforms import Transform, TransformChain, audio, image, video

Client = MX8Client


def run(
    *,
    source: str,
    sink: str,
    find: str | None = None,
    transforms: Transform | Sequence[Transform] | TransformChain | None = None,
    transform: Transform | Sequence[Transform] | TransformChain | None = None,
    client: MX8Client | None = None,
) -> Job:
    payload = transforms if transforms is not None else transform
    if payload is None:
        raise ValueError("`transform` is required")
    active_client = client or default_client()
    return active_client.submit_job(source=source, transform=payload, sink=sink, find=find)


__all__ = [
    "Client",
    "Job",
    "Transform",
    "TransformChain",
    "audio",
    "image",
    "run",
    "video",
    "transforms",
]
