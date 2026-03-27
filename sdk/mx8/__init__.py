from __future__ import annotations

from collections.abc import Sequence

from . import transforms as transforms
from .client import MX8Client, default_client
from .job import Job
from .transforms import Transform, TransformChain, audio, image, video
from .transforms.audio import normalize as _audio_normalize
from .transforms.audio import resample as _audio_resample
from .transforms.audio import transcode as _audio_transcode
from .transforms.image import convert as _image_convert
from .transforms.image import crop as _image_crop
from .transforms.image import filter as _image_filter
from .transforms.image import resize as _image_resize
from .transforms.audio import filter as _audio_filter
from .transforms.video import filter as _video_filter
from .transforms.video import resize as _video_resize
from .transforms.video import clip as _video_clip
from .transforms.video import extract_audio as _video_extract_audio
from .transforms.video import extract_frames
from .transforms.video import transcode as _video_transcode
from .work import FindWork, find

Client = MX8Client


def run(
    *,
    input: str | None = None,
    work: Transform | FindWork | Sequence[Transform | FindWork] | TransformChain | None = None,
    output: str | None = None,
    source: str | None = None,
    sink: str | None = None,
    find: str | None = None,
    transforms: Transform | Sequence[Transform] | TransformChain | None = None,
    transform: Transform | Sequence[Transform] | TransformChain | None = None,
    max: int | None = None,
    client: MX8Client | None = None,
) -> Job:
    normalized_input = input if input is not None else source
    normalized_output = output if output is not None else sink
    if normalized_input is None:
        raise ValueError("`input` is required")
    if normalized_output is None:
        raise ValueError("`output` is required")

    legacy_payload = transforms if transforms is not None else transform
    if work is not None and (legacy_payload is not None or find is not None):
        raise ValueError("use either `work=` or legacy `find`/`transform` arguments, not both")
    if work is None:
        if legacy_payload is None:
            raise ValueError("`work` is required")
        work_items: list[Transform | FindWork] = []
        if find is not None:
            work_items.append(globals()["find"](find))
        if isinstance(legacy_payload, Transform):
            work_items.append(legacy_payload)
        elif isinstance(legacy_payload, TransformChain):
            work_items.extend(legacy_payload.to_transforms())
        else:
            work_items.extend(list(legacy_payload))
        work = work_items

    active_client = client or default_client()
    return active_client.submit_job(
        input=normalized_input,
        work=work,
        output=normalized_output,
        max=max,
    )


def extract_audio(*, format: str, bitrate: str = "128k") -> Transform:
    return _video_extract_audio(format=format, bitrate=bitrate)


def clip(*, codec: str = "h264", crf: int = 23) -> Transform:
    return _video_clip(codec=codec, crf=crf)


def transcode(
    *,
    codec: str | None = None,
    crf: int = 23,
    format: str | None = None,
    bitrate: str = "128k",
) -> Transform:
    if codec is not None and format is not None:
        raise ValueError("transcode accepts either video params (`codec`, `crf`) or audio params (`format`, `bitrate`), not both")
    if codec is not None:
        return _video_transcode(codec=codec, crf=crf)
    if format is not None:
        return _audio_transcode(format=format, bitrate=bitrate)
    raise ValueError("transcode requires either `codec` for video or `format` for audio")


def resize(
    *,
    width: int,
    height: int,
    maintain_aspect: bool = True,
    media: str,
) -> Transform:
    normalized = media.strip().lower()
    if normalized == "video":
        return _video_resize(width=width, height=height, maintain_aspect=maintain_aspect)
    if normalized == "image":
        return _image_resize(width=width, height=height, maintain_aspect=maintain_aspect)
    raise ValueError("resize requires media='video' or media='image'")


def convert(*, format: str, quality: int = 85) -> Transform:
    return _image_convert(format=format, quality=quality)


def crop(*, width: int, height: int) -> Transform:
    return _image_crop(width=width, height=height)


def filter(*, expr: str, media: str) -> Transform:
    normalized = media.strip().lower()
    if normalized == "video":
        return _video_filter(expr=expr)
    if normalized == "image":
        return _image_filter(expr=expr)
    if normalized == "audio":
        return _audio_filter(expr=expr)
    raise ValueError("filter requires media='video', media='image', or media='audio'")


def resample(*, rate: int = 16000, channels: int = 1) -> Transform:
    return _audio_resample(rate=rate, channels=channels)


def normalize(*, loudness: float = -14.0) -> Transform:
    return _audio_normalize(loudness=loudness)


__all__ = [
    "Client",
    "FindWork",
    "Job",
    "Transform",
    "TransformChain",
    "audio",
    "convert",
    "crop",
    "clip",
    "extract_frames",
    "extract_audio",
    "filter",
    "find",
    "image",
    "normalize",
    "resample",
    "resize",
    "run",
    "transcode",
    "video",
    "transforms",
]
