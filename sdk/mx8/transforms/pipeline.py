from __future__ import annotations

from typing import Iterable, Sequence

from ._base import Transform
from . import audio as audio_module
from . import image as image_module
from . import video as video_module


class TransformChain:
    def __init__(self, transforms: Iterable[Transform] | None = None) -> None:
        self._transforms: list[Transform] = list(transforms) if transforms is not None else []

    def append(self, transform: Transform) -> TransformChain:
        self._transforms.append(transform)
        return self

    def extend(self, transforms: Sequence[Transform]) -> TransformChain:
        self._transforms.extend(transforms)
        return self

    def to_transforms(self) -> list[Transform]:
        return list(self._transforms)

    def to_payloads(self) -> list[dict[str, object]]:
        return [transform.to_payload() for transform in self._transforms]


class VideoTransformPipeline(TransformChain):
    def clip(self, *, codec: str = "h264", crf: int = 23, preset: str | None = None) -> VideoTransformPipeline:
        return self.append(video_module.clip(codec=codec, crf=crf, preset=preset))

    def transcode(self, *, codec: str, crf: int = 23, preset: str | None = None) -> VideoTransformPipeline:
        return self.append(video_module.transcode(codec=codec, crf=crf, preset=preset))

    def resize(self, *, width: int, height: int, maintain_aspect: bool = True) -> VideoTransformPipeline:
        return self.append(
            video_module.resize(width=width, height=height, maintain_aspect=maintain_aspect)
        )

    def extract_frames(self, *, fps: float, format: str) -> VideoTransformPipeline:
        return self.append(video_module.extract_frames(fps=fps, format=format))

    def extract_audio(self, *, format: str, bitrate: str = "128k") -> VideoTransformPipeline:
        return self.append(video_module.extract_audio(format=format, bitrate=bitrate))

    def extract(self, *, mode: str = "frames", **params: object) -> VideoTransformPipeline:
        normalized_mode = mode.strip().lower()
        if normalized_mode == "frames":
            return self.extract_frames(**params)
        if normalized_mode == "audio":
            return self.extract_audio(**params)
        raise ValueError("video.extract only supports 'frames' or 'audio' modes")

    def filter(self, *, expr: str) -> VideoTransformPipeline:
        return self.append(video_module.filter(expr=expr))


class AudioTransformPipeline(TransformChain):
    def transcode(self, *, format: str, bitrate: str = "128k") -> AudioTransformPipeline:
        return self.append(audio_module.transcode(format=format, bitrate=bitrate))

    def resample(self, *, rate: int = 16000, channels: int = 1) -> AudioTransformPipeline:
        return self.append(audio_module.resample(rate=rate, channels=channels))

    def normalize(self, *, loudness: float = -14.0) -> AudioTransformPipeline:
        return self.append(audio_module.normalize(loudness=loudness))

    def filter(self, *, expr: str) -> AudioTransformPipeline:
        return self.append(audio_module.filter(expr=expr))


class ImageTransformPipeline(TransformChain):
    def resize(self, *, width: int, height: int, maintain_aspect: bool = True) -> ImageTransformPipeline:
        return self.append(
            image_module.resize(width=width, height=height, maintain_aspect=maintain_aspect)
        )

    def crop(self, *, width: int, height: int) -> ImageTransformPipeline:
        return self.append(image_module.crop(width=width, height=height))

    def convert(self, *, format: str, quality: int = 85) -> ImageTransformPipeline:
        return self.append(image_module.convert(format=format, quality=quality))

    def filter(self, *, expr: str) -> ImageTransformPipeline:
        return self.append(image_module.filter(expr=expr))


class _VideoTransformsFactory:
    def clip(self, *, codec: str = "h264", crf: int = 23, preset: str | None = None) -> VideoTransformPipeline:
        return VideoTransformPipeline().clip(codec=codec, crf=crf, preset=preset)

    def extract(self, *, mode: str = "frames", **params: object) -> VideoTransformPipeline:
        return VideoTransformPipeline().extract(mode=mode, **params)

    def extract_frames(self, *, fps: float, format: str) -> VideoTransformPipeline:
        return VideoTransformPipeline().extract_frames(fps=fps, format=format)

    def extract_audio(self, *, format: str, bitrate: str = "128k") -> VideoTransformPipeline:
        return VideoTransformPipeline().extract_audio(format=format, bitrate=bitrate)

    def filter(self, *, expr: str) -> VideoTransformPipeline:
        return VideoTransformPipeline().filter(expr=expr)

    def resize(self, *, width: int, height: int, maintain_aspect: bool = True) -> VideoTransformPipeline:
        return VideoTransformPipeline().resize(width=width, height=height, maintain_aspect=maintain_aspect)

    def transcode(self, *, codec: str, crf: int = 23, preset: str | None = None) -> VideoTransformPipeline:
        return VideoTransformPipeline().transcode(codec=codec, crf=crf, preset=preset)


class _AudioTransformsFactory:
    def transcode(self, *, format: str, bitrate: str = "128k") -> AudioTransformPipeline:
        return AudioTransformPipeline().transcode(format=format, bitrate=bitrate)

    def resample(self, *, rate: int = 16000, channels: int = 1) -> AudioTransformPipeline:
        return AudioTransformPipeline().resample(rate=rate, channels=channels)

    def normalize(self, *, loudness: float = -14.0) -> AudioTransformPipeline:
        return AudioTransformPipeline().normalize(loudness=loudness)

    def filter(self, *, expr: str) -> AudioTransformPipeline:
        return AudioTransformPipeline().filter(expr=expr)


class _ImageTransformsFactory:
    def resize(self, *, width: int, height: int, maintain_aspect: bool = True) -> ImageTransformPipeline:
        return ImageTransformPipeline().resize(width=width, height=height, maintain_aspect=maintain_aspect)

    def crop(self, *, width: int, height: int) -> ImageTransformPipeline:
        return ImageTransformPipeline().crop(width=width, height=height)

    def convert(self, *, format: str, quality: int = 85) -> ImageTransformPipeline:
        return ImageTransformPipeline().convert(format=format, quality=quality)

    def filter(self, *, expr: str) -> ImageTransformPipeline:
        return ImageTransformPipeline().filter(expr=expr)


video = _VideoTransformsFactory()
audio = _AudioTransformsFactory()
image = _ImageTransformsFactory()
