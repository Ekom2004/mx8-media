from __future__ import annotations

from ._base import Transform


SUPPORTED_CODECS = frozenset({"h264", "h265", "av1"})
SUPPORTED_CONTAINERS = frozenset({"mp4"})
SUPPORTED_PRESETS = frozenset(
    {"ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"}
)
SUPPORTED_AUDIO_FORMATS = frozenset({"mp3", "wav", "flac"})
SUPPORTED_FRAME_FORMATS = frozenset({"jpg", "png"})


def _require_positive_int(name: str, value: int) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _normalize_codec(codec: str) -> str:
    normalized = codec.strip().lower()
    if normalized == "hevc":
        normalized = "h265"
    if normalized not in SUPPORTED_CODECS:
        raise ValueError("codec must be one of: h264, h265, av1")
    return normalized


def _normalize_container(container: str) -> str:
    normalized = container.strip().lower()
    if normalized not in SUPPORTED_CONTAINERS:
        raise ValueError("container must be one of: mp4")
    return normalized


def _require_crf(crf: int) -> int:
    if not 0 <= crf <= 51:
        raise ValueError("crf must be in 0..=51")
    return crf


def _normalize_preset(preset: str | None, *, codec: str) -> str | None:
    if preset is None:
        return None
    normalized = preset.strip().lower()
    if not normalized:
        raise ValueError("preset must be non-empty")
    if normalized not in SUPPORTED_PRESETS:
        raise ValueError(
            "preset must be one of: ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow"
        )
    if codec == "av1":
        raise ValueError("preset is only supported for h264 and h265")
    return normalized


def _normalize_audio_format(format: str) -> str:
    normalized = format.strip().lower()
    if normalized not in SUPPORTED_AUDIO_FORMATS:
        raise ValueError("format must be one of: mp3, wav, flac")
    return normalized


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def _normalize_frame_format(format: str) -> str:
    normalized = format.strip().lower()
    if normalized == "jpeg":
        normalized = "jpg"
    if normalized not in SUPPORTED_FRAME_FORMATS:
        raise ValueError("format must be one of: jpg, png")
    return normalized


def _require_positive_float(name: str, value: float) -> float:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def transcode(*, codec: str, crf: int = 23, preset: str | None = None) -> Transform:
    normalized_codec = _normalize_codec(codec)
    params = {
        "codec": normalized_codec,
        "crf": _require_crf(crf),
    }
    normalized_preset = _normalize_preset(preset, codec=normalized_codec)
    if normalized_preset is not None:
        params["preset"] = normalized_preset
    return Transform(kind="video.transcode", params=params)


def clip(*, codec: str = "h264", crf: int = 23, preset: str | None = None) -> Transform:
    normalized_codec = _normalize_codec(codec)
    params = {
        "codec": normalized_codec,
        "crf": _require_crf(crf),
    }
    normalized_preset = _normalize_preset(preset, codec=normalized_codec)
    if normalized_preset is not None:
        params["preset"] = normalized_preset
    return Transform(kind="clip", params=params)


def proxy(*, codec: str = "h264", crf: int = 28, preset: str | None = "veryfast") -> Transform:
    normalized_codec = _normalize_codec(codec)
    params = {
        "codec": normalized_codec,
        "crf": _require_crf(crf),
    }
    normalized_preset = _normalize_preset(preset, codec=normalized_codec)
    if normalized_preset is not None:
        params["preset"] = normalized_preset
    return Transform(kind="proxy", params=params)


def remux(*, container: str = "mp4") -> Transform:
    return Transform(
        kind="video.remux",
        params={"container": _normalize_container(container)},
    )


def resize(*, width: int, height: int, maintain_aspect: bool = True) -> Transform:
    return Transform(
        kind="video.resize",
        params={
            "width": _require_positive_int("width", width),
            "height": _require_positive_int("height", height),
            "maintain_aspect": maintain_aspect,
        },
    )


def extract_frames(*, fps: float, format: str) -> Transform:
    return Transform(
        kind="video.extract_frames",
        params={
            "fps": _require_positive_float("fps", fps),
            "format": _normalize_frame_format(format),
        },
    )


def extract_audio(*, format: str, bitrate: str = "128k") -> Transform:
    return Transform(
        kind="video.extract_audio",
        params={
            "format": _normalize_audio_format(format),
            "bitrate": _require_non_empty("bitrate", bitrate),
        },
    )


def filter(*, expr: str) -> Transform:
    return Transform(
        kind="video.filter",
        params={"expr": _require_non_empty("expr", expr)},
    )
