from __future__ import annotations

from ._base import Transform


SUPPORTED_FORMATS = frozenset({"jpg", "png", "webp"})


def _require_positive_int(name: str, value: int) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _normalize_format(format: str) -> str:
    normalized = format.strip().lower()
    if normalized == "jpeg":
        normalized = "jpg"
    if normalized not in SUPPORTED_FORMATS:
        raise ValueError("format must be one of: jpg, png, webp")
    return normalized


def _require_quality(quality: int) -> int:
    if not 1 <= quality <= 100:
        raise ValueError("quality must be in 1..=100")
    return quality


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def resize(*, width: int, height: int, maintain_aspect: bool = True) -> Transform:
    return Transform(
        kind="image.resize",
        params={
            "width": _require_positive_int("width", width),
            "height": _require_positive_int("height", height),
            "maintain_aspect": maintain_aspect,
        },
    )


def crop(*, width: int, height: int) -> Transform:
    return Transform(
        kind="image.crop",
        params={
            "width": _require_positive_int("width", width),
            "height": _require_positive_int("height", height),
        },
    )


def convert(*, format: str, quality: int = 85) -> Transform:
    return Transform(
        kind="image.convert",
        params={
            "format": _normalize_format(format),
            "quality": _require_quality(quality),
        },
    )


def filter(*, expr: str) -> Transform:
    return Transform(
        kind="image.filter",
        params={"expr": _require_non_empty("expr", expr)},
    )


def remove_background() -> Transform:
    return Transform(
        kind="image.remove_background",
        params={},
    )


def develop_raw() -> Transform:
    return Transform(
        kind="image.develop_raw",
        params={},
    )
