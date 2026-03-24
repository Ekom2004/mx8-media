from __future__ import annotations

import math

from ._base import Transform


def _require_positive_int(name: str, value: int) -> int:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _require_finite_float(name: str, value: float) -> float:
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must be non-empty")
    return normalized


def resample(*, rate: int = 16000, channels: int = 1) -> Transform:
    return Transform(
        kind="audio.resample",
        params={
            "rate": _require_positive_int("rate", rate),
            "channels": _require_positive_int("channels", channels),
        },
    )


def normalize(*, loudness: float = -14.0) -> Transform:
    return Transform(
        kind="audio.normalize",
        params={
            "loudness": _require_finite_float("loudness", loudness),
        },
    )


def filter(*, expr: str) -> Transform:
    return Transform(
        kind="audio.filter",
        params={"expr": _require_non_empty("expr", expr)},
    )
