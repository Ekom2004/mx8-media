from __future__ import annotations

import os
import subprocess
import tempfile
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from email.message import Message
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

try:
    import modal
except ImportError:  # pragma: no cover - local unit tests may run without Modal installed
    modal = None

_APP_NAME = os.getenv("MX8_FIND_MODAL_APP_NAME", "mx8-find-worker")
_MODEL_CACHE_DIR = Path("/models/hf")
_MODEL_CACHE_VOLUME_NAME = os.getenv("MX8_FIND_MODAL_HF_CACHE_VOLUME", "mx8-find-hf-cache")
_REMOTE_MEDIA_EXTENSIONS = {
    ".3gp",
    ".asf",
    ".avi",
    ".m2ts",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mts",
    ".ts",
    ".webm",
    ".wmv",
}

if modal is not None:
    _model_cache_volume = modal.Volume.from_name(_MODEL_CACHE_VOLUME_NAME, create_if_missing=True)
    _runtime_env_secret = modal.Secret.from_dict(
        {
            "MX8_FIND_BATCH_SIZE": os.getenv("MX8_FIND_BATCH_SIZE", "16"),
            "MX8_FIND_SCORE_THRESHOLD": os.getenv("MX8_FIND_SCORE_THRESHOLD", "-8.0"),
            "MX8_FIND_SCORE_MARGIN": os.getenv("MX8_FIND_SCORE_MARGIN", "2.0"),
            "MX8_FIND_PAD_BEFORE_MS": os.getenv("MX8_FIND_PAD_BEFORE_MS", "500"),
            "MX8_FIND_PAD_AFTER_MS": os.getenv("MX8_FIND_PAD_AFTER_MS", "1500"),
            "MX8_FIND_MERGE_GAP_MS": os.getenv("MX8_FIND_MERGE_GAP_MS", "1500"),
        }
    )
    app = modal.App(_APP_NAME)
    image = (
        modal.Image.debian_slim(python_version="3.11")
        .apt_install("ffmpeg")
        .pip_install(
            "torch>=2.7,<3.0",
            "transformers>=4.57,<5.0",
            "pillow>=10,<12",
            "safetensors>=0.4,<1.0",
        )
    )
else:  # pragma: no cover - local unit tests may run without Modal installed
    _model_cache_volume = None
    app = None
    image = None


@dataclass(frozen=True)
class MatchSegment:
    sample_id: int
    start_ms: int
    end_ms: int

    def validate(self) -> None:
        if self.sample_id < 0:
            raise ValueError("sample_id must be >= 0")
        if self.start_ms < 0:
            raise ValueError("start_ms must be >= 0")
        if self.end_ms <= self.start_ms:
            raise ValueError("end_ms must be > start_ms")


@dataclass(frozen=True)
class FindShard:
    shard_id: str
    job_id: str
    customer_id: str
    lane: str
    priority: int
    attempt: int
    query_id: str
    query_text: str
    source_uri: str
    asset_id: str
    decode_hint: str | None
    sample_id: int
    scan_start_ms: int
    scan_end_ms: int
    overlap_ms: int
    sample_fps: float
    model: str
    created_at_ms: int
    source_access_url: str | None = None

    def validate(self) -> None:
        if not self.shard_id.strip():
            raise ValueError("shard_id must be non-empty")
        if not self.job_id.strip():
            raise ValueError("job_id must be non-empty")
        if not self.customer_id.strip():
            raise ValueError("customer_id must be non-empty")
        if not self.lane.strip():
            raise ValueError("lane must be non-empty")
        if self.priority < 0:
            raise ValueError("priority must be >= 0")
        if self.attempt < 0:
            raise ValueError("attempt must be >= 0")
        if not self.query_id.strip():
            raise ValueError("query_id must be non-empty")
        if not self.query_text.strip():
            raise ValueError("query_text must be non-empty")
        if not self.source_uri.strip():
            raise ValueError("source_uri must be non-empty")
        if not self.asset_id.strip():
            raise ValueError("asset_id must be non-empty")
        if self.source_access_url is not None and not self.source_access_url.strip():
            raise ValueError("source_access_url must be non-empty when set")
        if self.sample_id < 0:
            raise ValueError("sample_id must be >= 0")
        if self.scan_start_ms < 0:
            raise ValueError("scan_start_ms must be >= 0")
        if self.scan_end_ms <= self.scan_start_ms:
            raise ValueError("scan_end_ms must be > scan_start_ms")
        if self.overlap_ms < 0:
            raise ValueError("overlap_ms must be >= 0")
        if self.sample_fps <= 0:
            raise ValueError("sample_fps must be > 0")
        if not self.model.strip():
            raise ValueError("model must be non-empty")


@dataclass(frozen=True)
class FindShardStats:
    sampled_frames: int
    decode_ms: int
    inference_ms: int
    wall_ms: int

    def validate(self) -> None:
        if self.sampled_frames < 0:
            raise ValueError("sampled_frames must be >= 0")
        if self.decode_ms < 0:
            raise ValueError("decode_ms must be >= 0")
        if self.inference_ms < 0:
            raise ValueError("inference_ms must be >= 0")
        if self.wall_ms < 0:
            raise ValueError("wall_ms must be >= 0")


@dataclass(frozen=True)
class FindShardResult:
    shard_id: str
    job_id: str
    customer_id: str
    asset_id: str
    status: str
    hits: tuple[MatchSegment, ...]
    stats: FindShardStats
    error: str | None = None

    def validate(self) -> None:
        if not self.shard_id.strip():
            raise ValueError("shard_id must be non-empty")
        if not self.job_id.strip():
            raise ValueError("job_id must be non-empty")
        if not self.customer_id.strip():
            raise ValueError("customer_id must be non-empty")
        if not self.asset_id.strip():
            raise ValueError("asset_id must be non-empty")
        if self.status not in {"ok", "error"}:
            raise ValueError(f"unsupported shard result status: {self.status!r}")
        if self.status == "ok" and self.error is not None:
            raise ValueError("successful shard result must not include error")
        if self.status == "error" and not (self.error or "").strip():
            raise ValueError("error shard result must include error")
        self.stats.validate()
        for hit in self.hits:
            hit.validate()


def find_shard_from_payload(payload: Mapping[str, object]) -> FindShard:
    shard = FindShard(
        shard_id=str(payload["shard_id"]),
        job_id=str(payload["job_id"]),
        customer_id=str(payload["customer_id"]),
        lane=str(payload["lane"]),
        priority=int(payload["priority"]),
        attempt=int(payload["attempt"]),
        query_id=str(payload["query_id"]),
        query_text=str(payload["query_text"]),
        source_uri=str(payload["source_uri"]),
        asset_id=str(payload["asset_id"]),
        decode_hint=_optional_str(payload.get("decode_hint")),
        sample_id=int(payload["sample_id"]),
        scan_start_ms=int(payload["scan_start_ms"]),
        scan_end_ms=int(payload["scan_end_ms"]),
        overlap_ms=int(payload["overlap_ms"]),
        sample_fps=float(payload["sample_fps"]),
        model=str(payload["model"]),
        created_at_ms=int(payload["created_at_ms"]),
        source_access_url=_optional_str(payload.get("source_access_url")),
    )
    shard.validate()
    return shard


def find_shard_result_to_payload(result: FindShardResult) -> dict[str, object]:
    result.validate()
    return {
        "shard_id": result.shard_id,
        "job_id": result.job_id,
        "customer_id": result.customer_id,
        "asset_id": result.asset_id,
        "status": result.status,
        "hits": [
            {
                "sample_id": hit.sample_id,
                "start_ms": hit.start_ms,
                "end_ms": hit.end_ms,
            }
            for hit in result.hits
        ],
        "stats": {
            "sampled_frames": result.stats.sampled_frames,
            "decode_ms": result.stats.decode_ms,
            "inference_ms": result.stats.inference_ms,
            "wall_ms": result.stats.wall_ms,
        },
        "error": result.error,
    }


@dataclass
class _ModelRuntime:
    processor: Any
    model: Any
    device: str
    torch_dtype: Any
    inference_mode: Any


_runtime_lock = threading.Lock()
_runtime_by_model: dict[str, _ModelRuntime] = {}


def process_shard_payload(payload: dict[str, object]) -> dict[str, object]:
    shard = find_shard_from_payload(payload)
    result = _process_shard(shard)
    return find_shard_result_to_payload(result)


if modal is not None:

    @app.function(
        image=image,
        gpu=os.getenv("MX8_FIND_MODAL_GPU", "L4"),
        timeout=int(os.getenv("MX8_FIND_MODAL_TIMEOUT_SECS", "3600")),
        volumes={"/models": _model_cache_volume},
        secrets=[_runtime_env_secret],
    )
    def process_shard(payload: dict[str, object]) -> dict[str, object]:
        return process_shard_payload(payload)


def _process_shard(shard: FindShard) -> FindShardResult:
    started = monotonic()
    sampled_frames = 0
    decode_ms = 0
    inference_ms = 0
    source_ref = shard.source_access_url or shard.source_uri
    try:
        with tempfile.TemporaryDirectory(prefix="mx8-find-frames-") as tempdir:
            frame_dir = Path(tempdir)
            decode_source_ref = _prepare_source_for_decode(
                source_ref=source_ref,
                shard=shard,
            )
            frame_times_ms, frame_paths, decode_ms = _extract_frames(
                source_ref=decode_source_ref,
                shard=shard,
                output_dir=frame_dir,
            )
            sampled_frames = len(frame_paths)
            if not frame_paths:
                hits: tuple[MatchSegment, ...] = ()
            else:
                scores, inference_ms = _score_frames(
                    shard=shard,
                    frame_paths=frame_paths,
                    source_ref=source_ref,
                )
                hits = _merge_frame_hits(
                    sample_id=shard.sample_id,
                    scan_start_ms=shard.scan_start_ms,
                    scan_end_ms=shard.scan_end_ms,
                    sample_fps=shard.sample_fps,
                    frame_times_ms=frame_times_ms,
                    scores=scores,
                )
        result = FindShardResult(
            shard_id=shard.shard_id,
            job_id=shard.job_id,
            customer_id=shard.customer_id,
            asset_id=shard.asset_id,
            status="ok",
            hits=hits,
            stats=FindShardStats(
                sampled_frames=sampled_frames,
                decode_ms=decode_ms,
                inference_ms=inference_ms,
                wall_ms=max(0, int((monotonic() - started) * 1000)),
            ),
        )
        result.validate()
        return result
    except Exception as exc:
        result = FindShardResult(
            shard_id=shard.shard_id,
            job_id=shard.job_id,
            customer_id=shard.customer_id,
            asset_id=shard.asset_id,
            status="error",
            hits=(),
            stats=FindShardStats(
                sampled_frames=sampled_frames,
                decode_ms=decode_ms,
                inference_ms=inference_ms,
                wall_ms=max(0, int((monotonic() - started) * 1000)),
            ),
            error=str(exc),
        )
        result.validate()
        return result


def _extract_frames(*, source_ref: str, shard: FindShard, output_dir: Path) -> tuple[list[int], list[Path], int]:
    started = monotonic()
    output_pattern = str(output_dir / "frame-%06d.jpg")
    scan_duration_secs = max(0.001, (shard.scan_end_ms - shard.scan_start_ms) / 1000.0)
    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-ss",
        f"{shard.scan_start_ms / 1000.0:.3f}",
        "-t",
        f"{scan_duration_secs:.3f}",
        "-i",
        source_ref,
        "-vf",
        f"fps={shard.sample_fps}",
        "-q:v",
        "2",
        "-vsync",
        "vfr",
        "-start_number",
        "0",
        output_pattern,
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ffmpeg frame extraction failed for {shard.asset_id}: {stderr}")
    frame_paths = sorted(output_dir.glob("frame-*.jpg"))
    if not frame_paths:
        return [], [], max(0, int((monotonic() - started) * 1000))
    step_ms = _frame_step_ms(shard.sample_fps)
    frame_times_ms = [
        min(shard.scan_end_ms - 1, shard.scan_start_ms + (index * step_ms))
        for index in range(len(frame_paths))
    ]
    return frame_times_ms, frame_paths, max(0, int((monotonic() - started) * 1000))


def _prepare_source_for_decode(*, source_ref: str, shard: FindShard) -> str:
    parsed = urlsplit(source_ref)
    if parsed.scheme not in {"http", "https"}:
        return source_ref
    request = Request(
        source_ref,
        headers={
            "Range": "bytes=0-0",
            "User-Agent": "mx8-find-worker/1",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=_remote_probe_timeout_secs()) as response:
            _validate_remote_source_response(
                status=getattr(response, "status", response.getcode()),
                headers=response.headers,
                source_ref=source_ref,
                shard=shard,
            )
    except Exception as exc:
        raise RuntimeError(f"remote source preflight failed for {shard.asset_id}: {exc}") from exc
    return source_ref


def _validate_remote_source_response(
    *,
    status: int,
    headers: Message,
    source_ref: str,
    shard: FindShard,
) -> None:
    accept_ranges = headers.get("Accept-Ranges", "")
    content_range = headers.get("Content-Range", "")
    content_type = headers.get_content_type()
    if not _supports_byte_ranges(status=status, accept_ranges=accept_ranges, content_range=content_range):
        raise RuntimeError(
            "http/https source must support byte-range reads "
            f"(status={status}, accept_ranges={accept_ranges or '<empty>'}, "
            f"content_range={content_range or '<empty>'})"
        )
    if not _is_supported_media_type(content_type=content_type, source_ref=source_ref, asset_id=shard.asset_id):
        raise RuntimeError(
            "http/https source must be a direct media object "
            f"(content_type={content_type or '<empty>'})"
        )


def _supports_byte_ranges(*, status: int, accept_ranges: str, content_range: str) -> bool:
    if status == 206:
        return True
    if accept_ranges.strip().lower() == "bytes":
        return True
    return content_range.strip().lower().startswith("bytes ")


def _is_supported_media_type(*, content_type: str, source_ref: str, asset_id: str) -> bool:
    normalized = (content_type or "").strip().lower()
    if normalized.startswith("video/") or normalized.startswith("audio/"):
        return True
    if normalized in {"application/octet-stream", "binary/octet-stream"}:
        return True
    suffix = Path(urlsplit(source_ref).path or asset_id).suffix.lower()
    if not normalized and suffix in _REMOTE_MEDIA_EXTENSIONS:
        return True
    return False


def _score_frames(*, shard: FindShard, frame_paths: Sequence[Path], source_ref: str) -> tuple[list[float], int]:
    started = monotonic()
    runtime = _runtime_for_model(shard.model)

    from PIL import Image

    scores: list[float] = []
    batch_size = _batch_size()
    for batch_paths in _batched(frame_paths, batch_size):
        images = []
        for path in batch_paths:
            with Image.open(path) as image:
                images.append(image.convert("RGB"))
        inputs = runtime.processor(
            text=[shard.query_text],
            images=images,
            return_tensors="pt",
            padding="max_length",
            truncation=True,
        )
        inputs = {name: tensor.to(runtime.device) for name, tensor in inputs.items()}
        with runtime.inference_mode():
            outputs = runtime.model(**inputs)
        batch_scores = outputs.logits_per_image.squeeze(-1).float()
        scores.extend(float(score) for score in batch_scores.detach().cpu().tolist())
    return scores, max(0, int((monotonic() - started) * 1000))


def _runtime_for_model(model_alias: str) -> _ModelRuntime:
    model_id = _resolve_model_id(model_alias)
    with _runtime_lock:
        runtime = _runtime_by_model.get(model_id)
        if runtime is not None:
            return runtime

        from transformers import AutoModel, AutoProcessor

        os.environ.setdefault("HF_HOME", str(_MODEL_CACHE_DIR))
        _MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        import torch

        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if device == "cuda" else torch.float32
        processor = AutoProcessor.from_pretrained(model_id, use_fast=False)
        model = AutoModel.from_pretrained(
            model_id,
            torch_dtype=torch_dtype,
            attn_implementation="sdpa",
        ).to(device)
        model.eval()
        runtime = _ModelRuntime(
            processor=processor,
            model=model,
            device=device,
            torch_dtype=torch_dtype,
            inference_mode=torch.inference_mode,
        )
        _runtime_by_model[model_id] = runtime
        if _model_cache_volume is not None:  # pragma: no branch - only true on Modal
            try:
                _model_cache_volume.commit()
            except Exception:
                pass
        return runtime


def _merge_frame_hits(
    *,
    sample_id: int,
    scan_start_ms: int,
    scan_end_ms: int,
    sample_fps: float,
    frame_times_ms: Sequence[int],
    scores: Sequence[float],
) -> tuple[MatchSegment, ...]:
    threshold = _match_threshold(scores)
    pad_before_ms = _pad_before_ms()
    pad_after_ms = _pad_after_ms()
    merge_gap_ms = _merge_gap_ms()
    frame_step_ms = _frame_step_ms(sample_fps)

    merged: list[MatchSegment] = []
    current: MatchSegment | None = None
    for frame_time_ms, score in zip(frame_times_ms, scores):
        if score < threshold:
            continue
        start_ms = max(scan_start_ms, frame_time_ms - pad_before_ms)
        end_ms = min(scan_end_ms, frame_time_ms + frame_step_ms + pad_after_ms)
        if end_ms <= start_ms:
            end_ms = min(scan_end_ms, start_ms + 1)
        if current is not None and start_ms <= current.end_ms + merge_gap_ms:
            current = MatchSegment(
                sample_id=sample_id,
                start_ms=current.start_ms,
                end_ms=max(current.end_ms, end_ms),
            )
            merged[-1] = current
            continue
        current = MatchSegment(sample_id=sample_id, start_ms=start_ms, end_ms=end_ms)
        merged.append(current)
    return tuple(merged)


def _match_threshold(scores: Sequence[float]) -> float:
    if not scores:
        return _score_threshold()
    return max(_score_threshold(), max(scores) - _score_margin())


def _resolve_model_id(model_alias: str) -> str:
    normalized = model_alias.strip().lower()
    if "/" in model_alias:
        return model_alias
    aliases = {
        "siglip2_base": "google/siglip2-base-patch16-224",
    }
    if normalized not in aliases:
        raise RuntimeError(f"unsupported find model alias in modal worker: {model_alias}")
    return aliases[normalized]


def _frame_step_ms(sample_fps: float) -> int:
    return max(1, int(round(1000.0 / max(sample_fps, 0.001))))


def _batch_size() -> int:
    return max(1, int(os.getenv("MX8_FIND_BATCH_SIZE", "16")))


def _score_threshold() -> float:
    return float(os.getenv("MX8_FIND_SCORE_THRESHOLD", "-8.0"))


def _score_margin() -> float:
    return float(os.getenv("MX8_FIND_SCORE_MARGIN", "2.0"))


def _pad_before_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_PAD_BEFORE_MS", "500")))


def _pad_after_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_PAD_AFTER_MS", "1500")))


def _merge_gap_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_MERGE_GAP_MS", "1500")))


def _remote_probe_timeout_secs() -> float:
    return max(1.0, float(os.getenv("MX8_FIND_REMOTE_PROBE_TIMEOUT_SECS", "10")))


def _batched(values: Sequence[Path], batch_size: int) -> Iterable[Sequence[Path]]:
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value)
