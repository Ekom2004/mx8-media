from __future__ import annotations

import os
import subprocess
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Any

try:
    import modal
except ImportError:  # pragma: no cover - local unit tests may run without Modal installed
    modal = None

from api.find_contracts import (
    FindShard,
    FindShardResult,
    FindShardStats,
    MatchSegment,
    find_shard_from_payload,
    find_shard_result_to_payload,
)

_APP_NAME = "mx8-find-worker"
_MODEL_CACHE_DIR = Path("/models/hf")
_MODEL_CACHE_VOLUME_NAME = os.getenv("MX8_FIND_MODAL_HF_CACHE_VOLUME", "mx8-find-hf-cache")

if modal is not None:
    _model_cache_volume = modal.Volume.from_name(_MODEL_CACHE_VOLUME_NAME, create_if_missing=True)
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


@dataclass
class _ModelRuntime:
    processor: Any
    model: Any
    device: str
    torch_dtype: Any


_runtime_lock = threading.Lock()
_runtime_by_model: dict[str, _ModelRuntime] = {}
_query_cache_lock = threading.Lock()
_query_cache: OrderedDict[tuple[str, str], tuple[float, Any]] = OrderedDict()


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
            frame_times_ms, frame_paths, decode_ms = _extract_frames(
                source_ref=source_ref,
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


def _score_frames(*, shard: FindShard, frame_paths: Sequence[Path], source_ref: str) -> tuple[list[float], int]:
    started = monotonic()
    runtime = _runtime_for_model(shard.model)
    query_embedding = _cached_query_embedding(runtime=runtime, shard=shard).to(runtime.device)

    import torch
    from PIL import Image

    scores: list[float] = []
    batch_size = _batch_size()
    for batch_paths in _batched(frame_paths, batch_size):
        images = []
        for path in batch_paths:
            with Image.open(path) as image:
                images.append(image.convert("RGB"))
        inputs = runtime.processor(images=images, return_tensors="pt")
        inputs = {name: tensor.to(runtime.device) for name, tensor in inputs.items()}
        with torch.inference_mode():
            image_features = runtime.model.get_image_features(**inputs)
        image_features = image_features.float()
        image_features = image_features / image_features.norm(p=2, dim=-1, keepdim=True)
        batch_scores = torch.matmul(image_features, query_embedding.unsqueeze(1)).squeeze(1)
        scores.extend(float(score) for score in batch_scores.detach().cpu().tolist())
    return scores, max(0, int((monotonic() - started) * 1000))


def _runtime_for_model(model_alias: str) -> _ModelRuntime:
    model_id = _resolve_model_id(model_alias)
    with _runtime_lock:
        runtime = _runtime_by_model.get(model_id)
        if runtime is not None:
            return runtime

        import torch
        from transformers import AutoModel, AutoProcessor

        os.environ.setdefault("HF_HOME", str(_MODEL_CACHE_DIR))
        _MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if device == "cuda" else torch.float32
        processor = AutoProcessor.from_pretrained(model_id)
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
        )
        _runtime_by_model[model_id] = runtime
        if _model_cache_volume is not None:  # pragma: no branch - only true on Modal
            try:
                _model_cache_volume.commit()
            except Exception:
                pass
        return runtime


def _cached_query_embedding(*, runtime: _ModelRuntime, shard: FindShard):
    query_key = (_resolve_model_id(shard.model), shard.query_id)
    now = monotonic()
    with _query_cache_lock:
        _prune_query_cache_locked(now=now)
        cached = _query_cache.get(query_key)
        if cached is not None:
            _query_cache.move_to_end(query_key)
            return cached[1]

    import torch

    inputs = runtime.processor(text=[shard.query_text], return_tensors="pt")
    inputs = {name: tensor.to(runtime.device) for name, tensor in inputs.items()}
    with torch.inference_mode():
        text_features = runtime.model.get_text_features(**inputs)
    text_features = text_features.float()
    text_features = text_features / text_features.norm(p=2, dim=-1, keepdim=True)
    embedding = text_features[0].detach().cpu()

    with _query_cache_lock:
        _query_cache[query_key] = (now, embedding)
        _query_cache.move_to_end(query_key)
        while len(_query_cache) > _query_cache_max_entries():
            _query_cache.popitem(last=False)
    return embedding


def _prune_query_cache_locked(*, now: float) -> None:
    ttl_secs = _query_cache_ttl_secs()
    expired = [
        query_key
        for query_key, (inserted_at, _) in _query_cache.items()
        if now - inserted_at > ttl_secs
    ]
    for query_key in expired:
        _query_cache.pop(query_key, None)


def _merge_frame_hits(
    *,
    sample_id: int,
    scan_start_ms: int,
    scan_end_ms: int,
    sample_fps: float,
    frame_times_ms: Sequence[int],
    scores: Sequence[float],
) -> tuple[MatchSegment, ...]:
    threshold = _score_threshold()
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


def _query_cache_ttl_secs() -> float:
    return max(1.0, float(os.getenv("MX8_FIND_QUERY_CACHE_TTL_SECS", "1800")))


def _query_cache_max_entries() -> int:
    return max(1, int(os.getenv("MX8_FIND_QUERY_CACHE_MAX_ENTRIES", "10000")))


def _batch_size() -> int:
    return max(1, int(os.getenv("MX8_FIND_BATCH_SIZE", "16")))


def _score_threshold() -> float:
    return float(os.getenv("MX8_FIND_SCORE_THRESHOLD", "0.25"))


def _pad_before_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_PAD_BEFORE_MS", "500")))


def _pad_after_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_PAD_AFTER_MS", "1500")))


def _merge_gap_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_MERGE_GAP_MS", "1500")))


def _batched(values: Sequence[Path], batch_size: int) -> Iterable[Sequence[Path]]:
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]
