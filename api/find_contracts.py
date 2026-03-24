from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

FIND_INTERACTIVE_LANE = "interactive"
FIND_BULK_LANE = "bulk"
FIND_LANES = (FIND_INTERACTIVE_LANE, FIND_BULK_LANE)


@dataclass(frozen=True)
class ManifestRecord:
    sample_id: int
    location: str
    byte_offset: int | None
    byte_length: int | None
    decode_hint: str | None
    segment_start_ms: int | None = None
    segment_end_ms: int | None = None

    def validate(self) -> None:
        if not self.location.strip():
            raise ValueError("location must be non-empty")
        if (self.byte_offset is None) != (self.byte_length is None):
            raise ValueError("byte_offset and byte_length must be set together")
        if self.byte_length is not None and self.byte_length <= 0:
            raise ValueError("byte_length must be > 0")
        if (self.segment_start_ms is None) != (self.segment_end_ms is None):
            raise ValueError("segment_start_ms and segment_end_ms must be set together")
        if self.segment_start_ms is not None and self.segment_end_ms is not None:
            if self.segment_end_ms <= self.segment_start_ms:
                raise ValueError("segment_end_ms must be > segment_start_ms")


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
        if self.lane not in FIND_LANES:
            raise ValueError(f"unsupported lane: {self.lane!r}")
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


def find_shard_to_payload(shard: FindShard) -> dict[str, object]:
    shard.validate()
    return {
        "shard_id": shard.shard_id,
        "job_id": shard.job_id,
        "customer_id": shard.customer_id,
        "lane": shard.lane,
        "priority": shard.priority,
        "attempt": shard.attempt,
        "query_id": shard.query_id,
        "query_text": shard.query_text,
        "source_uri": shard.source_uri,
        "asset_id": shard.asset_id,
        "decode_hint": shard.decode_hint,
        "source_access_url": shard.source_access_url,
        "sample_id": shard.sample_id,
        "scan_start_ms": shard.scan_start_ms,
        "scan_end_ms": shard.scan_end_ms,
        "overlap_ms": shard.overlap_ms,
        "sample_fps": shard.sample_fps,
        "model": shard.model,
        "created_at_ms": shard.created_at_ms,
    }


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
        source_access_url=_optional_str(payload.get("source_access_url")),
        sample_id=int(payload["sample_id"]),
        scan_start_ms=int(payload["scan_start_ms"]),
        scan_end_ms=int(payload["scan_end_ms"]),
        overlap_ms=int(payload["overlap_ms"]),
        sample_fps=float(payload["sample_fps"]),
        model=str(payload["model"]),
        created_at_ms=int(payload["created_at_ms"]),
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


def find_shard_result_from_payload(payload: Mapping[str, object]) -> FindShardResult:
    raw_hits = payload.get("hits")
    if not isinstance(raw_hits, list):
        raise ValueError("find shard result payload missing hits list")
    raw_stats = payload.get("stats")
    if not isinstance(raw_stats, Mapping):
        raise ValueError("find shard result payload missing stats object")
    result = FindShardResult(
        shard_id=str(payload["shard_id"]),
        job_id=str(payload["job_id"]),
        customer_id=str(payload["customer_id"]),
        asset_id=str(payload["asset_id"]),
        status=str(payload["status"]),
        hits=tuple(
            MatchSegment(
                sample_id=int(raw_hit["sample_id"]),
                start_ms=int(raw_hit["start_ms"]),
                end_ms=int(raw_hit["end_ms"]),
            )
            for raw_hit in raw_hits
            if isinstance(raw_hit, Mapping)
        ),
        stats=FindShardStats(
            sampled_frames=int(raw_stats["sampled_frames"]),
            decode_ms=int(raw_stats["decode_ms"]),
            inference_ms=int(raw_stats["inference_ms"]),
            wall_ms=int(raw_stats["wall_ms"]),
        ),
        error=_optional_str(payload.get("error")),
    )
    result.validate()
    return result


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
