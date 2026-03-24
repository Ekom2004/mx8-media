from __future__ import annotations

import json
import math
import os
import subprocess
import threading
from collections import OrderedDict, deque
from dataclasses import dataclass
from email.message import Message
from pathlib import Path
from time import monotonic, time
from typing import Protocol
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4

from .find_access import SourceAccessResolver, build_source_access_resolver
from .find_contracts import (
    FIND_BULK_LANE,
    FIND_INTERACTIVE_LANE,
    FIND_LANES,
    FindShard,
    FindShardResult,
    FindShardStats,
    ManifestRecord,
    MatchSegment,
    find_shard_result_from_payload,
    find_shard_to_payload,
)


class FindTransport(Protocol):
    def process_shard(self, shard: FindShard) -> FindShardResult: ...


class VideoDurationResolver(Protocol):
    def resolve_duration_ms(
        self,
        *,
        location: str,
        source_access_url: str | None,
        decode_hint: str | None,
    ) -> int: ...


@dataclass(frozen=True)
class FindJobSnapshot:
    job_id: str
    source_manifest_hash: str
    source_records: tuple[ManifestRecord, ...]
    status: str
    total_shards: int
    completed_shards: int
    segments: tuple[MatchSegment, ...] = ()
    error: str | None = None


@dataclass
class _PendingFindJob:
    source_manifest_hash: str
    source_records: tuple[ManifestRecord, ...]
    total_shards: int
    completed_shards: int = 0
    segments: list[MatchSegment] | None = None
    status: str = "running"
    error: str | None = None

    def snapshot(self, *, job_id: str) -> FindJobSnapshot:
        return FindJobSnapshot(
            job_id=job_id,
            source_manifest_hash=self.source_manifest_hash,
            source_records=self.source_records,
            status=self.status,
            total_shards=self.total_shards,
            completed_shards=self.completed_shards,
            segments=tuple(self.segments or []),
            error=self.error,
        )


class FindShardQueue:
    def __init__(
        self,
        *,
        worker_slots: int,
        base_active_shards_per_job: int,
        base_active_shards_per_customer: int,
        interactive_weight: int = 4,
        bulk_weight: int = 1,
        borrow_idle_capacity: bool = True,
    ) -> None:
        if worker_slots <= 0:
            raise ValueError("worker_slots must be > 0")
        if base_active_shards_per_job <= 0:
            raise ValueError("base_active_shards_per_job must be > 0")
        if base_active_shards_per_customer <= 0:
            raise ValueError("base_active_shards_per_customer must be > 0")
        if interactive_weight <= 0 or bulk_weight <= 0:
            raise ValueError("lane weights must be > 0")
        self._worker_slots = worker_slots
        self._base_active_shards_per_job = base_active_shards_per_job
        self._base_active_shards_per_customer = base_active_shards_per_customer
        self._borrow_idle_capacity = borrow_idle_capacity
        self._lane_cycle = [FIND_INTERACTIVE_LANE] * interactive_weight + [FIND_BULK_LANE] * bulk_weight
        self._lane_cursor = 0
        self._pending: dict[str, dict[str, dict[str, deque[FindShard]]]] = {
            lane: {} for lane in FIND_LANES
        }
        self._customer_order: dict[str, deque[str]] = {lane: deque() for lane in FIND_LANES}
        self._job_order: dict[tuple[str, str], deque[str]] = {}
        self._active_by_job: dict[str, int] = {}
        self._active_by_customer: dict[str, int] = {}
        self._active_total = 0
        self._closed = False
        self._cv = threading.Condition()

    def enqueue(self, shards: list[FindShard]) -> None:
        if not shards:
            return
        with self._cv:
            self._ensure_open()
            for shard in shards:
                shard.validate()
                self._enqueue_locked(shard)
            self._cv.notify_all()

    def lease(self, timeout_secs: float) -> FindShard | None:
        deadline = monotonic() + max(0.0, timeout_secs)
        with self._cv:
            while True:
                shard = self._pop_next_locked(allow_borrow=False)
                if shard is not None:
                    return shard
                if self._borrow_idle_capacity and self._active_total < self._worker_slots:
                    shard = self._pop_next_locked(allow_borrow=True)
                    if shard is not None:
                        return shard
                if self._closed:
                    return None
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return None
                self._cv.wait(timeout=remaining)

    def complete(self, shard: FindShard) -> None:
        with self._cv:
            self._active_total = max(0, self._active_total - 1)
            self._decrement_active(self._active_by_job, shard.job_id)
            self._decrement_active(self._active_by_customer, shard.customer_id)
            self._cv.notify_all()

    def close(self) -> None:
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("find shard queue is closed")

    def _enqueue_locked(self, shard: FindShard) -> None:
        lane_customers = self._pending[shard.lane]
        customer_jobs = lane_customers.setdefault(shard.customer_id, {})
        if shard.customer_id not in self._customer_order[shard.lane]:
            self._customer_order[shard.lane].append(shard.customer_id)
        jobs = customer_jobs.setdefault(shard.job_id, deque())
        order_key = (shard.lane, shard.customer_id)
        if order_key not in self._job_order:
            self._job_order[order_key] = deque()
        if shard.job_id not in self._job_order[order_key]:
            self._job_order[order_key].append(shard.job_id)
        jobs.append(shard)

    def _pop_next_locked(self, *, allow_borrow: bool) -> FindShard | None:
        for _ in range(len(self._lane_cycle)):
            lane = self._lane_cycle[self._lane_cursor]
            self._lane_cursor = (self._lane_cursor + 1) % len(self._lane_cycle)
            shard = self._pop_from_lane_locked(lane, allow_borrow=allow_borrow)
            if shard is not None:
                return shard
        return None

    def _pop_from_lane_locked(self, lane: str, *, allow_borrow: bool) -> FindShard | None:
        customer_order = self._customer_order[lane]
        for _ in range(len(customer_order)):
            customer_id = customer_order[0]
            customer_order.rotate(-1)
            shard = self._pop_from_customer_locked(lane, customer_id, allow_borrow=allow_borrow)
            if shard is not None:
                return shard
        return None

    def _pop_from_customer_locked(self, lane: str, customer_id: str, *, allow_borrow: bool) -> FindShard | None:
        customer_jobs = self._pending[lane].get(customer_id)
        if not customer_jobs:
            self._remove_customer_locked(lane, customer_id)
            return None
        order_key = (lane, customer_id)
        job_order = self._job_order.setdefault(order_key, deque())
        for _ in range(len(job_order)):
            job_id = job_order[0]
            job_order.rotate(-1)
            shards = customer_jobs.get(job_id)
            if not shards:
                self._remove_job_locked(lane, customer_id, job_id)
                continue
            if not allow_borrow and not self._within_caps_locked(job_id, customer_id):
                continue
            shard = shards.popleft()
            if not shards:
                self._remove_job_locked(lane, customer_id, job_id)
            self._active_total += 1
            self._active_by_job[job_id] = self._active_by_job.get(job_id, 0) + 1
            self._active_by_customer[customer_id] = self._active_by_customer.get(customer_id, 0) + 1
            return shard
        return None

    def _remove_customer_locked(self, lane: str, customer_id: str) -> None:
        self._pending[lane].pop(customer_id, None)
        if customer_id in self._customer_order[lane]:
            self._customer_order[lane] = deque(
                candidate for candidate in self._customer_order[lane] if candidate != customer_id
            )
        self._job_order.pop((lane, customer_id), None)

    def _remove_job_locked(self, lane: str, customer_id: str, job_id: str) -> None:
        customer_jobs = self._pending[lane].get(customer_id)
        if customer_jobs is None:
            return
        customer_jobs.pop(job_id, None)
        order_key = (lane, customer_id)
        if order_key in self._job_order and job_id in self._job_order[order_key]:
            self._job_order[order_key] = deque(
                candidate for candidate in self._job_order[order_key] if candidate != job_id
            )
        if customer_jobs:
            return
        self._remove_customer_locked(lane, customer_id)

    def _within_caps_locked(self, job_id: str, customer_id: str) -> bool:
        return (
            self._active_by_job.get(job_id, 0) < self._base_active_shards_per_job
            and self._active_by_customer.get(customer_id, 0) < self._base_active_shards_per_customer
        )

    @staticmethod
    def _decrement_active(values: dict[str, int], key: str) -> None:
        current = values.get(key)
        if current is None:
            return
        if current <= 1:
            values.pop(key, None)
            return
        values[key] = current - 1


class MockFindTransport:
    def __init__(self) -> None:
        self._query_cache_ttl_secs = find_query_cache_ttl_secs()
        self._query_cache_max_entries = find_query_cache_max_entries()
        self._cache_lock = threading.Lock()
        self._query_cache: OrderedDict[str, tuple[float, str]] = OrderedDict()

    def process_shard(self, shard: FindShard) -> FindShardResult:
        started = monotonic()
        needle = self._cached_query(shard)
        haystacks = [
            _normalize_text(shard.source_uri),
            _normalize_text(shard.asset_id),
            _normalize_text(shard.decode_hint or ""),
        ]
        hits: tuple[MatchSegment, ...] = ()
        if any(needle in haystack for haystack in haystacks):
            end_ms = min(shard.scan_end_ms, shard.scan_start_ms + 1_000)
            if end_ms <= shard.scan_start_ms:
                end_ms = shard.scan_start_ms + 1
            hits = (
                MatchSegment(
                    sample_id=shard.sample_id,
                    start_ms=shard.scan_start_ms,
                    end_ms=end_ms,
                ),
            )
        result = FindShardResult(
            shard_id=shard.shard_id,
            job_id=shard.job_id,
            customer_id=shard.customer_id,
            asset_id=shard.asset_id,
            status="ok",
            hits=hits,
            stats=FindShardStats(
                sampled_frames=estimate_sampled_frames(
                    scan_start_ms=shard.scan_start_ms,
                    scan_end_ms=shard.scan_end_ms,
                    sample_fps=shard.sample_fps,
                ),
                decode_ms=5,
                inference_ms=5,
                wall_ms=max(0, int((monotonic() - started) * 1000)),
            ),
        )
        result.validate()
        return result

    def _cached_query(self, shard: FindShard) -> str:
        with self._cache_lock:
            self._prune_cache_locked()
            cached = self._query_cache.get(shard.query_id)
            now = monotonic()
            if cached is not None and now - cached[0] <= self._query_cache_ttl_secs:
                self._query_cache.move_to_end(shard.query_id)
                return cached[1]
            normalized = _normalize_text(shard.query_text)
            self._query_cache[shard.query_id] = (now, normalized)
            self._query_cache.move_to_end(shard.query_id)
            while len(self._query_cache) > self._query_cache_max_entries:
                self._query_cache.popitem(last=False)
            return normalized

    def _prune_cache_locked(self) -> None:
        now = monotonic()
        expired = [
            query_id
            for query_id, (inserted_at, _) in self._query_cache.items()
            if now - inserted_at > self._query_cache_ttl_secs
        ]
        for query_id in expired:
            self._query_cache.pop(query_id, None)


class ModalFindTransport:
    def __init__(
        self,
        *,
        app_name: str,
        function_name: str,
        environment_name: str | None,
    ) -> None:
        self._app_name = app_name
        self._function_name = function_name
        self._environment_name = environment_name
        self._function = None
        self._lock = threading.Lock()

    def process_shard(self, shard: FindShard) -> FindShardResult:
        function = self._lookup_function()
        payload = find_shard_to_payload(shard)
        raw_result = function.remote(payload)
        if not isinstance(raw_result, dict):
            raise RuntimeError("modal find worker returned non-dict payload")
        result = find_shard_result_from_payload(raw_result)
        if result.shard_id != shard.shard_id:
            raise RuntimeError(
                f"modal find worker returned shard_id={result.shard_id!r} for {shard.shard_id!r}"
            )
        return result

    def _lookup_function(self):
        with self._lock:
            if self._function is None:
                import modal  # type: ignore

                self._function = modal.Function.from_name(
                    self._app_name,
                    self._function_name,
                    environment_name=self._environment_name,
                )
            return self._function


class FfprobeVideoDurationResolver:
    def __init__(self, *, timeout_secs: float) -> None:
        self._timeout_secs = timeout_secs
        self._cache: dict[str, int] = {}
        self._lock = threading.Lock()

    def resolve_duration_ms(
        self,
        *,
        location: str,
        source_access_url: str | None,
        decode_hint: str | None,
    ) -> int:
        cache_key = location.strip()
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        source_ref = (source_access_url or location).strip()
        ensure_remote_probe_source_supported(location=location, source_ref=source_ref)
        duration_ms = probe_video_duration_ms(source_ref, timeout_secs=self._timeout_secs)
        if duration_ms is None:
            raise RuntimeError(f"failed to determine video duration for {location}")
        with self._lock:
            self._cache[cache_key] = duration_ms
        return duration_ms


class FindDispatcher:
    def __init__(
        self,
        transport: FindTransport | None = None,
        *,
        access_resolver: SourceAccessResolver | None = None,
        duration_resolver: VideoDurationResolver | None = None,
    ) -> None:
        self._transport = transport
        self._access_resolver = access_resolver
        self._duration_resolver = duration_resolver
        self._queue = FindShardQueue(
            worker_slots=dispatcher_worker_count(),
            base_active_shards_per_job=base_active_shards_per_job(),
            base_active_shards_per_customer=base_active_shards_per_customer(),
        )
        self._jobs: dict[str, _PendingFindJob] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        if self._threads:
            return
        self._stop_event.clear()
        self._threads = [
            threading.Thread(
                target=self._worker_loop,
                name=f"mx8-find-dispatcher-{index}",
                daemon=True,
            )
            for index in range(dispatcher_worker_count())
        ]
        for thread in self._threads:
            thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._queue.close()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads = []
        transport = self._transport
        if transport is not None:
            close = getattr(transport, "close", None)
            if callable(close):
                close()

    def has_job(self, job_id: str) -> bool:
        with self._lock:
            return job_id in self._jobs

    def submit(
        self,
        *,
        job_id: str,
        customer_id: str,
        lane: str,
        priority: int,
        query_text: str,
        source_manifest_hash: str,
        source_records: list[ManifestRecord],
        video_records: list[ManifestRecord],
    ) -> bool:
        with self._lock:
            if job_id in self._jobs:
                return False
        shards = build_find_shards(
            job_id=job_id,
            customer_id=customer_id,
            lane=lane,
            priority=priority,
            query_id=f"qry_{uuid4().hex}",
            query_text=query_text,
            records=video_records,
            access_resolver=self._access_resolver_instance(),
            duration_resolver=self._duration_resolver_instance(),
        )
        job = _PendingFindJob(
            source_manifest_hash=source_manifest_hash,
            source_records=tuple(source_records),
            total_shards=len(shards),
            segments=[],
        )
        with self._lock:
            if job_id in self._jobs:
                return False
            self._jobs[job_id] = job
        self._queue.enqueue(shards)
        return True

    def pop_terminal_snapshot(self, job_id: str) -> FindJobSnapshot | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job.status not in {"complete", "failed"}:
                return None
            snapshot = job.snapshot(job_id=job_id)
            del self._jobs[job_id]
            return snapshot

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            shard = self._queue.lease(timeout_secs=0.25)
            if shard is None:
                continue
            try:
                result = self._transport_instance().process_shard(shard)
            except Exception as exc:
                result = FindShardResult(
                    shard_id=shard.shard_id,
                    job_id=shard.job_id,
                    customer_id=shard.customer_id,
                    asset_id=shard.asset_id,
                    status="error",
                    hits=(),
                    stats=FindShardStats(
                        sampled_frames=0,
                        decode_ms=0,
                        inference_ms=0,
                        wall_ms=0,
                    ),
                    error=str(exc),
                )
            finally:
                self._queue.complete(shard)
            self._record_result(result)

    def _record_result(self, result: FindShardResult) -> None:
        result.validate()
        with self._lock:
            job = self._jobs.get(result.job_id)
            if job is None or job.status in {"complete", "failed"}:
                return
            job.completed_shards += 1
            if result.status == "error":
                job.status = "failed"
                job.error = result.error or "find shard failed"
                return
            if job.segments is None:
                job.segments = []
            job.segments.extend(result.hits)
            if job.completed_shards >= job.total_shards:
                job.status = "complete"

    def _transport_instance(self) -> FindTransport:
        transport = self._transport
        if transport is None:
            transport = build_find_transport()
            self._transport = transport
        return transport

    def _access_resolver_instance(self) -> SourceAccessResolver:
        resolver = self._access_resolver
        if resolver is None:
            resolver = build_source_access_resolver()
            self._access_resolver = resolver
        return resolver

    def _duration_resolver_instance(self) -> VideoDurationResolver:
        resolver = self._duration_resolver
        if resolver is None:
            resolver = build_video_duration_resolver()
            self._duration_resolver = resolver
        return resolver


def build_find_transport() -> FindTransport:
    provider = os.getenv("MX8_FIND_PROVIDER", "modal").strip().lower()
    if provider == "mock":
        return MockFindTransport()
    if provider != "modal":
        raise RuntimeError(f"unsupported find provider: {provider}")
    app_name = os.getenv("MX8_FIND_MODAL_APP_NAME", "").strip()
    if not app_name:
        raise RuntimeError("MX8_FIND_MODAL_APP_NAME must be set for modal find transport")
    function_name = os.getenv("MX8_FIND_MODAL_FUNCTION_NAME", "process_shard").strip() or "process_shard"
    environment_name = os.getenv("MX8_FIND_MODAL_ENVIRONMENT", "").strip() or None
    return ModalFindTransport(
        app_name=app_name,
        function_name=function_name,
        environment_name=environment_name,
    )


def build_video_duration_resolver() -> VideoDurationResolver:
    return FfprobeVideoDurationResolver(timeout_secs=find_duration_probe_timeout_secs())


def dispatcher_worker_count() -> int:
    return max(1, int(os.getenv("MX8_FIND_DISPATCHER_WORKERS", "4")))


def find_shard_window_ms() -> int:
    return max(1_000, int(os.getenv("MX8_FIND_SHARD_WINDOW_MS", "60000")))


def find_shard_overlap_ms() -> int:
    return max(0, int(os.getenv("MX8_FIND_SHARD_OVERLAP_MS", "1000")))


def find_sample_fps() -> float:
    return max(0.1, float(os.getenv("MX8_FIND_SAMPLE_FPS", "1.0")))


def find_model_name() -> str:
    return os.getenv("MX8_FIND_MODEL", "siglip2_base").strip() or "siglip2_base"


def base_active_shards_per_job() -> int:
    return max(1, int(os.getenv("MX8_FIND_BASE_ACTIVE_SHARDS_PER_JOB", "8")))


def base_active_shards_per_customer() -> int:
    return max(1, int(os.getenv("MX8_FIND_BASE_ACTIVE_SHARDS_PER_CUSTOMER", "16")))


def find_query_cache_ttl_secs() -> float:
    return max(1.0, float(os.getenv("MX8_FIND_QUERY_CACHE_TTL_SECS", "1800")))


def find_query_cache_max_entries() -> int:
    return max(1, int(os.getenv("MX8_FIND_QUERY_CACHE_MAX_ENTRIES", "10000")))


def find_duration_probe_timeout_secs() -> float:
    return max(1.0, float(os.getenv("MX8_FIND_DURATION_PROBE_TIMEOUT_SECS", "15")))


def find_remote_probe_timeout_secs() -> float:
    return max(1.0, float(os.getenv("MX8_FIND_REMOTE_PROBE_TIMEOUT_SECS", "10")))


def build_find_shards(
    *,
    job_id: str,
    customer_id: str,
    lane: str,
    priority: int,
    query_id: str,
    query_text: str,
    records: list[ManifestRecord],
    access_resolver: SourceAccessResolver | None = None,
    duration_resolver: VideoDurationResolver | None = None,
) -> list[FindShard]:
    shards: list[FindShard] = []
    window_ms = find_shard_window_ms()
    overlap_ms = find_shard_overlap_ms()
    sample_fps = find_sample_fps()
    model = find_model_name()
    created_at_ms = int(time() * 1000)
    shard_index = 0
    for record in records:
        source_access_url = access_resolver.resolve(record.location) if access_resolver is not None else None
        for scan_start_ms, scan_end_ms in find_scan_ranges_for_record(
            record=record,
            source_access_url=source_access_url,
            window_ms=window_ms,
            overlap_ms=overlap_ms,
            duration_resolver=duration_resolver,
        ):
            shard = FindShard(
                shard_id=f"shd_{job_id}_{shard_index:06d}",
                job_id=job_id,
                customer_id=customer_id,
                lane=lane,
                priority=priority,
                attempt=0,
                query_id=query_id,
                query_text=query_text,
                source_uri=record.location,
                asset_id=asset_id_from_location(record.location),
                decode_hint=record.decode_hint,
                sample_id=record.sample_id,
                scan_start_ms=scan_start_ms,
                scan_end_ms=scan_end_ms,
                overlap_ms=overlap_ms,
                sample_fps=sample_fps,
                model=model,
                created_at_ms=created_at_ms,
                source_access_url=source_access_url,
            )
            shard.validate()
            shards.append(shard)
            shard_index += 1
    return shards


def find_scan_ranges_for_record(
    *,
    record: ManifestRecord,
    source_access_url: str | None,
    window_ms: int,
    overlap_ms: int,
    duration_resolver: VideoDurationResolver | None,
) -> list[tuple[int, int]]:
    if record.segment_start_ms is not None and record.segment_end_ms is not None:
        return slice_scan_ranges(
            start_ms=record.segment_start_ms,
            end_ms=record.segment_end_ms,
            window_ms=window_ms,
            overlap_ms=overlap_ms,
        )
    if duration_resolver is None:
        raise RuntimeError(
            f"duration resolver required to shard unbounded video record {record.location}"
        )
    duration_ms = duration_resolver.resolve_duration_ms(
        location=record.location,
        source_access_url=source_access_url,
        decode_hint=record.decode_hint,
    )
    return slice_scan_ranges(
        start_ms=0,
        end_ms=duration_ms,
        window_ms=window_ms,
        overlap_ms=overlap_ms,
    )


def slice_scan_ranges(
    *,
    start_ms: int,
    end_ms: int,
    window_ms: int,
    overlap_ms: int,
) -> list[tuple[int, int]]:
    if end_ms <= start_ms:
        raise RuntimeError(f"invalid scan range: start_ms={start_ms} end_ms={end_ms}")
    step_ms = max(1, window_ms - overlap_ms)
    ranges: list[tuple[int, int]] = []
    cursor = start_ms
    while cursor < end_ms:
        scan_end_ms = min(end_ms, cursor + window_ms)
        ranges.append((cursor, scan_end_ms))
        if scan_end_ms >= end_ms:
            break
        cursor += step_ms
    return ranges


def probe_video_duration_ms(source_ref: str, *, timeout_secs: float) -> int | None:
    command = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "stream=duration:format=duration",
        "-of",
        "json",
        source_ref,
    ]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_secs,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("ffprobe is required for find duration probing but was not found in PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"ffprobe timed out while probing {source_ref}") from exc
    if completed.returncode != 0:
        stderr = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"ffprobe duration probe failed for {source_ref}: {stderr}")
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ffprobe duration output was not valid JSON for {source_ref}: {exc}") from exc
    durations: list[float] = []
    for stream in payload.get("streams", []):
        duration = _parse_duration_secs(stream.get("duration"))
        if duration is not None:
            durations.append(duration)
    format_duration = _parse_duration_secs((payload.get("format") or {}).get("duration"))
    if format_duration is not None:
        durations.append(format_duration)
    if not durations:
        return None
    return max(1, int(max(durations) * 1000.0))


def ensure_remote_probe_source_supported(*, location: str, source_ref: str) -> None:
    parsed = urlsplit(source_ref)
    if parsed.scheme not in {"http", "https"}:
        return
    request = Request(
        source_ref,
        headers={
            "Range": "bytes=0-0",
            "User-Agent": "mx8-find-planner/1",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=find_remote_probe_timeout_secs()) as response:
            validate_remote_probe_response(
                status=getattr(response, "status", response.getcode()),
                headers=response.headers,
                source_ref=source_ref,
            )
    except Exception as exc:
        raise RuntimeError(f"remote source preflight failed for {location}: {exc}") from exc


def validate_remote_probe_response(*, status: int, headers: Message, source_ref: str) -> None:
    accept_ranges = headers.get("Accept-Ranges", "")
    content_range = headers.get("Content-Range", "")
    content_type = headers.get_content_type()
    if not remote_probe_supports_byte_ranges(
        status=status,
        accept_ranges=accept_ranges,
        content_range=content_range,
    ):
        raise RuntimeError(
            "http/https source must support byte-range reads "
            f"(status={status}, accept_ranges={accept_ranges or '<empty>'}, "
            f"content_range={content_range or '<empty>'})"
        )
    if not remote_probe_is_media(content_type=content_type, source_ref=source_ref):
        raise RuntimeError(
            "http/https source must be a direct media object "
            f"(content_type={content_type or '<empty>'})"
        )


def remote_probe_supports_byte_ranges(*, status: int, accept_ranges: str, content_range: str) -> bool:
    if status == 206:
        return True
    if accept_ranges.strip().lower() == "bytes":
        return True
    return content_range.strip().lower().startswith("bytes ")


def remote_probe_is_media(*, content_type: str, source_ref: str) -> bool:
    normalized = (content_type or "").strip().lower()
    if normalized.startswith("video/") or normalized.startswith("audio/"):
        return True
    if normalized in {"application/octet-stream", "binary/octet-stream"}:
        return True
    suffix = Path(urlsplit(source_ref).path).suffix.lower()
    return suffix in {
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


def _parse_duration_secs(value: object) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def estimate_sampled_frames(*, scan_start_ms: int, scan_end_ms: int, sample_fps: float) -> int:
    duration_ms = max(1, scan_end_ms - scan_start_ms)
    return max(1, int((duration_ms / 1000.0) * sample_fps))


def asset_id_from_location(location: str) -> str:
    path = urlsplit(location).path
    name = Path(path).name
    return name or location.rsplit("/", 1)[-1]


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())
