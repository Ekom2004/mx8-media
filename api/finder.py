from __future__ import annotations

import hashlib
import logging
import os
import shlex
import subprocess
import threading
from pathlib import Path
from typing import Callable, Protocol
from urllib.parse import urlsplit

from .find_contracts import FIND_INTERACTIVE_LANE, ManifestRecord, MatchSegment
from .find_dispatcher import FindDispatcher
from .models import JobProgressUpdate, JobRecord, JobStatus
from .storage import JobStore

LOGGER = logging.getLogger(__name__)
MANIFEST_SCHEMA_VERSION = 0
VIDEO_EXTENSIONS = {
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


class SourceManifestResolver(Protocol):
    def resolve(self, source: str) -> tuple[str, list[ManifestRecord]]: ...


class LocalFsManifestStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    @classmethod
    def from_env(cls) -> "LocalFsManifestStore":
        root = os.getenv("MX8_MANIFEST_STORE_ROOT", "").strip()
        if not root:
            home = os.path.expanduser("~")
            root = str(Path(home) / ".mx8" / "manifests") if home else "/tmp/.mx8/manifests"
        if root.startswith("s3://"):
            raise RuntimeError("planner currently requires a local filesystem manifest store root")
        return cls(Path(root))

    def get_manifest_bytes(self, manifest_hash: str) -> bytes:
        path = self._manifest_path(manifest_hash)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise RuntimeError(f"manifest not found in local manifest store: {manifest_hash}") from exc

    def put_manifest_bytes(self, manifest_bytes: bytes) -> str:
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        path = self._manifest_path(manifest_hash)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            existing = path.read_bytes()
            if existing != manifest_bytes:
                raise RuntimeError(f"manifest hash collision for {manifest_hash}")
            return manifest_hash
        path.write_bytes(manifest_bytes)
        return manifest_hash

    def _manifest_path(self, manifest_hash: str) -> Path:
        if not manifest_hash.strip() or "/" in manifest_hash or "\\" in manifest_hash or ".." in manifest_hash:
            raise RuntimeError(f"invalid manifest hash: {manifest_hash!r}")
        return self.root / "by-hash" / manifest_hash


class CommandSourceManifestResolver:
    def __init__(self, repo_root: Path, manifest_store: LocalFsManifestStore) -> None:
        self._repo_root = repo_root
        self._manifest_store = manifest_store

    def resolve(self, source: str) -> tuple[str, list[ManifestRecord]]:
        command = self._command_for_source(source)
        env = os.environ.copy()
        env["MX8_DATASET_LINK"] = source
        env["MX8_MANIFEST_STORE_ROOT"] = str(self._manifest_store.root)
        completed = subprocess.run(
            command,
            cwd=self._repo_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "snapshot resolve failed: "
                f"status={completed.returncode} stderr={completed.stderr.strip() or '<empty>'}"
            )
        manifest_hash = self._parse_manifest_hash(completed.stdout)
        manifest_bytes = self._manifest_store.get_manifest_bytes(manifest_hash)
        return manifest_hash, parse_canonical_manifest_tsv(manifest_bytes)

    def _command_for_source(self, source: str) -> list[str]:
        raw = os.getenv("MX8_SNAPSHOT_RESOLVE_CMD", "").strip()
        if raw:
            return shlex.split(raw)
        features: list[str] = []
        if source.startswith("s3://"):
            features.append("s3")
        if not features:
            binary = self._repo_root / "target" / "debug" / "mx8-snapshot-resolve"
            if binary.is_file() and os.access(binary, os.X_OK):
                return [str(binary)]
        command = ["cargo", "run", "-p", "mx8-snapshot", "--bin", "mx8-snapshot-resolve"]
        if features:
            command.extend(["--features", ",".join(features)])
        command.append("--")
        return command

    @staticmethod
    def _parse_manifest_hash(stdout: str) -> str:
        for line in stdout.splitlines():
            if line.startswith("manifest_hash:"):
                return line.split(":", 1)[1].strip()
        raise RuntimeError(f"snapshot resolve output did not contain manifest_hash: {stdout!r}")


class JobFinder:
    def __init__(
        self,
        store: JobStore,
        *,
        dispatcher: FindDispatcher | None = None,
        manifest_resolver: SourceManifestResolver | None = None,
        wake_scaler: Callable[[], None] | None = None,
    ) -> None:
        self._store = store
        self._wake_scaler = wake_scaler
        self._repo_root = Path(__file__).resolve().parent.parent
        self._manifest_store: LocalFsManifestStore | None = None
        self._manifest_resolver = manifest_resolver
        self._dispatcher = dispatcher or FindDispatcher()
        self._owns_dispatcher = dispatcher is None
        self._poll_secs = max(1.0, float(os.getenv("MX8_FIND_POLL_SECS", "2")))
        self._wake_event = threading.Event()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._owns_dispatcher:
            self._dispatcher.start()
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="mx8-media-finder",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None
        if self._owns_dispatcher:
            self._dispatcher.stop()

    def wake(self) -> None:
        self._wake_event.set()

    def reconcile_once(self) -> None:
        for record in self._store.list_jobs():
            if record.status != JobStatus.FINDING or record.find is None:
                continue
            terminal = self._dispatcher.pop_terminal_snapshot(record.id)
            if terminal is not None:
                self._finalize_record(record, terminal)
                continue
            if self._dispatcher.has_job(record.id):
                continue
            self._submit_record(record)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.reconcile_once()
            self._wake_event.wait(self._poll_secs)
            self._wake_event.clear()

    def _submit_record(self, record: JobRecord) -> None:
        try:
            source_manifest_hash, source_records = self._resolver().resolve(record.source)
            video_records = [candidate for candidate in source_records if is_video_record(candidate)]
            if not video_records:
                self._store.update_job_progress(
                    JobProgressUpdate(
                        job_id=record.id,
                        status=JobStatus.COMPLETE,
                        manifest_hash=source_manifest_hash,
                        matched_assets=0,
                        matched_segments=0,
                        total_objects=0,
                        total_bytes=0,
                        current_workers=0,
                        desired_workers=0,
                    )
                )
                return
            self._dispatcher.submit(
                job_id=record.id,
                customer_id=default_find_customer_id(),
                lane=default_find_lane(),
                priority=default_find_priority(),
                query_text=record.find or "",
                source_manifest_hash=source_manifest_hash,
                source_records=source_records,
                video_records=video_records,
            )
        except Exception:
            LOGGER.exception("finder failed to submit job %s", record.id)
            self._store.update_job_progress(
                JobProgressUpdate(
                    job_id=record.id,
                    status=JobStatus.FAILED,
                    current_workers=0,
                    desired_workers=0,
                )
            )

    def _finalize_record(self, record: JobRecord, snapshot) -> None:
        try:
            if snapshot.status == "failed":
                self._store.update_job_progress(
                    JobProgressUpdate(
                        job_id=record.id,
                        status=JobStatus.FAILED,
                        current_workers=0,
                        desired_workers=0,
                    )
                )
                return
            segments = normalize_segments(list(snapshot.segments), list(snapshot.source_records))
            if not segments:
                self._store.update_job_progress(
                    JobProgressUpdate(
                        job_id=record.id,
                        status=JobStatus.COMPLETE,
                        manifest_hash=snapshot.source_manifest_hash,
                        matched_assets=0,
                        matched_segments=0,
                        total_objects=0,
                        total_bytes=0,
                        current_workers=0,
                        desired_workers=0,
                    )
                )
                return
            derived_records = build_derived_manifest(list(snapshot.source_records), segments)
            manifest_bytes = canonical_manifest_bytes(derived_records)
            derived_manifest_hash = self._manifest_store_instance().put_manifest_bytes(manifest_bytes)
            matched_assets = len({segment.sample_id for segment in segments})
            matched_segments = len(segments)
            updated = self._store.update_job_progress(
                JobProgressUpdate(
                    job_id=record.id,
                    status=JobStatus.PLANNED,
                    manifest_hash=derived_manifest_hash,
                    matched_assets=matched_assets,
                    matched_segments=matched_segments,
                    total_objects=matched_segments,
                    total_bytes=0,
                    current_workers=0,
                    desired_workers=0,
                )
            )
            if updated is not None and should_auto_queue_after_plan():
                pending = self._store.update_job_status(record.id, JobStatus.PENDING)
                if pending is not None and self._wake_scaler is not None:
                    self._wake_scaler()
        except Exception:
            LOGGER.exception("finder failed to finalize job %s", record.id)
            self._store.update_job_progress(
                JobProgressUpdate(
                    job_id=record.id,
                    status=JobStatus.FAILED,
                    current_workers=0,
                    desired_workers=0,
                )
            )

    def _manifest_store_instance(self) -> LocalFsManifestStore:
        if self._manifest_store is None:
            self._manifest_store = LocalFsManifestStore.from_env()
        return self._manifest_store

    def _resolver(self) -> SourceManifestResolver:
        if self._manifest_resolver is None:
            self._manifest_resolver = CommandSourceManifestResolver(
                self._repo_root,
                self._manifest_store_instance(),
            )
        return self._manifest_resolver


def should_auto_queue_after_plan() -> bool:
    raw = os.getenv("MX8_FIND_AUTO_QUEUE_AFTER_PLAN", "true").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def default_find_customer_id() -> str:
    return os.getenv("MX8_FIND_DEFAULT_CUSTOMER_ID", "default").strip() or "default"


def default_find_lane() -> str:
    lane = os.getenv("MX8_FIND_DEFAULT_LANE", FIND_INTERACTIVE_LANE).strip().lower()
    if lane not in {FIND_INTERACTIVE_LANE, "bulk"}:
        return FIND_INTERACTIVE_LANE
    return lane


def default_find_priority() -> int:
    return max(0, int(os.getenv("MX8_FIND_DEFAULT_PRIORITY", "100")))


def parse_canonical_manifest_tsv(manifest_bytes: bytes) -> list[ManifestRecord]:
    try:
        text = manifest_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"manifest is not utf-8: {exc}") from exc
    lines = iter(text.splitlines())
    header = next((line for line in lines if line.strip()), None)
    if header is None:
        raise RuntimeError("manifest is empty")
    key, sep, value = header.partition("=")
    if sep != "=" or key.strip() != "schema_version":
        raise RuntimeError("manifest header must be schema_version=<n>")
    schema_version = int(value.strip())
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise RuntimeError(f"unsupported schema_version {schema_version}")
    records: list[ManifestRecord] = []
    for line_no, raw_line in enumerate(lines, start=2):
        line = raw_line.rstrip("\r")
        if not line.strip() or line.startswith("#"):
            continue
        cols = line.split("\t")
        if len(cols) not in {5, 7}:
            raise RuntimeError(f"line {line_no}: expected 5 or 7 columns")
        record = ManifestRecord(
            sample_id=int(cols[0].strip()),
            location=cols[1].strip(),
            byte_offset=parse_optional_int(cols[2]),
            byte_length=parse_optional_int(cols[3]),
            decode_hint=parse_optional_str(cols[4]),
            segment_start_ms=parse_optional_int(cols[5]) if len(cols) == 7 else None,
            segment_end_ms=parse_optional_int(cols[6]) if len(cols) == 7 else None,
        )
        record.validate()
        records.append(record)
    records.sort(key=lambda item: item.sample_id)
    for index, record in enumerate(records):
        if record.sample_id != index:
            raise RuntimeError(f"expected sample_id {index} but found {record.sample_id}")
    return records


def canonical_manifest_bytes(records: list[ManifestRecord]) -> bytes:
    lines = [f"schema_version={MANIFEST_SCHEMA_VERSION}\n"]
    for record in records:
        record.validate()
        lines.append(
            "\t".join(
                [
                    str(record.sample_id),
                    record.location,
                    "" if record.byte_offset is None else str(record.byte_offset),
                    "" if record.byte_length is None else str(record.byte_length),
                    record.decode_hint or "",
                    "" if record.segment_start_ms is None else str(record.segment_start_ms),
                    "" if record.segment_end_ms is None else str(record.segment_end_ms),
                ]
            )
            + "\n"
        )
    return "".join(lines).encode("utf-8")


def normalize_segments(
    raw_segments: list[MatchSegment],
    source_records: list[ManifestRecord],
) -> list[MatchSegment]:
    known_sample_ids = {record.sample_id for record in source_records}
    deduped: set[tuple[int, int, int]] = set()
    normalized: list[MatchSegment] = []
    for segment in raw_segments:
        segment.validate()
        if segment.sample_id not in known_sample_ids:
            raise RuntimeError(f"find provider returned unknown sample_id {segment.sample_id}")
        key = (segment.sample_id, segment.start_ms, segment.end_ms)
        if key in deduped:
            continue
        deduped.add(key)
        normalized.append(segment)
    normalized.sort(key=lambda item: (item.sample_id, item.start_ms, item.end_ms))
    return normalized


def build_derived_manifest(
    source_records: list[ManifestRecord],
    segments: list[MatchSegment],
) -> list[ManifestRecord]:
    records_by_id = {record.sample_id: record for record in source_records}
    derived: list[ManifestRecord] = []
    for index, segment in enumerate(segments):
        source_record = records_by_id[segment.sample_id]
        record = ManifestRecord(
            sample_id=index,
            location=source_record.location,
            byte_offset=source_record.byte_offset,
            byte_length=source_record.byte_length,
            decode_hint=source_record.decode_hint,
            segment_start_ms=segment.start_ms,
            segment_end_ms=segment.end_ms,
        )
        record.validate()
        derived.append(record)
    return derived


def parse_optional_int(raw: str) -> int | None:
    value = raw.strip()
    return int(value) if value else None


def parse_optional_str(raw: str) -> str | None:
    value = raw.strip()
    return value or None


def is_video_record(record: ManifestRecord) -> bool:
    hint = (record.decode_hint or "").strip().lower()
    if hint.startswith("mx8:video;"):
        return True
    path = urlsplit(record.location).path.lower()
    return any(path.endswith(extension) for extension in VIDEO_EXTENSIONS)


def normalize_text(value: str) -> str:
    return " ".join(value.lower().split())
