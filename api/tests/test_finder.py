from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.find_contracts import FindShard, FindShardResult, FindShardStats, ManifestRecord, MatchSegment
from api.find_access import PassthroughSourceAccessResolver
from api.find_dispatcher import FindDispatcher, FindTransport
from api.finder import JobFinder, LocalFsManifestStore, parse_canonical_manifest_tsv
from api.models import CreateJobRequest, JobStatus, TransformSpec
from api.storage import InMemoryJobStore


class FakeResolver:
    def __init__(self, manifest_hash: str, records: list[ManifestRecord]) -> None:
        self.manifest_hash = manifest_hash
        self.records = records

    def resolve(self, source: str) -> tuple[str, list[ManifestRecord]]:
        del source
        return self.manifest_hash, list(self.records)


class StaticTransport:
    def __init__(self, segments_by_sample_id: dict[int, list[MatchSegment]]) -> None:
        self._segments_by_sample_id = segments_by_sample_id

    def process_shard(self, shard: FindShard) -> FindShardResult:
        hits = tuple(self._segments_by_sample_id.get(shard.sample_id, []))
        return FindShardResult(
            shard_id=shard.shard_id,
            job_id=shard.job_id,
            customer_id=shard.customer_id,
            asset_id=shard.asset_id,
            status="ok",
            hits=hits,
            stats=FindShardStats(sampled_frames=1, decode_ms=1, inference_ms=1, wall_ms=1),
        )


class FakeDurationResolver:
    def __init__(self, durations_ms: dict[str, int]) -> None:
        self._durations_ms = durations_ms

    def resolve_duration_ms(
        self,
        *,
        location: str,
        source_access_url: str | None,
        decode_hint: str | None,
    ) -> int:
        del source_access_url
        del decode_hint
        return self._durations_ms[location]


class FinderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.prev_manifest_root = os.environ.get("MX8_MANIFEST_STORE_ROOT")
        self.prev_auto_queue = os.environ.get("MX8_FIND_AUTO_QUEUE_AFTER_PLAN")
        os.environ["MX8_MANIFEST_STORE_ROOT"] = self.tempdir.name
        os.environ["MX8_FIND_AUTO_QUEUE_AFTER_PLAN"] = "false"

    def tearDown(self) -> None:
        if self.prev_manifest_root is None:
            os.environ.pop("MX8_MANIFEST_STORE_ROOT", None)
        else:
            os.environ["MX8_MANIFEST_STORE_ROOT"] = self.prev_manifest_root
        if self.prev_auto_queue is None:
            os.environ.pop("MX8_FIND_AUTO_QUEUE_AFTER_PLAN", None)
        else:
            os.environ["MX8_FIND_AUTO_QUEUE_AFTER_PLAN"] = self.prev_auto_queue
        self.tempdir.cleanup()

    def test_find_job_with_matches_enters_planned_and_writes_derived_manifest(self) -> None:
        store = InMemoryJobStore()
        record = store.create_job(
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"})],
            )
        )
        dispatcher = FindDispatcher(
            access_resolver=PassthroughSourceAccessResolver(),
            transport=StaticTransport(
                {
                    1: [
                        MatchSegment(sample_id=1, start_ms=12_340, end_ms=12_890),
                        MatchSegment(sample_id=1, start_ms=20_000, end_ms=20_500),
                    ]
                }
            ),
            duration_resolver=FakeDurationResolver(
                {
                    "s3://bucket/input-0.mp4": 60_000,
                    "s3://bucket/input-1.mp4": 60_000,
                }
            ),
        )
        dispatcher.start()
        try:
            finder = JobFinder(
                store,
                dispatcher=dispatcher,
                manifest_resolver=FakeResolver(
                    "basehash",
                    [
                        ManifestRecord(
                            sample_id=0,
                            location="s3://bucket/input-0.mp4",
                            byte_offset=None,
                            byte_length=None,
                            decode_hint="mx8:video;codec=h264",
                        ),
                        ManifestRecord(
                            sample_id=1,
                            location="s3://bucket/input-1.mp4",
                            byte_offset=None,
                            byte_length=None,
                            decode_hint="mx8:video;codec=h264",
                        ),
                    ],
                ),
            )

            self._reconcile_until_terminal(finder, store, record.id)
        finally:
            dispatcher.stop()

        updated = store.get_job(record.id)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.status, JobStatus.PLANNED)
        self.assertEqual(updated.matched_assets, 1)
        self.assertEqual(updated.matched_segments, 2)
        self.assertEqual(updated.total_objects, 2)
        self.assertEqual(updated.total_bytes, 0)
        self.assertIsNotNone(updated.manifest_hash)

        manifest_store = LocalFsManifestStore.from_env()
        manifest_bytes = manifest_store.get_manifest_bytes(updated.manifest_hash or "")
        derived_records = parse_canonical_manifest_tsv(manifest_bytes)
        self.assertEqual(len(derived_records), 2)
        self.assertEqual(derived_records[0].location, "s3://bucket/input-1.mp4")
        self.assertEqual(derived_records[0].segment_start_ms, 12_340)
        self.assertEqual(derived_records[0].segment_end_ms, 12_890)
        self.assertEqual(derived_records[1].segment_start_ms, 20_000)
        self.assertEqual(derived_records[1].segment_end_ms, 20_500)

    def test_find_job_with_no_matches_completes_successfully(self) -> None:
        store = InMemoryJobStore()
        record = store.create_job(
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"})],
            )
        )
        dispatcher = FindDispatcher(
            transport=StaticTransport({}),
            access_resolver=PassthroughSourceAccessResolver(),
            duration_resolver=FakeDurationResolver({"s3://bucket/input-0.mp4": 60_000}),
        )
        dispatcher.start()
        try:
            finder = JobFinder(
                store,
                dispatcher=dispatcher,
                manifest_resolver=FakeResolver(
                    "basehash",
                    [
                        ManifestRecord(
                            sample_id=0,
                            location="s3://bucket/input-0.mp4",
                            byte_offset=None,
                            byte_length=None,
                            decode_hint="mx8:video;codec=h264",
                        )
                    ],
                ),
            )

            self._reconcile_until_terminal(finder, store, record.id)
        finally:
            dispatcher.stop()

        updated = store.get_job(record.id)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.status, JobStatus.COMPLETE)
        self.assertEqual(updated.matched_assets, 0)
        self.assertEqual(updated.matched_segments, 0)
        self.assertEqual(updated.total_objects, 0)
        self.assertEqual(updated.total_bytes, 0)
        self.assertEqual(updated.manifest_hash, "basehash")

    def test_find_requires_video_extract_frames(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "find currently requires video.extract_frames as the first transform"
        ):
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[TransformSpec(type="video.transcode", params={"codec": "h264", "crf": 23})],
            )

    def test_find_allows_image_transforms_after_video_extract_frames(self) -> None:
        request = CreateJobRequest(
            source="s3://raw-dashcam-archive/",
            sink="s3://training-dataset/",
            find="match",
            transforms=[
                TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"}),
                TransformSpec(
                    type="image.resize",
                    params={"width": 128, "height": 128, "maintain_aspect": False},
                ),
            ],
        )
        self.assertEqual(request.transforms[0].type, "video.extract_frames")
        self.assertEqual(request.transforms[1].type, "image.resize")

    def test_find_rejects_video_transforms_after_video_extract_frames(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "find currently supports only image transforms after video.extract_frames",
        ):
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[
                    TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"}),
                    TransformSpec(type="video.transcode", params={"codec": "h264", "crf": 23}),
                ],
            )

    def test_find_rejects_video_transform_before_video_extract_frames(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "find currently requires video.extract_frames as the first transform"
        ):
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[
                    TransformSpec(
                        type="video.resize",
                        params={"width": 1920, "height": 1080, "maintain_aspect": True},
                    ),
                    TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"}),
                ],
            )

    def test_find_job_auto_queues_after_plan_when_enabled(self) -> None:
        os.environ["MX8_FIND_AUTO_QUEUE_AFTER_PLAN"] = "true"
        store = InMemoryJobStore()
        record = store.create_job(
            CreateJobRequest(
                source="s3://raw-dashcam-archive/",
                sink="s3://training-dataset/",
                find="match",
                transforms=[TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"})],
            )
        )
        wake_calls: list[str] = []
        dispatcher = FindDispatcher(
            transport=StaticTransport({0: [MatchSegment(sample_id=0, start_ms=500, end_ms=1_500)]}),
            access_resolver=PassthroughSourceAccessResolver(),
            duration_resolver=FakeDurationResolver({"s3://bucket/input-0.mp4": 60_000}),
        )
        dispatcher.start()
        try:
            finder = JobFinder(
                store,
                dispatcher=dispatcher,
                manifest_resolver=FakeResolver(
                    "basehash",
                    [
                        ManifestRecord(
                            sample_id=0,
                            location="s3://bucket/input-0.mp4",
                            byte_offset=None,
                            byte_length=None,
                            decode_hint="mx8:video;codec=h264",
                        )
                    ],
                ),
                wake_scaler=lambda: wake_calls.append("wake"),
            )

            self._reconcile_until_terminal(finder, store, record.id)
        finally:
            dispatcher.stop()

        updated = store.get_job(record.id)
        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.status, JobStatus.PENDING)
        self.assertEqual(updated.matched_assets, 1)
        self.assertEqual(updated.matched_segments, 1)
        self.assertEqual(wake_calls, ["wake"])

    def test_finder_initializes_lazily_when_manifest_store_is_not_local(self) -> None:
        prev_manifest_root = os.environ.get("MX8_MANIFEST_STORE_ROOT")
        os.environ["MX8_MANIFEST_STORE_ROOT"] = "s3://planner-manifests"
        try:
            finder = JobFinder(InMemoryJobStore())
            finder.reconcile_once()
        finally:
            if prev_manifest_root is None:
                os.environ.pop("MX8_MANIFEST_STORE_ROOT", None)
            else:
                os.environ["MX8_MANIFEST_STORE_ROOT"] = prev_manifest_root

    def _reconcile_until_terminal(self, finder: JobFinder, store: InMemoryJobStore, job_id: str) -> None:
        for _ in range(50):
            finder.reconcile_once()
            record = store.get_job(job_id)
            if record is not None and record.status != JobStatus.FINDING:
                return
            time.sleep(0.01)
        self.fail("job did not leave FINDING")


if __name__ == "__main__":
    unittest.main()
