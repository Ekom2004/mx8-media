from __future__ import annotations

import sys
import time
import unittest
from email.message import Message
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.find_contracts import (
    FIND_INTERACTIVE_LANE,
    FindShard,
    ManifestRecord,
)
from api.find_access import PassthroughSourceAccessResolver
from api.find_dispatcher import (
    FindDispatcher,
    FindShardQueue,
    MockFindTransport,
    build_find_shards,
    validate_remote_probe_response,
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


class DispatcherTests(unittest.TestCase):
    def test_find_shard_queue_respects_base_job_cap(self) -> None:
        queue = FindShardQueue(
            worker_slots=2,
            base_active_shards_per_job=1,
            base_active_shards_per_customer=4,
        )
        queue.enqueue(
            [
                FindShard(
                    shard_id="shd-1",
                    job_id="job-1",
                    customer_id="cust-1",
                    lane=FIND_INTERACTIVE_LANE,
                    priority=100,
                    attempt=0,
                    query_id="qry-1",
                    query_text="match",
                    source_uri="s3://bucket/input-0.mp4",
                    asset_id="input-0.mp4",
                    decode_hint="mx8:video;codec=h264",
                    sample_id=0,
                    scan_start_ms=0,
                    scan_end_ms=1_000,
                    overlap_ms=0,
                    sample_fps=1.0,
                    model="siglip2_base",
                    created_at_ms=1,
                ),
                FindShard(
                    shard_id="shd-2",
                    job_id="job-1",
                    customer_id="cust-1",
                    lane=FIND_INTERACTIVE_LANE,
                    priority=100,
                    attempt=0,
                    query_id="qry-1",
                    query_text="match",
                    source_uri="s3://bucket/input-1.mp4",
                    asset_id="input-1.mp4",
                    decode_hint="mx8:video;codec=h264",
                    sample_id=1,
                    scan_start_ms=0,
                    scan_end_ms=1_000,
                    overlap_ms=0,
                    sample_fps=1.0,
                    model="siglip2_base",
                    created_at_ms=1,
                ),
                FindShard(
                    shard_id="shd-3",
                    job_id="job-2",
                    customer_id="cust-1",
                    lane=FIND_INTERACTIVE_LANE,
                    priority=100,
                    attempt=0,
                    query_id="qry-2",
                    query_text="match",
                    source_uri="s3://bucket/input-2.mp4",
                    asset_id="input-2.mp4",
                    decode_hint="mx8:video;codec=h264",
                    sample_id=2,
                    scan_start_ms=0,
                    scan_end_ms=1_000,
                    overlap_ms=0,
                    sample_fps=1.0,
                    model="siglip2_base",
                    created_at_ms=1,
                ),
            ]
        )

        first = queue.lease(timeout_secs=0.1)
        second = queue.lease(timeout_secs=0.1)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None and second is not None
        self.assertEqual({first.job_id, second.job_id}, {"job-1", "job-2"})

        queue.complete(first)
        queue.complete(second)
        queue.close()

    def test_dispatcher_processes_job_to_completion(self) -> None:
        dispatcher = FindDispatcher(
            transport=MockFindTransport(),
            access_resolver=PassthroughSourceAccessResolver(),
            duration_resolver=FakeDurationResolver(
                {
                    "s3://bucket/input-0.mp4": 1_000,
                    "s3://bucket/input-1.mp4": 1_000,
                }
            ),
        )
        dispatcher.start()
        try:
            source_records = [
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
            ]
            submitted = dispatcher.submit(
                job_id="job-1",
                customer_id="cust-1",
                lane=FIND_INTERACTIVE_LANE,
                priority=100,
                query_text="input-1",
                source_manifest_hash="basehash",
                source_records=source_records,
                video_records=source_records,
            )
            self.assertTrue(submitted)

            snapshot = None
            for _ in range(50):
                snapshot = dispatcher.pop_terminal_snapshot("job-1")
                if snapshot is not None:
                    break
                time.sleep(0.01)

            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.status, "complete")
            self.assertEqual(snapshot.source_manifest_hash, "basehash")
            self.assertEqual(len(snapshot.segments), 1)
            self.assertEqual(snapshot.segments[0].sample_id, 1)
        finally:
            dispatcher.stop()

    def test_build_find_shards_fans_out_long_video_duration(self) -> None:
        shards = build_find_shards(
            job_id="job-long",
            customer_id="cust-1",
            lane=FIND_INTERACTIVE_LANE,
            priority=100,
            query_id="qry-long",
            query_text="snowy stop sign",
            records=[
                ManifestRecord(
                    sample_id=0,
                    location="s3://bucket/long.mp4",
                    byte_offset=None,
                    byte_length=None,
                    decode_hint="mx8:video;codec=h264",
                )
            ],
            access_resolver=PassthroughSourceAccessResolver(),
            duration_resolver=FakeDurationResolver({"s3://bucket/long.mp4": 150_000}),
        )

        self.assertEqual(
            [(shard.scan_start_ms, shard.scan_end_ms) for shard in shards],
            [(0, 60_000), (59_000, 119_000), (118_000, 150_000)],
        )

    def test_remote_probe_response_rejects_non_range_media(self) -> None:
        headers = Message()
        headers["Content-Type"] = "video/mp4"

        with self.assertRaisesRegex(RuntimeError, "byte-range reads"):
            validate_remote_probe_response(
                status=200,
                headers=headers,
                source_ref="https://example.com/video.mp4",
            )

    def test_remote_probe_response_accepts_partial_content_media(self) -> None:
        headers = Message()
        headers["Content-Type"] = "video/mp4"
        headers["Content-Range"] = "bytes 0-0/2048"

        validate_remote_probe_response(
            status=206,
            headers=headers,
            source_ref="https://example.com/video.mp4",
        )


if __name__ == "__main__":
    unittest.main()
