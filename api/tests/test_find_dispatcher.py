from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.find_contracts import (
    FIND_INTERACTIVE_LANE,
    FindShard,
    ManifestRecord,
)
from api.find_access import PassthroughSourceAccessResolver
from api.find_dispatcher import FindDispatcher, FindShardQueue, MockFindTransport, find_shard_window_ms
from api.find_dispatcher import build_find_shards, shard_windows_for_duration


class DispatcherTests(unittest.TestCase):
    def test_find_shard_window_default_is_120_seconds(self) -> None:
        self.assertEqual(find_shard_window_ms(), 120_000)

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

    def test_dispatcher_stops_early_when_max_outputs_is_satisfied(self) -> None:
        dispatcher = FindDispatcher(
            transport=MockFindTransport(),
            access_resolver=PassthroughSourceAccessResolver(),
        )
        dispatcher.start()
        try:
            source_records = [
                ManifestRecord(
                    sample_id=index,
                    location=f"s3://bucket/input-{index}.mp4",
                    byte_offset=None,
                    byte_length=None,
                    decode_hint="mx8:video;codec=h264;duration_ms=1000",
                )
                for index in range(3)
            ]
            submitted = dispatcher.submit(
                job_id="job-max",
                customer_id="cust-1",
                lane=FIND_INTERACTIVE_LANE,
                priority=100,
                query_text="input",
                source_manifest_hash="basehash",
                source_records=source_records,
                video_records=source_records,
                max_outputs=1,
            )
            self.assertTrue(submitted)

            snapshot = None
            for _ in range(50):
                snapshot = dispatcher.pop_terminal_snapshot("job-max")
                if snapshot is not None:
                    break
                time.sleep(0.01)

            self.assertIsNotNone(snapshot)
            assert snapshot is not None
            self.assertEqual(snapshot.status, "complete")
            self.assertLess(snapshot.completed_shards, snapshot.total_shards)
            self.assertGreaterEqual(len(snapshot.segments), 1)
        finally:
            dispatcher.stop()

    def test_build_find_shards_fans_out_long_unsegmented_video(self) -> None:
        shards = build_find_shards(
            job_id="job-1",
            customer_id="cust-1",
            lane=FIND_INTERACTIVE_LANE,
            priority=100,
            query_id="qry-1",
            query_text="match",
            records=[
                ManifestRecord(
                    sample_id=7,
                    location="s3://bucket/long.mp4",
                    byte_offset=None,
                    byte_length=None,
                    decode_hint="mx8:video;codec=h264;duration_ms=300000",
                )
            ],
            access_resolver=PassthroughSourceAccessResolver(),
        )

        self.assertEqual(len(shards), 3)
        self.assertEqual(
            [(shard.scan_start_ms, shard.scan_end_ms) for shard in shards],
            [(0, 120_000), (119_000, 239_000), (238_000, 300_000)],
        )

    def test_dispatcher_can_restart_after_stop(self) -> None:
        dispatcher = FindDispatcher(
            transport=MockFindTransport(),
            access_resolver=PassthroughSourceAccessResolver(),
        )
        dispatcher.start()
        dispatcher.stop()
        dispatcher.start()
        try:
            submitted = dispatcher.submit(
                job_id="job-restart",
                customer_id="cust-1",
                lane=FIND_INTERACTIVE_LANE,
                priority=100,
                query_text="input-1",
                source_manifest_hash="basehash",
                source_records=[
                    ManifestRecord(
                        sample_id=1,
                        location="s3://bucket/input-1.mp4",
                        byte_offset=None,
                        byte_length=None,
                        decode_hint="mx8:video;codec=h264;duration_ms=1000",
                    )
                ],
                video_records=[
                    ManifestRecord(
                        sample_id=1,
                        location="s3://bucket/input-1.mp4",
                        byte_offset=None,
                        byte_length=None,
                        decode_hint="mx8:video;codec=h264;duration_ms=1000",
                    )
                ],
            )
            self.assertTrue(submitted)
        finally:
            dispatcher.stop()

    def test_shard_windows_for_duration_uses_overlap_step(self) -> None:
        self.assertEqual(
            shard_windows_for_duration(duration_ms=300_000, window_ms=120_000, overlap_ms=1_000),
            [(0, 120_000), (119_000, 239_000), (238_000, 300_000)],
        )


if __name__ == "__main__":
    unittest.main()
