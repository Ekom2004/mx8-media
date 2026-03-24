from __future__ import annotations

import sys
import unittest
from email.message import Message
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from inference.modal_find_worker import (
    _merge_frame_hits,
    _resolve_model_id,
    _validate_remote_source_response,
    find_shard_from_payload,
)


class ModalFindWorkerTests(unittest.TestCase):
    def test_merge_frame_hits_coalesces_neighboring_matches(self) -> None:
        hits = _merge_frame_hits(
            sample_id=42,
            scan_start_ms=0,
            scan_end_ms=10_000,
            sample_fps=1.0,
            frame_times_ms=[1_000, 2_000, 8_000],
            scores=[0.4, 0.6, 0.7],
        )

        self.assertEqual(len(hits), 2)
        self.assertEqual((hits[0].sample_id, hits[0].start_ms, hits[0].end_ms), (42, 500, 4_500))
        self.assertEqual((hits[1].sample_id, hits[1].start_ms, hits[1].end_ms), (42, 7_500, 10_000))

    def test_resolve_model_id_supports_siglip2_alias(self) -> None:
        self.assertEqual(_resolve_model_id("siglip2_base"), "google/siglip2-base-patch16-224")

    def test_find_shard_from_payload_keeps_optional_source_access_url(self) -> None:
        shard = find_shard_from_payload(
            {
                "shard_id": "shd_01",
                "job_id": "job_01",
                "customer_id": "cust_01",
                "lane": "interactive",
                "priority": 100,
                "attempt": 0,
                "query_id": "qry_01",
                "query_text": "snowy stop sign",
                "source_uri": "https://example.com/video.mp4",
                "asset_id": "video.mp4",
                "decode_hint": None,
                "source_access_url": "https://signed.example/video.mp4?token=abc",
                "sample_id": 0,
                "scan_start_ms": 0,
                "scan_end_ms": 60_000,
                "overlap_ms": 1_000,
                "sample_fps": 1.0,
                "model": "siglip2_base",
                "created_at_ms": 1_710_000_000_000,
            }
        )

        self.assertEqual(shard.source_access_url, "https://signed.example/video.mp4?token=abc")

    def test_remote_source_preflight_rejects_non_range_http_media(self) -> None:
        shard = find_shard_from_payload(
            {
                "shard_id": "shd_02",
                "job_id": "job_02",
                "customer_id": "cust_01",
                "lane": "interactive",
                "priority": 100,
                "attempt": 0,
                "query_id": "qry_02",
                "query_text": "snowy stop sign",
                "source_uri": "https://example.com/video.mp4",
                "asset_id": "video.mp4",
                "decode_hint": None,
                "source_access_url": "https://example.com/video.mp4",
                "sample_id": 0,
                "scan_start_ms": 0,
                "scan_end_ms": 60_000,
                "overlap_ms": 1_000,
                "sample_fps": 1.0,
                "model": "siglip2_base",
                "created_at_ms": 1_710_000_000_000,
            }
        )
        headers = Message()
        headers["Content-Type"] = "video/mp4"

        with self.assertRaisesRegex(RuntimeError, "byte-range reads"):
            _validate_remote_source_response(
                status=200,
                headers=headers,
                source_ref="https://example.com/video.mp4",
                shard=shard,
            )

    def test_remote_source_preflight_accepts_partial_content_video(self) -> None:
        shard = find_shard_from_payload(
            {
                "shard_id": "shd_03",
                "job_id": "job_03",
                "customer_id": "cust_01",
                "lane": "interactive",
                "priority": 100,
                "attempt": 0,
                "query_id": "qry_03",
                "query_text": "snowy stop sign",
                "source_uri": "https://example.com/video.mp4",
                "asset_id": "video.mp4",
                "decode_hint": None,
                "source_access_url": "https://example.com/video.mp4",
                "sample_id": 0,
                "scan_start_ms": 0,
                "scan_end_ms": 60_000,
                "overlap_ms": 1_000,
                "sample_fps": 1.0,
                "model": "siglip2_base",
                "created_at_ms": 1_710_000_000_000,
            }
        )
        headers = Message()
        headers["Content-Type"] = "video/mp4"
        headers["Content-Range"] = "bytes 0-0/2048"

        _validate_remote_source_response(
            status=206,
            headers=headers,
            source_ref="https://example.com/video.mp4",
            shard=shard,
        )


if __name__ == "__main__":
    unittest.main()
