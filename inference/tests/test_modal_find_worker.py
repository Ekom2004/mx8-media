from __future__ import annotations

import os
import sys
import unittest
from contextlib import nullcontext
from email.message import Message
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

import torch
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from inference.modal_find_worker import (
    _match_threshold,
    _merge_frame_hits,
    _resolve_model_id,
    _score_frames,
    _validate_remote_source_response,
    FindShard,
    find_shard_from_payload,
)


class ModalFindWorkerTests(unittest.TestCase):
    def test_match_threshold_uses_margin_above_floor(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "MX8_FIND_SCORE_THRESHOLD": "-8.0",
                "MX8_FIND_SCORE_MARGIN": "2.0",
            },
            clear=False,
        ):
            self.assertEqual(_match_threshold([-12.0, -2.5, -4.0]), -4.5)
            self.assertEqual(_match_threshold([-15.0, -11.0]), -8.0)

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

    def test_score_frames_uses_joint_logits(self) -> None:
        class FakeProcessor:
            def __call__(self, *, text, images, return_tensors, padding, truncation):
                self.last_call = {
                    "text": text,
                    "images": images,
                    "return_tensors": return_tensors,
                    "padding": padding,
                    "truncation": truncation,
                }
                return {"pixel_values": torch.tensor([1.0])}

        class FakeModel:
            def __call__(self, **inputs):
                return SimpleNamespace(logits_per_image=torch.tensor([[1.25], [2.5]]))

        processor = FakeProcessor()
        runtime = SimpleNamespace(
            processor=processor,
            model=FakeModel(),
            device="cpu",
            inference_mode=nullcontext,
        )
        shard = FindShard(
            shard_id="shd-joint",
            job_id="job-joint",
            customer_id="cust-1",
            lane="interactive",
            priority=100,
            attempt=0,
            query_id="qry-joint",
            query_text="a person",
            source_uri="https://example.com/video.mp4",
            asset_id="video.mp4",
            decode_hint=None,
            sample_id=0,
            scan_start_ms=0,
            scan_end_ms=2_000,
            overlap_ms=0,
            sample_fps=1.0,
            model="siglip2_base",
            created_at_ms=0,
        )

        with TemporaryDirectory(prefix="mx8-worker-test-") as tempdir:
            frame_dir = Path(tempdir)
            frame_paths = []
            for index, color in enumerate(((255, 0, 0), (0, 255, 0))):
                frame_path = frame_dir / f"frame-{index}.jpg"
                Image.new("RGB", (2, 2), color).save(frame_path)
                frame_paths.append(frame_path)

            with mock.patch("inference.modal_find_worker._runtime_for_model", return_value=runtime):
                scores, _ = _score_frames(shard=shard, frame_paths=frame_paths, source_ref=shard.source_uri)

        self.assertEqual(scores, [1.25, 2.5])
        self.assertEqual(processor.last_call["text"], ["a person"])
        self.assertEqual(processor.last_call["return_tensors"], "pt")
        self.assertEqual(processor.last_call["padding"], "max_length")
        self.assertTrue(processor.last_call["truncation"])


if __name__ == "__main__":
    unittest.main()
