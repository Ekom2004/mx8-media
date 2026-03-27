from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.models import JobRecord, JobStatus, JobView, SubmitJobRequest, TransformSpec


class SubmitJobRequestTests(unittest.TestCase):
    def test_canonical_input_work_output_normalizes_to_internal_request(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://training-dataset/",
                "max": 3,
                "work": [
                    {"type": "find", "params": {"query": "a stop sign"}},
                    {"type": "video.extract_frames", "params": {"fps": 1, "format": "jpeg"}},
                    {"type": "image.resize", "params": {"width": 128, "height": 128}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.source, "s3://raw-dashcam-archive/")
        self.assertEqual(internal.sink, "s3://training-dataset/")
        self.assertEqual(internal.find, "a stop sign")
        self.assertEqual(internal.max_outputs, 3)
        self.assertEqual(internal.transforms[0].type, "video.extract_frames")
        self.assertEqual(internal.transforms[0].params["format"], "jpg")
        self.assertEqual(internal.transforms[1].type, "image.resize")

    def test_legacy_shape_is_still_accepted(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "source": "s3://raw-dashcam-archive/",
                "sink": "s3://training-dataset/",
                "find": "a stop sign",
                "transforms": [
                    {"type": "video.extract_frames", "params": {"fps": 1, "format": "jpg"}}
                ],
            }
        )

        self.assertEqual(payload.input, "s3://raw-dashcam-archive/")
        self.assertEqual(payload.output, "s3://training-dataset/")
        self.assertEqual(payload.work[0].type, "find")
        self.assertEqual(payload.work[1].type, "video.extract_frames")

    def test_find_must_be_first_work_item(self) -> None:
        with self.assertRaisesRegex(ValueError, "find must be the first work item"):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-dashcam-archive/",
                    "output": "s3://training-dataset/",
                    "work": [
                        {"type": "video.extract_frames", "params": {"fps": 1, "format": "jpg"}},
                        {"type": "find", "params": {"query": "a stop sign"}},
                    ],
                }
            )

    def test_clip_alias_normalizes_to_video_transcode(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://training-dataset/",
                "work": [
                    {"type": "find", "params": {"query": "a stop sign"}},
                    {"type": "clip", "params": {}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.find, "a stop sign")
        self.assertEqual(internal.transforms[0].type, "video.transcode")
        self.assertEqual(internal.transforms[0].params["codec"], "h264")
        self.assertEqual(internal.transforms[0].params["crf"], 23)

    def test_find_only_job_is_allowed(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://search-results/",
                "work": [{"type": "find", "params": {"query": "a stop sign"}}],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.find, "a stop sign")
        self.assertEqual(internal.transforms, [])

    def test_max_must_be_positive(self) -> None:
        with self.assertRaisesRegex(ValueError, "max must be > 0"):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-dashcam-archive/",
                    "output": "s3://training-dataset/",
                    "max": 0,
                    "work": [
                        {"type": "video.extract_frames", "params": {"fps": 1, "format": "jpg"}}
                    ],
                }
            )

    def test_mixed_canonical_and_legacy_fields_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "use either canonical input/work/output fields"):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-dashcam-archive/",
                    "sink": "s3://training-dataset/",
                    "work": [
                        {"type": "video.extract_frames", "params": {"fps": 1, "format": "jpg"}}
                    ],
                }
            )


class JobViewTests(unittest.TestCase):
    def test_job_view_serializes_canonical_fields(self) -> None:
        record = JobRecord(
            id="job-123",
            status=JobStatus.FINDING,
            source="s3://raw-dashcam-archive/",
            sink="s3://training-dataset/",
            find="a stop sign",
            transforms=[
                TransformSpec(type="video.extract_frames", params={"fps": 1, "format": "jpg"})
            ],
            max_outputs=5,
            matched_assets=3,
            matched_segments=5,
        )

        view = JobView.from_record(record)

        self.assertEqual(view.input, "s3://raw-dashcam-archive/")
        self.assertEqual(view.output, "s3://training-dataset/")
        self.assertEqual(view.max, 5)
        self.assertEqual(view.work[0].type, "find")
        self.assertEqual(view.work[0].params["query"], "a stop sign")
        self.assertEqual(view.work[1].type, "video.extract_frames")


if __name__ == "__main__":
    unittest.main()
