from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.models import (
    JobFailureCategory,
    JobProgressUpdate,
    JobRecord,
    JobStage,
    JobStatus,
    JobView,
    SubmitJobRequest,
    TransformSpec,
)
from api.storage import InMemoryJobStore


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

    def test_proxy_alias_normalizes_to_video_transcode(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://review-proxies/",
                "work": [
                    {"type": "proxy", "params": {}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.transforms[0].type, "video.transcode")
        self.assertEqual(internal.transforms[0].params["codec"], "h264")
        self.assertEqual(internal.transforms[0].params["crf"], 28)
        self.assertEqual(internal.transforms[0].params["preset"], "veryfast")

    def test_develop_raw_alias_normalizes_to_image_develop_raw(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://camera-raw/",
                "output": "s3://standard-images/",
                "work": [
                    {"type": "develop_raw", "params": {}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.transforms[0].type, "image.develop_raw")
        self.assertEqual(internal.transforms[0].params, {})

    def test_remove_background_alias_normalizes_to_image_remove_background(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://product-images/",
                "output": "s3://cutouts/",
                "work": [
                    {"type": "remove_background", "params": {}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.transforms[0].type, "image.remove_background")
        self.assertEqual(internal.transforms[0].params, {})

    def test_image_develop_raw_rejects_params(self) -> None:
        with self.assertRaisesRegex(ValueError, "image.develop_raw does not accept params"):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://camera-raw/",
                    "output": "s3://standard-images/",
                    "work": [
                        {"type": "image.develop_raw", "params": {"format": "jpg"}},
                    ],
                }
            )

    def test_image_remove_background_rejects_params(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "image.remove_background does not accept params"
        ):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://product-images/",
                    "output": "s3://cutouts/",
                    "work": [
                        {"type": "image.remove_background", "params": {"quality": 90}},
                    ],
                }
            )

    def test_video_transcode_accepts_preset(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://training-dataset/",
                "work": [
                    {
                        "type": "video.transcode",
                        "params": {"codec": "H264", "crf": 23, "preset": "VeryFast"},
                    }
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.transforms[0].type, "video.transcode")
        self.assertEqual(internal.transforms[0].params["codec"], "h264")
        self.assertEqual(internal.transforms[0].params["crf"], 23)
        self.assertEqual(internal.transforms[0].params["preset"], "veryfast")

    def test_remux_alias_normalizes_to_video_remux(self) -> None:
        payload = SubmitJobRequest.model_validate(
            {
                "input": "s3://raw-dashcam-archive/",
                "output": "s3://rewrapped-media/",
                "work": [
                    {"type": "remux", "params": {}},
                ],
            }
        )

        internal = payload.to_internal()

        self.assertEqual(internal.transforms[0].type, "video.remux")
        self.assertEqual(internal.transforms[0].params["container"], "mp4")

    def test_video_remux_rejects_unsupported_container(self) -> None:
        with self.assertRaisesRegex(ValueError, "video.remux container must be one of mp4"):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-dashcam-archive/",
                    "output": "s3://rewrapped-media/",
                    "work": [
                        {"type": "video.remux", "params": {"container": "mkv"}},
                    ],
                }
            )

    def test_video_transcode_rejects_preset_for_av1(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "video.transcode preset is only supported for h264 and h265"
        ):
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-dashcam-archive/",
                    "output": "s3://training-dataset/",
                    "work": [
                        {
                            "type": "video.transcode",
                            "params": {"codec": "av1", "crf": 23, "preset": "veryfast"},
                        }
                    ],
                }
            )

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
            stage=JobStage.PLANNING,
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
        self.assertEqual(view.stage, JobStage.PLANNING)
        self.assertEqual(view.work[0].type, "find")
        self.assertEqual(view.work[0].params["query"], "a stop sign")
        self.assertEqual(view.work[1].type, "video.extract_frames")


class JobStoreTelemetryTests(unittest.TestCase):
    def test_job_store_tracks_stage_failure_and_events(self) -> None:
        store = InMemoryJobStore()
        record = store.create_job(
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://catalog-images/",
                    "output": "s3://catalog-images-web/",
                    "work": [
                        {"type": "resize", "params": {"width": 512, "height": 512, "media": "image"}}
                    ],
                }
            ).to_internal()
        )

        self.assertEqual(record.stage, JobStage.SUBMITTED)
        self.assertEqual(record.events[0].type, "job_submitted")

        updated = store.update_job_progress(
            JobProgressUpdate(
                job_id=record.id,
                status=JobStatus.RUNNING,
                current_workers=2,
                desired_workers=2,
                worker_pool="image-default",
            )
        )

        self.assertIsNotNone(updated)
        assert updated is not None
        self.assertEqual(updated.stage, JobStage.RUNNING)
        self.assertEqual(updated.events[-1].type, "job_running")
        self.assertEqual(updated.worker_pool, "image-default")

        failed = store.update_job_progress(
            JobProgressUpdate(
                job_id=record.id,
                status=JobStatus.FAILED,
                failure_category=JobFailureCategory.WORKER_ERROR,
                failure_message="worker crashed during image resize",
            )
        )

        self.assertIsNotNone(failed)
        assert failed is not None
        self.assertEqual(failed.stage, JobStage.FAILED)
        self.assertEqual(failed.failure_category, JobFailureCategory.WORKER_ERROR)
        self.assertEqual(failed.failure_message, "worker crashed during image resize")
        self.assertEqual(failed.events[-1].type, "job_failed")

    def test_job_store_filters_jobs_by_account_id(self) -> None:
        store = InMemoryJobStore()
        first = store.create_job(
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://catalog-images/",
                    "output": "s3://catalog-images-web/",
                    "work": [
                        {"type": "resize", "params": {"width": 512, "height": 512, "media": "image"}}
                    ],
                }
            ).to_internal().model_copy(update={"account_id": "acct-a"})
        )
        second = store.create_job(
            SubmitJobRequest.model_validate(
                {
                    "input": "s3://raw-video/",
                    "output": "s3://review-proxies/",
                    "work": [{"type": "proxy", "params": {}}],
                }
            ).to_internal().model_copy(update={"account_id": "acct-b"})
        )

        scoped = store.list_jobs(account_id="acct-a")
        self.assertEqual([job.id for job in scoped], [first.id])
        self.assertIsNotNone(store.get_job(first.id, account_id="acct-a"))
        self.assertIsNone(store.get_job(second.id, account_id="acct-a"))


if __name__ == "__main__":
    unittest.main()
