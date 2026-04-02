from __future__ import annotations

import re
import sys
import unittest
from pathlib import Path

if sys.version_info < (3, 10):
    raise unittest.SkipTest("sdk CLI tests require Python 3.10+")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk"))

from mx8.cli import render_job_detail, render_job_table


class CliRenderTests(unittest.TestCase):
    def test_render_job_detail_includes_failure_and_events(self) -> None:
        rendered = render_job_detail(
            {
                "id": "job_123456",
                "status": "FAILED",
                "stage": "FAILED",
                "input": "s3://catalog-images/",
                "output": "s3://catalog-images-web/",
                "work": [
                    {"type": "image.resize", "params": {"width": 1024, "height": 1024}},
                    {"type": "image.convert", "params": {"format": "webp", "quality": 85}},
                ],
                "outputs_written": 8400,
                "completed_objects": 8400,
                "completed_bytes": 2123456789,
                "current_workers": 4,
                "desired_workers": 4,
                "worker_pool": "image-default",
                "region": "us-east-1",
                "instance_type": "c7i.xlarge",
                "failure_category": "worker_error",
                "failure_message": "worker crashed during image resize",
                "events": [
                    {
                        "at": "2026-03-29T14:01:12.345678Z",
                        "type": "job_submitted",
                        "message": "Job submitted",
                    },
                    {
                        "at": "2026-03-29T14:07:44.000000Z",
                        "type": "job_failed",
                        "message": "worker crashed during image resize",
                    },
                ],
            }
        )
        plain = _strip_ansi(rendered)

        self.assertIn("Status: FAILED", plain)
        self.assertIn("Stage: FAILED", plain)
        self.assertIn("pool: image-default", plain)
        self.assertIn("category: worker_error", plain)
        self.assertIn("job_failed", plain)

    def test_render_job_table_filters_terminal_jobs_by_default(self) -> None:
        rendered = render_job_table(
            [
                {
                    "id": "job_running",
                    "status": "RUNNING",
                    "stage": "RUNNING",
                    "worker_pool": "image-default",
                    "outputs_written": 25,
                    "current_workers": 2,
                    "desired_workers": 2,
                    "events": [{"type": "job_running"}],
                    "updated_at": "2026-03-29T14:00:00Z",
                },
                {
                    "id": "job_done",
                    "status": "COMPLETE",
                    "stage": "COMPLETE",
                    "worker_pool": "image-default",
                    "outputs_written": 100,
                    "current_workers": 0,
                    "desired_workers": 0,
                    "events": [{"type": "job_completed"}],
                    "updated_at": "2026-03-29T14:05:00Z",
                },
            ]
        )
        plain = _strip_ansi(rendered)

        self.assertIn("job_runn", plain)
        self.assertNotIn("job_done", plain)


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


if __name__ == "__main__":
    unittest.main()
