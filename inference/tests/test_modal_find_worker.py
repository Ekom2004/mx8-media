from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from inference.modal_find_worker import _merge_frame_hits, _resolve_model_id


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


if __name__ == "__main__":
    unittest.main()
