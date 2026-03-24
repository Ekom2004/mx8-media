from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from api.find_access import PassthroughSourceAccessResolver, S3PresignedUrlResolver


class FakeS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], int]] = []

    def generate_presigned_url(self, operation: str, *, Params: dict[str, str], ExpiresIn: int) -> str:
        self.calls.append((operation, Params, ExpiresIn))
        return f"https://signed.example/{Params['Bucket']}/{Params['Key']}?expires={ExpiresIn}"


class FindAccessTests(unittest.TestCase):
    def test_passthrough_resolver_returns_location(self) -> None:
        resolver = PassthroughSourceAccessResolver()
        self.assertEqual(resolver.resolve("s3://bucket/key.mp4"), "s3://bucket/key.mp4")

    def test_s3_presigned_resolver_signs_s3_locations(self) -> None:
        client = FakeS3Client()
        resolver = S3PresignedUrlResolver(client, expires_secs=600)

        signed = resolver.resolve("s3://raw-dashcam-archive/drive-0001.mp4")

        self.assertEqual(
            signed,
            "https://signed.example/raw-dashcam-archive/drive-0001.mp4?expires=600",
        )
        self.assertEqual(
            client.calls,
            [
                (
                    "get_object",
                    {"Bucket": "raw-dashcam-archive", "Key": "drive-0001.mp4"},
                    600,
                )
            ],
        )

    def test_s3_presigned_resolver_passes_through_https(self) -> None:
        client = FakeS3Client()
        resolver = S3PresignedUrlResolver(client, expires_secs=600)

        url = resolver.resolve("https://cdn.example/video.mp4")

        self.assertEqual(url, "https://cdn.example/video.mp4")
        self.assertEqual(client.calls, [])

    def test_s3_presigned_resolver_rejects_unsupported_scheme(self) -> None:
        client = FakeS3Client()
        resolver = S3PresignedUrlResolver(client, expires_secs=600)

        with self.assertRaisesRegex(RuntimeError, "unsupported source URI"):
            resolver.resolve("/tmp/local.mp4")


if __name__ == "__main__":
    unittest.main()
