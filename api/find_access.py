from __future__ import annotations

import os
from typing import Protocol
from urllib.parse import urlsplit


class SourceAccessResolver(Protocol):
    def resolve(self, location: str) -> str | None: ...


class PassthroughSourceAccessResolver:
    def resolve(self, location: str) -> str | None:
        return location.strip() or None


class S3PresignedUrlResolver:
    def __init__(self, client, *, expires_secs: int) -> None:
        self._client = client
        self._expires_secs = max(60, expires_secs)

    def resolve(self, location: str) -> str | None:
        normalized = location.strip()
        if not normalized:
            return None
        parsed = urlsplit(normalized)
        if parsed.scheme in {"http", "https"}:
            return normalized
        if parsed.scheme != "s3":
            raise RuntimeError(f"unsupported source URI for modal find worker: {normalized}")
        bucket = parsed.netloc.strip()
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise RuntimeError(f"invalid s3 source URI for modal find worker: {normalized}")
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=self._expires_secs,
        )


def build_source_access_resolver(provider: str | None = None) -> SourceAccessResolver:
    normalized_provider = (provider or os.getenv("MX8_FIND_PROVIDER", "modal")).strip().lower()
    if normalized_provider == "mock":
        return PassthroughSourceAccessResolver()
    if normalized_provider != "modal":
        raise RuntimeError(f"unsupported find provider: {normalized_provider}")
    import boto3  # type: ignore

    region = os.getenv("MX8_FIND_S3_REGION", "").strip() or os.getenv("AWS_REGION", "").strip() or None
    client = boto3.client("s3", region_name=region)
    expires_secs = max(60, int(os.getenv("MX8_FIND_S3_PRESIGN_EXPIRES_SECS", "3600")))
    return S3PresignedUrlResolver(client, expires_secs=expires_secs)
