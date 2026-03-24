from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from .job import Job
from .transforms import Transform, TransformChain


class MX8APIError(RuntimeError):
    pass


@dataclass(slots=True)
class MX8Client:
    base_url: str
    api_key: str | None = None
    timeout_secs: float = 30.0

    def submit_job(
        self,
        *,
        source: str,
        transform: Transform | Sequence[Transform] | TransformChain,
        sink: str,
        find: str | None = None,
    ) -> Job:
        payload = {
            "source": source,
            "sink": sink,
            "transforms": _normalize_transforms(transform),
        }
        if find is not None:
            payload["find"] = find
        body = self._request("POST", "/v1/jobs", payload)
        return _job_from_payload(self, body)

    def get_job(self, job_id: str) -> Job:
        body = self._request("GET", f"/v1/jobs/{job_id}")
        return _job_from_payload(self, body)

    def list_jobs(self) -> list[Job]:
        body = self._request("GET", "/v1/jobs")
        return [_job_from_payload(self, item) for item in body]

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url.rstrip('/')}{path}"
        headers = {
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        data = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(payload).encode("utf-8")
        req = request.Request(url, data=data, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=self.timeout_secs) as resp:
                raw = resp.read()
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise MX8APIError(f"{method} {path} failed: {exc.code} {detail}") from exc
        except error.URLError as exc:
            raise MX8APIError(f"{method} {path} failed: {exc.reason}") from exc

        if not raw:
            return None
        return json.loads(raw.decode("utf-8"))


def default_client() -> MX8Client:
    return MX8Client(
        base_url=os.environ.get("MX8_API_BASE_URL", "http://127.0.0.1:8000"),
        api_key=os.environ.get("MX8_API_KEY"),
    )


def _normalize_transforms(
    transform: Transform | Sequence[Transform] | TransformChain,
) -> list[dict[str, Any]]:
    if isinstance(transform, Transform):
        return [transform.to_payload()]
    if isinstance(transform, TransformChain):
        payloads = transform.to_payloads()
    else:
        payloads = [item.to_payload() for item in transform]
    if not payloads:
        raise ValueError("transform must contain at least one transform")
    return payloads


def _job_from_payload(client: MX8Client, payload: dict[str, Any]) -> Job:
    return Job(
        client=client,
        id=payload["id"],
        status=payload["status"],
        source=payload["source"],
        sink=payload["sink"],
        find=payload.get("find"),
        matched_assets=payload.get("matched_assets"),
        matched_segments=payload.get("matched_segments"),
    )
