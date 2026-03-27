from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from .job import Job
from .transforms import Transform, TransformChain
from .work import FindWork


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
        input: str,
        work: Transform | FindWork | Sequence[Transform | FindWork] | TransformChain,
        output: str,
        max: int | None = None,
    ) -> Job:
        normalized = _normalize_work(work)
        payload = {
            "input": input,
            "output": output,
            "work": normalized["work"],
        }
        if max is not None:
            if max <= 0:
                raise ValueError("max must be > 0")
            payload["max"] = max
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


def _normalize_work(
    work: Transform | FindWork | Sequence[Transform | FindWork] | TransformChain,
) -> dict[str, Any]:
    if isinstance(work, Transform):
        items = [work.to_payload()]
    elif isinstance(work, FindWork):
        items = [work.to_payload()]
    elif isinstance(work, TransformChain):
        items = work.to_payloads()
    else:
        items = [item.to_payload() for item in work]
    if not items:
        raise ValueError("work must contain at least one operation")
    find_count = sum(1 for item in items if item.get("type") == "find")
    if find_count > 1:
        raise ValueError("work currently supports at most one find operation")
    if find_count == 1 and items[0].get("type") != "find":
        raise ValueError("find must be the first work item")
    return {"work": items}


def _job_from_payload(client: MX8Client, payload: dict[str, Any]) -> Job:
    work = tuple(payload.get("work") or ())
    find_query = payload.get("find")
    if find_query is None:
        for item in work:
            if item.get("type") == "find":
                find_query = item.get("params", {}).get("query")
                break
    return Job(
        client=client,
        id=payload["id"],
        status=payload["status"],
        input=payload.get("input", payload["source"]),
        output=payload.get("output", payload["sink"]),
        work=work,
        find=find_query,
        max=payload.get("max"),
        matched_assets=payload.get("matched_assets"),
        matched_segments=payload.get("matched_segments"),
    )
