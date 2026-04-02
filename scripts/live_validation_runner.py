#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class Scenario:
    name: str
    input_env: str
    output_suffix: str
    payload: dict


SCENARIOS: list[Scenario] = [
    Scenario(
        name="image_basic",
        input_env="MX8_VALIDATE_IMAGE_INPUT",
        output_suffix="image-basic/",
        payload={
            "work": [
                {"type": "resize", "params": {"width": 1024, "height": 1024, "media": "image"}},
                {"type": "convert", "params": {"format": "webp", "quality": 85}},
            ]
        },
    ),
    Scenario(
        name="image_raw",
        input_env="MX8_VALIDATE_RAW_INPUT",
        output_suffix="image-raw/",
        payload={
            "work": [
                {"type": "resize", "params": {"width": 2048, "height": 2048, "media": "image"}},
                {"type": "convert", "params": {"format": "jpg", "quality": 90}},
            ]
        },
    ),
    Scenario(
        name="image_remove_background",
        input_env="MX8_VALIDATE_IMAGE_INPUT",
        output_suffix="image-remove-background/",
        payload={
            "work": [
                {"type": "remove_background", "params": {}},
                {"type": "convert", "params": {"format": "png", "quality": 100}},
            ]
        },
    ),
    Scenario(
        name="video_proxy",
        input_env="MX8_VALIDATE_VIDEO_INPUT",
        output_suffix="video-proxy/",
        payload={"work": [{"type": "proxy", "params": {}}]},
    ),
    Scenario(
        name="video_transcode",
        input_env="MX8_VALIDATE_VIDEO_INPUT",
        output_suffix="video-transcode/",
        payload={
            "work": [
                {
                    "type": "video.transcode",
                    "params": {"codec": "h264", "crf": 23, "preset": "veryfast"},
                }
            ]
        },
    ),
    Scenario(
        name="video_extract_frames",
        input_env="MX8_VALIDATE_VIDEO_INPUT",
        output_suffix="video-extract-frames/",
        payload={
            "work": [
                {"type": "extract_frames", "params": {"fps": 1, "format": "jpg"}},
            ]
        },
    ),
    Scenario(
        name="video_extract_audio_chain",
        input_env="MX8_VALIDATE_VIDEO_INPUT",
        output_suffix="video-extract-audio/",
        payload={
            "work": [
                {"type": "extract_audio", "params": {"format": "wav", "bitrate": "192k"}},
                {"type": "resample", "params": {"rate": 16000, "channels": 1}},
                {"type": "normalize", "params": {"loudness": -14.0}},
            ]
        },
    ),
    Scenario(
        name="video_remux",
        input_env="MX8_VALIDATE_REMUX_INPUT",
        output_suffix="video-remux/",
        payload={"work": [{"type": "remux", "params": {"container": "mp4"}}]},
    ),
]


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(f"missing required env: {name}")
    return value


def api_request(method: str, path: str, payload: dict | None = None) -> dict:
    base_url = require_env("MX8_API_BASE_URL").rstrip("/")
    token = os.getenv("MX8_API_KEY", "").strip()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        raise SystemExit(f"{method} {path} failed: {exc.code} {body}") from exc


def find_scenario(name: str) -> Scenario:
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    raise SystemExit(f"unknown scenario: {name}")


def build_payload(scenario: Scenario) -> dict:
    sink_prefix = require_env("MX8_VALIDATE_OUTPUT_PREFIX")
    input_uri = require_env(scenario.input_env)
    return {
        "input": input_uri,
        "output": sink_prefix.rstrip("/") + "/" + scenario.output_suffix,
        "work": scenario.payload["work"],
    }


def submit(name: str) -> None:
    scenario = find_scenario(name)
    payload = build_payload(scenario)
    job = api_request("POST", "/v1/jobs", payload)
    print(json.dumps({"scenario": name, "job_id": job["id"], "payload": payload}, indent=2))


def wait_for_job(job_id: str, interval: float = 2.0) -> None:
    while True:
        job = api_request("GET", f"/v1/jobs/{job_id}")
        status = job.get("status")
        stage = job.get("stage")
        print(
            json.dumps(
                {
                    "job_id": job_id,
                    "status": status,
                    "stage": stage,
                    "outputs_written": job.get("outputs_written"),
                    "failure_category": job.get("failure_category"),
                    "failure_message": job.get("failure_message"),
                }
            )
        )
        if status in {"COMPLETE", "FAILED"}:
            return
        time.sleep(interval)


def usage() -> None:
    names = ", ".join(s.name for s in SCENARIOS)
    print("usage:")
    print("  live_validation_runner.py list")
    print("  live_validation_runner.py submit <scenario>")
    print("  live_validation_runner.py wait <job_id>")
    print(f"scenarios: {names}")


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        usage()
        return 1
    command = argv[1]
    if command == "list":
        for scenario in SCENARIOS:
            print(scenario.name)
        return 0
    if command == "submit" and len(argv) == 3:
        submit(argv[2])
        return 0
    if command == "wait" and len(argv) == 3:
        wait_for_job(argv[2])
        return 0
    usage()
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
