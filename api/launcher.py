from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import time
from pathlib import Path

from .models import JobRecord, TransformSpec


class CoordinatorLauncher:
    def __init__(self) -> None:
        self._processes: dict[str, list[subprocess.Popen[bytes]]] = {}
        self._repo_root = Path(__file__).resolve().parent.parent
        self._run_root = self._repo_root / ".mx8-media" / "launches"
        self._run_root.mkdir(parents=True, exist_ok=True)

    def launch(self, record: JobRecord, api_base_url: str) -> None:
        existing = self._processes.get(record.id, [])
        if any(process.poll() is None for process in existing):
            return

        bind_addr = self._allocate_bind_addr()
        run_dir = self._run_root / record.id
        run_dir.mkdir(parents=True, exist_ok=True)
        processes: list[subprocess.Popen[bytes]] = []

        coordinator_env = self._base_env(record, api_base_url)
        coordinator_env.update(
            {
                "MX8_COORD_BIND_ADDR": bind_addr,
                "MX8_WORLD_SIZE": coordinator_env.get("MX8_WORLD_SIZE", "1"),
                "MX8_MIN_WORLD_SIZE": coordinator_env.get("MX8_MIN_WORLD_SIZE", "1"),
                "MX8_COORD_HA_ENABLE": coordinator_env.get("MX8_COORD_HA_ENABLE", "false"),
                "MX8_COORD_STATE_STORE_ENABLE": coordinator_env.get(
                    "MX8_COORD_STATE_STORE_ENABLE", "false"
                ),
                "MX8_LEASE_LOG_PATH": coordinator_env.get("MX8_LEASE_LOG_PATH", "none"),
                "MX8_DATASET_LINK": record.source,
            }
        )
        coordinator_log_path = run_dir / "coordinator.log"
        coordinator = self._spawn_checked(
            self._coordinator_command(record),
            coordinator_env,
            coordinator_log_path,
            f"coordinator for job {record.id}",
        )
        processes.append(coordinator)

        self._wait_for_tcp(bind_addr)

        if self._local_agents_enabled():
            coord_url = f"http://{bind_addr}"
            agent_count = max(1, int(os.getenv("MX8_LOCAL_AGENT_COUNT", "1")))
            for index in range(agent_count):
                agent_env = self._base_env(record, api_base_url)
                agent_env.update(
                    {
                        "MX8_COORD_URL": coord_url,
                        "MX8_NODE_ID": f"local-node-{index}",
                        "MX8_DEV_LEASE_WANT": agent_env.get("MX8_DEV_LEASE_WANT", "1"),
                    }
                )
                agent_log_path = run_dir / f"agent-{index}.log"
                agent = self._spawn_checked(
                    self._agent_command(record),
                    agent_env,
                    agent_log_path,
                    f"agent {index} for job {record.id}",
                )
                processes.append(agent)

        self._processes[record.id] = processes

    def terminate_all(self) -> None:
        for processes in self._processes.values():
            for process in processes:
                if process.poll() is None:
                    process.terminate()
        self._processes.clear()

    def _coordinator_command(self, record: JobRecord) -> list[str]:
        raw = os.getenv("MX8_COORDINATOR_CMD", "").strip()
        if raw:
            return shlex.split(raw)
        command = ["cargo", "run", "-p", "mx8-coordinator"]
        if self._needs_s3(record):
            command.extend(["--features", "s3"])
        command.append("--")
        return command

    def _agent_command(self, record: JobRecord) -> list[str]:
        raw = os.getenv("MX8_AGENT_CMD", "").strip()
        if raw:
            return shlex.split(raw)
        command = ["cargo", "run", "-p", "mx8d-agent"]
        if self._needs_s3(record):
            command.extend(["--features", "s3"])
        command.append("--")
        return command

    def _allocate_bind_addr(self) -> str:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            host, port = sock.getsockname()
        return f"{host}:{port}"

    def _base_env(self, record: JobRecord, api_base_url: str) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "MX8_JOB_ID": record.id,
                "MX8_SOURCE_URI": record.source,
                "MX8_SINK_URI": record.sink,
                "MX8_AWS_REGION": env.get("MX8_AWS_REGION", "us-east-1"),
                "MX8_TRANSFORMS_JSON": json.dumps(
                    [self._rust_transform_json(transform) for transform in record.transforms]
                ),
                "MX8_API_BASE_URL": api_base_url.rstrip("/"),
            }
        )
        return env

    def _spawn_checked(
        self,
        command: list[str],
        env: dict[str, str],
        log_path: Path,
        label: str,
    ) -> subprocess.Popen[bytes]:
        log_handle = log_path.open("ab")
        process = subprocess.Popen(
            command,
            cwd=self._repo_root,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
        )
        time.sleep(0.5)
        return_code = process.poll()
        if return_code is not None:
            log_handle.close()
            raise RuntimeError(
                f"{label} exited immediately with status {return_code}; see {log_path}"
            )
        return process

    def _wait_for_tcp(self, bind_addr: str, timeout_secs: float | None = None) -> None:
        if timeout_secs is None:
            timeout_secs = float(os.getenv("MX8_LAUNCH_WAIT_SECS", "90"))
        host, port_text = bind_addr.rsplit(":", 1)
        deadline = time.time() + timeout_secs
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                with socket.create_connection((host, int(port_text)), timeout=0.5):
                    return
            except OSError as err:
                last_error = err
                time.sleep(0.1)
        raise RuntimeError(f"coordinator did not start listening on {bind_addr}: {last_error}")

    def _local_agents_enabled(self) -> bool:
        raw = os.getenv("MX8_LOCAL_AGENT_ENABLE", "true").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    def _needs_s3(self, record: JobRecord) -> bool:
        return record.source.startswith("s3://") or record.sink.startswith("s3://")

    def _rust_transform_json(self, transform: TransformSpec) -> dict[str, object]:
        params = dict(transform.params)
        if transform.type == "image.resize":
            return {
                "ImageResize": {
                    "width": params["width"],
                    "height": params["height"],
                    "maintain_aspect": params.get("maintain_aspect", True),
                }
            }
        if transform.type == "image.crop":
            return {
                "ImageCrop": {
                    "width": params["width"],
                    "height": params["height"],
                }
            }
        if transform.type == "image.convert":
            return {
                "ImageConvert": {
                    "format": params["format"],
                    "quality": params.get("quality", 85),
                }
            }
        if transform.type == "video.transcode":
            return {
                "VideoTranscode": {
                    "codec": params["codec"],
                    "crf": params.get("crf", 23),
                }
            }
        if transform.type == "video.resize":
            return {
                "VideoResize": {
                    "width": params["width"],
                    "height": params["height"],
                    "maintain_aspect": params.get("maintain_aspect", True),
                }
            }
        if transform.type == "video.extract_audio":
            return {
                "VideoExtractAudio": {
                    "format": params["format"],
                    "bitrate": params.get("bitrate", "128k"),
                }
            }
        if transform.type == "video.extract_frames":
            return {
                "VideoExtractFrames": {
                    "fps": params["fps"],
                    "format": params["format"],
                }
            }
        if transform.type == "audio.resample":
            return {
                "AudioResample": {
                    "rate": params["rate"],
                    "channels": params.get("channels", 1),
                }
            }
        if transform.type == "audio.normalize":
            return {
                "AudioNormalize": {
                    "loudness_lufs": params.get("loudness", -14.0),
                }
            }
        raise ValueError(f"unsupported transform type for coordinator launch: {transform.type}")
