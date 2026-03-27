from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from .models import JobRecord, TransformSpec


class WorkerBackend(Protocol):
    def reconcile(
        self,
        record: JobRecord,
        api_base_url: str,
        coordinator_bind_addr: str,
        desired_workers: int,
        region: str,
        instance_type: str,
        base_env: dict[str, str],
        run_dir: Path,
    ) -> None: ...

    def terminate_job(self, job_id: str) -> None: ...


@dataclass
class LaunchState:
    bind_addr: str
    run_dir: Path
    coordinator: subprocess.Popen[bytes]


@dataclass
class LocalAgentState:
    processes: list[subprocess.Popen[bytes]] = field(default_factory=list)
    next_agent_index: int = 0


class LocalWorkerBackend:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root
        self._jobs: dict[str, LocalAgentState] = {}
        self._lock = threading.Lock()

    def reconcile(
        self,
        record: JobRecord,
        api_base_url: str,
        coordinator_bind_addr: str,
        desired_workers: int,
        region: str,
        instance_type: str,
        base_env: dict[str, str],
        run_dir: Path,
    ) -> None:
        if not self._enabled():
            return
        del api_base_url, region, instance_type
        desired_workers = max(0, min(desired_workers, self._max_local_workers()))
        with self._lock:
            state = self._jobs.setdefault(record.id, LocalAgentState())
            state.processes = [process for process in state.processes if process.poll() is None]
            current_workers = len(state.processes)

            while current_workers < desired_workers:
                index = state.next_agent_index
                state.next_agent_index += 1
                agent = self._spawn_agent(record, coordinator_bind_addr, base_env, run_dir, index)
                state.processes.append(agent)
                current_workers += 1

            while current_workers > desired_workers:
                process = state.processes.pop()
                if process.poll() is None:
                    process.terminate()
                current_workers -= 1

    def terminate_job(self, job_id: str) -> None:
        with self._lock:
            state = self._jobs.pop(job_id, None)
        if state is None:
            return
        for process in state.processes:
            if process.poll() is None:
                process.terminate()

    def initial_worker_count(self) -> int:
        raw = os.getenv("MX8_LOCAL_AGENT_INITIAL_COUNT", "").strip()
        if raw:
            return max(0, int(raw))
        legacy = os.getenv("MX8_LOCAL_AGENT_COUNT", "").strip()
        if legacy:
            return max(0, int(legacy))
        return 1

    def _enabled(self) -> bool:
        raw = os.getenv("MX8_LOCAL_AGENT_ENABLE", "true").strip().lower()
        return raw not in {"0", "false", "no", "off"}

    def _max_local_workers(self) -> int:
        raw = os.getenv("MX8_SCALE_MAX_WORKERS", "").strip()
        if raw:
            return max(1, int(raw))
        return 8

    def _agent_command(self, record: JobRecord) -> list[str]:
        raw = os.getenv("MX8_AGENT_CMD", "").strip()
        if raw:
            return shlex.split(raw)
        features = _cargo_features(record)
        binary = _local_binary_command(self._repo_root, "mx8d-agent", features)
        if binary is not None:
            return binary
        command = ["cargo", "run", "-p", "mx8d-agent"]
        if features:
            command.extend(["--features", ",".join(features)])
        command.append("--")
        return command

    def _spawn_agent(
        self,
        record: JobRecord,
        coordinator_bind_addr: str,
        base_env: dict[str, str],
        run_dir: Path,
        index: int,
    ) -> subprocess.Popen[bytes]:
        env = dict(base_env)
        env.update(
            {
                "MX8_COORD_URL": f"http://{coordinator_bind_addr}",
                "MX8_NODE_ID": f"local-node-{index}",
                "MX8_DEV_LEASE_WANT": env.get("MX8_DEV_LEASE_WANT", "1"),
            }
        )
        log_path = run_dir / f"agent-{index}.log"
        return _spawn_checked(
            self._agent_command(record),
            env,
            log_path,
            f"agent {index} for job {record.id}",
            self._repo_root,
        )


class ExternalFleetBackend:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root
        self._desired_workers: dict[str, int] = {}
        self._lock = threading.Lock()

    def reconcile(
        self,
        record: JobRecord,
        api_base_url: str,
        coordinator_bind_addr: str,
        desired_workers: int,
        region: str,
        instance_type: str,
        base_env: dict[str, str],
        run_dir: Path,
    ) -> None:
        del api_base_url
        command = os.getenv("MX8_FLEET_RECONCILE_CMD", "").strip()
        if not command:
            raise RuntimeError("MX8_FLEET_RECONCILE_CMD must be set for external launcher backend")
        with self._lock:
            previous = self._desired_workers.get(record.id)
            if previous == desired_workers:
                return
            self._desired_workers[record.id] = desired_workers
        env = dict(base_env)
        env.update(
            {
                "MX8_COORD_URL": f"http://{coordinator_bind_addr}",
                "MX8_DESIRED_WORKERS": str(desired_workers),
                "MX8_REGION": region,
                "MX8_INSTANCE_TYPE": instance_type,
                "MX8_RUN_DIR": str(run_dir),
            }
        )
        log_path = run_dir / "fleet-reconcile.log"
        _run_command(
            shlex.split(command),
            env,
            log_path,
            f"fleet reconcile for job {record.id}",
            self._repo_root,
        )

    def terminate_job(self, job_id: str) -> None:
        command = os.getenv("MX8_FLEET_TERMINATE_CMD", "").strip()
        if not command:
            return
        with self._lock:
            self._desired_workers.pop(job_id, None)
        env = os.environ.copy()
        env["MX8_JOB_ID"] = job_id
        _run_command(
            shlex.split(command),
            env,
            self._repo_root / ".mx8-media" / "fleet-terminate.log",
            f"fleet terminate for job {job_id}",
            self._repo_root,
        )


class CoordinatorLauncher:
    def __init__(self) -> None:
        self._jobs: dict[str, LaunchState] = {}
        self._lock = threading.Lock()
        self._repo_root = Path(__file__).resolve().parent.parent
        self._run_root = self._repo_root / ".mx8-media" / "launches"
        self._run_root.mkdir(parents=True, exist_ok=True)
        self._worker_backend = self._build_worker_backend()

    def launch(self, record: JobRecord, api_base_url: str) -> None:
        with self._lock:
            state = self._jobs.get(record.id)
            if state is not None and state.coordinator.poll() is None:
                return

            bind_addr = _allocate_bind_addr()
            run_dir = self._run_root / record.id
            run_dir.mkdir(parents=True, exist_ok=True)

            coordinator_env = self._base_env(record, api_base_url)
            coordinator_env.update(
                {
                    "MX8_COORD_BIND_ADDR": bind_addr,
                    "MX8_WORLD_SIZE": coordinator_env.get("MX8_WORLD_SIZE", str(self._max_workers())),
                    "MX8_MIN_WORLD_SIZE": coordinator_env.get("MX8_MIN_WORLD_SIZE", "1"),
                    "MX8_COORD_HA_ENABLE": coordinator_env.get("MX8_COORD_HA_ENABLE", "false"),
                    "MX8_COORD_STATE_STORE_ENABLE": coordinator_env.get(
                        "MX8_COORD_STATE_STORE_ENABLE", "false"
                    ),
                    "MX8_LEASE_LOG_PATH": coordinator_env.get("MX8_LEASE_LOG_PATH", "none"),
                }
            )
            if record.manifest_hash:
                coordinator_env["MX8_MANIFEST_HASH"] = record.manifest_hash
                coordinator_env.pop("MX8_DATASET_LINK", None)
            else:
                coordinator_env["MX8_DATASET_LINK"] = record.source
            coordinator_log_path = run_dir / "coordinator.log"
            coordinator = _spawn_checked(
                self._coordinator_command(record),
                coordinator_env,
                coordinator_log_path,
                f"coordinator for job {record.id}",
                self._repo_root,
            )
            self._jobs[record.id] = LaunchState(
                bind_addr=bind_addr,
                run_dir=run_dir,
                coordinator=coordinator,
            )

        _wait_for_tcp(bind_addr)

        initial_count = self._initial_workers()
        self.scale_workers(
            record,
            api_base_url,
            initial_count,
            record.region or _default_region(record.source),
            record.instance_type or _default_instance_type(record),
        )

    def scale_workers(
        self,
        record: JobRecord,
        api_base_url: str,
        desired_workers: int,
        region: str,
        instance_type: str,
    ) -> None:
        with self._lock:
            state = self._jobs.get(record.id)
            if state is None or state.coordinator.poll() is not None:
                return
            bind_addr = state.bind_addr
            run_dir = state.run_dir
        self._worker_backend.reconcile(
            record=record,
            api_base_url=api_base_url,
            coordinator_bind_addr=bind_addr,
            desired_workers=desired_workers,
            region=region,
            instance_type=instance_type,
            base_env=self._base_env(record, api_base_url),
            run_dir=run_dir,
        )

    def terminate_job(self, job_id: str) -> None:
        self._worker_backend.terminate_job(job_id)
        with self._lock:
            state = self._jobs.pop(job_id, None)
        if state is None:
            return
        if state.coordinator.poll() is None:
            state.coordinator.terminate()

    def terminate_all(self) -> None:
        with self._lock:
            job_ids = list(self._jobs)
        for job_id in job_ids:
            self.terminate_job(job_id)

    def _build_worker_backend(self) -> WorkerBackend:
        backend = os.getenv("MX8_LAUNCH_BACKEND", "local").strip().lower()
        if backend == "external":
            return ExternalFleetBackend(self._repo_root)
        return LocalWorkerBackend(self._repo_root)

    def _coordinator_command(self, record: JobRecord) -> list[str]:
        raw = os.getenv("MX8_COORDINATOR_CMD", "").strip()
        if raw:
            return shlex.split(raw)
        features = _cargo_features(record)
        binary = _local_binary_command(self._repo_root, "mx8-coordinator", features)
        if binary is not None:
            return binary
        command = ["cargo", "run", "-p", "mx8-coordinator"]
        if features:
            command.extend(["--features", ",".join(features)])
        command.append("--")
        return command

    def _base_env(self, record: JobRecord, api_base_url: str) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "MX8_JOB_ID": record.id,
                "MX8_SOURCE_URI": record.source,
                "MX8_SINK_URI": record.sink,
                "MX8_AWS_REGION": env.get("MX8_AWS_REGION", "us-east-1"),
                "MX8_TRANSFORMS_JSON": json.dumps(
                    [_rust_transform_json(transform) for transform in record.transforms]
                ),
                "MX8_API_BASE_URL": api_base_url.rstrip("/"),
            }
        )
        if record.max_outputs is not None:
            env["MX8_MAX_OUTPUTS"] = str(record.max_outputs)
        return env

    def _max_workers(self) -> int:
        raw = os.getenv("MX8_SCALE_MAX_WORKERS", "").strip()
        if raw:
            return max(1, int(raw))
        return 8

    def _initial_workers(self) -> int:
        if isinstance(self._worker_backend, LocalWorkerBackend):
            return self._worker_backend.initial_worker_count()
        return 0


def _allocate_bind_addr() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        host, port = sock.getsockname()
    return f"{host}:{port}"


def _wait_for_tcp(bind_addr: str, timeout_secs: float | None = None) -> None:
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


def _spawn_checked(
    command: list[str],
    env: dict[str, str],
    log_path: Path,
    label: str,
    cwd: Path,
) -> subprocess.Popen[bytes]:
    log_handle = log_path.open("ab")
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    time.sleep(0.5)
    return_code = process.poll()
    if return_code is not None:
        log_handle.close()
        raise RuntimeError(f"{label} exited immediately with status {return_code}; see {log_path}")
    return process


def _run_command(
    command: list[str],
    env: dict[str, str],
    log_path: Path,
    label: str,
    cwd: Path,
) -> None:
    with log_path.open("ab") as handle:
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if completed.returncode != 0:
        raise RuntimeError(f"{label} failed with status {completed.returncode}; see {log_path}")


def _cargo_features(record: JobRecord) -> list[str]:
    features: list[str] = []
    for scheme in ("s3", "gcs", "azure"):
        if _record_uses_scheme(record, scheme):
            features.append(scheme)
    return features


def _record_uses_scheme(record: JobRecord, scheme: str) -> bool:
    prefixes = {
        "s3": ("s3://",),
        "gcs": ("gs://",),
        "azure": ("az://", "azure://"),
    }[scheme]
    return record.source.startswith(prefixes) or record.sink.startswith(prefixes)


def _default_region(source: str) -> str:
    if source.startswith("file://") or source.startswith("/"):
        return "local"
    return os.getenv("MX8_AWS_REGION", "us-east-1")


def _default_instance_type(record: JobRecord) -> str:
    media_types = {transform.type.split(".", 1)[0] for transform in record.transforms}
    if record.source.startswith("file://") or record.source.startswith("/"):
        family = next(iter(media_types)) if len(media_types) == 1 else "mixed"
        return f"local.{family}"
    if media_types == {"video"}:
        return os.getenv("MX8_VIDEO_INSTANCE_TYPE", "g4dn.xlarge")
    if media_types == {"audio"}:
        return os.getenv("MX8_AUDIO_INSTANCE_TYPE", "c7i.xlarge")
    return os.getenv("MX8_IMAGE_INSTANCE_TYPE", "c7i.xlarge")


def _local_binary_command(repo_root: Path, binary_name: str, features: list[str]) -> list[str] | None:
    if features:
        return None
    binary_path = repo_root / "target" / "debug" / binary_name
    if binary_path.is_file() and os.access(binary_path, os.X_OK):
        return [str(binary_path)]
    return None


def _rust_transform_json(transform: TransformSpec) -> dict[str, object]:
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
                "preset": params.get("preset", ""),
            }
        }
    if transform.type == "audio.transcode":
        return {
            "AudioTranscode": {
                "format": params["format"],
                "bitrate": params.get("bitrate", "128k"),
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
