from __future__ import annotations

import os
import socket
import subprocess
import time
from typing import Any

import modal


APP_NAME = os.getenv("MX8_MODAL_PROBE_APP_NAME", "mx8-workspace-probe")
GPU_TYPE = os.getenv("MX8_MODAL_PROBE_GPU", "L4")
CPU = float(os.getenv("MX8_MODAL_PROBE_CPU", "1.0"))
MEMORY_MB = int(os.getenv("MX8_MODAL_PROBE_MEMORY_MB", "2048"))

app = modal.App(APP_NAME)
image = modal.Image.debian_slim(python_version="3.11")


def _nvidia_smi_lines() -> list[str]:
    try:
        output = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=name,driver_version,memory.total",
                "--format=csv,noheader",
            ],
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    except Exception as exc:  # pragma: no cover - best effort debugging only
        return [f"nvidia-smi unavailable: {exc}"]
    return [line.strip() for line in output.splitlines() if line.strip()]


@app.function(
    image=image,
    gpu=GPU_TYPE,
    cpu=CPU,
    memory=MEMORY_MB,
    timeout=60 * 10,
)
def gpu_probe(label: str = "default", hold_secs: int = 20) -> dict[str, Any]:
    started_at = time.time()
    time.sleep(max(0, hold_secs))
    return {
        "label": label,
        "app_name": APP_NAME,
        "gpu_type": GPU_TYPE,
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "hold_secs": hold_secs,
        "started_at_unix": started_at,
        "finished_at_unix": time.time(),
        "nvidia_smi": _nvidia_smi_lines(),
    }


@app.local_entrypoint()
def main(label: str = "default", hold_secs: int = 20) -> None:
    result = gpu_probe.remote(label=label, hold_secs=hold_secs)
    print(result)
