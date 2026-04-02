from __future__ import annotations

import os
import subprocess
import tempfile
import time

import modal


APP_NAME = os.getenv("MX8_MODAL_NVENC_BENCH_APP", "mx8-nvenc-bench")
GPU_TYPE = os.getenv("MX8_MODAL_NVENC_GPU", "L4")

app = modal.App(APP_NAME)
image = modal.Image.debian_slim(python_version="3.11").apt_install(
    "ffmpeg",
)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@app.function(image=image, gpu=GPU_TYPE, timeout=60 * 30)
def bench_nvenc(duration_secs: int = 60, codec: str = "hevc_nvenc", preset: str = "p4") -> dict:
    with tempfile.TemporaryDirectory(prefix="mx8-nvenc-bench-") as tmpdir:
        input_path = os.path.join(tmpdir, f"input-{duration_secs}s.mp4")
        output_path = os.path.join(tmpdir, f"output-{duration_secs}s.mp4")

        _run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                f"testsrc=size=1280x720:rate=30",
                "-f",
                "lavfi",
                "-i",
                "sine=frequency=1000:sample_rate=44100",
                "-t",
                str(duration_secs),
                "-pix_fmt",
                "yuv420p",
                "-c:v",
                "libx264",
                "-c:a",
                "aac",
                input_path,
            ]
        )

        started = time.perf_counter()
        _run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-hwaccel",
                "cuda",
                "-i",
                input_path,
                "-c:v",
                codec,
                "-preset",
                preset,
                "-cq",
                "23",
                "-c:a",
                "copy",
                output_path,
            ]
        )
        elapsed = time.perf_counter() - started
        output_bytes = os.path.getsize(output_path)
        return {
            "gpu": GPU_TYPE,
            "duration_secs": duration_secs,
            "codec": codec,
            "preset": preset,
            "elapsed_secs": round(elapsed, 3),
            "realtime_x": round(duration_secs / elapsed, 2),
            "output_bytes": output_bytes,
        }


@app.local_entrypoint()
def main() -> None:
    for duration in (60, 120):
        result = bench_nvenc.remote(duration_secs=duration)
        print(result)
