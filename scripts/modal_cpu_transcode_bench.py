from __future__ import annotations

import os
import subprocess
import tempfile
import time

import modal


APP_NAME = os.getenv("MX8_MODAL_CPU_BENCH_APP", "mx8-cpu-transcode-bench")
CPU = float(os.getenv("MX8_MODAL_CPU_BENCH_CPU", "4.0"))
MEMORY_MB = int(os.getenv("MX8_MODAL_CPU_BENCH_MEMORY_MB", "8192"))

app = modal.App(APP_NAME)
image = modal.Image.debian_slim(python_version="3.11").apt_install(
    "ffmpeg",
)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@app.function(image=image, cpu=CPU, memory=MEMORY_MB, timeout=60 * 30)
def bench_cpu_transcode(
    duration_secs: int = 120,
    codec: str = "libx264",
    preset: str = "veryfast",
    crf: int = 23,
) -> dict:
    with tempfile.TemporaryDirectory(prefix="mx8-cpu-transcode-bench-") as tmpdir:
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
                "testsrc2=size=1920x1080:rate=30",
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
                "-preset",
                "veryfast",
                "-crf",
                "18",
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
                "-i",
                input_path,
                "-c:v",
                codec,
                "-preset",
                preset,
                "-crf",
                str(crf),
                "-c:a",
                "copy",
                output_path,
            ]
        )
        elapsed = time.perf_counter() - started
        output_bytes = os.path.getsize(output_path)
        return {
            "cpu": float(os.getenv("MX8_MODAL_CPU_BENCH_CPU", str(CPU))),
            "memory_mb": int(os.getenv("MX8_MODAL_CPU_BENCH_MEMORY_MB", str(MEMORY_MB))),
            "duration_secs": duration_secs,
            "codec": codec,
            "preset": preset,
            "crf": crf,
            "elapsed_secs": round(elapsed, 3),
            "realtime_x": round(duration_secs / elapsed, 2),
            "output_bytes": output_bytes,
        }


@app.local_entrypoint()
def main() -> None:
    for duration in (60, 120):
        result = bench_cpu_transcode.remote(duration_secs=duration)
        print(result)
