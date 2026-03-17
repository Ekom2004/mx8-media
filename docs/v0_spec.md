# mx8-media v0 — Working Spec

This document is a working specification for `mx8-media` v0.

The authoritative source of truth is `docs/ARCHITECTURE.md`.

If this file conflicts with `docs/ARCHITECTURE.md`, `ARCHITECTURE.md` wins.

---

## 1. The API Contract

```python
mx8.run(
    source,               # str: URI of input data
    transform=...,        # Transform | list[Transform]: operation(s) to apply
    sink=...,             # str: URI of output destination
)
```

- `source` is always the first positional argument.
- `transform` accepts a single transform or a list (pipeline chaining).
- `sink` is required. We never host output data.
- Returns a `Job` object with `.id`, `.status`, `.wait()`, `.poll()`.

---

## 2. Supported Sources (v0)

| Protocol | Example | Notes |
|---|---|---|
| Amazon S3 | `s3://bucket/prefix/` | Folder or single file |
| Amazon S3 | `s3://bucket/file.mp4` | Single object |
| HTTP/HTTPS | `https://example.com/file.mp4` | Single file only |

**Out of scope for v0:** GCS, Azure Blob, R2 (added in v1).

---

## 3. Supported Sinks (v0)

| Protocol | Example | Notes |
|---|---|---|
| Amazon S3 | `s3://bucket/output/` | Writes results with original filename preserved |

Output naming: `{sink}/{original_filename_without_ext}_{transform_tag}.{ext}`  
Example: `s3://out/video_001_720p.mp4`

---

## 4. Supported Transforms (v0)

### `video.*`

```python
video.transcode(codec="h264" | "h265" | "av1", crf=23)
```
Transcode video to a target codec. Preserves audio track. Default CRF=23 (high quality).

```python
video.resize(width=int, height=int, maintain_aspect=True)
```
Resize video frames to target resolution. Maintains aspect ratio by default (letterboxes if needed).

```python
video.extract_frames(fps=1, format="jpg" | "png")
```
Extract one frame per N seconds. Outputs individual image files. Each frame named `{source}_{timestamp_ms}.{format}`.

```python
video.extract_audio(format="mp3" | "wav" | "flac", bitrate="128k")
```
Strip the audio track from a video file. Outputs a standalone audio file.

---

### `image.*`

```python
image.resize(width=int, height=int, maintain_aspect=True)
```
Resize image files (jpg, png, webp, tiff, bmp). Maintains aspect ratio by default.

```python
image.crop(width=int, height=int)
```
Center-crop image files to the target size. Fails if the crop is larger than the current image.

```python
image.convert(format="jpg" | "png" | "webp", quality=85)
```
Convert image to a target format. `quality` applies to lossy formats (jpg, webp).

---

### `audio.*`

```python
audio.resample(rate=16000, channels=1)
```
Re-sample audio to a target sample rate and channel count. Default: 16kHz mono (Whisper-ready).

```python
audio.normalize(loudness=-14)
```
Normalize audio loudness to a target LUFS level. Default: -14 LUFS (broadcast/podcast standard).

---

## 5. Transform Chaining (v0)

A list of transforms runs as a pipeline. Each stage runs on the output of the previous stage.

```python
mx8.run(
    "s3://bucket/videos/",
    transform=[
        video.resize(width=1280, height=720),
        video.transcode(codec="h264"),
    ],
    sink="s3://bucket/output/",
)
```

Constraints:
- All transforms in a chain must operate on the same media type (video → video → video).
- Cross-type pipelines (e.g., video → audio) are not supported in v0. Use separate `mx8.run()` calls.

---

## 6. Compute Routing (v0)

| Transform | Hardware |
|---|---|
| `video.transcode`, `video.resize` | GPU (NVENC/NVDEC via ffmpeg-next) |
| `video.extract_frames` | GPU decode, CPU encode (JPEG) |
| `video.extract_audio` | CPU only |
| `image.*` | CPU only |
| `audio.*` | CPU only |

The `mx8-coordinator` automatically routes each job to the correct instance type. Users never configure hardware.

---

## 7. Pricing (v0)

| Tier | Price |
|---|---|
| Single transform | **$0.05 / GB of input** |
| Multi-transform (2+ in list) | **$0.07 / GB of input** |

Billing is based on **input data size**, not output size or video duration.
No charge for retries caused by mx8 infrastructure failures.

---

## 8. Job Lifecycle

```
PENDING → QUEUED → RUNNING → COMPLETE
                          ↘ FAILED
```

- `PENDING`: Job received, manifest being built.
- `QUEUED`: Manifest ready, waiting for compute.
- `RUNNING`: Leases active, workers processing.
- `COMPLETE`: All records processed and uploaded to sink.
- `FAILED`: Unrecoverable error. Job can be retried via `mx8.retry(job_id)`.

---

## 9. What Is NOT in v0

- GCS, Azure Blob, Cloudflare R2 sources/sinks
- Whisper transcription
- CLIP embeddings / semantic search
- BYOM (bring your own model) inference
- WebDataset / `.tar` packing
- Real-time / streaming jobs (v0 is batch only)
- UI / dashboard (API only)
- SOC 2 / HIPAA compliance tiers (planned v1)
