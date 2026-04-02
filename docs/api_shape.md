# MX8 API Surface

MX8 exposes one high-level job shape so teams can run large media transforms without building the control plane around them.

## Canonical Python shape

- `import mx8` is the primary entry point.
- `mx8.run(input=..., work=[...], output=...)` is the canonical SDK surface.
- Public work items are transform-first: `remove_background`, `clip`, `proxy`, `remux`, `transcode`, `extract_frames`, `extract_audio`, `resize`, `crop`, `convert`, `filter`, `resample`, and `normalize`.

The canonical Python flow looks like:

```python
import mx8

job = mx8.run(
    input="s3://catalog-images/",
    work=[
        mx8.crop(width=1200, height=1200),
        mx8.resize(width=1024, height=1024, media="image"),
        mx8.convert(format="webp", quality=85),
    ],
    output="s3://catalog-images-web/",
)

job.wait()
```

`work` is an ordered list of operations:

- `mx8.clip()`: exports selected video segments as clips. By default it emits H.264 MP4 output.
- `mx8.proxy()`: generates lighter review copies. By default it emits H.264 MP4 output with CRF 28 and `veryfast`.
- `mx8.remux(container="mp4")`: repackages compatible video streams without re-encoding.
- `mx8.remove_background()`: removes image backgrounds and emits alpha-backed image output.
- `mx8.transcode(codec="h264")`: transcodes video output.
- `mx8.transcode(format="flac")`: transcodes audio output.
- `mx8.extract_frames(fps=1, format="jpg")`: reads frames from video and emits image outputs.
- `mx8.extract_audio(format="mp3")`: extracts an audio artifact from video.
- `mx8.resize(width=1280, height=720, media="video")`: resizes video output.
- `mx8.resize(width=512, height=512, media="image")`: resizes image output.
- `mx8.crop(width=1200, height=1200)`: crops image output.
- `mx8.convert(format="webp", quality=85)`: converts image output.
- `mx8.filter(expr="...", media="audio")`: applies an audio, image, or video filter.
- `mx8.resample(rate=16000, channels=1)`: resamples audio output.
- `mx8.normalize(loudness=-14.0)`: normalizes audio output.
- `output="s3://clean/"`: declares where finished outputs land.

Media namespaces such as `mx8.video`, `mx8.image`, and `mx8.audio` remain available for deeper composition, but the public API is centered on `input`, `work`, and `output`.

## REST endpoints

| Verb | Path | Description |
|------|------|-------------|
| `POST` | `/v1/jobs` | Submit a job. The request body uses `input`, `work`, and `output`. |
| `GET` | `/v1/jobs/{job_id}` | Fetch status, stage, progress, failures, and recent events for one job. |
| `GET` | `/v1/jobs` | List jobs for the current environment. |

## Authentication

MX8 uses bearer-token auth for the public job API when the server is configured with API keys.

Server-side:

- `MX8_API_KEY=<token>` for a single shared key
- or `MX8_API_KEYS=acct_a=token_a,acct_b=token_b,op:internal=operator_token` for multiple account-scoped keys

Client-side:

- set `MX8_API_KEY=<token>`
- the SDK and operator CLI send `Authorization: Bearer <token>` automatically

If the server has no API keys configured, auth stays disabled for local development.

When account-scoped keys are configured:

- customer keys only see jobs for their own `account_id`
- operator keys (`op:...`) can see jobs across accounts

## Job payload

```jsonc
{
  "input": "s3://media-bucket/2026/",
  "work": [
    {"type": "resize", "params": {"width": 2048, "height": 2048, "media": "image"}},
    {"type": "convert", "params": {"format": "jpg", "quality": 90}}
  ],
  "output": "s3://standard-images/2026/"
}
```

The API still accepts the older `source/find/transforms/sink` request shape for compatibility, but `input/work/output` is the canonical public surface. Public REST aliases such as `remove_background`, `clip`, `proxy`, `remux`, `extract_frames`, `extract_audio`, `convert`, `crop`, `resample`, `normalize`, `transcode`, `resize`, and `filter` are normalized onto the underlying media-specific transform types inside the API.

## Job status surface

`GET /v1/jobs/{job_id}` returns the operational fields a customer or operator needs to understand the current state of the job:

```jsonc
{
  "id": "job_123",
  "status": "RUNNING",
  "stage": "RUNNING",
  "input": "s3://catalog-images/",
  "output": "s3://catalog-images-web/",
  "work": [
    {"type": "image.resize", "params": {"width": 1024, "height": 1024}},
    {"type": "image.convert", "params": {"format": "webp", "quality": 85}}
  ],
  "completed_objects": 8400,
  "completed_bytes": 2123456789,
  "outputs_written": 8400,
  "current_workers": 4,
  "desired_workers": 4,
  "worker_pool": "image-default",
  "region": "us-east-1",
  "instance_type": "c7i.xlarge",
  "failure_category": null,
  "failure_message": null,
  "events": [
    {
      "at": "2026-03-29T14:01:12.345678Z",
      "type": "job_submitted",
      "status": "PENDING",
      "stage": "SUBMITTED",
      "message": "Job submitted",
      "metadata": {
        "source": "s3://catalog-images/",
        "sink": "s3://catalog-images-web/"
      }
    },
    {
      "at": "2026-03-29T14:01:13.102030Z",
      "type": "worker_plan_updated",
      "status": "QUEUED",
      "stage": "QUEUED",
      "message": "Scaler updated the worker plan for the job",
      "metadata": {
        "desired_workers": 4,
        "region": "us-east-1",
        "instance_type": "c7i.xlarge",
        "worker_pool": "image-default"
      }
    }
  ]
}
```

Important semantics:

- `status` remains the coarse lifecycle signal.
- `stage` gives the operator-facing phase (`SUBMITTED`, `PLANNING`, `PLANNED`, `QUEUED`, `RUNNING`, `COMPLETE`, `FAILED`).
- `failure_category` and `failure_message` are populated when the job fails.
- `events` is the recent event trail for support and debugging.

## Support notes

- `proxy()` and `remux()` are first-class public workflows, not hidden aliases.
- `remove_background()` is an image-only workflow; it defaults to PNG output unless a later `convert(...)` overrides the output format.
- `find(...)` still exists in the API for controlled testing, but it is not the default public product promise. See the support matrix before exposing it to customers.
- Scheduler concerns such as exact worker placement and concurrency policy stay in the control plane rather than showing up in the public API.
- RAW inputs can flow through the normal image pipeline without a dedicated verb; if no later `convert(...)` overrides the output format, they default to JPEG output.
- `remove_background()` requires Python plus `rembg` in the worker environment today.

## Operator CLI

The SDK also ships a small operator CLI for internal support:

```bash
mx8 ops watch <job_id>
mx8 ops live
```

- `watch` renders one job in a live detail view
- `live` renders the active job table across the fleet
