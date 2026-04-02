# MX8 Media SDK

MX8 runs image, video, and audio transforms across large media datasets without forcing teams to build their own batch control plane.

## Install

```bash
pip install mx8
```

## Auth

Set the API base URL and bearer token in the environment:

```bash
export MX8_API_BASE_URL=http://127.0.0.1:8000
export MX8_API_KEY=your-token
```

If the server is configured with account-scoped keys, that token also determines which jobs the caller can see.

## Quick Start

```python
import mx8

job = mx8.run(
    input="s3://product-images/",
    work=[
        mx8.remove_background(),
        mx8.convert(format="png"),
    ],
    output="s3://cutouts/",
)

job.wait()
```

RAW image conversion looks like:

```python
job = mx8.run(
    input="s3://camera-raw/",
    work=[
        mx8.resize(width=2048, height=2048, media="image"),
        mx8.convert(format="jpg", quality=90),
    ],
    output="s3://standard-images/",
)
```

Background removal looks like:

```python
job = mx8.run(
    input="s3://product-images/",
    work=[
        mx8.remove_background(),
        mx8.convert(format="png"),
    ],
    output="s3://cutouts/",
)
```

## First-class workflows

- `mx8.remove_background()` for producing alpha-backed cutouts from images
- `mx8.proxy()` for lighter review copies
- `mx8.remux(container="mp4")` for stream-copy repackaging
- `mx8.clip()` for video segment exports
- `mx8.transcode(...)` for video or audio re-encoding
- `mx8.extract_frames(...)` and `mx8.extract_audio(...)` for dataset and media prep

RAW inputs are decoded into a standard image internally when they flow through the image pipeline. If no later `convert(...)` overrides the output format, RAW inputs default to JPEG output.
`remove_background()` defaults to PNG output unless a later `convert(...)` overrides the output format. The worker environment needs Python plus `rembg` available for the helper script in [scripts/remove_background.py](/Users/ekomotu/Desktop/mx8-projects/mx8-media/scripts/remove_background.py).

## Job visibility

`job.poll()` refreshes the operational fields returned by the API:

- `job.status`
- `job.stage`
- `job.outputs_written`
- `job.worker_pool`
- `job.failure_category`
- `job.failure_message`
- `job.events`

That means a design partner can hand you a `job.id`, and both sides can look at the same stage, failure reason, and recent event trail.

## Operator CLI

The SDK now ships a small operator CLI:

```bash
mx8 ops watch <job_id>
mx8 ops live
```

- `mx8 ops watch <job_id>` follows one job live
- `mx8 ops live` shows the current active job table

## API Shape

- `import mx8` is the primary entry point.
- `mx8.run(input=..., work=[...], output=...)` is the canonical job shape.
- Media namespaces remain available for deeper transform composition, but the default UX is an ordered `work=[...]` list.

See [docs/api_shape.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/api_shape.md) for the current SDK and REST surface, [docs/support_matrix.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/support_matrix.md) for the current support line, and [docs/design_partner_onboarding.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/design_partner_onboarding.md) for the current onboarding/support flow.
