# MX8 API Surface

MX8 exposes a few tightly focused surfaces so teams can describe transforms, control jobs, and monitor billing without onboarding dozens of services.

## SDK entry points

- `mx8.Client`: Handles authentication, job submission, and quota enforcement hooks.
- `mx8.transforms`: Host of media-specific builders (`video`, `image`, `audio`) that turn declarative chains into runnable DAGs.
- `mx8.jobs`: Helpers for inspecting status, auto-retries, and webhook delivery.

The canonical Python flow looks like:

```python
from mx8 import Client, transforms

client = Client(api_key="<token>")

job = client.submit_job(
    name="30TB frame extraction",
    source="s3://customer/dataset",
    transforms=transforms.video
        .extract("frames")
        .filter(expr="duration > 5 && byte_size > 1_000_000_000")
        .deduplicate()
        .export("s3://clean/"),
    concurrency=10,
    priority="priority-pool",
)

job.wait()
```

Transforms are **chainable** and designed to read naturally without SQL. Each step adds metadata to the job graph:

- `.extract("frames")`: reads an interval (seconds, frames, or scenes) from media and converts to image payloads.
. `.filter(expr="duration > 5 && corrupt == false")`: drops clips that fail the predicate; expressions can reference duration, format, codec, width, height, frame rate, byte size, checksum/hash, stream_id/media_type, and the corrupt flag so you can keep only clean data.
- `.deduplicate()`: eliminates redundant frames/media by content hash.
- `.export("s3://clean/")`: writes outputs back to S3 or another configured bucket, skipping corrupted files by default.

## REST endpoints

| Verb | Path | Description |
|------|------|-------------|
| `POST` | `/v1/jobs` | Submit a job. Returns a job ID, pipeline, and estimated cost. `transforms` payload mirrors the SDK chain.
| `GET` | `/v1/jobs/{job_id}` | Fetch status, progress, compute usage, and billing events. Supports `fields=events` for streaming updates.
| `PATCH` | `/v1/jobs/{job_id}` | Allow `action` of `pause`, `resume`, or `cancel`. Respects the same quotas as the SDK.
| `GET` | `/v1/jobs` | List a team’s jobs with filters for state, label, cost center, or target datasets.
| `POST` | `/v1/search` | (Optional) Run a lightweight search query over indexed metadata or vectors. You pay per query; refer to `docs/v0_spec.md` for pricing guidance.

### Job payload

```jsonc
{
  "name": "50TB cleanup",
  "source": "s3://media-bucket/2026/",
  "priority": "capacity-pool",
  "transforms": [
    {"type": "extract", "mode": "frames", "params": {"stride": 1}},
    {"type": "filter", "expr": "duration > 5"},
    {"type": "deduplicate"},
    {"type": "export", "destination": "s3://clean/output/"}
  ],
  "limits": {
    "max_workers": 40,
    "tasks_concurrency": 3
  }
}
```

`filters` are intentionally limited to boolean expressions over known numeric or string fields so the SDK can inline them into a filter graph without shipping an SQL parser.

## CLI helpers

```
mx8 job submit --source s3://bucket --transforms video.extract=frames,filter="duration>5",dedup,export=s3://clean/
mx8 job pause <id>
mx8 job resume <id>
mx8 job status <id>
```

The CLI reuses the same payloads as the SDK, and the `pause`/`resume` commands send the same `PATCH` actions documented above. This makes asynchronous jobs feel like terminal-native workflows rather than continuous re-deployments.
