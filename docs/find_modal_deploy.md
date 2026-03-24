# MX8 `find` Modal Deployment

This runbook covers the current v1 deployment path for the internal `find` inference worker.

## What gets deployed

- Modal app: `mx8-find-worker`
- Modal function: `process_shard`
- Worker source: `inference/modal_find_worker.py`

The worker reads `FindShard` payloads from the MX8 control plane, fetches source video from a presigned URL, samples frames with `ffmpeg`, scores them with SigLIP2, and returns shard-local hit segments.

## Prerequisites

- Modal CLI installed locally
- Access to a Modal workspace
- Modal auth configured, ideally via a service user:
  - `MODAL_TOKEN_ID`
  - `MODAL_TOKEN_SECRET`
- API host has AWS credentials that can presign source objects
- Source media lives in S3 or another HTTPS-accessible location

## Deploy

From repo root:

```bash
cd /Users/ekomotu/Desktop/mx8-projects/mx8-media
./scripts/deploy_find_worker.sh prod-find
```

If you prefer the raw Modal CLI:

```bash
cd /Users/ekomotu/Desktop/mx8-projects/mx8-media
modal deploy inference/modal_find_worker.py --env prod-find
```

The worker creates or reuses the Hugging Face cache volume named by `MX8_FIND_MODAL_HF_CACHE_VOLUME`. By default that is `mx8-find-hf-cache`.

## API configuration

Set these env vars on the MX8 API process:

```bash
export MX8_FIND_PROVIDER=modal
export MX8_FIND_MODAL_APP_NAME=mx8-find-worker
export MX8_FIND_MODAL_FUNCTION_NAME=process_shard
export MX8_FIND_MODAL_ENVIRONMENT=prod-find
export MX8_FIND_S3_REGION=us-east-1
export MX8_FIND_S3_PRESIGN_EXPIRES_SECS=3600
```

Optional tuning:

```bash
export MX8_FIND_DISPATCHER_WORKERS=4
export MX8_FIND_SAMPLE_FPS=1.0
export MX8_FIND_MODEL=siglip2_base
export MX8_FIND_BATCH_SIZE=16
export MX8_FIND_SCORE_THRESHOLD=-8.0
export MX8_FIND_SCORE_MARGIN=2.0
export MX8_FIND_PAD_BEFORE_MS=500
export MX8_FIND_PAD_AFTER_MS=1500
export MX8_FIND_MERGE_GAP_MS=1500
```

## Smoke test

1. Start the API with the env vars above.
2. Submit one small `find` job:

```python
mx8.run(
    source="s3://raw-dashcam-archive/",
    find="a stop sign covered in heavy snow",
    transform=mx8.video.extract_frames(fps=1, format="jpg"),
    sink="s3://training-dataset/",
)
```

3. Watch for the job to move:
   - `FINDING`
   - `PENDING`
   - `RUNNING`
   - `COMPLETE`

4. Check Modal logs if planning stalls or fails:

```bash
modal app logs mx8-find-worker --env prod-find
```

## Roll forward / rollback

- Redeploying the same app updates it in place:

```bash
./scripts/deploy_find_worker.sh prod-find
```

- To stop the deployed app:

```bash
modal app stop mx8-find-worker --env prod-find
```

- To revert, deploy the previous known-good worker revision from git and rerun the deploy script.

## Current limits

- The dispatcher queue is still in-process on the API side.
- Long videos now fan out into overlapping scan shards after planner-side duration probing.
- The worker supports `siglip2_base` today.
