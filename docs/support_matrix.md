# MX8 Support Matrix

This document is the current support line for design partners. If something is not listed as supported, do not promise it as a default public workflow.

## Supported

### Canonical job surface

- `mx8.run(input=..., work=[...], output=...)`
- `POST /v1/jobs`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`

### Image transforms

- `image.remove_background`
- `image.resize`
- `image.crop`
- `image.convert`
- `image.filter`
- Canonical aliases: `remove_background()`, `resize(..., media="image")`, `crop(...)`, `convert(...)`, `filter(..., media="image")`

### Video transforms

- `video.transcode`
- `video.remux`
- `video.extract_frames`
- `video.extract_audio`
- `video.resize`
- `video.filter`
- Canonical aliases: `clip()`, `proxy()`, `remux(...)`, `transcode(codec=...)`, `extract_frames(...)`, `extract_audio(...)`, `resize(..., media="video")`, `filter(..., media="video")`

### Audio transforms

- `audio.transcode`
- `audio.resample`
- `audio.normalize`
- `audio.filter`
- Canonical aliases: `transcode(format=...)`, `resample(...)`, `normalize(...)`, `filter(..., media="audio")`

### Sources and sinks

- `s3://...` is the default supported remote source and sink
- local filesystem paths are supported for local development and controlled runs

### Runtime dependencies

- RAW inputs are supported when the worker ships the current Rust RAW decoder path
- `remove_background()` is supported when Python plus `rembg` is present in the worker environment

### Job visibility

- `status`
- `stage`
- `failure_category`
- `failure_message`
- `completed_objects`
- `completed_bytes`
- `outputs_written`
- `current_workers`
- `desired_workers`
- `worker_pool`
- `region`
- `instance_type`
- recent `events`

## Experimental

- `find(...)` selection workflows
- search-led planner flows documented in [find_v1.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/find_v1.md)
- GCS and Azure URI handling outside controlled internal testing
- very large multi-TB video jobs that need custom fleet planning or deadline guarantees
- `remove_background()` quality guarantees for edge-heavy images such as hair, fur, and translucent objects

Experimental means:

- it may work
- it may be useful internally
- it is not the default public promise
- it should be called out explicitly during onboarding

## Not Yet Public

- a public `/v1/search` contract
- OCR-backed search
- reranking and temporal-reasoning features
- public worker-pool selection
- self-serve guarantees for jobs above 5 TB
- generic cross-cloud sink commitments beyond the supported S3-first path

## Operator notes

- `proxy()` is a named, opinionated `video.transcode` workflow with defaults
- `remux()` is full-file stream-copy repackaging only and currently supports `container="mp4"`
- `find -> remux` is intentionally not supported
- `remove_background()` defaults to PNG unless a later `convert(...)` overrides it
- jobs above 5 TB should move to onboarding/custom review even if the transform itself is simple
