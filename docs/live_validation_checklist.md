# MX8 Live Validation Checklist

Unit tests and local runs are not enough for design-partner readiness. This checklist is the minimum live validation bar for remote transform workflows.

## Target

Validate real `s3 -> s3` jobs for each public transform family.

## Required runs

### Image

- `resize -> convert`
- `crop -> resize -> convert`
- RAW input -> `resize -> convert`
- `remove_background -> convert`
- verify:
  - outputs land in the right prefix
  - counts match expectations
  - output format and dimensions are correct
  - `GET /v1/jobs/{job_id}` shows `stage=COMPLETE`
  - transparency is present for `remove_background`

### Video

- `proxy()`
- `transcode(codec="h264", crf=23, preset="veryfast")`
- `extract_frames(fps=1, format="jpg")`
- `extract_audio(format="wav", bitrate="192k")`
- verify:
  - outputs are playable
  - expected container/codec is present
  - counts and sizes are sane
  - job events show planner/scaler/runtime progression when applicable

### Remux

- `remux(container="mp4")` on a compatible source
- verify:
  - the job completes without a re-encode path
  - the output container is correct
  - the output is still playable
  - `events` and `failure_*` fields stay empty on success

### Audio

- `extract_audio -> resample -> normalize`
- verify:
  - sample rate and channels are correct
  - loudness normalization succeeded
  - outputs land in the requested sink

## Operator checks for every run

- can you explain the whole job from `job_id` alone?
- does `stage` reflect reality?
- does `worker_pool` populate as expected?
- do `outputs_written` and `completed_objects` match the sink contents?
- if the job fails, does `failure_category` make sense?
- does the event trail identify where it failed?

## Runner

Use [live_validation_runner.py](/Users/ekomotu/Desktop/mx8-projects/mx8-media/scripts/live_validation_runner.py) to make the remote checks repeatable.

Required env:

- `MX8_API_BASE_URL`
- `MX8_API_KEY`
- `MX8_VALIDATE_OUTPUT_PREFIX`
- one or more of:
  - `MX8_VALIDATE_IMAGE_INPUT`
  - `MX8_VALIDATE_RAW_INPUT`
  - `MX8_VALIDATE_VIDEO_INPUT`
  - `MX8_VALIDATE_REMUX_INPUT`

Examples:

```bash
python3 scripts/live_validation_runner.py list
python3 scripts/live_validation_runner.py submit image_basic
python3 scripts/live_validation_runner.py wait <job_id>
```

## Failure drills

Run at least one intentional failure for each family:

- bad source path
- bad sink path
- unsupported transform params
- worker launch failure if you can simulate it safely

The goal is to confirm:

- `failure_category`
- `failure_message`
- event trail quality
- support response speed from a single `job_id`
