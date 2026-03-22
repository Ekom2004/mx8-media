# MX8 `find` v1

## Public API

MX8 adds a new optional root-level `find` field on `mx8.run(...)`:

```python
mx8.run(
    source="s3://raw-dashcam-archive/",
    find="a stop sign covered in heavy snow",
    transform=mx8.video.extract_frames(fps=10, format="jpg"),
    sink="s3://training-dataset/",
)
```

`find` is a selection stage, not a transform. `filter` stays reserved for structured filtering.

## v1 semantics

- `find` is optional.
- If `find` is omitted, behavior is unchanged.
- For video in v1, `find` selects matching temporal segments.
- `transform` runs only on those matched segments.
- If no matches are found, the job completes successfully with zero outputs.

## Architecture

`find` resolves before coordinator scheduling:

```text
API -> planner/finder -> derived manifest -> coordinator -> workers
```

Locked decisions:

- Workers remain dumb.
- Workers never receive the query or model config.
- The semantic model lives on the planner/control-plane side.
- The model runs in a separate internal inference service near the planner/coordinator, not inside the coordinator process itself.

## Data model

API-layer jobs store:

- `find`
- `matched_assets`
- `matched_segments`

Job status adds:

- `FINDING`

The planner resolves `find` into a derived manifest. Agent-facing jobs stay transform-only. Manifest records will later gain temporal segment bounds so workers can process selected clips without knowing semantic search happened upstream.

## v1 scope

v1 supports:

- `find + video + extract_frames(...)`

Out of scope for v1:

- public model knobs
- public sampling knobs
- image/audio `find`
- reranking
- scheduler changes

## PR sequencing

1. PR1: SDK/API/storage/status plumbing for `find`
2. PR2: manifest temporal segment support
3. PR3: planner/finder + internal inference service
4. PR4: worker support for temporal segments

## PR1 behavior

Until the planner exists, jobs with `find` enter `FINDING` and are intentionally not launched by the scaler. This is safer than silently ignoring the query.
