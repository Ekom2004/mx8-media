# MX8 `find` v1

`find` currently exists as an experimental selection workflow for controlled testing. It is not part of the default public transform-first promise.

## Public API

MX8 exposes semantic selection as a `find` work item inside `mx8.run(...)`:

```python
mx8.run(
    input="s3://raw-dashcam-archive/",
    work=[
        mx8.find("a stop sign covered in heavy snow"),
        mx8.clip(),
    ],
    output="s3://review-clips/",
)
```

`find` is a selection stage, not a transform. It stays first in the ordered `work=[...]` list, ahead of the transform work that should run on matches.

## v1 semantics

- `mx8.find(...)` is optional.
- If no `find` work item is present, behavior is unchanged.
- For video in v1, `find` selects matching temporal segments.
- downstream work runs only on those matched segments.
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
- `manifest_hash`

Job status adds:

- `FINDING`
- `PLANNED`

The planner resolves `find` into a derived manifest. Agent-facing jobs stay transform-only. Manifest records will later gain temporal segment bounds so workers can process selected clips without knowing semantic search happened upstream.

## v1 scope

v1 supports:

- `find + video + clip()`
- `find + video + transcode(...)`
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

## PR3 behavior

Once the planner exists:

- zero-match jobs complete successfully with `matched_assets=0` and `matched_segments=0`
- matched jobs produce a derived manifest and move to `PLANNED`
- `PLANNED` jobs remain parked until PR4 adds segment-aware worker execution

## PR4 behavior

Once workers honor manifest segment bounds:

- the planner still writes a derived manifest and may briefly use `PLANNED`
- matched jobs immediately advance into the normal execution path after planning
- workers process only the selected temporal segments and never receive the semantic query
- segment-derived outputs include segment identity so multiple matches from the same source do not collide in the sink

## Operations

Operational deployment steps for the Modal-backed inference worker live in `docs/find_modal_deploy.md`.
