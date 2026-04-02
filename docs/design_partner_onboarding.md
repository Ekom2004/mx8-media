# MX8 Design Partner Onboarding

This is the current onboarding and support flow for design partners.

## 1. Provisioning

Every design partner should receive:

- a base API URL
- the canonical SDK shape: `mx8.run(input=..., work=[...], output=...)`
- the current support line from [support_matrix.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/support_matrix.md)

If API auth is enabled in the target environment, also provide:

- `MX8_API_BASE_URL`
- `MX8_API_KEY`

The public API uses bearer auth:

```http
Authorization: Bearer <token>
```

Each partner key should map to exactly one `account_id`. That account boundary controls which jobs they can create, list, and inspect.

If API auth is not enabled yet, access should still be provisioned through an isolated partner environment rather than a shared open endpoint.

## 2. First Job

Start every partner on a straightforward transform-only workflow.

Example image job:

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
```

Example video job:

```python
import mx8

job = mx8.run(
    input="s3://raw-video/",
    work=[
        mx8.proxy(),
    ],
    output="s3://review-proxies/",
)
```

## 3. What The Partner Gets Back

The critical support artifact is the `job_id`.

Once the job is submitted, both the partner and MX8 operators should be able to inspect:

- `status`
- `stage`
- `completed_objects`
- `completed_bytes`
- `outputs_written`
- `worker_pool`
- `region`
- `instance_type`
- `failure_category`
- `failure_message`
- recent `events`

That means the support loop can start from one identifier rather than log spelunking.

## 4. Support Flow

When a partner reports a problem:

1. they send the `job_id`
2. MX8 looks up `GET /v1/jobs/{job_id}`
3. MX8 checks `stage`, `failure_category`, `failure_message`, and `events`
4. MX8 decides whether the right next step is:
   - retry
   - rerun
   - input/config change
   - product bug fix

The operator should be able to answer these questions quickly:

- did the job fail in planning or execution?
- was it assigned a worker pool?
- were any outputs written?
- is the failure customer-actionable or MX8-actionable?

## 5. Support Contacts

Every design partner should know:

- where to send a failed `job_id`
- the expected support response window
- whether the workload is inside the supported line or in experimental territory

If a workflow is experimental, say that during onboarding rather than during the incident.

## 6. Validation Before Expanding Usage

Before moving a partner to larger or more expensive workloads, validate:

- one successful image transform run
- one successful video transform run
- one `proxy()` run if review copies matter
- one `remux()` run if repackaging matters
- job visibility from `job_id` alone

For now, real remote `s3 -> s3` validation should be treated as a readiness checklist item, not assumed from unit tests alone. Use [live_validation_checklist.md](/Users/ekomotu/Desktop/mx8-projects/mx8-media/docs/live_validation_checklist.md) as the execution checklist.
