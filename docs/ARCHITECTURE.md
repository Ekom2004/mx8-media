# mx8-media — System Architecture v0

This document is the authoritative source of truth for `mx8-media` v0.

If any other doc in this repo conflicts with this file, `ARCHITECTURE.md` wins.

---

## 1. The Big Picture

`mx8-media` is divided into two planes that never overlap:

```
┌─────────────────────────────────────────────────────────────────┐
│                     CONTROL PLANE                               │
│           (Python — lightweight, always on)                     │
│                                                                  │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│   │  Python SDK  │   │  FastAPI     │   │  Postgres DB     │   │
│   │  (mx8-py)    │──▶│  (api/)      │──▶│  jobs, users,    │   │
│   └──────────────┘   └──────┬───────┘   │  billing         │   │
│                             │           └──────────────────┘   │
│                             │                                   │
│                    ┌────────▼────────┐                          │
│                    │  Scaler Daemon  │                          │
│                    │  (Python/boto3) │                          │
│                    └────────┬────────┘                          │
└─────────────────────────────│───────────────────────────────────┘
                              │ Launch / Terminate EC2 Spot
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATA PLANE                                 │
│           (Rust — ephemeral, spun up per job)                   │
│                                                                  │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │               mx8-coordinator (Rust binary)             │  │
│   │                                                          │  │
│   │  - Resolves manifest from source S3 URI                 │  │
│   │  - Assigns leases to agents                             │  │
│   │  - Tracks cursors (fault tolerance)                     │  │
│   │  - Reports progress back to API via webhook             │  │
│   └──────────────┬───────────────────────────────────────────┘  │
│                  │ HTTP Leases                                   │
│       ┌──────────┼──────────┐                                   │
│       ▼          ▼          ▼                                   │
│   ┌───────┐  ┌───────┐  ┌───────┐   ... (N workers)            │
│   │mx8d-  │  │mx8d-  │  │mx8d-  │                              │
│   │agent  │  │agent  │  │agent  │                              │
│   │       │  │       │  │       │                              │
│   │Fetch→ │  │Fetch→ │  │Fetch→ │                              │
│   │Decode→│  │Decode→│  │Decode→│                              │
│   │Trans→ │  │Trans→ │  │Trans→ │                              │
│   │Sink   │  │Sink   │  │Sink   │                              │
│   └───────┘  └───────┘  └───────┘                              │
│       │                                                         │
│       ▼                                                         │
│   [Customer S3 Destination Bucket]                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Existing Rust Crates — Roles in mx8-media

All 9 crates in `crates/` are already present. Here is exactly what each one does in the new architecture:

| Crate | Role | Change Needed? |
|---|---|---|
| `mx8-core` | Pipeline stages: Fetch, Decode, Transform, Sink | **Yes** — add Transform stage + S3 Sink |
| `mx8-coordinator` | Lease manager, assigns work ranges to agents | **Minor** — add webhook to report job completion to API |
| `mx8d-agent` | Worker daemon, runs the pipeline loop | **Minor** — read job config from coordinator, not Python |
| `mx8-manifest-store` | Hashes source URI → deterministic file list | **None** — works as-is |
| `mx8-snapshot` | Pins manifest state for reproducibility | **None** — works as-is |
| `mx8-wire` | HTTP types for coordinator ↔ agent comms | **None** — works as-is |
| `mx8-proto` | Protobuf definitions | **None** — works as-is |
| `mx8-observe` | Tracing / metrics | **None** — works as-is |
| `mx8-runtime` | The former PyTorch integration layer | **Remove** — replaced by SDK + FastAPI |

---

## 3. New Components to Build

### 3A. The Python SDK (`sdk/`)
A pip-installable Python package (`mx8`). 

Responsibilities:
- Accept `mx8.run(source, transform=..., sink=...)` calls
- Serialize job config to JSON
- POST to the FastAPI server
- Return a `Job` object with `.wait()` and `.poll()` methods

```
sdk/
├── mx8/
│   ├── __init__.py        # exposes mx8.run()
│   ├── job.py             # Job class with .wait(), .poll()
│   ├── transforms/
│   │   ├── video.py       # video.transcode(), video.resize(), etc.
│   │   ├── image.py       # image.resize(), image.crop(), image.convert()
│   │   └── audio.py       # audio.resample(), audio.normalize()
│   └── client.py          # HTTP client to talk to the API
└── pyproject.toml
```

### 3B. The FastAPI Server (`api/`)
A thin Python web server deployed on Fly.io or Render.

Responsibilities:
- Authenticate requests via API keys
- Validate job shape
- Insert job into Postgres as `PENDING`
- Trigger the coordinator to start (via subprocess or RPC)
- Expose `/v1/jobs`, `/v1/jobs/{id}`, `/v1/jobs/{id}/status`
- Receive completion webhooks from the coordinator
- Trigger Stripe charges on completion

```
api/
├── main.py                # FastAPI app entrypoint
├── routers/
│   ├── jobs.py            # POST /v1/jobs, GET /v1/jobs/{id}
│   └── webhooks.py        # POST /internal/job-complete (from coordinator)
├── models.py              # Postgres ORM (SQLAlchemy or raw psycopg2)
├── billing.py             # Stripe integration
└── scaler.py              # boto3 Spot instance management
```

### 3C. The Scaler (`api/scaler.py`)
A background thread inside the FastAPI process (or a separate cron job).

Responsibilities:
- Poll Postgres every 60 seconds
- If `QUEUED` jobs exist with no running compute → launch Spot instances
- Detect which AWS region the source S3 bucket lives in → launch compute in same region
- Try instance types in priority order: `g4dn.xlarge` → `g4dn.2xlarge` → `g5.xlarge`
- When Postgres shows zero `RUNNING` or `QUEUED` jobs → terminate idle instances

### 3D. The Transform Stage (inside `mx8-core`)
The current `mx8-core` pipeline ends at `Deliver` (yielding samples to a Python consumer).

In mx8-media, `Deliver` is replaced by two new stages:

1. **`Transform`** — applies the user-specified operation to the decoded bytes:
   - `ffmpeg-next` for all video and audio ops (NVENC/NVDEC on GPU, software on CPU)
   - `image` crate for image resize/convert
2. **`Sink`** — uploads the transformed bytes directly to the destination S3 URI using a multipart upload stream. Uses the `aws-sdk-s3` crate already in workspace dependencies.

---

## 4. Data Flow (End to End)

```
1. Customer calls:
   mx8.run("s3://raw/", transform=video.extract_frames(fps=1), sink="s3://out/")

2. SDK → POST /v1/jobs with JSON payload → FastAPI

3. FastAPI:
   - Validates auth
   - Inserts job into Postgres (status=PENDING)
   - Returns job_id to SDK

4. Scaler (background):
   - Sees PENDING job
   - Detects source bucket region (us-east-1)
   - Launches 10x g4dn.xlarge Spot in us-east-1 via boto3
   - Each instance boots, downloads mx8-coordinator + mx8d-agent binaries from S3
   - mx8-coordinator starts, builds manifest from source S3 URI
   - Job status → QUEUED

5. mx8d-agent (on each Spot instance):
   - Requests a lease from coordinator (e.g., files 0-50)
   - For each file in lease:
     a. Fetch: streams bytes from source S3 (byte-range aware)
     b. Decode: decodes video frames using libav (NVDEC on GPU)
     c. Transform: extracts 1 frame/sec as JPEG using ffmpeg-next
     d. Sink: uploads each JPEG to destination S3 via multipart
     e. Cursor advances in coordinator
   - Reports progress to coordinator

6. mx8-coordinator:
   - Tracks all cursors
   - Reissues leases for any dead workers automatically
   - When all files processed → sends webhook to FastAPI:
     POST /internal/job-complete { job_id, total_bytes_processed }

7. FastAPI:
   - Marks job COMPLETE in Postgres
   - Calculates cost: total_bytes_gb × $0.05
   - Charges customer via Stripe
   - Scaler terminates Spot instances

8. Customer polls:
   job.wait()  # returns when job is COMPLETE
```

---

## 5. Job Config (Coordinator ↔ Agent Protocol)

The coordinator passes the job config to agents via a `JobSpec` struct in `mx8-wire`:

```rust
// mx8-wire/src/lib.rs (new addition)
pub struct JobSpec {
    pub job_id: String,
    pub source_uri: String,       // s3://bucket/prefix/
    pub sink_uri: String,         // s3://bucket/output/
    pub transforms: Vec<Transform>,
    pub aws_region: String,
}

pub enum Transform {
    VideoTranscode { codec: Codec, crf: u8 },
    VideoResize { width: u32, height: u32, maintain_aspect: bool },
    VideoExtractFrames { fps: f32, format: ImageFormat },
    VideoExtractAudio { format: AudioFormat, bitrate: String },
    ImageResize { width: u32, height: u32, maintain_aspect: bool },
    ImageCrop { width: u32, height: u32 },
    ImageConvert { format: ImageFormat, quality: u8 },
    AudioResample { rate: u32, channels: u8 },
    AudioNormalize { loudness_lufs: f32 },
}
```

---

## 6. Deployment Topology

```
Fly.io / Render (always on, cheap):
├── FastAPI server (1 instance, auto-scales to zero at night)
├── Postgres (managed, 1 GB starter)
└── Scaler thread (runs inside FastAPI process)

AWS (ephemeral, spun up per job):
├── mx8-coordinator (runs on 1 t3.medium On-Demand per job)
└── mx8d-agent × N (runs on g4dn.xlarge Spot, N based on job size)
    └── Same region as customer's S3 bucket (zero egress)
```

---

## 7. What to Build First (Sequence)

```
Week 1:  Add Transform stage to mx8-core (ffmpeg-next for video.extract_frames)
Week 2:  Add S3 Sink stage to mx8-core (replaces Deliver, uses aws-sdk-s3)
Week 3:  Add JobSpec to mx8-wire, wire into coordinator + agent
Week 4:  Build FastAPI server (job CRUD, auth, Postgres)
Week 5:  Build Python SDK (mx8.run(), Job.wait())
Week 6:  Build Scaler (boto3 Spot launch/terminate)
Week 7:  End-to-end integration test on real S3 data
Week 8:  Stripe billing webhook
Week 9:  Internal demo. Find first design partner.
```
