#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tonic::transport::Channel;
use tracing::{info, info_span, warn, Instrument};

use mx8_proto::v0::coordinator_client::CoordinatorClient;
use mx8_proto::v0::GetManifestRequest;
use mx8_proto::v0::HeartbeatRequest;
use mx8_proto::v0::Lease;
use mx8_proto::v0::NodeCaps;
use mx8_proto::v0::NodeStats;
use mx8_proto::v0::RegisterNodeRequest;
use mx8_proto::v0::ReportProgressRequest;
use mx8_proto::v0::RequestLeaseRequest;

use mx8_core::types::{
    normalize_audio_format, normalize_frame_format, normalize_image_format, normalize_video_codec,
    JobSpec as CoreJobSpec, ManifestRecord, TransformSpec,
};
use mx8_observe::metrics::{Counter, Gauge};
use mx8_wire::TryToCore;

use image::codecs::jpeg::JpegEncoder;
use image::imageops::FilterType;
use image::{DynamicImage, ImageFormat};
use mx8_runtime::pipeline::{Pipeline, RuntimeCaps};
use mx8_runtime::sink::Sink;
use mx8_runtime::types::Batch;
use webp_rust::ImageBuffer as WebpImageBuffer;
use webp_rust::{encode_lossless as encode_lossless_webp, encode_lossy as encode_lossy_webp};

#[cfg(feature = "s3")]
use aws_sdk_s3::primitives::ByteStream;
#[cfg(feature = "azure")]
use azure_storage_blobs::prelude::ClientBuilder as AzureClientBuilder;
#[cfg(feature = "gcs")]
use google_cloud_storage::client::Client as GcsClient;
#[cfg(feature = "gcs")]
use google_cloud_storage::http::objects::upload::{Media, UploadObjectRequest, UploadType};

const DEFAULT_GRPC_MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
static SEEKABLE_INPUT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "s3")]
type S3Client = aws_sdk_s3::Client;
#[cfg(feature = "gcs")]
type GcsStorageClient = GcsClient;
#[cfg(feature = "azure")]
type AzureStorageClientBuilder = AzureClientBuilder;

#[derive(Debug)]
struct TransformProcessLimiter {
    max: usize,
    state: Mutex<usize>,
    cv: Condvar,
}

impl TransformProcessLimiter {
    fn new(max: usize) -> Self {
        Self {
            max: std::cmp::max(1, max),
            state: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> Result<TransformProcessPermit<'_>> {
        let mut active = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("transform limiter lock poisoned"))?;
        while *active >= self.max {
            active = self
                .cv
                .wait(active)
                .map_err(|_| anyhow::anyhow!("transform limiter wait poisoned"))?;
        }
        *active += 1;
        Ok(TransformProcessPermit { limiter: self })
    }
}

#[derive(Debug)]
struct TransformProcessPermit<'a> {
    limiter: &'a TransformProcessLimiter,
}

impl Drop for TransformProcessPermit<'_> {
    fn drop(&mut self) {
        if let Ok(mut active) = self.limiter.state.lock() {
            *active = active.saturating_sub(1);
            self.limiter.cv.notify_one();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AudioStreamMetadata {
    sample_rate: u32,
    channels: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SampleExecutionPlan {
    source_location: String,
    segment_start_ms: Option<u64>,
    segment_end_ms: Option<u64>,
}

impl SampleExecutionPlan {
    fn full(source_location: &str) -> Self {
        Self {
            source_location: source_location.to_string(),
            segment_start_ms: None,
            segment_end_ms: None,
        }
    }

    fn from_record(record: &ManifestRecord) -> Self {
        Self {
            source_location: record.location.clone(),
            segment_start_ms: record.segment_start_ms,
            segment_end_ms: record.segment_end_ms,
        }
    }

    fn segment_label(&self, sample_id: u64) -> Option<String> {
        match (self.segment_start_ms, self.segment_end_ms) {
            (Some(start_ms), Some(end_ms)) => {
                Some(format!("segment_{sample_id:06}_{start_ms}_{end_ms}"))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Parser, Clone)]
#[command(name = "mx8d-agent")]
struct Args {
    /// Coordinator address, e.g. http://127.0.0.1:50051
    #[arg(long, env = "MX8_COORD_URL", default_value = "http://127.0.0.1:50051")]
    coord_url: String,

    #[arg(long, env = "MX8_JOB_ID", default_value = "local-job")]
    job_id: String,

    #[arg(long, env = "MX8_NODE_ID", default_value = "local-node")]
    node_id: String,

    /// Optional: periodically emit a metrics snapshot to logs.
    #[arg(long, env = "MX8_METRICS_SNAPSHOT_INTERVAL_MS", default_value_t = 0)]
    metrics_snapshot_interval_ms: u64,

    /// Development-only: request leases continuously with this `want` value.
    /// Set to 0 to disable.
    #[arg(long, env = "MX8_DEV_LEASE_WANT", default_value_t = 0)]
    dev_lease_want: u32,

    #[arg(long, env = "MX8_BATCH_SIZE_SAMPLES", default_value_t = 512)]
    batch_size_samples: usize,

    #[arg(long, env = "MX8_PREFETCH_BATCHES", default_value_t = 1)]
    prefetch_batches: usize,

    #[arg(long, env = "MX8_MAX_QUEUE_BATCHES", default_value_t = 64)]
    max_queue_batches: usize,

    #[arg(long, env = "MX8_MAX_INFLIGHT_BYTES", default_value_t = 128 * 1024 * 1024)]
    max_inflight_bytes: u64,

    #[arg(long, env = "MX8_TARGET_BATCH_BYTES")]
    target_batch_bytes: Option<u64>,

    #[arg(long, env = "MX8_MAX_BATCH_BYTES")]
    max_batch_bytes: Option<u64>,

    #[arg(long, env = "MX8_MAX_PROCESS_RSS_BYTES", hide = true)]
    max_process_rss_bytes: Option<u64>,

    /// Artificially slow down delivery to prove backpressure + cursor semantics.
    #[arg(long, env = "MX8_SINK_SLEEP_MS", default_value_t = 0)]
    sink_sleep_ms: u64,

    /// How often to report progress while executing a lease.
    #[arg(long, env = "MX8_PROGRESS_INTERVAL_MS", default_value_t = 500)]
    progress_interval_ms: u64,

    /// Max concurrent ffmpeg/media transform subprocesses within this agent.
    #[arg(long, env = "MX8_MAX_TRANSFORM_PROCESSES", default_value_t = 4)]
    max_transform_processes: usize,

    /// gRPC max message size (both decode/encode) for manifest proxying and future APIs.
    #[arg(
        long,
        env = "MX8_GRPC_MAX_MESSAGE_BYTES",
        default_value_t = DEFAULT_GRPC_MAX_MESSAGE_BYTES
    )]
    grpc_max_message_bytes: usize,
}

#[derive(Debug, Default)]
struct AgentMetrics {
    register_total: Counter,
    heartbeat_total: Counter,
    heartbeat_ok_total: Counter,
    heartbeat_err_total: Counter,
    inflight_bytes: Gauge,
    ram_high_water_bytes: Gauge,
    active_lease_tasks: Gauge,
    leases_started_total: Counter,
    leases_completed_total: Counter,
    delivered_samples_total: Counter,
    delivered_bytes_total: Counter,
    manifest_direct_stream_used_total: Counter,
    manifest_direct_stream_fallback_total: Counter,
    manifest_stream_truncated_total: Counter,
    manifest_schema_mismatch_total: Counter,
}

impl AgentMetrics {
    fn snapshot(&self, job_id: &str, node_id: &str, manifest_hash: &str) {
        tracing::info!(
            target: "mx8_metrics",
            job_id = %job_id,
            node_id = %node_id,
            manifest_hash = %manifest_hash,
            register_total = self.register_total.get(),
            heartbeat_total = self.heartbeat_total.get(),
            heartbeat_ok_total = self.heartbeat_ok_total.get(),
            heartbeat_err_total = self.heartbeat_err_total.get(),
            inflight_bytes = self.inflight_bytes.get(),
            ram_high_water_bytes = self.ram_high_water_bytes.get(),
            active_lease_tasks = self.active_lease_tasks.get(),
            leases_started_total = self.leases_started_total.get(),
            leases_completed_total = self.leases_completed_total.get(),
            delivered_samples_total = self.delivered_samples_total.get(),
            delivered_bytes_total = self.delivered_bytes_total.get(),
            manifest_direct_stream_used_total = self.manifest_direct_stream_used_total.get(),
            manifest_direct_stream_fallback_total = self.manifest_direct_stream_fallback_total.get(),
            manifest_stream_truncated_total = self.manifest_stream_truncated_total.get(),
            manifest_schema_mismatch_total = self.manifest_schema_mismatch_total.get(),
            "metrics"
        );
    }
}

fn atomic_fetch_max_u64(dst: &AtomicU64, value: u64) {
    let mut cur = dst.load(Ordering::Relaxed);
    while cur < value {
        match dst.compare_exchange(cur, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => cur = next,
        }
    }
}

#[derive(Debug)]
struct LeaseProgress {
    start_id: u64,
    end_id: u64,
    cursor: AtomicU64,
    delivered_samples: AtomicU64,
    delivered_bytes: AtomicU64,
}

impl LeaseProgress {
    fn new(start_id: u64, end_id: u64) -> Self {
        Self {
            start_id,
            end_id,
            cursor: AtomicU64::new(start_id),
            delivered_samples: AtomicU64::new(0),
            delivered_bytes: AtomicU64::new(0),
        }
    }

    fn cursor(&self) -> u64 {
        self.cursor.load(Ordering::Acquire)
    }

    fn delivered_samples(&self) -> u64 {
        self.delivered_samples.load(Ordering::Relaxed)
    }

    fn delivered_bytes(&self) -> u64 {
        self.delivered_bytes.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
struct LeaseProgressSink {
    progress: Arc<LeaseProgress>,
    sleep: Duration,
    cancelled: Arc<AtomicBool>,
}

impl Sink for LeaseProgressSink {
    fn deliver(&self, batch: Batch) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("lease revoked by coordinator");
        }
        if !self.sleep.is_zero() {
            std::thread::sleep(self.sleep);
        }
        if self.cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("lease revoked by coordinator");
        }

        self.progress
            .delivered_samples
            .fetch_add(batch.sample_count() as u64, Ordering::Relaxed);
        self.progress
            .delivered_bytes
            .fetch_add(batch.payload_len() as u64, Ordering::Relaxed);

        let max_id = batch
            .sample_ids
            .iter()
            .copied()
            .max()
            .unwrap_or(self.progress.start_id);
        let next_cursor = max_id
            .saturating_add(1)
            .clamp(self.progress.start_id, self.progress.end_id);
        atomic_fetch_max_u64(&self.progress.cursor, next_cursor);
        Ok(())
    }
}

struct JobSpecSink {
    progress: Arc<LeaseProgress>,
    sleep: Duration,
    cancelled: Arc<AtomicBool>,
    job_spec: Arc<CoreJobSpec>,
    sample_plans: Arc<HashMap<u64, SampleExecutionPlan>>,
    transform_slots: Arc<TransformProcessLimiter>,
    #[cfg(feature = "s3")]
    s3_client: Arc<S3Client>,
    #[cfg(feature = "gcs")]
    gcs_client: Arc<GcsStorageClient>,
    #[cfg(feature = "azure")]
    azure_client_builder: Arc<AzureStorageClientBuilder>,
}

#[derive(Debug)]
struct SinkArtifact {
    output_uri: String,
    payload: Vec<u8>,
}

impl Sink for JobSpecSink {
    fn deliver(&self, batch: Batch) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("lease revoked by coordinator");
        }
        if !self.sleep.is_zero() {
            std::thread::sleep(self.sleep);
        }
        if self.cancelled.load(Ordering::Relaxed) {
            anyhow::bail!("lease revoked by coordinator");
        }

        validate_sink_uri(&self.job_spec.sink_uri)?;

        let mut first_output_uri: Option<String> = None;
        for (idx, sample_id) in batch.sample_ids.iter().copied().enumerate() {
            let sample_plan = self.sample_plans.get(&sample_id).ok_or_else(|| {
                anyhow::anyhow!("missing sample execution plan for sample_id={sample_id}")
            })?;
            let payload = sample_payload_bytes(&batch, idx)?;
            let artifacts = render_outputs_for_sink_with_plan(
                &self.job_spec,
                sample_plan,
                sample_id,
                payload,
                Some(&self.transform_slots),
            )?;
            for artifact in artifacts {
                self.write_output(&artifact.output_uri, &artifact.payload)?;
                if first_output_uri.is_none() {
                    first_output_uri = Some(artifact.output_uri);
                }
            }
        }

        tracing::debug!(
            target: "mx8_proof",
            event = "sink_batch_planned",
            sink_uri = %self.job_spec.sink_uri,
            batch_samples = batch.sample_count() as u64,
            first_output_uri = first_output_uri.as_deref().unwrap_or(""),
            transform_count = self.job_spec.transforms.len() as u64,
            "planned sink outputs for batch"
        );

        self.progress
            .delivered_samples
            .fetch_add(batch.sample_count() as u64, Ordering::Relaxed);
        self.progress
            .delivered_bytes
            .fetch_add(batch.payload_len() as u64, Ordering::Relaxed);

        let max_id = batch
            .sample_ids
            .iter()
            .copied()
            .max()
            .unwrap_or(self.progress.start_id);
        let next_cursor = max_id
            .saturating_add(1)
            .clamp(self.progress.start_id, self.progress.end_id);
        atomic_fetch_max_u64(&self.progress.cursor, next_cursor);
        Ok(())
    }
}

impl JobSpecSink {
    fn write_output(&self, output_uri: &str, payload: &[u8]) -> Result<()> {
        if output_uri.starts_with("file://") {
            let path = parse_file_uri(output_uri)?;
            write_file_output(&path, payload)?;
            return Ok(());
        }

        if output_uri.starts_with("s3://") {
            #[cfg(feature = "s3")]
            {
                let (bucket, key) = parse_s3_uri(output_uri)?;
                let body = ByteStream::from(payload.to_vec());
                let client = self.s3_client.clone();
                return tokio::runtime::Handle::current().block_on(async move {
                    client
                        .put_object()
                        .bucket(bucket)
                        .key(key)
                        .body(body)
                        .send()
                        .await
                        .map_err(|err| anyhow::anyhow!("s3 put_object failed: {err}"))?;
                    Ok(())
                });
            }

            #[cfg(not(feature = "s3"))]
            {
                let _ = (output_uri, payload);
                anyhow::bail!("s3 sink requested but mx8d-agent was built without feature 's3'")
            }
        }

        if output_uri.starts_with("gs://") {
            #[cfg(feature = "gcs")]
            {
                let (bucket, key) = parse_gcs_uri(output_uri)?;
                let client = self.gcs_client.clone();
                let upload_type = UploadType::Simple(Media::new(key.clone()));
                let request = UploadObjectRequest {
                    bucket,
                    ..Default::default()
                };
                return tokio::runtime::Handle::current().block_on(async move {
                    client
                        .upload_object(&request, payload.to_vec(), &upload_type)
                        .await
                        .map_err(|err| anyhow::anyhow!("gcs upload_object failed: {err:?}"))?;
                    Ok(())
                });
            }

            #[cfg(not(feature = "gcs"))]
            {
                let _ = (output_uri, payload);
                anyhow::bail!("gs sink requested but mx8d-agent was built without feature 'gcs'")
            }
        }

        if output_uri.starts_with("az://") || output_uri.starts_with("azure://") {
            #[cfg(feature = "azure")]
            {
                let (container, blob) = parse_azure_uri(output_uri)?;
                let builder = self.azure_client_builder.clone();
                return tokio::runtime::Handle::current().block_on(async move {
                    builder
                        .as_ref()
                        .clone()
                        .blob_client(container, blob)
                        .put_block_blob(payload.to_vec())
                        .await
                        .map_err(|err| anyhow::anyhow!("azure put_block_blob failed: {err:?}"))?;
                    Ok(())
                });
            }

            #[cfg(not(feature = "azure"))]
            {
                let _ = (output_uri, payload);
                anyhow::bail!(
                    "azure sink requested but mx8d-agent was built without feature 'azure'"
                )
            }
        }

        anyhow::bail!("unsupported output uri scheme: {output_uri}")
    }
}

fn validate_sink_uri(sink_uri: &str) -> Result<()> {
    if sink_uri.starts_with("s3://")
        || sink_uri.starts_with("gs://")
        || sink_uri.starts_with("az://")
        || sink_uri.starts_with("azure://")
        || sink_uri.starts_with("file://")
    {
        return Ok(());
    }
    anyhow::bail!(
        "unsupported sink_uri scheme (expected s3://, gs://, az://, azure://, or file://): {sink_uri}"
    );
}

#[cfg(feature = "s3")]
fn parse_s3_uri(uri: &str) -> Result<(&str, &str)> {
    let raw = uri
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("expected s3:// uri, got {uri}"))?;
    let (bucket, key) = raw
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("s3 uri missing object key: {uri}"))?;
    if bucket.trim().is_empty() || key.trim().is_empty() {
        anyhow::bail!("invalid s3 uri: {uri}");
    }
    Ok((bucket, key))
}

fn parse_file_uri(uri: &str) -> Result<PathBuf> {
    let raw = uri
        .strip_prefix("file://")
        .ok_or_else(|| anyhow::anyhow!("expected file:// uri, got {uri}"))?;
    if raw.trim().is_empty() {
        anyhow::bail!("file uri missing path: {uri}");
    }
    Ok(PathBuf::from(raw))
}

#[cfg(feature = "gcs")]
fn parse_gcs_uri(uri: &str) -> Result<(String, String)> {
    let raw = uri
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow::anyhow!("expected gs:// uri, got {uri}"))?;
    let (bucket, key) = raw
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("gs uri missing object key: {uri}"))?;
    if bucket.trim().is_empty() || key.trim().is_empty() {
        anyhow::bail!("invalid gs uri: {uri}");
    }
    Ok((bucket.trim().to_string(), key.trim().to_string()))
}

#[cfg(feature = "azure")]
fn parse_azure_uri(uri: &str) -> Result<(&str, &str)> {
    let raw = uri
        .strip_prefix("az://")
        .or_else(|| uri.strip_prefix("azure://"))
        .ok_or_else(|| anyhow::anyhow!("expected az:// or azure:// uri, got {uri}"))?;
    let (container, blob) = raw
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("azure uri missing blob path: {uri}"))?;
    if container.trim().is_empty() || blob.trim().is_empty() {
        anyhow::bail!("invalid azure uri: {uri}");
    }
    Ok((container.trim(), blob.trim()))
}

fn write_file_output(path: &Path, payload: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            anyhow::anyhow!(
                "failed to create output directory {}: {err}",
                parent.display()
            )
        })?;
    }
    std::fs::write(path, payload)
        .map_err(|err| anyhow::anyhow!("failed to write output file {}: {err}", path.display()))
}

fn source_matches_job(source_uri: &str, record: &ManifestRecord) -> bool {
    let normalized_source_uri = normalize_storage_scheme(source_uri);
    let normalized_record_location = normalize_storage_scheme(&record.location);

    if let Some(source_path) = local_source_path(normalized_source_uri.as_str()) {
        if let Some(record_path) = local_source_path(&record.location) {
            let source_is_dir = normalized_source_uri.ends_with('/');
            return if source_is_dir {
                record_path.starts_with(&source_path)
            } else {
                record_path == source_path
            };
        }
    }

    let normalized = normalized_source_uri.trim_end_matches('/');
    if normalized.is_empty() {
        return true;
    }
    if normalized_source_uri.ends_with('/') {
        normalized_record_location.starts_with(normalized_source_uri.as_str())
            || normalized_record_location == normalized
    } else {
        normalized_record_location == normalized
    }
}

fn normalize_storage_scheme(uri: &str) -> String {
    if let Some(rest) = uri.strip_prefix("azure://") {
        return format!("az://{rest}");
    }
    uri.to_string()
}

fn local_source_path(location: &str) -> Option<PathBuf> {
    let path = if let Some(raw) = location.strip_prefix("file://") {
        PathBuf::from(raw)
    } else if location.starts_with('/') {
        PathBuf::from(location)
    } else {
        return None;
    };
    std::fs::canonicalize(path).ok()
}

fn build_sample_plans(
    records: &[ManifestRecord],
    job_spec: Option<&CoreJobSpec>,
) -> Result<HashMap<u64, SampleExecutionPlan>> {
    let mut plans = HashMap::with_capacity(records.len());
    for record in records {
        if let Some(spec) = job_spec {
            if !source_matches_job(&spec.source_uri, record) {
                anyhow::bail!(
                    "manifest record location {} is outside configured source_uri {}",
                    record.location,
                    spec.source_uri
                );
            }
        }
        plans.insert(record.sample_id, SampleExecutionPlan::from_record(record));
    }
    Ok(plans)
}

fn plan_output_uri_with_plan(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    sample_id: u64,
) -> Result<String> {
    validate_sink_uri(&job_spec.sink_uri)?;
    let base_name = source_basename_without_extension(&sample_plan.source_location)
        .unwrap_or_else(|| format!("sample_{sample_id}"));
    let transform_tag = transform_tag(&job_spec.transforms);
    let extension = output_extension(&job_spec.transforms, &sample_plan.source_location);
    let stem = if let Some(segment_label) = sample_plan.segment_label(sample_id) {
        format!("{base_name}_{segment_label}_{transform_tag}")
    } else {
        format!("{base_name}_{transform_tag}")
    };
    Ok(format!(
        "{}/{}.{}",
        job_spec.sink_uri.trim_end_matches('/'),
        stem,
        extension
    ))
}

fn plan_frame_output_uri_with_plan(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    sample_id: u64,
    frame_index: usize,
) -> Result<String> {
    validate_sink_uri(&job_spec.sink_uri)?;
    let base_name = source_basename_without_extension(&sample_plan.source_location)
        .unwrap_or_else(|| format!("sample_{sample_id}"));
    let extension = output_extension(&job_spec.transforms, &sample_plan.source_location);
    let stem = if let Some(segment_label) = sample_plan.segment_label(sample_id) {
        format!("{base_name}_{segment_label}_frames_{frame_index:06}")
    } else {
        format!("{base_name}_frames_{frame_index:06}")
    };
    Ok(format!(
        "{}/{}.{}",
        job_spec.sink_uri.trim_end_matches('/'),
        stem,
        extension
    ))
}

fn sample_payload_bytes<'a>(batch: &'a Batch, sample_index: usize) -> Result<&'a [u8]> {
    let start = usize::try_from(*batch.offsets.get(sample_index).ok_or_else(|| {
        anyhow::anyhow!("missing batch offset start for sample_index={sample_index}")
    })?)
    .map_err(|_| anyhow::anyhow!("batch offset start overflow for sample_index={sample_index}"))?;
    let end = usize::try_from(*batch.offsets.get(sample_index + 1).ok_or_else(|| {
        anyhow::anyhow!("missing batch offset end for sample_index={sample_index}")
    })?)
    .map_err(|_| anyhow::anyhow!("batch offset end overflow for sample_index={sample_index}"))?;
    if end < start || end > batch.payload.len() {
        anyhow::bail!(
            "invalid batch payload bounds for sample_index={sample_index}: start={start} end={end} payload_len={}",
            batch.payload.len()
        );
    }
    Ok(&batch.payload[start..end])
}

#[cfg(test)]
fn transform_payload_for_sink(
    job_spec: &CoreJobSpec,
    source_location: &str,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let sample_plan = SampleExecutionPlan::full(source_location);
    transform_payload_for_sink_with_slots(job_spec, &sample_plan, payload, None)
}

fn transform_payload_for_sink_with_slots(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<u8>> {
    let source_location = sample_plan.source_location.as_str();
    job_spec.validate()?;

    if job_spec.transforms.is_empty() {
        return Ok(payload.to_vec());
    }

    if video_extract_audio_spec(&job_spec.transforms).is_some() {
        return transform_video_extract_audio_chain_payload_for_sink(
            job_spec,
            sample_plan,
            payload,
            transform_slots,
        );
    }

    if job_spec.transforms.iter().all(is_image_transform) {
        return transform_image_payload_for_sink(job_spec, source_location, payload);
    }

    if job_spec.transforms.iter().all(is_video_transform) {
        return transform_video_payload_for_sink(job_spec, sample_plan, payload, transform_slots);
    }

    if job_spec.transforms.iter().all(is_audio_transform) {
        return transform_audio_payload_for_sink(
            job_spec,
            source_location,
            payload,
            transform_slots,
        );
    }

    anyhow::bail!(
        "mixed-media or unsupported transforms are not implemented in the worker right now"
    );
}

fn render_outputs_for_sink(
    job_spec: &CoreJobSpec,
    source_location: &str,
    sample_id: u64,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<SinkArtifact>> {
    let sample_plan = SampleExecutionPlan::full(source_location);
    render_outputs_for_sink_with_plan(job_spec, &sample_plan, sample_id, payload, transform_slots)
}

fn render_outputs_for_sink_with_plan(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    sample_id: u64,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<SinkArtifact>> {
    job_spec.validate()?;

    if video_extract_frames_spec(&job_spec.transforms).is_some() {
        return render_video_extract_frame_chain_outputs_for_sink(
            job_spec,
            sample_plan,
            sample_id,
            payload,
            transform_slots,
        );
    }

    let output_uri = plan_output_uri_with_plan(job_spec, sample_plan, sample_id)?;
    let transformed =
        transform_payload_for_sink_with_slots(job_spec, sample_plan, payload, transform_slots)?;
    Ok(vec![SinkArtifact {
        output_uri,
        payload: transformed,
    }])
}

fn is_image_transform(transform: &TransformSpec) -> bool {
    matches!(
        transform,
        TransformSpec::ImageResize { .. }
            | TransformSpec::ImageCrop { .. }
            | TransformSpec::ImageConvert { .. }
            | TransformSpec::ImageFilter { .. }
    )
}

fn is_video_transform(transform: &TransformSpec) -> bool {
    matches!(
        transform,
        TransformSpec::VideoResize { .. }
            | TransformSpec::VideoTranscode { .. }
            | TransformSpec::VideoExtractFrames { .. }
            | TransformSpec::VideoExtractAudio { .. }
            | TransformSpec::VideoFilter { .. }
    )
}

fn is_audio_transform(transform: &TransformSpec) -> bool {
    matches!(
        transform,
        TransformSpec::AudioResample { .. }
            | TransformSpec::AudioNormalize { .. }
            | TransformSpec::AudioFilter { .. }
    )
}

fn transform_image_payload_for_sink(
    job_spec: &CoreJobSpec,
    source_location: &str,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let mut image = image::load_from_memory(payload)
        .map_err(|err| anyhow::anyhow!("image decode failed for {source_location}: {err}"))?;

    for transform in &job_spec.transforms {
        match transform {
            TransformSpec::ImageResize {
                width,
                height,
                maintain_aspect,
            } => {
                image = if *maintain_aspect {
                    image.resize(*width, *height, FilterType::Lanczos3)
                } else {
                    image.resize_exact(*width, *height, FilterType::Lanczos3)
                };
            }
            TransformSpec::ImageCrop { width, height } => {
                let current_width = image.width();
                let current_height = image.height();
                if *width > current_width || *height > current_height {
                    anyhow::bail!(
                        "image crop {}x{} exceeds current image dimensions {}x{} for {source_location}",
                        width,
                        height,
                        current_width,
                        current_height
                    );
                }
                let x = (current_width - *width) / 2;
                let y = (current_height - *height) / 2;
                image = image.crop_imm(x, y, *width, *height);
            }
            TransformSpec::ImageConvert { .. } => {}
            TransformSpec::ImageFilter { .. } => continue,
            _ => anyhow::bail!("unsupported transform in image pipeline"),
        }
    }

    encode_transformed_image(
        &image,
        output_extension(&job_spec.transforms, source_location).as_str(),
        image_convert_quality(&job_spec.transforms),
    )
}

fn transform_video_payload_for_sink(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<u8>> {
    let source_location = sample_plan.source_location.as_str();
    if video_extract_audio_spec(&job_spec.transforms).is_some() {
        return transform_video_extract_audio_chain_payload_for_sink(
            job_spec,
            sample_plan,
            payload,
            transform_slots,
        );
    }

    let mut command = Command::new("ffmpeg");
    command.arg("-hide_banner").arg("-loglevel").arg("error");
    append_segment_window_args(&mut command, sample_plan)?;

    if let Some(filter_chain) = video_filter_chain(&job_spec.transforms) {
        command.arg("-vf").arg(filter_chain);
    }

    let codec = video_output_codec(&job_spec.transforms).unwrap_or("h264");
    let codec_arg = ffmpeg_video_codec(codec)?;
    let crf = video_output_crf(&job_spec.transforms).unwrap_or(23);
    command
        .arg("-c:v")
        .arg(codec_arg)
        .arg("-crf")
        .arg(crf.to_string())
        .arg("-c:a")
        .arg("copy")
        .arg("-movflags")
        .arg("frag_keyframe+empty_moov")
        .arg("-f")
        .arg("mp4")
        .arg("pipe:1");

    run_ffmpeg_stdout(
        command,
        payload,
        source_location,
        "video transform",
        transform_slots,
        FfmpegInputMode::SeekableFile,
    )
}

fn transform_audio_payload_for_sink(
    job_spec: &CoreJobSpec,
    source_location: &str,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<u8>> {
    let output_format = audio_output_format(source_location)?;
    let input_metadata = probe_audio_stream_metadata(payload, source_location)?;
    let mut command = Command::new("ffmpeg");
    command
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-vn")
        .arg("-sn")
        .arg("-dn");

    if let Some(filter_chain) = audio_filter_chain(&job_spec.transforms) {
        command.arg("-af").arg(filter_chain);
    }

    let rate = audio_output_rate(&job_spec.transforms).unwrap_or(input_metadata.sample_rate);
    command.arg("-ar").arg(rate.to_string());

    let channels = audio_output_channels(&job_spec.transforms).unwrap_or(input_metadata.channels);
    command.arg("-ac").arg(channels.to_string());

    let codec = ffmpeg_audio_codec(output_format)?;
    command.arg("-c:a").arg(codec);
    if output_format == "mp3" {
        command.arg("-b:a").arg("192k");
    }
    command
        .arg("-f")
        .arg(audio_output_muxer(output_format))
        .arg("pipe:1");

    run_ffmpeg_stdout(
        command,
        payload,
        source_location,
        "audio transform",
        transform_slots,
        FfmpegInputMode::PipeStdin,
    )
}

fn transform_video_extract_audio_chain_payload_for_sink(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<u8>> {
    let (extract_index, extracted_format) = video_extract_audio_stage(&job_spec.transforms)?;
    if extract_index != 0 {
        anyhow::bail!("video.extract_audio must be the first transform in the chain");
    }

    let extracted =
        extract_audio_payload_for_sink(job_spec, sample_plan, payload, transform_slots)?;
    let audio_tail = &job_spec.transforms[extract_index + 1..];
    if audio_tail.is_empty() {
        return Ok(extracted);
    }

    let chained_spec = job_spec_with_transforms(job_spec, audio_tail.to_vec());
    let chained_source =
        synthesized_media_location(sample_plan.source_location.as_str(), extracted_format);
    transform_audio_payload_for_sink(&chained_spec, &chained_source, &extracted, transform_slots)
}

fn extract_audio_payload_for_sink(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<u8>> {
    let source_location = sample_plan.source_location.as_str();
    let (format, bitrate) = match video_extract_audio_spec(&job_spec.transforms) {
        Some(TransformSpec::VideoExtractAudio { format, bitrate }) => {
            (format.as_str(), bitrate.as_str())
        }
        _ => anyhow::bail!("missing video.extract_audio transform"),
    };

    let mut command = Command::new("ffmpeg");
    command.arg("-hide_banner").arg("-loglevel").arg("error");
    append_segment_window_args(&mut command, sample_plan)?;
    command.arg("-vn");

    let codec = ffmpeg_audio_codec(format)?;
    command.arg("-c:a").arg(codec);
    if normalize_audio_format(format) == Some("mp3") {
        command.arg("-b:a").arg(bitrate);
    }
    command
        .arg("-f")
        .arg(audio_output_muxer(format))
        .arg("pipe:1");

    run_ffmpeg_stdout(
        command,
        payload,
        source_location,
        "audio extract",
        transform_slots,
        FfmpegInputMode::SeekableFile,
    )
}

fn extract_frame_outputs_for_sink(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    sample_id: u64,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<SinkArtifact>> {
    let (fps, format) = match video_extract_frames_spec(&job_spec.transforms) {
        Some(TransformSpec::VideoExtractFrames { fps, format }) => (*fps, format.as_str()),
        _ => anyhow::bail!("missing video.extract_frames transform"),
    };
    let frame_extension = normalize_frame_format(format)
        .ok_or_else(|| anyhow::anyhow!("unsupported frame format: {format}"))?
        .to_string();

    let mut command = Command::new("ffmpeg");
    command.arg("-hide_banner").arg("-loglevel").arg("error");
    append_segment_window_args(&mut command, sample_plan)?;
    command.arg("-vf").arg(format!("fps={fps}"));
    if normalize_frame_format(format) == Some("jpg") {
        command.arg("-q:v").arg("2");
    }
    command
        .arg("-f")
        .arg("image2pipe")
        .arg("-vcodec")
        .arg(frame_pipe_codec(format)?)
        .arg("pipe:1");

    let output = run_ffmpeg_stdout(
        command,
        payload,
        sample_plan.source_location.as_str(),
        "frame extract",
        transform_slots,
        FfmpegInputMode::SeekableFile,
    )?;
    let frames = split_frame_stream(&output, &frame_extension)?;
    if frames.is_empty() {
        anyhow::bail!(
            "ffmpeg frame extract produced no outputs for {}",
            sample_plan.source_location
        );
    }

    frames
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let output_uri =
                plan_frame_output_uri_with_plan(job_spec, sample_plan, sample_id, index)?;
            Ok(SinkArtifact {
                output_uri,
                payload,
            })
        })
        .collect()
}

fn render_video_extract_frame_chain_outputs_for_sink(
    job_spec: &CoreJobSpec,
    sample_plan: &SampleExecutionPlan,
    sample_id: u64,
    payload: &[u8],
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
) -> Result<Vec<SinkArtifact>> {
    let (extract_index, extracted_format) = video_extract_frames_stage(&job_spec.transforms)?;
    if extract_index != 0 {
        anyhow::bail!("video.extract_frames must be the first transform in the chain");
    }

    let extracted_frames =
        extract_frame_outputs_for_sink(job_spec, sample_plan, sample_id, payload, transform_slots)?;
    let image_tail = &job_spec.transforms[extract_index + 1..];
    if image_tail.is_empty() {
        return Ok(extracted_frames);
    }

    let chained_spec = job_spec_with_transforms(job_spec, image_tail.to_vec());
    extracted_frames
        .into_iter()
        .map(|artifact| {
            let chained_source = synthesized_media_location(&artifact.output_uri, extracted_format);
            let payload = transform_image_payload_for_sink(
                &chained_spec,
                &chained_source,
                &artifact.payload,
            )?;
            Ok(SinkArtifact {
                output_uri: artifact.output_uri,
                payload,
            })
        })
        .collect()
}

fn append_segment_window_args(
    command: &mut Command,
    sample_plan: &SampleExecutionPlan,
) -> Result<()> {
    match (sample_plan.segment_start_ms, sample_plan.segment_end_ms) {
        (Some(start_ms), Some(end_ms)) => {
            if end_ms <= start_ms {
                anyhow::bail!(
                    "segment_end_ms must be > segment_start_ms for {}",
                    sample_plan.source_location
                );
            }
            command.arg("-ss").arg(ffmpeg_time_arg(start_ms));
            command.arg("-t").arg(ffmpeg_time_arg(end_ms - start_ms));
            Ok(())
        }
        (None, None) => Ok(()),
        _ => anyhow::bail!(
            "segment_start_ms and segment_end_ms must both be set for {}",
            sample_plan.source_location
        ),
    }
}

fn ffmpeg_time_arg(total_ms: u64) -> String {
    let seconds = total_ms / 1_000;
    let milliseconds = total_ms % 1_000;
    format!("{seconds}.{milliseconds:03}")
}

fn video_filter_chain(transforms: &[TransformSpec]) -> Option<String> {
    let filters = transforms
        .iter()
        .filter_map(|transform| match transform {
            TransformSpec::VideoResize {
                width,
                height,
                maintain_aspect,
            } => Some(if *maintain_aspect {
                format!(
                    "scale=w={width}:h={height}:force_original_aspect_ratio=decrease,pad={width}:{height}:(ow-iw)/2:(oh-ih)/2"
                )
            } else {
                format!("scale=w={width}:h={height}")
            }),
            _ => None,
        })
        .collect::<Vec<_>>();

    if filters.is_empty() {
        None
    } else {
        Some(filters.join(","))
    }
}

fn video_output_codec(transforms: &[TransformSpec]) -> Option<&str> {
    transforms
        .iter()
        .rev()
        .find_map(|transform| match transform {
            TransformSpec::VideoTranscode { codec, .. } => normalize_video_codec(codec),
            _ => None,
        })
}

fn video_output_crf(transforms: &[TransformSpec]) -> Option<u32> {
    transforms
        .iter()
        .rev()
        .find_map(|transform| match transform {
            TransformSpec::VideoTranscode { crf, .. } => Some(*crf),
            _ => None,
        })
}

fn audio_filter_chain(transforms: &[TransformSpec]) -> Option<String> {
    let filters = transforms
        .iter()
        .filter_map(|transform| match transform {
            TransformSpec::AudioNormalize { loudness_lufs } => {
                Some(format!("loudnorm=I={loudness_lufs}:LRA=11:TP=-1.5"))
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if filters.is_empty() {
        None
    } else {
        Some(filters.join(","))
    }
}

fn audio_output_rate(transforms: &[TransformSpec]) -> Option<u32> {
    transforms
        .iter()
        .rev()
        .find_map(|transform| match transform {
            TransformSpec::AudioResample { rate, .. } => Some(*rate),
            _ => None,
        })
}

fn audio_output_channels(transforms: &[TransformSpec]) -> Option<u32> {
    transforms
        .iter()
        .rev()
        .find_map(|transform| match transform {
            TransformSpec::AudioResample { channels, .. } => Some(*channels),
            _ => None,
        })
}

fn video_extract_audio_spec(transforms: &[TransformSpec]) -> Option<&TransformSpec> {
    transforms
        .iter()
        .find(|transform| matches!(transform, TransformSpec::VideoExtractAudio { .. }))
}

fn video_extract_frames_spec(transforms: &[TransformSpec]) -> Option<&TransformSpec> {
    transforms
        .iter()
        .find(|transform| matches!(transform, TransformSpec::VideoExtractFrames { .. }))
}

fn video_extract_audio_stage(transforms: &[TransformSpec]) -> Result<(usize, &str)> {
    let Some(index) = transforms
        .iter()
        .position(|transform| matches!(transform, TransformSpec::VideoExtractAudio { .. }))
    else {
        anyhow::bail!("missing video.extract_audio transform");
    };

    let TransformSpec::VideoExtractAudio { format, .. } = &transforms[index] else {
        unreachable!("position matched video.extract_audio");
    };
    if !transforms[index + 1..].iter().all(is_audio_transform) {
        anyhow::bail!("video.extract_audio can only be followed by audio transforms");
    }
    Ok((index, format.as_str()))
}

fn video_extract_frames_stage(transforms: &[TransformSpec]) -> Result<(usize, &str)> {
    let Some(index) = transforms
        .iter()
        .position(|transform| matches!(transform, TransformSpec::VideoExtractFrames { .. }))
    else {
        anyhow::bail!("missing video.extract_frames transform");
    };

    let TransformSpec::VideoExtractFrames { format, .. } = &transforms[index] else {
        unreachable!("position matched video.extract_frames");
    };
    if !transforms[index + 1..].iter().all(is_image_transform) {
        anyhow::bail!("video.extract_frames can only be followed by image transforms");
    }
    Ok((index, format.as_str()))
}

fn ffmpeg_video_codec(codec: &str) -> Result<&'static str> {
    match codec {
        "h264" => Ok("libx264"),
        "h265" => Ok("libx265"),
        "av1" => Ok("libaom-av1"),
        other => anyhow::bail!("unsupported video codec: {other}"),
    }
}

fn ffmpeg_audio_codec(format: &str) -> Result<&'static str> {
    match normalize_audio_format(format) {
        Some("mp3") => Ok("libmp3lame"),
        Some("wav") => Ok("pcm_s16le"),
        Some("flac") => Ok("flac"),
        Some(other) => anyhow::bail!("unsupported audio format: {other}"),
        None => anyhow::bail!("unsupported audio format: {format}"),
    }
}

fn audio_output_format(source_location: &str) -> Result<&'static str> {
    let extension = source_extension(source_location)
        .ok_or_else(|| anyhow::anyhow!("audio source has no file extension: {source_location}"))?;
    normalize_audio_format(&extension).ok_or_else(|| {
        anyhow::anyhow!(
            "audio transforms currently support mp3, wav, or flac sources: {source_location}"
        )
    })
}

fn probe_audio_stream_metadata(input: &[u8], source_location: &str) -> Result<AudioStreamMetadata> {
    let mut child = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a:0")
        .arg("-show_entries")
        .arg("stream=sample_rate,channels")
        .arg("-of")
        .arg("default=noprint_wrappers=1:nokey=1")
        .arg("pipe:0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => {
                anyhow::anyhow!(
                    "ffprobe is required for audio transforms but was not found in PATH"
                )
            }
            _ => anyhow::anyhow!("ffprobe invocation failed for {source_location}: {err}"),
        })?;

    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to open ffprobe stdin for {source_location}"))?;
    let input_owned = input.to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&input_owned));

    let output = child
        .wait_with_output()
        .map_err(|err| anyhow::anyhow!("ffprobe wait failed for {source_location}: {err}"))?;
    let write_result = writer
        .join()
        .map_err(|_| anyhow::anyhow!("ffprobe stdin writer panicked for {source_location}"))?;
    write_result.map_err(|err| {
        anyhow::anyhow!("ffprobe stdin write failed for {source_location}: {err}")
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "ffprobe audio metadata failed for {source_location}: {}",
            stderr.trim()
        );
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| {
        anyhow::anyhow!("ffprobe output was not valid utf8 for {source_location}: {err}")
    })?;
    let mut lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let sample_rate = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("ffprobe sample_rate missing for {source_location}"))?
        .parse::<u32>()
        .map_err(|err| {
            anyhow::anyhow!("ffprobe sample_rate parse failed for {source_location}: {err}")
        })?;
    let channels = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("ffprobe channels missing for {source_location}"))?
        .parse::<u32>()
        .map_err(|err| {
            anyhow::anyhow!("ffprobe channels parse failed for {source_location}: {err}")
        })?;

    Ok(AudioStreamMetadata {
        sample_rate,
        channels,
    })
}

fn audio_output_muxer(format: &str) -> &'static str {
    match normalize_audio_format(format) {
        Some("mp3") => "mp3",
        Some("wav") => "wav",
        Some("flac") => "flac",
        _ => "data",
    }
}

fn frame_pipe_codec(format: &str) -> Result<&'static str> {
    match normalize_frame_format(format) {
        Some("jpg") => Ok("mjpeg"),
        Some("png") => Ok("png"),
        Some(other) => anyhow::bail!("unsupported frame format: {other}"),
        None => anyhow::bail!("unsupported frame format: {format}"),
    }
}

#[derive(Debug, Clone, Copy)]
enum FfmpegInputMode {
    PipeStdin,
    SeekableFile,
}

#[derive(Debug)]
struct SeekableInputFile {
    path: PathBuf,
}

impl SeekableInputFile {
    fn create(source_location: &str, input: &[u8]) -> Result<Self> {
        let dir = preferred_seekable_input_dir();
        let ext = source_extension(source_location).unwrap_or_else(|| "bin".to_string());
        for _ in 0..32 {
            let suffix = SEEKABLE_INPUT_COUNTER.fetch_add(1, Ordering::Relaxed);
            let name = format!(
                "mx8-media-{}-{}-{suffix}.{ext}",
                std::process::id(),
                mx8_observe::time::unix_time_ms()
            );
            let path = dir.join(name);
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut file) => {
                    file.write_all(input).map_err(|err| {
                        anyhow::anyhow!(
                            "failed to stage seekable input for {source_location}: {err}"
                        )
                    })?;
                    drop(file);
                    return Ok(Self { path });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "failed to create seekable input staging file for {source_location}: {err}"
                    ));
                }
            }
        }

        anyhow::bail!("failed to allocate unique seekable input staging path for {source_location}")
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for SeekableInputFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn preferred_seekable_input_dir() -> PathBuf {
    let shm = Path::new("/dev/shm");
    if shm.is_dir() {
        shm.to_path_buf()
    } else {
        std::env::temp_dir()
    }
}

fn run_ffmpeg_stdout(
    mut command: Command,
    input: &[u8],
    source_location: &str,
    op_name: &str,
    transform_slots: Option<&Arc<TransformProcessLimiter>>,
    input_mode: FfmpegInputMode,
) -> Result<Vec<u8>> {
    let _permit = transform_slots.map(|slots| slots.acquire()).transpose()?;

    let staged_input = match input_mode {
        FfmpegInputMode::PipeStdin => {
            command.arg("-i").arg("pipe:0").stdin(Stdio::piped());
            None
        }
        FfmpegInputMode::SeekableFile => {
            let staged = SeekableInputFile::create(source_location, input)?;
            command.arg("-i").arg(staged.path()).stdin(Stdio::null());
            Some(staged)
        }
    };
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => {
            anyhow::anyhow!("ffmpeg is required for {op_name} but was not found in PATH")
        }
        _ => anyhow::anyhow!("ffmpeg invocation failed for {source_location}: {err}"),
    })?;

    let writer = match input_mode {
        FfmpegInputMode::PipeStdin => {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                anyhow::anyhow!("failed to open ffmpeg stdin for {source_location}")
            })?;
            let input_owned = input.to_vec();
            Some(std::thread::spawn(move || stdin.write_all(&input_owned)))
        }
        FfmpegInputMode::SeekableFile => None,
    };

    let output = child
        .wait_with_output()
        .map_err(|err| anyhow::anyhow!("ffmpeg wait failed for {source_location}: {err}"))?;
    if let Some(writer) = writer {
        let write_result = writer
            .join()
            .map_err(|_| anyhow::anyhow!("ffmpeg stdin writer panicked for {source_location}"))?;
        write_result.map_err(|err| {
            anyhow::anyhow!("ffmpeg stdin write failed for {source_location}: {err}")
        })?;
    }
    drop(staged_input);

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "ffmpeg {op_name} failed for {source_location}: {}",
            stderr.trim()
        );
    }

    Ok(output.stdout)
}

fn split_frame_stream(bytes: &[u8], extension: &str) -> Result<Vec<Vec<u8>>> {
    match normalize_frame_format(extension) {
        Some("png") => split_png_stream(bytes),
        Some("jpg") => split_jpeg_stream(bytes),
        Some(other) => anyhow::bail!("unsupported frame format: {other}"),
        None => anyhow::bail!("unsupported frame format: {extension}"),
    }
}

fn split_png_stream(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    const PNG_SIGNATURE: &[u8; 8] = b"\x89PNG\r\n\x1a\n";
    let mut frames = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if index + PNG_SIGNATURE.len() > bytes.len()
            || &bytes[index..index + PNG_SIGNATURE.len()] != PNG_SIGNATURE
        {
            anyhow::bail!("invalid PNG frame stream");
        }
        let frame_start = index;
        index += PNG_SIGNATURE.len();

        loop {
            if index + 8 > bytes.len() {
                anyhow::bail!("truncated PNG frame stream");
            }
            let length = u32::from_be_bytes(bytes[index..index + 4].try_into().expect("png length"))
                as usize;
            let chunk_type = &bytes[index + 4..index + 8];
            let chunk_end = index
                .checked_add(12 + length)
                .ok_or_else(|| anyhow::anyhow!("PNG chunk length overflow"))?;
            if chunk_end > bytes.len() {
                anyhow::bail!("truncated PNG chunk in frame stream");
            }
            index = chunk_end;
            if chunk_type == b"IEND" {
                frames.push(bytes[frame_start..index].to_vec());
                break;
            }
        }
    }

    Ok(frames)
}

fn split_jpeg_stream(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        if index + 1 >= bytes.len() || bytes[index] != 0xFF || bytes[index + 1] != 0xD8 {
            anyhow::bail!("invalid JPEG frame stream");
        }
        let frame_start = index;
        index += 2;
        while index + 1 < bytes.len() {
            if bytes[index] == 0xFF && bytes[index + 1] == 0xD9 {
                index += 2;
                frames.push(bytes[frame_start..index].to_vec());
                break;
            }
            index += 1;
        }
        if frames.is_empty() || frames.last().map(|frame| frame.len()).unwrap_or(0) == 0 {
            anyhow::bail!("truncated JPEG frame stream");
        }
    }

    Ok(frames)
}

fn image_convert_quality(transforms: &[TransformSpec]) -> Option<u32> {
    transforms
        .iter()
        .rev()
        .find_map(|transform| match transform {
            TransformSpec::ImageConvert { quality, .. } => Some(*quality),
            _ => None,
        })
}

fn encode_transformed_image(
    image: &DynamicImage,
    extension: &str,
    quality: Option<u32>,
) -> Result<Vec<u8>> {
    match normalized_image_extension(extension).as_str() {
        "jpg" => encode_jpeg(image, quality.unwrap_or(85)),
        "png" => encode_with_format(image, ImageFormat::Png),
        "webp" => encode_webp(image, quality.unwrap_or(85)),
        "bmp" => encode_with_format(image, ImageFormat::Bmp),
        "tiff" => encode_with_format(image, ImageFormat::Tiff),
        other => anyhow::bail!("unsupported image output format: {other}"),
    }
}

fn encode_jpeg(image: &DynamicImage, quality: u32) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let quality_u8 = u8::try_from(quality.min(100))
        .map_err(|_| anyhow::anyhow!("jpeg quality out of range: {quality}"))?;
    let mut encoder = JpegEncoder::new_with_quality(&mut output, quality_u8);
    encoder
        .encode_image(image)
        .map_err(|err| anyhow::anyhow!("jpeg encode failed: {err}"))?;
    Ok(output)
}

fn encode_with_format(image: &DynamicImage, format: ImageFormat) -> Result<Vec<u8>> {
    let mut cursor = Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, format)
        .map_err(|err| anyhow::anyhow!("image encode failed: {err}"))?;
    Ok(cursor.into_inner())
}

fn encode_webp(image: &DynamicImage, quality: u32) -> Result<Vec<u8>> {
    let rgba = image.to_rgba8();
    let has_alpha = rgba.chunks_exact(4).any(|pixel| pixel[3] != u8::MAX);
    let width = usize::try_from(rgba.width())
        .map_err(|_| anyhow::anyhow!("image width does not fit into usize"))?;
    let height = usize::try_from(rgba.height())
        .map_err(|_| anyhow::anyhow!("image height does not fit into usize"))?;
    let webp_image = WebpImageBuffer {
        width,
        height,
        rgba: rgba.into_raw(),
    };

    if has_alpha {
        encode_lossless_webp(&webp_image, 4, None)
            .map_err(|err| anyhow::anyhow!("webp lossless encode failed: {err}"))
    } else {
        encode_lossy_webp(&webp_image, 4, quality as usize, None)
            .map_err(|err| anyhow::anyhow!("webp lossy encode failed: {err}"))
    }
}

fn source_basename_without_extension(source_location: &str) -> Option<String> {
    let file_name = source_location.rsplit('/').next()?;
    if file_name.is_empty() {
        return None;
    }
    let without_ext = file_name
        .rsplit_once('.')
        .map(|(stem, _)| stem)
        .unwrap_or(file_name);
    Some(without_ext.to_string())
}

fn source_extension(source_location: &str) -> Option<String> {
    let file_name = source_location.rsplit('/').next()?;
    let (_, ext) = file_name.rsplit_once('.')?;
    if ext.is_empty() {
        return None;
    }
    Some(ext.to_ascii_lowercase())
}

fn normalized_image_extension(ext: &str) -> String {
    match ext.trim().to_ascii_lowercase().as_str() {
        "jpeg" => "jpg".to_string(),
        "tif" => "tiff".to_string(),
        other => other.to_string(),
    }
}

fn output_extension(transforms: &[TransformSpec], source_location: &str) -> String {
    if let Ok((_, format)) = video_extract_frames_stage(transforms) {
        if let Some(converted) = transforms
            .iter()
            .rev()
            .find_map(|transform| match transform {
                TransformSpec::ImageConvert { format, .. } => {
                    match normalize_image_format(format) {
                        Some(normalized) => Some(normalized.to_string()),
                        None => Some(normalized_image_extension(format)),
                    }
                }
                _ => None,
            })
        {
            return converted;
        }
        return format.to_string();
    }

    if let Ok((_, format)) = video_extract_audio_stage(transforms) {
        return format.to_string();
    }

    match transforms.last() {
        Some(TransformSpec::VideoExtractFrames { format, .. }) => format.clone(),
        Some(TransformSpec::VideoExtractAudio { format, .. }) => format.clone(),
        Some(TransformSpec::ImageConvert { format, .. }) => match normalize_image_format(format) {
            Some(normalized) => normalized.to_string(),
            None => normalized_image_extension(format),
        },
        Some(TransformSpec::VideoTranscode { .. } | TransformSpec::VideoResize { .. }) => {
            "mp4".to_string()
        }
        _ => source_extension(source_location).unwrap_or_else(|| "bin".to_string()),
    }
}

fn synthesized_media_location(source_location: &str, extension: &str) -> String {
    let base_name = source_basename_without_extension(source_location)
        .unwrap_or_else(|| "artifact".to_string());
    format!("{base_name}.{extension}")
}

fn job_spec_with_transforms(job_spec: &CoreJobSpec, transforms: Vec<TransformSpec>) -> CoreJobSpec {
    CoreJobSpec {
        job_id: job_spec.job_id.clone(),
        source_uri: job_spec.source_uri.clone(),
        sink_uri: job_spec.sink_uri.clone(),
        aws_region: job_spec.aws_region.clone(),
        transforms,
    }
}

fn transform_tag(transforms: &[TransformSpec]) -> String {
    if transforms.is_empty() {
        return "identity".to_string();
    }

    transforms
        .iter()
        .map(|transform| match transform {
            TransformSpec::VideoTranscode { .. } => "transcode",
            TransformSpec::VideoResize { .. } => "resize",
            TransformSpec::VideoExtractFrames { .. } => "frames",
            TransformSpec::VideoExtractAudio { .. } => "extract_audio",
            TransformSpec::VideoFilter { .. } => "filter",
            TransformSpec::ImageResize { .. } => "resize",
            TransformSpec::ImageCrop { .. } => "crop",
            TransformSpec::ImageConvert { .. } => "convert",
            TransformSpec::ImageFilter { .. } => "filter",
            TransformSpec::AudioResample { .. } => "resample",
            TransformSpec::AudioNormalize { .. } => "normalize",
            TransformSpec::AudioFilter { .. } => "filter",
        })
        .collect::<Vec<_>>()
        .join("__")
}

async fn report_progress_loop(
    channel: Channel,
    ctx: ProgressLoopCtx,
    progress: Arc<LeaseProgress>,
    cancelled: Arc<AtomicBool>,
    mut done: tokio::sync::oneshot::Receiver<()>,
) {
    let mut client = CoordinatorClient::new(channel)
        .max_decoding_message_size(ctx.grpc_max_message_bytes)
        .max_encoding_message_size(ctx.grpc_max_message_bytes);
    let mut ticker = tokio::time::interval(std::cmp::max(Duration::from_millis(1), ctx.interval));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let cursor = progress.cursor();
                let delivered_samples = progress.delivered_samples();
                let delivered_bytes = progress.delivered_bytes();
                let req = ReportProgressRequest {
                    job_id: ctx.job_id.clone(),
                    node_id: ctx.node_id.clone(),
                    lease_id: ctx.lease_id.clone(),
                    cursor,
                    delivered_samples,
                    delivered_bytes,
                    unix_time_ms: mx8_observe::time::unix_time_ms(),
                };
                if let Err(err) = client.report_progress(req).await {
                    if err.code() == tonic::Code::NotFound {
                        cancelled.store(true, Ordering::Relaxed);
                        warn!(
                            target: "mx8_proof",
                            event = "lease_progress_stale_lease",
                            error = %err,
                            lease_id = %ctx.lease_id,
                            cursor = cursor,
                            "stale lease detected; cancelling local lease work"
                        );
                        return;
                    }
                    warn!(error = %err, lease_id = %ctx.lease_id, cursor = cursor, "ReportProgress failed");
                } else {
                    tracing::debug!(lease_id = %ctx.lease_id, cursor = cursor, "ReportProgress ok");
                }
            }
            _ = &mut done => {
                tracing::info!(
                    target: "mx8_proof",
                    event = "lease_progress_loop_done",
                    job_id = %ctx.job_id,
                    node_id = %ctx.node_id,
                    manifest_hash = %ctx.manifest_hash,
                    lease_id = %ctx.lease_id,
                    cursor = progress.cursor(),
                    delivered_samples = progress.delivered_samples(),
                    delivered_bytes = progress.delivered_bytes(),
                    "lease progress loop done"
                );
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ProgressLoopCtx {
    job_id: String,
    node_id: String,
    manifest_hash: String,
    lease_id: String,
    interval: Duration,
    grpc_max_message_bytes: usize,
}

#[derive(Debug, Clone)]
enum ManifestSource {
    DirectStream,
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse::<usize>().ok().unwrap_or(default),
        Err(_) => default,
    }
}

async fn fetch_manifest_records_for_range(
    channel: Channel,
    args: &Args,
    manifest_hash: &str,
    start_id: u64,
    end_id: u64,
    metrics: &Arc<AgentMetrics>,
) -> Result<Vec<mx8_core::types::ManifestRecord>> {
    let max_line_bytes = env_usize("MX8_AGENT_MANIFEST_STREAM_MAX_LINE_BYTES", 8 * 1024 * 1024);
    let req = GetManifestRequest {
        job_id: args.job_id.clone(),
        manifest_hash: manifest_hash.to_string(),
    };
    let mut client = CoordinatorClient::new(channel)
        .max_decoding_message_size(args.grpc_max_message_bytes)
        .max_encoding_message_size(args.grpc_max_message_bytes);

    let stream_resp = client.get_manifest_stream(req.clone()).await;
    match stream_resp {
        Ok(resp) => {
            let mut stream = resp.into_inner();
            let mut parser = mx8_runtime::pipeline::ManifestRangeStreamParser::new(
                start_id,
                end_id,
                max_line_bytes,
            )?;
            let mut stream_schema: Option<u32> = None;
            while let Some(chunk) = stream.message().await? {
                if let Some(existing) = stream_schema {
                    if chunk.schema_version != existing {
                        metrics.manifest_schema_mismatch_total.inc();
                        tracing::error!(
                            target: "mx8_proof",
                            event = "manifest_schema_mismatch",
                            job_id = %args.job_id,
                            node_id = %args.node_id,
                            manifest_hash = %manifest_hash,
                            expected_schema_version = existing,
                            got_schema_version = chunk.schema_version,
                            "manifest stream schema mismatch"
                        );
                        anyhow::bail!("GetManifestStream returned inconsistent schema_version");
                    }
                } else {
                    stream_schema = Some(chunk.schema_version);
                }
                parser.push_chunk(chunk.data.as_slice())?;
            }
            anyhow::ensure!(
                stream_schema.is_some(),
                "GetManifestStream returned no chunks (empty manifest)"
            );
            match parser.finish() {
                Ok(records) => Ok(records),
                Err(err) => {
                    metrics.manifest_stream_truncated_total.inc();
                    tracing::error!(
                        target: "mx8_proof",
                        event = "manifest_stream_truncated",
                        job_id = %args.job_id,
                        node_id = %args.node_id,
                        manifest_hash = %manifest_hash,
                        start_id = start_id,
                        end_id = end_id,
                        error = %err,
                        "manifest stream parse failed (fail-closed)"
                    );
                    Err(err)
                }
            }
        }
        Err(status) => {
            if status.code() != tonic::Code::Unimplemented {
                anyhow::bail!("GetManifestStream failed: {status}");
            }
            metrics.manifest_direct_stream_fallback_total.inc();
            tracing::warn!(
                target: "mx8_proof",
                event = "manifest_direct_stream_fallback",
                job_id = %args.job_id,
                node_id = %args.node_id,
                manifest_hash = %manifest_hash,
                start_id = start_id,
                end_id = end_id,
                reason = "GetManifestStream unimplemented",
                "falling back to unary GetManifest for direct-stream path"
            );
            let resp = client.get_manifest(req).await?.into_inner();
            mx8_runtime::pipeline::load_manifest_records_range_from_read(
                std::io::Cursor::new(resp.manifest_bytes),
                start_id,
                end_id,
            )
        }
    }
}

async fn run_lease(
    channel: Channel,
    pipeline: Arc<Pipeline>,
    _manifest_source: ManifestSource,
    args: &Args,
    manifest_hash: &str,
    metrics: &Arc<AgentMetrics>,
    lease: Lease,
    job_spec: Option<Arc<CoreJobSpec>>,
    transform_slots: Arc<TransformProcessLimiter>,
) -> Result<()> {
    let Some(range) = &lease.range else {
        anyhow::bail!("lease missing range");
    };

    metrics.manifest_direct_stream_used_total.inc();
    let records = fetch_manifest_records_for_range(
        channel.clone(),
        args,
        manifest_hash,
        range.start_id,
        range.end_id,
        metrics,
    )
    .await?;
    let sample_plans = Arc::new(build_sample_plans(&records, job_spec.as_deref())?);

    metrics.leases_started_total.inc();
    tracing::info!(
        target: "mx8_proof",
        event = "lease_started",
        job_id = %args.job_id,
        node_id = %args.node_id,
        manifest_hash = %manifest_hash,
        lease_id = %lease.lease_id,
        epoch = range.epoch,
        start_id = range.start_id,
        end_id = range.end_id,
        source = "direct_stream",
        "lease started"
    );

    let progress = Arc::new(LeaseProgress::new(range.start_id, range.end_id));
    let cancelled = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let reporter = tokio::spawn(report_progress_loop(
        channel.clone(),
        ProgressLoopCtx {
            job_id: args.job_id.clone(),
            node_id: args.node_id.clone(),
            manifest_hash: manifest_hash.to_string(),
            lease_id: lease.lease_id.clone(),
            interval: Duration::from_millis(args.progress_interval_ms),
            grpc_max_message_bytes: args.grpc_max_message_bytes,
        },
        progress.clone(),
        cancelled.clone(),
        done_rx,
    ));

    let run_res = if let Some(job_spec) = job_spec {
        #[cfg(feature = "s3")]
        let s3_client = Arc::new(mx8_runtime::s3::client_from_env().await?);
        #[cfg(feature = "gcs")]
        let gcs_client = Arc::new(mx8_runtime::gcs::client_from_env().await?);
        #[cfg(feature = "azure")]
        let azure_client_builder = Arc::new(mx8_runtime::azure::client_builder_from_env()?);
        let sink = Arc::new(JobSpecSink {
            progress: progress.clone(),
            sleep: Duration::from_millis(args.sink_sleep_ms),
            cancelled: cancelled.clone(),
            job_spec,
            sample_plans,
            transform_slots,
            #[cfg(feature = "s3")]
            s3_client,
            #[cfg(feature = "gcs")]
            gcs_client,
            #[cfg(feature = "azure")]
            azure_client_builder,
        });
        pipeline.run_manifest_records(sink, records).await
    } else {
        let sink = Arc::new(LeaseProgressSink {
            progress: progress.clone(),
            sleep: Duration::from_millis(args.sink_sleep_ms),
            cancelled: cancelled.clone(),
        });
        pipeline.run_manifest_records(sink, records).await
    };

    let _ = done_tx.send(());
    let _ = reporter.await;

    if cancelled.load(Ordering::Relaxed) {
        tracing::warn!(
            target: "mx8_proof",
            event = "lease_revoked",
            job_id = %args.job_id,
            node_id = %args.node_id,
            manifest_hash = %manifest_hash,
            lease_id = %lease.lease_id,
            cursor = progress.cursor(),
            delivered_samples = progress.delivered_samples(),
            delivered_bytes = progress.delivered_bytes(),
            "coordinator revoked lease; local work dropped"
        );
        return Ok(());
    }

    run_res?;

    let cursor = progress.cursor();
    let delivered_samples = progress.delivered_samples();
    let delivered_bytes = progress.delivered_bytes();

    metrics.leases_completed_total.inc();
    metrics.delivered_samples_total.inc_by(delivered_samples);
    metrics.delivered_bytes_total.inc_by(delivered_bytes);

    let mut client = CoordinatorClient::new(channel)
        .max_decoding_message_size(args.grpc_max_message_bytes)
        .max_encoding_message_size(args.grpc_max_message_bytes);
    let _ = client
        .report_progress(ReportProgressRequest {
            job_id: args.job_id.clone(),
            node_id: args.node_id.clone(),
            lease_id: lease.lease_id.clone(),
            cursor,
            delivered_samples,
            delivered_bytes,
            unix_time_ms: mx8_observe::time::unix_time_ms(),
        })
        .await;

    tracing::info!(
        target: "mx8_proof",
        event = "lease_finished",
        job_id = %args.job_id,
        node_id = %args.node_id,
        manifest_hash = %manifest_hash,
        lease_id = %lease.lease_id,
        cursor = cursor,
        delivered_samples = delivered_samples,
        delivered_bytes = delivered_bytes,
        "lease finished"
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    mx8_observe::logging::init_tracing();

    let args = Args::parse();
    let span = info_span!(
        "mx8d-agent",
        job_id = %args.job_id,
        node_id = %args.node_id,
        coord_url = %args.coord_url
    );

    async move {
        info!("starting agent (v0 skeleton)");
        let channel = Channel::from_shared(args.coord_url.clone())?
            .connect()
            .await?;
        let mut client = CoordinatorClient::new(channel.clone())
            .max_decoding_message_size(args.grpc_max_message_bytes)
            .max_encoding_message_size(args.grpc_max_message_bytes);

        let metrics = Arc::new(AgentMetrics::default());
        metrics.register_total.inc();
        let caps = Some(NodeCaps {
            max_fetch_concurrency: 32,
            max_decode_concurrency: 8,
            max_inflight_bytes: 1 << 30,
            max_ram_bytes: 4 << 30,
        });

        let mut register_resp = client
            .register_node(RegisterNodeRequest {
                job_id: args.job_id.clone(),
                node_id: args.node_id.clone(),
                caps: caps.clone(),
                resume_from: Vec::new(),
            })
            .await?
            .into_inner();

        while !register_resp.job_ready {
            tokio::time::sleep(Duration::from_millis(200)).await;
            register_resp = client
                .register_node(RegisterNodeRequest {
                    job_id: args.job_id.clone(),
                    node_id: args.node_id.clone(),
                    caps: caps.clone(),
                    resume_from: Vec::new(),
                })
                .await?
                .into_inner();
        }

        let job_spec = register_resp
            .job_spec
            .as_ref()
            .map(TryToCore::try_to_core)
            .transpose()
            .map_err(|err| anyhow::anyhow!("invalid JobSpec from coordinator: {err}"))?;
        if let Some(spec) = job_spec.as_ref() {
            spec.validate()
                .map_err(|err| anyhow::anyhow!("invalid JobSpec from coordinator: {err}"))?;
        }
        let job_spec = job_spec.map(Arc::new);
        let heartbeat_interval_ms = std::cmp::max(1, register_resp.heartbeat_interval_ms as u64);
        let manifest_hash = register_resp.manifest_hash.clone();

        info!(
            assigned_rank = register_resp.assigned_rank,
            world_size = register_resp.world_size,
            registered_nodes = register_resp.registered_nodes,
            job_ready = register_resp.job_ready,
            heartbeat_interval_ms = register_resp.heartbeat_interval_ms,
            lease_ttl_ms = register_resp.lease_ttl_ms,
            manifest_hash = %register_resp.manifest_hash,
            has_job_spec = job_spec.is_some(),
            "registered with coordinator"
        );
        if let Some(spec) = job_spec.as_ref() {
            info!(
                source_uri = %spec.source_uri,
                sink_uri = %spec.sink_uri,
                aws_region = %spec.aws_region,
                transform_count = spec.transforms.len(),
                "received JobSpec from coordinator"
            );
        }

        info!(
            target: "mx8_proof",
            event = "manifest_ingest_mode_selected",
            mode = "direct_stream",
            "using direct stream manifest ingest path"
        );
        let manifest_source = ManifestSource::DirectStream;

        if args.metrics_snapshot_interval_ms > 0 {
            let metrics_clone = metrics.clone();
            let job_id = args.job_id.clone();
            let node_id = args.node_id.clone();
            let manifest_hash = manifest_hash.clone();
            let interval_ms = args.metrics_snapshot_interval_ms;
            tokio::spawn(async move {
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_millis(interval_ms));
                loop {
                    ticker.tick().await;
                    metrics_clone.snapshot(&job_id, &node_id, &manifest_hash);
                }
            });
        }

        let heartbeat_channel = channel.clone();
        let heartbeat_job_id = args.job_id.clone();
        let heartbeat_node_id = args.node_id.clone();
        let metrics_clone = metrics.clone();
        let grpc_max = args.grpc_max_message_bytes;
        tokio::spawn(async move {
            let mut client = CoordinatorClient::new(heartbeat_channel)
                .max_decoding_message_size(grpc_max)
                .max_encoding_message_size(grpc_max);
            loop {
                tokio::time::sleep(Duration::from_millis(heartbeat_interval_ms)).await;
                metrics_clone.heartbeat_total.inc();
                let now_ms = mx8_observe::time::unix_time_ms();

                let stats = NodeStats {
                    inflight_bytes: metrics_clone.inflight_bytes.get(),
                    ram_high_water_bytes: metrics_clone.ram_high_water_bytes.get(),
                    fetch_queue_depth: 0,
                    decode_queue_depth: 0,
                    pack_queue_depth: 0,
                    autotune_enabled: false,
                    effective_prefetch_batches: 0,
                    effective_max_queue_batches: 0,
                    effective_want: 0,
                    autotune_pressure_milli: 0,
                    autotune_cooldown_ticks: 0,
                    batch_payload_p95_over_p50_milli: 0,
                    batch_jitter_slo_breaches_total: 0,
                };

                let res = client
                    .heartbeat(HeartbeatRequest {
                        job_id: heartbeat_job_id.clone(),
                        node_id: heartbeat_node_id.clone(),
                        unix_time_ms: now_ms,
                        stats: Some(stats),
                    })
                    .await;

                match res {
                    Ok(_) => {
                        metrics_clone.heartbeat_ok_total.inc();
                        tracing::debug!("heartbeat ok");
                    }
                    Err(err) => {
                        metrics_clone.heartbeat_err_total.inc();
                        tracing::warn!(error = %err, "heartbeat failed");
                    }
                }
            }
        });

        if args.dev_lease_want > 0 {
            let lease_channel = channel.clone();
            let args_clone = args.clone();
            let metrics_clone = metrics.clone();
            let manifest_hash_clone = manifest_hash.clone();
            let manifest_source = manifest_source.clone();
            let job_spec = job_spec.clone();
            let transform_slots = Arc::new(TransformProcessLimiter::new(args.max_transform_processes));
            tokio::spawn(async move {
                let want = std::cmp::max(1, args_clone.dev_lease_want);
                let grpc_max = args_clone.grpc_max_message_bytes;
                let caps = RuntimeCaps {
                    max_inflight_bytes: args_clone.max_inflight_bytes,
                    max_queue_batches: args_clone.max_queue_batches,
                    batch_size_samples: args_clone.batch_size_samples,
                    prefetch_batches: args_clone.prefetch_batches,
                    target_batch_bytes: args_clone.target_batch_bytes,
                    max_batch_bytes: args_clone.max_batch_bytes,
                    max_process_rss_bytes: args_clone.max_process_rss_bytes,
                };
                let pipeline = Arc::new(Pipeline::new(caps));

                // `want` is interpreted as "max concurrent leases per node".
                let target_concurrency = std::cmp::max(1, want as usize);
                let mut joinset: tokio::task::JoinSet<(String, anyhow::Result<()>)> =
                    tokio::task::JoinSet::new();
                let mut active: usize = 0;
                let mut next_request_at = tokio::time::Instant::now();

                loop {
                    let now = tokio::time::Instant::now();
                    if active < target_concurrency && now >= next_request_at {
                        let deficit = target_concurrency - active;
                        let deficit_u32 = u32::try_from(deficit).unwrap_or(u32::MAX);

                        let mut client = CoordinatorClient::new(lease_channel.clone())
                            .max_decoding_message_size(grpc_max)
                            .max_encoding_message_size(grpc_max);
                        let resp = client
                            .request_lease(RequestLeaseRequest {
                                job_id: args_clone.job_id.clone(),
                                node_id: args_clone.node_id.clone(),
                                want: std::cmp::max(1, deficit_u32),
                            })
                            .await;

                        let resp = match resp {
                            Ok(resp) => resp.into_inner(),
                            Err(err) => {
                                tracing::info!(error = %err, "RequestLease failed; backing off");
                                next_request_at =
                                    tokio::time::Instant::now() + Duration::from_millis(500);
                                continue;
                            }
                        };

                        if resp.leases.is_empty() {
                            let wait_ms = std::cmp::max(1, resp.wait_ms);
                            next_request_at = tokio::time::Instant::now()
                                + Duration::from_millis(wait_ms as u64);
                            continue;
                        }

                        for lease in resp.leases {
                            let pipeline = pipeline.clone();
                            let args = args_clone.clone();
                            let manifest_hash = manifest_hash_clone.clone();
                            let manifest_source = manifest_source.clone();
                            let metrics = metrics_clone.clone();
                            let ch = lease_channel.clone();
                            let lease_id = lease.lease_id.clone();
                            let job_spec = job_spec.clone();
                            let transform_slots = transform_slots.clone();

                            tracing::info!(
                                target: "mx8_proof",
                                event = "lease_task_spawned",
                                job_id = %args.job_id,
                                node_id = %args.node_id,
                                manifest_hash = %manifest_hash,
                                lease_id = %lease_id,
                                "lease task spawned"
                            );

                            joinset.spawn(async move {
                                let res = run_lease(
                                    ch,
                                    pipeline,
                                    manifest_source,
                                    &args,
                                    &manifest_hash,
                                    &metrics,
                                    lease,
                                    job_spec,
                                    transform_slots,
                                )
                                .await;
                                (lease_id, res)
                            });
                            active = active.saturating_add(1);
                            metrics_clone
                                .active_lease_tasks
                                .set(u64::try_from(active).unwrap_or(u64::MAX));
                            if active >= target_concurrency {
                                break;
                            }
                        }

                        next_request_at = tokio::time::Instant::now();
                        continue;
                    }

                    if active == 0 {
                        tokio::time::sleep_until(next_request_at).await;
                        continue;
                    }

                    tokio::select! {
                        res = joinset.join_next() => {
                            match res {
                                Some(Ok((lease_id, Ok(())))) => {
                                    tracing::info!(
                                        target: "mx8_proof",
                                        event = "lease_task_done",
                                        job_id = %args_clone.job_id,
                                        node_id = %args_clone.node_id,
                                        manifest_hash = %manifest_hash_clone,
                                        lease_id = %lease_id,
                                        "lease task done"
                                    );
                                }
                                Some(Ok((lease_id, Err(err)))) => {
                                    tracing::error!(
                                        target: "mx8_proof",
                                        event = "lease_task_done",
                                        job_id = %args_clone.job_id,
                                        node_id = %args_clone.node_id,
                                        manifest_hash = %manifest_hash_clone,
                                        lease_id = %lease_id,
                                        error = %err,
                                        "lease task done (error)"
                                    );
                                }
                                Some(Err(err)) => {
                                    tracing::error!(error = %err, "lease task join failed");
                                }
                                None => {}
                            }
                            active = active.saturating_sub(1);
                            metrics_clone
                                .active_lease_tasks
                                .set(u64::try_from(active).unwrap_or(u64::MAX));
                        }
                        _ = tokio::time::sleep_until(next_request_at), if active < target_concurrency => {
                            // Time to ask for more leases.
                        }
                    }
                }
            });
        }

        tokio::signal::ctrl_c().await?;
        Ok(())
    }
    .instrument(span)
    .await
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        normalized_image_extension, output_extension, render_outputs_for_sink,
        render_outputs_for_sink_with_plan, run_lease, transform_payload_for_sink, AgentMetrics,
        Args, CoreJobSpec, ManifestSource, SampleExecutionPlan, TransformProcessLimiter,
        TransformSpec,
    };
    use image::{DynamicImage, GenericImageView, ImageFormat};
    use mx8_proto::v0::coordinator_server::{Coordinator, CoordinatorServer};
    use mx8_proto::v0::{
        GetJobSnapshotRequest, GetJobSnapshotResponse, GetManifestRequest, GetManifestResponse,
        GetResumeCheckpointRequest, GetResumeCheckpointResponse, HeartbeatRequest,
        HeartbeatResponse, Lease, ManifestChunk, RegisterNodeRequest, RegisterNodeResponse,
        ReportProgressRequest, ReportProgressResponse, RequestLeaseRequest, RequestLeaseResponse,
        WorkRange,
    };
    use mx8_runtime::pipeline::{Pipeline, RuntimeCaps};
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::pin::Pin;
    use std::process::Command;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_stream::Stream;
    use tonic::transport::{Channel, Server};
    use tonic::{Request, Response, Status};

    #[derive(Clone)]
    struct MockCoordinator {
        manifest_bytes: Arc<Vec<u8>>,
        reports: Arc<Mutex<Vec<ReportProgressRequest>>>,
    }

    impl MockCoordinator {
        fn new(manifest_bytes: Vec<u8>) -> Self {
            Self {
                manifest_bytes: Arc::new(manifest_bytes),
                reports: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[tonic::async_trait]
    impl Coordinator for MockCoordinator {
        type GetManifestStreamStream =
            Pin<Box<dyn Stream<Item = Result<ManifestChunk, Status>> + Send + 'static>>;

        async fn register_node(
            &self,
            _request: Request<RegisterNodeRequest>,
        ) -> Result<Response<RegisterNodeResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn heartbeat(
            &self,
            _request: Request<HeartbeatRequest>,
        ) -> Result<Response<HeartbeatResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn request_lease(
            &self,
            _request: Request<RequestLeaseRequest>,
        ) -> Result<Response<RequestLeaseResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn report_progress(
            &self,
            request: Request<ReportProgressRequest>,
        ) -> Result<Response<ReportProgressResponse>, Status> {
            let mut guard = self
                .reports
                .lock()
                .map_err(|_| Status::internal("report lock poisoned"))?;
            guard.push(request.into_inner());
            Ok(Response::new(ReportProgressResponse {}))
        }

        async fn get_manifest(
            &self,
            _request: Request<GetManifestRequest>,
        ) -> Result<Response<GetManifestResponse>, Status> {
            Ok(Response::new(GetManifestResponse {
                manifest_bytes: self.manifest_bytes.as_ref().clone(),
                schema_version: mx8_core::types::MANIFEST_SCHEMA_VERSION,
            }))
        }

        async fn get_manifest_stream(
            &self,
            _request: Request<GetManifestRequest>,
        ) -> Result<Response<Self::GetManifestStreamStream>, Status> {
            let chunks = self
                .manifest_bytes
                .chunks(7)
                .map(|c| {
                    Ok(ManifestChunk {
                        data: c.to_vec(),
                        schema_version: mx8_core::types::MANIFEST_SCHEMA_VERSION,
                    })
                })
                .collect::<Vec<_>>();
            Ok(Response::new(Box::pin(tokio_stream::iter(chunks))))
        }

        async fn get_job_snapshot(
            &self,
            _request: Request<GetJobSnapshotRequest>,
        ) -> Result<Response<GetJobSnapshotResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn get_resume_checkpoint(
            &self,
            _request: Request<GetResumeCheckpointRequest>,
        ) -> Result<Response<GetResumeCheckpointResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }
    }

    fn test_args() -> Args {
        Args {
            coord_url: "http://127.0.0.1:0".to_string(),
            job_id: "job".to_string(),
            node_id: "node".to_string(),
            metrics_snapshot_interval_ms: 0,
            dev_lease_want: 0,
            batch_size_samples: 2,
            prefetch_batches: 1,
            max_queue_batches: 8,
            max_inflight_bytes: 8 * 1024 * 1024,
            target_batch_bytes: None,
            max_batch_bytes: None,
            max_process_rss_bytes: None,
            sink_sleep_ms: 0,
            progress_interval_ms: 60_000,
            max_transform_processes: 1,
            grpc_max_message_bytes: 64 * 1024 * 1024,
        }
    }

    fn test_pipeline() -> Arc<Pipeline> {
        Arc::new(Pipeline::new(RuntimeCaps {
            max_inflight_bytes: 8 * 1024 * 1024,
            max_queue_batches: 8,
            batch_size_samples: 2,
            prefetch_batches: 1,
            target_batch_bytes: None,
            max_batch_bytes: None,
            max_process_rss_bytes: None,
        }))
    }

    fn report_summary(
        reports: &[ReportProgressRequest],
        lease_id: &str,
    ) -> Option<(u64, u64, u64)> {
        let mut best: Option<(u64, u64, u64)> = None;
        for r in reports.iter().filter(|r| r.lease_id == lease_id) {
            let cand = (r.cursor, r.delivered_samples, r.delivered_bytes);
            if best.map(|b| cand.0 >= b.0).unwrap_or(true) {
                best = Some(cand);
            }
        }
        best
    }

    fn image_bytes(width: u32, height: u32, format: ImageFormat) -> Vec<u8> {
        let image = DynamicImage::new_rgb8(width, height);
        let mut cursor = Cursor::new(Vec::new());
        image.write_to(&mut cursor, format).expect("encode image");
        cursor.into_inner()
    }

    fn video_tools_available() -> bool {
        Command::new("ffmpeg")
            .arg("-version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
            && Command::new("ffprobe")
                .arg("-version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "mx8-agent-{name}-{}-{}",
            std::process::id(),
            mx8_observe::time::unix_time_ms()
        ));
        std::fs::create_dir_all(&root).expect("create temp dir");
        root
    }

    fn write_ffmpeg_test_video(path: &Path, width: u32, height: u32) {
        write_ffmpeg_test_video_custom(path, width, height, 1, "1");
    }

    fn write_ffmpeg_test_video_custom(
        path: &Path,
        width: u32,
        height: u32,
        duration_secs: u32,
        rate: &str,
    ) {
        let status = Command::new("ffmpeg")
            .arg("-y")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-f")
            .arg("lavfi")
            .arg("-i")
            .arg(format!("testsrc=size={}x{}:rate={rate}", width, height))
            .arg("-t")
            .arg(duration_secs.to_string())
            .arg("-pix_fmt")
            .arg("yuv420p")
            .arg(path)
            .status()
            .expect("run ffmpeg");
        assert!(status.success(), "ffmpeg fixture generation failed");
    }

    fn write_ffmpeg_test_video_with_audio(path: &Path, width: u32, height: u32) {
        let status = Command::new("ffmpeg")
            .arg("-y")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-f")
            .arg("lavfi")
            .arg("-i")
            .arg(format!("testsrc=size={}x{}:rate=1", width, height))
            .arg("-f")
            .arg("lavfi")
            .arg("-i")
            .arg("sine=frequency=1000:sample_rate=44100")
            .arg("-t")
            .arg("1")
            .arg("-pix_fmt")
            .arg("yuv420p")
            .arg("-c:v")
            .arg("libx264")
            .arg("-c:a")
            .arg("aac")
            .arg("-shortest")
            .arg(path)
            .status()
            .expect("run ffmpeg");
        assert!(status.success(), "ffmpeg audio fixture generation failed");
    }

    fn write_ffmpeg_test_audio(path: &Path, sample_rate: u32, channels: u32) {
        let status = Command::new("ffmpeg")
            .arg("-y")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-f")
            .arg("lavfi")
            .arg("-i")
            .arg(format!("sine=frequency=1000:sample_rate={sample_rate}"))
            .arg("-t")
            .arg("1")
            .arg("-ac")
            .arg(channels.to_string())
            .arg("-c:a")
            .arg("pcm_s16le")
            .arg(path)
            .status()
            .expect("run ffmpeg");
        assert!(status.success(), "ffmpeg audio fixture generation failed");
    }

    fn ffprobe_video_stream(path: &Path) -> (u32, u32, String) {
        let output = Command::new("ffprobe")
            .arg("-v")
            .arg("error")
            .arg("-select_streams")
            .arg("v:0")
            .arg("-show_entries")
            .arg("stream=codec_name,width,height")
            .arg("-of")
            .arg("default=noprint_wrappers=1:nokey=1")
            .arg(path)
            .output()
            .expect("run ffprobe");
        assert!(output.status.success(), "ffprobe failed");
        let stdout = String::from_utf8(output.stdout).expect("ffprobe output utf8");
        let mut lines = stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty());
        let codec = lines.next().expect("codec").to_string();
        let width = lines
            .next()
            .expect("width")
            .parse::<u32>()
            .expect("width parse");
        let height = lines
            .next()
            .expect("height")
            .parse::<u32>()
            .expect("height parse");
        (width, height, codec)
    }

    fn ffprobe_media_duration_secs(path: &Path) -> f64 {
        let output = Command::new("ffprobe")
            .arg("-v")
            .arg("error")
            .arg("-show_entries")
            .arg("format=duration")
            .arg("-of")
            .arg("default=noprint_wrappers=1:nokey=1")
            .arg(path)
            .output()
            .expect("run ffprobe");
        assert!(output.status.success(), "ffprobe failed");
        String::from_utf8(output.stdout)
            .expect("ffprobe output utf8")
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .expect("duration")
            .parse::<f64>()
            .expect("duration parse")
    }

    fn ffprobe_audio_stream(path: &Path) -> String {
        let output = Command::new("ffprobe")
            .arg("-v")
            .arg("error")
            .arg("-select_streams")
            .arg("a:0")
            .arg("-show_entries")
            .arg("stream=codec_name")
            .arg("-of")
            .arg("default=noprint_wrappers=1:nokey=1")
            .arg(path)
            .output()
            .expect("run ffprobe");
        assert!(output.status.success(), "ffprobe failed");
        String::from_utf8(output.stdout)
            .expect("ffprobe output utf8")
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .expect("audio codec")
            .to_string()
    }

    fn ffprobe_audio_details(path: &Path) -> (String, u32, u32) {
        let output = Command::new("ffprobe")
            .arg("-v")
            .arg("error")
            .arg("-select_streams")
            .arg("a:0")
            .arg("-show_entries")
            .arg("stream=codec_name,sample_rate,channels")
            .arg("-of")
            .arg("default=noprint_wrappers=1:nokey=1")
            .arg(path)
            .output()
            .expect("run ffprobe");
        assert!(output.status.success(), "ffprobe failed");
        let stdout = String::from_utf8(output.stdout).expect("ffprobe output utf8");
        let mut lines = stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty());
        let codec = lines.next().expect("audio codec").to_string();
        let sample_rate = lines
            .next()
            .expect("sample rate")
            .parse::<u32>()
            .expect("sample rate parse");
        let channels = lines
            .next()
            .expect("channels")
            .parse::<u32>()
            .expect("channels parse");
        (codec, sample_rate, channels)
    }

    #[test]
    fn image_resize_transform_rewrites_dimensions() {
        let payload = image_bytes(16, 8, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageResize {
                width: 4,
                height: 4,
                maintain_aspect: true,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.dimensions(), (4, 2));
    }

    #[test]
    fn image_crop_transform_rewrites_dimensions() {
        let payload = image_bytes(16, 8, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageCrop {
                width: 6,
                height: 4,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.dimensions(), (6, 4));
    }

    #[test]
    fn image_convert_transform_rewrites_format() {
        let payload = image_bytes(8, 8, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageConvert {
                format: "jpg".to_string(),
                quality: 80,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        assert!(transformed.starts_with(&[0xFF, 0xD8, 0xFF]));
    }

    #[test]
    fn image_resize_preserves_bmp_output_when_source_is_bmp() {
        let payload = image_bytes(10, 6, ImageFormat::Bmp);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageResize {
                width: 5,
                height: 3,
                maintain_aspect: false,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.bmp", payload.as_slice())
                .expect("transform");
        assert!(transformed.starts_with(b"BM"));
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.dimensions(), (5, 3));
    }

    #[test]
    fn image_resize_then_convert_rewrites_dimensions_and_format() {
        let payload = image_bytes(16, 8, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::ImageResize {
                    width: 4,
                    height: 4,
                    maintain_aspect: true,
                },
                TransformSpec::ImageConvert {
                    format: "webp".to_string(),
                    quality: 85,
                },
            ],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        assert_eq!(&transformed[..4], b"RIFF");
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.dimensions(), (4, 2));
    }

    #[test]
    fn image_crop_then_convert_rewrites_dimensions_and_format() {
        let payload = image_bytes(16, 8, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::ImageCrop {
                    width: 8,
                    height: 4,
                },
                TransformSpec::ImageConvert {
                    format: "jpg".to_string(),
                    quality: 80,
                },
            ],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        assert!(transformed.starts_with(&[0xFF, 0xD8, 0xFF]));
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.dimensions(), (8, 4));
    }

    #[test]
    fn image_convert_webp_preserves_alpha_images() {
        let mut image = image::RgbaImage::new(4, 4);
        image.fill(0);
        image.put_pixel(0, 0, image::Rgba([255, 0, 0, 127]));
        let mut cursor = Cursor::new(Vec::new());
        DynamicImage::ImageRgba8(image)
            .write_to(&mut cursor, ImageFormat::Png)
            .expect("encode png");
        let payload = cursor.into_inner();

        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageConvert {
                format: "webp".to_string(),
                quality: 85,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
                .expect("transform");
        assert_eq!(&transformed[..4], b"RIFF");
        let decoded = image::load_from_memory(&transformed).expect("decode output");
        assert_eq!(decoded.to_rgba8().get_pixel(0, 0)[3], 127);
    }

    #[test]
    fn image_crop_rejects_oversized_request() {
        let payload = image_bytes(4, 4, ImageFormat::Png);
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageCrop {
                width: 8,
                height: 4,
            }],
        };

        let err = transform_payload_for_sink(&spec, "s3://in/sample.png", payload.as_slice())
            .expect_err("expected oversized crop to fail");
        assert!(err.to_string().contains("exceeds current image dimensions"));
    }

    #[test]
    fn video_resize_transform_rewrites_dimensions() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-resize");
        let input_path = root.join("input.mp4");
        let output_path = root.join("output.mp4");
        write_ffmpeg_test_video(&input_path, 16, 8);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoResize {
                width: 4,
                height: 4,
                maintain_aspect: true,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.mp4", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output video");
        let (width, height, codec) = ffprobe_video_stream(&output_path);
        assert_eq!((width, height), (4, 4));
        assert_eq!(codec, "h264");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_resize_then_transcode_rewrites_dimensions_and_codec() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-resize-transcode");
        let input_path = root.join("input.mp4");
        let output_path = root.join("output.mp4");
        write_ffmpeg_test_video(&input_path, 128, 72);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::VideoResize {
                    width: 64,
                    height: 64,
                    maintain_aspect: true,
                },
                TransformSpec::VideoTranscode {
                    codec: "h265".to_string(),
                    crf: 28,
                },
            ],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.mp4", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output video");
        let (width, height, codec) = ffprobe_video_stream(&output_path);
        assert_eq!((width, height), (64, 64));
        assert_eq!(codec, "hevc");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_transcode_honors_segment_window_and_names_output_with_segment_identity() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-transcode-segment");
        let input_path = root.join("input.mp4");
        let output_path = root.join("segment.mp4");
        write_ffmpeg_test_video_custom(&input_path, 1280, 720, 4, "30");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoTranscode {
                codec: "h264".to_string(),
                crf: 23,
            }],
        };
        let sample_plan = SampleExecutionPlan {
            source_location: "s3://in/sample.mp4".to_string(),
            segment_start_ms: Some(1_000),
            segment_end_ms: Some(2_000),
        };

        let artifacts =
            render_outputs_for_sink_with_plan(&spec, &sample_plan, 21, payload.as_slice(), None)
                .expect("render outputs");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(
            artifacts[0].output_uri,
            "s3://out/sample_segment_000021_1000_2000_transcode.mp4"
        );
        std::fs::write(&output_path, &artifacts[0].payload).expect("write output video");
        let (width, height, codec) = ffprobe_video_stream(&output_path);
        assert_eq!((width, height), (1280, 720));
        assert_eq!(codec, "h264");
        let duration_secs = ffprobe_media_duration_secs(&output_path);
        assert!(
            (duration_secs - 1.0).abs() < 0.25,
            "expected ~1s segment output, got {duration_secs}s"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_audio_emits_wav_output() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-audio");
        let input_path = root.join("input.mp4");
        let output_path = root.join("output.wav");
        write_ffmpeg_test_video_with_audio(&input_path, 64, 64);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoExtractAudio {
                format: "wav".to_string(),
                bitrate: "128k".to_string(),
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.mp4", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output audio");
        let codec = ffprobe_audio_stream(&output_path);
        assert_eq!(codec, "pcm_s16le");

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_audio_then_audio_resample_rewrites_rate_and_channels() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-audio-chain");
        let input_path = root.join("input.mp4");
        let output_path = root.join("output.wav");
        write_ffmpeg_test_video_with_audio(&input_path, 64, 64);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::VideoExtractAudio {
                    format: "wav".to_string(),
                    bitrate: "128k".to_string(),
                },
                TransformSpec::AudioResample {
                    rate: 16_000,
                    channels: 1,
                },
            ],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.mp4", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output audio");
        let (codec, sample_rate, channels) = ffprobe_audio_details(&output_path);
        assert_eq!(codec, "pcm_s16le");
        assert_eq!(sample_rate, 16_000);
        assert_eq!(channels, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_emits_multiple_png_outputs() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_custom(&input_path, 64, 64, 2, "4");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoExtractFrames {
                fps: 2.0,
                format: "png".to_string(),
            }],
        };

        let artifacts =
            render_outputs_for_sink(&spec, "s3://in/sample.mp4", 7, payload.as_slice(), None)
                .expect("render outputs");
        assert_eq!(artifacts.len(), 4);
        assert_eq!(artifacts[0].output_uri, "s3://out/sample_frames_000000.png");
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.output_uri.ends_with(".png")));
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.payload.starts_with(&[0x89, b'P', b'N', b'G'])));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_handles_long_mp4_inputs() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames-long");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_custom(&input_path, 1280, 720, 30, "30");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoExtractFrames {
                fps: 1.0,
                format: "jpg".to_string(),
            }],
        };

        let artifacts =
            render_outputs_for_sink(&spec, "s3://in/sample.mp4", 11, payload.as_slice(), None)
                .expect("render outputs");
        assert_eq!(artifacts.len(), 30);
        assert_eq!(artifacts[0].output_uri, "s3://out/sample_frames_000000.jpg");
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.output_uri.ends_with(".jpg")));
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.payload.starts_with(&[0xFF, 0xD8, 0xFF])));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_honors_segment_window_and_names_outputs_with_segment_identity() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames-segment");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_custom(&input_path, 64, 64, 4, "2");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoExtractFrames {
                fps: 2.0,
                format: "jpg".to_string(),
            }],
        };
        let sample_plan = SampleExecutionPlan {
            source_location: "s3://in/sample.mp4".to_string(),
            segment_start_ms: Some(1_000),
            segment_end_ms: Some(2_000),
        };

        let artifacts =
            render_outputs_for_sink_with_plan(&spec, &sample_plan, 12, payload.as_slice(), None)
                .expect("render outputs");
        assert_eq!(artifacts.len(), 2);
        assert_eq!(
            artifacts[0].output_uri,
            "s3://out/sample_segment_000012_1000_2000_frames_000000.jpg"
        );
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.output_uri.contains("segment_000012_1000_2000")));
        assert!(artifacts
            .iter()
            .all(|artifact| artifact.payload.starts_with(&[0xFF, 0xD8, 0xFF])));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_segment_outputs_do_not_collide_for_same_source() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames-segment-collision");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_custom(&input_path, 64, 64, 4, "2");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoExtractFrames {
                fps: 2.0,
                format: "jpg".to_string(),
            }],
        };

        let first_plan = SampleExecutionPlan {
            source_location: "s3://in/sample.mp4".to_string(),
            segment_start_ms: Some(0),
            segment_end_ms: Some(1_000),
        };
        let second_plan = SampleExecutionPlan {
            source_location: "s3://in/sample.mp4".to_string(),
            segment_start_ms: Some(2_000),
            segment_end_ms: Some(3_000),
        };

        let first =
            render_outputs_for_sink_with_plan(&spec, &first_plan, 3, payload.as_slice(), None)
                .expect("first render");
        let second =
            render_outputs_for_sink_with_plan(&spec, &second_plan, 4, payload.as_slice(), None)
                .expect("second render");

        assert!(!first.is_empty());
        assert!(!second.is_empty());
        assert_ne!(first[0].output_uri, second[0].output_uri);
        assert!(first[0].output_uri.contains("segment_000003_0_1000"));
        assert!(second[0].output_uri.contains("segment_000004_2000_3000"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_then_image_resize_emits_resized_outputs() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames-chain");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_custom(&input_path, 64, 64, 2, "2");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::VideoExtractFrames {
                    fps: 1.0,
                    format: "jpg".to_string(),
                },
                TransformSpec::ImageResize {
                    width: 16,
                    height: 16,
                    maintain_aspect: false,
                },
            ],
        };

        let artifacts =
            render_outputs_for_sink(&spec, "s3://in/sample.mp4", 9, payload.as_slice(), None)
                .expect("render outputs");
        assert_eq!(artifacts.len(), 2);
        assert_eq!(artifacts[0].output_uri, "s3://out/sample_frames_000000.jpg");
        for artifact in artifacts {
            assert!(artifact.payload.starts_with(&[0xFF, 0xD8, 0xFF]));
            let decoded = image::load_from_memory(&artifact.payload).expect("decode resized frame");
            assert_eq!(decoded.dimensions(), (16, 16));
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    #[ignore = "benchmark"]
    fn profile_video_extract_frames_transform_throughput() {
        if !video_tools_available() {
            return;
        }

        for duration_secs in [60_u32, 120_u32] {
            let root = temp_test_dir(&format!("video-extract-frames-bench-{duration_secs}"));
            let input_path = root.join("input.mp4");
            let output_root = root.join("out");
            write_ffmpeg_test_video_custom(&input_path, 1280, 720, duration_secs, "30");
            let payload = std::fs::read(&input_path).expect("read input video");
            let spec = CoreJobSpec {
                job_id: "job".to_string(),
                source_uri: "file:///input".to_string(),
                sink_uri: format!("file://{}", output_root.display()),
                aws_region: "us-east-1".to_string(),
                transforms: vec![TransformSpec::VideoExtractFrames {
                    fps: 1.0,
                    format: "jpg".to_string(),
                }],
            };

            let render_started = Instant::now();
            let artifacts = render_outputs_for_sink(
                &spec,
                "file:///input/sample.mp4",
                1,
                payload.as_slice(),
                None,
            )
            .expect("render outputs");
            let render_elapsed = render_started.elapsed();

            let artifact_count = artifacts.len();
            let output_bytes: usize = artifacts
                .iter()
                .map(|artifact| artifact.payload.len())
                .sum();

            let write_started = Instant::now();
            for artifact in &artifacts {
                let path = super::parse_file_uri(&artifact.output_uri).expect("parse file uri");
                super::write_file_output(&path, &artifact.payload).expect("write file output");
            }
            let write_elapsed = write_started.elapsed();

            let total_elapsed = render_elapsed + write_elapsed;
            let realtime_multiple = duration_secs as f64 / total_elapsed.as_secs_f64();
            let outputs_per_sec = artifact_count as f64 / total_elapsed.as_secs_f64();

            println!(
                "extract_frames_transform duration={}s outputs={} output_bytes={} render_ms={} write_ms={} total_ms={} realtime_x={:.2} outputs_per_sec={:.2}",
                duration_secs,
                artifact_count,
                output_bytes,
                render_elapsed.as_millis(),
                write_elapsed.as_millis(),
                total_elapsed.as_millis(),
                realtime_multiple,
                outputs_per_sec,
            );

            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[test]
    #[ignore = "benchmark"]
    fn profile_video_transcode_transform_throughput() {
        if !video_tools_available() {
            return;
        }

        for duration_secs in [60_u32, 120_u32] {
            let root = temp_test_dir(&format!("video-transcode-bench-{duration_secs}"));
            let input_path = root.join("input.mp4");
            let output_root = root.join("out");
            write_ffmpeg_test_video_custom(&input_path, 1280, 720, duration_secs, "30");
            let payload = std::fs::read(&input_path).expect("read input video");
            let spec = CoreJobSpec {
                job_id: "job".to_string(),
                source_uri: "file:///input".to_string(),
                sink_uri: format!("file://{}", output_root.display()),
                aws_region: "us-east-1".to_string(),
                transforms: vec![TransformSpec::VideoTranscode {
                    codec: "h264".to_string(),
                    crf: 23,
                }],
            };

            let render_started = Instant::now();
            let artifacts = render_outputs_for_sink(
                &spec,
                "file:///input/sample.mp4",
                1,
                payload.as_slice(),
                None,
            )
            .expect("render outputs");
            let render_elapsed = render_started.elapsed();

            let artifact_count = artifacts.len();
            let output_bytes: usize = artifacts
                .iter()
                .map(|artifact| artifact.payload.len())
                .sum();

            let write_started = Instant::now();
            for artifact in &artifacts {
                let path = super::parse_file_uri(&artifact.output_uri).expect("parse file uri");
                super::write_file_output(&path, &artifact.payload).expect("write file output");
            }
            let write_elapsed = write_started.elapsed();

            let total_elapsed = render_elapsed + write_elapsed;
            let realtime_multiple = duration_secs as f64 / total_elapsed.as_secs_f64();

            println!(
                "video_transcode_transform duration={}s outputs={} output_bytes={} render_ms={} write_ms={} total_ms={} realtime_x={:.2}",
                duration_secs,
                artifact_count,
                output_bytes,
                render_elapsed.as_millis(),
                write_elapsed.as_millis(),
                total_elapsed.as_millis(),
                realtime_multiple,
            );

            let _ = std::fs::remove_dir_all(root);
        }
    }

    #[test]
    #[ignore = "benchmark"]
    fn profile_video_segment_export_transform_throughput() {
        if !video_tools_available() {
            return;
        }

        let input_duration_secs = 150_u32;
        let root = temp_test_dir("video-segment-export-bench");
        let input_path = root.join("input.mp4");
        let output_root = root.join("out");
        write_ffmpeg_test_video_custom(&input_path, 1280, 720, input_duration_secs, "30");
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "file:///input".to_string(),
            sink_uri: format!("file://{}", output_root.display()),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::VideoTranscode {
                codec: "h264".to_string(),
                crf: 23,
            }],
        };

        for clip_duration_secs in [20_u32, 60_u32] {
            let sample_plan = SampleExecutionPlan {
                source_location: "file:///input/sample.mp4".to_string(),
                segment_start_ms: Some(10_000),
                segment_end_ms: Some(10_000 + (clip_duration_secs as u64 * 1_000)),
            };

            let render_started = Instant::now();
            let artifacts = render_outputs_for_sink_with_plan(
                &spec,
                &sample_plan,
                clip_duration_secs as u64,
                payload.as_slice(),
                None,
            )
            .expect("render outputs");
            let render_elapsed = render_started.elapsed();

            let artifact_count = artifacts.len();
            let output_bytes: usize = artifacts
                .iter()
                .map(|artifact| artifact.payload.len())
                .sum();

            let write_started = Instant::now();
            for artifact in &artifacts {
                let path = super::parse_file_uri(&artifact.output_uri).expect("parse file uri");
                super::write_file_output(&path, &artifact.payload).expect("write file output");
            }
            let write_elapsed = write_started.elapsed();

            let total_elapsed = render_elapsed + write_elapsed;
            let realtime_multiple = clip_duration_secs as f64 / total_elapsed.as_secs_f64();

            println!(
                "video_segment_export_transform clip_duration={}s outputs={} output_bytes={} render_ms={} write_ms={} total_ms={} realtime_x={:.2}",
                clip_duration_secs,
                artifact_count,
                output_bytes,
                render_elapsed.as_millis(),
                write_elapsed.as_millis(),
                total_elapsed.as_millis(),
                realtime_multiple,
            );
        }

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_audio_rejects_non_audio_followups() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-audio-invalid-chain");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video_with_audio(&input_path, 64, 64);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::VideoExtractAudio {
                    format: "wav".to_string(),
                    bitrate: "128k".to_string(),
                },
                TransformSpec::ImageResize {
                    width: 16,
                    height: 16,
                    maintain_aspect: false,
                },
            ],
        };

        let err = transform_payload_for_sink(&spec, "s3://in/sample.mp4", payload.as_slice())
            .expect_err("expected invalid extract_audio followup to fail");
        assert!(err
            .to_string()
            .contains("video.extract_audio can only be followed by audio transforms"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn video_extract_frames_rejects_non_image_followups() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("video-extract-frames-invalid-chain");
        let input_path = root.join("input.mp4");
        write_ffmpeg_test_video(&input_path, 64, 64);
        let payload = std::fs::read(&input_path).expect("read input video");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::VideoExtractFrames {
                    fps: 1.0,
                    format: "jpg".to_string(),
                },
                TransformSpec::AudioNormalize {
                    loudness_lufs: -14.0,
                },
            ],
        };

        let err = render_outputs_for_sink(&spec, "s3://in/sample.mp4", 9, payload.as_slice(), None)
            .expect_err("expected invalid extract_frames followup to fail");
        assert!(err
            .to_string()
            .contains("video.extract_frames can only be followed by image transforms"));

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn audio_resample_rewrites_rate_and_channels() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("audio-resample");
        let input_path = root.join("input.wav");
        let output_path = root.join("output.wav");
        write_ffmpeg_test_audio(&input_path, 48_000, 2);
        let payload = std::fs::read(&input_path).expect("read input audio");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::AudioResample {
                rate: 16_000,
                channels: 1,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.wav", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output audio");
        let (codec, sample_rate, channels) = ffprobe_audio_details(&output_path);
        assert_eq!(codec, "pcm_s16le");
        assert_eq!(sample_rate, 16_000);
        assert_eq!(channels, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn audio_normalize_preserves_audio_container() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("audio-normalize");
        let input_path = root.join("input.wav");
        let output_path = root.join("output.wav");
        write_ffmpeg_test_audio(&input_path, 44_100, 2);
        let payload = std::fs::read(&input_path).expect("read input audio");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::AudioNormalize {
                loudness_lufs: -18.0,
            }],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.wav", payload.as_slice())
                .expect("transform");
        assert_ne!(transformed, payload);
        std::fs::write(&output_path, transformed).expect("write output audio");
        let (codec, sample_rate, channels) = ffprobe_audio_details(&output_path);
        assert_eq!(codec, "pcm_s16le");
        assert_eq!(sample_rate, 44_100);
        assert_eq!(channels, 2);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn audio_resample_then_normalize_rewrites_rate_and_channels() {
        if !video_tools_available() {
            return;
        }

        let root = temp_test_dir("audio-resample-normalize");
        let input_path = root.join("input.wav");
        let output_path = root.join("output.wav");
        write_ffmpeg_test_audio(&input_path, 48_000, 2);
        let payload = std::fs::read(&input_path).expect("read input audio");
        let spec = CoreJobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![
                TransformSpec::AudioResample {
                    rate: 22_050,
                    channels: 1,
                },
                TransformSpec::AudioNormalize {
                    loudness_lufs: -16.0,
                },
            ],
        };

        let transformed =
            transform_payload_for_sink(&spec, "s3://in/sample.wav", payload.as_slice())
                .expect("transform");
        std::fs::write(&output_path, transformed).expect("write output audio");
        let (codec, sample_rate, channels) = ffprobe_audio_details(&output_path);
        assert_eq!(codec, "pcm_s16le");
        assert_eq!(sample_rate, 22_050);
        assert_eq!(channels, 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn image_resize_normalizes_tif_extension_for_encoding() {
        let extension = output_extension(
            &[TransformSpec::ImageResize {
                width: 8,
                height: 8,
                maintain_aspect: true,
            }],
            "s3://in/sample.tif",
        );
        assert_eq!(normalized_image_extension(&extension), "tiff");
    }

    #[test]
    fn manifest_stream_truncated_fails_closed() {
        let mut parser =
            mx8_runtime::pipeline::ManifestRangeStreamParser::new(0, 2, 1024).expect("parser init");
        parser
            .push_chunk(
                format!(
                    "schema_version={}\n0\tloc0\t\t\t\n1\t",
                    mx8_core::types::MANIFEST_SCHEMA_VERSION
                )
                .as_bytes(),
            )
            .expect("push");
        let err = parser.finish().unwrap_err();
        assert!(err.to_string().contains("expected at least 2 columns"));
    }

    #[test]
    fn manifest_stream_schema_mismatch_hard_fails() {
        let mut parser =
            mx8_runtime::pipeline::ManifestRangeStreamParser::new(0, 1, 1024).expect("parser init");
        let mut stream_schema: Option<u32> = None;

        let chunks = vec![
            (
                mx8_core::types::MANIFEST_SCHEMA_VERSION,
                format!(
                    "schema_version={}\n",
                    mx8_core::types::MANIFEST_SCHEMA_VERSION
                )
                .into_bytes(),
            ),
            (
                mx8_core::types::MANIFEST_SCHEMA_VERSION + 1,
                b"0\tloc0\t\t\t\n".to_vec(),
            ),
        ];

        let mut got_mismatch = false;
        for (schema, payload) in chunks {
            if let Some(existing) = stream_schema {
                if schema != existing {
                    got_mismatch = true;
                    break;
                }
            } else {
                stream_schema = Some(schema);
            }
            parser.push_chunk(payload.as_slice()).expect("push chunk");
        }
        assert!(got_mismatch, "schema mismatch should hard-fail");
    }

    #[test]
    fn manifest_stream_parity_with_cached_path() {
        let manifest = format!(
            "schema_version={}\n0\tloc0\t\t\t\n1\tloc1\t\t\t\n2\tloc2\t\t\t\n3\tloc3\t\t\t\n",
            mx8_core::types::MANIFEST_SCHEMA_VERSION
        )
        .into_bytes();

        let full = mx8_runtime::pipeline::load_manifest_records_from_read(std::io::Cursor::new(
            manifest.clone(),
        ))
        .expect("full parse");
        let expected = full[1..3].to_vec();

        let mut parser =
            mx8_runtime::pipeline::ManifestRangeStreamParser::new(1, 3, 1024).expect("parser init");
        for ch in manifest.chunks(3) {
            parser.push_chunk(ch).expect("push chunk");
        }
        let got = parser.finish().expect("finish parse");
        assert_eq!(got, expected);
    }

    #[test]
    fn manifest_stream_backpressure_bounded() {
        let mut parser =
            mx8_runtime::pipeline::ManifestRangeStreamParser::new(0, 1, 8).expect("parser init");
        let err = parser.push_chunk(b"schema_version=1").unwrap_err();
        assert!(err.to_string().contains("manifest line exceeds max bytes"));
    }

    #[tokio::test]
    async fn lease_manifest_stream_matches_expected_progress() -> anyhow::Result<()> {
        let mut root = std::env::temp_dir();
        root.push(format!(
            "mx8-agent-lease-parity-{}-{}",
            std::process::id(),
            mx8_observe::time::unix_time_ms()
        ));
        std::fs::create_dir_all(&root)?;

        let mut manifest = String::new();
        manifest.push_str(&format!(
            "schema_version={}\n",
            mx8_core::types::MANIFEST_SCHEMA_VERSION
        ));
        for i in 0..5u64 {
            let p = root.join(format!("sample-{i}.bin"));
            std::fs::write(&p, [i as u8, (i.wrapping_mul(2)) as u8])?;
            manifest.push_str(&format!("{i}\t{}\t\t\t\n", p.display()));
        }
        let manifest_bytes = manifest.into_bytes();
        let mock = MockCoordinator::new(manifest_bytes.clone());
        let reports = mock.reports.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let incoming = TcpListenerStream::new(listener);
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(CoordinatorServer::new(mock))
                .serve_with_incoming(incoming)
                .await
        });

        let channel = Channel::from_shared(format!("http://{}", addr))?
            .connect()
            .await?;
        let args = test_args();
        let metrics = Arc::new(AgentMetrics::default());
        let pipeline = test_pipeline();
        let range = WorkRange {
            start_id: 1,
            end_id: 4,
            epoch: 1,
            seed: 0,
        };
        let stream_lease = Lease {
            lease_id: "lease-stream".to_string(),
            node_id: args.node_id.clone(),
            range: Some(range.clone()),
            cursor: range.start_id,
            expires_unix_time_ms: 0,
        };
        let manifest_hash = "manifest-h";

        run_lease(
            channel.clone(),
            pipeline,
            ManifestSource::DirectStream,
            &args,
            manifest_hash,
            &metrics,
            stream_lease,
            None,
            Arc::new(TransformProcessLimiter::new(1)),
        )
        .await?;

        let snapshot = reports
            .lock()
            .map_err(|_| anyhow::anyhow!("report lock poisoned"))?
            .clone();
        let direct = report_summary(&snapshot, "lease-stream")
            .ok_or_else(|| anyhow::anyhow!("missing direct-stream progress reports"))?;

        assert_eq!(direct.0, range.end_id);
        assert_eq!(direct.1, range.end_id - range.start_id);
        assert_eq!(direct.2, 6);

        server.abort();
        let _ = std::fs::remove_dir_all(root);
        Ok(())
    }
}
