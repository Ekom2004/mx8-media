use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ManifestHash(pub String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    pub job_id: String,
    pub source_uri: String,
    pub sink_uri: String,
    pub aws_region: String,
    pub transforms: Vec<TransformSpec>,
}

impl JobSpec {
    pub fn validate(&self) -> Result<(), JobSpecError> {
        if self.job_id.trim().is_empty() {
            return Err(JobSpecError::EmptyJobId);
        }
        if self.source_uri.trim().is_empty() {
            return Err(JobSpecError::EmptySourceUri);
        }
        if self.sink_uri.trim().is_empty() {
            return Err(JobSpecError::EmptySinkUri);
        }
        if self.aws_region.trim().is_empty() {
            return Err(JobSpecError::EmptyAwsRegion);
        }
        if self.transforms.is_empty() {
            return Err(JobSpecError::EmptyTransforms);
        }
        for (index, transform) in self.transforms.iter().enumerate() {
            transform
                .validate()
                .map_err(|source| JobSpecError::InvalidTransform { index, source })?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransformSpec {
    VideoTranscode {
        codec: String,
        crf: u32,
    },
    VideoResize {
        width: u32,
        height: u32,
        maintain_aspect: bool,
    },
    VideoExtractFrames {
        fps: f32,
        format: String,
    },
    VideoExtractAudio {
        format: String,
        bitrate: String,
    },
    ImageResize {
        width: u32,
        height: u32,
        maintain_aspect: bool,
    },
    ImageCrop {
        width: u32,
        height: u32,
    },
    ImageConvert {
        format: String,
        quality: u32,
    },
    AudioResample {
        rate: u32,
        channels: u32,
    },
    AudioNormalize {
        loudness_lufs: f32,
    },
}

impl TransformSpec {
    pub fn validate(&self) -> Result<(), TransformSpecError> {
        match self {
            Self::VideoTranscode { codec, crf } => {
                if codec.trim().is_empty() {
                    return Err(TransformSpecError::EmptyStringField("codec"));
                }
                if *crf > 51 {
                    return Err(TransformSpecError::CrfOutOfRange(*crf));
                }
            }
            Self::VideoResize { width, height, .. }
            | Self::ImageResize { width, height, .. }
            | Self::ImageCrop { width, height } => {
                validate_positive_dimension("width", *width)?;
                validate_positive_dimension("height", *height)?;
            }
            Self::VideoExtractFrames { fps, format } => {
                if !fps.is_finite() || *fps <= 0.0 {
                    return Err(TransformSpecError::FpsOutOfRange(*fps));
                }
                if format.trim().is_empty() {
                    return Err(TransformSpecError::EmptyStringField("format"));
                }
            }
            Self::VideoExtractAudio { format, bitrate } => {
                if format.trim().is_empty() {
                    return Err(TransformSpecError::EmptyStringField("format"));
                }
                if bitrate.trim().is_empty() {
                    return Err(TransformSpecError::EmptyStringField("bitrate"));
                }
            }
            Self::ImageConvert { format, quality } => {
                validate_image_format(format)?;
                if *quality == 0 || *quality > 100 {
                    return Err(TransformSpecError::ImageQualityOutOfRange(*quality));
                }
            }
            Self::AudioResample { rate, channels } => {
                if *rate == 0 {
                    return Err(TransformSpecError::InvalidUnsignedField("rate"));
                }
                if *channels == 0 {
                    return Err(TransformSpecError::InvalidUnsignedField("channels"));
                }
            }
            Self::AudioNormalize { loudness_lufs } => {
                if !loudness_lufs.is_finite() {
                    return Err(TransformSpecError::InvalidLoudness(*loudness_lufs));
                }
            }
        }
        Ok(())
    }
}

fn validate_positive_dimension(field: &'static str, value: u32) -> Result<(), TransformSpecError> {
    if value == 0 {
        return Err(TransformSpecError::InvalidUnsignedField(field));
    }
    Ok(())
}

pub fn normalize_image_format(format: &str) -> Option<&'static str> {
    match format.trim().to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => Some("jpg"),
        "png" => Some("png"),
        "webp" => Some("webp"),
        _ => None,
    }
}

fn validate_image_format(format: &str) -> Result<(), TransformSpecError> {
    if normalize_image_format(format).is_none() {
        return Err(TransformSpecError::UnsupportedImageFormat(
            format.trim().to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkRange {
    pub start_id: u64,
    pub end_id: u64, // half-open [start_id, end_id)
    pub epoch: u32,
    pub seed: u64,
}

impl WorkRange {
    pub fn len(&self) -> u64 {
        self.end_id.saturating_sub(self.start_id)
    }

    pub fn is_empty(&self) -> bool {
        self.start_id >= self.end_id
    }

    pub fn contains(&self, sample_id: u64) -> bool {
        self.start_id <= sample_id && sample_id < self.end_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub lease_id: LeaseId,
    pub node_id: NodeId,
    pub range: WorkRange,
    pub cursor: u64,
    /// Lease expiration timestamp in Unix milliseconds.
    pub expires_unix_time_ms: u64,
}

/// Operator-configured per-node caps enforced by `mx8d-agent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCaps {
    pub max_fetch_concurrency: u32,
    pub max_decode_concurrency: u32,
    pub max_inflight_bytes: u64,
    pub max_ram_bytes: u64,
}

/// Periodic node stats reported to the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStats {
    pub inflight_bytes: u64,
    pub ram_high_water_bytes: u64,
    pub fetch_queue_depth: u32,
    pub decode_queue_depth: u32,
    pub pack_queue_depth: u32,
}

/// Progress is reported in terms of the lease cursor.
///
/// v0 cursor semantics: cursor advances only after DELIVER (consumer receives the batch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProgressReport {
    pub job_id: JobId,
    pub node_id: NodeId,
    pub lease_id: LeaseId,
    pub cursor: u64,
    pub delivered_samples: u64,
    pub delivered_bytes: u64,
    pub unix_time_ms: u64,
}

/// v0 logical manifest schema version. Physical Parquet layout is defined later.
pub const MANIFEST_SCHEMA_VERSION: u32 = 0;

/// A single logical sample in the pinned snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub sample_id: u64,
    pub location: String,
    pub byte_offset: Option<u64>,
    pub byte_length: Option<u64>,
    pub decode_hint: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ManifestRecordError {
    #[error("location must be non-empty")]
    EmptyLocation,
    #[error("byte_offset and byte_length must be set together (both Some or both None)")]
    PartialByteRange,
    #[error("byte_length must be > 0 when byte range is specified")]
    NonPositiveByteLength,
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum JobSpecError {
    #[error("job_id must be non-empty")]
    EmptyJobId,
    #[error("source_uri must be non-empty")]
    EmptySourceUri,
    #[error("sink_uri must be non-empty")]
    EmptySinkUri,
    #[error("aws_region must be non-empty")]
    EmptyAwsRegion,
    #[error("transforms must be non-empty")]
    EmptyTransforms,
    #[error("invalid transform at index {index}: {source}")]
    InvalidTransform {
        index: usize,
        #[source]
        source: TransformSpecError,
    },
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum TransformSpecError {
    #[error("{0} must be non-empty")]
    EmptyStringField(&'static str),
    #[error("{0} must be > 0")]
    InvalidUnsignedField(&'static str),
    #[error("fps must be finite and > 0 (got {0})")]
    FpsOutOfRange(f32),
    #[error("image format must be one of jpg, png, webp (got {0})")]
    UnsupportedImageFormat(String),
    #[error("image quality must be in 1..=100 (got {0})")]
    ImageQualityOutOfRange(u32),
    #[error("video crf must be in 0..=51 (got {0})")]
    CrfOutOfRange(u32),
    #[error("audio loudness must be finite (got {0})")]
    InvalidLoudness(f32),
}

impl ManifestRecord {
    pub fn validate(&self) -> Result<(), ManifestRecordError> {
        if self.location.trim().is_empty() {
            return Err(ManifestRecordError::EmptyLocation);
        }

        match (self.byte_offset, self.byte_length) {
            (None, None) => Ok(()),
            (Some(_), Some(len)) => {
                if len == 0 {
                    return Err(ManifestRecordError::NonPositiveByteLength);
                }
                Ok(())
            }
            _ => Err(ManifestRecordError::PartialByteRange),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobSpec, JobSpecError, TransformSpec, TransformSpecError};

    fn base_job_spec() -> JobSpec {
        JobSpec {
            job_id: "job".to_string(),
            source_uri: "s3://in/".to_string(),
            sink_uri: "s3://out/".to_string(),
            aws_region: "us-east-1".to_string(),
            transforms: vec![TransformSpec::ImageResize {
                width: 64,
                height: 64,
                maintain_aspect: true,
            }],
        }
    }

    #[test]
    fn job_spec_validate_rejects_empty_transforms() {
        let mut spec = base_job_spec();
        spec.transforms.clear();
        let err = spec
            .validate()
            .expect_err("expected empty transforms to fail");
        assert_eq!(err, JobSpecError::EmptyTransforms);
    }

    #[test]
    fn image_resize_validate_rejects_zero_dimensions() {
        let err = TransformSpec::ImageResize {
            width: 0,
            height: 64,
            maintain_aspect: true,
        }
        .validate()
        .expect_err("expected invalid width");
        assert_eq!(err, TransformSpecError::InvalidUnsignedField("width"));
    }

    #[test]
    fn image_crop_validate_rejects_zero_dimensions() {
        let err = TransformSpec::ImageCrop {
            width: 32,
            height: 0,
        }
        .validate()
        .expect_err("expected invalid height");
        assert_eq!(err, TransformSpecError::InvalidUnsignedField("height"));
    }

    #[test]
    fn image_convert_validate_rejects_unsupported_format() {
        let err = TransformSpec::ImageConvert {
            format: "gif".to_string(),
            quality: 85,
        }
        .validate()
        .expect_err("expected unsupported format");
        assert_eq!(
            err,
            TransformSpecError::UnsupportedImageFormat("gif".to_string())
        );
    }

    #[test]
    fn image_convert_validate_rejects_invalid_quality() {
        let err = TransformSpec::ImageConvert {
            format: "jpg".to_string(),
            quality: 101,
        }
        .validate()
        .expect_err("expected invalid quality");
        assert_eq!(err, TransformSpecError::ImageQualityOutOfRange(101));
    }
}
