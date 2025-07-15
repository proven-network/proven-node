//! Metrics collection for S3 storage

use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, register_histogram_vec, register_int_counter_vec,
    register_int_gauge_vec,
};
use std::sync::OnceLock;

static METRICS: OnceLock<S3StorageMetrics> = OnceLock::new();

/// S3 storage metrics
pub struct S3StorageMetrics {
    /// Operation count by type and status
    pub operations: IntCounterVec,

    /// Operation latency by type
    pub latency: HistogramVec,

    /// Batch metrics
    pub batch_size: HistogramVec,
    pub batch_entries: HistogramVec,

    /// Cache metrics
    pub cache_hits: IntCounterVec,
    pub cache_misses: IntCounterVec,
    pub cache_evictions: IntCounterVec,
    pub cache_size_bytes: IntGaugeVec,

    /// WAL metrics
    pub wal_pending_bytes: IntGaugeVec,
    pub wal_operations: IntCounterVec,
    pub wal_latency: HistogramVec,

    /// S3 metrics
    pub s3_requests: IntCounterVec,
    pub s3_bytes_uploaded: IntCounterVec,
    pub s3_bytes_downloaded: IntCounterVec,

    /// Encryption metrics
    pub encryption_operations: IntCounterVec,
    pub compression_ratio: HistogramVec,
}

impl S3StorageMetrics {
    /// Initialize metrics
    pub fn init() -> &'static Self {
        METRICS.get_or_init(|| {
            let operations = register_int_counter_vec!(
                "s3_storage_operations_total",
                "Total number of storage operations",
                &["operation", "status", "namespace"]
            )
            .unwrap();

            let latency = register_histogram_vec!(
                "s3_storage_operation_duration_seconds",
                "Storage operation latency",
                &["operation", "namespace"],
                vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]
            )
            .unwrap();

            let batch_size = register_histogram_vec!(
                "s3_storage_batch_size_bytes",
                "Size of batches in bytes",
                &["namespace"],
                vec![1000.0, 10000.0, 100000.0, 1000000.0, 5000000.0, 10000000.0]
            )
            .unwrap();

            let batch_entries = register_histogram_vec!(
                "s3_storage_batch_entries",
                "Number of entries in batches",
                &["namespace"],
                vec![1.0, 10.0, 50.0, 100.0, 500.0, 1000.0, 5000.0]
            )
            .unwrap();

            let cache_hits = register_int_counter_vec!(
                "s3_storage_cache_hits_total",
                "Total cache hits",
                &["namespace"]
            )
            .unwrap();

            let cache_misses = register_int_counter_vec!(
                "s3_storage_cache_misses_total",
                "Total cache misses",
                &["namespace"]
            )
            .unwrap();

            let cache_evictions = register_int_counter_vec!(
                "s3_storage_cache_evictions_total",
                "Total cache evictions",
                &["namespace"]
            )
            .unwrap();

            let cache_size_bytes = register_int_gauge_vec!(
                "s3_storage_cache_size_bytes",
                "Current cache size in bytes",
                &["namespace"]
            )
            .unwrap();

            let wal_pending_bytes = register_int_gauge_vec!(
                "s3_storage_wal_pending_bytes",
                "Bytes pending in WAL",
                &["namespace"]
            )
            .unwrap();

            let wal_operations = register_int_counter_vec!(
                "s3_storage_wal_operations_total",
                "Total WAL operations",
                &["operation", "status"]
            )
            .unwrap();

            let wal_latency = register_histogram_vec!(
                "s3_storage_wal_duration_seconds",
                "WAL operation latency",
                &["operation"],
                vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]
            )
            .unwrap();

            let s3_requests = register_int_counter_vec!(
                "s3_storage_s3_requests_total",
                "Total S3 requests",
                &["operation", "status"]
            )
            .unwrap();

            let s3_bytes_uploaded = register_int_counter_vec!(
                "s3_storage_s3_bytes_uploaded_total",
                "Total bytes uploaded to S3",
                &["namespace"]
            )
            .unwrap();

            let s3_bytes_downloaded = register_int_counter_vec!(
                "s3_storage_s3_bytes_downloaded_total",
                "Total bytes downloaded from S3",
                &["namespace"]
            )
            .unwrap();

            let encryption_operations = register_int_counter_vec!(
                "s3_storage_encryption_operations_total",
                "Total encryption operations",
                &["operation", "target"]
            )
            .unwrap();

            let compression_ratio = register_histogram_vec!(
                "s3_storage_compression_ratio",
                "Compression ratio (original/compressed)",
                &["namespace"],
                vec![1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 20.0]
            )
            .unwrap();

            Self {
                operations,
                latency,
                batch_size,
                batch_entries,
                cache_hits,
                cache_misses,
                cache_evictions,
                cache_size_bytes,
                wal_pending_bytes,
                wal_operations,
                wal_latency,
                s3_requests,
                s3_bytes_uploaded,
                s3_bytes_downloaded,
                encryption_operations,
                compression_ratio,
            }
        })
    }

    /// Get the global metrics instance
    pub fn get() -> &'static Self {
        Self::init()
    }
}

/// Helper to time an operation
pub struct Timer {
    metric: HistogramVec,
    labels: Vec<String>,
    start: std::time::Instant,
}

impl Timer {
    /// Create a new timer
    pub fn new(metric: &HistogramVec, labels: &[&str]) -> Self {
        Self {
            metric: metric.clone(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            start: std::time::Instant::now(),
        }
    }

    /// Record the elapsed time
    pub fn record(self) {
        let duration = self.start.elapsed().as_secs_f64();
        let label_refs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        self.metric.with_label_values(&label_refs).observe(duration);
    }
}

/// Record a storage operation
pub fn record_operation(operation: &str, namespace: &str, success: bool) {
    let metrics = S3StorageMetrics::get();
    let status = if success { "success" } else { "error" };
    metrics
        .operations
        .with_label_values(&[operation, status, namespace])
        .inc();
}

/// Start timing an operation
pub fn start_timer(operation: &str, namespace: &str) -> Timer {
    let metrics = S3StorageMetrics::get();
    Timer::new(&metrics.latency, &[operation, namespace])
}

/// Record batch metrics
pub fn record_batch(namespace: &str, size_bytes: usize, entry_count: usize) {
    let metrics = S3StorageMetrics::get();
    metrics
        .batch_size
        .with_label_values(&[namespace])
        .observe(size_bytes as f64);
    metrics
        .batch_entries
        .with_label_values(&[namespace])
        .observe(entry_count as f64);
}

/// Record cache hit
pub fn record_cache_hit(namespace: &str) {
    let metrics = S3StorageMetrics::get();
    metrics.cache_hits.with_label_values(&[namespace]).inc();
}

/// Record cache miss
pub fn record_cache_miss(namespace: &str) {
    let metrics = S3StorageMetrics::get();
    metrics.cache_misses.with_label_values(&[namespace]).inc();
}

/// Record S3 upload
pub fn record_s3_upload(namespace: &str, bytes: usize, success: bool) {
    let metrics = S3StorageMetrics::get();
    let status = if success { "success" } else { "error" };
    metrics
        .s3_requests
        .with_label_values(&["put", status])
        .inc();

    if success {
        metrics
            .s3_bytes_uploaded
            .with_label_values(&[namespace])
            .inc_by(bytes as u64);
    }
}

/// Record S3 download
pub fn record_s3_download(namespace: &str, bytes: usize, success: bool) {
    let metrics = S3StorageMetrics::get();
    let status = if success { "success" } else { "error" };
    metrics
        .s3_requests
        .with_label_values(&["get", status])
        .inc();

    if success {
        metrics
            .s3_bytes_downloaded
            .with_label_values(&[namespace])
            .inc_by(bytes as u64);
    }
}

/// Record WAL operation
pub fn record_wal_operation(operation: &str, success: bool) -> Timer {
    let metrics = S3StorageMetrics::get();
    let status = if success { "success" } else { "error" };
    metrics
        .wal_operations
        .with_label_values(&[operation, status])
        .inc();

    Timer::new(&metrics.wal_latency, &[operation])
}

/// Record encryption operation
pub fn record_encryption(operation: &str, target: &str) {
    let metrics = S3StorageMetrics::get();
    metrics
        .encryption_operations
        .with_label_values(&[operation, target])
        .inc();
}

/// Record compression ratio
pub fn record_compression_ratio(namespace: &str, original_size: usize, compressed_size: usize) {
    if compressed_size > 0 {
        let ratio = original_size as f64 / compressed_size as f64;
        let metrics = S3StorageMetrics::get();
        metrics
            .compression_ratio
            .with_label_values(&[namespace])
            .observe(ratio);
    }
}
