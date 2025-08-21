//! Adaptive uncertainty tracking for HLC.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use tracing::debug;

use crate::constants::{
    AWS_TIME_SYNC_MAX_UNCERTAINTY_US, MIN_SAMPLES_FOR_STATS, NTS_PROCESSING_OVERHEAD_MS,
    P99_PERCENTILE,
};

/// Clock drift measurement.
#[derive(Debug, Clone)]
struct DriftMeasurement {
    /// Observed drift in microseconds
    drift_us: i64,
    /// Timestamp of measurement
    timestamp: DateTime<Utc>,
}

/// Tracks uncertainty measurements adaptively.
pub struct AdaptiveMeasurements {
    aws_measurements: Arc<RwLock<VecDeque<f64>>>,
    drift_measurements: Arc<RwLock<VecDeque<DriftMeasurement>>>,
    last_nts_time: Arc<RwLock<Option<DateTime<Utc>>>>,
    max_samples: usize,
    nts_overhead: Arc<RwLock<VecDeque<f64>>>,
    sample_count: Arc<RwLock<usize>>,
}

impl AdaptiveMeasurements {
    /// Create new adaptive measurements tracker.
    pub fn new() -> Self {
        Self {
            aws_measurements: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            drift_measurements: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            last_nts_time: Arc::new(RwLock::new(None)),
            max_samples: 100,
            nts_overhead: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            sample_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Update AWS Time Sync measurement.
    pub fn update_aws_measurement(&self, uncertainty_us: f64) {
        let mut measurements = self.aws_measurements.write();
        if measurements.len() >= self.max_samples {
            measurements.pop_front();
        }
        measurements.push_back(uncertainty_us);
    }

    /// Update NTS overhead measurement.
    pub fn update_nts_overhead(&self, overhead_ms: f64) {
        let mut measurements = self.nts_overhead.write();
        if measurements.len() >= self.max_samples {
            measurements.pop_front();
        }
        measurements.push_back(overhead_ms);
    }

    /// Update clock drift measurement.
    pub fn update_drift_measurement(&self, nts_time: DateTime<Utc>, local_time: DateTime<Utc>) {
        let drift_us = (local_time - nts_time).num_microseconds().unwrap_or(0);

        {
            let mut measurements = self.drift_measurements.write();
            if measurements.len() >= self.max_samples {
                measurements.pop_front();
            }
            measurements.push_back(DriftMeasurement {
                timestamp: Utc::now(),
                drift_us,
            });
        } // Drop lock early

        *self.last_nts_time.write() = Some(nts_time);
        *self.sample_count.write() += 1;
    }

    /// Get clock drift in PPM.
    pub fn get_clock_drift_ppm(&self) -> f64 {
        let measurements = self.drift_measurements.read();

        if measurements.len() < MIN_SAMPLES_FOR_STATS {
            return 0.0; // Not enough data
        }

        // Extract values while holding lock
        let first = &measurements[0];
        let last = &measurements[measurements.len() - 1];
        let first_drift = first.drift_us;
        let last_drift = last.drift_us;
        let first_ts = first.timestamp;
        let last_ts = last.timestamp;

        // Explicitly drop the lock before calculations
        drop(measurements);

        #[allow(clippy::cast_precision_loss)]
        let time_span = (last_ts - first_ts).num_seconds() as f64;
        if time_span <= 0.0 {
            return 0.0;
        }

        #[allow(clippy::cast_precision_loss)]
        let drift_change = (last_drift - first_drift) as f64;
        let drift_ppm = (drift_change / time_span) / 1.0; // microseconds per second = PPM

        drift_ppm.abs()
    }

    /// Get current uncertainty estimate in microseconds.
    pub fn get_uncertainty_us(&self) -> f64 {
        // Copy measurements to avoid holding lock across await
        let aws_measurements: Vec<f64> = {
            let measurements = self.aws_measurements.read();
            measurements.iter().copied().collect()
        };

        let drift_ppm = self.get_clock_drift_ppm();

        if aws_measurements.len() < MIN_SAMPLES_FOR_STATS {
            // Not enough samples, use conservative default
            return AWS_TIME_SYNC_MAX_UNCERTAINTY_US;
        }

        // Calculate P99 of measurements
        let mut sorted = aws_measurements;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Calculate P99 index with proper rounding
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let p99_index = ((sorted.len() as f64) * P99_PERCENTILE).round() as usize;
        let p99_value = sorted
            .get(p99_index)
            .copied()
            .unwrap_or(AWS_TIME_SYNC_MAX_UNCERTAINTY_US);

        // Add drift-based uncertainty (1 second worth of drift)
        let drift_uncertainty = drift_ppm * 1.0; // PPM for 1 second

        // Add NTS processing overhead (convert from ms to μs)
        let nts_overhead_us = NTS_PROCESSING_OVERHEAD_MS * 1000.0;

        let total_uncertainty = p99_value + drift_uncertainty + nts_overhead_us;

        debug!(
            "Adaptive uncertainty: {:.1}μs (P99={:.1}μs + drift={:.1}μs + NTS={:.1}μs)",
            total_uncertainty, p99_value, drift_uncertainty, nts_overhead_us
        );

        total_uncertainty
    }

    /// Get statistics for monitoring.
    pub fn get_stats(&self) -> AdaptiveStats {
        let aws_measurements = self.aws_measurements.read();
        let nts_overhead = self.nts_overhead.read();

        AdaptiveStats {
            aws_p99: Self::calculate_p99(&aws_measurements),
            aws_sample_count: aws_measurements.len(),
            nts_p99: Self::calculate_p99(&nts_overhead),
            nts_sample_count: nts_overhead.len(),
        }
    }

    fn calculate_p99(samples: &VecDeque<f64>) -> Option<f64> {
        if samples.len() < MIN_SAMPLES_FOR_STATS {
            return None;
        }

        let mut sorted: Vec<f64> = samples.iter().copied().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        // Calculate P99 index with proper rounding
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let p99_index = ((sorted.len() as f64) * P99_PERCENTILE).round() as usize;
        sorted.get(p99_index).copied()
    }

    /// Log current adaptive status.
    pub fn log_status(&self) {
        let aws_count = self.aws_measurements.read().len();
        let nts_count = self.nts_overhead.read().len();
        let drift_count = self.drift_measurements.read().len();
        let sample_count = *self.sample_count.read();
        let drift_ppm = self.get_clock_drift_ppm();
        let uncertainty = self.get_uncertainty_us();

        tracing::info!(
            "Adaptive measurements after {} samples:\n\
             AWS samples: {}, NTS samples: {}, Drift samples: {}\n\
             Clock drift: {:.2} PPM\n\
             Total uncertainty: {:.1}μs",
            sample_count,
            aws_count,
            nts_count,
            drift_count,
            drift_ppm,
            uncertainty
        );
    }
}

/// Statistics from adaptive measurements.
#[derive(Debug, Clone)]
#[allow(dead_code)] // These fields may be useful for future monitoring
pub struct AdaptiveStats {
    /// P99 AWS uncertainty
    pub aws_p99: Option<f64>,
    /// Number of AWS measurements
    pub aws_sample_count: usize,
    /// P99 NTS overhead
    pub nts_p99: Option<f64>,
    /// Number of NTS overhead measurements
    pub nts_sample_count: usize,
}

impl Default for AdaptiveMeasurements {
    fn default() -> Self {
        Self::new()
    }
}
