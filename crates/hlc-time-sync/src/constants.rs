//! Constants for HLC atomic implementation.

/// AWS Time Sync maximum uncertainty in microseconds.
/// Based on AWS documentation for Time Sync Service.
pub const AWS_TIME_SYNC_MAX_UNCERTAINTY_US: f64 = 1000.0;

/// NTS (Network Time Security) processing overhead in milliseconds.
/// Accounts for local network RTT and cryptographic validation.
pub const NTS_PROCESSING_OVERHEAD_MS: f64 = 2.0;

/// Warning threshold for clock drift in PPM (parts per million).
/// Standard quartz oscillators drift up to 100 PPM.
pub const CLOCK_DRIFT_WARNING_PPM: f64 = 100.0;

/// Critical threshold for clock drift in PPM.
/// Beyond this, the clock is considered unhealthy (0.5ms/second drift).
pub const CLOCK_DRIFT_CRITICAL_PPM: f64 = 500.0;

/// Default NTS update interval in seconds.
pub const NTS_UPDATE_INTERVAL_SECS: u64 = 300; // 5 minutes

/// Maximum age for NTS time to be considered fresh (seconds).
pub const NTS_MAX_AGE_SECS: u64 = 600; // 10 minutes

/// P99 percentile for adaptive measurements.
pub const P99_PERCENTILE: f64 = 0.99;

/// Minimum samples required for statistical significance.
pub const MIN_SAMPLES_FOR_STATS: usize = 30;
