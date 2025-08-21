//! NTS (Network Time Security) validation for secure time.

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::{debug, error, warn};

#[cfg(target_os = "linux")]
use chrony_candm::reply::ReplyBody;
#[cfg(target_os = "linux")]
use chrony_candm::request::RequestBody;
#[cfg(target_os = "linux")]
use chrony_candm::{ClientOptions, UnixDatagramClient};

use crate::adaptive::AdaptiveMeasurements;
use crate::constants::{NTS_MAX_AGE_SECS, NTS_UPDATE_INTERVAL_SECS};
use crate::error::{Error, Result};

use crate::messages::TimeResponse;

/// NTS validator using chrony.
pub struct NtsValidator {
    adaptive: Arc<AdaptiveMeasurements>,
    last_nts_time: Arc<RwLock<Option<DateTime<Utc>>>>,
    max_drift_ms: f64,
    update_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl NtsValidator {
    /// Create with adaptive measurements.
    pub fn with_adaptive(adaptive: Arc<AdaptiveMeasurements>, max_drift_ms: f64) -> Self {
        Self {
            adaptive,
            last_nts_time: Arc::new(RwLock::new(None)),
            max_drift_ms,
            update_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Start background NTS validation.
    pub async fn start_validation(&self) -> Result<()> {
        // Get initial NTS time
        self.update_nts_time().await?;

        // Start background update task
        let last_nts_time = Arc::clone(&self.last_nts_time);
        let _adaptive = Arc::clone(&self.adaptive);

        let handle = tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(NTS_UPDATE_INTERVAL_SECS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                match get_nts_time_from_chrony().await {
                    Ok(nts_time) => {
                        *last_nts_time.write() = Some(nts_time);
                        debug!("NTS time updated: {}", nts_time);
                    }
                    Err(e) => {
                        error!("Failed to update NTS time: {}", e);
                    }
                }
            }
        });

        *self.update_handle.write() = Some(handle);
        Ok(())
    }

    /// Update NTS time immediately.
    async fn update_nts_time(&self) -> Result<()> {
        let nts_time = get_nts_time_from_chrony().await?;
        *self.last_nts_time.write() = Some(nts_time);
        Ok(())
    }

    /// Validate host time against NTS.
    pub fn validate(&self, host_time: &TimeResponse) -> Result<bool> {
        let nts_time = self.last_nts_time.read().ok_or(Error::NtsNotSynchronized)?;

        let age = Utc::now() - nts_time;
        #[allow(clippy::cast_possible_wrap)]
        if age.num_seconds() > NTS_MAX_AGE_SECS as i64 {
            warn!("NTS time too old: {}s", age.num_seconds());
            return Ok(false);
        }

        #[allow(clippy::cast_precision_loss)]
        let drift_ms = (host_time.timestamp - nts_time).num_milliseconds() as f64;

        if drift_ms.abs() > self.max_drift_ms {
            return Err(Error::NtsValidationFailed(
                drift_ms.abs(),
                self.max_drift_ms,
            ));
        }

        // Update adaptive measurements
        self.adaptive.update_nts_overhead(drift_ms.abs());

        Ok(true)
    }

    /// Get validated time from NTS.
    pub async fn get_validated_time(&self) -> Result<DateTime<Utc>> {
        let nts_time = self.last_nts_time.read().ok_or(Error::NtsNotSynchronized)?;

        let age = Utc::now() - nts_time;
        #[allow(clippy::cast_possible_wrap)]
        if age.num_seconds() > NTS_MAX_AGE_SECS as i64 {
            // Try to update (DateTime is Copy, no need to drop)
            let _ = nts_time;
            self.update_nts_time().await?;
            return self.last_nts_time.read().ok_or(Error::NtsNotSynchronized);
        }

        Ok(nts_time)
    }

    /// Force an immediate NTS update.
    pub async fn force_update(&self) -> Result<()> {
        let new_time = get_nts_time_from_chrony().await?;
        *self.last_nts_time.write() = Some(new_time);
        debug!("Forced NTS update: {:?}", new_time);
        Ok(())
    }

    /// Check if validator is healthy.
    pub fn is_healthy(&self) -> Result<bool> {
        let nts_time = self.last_nts_time.read();

        nts_time.map_or(Ok(false), |time| {
            let age = Utc::now() - time;
            #[allow(clippy::cast_possible_wrap)]
            Ok(age.num_seconds() < NTS_MAX_AGE_SECS as i64)
        })
    }
}

/// Get NTS time from chrony (Linux only).
#[cfg(target_os = "linux")]
async fn get_nts_time_from_chrony() -> Result<DateTime<Utc>> {
    let mut client = UnixDatagramClient::new()
        .await
        .map_err(|e| Error::Chrony(format!("Failed to create client: {e}")))?;

    let reply = client
        .query(RequestBody::Tracking, ClientOptions::default())
        .await
        .map_err(|e| Error::Chrony(format!("Query failed: {e}")))?;

    let ReplyBody::Tracking(tracking) = reply.body else {
        return Err(Error::Chrony("Unexpected reply from chrony".to_string()));
    };

    // Check if synchronized
    if tracking.leap_status == 3 {
        return Err(Error::NtsNotSynchronized);
    }

    // Calculate current time with chrony's offset
    let system_time = Utc::now();
    let offset_us = (f64::from(tracking.current_correction) * 1_000_000.0) as i64;
    let corrected_time = system_time + chrono::Duration::microseconds(offset_us);

    debug!(
        "Chrony tracking: offset={:.3}ms, drift={:.3}PPM, stratum={}",
        f64::from(tracking.current_correction) * 1000.0,
        f64::from(tracking.freq_ppm),
        tracking.stratum
    );

    Ok(corrected_time)
}

/// Mock implementation for non-Linux platforms.
#[cfg(not(target_os = "linux"))]
#[allow(clippy::unused_async)]
async fn get_nts_time_from_chrony() -> Result<DateTime<Utc>> {
    warn!("NTS validation not available on non-Linux platforms; using system time as fallback");
    Ok(Utc::now())
}
