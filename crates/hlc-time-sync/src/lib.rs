//! Secure HLC implementation with NTS validation.
//!
//! Provides monotonic, unique timestamps for distributed transactions with
//! cryptographic time validation via NTS (Network Time Security).

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod adaptive;
mod client;
mod constants;
mod error;
mod messages;
mod nts;
mod server;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use proven_hlc::{HLCProvider, HlcConfig, HlcTimestamp, Result as HLCResult, TransactionTimestamp};
use proven_topology::NodeId;
use tracing::{debug, warn};

use adaptive::AdaptiveMeasurements;
use constants::{CLOCK_DRIFT_CRITICAL_PPM, CLOCK_DRIFT_WARNING_PPM};
use error::Result;
use nts::NtsValidator;

pub use client::TimeClient;
pub use messages::{TimeRequest, TimeResponse};
pub use server::TimeServer;

/// Secure HLC implementation with NTS validation.
///
/// Provides globally unique, monotonic timestamps with cryptographic
/// time validation for distributed transactions.
pub struct TimeSyncHlcProvider {
    /// Adaptive measurements for uncertainty
    adaptive: Arc<AdaptiveMeasurements>,
    /// Last seen physical time in microseconds
    last_physical: Arc<AtomicU64>,
    /// Logical counter for same physical time
    logical_counter: Arc<AtomicU32>,
    /// Node identifier
    node_id: NodeId,
    /// NTS validator for secure time
    nts_validator: Arc<NtsValidator>,
    /// Background recalibration task handle
    recalibration_handle: Arc<tokio::sync::RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// Time client for enclave mode
    time_client: Option<Arc<client::TimeClient>>,
}

impl TimeSyncHlcProvider {
    /// Create for Nitro Enclave with vsock communication on Linux or TCP on other platforms.
    ///
    /// # Errors
    ///
    /// Returns an error if connection fails.
    pub fn new_enclave(
        config: &HlcConfig,
        #[cfg(target_os = "linux")] vsock_addr: tokio_vsock::VsockAddr,
        #[cfg(not(target_os = "linux"))] tcp_addr: std::net::SocketAddr,
    ) -> Result<Self> {
        let adaptive = Arc::new(AdaptiveMeasurements::new());
        let nts_validator = Arc::new(NtsValidator::with_adaptive(
            Arc::clone(&adaptive),
            config.max_drift_ms,
        ));

        let time_client = Arc::new(client::TimeClient::new(
            #[cfg(target_os = "linux")]
            vsock_addr,
            #[cfg(not(target_os = "linux"))]
            tcp_addr,
        )?);

        let node_id = config.node_id.clone();
        let hlc = Self {
            adaptive,
            last_physical: Arc::new(AtomicU64::new(0)),
            logical_counter: Arc::new(AtomicU32::new(0)),
            node_id,
            nts_validator,
            recalibration_handle: Arc::new(tokio::sync::RwLock::new(None)),
            time_client: Some(time_client),
        };

        // Start background recalibration task
        hlc.start_recalibration_task();

        Ok(hlc)
    }

    /// Create for host with NTS only.
    ///
    /// # Errors
    ///
    /// Returns an error if NTS validation fails to start.
    pub async fn new_host(config: HlcConfig) -> Result<Self> {
        let adaptive = Arc::new(AdaptiveMeasurements::new());
        let nts_validator = Arc::new(NtsValidator::with_adaptive(
            Arc::clone(&adaptive),
            config.max_drift_ms,
        ));

        // Start NTS validation
        nts_validator.start_validation().await?;

        let node_id = config.node_id;
        let hlc = Self {
            adaptive,
            last_physical: Arc::new(AtomicU64::new(0)),
            logical_counter: Arc::new(AtomicU32::new(0)),
            node_id,
            nts_validator,
            recalibration_handle: Arc::new(tokio::sync::RwLock::new(None)),
            time_client: None,
        };

        // Start background recalibration task
        hlc.start_recalibration_task();

        Ok(hlc)
    }

    /// Get the next HLC timestamp with monotonicity guarantee.
    fn next_timestamp(&self) -> HlcTimestamp {
        // Get current time, ensuring non-negative microseconds
        #[allow(clippy::cast_sign_loss)]
        let physical_now = Utc::now().timestamp_micros().max(0) as u64;

        loop {
            let last_physical = self.last_physical.load(Ordering::SeqCst);

            if physical_now > last_physical {
                // Physical time has advanced
                if self
                    .last_physical
                    .compare_exchange(
                        last_physical,
                        physical_now,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    // Reset logical counter
                    self.logical_counter.store(0, Ordering::SeqCst);
                    return HlcTimestamp::new(physical_now, 0, self.node_id.clone());
                }
                // CAS failed, retry the loop
                continue;
            }

            // Physical time hasn't advanced or went backwards
            // Increment logical counter
            let logical = self.logical_counter.fetch_add(1, Ordering::SeqCst);

            // Ensure physical time doesn't go backwards
            let _ = self.last_physical.fetch_max(physical_now, Ordering::SeqCst);

            return HlcTimestamp::new(
                last_physical.max(physical_now),
                logical,
                self.node_id.clone(),
            );
        }
    }

    /// Update from remote timestamp for causality.
    fn update_from_remote_internal(&self, remote: &HlcTimestamp) {
        loop {
            let local_physical = self.last_physical.load(Ordering::SeqCst);
            let local_logical = self.logical_counter.load(Ordering::SeqCst);

            if remote.physical > local_physical {
                // Remote is ahead in physical time
                if self
                    .last_physical
                    .compare_exchange(
                        local_physical,
                        remote.physical,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
                {
                    // Set logical to remote + 1
                    self.logical_counter
                        .store(remote.logical + 1, Ordering::SeqCst);
                    return;
                }

                // CAS failed, retry the loop
            } else if remote.physical == local_physical && remote.logical >= local_logical {
                // Same physical time but remote has higher logical
                let new_logical = remote.logical + 1;
                let _ = self
                    .logical_counter
                    .fetch_max(new_logical, Ordering::SeqCst);
                return;
            }
        }
    }

    /// Start the background recalibration task.
    fn start_recalibration_task(&self) {
        let adaptive = Arc::clone(&self.adaptive);
        let nts_validator = Arc::clone(&self.nts_validator);
        let handle_lock = Arc::clone(&self.recalibration_handle);

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5 minutes
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Log current adaptive measurements status
                adaptive.log_status();

                // Update drift measurements if we have NTS time
                if let Ok(nts_time) = nts_validator.get_validated_time().await {
                    let local_time = Utc::now();
                    adaptive.update_drift_measurement(nts_time, local_time);
                }

                // Check drift and log warnings if excessive
                let drift_ppm = adaptive.get_clock_drift_ppm();
                if drift_ppm > CLOCK_DRIFT_WARNING_PPM {
                    warn!("Excessive clock drift detected: {:.1} PPM", drift_ppm);
                }

                debug!("Recalibration cycle complete");
            }
        });

        // Store the handle
        tokio::spawn(async move {
            *handle_lock.write().await = Some(handle);
        });
    }

    /// Force an NTS update.
    ///
    /// # Errors
    ///
    /// Returns an error if NTS update fails.
    pub async fn force_nts_update(&self) -> Result<()> {
        self.nts_validator.force_update().await
    }

    /// Check if the HLC provider is healthy.
    #[must_use]
    pub fn check_health(&self) -> bool {
        // Check NTS validator health
        if !self.nts_validator.is_healthy().unwrap_or(false) {
            return false;
        }

        // Check if we have recent measurements
        let stats = self.adaptive.get_stats();
        if stats.aws_sample_count < 5 && self.time_client.is_some() {
            return false; // Not enough samples for enclave mode
        }

        // Check excessive drift
        let drift_ppm = self.adaptive.get_clock_drift_ppm();
        if drift_ppm > CLOCK_DRIFT_CRITICAL_PPM {
            return false; // Excessive drift
        }

        true
    }
}

#[async_trait]
impl HLCProvider for TimeSyncHlcProvider {
    async fn now(&self) -> HLCResult<TransactionTimestamp> {
        // Get validated time for uncertainty bounds
        let wall_time = if let Some(ref client) = self.time_client {
            // Enclave mode: try host time with NTS validation
            match client.get_time().await {
                Ok(host_time) => {
                    // Validate against NTS
                    if self.nts_validator.validate(&host_time)? {
                        self.adaptive
                            .update_aws_measurement(host_time.uncertainty_us);
                        host_time.timestamp
                    } else {
                        warn!("Host time validation failed, using NTS");
                        self.nts_validator.get_validated_time().await?
                    }
                }
                Err(e) => {
                    debug!("Host time unavailable: {}", e);
                    self.nts_validator.get_validated_time().await?
                }
            }
        } else {
            // Host mode: use NTS directly
            self.nts_validator.get_validated_time().await?
        };

        // Get HLC timestamp
        let hlc = self.next_timestamp();

        // Get current uncertainty
        let uncertainty_us = self.adaptive.get_uncertainty_us();

        Ok(TransactionTimestamp::new(hlc, wall_time, uncertainty_us))
    }

    fn update_from(&self, remote: &HlcTimestamp) -> HLCResult<()> {
        self.update_from_remote_internal(remote);
        Ok(())
    }

    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    async fn is_healthy(&self) -> HLCResult<bool> {
        Ok(self.nts_validator.is_healthy()?)
    }

    async fn uncertainty_us(&self) -> f64 {
        self.adaptive.get_uncertainty_us()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_monotonicity() {
        let node_id = NodeId::from_seed(1);
        let config = HlcConfig::new(node_id);
        let hlc = TimeSyncHlcProvider::new_host(config).await.unwrap();

        let ts1 = hlc.now().await.unwrap();
        let ts2 = hlc.now().await.unwrap();
        let ts3 = hlc.now().await.unwrap();

        // Timestamps should be strictly increasing
        assert!(ts1.hlc <= ts2.hlc, "ts1 should be <= ts2");
        assert!(ts2.hlc <= ts3.hlc, "ts2 should be <= ts3");

        // At least one should be strictly less (not all equal)
        assert!(ts1.hlc < ts3.hlc, "ts1 should be < ts3");
    }

    #[tokio::test]
    async fn test_causality() {
        let node1_id = NodeId::from_seed(1);
        let node2_id = NodeId::from_seed(2);

        let hlc1 = TimeSyncHlcProvider::new_host(HlcConfig::new(node1_id))
            .await
            .unwrap();
        let hlc2 = TimeSyncHlcProvider::new_host(HlcConfig::new(node2_id))
            .await
            .unwrap();

        // Node 1 creates timestamp
        let ts1 = hlc1.now().await.unwrap();

        // Node 2 updates from node 1
        hlc2.update_from(&ts1.hlc).unwrap();

        // Node 2's next timestamp should be after node 1's
        let ts2 = hlc2.now().await.unwrap();
        assert!(ts2.hlc > ts1.hlc);
    }
}
