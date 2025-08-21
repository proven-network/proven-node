//! Secure HLC implementation with multi-source validation.
//!
//! Uses Chrony to track both AWS Time Sync (for precision) and Cloudflare NTS
//! (for cryptographic validation), detecting tampering by comparing their offsets.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod chrony;
mod error;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use proven_hlc::{HLCProvider, HlcConfig, HlcTimestamp, Result as HLCResult, TransactionTimestamp};
use proven_topology::NodeId;
use tracing::{debug, warn};

use chrony::ChronySourceManager;
use error::Result;

pub use error::Error;

/// Secure HLC implementation with multi-source validation.
///
/// Validates AWS Time Sync against Cloudflare NTS to detect tampering.
pub struct TimeSyncHlcProvider {
    /// Chrony source manager
    source_manager: Arc<ChronySourceManager>,
    /// Last seen physical time in microseconds
    last_physical: Arc<AtomicU64>,
    /// Logical counter for same physical time
    logical_counter: Arc<AtomicU32>,
    /// Node identifier
    node_id: NodeId,
    /// Background validation task handle
    validation_handle: Arc<tokio::sync::RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl TimeSyncHlcProvider {
    /// Create a new HLC provider with multi-source validation.
    ///
    /// # Errors
    ///
    /// Returns an error if Chrony is not available or sources cannot be configured.
    pub async fn new(config: HlcConfig) -> Result<Self> {
        let source_manager = Arc::new(ChronySourceManager::new(config.max_drift_ms));

        // Verify we can query sources
        tokio::task::spawn_blocking({
            let sm = Arc::clone(&source_manager);
            move || sm.get_all_sources()
        })
        .await
        .map_err(|e| Error::Chrony(format!("Failed to spawn blocking task: {e}")))?
        .map_err(|e| Error::Chrony(format!("Failed to query sources: {e}")))?;

        let node_id = config.node_id;
        let hlc = Self {
            source_manager,
            last_physical: Arc::new(AtomicU64::new(0)),
            logical_counter: Arc::new(AtomicU32::new(0)),
            node_id,
            validation_handle: Arc::new(tokio::sync::RwLock::new(None)),
        };

        // Start background validation task
        hlc.start_validation_task();

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
                    return HlcTimestamp::new(physical_now, 0, self.node_id);
                }
                // CAS failed, retry the loop
                continue;
            }

            // Physical time hasn't advanced or went backwards
            // Increment logical counter
            let logical = self.logical_counter.fetch_add(1, Ordering::SeqCst);

            // Ensure physical time doesn't go backwards
            let _ = self.last_physical.fetch_max(physical_now, Ordering::SeqCst);

            return HlcTimestamp::new(last_physical.max(physical_now), logical, self.node_id);
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
            } else {
                // Our clock is ahead, nothing to update
                return;
            }
        }
    }

    /// Start the background validation task.
    fn start_validation_task(&self) {
        let source_manager = Arc::clone(&self.source_manager);
        let handle_lock = Arc::clone(&self.validation_handle);

        let handle = tokio::spawn(async move {
            // Check every 30 seconds for better tamper detection
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                // Validate sources agree
                match source_manager.validate_sources() {
                    Ok((valid, divergence)) => {
                        if valid {
                            debug!("Time sources validated: divergence={:.3}ms", divergence);
                        } else {
                            warn!(
                                "Time source validation failed: divergence={:.3}ms",
                                divergence
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to validate time sources: {}", e);
                    }
                }
            }
        });

        // Store the handle
        tokio::spawn(async move {
            *handle_lock.write().await = Some(handle);
        });
    }

    /// Check if the HLC provider is healthy.
    #[must_use]
    pub async fn check_health(&self) -> bool {
        let source_manager = Arc::clone(&self.source_manager);
        tokio::task::spawn_blocking(move || match source_manager.validate_sources() {
            Ok((valid, _)) => valid,
            Err(_) => false,
        })
        .await
        .unwrap_or(false)
    }
}

#[async_trait]
impl HLCProvider for TimeSyncHlcProvider {
    async fn now(&self) -> HLCResult<TransactionTimestamp> {
        // Get validated time and uncertainty from Chrony
        let source_manager = Arc::clone(&self.source_manager);
        let (wall_time, uncertainty_us) =
            tokio::task::spawn_blocking(move || source_manager.get_validated_time())
                .await
                .map_err(|e| {
                    proven_hlc::Error::Internal(format!("Failed to spawn blocking task: {e}"))
                })?
                .map_err(|e| proven_hlc::Error::Internal(format!("Time validation failed: {e}")))?;

        // Get HLC timestamp
        let hlc = self.next_timestamp();

        Ok(TransactionTimestamp::new(hlc, wall_time, uncertainty_us))
    }

    fn update_from(&self, remote: &HlcTimestamp) -> HLCResult<()> {
        self.update_from_remote_internal(remote);
        Ok(())
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    async fn is_healthy(&self) -> HLCResult<bool> {
        Ok(self.check_health().await)
    }

    async fn uncertainty_us(&self) -> f64 {
        // Get current uncertainty from Chrony tracking
        // Note: We get tracking data directly, not validated time
        // This returns the actual uncertainty even if sources diverge
        let source_manager = Arc::clone(&self.source_manager);
        tokio::task::spawn_blocking(move || source_manager.get_tracking_uncertainty())
            .await
            .ok()
            .and_then(std::result::Result::ok)
            .unwrap_or(100_000.0) // Conservative 100ms fallback for safety
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_monotonicity() {
        let node_id = NodeId::from_seed(1);
        let config = HlcConfig::new(node_id);
        let hlc = TimeSyncHlcProvider::new(config).await.unwrap();

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

        let hlc1 = TimeSyncHlcProvider::new(HlcConfig::new(node1_id))
            .await
            .unwrap();
        let hlc2 = TimeSyncHlcProvider::new(HlcConfig::new(node2_id))
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
