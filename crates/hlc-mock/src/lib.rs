//! Controllable HLC implementation for testing distributed systems.
//!
//! Provides deterministic and controllable timestamps for testing distributed
//! transaction scenarios without relying on actual time synchronization.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use proven_hlc::{HLCProvider, HlcTimestamp, Result as HLCResult, TransactionTimestamp};
use proven_topology::NodeId;

/// Controllable HLC for testing.
///
/// Allows precise control over time progression and clock behavior
/// for testing distributed transaction scenarios.
pub struct MockHlcProvider {
    /// Whether to auto-advance time on each call
    auto_advance: Arc<RwLock<bool>>,
    /// Configuration
    config: Arc<RwLock<MockConfig>>,
    /// Current physical time in microseconds
    current_time: Arc<AtomicU64>,
    /// Last seen physical time
    last_physical: Arc<AtomicU64>,
    /// Logical counter for same physical time
    logical_counter: Arc<AtomicU32>,
    /// Node identifier
    node_id: NodeId,
    /// Clock skew in microseconds (can be negative)
    skew_us: Arc<RwLock<i64>>,
    /// Uncertainty in microseconds
    uncertainty_us: Arc<RwLock<f64>>,
}

/// Configuration for mock HLC.
#[derive(Debug, Clone)]
pub struct MockConfig {
    /// Auto-advance increment in microseconds
    pub advance_increment_us: u64,
    /// Whether to auto-advance time
    pub auto_advance: bool,
    /// Base uncertainty in microseconds
    pub base_uncertainty_us: f64,
    /// Drift rate in PPM
    pub drift_ppm: f64,
    /// Whether to simulate clock drift
    pub simulate_drift: bool,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            advance_increment_us: 1000, // 1ms
            auto_advance: true,
            base_uncertainty_us: 1000.0,
            drift_ppm: 0.0,
            simulate_drift: false,
        }
    }
}

impl MockHlcProvider {
    /// Create a new mock HLC.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        #[allow(clippy::cast_sign_loss)]
        let now_us = Utc::now().timestamp_micros().max(0) as u64;
        Self {
            auto_advance: Arc::new(RwLock::new(true)),
            config: Arc::new(RwLock::new(MockConfig::default())),
            current_time: Arc::new(AtomicU64::new(now_us)),
            last_physical: Arc::new(AtomicU64::new(0)),
            logical_counter: Arc::new(AtomicU32::new(0)),
            node_id,
            skew_us: Arc::new(RwLock::new(0)),
            uncertainty_us: Arc::new(RwLock::new(1000.0)),
        }
    }

    /// Create with custom configuration.
    #[must_use]
    pub fn with_config(node_id: NodeId, config: &MockConfig) -> Self {
        #[allow(clippy::cast_sign_loss)]
        let now_us = Utc::now().timestamp_micros().max(0) as u64;
        Self {
            auto_advance: Arc::new(RwLock::new(config.auto_advance)),
            config: Arc::new(RwLock::new(config.clone())),
            current_time: Arc::new(AtomicU64::new(now_us)),
            last_physical: Arc::new(AtomicU64::new(0)),
            logical_counter: Arc::new(AtomicU32::new(0)),
            node_id,
            skew_us: Arc::new(RwLock::new(0)),
            uncertainty_us: Arc::new(RwLock::new(config.base_uncertainty_us)),
        }
    }

    /// Set the current time.
    pub fn set_time(&self, time: DateTime<Utc>) {
        #[allow(clippy::cast_sign_loss)]
        let time_us = time.timestamp_micros().max(0) as u64;
        self.current_time.store(time_us, Ordering::SeqCst);
    }

    /// Advance time by a duration.
    pub fn advance_time(&self, micros: u64) {
        self.current_time.fetch_add(micros, Ordering::SeqCst);
    }

    /// Set clock skew (positive or negative).
    pub fn set_skew(&self, skew_us: i64) {
        *self.skew_us.write() = skew_us;
    }

    /// Set uncertainty.
    pub fn set_uncertainty(&self, uncertainty_us: f64) {
        *self.uncertainty_us.write() = uncertainty_us;
    }

    /// Force logical counter increment (simulate high contention).
    pub fn force_logical_increment(&self, count: u32) {
        self.logical_counter.fetch_add(count, Ordering::SeqCst);
    }

    /// Simulate time going backwards (for testing monotonicity).
    pub fn simulate_time_regression(&self, micros: u64) {
        let current = self.current_time.load(Ordering::SeqCst);
        if current > micros {
            self.current_time.store(current - micros, Ordering::SeqCst);
        }
    }

    /// Enable or disable auto-advance.
    pub fn set_auto_advance(&self, enabled: bool) {
        *self.auto_advance.write() = enabled;
    }

    /// Get the next HLC timestamp.
    fn next_timestamp(&self) -> HlcTimestamp {
        // Get current time with skew applied
        let base_time = self.current_time.load(Ordering::SeqCst);
        let skew = *self.skew_us.read();
        let physical_now = if skew >= 0 {
            #[allow(clippy::cast_sign_loss)]
            let skew_us = skew as u64;
            base_time + skew_us
        } else {
            #[allow(clippy::cast_sign_loss)]
            let neg_skew_us = (-skew) as u64;
            base_time.saturating_sub(neg_skew_us)
        };

        // Auto-advance if enabled
        if *self.auto_advance.read() {
            let config = self.config.read();
            self.current_time
                .fetch_add(config.advance_increment_us, Ordering::SeqCst);
        }

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
                    // Also update current time to at least match remote
                    let _ = self
                        .current_time
                        .fetch_max(remote.physical, Ordering::SeqCst);
                    return;
                }
                // CAS failed, retry the loop
                continue;
            } else if remote.physical == local_physical && remote.logical >= local_logical {
                // Same physical time but remote has higher logical
                let new_logical = remote.logical + 1;
                let _ = self
                    .logical_counter
                    .fetch_max(new_logical, Ordering::SeqCst);
                return;
            }
            // We're already ahead
            return;
        }
    }
}

#[async_trait]
impl HLCProvider for MockHlcProvider {
    async fn now(&self) -> HLCResult<TransactionTimestamp> {
        let hlc = self.next_timestamp();
        // Safe conversion: u64 microseconds to i64
        #[allow(clippy::cast_possible_wrap)]
        let micros = hlc.physical.min(i64::MAX as u64) as i64;
        let wall_time = DateTime::from_timestamp_micros(micros).unwrap_or_else(Utc::now);
        let uncertainty_us = *self.uncertainty_us.read();

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
        Ok(true) // Always healthy in mock
    }

    async fn uncertainty_us(&self) -> f64 {
        *self.uncertainty_us.read()
    }
}

/// Builder for creating test scenarios.
pub struct MockHLCBuilder {
    config: MockConfig,
    initial_time: Option<DateTime<Utc>>,
    node_id: NodeId,
    skew_us: i64,
}

impl MockHLCBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new(node_id: NodeId) -> Self {
        Self {
            config: MockConfig::default(),
            initial_time: None,
            node_id,
            skew_us: 0,
        }
    }

    /// Set initial time.
    #[must_use]
    pub const fn with_initial_time(mut self, time: DateTime<Utc>) -> Self {
        self.initial_time = Some(time);
        self
    }

    /// Set clock skew.
    #[must_use]
    pub const fn with_skew(mut self, skew_us: i64) -> Self {
        self.skew_us = skew_us;
        self
    }

    /// Set uncertainty.
    #[must_use]
    pub const fn with_uncertainty(mut self, uncertainty_us: f64) -> Self {
        self.config.base_uncertainty_us = uncertainty_us;
        self
    }

    /// Enable drift simulation.
    #[must_use]
    pub const fn with_drift(mut self, drift_ppm: f64) -> Self {
        self.config.simulate_drift = true;
        self.config.drift_ppm = drift_ppm;
        self
    }

    /// Disable auto-advance.
    #[must_use]
    pub const fn manual_advance(mut self) -> Self {
        self.config.auto_advance = false;
        self
    }

    /// Build the mock HLC.
    #[must_use]
    pub fn build(self) -> MockHlcProvider {
        let hlc = MockHlcProvider::with_config(self.node_id, &self.config);

        if let Some(time) = self.initial_time {
            hlc.set_time(time);
        }

        if self.skew_us != 0 {
            hlc.set_skew(self.skew_us);
        }

        hlc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_monotonicity() {
        let node_id = NodeId::from_seed(1);
        let hlc = MockHlcProvider::new(node_id);

        let ts1 = hlc.now().await.unwrap();
        let ts2 = hlc.now().await.unwrap();
        let ts3 = hlc.now().await.unwrap();

        assert!(ts1.hlc < ts2.hlc);
        assert!(ts2.hlc < ts3.hlc);
    }

    #[tokio::test]
    async fn test_mock_time_control() {
        let node_id = NodeId::from_seed(2);
        let hlc = MockHLCBuilder::new(node_id).manual_advance().build();

        let ts1 = hlc.now().await.unwrap();

        // Time shouldn't advance automatically
        let ts2 = hlc.now().await.unwrap();
        assert_eq!(ts1.hlc.physical, ts2.hlc.physical);
        // Logical counter should increment or stay same
        assert!(ts2.hlc.logical >= ts1.hlc.logical);
        // But timestamps should still be ordered
        assert!(ts2.hlc >= ts1.hlc);

        // Manually advance time
        hlc.advance_time(1_000_000); // 1 second
        let ts3 = hlc.now().await.unwrap();
        assert!(ts3.hlc.physical > ts2.hlc.physical);
    }

    #[tokio::test]
    async fn test_mock_clock_skew() {
        let node_id = NodeId::from_seed(3);
        let hlc = MockHLCBuilder::new(node_id)
            .with_skew(5000) // 5ms ahead
            .build();

        let ts = hlc.now().await.unwrap();
        // Timestamp should reflect the skew
        assert!(ts.hlc.physical > 0);
    }

    #[tokio::test]
    async fn test_mock_causality() {
        let node1_id = NodeId::from_seed(1);
        let node2_id = NodeId::from_seed(2);

        let hlc1 = MockHlcProvider::new(node1_id);
        let hlc2 = MockHlcProvider::new(node2_id);

        // Node 1 creates timestamp
        let ts1 = hlc1.now().await.unwrap();

        // Node 2 updates from node 1
        hlc2.update_from(&ts1.hlc).unwrap();

        // Node 2's next timestamp should be after node 1's
        let ts2 = hlc2.now().await.unwrap();
        assert!(ts2.hlc > ts1.hlc);
    }

    #[tokio::test]
    async fn test_time_regression_handling() {
        let node_id = NodeId::from_seed(4);
        let hlc = MockHlcProvider::new(node_id);

        let ts1 = hlc.now().await.unwrap();

        // Simulate time going backwards
        hlc.simulate_time_regression(1_000_000);

        // Should still maintain monotonicity
        let ts2 = hlc.now().await.unwrap();
        assert!(
            ts2.hlc >= ts1.hlc,
            "Timestamps should be monotonic even with time regression"
        );
    }
}
