//! Hybrid Logical Clock traits and types for distributed transactions.
//!
//! This crate provides the core abstractions for monotonic, unique timestamps
//! that preserve causality across distributed nodes. Designed specifically for
//! distributed transaction ordering in the Proven platform.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

pub use error::{Error, Result};

/// Immutable HLC timestamp with total ordering.
///
/// Provides globally unique, monotonic timestamps for distributed transactions.
/// The total ordering is: physical time, then logical counter, then node ID.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HlcTimestamp {
    /// Logical counter for uniqueness within same physical time
    pub logical: u32,
    /// Node that generated this timestamp
    pub node_id: NodeId,
    /// Physical time component (microseconds since Unix epoch)
    pub physical: u64,
}

impl HlcTimestamp {
    /// Create a new HLC timestamp.
    #[must_use]
    pub const fn new(physical: u64, logical: u32, node_id: NodeId) -> Self {
        Self {
            logical,
            node_id,
            physical,
        }
    }

    /// Convert physical time to `DateTime`.
    #[must_use]
    pub fn to_datetime(&self) -> DateTime<Utc> {
        // Safe conversion: u64 microseconds to i64
        // Will work until year 292277 (i64::MAX microseconds from epoch)
        #[allow(clippy::cast_possible_wrap)]
        let micros = self.physical.min(i64::MAX as u64) as i64;
        DateTime::from_timestamp_micros(micros).unwrap_or_else(Utc::now)
    }

    /// Check if two timestamps might be concurrent.
    ///
    /// This is a fast check based on timestamp values alone.
    /// For accurate concurrency detection with uncertainty, use `TransactionTimestamp`.
    #[must_use]
    pub fn might_be_concurrent(&self, other: &Self) -> bool {
        self.physical == other.physical && self.node_id != other.node_id
    }
}

impl PartialOrd for HlcTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HlcTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Total ordering: physical time, logical counter, then node ID
        match self.physical.cmp(&other.physical) {
            std::cmp::Ordering::Equal => match self.logical.cmp(&other.logical) {
                std::cmp::Ordering::Equal => self.node_id.cmp(&other.node_id),
                other => other,
            },
            other => other,
        }
    }
}

impl fmt::Display for HlcTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{:06}.{}",
            self.to_datetime().format("%Y-%m-%d %H:%M:%S%.6f"),
            self.logical,
            self.node_id
        )
    }
}

/// Transaction timestamp with uncertainty bounds.
///
/// Combines HLC for ordering with uncertainty for conflict detection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionTimestamp {
    /// HLC timestamp for total ordering
    pub hlc: HlcTimestamp,
    /// Uncertainty in microseconds for conflict resolution
    pub uncertainty_us: f64,
    /// Wall clock time for human readability
    pub wall_time: DateTime<Utc>,
}

impl TransactionTimestamp {
    /// Create a new transaction timestamp.
    #[must_use]
    pub const fn new(hlc: HlcTimestamp, wall_time: DateTime<Utc>, uncertainty_us: f64) -> Self {
        Self {
            hlc,
            uncertainty_us,
            wall_time,
        }
    }

    /// Check if this timestamp definitely happened before another.
    ///
    /// Returns true only if this timestamp plus its uncertainty is less than
    /// the other timestamp minus its uncertainty.
    #[must_use]
    pub fn definitely_before(&self, other: &Self) -> bool {
        // Safely handle uncertainty conversion (f64 to u64)
        // u64::MAX as f64 loses precision but is still massive (1.8e19)
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let self_uncertainty = self.uncertainty_us.max(0.0).min(u64::MAX as f64) as u64;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss
        )]
        let other_uncertainty = other.uncertainty_us.max(0.0).min(u64::MAX as f64) as u64;

        let self_upper = self.hlc.physical.saturating_add(self_uncertainty);
        let other_lower = other.hlc.physical.saturating_sub(other_uncertainty);

        self_upper < other_lower || (self_upper == other_lower && self.hlc < other.hlc)
    }

    /// Check if two timestamps might be concurrent.
    ///
    /// Returns true if the uncertainty windows overlap.
    #[must_use]
    pub fn might_be_concurrent(&self, other: &Self) -> bool {
        !self.definitely_before(other) && !other.definitely_before(self)
    }
}

impl PartialEq for TransactionTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.hlc == other.hlc
    }
}

impl Eq for TransactionTimestamp {}

impl PartialOrd for TransactionTimestamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransactionTimestamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hlc.cmp(&other.hlc)
    }
}

/// Core trait for HLC providers.
///
/// Implementations provide monotonic, unique timestamps for distributed transactions.
#[async_trait]
pub trait HLCProvider: Send + Sync {
    /// Get a unique, monotonic timestamp for transactions.
    ///
    /// Guarantees:
    /// - Monotonicity: Successive calls return increasing timestamps
    /// - Uniqueness: No two calls return the same timestamp
    /// - Causality: Preserves happened-before relationships
    async fn now(&self) -> Result<TransactionTimestamp>;

    /// Update clock from remote timestamp for causality.
    ///
    /// This ensures that future timestamps from this provider will be
    /// causally after the remote timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the remote timestamp is invalid or if the update fails.
    fn update_from(&self, remote: &HlcTimestamp) -> Result<()>;

    /// Get the node ID for this provider.
    fn node_id(&self) -> NodeId;

    /// Check if provider is healthy and can provide accurate timestamps.
    async fn is_healthy(&self) -> Result<bool>;

    /// Get current uncertainty bounds in microseconds.
    ///
    /// This represents the maximum clock skew between nodes.
    async fn uncertainty_us(&self) -> f64;
}

/// Vector clock for tracking causality across multiple nodes.
///
/// Used to detect concurrent operations and establish happened-before relationships.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VectorClock {
    /// Map of node ID to HLC timestamp
    clocks: HashMap<NodeId, HlcTimestamp>,
}

impl VectorClock {
    /// Create a new empty vector clock.
    #[must_use]
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Update the vector clock with a timestamp from a node.
    pub fn update(&mut self, timestamp: HlcTimestamp) {
        let node_id = timestamp.node_id;
        self.clocks
            .entry(node_id)
            .and_modify(|ts| {
                if timestamp > *ts {
                    *ts = timestamp.clone();
                }
            })
            .or_insert(timestamp);
    }

    /// Merge another vector clock into this one.
    pub fn merge(&mut self, other: &Self) {
        for (node_id, timestamp) in &other.clocks {
            self.clocks
                .entry(*node_id)
                .and_modify(|ts| {
                    if timestamp > ts {
                        *ts = timestamp.clone();
                    }
                })
                .or_insert_with(|| timestamp.clone());
        }
    }

    /// Check if this vector clock happened before another.
    #[must_use]
    pub fn happened_before(&self, other: &Self) -> bool {
        // All our timestamps must be <= other's
        for (node_id, our_ts) in &self.clocks {
            if let Some(their_ts) = other.clocks.get(node_id) {
                if our_ts > their_ts {
                    return false;
                }
            } else if our_ts.logical > 0 || our_ts.physical > 0 {
                // We have a timestamp they don't
                return false;
            }
        }

        // At least one of our timestamps must be < other's
        self.clocks.iter().any(|(node_id, our_ts)| {
            other
                .clocks
                .get(node_id)
                .is_some_and(|their_ts| our_ts < their_ts)
        })
    }

    /// Check if two vector clocks are concurrent.
    #[must_use]
    pub fn is_concurrent_with(&self, other: &Self) -> bool {
        !self.happened_before(other) && !other.happened_before(self)
    }
}

/// Builder for configuring HLC providers.
#[derive(Debug, Clone)]
pub struct HlcConfig {
    /// Whether to enable adaptive uncertainty tracking
    pub adaptive_uncertainty: bool,
    /// Base uncertainty in microseconds
    pub base_uncertainty_us: f64,
    /// Maximum acceptable clock drift in milliseconds
    pub max_drift_ms: f64,
    /// Node identifier
    pub node_id: NodeId,
}

impl HlcConfig {
    /// Create a new configuration with defaults.
    #[must_use]
    pub const fn new(node_id: NodeId) -> Self {
        Self {
            adaptive_uncertainty: true,
            base_uncertainty_us: 1000.0,
            max_drift_ms: 100.0,
            node_id,
        }
    }

    /// Set maximum acceptable clock drift.
    #[must_use]
    pub const fn with_max_drift(mut self, drift_ms: f64) -> Self {
        self.max_drift_ms = drift_ms;
        self
    }

    /// Set base uncertainty.
    #[must_use]
    pub const fn with_uncertainty(mut self, uncertainty_us: f64) -> Self {
        self.base_uncertainty_us = uncertainty_us;
        self
    }

    /// Enable or disable adaptive uncertainty tracking.
    #[must_use]
    pub const fn with_adaptive(mut self, adaptive: bool) -> Self {
        self.adaptive_uncertainty = adaptive;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hlc_ordering() {
        // Create test node IDs
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        let ts1 = HlcTimestamp::new(100, 0, node1);
        let ts2 = HlcTimestamp::new(100, 1, node1);
        let ts3 = HlcTimestamp::new(101, 0, node1);
        let ts4 = HlcTimestamp::new(100, 0, node2);

        // Physical time takes precedence
        assert!(ts1 < ts3);
        assert!(ts3 > ts1);

        // Then logical counter
        assert!(ts1 < ts2);
        assert!(ts2 > ts1);

        // Node ID provides total ordering (one must be less than the other)
        assert_ne!(ts1.cmp(&ts4), std::cmp::Ordering::Equal);

        // Same timestamp with same node should be equal
        let ts5 = HlcTimestamp::new(100, 0, node1);
        assert_eq!(ts1, ts5);
    }

    #[test]
    fn test_transaction_concurrency() {
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        let hlc1 = HlcTimestamp::new(1000, 0, node1);
        let hlc2 = HlcTimestamp::new(1001, 0, node2);

        let tx1 = TransactionTimestamp::new(hlc1, Utc::now(), 10.0);
        let tx2 = TransactionTimestamp::new(hlc2, Utc::now(), 10.0);

        // With overlapping uncertainty windows
        assert!(tx1.might_be_concurrent(&tx2));

        // Clear separation
        let hlc3 = HlcTimestamp::new(2000, 0, node2);
        let tx3 = TransactionTimestamp::new(hlc3, Utc::now(), 10.0);
        assert!(!tx1.might_be_concurrent(&tx3));
        assert!(tx1.definitely_before(&tx3));
    }

    #[test]
    fn test_vector_clock_causality() {
        let node1 = NodeId::from_seed(1);
        let node2 = NodeId::from_seed(2);

        let mut vc1 = VectorClock::new();
        let mut vc2 = VectorClock::new();

        // Node 1 performs operation
        let ts1 = HlcTimestamp::new(100, 0, node1);
        vc1.update(ts1);

        // Node 2 performs operation concurrently
        let ts2 = HlcTimestamp::new(101, 0, node2);
        vc2.update(ts2);

        // They are concurrent (neither happened before the other)
        assert!(vc1.is_concurrent_with(&vc2));
        assert!(vc2.is_concurrent_with(&vc1));

        // Node 2 receives Node 1's update and advances its own clock
        vc2.merge(&vc1);

        // After merge, vc2 has both timestamps
        assert_eq!(vc2.clocks.len(), 2);

        // Now update node2's own timestamp to a later value
        vc2.update(HlcTimestamp::new(102, 0, node2));

        // Create a new vector clock that only has node1's later timestamp
        let mut vc3 = VectorClock::new();
        vc3.update(HlcTimestamp::new(103, 0, node1));

        // Merge vc2 into vc3
        vc3.merge(&vc2);

        // Now vc2 happened before vc3 (vc3 has all of vc2's timestamps but later)
        assert!(vc2.happened_before(&vc3));
        assert!(!vc3.happened_before(&vc2));
    }
}
