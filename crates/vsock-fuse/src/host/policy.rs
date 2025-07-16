//! Storage tiering policy engine
//!
//! This module implements policies for deciding when to move data between
//! hot and cold storage tiers.

use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

use crate::{BlobId, StorageTier, storage::BlobInfo};

/// Policy decision for a blob
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyDecision {
    /// Keep in current tier
    Keep,
    /// Move to hot tier
    MoveToHot,
    /// Move to cold tier
    MoveToCold,
    /// Delete from this tier (exists in other tier)
    Delete,
}

/// Storage tiering policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Time after last access before moving to cold tier
    pub cold_after: Duration,
    /// Time after last access before considering for deletion
    pub delete_after: Duration,
    /// Minimum file size for cold storage (bytes)
    pub min_cold_size: u64,
    /// Maximum file size for hot storage (bytes)
    pub max_hot_size: u64,
    /// Hot tier usage threshold (0.0-1.0)
    pub hot_tier_threshold: f64,
    /// Prefetch patterns
    pub prefetch_patterns: Vec<String>,
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            cold_after: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            delete_after: Duration::from_secs(90 * 24 * 60 * 60), // 90 days
            min_cold_size: 1024 * 1024,                        // 1MB
            max_hot_size: 100 * 1024 * 1024,                   // 100MB
            hot_tier_threshold: 0.9,                           // 90% full
            prefetch_patterns: vec![
                "*.db".to_string(),
                "*.sqlite".to_string(),
                "*-journal".to_string(),
                "*-wal".to_string(),
            ],
        }
    }
}

/// Policy engine for storage tiering decisions
pub struct PolicyEngine {
    policy: TieringPolicy,
}

impl PolicyEngine {
    /// Create a new policy engine
    pub fn new(policy: TieringPolicy) -> Self {
        Self { policy }
    }

    /// Get the current policy
    pub fn policy(&self) -> &TieringPolicy {
        &self.policy
    }

    /// Evaluate policy for a single blob
    pub fn evaluate_blob(
        &self,
        blob_info: &BlobInfo,
        hot_tier_usage: f64,
        current_time: SystemTime,
    ) -> PolicyDecision {
        let age = current_time
            .duration_since(blob_info.last_accessed)
            .unwrap_or(Duration::from_secs(0));

        match blob_info.tier {
            StorageTier::Hot => {
                // Check if should move to cold
                if self.should_move_to_cold(blob_info, age, hot_tier_usage) {
                    PolicyDecision::MoveToCold
                } else {
                    PolicyDecision::Keep
                }
            }
            StorageTier::Cold => {
                // Check if should prefetch to hot
                if self.should_prefetch(blob_info) {
                    PolicyDecision::MoveToHot
                } else if age > self.policy.delete_after {
                    // Very old files might be candidates for deletion
                    PolicyDecision::Keep // For now, don't auto-delete
                } else {
                    PolicyDecision::Keep
                }
            }
            StorageTier::Both => {
                // Files in both tiers - check if we need to remove from one
                if hot_tier_usage > self.policy.hot_tier_threshold {
                    PolicyDecision::Delete // Delete from hot tier
                } else {
                    PolicyDecision::Keep
                }
            }
        }
    }

    /// Check if a blob should be moved to cold storage
    fn should_move_to_cold(
        &self,
        blob_info: &BlobInfo,
        age: Duration,
        hot_tier_usage: f64,
    ) -> bool {
        // Force move if hot tier is too full
        if hot_tier_usage > self.policy.hot_tier_threshold {
            return true;
        }

        // Don't move if too small
        if blob_info.size < self.policy.min_cold_size {
            return false;
        }

        // Move if old enough
        if age > self.policy.cold_after {
            return true;
        }

        // Move if too large for hot tier
        if blob_info.size > self.policy.max_hot_size {
            return true;
        }

        false
    }

    /// Check if a blob should be prefetched to hot storage
    fn should_prefetch(&self, blob_info: &BlobInfo) -> bool {
        // For now, just check if it was accessed recently
        // Accessed in last minute

        SystemTime::now()
            .duration_since(blob_info.last_accessed)
            .unwrap_or(Duration::from_secs(u64::MAX))
            < Duration::from_secs(60)
    }

    /// Get blobs that should be migrated
    pub fn get_migration_candidates(
        &self,
        blobs: &[BlobInfo],
        hot_tier_usage: f64,
        batch_size: usize,
    ) -> Vec<(BlobId, PolicyDecision)> {
        let current_time = SystemTime::now();
        let mut candidates = Vec::new();

        for blob in blobs {
            let decision = self.evaluate_blob(blob, hot_tier_usage, current_time);
            if decision != PolicyDecision::Keep {
                candidates.push((blob.blob_id, decision));
                if candidates.len() >= batch_size {
                    break;
                }
            }
        }

        // Sort by priority (oldest first for cold migration)
        candidates.sort_by_key(|(blob_id, decision)| {
            match decision {
                PolicyDecision::MoveToCold => {
                    // Find the blob and sort by age
                    blobs
                        .iter()
                        .find(|b| b.blob_id == *blob_id)
                        .map(|b| b.last_accessed)
                        .unwrap_or(SystemTime::UNIX_EPOCH)
                }
                _ => SystemTime::now(),
            }
        });

        candidates
    }
}

/// Access pattern tracker for intelligent prefetching
pub struct AccessPatternTracker {
    /// Recent access history
    recent_accesses: Vec<(BlobId, SystemTime)>,
    /// Maximum history size
    max_history: usize,
}

impl AccessPatternTracker {
    /// Create a new access pattern tracker
    pub fn new(max_history: usize) -> Self {
        Self {
            recent_accesses: Vec::with_capacity(max_history),
            max_history,
        }
    }

    /// Record an access
    pub fn record_access(&mut self, blob_id: BlobId) {
        let now = SystemTime::now();
        self.recent_accesses.push((blob_id, now));

        // Trim old entries
        if self.recent_accesses.len() > self.max_history {
            self.recent_accesses.remove(0);
        }
    }

    /// Get related blobs that might be accessed soon
    pub fn get_prefetch_candidates(&self, _blob_id: BlobId) -> Vec<BlobId> {
        // TODO: Implement pattern detection
        // For now, return empty list
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_decision_old_file() {
        let policy = TieringPolicy {
            cold_after: Duration::from_secs(60),
            ..Default::default()
        };
        let engine = PolicyEngine::new(policy);

        let blob = BlobInfo {
            blob_id: BlobId::new(),
            size: 10 * 1024 * 1024, // 10MB
            tier: StorageTier::Hot,
            last_accessed: SystemTime::now() - Duration::from_secs(120),
            created_at: SystemTime::now() - Duration::from_secs(240),
        };

        let decision = engine.evaluate_blob(&blob, 0.5, SystemTime::now());
        assert_eq!(decision, PolicyDecision::MoveToCold);
    }

    #[test]
    fn test_policy_decision_large_file() {
        let policy = TieringPolicy {
            max_hot_size: 1024 * 1024, // 1MB
            ..Default::default()
        };
        let engine = PolicyEngine::new(policy);

        let blob = BlobInfo {
            blob_id: BlobId::new(),
            size: 10 * 1024 * 1024, // 10MB
            tier: StorageTier::Hot,
            last_accessed: SystemTime::now(),
            created_at: SystemTime::now(),
        };

        let decision = engine.evaluate_blob(&blob, 0.5, SystemTime::now());
        assert_eq!(decision, PolicyDecision::MoveToCold);
    }

    #[test]
    fn test_policy_decision_hot_tier_full() {
        let engine = PolicyEngine::new(TieringPolicy::default());

        let blob = BlobInfo {
            blob_id: BlobId::new(),
            size: 10 * 1024 * 1024, // 10MB
            tier: StorageTier::Hot,
            last_accessed: SystemTime::now(),
            created_at: SystemTime::now(),
        };

        let decision = engine.evaluate_blob(&blob, 0.95, SystemTime::now());
        assert_eq!(decision, PolicyDecision::MoveToCold);
    }
}
