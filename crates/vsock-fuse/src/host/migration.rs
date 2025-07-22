//! Data migration scheduler for tiered storage
//!
//! This module handles background migration of data between hot and cold tiers
//! based on policy decisions.

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, mpsc};
use tokio::time;

use crate::{BlobId, error::Result};
use proven_logger::{error, info};

use super::{
    policy::{PolicyDecision, PolicyEngine},
    storage::{ColdTier, HotTier},
};

/// Migration task information
#[derive(Debug, Clone)]
pub struct MigrationTask {
    pub blob_id: BlobId,
    pub decision: PolicyDecision,
    pub priority: u8,
    pub created_at: SystemTime,
}

/// Migration statistics
#[derive(Debug, Default, Clone)]
pub struct MigrationStats {
    pub total_migrations: u64,
    pub successful_migrations: u64,
    pub failed_migrations: u64,
    pub bytes_migrated_to_cold: u64,
    pub bytes_migrated_to_hot: u64,
    pub last_migration_time: Option<SystemTime>,
    pub average_migration_duration: Duration,
}

/// Migration scheduler
pub struct MigrationScheduler {
    /// Policy engine
    policy_engine: Arc<PolicyEngine>,
    /// Hot tier storage
    hot_tier: Arc<HotTier>,
    /// Cold tier storage
    cold_tier: Arc<ColdTier>,
    /// Migration queue
    queue: Arc<Mutex<Vec<MigrationTask>>>,
    /// Migration statistics
    stats: Arc<Mutex<MigrationStats>>,
    /// Channel for stop signal
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl MigrationScheduler {
    /// Create a new migration scheduler
    pub fn new(
        policy_engine: Arc<PolicyEngine>,
        hot_tier: Arc<HotTier>,
        cold_tier: Arc<ColdTier>,
    ) -> Self {
        Self {
            policy_engine,
            hot_tier,
            cold_tier,
            queue: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(MigrationStats::default())),
            shutdown_tx: None,
        }
    }

    /// Start the migration scheduler
    pub async fn start(&mut self, scan_interval: Duration) -> Result<()> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        let policy_engine = self.policy_engine.clone();
        let hot_tier = self.hot_tier.clone();
        let cold_tier = self.cold_tier.clone();
        let queue = self.queue.clone();

        // Spawn scanner task
        tokio::spawn(async move {
            let mut interval = time::interval(scan_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = Self::scan_and_queue(
                            &policy_engine,
                            &hot_tier,
                            &cold_tier,
                            &queue,
                        ).await {
                            error!("Migration scan failed: {e}");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Migration scanner shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn worker tasks
        for i in 0..2 {
            let hot_tier = self.hot_tier.clone();
            let cold_tier = self.cold_tier.clone();
            let queue = self.queue.clone();
            let stats = self.stats.clone();

            tokio::spawn(async move {
                info!("Migration worker {i} started");

                loop {
                    // Get next task
                    let task = {
                        let mut q = queue.lock().await;
                        q.pop()
                    };

                    if let Some(task) = task {
                        let start_time = SystemTime::now();

                        match Self::execute_migration(&task, &hot_tier, &cold_tier).await {
                            Ok(bytes) => {
                                let duration = SystemTime::now()
                                    .duration_since(start_time)
                                    .unwrap_or(Duration::from_secs(0));

                                Self::update_stats(&stats, &task, true, bytes, duration).await;
                            }
                            Err(e) => {
                                error!("Migration failed for blob {:?}: {}", task.blob_id, e);

                                Self::update_stats(&stats, &task, false, 0, Duration::from_secs(0))
                                    .await;
                            }
                        }
                    } else {
                        // No tasks, sleep briefly
                        time::sleep(Duration::from_secs(10)).await;
                    }
                }
            });
        }

        Ok(())
    }

    /// Stop the migration scheduler
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Scan for migration candidates and queue them
    async fn scan_and_queue(
        policy_engine: &Arc<PolicyEngine>,
        hot_tier: &Arc<HotTier>,
        _cold_tier: &Arc<ColdTier>,
        queue: &Arc<Mutex<Vec<MigrationTask>>>,
    ) -> Result<()> {
        // Get hot tier statistics
        let hot_stats = hot_tier.get_stats().await?;
        let hot_usage = hot_stats.used_bytes as f64 / hot_stats.total_bytes as f64;

        // Get list of all blobs from hot tier
        let blobs = hot_tier.list_blobs().await?;

        // Get migration candidates
        let candidates = policy_engine.get_migration_candidates(
            &blobs, hot_usage, 100, // Batch size
        );

        // Queue migrations
        let mut q = queue.lock().await;
        let now = SystemTime::now();

        for (blob_id, decision) in candidates {
            let priority = match decision {
                PolicyDecision::MoveToCold if hot_usage > 0.95 => 255, // Urgent
                PolicyDecision::MoveToCold => 128,
                PolicyDecision::MoveToHot => 64,
                PolicyDecision::Delete => 32,
                PolicyDecision::Keep => continue,
            };

            q.push(MigrationTask {
                blob_id,
                decision,
                priority,
                created_at: now,
            });
        }

        // Sort by priority (highest first)
        q.sort_by_key(|t| std::cmp::Reverse(t.priority));

        Ok(())
    }

    /// Execute a single migration task
    async fn execute_migration(
        task: &MigrationTask,
        hot_tier: &Arc<HotTier>,
        cold_tier: &Arc<ColdTier>,
    ) -> Result<u64> {
        match task.decision {
            PolicyDecision::MoveToCold => {
                // Read from hot tier
                let data = hot_tier.get(&task.blob_id).await?;
                let size = data.len() as u64;

                // Write to cold tier
                cold_tier.store(task.blob_id, data).await?;

                // Delete from hot tier
                hot_tier.delete(&task.blob_id).await?;

                info!(
                    "Migrated blob {:?} to cold tier ({} bytes)",
                    task.blob_id, size
                );

                Ok(size)
            }
            PolicyDecision::MoveToHot => {
                // Read from cold tier
                let data = cold_tier.get(&task.blob_id).await?;
                let size = data.len() as u64;

                // Write to hot tier
                hot_tier.store(task.blob_id, data).await?;

                // Optionally keep in cold tier for redundancy
                // For now, we'll delete to save space
                cold_tier.delete(&task.blob_id).await?;

                info!(
                    "Migrated blob {:?} to hot tier ({} bytes)",
                    task.blob_id, size
                );

                Ok(size)
            }
            PolicyDecision::Delete => {
                // Just delete from hot tier (assumes it exists in cold)
                hot_tier.delete(&task.blob_id).await?;

                info!("Deleted blob {:?} from hot tier", task.blob_id);

                Ok(0)
            }
            PolicyDecision::Keep => {
                // Nothing to do
                Ok(0)
            }
        }
    }

    /// Update migration statistics
    async fn update_stats(
        stats: &Arc<Mutex<MigrationStats>>,
        task: &MigrationTask,
        success: bool,
        bytes: u64,
        duration: Duration,
    ) {
        let mut s = stats.lock().await;

        s.total_migrations += 1;

        if success {
            s.successful_migrations += 1;
            s.last_migration_time = Some(SystemTime::now());

            match task.decision {
                PolicyDecision::MoveToCold => {
                    s.bytes_migrated_to_cold += bytes;
                }
                PolicyDecision::MoveToHot => {
                    s.bytes_migrated_to_hot += bytes;
                }
                _ => {}
            }

            // Update average duration
            if s.successful_migrations == 1 {
                s.average_migration_duration = duration;
            } else {
                let total_duration = s.average_migration_duration.as_secs_f64()
                    * (s.successful_migrations - 1) as f64
                    + duration.as_secs_f64();
                s.average_migration_duration =
                    Duration::from_secs_f64(total_duration / s.successful_migrations as f64);
            }
        } else {
            s.failed_migrations += 1;
        }
    }

    /// Get migration statistics
    pub async fn get_stats(&self) -> MigrationStats {
        self.stats.lock().await.clone()
    }

    /// Get queue size
    pub async fn get_queue_size(&self) -> usize {
        self.queue.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::policy::TieringPolicy;

    #[tokio::test]
    async fn test_migration_scheduler_creation() {
        let policy = TieringPolicy::default();
        let policy_engine = Arc::new(PolicyEngine::new(policy));

        // Create mock tiers (would need proper mocks in real tests)
        let hot_tier = Arc::new(
            HotTier::new(std::path::PathBuf::from("/tmp/hot"))
                .await
                .unwrap(),
        );
        let cold_tier = Arc::new(
            ColdTier::new("test-bucket".to_string(), "test-prefix".to_string())
                .await
                .unwrap(),
        );

        let scheduler = MigrationScheduler::new(policy_engine, hot_tier, cold_tier);

        assert_eq!(scheduler.get_queue_size().await, 0);
    }
}
