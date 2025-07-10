//! Migration coordination for safe group transitions
//!
//! This module coordinates node migrations between consensus groups,
//! ensuring safe transitions with proper synchronization.

use crate::allocation::ConsensusGroupId;
use crate::error::{ConsensusResult, Error};
use crate::node_id::NodeId;
use crate::operations::{GlobalOperation, NodeOperation};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::rebalancer::{MigrationPhase, NodeMigration};

/// Events during migration coordination
#[derive(Debug, Clone)]
pub enum MigrationEvent {
    /// Node added to target group
    NodeAddedToTarget {
        /// ID of the node being added
        node_id: NodeId,
        /// ID of the target group
        group_id: ConsensusGroupId,
    },
    /// Node synced with target group
    NodeSyncedWithTarget {
        /// ID of the node that synced
        node_id: NodeId,
        /// ID of the target group
        group_id: ConsensusGroupId,
    },
    /// Node ready to leave source group
    NodeReadyToLeave {
        /// ID of the node ready to leave
        node_id: NodeId,
        /// ID of the source group
        group_id: ConsensusGroupId,
    },
    /// Node removed from source group
    NodeRemovedFromSource {
        /// ID of the node that was removed
        node_id: NodeId,
        /// ID of the source group
        group_id: ConsensusGroupId,
    },
    /// Migration completed successfully
    MigrationCompleted {
        /// ID of the migrated node
        node_id: NodeId,
        /// ID of the source group
        from_group: ConsensusGroupId,
        /// ID of the target group
        to_group: ConsensusGroupId,
    },
    /// Migration failed
    MigrationFailed {
        /// ID of the node whose migration failed
        node_id: NodeId,
        /// Reason for the failure
        reason: String,
    },
}

/// Coordinates migrations between consensus groups
pub struct MigrationCoordinator {
    /// Active migrations being coordinated
    active_migrations: HashMap<NodeId, MigrationCoordination>,
    /// Event sender for migration updates
    event_tx: mpsc::Sender<MigrationEvent>,
    /// Event receiver
    event_rx: mpsc::Receiver<MigrationEvent>,
    /// Configuration
    config: MigrationConfig,
}

/// Configuration for migration coordination
#[derive(Debug, Clone)]
pub struct MigrationConfig {
    /// Maximum time to wait for sync before timing out
    pub sync_timeout: Duration,
    /// Maximum time for entire migration
    pub migration_timeout: Duration,
    /// Interval for checking migration progress
    pub check_interval: Duration,
}

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            sync_timeout: Duration::from_secs(60),
            migration_timeout: Duration::from_secs(300),
            check_interval: Duration::from_secs(5),
        }
    }
}

/// State of a single migration coordination
#[derive(Debug, Clone)]
struct MigrationCoordination {
    /// The migration being coordinated
    migration: NodeMigration,
    /// Current phase
    phase: MigrationPhase,
    /// When coordination started
    started_at: Instant,
    /// Last progress update
    last_update: Instant,
    /// Sync progress (0.0 to 1.0)
    sync_progress: f64,
}

impl MigrationCoordinator {
    /// Create a new migration coordinator
    pub fn new(config: MigrationConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(100);
        Self {
            active_migrations: HashMap::new(),
            event_tx,
            event_rx,
            config,
        }
    }

    /// Start coordinating a migration
    pub async fn start_coordination(&mut self, migration: NodeMigration) -> ConsensusResult<()> {
        if self.active_migrations.contains_key(&migration.node_id) {
            return Err(Error::InvalidOperation(format!(
                "Migration already in progress for node {:?}",
                migration.node_id
            )));
        }

        let coordination = MigrationCoordination {
            migration: migration.clone(),
            phase: MigrationPhase::AddedToTarget,
            started_at: Instant::now(),
            last_update: Instant::now(),
            sync_progress: 0.0,
        };

        self.active_migrations
            .insert(migration.node_id.clone(), coordination);

        info!(
            "Started coordination for migration of node {:?} from {:?} to {:?}",
            migration.node_id, migration.from_group, migration.to_group
        );

        // Send initial event
        self.send_event(MigrationEvent::NodeAddedToTarget {
            node_id: migration.node_id,
            group_id: migration.to_group,
        })
        .await;

        Ok(())
    }

    /// Update sync progress for a migrating node
    pub async fn update_sync_progress(
        &mut self,
        node_id: &NodeId,
        progress: f64,
    ) -> ConsensusResult<()> {
        let (should_send_event, target_group) = {
            let coordination = self.active_migrations.get_mut(node_id).ok_or_else(|| {
                Error::InvalidOperation(format!("No active migration for node {:?}", node_id))
            })?;

            coordination.sync_progress = progress;
            coordination.last_update = Instant::now();

            // Check if sync is complete
            if progress >= 1.0 && coordination.phase == MigrationPhase::SyncingWithTarget {
                coordination.phase = MigrationPhase::ReadyToLeaveSource;
                (true, coordination.migration.to_group)
            } else {
                (false, ConsensusGroupId::new(0))
            }
        };

        if should_send_event {
            self.send_event(MigrationEvent::NodeSyncedWithTarget {
                node_id: node_id.clone(),
                group_id: target_group,
            })
            .await;

            info!(
                "Node {:?} fully synced with target group {:?}",
                node_id, target_group
            );
        }

        Ok(())
    }

    /// Mark node as ready to leave source group
    pub async fn mark_ready_to_leave(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let from_group = {
            let coordination = self.active_migrations.get_mut(node_id).ok_or_else(|| {
                Error::InvalidOperation(format!("No active migration for node {:?}", node_id))
            })?;

            if coordination.phase != MigrationPhase::SyncingWithTarget {
                return Err(Error::InvalidOperation(format!(
                    "Node {:?} not in correct phase for leaving source",
                    node_id
                )));
            }

            coordination.phase = MigrationPhase::ReadyToLeaveSource;
            coordination.migration.from_group
        };

        self.send_event(MigrationEvent::NodeReadyToLeave {
            node_id: node_id.clone(),
            group_id: from_group,
        })
        .await;

        Ok(())
    }

    /// Complete a migration
    pub async fn complete_migration(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let coordination = self.active_migrations.remove(node_id).ok_or_else(|| {
            Error::InvalidOperation(format!("No active migration for node {:?}", node_id))
        })?;

        self.send_event(MigrationEvent::MigrationCompleted {
            node_id: node_id.clone(),
            from_group: coordination.migration.from_group,
            to_group: coordination.migration.to_group,
        })
        .await;

        info!(
            "Migration completed for node {:?} in {:?}",
            node_id,
            coordination.started_at.elapsed()
        );

        Ok(())
    }

    /// Fail a migration
    pub async fn fail_migration(
        &mut self,
        node_id: &NodeId,
        reason: String,
    ) -> ConsensusResult<()> {
        self.active_migrations.remove(node_id);

        self.send_event(MigrationEvent::MigrationFailed {
            node_id: node_id.clone(),
            reason: reason.clone(),
        })
        .await;

        warn!("Migration failed for node {:?}: {}", node_id, reason);

        Ok(())
    }

    /// Check for timed out migrations
    pub async fn check_timeouts(&mut self) -> Vec<NodeId> {
        let mut timed_out = Vec::new();
        let now = Instant::now();

        for (node_id, coordination) in &self.active_migrations {
            let elapsed = now.duration_since(coordination.started_at);

            // Check overall timeout
            if elapsed > self.config.migration_timeout {
                timed_out.push(node_id.clone());
                continue;
            }

            // Check sync timeout
            if coordination.phase == MigrationPhase::SyncingWithTarget {
                let sync_elapsed = now.duration_since(coordination.last_update);
                if sync_elapsed > self.config.sync_timeout {
                    timed_out.push(node_id.clone());
                }
            }
        }

        // Fail timed out migrations
        for node_id in &timed_out {
            self.fail_migration(node_id, "Migration timed out".to_string())
                .await
                .ok();
        }

        timed_out
    }

    /// Get migration progress
    pub fn get_migration_progress(&self, node_id: &NodeId) -> Option<MigrationProgress> {
        self.active_migrations
            .get(node_id)
            .map(|coord| MigrationProgress {
                phase: coord.phase,
                sync_progress: coord.sync_progress,
                elapsed: coord.started_at.elapsed(),
                from_group: coord.migration.from_group,
                to_group: coord.migration.to_group,
            })
    }

    /// Get event receiver
    pub fn event_receiver(&mut self) -> &mut mpsc::Receiver<MigrationEvent> {
        &mut self.event_rx
    }

    /// Send migration event
    async fn send_event(&self, event: MigrationEvent) {
        if let Err(e) = self.event_tx.send(event).await {
            warn!("Failed to send migration event: {}", e);
        }
    }

    /// Generate global operations for migration steps
    pub fn generate_migration_operations(&self, node_id: &NodeId) -> Vec<GlobalOperation> {
        let mut operations = Vec::new();

        if let Some(coordination) = self.active_migrations.get(node_id) {
            match coordination.phase {
                MigrationPhase::AddedToTarget => {
                    // Already added, waiting for sync
                }
                MigrationPhase::SyncingWithTarget => {
                    // Node is syncing, no operations needed
                }
                MigrationPhase::ReadyToLeaveSource => {
                    // Generate operation to remove from source group
                    operations.push(GlobalOperation::Node(NodeOperation::RemoveFromGroup {
                        node_id: node_id.clone(),
                        group_id: coordination.migration.from_group,
                    }));
                }
                MigrationPhase::Complete => {
                    // Migration complete, no operations needed
                }
            }
        }

        operations
    }
}

/// Migration progress information
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    /// Current phase
    pub phase: MigrationPhase,
    /// Sync progress (0.0 to 1.0)
    pub sync_progress: f64,
    /// Time elapsed since start
    pub elapsed: Duration,
    /// Source group
    pub from_group: ConsensusGroupId,
    /// Target group
    pub to_group: ConsensusGroupId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_migration_coordination_flow() {
        let config = MigrationConfig::default();
        let mut coordinator = MigrationCoordinator::new(config);

        let node_id = NodeId::from_seed(1);
        let migration = NodeMigration {
            node_id: node_id.clone(),
            from_group: ConsensusGroupId::new(1),
            to_group: ConsensusGroupId::new(2),
            priority: 0.8,
            reason: "Test migration".to_string(),
        };

        // Start coordination
        coordinator.start_coordination(migration).await.unwrap();
        assert!(coordinator.active_migrations.contains_key(&node_id));

        // Update sync progress
        coordinator
            .update_sync_progress(&node_id, 0.5)
            .await
            .unwrap();
        let progress = coordinator.get_migration_progress(&node_id).unwrap();
        assert_eq!(progress.sync_progress, 0.5);

        // Complete sync
        coordinator
            .update_sync_progress(&node_id, 1.0)
            .await
            .unwrap();

        // Complete migration
        coordinator.complete_migration(&node_id).await.unwrap();
        assert!(!coordinator.active_migrations.contains_key(&node_id));
    }

    #[tokio::test]
    async fn test_migration_timeout() {
        let config = MigrationConfig {
            migration_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let mut coordinator = MigrationCoordinator::new(config);

        let node_id = NodeId::from_seed(1);
        let migration = NodeMigration {
            node_id: node_id.clone(),
            from_group: ConsensusGroupId::new(1),
            to_group: ConsensusGroupId::new(2),
            priority: 0.8,
            reason: "Test migration".to_string(),
        };

        coordinator.start_coordination(migration).await.unwrap();

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(2)).await;

        let timed_out = coordinator.check_timeouts().await;
        assert!(timed_out.contains(&node_id));
        assert!(!coordinator.active_migrations.contains_key(&node_id));
    }
}
