//! Cross-layer coordination for operations that span global and local consensus
//!
//! This module handles operations that require coordination between the global
//! and local consensus layers, ensuring proper sequencing, error handling, and rollback.

use crate::{
    ConsensusGroupId,
    core::global::StreamConfig,
    error::{ConsensusResult, Error},
    operations::handlers::GlobalOperationResponse,
};
use proven_governance::Governance;
use proven_transport::Transport;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::RwLock, time::timeout};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{
    events::{ConsensusEvent, EventBus, EventResult, create_reply_channel},
    global_layer::GlobalConsensusLayer,
    groups_layer::GroupsConsensusLayer,
};

/// Types of cross-layer operations that can be pending
#[derive(Debug, Clone)]
pub enum PendingCrossLayerOp {
    /// Stream creation operation
    StreamCreation {
        /// Stream name
        name: String,
        /// Target group
        group_id: ConsensusGroupId,
        /// When operation started
        started_at: Instant,
    },

    /// Stream deletion operation
    StreamDeletion {
        /// Stream name
        name: String,
        /// When operation started
        started_at: Instant,
    },

    /// Group creation operation
    GroupCreation {
        /// Group ID
        group_id: ConsensusGroupId,
        /// When operation started
        started_at: Instant,
    },

    /// Stream migration operation
    StreamMigration {
        /// Stream name
        stream_name: String,
        /// Source group
        from_group: ConsensusGroupId,
        /// Target group
        to_group: ConsensusGroupId,
        /// When operation started
        started_at: Instant,
    },
}

/// Coordinator for cross-layer consensus operations
pub struct CrossLayerCoordinator<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Event bus for async communication
    event_bus: Arc<EventBus>,
    /// Pending operations tracking
    pending_operations: Arc<RwLock<HashMap<Uuid, PendingCrossLayerOp>>>,
    /// Operation timeout
    operation_timeout: Duration,
    /// Phantom data for type parameters
    _phantom: std::marker::PhantomData<(T, G)>,
}

impl<T, G> CrossLayerCoordinator<T, G>
where
    T: Transport,
    G: Governance,
{
    /// Create a new coordinator
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self {
            event_bus,
            pending_operations: Arc::new(RwLock::new(HashMap::new())),
            operation_timeout: Duration::from_secs(30),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set operation timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Create a stream with cross-layer coordination
    pub async fn create_stream(
        &self,
        global: &GlobalConsensusLayer,
        _local: &GroupsConsensusLayer<T, G>,
        name: String,
        config: StreamConfig,
        group_id: ConsensusGroupId,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let op_id = Uuid::new_v4();
        info!(
            "Starting cross-layer stream creation: {} in group {:?}",
            name, group_id
        );

        // 1. Register pending operation
        self.pending_operations.write().await.insert(
            op_id,
            PendingCrossLayerOp::StreamCreation {
                name: name.clone(),
                group_id,
                started_at: Instant::now(),
            },
        );

        // 2. Execute global consensus operation
        let global_result = match global.create_stream(&name, &config, group_id).await {
            Ok(result) => result,
            Err(e) => {
                error!("Global stream creation failed: {}", e);
                self.pending_operations.write().await.remove(&op_id);
                return Err(e);
            }
        };

        // If global consensus failed, no need to proceed
        if !global_result.is_success() {
            self.pending_operations.write().await.remove(&op_id);
            return Ok(global_result);
        }

        // 3. Notify local layer to initialize the stream
        let (reply_tx, reply_rx) = create_reply_channel();
        let event = ConsensusEvent::StreamCreated {
            name: name.clone(),
            config,
            group_id,
            reply_to: reply_tx,
        };

        if let Err(e) = self.event_bus.publish(event).await {
            error!("Failed to publish stream creation event: {}", e);
            // Try to rollback
            self.rollback_stream_creation(global, &name).await;
            self.pending_operations.write().await.remove(&op_id);
            return Err(Error::InvalidOperation(format!(
                "Event publication failed: {e}"
            )));
        }

        // 4. Wait for local initialization with timeout
        match timeout(self.operation_timeout, reply_rx).await {
            Ok(Ok(EventResult::Success)) => {
                info!("Stream {} successfully initialized in local layer", name);
                self.pending_operations.write().await.remove(&op_id);
                Ok(global_result)
            }
            Ok(Ok(EventResult::Failed(err))) => {
                error!("Local stream initialization failed: {}", err);
                // Rollback global state
                self.rollback_stream_creation(global, &name).await;
                self.pending_operations.write().await.remove(&op_id);
                Err(Error::InvalidOperation(format!(
                    "Local initialization failed: {err}"
                )))
            }
            Ok(Ok(EventResult::Pending(_))) => {
                warn!("Local stream initialization returned pending status");
                // For now, treat as success but log warning
                self.pending_operations.write().await.remove(&op_id);
                Ok(global_result)
            }
            Ok(Err(_)) => {
                error!("Reply channel closed unexpectedly");
                self.rollback_stream_creation(global, &name).await;
                self.pending_operations.write().await.remove(&op_id);
                Err(Error::InvalidOperation(
                    "Internal communication error".to_string(),
                ))
            }
            Err(_) => {
                error!("Timeout waiting for local stream initialization");
                self.rollback_stream_creation(global, &name).await;
                self.pending_operations.write().await.remove(&op_id);
                Err(Error::InvalidOperation(
                    "Local initialization timeout".to_string(),
                ))
            }
        }
    }

    /// Delete a stream with cross-layer coordination
    pub async fn delete_stream(
        &self,
        global: &GlobalConsensusLayer,
        _local: &GroupsConsensusLayer<T, G>,
        name: String,
    ) -> ConsensusResult<GlobalOperationResponse> {
        let op_id = Uuid::new_v4();
        info!("Starting cross-layer stream deletion: {}", name);

        // 1. Register pending operation
        self.pending_operations.write().await.insert(
            op_id,
            PendingCrossLayerOp::StreamDeletion {
                name: name.clone(),
                started_at: Instant::now(),
            },
        );

        // 2. Get the stream's group_id from global state
        let group_id = {
            let state = global.global_state();
            let configs = state.stream_configs.read().await;
            match configs.get(&name) {
                Some(config) => match config.consensus_group {
                    Some(gid) => gid,
                    None => {
                        drop(configs);
                        self.pending_operations.write().await.remove(&op_id);
                        return Err(Error::InvalidOperation(format!(
                            "Stream '{name}' has no assigned consensus group"
                        )));
                    }
                },
                None => {
                    drop(configs);
                    self.pending_operations.write().await.remove(&op_id);
                    return Err(Error::InvalidOperation(format!(
                        "Stream '{name}' not found"
                    )));
                }
            }
        };

        // 3. Notify local layer to cleanup first
        let (reply_tx, reply_rx) = create_reply_channel();
        let event = ConsensusEvent::StreamDeleted {
            name: name.clone(),
            group_id,
            reply_to: reply_tx,
        };

        if let Err(e) = self.event_bus.publish(event).await {
            error!("Failed to publish stream deletion event: {}", e);
            self.pending_operations.write().await.remove(&op_id);
            return Err(Error::InvalidOperation(format!(
                "Event publication failed: {e}"
            )));
        }

        // 3. Wait for local cleanup
        match timeout(self.operation_timeout, reply_rx).await {
            Ok(Ok(EventResult::Success)) => {
                debug!("Local cleanup completed for stream {}", name);
            }
            Ok(Ok(EventResult::Failed(err))) => {
                warn!(
                    "Local cleanup failed: {}, continuing with global deletion",
                    err
                );
            }
            _ => {
                warn!("Timeout or error during local cleanup, continuing with global deletion");
            }
        }

        // 4. Execute global consensus operation
        let global_result = global.delete_stream(&name).await?;

        self.pending_operations.write().await.remove(&op_id);
        Ok(global_result)
    }

    /// Rollback a stream creation
    async fn rollback_stream_creation(&self, global: &GlobalConsensusLayer, stream_name: &str) {
        info!("Rolling back stream creation for {}", stream_name);

        match global.delete_stream(stream_name).await {
            Ok(_) => info!(
                "Successfully rolled back stream {} from global state",
                stream_name
            ),
            Err(e) => error!(
                "Failed to rollback stream {} from global state: {}",
                stream_name, e
            ),
        }
    }

    /// Get pending operations
    pub async fn get_pending_operations(&self) -> Vec<(Uuid, PendingCrossLayerOp)> {
        self.pending_operations
            .read()
            .await
            .iter()
            .map(|(id, op)| (*id, op.clone()))
            .collect()
    }

    /// Clean up stale operations
    pub async fn cleanup_stale_operations(&self, max_age: Duration) {
        let mut pending = self.pending_operations.write().await;
        let now = Instant::now();

        pending.retain(|id, op| {
            let started_at = match op {
                PendingCrossLayerOp::StreamCreation { started_at, .. } => started_at,
                PendingCrossLayerOp::StreamDeletion { started_at, .. } => started_at,
                PendingCrossLayerOp::GroupCreation { started_at, .. } => started_at,
                PendingCrossLayerOp::StreamMigration { started_at, .. } => started_at,
            };

            let age = now.duration_since(*started_at);
            if age > max_age {
                warn!("Removing stale operation {:?} (age: {:?})", id, age);
                false
            } else {
                true
            }
        });
    }
}
