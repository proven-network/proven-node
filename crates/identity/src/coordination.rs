//! Stream-based leadership coordination.
//!
//! This module implements leader election using engine streams instead of
//! a separate lock manager. The pattern uses lease-based leadership where
//! nodes compete to hold a time-limited lease.

#![allow(clippy::cast_possible_truncation)]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proven_engine::Client;
use proven_storage::LogIndex;
use proven_topology::NodeId;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::error::Error;

/// A leadership lease message published to the coordination stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadershipLease {
    /// The node claiming leadership
    pub node_id: NodeId,
    /// When this lease expires (milliseconds since UNIX epoch)
    pub expires_at_ms: u64,
    /// Sequence number for fencing (from the stream)
    pub sequence: u64,
}

impl LeadershipLease {
    /// Check if this lease is currently valid.
    pub fn is_valid(&self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.expires_at_ms > now_ms
    }

    /// Create a new lease with the given duration.
    pub fn new(node_id: NodeId, duration: Duration) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let expires_at_ms = now.as_millis() as u64 + duration.as_millis() as u64;

        Self {
            node_id,
            expires_at_ms,
            sequence: 0, // Will be set when published
        }
    }
}

/// Coordinates leadership election using a stream.
pub struct LeadershipCoordinator {
    /// Engine client
    client: Arc<Client>,
    /// Stream name for coordination
    stream_name: String,
    /// This node's ID
    node_id: NodeId,
    /// Lease duration
    lease_duration: Duration,
    /// How often to renew the lease
    renewal_interval: Duration,
    /// Current leadership state
    state: Arc<RwLock<LeadershipState>>,
    /// Background task handles
    tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,
}

#[derive(Debug, Clone)]
struct LeadershipState {
    /// Are we currently the leader?
    is_leader: bool,
    /// Current lease (if any)
    current_lease: Option<LeadershipLease>,
    /// Last sequence we've seen
    last_seen_sequence: u64,
}

impl LeadershipCoordinator {
    /// Create a new leadership coordinator.
    pub fn new(
        client: Arc<Client>,
        stream_name: String,
        node_id: NodeId,
        lease_duration: Duration,
        renewal_interval: Duration,
    ) -> Self {
        let coordinator = Self {
            client,
            stream_name,
            node_id,
            lease_duration,
            renewal_interval,
            state: Arc::new(RwLock::new(LeadershipState {
                is_leader: false,
                current_lease: None,
                last_seen_sequence: 0,
            })),
            tasks: Arc::new(RwLock::new(Vec::new())),
        };

        // Start background tasks
        coordinator.start_background_tasks();

        coordinator
    }

    /// Check if this node is currently the leader.
    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.is_leader
    }

    /// Start background tasks for leadership management.
    fn start_background_tasks(&self) {
        // Task 1: Watch for leadership changes
        let watch_handle = {
            let client = Arc::clone(&self.client);
            let stream_name = self.stream_name.clone();
            let node_id = self.node_id.clone();
            let state = Arc::clone(&self.state);

            tokio::spawn(async move {
                if let Err(e) =
                    Self::watch_leadership_stream(client, stream_name, node_id, state).await
                {
                    tracing::error!("Leadership watch task failed: {}", e);
                }
            })
        };

        // Task 2: Attempt to acquire/renew leadership
        let renewal_handle = {
            let client = Arc::clone(&self.client);
            let stream_name = self.stream_name.clone();
            let node_id = self.node_id.clone();
            let state = Arc::clone(&self.state);
            let lease_duration = self.lease_duration;
            let renewal_interval = self.renewal_interval;

            tokio::spawn(async move {
                Self::leadership_renewal_loop(
                    client,
                    stream_name,
                    node_id,
                    state,
                    lease_duration,
                    renewal_interval,
                )
                .await;
            })
        };

        // Store task handles
        let tasks = self.tasks.clone();
        tokio::spawn(async move {
            let mut guard = tasks.write().await;
            guard.push(watch_handle);
            guard.push(renewal_handle);
        });
    }

    /// Watch the leadership stream for changes.
    #[allow(clippy::cognitive_complexity)]
    async fn watch_leadership_stream(
        client: Arc<Client>,
        stream_name: String,
        node_id: NodeId,
        state: Arc<RwLock<LeadershipState>>,
    ) -> Result<(), Error> {
        use tokio::pin;

        // Start from the beginning to build complete state
        let start_seq = LogIndex::new(1).unwrap();

        let stream = client
            .stream_messages(stream_name.clone(), start_seq, None)
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        pin!(stream);

        tracing::info!("Started watching leadership stream with follow mode");

        while let Some(message) = tokio_stream::StreamExt::next(&mut stream).await {
            // Deserialize lease
            let lease: LeadershipLease = ciborium::de::from_reader(&message.data.payload[..])
                .map_err(|e| Error::Deserialization(e.to_string()))?;

            // Update state
            let mut state_guard = state.write().await;

            // Only consider valid leases
            if lease.is_valid() {
                state_guard.current_lease = Some(lease.clone());
                state_guard.is_leader = lease.node_id == node_id;

                if state_guard.is_leader {
                    tracing::info!(
                        "We are the leader (lease expires at {})",
                        lease.expires_at_ms
                    );
                } else {
                    tracing::debug!("Node {:?} is the leader", lease.node_id);
                }
            } else if let Some(current) = &state_guard.current_lease {
                // Lease expired
                if current.node_id == lease.node_id {
                    tracing::info!("Leadership lease expired for node {:?}", lease.node_id);
                    state_guard.current_lease = None;
                    if state_guard.is_leader {
                        state_guard.is_leader = false;
                        tracing::warn!("We lost leadership due to lease expiry");
                    }
                }
            }

            state_guard.last_seen_sequence = message.sequence.get();
        }

        Ok(())
    }

    /// Periodically attempt to acquire or renew leadership.
    #[allow(clippy::cognitive_complexity)]
    async fn leadership_renewal_loop(
        client: Arc<Client>,
        stream_name: String,
        node_id: NodeId,
        state: Arc<RwLock<LeadershipState>>,
        lease_duration: Duration,
        renewal_interval: Duration,
    ) {
        let mut interval = tokio::time::interval(renewal_interval);

        loop {
            interval.tick().await;

            // Check current state
            let should_claim = {
                let state_guard = state.read().await;

                // Claim if:
                // 1. We're already the leader (renewal)
                // 2. There's no current valid lease
                // 3. The current lease is about to expire (within renewal interval)
                if state_guard.is_leader {
                    true
                } else if let Some(lease) = &state_guard.current_lease {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    let expires_soon =
                        lease.expires_at_ms < now_ms + renewal_interval.as_millis() as u64;
                    !lease.is_valid() || expires_soon
                } else {
                    true
                }
            };

            if should_claim {
                // Try to claim leadership
                let lease = LeadershipLease::new(node_id.clone(), lease_duration);

                // Serialize lease
                let mut payload = Vec::new();
                if let Err(e) = ciborium::ser::into_writer(&lease, &mut payload) {
                    tracing::error!("Failed to serialize lease: {}", e);
                    continue;
                }

                // Publish lease claim
                let message = proven_engine::Message::new(payload);
                match client
                    .publish_to_stream(stream_name.clone(), vec![message])
                    .await
                {
                    Ok(_) => {
                        tracing::debug!("Published leadership lease claim");
                    }
                    Err(e) => {
                        tracing::error!("Failed to publish leadership lease: {}", e);
                    }
                }
            }
        }
    }
}

impl Drop for LeadershipCoordinator {
    fn drop(&mut self) {
        // Cancel background tasks
        let tasks = self.tasks.clone();
        tokio::spawn(async move {
            let guard = tasks.read().await;
            for handle in guard.iter() {
                handle.abort();
            }
        });
    }
}
