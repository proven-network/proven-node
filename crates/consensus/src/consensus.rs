//! Main consensus implementation
//!
//! This is the central component that owns all business logic and coordinates
//! between transport, storage, COSE, attestation, and Raft.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use proven_attestation::Attestor;
use proven_bootable::Bootable;
use proven_governance::Governance;

use crate::config::ConsensusConfig;
use crate::error::{ConsensusError, ConsensusResult};
use crate::network::messages::ClusterDiscoveryResponse;
use crate::network::{ClusterState, InitiatorReason};
use crate::state_machine::StreamStore;
use crate::types::{MessagingOperation, MessagingRequest, MessagingResponse, NodeId, TypeConfig};

/// Storage wrapper to handle different storage types
#[derive(Debug, Clone)]
pub enum StorageWrapper {
    /// Memory storage
    Memory(crate::storage::MemoryConsensusStorage),
    /// RocksDB storage
    RocksDB(crate::storage::RocksConsensusStorage),
}

/// Main consensus system that coordinates application-level logic
/// All networking, cluster management, and Raft concerns are handled by NetworkManager
pub struct Consensus<G, A>
where
    G: Governance + Send + Sync + 'static,
    A: Attestor + Send + Sync + 'static,
{
    /// High-level network manager (owns transport, Raft, topology, cluster state)
    network_manager: Arc<crate::network::NetworkManager<G, A>>,
    /// Node identifier (public key)
    node_id: NodeId,
    /// Stream store for local state
    stream_store: Arc<StreamStore>,
    /// Background task handles
    task_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
}

impl<G, A> Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new consensus instance with unified configuration
    pub async fn new(config: ConsensusConfig<G, A>) -> ConsensusResult<Self> {
        // Generate node ID from public key
        let node_id = NodeId::new(config.signing_key.verifying_key());

        info!("Creating consensus instance for node {}", node_id);

        // Create stream store
        let stream_store = Arc::new(StreamStore::new());

        // Create storage based on configuration
        let storage = match config.storage_config {
            crate::config::StorageConfig::Memory => {
                StorageWrapper::Memory(crate::storage::create_memory_storage()?)
            }
            crate::config::StorageConfig::RocksDB { path } => StorageWrapper::RocksDB(
                crate::storage::create_rocks_storage(&path.to_string_lossy())?,
            ),
        };

        // Create network manager (creates its own transport, COSE handler, etc.)
        let raft_config = Arc::new(config.raft_config);
        let network_manager = match &storage {
            StorageWrapper::Memory(mem_storage) => {
                crate::network::NetworkManager::new(
                    config.signing_key,
                    config.governance.clone(),
                    config.attestor.clone(),
                    node_id.clone(),
                    config.transport_config,
                    mem_storage.clone(),
                    raft_config,
                )
                .await
            }
            StorageWrapper::RocksDB(rocks_storage) => {
                crate::network::NetworkManager::new(
                    config.signing_key,
                    config.governance.clone(),
                    config.attestor.clone(),
                    node_id.clone(),
                    config.transport_config,
                    rocks_storage.clone(),
                    raft_config,
                )
                .await
            }
        }
        .map_err(|e| ConsensusError::Raft(format!("Failed to create NetworkManager: {e}")))?;

        // Create consensus instance
        let mut consensus = Self {
            node_id: node_id.clone(),
            stream_store,
            network_manager: Arc::new(network_manager),
            task_handles: Arc::new(std::sync::Mutex::new(Vec::new())),
        };

        // Start components
        consensus.start_internal().await?;

        Ok(consensus)
    }

    /// Start internal components
    async fn start_internal(&mut self) -> ConsensusResult<()> {
        // Start topology manager (now owned by NetworkManager)
        self.network_manager.start_topology().await?;

        // Start the network transport and message handling
        self.network_manager.start_network().await?;

        Ok(())
    }

    /// Get the current term
    pub fn current_term(&self) -> Option<u64> {
        self.network_manager.current_term()
    }

    /// Get the current leader
    pub async fn current_leader(&self) -> Option<String> {
        self.network_manager.current_leader().await
    }

    /// Get the cluster size
    pub fn cluster_size(&self) -> Option<usize> {
        self.network_manager.cluster_size()
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.network_manager.is_leader()
    }

    /// Check if this node has an active cluster
    pub async fn has_active_cluster(&self) -> bool {
        self.network_manager.has_active_cluster().await
    }

    /// Discover existing clusters (delegated to NetworkManager)
    async fn discover_existing_clusters(&self) -> ConsensusResult<Vec<ClusterDiscoveryResponse>> {
        // Delegate to NetworkManager
        self.network_manager.discover_existing_clusters().await
    }

    /// Check if this consensus instance supports HTTP integration
    pub fn supports_http_integration(&self) -> bool {
        self.network_manager.supports_http_integration()
    }

    /// Create HTTP router for WebSocket transport
    pub fn create_router(&self) -> ConsensusResult<Router> {
        if let Some(router) = self.network_manager.create_router() {
            Ok(router)
        } else {
            Err(ConsensusError::Configuration(
                "HTTP integration not supported by current transport".to_string(),
            ))
        }
    }

    /// Submit a messaging request for consensus
    pub async fn submit_request(
        &self,
        request: MessagingRequest,
    ) -> ConsensusResult<MessagingResponse> {
        // Submit to Raft for consensus through NetworkManager
        match self.network_manager.submit_request(request.clone()).await {
            Ok(raft_response) => {
                // If Raft successfully committed the operation, also apply it to the StreamStore
                if raft_response.success {
                    let stream_response = self
                        .stream_store
                        .apply_operation(&request.operation, raft_response.sequence)
                        .await;

                    // Use the StreamStore response as the final result since it has the actual state changes
                    Ok(stream_response)
                } else {
                    Ok(raft_response)
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Internal method to propose a request through Raft consensus
    async fn propose_request(&self, request: &MessagingRequest) -> ConsensusResult<u64> {
        self.network_manager.propose_request(request).await
    }

    /// Publish a message through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_message(&self, stream: String, data: Bytes) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream { stream, data },
        };

        self.propose_request(&request).await
    }

    /// Publish a message with metadata through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_message_with_metadata(
        &self,
        stream: String,
        data: Bytes,
        metadata: std::collections::HashMap<String, String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStreamWithMetadata {
                stream,
                data,
                metadata,
            },
        };

        self.propose_request(&request).await
    }

    /// Publish multiple messages as a batch through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish_batch(
        &self,
        stream: String,
        messages: Vec<Bytes>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::PublishBatchToStream { stream, messages },
        };

        self.propose_request(&request).await
    }

    /// Publish a message to a subject through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn publish(&self, subject: String, data: Bytes) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::Publish { subject, data },
        };

        self.propose_request(&request).await
    }

    /// Rollup operation through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn rollup_message(
        &self,
        stream: String,
        data: Bytes,
        expected_seq: u64,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::RollupStream {
                stream,
                data,
                expected_seq,
            },
        };

        self.propose_request(&request).await
    }

    /// Delete a message from a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn delete_message(&self, stream: String, sequence: u64) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::DeleteFromStream { stream, sequence },
        };

        self.propose_request(&request).await
    }

    /// Subscribe a stream to a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn subscribe_stream_to_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();
        let subject_pattern = subject_pattern.into();

        let request = MessagingRequest {
            operation: MessagingOperation::SubscribeToSubject {
                stream_name,
                subject_pattern,
            },
        };

        self.propose_request(&request).await
    }

    /// Unsubscribe a stream from a subject pattern through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn unsubscribe_stream_from_subject(
        &self,
        stream_name: impl Into<String>,
        subject_pattern: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::UnsubscribeFromSubject {
                stream_name: stream_name.into(),
                subject_pattern: subject_pattern.into(),
            },
        };

        self.propose_request(&request).await
    }

    /// Remove all subject subscriptions for a stream through consensus
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn remove_stream_subscriptions(
        &self,
        stream_name: impl Into<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::RemoveStreamSubscriptions {
                stream_name: stream_name.into(),
            },
        };

        self.propose_request(&request).await
    }

    /// Subscribe a stream to multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual subscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn bulk_subscribe_stream_to_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> ConsensusResult<u64> {
        let stream_name = stream_name.into();

        let request = MessagingRequest {
            operation: MessagingOperation::BulkSubscribeToSubjects {
                stream_name,
                subject_patterns,
            },
        };

        self.propose_request(&request).await
    }

    /// Unsubscribe a stream from multiple subject patterns in one consensus operation
    ///
    /// This is more efficient than multiple individual unsubscriptions.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft instance is not initialized or if consensus fails.
    pub async fn bulk_unsubscribe_stream_from_subjects(
        &self,
        stream_name: impl Into<String>,
        subject_patterns: Vec<String>,
    ) -> ConsensusResult<u64> {
        let request = MessagingRequest {
            operation: MessagingOperation::BulkUnsubscribeFromSubjects {
                stream_name: stream_name.into(),
                subject_patterns,
            },
        };

        self.propose_request(&request).await
    }

    /// Get a message by sequence number from local store
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub async fn get_message(&self, stream: &str, seq: u64) -> ConsensusResult<Option<Bytes>> {
        Ok(self.stream_store.get_message(stream, seq).await)
    }

    /// Get the last sequence number for a stream
    ///
    /// # Errors
    ///
    /// Currently always returns `Ok` but may return errors in future implementations.
    pub async fn last_sequence(&self, stream: &str) -> ConsensusResult<u64> {
        Ok(self.stream_store.last_sequence(stream).await)
    }

    /// Get all streams that should receive messages for a given subject
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn route_subject(&self, subject: &str) -> std::collections::HashSet<String> {
        self.stream_store.route_subject(subject).await
    }

    /// Get all subject patterns that a stream is subscribed to
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_stream_subjects(
        &self,
        stream_name: &str,
    ) -> Option<std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return None as this functionality needs to be added to StreamStore
        let _ = stream_name;
        None
    }

    /// Get all streams subscribed to a specific subject pattern
    ///
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_subject_streams(
        &self,
        subject_pattern: &str,
    ) -> Option<std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return None as this functionality needs to be added to StreamStore
        let _ = subject_pattern;
        None
    }

    /// Get a summary of all current subject subscriptions
    ///
    /// Returns a map from subject patterns to the streams subscribed to them.
    /// This reads from the consensus-synchronized routing table.
    pub async fn get_all_subscriptions(
        &self,
    ) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
        // TODO: Implement in StreamStore
        // For now, return empty map as this functionality needs to be added to StreamStore
        std::collections::HashMap::new()
    }

    /// Get current Raft metrics
    pub fn metrics(&self) -> Option<openraft::RaftMetrics<TypeConfig>> {
        self.network_manager.metrics()
    }

    /// Get connected peers
    pub async fn get_connected_peers(&self) -> ConsensusResult<Vec<(NodeId, bool, SystemTime)>> {
        self.network_manager.get_connected_peers().await
    }

    /// Get current cluster state
    pub async fn cluster_state(&self) -> ClusterState {
        self.network_manager.cluster_state().await
    }

    /// Wait for the cluster to be ready (either Initiator or Joined state)
    pub async fn wait_for_leader(
        &self,
        timeout: Option<std::time::Duration>,
    ) -> ConsensusResult<()> {
        self.network_manager.wait_for_leader(timeout).await
    }

    /// Check if cluster discovery is in progress
    pub async fn is_cluster_discovering(&self) -> bool {
        matches!(self.cluster_state().await, ClusterState::Discovering)
    }

    /// Start the consensus system and handle cluster discovery
    pub async fn start(&self) -> ConsensusResult<()> {
        info!("Starting consensus system for node {}", self.node_id);

        // Get all peers from topology
        let all_peers = self.network_manager.topology().get_all_peers().await;

        if all_peers.is_empty() {
            // Single node topology - immediately become initiator
            info!("Single node topology detected, becoming cluster initiator");
            self.initialize_single_node_cluster(InitiatorReason::SingleNode)
                .await?;
        } else {
            // Multiple nodes - start discovery (connections will be established on-demand)
            info!(
                "Multiple nodes in topology ({}), starting discovery with on-demand connections",
                all_peers.len()
            );

            info!("Starting cluster discovery with on-demand connections");
            self.discover_and_join_cluster(all_peers).await?;
        }

        Ok(())
    }

    /// Discover existing clusters and join or become initiator
    async fn discover_and_join_cluster(
        &self,
        all_peers: Vec<crate::types::Node>,
    ) -> ConsensusResult<()> {
        // Set state to discovering
        self.network_manager
            .set_cluster_state(ClusterState::Discovering)
            .await;

        let discovery_timeout = Duration::from_secs(10); // Shorter timeout for multi-node scenarios

        info!(
            "Starting cluster discovery for node {} with timeout: {:?}",
            self.node_id, discovery_timeout
        );

        // Run discovery with timeout
        let start_time = std::time::Instant::now();

        while start_time.elapsed() < discovery_timeout {
            tokio::time::sleep(Duration::from_millis(1000)).await;

            info!("Performing cluster discovery round...");

            // Clear previous responses (now handled by NetworkManager)
            // No need to clear manually as NetworkManager handles this

            // Perform discovery
            let responses = self.discover_existing_clusters().await?;

            // Check if any peer reports an active cluster
            if let Some(existing_cluster) = responses
                .iter()
                .find(|response| response.has_active_cluster)
            {
                info!(
                    "Found existing cluster reported by {}: term={:?}, leader={:?}, size={:?}",
                    existing_cluster.responder_id,
                    existing_cluster.current_term,
                    existing_cluster.current_leader,
                    existing_cluster.cluster_size
                );

                // Join the existing cluster via Raft membership change
                self.join_existing_cluster_via_raft(existing_cluster)
                    .await?;
                return Ok(());
            }

            info!("No active cluster found in this round, continuing discovery...");
        }

        // Discovery timeout - decide what to do based on our role
        info!(
            "Cluster discovery timeout after {:?}. Deciding cluster formation strategy.",
            discovery_timeout
        );

        // Use deterministic node ordering to decide who becomes the multi-node cluster coordinator
        let mut all_node_ids: Vec<NodeId> = all_peers
            .iter()
            .map(|p| NodeId::new(p.public_key()))
            .collect();
        all_node_ids.push(self.node_id.clone());
        all_node_ids.sort(); // Deterministic ordering

        if let Some(coordinator_id) = all_node_ids.first() {
            if coordinator_id == &self.node_id {
                info!("Selected as cluster coordinator, initializing multi-node cluster");

                // Peers are already GovernanceNodes, no conversion needed
                self.initialize_multi_node_cluster(all_peers).await?;
            } else {
                info!("Not selected as coordinator, waiting for coordinator to initialize cluster");
                // Wait a bit more for the coordinator to set up the cluster
                tokio::time::sleep(Duration::from_secs(2)).await;

                // Try one more discovery round
                let responses = self.discover_existing_clusters().await?;

                if let Some(existing_cluster) = responses
                    .iter()
                    .find(|response| response.has_active_cluster)
                {
                    info!("Found coordinator's cluster, attempting to join");
                    self.join_existing_cluster_via_raft(existing_cluster)
                        .await?;
                } else {
                    warn!("Coordinator cluster not found, falling back to single-node cluster");
                    self.initialize_single_node_cluster(InitiatorReason::DiscoveryTimeout)
                        .await?;
                }
            }
        } else {
            // Fallback
            self.initialize_single_node_cluster(InitiatorReason::DiscoveryTimeout)
                .await?;
        }

        Ok(())
    }

    /// Initialize as a single-node cluster
    async fn initialize_single_node_cluster(&self, reason: InitiatorReason) -> ConsensusResult<()> {
        self.network_manager
            .initialize_single_node_cluster(self.node_id.clone(), reason)
            .await
    }

    /// Initialize a multi-node cluster by starting as single-node and expecting others to join
    async fn initialize_multi_node_cluster(
        &self,
        peers: Vec<crate::types::Node>,
    ) -> ConsensusResult<()> {
        self.network_manager
            .initialize_multi_node_cluster(self.node_id.clone(), peers)
            .await
    }

    /// Join an existing cluster via Raft membership change (learner -> voter)
    async fn join_existing_cluster_via_raft(
        &self,
        existing_cluster: &ClusterDiscoveryResponse,
    ) -> ConsensusResult<()> {
        self.network_manager
            .join_existing_cluster_via_raft(self.node_id.clone(), existing_cluster)
            .await
    }

    /// Shutdown the consensus system
    pub async fn shutdown(&self) -> ConsensusResult<()> {
        info!("Shutting down consensus system");

        // Comprehensive shutdown of NetworkManager (includes Raft, transport, topology)
        if let Err(e) = self.network_manager.shutdown_all().await {
            warn!("NetworkManager shutdown error: {}", e);
        }

        // Wait for background tasks
        let handles = {
            let mut task_handles = self.task_handles.lock().unwrap();
            std::mem::take(&mut *task_handles)
        };

        for handle in handles {
            if !handle.is_finished() {
                handle.abort();
            }
        }

        info!("Consensus system shutdown complete");
        Ok(())
    }

    /// Get node ID
    pub fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Get the stream store
    pub fn stream_store(&self) -> &Arc<StreamStore> {
        &self.stream_store
    }
}

#[async_trait]
impl<G, A> Bootable for Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    fn bootable_name(&self) -> &'static str {
        "consensus-system"
    }

    async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting consensus system via Bootable trait for node {}",
            self.node_id
        );

        self.start()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Shutting down consensus system via Bootable trait for node {}",
            self.node_id
        );

        self.shutdown()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    async fn wait(&self) {
        // For now, just return immediately
        // In a full implementation, this would wait for background tasks to complete
        // but JoinHandle doesn't implement Clone, so we can't easily access them here
    }
}

impl<G, A> std::fmt::Debug for Consensus<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug,
    A: Attestor + Send + Sync + 'static + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Consensus")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::MessagingOperation;

    use ed25519_dalek::SigningKey;
    use openraft::Config as RaftConfig;
    use proven_attestation_mock::MockAttestor;
    use proven_governance::{GovernanceNode, Version};
    use proven_governance_mock::MockGovernance;
    use rand::rngs::OsRng;
    use std::collections::HashSet;
    use tracing_test::traced_test;

    // Helper to create a simple single-node governance for testing
    fn create_test_governance(port: u16, signing_key: &SigningKey) -> Arc<MockGovernance> {
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();

        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let topology_node = GovernanceNode {
            availability_zone: "test-az".to_string(),
            origin: format!("127.0.0.1:{port}"),
            public_key: signing_key.verifying_key(),
            region: "test-region".to_string(),
            specializations: HashSet::new(),
        };

        Arc::new(MockGovernance::new(
            vec![topology_node],
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ))
    }

    async fn create_test_consensus(port: u16) -> Consensus<MockGovernance, MockAttestor> {
        let signing_key = SigningKey::generate(&mut OsRng);
        let governance = create_test_governance(port, &signing_key);
        let attestor = Arc::new(MockAttestor::new());

        let config = ConsensusConfig {
            governance: governance.clone(),
            attestor: attestor.clone(),
            signing_key: signing_key.clone(),
            raft_config: RaftConfig::default(),
            transport_config: crate::transport::TransportConfig::Tcp {
                listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
            },
            storage_config: crate::config::StorageConfig::Memory,
            cluster_discovery_timeout: None,
        };

        Consensus::new(config).await.unwrap()
    }

    fn next_port() -> u16 {
        proven_util::port_allocator::allocate_port()
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consensus_creation() {
        let consensus = create_test_consensus(next_port()).await;

        // Verify basic properties
        assert!(!consensus.node_id().to_string().is_empty());

        println!("✅ Consensus creation test passed");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consensus_lifecycle() {
        let consensus = create_test_consensus(next_port()).await;
        let node_id = consensus.node_id().to_string();

        println!("Testing consensus lifecycle for node: {}", &node_id[..8]);

        // Test start - this should now work since we fixed the compilation
        // Note: We're not calling start() since it's not implemented yet
        // but we can test basic functionality

        // Test that we can access the stream store
        let stream_store = consensus.stream_store();
        let last_seq = stream_store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 0); // Should be 0 for a new stream

        let is_leader = consensus.is_leader();
        println!("Is leader: {}", is_leader);

        // Test shutdown
        let shutdown_result = consensus.shutdown().await;
        assert!(
            shutdown_result.is_ok(),
            "Consensus shutdown should succeed: {shutdown_result:?}"
        );

        println!(
            "✅ Consensus lifecycle test passed for node: {}",
            &node_id[..8]
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consensus_messaging() {
        let consensus = create_test_consensus(next_port()).await;

        // Test cluster discovery (should work even without full cluster)
        let discovery_responses = consensus.discover_existing_clusters().await;
        assert!(
            discovery_responses.is_ok(),
            "Discovery should succeed: {discovery_responses:?}"
        );

        let responses = discovery_responses.unwrap();
        println!("Discovery responses: {}", responses.len());

        // Test stream store operations
        let stream_store = consensus.stream_store();

        // Test adding some data to the stream store directly
        let test_data = bytes::Bytes::from("test message");
        let result = stream_store
            .apply_operation(
                &MessagingOperation::PublishToStream {
                    stream: "test-stream".to_string(),
                    data: test_data.clone(),
                },
                1,
            )
            .await;

        assert!(result.success, "Stream operation should succeed");
        assert_eq!(result.sequence, 1);

        // Test retrieving the message
        let retrieved = stream_store.get_message("test-stream", 1).await;
        assert_eq!(retrieved, Some(test_data));

        // Test last sequence
        let last_seq = stream_store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 1);

        println!("✅ Consensus messaging test passed");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_leader_election_single_node() {
        let consensus = create_test_consensus(next_port()).await;
        let node_id = consensus.node_id().to_string();

        println!(
            "Testing single-node leader election for node: {}",
            &node_id[..8]
        );

        // Initially, the node should not be a leader (not initialized)
        assert!(!consensus.is_leader());
        // Raft always has a term (starts at 0), so we check it's 0 before initialization
        assert_eq!(consensus.current_term(), Some(0));
        assert_eq!(consensus.current_leader().await, None);

        // Start the consensus system - should automatically become leader (single node)
        let start_result = consensus.start().await;
        assert!(
            start_result.is_ok(),
            "Start should succeed: {start_result:?}"
        );

        // Wait for the cluster to be ready
        consensus
            .wait_for_leader(Some(Duration::from_secs(10)))
            .await
            .unwrap();

        // After initialization, this node should be the leader
        println!("Checking leadership status...");
        println!("Current term: {:?}", consensus.current_term());
        println!("Current leader: {:?}", consensus.current_leader().await);
        println!("Is leader: {}", consensus.is_leader());
        println!("Cluster size: {:?}", consensus.cluster_size());

        // In a single-node cluster, this node should have a term > 0 after initialization
        let current_term = consensus.current_term().unwrap_or(0);
        assert!(
            current_term > 0,
            "Term should be > 0 after initialization, got {}",
            current_term
        );
        assert_eq!(consensus.cluster_size(), Some(1));

        // The node should be the leader after waiting for cluster to be ready
        assert!(
            consensus.is_leader(),
            "Node should be leader after cluster is ready"
        );
        assert_eq!(consensus.current_leader().await, Some(node_id.clone()));
        println!("✅ Node {} successfully became leader", &node_id[..8]);

        // Test that we can submit a proposal through Raft
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream {
                stream: "leader-test".to_string(),
                data: Bytes::from("leader message"),
            },
        };

        // This will test if consensus is properly initialized and can accept proposals
        let proposal_result = consensus.submit_request(request).await;
        assert!(
            proposal_result.is_ok(),
            "Proposal should succeed after cluster is ready"
        );

        // Shutdown
        let shutdown_result = consensus.shutdown().await;
        assert!(
            shutdown_result.is_ok(),
            "Shutdown should succeed: {shutdown_result:?}"
        );

        println!("✅ Single-node leader election test completed");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_leader_election_workflow() {
        // Test that demonstrates leader election workflow works correctly
        let consensus = create_test_consensus(next_port()).await;
        let node_id = consensus.node_id().to_string();

        println!(
            "Testing leader election workflow for node: {}",
            &node_id[..8]
        );

        // Test 1: Node starts without being a leader
        assert!(!consensus.is_leader());
        assert_eq!(consensus.current_term(), Some(0));

        // Test 2: Start consensus (single node should become leader)
        let start_result = consensus.start().await;
        assert!(start_result.is_ok());

        // Wait for the cluster to be ready
        consensus
            .wait_for_leader(Some(Duration::from_secs(10)))
            .await
            .unwrap();

        // Test 3: Verify leadership is established
        let final_term = consensus.current_term().unwrap_or(0);
        assert!(
            final_term > 0,
            "Term should advance after leadership election"
        );

        let leader_id = consensus.current_leader().await;
        assert_eq!(
            leader_id,
            Some(node_id.clone()),
            "Node should be the leader"
        );

        assert!(consensus.is_leader(), "Node should report itself as leader");

        // Test 4: Test that leader can process requests
        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream {
                stream: "leadership-test".to_string(),
                data: Bytes::from("leader can process this"),
            },
        };

        let result = consensus.submit_request(request).await;
        assert!(result.is_ok(), "Leader should be able to process requests");

        // Test 5: Verify state machine received the request
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream_store = consensus.stream_store();
        let last_seq = stream_store.last_sequence("leadership-test").await;
        assert!(
            last_seq > 0,
            "Request should have been processed by state machine"
        );

        // Shutdown
        let shutdown_result = consensus.shutdown().await;
        assert!(shutdown_result.is_ok());

        println!("✅ Leader election workflow test completed successfully");
        println!("   - Single node correctly became leader");
        println!("   - Term advanced from 0 to {}", final_term);
        println!("   - Leader processed requests successfully");
        println!("   - State machine applied the requests");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_multi_node_cluster_formation() {
        println!("🧪 Testing multi-node cluster formation with shared governance");

        let num_nodes = 3;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys for all nodes
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance
        for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
            println!(
                "✅ Created node {} listening on port {} with shared governance",
                i, port
            );
        }

        // Start all nodes simultaneously to allow proper cluster formation
        println!("🚀 Starting all nodes to enable cluster formation...");

        // Start all nodes sequentially but quickly - they all need to be listening before discovery
        for (i, node) in nodes.iter().enumerate() {
            println!("Starting node {} ({})", i, &node.node_id());
            let start_result = node.start().await;
            assert!(
                start_result.is_ok(),
                "Node {} start should succeed: {start_result:?}",
                i
            );
            // Very short delay to allow listener to start
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        println!("✅ All nodes started and listening - networks ready for discovery");

        // Give extra time for all listeners to be fully ready and discovery to happen
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Give time for cluster formation and leader election
        println!("⏳ Waiting for cluster formation and leader election...");
        tokio::time::sleep(Duration::from_secs(8)).await;

        // Collect cluster state from all nodes
        let mut leader_count = 0;
        let mut cluster_leaders = Vec::new();
        let mut cluster_sizes = Vec::new();
        let mut cluster_terms = Vec::new();

        println!("📊 Analyzing cluster state:");
        for (i, node) in nodes.iter().enumerate() {
            let is_leader = node.is_leader();
            let current_leader = node.current_leader().await;
            let cluster_size = node.cluster_size();
            let current_term = node.current_term();

            println!(
                "   Node {} - Leader: {}, Current Leader: {:?}, Term: {:?}, Cluster Size: {:?}",
                i,
                is_leader,
                current_leader.as_ref().map(|id| &id[..8]),
                current_term,
                cluster_size
            );

            if is_leader {
                leader_count += 1;
            }

            cluster_leaders.push(current_leader);
            cluster_sizes.push(cluster_size);
            cluster_terms.push(current_term);
        }

        // Assert correct cluster formation
        println!("🔍 Verifying correct cluster formation...");

        // 1. There should be exactly 1 leader across all nodes
        assert_eq!(
            leader_count, 1,
            "Expected exactly 1 leader, found {}",
            leader_count
        );
        println!("✅ Exactly 1 leader found");

        // 2. All nodes should report the same cluster size of 3
        let expected_cluster_size = Some(num_nodes);
        for (i, &cluster_size) in cluster_sizes.iter().enumerate() {
            assert_eq!(
                cluster_size, expected_cluster_size,
                "Node {} reports cluster size {:?}, expected {:?}",
                i, cluster_size, expected_cluster_size
            );
        }
        println!("✅ All nodes report cluster size of {}", num_nodes);

        // 3. All nodes should agree on who the leader is
        let first_leader = &cluster_leaders[0];
        assert!(first_leader.is_some(), "No leader found");
        for (i, leader) in cluster_leaders.iter().enumerate() {
            assert_eq!(
                leader,
                first_leader,
                "Node {} reports leader {:?}, expected {:?}",
                i,
                leader.as_ref().map(|s| &s[..8]),
                first_leader.as_ref().map(|s| &s[..8])
            );
        }
        println!(
            "✅ All nodes agree on leader: {}",
            &first_leader.as_ref().unwrap()[..8]
        );

        // 4. All nodes should have the same term (indicating same cluster)
        let first_term = cluster_terms[0];
        assert!(
            first_term.is_some() && first_term.unwrap() > 0,
            "Invalid term"
        );
        for (i, &term) in cluster_terms.iter().enumerate() {
            assert_eq!(
                term, first_term,
                "Node {} reports term {:?}, expected {:?}",
                i, term, first_term
            );
        }
        println!("✅ All nodes agree on term: {}", first_term.unwrap());

        println!(
            "🎉 SUCCESS: Proper single-cluster formation with {} members and 1 leader!",
            num_nodes
        );

        // Test cluster functionality - only the leader should accept writes
        println!("🔍 Testing cluster functionality...");
        let leader_node = nodes
            .iter()
            .find(|node| node.is_leader())
            .expect("Should have exactly one leader");

        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream {
                stream: "cluster-test".to_string(),
                data: Bytes::from("test message from cluster"),
            },
        };

        match leader_node.submit_request(request).await {
            Ok(_) => {
                println!("✅ Leader successfully processed operation");
            }
            Err(e) => {
                panic!("Leader failed to process operation: {}", e);
            }
        }

        println!("✅ Cluster functionality verified");

        // Graceful shutdown
        println!("🛑 Shutting down all nodes...");
        for (i, node) in nodes.iter().enumerate() {
            let shutdown_result = node.shutdown().await;
            assert!(
                shutdown_result.is_ok(),
                "Node {} shutdown should succeed",
                i
            );
            println!("✅ Node {} shutdown successfully", i);
        }

        println!("🎯 Multi-node cluster formation test completed successfully!");
        println!(
            "🎉 Verified: Single cluster with {} members and 1 leader",
            num_nodes
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn test_simultaneous_node_discovery() {
        println!("🧪 Testing simultaneous node discovery");

        let num_nodes = 2; // Start with just 2 nodes for simplicity
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys for all nodes
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance
        for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
            println!(
                "✅ Created node {} listening on port {} with shared governance",
                i, port
            );
        }

        // Start ALL nodes simultaneously
        println!("🚀 Starting all nodes simultaneously...");
        let start_futures: Vec<_> = nodes.iter().map(|node| node.start()).collect();

        // Use try_join_all to start all nodes at the same time
        let start_results = futures::future::try_join_all(start_futures).await;
        assert!(start_results.is_ok(), "All nodes should start successfully");

        println!("✅ All nodes started simultaneously");

        // Give time for cluster discovery
        println!("⏳ Waiting for cluster discovery...");
        tokio::time::sleep(Duration::from_secs(35)).await; // Give plenty of time

        // Check if any discovery responses were received
        println!("📊 Checking discovery results:");
        for (i, node) in nodes.iter().enumerate() {
            let cluster_state = node.cluster_state().await;
            let is_leader = node.is_leader();
            let current_leader = node.current_leader().await;

            println!(
                "   Node {} - State: {:?}, Leader: {}, Current Leader: {:?}",
                i,
                cluster_state,
                is_leader,
                current_leader.as_ref().map(|id| &id[..8])
            );
        }

        // Graceful shutdown
        println!("🛑 Shutting down all nodes...");
        for (i, node) in nodes.iter().enumerate() {
            let shutdown_result = node.shutdown().await;
            assert!(
                shutdown_result.is_ok(),
                "Node {} shutdown should succeed",
                i
            );
            println!("✅ Node {} shutdown successfully", i);
        }

        println!("🎯 Simultaneous discovery test completed");
    }

    // Helper function to create shared governance for multiple nodes
    #[allow(dead_code)]
    fn create_shared_governance(ports: &[u16]) -> Arc<MockGovernance> {
        let attestor = MockAttestor::new();
        let actual_pcrs = attestor.pcrs_sync();
        let test_version = Version {
            ne_pcr0: actual_pcrs.pcr0,
            ne_pcr1: actual_pcrs.pcr1,
            ne_pcr2: actual_pcrs.pcr2,
        };

        let mut topology_nodes = Vec::new();
        for port in ports {
            let signing_key = SigningKey::generate(&mut OsRng);
            let topology_node = GovernanceNode {
                availability_zone: "test-az".to_string(),
                origin: format!("http://127.0.0.1:{}", port),
                public_key: signing_key.verifying_key(),
                region: "test-region".to_string(),
                specializations: HashSet::new(),
            };
            topology_nodes.push(topology_node);
        }

        Arc::new(MockGovernance::new(
            topology_nodes,
            vec![test_version],
            "http://localhost:3200".to_string(),
            vec![],
        ))
    }

    #[tokio::test]
    #[traced_test]
    async fn test_consensus_transport_types() {
        // Test TCP transport creation
        let tcp_consensus = create_test_consensus(next_port()).await;
        assert!(!tcp_consensus.supports_http_integration());

        // Test WebSocket transport creation
        let signing_key = SigningKey::generate(&mut OsRng);
        let governance = create_test_governance(next_port(), &signing_key);
        let attestor = Arc::new(MockAttestor::new());

        let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");

        let config = ConsensusConfig {
            governance: governance.clone(),
            attestor: attestor.clone(),
            signing_key: signing_key.clone(),
            raft_config: RaftConfig::default(),
            transport_config: crate::transport::TransportConfig::WebSocket,
            storage_config: crate::config::StorageConfig::RocksDB {
                path: temp_dir.path().to_path_buf(),
            },
            cluster_discovery_timeout: None,
        };

        let ws_consensus = Consensus::new(config).await.unwrap();

        assert!(ws_consensus.supports_http_integration());

        println!("✅ Transport types test passed");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_stream_operations() {
        let consensus = create_test_consensus(next_port()).await;

        consensus.start().await.unwrap();

        // Wait for the cluster to be ready
        consensus
            .wait_for_leader(Some(Duration::from_secs(10)))
            .await
            .unwrap();

        // Test that we can access stream store directly
        let stream_store = consensus.stream_store();
        let last_seq = stream_store.last_sequence("test-stream").await;
        assert_eq!(last_seq, 0); // Should be 0 for a new stream

        // Test get_message (should return None for non-existent message)
        let result = consensus.get_message("test-stream", 1).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);

        // Test last_sequence
        let result = consensus.last_sequence("test-stream").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);

        // Test route_subject (should return empty set for non-existent subject)
        let routes = consensus.route_subject("test.subject").await;
        assert!(routes.is_empty());

        // Test placeholder methods (these return None/empty until fully implemented)
        let stream_subjects = consensus.get_stream_subjects("test-stream").await;
        assert_eq!(stream_subjects, None);

        let subject_streams = consensus.get_subject_streams("test.*").await;
        assert_eq!(subject_streams, None);

        let all_subscriptions = consensus.get_all_subscriptions().await;
        assert!(all_subscriptions.is_empty());

        // Test leader status (should be true after cluster is ready)
        let is_leader = consensus.is_leader();
        assert!(is_leader, "Node should be leader after cluster is ready");

        // Test metrics (should be available after cluster is ready)
        let metrics = consensus.metrics();
        assert!(
            metrics.is_some(),
            "Metrics should be available after cluster is ready"
        );

        println!("✅ Stream operations interface test passed");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_unidirectional_connection_basic() {
        println!("🧪 Testing basic unidirectional connection message sending");

        let num_nodes = 2;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys for all nodes
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance
        // Note: The consensus constructor automatically starts the network
        for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
            println!(
                "✅ Created node {} listening on port {} with shared governance",
                i, port
            );
        }

        println!(
            "✅ Created {} nodes (networks already started)",
            nodes.len()
        );

        // Wait a moment for listeners to be ready
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Test sending a message from node 0 to node 1 (this will establish connection on-demand)
        let node_1_id = NodeId::new(signing_keys[1].verifying_key());
        let test_message = crate::network::messages::Message::Application(Box::new(
            crate::network::messages::ApplicationMessage::ClusterDiscovery(
                crate::network::messages::ClusterDiscoveryRequest {
                    requester_id: nodes[0].node_id().clone(),
                },
            ),
        ));

        // Send message which should establish connection on-demand
        let send_result = nodes[0]
            .network_manager
            .send_message(node_1_id.clone(), test_message)
            .await;
        println!("📤 Message send result: {:?}", send_result);

        // Wait a moment for connection to be established
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Check connected peers
        let connected_peers = nodes[0]
            .network_manager
            .get_connected_peers()
            .await
            .unwrap();
        println!("📡 Node 0 connected peers: {:?}", connected_peers);

        // Verify on-demand connection was established
        assert!(
            !connected_peers.is_empty(),
            "Node 0 should have connected peers after sending message"
        );
        assert_eq!(
            connected_peers.len(),
            1,
            "Node 0 should have exactly 1 connected peer"
        );

        println!("✅ Basic unidirectional connection test completed successfully");
        println!("✅ Confirmed on-demand connections work without block_on issues");

        // Cleanup
        for node in nodes {
            let _ = node.shutdown().await;
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_cluster_discovery_with_correlation_ids() {
        println!("🧪 Testing cluster discovery with correlation ID tracking");

        let num_nodes = 2;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys for all nodes
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance
        for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
            println!(
                "✅ Created node {} listening on port {} with shared governance",
                i, port
            );
        }

        println!(
            "✅ Created {} nodes (networks already started)",
            nodes.len()
        );

        // Wait a moment for listeners to be ready
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Test cluster discovery from node 0 (this should use correlation IDs)
        println!("🔍 Starting cluster discovery from node 0...");
        let discovery_result = nodes[0].discover_existing_clusters().await;

        println!("📋 Discovery result: {:?}", discovery_result);

        // Discovery should work even if nodes don't have active clusters yet
        assert!(discovery_result.is_ok(), "Discovery should succeed");
        let responses = discovery_result.unwrap();
        println!("✅ Received {} discovery responses", responses.len());

        // Each node should respond to discovery requests
        assert_eq!(
            responses.len(),
            1,
            "Should receive 1 discovery response from the other node"
        );

        // Verify the response content
        let response = &responses[0];
        assert_eq!(
            response.responder_id,
            NodeId::new(signing_keys[1].verifying_key())
        );
        assert!(!response.has_active_cluster); // Nodes are not in clusters yet

        println!("✅ Cluster discovery with correlation IDs test completed successfully");

        // Cleanup
        for node in nodes {
            let _ = node.shutdown().await;
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_end_to_end_discovery_and_join_flow() {
        println!("🧪 Testing complete end-to-end discovery and join flow");

        let num_nodes = 3;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();

        // Allocate ports and generate keys for all nodes
        for i in 0..num_nodes {
            let port = proven_util::port_allocator::allocate_port();
            println!("🔌 Allocated port {} for node {}", port, i);
            ports.push(port);
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 All allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance
        for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::Tcp {
                    listen_addr: format!("127.0.0.1:{port}").parse().unwrap(),
                },
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
            println!("✅ Created node {} on port {}", i, port);
        }

        println!("\n🎬 Starting end-to-end test scenario");

        // Step 1: Start the first node as cluster leader
        println!("\n📍 Step 1: Starting node 0 as initial cluster leader");
        let leader_start_result = nodes[0].start().await;
        assert!(
            leader_start_result.is_ok(),
            "Leader node should start successfully"
        );

        // Wait for leader to be established
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Verify node 0 is the leader
        let mut attempts = 0;
        while !nodes[0].is_leader() && attempts < 10 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            attempts += 1;
        }
        assert!(nodes[0].is_leader(), "Node 0 should become the leader");
        println!("✅ Node 0 is now the cluster leader");

        // Step 2: Test discovery from node 1
        println!("\n📍 Step 2: Testing cluster discovery from node 1");
        let discovery_results = nodes[1].discover_existing_clusters().await.unwrap();
        println!(
            "🔍 Discovery found {} active clusters",
            discovery_results.len()
        );

        // Should find the leader's cluster
        assert!(
            !discovery_results.is_empty(),
            "Should discover at least one cluster"
        );

        let leader_cluster = discovery_results
            .iter()
            .find(|r| r.has_active_cluster)
            .expect("Should find an active cluster from the leader");

        assert!(
            leader_cluster.has_active_cluster,
            "Leader should report active cluster"
        );
        assert_eq!(
            leader_cluster.responder_id,
            NodeId::new(signing_keys[0].verifying_key()),
            "Should discover leader cluster from node 0"
        );
        println!("✅ Successfully discovered leader's cluster");

        // Step 3: Test joining the cluster
        println!("\n📍 Step 3: Node 1 joining the cluster via NetworkManager");

        // Use NetworkManager's join method directly
        let join_result = nodes[1]
            .network_manager
            .join_existing_cluster_via_raft(
                NodeId::new(signing_keys[1].verifying_key()),
                leader_cluster,
            )
            .await;

        assert!(
            join_result.is_ok(),
            "Node 1 should successfully join the cluster: {:?}",
            join_result
        );
        println!("✅ Node 1 successfully joined the cluster");

        // Step 4: Verify cluster membership
        println!("\n📍 Step 4: Verifying cluster membership");

        // Wait for membership changes to propagate
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Check cluster state on both nodes
        let leader_cluster_size = nodes[0].cluster_size();
        let joiner_cluster_size = nodes[1].cluster_size();

        println!("Leader reports cluster size: {:?}", leader_cluster_size);
        println!("Joiner reports cluster size: {:?}", joiner_cluster_size);

        // Both nodes should see the same cluster size
        assert_eq!(
            leader_cluster_size,
            Some(2),
            "Leader should see cluster size of 2"
        );
        assert_eq!(
            joiner_cluster_size,
            Some(2),
            "Joiner should see cluster size of 2"
        );

        // Check that both nodes agree on the leader
        let leader_id_from_leader = nodes[0].current_leader().await;
        let leader_id_from_joiner = nodes[1].current_leader().await;
        assert_eq!(
            leader_id_from_leader, leader_id_from_joiner,
            "Both nodes should agree on the leader"
        );
        println!("✅ Both nodes agree on cluster membership");

        // Step 5: Test a third node joining
        println!("\n📍 Step 5: Testing third node joining");

        // Discover from node 2
        let discovery_results_2 = nodes[2].discover_existing_clusters().await.unwrap();
        assert!(
            !discovery_results_2.is_empty(),
            "Node 2 should discover the existing cluster"
        );

        let cluster_info = discovery_results_2
            .iter()
            .find(|r| r.has_active_cluster)
            .expect("Should find the existing cluster");

        // Join the cluster
        let join_result_2 = nodes[2]
            .network_manager
            .join_existing_cluster_via_raft(
                NodeId::new(signing_keys[2].verifying_key()),
                cluster_info,
            )
            .await;

        assert!(
            join_result_2.is_ok(),
            "Node 2 should successfully join the cluster: {:?}",
            join_result_2
        );
        println!("✅ Node 2 successfully joined the cluster");

        // Step 6: Final verification
        println!("\n📍 Step 6: Final cluster verification");

        // Wait for final membership changes
        tokio::time::sleep(Duration::from_secs(3)).await;

        // All nodes should see cluster size of 3
        for (i, node) in nodes.iter().enumerate() {
            let cluster_size = node.cluster_size();
            assert_eq!(
                cluster_size,
                Some(3),
                "Node {} should see cluster size of 3, got {:?}",
                i,
                cluster_size
            );
            println!("✅ Node {} reports cluster size: {:?}", i, cluster_size);
        }

        // Test cluster functionality - submit a request through the leader
        println!("\n📍 Step 7: Testing cluster functionality");
        let test_request = MessagingRequest {
            operation: MessagingOperation::PublishToStream {
                stream: "test-cluster-stream".to_string(),
                data: Bytes::from("cluster is working!"),
            },
        };

        let submit_result = nodes[0].submit_request(test_request).await;
        assert!(
            submit_result.is_ok(),
            "Should be able to submit request to cluster"
        );
        println!("✅ Successfully submitted request to cluster");

        // Verify the message was applied to the state machine
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream_store = nodes[0].stream_store();
        let last_seq = stream_store.last_sequence("test-cluster-stream").await;
        assert!(
            last_seq > 0,
            "Message should have been applied to state machine"
        );
        println!(
            "✅ Message applied to state machine with sequence {}",
            last_seq
        );

        // Graceful shutdown
        println!("\n🛑 Shutting down all nodes...");
        for (i, node) in nodes.iter().enumerate() {
            let shutdown_result = node.shutdown().await;
            assert!(
                shutdown_result.is_ok(),
                "Node {} should shutdown cleanly",
                i
            );
            println!("✅ Node {} shutdown successfully", i);
        }

        println!("\n🎉 END-TO-END TEST COMPLETED SUCCESSFULLY!");
        println!("✅ Discovery: Node 1 found leader's cluster");
        println!("✅ Join: Node 1 successfully joined via cluster join request");
        println!("✅ Membership: All nodes agree on cluster membership");
        println!("✅ Third Join: Node 2 successfully joined the existing cluster");
        println!("✅ Functionality: Cluster processes requests correctly");
        println!("✅ Final State: 3-node cluster with proper consensus");
    }

    #[tokio::test]
    #[traced_test]
    async fn test_websocket_leader_election() {
        println!("🧪 Testing WebSocket-based leader election");

        let num_nodes = 3;
        let mut nodes = Vec::new();
        let mut ports = Vec::new();
        let mut signing_keys = Vec::new();
        let mut http_servers = Vec::new();

        // Allocate ports and generate keys for all nodes
        for _i in 0..num_nodes {
            ports.push(next_port());
            signing_keys.push(SigningKey::generate(&mut OsRng));
        }

        println!("📋 Allocated ports: {:?}", ports);

        // Create a shared governance that knows about all nodes
        let shared_governance = {
            let attestor = MockAttestor::new();
            let actual_pcrs = attestor.pcrs_sync();
            let test_version = Version {
                ne_pcr0: actual_pcrs.pcr0,
                ne_pcr1: actual_pcrs.pcr1,
                ne_pcr2: actual_pcrs.pcr2,
            };

            let governance = Arc::new(MockGovernance::new(
                vec![], // Start with empty topology
                vec![test_version],
                "http://localhost:3200".to_string(),
                vec![],
            ));

            // Add all nodes to the shared governance
            for (i, (&port, signing_key)) in ports.iter().zip(signing_keys.iter()).enumerate() {
                let node = GovernanceNode {
                    availability_zone: "test-az".to_string(),
                    origin: format!("http://127.0.0.1:{}", port),
                    public_key: signing_key.verifying_key(),
                    region: "test-region".to_string(),
                    specializations: HashSet::new(),
                };

                governance
                    .add_node(node)
                    .expect("Failed to add node to governance");
                println!("✅ Added node {} to shared governance", i);
            }

            governance
        };

        // Create consensus nodes using the shared governance with WebSocket transport
        for signing_key in signing_keys.iter() {
            let attestor = Arc::new(MockAttestor::new());

            let config = ConsensusConfig {
                governance: shared_governance.clone(),
                attestor: attestor.clone(),
                signing_key: signing_key.clone(),
                raft_config: RaftConfig::default(),
                transport_config: crate::transport::TransportConfig::WebSocket,
                storage_config: crate::config::StorageConfig::Memory,
                cluster_discovery_timeout: None,
            };

            let consensus = Consensus::new(config).await.unwrap();
            nodes.push(consensus);
        }

        // Start HTTP servers for each node with WebSocket integration
        println!("🚀 Starting HTTP servers with WebSocket integration...");
        for (i, (node, &port)) in nodes.iter().zip(ports.iter()).enumerate() {
            // Get the WebSocket router from the consensus
            let router = node.create_router().expect("Should create router");

            // Create the HTTP server
            let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
                .await
                .expect("Should bind to port");

            println!("✅ Node {} HTTP server listening on port {}", i, port);

            // Start the HTTP server in the background
            let server_handle = tokio::spawn(async move {
                axum::serve(listener, router)
                    .await
                    .expect("HTTP server should run");
            });

            http_servers.push(server_handle);

            // Short delay to let server start
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        println!("✅ All HTTP servers started with WebSocket endpoints");

        // Start all consensus nodes
        println!("🚀 Starting all consensus nodes...");
        for (i, node) in nodes.iter().enumerate() {
            println!(
                "Starting WebSocket consensus node {} ({})",
                i,
                &node.node_id()
            );
            let start_result = node.start().await;
            assert!(
                start_result.is_ok(),
                "WebSocket node {} start should succeed: {start_result:?}",
                i
            );
            // Short delay to allow listener to start
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        println!("✅ All WebSocket consensus nodes started");

        // Give time for WebSocket connections and cluster formation
        println!("⏳ Waiting for WebSocket cluster formation and leader election...");
        tokio::time::sleep(Duration::from_secs(10)).await;

        // Collect cluster state from all nodes
        let mut leader_count = 0;
        let mut cluster_leaders = Vec::new();
        let mut cluster_sizes = Vec::new();
        let mut cluster_terms = Vec::new();

        println!("📊 Analyzing WebSocket cluster state:");
        for (i, node) in nodes.iter().enumerate() {
            let is_leader = node.is_leader();
            let current_leader = node.current_leader().await;
            let cluster_size = node.cluster_size();
            let current_term = node.current_term();

            println!(
                "   WebSocket Node {} - Leader: {}, Current Leader: {:?}, Term: {:?}, Cluster Size: {:?}",
                i,
                is_leader,
                current_leader.as_ref().map(|id| &id[..8]),
                current_term,
                cluster_size
            );

            if is_leader {
                leader_count += 1;
            }

            cluster_leaders.push(current_leader);
            cluster_sizes.push(cluster_size);
            cluster_terms.push(current_term);
        }

        // Assert correct WebSocket cluster formation
        println!("🔍 Verifying correct WebSocket cluster formation...");

        // 1. There should be exactly 1 leader across all nodes
        assert_eq!(
            leader_count, 1,
            "Expected exactly 1 leader in WebSocket cluster, found {}",
            leader_count
        );
        println!("✅ Exactly 1 leader found in WebSocket cluster");

        // 2. All nodes should report the same cluster size of 3
        let expected_cluster_size = Some(num_nodes);
        for (i, &cluster_size) in cluster_sizes.iter().enumerate() {
            assert_eq!(
                cluster_size, expected_cluster_size,
                "WebSocket Node {} reports cluster size {:?}, expected {:?}",
                i, cluster_size, expected_cluster_size
            );
        }
        println!(
            "✅ All WebSocket nodes report cluster size of {}",
            num_nodes
        );

        // 3. All nodes should agree on who the leader is
        let first_leader = &cluster_leaders[0];
        assert!(
            first_leader.is_some(),
            "No leader found in WebSocket cluster"
        );
        for (i, leader) in cluster_leaders.iter().enumerate() {
            assert_eq!(
                leader,
                first_leader,
                "WebSocket Node {} reports leader {:?}, expected {:?}",
                i,
                leader.as_ref().map(|s| &s[..8]),
                first_leader.as_ref().map(|s| &s[..8])
            );
        }
        println!(
            "✅ All WebSocket nodes agree on leader: {}",
            &first_leader.as_ref().unwrap()[..8]
        );

        // 4. All nodes should have the same term (indicating same cluster)
        let first_term = cluster_terms[0];
        assert!(
            first_term.is_some() && first_term.unwrap() > 0,
            "Invalid term in WebSocket cluster"
        );
        for (i, &term) in cluster_terms.iter().enumerate() {
            assert_eq!(
                term, first_term,
                "WebSocket Node {} reports term {:?}, expected {:?}",
                i, term, first_term
            );
        }
        println!(
            "✅ All WebSocket nodes agree on term: {}",
            first_term.unwrap()
        );

        println!(
            "🎉 SUCCESS: WebSocket cluster formation with {} members and 1 leader!",
            num_nodes
        );

        // Test WebSocket cluster functionality - only the leader should accept writes
        println!("🔍 Testing WebSocket cluster functionality...");
        let leader_node = nodes
            .iter()
            .find(|node| node.is_leader())
            .expect("Should have exactly one leader");

        let request = MessagingRequest {
            operation: MessagingOperation::PublishToStream {
                stream: "websocket-cluster-test".to_string(),
                data: Bytes::from("test message from WebSocket cluster"),
            },
        };

        match leader_node.submit_request(request).await {
            Ok(_) => {
                println!("✅ WebSocket leader successfully processed operation");
            }
            Err(e) => {
                panic!("WebSocket leader failed to process operation: {}", e);
            }
        }

        // Verify the message was applied to the state machine
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream_store = leader_node.stream_store();
        let last_seq = stream_store.last_sequence("websocket-cluster-test").await;
        assert!(
            last_seq > 0,
            "Message should have been applied to WebSocket cluster state machine"
        );
        println!(
            "✅ WebSocket cluster message applied with sequence {}",
            last_seq
        );

        println!("✅ WebSocket cluster functionality verified");

        // Test WebSocket connection status
        println!("🔍 Testing WebSocket connection status...");
        for (i, node) in nodes.iter().enumerate() {
            match node.get_connected_peers().await {
                Ok(peers) => {
                    println!(
                        "✅ WebSocket Node {} has {} connected peers",
                        i,
                        peers.len()
                    );
                    for (peer_id, connected, _) in peers {
                        println!(
                            "  - Peer {}: connected={}",
                            &peer_id.to_hex()[..8],
                            connected
                        );
                    }
                }
                Err(e) => {
                    println!("⚠️  WebSocket Node {} failed to get peer info: {}", i, e);
                }
            }
        }

        // Graceful shutdown
        println!("🛑 Shutting down WebSocket cluster...");
        for (i, node) in nodes.iter().enumerate() {
            let shutdown_result = node.shutdown().await;
            assert!(
                shutdown_result.is_ok(),
                "WebSocket Node {} shutdown should succeed",
                i
            );
            println!("✅ WebSocket Node {} shutdown successfully", i);
        }

        // Shutdown HTTP servers
        println!("🛑 Shutting down HTTP servers...");
        for (i, server_handle) in http_servers.into_iter().enumerate() {
            server_handle.abort();
            println!("✅ HTTP server {} shutdown", i);
        }

        println!("🎯 WebSocket leader election test completed successfully!");
        println!(
            "🎉 Verified: WebSocket cluster with {} members and 1 leader using HTTP integration",
            num_nodes
        );
    }
}
