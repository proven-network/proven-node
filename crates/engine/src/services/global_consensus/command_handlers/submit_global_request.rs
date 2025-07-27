//! Handler for SubmitGlobalRequest command

use async_trait::async_trait;
use proven_attestation::Attestor;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

use crate::consensus::global::{GlobalConsensusLayer, GlobalRequest, GlobalResponse};
use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::events::{Error as EventError, EventMetadata, RequestHandler};
use crate::services::global_consensus::commands::SubmitGlobalRequest;
use proven_network::NetworkManager;
use proven_storage::LogStorage;
use proven_topology::{NodeId, TopologyAdaptor};
use proven_transport::Transport;

/// Handler for SubmitGlobalRequest command
pub struct SubmitGlobalRequestHandler<T, G, L, A>
where
    T: Transport,
    G: TopologyAdaptor,
    L: LogStorage,
    A: Attestor,
{
    consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
    network_manager: Arc<NetworkManager<T, G, A>>,
}

impl<T, G, L, A> SubmitGlobalRequestHandler<T, G, L, A>
where
    T: Transport,
    G: TopologyAdaptor,
    L: LogStorage,
    A: Attestor,
{
    pub fn new(
        consensus_layer: Arc<RwLock<Option<Arc<GlobalConsensusLayer<L>>>>>,
        network_manager: Arc<NetworkManager<T, G, A>>,
    ) -> Self {
        Self {
            consensus_layer,
            network_manager,
        }
    }

    /// Forward a request to a specific node
    async fn forward_to_node(
        &self,
        target_node: NodeId,
        request: GlobalRequest,
    ) -> ConsensusResult<GlobalResponse> {
        use crate::services::global_consensus::messages::{
            GlobalConsensusMessage, GlobalConsensusResponse,
        };

        debug!("Forwarding global request to node {}", target_node);

        let message = GlobalConsensusMessage::Consensus(request);
        let response = self
            .network_manager
            .request_with_timeout(target_node, message, std::time::Duration::from_secs(30))
            .await
            .map_err(|e| Error::with_context(ErrorKind::Network, e.to_string()))?;

        match response {
            GlobalConsensusResponse::Consensus(resp) => Ok(resp),
            _ => Err(Error::with_context(
                ErrorKind::InvalidState,
                "Unexpected response type from forwarded request",
            )),
        }
    }
}

#[async_trait]
impl<T, G, L, A> RequestHandler<SubmitGlobalRequest> for SubmitGlobalRequestHandler<T, G, L, A>
where
    T: Transport + 'static,
    G: TopologyAdaptor + 'static,
    L: LogStorage + 'static,
    A: Attestor + 'static,
{
    async fn handle(
        &self,
        request: SubmitGlobalRequest,
        _metadata: EventMetadata,
    ) -> Result<GlobalResponse, EventError> {
        debug!("SubmitGlobalRequestHandler: Processing global request");

        // Check if consensus layer is initialized
        let consensus_guard = self.consensus_layer.read().await;
        let consensus = consensus_guard
            .as_ref()
            .ok_or_else(|| EventError::Internal("Global consensus not initialized".to_string()))?;

        // Submit to consensus
        match consensus.submit_request(request.request.clone()).await {
            Ok(response) => Ok(response),
            Err(e) if e.is_not_leader() => {
                // We're not the leader - forward to the leader if known
                if let Some(leader) = e.get_leader() {
                    drop(consensus_guard); // Release the lock before network call
                    self.forward_to_node(leader.clone(), request.request)
                        .await
                        .map_err(|e| EventError::Internal(format!("Failed to forward: {e}")))
                } else {
                    // No leader info in error, preserve the not-leader error
                    Err(EventError::Internal(format!("NotLeader: {e}")))
                }
            }
            Err(e) => Err(EventError::Internal(format!(
                "Failed to submit global request: {e}"
            ))),
        }
    }
}
