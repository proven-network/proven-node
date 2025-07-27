//! Network service handler for global consensus

use async_trait::async_trait;
use proven_network::{NetworkResult, Service, ServiceContext};
use proven_storage::LogStorage;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::messages::{
    CheckClusterExistsResponse, GlobalConsensusMessage, GlobalConsensusResponse,
};
use crate::consensus::global::GlobalConsensusLayer;
use crate::consensus::global::raft::GlobalRaftMessageHandler;

/// Type alias for consensus layer
type ConsensusLayer<S> = Arc<RwLock<Option<Arc<GlobalConsensusLayer<S>>>>>;

/// Global consensus service handler
pub struct GlobalConsensusHandler<S>
where
    S: LogStorage + 'static,
{
    consensus_layer: ConsensusLayer<S>,
}

impl<S> GlobalConsensusHandler<S>
where
    S: LogStorage + 'static,
{
    /// Create a new handler
    pub fn new(consensus_layer: ConsensusLayer<S>) -> Self {
        Self { consensus_layer }
    }
}

#[async_trait]
impl<S> Service for GlobalConsensusHandler<S>
where
    S: LogStorage + Send + Sync + 'static,
{
    type Request = GlobalConsensusMessage;

    async fn handle(
        &self,
        message: Self::Request,
        _ctx: ServiceContext,
    ) -> NetworkResult<<Self::Request as proven_network::ServiceMessage>::Response> {
        match message {
            GlobalConsensusMessage::CheckClusterExists(_req) => {
                // Handle cluster exists check without requiring consensus layer
                let layer_guard = self.consensus_layer.read().await;
                let response = if let Some(layer) = layer_guard.as_ref() {
                    // We have a consensus layer, check its state
                    CheckClusterExistsResponse {
                        cluster_exists: true,
                        current_leader: layer.get_leader(),
                        current_term: layer.get_current_term(),
                        members: layer.get_members(),
                    }
                } else {
                    // No consensus layer yet
                    CheckClusterExistsResponse {
                        cluster_exists: false,
                        current_leader: None,
                        current_term: 0,
                        members: vec![],
                    }
                };
                Ok(GlobalConsensusResponse::CheckClusterExists(response))
            }
            _ => {
                // All other messages require consensus layer
                let layer_guard = self.consensus_layer.read().await;
                let layer = layer_guard.as_ref().ok_or_else(|| {
                    proven_network::NetworkError::ServiceError(
                        "Consensus not initialized".to_string(),
                    )
                })?;

                let handler: &dyn GlobalRaftMessageHandler = layer.as_ref();

                match message {
                    GlobalConsensusMessage::Vote(req) => {
                        let resp = handler.handle_vote(req).await.map_err(|e| {
                            proven_network::NetworkError::ServiceError(e.to_string())
                        })?;
                        Ok(GlobalConsensusResponse::Vote(resp))
                    }
                    GlobalConsensusMessage::AppendEntries(req) => {
                        let resp = handler.handle_append_entries(req).await.map_err(|e| {
                            proven_network::NetworkError::ServiceError(e.to_string())
                        })?;
                        Ok(GlobalConsensusResponse::AppendEntries(resp))
                    }
                    GlobalConsensusMessage::InstallSnapshot(req) => {
                        let resp = handler.handle_install_snapshot(req).await.map_err(|e| {
                            proven_network::NetworkError::ServiceError(e.to_string())
                        })?;
                        Ok(GlobalConsensusResponse::InstallSnapshot(resp))
                    }
                    GlobalConsensusMessage::Consensus(req) => {
                        let resp = layer.submit_request(req).await.map_err(|e| {
                            proven_network::NetworkError::ServiceError(e.to_string())
                        })?;
                        Ok(GlobalConsensusResponse::Consensus(resp))
                    }
                    GlobalConsensusMessage::CheckClusterExists(_) => {
                        unreachable!("Already handled above")
                    }
                }
            }
        }
    }
}
