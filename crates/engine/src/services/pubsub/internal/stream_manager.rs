//! Stream management component for PubSub service

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use proven_attestation::Attestor;
use proven_network::{NetworkManager, Stream};
use proven_topology::NodeId;
use proven_topology::TopologyAdaptor;
use proven_transport::Transport;
use tracing::{debug, info, warn};

/// Manages publish and control streams for PubSub
#[derive(Clone)]
pub struct StreamManager {
    publish_streams: Arc<RwLock<HashMap<NodeId, Stream>>>,
    control_streams: Arc<RwLock<HashMap<NodeId, Stream>>>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self {
            publish_streams: Arc::new(RwLock::new(HashMap::new())),
            control_streams: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get publish streams
    pub fn get_publish_streams(&self) -> Arc<RwLock<HashMap<NodeId, Stream>>> {
        self.publish_streams.clone()
    }

    /// Get control streams
    pub fn get_control_streams(&self) -> Arc<RwLock<HashMap<NodeId, Stream>>> {
        self.control_streams.clone()
    }

    /// Clear all streams
    pub async fn clear(&self) {
        self.publish_streams.write().await.clear();
        self.control_streams.write().await.clear();
    }

    /// Maintain streams to cluster members
    pub async fn maintain_streams<T, G, A>(
        &self,
        node_id: &NodeId,
        cluster_members: Arc<RwLock<HashSet<NodeId>>>,
        network_manager: Arc<NetworkManager<T, G, A>>,
        interest_propagator: Arc<super::InterestPropagator>,
    ) where
        T: Transport + Send + Sync + 'static,
        G: TopologyAdaptor + Send + Sync + 'static,
        A: Attestor + Send + Sync + 'static,
    {
        loop {
            let members = cluster_members.read().await.clone();

            // Ensure streams exist for all members
            for peer in &members {
                if peer == node_id {
                    continue;
                }

                // Ensure publish stream exists
                if !self.has_publish_stream(peer).await
                    && let Err(e) = self.open_publish_stream(peer, &network_manager).await
                {
                    debug!("Failed to open publish stream to {}: {}", peer, e);
                }

                // Ensure control stream exists
                if !self.has_control_stream(peer).await
                    && let Err(e) = self
                        .open_control_stream(peer, &network_manager, &interest_propagator)
                        .await
                {
                    debug!("Failed to open control stream to {}: {}", peer, e);
                }
            }

            // Remove streams for nodes that are no longer members
            self.retain_streams(&members).await;

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Check if publish stream exists for peer
    async fn has_publish_stream(&self, peer: &NodeId) -> bool {
        self.publish_streams.read().await.contains_key(peer)
    }

    /// Check if control stream exists for peer
    async fn has_control_stream(&self, peer: &NodeId) -> bool {
        self.control_streams.read().await.contains_key(peer)
    }

    /// Open publish stream to peer
    async fn open_publish_stream<T, G, A>(
        &self,
        peer: &NodeId,
        network_manager: &NetworkManager<T, G, A>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Transport,
        G: TopologyAdaptor,
        A: Attestor,
    {
        let mut metadata = HashMap::new();
        metadata.insert("channel".to_string(), "publish".to_string());

        let stream = network_manager
            .open_stream(*peer, "pubsub", metadata)
            .await?;

        debug!("Opened publish stream to {}", peer);
        self.publish_streams.write().await.insert(*peer, stream);
        Ok(())
    }

    /// Open control stream to peer
    async fn open_control_stream<T, G, A>(
        &self,
        peer: &NodeId,
        network_manager: &NetworkManager<T, G, A>,
        interest_propagator: &super::InterestPropagator,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        T: Transport,
        G: TopologyAdaptor,
        A: Attestor,
    {
        let mut metadata = HashMap::new();
        metadata.insert("channel".to_string(), "control".to_string());

        let stream = network_manager
            .open_stream(*peer, "pubsub", metadata)
            .await?;

        debug!("Opened control stream to {}", peer);

        // Send initial interests before storing the stream
        if let Err(e) = interest_propagator.send_interests_to_stream(&stream).await {
            warn!("Failed to send initial interests to {}: {}", peer, e);
        }

        self.control_streams.write().await.insert(*peer, stream);
        Ok(())
    }

    /// Retain only streams for current members
    async fn retain_streams(&self, members: &HashSet<NodeId>) {
        self.publish_streams
            .write()
            .await
            .retain(|node_id, _| members.contains(node_id));
        self.control_streams
            .write()
            .await
            .retain(|node_id, _| members.contains(node_id));
    }
}
