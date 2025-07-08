//! Bridge between PubSub and Streams
//!
//! This module provides the integration between the peer-to-peer PubSub system
//! and the consensus-based Streams, allowing streams to automatically capture
//! messages published via PubSub.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{debug, error, info};

use crate::NodeId;
use crate::global::{
    GlobalManager, GlobalOperation, GlobalRequest, PubSubMessageSource, StreamStore,
};
use crate::subscription::SubscriptionInvoker;
use proven_governance::Governance;

use super::PubSubManager;

/// Handle for a stream subscription
#[derive(Debug)]
pub struct StreamSubscriptionHandle {
    /// Stream name
    #[allow(dead_code)]
    pub stream_name: String,
    /// Subject pattern
    #[allow(dead_code)]
    pub subject_pattern: String,
    /// Subscription ID
    pub subscription_id: String,
}

/// Bridge between PubSub and Streams
pub struct StreamBridge<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// PubSub manager
    #[allow(dead_code)]
    pubsub_manager: Arc<PubSubManager<G, A>>,
    /// Global manager for consensus operations
    global_manager: Arc<GlobalManager<G, A>>,
    /// Stream store for registering handlers
    stream_store: Arc<StreamStore>,
    /// Stream subscriptions: stream_name -> (subject_pattern -> subscription_handle)
    stream_subscriptions: Arc<RwLock<HashMap<String, HashMap<String, StreamSubscriptionHandle>>>>,
    /// Background task handle
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl<G, A> StreamBridge<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new StreamBridge
    pub fn new(
        pubsub_manager: Arc<PubSubManager<G, A>>,
        global_manager: Arc<GlobalManager<G, A>>,
        stream_store: Arc<StreamStore>,
    ) -> Self {
        Self {
            pubsub_manager,
            global_manager,
            stream_store,
            stream_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            task_handle: None,
        }
    }

    /// Subscribe a stream to a subject pattern
    pub async fn subscribe_stream_to_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Subscribing stream '{}' to subject pattern '{}'",
            stream_name, subject_pattern
        );

        // Create subscription handler
        let handler = StreamSubscriptionHandler::new(
            stream_name.to_string(),
            subject_pattern.to_string(),
            self.global_manager.clone(),
        );

        // Register with the stream store's subscription handlers
        self.stream_store
            .register_subscription_handler(Arc::new(handler));

        // Track the subscription
        let handle = StreamSubscriptionHandle {
            stream_name: stream_name.to_string(),
            subject_pattern: subject_pattern.to_string(),
            subscription_id: format!("{}-{}", stream_name, subject_pattern),
        };

        let mut subscriptions = self.stream_subscriptions.write();
        subscriptions
            .entry(stream_name.to_string())
            .or_default()
            .insert(subject_pattern.to_string(), handle);

        Ok(())
    }

    /// Unsubscribe a stream from a subject pattern
    pub async fn unsubscribe_stream_from_subject(
        &self,
        stream_name: &str,
        subject_pattern: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Unsubscribing stream '{}' from subject pattern '{}'",
            stream_name, subject_pattern
        );

        let subscription_id = format!("{}-{}", stream_name, subject_pattern);

        // Unregister from stream store
        self.stream_store
            .unregister_subscription_handler(&subscription_id);

        // Remove from tracking
        let mut subscriptions = self.stream_subscriptions.write();
        if let Some(stream_subs) = subscriptions.get_mut(stream_name) {
            stream_subs.remove(subject_pattern);
            if stream_subs.is_empty() {
                subscriptions.remove(stream_name);
            }
        }

        Ok(())
    }

    /// Get all subscriptions for a stream
    pub fn get_stream_subscriptions(&self, stream_name: &str) -> Vec<String> {
        let subscriptions = self.stream_subscriptions.read();
        subscriptions
            .get(stream_name)
            .map(|subs| subs.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Start the bridge (if needed for background tasks)
    pub fn start(&mut self) {
        // Currently no background tasks needed
        // This is here for future expansion if needed
    }

    /// Shutdown the bridge
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.task_handle.take() {
            handle.abort();
        }

        // Clear all subscriptions
        let subscriptions = self.stream_subscriptions.write();
        for (_, stream_subs) in subscriptions.iter() {
            for handle in stream_subs.values() {
                self.stream_store
                    .unregister_subscription_handler(&handle.subscription_id);
            }
        }
    }
}

/// Subscription handler that bridges PubSub messages to Streams
#[derive(Clone)]
struct StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Stream name
    stream_name: String,
    /// Subject pattern
    subject_pattern: String,
    /// Subscription ID
    subscription_id: String,
    /// Global manager for consensus operations
    global_manager: Arc<GlobalManager<G, A>>,
}

impl<G, A> StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    /// Create a new handler
    fn new(
        stream_name: String,
        subject_pattern: String,
        global_manager: Arc<GlobalManager<G, A>>,
    ) -> Self {
        let subscription_id = format!("{}-{}", stream_name, subject_pattern);
        Self {
            stream_name,
            subject_pattern,
            subscription_id,
            global_manager,
        }
    }
}

impl<G, A> std::fmt::Debug for StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSubscriptionHandler")
            .field("stream_name", &self.stream_name)
            .field("subject_pattern", &self.subject_pattern)
            .field("subscription_id", &self.subscription_id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<G, A> SubscriptionInvoker for StreamSubscriptionHandler<G, A>
where
    G: Governance + Send + Sync + 'static + std::fmt::Debug + Clone,
    A: proven_attestation::Attestor + Send + Sync + 'static + std::fmt::Debug + Clone,
{
    async fn invoke(
        &self,
        subject: &str,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "StreamSubscriptionHandler: Received message for subject '{}' to stream '{}'",
            subject, self.stream_name
        );

        // Extract source node ID from metadata
        let source_node_id = metadata
            .get("source_node")
            .and_then(|s| NodeId::from_hex(s).ok());

        // Create PubSub source
        let source = PubSubMessageSource {
            node_id: source_node_id,
            timestamp_secs: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // Submit to consensus for persistent storage
        let request = GlobalRequest {
            operation: GlobalOperation::PublishFromPubSub {
                stream_name: self.stream_name.clone(),
                subject: subject.to_string(),
                data: message,
                source,
            },
        };

        match self.global_manager.submit_request(request).await {
            Ok(response) => {
                if response.success {
                    debug!(
                        "Successfully stored PubSub message to stream '{}' at sequence {}",
                        self.stream_name, response.sequence
                    );
                } else {
                    error!(
                        "Failed to store PubSub message to stream '{}': {:?}",
                        self.stream_name, response.error
                    );
                }
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to submit PubSub message to consensus for stream '{}': {}",
                    self.stream_name, e
                );
                Err(Box::new(e))
            }
        }
    }

    fn subscription_id(&self) -> &str {
        &self.subscription_id
    }

    fn subject_pattern(&self) -> &str {
        &self.subject_pattern
    }
}

#[cfg(test)]
#[path = "stream_bridge_tests.rs"]
mod tests;
