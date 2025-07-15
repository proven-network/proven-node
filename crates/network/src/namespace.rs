//! Namespace-based message handling infrastructure

use crate::error::{NetworkError, NetworkResult};
use crate::handler::HandlerResult;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use dashmap::DashMap;
use proven_topology::NodeId;
use tokio::sync::RwLock;
use tokio::sync::oneshot;
use tracing::debug;
use uuid::Uuid;

/// Trait for types that can provide a message type string
pub trait MessageType {
    /// Get the message type string for this message
    fn message_type(&self) -> &'static str;
}

/// A pending request waiting for a response
#[derive(Debug)]
pub struct PendingRequest {
    /// Channel to send the response on
    pub tx: oneshot::Sender<Bytes>,
    /// When the request was sent
    pub sent_at: Instant,
    /// Optional timeout duration
    pub timeout: Option<Duration>,
}

/// Metrics for a namespace
#[derive(Debug, Default)]
pub struct NamespaceMetrics {
    /// Number of requests sent
    pub request_count: AtomicU64,
    /// Number of responses received
    pub response_count: AtomicU64,
    /// Number of messages handled
    pub message_count: AtomicU64,
    /// Last activity timestamp (as unix timestamp)
    pub last_activity: AtomicU64,
}

impl NamespaceMetrics {
    /// Record a request
    pub fn record_request(&self) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.update_activity();
    }

    /// Record a response
    pub fn record_response(&self) {
        self.response_count.fetch_add(1, Ordering::Relaxed);
        self.update_activity();
    }

    /// Record a message
    pub fn record_message(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.update_activity();
    }

    /// Update last activity timestamp
    fn update_activity(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_activity.store(now, Ordering::Relaxed);
    }
}

use std::future::Future;
use std::pin::Pin;

/// Type-erased handler function
pub type NamespaceHandler = Arc<
    dyn Fn(
            NodeId,
            Bytes,
            &str,
            Option<Uuid>,
        ) -> Pin<Box<dyn Future<Output = HandlerResult<Option<Bytes>>> + Send>>
        + Send
        + Sync,
>;

/// State for a single namespace
#[derive(Default)]
pub struct NamespaceState {
    /// Pending requests waiting for responses
    pub pending_requests: DashMap<Uuid, PendingRequest>,
    /// Handlers for specific message types within this namespace
    pub handlers: DashMap<String, NamespaceHandler>,
    /// Metrics for this namespace
    pub metrics: NamespaceMetrics,
}

impl NamespaceState {
    /// Create a new namespace state
    pub fn new() -> Self {
        Self {
            pending_requests: DashMap::new(),
            handlers: DashMap::new(),
            metrics: NamespaceMetrics::default(),
        }
    }

    /// Clean up old pending requests
    pub fn cleanup_old_requests(&self, max_age: Duration) -> usize {
        let now = Instant::now();
        let mut removed = 0;

        // Collect keys to remove to avoid holding locks
        let keys_to_remove: Vec<Uuid> = self
            .pending_requests
            .iter()
            .filter_map(|entry| {
                let age = now.duration_since(entry.value().sent_at);
                if age > max_age {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        // Remove old requests
        for key in keys_to_remove {
            if self.pending_requests.remove(&key).is_some() {
                removed += 1;
            }
        }

        if removed > 0 {
            debug!("Cleaned up {} old pending requests", removed);
        }

        removed
    }
}

/// Manages all namespaces in the network
#[derive(Default)]
pub struct NamespaceManager {
    /// Map of namespace name to namespace state
    namespaces: Arc<RwLock<HashMap<String, Arc<NamespaceState>>>>,
}

impl NamespaceManager {
    /// Create a new namespace manager
    pub fn new() -> Self {
        Self {
            namespaces: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a namespace
    pub async fn register_namespace(&self, namespace: &str) -> NetworkResult<()> {
        let mut namespaces = self.namespaces.write().await;
        if namespaces.contains_key(namespace) {
            return Err(NetworkError::Internal(format!(
                "Namespace '{namespace}' already registered"
            )));
        }

        namespaces.insert(namespace.to_string(), Arc::new(NamespaceState::new()));
        debug!("Registered namespace: {}", namespace);
        Ok(())
    }

    /// Get or create a namespace
    pub async fn get_or_create_namespace(&self, namespace: &str) -> Arc<NamespaceState> {
        let namespaces = self.namespaces.read().await;
        if let Some(state) = namespaces.get(namespace) {
            return Arc::clone(state);
        }
        drop(namespaces);

        // Create the namespace if it doesn't exist
        let mut namespaces = self.namespaces.write().await;
        namespaces
            .entry(namespace.to_string())
            .or_insert_with(|| {
                debug!("Auto-creating namespace: {}", namespace);
                Arc::new(NamespaceState::new())
            })
            .clone()
    }

    /// Extract namespace from a full message type
    /// Format: "<namespace>.<message_type>"
    pub fn extract_namespace(full_message_type: &str) -> (&str, &str) {
        if let Some(dot_pos) = full_message_type.rfind('.') {
            let namespace = &full_message_type[..dot_pos];
            let message_type = &full_message_type[dot_pos + 1..];
            (namespace, message_type)
        } else {
            // No namespace prefix, use default
            ("default", full_message_type)
        }
    }

    /// Combine namespace and message type
    pub fn combine_message_type(namespace: &str, message_type: &str) -> String {
        format!("{namespace}.{message_type}")
    }

    /// Clean up old requests across all namespaces
    pub async fn cleanup_all_namespaces(&self, max_age: Duration) {
        let namespaces = self.namespaces.read().await;
        for (name, state) in namespaces.iter() {
            let removed = state.cleanup_old_requests(max_age);
            if removed > 0 {
                debug!(
                    "Cleaned up {} old requests in namespace '{}'",
                    removed, name
                );
            }
        }
    }

    /// Get metrics for all namespaces
    pub async fn get_all_metrics(&self) -> HashMap<String, (u64, u64, u64)> {
        let namespaces = self.namespaces.read().await;
        let mut metrics = HashMap::new();

        for (name, state) in namespaces.iter() {
            metrics.insert(
                name.clone(),
                (
                    state.metrics.request_count.load(Ordering::Relaxed),
                    state.metrics.response_count.load(Ordering::Relaxed),
                    state.metrics.message_count.load(Ordering::Relaxed),
                ),
            );
        }

        metrics
    }
}
