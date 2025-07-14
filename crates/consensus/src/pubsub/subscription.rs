//! Subscription handling for consensus messaging

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

/// Trait for subscription handlers that can be invoked when messages are published to subjects
#[async_trait]
pub trait SubscriptionInvoker: Send + Sync + std::fmt::Debug {
    /// Invoke the subscription handler with a message
    async fn invoke(
        &self,
        subject: &str,
        message: Bytes,
        metadata: HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Get the subscription ID for this invoker
    fn subscription_id(&self) -> &str;

    /// Get the subject pattern this subscription is interested in
    fn subject_pattern(&self) -> &str;
}

/// Type alias for subscription handler storage
pub type SubscriptionHandlerMap =
    Arc<parking_lot::RwLock<HashMap<String, Vec<Arc<dyn SubscriptionInvoker>>>>>;
