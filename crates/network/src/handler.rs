//! Message handler registration and routing

use crate::error::{NetworkError, NetworkResult};
use crate::message::{HandledMessage, NetworkEnvelope, NetworkMessage};

use bytes::Bytes;
use dashmap::DashMap;
use proven_logger::{debug, info};
use proven_topology::NodeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

/// Type alias for async handler results
pub type HandlerResult<T> = NetworkResult<T>;

/// Type alias for boxed futures
pub type BoxedFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Handler function signature
pub type HandlerFn<M> = Arc<
    dyn Fn(NodeId, M, Option<Uuid>) -> BoxedFuture<HandlerResult<<M as HandledMessage>::Response>>
        + Send
        + Sync,
>;

/// Registry for message handlers
pub struct HandlerRegistry {
    /// Map of message type to handler
    handlers: Arc<DashMap<&'static str, Arc<dyn MessageHandler>>>,
}

/// Trait for type-erased message handlers
trait MessageHandler: Send + Sync {
    /// Handle a message and return a response
    fn handle_request(
        &self,
        sender: NodeId,
        payload: &[u8],
        correlation_id: Option<Uuid>,
    ) -> BoxedFuture<HandlerResult<Option<Box<dyn NetworkMessage>>>>;
}

/// Concrete implementation of a typed handler
struct TypedHandler<M>
where
    M: HandledMessage,
{
    handler: HandlerFn<M>,
    _phantom: std::marker::PhantomData<M>,
}

impl HandlerRegistry {
    /// Create a new handler registry
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(DashMap::new()),
        }
    }

    /// Register a handler for a specific message type
    pub fn register<M, F, Fut>(&self, handler: F) -> NetworkResult<()>
    where
        M: HandledMessage + serde::de::DeserializeOwned + 'static,
        M::Response: 'static,
        F: Fn(NodeId, M, Option<Uuid>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HandlerResult<M::Response>> + Send + 'static,
    {
        let handler_fn: HandlerFn<M> = Arc::new(move |sender, msg, corr_id| {
            let fut = handler(sender, msg, corr_id);
            Box::pin(fut)
        });

        let typed_handler = TypedHandler {
            handler: handler_fn,
            _phantom: std::marker::PhantomData,
        };

        let boxed_handler: Arc<dyn MessageHandler> = Arc::new(typed_handler);

        // Get message type from a dummy instance
        // This is a limitation we'll need to work around
        let msg_type = std::any::type_name::<M>();

        self.handlers.insert(msg_type, boxed_handler);

        info!("Registered handler for message type: {msg_type}");
        Ok(())
    }

    /// Handle an incoming message envelope
    pub async fn handle_message(
        &self,
        envelope: NetworkEnvelope,
    ) -> HandlerResult<Option<NetworkEnvelope>> {
        let handler = self
            .handlers
            .get(envelope.message_type.as_str())
            .ok_or_else(|| NetworkError::NoHandler(envelope.message_type.clone()))?;

        debug!("Found handler for message type: {}", envelope.message_type);

        match handler
            .handle_request(
                envelope.sender.clone(),
                &envelope.payload,
                envelope.correlation_id,
            )
            .await
        {
            Ok(Some(response)) => {
                // Create response envelope
                let response_bytes = response.serialize()?;
                let response_envelope = if let Some(corr_id) = envelope.correlation_id {
                    NetworkEnvelope::response(
                        envelope.sender,
                        response_bytes,
                        response.message_type().to_string(),
                        corr_id,
                    )
                } else {
                    NetworkEnvelope::new(
                        envelope.sender,
                        response_bytes,
                        response.message_type().to_string(),
                    )
                };
                Ok(Some(response_envelope))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Handle an incoming raw message (bytes + metadata)
    pub async fn handle_raw_message(
        &self,
        sender: NodeId,
        payload: Bytes,
        message_type: String,
        correlation_id: Option<Uuid>,
    ) -> HandlerResult<Option<Bytes>> {
        let handler = self
            .handlers
            .get(message_type.as_str())
            .ok_or_else(|| NetworkError::NoHandler(message_type.clone()))?;

        debug!("Found handler for message type: {message_type}");

        match handler
            .handle_request(sender, &payload, correlation_id)
            .await
        {
            Ok(Some(response)) => {
                // Serialize response to bytes
                Ok(Some(response.serialize()?))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl<M> MessageHandler for TypedHandler<M>
where
    M: HandledMessage + serde::de::DeserializeOwned + 'static,
    M::Response: 'static,
{
    fn handle_request(
        &self,
        sender: NodeId,
        payload: &[u8],
        correlation_id: Option<Uuid>,
    ) -> BoxedFuture<HandlerResult<Option<Box<dyn NetworkMessage>>>> {
        let handler = self.handler.clone();
        let payload = payload.to_vec();

        Box::pin(async move {
            // Deserialize the message
            let message: M = ciborium::de::from_reader(&payload[..])?;

            // Call the handler
            match handler(sender, message, correlation_id).await {
                Ok(response) => Ok(Some(Box::new(response) as Box<dyn NetworkMessage>)),
                Err(e) => Err(e),
            }
        })
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
