//! Type-safe PubSub messaging
//!
//! This module provides strongly-typed wrappers around the PubSub system,
//! allowing users to work with their own types instead of raw bytes.

use super::{
    errors::{ClientError, ClientResult},
    types::MessageType,
};
use crate::pubsub::{PubSubManager, Subscription};
use std::sync::Arc;

use std::{marker::PhantomData, time::Duration};

use bytes::Bytes;
use proven_governance::Governance;
use proven_network::Transport;

/// Type-safe publisher for PubSub messages
///
/// This struct provides methods for publishing typed messages to subjects
/// and making typed request-response calls.
///
/// # Examples
///
/// ```ignore
/// let publisher: TypedPublisher<MyMessage> = client.publisher();
///
/// // Publish a message
/// publisher.publish("events.order", MyMessage { id: 123 }).await?;
///
/// // Make a request
/// let response: MyResponse = publisher
///     .request("service.query", MyRequest { query: "test" })
///     .await?;
/// ```
#[derive(Clone)]
pub struct TypedPublisher<M, T, G>
where
    M: MessageType,
    T: Transport,
    G: Governance,
{
    manager: Arc<PubSubManager<T, G>>,
    _marker: PhantomData<M>,
}

impl<M, T, G> TypedPublisher<M, T, G>
where
    M: MessageType,
    T: Transport,
    G: Governance,
{
    /// Create a new typed publisher
    pub(crate) fn new(manager: Arc<PubSubManager<T, G>>) -> Self {
        Self {
            manager,
            _marker: PhantomData,
        }
    }

    /// Publish a typed message to a subject
    pub async fn publish(&self, subject: &str, message: M) -> ClientResult<()> {
        let bytes: Bytes = message
            .try_into()
            .map_err(|e: M::SerializeError| ClientError::serialization(e))?;

        self.manager
            .publish(subject, bytes)
            .await
            .map_err(ClientError::PubSub)
    }

    /// Make a request and wait for a typed response
    ///
    /// The response type `R` must also implement `MessageType`.
    pub async fn request<R>(&self, subject: &str, message: M) -> ClientResult<R>
    where
        R: MessageType,
    {
        let bytes: Bytes = message
            .try_into()
            .map_err(|e: M::SerializeError| ClientError::serialization(e))?;

        let response_bytes = self
            .manager
            .request(subject, bytes, Duration::from_secs(5))
            .await
            .map_err(ClientError::PubSub)?;

        R::try_from(response_bytes)
            .map_err(|e: R::DeserializeError| ClientError::deserialization(e))
    }

    /// Make a request with a custom timeout
    pub async fn request_with_timeout<R>(
        &self,
        subject: &str,
        message: M,
        timeout: Duration,
    ) -> ClientResult<R>
    where
        R: MessageType,
    {
        let bytes: Bytes = message
            .try_into()
            .map_err(|e: M::SerializeError| ClientError::serialization(e))?;

        let response_bytes = self
            .manager
            .request(subject, bytes, timeout)
            .await
            .map_err(ClientError::PubSub)?;

        R::try_from(response_bytes)
            .map_err(|e: R::DeserializeError| ClientError::deserialization(e))
    }
}

/// Type-safe subscription for PubSub messages
///
/// This struct provides an async iterator interface for receiving
/// typed messages from subscribed subjects.
///
/// # Examples
///
/// ```ignore
/// let mut subscription: TypedSubscription<MyMessage> =
///     client.subscribe("events.*").await?;
///
/// while let Some(result) = subscription.next().await {
///     match result {
///         Ok(msg) => println!("Got message on {}: {:?}", msg.subject, msg.data),
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// ```
pub struct TypedSubscription<M, T, G>
where
    M: MessageType,
    T: Transport,
    G: Governance,
{
    inner: Subscription<T, G>,
    _phantom: PhantomData<M>,
}

impl<M, T, G> TypedSubscription<M, T, G>
where
    M: MessageType,
    T: Transport,
    G: Governance,
{
    /// Create a new typed subscription
    pub(crate) fn new(inner: Subscription<T, G>) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Receive the next typed message
    ///
    /// Returns `None` if the subscription is closed.
    pub async fn next(&mut self) -> Option<ClientResult<TypedMessage<M>>> {
        match self.inner.receiver.recv().await {
            Some((subject, bytes)) => {
                match M::try_from(bytes) {
                    Ok(data) => Some(Ok(TypedMessage {
                        subject,
                        data,
                        // Note: We don't have access to reply_to and publisher here
                        // This would require changes to the underlying Subscription type
                    })),
                    Err(e) => Some(Err(ClientError::deserialization(e))),
                }
            }
            None => None,
        }
    }

    /// Try to receive the next message without blocking
    pub fn try_next(&mut self) -> Option<ClientResult<TypedMessage<M>>> {
        match self.inner.receiver.try_recv() {
            Ok((subject, bytes)) => match M::try_from(bytes) {
                Ok(data) => Some(Ok(TypedMessage { subject, data })),
                Err(e) => Some(Err(ClientError::deserialization(e))),
            },
            Err(_) => None,
        }
    }

    /// Get the subscription ID
    pub fn id(&self) -> &str {
        &self.inner.id
    }

    /// Get the subject pattern
    pub fn subject(&self) -> &str {
        &self.inner.subject
    }
}

/// A typed message received from a subscription
#[derive(Debug, Clone)]
pub struct TypedMessage<T> {
    /// The subject the message was published to
    pub subject: String,
    /// The message data
    pub data: T,
}

/// Request handler for typed request-response patterns
///
/// This trait can be implemented to handle incoming requests.
pub trait RequestHandler<Req, Res>: Send + Sync + 'static
where
    Req: MessageType,
    Res: MessageType,
{
    /// Handle an incoming request
    fn handle(&self, request: Req) -> impl std::future::Future<Output = ClientResult<Res>> + Send;
}

/// A simple function-based request handler
pub struct FnHandler<F, Req, Res>
where
    F: Fn(Req) -> Res + Send + Sync + 'static,
    Req: MessageType,
    Res: MessageType,
{
    handler: F,
    _phantom: PhantomData<(Req, Res)>,
}

impl<F, Req, Res> FnHandler<F, Req, Res>
where
    F: Fn(Req) -> Res + Send + Sync + 'static,
    Req: MessageType,
    Res: MessageType,
{
    /// Create a new function-based handler
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            _phantom: PhantomData,
        }
    }
}

impl<F, Req, Res> RequestHandler<Req, Res> for FnHandler<F, Req, Res>
where
    F: Fn(Req) -> Res + Send + Sync + 'static,
    Req: MessageType,
    Res: MessageType,
{
    async fn handle(&self, request: Req) -> ClientResult<Res> {
        Ok((self.handler)(request))
    }
}
