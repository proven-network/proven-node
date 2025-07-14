//! Ergonomic handler registration with builder pattern

use crate::{
    error::{NetworkError, NetworkResult},
    handler::HandlerRegistry,
    message::{HandledMessage, SerializableMessage},
};
use proven_topology::NodeId;
use std::{future::Future, marker::PhantomData};
use uuid::Uuid;

/// Builder for registering message handlers with less boilerplate
pub struct HandlerBuilder<'a, M, S = ()>
where
    M: HandledMessage + SerializableMessage,
{
    registry: &'a HandlerRegistry,
    state: Option<S>,
    _phantom: PhantomData<M>,
}

impl<'a, M> HandlerBuilder<'a, M, ()>
where
    M: HandledMessage + SerializableMessage + serde::de::DeserializeOwned + 'static,
    M::Response: 'static,
{
    /// Create a new handler builder
    pub(crate) fn new(registry: &'a HandlerRegistry) -> Self {
        Self {
            registry,
            state: None,
            _phantom: PhantomData,
        }
    }

    /// Add state that will be available to the handler
    pub fn with_state<S>(self, state: S) -> HandlerBuilder<'a, M, S>
    where
        S: Clone + Send + Sync + 'static,
    {
        HandlerBuilder {
            registry: self.registry,
            state: Some(state),
            _phantom: PhantomData,
        }
    }

    /// Register a simple async function as a handler
    pub fn async_fn<F, Fut>(self, handler: F) -> NetworkResult<()>
    where
        F: Fn(NodeId, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NetworkResult<M::Response>> + Send + 'static,
    {
        self.registry
            .register(move |sender, msg, _correlation_id| handler(sender, msg))
    }

    /// Register a handler that returns any error type
    pub fn async_fn_with_error<F, Fut, E>(self, handler: F) -> NetworkResult<()>
    where
        F: Fn(NodeId, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M::Response, E>> + Send + 'static,
        E: Into<NetworkError> + 'static,
    {
        self.registry.register(move |sender, msg, _correlation_id| {
            let fut = handler(sender, msg);
            async move { fut.await.map_err(Into::into) }
        })
    }
}

impl<'a, M, S> HandlerBuilder<'a, M, S>
where
    M: HandledMessage + SerializableMessage + serde::de::DeserializeOwned + 'static,
    M::Response: 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Register an async handler that has access to the state
    pub fn async_handler<F, Fut>(self, handler: F) -> NetworkResult<()>
    where
        F: Fn(S, NodeId, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NetworkResult<M::Response>> + Send + 'static,
    {
        let state = self
            .state
            .expect("with_state must be called before async_handler");

        self.registry.register(move |sender, msg, _correlation_id| {
            let state = state.clone();
            handler(state, sender, msg)
        })
    }

    /// Register a handler with state that returns any error type
    pub fn async_handler_with_error<F, Fut, E>(self, handler: F) -> NetworkResult<()>
    where
        F: Fn(S, NodeId, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<M::Response, E>> + Send + 'static,
        E: Into<NetworkError> + 'static,
    {
        let state = self
            .state
            .expect("with_state must be called before async_handler_with_error");

        self.registry.register(move |sender, msg, _correlation_id| {
            let state = state.clone();
            let fut = handler(state, sender, msg);
            async move { fut.await.map_err(Into::into) }
        })
    }

    /// Register a handler that has access to the correlation ID
    pub fn async_handler_with_context<F, Fut>(self, handler: F) -> NetworkResult<()>
    where
        F: Fn(S, HandlerContext, M) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = NetworkResult<M::Response>> + Send + 'static,
    {
        let state = self
            .state
            .expect("with_state must be called before async_handler_with_context");

        self.registry.register(move |sender, msg, correlation_id| {
            let state = state.clone();
            let ctx = HandlerContext {
                sender,
                correlation_id,
            };
            handler(state, ctx, msg)
        })
    }
}

/// Context information available to handlers
#[derive(Debug, Clone)]
pub struct HandlerContext {
    /// The sender of the message
    pub sender: NodeId,
    /// Optional correlation ID for request/response tracking
    pub correlation_id: Option<Uuid>,
}

/// Extension trait to add the handler builder to NetworkManager
pub trait HandlerBuilderExt {
    /// Start building a handler for the given message type
    fn handle<M>(&'_ self) -> HandlerBuilder<'_, M>
    where
        M: HandledMessage + SerializableMessage + serde::de::DeserializeOwned + 'static,
        M::Response: 'static;
}

impl<T, G> HandlerBuilderExt for crate::NetworkManager<T, G>
where
    T: crate::Transport,
    G: proven_governance::Governance,
{
    fn handle<M>(&'_ self) -> HandlerBuilder<'_, M>
    where
        M: HandledMessage + SerializableMessage + serde::de::DeserializeOwned + 'static,
        M::Response: 'static,
    {
        HandlerBuilder::new(self.handler_registry())
    }
}
