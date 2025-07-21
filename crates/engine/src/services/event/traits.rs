//! Core traits for the event system

use async_trait::async_trait;
use std::any::{Any, TypeId};
use std::fmt::Debug;

/// Priority levels for event processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Trait that all service events must implement
pub trait ServiceEvent: Debug + Clone + Send + Sync + 'static {
    /// Human-readable event type name for logging
    fn event_name(&self) -> &'static str;

    /// Convert to Any for type erasure
    fn as_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

/// Event handler trait for specific event types
#[async_trait]
pub trait EventHandler<E: ServiceEvent>: Send + Sync {
    /// Get the priority for handling events
    ///
    /// Default implementation returns Normal priority.
    /// Override this to handle events synchronously (Critical) or with other priorities.
    fn priority(&self) -> EventPriority {
        EventPriority::Normal
    }

    async fn handle(&self, event: E);
}

/// Type-erased event handler for internal use
#[async_trait]
pub(crate) trait TypeErasedHandler: Send + Sync {
    async fn handle_any(&self, event: Box<dyn Any + Send>);
    fn type_id(&self) -> TypeId;
    fn priority(&self) -> EventPriority;
}

/// Wrapper to convert typed handlers to type-erased handlers
pub(crate) struct TypeErasedHandlerWrapper<E, H>
where
    E: ServiceEvent,
    H: EventHandler<E>,
{
    pub handler: H,
    _phantom: std::marker::PhantomData<E>,
}

impl<E, H> TypeErasedHandlerWrapper<E, H>
where
    E: ServiceEvent,
    H: EventHandler<E>,
{
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<E, H> TypeErasedHandler for TypeErasedHandlerWrapper<E, H>
where
    E: ServiceEvent,
    H: EventHandler<E>,
{
    async fn handle_any(&self, event: Box<dyn Any + Send>) {
        if let Ok(event) = event.downcast::<E>() {
            self.handler.handle(*event).await;
        }
    }

    fn type_id(&self) -> TypeId {
        TypeId::of::<E>()
    }

    fn priority(&self) -> EventPriority {
        self.handler.priority()
    }
}

/// Convenience wrapper for closure-based handlers
pub struct ClosureHandler<E, F>
where
    E: ServiceEvent,
    F: Fn(E) + Send + Sync,
{
    closure: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<E, F> ClosureHandler<E, F>
where
    E: ServiceEvent,
    F: Fn(E) + Send + Sync,
{
    pub fn new(closure: F) -> Self {
        Self {
            closure,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<E, F> EventHandler<E> for ClosureHandler<E, F>
where
    E: ServiceEvent,
    F: Fn(E) + Send + Sync,
{
    async fn handle(&self, event: E) {
        (self.closure)(event);
    }
}
