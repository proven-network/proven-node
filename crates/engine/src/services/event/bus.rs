//! Type-safe event bus implementation

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use super::traits::{
    ClosureHandler, EventHandler, EventPriority, ServiceEvent, TypeErasedHandler,
    TypeErasedHandlerWrapper,
};

/// Type alias for the handler storage
type HandlerMap = HashMap<TypeId, Vec<Arc<dyn TypeErasedHandler>>>;

/// Statistics for the event bus
#[derive(Debug, Clone, Default)]
pub struct EventBusStats {
    pub total_events_published: u64,
    pub total_handlers_invoked: u64,
    pub events_by_type: HashMap<String, u64>,
}

/// Type-safe event bus for inter-service communication
pub struct EventBus {
    handlers: Arc<RwLock<HandlerMap>>,
    stats: Arc<RwLock<EventBusStats>>,
}

impl EventBus {
    /// Create a new event bus
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(EventBusStats::default())),
        }
    }

    /// Subscribe to a specific event type with a handler
    pub async fn subscribe<E, H>(&self, handler: H) -> SubscriptionId
    where
        E: ServiceEvent,
        H: EventHandler<E> + 'static,
    {
        let type_id = TypeId::of::<E>();
        let wrapped = Arc::new(TypeErasedHandlerWrapper::new(handler));

        let mut handlers = self.handlers.write().await;
        let handler_list = handlers.entry(type_id).or_insert_with(Vec::new);
        let subscription_id = SubscriptionId {
            type_id,
            handler_index: handler_list.len(),
        };

        handler_list.push(wrapped as Arc<dyn TypeErasedHandler>);

        debug!(
            "New subscription for event type: {:?}, total handlers: {}",
            std::any::type_name::<E>(),
            handler_list.len()
        );

        subscription_id
    }

    /// Subscribe with a closure
    pub async fn subscribe_fn<E, F>(&self, closure: F) -> SubscriptionId
    where
        E: ServiceEvent,
        F: Fn(E) + Send + Sync + 'static,
    {
        self.subscribe::<E, _>(ClosureHandler::new(closure)).await
    }

    /// Publish an event to all subscribers
    pub async fn publish<E: ServiceEvent>(&self, event: E) {
        let type_id = TypeId::of::<E>();
        let event_name = event.event_name();

        debug!("Publishing event: {}", event_name);

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_events_published += 1;
            *stats
                .events_by_type
                .entry(event_name.to_string())
                .or_insert(0) += 1;
        }

        // Get handlers
        let handlers = {
            let handlers_map = self.handlers.read().await;
            handlers_map.get(&type_id).cloned()
        };

        if let Some(event_handlers) = handlers {
            let handler_count = event_handlers.len();
            debug!("Dispatching {} to {} handlers", event_name, handler_count);

            // Update handler invocation stats
            {
                let mut stats = self.stats.write().await;
                stats.total_handlers_invoked += handler_count as u64;
            }

            // Group handlers by priority
            let mut critical_handlers = Vec::new();
            let mut other_handlers = Vec::new();

            for handler in event_handlers {
                if handler.priority() == EventPriority::Critical {
                    critical_handlers.push(handler);
                } else {
                    other_handlers.push(handler);
                }
            }

            // Handle critical priority handlers synchronously
            for handler in critical_handlers {
                debug!("Handling {} synchronously (Critical priority)", event_name);
                handler.handle_any(Box::new(event.clone())).await;
            }

            // Handle other priority handlers asynchronously
            for handler in other_handlers {
                let event_clone = event.clone();
                let handler = handler.clone();
                let event_name = event_name.to_string();
                let priority = handler.priority();

                debug!(
                    "Handling {} asynchronously (priority: {:?})",
                    event_name, priority
                );

                tokio::spawn(async move {
                    if let Err(e) = tokio::spawn(async move {
                        handler.handle_any(Box::new(event_clone)).await;
                    })
                    .await
                    {
                        error!("Handler panicked while processing {}: {:?}", event_name, e);
                    }
                });
            }
        } else {
            debug!("No handlers registered for event: {}", event_name);
        }
    }

    /// Unsubscribe a handler
    pub async fn unsubscribe(&self, subscription_id: SubscriptionId) -> bool {
        let mut handlers = self.handlers.write().await;

        if let Some(handler_list) = handlers.get_mut(&subscription_id.type_id)
            && subscription_id.handler_index < handler_list.len()
        {
            handler_list.remove(subscription_id.handler_index);

            // If no more handlers for this type, remove the entry
            if handler_list.is_empty() {
                handlers.remove(&subscription_id.type_id);
            }

            return true;
        }

        false
    }

    /// Get current statistics
    pub async fn stats(&self) -> EventBusStats {
        self.stats.read().await.clone()
    }

    /// Get the number of subscribers for a specific event type
    pub async fn subscriber_count<E: ServiceEvent>(&self) -> usize {
        let type_id = TypeId::of::<E>();
        let handlers = self.handlers.read().await;
        handlers.get(&type_id).map(|h| h.len()).unwrap_or(0)
    }
}

/// Subscription identifier for unsubscribing
#[derive(Debug, Clone, Copy)]
pub struct SubscriptionId {
    type_id: TypeId,
    handler_index: usize,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[derive(Debug, Clone)]
    struct TestEvent {
        message: String,
    }

    impl ServiceEvent for TestEvent {
        fn event_name(&self) -> &'static str {
            "TestEvent"
        }
    }

    struct TestHandler {
        received: Arc<RwLock<Vec<String>>>,
    }

    #[async_trait]
    impl EventHandler<TestEvent> for TestHandler {
        async fn handle(&self, event: TestEvent) {
            self.received.write().await.push(event.message);
        }
    }

    #[tokio::test]
    async fn test_event_bus() {
        let bus = EventBus::new();
        let received = Arc::new(RwLock::new(Vec::new()));

        let handler = TestHandler {
            received: received.clone(),
        };

        bus.subscribe::<TestEvent, _>(handler).await;

        bus.publish(TestEvent {
            message: "Hello".to_string(),
        })
        .await;

        // Give async handler time to process
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let messages = received.read().await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0], "Hello");
    }
}
