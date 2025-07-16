//! Event router implementation

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

use super::types::*;

/// Event handler trait
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// Handle an event
    async fn handle(&self, envelope: EventEnvelope) -> EventingResult<EventResult>;

    /// Get handler name
    fn name(&self) -> &str;
}

/// Event route definition
#[derive(Clone)]
pub struct EventRoute {
    /// Route pattern
    pub pattern: EventRoutePattern,
    /// Handler name
    pub handler_name: String,
    /// Route priority (higher = higher priority)
    pub priority: i32,
}

/// Event route pattern
#[derive(Clone)]
pub enum EventRoutePattern {
    /// Match by event type
    ByType(EventType),
    /// Match by event type and priority
    ByTypeAndPriority(EventType, EventPriority),
    /// Match all events
    All,
    /// Custom matcher
    Custom(Arc<dyn Fn(&EventEnvelope) -> bool + Send + Sync>),
}

/// Event router
pub struct EventRouter {
    /// Registered handlers
    handlers: Arc<RwLock<HashMap<String, Arc<dyn EventHandler>>>>,
    /// Routing table
    routes: Arc<RwLock<Vec<EventRoute>>>,
    /// Default handler for unmatched events
    default_handler: Option<Arc<dyn EventHandler>>,
}

impl EventRouter {
    /// Create a new event router
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            routes: Arc::new(RwLock::new(Vec::new())),
            default_handler: None,
        }
    }

    /// Create router with default handler
    pub fn with_default_handler(handler: Arc<dyn EventHandler>) -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
            routes: Arc::new(RwLock::new(Vec::new())),
            default_handler: Some(handler),
        }
    }

    /// Register an event handler
    pub async fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn EventHandler>,
    ) -> EventingResult<()> {
        let mut handlers = self.handlers.write().await;
        if handlers.contains_key(&name) {
            return Err(EventError::HandlerError(format!(
                "Handler {name} already registered"
            )));
        }

        handlers.insert(name.clone(), handler);
        debug!("Registered event handler: {}", name);
        Ok(())
    }

    /// Unregister an event handler
    pub async fn unregister_handler(&self, name: &str) -> EventingResult<()> {
        self.handlers.write().await.remove(name);

        // Remove routes for this handler
        let mut routes = self.routes.write().await;
        routes.retain(|r| r.handler_name != name);

        debug!("Unregistered event handler: {}", name);
        Ok(())
    }

    /// Add a route
    pub async fn add_route(&self, route: EventRoute) -> EventingResult<()> {
        let handlers = self.handlers.read().await;
        if !handlers.contains_key(&route.handler_name) {
            return Err(EventError::HandlerError(format!(
                "Handler {} not found",
                route.handler_name
            )));
        }
        drop(handlers);

        let mut routes = self.routes.write().await;
        routes.push(route);

        // Sort by priority (descending)
        routes.sort_by(|a, b| b.priority.cmp(&a.priority));

        Ok(())
    }

    /// Route an event to appropriate handler
    pub async fn route(&self, envelope: EventEnvelope) -> EventingResult<EventResult> {
        // Find matching route
        let handler_name = {
            let routes = self.routes.read().await;
            routes
                .iter()
                .find(|route| self.matches_pattern(&route.pattern, &envelope))
                .map(|route| route.handler_name.clone())
        };

        // Route to handler if found
        if let Some(handler_name) = handler_name {
            let handlers = self.handlers.read().await;
            if let Some(handler) = handlers.get(&handler_name) {
                let handler = handler.clone();
                drop(handlers);

                debug!(
                    "Routing event {} to handler {}",
                    envelope.metadata.id, handler_name
                );

                return handler.handle(envelope).await;
            }
        }

        // Use default handler if available
        if let Some(handler) = &self.default_handler {
            debug!("Routing event {} to default handler", envelope.metadata.id);
            return handler.handle(envelope).await;
        }

        warn!("No handler found for event {}", envelope.metadata.id);
        Ok(EventResult::Ignored)
    }

    /// Check if event matches pattern
    fn matches_pattern(&self, pattern: &EventRoutePattern, envelope: &EventEnvelope) -> bool {
        match pattern {
            EventRoutePattern::ByType(event_type) => envelope.metadata.event_type == *event_type,
            EventRoutePattern::ByTypeAndPriority(event_type, priority) => {
                envelope.metadata.event_type == *event_type
                    && envelope.metadata.priority == *priority
            }
            EventRoutePattern::All => true,
            EventRoutePattern::Custom(matcher) => matcher(envelope),
        }
    }

    /// Get registered handler names
    pub async fn get_handlers(&self) -> Vec<String> {
        self.handlers.read().await.keys().cloned().collect()
    }

    /// Get routes
    pub async fn get_routes(&self) -> Vec<EventRoute> {
        self.routes.read().await.clone()
    }
}

/// Default event handler that logs events
pub struct LoggingHandler {
    name: String,
}

impl LoggingHandler {
    /// Create a new logging handler
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[async_trait]
impl EventHandler for LoggingHandler {
    async fn handle(&self, envelope: EventEnvelope) -> EventingResult<EventResult> {
        debug!(
            "Handler {} received event {} of type {:?}",
            self.name, envelope.metadata.id, envelope.metadata.event_type
        );
        Ok(EventResult::Success)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Error handler that always returns errors
pub struct ErrorHandler {
    name: String,
    error_message: String,
}

impl ErrorHandler {
    /// Create a new error handler
    pub fn new(name: String, error_message: String) -> Self {
        Self {
            name,
            error_message,
        }
    }
}

#[async_trait]
impl EventHandler for ErrorHandler {
    async fn handle(&self, envelope: EventEnvelope) -> EventingResult<EventResult> {
        error!(
            "Handler {} failed for event {}: {}",
            self.name, envelope.metadata.id, self.error_message
        );
        Ok(EventResult::Failed(self.error_message.clone()))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_event_routing() {
        let router = EventRouter::new();

        // Register handler
        let handler = Arc::new(LoggingHandler::new("test".to_string()));
        router
            .register_handler("test".to_string(), handler)
            .await
            .unwrap();

        // Add route
        let route = EventRoute {
            pattern: EventRoutePattern::ByType(EventType::Stream),
            handler_name: "test".to_string(),
            priority: 1,
        };
        router.add_route(route).await.unwrap();

        // Create event
        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: EventType::Stream,
                priority: EventPriority::Normal,
                source: "test".to_string(),
                correlation_id: None,
                tags: vec![],
            },
            event: Event::Custom {
                event_type: "test".to_string(),
                payload: serde_json::Value::Null,
            },
        };

        // Route event
        let result = router.route(envelope).await.unwrap();
        assert!(matches!(result, EventResult::Success));
    }

    #[tokio::test]
    async fn test_route_priority() {
        let router = EventRouter::new();

        // Register handlers
        let handler1 = Arc::new(LoggingHandler::new("handler1".to_string()));
        let handler2 = Arc::new(LoggingHandler::new("handler2".to_string()));

        router
            .register_handler("handler1".to_string(), handler1)
            .await
            .unwrap();
        router
            .register_handler("handler2".to_string(), handler2)
            .await
            .unwrap();

        // Add routes with different priorities
        router
            .add_route(EventRoute {
                pattern: EventRoutePattern::All,
                handler_name: "handler1".to_string(),
                priority: 1,
            })
            .await
            .unwrap();

        router
            .add_route(EventRoute {
                pattern: EventRoutePattern::All,
                handler_name: "handler2".to_string(),
                priority: 10, // Higher priority
            })
            .await
            .unwrap();

        // Route should go to handler2 due to higher priority
        let routes = router.get_routes().await;
        assert_eq!(routes[0].handler_name, "handler2");
    }
}
