//! High-performance event bus implementation

use dashmap::DashMap;
use flume::{bounded, unbounded};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

use super::{
    Error, Result,
    metrics::{MetricsCollector, MetricsGuard},
    traits::{Event, EventHandler, Request, RequestHandler, StreamHandler, StreamRequest},
    types::{ChannelConfig, EventMetadata, OverflowPolicy, Priority},
};

type BoxedAny = Box<dyn Any + Send + Sync>;

/// A subscription handle that automatically unsubscribes on drop
pub struct Subscription {
    bus: Arc<EventBus>,
    type_id: TypeId,
    handler_id: usize,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // TODO: Implement unsubscribe
    }
}

/// Builder for configuring the event bus
#[derive(Clone)]
pub struct EventBusBuilder {
    default_channel_config: ChannelConfig,
    per_type_configs: HashMap<TypeId, ChannelConfig>,
    worker_threads: usize,
    enable_metrics: bool,
    enable_persistence: bool,
    shutdown_timeout: Duration,
}

impl Default for EventBusBuilder {
    fn default() -> Self {
        Self {
            default_channel_config: ChannelConfig::default(),
            per_type_configs: HashMap::new(),
            worker_threads: num_cpus::get(),
            enable_metrics: true,
            enable_persistence: false,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl EventBusBuilder {
    /// Create a new event bus builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default channel configuration
    pub fn channel_config(mut self, config: ChannelConfig) -> Self {
        self.default_channel_config = config;
        self
    }

    /// Set channel configuration for a specific type
    pub fn type_config<T: 'static>(mut self, config: ChannelConfig) -> Self {
        self.per_type_configs.insert(TypeId::of::<T>(), config);
        self
    }

    /// Set the number of worker threads per priority level
    pub fn worker_threads(mut self, count: usize) -> Self {
        self.worker_threads = count;
        self
    }

    /// Enable or disable metrics collection
    pub fn enable_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    /// Build the event bus with the configured settings
    pub fn build(self) -> EventBus {
        EventBus::new(self)
    }
}

// Type aliases to reduce complexity
type EventSender = flume::Sender<(BoxedAny, EventMetadata)>;
type RequestSender = flume::Sender<(BoxedAny, EventMetadata, oneshot::Sender<BoxedAny>)>;
type StreamSender = flume::Sender<(BoxedAny, EventMetadata, flume::Sender<BoxedAny>)>;

/// The main event bus
#[derive(Clone)]
pub struct EventBus {
    /// Event subscriptions by type
    event_subs: Arc<DashMap<TypeId, Vec<EventSender>>>,

    /// Request handlers by type  
    request_handlers: Arc<DashMap<TypeId, RequestSender>>,

    /// Stream handlers by type
    stream_handlers: Arc<DashMap<TypeId, StreamSender>>,

    /// Worker tasks for each priority level
    workers: Arc<RwLock<HashMap<Priority, Vec<JoinHandle<()>>>>>,

    /// Metrics collector
    metrics: Arc<MetricsCollector>,

    /// Configuration
    config: EventBusBuilder,

    /// Shutdown flag
    shutdown: Arc<tokio::sync::Notify>,
}

impl EventBus {
    fn new(config: EventBusBuilder) -> Self {
        let metrics = Arc::new(MetricsCollector::new(config.enable_metrics));

        Self {
            event_subs: Arc::new(DashMap::new()),
            request_handlers: Arc::new(DashMap::new()),
            stream_handlers: Arc::new(DashMap::new()),
            workers: Arc::new(RwLock::new(HashMap::new())),
            metrics,
            config,
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Subscribe to events of type E
    pub fn subscribe<E: Event, H: EventHandler<E>>(&self, handler: H) -> flume::Receiver<E> {
        let (_tx, rx) = if let Some(capacity) = self.config.default_channel_config.capacity {
            bounded(capacity)
        } else {
            unbounded()
        };

        // Create a wrapper that converts from BoxedAny back to E
        let (internal_tx, internal_rx) = unbounded::<(BoxedAny, EventMetadata)>();

        self.event_subs
            .entry(TypeId::of::<E>())
            .or_default()
            .push(internal_tx);

        // Spawn a task to handle type conversion and forwarding
        let handler = Arc::new(handler);
        tokio::spawn(async move {
            while let Ok((boxed, metadata)) = internal_rx.recv_async().await {
                if let Ok(event) = boxed.downcast::<E>() {
                    handler.handle(*event, metadata).await;
                }
            }
            handler.shutdown().await;
        });

        rx
    }

    /// Emit an event to all subscribers
    #[instrument(skip(self, event), fields(event_type = E::event_type()))]
    pub fn emit<E: Event>(&self, event: E) {
        let metadata = EventMetadata::new("event_bus").with_priority(E::priority());
        let guard = MetricsGuard::new(&self.metrics, E::event_type());

        // Check for expiry
        if metadata.is_expired() {
            guard.timeout();
            return;
        }

        if let Some(subs) = self.event_subs.get(&TypeId::of::<E>()) {
            let mut sent = false;

            for tx in subs.iter() {
                match self.config.default_channel_config.overflow_policy {
                    OverflowPolicy::Block => {
                        if tx.send((Box::new(event.clone()), metadata.clone())).is_ok() {
                            sent = true;
                        }
                    }
                    OverflowPolicy::DropNewest => {
                        let _ = tx.try_send((Box::new(event.clone()), metadata.clone()));
                        sent = true;
                    }
                    OverflowPolicy::DropOldest => {
                        // Try to send, if full, remove one and retry
                        if tx
                            .try_send((Box::new(event.clone()), metadata.clone()))
                            .is_err()
                        {
                            // For drop oldest, we'd need access to the receiver side
                            // For now, just try to send anyway
                            let _ = tx.try_send((Box::new(event.clone()), metadata.clone()));
                        }
                        sent = true;
                    }
                    OverflowPolicy::Error => {
                        if tx
                            .try_send((Box::new(event.clone()), metadata.clone()))
                            .is_err()
                        {
                            guard.error();
                            return;
                        }
                        sent = true;
                    }
                }
            }

            if sent {
                guard.success();
            } else {
                guard.error();
            }
        } else {
            debug!("No subscribers for event type: {}", E::event_type());
            guard.error();
        }
    }

    /// Register a handler for requests of type R
    pub fn handle_requests<R: Request, H: RequestHandler<R>>(&self, handler: H) -> Result<()> {
        let (tx, rx) = unbounded::<(BoxedAny, EventMetadata, oneshot::Sender<BoxedAny>)>();

        if self
            .request_handlers
            .insert(TypeId::of::<R>(), tx)
            .is_some()
        {
            return Err(Error::InvalidConfig {
                reason: format!("Handler already registered for {}", R::request_type()),
            });
        }

        let handler = Arc::new(handler);
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Ok((boxed, metadata, resp_tx)) = rx.recv_async().await {
                let guard = MetricsGuard::new(&metrics, R::request_type());

                if metadata.is_expired() {
                    let _ =
                        resp_tx.send(Box::new(Err::<R::Response, Error>(Error::DeadlineExceeded)));
                    guard.timeout();
                    continue;
                }

                if let Ok(request) = boxed.downcast::<R>() {
                    match handler.handle(*request, metadata).await {
                        Ok(response) => {
                            let _ = resp_tx.send(Box::new(Ok::<R::Response, Error>(response)));
                            guard.success();
                        }
                        Err(e) => {
                            let _ = resp_tx.send(Box::new(Err::<R::Response, Error>(e)));
                            guard.error();
                        }
                    }
                } else {
                    guard.error();
                }
            }
            handler.shutdown().await;
        });

        Ok(())
    }

    /// Send a request and wait for response
    #[instrument(skip(self, request), fields(request_type = R::request_type()))]
    pub async fn request<R: Request>(&self, request: R) -> Result<R::Response> {
        self.request_with_timeout(request, R::default_timeout())
            .await
    }

    /// Send a request with custom timeout
    pub async fn request_with_timeout<R: Request>(
        &self,
        request: R,
        timeout: Duration,
    ) -> Result<R::Response> {
        let guard = MetricsGuard::new(&self.metrics, R::request_type());

        if let Some(handler_tx) = self.request_handlers.get(&TypeId::of::<R>()) {
            let (resp_tx, resp_rx) = oneshot::channel();
            let metadata = EventMetadata::new("event_bus")
                .with_priority(R::priority())
                .with_timeout(timeout);

            handler_tx
                .send((Box::new(request), metadata, resp_tx))
                .map_err(|_| Error::NoHandler {
                    type_name: R::request_type(),
                })?;

            match tokio::time::timeout(timeout, resp_rx).await {
                Ok(Ok(boxed)) => {
                    if let Ok(result) = boxed.downcast::<Result<R::Response>>() {
                        match *result {
                            Ok(response) => {
                                guard.success();
                                Ok(response)
                            }
                            Err(e) => {
                                guard.error();
                                Err(e)
                            }
                        }
                    } else {
                        guard.error();
                        Err(Error::Internal("Failed to downcast response".into()))
                    }
                }
                Ok(Err(_)) => {
                    guard.error();
                    Err(Error::Internal("Handler dropped".into()))
                }
                Err(_) => {
                    guard.timeout();
                    Err(Error::Timeout { duration: timeout })
                }
            }
        } else {
            guard.error();
            Err(Error::NoHandler {
                type_name: R::request_type(),
            })
        }
    }

    /// Register a handler for stream requests
    pub fn handle_streams<R: StreamRequest, H: StreamHandler<R>>(&self, handler: H) -> Result<()> {
        let (tx, rx) = unbounded::<(BoxedAny, EventMetadata, flume::Sender<BoxedAny>)>();

        if self.stream_handlers.insert(TypeId::of::<R>(), tx).is_some() {
            return Err(Error::InvalidConfig {
                reason: format!(
                    "Stream handler already registered for {}",
                    R::request_type()
                ),
            });
        }

        let handler = Arc::new(handler);
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            while let Ok((boxed, metadata, item_tx)) = rx.recv_async().await {
                let guard = MetricsGuard::new(&metrics, R::request_type());

                if let Ok(request) = boxed.downcast::<R>() {
                    // Create a typed sender that converts R::Item to BoxedAny
                    let (typed_tx, typed_rx) = unbounded::<R::Item>();

                    // Spawn a task to forward typed items to boxed channel
                    tokio::spawn(async move {
                        while let Ok(item) = typed_rx.recv_async().await {
                            if item_tx.send(Box::new(item)).is_err() {
                                break;
                            }
                        }
                    });

                    match handler.handle(*request, metadata, typed_tx).await {
                        Ok(()) => guard.success(),
                        Err(_) => guard.error(),
                    }
                }
            }
            handler.shutdown().await;
        });

        Ok(())
    }

    /// Send a stream request and receive a stream of responses
    pub async fn stream<R: StreamRequest>(&self, request: R) -> Result<flume::Receiver<R::Item>> {
        if let Some(handler_tx) = self.stream_handlers.get(&TypeId::of::<R>()) {
            let (item_tx, item_rx) = unbounded::<BoxedAny>();
            let metadata = EventMetadata::new("event_bus");

            handler_tx
                .send((Box::new(request), metadata, item_tx))
                .map_err(|_| Error::NoHandler {
                    type_name: R::request_type(),
                })?;

            // Create a typed receiver that converts from BoxedAny to R::Item
            let (typed_tx, typed_rx) = unbounded::<R::Item>();

            tokio::spawn(async move {
                while let Ok(boxed) = item_rx.recv_async().await {
                    if let Ok(item) = boxed.downcast::<R::Item>()
                        && typed_tx.send(*item).is_err()
                    {
                        break;
                    }
                }
            });

            Ok(typed_rx)
        } else {
            Err(Error::NoHandler {
                type_name: R::request_type(),
            })
        }
    }

    /// Gracefully shutdown the event bus
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown.notify_waiters();

        // Wait for all workers to finish
        let mut workers = self.workers.write().await;
        for (_, handles) in workers.drain() {
            for handle in handles {
                let _ = tokio::time::timeout(self.config.shutdown_timeout, handle).await;
            }
        }

        Ok(())
    }

    /// Get metrics summary
    pub fn metrics_summary(&self) -> HashMap<&'static str, super::metrics::TypeStatsSummary> {
        self.metrics.get_summary()
    }
}
