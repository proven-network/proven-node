//! Main event service implementation

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use super::bus::{EventBus, EventPublisher, EventSubscriber};
use super::filters::EventFilter;
use super::store::EventStore;
use super::types::*;
use uuid::Uuid;

/// Event service for managing event-driven communication
pub struct EventService {
    /// Service configuration
    config: EventConfig,

    /// Event bus
    bus: Arc<EventBus>,

    /// Event store (optional)
    store: Option<Arc<EventStore>>,

    /// Event processing channel
    event_channel: (mpsc::Sender<EventEnvelope>, mpsc::Receiver<EventEnvelope>),

    /// Background tasks
    background_tasks: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown_signal: Arc<tokio::sync::Notify>,

    /// Service state
    state: Arc<RwLock<ServiceState>>,

    /// Event processing metrics
    metrics: Arc<EventMetrics>,
}

/// Event processing metrics
struct EventMetrics {
    events_processed: AtomicU64,
    events_failed: AtomicU64,
}

/// Service state
#[derive(Debug, Clone, Copy, PartialEq)]
enum ServiceState {
    /// Not started
    NotStarted,
    /// Running
    Running,
    /// Stopping
    Stopping,
    /// Stopped
    Stopped,
}

impl EventService {
    /// Create a new event service
    pub fn new(config: EventConfig) -> Self {
        let bus = Arc::new(EventBus::new(config.clone()));

        let store = if config.enable_persistence {
            Some(Arc::new(EventStore::new(
                10000, // Max events
                config.retention_duration,
            )))
        } else {
            None
        };

        let event_channel = mpsc::channel(config.bus_capacity);

        Self {
            config,
            bus,
            store,
            event_channel,
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
            metrics: Arc::new(EventMetrics {
                events_processed: AtomicU64::new(0),
                events_failed: AtomicU64::new(0),
            }),
        }
    }

    /// Start the event service
    pub async fn start(&mut self) -> EventingResult<()> {
        let mut state = self.state.write().await;
        match *state {
            ServiceState::NotStarted | ServiceState::Stopped => {
                *state = ServiceState::Running;
            }
            _ => {
                return Err(EventError::Internal(format!(
                    "Service cannot be started from {:?} state",
                    *state
                )));
            }
        }
        drop(state);

        info!("Starting event service");

        // Start event processor
        let processor_task = self.spawn_event_processor();

        // Start cleanup task if persistence is enabled
        let cleanup_task = if self.store.is_some() {
            Some(self.spawn_cleanup_task())
        } else {
            None
        };

        // Now acquire the lock and add tasks
        let mut tasks = self.background_tasks.write().await;
        tasks.push(processor_task);
        if let Some(cleanup) = cleanup_task {
            tasks.push(cleanup);
        }

        // Start metrics collection if enabled
        if self.config.enable_metrics {
            tasks.push(self.spawn_metrics_task());
        }

        Ok(())
    }

    /// Stop the event service
    pub async fn stop(&self) -> EventingResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        *state = ServiceState::Stopping;
        drop(state);

        info!("Stopping event service");

        // Signal shutdown
        self.shutdown_signal.notify_waiters();

        // Wait for tasks
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            if let Err(e) = task.await {
                error!("Error stopping event task: {}", e);
            }
        }

        let mut state = self.state.write().await;
        *state = ServiceState::Stopped;

        Ok(())
    }

    /// Publish an event
    pub async fn publish(&self, event: Event, source: String) -> EventingResult<()> {
        self.ensure_running().await?;

        let envelope = EventEnvelope {
            metadata: EventMetadata {
                id: Uuid::new_v4(),
                timestamp: SystemTime::now(),
                event_type: event.event_type(),
                priority: event.default_priority(),
                source,
                correlation_id: None,
                tags: Vec::new(),
                synchronous: false,
            },
            event,
        };

        self.event_channel
            .0
            .send(envelope.clone())
            .await
            .map_err(|_| EventError::PublishFailed("Failed to send event".to_string()))?;

        Ok(())
    }

    /// Publish an event with custom metadata
    pub async fn publish_with_metadata(
        &self,
        event: Event,
        metadata: EventMetadata,
    ) -> EventingResult<()> {
        self.ensure_running().await?;

        let envelope = EventEnvelope { metadata, event };

        self.event_channel
            .0
            .send(envelope)
            .await
            .map_err(|_| EventError::PublishFailed("Failed to send event".to_string()))?;

        Ok(())
    }

    /// Subscribe to events
    pub async fn subscribe(
        &self,
        subscriber_id: String,
        filter: EventFilter,
    ) -> EventingResult<EventSubscriber> {
        self.ensure_running().await?;
        self.bus.subscribe(subscriber_id, filter).await
    }

    /// Unsubscribe from events
    pub async fn unsubscribe(&self, subscriber_id: &str) -> EventingResult<()> {
        self.bus.unsubscribe(subscriber_id).await
    }

    /// Create a publisher handle
    pub fn create_publisher(&self) -> EventPublisher {
        EventPublisher::new(self.event_channel.0.clone())
    }

    /// Query event history
    pub async fn query_events(
        &self,
        query: crate::services::event::EventQuery,
    ) -> EventingResult<Vec<crate::services::event::EventHistory>> {
        if let Some(store) = &self.store {
            store.query(&query).await
        } else {
            Err(EventError::StoreError(
                "Event persistence not enabled".to_string(),
            ))
        }
    }

    /// Get event service statistics
    pub async fn get_stats(&self) -> EventingResult<EventServiceStats> {
        let subscriber_count = self.bus.subscriber_count().await;

        let store_stats = if let Some(store) = &self.store {
            Some(store.get_stats().await?)
        } else {
            None
        };

        Ok(EventServiceStats {
            subscriber_count,
            store_stats,
        })
    }

    // Private helper methods

    /// Ensure service is running
    async fn ensure_running(&self) -> EventingResult<()> {
        let state = self.state.read().await;
        if *state != ServiceState::Running {
            return Err(EventError::NotStarted);
        }
        Ok(())
    }

    /// Spawn event processor task
    fn spawn_event_processor(&mut self) -> JoinHandle<()> {
        let bus = self.bus.clone();
        let store = self.store.clone();
        let shutdown = self.shutdown_signal.clone();
        let metrics = self.metrics.clone();

        // Take the receiver from the channel pair without creating a new channel
        let mut event_receiver = {
            let (tx, rx) = mpsc::channel(self.config.bus_capacity);
            let (old_tx, old_rx) = std::mem::replace(&mut self.event_channel, (tx, rx));
            // Put the old sender back so EventPublisher instances keep working
            self.event_channel.0 = old_tx;
            old_rx
        };

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(envelope) = event_receiver.recv() => {
                        let start = std::time::Instant::now();
                        let event_id = envelope.metadata.id;

                        debug!(
                            "Processing event {} of type {:?}",
                            event_id, envelope.metadata.event_type
                        );

                        // Store event if persistence is enabled
                        if let Some(store) = &store {
                            let processing_time = start.elapsed();
                            if let Err(e) = store.store_with_timing(
                                envelope.clone(),
                                EventResult::Success,
                                processing_time,
                            ).await {
                                error!("Failed to store event: {}", e);
                            }
                        }

                        // Publish to bus for subscribers
                        if let Err(e) = bus.publish(envelope).await {
                            error!("Failed to publish event to bus: {}", e);
                            metrics.events_failed.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.events_processed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    _ = shutdown.notified() => {
                        debug!("Event processor shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn cleanup task
    fn spawn_cleanup_task(&self) -> JoinHandle<()> {
        let store = self.store.clone();
        let bus = self.bus.clone();
        let interval = Duration::from_secs(3600); // 1 hour
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Clean event store
                        if let Some(store) = &store
                            && let Err(e) = store.clean_expired().await {
                                error!("Failed to clean event store: {}", e);
                            }

                        // Clean dedup cache
                        bus.clean_dedup_cache().await;
                    }
                    _ = shutdown.notified() => {
                        debug!("Cleanup task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn metrics task
    fn spawn_metrics_task(&self) -> JoinHandle<()> {
        let bus = self.bus.clone();
        let store = self.store.clone();
        let interval = Duration::from_secs(60);
        let shutdown = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        let subscriber_count = bus.subscriber_count().await;

                        debug!(
                            "Event service metrics - subscribers: {}",
                            subscriber_count
                        );

                        if let Some(store) = &store
                            && let Ok(stats) = store.get_stats().await {
                                debug!(
                                    "Event store metrics - total: {}, avg processing: {:?}",
                                    stats.total_events,
                                    stats.avg_processing_time
                                );
                            }
                    }
                    _ = shutdown.notified() => {
                        debug!("Metrics task shutting down");
                        break;
                    }
                }
            }
        })
    }
}

/// Event service statistics
#[derive(Debug, Clone)]
pub struct EventServiceStats {
    /// Number of active subscribers
    pub subscriber_count: usize,
    /// Event store statistics
    pub store_stats: Option<crate::services::event::store::EventStoreStats>,
}

impl Drop for EventService {
    fn drop(&mut self) {
        // Ensure shutdown on drop
        self.shutdown_signal.notify_waiters();
    }
}

// Re-export config for convenience
pub use super::types::EventConfig;
