//! New type-safe event service implementation

use async_trait::async_trait;
use proven_logger::{error, info};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{ConsensusResult, Error, ErrorKind};
use crate::foundation::{
    ServiceLifecycle,
    traits::{HealthStatus, ServiceHealth},
};

use super::bus::EventBus;
use super::traits::ServiceEvent;

/// Configuration for the event service
#[derive(Debug, Clone)]
pub struct EventServiceConfig {
    /// Whether to log all events (for debugging)
    pub log_events: bool,
    /// Whether to track event statistics
    pub enable_stats: bool,
}

impl Default for EventServiceConfig {
    fn default() -> Self {
        Self {
            log_events: false,
            enable_stats: true,
        }
    }
}

/// Service state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Stopped,
}

/// New type-safe event service
pub struct EventService {
    /// The event bus
    bus: Arc<EventBus>,
    /// Service configuration
    config: EventServiceConfig,
    /// Service state
    state: Arc<RwLock<ServiceState>>,
    /// Stats collection task handle
    stats_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Cancellation token
    cancellation_token: CancellationToken,
}

impl EventService {
    /// Create a new event service
    pub fn new(config: EventServiceConfig) -> Self {
        Self {
            bus: Arc::new(EventBus::new()),
            config,
            state: Arc::new(RwLock::new(ServiceState::NotStarted)),
            stats_task: Arc::new(RwLock::new(None)),
            cancellation_token: CancellationToken::new(),
        }
    }

    /// Get the event bus for publishing and subscribing
    pub fn bus(&self) -> Arc<EventBus> {
        self.bus.clone()
    }

    /// Publish an event
    pub async fn publish<E: ServiceEvent>(&self, event: E) {
        if self.config.log_events {
            info!("Publishing event: {event:?}");
        }
        self.bus.publish(event).await;
    }

    /// Start stats collection task
    async fn start_stats_collection(&self) {
        if !self.config.enable_stats {
            return;
        }

        let bus = self.bus.clone();
        let token = self.cancellation_token.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let stats = bus.stats().await;
                        info!(
                            "Event stats - Published: {}, Handlers invoked: {}, By type: {:?}",
                            stats.total_events_published,
                            stats.total_handlers_invoked,
                            stats.events_by_type
                        );
                    }
                    _ = token.cancelled() => {
                        break;
                    }
                }
            }
        });

        *self.stats_task.write().await = Some(handle);
    }
}

#[async_trait]
impl ServiceLifecycle for EventService {
    async fn start(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::NotStarted {
            return Err(Error::with_context(
                ErrorKind::InvalidState,
                "Event service already started",
            ));
        }

        info!("Starting event service");

        // Start stats collection
        self.start_stats_collection().await;

        *state = ServiceState::Running;
        info!("Event service started");

        Ok(())
    }

    async fn stop(&self) -> ConsensusResult<()> {
        let mut state = self.state.write().await;
        if *state != ServiceState::Running {
            return Ok(());
        }

        info!("Stopping event service");

        // Cancel tasks
        self.cancellation_token.cancel();

        // Wait for stats task
        if let Some(handle) = self.stats_task.write().await.take()
            && let Err(e) = handle.await
        {
            error!("Stats task error: {e}");
        }

        *state = ServiceState::Stopped;
        info!("Event service stopped");

        Ok(())
    }

    async fn is_running(&self) -> bool {
        *self.state.read().await == ServiceState::Running
    }

    async fn health_check(&self) -> ConsensusResult<ServiceHealth> {
        let state = self.state.read().await;
        let (status, message) = match *state {
            ServiceState::Running => (HealthStatus::Healthy, None),
            ServiceState::NotStarted => (HealthStatus::Unhealthy, Some("Not started".to_string())),
            ServiceState::Stopped => (HealthStatus::Unhealthy, Some("Stopped".to_string())),
        };

        Ok(ServiceHealth {
            name: "event".to_string(),
            status,
            message,
            subsystems: Vec::new(),
        })
    }
}

impl Clone for EventService {
    fn clone(&self) -> Self {
        Self {
            bus: self.bus.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            stats_task: self.stats_task.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}
