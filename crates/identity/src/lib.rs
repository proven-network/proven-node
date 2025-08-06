//! Event-driven identity manager using engine streams directly.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod command;
mod consumer;
mod coordination;
mod error;
mod event;
mod identity;
mod response;
mod service;
mod view;

pub use command::Command;
pub use consumer::IdentityViewConsumer;
pub use error::Error;
pub use event::Event;
pub use identity::Identity;
pub use response::Response;
pub use view::IdentityView;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use proven_engine::{Client, Message, StreamConfig};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::coordination::LeadershipCoordinator;
use crate::service::{CommandService, CommandServiceHandler};

/// Configuration for the `IdentityManager`
#[derive(Clone)]
pub struct IdentityManagerConfig {
    /// Prefix for stream names and subjects (e.g., "identity" results in "identity.commands", "identity-events", etc.)
    pub stream_prefix: String,
    /// Leadership lease duration (how long a leader holds leadership)
    pub leadership_lease_duration: Duration,
    /// Leadership renewal interval (how often leader renews lease)
    pub leadership_renewal_interval: Duration,
    /// Command processing timeout
    pub command_timeout: Duration,
}

impl Default for IdentityManagerConfig {
    fn default() -> Self {
        Self {
            stream_prefix: "identity".to_string(),
            leadership_lease_duration: Duration::from_secs(30),
            leadership_renewal_interval: Duration::from_secs(10),
            command_timeout: Duration::from_secs(30),
        }
    }
}

/// Trait for managing identities.
#[async_trait]
pub trait IdentityManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Get an identity by its ID.
    async fn get_identity(&self, identity_id: &Uuid) -> Result<Option<Identity>, Error>;

    /// Get an existing identity by PRF public key, or create a new one if it doesn't exist.
    async fn get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: &Bytes,
    ) -> Result<Identity, Error>;

    /// Check if an identity exists.
    async fn identity_exists(&self, identity_id: &Uuid) -> Result<bool, Error>;

    /// List all identities.
    async fn list_identities(&self) -> Result<Vec<Identity>, Error>;
}

/// Event-driven identity manager using engine streams directly.
///
/// This implementation uses the engine's stream primitives directly without
/// the messaging abstraction layer. It implements the same patterns:
/// - Command stream for requests
/// - Event stream for state changes
/// - Stream-based leadership election
/// - In-memory view built from events
pub struct IdentityManager {
    /// Engine client
    client: Arc<Client>,

    /// Configuration
    config: IdentityManagerConfig,

    /// Command service (runs on leader)
    command_service: Arc<Mutex<Option<CommandService>>>,

    /// Service handler for direct execution when leader
    service_handler: Arc<CommandServiceHandler>,

    /// Leadership coordinator
    leadership: Arc<LeadershipCoordinator>,

    /// Event consumer
    event_consumer: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// View of identity state
    view: IdentityView,

    /// Subject for commands
    command_subject: String,
    /// Stream names
    event_stream: String,
    leadership_stream: String,
}

impl IdentityManager {
    /// Create a new identity manager.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to create any of the required streams
    /// - Failed to start the event consumer
    #[allow(clippy::cognitive_complexity)]
    pub async fn new(client: Arc<Client>, config: IdentityManagerConfig) -> Result<Self, Error> {
        // Create subject name for commands
        let command_subject = format!("{}.commands", config.stream_prefix);
        // Create stream names
        let event_stream = format!("{}-events", config.stream_prefix);
        let leadership_stream = format!("{}-leadership", config.stream_prefix);

        // Create streams if they don't exist (only event and leadership streams needed)
        let stream_config = StreamConfig::default();
        client
            .create_global_stream(event_stream.clone(), stream_config.clone())
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        client
            .create_global_stream(leadership_stream.clone(), stream_config)
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // Create view
        let view = IdentityView::new();

        // Create service handler
        let service_handler = Arc::new(CommandServiceHandler::new(
            view.clone(),
            client.clone(),
            event_stream.clone(),
        ));

        // Create leadership coordinator
        let leadership = Arc::new(LeadershipCoordinator::new(
            client.clone(),
            leadership_stream.clone(),
            client.node_id().clone(),
            config.leadership_lease_duration,
            config.leadership_renewal_interval,
        ));

        // Start event consumer
        let event_consumer = {
            let consumer =
                consumer::start_event_consumer(client.clone(), event_stream.clone(), view.clone())
                    .await?;
            Arc::new(Mutex::new(Some(consumer)))
        };

        let manager = Self {
            client,
            config,
            command_service: Arc::new(Mutex::new(None)),
            service_handler,
            leadership,
            event_consumer,
            view,
            command_subject,
            event_stream,
            leadership_stream,
        };

        // Start leadership monitoring
        manager.start_leadership_monitor();

        // Wait for initial leadership determination and service startup
        let ready_timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();

        loop {
            if manager.is_service_ready().await {
                break;
            }

            if start.elapsed() > ready_timeout {
                return Err(Error::Service(
                    "Identity service failed to become ready within 30 seconds".to_string(),
                ));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!("Identity manager initialized and ready");

        Ok(manager)
    }

    /// Get the identity view for direct queries.
    #[must_use]
    pub const fn view(&self) -> &IdentityView {
        &self.view
    }

    /// Check if this node is currently the leader.
    pub async fn is_leader(&self) -> bool {
        self.leadership.is_leader().await
    }

    /// Check if the service is ready to handle commands.
    async fn is_service_ready(&self) -> bool {
        // Check if we're the leader
        let is_leader = self.leadership.is_leader().await;

        if is_leader {
            // If we're the leader, check if our command service is running
            self.command_service.lock().await.is_some()
        } else {
            // If we're not the leader, check if there IS a leader in the cluster
            // by trying to determine who the current leader is
            self.leadership.has_leader().await
        }
    }

    /// Start the leadership monitoring task.
    fn start_leadership_monitor(&self) {
        let leadership = Arc::clone(&self.leadership);
        let command_service = Arc::clone(&self.command_service);
        let client = Arc::clone(&self.client);
        let command_subject = self.command_subject.clone();
        let service_handler = Arc::clone(&self.service_handler);

        tokio::spawn(async move {
            let mut was_leader = false;

            loop {
                let is_leader = leadership.is_leader().await;

                if is_leader != was_leader {
                    tracing::info!(
                        "Leadership status changed: is_leader={}, was_leader={}",
                        is_leader,
                        was_leader
                    );
                }

                if is_leader && !was_leader {
                    // Became leader - start command service
                    tracing::info!("Became leader, starting command service");

                    let service = CommandService::new(
                        client.clone(),
                        command_subject.clone(),
                        service_handler.clone(),
                    );
                    let mut guard = command_service.lock().await;
                    *guard = Some(service);
                    drop(guard);
                    tracing::info!("Command service started successfully");
                    was_leader = true;
                } else if !is_leader && was_leader {
                    // Lost leadership - stop command service
                    tracing::info!("Lost leadership, stopping command service");
                    let mut guard = command_service.lock().await;
                    if let Some(service) = guard.take() {
                        drop(guard);
                        service.stop().await;
                    } else {
                        drop(guard);
                    }
                    was_leader = false;
                }

                // Check leadership status periodically
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    /// Execute a command (direct execution if leader, otherwise via pubsub request-reply).
    async fn execute_command(&self, command: Command) -> Result<Response, Error> {
        // If we're the leader, execute directly
        let is_leader = self.is_leader().await;
        tracing::debug!("Executing command, is_leader: {}", is_leader);

        if is_leader {
            return Ok(self.service_handler.handle_command(command).await);
        }

        // Otherwise, use pubsub request-reply
        tracing::debug!("Not leader, sending command via pubsub request-reply");

        // Serialize command
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&command, &mut payload)?;
        let message = Message::new(payload);

        // Send request and wait for response
        let response_msg = self
            .client
            .request(&self.command_subject, message, self.config.command_timeout)
            .await
            .map_err(|e| {
                if e.to_string().contains("No responders") {
                    Error::Service("No command service available".to_string())
                } else {
                    Error::Service(e.to_string())
                }
            })?;

        // Deserialize response
        let response: Response = ciborium::de::from_reader(response_msg.payload.as_ref())?;
        Ok(response)
    }
}

impl Clone for IdentityManager {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            config: self.config.clone(),
            command_service: Arc::clone(&self.command_service),
            service_handler: Arc::clone(&self.service_handler),
            leadership: Arc::clone(&self.leadership),
            event_consumer: Arc::clone(&self.event_consumer),
            view: self.view.clone(),
            command_subject: self.command_subject.clone(),
            event_stream: self.event_stream.clone(),
            leadership_stream: self.leadership_stream.clone(),
        }
    }
}

#[async_trait]
impl IdentityManagement for IdentityManager {
    async fn get_identity(&self, identity_id: &Uuid) -> Result<Option<Identity>, Error> {
        // Query the view directly for fast performance
        Ok(self.view.get_identity(identity_id))
    }

    async fn get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: &Bytes,
    ) -> Result<Identity, Error> {
        // First, check if the identity already exists in the view
        if let Some(identity) = self.view.get_identity_by_prf_public_key(prf_public_key) {
            return Ok(identity);
        }

        // If not, create a new identity
        let command = Command::CreateIdentityWithPrfPublicKey {
            prf_public_key: prf_public_key.clone(),
        };

        match self.execute_command(command).await? {
            Response::IdentityCreated {
                identity,
                last_event_seq,
            } => {
                // Wait for the view to be updated with the events
                self.view.wait_for_seq(last_event_seq).await;
                Ok(identity)
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            Response::PrfPublicKeyLinked { .. } => Err(Error::UnexpectedResponse),
        }
    }

    async fn identity_exists(&self, identity_id: &Uuid) -> Result<bool, Error> {
        Ok(self.view.identity_exists(identity_id))
    }

    async fn list_identities(&self) -> Result<Vec<Identity>, Error> {
        Ok(self.view.list_all_identities())
    }
}
