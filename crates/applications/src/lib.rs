//! Event-driven applications manager using engine streams directly.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod application;
mod command;
mod consumer;
mod coordination;
mod error;
mod event;
mod response;
mod service;
mod view;

pub use application::Application;
pub use command::Command;
pub use consumer::ApplicationViewConsumer;
pub use error::Error;
pub use event::Event;
pub use response::Response;
pub use view::ApplicationView;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use proven_engine::{Client, StreamConfig};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::coordination::LeadershipCoordinator;
use crate::service::{CommandService, CommandServiceHandler};

/// Options for creating a new application.
pub struct CreateApplicationOptions {
    /// Owner's identity ID.
    pub owner_identity_id: Uuid,
}

/// Configuration for the `ApplicationManager`
#[derive(Clone)]
pub struct ApplicationManagerConfig {
    /// Prefix for stream names (e.g., "apps" results in "apps-commands", "apps-events", etc.)
    pub stream_prefix: String,
    /// Leadership lease duration (how long a leader holds leadership)
    pub leadership_lease_duration: Duration,
    /// Leadership renewal interval (how often leader renews lease)
    pub leadership_renewal_interval: Duration,
    /// Command processing timeout
    pub command_timeout: Duration,
}

impl Default for ApplicationManagerConfig {
    fn default() -> Self {
        Self {
            stream_prefix: "applications".to_string(),
            leadership_lease_duration: Duration::from_secs(30),
            leadership_renewal_interval: Duration::from_secs(10),
            command_timeout: Duration::from_secs(30), // Increased timeout for engine startup
        }
    }
}

/// Trait for managing applications.
#[async_trait]
pub trait ApplicationManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Add an allowed origin to an application.
    async fn add_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &proven_util::Origin,
    ) -> Result<(), Error>;

    /// Check if an application exists.
    async fn application_exists(&self, application_id: &Uuid) -> Result<bool, Error>;

    /// Archive an application.
    async fn archive_application(&self, application_id: &Uuid) -> Result<(), Error>;

    /// Create a new application.
    async fn create_application(
        &self,
        options: &CreateApplicationOptions,
    ) -> Result<Application, Error>;

    /// Get an application by its ID.
    async fn get_application(&self, application_id: &Uuid) -> Result<Option<Application>, Error>;

    /// Link an HTTP domain to an application.
    async fn link_http_domain(
        &self,
        application_id: &Uuid,
        http_domain: &proven_util::Domain,
    ) -> Result<(), Error>;

    /// List all applications.
    async fn list_all_applications(&self) -> Result<Vec<Application>, Error>;

    /// List all applications owned by a specific user.
    async fn list_applications_by_owner(&self, owner_id: &Uuid) -> Result<Vec<Application>, Error>;

    /// Remove an allowed origin from an application.
    async fn remove_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &proven_util::Origin,
    ) -> Result<(), Error>;

    /// Transfer ownership of an application.
    async fn transfer_ownership(
        &self,
        application_id: &Uuid,
        new_owner_id: &Uuid,
    ) -> Result<(), Error>;

    /// Unlink an HTTP domain from an application.
    async fn unlink_http_domain(
        &self,
        application_id: &Uuid,
        http_domain: &proven_util::Domain,
    ) -> Result<(), Error>;
}

/// Event-driven application manager using engine streams directly.
///
/// This implementation uses the engine's stream primitives directly without
/// the messaging abstraction layer. It implements the same patterns:
/// - Command stream for requests
/// - Event stream for state changes
/// - Stream-based leadership election
/// - In-memory view built from events
pub struct ApplicationManager {
    /// Engine client
    client: Arc<Client>,

    /// Configuration
    config: ApplicationManagerConfig,

    /// Command service (runs on leader)
    command_service: Arc<Mutex<Option<CommandService>>>,

    /// Service handler for direct execution when leader
    service_handler: Arc<CommandServiceHandler>,

    /// Leadership coordinator
    leadership: Arc<LeadershipCoordinator>,

    /// Event consumer
    event_consumer: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// View of application state
    view: ApplicationView,

    /// Stream names
    command_stream: String,
    event_stream: String,
    leadership_stream: String,
}

impl ApplicationManager {
    /// Create a new application manager.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Failed to create any of the required streams
    /// - Failed to start the event consumer
    pub async fn new(client: Arc<Client>, config: ApplicationManagerConfig) -> Result<Self, Error> {
        // Create stream names
        let command_stream = format!("{}-commands", config.stream_prefix);
        let event_stream = format!("{}-events", config.stream_prefix);
        let leadership_stream = format!("{}-leadership", config.stream_prefix);

        // Create streams if they don't exist
        let stream_config = StreamConfig::default();
        client
            .create_stream(command_stream.clone(), stream_config.clone())
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        client
            .create_stream(event_stream.clone(), stream_config.clone())
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        client
            .create_stream(leadership_stream.clone(), stream_config)
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // Create view
        let view = ApplicationView::new();

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
            command_stream,
            event_stream,
            leadership_stream,
        };

        // Start leadership monitoring
        manager.start_leadership_monitor();

        Ok(manager)
    }

    /// Get the application view for direct queries.
    #[must_use]
    pub const fn view(&self) -> &ApplicationView {
        &self.view
    }

    /// Check if this node is currently the leader.
    pub async fn is_leader(&self) -> bool {
        self.leadership.is_leader().await
    }

    /// Start the leadership monitoring task.
    fn start_leadership_monitor(&self) {
        let leadership = Arc::clone(&self.leadership);
        let command_service = Arc::clone(&self.command_service);
        let client = Arc::clone(&self.client);
        let command_stream = self.command_stream.clone();
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
                        command_stream.clone(),
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

    /// Execute a command (direct execution if leader, otherwise via stream).
    async fn execute_command(&self, command: Command) -> Result<Response, Error> {
        // If we're the leader, execute directly
        let is_leader = self.is_leader().await;
        tracing::debug!("Executing command, is_leader: {}", is_leader);

        if is_leader {
            return Ok(self.service_handler.handle_command(command).await);
        }

        // Otherwise, publish to command stream and wait for response
        tracing::debug!("Not leader, sending command via stream");
        service::execute_command_via_stream(
            &self.client,
            &self.command_stream,
            command,
            self.config.command_timeout,
        )
        .await
    }
}

impl Clone for ApplicationManager {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            config: self.config.clone(),
            command_service: Arc::clone(&self.command_service),
            service_handler: Arc::clone(&self.service_handler),
            leadership: Arc::clone(&self.leadership),
            event_consumer: Arc::clone(&self.event_consumer),
            view: self.view.clone(),
            command_stream: self.command_stream.clone(),
            event_stream: self.event_stream.clone(),
            leadership_stream: self.leadership_stream.clone(),
        }
    }
}

#[async_trait]
impl ApplicationManagement for ApplicationManager {
    async fn add_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &proven_util::Origin,
    ) -> Result<(), Error> {
        let command = Command::AddAllowedOrigin {
            application_id: *application_id,
            origin: origin.clone(),
        };

        match self.execute_command(command).await? {
            Response::AllowedOriginAdded { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn application_exists(&self, application_id: &Uuid) -> Result<bool, Error> {
        Ok(self.view.application_exists(application_id))
    }

    async fn archive_application(&self, application_id: &Uuid) -> Result<(), Error> {
        let command = Command::Archive {
            application_id: *application_id,
        };

        match self.execute_command(command).await? {
            Response::Archived { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn create_application(
        &self,
        options: &CreateApplicationOptions,
    ) -> Result<Application, Error> {
        let command = Command::Create {
            owner_identity_id: options.owner_identity_id,
        };

        let (application_id, last_event_seq) = match self.execute_command(command).await? {
            Response::Created {
                application_id,
                last_event_seq,
                ..
            } => (application_id, last_event_seq),
            Response::Error { message } | Response::InternalError { message } => {
                return Err(Error::Command(message));
            }
            _ => return Err(Error::UnexpectedResponse),
        };

        self.view.wait_for_seq(last_event_seq).await;

        Ok(self
            .view
            .get_application(&application_id)
            .ok_or(Error::UnexpectedResponse)?)
    }

    async fn get_application(&self, application_id: &Uuid) -> Result<Option<Application>, Error> {
        Ok(self.view.get_application(application_id))
    }

    async fn link_http_domain(
        &self,
        application_id: &Uuid,
        http_domain: &proven_util::Domain,
    ) -> Result<(), Error> {
        let command = Command::LinkHttpDomain {
            application_id: *application_id,
            http_domain: http_domain.clone(),
        };

        match self.execute_command(command).await? {
            Response::HttpDomainLinked { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn list_all_applications(&self) -> Result<Vec<Application>, Error> {
        Ok(self.view.list_all_applications())
    }

    async fn list_applications_by_owner(&self, owner_id: &Uuid) -> Result<Vec<Application>, Error> {
        Ok(self.view.list_applications_by_owner(owner_id))
    }

    async fn remove_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &proven_util::Origin,
    ) -> Result<(), Error> {
        let command = Command::RemoveAllowedOrigin {
            application_id: *application_id,
            origin: origin.clone(),
        };

        match self.execute_command(command).await? {
            Response::AllowedOriginRemoved { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn transfer_ownership(
        &self,
        application_id: &Uuid,
        new_owner_id: &Uuid,
    ) -> Result<(), Error> {
        let command = Command::TransferOwnership {
            application_id: *application_id,
            new_owner_id: *new_owner_id,
        };

        match self.execute_command(command).await? {
            Response::OwnershipTransferred { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn unlink_http_domain(
        &self,
        application_id: &Uuid,
        http_domain: &proven_util::Domain,
    ) -> Result<(), Error> {
        let command = Command::UnlinkHttpDomain {
            application_id: *application_id,
            http_domain: http_domain.clone(),
        };

        match self.execute_command(command).await? {
            Response::HttpDomainUnlinked { last_event_seq } => {
                // Wait for the view to process the event
                self.view.wait_for_seq(last_event_seq).await;
                Ok(())
            }
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }
}
