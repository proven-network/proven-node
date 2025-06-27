//! Event-driven applications manager using messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::type_complexity)]

mod application;
mod command;
mod error;
mod event;
mod response;
mod service_handler;
mod view;

pub use application::Application;
pub use command::Command;
pub use error::Error;
pub use event::Event;
pub use response::Response;
pub use service_handler::ApplicationServiceHandler;
pub use view::{ApplicationView, ApplicationViewConsumerHandler};

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_locks::LockManager;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream};
use proven_util::{Domain, Origin};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Lock resource ID for application service leadership
const APPLICATION_SERVICE_LEADER_LOCK: &str = "application_service_leader";

/// Options for creating a new application.
pub struct CreateApplicationOptions {
    /// Owner's identity ID.
    pub owner_identity_id: Uuid,
}

/// Trait for managing applications.
#[async_trait]
pub trait ApplicationManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Add an allowed origin to an application.
    async fn add_allowed_origin(&self, application_id: &Uuid, origin: &Origin)
    -> Result<(), Error>;

    /// Check if an application exists.
    async fn application_exists(&self, application_id: Uuid) -> Result<bool, Error>;

    /// Archive an application.
    async fn archive_application(&self, application_id: Uuid) -> Result<(), Error>;

    /// Create a new application.
    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<Application, Error>;

    /// Get an application by its ID.
    async fn get_application(&self, application_id: Uuid) -> Result<Option<Application>, Error>;

    /// Link an HTTP domain to an application.
    async fn link_http_domain(
        &self,
        application_id: Uuid,
        http_domain: Domain,
    ) -> Result<(), Error>;

    /// List all applications.
    async fn list_all_applications(&self) -> Result<Vec<Application>, Error>;

    /// List all applications owned by a specific user.
    async fn list_applications_by_owner(&self, owner_id: Uuid) -> Result<Vec<Application>, Error>;

    /// Remove an allowed origin from an application.
    async fn remove_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &Origin,
    ) -> Result<(), Error>;

    /// Transfer ownership of an application.
    async fn transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Result<(), Error>;

    /// Unlink an HTTP domain from an application.
    async fn unlink_http_domain(
        &self,
        application_id: Uuid,
        http_domain: Domain,
    ) -> Result<(), Error>;
}

/// Event-driven application manager using dual messaging streams and distributed leadership.
///
/// Commands go through the command stream, events are published to the event stream.
/// The view is built by consuming events from the event stream.
/// Only the leader node runs the command service.
pub struct ApplicationManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    /// Cached command client
    client: Arc<OnceLock<
        <CS::Initialized as InitializedStream<Command, DeserializeError, SerializeError>>::Client<
            ApplicationServiceHandler<ES::Initialized>,
        >,
    >>,

    /// Client options for the command client
    client_options:
        <<CS::Initialized as InitializedStream<Command, DeserializeError, SerializeError>>::Client<
            ApplicationServiceHandler<ES::Initialized>,
        > as Client<
            ApplicationServiceHandler<ES::Initialized>,
            Command,
            DeserializeError,
            SerializeError,
        >>::Options,

    /// Initialized command stream for processing commands
    command_stream: CS::Initialized,

    /// Event consumer (started during initialization)
    consumer:
        <ES::Initialized as InitializedStream<Event, DeserializeError, SerializeError>>::Consumer<
            ApplicationViewConsumerHandler,
        >,

    /// Initialized event stream for publishing/consuming events
    event_stream: ES::Initialized,

    /// Current leadership guard (Some if we're the leader)
    leadership_guard: Arc<Mutex<Option<LM::Guard>>>,

    /// Leadership monitoring task handle
    leadership_monitor: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// Lock manager for leader election
    lock_manager: LM,

    /// Cached service
    service: Arc<OnceLock<
        <CS::Initialized as InitializedStream<Command, DeserializeError, SerializeError>>::Service<
            ApplicationServiceHandler<ES::Initialized>,
        >,
    >>,

    /// Service options for the command service
    service_options: <<CS::Initialized as InitializedStream<
        Command,
        DeserializeError,
        SerializeError,
    >>::Service<ApplicationServiceHandler<ES::Initialized>> as Service<
        ApplicationServiceHandler<ES::Initialized>,
        Command,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// Direct access to the view for queries
    view: ApplicationView,
}

impl<CS, ES, LM> ApplicationManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    /// Create a new application manager with dual streams and distributed leadership.
    ///
    /// This creates a manager that uses separate streams for commands and events:
    /// - Commands are sent through the command stream and processed by a service (leader only)
    /// - Events are published to the event stream and consumed to build the view (all nodes)
    /// - Queries access the view directly for fast performance (all nodes)
    /// - Leadership is managed through the provided lock manager
    ///
    /// # Errors
    ///
    /// This function will return an error if the command stream or event stream cannot be initialized.
    pub async fn new(
        command_stream: CS,
        event_stream: ES,
        service_options: <<CS::Initialized as InitializedStream<
            Command,
            DeserializeError,
            SerializeError,
        >>::Service<ApplicationServiceHandler<ES::Initialized>> as Service<
            ApplicationServiceHandler<ES::Initialized>,
            Command,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<CS::Initialized as InitializedStream<
            Command,
            DeserializeError,
            SerializeError,
        >>::Client<ApplicationServiceHandler<ES::Initialized>> as Client<
            ApplicationServiceHandler<ES::Initialized>,
            Command,
            DeserializeError,
            SerializeError,
        >>::Options,
        consumer_options: <<ES::Initialized as InitializedStream<
            Event,
            DeserializeError,
            SerializeError,
        >>::Consumer<ApplicationViewConsumerHandler> as Consumer<
            ApplicationViewConsumerHandler,
            Event,
            DeserializeError,
            SerializeError,
        >>::Options,
        lock_manager: LM,
    ) -> Result<Self, Error> {
        let view = ApplicationView::new();

        // Initialize streams
        let command_stream = command_stream
            .init()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        let event_stream = event_stream
            .init()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // Create and start the event consumer
        let handler = ApplicationViewConsumerHandler::new(view.clone());
        let consumer = event_stream
            .consumer("APPLICATION_VIEW", consumer_options, handler)
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        consumer
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Allow the view to initialize before returning
        let last_event_seq = event_stream
            .last_seq()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;
        view.wait_for_seq(last_event_seq).await;

        Ok(Self {
            client: Arc::new(OnceLock::new()),
            client_options,
            command_stream,
            consumer,
            event_stream,
            leadership_guard: Arc::new(Mutex::new(None)),
            leadership_monitor: Arc::new(Mutex::new(None)),
            lock_manager,
            service: Arc::new(OnceLock::new()),
            service_options,
            view,
        })
    }

    /// Get the application view for direct queries.
    ///
    /// This provides direct access to the in-memory view for fast queries
    /// without going through the request/response cycle.
    pub const fn view(&self) -> &ApplicationView {
        &self.view
    }

    /// Get or create the client for sending commands.
    async fn get_client(
        &self,
    ) -> Result<
        <CS::Initialized as InitializedStream<Command, DeserializeError, SerializeError>>::Client<
            ApplicationServiceHandler<ES::Initialized>,
        >,
        Error,
    > {
        if let Some(client) = OnceLock::get(&self.client) {
            return Ok(client.clone());
        }

        // Try to acquire leadership and start service
        self.ensure_leadership_and_service().await?;

        let client = self
            .command_stream
            .client("APPLICATION_SERVICE", self.client_options.clone())
            .await
            .map_err(|e| Error::Client(e.to_string()))?;

        match OnceLock::set(&self.client, client.clone()) {
            Ok(()) => Ok(client),
            Err(_) => {
                // Another thread set it first, use the one that was set
                Ok(OnceLock::get(&self.client).unwrap().clone())
            }
        }
    }

    /// Attempt to acquire leadership and start service if successful.
    /// Returns Ok(()) regardless of whether leadership was acquired.
    async fn ensure_leadership_and_service(&self) -> Result<(), Error> {
        // Check if service is already running
        if OnceLock::get(&self.service).is_some() {
            return Ok(());
        }

        // Check if we already hold leadership
        {
            let guard = self.leadership_guard.lock().await;
            if guard.is_some() {
                // We have leadership but no service - start it
                return self.start_service_as_leader().await;
            }
        }

        // Try to acquire leadership
        match self
            .lock_manager
            .try_lock(APPLICATION_SERVICE_LEADER_LOCK.to_string())
            .await
        {
            Ok(Some(guard)) => {
                tracing::info!("Acquired application service leadership");

                // Store the leadership guard
                {
                    let mut leadership_guard = self.leadership_guard.lock().await;
                    *leadership_guard = Some(guard);
                }

                // Start the service
                self.start_service_as_leader().await?;

                // Start leadership monitoring
                self.start_leadership_monitor().await;

                Ok(())
            }
            Ok(None) => {
                tracing::debug!("Another node holds application service leadership");
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to acquire application service leadership: {:?}", e);
                Err(Error::Service(format!(
                    "Leadership acquisition failed: {e}"
                )))
            }
        }
    }

    /// Start the service as the leader.
    async fn start_service_as_leader(&self) -> Result<(), Error> {
        if OnceLock::get(&self.service).is_some() {
            return Ok(());
        }

        let handler = ApplicationServiceHandler::new(self.view.clone(), self.event_stream.clone());

        let service = self
            .command_stream
            .service("APPLICATION_SERVICE", self.service_options.clone(), handler)
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Start the service to begin processing requests
        service
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Store the service (ignore result if another thread set it first)
        let _ = self.service.set(service);

        tracing::info!("Application service started as leader");
        Ok(())
    }

    /// Start the leadership monitoring background task.
    async fn start_leadership_monitor(&self) {
        let mut monitor_guard = self.leadership_monitor.lock().await;
        if monitor_guard.is_some() {
            return; // Already monitoring
        }

        let leadership_guard = Arc::clone(&self.leadership_guard);
        let lock_manager = self.lock_manager.clone();

        let handle = tokio::spawn(async move {
            Self::leadership_monitor_task(leadership_guard, lock_manager).await;
        });

        tracing::debug!("Started application service leadership monitor");
        *monitor_guard = Some(handle);
    }

    /// Background task to monitor leadership status.
    async fn leadership_monitor_task(
        leadership_guard: Arc<Mutex<Option<LM::Guard>>>,
        lock_manager: LM,
    ) {
        let mut check_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            check_interval.tick().await;

            // Check if we still hold leadership
            let still_leader = {
                let guard = leadership_guard.lock().await;
                guard.is_some()
            };

            if !still_leader {
                tracing::warn!("Lost application service leadership");

                // Try to reacquire leadership
                match lock_manager
                    .try_lock(APPLICATION_SERVICE_LEADER_LOCK.to_string())
                    .await
                {
                    Ok(Some(new_guard)) => {
                        tracing::info!("Reacquired application service leadership");
                        let mut guard = leadership_guard.lock().await;
                        *guard = Some(new_guard);
                        // Note: Service is already running, no need to restart
                    }
                    Ok(None) => {
                        tracing::debug!("Another node still holds application service leadership");
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to reacquire application service leadership: {:?}",
                            e
                        );
                    }
                }
            }
        }
    }

    /// Check if this node is currently the leader.
    pub async fn is_leader(&self) -> bool {
        let guard = self.leadership_guard.lock().await;
        guard.is_some()
    }
}

impl<CS, ES, LM> Clone for ApplicationManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    CS::Initialized: Clone,
    ES: Stream<Event, DeserializeError, SerializeError>,
    ES::Initialized: Clone,
    LM: LockManager + Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            client_options: self.client_options.clone(),
            command_stream: self.command_stream.clone(),
            consumer: self.consumer.clone(),
            event_stream: self.event_stream.clone(),
            leadership_guard: Arc::clone(&self.leadership_guard),
            leadership_monitor: Arc::clone(&self.leadership_monitor),
            lock_manager: self.lock_manager.clone(),
            service: Arc::clone(&self.service),
            service_options: self.service_options.clone(),
            view: self.view.clone(),
        }
    }
}

#[async_trait]
impl<CS, ES, LM> ApplicationManagement for ApplicationManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    async fn add_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &Origin,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::AddAllowedOrigin {
            application_id: *application_id,
            origin: origin.clone(),
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::AllowedOriginAdded { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn application_exists(&self, application_id: Uuid) -> Result<bool, Error> {
        Ok(self.view.application_exists(application_id).await)
    }

    async fn archive_application(&self, application_id: Uuid) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::Archive { application_id };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::Archived { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn create_application(
        &self,
        options: CreateApplicationOptions,
    ) -> Result<Application, Error> {
        let client = self.get_client().await?;

        let command = Command::Create {
            owner_identity_id: options.owner_identity_id,
        };

        let (application_id, last_event_seq) = match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::Created {
                application_id,
                last_event_seq,
                ..
            }) => (application_id, last_event_seq),
            ClientResponseType::Response(Response::Error { message }) => {
                return Err(Error::Command(message));
            }
            _ => return Err(Error::UnexpectedResponse),
        };

        self.view.wait_for_seq(last_event_seq).await;

        Ok(self
            .view
            .get_application(application_id)
            .await
            .ok_or(Error::UnexpectedResponse)?)
    }

    async fn get_application(&self, application_id: Uuid) -> Result<Option<Application>, Error> {
        Ok(self.view.get_application(application_id).await)
    }

    async fn link_http_domain(
        &self,
        application_id: Uuid,
        http_domain: Domain,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::LinkHttpDomain {
            application_id,
            http_domain,
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::HttpDomainLinked { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn list_all_applications(&self) -> Result<Vec<Application>, Error> {
        Ok(self.view.list_all_applications().await)
    }

    async fn list_applications_by_owner(&self, owner_id: Uuid) -> Result<Vec<Application>, Error> {
        Ok(self.view.list_applications_by_owner(owner_id).await)
    }

    async fn remove_allowed_origin(
        &self,
        application_id: &Uuid,
        origin: &Origin,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::RemoveAllowedOrigin {
            application_id: *application_id,
            origin: origin.clone(),
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::AllowedOriginRemoved { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::TransferOwnership {
            application_id,
            new_owner_id,
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::OwnershipTransferred { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn unlink_http_domain(
        &self,
        application_id: Uuid,
        http_domain: Domain,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = Command::UnlinkHttpDomain {
            application_id,
            http_domain,
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(Response::HttpDomainUnlinked { .. }) => Ok(()),
            ClientResponseType::Response(Response::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use proven_locks_memory::MemoryLockManager;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        consumer::MemoryConsumerOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use std::str::FromStr;
    use uuid::Uuid;

    type TestCommandStream = MemoryStream<Command, DeserializeError, SerializeError>;
    type TestEventStream = MemoryStream<Event, DeserializeError, SerializeError>;
    type TestApplicationManager =
        ApplicationManager<TestCommandStream, TestEventStream, MemoryLockManager>;

    async fn create_test_manager() -> TestApplicationManager {
        let stream_name = format!("test-stream-{}", Uuid::new_v4());
        let command_stream =
            MemoryStream::new(stream_name.clone() + "-commands", MemoryStreamOptions);
        let event_stream = MemoryStream::new(stream_name + "-events", MemoryStreamOptions);
        let lock_manager = MemoryLockManager::new();

        ApplicationManager::new(
            command_stream,
            event_stream,
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
            lock_manager,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_create_application() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        let result = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await;

        assert!(result.is_ok());
        let app = result.unwrap();
        assert_eq!(app.owner_id, owner_id);
    }

    #[tokio::test]
    async fn test_get_application_from_view() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Get from view
        let retrieved = manager.get_application(app.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().owner_id, owner_id);
    }

    #[tokio::test]
    async fn test_direct_view_queries() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Direct view access should work immediately (no async required since view is shared)
        assert_eq!(manager.view().application_count().await, 0);

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Direct view access
        assert_eq!(manager.view().application_count().await, 1);
        assert!(manager.view().application_exists(app.id).await);
        assert_eq!(
            manager
                .view()
                .list_applications_by_owner(owner_id)
                .await
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn test_transfer_ownership() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let new_owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Transfer ownership
        let result = manager.transfer_ownership(app.id, new_owner_id).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify ownership changed in view
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.owner_id, new_owner_id);
    }

    #[tokio::test]
    async fn test_archive_application() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify exists
        assert!(manager.view().application_exists(app.id).await);

        // Archive application
        let result = manager.archive_application(app.id).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify no longer exists in view
        assert!(!manager.view().application_exists(app.id).await);
    }

    #[tokio::test]
    async fn test_concurrent_application_creation() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create multiple applications concurrently
        let mut handles = vec![];
        for _i in 0..5 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                manager_clone
                    .create_application(CreateApplicationOptions {
                        owner_identity_id: owner_id,
                    })
                    .await
            });
            handles.push(handle);
        }

        // Wait for all to complete
        let results: Result<Vec<_>, _> = futures::future::try_join_all(handles).await;
        assert!(results.is_ok());

        let apps = results.unwrap();
        assert_eq!(apps.len(), 5);

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Verify all applications exist in view
        assert_eq!(manager.view().application_count().await, 5);
        assert_eq!(
            manager
                .view()
                .list_applications_by_owner(owner_id)
                .await
                .len(),
            5
        );
    }

    #[tokio::test]
    async fn test_last_processed_seq_tracking() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Initially, no events processed
        assert_eq!(manager.view().last_processed_seq(), 0);

        // Create application (publishes 1 event)
        let _app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should have processed sequence 0 (first event in memory streams)
        assert_eq!(manager.view().last_processed_seq(), 1);

        // Create another application (publishes 1 more event)
        let app2 = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should have processed sequence 1 (second event)
        assert_eq!(manager.view().last_processed_seq(), 2);

        // Transfer ownership (publishes 1 more event)
        let _result = manager.transfer_ownership(app2.id, Uuid::new_v4()).await;

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should have processed sequence 2 (third event)
        assert_eq!(manager.view().last_processed_seq(), 3);
    }

    #[tokio::test]
    async fn test_strong_consistency_commands() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application (does not require strong consistency)
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Now test commands that require strong consistency
        // Transfer ownership (requires strong consistency)
        let new_owner_id = Uuid::new_v4();
        let result = manager.transfer_ownership(app.id, new_owner_id).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Archive application (requires strong consistency)
        let result = manager.archive_application(app.id).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify application is gone
        assert!(!manager.view().application_exists(app.id).await);
    }

    #[tokio::test]
    async fn test_leadership_functionality() {
        let manager = create_test_manager().await;

        // Initially not a leader
        assert!(!manager.is_leader().await);

        // Trigger client creation which should attempt leadership acquisition
        let owner_id = Uuid::new_v4();
        let result = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await;

        assert!(result.is_ok());

        // Should now be leader (memory lock manager allows single acquisition)
        assert!(manager.is_leader().await);

        // Service should be running
        assert!(manager.service.get().is_some());
    }

    #[tokio::test]
    async fn test_multi_node_leadership() {
        // Create shared streams and lock manager (simulating the same NATS cluster)
        let shared_lock_manager = MemoryLockManager::new();
        let stream_name = format!("test-shared-stream-{}", Uuid::new_v4());
        let command_stream =
            MemoryStream::new(stream_name.clone() + "-commands", MemoryStreamOptions);
        let event_stream = MemoryStream::new(stream_name + "-events", MemoryStreamOptions);

        // Create two managers sharing the same streams and lock manager (simulating different nodes)
        let manager1 = ApplicationManager::new(
            command_stream.clone(),
            event_stream.clone(),
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
            shared_lock_manager.clone(),
        )
        .await
        .unwrap();

        let manager2 = ApplicationManager::new(
            command_stream.clone(),
            event_stream.clone(),
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
            shared_lock_manager.clone(),
        )
        .await
        .unwrap();

        // Both start as non-leaders
        assert!(!manager1.is_leader().await);
        assert!(!manager2.is_leader().await);

        // First manager creates an application (should become leader)
        let owner_id = Uuid::new_v4();
        let result1 = manager1
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await;

        assert!(result1.is_ok());
        assert!(manager1.is_leader().await);

        // Second manager tries to create an application
        // This should work because NATS will route it to manager1's service
        let result2 = manager2
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await;

        assert!(result2.is_ok());

        // Manager1 should still be the leader, manager2 should not become leader
        assert!(manager1.is_leader().await);
        assert!(!manager2.is_leader().await);

        // Manager1 should have the service, manager2 should not
        assert!(manager1.service.get().is_some()); // Leader has service

        // Both should see the applications in their views (eventually)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(manager1.view().application_count().await, 2);
        assert_eq!(manager2.view().application_count().await, 2);
    }

    #[tokio::test]
    async fn test_link_http_domain_success() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link HTTP domain
        let result = manager.link_http_domain(app.id, domain.clone()).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify domain is linked in view
        assert!(manager.view().http_domain_linked(&domain).await);
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain)
                .await,
            Some(app.id)
        );

        // Verify application has the domain in its linked domains
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.linked_http_domains, vec![domain]);
    }

    #[tokio::test]
    async fn test_link_http_domain_to_nonexistent_application() {
        let manager = create_test_manager().await;
        let nonexistent_app_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Try to link domain to non-existent application
        let result = manager.link_http_domain(nonexistent_app_id, domain).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "Application not found"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_link_already_linked_domain() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create first application
        let app1 = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Create second application
        let app2 = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link domain to first application
        let result = manager.link_http_domain(app1.id, domain.clone()).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to link same domain to second application (should fail)
        let result = manager.link_http_domain(app2.id, domain).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "HTTP domain already linked"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_unlink_http_domain_success() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link HTTP domain
        let result = manager.link_http_domain(app.id, domain.clone()).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify domain is linked
        assert!(manager.view().http_domain_linked(&domain).await);

        // Unlink HTTP domain
        let result = manager.unlink_http_domain(app.id, domain.clone()).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify domain is no longer linked
        assert!(!manager.view().http_domain_linked(&domain).await);
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain)
                .await,
            None
        );

        // Verify application no longer has the domain
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.linked_http_domains, Vec::<Domain>::new());
    }

    #[tokio::test]
    async fn test_unlink_http_domain_from_nonexistent_application() {
        let manager = create_test_manager().await;
        let nonexistent_app_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Try to unlink domain from non-existent application
        let result = manager.unlink_http_domain(nonexistent_app_id, domain).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "Application not found"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_unlink_non_linked_domain() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to unlink domain that was never linked
        let result = manager.unlink_http_domain(app.id, domain).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "HTTP domain not linked to this application"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_unlink_domain_from_wrong_application() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create two applications
        let app1 = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        let app2 = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link domain to first application
        let result = manager.link_http_domain(app1.id, domain.clone()).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to unlink domain from second application (should fail)
        let result = manager.unlink_http_domain(app2.id, domain).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "HTTP domain not linked to this application"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_multiple_domains_per_application() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain1 = Domain::from_str("example.com").unwrap();
        let domain2 = Domain::from_str("test.com").unwrap();
        let domain3 = Domain::from_str("demo.org").unwrap();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link multiple domains
        assert!(
            manager
                .link_http_domain(app.id, domain1.clone())
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(
            manager
                .link_http_domain(app.id, domain2.clone())
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(
            manager
                .link_http_domain(app.id, domain3.clone())
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify all domains are linked
        assert!(manager.view().http_domain_linked(&domain1).await);
        assert!(manager.view().http_domain_linked(&domain2).await);
        assert!(manager.view().http_domain_linked(&domain3).await);

        // Verify all domains point to the same application
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain1)
                .await,
            Some(app.id)
        );
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain2)
                .await,
            Some(app.id)
        );
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain3)
                .await,
            Some(app.id)
        );

        // Verify application has all domains
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.linked_http_domains.len(), 3);
        assert!(updated_app.linked_http_domains.contains(&domain1));
        assert!(updated_app.linked_http_domains.contains(&domain2));
        assert!(updated_app.linked_http_domains.contains(&domain3));

        // Unlink one domain
        assert!(
            manager
                .unlink_http_domain(app.id, domain2.clone())
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify only domain2 is unlinked
        assert!(manager.view().http_domain_linked(&domain1).await);
        assert!(!manager.view().http_domain_linked(&domain2).await);
        assert!(manager.view().http_domain_linked(&domain3).await);

        // Verify application only has remaining domains
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.linked_http_domains.len(), 2);
        assert!(updated_app.linked_http_domains.contains(&domain1));
        assert!(!updated_app.linked_http_domains.contains(&domain2));
        assert!(updated_app.linked_http_domains.contains(&domain3));
    }

    #[tokio::test]
    async fn test_domain_linking_with_application_archival() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();
        let domain = Domain::from_str("example.com").unwrap();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Link domain to application
        assert!(
            manager
                .link_http_domain(app.id, domain.clone())
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify domain is linked
        assert!(manager.view().http_domain_linked(&domain).await);

        // Archive the application
        assert!(manager.archive_application(app.id).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify application is archived and domain is no longer linked
        assert!(!manager.view().application_exists(app.id).await);
        assert!(!manager.view().http_domain_linked(&domain).await);
        assert_eq!(
            manager
                .view()
                .get_application_id_for_http_domain(&domain)
                .await,
            None
        );
    }

    #[tokio::test]
    async fn test_concurrent_domain_linking() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to link the same domain concurrently (should only succeed once)
        let domain = Domain::from_str("example.com").unwrap();
        let manager_clone = manager.clone();
        let domain_clone = domain.clone();

        let handle1 = tokio::spawn(async move { manager.link_http_domain(app.id, domain).await });

        let handle2 =
            tokio::spawn(async move { manager_clone.link_http_domain(app.id, domain_clone).await });

        let results = futures::future::join_all([handle1, handle2]).await;

        // One should succeed, one should fail
        let results: Vec<Result<(), Error>> = results.into_iter().map(|r| r.unwrap()).collect();
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        let error_count = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(success_count, 1);
        assert_eq!(error_count, 1);

        // Verify the error is the expected one
        let error_result = results.iter().find(|r| r.is_err()).unwrap();
        match error_result.as_ref().unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "HTTP domain already linked"),
            _ => panic!("Expected command error"),
        }
    }

    #[tokio::test]
    async fn test_add_allowed_origin_success() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin = Origin::from_str("https://example.com").unwrap();

        // Add allowed origin
        let result = manager.add_allowed_origin(&app.id, &origin).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify origin was added
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins, vec![origin]);
    }

    #[tokio::test]
    async fn test_add_allowed_origin_to_nonexistent_application() {
        let manager = create_test_manager().await;
        let nonexistent_id = Uuid::new_v4();
        let origin = Origin::from_str("https://example.com").unwrap();

        let result = manager.add_allowed_origin(&nonexistent_id, &origin).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Command(_)));
    }

    #[tokio::test]
    async fn test_add_duplicate_allowed_origin() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin = Origin::from_str("https://example.com").unwrap();

        // Add allowed origin first time
        let result = manager.add_allowed_origin(&app.id, &origin).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to add the same origin again
        let result = manager.add_allowed_origin(&app.id, &origin).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Command(_)));

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify origin appears only once
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins, vec![origin]);
    }

    #[tokio::test]
    async fn test_remove_allowed_origin_success() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin = Origin::from_str("https://example.com").unwrap();

        // Add allowed origin
        let result = manager.add_allowed_origin(&app.id, &origin).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Remove allowed origin
        let result = manager.remove_allowed_origin(&app.id, &origin).await;
        assert!(result.is_ok());

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify origin was removed
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins, Vec::<Origin>::new());
    }

    #[tokio::test]
    async fn test_remove_allowed_origin_from_nonexistent_application() {
        let manager = create_test_manager().await;
        let nonexistent_id = Uuid::new_v4();
        let origin = Origin::from_str("https://example.com").unwrap();

        let result = manager
            .remove_allowed_origin(&nonexistent_id, &origin)
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Command(_)));
    }

    #[tokio::test]
    async fn test_remove_non_allowed_origin() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin = Origin::from_str("https://example.com").unwrap();

        // Try to remove origin that was never added
        let result = manager.remove_allowed_origin(&app.id, &origin).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Command(_)));
    }

    #[tokio::test]
    async fn test_multiple_allowed_origins_per_application() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin1 = Origin::from_str("https://example1.com").unwrap();
        let origin2 = Origin::from_str("https://example2.com").unwrap();
        let origin3 = Origin::from_str("https://example3.com").unwrap();

        // Add multiple origins
        assert!(manager.add_allowed_origin(&app.id, &origin1).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(manager.add_allowed_origin(&app.id, &origin2).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        assert!(manager.add_allowed_origin(&app.id, &origin3).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify all origins were added
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins.len(), 3);
        assert!(updated_app.allowed_origins.contains(&origin1));
        assert!(updated_app.allowed_origins.contains(&origin2));
        assert!(updated_app.allowed_origins.contains(&origin3));

        // Remove one origin
        assert!(
            manager
                .remove_allowed_origin(&app.id, &origin2)
                .await
                .is_ok()
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify only the specified origin was removed
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins.len(), 2);
        assert!(updated_app.allowed_origins.contains(&origin1));
        assert!(!updated_app.allowed_origins.contains(&origin2));
        assert!(updated_app.allowed_origins.contains(&origin3));
    }

    #[tokio::test]
    async fn test_allowed_origins_with_application_archival() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let origin = Origin::from_str("https://example.com").unwrap();

        // Add allowed origin
        assert!(manager.add_allowed_origin(&app.id, &origin).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify origin was added
        let updated_app = manager.view().get_application(app.id).await.unwrap();
        assert_eq!(updated_app.allowed_origins, vec![origin.clone()]);

        // Archive application
        assert!(manager.archive_application(app.id).await.is_ok());
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify application is gone
        assert!(manager.view().get_application(app.id).await.is_none());

        // Try to add origin to archived application (should fail)
        let result = manager.add_allowed_origin(&app.id, &origin).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Command(_)));
    }

    #[tokio::test]
    async fn test_concurrent_allowed_origin_operations() {
        let manager = create_test_manager().await;
        let owner_id = Uuid::new_v4();

        // Create application
        let app = manager
            .create_application(CreateApplicationOptions {
                owner_identity_id: owner_id,
            })
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Try to add the same origin concurrently (should only succeed once)
        let origin = Origin::from_str("https://concurrent-test.com").unwrap();
        let manager_clone = manager.clone();
        let origin_clone = origin.clone();

        let handle1 =
            tokio::spawn(async move { manager.add_allowed_origin(&app.id, &origin).await });

        let handle2 = tokio::spawn(async move {
            manager_clone
                .add_allowed_origin(&app.id, &origin_clone)
                .await
        });

        let results = futures::future::join_all([handle1, handle2]).await;

        // One should succeed, one should fail
        let results: Vec<Result<(), Error>> = results.into_iter().map(|r| r.unwrap()).collect();
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        let error_count = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(success_count, 1);
        assert_eq!(error_count, 1);

        // Verify the error is the expected one
        let error_result = results.iter().find(|r| r.is_err()).unwrap();
        match error_result.as_ref().unwrap_err() {
            Error::Command(msg) => assert_eq!(msg, "Origin already in allowed origins"),
            _ => panic!("Expected command error"),
        }
    }
}
