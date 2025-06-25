//! Event-driven applications manager using messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod application;
mod error;
mod events;
mod request;
mod response;
mod service_handler;
mod view;

pub use application::Application;
pub use error::Error;
pub use events::ApplicationEvent;
pub use request::ApplicationCommand;
pub use response::ApplicationCommandResponse;
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
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing;
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

    /// List all applications owned by a specific user.
    async fn list_applications_by_owner(&self, owner_id: Uuid) -> Result<Vec<Application>, Error>;

    /// Transfer ownership of an application.
    async fn transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Result<(), Error>;
}

/// Event-driven application manager using dual messaging streams and distributed leadership.
/// Commands go through the command stream, events are published to the event stream.
/// The view is built by consuming events from the event stream.
/// Only the leader node runs the command service.
pub struct ApplicationManager<CS, ES, LM>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    /// Cached command client
    client: OnceLock<
        <CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Client<ApplicationServiceHandler<ES::Initialized>>,
    >,

    /// Client options for the command client
    client_options: <<CS::Initialized as InitializedStream<
        ApplicationCommand,
        DeserializeError,
        SerializeError,
    >>::Client<ApplicationServiceHandler<ES::Initialized>> as Client<
        ApplicationServiceHandler<ES::Initialized>,
        ApplicationCommand,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// Command stream for processing commands
    command_stream: CS,

    /// Cached consumer (to ensure it only starts once)
    consumer: OnceLock<
        <ES::Initialized as InitializedStream<
            ApplicationEvent,
            DeserializeError,
            SerializeError,
        >>::Consumer<ApplicationViewConsumerHandler>,
    >,

    /// Consumer options for the event consumer
    consumer_options: <<ES::Initialized as InitializedStream<
        ApplicationEvent,
        DeserializeError,
        SerializeError,
    >>::Consumer<ApplicationViewConsumerHandler> as Consumer<
        ApplicationViewConsumerHandler,
        ApplicationEvent,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// Event stream for publishing/consuming events
    event_stream: ES,

    /// Current leadership guard (Some if we're the leader)
    leadership_guard: Arc<Mutex<Option<LM::Guard>>>,

    /// Leadership monitoring task handle
    leadership_monitor: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// Lock manager for leader election
    lock_manager: LM,

    /// Cached service (to ensure it only starts once)
    service: OnceLock<
        <CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Service<ApplicationServiceHandler<ES::Initialized>>,
    >,

    /// Service options for the command service
    service_options: <<CS::Initialized as InitializedStream<
        ApplicationCommand,
        DeserializeError,
        SerializeError,
    >>::Service<ApplicationServiceHandler<ES::Initialized>> as Service<
        ApplicationServiceHandler<ES::Initialized>,
        ApplicationCommand,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// Direct access to the view for queries
    view: ApplicationView,
}

impl<CS, ES, LM> ApplicationManager<CS, ES, LM>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
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
    /// # Arguments
    ///
    /// * `command_stream` - Stream for processing application commands
    /// * `event_stream` - Stream for publishing and consuming application events
    /// * `lock_manager` - Lock manager for distributed leadership coordination
    /// * `service_options` - Options for the command processing service
    /// * `client_options` - Options for the command client
    /// * `consumer_options` - Options for the event consumer
    pub fn new(
        command_stream: CS,
        event_stream: ES,
        service_options: <<CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Service<ApplicationServiceHandler<ES::Initialized>> as Service<
            ApplicationServiceHandler<ES::Initialized>,
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Client<ApplicationServiceHandler<ES::Initialized>> as Client<
            ApplicationServiceHandler<ES::Initialized>,
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Options,
        consumer_options: <<ES::Initialized as InitializedStream<
            ApplicationEvent,
            DeserializeError,
            SerializeError,
        >>::Consumer<ApplicationViewConsumerHandler> as Consumer<
            ApplicationViewConsumerHandler,
            ApplicationEvent,
            DeserializeError,
            SerializeError,
        >>::Options,
        lock_manager: LM,
    ) -> Self {
        let view = ApplicationView::new();

        Self {
            client: OnceLock::new(),
            client_options,
            command_stream,
            consumer: OnceLock::new(),
            consumer_options,
            event_stream,
            leadership_guard: Arc::new(Mutex::new(None)),
            leadership_monitor: Arc::new(Mutex::new(None)),
            lock_manager,
            service: OnceLock::new(),
            service_options,
            view,
        }
    }

    /// Get the application view for direct queries.
    ///
    /// This provides direct access to the in-memory view for fast queries
    /// without going through the request/response cycle.
    pub fn view(&self) -> &ApplicationView {
        &self.view
    }

    /// Get or create the client for sending commands.
    async fn get_client(
        &self,
    ) -> Result<
        <CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Client<ApplicationServiceHandler<ES::Initialized>>,
        Error,
    > {
        if let Some(client) = self.client.get() {
            return Ok(client.clone());
        }

        let command_stream = self
            .command_stream
            .init()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        let event_stream = self
            .event_stream
            .init()
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        // Always ensure consumer is running (all nodes need views)
        self.ensure_consumer_running(&event_stream).await?;

        // Try to acquire leadership and start service
        self.ensure_leadership_and_service(&command_stream, &event_stream)
            .await?;

        let client = command_stream
            .client("APPLICATION_SERVICE", self.client_options.clone())
            .await
            .map_err(|e| Error::Client(e.to_string()))?;

        match self.client.set(client.clone()) {
            Ok(()) => Ok(client),
            Err(_) => {
                // Another thread set it first, use the one that was set
                Ok(self.client.get().unwrap().clone())
            }
        }
    }

    /// Attempt to acquire leadership and start service if successful.
    /// Returns Ok(()) regardless of whether leadership was acquired.
    async fn ensure_leadership_and_service(
        &self,
        command_stream: &CS::Initialized,
        event_stream: &ES::Initialized,
    ) -> Result<(), Error> {
        // Check if service is already running
        if self.service.get().is_some() {
            return Ok(());
        }

        // Check if we already hold leadership
        {
            let guard = self.leadership_guard.lock().await;
            if guard.is_some() {
                // We have leadership but no service - start it
                return self
                    .start_service_as_leader(command_stream, event_stream)
                    .await;
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
                self.start_service_as_leader(command_stream, event_stream)
                    .await?;

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
                    "Leadership acquisition failed: {}",
                    e
                )))
            }
        }
    }

    /// Start the service as the leader.
    async fn start_service_as_leader(
        &self,
        command_stream: &CS::Initialized,
        event_stream: &ES::Initialized,
    ) -> Result<(), Error> {
        if self.service.get().is_some() {
            return Ok(());
        }

        let handler = ApplicationServiceHandler::new(self.view.clone(), event_stream.clone());

        let service = command_stream
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

        *monitor_guard = Some(handle);
        tracing::debug!("Started application service leadership monitor");
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

    /// Ensure the event consumer is running to update the view.
    async fn ensure_consumer_running(&self, event_stream: &ES::Initialized) -> Result<(), Error> {
        if self.consumer.get().is_some() {
            return Ok(());
        }

        let handler = ApplicationViewConsumerHandler::new(self.view.clone());

        let consumer = event_stream
            .consumer("APPLICATION_VIEW", self.consumer_options.clone(), handler)
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Start the consumer to begin processing events
        consumer
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Store the consumer (ignore result if another thread set it first)
        let _ = self.consumer.set(consumer);

        Ok(())
    }
}

impl<CS, ES, LM> Clone for ApplicationManager<CS, ES, LM>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError> + Clone,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError> + Clone,
    LM: LockManager + Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            client_options: self.client_options.clone(),
            command_stream: self.command_stream.clone(),
            consumer: self.consumer.clone(),
            consumer_options: self.consumer_options.clone(),
            event_stream: self.event_stream.clone(),
            leadership_guard: Arc::clone(&self.leadership_guard),
            leadership_monitor: Arc::clone(&self.leadership_monitor),
            lock_manager: self.lock_manager.clone(),
            service: self.service.clone(),
            service_options: self.service_options.clone(),
            view: self.view.clone(),
        }
    }
}

#[async_trait]
impl<CS, ES, LM> ApplicationManagement for ApplicationManager<CS, ES, LM>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    async fn application_exists(&self, application_id: Uuid) -> Result<bool, Error> {
        Ok(self.view.application_exists(application_id).await)
    }

    async fn archive_application(&self, application_id: Uuid) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = ApplicationCommand::ArchiveApplication { application_id };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(ApplicationCommandResponse::ApplicationArchived {
                ..
            }) => Ok(()),
            ClientResponseType::Response(ApplicationCommandResponse::Error { message }) => {
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

        let command = ApplicationCommand::CreateApplication {
            owner_identity_id: options.owner_identity_id,
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(ApplicationCommandResponse::ApplicationCreated {
                application,
                ..
            }) => Ok(application),
            ClientResponseType::Response(ApplicationCommandResponse::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn get_application(&self, application_id: Uuid) -> Result<Option<Application>, Error> {
        // Query directly from view - no request/response overhead
        Ok(self.view.get_application(application_id).await)
    }

    async fn list_applications_by_owner(&self, owner_id: Uuid) -> Result<Vec<Application>, Error> {
        Ok(self.view.list_applications_by_owner(owner_id).await)
    }

    async fn transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Result<(), Error> {
        let client = self.get_client().await?;

        let command = ApplicationCommand::TransferOwnership {
            application_id,
            new_owner_id,
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(ApplicationCommandResponse::OwnershipTransferred {
                ..
            }) => Ok(()),
            ClientResponseType::Response(ApplicationCommandResponse::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures;
    use proven_locks_memory::MemoryLockManager;
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        consumer::MemoryConsumerOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use uuid::Uuid;

    type TestCommandStream = MemoryStream<ApplicationCommand, DeserializeError, SerializeError>;
    type TestEventStream = MemoryStream<ApplicationEvent, DeserializeError, SerializeError>;
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
        assert_eq!(app.owner_identity_id, owner_id);
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
        assert_eq!(retrieved.unwrap().owner_identity_id, owner_id);
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
        assert_eq!(updated_app.owner_identity_id, new_owner_id);
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
        );

        let manager2 = ApplicationManager::new(
            command_stream.clone(),
            event_stream.clone(),
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
            shared_lock_manager.clone(),
        );

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

        // Both should have consumers for the shared event stream
        // Give a moment for the consumer to start
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert!(manager2.consumer.get().is_some()); // Both have consumers for views

        // Both should see the applications in their views (eventually)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(manager1.view().application_count().await, 2);
        assert_eq!(manager2.view().application_count().await, 2);
    }
}
