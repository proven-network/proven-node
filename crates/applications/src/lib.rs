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

use std::sync::OnceLock;

use async_trait::async_trait;
use proven_bootable::Bootable;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

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

/// Event-driven application manager using dual messaging streams.
/// Commands go through the command stream, events are published to the event stream.
/// The view is built by consuming events from the event stream.
#[derive(Clone)]
pub struct ApplicationManager<CS, ES>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
{
    /// Command stream for processing commands
    command_stream: CS,
    /// Event stream for publishing/consuming events
    event_stream: ES,

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

    /// Cached command client
    client: OnceLock<
        <CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Client<ApplicationServiceHandler<ES::Initialized>>,
    >,

    /// Cached service (to ensure it only starts once)
    service: OnceLock<
        <CS::Initialized as InitializedStream<
            ApplicationCommand,
            DeserializeError,
            SerializeError,
        >>::Service<ApplicationServiceHandler<ES::Initialized>>,
    >,

    /// Cached consumer (to ensure it only starts once)
    consumer: OnceLock<
        <ES::Initialized as InitializedStream<
            ApplicationEvent,
            DeserializeError,
            SerializeError,
        >>::Consumer<ApplicationViewConsumerHandler>,
    >,

    /// Direct access to the view for queries
    view: ApplicationView,
}

impl<CS, ES> ApplicationManager<CS, ES>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
{
    /// Create a new application manager with dual streams.
    ///
    /// This creates a manager that uses separate streams for commands and events:
    /// - Commands are sent through the command stream and processed by a service
    /// - Events are published to the event stream and consumed to build the view
    /// - Queries access the view directly for fast performance
    ///
    /// # Arguments
    ///
    /// * `command_stream` - Stream for processing application commands
    /// * `event_stream` - Stream for publishing and consuming application events
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
    ) -> Self {
        let view = ApplicationView::new();

        Self {
            command_stream,
            event_stream,
            service_options,
            client_options,
            consumer_options,
            client: OnceLock::new(),
            service: OnceLock::new(),
            consumer: OnceLock::new(),
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

        // Ensure both service and consumer are running
        self.ensure_service_running(&command_stream, &event_stream)
            .await?;
        self.ensure_consumer_running(&event_stream).await?;

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

    /// Ensure the application command service is running.
    async fn ensure_service_running(
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

        Ok(())
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

#[async_trait]
impl<CS, ES> ApplicationManagement for ApplicationManager<CS, ES>
where
    CS: Stream<ApplicationCommand, DeserializeError, SerializeError>,
    ES: Stream<ApplicationEvent, DeserializeError, SerializeError>,
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
    use proven_messaging_memory::{
        client::MemoryClientOptions,
        consumer::MemoryConsumerOptions,
        service::MemoryServiceOptions,
        stream::{MemoryStream, MemoryStreamOptions},
    };
    use uuid::Uuid;

    type TestCommandStream = MemoryStream<ApplicationCommand, DeserializeError, SerializeError>;
    type TestEventStream = MemoryStream<ApplicationEvent, DeserializeError, SerializeError>;
    type TestApplicationManager = ApplicationManager<TestCommandStream, TestEventStream>;

    async fn create_test_manager() -> TestApplicationManager {
        let stream_name = format!("test-stream-{}", Uuid::new_v4());
        let command_stream =
            MemoryStream::new(stream_name.clone() + "-commands", MemoryStreamOptions);
        let event_stream = MemoryStream::new(stream_name + "-events", MemoryStreamOptions);

        ApplicationManager::new(
            command_stream,
            event_stream,
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
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
}
