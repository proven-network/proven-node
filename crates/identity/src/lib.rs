//! Event-driven identity manager using messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod events;
mod identity;
mod request;
mod response;
mod service_handler;
mod view;

pub use error::Error;
pub use events::IdentityEvent;
pub use identity::Identity;
pub use request::IdentityCommand;
pub use response::IdentityCommandResponse;
pub use service_handler::IdentityServiceHandler;
pub use view::{IdentityView, IdentityViewConsumerHandler};

use std::sync::OnceLock;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Trait for managing identities.
#[async_trait]
pub trait IdentityManagement
where
    Self: Clone + Send + Sync + 'static,
{
    /// Get an identity by its ID.
    async fn get_identity(&self, identity_id: Uuid) -> Result<Option<Identity>, Error>;

    /// Get an existing identity by PRF public key, or create a new one if it doesn't exist.
    async fn get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: &Bytes,
    ) -> Result<Identity, Error>;

    /// Check if an identity exists.
    async fn identity_exists(&self, identity_id: Uuid) -> Result<bool, Error>;

    /// List all identities.
    async fn list_identities(&self) -> Result<Vec<Identity>, Error>;
}

/// Event-driven identity manager using dual messaging streams.
/// Commands go through the command stream, events are published to the event stream.
/// The view is built by consuming events from the event stream.
#[derive(Clone)]
pub struct IdentityManager<CS, ES>
where
    CS: Stream<IdentityCommand, DeserializeError, SerializeError>,
    ES: Stream<IdentityEvent, DeserializeError, SerializeError>,
{
    /// Command stream for processing commands
    command_stream: CS,
    /// Event stream for publishing/consuming events
    event_stream: ES,

    /// Service options for the command service
    service_options: <<CS::Initialized as InitializedStream<
        IdentityCommand,
        DeserializeError,
        SerializeError,
    >>::Service<IdentityServiceHandler<ES::Initialized>> as Service<
        IdentityServiceHandler<ES::Initialized>,
        IdentityCommand,
        DeserializeError,
        SerializeError,
    >>::Options,
    /// Client options for the command client
    client_options: <<CS::Initialized as InitializedStream<
        IdentityCommand,
        DeserializeError,
        SerializeError,
    >>::Client<IdentityServiceHandler<ES::Initialized>> as Client<
        IdentityServiceHandler<ES::Initialized>,
        IdentityCommand,
        DeserializeError,
        SerializeError,
    >>::Options,
    /// Consumer options for the event consumer
    consumer_options: <<ES::Initialized as InitializedStream<
        IdentityEvent,
        DeserializeError,
        SerializeError,
    >>::Consumer<IdentityViewConsumerHandler> as Consumer<
        IdentityViewConsumerHandler,
        IdentityEvent,
        DeserializeError,
        SerializeError,
    >>::Options,

    /// Cached command client
    client:
        OnceLock<
            <CS::Initialized as InitializedStream<
                IdentityCommand,
                DeserializeError,
                SerializeError,
            >>::Client<IdentityServiceHandler<ES::Initialized>>,
        >,

    /// Cached service (to ensure it only starts once)
    service:
        OnceLock<
            <CS::Initialized as InitializedStream<
                IdentityCommand,
                DeserializeError,
                SerializeError,
            >>::Service<IdentityServiceHandler<ES::Initialized>>,
        >,

    /// Cached consumer (to ensure it only starts once)
    consumer:
        OnceLock<
            <ES::Initialized as InitializedStream<
                IdentityEvent,
                DeserializeError,
                SerializeError,
            >>::Consumer<IdentityViewConsumerHandler>,
        >,

    /// Direct access to the view for queries
    view: IdentityView,
}

impl<CS, ES> IdentityManager<CS, ES>
where
    CS: Stream<IdentityCommand, DeserializeError, SerializeError>,
    ES: Stream<IdentityEvent, DeserializeError, SerializeError>,
{
    /// Create a new identity manager with dual streams.
    ///
    /// This creates a manager that uses separate streams for commands and events:
    /// - Commands are sent through the command stream and processed by a service
    /// - Events are published to the event stream and consumed to build the view
    /// - Queries access the view directly for fast performance
    ///
    /// # Arguments
    ///
    /// * `command_stream` - Stream for processing identity commands
    /// * `event_stream` - Stream for publishing and consuming identity events
    /// * `service_options` - Options for the command processing service
    /// * `client_options` - Options for the command client
    /// * `consumer_options` - Options for the event consumer
    pub fn new(
        command_stream: CS,
        event_stream: ES,
        service_options: <<CS::Initialized as InitializedStream<
            IdentityCommand,
            DeserializeError,
            SerializeError,
        >>::Service<IdentityServiceHandler<ES::Initialized>> as Service<
            IdentityServiceHandler<ES::Initialized>,
            IdentityCommand,
            DeserializeError,
            SerializeError,
        >>::Options,
        client_options: <<CS::Initialized as InitializedStream<
            IdentityCommand,
            DeserializeError,
            SerializeError,
        >>::Client<IdentityServiceHandler<ES::Initialized>> as Client<
            IdentityServiceHandler<ES::Initialized>,
            IdentityCommand,
            DeserializeError,
            SerializeError,
        >>::Options,
        consumer_options: <<ES::Initialized as InitializedStream<
            IdentityEvent,
            DeserializeError,
            SerializeError,
        >>::Consumer<IdentityViewConsumerHandler> as Consumer<
            IdentityViewConsumerHandler,
            IdentityEvent,
            DeserializeError,
            SerializeError,
        >>::Options,
    ) -> Self {
        let view = IdentityView::new();
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

    /// Get direct access to the view for queries.
    pub fn view(&self) -> &IdentityView {
        &self.view
    }

    async fn get_client(
        &self,
    ) -> Result<
        <CS::Initialized as InitializedStream<
            IdentityCommand,
            DeserializeError,
            SerializeError,
        >>::Client<IdentityServiceHandler<ES::Initialized>>,
        Error,
    >{
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
            .client("IDENTITY_SERVICE", self.client_options.clone())
            .await
            .map_err(|e| Error::Client(e.to_string()))?;

        // Try to set the client, but if another thread beat us to it, use theirs
        match self.client.set(client.clone()) {
            Ok(()) => Ok(client),
            Err(_) => Ok(self.client.get().unwrap().clone()),
        }
    }

    async fn ensure_service_running(
        &self,
        command_stream: &CS::Initialized,
        event_stream: &ES::Initialized,
    ) -> Result<(), Error> {
        if self.service.get().is_some() {
            return Ok(());
        }

        let handler = IdentityServiceHandler::new(self.view.clone(), event_stream.clone());

        let service = command_stream
            .service("IDENTITY_SERVICE", self.service_options.clone(), handler)
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Start the service to begin processing requests
        service
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Try to set the service, but if another thread beat us to it, that's fine
        let _ = self.service.set(service);
        Ok(())
    }

    async fn ensure_consumer_running(&self, event_stream: &ES::Initialized) -> Result<(), Error> {
        if self.consumer.get().is_some() {
            return Ok(());
        }

        let handler = IdentityViewConsumerHandler::new(self.view.clone());

        let consumer = event_stream
            .consumer("IDENTITY_VIEW", self.consumer_options.clone(), handler)
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Start the consumer to begin processing events
        consumer
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Try to set the consumer, but if another thread beat us to it, that's fine
        let _ = self.consumer.set(consumer);
        Ok(())
    }
}

#[async_trait]
impl<CS, ES> IdentityManagement for IdentityManager<CS, ES>
where
    CS: Stream<IdentityCommand, DeserializeError, SerializeError>,
    ES: Stream<IdentityEvent, DeserializeError, SerializeError>,
{
    async fn get_identity(&self, identity_id: Uuid) -> Result<Option<Identity>, Error> {
        // Query the view directly for fast performance
        Ok(self.view.get_identity(identity_id).await)
    }

    async fn get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: &Bytes,
    ) -> Result<Identity, Error> {
        let client = self.get_client().await?;

        let command = IdentityCommand::GetOrCreateIdentityByPrfPublicKey {
            prf_public_key: prf_public_key.clone(),
        };

        match client
            .request(command)
            .await
            .map_err(|e| Error::Client(e.to_string()))?
        {
            ClientResponseType::Response(IdentityCommandResponse::IdentityRetrieved {
                identity,
            }) => Ok(identity),
            ClientResponseType::Response(IdentityCommandResponse::Error { message }) => {
                Err(Error::Command(message))
            }
            _ => Err(Error::UnexpectedResponse),
        }
    }

    async fn identity_exists(&self, identity_id: Uuid) -> Result<bool, Error> {
        Ok(self.view.identity_exists(identity_id).await)
    }

    async fn list_identities(&self) -> Result<Vec<Identity>, Error> {
        Ok(self.view.list_all_identities().await)
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

    type TestCommandStream = MemoryStream<IdentityCommand, DeserializeError, SerializeError>;
    type TestEventStream = MemoryStream<IdentityEvent, DeserializeError, SerializeError>;
    type TestIdentityManager = IdentityManager<TestCommandStream, TestEventStream>;

    async fn create_test_manager() -> TestIdentityManager {
        let stream_name = format!("test-stream-{}", Uuid::new_v4());
        let command_stream =
            MemoryStream::new(stream_name.clone() + "-commands", MemoryStreamOptions);
        let event_stream = MemoryStream::new(stream_name + "-events", MemoryStreamOptions);

        IdentityManager::new(
            command_stream,
            event_stream,
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
        )
    }

    #[tokio::test]
    async fn test_get_or_create_identity_by_prf_public_key() {
        let manager = create_test_manager().await;
        let prf_public_key = Bytes::from(vec![1u8; 32]);

        // First call should create the identity
        let identity1 = manager
            .get_or_create_identity_by_prf_public_key(&prf_public_key)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Second call should return the same identity
        let identity2 = manager
            .get_or_create_identity_by_prf_public_key(&prf_public_key)
            .await
            .unwrap();

        assert_eq!(identity1.id, identity2.id);
    }

    #[tokio::test]
    async fn test_get_identity_from_view() {
        let manager = create_test_manager().await;
        let prf_public_key = Bytes::from(vec![2u8; 32]);

        let created_identity = manager
            .get_or_create_identity_by_prf_public_key(&prf_public_key)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Get from view
        let retrieved_identity = manager.get_identity(created_identity.id).await.unwrap();
        assert!(retrieved_identity.is_some());
        assert_eq!(retrieved_identity.unwrap().id, created_identity.id);
    }

    #[tokio::test]
    async fn test_direct_view_queries() {
        let manager = create_test_manager().await;

        // Direct view access should work immediately (no async required since view is shared)
        assert_eq!(manager.view().identity_count().await, 0);
        assert_eq!(manager.view().prf_public_key_count().await, 0);

        // Create identities
        let prf1 = Bytes::from(vec![3u8; 32]);
        let prf2 = Bytes::from(vec![4u8; 32]);

        let identity1 = manager
            .get_or_create_identity_by_prf_public_key(&prf1)
            .await
            .unwrap();
        let identity2 = manager
            .get_or_create_identity_by_prf_public_key(&prf2)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Direct view access
        assert_eq!(manager.view().identity_count().await, 2);
        assert_eq!(manager.view().prf_public_key_count().await, 2);
        assert!(manager.view().identity_exists(identity1.id).await);
        assert!(manager.view().identity_exists(identity2.id).await);
        assert!(manager.view().prf_public_key_exists(&prf1).await);
        assert!(manager.view().prf_public_key_exists(&prf2).await);

        let all_identities = manager.list_identities().await.unwrap();
        assert_eq!(all_identities.len(), 2);
    }

    #[tokio::test]
    async fn test_prf_public_key_lookup() {
        let manager = create_test_manager().await;
        let prf_public_key = Bytes::from(vec![5u8; 32]);

        let created_identity = manager
            .get_or_create_identity_by_prf_public_key(&prf_public_key)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Lookup by PRF public key should work
        let found_identity = manager
            .view()
            .get_identity_by_prf_public_key(&prf_public_key)
            .await;
        assert!(found_identity.is_some());
        assert_eq!(found_identity.unwrap().id, created_identity.id);
    }

    #[tokio::test]
    async fn test_concurrent_identity_creation() {
        let manager = create_test_manager().await;

        // Create multiple identities concurrently with different PRF keys
        let tasks: Vec<_> = (0..5)
            .map(|i| {
                let manager = manager.clone();
                tokio::spawn(async move {
                    let prf_key = Bytes::from(vec![i as u8 + 10; 32]);
                    manager
                        .get_or_create_identity_by_prf_public_key(&prf_key)
                        .await
                })
            })
            .collect();

        let results: Vec<_> = futures::future::try_join_all(tasks).await.unwrap();
        assert!(results.iter().all(|r| r.is_ok()));

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let all_identities = manager.list_identities().await.unwrap();
        assert_eq!(all_identities.len(), 5);
        assert_eq!(manager.view().prf_public_key_count().await, 5);
    }
}
