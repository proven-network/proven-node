//! Event-driven identity manager using messaging primitives.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod command;
mod error;
mod event;
mod identity;
mod response;
mod service_handler;
mod view;

pub use command::Command;
pub use error::Error;
pub use event::Event;
pub use identity::Identity;
pub use response::Response;
pub use service_handler::IdentityServiceHandler;
pub use view::{IdentityView, IdentityViewConsumerHandler};

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use proven_bootable::Bootable;
use proven_locks::LockManager;
use proven_messaging::client::{Client, ClientResponseType};
use proven_messaging::consumer::Consumer;
use proven_messaging::service::Service;
use proven_messaging::stream::{InitializedStream, Stream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

// Type aliases to simplify complex generic signatures
type InitializedCommandStream<CS> =
    <CS as Stream<Command, DeserializeError, SerializeError>>::Initialized;
type InitializedEventStream<ES> =
    <ES as Stream<Event, DeserializeError, SerializeError>>::Initialized;

type ServiceHandler<ES> = IdentityServiceHandler<InitializedEventStream<ES>>;

type CommandClient<CS, ES> = <InitializedCommandStream<CS> as InitializedStream<
    Command,
    DeserializeError,
    SerializeError,
>>::Client<ServiceHandler<ES>>;

type CommandService<CS, ES> = <InitializedCommandStream<CS> as InitializedStream<
    Command,
    DeserializeError,
    SerializeError,
>>::Service<ServiceHandler<ES>>;

type EventConsumer<ES> = <InitializedEventStream<ES> as InitializedStream<
    Event,
    DeserializeError,
    SerializeError,
>>::Consumer<IdentityViewConsumerHandler>;

type ClientOptions<CS, ES> = <CommandClient<CS, ES> as Client<
    ServiceHandler<ES>,
    Command,
    DeserializeError,
    SerializeError,
>>::Options;

type ServiceOptions<CS, ES> = <CommandService<CS, ES> as Service<
    ServiceHandler<ES>,
    Command,
    DeserializeError,
    SerializeError,
>>::Options;

type ConsumerOptions<ES> = <EventConsumer<ES> as Consumer<
    IdentityViewConsumerHandler,
    Event,
    DeserializeError,
    SerializeError,
>>::Options;

/// Lock resource ID for identity service leadership
const IDENTITY_SERVICE_LEADER_LOCK: &str = "identity_service_leader";

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

/// Event-driven identity manager using dual messaging streams and distributed leadership.
///
/// Commands go through the command stream, events are published to the event stream.
/// The view is built by consuming events from the event stream.
/// Only the leader node runs the command service.
pub struct IdentityManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    /// Cached command client
    client: Arc<OnceLock<CommandClient<CS, ES>>>,

    /// Client options for the command client
    client_options: ClientOptions<CS, ES>,

    /// Initialized command stream for processing commands
    command_stream: InitializedCommandStream<CS>,

    /// Event consumer (started during initialization)
    consumer: EventConsumer<ES>,

    /// Initialized event stream for publishing/consuming events
    event_stream: InitializedEventStream<ES>,

    /// Direct reference to service handler for optimized leader execution
    handler: Arc<OnceLock<ServiceHandler<ES>>>,

    /// Leadership guard (Some when this node is the leader)
    leadership_guard: Arc<Mutex<Option<LM::Guard>>>,

    /// Leadership monitor task handle
    leadership_monitor: Arc<Mutex<Option<JoinHandle<()>>>>,

    /// Lock manager for distributed leadership
    lock_manager: LM,

    /// Cached service
    service: Arc<OnceLock<CommandService<CS, ES>>>,

    /// Service options for the command service
    service_options: ServiceOptions<CS, ES>,

    /// Direct access to the view for queries
    view: IdentityView,
}

impl<CS, ES, LM> IdentityManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    /// Create a new identity manager with dual streams and distributed leadership.
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
        service_options: ServiceOptions<CS, ES>,
        client_options: ClientOptions<CS, ES>,
        consumer_options: ConsumerOptions<ES>,
        lock_manager: LM,
    ) -> Result<Self, Error> {
        let view = IdentityView::new();

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
        let handler = IdentityViewConsumerHandler::new(view.clone());
        let consumer = event_stream
            .consumer("IDENTITY_VIEW", consumer_options, handler)
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
            handler: Arc::new(OnceLock::new()),
            leadership_guard: Arc::new(Mutex::new(None)),
            leadership_monitor: Arc::new(Mutex::new(None)),
            lock_manager,
            service: Arc::new(OnceLock::new()),
            service_options,
            view,
        })
    }

    /// Get direct access to the view for queries.
    pub const fn view(&self) -> &IdentityView {
        &self.view
    }

    /// Get or create the client for sending commands.
    async fn get_client(&self) -> Result<CommandClient<CS, ES>, Error> {
        if let Some(client) = OnceLock::get(&self.client) {
            return Ok(client.clone());
        }

        // Try to acquire leadership and start service
        self.ensure_leadership_and_service().await?;

        let client = self
            .command_stream
            .client("IDENTITY_SERVICE", self.client_options.clone())
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

        // Try to acquire leadership with retry logic for robustness against transient failures
        self.try_acquire_leadership_with_retry().await
    }

    /// Try to acquire leadership with exponential backoff retry for transient failures.
    #[allow(clippy::cognitive_complexity)]
    async fn try_acquire_leadership_with_retry(&self) -> Result<(), Error> {
        let max_attempts = 3;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(100);
        let max_delay = Duration::from_secs(2);

        while attempt < max_attempts {
            attempt += 1;

            match self
                .lock_manager
                .try_lock(IDENTITY_SERVICE_LEADER_LOCK.to_string())
                .await
            {
                Ok(Some(guard)) => {
                    tracing::info!(
                        "Acquired identity service leadership on attempt {}",
                        attempt
                    );

                    // Store the leadership guard
                    {
                        let mut leadership_guard = self.leadership_guard.lock().await;
                        *leadership_guard = Some(guard);
                    }

                    // Start the service
                    self.start_service_as_leader().await?;

                    // Start leadership monitoring
                    self.start_leadership_monitor().await;

                    return Ok(());
                }
                Ok(None) => {
                    tracing::debug!("Another node holds identity service leadership");
                    return Ok(());
                }
                Err(e) => {
                    if attempt < max_attempts {
                        // Calculate delay with exponential backoff and jitter
                        let delay = std::cmp::min(base_delay * 2_u32.pow(attempt - 1), max_delay);

                        // Add jitter to prevent thundering herd
                        #[allow(clippy::cast_possible_truncation)]
                        #[allow(clippy::cast_sign_loss)]
                        #[allow(clippy::cast_precision_loss)]
                        let jitter = Duration::from_millis(
                            (delay.as_millis() as f64 * fastrand::f64() * 0.1) as u64,
                        );
                        let total_delay = delay + jitter;

                        tracing::warn!(
                            "Failed to acquire identity service leadership on attempt {} ({}), retrying in {:?}: {:?}",
                            attempt,
                            max_attempts,
                            total_delay,
                            e
                        );

                        tokio::time::sleep(total_delay).await;
                    } else {
                        tracing::error!(
                            "Failed to acquire identity service leadership after {} attempts: {:?}",
                            max_attempts,
                            e
                        );
                        return Err(Error::Service(format!(
                            "Leadership acquisition failed after {max_attempts} attempts: {e}"
                        )));
                    }
                }
            }
        }

        unreachable!()
    }

    /// Start the service as the leader.
    async fn start_service_as_leader(&self) -> Result<(), Error> {
        if OnceLock::get(&self.service).is_some() {
            return Ok(());
        }

        let handler = IdentityServiceHandler::new(self.view.clone(), self.event_stream.clone());

        let service = self
            .command_stream
            .service(
                "IDENTITY_SERVICE",
                self.service_options.clone(),
                handler.clone(),
            )
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Start the service to begin processing requests
        service
            .start()
            .await
            .map_err(|e| Error::Service(e.to_string()))?;

        // Store both the service and handler for direct access
        let _ = self.service.set(service);
        let _ = self.handler.set(handler);

        tracing::info!("Identity service started as leader");
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

        tracing::debug!("Started identity service leadership monitor");
        *monitor_guard = Some(handle);
    }

    /// Background task to monitor leadership status.
    #[allow(clippy::cognitive_complexity)]
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
                tracing::warn!("Lost identity service leadership");

                // Try to reacquire leadership
                match lock_manager
                    .try_lock(IDENTITY_SERVICE_LEADER_LOCK.to_string())
                    .await
                {
                    Ok(Some(new_guard)) => {
                        tracing::info!("Reacquired identity service leadership");
                        let mut guard = leadership_guard.lock().await;
                        *guard = Some(new_guard);
                        // Note: Service is already running, no need to restart
                    }
                    Ok(None) => {
                        tracing::debug!("Another node still holds identity service leadership");
                    }
                    Err(e) => {
                        tracing::error!("Failed to reacquire identity service leadership: {:?}", e);
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

    /// Execute a command directly via the service handler if we're the leader,
    /// otherwise fall back to the client path with retry logic. This optimization
    /// bypasses the command stream for better performance when we're the leader.
    async fn execute_command(&self, command: Command) -> Result<Response, Error> {
        // Check if we're the leader and have a cached handler
        if self.is_leader().await
            && let Some(handler) = OnceLock::get(&self.handler)
        {
            // Direct execution path - bypass command stream entirely
            return Ok(handler.handle_command(command).await);
        }

        // Fallback to client path with retry logic (non-leader or handler not available)
        self.execute_command_via_client_with_retry(command).await
    }

    /// Execute a command via the client path with retry logic for transient failures.
    #[allow(clippy::cognitive_complexity)]
    async fn execute_command_via_client_with_retry(
        &self,
        command: Command,
    ) -> Result<Response, Error> {
        let max_attempts = 3;
        let mut attempt = 0;
        let base_delay = Duration::from_millis(100);
        let max_delay = Duration::from_secs(2);

        while attempt < max_attempts {
            attempt += 1;

            let client = self.get_client().await?;

            match client.request(command.clone()).await {
                Ok(ClientResponseType::Response(response)) => {
                    if attempt > 1 {
                        tracing::info!(
                            "Command executed successfully via client on attempt {}",
                            attempt
                        );
                    }
                    return Ok(response);
                }
                Ok(ClientResponseType::Stream(_)) => {
                    return Err(Error::UnexpectedResponse);
                }
                Err(e) => {
                    if attempt < max_attempts && Self::is_retriable_client_error(&e) {
                        // Calculate delay with exponential backoff and jitter
                        let delay = std::cmp::min(base_delay * 2_u32.pow(attempt - 1), max_delay);

                        // Add jitter to prevent thundering herd
                        #[allow(clippy::cast_possible_truncation)]
                        #[allow(clippy::cast_sign_loss)]
                        #[allow(clippy::cast_precision_loss)]
                        let jitter = Duration::from_millis(
                            (delay.as_millis() as f64 * fastrand::f64() * 0.1) as u64,
                        );
                        let total_delay = delay + jitter;

                        tracing::warn!(
                            "Command execution failed via client on attempt {} ({}), retrying in {:?}: {:?}",
                            attempt,
                            max_attempts,
                            total_delay,
                            e
                        );

                        tokio::time::sleep(total_delay).await;
                    } else {
                        tracing::error!(
                            "Command execution failed via client after {} attempts: {:?}",
                            max_attempts,
                            e
                        );
                        return Err(Error::Client(e.to_string()));
                    }
                }
            }
        }

        unreachable!()
    }

    /// Check if a client error is retriable (transient network/timeout issues).
    fn is_retriable_client_error(error: &dyn std::error::Error) -> bool {
        let error_string = error.to_string().to_lowercase();

        // Check for common transient error patterns
        error_string.contains("timeout")
            || error_string.contains("connection")
            || error_string.contains("network")
            || error_string.contains("temporarily unavailable")
            || error_string.contains("service unavailable")
            || error_string.contains("try again")
            || error_string.contains("jetstream request timed out")
            || error_string.contains("no responders")
            || error_string.contains("request timeout")
    }
}

impl<CS, ES, LM> Clone for IdentityManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    InitializedCommandStream<CS>: Clone,
    ES: Stream<Event, DeserializeError, SerializeError>,
    InitializedEventStream<ES>: Clone,
    EventConsumer<ES>: Clone,
    ClientOptions<CS, ES>: Clone,
    ServiceOptions<CS, ES>: Clone,
    LM: LockManager + Clone,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            client_options: self.client_options.clone(),
            command_stream: self.command_stream.clone(),
            consumer: self.consumer.clone(),
            event_stream: self.event_stream.clone(),
            handler: Arc::clone(&self.handler),
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
impl<CS, ES, LM> IdentityManagement for IdentityManager<CS, ES, LM>
where
    CS: Stream<Command, DeserializeError, SerializeError>,
    ES: Stream<Event, DeserializeError, SerializeError>,
    LM: LockManager + Clone,
{
    async fn get_identity(&self, identity_id: &Uuid) -> Result<Option<Identity>, Error> {
        // Query the view directly for fast performance
        Ok(self.view.get_identity(identity_id).await)
    }

    async fn get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: &Bytes,
    ) -> Result<Identity, Error> {
        let command = Command::GetOrCreateIdentityByPrfPublicKey {
            prf_public_key: prf_public_key.clone(),
        };

        match self.execute_command(command).await? {
            Response::IdentityRetrieved { identity, .. } => Ok(identity),
            Response::Error { message } | Response::InternalError { message } => {
                Err(Error::Command(message))
            }
            Response::PrfPublicKeyLinked { .. } => Err(Error::UnexpectedResponse),
        }
    }

    async fn identity_exists(&self, identity_id: &Uuid) -> Result<bool, Error> {
        Ok(self.view.identity_exists(identity_id).await)
    }

    async fn list_identities(&self) -> Result<Vec<Identity>, Error> {
        Ok(self.view.list_all_identities().await)
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
    use uuid::Uuid;

    type TestCommandStream = MemoryStream<Command, DeserializeError, SerializeError>;
    type TestEventStream = MemoryStream<Event, DeserializeError, SerializeError>;
    type TestIdentityManager =
        IdentityManager<TestCommandStream, TestEventStream, MemoryLockManager>;

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
            MemoryLockManager::new(),
        )
        .await
        .unwrap()
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
        let retrieved_identity = manager.get_identity(&created_identity.id).await.unwrap();
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
        assert!(manager.view().identity_exists(&identity1.id).await);
        assert!(manager.view().identity_exists(&identity2.id).await);
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
                    #[allow(clippy::cast_possible_truncation)]
                    #[allow(clippy::cast_sign_loss)]
                    let prf_key = Bytes::from(vec![i as u8 + 10; 32]);
                    manager
                        .get_or_create_identity_by_prf_public_key(&prf_key)
                        .await
                })
            })
            .collect();

        let results: Vec<_> = futures::future::try_join_all(tasks).await.unwrap();
        assert!(results.iter().all(Result::is_ok));

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let all_identities = manager.list_identities().await.unwrap();
        assert_eq!(all_identities.len(), 5);
        assert_eq!(manager.view().prf_public_key_count().await, 5);
    }

    #[tokio::test]
    async fn test_last_processed_seq_tracking() {
        let manager = create_test_manager().await;

        // Initially, no events processed
        assert_eq!(manager.view().last_processed_seq(), 0);

        // Create identity (publishes 2 events via publish_batch: Created + PrfPublicKeyLinked)
        let prf_key1 = Bytes::from(vec![1u8; 32]);
        let _identity1 = manager
            .get_or_create_identity_by_prf_public_key(&prf_key1)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should have processed sequence 2 (last of the 2 batched events: seq 1 and 2)
        assert_eq!(manager.view().last_processed_seq(), 2);

        // Create another identity (publishes 2 more events)
        let prf_key2 = Bytes::from(vec![2u8; 32]);
        let _identity2 = manager
            .get_or_create_identity_by_prf_public_key(&prf_key2)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should have processed sequence 4 (last of the next 2 batched events: seq 3 and 4)
        assert_eq!(manager.view().last_processed_seq(), 4);
    }

    #[tokio::test]
    async fn test_strong_consistency_commands() {
        let manager = create_test_manager().await;

        // Both identity command variants require strong consistency
        // Test GetOrCreateIdentityByPrfPublicKey (requires strong consistency)
        let prf_key1 = Bytes::from(vec![1u8; 32]);
        let identity1 = manager
            .get_or_create_identity_by_prf_public_key(&prf_key1)
            .await
            .unwrap();

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Verify identity was created and is in view
        assert!(manager.view().identity_exists(&identity1.id).await);
        assert!(manager.view().prf_public_key_exists(&prf_key1).await);

        // Test that getting the same PRF key returns the same identity (no duplicate creation)
        let identity1_again = manager
            .get_or_create_identity_by_prf_public_key(&prf_key1)
            .await
            .unwrap();

        assert_eq!(identity1.id, identity1_again.id);

        // Give event processing time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Should still only have 1 identity (no duplicate was created)
        assert_eq!(manager.view().identity_count().await, 1);
        assert_eq!(manager.view().prf_public_key_count().await, 1);
    }

    #[tokio::test]
    async fn test_leadership_functionality() {
        let manager = create_test_manager().await;

        // Initially not a leader
        assert!(!manager.is_leader().await);

        // Trigger client creation which should attempt leadership acquisition
        let prf_key = Bytes::from(vec![1u8; 32]);
        let result = manager
            .get_or_create_identity_by_prf_public_key(&prf_key)
            .await;

        assert!(result.is_ok());

        // Should now be leader (memory lock manager allows single acquisition)
        assert!(manager.is_leader().await);

        // Service should be running
        assert!(manager.service.get().is_some());
    }

    #[tokio::test]
    async fn test_multi_node_leadership() {
        // Create shared streams and lock manager
        let shared_lock_manager = MemoryLockManager::new();
        let stream_name = format!("test-shared-stream-{}", Uuid::new_v4());
        let command_stream =
            MemoryStream::new(stream_name.clone() + "-commands", MemoryStreamOptions);
        let event_stream = MemoryStream::new(stream_name + "-events", MemoryStreamOptions);

        // Create two managers sharing the same streams and lock manager (simulating different nodes)
        let manager1 = IdentityManager::new(
            command_stream.clone(),
            event_stream.clone(),
            MemoryServiceOptions,
            MemoryClientOptions,
            MemoryConsumerOptions,
            shared_lock_manager.clone(),
        )
        .await
        .unwrap();

        let manager2 = IdentityManager::new(
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

        // First manager creates an identity (should become leader)
        let prf_key1 = Bytes::from(vec![1u8; 32]);
        let result1 = manager1
            .get_or_create_identity_by_prf_public_key(&prf_key1)
            .await;

        assert!(result1.is_ok());
        assert!(manager1.is_leader().await);

        // Second manager tries to create an identity
        let prf_key2 = Bytes::from(vec![2u8; 32]);
        let result2 = manager2
            .get_or_create_identity_by_prf_public_key(&prf_key2)
            .await;

        assert!(result2.is_ok());

        // Manager1 should still be the leader, manager2 should not become leader
        assert!(manager1.is_leader().await);
        assert!(!manager2.is_leader().await);

        // Manager1 should have the service, manager2 should not
        assert!(manager1.service.get().is_some()); // Leader has service

        // Both should see the identities in their views (eventually)
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert_eq!(manager1.view().identity_count().await, 2);
        assert_eq!(manager2.view().identity_count().await, 2);
    }
}
