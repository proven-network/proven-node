//! Service pattern implementation for command processing.
//!
//! This module implements the request/response pattern on top of engine streams,
//! replacing the messaging service abstraction.

use std::collections::HashMap;
use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proven_engine::{Client, StoredMessage};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{Command, Error, Event, Response, view::IdentityView};

/// Metadata keys for request/response correlation
const REQUEST_ID_KEY: &str = "request_id";
const RESPONSE_TO_KEY: &str = "response_to";
const REQUEST_TYPE_KEY: &str = "request_type";

/// Request wrapper for commands sent via stream
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamRequest {
    /// Unique request ID for correlation
    id: Uuid,
    /// The actual command
    command: Command,
    /// Node that sent the request (for response routing)
    from_node: String,
}

/// Response wrapper for responses sent via stream
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamResponse {
    /// Request ID this is responding to
    request_id: Uuid,
    /// The actual response
    response: Response,
}

/// Handler for processing commands
pub struct CommandServiceHandler<T, G, S>
where
    T: proven_transport::Transport,
    G: proven_topology::TopologyAdaptor,
    S: proven_storage::StorageAdaptor,
{
    /// Identity view for validation
    view: IdentityView,
    /// Engine client for publishing events
    client: Arc<Client<T, G, S>>,
    /// Event stream name
    event_stream: String,
}

impl<T, G, S> CommandServiceHandler<T, G, S>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    S: proven_storage::StorageAdaptor + 'static,
{
    /// Create a new command service handler.
    pub const fn new(
        view: IdentityView,
        client: Arc<Client<T, G, S>>,
        event_stream: String,
    ) -> Self {
        Self {
            view,
            client,
            event_stream,
        }
    }

    /// Handle a command and return a response.
    pub async fn handle_command(&self, command: Command) -> Response {
        // Ensure view is up to date for commands that need it
        if Self::requires_strong_consistency(&command) {
            // Get last event sequence
            match self.client.get_stream_info(&self.event_stream).await {
                Ok(Some(info)) => {
                    self.view.wait_for_seq(info.last_sequence.get()).await;
                }
                Ok(None) => {
                    return Response::InternalError {
                        message: "Event stream not found".to_string(),
                    };
                }
                Err(e) => {
                    return Response::InternalError {
                        message: e.to_string(),
                    };
                }
            }
        }

        // Process the command
        match command {
            Command::CreateIdentityWithPrfPublicKey { prf_public_key } => {
                self.handle_create_identity_with_prf_public_key(prf_public_key)
                    .await
            }
            Command::LinkPrfPublicKey {
                identity_id,
                prf_public_key,
            } => {
                self.handle_link_prf_public_key(identity_id, prf_public_key)
                    .await
            }
        }
    }

    /// Check if a command requires strong consistency.
    const fn requires_strong_consistency(command: &Command) -> bool {
        match command {
            Command::CreateIdentityWithPrfPublicKey { .. } => false,
            Command::LinkPrfPublicKey { .. } => true,
        }
    }

    /// Publish events to the event stream.
    async fn publish_events(&self, events: Vec<Event>) -> Result<u64, String> {
        // Publish events individually (engine doesn't support batch publishing yet)
        for event in events {
            let mut payload = Vec::new();
            ciborium::ser::into_writer(&event, &mut payload)
                .map_err(|e| format!("Failed to serialize event: {e}"))?;

            self.client
                .publish(self.event_stream.clone(), payload, None)
                .await
                .map_err(|e| format!("Failed to publish event: {e}"))?;
        }

        // Get stream info to return the last sequence number
        match self.client.get_stream_info(&self.event_stream).await {
            Ok(Some(info)) => Ok(info.last_sequence.get()),
            Ok(None) => Err("Event stream not found".to_string()),
            Err(e) => Err(e.to_string()),
        }
    }

    /// Handle `CreateIdentityWithPrfPublicKey` command.
    async fn handle_create_identity_with_prf_public_key(&self, prf_public_key: Bytes) -> Response {
        // Check if identity already exists for this PRF public key
        if self.view.prf_public_key_exists(&prf_public_key).await {
            return Response::Error {
                message: "PRF public key already associated with an identity".to_string(),
            };
        }

        // Create new identity
        let identity_id = Uuid::new_v4();
        let identity = crate::Identity::new(identity_id);

        // Create events
        let events = vec![
            Event::Created {
                identity_id,
                created_at: chrono::Utc::now(),
            },
            Event::PrfPublicKeyLinked {
                identity_id,
                prf_public_key,
                linked_at: chrono::Utc::now(),
            },
        ];

        // Publish events
        match self.publish_events(events).await {
            Ok(last_event_seq) => Response::IdentityCreated {
                identity,
                last_event_seq,
            },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    /// Handle `LinkPrfPublicKey` command.
    async fn handle_link_prf_public_key(
        &self,
        identity_id: Uuid,
        prf_public_key: Bytes,
    ) -> Response {
        // Validate identity exists
        if !self.view.identity_exists(&identity_id).await {
            return Response::Error {
                message: "Identity not found".to_string(),
            };
        }

        // Check if PRF public key is already linked
        if self.view.prf_public_key_exists(&prf_public_key).await {
            return Response::Error {
                message: "PRF public key already linked to an identity".to_string(),
            };
        }

        // Create event
        let event = Event::PrfPublicKeyLinked {
            identity_id,
            prf_public_key,
            linked_at: chrono::Utc::now(),
        };

        // Publish event
        match self.publish_events(vec![event]).await {
            Ok(last_event_seq) => Response::PrfPublicKeyLinked { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }
}

/// Service that processes commands from the command stream
pub struct CommandService {
    /// Background task handle
    handle: Option<JoinHandle<()>>,
    /// Shutdown channel
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl CommandService {
    /// Create and start a new command service.
    pub fn new<T, G, S>(
        client: Arc<Client<T, G, S>>,
        command_stream: String,
        handler: Arc<CommandServiceHandler<T, G, S>>,
    ) -> Self
    where
        T: proven_transport::Transport + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        S: proven_storage::StorageAdaptor + 'static,
    {
        let mut service = Self {
            handle: None,
            shutdown_tx: None,
        };

        service.start(client, command_stream, handler);

        service
    }

    /// Start the service.
    pub fn start<T, G, S>(
        &mut self,
        client: Arc<Client<T, G, S>>,
        command_stream: String,
        handler: Arc<CommandServiceHandler<T, G, S>>,
    ) where
        T: proven_transport::Transport + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        S: proven_storage::StorageAdaptor + 'static,
    {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let handle = tokio::spawn(async move {
            if let Err(e) =
                Self::run_service_loop(client, command_stream, handler, shutdown_rx).await
            {
                tracing::error!("Command service error: {}", e);
            }
        });

        self.handle = Some(handle);
    }

    /// Stop the service.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Run the service loop.
    #[allow(clippy::cognitive_complexity)]
    async fn run_service_loop<T, G, S>(
        client: Arc<Client<T, G, S>>,
        command_stream: String,
        handler: Arc<CommandServiceHandler<T, G, S>>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Error>
    where
        T: proven_transport::Transport + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        S: proven_storage::StorageAdaptor + 'static,
    {
        // Track last processed sequence
        let mut last_sequence = 0u64;

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                tracing::info!("Command service shutting down");
                break;
            }

            // Read next batch of messages
            let start_seq = NonZero::new(last_sequence + 1).unwrap();
            let count = NonZero::new(10).unwrap(); // Process up to 10 at a time

            match client
                .read_stream(command_stream.clone(), start_seq, count)
                .await
            {
                Ok(messages) => {
                    if messages.is_empty() {
                        // No new messages, wait a bit
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    for message in messages {
                        last_sequence = message.sequence.get();

                        // Process the message
                        if let Err(e) =
                            Self::process_message(&client, &command_stream, &handler, message).await
                        {
                            tracing::error!("Failed to process message: {}", e);
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to read command stream: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }

    /// Process a single command message.
    async fn process_message<T, G, S>(
        client: &Arc<Client<T, G, S>>,
        command_stream: &str,
        handler: &Arc<CommandServiceHandler<T, G, S>>,
        message: StoredMessage,
    ) -> Result<(), Error>
    where
        T: proven_transport::Transport + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        S: proven_storage::StorageAdaptor + 'static,
    {
        // Check if this is a request
        let headers: HashMap<String, String> = message
            .data
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        if headers
            .get(REQUEST_TYPE_KEY)
            .map(std::string::String::as_str)
            == Some("command")
        {
            // Deserialize request
            let request: StreamRequest = ciborium::de::from_reader(&message.data.payload[..])
                .map_err(|e| Error::Deserialization(e.to_string()))?;

            // Process command
            let response = handler.handle_command(request.command).await;

            // Send response if needed
            if let Some(request_id) = headers.get(REQUEST_ID_KEY) {
                let stream_response = StreamResponse {
                    request_id: Uuid::parse_str(request_id)
                        .map_err(|e| Error::Deserialization(e.to_string()))?,
                    response,
                };

                // Serialize response
                let mut payload = Vec::new();
                ciborium::ser::into_writer(&stream_response, &mut payload)?;

                // Create response metadata
                let mut response_metadata = HashMap::new();
                response_metadata.insert(RESPONSE_TO_KEY.to_string(), request_id.clone());
                response_metadata.insert(REQUEST_TYPE_KEY.to_string(), "response".to_string());

                // Publish response
                client
                    .publish(command_stream.to_string(), payload, Some(response_metadata))
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;
            }

            // Delete the processed message
            client
                .delete_message(command_stream.to_string(), message.sequence)
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;
        }

        Ok(())
    }
}

/// Execute a command via the stream and wait for response.
pub async fn execute_command_via_stream<T, G, S>(
    client: &Arc<Client<T, G, S>>,
    command_stream: &str,
    command: Command,
    timeout_duration: Duration,
) -> Result<Response, Error>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    S: proven_storage::StorageAdaptor + 'static,
{
    let request_id = Uuid::new_v4();

    // Create request
    let request = StreamRequest {
        id: request_id,
        command,
        from_node: client.node_id().to_string(),
    };

    // Serialize request
    let mut payload = Vec::new();
    ciborium::ser::into_writer(&request, &mut payload)?;

    // Create metadata
    let mut metadata = HashMap::new();
    metadata.insert(REQUEST_ID_KEY.to_string(), request_id.to_string());
    metadata.insert(REQUEST_TYPE_KEY.to_string(), "command".to_string());

    // Set up response listener before sending request
    let (response_tx, response_rx) = oneshot::channel();

    // Start listening for response in background
    let client_clone = client.clone();
    let command_stream_clone = command_stream.to_string();
    let listen_handle = tokio::spawn(async move {
        listen_for_response(client_clone, command_stream_clone, request_id, response_tx).await;
    });

    // Publish request
    client
        .publish(command_stream.to_string(), payload, Some(metadata))
        .await
        .map_err(|e| Error::Stream(e.to_string()))?;

    // Wait for response with timeout
    match timeout(timeout_duration, response_rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
            listen_handle.abort();
            Err(Error::Service("Response channel closed".to_string()))
        }
        Err(_) => {
            listen_handle.abort();
            Err(Error::Timeout)
        }
    }
}

/// Listen for a response to a specific request.
async fn listen_for_response<T, G, S>(
    client: Arc<Client<T, G, S>>,
    command_stream: String,
    request_id: Uuid,
    response_tx: oneshot::Sender<Response>,
) where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    S: proven_storage::StorageAdaptor + 'static,
{
    // Start from the beginning of the stream to find our response
    // We could optimize this by tracking message positions, but for now this works
    let start_seq = NonZero::new(1).unwrap();

    // Use follow mode to wait for our response
    let mut stream = match client
        .stream_messages(command_stream.clone(), start_seq, None)
        .await
    {
        Ok(s) => s.follow(),
        Err(e) => {
            tracing::error!("Failed to create stream for response listening: {}", e);
            return;
        }
    };

    while let Some(result) = tokio_stream::StreamExt::next(&mut stream).await {
        match result {
            Ok(message) => {
                let headers: HashMap<String, String> = message
                    .data
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                if headers
                    .get(RESPONSE_TO_KEY)
                    .map(std::string::String::as_str)
                    == Some(&request_id.to_string())
                    && headers
                        .get(REQUEST_TYPE_KEY)
                        .map(std::string::String::as_str)
                        == Some("response")
                {
                    // Found our response!
                    if let Ok(stream_response) =
                        ciborium::de::from_reader::<StreamResponse, _>(&message.data.payload[..])
                        && stream_response.request_id == request_id
                    {
                        let _ = response_tx.send(stream_response.response);
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error reading response stream: {}", e);
                // Continue listening after errors
            }
        }
    }
}
