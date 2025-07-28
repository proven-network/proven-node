//! Service pattern implementation for command processing.
//!
//! This module implements the request/response pattern on top of engine streams,
//! replacing the messaging service abstraction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use proven_engine::{Client, Message};
use proven_storage::LogIndex;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{Command, Error, Event, Response, view::ApplicationView};

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
pub struct CommandServiceHandler {
    /// Application view for validation
    view: ApplicationView,
    /// Engine client for publishing events
    client: Arc<Client>,
    /// Event stream name
    event_stream: String,
}

impl CommandServiceHandler {
    /// Create a new command service handler.
    pub const fn new(view: ApplicationView, client: Arc<Client>, event_stream: String) -> Self {
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
            match self.client.get_stream_state(&self.event_stream).await {
                Ok(Some(state)) => {
                    if let Some(last_seq) = state.last_sequence {
                        self.view.wait_for_seq(last_seq.get()).await;
                    }
                }
                Ok(None) => {
                    return Response::InternalError {
                        message: "Event stream state not found".to_string(),
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
            Command::AddAllowedOrigin {
                application_id,
                origin,
            } => self.handle_add_allowed_origin(application_id, origin).await,
            Command::Archive { application_id } => self.handle_archive(application_id).await,
            Command::Create { owner_identity_id } => self.handle_create(owner_identity_id).await,
            Command::LinkHttpDomain {
                application_id,
                http_domain,
            } => {
                self.handle_link_http_domain(application_id, http_domain)
                    .await
            }
            Command::RemoveAllowedOrigin {
                application_id,
                origin,
            } => {
                self.handle_remove_allowed_origin(application_id, origin)
                    .await
            }
            Command::TransferOwnership {
                application_id,
                new_owner_id,
            } => {
                self.handle_transfer_ownership(application_id, new_owner_id)
                    .await
            }
            Command::UnlinkHttpDomain {
                application_id,
                http_domain,
            } => {
                self.handle_unlink_http_domain(application_id, http_domain)
                    .await
            }
        }
    }

    /// Check if a command requires strong consistency.
    const fn requires_strong_consistency(command: &Command) -> bool {
        match command {
            Command::AddAllowedOrigin { .. }
            | Command::Archive { .. }
            | Command::RemoveAllowedOrigin { .. }
            | Command::TransferOwnership { .. }
            | Command::LinkHttpDomain { .. }
            | Command::UnlinkHttpDomain { .. } => true,
            Command::Create { .. } => false,
        }
    }

    /// Publish an event to the event stream.
    async fn publish_event(&self, event: Event) -> Result<u64, String> {
        // Serialize event
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&event, &mut payload)
            .map_err(|e| format!("Failed to serialize event: {e}"))?;

        // Publish to event stream
        let message = proven_engine::Message::new(payload);
        match self
            .client
            .publish_to_stream(self.event_stream.clone(), vec![message])
            .await
        {
            Ok(seq) => Ok(seq.get()),
            Err(e) => Err(e.to_string()),
        }
    }

    // Command handlers (same logic as before)

    async fn handle_add_allowed_origin(
        &self,
        application_id: Uuid,
        origin: proven_util::Origin,
    ) -> Response {
        // Validate application exists
        if !self.view.application_exists(&application_id).await {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        // Check if origin is already allowed
        if let Some(app) = self.view.get_application(&application_id).await
            && app.allowed_origins.contains(&origin)
        {
            return Response::Error {
                message: "Origin already in allowed origins".to_string(),
            };
        }

        let event = Event::AllowedOriginAdded {
            application_id,
            origin,
            added_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::AllowedOriginAdded { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_archive(&self, application_id: Uuid) -> Response {
        if !self.view.application_exists(&application_id).await {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        let event = Event::Archived {
            application_id,
            archived_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::Archived { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_create(&self, owner_identity_id: Uuid) -> Response {
        let application_id = Uuid::new_v4();

        let event = Event::Created {
            application_id,
            owner_identity_id,
            created_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::Created {
                application_id,
                last_event_seq,
            },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_link_http_domain(
        &self,
        application_id: Uuid,
        http_domain: proven_util::Domain,
    ) -> Response {
        if !self.view.application_exists(&application_id).await {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if self.view.http_domain_linked(&http_domain).await {
            return Response::Error {
                message: "HTTP domain already linked".to_string(),
            };
        }

        let event = Event::HttpDomainLinked {
            application_id,
            http_domain,
            linked_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::HttpDomainLinked { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_remove_allowed_origin(
        &self,
        application_id: Uuid,
        origin: proven_util::Origin,
    ) -> Response {
        if !self.view.application_exists(&application_id).await {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if let Some(app) = self.view.get_application(&application_id).await
            && !app.allowed_origins.contains(&origin)
        {
            return Response::Error {
                message: "Origin not in allowed origins".to_string(),
            };
        }

        let event = Event::AllowedOriginRemoved {
            application_id,
            origin,
            removed_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::AllowedOriginRemoved { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Response {
        let old_owner_id = match self.view.get_application(&application_id).await {
            Some(app) => app.owner_id,
            None => {
                return Response::Error {
                    message: "Application not found".to_string(),
                };
            }
        };

        let event = Event::OwnershipTransferred {
            application_id,
            old_owner_id,
            new_owner_id,
            transferred_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::OwnershipTransferred { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_unlink_http_domain(
        &self,
        application_id: Uuid,
        http_domain: proven_util::Domain,
    ) -> Response {
        if !self.view.application_exists(&application_id).await {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if let Some(app_id) = self
            .view
            .get_application_id_for_http_domain(&http_domain)
            .await
        {
            if app_id != application_id {
                return Response::Error {
                    message: "HTTP domain not linked to this application".to_string(),
                };
            }
        } else {
            return Response::Error {
                message: "HTTP domain not linked to this application".to_string(),
            };
        }

        let event = Event::HttpDomainUnlinked {
            application_id,
            http_domain,
            unlinked_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::HttpDomainUnlinked { last_event_seq },
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
    pub fn new(
        client: Arc<Client>,
        command_stream: String,
        handler: Arc<CommandServiceHandler>,
    ) -> Self {
        let mut service = Self {
            handle: None,
            shutdown_tx: None,
        };

        service.start(client, command_stream, handler);

        service
    }

    /// Start the service.
    pub fn start(
        &mut self,
        client: Arc<Client>,
        command_stream: String,
        handler: Arc<CommandServiceHandler>,
    ) {
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
    async fn run_service_loop(
        client: Arc<Client>,
        command_stream: String,
        handler: Arc<CommandServiceHandler>,
        mut shutdown_rx: oneshot::Receiver<()>,
    ) -> Result<(), Error> {
        use tokio::pin;
        use tokio_stream::StreamExt;

        // Use streaming API instead of polling
        let start_seq = LogIndex::new(1).unwrap();
        let stream = client
            .stream_messages(command_stream.clone(), start_seq, None)
            .await
            .map_err(|e| Error::Stream(e.to_string()))?;

        pin!(stream);

        loop {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                tracing::info!("Command service shutting down");
                break;
            }

            // Wait for next message with timeout
            match tokio::time::timeout(Duration::from_millis(100), stream.next()).await {
                Ok(Some((message, _timestamp, sequence))) => {
                    // Process the message
                    if let Err(e) =
                        Self::process_message(&client, &command_stream, &handler, message, sequence)
                            .await
                    {
                        tracing::error!("Failed to process message: {}", e);
                    }
                }
                Ok(None) => {
                    // Stream ended
                    tracing::info!("Command stream ended");
                    break;
                }
                Err(_) => {
                    // Timeout - check for shutdown again
                }
            }
        }

        Ok(())
    }

    /// Process a single command message.
    async fn process_message(
        client: &Arc<Client>,
        command_stream: &str,
        handler: &Arc<CommandServiceHandler>,
        message: Message,
        sequence: u64,
    ) -> Result<(), Error> {
        // Check if this is a request
        let headers: HashMap<String, String> = message
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
            let request: StreamRequest = ciborium::de::from_reader(&message.payload[..])
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
                let mut message = proven_engine::Message::new(payload);
                for (k, v) in response_metadata {
                    message = message.with_header(k, v);
                }
                client
                    .publish_to_stream(command_stream.to_string(), vec![message])
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;
            }

            // Delete the processed message
            client
                .delete_message(command_stream.to_string(), LogIndex::new(sequence).unwrap())
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;
        }

        Ok(())
    }
}

/// Execute a command via the stream and wait for response.
pub async fn execute_command_via_stream(
    client: &Arc<Client>,
    command_stream: &str,
    command: Command,
    timeout_duration: Duration,
) -> Result<Response, Error> {
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
    let mut message = proven_engine::Message::new(payload);
    for (k, v) in metadata {
        message = message.with_header(k, v);
    }
    client
        .publish_to_stream(command_stream.to_string(), vec![message])
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
async fn listen_for_response(
    client: Arc<Client>,
    command_stream: String,
    request_id: Uuid,
    response_tx: oneshot::Sender<Response>,
) {
    use tokio::pin;
    use tokio_stream::StreamExt;

    // Start from the beginning of the stream to find our response
    // We could optimize this by tracking message positions, but for now this works
    let start_seq = LogIndex::new(1).unwrap();

    // Use follow mode to wait for our response
    let stream = match client
        .stream_messages(command_stream.clone(), start_seq, None)
        .await
    {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to create stream for response listening: {}", e);
            return;
        }
    };

    pin!(stream);
    while let Some((message, _timestamp, _sequence)) = stream.next().await {
        let headers: HashMap<String, String> = message
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
                ciborium::de::from_reader::<StreamResponse, _>(&message.payload[..])
                && stream_response.request_id == request_id
            {
                let _ = response_tx.send(stream_response.response);
                return;
            }
        }
    }
}
