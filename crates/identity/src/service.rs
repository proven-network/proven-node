//! Service pattern implementation for command processing using pubsub.
//!
//! This module implements the request/response pattern using pubsub request-reply,
//! replacing the stream-based command processing for better performance.

use std::sync::Arc;

use bytes::Bytes;
use proven_engine::{Client, Message};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error};

use crate::{Command, Error, Event, Response, view::IdentityView};

/// Handler for processing commands
pub struct CommandServiceHandler {
    /// Identity view for validation
    view: IdentityView,
    /// Engine client for publishing events
    client: Arc<Client>,
    /// Event stream name
    event_stream: String,
}

impl CommandServiceHandler {
    /// Create a new command service handler.
    pub const fn new(view: IdentityView, client: Arc<Client>, event_stream: String) -> Self {
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

    // Command handlers

    async fn handle_create_identity_with_prf_public_key(&self, prf_public_key: Bytes) -> Response {
        // Check if identity already exists
        if let Some(identity) = self.view.get_identity_by_prf_public_key(&prf_public_key) {
            return Response::IdentityCreated {
                identity,
                last_event_seq: 0, // Not creating a new identity
            };
        }

        let identity_id = uuid::Uuid::new_v4();

        let event = Event::Created {
            identity_id,
            created_at: chrono::Utc::now(),
        };

        let _last_event_seq = match self.publish_event(event).await {
            Ok(seq) => seq,
            Err(error_msg) => return Response::InternalError { message: error_msg },
        };

        // Now link the PRF public key
        let link_event = Event::PrfPublicKeyLinked {
            identity_id,
            prf_public_key: prf_public_key.clone(),
            linked_at: chrono::Utc::now(),
        };

        match self.publish_event(link_event).await {
            Ok(seq) => {
                // Get the created identity from the view
                self.view.get_identity(&identity_id).map_or_else(
                    || Response::InternalError {
                        message: "Failed to retrieve created identity".to_string(),
                    },
                    |identity| Response::IdentityCreated {
                        identity,
                        last_event_seq: seq,
                    },
                )
            }
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    async fn handle_link_prf_public_key(
        &self,
        identity_id: uuid::Uuid,
        prf_public_key: Bytes,
    ) -> Response {
        // Check identity exists
        if !self.view.identity_exists(&identity_id) {
            return Response::Error {
                message: "Identity not found".to_string(),
            };
        }

        // Check if PRF public key is already linked to another identity
        if let Some(existing_identity) = self.view.get_identity_by_prf_public_key(&prf_public_key) {
            if existing_identity.id != identity_id {
                return Response::Error {
                    message: format!(
                        "PRF public key already linked to identity {}",
                        existing_identity.id
                    ),
                };
            }
            // Already linked to this identity, return success
            return Response::PrfPublicKeyLinked {
                last_event_seq: 0, // Not publishing a new event
            };
        }

        let event = Event::PrfPublicKeyLinked {
            identity_id,
            prf_public_key,
            linked_at: chrono::Utc::now(),
        };

        match self.publish_event(event).await {
            Ok(last_event_seq) => Response::PrfPublicKeyLinked { last_event_seq },
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }
}

/// Service that processes commands using pubsub subscription
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
        command_subject: String,
        handler: Arc<CommandServiceHandler>,
    ) -> Self {
        let mut service = Self {
            handle: None,
            shutdown_tx: None,
        };

        service.start(client, command_subject, handler);

        service
    }

    /// Start the service.
    pub fn start(
        &mut self,
        client: Arc<Client>,
        command_subject: String,
        handler: Arc<CommandServiceHandler>,
    ) {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        let handle = tokio::spawn(async move {
            debug!("Starting command service for subject '{}'", command_subject);

            // Subscribe to the command subject
            let mut subscription = match client.subscribe(&command_subject, None).await {
                Ok(sub) => sub,
                Err(e) => {
                    error!("Failed to subscribe to command subject: {}", e);
                    return;
                }
            };

            debug!("Command service subscribed and ready");

            // Process incoming requests
            loop {
                tokio::select! {
                    message = subscription.next() => {
                        if let Some(message) = message {
                            if let Some(reply_to) = message.get_header("reply_to") {
                                let correlation_id = message.get_header("correlation_id");
                                let handler = handler.clone();
                                let client = client.clone();
                                let reply_to = reply_to.to_string();
                                let correlation_id = correlation_id.map(std::string::ToString::to_string);

                                // Spawn a task to handle the request asynchronously
                                tokio::spawn(async move {
                                    if let Err(e) = Self::process_request(
                                        client,
                                        handler,
                                        message,
                                        reply_to,
                                        correlation_id,
                                    )
                                    .await
                                    {
                                        error!("Failed to process request: {}", e);
                                    }
                                });
                            } else {
                                debug!("Received message without reply_to header, ignoring");
                            }
                        } else {
                            debug!("Subscription closed");
                            break;
                        }
                    }
                    _ = &mut shutdown_rx => {
                        debug!("Command service shutting down");
                        break;
                    }
                }
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

    /// Process a single command request.
    async fn process_request(
        client: Arc<Client>,
        handler: Arc<CommandServiceHandler>,
        message: Message,
        reply_to: String,
        correlation_id: Option<String>,
    ) -> Result<(), Error> {
        debug!("Processing command request");

        // Deserialize command
        let command: Command = ciborium::de::from_reader(message.payload.as_ref())?;

        // Process command
        let response = handler.handle_command(command).await;

        // Serialize response
        let mut payload = Vec::new();
        ciborium::ser::into_writer(&response, &mut payload)?;

        // Create response message with correlation ID
        let mut response_msg = Message::new(payload);
        if let Some(correlation_id) = correlation_id {
            response_msg = response_msg.with_header("correlation_id", correlation_id);
        }

        // Send response to reply_to subject
        client
            .publish(&reply_to, vec![response_msg])
            .await
            .map_err(|e| Error::Service(format!("Failed to send response: {e}")))?;

        Ok(())
    }
}
