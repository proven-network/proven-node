//! Service pattern implementation for command processing using pubsub.
//!
//! This module implements the request/response pattern using pubsub request-reply,
//! replacing the stream-based command processing for better performance.

use std::sync::Arc;

use proven_engine::{Client, Message};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::{debug, error};

use crate::{Command, Error, Event, Response, view::ApplicationView};

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
        application_id: uuid::Uuid,
        origin: proven_util::Origin,
    ) -> Response {
        // Validate application exists
        if !self.view.application_exists(&application_id) {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        // Check if origin is already allowed
        if let Some(app) = self.view.get_application(&application_id)
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

    async fn handle_archive(&self, application_id: uuid::Uuid) -> Response {
        if !self.view.application_exists(&application_id) {
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

    async fn handle_create(&self, owner_identity_id: uuid::Uuid) -> Response {
        let application_id = uuid::Uuid::new_v4();

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
        application_id: uuid::Uuid,
        http_domain: proven_util::Domain,
    ) -> Response {
        if !self.view.application_exists(&application_id) {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if self.view.http_domain_linked(&http_domain) {
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
        application_id: uuid::Uuid,
        origin: proven_util::Origin,
    ) -> Response {
        if !self.view.application_exists(&application_id) {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if let Some(app) = self.view.get_application(&application_id)
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
        application_id: uuid::Uuid,
        new_owner_id: uuid::Uuid,
    ) -> Response {
        let old_owner_id = match self.view.get_application(&application_id) {
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
        application_id: uuid::Uuid,
        http_domain: proven_util::Domain,
    ) -> Response {
        if !self.view.application_exists(&application_id) {
            return Response::Error {
                message: "Application not found".to_string(),
            };
        }

        if let Some(app_id) = self.view.get_application_id_for_http_domain(&http_domain) {
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
