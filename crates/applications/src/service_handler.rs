use crate::{Error, command::Command, event::Event, response::Response, view::ApplicationView};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use proven_messaging::stream::InitializedStream;
use proven_util::{Domain, Origin};
use uuid::Uuid;

type DeserializeError = ciborium::de::Error<std::io::Error>;
type SerializeError = ciborium::ser::Error<std::io::Error>;

/// Service handler that processes application commands and publishes events
#[derive(Clone, Debug)]
pub struct ApplicationServiceHandler<ES>
where
    ES: InitializedStream<Event, DeserializeError, SerializeError>,
{
    /// Event stream for publishing events
    event_stream: ES,

    /// Track last processed sequence for snapshots/monitoring
    last_processed_seq: Arc<AtomicU64>,

    /// Reference to the view for validation (read-only)
    view: ApplicationView,
}

impl<ES> ApplicationServiceHandler<ES>
where
    ES: InitializedStream<Event, DeserializeError, SerializeError>,
{
    /// Creates a new application service handler.
    ///
    /// # Arguments
    ///
    /// * `view` - Shared application view for read-only validation
    /// * `event_stream` - Stream for publishing events after successful command processing
    pub fn new(view: ApplicationView, event_stream: ES) -> Self {
        Self {
            view,
            event_stream,
            last_processed_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gets a reference to the application view.
    pub const fn view(&self) -> &ApplicationView {
        &self.view
    }

    /// Gets the sequence number of the last processed command.
    /// Useful for monitoring and creating snapshots.
    pub fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq.load(Ordering::SeqCst)
    }

    /// Determines if a command requires strong consistency (view synchronization)
    /// before processing to prevent conflicts or ensure accurate reads.
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

    /// Handle commands by generating and publishing events
    pub async fn handle_command(&self, command: Command) -> Response {
        // If this command requires strong consistency, ensure view is caught up
        if Self::requires_strong_consistency(&command) {
            match self.event_stream.last_seq().await {
                Ok(current_seq) => {
                    self.view.wait_for_seq(current_seq).await;
                }
                Err(e) => {
                    return Response::InternalError {
                        message: e.to_string(),
                    };
                }
            }
        }

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

    /// Handle `AddAllowedOrigin` command
    async fn handle_add_allowed_origin(&self, application_id: Uuid, origin: Origin) -> Response {
        // Validate application exists
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
        }

        // Check if origin is already in the allowed origins list
        if let Some(app) = self.view.get_application(&application_id).await {
            if app.allowed_origins.contains(&origin) {
                return Response::Error {
                    message: "Origin already in allowed origins".to_string(),
                };
            }
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

    /// Handle Archive command
    async fn handle_archive(&self, application_id: Uuid) -> Response {
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
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

    /// Handle Create command
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

    /// Handle `LinkHttpDomain` command
    async fn handle_link_http_domain(&self, application_id: Uuid, http_domain: Domain) -> Response {
        // Validate application exists
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
        }

        // Validate HTTP domain is not already linked
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

    /// Handle `RemoveAllowedOrigin` command
    async fn handle_remove_allowed_origin(&self, application_id: Uuid, origin: Origin) -> Response {
        // Validate application exists
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
        }

        // Check if origin is in the allowed origins list
        if let Some(app) = self.view.get_application(&application_id).await {
            if !app.allowed_origins.contains(&origin) {
                return Response::Error {
                    message: "Origin not in allowed origins".to_string(),
                };
            }
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

    /// Handle `TransferOwnership` command
    async fn handle_transfer_ownership(
        &self,
        application_id: Uuid,
        new_owner_id: Uuid,
    ) -> Response {
        // Validate application exists
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
        }

        // Get current owner for event
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

    /// Handle `UnlinkHttpDomain` command
    async fn handle_unlink_http_domain(
        &self,
        application_id: Uuid,
        http_domain: Domain,
    ) -> Response {
        // Validate application exists
        if let Err(error_response) = self.validate_application_exists(&application_id).await {
            return error_response;
        }

        // Validate HTTP domain is linked to this particular application
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

    /// Helper method to publish events and handle errors consistently
    async fn publish_event(&self, event: Event) -> Result<u64, String> {
        self.event_stream
            .publish(event)
            .await
            .map_err(|e| e.to_string())
    }

    /// Helper method to validate that an application exists
    async fn validate_application_exists(&self, application_id: &Uuid) -> Result<(), Response> {
        if !self.view.application_exists(application_id).await {
            return Err(Response::Error {
                message: "Application not found".to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl<ES> ServiceHandler<Command, DeserializeError, SerializeError> for ApplicationServiceHandler<ES>
where
    ES: InitializedStream<Event, DeserializeError, SerializeError>,
{
    type Error = Error;
    type ResponseType = Response;
    type ResponseDeserializationError = DeserializeError;
    type ResponseSerializationError = SerializeError;

    async fn handle<R>(
        &self,
        command: Command,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                Command,
                DeserializeError,
                SerializeError,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
    {
        // Handle the command
        let response = self.handle_command(command).await;

        // Update sequence tracking
        self.last_processed_seq
            .store(responder.stream_sequence(), Ordering::SeqCst);

        // Send response
        Ok(responder.reply_and_delete_request(response).await)
    }

    /// Called when the service has processed all existing events
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
