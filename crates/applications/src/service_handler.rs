use crate::{Error, command::Command, event::Event, response::Response, view::ApplicationView};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use proven_messaging::service_handler::ServiceHandler;
use proven_messaging::service_responder::ServiceResponder;
use proven_messaging::stream::InitializedStream;
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
            Command::Archive { .. }
            | Command::TransferOwnership { .. }
            | Command::LinkHttpDomain { .. }
            | Command::UnlinkHttpDomain { .. } => true,
            Command::Create { .. } => false,
        }
    }

    /// Handle commands by generating and publishing events
    // TODO: Refactor this to be more readable
    #[allow(clippy::too_many_lines)]
    async fn handle_command(&self, command: Command) -> Result<Response, Error> {
        match command {
            Command::Archive { application_id } => {
                if !self.view.application_exists(application_id).await {
                    return Ok(Response::Error {
                        message: "Application not found".to_string(),
                    });
                }

                let event = Event::Archived {
                    application_id,
                    archived_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                let last_event_seq = self
                    .event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(Response::Archived { last_event_seq })
            }

            Command::Create { owner_identity_id } => {
                let application_id = Uuid::new_v4();

                // Publish event to event stream
                let last_event_seq = self
                    .event_stream
                    .publish(Event::Created {
                        application_id,
                        owner_identity_id,
                        created_at: chrono::Utc::now(),
                    })
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(Response::Created {
                    application_id,
                    last_event_seq,
                })
            }

            Command::LinkHttpDomain {
                application_id,
                http_domain,
            } => {
                // Validate application exists
                if !self.view.application_exists(application_id).await {
                    return Ok(Response::Error {
                        message: "Application not found".to_string(),
                    });
                }

                // Validate HTTP domain is not already linked
                if self.view.http_domain_linked(&http_domain).await {
                    return Ok(Response::Error {
                        message: "HTTP domain already linked".to_string(),
                    });
                }

                let event = Event::HttpDomainLinked {
                    application_id,
                    http_domain,
                    linked_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                let last_event_seq = self
                    .event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(Response::HttpDomainLinked { last_event_seq })
            }

            Command::TransferOwnership {
                application_id,
                new_owner_id,
            } => {
                // Validate application exists
                if !self.view.application_exists(application_id).await {
                    return Ok(Response::Error {
                        message: "Application not found".to_string(),
                    });
                }

                // Get current owner for event
                let old_owner_id = self
                    .view
                    .get_application(application_id)
                    .await
                    .map(|app| app.owner_id)
                    .ok_or(Error::ApplicationNotFound(application_id))?;

                let event = Event::OwnershipTransferred {
                    application_id,
                    old_owner_id,
                    new_owner_id,
                    transferred_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                let last_event_seq = self
                    .event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(Response::OwnershipTransferred { last_event_seq })
            }

            Command::UnlinkHttpDomain {
                application_id,
                http_domain,
            } => {
                // Validate application exists
                if !self.view.application_exists(application_id).await {
                    return Ok(Response::Error {
                        message: "Application not found".to_string(),
                    });
                }

                // Validate HTTP domain is linked to this particular application
                if let Some(app_id) = self
                    .view
                    .get_application_id_for_http_domain(&http_domain)
                    .await
                {
                    if app_id != application_id {
                        return Ok(Response::Error {
                            message: "HTTP domain not linked to this application".to_string(),
                        });
                    }
                } else {
                    return Ok(Response::Error {
                        message: "HTTP domain not linked to this application".to_string(),
                    });
                }

                let event = Event::HttpDomainUnlinked {
                    application_id,
                    http_domain,
                    unlinked_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                let last_event_seq = self
                    .event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(Response::HttpDomainUnlinked { last_event_seq })
            }
        }
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
        // If this command requires strong consistency, ensure view is caught up
        if Self::requires_strong_consistency(&command) {
            let current_seq = self
                .event_stream
                .last_seq()
                .await
                .map_err(|e| Error::Stream(e.to_string()))?;

            self.view.wait_for_seq(current_seq).await;
        }

        // Handle the command
        let response = self.handle_command(command).await?;

        // Update sequence tracking
        self.last_processed_seq
            .store(responder.stream_sequence(), Ordering::SeqCst);

        // Send response
        Ok(responder.reply(response).await)
    }

    /// Called when the service has processed all existing events
    async fn on_caught_up(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}
