use crate::{
    Application, Error, events::ApplicationEvent, request::ApplicationCommand,
    response::ApplicationCommandResponse, view::ApplicationView,
};

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
    ES: InitializedStream<ApplicationEvent, DeserializeError, SerializeError>,
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
    ES: InitializedStream<ApplicationEvent, DeserializeError, SerializeError>,
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
    pub fn view(&self) -> &ApplicationView {
        &self.view
    }

    /// Gets the sequence number of the last processed command.
    /// Useful for monitoring and creating snapshots.
    pub fn last_processed_seq(&self) -> u64 {
        self.last_processed_seq.load(Ordering::SeqCst)
    }

    /// Handle commands by generating and publishing events
    async fn handle_command(
        &self,
        command: ApplicationCommand,
    ) -> Result<ApplicationCommandResponse, Error> {
        match command {
            ApplicationCommand::CreateApplication { owner_identity_id } => {
                let application_id = Uuid::new_v4();

                // Create event
                let event = ApplicationEvent::Created {
                    application_id,
                    owner_identity_id,
                    created_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                self.event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                let application = Application {
                    id: application_id,
                    owner_identity_id,
                };

                Ok(ApplicationCommandResponse::ApplicationCreated { application })
            }

            ApplicationCommand::TransferOwnership {
                application_id,
                new_owner_id,
            } => {
                // Validate application exists
                if !self.view.application_exists(application_id).await {
                    return Ok(ApplicationCommandResponse::Error {
                        message: "Application not found".to_string(),
                    });
                }

                // Get current owner for event
                let old_owner_id = self
                    .view
                    .get_application(application_id)
                    .await
                    .map(|app| app.owner_identity_id)
                    .ok_or_else(|| Error::ApplicationNotFound(application_id))?;

                let event = ApplicationEvent::OwnershipTransferred {
                    application_id,
                    old_owner_id,
                    new_owner_id,
                    transferred_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                self.event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(ApplicationCommandResponse::OwnershipTransferred)
            }

            ApplicationCommand::ArchiveApplication { application_id } => {
                if !self.view.application_exists(application_id).await {
                    return Ok(ApplicationCommandResponse::Error {
                        message: "Application not found".to_string(),
                    });
                }

                let event = ApplicationEvent::Archived {
                    application_id,
                    archived_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                self.event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(ApplicationCommandResponse::ApplicationArchived)
            }
        }
    }
}

#[async_trait]
impl<ES> ServiceHandler<ApplicationCommand, DeserializeError, SerializeError>
    for ApplicationServiceHandler<ES>
where
    ES: InitializedStream<ApplicationEvent, DeserializeError, SerializeError>,
{
    type Error = Error;
    type ResponseType = ApplicationCommandResponse;
    type ResponseDeserializationError = DeserializeError;
    type ResponseSerializationError = SerializeError;

    async fn handle<R>(
        &self,
        command: ApplicationCommand,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                ApplicationCommand,
                DeserializeError,
                SerializeError,
                Self::ResponseType,
                Self::ResponseDeserializationError,
                Self::ResponseSerializationError,
            >,
    {
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
