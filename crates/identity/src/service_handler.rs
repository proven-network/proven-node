use crate::{
    Error, Identity, events::IdentityEvent, request::IdentityCommand,
    response::IdentityCommandResponse, view::IdentityView,
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

/// Service handler that processes identity commands and publishes events
#[derive(Clone, Debug)]
pub struct IdentityServiceHandler<ES>
where
    ES: InitializedStream<IdentityEvent, DeserializeError, SerializeError>,
{
    /// Event stream for publishing events
    event_stream: ES,

    /// Track last processed sequence for snapshots/monitoring
    last_processed_seq: Arc<AtomicU64>,

    /// Reference to the view for validation (read-only)
    view: IdentityView,
}

impl<ES> IdentityServiceHandler<ES>
where
    ES: InitializedStream<IdentityEvent, DeserializeError, SerializeError>,
{
    /// Creates a new identity service handler.
    ///
    /// # Arguments
    ///
    /// * `view` - Shared identity view for read-only validation
    /// * `event_stream` - Stream for publishing events after successful command processing
    pub fn new(view: IdentityView, event_stream: ES) -> Self {
        Self {
            view,
            event_stream,
            last_processed_seq: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Gets a reference to the identity view.
    pub fn view(&self) -> &IdentityView {
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
        command: IdentityCommand,
    ) -> Result<IdentityCommandResponse, Error> {
        match command {
            IdentityCommand::GetOrCreateIdentityByPrfPublicKey { prf_public_key } => {
                // Check if identity already exists for this PRF public key
                if let Some(identity) = self
                    .view
                    .get_identity_by_prf_public_key(&prf_public_key)
                    .await
                {
                    return Ok(IdentityCommandResponse::IdentityRetrieved { identity });
                }

                // Create new identity
                let identity_id = Uuid::new_v4();

                // Create identity creation event
                let create_event = IdentityEvent::Created {
                    identity_id,
                    created_at: chrono::Utc::now(),
                };

                // Publish identity creation event
                self.event_stream
                    .publish(create_event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                // Create PRF public key linking event
                let link_event = IdentityEvent::PrfPublicKeyLinked {
                    identity_id,
                    prf_public_key: prf_public_key.clone(),
                    linked_at: chrono::Utc::now(),
                };

                // Publish PRF public key linking event
                self.event_stream
                    .publish(link_event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                let identity = Identity { id: identity_id };
                Ok(IdentityCommandResponse::IdentityRetrieved { identity })
            }

            IdentityCommand::LinkPrfPublicKey {
                identity_id,
                prf_public_key,
            } => {
                // Validate identity exists
                if !self.view.identity_exists(identity_id).await {
                    return Ok(IdentityCommandResponse::Error {
                        message: "Identity not found".to_string(),
                    });
                }

                // Check if PRF public key is already linked
                if self.view.prf_public_key_exists(&prf_public_key).await {
                    return Ok(IdentityCommandResponse::Error {
                        message: "PRF public key already linked to an identity".to_string(),
                    });
                }

                let event = IdentityEvent::PrfPublicKeyLinked {
                    identity_id,
                    prf_public_key,
                    linked_at: chrono::Utc::now(),
                };

                // Publish event to event stream
                self.event_stream
                    .publish(event)
                    .await
                    .map_err(|e| Error::Stream(e.to_string()))?;

                Ok(IdentityCommandResponse::PrfPublicKeyLinked)
            }
        }
    }
}

#[async_trait]
impl<ES> ServiceHandler<IdentityCommand, DeserializeError, SerializeError>
    for IdentityServiceHandler<ES>
where
    ES: InitializedStream<IdentityEvent, DeserializeError, SerializeError>,
{
    type Error = Error;
    type ResponseType = IdentityCommandResponse;
    type ResponseDeserializationError = DeserializeError;
    type ResponseSerializationError = SerializeError;

    async fn handle<R>(
        &self,
        command: IdentityCommand,
        responder: R,
    ) -> Result<R::UsedResponder, Self::Error>
    where
        R: ServiceResponder<
                IdentityCommand,
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
