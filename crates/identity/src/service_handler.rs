use crate::{Command, Error, Event, Identity, IdentityView, Response};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
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
    ES: InitializedStream<Event, DeserializeError, SerializeError>,
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
    ES: InitializedStream<Event, DeserializeError, SerializeError>,
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
    pub const fn view(&self) -> &IdentityView {
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
        matches!(
            command,
            Command::GetOrCreateIdentityByPrfPublicKey { .. } | Command::LinkPrfPublicKey { .. }
        )
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
            Command::GetOrCreateIdentityByPrfPublicKey { prf_public_key } => {
                self.handle_get_or_create_identity_by_prf_public_key(prf_public_key)
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

    /// Handle `GetOrCreateIdentityByPrfPublicKey` command
    async fn handle_get_or_create_identity_by_prf_public_key(
        &self,
        prf_public_key: Bytes,
    ) -> Response {
        // Check if identity already exists for this PRF public key
        if let Some(identity) = self
            .view
            .get_identity_by_prf_public_key(&prf_public_key)
            .await
        {
            // For existing identities, we don't publish events, so use 0 as placeholder
            // TODO: Consider how to handle this case for read-your-own-writes consistency
            return Response::IdentityRetrieved {
                identity,
                last_event_seq: 0,
            };
        }

        // Create new identity
        let identity_id = Uuid::new_v4();
        let now = chrono::Utc::now();

        // Create both events for atomic publishing
        let events = vec![
            Event::Created {
                created_at: now,
                identity_id,
            },
            Event::PrfPublicKeyLinked {
                identity_id,
                linked_at: now,
                prf_public_key: prf_public_key.clone(),
            },
        ];

        // Publish both events atomically
        match self.publish_event_batch(events).await {
            Ok(last_event_seq) => {
                let identity = Identity { id: identity_id };
                Response::IdentityRetrieved {
                    identity,
                    last_event_seq,
                }
            }
            Err(error_msg) => Response::InternalError { message: error_msg },
        }
    }

    /// Handle `LinkPrfPublicKey` command
    async fn handle_link_prf_public_key(
        &self,
        identity_id: Uuid,
        prf_public_key: Bytes,
    ) -> Response {
        // Validate identity exists
        if let Err(error_response) = self.validate_identity_exists(&identity_id).await {
            return error_response;
        }

        // Check if PRF public key is already linked
        if self.view.prf_public_key_exists(&prf_public_key).await {
            return Response::Error {
                message: "PRF public key already linked to an identity".to_string(),
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

    /// Helper method to publish a single event and handle errors consistently
    async fn publish_event(&self, event: Event) -> Result<u64, String> {
        self.event_stream
            .publish(event)
            .await
            .map_err(|e| e.to_string())
    }

    /// Helper method to publish multiple events in batch and handle errors consistently
    async fn publish_event_batch(&self, events: Vec<Event>) -> Result<u64, String> {
        self.event_stream
            .publish_batch(events)
            .await
            .map_err(|e| e.to_string())
    }

    /// Helper method to validate that an identity exists
    async fn validate_identity_exists(&self, identity_id: &Uuid) -> Result<(), Response> {
        if !self.view.identity_exists(identity_id).await {
            return Err(Response::Error {
                message: "Identity not found".to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl<ES> ServiceHandler<Command, DeserializeError, SerializeError> for IdentityServiceHandler<ES>
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
