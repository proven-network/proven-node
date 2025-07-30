//! Handler for HasResponders command

use async_trait::async_trait;
use tracing::debug;

use crate::foundation::events::{Error, EventMetadata, RequestHandler};
use crate::services::pubsub::commands::HasResponders;
use crate::services::pubsub::interest::InterestTracker;
use crate::services::pubsub::streaming_router::StreamingMessageRouter;

/// Handler for HasResponders command
#[derive(Clone)]
pub struct HasRespondersHandler {
    interest_tracker: InterestTracker,
    message_router: StreamingMessageRouter,
}

impl HasRespondersHandler {
    pub fn new(interest_tracker: InterestTracker, message_router: StreamingMessageRouter) -> Self {
        Self {
            interest_tracker,
            message_router,
        }
    }
}

#[async_trait]
impl RequestHandler<HasResponders> for HasRespondersHandler {
    async fn handle(
        &self,
        request: HasResponders,
        _metadata: EventMetadata,
    ) -> Result<bool, Error> {
        let subject = request.subject.as_ref();
        debug!("Checking for responders on subject: {}", subject);

        // Check local subscribers first
        let has_local = self.message_router.has_subscribers(subject).await;

        if has_local {
            debug!("Found local subscribers for subject: {}", subject);
            return Ok(true);
        }

        // Check remote interest
        let has_remote = self.interest_tracker.has_interest(subject).await;

        debug!(
            "Subject {} has {} responders",
            subject,
            if has_remote { "remote" } else { "no" }
        );

        Ok(has_remote)
    }
}
