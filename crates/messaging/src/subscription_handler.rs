use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for subscriber errors
pub trait SubscriptionHandlerError: Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait SubscriptionHandler<T = Bytes>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: SubscriptionHandlerError;

    /// Handles the given data.
    async fn handle(
        &self,
        subject_string: String,
        data: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), Self::Error>;
}
