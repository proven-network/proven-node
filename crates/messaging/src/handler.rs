use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for subscriber errors
pub trait HandlerError: Clone + Debug + Error + Send + Sync + 'static {}

/// A trait representing a subscriber of a subject.
#[async_trait]
pub trait Handler<T>
where
    Self: Clone + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the subscriber.
    type Error: HandlerError;

    /// Handles the given data.
    async fn handle(
        &self,
        subject: String,
        data: T,
        headers: Option<HashMap<String, String>>,
    ) -> Result<(), Self::Error>;
}
