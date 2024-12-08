mod error;

pub use error::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::{Handler, PublishableSubject, Subscriber};

/// A NATS-backed publishable subject
#[derive(Clone, Debug)]
pub struct NatsPublishableSubject<T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> NatsPublishableSubject<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new NATS subject.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
    pub fn new(client: Client, subject_partial: impl Into<String>) -> Result<Self, Error<DE, SE>> {
        let subject = subject_partial.into();
        if subject.contains('.') || subject.contains('*') || subject.contains('>') {
            return Err(Error::InvalidSubjectPartial);
        }
        Ok(Self {
            client,
            full_subject: subject,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        })
    }

    fn headers_to_message_headers(headers: HashMap<String, String>) -> async_nats::HeaderMap {
        let mut message_headers = async_nats::HeaderMap::new();
        for (key, value) in headers {
            message_headers.insert::<&str, &str>(format!("Proven-{key}").as_str(), value.as_str());
        }
        message_headers
    }
}

#[async_trait]
impl<T, DE, SE> PublishableSubject<T, DE, SE> for NatsPublishableSubject<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = DE>
        + TryInto<Bytes, Error = SE>
        + 'static,
{
    type Error = Error<DE, SE>;
    type Type = T;

    async fn publish(&self, data: T) -> Result<(), Self::Error> {
        self.publish_with_headers(data, HashMap::new()).await
    }

    async fn publish_with_headers<H>(&self, data: T, headers: H) -> Result<(), Self::Error>
    where
        H: Clone + Into<HashMap<String, String>> + Send,
    {
        let payload: Bytes = data.try_into().map_err(|e| Error::Serialize(e))?;
        let headers = Self::headers_to_message_headers(headers.into());

        self.client
            .publish_with_headers(self.full_subject.clone(), headers, payload)
            .await
            .map_err(|e| Error::Publish(e.kind()))?;

        Ok(())
    }

    async fn subscribe<X, Y>(&self, options: Y::Options, handler: X) -> Result<Y, Y::Error>
    where
        X: Handler<Self::Type>,
        Y: Subscriber<X, Self::Type, DE, SE>,
    {
        Y::new(self.full_subject.clone(), options, handler).await
    }
}

// TODO: Implement scoped subjects similar to memory implementation
// ... (use the same macro pattern from messaging-memory)
