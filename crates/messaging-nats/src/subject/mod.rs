mod error;

pub use error::Error;

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;

use crate::subscription::{NatsSubscription, NatsSubscriptionOptions};

/// A NATS-backed publishable subject
#[derive(Clone, Debug)]
pub struct NatsPublishableSubject<T = Bytes> {
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
}

impl<T> From<NatsPublishableSubject<T>> for String {
    fn from(subject: NatsPublishableSubject<T>) -> Self {
        subject.full_subject
    }
}

impl<T> NatsPublishableSubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new `NatsPublishableSubject`.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
    pub fn new(client: Client, subject_partial: impl Into<String>) -> Result<Self, Error> {
        let subject = subject_partial.into();
        if subject.contains('.') || subject.contains('*') || subject.contains('>') {
            return Err(Error::InvalidSubjectPartial);
        }
        Ok(Self {
            client,
            full_subject: subject,
            _marker: PhantomData,
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
impl<T> PublishableSubject<T> for NatsPublishableSubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;

    async fn publish(&self, data: T) -> Result<(), Self::Error> {
        self.publish_with_headers(data, HashMap::new()).await
    }

    async fn publish_with_headers<H>(&self, data: T, headers: H) -> Result<(), Error>
    where
        H: Clone + Into<HashMap<String, String>> + Send,
    {
        let payload: Bytes = data.try_into()?;
        let headers = Self::headers_to_message_headers(headers.into());

        self.client
            .publish_with_headers(self.full_subject.clone(), headers, payload)
            .await
            .map_err(|e| Error::Publish(e.kind()))?;

        Ok(())
    }

    async fn subscribe<X>(&self, handler: X) -> Result<NatsSubscription<X, T>, Self::Error>
    where
        X: SubscriptionHandler<T>,
    {
        let subscription = NatsSubscription::new(
            self.full_subject.clone(),
            NatsSubscriptionOptions {
                client: self.client.clone(),
            },
            handler.clone(),
        )
        .await?;

        Ok(subscription)
    }
}

/// A NATS-backed subscribe-only subject
#[derive(Clone, Debug)]
pub struct NatsSubject<T = Bytes> {
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
}

impl<T> From<NatsSubject<T>> for String {
    fn from(subject: NatsSubject<T>) -> Self {
        subject.full_subject
    }
}

impl<T> NatsSubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// Creates a new `NatsSubject`.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
    pub fn new(client: Client, subject_partial: impl Into<String>) -> Result<Self, Error> {
        let subject = subject_partial.into();
        if subject.contains('.') || subject.contains('*') || subject.contains('>') {
            return Err(Error::InvalidSubjectPartial);
        }
        Ok(Self {
            client,
            full_subject: subject,
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<T> Subject<T> for NatsSubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;

    async fn subscribe<X>(&self, handler: X) -> Result<NatsSubscription<X, T>, Self::Error>
    where
        X: SubscriptionHandler<T>,
    {
        let subscription = NatsSubscription::new(
            self.full_subject.clone(),
            NatsSubscriptionOptions {
                client: self.client.clone(),
            },
            handler.clone(),
        )
        .await?;

        Ok(subscription)
    }
}

macro_rules! impl_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[doc = $doc_pub]
            #[derive(Clone, Debug)]
            pub struct [< NatsPublishableSubject $index >]<T = Bytes>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> [< NatsPublishableSubject $index >]<T>
            where
                Self: [< PublishableSubject $index >]<T>,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                /// Creates a new `NatsPublishableSubject`.
                ///
                /// # Errors
                ///
                /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
                pub fn new(client: Client, subject_partial: impl Into<String>) -> Result<Self, Error> {
                    let subject = subject_partial.into();
                    if subject.contains('.') || subject.contains('*') || subject.contains('>') {
                        return Err(Error::InvalidSubjectPartial);
                    }
                    Ok(Self {
                        client,
                        full_subject: subject,
                        _marker: PhantomData,
                    })
                }
            }

            #[async_trait]
            impl<T> [< PublishableSubject $index >]<T> for [< NatsPublishableSubject $index >]<T>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                type Error = Error;

                type Type = T;

                type Scoped = $parent_pub<T>;

                type WildcardAllScoped = NatsSubject<T>;

                type WildcardAnyScoped = $parent_sub<T>;

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_sub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_pub {
                        client: self.client.clone(),
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }

            #[doc = $doc_sub]
            #[derive(Clone, Debug)]
            pub struct [< NatsSubject $index >]<T = Bytes>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> [< NatsSubject $index >]<T>
            where
                Self: [< Subject $index >]<T>,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                /// Creates a new `NatsSubject`.
                ///
                /// # Errors
                ///
                /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
                pub fn new(client: Client, subject_partial: impl Into<String>) -> Result<Self, Error> {
                    let subject = subject_partial.into();
                    if subject.contains('.') || subject.contains('*') || subject.contains('>') {
                        return Err(Error::InvalidSubjectPartial);
                    }
                    Ok(Self {
                        client,
                        full_subject: subject,
                        _marker: PhantomData,
                    })
                }
            }

            #[async_trait]
            impl<T> [< Subject $index >]<T> for [< NatsSubject $index >]<T>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,
            {
                type Error = Error;

                type Type = T;

                type Scoped = $parent_sub<T>;

                type WildcardAllScoped = NatsSubject<T>;

                fn any(&self) -> Self::Scoped {
                    $parent_sub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_sub {
                        client: self.client.clone(),
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_subject!(
    1,
    NatsPublishableSubject,
    NatsSubject,
    "A single-scoped NATS publishable subject.",
    "A single-scoped NATS subject."
);

impl_scoped_subject!(
    2,
    NatsPublishableSubject1,
    NatsSubject1,
    "A double-scoped NATS publishable subject.",
    "A double-scoped NATS subject."
);

impl_scoped_subject!(
    3,
    NatsPublishableSubject2,
    NatsSubject2,
    "A triple-scoped NATS publishable subject.",
    "A triple-scoped NATS subject."
);
