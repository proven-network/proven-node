mod error;

use crate::stream::NatsStream;
use crate::subscription::{NatsSubscription, NatsSubscriptionOptions};
pub use error::Error;
use proven_messaging::Message;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::stream::Stream;
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;

/// A NATS-backed publishable subject
#[derive(Clone, Debug)]
pub struct NatsSubject<T> {
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
impl<T> Subject for NatsSubject<T>
where
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

    type SubscriptionType<X>
        = NatsSubscription<X, T, X::ResponseType>
    where
        X: SubscriptionHandler<Type = T>;

    type StreamType = NatsStream<T>;

    async fn subscribe<X>(
        &self,
        handler: X,
    ) -> Result<NatsSubscription<X, Self::Type, X::ResponseType>, Self::Error>
    where
        X: SubscriptionHandler<Type = Self::Type>,
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

    async fn to_stream<K>(
        &self,
        _stream_name: K,
        _options: <Self::StreamType as Stream>::Options,
    ) -> Result<Self::StreamType, <Self::StreamType as Stream>::Error>
    where
        K: Clone + Into<String> + Send,
    {
        unimplemented!()
    }
}

#[async_trait]
impl<T> PublishableSubject for NatsSubject<T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    async fn publish(&self, message: Message<T>) -> Result<(), Self::Error> {
        let payload: Bytes = message.payload.try_into()?;

        if let Some(headers) = message.headers {
            self.client
                .publish_with_headers(self.full_subject.clone(), headers, payload)
                .await
                .map_err(|e| Error::Publish(e.kind()))?;
        } else {
            self.client
                .publish(self.full_subject.clone(), payload)
                .await
                .map_err(|e| Error::Publish(e.kind()))?;
        }

        Ok(())
    }

    async fn request<X>(&self, _message: Message<T>) -> Result<Message<X::ResponseType>, Error>
    where
        X: SubscriptionHandler<Type = Self::Type>,
    {
        unimplemented!()
    }
}

/// A NATS-backed subscribe-only subject
#[derive(Clone, Debug)]
pub struct NatsUnpublishableSubject<T> {
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
}

impl<T> From<NatsUnpublishableSubject<T>> for String {
    fn from(subject: NatsUnpublishableSubject<T>) -> Self {
        subject.full_subject
    }
}

impl<T> From<NatsSubject<T>> for NatsUnpublishableSubject<T> {
    fn from(subject: NatsSubject<T>) -> Self {
        Self {
            client: subject.client,
            full_subject: subject.full_subject,
            _marker: PhantomData,
        }
    }
}

impl<T> NatsUnpublishableSubject<T>
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
impl<T> Subject for NatsUnpublishableSubject<T>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
        + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
        + 'static,
{
    type Error = Error;
    type DeserializationError = ciborium::de::Error<std::io::Error>;
    type SerializationError = ciborium::ser::Error<std::io::Error>;
    type Type = T;

    type SubscriptionType<X>
        = NatsSubscription<X, T, X::ResponseType>
    where
        X: SubscriptionHandler<Type = T>;

    type StreamType = NatsStream<T>;

    async fn subscribe<X>(
        &self,
        handler: X,
    ) -> Result<NatsSubscription<X, Self::Type, X::ResponseType>, Self::Error>
    where
        X: SubscriptionHandler<Type = Self::Type>,
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

    async fn to_stream<K>(
        &self,
        _stream_name: K,
        _options: <Self::StreamType as Stream>::Options,
    ) -> Result<Self::StreamType, <Self::StreamType as Stream>::Error>
    where
        K: Clone + Into<String> + Send,
    {
        unimplemented!()
    }
}

macro_rules! define_scoped_subject {
    ($n:expr, $parent:ident, $parent_non_pub:ident, $doc:expr, $doc_non_pub:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct [<NatsSubject $n>]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> From<[<NatsSubject $n>]<T>> for String
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                fn from(subject: [<NatsSubject $n>]<T>) -> Self {
                    subject.full_subject
                }
            }

            impl<T> [<NatsSubject $n>]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                #[doc = "Creates a new `NatsStream" $n "`."]
                ///
                /// # Errors
                /// Returns an error if the subject partial contains '.', '*' or '>'
                pub fn new<K>(client: Client, subject_partial: K) -> Result<Self, Error>
                where
                    K: Clone + Into<String> + Send,
                {
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

            impl<T> [<PublishableSubject $n>] for [<NatsSubject $n>]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                type Type = T;

                type Scoped = $parent<T>;
                type WildcardAllScoped = NatsUnpublishableSubject<T>;
                type WildcardAnyScoped = $parent_non_pub<T>;

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsUnpublishableSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_non_pub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send,
                {
                    $parent {
                        client: self.client.clone(),
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }

            #[doc = $doc_non_pub]
            #[derive(Clone, Debug)]
            pub struct [<NatsUnpublishableSubject $n>]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> From<[<NatsUnpublishableSubject $n>]<T>> for String
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                fn from(subject: [<NatsUnpublishableSubject $n>]<T>) -> Self {
                    subject.full_subject
                }
            }

            impl<T> [<Subject $n>] for [<NatsUnpublishableSubject $n>]<T>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = ciborium::de::Error<std::io::Error>>
                    + TryInto<Bytes, Error = ciborium::ser::Error<std::io::Error>>
                    + 'static,

            {
                type Type = T;

                type Scoped = $parent_non_pub<T>;
                type WildcardAllScoped = NatsUnpublishableSubject<T>;
                type WildcardAnyScoped = $parent_non_pub<T>;

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsUnpublishableSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_non_pub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send,
                {
                    $parent_non_pub {
                        client: self.client.clone(),
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

define_scoped_subject!(
    1,
    NatsSubject,
    NatsUnpublishableSubject,
    "A single-scoped NATS subject that is both publishable and subscribable.",
    "A single-scoped NATS subject that is subscribable."
);

define_scoped_subject!(
    2,
    NatsSubject1,
    NatsUnpublishableSubject1,
    "A double-scoped NATS subject that is both publishable and subscribable.",
    "A double-scoped NATS subject that is subscribable."
);

define_scoped_subject!(
    3,
    NatsSubject2,
    NatsUnpublishableSubject2,
    "A triple-scoped NATS subject that is both publishable and subscribable.",
    "A triple-scoped NATS subject that is subscribable."
);
