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
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;

/// A NATS-backed publishable subject
pub struct NatsPublishableSubject<T = Bytes, DE = Infallible, SE = Infallible>
where
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
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> From<NatsPublishableSubject<T, DE, SE>> for String
where
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
    fn from(subject: NatsPublishableSubject<T, DE, SE>) -> Self {
        subject.full_subject
    }
}

impl<T, DE, SE> Clone for NatsPublishableSubject<T, DE, SE>
where
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
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<T, DE, SE> Debug for NatsPublishableSubject<T, DE, SE>
where
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
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsPublishableSubject")
            .field("client", &self.client)
            .field("full_subject", &self.full_subject)
            .finish()
    }
}

impl<T, DE, SE> NatsPublishableSubject<T, DE, SE>
where
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
    /// Creates a new `NatsPublishableSubject`.
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
        X: SubscriptionHandler<T, DE, SE>,
        Y: Subscription<X, T, DE, SE>,
    {
        Y::new(self.full_subject.clone(), options, handler).await
    }
}

/// A NATS-backed subscribe-only subject
pub struct NatsSubject<T = Bytes, DE = Infallible, SE = Infallible> {
    client: Client,
    full_subject: String,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> From<NatsSubject<T, DE, SE>> for String
where
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
    fn from(subject: NatsSubject<T, DE, SE>) -> Self {
        subject.full_subject
    }
}

impl<T, DE, SE> Clone for NatsSubject<T, DE, SE> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<T, DE, SE> Debug for NatsSubject<T, DE, SE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsSubject")
            .field("client", &self.client)
            .field("full_subject", &self.full_subject)
            .finish()
    }
}

impl<T, DE, SE> NatsSubject<T, DE, SE>
where
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
    /// Creates a new `NatsSubject`.
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
}

#[async_trait]
impl<T, DE, SE> Subject<T, DE, SE> for NatsSubject<T, DE, SE>
where
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

    async fn subscribe<X, Y>(&self, options: Y::Options, handler: X) -> Result<Y, Y::Error>
    where
        X: SubscriptionHandler<T, DE, SE>,
        Y: Subscription<X, T, DE, SE>,
    {
        Y::new(self.full_subject.clone(), options, handler).await
    }
}

macro_rules! impl_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[doc = $doc_pub]
            pub struct [< NatsPublishableSubject $index >]<T = Bytes, DE = Infallible, SE = Infallible>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
                _marker2: PhantomData<DE>,
                _marker3: PhantomData<SE>,
            }

            impl<T, DE, SE> Clone for [< NatsPublishableSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            impl<T, DE, SE> Debug for [< NatsPublishableSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct("NatsPublishableSubject")
                        .field("client", &self.client)
                        .field("full_subject", &self.full_subject)
                        .finish()
                }
            }

            impl<T, DE, SE> [< NatsPublishableSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                /// Creates a new `NatsPublishableSubject`.
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
            }

            #[async_trait]
            impl<T, DE, SE> [< PublishableSubject $index >]<T, DE, SE> for [< NatsPublishableSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                type Error = Error<DE, SE>;
                type Type = T;
                type Scoped = $parent_pub<T, DE, SE>;
                type WildcardAllScoped = NatsSubject<T, DE, SE>;
                type WildcardAnyScoped = $parent_sub<T, DE, SE>;

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_sub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
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
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            #[doc = $doc_sub]
            pub struct [< NatsSubject $index >]<T = Bytes, DE = Infallible, SE = Infallible>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<T>,
                _marker2: PhantomData<DE>,
                _marker3: PhantomData<SE>,
            }

            impl<T, DE, SE> Clone for [< NatsSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            impl<T, DE, SE> Debug for [< NatsSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct("NatsSubject")
                        .field("client", &self.client)
                        .field("full_subject", &self.full_subject)
                        .finish()
                }
            }

            impl<T, DE, SE> [< NatsSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                /// Creates a new `NatsSubject`.
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
            }

            #[async_trait]
            impl<T, DE, SE> [< Subject $index >]<T, DE, SE> for [< NatsSubject $index >]<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone + Debug + Send + Sync + TryFrom<Bytes, Error = DE> + TryInto<Bytes, Error = SE> + 'static,
            {
                type Error = Error<DE, SE>;
                type Type = T;
                type Scoped = $parent_sub<T, DE, SE>;
                type WildcardAllScoped = NatsSubject<T, DE, SE>;

                fn any(&self) -> Self::Scoped {
                    $parent_sub {
                        client: self.client.clone(),
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    NatsSubject {
                        client: self.client.clone(),
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
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
                        _marker2: PhantomData,
                        _marker3: PhantomData,
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
