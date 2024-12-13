mod error;

use crate::stream::InitializedNatsStream;
use crate::subscription::{NatsSubscription, NatsSubscriptionOptions};
pub use error::Error;
use proven_messaging::Message;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::stream::InitializedStream;
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;

/// Options for a NATS subject.
#[derive(Clone, Debug)]
pub struct NatsSubjectOptions {
    /// The NATS client to use.
    pub client: Client,
}

/// A NATS-backed publishable subject
#[derive(Debug)]
pub struct NatsSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    client: Client,
    full_subject: String,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for NatsSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> From<NatsSubject<T, D, S>> for String
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn from(subject: NatsSubject<T, D, S>) -> Self {
        subject.full_subject
    }
}

impl<T, D, S> NatsSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new `NatsPublishableSubject`.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidSubjectPartial` if the subject contains invalid characters.
    pub fn new(
        subject_partial: impl Into<String>,
        NatsSubjectOptions { client }: NatsSubjectOptions,
    ) -> Result<Self, Error<D, S>> {
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
impl<T, D, S> Subject<T, D, S> for NatsSubject<T, D, S>
where
    Self: Into<NatsUnpublishableSubject<T, D, S>>,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type SubscriptionType<X>
        = NatsSubscription<X, T, D, S>
    where
        X: SubscriptionHandler<T, D, S>;

    type StreamType = InitializedNatsStream<T, D, S>;

    async fn subscribe<X>(
        &self,
        handler: X,
    ) -> Result<NatsSubscription<X, T, D, S>, Self::Error<D, S>>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        let subscription = NatsSubscription::new(
            NatsUnpublishableSubject::<T, D, S>::from(self.clone()),
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
        stream_name: K,
        options: <InitializedNatsStream<T, D, S> as InitializedStream<T, D, S>>::Options,
    ) -> Result<
        InitializedNatsStream<T, D, S>,
        <Self::StreamType as InitializedStream<T, D, S>>::Error,
    >
    where
        K: Clone + Into<String> + Send,
    {
        let unpublishable_subject = NatsUnpublishableSubject::<T, D, S>::from(self.clone());

        InitializedNatsStream::<T, D, S>::new_with_subjects(
            stream_name.into(),
            options,
            vec![unpublishable_subject],
        )
        .await
    }
}

// Only implement Publishable for non-wildcard subjects
#[async_trait]
impl<T, D, S> PublishableSubject<T, D, S> for NatsSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    #[allow(clippy::significant_drop_tightening)]
    async fn publish(&self, message: Message<T>) -> Result<(), Self::Error<D, S>> {
        let payload: Bytes = message
            .payload
            .try_into()
            .map_err(|e| Error::Serialize(e))?;

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

    async fn request<X>(
        &self,
        _message: Message<T>,
    ) -> Result<Message<X::ResponseType>, Self::Error<D, S>>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        unimplemented!()
    }
}

/// A subject that is not publishable (contains a wildcard).
#[derive(Debug)]
pub struct NatsUnpublishableSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    client: Client,
    full_subject: String,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for NatsUnpublishableSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> From<NatsUnpublishableSubject<T, D, S>> for String
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn from(subject: NatsUnpublishableSubject<T, D, S>) -> Self {
        subject.full_subject
    }
}

impl<T, D, S> From<NatsSubject<T, D, S>> for NatsUnpublishableSubject<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn from(subject: NatsSubject<T, D, S>) -> Self {
        Self {
            client: subject.client.clone(),
            full_subject: subject.full_subject,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Subject<T, D, S> for NatsUnpublishableSubject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error<DE, SE>
        = Error<DE, SE>
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type SubscriptionType<X>
        = NatsSubscription<X, T, D, S>
    where
        X: SubscriptionHandler<T, D, S>;

    type StreamType = InitializedNatsStream<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error<D, S>>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        NatsSubscription::new(
            self.clone(),
            NatsSubscriptionOptions {
                client: self.client.clone(),
            },
            handler,
        )
        .await
        .map_err(|e| Error::SubscriptionError(e))
    }

    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <InitializedNatsStream<T, D, S> as InitializedStream<T, D, S>>::Options,
    ) -> Result<
        InitializedNatsStream<T, D, S>,
        <Self::StreamType as InitializedStream<T, D, S>>::Error,
    >
    where
        K: Clone + Into<String> + Send,
    {
        InitializedNatsStream::<T, D, S>::new_with_subjects(
            stream_name.into(),
            options,
            vec![self.clone()],
        )
        .await
    }
}

macro_rules! define_scoped_subject {
    ($n:expr, $parent:ident, $parent_non_pub:ident, $doc:expr, $doc_non_pub:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug)]
            pub struct [<NatsSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [<NatsSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> From<[<NatsSubject $n>]<T, D, S>> for String
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                fn from(subject: [<NatsSubject $n>]<T, D, S>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, D, S> [<NatsSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                #[doc = "Creates a new `NatsStream" $n "`."]
                ///
                /// # Errors
                /// Returns an error if the subject partial contains '.', '*' or '>'
                pub fn new(
                    subject_partial: impl Into<String>,
                    NatsSubjectOptions { client }: NatsSubjectOptions,
                ) -> Result<Self, Error<D, S>> {
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

            impl<T, D, S> [<PublishableSubject $n>]<T, D, S> for [<NatsSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                type Scoped = $parent<T, D, S>;
                type WildcardAllScoped = NatsUnpublishableSubject<T, D, S>;
                type WildcardAnyScoped = $parent_non_pub<T, D, S>;

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
            #[derive(Debug)]
            pub struct [<NatsUnpublishableSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                client: Client,
                full_subject: String,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [<NatsUnpublishableSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> From<[<NatsUnpublishableSubject $n>]<T, D, S>> for String
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                fn from(subject: [<NatsUnpublishableSubject $n>]<T, D, S>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, D, S> [<Subject $n>]<T, D, S> for [<NatsUnpublishableSubject $n>]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,

            {
                type Scoped = $parent_non_pub<T, D, S>;
                type WildcardAllScoped = NatsUnpublishableSubject<T, D, S>;
                type WildcardAnyScoped = $parent_non_pub<T, D, S>;

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
    "A single-scoped subject that is both publishable and subscribable.",
    "A single-scoped subject that is subscribable."
);

define_scoped_subject!(
    2,
    NatsSubject1,
    NatsUnpublishableSubject1,
    "A double-scoped subject that is both publishable and subscribable.",
    "A double-scoped subject that is subscribable."
);

define_scoped_subject!(
    3,
    NatsSubject2,
    NatsUnpublishableSubject2,
    "A triple-scoped subject that is both publishable and subscribable.",
    "A triple-scoped subject that is subscribable."
);
