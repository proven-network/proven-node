#![allow(clippy::too_many_lines)]
mod error;

use crate::stream::InitializedMemoryStream;
use crate::subscription::{MemorySubscription, MemorySubscriptionOptions};
use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;

use std::error::Error as StdError;
use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::stream::InitializedStream;
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;
use proven_messaging::Message;

/// A subject that is both publishable and subscribable.
#[derive(Debug)]
pub struct MemorySubject<T, D, S>
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
    full_subject: String,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for MemorySubject<T, D, S>
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
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> From<MemorySubject<T, D, S>> for String
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
    fn from(subject: MemorySubject<T, D, S>) -> Self {
        subject.full_subject
    }
}

impl<T, D, S> MemorySubject<T, D, S>
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
    /// Creates a new `MemorySubject`.
    ///
    /// # Errors
    /// Returns an error if the subject partial contains '.', '*' or '>'
    pub fn new<K>(subject_partial: K) -> Result<Self, Error>
    where
        K: Clone + Into<String> + Send,
    {
        let subject = subject_partial.into();
        if subject.contains('.') || subject.contains('*') || subject.contains('>') {
            return Err(Error::InvalidSubjectPartial);
        }
        Ok(Self {
            full_subject: subject,
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<T, D, S> Subject<T, D, S> for MemorySubject<T, D, S>
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
        = Error
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type SubscriptionType<X>
        = MemorySubscription<X, T, D, S>
    where
        X: SubscriptionHandler<T, D, S>;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Error>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        MemorySubscription::new(
            MemoryUnpublishableSubject::<T, D, S>::from(self.clone()),
            MemorySubscriptionOptions,
            handler,
        )
        .await
        .map_err(|_| Error::Subscribe)
    }

    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <InitializedMemoryStream<T, D, S> as InitializedStream<T, D, S>>::Options,
    ) -> Result<
        InitializedMemoryStream<T, D, S>,
        <Self::StreamType as InitializedStream<T, D, S>>::Error,
    >
    where
        K: Clone + Into<String> + Send,
    {
        let unpublishable_subject = MemoryUnpublishableSubject::<T, D, S>::from(self.clone());

        InitializedMemoryStream::<T, D, S>::new_with_subjects(
            stream_name.into(),
            options,
            vec![unpublishable_subject],
        )
        .await
    }
}

// Only implement Publishable for non-wildcard subjects
#[async_trait]
impl<T, D, S> PublishableSubject<T, D, S> for MemorySubject<T, D, S>
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
    async fn publish(&self, message: Message<T>) -> Result<(), Error> {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<SubjectState<T>>() {
            state.put(SubjectState::<T>::default());
        }
        let subject_state = state.borrow::<SubjectState<T>>();
        let subjects = subject_state.subjects.lock().await;
        let subscribers = subjects.keys().cloned().collect::<Vec<_>>();
        for subscriber in subscribers {
            let subscriber_parts: Vec<&str> = subscriber.split('.').collect();
            let full_subject_parts: Vec<&str> = self.full_subject.split('.').collect();
            if full_subject_parts
                .iter()
                .zip(subscriber_parts.iter())
                .all(|(n, s)| *s == "*" || *s == ">" || *n == *s)
            {
                if let Some(sender) = subjects.get(&subscriber) {
                    let _ = sender.send(message.clone());
                }
            }
        }
        Ok(())
    }

    async fn request<X>(&self, _message: Message<T>) -> Result<Message<X::ResponseType>, Error>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        unimplemented!()
    }
}

/// A subject that is not publishable (contains a wildcard).
#[derive(Debug)]
pub struct MemoryUnpublishableSubject<T, D, S>
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
    full_subject: String,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for MemoryUnpublishableSubject<T, D, S>
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
            full_subject: self.full_subject.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> From<MemoryUnpublishableSubject<T, D, S>> for String
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
    fn from(subject: MemoryUnpublishableSubject<T, D, S>) -> Self {
        subject.full_subject
    }
}

impl<T, D, S> From<MemorySubject<T, D, S>> for MemoryUnpublishableSubject<T, D, S>
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
    fn from(subject: MemorySubject<T, D, S>) -> Self {
        Self {
            full_subject: subject.full_subject,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, D, S> Subject<T, D, S> for MemoryUnpublishableSubject<T, D, S>
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
        = Error
    where
        DE: Debug + Send + StdError + Sync + 'static,
        SE: Debug + Send + StdError + Sync + 'static;

    type SubscriptionType<X>
        = MemorySubscription<X, T, D, S>
    where
        X: SubscriptionHandler<T, D, S>;

    type StreamType = InitializedMemoryStream<T, D, S>;

    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Error>
    where
        X: SubscriptionHandler<T, D, S>,
    {
        MemorySubscription::new(self.clone(), MemorySubscriptionOptions, handler)
            .await
            .map_err(|_| Error::Subscribe)
    }

    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <InitializedMemoryStream<T, D, S> as InitializedStream<T, D, S>>::Options,
    ) -> Result<
        InitializedMemoryStream<T, D, S>,
        <Self::StreamType as InitializedStream<T, D, S>>::Error,
    >
    where
        K: Clone + Into<String> + Send,
    {
        InitializedMemoryStream::<T, D, S>::new_with_subjects(
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
            pub struct [<MemorySubject $n>]<T, D, S>
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
                full_subject: String,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [<MemorySubject $n>]<T, D, S>
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
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> From<[<MemorySubject $n>]<T, D, S>> for String
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
                fn from(subject: [<MemorySubject $n>]<T, D, S>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, D, S> [<MemorySubject $n>]<T, D, S>
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
                #[doc = "Creates a new `MemoryStream" $n "`."]
                ///
                /// # Errors
                /// Returns an error if the subject partial contains '.', '*' or '>'
                pub fn new<K>(subject_partial: K) -> Result<Self, Error>
                where
                    K: Clone + Into<String> + Send,
                {
                    let subject = subject_partial.into();
                    if subject.contains('.') || subject.contains('*') || subject.contains('>') {
                        return Err(Error::InvalidSubjectPartial);
                    }
                    Ok(Self {
                        full_subject: subject,
                        _marker: PhantomData,
                    })
                }
            }

            impl<T, D, S> [<PublishableSubject $n>]<T, D, S> for [<MemorySubject $n>]<T, D, S>
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
                type WildcardAllScoped = MemoryUnpublishableSubject<T, D, S>;
                type WildcardAnyScoped = $parent_non_pub<T, D, S>;

                fn all(&self) -> Self::WildcardAllScoped {
                    MemoryUnpublishableSubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_non_pub {
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send,
                {
                    $parent {
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }

            #[doc = $doc_non_pub]
            #[derive(Debug)]
            pub struct [<MemoryUnpublishableSubject $n>]<T, D, S>
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
                full_subject: String,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [<MemoryUnpublishableSubject $n>]<T, D, S>
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
                        full_subject: self.full_subject.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> From<[<MemoryUnpublishableSubject $n>]<T, D, S>> for String
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
                fn from(subject: [<MemoryUnpublishableSubject $n>]<T, D, S>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, D, S> [<Subject $n>]<T, D, S> for [<MemoryUnpublishableSubject $n>]<T, D, S>
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
                type WildcardAllScoped = MemoryUnpublishableSubject<T, D, S>;
                type WildcardAnyScoped = $parent_non_pub<T, D, S>;

                fn all(&self) -> Self::WildcardAllScoped {
                    MemoryUnpublishableSubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_non_pub {
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send,
                {
                    $parent_non_pub {
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
    MemorySubject,
    MemoryUnpublishableSubject,
    "A single-scoped subject that is both publishable and subscribable.",
    "A single-scoped subject that is subscribable."
);

define_scoped_subject!(
    2,
    MemorySubject1,
    MemoryUnpublishableSubject1,
    "A double-scoped subject that is both publishable and subscribable.",
    "A double-scoped subject that is subscribable."
);

define_scoped_subject!(
    3,
    MemorySubject2,
    MemoryUnpublishableSubject2,
    "A triple-scoped subject that is both publishable and subscribable.",
    "A triple-scoped subject that is subscribable."
);
