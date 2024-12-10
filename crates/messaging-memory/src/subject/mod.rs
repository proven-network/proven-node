#![allow(clippy::too_many_lines)]
mod error;

use crate::subscription::{MemorySubscription, MemorySubscriptionOptions};
use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;
use proven_messaging::Message;

use std::fmt::Debug;
use std::marker::PhantomData;

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::subject::{
    PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3, Subject,
    Subject1, Subject2, Subject3,
};
use proven_messaging::subscription::Subscription;
use proven_messaging::subscription_handler::SubscriptionHandler;

/// A subject that is both publishable and subscribable.
#[derive(Clone, Debug)]
pub struct MemorySubject<T = Bytes, R = Bytes>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    full_subject: String,
    _marker: PhantomData<(T, R)>,
}

impl<T, R> From<MemorySubject<T, R>> for String
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    fn from(subject: MemorySubject<T, R>) -> Self {
        subject.full_subject
    }
}

impl<T, R> MemorySubject<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
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
impl<T, R> Subject for MemorySubject<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;
    type Type = T;
    type ResponseType = R;
    type SubscriptionType<X>
        = MemorySubscription<X, T, R>
    where
        X: SubscriptionHandler<Type = T, ResponseType = R>;

    async fn subscribe<X>(&self, handler: X) -> Result<MemorySubscription<X, T, R>, Error>
    where
        X: SubscriptionHandler<Type = T, ResponseType = R>,
    {
        MemorySubscription::new(
            self.full_subject.clone(),
            MemorySubscriptionOptions,
            handler,
        )
        .await
        .map_err(|_| Error::Subscribe)
    }
}

// Only implement Publishable for non-wildcard subjects
#[async_trait]
impl<T, R> PublishableSubject for MemorySubject<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    #[allow(clippy::significant_drop_tightening)]
    async fn publish(&self, message: Message<T>) -> Result<(), Error> {
        let mut state = GLOBAL_STATE.lock().await;
        if !state.has::<SubjectState<Self::Type>>() {
            state.put(SubjectState::<Self::Type>::default());
        }
        let subject_state = state.borrow::<SubjectState<Self::Type>>();
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

    async fn request(&self, _message: Message<T>) -> Result<Message<R>, Error> {
        unimplemented!()
    }
}

/// A subject that is not publishable (contains a wildcard).
#[derive(Clone, Debug)]
pub struct MemoryUnpublishableSubject<T = Bytes, R = Bytes>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    full_subject: String,
    _marker: PhantomData<(T, R)>,
}

impl<T, R> From<MemoryUnpublishableSubject<T, R>> for String
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    fn from(subject: MemoryUnpublishableSubject<T, R>) -> Self {
        subject.full_subject
    }
}

impl<T, R> From<MemorySubject<T, R>> for MemoryUnpublishableSubject<T, R>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    fn from(subject: MemorySubject<T, R>) -> Self {
        Self {
            full_subject: subject.full_subject,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<T, R> Subject for MemoryUnpublishableSubject<T, R>
where
    T: Clone + Debug + Send + Sync + 'static,
    R: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;
    type Type = T;
    type ResponseType = R;
    type SubscriptionType<X>
        = MemorySubscription<X, T, R>
    where
        X: SubscriptionHandler<Type = T, ResponseType = R>;

    async fn subscribe<X>(&self, handler: X) -> Result<MemorySubscription<X, T, R>, Error>
    where
        X: SubscriptionHandler<Type = T, ResponseType = R>,
    {
        MemorySubscription::new(
            self.full_subject.clone(),
            MemorySubscriptionOptions,
            handler,
        )
        .await
        .map_err(|_| Error::Subscribe)
    }
}

macro_rules! define_scoped_subject {
    ($n:expr, $parent:ident, $parent_non_pub:ident, $doc:expr, $doc_non_pub:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct [<MemorySubject $n>]<T, R>
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                full_subject: String,
                _marker: PhantomData<(T, R)>,
            }

            impl<T, R> From<[<MemorySubject $n>]<T, R>> for String
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                fn from(subject: [<MemorySubject $n>]<T, R>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, R> [<MemorySubject $n>]<T, R>
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
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

            impl<T, R> [<PublishableSubject $n>] for [<MemorySubject $n>]<T, R>
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                type Type = T;
                type ResponseType = R;
                type Scoped = $parent<T, R>;
                type WildcardAllScoped = MemoryUnpublishableSubject<T, R>;
                type WildcardAnyScoped = $parent_non_pub<T, R>;

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
            #[derive(Clone, Debug)]
            pub struct [<MemoryUnpublishableSubject $n>]<T, R>
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                full_subject: String,
                _marker: PhantomData<(T, R)>,
            }

            impl<T, R> From<[<MemoryUnpublishableSubject $n>]<T, R>> for String
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                fn from(subject: [<MemoryUnpublishableSubject $n>]<T, R>) -> Self {
                    subject.full_subject
                }
            }

            impl<T, R> [<Subject $n>] for [<MemoryUnpublishableSubject $n>]<T, R>
            where
                T: Clone + Debug + Send + Sync + 'static,
                R: Clone + Debug + Send + Sync + 'static,
            {
                type Type = T;
                type ResponseType = R;
                type Scoped = $parent_non_pub<T, R>;
                type WildcardAllScoped = MemoryUnpublishableSubject<T, R>;
                type WildcardAnyScoped = $parent_non_pub<T, R>;

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
