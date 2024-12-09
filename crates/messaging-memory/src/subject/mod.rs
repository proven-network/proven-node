#![allow(clippy::too_many_lines)]
mod error;

use crate::subscription::{MemorySubscription, MemorySubscriptionOptions};
use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;

use std::collections::HashMap;
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

/// A in-memory publishable subject.
#[derive(Clone, Debug)]
pub struct MemoryPublishableSubject<T = Bytes> {
    full_subject: String,
    _marker: PhantomData<T>,
}

impl<T> From<MemoryPublishableSubject<T>> for String {
    fn from(subject: MemoryPublishableSubject<T>) -> Self {
        subject.full_subject
    }
}

impl<T> MemoryPublishableSubject<T> {
    /// Creates a new in-memory publishable subject.
    ///
    /// # Errors
    /// Returns an error if the subject partial contains '.', '*' or '>'
    pub fn new<K>(subject_partial: K) -> Result<Self, Error>
    where
        K: Into<String> + Send,
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
impl<T> PublishableSubject<T> for MemoryPublishableSubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    async fn publish(&self, data: T) -> Result<(), Self::Error> {
        self.publish_with_headers(data, HashMap::new()).await
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn publish_with_headers<H>(&self, data: T, headers: H) -> Result<(), Self::Error>
    where
        H: Clone + Into<HashMap<String, String>> + Send,
    {
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
                    let headers = headers.clone().into();
                    if headers.is_empty() {
                        let _ = sender.send((data.clone(), None));
                    } else {
                        let _ = sender.send((data.clone(), Some(headers)));
                    }
                }
            }
        }
        Ok(())
    }

    async fn subscribe<X>(&self, handler: X) -> Result<MemorySubscription<X, T>, Error>
    where
        X: SubscriptionHandler<T>,
    {
        let subscription = MemorySubscription::new(
            self.full_subject.clone(),
            MemorySubscriptionOptions,
            handler.clone(),
        )
        .await?;

        Ok(subscription)
    }
}

/// A in-memory subject which can be subscribed only.
#[derive(Clone, Debug)]
pub struct MemorySubject<T = Bytes> {
    full_subject: String,
    _marker: PhantomData<T>,
}

impl<T> From<MemorySubject<T>> for String {
    fn from(subject: MemorySubject<T>) -> Self {
        subject.full_subject
    }
}

impl<T> MemorySubject<T> {
    /// Creates a new in-memory subject.
    ///
    /// # Errors
    /// Returns an error if the subject partial contains '.', '*' or '>'
    pub fn new<K>(subject_partial: K) -> Result<Self, Error>
    where
        K: Into<String> + Send,
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
impl<T> Subject<T> for MemorySubject<T>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;

    async fn subscribe<X>(&self, handler: X) -> Result<MemorySubscription<X, T>, Error>
    where
        X: SubscriptionHandler<T>,
    {
        let subscription = MemorySubscription::new(
            self.full_subject.clone(),
            MemorySubscriptionOptions,
            handler.clone(),
        )
        .await?;

        Ok(subscription)
    }
}

macro_rules! impl_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[derive(Clone, Debug)]
            #[doc = $doc_pub]
            pub struct [< MemoryPublishableSubject $index >]<T = Bytes> {
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> [< MemoryPublishableSubject $index >]<T> {
                /// Creates a new in-memory publishable subject.
                ///
                /// # Errors
                /// Returns an error if the subject partial contains '.', '*' or '>'
                pub fn new<K>(subject_partial: K) -> Result<Self, Error>
                where
                    K: Into<String> + Send,
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
            impl<T> [< PublishableSubject $index >]<T> for [< MemoryPublishableSubject $index >]<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                type Error = Error;

                type Type = T;

                type Scoped = $parent_pub<T>;

                type WildcardAllScoped = MemorySubject<T>;

                type WildcardAnyScoped = $parent_sub<T>;

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_sub {
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    MemorySubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_pub {
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _marker: PhantomData,
                    }
                }
            }

            #[derive(Clone, Debug)]
            #[doc = $doc_sub]
            pub struct [< MemorySubject $index >]<T = Bytes> {
                full_subject: String,
                _marker: PhantomData<T>,
            }

            impl<T> [< MemorySubject $index >]<T> {
                /// Creates a new in-memory subject.
                ///
                /// # Errors
                /// Returns an error if the subject partial contains '.', '*' or '>'
                pub fn new<K>(subject_partial: K) -> Result<Self, Error>
                where
                    K: Into<String> + Send,
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
            impl<T> [< Subject $index >]<T> for [< MemorySubject $index >]<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                type Error = Error;

                type Type = T;

                type Scoped = $parent_sub<T>;

                type WildcardAllScoped = MemorySubject<T>;

                fn any(&self) -> Self::Scoped {
                    $parent_sub {
                        full_subject: format!("{}.*", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    MemorySubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _marker: PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_sub {
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
    MemoryPublishableSubject,
    MemorySubject,
    "A single-scoped in-memory publishable subject.",
    "A single-scoped in-memory subject."
);

impl_scoped_subject!(
    2,
    MemoryPublishableSubject1,
    MemorySubject1,
    "A double-scoped in-memory publishable subject.",
    "A double-scoped in-memory subject."
);

impl_scoped_subject!(
    3,
    MemoryPublishableSubject2,
    MemorySubject2,
    "A triple-scoped in-memory publishable subject.",
    "A triple-scoped in-memory subject."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publishable_subject_validation() {
        // Valid names should work
        assert!(MemoryPublishableSubject::<()>::new("foo").is_ok());
        assert!(MemoryPublishableSubject::<()>::new("foo-bar").is_ok());
        assert!(MemoryPublishableSubject::<()>::new("foo_bar").is_ok());
        assert!(MemoryPublishableSubject::<()>::new("123").is_ok());

        // Invalid names should fail
        assert!(matches!(
            MemoryPublishableSubject::<()>::new("foo.bar"),
            Err(Error::InvalidSubjectPartial)
        ));
        assert!(matches!(
            MemoryPublishableSubject::<()>::new("foo*"),
            Err(Error::InvalidSubjectPartial)
        ));
        assert!(matches!(
            MemoryPublishableSubject::<()>::new("foo>"),
            Err(Error::InvalidSubjectPartial)
        ));
    }

    #[test]
    fn test_subject_validation() {
        // Valid names should work
        assert!(MemorySubject::<()>::new("foo").is_ok());
        assert!(MemorySubject::<()>::new("foo-bar").is_ok());
        assert!(MemorySubject::<()>::new("foo_bar").is_ok());
        assert!(MemorySubject::<()>::new("123").is_ok());

        // Invalid names should fail
        assert!(matches!(
            MemorySubject::<()>::new("foo.bar"),
            Err(Error::InvalidSubjectPartial)
        ));
        assert!(matches!(
            MemorySubject::<()>::new("foo*"),
            Err(Error::InvalidSubjectPartial)
        ));
        assert!(matches!(
            MemorySubject::<()>::new("foo>"),
            Err(Error::InvalidSubjectPartial)
        ));
    }
}
