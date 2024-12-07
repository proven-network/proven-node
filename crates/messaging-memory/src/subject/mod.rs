#![allow(clippy::too_many_lines)]
mod error;

use crate::{SubjectState, GLOBAL_STATE};
pub use error::Error;

use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use bytes::Bytes;
use proven_messaging::{
    Handler, PublishableSubject, PublishableSubject1, PublishableSubject2, PublishableSubject3,
    Subject, Subject1, Subject2, Subject3, Subscriber,
};

/// A in-memory publishable subject.
#[derive(Clone, Debug)]
pub struct InMemoryPublishableSubject<T = Bytes> {
    full_subject: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> InMemoryPublishableSubject<T> {
    /// Creates a new in-memory publishable subject.
    pub fn new<K>(subject_partial: K) -> Self
    where
        K: Into<String> + Send,
    {
        Self {
            full_subject: subject_partial.into(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> PublishableSubject<T> for InMemoryPublishableSubject<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;
    type Type = T;

    async fn publish(&self, data: T) -> Result<(), Self::Error> {
        self.publish_with_headers(data, std::collections::HashMap::new())
            .await
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

    async fn subscribe<X, Y>(&self, handler: X) -> Result<Y, Y::Error>
    where
        X: Handler<Self::Type>,
        Y: Subscriber<Self::Type, X>,
    {
        Ok(Y::new(self.full_subject.clone(), handler.clone()).await?)
    }
}

/// A in-memory subject which can be subscribed only.
#[derive(Clone, Debug)]
pub struct InMemorySubject<T = Bytes> {
    full_subject: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> InMemorySubject<T> {
    /// Creates a new in-memory subject.
    pub fn new<K>(subject_partial: K) -> Self
    where
        K: Into<String> + Send,
    {
        Self {
            full_subject: subject_partial.into(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> Subject<T> for InMemorySubject<T>
where
    T: Clone + Debug + Send + Sync + 'static,
{
    type Error = Error;
    type Type = T;

    async fn subscribe<X, Y>(&self, handler: X) -> Result<Y, Y::Error>
    where
        X: Handler<Self::Type>,
        Y: Subscriber<Self::Type, X>,
    {
        Ok(Y::new(self.full_subject.clone(), handler.clone()).await?)
    }
}

macro_rules! impl_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        preinterpret::preinterpret! {
            [!set! #pub_full_subject = [!ident! InMemoryPublishableSubject $index]]
            [!set! #sub_full_subject = [!ident! InMemorySubject $index]]
            [!set! #pub_trait_full_subject = [!ident! PublishableSubject $index]]
            [!set! #sub_trait_full_subject = [!ident! Subject $index]]

            #[derive(Clone, Debug)]
            #[doc = $doc_pub]
            pub struct #pub_full_subject<T = Bytes> {
                full_subject: String,
                _phantom: std::marker::PhantomData<T>,
            }

            impl<T> #pub_full_subject<T> {
                /// Creates a new in-memory publishable subject.
                pub fn new<K>(subject_partial: K) -> Self
                where
                    K: Into<String> + Send,
                {
                    Self {
                        full_subject: subject_partial.into(),
                        _phantom: std::marker::PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T> #pub_trait_full_subject<T> for #pub_full_subject<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                type Error = Error;
                type Type = T;
                type Scoped = $parent_pub<T>;
                type WildcardAllScoped = InMemorySubject<T>;
                type WildcardAnyScoped = $parent_sub<T>;

                fn any(&self) -> Self::WildcardAnyScoped {
                    $parent_sub {
                        full_subject: format!("{}.*", self.full_subject),
                        _phantom: std::marker::PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    InMemorySubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _phantom: std::marker::PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_pub {
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _phantom: std::marker::PhantomData,
                    }
                }
            }

            #[derive(Clone, Debug)]
            #[doc = $doc_sub]
            pub struct #sub_full_subject<T = Bytes> {
                full_subject: String,
                _phantom: std::marker::PhantomData<T>,
            }

            impl<T> #sub_full_subject<T> {
                /// Creates a new in-memory subject.
                pub fn new<K>(subject_partial: K) -> Self
                where
                    K: Into<String> + Send,
                {
                    Self {
                        full_subject: subject_partial.into(),
                        _phantom: std::marker::PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T> #sub_trait_full_subject<T> for #sub_full_subject<T>
            where
                T: Clone + Debug + Send + Sync + 'static,
            {
                type Error = Error;
                type Type = T;
                type Scoped = $parent_sub<T>;
                type WildcardAllScoped = InMemorySubject<T>;

                fn any(&self) -> Self::Scoped {
                    $parent_sub {
                        full_subject: format!("{}.*", self.full_subject),
                        _phantom: std::marker::PhantomData,
                    }
                }

                fn all(&self) -> Self::WildcardAllScoped {
                    InMemorySubject {
                        full_subject: format!("{}.>", self.full_subject),
                        _phantom: std::marker::PhantomData,
                    }
                }

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send,
                {
                    $parent_sub {
                        full_subject: format!("{}.{}", self.full_subject, scope.into()),
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_subject!(
    1,
    InMemoryPublishableSubject,
    InMemorySubject,
    "A single-scoped in-memory publishable subject.",
    "A single-scoped in-memory subject."
);

impl_scoped_subject!(
    2,
    InMemoryPublishableSubject1,
    InMemorySubject1,
    "A double-scoped in-memory publishable subject.",
    "A double-scoped in-memory subject."
);

impl_scoped_subject!(
    3,
    InMemoryPublishableSubject2,
    InMemorySubject2,
    "A triple-scoped in-memory publishable subject.",
    "A triple-scoped in-memory subject."
);
