use crate::subscription::Subscription;
use crate::subscription_handler::SubscriptionHandler;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for subject errors
pub trait SubjectError: Error + Send + Sync + 'static {}

/// A trait representing a subject.
#[async_trait]
pub trait PublishableSubject<T>
where
    Self: Clone + Into<String> + Send + Sized + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: SubjectError;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync = T;

    /// Publishes the given data with no expectation of a response.
    async fn publish(&self, message: Message<T>) -> Result<(), Self::Error>;

    /// Subscribes to the subject and processes messages with the given handler.
    async fn subscribe<X>(&self, handler: X) -> Result<impl Subscription<X, T>, Self::Error>
    where
        X: SubscriptionHandler<T>;
}

/// A trait representing a subject.
#[async_trait]
pub trait Subject<T>
where
    Self: Clone + Into<String> + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: SubjectError;

    /// Subscribes to the subject and processes messages with the given handler.
    async fn subscribe<X>(&self, handler: X) -> Result<impl Subscription<X, T>, Self::Error>
    where
        X: SubscriptionHandler<T>;
}

macro_rules! define_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc_pub]
            pub trait [< PublishableSubject $index >]<T>
            where
                Self: Clone + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: SubjectError;

                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync = T;

                /// The scoped type for the subject.
                type Scoped: $parent_pub<T>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<T>;

                /// Refines the subject with a greedy wildcard scope.
                fn all(&self) -> Self::WildcardAllScoped;

                /// Refines the subject with a wildcard scope.
                fn any(&self) -> Self::WildcardAnyScoped;

                /// Refines the subject with the given concrete scope.
                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send;
            }

            #[async_trait]
            #[doc = $doc_sub]
            pub trait [< Subject $index >]<T>
            where
                Self: Clone + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: SubjectError;

                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync = T;

                /// The scoped type for the subject.
                type Scoped: $parent_sub<T>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T>;

                /// Refines the subject with a greedy wildcard scope.
                fn all(&self) -> Self::WildcardAllScoped;

                /// Refines the subject with a wildcard scope.
                fn any(&self) -> Self::Scoped;

                /// Refines the subject with the given concrete scope.
                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Clone + Into<String> + Send;
            }
        }
    };
}

define_scoped_subject!(
    1,
    PublishableSubject,
    Subject,
    "A trait representing a single-scoped publishable subject.",
    "A trait representing a single-scoped subject."
);

define_scoped_subject!(
    2,
    PublishableSubject1,
    Subject1,
    "A trait representing a double-scoped publishable subject.",
    "A trait representing a double-scoped subject."
);

define_scoped_subject!(
    3,
    PublishableSubject2,
    Subject2,
    "A trait representing a triple-scoped publishable subject.",
    "A trait representing a triple-scoped subject."
);
