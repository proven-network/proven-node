use crate::subscription::Subscription;
use crate::subscription_handler::SubscriptionHandler;

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for subject errors
pub trait SubjectError: Error + Send + Sync + 'static {}

/// A trait representing a subject.
#[async_trait]
pub trait PublishableSubject<T, DE, SE>
where
    Self: Clone + Into<String> + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: SubjectError;

    /// The type of data in the stream.
    type Type: Clone + Debug + Send + Sync = T;

    /// Publishes the given data with no expectation of a response.
    async fn publish(&self, data: T) -> Result<(), Self::Error>;

    /// Publishes the given data with the given headers and no expectation of a response.
    async fn publish_with_headers<H>(&self, data: T, headers: H) -> Result<(), Self::Error>
    where
        H: Clone + Into<HashMap<String, String>> + Send;

    /// Subscribes to the subject and processes messages with the given handler.
    async fn subscribe<X, Y>(&self, options: Y::Options, handler: X) -> Result<Y, Y::Error>
    where
        X: SubscriptionHandler<T, DE, SE>,
        Y: Subscription<X, T, DE, SE>;
}

/// A trait representing a subject.
#[async_trait]
pub trait Subject<T, DE, SE>
where
    Self: Clone + Into<String> + Send + Sync + 'static,
    DE: Error + Send + Sync + 'static,
    SE: Error + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    /// The error type for the stream.
    type Error: SubjectError;

    /// Subscribes to the subject and processes messages with the given handler.
    async fn subscribe<X, Y>(&self, options: Y::Options, handler: X) -> Result<Y, Y::Error>
    where
        X: SubscriptionHandler<T, DE, SE>,
        Y: Subscription<X, T, DE, SE>;
}

macro_rules! define_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc_pub]
            pub trait [< PublishableSubject $index >]<T, DE, SE>
            where
                Self: Clone + Send + Sync + 'static,
                DE: Error + Send + Sync + 'static,
                SE: Error + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: SubjectError;

                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync = T;

                /// The scoped type for the subject.
                type Scoped: $parent_pub<T, DE, SE>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T, DE, SE>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<T, DE, SE>;

                /// Refines the subject with a greedy wildcard scope.
                fn all(&self) -> Self::WildcardAllScoped;

                /// Refines the subject with a wildcard scope.
                fn any(&self) -> Self::WildcardAnyScoped;

                /// Refines the subject with the given concrete scope.
                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send;
            }

            #[async_trait]
            #[doc = $doc_sub]
            pub trait [< Subject $index >]<T, DE, SE>
            where
                Self: Clone + Send + Sync + 'static,
                DE: Error + Send + Sync + 'static,
                SE: Error + Send + Sync + 'static,
                T: Clone + Debug + Send + Sync + 'static,
            {
                /// The error type for the stream.
                type Error: SubjectError;

                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync = T;

                /// The scoped type for the subject.
                type Scoped: $parent_sub<T, DE, SE>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T, DE, SE>;

                /// Refines the subject with a greedy wildcard scope.
                fn all(&self) -> Self::WildcardAllScoped;

                /// Refines the subject with a wildcard scope.
                fn any(&self) -> Self::Scoped;

                /// Refines the subject with the given concrete scope.
                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: Into<String> + Send;
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
