use crate::stream::Stream;
use crate::subscription::Subscription;
use crate::subscription_handler::SubscriptionHandler;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for subject errors
pub trait SubjectError: Error + Send + Sync + 'static {}

/// Capability to publish messages (lost if wildcard scoped)
#[async_trait]
pub trait PublishableSubject<T, D, S>: Subject<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// Publish a message to the subject.
    async fn publish(&self, message: Message<T>) -> Result<(), Self::Error>;

    /// Publish a message to the subject and await a response.
    async fn request<X>(
        &self,
        message: Message<T>,
    ) -> Result<Message<X::ResponseType>, Self::Error>
    where
        X: SubscriptionHandler<T, D, S>;
}

/// Base subject trait
#[async_trait]
pub trait Subject<T, D, S>: Clone + Debug + Into<String> + Send + Sync + 'static
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Error + Send + Sync + 'static,
    S: Debug + Error + Send + Sync + 'static,
{
    /// The error type for the subject.
    type Error: SubjectError;

    /// The type of subscription returned by the subject.
    type SubscriptionType<X>: Subscription<Self, X, T, D, S>
    where
        X: SubscriptionHandler<T, D, S>;

    /// The type of stream created by `to_stream`.
    type StreamType: Stream<T, D, S>;

    /// Subscribe to messages on the subject.
    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: SubscriptionHandler<T, D, S>;

    /// Convert the subject to a stream.
    async fn to_stream<K>(
        &self,
        stream_name: K,
        options: <Self::StreamType as Stream<T, D, S>>::Options,
    ) -> Result<Self::StreamType, <Self::StreamType as Stream<T, D, S>>::Error>
    where
        K: Clone + Into<String> + Send,
        Self::StreamType: Stream<T, D, S>;
}

macro_rules! define_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc_pub]
            pub trait [< PublishableSubject $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Error + Send + Sync + 'static,
                S: Debug + Error + Send + Sync + 'static,
            {
                /// The scoped type for the subject.
                type Scoped: $parent_pub<T, D, S>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T, D, S>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<T, D, S>;

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
            pub trait [< Subject $index >]<T, D, S>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Error + Send + Sync + 'static,
                S: Debug + Error + Send + Sync + 'static,
            {
                /// The scoped type for the subject.
                type Scoped: $parent_sub<T, D, S>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<T, D, S>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<T, D, S>;

                /// Refines the subject with a greedy wildcard scope.
                fn all(&self) -> Self::WildcardAllScoped;

                /// Refines the subject with a wildcard scope.
                fn any(&self) -> Self::WildcardAnyScoped;

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
