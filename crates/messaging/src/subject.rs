use crate::subscription::Subscription;
use crate::subscription_handler::SubscriptionHandler;
use crate::Message;

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for subject errors
pub trait SubjectError: Error + Send + Sync + 'static {}

/// Capability to publish messages (lost if wildcard scoped)
#[async_trait]
pub trait PublishableSubject: Subject {
    /// Publish a message to the subject.
    async fn publish(&self, message: Message<Self::Type>) -> Result<(), Self::Error>;

    /// Publish a message to the subject and await a response.
    async fn request(
        &self,
        message: Message<Self::Type>,
    ) -> Result<Message<Self::ResponseType>, Self::Error>;
}

/// Base subject trait
#[async_trait]
pub trait Subject: Clone + Into<String> + Send + Sync + 'static {
    /// The error type for the subject.
    type Error: SubjectError;

    /// The type of messages published to the subject.
    type Type: Clone + Debug + Send + Sync + 'static;

    /// The type of responses expected from requests.
    type ResponseType: Clone + Debug + Send + Sync + 'static;

    /// The type of subscription returned by the subject.
    type SubscriptionType<X>: Subscription<X, Type = Self::Type, ResponseType = Self::ResponseType>
    where
        X: SubscriptionHandler<Type = Self::Type, ResponseType = Self::ResponseType>;

    /// Subscribe to messages on the subject.
    async fn subscribe<X>(&self, handler: X) -> Result<Self::SubscriptionType<X>, Self::Error>
    where
        X: SubscriptionHandler<Type = Self::Type, ResponseType = Self::ResponseType>;
}

macro_rules! define_scoped_subject {
    ($index:expr, $parent_pub:ident, $parent_sub:ident, $doc_pub:expr, $doc_sub:expr) => {
        paste::paste! {
            #[async_trait]
            #[doc = $doc_pub]
            pub trait [< PublishableSubject $index >] {
                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync + 'static;

                /// Response type for the stream.
                type ResponseType: Clone + Debug + Send + Sync + 'static;

                /// The scoped type for the subject.
                type Scoped: $parent_pub<Type = Self::Type, ResponseType = Self::ResponseType>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<Type = Self::Type, ResponseType = Self::ResponseType>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<Type = Self::Type, ResponseType = Self::ResponseType>;

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
            pub trait [< Subject $index >]
            where
                Self: Clone + Send + Sync + 'static,
            {
                /// The type of data in the stream.
                type Type: Clone + Debug + Send + Sync + 'static;

                /// Response type for the stream.
                type ResponseType: Clone + Debug + Send + Sync + 'static;

                /// The scoped type for the subject.
                type Scoped: $parent_sub<Type = Self::Type, ResponseType = Self::ResponseType>;

                /// The wildcard all scoped type for the subject.
                type WildcardAllScoped: Subject<Type = Self::Type, ResponseType = Self::ResponseType>;

                /// The wildcard any scoped type for the subject.
                type WildcardAnyScoped: $parent_sub<Type = Self::Type, ResponseType = Self::ResponseType>;

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
