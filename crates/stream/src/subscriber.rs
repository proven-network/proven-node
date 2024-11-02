use std::error::Error;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;

use async_trait::async_trait;

#[async_trait]
pub trait StreamSubscriber<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: Debug + Error + Send + Sync,
{
    type SubscriberError: Debug + Error + Send + Sync;

    async fn subscribe(
        &self,
        subject: String,
        handler: impl Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, HandlerError>> + Send>>
            + Send
            + Sync,
    ) -> Result<(), Self::SubscriberError>;
}

#[async_trait]
pub trait StreamSubscriber1<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: Debug + Error + Send + Sync,
{
    type SubscriberError: Debug + Error + Send + Sync;
    type Scoped: StreamSubscriber<HandlerError, SubscriberError = Self::SubscriberError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait StreamSubscriber2<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: Debug + Error + Send + Sync,
{
    type SubscriberError: Debug + Error + Send + Sync;
    type Scoped: StreamSubscriber1<HandlerError, SubscriberError = Self::SubscriberError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait StreamSubscriber3<HandlerError>: Clone + Send + Sync + 'static
where
    HandlerError: Debug + Error + Send + Sync,
{
    type SubscriberError: Debug + Error + Send + Sync;
    type Scoped: StreamSubscriber2<HandlerError, SubscriberError = Self::SubscriberError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
