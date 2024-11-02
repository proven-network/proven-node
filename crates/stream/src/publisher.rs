use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait StreamPublisher: Clone + Send + Sync + 'static {
    type PublisherError: Debug + Error + Send + Sync;

    async fn publish(&self, subject: String, data: Vec<u8>) -> Result<(), Self::PublisherError>;
    async fn request(
        &self,
        subject: String,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, Self::PublisherError>;
}

#[async_trait]
pub trait StreamPublisher1: Clone + Send + Sync + 'static {
    type PublisherError: Debug + Error + Send + Sync;
    type Scoped: StreamPublisher<PublisherError = Self::PublisherError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait StreamPublisher2: Clone + Send + Sync + 'static {
    type PublisherError: Debug + Error + Send + Sync;
    type Scoped: StreamPublisher1<PublisherError = Self::PublisherError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait StreamPublisher3: Clone + Send + Sync + 'static {
    type PublisherError: Debug + Error + Send + Sync;
    type Scoped: StreamPublisher2<PublisherError = Self::PublisherError>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
