use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;

    async fn del(&self, key: String) -> Result<(), Self::SE>;
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE>;
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE>;
}

#[async_trait]
pub trait Store1: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: Store<SE = Self::SE>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

#[async_trait]
pub trait Store2: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: Store1<SE = Self::SE>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
