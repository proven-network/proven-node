use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait Store: Send + Sync {
    type SE: Debug + Error;
    async fn del(&self, key: String) -> Result<(), Self::SE>;
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE>;
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE>;
}
