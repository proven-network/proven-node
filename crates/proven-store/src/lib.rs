use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;

    // Asynchronous methods
    async fn del(&self, key: String) -> Result<(), Self::SE>;
    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE>;
    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE>;

    // Blocking methods
    fn del_blocking(&self, key: String) -> Result<(), Self::SE>;
    fn get_blocking(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE>;
    fn put_blocking(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE>;
}
