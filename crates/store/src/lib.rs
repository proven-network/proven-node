use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for store errors
pub trait StoreError: Debug + Error + Send + Sync {}

/// A trait representing a key-value store with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn del(&self, key: String) -> Result<(), Self::SE>`: Deletes a key from the store.
/// - `async fn get(&self, key: String) -> Result<Option<Bytes>, Self::SE>`: Retrieves the value associated with a key.
/// - `async fn keys(&self) -> Result<Vec<String>, Self::SE>`: Retrieves all keys in the store.
/// - `async fn put(&self, key: String, bytes: Bytes) -> Result<(), Self::SE>`: Stores a key-value pair.
#[async_trait]
pub trait Store: Clone + Send + Sync + 'static {
    type Error: StoreError;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error>;
    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error>;
    async fn keys(&self) -> Result<Vec<String>, Self::Error>;
    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::Error>;
}

macro_rules! define_scoped_store {
    ($name:ident, $parent:ident, $doc:expr) => {
        #[async_trait]
        #[doc = $doc]
        pub trait $name: Clone + Send + Sync + 'static {
            type Error: StoreError;
            type Scoped: $parent<Error = Self::Error>;

            fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped;
        }
    };
}

define_scoped_store!(
    Store1,
    Store,
    "A trait representing a single-scoped key-value store with asynchronous operations."
);
define_scoped_store!(
    Store2,
    Store1,
    "A trait representing a double-scoped key-value store with asynchronous operations."
);
define_scoped_store!(
    Store3,
    Store2,
    "A trait representing a triple-scoped key-value store with asynchronous operations."
);
