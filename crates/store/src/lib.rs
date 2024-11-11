use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;

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
    type SE: Debug + Error + Send + Sync;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::SE>;
    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::SE>;
    async fn keys(&self) -> Result<Vec<String>, Self::SE>;
    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::SE>;
}

#[async_trait]
/// A trait representing a scoped key-value store with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
/// - `Scoped`: The scoped store type that implements the `Store` trait.
///
/// # Required Methods
/// - `fn scope(&self, scope: String) -> Self::Scoped`: Add a scope and make the store usable.
pub trait Store1: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: Store<SE = Self::SE>;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped;
}

#[async_trait]
/// A trait representing a dobule-scoped key-value store with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
/// - `Scoped`: The scoped store type that implements the `Store1` trait.
///
/// # Required Methods
/// - `fn scope(&self, scope: String) -> Self::Scoped`: Add a scope and make the store usable.
pub trait Store2: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: Store1<SE = Self::SE>;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped;
}

/// A trait representing a tripe-scoped key-value store with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
/// - `Scoped`: The double-scoped store type that implements the `Store2` trait.
///
/// # Required Methods
/// - `fn scope(&self, scope: String) -> Self::Scoped`: Add one of three scopee.
#[async_trait]
pub trait Store3: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: Store2<SE = Self::SE>;

    fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped;
}
