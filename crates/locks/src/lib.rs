use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// A trait representing a distributed lock manager with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn acquire(&self, key: String) -> Result<(), Self::SE>`: Acquires a lock for a key.
/// - `async fn release(&self, key: String) -> Result<(), Self::SE>`: Releases a lock for a key.
#[async_trait]
pub trait LockManager: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;

    async fn acquire(&self, key: String) -> Result<(), Self::SE>;
    async fn release(&self, key: String) -> Result<(), Self::SE>;
}

/// A trait representing a scoped lock manager with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `fn scope(&self, scope: String) -> Self::Scoped`: Add a scope and make the lock manager usable.
#[async_trait]
pub trait LockManager1: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: LockManager<SE = Self::SE>;

    fn scope(&self, scope: String) -> Self::Scoped;
}

/// A trait representing a double-scoped lock manager with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `fn scope(&self, scope: String) -> Self::Scoped`: Add one of two scopes.
#[async_trait]
pub trait LockManager2: Clone + Send + Sync + 'static {
    type SE: Debug + Error + Send + Sync;
    type Scoped: LockManager1<SE = Self::SE>;

    fn scope(&self, scope: String) -> Self::Scoped;
}
