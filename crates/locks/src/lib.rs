//! Abstract interface for managing system-global distributed locks.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for `LockManager` errors
pub trait LockManagerError: Debug + Error + Send + Sync {}

/// A trait representing a distributed lock manager with asynchronous operations.
///
/// # Associated Types
/// - `SE`: The error type that implements `Debug`, `Error`, `Send`, and `Sync`.
///
/// # Required Methods
/// - `async fn acquire(&self, key: String) -> Result<(), Self::SE>`: Acquires a lock for a key.
/// - `async fn release(&self, key: String) -> Result<(), Self::SE>`: Releases a lock for a key.
#[async_trait]
pub trait LockManager
where
    Self: Clone + Send + Sync + 'static,
{
    /// The error type for the lock manager.
    type Error: LockManagerError;

    /// Acquires a lock for a resource.
    async fn acquire(&self, resource_id: String) -> Result<(), Self::Error>;

    /// Releases a lock for a resource.
    async fn release(&self, resource_id: String) -> Result<(), Self::Error>;
}

macro_rules! define_scoped_lock_manager {
    ($name:ident, $parent:ident, $doc:expr) => {
        #[async_trait]
        #[doc = $doc]
        pub trait $name
        where
            Self: Clone + Send + Sync + 'static,
        {
            /// The error type for the lock manager.
            type Error: LockManagerError;

            /// The scoped lock manager type.
            type Scoped: $parent<Error = Self::Error>;

            /// Creates a scoped lock manager.
            fn scope<S>(&self, scope: S) -> Self::Scoped
            where
                S: Clone + Into<String> + Send;
        }
    };
}

define_scoped_lock_manager!(
    LockManager1,
    LockManager,
    "A trait representing a single-scoped lock manager with asynchronous operations."
);
define_scoped_lock_manager!(
    LockManager2,
    LockManager1,
    "A trait representing a double-scoped lock manager with asynchronous operations."
);
define_scoped_lock_manager!(
    LockManager3,
    LockManager2,
    "A trait representing a triple-scoped lock manager with asynchronous operations."
);
