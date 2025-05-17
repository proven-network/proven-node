//! Abstract interface for managing system-global distributed locks.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::error::Error;
use std::fmt::Debug;

use async_trait::async_trait;

/// Marker trait for `LockManager` errors
pub trait LockManagerError: Debug + Error + Send + Sync + 'static {}

/// Represents the current status of a lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LockStatus {
    /// The lock is free.
    Free,

    /// The lock is held by another instance, identified by the String.
    HeldByOther(String),

    /// The lock is held by the current instance/manager.
    HeldBySelf,
}

/// A trait representing a distributed lock manager with asynchronous operations.
/// Locks are acquired with a TTL and are represented by a guard type that
/// automatically releases the lock when dropped.
#[async_trait]
pub trait LockManager: Send + Sync + 'static {
    /// The error type for lock operations.
    type Error: LockManagerError + Send + Sync + 'static;

    /// The guard type that releases the lock on drop.
    type Guard: Send + Sync + 'static;

    /// Checks the status of a lock.
    ///
    /// # Arguments
    /// * `resource_id`: The unique identifier for the resource.
    async fn check(&self, resource_id: String) -> Result<LockStatus, Self::Error>;

    /// Attempts to acquire a lock, waiting indefinitely until it becomes available.
    ///
    /// On successful acquisition, returns a lock guard. The lock is automatically
    /// released when the guard is dropped.
    ///
    /// # Arguments
    /// * `resource_id`: A unique identifier for the resource to be locked.
    async fn lock(&self, resource_id: String) -> Result<Self::Guard, Self::Error>;

    /// Attempts to acquire a lock without waiting.
    ///
    /// If the lock is acquired successfully, returns `Ok(Some(Guard))`.
    /// If the lock is currently held by another, returns `Ok(None)`.
    /// If an error occurs during the attempt, returns `Err(Self::Error)`.
    ///
    /// # Arguments
    /// * `resource_id`: A unique identifier for the resource to be locked.
    async fn try_lock(&self, resource_id: String) -> Result<Option<Self::Guard>, Self::Error>;
}

macro_rules! define_scoped_lock_manager {
    ($name:ident, $parent:ident, $doc:expr_2021) => {
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
                S: AsRef<str> + Send;
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
