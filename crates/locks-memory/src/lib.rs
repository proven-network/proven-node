//! In-memory (single node) implementation of locks for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

use error::Error;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use proven_locks::{LockManager, LockManager1, LockManager2, LockManager3};
use tokio::sync::Mutex;

/// In-memory lock manager.
#[derive(Clone, Debug, Default)]
pub struct MemoryLockManager {
    map: Arc<Mutex<HashSet<String>>>,
    prefix: Option<String>,
}

impl MemoryLockManager {
    /// Creates a new instance of `MemoryLockManager`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashSet::new())),
            prefix: None,
        }
    }

    fn with_scope(prefix: String) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashSet::new())),
            prefix: Some(prefix),
        }
    }

    fn get_key(&self, key: String) -> String {
        match &self.prefix {
            Some(prefix) => format!("{prefix}:{key}"),
            None => key,
        }
    }
}

#[async_trait]
impl LockManager for MemoryLockManager {
    type Error = Error;

    async fn acquire(&self, resource_id: String) -> Result<(), Self::Error> {
        let key = self.get_key(resource_id);

        self.map.lock().await.insert(key);

        Ok(())
    }

    async fn release(&self, resource_id: String) -> Result<(), Self::Error> {
        let key = self.get_key(resource_id);

        self.map.lock().await.remove(&key);

        Ok(())
    }
}

macro_rules! impl_scoped_lock_manager {
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl $name for MemoryLockManager {
            type Error = Error;
            type Scoped = Self;

            fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
                let new_scope = match &self.prefix {
                    Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                    None => scope.into(),
                };
                Self::with_scope(new_scope)
            }
        }
    };
}

impl_scoped_lock_manager!(LockManager1, LockManager);
impl_scoped_lock_manager!(LockManager2, LockManager1);
impl_scoped_lock_manager!(LockManager3, LockManager2);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_and_release_lock() {
        let manager = MemoryLockManager::new();
        let key = "test_key".to_string();

        // Acquire the lock
        assert!(manager.acquire(key.clone()).await.is_ok());

        // Ensure the lock is held
        assert!(manager.map.lock().await.contains(&key));

        // Release the lock
        assert!(manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released
        assert!(!manager.map.lock().await.contains(&key));
    }

    #[tokio::test]
    async fn test_scoped_lock_manager() {
        let unscoped_manager = MemoryLockManager::new();
        let scoped_manager = LockManager1::scope(&unscoped_manager, "scope".to_string());
        let key = "test_key".to_string();
        let scoped_key = "scope:test_key".to_string();

        // Acquire the lock with scoped manager
        assert!(scoped_manager.acquire(key.clone()).await.is_ok());

        // Ensure the lock is held with scoped key
        assert!(scoped_manager.map.lock().await.contains(&scoped_key));

        // Release the lock with scoped manager
        assert!(scoped_manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released with scoped key
        assert!(!scoped_manager.map.lock().await.contains(&key));
    }

    #[tokio::test]
    async fn test_nested_scoped_lock_manager() {
        let unscoped_manager = MemoryLockManager::new();
        let partially_scoped_manager = LockManager2::scope(&unscoped_manager, "scope1".to_string());
        let scoped_manager = LockManager1::scope(&partially_scoped_manager, "scope2".to_string());
        let key = "test_key".to_string();
        let nested_scoped_key = "scope1:scope2:test_key".to_string();

        // Acquire the lock with nested scoped manager
        assert!(scoped_manager.acquire(key.clone()).await.is_ok());

        // Ensure the lock is held with scoped key
        assert!(scoped_manager.map.lock().await.contains(&nested_scoped_key));

        // Release the lock with scoped manager
        assert!(scoped_manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released with scoped key
        assert!(!scoped_manager.map.lock().await.contains(&nested_scoped_key));
    }
}
