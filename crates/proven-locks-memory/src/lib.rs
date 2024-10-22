mod error;

use error::Error;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use proven_locks::{LockManager, LockManager1, LockManager2};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct MemoryLockManager {
    map: Arc<Mutex<HashSet<String>>>,
    prefix: Option<String>,
}

/// MemoryLockManager is an in-memory implementation of the `LockManager`, `LockManager1`, and `LockManager2` traits.
/// It uses a `HashSet` protected by a `Mutex` to store keys as strings.
/// The store supports optional scoping of keys using a prefix.
impl MemoryLockManager {
    pub fn new() -> Self {
        MemoryLockManager {
            map: Arc::new(Mutex::new(HashSet::new())),
            prefix: None,
        }
    }

    fn with_scope(prefix: String) -> Self {
        MemoryLockManager {
            map: Arc::new(Mutex::new(HashSet::new())),
            prefix: Some(prefix),
        }
    }

    fn get_key(&self, key: String) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}:{}", prefix, key),
            None => key,
        }
    }
}

#[async_trait]
impl LockManager1 for MemoryLockManager {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };
        Self::with_scope(new_scope)
    }
}

#[async_trait]
impl LockManager2 for MemoryLockManager {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };
        Self::with_scope(new_scope)
    }
}

#[async_trait]
impl LockManager for MemoryLockManager {
    type SE = Error;

    async fn acquire(&self, key: String) -> Result<(), Self::SE> {
        let key = self.get_key(key);
        let mut lock = self.map.lock().await;
        lock.insert(key);

        Ok(())
    }

    async fn release(&self, key: String) -> Result<(), Self::SE> {
        let key = self.get_key(key);
        let mut lock = self.map.lock().await;
        lock.remove(&key);

        Ok(())
    }
}

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
        {
            let lock = manager.map.lock().await;
            assert!(lock.contains(&key));
        }

        // Release the lock
        assert!(manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released
        {
            let lock = manager.map.lock().await;
            assert!(!lock.contains(&key));
        }
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
        {
            let lock = scoped_manager.map.lock().await;
            assert!(lock.contains(&scoped_key));
        }

        // Release the lock with scoped manager
        assert!(scoped_manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released with scoped key
        {
            let lock = scoped_manager.map.lock().await;
            assert!(!lock.contains(&scoped_key));
        }
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
        {
            let lock = scoped_manager.map.lock().await;
            assert!(lock.contains(&nested_scoped_key));
        }

        // Release the lock with scoped manager
        assert!(scoped_manager.release(key.clone()).await.is_ok());

        // Ensure the lock is released with scoped key
        {
            let lock = scoped_manager.map.lock().await;
            assert!(!lock.contains(&nested_scoped_key));
        }
    }
}
