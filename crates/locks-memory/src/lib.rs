//! In-memory (single node) implementation of locks for local development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
pub use error::Error;

use async_trait::async_trait;
use proven_locks::{LockManager, LockManager1, LockManager2, LockManager3, LockStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, instrument};

/// Represents the details of a lock held in-memory.
#[derive(Clone, Debug)]
struct Lock;

/// In-memory (single node) implementation of locks for local development.
#[derive(Clone, Debug)]
pub struct MemoryLockManager {
    /// Store current locks.
    locks: Arc<Mutex<HashMap<String, Lock>>>,

    /// Optional prefix for keys, to simulate scoping.
    prefix: Option<String>,
}

impl MemoryLockManager {
    /// Creates a new instance of `MemoryLockManager`.
    pub fn new() -> Self {
        Self {
            locks: Arc::new(Mutex::new(HashMap::new())),
            prefix: None,
        }
    }

    fn get_full_key(&self, resource_id: &str) -> String {
        match &self.prefix {
            Some(p) => format!("{}:{}", p, resource_id),
            None => resource_id.to_string(),
        }
    }
}

/// Guard for an in-memory lock. Releases the lock on Drop.
#[must_use = "lock will be released when dropped"]
#[derive(Debug)]
pub struct MemoryLockGuard {
    /// The key of the lock being held.
    key: String,

    /// The map of locks being held.
    locks_map: Arc<Mutex<HashMap<String, Lock>>>,
}

impl Drop for MemoryLockGuard {
    #[instrument(skip(self), fields(key = %self.key))]
    fn drop(&mut self) {
        debug!("Dropping MemoryLockGuard, attempting release.");
        if let Ok(mut locked_map) = self.locks_map.try_lock() {
            // Simple removal, as there's no ref counting
            if locked_map.remove(&self.key).is_some() {
                debug!("Lock for key '{}' released.", self.key);
            } else {
                debug!(
                    "Lock for key '{}' not found in map during guard drop. Already released or never properly acquired.",
                    self.key
                );
            }
        } else {
            tracing::warn!(key = %self.key, "Failed to acquire map's mutex (try_lock) in MemoryLockGuard::drop. Lock may not be immediately released by this guard's action.");
        }
    }
}

#[async_trait]
impl LockManager for MemoryLockManager {
    type Error = Error;
    type Guard = MemoryLockGuard;

    #[instrument(skip(self), fields(key = %resource_id))]
    async fn check(&self, resource_id: String) -> Result<LockStatus, Self::Error> {
        let key = self.get_full_key(&resource_id);
        let locked_map = self.locks.lock().await;

        // If key exists, it's held by self (no expiration logic)
        if locked_map.contains_key(&key) {
            debug!("Lock for key '{}' is HeldBySelf.", key);
            Ok(LockStatus::HeldBySelf)
        } else {
            debug!("No lock found for key '{}', status is Free.", key);
            Ok(LockStatus::Free)
        }
    }

    #[instrument(skip(self), fields(key = %resource_id))]
    async fn lock(&self, resource_id: String) -> Result<Self::Guard, Self::Error> {
        let key = self.get_full_key(&resource_id);
        let poll_interval = tokio::time::Duration::from_millis(50); // How often to retry

        loop {
            let mut locked_map = self.locks.lock().await;
            if !locked_map.contains_key(&key) {
                // Lock is free, acquire it
                debug!("Acquiring new lock for key '{}' (lock operation).", key);
                locked_map.insert(key.clone(), Lock {});
                return Ok(MemoryLockGuard {
                    locks_map: Arc::clone(&self.locks),
                    key,
                });
            }
            // Drop MutexGuard to release lock on map, before sleeping
            drop(locked_map);
            debug!("Lock for key '{}' is currently held. Waiting...", key);
            tokio::time::sleep(poll_interval).await;
        }
    }

    #[instrument(skip(self), fields(key = %resource_id))]
    async fn try_lock(&self, resource_id: String) -> Result<Option<Self::Guard>, Self::Error> {
        let key = self.get_full_key(&resource_id);
        let mut locked_map = self.locks.lock().await;

        if locked_map.contains_key(&key) {
            // Lock is held
            debug!("try_lock failed for key '{}': Held.", key);
            Ok(None)
        } else {
            // Lock is free, acquire it.
            debug!("try_lock: acquiring new lock for key '{}'.", key);
            locked_map.insert(key.clone(), Lock {});
            Ok(Some(MemoryLockGuard {
                locks_map: Arc::clone(&self.locks),
                key,
            }))
        }
    }
}

// Scoped LockManager implementations
macro_rules! impl_scoped_memory_lock_manager {
    (
        $trait_name:ident,
        $parent_trait:ty
    ) => {
        #[async_trait]
        impl $trait_name for MemoryLockManager {
            type Error = Error;
            type Scoped = Self;

            fn scope<S>(&self, scope: S) -> Self::Scoped
            where
                S: AsRef<str> + Send,
            {
                let new_prefix = match &self.prefix {
                    Some(existing_prefix) => format!("{}:{}", existing_prefix, scope.as_ref()),
                    None => scope.as_ref().to_string(),
                };
                Self {
                    locks: Arc::clone(&self.locks),
                    prefix: Some(new_prefix),
                }
            }
        }
    };
}

impl_scoped_memory_lock_manager!(LockManager1, LockManager);
impl_scoped_memory_lock_manager!(LockManager2, LockManager1);
impl_scoped_memory_lock_manager!(LockManager3, LockManager2);

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::time::Duration;

    use assert_matches::assert_matches;

    #[tokio::test]
    async fn test_lock_and_drop_release() {
        let manager = MemoryLockManager::new();
        let lock_key = "resource1".to_string();

        {
            let _guard = manager
                .lock(lock_key.clone())
                .await
                .expect("Failed to lock");
            let status = manager.check(lock_key.clone()).await.expect("Check failed");
            assert_eq!(status, LockStatus::HeldBySelf);
        }

        let status_after_drop = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(status_after_drop, LockStatus::Free);
    }

    #[tokio::test]
    async fn test_try_lock_conflict() {
        let manager = MemoryLockManager::new();
        let lock_key = "resource_conflict".to_string();

        let _guard1 = manager
            .lock(lock_key.clone())
            .await
            .expect("Manager1 lock failed");

        let result2 = manager.try_lock(lock_key.clone()).await;
        assert_matches!(result2, Ok(None));

        let status = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(status, LockStatus::HeldBySelf);
    }

    #[tokio::test]
    async fn test_lock_waits_for_self_release() {
        let manager = MemoryLockManager::new();
        let lock_key = "reentrant_resource".to_string();

        // Acquire the first guard
        let guard1 = manager
            .lock(lock_key.clone())
            .await
            .expect("First lock failed");

        let manager_clone = manager.clone();
        let lock_key_clone = lock_key.clone();

        // Spawn a task that attempts to lock the same key. It should wait.
        let lock_task = tokio::spawn(async move {
            debug!("Task attempting to lock '{}'", lock_key_clone);
            let _guard2 = manager_clone
                .lock(lock_key_clone.clone())
                .await
                .expect("Second lock failed in task");
            debug!("Task acquired lock for '{}'", lock_key_clone);
            // Guard2 will be dropped when task finishes
        });

        // Give the task a moment to attempt the lock and block
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check status, should still be held by guard1
        let status_while_task_waits = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(
            status_while_task_waits,
            LockStatus::HeldBySelf,
            "Lock should be held by guard1 while task waits"
        );

        debug!("Dropping guard1 for '{}'", lock_key);
        drop(guard1); // Release the first lock

        // The task should now be able to acquire the lock
        // Wait for the task to complete
        match tokio::time::timeout(Duration::from_secs(1), lock_task).await {
            Ok(Ok(())) => {
                // Task completed successfully
            }
            Ok(Err(e)) => panic!("Lock task panicked: {:?}", e),
            Err(_) => panic!("Lock task timed out after guard1 was dropped"),
        }

        // After task completes, guard2 (from the task) is dropped, so lock should be free
        let status_after_task = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(
            status_after_task,
            LockStatus::Free,
            "Lock should be free after task completes and its guard is dropped"
        );
    }

    #[tokio::test]
    async fn test_check_status() {
        let manager = MemoryLockManager::new();
        let lock_key_free = "resource_free".to_string();
        let lock_key_held = "resource_held".to_string();

        let status_free = manager
            .check(lock_key_free.clone())
            .await
            .expect("Check free failed");
        assert_eq!(status_free, LockStatus::Free);

        let _guard_held = manager
            .lock(lock_key_held.clone())
            .await
            .expect("Lock failed");
        let status_held = manager
            .check(lock_key_held.clone())
            .await
            .expect("Check held failed");
        assert_eq!(status_held, LockStatus::HeldBySelf);
    }

    #[tokio::test]
    async fn test_scoped_memory_lock_manager() {
        let base_manager = MemoryLockManager::new();
        let scope1_id = "SCOPE1";
        let scope2_id = "SCOPE2";

        let manager_s1 = LockManager1::scope(&base_manager, scope1_id.to_string());
        let manager_s1_s2 = LockManager2::scope(&manager_s1, scope2_id.to_string());

        let res_id = "my_resource".to_string();

        let g_s1_s2 = manager_s1_s2.lock(res_id.clone()).await.unwrap();
        assert_eq!(
            manager_s1_s2.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );

        assert_eq!(
            base_manager.check(res_id.clone()).await.unwrap(),
            LockStatus::Free
        );
        let _g_base = base_manager.lock(res_id.clone()).await.unwrap();
        assert_eq!(
            base_manager.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );

        assert_eq!(
            manager_s1.check(res_id.clone()).await.unwrap(),
            LockStatus::Free
        );
        let _g_s1 = manager_s1.lock(res_id.clone()).await.unwrap();
        assert_eq!(
            manager_s1.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );

        assert_eq!(
            manager_s1_s2.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );

        drop(g_s1_s2);
        assert_eq!(
            manager_s1_s2.check(res_id.clone()).await.unwrap(),
            LockStatus::Free
        );

        assert_eq!(
            base_manager.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );
        assert_eq!(
            manager_s1.check(res_id.clone()).await.unwrap(),
            LockStatus::HeldBySelf
        );
    }

    #[tokio::test]
    async fn test_drop_behavior_contended_mutex_with_lock() {
        let manager = MemoryLockManager::new();
        let lock_key = "contended_drop_resource".to_string();

        let guard = manager
            .lock(lock_key.clone())
            .await
            .expect("Failed to lock");

        let locks_map_clone = Arc::clone(&manager.locks);
        let _map_guard = locks_map_clone.lock().await;

        drop(guard);

        drop(_map_guard);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(
            status,
            LockStatus::HeldBySelf,
            "Lock should still be held as drop failed to acquire mutex"
        );
    }

    #[tokio::test]
    async fn test_try_lock_on_free_lock() {
        let manager = MemoryLockManager::new();
        let lock_key = "try_free_resource".to_string();

        let result = manager.try_lock(lock_key.clone()).await;
        assert_matches!(
            result,
            Ok(Some(_)),
            "Should successfully try_lock a free lock"
        );

        let status = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(status, LockStatus::HeldBySelf);
    }
}
