//! Implementation of distributed locks using NATS Jetstream with HA replication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod guard;

pub use error::Error;
pub use guard::NatsLockGuard;

use async_nats::jetstream::kv::{Config as KvConfig, CreateErrorKind, Store as KvStore};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use proven_locks::{LockManager, LockManager1, LockManager2, LockManager3, LockStatus};
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

// renew at 80% of bucket_max_age to avoid gap between renewal and expiration
const RENEWAL_INTERVAL_RATIO_OF_BUCKET_MAX_AGE: f64 = 0.8;

/// Configuration for the NatsLockManager.
#[derive(Clone, Debug)]
pub struct NatsLockManagerConfig {
    /// The bucket to use for the key-value store (may be refined through scopes)
    pub bucket: String,

    /// The NATS client to use.
    pub client: Client,

    /// A unique identifier for this lock manager instance/client.
    pub local_identifier: String,

    /// Number of replicas for the KV store. Should be set to at least 3 in production for HA.
    pub num_replicas: usize,

    /// Whether to persist the locks to disk.
    pub persist: bool,

    /// TTL for for locks. Prevents locks from being held indefinitely if failure to release lock via guard drop.
    pub ttl: Duration,
}

/// A distributed lock manager implemented using NATS JetStream KV Store.
#[derive(Clone)]
pub struct NatsLockManager {
    bucket: String,
    client: Client,
    local_identifier: String,
    local_identifier_bytes: Bytes,
    jetstream_context: JetStreamContext,
    ttl: Duration,
    num_replicas: usize,
    persist: bool,
}

impl NatsLockManager {
    /// Creates a new instance of `NatsLockManager`. This is a synchronous constructor.
    /// The actual KV bucket is created/accessed lazily on first operation.
    pub fn new(
        NatsLockManagerConfig {
            bucket,
            client,
            local_identifier,
            num_replicas,
            persist,
            ttl,
        }: NatsLockManagerConfig,
    ) -> Self {
        let jetstream_context = async_nats::jetstream::new(client.clone());
        let local_identifier_bytes = Bytes::from(local_identifier.clone());

        Self {
            bucket,
            client,
            jetstream_context,
            local_identifier,
            local_identifier_bytes,
            num_replicas,
            persist,
            ttl,
        }
    }

    /// Gets or creates the KV store for the current bucket name and configuration.
    async fn get_kv_store(&self) -> Result<KvStore, Error> {
        let kv_config = KvConfig {
            bucket: self.bucket.clone(),
            max_age: self.ttl,
            num_replicas: self.num_replicas,
            storage: if self.persist {
                async_nats::jetstream::stream::StorageType::File
            } else {
                async_nats::jetstream::stream::StorageType::Memory
            },
            ..Default::default()
        };

        self.jetstream_context
            .create_or_update_key_value(kv_config)
            .await
            .map_err(|e| Error::CreateKvError(e))
    }

    #[instrument(skip(self), fields(bucket = %self.bucket, key = %resource_id, local_id = %self.local_identifier))]
    async fn try_lock_internal(
        &self,
        resource_id: String, // resource_id is the key within the current bucket
    ) -> Result<Option<<Self as LockManager>::Guard>, <Self as LockManager>::Error> {
        let kv_store = self.get_kv_store().await?;

        match kv_store
            .create(&resource_id, self.local_identifier_bytes.clone())
            .await
        {
            Ok(initial_revision) => {
                info!(bucket = %self.bucket, key = %resource_id, value = %self.local_identifier, revision = initial_revision, "Lock acquired via create.");

                Ok(Some(NatsLockGuard::new(
                    kv_store.clone(),
                    resource_id.clone(),
                    self.local_identifier_bytes.clone(),
                    self.ttl,
                    initial_revision,
                )))
            }
            Err(create_err) => {
                if !matches!(create_err.kind(), CreateErrorKind::AlreadyExists) {
                    error!(bucket = %self.bucket, key = %resource_id, error = ?create_err, "try_lock_internal failed: NATS KV create error (not AlreadyExists).");
                    return Err(Error::CreateError(create_err));
                }

                debug!(bucket = %self.bucket, key = %resource_id, "create failed (AlreadyExists). Fetching entry to check value.");
                match kv_store.entry(&resource_id).await {
                    Ok(Some(kv_entry)) => {
                        if kv_entry.value.is_empty() {
                            debug!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Found empty tombstone. Attempting to update and acquire.");
                            match kv_store
                                .update(
                                    &resource_id,
                                    self.local_identifier_bytes.clone(),
                                    kv_entry.revision,
                                )
                                .await
                            {
                                Ok(new_revision) => {
                                    info!(bucket = %self.bucket, key = %resource_id, value = %self.local_identifier, old_rev = kv_entry.revision, new_rev = new_revision, "Lock acquired by updating empty tombstone.");

                                    Ok(Some(NatsLockGuard::new(
                                        kv_store.clone(),
                                        resource_id.clone(),
                                        self.local_identifier_bytes.clone(),
                                        self.ttl,
                                        new_revision,
                                    )))
                                }
                                Err(update_err) => {
                                    warn!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, error = ?update_err, "Failed to update empty tombstone. Lock likely acquired by another.");
                                    Ok(None)
                                }
                            }
                        } else if kv_entry.value == self.local_identifier_bytes {
                            warn!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Key exists with our value, but create failed. Inconsistent state. Not acquiring.");
                            Ok(None)
                        } else {
                            debug!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Lock already held by another (non-empty, not self).");
                            Ok(None)
                        }
                    }
                    Ok(None) => {
                        warn!(bucket = %self.bucket, key = %resource_id, "create failed (AlreadyExists), but subsequent entry() found no key. Race condition?");
                        Ok(None)
                    }
                    Err(entry_err) => {
                        error!(bucket = %self.bucket, key = %resource_id, error = ?entry_err, "Failed to get entry after create reported AlreadyExists.");
                        Err(Error::EntryError(entry_err))
                    }
                }
            }
        }
    }
}

#[async_trait]
impl LockManager for NatsLockManager {
    type Error = Error;
    type Guard = NatsLockGuard;

    #[instrument(skip(self), fields(bucket = %self.bucket, key = %resource_id, local_id = %self.local_identifier))]
    async fn try_lock(&self, resource_id: String) -> Result<Option<Self::Guard>, Self::Error> {
        self.try_lock_internal(resource_id).await
    }

    #[instrument(skip(self), fields(bucket = %self.bucket, key = %resource_id, local_id = %self.local_identifier))]
    async fn lock(&self, resource_id: String) -> Result<Self::Guard, Self::Error> {
        debug!(bucket = %self.bucket, key = %resource_id, "Attempting to acquire lock (will wait if necessary)");
        let poll_interval = Duration::from_millis(100);

        loop {
            match self.try_lock_internal(resource_id.clone()).await {
                Ok(Some(guard)) => {
                    info!(bucket = %self.bucket, key = %resource_id, "Lock acquired successfully after waiting.");
                    return Ok(guard);
                }
                Ok(None) => {
                    debug!(bucket = %self.bucket, key = %resource_id, "Lock currently held by another or temporarily unavailable. Waiting for {:?} before retrying.", poll_interval);
                    tokio::time::sleep(poll_interval).await;
                }
                Err(e) => {
                    error!(bucket = %self.bucket, key = %resource_id, error = ?e, "Error during lock attempt, aborting.");
                    return Err(e);
                }
            }
        }
    }

    #[instrument(skip(self), fields(bucket = %self.bucket, key = %resource_id))]
    async fn check(&self, resource_id: String) -> Result<LockStatus, Self::Error> {
        let kv_store = self.get_kv_store().await?;

        debug!(bucket = %self.bucket, key = %resource_id, "Checking lock status");

        match kv_store.get(&resource_id).await {
            // Use the obtained kv_store
            Ok(Some(retrieved_value_bytes)) => {
                if retrieved_value_bytes.is_empty() {
                    debug!(bucket = %self.bucket, key = %resource_id, "Lock is free (key found with empty value).");
                    Ok(LockStatus::Free)
                } else if retrieved_value_bytes == self.local_identifier_bytes {
                    debug!(bucket = %self.bucket, key = %resource_id, "Lock held by self");
                    Ok(LockStatus::HeldBySelf)
                } else {
                    let other_holder_str =
                        String::from_utf8_lossy(&retrieved_value_bytes).to_string();
                    debug!(bucket = %self.bucket, key = %resource_id, "Lock held by other ('{}')", other_holder_str);
                    Ok(LockStatus::HeldByOther(other_holder_str))
                }
            }
            Ok(None) => {
                debug!(bucket = %self.bucket, key = %resource_id, "Lock is free (key not found).");
                Ok(LockStatus::Free)
            }
            Err(get_err) => {
                // get_err is async_nats::jetstream::kv::GetError
                error!(bucket = %self.bucket, key = %resource_id, error = ?get_err, "Error checking lock status.");
                Err(Error::EntryError(get_err)) // Should map to Error::EntryError if that's from kv::GetError in error.rs
            }
        }
    }
}

macro_rules! impl_scoped_lock_manager_for_nats {
    (
        $trait_name:ident,
        $parent_trait:ty
    ) => {
        #[async_trait]
        impl $trait_name for NatsLockManager {
            type Error = Error; // Assuming Error is the one from this crate's error module
            type Scoped = Self;

            fn scope<S>(&self, scope: S) -> Self::Scoped
            where
                S: AsRef<str> + Send,
            {
                let mut bucket = self.bucket.clone();
                bucket.push_str(&format!("_{}", scope.as_ref()));

                Self::Scoped::new(NatsLockManagerConfig {
                    client: self.client.clone(),
                    bucket,
                    local_identifier: self.local_identifier.clone(),
                    num_replicas: self.num_replicas,
                    persist: self.persist,
                    ttl: self.ttl,
                })
            }
        }
    };
}

impl_scoped_lock_manager_for_nats!(LockManager1, LockManager);
impl_scoped_lock_manager_for_nats!(LockManager2, LockManager1);
impl_scoped_lock_manager_for_nats!(LockManager3, LockManager2);

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use async_nats::connect;
    use std::time::Duration;
    use tokio::time::timeout;
    use uuid::Uuid;

    async fn get_test_js_context(bucket_prefix: &str) -> (Client, String) {
        let nats_url =
            std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
        let client = connect(&nats_url)
            .await
            .expect("Failed to connect to NATS for tests");
        let bucket_name = format!(
            "test_locks_{}_{}",
            bucket_prefix,
            Uuid::new_v4().as_hyphenated().to_string()
        );
        (client, bucket_name)
    }

    fn generate_local_id(suffix: &str) -> String {
        format!("test_holder_{}_{}", suffix, Uuid::new_v4().as_hyphenated())
    }

    #[tokio::test]
    async fn test_lock_and_drop_release() {
        let (client, bucket_name) = get_test_js_context("lock_drop").await;
        let local_id = generate_local_id("lock_drop");
        let config = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager = NatsLockManager::new(config);

        let lock_key = "my_resource_lock_drop".to_string();

        {
            let _guard = manager
                .lock(lock_key.clone())
                .await
                .expect("Lock acquisition failed");
            info!("Lock acquired, guard in scope.");
            let status = manager.check(lock_key.clone()).await.expect("Check failed");
            assert_eq!(
                status,
                LockStatus::HeldBySelf,
                "Lock should be held by self"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        info!("Guard dropped. Waiting briefly for potential cleanup/TTL to ensure release is checked properly.");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let status_after_drop = manager
            .check(lock_key.clone())
            .await
            .expect("Check after drop failed");
        assert_eq!(
            status_after_drop,
            LockStatus::Free,
            "Lock should be free after drop. Status: {:?}",
            status_after_drop
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_try_lock_conflict() {
        let (client, bucket_name) = get_test_js_context("try_conflict").await;
        let local_id1 = generate_local_id("try_conflict1");
        let config1 = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id1.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager1 = NatsLockManager::new(config1);

        let local_id2 = generate_local_id("try_conflict2");
        let config2 = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id2.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager2 = NatsLockManager::new(config2);

        let lock_key = "my_resource_try_conflict".to_string();

        let _guard1 = manager1
            .lock(lock_key.clone())
            .await
            .expect("Manager1 lock failed");
        info!("Manager1 acquired lock.");

        // Manager2 tries to try_lock the same lock
        let result2 = manager2.try_lock(lock_key.clone()).await;
        assert_matches!(
            result2,
            Ok(None),
            "Manager2 should fail to try_lock already held lock"
        );

        // Check status from manager2's perspective
        let status_by_manager2 = manager2
            .check(lock_key.clone())
            .await
            .expect("Manager2 check failed");
        assert_eq!(
            status_by_manager2,
            LockStatus::HeldByOther(local_id1.clone()),
            "Lock should be held by manager1"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_lock_waits_for_release() {
        let (client, bucket_name) = get_test_js_context("lock_waits").await;
        let local_id1 = generate_local_id("lock_waits1");
        let config1 = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id1.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager1 = NatsLockManager::new(config1);

        let local_id2 = generate_local_id("lock_waits2");
        let config2 = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id2.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager2 = NatsLockManager::new(config2);

        let lock_key = "my_resource_lock_waits".to_string();

        // Manager1 locks the resource
        let guard1 = manager1
            .lock(lock_key.clone())
            .await
            .expect("Manager1 lock failed");
        info!("Manager1 locked resource.");

        let manager2_lock_future = manager2.lock(lock_key.clone());

        // Variable to ensure manager2 indeed locks after guard1 is dropped.
        // It's set to true in the success path of the timeout block.
        let mut locked_by_m2 = false;

        tokio::select! {
            biased;
            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                // manager2.lock should still be waiting
                info!("Manager2 still waiting for lock as expected.");
                // locked_by_m2 remains false here
            }
            // Ensure this future is driven by the select!
            // The original future is consumed by the select
            result = manager2_lock_future => {
                 match result {
                    Ok(_guard2) => panic!("Manager2 acquired lock too early, before manager1 released it."),
                    Err(e) => panic!("Manager2 lock future failed unexpectedly while manager1 held lock: {:?}", e),
                 }
            }
        }

        info!("Dropping manager1's guard.");
        drop(guard1);

        // Now manager2 should be able to acquire the lock
        // Give it a bit more time than the polling interval in lock()
        // We re-initiate the lock attempt for manager2 here, as the previous future was consumed or timed out.
        match timeout(Duration::from_secs(3), manager2.lock(lock_key.clone())).await {
            Ok(Ok(_guard2)) => {
                info!("Manager2 acquired lock after manager1 released it.");
                locked_by_m2 = true; // Set the flag here
            }
            Ok(Err(e)) => panic!("Manager2 failed to acquire lock after release: {:?}", e),
            Err(_) => {
                // If it times out here, locked_by_m2 remains false, and the assert below will catch it.
                warn!("Manager2 timed out waiting for lock after release");
            }
        }
        // Assert the final state of locked_by_m2 outside the timeout block.
        assert!(
            locked_by_m2,
            "Manager2 should have acquired the lock after manager1 released it."
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_check_status() {
        let (client, bucket_name) = get_test_js_context("check").await;
        let local_id_self = generate_local_id("check_self");
        let config_self = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id_self.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager_self = NatsLockManager::new(config_self);

        let local_id_other = generate_local_id("check_other");
        let config_other = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id_other.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager_other = NatsLockManager::new(config_other);

        let lock_key = "my_resource_check".to_string();

        // 1. Check free lock
        let status_free = manager_self
            .check(lock_key.clone())
            .await
            .expect("Check free failed");
        assert_eq!(
            status_free,
            LockStatus::Free,
            "Lock should initially be free"
        );

        // 2. Self acquires, check self
        let _guard_self = manager_self
            .lock(lock_key.clone())
            .await
            .expect("Self lock failed");
        let status_held_by_self = manager_self
            .check(lock_key.clone())
            .await
            .expect("Check self failed");
        assert_eq!(
            status_held_by_self,
            LockStatus::HeldBySelf,
            "Lock should be HeldBySelf"
        );

        // 3. Other checks, should see HeldByOther
        let status_seen_by_other = manager_other
            .check(lock_key.clone())
            .await
            .expect("Other check failed");
        assert_eq!(
            status_seen_by_other,
            LockStatus::HeldByOther(local_id_self.clone()),
            "Lock should be HeldByOther(self)"
        );
        drop(_guard_self);
        tokio::time::sleep(Duration::from_millis(100)).await; // allow drop actions

        // 4. Other acquires, self checks
        let _guard_other = manager_other
            .lock(lock_key.clone())
            .await
            .expect("Other lock failed");
        let status_seen_by_self = manager_self
            .check(lock_key.clone())
            .await
            .expect("Self check (other held) failed");
        assert_eq!(
            status_seen_by_self,
            LockStatus::HeldByOther(local_id_other.clone()),
            "Lock should be HeldByOther(other)"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_scoped_lock_manager_new_api() {
        let (client, bucket_name) = get_test_js_context("scoped_new").await;
        let local_id = generate_local_id("scoped_new");
        let base_config = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: Duration::from_secs(3600),
            local_identifier: local_id.clone(),
            num_replicas: 1,
            persist: false,
        };
        let base_manager = NatsLockManager::new(base_config);

        let scope1_id = "REGION_A";
        let scoped_manager1 = LockManager1::scope(&base_manager, scope1_id.to_string());

        let lock_key_scoped = "my_scoped_resource".to_string();

        // Acquire with scoped manager
        let _guard_scoped = scoped_manager1
            .lock(lock_key_scoped.clone())
            .await
            .expect("Scoped lock failed");

        // Check with scoped manager (should be HeldBySelf)
        let status_scoped_self = scoped_manager1
            .check(lock_key_scoped.clone())
            .await
            .expect("Scoped self check failed");
        assert_eq!(
            status_scoped_self,
            LockStatus::HeldBySelf,
            "Scoped lock should be HeldBySelf from its manager"
        );

        // Check with base manager for the same resource ID (`lock_key_scoped`) in *its own* bucket.
        // It should be Free because scoped_manager1 locked it in a *different* bucket,
        // demonstrating isolation.
        let status_in_base_bucket = base_manager
            .check(lock_key_scoped.clone())
            .await
            .expect("Base manager checking for lock_key_scoped in its own bucket failed");
        assert_eq!(status_in_base_bucket, LockStatus::Free,
            "Base manager should find lock_key_scoped to be Free in its own bucket, as the scoped lock is in a different bucket.");

        // Base manager acquires its own lock (different key)
        let lock_key_base = "my_base_resource".to_string();
        let _guard_base = base_manager
            .lock(lock_key_base.clone())
            .await
            .expect("Base lock failed");
        let status_base_self = base_manager
            .check(lock_key_base.clone())
            .await
            .expect("Base self check failed");
        assert_eq!(
            status_base_self,
            LockStatus::HeldBySelf,
            "Base lock should be HeldBySelf"
        );

        // Scoped manager checks base manager's lock (it should be free for the scoped manager as keys are different)
        let status_scoped_sees_base = scoped_manager1
            .check(lock_key_base.clone())
            .await
            .expect("Scoped check of base key failed");
        assert_eq!(
            status_scoped_sees_base,
            LockStatus::Free,
            "Scoped manager should see base lock (non-prefixed from its perspective) as Free"
        );

        drop(_guard_scoped);
        drop(_guard_base);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(
            scoped_manager1
                .check(lock_key_scoped.clone())
                .await
                .unwrap(),
            LockStatus::Free
        );
        assert_eq!(
            base_manager.check(lock_key_base.clone()).await.unwrap(),
            LockStatus::Free
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_lock_renewal() {
        let local_id = generate_local_id("renewal");
        let bucket_max_age_for_test = Duration::from_secs(6);

        let expected_renewal_interval =
            bucket_max_age_for_test.mul_f64(RENEWAL_INTERVAL_RATIO_OF_BUCKET_MAX_AGE);
        let renewal_check_wait_time = expected_renewal_interval.mul_f64(1.5);

        let (client, bucket_name) = get_test_js_context("renewal").await;
        let config = NatsLockManagerConfig {
            client: client.clone(),
            bucket: bucket_name.clone(),
            ttl: bucket_max_age_for_test,
            local_identifier: local_id.clone(),
            num_replicas: 1,
            persist: false,
        };
        let manager = NatsLockManager::new(config);

        let lock_key = "my_resource_renewal".to_string();

        let guard = manager.lock(lock_key.clone()).await.expect("Lock failed");
        info!("Lock acquired for renewal test. Bucket max_age: {:?}, Expected renewal interval: {:?}, Wait time for check: {:?}", 
            bucket_max_age_for_test, expected_renewal_interval, renewal_check_wait_time);

        tokio::time::sleep(renewal_check_wait_time).await;

        let status_after_wait = manager
            .check(lock_key.clone())
            .await
            .expect("Check after wait failed");
        assert_eq!(
            status_after_wait,
            LockStatus::HeldBySelf,
            "Lock should still be held by self due to renewal. Status: {:?}",
            status_after_wait
        );

        drop(guard);
        info!("Renewal guard dropped.");

        // Give time for drop to propagate release
        tokio::time::sleep(Duration::from_millis(500)).await;

        let status_after_drop = manager
            .check(lock_key.clone())
            .await
            .expect("Check after drop failed");
        assert_eq!(
            status_after_drop,
            LockStatus::Free,
            "Lock should be free after dropping the guard. Status: {:?}",
            status_after_drop
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }
}
