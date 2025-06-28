//! Implementation of distributed locks using NATS Jetstream with HA replication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;
mod guard;

pub use error::Error;
pub use guard::NatsLockGuard;

use async_nats::Client;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::jetstream::kv::{Config as KvConfig, Store as KvStore};
use async_trait::async_trait;
use bytes::Bytes;
use proven_locks::{LockManager, LockManager1, LockManager2, LockManager3, LockStatus};
use std::fmt::Write;
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

// renew at 80% of bucket_max_age to avoid gap between renewal and expiration
const RENEWAL_INTERVAL_RATIO_OF_BUCKET_MAX_AGE: f64 = 0.8;

/// Configuration for the `NatsLockManager`.
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

    /// Timeout for individual NATS `JetStream` operations. Defaults to 10 seconds if not set.
    pub operation_timeout: Option<Duration>,

    /// Maximum number of retry attempts for failed operations. Defaults to 3 if not set.
    pub max_retries: Option<usize>,

    /// Base delay for exponential backoff between retries. Defaults to 100ms if not set.
    pub retry_base_delay: Option<Duration>,

    /// Maximum delay for exponential backoff. Defaults to 5 seconds if not set.
    pub retry_max_delay: Option<Duration>,
}

/// A distributed lock manager implemented using NATS jetstream KV Store.
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
    operation_timeout: Duration,
    max_retries: usize,
    retry_base_delay: Duration,
    retry_max_delay: Duration,
}

impl NatsLockManager {
    /// Creates a new instance of `NatsLockManager`. This is a synchronous constructor.
    /// The actual KV bucket is created/accessed lazily on first operation.
    #[must_use]
    pub fn new(
        NatsLockManagerConfig {
            bucket,
            client,
            local_identifier,
            num_replicas,
            persist,
            ttl,
            operation_timeout,
            max_retries,
            retry_base_delay,
            retry_max_delay,
        }: NatsLockManagerConfig,
    ) -> Self {
        let jetstream_context = async_nats::jetstream::new(client.clone());
        let local_identifier_bytes = Bytes::from(local_identifier.clone());

        Self {
            bucket,
            client,
            local_identifier,
            local_identifier_bytes,
            jetstream_context,
            ttl,
            num_replicas,
            persist,
            operation_timeout: operation_timeout.unwrap_or(Duration::from_secs(10)),
            max_retries: max_retries.unwrap_or(3),
            retry_base_delay: retry_base_delay.unwrap_or(Duration::from_millis(100)),
            retry_max_delay: retry_max_delay.unwrap_or(Duration::from_secs(5)),
        }
    }

    /// Executes an operation with retry logic and exponential backoff.
    async fn with_retry<F, Fut, R, E>(&self, operation_name: &str, operation: F) -> Result<R, Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<R, E>>,
        E: std::fmt::Display + std::fmt::Debug,
    {
        let mut attempts = 0;
        let mut delay = self.retry_base_delay;

        loop {
            attempts += 1;

            match tokio::time::timeout(self.operation_timeout, operation()).await {
                Ok(Ok(result)) => {
                    if attempts > 1 {
                        debug!(
                            operation = operation_name,
                            attempts = attempts,
                            "Operation succeeded after retry"
                        );
                    }
                    return Ok(result);
                }
                Ok(Err(e)) => {
                    let error_str = e.to_string();
                    let is_retriable = error_str.contains("timeout")
                        || error_str.contains("connection")
                        || error_str.contains("unavailable")
                        || error_str.contains("temporary")
                        || error_str.contains("stream not found");

                    if !is_retriable || attempts >= self.max_retries {
                        if attempts >= self.max_retries {
                            error!(
                                operation = operation_name,
                                attempts = attempts,
                                error = ?e,
                                "Operation failed after maximum retries"
                            );
                            return Err(Error::MaxRetriesExceeded {
                                max_attempts: self.max_retries,
                                last_error: error_str,
                            });
                        }
                        debug!(
                            operation = operation_name,
                            error = ?e,
                            "Operation failed with non-retriable error"
                        );
                        return Err(Error::MaxRetriesExceeded {
                            max_attempts: 1,
                            last_error: error_str,
                        });
                    }

                    warn!(
                        operation = operation_name,
                        attempt = attempts,
                        max_attempts = self.max_retries,
                        delay = ?delay,
                        error = ?e,
                        "Operation failed, retrying"
                    );
                }
                Err(_timeout) => {
                    if attempts >= self.max_retries {
                        error!(
                            operation = operation_name,
                            attempts = attempts,
                            timeout = ?self.operation_timeout,
                            "Operation timed out after maximum retries"
                        );
                        return Err(Error::Timeout {
                            attempts,
                            last_error: format!(
                                "Operation timed out after {:?}",
                                self.operation_timeout
                            ),
                        });
                    }

                    warn!(
                        operation = operation_name,
                        attempt = attempts,
                        timeout = ?self.operation_timeout,
                        delay = ?delay,
                        "Operation timed out, retrying"
                    );
                }
            }

            // Sleep before retry
            tokio::time::sleep(delay).await;

            // Exponential backoff with jitter
            delay = std::cmp::min(
                delay.mul_f64(fastrand::f64().mul_add(0.1, 2.0)), // Add 0-10% jitter
                self.retry_max_delay,
            );
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

        self.with_retry("create_or_update_kv", || {
            let jetstream_context = self.jetstream_context.clone();
            let config = kv_config.clone();
            async move { jetstream_context.create_or_update_key_value(config).await }
        })
        .await
        .map_err(|e| match e {
            Error::CreateKvError(inner) => Error::CreateKvError(inner),
            other => other,
        })
    }

    #[instrument(skip(self), fields(bucket = %self.bucket, key = %resource_id, local_id = %self.local_identifier))]
    async fn try_lock_internal(
        &self,
        resource_id: String, // resource_id is the key within the current bucket
    ) -> Result<Option<<Self as LockManager>::Guard>, <Self as LockManager>::Error> {
        let kv_store = self.get_kv_store().await?;

        let create_result = self
            .with_retry("kv_create", || {
                let kv_store = kv_store.clone();
                let resource_id = resource_id.clone();
                let value = self.local_identifier_bytes.clone();
                async move { kv_store.create(&resource_id, value).await }
            })
            .await;

        match create_result {
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
            Err(
                Error::CreateKvError(_) | Error::Timeout { .. } | Error::MaxRetriesExceeded { .. },
            ) => {
                // For KV create operations specifically, we need to handle the AlreadyExists case
                let entry_result = self
                    .with_retry("kv_entry", || {
                        let kv_store = kv_store.clone();
                        let resource_id = resource_id.clone();
                        async move { kv_store.entry(&resource_id).await }
                    })
                    .await
                    .map_err(|e| Error::Timeout {
                        attempts: 1,
                        last_error: format!("Entry operation failed: {e}"),
                    })?;

                if let Some(kv_entry) = entry_result {
                    if kv_entry.value.is_empty() {
                        debug!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Found empty tombstone. Attempting to update and acquire.");

                        let update_result = self
                            .with_retry("kv_update", || {
                                let kv_store = kv_store.clone();
                                let resource_id = resource_id.clone();
                                let value = self.local_identifier_bytes.clone();
                                let revision = kv_entry.revision;
                                async move { kv_store.update(&resource_id, value, revision).await }
                            })
                            .await;

                        if let Ok(new_revision) = update_result {
                            info!(bucket = %self.bucket, key = %resource_id, value = %self.local_identifier, old_rev = kv_entry.revision, new_rev = new_revision, "Lock acquired by updating empty tombstone.");

                            Ok(Some(NatsLockGuard::new(
                                kv_store.clone(),
                                resource_id.clone(),
                                self.local_identifier_bytes.clone(),
                                self.ttl,
                                new_revision,
                            )))
                        } else {
                            warn!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Failed to update empty tombstone. Lock likely acquired by another.");
                            Ok(None)
                        }
                    } else if kv_entry.value == self.local_identifier_bytes {
                        warn!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Key exists with our value, but create failed. Inconsistent state. Not acquiring.");
                        Ok(None)
                    } else {
                        debug!(bucket = %self.bucket, key = %resource_id, revision = kv_entry.revision, "Lock already held by another (non-empty, not self).");
                        Ok(None)
                    }
                } else {
                    warn!(bucket = %self.bucket, key = %resource_id, "create failed, but subsequent entry() found no key. Race condition?");
                    Ok(None)
                }
            }
            Err(other) => Err(other),
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
        let mut poll_interval = Duration::from_millis(100);
        let max_poll_interval = Duration::from_secs(1);

        loop {
            match self.try_lock_internal(resource_id.clone()).await {
                Ok(Some(guard)) => {
                    info!(bucket = %self.bucket, key = %resource_id, "Lock acquired successfully after waiting.");
                    return Ok(guard);
                }
                Ok(None) => {
                    debug!(bucket = %self.bucket, key = %resource_id, "Lock currently held by another or temporarily unavailable. Waiting for {:?} before retrying.", poll_interval);
                    tokio::time::sleep(poll_interval).await;

                    // Exponential backoff for polling with jitter
                    poll_interval = std::cmp::min(
                        poll_interval.mul_f64(fastrand::f64().mul_add(0.1, 1.5)),
                        max_poll_interval,
                    );
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

        let get_result = self
            .with_retry("kv_get", || {
                let kv_store = kv_store.clone();
                let resource_id = resource_id.clone();
                async move { kv_store.get(&resource_id).await }
            })
            .await
            .map_err(|e| Error::Timeout {
                attempts: 1,
                last_error: format!("Get operation failed: {e}"),
            })?;

        get_result.map_or_else(
            || {
                debug!(bucket = %self.bucket, key = %resource_id, "Lock is free (key not found).");
                Ok(LockStatus::Free)
            },
            |retrieved_value_bytes| {
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
            },
        )
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
                write!(bucket, "_{}", scope.as_ref()).unwrap();

                Self::Scoped::new(NatsLockManagerConfig {
                    client: self.client.clone(),
                    bucket,
                    local_identifier: self.local_identifier.clone(),
                    num_replicas: self.num_replicas,
                    persist: self.persist,
                    ttl: self.ttl,
                    operation_timeout: Some(self.operation_timeout),
                    max_retries: Some(self.max_retries),
                    retry_base_delay: Some(self.retry_base_delay),
                    retry_max_delay: Some(self.retry_max_delay),
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
            Uuid::new_v4().as_hyphenated()
        );
        (client, bucket_name)
    }

    fn generate_local_id(suffix: &str) -> String {
        format!("test_holder_{}_{}", suffix, Uuid::new_v4().as_hyphenated())
    }

    fn create_test_config(
        client: Client,
        bucket: String,
        local_identifier: String,
    ) -> NatsLockManagerConfig {
        NatsLockManagerConfig {
            client,
            bucket,
            ttl: Duration::from_secs(3600),
            local_identifier,
            num_replicas: 1,
            persist: false,
            operation_timeout: None,
            max_retries: None,
            retry_base_delay: None,
            retry_max_delay: None,
        }
    }

    #[tokio::test]
    async fn test_lock_and_drop_release() {
        let (client, bucket_name) = get_test_js_context("lock_drop").await;
        let local_id = generate_local_id("lock_drop");
        let config = create_test_config(client.clone(), bucket_name.clone(), local_id.clone());
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

        info!(
            "Guard dropped. Waiting briefly for potential cleanup/TTL to ensure release is checked properly."
        );
        tokio::time::sleep(Duration::from_millis(500)).await;

        let status_after_drop = manager
            .check(lock_key.clone())
            .await
            .expect("Check after drop failed");
        assert_eq!(
            status_after_drop,
            LockStatus::Free,
            "Lock should be free after drop. Status: {status_after_drop:?}"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_try_lock_conflict() {
        let (client, bucket_name) = get_test_js_context("try_conflict").await;
        let local_id1 = generate_local_id("try_conflict1");
        let config1 = create_test_config(client.clone(), bucket_name.clone(), local_id1.clone());
        let manager1 = NatsLockManager::new(config1);

        let local_id2 = generate_local_id("try_conflict2");
        let config2 = create_test_config(client.clone(), bucket_name.clone(), local_id2.clone());
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
        let config1 = create_test_config(client.clone(), bucket_name.clone(), local_id1.clone());
        let manager1 = NatsLockManager::new(config1);

        let local_id2 = generate_local_id("lock_waits2");
        let config2 = create_test_config(client.clone(), bucket_name.clone(), local_id2.clone());
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
            () = tokio::time::sleep(Duration::from_millis(500)) => {
                // manager2.lock should still be waiting
                info!("Manager2 still waiting for lock as expected.");
                // locked_by_m2 remains false here
            }
            // Ensure this future is driven by the select!
            // The original future is consumed by the select
            result = manager2_lock_future => {
                 match result {
                    Ok(_guard2) => panic!("Manager2 acquired lock too early, before manager1 released it."),
                    Err(e) => panic!("Manager2 lock future failed unexpectedly while manager1 held lock: {e:?}"),
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
            Ok(Err(e)) => panic!("Manager2 failed to acquire lock after release: {e:?}"),
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
        let (client, bucket_name) = get_test_js_context("check_status").await;
        let local_id1 = generate_local_id("check_status1");
        let config1 = create_test_config(client.clone(), bucket_name.clone(), local_id1.clone());
        let manager1 = NatsLockManager::new(config1);

        let local_id2 = generate_local_id("check_status2");
        let config2 = create_test_config(client.clone(), bucket_name.clone(), local_id2.clone());
        let manager2 = NatsLockManager::new(config2);

        let lock_key = "my_resource_check_status".to_string();

        // Initially, the lock should be free
        let status = manager1
            .check(lock_key.clone())
            .await
            .expect("Check failed");
        assert_eq!(status, LockStatus::Free, "Lock should initially be free");

        // Manager1 acquires the lock
        let _guard1 = manager1
            .lock(lock_key.clone())
            .await
            .expect("Manager1 lock failed");

        // Now manager1 should see it as held by self
        let status_by_manager1 = manager1
            .check(lock_key.clone())
            .await
            .expect("Manager1 check failed");
        assert_eq!(
            status_by_manager1,
            LockStatus::HeldBySelf,
            "Manager1 should see lock as held by self"
        );

        // Manager2 should see it as held by manager1
        let status_by_manager2 = manager2
            .check(lock_key.clone())
            .await
            .expect("Manager2 check failed");
        assert_eq!(
            status_by_manager2,
            LockStatus::HeldByOther(local_id1.clone()),
            "Manager2 should see lock as held by manager1"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_scoped_lock_manager_new_api() {
        let (client, bucket_name) = get_test_js_context("scoped").await;
        let local_id = generate_local_id("scoped");
        let config = create_test_config(client.clone(), bucket_name.clone(), local_id.clone());
        let manager = NatsLockManager::new(config);

        let scoped_manager = LockManager1::scope(&manager, "test_scope");

        let lock_key = "my_resource_scoped".to_string();

        let _guard = scoped_manager
            .lock(lock_key.clone())
            .await
            .expect("Scoped lock failed");
        info!("Scoped lock acquired.");

        // Original manager should not see the lock
        let status_original = manager
            .check(lock_key.clone())
            .await
            .expect("Original check failed");
        assert_eq!(
            status_original,
            LockStatus::Free,
            "Original manager should not see scoped lock"
        );

        // Scoped manager should see it
        let status_scoped = scoped_manager
            .check(lock_key.clone())
            .await
            .expect("Scoped check failed");
        assert_eq!(
            status_scoped,
            LockStatus::HeldBySelf,
            "Scoped manager should see its own lock"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
        js_context
            .delete_key_value(&format!("{bucket_name}_test_scope"))
            .await
            .ok();
    }

    #[tokio::test]
    async fn test_lock_renewal() {
        let (client, bucket_name) = get_test_js_context("renewal").await;
        let local_id = generate_local_id("renewal");
        let mut config = create_test_config(client.clone(), bucket_name.clone(), local_id.clone());
        config.ttl = Duration::from_secs(3); // Short TTL for testing
        let manager = NatsLockManager::new(config);

        let lock_key = "my_resource_renewal".to_string();

        let guard = manager
            .lock(lock_key.clone())
            .await
            .expect("Lock acquisition failed");
        info!("Lock acquired with short TTL.");

        // Wait longer than TTL but less than renewal would allow
        tokio::time::sleep(Duration::from_secs(4)).await;

        // Lock should still be held due to renewal
        let status = manager.check(lock_key.clone()).await.expect("Check failed");
        assert_eq!(
            status,
            LockStatus::HeldBySelf,
            "Lock should still be held due to renewal"
        );

        drop(guard);
        info!("Guard dropped.");

        // Give a bit of time for cleanup
        tokio::time::sleep(Duration::from_millis(500)).await;

        let status_after_drop = manager
            .check(lock_key.clone())
            .await
            .expect("Check after drop failed");
        assert_eq!(
            status_after_drop,
            LockStatus::Free,
            "Lock should be free after drop"
        );

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }

    #[tokio::test]
    async fn test_timeout_and_retry_robustness() {
        let (client, bucket_name) = get_test_js_context("timeout_retry").await;
        let local_id = generate_local_id("timeout_retry");
        let mut config = create_test_config(client.clone(), bucket_name.clone(), local_id.clone());

        // Configure more aggressive retry settings for testing
        config.operation_timeout = Some(Duration::from_millis(500));
        config.max_retries = Some(2);
        config.retry_base_delay = Some(Duration::from_millis(50));
        config.retry_max_delay = Some(Duration::from_millis(200));

        let manager = NatsLockManager::new(config);

        let lock_key = "my_resource_timeout_retry".to_string();

        // This should work even with short timeouts due to retry logic
        let _guard = manager
            .lock(lock_key.clone())
            .await
            .expect("Lock acquisition should succeed with retries");

        info!("Lock acquired successfully despite timeout configuration.");

        let js_context = async_nats::jetstream::new(client.clone());
        js_context.delete_key_value(&bucket_name).await.ok();
    }
}
