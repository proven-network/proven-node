use crate::RENEWAL_INTERVAL_RATIO_OF_BUCKET_MAX_AGE;

use async_nats::jetstream::kv::Store as KvStore;
use async_nats::jetstream::kv::UpdateErrorKind;
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

/// Guard for a NATS-backed lock. Handles TTL renewal and releases the lock on Drop.
#[derive(Debug)]
#[must_use = "lock will be released when dropped"]
pub struct NatsLockGuard {
    key: String,
    kv_store: KvStore,
    lock_value: Bytes,
    revision: Arc<Mutex<u64>>, // Revision of the KV entry, shared for updates
    renewal_task: JoinHandle<()>,
    stop_renewal_flag: Arc<Mutex<bool>>,
}

impl NatsLockGuard {
    /// Create a new `NatsLockGuard`.
    ///
    /// # Arguments
    ///
    /// * `kv_store` - The KV store to use.
    /// * `key` - The key to use for the lock.
    /// * `lock_value` - The value to use for the lock.
    /// * `ttl` - The TTL for the lock.
    /// * `initial_revision` - The initial revision of the KV entry.
    pub fn new(
        kv_store: KvStore,
        key: String,
        lock_value: Bytes,
        ttl: Duration,
        initial_revision: u64,
    ) -> Self {
        let stop_renewal_flag = Arc::new(Mutex::new(false));
        let revision_arc = Arc::new(Mutex::new(initial_revision));

        let renewal_task = Self::start_renewal_task(
            kv_store.clone(),
            key.clone(),
            lock_value.clone(),
            ttl,
            stop_renewal_flag.clone(),
            revision_arc.clone(),
        );

        Self {
            kv_store,
            key,
            lock_value,
            revision: revision_arc,
            renewal_task,
            stop_renewal_flag,
        }
    }

    #[instrument(skip(kv_store, stop_renewal_flag, revision_arc), fields(key = %key, value = %String::from_utf8_lossy(lock_value.as_ref()), ttl = ?ttl))]
    fn start_renewal_task(
        kv_store: KvStore,
        key: String,
        lock_value: Bytes,
        ttl: Duration,
        stop_renewal_flag: Arc<Mutex<bool>>,
        revision_arc: Arc<Mutex<u64>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            if ttl.is_zero() || ttl.as_secs_f64() < 0.1 {
                warn!(key = %key, ?ttl, "Bucket max_age is too small or zero, renewal task will not run effectively.");
                return;
            }
            let renewal_interval = ttl.mul_f64(RENEWAL_INTERVAL_RATIO_OF_BUCKET_MAX_AGE);
            if renewal_interval.is_zero() {
                warn!(key = %key, ?ttl, ?renewal_interval, "Calculated renewal interval is zero, renewal task will not run effectively.");
                return;
            }
            debug!(key = %key, ?renewal_interval, ?ttl, "Starting lock renewal task.");

            loop {
                tokio::time::sleep(renewal_interval).await;

                if *stop_renewal_flag.lock().unwrap() {
                    debug!(key = %key, "Stop signal received, terminating renewal task.");
                    break;
                }

                debug!(key = %key, "Attempting to renew lock (by put). Revision: {}", *revision_arc.lock().unwrap());
                match kv_store.put(&key, lock_value.clone()).await {
                    // put refreshes TTL implicitly if bucket has max_age
                    Ok(new_revision) => {
                        debug!(key = %key, new_revision = new_revision, "Lock renewed successfully.");
                        let mut current_revision = revision_arc.lock().unwrap();
                        *current_revision = new_revision;
                    }
                    Err(e) => {
                        error!(key = %key, error = ?e, "Failed to renew lock. Renewal task stopping.");
                        break;
                    }
                }
            }
            debug!(key = %key, "Lock renewal task finished.");
        })
    }
}

impl Drop for NatsLockGuard {
    #[instrument(skip(self), fields(key = %self.key, value = %String::from_utf8_lossy(self.lock_value.as_ref())))]
    fn drop(&mut self) {
        let revision_to_update = *self.revision.lock().unwrap();
        info!(key = %self.key, revision = revision_to_update, "Dropping NatsLockGuard, attempting conditional release (update to empty).");

        if let Ok(mut guard) = self.stop_renewal_flag.lock() {
            *guard = true;
        }
        self.renewal_task.abort();

        let kv_store_clone = self.kv_store.clone();
        let key_clone = self.key.clone();

        tokio::spawn(async move {
            debug!(key = %key_clone, revision = revision_to_update, "Attempting conditional update to empty for lock in Drop.");

            match kv_store_clone
                .update(&key_clone, Bytes::new(), revision_to_update)
                .await
            {
                Ok(new_revision) => {
                    info!(key = %key_clone, old_revision = revision_to_update, new_revision = new_revision, "Successfully released lock (updated to empty) in NATS KV.");
                }
                Err(update_err) => match update_err.kind() {
                    UpdateErrorKind::WrongLastRevision => {
                        info!(key = %key_clone, expected_revision = revision_to_update, error_kind = ?update_err.kind(), "Conditional release failed: lock revision changed (WrongLastRevision). TTL will handle release.");
                    }
                    _ => {
                        warn!(key = %key_clone, revision = revision_to_update, error = ?update_err, "Failed to update lock to empty in NATS KV (best effort). Other error than WrongLastRevision. TTL is fallback.");
                    }
                },
            }
        });

        debug!(
            "Lock for key '{}' (revision {}) conditional release attempted. TTL is the fallback release mechanism.",
            self.key, revision_to_update
        );
    }
}
