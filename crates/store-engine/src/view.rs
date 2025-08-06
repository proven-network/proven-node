//! In-memory view of store state built from stream events

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

/// Operation on a key in the store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyOperation {
    /// Key was added or updated
    Add { key: String },
    /// Key was removed
    Remove { key: String },
}

/// In-memory view of the store, built from consuming the stream
#[derive(Clone)]
pub struct StoreView {
    /// The key-value data
    data: Arc<DashMap<String, Bytes>>,

    /// All keys that exist (for efficient key listing)
    keys: Arc<DashMap<String, ()>>,

    /// Last processed sequence number for data stream
    last_data_seq: Arc<AtomicU64>,

    /// Last processed sequence number for key stream
    last_key_seq: Arc<AtomicU64>,

    /// Notify for sequence updates
    seq_notify: Arc<Notify>,
}

impl StoreView {
    /// Create a new empty view
    #[must_use]
    pub fn new() -> Self {
        Self {
            data: Arc::new(DashMap::new()),
            keys: Arc::new(DashMap::new()),
            last_data_seq: Arc::new(AtomicU64::new(0)),
            last_key_seq: Arc::new(AtomicU64::new(0)),
            seq_notify: Arc::new(Notify::new()),
        }
    }

    /// Apply a data message to the view
    pub fn apply_data_message(&self, key: String, value: Option<Bytes>, sequence: u64) {
        if let Some(value) = value {
            // Add or update
            self.data.insert(key.clone(), value);
            self.keys.insert(key, ());
        } else {
            // Delete
            self.data.remove(&key);
            self.keys.remove(&key);
        }

        self.last_data_seq.store(sequence, Ordering::SeqCst);
        self.seq_notify.notify_waiters();
    }

    /// Apply a key operation to the view
    pub fn apply_key_operation(&self, operation: &KeyOperation, sequence: u64) {
        match operation {
            KeyOperation::Add { key } => {
                self.keys.insert(key.clone(), ());
            }
            KeyOperation::Remove { key } => {
                self.keys.remove(key);
            }
        }

        self.last_key_seq.store(sequence, Ordering::SeqCst);
    }

    /// Get a value by key
    #[must_use]
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.data.get(key).map(|entry| entry.value().clone())
    }

    /// Put a value (updates local view immediately)
    pub fn put(&self, key: String, value: Bytes) {
        self.data.insert(key.clone(), value);
        self.keys.insert(key, ());
    }

    /// Delete a value (updates local view immediately)
    pub fn delete(&self, key: &str) {
        self.data.remove(key);
        self.keys.remove(key);
    }

    /// Get all keys
    #[must_use]
    pub fn keys(&self) -> Vec<String> {
        self.keys.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Get keys with a prefix
    #[must_use]
    pub fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.keys
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the last processed data sequence
    #[allow(dead_code)]
    pub fn last_data_seq(&self) -> u64 {
        self.last_data_seq.load(Ordering::SeqCst)
    }

    /// Get the last processed key sequence
    #[allow(dead_code)]
    pub fn last_key_seq(&self) -> u64 {
        self.last_key_seq.load(Ordering::SeqCst)
    }

    /// Wait for a specific data sequence to be processed
    #[allow(dead_code)]
    pub async fn wait_for_data_seq(&self, target_seq: u64) {
        loop {
            let current_seq = self.last_data_seq.load(Ordering::SeqCst);
            if current_seq >= target_seq {
                return;
            }
            self.seq_notify.notified().await;
        }
    }
}

impl Default for StoreView {
    fn default() -> Self {
        Self::new()
    }
}
