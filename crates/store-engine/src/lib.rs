//! Store implementation backed by the Proven consensus engine
//!
//! This crate provides a key-value store implementation that uses the Proven
//! consensus engine's stream functionality for storage. Each store operation
//! is backed by the distributed consensus system, providing strong consistency
//! and fault tolerance.
//!
//! # Architecture
//!
//! The store maps KV operations to stream operations:
//! - Each scoped store corresponds to a stream in the consensus engine
//! - Keys are encoded as stream message keys
//! - Values are stored as stream message payloads
//! - Metadata (like TTL) can be stored in message headers

#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

mod error;

pub use error::Error;

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use lru::LruCache;
use proven_engine::{Client, StreamConfig};
use proven_storage::LogIndex;
use proven_store::{Store, Store1, Store2, Store3};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, info};

/// Special stream name for key listing
const KEY_INDEX_STREAM: &str = "__store_keys__";

/// Default cache size for store states
const DEFAULT_CACHE_SIZE: usize = 1000;

/// How often to sync with the stream by default
const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_millis(100);

/// Type alias for the store state cache
type StoreStateCache = Arc<Mutex<LruCache<String, Arc<CachedStoreState>>>>;

/// Global cache for store states to avoid replaying messages
static STORE_STATE_CACHE: std::sync::OnceLock<StoreStateCache> = std::sync::OnceLock::new();

/// Get or initialize the global store state cache
fn get_store_cache() -> StoreStateCache {
    STORE_STATE_CACHE
        .get_or_init(|| {
            Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap(),
            )))
        })
        .clone()
}

/// Key operation for tracking keys in a store
#[derive(Debug, Clone, Serialize, Deserialize)]
enum KeyOperation {
    /// A key was added
    Add { key: String },
    /// A key was removed  
    Remove { key: String },
}

/// Cached store state that can be shared across instances
struct CachedStoreState {
    /// The actual store state
    state: Arc<RwLock<StoreStateInner>>,
    /// When we last synced with the stream
    last_sync: Arc<RwLock<Instant>>,
    /// Stream name for this state
    #[allow(dead_code)]
    stream_name: String,
}

/// Inner state that's protected by `RwLock`
#[derive(Debug, Clone)]
struct StoreStateInner {
    /// The key-value data  
    data: HashMap<String, Bytes>,
    /// Last sequence number read from stream
    last_sequence: u64,
}

/// Engine-backed key-value store
pub struct EngineStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// The engine client
    client: Arc<Client>,
    /// Optional scope prefix
    prefix: Option<String>,
    /// Phantom data for type parameters
    _marker: std::marker::PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for EngineStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, D, S> Debug for EngineStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineStore")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl<T, D, S> EngineStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// Create a new engine store with automatic group assignment
    #[must_use]
    pub const fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            prefix: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a scoped store
    const fn with_scope(client: Arc<Client>, prefix: String) -> Self {
        Self {
            client,
            prefix: Some(prefix),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the stream name for a given key
    fn get_stream_name(&self) -> String {
        self.prefix.as_ref().map_or_else(
            || "store_default".to_string(),
            |prefix| format!("store_{prefix}"),
        )
    }

    /// Get the key index stream name
    fn get_key_index_stream(&self) -> String {
        self.prefix.as_ref().map_or_else(
            || KEY_INDEX_STREAM.to_string(),
            |prefix| format!("{KEY_INDEX_STREAM}_{prefix}"),
        )
    }

    /// Ensure the stream exists
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::too_many_lines)]
    async fn ensure_stream(&self) -> Result<(), Error> {
        const MAX_RETRIES: u32 = 10;
        const RETRY_DELAY_MS: u64 = 100;

        const MAX_RETRIES_KEY_INDEX: u32 = 10;
        const RETRY_DELAY_MS_KEY_INDEX: u64 = 100;

        let stream_name = self.get_stream_name();

        // Check if stream exists
        let stream_exists = match self.client.get_stream_info(&stream_name).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !stream_exists {
            // Create the stream
            let config = StreamConfig::default();
            self.client
                .create_group_stream(stream_name.clone(), config)
                .await
                .map_err(|e| Error::Engine(e.to_string()))?;
            info!("Created store stream: {}", stream_name);

            // Wait for stream to be fully available in routing table
            // This prevents race conditions where the stream is created but not yet routable
            let mut retries = 0;

            while retries < MAX_RETRIES {
                // Try to verify the stream is accessible by checking if it exists
                match self.client.get_stream_info(&stream_name).await {
                    Ok(Some(_)) => {
                        debug!("Store stream {} verified as accessible", stream_name);
                        break;
                    }
                    Ok(None) => {
                        debug!(
                            "Store stream {} not yet accessible, retrying...",
                            stream_name
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Error checking store stream {}: {}, retrying...",
                            stream_name, e
                        );
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                retries += 1;
            }

            if retries >= MAX_RETRIES {
                return Err(Error::Engine(format!(
                    "Stream {stream_name} created but not accessible after {MAX_RETRIES} retries"
                )));
            }
        }

        // Also ensure key index stream exists
        let key_index = self.get_key_index_stream();
        let key_index_exists = match self.client.get_stream_info(&key_index).await {
            Ok(Some(_)) => true,
            Ok(None) | Err(_) => false,
        };

        if !key_index_exists {
            let config = StreamConfig::default();
            self.client
                .create_group_stream(key_index.clone(), config)
                .await
                .map_err(|e| Error::Engine(e.to_string()))?;
            info!("Created key index stream: {}", key_index);

            // Wait for key index stream to be fully available
            let mut retries = 0;

            while retries < MAX_RETRIES_KEY_INDEX {
                match self.client.get_stream_info(&key_index).await {
                    Ok(Some(_)) => {
                        debug!("Key index stream {} verified as accessible", key_index);
                        break;
                    }
                    Ok(None) => {
                        debug!(
                            "Key index stream {} not yet accessible, retrying...",
                            key_index
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Error checking key index stream {}: {}, retrying...",
                            key_index, e
                        );
                    }
                }

                tokio::time::sleep(tokio::time::Duration::from_millis(RETRY_DELAY_MS_KEY_INDEX))
                    .await;
                retries += 1;
            }

            if retries >= MAX_RETRIES {
                return Err(Error::Engine(format!(
                    "Key index stream {key_index} created but not accessible after {MAX_RETRIES} retries"
                )));
            }
        }

        Ok(())
    }

    /// Get or create the cached state for this store
    async fn get_or_create_cached_state(&self) -> Result<Arc<CachedStoreState>, Error> {
        let stream_name = self.get_stream_name();
        let cache = get_store_cache();

        // Check if we already have this state cached
        {
            let mut cache_guard = cache.lock().await;
            if let Some(cached) = cache_guard.get(&stream_name) {
                return Ok(cached.clone());
            }
        }

        // Create new cached state
        let cached_state = Arc::new(CachedStoreState {
            state: Arc::new(RwLock::new(StoreStateInner {
                data: HashMap::new(),
                last_sequence: 0,
            })),
            last_sync: Arc::new(RwLock::new(
                Instant::now().checked_sub(Duration::from_secs(60)).unwrap(),
            )), // Force initial sync
            stream_name: stream_name.clone(),
        });

        // Insert into cache
        {
            let mut cache_guard = cache.lock().await;
            cache_guard.put(stream_name, cached_state.clone());
        }

        Ok(cached_state)
    }

    /// Check if we should sync the state
    async fn should_sync(&self, cached_state: &CachedStoreState) -> bool {
        let last_sync = cached_state.last_sync.read().await;
        last_sync.elapsed() > DEFAULT_SYNC_INTERVAL
    }

    /// Track a key operation
    async fn track_key_operation(&self, op: KeyOperation) -> Result<(), Error> {
        let payload = serde_json::to_vec(&op).map_err(|e| Error::Serialization(e.to_string()))?;

        self.client
            .publish_to_stream(self.get_key_index_stream(), vec![payload])
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))
    }

    /// Sync state from stream (incremental)
    async fn sync_state(&self, cached_state: &Arc<CachedStoreState>) -> Result<(), Error> {
        use futures::StreamExt;

        let stream_name = self.get_stream_name();

        // Check if we should sync
        if !self.should_sync(cached_state).await {
            return Ok(());
        }

        // Get current state
        let last_sequence = {
            let state = cached_state.state.read().await;
            state.last_sequence
        };

        // Read only new messages since last sync
        let start = LogIndex::new(last_sequence + 1)
            .ok_or_else(|| Error::Engine("Start sequence must be greater than 0".to_string()))?;

        // Use streaming API for bounded read
        let stream = self
            .client
            .stream_messages(stream_name.clone(), Some(start))
            .await
            .map_err(|e| Error::Engine(e.to_string()))?;

        let mut total_messages = 0;

        // Process messages
        {
            let mut state = cached_state.state.write().await;
            let mut message_stream = Box::pin(stream);

            while let Some((msg, _timestamp, sequence)) = message_stream.next().await {
                total_messages += 1;

                // Extract key from headers
                if let Some(key) = msg
                    .headers
                    .iter()
                    .find(|(k, _)| k == "key")
                    .map(|(_, v)| v.clone())
                {
                    // Check if this is a delete (empty payload with deleted=true)
                    let is_deleted = msg
                        .headers
                        .iter()
                        .any(|(k, v)| k == "deleted" && v == "true");

                    if is_deleted {
                        state.data.remove(&key);
                    } else {
                        // Store the payload directly as bytes
                        state.data.insert(key, msg.payload);
                    }
                }

                state.last_sequence = sequence;
            }
            drop(state);
        }

        // Update last sync time
        {
            let mut last_sync = cached_state.last_sync.write().await;
            *last_sync = Instant::now();
        }

        if total_messages > 0 {
            debug!(
                "Synced {} new messages for stream {}",
                total_messages, stream_name
            );
        }

        Ok(())
    }
}

#[async_trait]
impl<T, D, S> Store<T, D, S> for EngineStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;

    async fn delete<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_stream().await?;

        let key = key.as_ref();
        debug!("Deleting key: {}", key);

        // Track the key removal
        self.track_key_operation(KeyOperation::Remove {
            key: key.to_string(),
        })
        .await?;

        // Mark as deleted in stream
        let message = proven_engine::Message::new(vec![])
            .with_header("deleted", "true")
            .with_header("key", key);

        self.client
            .publish_to_stream(self.get_stream_name(), vec![message])
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))?;

        // Update cache immediately
        let cached_state = self.get_or_create_cached_state().await?;
        {
            let mut state = cached_state.state.write().await;
            state.data.remove(key);
            // Don't update sequence number here - let sync handle it
        }

        Ok(())
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_stream().await?;

        let key = key.as_ref();
        debug!("Getting key: {}", key);

        // Get or create cached state
        let cached_state = self.get_or_create_cached_state().await?;

        // Sync if needed (incremental)
        self.sync_state(&cached_state).await?;

        // Get value from cache
        let state = cached_state.state.read().await;
        state.data.get(key).map_or_else(
            || Ok(None),
            |bytes| match T::try_from(bytes.clone()) {
                Ok(value) => Ok(Some(value)),
                Err(e) => {
                    debug!("Failed to deserialize value for key {}: {:?}", key, e);
                    Ok(None)
                }
            },
        )
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        self.keys_with_prefix("").await
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        self.ensure_stream().await?;

        let prefix = prefix.as_ref();
        debug!("Listing keys with prefix: {}", prefix);

        // Get or create cached state
        let cached_state = self.get_or_create_cached_state().await?;

        // Sync if needed (incremental)
        self.sync_state(&cached_state).await?;

        // Get keys from cache
        let state = cached_state.state.read().await;
        Ok(state
            .data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_stream().await?;

        let key = key.as_ref();
        debug!("Putting key: {}", key);

        // Convert value to bytes
        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Create message with key in header
        let message = proven_engine::Message::new(bytes.to_vec()).with_header("key", key);

        // Publish the value
        self.client
            .publish_to_stream(self.get_stream_name(), vec![message])
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))?;

        // Track the key addition
        self.track_key_operation(KeyOperation::Add {
            key: key.to_string(),
        })
        .await?;

        // Update cache immediately
        let cached_state = self.get_or_create_cached_state().await?;
        {
            let mut state = cached_state.state.write().await;
            state.data.insert(key.to_string(), bytes);
            // Don't update sequence number here - let sync handle it
        }

        Ok(())
    }
}

// Scoped store implementations following the same pattern as MemoryStore

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< EngineStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                client: Arc<Client>,
                prefix: Option<String>,
                _marker: std::marker::PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< EngineStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        prefix: self.prefix.clone(),
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            impl<T, D, S> Debug for [< EngineStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                    f.debug_struct(stringify!([< EngineStore $index >]))
                        .field("prefix", &self.prefix)
                        .finish()
                }
            }

            impl<T, D, S> [< EngineStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                /// Create a new scoped engine store with automatic group assignment
                #[must_use]
                pub const fn new(
                    client: Arc<Client>,
                ) -> Self {
                    Self {
                        client,
                        prefix: None,
                        _marker: std::marker::PhantomData,
                    }
                }

                /// Create a scoped store
                #[allow(dead_code)]
                const fn with_scope(
                    client: Arc<Client>,
                    prefix: String,
                ) -> Self {
                    Self {
                        client,
                        prefix: Some(prefix),
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< EngineStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                type Error = Error;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> $parent<T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.as_ref()),
                        None => scope.as_ref().to_string(),
                    };
                    $parent::<T, D, S>::with_scope(
                        self.client.clone(),
                        new_scope,
                    )
                }
            }
        }
    };
}

impl_scoped_store!(
    1,
    EngineStore,
    Store,
    "A single-scoped engine-backed KV store."
);
impl_scoped_store!(
    2,
    EngineStore1,
    Store1,
    "A double-scoped engine-backed KV store."
);
impl_scoped_store!(
    3,
    EngineStore2,
    Store2,
    "A triple-scoped engine-backed KV store."
);

#[cfg(test)]
mod tests {

    // TODO: Add integration tests with a test engine setup
    #[test]
    fn test_store_creation() {
        // This is just a placeholder to ensure the module compiles
        // Real tests would require setting up an engine with test infrastructure
    }
}
