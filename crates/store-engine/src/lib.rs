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
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_engine::{Client, stream::StreamConfig};
use proven_store::{Store, Store1, Store2, Store3};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Special stream name for key listing
const KEY_INDEX_STREAM: &str = "__store_keys__";

/// Key operation for tracking keys in a store
#[derive(Debug, Clone, Serialize, Deserialize)]
enum KeyOperation {
    /// A key was added
    Add { key: String },
    /// A key was removed  
    Remove { key: String },
}

/// In-memory state for a store
#[derive(Debug, Clone)]
struct StoreState<T> {
    /// The key-value data
    data: HashMap<String, T>,
    /// Last sequence number read from stream
    last_sequence: u64,
}

/// Engine-backed key-value store
pub struct EngineStore<T = Bytes, D = Infallible, S = Infallible>
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
    client: Arc<dyn ClientWrapper>,
    /// Optional scope prefix
    prefix: Option<String>,
    /// In-memory state cache
    state: Arc<RwLock<StoreState<T>>>,
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
            state: self.state.clone(),
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

/// Trait to abstract over the engine client's concrete types
#[async_trait]
trait ClientWrapper: Debug + Send + Sync + 'static {
    /// Create a stream with automatic group assignment
    async fn create_stream_auto(&self, name: String, config: StreamConfig) -> Result<(), Error>;

    /// Publish to a stream
    async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<(), Error>;

    /// Check if stream exists
    async fn stream_exists(&self, name: &str) -> Result<bool, Error>;

    /// Read messages from a stream
    async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: u64,
        count: u64,
    ) -> Result<Vec<proven_engine::stream::StoredMessage>, Error>;
}

/// Wrapper implementation for the engine client
struct ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    inner: Client<T, G, L>,
}

impl<T, G, L> Debug for ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientWrapperImpl").finish()
    }
}

#[async_trait]
impl<T, G, L> ClientWrapper for ClientWrapperImpl<T, G, L>
where
    T: proven_transport::Transport + Debug + 'static,
    G: proven_topology::TopologyAdaptor + 'static,
    L: proven_storage::LogStorage + Debug + 'static,
{
    async fn create_stream_auto(&self, name: String, config: StreamConfig) -> Result<(), Error> {
        self.inner
            .create_stream(name, config)
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))
    }

    async fn publish(
        &self,
        stream: String,
        payload: Vec<u8>,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<(), Error> {
        self.inner
            .publish(stream, payload, metadata)
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))
    }

    async fn stream_exists(&self, name: &str) -> Result<bool, Error> {
        self.inner
            .get_stream_info(name)
            .await
            .map(|info| info.is_some())
            .map_err(|e| Error::Engine(e.to_string()))
    }

    async fn read_stream(
        &self,
        stream_name: String,
        start_sequence: u64,
        count: u64,
    ) -> Result<Vec<proven_engine::stream::StoredMessage>, Error> {
        self.inner
            .read_stream(stream_name, start_sequence, count)
            .await
            .map_err(|e| Error::Engine(e.to_string()))
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
    pub fn new<Tr, G, L>(client: Client<Tr, G, L>) -> Self
    where
        Tr: proven_transport::Transport + Debug + 'static,
        G: proven_topology::TopologyAdaptor + 'static,
        L: proven_storage::LogStorage + Debug + 'static,
    {
        Self {
            client: Arc::new(ClientWrapperImpl { inner: client }),
            prefix: None,
            state: Arc::new(RwLock::new(StoreState {
                data: HashMap::new(),
                last_sequence: 0,
            })),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a scoped store
    fn with_scope(client: Arc<dyn ClientWrapper>, prefix: String) -> Self {
        Self {
            client,
            prefix: Some(prefix),
            state: Arc::new(RwLock::new(StoreState {
                data: HashMap::new(),
                last_sequence: 0,
            })),
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
        if !self.client.stream_exists(&stream_name).await? {
            // Create the stream
            let config = StreamConfig::default();
            match self
                .client
                .create_stream_auto(stream_name.clone(), config)
                .await
            {
                Ok(()) => {
                    info!("Created store stream: {}", stream_name);
                }
                Err(e) => {
                    // Check if error is because stream already exists
                    let error_msg = e.to_string();
                    if error_msg.contains("already exists") {
                        debug!("Store stream {} already exists, continuing", stream_name);
                    } else {
                        return Err(e);
                    }
                }
            }

            // Wait for stream to be fully available in routing table
            // This prevents race conditions where the stream is created but not yet routable
            let mut retries = 0;

            while retries < MAX_RETRIES {
                // Try to verify the stream is accessible by checking if it exists
                match self.client.stream_exists(&stream_name).await {
                    Ok(true) => {
                        debug!("Store stream {} verified as accessible", stream_name);
                        break;
                    }
                    Ok(false) => {
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
        if !self.client.stream_exists(&key_index).await? {
            let config = StreamConfig::default();
            match self
                .client
                .create_stream_auto(key_index.clone(), config)
                .await
            {
                Ok(()) => {
                    info!("Created key index stream: {}", key_index);
                }
                Err(e) => {
                    // Check if error is because stream already exists
                    let error_msg = e.to_string();
                    if error_msg.contains("already exists") {
                        debug!("Key index stream {} already exists, continuing", key_index);
                    } else {
                        return Err(e);
                    }
                }
            }

            // Wait for key index stream to be fully available
            let mut retries = 0;

            while retries < MAX_RETRIES_KEY_INDEX {
                match self.client.stream_exists(&key_index).await {
                    Ok(true) => {
                        debug!("Key index stream {} verified as accessible", key_index);
                        break;
                    }
                    Ok(false) => {
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

    /// Track a key operation
    async fn track_key_operation(&self, op: KeyOperation) -> Result<(), Error> {
        let payload = serde_json::to_vec(&op).map_err(|e| Error::Serialization(e.to_string()))?;

        self.client
            .publish(self.get_key_index_stream(), payload, None)
            .await?;

        Ok(())
    }

    /// Sync state from stream
    async fn sync_state(&self) -> Result<(), Error> {
        let stream_name = self.get_stream_name();
        let mut state = self.state.write().await;

        // Read messages from where we left off
        let batch_size = 100;
        loop {
            let messages = self
                .client
                .read_stream(stream_name.clone(), state.last_sequence + 1, batch_size)
                .await?;

            if messages.is_empty() {
                break;
            }

            let message_count = messages.len();
            for msg in messages {
                // Extract key from metadata
                if let Some(key) = msg
                    .data
                    .headers
                    .iter()
                    .find(|(k, _)| k == "key")
                    .map(|(_, v)| v.clone())
                {
                    // Check if this is a delete (empty payload with deleted=true)
                    let is_deleted = msg
                        .data
                        .headers
                        .iter()
                        .any(|(k, v)| k == "deleted" && v == "true");

                    if is_deleted {
                        state.data.remove(&key);
                    } else {
                        // Try to deserialize the value
                        match T::try_from(msg.data.payload) {
                            Ok(value) => {
                                state.data.insert(key, value);
                            }
                            Err(e) => {
                                debug!("Failed to deserialize value: {:?}", e);
                            }
                        }
                    }
                }

                state.last_sequence = msg.sequence;
            }

            #[allow(clippy::cast_possible_truncation)]
            if message_count < batch_size as usize {
                break;
            }
        }

        drop(state);
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

        // In a real implementation, we'd need a way to mark a key as deleted
        // For now, we'll store a special tombstone value
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("deleted".to_string(), "true".to_string());
        metadata.insert("key".to_string(), key.to_string());

        self.client
            .publish(self.get_stream_name(), vec![], Some(metadata))
            .await?;

        Ok(())
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_stream().await?;

        let key = key.as_ref();
        debug!("Getting key: {}", key);

        // Sync state from stream
        self.sync_state().await?;

        // Get value from state
        let state = self.state.read().await;
        Ok(state.data.get(key).cloned())
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

        // Sync state from stream
        self.sync_state().await?;

        // Get keys from state
        Ok(self
            .state
            .read()
            .await
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

        // Clone the value before conversion since we need it later
        let value_clone = value.clone();

        // Convert value to bytes
        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Create metadata with the key
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("key".to_string(), key.to_string());

        // Publish the value
        self.client
            .publish(self.get_stream_name(), bytes.to_vec(), Some(metadata))
            .await?;

        // Track the key addition
        self.track_key_operation(KeyOperation::Add {
            key: key.to_string(),
        })
        .await?;

        // Update local state
        let mut state = self.state.write().await;
        state.data.insert(key.to_string(), value_clone);
        drop(state);

        Ok(())
    }
}

// Scoped store implementations following the same pattern as MemoryStore

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< EngineStore $index >]<T = Bytes, D = Infallible, S = Infallible>
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
                client: Arc<dyn ClientWrapper>,
                prefix: Option<String>,
                state: Arc<RwLock<StoreState<T>>>,
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
                        state: self.state.clone(),
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
                #[must_use] pub fn new<Tr, G, L>(
                    client: Client<Tr, G, L>,
                ) -> Self
                where
                    Tr: proven_transport::Transport + Debug + 'static,
                    G: proven_topology::TopologyAdaptor + 'static,
                    L: proven_storage::LogStorage + Debug + 'static,
                {
                    Self {
                        client: Arc::new(ClientWrapperImpl { inner: client }),
                        prefix: None,
                        state: Arc::new(RwLock::new(StoreState {
                            data: HashMap::new(),
                            last_sequence: 0,
                        })),
                        _marker: std::marker::PhantomData,
                    }
                }

                /// Create a scoped store
                #[allow(dead_code)]
                fn with_scope(
                    client: Arc<dyn ClientWrapper>,
                    prefix: String,
                ) -> Self {
                    Self {
                        client,
                        prefix: Some(prefix),
                        state: Arc::new(RwLock::new(StoreState {
                            data: HashMap::new(),
                            last_sequence: 0,
                        })),
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
