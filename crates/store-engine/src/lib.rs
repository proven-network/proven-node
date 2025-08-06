//! Store implementation backed by the Proven consensus engine
//!
//! This crate provides a simple, performant key-value store that uses
//! the Proven engine's streaming capabilities. It maintains an in-memory
//! view of the data that is continuously updated from the engine's streams.

#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::module_name_repetitions)]

mod consumer;
mod error;
mod view;

pub use error::Error;
use view::{KeyOperation, StoreView};

use std::error::Error as StdError;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_engine::{Client, Message, StreamConfig};
use proven_store::{Store, Store1, Store2, Store3};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::consumer::StoreConsumer;

/// Special stream name for key listing
const KEY_INDEX_STREAM: &str = "__store_keys__";

/// Engine-backed key-value store
#[allow(clippy::type_complexity)]
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
    /// Engine client
    client: Arc<Client>,
    /// Store view
    view: StoreView,
    /// Optional scope prefix
    prefix: Option<String>,
    /// Consumer handles (only set for root store)
    consumer_handles: Arc<Mutex<Option<(JoinHandle<()>, JoinHandle<()>)>>>,
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
            view: self.view.clone(),
            prefix: self.prefix.clone(),
            consumer_handles: self.consumer_handles.clone(),
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
    /// Create a new engine store
    #[must_use]
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            view: StoreView::new(),
            prefix: None,
            consumer_handles: Arc::new(Mutex::new(None)),
            _marker: std::marker::PhantomData,
        }
    }

    /// Create a scoped store
    fn with_scope(client: Arc<Client>, view: StoreView, prefix: String) -> Self {
        Self {
            client,
            view,
            prefix: Some(prefix),
            consumer_handles: Arc::new(Mutex::new(None)),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the stream name for this store
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

    /// Ensure streams exist and start consumers
    #[allow(clippy::cognitive_complexity)]
    async fn ensure_initialized(&self) -> Result<(), Error> {
        // Check if already initialized
        {
            let handles = self.consumer_handles.lock().await;
            if handles.is_some() {
                return Ok(());
            }
        }

        let data_stream = self.get_stream_name();
        let key_stream = self.get_key_index_stream();

        // Create data stream if it doesn't exist
        if let Ok(Some(_)) = self.client.get_stream_info(&data_stream).await {
            debug!("Data stream {} already exists", data_stream);
        } else {
            let config = StreamConfig::default();
            self.client
                .create_group_stream(data_stream.clone(), config)
                .await?;
            info!("Created data stream: {}", data_stream);
        }

        // Create key index stream if it doesn't exist
        if let Ok(Some(_)) = self.client.get_stream_info(&key_stream).await {
            debug!("Key stream {} already exists", key_stream);
        } else {
            let config = StreamConfig::default();
            self.client
                .create_group_stream(key_stream.clone(), config)
                .await?;
            info!("Created key stream: {}", key_stream);
        }

        // Start consumers
        let consumer = StoreConsumer::new(
            self.view.clone(),
            self.client.clone(),
            data_stream,
            key_stream,
        );

        let handles = consumer.start().await?;

        // Store handles
        {
            let mut consumer_handles = self.consumer_handles.lock().await;
            *consumer_handles = Some(handles);
        }

        Ok(())
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
        self.ensure_initialized().await?;

        let key = key.as_ref();
        debug!("Deleting key: {}", key);

        // Update local view immediately
        self.view.delete(key);

        // Track the key removal
        self.track_key_operation(KeyOperation::Remove {
            key: key.to_string(),
        })
        .await?;

        // Mark as deleted in stream
        let message = Message::new(vec![])
            .with_header("deleted", "true")
            .with_header("key", key);

        self.client
            .publish_to_stream(self.get_stream_name(), vec![message])
            .await
            .map(|_| ())
            .map_err(|e| Error::Engine(e.to_string()))
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_initialized().await?;

        let key = key.as_ref();
        debug!("Getting key: {}", key);

        // Get from view
        self.view.get(key).map_or_else(
            || Ok(None),
            |bytes| match T::try_from(bytes) {
                Ok(value) => Ok(Some(value)),
                Err(e) => {
                    debug!("Failed to deserialize value for key {}: {:?}", key, e);
                    Ok(None)
                }
            },
        )
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        self.ensure_initialized().await?;
        Ok(self.view.keys())
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        self.ensure_initialized().await?;
        Ok(self.view.keys_with_prefix(prefix.as_ref()))
    }

    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.ensure_initialized().await?;

        let key = key.as_ref();
        debug!("Putting key: {}", key);

        // Convert value to bytes
        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialization(e.to_string()))?;

        // Update local view immediately
        self.view.put(key.to_string(), bytes.clone());

        // Create message with key in header
        let message = Message::new(bytes.to_vec()).with_header("key", key);

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
        .await
    }
}

// Scoped store implementations
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
                view: StoreView,
                prefix: Option<String>,
                consumer_handles: Arc<Mutex<Option<(JoinHandle<()>, JoinHandle<()>)>>>,
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
                        view: self.view.clone(),
                        prefix: self.prefix.clone(),
                        consumer_handles: self.consumer_handles.clone(),
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
                /// Create a new scoped engine store
                #[must_use]
                pub fn new(client: Arc<Client>) -> Self {
                    Self {
                        client,
                        view: StoreView::new(),
                        prefix: None,
                        consumer_handles: Arc::new(Mutex::new(None)),
                        _marker: std::marker::PhantomData,
                    }
                }

                /// Create a scoped store
                #[allow(dead_code)]
                fn with_scope(client: Arc<Client>, view: StoreView, prefix: String) -> Self {
                    Self {
                        client,
                        view,
                        prefix: Some(prefix),
                        consumer_handles: Arc::new(Mutex::new(None)),
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
                        self.view.clone(),
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
    #[test]
    fn test_store_creation() {
        // Basic compilation test
    }
}
