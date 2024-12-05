//! Implementation of key-value storage using NATS with HA replication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv::{Config, Store as KvStore};
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::Client;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use proven_store::{Store, Store1, Store2, Store3};

/// Options for configuring a `NatsStore`.
pub struct NatsStoreOptions {
    /// The NATS client to use.
    pub client: Client,

    /// The bucket to use for the key-value store (may be refined through scopes)
    pub bucket: String,

    /// The maximum age of entries in the store. Use `Duration::ZERO` for no expiry.
    pub max_age: Duration,

    /// Whether to persist the store to disk.
    pub persist: bool,
}

/// KV store using NATS JS.
#[derive(Clone, Debug)]
pub struct NatsStore {
    bucket: String,
    jetstream_context: JetStreamContext,
    max_age: Duration,
    persist: bool,
}

impl NatsStore {
    /// Creates a new `NatsStore` with the specified options.
    #[must_use]
    pub fn new(
        NatsStoreOptions {
            client,
            bucket,
            max_age,
            persist,
        }: NatsStoreOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client);

        Self {
            bucket,
            jetstream_context,
            max_age,
            persist,
        }
    }

    #[allow(dead_code)]
    fn with_scope(&self, scope: String) -> Self {
        Self {
            bucket: scope,
            jetstream_context: self.jetstream_context.clone(),
            max_age: self.max_age,
            persist: self.persist,
        }
    }

    async fn get_kv_store(&self) -> Result<KvStore, Error> {
        let config = Config {
            bucket: self.bucket.clone(),
            max_age: self.max_age,
            storage: if self.persist {
                jetstream::stream::StorageType::File
            } else {
                jetstream::stream::StorageType::Memory
            },
            ..Default::default()
        };

        self.jetstream_context
            .create_key_value(config)
            .await
            .map_err(|e| Error::CreateKeyValue(e.kind()))
    }
}

#[async_trait]
impl Store for NatsStore {
    type Error = Error;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        self.get_kv_store()
            .await?
            .delete(key.into())
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
        self.get_kv_store()
            .await?
            .get(key)
            .await
            .map_err(|e| Error::Entry(e.kind()))
    }

    // TODO: Better error handling
    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        Ok(self
            .get_kv_store()
            .await?
            .keys()
            .await
            .map_err(|e| Error::Watch(e.kind()))?
            .try_collect::<Vec<String>>()
            .await
            .unwrap())
    }

    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::Error> {
        self.get_kv_store()
            .await?
            .put(key.into(), bytes)
            .await
            .map_err(|e| Error::Put(e.kind()))?;

        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! NatsStore $index]]
            [!set! #trait_name = [!ident! Store $index]]

            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct #name {
                bucket: String,
                jetstream_context: JetStreamContext,
                max_age: Duration,
                persist: bool,
            }

            impl #name {
                /// Creates a new `#name` with the specified options.
                #[must_use]
                pub fn new(
                    NatsStoreOptions {
                        client,
                        bucket,
                        max_age,
                        persist,
                    }: NatsStoreOptions,
                ) -> Self {
                    let jetstream_context = jetstream::new(client);

                    Self {
                        bucket,
                        jetstream_context,
                        max_age,
                        persist,
                    }
                }

                fn with_scope(&self, scope: String) -> $parent {
                    $parent {
                        bucket: scope,
                        jetstream_context: self.jetstream_context.clone(),
                        max_age: self.max_age,
                        persist: self.persist,
                    }
                }
            }

            #[async_trait]
            impl #trait_name for #name {
                type Error = Error;
                type Scoped = $parent;

                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> $parent {
                    self.with_scope(format!("{}.{}", self.bucket, scope.into()))
                }
            }
        }
    };
}

impl_scoped_store!(
    1,
    NatsStore,
    Store,
    "A single-scoped KV store using NATS JetStream."
);
impl_scoped_store!(
    2,
    NatsStore1,
    Store1,
    "A double-scoped KV store using NATS JetStream."
);
impl_scoped_store!(
    3,
    NatsStore2,
    Store2,
    "A triple-scoped KV store using NATS JetStream."
);
