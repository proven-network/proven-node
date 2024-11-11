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

pub struct NatsStoreOptions {
    pub client: Client,
    pub bucket: String,
    pub max_age: Duration,
    pub persist: bool,
}

#[derive(Clone, Debug)]
pub struct NatsStore {
    bucket: String,
    jetstream_context: JetStreamContext,
    max_age: Duration,
    persist: bool,
}

/// NatsStore is a NATS JetStream implementation of the `Store`, `Store2`, and `Store3` traits.
/// It uses NATS JetStream to store key-value pairs, where keys are strings and values are byte vectors.
/// The store supports optional scoping of keys using bucket name prefixes.
impl NatsStore {
    pub async fn new(
        NatsStoreOptions {
            client,
            bucket,
            max_age,
            persist,
        }: NatsStoreOptions,
    ) -> Result<Self, Error> {
        let jetstream_context = jetstream::new(client.clone());

        Ok(NatsStore {
            bucket,
            jetstream_context,
            max_age,
            persist,
        })
    }

    pub async fn new_with_jetstream_context(
        jetstream_context: JetStreamContext,
        bucket: String,
        max_age: Duration,
        persist: bool,
    ) -> Self {
        NatsStore {
            bucket,
            jetstream_context,
            max_age,
            persist,
        }
    }

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
            .map_err(|e| Error::Create(e.kind()))
    }
}

#[async_trait]
impl Store for NatsStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        self.get_kv_store()
            .await?
            .delete(key)
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Bytes>, Self::SE> {
        self.get_kv_store()
            .await?
            .get(key)
            .await
            .map_err(|e| Error::Get(e.kind()))
    }

    // TODO: Better error handling
    async fn keys(&self) -> Result<Vec<String>, Self::SE> {
        Ok(self
            .get_kv_store()
            .await?
            .keys()
            .await
            .map_err(|e| Error::Keys(e.kind()))?
            .try_collect::<Vec<String>>()
            .await
            .unwrap())
    }

    async fn put(&self, key: String, bytes: Bytes) -> Result<(), Self::SE> {
        self.get_kv_store()
            .await?
            .put(key, bytes)
            .await
            .map_err(|e| Error::Put(e.kind()))?;

        Ok(())
    }
}

#[async_trait]
impl Store1 for NatsStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(format!("{}.{}", self.bucket, scope))
    }
}

#[async_trait]
impl Store2 for NatsStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(format!("{}.{}", self.bucket, scope))
    }
}

#[async_trait]
impl Store3 for NatsStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        self.with_scope(format!("{}.{}", self.bucket, scope))
    }
}
