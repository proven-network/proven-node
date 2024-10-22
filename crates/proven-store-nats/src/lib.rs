mod error;

pub use error::Error;

use std::time::Duration;

use async_nats::jetstream::kv::{Config, Store as KvStore};
use async_trait::async_trait;
use proven_store::{Store, Store1, Store2};

#[derive(Clone, Debug)]
pub struct NatsStore {
    bucket: String,
    jetstream_context: async_nats::jetstream::Context,
    max_age: Duration,
}

/// NatsStore is a NATS JetStream implementation of the `Store`, `Store1`, and `Store2` traits.
/// It uses NATS JetStream to store key-value pairs, where keys are strings and values are byte vectors.
/// The store supports optional scoping of keys using bucket name prefixes.
impl NatsStore {
    pub async fn new(
        client: async_nats::Client,
        bucket: String,
        max_age: Duration,
    ) -> Result<Self, Error> {
        let jetstream_context = async_nats::jetstream::new(client.clone());

        Ok(NatsStore {
            bucket,
            jetstream_context,
            max_age,
        })
    }

    fn with_scope(&self, scope: String) -> Self {
        Self {
            bucket: scope,
            jetstream_context: self.jetstream_context.clone(),
            max_age: self.max_age,
        }
    }

    async fn get_kv_store(&self) -> Result<KvStore, Error> {
        let config = Config {
            bucket: self.bucket.clone(),
            max_age: self.max_age,
            ..Default::default()
        };

        self.jetstream_context
            .create_key_value(config)
            .await
            .map_err(|e| Error::Create(e.kind()))
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

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        self.get_kv_store()
            .await?
            .get(key)
            .await
            .map_err(|e| Error::Get(e.kind()))
            .map(|v| v.map(|v| v.to_vec()))
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        self.get_kv_store()
            .await?
            .put(key, bytes.into())
            .await
            .map_err(|e| Error::Put(e.kind()))?;

        Ok(())
    }
}
