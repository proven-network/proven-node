//! Implementation of key-value storage using NATS with HA replication.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::too_many_lines)]

mod error;

pub use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Result as FmtResult, Write};
use std::marker::PhantomData;
use std::time::Duration;

use async_nats::Client;
use async_nats::jetstream;
use async_nats::jetstream::Context as JetStreamContext;
use async_nats::jetstream::kv::{Config, Store as KvStore};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use proven_store::{Store, Store1, Store2, Store3};

/// Options for configuring a `NatsStore`.
pub struct NatsStoreOptions {
    /// The bucket to use for the key-value store (may be refined through scopes)
    pub bucket: String,

    /// The NATS client to use.
    pub client: Client,

    /// The maximum age of entries in the store. Use `Duration::ZERO` for no expiry.
    pub max_age: Duration,

    /// Number of replicas for the KV store. Should be set to at least 3 in production for HA.
    pub num_replicas: usize,

    /// Whether to persist the store to disk.
    pub persist: bool,
}

/// KV store using NATS JS.
#[derive(Debug)]
pub struct NatsStore<T = Bytes, D = Infallible, S = Infallible>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
{
    bucket: String,
    client: Client,
    jetstream_context: JetStreamContext,
    max_age: Duration,
    num_replicas: usize,
    persist: bool,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for NatsStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            client: self.client.clone(),
            jetstream_context: self.jetstream_context.clone(),
            max_age: self.max_age,
            num_replicas: self.num_replicas,
            persist: self.persist,
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> NatsStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
{
    /// Creates a new `NatsStore` with the specified options.
    #[must_use]
    pub fn new(
        NatsStoreOptions {
            client,
            bucket,
            max_age,
            num_replicas,
            persist,
        }: NatsStoreOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client.clone());

        Self {
            bucket,
            client,
            jetstream_context,
            max_age,
            num_replicas,
            persist,
            _marker: PhantomData,
        }
    }

    async fn get_kv_store(&self) -> Result<KvStore, Error> {
        let config = Config {
            bucket: self.bucket.clone(),
            max_age: self.max_age,
            num_replicas: self.num_replicas,
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
impl<T, D, S> Store<T, D, S> for NatsStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
{
    type Error = Error;

    async fn delete<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.get_kv_store()
            .await?
            .delete(key.as_ref())
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        match self
            .get_kv_store()
            .await?
            .get(key.as_ref())
            .await
            .map_err(|e| Error::Entry(e.kind()))?
        {
            Some(bytes) => {
                let value: T = bytes
                    .try_into()
                    .map_err(|e: D| Error::Deserialize(e.to_string()))?;

                Ok(Some(value))
            }
            _ => Ok(None),
        }
    }

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

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        let prefix = prefix.as_ref();

        Ok(self
            .get_kv_store()
            .await?
            .keys()
            .await
            .map_err(|e| Error::Watch(e.kind()))?
            .try_collect::<Vec<String>>()
            .await
            .unwrap()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .collect())
    }

    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;

        self.get_kv_store()
            .await?
            .put(key.as_ref(), bytes)
            .await
            .map_err(|e| Error::Put(e.kind()))?;

        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< NatsStore $index >]<T = Bytes, D = Infallible, S = Infallible>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                bucket: String,
                client: Client,
                jetstream_context: JetStreamContext,
                max_age: Duration,
                num_replicas: usize,
                persist: bool,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< NatsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        bucket: self.bucket.clone(),
                        client: self.client.clone(),
                        jetstream_context: self.jetstream_context.clone(),
                        max_age: self.max_age,
                        num_replicas: self.num_replicas,
                        persist: self.persist,
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> Debug for [< NatsStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                    f.debug_struct(stringify!([< NatsStore $index >]))
                        .field("bucket", &self.bucket)
                        .finish()
                }
            }

            impl<T, D, S> [< NatsStore $index >]<T, D, S>
            where
                Self: Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                /// Creates a new `[< NatsStore $index >]` with the specified options.
                #[must_use]
                pub fn new(
                    NatsStoreOptions {
                        client,
                        bucket,
                        max_age,
                        num_replicas,
                        persist,
                    }: NatsStoreOptions,
                ) -> Self {
                    let jetstream_context = jetstream::new(client.clone());

                    Self {
                        bucket,
                        client,
                        jetstream_context,
                        max_age,
                        num_replicas,
                        persist,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< NatsStore $index >]<T, D, S>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
            {
                type Error = Error;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: AsRef<str> + Send,
                {
                    let mut bucket = self.bucket.clone();
                    write!(bucket, "_{}", scope.as_ref()).unwrap();

                    Self::Scoped::new(NatsStoreOptions {
                        client: self.client.clone(),
                        bucket,
                        max_age: self.max_age,
                        num_replicas: self.num_replicas,
                        persist: self.persist,
                    })
                }
            }
        }
    };
}

impl_scoped_store!(
    1,
    NatsStore,
    Store,
    "A single-scoped KV store using NATS `JetStream`."
);
impl_scoped_store!(
    2,
    NatsStore1,
    Store1,
    "A double-scoped KV store using NATS `JetStream`."
);
impl_scoped_store!(
    3,
    NatsStore2,
    Store2,
    "A triple-scoped KV store using NATS `JetStream`."
);

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    #[tokio::test]
    async fn test_keys_with_prefix() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let store = NatsStore::new(NatsStoreOptions {
            client,
            bucket: "test".to_string(),
            max_age: Duration::from_secs(3600),
            num_replicas: 1,
            persist: false,
        });

        // Setup test data
        store.put("test/one.txt", Bytes::from("one")).await.unwrap();
        store.put("test/two.txt", Bytes::from("two")).await.unwrap();
        store
            .put("other/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        // Test prefix filtering
        let all_keys = store.keys().await.unwrap();
        assert_eq!(all_keys.len(), 3);

        let filtered_keys = store.keys_with_prefix("test/").await.unwrap();
        assert_eq!(filtered_keys.len(), 2);
        assert!(filtered_keys.contains(&"test/one.txt".to_string()));
        assert!(filtered_keys.contains(&"test/two.txt".to_string()));
    }

    #[tokio::test]
    async fn test_scoped_store_with_prefix() {
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let store = NatsStore1::new(NatsStoreOptions {
            client,
            bucket: "scoped".to_string(),
            max_age: Duration::from_secs(3600),
            num_replicas: 1,
            persist: false,
        });

        let scoped = store.scope("myapp");
        scoped
            .put("test/one.txt", Bytes::from("one"))
            .await
            .unwrap();
        scoped
            .put("test/two.txt", Bytes::from("two"))
            .await
            .unwrap();
        scoped
            .put("other/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        let keys = scoped.keys_with_prefix("test/").await.unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&"test/one.txt".to_string()));
        assert!(keys.contains(&"test/two.txt".to_string()));
    }
}
