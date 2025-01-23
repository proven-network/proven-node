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
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::marker::PhantomData;
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
            persist,
        }: NatsStoreOptions,
    ) -> Self {
        let jetstream_context = jetstream::new(client.clone());

        Self {
            bucket,
            client,
            jetstream_context,
            max_age,
            persist,
            _marker: PhantomData,
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

    async fn delete<K: Clone + Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        self.get_kv_store()
            .await?
            .delete(key.into())
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get<K: Clone + Into<String> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
        if let Some(bytes) = self
            .get_kv_store()
            .await?
            .get(key)
            .await
            .map_err(|e| Error::Entry(e.kind()))?
        {
            let value: T = bytes
                .try_into()
                .map_err(|e: D| Error::Deserialize(e.to_string()))?;

            Ok(Some(value))
        } else {
            Ok(None)
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

    async fn put<K: Clone + Into<String> + Send>(
        &self,
        key: K,
        value: T,
    ) -> Result<(), Self::Error> {
        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;
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
                        persist,
                    }: NatsStoreOptions,
                ) -> Self {
                    let jetstream_context = jetstream::new(client.clone());

                    Self {
                        bucket,
                        client,
                        jetstream_context,
                        max_age,
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
                    K: Clone + Into<String> + Send,
                {
                    let mut bucket = self.bucket.clone();
                    bucket.push_str(&format!(".{}", scope.into()));
                    Self::Scoped::new(NatsStoreOptions {
                        client: self.client.clone(),
                        bucket,
                        max_age: self.max_age,
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
