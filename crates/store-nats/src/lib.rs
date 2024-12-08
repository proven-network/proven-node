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
pub struct NatsStore<T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    bucket: String,
    client: Client,
    jetstream_context: JetStreamContext,
    max_age: Duration,
    persist: bool,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> Clone for NatsStore<T, DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            client: self.client.clone(),
            jetstream_context: self.jetstream_context.clone(),
            max_age: self.max_age,
            persist: self.persist,
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<T, DE, SE> Debug for NatsStore<T, DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("NatsStore")
            .field("bucket", &self.bucket)
            .field("max_age", &self.max_age)
            .field("persist", &self.persist)
            .finish_non_exhaustive()
    }
}

impl<T, DE, SE> NatsStore<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
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
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }

    async fn get_kv_store(&self) -> Result<KvStore, Error<DE, SE>> {
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
impl<T, DE, SE> Store<T, DE, SE> for NatsStore<T, DE, SE>
where
    Self: Clone + Debug + Send + Sync + 'static,
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = DE>
        + TryInto<Bytes, Error = SE>
        + 'static,
{
    type Error = Error<DE, SE>;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        self.get_kv_store()
            .await?
            .delete(key.into())
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
        if let Some(bytes) = self
            .get_kv_store()
            .await?
            .get(key)
            .await
            .map_err(|e| Error::Entry(e.kind()))?
        {
            let value: T = bytes.try_into().map_err(|e| Error::Deserialize(e))?;

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

    async fn put<K: Into<String> + Send>(&self, key: K, value: T) -> Result<(), Self::Error> {
        let bytes: Bytes = value.try_into().map_err(|e| Error::Serialize(e))?;
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
            pub struct #name<T = Bytes, DE = Infallible, SE = Infallible>
            where
                Self: Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                bucket: String,
                client: Client,
                jetstream_context: JetStreamContext,
                max_age: Duration,
                persist: bool,
                _marker: PhantomData<T>,
                _marker2: PhantomData<DE>,
                _marker3: PhantomData<SE>,
            }

            impl<T, DE, SE> Clone for #name<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        bucket: self.bucket.clone(),
                        client: self.client.clone(),
                        jetstream_context: self.jetstream_context.clone(),
                        max_age: self.max_age,
                        persist: self.persist,
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            impl<T, DE, SE> Debug for #name<T, DE, SE>
            where
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                    f.debug_struct(stringify!(#name))
                        .field("bucket", &self.bucket)
                        .finish()
                }
            }

            impl<T, DE, SE> #name<T, DE, SE>
            where
                Self: Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
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
                    let jetstream_context = jetstream::new(client.clone());

                    Self {
                        bucket,
                        client,
                        jetstream_context,
                        max_age,
                        persist,
                        _marker: PhantomData,
                        _marker2: PhantomData,
                        _marker3: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, DE, SE> #trait_name<T, DE, SE> for #name<T, DE, SE>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                DE: Send + StdError + Sync + 'static,
                SE: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = DE>
                    + TryInto<Bytes, Error = SE>
                    + 'static,
            {
                type Error = Error<DE, SE>;
                type Scoped = $parent<T, DE, SE>;

                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
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
