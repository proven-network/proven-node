//! Implementation of key-value storage using AWS Secrets Manager. Values
//! typically double-encrypted using KMS with enclave-specific keys.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::too_many_lines)]

mod error;

pub use error::Error;

use std::collections::HashMap;
use std::convert::Infallible;
use std::convert::{TryFrom, TryInto};
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::marker::PhantomData;

use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_secretsmanager::Client;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};

/// Options for configuring an `AsmStore`.
pub struct AsmStoreOptions {
    /// The AWS region to use.
    pub region: String,

    /// The name of the secret to use.
    pub secret_name: String,
}

/// KV store using AWS Secrets Manager.
pub struct AsmStore<T = Bytes, D = Infallible, S = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    client: aws_sdk_secretsmanager::Client,
    prefix: Option<String>,
    secret_name: String,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for AsmStore<T, D, S>
where
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            secret_name: self.secret_name.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> Debug for AsmStore<T, D, S>
where
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("AsmStore")
            .field("client", &self.client)
            .field("prefix", &self.prefix)
            .field("secret_name", &self.secret_name)
            .finish()
    }
}

impl<T, D, S> AsmStore<T, D, S>
where
    Self: Clone + Debug + Send + Sync + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
{
    /// Creates a new `AsmStore` with the specified options.
    pub async fn new(
        AsmStoreOptions {
            region,
            secret_name,
        }: AsmStoreOptions,
    ) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            client: Client::new(&config),
            prefix: None,
            secret_name,
            _marker: PhantomData,
        }
    }

    const fn new_with_client_and_prefix(
        client: Client,
        secret_name: String,
        prefix: Option<String>,
    ) -> Self {
        Self {
            client,
            prefix,
            secret_name,
            _marker: PhantomData,
        }
    }

    fn get_secret_name(&self) -> String {
        self.prefix.as_ref().map_or_else(
            || self.secret_name.clone(),
            |prefix| format!("{}-{}", prefix, self.secret_name),
        )
    }

    async fn get_secret_map(&self) -> Result<HashMap<String, Bytes>, Error> {
        let resp = self
            .client
            .get_secret_value()
            .secret_id(self.get_secret_name())
            .send()
            .await;

        match resp {
            Ok(resp) => {
                let secret_string = resp.secret_string.unwrap_or_default();
                Ok(serde_json::from_str(&secret_string).unwrap_or_default())
            }
            Err(_) => Ok(HashMap::new()),
        }
    }

    async fn update_secret_map(&self, secret_map: HashMap<String, Bytes>) -> Result<(), Error> {
        let updated_secret_string = serde_json::to_string(&secret_map).unwrap();
        let update_resp = self
            .client
            .update_secret()
            .secret_id(&self.secret_name)
            .secret_string(&updated_secret_string)
            .send()
            .await;

        match update_resp {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::SecretsManager(e.into())),
        }
    }
}

#[async_trait]
impl<T, D, S> Store<T, D, S> for AsmStore<T, D, S>
where
    Self: Clone + Send + Sync + 'static,
    D: Send + StdError + Sync + 'static,
    S: Send + StdError + Sync + 'static,
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
{
    type Error = Error;

    async fn delete<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let mut secret_map = self.get_secret_map().await?;

        secret_map.remove(key.as_ref());

        self.update_secret_map(secret_map).await
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let secret_map = self.get_secret_map().await?;

        match secret_map.get(key.as_ref()) {
            Some(bytes) => {
                let value: T = bytes
                    .clone()
                    .try_into()
                    .map_err(|e: D| Error::Deserialize(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let map = self.get_secret_map().await?;

        Ok(map.keys().cloned().collect())
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        let map = self.get_secret_map().await?;

        Ok(map
            .into_iter()
            .filter(|(key, _)| key.starts_with(prefix.as_ref()))
            .map(|(key, _)| key)
            .collect())
    }

    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let mut secret_map = self.get_secret_map().await?;

        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;
        secret_map.insert(key.as_ref().to_string(), bytes);

        self.update_secret_map(secret_map).await
    }
}

macro_rules! impl_scoped_store {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< AsmStore $index >]<T = Bytes, D = Infallible, S = Infallible>
            where
            Self: Debug + Send + Sync + 'static,
            D: Send + StdError + Sync + 'static,
            S: Send + StdError + Sync + 'static,
            T: Clone
                + Debug
                + Send
                + Sync
                + TryFrom<Bytes, Error = D>
                + TryInto<Bytes, Error = S>
                + 'static,
            {
                client: aws_sdk_secretsmanager::Client,
                prefix: Option<String>,
                secret_name: String,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< AsmStore $index >]<T, D, S>
            where
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        client: self.client.clone(),
                        prefix: self.prefix.clone(),
                        secret_name: self.secret_name.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> Debug for [< AsmStore $index >]<T, D, S>
            where
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
            {
                fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
                    f.debug_struct(stringify!([< AsmStore $index >]))
                        .field("client", &self.client)
                        .field("prefix", &self.prefix)
                        .field("secret_name", &self.secret_name)
                        .finish()
                }
            }

            impl<T, D, S> [< AsmStore $index >]<T, D, S>
            where
                Self: Debug + Send + Sync + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
            {
                /// Creates a new `[< AsmStore $index >]` with the specified options.
                #[must_use]
                pub const fn new_with_client_and_prefix(
                    client: Client,
                    secret_name: String,
                    prefix: Option<String>,
                ) -> Self {
                    Self {
                        client,
                        prefix,
                        secret_name,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< AsmStore $index >]<T, D, S>
            where
                Self: Clone + Debug + Send + Sync + 'static,
                D: Send + StdError + Sync + 'static,
                S: Send + StdError + Sync + 'static,
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
            {
                type Error = Error;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> Self::Scoped
                where
                    K: AsRef<str> + Send
                {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.as_ref()),
                        None => scope.as_ref().to_string(),
                    };

                    Self::Scoped::new_with_client_and_prefix(
                        self.client.clone(),
                        self.secret_name.clone(),
                        Some(new_scope),
                    )
                }
            }
        }
    };
}

impl_scoped_store!(
    1,
    AsmStore,
    Store,
    "A single-scoped KV store using AWS Secrets Manager."
);
impl_scoped_store!(
    2,
    AsmStore1,
    Store1,
    "A double-scoped KV store using AWS Secrets Manager."
);
impl_scoped_store!(
    3,
    AsmStore2,
    Store2,
    "A triple-scoped KV store using AWS Secrets Manager."
);
