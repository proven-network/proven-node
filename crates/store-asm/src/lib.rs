//! Implementation of key-value storage using AWS Secrets Manager. Values
//! typically double-encrypted using KMS with enclave-specific keys.
#![warn(missing_docs)]
#![warn(clippy::all)]
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
pub struct AsmStore<T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Debug + Send + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    client: aws_sdk_secretsmanager::Client,
    prefix: Option<String>,
    secret_name: String,
    _marker: PhantomData<T>,
    _marker2: PhantomData<DE>,
    _marker3: PhantomData<SE>,
}

impl<T, DE, SE> Clone for AsmStore<T, DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
    T: Clone + Debug + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            prefix: self.prefix.clone(),
            secret_name: self.secret_name.clone(),
            _marker: PhantomData,
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }
}

impl<T, DE, SE> Debug for AsmStore<T, DE, SE>
where
    DE: Send + StdError + Sync + 'static,
    SE: Send + StdError + Sync + 'static,
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

impl<T, DE, SE> AsmStore<T, DE, SE>
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
            _marker2: PhantomData,
            _marker3: PhantomData,
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
            _marker2: PhantomData,
            _marker3: PhantomData,
        }
    }

    fn get_secret_name(&self) -> String {
        self.prefix.as_ref().map_or_else(
            || self.secret_name.clone(),
            |prefix| format!("{}-{}", prefix, self.secret_name),
        )
    }

    async fn get_secret_map(&self) -> Result<HashMap<String, Bytes>, Error<DE, SE>> {
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

    async fn update_secret_map(
        &self,
        secret_map: HashMap<String, Bytes>,
    ) -> Result<(), Error<DE, SE>> {
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
impl<T, DE, SE> Store<T, DE, SE> for AsmStore<T, DE, SE>
where
    Self: Clone + Send + Sync + 'static,
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
        let mut secret_map = self.get_secret_map().await?;

        secret_map.remove(&key.into());

        self.update_secret_map(secret_map).await
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
        let secret_map = self.get_secret_map().await?;

        match secret_map.get(&key.into()) {
            Some(bytes) => {
                let value: T = bytes
                    .clone()
                    .try_into()
                    .map_err(|e| Error::Deserialize(e))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let secret_map = self.get_secret_map().await?;
        Ok(secret_map.keys().cloned().collect())
    }

    async fn put<K: Into<String> + Send>(&self, key: K, value: T) -> Result<(), Self::Error> {
        let mut secret_map = self.get_secret_map().await?;

        let bytes: Bytes = value.try_into().map_err(|e| Error::Serialize(e))?;
        secret_map.insert(key.into(), bytes);

        self.update_secret_map(secret_map).await
    }
}

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! AsmStore $index]]
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
                client: aws_sdk_secretsmanager::Client,
                prefix: Option<String>,
                secret_name: String,
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
                        client: self.client.clone(),
                        prefix: self.prefix.clone(),
                        secret_name: self.secret_name.clone(),
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
                        .field("client", &self.client)
                        .field("prefix", &self.prefix)
                        .field("secret_name", &self.secret_name)
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
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                        None => scope.into(),
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
