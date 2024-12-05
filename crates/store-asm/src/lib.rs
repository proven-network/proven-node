//! Implementation of key-value storage using AWS Secrets Manager. Values
//! typically double-encrypted using KMS with enclave-specific keys.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_secretsmanager::Client;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};

/// KV store using AWS Secrets Manager.
#[derive(Clone, Debug)]
pub struct AsmStore {
    client: aws_sdk_secretsmanager::Client,
    prefix: Option<String>,
    secret_name: String,
}

/// Options for configuring an `AsmStore`.
pub struct AsmStoreOptions {
    /// The AWS region to use.
    pub region: String,

    /// The name of the secret to use.
    pub secret_name: String,
}

impl AsmStore {
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
impl Store for AsmStore {
    type Error = Error;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        let mut secret_map = self.get_secret_map().await?;

        secret_map.remove(&key.into());

        self.update_secret_map(secret_map).await
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
        let secret_map = self.get_secret_map().await?;

        Ok(secret_map.get(&key.into()).cloned())
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let secret_map = self.get_secret_map().await?;
        Ok(secret_map.keys().cloned().collect())
    }

    async fn put<K: Into<String> + Send>(&self, key: K, value: Bytes) -> Result<(), Self::Error> {
        let mut secret_map = self.get_secret_map().await?;

        secret_map.insert(key.into(), value);

        self.update_secret_map(secret_map).await
    }
}

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! AsmStore $index]]
            [!set! #trait_name = [!ident! Store $index]]

            #[doc = $doc]
            #[derive(Clone, Debug)]
            pub struct #name {
                client: aws_sdk_secretsmanager::Client,
                prefix: Option<String>,
                secret_name: String,
            }

            impl #name {
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
                    }
                }
            }

            #[async_trait]
            impl #trait_name for #name {
                type Error = Error;
                type Scoped = $parent;

                fn [!ident! scope_ $index]<S: Into<String> + Send>(&self, scope: S) -> $parent {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                        None => scope.into(),
                    };

                    $parent::new_with_client_and_prefix(
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
