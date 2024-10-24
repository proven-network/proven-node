mod error;

pub use error::Error;

use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::Region;
use aws_sdk_secretsmanager::Client;
use proven_store::{Store, Store1, Store2, Store3};

#[derive(Clone, Debug)]
pub struct AsmStore {
    client: aws_sdk_secretsmanager::Client,
    prefix: Option<String>,
    secret_name: String,
}

/// AsmStore is an AWS Secrets Manager implementation of the `Store`, `Store2`, and `Store3` traits.
/// It uses AWS Secrets Manager to store key-value pairs, where keys are strings and values are byte vectors.
/// The store supports optional scoping of keys using prefixes in the secret name.
impl AsmStore {
    pub async fn new(region: String, secret_name: String) -> Self {
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

    fn new_with_client_and_prefix(
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
        match &self.prefix {
            Some(prefix) => format!("{}-{}", prefix, self.secret_name),
            None => self.secret_name.clone(),
        }
    }

    async fn get_secret_map(&self) -> Result<HashMap<String, Vec<u8>>, Error> {
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

    async fn update_secret_map(&self, secret_map: HashMap<String, Vec<u8>>) -> Result<(), Error> {
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
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let mut secret_map = self.get_secret_map().await?;

        secret_map.remove(&key);

        self.update_secret_map(secret_map).await
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        let secret_map = self.get_secret_map().await?;

        Ok(secret_map.get(&key).cloned())
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<(), Self::SE> {
        let mut secret_map = self.get_secret_map().await?;

        secret_map.insert(key, value);

        self.update_secret_map(secret_map).await
    }
}

#[async_trait]
impl Store1 for AsmStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };

        Self::new_with_client_and_prefix(
            self.client.clone(),
            self.secret_name.clone(),
            Some(new_scope),
        )
    }
}

#[async_trait]
impl Store2 for AsmStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };

        Self::new_with_client_and_prefix(
            self.client.clone(),
            self.secret_name.clone(),
            Some(new_scope),
        )
    }
}

#[async_trait]
impl Store3 for AsmStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };

        Self::new_with_client_and_prefix(
            self.client.clone(),
            self.secret_name.clone(),
            Some(new_scope),
        )
    }
}
