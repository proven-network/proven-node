mod error;

pub use error::Error;

use std::collections::HashMap;

use async_trait::async_trait;
use aws_config::Region;
use proven_store::Store;

#[derive(Debug)]
pub struct AsmStore {
    client: aws_sdk_secretsmanager::Client,
    secret_id: String,
}

impl AsmStore {
    pub async fn new(region: String, secret_id: String) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            client: aws_sdk_secretsmanager::Client::new(&config),
            secret_id,
        }
    }

    async fn get_secret_map(&self) -> Result<HashMap<String, Vec<u8>>, Error> {
        let resp = self
            .client
            .get_secret_value()
            .secret_id(&self.secret_id)
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
            .secret_id(&self.secret_id)
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
