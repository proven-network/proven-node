mod error;

pub use error::Error;

use async_trait::async_trait;
use aws_config::Region;
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use blake3::Hasher;
use proven_store::{Store, Store1, Store2};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
pub struct S3Store {
    bucket: String,
    client: aws_sdk_s3::Client,
    secret_key: [u8; 32],
    prefix: Option<String>,
}

/// S3Store is an Amazon S3 implementation of the `Store`, `Store1`, and `Store2` traits.
/// It uses Amazon S3 to store key-value pairs, where keys are strings and values are byte vectors.
/// The store supports optional scoping of keys using a prefix.
/// The store uses AES-256 encryption with a secret key to encrypt values before storing them.
impl S3Store {
    pub async fn new(bucket: String, region: String, secret_key: [u8; 32]) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            bucket,
            client: aws_sdk_s3::Client::new(&config),
            secret_key,
            prefix: None,
        }
    }

    fn generate_aes_key(&self, key: &String) -> [u8; 32] {
        let mut hasher = Hasher::new_keyed(&self.secret_key);
        hasher.update(key.as_bytes());
        *hasher.finalize().as_bytes()
    }

    fn get_key(&self, key: String) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key),
            None => key,
        }
    }
}

#[async_trait]
impl Store1 for S3Store {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let prefix = match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, scope),
            None => scope,
        };
        S3Store {
            bucket: self.bucket.clone(),
            client: self.client.clone(),
            secret_key: self.secret_key,
            prefix: Some(prefix),
        }
    }
}

#[async_trait]
impl Store2 for S3Store {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let prefix = match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, scope),
            None => scope,
        };
        S3Store {
            bucket: self.bucket.clone(),
            client: self.client.clone(),
            secret_key: self.secret_key,
            prefix: Some(prefix),
        }
    }
}

#[async_trait]
impl Store for S3Store {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let resp = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await;

        match resp {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::S3(e.into())),
        }
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        let key = self.get_key(key);
        let sse_key = self.generate_aes_key(&key);

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .sse_customer_algorithm("AES256")
            .sse_customer_key(base64.encode(sse_key))
            .sse_customer_key_md5(base64.encode(md5::compute(sse_key).as_slice()))
            .send()
            .await;

        match resp {
            Err(e) => {
                if e.to_string().contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(Error::S3(e.into()))
                }
            }
            Ok(resp) => {
                let mut body = resp.body.into_async_read();
                let mut buf = Vec::<u8>::new();
                body.read_to_end(&mut buf).await?;

                Ok(Some(buf))
            }
        }
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        let key = self.get_key(key);
        let sse_key = self.generate_aes_key(&key);

        let resp = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(bytes.into())
            .sse_customer_algorithm("AES256")
            .sse_customer_key(base64.encode(sse_key))
            .sse_customer_key_md5(base64.encode(md5::compute(sse_key).as_slice()))
            .send()
            .await;

        match resp {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::S3(e.into())),
        }
    }
}
