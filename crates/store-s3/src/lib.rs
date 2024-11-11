mod error;

pub use error::Error;

use async_trait::async_trait;
use aws_config::Region;
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use blake3::Hasher;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
pub struct S3Store {
    bucket: String,
    client: aws_sdk_s3::Client,
    secret_key: [u8; 32],
    prefix: Option<String>,
}

/// S3Store is an Amazon S3 implementation of the `Store`, `Store2`, and `Store3` traits.
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

    async fn get(&self, key: String) -> Result<Option<Bytes>, Self::SE> {
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
                let mut buf = Vec::<u8>::with_capacity(resp.content_length.unwrap_or(0) as usize);
                body.read_to_end(&mut buf).await?;

                Ok(Some(Bytes::from(buf)))
            }
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::SE> {
        if let Some(prefix) = &self.prefix {
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .send()
                .await;

            match resp {
                Ok(resp) => {
                    let keys = resp
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|obj| obj.key)
                        .filter_map(|key| key.strip_prefix(prefix).map(String::from))
                        .collect();
                    Ok(keys)
                }
                Err(e) => Err(Error::S3(e.into())),
            }
        } else {
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .send()
                .await;

            match resp {
                Ok(resp) => {
                    let keys = resp
                        .contents
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|obj| obj.key)
                        .collect();
                    Ok(keys)
                }
                Err(e) => Err(Error::S3(e.into())),
            }
        }
    }

    async fn put(&self, key: String, bytes: Bytes) -> Result<(), Self::SE> {
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
impl Store3 for S3Store {
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
