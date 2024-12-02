//! Implementation of key-value storage using AWS S3. Values encrypted using
//! AES-256 via SSE-C.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use async_trait::async_trait;
use aws_config::Region;
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use blake3::Hasher;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::io::AsyncReadExt;

/// KV store using AWS S3.
#[derive(Clone, Debug)]
pub struct S3Store {
    bucket: String,
    client: aws_sdk_s3::Client,
    secret_key: [u8; 32],
    prefix: Option<String>,
}

/// Options for configuring an `S3Store`.
pub struct S3StoreOptions {
    /// The S3 bucket to use for the key-value store (must be created in advance currently).
    pub bucket: String,

    /// The AWS region to use.
    pub region: String,

    /// The secret key to use for AES-256 encryption.
    pub secret_key: [u8; 32],
}

impl S3Store {
    /// Creates a new `S3Store` with the specified options.
    pub async fn new(
        S3StoreOptions {
            bucket,
            region,
            secret_key,
        }: S3StoreOptions,
    ) -> Self {
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

    fn get_key<K: Into<String>>(&self, key: K) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}/{}", prefix, key.into()),
            None => key.into(),
        }
    }
}

#[async_trait]
impl Store for S3Store {
    type Error = Error;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        let key = self.get_key(key);

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

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
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
                let content_length = resp.content_length.unwrap_or(0);
                let mut buf = if content_length > 0 {
                    Vec::<u8>::with_capacity(
                        content_length
                            .try_into()
                            .map_err(|_| Error::BadContentLength(content_length))?,
                    )
                } else {
                    Vec::new()
                };
                body.read_to_end(&mut buf)
                    .await
                    .map_err(|e| Error::IoError("read body error", e))?;

                Ok(Some(Bytes::from(buf)))
            }
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
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

    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::Error> {
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

macro_rules! impl_scoped_store {
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl $name for S3Store {
            type Error = Error;
            type Scoped = Self;

            fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
                let prefix = match &self.prefix {
                    Some(prefix) => format!("{}/{}", prefix, scope.into()),
                    None => scope.into(),
                };
                S3Store {
                    bucket: self.bucket.clone(),
                    client: self.client.clone(),
                    secret_key: self.secret_key,
                    prefix: Some(prefix),
                }
            }
        }
    };
}

impl_scoped_store!(Store1, Store);
impl_scoped_store!(Store2, Store1);
impl_scoped_store!(Store3, Store2);
