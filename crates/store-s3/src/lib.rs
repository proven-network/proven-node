//! Implementation of key-value storage using AWS S3. Values encrypted using
//! AES-256 via SS-C.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use std::convert::Infallible;
use std::convert::{TryFrom, TryInto};
use std::error::Error as StdError;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::marker::PhantomData;

use async_trait::async_trait;
use aws_config::Region;
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use blake3::Hasher;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::io::AsyncReadExt;

/// Options for configuring an `S3Store`.
pub struct S3StoreOptions {
    /// The S3 bucket to use for the key-value store (must be created in advance currently).
    pub bucket: String,

    /// The optional prefix to use for all keys (may be extended through scopes).
    pub prefix: Option<String>,

    /// The AWS region to use.
    pub region: String,

    /// The secret key to use for AES-256 encryption.
    pub secret_key: [u8; 32],
}

/// KV store using AWS S3.
#[derive(Debug)]
pub struct S3Store<T = Bytes, D = Infallible, S = Infallible>
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
    client: aws_sdk_s3::Client,
    secret_key: [u8; 32],
    prefix: Option<String>,
    _marker: PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for S3Store<T, D, S>
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
            secret_key: self.secret_key,
            prefix: self.prefix.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T, D, S> S3Store<T, D, S>
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
    /// Creates a new `S3Store` with the specified options.
    pub async fn new(
        S3StoreOptions {
            bucket,
            prefix,
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
            prefix,
            _marker: PhantomData,
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
impl<T, D, S> Store<T, D, S> for S3Store<T, D, S>
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
    type Error = Error;

    async fn delete<K: Clone + Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
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

    async fn get<K: Clone + Into<String> + Send>(&self, key: K) -> Result<Option<T>, Self::Error> {
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
                    .map_err(|e| Error::Io("read body error", e))?;

                let value: T = Bytes::from(buf)
                    .try_into()
                    .map_err(|e: D| Error::Deserialize(e.to_string()))?;

                Ok(Some(value))
            }
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        if let Some(prefix) = &self.prefix {
            Ok(self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix)
                .send()
                .await
                .map_err(|e| Error::S3(e.into()))?
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|obj| obj.key)
                .filter_map(|key| key.strip_prefix(prefix).map(String::from))
                .collect())
        } else {
            Ok(self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .send()
                .await
                .map_err(|e| Error::S3(e.into()))?
                .contents
                .unwrap_or_default()
                .into_iter()
                .filter_map(|obj| obj.key)
                .collect())
        }
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: Clone + Into<String> + Send,
    {
        let prefix = prefix.into();
        let full_prefix = match &self.prefix {
            Some(store_prefix) => format!("{store_prefix}/{prefix}"),
            None => prefix,
        };

        Ok(self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(full_prefix.clone())
            .send()
            .await
            .map_err(|e| Error::S3(e.into()))?
            .contents
            .unwrap_or_default()
            .into_iter()
            .filter_map(|obj| obj.key)
            .filter_map(|key| key.strip_prefix(&full_prefix).map(String::from))
            .collect())
    }

    async fn put<K: Clone + Into<String> + Send>(
        &self,
        key: K,
        value: T,
    ) -> Result<(), Self::Error> {
        let key = self.get_key(key);
        let sse_key = self.generate_aes_key(&key);

        let bytes: Bytes = value
            .try_into()
            .map_err(|e| Error::Serialize(e.to_string()))?;

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
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            pub struct [< S3Store $index >]<T = Bytes, D = Infallible, S = Infallible>
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
                bucket: String,
                client: aws_sdk_s3::Client,
                secret_key: [u8; 32],
                prefix: Option<String>,
                _marker: PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< S3Store $index >]<T, D, S>
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
                        bucket: self.bucket.clone(),
                        client: self.client.clone(),
                        secret_key: self.secret_key,
                        prefix: self.prefix.clone(),
                        _marker: PhantomData,
                    }
                }
            }

            impl<T, D, S> Debug for [< S3Store $index >]<T, D, S>
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
                    f.debug_struct(stringify!([< S3Store $index >]))
                        .field("bucket", &self.bucket)
                        .field("prefix", &self.prefix)
                        .finish()
                }
            }

            impl<T, D, S> [< S3Store $index >]<T, D, S>
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
                /// Creates a new `[< S3Store $index >]` with the specified options.
                pub async fn new(
                    S3StoreOptions {
                        bucket,
                        prefix,
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
                        prefix,
                        _marker: PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< S3Store $index >]<T, D, S>
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
                    K: Clone + Into<String> + Send,
                {
                    let prefix = match &self.prefix {
                        Some(prefix) => format!("{}/{}", prefix, scope.into()),
                        None => scope.into(),
                    };
                    Self::Scoped {
                        bucket: self.bucket.clone(),
                        client: self.client.clone(),
                        secret_key: self.secret_key,
                        prefix: Some(prefix),
                        _marker: PhantomData,
                    }
                }
            }
        }
    };
}

impl_scoped_store!(1, S3Store, Store, "A single-scoped KV store using AWS S3.");
impl_scoped_store!(
    2,
    S3Store1,
    Store1,
    "A double-scoped KV store using AWS S3."
);
impl_scoped_store!(
    3,
    S3Store2,
    Store2,
    "A triple-scoped KV store using AWS S3."
);
