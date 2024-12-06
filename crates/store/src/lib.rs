//! Abstract interface for managing KV storage.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

use std::convert::Infallible;
use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;

/// Marker trait for store errors
pub trait StoreError: Error + Send + Sync + 'static {}

/// A trait representing a key-value store with asynchronous operations.
#[async_trait]
pub trait Store<T = Bytes, DE = Infallible, SE = Infallible>
where
    Self: Clone + Send + Sync + 'static,
    DE: std::error::Error + Send + Sync + 'static,
    SE: std::error::Error + Send + Sync + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// The error type for the store.
    type Error: StoreError;

    /// Deletes a key from the store.
    async fn del<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: Into<String> + Send;

    /// Retrieves the value associated with a key.
    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: Into<String> + Send;

    /// Retrieves all keys in the store.
    async fn keys(&self) -> Result<Vec<String>, Self::Error>;

    /// Stores a key-value pair.
    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: Into<String> + Send;
}

macro_rules! define_scoped_store {
    ($index:expr, $parent:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! Store $index]]

            #[async_trait]
            #[doc = $doc]
            pub trait #name<T = Bytes, DE = Infallible, SE = Infallible>
            where
                Self: Clone + Send + Sync + 'static,
                DE: std::error::Error + Send + Sync + 'static,
                SE: std::error::Error + Send + Sync + 'static,
                T: Clone + Send + Sync + 'static,
            {
                /// The error type for the store.
                type Error: StoreError;

                /// The scoped store type.
                type Scoped: $parent<T, DE, SE, Error = Self::Error> + Clone + Send + Sync + 'static;

                /// Creates a scoped store.
                fn [!ident! scope_ $index]<S>(&self, scope: S) -> <Self as #name<T, DE, SE>>::Scoped
                where
                    S: Into<String> + Send;
            }

        }
    };
}

define_scoped_store!(
    1,
    Store,
    "A trait representing a single-scoped key-value store with asynchronous operations."
);
define_scoped_store!(
    2,
    Store1,
    "A trait representing a double-scoped key-value store with asynchronous operations."
);
define_scoped_store!(
    3,
    Store2,
    "A trait representing a triple-scoped key-value store with asynchronous operations."
);
