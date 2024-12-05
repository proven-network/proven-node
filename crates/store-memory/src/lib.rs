//! In-memory (single node) implementation of key-value storage for local
//! development.
#![warn(missing_docs)]
#![warn(clippy::all)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]

mod error;

pub use error::Error;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::sync::Mutex;

/// In-memory key-value store.
#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    map: Arc<Mutex<HashMap<String, Bytes>>>,
    prefix: Option<String>,
}

impl MemoryStore {
    /// Creates a new `MemoryStore`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            prefix: None,
        }
    }

    fn with_scope(prefix: String) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            prefix: Some(prefix),
        }
    }

    fn get_key<K: Into<String>>(&self, key: K) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}:{}", prefix, key.into()),
            None => key.into(),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    type Error = Error;

    async fn del<K: Into<String> + Send>(&self, key: K) -> Result<(), Self::Error> {
        self.map.lock().await.remove(&self.get_key(key));
        Ok(())
    }

    async fn get<K: Into<String> + Send>(&self, key: K) -> Result<Option<Bytes>, Self::Error> {
        let map = self.map.lock().await;
        Ok(map.get(&self.get_key(key)).cloned())
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        let map = self.map.lock().await;
        Ok(map
            .keys()
            .filter(|&key| {
                self.prefix
                    .as_ref()
                    .map_or(true, |prefix| key.starts_with(prefix))
            })
            .cloned()
            .collect())
    }

    async fn put<K: Into<String> + Send>(&self, key: K, bytes: Bytes) -> Result<(), Self::Error> {
        self.map.lock().await.insert(self.get_key(key), bytes);
        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr, $parent:ident, $parent_trait:ident, $doc:expr) => {
        preinterpret::preinterpret! {
            [!set! #name = [!ident! MemoryStore $index]]
            [!set! #trait_name = [!ident! Store $index]]

            #[doc = $doc]
            #[derive(Clone, Debug, Default)]
            pub struct #name {
                prefix: Option<String>,
            }

            impl #name {
                /// Creates a new `#name`.
                #[must_use]
                pub const fn new() -> Self {
                    Self {
                        prefix: None,
                    }
                }

                #[allow(dead_code)]
                const fn with_scope(prefix: String) -> Self {
                    Self {
                        prefix: Some(prefix),
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
                    $parent::with_scope(new_scope)
                }
            }
        }
    };
}

impl_scoped_store!(1, MemoryStore, Store, "A single-scoped in-memory KV store.");
impl_scoped_store!(
    2,
    MemoryStore1,
    Store1,
    "A double-scoped in-memory KV store."
);
impl_scoped_store!(
    3,
    MemoryStore2,
    Store2,
    "A triple-scoped in-memory KV store."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let store = MemoryStore::new();
        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        store.put(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_del() {
        let store = MemoryStore::new();
        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        store.put(key.clone(), value.clone()).await.unwrap();
        store.del(key.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_scope() {
        let store = MemoryStore1::new();
        let scoped_store = store.scope_1("scope");

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_nested_scope() {
        let store = MemoryStore2::new();
        let partial_scoped_store = store.scope_2("scope1");
        let scoped_store = partial_scoped_store.scope_1("scope2");

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }
}
