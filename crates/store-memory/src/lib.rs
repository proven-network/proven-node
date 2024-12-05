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
    ($name:ident, $parent:ident) => {
        #[async_trait]
        impl $name for MemoryStore {
            type Scoped = Self;

            fn scope<S: Into<String> + Send>(&self, scope: S) -> Self::Scoped {
                let new_scope = match &self.prefix {
                    Some(existing_scope) => format!("{}:{}", existing_scope, scope.into()),
                    None => scope.into(),
                };
                Self::with_scope(new_scope)
            }
        }
    };
}

impl_scoped_store!(Store1, Store);
impl_scoped_store!(Store2, Store1);
impl_scoped_store!(Store3, Store2);

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
        let store = MemoryStore::new();
        let scoped_store = Store2::scope(&store, "scope".to_string());

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));

        // Ensure the value is not accessible without the scope
        let result_without_scope = store.get(key).await.unwrap();
        assert_eq!(result_without_scope, None);
    }

    #[tokio::test]
    async fn test_nested_scope() {
        let store = MemoryStore::new();
        let partial_scoped_store = Store2::scope(&store, "scope1".to_string());
        let scoped_store = Store3::scope(&partial_scoped_store, "scope2".to_string());

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));

        // Ensure the value is not accessible without the nested scope
        let result_without_scope = store.get(key.clone()).await.unwrap();
        assert_eq!(result_without_scope, None);

        let result_with_partial_scope = partial_scoped_store.get(key).await.unwrap();
        assert_eq!(result_with_partial_scope, None);
    }
}
