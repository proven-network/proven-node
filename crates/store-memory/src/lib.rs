mod error;

use error::Error;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};
use tokio::sync::Mutex;

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    map: Arc<Mutex<HashMap<String, Bytes>>>,
    prefix: Option<String>,
}

/// MemoryStore is an in-memory implementation of the `Store`, `Store2`, and `Store3` traits.
/// It uses a `HashMap` protected by a `Mutex` to store key-value pairs, where keys are strings
/// and values are byte vectors. The store supports optional scoping of keys using a prefix.
impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            map: Arc::new(Mutex::new(HashMap::new())),
            prefix: None,
        }
    }

    fn with_scope(prefix: String) -> Self {
        MemoryStore {
            map: Arc::new(Mutex::new(HashMap::new())),
            prefix: Some(prefix),
        }
    }

    fn get_key(&self, key: String) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}:{}", prefix, key),
            None => key,
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let mut map = self.map.lock().await;
        map.remove(&self.get_key(key));
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Bytes>, Self::SE> {
        let map = self.map.lock().await;
        Ok(map.get(&self.get_key(key)).cloned())
    }

    async fn keys(&self) -> Result<Vec<String>, Self::SE> {
        let map = self.map.lock().await;
        Ok(map
            .keys()
            .filter(|&key| {
                if let Some(prefix) = &self.prefix {
                    key.starts_with(prefix)
                } else {
                    true
                }
            })
            .cloned()
            .collect())
    }

    async fn put(&self, key: String, bytes: Bytes) -> Result<(), Self::SE> {
        let mut map = self.map.lock().await;
        map.insert(self.get_key(key), bytes);
        Ok(())
    }
}

#[async_trait]
impl Store1 for MemoryStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };
        Self::with_scope(new_scope)
    }
}

#[async_trait]
impl Store2 for MemoryStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };
        Self::with_scope(new_scope)
    }
}

#[async_trait]
impl Store3 for MemoryStore {
    type SE = Error;
    type Scoped = Self;

    fn scope(&self, scope: String) -> Self::Scoped {
        let new_scope = match &self.prefix {
            Some(existing_scope) => format!("{}:{}", existing_scope, scope),
            None => scope,
        };
        Self::with_scope(new_scope)
    }
}

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
