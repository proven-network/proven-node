//! In-memory (single node) implementation of key-value storage for local
//! development.
#![warn(missing_docs)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![allow(clippy::type_complexity)]
#![allow(clippy::option_if_let_else)]

mod error;

pub use error::Error;

use std::convert::Infallible;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::{collections::HashMap, fmt::Debug};

use async_trait::async_trait;
use bytes::Bytes;
use proven_store::{Store, Store1, Store2, Store3};

static GLOBAL_MAPS: OnceLock<Mutex<HashMap<String, Arc<Mutex<HashMap<String, Bytes>>>>>> =
    OnceLock::new();

/// In-memory key-value store.
#[derive(Debug, Default)]
pub struct MemoryStore<T = Bytes, D = Infallible, S = Infallible>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    map: Arc<Mutex<HashMap<String, Bytes>>>,
    prefix: Option<String>,
    _marker: std::marker::PhantomData<(T, D, S)>,
}

impl<T, D, S> Clone for MemoryStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            prefix: self.prefix.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, D, S> MemoryStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    /// Creates a new `MemoryStore`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            prefix: None,
            _marker: std::marker::PhantomData,
        }
    }

    fn with_scope(prefix: String) -> Self {
        // Removed async
        let global_maps = GLOBAL_MAPS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut global_maps = global_maps.lock().unwrap();

        let map = if let Some(existing_map) = global_maps.get(&prefix) {
            existing_map.clone()
        } else {
            let new_map = Arc::new(Mutex::new(HashMap::new()));
            global_maps.insert(prefix.clone(), new_map.clone());
            new_map
        };
        drop(global_maps);

        Self {
            map,
            prefix: Some(prefix),
            _marker: std::marker::PhantomData,
        }
    }

    fn get_key<K: AsRef<str>>(&self, key: K) -> String {
        match &self.prefix {
            Some(prefix) => format!("{}:{}", prefix, key.as_ref()),
            None => key.as_ref().to_string(),
        }
    }
}

#[async_trait]
impl<T, D, S> Store<T, D, S> for MemoryStore<T, D, S>
where
    T: Clone
        + Debug
        + Send
        + Sync
        + TryFrom<Bytes, Error = D>
        + TryInto<Bytes, Error = S>
        + 'static,
    D: Debug + Send + StdError + Sync + 'static,
    S: Debug + Send + StdError + Sync + 'static,
{
    type Error = Error;

    async fn delete<K>(&self, key: K) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.map.lock().unwrap().remove(&self.get_key(key));
        Ok(())
    }

    async fn get<K>(&self, key: K) -> Result<Option<T>, Self::Error>
    where
        K: AsRef<str> + Send,
    {
        let map = self.map.lock().unwrap();

        match map.get(&self.get_key(key)).cloned() {
            Some(value) => {
                let value = T::try_from(value).map_err(|e| Error::Deserialize(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    async fn keys(&self) -> Result<Vec<String>, Self::Error> {
        self.keys_with_prefix(String::new()).await
    }

    async fn keys_with_prefix<P>(&self, prefix: P) -> Result<Vec<String>, Self::Error>
    where
        P: AsRef<str> + Send,
    {
        let prefix = prefix.as_ref();

        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            // First filter by store's global prefix if it exists
            .filter(|&key| {
                self.prefix
                    .as_ref()
                    .map_or(true, |store_prefix| key.starts_with(store_prefix))
            })
            .filter_map(|key| {
                // Strip the store prefix if it exists
                let key = if let Some(store_prefix) = &self.prefix {
                    key.strip_prefix(store_prefix)?
                        .strip_prefix(':')?
                        .to_string()
                } else {
                    key.to_string()
                };

                // Apply the optional parameter prefix
                if key.starts_with(prefix) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect())
    }

    async fn put<K>(&self, key: K, value: T) -> Result<(), Self::Error>
    where
        K: AsRef<str> + Send,
    {
        self.map.lock().unwrap().insert(
            self.get_key(key),
            value
                .try_into()
                .map_err(|e| Error::Deserialize(e.to_string()))?,
        );

        Ok(())
    }
}

macro_rules! impl_scoped_store {
    ($index:expr_2021, $parent:ident, $parent_trait:ident, $doc:expr_2021) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Debug, Default)]
            pub struct [< MemoryStore $index >]<T = Bytes, D = Infallible, S = Infallible>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                prefix: Option<String>,
                _marker: std::marker::PhantomData<(T, D, S)>,
            }

            impl<T, D, S> Clone for [< MemoryStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                fn clone(&self) -> Self {
                    Self {
                        prefix: self.prefix.clone(),
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            impl<T, D, S> [< MemoryStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                /// Creates a new `[< MemoryStore $index >]`.
                #[must_use]
                pub const fn new() -> Self {
                    Self {
                        prefix: None,
                        _marker: std::marker::PhantomData,
                    }
                }

                #[allow(dead_code)]
                const fn with_scope(prefix: String) -> Self {
                    Self {
                        prefix: Some(prefix),
                        _marker: std::marker::PhantomData,
                    }
                }
            }

            #[async_trait]
            impl<T, D, S> [< Store $index >]<T, D, S> for [< MemoryStore $index >]<T, D, S>
            where
                T: Clone
                    + Debug
                    + Send
                    + Sync
                    + TryFrom<Bytes, Error = D>
                    + TryInto<Bytes, Error = S>
                    + 'static,
                D: Debug + Send + StdError + Sync + 'static,
                S: Debug + Send + StdError + Sync + 'static,
            {
                type Error = Error;
                type Scoped = $parent<T, D, S>;

                fn scope<K>(&self, scope: K) -> $parent<T, D, S>
                where
                    K: AsRef<str> + Send,
                {
                    let new_scope = match &self.prefix {
                        Some(existing_scope) => format!("{}:{}", existing_scope, scope.as_ref()),
                        None => scope.as_ref().to_string(),
                    };
                    $parent::<T, D, S>::with_scope(new_scope)
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
        store.delete(key.clone()).await.unwrap();
        let result = store.get(key).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_scope() {
        let store = MemoryStore1::new();
        let scoped_store = store.scope("scope");
        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();

        let store = MemoryStore1::new();
        let scoped_store = store.scope("scope");
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_nested_scope() {
        let store = MemoryStore2::new();
        let partial_scoped_store = store.scope("scope1");
        let scoped_store = partial_scoped_store.scope("scope2");

        let key = "test_key".to_string();
        let value = Bytes::from_static(b"test_value");

        scoped_store.put(key.clone(), value.clone()).await.unwrap();
        let result = scoped_store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[derive(Clone, Debug, PartialEq)]
    struct CustomType(i32);

    #[allow(clippy::infallible_try_from)]
    impl TryFrom<Bytes> for CustomType {
        type Error = Infallible;

        fn try_from(value: Bytes) -> Result<Self, Self::Error> {
            Ok(Self(i32::from_be_bytes(value.as_ref().try_into().unwrap())))
        }
    }

    impl TryInto<Bytes> for CustomType {
        type Error = Infallible;

        fn try_into(self) -> Result<Bytes, Self::Error> {
            Ok(Bytes::from(self.0.to_be_bytes().to_vec()))
        }
    }

    #[tokio::test]
    async fn test_non_bytes() {
        let store: MemoryStore<CustomType> = MemoryStore::new();
        let key = "test_key".to_string();
        let value = CustomType(42);

        store.put(key.clone(), value.clone()).await.unwrap();
        let result = store.get(key.clone()).await.unwrap();

        assert_eq!(result, Some(value));
    }

    #[tokio::test]
    async fn test_keys_with_store_prefix() {
        let store = MemoryStore1::new();
        let scoped = store.scope("test");

        scoped.put("one.txt", Bytes::from("one")).await.unwrap();
        scoped.put("two.txt", Bytes::from("two")).await.unwrap();
        scoped
            .put("sub/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        let keys = scoped.keys().await.unwrap();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"one.txt".to_string()));
        assert!(keys.contains(&"two.txt".to_string()));
        assert!(keys.contains(&"sub/three.txt".to_string()));
    }

    #[tokio::test]
    async fn test_keys_with_param_prefix() {
        let store = MemoryStore1::new();
        let scoped = store.scope("test");

        scoped.put("one.txt", Bytes::from("one")).await.unwrap();
        scoped.put("two.txt", Bytes::from("two")).await.unwrap();
        scoped
            .put("sub/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        let keys = scoped.keys_with_prefix("sub/").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&"sub/three.txt".to_string()));
    }

    #[tokio::test]
    async fn test_keys_with_both_prefixes() {
        let store = MemoryStore2::new();
        let scoped1 = store.scope("test1");
        let scoped2 = scoped1.scope("test2");

        scoped2.put("one.txt", Bytes::from("one")).await.unwrap();
        scoped2.put("two.txt", Bytes::from("two")).await.unwrap();
        scoped2
            .put("sub/three.txt", Bytes::from("three"))
            .await
            .unwrap();

        let keys = scoped2.keys_with_prefix("sub/").await.unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&"sub/three.txt".to_string()));
    }
}
