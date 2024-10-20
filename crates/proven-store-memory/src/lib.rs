mod error;

use error::Error;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proven_store::{Store, Store1, Store2};

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    map: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    prefix: Option<String>,
}

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
impl Store for MemoryStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let mut map = self.map.lock().unwrap();
        map.remove(&self.get_key(key));
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        let map = self.map.lock().unwrap();
        Ok(map.get(&self.get_key(key)).cloned())
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        let mut map = self.map.lock().unwrap();
        map.insert(self.get_key(key), bytes);
        Ok(())
    }
}
