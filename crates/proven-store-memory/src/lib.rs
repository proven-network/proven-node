mod error;

use error::Error;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use proven_store::Store;

#[derive(Clone, Debug, Default)]
pub struct MemoryStore {
    map: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        MemoryStore {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Store for MemoryStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        let mut map = self.map.lock().unwrap();
        map.remove(&key);
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        let map = self.map.lock().unwrap();
        Ok(map.get(&key).cloned())
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        let mut map = self.map.lock().unwrap();
        map.insert(key, bytes);
        Ok(())
    }
}
