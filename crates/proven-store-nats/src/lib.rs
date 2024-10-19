mod error;

pub use error::Error;

pub use async_nats::jetstream::kv::Config as NatsKeyValueConfig;
use async_trait::async_trait;
use proven_store::Store;

#[derive(Clone, Debug)]
pub struct NatsStore {
    nats_kv_store: async_nats::jetstream::kv::Store,
}

impl NatsStore {
    pub async fn new(
        client: async_nats::Client,
        config: NatsKeyValueConfig,
    ) -> Result<Self, Error> {
        let jetstream_context = async_nats::jetstream::new(client.clone());
        let nats_kv_store = jetstream_context
            .create_key_value(config)
            .await
            .map_err(|e| Error::Create(e.kind()))?;

        Ok(NatsStore { nats_kv_store })
    }
}

#[async_trait]
impl Store for NatsStore {
    type SE = Error;

    async fn del(&self, key: String) -> Result<(), Self::SE> {
        self.nats_kv_store
            .delete(key)
            .await
            .map_err(|e| Error::Delete(e.kind()))?;

        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        self.nats_kv_store
            .get(key)
            .await
            .map_err(|e| Error::Get(e.kind()))
            .map(|v| v.map(|v| v.to_vec()))
    }

    async fn put(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        self.nats_kv_store
            .put(key, bytes.into())
            .await
            .map_err(|e| Error::Put(e.kind()))?;

        Ok(())
    }

    fn del_blocking(&self, key: String) -> Result<(), Self::SE> {
        tokio::runtime::Handle::current().block_on(self.del(key))
    }

    fn get_blocking(&self, key: String) -> Result<Option<Vec<u8>>, Self::SE> {
        tokio::runtime::Handle::current().block_on(self.get(key))
    }

    fn put_blocking(&self, key: String, bytes: Vec<u8>) -> Result<(), Self::SE> {
        tokio::runtime::Handle::current().block_on(self.put(key, bytes))
    }
}
