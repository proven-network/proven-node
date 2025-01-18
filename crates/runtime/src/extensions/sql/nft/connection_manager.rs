use std::collections::HashMap;
use std::sync::Arc;

use proven_sql::{SqlStore, SqlStore1, SqlStore2};
use tokio::sync::Mutex;

#[allow(clippy::type_complexity)]
pub struct NftSqlConnectionManager<NSS: SqlStore2> {
    connections:
        Arc<Mutex<HashMap<String, <<NSS::Scoped as SqlStore1>::Scoped as SqlStore>::Connection>>>,
    migrations: HashMap<String, Vec<String>>,
    sql_store: Arc<Mutex<NSS>>,
}

impl<NSS: SqlStore2> NftSqlConnectionManager<NSS> {
    pub fn new(sql_store: NSS, migrations: HashMap<String, Vec<String>>) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            migrations,
            sql_store: Arc::new(Mutex::new(sql_store)),
        }
    }

    pub async fn connect(
        &self,
        db_name: String,
        resource_address: String,
        nft_id: String,
    ) -> Result<<<NSS::Scoped as SqlStore1>::Scoped as SqlStore>::Connection, NSS::Error> {
        let connection_key = format!("{db_name}-{resource_address}:{nft_id}");
        let mut connections = self.connections.lock().await;

        if let Some(connection) = connections.get(&connection_key) {
            return Ok(connection.clone());
        }

        let migrations = self
            .migrations
            .clone()
            .entry(db_name.clone())
            .or_default()
            .clone();

        let connection = self
            .sql_store
            .lock()
            .await
            .scope(db_name.clone())
            .scope(format!("{resource_address}:{nft_id}"))
            .connect(migrations)
            .await?;

        connections.insert(connection_key, connection.clone());

        Ok(connection)
    }
}
