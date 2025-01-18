use std::collections::HashMap;
use std::sync::Arc;

use proven_sql::{SqlStore, SqlStore1};
use tokio::sync::Mutex;

pub struct PersonalSqlConnectionManager<PSS: SqlStore1> {
    connections: Arc<Mutex<HashMap<String, <PSS::Scoped as SqlStore>::Connection>>>,
    migrations: HashMap<String, Vec<String>>,
    sql_store: Arc<Mutex<PSS>>,
}

impl<PSS: SqlStore1> PersonalSqlConnectionManager<PSS> {
    pub fn new(sql_store: PSS, migrations: HashMap<String, Vec<String>>) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            migrations,
            sql_store: Arc::new(Mutex::new(sql_store)),
        }
    }

    pub async fn connect(
        &self,
        db_name: String,
    ) -> Result<<PSS::Scoped as SqlStore>::Connection, PSS::Error> {
        let mut connections = self.connections.lock().await;

        if let Some(connection) = connections.get(&db_name) {
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
            .connect(migrations)
            .await?;

        connections.insert(db_name, connection.clone());

        Ok(connection)
    }
}
