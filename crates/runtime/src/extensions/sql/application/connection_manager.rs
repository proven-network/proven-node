use std::collections::HashMap;
use std::sync::Arc;

use proven_sql::{SqlStore, SqlStore1};
use tokio::sync::Mutex;

pub struct ApplicationSqlConnectionManager<ASS: SqlStore1> {
    connections: Arc<Mutex<HashMap<String, <ASS::Scoped as SqlStore>::Connection>>>,
    migrations: HashMap<String, Vec<String>>,
    sql_store: Arc<Mutex<ASS>>,
}

impl<ASS: SqlStore1> ApplicationSqlConnectionManager<ASS> {
    pub fn new(sql_store: ASS, migrations: HashMap<String, Vec<String>>) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            migrations,
            sql_store: Arc::new(Mutex::new(sql_store)),
        }
    }

    pub async fn connect(
        &self,
        db_name: String,
    ) -> Result<<ASS::Scoped as SqlStore>::Connection, ASS::Error> {
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
            .scope_1(db_name.clone())
            .connect(migrations)
            .await?;

        connections.insert(db_name, connection.clone());

        Ok(connection)
    }
}
