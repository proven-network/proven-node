mod database;
mod error;

pub use database::Database;
pub use error::{Error, Result};

use proven_store::{Store, Store1};
use proven_stream::{Stream, Stream1};

#[derive(Clone)]
pub struct SqlManagerOptions<LS: Store1, SS: Stream1<Error>> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream_subscriber: SS,
}

pub struct SqlManager<LS: Store1, SS: Stream1<Error>> {
    leader_store: LS,
    local_name: String,
    stream_subscriber: SS,
}

impl<LS: Store1, SS: Stream1<Error>> SqlManager<LS, SS> {
    pub fn new(
        SqlManagerOptions {
            leader_store,
            local_name,
            stream_subscriber,
        }: SqlManagerOptions<LS, SS>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream_subscriber,
        }
    }

    pub async fn connect(&self, application_id: String, db_name: String) -> SS::Scoped {
        let scoped_leader_store = self.leader_store.scope(application_id.clone());
        let scoped_stream_subscriber = self.stream_subscriber.scope(application_id.clone());
        let current_leader = scoped_leader_store.get(db_name.clone()).await.unwrap();

        // If no current_leader, then we will try become the leader
        // If we are the leader, then we extend the lease
        if current_leader.is_none()
            || self.local_name == String::from_utf8(current_leader.clone().unwrap()).unwrap()
        {
            scoped_leader_store
                .put(db_name.clone(), self.local_name.clone().into_bytes())
                .await
                .unwrap();
        }

        let database = Database::connect().await;

        tokio::spawn({
            let database = database.clone();
            let scoped_stream_subscriber = scoped_stream_subscriber.clone();
            let db_name = db_name.clone();

            async move {
                scoped_stream_subscriber
                    .handle(db_name, move |bytes: Vec<u8>| {
                        let database = database.clone();
                        Box::pin(async move {
                            let sql = String::from_utf8(bytes)?;
                            println!("SQL: {:?}", sql);
                            let response = database.execute(&sql, ()).await?;
                            println!("Response: {:?}", response);
                            Ok(response.to_le_bytes().to_vec())
                        })
                    })
                    .await
                    .unwrap();
            }
        });

        scoped_stream_subscriber
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_store_memory::MemoryStore;
    use proven_stream_nats::ScopeMethod;
    use proven_stream_nats::{NatsStream, NatsStreamOptions};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_manager() {
        let result = timeout(Duration::from_secs(5), async {
            let client = async_nats::connect("nats://localhost:4222").await.unwrap();

            let leader_store = MemoryStore::new();

            let stream_subscriber = NatsStream::new(NatsStreamOptions {
                client: client.clone(),
                local_name: "my-machine".to_string(),
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "sql".to_string(),
            });

            let sql_manager = SqlManager::new(SqlManagerOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream_subscriber,
            });

            let scoped_stream_publisher = sql_manager
                .connect("test".to_string(), "test".to_string())
                .await;

            tokio::time::sleep(Duration::from_secs(1)).await;

            let response = scoped_stream_publisher
                .request(
                    "test".to_string(),
                    "CREATE TABLE IF NOT EXISTS users (email TEXT)"
                        .to_string()
                        .into_bytes(),
                )
                .await
                .unwrap();

            println!("ttttResponse: {:?}", response);
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
