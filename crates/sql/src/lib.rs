mod database;
mod error;

pub use database::Database;
pub use error::{Error, Result};

use proven_store::{Store, Store1};
use proven_stream::StreamPublisher1;
use proven_stream::{StreamSubscriber, StreamSubscriber1};

#[derive(Clone)]
pub struct SqlManagerOptions<LS: Store1, SP: StreamPublisher1, SS: StreamSubscriber1<Error>> {
    pub leader_store: LS,
    pub local_name: String,
    pub stream_publisher: SP,
    pub stream_subscriber: SS,
}

pub struct SqlManager<LS: Store1, SP: StreamPublisher1, SS: StreamSubscriber1<Error>> {
    leader_store: LS,
    local_name: String,
    stream_publisher: SP,
    stream_subscriber: SS,
}

impl<LS: Store1, SP: StreamPublisher1, SS: StreamSubscriber1<Error>> SqlManager<LS, SP, SS> {
    pub fn new(
        SqlManagerOptions {
            leader_store,
            local_name,
            stream_publisher,
            stream_subscriber,
        }: SqlManagerOptions<LS, SP, SS>,
    ) -> Self {
        Self {
            leader_store,
            local_name,
            stream_publisher,
            stream_subscriber,
        }
    }

    pub async fn connect(&self, application_id: String, db_name: String) -> SP::Scoped {
        let scoped_leader_store = self.leader_store.scope(application_id.clone());
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
            let stream_subscriber = self.stream_subscriber.clone();
            let application_id = application_id.clone();
            let db_name = db_name.clone();

            async move {
                stream_subscriber
                    .scope(application_id)
                    .subscribe(db_name, move |bytes: Vec<u8>| {
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

        self.stream_publisher.scope(application_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proven_store_memory::MemoryStore;
    use proven_stream::StreamPublisher;
    use proven_stream_nats::ScopeMethod;
    use proven_stream_nats::{NatsStreamPublisher, NatsStreamPublisherOptions};
    use proven_stream_nats::{NatsStreamSubscriber, NatsStreamSubscriberOptions};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_sql_manager() {
        let result = timeout(Duration::from_secs(5), async {
            let client = async_nats::connect("nats://localhost:4222").await.unwrap();

            let leader_store = MemoryStore::new();

            let stream_publisher = NatsStreamPublisher::new(NatsStreamPublisherOptions {
                client: client.clone(),
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "sql".to_string(),
            });

            let stream_subscriber = NatsStreamSubscriber::new(NatsStreamSubscriberOptions {
                client: client.clone(),
                scope_method: ScopeMethod::StreamPostfix,
                stream_name: "sql".to_string(),
            });

            let sql_manager = SqlManager::new(SqlManagerOptions {
                leader_store,
                local_name: "my-machine".to_string(),
                stream_publisher,
                stream_subscriber,
            });

            let scoped_stream_publisher = sql_manager
                .connect("test".to_string(), "test".to_string())
                .await;

            let response = scoped_stream_publisher
                .request("test".to_string(), "SELECT 1".to_string().into_bytes())
                .await
                .unwrap();

            // try to print the response as ascii
            let response_ascii = String::from_utf8(response.clone()).unwrap();
            println!("Response: {:?}", response_ascii);

            assert_eq!(response, 1u64.to_le_bytes().to_vec());
        })
        .await;

        assert!(result.is_ok(), "Test timed out");
    }
}
