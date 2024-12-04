use proven_sql_streamed::{SqlStreamHandler, StreamedSqlStore, StreamedSqlStoreOptions};

use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store_memory::MemoryStore;
use proven_stream_nats::{NatsStream, NatsStreamOptions};
use tokio::time::{timeout, Duration};

async fn setup(stream_name: &str) -> StreamedSqlStore<NatsStream<SqlStreamHandler>, MemoryStore> {
    let client = async_nats::connect("localhost:4222").await.unwrap();
    let jetstream_context = async_nats::jetstream::new(client.clone());

    let _ = jetstream_context
        .delete_stream(format!("{}_request", stream_name).to_ascii_uppercase())
        .await;

    let _ = jetstream_context
        .delete_stream(format!("{}_reply", stream_name).to_ascii_uppercase())
        .await;

    let leader_store = MemoryStore::new();
    let stream = NatsStream::new(NatsStreamOptions {
        client,
        stream_name: stream_name.to_string(),
    });

    StreamedSqlStore::new(StreamedSqlStoreOptions {
        leader_store,
        local_name: "my-machine".to_string(),
        stream,
    })
}

#[tokio::test]
async fn test_nats_sql_store() {
    let result = timeout(Duration::from_secs(5), async {
        let sql_store = setup("test_nats_sql_store").await;

        let connection = sql_store
            .connect(vec![
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
            ])
            .await
            .unwrap();

        let response = connection
            .execute(
                "INSERT INTO users (id, email) VALUES (?1, ?2)".to_string(),
                vec![
                    SqlParam::Integer(1),
                    SqlParam::Text("alice@example.com".to_string()),
                ],
            )
            .await
            .unwrap();

        assert_eq!(response, 1);

        let response = connection
            .query("SELECT id, email FROM users".to_string(), vec![])
            .await
            .unwrap();

        assert_eq!(response.column_count, 2);
        assert_eq!(
            response.column_names,
            vec!["id".to_string(), "email".to_string()]
        );
        assert_eq!(
            response.column_types,
            vec!["INTEGER".to_string(), "TEXT".to_string()]
        );
        assert_eq!(
            response.rows,
            vec![vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string())
            ]]
        );
    })
    .await;

    assert!(result.is_ok(), "Test timed out");
}

#[tokio::test]
async fn test_nats_invalid_sql_migration() {
    let result = timeout(Duration::from_secs(5), async {
        let sql_store = setup("test_nats_invalid_sql_migration").await;

        let connection_result = sql_store.connect(vec!["INVALID SQL STATEMENT"]).await;

        assert!(
            connection_result.is_err(),
            "Expected an error due to invalid SQL"
        );
    })
    .await;

    assert!(result.is_ok(), "Test timed out");
}
