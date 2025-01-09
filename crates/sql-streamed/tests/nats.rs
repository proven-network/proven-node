use std::convert::Infallible;

use bytes::Bytes;
use futures::StreamExt;
use proven_messaging::stream::{InitializedStream, Stream};
use proven_messaging_nats::{
    client::NatsClientOptions,
    service::NatsServiceOptions,
    stream::{NatsStream, NatsStreamOptions},
};
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_sql_streamed::{Request, StreamedSqlStore};
use proven_store::Store;
use proven_store_memory::MemoryStore;
use tokio::time::{sleep, timeout, Duration};

async fn setup(
    stream_name: &str,
) -> StreamedSqlStore<
    NatsStream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
    MemoryStore<Bytes, Infallible, Infallible>,
> {
    let client = async_nats::connect("localhost:4222").await.unwrap();
    let jetstream_context = async_nats::jetstream::new(client.clone());

    let _ = jetstream_context.delete_stream(stream_name).await;

    let stream = NatsStream::new(
        stream_name,
        NatsStreamOptions {
            client: client.clone(),
        },
    );

    StreamedSqlStore::new(
        stream,
        NatsServiceOptions {
            client: client.clone(),
            durable_name: None,
            jetstream_context,
        },
        NatsClientOptions {
            client: client.clone(),
        },
        MemoryStore::new(),
    )
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

        let mut rows = connection
            .query("SELECT id, email FROM users".to_string(), vec![])
            .await
            .unwrap();

        let mut results = Vec::new();
        while let Some(row) = rows.next().await {
            results.push(row);
        }

        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0],
            vec![
                SqlParam::Integer(1),
                SqlParam::Text("alice@example.com".to_string())
            ]
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

#[tokio::test]
async fn test_nats_snapshotting() {
    let result = timeout(Duration::from_secs(5), async {
        let stream_name = "test_nats_snapshotting";
        let client = async_nats::connect("localhost:4222").await.unwrap();
        let jetstream_context = async_nats::jetstream::new(client.clone());

        let _ = jetstream_context.delete_stream(stream_name).await;

        let stream = NatsStream::new(
            stream_name,
            NatsStreamOptions {
                client: client.clone(),
            },
        );

        let snapshot_store = MemoryStore::new();

        let sql_store = StreamedSqlStore::new(
            stream.clone(),
            NatsServiceOptions {
                client: client.clone(),
                durable_name: None,
                jetstream_context: jetstream_context.clone(),
            },
            NatsClientOptions {
                client: client.clone(),
            },
            snapshot_store.clone(),
        );

        let connection = sql_store
            .connect(vec![
                "CREATE TABLE IF NOT EXISTS users (id INTEGER, email TEXT)",
            ])
            .await
            .unwrap();

        // Execute 1000 inserts
        for i in 0..1000 {
            connection
                .execute(
                    "INSERT INTO users (id, email) VALUES (?1, ?2)".to_string(),
                    vec![
                        SqlParam::Integer(i),
                        SqlParam::Text("alice@example.com".to_string()),
                    ],
                )
                .await
                .unwrap();
        }

        // Wait for 3 seconds to allow snapshotting
        sleep(Duration::from_secs(3)).await;

        let initialized_stream = stream.init().await.unwrap();

        // Check last message in the stream is a snapshot and that snapshot is in the snapshot store
        match initialized_stream.last_message().await.unwrap().unwrap() {
            Request::Snapshot(snapshot_key) => {
                let snapstop = snapshot_store.get(&snapshot_key).await.unwrap();
                assert!(snapstop.is_some());
            }
            _ => panic!("Expected last message to be a snapshot"),
        }

        // Check that the other messages have been purged in rollup.
        let mut raw_stream = jetstream_context.get_stream(stream_name).await.unwrap();
        assert_eq!(raw_stream.info().await.unwrap().state.messages, 1);
    })
    .await;

    assert!(result.is_ok(), "Test timed out");
}
