use std::convert::Infallible;
use std::time::Duration;

use proven_sql_streamed::{Connection, Request, StreamedSqlStore};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use proven_messaging::stream::Stream;
use proven_messaging_nats::{
    client::NatsClientOptions,
    service::NatsServiceOptions,
    stream::{NatsStream, NatsStreamOptions},
};
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store_memory::MemoryStore;
use tokio::runtime::Runtime;

async fn setup(
    stream_name: &str,
) -> Connection<
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
            num_replicas: 3,
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
    .connect(vec![
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
    ])
    .await
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let connection = rt.block_on(setup("nats_cluster_inserts"));

    c.bench_with_input(
        BenchmarkId::new("writes", "nats_cluster"),
        &connection,
        |b, s| {
            b.to_async(&rt).iter(|| async {
                let _guard = rt.enter();
                s.execute(
                    "INSERT INTO users (email) VALUES (?1)".to_string(),
                    vec![SqlParam::Text("alice@example.com".to_string())],
                )
                .await
                .unwrap()
            });
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(120));
    targets = criterion_benchmark
}
criterion_main!(benches);
