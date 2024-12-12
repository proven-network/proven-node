use std::time::Duration;

use proven_sql_streamed::{Connection, Request, StreamedSqlStore};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use proven_messaging::stream::Stream;
use proven_messaging_nats::{
    client::NatsClientOptions,
    service::NatsServiceOptions,
    stream::{NatsStream, NatsStreamOptions},
};
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use tokio::runtime::Runtime;

async fn setup(
    stream_name: &str,
) -> Connection<
    NatsStream<Request, ciborium::de::Error<std::io::Error>, ciborium::ser::Error<std::io::Error>>,
> {
    let client = async_nats::connect("localhost:4222").await.unwrap();
    let jetstream_context = async_nats::jetstream::new(client.clone());

    jetstream_context.delete_stream(stream_name).await.unwrap();

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
        NatsClientOptions,
    )
    .connect(vec![
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
    ])
    .await
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let connection = rt.block_on(setup("nats_inserts"));

    c.bench_with_input(BenchmarkId::new("writes", "nats"), &connection, |b, s| {
        b.to_async(&rt).iter(|| async {
            s.execute(
                "INSERT INTO users (email) VALUES (?1)".to_string(),
                vec![SqlParam::Text("alice@example.com".to_string())],
            )
            .await
            .unwrap()
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(120));
    targets = criterion_benchmark
}
criterion_main!(benches);
