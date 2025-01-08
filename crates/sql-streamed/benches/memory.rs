use std::time::Duration;

use proven_sql_streamed::{Connection, Request, StreamedSqlStore};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use proven_messaging::stream::Stream;
use proven_messaging_memory::{
    client::MemoryClientOptions,
    service::MemoryServiceOptions,
    stream::{MemoryStream, MemoryStreamOptions},
};
use proven_sql::{SqlConnection, SqlParam, SqlStore};
use proven_store_memory::MemoryStore;
use tokio::runtime::Runtime;

async fn setup(
    stream_name: &str,
) -> Connection<
    MemoryStream<
        Request,
        ciborium::de::Error<std::io::Error>,
        ciborium::ser::Error<std::io::Error>,
    >,
> {
    let stream = MemoryStream::new(stream_name, MemoryStreamOptions);

    StreamedSqlStore::new(
        stream,
        MemoryServiceOptions,
        MemoryClientOptions,
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

    let connection = rt.block_on(setup("memory_inserts"));

    c.bench_with_input(BenchmarkId::new("writes", "memory"), &connection, |b, s| {
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
