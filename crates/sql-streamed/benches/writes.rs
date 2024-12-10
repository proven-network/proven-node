// use std::time::Duration;

// use proven_sql_streamed::{Connection, StreamedSqlStore, StreamedSqlStoreOptions};

// use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
// use proven_messaging_nats::stream::{NatsStream, NatsStreamOptions};
// use proven_sql::{SqlConnection, SqlParam, SqlStore};
// use proven_store_memory::MemoryStore;
// use tokio::runtime::Runtime;

// async fn setup(stream_name: &str) -> Connection<NatsStream<Request>, MemoryStore> {
//     let client = async_nats::connect("localhost:4222").await.unwrap();
//     let jetstream_context = async_nats::jetstream::new(client.clone());

//     let _ = jetstream_context
//         .delete_stream(format!("{}_request", stream_name).to_ascii_uppercase())
//         .await;

//     let _ = jetstream_context
//         .delete_stream(format!("{}_reply", stream_name).to_ascii_uppercase())
//         .await;

//     let leader_store = MemoryStore::new();
//     let stream = NatsStream::new(NatsStreamOptions {
//         client,
//         stream_name: stream_name.to_string(),
//     });

//     StreamedSqlStore::new(StreamedSqlStoreOptions {
//         leader_store,
//         local_name: "my-machine".to_string(),
//         stream,
//     })
//     .connect(vec![
//         "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, email TEXT)",
//     ])
//     .await
//     .unwrap()
// }

// fn criterion_benchmark(c: &mut Criterion) {
//     let rt = Runtime::new().unwrap();

//     let connection = rt.block_on(setup("nats_inserts"));

//     c.bench_with_input(BenchmarkId::new("writes", "nats"), &connection, |b, s| {
//         b.to_async(&rt).iter(|| async {
//             s.execute(
//                 "INSERT INTO users (email) VALUES (?1)".to_string(),
//                 vec![SqlParam::Text("alice@example.com".to_string())],
//             )
//             .await
//             .unwrap()
//         });
//     });
// }

// criterion_group! {
//     name = benches;
//     config = Criterion::default().measurement_time(Duration::from_secs(120));
//     targets = criterion_benchmark
// }
// criterion_main!(benches);

fn main() {
    println!("Hello, world!");
}
