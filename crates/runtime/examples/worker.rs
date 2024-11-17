use proven_runtime::{ExecutionRequest, RuntimeOptions, Worker};

use std::sync::Arc;

use futures::future::join_all;
use proven_store_memory::MemoryStore;
use rustyscript::Error;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::Instant;

static EXECUTIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let module = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:sessions";

        export const handler = async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }
    "#
    .to_string();

    let worker = Arc::new(Mutex::new(
        Worker::new(RuntimeOptions {
            application_store: MemoryStore::new(),
            gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            handler_name: Some("handler".to_string()),
            module,
            nft_store: MemoryStore::new(),
            personal_store: MemoryStore::new(),
        })
        .await
        .unwrap(),
    ));
    let mut handles = vec![];
    let durations = Arc::new(Mutex::new(vec![]));

    for _ in 0..EXECUTIONS {
        let worker = Arc::clone(&worker);
        let durations = Arc::clone(&durations);
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest {
                accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                args: vec![json!(10), json!(20)],
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: Some("identity".to_string()),
            };

            let start = Instant::now();
            let result = worker.lock().await.execute(request).await.unwrap();
            let duration = start.elapsed();
            durations.lock().await.push(duration);

            assert!(result.output.is_number());
            let output = result.output.as_i64().unwrap();
            assert_eq!(output, 30);
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let start = Instant::now();
    join_all(handles).await;
    let duration = start.elapsed();
    println!("{} tasks completed in {:?}", EXECUTIONS, duration);

    let durations = durations.lock().await;
    let mut durations_vec: Vec<_> = durations.iter().cloned().collect();
    durations_vec.sort();

    let min_duration = durations_vec.first().unwrap();
    let max_duration = durations_vec.last().unwrap();
    let average_duration = durations_vec.iter().sum::<std::time::Duration>() / EXECUTIONS as u32;
    let median_duration = if EXECUTIONS % 2 == 0 {
        (durations_vec[EXECUTIONS / 2 - 1] + durations_vec[EXECUTIONS / 2]) / 2
    } else {
        durations_vec[EXECUTIONS / 2]
    };

    println!("Min execution time: {:?}", min_duration);
    println!("Max execution time: {:?}", max_duration);
    println!("Average execution time: {:?}", average_duration);
    println!("Median execution time: {:?}", median_duration);

    Ok(())
}
