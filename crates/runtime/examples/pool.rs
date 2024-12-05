use proven_runtime::{Error, ExecutionRequest, Pool, PoolOptions, PoolRuntimeOptions};

use std::sync::Arc;

use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore2, MemoryStore3};
use radix_common::network::NetworkDefinition;
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::Instant;

static EXECUTIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let pool = Pool::new(PoolOptions {
        application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
        application_store: MemoryStore2::new(),
        max_workers: 10,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore3::new(),
        radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        radix_network_definition: NetworkDefinition::stokenet(),
    })
    .await;

    let mut handles = vec![];
    let durations = Arc::new(Mutex::new(vec![]));

    let base_script = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:sessions";

        export const handler = async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }
    "#;

    for i in 0..EXECUTIONS {
        // Add the invocation number to every 2nd script to make it unique
        // This tests a mix of unique and duplicate scripts
        let module = if i % 2 == 0 {
            format!("// Invocation: {}\n{}", i, base_script)
        } else {
            base_script.to_string()
        };

        let pool = Arc::clone(&pool);
        let durations = Arc::clone(&durations);
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest {
                accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                args: vec![json!(10), json!(20)],
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: Some("identity".to_string()),
            };

            let start = Instant::now();
            let result = pool
                .execute(
                    PoolRuntimeOptions {
                        handler_name: Some("handler".to_string()),
                        module,
                    },
                    request,
                )
                .await;
            let duration = start.elapsed();
            durations.lock().await.push(duration);

            match result {
                Ok(result) => {
                    assert!(result.output.is_number());
                    let output = result.output.as_i64().unwrap();
                    assert_eq!(output, 30);
                }
                Err(e) => eprintln!("Error in result: {:?}", e),
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let start = Instant::now();
    futures::future::join_all(handles).await;
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
