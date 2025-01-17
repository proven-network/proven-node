use proven_runtime::{
    hash_options, Error, ExecutionRequest, Pool, PoolOptions, PoolRuntimeOptions,
};

use std::sync::Arc;

use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
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
        max_workers: 100,
        nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        nft_store: MemoryStore3::new(),
        personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
        personal_store: MemoryStore3::new(),
        radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
        radix_network_definition: NetworkDefinition::stokenet(),
        radix_nft_verifier: MockRadixNftVerifier::new(),
    })
    .await;
    let mut handles = vec![];
    let durations = Arc::new(Mutex::new(vec![]));

    let module = r#"
        import { getCurrentAccounts, getCurrentIdentity } from "proven:session";

        export const handler = async (a, b) => {
            const userId = getCurrentIdentity();
            const accounts = getCurrentAccounts();
            console.info("Current identity: " + userId);
            console.log("Current accounts: " + accounts);

            return a + b;
        }
    "#;

    let runtime_options = PoolRuntimeOptions {
        handler_name: Some("handler".to_string()),
        module: module.to_string(),
    };
    let options_hash = hash_options(&runtime_options)?;

    // Warm up pool with a full execution
    Arc::clone(&pool)
        .execute(
            runtime_options,
            ExecutionRequest {
                accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                args: vec![json!(10), json!(20)],
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: Some("identity".to_string()),
            },
        )
        .await
        .unwrap();

    for _ in 0..EXECUTIONS {
        let pool = Arc::clone(&pool);
        let durations = Arc::clone(&durations);
        let options_hash = options_hash.clone();
        let handle = tokio::spawn(async move {
            let request = ExecutionRequest {
                accounts: Some(vec!["account1".to_string(), "account2".to_string()]),
                args: vec![json!(10), json!(20)],
                dapp_definition_address: "dapp_definition_address".to_string(),
                identity: Some("identity".to_string()),
            };

            let start = Instant::now();
            let result = pool.execute_prehashed(options_hash, request).await;
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
