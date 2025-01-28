use proven_runtime::{ExecutionRequest, HandlerSpecifier, ModuleLoader, RuntimeOptions, Worker};

use std::sync::Arc;

use futures::future::join_all;
use proven_code_package::CodePackage;
use proven_radix_nft_verifier_mock::MockRadixNftVerifier;
use proven_sql_direct::{DirectSqlStore2, DirectSqlStore3};
use proven_store_memory::{MemoryStore, MemoryStore2, MemoryStore3};
use radix_common::network::NetworkDefinition;
use rustyscript::Error;
use serde_json::json;
use tempfile::tempdir;
use tokio::sync::Mutex;
use tokio::time::Instant;

static EXECUTIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let module_loader = ModuleLoader::new(
        CodePackage::from_str(
            r#"
            import { run } from "@proven-network/handler";
            import { getCurrentAccounts, getCurrentIdentity } from "proven:session";

            export const handler = run(async (a, b) => {
                const userId = getCurrentIdentity();
                const accounts = getCurrentAccounts();
                console.info("Current identity: " + userId);
                console.log("Current accounts: " + accounts);

                return a + b;
            });
        "#,
        )
        .unwrap(),
    );

    let worker = Arc::new(Mutex::new(
        Worker::new(RuntimeOptions {
            application_sql_store: DirectSqlStore2::new(tempdir().unwrap().into_path()),
            application_store: MemoryStore2::new(),
            file_system_store: MemoryStore::new(),
            module_loader,
            nft_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            nft_store: MemoryStore3::new(),
            personal_sql_store: DirectSqlStore3::new(tempdir().unwrap().into_path()),
            personal_store: MemoryStore3::new(),
            radix_gateway_origin: "https://stokenet.radixdlt.com".to_string(),
            radix_network_definition: NetworkDefinition::stokenet(),
            radix_nft_verifier: MockRadixNftVerifier::new(),
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
            let request = ExecutionRequest::Rpc {
                accounts: vec!["my_account_1".to_string(), "my_account_2".to_string()],
                args: vec![json!(10), json!(20)],
                dapp_definition_address: "dapp_definition_address".to_string(),
                handler_specifier: HandlerSpecifier::parse("file:///main.ts#handler").unwrap(),
                identity: "my_identity".to_string(),
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
